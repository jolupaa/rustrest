use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use http_body_util::{BodyExt, Full, Limited};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::net::TcpListener;

/// Maximum request body we will buffer into memory (64 KB).
const MAX_BODY_BYTES: usize = 64 * 1024;

/// A route handler, normalized from a sync or async user handler. `Arc` so it
/// can be cloned into the middleware chain (see [`Next`]).
pub type Handler =
    Arc<dyn Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync>;

/// The continuation passed to a middleware: calling it runs the rest of the
/// chain (the next middleware, or finally the matched handler).
pub type Next = Box<dyn FnOnce(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send>;

/// A middleware in the onion model: receives the request and `next`, and may
/// run code before/after `next(req).await`, or short-circuit by returning a
/// `Response` without calling `next`.
pub type Middleware =
    Arc<dyn Fn(Request, Next) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync>;

/// A single segment of a route pattern.
#[derive(Clone)]
enum Segment {
    Static(String),
    /// A `:name` placeholder, storing `name` (without the colon).
    Param(String),
}

/// A registered route: method, parsed path pattern, handler, and the
/// middleware chain (outermost-first) accumulated from the routers it was
/// mounted through.
struct Route {
    method: String,
    pattern: Vec<Segment>,
    handler: Handler,
    middlewares: Vec<Middleware>,
}

/// Splits a path into non-empty segments (trailing/duplicate slashes ignored).
fn path_segments(path: &str) -> Vec<&str> {
    path.split('/').filter(|s| !s.is_empty()).collect()
}

/// Parses a route pattern like `/users/:id` into segments.
fn parse_pattern(path: &str) -> Vec<Segment> {
    path_segments(path)
        .into_iter()
        .map(|s| match s.strip_prefix(':') {
            Some(name) => Segment::Param(name.to_string()),
            None => Segment::Static(s.to_string()),
        })
        .collect()
}

/// Matches a parsed pattern against concrete path segments, capturing params.
/// Returns `None` if the pattern does not match.
fn match_pattern(pattern: &[Segment], segments: &[&str]) -> Option<HashMap<String, String>> {
    if pattern.len() != segments.len() {
        return None;
    }
    let mut params = HashMap::new();
    for (seg, actual) in pattern.iter().zip(segments) {
        match seg {
            Segment::Static(s) if s == actual => {}
            Segment::Static(_) => return None,
            Segment::Param(name) => {
                params.insert(name.clone(), (*actual).to_string());
            }
        }
    }
    Some(params)
}

/// Request data handed to each route handler. Fields are part of the
/// handler-facing API; some demo handlers ignore them.
#[allow(dead_code)]
pub struct Request {
    pub method: String,
    pub path: String,
    /// Raw query string, if any: `/users?id=1` -> `Some("id=1")`.
    pub query: Option<String>,
    pub headers: HashMap<String, String>,
    /// Request body, decoded as UTF-8 (lossy), capped at 64 KB.
    pub body: String,
    /// Captured path parameters, e.g. `/users/:id` matching `/users/42`
    /// yields `{"id": "42"}`.
    pub params: HashMap<String, String>,
}

impl Request {
    /// Returns a captured path parameter by name.
    pub fn param(&self, name: &str) -> Option<&str> {
        self.params.get(name).map(String::as_str)
    }

    /// Deserializes the request body as JSON into `T`.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_str(&self.body)
    }
}

pub struct Response {
    pub status: u16,
    pub body: String,
    pub content_type: &'static str,
}

impl Response {
    pub fn send(text: &str) -> Self {
        Self {
            status: 200,
            body: text.to_string(),
            content_type: "text/plain; charset=utf-8",
        }
    }

    /// Serializes `value` to JSON. If serialization fails, degrades to a 500.
    pub fn json<T: Serialize>(value: &T) -> Self {
        match serde_json::to_string(value) {
            Ok(body) => Self {
                status: 200,
                body,
                content_type: "application/json",
            },
            Err(_) => Self {
                status: 500,
                body: "500 Internal Server Error".to_string(),
                content_type: "text/plain; charset=utf-8",
            },
        }
    }

    pub fn not_found() -> Self {
        Self {
            status: 404,
            body: "404 Not Found".to_string(),
            content_type: "text/plain; charset=utf-8",
        }
    }

    pub fn bad_request() -> Self {
        Self {
            status: 400,
            body: "400 Bad Request".to_string(),
            content_type: "text/plain; charset=utf-8",
        }
    }

    /// Converts our framework response into a hyper response.
    fn into_hyper(self) -> hyper::Response<Full<Bytes>> {
        hyper::Response::builder()
            .status(self.status)
            .header(hyper::header::CONTENT_TYPE, self.content_type)
            .body(Full::new(Bytes::from(self.body)))
            .expect("status and headers are always valid")
    }
}

/// Converts a user handler — synchronous *or* asynchronous — into the internal
/// [`Handler`] shape.
///
/// The `Marker` type parameter only exists so the two blanket impls (one for
/// `Fn(Request) -> Response`, one for `Fn(Request) -> Future`) can coexist
/// without overlapping. Callers never name it; it is inferred from the
/// closure's return type.
pub trait IntoHandler<Marker> {
    fn into_handler(self) -> Handler;
}

#[doc(hidden)]
pub struct SyncMarker;
#[doc(hidden)]
pub struct AsyncMarker;

// Synchronous handlers: `|req| Response`.
impl<F> IntoHandler<SyncMarker> for F
where
    F: Fn(Request) -> Response + Send + Sync + 'static,
{
    fn into_handler(self) -> Handler {
        Arc::new(
            move |req| -> Pin<Box<dyn Future<Output = Response> + Send>> {
                let res = self(req);
                Box::pin(async move { res })
            },
        )
    }
}

// Asynchronous handlers: `|req| async { Response }`.
impl<F, Fut> IntoHandler<AsyncMarker> for F
where
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Response> + Send + 'static,
{
    fn into_handler(self) -> Handler {
        Arc::new(
            move |req| -> Pin<Box<dyn Future<Output = Response> + Send>> { Box::pin(self(req)) },
        )
    }
}

/// Converts a user middleware closure into the internal [`Middleware`] shape.
pub trait IntoMiddleware {
    fn into_middleware(self) -> Middleware;
}

impl<F, Fut> IntoMiddleware for F
where
    F: Fn(Request, Next) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Response> + Send + 'static,
{
    fn into_middleware(self) -> Middleware {
        Arc::new(
            move |req, next| -> Pin<Box<dyn Future<Output = Response> + Send>> {
                Box::pin(self(req, next))
            },
        )
    }
}

/// A collection of routes that can be defined independently (e.g. in its own
/// module/file) and mounted onto an [`App`] or another `Router` under a prefix.
pub struct Router {
    routes: Vec<Route>,
    middlewares: Vec<Middleware>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            routes: Vec::new(),
            middlewares: Vec::new(),
        }
    }

    pub fn get<H, M>(&mut self, path: &str, handler: H)
    where
        H: IntoHandler<M>,
    {
        self.add("GET", path, handler);
    }

    pub fn post<H, M>(&mut self, path: &str, handler: H)
    where
        H: IntoHandler<M>,
    {
        self.add("POST", path, handler);
    }

    pub fn put<H, M>(&mut self, path: &str, handler: H)
    where
        H: IntoHandler<M>,
    {
        self.add("PUT", path, handler);
    }

    pub fn delete<H, M>(&mut self, path: &str, handler: H)
    where
        H: IntoHandler<M>,
    {
        self.add("DELETE", path, handler);
    }

    /// Adds a middleware scoped to this router: it wraps every route in this
    /// router (and routers mounted into it), and nothing else. Applied when
    /// the router is mounted.
    pub fn layer<MW: IntoMiddleware>(&mut self, middleware: MW) {
        self.middlewares.push(middleware.into_middleware());
    }

    fn add<H, M>(&mut self, method: &str, path: &str, handler: H)
    where
        H: IntoHandler<M>,
    {
        self.routes.push(Route {
            method: method.to_string(),
            pattern: parse_pattern(path),
            handler: handler.into_handler(),
            middlewares: Vec::new(),
        });
    }

    /// Mounts another router under `prefix`, prepending `prefix` to every one
    /// of its route patterns and baking `other`'s scoped middlewares into each
    /// route. Routes are flattened, so nesting composes (a router that already
    /// had sub-routers mounted carries their patterns and middlewares along).
    pub fn mount(&mut self, prefix: &str, other: Router) {
        let prefix = parse_pattern(prefix);
        let scoped = other.middlewares;
        for route in other.routes {
            let mut pattern = prefix.clone();
            pattern.extend(route.pattern);
            // `other`'s own middlewares wrap its routes from the outside, then
            // any middlewares the route already carries (from deeper mounts).
            let mut middlewares = scoped.clone();
            middlewares.extend(route.middlewares);
            self.routes.push(Route {
                method: route.method,
                pattern,
                handler: route.handler,
                middlewares,
            });
        }
    }

    /// Finds the first registered route matching `method` + `path`, returning
    /// a clone of its handler, its scoped middleware chain, and any captured
    /// path parameters. Routes are tried in registration order (register more
    /// specific routes first).
    fn route(
        &self,
        method: &str,
        path: &str,
    ) -> Option<(Handler, Vec<Middleware>, HashMap<String, String>)> {
        let segments = path_segments(path);
        for route in &self.routes {
            if route.method != method {
                continue;
            }
            if let Some(params) = match_pattern(&route.pattern, &segments) {
                return Some((
                    Arc::clone(&route.handler),
                    route.middlewares.clone(),
                    params,
                ));
            }
        }
        None
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

/// A handler that always responds 404, used when no route matches (so global
/// middleware still runs for unmatched requests).
fn not_found_handler() -> Handler {
    Arc::new(
        |_req: Request| -> Pin<Box<dyn Future<Output = Response> + Send>> {
            Box::pin(async { Response::not_found() })
        },
    )
}

pub struct App {
    router: Router,
    middlewares: Vec<Middleware>,
}

impl App {
    pub fn new() -> Self {
        Self {
            router: Router::new(),
            middlewares: Vec::new(),
        }
    }

    pub fn get<H, M>(&mut self, path: &str, handler: H)
    where
        H: IntoHandler<M>,
    {
        self.router.get(path, handler);
    }

    // These delegate to the root router and are part of the public API even
    // when a given binary registers its routes through a Router instead.
    #[allow(dead_code)]
    pub fn post<H, M>(&mut self, path: &str, handler: H)
    where
        H: IntoHandler<M>,
    {
        self.router.post(path, handler);
    }

    #[allow(dead_code)]
    pub fn put<H, M>(&mut self, path: &str, handler: H)
    where
        H: IntoHandler<M>,
    {
        self.router.put(path, handler);
    }

    #[allow(dead_code)]
    pub fn delete<H, M>(&mut self, path: &str, handler: H)
    where
        H: IntoHandler<M>,
    {
        self.router.delete(path, handler);
    }

    /// Mounts a router under `prefix` (Express-style sub-routes).
    pub fn mount(&mut self, prefix: &str, router: Router) {
        self.router.mount(prefix, router);
    }

    /// Adds a global middleware (onion model). Middlewares run in registration
    /// order on the way in, and in reverse on the way out.
    pub fn layer<MW: IntoMiddleware>(&mut self, middleware: MW) {
        self.middlewares.push(middleware.into_middleware());
    }

    pub async fn listen(self, address: &str) {
        let listener = TcpListener::bind(address)
            .await
            .expect("Unable to start server");
        let app = Arc::new(self);

        println!("Servidor escuchando en http://{}", address);

        loop {
            let (stream, _) = listener
                .accept()
                .await
                .expect("Error accepting the connection");

            // Adapt the tokio stream to hyper's IO traits.
            let io = TokioIo::new(stream);
            let app = Arc::clone(&app);

            // Serve each connection concurrently.
            tokio::spawn(async move {
                let service = service_fn(move |req: hyper::Request<Incoming>| {
                    let app = Arc::clone(&app);
                    async move { Ok::<_, Infallible>(app.handle(req).await) }
                });

                if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                    eprintln!("Error sirviendo la conexión: {:?}", err);
                }
            });
        }
    }

    /// Translates a hyper request into a [`Request`] (method, path, query,
    /// headers, size-bounded body), dispatches it, and converts the result.
    async fn handle(&self, req: hyper::Request<Incoming>) -> hyper::Response<Full<Bytes>> {
        // Read everything that only needs a borrow before consuming the body.
        let method = req.method().as_str().to_string();
        let path = req.uri().path().to_string();
        let query = req.uri().query().map(|q| q.to_string());
        let headers = req
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_string(),
                    value.to_str().unwrap_or("").to_string(),
                )
            })
            .collect();

        // Buffer the body up to MAX_BODY_BYTES; on overflow or read error,
        // fall back to an empty body.
        let body = match Limited::new(req.into_body(), MAX_BODY_BYTES)
            .collect()
            .await
        {
            Ok(collected) => String::from_utf8_lossy(&collected.to_bytes()).into_owned(),
            Err(_) => String::new(),
        };

        let request = Request {
            method,
            path,
            query,
            headers,
            body,
            params: HashMap::new(),
        };

        self.dispatch(request).await.into_hyper()
    }

    /// Routes the request (capturing path params), then runs it through the
    /// middleware onion ending at the matched handler (or a 404 handler).
    async fn dispatch(&self, mut request: Request) -> Response {
        let (handler, route_middlewares, params) = self
            .router
            .route(&request.method, &request.path)
            .unwrap_or_else(|| (not_found_handler(), Vec::new(), HashMap::new()));
        request.params = params;

        // Innermost layer: the matched handler.
        let mut next: Next = Box::new(move |req| (*handler)(req));

        // Route-scoped middlewares (inner), then global App middlewares
        // (outer). Each group is wrapped last-to-first so the first-registered
        // middleware in the group ends up outermost within that group.
        for middleware in route_middlewares.iter().rev() {
            let middleware = Arc::clone(middleware);
            let inner = next;
            next = Box::new(move |req| (*middleware)(req, inner));
        }
        for middleware in self.middlewares.iter().rev() {
            let middleware = Arc::clone(middleware);
            let inner = next;
            next = Box::new(move |req| (*middleware)(req, inner));
        }

        next(request).await
    }
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn dummy_request(body: &str) -> Request {
        Request {
            method: "GET".to_string(),
            path: "/".to_string(),
            query: None,
            headers: HashMap::new(),
            body: body.to_string(),
            params: HashMap::new(),
        }
    }

    #[test]
    fn send_sets_text_plain_and_200() {
        let res = Response::send("hola");
        assert_eq!(res.status, 200);
        assert_eq!(res.body, "hola");
        assert_eq!(res.content_type, "text/plain; charset=utf-8");
    }

    #[test]
    fn not_found_sets_404() {
        let res = Response::not_found();
        assert_eq!(res.status, 404);
        assert_eq!(res.body, "404 Not Found");
    }

    #[test]
    fn bad_request_sets_400() {
        let res = Response::bad_request();
        assert_eq!(res.status, 400);
        assert_eq!(res.content_type, "text/plain; charset=utf-8");
    }

    #[test]
    fn json_serializes_value_with_200_and_json_content_type() {
        #[derive(Serialize)]
        struct User {
            id: u32,
            name: &'static str,
        }
        let res = Response::json(&User { id: 1, name: "Ada" });
        assert_eq!(res.status, 200);
        assert_eq!(res.content_type, "application/json");
        assert_eq!(res.body, r#"{"id":1,"name":"Ada"}"#);
    }

    #[test]
    fn json_serialization_error_degrades_to_500() {
        // serde_json cannot serialize a map with non-string (tuple) keys.
        let mut map: HashMap<(i32, i32), i32> = HashMap::new();
        map.insert((1, 2), 3);
        let res = Response::json(&map);
        assert_eq!(res.status, 500);
        assert_eq!(res.content_type, "text/plain; charset=utf-8");
    }

    #[test]
    fn into_hyper_maps_status_and_content_type_header() {
        let res = Response::send("hi").into_hyper();
        assert_eq!(res.status(), 200);
        assert_eq!(
            res.headers().get(hyper::header::CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
    }

    #[test]
    fn request_json_deserializes_body() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct User {
            id: u32,
            name: String,
        }
        let req = dummy_request(r#"{"id":1,"name":"Ada"}"#);
        let user: User = req.json().unwrap();
        assert_eq!(
            user,
            User {
                id: 1,
                name: "Ada".to_string()
            }
        );
    }

    #[test]
    fn request_json_errors_on_invalid_body() {
        let req = dummy_request("not json");
        assert!(req.json::<serde_json::Value>().is_err());
    }

    #[test]
    fn match_pattern_captures_params_and_rejects_mismatches() {
        let pattern = parse_pattern("/users/:id/posts");
        let params = match_pattern(&pattern, &path_segments("/users/42/posts")).unwrap();
        assert_eq!(params.get("id").map(String::as_str), Some("42"));
        assert!(match_pattern(&pattern, &path_segments("/users/42")).is_none());
        assert!(match_pattern(&pattern, &path_segments("/users/42/comments")).is_none());
    }

    #[test]
    fn router_matches_method_and_path_param() {
        let mut router = Router::new();
        router.get("/users", |_r: Request| Response::send("list"));
        router.get("/users/:id", |req: Request| {
            Response::send(req.param("id").unwrap_or("?"))
        });

        assert!(router.route("GET", "/users").is_some());
        assert!(router.route("POST", "/users").is_none());
        assert!(router.route("GET", "/nope/extra").is_none());

        let (_handler, _mws, params) = router.route("GET", "/users/42").expect("should match");
        assert_eq!(params.get("id").map(String::as_str), Some("42"));
    }

    #[test]
    fn mount_concatenates_prefixes_across_nesting() {
        let mut users = Router::new();
        users.get("/:id", |req: Request| {
            Response::send(req.param("id").unwrap_or("?"))
        });

        let mut api = Router::new();
        api.mount("/users", users);

        let mut root = Router::new();
        root.mount("/api", api);

        let (_handler, _mws, params) = root.route("GET", "/api/users/42").expect("should match");
        assert_eq!(params.get("id").map(String::as_str), Some("42"));
        // Only `/:id` was registered, so the bare collection path does not match.
        assert!(root.route("GET", "/api/users").is_none());
    }

    #[tokio::test]
    async fn dispatch_runs_sync_handler() {
        let mut app = App::new();
        app.get("/", |_r: Request| Response::send("sync"));
        let res = app.dispatch(dummy_request("")).await;
        assert_eq!(res.body, "sync");
    }

    #[tokio::test]
    async fn dispatch_runs_async_handler() {
        let mut app = App::new();
        app.get("/", |_r: Request| async move { Response::send("async") });
        let res = app.dispatch(dummy_request("")).await;
        assert_eq!(res.body, "async");
    }

    #[tokio::test]
    async fn dispatch_unmatched_returns_404() {
        let app = App::new();
        let res = app.dispatch(dummy_request("")).await;
        assert_eq!(res.status, 404);
    }

    #[tokio::test]
    async fn middleware_wraps_handler_and_runs() {
        let mut app = App::new();
        app.get("/", |_r: Request| Response::send("handler"));

        let hits = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&hits);
        app.layer(move |req: Request, next: Next| {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                next(req).await
            }
        });

        let res = app.dispatch(dummy_request("")).await;
        assert_eq!(res.body, "handler");
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn middleware_can_short_circuit() {
        let mut app = App::new();
        app.get("/", |_r: Request| Response::send("handler"));
        app.layer(|_req: Request, _next: Next| async move { Response::send("blocked") });

        let res = app.dispatch(dummy_request("")).await;
        assert_eq!(res.body, "blocked");
    }

    #[tokio::test]
    async fn middlewares_nest_in_registration_order() {
        let mut app = App::new();
        app.get("/", |_r: Request| Response::send("h"));

        let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let o1 = Arc::clone(&order);
        app.layer(move |req: Request, next: Next| {
            let o1 = Arc::clone(&o1);
            async move {
                o1.lock().unwrap().push("mw1-in");
                let res = next(req).await;
                o1.lock().unwrap().push("mw1-out");
                res
            }
        });
        let o2 = Arc::clone(&order);
        app.layer(move |req: Request, next: Next| {
            let o2 = Arc::clone(&o2);
            async move {
                o2.lock().unwrap().push("mw2-in");
                let res = next(req).await;
                o2.lock().unwrap().push("mw2-out");
                res
            }
        });

        let _ = app.dispatch(dummy_request("")).await;
        assert_eq!(
            *order.lock().unwrap(),
            vec!["mw1-in", "mw2-in", "mw2-out", "mw1-out"]
        );
    }

    #[tokio::test]
    async fn router_layer_scopes_middleware_to_its_routes() {
        let hits = Arc::new(AtomicUsize::new(0));

        let mut scoped = Router::new();
        let counter = Arc::clone(&hits);
        scoped.layer(move |req: Request, next: Next| {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                next(req).await
            }
        });
        scoped.get("/thing", |_r: Request| Response::send("scoped"));

        let mut app = App::new();
        app.get("/", |_r: Request| Response::send("root"));
        app.mount("/api", scoped);

        // A request under the mount runs the scoped middleware.
        let mut req = dummy_request("");
        req.path = "/api/thing".to_string();
        let res = app.dispatch(req).await;
        assert_eq!(res.body, "scoped");
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        // A request outside the mount does not.
        let res = app.dispatch(dummy_request("")).await;
        assert_eq!(res.body, "root");
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }
}
