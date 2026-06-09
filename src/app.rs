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

/// Internal handler shape: every user handler (sync or async) is normalized
/// into a function returning a boxed `Future<Output = Response>` via
/// [`IntoHandler`].
pub type Handler =
    Box<dyn Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync>;

/// A single segment of a route pattern.
enum Segment {
    Static(String),
    /// A `:name` placeholder, storing `name` (without the colon).
    Param(String),
}

/// A registered route: an HTTP method, a parsed path pattern, and its handler.
struct Route {
    method: String,
    pattern: Vec<Segment>,
    handler: Handler,
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

pub struct App {
    routes: Vec<Route>,
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
        Box::new(
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
        Box::new(
            move |req| -> Pin<Box<dyn Future<Output = Response> + Send>> { Box::pin(self(req)) },
        )
    }
}

impl App {
    pub fn new() -> Self {
        Self { routes: Vec::new() }
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

    fn add<H, M>(&mut self, method: &str, path: &str, handler: H)
    where
        H: IntoHandler<M>,
    {
        self.routes.push(Route {
            method: method.to_string(),
            pattern: parse_pattern(path),
            handler: handler.into_handler(),
        });
    }

    /// Finds the first registered route matching `method` + `path`, returning
    /// its handler and any captured path parameters. Routes are tried in
    /// registration order (register more specific routes first).
    fn route(&self, method: &str, path: &str) -> Option<(&Handler, HashMap<String, String>)> {
        let segments = path_segments(path);
        for route in &self.routes {
            if route.method != method {
                continue;
            }
            if let Some(params) = match_pattern(&route.pattern, &segments) {
                return Some((&route.handler, params));
            }
        }
        None
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

    /// Builds a [`Request`] from the hyper request, routes it to a matching
    /// handler (capturing path params) or 404, awaits the handler, and
    /// converts the result to a hyper response.
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

        // Match the route before consuming the body so we can capture params.
        let matched = self.route(&method, &path);

        // Buffer the body up to MAX_BODY_BYTES; on overflow or read error,
        // fall back to an empty body.
        let body = match Limited::new(req.into_body(), MAX_BODY_BYTES)
            .collect()
            .await
        {
            Ok(collected) => String::from_utf8_lossy(&collected.to_bytes()).into_owned(),
            Err(_) => String::new(),
        };

        let response = match matched {
            Some((handler, params)) => {
                let request = Request {
                    method,
                    path,
                    query,
                    headers,
                    body,
                    params,
                };
                handler(request).await
            }
            None => Response::not_found(),
        };

        response.into_hyper()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::collections::HashMap;

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

    #[tokio::test]
    async fn sync_handler_is_accepted_and_runs() {
        let handler: Handler = (|_req: Request| Response::send("sync")).into_handler();
        let res = handler(dummy_request("")).await;
        assert_eq!(res.status, 200);
        assert_eq!(res.body, "sync");
    }

    #[tokio::test]
    async fn async_handler_is_accepted_and_runs() {
        let handler: Handler =
            (|_req: Request| async move { Response::send("async") }).into_handler();
        let res = handler(dummy_request("")).await;
        assert_eq!(res.status, 200);
        assert_eq!(res.body, "async");
    }

    #[test]
    fn match_pattern_captures_params_and_rejects_mismatches() {
        let pattern = parse_pattern("/users/:id/posts");
        // Matching path captures the param.
        let params = match_pattern(&pattern, &path_segments("/users/42/posts")).unwrap();
        assert_eq!(params.get("id").map(String::as_str), Some("42"));
        // Different length -> no match.
        assert!(match_pattern(&pattern, &path_segments("/users/42")).is_none());
        // Different static segment -> no match.
        assert!(match_pattern(&pattern, &path_segments("/users/42/comments")).is_none());
    }

    #[tokio::test]
    async fn routing_matches_method_and_path_param() {
        let mut app = App::new();
        app.get("/users", |_r: Request| Response::send("list"));
        app.get("/users/:id", |req: Request| {
            Response::send(req.param("id").unwrap_or("?"))
        });

        // Static route matches.
        assert!(app.route("GET", "/users").is_some());
        // Method mismatch / unknown path -> no match.
        assert!(app.route("POST", "/users").is_none());
        assert!(app.route("GET", "/nope/extra").is_none());

        // Param route captures the value and the handler can read it.
        let (handler, params) = app.route("GET", "/users/42").expect("should match");
        assert_eq!(params.get("id").map(String::as_str), Some("42"));
        let mut req = dummy_request("");
        req.params = params;
        let res = handler(req).await;
        assert_eq!(res.body, "42");
    }
}
