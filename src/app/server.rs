use std::collections::HashMap;
use std::convert::Infallible;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use futures_util::FutureExt;
use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use hyper::header::{CONNECTION, COOKIE, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION, UPGRADE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use super::{
    ErrorHandler, HttpError, IntoHandler, IntoMiddleware, Middleware, Next, Request, Response,
    Router, StateStore,
};
use super::{ResponseBody, not_found_handler, panic_response, parse_cookies, parse_query};

/// Maximum request body we will buffer into memory (64 KB).
const MAX_BODY_BYTES: usize = 64 * 1024;

fn is_websocket_upgrade_request(req: &hyper::Request<Incoming>) -> bool {
    req.method().as_str().eq_ignore_ascii_case("GET")
        && req
            .headers()
            .get(UPGRADE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.eq_ignore_ascii_case("websocket"))
        && req
            .headers()
            .get(CONNECTION)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value
                    .split(',')
                    .any(|part| part.trim().eq_ignore_ascii_case("upgrade"))
            })
        && req.headers().contains_key(SEC_WEBSOCKET_KEY)
        && req
            .headers()
            .get(SEC_WEBSOCKET_VERSION)
            .and_then(|value| value.to_str().ok())
            == Some("13")
}

pub struct App {
    router: Router,
    middlewares: Vec<Middleware>,
    state: StateStore,
    error_handler: Option<ErrorHandler>,
}

impl App {
    pub fn new() -> Self {
        Self {
            router: Router::new(),
            middlewares: Vec::new(),
            state: StateStore::default(),
            error_handler: None,
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

    pub fn patch<H, M>(&mut self, path: &str, handler: H)
    where
        H: IntoHandler<M>,
    {
        self.router.patch(path, handler);
    }

    pub fn options<H, M>(&mut self, path: &str, handler: H)
    where
        H: IntoHandler<M>,
    {
        self.router.options(path, handler);
    }

    pub fn head<H, M>(&mut self, path: &str, handler: H)
    where
        H: IntoHandler<M>,
    {
        self.router.head(path, handler);
    }

    pub fn all<H, M>(&mut self, path: &str, handler: H)
    where
        H: IntoHandler<M>,
    {
        self.router.all(path, handler);
    }

    pub fn websocket<F, Fut>(&mut self, path: &str, handler: F)
    where
        F: Fn(super::WebSocket) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        self.router.websocket(path, handler);
    }

    pub fn ws<F, Fut>(&mut self, path: &str, handler: F)
    where
        F: Fn(super::WebSocket) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        self.router.ws(path, handler);
    }

    /// Mounts a router under `prefix` (Express-style sub-routes).
    pub fn mount(&mut self, prefix: &str, router: Router) {
        self.router.mount(prefix, router);
    }

    pub fn fallback<H, M>(&mut self, handler: H)
    where
        H: IntoHandler<M>,
    {
        self.router.fallback(handler);
    }

    pub fn static_files<P>(&mut self, prefix: &str, root: P)
    where
        P: Into<std::path::PathBuf>,
    {
        self.router.static_files(prefix, root);
    }

    pub fn state<T>(&mut self, value: T)
    where
        T: Send + Sync + 'static,
    {
        self.state.insert(value);
    }

    pub fn error_handler<F>(&mut self, handler: F)
    where
        F: Fn(HttpError) -> Response + Send + Sync + 'static,
    {
        self.error_handler = Some(Arc::new(handler));
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

        println!("Server listening at http://{}", address);
        self.serve(listener).await;
    }

    pub async fn serve(self, listener: TcpListener) {
        let app = Arc::new(self);
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

                if let Err(err) = http1::Builder::new()
                    .serve_connection(io, service)
                    .with_upgrades()
                    .await
                {
                    eprintln!("Error serving connection: {:?}", err);
                }
            });
        }
    }

    /// Translates a hyper request into a [`Request`] (method, path, query,
    /// headers, size-bounded body), dispatches it, and converts the result.
    async fn handle(&self, mut req: hyper::Request<Incoming>) -> hyper::Response<ResponseBody> {
        // Read everything that only needs a borrow before consuming the body.
        let method = req.method().as_str().to_string();
        let path = req.uri().path().to_string();
        let raw_query = req.uri().query().map(|q| q.to_string());
        let query = raw_query.as_deref().map(parse_query).unwrap_or_default();
        let upgrade = if is_websocket_upgrade_request(&req) {
            Some(hyper::upgrade::on(&mut req))
        } else {
            None
        };
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
        let cookies = req
            .headers()
            .get(COOKIE)
            .and_then(|value| value.to_str().ok())
            .map(parse_cookies)
            .unwrap_or_default();

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
            raw_query,
            query,
            headers,
            cookies,
            body,
            params: HashMap::new(),
            state: self.state.clone(),
            upgrade,
        };

        self.dispatch(request).await.into_hyper()
    }

    /// Routes the request (capturing path params), then runs it through the
    /// middleware onion ending at the matched handler (or a 404 handler).
    pub(crate) async fn dispatch(&self, mut request: Request) -> Response {
        request.state = self.state.clone();
        let is_head = request.method == "HEAD";
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

        let mut response = match catch_unwind(AssertUnwindSafe(|| next(request))) {
            Ok(future) => match AssertUnwindSafe(future).catch_unwind().await {
                Ok(response) => response,
                Err(_) => panic_response(),
            },
            Err(_) => panic_response(),
        };
        if let Some(err) = response.take_error()
            && let Some(handler) = &self.error_handler
        {
            response = handler(err);
        }
        if is_head {
            response.clear_body();
        }
        response
    }
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}
