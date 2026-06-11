use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

use futures_util::FutureExt;
use http_body_util::{BodyExt, LengthLimitError, Limited};
use hyper::body::Incoming;
use hyper::header::{CONNECTION, COOKIE, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION, UPGRADE};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::{TcpListener, ToSocketAddrs};

use super::router::{MatchedRoute, RouteKind};
use super::websocket::{header_value_contains_token, is_valid_websocket_key};
use super::{
    ErrorHandler, HttpError, IntoHandler, IntoMiddleware, Middleware, Next, Request, Response,
    RouteHandle, Router, StateStore, WebSocketConfig, WebSocketObserver, WebSocketRuntimeHandle,
};
use super::{
    Handler, ResponseBody, allow_header_value, method_not_allowed_handler, not_found_handler,
    options_handler, panic_response, parse_cookies, parse_query,
};

/// Default maximum request body we will buffer into memory (64 KB).
const DEFAULT_MAX_BODY_BYTES: usize = 64 * 1024;

/// How request paths with a trailing slash (`/users/`) are treated relative
/// to the canonical, slash-less route (`/users`). The root path `/` is always
/// canonical.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum TrailingSlash {
    /// Trailing slashes are ignored: `/users/` matches `/users` (default).
    #[default]
    Ignore,
    /// Non-canonical paths do not match anything and fall through to 404.
    Strict,
    /// Non-canonical paths get a `308 Permanent Redirect` to the canonical
    /// path, preserving the query string.
    Redirect,
}

/// Server-wide limits and timeouts, configured via builder methods on [`App`].
#[derive(Clone, Copy)]
pub struct ServerConfig {
    pub(crate) max_body_size: usize,
    pub(crate) request_timeout: Option<Duration>,
    pub(crate) header_read_timeout: Option<Duration>,
    pub(crate) trailing_slash: TrailingSlash,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_body_size: DEFAULT_MAX_BODY_BYTES,
            request_timeout: None,
            header_read_timeout: None,
            trailing_slash: TrailingSlash::default(),
        }
    }
}

fn is_websocket_upgrade_request(req: &hyper::Request<Incoming>) -> bool {
    req.version() == hyper::Version::HTTP_11
        && req.method().as_str().eq_ignore_ascii_case("GET")
        && req.headers().get_all(UPGRADE).iter().any(|value| {
            value
                .to_str()
                .is_ok_and(|value| header_value_contains_token(value, "websocket"))
        })
        && req.headers().get_all(CONNECTION).iter().any(|value| {
            value
                .to_str()
                .is_ok_and(|value| header_value_contains_token(value, "upgrade"))
        })
        && req
            .headers()
            .get(SEC_WEBSOCKET_KEY)
            .and_then(|value| value.to_str().ok())
            .is_some_and(is_valid_websocket_key)
        && req
            .headers()
            .get(SEC_WEBSOCKET_VERSION)
            .and_then(|value| value.to_str().ok())
            == Some("13")
}

#[derive(Clone, Copy)]
pub(crate) enum TransportSecurity {
    Plain,
    Tls,
}

impl TransportSecurity {
    fn is_secure(self) -> bool {
        matches!(self, Self::Tls)
    }
}

pub struct App {
    router: Router,
    middlewares: Vec<Middleware>,
    state: StateStore,
    error_handler: Option<ErrorHandler>,
    pub(crate) config: ServerConfig,
    websocket_runtime: WebSocketRuntimeHandle,
    websocket_defaults: WebSocketConfig,
}

impl App {
    pub fn new() -> Self {
        Self {
            router: Router::new(),
            middlewares: Vec::new(),
            state: StateStore::default(),
            error_handler: None,
            config: ServerConfig::default(),
            websocket_runtime: WebSocketRuntimeHandle::local(),
            websocket_defaults: WebSocketConfig::new(),
        }
    }

    pub fn websocket_runtime(&self) -> WebSocketRuntimeHandle {
        self.websocket_runtime.clone()
    }

    pub fn websocket_defaults(&mut self, config: WebSocketConfig) -> &mut Self {
        self.websocket_defaults = config;
        self
    }

    pub fn websocket_observer(&mut self, observer: Arc<dyn WebSocketObserver>) -> &mut Self {
        self.websocket_runtime.set_observer(observer);
        self
    }

    pub(crate) fn validate_websockets(&self) -> io::Result<()> {
        self.router
            .validate_websockets(&self.websocket_defaults)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))
    }

    /// Sets the maximum request body size buffered into memory. Requests whose
    /// body exceeds this return `413 Payload Too Large`. Defaults to 64 KB.
    pub fn max_body_size(&mut self, bytes: usize) -> &mut Self {
        self.config.max_body_size = bytes;
        self
    }

    /// Sets a per-request timeout for handler execution. On timeout the client
    /// receives `408 Request Timeout`. Defaults to no timeout.
    pub fn request_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.config.request_timeout = Some(timeout);
        self
    }

    /// Sets how long a connection may take to send its request headers
    /// (slow-loris protection). Defaults to no timeout.
    pub fn header_read_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.config.header_read_timeout = Some(timeout);
        self
    }

    /// Sets the trailing-slash policy (default: [`TrailingSlash::Ignore`]).
    pub fn trailing_slash(&mut self, policy: TrailingSlash) -> &mut Self {
        self.config.trailing_slash = policy;
        self
    }

    /// Lists every registered route in registration order (see
    /// [`Router::routes`]).
    pub fn routes(&self) -> Vec<super::RouteInfo> {
        self.router.routes()
    }

    /// Prints the registered routes, one per line, method first.
    pub fn print_routes(&self) {
        for route in self.routes() {
            println!("{:<7} {}", route.method, route.path);
        }
    }

    /// Builds an OpenAPI 3.0 document (as JSON) describing the routes
    /// registered so far. See [`App::serve_docs`] for serving it.
    pub fn openapi(&self, title: &str, version: &str) -> serde_json::Value {
        super::openapi::build_document(title, version, &self.routes())
    }

    /// Registers `GET {prefix}/openapi.json` (the OpenAPI document) and
    /// `GET {prefix}` (Swagger UI reading it). The document is a snapshot of
    /// the routes registered so far — call this after registering them.
    pub fn serve_docs(&mut self, prefix: &str, title: &str, version: &str) {
        let prefix = format!("/{}", prefix.trim_matches('/'));
        let spec_url = format!("{}/openapi.json", prefix.trim_end_matches('/'));
        let document = self.openapi(title, version);
        let html = super::openapi::swagger_ui_html(title, &spec_url);

        self.get(&spec_url, move |_req: Request| Response::json(&document));
        self.get(&prefix, move |_req: Request| {
            Response::send(html.as_str()).content_type("text/html; charset=utf-8")
        });
    }

    pub fn get<H, M>(&mut self, path: &str, handler: H) -> RouteHandle<'_>
    where
        H: IntoHandler<M>,
    {
        self.router.get(path, handler)
    }

    // These delegate to the root router and are part of the public API even
    // when a given binary registers its routes through a Router instead.
    pub fn post<H, M>(&mut self, path: &str, handler: H) -> RouteHandle<'_>
    where
        H: IntoHandler<M>,
    {
        self.router.post(path, handler)
    }

    pub fn put<H, M>(&mut self, path: &str, handler: H) -> RouteHandle<'_>
    where
        H: IntoHandler<M>,
    {
        self.router.put(path, handler)
    }

    pub fn delete<H, M>(&mut self, path: &str, handler: H) -> RouteHandle<'_>
    where
        H: IntoHandler<M>,
    {
        self.router.delete(path, handler)
    }

    pub fn patch<H, M>(&mut self, path: &str, handler: H) -> RouteHandle<'_>
    where
        H: IntoHandler<M>,
    {
        self.router.patch(path, handler)
    }

    pub fn options<H, M>(&mut self, path: &str, handler: H) -> RouteHandle<'_>
    where
        H: IntoHandler<M>,
    {
        self.router.options(path, handler)
    }

    pub fn head<H, M>(&mut self, path: &str, handler: H) -> RouteHandle<'_>
    where
        H: IntoHandler<M>,
    {
        self.router.head(path, handler)
    }

    pub fn all<H, M>(&mut self, path: &str, handler: H) -> RouteHandle<'_>
    where
        H: IntoHandler<M>,
    {
        self.router.all(path, handler)
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

    /// Like [`App::websocket`], with subprotocols, message size limits, and
    /// keepalive pings from `config`.
    pub fn websocket_with<F, Fut>(&mut self, path: &str, config: super::WebSocketConfig, handler: F)
    where
        F: Fn(super::WebSocket) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        self.router.websocket_with(path, config, handler);
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

    /// Binds to `address` and serves connections until the process is killed.
    /// Returns an error only if binding fails; accept errors are non-fatal.
    pub async fn listen(self, address: impl ToSocketAddrs) -> io::Result<()> {
        self.validate_websockets()?;
        let listener = TcpListener::bind(address).await?;
        if let Ok(local) = listener.local_addr() {
            println!("Server listening at http://{}", local);
        }
        self.serve(listener).await
    }

    /// Binds to `address` and serves until `shutdown` resolves, then drains
    /// in-flight connections gracefully.
    pub async fn listen_with_shutdown(
        self,
        address: impl ToSocketAddrs,
        shutdown: impl Future<Output = ()> + Send,
    ) -> io::Result<()> {
        self.validate_websockets()?;
        let listener = TcpListener::bind(address).await?;
        if let Ok(local) = listener.local_addr() {
            println!("Server listening at http://{}", local);
        }
        self.serve_with_shutdown(listener, shutdown).await
    }

    /// Serves connections on `listener` until the process is killed.
    pub async fn serve(self, listener: TcpListener) -> io::Result<()> {
        self.serve_with_shutdown(listener, std::future::pending::<()>())
            .await
    }

    /// Serves connections on `listener` until `shutdown` resolves. A transient
    /// accept error is logged and retried — it never tears down the server.
    /// Once `shutdown` fires, the listener is closed and outstanding
    /// connections are drained (bounded by a 10s timeout).
    pub async fn serve_with_shutdown(
        self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()> + Send,
    ) -> io::Result<()> {
        self.validate_websockets()?;
        let header_read_timeout = self.config.header_read_timeout;
        let app = Arc::new(self);
        let mut builder = auto::Builder::new(TokioExecutor::new());
        if let Some(timeout) = header_read_timeout {
            builder
                .http1()
                .timer(TokioTimer::new())
                .header_read_timeout(timeout);
        }
        let graceful = GracefulShutdown::new();
        let mut shutdown = std::pin::pin!(shutdown);

        loop {
            let (stream, peer) = tokio::select! {
                accepted = listener.accept() => match accepted {
                    Ok(pair) => pair,
                    Err(err) => {
                        eprintln!("Error accepting connection: {}", err);
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        continue;
                    }
                },
                _ = &mut shutdown => break,
            };

            // Adapt the tokio stream to hyper's IO traits. `auto::Builder`
            // serves HTTP/1 and HTTP/2 and supports upgrades (WebSockets);
            // `into_owned` detaches the connection so it can outlive `builder`.
            let io = TokioIo::new(stream);
            let app = Arc::clone(&app);
            let connection = builder
                .serve_connection_with_upgrades(
                    io,
                    service_fn(move |req: hyper::Request<Incoming>| {
                        let app = Arc::clone(&app);
                        async move {
                            Ok::<_, Infallible>(
                                app.handle(req, Some(peer), TransportSecurity::Plain).await,
                            )
                        }
                    }),
                )
                .into_owned();

            // Serve each connection concurrently, watched for graceful drain.
            let watched = graceful.watch(connection);
            tokio::spawn(async move {
                if let Err(err) = watched.await {
                    eprintln!("Error serving connection: {:?}", err);
                }
            });
        }

        // Stop accepting new connections, then drain the in-flight ones.
        drop(listener);
        tokio::select! {
            _ = graceful.shutdown() => {}
            _ = tokio::time::sleep(Duration::from_secs(10)) => {
                eprintln!("Timed out waiting for in-flight connections to drain");
            }
        }
        Ok(())
    }

    /// Translates a hyper request into a [`Request`] (method, path, query,
    /// headers, size-bounded body), dispatches it, and converts the result.
    pub(crate) async fn handle(
        &self,
        mut req: hyper::Request<Incoming>,
        remote_addr: Option<SocketAddr>,
        transport_security: TransportSecurity,
    ) -> hyper::Response<ResponseBody> {
        // Read everything that only needs a borrow before consuming the body.
        let version = req.version();
        let method = req.method().as_str().to_string();
        let path = req.uri().path().to_string();
        let raw_query = req.uri().query().map(|q| q.to_string());
        let query = raw_query.as_deref().map(parse_query).unwrap_or_default();
        let upgrade = if is_websocket_upgrade_request(&req) {
            Some(hyper::upgrade::on(&mut req))
        } else {
            None
        };
        // Build a convenience single-value map (last value wins) and a
        // full-fidelity list that preserves duplicate headers.
        let mut headers: HashMap<String, String> = HashMap::new();
        let mut header_pairs: Vec<(String, String)> = Vec::new();
        for (name, value) in req.headers().iter() {
            let name = name.as_str().to_string();
            let value = value.to_str().unwrap_or("").to_string();
            headers.insert(name.clone(), value.clone());
            header_pairs.push((name, value));
        }
        let cookies = req
            .headers()
            .get(COOKIE)
            .and_then(|value| value.to_str().ok())
            .map(parse_cookies)
            .unwrap_or_default();

        // Buffer the body up to the configured limit. On overflow return 413;
        // on any other read error return 400 (no longer a silent empty body).
        let body = match Limited::new(req.into_body(), self.config.max_body_size)
            .collect()
            .await
        {
            Ok(collected) => collected.to_bytes(),
            Err(err) => {
                let error = if err.downcast_ref::<LengthLimitError>().is_some() {
                    HttpError::new(413, "Payload Too Large")
                } else {
                    HttpError::bad_request("Could not read request body")
                };
                return self.error_response(error).into_hyper();
            }
        };

        let request = Request {
            version,
            method,
            path,
            raw_query,
            query,
            headers,
            cookies,
            body,
            params: HashMap::new(),
            route_pattern: None,
            websocket_runtime: self.websocket_runtime.clone(),
            resolved_websocket_config: None,
            state: self.state.clone(),
            upgrade,
            remote_addr,
            secure_transport: transport_security.is_secure(),
            header_pairs,
        };

        self.run_request(request).await.into_hyper()
    }

    /// Runs a request through dispatch, applying the configured per-request
    /// timeout (408 on expiry). Shared by the real server and the test client.
    pub(crate) async fn run_request(&self, request: Request) -> Response {
        match self.config.request_timeout {
            Some(timeout) => match tokio::time::timeout(timeout, self.dispatch(request)).await {
                Ok(response) => response,
                Err(_) => self.error_response(HttpError::new(408, "Request Timeout")),
            },
            None => self.dispatch(request).await,
        }
    }

    /// Builds a response for an error, routing it through the registered
    /// `error_handler` if one is set, otherwise a default plain-text response.
    pub(crate) fn error_response(&self, error: HttpError) -> Response {
        match &self.error_handler {
            Some(handler) => handler(error),
            None => Response::from_error(error),
        }
    }

    /// Resolves a request that did not directly match a route: auto-serves
    /// HEAD from a matching GET, auto-answers OPTIONS with `Allow`, returns 405
    /// when the path exists for other methods, or falls through to 404.
    fn resolve_miss(&self, method: &str, path: &str) -> MatchedRoute {
        let allowed = self.router.allowed_methods(path);
        if allowed.is_empty() {
            MatchedRoute {
                handler: not_found_handler(),
                middlewares: Vec::new(),
                params: HashMap::new(),
                pattern: path.to_string(),
                kind: RouteKind::Http,
            }
        } else if method == "HEAD" && allowed.iter().any(|m| m == "GET") {
            self.router
                .route("GET", path)
                .expect("GET route present per allowed_methods")
        } else if method == "OPTIONS" {
            MatchedRoute {
                handler: options_handler(allow_header_value(&allowed)),
                middlewares: Vec::new(),
                params: HashMap::new(),
                pattern: path.to_string(),
                kind: RouteKind::Http,
            }
        } else {
            MatchedRoute {
                handler: method_not_allowed_handler(allow_header_value(&allowed)),
                middlewares: Vec::new(),
                params: HashMap::new(),
                pattern: path.to_string(),
                kind: RouteKind::Http,
            }
        }
    }

    /// Applies the trailing-slash policy to a non-canonical request path,
    /// returning the substitute handler (404 or 308 redirect) that should run
    /// through the middleware onion instead of route lookup.
    fn trailing_slash_miss(&self, request: &Request) -> Option<Handler> {
        if request.path.len() <= 1 || !request.path.ends_with('/') {
            return None;
        }
        match self.config.trailing_slash {
            TrailingSlash::Ignore => None,
            TrailingSlash::Strict => Some(not_found_handler()),
            TrailingSlash::Redirect => {
                let mut location = request.path.trim_end_matches('/').to_string();
                if location.is_empty() {
                    location.push('/');
                }
                if let Some(query) = &request.raw_query {
                    location.push('?');
                    location.push_str(query);
                }
                Some(Arc::new(move |_req| {
                    let location = location.clone();
                    Box::pin(async move { Response::redirect_with_status(&location, 308) })
                }))
            }
        }
    }

    /// Routes the request (capturing path params), then runs it through the
    /// middleware onion ending at the matched handler (or a 404 handler).
    pub(crate) async fn dispatch(&self, mut request: Request) -> Response {
        request.state = self.state.clone();
        request.websocket_runtime = self.websocket_runtime.clone();
        let is_head = request.method == "HEAD";
        let matched = match self.trailing_slash_miss(&request) {
            Some(handler) => MatchedRoute {
                handler,
                middlewares: Vec::new(),
                params: HashMap::new(),
                pattern: request.path.clone(),
                kind: RouteKind::Http,
            },
            None => match self.router.route(&request.method, &request.path) {
                Some(found) => found,
                None => self.resolve_miss(&request.method, &request.path),
            },
        };
        let MatchedRoute {
            handler,
            middlewares: route_middlewares,
            params,
            pattern,
            kind,
        } = matched;
        request.params = params;
        request.route_pattern = Some(pattern);
        request.resolved_websocket_config = match kind {
            RouteKind::Http => None,
            RouteKind::WebSocket(route_config) => {
                Some(super::websocket::ResolvedWebSocketConfig::from_layers(
                    &self.websocket_defaults,
                    &route_config,
                ))
            }
        };

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
