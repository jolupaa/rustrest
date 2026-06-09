use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::io;
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

use super::{
    ErrorHandler, HttpError, IntoHandler, IntoMiddleware, Middleware, Next, Request, Response,
    Router, StateStore,
};
use super::{ResponseBody, not_found_handler, panic_response, parse_cookies, parse_query};

/// Default maximum request body we will buffer into memory (64 KB).
const DEFAULT_MAX_BODY_BYTES: usize = 64 * 1024;

/// Server-wide limits and timeouts, configured via builder methods on [`App`].
#[derive(Clone, Copy)]
pub struct ServerConfig {
    max_body_size: usize,
    request_timeout: Option<Duration>,
    header_read_timeout: Option<Duration>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_body_size: DEFAULT_MAX_BODY_BYTES,
            request_timeout: None,
            header_read_timeout: None,
        }
    }
}

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
    config: ServerConfig,
}

impl App {
    pub fn new() -> Self {
        Self {
            router: Router::new(),
            middlewares: Vec::new(),
            state: StateStore::default(),
            error_handler: None,
            config: ServerConfig::default(),
        }
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

    /// Binds to `address` and serves connections until the process is killed.
    /// Returns an error only if binding fails; accept errors are non-fatal.
    pub async fn listen(self, address: impl ToSocketAddrs) -> io::Result<()> {
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
            let stream = tokio::select! {
                accepted = listener.accept() => match accepted {
                    Ok((stream, _peer)) => stream,
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
                        async move { Ok::<_, Infallible>(app.handle(req).await) }
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

        // Apply the per-request timeout (if configured) around the handler.
        let response = match self.config.request_timeout {
            Some(timeout) => match tokio::time::timeout(timeout, self.dispatch(request)).await {
                Ok(response) => response,
                Err(_) => self.error_response(HttpError::new(408, "Request Timeout")),
            },
            None => self.dispatch(request).await,
        };

        response.into_hyper()
    }

    /// Builds a response for an error, routing it through the registered
    /// `error_handler` if one is set, otherwise a default plain-text response.
    fn error_response(&self, error: HttpError) -> Response {
        match &self.error_handler {
            Some(handler) => handler(error),
            None => Response::from_error(error),
        }
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
