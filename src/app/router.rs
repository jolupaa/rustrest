use std::collections::HashMap;
use std::future::Future;
use std::path::{Component, Path as FsPath, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use hyper::body::Bytes;

use super::decode_component;
use super::{
    Handler, HttpError, IntoHandler, IntoMiddleware, Middleware, Next, Request, Response,
    WebSocket, WebSocketHandler,
};

pub(crate) const METHOD_ALL: &str = "*";

/// A single segment of a route pattern.
#[derive(Clone)]
pub(crate) enum Segment {
    Static(String),
    /// A `:name` placeholder, storing `name` (without the colon).
    Param(String),
    /// A trailing `*name` placeholder, capturing the rest of the path.
    Wildcard(String),
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
pub(crate) fn path_segments(path: &str) -> Vec<&str> {
    path.split('/').filter(|s| !s.is_empty()).collect()
}

/// Parses a route pattern like `/users/:id` into segments.
pub(crate) fn parse_pattern(path: &str) -> Vec<Segment> {
    path_segments(path)
        .into_iter()
        .map(|s| match s.strip_prefix(':') {
            Some(name) => Segment::Param(name.to_string()),
            None => match s.strip_prefix('*') {
                Some("") => Segment::Wildcard("path".to_string()),
                Some(name) => Segment::Wildcard(name.to_string()),
                None => Segment::Static(s.to_string()),
            },
        })
        .collect()
}

/// Matches a parsed pattern against concrete path segments, capturing params.
/// Returns `None` if the pattern does not match.
pub(crate) fn match_pattern(
    pattern: &[Segment],
    segments: &[&str],
) -> Option<HashMap<String, String>> {
    let mut params = HashMap::new();
    let mut index = 0;
    for (pattern_index, seg) in pattern.iter().enumerate() {
        if let Segment::Wildcard(name) = seg {
            if pattern_index != pattern.len() - 1 {
                return None;
            }
            params.insert(name.clone(), segments[index..].join("/"));
            return Some(params);
        }

        let actual = segments.get(index)?;
        match seg {
            Segment::Static(s) if s == *actual => {}
            Segment::Static(_) => return None,
            Segment::Param(name) => {
                params.insert(name.clone(), (*actual).to_string());
            }
            Segment::Wildcard(_) => unreachable!("wildcards are handled before segment matching"),
        }
        index += 1;
    }

    if index != segments.len() {
        return None;
    }
    Some(params)
}

/// A collection of routes that can be defined independently (e.g. in its own
/// module/file) and mounted onto an [`App`](super::App) or another `Router`
/// under a prefix.
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

    pub fn get<H, M>(&mut self, path: &str, handler: H) -> RouteHandle<'_>
    where
        H: IntoHandler<M>,
    {
        self.add("GET", path, handler)
    }

    pub fn post<H, M>(&mut self, path: &str, handler: H) -> RouteHandle<'_>
    where
        H: IntoHandler<M>,
    {
        self.add("POST", path, handler)
    }

    pub fn put<H, M>(&mut self, path: &str, handler: H) -> RouteHandle<'_>
    where
        H: IntoHandler<M>,
    {
        self.add("PUT", path, handler)
    }

    pub fn delete<H, M>(&mut self, path: &str, handler: H) -> RouteHandle<'_>
    where
        H: IntoHandler<M>,
    {
        self.add("DELETE", path, handler)
    }

    pub fn patch<H, M>(&mut self, path: &str, handler: H) -> RouteHandle<'_>
    where
        H: IntoHandler<M>,
    {
        self.add("PATCH", path, handler)
    }

    pub fn options<H, M>(&mut self, path: &str, handler: H) -> RouteHandle<'_>
    where
        H: IntoHandler<M>,
    {
        self.add("OPTIONS", path, handler)
    }

    pub fn head<H, M>(&mut self, path: &str, handler: H) -> RouteHandle<'_>
    where
        H: IntoHandler<M>,
    {
        self.add("HEAD", path, handler)
    }

    pub fn all<H, M>(&mut self, path: &str, handler: H) -> RouteHandle<'_>
    where
        H: IntoHandler<M>,
    {
        self.add(METHOD_ALL, path, handler)
    }

    pub fn websocket<F, Fut>(&mut self, path: &str, handler: F)
    where
        F: Fn(WebSocket) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handler: WebSocketHandler = Arc::new(move |socket| Box::pin(handler(socket)));
        self.get(path, move |req: Request| {
            let handler = Arc::clone(&handler);
            req.websocket(handler)
        });
    }

    pub fn ws<F, Fut>(&mut self, path: &str, handler: F)
    where
        F: Fn(WebSocket) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.websocket(path, handler);
    }

    /// Adds a middleware scoped to this router: it wraps every route in this
    /// router (and routers mounted into it), and nothing else. Applied when
    /// the router is mounted.
    pub fn layer<MW: IntoMiddleware>(&mut self, middleware: MW) {
        self.middlewares.push(middleware.into_middleware());
    }

    pub fn guard<G>(&mut self, guard: G)
    where
        G: Fn(&Request) -> bool + Send + Sync + 'static,
    {
        let guard = Arc::new(guard);
        self.layer(move |req: Request, next: Next| {
            let guard = Arc::clone(&guard);
            async move {
                if guard(&req) {
                    next(req).await
                } else {
                    Response::from_error(HttpError::forbidden("Access denied"))
                }
            }
        });
    }

    pub fn fallback<H, M>(&mut self, handler: H)
    where
        H: IntoHandler<M>,
    {
        self.add(METHOD_ALL, "/*path", handler);
    }

    pub fn static_files<P>(&mut self, prefix: &str, root: P)
    where
        P: Into<PathBuf>,
    {
        let root = Arc::new(root.into());
        let pattern = join_paths(prefix, "/*path");
        self.add_static_route("GET", &pattern, Arc::clone(&root));
        self.add_static_route("HEAD", &pattern, root);
    }

    fn add<H, M>(&mut self, method: &str, path: &str, handler: H) -> RouteHandle<'_>
    where
        H: IntoHandler<M>,
    {
        self.routes.push(Route {
            method: method.to_string(),
            pattern: parse_pattern(path),
            handler: handler.into_handler(),
            middlewares: Vec::new(),
        });
        let index = self.routes.len() - 1;
        RouteHandle {
            router: self,
            index,
        }
    }

    fn add_static_route(&mut self, method: &str, path: &str, root: Arc<PathBuf>) {
        let handler: Handler = Arc::new(
            move |req| -> Pin<Box<dyn Future<Output = Response> + Send>> {
                let root = Arc::clone(&root);
                Box::pin(async move { serve_static_file(root, req).await })
            },
        );

        self.routes.push(Route {
            method: method.to_string(),
            pattern: parse_pattern(path),
            handler,
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
    pub(crate) fn route(
        &self,
        method: &str,
        path: &str,
    ) -> Option<(Handler, Vec<Middleware>, HashMap<String, String>)> {
        let segments = path_segments(path);
        for route in &self.routes {
            if route.method != METHOD_ALL && route.method != method {
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

    /// Returns the distinct concrete methods registered for routes whose
    /// pattern matches `path` (ignoring the request method). Used to build the
    /// `Allow` header for 405/OPTIONS responses. `*` (catch-all) is excluded.
    pub(crate) fn allowed_methods(&self, path: &str) -> Vec<String> {
        let segments = path_segments(path);
        let mut methods = Vec::new();
        for route in &self.routes {
            if route.method == METHOD_ALL {
                continue;
            }
            if match_pattern(&route.pattern, &segments).is_some()
                && !methods.contains(&route.method)
            {
                methods.push(route.method.clone());
            }
        }
        methods
    }
}

/// Builds an `Allow` header value from the matched methods, implicitly adding
/// `HEAD` (when `GET` is present) and `OPTIONS`, both of which the server
/// answers automatically.
pub(crate) fn allow_header_value(allowed: &[String]) -> String {
    let mut methods = allowed.to_vec();
    if methods.iter().any(|m| m == "GET") && !methods.iter().any(|m| m == "HEAD") {
        methods.push("HEAD".to_string());
    }
    if !methods.iter().any(|m| m == "OPTIONS") {
        methods.push("OPTIONS".to_string());
    }
    methods.join(", ")
}

/// A handle to a just-registered route, returned by the route methods so that
/// per-route middleware can be attached: `app.get("/admin", h).layer(auth)`.
/// The handle is ignorable when no per-route middleware is needed.
pub struct RouteHandle<'a> {
    router: &'a mut Router,
    index: usize,
}

impl RouteHandle<'_> {
    /// Adds a middleware that wraps only this route. Repeated calls stack, with
    /// the first-added middleware outermost.
    pub fn layer<MW: IntoMiddleware>(self, middleware: MW) -> Self {
        self.router.routes[self.index]
            .middlewares
            .push(middleware.into_middleware());
        self
    }
}

fn join_paths(prefix: &str, suffix: &str) -> String {
    let prefix = prefix.trim_end_matches('/');
    let suffix = suffix.trim_start_matches('/');

    match (prefix.is_empty(), suffix.is_empty()) {
        (true, true) => "/".to_string(),
        (true, false) => format!("/{}", suffix),
        (false, true) => prefix.to_string(),
        (false, false) => format!("{}/{}", prefix, suffix),
    }
}

async fn serve_static_file(root: Arc<PathBuf>, req: Request) -> Response {
    let Some(mut file_path) = safe_static_path(&root, req.param("path").unwrap_or("")) else {
        return Response::bad_request();
    };

    if matches!(tokio::fs::metadata(&file_path).await, Ok(metadata) if metadata.is_dir()) {
        file_path.push("index.html");
    }

    match tokio::fs::read(&file_path).await {
        Ok(bytes) => Response::bytes(Bytes::from(bytes), content_type_for_path(&file_path)),
        Err(_) => Response::not_found(),
    }
}

fn safe_static_path(root: &FsPath, requested: &str) -> Option<PathBuf> {
    let decoded = decode_component(requested, false);
    let relative = if decoded.is_empty() {
        "index.html"
    } else {
        decoded.as_str()
    };

    let mut path = root.to_path_buf();
    for component in FsPath::new(relative).components() {
        match component {
            Component::Normal(part) => path.push(part),
            Component::CurDir => {}
            Component::Prefix(_) | Component::RootDir | Component::ParentDir => return None,
        }
    }

    Some(path)
}

fn content_type_for_path(path: &FsPath) -> &'static str {
    match path.extension().and_then(|ext| ext.to_str()).unwrap_or("") {
        "html" | "htm" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "json" => "application/json",
        "txt" => "text/plain; charset=utf-8",
        "csv" => "text/csv; charset=utf-8",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "ico" => "image/x-icon",
        "wasm" => "application/wasm",
        "pdf" => "application/pdf",
        _ => "application/octet-stream",
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}
