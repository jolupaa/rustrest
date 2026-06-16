use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::io::SeekFrom;
use std::path::{Component, Path as FsPath, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, UNIX_EPOCH};

use futures_util::Stream;
use hyper::body::Bytes;
use hyper::{Method, StatusCode};
use percent_encoding::percent_decode_str;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use super::trie::RouteIndex;
use super::websocket::ResolvedWebSocketConfig;
use super::{
    Handler, HttpError, IntoHandler, IntoMiddleware, IntoWebSocketHandler, IntoWebSocketOutput,
    Middleware, Next, Request, Response, WebSocket, WebSocketConfig, WsError,
};

pub(crate) const METHOD_ALL: &str = "*";

/// A single segment of a route pattern.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Segment {
    Static(String),
    /// A `:name` placeholder, storing `name` (without the colon).
    Param(String),
    /// A trailing `*name` placeholder, capturing the rest of the path.
    Wildcard(String),
}

/// A validated, canonical route pattern such as `/users/:id`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RoutePattern {
    rendered: String,
    segments: Vec<Segment>,
    parameter_names: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteErrorKind {
    MissingLeadingSlash,
    EmptyParameter,
    DuplicateParameter,
    NonTerminalWildcard,
    DuplicateRoute,
    ConflictingPattern,
    DuplicateName,
    InvalidHostPattern,
    MissingUrlParameter,
    UnexpectedUrlParameter,
    InvalidPercentEncoding,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteError {
    kind: RouteErrorKind,
    message: String,
}

impl RouteError {
    fn new(kind: RouteErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn kind(&self) -> RouteErrorKind {
        self.kind
    }
}

impl Display for RouteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for RouteError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteMatchErrorKind {
    InvalidPathEncoding,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteMatchError {
    kind: RouteMatchErrorKind,
    message: String,
}

impl RouteMatchError {
    pub fn invalid_path_encoding() -> Self {
        Self {
            kind: RouteMatchErrorKind::InvalidPathEncoding,
            message: "La ruta contiene codificacion porcentual invalida".to_string(),
        }
    }

    pub fn kind(&self) -> RouteMatchErrorKind {
        self.kind
    }
}

impl Display for RouteMatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for RouteMatchError {}

impl From<RouteMatchError> for HttpError {
    fn from(error: RouteMatchError) -> Self {
        match error.kind {
            RouteMatchErrorKind::InvalidPathEncoding => HttpError::new(
                StatusCode::BAD_REQUEST,
                "invalid_path_encoding",
                "La ruta contiene codificacion porcentual invalida",
            )
            .with_source(error),
        }
    }
}

impl RoutePattern {
    pub fn parse(path: &str) -> Result<Self, RouteError> {
        if !path.starts_with('/') {
            return Err(RouteError::new(
                RouteErrorKind::MissingLeadingSlash,
                "Las rutas deben empezar con /",
            ));
        }

        let raw_segments: Vec<&str> = path_segments(path);
        let mut segments = Vec::with_capacity(raw_segments.len());
        let mut parameter_names = Vec::new();
        let mut seen = HashSet::new();

        for (index, raw) in raw_segments.iter().enumerate() {
            if let Some(name) = raw.strip_prefix(':') {
                validate_parameter_name(name, &mut seen, &mut parameter_names)?;
                segments.push(Segment::Param(name.to_string()));
            } else if let Some(name) = raw.strip_prefix('*') {
                if index != raw_segments.len() - 1 {
                    return Err(RouteError::new(
                        RouteErrorKind::NonTerminalWildcard,
                        "Los comodines solo pueden aparecer al final de la ruta",
                    ));
                }
                validate_parameter_name(name, &mut seen, &mut parameter_names)?;
                segments.push(Segment::Wildcard(name.to_string()));
            } else {
                segments.push(Segment::Static((*raw).to_string()));
            }
        }

        Ok(Self::from_validated_segments(segments, parameter_names))
    }

    fn from_validated_segments(segments: Vec<Segment>, parameter_names: Vec<String>) -> Self {
        let rendered = render_pattern(&segments);
        Self {
            rendered,
            segments,
            parameter_names,
        }
    }

    fn join(prefix: &Self, suffix: &Self) -> Result<Self, RouteError> {
        let mut segments = prefix.segments.clone();
        segments.extend(suffix.segments.clone());
        validate_segments(segments)
    }

    pub fn as_str(&self) -> &str {
        &self.rendered
    }

    fn segments(&self) -> &[Segment] {
        &self.segments
    }

    fn parameter_names(&self) -> &[String] {
        &self.parameter_names
    }

    fn conflict_key(&self) -> String {
        if self.segments.is_empty() {
            return "/".to_string();
        }

        let mut out = String::new();
        for segment in &self.segments {
            out.push('/');
            match segment {
                Segment::Static(value) => out.push_str(value),
                Segment::Param(_) => out.push(':'),
                Segment::Wildcard(_) => out.push('*'),
            }
        }
        out
    }
}

fn validate_parameter_name(
    name: &str,
    seen: &mut HashSet<String>,
    parameter_names: &mut Vec<String>,
) -> Result<(), RouteError> {
    if name.is_empty() {
        return Err(RouteError::new(
            RouteErrorKind::EmptyParameter,
            "Los parametros de ruta no pueden estar vacios",
        ));
    }
    if !seen.insert(name.to_string()) {
        return Err(RouteError::new(
            RouteErrorKind::DuplicateParameter,
            format!("Parametro duplicado: {name}"),
        ));
    }
    parameter_names.push(name.to_string());
    Ok(())
}

fn validate_segments(segments: Vec<Segment>) -> Result<RoutePattern, RouteError> {
    let mut parameter_names = Vec::new();
    let mut seen = HashSet::new();
    let last = segments.len().saturating_sub(1);
    for (index, segment) in segments.iter().enumerate() {
        match segment {
            Segment::Param(name) => {
                validate_parameter_name(name, &mut seen, &mut parameter_names)?;
            }
            Segment::Wildcard(name) => {
                if index != last {
                    return Err(RouteError::new(
                        RouteErrorKind::NonTerminalWildcard,
                        "Los comodines solo pueden aparecer al final de la ruta",
                    ));
                }
                validate_parameter_name(name, &mut seen, &mut parameter_names)?;
            }
            Segment::Static(_) => {}
        }
    }
    Ok(RoutePattern::from_validated_segments(
        segments,
        parameter_names,
    ))
}

/// A registered route: method, parsed path pattern, handler, and the
/// middleware chain (outermost-first) accumulated from the routers it was
/// mounted through.
struct Route {
    method: String,
    pattern: RoutePattern,
    handler: Handler,
    middlewares: Vec<Middleware>,
    kind: RouteKind,
    meta: RouteMeta,
    name: Option<String>,
    host: Option<HostPattern>,
}

#[derive(Clone)]
pub(crate) enum RouteKind {
    Http,
    WebSocket(Box<WebSocketConfig>),
}

pub struct MatchedRoute {
    pub(crate) handler: Handler,
    pub(crate) middlewares: Vec<Middleware>,
    pub params: HashMap<String, String>,
    pub pattern: String,
    pub(crate) kind: RouteKind,
    pub(crate) body_limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostPattern {
    Exact(String),
    WildcardSubdomain(String),
}

impl HostPattern {
    pub fn parse(pattern: &str) -> Result<Self, RouteError> {
        let host = normalize_host(pattern).ok_or_else(|| {
            RouteError::new(
                RouteErrorKind::InvalidHostPattern,
                "El patron de host no es valido",
            )
        })?;

        if let Some(suffix) = host.strip_prefix("*.") {
            if suffix.is_empty() || suffix.contains('*') {
                return Err(RouteError::new(
                    RouteErrorKind::InvalidHostPattern,
                    "El comodin de host debe tener un sufijo valido",
                ));
            }
            Ok(Self::WildcardSubdomain(suffix.to_string()))
        } else if host.contains('*') {
            Err(RouteError::new(
                RouteErrorKind::InvalidHostPattern,
                "El comodin de host solo puede aparecer como prefijo *.",
            ))
        } else {
            Ok(Self::Exact(host))
        }
    }

    fn matches_request(&self, host: Option<&str>) -> bool {
        let Some(host) = host.and_then(normalize_host) else {
            return false;
        };
        self.matches_normalized(&host)
    }

    fn matches_normalized(&self, host: &str) -> bool {
        match self {
            Self::Exact(expected) => expected == host,
            Self::WildcardSubdomain(suffix) => {
                host.len() > suffix.len() + 1
                    && host.ends_with(suffix)
                    && host.as_bytes()[host.len() - suffix.len() - 1] == b'.'
            }
        }
    }
}

fn normalize_host(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.contains('@')
        || trimmed.contains('/')
        || trimmed.contains('\\')
        || trimmed
            .chars()
            .any(|ch| ch.is_control() || ch.is_whitespace())
    {
        return None;
    }

    let without_port = strip_valid_port(trimmed)?;
    if without_port.is_empty() {
        return None;
    }
    Some(without_port.to_ascii_lowercase())
}

fn strip_valid_port(host: &str) -> Option<&str> {
    if host.starts_with('[') {
        let end = host.find(']')?;
        let after = &host[end + 1..];
        if after.is_empty() {
            return Some(host);
        }
        let port = after.strip_prefix(':')?;
        if port.is_empty() || !port.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        return Some(&host[..=end]);
    }

    match host.rsplit_once(':') {
        Some((name, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => {
            Some(name)
        }
        Some(_) if host.matches(':').count() == 1 => None,
        _ => Some(host),
    }
}

/// Metadata and transport policy attached to a route via [`RouteHandle`].
/// Documentation fields are surfaced in [`RouteInfo`] and OpenAPI.
#[derive(Clone, Default)]
pub(crate) struct RouteMeta {
    summary: Option<String>,
    description: Option<String>,
    tags: Vec<String>,
    body_limit: Option<usize>,
}

/// Splits a path into non-empty segments (trailing/duplicate slashes ignored).
pub(crate) fn path_segments(path: &str) -> Vec<&str> {
    path.split('/').filter(|s| !s.is_empty()).collect()
}

/// Parses a route pattern like `/users/:id` into segments.
pub(crate) fn parse_pattern(path: &str) -> Vec<Segment> {
    RoutePattern::parse(path)
        .expect("valid route pattern")
        .segments
}

/// Matches a parsed pattern against concrete path segments, capturing params.
/// Returns `None` if the pattern does not match.
pub(crate) fn match_pattern(
    pattern: &[Segment],
    segments: &[&str],
) -> Option<HashMap<String, String>> {
    match_pattern_strict(pattern, segments).ok().flatten()
}

fn match_pattern_strict(
    pattern: &[Segment],
    segments: &[&str],
) -> Result<Option<HashMap<String, String>>, RouteMatchError> {
    let mut params = HashMap::new();
    let mut index = 0;
    for (pattern_index, seg) in pattern.iter().enumerate() {
        if let Segment::Wildcard(name) = seg {
            if pattern_index != pattern.len() - 1 {
                return Ok(None);
            }
            let mut decoded = Vec::new();
            for segment in &segments[index..] {
                decoded.push(decode_path_segment(segment)?);
            }
            params.insert(name.clone(), decoded.join("/"));
            return Ok(Some(params));
        }

        let Some(actual) = segments.get(index) else {
            return Ok(None);
        };
        match seg {
            Segment::Static(s) if s == *actual => {}
            Segment::Static(_) => return Ok(None),
            Segment::Param(name) => {
                params.insert(name.clone(), decode_path_segment(actual)?);
            }
            Segment::Wildcard(_) => unreachable!("wildcards are handled before segment matching"),
        }
        index += 1;
    }

    if index != segments.len() {
        return Ok(None);
    }
    Ok(Some(params))
}

fn decode_path_segment(segment: &str) -> Result<String, RouteMatchError> {
    validate_percent_triplets(segment)?;
    percent_decode_str(segment)
        .decode_utf8()
        .map(|value| value.into_owned())
        .map_err(|_| RouteMatchError::invalid_path_encoding())
}

fn validate_percent_triplets(input: &str) -> Result<(), RouteMatchError> {
    let bytes = input.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit()
            {
                return Err(RouteMatchError::invalid_path_encoding());
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    Ok(())
}

/// A collection of routes that can be defined independently (e.g. in its own
/// module/file) and mounted onto an [`App`](super::App) or another `Router`
/// under a prefix.
pub struct Router {
    routes: Vec<Route>,
    middlewares: Vec<Middleware>,
    /// Trie index over `routes`, built lazily on first lookup and dropped on
    /// every mutation (all mutations require `&mut self`, so a stale index can
    /// never be observed).
    index: OnceLock<RouteIndex>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            routes: Vec::new(),
            middlewares: Vec::new(),
            index: OnceLock::new(),
        }
    }

    fn index(&self) -> &RouteIndex {
        self.index.get_or_init(|| {
            RouteIndex::build(
                self.routes
                    .iter()
                    .map(|route| (route.method.as_str(), route.pattern.segments())),
            )
        })
    }

    pub fn route<H, M>(
        &mut self,
        method: Method,
        path: &str,
        handler: H,
    ) -> Result<RouteHandle<'_>, RouteError>
    where
        H: IntoHandler<M>,
    {
        self.add(method.as_str(), path, handler)
    }

    pub fn get<H, M>(&mut self, path: &str, handler: H) -> Result<RouteHandle<'_>, RouteError>
    where
        H: IntoHandler<M>,
    {
        self.add("GET", path, handler)
    }

    pub fn post<H, M>(&mut self, path: &str, handler: H) -> Result<RouteHandle<'_>, RouteError>
    where
        H: IntoHandler<M>,
    {
        self.add("POST", path, handler)
    }

    pub fn put<H, M>(&mut self, path: &str, handler: H) -> Result<RouteHandle<'_>, RouteError>
    where
        H: IntoHandler<M>,
    {
        self.add("PUT", path, handler)
    }

    pub fn delete<H, M>(&mut self, path: &str, handler: H) -> Result<RouteHandle<'_>, RouteError>
    where
        H: IntoHandler<M>,
    {
        self.add("DELETE", path, handler)
    }

    pub fn patch<H, M>(&mut self, path: &str, handler: H) -> Result<RouteHandle<'_>, RouteError>
    where
        H: IntoHandler<M>,
    {
        self.add("PATCH", path, handler)
    }

    pub fn options<H, M>(&mut self, path: &str, handler: H) -> Result<RouteHandle<'_>, RouteError>
    where
        H: IntoHandler<M>,
    {
        self.add("OPTIONS", path, handler)
    }

    pub fn head<H, M>(&mut self, path: &str, handler: H) -> Result<RouteHandle<'_>, RouteError>
    where
        H: IntoHandler<M>,
    {
        self.add("HEAD", path, handler)
    }

    pub fn all<H, M>(&mut self, path: &str, handler: H) -> Result<RouteHandle<'_>, RouteError>
    where
        H: IntoHandler<M>,
    {
        self.add(METHOD_ALL, path, handler)
    }

    pub fn websocket<F, Fut, O>(
        &mut self,
        path: &str,
        handler: F,
    ) -> Result<RouteHandle<'_>, RouteError>
    where
        F: Fn(WebSocket) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = O> + Send + 'static,
        O: IntoWebSocketOutput + Send + 'static,
    {
        self.websocket_with(path, WebSocketConfig::new(), handler)
    }

    pub fn ws<F, Fut, O>(&mut self, path: &str, handler: F) -> Result<RouteHandle<'_>, RouteError>
    where
        F: Fn(WebSocket) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = O> + Send + 'static,
        O: IntoWebSocketOutput + Send + 'static,
    {
        self.websocket(path, handler)
    }

    /// Like [`Router::websocket`], with subprotocols, message size limits,
    /// and keepalive pings from `config`.
    pub fn websocket_with<F, Fut, O>(
        &mut self,
        path: &str,
        config: WebSocketConfig,
        handler: F,
    ) -> Result<RouteHandle<'_>, RouteError>
    where
        F: Fn(WebSocket) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = O> + Send + 'static,
        O: IntoWebSocketOutput + Send + 'static,
    {
        let handler = handler.into_normalized_websocket_handler();
        let kind = RouteKind::WebSocket(Box::new(config.clone()));
        self.add_with_kind(
            "GET",
            path,
            move |req: Request| {
                let handler = Arc::clone(&handler);
                req.websocket_with_normalized(config.clone(), handler)
            },
            kind,
        )
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

    pub fn fallback<H, M>(&mut self, handler: H) -> Result<RouteHandle<'_>, RouteError>
    where
        H: IntoHandler<M>,
    {
        self.add(METHOD_ALL, "/*path", handler)
    }

    pub fn static_files<P>(&mut self, prefix: &str, root: P) -> Result<(), RouteError>
    where
        P: Into<PathBuf>,
    {
        let root = Arc::new(root.into());
        let pattern = join_paths(prefix, "/*path");
        let get_pattern = RoutePattern::parse(&pattern)?;
        let head_pattern = get_pattern.clone();

        self.validate_route_insert("GET", &get_pattern, None, None)?;
        self.validate_route_insert("HEAD", &head_pattern, None, None)?;

        self.routes
            .push(static_route("GET", get_pattern, Arc::clone(&root)));
        self.routes.push(static_route("HEAD", head_pattern, root));
        self.index.take();
        Ok(())
    }

    fn add<H, M>(
        &mut self,
        method: &str,
        path: &str,
        handler: H,
    ) -> Result<RouteHandle<'_>, RouteError>
    where
        H: IntoHandler<M>,
    {
        self.add_with_kind(method, path, handler, RouteKind::Http)
    }

    fn add_with_kind<H, M>(
        &mut self,
        method: &str,
        path: &str,
        handler: H,
        kind: RouteKind,
    ) -> Result<RouteHandle<'_>, RouteError>
    where
        H: IntoHandler<M>,
    {
        let pattern = RoutePattern::parse(path)?;
        self.validate_route_insert(method, &pattern, None, None)?;

        self.routes.push(Route {
            method: method.to_string(),
            pattern,
            handler: handler.into_handler(),
            middlewares: Vec::new(),
            kind,
            meta: RouteMeta::default(),
            name: None,
            host: None,
        });
        self.index.take();
        let index = self.routes.len() - 1;
        Ok(RouteHandle {
            router: self,
            index,
        })
    }

    /// Mounts another router under `prefix`, prepending `prefix` to every one
    /// of its route patterns and baking `other`'s scoped middlewares into each
    /// route. Routes are flattened, so nesting composes (a router that already
    /// had sub-routers mounted carries their patterns and middlewares along).
    pub fn mount(&mut self, prefix: &str, other: Router) -> Result<(), RouteError> {
        let prefix = RoutePattern::parse(prefix)?;
        let scoped = other.middlewares;
        let mut mounted = Vec::new();
        for route in other.routes {
            let pattern = RoutePattern::join(&prefix, &route.pattern)?;
            self.validate_route_insert(
                &route.method,
                &pattern,
                route.host.as_ref(),
                route.name.as_deref(),
            )?;
            validate_route_batch_insert(
                &mounted,
                &route.method,
                &pattern,
                route.host.as_ref(),
                route.name.as_deref(),
            )?;
            // `other`'s own middlewares wrap its routes from the outside, then
            // any middlewares the route already carries (from deeper mounts).
            let mut middlewares = scoped.clone();
            middlewares.extend(route.middlewares);
            mounted.push(Route {
                method: route.method,
                pattern,
                handler: route.handler,
                middlewares,
                kind: route.kind,
                meta: route.meta,
                name: route.name,
                host: route.host,
            });
        }
        self.routes.extend(mounted);
        self.index.take();
        Ok(())
    }

    fn validate_route_insert(
        &self,
        method: &str,
        pattern: &RoutePattern,
        host: Option<&HostPattern>,
        name: Option<&str>,
    ) -> Result<(), RouteError> {
        if let Some(name) = name
            && self
                .routes
                .iter()
                .any(|route| route.name.as_deref() == Some(name))
        {
            return Err(RouteError::new(
                RouteErrorKind::DuplicateName,
                format!("Nombre de ruta duplicado: {name}"),
            ));
        }

        let conflict_key = pattern.conflict_key();
        for route in &self.routes {
            if route.method != method || route.pattern.conflict_key() != conflict_key {
                continue;
            }
            if !hosts_overlap(route.host.as_ref(), host) {
                continue;
            }
            let kind = if route.pattern.as_str() == pattern.as_str() {
                RouteErrorKind::DuplicateRoute
            } else {
                RouteErrorKind::ConflictingPattern
            };
            return Err(RouteError::new(
                kind,
                format!(
                    "La ruta {} {} entra en conflicto con {}",
                    method,
                    pattern.as_str(),
                    route.pattern.as_str()
                ),
            ));
        }

        Ok(())
    }

    fn validate_route_host_update(
        &self,
        index: usize,
        host: &HostPattern,
    ) -> Result<(), RouteError> {
        let route = &self.routes[index];
        let conflict_key = route.pattern.conflict_key();
        for (other_index, other) in self.routes.iter().enumerate() {
            if other_index == index
                || other.method != route.method
                || other.pattern.conflict_key() != conflict_key
            {
                continue;
            }
            if !hosts_overlap(other.host.as_ref(), Some(host)) {
                continue;
            }
            let kind = if other.pattern.as_str() == route.pattern.as_str() {
                RouteErrorKind::DuplicateRoute
            } else {
                RouteErrorKind::ConflictingPattern
            };
            return Err(RouteError::new(
                kind,
                format!(
                    "La ruta {} {} entra en conflicto con {}",
                    route.method,
                    route.pattern.as_str(),
                    other.pattern.as_str()
                ),
            ));
        }
        Ok(())
    }

    /// Finds the best route for `method` + `path` via the trie index,
    /// returning a clone of its handler, its scoped middleware chain, and any
    /// captured path parameters. Precedence: static segments beat `:params`,
    /// which beat trailing `*wildcards` (backtracking across branches); on the
    /// same path an exact-method route beats an `all()` route; remaining ties
    /// go to the first-registered route.
    pub fn resolve(
        &self,
        method: &Method,
        path: &str,
        host: Option<&str>,
    ) -> Result<Option<MatchedRoute>, RouteMatchError> {
        self.resolve_method(method.as_str(), path, host)
    }

    pub(crate) fn resolve_method(
        &self,
        method: &str,
        path: &str,
        host: Option<&str>,
    ) -> Result<Option<MatchedRoute>, RouteMatchError> {
        let segments = path_segments(path);
        for index in self.index().find_candidates(method, &segments) {
            let route = &self.routes[index];
            if !host_matches(route.host.as_ref(), host) {
                continue;
            }
            // The index only returns routes whose pattern matches these
            // segments structurally, so `None` here is defensive.
            let Some(params) = match_pattern_strict(route.pattern.segments(), &segments)? else {
                continue;
            };
            return Ok(Some(MatchedRoute {
                handler: Arc::clone(&route.handler),
                middlewares: route.middlewares.clone(),
                params,
                pattern: route.pattern.as_str().to_string(),
                kind: route.kind.clone(),
                body_limit: route.meta.body_limit,
            }));
        }
        Ok(None)
    }

    pub(crate) fn validate_websockets(
        &self,
        app_defaults: &WebSocketConfig,
    ) -> Result<(), WsError> {
        for route in &self.routes {
            if let RouteKind::WebSocket(route_config) = &route.kind {
                ResolvedWebSocketConfig::from_layers(app_defaults, route_config).validate()?;
            }
        }
        Ok(())
    }

    /// Lists every registered route (method + pattern) in registration order,
    /// with mounted prefixes already applied. `all()` routes report `*`.
    pub fn routes(&self) -> Vec<RouteInfo> {
        self.routes
            .iter()
            .map(|route| RouteInfo {
                method: route.method.clone(),
                path: route.pattern.as_str().to_string(),
                summary: route.meta.summary.clone(),
                description: route.meta.description.clone(),
                tags: route.meta.tags.clone(),
            })
            .collect()
    }

    /// Returns the distinct concrete methods registered for routes whose
    /// pattern matches `path` (ignoring the request method), in registration
    /// order. Used to build the `Allow` header for 405/OPTIONS responses.
    /// `*` (catch-all) is excluded.
    pub(crate) fn allowed_methods(
        &self,
        path: &str,
        host: Option<&str>,
    ) -> Result<Vec<String>, RouteMatchError> {
        let segments = path_segments(path);
        let mut methods = Vec::new();
        for (index, method) in self.index().matching_methods(&segments) {
            let route = &self.routes[index];
            if !host_matches(route.host.as_ref(), host) {
                continue;
            }
            match_pattern_strict(route.pattern.segments(), &segments)?;
            if method != METHOD_ALL && !methods.contains(&method) {
                methods.push(method);
            }
        }
        Ok(methods)
    }

    pub fn url_for<K, V, I>(&self, name: &str, params: I) -> Result<String, RouteError>
    where
        K: AsRef<str>,
        V: AsRef<str>,
        I: IntoIterator<Item = (K, V)>,
    {
        let route = self
            .routes
            .iter()
            .find(|route| route.name.as_deref() == Some(name))
            .ok_or_else(|| {
                RouteError::new(
                    RouteErrorKind::MissingUrlParameter,
                    format!("No existe una ruta con nombre {name}"),
                )
            })?;
        build_url(&route.pattern, params)
    }
}

fn validate_route_batch_insert(
    routes: &[Route],
    method: &str,
    pattern: &RoutePattern,
    host: Option<&HostPattern>,
    name: Option<&str>,
) -> Result<(), RouteError> {
    if let Some(name) = name
        && routes
            .iter()
            .any(|route| route.name.as_deref() == Some(name))
    {
        return Err(RouteError::new(
            RouteErrorKind::DuplicateName,
            format!("Nombre de ruta duplicado: {name}"),
        ));
    }

    let conflict_key = pattern.conflict_key();
    for route in routes {
        if route.method != method
            || route.pattern.conflict_key() != conflict_key
            || !hosts_overlap(route.host.as_ref(), host)
        {
            continue;
        }
        let kind = if route.pattern.as_str() == pattern.as_str() {
            RouteErrorKind::DuplicateRoute
        } else {
            RouteErrorKind::ConflictingPattern
        };
        return Err(RouteError::new(
            kind,
            format!(
                "La ruta {} {} entra en conflicto con {}",
                method,
                pattern.as_str(),
                route.pattern.as_str()
            ),
        ));
    }
    Ok(())
}

fn host_matches(pattern: Option<&HostPattern>, host: Option<&str>) -> bool {
    match pattern {
        Some(pattern) => pattern.matches_request(host),
        None => true,
    }
}

fn hosts_overlap(left: Option<&HostPattern>, right: Option<&HostPattern>) -> bool {
    match (left, right) {
        (None, None) | (None, Some(_)) | (Some(_), None) => true,
        (Some(HostPattern::Exact(left)), Some(HostPattern::Exact(right))) => left == right,
        (Some(HostPattern::Exact(host)), Some(pattern))
        | (Some(pattern), Some(HostPattern::Exact(host))) => pattern.matches_normalized(host),
        (
            Some(HostPattern::WildcardSubdomain(left)),
            Some(HostPattern::WildcardSubdomain(right)),
        ) => left == right || suffix_overlaps(left, right) || suffix_overlaps(right, left),
    }
}

fn suffix_overlaps(more_specific: &str, broader: &str) -> bool {
    more_specific.len() > broader.len()
        && more_specific.ends_with(broader)
        && more_specific.as_bytes()[more_specific.len() - broader.len() - 1] == b'.'
}

fn build_url<K, V, I>(pattern: &RoutePattern, params: I) -> Result<String, RouteError>
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    let params: HashMap<String, String> = params
        .into_iter()
        .map(|(key, value)| (key.as_ref().to_string(), value.as_ref().to_string()))
        .collect();
    let expected: HashSet<&str> = pattern
        .parameter_names()
        .iter()
        .map(String::as_str)
        .collect();

    for expected_name in &expected {
        if !params.contains_key(*expected_name) {
            return Err(RouteError::new(
                RouteErrorKind::MissingUrlParameter,
                format!("Falta el parametro de URL {expected_name}"),
            ));
        }
    }
    for provided in params.keys() {
        if !expected.contains(provided.as_str()) {
            return Err(RouteError::new(
                RouteErrorKind::UnexpectedUrlParameter,
                format!("Parametro de URL inesperado: {provided}"),
            ));
        }
    }

    if pattern.segments().is_empty() {
        return Ok("/".to_string());
    }

    let mut out = String::new();
    for segment in pattern.segments() {
        out.push('/');
        match segment {
            Segment::Static(value) => out.push_str(value),
            Segment::Param(name) => {
                let value = params.get(name).expect("checked above");
                out.push_str(&encode_path_segment(value));
            }
            Segment::Wildcard(name) => {
                let value = params.get(name).expect("checked above");
                let encoded = value
                    .split('/')
                    .map(encode_path_segment)
                    .collect::<Vec<_>>()
                    .join("/");
                out.push_str(&encoded);
            }
        }
    }
    Ok(out)
}

fn encode_path_segment(input: &str) -> String {
    let mut encoded = String::new();
    for byte in input.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(*byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn static_route(method: &str, pattern: RoutePattern, root: Arc<PathBuf>) -> Route {
    let handler: Handler = Arc::new(
        move |req| -> Pin<Box<dyn Future<Output = Response> + Send>> {
            let root = Arc::clone(&root);
            Box::pin(async move { serve_static_file(root, req).await })
        },
    );

    Route {
        method: method.to_string(),
        pattern,
        handler,
        middlewares: Vec::new(),
        kind: RouteKind::Http,
        meta: RouteMeta::default(),
        name: None,
        host: None,
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

/// One entry of a route listing, as returned by [`Router::routes`] /
/// `App::routes`: the HTTP method (`*` for `all()` routes), the route pattern
/// with `:param` / `*wildcard` placeholders, and any documentation attached
/// through [`RouteHandle`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteInfo {
    pub method: String,
    pub path: String,
    pub summary: Option<String>,
    pub description: Option<String>,
    pub tags: Vec<String>,
}

/// Renders a parsed pattern back to its `/users/:id` string form.
fn render_pattern(pattern: &[Segment]) -> String {
    if pattern.is_empty() {
        return "/".to_string();
    }
    let mut out = String::new();
    for segment in pattern {
        out.push('/');
        match segment {
            Segment::Static(s) => out.push_str(s),
            Segment::Param(name) => {
                out.push(':');
                out.push_str(name);
            }
            Segment::Wildcard(name) => {
                out.push('*');
                out.push_str(name);
            }
        }
    }
    out
}

/// A handle to a just-registered route, returned by the route methods so that
/// middleware and transport policy can be scoped to it. The handle is
/// ignorable when no route-specific configuration is needed.
pub struct RouteHandle<'a> {
    router: &'a mut Router,
    index: usize,
}

impl Debug for RouteHandle<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouteHandle")
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

impl RouteHandle<'_> {
    fn fail_and_remove(self, error: RouteError) -> Result<Self, RouteError> {
        self.router.routes.remove(self.index);
        self.router.index.take();
        Err(error)
    }

    /// Adds a middleware that wraps only this route. Repeated calls stack, with
    /// the first-added middleware outermost.
    pub fn layer<MW: IntoMiddleware>(self, middleware: MW) -> Self {
        self.router.routes[self.index]
            .middlewares
            .push(middleware.into_middleware());
        self
    }

    /// Sets the maximum request body size for this route. The application-level
    /// maximum remains a hard ceiling.
    pub fn body_limit(self, bytes: usize) -> Self {
        self.router.routes[self.index].meta.body_limit = Some(bytes);
        self
    }

    pub fn name(self, name: &str) -> Result<Self, RouteError> {
        if self
            .router
            .routes
            .iter()
            .enumerate()
            .any(|(index, route)| index != self.index && route.name.as_deref() == Some(name))
        {
            return self.fail_and_remove(RouteError::new(
                RouteErrorKind::DuplicateName,
                format!("Nombre de ruta duplicado: {name}"),
            ));
        }
        self.router.routes[self.index].name = Some(name.to_string());
        Ok(self)
    }

    pub fn host(self, host: &str) -> Result<Self, RouteError> {
        let host = match HostPattern::parse(host) {
            Ok(host) => host,
            Err(error) => return self.fail_and_remove(error),
        };
        if let Err(error) = self.router.validate_route_host_update(self.index, &host) {
            return self.fail_and_remove(error);
        }
        self.router.routes[self.index].host = Some(host);
        self.router.index.take();
        Ok(self)
    }

    /// Applies an execution timeout to this route only.
    pub fn timeout(self, timeout: Duration) -> Self {
        self.router.routes[self.index]
            .middlewares
            .push(super::middleware::timeout(timeout));
        self
    }

    /// Sets a one-line summary for this route (route listings + OpenAPI).
    pub fn summary(self, summary: &str) -> Self {
        self.router.routes[self.index].meta.summary = Some(summary.to_string());
        self
    }

    /// Sets a longer description for this route (OpenAPI).
    pub fn description(self, description: &str) -> Self {
        self.router.routes[self.index].meta.description = Some(description.to_string());
        self
    }

    /// Adds a tag to this route (repeatable; groups operations in OpenAPI).
    pub fn tag(self, tag: &str) -> Self {
        self.router.routes[self.index]
            .meta
            .tags
            .push(tag.to_string());
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

    let Ok(metadata) = tokio::fs::metadata(&file_path).await else {
        return Response::not_found();
    };
    let total_len = metadata.len();
    let modified = metadata.modified().ok();
    let etag = modified.map(|time| {
        let stamp = time
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("\"{:x}-{:x}\"", total_len, stamp)
    });

    let validators = |res: Response| -> Response {
        let mut res = res.header("accept-ranges", "bytes");
        if let Some(etag) = &etag {
            res = res.header("etag", etag);
        }
        if let Some(modified) = modified {
            res = res.header("last-modified", &httpdate::fmt_http_date(modified));
        }
        res
    };

    let range_requested = req.header("range").is_some();
    let preconditions =
        super::middleware::evaluate_preconditions(&req, etag.as_deref(), modified, range_requested);
    match preconditions {
        super::middleware::PreconditionResult::Failed => {
            return validators(Response::send("").status(412));
        }
        super::middleware::PreconditionResult::NotModified => {
            return validators(Response::send("").status(304));
        }
        super::middleware::PreconditionResult::Proceed
        | super::middleware::PreconditionResult::IgnoreRange => {}
    }

    // A structurally valid but unsatisfiable Range gets 416; a malformed one
    // is ignored and the full file is served (as the RFC allows).
    let range = if preconditions == super::middleware::PreconditionResult::IgnoreRange {
        None
    } else {
        match req
            .header("range")
            .map(|raw| parse_byte_range(raw, total_len))
        {
            Some(RangeParse::Satisfiable(start, end)) => Some((start, end)),
            Some(RangeParse::Unsatisfiable) => {
                return validators(
                    Response::send("")
                        .status(416)
                        .header("content-range", &format!("bytes */{}", total_len)),
                );
            }
            Some(RangeParse::Ignored) | None => None,
        }
    };

    let Ok(mut file) = tokio::fs::File::open(&file_path).await else {
        return Response::not_found();
    };

    let (start, len, mut response_status) = match range {
        Some((start, end)) => (start, end - start + 1, 206),
        None => (0, total_len, 200),
    };
    if start > 0 && file.seek(SeekFrom::Start(start)).await.is_err() {
        return Response::internal_server_error();
    }
    // An empty file has nothing to stream; serve it as a normal 200.
    if len == 0 {
        response_status = 200;
    }

    let mut res = Response::stream(file_stream(file, len))
        .status(response_status)
        .content_type(content_type_for_path(&file_path))
        .header("content-length", &len.to_string());
    if let Some((start, end)) = range {
        res = res.header(
            "content-range",
            &format!("bytes {}-{}/{}", start, end, total_len),
        );
    }
    validators(res)
}

enum RangeParse {
    Satisfiable(u64, u64),
    Unsatisfiable,
    Ignored,
}

/// Parses a single-range `Range: bytes=...` header against a resource of
/// `total_len` bytes. Multi-range and malformed headers are ignored.
fn parse_byte_range(raw: &str, total_len: u64) -> RangeParse {
    let Some(spec) = raw.trim().strip_prefix("bytes=") else {
        return RangeParse::Ignored;
    };
    if spec.contains(',') {
        return RangeParse::Ignored;
    }
    let Some((start_raw, end_raw)) = spec.split_once('-') else {
        return RangeParse::Ignored;
    };
    let (start_raw, end_raw) = (start_raw.trim(), end_raw.trim());

    if start_raw.is_empty() {
        // Suffix form: last N bytes.
        let Ok(suffix) = end_raw.parse::<u64>() else {
            return RangeParse::Ignored;
        };
        if suffix == 0 || total_len == 0 {
            return RangeParse::Unsatisfiable;
        }
        let start = total_len.saturating_sub(suffix);
        return RangeParse::Satisfiable(start, total_len - 1);
    }

    let Ok(start) = start_raw.parse::<u64>() else {
        return RangeParse::Ignored;
    };
    if start >= total_len {
        return RangeParse::Unsatisfiable;
    }
    let end = if end_raw.is_empty() {
        total_len - 1
    } else {
        match end_raw.parse::<u64>() {
            Ok(end) => end.min(total_len - 1),
            Err(_) => return RangeParse::Ignored,
        }
    };
    if end < start {
        return RangeParse::Ignored;
    }
    RangeParse::Satisfiable(start, end)
}

/// Streams `len` bytes from `file` in 64 KB chunks. On a read error the
/// body terminates with that error instead of silently ending early.
fn file_stream(
    file: tokio::fs::File,
    len: u64,
) -> impl Stream<Item = std::io::Result<Bytes>> + Send {
    futures_util::stream::unfold((file, len), |(mut file, remaining)| async move {
        if remaining == 0 {
            return None;
        }
        let chunk = remaining.min(64 * 1024) as usize;
        let mut buffer = vec![0u8; chunk];
        match file.read(&mut buffer).await {
            Ok(0) => Some((
                Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "static file ended before content-length bytes were read",
                )),
                (file, 0),
            )),
            Err(error) => Some((Err(error), (file, 0))),
            Ok(read) => {
                buffer.truncate(read);
                Some((Ok(Bytes::from(buffer)), (file, remaining - read as u64)))
            }
        }
    })
}

fn safe_static_path(root: &FsPath, requested: &str) -> Option<PathBuf> {
    let relative = if requested.is_empty() {
        "index.html"
    } else {
        requested
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
