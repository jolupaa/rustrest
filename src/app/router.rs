use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path as FsPath, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, UNIX_EPOCH};

use cap_std::ambient_authority;
use cap_std::fs::Dir;
use futures_util::Stream;
use hyper::body::Bytes;
use hyper::{Method, StatusCode};
use percent_encoding::percent_decode_str;
use tokio::sync::{Semaphore, mpsc, oneshot};

use super::trie::RouteIndex;
use super::websocket::ResolvedWebSocketConfig;
use super::{
    Handler, HttpError, IntoHandler, IntoMiddleware, IntoWebSocketHandler, IntoWebSocketOutput,
    Middleware, Next, Request, Response, WebSocket, WebSocketConfig, WsError,
};

pub(crate) const METHOD_ALL: &str = "*";
const MAX_PATH_SEGMENTS: usize = 256;
const STATIC_OPEN_CONCURRENCY: usize = 64;
const STATIC_STREAM_CHANNEL_CAPACITY: usize = 1;
const DEFAULT_STATIC_STREAM_START_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_STATIC_STREAM_STALL_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_STATIC_STREAM_MAX_DURATION: Duration = Duration::from_secs(5 * 60);
static STATIC_OPEN_ADMISSION: OnceLock<Arc<Semaphore>> = OnceLock::new();

/// A single segment of a route pattern.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Segment {
    Static(String),
    /// A `:name` placeholder, storing `name` (without the colon).
    Param(String),
    /// A trailing `*name` placeholder, capturing the rest of the path.
    Wildcard(String),
}

/// A parameter-name-independent route shape used for conflict detection.
///
/// Keeping segment kinds and boundaries structural avoids conflating literals
/// such as `:`, `*`, or `a/b` with placeholder markers or path separators.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum ConflictSegment {
    Static(String),
    Param,
    Wildcard,
}

/// A validated, canonical route pattern such as `/users/:id`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RoutePattern {
    rendered: String,
    segments: Vec<Segment>,
    parameter_names: Vec<String>,
    conflict_key: Vec<ConflictSegment>,
}

/// Stable classification for errors raised while registering or configuring a route.
///
/// This enum is non-exhaustive so RustRest can add new validation failures without
/// breaking downstream code. Consumers must include a wildcard arm when matching it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RouteErrorKind {
    /// A route pattern does not start with `/`.
    MissingLeadingSlash,
    /// A parameter or wildcard has no name.
    EmptyParameter,
    /// A parameter or wildcard name contains unsupported characters.
    InvalidParameter,
    /// A route pattern declares the same parameter name more than once.
    DuplicateParameter,
    /// A wildcard appears before the final path segment.
    NonTerminalWildcard,
    /// A route pattern exceeds the framework's segment limit.
    TooManySegments,
    /// The same method, path, and host constraint are already registered.
    DuplicateRoute,
    /// A structurally equivalent parameterized pattern is already registered.
    ConflictingPattern,
    /// A named route reuses a name that is already registered.
    DuplicateName,
    /// A host constraint is malformed or unsupported.
    InvalidHostPattern,
    /// URL generation did not receive a required route parameter.
    MissingUrlParameter,
    /// URL generation received a parameter not declared by the route.
    UnexpectedUrlParameter,
    /// A registered route pattern contains malformed percent encoding.
    InvalidPercentEncoding,
    /// A static-file root could not be opened safely.
    InvalidStaticRoot,
    /// Static-file options are internally inconsistent or unsafe.
    InvalidStaticOptions,
    /// The HTTP method cannot be represented by an ordinary response route.
    UnsupportedMethod,
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

/// Stable classification for failures encountered while matching an incoming path.
///
/// This enum is non-exhaustive so new path-validation failures can be introduced
/// compatibly. Consumers must include a wildcard arm when matching it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RouteMatchErrorKind {
    /// The request path contains malformed encoding, invalid UTF-8, or decoded controls.
    InvalidPathEncoding,
    /// The request path exceeds the framework's segment limit.
    TooManySegments,
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

    pub fn too_many_segments() -> Self {
        Self {
            kind: RouteMatchErrorKind::TooManySegments,
            message: format!("La ruta no puede contener mas de {MAX_PATH_SEGMENTS} segmentos"),
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
            RouteMatchErrorKind::TooManySegments => HttpError::new(
                StatusCode::URI_TOO_LONG,
                "too_many_path_segments",
                "La ruta contiene demasiados segmentos",
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
        if raw_segments.len() > MAX_PATH_SEGMENTS {
            return Err(RouteError::new(
                RouteErrorKind::TooManySegments,
                format!("Las rutas no pueden contener mas de {MAX_PATH_SEGMENTS} segmentos"),
            ));
        }
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
                segments.push(Segment::Static(decode_route_static_segment(raw)?));
            }
        }

        Ok(Self::from_validated_segments(segments, parameter_names))
    }

    fn from_validated_segments(segments: Vec<Segment>, parameter_names: Vec<String>) -> Self {
        let rendered = render_pattern(&segments);
        let conflict_key = segments
            .iter()
            .map(|segment| match segment {
                Segment::Static(value) => ConflictSegment::Static(value.clone()),
                Segment::Param(_) => ConflictSegment::Param,
                Segment::Wildcard(_) => ConflictSegment::Wildcard,
            })
            .collect();
        Self {
            rendered,
            segments,
            parameter_names,
            conflict_key,
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

    fn conflict_key(&self) -> &[ConflictSegment] {
        &self.conflict_key
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
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(RouteError::new(
            RouteErrorKind::InvalidParameter,
            format!("Nombre de parametro de ruta no valido: {name}"),
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
    if segments.len() > MAX_PATH_SEGMENTS {
        return Err(RouteError::new(
            RouteErrorKind::TooManySegments,
            format!("Las rutas no pueden contener mas de {MAX_PATH_SEGMENTS} segmentos"),
        ));
    }
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

fn decode_route_static_segment(raw: &str) -> Result<String, RouteError> {
    let bytes = raw.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit()
            {
                return Err(RouteError::new(
                    RouteErrorKind::InvalidPercentEncoding,
                    "El patron de ruta contiene codificacion porcentual invalida",
                ));
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    let decoded = percent_decode_str(raw).decode_utf8().map_err(|_| {
        RouteError::new(
            RouteErrorKind::InvalidPercentEncoding,
            "El patron de ruta contiene UTF-8 porcentual no valido",
        )
    })?;
    if decoded.chars().any(char::is_control) {
        return Err(RouteError::new(
            RouteErrorKind::InvalidPercentEncoding,
            "El patron de ruta contiene caracteres de control",
        ));
    }
    Ok(decoded.into_owned())
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
#[cfg(test)]
pub(crate) fn parse_pattern(path: &str) -> Vec<Segment> {
    RoutePattern::parse(path)
        .expect("valid route pattern")
        .segments
}

/// Matches a parsed pattern against concrete path segments, capturing params.
/// Returns `None` if the pattern does not match.
#[cfg(test)]
pub(crate) fn match_pattern(
    pattern: &[Segment],
    segments: &[&str],
) -> Option<HashMap<String, String>> {
    let decoded = segments
        .iter()
        .map(|segment| decode_path_segment(segment))
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    match_decoded_pattern(pattern, &decoded)
}

fn decode_path_segments(path: &str) -> Result<Vec<Cow<'_, str>>, RouteMatchError> {
    let segments = path_segments(path);
    if segments.len() > MAX_PATH_SEGMENTS {
        return Err(RouteMatchError::too_many_segments());
    }
    segments.into_iter().map(decode_path_segment).collect()
}

fn match_decoded_pattern(
    pattern: &[Segment],
    segments: &[Cow<'_, str>],
) -> Option<HashMap<String, String>> {
    let mut params = HashMap::new();
    let mut index = 0;
    for (pattern_index, seg) in pattern.iter().enumerate() {
        if let Segment::Wildcard(name) = seg {
            if pattern_index != pattern.len() - 1 {
                return None;
            }
            let captured = segments[index..]
                .iter()
                .map(|segment| segment.as_ref())
                .collect::<Vec<_>>()
                .join("/");
            params.insert(name.clone(), captured);
            return Some(params);
        }

        let actual = segments.get(index)?;
        match seg {
            Segment::Static(s) if s == actual.as_ref() => {}
            Segment::Static(_) => return None,
            Segment::Param(name) => {
                params.insert(name.clone(), actual.to_string());
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

fn decode_path_segment(segment: &str) -> Result<Cow<'_, str>, RouteMatchError> {
    validate_percent_triplets(segment)?;
    let decoded = percent_decode_str(segment)
        .decode_utf8()
        .map_err(|_| RouteMatchError::invalid_path_encoding())?;
    if decoded.chars().any(char::is_control) {
        return Err(RouteMatchError::invalid_path_encoding());
    }
    Ok(decoded)
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

/// Policy for files or directories whose name starts with `.`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Dotfiles {
    /// Return 404 without touching dotfiles. This is the safe default.
    #[default]
    Deny,
    /// Serve dotfiles that are otherwise contained beneath the static root.
    Allow,
}

/// Security policy for a static-file mount.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StaticFilesOptions {
    dotfiles: Dotfiles,
    stream_start_timeout: Option<Duration>,
    stream_stall_timeout: Option<Duration>,
    stream_max_duration: Option<Duration>,
}

impl StaticFilesOptions {
    pub const fn new() -> Self {
        Self {
            dotfiles: Dotfiles::Deny,
            stream_start_timeout: Some(DEFAULT_STATIC_STREAM_START_TIMEOUT),
            stream_stall_timeout: Some(DEFAULT_STATIC_STREAM_STALL_TIMEOUT),
            stream_max_duration: Some(DEFAULT_STATIC_STREAM_MAX_DURATION),
        }
    }

    pub const fn dotfiles(mut self, policy: Dotfiles) -> Self {
        self.dotfiles = policy;
        self
    }

    /// Maximum time the response body may remain completely unpolled before
    /// its file and admission permit are released. The safe default is one
    /// minute, longer than the default request/middleware deadline.
    pub const fn stream_start_timeout(mut self, timeout: Duration) -> Self {
        self.stream_start_timeout = Some(timeout);
        self
    }

    /// Disables the first-body-poll deadline. Only use this when the transport
    /// guarantees that response bodies are polled or cancelled promptly.
    pub const fn disable_stream_start_timeout(mut self) -> Self {
        self.stream_start_timeout = None;
        self
    }

    /// Maximum time a static-file producer may remain blocked while handing a
    /// chunk to the HTTP transport. The safe default is 15 seconds.
    pub const fn stream_stall_timeout(mut self, timeout: Duration) -> Self {
        self.stream_stall_timeout = Some(timeout);
        self
    }

    /// Disables the stalled-consumer deadline. Only use this when an upstream
    /// proxy enforces an equivalent response-write timeout.
    pub const fn disable_stream_stall_timeout(mut self) -> Self {
        self.stream_stall_timeout = None;
        self
    }

    /// Maximum total lifetime of a static-file producer. The safe default is
    /// five minutes, which prevents slow-drip consumers from holding a file
    /// descriptor and the global admission permit forever.
    pub const fn stream_max_duration(mut self, timeout: Duration) -> Self {
        self.stream_max_duration = Some(timeout);
        self
    }

    /// Disables the total stream lifetime. Only use this with an equivalent
    /// externally enforced response lifetime.
    pub const fn disable_stream_max_duration(mut self) -> Self {
        self.stream_max_duration = None;
        self
    }
}

impl Default for StaticFilesOptions {
    fn default() -> Self {
        Self::new()
    }
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
        self.static_files_with_options(prefix, root, StaticFilesOptions::default())
    }

    /// Mounts a capability-confined static directory with an explicit
    /// dotfile policy.
    pub fn static_files_with_options<P>(
        &mut self,
        prefix: &str,
        root: P,
        options: StaticFilesOptions,
    ) -> Result<(), RouteError>
    where
        P: Into<PathBuf>,
    {
        validate_static_options(options)?;
        let root_path = root.into();
        let root = Dir::open_ambient_dir(&root_path, ambient_authority()).map_err(|error| {
            RouteError::new(
                RouteErrorKind::InvalidStaticRoot,
                format!(
                    "No se pudo abrir la raiz de archivos estaticos {}: {error}",
                    root_path.display()
                ),
            )
        })?;
        let root = Arc::new(root);
        let pattern = join_paths(prefix, "/*path");
        let get_pattern = RoutePattern::parse(&pattern)?;
        let head_pattern = get_pattern.clone();

        self.validate_route_insert("GET", &get_pattern, None, None)?;
        self.validate_route_insert("HEAD", &head_pattern, None, None)?;

        self.routes
            .push(static_route("GET", get_pattern, Arc::clone(&root), options));
        self.routes
            .push(static_route("HEAD", head_pattern, root, options));
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
        // CONNECT changes the HTTP connection into a tunnel and therefore
        // cannot be implemented by an ordinary request/response handler. Do
        // not advertise a route whose successful response would not own the
        // transport; a dedicated tunnel API can add this safely in the future.
        if method.eq_ignore_ascii_case(Method::CONNECT.as_str()) {
            return Err(RouteError::new(
                RouteErrorKind::UnsupportedMethod,
                "CONNECT requiere una API de tunel dedicada y no se puede registrar como ruta HTTP",
            ));
        }
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
        if let Some(name) = name {
            if self
                .routes
                .iter()
                .any(|route| route.name.as_deref() == Some(name))
            {
                return Err(RouteError::new(
                    RouteErrorKind::DuplicateName,
                    format!("Nombre de ruta duplicado: {name}"),
                ));
            }
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
        // Decode and validate every request segment exactly once before trie
        // lookup. The vector retains segment boundaries, so an encoded slash
        // (`%2F`) remains data inside one captured parameter rather than
        // becoming a path separator.
        let segments = decode_path_segments(path)?;
        for index in self.index().find_candidates(method, &segments) {
            let route = &self.routes[index];
            if !host_matches(route.host.as_ref(), host) {
                continue;
            }
            // The index only returns routes whose pattern matches these
            // segments structurally, so `None` here is defensive.
            let Some(params) = match_decoded_pattern(route.pattern.segments(), &segments) else {
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

    /// Resolves only ordinary HTTP routes. This is used for implicit `HEAD`
    /// fallback so a WebSocket route registered as `GET` is never invoked as
    /// an HTTP handler.
    pub(crate) fn resolve_http_method(
        &self,
        method: &str,
        path: &str,
        host: Option<&str>,
    ) -> Result<Option<MatchedRoute>, RouteMatchError> {
        let segments = decode_path_segments(path)?;
        for index in self.index().find_candidates(method, &segments) {
            let route = &self.routes[index];
            if let RouteKind::WebSocket(_) = &route.kind {
                continue;
            }
            if !host_matches(route.host.as_ref(), host) {
                continue;
            }
            let Some(params) = match_decoded_pattern(route.pattern.segments(), &segments) else {
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
        let segments = decode_path_segments(path)?;
        let mut methods = Vec::new();
        for (index, method) in self.index().matching_methods(&segments) {
            let route = &self.routes[index];
            if !host_matches(route.host.as_ref(), host) {
                continue;
            }
            if match_decoded_pattern(route.pattern.segments(), &segments).is_none() {
                continue;
            }
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
    if let Some(name) = name {
        if routes
            .iter()
            .any(|route| route.name.as_deref() == Some(name))
        {
            return Err(RouteError::new(
                RouteErrorKind::DuplicateName,
                format!("Nombre de ruta duplicado: {name}"),
            ));
        }
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
            Segment::Static(value) => out.push_str(&encode_path_segment(value)),
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

fn static_route(
    method: &str,
    pattern: RoutePattern,
    root: Arc<Dir>,
    options: StaticFilesOptions,
) -> Route {
    let handler: Handler = Arc::new(
        move |req| -> Pin<Box<dyn Future<Output = Response> + Send>> {
            let root = Arc::clone(&root);
            Box::pin(async move { serve_static_file(root, req, options).await })
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
/// `HEAD` only when an ordinary HTTP `GET` can serve it, plus `OPTIONS`.
pub(crate) fn allow_header_value(allowed: &[String], implicit_head: bool) -> String {
    let mut methods = allowed.to_vec();
    if implicit_head && !methods.iter().any(|m| m == "HEAD") {
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
            Segment::Static(s) => out.push_str(&encode_path_segment(s)),
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

async fn serve_static_file(root: Arc<Dir>, req: Request, options: StaticFilesOptions) -> Response {
    let relative = match safe_static_relative(req.param("path").unwrap_or(""), options.dotfiles) {
        Ok(relative) => relative,
        Err(StaticPathRejection::Hidden) => return Response::not_found(),
        Err(StaticPathRejection::Invalid) => return Response::bad_request(),
    };
    let admission = Arc::clone(
        STATIC_OPEN_ADMISSION.get_or_init(|| Arc::new(Semaphore::new(STATIC_OPEN_CONCURRENCY))),
    );
    let permit = match admission.acquire_owned().await {
        Ok(permit) => permit,
        Err(_) => return Response::internal_server_error(),
    };
    let opened =
        tokio::task::spawn_blocking(move || (open_static_file(&root, relative), permit)).await;
    let (opened, permit) = match opened {
        Ok(opened) => opened,
        Err(error) => {
            return Response::from_error(
                HttpError::internal_server_error("No se pudo abrir el archivo estatico")
                    .with_source(error),
            );
        }
    };
    let OpenedStaticFile {
        file,
        relative_path,
        total_len,
        modified,
    } = match opened {
        Ok(opened) => opened,
        Err(error) if static_error_is_not_found(&error) => {
            return Response::not_found();
        }
        Err(error) => {
            return Response::from_error(
                HttpError::internal_server_error("No se pudo abrir el archivo estatico")
                    .with_source(error),
            );
        }
    };
    let etag = modified.map(|time| {
        let stamp = time
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        // Length + modification time is efficient but is not a byte-for-byte
        // content hash, so it is a weak validator by definition.
        format!("W/\"{:x}-{:x}\"", total_len, stamp)
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

    // RFC range processing applies to GET. A HEAD request must describe the
    // full selected representation rather than turning a Range field into a
    // misleading 206/416 response with no body.
    let range_header = req.method.eq_ignore_ascii_case("GET").then(|| {
        let values = req.headers_all("range");
        if values.is_empty() {
            req.header("range").map(str::to_string)
        } else {
            Some(values.join(","))
        }
    });
    let range_header = range_header.flatten();
    let range_requested = range_header.is_some();
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
        match range_header
            .as_deref()
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

    // HEAD is metadata-only. Do not start the eager supervised producer: an
    // empty stream retains the selected representation length without making
    // finalization compare it to an empty buffered body. The opened file and
    // admission permit are dropped as this handler returns.
    if req.method.eq_ignore_ascii_case("HEAD") {
        return validators(
            Response::stream(futures_util::stream::empty::<std::io::Result<Bytes>>())
                .content_type(content_type_for_path(&relative_path))
                .header("content-length", &total_len.to_string()),
        );
    }

    let (start, len, mut response_status) = match range {
        Some((start, end)) => (start, end - start + 1, 206),
        None => (0, total_len, 200),
    };
    // An empty file has nothing to stream; serve it as a normal 200.
    if len == 0 {
        response_status = 200;
        return validators(
            Response::stream(futures_util::stream::empty::<std::io::Result<Bytes>>())
                .status(response_status)
                .content_type(content_type_for_path(&relative_path))
                .header("content-length", "0"),
        );
    }

    let (file, permit) = if start > 0 {
        let seek = tokio::task::spawn_blocking(move || {
            let mut file = file;
            let result = file.seek(SeekFrom::Start(start));
            (file, permit, result)
        })
        .await;
        match seek {
            Ok((file, permit, Ok(_))) => (file, permit),
            Ok((_file, _permit, Err(error))) => {
                return Response::from_error(
                    HttpError::internal_server_error("No se pudo posicionar el archivo estatico")
                        .with_source(error),
                );
            }
            Err(error) => {
                return Response::from_error(
                    HttpError::internal_server_error("No se pudo posicionar el archivo estatico")
                        .with_source(error),
                );
            }
        }
    } else {
        (file, permit)
    };

    let mut res = Response::stream(file_stream(
        file,
        len,
        permit,
        options.stream_start_timeout,
        options.stream_stall_timeout,
        options.stream_max_duration,
    ))
    .status(response_status)
    .content_type(content_type_for_path(&relative_path))
    .header("content-length", &len.to_string());
    if let Some((start, end)) = range {
        res = res.header(
            "content-range",
            &format!("bytes {}-{}/{}", start, end, total_len),
        );
    }
    validators(res)
}

fn static_error_is_not_found(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::NotFound
        // cap-std intentionally reports a symlink/capability-root escape as a
        // synthetic PermissionDenied error. Hide that client-selected path as
        // 404 while preserving genuine OS permission/resource errors as 500.
        || error.kind() == std::io::ErrorKind::PermissionDenied
            && error.to_string() == "a path led outside of the filesystem"
}

enum RangeParse {
    Satisfiable(u64, u64),
    Unsatisfiable,
    Ignored,
}

/// Parses a single-range `Range: bytes=...` header against a resource of
/// `total_len` bytes. Multi-range and malformed headers are ignored.
fn parse_byte_range(raw: &str, total_len: u64) -> RangeParse {
    let Some((unit, spec)) = raw.trim().split_once('=') else {
        return RangeParse::Ignored;
    };
    if !unit.trim().eq_ignore_ascii_case("bytes") {
        return RangeParse::Ignored;
    }
    let spec = spec.trim();
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

struct PendingStaticStream {
    file: std::fs::File,
    permit: tokio::sync::OwnedSemaphorePermit,
}

enum StaticStreamState {
    Pending {
        source: Arc<Mutex<Option<PendingStaticStream>>>,
        cancel_start_lease: Option<oneshot::Sender<()>>,
        len: u64,
        stall_timeout: Option<Duration>,
        max_duration: Option<Duration>,
    },
    Active {
        receiver: mpsc::Receiver<std::io::Result<Bytes>>,
        remaining: u64,
    },
    Finished,
}

/// Lazily streams `len` bytes from `file` in 64 KB chunks through a one-chunk
/// channel. The producer starts only on the body's first poll, so outbound
/// middleware time does not consume its stall/lifetime budgets. A separate
/// start lease releases an entirely unpolled file. Each blocking OS read owns
/// the admission permit, so timing out its Tokio join cannot detach an
/// unaccounted file descriptor. On any early exit, the consumer emits
/// `UnexpectedEof` rather than silently ending before Content-Length.
fn file_stream(
    file: std::fs::File,
    len: u64,
    permit: tokio::sync::OwnedSemaphorePermit,
    start_timeout: Option<Duration>,
    stall_timeout: Option<Duration>,
    max_duration: Option<Duration>,
) -> impl Stream<Item = std::io::Result<Bytes>> + Send {
    let source = Arc::new(Mutex::new(Some(PendingStaticStream { file, permit })));
    let supervisor_source = Arc::clone(&source);
    let (cancel_start_lease, start_lease_cancelled) = oneshot::channel();
    tokio::spawn(async move {
        match start_timeout {
            Some(timeout) => {
                tokio::select! {
                    _ = tokio::time::sleep(timeout) => {}
                    _ = start_lease_cancelled => {}
                }
            }
            None => {
                let _ = start_lease_cancelled.await;
            }
        }
        supervisor_source
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
    });

    futures_util::stream::unfold(
        StaticStreamState::Pending {
            source,
            cancel_start_lease: Some(cancel_start_lease),
            len,
            stall_timeout,
            max_duration,
        },
        |mut state| async move {
            loop {
                match state {
                    StaticStreamState::Pending {
                        source,
                        mut cancel_start_lease,
                        len,
                        stall_timeout,
                        max_duration,
                    } => {
                        let source = source
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .take();
                        // Wakes the start-lease supervisor. The source has
                        // already moved to this body poll, so its cleanup is a
                        // no-op.
                        drop(cancel_start_lease.take());
                        let Some(source) = source else {
                            return Some((
                                Err(std::io::Error::new(
                                    std::io::ErrorKind::TimedOut,
                                    "static file body was not polled before its start deadline",
                                )),
                                StaticStreamState::Finished,
                            ));
                        };
                        state = StaticStreamState::Active {
                            receiver: spawn_static_file_producer(
                                source,
                                len,
                                stall_timeout,
                                max_duration,
                            ),
                            remaining: len,
                        };
                    }
                    StaticStreamState::Active {
                        mut receiver,
                        remaining,
                    } => match receiver.recv().await {
                        Some(Ok(bytes)) => {
                            let remaining = remaining.saturating_sub(bytes.len() as u64);
                            return Some((
                                Ok(bytes),
                                StaticStreamState::Active {
                                    receiver,
                                    remaining,
                                },
                            ));
                        }
                        Some(Err(error)) => {
                            return Some((Err(error), StaticStreamState::Finished));
                        }
                        None if remaining > 0 => {
                            return Some((
                                Err(std::io::Error::new(
                                    std::io::ErrorKind::UnexpectedEof,
                                    "static file stream ended before content-length bytes were sent",
                                )),
                                StaticStreamState::Finished,
                            ));
                        }
                        None => return None,
                    },
                    StaticStreamState::Finished => return None,
                }
            }
        },
    )
}

fn spawn_static_file_producer(
    source: PendingStaticStream,
    len: u64,
    stall_timeout: Option<Duration>,
    max_duration: Option<Duration>,
) -> mpsc::Receiver<std::io::Result<Bytes>> {
    let (sender, receiver) = mpsc::channel(STATIC_STREAM_CHANNEL_CAPACITY);
    tokio::spawn(async move {
        let mut source = source;
        let mut remaining = len;
        let now = tokio::time::Instant::now();
        let deadline = max_duration.and_then(|duration| now.checked_add(duration));

        while remaining > 0 {
            if deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
                break;
            }
            let chunk = remaining.min(64 * 1024) as usize;
            let read_task = tokio::task::spawn_blocking(move || {
                let PendingStaticStream { mut file, permit } = source;
                let mut buffer = vec![0u8; chunk];
                let read = file.read(&mut buffer);
                (PendingStaticStream { file, permit }, buffer, read)
            });
            let (next_source, mut buffer, read) = match deadline_remaining(deadline) {
                Some(remaining_time) => match tokio::time::timeout(remaining_time, read_task).await
                {
                    Ok(Ok(read)) => read,
                    // The blocking read retains source (including the permit)
                    // until the OS operation actually returns.
                    Ok(Err(_)) | Err(_) => break,
                },
                None => match read_task.await {
                    Ok(read) => read,
                    Err(_) => break,
                },
            };
            source = next_source;
            let item = match read {
                Ok(0) => Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "static file ended before content-length bytes were read",
                )),
                Err(error) => Err(error),
                Ok(read) => {
                    buffer.truncate(read);
                    remaining -= read as u64;
                    Ok(Bytes::from(buffer))
                }
            };
            let terminal = item.is_err();
            if !send_static_chunk(&sender, item, stall_timeout, deadline).await {
                break;
            }
            if terminal {
                break;
            }
        }
    });
    receiver
}

async fn send_static_chunk(
    sender: &mpsc::Sender<std::io::Result<Bytes>>,
    item: std::io::Result<Bytes>,
    stall_timeout: Option<Duration>,
    deadline: Option<tokio::time::Instant>,
) -> bool {
    if deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
        return false;
    }
    let timeout = match (stall_timeout, deadline_remaining(deadline)) {
        (Some(stall), Some(lifetime)) => Some(stall.min(lifetime)),
        (Some(stall), None) => Some(stall),
        (None, Some(lifetime)) => Some(lifetime),
        (None, None) => None,
    };
    match timeout {
        Some(timeout) => tokio::time::timeout(timeout, sender.send(item))
            .await
            .is_ok_and(|result| result.is_ok()),
        None => sender.send(item).await.is_ok(),
    }
}

fn deadline_remaining(deadline: Option<tokio::time::Instant>) -> Option<Duration> {
    deadline.map(|deadline| deadline.saturating_duration_since(tokio::time::Instant::now()))
}

fn validate_static_options(options: StaticFilesOptions) -> Result<(), RouteError> {
    let now = std::time::Instant::now();
    for (name, timeout) in [
        ("stream_start_timeout", options.stream_start_timeout),
        ("stream_stall_timeout", options.stream_stall_timeout),
        ("stream_max_duration", options.stream_max_duration),
    ] {
        if timeout.is_some_and(|timeout| timeout.is_zero() || now.checked_add(timeout).is_none()) {
            return Err(RouteError::new(
                RouteErrorKind::InvalidStaticOptions,
                format!("{name} debe ser positivo y representable"),
            ));
        }
    }
    Ok(())
}

struct OpenedStaticFile {
    file: std::fs::File,
    relative_path: PathBuf,
    total_len: u64,
    modified: Option<std::time::SystemTime>,
}

fn open_static_file(root: &Dir, mut relative_path: PathBuf) -> std::io::Result<OpenedStaticFile> {
    let mut file = root.open(&relative_path)?;
    let mut metadata = file.metadata()?;
    if metadata.is_dir() {
        relative_path.push("index.html");
        file = root.open(&relative_path)?;
        metadata = file.metadata()?;
    }
    if !metadata.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "la ruta estatica no es un archivo regular",
        ));
    }
    Ok(OpenedStaticFile {
        file: file.into_std(),
        relative_path,
        total_len: metadata.len(),
        modified: metadata.modified().ok().map(|time| time.into_std()),
    })
}

#[derive(Clone, Copy)]
enum StaticPathRejection {
    Hidden,
    Invalid,
}

fn safe_static_relative(
    requested: &str,
    dotfiles: Dotfiles,
) -> Result<PathBuf, StaticPathRejection> {
    let relative = if requested.is_empty() {
        "index.html"
    } else {
        requested
    };

    let mut path = PathBuf::new();
    for component in FsPath::new(relative).components() {
        match component {
            Component::Normal(part) => {
                if dotfiles == Dotfiles::Deny && part.to_string_lossy().starts_with('.') {
                    return Err(StaticPathRejection::Hidden);
                }
                path.push(part);
            }
            Component::CurDir if dotfiles == Dotfiles::Deny => {
                return Err(StaticPathRejection::Hidden);
            }
            Component::CurDir => {}
            Component::Prefix(_) | Component::RootDir | Component::ParentDir => {
                return Err(StaticPathRejection::Invalid);
            }
        }
    }

    Ok(path)
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

#[cfg(test)]
mod static_stream_deadline_tests {
    use super::*;
    use futures_util::StreamExt;

    fn test_file(label: &str) -> (PathBuf, Vec<u8>) {
        let unique = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "rustrest-static-{label}-{}-{unique}.bin",
            std::process::id()
        ));
        let body = vec![b'x'; 4 * 64 * 1024];
        std::fs::write(&path, &body).unwrap();
        (path, body)
    }

    #[tokio::test]
    async fn stalled_static_consumer_releases_the_file_admission_permit() {
        let (path, body) = test_file("stall");
        let file = std::fs::File::open(&path).unwrap();
        let admission = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&admission).acquire_owned().await.unwrap();
        let mut stream = Box::pin(file_stream(
            file,
            body.len() as u64,
            permit,
            Some(Duration::from_secs(1)),
            Some(Duration::from_millis(25)),
            Some(Duration::from_secs(1)),
        ));

        assert_eq!(stream.next().await.unwrap().unwrap().len(), 64 * 1024);
        let reacquired = tokio::time::timeout(
            Duration::from_secs(2),
            Arc::clone(&admission).acquire_owned(),
        )
        .await
        .expect("a stalled response must release static admission")
        .unwrap();
        drop(reacquired);

        let mut saw_deadline_error = false;
        while let Some(chunk) = stream.next().await {
            if let Err(error) = chunk {
                assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
                saw_deadline_error = true;
                break;
            }
        }
        assert!(saw_deadline_error);

        drop(stream);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn entirely_unpolled_static_body_releases_its_start_lease() {
        let (path, body) = test_file("unpolled");
        let file = std::fs::File::open(&path).unwrap();
        let admission = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&admission).acquire_owned().await.unwrap();
        let mut stream = Box::pin(file_stream(
            file,
            body.len() as u64,
            permit,
            Some(Duration::from_millis(25)),
            Some(Duration::from_secs(1)),
            Some(Duration::from_secs(1)),
        ));

        let reacquired = tokio::time::timeout(
            Duration::from_secs(2),
            Arc::clone(&admission).acquire_owned(),
        )
        .await
        .expect("an unpolled response must release static admission")
        .unwrap();
        drop(reacquired);
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().kind(),
            std::io::ErrorKind::TimedOut
        );

        drop(stream);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn outbound_middleware_delay_does_not_consume_the_stall_budget() {
        let (path, body) = test_file("middleware");
        let file = std::fs::File::open(&path).unwrap();
        let admission = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&admission).acquire_owned().await.unwrap();
        let mut stream = Box::pin(file_stream(
            file,
            body.len() as u64,
            permit,
            Some(Duration::from_secs(1)),
            Some(Duration::from_millis(25)),
            Some(Duration::from_secs(1)),
        ));

        tokio::time::sleep(Duration::from_millis(75)).await;
        let mut received = Vec::new();
        while let Some(chunk) = stream.next().await {
            received.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(received, body);

        drop(stream);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn static_stream_deadlines_must_be_positive_and_representable() {
        for options in [
            StaticFilesOptions::new().stream_start_timeout(Duration::ZERO),
            StaticFilesOptions::new().stream_stall_timeout(Duration::ZERO),
            StaticFilesOptions::new().stream_max_duration(Duration::ZERO),
            StaticFilesOptions::new().stream_start_timeout(Duration::MAX),
            StaticFilesOptions::new().stream_stall_timeout(Duration::MAX),
            StaticFilesOptions::new().stream_max_duration(Duration::MAX),
        ] {
            assert_eq!(
                validate_static_options(options).unwrap_err().kind(),
                RouteErrorKind::InvalidStaticOptions
            );
        }
    }
}
