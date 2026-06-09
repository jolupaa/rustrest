#![allow(dead_code)]

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Component, Path as FsPath, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use base64::Engine;
use futures_util::{FutureExt, Stream, StreamExt};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Empty, Full, Limited, StreamBody};
use hyper::HeaderMap;
use hyper::body::{Bytes, Frame, Incoming};
use hyper::header::{
    CACHE_CONTROL, CONNECTION, CONTENT_TYPE, COOKIE, HeaderName, HeaderValue, SEC_WEBSOCKET_ACCEPT,
    SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION, SET_COOKIE, UPGRADE,
};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha1::{Digest, Sha1};
use tokio::net::TcpListener;

mod error;
mod extract;
pub mod middleware;
mod sse;

pub use error::{HttpError, IntoHttpError};
pub use extract::{FromRequest, Json, Path, Query, State};
pub use sse::SseEvent;

/// Maximum request body we will buffer into memory (64 KB).
const MAX_BODY_BYTES: usize = 64 * 1024;
const METHOD_ALL: &str = "*";

type ResponseBody = UnsyncBoxBody<Bytes, Infallible>;
type ResponseStream = Pin<Box<dyn Stream<Item = Result<Frame<Bytes>, Infallible>> + Send>>;

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
pub type ErrorHandler = Arc<dyn Fn(HttpError) -> Response + Send + Sync>;

/// A single segment of a route pattern.
#[derive(Clone)]
enum Segment {
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
fn path_segments(path: &str) -> Vec<&str> {
    path.split('/').filter(|s| !s.is_empty()).collect()
}

/// Parses a route pattern like `/users/:id` into segments.
fn parse_pattern(path: &str) -> Vec<Segment> {
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
fn match_pattern(pattern: &[Segment], segments: &[&str]) -> Option<HashMap<String, String>> {
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

fn decode_component(input: &str, plus_as_space: bool) -> String {
    let bytes = input.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'+' if plus_as_space => {
                decoded.push(b' ');
                index += 1;
            }
            b'%' if index + 2 < bytes.len() => {
                match (hex_value(bytes[index + 1]), hex_value(bytes[index + 2])) {
                    (Some(high), Some(low)) => {
                        decoded.push((high << 4) | low);
                        index += 3;
                    }
                    _ => {
                        decoded.push(bytes[index]);
                        index += 1;
                    }
                }
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }

    String::from_utf8_lossy(&decoded).into_owned()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn parse_query(query: &str) -> HashMap<String, Vec<String>> {
    let mut params = HashMap::new();

    for pair in query.split('&').filter(|part| !part.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_component(key, true);
        let value = decode_component(value, true);
        params.entry(key).or_insert_with(Vec::new).push(value);
    }

    params
}

fn parse_cookies(header: &str) -> HashMap<String, String> {
    let mut cookies = HashMap::new();

    for part in header.split(';') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((name, value)) = part.split_once('=') {
            cookies.insert(name.trim().to_string(), value.trim().to_string());
        }
    }

    cookies
}

#[derive(Clone, Default)]
pub struct StateStore {
    values: Arc<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl StateStore {
    pub fn insert<T>(&mut self, value: T)
    where
        T: Send + Sync + 'static,
    {
        Arc::make_mut(&mut self.values).insert(TypeId::of::<T>(), Arc::new(value));
    }

    pub fn get<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.values
            .get(&TypeId::of::<T>())
            .and_then(|value| Arc::clone(value).downcast::<T>().ok())
    }
}

/// Request data handed to each route handler. Fields are part of the
/// handler-facing API; some demo handlers ignore them.
#[allow(dead_code)]
pub struct Request {
    pub method: String,
    pub path: String,
    /// Raw query string, if any: `/users?id=1` -> `Some("id=1")`.
    pub raw_query: Option<String>,
    /// Parsed query string. Repeated params keep all values in arrival order.
    pub query: HashMap<String, Vec<String>>,
    pub headers: HashMap<String, String>,
    pub cookies: HashMap<String, String>,
    /// Request body, decoded as UTF-8 (lossy), capped at 64 KB.
    pub body: String,
    /// Captured path parameters, e.g. `/users/:id` matching `/users/42`
    /// yields `{"id": "42"}`.
    pub params: HashMap<String, String>,
    state: StateStore,
}

impl Request {
    /// Returns a captured path parameter by name.
    pub fn param(&self, name: &str) -> Option<&str> {
        self.params.get(name).map(String::as_str)
    }

    /// Returns the first parsed query parameter by name.
    pub fn query(&self, name: &str) -> Option<&str> {
        self.query
            .get(name)
            .and_then(|values| values.first())
            .map(String::as_str)
    }

    /// Returns all parsed query parameter values for a repeated key.
    pub fn query_all(&self, name: &str) -> Vec<&str> {
        self.query
            .get(name)
            .map(|values| values.iter().map(String::as_str).collect())
            .unwrap_or_default()
    }

    /// Returns a parsed cookie by name.
    pub fn cookie(&self, name: &str) -> Option<&str> {
        self.cookies.get(name).map(String::as_str)
    }

    /// Returns a request header by name, case-insensitively.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(name)
            .or_else(|| {
                let lower = name.to_ascii_lowercase();
                self.headers.get(&lower)
            })
            .map(String::as_str)
    }

    /// Returns shared application state by type.
    pub fn state<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.state.get::<T>()
    }

    pub fn extract<E>(&self) -> Result<E, HttpError>
    where
        E: FromRequest,
    {
        E::from_request(self)
    }

    pub fn is_websocket_upgrade(&self) -> bool {
        self.method.eq_ignore_ascii_case("GET")
            && self
                .header("upgrade")
                .is_some_and(|value| value.eq_ignore_ascii_case("websocket"))
            && self.header("connection").is_some_and(|value| {
                value
                    .split(',')
                    .any(|part| part.trim().eq_ignore_ascii_case("upgrade"))
            })
            && self.header(SEC_WEBSOCKET_KEY.as_str()).is_some()
            && self.header(SEC_WEBSOCKET_VERSION.as_str()) == Some("13")
    }

    /// Deserializes the request body as JSON into `T`.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_str(&self.body)
    }
}

enum BodyKind {
    Bytes(Bytes),
    Stream(ResponseStream),
    Empty,
}

pub struct Response {
    pub status: u16,
    pub body: String,
    pub content_type: String,
    pub headers: HeaderMap,
    body_kind: BodyKind,
    error: Option<HttpError>,
}

impl Response {
    pub fn send(text: &str) -> Self {
        Self::bytes(Bytes::from(text.to_string()), "text/plain; charset=utf-8")
    }

    pub fn bytes(bytes: Bytes, content_type: impl Into<String>) -> Self {
        Self {
            status: 200,
            body: String::from_utf8_lossy(&bytes).into_owned(),
            content_type: content_type.into(),
            headers: HeaderMap::new(),
            body_kind: BodyKind::Bytes(bytes),
            error: None,
        }
    }

    pub fn stream<S>(stream: S) -> Self
    where
        S: Stream<Item = Result<Bytes, Infallible>> + Send + 'static,
    {
        let frames = stream.map(|chunk| chunk.map(Frame::data));
        Self {
            status: 200,
            body: String::new(),
            content_type: "application/octet-stream".to_string(),
            headers: HeaderMap::new(),
            body_kind: BodyKind::Stream(Box::pin(frames)),
            error: None,
        }
    }

    pub fn sse<S>(events: S) -> Self
    where
        S: Stream<Item = SseEvent> + Send + 'static,
    {
        let chunks = events.map(|event| Ok(Bytes::from(event.format())));
        Self::stream(chunks)
            .content_type("text/event-stream")
            .header(CACHE_CONTROL.as_str(), "no-cache")
            .header(CONNECTION.as_str(), "keep-alive")
    }

    /// Serializes `value` to JSON. If serialization fails, degrades to a 500.
    pub fn json<T: Serialize>(value: &T) -> Self {
        match serde_json::to_string(value) {
            Ok(body) => Self::bytes(Bytes::from(body), "application/json"),
            Err(_) => Self::internal_server_error(),
        }
    }

    pub fn not_found() -> Self {
        Self::bytes(
            Bytes::from_static(b"404 No encontrado"),
            "text/plain; charset=utf-8",
        )
        .status(404)
    }

    pub fn bad_request() -> Self {
        Self::bytes(
            Bytes::from_static(b"400 Peticion incorrecta"),
            "text/plain; charset=utf-8",
        )
        .status(400)
    }

    pub fn internal_server_error() -> Self {
        Self::from_error(HttpError::internal_server_error(
            "Error interno del servidor",
        ))
    }

    pub fn from_error(error: HttpError) -> Self {
        let status = error.status();
        let body = format!("{} {}", status, error.message());
        let mut response =
            Self::bytes(Bytes::from(body), "text/plain; charset=utf-8").status(status);
        response.error = Some(error);
        response
    }

    pub fn redirect(location: &str) -> Self {
        Self::redirect_with_status(location, 302)
    }

    pub fn redirect_with_status(location: &str, status: u16) -> Self {
        Self::send("").status(status).header("location", location)
    }

    pub fn status(mut self, status: u16) -> Self {
        self.status = status;
        self
    }

    pub fn content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.set_header(name, value);
        self
    }

    pub fn append_header(mut self, name: &str, value: &str) -> Self {
        self.add_header(name, value);
        self
    }

    pub fn cookie(self, name: &str, value: &str) -> Self {
        let name = sanitize_cookie_part(name);
        let value = sanitize_cookie_part(value);
        self.append_header(
            SET_COOKIE.as_str(),
            &format!("{}={}; Path=/; HttpOnly", name, value),
        )
    }

    pub fn websocket(req: &Request) -> Result<Self, HttpError> {
        if !req.is_websocket_upgrade() {
            return Err(HttpError::bad_request("Upgrade WebSocket invalido"));
        }

        let key = req
            .header(SEC_WEBSOCKET_KEY.as_str())
            .ok_or_else(|| HttpError::bad_request("Falta Sec-WebSocket-Key"))?;
        let mut hasher = Sha1::new();
        hasher.update(key.as_bytes());
        hasher.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
        let accept = base64::engine::general_purpose::STANDARD.encode(hasher.finalize());

        Ok(Self::send("")
            .status(101)
            .header(UPGRADE.as_str(), "websocket")
            .header(CONNECTION.as_str(), "Upgrade")
            .header(SEC_WEBSOCKET_ACCEPT.as_str(), &accept))
    }

    fn set_header(&mut self, name: &str, value: &str) {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            self.headers.insert(name, value);
        }
    }

    fn add_header(&mut self, name: &str, value: &str) {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            self.headers.append(name, value);
        }
    }

    fn clear_body(&mut self) {
        self.body.clear();
        self.body_kind = BodyKind::Empty;
    }

    pub(super) fn map_body_bytes<F>(&mut self, mapper: F) -> Result<(), HttpError>
    where
        F: FnOnce(&[u8]) -> Result<Vec<u8>, HttpError>,
    {
        if let BodyKind::Bytes(bytes) = &self.body_kind {
            let mapped = Bytes::from(mapper(bytes)?);
            self.body = String::from_utf8_lossy(&mapped).into_owned();
            self.body_kind = BodyKind::Bytes(mapped);
        }
        Ok(())
    }

    fn take_error(&mut self) -> Option<HttpError> {
        self.error.take()
    }

    /// Converts our framework response into a hyper response.
    fn into_hyper(self) -> hyper::Response<ResponseBody> {
        let Response {
            status,
            body: _,
            content_type,
            headers,
            body_kind,
            error: _,
        } = self;

        let hyper_body = match body_kind {
            BodyKind::Bytes(bytes) => Full::new(bytes).boxed_unsync(),
            BodyKind::Stream(stream) => StreamBody::new(stream).boxed_unsync(),
            BodyKind::Empty => Empty::<Bytes>::new().boxed_unsync(),
        };

        let mut builder = hyper::Response::builder().status(status);
        if !headers.contains_key(CONTENT_TYPE) {
            builder = builder.header(CONTENT_TYPE, content_type);
        }
        for (name, value) in &headers {
            builder = builder.header(name, value);
        }

        builder
            .body(hyper_body)
            .expect("status and headers are always valid")
    }
}

fn sanitize_cookie_part(value: &str) -> String {
    value
        .chars()
        .filter(|ch| !matches!(ch, ';' | ',' | '\r' | '\n'))
        .collect()
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

pub trait IntoResponse {
    fn into_response(self) -> Response;
}

impl IntoResponse for Response {
    fn into_response(self) -> Response {
        self
    }
}

impl<E> IntoResponse for Result<Response, E>
where
    E: IntoHttpError,
{
    fn into_response(self) -> Response {
        match self {
            Ok(response) => response,
            Err(err) => {
                let err = err.into_http_error();
                eprintln!("Handler devolvió error: {}", err);
                Response::from_error(err)
            }
        }
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        Response::from_error(self)
    }
}

#[doc(hidden)]
pub struct SyncMarker;
#[doc(hidden)]
pub struct AsyncMarker;

// Synchronous handlers: `|req| Response`.
impl<F, R> IntoHandler<SyncMarker> for F
where
    F: Fn(Request) -> R + Send + Sync + 'static,
    R: IntoResponse + Send + 'static,
{
    fn into_handler(self) -> Handler {
        Arc::new(
            move |req| -> Pin<Box<dyn Future<Output = Response> + Send>> {
                match catch_unwind(AssertUnwindSafe(|| self(req))) {
                    Ok(res) => Box::pin(async move { res.into_response() }),
                    Err(_) => Box::pin(async { panic_response() }),
                }
            },
        )
    }
}

// Asynchronous handlers: `|req| async { Response }`.
impl<F, Fut, R> IntoHandler<AsyncMarker> for F
where
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoResponse + Send + 'static,
{
    fn into_handler(self) -> Handler {
        Arc::new(
            move |req| -> Pin<Box<dyn Future<Output = Response> + Send>> {
                match catch_unwind(AssertUnwindSafe(|| self(req))) {
                    Ok(future) => Box::pin(async move {
                        match AssertUnwindSafe(future).catch_unwind().await {
                            Ok(res) => res.into_response(),
                            Err(_) => panic_response(),
                        }
                    }),
                    Err(_) => Box::pin(async { panic_response() }),
                }
            },
        )
    }
}

fn panic_response() -> Response {
    eprintln!("Un handler o middleware lanzó panic; devolviendo 500.");
    Response::internal_server_error()
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

impl IntoMiddleware for Middleware {
    fn into_middleware(self) -> Middleware {
        self
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

    pub fn patch<H, M>(&mut self, path: &str, handler: H)
    where
        H: IntoHandler<M>,
    {
        self.add("PATCH", path, handler);
    }

    pub fn options<H, M>(&mut self, path: &str, handler: H)
    where
        H: IntoHandler<M>,
    {
        self.add("OPTIONS", path, handler);
    }

    pub fn head<H, M>(&mut self, path: &str, handler: H)
    where
        H: IntoHandler<M>,
    {
        self.add("HEAD", path, handler);
    }

    pub fn all<H, M>(&mut self, path: &str, handler: H)
    where
        H: IntoHandler<M>,
    {
        self.add(METHOD_ALL, path, handler);
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
                    Response::from_error(HttpError::forbidden("Acceso denegado"))
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
    fn route(
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
        P: Into<PathBuf>,
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

        println!("Servidor escuchando en http://{}", address);
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

                if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                    eprintln!("Error sirviendo la conexión: {:?}", err);
                }
            });
        }
    }

    /// Translates a hyper request into a [`Request`] (method, path, query,
    /// headers, size-bounded body), dispatches it, and converts the result.
    async fn handle(&self, req: hyper::Request<Incoming>) -> hyper::Response<ResponseBody> {
        // Read everything that only needs a borrow before consuming the body.
        let method = req.method().as_str().to_string();
        let path = req.uri().path().to_string();
        let raw_query = req.uri().query().map(|q| q.to_string());
        let query = raw_query.as_deref().map(parse_query).unwrap_or_default();
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
        };

        self.dispatch(request).await.into_hyper()
    }

    /// Routes the request (capturing path params), then runs it through the
    /// middleware onion ending at the matched handler (or a 404 handler).
    async fn dispatch(&self, mut request: Request) -> Response {
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

#[cfg(test)]
mod tests;
