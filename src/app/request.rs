use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

use hyper::body::Bytes;
use hyper::header::{HeaderName, HeaderValue, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION};
use hyper::upgrade::OnUpgrade;
use serde::de::DeserializeOwned;

use super::body::DEFAULT_BODY_LIMIT;
use super::websocket::ResolvedWebSocketConfig;
use super::{
    BodyStream, FromRequest, FromRequestParts, HttpError, RequestBody, StateStore,
    WebSocketRuntimeHandle,
};

/// Request data handed to each route handler. Fields are part of the
/// handler-facing API; some demo handlers ignore them.
#[allow(dead_code)]
pub struct Request {
    pub(crate) version: hyper::Version,
    pub method: String,
    pub path: String,
    /// Raw query string, if any: `/users?id=1` -> `Some("id=1")`.
    pub raw_query: Option<String>,
    /// Parsed query string. Repeated params keep all values in arrival order.
    pub query: HashMap<String, Vec<String>>,
    // Kept private so middleware cannot mutate the collapsed map without also
    // updating `header_pairs`, which backs security-sensitive typed and
    // duplicate-preserving header access.
    pub(crate) headers: HashMap<String, String>,
    // Derived from Cookie fields; private for the same synchronization reason
    // as the header views.
    pub(crate) cookies: HashMap<String, String>,
    pub(crate) body: RequestBody,
    pub(crate) body_limit: usize,
    /// Captured path parameters, e.g. `/users/:id` matching `/users/42`
    /// yields `{"id": "42"}`.
    pub params: HashMap<String, String>,
    pub(crate) route_pattern: Option<String>,
    pub(crate) websocket_runtime: WebSocketRuntimeHandle,
    pub(crate) resolved_websocket_config: Option<ResolvedWebSocketConfig>,
    pub(crate) state: StateStore,
    pub(crate) extensions: StateStore,
    pub(crate) upgrade: Option<OnUpgrade>,
    pub(crate) remote_addr: Option<SocketAddr>,
    pub(crate) secure_transport: bool,
    /// All inbound header (lowercased-name, value) pairs in arrival order,
    /// preserving duplicates that the convenience `headers` map collapses.
    pub(crate) header_pairs: Vec<(String, String)>,
    /// Set only by the `Sessions` middleware; never derived from input.
    pub(crate) session_id: Option<String>,
}

impl Request {
    /// Starts building a `Request` by hand — the entry point for unit-testing
    /// handlers and middleware without a TCP connection.
    pub fn builder() -> RequestBuilder {
        RequestBuilder::new()
    }

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

    /// Returns the parsed request cookies as a read-only map.
    pub fn cookies(&self) -> &HashMap<String, String> {
        &self.cookies
    }

    /// Inserts or replaces one parsed request cookie and synchronizes the
    /// `Cookie` header seen by raw and typed header accessors.
    pub fn set_cookie(&mut self, name: &str, value: &str) -> Result<(), HttpError> {
        if !valid_request_cookie_name(name) || !valid_request_cookie_value(value) {
            return Err(HttpError::bad_request(
                "El nombre o valor de la cookie de solicitud no es valido",
            ));
        }
        self.cookies.insert(name.to_string(), value.to_string());
        self.rewrite_cookie_header();
        Ok(())
    }

    /// Removes a parsed request cookie and synchronizes the `Cookie` header.
    pub fn remove_cookie(&mut self, name: &str) {
        self.cookies.remove(name);
        self.rewrite_cookie_header();
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

    /// Replaces every occurrence of a request header with one validated value.
    ///
    /// This keeps [`Request::header`], [`Request::headers_all`] and typed
    /// header extractors synchronized. It is the safe way for middleware to
    /// rewrite inbound headers.
    pub fn set_header(&mut self, name: &str, value: &str) -> Result<(), HttpError> {
        let name = parse_header_name(name)?;
        validate_header_value(value)?;
        let normalized = name.as_str().to_string();

        self.headers.insert(normalized.clone(), value.to_string());
        self.header_pairs
            .retain(|(existing, _)| !existing.eq_ignore_ascii_case(&normalized));
        self.header_pairs.push((normalized, value.to_string()));
        if name == hyper::header::COOKIE {
            self.rebuild_cookie_view();
        }
        Ok(())
    }

    /// Appends a validated request-header value while retaining earlier
    /// occurrences for duplicate-aware and typed accessors.
    ///
    /// The convenience [`Request::header`] view returns the most recently
    /// appended value.
    pub fn append_header(&mut self, name: &str, value: &str) -> Result<(), HttpError> {
        let name = parse_header_name(name)?;
        validate_header_value(value)?;
        let normalized = name.as_str().to_string();

        self.headers.insert(normalized.clone(), value.to_string());
        self.header_pairs.push((normalized, value.to_string()));
        if name == hyper::header::COOKIE {
            self.rebuild_cookie_view();
        }
        Ok(())
    }

    /// Removes every occurrence of a request header from all header views.
    pub fn remove_header(&mut self, name: &str) {
        self.headers
            .retain(|existing, _| !existing.eq_ignore_ascii_case(name));
        self.header_pairs
            .retain(|(existing, _)| !existing.eq_ignore_ascii_case(name));
        if name.eq_ignore_ascii_case(hyper::header::COOKIE.as_str()) {
            self.cookies.clear();
        }
    }

    /// Returns the collapsed, lowercased request-header map.
    ///
    /// Repeated values are available through [`Request::headers_all`].
    pub fn headers(&self) -> &HashMap<String, String> {
        &self.headers
    }

    fn rebuild_cookie_view(&mut self) {
        self.cookies.clear();
        for (_, value) in self
            .header_pairs
            .iter()
            .filter(|(name, _)| name.eq_ignore_ascii_case(hyper::header::COOKIE.as_str()))
        {
            self.cookies.extend(parse_cookies(value));
        }
    }

    fn rewrite_cookie_header(&mut self) {
        self.headers.remove(hyper::header::COOKIE.as_str());
        self.header_pairs
            .retain(|(name, _)| !name.eq_ignore_ascii_case(hyper::header::COOKIE.as_str()));
        if self.cookies.is_empty() {
            return;
        }

        let mut cookies = self.cookies.iter().collect::<Vec<_>>();
        cookies.sort_unstable_by_key(|(name, _)| *name);
        let value = cookies
            .into_iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("; ");
        self.headers
            .insert(hyper::header::COOKIE.as_str().to_string(), value.clone());
        self.header_pairs
            .push((hyper::header::COOKIE.as_str().to_string(), value));
    }

    /// Returns all values for a request header, case-insensitively, in arrival
    /// order. Preserves duplicates (e.g. multiple `X-Forwarded-For`) that the
    /// `header()`/`headers` convenience view collapses to a single value.
    pub fn headers_all(&self, name: &str) -> Vec<&str> {
        self.header_pairs
            .iter()
            .filter(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
            .collect()
    }

    /// Returns a field that must occur at most once, rejecting ambiguous
    /// duplicates instead of silently choosing the first or last value.
    pub fn singleton_header(&self, name: &str) -> Result<Option<&str>, HttpError> {
        let values = self.headers_all(name);
        match values.as_slice() {
            [] => Ok(self.header(name)),
            [value] => Ok(Some(*value)),
            _ => Err(HttpError::duplicate_header(name)),
        }
    }

    /// The `Last-Event-ID` header an SSE client sends when reconnecting, so
    /// handlers can resume the stream after the last event it received.
    pub fn last_event_id(&self) -> Option<&str> {
        self.header("last-event-id")
    }

    /// Returns the client's socket address, if known (set by the server).
    pub fn remote_addr(&self) -> Option<SocketAddr> {
        self.remote_addr
    }

    /// Returns the HTTP version used by the client request.
    pub fn version(&self) -> hyper::Version {
        self.version
    }

    /// Returns whether the request arrived over a secure transport.
    pub fn is_secure(&self) -> bool {
        self.secure_transport
    }

    pub(crate) fn route_pattern(&self) -> Option<&str> {
        self.route_pattern.as_deref()
    }

    pub(crate) fn set_body_limit(&mut self, limit: usize) {
        self.body_limit = limit;
        self.body.set_default_limit(limit);
    }

    pub(crate) fn validate_content_length(&self) -> Result<(), HttpError> {
        let mut content_length: Option<u64> = None;
        let values = self.headers_all("content-length");
        let values = if values.is_empty() {
            self.header("content-length").into_iter().collect()
        } else {
            values
        };

        if !values.is_empty() && self.header("transfer-encoding").is_some() {
            return Err(HttpError::new(
                hyper::StatusCode::BAD_REQUEST,
                "ambiguous_message_framing",
                "Transfer-Encoding y Content-Length no pueden combinarse",
            ));
        }

        for raw_value in values {
            for raw_part in raw_value.split(',') {
                let value = raw_part.trim();
                if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(HttpError::invalid_content_length());
                }

                let value = value
                    .parse::<u64>()
                    .map_err(|_| HttpError::invalid_content_length())?;
                match content_length {
                    Some(previous) if previous != value => {
                        return Err(HttpError::invalid_content_length());
                    }
                    Some(_) => {}
                    None => content_length = Some(value),
                }
            }
        }

        // Hyper's HTTP/1 parser rejects malformed and conflicting values before
        // service_fn. This validation covers TestClient and transports that
        // preserve repeated fields or comma lists for framework-level handling.
        if let Some(content_length) = content_length {
            if content_length > self.body_limit as u64 {
                return Err(HttpError::payload_too_large_limit(self.body_limit));
            }
            // An in-process buffered request is already complete. Accepting a
            // declared length that differs from those bytes would let
            // TestClient exercise a request a real HTTP transport cannot
            // dispatch as a complete framed message.
            if self
                .body
                .buffered_len()
                .is_some_and(|actual| content_length != actual as u64)
            {
                return Err(HttpError::invalid_content_length());
            }
        }

        Ok(())
    }

    /// Returns shared application state by type.
    pub fn state<T>(&self) -> Option<std::sync::Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.state.get::<T>()
    }

    /// Returns request-local extension data by type.
    pub fn extension<T>(&self) -> Option<std::sync::Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.extensions.get::<T>()
    }

    /// Stores request-local extension data by type.
    pub fn insert_extension<T>(&mut self, value: T)
    where
        T: Send + Sync + 'static,
    {
        self.extensions.insert(value);
    }

    pub fn parts_mut(&mut self) -> RequestParts<'_> {
        RequestParts { request: self }
    }

    pub async fn extract<E>(&mut self) -> Result<E, E::Rejection>
    where
        E: FromRequest,
    {
        E::from_request(self).await
    }

    pub async fn extract_parts<E>(&mut self) -> Result<E, E::Rejection>
    where
        E: FromRequestParts,
    {
        let mut parts = self.parts_mut();
        E::from_request_parts(&mut parts).await
    }

    pub fn is_websocket_upgrade(&self) -> bool {
        let Some(host) = super::websocket::singleton_header(self, "host") else {
            return false;
        };
        let Some(key) = super::websocket::singleton_header(self, SEC_WEBSOCKET_KEY.as_str()) else {
            return false;
        };
        let Some(version) =
            super::websocket::singleton_header(self, SEC_WEBSOCKET_VERSION.as_str())
        else {
            return false;
        };

        self.version == hyper::Version::HTTP_11
            && self.method.eq_ignore_ascii_case("GET")
            && !host.trim().is_empty()
            && super::websocket::request_header_contains_token(self, "upgrade", "websocket")
            && super::websocket::request_header_contains_token(self, "connection", "upgrade")
            && super::websocket::is_valid_websocket_key(key)
            && version.trim() == "13"
    }

    /// Returns the request body abstraction.
    pub fn body(&self) -> &RequestBody {
        &self.body
    }

    /// Returns the mutable request body abstraction.
    pub fn body_mut(&mut self) -> &mut RequestBody {
        &mut self.body
    }

    /// Takes the one-shot request body stream. The effective application or
    /// route body limit is enforced incrementally across its chunks.
    pub fn take_body_stream(&mut self) -> Result<BodyStream, HttpError> {
        self.body.take_stream()
    }

    /// Collects the request body as binary-safe bytes.
    pub async fn bytes(&mut self) -> Result<Bytes, HttpError> {
        self.body.collect(self.body_limit).await
    }

    /// Collects the request body as strict UTF-8 text.
    pub async fn text(&mut self) -> Result<String, HttpError> {
        String::from_utf8(self.bytes().await?.to_vec())
            .map_err(|error| HttpError::invalid_utf8().with_source(error))
    }

    /// Collects the request body as lossy UTF-8 text.
    pub async fn text_lossy(&mut self) -> Result<String, HttpError> {
        Ok(String::from_utf8_lossy(&self.bytes().await?).into_owned())
    }

    /// Collects and deserializes the request body as JSON into `T`.
    pub async fn json<T: DeserializeOwned>(&mut self) -> Result<T, HttpError> {
        serde_json::from_slice(&self.bytes().await?)
            .map_err(|error| HttpError::invalid_json().with_source(error))
    }
}

fn parse_header_name(name: &str) -> Result<HeaderName, HttpError> {
    HeaderName::try_from(name).map_err(|error| {
        HttpError::bad_request("El nombre del encabezado de solicitud no es valido")
            .with_source(error)
    })
}

fn validate_header_value(value: &str) -> Result<(), HttpError> {
    HeaderValue::try_from(value).map(|_| ()).map_err(|error| {
        HttpError::bad_request("El valor del encabezado de solicitud no es valido")
            .with_source(error)
    })
}

fn valid_request_cookie_name(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

fn valid_request_cookie_value(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| matches!(byte, 0x21 | 0x23..=0x2b | 0x2d..=0x3a | 0x3c..=0x5b | 0x5d..=0x7e))
}

pub struct RequestParts<'a> {
    pub(crate) request: &'a mut Request,
}

impl RequestParts<'_> {
    pub fn method(&self) -> &str {
        &self.request.method
    }

    pub fn path(&self) -> &str {
        &self.request.path
    }

    pub fn raw_query(&self) -> Option<&str> {
        self.request.raw_query.as_deref()
    }

    pub fn version(&self) -> hyper::Version {
        self.request.version
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.request.header(name)
    }

    pub fn headers_all(&self, name: &str) -> Vec<&str> {
        self.request.headers_all(name)
    }

    pub fn cookies(&self) -> &HashMap<String, String> {
        &self.request.cookies
    }

    pub fn headers(&self) -> &HashMap<String, String> {
        &self.request.headers
    }

    pub fn params(&self) -> &HashMap<String, String> {
        &self.request.params
    }

    pub fn state<T>(&self) -> Option<std::sync::Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.request.state::<T>()
    }

    pub fn extension<T>(&self) -> Option<std::sync::Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.request.extensions.get::<T>()
    }

    pub fn remote_addr(&self) -> Option<SocketAddr> {
        self.request.remote_addr
    }

    pub fn is_secure(&self) -> bool {
        self.request.secure_transport
    }

    pub fn matched_path(&self) -> Option<&str> {
        self.request.route_pattern.as_deref()
    }

    pub fn original_uri(&self) -> hyper::Uri {
        let mut uri = self.request.path.clone();
        if let Some(query) = &self.request.raw_query {
            uri.push('?');
            uri.push_str(query);
        }
        uri.parse().unwrap_or_else(|_| hyper::Uri::from_static("/"))
    }
}

/// Builds a [`Request`] piece by piece. Header names are lowercased to mirror
/// how the real server normalizes them; a path given as `/x?a=1` is split into
/// path + query automatically.
pub struct RequestBuilder {
    version: hyper::Version,
    method: String,
    path: String,
    raw_query: Option<String>,
    headers: Vec<(String, String)>,
    cookies: HashMap<String, String>,
    params: HashMap<String, String>,
    route_pattern: Option<String>,
    state: StateStore,
    extensions: StateStore,
    body: Bytes,
    body_limit: usize,
    remote_addr: Option<SocketAddr>,
    secure_transport: bool,
}

impl RequestBuilder {
    pub fn new() -> Self {
        Self {
            version: hyper::Version::HTTP_11,
            method: "GET".to_string(),
            path: "/".to_string(),
            raw_query: None,
            headers: Vec::new(),
            cookies: HashMap::new(),
            params: HashMap::new(),
            route_pattern: None,
            state: StateStore::default(),
            extensions: StateStore::default(),
            body: Bytes::new(),
            body_limit: DEFAULT_BODY_LIMIT,
            remote_addr: None,
            secure_transport: false,
        }
    }

    pub fn method(mut self, method: &str) -> Self {
        self.method = method.to_ascii_uppercase();
        self
    }

    /// Sets the request path. A query string may be included (`/x?a=1`).
    pub fn path(mut self, path: &str) -> Self {
        match path.split_once('?') {
            Some((path, query)) => {
                self.path = path.to_string();
                self.raw_query = Some(query.to_string());
            }
            None => {
                self.path = path.to_string();
                self.raw_query = None;
            }
        }
        self
    }

    /// Sets the raw query string (without the leading `?`).
    pub fn query(mut self, raw_query: &str) -> Self {
        self.raw_query = Some(raw_query.to_string());
        self
    }

    /// Appends a header. Repeated names are all kept (see `headers_all`).
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers
            .push((name.to_ascii_lowercase(), value.to_string()));
        self
    }

    pub fn cookie(mut self, name: &str, value: &str) -> Self {
        self.cookies.insert(name.to_string(), value.to_string());
        self
    }

    /// Sets a captured path parameter, as routing would have.
    pub fn param(mut self, name: &str, value: &str) -> Self {
        self.params.insert(name.to_string(), value.to_string());
        self
    }

    /// Inserts a value into the request's state store (one per type).
    pub fn state<T>(mut self, value: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        self.state.insert(value);
        self
    }

    pub fn extension<T>(mut self, value: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        self.extensions.insert(value);
        self
    }

    pub fn matched_path(self, pattern: &str) -> Self {
        self.route_pattern(pattern)
    }

    pub fn route_pattern(mut self, pattern: &str) -> Self {
        self.route_pattern = Some(pattern.to_string());
        self
    }

    pub fn body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = body.into();
        self
    }

    /// Sets the collection limit used by body-reading request methods.
    pub fn body_limit(mut self, limit: usize) -> Self {
        self.body_limit = limit;
        self
    }

    /// Serializes `value` as the JSON body and sets the content type.
    ///
    /// Panics if serialization fails, which keeps hand-built tests from
    /// silently sending a different empty payload. Use [`Self::try_json`] for
    /// fallible construction.
    pub fn json<T: serde::Serialize>(self, value: &T) -> Self {
        self.try_json(value)
            .expect("request JSON serialization failed")
    }

    /// Fallible variant of [`Self::json`].
    pub fn try_json<T: serde::Serialize>(self, value: &T) -> Result<Self, serde_json::Error> {
        let body = serde_json::to_vec(value)?;
        Ok(self.header("content-type", "application/json").body(body))
    }

    pub fn remote_addr(mut self, addr: SocketAddr) -> Self {
        self.remote_addr = Some(addr);
        self
    }

    pub fn secure(mut self, secure: bool) -> Self {
        self.secure_transport = secure;
        self
    }

    pub fn build(self) -> Request {
        let query = self
            .raw_query
            .as_deref()
            .map(parse_query)
            .unwrap_or_default();
        let mut explicit_cookie_names = HashSet::new();
        for (name, value) in &self.headers {
            if name == hyper::header::COOKIE.as_str() {
                explicit_cookie_names.extend(parse_cookies(value).into_keys());
            }
        }
        let mut synthetic_cookies = self
            .cookies
            .into_iter()
            .filter(|(name, _)| !explicit_cookie_names.contains(name))
            .collect::<Vec<_>>();
        synthetic_cookies.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));

        let mut header_pairs = self.headers;
        if !synthetic_cookies.is_empty() {
            let value = synthetic_cookies
                .into_iter()
                .map(|(name, value)| format!("{name}={value}"))
                .collect::<Vec<_>>()
                .join("; ");
            // Place synthesized cookies before explicit Cookie fields so every
            // explicit name retains last-value-wins precedence and duplicate
            // explicit field lines remain untouched.
            header_pairs.insert(0, (hyper::header::COOKIE.as_str().to_string(), value));
        }

        let mut headers = HashMap::new();
        let mut cookies = HashMap::new();
        for (name, value) in &header_pairs {
            if name == hyper::header::COOKIE.as_str() {
                for (cookie_name, cookie_value) in parse_cookies(value) {
                    // RFC Cookie fields are processed in arrival order. Match
                    // the network path by letting the last duplicate name win.
                    cookies.insert(cookie_name, cookie_value);
                }
            }
            headers.insert(name.clone(), value.clone());
        }

        Request {
            version: self.version,
            method: self.method,
            path: self.path,
            raw_query: self.raw_query,
            query,
            headers,
            cookies,
            body: RequestBody::buffered(self.body, self.body_limit),
            body_limit: self.body_limit,
            params: self.params,
            route_pattern: self.route_pattern,
            websocket_runtime: WebSocketRuntimeHandle::local(),
            resolved_websocket_config: None,
            state: self.state,
            extensions: self.extensions,
            upgrade: None,
            remote_addr: self.remote_addr,
            secure_transport: self.secure_transport,
            header_pairs,
            session_id: None,
        }
    }
}

impl Default for RequestBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn decode_component(input: &str, plus_as_space: bool) -> String {
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

pub(crate) fn parse_query(query: &str) -> HashMap<String, Vec<String>> {
    let mut params = HashMap::new();

    for pair in query.split('&').filter(|part| !part.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_component(key, true);
        let value = decode_component(value, true);
        params.entry(key).or_insert_with(Vec::new).push(value);
    }

    params
}

pub(crate) fn parse_cookies(header: &str) -> HashMap<String, String> {
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
