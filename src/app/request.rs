use std::borrow::Cow;
use std::collections::HashMap;
use std::net::SocketAddr;

use hyper::body::Bytes;
use hyper::header::{SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION};
use hyper::upgrade::OnUpgrade;
use serde::de::DeserializeOwned;

use super::{FromRequest, HttpError, StateStore};

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
    /// Raw request body bytes (binary-safe), capped by the server's body limit.
    pub(crate) body: Bytes,
    /// Captured path parameters, e.g. `/users/:id` matching `/users/42`
    /// yields `{"id": "42"}`.
    pub params: HashMap<String, String>,
    pub(crate) state: StateStore,
    pub(crate) upgrade: Option<OnUpgrade>,
    pub(crate) remote_addr: Option<SocketAddr>,
    /// All inbound header (lowercased-name, value) pairs in arrival order,
    /// preserving duplicates that the convenience `headers` map collapses.
    pub(crate) header_pairs: Vec<(String, String)>,
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

    /// The `Last-Event-ID` header an SSE client sends when reconnecting, so
    /// handlers can resume the stream after the last event it received.
    pub fn last_event_id(&self) -> Option<&str> {
        self.header("last-event-id")
    }

    /// Returns the client's socket address, if known (set by the server).
    pub fn remote_addr(&self) -> Option<SocketAddr> {
        self.remote_addr
    }

    /// Returns shared application state by type.
    pub fn state<T>(&self) -> Option<std::sync::Arc<T>>
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

    /// Returns the raw request body bytes (binary-safe).
    pub fn bytes(&self) -> &[u8] {
        &self.body
    }

    /// Returns the request body as a lossy UTF-8 string view.
    pub fn text(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.body)
    }

    /// Deserializes the request body as JSON into `T`.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(&self.body)
    }
}

/// Builds a [`Request`] piece by piece. Header names are lowercased to mirror
/// how the real server normalizes them; a path given as `/x?a=1` is split into
/// path + query automatically.
pub struct RequestBuilder {
    method: String,
    path: String,
    raw_query: Option<String>,
    headers: Vec<(String, String)>,
    cookies: HashMap<String, String>,
    params: HashMap<String, String>,
    state: StateStore,
    body: Bytes,
    remote_addr: Option<SocketAddr>,
}

impl RequestBuilder {
    pub fn new() -> Self {
        Self {
            method: "GET".to_string(),
            path: "/".to_string(),
            raw_query: None,
            headers: Vec::new(),
            cookies: HashMap::new(),
            params: HashMap::new(),
            state: StateStore::default(),
            body: Bytes::new(),
            remote_addr: None,
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
            None => self.path = path.to_string(),
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

    pub fn body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = body.into();
        self
    }

    /// Serializes `value` as the JSON body and sets the content type.
    pub fn json<T: serde::Serialize>(self, value: &T) -> Self {
        let body = serde_json::to_vec(value).unwrap_or_default();
        self.header("content-type", "application/json").body(body)
    }

    pub fn remote_addr(mut self, addr: SocketAddr) -> Self {
        self.remote_addr = Some(addr);
        self
    }

    pub fn build(self) -> Request {
        let query = self
            .raw_query
            .as_deref()
            .map(parse_query)
            .unwrap_or_default();
        let mut headers = HashMap::new();
        let mut cookies = self.cookies;
        for (name, value) in &self.headers {
            if name == "cookie" {
                for (cookie_name, cookie_value) in parse_cookies(value) {
                    cookies.entry(cookie_name).or_insert(cookie_value);
                }
            }
            headers.insert(name.clone(), value.clone());
        }

        Request {
            method: self.method,
            path: self.path,
            raw_query: self.raw_query,
            query,
            headers,
            cookies,
            body: self.body,
            params: self.params,
            state: self.state,
            upgrade: None,
            remote_addr: self.remote_addr,
            header_pairs: self.headers,
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
