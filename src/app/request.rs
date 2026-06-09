use std::collections::HashMap;

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
    /// Request body, decoded as UTF-8 (lossy), capped at 64 KB.
    pub body: String,
    /// Captured path parameters, e.g. `/users/:id` matching `/users/42`
    /// yields `{"id": "42"}`.
    pub params: HashMap<String, String>,
    pub(crate) state: StateStore,
    pub(crate) upgrade: Option<OnUpgrade>,
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

    /// Deserializes the request body as JSON into `T`.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_str(&self.body)
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
