use std::time::Duration;

use super::Request;

/// Per-route WebSocket options: subprotocol negotiation, incoming message
/// size limit, and automatic keepalive pings. Pass to
/// [`Request::websocket_with`] or `websocket_with` on `App`/`Router`.
#[derive(Clone, Default)]
pub struct WebSocketConfig {
    protocols: Vec<String>,
    pub(super) max_message_size: Option<usize>,
    pub(super) ping_interval: Option<Duration>,
}

impl WebSocketConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Declares the subprotocols the server supports. The first
    /// client-offered protocol in this list is selected and echoed in the
    /// `Sec-WebSocket-Protocol` response header.
    pub fn protocols(mut self, protocols: &[&str]) -> Self {
        self.protocols = protocols.iter().map(|p| p.to_string()).collect();
        self
    }

    /// Caps the size of incoming messages; larger ones error the connection.
    pub fn max_message_size(mut self, bytes: usize) -> Self {
        self.max_message_size = Some(bytes);
        self
    }

    /// Sends a Ping whenever the connection has been idle in
    /// [`WebSocket::recv`](super::WebSocket::recv) for `interval`, keeping intermediaries from
    /// dropping quiet connections.
    pub fn ping_interval(mut self, interval: Duration) -> Self {
        self.ping_interval = Some(interval);
        self
    }

    /// Picks the first client-offered subprotocol the server supports.
    pub(super) fn negotiate(&self, req: &Request) -> Option<String> {
        for raw in req.headers_all("sec-websocket-protocol") {
            for candidate in raw.split(',') {
                let candidate = candidate.trim();
                if self
                    .protocols
                    .iter()
                    .any(|supported| supported.eq_ignore_ascii_case(candidate))
                {
                    return Some(candidate.to_string());
                }
            }
        }
        None
    }
}
