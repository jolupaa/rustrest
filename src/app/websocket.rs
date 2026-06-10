mod config;
mod error;
mod socket;
#[cfg(test)]
mod tests;
mod types;

use hyper::upgrade::OnUpgrade;
use hyper_util::rt::TokioIo;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Role;

use super::{HttpError, Request, Response};

pub(crate) use config::ResolvedWebSocketConfig;
pub use config::{BackpressurePolicy, OriginPolicy, WebSocketConfig};
pub use error::{WebSocketCapacityError, WebSocketError, WebSocketTimeout, WsError};
pub use socket::{
    IntoWebSocketHandler, WebSocket, WebSocketEvent, WebSocketHandler, WebSocketMessage,
};
pub use types::WebSocketErrorCategory;

impl Request {
    pub fn websocket<H>(self, handler: H) -> Response
    where
        H: IntoWebSocketHandler,
    {
        self.websocket_with(WebSocketConfig::default(), handler)
    }

    /// Like [`Request::websocket`], with subprotocol negotiation, message
    /// size limits, and keepalive pings from `config`.
    pub fn websocket_with<H>(self, config: WebSocketConfig, handler: H) -> Response
    where
        H: IntoWebSocketHandler,
    {
        match self.into_websocket_response(config, handler.into_websocket_handler()) {
            Ok(response) => response,
            Err(err) => Response::from_error(err),
        }
    }

    fn into_websocket_response(
        self,
        config: WebSocketConfig,
        handler: WebSocketHandler,
    ) -> Result<Response, HttpError> {
        let protocol = config.negotiate(&self);
        let mut response = Response::websocket(&self)?;
        if let Some(protocol) = &protocol {
            response = response.header("sec-websocket-protocol", protocol);
        }
        let upgrade = self
            .upgrade
            .ok_or_else(|| HttpError::bad_request("WebSocket upgrade is not available"))?;
        spawn_websocket(upgrade, config, protocol, handler);
        Ok(response)
    }
}

fn spawn_websocket(
    upgrade: OnUpgrade,
    config: WebSocketConfig,
    protocol: Option<String>,
    handler: WebSocketHandler,
) {
    let config = ResolvedWebSocketConfig::from_layers(&WebSocketConfig::default(), &config);
    tokio::spawn(async move {
        match upgrade.await {
            Ok(upgraded) => {
                let io = TokioIo::new(upgraded);
                let stream = WebSocketStream::from_raw_socket(
                    io,
                    Role::Server,
                    Some(config.tungstenite_config()),
                )
                .await;
                handler(WebSocket::new(stream, protocol, config.ping_interval)).await;
            }
            Err(err) => {
                eprintln!("WebSocket upgrade failed: {}", err);
            }
        }
    });
}

/// A clonable fan-out channel for WebSocket rooms: handlers `subscribe()`
/// and forward received messages to their socket, while any holder of the
/// `WsBroadcast` can `send` to every current subscriber. Backed by
/// `tokio::sync::broadcast` (lagging subscribers skip the oldest messages).
#[derive(Clone)]
pub struct WsBroadcast {
    sender: tokio::sync::broadcast::Sender<WebSocketMessage>,
}

impl WsBroadcast {
    /// Creates a channel buffering up to `capacity` in-flight messages.
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = tokio::sync::broadcast::channel(capacity);
        Self { sender }
    }

    /// Sends to every current subscriber, returning how many received it.
    pub fn send(&self, message: WebSocketMessage) -> usize {
        self.sender.send(message).unwrap_or(0)
    }

    pub fn send_text(&self, text: &str) -> usize {
        self.send(WebSocketMessage::text(text))
    }

    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<WebSocketMessage> {
        self.sender.subscribe()
    }

    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }
}
