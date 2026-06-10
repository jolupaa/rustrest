use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use hyper::upgrade::OnUpgrade;
use hyper_util::rt::TokioIo;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Role;

use super::{HttpError, Request, Response};

pub use tokio_tungstenite::tungstenite::Message as WebSocketMessage;

/// Per-route WebSocket options: subprotocol negotiation, incoming message
/// size limit, and automatic keepalive pings. Pass to
/// [`Request::websocket_with`] or `websocket_with` on `App`/`Router`.
#[derive(Clone, Default)]
pub struct WebSocketConfig {
    protocols: Vec<String>,
    max_message_size: Option<usize>,
    ping_interval: Option<Duration>,
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
    /// [`WebSocket::recv`] for `interval`, keeping intermediaries from
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

type WebSocketInner = WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>;

pub type WebSocketHandler =
    Arc<dyn Fn(WebSocket) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

pub trait IntoWebSocketHandler {
    fn into_websocket_handler(self) -> WebSocketHandler;
}

impl<F, Fut> IntoWebSocketHandler for F
where
    F: Fn(WebSocket) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    fn into_websocket_handler(self) -> WebSocketHandler {
        Arc::new(move |socket| Box::pin(self(socket)))
    }
}

impl IntoWebSocketHandler for WebSocketHandler {
    fn into_websocket_handler(self) -> WebSocketHandler {
        self
    }
}

#[derive(Debug)]
pub enum WebSocketError {
    Protocol(tokio_tungstenite::tungstenite::Error),
    Json(serde_json::Error),
}

impl std::fmt::Display for WebSocketError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WebSocketError::Protocol(err) => write!(f, "websocket protocol error: {}", err),
            WebSocketError::Json(err) => write!(f, "websocket JSON error: {}", err),
        }
    }
}

impl std::error::Error for WebSocketError {}

impl From<tokio_tungstenite::tungstenite::Error> for WebSocketError {
    fn from(value: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::Protocol(value)
    }
}

impl From<serde_json::Error> for WebSocketError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebSocketEvent<T = serde_json::Value> {
    pub event: String,
    pub data: T,
}

pub struct WebSocket {
    inner: WebSocketInner,
    protocol: Option<String>,
    ping_interval: Option<Duration>,
}

impl WebSocket {
    pub(super) fn new(
        inner: WebSocketInner,
        protocol: Option<String>,
        ping_interval: Option<Duration>,
    ) -> Self {
        Self {
            inner,
            protocol,
            ping_interval,
        }
    }

    /// The subprotocol negotiated during the handshake, if any.
    pub fn protocol(&self) -> Option<&str> {
        self.protocol.as_deref()
    }

    pub async fn recv(&mut self) -> Result<Option<WebSocketMessage>, WebSocketError> {
        loop {
            let next = match self.ping_interval {
                Some(interval) => match tokio::time::timeout(interval, self.inner.next()).await {
                    Ok(next) => next,
                    Err(_) => {
                        // Idle for a full interval: ping and keep waiting.
                        self.ping(hyper::body::Bytes::new()).await?;
                        continue;
                    }
                },
                None => self.inner.next().await,
            };
            return match next {
                Some(Ok(message)) => Ok(Some(message)),
                Some(Err(err)) => Err(err.into()),
                None => Ok(None),
            };
        }
    }

    pub async fn send(&mut self, message: WebSocketMessage) -> Result<(), WebSocketError> {
        self.inner.send(message).await.map_err(Into::into)
    }

    pub async fn send_text(&mut self, text: &str) -> Result<(), WebSocketError> {
        self.send(WebSocketMessage::text(text.to_string())).await
    }

    pub async fn send_binary(
        &mut self,
        bytes: impl Into<hyper::body::Bytes>,
    ) -> Result<(), WebSocketError> {
        self.send(WebSocketMessage::binary(bytes.into())).await
    }

    pub async fn send_json<T>(&mut self, value: &T) -> Result<(), WebSocketError>
    where
        T: Serialize,
    {
        let text = serde_json::to_string(value)?;
        self.send_text(&text).await
    }

    pub async fn recv_json<T>(&mut self) -> Result<Option<T>, WebSocketError>
    where
        T: DeserializeOwned,
    {
        match self.recv().await? {
            Some(message) if message.is_text() || message.is_binary() => {
                Ok(Some(serde_json::from_slice(&message.into_data())?))
            }
            Some(_) => Ok(None),
            None => Ok(None),
        }
    }

    pub async fn send_event<T>(&mut self, event: &str, data: &T) -> Result<(), WebSocketError>
    where
        T: Serialize,
    {
        self.send_json(&WebSocketEvent {
            event: event.to_string(),
            data,
        })
        .await
    }

    pub async fn recv_event<T>(&mut self) -> Result<Option<WebSocketEvent<T>>, WebSocketError>
    where
        T: DeserializeOwned,
    {
        self.recv_json::<WebSocketEvent<T>>().await
    }

    pub async fn ping(
        &mut self,
        payload: impl Into<hyper::body::Bytes>,
    ) -> Result<(), WebSocketError> {
        self.send(WebSocketMessage::Ping(payload.into())).await
    }

    pub async fn pong(
        &mut self,
        payload: impl Into<hyper::body::Bytes>,
    ) -> Result<(), WebSocketError> {
        self.send(WebSocketMessage::Pong(payload.into())).await
    }

    pub async fn close(&mut self) -> Result<(), WebSocketError> {
        self.send(WebSocketMessage::Close(None)).await
    }
}

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
    tokio::spawn(async move {
        match upgrade.await {
            Ok(upgraded) => {
                let io = TokioIo::new(upgraded);
                let stream_config = config.max_message_size.map(|bytes| {
                    tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
                        .max_message_size(Some(bytes))
                });
                let stream =
                    WebSocketStream::from_raw_socket(io, Role::Server, stream_config).await;
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
