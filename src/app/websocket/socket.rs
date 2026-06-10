use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use hyper_util::rt::TokioIo;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio_tungstenite::WebSocketStream;

use super::WebSocketError;

pub use super::types::{WebSocketEvent, WebSocketMessage};

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
