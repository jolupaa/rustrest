use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use hyper::upgrade::OnUpgrade;
use hyper_util::rt::TokioIo;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Role;

use super::{HttpError, Request, Response};

pub use tokio_tungstenite::tungstenite::Message as WebSocketMessage;

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
}

impl WebSocket {
    pub(super) fn new(inner: WebSocketInner) -> Self {
        Self { inner }
    }

    pub async fn recv(&mut self) -> Result<Option<WebSocketMessage>, WebSocketError> {
        match self.inner.next().await {
            Some(Ok(message)) => Ok(Some(message)),
            Some(Err(err)) => Err(err.into()),
            None => Ok(None),
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
        match self.into_websocket_response(handler.into_websocket_handler()) {
            Ok(response) => response,
            Err(err) => Response::from_error(err),
        }
    }

    fn into_websocket_response(self, handler: WebSocketHandler) -> Result<Response, HttpError> {
        let response = Response::websocket(&self)?;
        let upgrade = self
            .upgrade
            .ok_or_else(|| HttpError::bad_request("WebSocket upgrade is not available"))?;
        spawn_websocket(upgrade, handler);
        Ok(response)
    }
}

fn spawn_websocket(upgrade: OnUpgrade, handler: WebSocketHandler) {
    tokio::spawn(async move {
        match upgrade.await {
            Ok(upgraded) => {
                let io = TokioIo::new(upgraded);
                let stream = WebSocketStream::from_raw_socket(io, Role::Server, None).await;
                handler(WebSocket::new(stream)).await;
            }
            Err(err) => {
                eprintln!("WebSocket upgrade failed: {}", err);
            }
        }
    });
}
