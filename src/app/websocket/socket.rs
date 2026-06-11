use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use hyper::body::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::{mpsc, watch};

use super::WebSocketError;
use super::driver::{CONTROL_CHANNEL_CAPACITY, ControlCommand, DriverChannels, OutboundCommand};
use super::types::{WebSocketCloseInfo, WebSocketId};

pub use super::types::{WebSocketEvent, WebSocketMessage};

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
    receiver: WebSocketReceiver,
    sender: WebSocketSender,
}

#[derive(Clone)]
pub struct WebSocketSender {
    shared: Arc<SocketShared>,
}

pub struct WebSocketReceiver {
    inbound: mpsc::Receiver<Result<WebSocketMessage, WebSocketError>>,
    close_rx: watch::Receiver<Option<WebSocketCloseInfo>>,
}

#[derive(Clone)]
pub(crate) struct InternalWebSocketSender {
    _shared: Arc<SocketShared>,
}

struct SocketShared {
    id: WebSocketId,
    remote_addr: Option<SocketAddr>,
    route: String,
    protocol: Option<String>,
    outbound: mpsc::Sender<OutboundCommand>,
    control: mpsc::Sender<ControlCommand>,
}

pub(crate) struct SocketMetadata {
    pub id: WebSocketId,
    pub remote_addr: Option<SocketAddr>,
    pub route: String,
    pub protocol: Option<String>,
}

pub(crate) fn channel_pair(
    metadata: SocketMetadata,
    inbound_capacity: usize,
    outbound_capacity: usize,
) -> (WebSocket, InternalWebSocketSender, DriverChannels) {
    let (inbound_tx, inbound) = mpsc::channel(inbound_capacity);
    let (outbound, outbound_rx) = mpsc::channel(outbound_capacity);
    let (control, control_rx) = mpsc::channel(CONTROL_CHANNEL_CAPACITY);
    let (close_tx, close_rx) = watch::channel(None);
    let shared = Arc::new(SocketShared {
        id: metadata.id,
        remote_addr: metadata.remote_addr,
        route: metadata.route,
        protocol: metadata.protocol,
        outbound,
        control,
    });
    let socket = WebSocket {
        receiver: WebSocketReceiver { inbound, close_rx },
        sender: WebSocketSender {
            shared: shared.clone(),
        },
    };
    let internal_sender = InternalWebSocketSender { _shared: shared };
    let channels = DriverChannels {
        inbound_tx,
        outbound_rx,
        control_rx,
        close_tx,
    };

    (socket, internal_sender, channels)
}

impl WebSocket {
    /// The subprotocol negotiated during the handshake, if any.
    pub fn protocol(&self) -> Option<&str> {
        self.sender.protocol()
    }

    pub fn id(&self) -> WebSocketId {
        self.sender.id()
    }

    pub fn remote_addr(&self) -> Option<SocketAddr> {
        self.sender.remote_addr()
    }

    pub fn route(&self) -> &str {
        self.sender.route()
    }

    pub fn split(self) -> (WebSocketReceiver, WebSocketSender) {
        (self.receiver, self.sender)
    }

    pub async fn recv(&mut self) -> Result<Option<WebSocketMessage>, WebSocketError> {
        self.receiver.recv().await
    }

    pub async fn send(&mut self, message: WebSocketMessage) -> Result<(), WebSocketError> {
        self.sender.send(message).await
    }

    pub async fn send_text(&mut self, text: &str) -> Result<(), WebSocketError> {
        self.sender.send_text(text).await
    }

    pub async fn send_binary(&mut self, bytes: impl Into<Bytes>) -> Result<(), WebSocketError> {
        self.sender.send_binary(bytes).await
    }

    pub async fn send_json<T>(&mut self, value: &T) -> Result<(), WebSocketError>
    where
        T: Serialize,
    {
        self.sender.send_json(value).await
    }

    pub async fn recv_json<T>(&mut self) -> Result<Option<T>, WebSocketError>
    where
        T: DeserializeOwned,
    {
        self.receiver.recv_json().await
    }

    pub async fn send_event<T>(&mut self, event: &str, data: &T) -> Result<(), WebSocketError>
    where
        T: Serialize,
    {
        self.sender.send_event(event, data).await
    }

    pub async fn recv_event<T>(&mut self) -> Result<Option<WebSocketEvent<T>>, WebSocketError>
    where
        T: DeserializeOwned,
    {
        self.receiver.recv_event().await
    }

    pub async fn ping(&mut self, payload: impl Into<Bytes>) -> Result<(), WebSocketError> {
        self.sender.ping(payload).await
    }

    pub async fn pong(&mut self, payload: impl Into<Bytes>) -> Result<(), WebSocketError> {
        self.sender.pong(payload).await
    }

    pub async fn close(&mut self) -> Result<(), WebSocketError> {
        self.sender.close().await
    }
}

impl WebSocketSender {
    pub fn protocol(&self) -> Option<&str> {
        self.shared.protocol.as_deref()
    }

    pub fn id(&self) -> WebSocketId {
        self.shared.id
    }

    pub fn remote_addr(&self) -> Option<SocketAddr> {
        self.shared.remote_addr
    }

    pub fn route(&self) -> &str {
        &self.shared.route
    }

    pub async fn send(&self, message: WebSocketMessage) -> Result<(), WebSocketError> {
        match message {
            WebSocketMessage::Ping(payload) => {
                self.send_control(ControlCommand::Ping(payload)).await
            }
            WebSocketMessage::Pong(payload) => {
                self.send_control(ControlCommand::Pong(payload)).await
            }
            WebSocketMessage::Close(frame) => self.send_control(ControlCommand::Close(frame)).await,
            message => self
                .shared
                .outbound
                .send(OutboundCommand::Message(message))
                .await
                .map_err(|_| closed_error()),
        }
    }

    pub async fn send_text(&self, text: &str) -> Result<(), WebSocketError> {
        self.send(WebSocketMessage::text(text.to_string())).await
    }

    pub async fn send_binary(&self, bytes: impl Into<Bytes>) -> Result<(), WebSocketError> {
        self.send(WebSocketMessage::binary(bytes.into())).await
    }

    pub async fn send_json<T>(&self, value: &T) -> Result<(), WebSocketError>
    where
        T: Serialize,
    {
        let text = serde_json::to_string(value)?;
        self.send_text(&text).await
    }

    pub async fn send_event<T>(&self, event: &str, data: &T) -> Result<(), WebSocketError>
    where
        T: Serialize,
    {
        self.send_json(&WebSocketEvent {
            event: event.to_string(),
            data,
        })
        .await
    }

    pub async fn ping(&self, payload: impl Into<Bytes>) -> Result<(), WebSocketError> {
        self.send_control(ControlCommand::Ping(payload.into()))
            .await
    }

    pub async fn pong(&self, payload: impl Into<Bytes>) -> Result<(), WebSocketError> {
        self.send_control(ControlCommand::Pong(payload.into()))
            .await
    }

    pub async fn close(&self) -> Result<(), WebSocketError> {
        self.send_control(ControlCommand::Close(None)).await
    }

    async fn send_control(&self, command: ControlCommand) -> Result<(), WebSocketError> {
        self.shared
            .control
            .send(command)
            .await
            .map_err(|_| closed_error())
    }
}

impl WebSocketReceiver {
    pub async fn recv(&mut self) -> Result<Option<WebSocketMessage>, WebSocketError> {
        match self.inbound.recv().await {
            Some(Ok(message)) => Ok(Some(message)),
            Some(Err(error)) => Err(error),
            None => Ok(None),
        }
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

    pub async fn recv_event<T>(&mut self) -> Result<Option<WebSocketEvent<T>>, WebSocketError>
    where
        T: DeserializeOwned,
    {
        self.recv_json::<WebSocketEvent<T>>().await
    }
}

fn closed_error() -> WebSocketError {
    WebSocketError::Protocol(tokio_tungstenite::tungstenite::Error::ConnectionClosed)
}
