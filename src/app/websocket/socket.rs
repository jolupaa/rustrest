use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use hyper::body::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

use super::driver::{CONTROL_CHANNEL_CAPACITY, ControlCommand, DriverChannels, OutboundCommand};
use super::runtime::WebSocketRuntimeHandle;
use super::types::{WebSocketCloseInfo, WebSocketCloseInitiator, WebSocketId};
use super::{
    BackpressurePolicy, ResolvedWebSocketConfig, WebSocketCapacityError, WebSocketError,
    WebSocketTimeout, WsError,
};

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
    backpressure_policy: BackpressurePolicy,
    send_timeout: Duration,
    close_rx: watch::Receiver<Option<WebSocketCloseInfo>>,
    runtime: WebSocketRuntimeHandle,
}

pub(crate) struct SocketMetadata {
    pub id: WebSocketId,
    pub remote_addr: Option<SocketAddr>,
    pub route: String,
    pub protocol: Option<String>,
}

pub(crate) fn channel_pair(
    metadata: SocketMetadata,
    config: &ResolvedWebSocketConfig,
    runtime: WebSocketRuntimeHandle,
) -> (WebSocket, InternalWebSocketSender, DriverChannels) {
    let (inbound_tx, inbound) = mpsc::channel(config.inbound_capacity);
    let (outbound, outbound_rx) = mpsc::channel(config.outbound_capacity);
    let (control, control_rx) = mpsc::channel(CONTROL_CHANNEL_CAPACITY);
    let (close_tx, close_rx) = watch::channel(None);
    let shared = Arc::new(SocketShared {
        id: metadata.id,
        remote_addr: metadata.remote_addr,
        route: metadata.route,
        protocol: metadata.protocol,
        outbound,
        control,
        backpressure_policy: config.backpressure_policy,
        send_timeout: config.send_timeout,
        close_rx: close_rx.clone(),
        runtime,
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
        self.sender.send(message).await.map_err(compatibility_error)
    }

    pub async fn send_text(&mut self, text: &str) -> Result<(), WebSocketError> {
        self.sender
            .send_text(text)
            .await
            .map_err(compatibility_error)
    }

    pub async fn send_binary(&mut self, bytes: impl Into<Bytes>) -> Result<(), WebSocketError> {
        self.sender
            .send_binary(bytes)
            .await
            .map_err(compatibility_error)
    }

    pub async fn send_json<T>(&mut self, value: &T) -> Result<(), WebSocketError>
    where
        T: Serialize,
    {
        self.sender
            .send_json(value)
            .await
            .map_err(compatibility_error)
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
        self.sender
            .send_event(event, data)
            .await
            .map_err(compatibility_error)
    }

    pub async fn recv_event<T>(&mut self) -> Result<Option<WebSocketEvent<T>>, WebSocketError>
    where
        T: DeserializeOwned,
    {
        self.receiver.recv_event().await
    }

    pub async fn ping(&mut self, payload: impl Into<Bytes>) -> Result<(), WebSocketError> {
        self.sender.ping(payload).await.map_err(compatibility_error)
    }

    pub async fn pong(&mut self, payload: impl Into<Bytes>) -> Result<(), WebSocketError> {
        self.sender.pong(payload).await.map_err(compatibility_error)
    }

    pub async fn close(&mut self) -> Result<(), WebSocketError> {
        self.sender.close().await.map_err(compatibility_error)
    }

    pub async fn close_with(
        &mut self,
        code: u16,
        reason: impl Into<String>,
    ) -> Result<(), WebSocketError> {
        self.sender
            .close_with(code, reason)
            .await
            .map_err(compatibility_error)
    }

    pub async fn closed(&mut self) -> WebSocketCloseInfo {
        self.receiver.closed().await
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

    pub fn try_send(&self, message: WebSocketMessage) -> Result<(), WsError> {
        match message {
            WebSocketMessage::Ping(payload) => self.try_send_control(ControlCommand::Ping(payload)),
            WebSocketMessage::Pong(payload) => self.try_send_control(ControlCommand::Pong(payload)),
            WebSocketMessage::Close(frame) => self.try_send_control(ControlCommand::Close(frame)),
            message => {
                let permit =
                    self.shared.outbound.clone().try_reserve_owned().map_err(
                        |error| match error {
                            mpsc::error::TrySendError::Full(_) => {
                                self.shared.runtime.record_saturated_send();
                                WsError::Capacity(WebSocketCapacityError::OutboundQueue)
                            }
                            mpsc::error::TrySendError::Closed(_) => WsError::Closed,
                        },
                    )?;
                permit.send(OutboundCommand::Message(message));
                Ok(())
            }
        }
    }

    pub async fn send(&self, message: WebSocketMessage) -> Result<(), WsError> {
        match message {
            WebSocketMessage::Ping(payload) => {
                self.send_control(ControlCommand::Ping(payload)).await
            }
            WebSocketMessage::Pong(payload) => {
                self.send_control(ControlCommand::Pong(payload)).await
            }
            WebSocketMessage::Close(frame) => self.send_control(ControlCommand::Close(frame)).await,
            message => self.send_application(message).await,
        }
    }

    pub async fn send_text(&self, text: &str) -> Result<(), WsError> {
        self.send(WebSocketMessage::text(text.to_string())).await
    }

    pub async fn send_binary(&self, bytes: impl Into<Bytes>) -> Result<(), WsError> {
        self.send(WebSocketMessage::binary(bytes.into())).await
    }

    pub async fn send_json<T>(&self, value: &T) -> Result<(), WsError>
    where
        T: Serialize,
    {
        let text = serde_json::to_string(value)?;
        self.send_text(&text).await
    }

    pub async fn send_event<T>(&self, event: &str, data: &T) -> Result<(), WsError>
    where
        T: Serialize,
    {
        self.send_json(&WebSocketEvent {
            event: event.to_string(),
            data,
        })
        .await
    }

    pub async fn ping(&self, payload: impl Into<Bytes>) -> Result<(), WsError> {
        self.send_control(ControlCommand::Ping(payload.into()))
            .await
    }

    pub async fn pong(&self, payload: impl Into<Bytes>) -> Result<(), WsError> {
        self.send_control(ControlCommand::Pong(payload.into()))
            .await
    }

    pub async fn close(&self) -> Result<(), WsError> {
        self.close_with(1000, "").await
    }

    pub async fn close_with(&self, code: u16, reason: impl Into<String>) -> Result<(), WsError> {
        let reason = reason.into();
        validate_close(code, &reason)?;
        let frame = CloseFrame {
            code: CloseCode::from(code),
            reason: reason.into(),
        };
        self.send_control(ControlCommand::Close(Some(frame))).await
    }

    pub async fn closed(&self) -> WebSocketCloseInfo {
        let mut close_rx = self.shared.close_rx.clone();
        wait_for_close(&mut close_rx).await
    }

    async fn send_application(&self, message: WebSocketMessage) -> Result<(), WsError> {
        match self.shared.backpressure_policy {
            BackpressurePolicy::Wait => {
                let permit = tokio::time::timeout(
                    self.shared.send_timeout,
                    self.shared.outbound.clone().reserve_owned(),
                )
                .await
                .map_err(|_| {
                    self.shared.runtime.record_saturated_send();
                    WsError::Timeout(WebSocketTimeout::Send)
                })?
                .map_err(|_| WsError::Closed)?;
                permit.send(OutboundCommand::Message(message));
                Ok(())
            }
            BackpressurePolicy::Reject => self.try_send(message),
            BackpressurePolicy::Disconnect => match self.try_send(message) {
                Ok(()) => Ok(()),
                Err(WsError::Capacity(_)) => {
                    self.disconnect_slow_consumer().await?;
                    Err(WsError::Capacity(WebSocketCapacityError::OutboundQueue))
                }
                Err(error) => Err(error),
            },
        }
    }

    async fn disconnect_slow_consumer(&self) -> Result<(), WsError> {
        let frame = CloseFrame {
            code: CloseCode::Again,
            reason: "Cliente WebSocket demasiado lento".into(),
        };
        self.send_control(ControlCommand::Disconnect(Some(frame)))
            .await
    }

    fn try_send_control(&self, command: ControlCommand) -> Result<(), WsError> {
        self.shared
            .control
            .try_send(command)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    self.shared.runtime.record_saturated_send();
                    WsError::Capacity(WebSocketCapacityError::OutboundQueue)
                }
                mpsc::error::TrySendError::Closed(_) => WsError::Closed,
            })
    }

    async fn send_control(&self, command: ControlCommand) -> Result<(), WsError> {
        self.shared
            .control
            .send(command)
            .await
            .map_err(|_| WsError::Closed)
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

    pub async fn closed(&mut self) -> WebSocketCloseInfo {
        wait_for_close(&mut self.close_rx).await
    }
}

fn validate_close(code: u16, reason: &str) -> Result<(), WsError> {
    let valid_code = matches!(
        code,
        1000 | 1001 | 1002 | 1003 | 1007 | 1008 | 1009 | 1010 | 1011 | 1012 | 1013
    ) || (3000..=4999).contains(&code);
    if !valid_code {
        return Err(WsError::InvalidClose {
            code,
            reason: "codigo de cierre reservado o no asignado".to_string(),
        });
    }
    if reason.len() > 123 {
        return Err(WsError::InvalidClose {
            code,
            reason: "la razon de cierre supera 123 bytes UTF-8".to_string(),
        });
    }
    Ok(())
}

fn compatibility_error(error: WsError) -> WebSocketError {
    match error {
        WsError::WebSocket(error) => error,
        error => WebSocketError::Protocol(tokio_tungstenite::tungstenite::Error::Io(
            io::Error::other(error.to_string()),
        )),
    }
}

async fn wait_for_close(
    close_rx: &mut watch::Receiver<Option<WebSocketCloseInfo>>,
) -> WebSocketCloseInfo {
    loop {
        if let Some(close_info) = close_rx.borrow().clone() {
            return close_info;
        }
        if close_rx.changed().await.is_err() {
            return WebSocketCloseInfo {
                code: 1006,
                reason: "El driver WebSocket termino sin publicar el cierre".to_string(),
                initiator: WebSocketCloseInitiator::ProtocolError,
                clean: false,
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::app::websocket::runtime::WebSocketRuntimeHandle;
    use crate::app::websocket::{
        BackpressurePolicy, ResolvedWebSocketConfig, WebSocketCapacityError, WebSocketConfig,
        WebSocketTimeout, WsError,
    };

    fn sender_with_policy(
        policy: BackpressurePolicy,
        send_timeout: Duration,
    ) -> (WebSocketSender, DriverChannels, WebSocketRuntimeHandle) {
        let runtime = WebSocketRuntimeHandle::local();
        let config = ResolvedWebSocketConfig::from_layers(
            &WebSocketConfig::new(),
            &WebSocketConfig::new()
                .outbound_capacity(1)
                .backpressure_policy(policy)
                .send_timeout(send_timeout),
        );
        let (socket, _internal_sender, channels) = channel_pair(
            SocketMetadata {
                id: WebSocketId(1),
                remote_addr: None,
                route: "/ws".to_string(),
                protocol: None,
            },
            &config,
            runtime.clone(),
        );
        let (_receiver, sender) = socket.split();
        (sender, channels, runtime)
    }

    #[tokio::test]
    async fn websocket_backpressure_try_send_reports_capacity_immediately() {
        let (sender, _channels, runtime) =
            sender_with_policy(BackpressurePolicy::Wait, Duration::from_secs(1));

        sender.try_send(WebSocketMessage::text("first")).unwrap();
        let error = sender
            .try_send(WebSocketMessage::text("second"))
            .unwrap_err();

        assert!(matches!(
            error,
            WsError::Capacity(WebSocketCapacityError::OutboundQueue)
        ));
        assert_eq!(runtime.stats().saturated_sends, 1);
    }

    #[tokio::test]
    async fn websocket_backpressure_wait_times_out() {
        let (sender, _channels, runtime) =
            sender_with_policy(BackpressurePolicy::Wait, Duration::from_millis(20));
        sender.try_send(WebSocketMessage::text("first")).unwrap();

        let error = sender
            .send(WebSocketMessage::text("second"))
            .await
            .unwrap_err();

        assert!(matches!(error, WsError::Timeout(WebSocketTimeout::Send)));
        assert_eq!(runtime.stats().saturated_sends, 1);
    }

    #[tokio::test]
    async fn websocket_backpressure_reject_does_not_wait() {
        let (sender, _channels, runtime) =
            sender_with_policy(BackpressurePolicy::Reject, Duration::from_secs(1));
        sender.try_send(WebSocketMessage::text("first")).unwrap();

        let error = tokio::time::timeout(
            Duration::from_millis(50),
            sender.send(WebSocketMessage::text("second")),
        )
        .await
        .expect("Reject must return without waiting")
        .unwrap_err();

        assert!(matches!(
            error,
            WsError::Capacity(WebSocketCapacityError::OutboundQueue)
        ));
        assert_eq!(runtime.stats().saturated_sends, 1);
    }

    #[tokio::test]
    async fn websocket_backpressure_disconnect_queues_close_1013() {
        let (sender, mut channels, runtime) =
            sender_with_policy(BackpressurePolicy::Disconnect, Duration::from_secs(1));
        sender.try_send(WebSocketMessage::text("first")).unwrap();

        let error = sender
            .send(WebSocketMessage::text("second"))
            .await
            .unwrap_err();
        let command = channels.control_rx.recv().await.unwrap();

        assert!(matches!(
            error,
            WsError::Capacity(WebSocketCapacityError::OutboundQueue)
        ));
        let ControlCommand::Disconnect(Some(frame)) = command else {
            panic!("Disconnect must enqueue a Close frame");
        };
        assert_eq!(u16::from(frame.code), 1013);
        assert_eq!(runtime.stats().saturated_sends, 1);
    }

    #[tokio::test]
    async fn websocket_split_handles_observe_published_close() {
        let runtime = WebSocketRuntimeHandle::local();
        let config = ResolvedWebSocketConfig::from_layers(
            &WebSocketConfig::new(),
            &WebSocketConfig::new().outbound_capacity(1),
        );
        let (socket, _internal_sender, channels) = channel_pair(
            SocketMetadata {
                id: WebSocketId(1),
                remote_addr: None,
                route: "/ws".to_string(),
                protocol: None,
            },
            &config,
            runtime,
        );
        let (mut receiver, sender) = socket.split();
        let expected = WebSocketCloseInfo {
            code: 1000,
            reason: "finalizado".to_string(),
            initiator: super::super::WebSocketCloseInitiator::Local,
            clean: true,
        };
        channels.close_tx.send(Some(expected.clone())).unwrap();

        assert_eq!(sender.closed().await, expected);
        assert_eq!(receiver.closed().await, expected);
    }

    #[tokio::test]
    async fn websocket_close_code_rejects_reserved_codes() {
        let (sender, _channels, _runtime) =
            sender_with_policy(BackpressurePolicy::Wait, Duration::from_secs(1));

        let error = sender.close_with(1005, "reservado").await.unwrap_err();

        assert!(matches!(error, WsError::InvalidClose { code: 1005, .. }));
    }

    #[tokio::test]
    async fn websocket_close_code_rejects_reasons_over_123_bytes() {
        let (sender, _channels, _runtime) =
            sender_with_policy(BackpressurePolicy::Wait, Duration::from_secs(1));
        let reason = "á".repeat(62);

        let error = sender.close_with(1000, reason).await.unwrap_err();

        assert!(matches!(error, WsError::InvalidClose { code: 1000, .. }));
    }
}
