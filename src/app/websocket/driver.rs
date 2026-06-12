use std::future::pending;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use hyper::body::Bytes;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::Instant;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::error::CapacityError;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Role};

use super::runtime::{ConnectionPermit, WebSocketRuntimeHandle};
use super::socket::{NormalizedWebSocketHandler, WebSocket};
use super::types::{WebSocketCloseInfo, WebSocketCloseInitiator, WebSocketMessage};
use super::{ResolvedWebSocketConfig, WebSocketError, WsError};

type WebSocketTransport = WebSocketStream<TokioIo<Upgraded>>;

pub(crate) const CONTROL_CHANNEL_CAPACITY: usize = 8;

pub(crate) enum OutboundCommand {
    Message(WebSocketMessage),
}

pub(crate) enum ControlCommand {
    Ping(Bytes),
    Pong(Bytes),
    Close(Option<CloseFrame>),
    Disconnect(Option<CloseFrame>),
}

pub(crate) struct DriverChannels {
    pub inbound_tx: mpsc::Sender<Result<WebSocketMessage, WebSocketError>>,
    pub outbound_rx: mpsc::Receiver<OutboundCommand>,
    pub control_rx: mpsc::Receiver<ControlCommand>,
    pub close_tx: watch::Sender<Option<WebSocketCloseInfo>>,
    pub sender_count_rx: watch::Receiver<usize>,
}

pub(crate) struct DriverTask {
    pub abort_handle: AbortHandle,
    pub start_tx: oneshot::Sender<bool>,
}

enum DriverStart {
    Registered(oneshot::Receiver<Result<(), WsError>>),
    Rejected,
}

pub(crate) async fn spawn(
    upgraded: Upgraded,
    config: ResolvedWebSocketConfig,
    handler: NormalizedWebSocketHandler,
    socket: WebSocket,
    channels: DriverChannels,
    permit: ConnectionPermit,
) -> DriverTask {
    let runtime = permit.runtime();
    let io = TokioIo::new(upgraded);
    let stream =
        WebSocketStream::from_raw_socket(io, Role::Server, Some(config.tungstenite_config())).await;
    let (start_tx, start_rx) = oneshot::channel();
    let (driver_start_tx, driver_start_rx) = oneshot::channel();
    let driver = tokio::spawn(run(stream, config, channels, driver_start_rx, runtime));
    let abort_handle = driver.abort_handle();
    tokio::spawn(supervise(
        start_rx,
        driver_start_tx,
        driver,
        handler,
        socket,
        permit,
    ));

    DriverTask {
        abort_handle,
        start_tx,
    }
}

async fn run(
    mut stream: WebSocketTransport,
    config: ResolvedWebSocketConfig,
    channels: DriverChannels,
    start_rx: oneshot::Receiver<DriverStart>,
    runtime: WebSocketRuntimeHandle,
) -> DriverOutcome {
    let mut handler_done = match start_rx.await.unwrap_or(DriverStart::Rejected) {
        DriverStart::Registered(handler_done) => handler_done,
        DriverStart::Rejected => {
            let frame = CloseFrame {
                code: CloseCode::Away,
                reason: "El servidor se esta cerrando".into(),
            };
            let _ =
                write_control(&mut stream, ControlCommand::Disconnect(Some(frame.clone()))).await;
            let close_info = close_info(Some(&frame), WebSocketCloseInitiator::Runtime, false);
            let _ = channels.close_tx.send(Some(close_info.clone()));
            return DriverOutcome {
                close_info,
                handler_completed: false,
            };
        }
    };

    let DriverChannels {
        inbound_tx,
        mut outbound_rx,
        mut control_rx,
        close_tx,
        mut sender_count_rx,
    } = channels;
    let outcome = drive(
        &mut stream,
        &inbound_tx,
        &mut outbound_rx,
        &mut control_rx,
        &mut handler_done,
        &mut sender_count_rx,
        (&config, &runtime),
    )
    .await;

    let _ = close_tx.send(Some(outcome.close_info.clone()));
    drop(inbound_tx);
    outcome
}

async fn supervise(
    start_rx: oneshot::Receiver<bool>,
    driver_start_tx: oneshot::Sender<DriverStart>,
    driver: JoinHandle<DriverOutcome>,
    handler: NormalizedWebSocketHandler,
    socket: WebSocket,
    _permit: ConnectionPermit,
) {
    if !start_rx.await.unwrap_or(false) {
        let _ = driver_start_tx.send(DriverStart::Rejected);
        let _ = driver.await;
        return;
    }

    let (handler_done_tx, handler_done_rx) = oneshot::channel();
    let handler = tokio::spawn(async move {
        let result = handler(socket).await;
        let _ = handler_done_tx.send(result);
    });
    if driver_start_tx
        .send(DriverStart::Registered(handler_done_rx))
        .is_err()
    {
        finish_handler(handler, false).await;
        return;
    }

    let handler_completed = driver
        .await
        .ok()
        .is_some_and(|outcome| outcome.handler_completed);
    if !handler_completed {
        tokio::task::yield_now().await;
    }
    finish_handler(handler, handler_completed).await;
}

async fn finish_handler(handler: JoinHandle<()>, completed: bool) {
    if !completed && !handler.is_finished() {
        handler.abort();
    }
    let _ = handler.await;
}

struct DriverOutcome {
    close_info: WebSocketCloseInfo,
    handler_completed: bool,
}

struct DriverState {
    close_sent: bool,
    close_received: bool,
    close_info: Option<WebSocketCloseInfo>,
    close_deadline: Option<Instant>,
    pending_ping: Option<(Bytes, Instant)>,
    last_inbound_frame: Instant,
    last_application_message: Instant,
    opened_at: Instant,
    next_ping_token: u64,
    message_window_started: Instant,
    message_count: u32,
}

impl DriverState {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            close_sent: false,
            close_received: false,
            close_info: None,
            close_deadline: None,
            pending_ping: None,
            last_inbound_frame: now,
            last_application_message: now,
            opened_at: now,
            next_ping_token: 1,
            message_window_started: now,
            message_count: 0,
        }
    }

    fn outcome(&self, handler_completed: bool) -> DriverOutcome {
        let mut close_info = self.close_info.clone().unwrap_or_else(peer_disconnect_info);
        close_info.clean = self.close_sent && self.close_received;
        DriverOutcome {
            close_info,
            handler_completed,
        }
    }
}

async fn drive(
    stream: &mut WebSocketTransport,
    inbound_tx: &mpsc::Sender<Result<WebSocketMessage, WebSocketError>>,
    outbound_rx: &mut mpsc::Receiver<OutboundCommand>,
    control_rx: &mut mpsc::Receiver<ControlCommand>,
    handler_done: &mut oneshot::Receiver<Result<(), WsError>>,
    sender_count_rx: &mut watch::Receiver<usize>,
    lifecycle: (&ResolvedWebSocketConfig, &WebSocketRuntimeHandle),
) -> DriverOutcome {
    let (config, runtime) = lifecycle;
    let mut control_open = true;
    let mut outbound_open = true;
    let mut handler_completed = false;
    let mut state = DriverState::new();

    loop {
        tokio::select! {
            biased;

            command = control_rx.recv(), if control_open => {
                let Some(command) = command else {
                    control_open = false;
                    continue;
                };
                let disconnect = matches!(&command, ControlCommand::Disconnect(_));
                if state.close_sent {
                    continue;
                }
                if matches!(&command, ControlCommand::Close(_) | ControlCommand::Disconnect(_)) {
                    close_command_channels(
                        control_rx,
                        outbound_rx,
                        &mut control_open,
                        &mut outbound_open,
                    );
                }
                if matches!(&command, ControlCommand::Close(_)) {
                    for message in take_queued_before_close(outbound_rx) {
                        if let Err(error) = stream.send(message).await {
                            return protocol_outcome(inbound_tx, error);
                        }
                    }
                }
                match command {
                    ControlCommand::Close(frame) | ControlCommand::Disconnect(frame) => {
                        let initiator = if disconnect {
                            WebSocketCloseInitiator::Runtime
                        } else {
                            WebSocketCloseInitiator::Local
                        };
                        if let Err(error) = initiate_close(
                            stream,
                            &mut state,
                            frame,
                            initiator,
                            config.close_timeout,
                        )
                        .await
                        {
                            return protocol_outcome(inbound_tx, error);
                        }
                    }
                    command => {
                        if let Err(error) = write_control(stream, command).await {
                            return protocol_outcome(inbound_tx, error);
                        }
                    }
                }
            }

            _ = wait_until(state.close_deadline), if state.close_sent => {
                return state.outcome(handler_completed);
            }

            _ = wait_until(pong_deadline(&state, config)), if !state.close_sent => {
                runtime.record_heartbeat_timeout();
                state.pending_ping = None;
                close_command_channels(
                    control_rx,
                    outbound_rx,
                    &mut control_open,
                    &mut outbound_open,
                );
                let frame = CloseFrame {
                    code: CloseCode::Away,
                    reason: "Tiempo de espera de Pong agotado".into(),
                };
                if let Err(error) = initiate_close(
                    stream,
                    &mut state,
                    Some(frame),
                    WebSocketCloseInitiator::Timeout,
                    config.close_timeout,
                )
                .await
                {
                    return protocol_outcome(inbound_tx, error);
                }
            }

            _ = wait_until(lifetime_deadline(&state, config)), if !state.close_sent => {
                close_command_channels(
                    control_rx,
                    outbound_rx,
                    &mut control_open,
                    &mut outbound_open,
                );
                let frame = CloseFrame {
                    code: CloseCode::Away,
                    reason: "Duracion maxima de conexion agotada".into(),
                };
                if let Err(error) = initiate_close(
                    stream,
                    &mut state,
                    Some(frame),
                    WebSocketCloseInitiator::Timeout,
                    config.close_timeout,
                )
                .await
                {
                    return protocol_outcome(inbound_tx, error);
                }
            }

            _ = wait_until(idle_deadline(&state, config)), if !state.close_sent => {
                close_command_channels(
                    control_rx,
                    outbound_rx,
                    &mut control_open,
                    &mut outbound_open,
                );
                let frame = CloseFrame {
                    code: CloseCode::Away,
                    reason: "Tiempo de inactividad agotado".into(),
                };
                if let Err(error) = initiate_close(
                    stream,
                    &mut state,
                    Some(frame),
                    WebSocketCloseInitiator::Timeout,
                    config.close_timeout,
                )
                .await
                {
                    return protocol_outcome(inbound_tx, error);
                }
            }

            command = outbound_rx.recv(), if outbound_open && !state.close_sent => {
                let Some(OutboundCommand::Message(message)) = command else {
                    outbound_open = false;
                    continue;
                };
                if let Err(error) = stream.send(message).await {
                    return protocol_outcome(inbound_tx, error);
                }
            }

            incoming = stream.next() => {
                match incoming {
                    Some(Ok(message)) => {
                        let received_at = Instant::now();
                        state.last_inbound_frame = received_at;
                        if message.is_text() || message.is_binary() {
                            if message_rate_exceeded(&mut state, config, received_at) {
                                close_command_channels(
                                    control_rx,
                                    outbound_rx,
                                    &mut control_open,
                                    &mut outbound_open,
                                );
                                let frame = CloseFrame {
                                    code: CloseCode::Policy,
                                    reason: "Limite de mensajes WebSocket excedido".into(),
                                };
                                if let Err(error) = initiate_close(
                                    stream,
                                    &mut state,
                                    Some(frame),
                                    WebSocketCloseInitiator::Runtime,
                                    config.close_timeout,
                                )
                                .await
                                {
                                    return protocol_outcome(inbound_tx, error);
                                }
                                continue;
                            }
                            state.last_application_message = state.last_inbound_frame;
                        }
                        if let WebSocketMessage::Pong(payload) = &message {
                            if state
                                .pending_ping
                                .as_ref()
                                .is_some_and(|(expected, _)| expected == payload)
                            {
                                state.pending_ping = None;
                            }
                        }
                        if message.is_ping() || message.is_close() {
                            if let Err(error) = stream.flush().await {
                                return protocol_outcome(inbound_tx, error);
                            }
                        }
                        if let WebSocketMessage::Close(frame) = &message {
                            state.close_received = true;
                            if state.close_info.is_none() {
                                state.close_info = Some(close_info(
                                    frame.as_ref(),
                                    WebSocketCloseInitiator::Peer,
                                    true,
                                ));
                            }
                            state.close_sent = true;
                        }
                        if deliver_inbound(inbound_tx, message, config.send_timeout).await
                            == InboundDelivery::TimedOut
                            && !state.close_sent
                        {
                            close_command_channels(
                                control_rx,
                                outbound_rx,
                                &mut control_open,
                                &mut outbound_open,
                            );
                            let frame = CloseFrame {
                                code: CloseCode::Again,
                                reason: "El handler WebSocket no consume mensajes a tiempo".into(),
                            };
                            if let Err(error) = initiate_close(
                                stream,
                                &mut state,
                                Some(frame),
                                WebSocketCloseInitiator::Runtime,
                                config.close_timeout,
                            )
                            .await
                            {
                                return protocol_outcome(inbound_tx, error);
                            }
                        }
                        if state.close_received {
                            return state.outcome(handler_completed);
                        }
                    }
                    Some(Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed))
                        if state.close_sent =>
                    {
                        state.close_received = true;
                        return state.outcome(handler_completed);
                    }
                    Some(Err(tokio_tungstenite::tungstenite::Error::Capacity(
                        CapacityError::MessageTooLong { .. },
                    ))) => {
                        close_command_channels(
                            control_rx,
                            outbound_rx,
                            &mut control_open,
                            &mut outbound_open,
                        );
                        let frame = CloseFrame {
                            code: CloseCode::Size,
                            reason: "Mensaje WebSocket demasiado grande".into(),
                        };
                        if let Err(error) = initiate_close(
                            stream,
                            &mut state,
                            Some(frame),
                            WebSocketCloseInitiator::ProtocolError,
                            config.close_timeout,
                        )
                        .await
                        {
                            return protocol_outcome(inbound_tx, error);
                        }
                    }
                    Some(Err(error)) => return protocol_outcome(inbound_tx, error),
                    None => return state.outcome(handler_completed),
                }
            }

            _ = wait_until(ping_deadline(&state, config)), if !state.close_sent => {
                let payload = Bytes::copy_from_slice(&state.next_ping_token.to_be_bytes());
                state.next_ping_token = state.next_ping_token.wrapping_add(1);
                if let Err(error) = stream.send(WebSocketMessage::Ping(payload.clone())).await {
                    return protocol_outcome(inbound_tx, error);
                }
                state.pending_ping = Some((payload, Instant::now()));
            }

            result = &mut *handler_done, if !handler_completed => {
                handler_completed = true;
                match result {
                    Ok(Ok(())) if !state.close_sent => {
                        if *sender_count_rx.borrow() == 0 {
                            close_command_channels(
                                control_rx,
                                outbound_rx,
                                &mut control_open,
                                &mut outbound_open,
                            );
                            let frame = CloseFrame {
                                code: CloseCode::Normal,
                                reason: "".into(),
                            };
                            if let Err(error) = initiate_close(
                                stream,
                                &mut state,
                                Some(frame),
                                WebSocketCloseInitiator::Handler,
                                config.close_timeout,
                            )
                            .await
                            {
                                return protocol_outcome(inbound_tx, error);
                            }
                        }
                    }
                    Ok(Ok(())) => {}
                    Ok(Err(_)) | Err(_) if !state.close_sent => {
                        close_command_channels(
                            control_rx,
                            outbound_rx,
                            &mut control_open,
                            &mut outbound_open,
                        );
                        let frame = CloseFrame {
                            code: CloseCode::Error,
                            reason: "El handler WebSocket fallo".into(),
                        };
                        if let Err(error) = initiate_close(
                            stream,
                            &mut state,
                            Some(frame),
                            WebSocketCloseInitiator::Handler,
                            config.close_timeout,
                        )
                        .await
                        {
                            return protocol_outcome(inbound_tx, error);
                        }
                    }
                    Ok(Err(_)) | Err(_) => {}
                }
            }

            changed = sender_count_rx.changed(), if handler_completed && !state.close_sent => {
                if changed.is_err() || *sender_count_rx.borrow_and_update() == 0 {
                    close_command_channels(
                        control_rx,
                        outbound_rx,
                        &mut control_open,
                        &mut outbound_open,
                    );
                    let frame = CloseFrame {
                        code: CloseCode::Normal,
                        reason: "".into(),
                    };
                    if let Err(error) = initiate_close(
                        stream,
                        &mut state,
                        Some(frame),
                        WebSocketCloseInitiator::Handler,
                        config.close_timeout,
                    )
                    .await
                    {
                        return protocol_outcome(inbound_tx, error);
                    }
                }
            }
        }
    }
}

fn close_command_channels(
    control_rx: &mut mpsc::Receiver<ControlCommand>,
    outbound_rx: &mut mpsc::Receiver<OutboundCommand>,
    control_open: &mut bool,
    outbound_open: &mut bool,
) {
    control_rx.close();
    outbound_rx.close();
    *control_open = false;
    *outbound_open = false;
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum InboundDelivery {
    Delivered,
    ReceiverClosed,
    TimedOut,
}

async fn deliver_inbound(
    inbound_tx: &mpsc::Sender<Result<WebSocketMessage, WebSocketError>>,
    message: WebSocketMessage,
    timeout: Duration,
) -> InboundDelivery {
    if inbound_tx.is_closed() {
        return InboundDelivery::ReceiverClosed;
    }
    match tokio::time::timeout(timeout, inbound_tx.send(Ok(message))).await {
        Ok(Ok(())) => InboundDelivery::Delivered,
        Ok(Err(_)) => InboundDelivery::ReceiverClosed,
        Err(_) => InboundDelivery::TimedOut,
    }
}

async fn initiate_close(
    stream: &mut WebSocketTransport,
    state: &mut DriverState,
    frame: Option<CloseFrame>,
    initiator: WebSocketCloseInitiator,
    close_timeout: Duration,
) -> Result<(), tokio_tungstenite::tungstenite::Error> {
    if state.close_sent {
        return Ok(());
    }
    state.close_info = Some(close_info(frame.as_ref(), initiator, false));
    stream.send(WebSocketMessage::Close(frame)).await?;
    state.close_sent = true;
    state.close_deadline = Some(Instant::now() + close_timeout);
    Ok(())
}

fn ping_deadline(state: &DriverState, config: &ResolvedWebSocketConfig) -> Option<Instant> {
    if state.pending_ping.is_some() {
        None
    } else {
        config
            .ping_interval
            .map(|interval| state.last_inbound_frame + interval)
    }
}

fn pong_deadline(state: &DriverState, config: &ResolvedWebSocketConfig) -> Option<Instant> {
    state
        .pending_ping
        .as_ref()
        .map(|(_, sent_at)| *sent_at + config.pong_timeout)
}

fn idle_deadline(state: &DriverState, config: &ResolvedWebSocketConfig) -> Option<Instant> {
    config
        .idle_timeout
        .map(|timeout| state.last_application_message + timeout)
}

fn lifetime_deadline(state: &DriverState, config: &ResolvedWebSocketConfig) -> Option<Instant> {
    config
        .max_connection_lifetime
        .map(|lifetime| state.opened_at + lifetime)
}

fn message_rate_exceeded(
    state: &mut DriverState,
    config: &ResolvedWebSocketConfig,
    now: Instant,
) -> bool {
    let Some(limit) = &config.message_rate_limit else {
        return false;
    };
    if now.duration_since(state.message_window_started) >= limit.interval {
        state.message_window_started = now;
        state.message_count = 0;
    }
    if state.message_count >= limit.max_messages {
        return true;
    }
    state.message_count += 1;
    false
}

async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => pending::<()>().await,
    }
}

fn take_queued_before_close(
    outbound_rx: &mut mpsc::Receiver<OutboundCommand>,
) -> Vec<WebSocketMessage> {
    let queued = outbound_rx.len();
    let mut messages = Vec::with_capacity(queued);
    for _ in 0..queued {
        let Ok(OutboundCommand::Message(message)) = outbound_rx.try_recv() else {
            break;
        };
        messages.push(message);
    }
    messages
}

async fn write_control(
    stream: &mut WebSocketTransport,
    command: ControlCommand,
) -> Result<(), tokio_tungstenite::tungstenite::Error> {
    let message = match command {
        ControlCommand::Ping(payload) => WebSocketMessage::Ping(payload),
        ControlCommand::Pong(payload) => WebSocketMessage::Pong(payload),
        ControlCommand::Close(frame) | ControlCommand::Disconnect(frame) => {
            WebSocketMessage::Close(frame)
        }
    };
    stream.send(message).await
}

fn protocol_outcome(
    inbound_tx: &mpsc::Sender<Result<WebSocketMessage, WebSocketError>>,
    error: tokio_tungstenite::tungstenite::Error,
) -> DriverOutcome {
    let reason = error.to_string();
    let _ = inbound_tx.try_send(Err(error.into()));
    DriverOutcome {
        close_info: WebSocketCloseInfo {
            code: 1006,
            reason,
            initiator: WebSocketCloseInitiator::ProtocolError,
            clean: false,
        },
        handler_completed: false,
    }
}

fn close_info(
    frame: Option<&CloseFrame>,
    initiator: WebSocketCloseInitiator,
    clean: bool,
) -> WebSocketCloseInfo {
    WebSocketCloseInfo {
        code: frame.map_or(1005, |frame| frame.code.into()),
        reason: frame.map_or_else(String::new, |frame| frame.reason.to_string()),
        initiator,
        clean,
    }
}

fn peer_disconnect_info() -> WebSocketCloseInfo {
    WebSocketCloseInfo {
        code: 1006,
        reason: String::new(),
        initiator: WebSocketCloseInitiator::Peer,
        clean: false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::app::websocket::runtime::WebSocketRuntimeHandle;
    use crate::app::websocket::socket::{SocketMetadata, channel_pair};
    use crate::app::websocket::{ResolvedWebSocketConfig, WebSocketConfig};

    #[tokio::test]
    async fn close_drain_only_takes_the_initial_queue_snapshot() {
        let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::channel(2);
        outbound_tx
            .try_send(OutboundCommand::Message(WebSocketMessage::text("first")))
            .unwrap();
        outbound_tx
            .try_send(OutboundCommand::Message(WebSocketMessage::text("second")))
            .unwrap();

        outbound_rx.close();
        let queued = take_queued_before_close(&mut outbound_rx);

        let late_send = outbound_tx
            .try_send(OutboundCommand::Message(WebSocketMessage::text("late")))
            .unwrap_err();
        assert_eq!(queued.len(), 2);
        assert!(matches!(
            late_send,
            tokio::sync::mpsc::error::TrySendError::Closed(_)
        ));
        assert_eq!(outbound_rx.len(), 0);
        let texts = queued
            .into_iter()
            .map(|message| message.into_text().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(texts, ["first", "second"]);
    }

    #[tokio::test]
    async fn forced_driver_abort_cleans_handler_before_releasing_permit() {
        struct HandlerGuard {
            runtime: WebSocketRuntimeHandle,
            cleanup_tx: mpsc::Sender<usize>,
        }

        impl Drop for HandlerGuard {
            fn drop(&mut self) {
                let _ = self
                    .cleanup_tx
                    .send(self.runtime.stats().active_connections);
            }
        }

        let runtime = WebSocketRuntimeHandle::local();
        let config =
            ResolvedWebSocketConfig::from_layers(&WebSocketConfig::new(), &WebSocketConfig::new());
        let permit = runtime.admit("/ws", None, None, &config).unwrap();
        let id = permit.id();
        let (socket, internal_sender, _channels) = channel_pair(
            SocketMetadata {
                id,
                remote_addr: None,
                route: "/ws".to_string(),
                protocol: None,
            },
            &config,
            runtime.clone(),
        );
        let (start_tx, start_rx) = oneshot::channel();
        let (driver_start_tx, driver_start_rx) = oneshot::channel();
        let (cleanup_tx, cleanup_rx) = mpsc::channel();
        let started = Arc::new(tokio::sync::Notify::new());
        let handler_started = started.clone();
        let cleanup_sender = Arc::new(Mutex::new(Some(cleanup_tx)));
        let handler_runtime = runtime.clone();
        let handler: NormalizedWebSocketHandler = Arc::new(move |_socket| {
            let handler_runtime = handler_runtime.clone();
            let handler_started = handler_started.clone();
            let cleanup_tx = cleanup_sender.lock().unwrap().take().unwrap();
            Box::pin(async move {
                let _guard = HandlerGuard {
                    runtime: handler_runtime,
                    cleanup_tx,
                };
                handler_started.notify_one();
                pending::<Result<(), WsError>>().await
            })
        });
        let driver = tokio::spawn(async move {
            match driver_start_rx.await.unwrap() {
                DriverStart::Registered(_handler_done) => pending::<DriverOutcome>().await,
                DriverStart::Rejected => panic!("the registered driver should start"),
            }
        });
        let driver_abort = driver.abort_handle();
        let supervisor = tokio::spawn(supervise(
            start_rx,
            driver_start_tx,
            driver,
            handler,
            socket,
            permit,
        ));

        assert!(runtime.register_driver(id, internal_sender, driver_abort.clone()));
        start_tx.send(true).unwrap();
        started.notified().await;

        driver_abort.abort();
        supervisor.await.unwrap();

        assert_eq!(cleanup_rx.try_recv().unwrap(), 1);
        assert_eq!(runtime.stats().active_connections, 0);
        assert_eq!(runtime.stats().closed_connections, 1);
    }
}
