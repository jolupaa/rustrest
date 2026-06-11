use std::future::pending;

use futures_util::{SinkExt, StreamExt};
use hyper::body::Bytes;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::Instant;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Role};

use super::runtime::ConnectionPermit;
use super::socket::{WebSocket, WebSocketHandler};
use super::types::{WebSocketCloseInfo, WebSocketCloseInitiator, WebSocketMessage};
use super::{ResolvedWebSocketConfig, WebSocketError};

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
}

pub(crate) struct DriverTask {
    pub abort_handle: AbortHandle,
    pub start_tx: oneshot::Sender<bool>,
}

enum DriverStart {
    Registered(oneshot::Receiver<()>),
    Rejected,
}

pub(crate) async fn spawn(
    upgraded: Upgraded,
    config: ResolvedWebSocketConfig,
    handler: WebSocketHandler,
    socket: WebSocket,
    channels: DriverChannels,
    permit: ConnectionPermit,
) -> DriverTask {
    let io = TokioIo::new(upgraded);
    let stream =
        WebSocketStream::from_raw_socket(io, Role::Server, Some(config.tungstenite_config())).await;
    let (start_tx, start_rx) = oneshot::channel();
    let (driver_start_tx, driver_start_rx) = oneshot::channel();
    let driver = tokio::spawn(run(stream, config, channels, driver_start_rx));
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
    } = channels;
    let outcome = drive(
        &mut stream,
        &inbound_tx,
        &mut outbound_rx,
        &mut control_rx,
        &mut handler_done,
        config.ping_interval,
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
    handler: WebSocketHandler,
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
        handler(socket).await;
        let _ = handler_done_tx.send(());
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

async fn drive(
    stream: &mut WebSocketTransport,
    inbound_tx: &mpsc::Sender<Result<WebSocketMessage, WebSocketError>>,
    outbound_rx: &mut mpsc::Receiver<OutboundCommand>,
    control_rx: &mut mpsc::Receiver<ControlCommand>,
    handler_done: &mut oneshot::Receiver<()>,
    ping_interval: Option<std::time::Duration>,
) -> DriverOutcome {
    let mut control_open = true;
    let mut outbound_open = true;
    let mut pending_close = None;
    let mut next_ping = ping_interval.map(|interval| Instant::now() + interval);

    loop {
        tokio::select! {
            biased;

            command = control_rx.recv(), if control_open => {
                let Some(command) = command else {
                    control_open = false;
                    continue;
                };
                let disconnect = matches!(&command, ControlCommand::Disconnect(_));
                if matches!(&command, ControlCommand::Close(_)) {
                    loop {
                        match outbound_rx.try_recv() {
                            Ok(OutboundCommand::Message(message)) => {
                                if let Err(error) = stream.send(message).await {
                                    return protocol_outcome(inbound_tx, error);
                                }
                            }
                            Err(mpsc::error::TryRecvError::Empty) => break,
                            Err(mpsc::error::TryRecvError::Disconnected) => {
                                outbound_open = false;
                                break;
                            }
                        }
                    }
                }
                if let ControlCommand::Close(frame) | ControlCommand::Disconnect(frame) = &command {
                    pending_close = Some(close_info(
                        frame.as_ref(),
                        if disconnect {
                            WebSocketCloseInitiator::Runtime
                        } else {
                            WebSocketCloseInitiator::Local
                        },
                        false,
                    ));
                }
                if let Err(error) = write_control(stream, command).await {
                    return protocol_outcome(inbound_tx, error);
                }
                if disconnect {
                    return DriverOutcome {
                        close_info: pending_close.unwrap_or_else(runtime_close_info),
                        handler_completed: false,
                    };
                }
            }

            command = outbound_rx.recv(), if outbound_open => {
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
                        if let Some(interval) = ping_interval {
                            next_ping = Some(Instant::now() + interval);
                        }
                        if message.is_ping() || message.is_close() {
                            if let Err(error) = stream.flush().await {
                                return protocol_outcome(inbound_tx, error);
                            }
                        }
                        if let WebSocketMessage::Close(frame) = &message {
                            pending_close = Some(close_info(
                                frame.as_ref(),
                                WebSocketCloseInitiator::Peer,
                                true,
                            ));
                        }
                        if inbound_tx.send(Ok(message)).await.is_err() {
                            return DriverOutcome {
                                close_info: pending_close.unwrap_or_else(handler_close_info),
                                handler_completed: false,
                            };
                        }
                    }
                    Some(Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed))
                        if pending_close.is_some() =>
                    {
                        return DriverOutcome {
                            close_info: pending_close.expect("el cierre pendiente fue comprobado"),
                            handler_completed: false,
                        };
                    }
                    Some(Err(error)) => return protocol_outcome(inbound_tx, error),
                    None => {
                        return DriverOutcome {
                            close_info: pending_close.unwrap_or_else(peer_disconnect_info),
                            handler_completed: false,
                        };
                    }
                }
            }

            _ = wait_for_ping(next_ping) => {
                if let Err(error) = stream.send(WebSocketMessage::Ping(Bytes::new())).await {
                    return protocol_outcome(inbound_tx, error);
                }
                next_ping = ping_interval.map(|interval| Instant::now() + interval);
            }

            _ = &mut *handler_done => {
                return DriverOutcome {
                    close_info: pending_close.unwrap_or_else(handler_close_info),
                    handler_completed: true,
                };
            }
        }
    }
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

async fn wait_for_ping(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => pending::<()>().await,
    }
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

fn handler_close_info() -> WebSocketCloseInfo {
    WebSocketCloseInfo {
        code: 1000,
        reason: String::new(),
        initiator: WebSocketCloseInitiator::Handler,
        clean: false,
    }
}

fn runtime_close_info() -> WebSocketCloseInfo {
    WebSocketCloseInfo {
        code: 1001,
        reason: "El servidor se esta cerrando".to_string(),
        initiator: WebSocketCloseInitiator::Runtime,
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
            8,
            8,
        );
        let (start_tx, start_rx) = oneshot::channel();
        let (driver_start_tx, driver_start_rx) = oneshot::channel();
        let (cleanup_tx, cleanup_rx) = mpsc::channel();
        let started = Arc::new(tokio::sync::Notify::new());
        let handler_started = started.clone();
        let cleanup_sender = Arc::new(Mutex::new(Some(cleanup_tx)));
        let handler_runtime = runtime.clone();
        let handler: WebSocketHandler = Arc::new(move |_socket| {
            let handler_runtime = handler_runtime.clone();
            let handler_started = handler_started.clone();
            let cleanup_tx = cleanup_sender.lock().unwrap().take().unwrap();
            Box::pin(async move {
                let _guard = HandlerGuard {
                    runtime: handler_runtime,
                    cleanup_tx,
                };
                handler_started.notify_one();
                pending::<()>().await;
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
