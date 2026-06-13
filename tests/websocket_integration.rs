use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use rustrest::{
    App, BackpressurePolicy, InMemoryWsBroker, WebSocketCapacityError, WebSocketConfig,
    WebSocketEvent, WebSocketObservation, WebSocketObserver, WebSocketRuntimeHandle,
    WsBroadcastError, WsBroadcastReport, WsBroker, WsBrokerPayload, WsBrokerPublication,
    WsBrokerTarget, WsError, WsHub, WsNodeId, WsPublicationId,
};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

const IO_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_RESPONSE_BYTES: usize = 16 * 1024;

struct ServerGuard(JoinHandle<()>);

#[derive(Clone)]
struct RecordingObserver(Arc<Mutex<Vec<String>>>);

impl WebSocketObserver for RecordingObserver {
    fn observe(&self, event: &WebSocketObservation<'_>) {
        let value = match event {
            WebSocketObservation::Accepted { id, route } => format!("accepted:{id}:{route}"),
            WebSocketObservation::Rejected { route, reason } => {
                format!("rejected:{route}:{reason}")
            }
            WebSocketObservation::Opened { id } => format!("opened:{id}"),
            WebSocketObservation::Message {
                id,
                outbound,
                bytes,
            } => format!("message:{id}:{outbound}:{bytes}"),
            WebSocketObservation::QueueSaturated { id, outbound } => {
                format!("queue:{id}:{outbound}")
            }
            WebSocketObservation::HeartbeatTimeout { id } => format!("heartbeat:{id}"),
            WebSocketObservation::Closed { id, code, clean } => {
                format!("closed:{id}:{code:?}:{clean}")
            }
            WebSocketObservation::HandlerFailed { id } => format!("handler_failed:{id}"),
            WebSocketObservation::ForcedShutdown { id } => format!("forced_shutdown:{id}"),
            _ => return,
        };
        self.0.lock().unwrap().push(value);
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn spawn_app(configure: impl FnOnce(&mut App)) -> (SocketAddr, ServerGuard) {
    let (addr, _runtime, server) = spawn_app_with_runtime(configure).await;
    (addr, server)
}

async fn spawn_app_with_runtime(
    configure: impl FnOnce(&mut App),
) -> (SocketAddr, WebSocketRuntimeHandle, ServerGuard) {
    let mut app = App::new();
    configure(&mut app);
    let runtime = app.websocket_runtime();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        app.serve(listener).await.unwrap();
    });

    (addr, runtime, ServerGuard(server))
}

async fn wait_for_connections(runtime: &WebSocketRuntimeHandle, active: usize, closed: u64) {
    tokio::time::timeout(IO_TIMEOUT, async {
        loop {
            let stats = runtime.stats();
            if stats.active_connections == active && stats.closed_connections == closed {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("websocket runtime stats should settle before the deadline");
}

async fn wait_for_broker(runtime: &WebSocketRuntimeHandle) {
    tokio::time::timeout(IO_TIMEOUT, async {
        loop {
            if runtime.stats().broker_connected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("websocket broker should connect before the deadline");
}

async fn spawn_broker_app(
    broker: Arc<InMemoryWsBroker>,
    node_id: u64,
    report_tx: Option<
        tokio::sync::mpsc::UnboundedSender<Result<WsBroadcastReport, WsBroadcastError>>,
    >,
) -> (SocketAddr, WebSocketRuntimeHandle, ServerGuard) {
    let hub = WsHub::builder()
        .broker(broker)
        .node_id(WsNodeId::new(node_id))
        .build()
        .unwrap();
    let mut app = App::new();
    app.websocket_hub(hub);
    for path in ["/chat/:channel", "/admin/chat/:channel"] {
        let report_tx = report_tx.clone();
        app.websocket(path, move |mut socket| {
            let report_tx = report_tx.clone();
            async move {
                while let Some(message) = socket.recv().await.unwrap() {
                    let Ok(text) = message.into_text() else {
                        continue;
                    };
                    if let Some(rooms) = text.strip_prefix("join:") {
                        socket.join_many(rooms.split(',')).await.unwrap();
                        socket.send_text("ready").await.unwrap();
                    } else if let Some(command) = text.strip_prefix("to:") {
                        let (room, payload) = command.split_once(':').unwrap();
                        let result = socket.to(room).send_text(payload).await;
                        if let Some(report_tx) = &report_tx {
                            report_tx.send(result).unwrap();
                        }
                    }
                }
            }
        });
    }
    let runtime = app.websocket_runtime();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = ServerGuard(tokio::spawn(async move {
        app.serve(listener).await.unwrap();
    }));
    (addr, runtime, server)
}

async fn raw_handshake(addr: SocketAddr, headers: &[(&str, &str)]) -> String {
    tokio::time::timeout(IO_TIMEOUT, async {
        let mut request = format!(
            "GET /ws HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nConnection: close\r\n"
        );
        for (name, value) in headers {
            request.push_str(name);
            request.push_str(": ");
            request.push_str(value);
            request.push_str("\r\n");
        }
        request.push_str("\r\n");

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();

        let mut response = Vec::new();
        while !response.windows(4).any(|window| window == b"\r\n\r\n") {
            assert!(
                response.len() < MAX_RESPONSE_BYTES,
                "response headers exceeded {MAX_RESPONSE_BYTES} bytes"
            );
            let mut chunk = [0_u8; 1024];
            let limit = (MAX_RESPONSE_BYTES - response.len()).min(chunk.len());
            let read = stream.read(&mut chunk[..limit]).await.unwrap();
            if read == 0 {
                break;
            }
            response.extend_from_slice(&chunk[..read]);
        }

        String::from_utf8_lossy(&response).into_owned()
    })
    .await
    .expect("handshake I/O should complete before the deadline")
}

async fn raw_websocket(addr: SocketAddr) -> TcpStream {
    tokio::time::timeout(IO_TIMEOUT, async {
        let request = format!(
            "GET /ws HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
        );
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();

        let mut response = Vec::new();
        while !response.windows(4).any(|window| window == b"\r\n\r\n") {
            let mut byte = [0_u8; 1];
            assert_eq!(stream.read(&mut byte).await.unwrap(), 1);
            response.push(byte[0]);
        }
        assert!(
            String::from_utf8_lossy(&response).starts_with("HTTP/1.1 101"),
            "{}",
            String::from_utf8_lossy(&response)
        );
        stream
    })
    .await
    .expect("raw websocket handshake should complete before the deadline")
}

async fn join_test_rooms(
    client: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
    rooms: &str,
) {
    client
        .send(Message::Text(format!("join:{rooms}").into()))
        .await
        .unwrap();
    let ready = tokio::time::timeout(IO_TIMEOUT, client.next())
        .await
        .expect("room join acknowledgement should arrive")
        .unwrap()
        .unwrap();
    assert_eq!(ready.into_text().unwrap(), "ready");
}

async fn assert_no_websocket_message(
    client: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
) {
    assert!(
        tokio::time::timeout(Duration::from_millis(75), client.next())
            .await
            .is_err(),
        "client unexpectedly received a websocket message"
    );
}

#[tokio::test]
async fn websocket_room_broadcast_excludes_sender_and_respects_route_scope() {
    let (report_tx, mut report_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut app = App::new();
    let hub = app.websocket_hub_handle();
    for path in ["/chat/:channel", "/admin/chat/:channel"] {
        let report_tx = report_tx.clone();
        app.websocket(path, move |mut socket| {
            let report_tx = report_tx.clone();
            async move {
                while let Some(message) = socket.recv().await.unwrap() {
                    let Ok(text) = message.into_text() else {
                        continue;
                    };
                    if let Some(rooms) = text.strip_prefix("join:") {
                        socket.join_many(rooms.split(',')).await.unwrap();
                        socket.send_text("ready").await.unwrap();
                    } else if let Some(command) = text.strip_prefix("to:") {
                        let (room, payload) = command.split_once(':').unwrap();
                        let report = socket.to(room).send_text(payload).await.unwrap();
                        report_tx.send(report).unwrap();
                    }
                }
            }
        });
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _server = ServerGuard(tokio::spawn(async move {
        app.serve(listener).await.unwrap();
    }));

    let (mut origin, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/chat/a"))
        .await
        .unwrap();
    let (mut peer, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/chat/b"))
        .await
        .unwrap();
    let (mut admin, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/admin/chat/a"))
        .await
        .unwrap();
    join_test_rooms(&mut origin, "general").await;
    join_test_rooms(&mut peer, "general").await;
    join_test_rooms(&mut admin, "general").await;

    origin
        .send(Message::Text("to:general:hola-room".into()))
        .await
        .unwrap();
    let report = tokio::time::timeout(IO_TIMEOUT, report_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.matched, 1);
    assert_eq!(report.enqueued, 1);
    assert_eq!(report.rejected, 0);
    assert_eq!(report.disconnected, 0);
    let message = tokio::time::timeout(IO_TIMEOUT, peer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(message.into_text().unwrap(), "hola-room");
    assert_no_websocket_message(&mut origin).await;
    assert_no_websocket_message(&mut admin).await;

    let report = hub
        .route("/chat/:channel")
        .all()
        .send_text("solo-chat")
        .await
        .unwrap();
    assert_eq!(report.matched, 2);
    for client in [&mut origin, &mut peer] {
        let message = tokio::time::timeout(IO_TIMEOUT, client.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(message.into_text().unwrap(), "solo-chat");
    }
    assert_no_websocket_message(&mut admin).await;

    let report = hub.all().send_text("todos").await.unwrap();
    assert_eq!(report.matched, 3);
    for client in [&mut origin, &mut peer, &mut admin] {
        let message = tokio::time::timeout(IO_TIMEOUT, client.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(message.into_text().unwrap(), "todos");
    }
}

#[tokio::test]
async fn websocket_multi_room_deduplicates_local_recipient() {
    let (report_tx, mut report_rx) = tokio::sync::mpsc::unbounded_channel();
    let (addr, _server) = spawn_app(move |app| {
        app.websocket("/chat/:channel", move |mut socket| {
            let report_tx = report_tx.clone();
            async move {
                while let Some(message) = socket.recv().await.unwrap() {
                    let Ok(text) = message.into_text() else {
                        continue;
                    };
                    if let Some(rooms) = text.strip_prefix("join:") {
                        socket.join_many(rooms.split(',')).await.unwrap();
                        socket.send_text("ready").await.unwrap();
                    } else if let Some(command) = text.strip_prefix("to_many:") {
                        let (rooms, payload) = command.split_once(':').unwrap();
                        let report = socket
                            .to_many(rooms.split(','))
                            .send_text(payload)
                            .await
                            .unwrap();
                        report_tx.send(report).unwrap();
                    }
                }
            }
        });
    })
    .await;
    let (mut origin, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/chat/a"))
        .await
        .unwrap();
    let (mut peer, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/chat/b"))
        .await
        .unwrap();
    join_test_rooms(&mut peer, "general,equipo-7").await;

    origin
        .send(Message::Text("to_many:general,equipo-7:una-vez".into()))
        .await
        .unwrap();
    let report = tokio::time::timeout(IO_TIMEOUT, report_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.matched, 1);
    assert_eq!(report.enqueued, 1);
    let message = tokio::time::timeout(IO_TIMEOUT, peer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(message.into_text().unwrap(), "una-vez");
    assert_no_websocket_message(&mut peer).await;
    assert_no_websocket_message(&mut origin).await;
}

#[tokio::test]
async fn websocket_broker_two_nodes_delivers_once_and_preserves_route_scope() {
    let broker = Arc::new(InMemoryWsBroker::new(64));
    let (report_tx, mut report_rx) = tokio::sync::mpsc::unbounded_channel();
    let (addr_a, runtime_a, _server_a) =
        spawn_broker_app(broker.clone(), 101, Some(report_tx)).await;
    let (addr_b, runtime_b, _server_b) = spawn_broker_app(broker.clone(), 202, None).await;
    wait_for_broker(&runtime_a).await;
    wait_for_broker(&runtime_b).await;

    let (mut origin, _) = tokio_tungstenite::connect_async(format!("ws://{addr_a}/chat/a"))
        .await
        .unwrap();
    let (mut local_peer, _) = tokio_tungstenite::connect_async(format!("ws://{addr_a}/chat/b"))
        .await
        .unwrap();
    let (mut remote_peer, _) = tokio_tungstenite::connect_async(format!("ws://{addr_b}/chat/c"))
        .await
        .unwrap();
    let (mut isolated, _) = tokio_tungstenite::connect_async(format!("ws://{addr_b}/admin/chat/c"))
        .await
        .unwrap();
    for client in [
        &mut origin,
        &mut local_peer,
        &mut remote_peer,
        &mut isolated,
    ] {
        join_test_rooms(client, "general").await;
    }

    origin
        .send(Message::Text("to:general:entre-nodos".into()))
        .await
        .unwrap();
    let report = tokio::time::timeout(IO_TIMEOUT, report_rx.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(report.matched, 1);
    assert_eq!(report.enqueued, 1);
    assert_eq!(report.remote, rustrest::WsRemotePublish::Published);
    for client in [&mut local_peer, &mut remote_peer] {
        let message = tokio::time::timeout(IO_TIMEOUT, client.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(message.into_text().unwrap(), "entre-nodos");
        assert_no_websocket_message(client).await;
    }
    assert_no_websocket_message(&mut origin).await;
    assert_no_websocket_message(&mut isolated).await;

    let duplicate = WsBrokerPublication::new(
        WsPublicationId::new(77),
        WsNodeId::new(999),
        WsBrokerTarget::RouteRooms {
            route: "/chat/:channel".into(),
            rooms: vec!["general".into()],
        },
        WsBrokerPayload::Text("sin-duplicado".into()),
    );
    broker.publish(duplicate.clone()).await.unwrap();
    broker.publish(duplicate).await.unwrap();
    for client in [&mut origin, &mut local_peer, &mut remote_peer] {
        let message = tokio::time::timeout(IO_TIMEOUT, client.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(message.into_text().unwrap(), "sin-duplicado");
        assert_no_websocket_message(client).await;
    }
    assert_no_websocket_message(&mut isolated).await;
}

#[tokio::test]
async fn websocket_broker_failure_keeps_local_report() {
    let broker = Arc::new(InMemoryWsBroker::new(64));
    let (report_tx, mut report_rx) = tokio::sync::mpsc::unbounded_channel();
    let (addr, runtime, _server) = spawn_broker_app(broker.clone(), 303, Some(report_tx)).await;
    wait_for_broker(&runtime).await;
    let (mut origin, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/chat/a"))
        .await
        .unwrap();
    let (mut local_peer, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/chat/b"))
        .await
        .unwrap();
    join_test_rooms(&mut origin, "general").await;
    join_test_rooms(&mut local_peer, "general").await;
    broker.close();

    origin
        .send(Message::Text("to:general:solo-local".into()))
        .await
        .unwrap();
    let result = tokio::time::timeout(IO_TIMEOUT, report_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let WsBroadcastError::Broker {
        source: _,
        local_report,
    } = result.unwrap_err()
    else {
        panic!("expected broker failure");
    };
    assert_eq!(local_report.matched, 1);
    assert_eq!(local_report.enqueued, 1);
    let message = tokio::time::timeout(IO_TIMEOUT, local_peer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(message.into_text().unwrap(), "solo-local");
    assert_no_websocket_message(&mut origin).await;
}

#[tokio::test]
async fn websocket_local_administration_sends_disconnects_and_snapshots_rooms() {
    let (id_tx, mut id_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut app = App::new();
    let hub = app.websocket_hub_handle();
    app.websocket("/admin/:tenant", move |socket| {
        let id_tx = id_tx.clone();
        async move {
            socket.join_many(["zeta", "general"]).await.unwrap();
            id_tx.send(socket.id()).unwrap();
            std::future::pending::<()>().await;
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _server = ServerGuard(tokio::spawn(async move {
        app.serve(listener).await.unwrap();
    }));
    let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/admin/acme"))
        .await
        .unwrap();
    let id = tokio::time::timeout(IO_TIMEOUT, id_rx.recv())
        .await
        .unwrap()
        .unwrap();

    let local = hub.local_socket(id).expect("local socket should exist");
    assert_eq!(local.id(), id);
    assert_eq!(local.route(), "/admin/:tenant");
    assert!(local.remote_addr().is_some());
    assert_eq!(local.rooms(), ["general", "zeta"]);
    assert!(local.opened_at() <= SystemTime::now());
    assert_eq!(local.lifecycle(), rustrest::WebSocketLifecycleState::Open);
    assert_eq!(hub.local_connection_count(), 1);

    local
        .send_event("account:changed", &json!({ "active": true }))
        .await
        .unwrap();
    let event = tokio::time::timeout(IO_TIMEOUT, client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let event: WebSocketEvent<serde_json::Value> =
        serde_json::from_str(event.to_text().unwrap()).unwrap();
    assert_eq!(event.event, "account:changed");
    assert_eq!(event.data, json!({ "active": true }));

    let disconnect_hub = hub.clone();
    let disconnect = tokio::spawn(async move {
        disconnect_hub
            .disconnect_local(id, 1008, "no autorizado")
            .await
    });
    let close = receive_close_frame(&mut client).await;
    assert_eq!(u16::from(close.code), 1008);
    assert_eq!(close.reason, "no autorizado");
    client.flush().await.unwrap();
    disconnect.await.unwrap().unwrap();

    assert!(hub.local_socket(id).is_none());
    assert_eq!(hub.local_connection_count(), 0);
    assert!(matches!(
        hub.disconnect_local(id, 1008, "otra vez").await,
        Err(WsError::ConnectionNotFound(missing)) if missing == id
    ));
}

#[tokio::test]
async fn websocket_runtime_stats_and_observer_record_message_metadata() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let observations = Arc::new(Mutex::new(Vec::new()));
    let release = Arc::new(Semaphore::new(0));
    let observer_events = observations.clone();
    let handler_release = release.clone();
    let (addr, runtime, _server) = spawn_app_with_runtime(move |app| {
        app.websocket_observer(Arc::new(RecordingObserver(observer_events)));
        app.websocket_with(
            "/ws",
            WebSocketConfig::new().protocols(&["superchat"]),
            move |mut socket| {
                let handler_release = handler_release.clone();
                async move {
                    if let Some(message) = socket.recv().await.unwrap() {
                        socket.send(message).await.unwrap();
                    }
                    handler_release.acquire().await.unwrap().forget();
                }
            },
        );
    })
    .await;

    let mut request = format!("ws://{addr}/ws").into_client_request().unwrap();
    request
        .headers_mut()
        .insert("sec-websocket-protocol", "superchat".parse().unwrap());
    let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    wait_for_connections(&runtime, 1, 0).await;

    let snapshot = runtime.connections().pop().unwrap();
    assert_eq!(snapshot.route, "/ws");
    assert!(snapshot.remote_addr.is_some());
    assert_eq!(snapshot.protocol.as_deref(), Some("superchat"));
    assert!(snapshot.opened_at <= SystemTime::now());

    let payload = "contenido-privado";
    client.send(Message::Text(payload.into())).await.unwrap();
    let echo = tokio::time::timeout(IO_TIMEOUT, client.next())
        .await
        .expect("echo should arrive before the deadline")
        .unwrap()
        .unwrap();
    assert_eq!(echo.into_text().unwrap(), payload);
    release.add_permits(1);
    let close = receive_close_frame(&mut client).await;
    assert_eq!(u16::from(close.code), 1000);
    client.flush().await.unwrap();
    wait_for_connections(&runtime, 0, 1).await;

    let stats = runtime.stats();
    assert_eq!(stats.messages_received, 1);
    assert_eq!(stats.messages_sent, 1);
    assert_eq!(stats.bytes_received, payload.len() as u64);
    assert_eq!(stats.bytes_sent, payload.len() as u64);

    let events = observations.lock().unwrap().clone();
    let id = snapshot.id.to_string();
    assert!(events.iter().any(|event| event == &format!("opened:{id}")));
    assert!(
        events
            .iter()
            .any(|event| event == &format!("message:{id}:false:{}", payload.len()))
    );
    assert!(
        events
            .iter()
            .any(|event| event == &format!("message:{id}:true:{}", payload.len()))
    );
    assert!(
        events
            .iter()
            .any(|event| event.starts_with(&format!("closed:{id}:")))
    );
    assert!(events.iter().all(|event| !event.contains(payload)));
}

#[tokio::test]
async fn websocket_runtime_stats_count_messages_flushed_before_local_close() {
    let (addr, runtime, _server) = spawn_app_with_runtime(|app| {
        app.websocket("/ws", |socket| async move {
            let (_receiver, sender) = socket.split();
            sender.try_send(Message::Text("antes".into())).unwrap();
            sender.close().await.unwrap();
        });
    })
    .await;
    let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    let message = tokio::time::timeout(IO_TIMEOUT, client.next())
        .await
        .expect("queued message should arrive before close")
        .unwrap()
        .unwrap();
    assert_eq!(message.into_text().unwrap(), "antes");
    let close = receive_close_frame(&mut client).await;
    assert_eq!(u16::from(close.code), 1000);
    client.flush().await.unwrap();
    wait_for_connections(&runtime, 0, 1).await;

    let stats = runtime.stats();
    assert_eq!(stats.messages_sent, 1);
    assert_eq!(stats.bytes_sent, 5);
}

#[tokio::test]
async fn websocket_observer_panic_does_not_break_echo() {
    struct PanickingObserver;

    impl WebSocketObserver for PanickingObserver {
        fn observe(&self, _event: &WebSocketObservation<'_>) {
            panic!("observer panic");
        }
    }

    let (addr, _server) = spawn_app(|app| {
        app.websocket_observer(Arc::new(PanickingObserver));
        app.websocket("/ws", |mut socket| async move {
            if let Some(message) = socket.recv().await.unwrap() {
                socket.send(message).await.unwrap();
            }
        });
    })
    .await;
    let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    client.send(Message::Text("eco".into())).await.unwrap();
    let echo = tokio::time::timeout(IO_TIMEOUT, client.next())
        .await
        .expect("observer panic must not block echo")
        .unwrap()
        .unwrap();
    assert_eq!(echo.into_text().unwrap(), "eco");
}

#[cfg(feature = "tracing")]
#[tokio::test]
async fn websocket_observer_tracing_records_metadata_without_payload() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Metadata, Subscriber};

    struct FieldVisitor<'a>(&'a Arc<Mutex<Vec<String>>>);

    impl Visit for FieldVisitor<'_> {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0
                .lock()
                .unwrap()
                .push(format!("{}={value:?}", field.name()));
        }
    }

    struct CapturingSubscriber {
        fields: Arc<Mutex<Vec<String>>>,
        next_id: AtomicU64,
    }

    impl Subscriber for CapturingSubscriber {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, attributes: &Attributes<'_>) -> Id {
            self.fields
                .lock()
                .unwrap()
                .push(format!("span={}", attributes.metadata().name()));
            attributes.record(&mut FieldVisitor(&self.fields));
            Id::from_u64(self.next_id.fetch_add(1, Ordering::Relaxed))
        }

        fn record(&self, _span: &Id, values: &Record<'_>) {
            values.record(&mut FieldVisitor(&self.fields));
        }

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, event: &Event<'_>) {
            self.fields
                .lock()
                .unwrap()
                .push(format!("event={}", event.metadata().name()));
            event.record(&mut FieldVisitor(&self.fields));
        }

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

    let fields = Arc::new(Mutex::new(Vec::new()));
    let subscriber = CapturingSubscriber {
        fields: fields.clone(),
        next_id: AtomicU64::new(1),
    };
    tracing::subscriber::set_global_default(subscriber).unwrap();
    let mut app = App::new();
    app.websocket("/ws", |mut socket| async move {
        if let Some(message) = socket.recv().await.unwrap() {
            socket.send(message).await.unwrap();
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve(listener));
    let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    let payload = "contenido-ultrasecreto";
    client.send(Message::Text(payload.into())).await.unwrap();
    let _ = tokio::time::timeout(IO_TIMEOUT, client.next())
        .await
        .expect("echo should arrive before the deadline")
        .unwrap()
        .unwrap();
    let _ = receive_close_frame(&mut client).await;
    client.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    server.abort();

    let fields = fields.lock().unwrap();
    assert!(
        fields
            .iter()
            .any(|field| field == "span=websocket.connection"),
        "captured tracing fields: {fields:?}"
    );
    assert!(fields.iter().any(|field| field.starts_with("ws.id=")));
    assert!(fields.iter().any(|field| field == "ws.route=\"/ws\""));
    assert!(fields.iter().any(|field| field.starts_with("ws.bytes=")));
    assert!(fields.iter().all(|field| !field.contains(payload)));
}

#[tokio::test]
async fn websocket_rejects_invalid_version_with_426() {
    let (addr, _server) = spawn_app(|app| {
        app.websocket("/ws", |_socket| async move {});
    })
    .await;

    let response = raw_handshake(
        addr,
        &[
            ("Sec-WebSocket-Version", "12"),
            ("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ=="),
        ],
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 426"), "{response}");
    assert!(response.contains("sec-websocket-version: 13"), "{response}");
}

#[tokio::test]
async fn websocket_rejects_key_that_is_not_sixteen_decoded_bytes() {
    let (addr, _server) = spawn_app(|app| {
        app.websocket("/ws", |_socket| async move {});
    })
    .await;

    let response = raw_handshake(
        addr,
        &[
            ("Sec-WebSocket-Version", "13"),
            ("Sec-WebSocket-Key", "YQ=="),
        ],
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
}

#[tokio::test]
async fn websocket_rejects_duplicate_key() {
    let (addr, _server) = spawn_app(|app| {
        app.websocket("/ws", |_socket| async move {});
    })
    .await;

    let response = raw_handshake(
        addr,
        &[
            ("Sec-WebSocket-Version", "13"),
            ("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ=="),
            ("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ=="),
        ],
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
}

#[tokio::test]
async fn websocket_rejects_duplicate_version() {
    let (addr, _server) = spawn_app(|app| {
        app.websocket("/ws", |_socket| async move {});
    })
    .await;

    let response = raw_handshake(
        addr,
        &[
            ("Sec-WebSocket-Version", "13"),
            ("Sec-WebSocket-Version", "13"),
            ("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ=="),
        ],
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
}

#[tokio::test]
async fn websocket_process_capacity_rejects_before_101() {
    let (addr, _server) = spawn_app(|app| {
        app.websocket_defaults(WebSocketConfig::new().max_connections(1));
        app.websocket("/ws", |_socket| async move {
            std::future::pending::<()>().await;
        });
    })
    .await;
    let (_first, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    let response = raw_handshake(
        addr,
        &[
            ("Sec-WebSocket-Version", "13"),
            ("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ=="),
        ],
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 503"), "{response}");
}

#[tokio::test]
async fn websocket_route_capacity_rejects_before_101() {
    let (addr, _server) = spawn_app(|app| {
        app.websocket_with(
            "/ws",
            WebSocketConfig::new().max_connections(1),
            |_socket| async move {
                std::future::pending::<()>().await;
            },
        );
    })
    .await;
    let (_first, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    let response = raw_handshake(
        addr,
        &[
            ("Sec-WebSocket-Version", "13"),
            ("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ=="),
        ],
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 503"), "{response}");
}

#[tokio::test]
async fn websocket_ip_capacity_rejects_with_retry_after_before_101() {
    let (addr, _server) = spawn_app(|app| {
        app.websocket_with(
            "/ws",
            WebSocketConfig::new().max_connections_per_ip(1),
            |_socket| async move {
                std::future::pending::<()>().await;
            },
        );
    })
    .await;
    let (_first, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    let response = raw_handshake(
        addr,
        &[
            ("Sec-WebSocket-Version", "13"),
            ("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ=="),
        ],
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 429"), "{response}");
    assert!(response.contains("retry-after: 1"), "{response}");
}

#[tokio::test]
async fn websocket_routes_exchange_messages_and_events() {
    let mut app = App::new();
    app.websocket("/ws", |mut socket| async move {
        while let Some(message) = socket.recv().await.unwrap() {
            if message.is_text() {
                let text = message.into_text().unwrap().to_string();
                socket.send_text(&format!("echo:{}", text)).await.unwrap();
                socket
                    .send_event("server:echo", &json!({ "text": text }))
                    .await
                    .unwrap();
                socket.close().await.unwrap();
                break;
            }
        }
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve(listener));

    let (mut client, _response) = tokio_tungstenite::connect_async(format!("ws://{}/ws", addr))
        .await
        .unwrap();

    client.send(Message::Text("hello".into())).await.unwrap();

    let text = client.next().await.unwrap().unwrap();
    assert_eq!(text.into_text().unwrap(), "echo:hello");

    let event = client.next().await.unwrap().unwrap();
    let event: WebSocketEvent<serde_json::Value> =
        serde_json::from_str(event.to_text().unwrap()).unwrap();
    assert_eq!(event.event, "server:echo");
    assert_eq!(event.data, json!({ "text": "hello" }));

    let close = client.next().await.unwrap().unwrap();
    assert!(close.is_close());
    server.abort();
}

#[tokio::test]
async fn websocket_config_negotiates_protocol_pings_and_limits_message_size() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let mut app = App::new();
    let config = WebSocketConfig::new()
        .protocols(&["superchat"])
        .ping_interval(Duration::from_millis(100))
        .pong_timeout(Duration::from_millis(50))
        .max_message_size(1024);
    app.websocket_with("/ws", config, |mut socket| async move {
        let protocol = socket.protocol().unwrap_or("none").to_string();
        while let Ok(Some(message)) = socket.recv().await {
            if message.is_text() {
                let text = message.into_text().unwrap();
                if socket
                    .send_text(&format!("{}:{}", protocol, text))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve(listener));

    let mut ws_request = format!("ws://{}/ws", addr).into_client_request().unwrap();
    ws_request
        .headers_mut()
        .insert("sec-websocket-protocol", "chat, superchat".parse().unwrap());
    let (mut client, response) = tokio_tungstenite::connect_async(ws_request).await.unwrap();

    // The server picked the protocol it supports and echoed it.
    assert_eq!(
        response.headers().get("sec-websocket-protocol").unwrap(),
        "superchat"
    );

    // Round-trip: the handler sees the negotiated protocol too.
    client.send(Message::Text("hola".into())).await.unwrap();
    let echo = tokio::time::timeout(Duration::from_secs(2), client.next())
        .await
        .expect("echo in time")
        .unwrap()
        .unwrap();
    assert_eq!(echo.into_text().unwrap(), "superchat:hola");

    // With the connection idle, the keepalive ping arrives.
    let ping = tokio::time::timeout(Duration::from_secs(2), client.next())
        .await
        .expect("ping in time")
        .unwrap()
        .unwrap();
    assert!(ping.is_ping(), "expected keepalive ping, got {ping:?}");

    // A message over max_message_size closes with RFC 6455 code 1009.
    let big = "x".repeat(8 * 1024);
    client.send(Message::Text(big.into())).await.unwrap();
    let close = loop {
        match tokio::time::timeout(Duration::from_secs(2), client.next())
            .await
            .expect("connection should settle in time")
        {
            Some(Ok(message)) if message.is_ping() || message.is_pong() => continue,
            Some(Ok(Message::Close(Some(frame)))) => break frame,
            Some(Ok(Message::Close(None))) => panic!("oversized close should include code 1009"),
            Some(Ok(message)) => {
                panic!("oversized message should not produce a reply, got {message:?}")
            }
            Some(Err(error)) => panic!("oversized message should close cleanly: {error}"),
            None => panic!("oversized message should produce Close 1009"),
        }
    };
    assert_eq!(u16::from(close.code), 1009);
    server.abort();
}

#[tokio::test]
async fn websocket_sender_progresses_independently() {
    let (addr, _server) = spawn_app(|app| {
        app.websocket("/ws", |socket| async move {
            let (_receiver, sender) = socket.split();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(25)).await;
                sender.send_text("background").await.unwrap();
            });
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
    })
    .await;

    let (mut client, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    let message = tokio::time::timeout(IO_TIMEOUT, client.next())
        .await
        .expect("background send should arrive before the deadline")
        .unwrap()
        .unwrap();

    assert_eq!(message.into_text().unwrap(), "background");
}

#[tokio::test]
async fn websocket_sender_survives_a_dropped_receiver() {
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::unbounded_channel();
    let send_gate = std::sync::Arc::new(Semaphore::new(0));
    let handler_gate = send_gate.clone();
    let (addr, _server) = spawn_app(move |app| {
        let handler_gate = handler_gate.clone();
        app.websocket("/ws", move |socket| {
            let handler_gate = handler_gate.clone();
            let ready_tx = ready_tx.clone();
            async move {
                let (receiver, sender) = socket.split();
                drop(receiver);
                ready_tx.send(()).unwrap();
                handler_gate.acquire().await.unwrap().forget();
                sender.send_text("still-open").await.unwrap();
            }
        });
    })
    .await;

    let (mut client, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    tokio::time::timeout(IO_TIMEOUT, ready_rx.recv())
        .await
        .expect("handler should drop its receiver before the deadline")
        .expect("handler should announce that its receiver was dropped");

    client.send(Message::Text("ignored".into())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(25)).await;
    send_gate.add_permits(1);

    let message = tokio::time::timeout(IO_TIMEOUT, client.next())
        .await
        .expect("sender should remain usable after inbound traffic")
        .expect("connection should remain open")
        .expect("server should send a message");
    assert_eq!(message.into_text().unwrap(), "still-open");
}

#[tokio::test]
async fn websocket_control_frames_remain_visible_to_recv() {
    let (seen_tx, mut seen_rx) = tokio::sync::mpsc::unbounded_channel();
    let (addr, _server) = spawn_app(move |app| {
        app.websocket("/ws", move |mut socket| {
            let seen_tx = seen_tx.clone();
            async move {
                while let Some(message) = socket.recv().await.unwrap() {
                    let kind = if message.is_ping() {
                        "ping"
                    } else if message.is_pong() {
                        "pong"
                    } else if message.is_close() {
                        "close"
                    } else {
                        continue;
                    };
                    seen_tx.send(kind).unwrap();
                    if message.is_close() {
                        break;
                    }
                }
            }
        });
    })
    .await;

    let (mut client, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    client.send(Message::Ping("visible".into())).await.unwrap();
    assert_eq!(
        tokio::time::timeout(IO_TIMEOUT, seen_rx.recv())
            .await
            .unwrap()
            .unwrap(),
        "ping"
    );

    client.send(Message::Pong("visible".into())).await.unwrap();
    assert_eq!(
        tokio::time::timeout(IO_TIMEOUT, seen_rx.recv())
            .await
            .unwrap()
            .unwrap(),
        "pong"
    );

    client.close(None).await.unwrap();
    assert_eq!(
        tokio::time::timeout(IO_TIMEOUT, seen_rx.recv())
            .await
            .unwrap()
            .unwrap(),
        "close"
    );
}

#[tokio::test]
async fn websocket_runtime_releases_permit_after_handler_completion() {
    let release = std::sync::Arc::new(Semaphore::new(0));
    let handler_release = release.clone();
    let (addr, runtime, _server) = spawn_app_with_runtime(move |app| {
        let handler_release = handler_release.clone();
        app.websocket("/ws", move |_socket| {
            let handler_release = handler_release.clone();
            async move {
                handler_release.acquire().await.unwrap().forget();
            }
        });
    })
    .await;

    let (mut client, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    wait_for_connections(&runtime, 1, 0).await;

    release.add_permits(1);
    let close = receive_close_frame(&mut client).await;
    assert_eq!(u16::from(close.code), 1000);
    client.flush().await.unwrap();
    wait_for_connections(&runtime, 0, 1).await;
    let stats = runtime.stats();
    assert_eq!(stats.accepted_connections, 1);
    assert_eq!(stats.rejected_connections, 0);
}

#[tokio::test]
async fn websocket_runtime_releases_permit_after_transport_close() {
    let (addr, runtime, _server) = spawn_app_with_runtime(|app| {
        app.websocket("/ws", |_socket| async move {
            std::future::pending::<()>().await;
        });
    })
    .await;

    let (mut client, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    wait_for_connections(&runtime, 1, 0).await;

    client.close(None).await.unwrap();
    drop(client);
    wait_for_connections(&runtime, 0, 1).await;
    assert_eq!(runtime.stats().accepted_connections, 1);
}

#[tokio::test]
async fn websocket_observer_records_queue_saturation_for_slow_consumer() {
    let (saturated_tx, mut saturated_rx) = tokio::sync::mpsc::unbounded_channel();
    let observations = Arc::new(Mutex::new(Vec::new()));
    let observer_events = observations.clone();
    let (addr, runtime, _server) = spawn_app_with_runtime(move |app| {
        app.websocket_observer(Arc::new(RecordingObserver(observer_events)));
        app.websocket_with(
            "/ws",
            WebSocketConfig::new()
                .outbound_capacity(1)
                .backpressure_policy(BackpressurePolicy::Disconnect),
            move |socket| {
                let saturated_tx = saturated_tx.clone();
                async move {
                    let (_receiver, sender) = socket.split();
                    for sequence in 0..100_000_u32 {
                        match sender.send_text(&format!("message-{sequence}")).await {
                            Ok(()) => {}
                            Err(WsError::Capacity(WebSocketCapacityError::OutboundQueue)) => {
                                saturated_tx.send(()).unwrap();
                                return;
                            }
                            Err(error) => panic!("unexpected send error: {error}"),
                        }
                    }
                    panic!("outbound queue did not saturate");
                }
            },
        );
    })
    .await;

    let (mut client, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    tokio::time::timeout(IO_TIMEOUT, saturated_rx.recv())
        .await
        .expect("outbound queue should saturate before the deadline")
        .expect("handler should report saturation");

    let close = tokio::time::timeout(IO_TIMEOUT, async {
        loop {
            match client.next().await {
                Some(Ok(Message::Close(frame))) => break frame,
                Some(Ok(_)) => continue,
                Some(Err(error)) => panic!("connection failed before Close: {error}"),
                None => panic!("connection ended before Close"),
            }
        }
    })
    .await
    .expect("Close 1013 should arrive before the deadline")
    .expect("Close 1013 should include a frame");
    assert_eq!(u16::from(close.code), 1013);
    client.flush().await.unwrap();

    wait_for_connections(&runtime, 0, 1).await;
    let stats = runtime.stats();
    assert_eq!(stats.saturated_sends, 1);
    assert_eq!(stats.closed_connections, 1);
    assert!(
        observations
            .lock()
            .unwrap()
            .iter()
            .any(|event| event.starts_with("queue:") && event.ends_with(":true"))
    );
}

async fn receive_close_frame(
    client: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
) -> tokio_tungstenite::tungstenite::protocol::CloseFrame {
    tokio::time::timeout(IO_TIMEOUT, async {
        loop {
            match client.next().await {
                Some(Ok(Message::Close(Some(frame)))) => break frame,
                Some(Ok(Message::Close(None))) => panic!("Close frame should include a code"),
                Some(Ok(_)) => continue,
                Some(Err(error)) => panic!("connection failed before Close: {error}"),
                None => panic!("connection ended before Close"),
            }
        }
    })
    .await
    .expect("Close should arrive before the deadline")
}

#[tokio::test]
async fn websocket_heartbeat_ping_does_not_require_handler_recv() {
    let (addr, _server) = spawn_app(|app| {
        app.websocket_with(
            "/ws",
            WebSocketConfig::new()
                .ping_interval(Duration::from_millis(80))
                .pong_timeout(Duration::from_millis(20)),
            |socket| async move {
                std::future::pending::<()>().await;
                drop(socket);
            },
        );
    })
    .await;
    let (mut client, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    let ping = tokio::time::timeout(IO_TIMEOUT, client.next())
        .await
        .expect("heartbeat Ping should arrive")
        .unwrap()
        .unwrap();
    assert!(ping.is_ping());
}

#[tokio::test]
async fn websocket_heartbeat_matching_pong_keeps_connection_alive() {
    let (addr, _server) = spawn_app(|app| {
        app.websocket_with(
            "/ws",
            WebSocketConfig::new()
                .ping_interval(Duration::from_millis(80))
                .pong_timeout(Duration::from_millis(30)),
            |socket| async move {
                std::future::pending::<()>().await;
                drop(socket);
            },
        );
    })
    .await;
    let (mut client, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    let first = tokio::time::timeout(IO_TIMEOUT, client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let payload = match first {
        Message::Ping(payload) => payload,
        message => panic!("expected Ping, got {message:?}"),
    };
    client.send(Message::Pong(payload)).await.unwrap();

    let second = tokio::time::timeout(IO_TIMEOUT, client.next())
        .await
        .expect("a matching Pong should allow the next heartbeat")
        .unwrap()
        .unwrap();
    assert!(second.is_ping(), "expected another Ping, got {second:?}");
}

#[tokio::test]
async fn websocket_observer_records_heartbeat_timeout() {
    let observations = Arc::new(Mutex::new(Vec::new()));
    let observer_events = observations.clone();
    let (addr, runtime, _server) = spawn_app_with_runtime(move |app| {
        app.websocket_observer(Arc::new(RecordingObserver(observer_events)));
        app.websocket_with(
            "/ws",
            WebSocketConfig::new()
                .ping_interval(Duration::from_millis(80))
                .pong_timeout(Duration::from_millis(20))
                .close_timeout(Duration::from_millis(50)),
            |_socket| async move { std::future::pending::<()>().await },
        );
    })
    .await;
    let (mut client, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    let ping = tokio::time::timeout(IO_TIMEOUT, client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(ping.is_ping());

    tokio::time::sleep(Duration::from_millis(50)).await;
    let close = receive_close_frame(&mut client).await;
    assert_eq!(u16::from(close.code), 1001);
    wait_for_connections(&runtime, 0, 1).await;
    assert_eq!(runtime.stats().heartbeat_timeouts, 1);
    assert!(
        observations
            .lock()
            .unwrap()
            .iter()
            .any(|event| event.starts_with("heartbeat:"))
    );
}

#[tokio::test]
async fn websocket_close_idle_timeout_uses_1001() {
    let (addr, _server) = spawn_app(|app| {
        app.websocket_with(
            "/ws",
            WebSocketConfig::new()
                .disable_ping()
                .idle_timeout(Duration::from_millis(50))
                .close_timeout(Duration::from_millis(50)),
            |_socket| async move { std::future::pending::<()>().await },
        );
    })
    .await;
    let (mut client, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    let close = receive_close_frame(&mut client).await;
    assert_eq!(u16::from(close.code), 1001);
}

#[tokio::test]
async fn websocket_close_lifetime_expires_while_messages_flow() {
    let (addr, _server) = spawn_app(|app| {
        app.websocket_with(
            "/ws",
            WebSocketConfig::new()
                .disable_ping()
                .idle_timeout(Duration::from_secs(1))
                .max_connection_lifetime(Duration::from_millis(100))
                .close_timeout(Duration::from_millis(50)),
            |_socket| async move { std::future::pending::<()>().await },
        );
    })
    .await;
    let (mut client, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    for _ in 0..4 {
        client.send(Message::Text("active".into())).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let close = receive_close_frame(&mut client).await;
    assert_eq!(u16::from(close.code), 1001);
}

#[tokio::test]
async fn websocket_close_handshake_reports_clean_local_close() {
    let (close_tx, mut close_rx) = tokio::sync::mpsc::unbounded_channel();
    let (addr, _server) = spawn_app(move |app| {
        let close_tx = close_tx.clone();
        app.websocket("/ws", move |mut socket| {
            let close_tx = close_tx.clone();
            async move {
                socket.close_with(1000, "finalizado").await.unwrap();
                close_tx.send(socket.closed().await).unwrap();
            }
        });
    })
    .await;
    let (mut client, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    let frame = receive_close_frame(&mut client).await;
    assert_eq!(u16::from(frame.code), 1000);
    assert_eq!(frame.reason, "finalizado");
    client.flush().await.unwrap();

    let close = tokio::time::timeout(IO_TIMEOUT, close_rx.recv())
        .await
        .expect("handler should observe closure")
        .expect("handler should publish close info");
    assert_eq!(close.code, 1000);
    assert_eq!(close.reason, "finalizado");
    assert_eq!(close.initiator, rustrest::WebSocketCloseInitiator::Local);
    assert!(close.clean);
}

#[tokio::test]
async fn websocket_close_rejects_control_sends_after_closing_starts() {
    let (late_send_tx, mut late_send_rx) = tokio::sync::mpsc::unbounded_channel();
    let (addr, _server) = spawn_app(move |app| {
        let late_send_tx = late_send_tx.clone();
        app.websocket("/ws", move |socket| {
            let late_send_tx = late_send_tx.clone();
            async move {
                let (_receiver, sender) = socket.split();
                sender.close().await.unwrap();
                tokio::time::sleep(Duration::from_millis(25)).await;
                late_send_tx.send(sender.ping(Vec::new()).await).unwrap();
            }
        });
    })
    .await;
    let (mut client, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    let frame = receive_close_frame(&mut client).await;
    assert_eq!(u16::from(frame.code), 1000);
    let late_send = tokio::time::timeout(IO_TIMEOUT, late_send_rx.recv())
        .await
        .expect("late send should finish before the deadline")
        .expect("handler should publish the late send result");
    assert!(matches!(late_send, Err(WsError::Closed)));
}

#[tokio::test]
async fn websocket_slow_consumer_closes_with_1013() {
    let (addr, _server) = spawn_app(|app| {
        app.websocket_with(
            "/ws",
            WebSocketConfig::new()
                .disable_ping()
                .inbound_capacity(1)
                .send_timeout(Duration::from_millis(30))
                .close_timeout(Duration::from_millis(50)),
            |socket| async move {
                std::future::pending::<()>().await;
                drop(socket);
            },
        );
    })
    .await;
    let (mut client, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    client.send(Message::Text("first".into())).await.unwrap();
    client.send(Message::Text("second".into())).await.unwrap();

    let frame = receive_close_frame(&mut client).await;
    assert_eq!(u16::from(frame.code), 1013);
}

#[tokio::test]
async fn websocket_close_waits_for_peer_after_handler_returns() {
    let (requested_tx, mut requested_rx) = tokio::sync::mpsc::unbounded_channel();
    let (addr, runtime, _server) = spawn_app_with_runtime(move |app| {
        let requested_tx = requested_tx.clone();
        app.websocket_with(
            "/ws",
            WebSocketConfig::new().close_timeout(Duration::from_millis(200)),
            move |socket| {
                let requested_tx = requested_tx.clone();
                async move {
                    let (_receiver, sender) = socket.split();
                    sender.close().await.unwrap();
                    requested_tx.send(()).unwrap();
                }
            },
        );
    })
    .await;
    let (mut client, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    tokio::time::timeout(IO_TIMEOUT, requested_rx.recv())
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(25)).await;

    assert_eq!(runtime.stats().active_connections, 1);
    let frame = receive_close_frame(&mut client).await;
    assert_eq!(u16::from(frame.code), 1000);
    client.flush().await.unwrap();
    wait_for_connections(&runtime, 0, 1).await;
}

#[tokio::test]
async fn websocket_observer_records_handler_panic_and_isolates_connection() {
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let handler_attempts = attempts.clone();
    let observations = Arc::new(Mutex::new(Vec::new()));
    let observer_events = observations.clone();
    let (addr, _server) = spawn_app(move |app| {
        app.websocket_observer(Arc::new(RecordingObserver(observer_events)));
        let handler_attempts = handler_attempts.clone();
        app.websocket("/ws", move |mut socket| {
            let attempt = handler_attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                if attempt == 0 {
                    panic!("panic aislado del handler");
                }
                while let Some(message) = socket.recv().await.unwrap() {
                    if message.is_text() {
                        socket.send(message).await.unwrap();
                        break;
                    }
                }
            }
        });
    })
    .await;

    let (mut first, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    let close = receive_close_frame(&mut first).await;
    assert_eq!(u16::from(close.code), 1011);
    first.flush().await.unwrap();

    let (mut second, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    second.send(Message::Text("healthy".into())).await.unwrap();
    let echo = tokio::time::timeout(IO_TIMEOUT, second.next())
        .await
        .expect("second connection should remain healthy")
        .unwrap()
        .unwrap();
    assert_eq!(echo.into_text().unwrap(), "healthy");
    assert!(
        observations
            .lock()
            .unwrap()
            .iter()
            .any(|event| event.starts_with("handler_failed:"))
    );
}

#[tokio::test]
async fn websocket_handler_completion_closes_with_1000() {
    let (addr, _server) = spawn_app(|app| {
        app.websocket("/ws", |_socket| async move {});
    })
    .await;
    let (mut client, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    let close = receive_close_frame(&mut client).await;
    assert_eq!(u16::from(close.code), 1000);
}

#[tokio::test]
async fn websocket_handler_background_sender_outlives_handler() {
    let (addr, _server) = spawn_app(|app| {
        app.websocket("/ws", |socket| async move {
            let (_receiver, sender) = socket.split();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(25)).await;
                sender.send_text("background-after-handler").await.unwrap();
            });
        });
    })
    .await;
    let (mut client, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    let message = tokio::time::timeout(IO_TIMEOUT, client.next())
        .await
        .expect("background sender should remain connected")
        .unwrap()
        .unwrap();
    assert_eq!(message.into_text().unwrap(), "background-after-handler");
    let close = receive_close_frame(&mut client).await;
    assert_eq!(u16::from(close.code), 1000);
}

#[tokio::test]
async fn websocket_message_rate_overflow_closes_with_1008() {
    let (received_tx, mut received_rx) = tokio::sync::mpsc::unbounded_channel();
    let (addr, _server) = spawn_app(move |app| {
        let received_tx = received_tx.clone();
        app.websocket_with(
            "/ws",
            WebSocketConfig::new().message_rate_limit(2, Duration::from_secs(1)),
            move |mut socket| {
                let received_tx = received_tx.clone();
                async move {
                    while let Some(message) = socket.recv().await.unwrap() {
                        if message.is_text() {
                            received_tx
                                .send(message.into_text().unwrap().to_string())
                                .unwrap();
                        }
                    }
                }
            },
        );
    })
    .await;
    let (mut client, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    client.send(Message::Text("one".into())).await.unwrap();
    client.send(Message::Text("two".into())).await.unwrap();
    client.send(Message::Text("three".into())).await.unwrap();

    let close = receive_close_frame(&mut client).await;
    assert_eq!(u16::from(close.code), 1008);
    assert_eq!(received_rx.recv().await.unwrap(), "one");
    assert_eq!(received_rx.recv().await.unwrap(), "two");
    assert!(received_rx.try_recv().is_err());
}

#[tokio::test]
async fn websocket_message_rate_resets_after_window_rollover() {
    let (received_tx, mut received_rx) = tokio::sync::mpsc::unbounded_channel();
    let (addr, _server) = spawn_app(move |app| {
        let received_tx = received_tx.clone();
        app.websocket_with(
            "/ws",
            WebSocketConfig::new().message_rate_limit(1, Duration::from_millis(40)),
            move |mut socket| {
                let received_tx = received_tx.clone();
                async move {
                    while let Some(message) = socket.recv().await.unwrap() {
                        if message.is_text() {
                            received_tx.send(()).unwrap();
                        }
                    }
                }
            },
        );
    })
    .await;
    let (mut client, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    client.send(Message::Text("first".into())).await.unwrap();
    tokio::time::timeout(IO_TIMEOUT, received_rx.recv())
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    client.send(Message::Text("second".into())).await.unwrap();
    tokio::time::timeout(IO_TIMEOUT, received_rx.recv())
        .await
        .expect("message after window rollover should be delivered")
        .unwrap();
}

#[tokio::test]
async fn websocket_shutdown_sends_1001_and_drains_cooperative_client() {
    let mut app = App::new();
    app.websocket_defaults(WebSocketConfig::new().close_timeout(Duration::from_millis(200)));
    app.websocket("/ws", |_socket| async move {
        std::future::pending::<()>().await;
    });
    let runtime = app.websocket_runtime();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(app.serve_with_shutdown(listener, async move {
        let _ = shutdown_rx.await;
    }));

    let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    wait_for_connections(&runtime, 1, 0).await;
    shutdown_tx.send(()).unwrap();

    let close = tokio::time::timeout(IO_TIMEOUT, client.next())
        .await
        .expect("shutdown close frame should arrive before the deadline")
        .expect("server should send a websocket frame")
        .expect("shutdown frame should be valid");
    let Message::Close(Some(frame)) = close else {
        panic!("expected a close frame, got {close:?}");
    };
    assert_eq!(frame.code, CloseCode::Away);
    assert_eq!(frame.reason, "apagado del servidor");
    client.flush().await.unwrap();

    let result = tokio::time::timeout(IO_TIMEOUT, server)
        .await
        .expect("server shutdown should finish before the deadline")
        .expect("server task should not panic");
    assert!(result.is_ok());
    assert_eq!(runtime.stats().active_connections, 0);
}

#[tokio::test]
async fn websocket_shutdown_waits_for_uncooperative_client_close_timeout() {
    let close_timeout = Duration::from_millis(150);
    let mut app = App::new();
    app.websocket_defaults(WebSocketConfig::new().close_timeout(close_timeout));
    app.websocket("/ws", |_socket| async move {
        std::future::pending::<()>().await;
    });
    let runtime = app.websocket_runtime();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(app.serve_with_shutdown(listener, async move {
        let _ = shutdown_rx.await;
    }));
    let _client = raw_websocket(addr).await;
    wait_for_connections(&runtime, 1, 0).await;

    let started = tokio::time::Instant::now();
    shutdown_tx.send(()).unwrap();
    let result = tokio::time::timeout(IO_TIMEOUT, server)
        .await
        .expect("forced websocket shutdown should finish before the deadline")
        .expect("server task should not panic");

    assert!(result.is_ok());
    assert!(
        started.elapsed() >= close_timeout,
        "server returned before the websocket close grace period elapsed"
    );
    assert_eq!(runtime.stats().active_connections, 0);
}

#[tokio::test]
async fn websocket_runtime_close_targets_one_connection_and_reports_missing_id() {
    let (addr, runtime, _server) = spawn_app_with_runtime(|app| {
        app.websocket("/ws", |_socket| async move {
            std::future::pending::<()>().await;
        });
    })
    .await;
    let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    wait_for_connections(&runtime, 1, 0).await;
    let id = runtime.connections()[0].id;

    let close_runtime = runtime.clone();
    let close = tokio::spawn(async move { close_runtime.close(id, 1001, "mantenimiento").await });
    let message = tokio::time::timeout(IO_TIMEOUT, client.next())
        .await
        .expect("administrative close should arrive before the deadline")
        .expect("server should send a websocket frame")
        .expect("administrative close should be valid");
    let Message::Close(Some(frame)) = message else {
        panic!("expected a close frame, got {message:?}");
    };
    assert_eq!(frame.code, CloseCode::Away);
    assert_eq!(frame.reason, "mantenimiento");
    client.flush().await.unwrap();
    close.await.unwrap().unwrap();

    assert!(matches!(
        runtime.close(id, 1001, "otra vez").await,
        Err(WsError::ConnectionNotFound(missing)) if missing == id
    ));
}

#[tokio::test]
async fn websocket_runtime_shutdown_drains_and_rejects_future_upgrades() {
    let (addr, runtime, _server) = spawn_app_with_runtime(|app| {
        app.websocket_defaults(WebSocketConfig::new().close_timeout(Duration::from_millis(200)));
        app.websocket("/ws", |_socket| async move {
            std::future::pending::<()>().await;
        });
    })
    .await;
    let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    wait_for_connections(&runtime, 1, 0).await;

    let shutdown_runtime = runtime.clone();
    let shutdown = tokio::spawn(async move { shutdown_runtime.shutdown().await });
    let message = tokio::time::timeout(IO_TIMEOUT, client.next())
        .await
        .expect("runtime shutdown close should arrive before the deadline")
        .expect("server should send a websocket frame")
        .expect("runtime shutdown close should be valid");
    let Message::Close(Some(frame)) = message else {
        panic!("expected a close frame, got {message:?}");
    };
    assert_eq!(frame.code, CloseCode::Away);
    assert_eq!(frame.reason, "apagado del servidor");
    client.flush().await.unwrap();
    shutdown.await.unwrap().unwrap();

    let response = raw_handshake(
        addr,
        &[
            ("Sec-WebSocket-Version", "13"),
            ("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ=="),
        ],
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 503"), "{response}");
}
