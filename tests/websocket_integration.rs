use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::time::Duration;

use rustrest::{
    App, BackpressurePolicy, WebSocketCapacityError, WebSocketConfig, WebSocketEvent,
    WebSocketRuntimeHandle, WsError,
};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

const IO_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_RESPONSE_BYTES: usize = 16 * 1024;

struct ServerGuard(JoinHandle<()>);

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
async fn websocket_backpressure_disconnect_closes_slow_consumer_with_1013() {
    let (saturated_tx, mut saturated_rx) = tokio::sync::mpsc::unbounded_channel();
    let (addr, runtime, _server) = spawn_app_with_runtime(move |app| {
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
async fn websocket_heartbeat_missing_pong_closes_with_1001() {
    let (addr, runtime, _server) = spawn_app_with_runtime(|app| {
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
async fn websocket_handler_panic_closes_only_that_connection_with_1011() {
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let handler_attempts = attempts.clone();
    let (addr, _server) = spawn_app(move |app| {
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
