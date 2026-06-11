use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::time::Duration;

use rustrest::{App, WebSocketConfig, WebSocketEvent, WebSocketRuntimeHandle};
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

    // A message over max_message_size is never echoed: the server side
    // errors out and the connection ends (close frame or abrupt error).
    let big = "x".repeat(8 * 1024);
    client.send(Message::Text(big.into())).await.unwrap();
    loop {
        match tokio::time::timeout(Duration::from_secs(2), client.next())
            .await
            .expect("connection should settle in time")
        {
            Some(Ok(message)) if message.is_ping() || message.is_pong() => continue,
            Some(Ok(message)) if message.is_close() => break,
            Some(Ok(message)) => {
                panic!("oversized message should not produce a reply, got {message:?}")
            }
            Some(Err(_)) | None => break,
        }
    }
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

    let (_client, _response) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    wait_for_connections(&runtime, 1, 0).await;

    release.add_permits(1);
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
