use std::net::SocketAddr;
use std::time::Duration;

use rustrest::{App, WebSocketConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

const IO_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_RESPONSE_BYTES: usize = 16 * 1024;

struct ServerGuard(JoinHandle<()>);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn spawn_app(configure: impl FnOnce(&mut App)) -> (SocketAddr, ServerGuard) {
    let mut app = App::new();
    configure(&mut app);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        app.serve(listener).await.unwrap();
    });

    (addr, ServerGuard(server))
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
