use std::net::SocketAddr;

use rustrest::App;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

async fn spawn_app(configure: impl FnOnce(&mut App)) -> (SocketAddr, JoinHandle<()>) {
    let mut app = App::new();
    configure(&mut app);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        app.serve(listener).await.unwrap();
    });

    (addr, server)
}

async fn raw_handshake(addr: SocketAddr, headers: &[(&str, &str)]) -> String {
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

    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response
}

#[tokio::test]
async fn websocket_rejects_invalid_version_with_426() {
    let (addr, server) = spawn_app(|app| {
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
    server.abort();
}

#[tokio::test]
async fn websocket_rejects_key_that_is_not_sixteen_decoded_bytes() {
    let (addr, server) = spawn_app(|app| {
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
    server.abort();
}
