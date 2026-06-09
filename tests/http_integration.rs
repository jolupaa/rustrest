use futures_util::{SinkExt, StreamExt};
use rustrest::app::{App, Request, Response, WebSocketEvent};
use serde_json::json;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn oversized_body_returns_413() {
    let mut app = App::new();
    app.max_body_size(16);
    app.post("/upload", |_req: Request| Response::send("ok"));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve(listener));

    let body = "x".repeat(100);
    let request = format!(
        "POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    server.abort();

    assert!(
        response.starts_with("HTTP/1.1 413"),
        "expected 413, got: {response}"
    );
}

#[tokio::test]
async fn slow_handler_times_out_with_408() {
    let mut app = App::new();
    app.request_timeout(Duration::from_millis(50));
    app.get("/slow", |_req: Request| async {
        tokio::time::sleep(Duration::from_secs(5)).await;
        Response::send("too late")
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve(listener));

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"GET /slow HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    server.abort();

    assert!(
        response.starts_with("HTTP/1.1 408"),
        "expected 408, got: {response}"
    );
}

#[tokio::test]
async fn serve_with_shutdown_returns_after_signal() {
    let mut app = App::new();
    app.get("/ping", |_req: Request| Response::send("pong"));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(app.serve_with_shutdown(listener, async move {
        let _ = rx.await;
    }));

    // A request before shutdown is served normally.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"GET /ping HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    assert!(response.contains("pong"));

    // After signaling shutdown, the server stops accepting and returns Ok.
    tx.send(()).unwrap();
    let joined = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server should stop within timeout")
        .expect("server task should not panic");
    assert!(joined.is_ok());
}

#[tokio::test]
async fn app_serves_real_http_requests() {
    let mut app = App::new();
    app.get("/hello", |_req: Request| Response::send("hello http"));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve(listener));

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"GET /hello HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    server.abort();

    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("content-type: text/plain; charset=utf-8"));
    assert!(response.ends_with("hello http"));
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
