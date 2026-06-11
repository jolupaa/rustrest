use futures_util::{SinkExt, StreamExt};
use rustrest::app::{App, Request, Response, WebSocketEvent};
use serde_json::json;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn duplicate_request_headers_are_all_preserved() {
    let mut app = App::new();
    app.get("/h", |req: Request| {
        Response::send(&req.headers_all("x-tag").join(","))
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve(listener));

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            b"GET /h HTTP/1.1\r\nHost: localhost\r\nX-Tag: a\r\nX-Tag: b\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    server.abort();

    assert!(
        response.ends_with("a,b"),
        "expected both header values, got: {response}"
    );
}

#[tokio::test]
async fn request_exposes_client_peer_address() {
    let mut app = App::new();
    app.get("/whoami", |req: Request| match req.remote_addr() {
        Some(addr) => Response::send(&addr.ip().to_string()),
        None => Response::send("none"),
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve(listener));

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"GET /whoami HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    server.abort();

    assert!(
        response.ends_with("127.0.0.1"),
        "expected client ip in body, got: {response}"
    );
}

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

#[tokio::test]
async fn websocket_config_negotiates_protocol_pings_and_limits_message_size() {
    use rustrest::app::WebSocketConfig;
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
