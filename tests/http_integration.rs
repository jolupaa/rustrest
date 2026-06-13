use rustrest::app::{App, Request, Response};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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
