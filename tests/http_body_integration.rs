use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::{StreamExt, stream};
use hyper::StatusCode;
use hyper::body::Bytes;
use rustrest::{
    App, BodyStream, BoxError, HttpError, Next, Request, RequestBody, Response, TestClient,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

async fn spawn_app(app: App) -> (SocketAddr, oneshot::Sender<()>, JoinHandle<io::Result<()>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(app.serve_with_shutdown(listener, async move {
        let _ = shutdown_rx.await;
    }));

    (addr, shutdown_tx, server)
}

async fn read_headers(stream: &mut TcpStream) -> io::Result<String> {
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut response = Vec::new();
        let mut chunk = [0_u8; 256];

        loop {
            let read = stream.read(&mut chunk).await?;
            if read == 0 {
                break;
            }
            response.extend_from_slice(&chunk[..read]);
            if response.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }

        String::from_utf8(response)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    })
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "timed out waiting for response headers",
        )
    })?
}

async fn shutdown_server(shutdown: oneshot::Sender<()>, mut server: JoinHandle<io::Result<()>>) {
    let _ = shutdown.send(());
    match tokio::time::timeout(Duration::from_secs(2), &mut server).await {
        Ok(result) => result
            .expect("server task should not panic")
            .expect("server should shut down cleanly"),
        Err(_) => {
            server.abort();
            match server.await {
                Ok(result) => result.expect("server should not fail after abort"),
                Err(error) if error.is_cancelled() => {}
                Err(error) => panic!("server task failed after abort: {error}"),
            }
        }
    }
}

async fn read_response(stream: &mut TcpStream) -> io::Result<String> {
    tokio::time::timeout(Duration::from_secs(1), async {
        let mut response = String::new();
        stream.read_to_string(&mut response).await?;
        Ok(response)
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "timed out waiting for response"))?
}

#[tokio::test]
async fn middleware_can_reject_before_request_body_is_sent() {
    let mut app = App::new();
    app.layer(|req: Request, next: Next| async move {
        if req.header("authorization").is_none() {
            return Response::from_error(HttpError::unauthorized("Falta autenticacion"));
        }
        next(req).await
    });
    app.post("/upload", |_req: Request| Response::send("ok"));

    let (addr, shutdown, server) = spawn_app(app).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1048576\r\n\r\n")
        .await
        .unwrap();

    let response = read_headers(&mut stream).await;
    drop(stream);
    shutdown_server(shutdown, server).await;

    let response = response.expect("middleware should respond after receiving only headers");
    assert!(
        response.starts_with("HTTP/1.1 401"),
        "expected 401, got: {response}"
    );
}

#[tokio::test]
async fn incoming_request_body_can_be_collected() {
    let mut app = App::new();
    app.post("/echo", |mut req: Request| async move {
        let body = req.bytes().await?;
        Ok::<_, HttpError>(Response::send(&body.len().to_string()))
    });

    let (addr, shutdown, server) = spawn_app(app).await;
    let mut socket = TcpStream::connect(addr).await.unwrap();
    socket
        .write_all(
            b"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 11\r\nConnection: close\r\n\r\nhello ",
        )
        .await
        .unwrap();
    socket.write_all(b"world").await.unwrap();

    let response = read_response(&mut socket).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.ends_with("11"), "{response}");

    let _ = shutdown.send(());
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn streamed_body_enforces_the_limit_incrementally() {
    let exact_chunks: Vec<Result<Bytes, io::Error>> =
        vec![Ok(Bytes::from_static(b"ab")), Ok(Bytes::from_static(b"cd"))];
    let mut exact = RequestBody::from_stream(stream::iter(exact_chunks), 4);
    assert_eq!(exact.collect_default().await.unwrap(), "abcd");

    let oversized_chunks: Vec<Result<Bytes, io::Error>> = vec![
        Ok(Bytes::from_static(b"ab")),
        Ok(Bytes::from_static(b"cde")),
    ];
    let mut oversized = RequestBody::from_stream(stream::iter(oversized_chunks), 4);
    let error = oversized.collect_default().await.unwrap_err();
    assert_eq!(error.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(error.code(), "payload_too_large");

    let second_error = oversized.collect_default().await.unwrap_err();
    assert_eq!(second_error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(second_error.code(), "body_already_consumed");
}

#[tokio::test]
async fn body_stream_read_error_is_safe_and_retains_its_source() {
    let chunks = vec![
        Ok(Bytes::from_static(b"partial")),
        Err(io::Error::other("socket contained private diagnostics")),
    ];
    let mut body = RequestBody::from_stream(stream::iter(chunks), 1024);

    let error = body.collect_default().await.unwrap_err();

    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error.code(), "body_read");
    assert!(!error.public_message().contains("private diagnostics"));
    assert_eq!(
        error.source().unwrap().to_string(),
        "socket contained private diagnostics"
    );

    let second_error = body.collect_default().await.unwrap_err();
    assert_eq!(second_error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(second_error.code(), "body_already_consumed");
}

#[tokio::test]
async fn taken_body_stream_preserves_the_raw_error() {
    let chunks = vec![Err::<Bytes, _>(io::Error::other(
        "socket contained raw diagnostics",
    ))];
    let mut body = RequestBody::from_stream(stream::iter(chunks), 1024);
    let mut stream: BodyStream = body.take_stream().unwrap();

    let error: BoxError = stream.next().await.unwrap().unwrap_err();

    assert_eq!(error.to_string(), "socket contained raw diagnostics");
    assert!(error.downcast_ref::<io::Error>().is_some());
}

#[tokio::test]
async fn body_is_already_consumed_after_taking_its_stream() {
    let mut request = Request::builder().body("hello").build();
    let mut stream: BodyStream = request.take_body_stream().unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap(), "hello");
    drop(stream);

    let error = request.bytes().await.unwrap_err();
    assert_eq!(error.code(), "body_already_consumed");
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn buffered_builder_body_can_be_collected_repeatedly() {
    let mut request = Request::builder().body("hola").build();

    assert_eq!(request.body().size_hint().exact(), Some(4));
    assert_eq!(request.body_mut().collect_default().await.unwrap(), "hola");
    assert_eq!(request.bytes().await.unwrap(), "hola");
    assert_eq!(request.bytes().await.unwrap(), "hola");
}

#[tokio::test]
async fn request_text_is_strict_and_text_lossy_is_explicit() {
    let mut request = Request::builder().body(vec![0xff, b'h', b'i']).build();

    let error = request.text().await.unwrap_err();
    assert_eq!(error.code(), "invalid_utf8");
    assert!(error.source().is_some());
    assert_eq!(request.text_lossy().await.unwrap(), "\u{fffd}hi");
}

#[tokio::test]
async fn invalid_json_keeps_its_source_out_of_problem_details() {
    let mut request = Request::builder().body("{private invalid json").build();

    let error = request.json::<serde_json::Value>().await.unwrap_err();
    assert_eq!(error.code(), "invalid_json");
    assert!(error.source().is_some());
    let response = Response::from_error(error);
    let problem = response.body_text();
    assert!(problem.contains(r#""code":"invalid_json""#));
    assert!(!problem.contains("expected ident"));
    assert!(!problem.contains("line 1 column"));
}

#[tokio::test]
async fn test_client_middleware_can_reject_oversized_body_without_collecting_it() {
    let dispatched = Arc::new(AtomicBool::new(false));
    let handler_dispatched = Arc::clone(&dispatched);
    let mut app = App::new();
    app.max_body_size(4);
    app.layer(|req: Request, next: Next| async move {
        if req.header("authorization").is_none() {
            return Response::from_error(HttpError::unauthorized("Falta autenticacion"));
        }
        next(req).await
    });
    app.post("/upload", move |_req: Request| {
        handler_dispatched.store(true, Ordering::SeqCst);
        Response::send("ok")
    });
    let client = TestClient::new(app);

    let response = client.post("/upload").body("12345").send().await;

    assert_eq!(response.status, 401);
    assert!(!dispatched.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_client_handler_can_ignore_oversized_body() {
    let mut app = App::new();
    app.max_body_size(4);
    app.post("/upload", |_req: Request| Response::send("ok"));
    let client = TestClient::new(app);

    let response = client.post("/upload").body("12345").send().await;

    assert_eq!(response.status, 200);
    assert_eq!(response.body_text(), "ok");
}

#[tokio::test]
async fn test_client_enforces_body_limit_when_handler_collects_body() {
    let body_reads = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    app.max_body_size(4);
    let bytes_reads = Arc::clone(&body_reads);
    app.post("/bytes", move |mut req: Request| {
        let body_reads = Arc::clone(&bytes_reads);
        async move {
            body_reads.fetch_add(1, Ordering::SeqCst);
            req.bytes().await?;
            Ok::<_, HttpError>(Response::send("ok"))
        }
    });
    let text_reads = Arc::clone(&body_reads);
    app.post("/text", move |mut req: Request| {
        let body_reads = Arc::clone(&text_reads);
        async move {
            body_reads.fetch_add(1, Ordering::SeqCst);
            req.text().await?;
            Ok::<_, HttpError>(Response::send("ok"))
        }
    });
    let json_reads = Arc::clone(&body_reads);
    app.post("/json", move |mut req: Request| {
        let body_reads = Arc::clone(&json_reads);
        async move {
            body_reads.fetch_add(1, Ordering::SeqCst);
            req.json::<serde_json::Value>().await?;
            Ok::<_, HttpError>(Response::send("ok"))
        }
    });
    let client = TestClient::new(app);

    for path in ["/bytes", "/text", "/json"] {
        let response = client.post(path).body("12345").send().await;

        assert_eq!(response.status, 413, "unexpected status for {path}");
        assert!(
            response
                .body_text()
                .contains(r#""code":"payload_too_large""#),
            "unexpected response for {path}: {}",
            response.body_text()
        );
    }
    assert_eq!(body_reads.load(Ordering::SeqCst), 3);
}
