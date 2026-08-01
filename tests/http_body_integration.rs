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

fn assert_problem_code(response: &Response, status: u16, code: &str) {
    assert_eq!(response.status, status);
    assert!(
        response
            .body_text()
            .contains(&format!(r#""code":"{code}""#)),
        "unexpected response: {}",
        response.body_text()
    );
}

#[tokio::test]
async fn route_body_limit_can_reduce_the_server_default() {
    let mut app = App::new();
    app.max_body_size(1024 * 1024);
    let _ = app
        .post("/json", |mut req: Request| async move {
            req.bytes()
                .await
                .map(|body| Response::send(&body.len().to_string()))
        })
        .unwrap()
        .body_limit(1024);
    let _ = app
        .post("/upload", |mut req: Request| async move {
            req.bytes()
                .await
                .map(|body| Response::send(&body.len().to_string()))
        })
        .unwrap()
        .body_limit(1024 * 1024);

    let client = TestClient::new(app);

    let rejected = client.post("/json").body(vec![0; 2048]).send().await;
    assert_problem_code(&rejected, 413, "payload_too_large");

    let accepted = client.post("/upload").body(vec![0; 2048]).send().await;
    assert_eq!(accepted.status, 200);
    assert_eq!(accepted.body_text(), "2048");
}

#[tokio::test]
async fn server_body_limit_is_a_hard_ceiling_for_routes() {
    let mut app = App::new();
    app.max_body_size(1024);
    let _ = app
        .post("/upload", |mut req: Request| async move {
            req.bytes()
                .await
                .map(|body| Response::send(&body.len().to_string()))
        })
        .unwrap()
        .body_limit(1024 * 1024);

    let response = TestClient::new(app)
        .post("/upload")
        .body(vec![0; 2048])
        .send()
        .await;

    assert_problem_code(&response, 413, "payload_too_large");
}

#[tokio::test]
async fn route_timeout_wraps_only_the_selected_route() {
    let mut app = App::new();
    let _ = app
        .get("/slow", |_req: Request| async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Response::send("late")
        })
        .unwrap()
        .timeout(Duration::from_millis(20));
    let _ = app
        .get("/fast", |_req: Request| Response::send("ok"))
        .unwrap();

    let client = TestClient::new(app);

    let timed_out = client.get("/slow").send().await;
    assert_problem_code(&timed_out, 408, "request_timeout");

    let fast = client.get("/fast").send().await;
    assert_eq!(fast.status, 200);
    assert_eq!(fast.body_text(), "ok");
}

#[tokio::test]
async fn known_oversized_content_length_is_visible_to_global_middleware_before_body_poll() {
    let middleware_ran = Arc::new(AtomicBool::new(false));
    let handler_ran = Arc::new(AtomicBool::new(false));
    let mut app = App::new();
    app.max_body_size(1024 * 1024);
    let middleware_flag = Arc::clone(&middleware_ran);
    app.layer(move |req: Request, next: Next| {
        middleware_flag.store(true, Ordering::SeqCst);
        async move { next(req).await }
    });
    let handler_flag = Arc::clone(&handler_ran);
    let _ = app
        .post("/upload", move |_req: Request| {
            handler_flag.store(true, Ordering::SeqCst);
            Response::send("ok")
        })
        .unwrap()
        .body_limit(4);

    let (addr, shutdown, server) = spawn_app(app).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            b"POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();

    let response = read_response(&mut stream)
        .await
        .expect("server should reject from headers without waiting for the payload");
    shutdown_server(shutdown, server).await;

    assert!(response.starts_with("HTTP/1.1 413"), "{response}");
    assert!(
        response.contains(r#""code":"payload_too_large""#),
        "{response}"
    );
    assert!(middleware_ran.load(Ordering::SeqCst));
    assert!(!handler_ran.load(Ordering::SeqCst));
}

#[tokio::test]
async fn invalid_content_length_is_visible_to_global_middleware_before_handler() {
    let middleware_ran = Arc::new(AtomicBool::new(false));
    let handler_ran = Arc::new(AtomicBool::new(false));
    let mut app = App::new();
    let middleware_flag = Arc::clone(&middleware_ran);
    app.layer(move |req: Request, next: Next| {
        middleware_flag.store(true, Ordering::SeqCst);
        async move { next(req).await }
    });
    let handler_flag = Arc::clone(&handler_ran);
    let _ = app.post("/upload", move |_req: Request| {
        handler_flag.store(true, Ordering::SeqCst);
        Response::send("ok")
    });
    let client = TestClient::new(app);

    let response = client
        .post("/upload")
        .header("content-length", "not-a-number")
        .body("hello")
        .send()
        .await;

    assert_problem_code(&response, 400, "invalid_content_length");
    assert!(middleware_ran.load(Ordering::SeqCst));
    assert!(!handler_ran.load(Ordering::SeqCst));
}

#[tokio::test]
async fn malformed_path_is_visible_to_global_middleware() {
    let middleware_runs = Arc::new(AtomicUsize::new(0));
    let middleware_counter = Arc::clone(&middleware_runs);
    let mut app = App::new();
    app.layer(move |req: Request, next: Next| {
        middleware_counter.fetch_add(1, Ordering::SeqCst);
        async move { next(req).await }
    });
    let _ = app.get("/users/:id", |_req: Request| Response::send("ok"));

    let response = TestClient::new(app).get("/users/%ZZ").send().await;

    assert_problem_code(&response, 400, "invalid_path_encoding");
    assert_eq!(middleware_runs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn content_length_errors_use_the_global_error_handler() {
    let mut app = App::new();
    app.error_handler(|error: HttpError| {
        Response::send(error.code())
            .status(error.status().as_u16())
            .header("x-error-code", error.code())
    });
    let _ = app.post("/upload", |_req: Request| Response::send("ok"));
    let client = TestClient::new(app);

    let response = client
        .post("/upload")
        .header("content-length", "invalid")
        .send()
        .await;

    assert_eq!(response.status, 400);
    assert_eq!(response.body_text(), "invalid_content_length");
    assert_eq!(
        response
            .headers
            .get("x-error-code")
            .and_then(|value| value.to_str().ok()),
        Some("invalid_content_length")
    );
}

#[tokio::test]
async fn conflicting_content_length_is_visible_to_global_middleware_before_handler() {
    for values in [["5", "6"], ["5, 6", ""]] {
        let middleware_runs = Arc::new(AtomicUsize::new(0));
        let handler_runs = Arc::new(AtomicUsize::new(0));
        let mut app = App::new();
        let middleware_counter = Arc::clone(&middleware_runs);
        app.layer(move |req: Request, next: Next| {
            middleware_counter.fetch_add(1, Ordering::SeqCst);
            async move { next(req).await }
        });
        let handler_counter = Arc::clone(&handler_runs);
        let _ = app.post("/upload", move |_req: Request| {
            handler_counter.fetch_add(1, Ordering::SeqCst);
            Response::send("ok")
        });
        let client = TestClient::new(app);
        let mut request = client.post("/upload").header("content-length", values[0]);
        if !values[1].is_empty() {
            request = request.header("content-length", values[1]);
        }

        let response = request.body("hello").send().await;

        assert_problem_code(&response, 400, "invalid_content_length");
        assert_eq!(middleware_runs.load(Ordering::SeqCst), 1);
        assert_eq!(handler_runs.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn identical_content_length_values_are_accepted() {
    let mut app = App::new();
    let _ = app.post("/duplicates", |mut req: Request| async move {
        req.bytes()
            .await
            .map(|body| Response::send(&body.len().to_string()))
    });
    let _ = app.post("/list", |mut req: Request| async move {
        req.bytes()
            .await
            .map(|body| Response::send(&body.len().to_string()))
    });
    let client = TestClient::new(app);

    let duplicate_fields = client
        .post("/duplicates")
        .header("content-length", "5")
        .header("content-length", "5")
        .body("hello")
        .send()
        .await;
    assert_eq!(duplicate_fields.status, 200);
    assert_eq!(duplicate_fields.body_text(), "5");

    let identical_list = client
        .post("/list")
        .header("content-length", "5, 5")
        .body("hello")
        .send()
        .await;
    assert_eq!(identical_list.status, 200);
    assert_eq!(identical_list.body_text(), "5");
}

async fn assert_http1_content_length_parse_error(content_length_headers: &str) {
    let middleware_runs = Arc::new(AtomicUsize::new(0));
    let handler_runs = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    app.error_handler(|error: HttpError| {
        Response::send(error.code())
            .status(error.status().as_u16())
            .header("x-rustrest-error", "true")
    });
    let middleware_counter = Arc::clone(&middleware_runs);
    app.layer(move |req: Request, next: Next| {
        middleware_counter.fetch_add(1, Ordering::SeqCst);
        async move { next(req).await }
    });
    let handler_counter = Arc::clone(&handler_runs);
    let _ = app.post("/upload", move |_req: Request| {
        handler_counter.fetch_add(1, Ordering::SeqCst);
        Response::send("ok")
    });

    let (addr, shutdown, server) = spawn_app(app).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "POST /upload HTTP/1.1\r\nHost: localhost\r\n{content_length_headers}Connection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await.unwrap();

    let response = read_response(&mut stream).await.unwrap();
    shutdown_server(shutdown, server).await;

    let lower_response = response.to_ascii_lowercase();
    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    // Hyper rejects these malformed HTTP/1 messages before RustRest's service_fn.
    // Transport-generated 400 responses cannot use the framework error handler or problem body.
    assert!(!lower_response.contains("x-rustrest-error"), "{response}");
    assert!(
        !lower_response.contains("application/problem+json"),
        "{response}"
    );
    assert!(!response.contains("invalid_content_length"), "{response}");
    assert_eq!(middleware_runs.load(Ordering::SeqCst), 0);
    assert_eq!(handler_runs.load(Ordering::SeqCst), 0);
}

async fn assert_http1_ambiguous_framing_rejected_at_transport(
    leading_lines: &str,
    framing_headers: &str,
) {
    let middleware_runs = Arc::new(AtomicUsize::new(0));
    let handler_runs = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    app.error_handler(|error: HttpError| {
        Response::send(error.code())
            .status(error.status().as_u16())
            .header("x-rustrest-error", "true")
    });
    let middleware_counter = Arc::clone(&middleware_runs);
    app.layer(move |req: Request, next: Next| {
        middleware_counter.fetch_add(1, Ordering::SeqCst);
        async move { next(req).await }
    });
    let handler_counter = Arc::clone(&handler_runs);
    let _ = app.post("/upload", move |_req: Request| {
        handler_counter.fetch_add(1, Ordering::SeqCst);
        Response::send("ok")
    });

    let (addr, shutdown, server) = spawn_app(app).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "{leading_lines}POST /upload HTTP/1.1\r\nHost: localhost\r\n{framing_headers}Connection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await.unwrap();

    let response = read_response(&mut stream).await.unwrap();
    shutdown_server(shutdown, server).await;

    let lower_response = response.to_ascii_lowercase();
    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    assert!(
        !lower_response.contains("x-rustrest-error: true"),
        "{response}"
    );
    assert!(
        lower_response.contains("application/problem+json"),
        "{response}"
    );
    assert!(
        response.contains(r#""code":"ambiguous_message_framing""#),
        "{response}"
    );
    assert_eq!(middleware_runs.load(Ordering::SeqCst), 0);
    assert_eq!(handler_runs.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn http1_invalid_content_length_is_rejected_by_hyper() {
    assert_http1_content_length_parse_error("Content-Length: invalid\r\n").await;
}

#[tokio::test]
async fn http1_empty_content_length_is_rejected_by_hyper() {
    assert_http1_content_length_parse_error("Content-Length:\r\n").await;
}

#[tokio::test]
async fn http1_overflowing_content_length_is_rejected_by_hyper() {
    assert_http1_content_length_parse_error("Content-Length: 18446744073709551616\r\n").await;
}

#[tokio::test]
async fn http1_conflicting_content_length_is_rejected_by_hyper() {
    assert_http1_content_length_parse_error("Content-Length: 5\r\nContent-Length: 6\r\n").await;
}

#[tokio::test]
async fn http1_ambiguous_content_length_list_is_rejected_by_hyper() {
    assert_http1_content_length_parse_error("Content-Length: 5, 6\r\n").await;
}

#[tokio::test]
async fn http1_transfer_encoding_with_content_length_is_rejected_before_dispatch() {
    for headers in [
        "Transfer-Encoding: chunked\r\nContent-Length: 5\r\n",
        "Content-Length: 5\r\nTransfer-Encoding: chunked\r\n",
    ] {
        assert_http1_ambiguous_framing_rejected_at_transport("", headers).await;
    }
}

#[tokio::test]
async fn http1_leading_empty_lines_cannot_hide_ambiguous_framing() {
    for leading_lines in ["\r\n\r\n", "\n\n", "\r\n\n"] {
        assert_http1_ambiguous_framing_rejected_at_transport(
            leading_lines,
            "Transfer-Encoding: chunked\r\nContent-Length: 5\r\n",
        )
        .await;
    }
}

#[tokio::test]
async fn http1_connection_closes_before_a_pipelined_ambiguous_request() {
    let second_handler_runs = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    let _ = app.get("/first", |_req: Request| Response::send("primera"));
    let runs = Arc::clone(&second_handler_runs);
    let _ = app.post("/second", move |_req: Request| {
        runs.fetch_add(1, Ordering::SeqCst);
        Response::send("segunda")
    });

    let (addr, shutdown, server) = spawn_app(app).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            b"GET /first HTTP/1.1\r\nHost: localhost\r\n\r\nPOST /second HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nContent-Length: 4\r\n\r\n0\r\n\r\n",
        )
        .await
        .unwrap();

    let response = read_response(&mut stream).await.unwrap();
    shutdown_server(shutdown, server).await;

    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.to_ascii_lowercase().contains("connection: close"));
    assert_eq!(response.matches("HTTP/1.1").count(), 1, "{response}");
    assert!(!response.contains("segunda"), "{response}");
    assert_eq!(second_handler_runs.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unsuccessful_upgrade_closes_before_a_pipelined_request() {
    let second_handler_runs = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    let _ = app.get("/plain", |_req: Request| Response::send("no upgrade"));
    let runs = Arc::clone(&second_handler_runs);
    let _ = app.post("/second", move |_req: Request| {
        runs.fetch_add(1, Ordering::SeqCst);
        Response::send("segunda")
    });

    let (addr, shutdown, server) = spawn_app(app).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            b"GET /plain HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\nPOST /second HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nContent-Length: 4\r\n\r\n0\r\n\r\n",
        )
        .await
        .unwrap();

    let response = read_response(&mut stream).await.unwrap();
    shutdown_server(shutdown, server).await;

    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.to_ascii_lowercase().contains("connection: close"));
    assert_eq!(response.matches("HTTP/1.1").count(), 1, "{response}");
    assert_eq!(second_handler_runs.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn http1_transfer_encoding_with_chunked_before_final_coding_is_rejected_by_hyper() {
    assert_http1_content_length_parse_error("Transfer-Encoding: chunked, gzip\r\n").await;
}

#[tokio::test]
async fn http1_identical_duplicate_content_length_is_accepted_by_hyper() {
    let middleware_runs = Arc::new(AtomicUsize::new(0));
    let handler_runs = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    let middleware_counter = Arc::clone(&middleware_runs);
    app.layer(move |req: Request, next: Next| {
        middleware_counter.fetch_add(1, Ordering::SeqCst);
        async move { next(req).await }
    });
    let handler_counter = Arc::clone(&handler_runs);
    let _ = app.post("/upload", move |mut req: Request| {
        handler_counter.fetch_add(1, Ordering::SeqCst);
        async move {
            req.bytes()
                .await
                .map(|body| Response::send(&body.len().to_string()))
        }
    });

    let (addr, shutdown, server) = spawn_app(app).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            b"POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
        )
        .await
        .unwrap();

    let response = read_response(&mut stream).await.unwrap();
    shutdown_server(shutdown, server).await;

    // Hyper validates identical duplicates and exposes a normalized Content-Length to RustRest.
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.ends_with('5'), "{response}");
    assert_eq!(middleware_runs.load(Ordering::SeqCst), 1);
    assert_eq!(handler_runs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn expect_continue_unauthorized_receives_only_the_final_response() {
    let mut app = App::new();
    app.layer(|req: Request, next: Next| async move {
        if req.header("authorization").is_none() {
            return Response::from_error(HttpError::unauthorized("Falta autenticacion"));
        }
        next(req).await
    });
    let _ = app.post("/upload", |mut req: Request| async move {
        req.bytes().await?;
        Ok::<_, HttpError>(Response::send("ok"))
    });

    let (addr, shutdown, server) = spawn_app(app).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            b"POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();

    let response = read_response(&mut stream).await.unwrap();
    shutdown_server(shutdown, server).await;

    assert!(response.starts_with("HTTP/1.1 401"), "{response}");
    assert!(!response.contains("100 Continue"), "{response}");
}

#[tokio::test]
async fn expect_continue_authorized_receives_continue_before_sending_body() {
    let mut app = App::new();
    app.layer(|req: Request, next: Next| async move {
        if req.header("authorization").is_none() {
            return Response::from_error(HttpError::unauthorized("Falta autenticacion"));
        }
        next(req).await
    });
    let _ = app.post("/upload", |mut req: Request| async move {
        let body = req.bytes().await?;
        Ok::<_, HttpError>(Response::send(&body.len().to_string()))
    });

    let (addr, shutdown, server) = spawn_app(app).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            b"POST /upload HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer test\r\nContent-Length: 5\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();

    let interim = read_headers(&mut stream).await.unwrap();
    assert_eq!(interim, "HTTP/1.1 100 Continue\r\n\r\n");

    stream.write_all(b"hello").await.unwrap();
    let response = read_response(&mut stream).await.unwrap();
    shutdown_server(shutdown, server).await;

    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.ends_with('5'), "{response}");
}

#[tokio::test]
async fn middleware_can_reject_unknown_body_before_request_body_is_sent() {
    let mut app = App::new();
    app.layer(|req: Request, next: Next| async move {
        if req.header("authorization").is_none() {
            return Response::from_error(HttpError::unauthorized("Falta autenticacion"));
        }
        next(req).await
    });
    let _ = app.post("/upload", |_req: Request| Response::send("ok"));

    let (addr, shutdown, server) = spawn_app(app).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            b"POST /upload HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n",
        )
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
    let _ = app.post("/echo", |mut req: Request| async move {
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
async fn taken_body_stream_enforces_its_configured_limit() {
    let chunks: Vec<Result<Bytes, io::Error>> = vec![
        Ok(Bytes::from_static(b"ab")),
        Ok(Bytes::from_static(b"cde")),
        Ok(Bytes::from_static(b"ignored")),
    ];
    let mut body = RequestBody::from_stream(stream::iter(chunks), 4);
    let mut stream = body.take_stream().unwrap();

    assert_eq!(stream.next().await.unwrap().unwrap(), "ab");
    let error = HttpError::body_read(stream.next().await.unwrap().unwrap_err());
    assert_eq!(error.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(error.code(), "payload_too_large");
    assert!(stream.next().await.is_none());
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
    let _ = app.post("/upload", move |_req: Request| {
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
    let _ = app.post("/upload", |_req: Request| Response::send("ok"));
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
    let _ = app.post("/bytes", move |mut req: Request| {
        let body_reads = Arc::clone(&bytes_reads);
        async move {
            body_reads.fetch_add(1, Ordering::SeqCst);
            req.bytes().await?;
            Ok::<_, HttpError>(Response::send("ok"))
        }
    });
    let text_reads = Arc::clone(&body_reads);
    let _ = app.post("/text", move |mut req: Request| {
        let body_reads = Arc::clone(&text_reads);
        async move {
            body_reads.fetch_add(1, Ordering::SeqCst);
            req.text().await?;
            Ok::<_, HttpError>(Response::send("ok"))
        }
    });
    let json_reads = Arc::clone(&body_reads);
    let _ = app.post("/json", move |mut req: Request| {
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
