use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper::{Request as HyperRequest, Version};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustrest::app::{App, HttpError, Request, Response, TestClient};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const HTTP2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const HTTP2_EMPTY_SETTINGS: &[u8] = &[0, 0, 0, 4, 0, 0, 0, 0, 0];

fn manual_switching_protocols_response() -> Response {
    Response::send("")
        .status(101)
        .header("connection", "Upgrade")
        .header("upgrade", "websocket")
        // Valid base64 encoding of 20 bytes, but intentionally not the
        // digest corresponding to the request's Sec-WebSocket-Key.
        .header("sec-websocket-accept", "AAAAAAAAAAAAAAAAAAAAAAAAAAA=")
}

fn unowned_but_well_formed_switching_protocols_response() -> Response {
    Response::send("")
        .status(101)
        .header("connection", "Upgrade")
        .header("upgrade", "websocket")
        // Correct digest for the fixed RFC example key used below. A matching
        // digest alone must not authorize a protocol switch: only the
        // consuming WebSocket API owns the upgraded transport.
        .header("sec-websocket-accept", "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")
}

async fn raw_exchange(addr: std::net::SocketAddr, request: &[u8]) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(request).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    })
    .await
    .expect("raw HTTP exchange should finish")
}

#[tokio::test]
async fn duplicate_request_headers_are_all_preserved() {
    let mut app = App::new();
    let _ = app.get("/h", |req: Request| {
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
async fn duplicate_cookie_headers_are_combined_in_arrival_order() {
    let mut app = App::new();
    let _ = app.get("/cookies", |req: Request| {
        Response::send(&format!(
            "{}|{}",
            req.cookie("sid").unwrap_or("missing"),
            req.cookie("theme").unwrap_or("missing")
        ))
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve(listener));
    let response = raw_exchange(
        addr,
        b"GET /cookies HTTP/1.1\r\nHost: localhost\r\nCookie: sid=abc\r\nCookie: theme=dark\r\nConnection: close\r\n\r\n",
    )
    .await;
    server.abort();

    assert!(response.ends_with("abc|dark"), "{response}");
}

#[tokio::test]
async fn http1_response_trailers_follow_client_te_negotiation() {
    let mut app = App::new();
    let _ = app.get("/trailers", |_req: Request| {
        let mut trailers = hyper::HeaderMap::new();
        trailers.insert("x-checksum", hyper::header::HeaderValue::from_static("abc"));
        Response::send("data").with_trailers(trailers)
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve(listener));

    let negotiated = raw_exchange(
        addr,
        b"GET /trailers HTTP/1.1\r\nHost: localhost\r\nTE: trailers\r\nConnection: TE, close\r\n\r\n",
    )
    .await;
    let negotiated_body = negotiated.split_once("\r\n\r\n").unwrap().1;
    assert!(
        negotiated_body.ends_with("0\r\nx-checksum: abc\r\n\r\n"),
        "{negotiated}"
    );

    let unsupported = raw_exchange(
        addr,
        b"GET /trailers HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    let unsupported_body = unsupported.split_once("\r\n\r\n").unwrap().1;
    assert!(unsupported_body.ends_with("0\r\n\r\n"), "{unsupported}");
    assert!(
        !unsupported_body.contains("\r\nx-checksum: abc\r\n"),
        "{unsupported}"
    );

    server.abort();
}

#[tokio::test]
async fn request_exposes_client_peer_address() {
    let mut app = App::new();
    let _ = app.get("/whoami", |req: Request| match req.remote_addr() {
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
    let _ = app.post("/upload", |mut req: Request| async move {
        req.bytes().await?;
        Ok::<_, HttpError>(Response::send("ok"))
    });

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
    let _ = app.get("/slow", |_req: Request| async {
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
async fn slow_request_body_is_bounded_by_the_request_deadline() {
    let mut app = App::new();
    app.request_timeout(Duration::from_millis(50));
    let _ = app.post("/slow-body", |mut req: Request| async move {
        let body = req.bytes().await?;
        Ok::<_, HttpError>(Response::send(&format!("{} bytes", body.len())))
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve(listener));
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            b"POST /slow-body HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nConnection: close\r\n\r\nx",
        )
        .await
        .unwrap();

    let mut response = String::new();
    tokio::time::timeout(Duration::from_secs(1), stream.read_to_string(&mut response))
        .await
        .expect("a stalled body must not hold the request open")
        .unwrap();
    server.abort();

    assert!(
        response.starts_with("HTTP/1.1 408"),
        "expected 408, got: {response}"
    );
}

#[tokio::test]
async fn serves_real_http2_prior_knowledge_requests() {
    let mut app = App::new();
    let _ = app.get("/h2", |req: Request| {
        assert_eq!(req.version(), Version::HTTP_2);
        Response::send("hola h2")
    });
    let _ = app.get("/h2-headers", |_req: Request| {
        Response::send("ok")
            .header("connection", "keep-alive, x-remove")
            .header("keep-alive", "timeout=5")
            .header("upgrade", "h2c")
            .header("x-remove", "nominated")
            .header("x-keep", "end-to-end")
    });
    let _ = app.get("/manual-101", |_req: Request| {
        manual_switching_protocols_response()
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve(listener));
    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake::<_, Empty<Bytes>>(TokioIo::new(stream))
        .await
        .unwrap();
    let client_connection = tokio::spawn(connection);

    let request = HyperRequest::builder()
        .uri(format!("http://{addr}/h2"))
        .body(Empty::<Bytes>::new())
        .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(1), sender.send_request(request))
        .await
        .expect("HTTP/2 response should arrive")
        .unwrap();
    assert_eq!(response.version(), Version::HTTP_2);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body, "hola h2");

    let request = HyperRequest::builder()
        .uri(format!("http://{addr}/h2-headers"))
        .body(Empty::<Bytes>::new())
        .unwrap();
    let response = sender.send_request(request).await.unwrap();
    assert!(response.headers().get("connection").is_none());
    assert!(response.headers().get("keep-alive").is_none());
    assert!(response.headers().get("upgrade").is_none());
    assert!(response.headers().get("x-remove").is_none());
    assert_eq!(response.headers().get("x-keep").unwrap(), "end-to-end");
    response.into_body().collect().await.unwrap();

    let request = HyperRequest::builder()
        .uri(format!("http://{addr}/manual-101"))
        .body(Empty::<Bytes>::new())
        .unwrap();
    let response = sender.send_request(request).await.unwrap();
    assert_eq!(response.status(), 500);

    drop(sender);
    server.abort();
    client_connection.abort();
}

#[tokio::test]
#[allow(deprecated)]
async fn manual_http1_switching_protocols_without_upgrade_is_rejected() {
    let mut app = App::new();
    let _ = app.get("/manual-101", |_req: Request| {
        manual_switching_protocols_response()
    });
    let _ = app.get("/manual-correct-101", |_req: Request| {
        unowned_but_well_formed_switching_protocols_response()
    });
    let _ = app.get("/legacy-helper", |req: Request| Response::websocket(&req));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve(listener));

    let response = raw_exchange(
        addr,
        b"GET /manual-101 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 500"), "{response}");

    let wrong_accept = raw_exchange(
        addr,
        b"GET /manual-101 HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
    )
    .await;
    assert!(wrong_accept.starts_with("HTTP/1.1 500"), "{wrong_accept}");

    let unowned_upgrade = raw_exchange(
        addr,
        b"GET /manual-correct-101 HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
    )
    .await;
    assert!(
        unowned_upgrade.starts_with("HTTP/1.1 500"),
        "{unowned_upgrade}"
    );

    let legacy = raw_exchange(
        addr,
        b"GET /legacy-helper HTTP/1.1\r\nHost: localhost\r\nOrigin: http://localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
    )
    .await;
    assert!(legacy.starts_with("HTTP/1.1 500"), "{legacy}");
    server.abort();
}

#[tokio::test]
async fn incomplete_http2_prefaces_are_closed_under_the_header_deadline() {
    let mut app = App::new();
    app.max_connections(1)
        .header_read_timeout(Duration::from_millis(50));
    let _ = app.get("/ping", |_req: Request| Response::send("pong"));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve(listener));

    for partial in [&HTTP2_PREFACE[..18], HTTP2_PREFACE] {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(partial).await.unwrap();
        let mut response = Vec::new();
        let closed =
            tokio::time::timeout(Duration::from_secs(1), stream.read_to_end(&mut response))
                .await
                .expect("an incomplete HTTP/2 preface should be closed");
        assert!(closed.is_ok(), "{closed:?}");
    }

    let probe = raw_exchange(
        addr,
        b"GET /ping HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(probe.ends_with("pong"), "{probe}");
    server.abort();
}

#[tokio::test]
async fn unresponsive_http2_peer_is_closed_by_keepalive_timeout() {
    let mut app = App::new();
    app.header_read_timeout(Duration::from_millis(250))
        .http2_keep_alive_interval(Duration::from_millis(20))
        .http2_keep_alive_timeout(Duration::from_millis(20));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve(listener));
    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream.write_all(HTTP2_PREFACE).await.unwrap();
    stream.write_all(HTTP2_EMPTY_SETTINGS).await.unwrap();
    let mut received = Vec::new();
    let closed = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let mut chunk = [0_u8; 256];
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(length) => received.extend_from_slice(&chunk[..length]),
            }
        }
    })
    .await;
    server.abort();

    assert!(
        closed.is_ok(),
        "an HTTP/2 peer that ignores PING must be closed"
    );
    assert!(
        !received.is_empty(),
        "the server should complete the HTTP/2 handshake before probing liveness"
    );
}

#[tokio::test]
async fn serve_with_shutdown_returns_after_signal() {
    let mut app = App::new();
    let _ = app.get("/ping", |_req: Request| Response::send("pong"));

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
    let _ = app.get("/hello", |_req: Request| Response::send("hello http"));

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
async fn request_head_limits_are_enforced_before_dispatch() {
    let mut app = App::new();
    app.max_request_target_size(16)
        .max_query_string_size(4)
        .max_request_header_count(3)
        .max_request_header_bytes(64);
    let _ = app.get("/ok", |_req: Request| Response::send("ok"));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve(listener));

    let target = raw_exchange(
        addr,
        b"GET /this-target-is-too-long HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(target.starts_with("HTTP/1.1 414"), "{target}");
    assert!(target.contains(r#""code":"request_target_too_large""#));

    let query = raw_exchange(
        addr,
        b"GET /ok?12345 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(query.starts_with("HTTP/1.1 414"), "{query}");
    assert!(query.contains(r#""code":"query_too_large""#));

    let too_many = raw_exchange(
        addr,
        b"GET /ok HTTP/1.1\r\nHost: localhost\r\nX-One: 1\r\nX-Two: 2\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(too_many.starts_with("HTTP/1.1 431"), "{too_many}");

    let oversized = raw_exchange(
        addr,
        b"GET /ok HTTP/1.1\r\nHost: localhost\r\nX-Large: abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(oversized.starts_with("HTTP/1.1 431"), "{oversized}");
    assert!(
        oversized.contains(r#""code":"request_headers_too_large""#),
        "{oversized}"
    );

    server.abort();
}

#[tokio::test]
async fn conflicting_absolute_authority_and_host_are_rejected() {
    let mut app = App::new();
    let _ = app.get("/ok", |_req: Request| Response::send("ok"));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve(listener));

    let response = raw_exchange(
        addr,
        b"GET http://example.test/ok HTTP/1.1\r\nHost: other.test\r\nConnection: close\r\n\r\n",
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    assert!(response.contains(r#""code":"invalid_authority""#));

    let missing_host = raw_exchange(
        addr,
        b"GET http://example.test/ok HTTP/1.1\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(missing_host.starts_with("HTTP/1.1 400"), "{missing_host}");
    assert!(
        missing_host.contains(r#""code":"invalid_authority""#),
        "{missing_host}"
    );
    server.abort();
}

#[tokio::test]
async fn connection_admission_limit_is_global_and_releases_on_close() {
    let mut app = App::new();
    app.max_connections(1)
        .header_read_timeout(Duration::from_secs(5));
    let _ = app.get("/ping", |_req: Request| Response::send("pong"));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(app.serve_with_shutdown(listener, async move {
        let _ = shutdown_rx.await;
    }));

    let mut first = TcpStream::connect(addr).await.unwrap();
    first
        .write_all(b"GET /ping HTTP/1.1\r\nHost: local")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(25)).await;

    let mut second = TcpStream::connect(addr).await.unwrap();
    let _ = second
        .write_all(b"GET /ping HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await;
    let mut rejected = Vec::new();
    let second_result = tokio::time::timeout(
        Duration::from_millis(500),
        second.read_to_end(&mut rejected),
    )
    .await
    .expect("over-limit connection should be closed promptly");
    if second_result.is_ok() {
        assert!(
            !String::from_utf8_lossy(&rejected).contains("pong"),
            "over-limit connection was unexpectedly served"
        );
    }

    drop(first);
    let mut accepted = false;
    for _ in 0..20 {
        if let Ok(mut probe) = TcpStream::connect(addr).await {
            if probe
                .write_all(b"GET /ping HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .await
                .is_ok()
            {
                let mut response = String::new();
                if let Ok(Ok(_)) = tokio::time::timeout(
                    Duration::from_millis(500),
                    probe.read_to_string(&mut response),
                )
                .await
                {
                    if response.ends_with("pong") {
                        accepted = true;
                        break;
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(accepted, "the admission permit was not released");

    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("server should finish")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn graceful_timeout_aborts_partial_plaintext_connections() {
    let mut app = App::new();
    app.header_read_timeout(Duration::from_secs(5))
        .graceful_shutdown_timeout(Duration::from_millis(50));
    let _ = app.get("/ping", |_req: Request| Response::send("pong"));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(app.serve_with_shutdown(listener, async move {
        let _ = shutdown_rx.await;
    }));

    let mut partial = TcpStream::connect(addr).await.unwrap();
    partial
        .write_all(b"GET /ping HTTP/1.1\r\nHost: local")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(25)).await;
    shutdown_tx.send(()).unwrap();

    let result = tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("graceful timeout should bound shutdown")
        .expect("server task should not panic");
    assert!(result.is_ok());

    let mut byte = [0_u8; 1];
    let closed = tokio::time::timeout(Duration::from_millis(250), partial.read(&mut byte))
        .await
        .expect("aborted connection should close");
    assert!(matches!(closed, Ok(0) | Err(_)), "{closed:?}");
}

#[tokio::test]
async fn websocket_only_get_does_not_implicitly_serve_head() {
    let mut app = App::new();
    app.websocket("/ws", |_socket| async {}).unwrap();
    let client = TestClient::new(app);

    let head = client.head("/ws").send().await;
    assert_eq!(head.status, 405);
    let head_allow = head
        .headers
        .get("allow")
        .and_then(|value| value.to_str().ok())
        .unwrap();
    assert_eq!(head_allow, "GET, OPTIONS");

    let options = client.options("/ws").send().await;
    assert_eq!(options.status, 204);
    let options_allow = options
        .headers
        .get("allow")
        .and_then(|value| value.to_str().ok())
        .unwrap();
    assert_eq!(options_allow, "GET, OPTIONS");
}
