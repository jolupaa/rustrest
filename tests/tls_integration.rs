#![cfg(feature = "tls")]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper::{Request as HyperRequest, Version};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustrest::{App, Request, Response, WebSocketConfig, WsError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::client::TlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderValue, header::SEC_WEBSOCKET_PROTOCOL};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{Message, handshake::client::Response as WsResponse};

const IO_TIMEOUT: Duration = Duration::from_secs(2);
const HTTP2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const HTTP2_EMPTY_SETTINGS: &[u8] = &[0, 0, 0, 4, 0, 0, 0, 0, 0];
type TlsWebSocket = WebSocketStream<TlsStream<TcpStream>>;

struct TlsFixture {
    cert: tokio_rustls::rustls::pki_types::CertificateDer<'static>,
}

impl TlsFixture {
    fn new(_name: &str) -> Self {
        use tokio_rustls::rustls::pki_types::pem::PemObject;

        let cert = tokio_rustls::rustls::pki_types::CertificateDer::from_pem_slice(include_bytes!(
            "fixtures/localhost-cert.pem"
        ))
        .expect("the committed localhost test certificate must be valid PEM");
        Self { cert }
    }

    fn server_config(&self) -> rustrest::tls::ServerConfig {
        rustrest::tls::config_from_pem(
            fixture_path("localhost-cert.pem"),
            fixture_path("localhost-key.pem"),
        )
        .unwrap()
    }

    fn connector_with_alpn(&self, protocols: &[&[u8]]) -> tokio_rustls::TlsConnector {
        let mut roots = tokio_rustls::rustls::RootCertStore::empty();
        roots.add(self.cert.clone()).unwrap();
        let mut config = tokio_rustls::rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        config.alpn_protocols = protocols.iter().map(|protocol| protocol.to_vec()).collect();
        tokio_rustls::TlsConnector::from(Arc::new(config))
    }

    async fn tls_stream(&self, addr: std::net::SocketAddr) -> TlsStream<TcpStream> {
        self.tls_stream_with_alpn(addr, &[]).await
    }

    async fn tls_stream_with_alpn(
        &self,
        addr: std::net::SocketAddr,
        protocols: &[&[u8]],
    ) -> TlsStream<TcpStream> {
        tokio::time::timeout(IO_TIMEOUT, async {
            let stream = loop {
                match TcpStream::connect(addr).await {
                    Ok(stream) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    Err(error) => panic!("TLS TCP connection failed: {error}"),
                }
            };
            let server_name =
                tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap();
            self.connector_with_alpn(protocols)
                .connect(server_name, stream)
                .await
                .unwrap()
        })
        .await
        .expect("TLS connection should complete before the deadline")
    }

    async fn websocket(
        &self,
        addr: std::net::SocketAddr,
        path: &str,
        protocol: Option<&'static str>,
    ) -> (TlsWebSocket, WsResponse) {
        let tls = self.tls_stream(addr).await;
        let mut request = format!("wss://localhost:{}{path}", addr.port())
            .into_client_request()
            .unwrap();
        request.headers_mut().insert(
            "origin",
            format!("https://localhost:{}", addr.port())
                .parse()
                .unwrap(),
        );
        if let Some(protocol) = protocol {
            request
                .headers_mut()
                .insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static(protocol));
        }
        tokio::time::timeout(IO_TIMEOUT, tokio_tungstenite::client_async(request, tls))
            .await
            .expect("WSS handshake should complete before the deadline")
            .unwrap()
    }
}

fn fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

async fn next_message(client: &mut TlsWebSocket) -> Message {
    tokio::time::timeout(IO_TIMEOUT, client.next())
        .await
        .expect("WSS message should arrive before the deadline")
        .expect("WSS stream should remain open")
        .expect("WSS frame should be valid")
}

async fn read_tls_until_closed(stream: &mut TlsStream<TcpStream>) -> Vec<u8> {
    let mut received = Vec::new();
    tokio::time::timeout(IO_TIMEOUT, async {
        loop {
            let mut chunk = [0_u8; 512];
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(length) => received.extend_from_slice(&chunk[..length]),
            }
        }
    })
    .await
    .expect("the TLS connection should close before the deadline");
    received
}

#[tokio::test]
async fn serves_https_with_rustls() {
    let fixture = TlsFixture::new("https");
    let mut app = App::new();
    app.get("/secure", |_req: Request| Response::send("hola tls"))
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve_tls(listener, fixture.server_config()));
    let mut tls = fixture.tls_stream(addr).await;

    tokio::time::timeout(IO_TIMEOUT, async {
        tls.write_all(b"GET /secure HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        tls.read_to_string(&mut response).await.unwrap();
        response
    })
    .await
    .map(|response| {
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.ends_with("hola tls"), "{response}");
    })
    .expect("HTTPS exchange should complete before the deadline");
    server.abort();
}

#[tokio::test]
async fn serves_http2_over_tls_when_alpn_negotiates_h2() {
    let fixture = TlsFixture::new("https-h2");
    let mut app = App::new();
    app.get("/secure-h2", |req: Request| {
        assert_eq!(req.version(), Version::HTTP_2);
        Response::send("hola tls h2")
    })
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve_tls(listener, fixture.server_config()));
    let tls = fixture.tls_stream_with_alpn(addr, &[b"h2"]).await;
    assert_eq!(tls.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));

    let (mut sender, connection) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake::<_, Empty<Bytes>>(TokioIo::new(tls))
        .await
        .unwrap();
    let client_connection = tokio::spawn(connection);
    let request = HyperRequest::builder()
        .uri(format!("https://localhost:{}/secure-h2", addr.port()))
        .body(Empty::<Bytes>::new())
        .unwrap();
    let response = tokio::time::timeout(IO_TIMEOUT, sender.send_request(request))
        .await
        .expect("TLS HTTP/2 response should arrive")
        .unwrap();
    assert_eq!(response.version(), Version::HTTP_2);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body, "hola tls h2");

    drop(sender);
    server.abort();
    client_connection.abort();
}

#[tokio::test]
async fn tls_rejects_protocols_that_conflict_with_negotiated_alpn() {
    let fixture = TlsFixture::new("alpn-mismatch");
    let dispatches = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    let dispatches_for_handler = Arc::clone(&dispatches);
    app.get("/secure", move |_req: Request| {
        dispatches_for_handler.fetch_add(1, Ordering::SeqCst);
        Response::send("unexpected")
    })
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve_tls(listener, fixture.server_config()));

    let mut h2_tls = fixture.tls_stream_with_alpn(addr, &[b"h2"]).await;
    assert_eq!(h2_tls.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));
    h2_tls
        .write_all(b"GET /secure HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    assert!(read_tls_until_closed(&mut h2_tls).await.is_empty());

    let mut http1_tls = fixture.tls_stream(addr).await;
    assert_eq!(http1_tls.get_ref().1.alpn_protocol(), None);
    http1_tls.write_all(HTTP2_PREFACE).await.unwrap();
    http1_tls.write_all(HTTP2_EMPTY_SETTINGS).await.unwrap();
    assert!(read_tls_until_closed(&mut http1_tls).await.is_empty());

    server.abort();
    assert_eq!(dispatches.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn tls_raw_preflight_rejects_ambiguous_http1_framing() {
    let fixture = TlsFixture::new("ambiguous-framing");
    let dispatches = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    let dispatches_for_handler = Arc::clone(&dispatches);
    app.post("/upload", move |_req: Request| {
        dispatches_for_handler.fetch_add(1, Ordering::SeqCst);
        Response::send("unexpected")
    })
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve_tls(listener, fixture.server_config()));
    let mut tls = fixture.tls_stream(addr).await;

    tls.write_all(
        b"\r\n\r\nPOST /upload HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nContent-Length: 4\r\n\r\n0\r\n\r\n",
    )
    .await
    .unwrap();
    let response = String::from_utf8(read_tls_until_closed(&mut tls).await).unwrap();
    server.abort();

    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    assert!(response.contains("ambiguous_message_framing"), "{response}");
    assert_eq!(dispatches.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn tls_honors_http1_header_read_timeout() {
    let fixture = TlsFixture::new("header-timeout");
    let mut app = App::new();
    app.header_read_timeout(Duration::from_millis(50));
    app.get("/secure", |_req: Request| Response::send("hola tls"))
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve_tls(listener, fixture.server_config()));
    let mut tls = fixture.tls_stream(addr).await;

    tls.write_all(b"GET /secure HTTP/1.1\r\nHost: local")
        .await
        .unwrap();
    let mut first_byte = [0_u8; 1];
    let read = tokio::time::timeout(IO_TIMEOUT, tls.read(&mut first_byte))
        .await
        .expect("TLS HTTP/1 header timeout should close the connection or send a response");
    if let Ok(1) = read {
        assert_eq!(first_byte[0], b'H');
    }

    server.abort();
}

#[tokio::test]
async fn tls_handshake_timeout_closes_silent_tcp_clients() {
    let fixture = TlsFixture::new("handshake-timeout");
    let mut app = App::new();
    app.tls_handshake_timeout(Duration::from_millis(50));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve_tls(listener, fixture.server_config()));
    let mut raw = TcpStream::connect(addr).await.unwrap();

    let mut byte = [0_u8; 1];
    let closed = tokio::time::timeout(IO_TIMEOUT, raw.read(&mut byte))
        .await
        .expect("silent TLS client should be closed by the handshake deadline");
    assert!(matches!(closed, Ok(0) | Err(_)), "{closed:?}");

    server.abort();
}

#[tokio::test]
async fn graceful_timeout_aborts_incomplete_tls_handshakes() {
    let fixture = TlsFixture::new("handshake-shutdown");
    let mut app = App::new();
    app.tls_handshake_timeout(Duration::from_secs(5))
        .graceful_shutdown_timeout(Duration::from_millis(50));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server =
        tokio::spawn(
            app.serve_tls_with_shutdown(listener, fixture.server_config(), async move {
                let _ = shutdown_rx.await;
            }),
        );
    let mut raw = TcpStream::connect(addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(25)).await;
    shutdown_tx.send(()).unwrap();

    let result = tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("graceful timeout should bound a slow TLS handshake")
        .expect("TLS server task should not panic");
    assert!(result.is_ok());

    let mut byte = [0_u8; 1];
    let closed = tokio::time::timeout(Duration::from_millis(250), raw.read(&mut byte))
        .await
        .expect("aborted TLS handshake should close");
    assert!(matches!(closed, Ok(0) | Err(_)), "{closed:?}");
}

#[tokio::test]
async fn websocket_tls_negotiates_protocol_and_echoes_text_binary() {
    let fixture = TlsFixture::new("echo");
    let mut app = App::new();
    app.websocket_with(
        "/ws",
        WebSocketConfig::new()
            .protocols(&["chat"])
            .require_protocol(true),
        |mut socket| async move {
            while let Some(message) = socket.recv().await? {
                if message.is_text() || message.is_binary() {
                    socket.send(message).await?;
                } else if message.is_close() {
                    break;
                }
            }
            Ok::<(), WsError>(())
        },
    )
    .unwrap();
    let runtime = app.websocket_runtime();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve_tls(listener, fixture.server_config()));
    let (mut client, response) = fixture.websocket(addr, "/ws", Some("chat")).await;

    assert_eq!(
        response
            .headers()
            .get(SEC_WEBSOCKET_PROTOCOL)
            .and_then(|value| value.to_str().ok()),
        Some("chat")
    );
    tokio::time::timeout(IO_TIMEOUT, client.send(Message::text("hola wss")))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next_message(&mut client).await, Message::text("hola wss"));
    tokio::time::timeout(IO_TIMEOUT, client.send(Message::binary(vec![1, 2, 3])))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        next_message(&mut client).await,
        Message::binary(vec![1, 2, 3])
    );
    tokio::time::timeout(IO_TIMEOUT, client.send(Message::Close(None)))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(next_message(&mut client).await, Message::Close(_)));
    tokio::time::timeout(IO_TIMEOUT, async {
        while runtime.stats().active_connections != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    server.abort();
}

#[tokio::test]
async fn websocket_tls_rooms_broadcast_over_wss() {
    let fixture = TlsFixture::new("rooms");
    let mut app = App::new();
    app.websocket("/rooms", |mut socket| async move {
        socket.join("general").await?;
        socket.send_text("ready").await?;
        while let Some(message) = socket.recv().await? {
            if message.is_text() {
                if let Err(error) = socket.to("general").send(message).await {
                    eprintln!("Fallo de broadcast WSS: {error}");
                }
            }
        }
        Ok::<(), WsError>(())
    })
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve_tls(listener, fixture.server_config()));
    let (mut origin, _) = fixture.websocket(addr, "/rooms", None).await;
    let (mut peer, _) = fixture.websocket(addr, "/rooms", None).await;
    assert_eq!(next_message(&mut origin).await, Message::text("ready"));
    assert_eq!(next_message(&mut peer).await, Message::text("ready"));

    tokio::time::timeout(IO_TIMEOUT, origin.send(Message::text("hola room wss")))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        next_message(&mut peer).await,
        Message::text("hola room wss")
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), origin.next())
            .await
            .is_err()
    );
    server.abort();
}

#[tokio::test]
async fn websocket_tls_heartbeat_keeps_connection_alive() {
    let fixture = TlsFixture::new("heartbeat");
    let mut app = App::new();
    app.websocket_with(
        "/heartbeat",
        WebSocketConfig::new()
            .ping_interval(Duration::from_millis(100))
            .pong_timeout(Duration::from_millis(40)),
        |_socket| async move {
            std::future::pending::<()>().await;
        },
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve_tls(listener, fixture.server_config()));
    let (mut client, _) = fixture.websocket(addr, "/heartbeat", None).await;

    for _ in 0..2 {
        assert!(matches!(next_message(&mut client).await, Message::Ping(_)));
        tokio::time::timeout(IO_TIMEOUT, client.flush())
            .await
            .unwrap()
            .unwrap();
    }
    server.abort();
}

#[tokio::test]
async fn websocket_tls_shutdown_sends_1001_and_drains_runtime() {
    let fixture = TlsFixture::new("shutdown");
    let mut app = App::new();
    app.websocket_defaults(WebSocketConfig::new().close_timeout(Duration::from_millis(200)));
    app.websocket("/ws", |_socket| async move {
        std::future::pending::<()>().await;
    })
    .unwrap();
    let runtime = app.websocket_runtime();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server =
        tokio::spawn(
            app.serve_tls_with_shutdown(listener, fixture.server_config(), async move {
                let _ = shutdown_rx.await;
            }),
        );
    let (mut client, _) = fixture.websocket(addr, "/ws", None).await;

    tokio::time::timeout(IO_TIMEOUT, async {
        while runtime.stats().active_connections != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("websocket should register before shutdown");
    shutdown_tx.send(()).unwrap();

    let Message::Close(Some(frame)) = next_message(&mut client).await else {
        panic!("expected a close frame");
    };
    assert_eq!(frame.code, CloseCode::Away);
    assert_eq!(frame.reason, "apagado del servidor");
    tokio::time::timeout(IO_TIMEOUT, client.flush())
        .await
        .unwrap()
        .unwrap();
    let result = tokio::time::timeout(IO_TIMEOUT, server)
        .await
        .expect("TLS server shutdown should finish before the deadline")
        .expect("TLS server task should not panic");
    assert!(result.is_ok());
    assert_eq!(runtime.stats().active_connections, 0);
}
