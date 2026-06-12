#![cfg(feature = "tls")]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
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
type TlsWebSocket = WebSocketStream<TlsStream<TcpStream>>;

struct TlsFixture {
    dir: PathBuf,
    cert: tokio_rustls::rustls::pki_types::CertificateDer<'static>,
}

impl TlsFixture {
    fn new(name: &str) -> Self {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let dir = std::env::temp_dir().join(format!(
            "rustrest-tls-{name}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("cert.pem"), certified.cert.pem()).unwrap();
        std::fs::write(dir.join("key.pem"), certified.key_pair.serialize_pem()).unwrap();
        Self {
            dir,
            cert: certified.cert.der().clone(),
        }
    }

    fn server_config(&self) -> rustrest::tls::ServerConfig {
        rustrest::tls::config_from_pem(self.dir.join("cert.pem"), self.dir.join("key.pem")).unwrap()
    }

    fn connector(&self) -> tokio_rustls::TlsConnector {
        let mut roots = tokio_rustls::rustls::RootCertStore::empty();
        roots.add(self.cert.clone()).unwrap();
        let config = tokio_rustls::rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        tokio_rustls::TlsConnector::from(Arc::new(config))
    }

    async fn tls_stream(&self, addr: std::net::SocketAddr) -> TlsStream<TcpStream> {
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
            self.connector().connect(server_name, stream).await.unwrap()
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

impl Drop for TlsFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn next_message(client: &mut TlsWebSocket) -> Message {
    tokio::time::timeout(IO_TIMEOUT, client.next())
        .await
        .expect("WSS message should arrive before the deadline")
        .expect("WSS stream should remain open")
        .expect("WSS frame should be valid")
}

#[tokio::test]
async fn serves_https_with_rustls() {
    let fixture = TlsFixture::new("https");
    let mut app = App::new();
    app.get("/secure", |_req: Request| Response::send("hola tls"));
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
    );
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
            if message.is_text()
                && let Err(error) = socket.to("general").send(message).await
            {
                eprintln!("Fallo de broadcast WSS: {error}");
            }
        }
        Ok::<(), WsError>(())
    });
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
    );
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
    });
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
