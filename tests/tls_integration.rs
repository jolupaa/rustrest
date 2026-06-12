#![cfg(feature = "tls")]

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use rustrest::{App, Request, Response, WebSocketConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

#[tokio::test]
async fn serves_https_with_rustls() {
    // Self-signed certificate for localhost (test-only).
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();

    // Round-trip the PEMs through the loader helper.
    let dir = std::env::temp_dir().join(format!(
        "rustrest-tls-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, certified.cert.pem()).unwrap();
    std::fs::write(&key_path, certified.key_pair.serialize_pem()).unwrap();
    let server_config = rustrest::tls::config_from_pem(&cert_path, &key_path).unwrap();

    let mut app = App::new();
    app.get("/secure", |_req: Request| Response::send("hola tls"));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(app.serve_tls(listener, server_config));

    // A rustls client that trusts only our self-signed certificate.
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    roots.add(certified.cert.der().clone()).unwrap();
    let client_config = tokio_rustls::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
    let stream = TcpStream::connect(addr).await.unwrap();
    let server_name = tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let mut tls = connector.connect(server_name, stream).await.unwrap();

    tls.write_all(b"GET /secure HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = String::new();
    tls.read_to_string(&mut response).await.unwrap();
    server.abort();
    std::fs::remove_dir_all(dir).ok();

    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.ends_with("hola tls"), "{response}");
}

#[tokio::test]
async fn websocket_tls_shutdown_sends_1001_and_drains_runtime() {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let dir = std::env::temp_dir().join(format!(
        "rustrest-tls-websocket-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, certified.cert.pem()).unwrap();
    std::fs::write(&key_path, certified.key_pair.serialize_pem()).unwrap();
    let server_config = rustrest::tls::config_from_pem(&cert_path, &key_path).unwrap();

    let mut app = App::new();
    app.websocket_defaults(WebSocketConfig::new().close_timeout(Duration::from_millis(200)));
    app.websocket("/ws", |_socket| async move {
        std::future::pending::<()>().await;
    });
    let runtime = app.websocket_runtime();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        app.serve_tls_with_shutdown(listener, server_config, async move {
            let _ = shutdown_rx.await;
        }),
    );

    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    roots.add(certified.cert.der().clone()).unwrap();
    let client_config = tokio_rustls::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
    let stream = TcpStream::connect(addr).await.unwrap();
    let server_name = tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let tls = connector.connect(server_name, stream).await.unwrap();
    let (mut client, _) =
        tokio_tungstenite::client_async(format!("wss://localhost:{}/ws", addr.port()), tls)
            .await
            .unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        while runtime.stats().active_connections != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("websocket should register before shutdown");
    shutdown_tx.send(()).unwrap();

    let message = tokio::time::timeout(Duration::from_secs(2), client.next())
        .await
        .expect("TLS websocket close should arrive before the deadline")
        .expect("server should send a websocket frame")
        .expect("TLS websocket close should be valid");
    let Message::Close(Some(frame)) = message else {
        panic!("expected a close frame, got {message:?}");
    };
    assert_eq!(frame.code, CloseCode::Away);
    assert_eq!(frame.reason, "apagado del servidor");
    client.flush().await.unwrap();

    let result = tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("TLS server shutdown should finish before the deadline")
        .expect("TLS server task should not panic");
    std::fs::remove_dir_all(dir).ok();
    assert!(result.is_ok());
    assert_eq!(runtime.stats().active_connections, 0);
}
