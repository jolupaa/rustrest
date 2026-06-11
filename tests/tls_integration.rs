#![cfg(feature = "tls")]

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rustrest::{App, Request, Response};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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
