//! HTTPS serving via rustls (cargo feature `tls`): `App::listen_tls` /
//! `serve_tls`, plus a PEM certificate/key loader.

use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use hyper_util::server::graceful::GracefulShutdown;
use rustls_pki_types::pem::{Error as PemError, PemObject};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio_rustls::TlsAcceptor;

pub use tokio_rustls::rustls::ServerConfig;

use super::App;
use super::server::{
    AdmittedIo, DetectedProtocol, ExpectedProtocol, TransportSecurity, drain_server_connections,
    finish_connection_tasks, preflight_connection, reap_connection_tasks, try_admit_connection,
};

fn expected_protocol_from_alpn(alpn: Option<&[u8]>) -> io::Result<ExpectedProtocol> {
    match alpn {
        Some(b"h2") => Ok(ExpectedProtocol::Http2),
        Some(b"http/1.1") | None => Ok(ExpectedProtocol::Http1),
        Some(protocol) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "el protocolo ALPN negociado no es compatible: {}",
                String::from_utf8_lossy(protocol)
            ),
        )),
    }
}

/// Builds a rustls [`ServerConfig`] from PEM certificate-chain and private-key
/// files, with ALPN advertising HTTP/2 and HTTP/1.1.
pub fn config_from_pem(
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> io::Result<ServerConfig> {
    let certs = CertificateDer::pem_file_iter(cert_path)
        .map_err(pem_error_to_io)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(pem_error_to_io)?;
    let key = match PrivateKeyDer::from_pem_file(key_path) {
        Ok(key) => key,
        Err(PemError::NoItemsFound) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "no private key found in PEM",
            ));
        }
        Err(error) => return Err(pem_error_to_io(error)),
    };

    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(config)
}

fn pem_error_to_io(error: PemError) -> io::Error {
    match error {
        PemError::Io(error) => error,
        error => io::Error::new(io::ErrorKind::InvalidData, error),
    }
}

impl App {
    /// Binds to `address` and serves HTTPS until the process is killed.
    pub async fn listen_tls(
        self,
        address: impl ToSocketAddrs,
        config: ServerConfig,
    ) -> io::Result<()> {
        self.validate_websockets()?;
        let listener = TcpListener::bind(address).await?;
        if let Ok(local) = listener.local_addr() {
            println!("Server listening at https://{}", local);
        }
        self.serve_tls(listener, config).await
    }

    /// Serves HTTPS connections on `listener` until the process is killed.
    pub async fn serve_tls(self, listener: TcpListener, config: ServerConfig) -> io::Result<()> {
        self.serve_tls_with_shutdown(listener, config, std::future::pending::<()>())
            .await
    }

    /// Serves HTTPS until `shutdown` resolves, then drains in-flight
    /// connections like [`App::serve_with_shutdown`]. TLS handshake failures
    /// are logged per connection and never tear down the server.
    pub async fn serve_tls_with_shutdown(
        self,
        listener: TcpListener,
        config: ServerConfig,
        shutdown: impl Future<Output = ()> + Send,
    ) -> io::Result<()> {
        self.validate_websockets()?;
        self.websocket_runtime().start_broker().await;
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let builder = Arc::new(self.connection_builder());
        let websocket_builder = Arc::new(self.websocket_connection_builder());
        let http2_builder = Arc::new(self.http2_connection_builder());
        let admission = self.connection_admission();
        let handshake_timeout = self.config.tls_handshake_timeout;
        let header_read_timeout = self.config.header_read_timeout;
        let max_head_bytes = self.max_http1_head_bytes();
        let graceful_timeout = self.config.graceful_shutdown_timeout;
        let app = Arc::new(self);
        let graceful = GracefulShutdown::new();
        let mut connection_tasks = tokio::task::JoinSet::new();
        let mut shutdown = std::pin::pin!(shutdown);

        loop {
            reap_connection_tasks(&mut connection_tasks);
            let (stream, peer) = tokio::select! {
                accepted = listener.accept() => match accepted {
                    Ok(pair) => pair,
                    Err(err) => {
                        eprintln!("Error accepting connection: {}", err);
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        continue;
                    }
                },
                _ = &mut shutdown => break,
            };

            let permit = match try_admit_connection(admission.as_ref()) {
                Ok(permit) => permit,
                Err(()) => {
                    drop(stream);
                    continue;
                }
            };
            let acceptor = acceptor.clone();
            let app = Arc::clone(&app);
            let builder = Arc::clone(&builder);
            let websocket_builder = Arc::clone(&websocket_builder);
            let http2_builder = Arc::clone(&http2_builder);
            let watcher = graceful.watcher();
            let stream = AdmittedIo::new(stream, permit);

            // The TLS handshake runs inside the task so a slow or failing
            // handshake never blocks the accept loop.
            connection_tasks.spawn(async move {
                match tokio::time::timeout(handshake_timeout, acceptor.accept(stream)).await {
                    Ok(Ok(tls_stream)) => {
                        let expected_protocol = match expected_protocol_from_alpn(
                            tls_stream.get_ref().1.alpn_protocol(),
                        ) {
                            Ok(protocol) => protocol,
                            Err(error) => {
                                eprintln!("Error negociando el protocolo TLS: {error}");
                                return;
                            }
                        };
                        let tls_stream = match preflight_connection(
                            tls_stream,
                            max_head_bytes,
                            header_read_timeout,
                            expected_protocol,
                        )
                        .await
                        {
                            Ok(Some(stream)) => stream,
                            Ok(None) => return,
                            Err(error) => {
                                if error.kind() != io::ErrorKind::TimedOut {
                                    eprintln!("Error inspeccionando la conexion TLS: {error}");
                                }
                                return;
                            }
                        };
                        let protocol = tls_stream.protocol();
                        let websocket_upgrade = tls_stream.is_websocket_upgrade();
                        let io = TokioIo::new(tls_stream);
                        let result = match protocol {
                            DetectedProtocol::Http1 => {
                                let builder = if websocket_upgrade {
                                    websocket_builder
                                } else {
                                    builder
                                };
                                let connection = builder
                                    .serve_connection_with_upgrades(
                                        io,
                                        service_fn(move |req: hyper::Request<Incoming>| {
                                            let app = Arc::clone(&app);
                                            async move {
                                                let mut response = app
                                                    .handle(req, Some(peer), TransportSecurity::Tls)
                                                    .await;
                                                if websocket_upgrade
                                                    && response.status()
                                                        != hyper::StatusCode::SWITCHING_PROTOCOLS
                                                {
                                                    response.headers_mut().insert(
                                                        hyper::header::CONNECTION,
                                                        hyper::header::HeaderValue::from_static(
                                                            "close",
                                                        ),
                                                    );
                                                }
                                                Ok::<_, Infallible>(response)
                                            }
                                        }),
                                    )
                                    .into_owned();
                                watcher.watch(connection).await
                            }
                            DetectedProtocol::Http2 => {
                                let connection = http2_builder.serve_connection(
                                    io,
                                    service_fn(move |req: hyper::Request<Incoming>| {
                                        let app = Arc::clone(&app);
                                        async move {
                                            Ok::<_, Infallible>(
                                                app.handle(req, Some(peer), TransportSecurity::Tls)
                                                    .await,
                                            )
                                        }
                                    }),
                                );
                                watcher.watch(connection).await
                            }
                        };
                        if let Err(err) = result {
                            eprintln!("Error serving TLS connection: {:?}", err);
                        }
                    }
                    Ok(Err(err)) => eprintln!("TLS handshake failed: {}", err),
                    Err(_) => eprintln!("TLS handshake timed out"),
                }
            });
        }

        // Stop accepting new connections, then drain the in-flight ones.
        drop(listener);
        let runtime = app.websocket_runtime();
        let drained =
            drain_server_connections(&runtime, graceful.shutdown(), graceful_timeout).await;
        finish_connection_tasks(&mut connection_tasks, !drained).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_path(name: &str) -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join(name)
    }

    #[test]
    fn maintained_pem_loader_accepts_the_test_chain_and_preserves_io_errors() {
        let config = config_from_pem(
            fixture_path("localhost-cert.pem"),
            fixture_path("localhost-key.pem"),
        )
        .expect("the committed TLS fixture must remain loadable");
        assert_eq!(
            config.alpn_protocols,
            [b"h2".to_vec(), b"http/1.1".to_vec()]
        );

        let error = config_from_pem(
            fixture_path("missing-cert.pem"),
            fixture_path("localhost-key.pem"),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn alpn_selects_one_strict_http_protocol() {
        assert_eq!(
            expected_protocol_from_alpn(Some(b"h2")).unwrap(),
            ExpectedProtocol::Http2
        );
        assert_eq!(
            expected_protocol_from_alpn(Some(b"http/1.1")).unwrap(),
            ExpectedProtocol::Http1
        );
        assert_eq!(
            expected_protocol_from_alpn(None).unwrap(),
            ExpectedProtocol::Http1
        );

        let error = expected_protocol_from_alpn(Some(b"http/3")).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}
