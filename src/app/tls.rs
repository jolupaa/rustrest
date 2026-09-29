//! HTTPS serving via rustls (cargo feature `tls`): `App::listen_tls` /
//! `serve_tls`, plus a PEM certificate/key loader.

use std::future::Future;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use hyper_util::server::graceful::GracefulShutdown;
use rustls_pki_types::pem::{Error as PemError, PemObject};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio_rustls::TlsAcceptor;

pub use tokio_rustls::rustls::ServerConfig;

use super::App;
use super::server::{
    AdmittedIo, ExpectedProtocol, TransportSecurity, drain_server_connections,
    finish_connection_tasks, preflight_connection, reap_connection_tasks,
    serve_detected_connection, try_admit_connection,
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
        let builders = Arc::new(self.connection_builders());
        let admission = self.connection_admission();
        let handshake_timeout = self.config.tls_handshake_timeout;
        let header_read_timeout = self.config.header_read_timeout;
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
                        super::log::log_error!("Error aceptando una conexion: {err}");
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        continue;
                    }
                },
                _ = &mut shutdown => break,
            };

            // Responses are written as soon as they are ready; Nagle's
            // algorithm would hold small writes back until the peer's delayed
            // ACK (tens of milliseconds, most visible on HTTP/2).
            let _ = stream.set_nodelay(true);
            let permit = match try_admit_connection(admission.as_ref()) {
                Ok(permit) => permit,
                Err(()) => {
                    drop(stream);
                    continue;
                }
            };
            let acceptor = acceptor.clone();
            let app = Arc::clone(&app);
            let builders = Arc::clone(&builders);
            let watcher = graceful.watcher();
            let stream = AdmittedIo::new(stream, permit);

            // The TLS handshake runs inside the task so a slow or failing
            // handshake never blocks the accept loop.
            connection_tasks.spawn(async move {
                let tls_stream =
                    match tokio::time::timeout(handshake_timeout, acceptor.accept(stream)).await {
                        Ok(Ok(tls_stream)) => tls_stream,
                        Ok(Err(err)) => {
                            super::log::log_debug!("Fallo el handshake TLS: {err}");
                            return;
                        }
                        Err(_) => {
                            super::log::log_debug!("Se agoto el tiempo del handshake TLS");
                            return;
                        }
                    };
                let expected_protocol =
                    match expected_protocol_from_alpn(tls_stream.get_ref().1.alpn_protocol()) {
                        Ok(protocol) => protocol,
                        Err(error) => {
                            super::log::log_debug!("Error negociando el protocolo TLS: {error}");
                            return;
                        }
                    };
                let tls_stream =
                    match preflight_connection(tls_stream, header_read_timeout, expected_protocol)
                        .await
                    {
                        Ok(stream) => stream,
                        Err(error) => {
                            super::log::log_debug!("Error inspeccionando la conexion TLS: {error}");
                            return;
                        }
                    };
                serve_detected_connection(
                    app,
                    tls_stream,
                    peer,
                    TransportSecurity::Tls,
                    builders,
                    watcher,
                )
                .await;
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
