//! HTTPS serving via rustls (cargo feature `tls`): `App::listen_tls` /
//! `serve_tls`, plus a PEM certificate/key loader.

use std::convert::Infallible;
use std::fs::File;
use std::future::Future;
use std::io::{self, BufReader};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio_rustls::TlsAcceptor;

pub use tokio_rustls::rustls::ServerConfig;

use super::App;
use super::server::{TransportSecurity, drain_server_connections};

/// Builds a rustls [`ServerConfig`] from PEM certificate-chain and private-key
/// files, with ALPN advertising HTTP/2 and HTTP/1.1.
pub fn config_from_pem(
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> io::Result<ServerConfig> {
    let certs = rustls_pemfile::certs(&mut BufReader::new(File::open(cert_path)?))
        .collect::<Result<Vec<_>, _>>()?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(File::open(key_path)?))?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no private key found in PEM"))?;

    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(config)
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
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let app = Arc::new(self);
        let builder = Arc::new(auto::Builder::new(TokioExecutor::new()));
        let graceful = GracefulShutdown::new();
        let mut shutdown = std::pin::pin!(shutdown);

        loop {
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

            let acceptor = acceptor.clone();
            let app = Arc::clone(&app);
            let builder = Arc::clone(&builder);
            let watcher = graceful.watcher();

            // The TLS handshake runs inside the task so a slow or failing
            // handshake never blocks the accept loop.
            tokio::spawn(async move {
                match acceptor.accept(stream).await {
                    Ok(tls_stream) => {
                        let io = TokioIo::new(tls_stream);
                        let connection = builder
                            .serve_connection_with_upgrades(
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
                            )
                            .into_owned();
                        if let Err(err) = watcher.watch(connection).await {
                            eprintln!("Error serving TLS connection: {:?}", err);
                        }
                    }
                    Err(err) => eprintln!("TLS handshake failed: {}", err),
                }
            });
        }

        // Stop accepting new connections, then drain the in-flight ones.
        drop(listener);
        drain_server_connections(&app.websocket_runtime(), graceful.shutdown()).await;
        Ok(())
    }
}
