//! Wire-level tests for HTTP/1.1 persistent connections: reuse, pipelining,
//! per-request framing validation, and connection teardown rules
//! (RFC 9112 §6.1, §9.3).

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use rustrest::{App, Next, Request, Response};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

type Server = JoinHandle<io::Result<()>>;

async fn spawn_app(app: App) -> (SocketAddr, oneshot::Sender<()>, Server) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(app.serve_with_shutdown(listener, async move {
        let _ = shutdown_rx.await;
    }));
    (addr, shutdown_tx, server)
}

async fn shutdown_server(shutdown: oneshot::Sender<()>, server: Server) {
    let _ = shutdown.send(());
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server should shut down")
        .expect("server task should not panic")
        .expect("server should shut down cleanly");
}

/// A response read from a persistent connection. Only `Content-Length`
/// framed responses are supported, which is all these tests produce.
#[derive(Debug)]
struct WireResponse {
    head: String,
    body: String,
}

impl WireResponse {
    fn status(&self) -> u16 {
        self.head
            .split(' ')
            .nth(1)
            .and_then(|status| status.parse().ok())
            .expect("status line")
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.head.lines().skip(1).find_map(|line| {
            let (field, value) = line.split_once(':')?;
            field
                .trim()
                .eq_ignore_ascii_case(name)
                .then_some(value.trim())
        })
    }

    fn closes_connection(&self) -> bool {
        self.header("connection")
            .is_some_and(|value| value.eq_ignore_ascii_case("close"))
    }
}

struct Connection {
    stream: TcpStream,
    buffer: Vec<u8>,
}

impl Connection {
    async fn open(addr: SocketAddr) -> Self {
        Self {
            stream: TcpStream::connect(addr).await.unwrap(),
            buffer: Vec::new(),
        }
    }

    async fn send(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.unwrap();
    }

    async fn fill(&mut self) -> io::Result<usize> {
        let mut chunk = [0_u8; 4096];
        let read = tokio::time::timeout(Duration::from_secs(2), self.stream.read(&mut chunk))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "timed out reading"))??;
        self.buffer.extend_from_slice(&chunk[..read]);
        Ok(read)
    }

    async fn response(&mut self) -> WireResponse {
        let head_end = loop {
            if let Some(end) = self.buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                break end + 4;
            }
            let read = self.fill().await.expect("response head");
            assert!(
                read > 0,
                "connection closed before a complete response head"
            );
        };
        let head = String::from_utf8(self.buffer[..head_end].to_vec()).unwrap();
        let length = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.trim()
                    .eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        while self.buffer.len() < head_end + length {
            let read = self.fill().await.expect("response body");
            assert!(read > 0, "connection closed before the response body ended");
        }
        let body = String::from_utf8(self.buffer[head_end..head_end + length].to_vec()).unwrap();
        self.buffer.drain(..head_end + length);
        WireResponse { head, body }
    }

    /// Asserts that the server closes the connection without sending more
    /// response bytes.
    async fn expect_closed(&mut self) {
        assert!(
            self.buffer.is_empty(),
            "unexpected bytes: {:?}",
            String::from_utf8_lossy(&self.buffer)
        );
        match self.fill().await {
            Ok(0) => {}
            Ok(_) => panic!(
                "unexpected bytes after the final response: {:?}",
                String::from_utf8_lossy(&self.buffer)
            ),
            Err(error) if error.kind() == io::ErrorKind::ConnectionReset => {}
            Err(error) => panic!("connection was not closed: {error}"),
        }
    }
}

fn counting_app(runs: &Arc<AtomicUsize>) -> App {
    let mut app = App::new();
    app.get("/a", |_req: Request| Response::send("uno"))
        .unwrap();
    app.get("/b", |_req: Request| Response::send("dos"))
        .unwrap();
    app.get("/port", |req: Request| {
        Response::send(
            &req.remote_addr()
                .map(|addr| addr.port())
                .unwrap_or(0)
                .to_string(),
        )
    })
    .unwrap();
    let counter = Arc::clone(runs);
    app.post("/echo", move |mut req: Request| {
        counter.fetch_add(1, Ordering::SeqCst);
        async move { req.text().await.map(|body| Response::send(&body)) }
    })
    .unwrap();
    app
}

#[tokio::test]
async fn http11_connections_are_reused_for_sequential_requests() {
    let runs = Arc::new(AtomicUsize::new(0));
    let (addr, shutdown, server) = spawn_app(counting_app(&runs)).await;
    let mut connection = Connection::open(addr).await;

    connection
        .send(b"GET /port HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await;
    let first = connection.response().await;
    assert_eq!(first.status(), 200, "{first:?}");
    assert!(!first.closes_connection(), "{first:?}");

    connection
        .send(b"GET /port HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await;
    let second = connection.response().await;
    assert_eq!(second.status(), 200, "{second:?}");
    assert_eq!(first.body, second.body, "both requests share one socket");

    connection
        .send(b"GET /a HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await;
    let last = connection.response().await;
    assert_eq!(last.body, "uno");
    assert!(last.closes_connection(), "{last:?}");
    connection.expect_closed().await;

    shutdown_server(shutdown, server).await;
}

#[tokio::test]
async fn http11_pipelined_requests_are_answered_in_order() {
    let runs = Arc::new(AtomicUsize::new(0));
    let (addr, shutdown, server) = spawn_app(counting_app(&runs)).await;
    let mut connection = Connection::open(addr).await;

    // RFC 9112 §2.2: a server SHOULD ignore empty lines before a request line.
    connection
        .send(
            b"GET /a HTTP/1.1\r\nHost: localhost\r\n\r\n\r\nPOST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\nhelloGET /b HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .await;

    let bodies = [
        connection.response().await.body,
        connection.response().await.body,
        connection.response().await.body,
    ];
    assert_eq!(bodies, ["uno", "hello", "dos"]);
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    shutdown_server(shutdown, server).await;
}

#[tokio::test]
async fn a_request_embedded_in_a_content_length_body_is_never_dispatched() {
    let runs = Arc::new(AtomicUsize::new(0));
    let b_runs = Arc::new(AtomicUsize::new(0));
    let mut app = counting_app(&runs);
    let counter = Arc::clone(&b_runs);
    app.get("/smuggled", move |_req: Request| {
        counter.fetch_add(1, Ordering::SeqCst);
        Response::send("smuggled")
    })
    .unwrap();
    let (addr, shutdown, server) = spawn_app(app).await;
    let mut connection = Connection::open(addr).await;

    let embedded = "GET /smuggled HTTP/1.1\r\nHost: localhost\r\n\r\n";
    let request = format!(
        "POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{embedded}GET /b HTTP/1.1\r\nHost: localhost\r\n\r\n",
        embedded.len()
    );
    connection.send(request.as_bytes()).await;

    assert_eq!(connection.response().await.body, embedded);
    assert_eq!(connection.response().await.body, "dos");
    assert_eq!(b_runs.load(Ordering::SeqCst), 0);

    shutdown_server(shutdown, server).await;
}

async fn assert_ambiguous_framing_rejected_after_a_reused_request(framing: &str) {
    let runs = Arc::new(AtomicUsize::new(0));
    let middleware_runs = Arc::new(AtomicUsize::new(0));
    let mut app = counting_app(&runs);
    app.error_handler(|error| {
        Response::send(error.code())
            .status(error.status().as_u16())
            .header("x-rustrest-error", "true")
    });
    let counter = Arc::clone(&middleware_runs);
    app.layer(move |req: Request, next: Next| {
        counter.fetch_add(1, Ordering::SeqCst);
        async move { next(req).await }
    });
    let (addr, shutdown, server) = spawn_app(app).await;
    let mut connection = Connection::open(addr).await;

    connection
        .send(b"GET /a HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await;
    let first = connection.response().await;
    assert_eq!(first.body, "uno");
    assert!(!first.closes_connection(), "{first:?}");

    let request = format!(
        "POST /echo HTTP/1.1\r\nHost: localhost\r\n{framing}\r\n0\r\n\r\nGET /b HTTP/1.1\r\nHost: localhost\r\n\r\n"
    );
    connection.send(request.as_bytes()).await;
    let rejected = connection.response().await;
    assert_eq!(rejected.status(), 400, "{rejected:?}");
    assert!(
        rejected
            .body
            .contains(r#""code":"ambiguous_message_framing""#),
        "{rejected:?}"
    );
    assert!(
        rejected.header("x-rustrest-error").is_none(),
        "{rejected:?}"
    );
    assert!(rejected.closes_connection(), "{rejected:?}");
    connection.expect_closed().await;

    assert_eq!(runs.load(Ordering::SeqCst), 0, "handler must not run");
    assert_eq!(
        middleware_runs.load(Ordering::SeqCst),
        1,
        "only the first request enters middleware"
    );
    shutdown_server(shutdown, server).await;
}

#[tokio::test]
async fn ambiguous_framing_is_rejected_on_every_request_of_a_persistent_connection() {
    // Hyper drops a Content-Length that follows Transfer-Encoding before the
    // service sees the head, so both orders must be caught on the raw bytes.
    assert_ambiguous_framing_rejected_after_a_reused_request(
        "Transfer-Encoding: chunked\r\nContent-Length: 5\r\n",
    )
    .await;
    assert_ambiguous_framing_rejected_after_a_reused_request(
        "Content-Length: 5\r\nTransfer-Encoding: chunked\r\n",
    )
    .await;
}

#[tokio::test]
async fn a_chunked_request_is_served_and_then_the_connection_closes() {
    let runs = Arc::new(AtomicUsize::new(0));
    let b_runs = Arc::new(AtomicUsize::new(0));
    let mut app = counting_app(&runs);
    let counter = Arc::clone(&b_runs);
    app.get("/after", move |_req: Request| {
        counter.fetch_add(1, Ordering::SeqCst);
        Response::send("after")
    })
    .unwrap();
    let (addr, shutdown, server) = spawn_app(app).await;
    let mut connection = Connection::open(addr).await;

    connection
        .send(
            b"POST /echo HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\nGET /after HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .await;
    let response = connection.response().await;
    assert_eq!(response.status(), 200, "{response:?}");
    assert_eq!(response.body, "hello");
    assert!(response.closes_connection(), "{response:?}");
    connection.expect_closed().await;
    assert_eq!(b_runs.load(Ordering::SeqCst), 0);

    shutdown_server(shutdown, server).await;
}

#[tokio::test]
async fn an_idle_persistent_connection_is_closed_by_the_header_read_timeout() {
    let runs = Arc::new(AtomicUsize::new(0));
    let mut app = counting_app(&runs);
    app.header_read_timeout(Duration::from_millis(200));
    let (addr, shutdown, server) = spawn_app(app).await;
    let mut connection = Connection::open(addr).await;

    connection
        .send(b"GET /a HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await;
    assert_eq!(connection.response().await.body, "uno");

    let idle_since = Instant::now();
    connection.expect_closed().await;
    assert!(
        idle_since.elapsed() < Duration::from_millis(1500),
        "idle connection outlived the header read timeout: {:?}",
        idle_since.elapsed()
    );

    shutdown_server(shutdown, server).await;
}

#[tokio::test]
async fn graceful_shutdown_closes_idle_persistent_connections_promptly() {
    let runs = Arc::new(AtomicUsize::new(0));
    let mut app = counting_app(&runs);
    app.graceful_shutdown_timeout(Duration::from_secs(5));
    let (addr, shutdown, server) = spawn_app(app).await;
    let mut connection = Connection::open(addr).await;

    connection
        .send(b"GET /a HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await;
    assert_eq!(connection.response().await.body, "uno");

    let started = Instant::now();
    shutdown_server(shutdown, server).await;
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "idle keep-alive connection delayed shutdown by {:?}",
        started.elapsed()
    );
    connection.expect_closed().await;
}

#[tokio::test]
async fn an_unsuccessful_upgrade_still_closes_the_connection() {
    let runs = Arc::new(AtomicUsize::new(0));
    let (addr, shutdown, server) = spawn_app(counting_app(&runs)).await;
    let mut connection = Connection::open(addr).await;

    connection
        .send(
            b"GET /a HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\nGET /b HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .await;
    let response = connection.response().await;
    assert_eq!(response.body, "uno");
    assert!(response.closes_connection(), "{response:?}");
    connection.expect_closed().await;

    shutdown_server(shutdown, server).await;
}

/// Sends `first`, then a pipelined HTTP/1.1 request with a hidden
/// `Content-Length`, and returns the raw output plus how often the smuggled
/// handler ran.
async fn smuggle_after(first: &[u8]) -> (String, usize) {
    let runs = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    app.get("/plain", |_req: Request| Response::send("primera"))
        .unwrap();
    let counter = Arc::clone(&runs);
    app.post("/second", move |_req: Request| {
        counter.fetch_add(1, Ordering::SeqCst);
        Response::send("segunda")
    })
    .unwrap();
    let (addr, shutdown, server) = spawn_app(app).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut bytes = first.to_vec();
    bytes.extend_from_slice(
        b"POST /second HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nContent-Length: 40\r\n\r\n0\r\n\r\n",
    );
    stream.write_all(&bytes).await.unwrap();
    let mut output = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut output)).await;
    shutdown_server(shutdown, server).await;
    (
        String::from_utf8_lossy(&output).into_owned(),
        runs.load(Ordering::SeqCst),
    )
}

// Hyper rewrites `Connection: close` to `keep-alive` on responses to an
// HTTP/1.0 keep-alive request unless the response itself is HTTP/1.0, so a
// forced close must also downgrade the response version.
#[tokio::test]
async fn http10_terminal_requests_cannot_keep_the_connection_alive() {
    for first in [
        &b"GET /plain HTTP/1.0\r\nConnection: Upgrade, keep-alive\r\nUpgrade: websocket\r\n\r\n"[..],
        b"CONNECT localhost:443 HTTP/1.0\r\nConnection: keep-alive\r\n\r\n",
        b"GET /plain HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n",
    ] {
        let (output, runs) = smuggle_after(first).await;
        assert_eq!(runs, 0, "smuggled request dispatched:\n{output}");
        assert_eq!(output.matches("HTTP/1.").count(), 1, "{output}");
        assert!(!output.to_ascii_lowercase().contains("keep-alive"), "{output}");
    }
}

#[tokio::test]
async fn http10_keep_alive_requests_are_still_inspected() {
    let (output, runs) =
        smuggle_after(b"GET /plain HTTP/1.0\r\nConnection: keep-alive\r\n\r\n").await;
    assert_eq!(runs, 0, "{output}");
    assert!(output.contains("primera"), "{output}");
    assert!(output.contains("ambiguous_message_framing"), "{output}");
}

struct DropFlag(Arc<std::sync::atomic::AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn graceful_shutdown_drains_an_in_flight_request() {
    let started = Arc::new(tokio::sync::Notify::new());
    let mut app = App::new();
    let notify = Arc::clone(&started);
    app.get("/slow", move |_req: Request| {
        let notify = Arc::clone(&notify);
        async move {
            notify.notify_one();
            tokio::time::sleep(Duration::from_millis(300)).await;
            Response::send("terminado")
        }
    })
    .unwrap();
    let (addr, shutdown, server) = spawn_app(app).await;
    let mut connection = Connection::open(addr).await;
    connection
        .send(b"GET /slow HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await;
    started.notified().await;

    let _ = shutdown.send(());
    let response = connection.response().await;
    assert_eq!(response.status(), 200, "{response:?}");
    assert_eq!(response.body, "terminado");
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server should finish draining")
        .unwrap()
        .unwrap();
    assert!(
        TcpStream::connect(addr).await.is_err(),
        "the listener must be closed after shutdown"
    );
}

#[tokio::test]
async fn a_client_disconnect_cancels_the_running_handler() {
    let started = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut app = App::new();
    let notify = Arc::clone(&started);
    let flag = Arc::clone(&dropped);
    app.get("/hang", move |_req: Request| {
        let notify = Arc::clone(&notify);
        let guard = DropFlag(Arc::clone(&flag));
        async move {
            let _guard = guard;
            notify.notify_one();
            tokio::time::sleep(Duration::from_secs(30)).await;
            Response::send("nunca")
        }
    })
    .unwrap();
    let (addr, shutdown, server) = spawn_app(app).await;
    let mut connection = Connection::open(addr).await;
    connection
        .send(b"GET /hang HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await;
    started.notified().await;
    drop(connection);

    let deadline = Instant::now() + Duration::from_secs(2);
    while !dropped.load(Ordering::SeqCst) {
        assert!(
            Instant::now() < deadline,
            "handler kept running after disconnect"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    shutdown_server(shutdown, server).await;
}
