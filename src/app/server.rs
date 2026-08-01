use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::FutureExt;
use hyper::body::Incoming;
use hyper::header::{
    CONNECTION, CONTENT_LENGTH, COOKIE, HOST, HeaderValue, SEC_WEBSOCKET_ACCEPT, SEC_WEBSOCKET_KEY,
    SEC_WEBSOCKET_VERSION, TRANSFER_ENCODING, UPGRADE,
};
use hyper::service::service_fn;
use hyper::{HeaderMap, Method, StatusCode, Uri, Version};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinError, JoinSet};

use super::router::{MatchedRoute, RouteKind};
use super::websocket::{header_value_contains_token, is_valid_websocket_key};
use super::{
    ErrorHandler, HttpError, IntoHandler, IntoMiddleware, Middleware, Next, Request, RequestBody,
    Response, RouteError, RouteHandle, Router, StateStore, WebSocketConfig, WebSocketObserver,
    WebSocketRuntimeHandle, WsHub,
};
use super::{
    Handler, ResponseBody, allow_header_value, method_not_allowed_handler, not_found_handler,
    options_handler, panic_response, parse_cookies, parse_query,
};

const DEFAULT_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(feature = "tls")]
const DEFAULT_TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_MAX_CONNECTIONS: usize = 10_000;
const DEFAULT_MAX_REQUEST_TARGET_BYTES: usize = 8 * 1024;
const DEFAULT_MAX_QUERY_BYTES: usize = 8 * 1024;
const DEFAULT_MAX_REQUEST_HEADER_COUNT: usize = 100;
const DEFAULT_MAX_REQUEST_HEADER_BYTES: usize = 32 * 1024;
const MAX_REQUEST_HEADER_COUNT: usize = 1_024;
const DEFAULT_MAX_HTTP2_CONCURRENT_STREAMS: u32 = 100;
const DEFAULT_MAX_HTTP2_SEND_BUFFER_BYTES: usize = 64 * 1024;
const DEFAULT_HTTP2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_HTTP2_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(10);
const MIN_HTTP1_BUFFER_BYTES: usize = 8 * 1024;
const HTTP1_HEAD_FIXED_OVERHEAD_BYTES: usize = 1024;
const HTTP1_HEADER_FRAMING_OVERHEAD_BYTES: usize = 4;
const HTTP2_PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const HTTP2_FRAME_HEADER_BYTES: usize = 9;
const HTTP2_SETTINGS_FRAME: u8 = 0x4;
const HTTP2_SETTINGS_ACK: u8 = 0x1;
const HTTP2_MAX_INITIAL_SETTINGS_BYTES: usize = 16 * 1024;

pub(crate) async fn drain_server_connections(
    runtime: &WebSocketRuntimeHandle,
    http_shutdown: impl Future<Output = ()>,
    timeout: Duration,
) -> bool {
    runtime.begin_shutdown().await;
    let websocket_grace = runtime.shutdown_grace_period();
    let websocket_shutdown = async {
        if runtime.drain(websocket_grace).await.is_err() {
            runtime.abort_remaining();
            runtime.wait_until_empty().await;
        }
    };

    tokio::select! {
        _ = async { tokio::join!(http_shutdown, websocket_shutdown); } => true,
        _ = tokio::time::sleep(timeout) => {
            eprintln!("Timed out waiting for in-flight connections to drain");
            runtime.abort_remaining();
            false
        }
    }
}

/// Keeps a connection-admission permit attached to the transport itself. This
/// way an HTTP/1 upgrade (for example WebSocket) continues counting against the
/// global connection limit after Hyper hands the IO to the upgrade consumer.
pub(crate) struct AdmittedIo<T> {
    inner: T,
    _permit: Option<OwnedSemaphorePermit>,
}

impl<T> AdmittedIo<T> {
    pub(crate) fn new(inner: T, permit: Option<OwnedSemaphorePermit>) -> Self {
        Self {
            inner,
            _permit: permit,
        }
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for AdmittedIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buffer)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for AdmittedIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, buffers)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExpectedProtocol {
    Auto,
    Http1,
    Http2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DetectedProtocol {
    Http1,
    Http2,
}

/// Replays bytes consumed by the request-head preflight before delegating all
/// subsequent I/O to Hyper.
pub(crate) struct PrefixedIo<T> {
    inner: T,
    prefix: Vec<u8>,
    position: usize,
    protocol: DetectedProtocol,
    websocket_upgrade: bool,
}

impl<T> PrefixedIo<T> {
    fn new(inner: T, prefix: Vec<u8>, protocol: DetectedProtocol, websocket_upgrade: bool) -> Self {
        Self {
            inner,
            prefix,
            position: 0,
            protocol,
            websocket_upgrade,
        }
    }

    pub(crate) fn protocol(&self) -> DetectedProtocol {
        self.protocol
    }

    pub(crate) fn is_websocket_upgrade(&self) -> bool {
        self.websocket_upgrade
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for PrefixedIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.position < self.prefix.len() {
            let remaining = &self.prefix[self.position..];
            let length = remaining.len().min(buffer.remaining());
            buffer.put_slice(&remaining[..length]);
            self.position += length;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buffer)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for PrefixedIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, buffers)
    }
}

/// Inspects the first connection preface under one deadline. HTTP/1 requests
/// are checked for ambiguous TE+CL framing before Hyper normalizes the fields;
/// HTTP/2 clients must deliver the complete connection preface and initial
/// SETTINGS frame before the same deadline expires.
pub(crate) async fn preflight_connection<T>(
    io: T,
    max_head_bytes: usize,
    timeout: Option<Duration>,
    expected_protocol: ExpectedProtocol,
) -> io::Result<Option<PrefixedIo<T>>>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let inspect = inspect_connection(io, max_head_bytes, expected_protocol);
    match timeout {
        Some(timeout) => tokio::time::timeout(timeout, inspect).await.map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "se agoto el tiempo al leer la cabecera o el prefacio HTTP/2",
            )
        })?,
        None => inspect.await,
    }
}

async fn inspect_connection<T>(
    mut io: T,
    max_head_bytes: usize,
    expected_protocol: ExpectedProtocol,
) -> io::Result<Option<PrefixedIo<T>>>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let max_head_bytes = max_head_bytes.max(MIN_HTTP1_BUFFER_BYTES);
    let mut prefix = Vec::with_capacity(max_head_bytes.min(MIN_HTTP1_BUFFER_BYTES));
    let mut chunk = [0_u8; 4096];

    loop {
        let preface_state = inspect_http2_client_preface(&prefix)?;
        match preface_state {
            Http2PrefaceState::Complete => {
                if expected_protocol == ExpectedProtocol::Http1 {
                    return Err(protocol_mismatch("HTTP/2", "HTTP/1.1"));
                }
                return Ok(Some(PrefixedIo::new(
                    io,
                    prefix,
                    DetectedProtocol::Http2,
                    false,
                )));
            }
            Http2PrefaceState::Pending => {
                let max_preface_bytes = HTTP2_PREFACE.len()
                    + HTTP2_FRAME_HEADER_BYTES
                    + HTTP2_MAX_INITIAL_SETTINGS_BYTES;
                if prefix.len() >= max_preface_bytes {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "el frame SETTINGS inicial de HTTP/2 es demasiado grande",
                    ));
                }
            }
            Http2PrefaceState::NotHttp2 => {
                if expected_protocol == ExpectedProtocol::Http2 {
                    return Err(protocol_mismatch("HTTP/1.1", "HTTP/2"));
                }

                if let Some((start, end)) = find_http1_head_bounds(&prefix) {
                    let head = &prefix[start..end];
                    if has_ambiguous_http1_framing(head) {
                        write_ambiguous_framing_response(&mut io).await?;
                        return Ok(None);
                    }
                    let websocket_upgrade = raw_head_is_websocket_upgrade(head);
                    return Ok(Some(PrefixedIo::new(
                        io,
                        prefix,
                        DetectedProtocol::Http1,
                        websocket_upgrade,
                    )));
                }

                if prefix.len() >= max_head_bytes {
                    // Hyper owns the authoritative syntax/header-size error.
                    // Replaying the capped prefix lets its parser generate 431.
                    return Ok(Some(PrefixedIo::new(
                        io,
                        prefix,
                        DetectedProtocol::Http1,
                        false,
                    )));
                }
            }
        }

        let active_limit = if matches!(preface_state, Http2PrefaceState::Pending) {
            HTTP2_PREFACE.len() + HTTP2_FRAME_HEADER_BYTES + HTTP2_MAX_INITIAL_SETTINGS_BYTES
        } else {
            max_head_bytes
        };
        let remaining = active_limit.saturating_sub(prefix.len());
        let chunk_length = remaining.min(chunk.len());
        if chunk_length == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "el prefacio de la conexion supera el limite configurado",
            ));
        }
        let read = io.read(&mut chunk[..chunk_length]).await?;
        if read == 0 {
            if expected_protocol == ExpectedProtocol::Http2
                || (!prefix.is_empty() && HTTP2_PREFACE.starts_with(&prefix))
            {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "el prefacio HTTP/2 del cliente esta incompleto",
                ));
            }
            return Ok(Some(PrefixedIo::new(
                io,
                prefix,
                DetectedProtocol::Http1,
                false,
            )));
        }
        prefix.extend_from_slice(&chunk[..read]);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Http2PrefaceState {
    Pending,
    Complete,
    NotHttp2,
}

fn inspect_http2_client_preface(bytes: &[u8]) -> io::Result<Http2PrefaceState> {
    if bytes.len() < HTTP2_PREFACE.len() {
        return Ok(if HTTP2_PREFACE.starts_with(bytes) {
            Http2PrefaceState::Pending
        } else {
            Http2PrefaceState::NotHttp2
        });
    }
    if !bytes.starts_with(HTTP2_PREFACE) {
        return Ok(Http2PrefaceState::NotHttp2);
    }

    let frame_start = HTTP2_PREFACE.len();
    let Some(frame_header) = bytes.get(frame_start..frame_start + HTTP2_FRAME_HEADER_BYTES) else {
        return Ok(Http2PrefaceState::Pending);
    };
    let payload_length = (usize::from(frame_header[0]) << 16)
        | (usize::from(frame_header[1]) << 8)
        | usize::from(frame_header[2]);
    let frame_type = frame_header[3];
    let flags = frame_header[4];
    let stream_id = u32::from_be_bytes(frame_header[5..9].try_into().expect("four-byte stream ID"))
        & 0x7fff_ffff;

    if frame_type != HTTP2_SETTINGS_FRAME
        || flags & HTTP2_SETTINGS_ACK != 0
        || stream_id != 0
        || payload_length % 6 != 0
        || payload_length > HTTP2_MAX_INITIAL_SETTINGS_BYTES
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "el frame SETTINGS inicial de HTTP/2 no es valido",
        ));
    }

    let total = frame_start + HTTP2_FRAME_HEADER_BYTES + payload_length;
    Ok(if bytes.len() >= total {
        Http2PrefaceState::Complete
    } else {
        Http2PrefaceState::Pending
    })
}

fn protocol_mismatch(received: &str, negotiated: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("se recibio {received} despues de negociar {negotiated}"),
    )
}

/// Locates the first HTTP/1 request head using httparse/Hyper's newline
/// policy: leading empty CRLF or LF lines are ignored and either newline form
/// terminates request/header lines. Returning the actual request-line start is
/// important because otherwise leading blank lines can hide framing fields
/// from the raw TE+CL check while Hyper still accepts the request.
fn find_http1_head_bounds(bytes: &[u8]) -> Option<(usize, usize)> {
    let mut start = 0;
    loop {
        match bytes.get(start..) {
            Some(remaining) if remaining.starts_with(b"\r\n") => start += 2,
            Some(remaining) if remaining.starts_with(b"\n") => start += 1,
            _ => break,
        }
    }

    // The first non-empty line is the request line.
    let mut cursor = next_http1_line(bytes, start)?.1;
    loop {
        let (line_start, next) = next_http1_line(bytes, cursor)?;
        let mut line_end = next - 1;
        if line_end > line_start && bytes[line_end - 1] == b'\r' {
            line_end -= 1;
        }
        if line_end == line_start {
            return Some((start, next));
        }
        cursor = next;
    }
}

fn next_http1_line(bytes: &[u8], start: usize) -> Option<(usize, usize)> {
    let relative_end = bytes.get(start..)?.iter().position(|byte| *byte == b'\n')?;
    Some((start, start + relative_end + 1))
}

fn has_ambiguous_http1_framing(head: &[u8]) -> bool {
    let mut lines = head.split(|byte| *byte == b'\n');
    let _request_line = lines.next();
    let mut transfer_encoding = false;
    let mut content_length = false;

    for line in lines {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            break;
        }
        let Some(colon) = line.iter().position(|byte| *byte == b':') else {
            continue;
        };
        let name = &line[..colon];
        transfer_encoding |= name.eq_ignore_ascii_case(TRANSFER_ENCODING.as_str().as_bytes());
        content_length |= name.eq_ignore_ascii_case(CONTENT_LENGTH.as_str().as_bytes());
    }

    transfer_encoding && content_length
}

fn raw_head_is_websocket_upgrade(head: &[u8]) -> bool {
    let mut lines = head.split(|byte| *byte == b'\n');
    let Some(request_line) = lines.next() else {
        return false;
    };
    if !request_line.starts_with(b"GET ") {
        return false;
    }

    let mut connection_upgrade = false;
    let mut websocket_upgrade = false;
    for line in lines {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            break;
        }
        let Some(colon) = line.iter().position(|byte| *byte == b':') else {
            continue;
        };
        let name = &line[..colon];
        let value = trim_ascii_whitespace(&line[colon + 1..]);
        if name.eq_ignore_ascii_case(CONNECTION.as_str().as_bytes()) {
            connection_upgrade |= value
                .split(|byte| *byte == b',')
                .map(trim_ascii_whitespace)
                .any(|token| token.eq_ignore_ascii_case(b"upgrade"));
        } else if name.eq_ignore_ascii_case(UPGRADE.as_str().as_bytes()) {
            websocket_upgrade |= value
                .split(|byte| *byte == b',')
                .map(trim_ascii_whitespace)
                .any(|token| token.eq_ignore_ascii_case(b"websocket"));
        }
    }
    connection_upgrade && websocket_upgrade
}

fn trim_ascii_whitespace(mut value: &[u8]) -> &[u8] {
    while value
        .first()
        .is_some_and(|byte| matches!(byte, b' ' | b'\t'))
    {
        value = &value[1..];
    }
    while value
        .last()
        .is_some_and(|byte| matches!(byte, b' ' | b'\t'))
    {
        value = &value[..value.len() - 1];
    }
    value
}

async fn write_ambiguous_framing_response<T>(io: &mut T) -> io::Result<()>
where
    T: AsyncWrite + Unpin,
{
    const BODY: &str = r#"{"type":"about:blank#ambiguous_message_framing","title":"Bad Request","status":400,"detail":"Transfer-Encoding y Content-Length no pueden combinarse","code":"ambiguous_message_framing"}"#;
    let response = format!(
        "HTTP/1.1 400 Bad Request\r\ncontent-type: application/problem+json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{BODY}",
        BODY.len()
    );
    io.write_all(response.as_bytes()).await?;
    io.flush().await?;
    io.shutdown().await
}

pub(crate) fn try_admit_connection(
    admission: Option<&Arc<Semaphore>>,
) -> Result<Option<OwnedSemaphorePermit>, ()> {
    match admission {
        Some(admission) => Arc::clone(admission)
            .try_acquire_owned()
            .map(Some)
            .map_err(|_| ()),
        None => Ok(None),
    }
}

fn report_connection_task(result: Result<(), JoinError>) {
    if let Err(error) = result {
        if !error.is_cancelled() {
            eprintln!("La tarea de conexion termino de forma inesperada: {error}");
        }
    }
}

pub(crate) fn reap_connection_tasks(tasks: &mut JoinSet<()>) {
    while let Some(result) = tasks.try_join_next() {
        report_connection_task(result);
    }
}

pub(crate) async fn finish_connection_tasks(tasks: &mut JoinSet<()>, abort: bool) {
    if abort {
        tasks.abort_all();
    }
    while let Some(result) = tasks.join_next().await {
        report_connection_task(result);
    }
}

/// How request paths with a trailing slash (`/users/`) are treated relative
/// to the canonical, slash-less route (`/users`). The root path `/` is always
/// canonical.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum TrailingSlash {
    /// Trailing slashes are ignored: `/users/` matches `/users` (default).
    #[default]
    Ignore,
    /// Non-canonical paths do not match anything and fall through to 404.
    Strict,
    /// Non-canonical paths get a `308 Permanent Redirect` to the canonical
    /// path, preserving the query string.
    Redirect,
}

/// Server-wide limits and timeouts, configured via builder methods on [`App`].
#[derive(Clone, Copy)]
pub struct ServerConfig {
    pub(crate) max_body_size: usize,
    pub(crate) request_timeout: Option<Duration>,
    pub(crate) header_read_timeout: Option<Duration>,
    pub(crate) http2_keep_alive_interval: Option<Duration>,
    pub(crate) http2_keep_alive_timeout: Duration,
    #[cfg(feature = "tls")]
    pub(crate) tls_handshake_timeout: Duration,
    pub(crate) graceful_shutdown_timeout: Duration,
    pub(crate) max_connections: Option<usize>,
    pub(crate) max_request_target_bytes: usize,
    pub(crate) max_query_bytes: usize,
    pub(crate) max_request_header_count: usize,
    pub(crate) max_request_header_bytes: usize,
    pub(crate) trailing_slash: TrailingSlash,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_body_size: super::body::DEFAULT_BODY_LIMIT,
            request_timeout: Some(DEFAULT_REQUEST_TIMEOUT),
            header_read_timeout: Some(DEFAULT_HEADER_READ_TIMEOUT),
            http2_keep_alive_interval: Some(DEFAULT_HTTP2_KEEP_ALIVE_INTERVAL),
            http2_keep_alive_timeout: DEFAULT_HTTP2_KEEP_ALIVE_TIMEOUT,
            #[cfg(feature = "tls")]
            tls_handshake_timeout: DEFAULT_TLS_HANDSHAKE_TIMEOUT,
            graceful_shutdown_timeout: DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT,
            max_connections: Some(DEFAULT_MAX_CONNECTIONS),
            max_request_target_bytes: DEFAULT_MAX_REQUEST_TARGET_BYTES,
            max_query_bytes: DEFAULT_MAX_QUERY_BYTES,
            max_request_header_count: DEFAULT_MAX_REQUEST_HEADER_COUNT,
            max_request_header_bytes: DEFAULT_MAX_REQUEST_HEADER_BYTES,
            trailing_slash: TrailingSlash::default(),
        }
    }
}

fn request_target_len(method: &hyper::Method, uri: &Uri) -> usize {
    if method == hyper::Method::CONNECT && uri.scheme().is_none() {
        return uri
            .authority()
            .map_or(0, |authority| authority.as_str().len());
    }

    let path_and_query = uri
        .path_and_query()
        .map_or_else(|| uri.path().len(), |value| value.as_str().len());
    match (uri.scheme(), uri.authority()) {
        (Some(scheme), Some(authority)) => scheme
            .as_str()
            .len()
            .saturating_add(3)
            .saturating_add(authority.as_str().len())
            .saturating_add(path_and_query),
        _ => path_and_query,
    }
}

fn request_head_error(status: StatusCode, code: &'static str, message: &'static str) -> HttpError {
    HttpError::new(status, code, message)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ValidatedAuthority {
    host: String,
    port: Option<u16>,
}

impl ValidatedAuthority {
    /// Parses the HTTP `host[:port]` authority form without relying on
    /// `http::uri::Authority::port_u16()`: `Authority` deliberately accepts
    /// userinfo and opaque/non-numeric port suffixes that are not valid in a
    /// request `Host` or `:authority` value.
    fn parse(raw: &str) -> Option<Self> {
        if raw.is_empty()
            || raw.contains('@')
            || raw.contains('/')
            || raw.contains('\\')
            || raw
                .chars()
                .any(|character| character.is_control() || character.is_whitespace())
            || raw.parse::<hyper::http::uri::Authority>().is_err()
        {
            return None;
        }

        if let Some(bracketed) = raw.strip_prefix('[') {
            let closing = bracketed.find(']')?;
            let address = bracketed[..closing].parse::<std::net::Ipv6Addr>().ok()?;
            let suffix = &bracketed[closing + 1..];
            let port = if suffix.is_empty() {
                None
            } else {
                Some(parse_authority_port(suffix.strip_prefix(':')?)?)
            };
            return Some(Self {
                host: format!("[{address}]"),
                port,
            });
        }

        if raw.contains('[') || raw.contains(']') || raw.matches(':').count() > 1 {
            return None;
        }
        let (host, port) = match raw.split_once(':') {
            Some((host, port)) => (host, Some(parse_authority_port(port)?)),
            None => (raw, None),
        };
        if host.is_empty() {
            return None;
        }

        Some(Self {
            host: host.to_ascii_lowercase(),
            port,
        })
    }

    fn normalized(&self) -> String {
        match self.port {
            Some(port) => format!("{}:{port}", self.host),
            None => self.host.clone(),
        }
    }

    fn equivalent_to(&self, other: &Self, default_port: Option<u16>) -> bool {
        self.host == other.host && self.port.or(default_port) == other.port.or(default_port)
    }
}

fn parse_authority_port(raw: &str) -> Option<u16> {
    if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    raw.parse().ok()
}

fn default_port_for_uri(uri: &Uri) -> Option<u16> {
    match uri.scheme_str() {
        Some(scheme)
            if scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("ws") =>
        {
            Some(80)
        }
        Some(scheme)
            if scheme.eq_ignore_ascii_case("https") || scheme.eq_ignore_ascii_case("wss") =>
        {
            Some(443)
        }
        _ => None,
    }
}

/// Validates transport-level request metadata before building the framework's
/// convenience query/header maps. The returned authority is normalized for
/// routing and is sourced from `Host` or, when absent, HTTP/2 `:authority`.
fn validate_request_head(
    config: &ServerConfig,
    version: Version,
    method: &hyper::Method,
    uri: &Uri,
    headers: &HeaderMap,
) -> Result<Option<String>, HttpError> {
    if request_target_len(method, uri) > config.max_request_target_bytes {
        return Err(request_head_error(
            StatusCode::URI_TOO_LONG,
            "request_target_too_large",
            "El objetivo de la solicitud supera el limite configurado",
        ));
    }
    if uri.query().map_or(0, str::len) > config.max_query_bytes {
        return Err(request_head_error(
            StatusCode::URI_TOO_LONG,
            "query_too_large",
            "La consulta supera el limite configurado",
        ));
    }
    if headers.len() > config.max_request_header_count {
        return Err(request_head_error(
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            "too_many_headers",
            "La solicitud contiene demasiados encabezados",
        ));
    }

    let header_bytes = headers.iter().fold(0_usize, |total, (name, value)| {
        total
            .saturating_add(name.as_str().len())
            .saturating_add(value.as_bytes().len())
    });
    if header_bytes > config.max_request_header_bytes {
        return Err(request_head_error(
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            "request_headers_too_large",
            "Los encabezados de la solicitud superan el limite configurado",
        ));
    }
    if headers.iter().any(|(_, value)| value.to_str().is_err()) {
        return Err(request_head_error(
            StatusCode::BAD_REQUEST,
            "invalid_header_value",
            "La solicitud contiene un valor de encabezado no valido",
        ));
    }
    if headers.contains_key(TRANSFER_ENCODING) && headers.contains_key(CONTENT_LENGTH) {
        return Err(request_head_error(
            StatusCode::BAD_REQUEST,
            "ambiguous_message_framing",
            "Transfer-Encoding y Content-Length no pueden combinarse",
        ));
    }

    let mut host_values = headers.get_all(HOST).iter();
    let host = host_values.next();
    if host_values.next().is_some() {
        return Err(request_head_error(
            StatusCode::BAD_REQUEST,
            "invalid_authority",
            "La solicitud contiene mas de un encabezado Host",
        ));
    }
    let host = match host {
        Some(value) => {
            let value = value.to_str().map_err(|_| {
                request_head_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_authority",
                    "El encabezado Host no es valido",
                )
            })?;
            Some(ValidatedAuthority::parse(value).ok_or_else(|| {
                request_head_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_authority",
                    "El encabezado Host no es valido",
                )
            })?)
        }
        None => None,
    };
    let uri_authority = uri
        .authority()
        .map(|authority| {
            ValidatedAuthority::parse(authority.as_str()).ok_or_else(|| {
                request_head_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_authority",
                    "La autoridad del objetivo de solicitud no es valida",
                )
            })
        })
        .transpose()?;

    if version == Version::HTTP_11 && host.is_none() {
        return Err(request_head_error(
            StatusCode::BAD_REQUEST,
            "invalid_authority",
            "HTTP/1.1 requiere exactamente un encabezado Host",
        ));
    }

    if let (Some(host), Some(uri_authority)) = (&host, &uri_authority) {
        if !host.equivalent_to(uri_authority, default_port_for_uri(uri)) {
            return Err(request_head_error(
                StatusCode::BAD_REQUEST,
                "invalid_authority",
                "Host y :authority no coinciden",
            ));
        }
    }

    let authority = host.as_ref().or(uri_authority.as_ref());
    if authority.is_none()
        && matches!(
            version,
            Version::HTTP_11 | Version::HTTP_2 | Version::HTTP_3
        )
    {
        return Err(request_head_error(
            StatusCode::BAD_REQUEST,
            "invalid_authority",
            "La solicitud no incluye Host o :authority",
        ));
    }

    Ok(authority.map(ValidatedAuthority::normalized))
}

fn is_websocket_upgrade_request(req: &hyper::Request<Incoming>) -> bool {
    req.version() == hyper::Version::HTTP_11
        && req.method().as_str().eq_ignore_ascii_case("GET")
        && req.headers().get_all(UPGRADE).iter().any(|value| {
            value
                .to_str()
                .is_ok_and(|value| header_value_contains_token(value, "websocket"))
        })
        && req.headers().get_all(CONNECTION).iter().any(|value| {
            value
                .to_str()
                .is_ok_and(|value| header_value_contains_token(value, "upgrade"))
        })
        && req
            .headers()
            .get(SEC_WEBSOCKET_KEY)
            .and_then(|value| value.to_str().ok())
            .is_some_and(is_valid_websocket_key)
        && req
            .headers()
            .get(SEC_WEBSOCKET_VERSION)
            .and_then(|value| value.to_str().ok())
            == Some("13")
}

#[derive(Clone, Copy)]
pub(crate) enum TransportSecurity {
    Plain,
    #[cfg(feature = "tls")]
    Tls,
}

impl TransportSecurity {
    fn is_secure(self) -> bool {
        match self {
            Self::Plain => false,
            #[cfg(feature = "tls")]
            Self::Tls => true,
        }
    }
}

pub struct App {
    router: Router,
    middlewares: Vec<Middleware>,
    state: StateStore,
    error_handler: Option<ErrorHandler>,
    pub(crate) config: ServerConfig,
    websocket_runtime: WebSocketRuntimeHandle,
    websocket_hub: WsHub,
    websocket_defaults: WebSocketConfig,
}

impl App {
    pub fn new() -> Self {
        let websocket_hub = WsHub::local();
        let websocket_runtime = websocket_hub.runtime();
        Self {
            router: Router::new(),
            middlewares: Vec::new(),
            state: StateStore::default(),
            error_handler: None,
            config: ServerConfig::default(),
            websocket_runtime,
            websocket_hub,
            websocket_defaults: WebSocketConfig::new(),
        }
    }

    pub fn websocket_runtime(&self) -> WebSocketRuntimeHandle {
        self.websocket_runtime.clone()
    }

    /// Installs the WebSocket hub used by subsequently served requests.
    /// Configure it before passing the app to `serve` or `listen`.
    pub fn websocket_hub(&mut self, hub: WsHub) -> &mut Self {
        self.websocket_runtime = hub.runtime();
        self.websocket_hub = hub;
        self
    }

    /// Returns a cloneable handle to the app's WebSocket hub.
    pub fn websocket_hub_handle(&self) -> WsHub {
        self.websocket_hub.clone()
    }

    pub fn websocket_defaults(&mut self, config: WebSocketConfig) -> &mut Self {
        self.websocket_defaults = config;
        self
    }

    pub fn websocket_observer(&mut self, observer: Arc<dyn WebSocketObserver>) -> &mut Self {
        self.websocket_runtime.set_observer(observer);
        self
    }

    pub(crate) fn validate_websockets(&self) -> io::Result<()> {
        self.router
            .validate_websockets(&self.websocket_defaults)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))
    }

    /// Sets the hard maximum request body size buffered into memory. Route-level
    /// limits may lower, but never raise, this ceiling. Requests whose body
    /// exceeds the effective limit return `413 Payload Too Large`. Defaults to
    /// 64 KB.
    pub fn max_body_size(&mut self, bytes: usize) -> &mut Self {
        self.config.max_body_size = bytes;
        self
    }

    /// Sets a total deadline for request dispatch, including body reads and
    /// handler execution. On timeout the client receives `408 Request Timeout`.
    /// Defaults to 30 seconds.
    pub fn request_timeout(&mut self, timeout: Duration) -> &mut Self {
        assert!(
            !timeout.is_zero(),
            "request timeout must be greater than zero"
        );
        self.config.request_timeout = Some(timeout);
        self
    }

    /// Disables the total request/body/handler deadline. Long-lived response
    /// streams are not constrained by this deadline because it ends once the
    /// handler returns the stream.
    pub fn disable_request_timeout(&mut self) -> &mut Self {
        self.config.request_timeout = None;
        self
    }

    /// Sets how long a connection may take to send its HTTP/1 request headers
    /// or its HTTP/2 client preface (slow-loris protection). Defaults to 10
    /// seconds.
    pub fn header_read_timeout(&mut self, timeout: Duration) -> &mut Self {
        assert!(
            !timeout.is_zero() && std::time::Instant::now().checked_add(timeout).is_some(),
            "header read timeout must be positive and representable"
        );
        self.config.header_read_timeout = Some(timeout);
        self
    }

    /// Disables the HTTP/1 request-header deadline. This is an explicit escape
    /// hatch for trusted environments; production servers should normally
    /// retain a finite timeout.
    pub fn disable_header_read_timeout(&mut self) -> &mut Self {
        self.config.header_read_timeout = None;
        self
    }

    /// Sets how often an idle HTTP/2 connection is probed with PING. Defaults
    /// to 30 seconds.
    pub fn http2_keep_alive_interval(&mut self, interval: Duration) -> &mut Self {
        assert!(
            !interval.is_zero() && std::time::Instant::now().checked_add(interval).is_some(),
            "HTTP/2 keepalive interval must be positive and representable"
        );
        self.config.http2_keep_alive_interval = Some(interval);
        self
    }

    /// Disables HTTP/2 liveness probes. Production servers should normally
    /// retain a finite interval and timeout.
    pub fn disable_http2_keep_alive(&mut self) -> &mut Self {
        self.config.http2_keep_alive_interval = None;
        self
    }

    /// Sets how long an HTTP/2 peer has to acknowledge a keepalive PING.
    /// Defaults to 10 seconds.
    pub fn http2_keep_alive_timeout(&mut self, timeout: Duration) -> &mut Self {
        assert!(
            !timeout.is_zero() && std::time::Instant::now().checked_add(timeout).is_some(),
            "HTTP/2 keepalive timeout must be positive and representable"
        );
        self.config.http2_keep_alive_timeout = timeout;
        self
    }

    /// Sets the maximum duration of a TLS handshake. Defaults to 10 seconds.
    #[cfg(feature = "tls")]
    pub fn tls_handshake_timeout(&mut self, timeout: Duration) -> &mut Self {
        assert!(
            !timeout.is_zero(),
            "TLS handshake timeout must be greater than zero"
        );
        self.config.tls_handshake_timeout = timeout;
        self
    }

    /// Sets how long graceful shutdown waits for HTTP and WebSocket
    /// connections before aborting the remaining connection tasks. Defaults
    /// to 10 seconds.
    pub fn graceful_shutdown_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.config.graceful_shutdown_timeout = timeout;
        self
    }

    /// Sets the maximum number of concurrently admitted transport
    /// connections. The limit includes TLS handshakes and upgraded
    /// connections. Defaults to 10,000.
    pub fn max_connections(&mut self, limit: usize) -> &mut Self {
        self.config.max_connections = Some(limit.min(Semaphore::MAX_PERMITS));
        self
    }

    /// Disables the global connection-admission limit.
    pub fn disable_connection_limit(&mut self) -> &mut Self {
        self.config.max_connections = None;
        self
    }

    /// Sets the maximum request-target size in bytes. Defaults to 8 KiB.
    pub fn max_request_target_size(&mut self, bytes: usize) -> &mut Self {
        self.config.max_request_target_bytes = bytes;
        self
    }

    /// Sets the maximum raw query-string size in bytes. Defaults to 8 KiB.
    pub fn max_query_string_size(&mut self, bytes: usize) -> &mut Self {
        self.config.max_query_bytes = bytes;
        self
    }

    /// Sets the maximum number of request header fields, counting duplicate
    /// fields separately. Defaults to 100 and is capped at 1,024 so a
    /// configuration mistake cannot force Hyper to allocate an unbounded
    /// parser table per connection.
    pub fn max_request_header_count(&mut self, count: usize) -> &mut Self {
        self.config.max_request_header_count = count.min(MAX_REQUEST_HEADER_COUNT);
        self
    }

    /// Sets the logical request-header budget in bytes. The budget is the sum
    /// of every field-name and raw field-value length; duplicate fields count
    /// separately. Defaults to 32 KiB.
    pub fn max_request_header_bytes(&mut self, bytes: usize) -> &mut Self {
        self.config.max_request_header_bytes = bytes;
        self
    }

    /// Sets the trailing-slash policy (default: [`TrailingSlash::Ignore`]).
    pub fn trailing_slash(&mut self, policy: TrailingSlash) -> &mut Self {
        self.config.trailing_slash = policy;
        self
    }

    /// Lists every registered route in registration order (see
    /// [`Router::routes`]).
    pub fn routes(&self) -> Vec<super::RouteInfo> {
        self.router.routes()
    }

    /// Prints the registered routes, one per line, method first.
    pub fn print_routes(&self) {
        for route in self.routes() {
            println!("{:<7} {}", route.method, route.path);
        }
    }

    /// Builds an OpenAPI 3.0 document (as JSON) describing the routes
    /// registered so far. See [`App::serve_docs`] for serving it.
    pub fn openapi(&self, title: &str, version: &str) -> serde_json::Value {
        super::openapi::build_document(title, version, &self.routes())
    }

    /// Registers `GET {prefix}/openapi.json` (the OpenAPI document) and
    /// `GET {prefix}` (Swagger UI reading it). The document is a snapshot of
    /// the routes registered so far — call this after registering them.
    pub fn serve_docs(
        &mut self,
        prefix: &str,
        title: &str,
        version: &str,
    ) -> Result<(), RouteError> {
        let prefix = format!("/{}", prefix.trim_matches('/'));
        let spec_url = format!("{}/openapi.json", prefix.trim_end_matches('/'));
        let document = self.openapi(title, version);
        let html = super::openapi::swagger_ui_html(title, &spec_url);

        let mut docs = Router::new();
        let _ = docs.get(&spec_url, move |_req: Request| Response::json(&document))?;
        let _ = docs.get(&prefix, move |_req: Request| {
            Response::send(html.as_str()).content_type("text/html; charset=utf-8")
        })?;
        self.router.mount("/", docs)
    }

    pub fn route<H, M>(
        &mut self,
        method: hyper::Method,
        path: &str,
        handler: H,
    ) -> Result<RouteHandle<'_>, RouteError>
    where
        H: IntoHandler<M>,
    {
        self.router.route(method, path, handler)
    }

    pub fn get<H, M>(&mut self, path: &str, handler: H) -> Result<RouteHandle<'_>, RouteError>
    where
        H: IntoHandler<M>,
    {
        self.router.get(path, handler)
    }

    // These delegate to the root router and are part of the public API even
    // when a given binary registers its routes through a Router instead.
    pub fn post<H, M>(&mut self, path: &str, handler: H) -> Result<RouteHandle<'_>, RouteError>
    where
        H: IntoHandler<M>,
    {
        self.router.post(path, handler)
    }

    pub fn put<H, M>(&mut self, path: &str, handler: H) -> Result<RouteHandle<'_>, RouteError>
    where
        H: IntoHandler<M>,
    {
        self.router.put(path, handler)
    }

    pub fn delete<H, M>(&mut self, path: &str, handler: H) -> Result<RouteHandle<'_>, RouteError>
    where
        H: IntoHandler<M>,
    {
        self.router.delete(path, handler)
    }

    pub fn patch<H, M>(&mut self, path: &str, handler: H) -> Result<RouteHandle<'_>, RouteError>
    where
        H: IntoHandler<M>,
    {
        self.router.patch(path, handler)
    }

    pub fn options<H, M>(&mut self, path: &str, handler: H) -> Result<RouteHandle<'_>, RouteError>
    where
        H: IntoHandler<M>,
    {
        self.router.options(path, handler)
    }

    pub fn head<H, M>(&mut self, path: &str, handler: H) -> Result<RouteHandle<'_>, RouteError>
    where
        H: IntoHandler<M>,
    {
        self.router.head(path, handler)
    }

    pub fn all<H, M>(&mut self, path: &str, handler: H) -> Result<RouteHandle<'_>, RouteError>
    where
        H: IntoHandler<M>,
    {
        self.router.all(path, handler)
    }

    pub fn websocket<F, Fut, O>(
        &mut self,
        path: &str,
        handler: F,
    ) -> Result<RouteHandle<'_>, RouteError>
    where
        F: Fn(super::WebSocket) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = O> + Send + 'static,
        O: super::IntoWebSocketOutput + Send + 'static,
    {
        self.router.websocket(path, handler)
    }

    pub fn ws<F, Fut, O>(&mut self, path: &str, handler: F) -> Result<RouteHandle<'_>, RouteError>
    where
        F: Fn(super::WebSocket) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = O> + Send + 'static,
        O: super::IntoWebSocketOutput + Send + 'static,
    {
        self.router.ws(path, handler)
    }

    /// Like [`App::websocket`], with subprotocols, message size limits, and
    /// keepalive pings from `config`.
    pub fn websocket_with<F, Fut, O>(
        &mut self,
        path: &str,
        config: super::WebSocketConfig,
        handler: F,
    ) -> Result<RouteHandle<'_>, RouteError>
    where
        F: Fn(super::WebSocket) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = O> + Send + 'static,
        O: super::IntoWebSocketOutput + Send + 'static,
    {
        self.router.websocket_with(path, config, handler)
    }

    /// Mounts a router under `prefix` (Express-style sub-routes).
    pub fn mount(&mut self, prefix: &str, router: Router) -> Result<(), RouteError> {
        self.router.mount(prefix, router)
    }

    pub fn fallback<H, M>(&mut self, handler: H) -> Result<RouteHandle<'_>, RouteError>
    where
        H: IntoHandler<M>,
    {
        self.router.fallback(handler)
    }

    pub fn static_files<P>(&mut self, prefix: &str, root: P) -> Result<(), RouteError>
    where
        P: Into<std::path::PathBuf>,
    {
        self.router.static_files(prefix, root)
    }

    pub fn static_files_with_options<P>(
        &mut self,
        prefix: &str,
        root: P,
        options: super::StaticFilesOptions,
    ) -> Result<(), RouteError>
    where
        P: Into<std::path::PathBuf>,
    {
        self.router.static_files_with_options(prefix, root, options)
    }

    pub fn url_for<K, V, I>(&self, name: &str, params: I) -> Result<String, RouteError>
    where
        K: AsRef<str>,
        V: AsRef<str>,
        I: IntoIterator<Item = (K, V)>,
    {
        self.router.url_for(name, params)
    }

    pub fn state<T>(&mut self, value: T)
    where
        T: Send + Sync + 'static,
    {
        self.state.insert(value);
    }

    pub fn error_handler<F>(&mut self, handler: F)
    where
        F: Fn(HttpError) -> Response + Send + Sync + 'static,
    {
        self.error_handler = Some(Arc::new(handler));
    }

    /// Adds a global middleware (onion model). Middlewares run in registration
    /// order on the way in, and in reverse on the way out.
    pub fn layer<MW: IntoMiddleware>(&mut self, middleware: MW) {
        self.middlewares.push(middleware.into_middleware());
    }

    /// Binds to `address` and serves connections until the process is killed.
    /// Returns an error only if binding fails; accept errors are non-fatal.
    pub async fn listen(self, address: impl ToSocketAddrs) -> io::Result<()> {
        self.validate_websockets()?;
        let listener = TcpListener::bind(address).await?;
        if let Ok(local) = listener.local_addr() {
            println!("Server listening at http://{}", local);
        }
        self.serve(listener).await
    }

    /// Binds to `address` and serves until `shutdown` resolves, then drains
    /// in-flight connections gracefully.
    pub async fn listen_with_shutdown(
        self,
        address: impl ToSocketAddrs,
        shutdown: impl Future<Output = ()> + Send,
    ) -> io::Result<()> {
        self.validate_websockets()?;
        let listener = TcpListener::bind(address).await?;
        if let Ok(local) = listener.local_addr() {
            println!("Server listening at http://{}", local);
        }
        self.serve_with_shutdown(listener, shutdown).await
    }

    /// Serves connections on `listener` until the process is killed.
    pub async fn serve(self, listener: TcpListener) -> io::Result<()> {
        self.serve_with_shutdown(listener, std::future::pending::<()>())
            .await
    }

    /// Builds the Hyper connection server shared by plaintext and TLS
    /// transports so transport-independent limits cannot diverge.
    pub(crate) fn connection_builder(&self) -> auto::Builder<TokioExecutor> {
        self.connection_builder_with_http1_keep_alive(false)
    }

    pub(crate) fn websocket_connection_builder(&self) -> auto::Builder<TokioExecutor> {
        self.connection_builder_with_http1_keep_alive(true)
    }

    pub(crate) fn http2_connection_builder(&self) -> auto::Builder<TokioExecutor> {
        self.connection_builder_with_http1_keep_alive(false)
            .http2_only()
    }

    fn connection_builder_with_http1_keep_alive(
        &self,
        http1_keep_alive: bool,
    ) -> auto::Builder<TokioExecutor> {
        let mut builder = auto::Builder::new(TokioExecutor::new());
        builder
            .http1()
            .max_headers(self.config.max_request_header_count)
            .max_buf_size(self.max_http1_head_bytes())
            // Hyper normalizes TE+CL before the service and exposes no hook
            // for rejecting the ambiguity on every request in a persistent
            // connection. One request per HTTP/1 connection lets the raw
            // preflight enforce the invariant without reimplementing HTTP
            // body framing. HTTP/2 multiplexing and upgrades are unaffected.
            .keep_alive(http1_keep_alive);
        if let Some(timeout) = self.config.header_read_timeout {
            builder
                .http1()
                .timer(TokioTimer::new())
                .header_read_timeout(timeout);
        }
        let mut http2 = builder.http2();
        http2
            .max_header_list_size(
                self.config.max_request_header_bytes.min(u32::MAX as usize) as u32,
            )
            .max_concurrent_streams(DEFAULT_MAX_HTTP2_CONCURRENT_STREAMS)
            .max_send_buf_size(DEFAULT_MAX_HTTP2_SEND_BUFFER_BYTES)
            .timer(TokioTimer::new());
        if let Some(interval) = self.config.http2_keep_alive_interval {
            http2
                .keep_alive_interval(interval)
                .keep_alive_timeout(self.config.http2_keep_alive_timeout);
        }
        builder
    }

    pub(crate) fn max_http1_head_bytes(&self) -> usize {
        self.config
            .max_request_target_bytes
            .saturating_add(self.config.max_request_header_bytes)
            .saturating_add(
                self.config
                    .max_request_header_count
                    .saturating_mul(HTTP1_HEADER_FRAMING_OVERHEAD_BYTES),
            )
            .saturating_add(HTTP1_HEAD_FIXED_OVERHEAD_BYTES)
            .max(MIN_HTTP1_BUFFER_BYTES)
    }

    pub(crate) fn connection_admission(&self) -> Option<Arc<Semaphore>> {
        self.config
            .max_connections
            .map(|limit| Arc::new(Semaphore::new(limit)))
    }

    /// Serves connections on `listener` until `shutdown` resolves. A transient
    /// accept error is logged and retried — it never tears down the server.
    /// Once `shutdown` fires, the listener is closed and outstanding
    /// connections are drained (bounded by a 10s timeout).
    pub async fn serve_with_shutdown(
        self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()> + Send,
    ) -> io::Result<()> {
        self.validate_websockets()?;
        self.websocket_runtime.start_broker().await;
        let builder = Arc::new(self.connection_builder());
        let websocket_builder = Arc::new(self.websocket_connection_builder());
        let http2_builder = Arc::new(self.http2_connection_builder());
        let admission = self.connection_admission();
        let max_head_bytes = self.max_http1_head_bytes();
        let header_read_timeout = self.config.header_read_timeout;
        let app = Arc::new(self);
        let graceful = GracefulShutdown::new();
        let mut connection_tasks = JoinSet::new();
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

            let io = AdmittedIo::new(stream, permit);
            let app = Arc::clone(&app);
            let builder = Arc::clone(&builder);
            let websocket_builder = Arc::clone(&websocket_builder);
            let http2_builder = Arc::clone(&http2_builder);
            let watcher = graceful.watcher();
            connection_tasks.spawn(async move {
                let io = match preflight_connection(
                    io,
                    max_head_bytes,
                    header_read_timeout,
                    ExpectedProtocol::Auto,
                )
                .await
                {
                    Ok(Some(io)) => io,
                    Ok(None) => return,
                    Err(error) => {
                        if error.kind() != io::ErrorKind::TimedOut {
                            eprintln!("Error inspeccionando la conexion: {error}");
                        }
                        return;
                    }
                };
                let protocol = io.protocol();
                let websocket_upgrade = io.is_websocket_upgrade();
                let io = TokioIo::new(io);
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
                                            .handle(req, Some(peer), TransportSecurity::Plain)
                                            .await;
                                        if websocket_upgrade
                                            && response.status() != StatusCode::SWITCHING_PROTOCOLS
                                        {
                                            response.headers_mut().insert(
                                                CONNECTION,
                                                HeaderValue::from_static("close"),
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
                                        app.handle(req, Some(peer), TransportSecurity::Plain).await,
                                    )
                                }
                            }),
                        );
                        watcher.watch(connection).await
                    }
                };
                if let Err(err) = result {
                    eprintln!("Error serving connection: {:?}", err);
                }
            });
        }

        // Stop accepting new connections, then drain the in-flight ones.
        drop(listener);
        let drained = drain_server_connections(
            &app.websocket_runtime,
            graceful.shutdown(),
            app.config.graceful_shutdown_timeout,
        )
        .await;
        finish_connection_tasks(&mut connection_tasks, !drained).await;
        Ok(())
    }

    /// Translates a hyper request into a [`Request`] without consuming its
    /// body, dispatches it, and converts the result.
    pub(crate) async fn handle(
        &self,
        mut req: hyper::Request<Incoming>,
        remote_addr: Option<SocketAddr>,
        transport_security: TransportSecurity,
    ) -> hyper::Response<ResponseBody> {
        let authority = match validate_request_head(
            &self.config,
            req.version(),
            req.method(),
            req.uri(),
            req.headers(),
        ) {
            Ok(authority) => authority,
            Err(error) => return self.error_response(error).into_hyper(),
        };
        let upgrade = if is_websocket_upgrade_request(&req) {
            Some(hyper::upgrade::on(&mut req))
        } else {
            None
        };
        let (parts, body) = req.into_parts();
        let version = parts.version;
        let method = parts.method.as_str().to_string();
        let path = parts.uri.path().to_string();
        let raw_query = parts.uri.query().map(|q| q.to_string());
        let query = raw_query.as_deref().map(parse_query).unwrap_or_default();
        // Build a convenience single-value map (last value wins) and a
        // full-fidelity list that preserves duplicate headers.
        let mut headers: HashMap<String, String> = HashMap::new();
        let mut header_pairs: Vec<(String, String)> = Vec::new();
        for (name, value) in &parts.headers {
            let name = name.as_str().to_string();
            // `validate_request_head` rejected non-visible field values before
            // this conversion, so the framework's string view is lossless.
            let value = value
                .to_str()
                .expect("validated request header values are visible ASCII")
                .to_string();
            headers.insert(name.clone(), value.clone());
            header_pairs.push((name, value));
        }
        if !headers.contains_key("host") {
            if let Some(authority) = authority {
                headers.insert("host".to_string(), authority.clone());
                header_pairs.push(("host".to_string(), authority));
            }
        }
        let mut cookies = HashMap::new();
        for value in parts.headers.get_all(COOKIE) {
            if let Ok(value) = value.to_str() {
                // Multiple Cookie fields are equivalent to one field joined
                // with `; `. Extending in arrival order gives later duplicate
                // cookie names the same last-value-wins behavior.
                cookies.extend(parse_cookies(value));
            }
        }

        let request = Request {
            version,
            method,
            path,
            raw_query,
            query,
            headers,
            cookies,
            body: RequestBody::incoming(body, self.config.max_body_size),
            body_limit: self.config.max_body_size,
            params: HashMap::new(),
            route_pattern: None,
            websocket_runtime: self.websocket_runtime.clone(),
            resolved_websocket_config: None,
            state: self.state.clone(),
            extensions: StateStore::default(),
            upgrade,
            remote_addr,
            secure_transport: transport_security.is_secure(),
            header_pairs,
        };

        self.run_request(request).await.into_hyper()
    }

    /// Runs a request through dispatch, applying the configured per-request
    /// timeout (408 on expiry). Shared by the real server and the test client.
    pub(crate) async fn run_request(&self, request: Request) -> Response {
        let is_head = request.method == "HEAD";
        let is_connect = request
            .method
            .eq_ignore_ascii_case(Method::CONNECT.as_str());
        let request_version = request.version();
        let expected_websocket_accept = (request.upgrade.is_some()
            && request.is_websocket_upgrade())
        .then(|| {
            request
                .singleton_header(SEC_WEBSOCKET_KEY.as_str())
                .ok()
                .flatten()
                .map(super::response::websocket_accept)
        })
        .flatten();
        let mut response = match self.config.request_timeout {
            Some(timeout) => match tokio::time::timeout(timeout, self.dispatch(request)).await {
                Ok(response) => response,
                Err(_) => self.error_response(HttpError::request_timeout("Request Timeout")),
            },
            None => self.dispatch(request).await,
        };
        if response.status == StatusCode::SWITCHING_PROTOCOLS.as_u16() {
            let valid_accept =
                authorized_websocket_switch(&response, expected_websocket_accept.as_deref());
            if !valid_accept {
                response = Response::internal_server_error();
            }
        }
        if matches!(request_version, Version::HTTP_2 | Version::HTTP_3) {
            response.strip_http2_connection_headers();
        }
        enforce_connect_rejection_status(&mut response, is_connect);

        // Validate the complete selected representation before applying HEAD
        // semantics. Otherwise discarding the body could hide an invalid
        // Content-Length and a replacement 500 could leak its body on HEAD.
        response = response.finalize();
        if is_head {
            response.prepare_for_head();
            response = response.finalize();
        }
        response
    }

    /// Builds a response for an error, routing it through the registered
    /// `error_handler` if one is set, otherwise a default problem response.
    pub(crate) fn error_response(&self, error: HttpError) -> Response {
        render_http_error(error, self.error_handler.as_ref())
    }

    /// Resolves a request that did not directly match a route: auto-serves
    /// HEAD from a matching GET, auto-answers OPTIONS with `Allow`, returns 405
    /// when the path exists for other methods, or falls through to 404.
    fn resolve_miss(
        &self,
        method: &str,
        path: &str,
        host: Option<&str>,
    ) -> Result<MatchedRoute, super::RouteMatchError> {
        let allowed = self.router.allowed_methods(path, host)?;
        let implicit_head = self.router.resolve_http_method("GET", path, host)?;
        let has_implicit_head = implicit_head.is_some();
        if allowed.is_empty() {
            Ok(MatchedRoute {
                handler: not_found_handler(),
                middlewares: Vec::new(),
                params: HashMap::new(),
                pattern: path.to_string(),
                kind: RouteKind::Http,
                body_limit: None,
            })
        } else if method == "HEAD" && has_implicit_head {
            Ok(implicit_head.expect("ordinary GET route present"))
        } else if method == "OPTIONS" {
            Ok(MatchedRoute {
                handler: options_handler(allow_header_value(&allowed, has_implicit_head)),
                middlewares: Vec::new(),
                params: HashMap::new(),
                pattern: path.to_string(),
                kind: RouteKind::Http,
                body_limit: None,
            })
        } else {
            Ok(MatchedRoute {
                handler: method_not_allowed_handler(allow_header_value(
                    &allowed,
                    has_implicit_head,
                )),
                middlewares: Vec::new(),
                params: HashMap::new(),
                pattern: path.to_string(),
                kind: RouteKind::Http,
                body_limit: None,
            })
        }
    }

    /// Applies the trailing-slash policy to a non-canonical request path,
    /// returning the substitute handler (404 or 308 redirect) that should run
    /// through the middleware onion instead of route lookup.
    fn trailing_slash_miss(&self, request: &Request) -> Option<Handler> {
        if request.path.len() <= 1 || !request.path.ends_with('/') {
            return None;
        }
        match self.config.trailing_slash {
            TrailingSlash::Ignore => None,
            TrailingSlash::Strict => Some(not_found_handler()),
            TrailingSlash::Redirect => {
                let canonical_path = request.path.trim_end_matches('/');
                let canonical_path = canonical_path.replace('\\', "%5C");
                let mut location = if canonical_path.is_empty() {
                    "/".to_string()
                } else if canonical_path.starts_with("//") {
                    // Multiple leading slashes represent the same route
                    // segments, but `//host` in Location is scheme-relative.
                    format!("/{}", canonical_path.trim_start_matches('/'))
                } else {
                    canonical_path
                };
                if let Some(query) = &request.raw_query {
                    location.push('?');
                    location.push_str(query);
                }
                Some(Arc::new(move |_req| {
                    let location = location.clone();
                    Box::pin(async move { Response::redirect_with_status(&location, 308) })
                }))
            }
        }
    }

    async fn execute_chain(
        &self,
        request: Request,
        mut next: Next,
        route_middlewares: &[Middleware],
    ) -> Response {
        // Render handler errors before unwinding through middleware so
        // outbound post-processing sees the final application error response.
        next = guarded_next(next, self.error_handler.clone());

        // Route-scoped middlewares (inner), then global App middlewares
        // (outer). Each group is wrapped last-to-first so the first-registered
        // middleware in the group ends up outermost within that group.
        for middleware in route_middlewares.iter().rev() {
            let middleware = Arc::clone(middleware);
            let inner = next;
            let combined: Next = Box::new(move |req| (*middleware)(req, inner));
            next = guarded_next(combined, self.error_handler.clone());
        }
        for middleware in self.middlewares.iter().rev() {
            let middleware = Arc::clone(middleware);
            let inner = next;
            let combined: Next = Box::new(move |req| (*middleware)(req, inner));
            next = guarded_next(combined, self.error_handler.clone());
        }

        let response = match catch_unwind(AssertUnwindSafe(|| next(request))) {
            Ok(future) => match AssertUnwindSafe(future).catch_unwind().await {
                Ok(response) => response,
                Err(_) => panic_response(),
            },
            Err(_) => panic_response(),
        };
        render_response_error(response, self.error_handler.as_ref())
    }

    async fn dispatch_error_through_global(&self, request: Request, error: HttpError) -> Response {
        let response = self.error_response(error);
        let next: Next = Box::new(move |_request| Box::pin(async move { response }));
        self.execute_chain(request, next, &[]).await
    }

    /// Routes the request (capturing path params), then runs it through the
    /// middleware onion ending at the matched handler (or a 404 handler).
    pub(crate) async fn dispatch(&self, mut request: Request) -> Response {
        request.state = self.state.clone();
        request.websocket_runtime = self.websocket_runtime.clone();
        if request
            .method
            .eq_ignore_ascii_case(Method::CONNECT.as_str())
        {
            let mut response = self
                .dispatch_error_through_global(
                    request,
                    HttpError::new(
                        StatusCode::NOT_IMPLEMENTED,
                        "connect_not_supported",
                        "CONNECT requiere una API de tunel dedicada",
                    ),
                )
                .await;
            enforce_connect_rejection_status(&mut response, true);
            return response;
        }
        let host = request.header("host").map(str::to_string);
        let matched = match self.trailing_slash_miss(&request) {
            Some(handler) => MatchedRoute {
                handler,
                middlewares: Vec::new(),
                params: HashMap::new(),
                pattern: request.path.clone(),
                kind: RouteKind::Http,
                body_limit: None,
            },
            None => {
                match self
                    .router
                    .resolve_method(&request.method, &request.path, host.as_deref())
                {
                    Ok(Some(found)) => found,
                    Ok(None) => {
                        match self.resolve_miss(&request.method, &request.path, host.as_deref()) {
                            Ok(miss) => miss,
                            Err(error) => {
                                return self
                                    .dispatch_error_through_global(request, error.into())
                                    .await;
                            }
                        }
                    }
                    Err(error) => {
                        return self
                            .dispatch_error_through_global(request, error.into())
                            .await;
                    }
                }
            }
        };
        let MatchedRoute {
            handler,
            middlewares: route_middlewares,
            params,
            pattern,
            kind,
            body_limit,
        } = matched;
        let effective_body_limit = body_limit
            .unwrap_or(self.config.max_body_size)
            .min(self.config.max_body_size);
        request.set_body_limit(effective_body_limit);
        request.params = params;
        request.route_pattern = Some(pattern);
        request.resolved_websocket_config = match kind {
            RouteKind::Http => None,
            RouteKind::WebSocket(route_config) => {
                Some(super::websocket::ResolvedWebSocketConfig::from_layers(
                    &self.websocket_defaults,
                    &route_config,
                ))
            }
        };

        if let Err(error) = request.validate_content_length() {
            return self.dispatch_error_through_global(request, error).await;
        }

        // Innermost layer: the matched handler.
        let next: Next = Box::new(move |req| (*handler)(req));
        self.execute_chain(request, next, &route_middlewares).await
    }
}

fn enforce_connect_rejection_status(response: &mut Response, is_connect: bool) {
    if !is_connect {
        return;
    }
    if StatusCode::from_u16(response.status).is_ok_and(|status| status.is_success()) {
        // Every 2xx response to CONNECT changes the HTTP connection into a
        // tunnel. Until the framework exposes a transport-owning tunnel API,
        // no application error renderer or middleware may opt into that
        // protocol transition merely by rewriting the status.
        response.status = StatusCode::NOT_IMPLEMENTED.as_u16();
    }
}

fn authorized_websocket_switch(response: &Response, expected_accept: Option<&str>) -> bool {
    response.websocket_upgrade_authorized()
        && expected_accept.is_some_and(|expected| {
            let mut values = response.headers.get_all(SEC_WEBSOCKET_ACCEPT).iter();
            let first = values.next().and_then(|value| value.to_str().ok());
            first == Some(expected) && values.next().is_none()
        })
}

fn guarded_next(next: Next, error_handler: Option<ErrorHandler>) -> Next {
    Box::new(move |request| {
        let invoked = catch_unwind(AssertUnwindSafe(|| next(request)));
        Box::pin(async move {
            let response = match invoked {
                Ok(future) => match AssertUnwindSafe(future).catch_unwind().await {
                    Ok(response) => response,
                    Err(_) => panic_response(),
                },
                Err(_) => panic_response(),
            };
            render_response_error(response, error_handler.as_ref())
        })
    })
}

fn render_response_error(mut response: Response, error_handler: Option<&ErrorHandler>) -> Response {
    match response.take_error() {
        Some(error) if error_handler.is_some() => render_http_error(error, error_handler),
        Some(_) | None => response,
    }
}

fn render_http_error(error: HttpError, error_handler: Option<&ErrorHandler>) -> Response {
    let Some(handler) = error_handler else {
        return Response::from_error(error);
    };

    let required_headers = error.headers().clone();
    let mut response = match catch_unwind(AssertUnwindSafe(|| handler(error))) {
        Ok(response) => response,
        Err(_) => {
            eprintln!("El manejador global de errores hizo panic; devolviendo 500.");
            let mut response = Response::internal_server_error();
            // The renderer itself failed. This generic fallback is final and
            // must not be fed recursively to the same panicking renderer.
            let _ = response.take_error();
            return response;
        }
    };
    // A custom renderer is the terminal representation even if it built its
    // result through Response::from_error.
    let _ = response.take_error();
    for name in required_headers.keys() {
        response.headers.remove(name);
    }
    for (name, value) in &required_headers {
        response.headers.append(name, value.clone());
    }
    response
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use hyper::header::HeaderValue;

    fn http2_preface_with_frame(
        payload_length: usize,
        frame_type: u8,
        flags: u8,
        stream_id: u32,
        payload: &[u8],
    ) -> Vec<u8> {
        let mut bytes = HTTP2_PREFACE.to_vec();
        bytes.extend_from_slice(&[
            ((payload_length >> 16) & 0xff) as u8,
            ((payload_length >> 8) & 0xff) as u8,
            (payload_length & 0xff) as u8,
            frame_type,
            flags,
        ]);
        bytes.extend_from_slice(&stream_id.to_be_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }

    #[test]
    fn http2_preflight_requires_a_complete_valid_initial_settings_frame() {
        assert_eq!(
            inspect_http2_client_preface(&HTTP2_PREFACE[..12]).unwrap(),
            Http2PrefaceState::Pending
        );
        assert_eq!(
            inspect_http2_client_preface(HTTP2_PREFACE).unwrap(),
            Http2PrefaceState::Pending
        );

        let valid = http2_preface_with_frame(0, HTTP2_SETTINGS_FRAME, 0, 0, &[]);
        assert_eq!(
            inspect_http2_client_preface(&valid).unwrap(),
            Http2PrefaceState::Complete
        );

        let payload = [0, 3, 0, 0, 0, 100];
        let mut partial =
            http2_preface_with_frame(payload.len(), HTTP2_SETTINGS_FRAME, 0, 0, &payload);
        partial.pop();
        assert_eq!(
            inspect_http2_client_preface(&partial).unwrap(),
            Http2PrefaceState::Pending
        );
    }

    #[test]
    fn http2_preflight_rejects_invalid_initial_frames() {
        for invalid in [
            http2_preface_with_frame(0, 0, 0, 0, &[]),
            http2_preface_with_frame(0, HTTP2_SETTINGS_FRAME, HTTP2_SETTINGS_ACK, 0, &[]),
            http2_preface_with_frame(0, HTTP2_SETTINGS_FRAME, 0, 1, &[]),
            http2_preface_with_frame(1, HTTP2_SETTINGS_FRAME, 0, 0, &[0]),
            http2_preface_with_frame(
                HTTP2_MAX_INITIAL_SETTINGS_BYTES + 6,
                HTTP2_SETTINGS_FRAME,
                0,
                0,
                &[],
            ),
        ] {
            let error = inspect_http2_client_preface(&invalid).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        }
    }

    #[test]
    fn production_transport_timeouts_are_finite_by_default_and_can_be_disabled() {
        let mut app = App::new();
        assert_eq!(app.config.request_timeout, Some(DEFAULT_REQUEST_TIMEOUT));
        assert_eq!(
            app.config.http2_keep_alive_interval,
            Some(DEFAULT_HTTP2_KEEP_ALIVE_INTERVAL)
        );
        assert_eq!(
            app.config.http2_keep_alive_timeout,
            DEFAULT_HTTP2_KEEP_ALIVE_TIMEOUT
        );
        app.disable_request_timeout().disable_http2_keep_alive();
        assert_eq!(app.config.request_timeout, None);
        assert_eq!(app.config.http2_keep_alive_interval, None);
    }

    #[test]
    fn invalid_transport_deadlines_are_rejected_as_configuration_errors() {
        fn rejects(configure: impl FnOnce(&mut App)) -> bool {
            std::panic::catch_unwind(AssertUnwindSafe(|| {
                let mut app = App::new();
                configure(&mut app);
            }))
            .is_err()
        }

        assert!(rejects(|app| {
            app.request_timeout(Duration::ZERO);
        }));
        assert!(rejects(|app| {
            app.header_read_timeout(Duration::ZERO);
        }));
        assert!(rejects(|app| {
            app.http2_keep_alive_interval(Duration::ZERO);
        }));
        assert!(rejects(|app| {
            app.http2_keep_alive_timeout(Duration::ZERO);
        }));
        assert!(rejects(|app| {
            app.http2_keep_alive_interval(Duration::MAX);
        }));
        assert!(rejects(|app| {
            app.http2_keep_alive_timeout(Duration::MAX);
        }));
        assert!(rejects(|app| {
            app.header_read_timeout(Duration::MAX);
        }));

        #[cfg(feature = "tls")]
        assert!(rejects(|app| {
            app.tls_handshake_timeout(Duration::ZERO);
        }));
    }

    #[test]
    fn request_header_count_is_capped_and_http1_buffer_includes_framing() {
        let mut app = App::new();
        app.max_request_target_size(10_000)
            .max_request_header_bytes(1)
            .max_request_header_count(usize::MAX);

        assert_eq!(
            app.config.max_request_header_count,
            MAX_REQUEST_HEADER_COUNT
        );
        assert_eq!(
            app.max_http1_head_bytes(),
            (10_000 + 1)
                + MAX_REQUEST_HEADER_COUNT * HTTP1_HEADER_FRAMING_OVERHEAD_BYTES
                + HTTP1_HEAD_FIXED_OVERHEAD_BYTES
        );
    }

    #[tokio::test]
    async fn graceful_timeout_does_not_wait_forever_for_unregistered_websocket_driver() {
        let runtime = WebSocketRuntimeHandle::local();
        let config = super::super::websocket::ResolvedWebSocketConfig::from_layers(
            &WebSocketConfig::default(),
            &WebSocketConfig::default(),
        );
        let permit = runtime.admit("/ws", None, None, &config).unwrap();

        let drained = tokio::time::timeout(
            Duration::from_millis(250),
            drain_server_connections(
                &runtime,
                std::future::pending::<()>(),
                Duration::from_millis(10),
            ),
        )
        .await
        .expect("the graceful-shutdown deadline must remain a hard upper bound");

        assert!(!drained);
        assert_eq!(runtime.stats().active_connections, 1);
        drop(permit);
        assert_eq!(runtime.stats().active_connections, 0);
    }

    #[test]
    fn http2_authority_is_used_when_host_is_absent() {
        let uri = Uri::from_static("https://api.example.test/users?q=rust");
        let authority = validate_request_head(
            &ServerConfig::default(),
            Version::HTTP_2,
            &hyper::Method::GET,
            &uri,
            &HeaderMap::new(),
        )
        .unwrap();

        assert_eq!(authority.as_deref(), Some("api.example.test"));
    }

    #[test]
    fn strict_authority_validation_rejects_userinfo_and_invalid_ports() {
        // `http::uri::Authority` is intentionally permissive here, so merely
        // parsing into that type is not sufficient request-head validation.
        for raw in [
            "user@example.test",
            "example.test:abc",
            "example.test:",
            "example.test:65536",
        ] {
            assert!(
                raw.parse::<hyper::http::uri::Authority>().is_ok(),
                "the upstream parser behavior changed for {raw:?}"
            );
            assert!(ValidatedAuthority::parse(raw).is_none(), "accepted {raw:?}");
        }
        for raw in [
            "",
            ":80",
            " example.test",
            "example.test\t",
            "example.test\u{7f}",
            "example.test/path",
            "[::1]:abc",
            "[::1]:65536",
            "[::1]suffix",
        ] {
            assert!(ValidatedAuthority::parse(raw).is_none(), "accepted {raw:?}");
        }

        for raw in [
            "user@example.test",
            "example.test:abc",
            "example.test:",
            "example.test:65536",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(HOST, HeaderValue::from_static(raw));
            let error = validate_request_head(
                &ServerConfig::default(),
                Version::HTTP_11,
                &hyper::Method::GET,
                &Uri::from_static("/"),
                &headers,
            )
            .unwrap_err();
            assert_eq!(error.code(), "invalid_authority", "accepted Host {raw:?}");
        }

        for raw in [
            "https://user@example.test/",
            "https://example.test:abc/",
            "https://example.test:/",
            "https://example.test:65536/",
        ] {
            let uri = raw
                .parse::<Uri>()
                .expect("the permissive URI parser accepts it");
            let error = validate_request_head(
                &ServerConfig::default(),
                Version::HTTP_2,
                &hyper::Method::GET,
                &uri,
                &HeaderMap::new(),
            )
            .unwrap_err();
            assert_eq!(
                error.code(),
                "invalid_authority",
                "accepted URI authority {raw:?}"
            );
        }

        let mut headers = HeaderMap::new();
        headers.insert(HOST, HeaderValue::from_static("example.test"));
        let error = validate_request_head(
            &ServerConfig::default(),
            Version::HTTP_11,
            &hyper::Method::GET,
            &Uri::from_static("http://user@example.test/"),
            &headers,
        )
        .unwrap_err();
        assert_eq!(error.code(), "invalid_authority");
    }

    #[test]
    fn strict_authority_validation_accepts_hosts_ips_and_default_ports() {
        for (raw, expected) in [
            ("API.Example.TEST", "api.example.test"),
            ("localhost:0", "localhost:0"),
            ("example.test:65535", "example.test:65535"),
            ("127.0.0.1:8080", "127.0.0.1:8080"),
            (
                "[2001:0DB8:0000:0000:0000:0000:0000:0001]:443",
                "[2001:db8::1]:443",
            ),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(HOST, HeaderValue::from_static(raw));
            let authority = validate_request_head(
                &ServerConfig::default(),
                Version::HTTP_11,
                &hyper::Method::GET,
                &Uri::from_static("/"),
                &headers,
            )
            .unwrap();
            assert_eq!(authority.as_deref(), Some(expected));
        }

        for (uri, host, expected) in [
            (
                Uri::from_static("http://example.test:80/"),
                "EXAMPLE.TEST",
                "example.test",
            ),
            (
                Uri::from_static("https://example.test/"),
                "example.test:443",
                "example.test:443",
            ),
            (
                Uri::from_static("https://[2001:db8::1]/"),
                "[2001:0DB8:0:0:0:0:0:1]:443",
                "[2001:db8::1]:443",
            ),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(HOST, HeaderValue::from_static(host));
            let authority = validate_request_head(
                &ServerConfig::default(),
                Version::HTTP_11,
                &hyper::Method::GET,
                &uri,
                &headers,
            )
            .unwrap();
            assert_eq!(authority.as_deref(), Some(expected));
        }

        let mut headers = HeaderMap::new();
        headers.insert(HOST, HeaderValue::from_static("example.test:444"));
        let error = validate_request_head(
            &ServerConfig::default(),
            Version::HTTP_11,
            &hyper::Method::GET,
            &Uri::from_static("https://example.test/"),
            &headers,
        )
        .unwrap_err();
        assert_eq!(error.code(), "invalid_authority");
    }

    #[test]
    fn conflicting_host_and_uri_authority_are_rejected() {
        let uri = Uri::from_static("https://api.example.test/users");
        let mut headers = HeaderMap::new();
        headers.insert(HOST, HeaderValue::from_static("other.example.test"));

        let error = validate_request_head(
            &ServerConfig::default(),
            Version::HTTP_2,
            &hyper::Method::GET,
            &uri,
            &headers,
        )
        .unwrap_err();

        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error.code(), "invalid_authority");
    }

    #[test]
    fn duplicate_host_fields_are_rejected_even_when_equal() {
        let uri = Uri::from_static("/");
        let mut headers = HeaderMap::new();
        headers.append(HOST, HeaderValue::from_static("example.test"));
        headers.append(HOST, HeaderValue::from_static("example.test"));

        let error = validate_request_head(
            &ServerConfig::default(),
            Version::HTTP_11,
            &hyper::Method::GET,
            &uri,
            &headers,
        )
        .unwrap_err();

        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error.code(), "invalid_authority");
    }

    #[test]
    fn http11_absolute_form_still_requires_a_host_field() {
        let uri = Uri::from_static("http://example.test/resource");
        let error = validate_request_head(
            &ServerConfig::default(),
            Version::HTTP_11,
            &hyper::Method::GET,
            &uri,
            &HeaderMap::new(),
        )
        .unwrap_err();

        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error.code(), "invalid_authority");
    }

    #[test]
    fn non_visible_request_header_values_are_rejected_instead_of_erased() {
        let uri = Uri::from_static("/");
        let mut headers = HeaderMap::new();
        headers.insert(HOST, HeaderValue::from_static("example.test"));
        headers.insert(
            "x-binary",
            HeaderValue::from_bytes(&[0x80]).expect("obs-text is representable by HeaderValue"),
        );

        let error = validate_request_head(
            &ServerConfig::default(),
            Version::HTTP_11,
            &hyper::Method::GET,
            &uri,
            &headers,
        )
        .unwrap_err();

        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error.code(), "invalid_header_value");
    }

    #[test]
    fn raw_websocket_candidate_detection_uses_http_token_lists() {
        let head = b"GET /ws HTTP/1.1\r\nHost: example.test\r\nConnection: keep-alive, Upgrade\r\nUpgrade: h2c, websocket\r\n\r\n";
        assert!(raw_head_is_websocket_upgrade(head));

        let ordinary_spacing = b"GET /ws HTTP/1.1\r\nHost: example.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n";
        assert!(raw_head_is_websocket_upgrade(ordinary_spacing));

        let empty_values =
            b"GET /ws HTTP/1.1\r\nHost: example.test\r\nConnection: \r\nUpgrade:\t\r\n\r\n";
        assert!(!raw_head_is_websocket_upgrade(empty_values));
        assert_eq!(trim_ascii_whitespace(b" "), b"");
        assert_eq!(trim_ascii_whitespace(b"\t"), b"");
    }

    #[test]
    fn only_transport_owning_websocket_responses_can_authorize_a_switch() {
        let accept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
        let mut response = Response::send("")
            .status(101)
            .header(CONNECTION.as_str(), "Upgrade")
            .header(UPGRADE.as_str(), "websocket")
            .header(SEC_WEBSOCKET_ACCEPT.as_str(), accept);

        assert!(!authorized_websocket_switch(&response, Some(accept)));
        response.authorize_websocket_upgrade();
        assert!(authorized_websocket_switch(&response, Some(accept)));
        assert!(!authorized_websocket_switch(&response, None));
        assert!(!authorized_websocket_switch(&response, Some("wrong")));
    }
}
