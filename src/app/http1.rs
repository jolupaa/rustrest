//! Per-request HTTP/1 framing inspection for persistent connections.
//!
//! Hyper 1.x silently drops a `Content-Length` field that follows
//! `Transfer-Encoding` before the service sees the request head (see
//! `Server::parse` in hyper's `proto/h1/role.rs`), so the application cannot
//! tell that such a request was ambiguous. RFC 9112 §6.1 allows a server to
//! reject a request that carries both fields and requires the connection to
//! be closed after responding either way.
//!
//! [`Http1InspectedIo`] sits between the socket and Hyper and parses every
//! request head with `httparse`, the parser Hyper uses, before Hyper reads the
//! same bytes. Verdicts reach the service through [`Http1Verdicts`] in request
//! order, which Hyper preserves because an HTTP/1 connection dispatches one
//! request at a time. The inspector only follows `Content-Length` framing: a
//! head whose successor cannot be located without decoding the body or
//! protocol switch (`Transfer-Encoding`, `Upgrade`, `CONNECT`), or that it
//! cannot parse, is terminal. The service closes the connection after
//! answering a terminal request, so Hyper never parses a head the inspector
//! did not see.
//!
//! Hyper is used with its default `httparse` configuration. If a lenient
//! parser option (for example `ignore_invalid_headers`) is ever enabled on the
//! HTTP/1 builder, the same option must be applied in [`inspect_head`].

use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Hyper rejects lengths above `DecodedLength::MAX_LEN` (`u64::MAX - 2`).
const MAX_DECODABLE_LENGTH: u64 = u64::MAX - 2;
/// Verdicts waiting for Hyper to dispatch pipelined requests. Hyper bounds how
/// far it reads ahead, so this cap is only reached by pathological pipelining;
/// requests beyond it are refused and the connection closes.
const MAX_PENDING_VERDICTS: usize = 256;
const INLINE_HEADER_SLOTS: usize = 128;
/// Capacity kept for the partial-head buffer between requests.
const RETAINED_HEAD_CAPACITY: usize = 8 * 1024;

/// What the service must do with the next request dispatched by Hyper.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HeadVerdict {
    /// Unambiguous `Content-Length` (or no body) framing: the connection may be
    /// reused after the response.
    Persistent,
    /// The request may be served, but the connection must close afterwards
    /// because the next message boundary is not tracked.
    Terminal,
    /// `Transfer-Encoding` and `Content-Length` were both present, in either
    /// order. The request must be rejected and the connection closed.
    Ambiguous,
    /// Hyper dispatched a head the inspector never classified (after a
    /// terminal head whose close the peer somehow avoided, or beyond the
    /// pipelining cap). It must be refused and the connection closed.
    Unclassified,
}

/// In-order verdict queue shared by the inspected transport and the service.
#[derive(Debug, Default)]
pub(crate) struct Http1Verdicts {
    pending: Mutex<VecDeque<HeadVerdict>>,
}

impl Http1Verdicts {
    fn push(&self, verdict: HeadVerdict) -> bool {
        let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        if pending.len() >= MAX_PENDING_VERDICTS {
            return false;
        }
        pending.push_back(verdict);
        true
    }

    /// Returns the verdict for the request Hyper is dispatching. A request the
    /// inspector did not classify is refused: serving it could expose a head
    /// whose framing was never checked.
    pub(crate) fn next(&self) -> HeadVerdict {
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
            .unwrap_or(HeadVerdict::Unclassified)
    }
}

enum InspectState {
    /// Waiting for (the rest of) a request head.
    Head,
    /// Skipping the remaining `Content-Length` body bytes.
    Body(u64),
    /// No further heads will be served on this connection.
    Passthrough,
}

enum HeadParse {
    Complete {
        length: usize,
        verdict: HeadVerdict,
        body_length: u64,
    },
    Partial,
    Invalid,
}

/// Transport wrapper that classifies each HTTP/1 request head before Hyper
/// reads it. Writes pass through untouched.
pub(crate) struct Http1InspectedIo<T> {
    inner: T,
    verdicts: Arc<Http1Verdicts>,
    state: InspectState,
    head: Vec<u8>,
    /// Prefix of `head` already known to contain no complete head terminator.
    head_scanned: usize,
    max_head_bytes: usize,
    max_headers: usize,
    #[cfg(test)]
    buffered_parses: usize,
}

impl<T> Http1InspectedIo<T> {
    /// `max_head_bytes` and `max_headers` must match the HTTP/1 builder's
    /// `max_buf_size` and `max_headers`, so every head Hyper can parse is also
    /// parsed here.
    pub(crate) fn new(
        inner: T,
        verdicts: Arc<Http1Verdicts>,
        max_head_bytes: usize,
        max_headers: usize,
    ) -> Self {
        Self {
            inner,
            verdicts,
            state: InspectState::Head,
            head: Vec::new(),
            head_scanned: 0,
            max_head_bytes,
            max_headers,
            #[cfg(test)]
            buffered_parses: 0,
        }
    }

    fn observe(&mut self, mut input: &[u8]) {
        while !input.is_empty() {
            match self.state {
                InspectState::Passthrough => return,
                InspectState::Body(remaining) => {
                    let skipped = remaining.min(input.len() as u64);
                    input = &input[skipped as usize..];
                    self.state = match remaining - skipped {
                        0 => InspectState::Head,
                        remaining => InspectState::Body(remaining),
                    };
                }
                InspectState::Head if self.head.is_empty() => {
                    match inspect_head(input, self.max_headers) {
                        HeadParse::Complete {
                            length,
                            verdict,
                            body_length,
                        } => {
                            input = &input[length..];
                            self.finish_head(verdict, body_length);
                        }
                        HeadParse::Partial => {
                            self.buffer_partial_head(input);
                            return;
                        }
                        HeadParse::Invalid => {
                            self.finish_head(HeadVerdict::Terminal, 0);
                            return;
                        }
                    }
                }
                InspectState::Head => {
                    self.head.extend_from_slice(input);
                    let skipped = trim_leading_empty_lines(&mut self.head);
                    self.head_scanned = self.head_scanned.saturating_sub(skipped);
                    if self.head.is_empty() {
                        return;
                    }
                    // Re-parsing only once an empty line may have arrived
                    // keeps a drip-fed head linear instead of quadratic.
                    if !contains_head_terminator(&self.head, self.head_scanned) {
                        self.head_scanned = self.head.len();
                        if self.head.len() >= self.max_head_bytes {
                            self.finish_head(HeadVerdict::Terminal, 0);
                        }
                        return;
                    }
                    #[cfg(test)]
                    {
                        self.buffered_parses += 1;
                    }
                    match inspect_head(&self.head, self.max_headers) {
                        HeadParse::Complete {
                            length,
                            verdict,
                            body_length,
                        } => {
                            let rest = self.head.split_off(length);
                            self.reset_head_buffer();
                            self.finish_head(verdict, body_length);
                            // The head buffer is empty again, so this call
                            // takes the borrowed fast path and cannot recurse
                            // further.
                            self.observe(&rest);
                        }
                        HeadParse::Partial => {
                            if self.head.len() >= self.max_head_bytes {
                                self.finish_head(HeadVerdict::Terminal, 0);
                            }
                        }
                        HeadParse::Invalid => self.finish_head(HeadVerdict::Terminal, 0),
                    }
                    return;
                }
            }
        }
    }

    fn buffer_partial_head(&mut self, bytes: &[u8]) {
        if bytes.len() >= self.max_head_bytes {
            // Hyper answers an oversized head itself and then closes.
            self.finish_head(HeadVerdict::Terminal, 0);
        } else {
            self.head.extend_from_slice(bytes);
            trim_leading_empty_lines(&mut self.head);
            // `bytes` was just parsed as incomplete, but a terminator may
            // still straddle it and the next read.
            self.head_scanned = self.head.len();
        }
    }

    fn reset_head_buffer(&mut self) {
        self.head.clear();
        self.head_scanned = 0;
        if self.head.capacity() > RETAINED_HEAD_CAPACITY {
            self.head.shrink_to(RETAINED_HEAD_CAPACITY);
        }
    }

    fn finish_head(&mut self, verdict: HeadVerdict, body_length: u64) {
        let accepted = self.verdicts.push(verdict);
        self.state = match verdict {
            HeadVerdict::Persistent if accepted && body_length == 0 => InspectState::Head,
            HeadVerdict::Persistent if accepted => InspectState::Body(body_length),
            _ => {
                self.head = Vec::new();
                self.head_scanned = 0;
                InspectState::Passthrough
            }
        };
    }
}

/// Drops the empty lines (`\r\n` or `\n`) that may precede a request line
/// (RFC 9112 §2.2); `httparse` skips them too, so keeping them would only make
/// a client that drips blank lines trigger repeated full re-parses. A lone
/// trailing `\r` is kept because its `\n` may still arrive. Returns how many
/// bytes were removed.
fn trim_leading_empty_lines(buffer: &mut Vec<u8>) -> usize {
    let mut skipped = 0;
    loop {
        match &buffer[skipped..] {
            [b'\r', b'\n', ..] => skipped += 2,
            [b'\n', ..] => skipped += 1,
            _ => break,
        }
    }
    buffer.drain(..skipped);
    skipped
}

/// Whether `bytes[scanned..]` (with a small overlap for a terminator split
/// across reads) contains the empty line (`\n\n` or `\n\r\n`) that can
/// complete a request head. Leading empty lines also match, which only
/// costs one extra parse.
fn contains_head_terminator(bytes: &[u8], scanned: usize) -> bool {
    let start = scanned.saturating_sub(2);
    let window = &bytes[start..];
    window.windows(2).any(|pair| pair == b"\n\n")
        || window.windows(3).any(|triple| triple == b"\n\r\n")
}

/// Parses one request head exactly as Hyper's HTTP/1 server does and
/// classifies its framing.
fn inspect_head(bytes: &[u8], max_headers: usize) -> HeadParse {
    let mut inline = [httparse::EMPTY_HEADER; INLINE_HEADER_SLOTS];
    let mut heap = Vec::new();
    let headers = if max_headers <= INLINE_HEADER_SLOTS {
        &mut inline[..max_headers]
    } else {
        heap.resize(max_headers, httparse::EMPTY_HEADER);
        heap.as_mut_slice()
    };
    let mut request = httparse::Request::new(headers);
    let length = match request.parse(bytes) {
        Ok(httparse::Status::Complete(length)) => length,
        Ok(httparse::Status::Partial) => return HeadParse::Partial,
        Err(_) => return HeadParse::Invalid,
    };

    let mut transfer_encoding = false;
    let mut content_length_present = false;
    let mut content_length = None;
    let mut invalid_content_length = false;
    let mut upgrade = false;
    for header in request.headers.iter() {
        if header.name.eq_ignore_ascii_case("transfer-encoding") {
            transfer_encoding = true;
        } else if header.name.eq_ignore_ascii_case("content-length") {
            content_length_present = true;
            match parse_content_length(header.value) {
                Some(value) if content_length.is_none_or(|previous| previous == value) => {
                    content_length = Some(value);
                }
                _ => invalid_content_length = true,
            }
        } else if header.name.eq_ignore_ascii_case("upgrade") {
            upgrade = true;
        }
    }
    let connect = request
        .method
        .is_some_and(|method| method.eq_ignore_ascii_case("CONNECT"));

    let verdict = if transfer_encoding && content_length_present {
        HeadVerdict::Ambiguous
    } else if transfer_encoding || upgrade || connect || invalid_content_length {
        HeadVerdict::Terminal
    } else {
        HeadVerdict::Persistent
    };
    HeadParse::Complete {
        length,
        verdict,
        body_length: content_length.unwrap_or(0),
    }
}

/// Mirrors Hyper's strict `Content-Length` grammar: ASCII digits only, no
/// sign, list, or whitespace, and a value Hyper can decode.
fn parse_content_length(value: &[u8]) -> Option<u64> {
    if value.is_empty() {
        return None;
    }
    let mut length = 0_u64;
    for &byte in value {
        if !byte.is_ascii_digit() {
            return None;
        }
        length = length
            .checked_mul(10)?
            .checked_add(u64::from(byte - b'0'))?;
    }
    (length <= MAX_DECODABLE_LENGTH).then_some(length)
}

impl<T: AsyncRead + Unpin> AsyncRead for Http1InspectedIo<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let filled_before = buffer.filled().len();
        let result = Pin::new(&mut this.inner).poll_read(cx, buffer);
        if let Poll::Ready(Ok(())) = result {
            this.observe(&buffer.filled()[filled_before..]);
        }
        result
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for Http1InspectedIo<T> {
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

#[cfg(test)]
mod tests {
    use super::*;

    const MAX_HEAD: usize = 16 * 1024;

    fn verdicts_for(stream: &[u8], split: impl IntoIterator<Item = usize>) -> Vec<HeadVerdict> {
        let verdicts = Arc::new(Http1Verdicts::default());
        let mut io = Http1InspectedIo::new((), Arc::clone(&verdicts), MAX_HEAD, 100);
        let mut start = 0;
        for end in split.into_iter().chain([stream.len()]) {
            io.observe(&stream[start..end]);
            start = end;
        }
        let pending = verdicts.pending.lock().unwrap();
        pending.iter().copied().collect()
    }

    /// Feeds `stream` at every possible single split point and in one-byte
    /// chunks, asserting the verdict sequence never depends on read sizes.
    fn assert_verdicts(stream: &[u8], expected: &[HeadVerdict]) {
        assert_eq!(verdicts_for(stream, []), expected, "whole stream");
        for split in 1..stream.len() {
            assert_eq!(verdicts_for(stream, [split]), expected, "split at {split}");
        }
        assert_eq!(
            verdicts_for(stream, 1..stream.len()),
            expected,
            "one byte per read"
        );
    }

    use HeadVerdict::{Ambiguous, Persistent, Terminal, Unclassified};

    #[test]
    fn content_length_framing_is_followed_across_pipelined_requests() {
        let embedded = "GET /smuggled HTTP/1.1\r\n\r\n";
        let stream = format!(
            "POST /a HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{embedded}GET /b HTTP/1.1\r\nHost: x\r\n\r\n",
            embedded.len()
        );
        assert_verdicts(stream.as_bytes(), &[Persistent, Persistent]);
    }

    #[test]
    fn transfer_encoding_with_content_length_is_ambiguous_in_either_order() {
        for framing in [
            "Transfer-Encoding: chunked\r\nContent-Length: 3\r\n",
            "Content-Length: 3\r\nTransfer-Encoding: chunked\r\n",
            "transfer-encoding: chunked\r\ncontent-length: 3\r\n",
            "Content-Length: 3\r\nTransfer-Encoding: identity\r\n",
        ] {
            let stream = format!(
                "GET /a HTTP/1.1\r\nHost: x\r\n\r\nPOST /b HTTP/1.1\r\nHost: x\r\n{framing}\r\n0\r\n\r\nGET /c HTTP/1.1\r\nHost: x\r\n\r\n"
            );
            assert_verdicts(stream.as_bytes(), &[Persistent, Ambiguous]);
        }
    }

    #[test]
    fn leading_empty_lines_cannot_hide_a_head() {
        assert_verdicts(
            b"GET /a HTTP/1.1\r\nHost: x\r\n\r\n\r\n\n\r\nPOST /b HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nContent-Length: 3\r\n\r\n",
            &[Persistent, Ambiguous],
        );
    }

    #[test]
    fn heads_whose_successor_is_not_tracked_are_terminal() {
        for head in [
            "POST /a HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n",
            "GET /a HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n",
            "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n",
            "POST /a HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\n",
            "POST /a HTTP/1.1\r\nHost: x\r\nContent-Length: 5, 5\r\n\r\n",
            "POST /a HTTP/1.1\r\nHost: x\r\nContent-Length: +5\r\n\r\n",
            "POST /a HTTP/1.1\r\nHost: x\r\nContent-Length: 18446744073709551615\r\n\r\n",
        ] {
            // Nothing after a terminal head is inspected.
            let stream = format!("{head}GET /b HTTP/1.1\r\nHost: x\r\n\r\n");
            assert_verdicts(stream.as_bytes(), &[Terminal]);
        }
    }

    #[test]
    fn identical_duplicate_content_lengths_are_followed_like_hyper() {
        assert_verdicts(
            b"POST /a HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\nContent-Length: 2\r\n\r\nokGET /b HTTP/1.1\r\nHost: x\r\n\r\n",
            &[Persistent, Persistent],
        );
    }

    #[test]
    fn malformed_heads_are_terminal() {
        assert_verdicts(
            b"GET /a HTTP/1.1\r\nHost: x\r\n\r\nGET /b HTTP/1.1\r\nBad Header: x\r\n\r\nGET /c HTTP/1.1\r\n\r\n",
            &[Persistent, Terminal],
        );
    }

    #[test]
    fn too_many_headers_for_hyper_are_terminal() {
        let verdicts = Arc::new(Http1Verdicts::default());
        let mut io = Http1InspectedIo::new((), Arc::clone(&verdicts), MAX_HEAD, 2);
        io.observe(b"GET /a HTTP/1.1\r\nHost: x\r\nA: 1\r\nB: 2\r\n\r\n");
        assert_eq!(verdicts.next(), Terminal);
    }

    #[test]
    fn oversized_partial_heads_are_terminal_and_stop_buffering() {
        let verdicts = Arc::new(Http1Verdicts::default());
        let mut io = Http1InspectedIo::new((), Arc::clone(&verdicts), 64, 100);
        io.observe(b"GET /a HTTP/1.1\r\nHost: x\r\n");
        io.observe(&[b'a'; 64]);
        assert_eq!(verdicts.next(), Terminal);
        assert!(matches!(io.state, InspectState::Passthrough));
        assert!(io.head.is_empty());
        io.observe(b"GET /b HTTP/1.1\r\nHost: x\r\n\r\n");
        assert_eq!(verdicts.next(), Unclassified, "empty queue fails closed");
    }

    #[test]
    fn verdict_queue_is_bounded_and_fails_closed() {
        let verdicts = Arc::new(Http1Verdicts::default());
        let mut io = Http1InspectedIo::new((), Arc::clone(&verdicts), MAX_HEAD, 100);
        let request = b"GET / HTTP/1.1\r\nHost: x\r\n\r\n";
        let stream: Vec<u8> = request
            .iter()
            .copied()
            .cycle()
            .take(request.len() * (MAX_PENDING_VERDICTS + 10))
            .collect();
        io.observe(&stream);
        for _ in 0..MAX_PENDING_VERDICTS {
            assert_eq!(verdicts.next(), Persistent);
        }
        assert_eq!(verdicts.next(), Unclassified);
        assert!(matches!(io.state, InspectState::Passthrough));
    }

    #[test]
    fn drip_fed_heads_are_parsed_once_per_terminator_not_per_read() {
        let verdicts = Arc::new(Http1Verdicts::default());
        let mut io = Http1InspectedIo::new((), Arc::clone(&verdicts), 64 * 1024, 100);
        let mut head = b"GET / HTTP/1.1\r\nHost: x\r\nX-Pad: ".to_vec();
        head.extend(std::iter::repeat_n(b'a', 30_000));
        head.extend_from_slice(b"\r\n\r\n");
        for byte in &head {
            io.observe(std::slice::from_ref(byte));
        }
        assert_eq!(verdicts.next(), Persistent);
        assert!(io.buffered_parses <= 2, "{} parses", io.buffered_parses);
    }

    #[test]
    fn drip_fed_blank_lines_do_not_trigger_repeated_parses() {
        let verdicts = Arc::new(Http1Verdicts::default());
        let mut io = Http1InspectedIo::new((), Arc::clone(&verdicts), 64 * 1024, 100);
        io.observe(b"GET /a HTTP/1.1\r\nHost: x\r\n\r\n");
        for _ in 0..20_000 {
            io.observe(b"\r");
            io.observe(b"\n");
        }
        assert!(io.head.is_empty());
        io.observe(b"GET /b HTTP/1.1\r\nHost: x\r\n\r\n");
        assert_eq!(verdicts.next(), Persistent);
        assert_eq!(verdicts.next(), Persistent);
        assert!(io.buffered_parses <= 2, "{} parses", io.buffered_parses);
    }

    #[test]
    fn content_length_parser_matches_hyper_strictness() {
        assert_eq!(parse_content_length(b"0"), Some(0));
        assert_eq!(parse_content_length(b"0042"), Some(42));
        assert_eq!(
            parse_content_length(b"18446744073709551613"),
            Some(MAX_DECODABLE_LENGTH)
        );
        for invalid in [
            &b""[..],
            b" 1",
            b"1 ",
            b"-1",
            b"+1",
            b"1,1",
            b"0x10",
            b"18446744073709551614",
            b"99999999999999999999",
        ] {
            assert_eq!(parse_content_length(invalid), None, "{invalid:?}");
        }
    }
}
