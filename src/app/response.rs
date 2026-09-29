use std::error::Error;
use std::fmt::{Display, Formatter};
use std::pin::Pin;

use base64::Engine;
use futures_util::{Stream, StreamExt, stream};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Empty, Full, StreamBody};
use hyper::body::{Bytes, Frame};
use hyper::header::{
    CACHE_CONTROL, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HeaderName, HeaderValue,
    SEC_WEBSOCKET_ACCEPT, SEC_WEBSOCKET_KEY, TE, TRAILER, TRANSFER_ENCODING, UPGRADE,
};
use hyper::{HeaderMap, StatusCode};
use serde::Serialize;
use sha1::{Digest, Sha1};

use super::cookie::Cookie;
use super::websocket::{ResolvedWebSocketConfig, validate_handshake};
use super::{BoxError, HttpError, IntoHttpError, Request, SseEvent, WebSocketConfig};

pub(crate) type ResponseBody = UnsyncBoxBody<Bytes, BoxError>;

/// Percent-encodes the bytes of `location` that may not appear in a
/// URI-reference, like Express's `encodeurl`: reserved and unreserved
/// characters and valid `%XX` escapes are kept, everything else (spaces,
/// controls, non-ASCII UTF-8) is encoded.
fn encode_location(location: &str) -> String {
    let bytes = location.as_bytes();
    let mut encoded = String::with_capacity(location.len());
    for (index, &byte) in bytes.iter().enumerate() {
        let is_escape = byte == b'%'
            && bytes
                .get(index + 1..index + 3)
                .is_some_and(|hex| hex.iter().all(u8::is_ascii_hexdigit));
        let allowed =
            byte.is_ascii_alphanumeric() || b"-._~:/?#[]@!$&'()*+,;=".contains(&byte) || is_escape;
        if allowed {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}
type ResponseStream = Pin<Box<dyn Stream<Item = Result<Frame<Bytes>, BoxError>> + Send>>;

enum BodyKind {
    Bytes(Bytes),
    Stream(ResponseStream),
    Empty,
}

pub struct Response {
    pub status: u16,
    pub content_type: String,
    pub headers: HeaderMap,
    body_kind: BodyKind,
    trailers: Option<HeaderMap>,
    error: Option<HttpError>,
    build_error: Option<ResponseBuildError>,
    websocket_upgrade_authorized: bool,
}

impl Response {
    pub fn send(text: &str) -> Self {
        Self::bytes(Bytes::from(text.to_string()), "text/plain; charset=utf-8")
    }

    pub fn bytes(bytes: Bytes, content_type: impl Into<String>) -> Self {
        Self {
            status: 200,
            content_type: content_type.into(),
            headers: HeaderMap::new(),
            body_kind: BodyKind::Bytes(bytes),
            trailers: None,
            error: None,
            build_error: None,
            websocket_upgrade_authorized: false,
        }
    }

    pub fn stream<S, E>(stream: S) -> Self
    where
        S: Stream<Item = Result<Bytes, E>> + Send + 'static,
        E: Into<BoxError> + 'static,
    {
        let frames = stream.map(|chunk| chunk.map(Frame::data).map_err(Into::into));
        Self::from_frame_stream(frames)
    }

    fn from_frame_stream<S>(stream: S) -> Self
    where
        S: Stream<Item = Result<Frame<Bytes>, BoxError>> + Send + 'static,
    {
        Self {
            status: 200,
            content_type: "application/octet-stream".to_string(),
            headers: HeaderMap::new(),
            body_kind: BodyKind::Stream(Box::pin(stream)),
            trailers: None,
            error: None,
            build_error: None,
            websocket_upgrade_authorized: false,
        }
    }

    pub fn sse<S>(events: S) -> Self
    where
        S: Stream<Item = SseEvent> + Send + 'static,
    {
        let chunks = events.map(|event| event.try_format().map(Bytes::from));
        Self::stream(chunks)
            .content_type("text/event-stream")
            .header(CACHE_CONTROL.as_str(), "no-cache")
    }

    /// Like [`Response::sse`], but whenever `events` stays quiet for
    /// `heartbeat` a `: keep-alive` comment is emitted so proxies do not drop
    /// the idle connection. The stream still ends when `events` ends.
    pub fn sse_with_heartbeat<S>(events: S, heartbeat: std::time::Duration) -> Self
    where
        S: Stream<Item = SseEvent> + Send + 'static,
    {
        if heartbeat.is_zero() {
            return Self::sse(events);
        }
        let merged = futures_util::stream::unfold(Box::pin(events), move |mut events| async move {
            match tokio::time::timeout(heartbeat, events.next()).await {
                Ok(Some(event)) => Some((event, events)),
                Ok(None) => None,
                Err(_) => Some((SseEvent::comment("keep-alive"), events)),
            }
        });
        Self::sse(merged)
    }

    /// Serializes `value` to JSON. If serialization fails, degrades to a 500.
    pub fn json<T: Serialize>(value: &T) -> Self {
        match serde_json::to_string(value) {
            Ok(body) => Self::bytes(Bytes::from(body), "application/json"),
            Err(error) => Self::from_error(
                HttpError::internal_server_error("Internal Server Error").with_source(error),
            ),
        }
    }

    pub fn not_found() -> Self {
        Self::from_error(HttpError::not_found("Not Found"))
    }

    pub fn bad_request() -> Self {
        Self::from_error(HttpError::bad_request("Bad Request"))
    }

    pub fn internal_server_error() -> Self {
        Self::from_error(HttpError::internal_server_error("Internal Server Error"))
    }

    pub fn from_error(error: HttpError) -> Self {
        let status = error.status();
        let body = serde_json::json!({
            "type": format!("about:blank#{}", error.code()),
            "title": status.canonical_reason().unwrap_or("HTTP Error"),
            "status": status.as_u16(),
            "detail": error.public_message(),
            "code": error.code(),
        })
        .to_string();
        let mut response =
            Self::bytes(Bytes::from(body), "application/problem+json").status(status.as_u16());
        response.headers = error.headers().clone();
        response.error = Some(error);
        response
    }

    /// `302 Found` to `location`. Characters that are not valid in a
    /// URI-reference (RFC 3986), such as spaces or non-ASCII text, are
    /// percent-encoded; existing `%XX` escapes are preserved.
    pub fn redirect(location: &str) -> Self {
        Self::redirect_with_status(location, 302)
    }

    /// Like [`Response::redirect`] with an explicit status. A status outside
    /// `300..=399` is recorded as a construction error and rendered as `500`.
    pub fn redirect_with_status(location: &str, status: u16) -> Self {
        let response = Self::send("")
            .status(status)
            .header("location", &encode_location(location));
        if (300..=399).contains(&status) {
            response
        } else {
            let mut response = response;
            response.record_build_error(ResponseBuildError::invalid_status(format!(
                "{status} no es un estado de redireccion"
            )));
            response
        }
    }

    pub fn status(mut self, status: u16) -> Self {
        match StatusCode::from_u16(status) {
            Ok(status) => self.status = status.as_u16(),
            Err(error) => self.record_build_error(ResponseBuildError::invalid_status(error)),
        }
        self
    }

    pub fn try_status(mut self, status: u16) -> Result<Self, ResponseBuildError> {
        let status = StatusCode::from_u16(status).map_err(ResponseBuildError::invalid_status)?;
        self.status = status.as_u16();
        Ok(self)
    }

    pub fn content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        if let Err(error) = self.try_set_header(name, value) {
            self.record_build_error(error);
        }
        self
    }

    pub fn try_header<N, V>(mut self, name: N, value: V) -> Result<Self, ResponseBuildError>
    where
        N: TryInto<HeaderName>,
        N::Error: Into<BoxError>,
        V: TryInto<HeaderValue>,
        V::Error: Into<BoxError>,
    {
        self.headers.insert(
            name.try_into()
                .map_err(ResponseBuildError::invalid_header_name)?,
            value
                .try_into()
                .map_err(ResponseBuildError::invalid_header_value)?,
        );
        Ok(self)
    }

    pub fn append_header(mut self, name: &str, value: &str) -> Self {
        if let Err(error) = self.try_add_header(name, value) {
            self.record_build_error(error);
        }
        self
    }

    pub fn try_append_header<N, V>(mut self, name: N, value: V) -> Result<Self, ResponseBuildError>
    where
        N: TryInto<HeaderName>,
        N::Error: Into<BoxError>,
        V: TryInto<HeaderValue>,
        V::Error: Into<BoxError>,
    {
        self.headers.append(
            name.try_into()
                .map_err(ResponseBuildError::invalid_header_name)?,
            value
                .try_into()
                .map_err(ResponseBuildError::invalid_header_value)?,
        );
        Ok(self)
    }

    pub fn with_trailers(mut self, trailers: HeaderMap) -> Self {
        if let Err(error) = validate_trailers(&trailers) {
            self.record_build_error(error);
        } else {
            self.trailers = Some(trailers);
        }
        self
    }

    pub fn try_with_trailers(mut self, trailers: HeaderMap) -> Result<Self, ResponseBuildError> {
        validate_trailers(&trailers)?;
        self.trailers = Some(trailers);
        Ok(self)
    }

    pub fn cookie(self, name: &str, value: &str) -> Self {
        self.set_cookie(Cookie::new(name, value).http_only(true))
    }

    #[deprecated(
        since = "0.4.0",
        note = "this helper cannot own Hyper's upgraded stream; use App::websocket, Router::websocket, or Request::websocket"
    )]
    pub fn websocket(req: &Request) -> Result<Self, HttpError> {
        let defaults = WebSocketConfig::default();
        let config = ResolvedWebSocketConfig::from_layers(&defaults, &defaults);
        validate_handshake(req, &config).map_err(|rejection| rejection.into_http_error())?;
        if req.upgrade.is_some() {
            return Err(HttpError::internal_server_error(
                "Response::websocket no puede poseer el transporte actualizado; use una ruta WebSocket",
            ));
        }

        let key = req
            .header(SEC_WEBSOCKET_KEY.as_str())
            .ok_or_else(|| HttpError::bad_request("Falta Sec-WebSocket-Key"))?;
        let accept = websocket_accept(key);

        Ok(Self::send("")
            .status(101)
            .header(UPGRADE.as_str(), "websocket")
            .header(CONNECTION.as_str(), "Upgrade")
            .header(SEC_WEBSOCKET_ACCEPT.as_str(), &accept))
    }

    fn try_set_header(&mut self, name: &str, value: &str) -> Result<(), ResponseBuildError> {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(ResponseBuildError::invalid_header_name)?;
        let value =
            HeaderValue::from_str(value).map_err(ResponseBuildError::invalid_header_value)?;
        self.headers.insert(name, value);
        Ok(())
    }

    fn try_add_header(&mut self, name: &str, value: &str) -> Result<(), ResponseBuildError> {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(ResponseBuildError::invalid_header_name)?;
        let value =
            HeaderValue::from_str(value).map_err(ResponseBuildError::invalid_header_value)?;
        self.headers.append(name, value);
        Ok(())
    }

    fn record_build_error(&mut self, error: ResponseBuildError) {
        if self.build_error.is_none() {
            self.build_error = Some(error);
        }
    }

    /// Returns the response body bytes when it is an in-memory body.
    /// Streamed and empty bodies return `None` (nothing to read up-front).
    pub fn body_bytes(&self) -> Option<&[u8]> {
        match &self.body_kind {
            BodyKind::Bytes(bytes) => Some(bytes),
            BodyKind::Stream(_) | BodyKind::Empty => None,
        }
    }

    /// Returns the in-memory body as a lossy UTF-8 view (empty for streams).
    pub fn body_text(&self) -> std::borrow::Cow<'_, str> {
        match &self.body_kind {
            BodyKind::Bytes(bytes) => String::from_utf8_lossy(bytes),
            BodyKind::Stream(_) | BodyKind::Empty => std::borrow::Cow::Borrowed(""),
        }
    }

    pub(crate) fn clear_body(&mut self) {
        self.body_kind = BodyKind::Empty;
    }

    pub(crate) fn clear_body_and_trailers(&mut self) {
        self.clear_body();
        self.trailers = None;
        self.headers.remove(TRAILER);
    }

    pub(crate) fn has_trailers(&self) -> bool {
        self.trailers.is_some()
    }

    /// Removes connection-specific response fields before HTTP/2 emission.
    /// A trailer nominated by `Connection` is rejected instead of becoming an
    /// end-to-end field after the nominating header is stripped.
    pub(crate) fn strip_http2_connection_headers(&mut self) {
        let nominated = connection_nominated_headers(&self.headers);
        if let Some(trailers) = &self.trailers {
            if let Some(name) = nominated.iter().find(|name| trailers.contains_key(*name)) {
                self.record_build_error(ResponseBuildError::invalid_trailer_name(name.as_str()));
            }
        }

        self.headers.remove(CONNECTION);
        self.headers.remove(UPGRADE);
        self.headers.remove(TE);
        self.headers.remove("keep-alive");
        self.headers.remove("proxy-connection");
        for name in nominated {
            self.headers.remove(name);
        }
    }

    /// Removes a `HEAD` response body while retaining the representation
    /// length a corresponding `GET` would have sent. Explicit lengths are
    /// validated against buffered bodies before the bytes are discarded.
    pub(crate) fn prepare_for_head(&mut self) {
        // A HEAD response never carries a content section, including trailer
        // fields that would otherwise follow the body stream.
        self.trailers = None;
        self.headers.remove(TRAILER);
        let Ok(status) = StatusCode::from_u16(self.status) else {
            self.clear_body();
            return;
        };
        if status_has_no_content(status) {
            self.clear_body();
            return;
        }

        self.preserve_buffered_representation_length();
        self.clear_body();
    }

    /// Converts a selected buffered representation into a `304` response
    /// without losing the bytes needed to validate or derive Content-Length.
    pub(crate) fn prepare_for_not_modified(&mut self) {
        self.validate_selected_representation_before_discard();
        self.clear_body_and_trailers();
    }

    fn validate_selected_representation_before_discard(&mut self) {
        if let Some(trailers) = &self.trailers {
            if let Err(error) = validate_response_trailers(&self.headers, trailers) {
                self.record_build_error(error);
            }
        } else if self.headers.contains_key(TRAILER) {
            self.record_build_error(ResponseBuildError::trailer_without_fields());
        }

        if self.headers.contains_key(TRANSFER_ENCODING) {
            self.record_build_error(ResponseBuildError::explicit_transfer_encoding());
        }

        let content_length = match parse_content_length(&mut self.headers) {
            Ok(content_length) => content_length,
            Err(error) => {
                self.record_build_error(error);
                None
            }
        };
        if content_length.is_some() && self.trailers.is_some() {
            self.record_build_error(ResponseBuildError::content_length_with_trailers());
        }

        if let BodyKind::Bytes(bytes) = &self.body_kind {
            let actual = bytes.len() as u64;
            match content_length {
                Some(declared) if declared != actual => {
                    self.record_build_error(ResponseBuildError::content_length_mismatch(
                        declared, actual,
                    ));
                }
                Some(_) => {}
                None => {
                    self.headers.insert(
                        CONTENT_LENGTH,
                        HeaderValue::from_str(&actual.to_string())
                            .expect("a decimal content length is a valid header value"),
                    );
                }
            }
        }

        if !self.headers.contains_key(CONTENT_TYPE) {
            if let Err(error) = HeaderValue::from_str(&self.content_type) {
                self.record_build_error(ResponseBuildError::invalid_header_value(error));
            }
        }
    }

    fn preserve_buffered_representation_length(&mut self) {
        let BodyKind::Bytes(bytes) = &self.body_kind else {
            return;
        };
        let actual = bytes.len() as u64;
        match parse_content_length(&mut self.headers) {
            Ok(Some(declared)) if declared != actual => {
                self.record_build_error(ResponseBuildError::content_length_mismatch(
                    declared, actual,
                ));
            }
            Ok(Some(_)) => {}
            Ok(None) => {
                self.headers.insert(
                    CONTENT_LENGTH,
                    HeaderValue::from_str(&actual.to_string())
                        .expect("a decimal content length is a valid header value"),
                );
            }
            Err(error) => self.record_build_error(error),
        }
    }

    /// Replaces an existing buffered body and returns `false` for streamed or
    /// empty responses. This lets middleware transform bytes without exposing
    /// the internal body representation.
    pub(crate) fn replace_body_bytes(&mut self, bytes: Bytes) -> bool {
        if !matches!(self.body_kind, BodyKind::Bytes(_)) {
            return false;
        }
        self.body_kind = BodyKind::Bytes(bytes);
        true
    }

    pub(crate) fn take_error(&mut self) -> Option<HttpError> {
        self.error.take()
    }

    pub(crate) fn authorize_websocket_upgrade(&mut self) {
        self.websocket_upgrade_authorized = true;
    }

    pub(crate) fn websocket_upgrade_authorized(&self) -> bool {
        self.websocket_upgrade_authorized
    }

    /// Applies the same status/body/framing validation used at the network
    /// boundary while retaining the framework-native response type.
    pub(crate) fn finalize(mut self) -> Self {
        if let Some(error) = self.build_error.take() {
            return invalid_response_value(error);
        }
        let status = match StatusCode::from_u16(self.status) {
            Ok(status) => status,
            Err(error) => {
                return invalid_response_value(ResponseBuildError::invalid_status(error));
            }
        };
        if status.is_informational() && status != StatusCode::SWITCHING_PROTOCOLS {
            return invalid_response_value(ResponseBuildError::interim_status(status));
        }
        if status == StatusCode::SWITCHING_PROTOCOLS
            && !valid_websocket_switching_protocols(&self.headers)
        {
            return invalid_response_value(ResponseBuildError::invalid_switching_protocols());
        }
        if let Some(trailers) = &self.trailers {
            if let Err(error) = validate_response_trailers(&self.headers, trailers) {
                return invalid_response_value(error);
            }
            let trailer_names = trailers
                .keys()
                .map(HeaderName::as_str)
                .collect::<Vec<_>>()
                .join(", ");
            let trailer_names = HeaderValue::from_str(&trailer_names)
                .expect("validated trailer names form a valid header value");
            self.headers.insert(TRAILER, trailer_names);
        } else if self.headers.contains_key(TRAILER) {
            return invalid_response_value(ResponseBuildError::trailer_without_fields());
        }
        if let Err(error) = finalize_framing(&mut self, status) {
            return invalid_response_value(error);
        }
        if !status_has_no_content(status) && !self.headers.contains_key(CONTENT_TYPE) {
            let content_type = match HeaderValue::from_str(&self.content_type) {
                Ok(content_type) => content_type,
                Err(error) => {
                    return invalid_response_value(ResponseBuildError::invalid_header_value(error));
                }
            };
            self.headers.insert(CONTENT_TYPE, content_type);
        }
        if !status_has_no_content(status)
            && !self.headers.contains_key(CONTENT_LENGTH)
            && self.trailers.is_none()
        {
            if let BodyKind::Bytes(bytes) = &self.body_kind {
                self.headers.insert(
                    CONTENT_LENGTH,
                    HeaderValue::from_str(&bytes.len().to_string())
                        .expect("a decimal content length is a valid header value"),
                );
            }
        }
        self
    }

    /// Converts our framework response into a hyper response.
    pub(crate) fn into_hyper(self) -> hyper::Response<ResponseBody> {
        let response = self.finalize();
        let Response {
            status,
            content_type,
            headers,
            body_kind,
            trailers,
            error: _,
            build_error: _,
            websocket_upgrade_authorized: _,
        } = response;
        let status =
            StatusCode::from_u16(status).expect("a finalized response always has a valid status");

        let hyper_body = match (body_kind, trailers) {
            (BodyKind::Bytes(bytes), None) => Full::new(bytes)
                .map_err(|never| match never {})
                .boxed_unsync(),
            (BodyKind::Stream(stream), None) => StreamBody::new(stream).boxed_unsync(),
            (BodyKind::Empty, None) => Empty::<Bytes>::new()
                .map_err(|never| match never {})
                .boxed_unsync(),
            (BodyKind::Bytes(bytes), trailers) => {
                let stream = Box::pin(stream::once(async move {
                    Ok::<_, BoxError>(Frame::data(bytes))
                }));
                body_with_trailers(stream, trailers)
            }
            (BodyKind::Stream(stream), trailers) => body_with_trailers(stream, trailers),
            (BodyKind::Empty, trailers) => {
                let stream = Box::pin(stream::empty());
                body_with_trailers(stream, trailers)
            }
        };

        let mut builder = hyper::Response::builder().status(status);
        if !status_has_no_content(status) && !headers.contains_key(CONTENT_TYPE) {
            builder = builder.header(
                CONTENT_TYPE,
                HeaderValue::from_str(&content_type)
                    .expect("a finalized response always has a valid content type"),
            );
        }
        for (name, value) in &headers {
            builder = builder.header(name, value);
        }

        match builder.body(hyper_body) {
            Ok(response) => response,
            Err(error) => invalid_response(ResponseBuildError::builder(error)),
        }
    }
}

pub(crate) fn websocket_accept(key: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(key.as_bytes());
    hasher.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    base64::engine::general_purpose::STANDARD.encode(hasher.finalize())
}

fn body_with_trailers(stream: ResponseStream, trailers: Option<HeaderMap>) -> ResponseBody {
    let Some(trailers) = trailers else {
        return StreamBody::new(stream).boxed_unsync();
    };
    let trailers = stream::once(async move { Ok::<_, BoxError>(Frame::trailers(trailers)) });
    StreamBody::new(stream.chain(trailers)).boxed_unsync()
}

fn validate_trailers(trailers: &HeaderMap) -> Result<(), ResponseBuildError> {
    for name in trailers.keys() {
        if is_forbidden_trailer(name) {
            return Err(ResponseBuildError::invalid_trailer_name(name.as_str()));
        }
    }
    Ok(())
}

fn validate_response_trailers(
    headers: &HeaderMap,
    trailers: &HeaderMap,
) -> Result<(), ResponseBuildError> {
    validate_trailers(trailers)?;
    for name in connection_nominated_headers(headers) {
        if trailers.contains_key(&name) {
            return Err(ResponseBuildError::invalid_trailer_name(name.as_str()));
        }
    }
    Ok(())
}

fn connection_nominated_headers(headers: &HeaderMap) -> Vec<HeaderName> {
    headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect()
}

fn is_forbidden_trailer(name: &HeaderName) -> bool {
    let name = name.as_str();
    if name.starts_with("access-control-")
        || name.starts_with("cross-origin-")
        || name.starts_with("proxy-")
        || name.starts_with("sec-websocket-")
    {
        return true;
    }
    matches!(
        name,
        "accept-ranges"
            | "age"
            | "allow"
            | "alt-svc"
            | "authorization"
            | "cache-control"
            | "connection"
            | "content-disposition"
            | "content-encoding"
            | "content-language"
            | "content-length"
            | "content-location"
            | "content-range"
            | "content-security-policy"
            | "content-type"
            | "cookie"
            | "date"
            | "expect"
            | "expires"
            | "host"
            | "if-match"
            | "if-modified-since"
            | "if-none-match"
            | "if-range"
            | "if-unmodified-since"
            | "keep-alive"
            | "last-modified"
            | "link"
            | "location"
            | "max-forwards"
            | "permissions-policy"
            | "pragma"
            | "range"
            | "referrer-policy"
            | "refresh"
            | "retry-after"
            | "server"
            | "set-cookie"
            | "strict-transport-security"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "vary"
            | "warning"
            | "www-authenticate"
            | "x-content-type-options"
            | "x-frame-options"
    )
}

fn valid_websocket_switching_protocols(headers: &HeaderMap) -> bool {
    response_header_contains_token(headers, CONNECTION, "upgrade")
        && response_header_contains_token(headers, UPGRADE, "websocket")
        && headers.get_all(SEC_WEBSOCKET_ACCEPT).iter().count() == 1
        && headers
            .get(SEC_WEBSOCKET_ACCEPT)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| {
                base64::engine::general_purpose::STANDARD
                    .decode(value.trim())
                    .ok()
            })
            .is_some_and(|accept| accept.len() == 20)
}

fn response_header_contains_token(
    headers: &HeaderMap,
    name: hyper::header::HeaderName,
    expected: &str,
) -> bool {
    headers.get_all(name).iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case(expected))
        })
    })
}

fn finalize_framing(response: &mut Response, status: StatusCode) -> Result<(), ResponseBuildError> {
    let content_length = parse_content_length(&mut response.headers)?;
    if response.headers.contains_key(TRANSFER_ENCODING) {
        return Err(ResponseBuildError::explicit_transfer_encoding());
    }
    if content_length.is_some() && response.trailers.is_some() {
        return Err(ResponseBuildError::content_length_with_trailers());
    }

    // A 304 may carry the length of the selected representation even though
    // it carries no content. When buffered bytes are available, validate that
    // metadata before discarding them.
    if status == StatusCode::NOT_MODIFIED {
        if let (Some(expected), BodyKind::Bytes(bytes)) = (content_length, &response.body_kind) {
            if expected != bytes.len() as u64 {
                return Err(ResponseBuildError::content_length_mismatch(
                    expected,
                    bytes.len() as u64,
                ));
            }
        }
    }

    if status_has_no_content(status) {
        response.clear_body();
        response.trailers = None;
        response.headers.remove(TRAILER);
        response.headers.remove(TRANSFER_ENCODING);
        if status != StatusCode::NOT_MODIFIED {
            response.headers.remove(CONTENT_LENGTH);
        }
        return Ok(());
    }

    if let (Some(expected), BodyKind::Bytes(bytes)) = (content_length, &response.body_kind) {
        if expected != bytes.len() as u64 {
            return Err(ResponseBuildError::content_length_mismatch(
                expected,
                bytes.len() as u64,
            ));
        }
    }
    Ok(())
}

fn parse_content_length(headers: &mut HeaderMap) -> Result<Option<u64>, ResponseBuildError> {
    let mut parsed = None;
    for value in headers.get_all(CONTENT_LENGTH).iter() {
        let value = value
            .to_str()
            .map_err(ResponseBuildError::invalid_content_length)?;
        for candidate in value.split(',') {
            let candidate = candidate.trim();
            if candidate.is_empty() || !candidate.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(ResponseBuildError::invalid_content_length_value(candidate));
            }
            let length = candidate
                .parse::<u64>()
                .map_err(ResponseBuildError::invalid_content_length)?;
            if parsed.is_some_and(|current| current != length) {
                return Err(ResponseBuildError::conflicting_content_length());
            }
            parsed = Some(length);
        }
    }
    if let Some(length) = parsed {
        headers.insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&length.to_string())
                .expect("a decimal content length is a valid header value"),
        );
    }
    Ok(parsed)
}

fn status_has_no_content(status: StatusCode) -> bool {
    status.is_informational()
        || matches!(
            status,
            StatusCode::NO_CONTENT | StatusCode::RESET_CONTENT | StatusCode::NOT_MODIFIED
        )
}

fn invalid_response(error: ResponseBuildError) -> hyper::Response<ResponseBody> {
    invalid_response_value(error).into_hyper()
}

fn invalid_response_value(error: ResponseBuildError) -> Response {
    Response::from_error(
        HttpError::internal_server_error("No se pudo construir la respuesta").with_source(error),
    )
    .finalize()
}

#[derive(Debug)]
pub struct ResponseBuildError {
    message: String,
    source: Option<BoxError>,
}

impl ResponseBuildError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }

    fn with_source<E>(message: impl Into<String>, source: E) -> Self
    where
        E: Into<BoxError>,
    {
        Self {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    pub fn invalid_status<E>(source: E) -> Self
    where
        E: Into<BoxError>,
    {
        Self::with_source("El estado HTTP de la respuesta no es valido", source)
    }

    pub fn invalid_header_name<E>(source: E) -> Self
    where
        E: Into<BoxError>,
    {
        Self::with_source(
            "El nombre de encabezado de la respuesta no es valido",
            source,
        )
    }

    pub fn invalid_header_value<E>(source: E) -> Self
    where
        E: Into<BoxError>,
    {
        Self::with_source(
            "El valor de encabezado de la respuesta no es valido",
            source,
        )
    }

    pub fn invalid_trailer_name(name: &str) -> Self {
        Self::new(format!(
            "El trailer de la respuesta no esta permitido: {name}"
        ))
    }

    pub fn invalid_content_length<E>(source: E) -> Self
    where
        E: Into<BoxError>,
    {
        Self::with_source("Content-Length de la respuesta no es valido", source)
    }

    pub fn invalid_content_length_value(value: &str) -> Self {
        Self::new(format!(
            "Content-Length de la respuesta no es valido: {value}"
        ))
    }

    pub fn conflicting_content_length() -> Self {
        Self::new("La respuesta contiene valores Content-Length en conflicto")
    }

    pub fn content_length_mismatch(declared: u64, actual: u64) -> Self {
        Self::new(format!(
            "Content-Length ({declared}) no coincide con el cuerpo ({actual})"
        ))
    }

    pub fn explicit_transfer_encoding() -> Self {
        Self::new(
            "Transfer-Encoding de la respuesta lo debe seleccionar automaticamente el servidor",
        )
    }

    pub fn content_length_with_trailers() -> Self {
        Self::new("Content-Length no se puede combinar con trailers de respuesta")
    }

    pub fn interim_status(status: StatusCode) -> Self {
        Self::new(format!(
            "El estado provisional {} no puede ser la respuesta final de un manejador",
            status.as_u16()
        ))
    }

    pub fn invalid_switching_protocols() -> Self {
        Self::new("La respuesta 101 no contiene un handshake WebSocket valido")
    }

    pub fn trailer_without_fields() -> Self {
        Self::new("El encabezado Trailer requiere campos trailer en la respuesta")
    }

    pub fn builder<E>(source: E) -> Self
    where
        E: Into<BoxError>,
    {
        Self::with_source("No se pudo construir la respuesta HTTP", source)
    }
}

impl Display for ResponseBuildError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for ResponseBuildError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

pub trait IntoResponse {
    fn into_response(self) -> Response;
}

impl IntoResponse for Response {
    fn into_response(self) -> Response {
        self
    }
}

impl<E> IntoResponse for Result<Response, E>
where
    E: IntoHttpError,
{
    fn into_response(self) -> Response {
        match self {
            Ok(response) => response,
            Err(err) => {
                let err = err.into_http_error();
                // Client errors are expected outcomes, not server faults.
                if err.status().is_server_error() {
                    match err.source() {
                        Some(source) => super::log::log_error!(
                            "El manejador devolvio un error {}: {} ({})",
                            err.status().as_u16(),
                            err.public_message(),
                            source
                        ),
                        None => super::log::log_error!(
                            "El manejador devolvio un error {}: {}",
                            err.status().as_u16(),
                            err.public_message()
                        ),
                    }
                }
                Response::from_error(err)
            }
        }
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        Response::from_error(self)
    }
}
