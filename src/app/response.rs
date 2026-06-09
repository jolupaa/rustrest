use std::convert::Infallible;
use std::pin::Pin;

use base64::Engine;
use futures_util::{Stream, StreamExt};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Empty, Full, StreamBody};
use hyper::HeaderMap;
use hyper::body::{Bytes, Frame};
use hyper::header::{
    CACHE_CONTROL, CONNECTION, CONTENT_TYPE, HeaderName, HeaderValue, SEC_WEBSOCKET_ACCEPT,
    SEC_WEBSOCKET_KEY, SET_COOKIE, UPGRADE,
};
use serde::Serialize;
use sha1::{Digest, Sha1};

use super::{HttpError, IntoHttpError, Request, SseEvent};

pub(crate) type ResponseBody = UnsyncBoxBody<Bytes, Infallible>;
type ResponseStream = Pin<Box<dyn Stream<Item = Result<Frame<Bytes>, Infallible>> + Send>>;

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
    error: Option<HttpError>,
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
            error: None,
        }
    }

    pub fn stream<S>(stream: S) -> Self
    where
        S: Stream<Item = Result<Bytes, Infallible>> + Send + 'static,
    {
        let frames = stream.map(|chunk| chunk.map(Frame::data));
        Self {
            status: 200,
            content_type: "application/octet-stream".to_string(),
            headers: HeaderMap::new(),
            body_kind: BodyKind::Stream(Box::pin(frames)),
            error: None,
        }
    }

    pub fn sse<S>(events: S) -> Self
    where
        S: Stream<Item = SseEvent> + Send + 'static,
    {
        let chunks = events.map(|event| Ok(Bytes::from(event.format())));
        Self::stream(chunks)
            .content_type("text/event-stream")
            .header(CACHE_CONTROL.as_str(), "no-cache")
            .header(CONNECTION.as_str(), "keep-alive")
    }

    /// Serializes `value` to JSON. If serialization fails, degrades to a 500.
    pub fn json<T: Serialize>(value: &T) -> Self {
        match serde_json::to_string(value) {
            Ok(body) => Self::bytes(Bytes::from(body), "application/json"),
            Err(_) => Self::internal_server_error(),
        }
    }

    pub fn not_found() -> Self {
        Self::bytes(
            Bytes::from_static(b"404 Not Found"),
            "text/plain; charset=utf-8",
        )
        .status(404)
    }

    pub fn bad_request() -> Self {
        Self::bytes(
            Bytes::from_static(b"400 Bad Request"),
            "text/plain; charset=utf-8",
        )
        .status(400)
    }

    pub fn internal_server_error() -> Self {
        Self::from_error(HttpError::internal_server_error("Internal Server Error"))
    }

    pub fn from_error(error: HttpError) -> Self {
        let status = error.status();
        let body = format!("{} {}", status, error.message());
        let mut response =
            Self::bytes(Bytes::from(body), "text/plain; charset=utf-8").status(status);
        response.error = Some(error);
        response
    }

    pub fn redirect(location: &str) -> Self {
        Self::redirect_with_status(location, 302)
    }

    pub fn redirect_with_status(location: &str, status: u16) -> Self {
        Self::send("").status(status).header("location", location)
    }

    pub fn status(mut self, status: u16) -> Self {
        self.status = status;
        self
    }

    pub fn content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.set_header(name, value);
        self
    }

    pub fn append_header(mut self, name: &str, value: &str) -> Self {
        self.add_header(name, value);
        self
    }

    pub fn cookie(self, name: &str, value: &str) -> Self {
        let name = sanitize_cookie_part(name);
        let value = sanitize_cookie_part(value);
        self.append_header(
            SET_COOKIE.as_str(),
            &format!("{}={}; Path=/; HttpOnly", name, value),
        )
    }

    pub fn websocket(req: &Request) -> Result<Self, HttpError> {
        if !req.is_websocket_upgrade() {
            return Err(HttpError::bad_request("Invalid WebSocket upgrade"));
        }

        let key = req
            .header(SEC_WEBSOCKET_KEY.as_str())
            .ok_or_else(|| HttpError::bad_request("Missing Sec-WebSocket-Key"))?;
        let mut hasher = Sha1::new();
        hasher.update(key.as_bytes());
        hasher.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
        let accept = base64::engine::general_purpose::STANDARD.encode(hasher.finalize());

        Ok(Self::send("")
            .status(101)
            .header(UPGRADE.as_str(), "websocket")
            .header(CONNECTION.as_str(), "Upgrade")
            .header(SEC_WEBSOCKET_ACCEPT.as_str(), &accept))
    }

    fn set_header(&mut self, name: &str, value: &str) {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            self.headers.insert(name, value);
        }
    }

    fn add_header(&mut self, name: &str, value: &str) {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            self.headers.append(name, value);
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

    pub(crate) fn map_body_bytes<F>(&mut self, mapper: F) -> Result<(), HttpError>
    where
        F: FnOnce(&[u8]) -> Result<Vec<u8>, HttpError>,
    {
        if let BodyKind::Bytes(bytes) = &self.body_kind {
            let mapped = Bytes::from(mapper(bytes)?);
            self.body_kind = BodyKind::Bytes(mapped);
        }
        Ok(())
    }

    pub(crate) fn take_error(&mut self) -> Option<HttpError> {
        self.error.take()
    }

    /// Converts our framework response into a hyper response.
    pub(crate) fn into_hyper(self) -> hyper::Response<ResponseBody> {
        let Response {
            status,
            content_type,
            headers,
            body_kind,
            error: _,
        } = self;

        let hyper_body = match body_kind {
            BodyKind::Bytes(bytes) => Full::new(bytes).boxed_unsync(),
            BodyKind::Stream(stream) => StreamBody::new(stream).boxed_unsync(),
            BodyKind::Empty => Empty::<Bytes>::new().boxed_unsync(),
        };

        let mut builder = hyper::Response::builder().status(status);
        if !headers.contains_key(CONTENT_TYPE) {
            builder = builder.header(CONTENT_TYPE, content_type);
        }
        for (name, value) in &headers {
            builder = builder.header(name, value);
        }

        builder
            .body(hyper_body)
            .expect("status and headers are always valid")
    }
}

fn sanitize_cookie_part(value: &str) -> String {
    value
        .chars()
        .filter(|ch| !matches!(ch, ';' | ',' | '\r' | '\n'))
        .collect()
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
                eprintln!("Handler returned error: {}", err);
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
