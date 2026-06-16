use std::error::Error;
use std::fmt::{Display, Formatter};
use std::pin::Pin;

use base64::Engine;
use futures_util::{Stream, StreamExt, stream};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Empty, Full, StreamBody};
use hyper::body::{Bytes, Frame};
use hyper::header::{
    CACHE_CONTROL, CONNECTION, CONTENT_TYPE, HeaderName, HeaderValue, SEC_WEBSOCKET_ACCEPT,
    SEC_WEBSOCKET_KEY, SET_COOKIE, UPGRADE,
};
use hyper::{HeaderMap, StatusCode};
use serde::Serialize;
use sha1::{Digest, Sha1};

use super::websocket::{ResolvedWebSocketConfig, validate_handshake};
use super::{BoxError, HttpError, IntoHttpError, Request, SseEvent, WebSocketConfig};

pub(crate) type ResponseBody = UnsyncBoxBody<Bytes, BoxError>;
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
            .header(CONNECTION.as_str(), "keep-alive")
    }

    /// Like [`Response::sse`], but whenever `events` stays quiet for
    /// `heartbeat` a `: keep-alive` comment is emitted so proxies do not drop
    /// the idle connection. The stream still ends when `events` ends.
    pub fn sse_with_heartbeat<S>(events: S, heartbeat: std::time::Duration) -> Self
    where
        S: Stream<Item = SseEvent> + Send + 'static,
    {
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

    pub fn redirect(location: &str) -> Self {
        Self::redirect_with_status(location, 302)
    }

    pub fn redirect_with_status(location: &str, status: u16) -> Self {
        Self::send("").status(status).header("location", location)
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
        let name = sanitize_cookie_part(name);
        let value = sanitize_cookie_part(value);
        self.append_header(
            SET_COOKIE.as_str(),
            &format!("{}={}; Path=/; HttpOnly", name, value),
        )
    }

    pub fn websocket(req: &Request) -> Result<Self, HttpError> {
        let defaults = WebSocketConfig::default();
        let config = ResolvedWebSocketConfig::from_layers(&defaults, &defaults);
        validate_handshake(req, &config).map_err(|rejection| rejection.into_http_error())?;

        let key = req
            .header(SEC_WEBSOCKET_KEY.as_str())
            .ok_or_else(|| HttpError::bad_request("Falta Sec-WebSocket-Key"))?;
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
            trailers,
            error: _,
            build_error,
        } = self;

        if let Some(error) = build_error {
            return invalid_response(error);
        }
        let status = match StatusCode::from_u16(status) {
            Ok(status) => status,
            Err(error) => return invalid_response(ResponseBuildError::invalid_status(error)),
        };
        let content_type = match HeaderValue::from_str(&content_type) {
            Ok(content_type) => content_type,
            Err(error) => return invalid_response(ResponseBuildError::invalid_header_value(error)),
        };
        if let Some(trailers) = &trailers {
            if let Err(error) = validate_trailers(trailers) {
                return invalid_response(error);
            }
        }

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
        if !headers.contains_key(CONTENT_TYPE) {
            builder = builder.header(CONTENT_TYPE, content_type);
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

fn body_with_trailers(stream: ResponseStream, trailers: Option<HeaderMap>) -> ResponseBody {
    let Some(trailers) = trailers else {
        return StreamBody::new(stream).boxed_unsync();
    };
    let trailers = stream::once(async move { Ok::<_, BoxError>(Frame::trailers(trailers)) });
    StreamBody::new(stream.chain(trailers)).boxed_unsync()
}

fn validate_trailers(trailers: &HeaderMap) -> Result<(), ResponseBuildError> {
    for name in trailers.keys() {
        if name.as_str().starts_with(':') {
            return Err(ResponseBuildError::invalid_trailer_name(name.as_str()));
        }
    }
    Ok(())
}

fn invalid_response(error: ResponseBuildError) -> hyper::Response<ResponseBody> {
    Response::from_error(
        HttpError::internal_server_error("No se pudo construir la respuesta").with_source(error),
    )
    .into_hyper()
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
            "El nombre de trailer de la respuesta no es valido: {name}"
        ))
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
