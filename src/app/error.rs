use std::borrow::Cow;
use std::error::Error;
use std::fmt::{Debug, Display};

use hyper::StatusCode;
use hyper::header::{HeaderMap, HeaderName, HeaderValue};

use super::Response;
use super::body::BodyLimitExceeded;

pub type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Debug, Default)]
struct HttpErrorDetails {
    source: Option<BoxError>,
    headers: HeaderMap,
    /// The rejected value was genuinely absent (see `OptionalRejection`).
    missing: bool,
}

#[derive(Debug)]
pub struct HttpError {
    status: StatusCode,
    code: Cow<'static, str>,
    public_message: Cow<'static, str>,
    details: Box<HttpErrorDetails>,
}

impl HttpError {
    pub fn new(
        status: StatusCode,
        code: impl Into<Cow<'static, str>>,
        public_message: impl Into<Cow<'static, str>>,
    ) -> Self {
        Self {
            status,
            code: code.into(),
            public_message: public_message.into(),
            details: Box::default(),
        }
    }

    pub fn bad_request(public_message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", public_message)
    }

    pub fn unauthorized(public_message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized", public_message)
    }

    pub fn forbidden(public_message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "forbidden", public_message)
    }

    pub fn not_found(public_message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", public_message)
    }

    pub fn method_not_allowed(public_message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
            public_message,
        )
    }

    pub fn payload_too_large(public_message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            public_message,
        )
    }

    pub fn payload_too_large_limit(limit: usize) -> Self {
        Self::payload_too_large(format!(
            "El cuerpo de la solicitud supera el limite de {limit} bytes"
        ))
    }

    pub fn invalid_content_length() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "invalid_content_length",
            "El encabezado Content-Length no es valido",
        )
    }

    pub fn duplicate_header(name: &str) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "duplicate_header",
            format!("El encabezado {name} debe aparecer como maximo una vez"),
        )
    }

    pub fn body_read(source: BoxError) -> Self {
        match source.downcast::<BodyLimitExceeded>() {
            Ok(error) => Self::payload_too_large_limit(error.limit()).with_source(error),
            Err(source) => Self::new(
                StatusCode::BAD_REQUEST,
                "body_read",
                "No se pudo leer el cuerpo de la solicitud",
            )
            .with_source(source),
        }
    }

    pub fn body_already_consumed() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "body_already_consumed",
            "El cuerpo de la solicitud ya fue consumido",
        )
    }

    pub fn body_not_buffered() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "body_not_buffered",
            "El cuerpo de la solicitud requiere lectura asincrona",
        )
    }

    pub fn invalid_utf8() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "invalid_utf8",
            "El cuerpo de la solicitud no es UTF-8 valido",
        )
    }

    pub fn invalid_json() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "invalid_json",
            "El cuerpo de la solicitud no contiene JSON valido",
        )
    }

    pub fn request_timeout(public_message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(
            StatusCode::REQUEST_TIMEOUT,
            "request_timeout",
            public_message,
        )
    }

    pub fn too_many_requests(public_message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            "too_many_requests",
            public_message,
        )
    }

    pub fn upgrade_required(public_message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(
            StatusCode::UPGRADE_REQUIRED,
            "upgrade_required",
            public_message,
        )
    }

    pub fn internal_server_error(public_message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_server_error",
            public_message,
        )
    }

    pub fn header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.details.headers.insert(name, value);
        self
    }

    pub fn append_header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.details.headers.append(name, value);
        self
    }

    /// Marks this rejection as describing a value that is genuinely absent
    /// (no header, no extension, no body), so an `Option<E>` extractor
    /// yields `None` instead of propagating it. Malformed input must not be
    /// marked.
    pub fn missing(mut self) -> Self {
        self.details.missing = true;
        self
    }

    /// Whether [`HttpError::missing`] marked this rejection as an absence.
    pub fn is_missing(&self) -> bool {
        self.details.missing
    }

    pub fn with_source<E>(mut self, source: E) -> Self
    where
        E: Into<BoxError>,
    {
        self.details.source = Some(source.into());
        self
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn public_message(&self) -> &str {
        &self.public_message
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.details.headers
    }

    pub fn source(&self) -> Option<&(dyn Error + Send + Sync + 'static)> {
        self.details.source.as_deref()
    }

    #[deprecated(since = "0.3.0", note = "use public_message() instead")]
    pub fn message(&self) -> &str {
        self.public_message()
    }
}

impl Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.status.as_u16(), self.public_message)
    }
}

impl Error for HttpError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.details
            .source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

pub trait IntoHttpError {
    fn into_http_error(self) -> HttpError;
}

#[derive(Debug)]
struct HandlerMessageError(String);

impl Display for HandlerMessageError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for HandlerMessageError {}

fn private_handler_error(message: String) -> HttpError {
    HttpError::internal_server_error("Error interno del servidor")
        .with_source(HandlerMessageError(message))
}

impl IntoHttpError for HttpError {
    fn into_http_error(self) -> HttpError {
        self
    }
}

impl IntoHttpError for &'static str {
    fn into_http_error(self) -> HttpError {
        private_handler_error(self.to_string())
    }
}

impl IntoHttpError for String {
    fn into_http_error(self) -> HttpError {
        private_handler_error(self)
    }
}

impl From<HttpError> for Response {
    fn from(value: HttpError) -> Self {
        Response::from_error(value)
    }
}
