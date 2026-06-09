use std::fmt::Display;

use super::Response;

#[derive(Clone, Debug)]
pub struct HttpError {
    status: u16,
    message: String,
}

impl HttpError {
    pub fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(400, message)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(401, message)
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(403, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(404, message)
    }

    pub fn internal_server_error(message: impl Into<String>) -> Self {
        Self::new(500, message)
    }

    pub fn status(&self) -> u16 {
        self.status
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.status, self.message)
    }
}

pub trait IntoHttpError {
    fn into_http_error(self) -> HttpError;
}

impl IntoHttpError for HttpError {
    fn into_http_error(self) -> HttpError {
        self
    }
}

impl IntoHttpError for &'static str {
    fn into_http_error(self) -> HttpError {
        HttpError::internal_server_error(self)
    }
}

impl IntoHttpError for String {
    fn into_http_error(self) -> HttpError {
        HttpError::internal_server_error(self)
    }
}

impl From<HttpError> for Response {
    fn from(value: HttpError) -> Self {
        Response::from_error(value)
    }
}
