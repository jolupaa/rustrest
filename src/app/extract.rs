use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use headers as typed_headers;
use hyper::body::Bytes;
use hyper::header::HeaderValue;
use hyper::{Method, StatusCode, Uri, Version};
use serde::de::DeserializeOwned;

use super::{HttpError, IntoResponse, Request, RequestParts};

pub trait FromRequestParts: Sized {
    type Rejection: IntoResponse + Send;

    fn from_request_parts(
        parts: &mut RequestParts<'_>,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send;
}

pub trait FromRequest: Sized {
    type Rejection: IntoResponse + Send;

    fn from_request(
        req: &mut Request,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send;
}

pub struct Json<T>(pub T);
pub struct Form<T>(pub T);
pub struct Path<T>(pub T);
pub struct Query<T>(pub T);
pub struct State<T>(pub Arc<T>);
pub struct Cookies<T>(pub T);
pub struct Headers<T>(pub T);
pub struct MatchedPath(pub String);
pub struct OriginalUri(pub Uri);
pub struct ConnectInfo(pub Option<SocketAddr>);
pub struct Extension<T>(pub T);
pub struct TypedHeader<T: typed_headers::Header>(pub T);

impl<T> FromRequest for Json<T>
where
    T: DeserializeOwned,
{
    type Rejection = HttpError;

    async fn from_request(req: &mut Request) -> Result<Self, Self::Rejection> {
        require_content_type(
            req.header("content-type"),
            is_json_content_type,
            "application/json",
        )?;
        serde_json::from_slice(&req.bytes().await?)
            .map(Json)
            .map_err(|err| HttpError::invalid_json().with_source(err))
    }
}

impl<T> FromRequest for Form<T>
where
    T: DeserializeOwned,
{
    type Rejection = HttpError;

    async fn from_request(req: &mut Request) -> Result<Self, Self::Rejection> {
        require_content_type(
            req.header("content-type"),
            |value| value.eq_ignore_ascii_case("application/x-www-form-urlencoded"),
            "application/x-www-form-urlencoded",
        )?;
        super::form::deserialize_form(&req.bytes().await?).map(Form)
    }
}

impl FromRequest for Bytes {
    type Rejection = HttpError;

    async fn from_request(req: &mut Request) -> Result<Self, Self::Rejection> {
        req.bytes().await
    }
}

impl FromRequest for String {
    type Rejection = HttpError;

    async fn from_request(req: &mut Request) -> Result<Self, Self::Rejection> {
        req.text().await
    }
}

impl<T> FromRequestParts for Path<T>
where
    T: DeserializeOwned,
{
    type Rejection = HttpError;

    async fn from_request_parts(parts: &mut RequestParts<'_>) -> Result<Self, Self::Rejection> {
        deserialize_path(parts.params()).map(Path)
    }
}

impl<T> FromRequestParts for Query<T>
where
    T: DeserializeOwned,
{
    type Rejection = HttpError;

    async fn from_request_parts(parts: &mut RequestParts<'_>) -> Result<Self, Self::Rejection> {
        serde_html_form::from_str(parts.raw_query().unwrap_or(""))
            .map(Query)
            .map_err(|err| {
                HttpError::bad_request(format!("Invalid query string: {}", err)).with_source(err)
            })
    }
}

impl<T> FromRequestParts for State<T>
where
    T: Send + Sync + 'static,
{
    type Rejection = HttpError;

    async fn from_request_parts(parts: &mut RequestParts<'_>) -> Result<Self, Self::Rejection> {
        parts
            .state::<T>()
            .map(State)
            .ok_or_else(|| HttpError::internal_server_error("State not found"))
    }
}

impl<T> FromRequestParts for Cookies<T>
where
    T: DeserializeOwned,
{
    type Rejection = HttpError;

    async fn from_request_parts(parts: &mut RequestParts<'_>) -> Result<Self, Self::Rejection> {
        deserialize_string_map(parts.cookies())
            .map(Cookies)
            .map_err(|err| {
                HttpError::bad_request(format!("Invalid cookies: {}", err)).with_source(err)
            })
    }
}

impl<T> FromRequestParts for Headers<T>
where
    T: DeserializeOwned,
{
    type Rejection = HttpError;

    async fn from_request_parts(parts: &mut RequestParts<'_>) -> Result<Self, Self::Rejection> {
        deserialize_string_map(parts.headers())
            .map(Headers)
            .map_err(|err| {
                HttpError::bad_request(format!("Invalid headers: {}", err)).with_source(err)
            })
    }
}

impl FromRequestParts for MatchedPath {
    type Rejection = HttpError;

    async fn from_request_parts(parts: &mut RequestParts<'_>) -> Result<Self, Self::Rejection> {
        parts
            .matched_path()
            .map(|path| MatchedPath(path.to_string()))
            .ok_or_else(|| HttpError::internal_server_error("Matched path not available"))
    }
}

impl FromRequestParts for OriginalUri {
    type Rejection = HttpError;

    async fn from_request_parts(parts: &mut RequestParts<'_>) -> Result<Self, Self::Rejection> {
        Ok(OriginalUri(parts.original_uri()))
    }
}

impl FromRequestParts for ConnectInfo {
    type Rejection = HttpError;

    async fn from_request_parts(parts: &mut RequestParts<'_>) -> Result<Self, Self::Rejection> {
        Ok(ConnectInfo(parts.remote_addr()))
    }
}

impl<T> FromRequestParts for Extension<T>
where
    T: Clone + Send + Sync + 'static,
{
    type Rejection = HttpError;

    async fn from_request_parts(parts: &mut RequestParts<'_>) -> Result<Self, Self::Rejection> {
        parts
            .extension::<T>()
            .map(|value| Extension((*value).clone()))
            .ok_or_else(|| HttpError::internal_server_error("Extension not found"))
    }
}

impl<T> FromRequestParts for TypedHeader<T>
where
    T: typed_headers::Header,
{
    type Rejection = HttpError;

    async fn from_request_parts(parts: &mut RequestParts<'_>) -> Result<Self, Self::Rejection> {
        let name = T::name();
        let raw_values = parts.headers_all(name.as_str());
        if raw_values.is_empty() {
            return Err(HttpError::new(
                StatusCode::BAD_REQUEST,
                "missing_header",
                format!("Falta el encabezado {}", name.as_str()),
            ));
        }

        let values = raw_values
            .into_iter()
            .map(HeaderValue::from_str)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| {
                HttpError::new(
                    StatusCode::BAD_REQUEST,
                    "invalid_header",
                    format!("El encabezado {} no es valido", name.as_str()),
                )
                .with_source(err)
            })?;
        let mut iter = values.iter();
        T::decode(&mut iter).map(TypedHeader).map_err(|err| {
            HttpError::new(
                StatusCode::BAD_REQUEST,
                "invalid_header",
                format!("El encabezado {} no es valido", name.as_str()),
            )
            .with_source(err)
        })
    }
}

impl FromRequestParts for Method {
    type Rejection = HttpError;

    async fn from_request_parts(parts: &mut RequestParts<'_>) -> Result<Self, Self::Rejection> {
        parts
            .method()
            .parse()
            .map_err(|err| HttpError::bad_request("Invalid method").with_source(err))
    }
}

impl FromRequestParts for Version {
    type Rejection = HttpError;

    async fn from_request_parts(parts: &mut RequestParts<'_>) -> Result<Self, Self::Rejection> {
        Ok(parts.version())
    }
}

impl<E> FromRequest for Option<E>
where
    E: FromRequest,
{
    type Rejection = HttpError;

    async fn from_request(req: &mut Request) -> Result<Self, Self::Rejection> {
        Ok(E::from_request(req).await.ok())
    }
}

impl<E> FromRequestParts for Option<E>
where
    E: FromRequestParts,
{
    type Rejection = HttpError;

    async fn from_request_parts(parts: &mut RequestParts<'_>) -> Result<Self, Self::Rejection> {
        Ok(E::from_request_parts(parts).await.ok())
    }
}

impl<E> FromRequest for Result<E, E::Rejection>
where
    E: FromRequest,
{
    type Rejection = HttpError;

    async fn from_request(req: &mut Request) -> Result<Self, Self::Rejection> {
        Ok(E::from_request(req).await)
    }
}

impl<E> FromRequestParts for Result<E, E::Rejection>
where
    E: FromRequestParts,
{
    type Rejection = HttpError;

    async fn from_request_parts(parts: &mut RequestParts<'_>) -> Result<Self, Self::Rejection> {
        Ok(E::from_request_parts(parts).await)
    }
}

fn deserialize_path<T>(params: &HashMap<String, String>) -> Result<T, HttpError>
where
    T: DeserializeOwned,
{
    let encoded = serde_urlencoded::to_string(params).map_err(|err| {
        HttpError::bad_request(format!("Invalid path parameters: {}", err)).with_source(err)
    })?;
    match serde_urlencoded::from_str(&encoded) {
        Ok(value) => Ok(value),
        Err(struct_error) => {
            if params.len() == 1 {
                let raw = params.values().next().expect("len checked");
                if let Some(value) = deserialize_scalar(raw) {
                    return Ok(value);
                }
            }
            Err(
                HttpError::bad_request(format!("Invalid path parameters: {}", struct_error))
                    .with_source(struct_error),
            )
        }
    }
}

fn deserialize_scalar<T: DeserializeOwned>(raw: &str) -> Option<T> {
    serde_json::from_str(raw)
        .ok()
        .or_else(|| serde_json::from_value(serde_json::Value::String(raw.to_string())).ok())
}

fn deserialize_string_map<T: DeserializeOwned>(
    map: &HashMap<String, String>,
) -> Result<T, serde_json::Error> {
    serde_json::from_value(serde_json::to_value(map)?)
}

fn require_content_type(
    content_type: Option<&str>,
    accepted: impl FnOnce(&str) -> bool,
    expected: &str,
) -> Result<(), HttpError> {
    let media_type = content_type
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .unwrap_or("");
    if accepted(media_type) {
        Ok(())
    } else {
        Err(HttpError::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            format!("Se esperaba Content-Type {expected}"),
        ))
    }
}

fn is_json_content_type(media_type: &str) -> bool {
    let media_type = media_type.to_ascii_lowercase();
    media_type == "application/json"
        || media_type
            .strip_prefix("application/")
            .is_some_and(|suffix| suffix.ends_with("+json"))
}
