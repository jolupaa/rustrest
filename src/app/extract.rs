use std::collections::HashMap;
use std::sync::Arc;

use hyper::body::Bytes;
use serde::de::DeserializeOwned;

use super::{HttpError, Request};

pub trait FromRequest: Sized {
    fn from_request(req: &Request) -> Result<Self, HttpError>;
}

pub struct Json<T>(pub T);
pub struct Form<T>(pub T);
pub struct Path<T>(pub T);
pub struct Query<T>(pub T);
pub struct State<T>(pub Arc<T>);
/// Deserializes all request cookies into `T` (string-valued fields).
pub struct Cookies<T>(pub T);
/// Deserializes all request headers into `T` (string-valued fields; header
/// names are lowercase, so use `#[serde(rename = "x-api-key")]`).
pub struct Headers<T>(pub T);

impl<T> FromRequest for Form<T>
where
    T: DeserializeOwned,
{
    fn from_request(req: &Request) -> Result<Self, HttpError> {
        req.form().map(Form)
    }
}

impl<T> FromRequest for Json<T>
where
    T: DeserializeOwned,
{
    fn from_request(req: &Request) -> Result<Self, HttpError> {
        req.json()
            .map(Json)
            .map_err(|err| HttpError::bad_request(format!("Invalid JSON: {}", err)))
    }
}

impl<T> FromRequest for Path<T>
where
    T: DeserializeOwned,
{
    fn from_request(req: &Request) -> Result<Self, HttpError> {
        let map_error =
            |err: String| HttpError::bad_request(format!("Invalid path parameters: {}", err));

        // Structs deserialize from the param map; a single captured param can
        // also deserialize directly into a scalar (`Path<u32>` for `/:id`).
        let encoded =
            serde_urlencoded::to_string(&req.params).map_err(|err| map_error(err.to_string()))?;
        match serde_urlencoded::from_str(&encoded) {
            Ok(value) => Ok(Path(value)),
            Err(struct_error) => {
                if req.params.len() == 1 {
                    let raw = req.params.values().next().expect("len checked");
                    if let Some(value) = deserialize_scalar(raw) {
                        return Ok(Path(value));
                    }
                }
                Err(map_error(struct_error.to_string()))
            }
        }
    }
}

/// Deserializes a raw string into a scalar `T`: numbers/booleans parse as
/// their JSON form, anything else falls back to a JSON string.
fn deserialize_scalar<T: DeserializeOwned>(raw: &str) -> Option<T> {
    serde_json::from_str(raw)
        .ok()
        .or_else(|| serde_json::from_value(serde_json::Value::String(raw.to_string())).ok())
}

impl<T> FromRequest for Query<T>
where
    T: DeserializeOwned,
{
    fn from_request(req: &Request) -> Result<Self, HttpError> {
        serde_html_form::from_str(req.raw_query.as_deref().unwrap_or(""))
            .map(Query)
            .map_err(|err| HttpError::bad_request(format!("Invalid query string: {}", err)))
    }
}

impl<T> FromRequest for State<T>
where
    T: Send + Sync + 'static,
{
    fn from_request(req: &Request) -> Result<Self, HttpError> {
        req.state::<T>()
            .map(State)
            .ok_or_else(|| HttpError::internal_server_error("State not found"))
    }
}

impl FromRequest for Bytes {
    fn from_request(req: &Request) -> Result<Self, HttpError> {
        Ok(req.body.clone())
    }
}

impl FromRequest for String {
    fn from_request(req: &Request) -> Result<Self, HttpError> {
        Ok(req.text().into_owned())
    }
}

// `Option<E>` turns extraction failures into `None` instead of an error.
impl<E> FromRequest for Option<E>
where
    E: FromRequest,
{
    fn from_request(req: &Request) -> Result<Self, HttpError> {
        Ok(E::from_request(req).ok())
    }
}

// `Result<E, HttpError>` hands the failure to the handler to inspect.
impl<E> FromRequest for Result<E, HttpError>
where
    E: FromRequest,
{
    fn from_request(req: &Request) -> Result<Self, HttpError> {
        Ok(E::from_request(req))
    }
}

impl<T> FromRequest for Cookies<T>
where
    T: DeserializeOwned,
{
    fn from_request(req: &Request) -> Result<Self, HttpError> {
        deserialize_string_map(&req.cookies)
            .map(Cookies)
            .map_err(|err| HttpError::bad_request(format!("Invalid cookies: {}", err)))
    }
}

impl<T> FromRequest for Headers<T>
where
    T: DeserializeOwned,
{
    fn from_request(req: &Request) -> Result<Self, HttpError> {
        deserialize_string_map(&req.headers)
            .map(Headers)
            .map_err(|err| HttpError::bad_request(format!("Invalid headers: {}", err)))
    }
}

fn deserialize_string_map<T: DeserializeOwned>(
    map: &HashMap<String, String>,
) -> Result<T, serde_json::Error> {
    serde_json::from_value(serde_json::to_value(map)?)
}
