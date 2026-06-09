use std::sync::Arc;

use serde::de::DeserializeOwned;

use super::{HttpError, Request};

pub trait FromRequest: Sized {
    fn from_request(req: &Request) -> Result<Self, HttpError>;
}

pub struct Json<T>(pub T);
pub struct Path<T>(pub T);
pub struct Query<T>(pub T);
pub struct State<T>(pub Arc<T>);

impl<T> FromRequest for Json<T>
where
    T: DeserializeOwned,
{
    fn from_request(req: &Request) -> Result<Self, HttpError> {
        req.json()
            .map(Json)
            .map_err(|err| HttpError::bad_request(format!("JSON invalido: {}", err)))
    }
}

impl<T> FromRequest for Path<T>
where
    T: DeserializeOwned,
{
    fn from_request(req: &Request) -> Result<Self, HttpError> {
        let encoded = serde_urlencoded::to_string(&req.params).map_err(|err| {
            HttpError::bad_request(format!("Parametros de ruta invalidos: {}", err))
        })?;
        serde_urlencoded::from_str(&encoded)
            .map(Path)
            .map_err(|err| HttpError::bad_request(format!("Parametros de ruta invalidos: {}", err)))
    }
}

impl<T> FromRequest for Query<T>
where
    T: DeserializeOwned,
{
    fn from_request(req: &Request) -> Result<Self, HttpError> {
        serde_html_form::from_str(req.raw_query.as_deref().unwrap_or(""))
            .map(Query)
            .map_err(|err| HttpError::bad_request(format!("Query invalida: {}", err)))
    }
}

impl<T> FromRequest for State<T>
where
    T: Send + Sync + 'static,
{
    fn from_request(req: &Request) -> Result<Self, HttpError> {
        req.state::<T>()
            .map(State)
            .ok_or_else(|| HttpError::internal_server_error("Estado no encontrado"))
    }
}
