use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;

use futures_util::FutureExt;
use hyper::header::{ALLOW, HeaderValue};

use super::{FromRequest, FromRequestParts, HttpError, IntoResponse, Request, Response};

/// A route handler, normalized from a sync or async user handler. `Arc` so it
/// can be cloned into the middleware chain (see [`Next`]).
pub type Handler =
    Arc<dyn Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync>;

/// The continuation passed to a middleware: calling it runs the rest of the
/// chain (the next middleware, or finally the matched handler).
pub type Next = Box<dyn FnOnce(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send>;

/// A middleware in the onion model: receives the request and `next`, and may
/// run code before/after `next(req).await`, or short-circuit by returning a
/// `Response` without calling `next`.
pub type Middleware =
    Arc<dyn Fn(Request, Next) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync>;

pub type ErrorHandler = Arc<dyn Fn(HttpError) -> Response + Send + Sync>;

/// Converts a user handler — synchronous *or* asynchronous — into the internal
/// [`Handler`] shape.
///
/// The `Marker` type parameter only exists so the two blanket impls (one for
/// `Fn(Request) -> Response`, one for `Fn(Request) -> Future`) can coexist
/// without overlapping. Callers never name it; it is inferred from the
/// closure's return type.
///
/// Body-consuming extractors must be the final typed argument; a parts extractor
/// after a body extractor is rejected at compile time:
///
/// ```rust,compile_fail
/// use rustrest::{App, Json, Path, Response};
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Input {
///     name: String,
/// }
///
/// async fn bad(Json(_input): Json<Input>, Path(_id): Path<u64>) -> Response {
///     Response::send("bad")
/// }
///
/// let mut app = App::new();
/// app.post("/users/:id", bad).unwrap();
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` no es un manejador de RustRest valido",
    label = "este valor no puede registrarse como manejador de ruta",
    note = "un manejador recibe `Request` (anote `|req: Request|`) o hasta 8 extractores tipados, \
            con el extractor de cuerpo al final",
    note = "y devuelve `Response`, `Result<Response, E>` con `E: IntoHttpError`, \
            u otro tipo `IntoResponse`, de forma sincrona o `async`"
)]
pub trait IntoHandler<Marker> {
    fn into_handler(self) -> Handler;
}

#[doc(hidden)]
pub struct SyncMarker;
#[doc(hidden)]
pub struct AsyncMarker;
#[doc(hidden)]
pub struct ExtractPartsSyncMarker<T>(std::marker::PhantomData<T>);
#[doc(hidden)]
pub struct ExtractPartsAsyncMarker<T>(std::marker::PhantomData<T>);
#[doc(hidden)]
pub struct ExtractBodySyncMarker<T>(std::marker::PhantomData<T>);
#[doc(hidden)]
pub struct ExtractBodyAsyncMarker<T>(std::marker::PhantomData<T>);

// Synchronous handlers: `|req| Response`.
impl<F, R> IntoHandler<SyncMarker> for F
where
    F: Fn(Request) -> R + Send + Sync + 'static,
    R: IntoResponse + Send + 'static,
{
    fn into_handler(self) -> Handler {
        Arc::new(
            move |req| -> Pin<Box<dyn Future<Output = Response> + Send>> {
                match catch_unwind(AssertUnwindSafe(|| self(req))) {
                    Ok(res) => Box::pin(async move { res.into_response() }),
                    Err(_) => Box::pin(async { panic_response() }),
                }
            },
        )
    }
}

// Asynchronous handlers: `|req| async { Response }`.
impl<F, Fut, R> IntoHandler<AsyncMarker> for F
where
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoResponse + Send + 'static,
{
    fn into_handler(self) -> Handler {
        Arc::new(
            move |req| -> Pin<Box<dyn Future<Output = Response> + Send>> {
                match catch_unwind(AssertUnwindSafe(|| self(req))) {
                    Ok(future) => Box::pin(async move {
                        match AssertUnwindSafe(future).catch_unwind().await {
                            Ok(res) => res.into_response(),
                            Err(_) => panic_response(),
                        }
                    }),
                    Err(_) => Box::pin(async { panic_response() }),
                }
            },
        )
    }
}

macro_rules! impl_parts_handler {
    ($(($ty:ident, $var:ident)),+) => {
        impl<F, R, $($ty,)+> IntoHandler<ExtractPartsSyncMarker<($($ty,)+)>> for F
        where
            F: Fn($($ty),+) -> R + Send + Sync + 'static,
            R: IntoResponse + Send + 'static,
            $($ty: FromRequestParts + Send + 'static,)+
        {
            fn into_handler(self) -> Handler {
                let handler = Arc::new(self);
                Arc::new(move |mut req| -> Pin<Box<dyn Future<Output = Response> + Send>> {
                    let handler = Arc::clone(&handler);
                    Box::pin(async move {
                        let mut parts = req.parts_mut();
                        $(
                            let $var = match <$ty as FromRequestParts>::from_request_parts(&mut parts).await {
                                Ok(value) => value,
                                Err(rejection) => return rejection.into_response(),
                            };
                        )+

                        match catch_unwind(AssertUnwindSafe(|| (handler)($($var),+))) {
                            Ok(res) => res.into_response(),
                            Err(_) => panic_response(),
                        }
                    })
                })
            }
        }

        impl<F, Fut, R, $($ty,)+> IntoHandler<ExtractPartsAsyncMarker<($($ty,)+)>> for F
        where
            F: Fn($($ty),+) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = R> + Send + 'static,
            R: IntoResponse + Send + 'static,
            $($ty: FromRequestParts + Send + 'static,)+
        {
            fn into_handler(self) -> Handler {
                let handler = Arc::new(self);
                Arc::new(move |mut req| -> Pin<Box<dyn Future<Output = Response> + Send>> {
                    let handler = Arc::clone(&handler);
                    Box::pin(async move {
                        let mut parts = req.parts_mut();
                        $(
                            let $var = match <$ty as FromRequestParts>::from_request_parts(&mut parts).await {
                                Ok(value) => value,
                                Err(rejection) => return rejection.into_response(),
                            };
                        )+

                        match catch_unwind(AssertUnwindSafe(|| (handler)($($var),+))) {
                            Ok(future) => match AssertUnwindSafe(future).catch_unwind().await {
                                Ok(res) => res.into_response(),
                                Err(_) => panic_response(),
                            },
                            Err(_) => panic_response(),
                        }
                    })
                })
            }
        }
    };
}

macro_rules! impl_body_only_handler {
    ($body:ident, $body_var:ident) => {
        impl<F, R, $body> IntoHandler<ExtractBodySyncMarker<($body,)>> for F
        where
            F: Fn($body) -> R + Send + Sync + 'static,
            R: IntoResponse + Send + 'static,
            $body: FromRequest + Send + 'static,
        {
            fn into_handler(self) -> Handler {
                let handler = Arc::new(self);
                Arc::new(
                    move |mut req| -> Pin<Box<dyn Future<Output = Response> + Send>> {
                        let handler = Arc::clone(&handler);
                        Box::pin(async move {
                            let $body_var =
                                match <$body as FromRequest>::from_request(&mut req).await {
                                    Ok(value) => value,
                                    Err(rejection) => return rejection.into_response(),
                                };
                            match catch_unwind(AssertUnwindSafe(|| (handler)($body_var))) {
                                Ok(res) => res.into_response(),
                                Err(_) => panic_response(),
                            }
                        })
                    },
                )
            }
        }

        impl<F, Fut, R, $body> IntoHandler<ExtractBodyAsyncMarker<($body,)>> for F
        where
            F: Fn($body) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = R> + Send + 'static,
            R: IntoResponse + Send + 'static,
            $body: FromRequest + Send + 'static,
        {
            fn into_handler(self) -> Handler {
                let handler = Arc::new(self);
                Arc::new(
                    move |mut req| -> Pin<Box<dyn Future<Output = Response> + Send>> {
                        let handler = Arc::clone(&handler);
                        Box::pin(async move {
                            let $body_var =
                                match <$body as FromRequest>::from_request(&mut req).await {
                                    Ok(value) => value,
                                    Err(rejection) => return rejection.into_response(),
                                };
                            match catch_unwind(AssertUnwindSafe(|| (handler)($body_var))) {
                                Ok(future) => match AssertUnwindSafe(future).catch_unwind().await {
                                    Ok(res) => res.into_response(),
                                    Err(_) => panic_response(),
                                },
                                Err(_) => panic_response(),
                            }
                        })
                    },
                )
            }
        }
    };
}

macro_rules! impl_parts_body_handler {
    ($(($ty:ident, $var:ident)),+ ; ($body:ident, $body_var:ident)) => {
        impl<F, R, $($ty,)+ $body> IntoHandler<ExtractBodySyncMarker<($($ty,)+ $body)>> for F
        where
            F: Fn($($ty,)+ $body) -> R + Send + Sync + 'static,
            R: IntoResponse + Send + 'static,
            $($ty: FromRequestParts + Send + 'static,)+
            $body: FromRequest + Send + 'static,
        {
            fn into_handler(self) -> Handler {
                let handler = Arc::new(self);
                Arc::new(move |mut req| -> Pin<Box<dyn Future<Output = Response> + Send>> {
                    let handler = Arc::clone(&handler);
                    Box::pin(async move {
                        $(
                            let $var;
                        )+
                        {
                            let mut parts = req.parts_mut();
                            $(
                                $var = match <$ty as FromRequestParts>::from_request_parts(&mut parts).await {
                                    Ok(value) => value,
                                    Err(rejection) => return rejection.into_response(),
                                };
                            )+
                        }
                        let $body_var = match <$body as FromRequest>::from_request(&mut req).await {
                            Ok(value) => value,
                            Err(rejection) => return rejection.into_response(),
                        };

                        match catch_unwind(AssertUnwindSafe(|| (handler)($($var,)+ $body_var))) {
                            Ok(res) => res.into_response(),
                            Err(_) => panic_response(),
                        }
                    })
                })
            }
        }

        impl<F, Fut, R, $($ty,)+ $body> IntoHandler<ExtractBodyAsyncMarker<($($ty,)+ $body)>> for F
        where
            F: Fn($($ty,)+ $body) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = R> + Send + 'static,
            R: IntoResponse + Send + 'static,
            $($ty: FromRequestParts + Send + 'static,)+
            $body: FromRequest + Send + 'static,
        {
            fn into_handler(self) -> Handler {
                let handler = Arc::new(self);
                Arc::new(move |mut req| -> Pin<Box<dyn Future<Output = Response> + Send>> {
                    let handler = Arc::clone(&handler);
                    Box::pin(async move {
                        $(
                            let $var;
                        )+
                        {
                            let mut parts = req.parts_mut();
                            $(
                                $var = match <$ty as FromRequestParts>::from_request_parts(&mut parts).await {
                                    Ok(value) => value,
                                    Err(rejection) => return rejection.into_response(),
                                };
                            )+
                        }
                        let $body_var = match <$body as FromRequest>::from_request(&mut req).await {
                            Ok(value) => value,
                            Err(rejection) => return rejection.into_response(),
                        };

                        match catch_unwind(AssertUnwindSafe(|| (handler)($($var,)+ $body_var))) {
                            Ok(future) => match AssertUnwindSafe(future).catch_unwind().await {
                                Ok(res) => res.into_response(),
                                Err(_) => panic_response(),
                            },
                            Err(_) => panic_response(),
                        }
                    })
                })
            }
        }
    };
}

impl_body_only_handler!(B, body);
impl_parts_handler!((A, a));
impl_parts_handler!((A, a), (B, b));
impl_parts_handler!((A, a), (B, b), (C, c));
impl_parts_handler!((A, a), (B, b), (C, c), (D, d));
impl_parts_handler!((A, a), (B, b), (C, c), (D, d), (E, e));
impl_parts_handler!((A, a), (B, b), (C, c), (D, d), (E, e), (G, g));
impl_parts_handler!((A, a), (B, b), (C, c), (D, d), (E, e), (G, g), (H, h));
impl_parts_handler!(
    (A, a),
    (B, b),
    (C, c),
    (D, d),
    (E, e),
    (G, g),
    (H, h),
    (I, i)
);

impl_parts_body_handler!((A, a); (B, body));
impl_parts_body_handler!((A, a), (B, b); (C, body));
impl_parts_body_handler!((A, a), (B, b), (C, c); (D, body));
impl_parts_body_handler!((A, a), (B, b), (C, c), (D, d); (E, body));
impl_parts_body_handler!((A, a), (B, b), (C, c), (D, d), (E, e); (G, body));
impl_parts_body_handler!((A, a), (B, b), (C, c), (D, d), (E, e), (G, g); (H, body));
impl_parts_body_handler!(
    (A, a),
    (B, b),
    (C, c),
    (D, d),
    (E, e),
    (G, g),
    (H, h);
    (I, body)
);

pub(crate) fn panic_response() -> Response {
    super::log::log_error!("Un manejador o middleware hizo panic; devolviendo 500");
    Response::internal_server_error()
}

/// Converts a user middleware closure into the internal [`Middleware`] shape.
#[diagnostic::on_unimplemented(
    message = "`{Self}` no es un middleware de RustRest valido",
    label = "se esperaba `Fn(Request, Next) -> impl Future<Output = Response>`",
    note = "anote los parametros del closure (`|req: Request, next: Next|`) \
            o use `middleware::from_fn(|req, next| async move {{ .. }})`"
)]
pub trait IntoMiddleware {
    fn into_middleware(self) -> Middleware;
}

impl<F, Fut> IntoMiddleware for F
where
    F: Fn(Request, Next) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Response> + Send + 'static,
{
    fn into_middleware(self) -> Middleware {
        Arc::new(
            move |req, next| -> Pin<Box<dyn Future<Output = Response> + Send>> {
                Box::pin(self(req, next))
            },
        )
    }
}

impl IntoMiddleware for Middleware {
    fn into_middleware(self) -> Middleware {
        self
    }
}

/// A handler that always responds 404, used when no route matches (so global
/// middleware still runs for unmatched requests).
pub(crate) fn not_found_handler() -> Handler {
    Arc::new(
        |_req: Request| -> Pin<Box<dyn Future<Output = Response> + Send>> {
            Box::pin(async { Response::not_found() })
        },
    )
}

/// A handler that responds `405 Method Not Allowed` with the given `Allow`
/// header. Carries an `HttpError` so a registered error handler can format it.
pub(crate) fn method_not_allowed_handler(allow: String) -> Handler {
    Arc::new(
        move |_req: Request| -> Pin<Box<dyn Future<Output = Response> + Send>> {
            let allow = allow.clone();
            Box::pin(async move {
                let allow = HeaderValue::from_str(&allow).expect("generated Allow header is valid");
                Response::from_error(
                    HttpError::method_not_allowed("Method Not Allowed").header(ALLOW, allow),
                )
            })
        },
    )
}

/// A handler that auto-answers `OPTIONS` with `204 No Content` and an `Allow`
/// header listing the methods registered for the path.
pub(crate) fn options_handler(allow: String) -> Handler {
    Arc::new(
        move |_req: Request| -> Pin<Box<dyn Future<Output = Response> + Send>> {
            let allow = allow.clone();
            Box::pin(async move { Response::send("").status(204).header("allow", &allow) })
        },
    )
}
