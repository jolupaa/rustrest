use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;

use futures_util::FutureExt;

use super::{HttpError, IntoResponse, Request, Response};

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
pub trait IntoHandler<Marker> {
    fn into_handler(self) -> Handler;
}

#[doc(hidden)]
pub struct SyncMarker;
#[doc(hidden)]
pub struct AsyncMarker;

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

pub(crate) fn panic_response() -> Response {
    eprintln!("A handler or middleware panicked; returning 500.");
    Response::internal_server_error()
}

/// Converts a user middleware closure into the internal [`Middleware`] shape.
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
                Response::from_error(HttpError::new(405, "Method Not Allowed"))
                    .header("allow", &allow)
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
