#![allow(dead_code)]

//! Framework core. The implementation is split into focused submodules; this
//! file wires them together and re-exports the public API (also surfaced at the
//! crate root via `lib.rs`).

mod error;
mod extract;
mod handler;
pub mod middleware;
mod request;
mod response;
mod router;
mod server;
mod sse;
mod state;
mod websocket;

pub use error::{HttpError, IntoHttpError};
pub use extract::{FromRequest, Json, Path, Query, State};
pub use handler::{ErrorHandler, Handler, IntoHandler, IntoMiddleware, Middleware, Next};
pub use request::Request;
pub use response::{IntoResponse, Response};
pub use router::{RouteHandle, Router};
pub use server::App;
pub use sse::SseEvent;
pub use state::StateStore;
pub use websocket::{
    IntoWebSocketHandler, WebSocket, WebSocketError, WebSocketEvent, WebSocketHandler,
    WebSocketMessage,
};

// Crate-internal helpers shared across submodules.
pub(crate) use handler::{not_found_handler, panic_response};
pub(crate) use request::{decode_component, parse_cookies, parse_query};
pub(crate) use response::ResponseBody;

#[cfg(test)]
mod tests;
