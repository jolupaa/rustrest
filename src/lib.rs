//! RustRest is a minimal Express-style HTTP framework for Rust.
//!
//! The main API can be imported from the crate root:
//!
//! ```rust,no_run
//! use rustrest::{App, Request, Response};
//!
//! #[tokio::main]
//! async fn main() -> std::io::Result<()> {
//!     let mut app = App::new();
//!
//!     app.get("/", |_req: Request| {
//!         Response::send("Hello from RustRest")
//!     });
//!
//!     app.listen("127.0.0.1:3000").await
//! }
//! ```
//!
//! The [`app`] module is also available with the framework's public core types.
#![forbid(unsafe_code)]

pub mod app;

pub use app::{
    App, ErrorHandler, FromRequest, Handler, HttpError, IntoHandler, IntoHttpError, IntoMiddleware,
    IntoResponse, IntoWebSocketHandler, Json, Middleware, Next, Path, Query, Request, Response,
    RouteHandle, Router, SseEvent, State, StateStore, WebSocket, WebSocketError, WebSocketEvent,
    WebSocketHandler, WebSocketMessage, middleware,
};
