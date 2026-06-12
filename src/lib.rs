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

#[cfg(feature = "tls")]
pub use app::tls;
pub use app::{
    App, BackpressurePolicy, Cookie, Cookies, ErrorHandler, Form, FromRequest, Handler, Headers,
    HttpError, IntoHandler, IntoHttpError, IntoMiddleware, IntoResponse, IntoWebSocketHandler,
    IntoWebSocketOutput, Json, Middleware, MultipartPart, Next, OriginPolicy, Path, Query, Request,
    RequestBuilder, Response, RouteHandle, RouteInfo, Router, SameSite, Sessions, SseEvent, State,
    StateStore, TestClient, TestRequest, TrailingSlash, WebSocket, WebSocketCapacityError,
    WebSocketCloseInfo, WebSocketCloseInitiator, WebSocketConfig, WebSocketConnectionSnapshot,
    WebSocketError, WebSocketErrorCategory, WebSocketEvent, WebSocketHandler, WebSocketId,
    WebSocketMessage, WebSocketObservation, WebSocketObserver, WebSocketReceiver,
    WebSocketRuntimeHandle, WebSocketSender, WebSocketStats, WebSocketTimeout, WsBroadcast,
    WsError, middleware, sign_value, verify_value,
};
