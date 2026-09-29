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
//!     }).unwrap();
//!
//!     app.listen("127.0.0.1:3000").await
//! }
//! ```
//!
//! The [`app`] module is also available with the framework's public core types.
//! See `docs/migrations/0.3-to-0.4.md` in the repository for the latest migration guide.
#![forbid(unsafe_code)]

pub mod app;

/// The typed-header crate behind [`TypedHeader`].
pub use headers;
/// The byte buffer used for request and response bodies.
pub use hyper::body::Bytes;
/// The `http` crate version used by RustRest's public types (`Method`,
/// `StatusCode`, `HeaderMap`, `HeaderName`, `HeaderValue`, `Uri`, ...).
/// Importing it from here keeps application code on a matching version.
pub use hyper::http;

#[cfg(feature = "tls")]
pub use app::tls;
pub use app::{
    App, BackpressurePolicy, BodyStream, BoxError, ConnectInfo, Cookie, Cookies, Dotfiles,
    ErrorHandler, Extension, Form, FromRequest, FromRequestParts, Handler, Headers, HostPattern,
    HttpError, InMemoryWsBroker, IntoHandler, IntoHttpError, IntoMiddleware, IntoResponse,
    IntoWebSocketHandler, IntoWebSocketOutput, Json, MatchedPath, Middleware, MultipartLimits,
    MultipartPart, Next, OptionalRejection, OriginPolicy, OriginalUri, Path, Query, Request,
    RequestBody, RequestBuilder, RequestParts, Response, ResponseBuildError, RouteError,
    RouteErrorKind, RouteHandle, RouteInfo, RouteMatchError, RouteMatchErrorKind, RoutePattern,
    Router, SameSite, SessionConfigError, SessionDataError, SessionDataErrorKind, Sessions,
    SseError, SseEvent, State, StateStore, StaticFilesOptions, TestClient, TestRequest,
    TrailingSlash, TypedHeader, WebSocket, WebSocketCapacityError, WebSocketCloseInfo,
    WebSocketCloseInitiator, WebSocketConfig, WebSocketConnectionSnapshot, WebSocketError,
    WebSocketErrorCategory, WebSocketEvent, WebSocketHandler, WebSocketId, WebSocketLifecycleState,
    WebSocketMessage, WebSocketObservation, WebSocketObserver, WebSocketReceiver,
    WebSocketRuntimeHandle, WebSocketSender, WebSocketStats, WebSocketTimeout, WsBroadcast,
    WsBroadcastError, WsBroadcastReport, WsBroker, WsBrokerError, WsBrokerErrorCategory,
    WsBrokerPayload, WsBrokerPublication, WsBrokerStream, WsBrokerTarget, WsError, WsHub,
    WsHubBuilder, WsLocalSocket, WsNodeId, WsPublicationId, WsRemotePublish, WsRoute, WsTarget,
    middleware, sign_value, verify_value,
};
