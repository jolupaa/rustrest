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
#![forbid(unsafe_code)]

pub mod app;

#[cfg(feature = "tls")]
pub use app::tls;
pub use app::{
    App, BackpressurePolicy, BodyStream, BoxError, ConnectInfo, Cookie, Cookies, ErrorHandler,
    Extension, Form, FromRequest, FromRequestParts, Handler, Headers, HostPattern, HttpError,
    InMemoryWsBroker, IntoHandler, IntoHttpError, IntoMiddleware, IntoResponse,
    IntoWebSocketHandler, IntoWebSocketOutput, Json, MatchedPath, Middleware, MultipartPart, Next,
    OriginPolicy, OriginalUri, Path, Query, Request, RequestBody, RequestBuilder, RequestParts,
    Response, ResponseBuildError, RouteError, RouteErrorKind, RouteHandle, RouteInfo,
    RouteMatchError, RouteMatchErrorKind, RoutePattern, Router, SameSite, Sessions, SseError,
    SseEvent, State, StateStore, TestClient, TestRequest, TrailingSlash, TypedHeader, WebSocket,
    WebSocketCapacityError, WebSocketCloseInfo, WebSocketCloseInitiator, WebSocketConfig,
    WebSocketConnectionSnapshot, WebSocketError, WebSocketErrorCategory, WebSocketEvent,
    WebSocketHandler, WebSocketId, WebSocketLifecycleState, WebSocketMessage, WebSocketObservation,
    WebSocketObserver, WebSocketReceiver, WebSocketRuntimeHandle, WebSocketSender, WebSocketStats,
    WebSocketTimeout, WsBroadcast, WsBroadcastError, WsBroadcastReport, WsBroker, WsBrokerError,
    WsBrokerErrorCategory, WsBrokerPayload, WsBrokerPublication, WsBrokerStream, WsBrokerTarget,
    WsError, WsHub, WsHubBuilder, WsLocalSocket, WsNodeId, WsPublicationId, WsRemotePublish,
    WsRoute, WsTarget, middleware, sign_value, verify_value,
};
