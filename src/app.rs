//! Framework core. The implementation is split into focused submodules; this
//! file wires them together and re-exports the public API (also surfaced at the
//! crate root via `lib.rs`).

mod body;
mod cookie;
mod error;
mod extract;
mod form;
mod handler;
pub mod middleware;
mod openapi;
mod request;
mod response;
mod router;
mod server;
mod session;
mod sse;
mod state;
mod testing;
#[cfg(feature = "tls")]
pub mod tls;
mod trie;
mod websocket;

pub use body::{BodyStream, RequestBody};
pub use cookie::{Cookie, SameSite, sign_value, verify_value};
pub use error::{BoxError, HttpError, IntoHttpError};
pub use extract::{
    ConnectInfo, Cookies, Extension, Form, FromRequest, FromRequestParts, Headers, Json,
    MatchedPath, OptionalRejection, OriginalUri, Path, Query, State, TypedHeader,
};
pub use form::{MultipartLimits, MultipartPart};
pub use handler::{ErrorHandler, Handler, IntoHandler, IntoMiddleware, Middleware, Next};
pub use request::{Request, RequestBuilder, RequestParts};
pub use response::{IntoResponse, Response, ResponseBuildError};
pub use router::{
    Dotfiles, HostPattern, RouteError, RouteErrorKind, RouteHandle, RouteInfo, RouteMatchError,
    RouteMatchErrorKind, RoutePattern, Router, StaticFilesOptions,
};
pub use server::{App, TrailingSlash};
pub use session::{SessionConfigError, SessionDataError, SessionDataErrorKind, Sessions};
pub use sse::{SseError, SseEvent};
pub use state::StateStore;
pub use testing::{TestClient, TestRequest};
pub use websocket::{
    BackpressurePolicy, InMemoryWsBroker, IntoWebSocketHandler, IntoWebSocketOutput, OriginPolicy,
    WebSocket, WebSocketCapacityError, WebSocketCloseInfo, WebSocketCloseInitiator,
    WebSocketConfig, WebSocketConnectionSnapshot, WebSocketError, WebSocketErrorCategory,
    WebSocketEvent, WebSocketHandler, WebSocketId, WebSocketLifecycleState, WebSocketMessage,
    WebSocketObservation, WebSocketObserver, WebSocketReceiver, WebSocketRuntimeHandle,
    WebSocketSender, WebSocketStats, WebSocketTimeout, WsBroadcast, WsBroadcastError,
    WsBroadcastReport, WsBroker, WsBrokerError, WsBrokerErrorCategory, WsBrokerPayload,
    WsBrokerPublication, WsBrokerStream, WsBrokerTarget, WsError, WsHub, WsHubBuilder,
    WsLocalSocket, WsNodeId, WsPublicationId, WsRemotePublish, WsRoute, WsTarget,
};

// Crate-internal helpers shared across submodules.
pub(crate) use handler::{
    method_not_allowed_handler, not_found_handler, options_handler, panic_response,
};
pub(crate) use request::{parse_cookies, parse_query};
pub(crate) use response::ResponseBody;
pub(crate) use router::allow_header_value;

#[cfg(test)]
mod tests;
