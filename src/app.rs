#![allow(dead_code)]

//! Framework core. The implementation is split into focused submodules; this
//! file wires them together and re-exports the public API (also surfaced at the
//! crate root via `lib.rs`).

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

pub use cookie::{Cookie, SameSite, sign_value, verify_value};
pub use error::{HttpError, IntoHttpError};
pub use extract::{Cookies, Form, FromRequest, Headers, Json, Path, Query, State};
pub use form::MultipartPart;
pub use handler::{ErrorHandler, Handler, IntoHandler, IntoMiddleware, Middleware, Next};
pub use request::{Request, RequestBuilder};
pub use response::{IntoResponse, Response};
pub use router::{RouteHandle, RouteInfo, Router};
pub use server::{App, TrailingSlash};
pub use session::Sessions;
pub use sse::SseEvent;
pub use state::StateStore;
pub use testing::{TestClient, TestRequest};
pub use websocket::{
    BackpressurePolicy, InMemoryWsBroker, IntoWebSocketHandler, IntoWebSocketOutput, OriginPolicy,
    WebSocket, WebSocketCapacityError, WebSocketCloseInfo, WebSocketCloseInitiator,
    WebSocketConfig, WebSocketConnectionSnapshot, WebSocketError, WebSocketErrorCategory,
    WebSocketEvent, WebSocketHandler, WebSocketId, WebSocketLifecycleState, WebSocketMessage,
    WebSocketObservation, WebSocketObserver, WebSocketReceiver, WebSocketRuntimeHandle,
    WebSocketSender, WebSocketStats, WebSocketTimeout, WsBroadcast, WsBroadcastError,
    WsBroadcastReport, WsBroker, WsBrokerError, WsBrokerPayload, WsBrokerPublication,
    WsBrokerStream, WsBrokerTarget, WsError, WsHub, WsHubBuilder, WsLocalSocket, WsNodeId,
    WsPublicationId, WsRemotePublish, WsRoute, WsTarget,
};

// Crate-internal helpers shared across submodules.
pub(crate) use handler::{
    method_not_allowed_handler, not_found_handler, options_handler, panic_response,
};
pub(crate) use request::{decode_component, parse_cookies, parse_query};
pub(crate) use response::ResponseBody;
pub(crate) use router::allow_header_value;

#[cfg(test)]
mod tests;
