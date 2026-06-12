use super::types::{WebSocketErrorCategory, WebSocketId};

#[derive(Debug)]
pub enum WebSocketError {
    Protocol(tokio_tungstenite::tungstenite::Error),
    Json(serde_json::Error),
}

impl WebSocketError {
    pub fn category(&self) -> WebSocketErrorCategory {
        match self {
            Self::Protocol(_) => WebSocketErrorCategory::Protocol,
            Self::Json(_) => WebSocketErrorCategory::Json,
        }
    }
}

impl std::fmt::Display for WebSocketError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WebSocketError::Protocol(err) => write!(f, "websocket protocol error: {}", err),
            WebSocketError::Json(err) => write!(f, "websocket JSON error: {}", err),
        }
    }
}

impl std::error::Error for WebSocketError {}

impl From<tokio_tungstenite::tungstenite::Error> for WebSocketError {
    fn from(value: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::Protocol(value)
    }
}

impl From<serde_json::Error> for WebSocketError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum WsError {
    WebSocket(WebSocketError),
    Timeout(WebSocketTimeout),
    Capacity(WebSocketCapacityError),
    InvalidConfiguration(String),
    InvalidClose { code: u16, reason: String },
    InvalidRoom(String),
    RoomLimit,
    ConnectionNotFound(WebSocketId),
    Shutdown,
    HandlerPanic,
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WebSocketTimeout {
    Send,
    Pong,
    Idle,
    Lifetime,
    Close,
    Shutdown,
    Broker,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WebSocketCapacityError {
    InboundQueue,
    OutboundQueue,
    GlobalConnections,
    RouteConnections,
    IpConnections,
    MessageRate,
}

impl std::fmt::Display for WsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WebSocket(error) => error.fmt(f),
            Self::Timeout(timeout) => write!(f, "tiempo limite WebSocket agotado: {timeout:?}"),
            Self::Capacity(capacity) => {
                write!(f, "capacidad WebSocket agotada: {capacity:?}")
            }
            Self::InvalidConfiguration(message) => {
                write!(f, "configuracion WebSocket no valida: {message}")
            }
            Self::InvalidClose { code, reason } => {
                write!(f, "cierre WebSocket no valido ({code}): {reason}")
            }
            Self::InvalidRoom(room) => write!(f, "room WebSocket no valido: {room}"),
            Self::RoomLimit => f.write_str("limite de rooms WebSocket alcanzado"),
            Self::ConnectionNotFound(id) => {
                write!(f, "la conexion WebSocket {id} no existe en este runtime")
            }
            Self::Shutdown => f.write_str("el runtime WebSocket se esta cerrando"),
            Self::HandlerPanic => f.write_str("el handler WebSocket termino con panic"),
            Self::Closed => f.write_str("la conexion WebSocket esta cerrada"),
        }
    }
}

impl std::error::Error for WsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::WebSocket(error) => Some(error),
            _ => None,
        }
    }
}

impl From<WebSocketError> for WsError {
    fn from(value: WebSocketError) -> Self {
        Self::WebSocket(value)
    }
}

impl From<serde_json::Error> for WsError {
    fn from(value: serde_json::Error) -> Self {
        Self::WebSocket(value.into())
    }
}
