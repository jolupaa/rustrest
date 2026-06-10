#[derive(Debug)]
pub enum WebSocketError {
    Protocol(tokio_tungstenite::tungstenite::Error),
    Json(serde_json::Error),
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
