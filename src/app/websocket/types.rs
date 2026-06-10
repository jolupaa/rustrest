use serde::{Deserialize, Serialize};

pub use tokio_tungstenite::tungstenite::Message as WebSocketMessage;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WebSocketErrorCategory {
    Protocol,
    Json,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebSocketEvent<T = serde_json::Value> {
    pub event: String,
    pub data: T,
}
