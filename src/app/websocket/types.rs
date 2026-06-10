use serde::{Deserialize, Serialize};

pub use tokio_tungstenite::tungstenite::Message as WebSocketMessage;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebSocketEvent<T = serde_json::Value> {
    pub event: String,
    pub data: T,
}
