use std::fmt;
use std::net::SocketAddr;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use super::{WsBrokerErrorCategory, WsNodeId, WsPublicationId};

pub use tokio_tungstenite::tungstenite::Message as WebSocketMessage;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WebSocketId(pub(crate) u64);

impl fmt::Display for WebSocketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct WebSocketCloseInfo {
    pub code: u16,
    pub reason: String,
    pub initiator: WebSocketCloseInitiator,
    pub clean: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WebSocketCloseInitiator {
    Local,
    Peer,
    Runtime,
    Timeout,
    ProtocolError,
    Handler,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WebSocketLifecycleState {
    Connecting,
    Open,
    Closing,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct WebSocketConnectionSnapshot {
    pub id: WebSocketId,
    pub route: String,
    pub remote_addr: Option<SocketAddr>,
    pub protocol: Option<String>,
    pub opened_at: SystemTime,
    pub rooms: Vec<String>,
    pub lifecycle: WebSocketLifecycleState,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct WebSocketStats {
    pub active_connections: usize,
    pub accepted_connections: u64,
    pub rejected_connections: u64,
    pub closed_connections: u64,
    pub messages_received: u64,
    pub messages_sent: u64,
    pub bytes_received: u64,
    pub bytes_sent: u64,
    pub saturated_sends: u64,
    pub heartbeat_timeouts: u64,
    pub active_rooms: usize,
    pub room_joins: u64,
    pub room_leaves: u64,
    pub local_broadcasts: u64,
    pub partial_broadcasts: u64,
    pub broker_publications: u64,
    pub broker_errors: u64,
    pub broker_connected: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum WsRemotePublish {
    #[default]
    NotConfigured,
    Published,
}

#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
/// Metadata emitted by the WebSocket runtime. Message payloads are never
/// included; message events expose only direction and encoded byte length.
pub enum WebSocketObservation<'a> {
    Accepted {
        id: WebSocketId,
        route: &'a str,
    },
    Rejected {
        route: &'a str,
        reason: &'a str,
    },
    Opened {
        id: WebSocketId,
    },
    Message {
        id: WebSocketId,
        outbound: bool,
        bytes: usize,
    },
    QueueSaturated {
        id: WebSocketId,
        outbound: bool,
    },
    HeartbeatTimeout {
        id: WebSocketId,
    },
    Closed {
        id: WebSocketId,
        code: Option<u16>,
        clean: bool,
    },
    HandlerFailed {
        id: WebSocketId,
    },
    ForcedShutdown {
        id: WebSocketId,
    },
    RoomJoined {
        id: WebSocketId,
        route: &'a str,
        room: &'a str,
    },
    RoomLeft {
        id: WebSocketId,
        route: &'a str,
        room: &'a str,
    },
    Broadcast {
        route: Option<&'a str>,
        room_count: usize,
        matched: usize,
        enqueued: usize,
        rejected: usize,
        disconnected: usize,
        remote: WsRemotePublish,
    },
    BrokerConnected {
        node: WsNodeId,
    },
    BrokerDisconnected {
        node: WsNodeId,
        reason: WsBrokerErrorCategory,
    },
    BrokerLagged {
        node: WsNodeId,
        skipped: u64,
    },
    BrokerInvalidPublication {
        origin: WsNodeId,
        publication: WsPublicationId,
    },
}

/// Receives synchronous WebSocket runtime metadata callbacks.
///
/// Implementations should return quickly and must move expensive work onto
/// their own bounded queue. Observer panics are isolated from connections.
pub trait WebSocketObserver: Send + Sync + 'static {
    fn observe(&self, _event: &WebSocketObservation<'_>) {}
}

impl WebSocketObserver for () {}

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
