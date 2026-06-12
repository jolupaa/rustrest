use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use futures_util::Stream;
use futures_util::future::BoxFuture;
use hyper::body::Bytes;

pub type WsBrokerStream =
    Pin<Box<dyn Stream<Item = Result<WsBrokerPublication, WsBrokerError>> + Send>>;

static NEXT_NODE_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) fn allocate_node_id() -> WsNodeId {
    WsNodeId::new(NEXT_NODE_ID.fetch_add(1, Ordering::Relaxed))
}

pub trait WsBroker: Send + Sync + 'static {
    fn publish<'a>(
        &'a self,
        publication: WsBrokerPublication,
    ) -> BoxFuture<'a, Result<(), WsBrokerError>>;

    fn subscribe<'a>(
        &'a self,
        node: WsNodeId,
    ) -> BoxFuture<'a, Result<WsBrokerStream, WsBrokerError>>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WsNodeId(u64);

impl WsNodeId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WsPublicationId(u64);

impl WsPublicationId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct WsBrokerPublication {
    pub id: WsPublicationId,
    pub origin: WsNodeId,
    pub target: WsBrokerTarget,
    pub payload: WsBrokerPayload,
}

impl WsBrokerPublication {
    pub fn new(
        id: WsPublicationId,
        origin: WsNodeId,
        target: WsBrokerTarget,
        payload: WsBrokerPayload,
    ) -> Self {
        Self {
            id,
            origin,
            target,
            payload,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WsBrokerTarget {
    RouteRooms { route: String, rooms: Vec<String> },
    RouteAll { route: String },
    AllRoutes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WsBrokerPayload {
    Text(String),
    Binary(Bytes),
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WsBrokerError {
    Unavailable,
    Timeout,
    Lagged(u64),
    InvalidPublication(String),
    SubscriptionClosed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WsBrokerErrorCategory {
    Unavailable,
    Timeout,
    Lagged,
    InvalidPublication,
    SubscriptionClosed,
}

impl WsBrokerError {
    pub fn category(&self) -> WsBrokerErrorCategory {
        match self {
            Self::Unavailable => WsBrokerErrorCategory::Unavailable,
            Self::Timeout => WsBrokerErrorCategory::Timeout,
            Self::Lagged(_) => WsBrokerErrorCategory::Lagged,
            Self::InvalidPublication(_) => WsBrokerErrorCategory::InvalidPublication,
            Self::SubscriptionClosed => WsBrokerErrorCategory::SubscriptionClosed,
        }
    }
}

impl std::fmt::Display for WsBrokerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable => f.write_str("el broker WebSocket no esta disponible"),
            Self::Timeout => f.write_str("el broker WebSocket agoto su tiempo limite"),
            Self::Lagged(count) => {
                write!(
                    f,
                    "la suscripcion del broker WebSocket perdio {count} publicaciones"
                )
            }
            Self::InvalidPublication(message) => {
                write!(f, "publicacion de broker WebSocket no valida: {message}")
            }
            Self::SubscriptionClosed => f.write_str("la suscripcion del broker WebSocket se cerro"),
        }
    }
}

impl std::error::Error for WsBrokerError {}

pub struct InMemoryWsBroker {
    sender: Mutex<Option<tokio::sync::broadcast::Sender<WsBrokerPublication>>>,
}

impl InMemoryWsBroker {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = tokio::sync::broadcast::channel(capacity);
        Self {
            sender: Mutex::new(Some(sender)),
        }
    }

    pub fn close(&self) {
        self.sender
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
    }
}

impl WsBroker for InMemoryWsBroker {
    fn publish<'a>(
        &'a self,
        publication: WsBrokerPublication,
    ) -> BoxFuture<'a, Result<(), WsBrokerError>> {
        Box::pin(async move {
            let sender = self
                .sender
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let sender = sender.as_ref().ok_or(WsBrokerError::Unavailable)?;
            let _ = sender.send(publication);
            Ok(())
        })
    }

    fn subscribe<'a>(
        &'a self,
        _node: WsNodeId,
    ) -> BoxFuture<'a, Result<WsBrokerStream, WsBrokerError>> {
        Box::pin(async move {
            let receiver = self
                .sender
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_ref()
                .ok_or(WsBrokerError::SubscriptionClosed)?
                .subscribe();
            let stream = futures_util::stream::unfold(
                (receiver, false),
                |(mut receiver, terminated)| async move {
                    if terminated {
                        return None;
                    }
                    let (item, terminated) = match receiver.recv().await {
                        Ok(publication) => (Ok(publication), false),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                            (Err(WsBrokerError::Lagged(count)), false)
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            (Err(WsBrokerError::SubscriptionClosed), true)
                        }
                    };
                    Some((item, (receiver, terminated)))
                },
            );
            Ok(Box::pin(stream) as WsBrokerStream)
        })
    }
}
