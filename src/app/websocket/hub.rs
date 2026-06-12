use std::sync::Arc;
use std::time::Duration;

use super::{WebSocketRuntimeHandle, WsError};

const DEFAULT_MAX_ROOMS_PER_CONNECTION: usize = 32;
const DEFAULT_MAX_ROOM_NAME_BYTES: usize = 128;
const DEFAULT_BROADCAST_CONCURRENCY: usize = 64;
const DEFAULT_BROKER_OPERATION_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone)]
/// Cloneable process-local WebSocket room and broadcast handle.
pub struct WsHub {
    runtime: WebSocketRuntimeHandle,
    pub(crate) config: Arc<WsHubConfig>,
}

pub(crate) struct WsHubConfig {
    pub max_rooms_per_connection: usize,
    pub max_room_name_bytes: usize,
    pub broadcast_concurrency: usize,
    pub broker_operation_timeout: Duration,
}

/// Builder for hard hub ceilings and future broker behavior.
pub struct WsHubBuilder {
    max_rooms_per_connection: usize,
    max_room_name_bytes: usize,
    broadcast_concurrency: usize,
    broker_operation_timeout: Duration,
}

impl WsHub {
    pub fn builder() -> WsHubBuilder {
        WsHubBuilder::default()
    }

    pub fn local() -> Self {
        WsHubBuilder::default()
            .build()
            .expect("la configuracion WebSocket local por defecto debe ser valida")
    }

    pub(crate) fn runtime(&self) -> WebSocketRuntimeHandle {
        self.runtime.clone()
    }
}

impl Default for WsHub {
    fn default() -> Self {
        Self::local()
    }
}

impl WsHubBuilder {
    pub fn max_rooms_per_connection(mut self, max_rooms: usize) -> Self {
        self.max_rooms_per_connection = max_rooms;
        self
    }

    pub fn max_room_name_bytes(mut self, max_bytes: usize) -> Self {
        self.max_room_name_bytes = max_bytes;
        self
    }

    pub fn broadcast_concurrency(mut self, concurrency: usize) -> Self {
        self.broadcast_concurrency = concurrency;
        self
    }

    pub fn broker_operation_timeout(mut self, timeout: Duration) -> Self {
        self.broker_operation_timeout = timeout;
        self
    }

    pub fn build(self) -> Result<WsHub, WsError> {
        if self.max_rooms_per_connection == 0
            || self.max_room_name_bytes == 0
            || self.broadcast_concurrency == 0
            || self.broker_operation_timeout.is_zero()
        {
            return Err(WsError::InvalidConfiguration(
                "los limites del hub WebSocket deben ser mayores que cero".into(),
            ));
        }
        let config = Arc::new(WsHubConfig {
            max_rooms_per_connection: self.max_rooms_per_connection,
            max_room_name_bytes: self.max_room_name_bytes,
            broadcast_concurrency: self.broadcast_concurrency,
            broker_operation_timeout: self.broker_operation_timeout,
        });
        let runtime = WebSocketRuntimeHandle::local_with_room_limits(
            config.max_rooms_per_connection,
            config.max_room_name_bytes,
        );
        Ok(WsHub { runtime, config })
    }
}

impl Default for WsHubBuilder {
    fn default() -> Self {
        Self {
            max_rooms_per_connection: DEFAULT_MAX_ROOMS_PER_CONNECTION,
            max_room_name_bytes: DEFAULT_MAX_ROOM_NAME_BYTES,
            broadcast_concurrency: DEFAULT_BROADCAST_CONCURRENCY,
            broker_operation_timeout: DEFAULT_BROKER_OPERATION_TIMEOUT,
        }
    }
}
