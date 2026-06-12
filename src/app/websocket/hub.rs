use std::collections::{BTreeSet, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures_util::StreamExt;
use hyper::body::Bytes;
use serde::Serialize;

use super::socket::{InternalWebSocketSender, LocalEnqueueOutcome, validate_close};
use super::{
    WebSocketConnectionSnapshot, WebSocketEvent, WebSocketId, WebSocketLifecycleState,
    WebSocketMessage, WebSocketRuntimeHandle, WsBroadcastError, WsBroker, WsBrokerError,
    WsBrokerPayload, WsBrokerPublication, WsBrokerTarget, WsError, WsNodeId,
};

const DEFAULT_MAX_ROOMS_PER_CONNECTION: usize = 32;
const DEFAULT_MAX_ROOM_NAME_BYTES: usize = 128;
const DEFAULT_BROADCAST_CONCURRENCY: usize = 64;
const DEFAULT_BROKER_OPERATION_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_BROKER_ROUTE_BYTES: usize = 1024;
const MAX_BROKER_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

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
    pub broker: Option<Arc<dyn WsBroker>>,
    pub node_id: WsNodeId,
}

/// Builder for hard hub ceilings and future broker behavior.
pub struct WsHubBuilder {
    max_rooms_per_connection: usize,
    max_room_name_bytes: usize,
    broadcast_concurrency: usize,
    broker_operation_timeout: Duration,
    broker: Option<Arc<dyn WsBroker>>,
    node_id: Option<WsNodeId>,
}

#[derive(Clone)]
pub struct WsRoute {
    hub: WsHub,
    route: String,
}

#[derive(Clone)]
pub struct WsTarget {
    hub: WsHub,
    route: Option<String>,
    rooms: Vec<String>,
    all_in_scope: bool,
    excluded: HashSet<WebSocketId>,
    publish_remote: bool,
}

#[derive(Clone)]
pub struct WsLocalSocket {
    snapshot: WebSocketConnectionSnapshot,
    sender: InternalWebSocketSender,
}

#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct WsBroadcastReport {
    pub matched: usize,
    pub enqueued: usize,
    pub rejected: usize,
    pub disconnected: usize,
    pub remote: WsRemotePublish,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum WsRemotePublish {
    #[default]
    NotConfigured,
    Published,
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

    pub(crate) fn from_runtime(runtime: WebSocketRuntimeHandle) -> Self {
        let runtime_config = runtime.hub_config();
        Self {
            runtime,
            config: Arc::new(WsHubConfig {
                max_rooms_per_connection: runtime_config.max_rooms_per_connection,
                max_room_name_bytes: runtime_config.max_room_name_bytes,
                broadcast_concurrency: runtime_config.broadcast_concurrency,
                broker_operation_timeout: runtime_config.broker_operation_timeout,
                broker: runtime_config.broker,
                node_id: runtime_config.node_id,
            }),
        }
    }

    pub fn route(&self, route: impl Into<String>) -> WsRoute {
        WsRoute {
            hub: self.clone(),
            route: route.into(),
        }
    }

    pub fn all(&self) -> WsTarget {
        WsTarget {
            hub: self.clone(),
            route: None,
            rooms: Vec::new(),
            all_in_scope: true,
            excluded: HashSet::new(),
            publish_remote: true,
        }
    }

    pub fn local_socket(&self, id: WebSocketId) -> Option<WsLocalSocket> {
        self.runtime.local_socket(id).map(|parts| WsLocalSocket {
            snapshot: parts.snapshot,
            sender: parts.sender,
        })
    }

    pub async fn disconnect_local(
        &self,
        id: WebSocketId,
        code: u16,
        reason: &str,
    ) -> Result<(), WsError> {
        self.runtime.close(id, code, reason).await
    }

    pub fn local_connection_count(&self) -> usize {
        self.runtime.stats().active_connections
    }

    pub fn node_id(&self) -> WsNodeId {
        self.config.node_id
    }

    pub(crate) fn validate_broker_publication(
        &self,
        publication: &WsBrokerPublication,
    ) -> Result<(), WsBrokerError> {
        match &publication.target {
            WsBrokerTarget::RouteRooms { route, rooms } => {
                validate_broker_route(route)?;
                if rooms.is_empty() || rooms.len() > self.config.max_rooms_per_connection {
                    return Err(WsBrokerError::InvalidPublication(
                        "el destino de rooms debe incluir una cantidad valida".into(),
                    ));
                }
                for room in rooms {
                    validate_broker_room(room, self.config.max_room_name_bytes)?;
                }
            }
            WsBrokerTarget::RouteAll { route } => validate_broker_route(route)?,
            WsBrokerTarget::AllRoutes => {}
        }
        let payload_len = match &publication.payload {
            WsBrokerPayload::Text(text) => text.len(),
            WsBrokerPayload::Binary(bytes) => bytes.len(),
        };
        if payload_len > MAX_BROKER_PAYLOAD_BYTES {
            return Err(WsBrokerError::InvalidPublication(
                "el payload supera el limite del broker WebSocket".into(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn deliver_broker_publication(&self, publication: WsBrokerPublication) {
        let target = match publication.target {
            WsBrokerTarget::RouteRooms { route, rooms } => WsTarget {
                hub: self.clone(),
                route: Some(route),
                rooms: normalize_rooms(rooms),
                all_in_scope: false,
                excluded: HashSet::new(),
                publish_remote: false,
            },
            WsBrokerTarget::RouteAll { route } => WsTarget {
                hub: self.clone(),
                route: Some(route),
                rooms: Vec::new(),
                all_in_scope: true,
                excluded: HashSet::new(),
                publish_remote: false,
            },
            WsBrokerTarget::AllRoutes => WsTarget {
                hub: self.clone(),
                route: None,
                rooms: Vec::new(),
                all_in_scope: true,
                excluded: HashSet::new(),
                publish_remote: false,
            },
        };
        let message = match publication.payload {
            WsBrokerPayload::Text(text) => WebSocketMessage::text(text),
            WsBrokerPayload::Binary(bytes) => WebSocketMessage::binary(bytes),
        };
        let _ = target.send_local(message).await;
    }
}

impl WsLocalSocket {
    pub fn id(&self) -> WebSocketId {
        self.snapshot.id
    }

    pub fn route(&self) -> &str {
        &self.snapshot.route
    }

    pub fn remote_addr(&self) -> Option<SocketAddr> {
        self.snapshot.remote_addr
    }

    pub fn protocol(&self) -> Option<&str> {
        self.snapshot.protocol.as_deref()
    }

    pub fn opened_at(&self) -> SystemTime {
        self.snapshot.opened_at
    }

    pub fn rooms(&self) -> &[String] {
        &self.snapshot.rooms
    }

    pub fn lifecycle(&self) -> WebSocketLifecycleState {
        self.snapshot.lifecycle
    }

    pub async fn send(&self, message: WebSocketMessage) -> Result<(), WsError> {
        self.sender.send(message).await
    }

    pub async fn send_text(&self, text: &str) -> Result<(), WsError> {
        self.send(WebSocketMessage::text(text.to_string())).await
    }

    pub async fn send_binary(&self, bytes: impl Into<Bytes>) -> Result<(), WsError> {
        self.send(WebSocketMessage::binary(bytes.into())).await
    }

    pub async fn send_json<T>(&self, value: &T) -> Result<(), WsError>
    where
        T: Serialize,
    {
        self.send_text(&serde_json::to_string(value)?).await
    }

    pub async fn send_event<T>(&self, event: &str, data: &T) -> Result<(), WsError>
    where
        T: Serialize,
    {
        self.send_json(&WebSocketEvent {
            event: event.to_string(),
            data,
        })
        .await
    }

    pub async fn close(&self) -> Result<(), WsError> {
        self.close_with(1000, "").await
    }

    pub async fn close_with(&self, code: u16, reason: &str) -> Result<(), WsError> {
        validate_close(code, reason)?;
        self.sender.disconnect(code, reason).await
    }

    pub async fn closed(&self) -> super::WebSocketCloseInfo {
        self.sender.closed().await
    }
}

impl WsRoute {
    pub fn to(&self, room: impl Into<String>) -> WsTarget {
        self.to_many([room.into()])
    }

    pub fn to_many<I, S>(&self, rooms: I) -> WsTarget
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        WsTarget {
            hub: self.hub.clone(),
            route: Some(self.route.clone()),
            rooms: normalize_rooms(rooms),
            all_in_scope: false,
            excluded: HashSet::new(),
            publish_remote: true,
        }
    }

    pub fn all(&self) -> WsTarget {
        WsTarget {
            hub: self.hub.clone(),
            route: Some(self.route.clone()),
            rooms: Vec::new(),
            all_in_scope: true,
            excluded: HashSet::new(),
            publish_remote: true,
        }
    }

    pub async fn local_room_size(&self, room: &str) -> usize {
        self.hub.runtime.local_room_size(&self.route, room)
    }
}

impl WsTarget {
    pub fn except(mut self, id: WebSocketId) -> Self {
        self.excluded.insert(id);
        self
    }

    pub async fn send(
        &self,
        message: WebSocketMessage,
    ) -> Result<WsBroadcastReport, WsBroadcastError> {
        if !message.is_text() && !message.is_binary() {
            return Err(WsBroadcastError::InvalidMessage);
        }
        for room in &self.rooms {
            if room.is_empty()
                || room.contains('\0')
                || room.len() > self.hub.config.max_room_name_bytes
            {
                return Err(WsBroadcastError::InvalidRoom(room.clone()));
            }
        }
        let mut report = self.send_local(message.clone()).await;
        if !self.publish_remote {
            return Ok(report);
        }
        let Some(broker) = &self.hub.config.broker else {
            return Ok(report);
        };
        let publication = WsBrokerPublication::new(
            self.hub.runtime.next_publication_id(),
            self.hub.config.node_id,
            self.broker_target(),
            broker_payload(&message).ok_or(WsBroadcastError::InvalidMessage)?,
        );
        let published = tokio::time::timeout(
            self.hub.config.broker_operation_timeout,
            broker.publish(publication),
        )
        .await;
        match published {
            Ok(Ok(())) => {
                report.remote = WsRemotePublish::Published;
                Ok(report)
            }
            Ok(Err(source)) => Err(WsBroadcastError::Broker {
                source,
                local_report: report,
            }),
            Err(_) => Err(WsBroadcastError::Broker {
                source: WsBrokerError::Timeout,
                local_report: report,
            }),
        }
    }

    async fn send_local(&self, message: WebSocketMessage) -> WsBroadcastReport {
        let recipients = self.hub.runtime.select_broadcast_recipients(
            self.route.as_deref(),
            &self.rooms,
            self.all_in_scope,
            &self.excluded,
        );
        let matched = recipients.len();
        let outcomes = futures_util::stream::iter(recipients.into_iter().map(|recipient| {
            let message = message.clone();
            async move {
                let _id = recipient.id;
                match recipient.sender {
                    Some(sender) => sender.enqueue(message).await,
                    None => LocalEnqueueOutcome::Disconnected,
                }
            }
        }))
        .buffer_unordered(self.hub.config.broadcast_concurrency)
        .collect::<Vec<_>>()
        .await;
        let mut report = WsBroadcastReport {
            matched,
            ..WsBroadcastReport::default()
        };
        for outcome in outcomes {
            match outcome {
                LocalEnqueueOutcome::Enqueued => report.enqueued += 1,
                LocalEnqueueOutcome::Rejected => report.rejected += 1,
                LocalEnqueueOutcome::Disconnected => report.disconnected += 1,
            }
        }
        debug_assert_eq!(
            report.matched,
            report.enqueued + report.rejected + report.disconnected
        );
        report
    }

    fn broker_target(&self) -> WsBrokerTarget {
        match (self.route.as_ref(), self.all_in_scope) {
            (Some(route), false) => WsBrokerTarget::RouteRooms {
                route: route.clone(),
                rooms: self.rooms.clone(),
            },
            (Some(route), true) => WsBrokerTarget::RouteAll {
                route: route.clone(),
            },
            (None, true) => WsBrokerTarget::AllRoutes,
            (None, false) => unreachable!("un destino por rooms siempre tiene una ruta"),
        }
    }

    pub async fn send_text(&self, text: &str) -> Result<WsBroadcastReport, WsBroadcastError> {
        self.send(WebSocketMessage::text(text.to_string())).await
    }

    pub async fn send_binary(
        &self,
        bytes: impl Into<Bytes>,
    ) -> Result<WsBroadcastReport, WsBroadcastError> {
        self.send(WebSocketMessage::binary(bytes.into())).await
    }

    pub async fn send_json<T>(&self, value: &T) -> Result<WsBroadcastReport, WsBroadcastError>
    where
        T: Serialize,
    {
        self.send_text(&serde_json::to_string(value)?).await
    }

    pub async fn send_event<T>(
        &self,
        event: &str,
        data: &T,
    ) -> Result<WsBroadcastReport, WsBroadcastError>
    where
        T: Serialize,
    {
        self.send_json(&WebSocketEvent {
            event: event.to_string(),
            data,
        })
        .await
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

    pub fn broker(mut self, broker: Arc<dyn WsBroker>) -> Self {
        self.broker = Some(broker);
        self
    }

    pub fn node_id(mut self, node_id: WsNodeId) -> Self {
        self.node_id = Some(node_id);
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
        let node_id = self.node_id.unwrap_or_else(super::broker::allocate_node_id);
        let config = Arc::new(WsHubConfig {
            max_rooms_per_connection: self.max_rooms_per_connection,
            max_room_name_bytes: self.max_room_name_bytes,
            broadcast_concurrency: self.broadcast_concurrency,
            broker_operation_timeout: self.broker_operation_timeout,
            broker: self.broker,
            node_id,
        });
        let runtime = WebSocketRuntimeHandle::local_with_hub_config(
            config.max_rooms_per_connection,
            config.max_room_name_bytes,
            config.broadcast_concurrency,
            config.broker_operation_timeout,
            config.broker.clone(),
            config.node_id,
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
            broker: None,
            node_id: None,
        }
    }
}

fn normalize_rooms<I, S>(rooms: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    rooms
        .into_iter()
        .map(Into::into)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn broker_payload(message: &WebSocketMessage) -> Option<WsBrokerPayload> {
    match message {
        WebSocketMessage::Text(text) => Some(WsBrokerPayload::Text(text.to_string())),
        WebSocketMessage::Binary(bytes) => Some(WsBrokerPayload::Binary(bytes.clone())),
        _ => None,
    }
}

fn validate_broker_route(route: &str) -> Result<(), WsBrokerError> {
    if route.is_empty() || route.contains('\0') || route.len() > MAX_BROKER_ROUTE_BYTES {
        return Err(WsBrokerError::InvalidPublication(
            "la ruta del broker WebSocket no es valida".into(),
        ));
    }
    Ok(())
}

fn validate_broker_room(room: &str, max_bytes: usize) -> Result<(), WsBrokerError> {
    if room.is_empty() || room.contains('\0') || room.len() > max_bytes {
        return Err(WsBrokerError::InvalidPublication(
            "una room del broker WebSocket no es valida".into(),
        ));
    }
    Ok(())
}
