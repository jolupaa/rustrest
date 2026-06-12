use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::{Duration, SystemTime};

use futures_util::StreamExt;
use tokio::sync::{Notify, watch};
use tokio::task::AbortHandle;

use super::ResolvedWebSocketConfig;
use super::broker::{
    WsBroker, WsBrokerError, WsBrokerErrorCategory, WsBrokerPublication, WsNodeId, WsPublicationId,
    allocate_node_id,
};
use super::socket::{InternalWebSocketSender, validate_close};
use super::types::{
    WebSocketCloseInfo, WebSocketConnectionSnapshot, WebSocketId, WebSocketLifecycleState,
    WebSocketObservation, WebSocketObserver, WebSocketStats,
};
use super::{WebSocketTimeout, WsBroadcastReport, WsError};

struct RuntimeInner {
    next_id: AtomicU64,
    next_publication_id: AtomicU64,
    broker_started: AtomicBool,
    registry: Mutex<Registry>,
    shutdown_tx: watch::Sender<bool>,
    empty: Notify,
    registry_changed: Notify,
    observer: RwLock<Arc<dyn WebSocketObserver>>,
    max_rooms_per_connection: usize,
    max_room_name_bytes: usize,
    broadcast_concurrency: usize,
    broker_operation_timeout: Duration,
    broker: Option<Arc<dyn WsBroker>>,
    node_id: WsNodeId,
}

struct Registry {
    accepting: bool,
    connections: HashMap<WebSocketId, ConnectionEntry>,
    route_counts: HashMap<String, usize>,
    ip_counts: HashMap<IpAddr, usize>,
    rooms: HashMap<(String, String), HashSet<WebSocketId>>,
    broker_connected: bool,
    seen_publications: HashSet<(WsNodeId, WsPublicationId)>,
    seen_publication_order: VecDeque<(WsNodeId, WsPublicationId)>,
    counters: WebSocketCounters,
}

const MAX_SEEN_PUBLICATIONS: usize = 4096;

#[derive(Clone)]
struct ConnectionEntry {
    route: String,
    remote_addr: Option<SocketAddr>,
    protocol: Option<String>,
    opened_at: SystemTime,
    close_timeout: Duration,
    max_rooms_per_connection: usize,
    max_room_name_bytes: usize,
    rooms: BTreeSet<String>,
    lifecycle: WebSocketLifecycleState,
    internal_sender: Option<InternalWebSocketSender>,
    driver_abort: Option<AbortHandle>,
    forced_shutdown_observed: bool,
}

#[derive(Default)]
struct WebSocketCounters {
    accepted_connections: u64,
    rejected_connections: u64,
    closed_connections: u64,
    messages_received: u64,
    messages_sent: u64,
    bytes_received: u64,
    bytes_sent: u64,
    saturated_sends: u64,
    heartbeat_timeouts: u64,
    room_joins: u64,
    room_leaves: u64,
    local_broadcasts: u64,
    partial_broadcasts: u64,
    broker_publications: u64,
    broker_errors: u64,
}

#[derive(Clone)]
pub struct WebSocketRuntimeHandle {
    inner: Arc<RuntimeInner>,
}

pub(crate) struct LocalBroadcastRecipient {
    pub id: WebSocketId,
    pub sender: Option<InternalWebSocketSender>,
}

pub(crate) struct LocalSocketParts {
    pub snapshot: WebSocketConnectionSnapshot,
    pub sender: InternalWebSocketSender,
}

pub(crate) struct RuntimeHubConfig {
    pub max_rooms_per_connection: usize,
    pub max_room_name_bytes: usize,
    pub broadcast_concurrency: usize,
    pub broker_operation_timeout: Duration,
    pub broker: Option<Arc<dyn WsBroker>>,
    pub node_id: WsNodeId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AdmissionError {
    Shutdown,
    ProcessCapacity,
    RouteCapacity,
    IpCapacity,
}

impl AdmissionError {
    pub(crate) fn reason(self) -> &'static str {
        match self {
            Self::Shutdown => "shutdown",
            Self::ProcessCapacity => "process_capacity",
            Self::RouteCapacity => "route_capacity",
            Self::IpCapacity => "ip_capacity",
        }
    }
}

pub(crate) struct ConnectionPermit {
    id: WebSocketId,
    runtime: WebSocketRuntimeHandle,
    released: bool,
}

impl ConnectionPermit {
    pub(crate) fn id(&self) -> WebSocketId {
        self.id
    }

    pub(crate) fn runtime(&self) -> WebSocketRuntimeHandle {
        self.runtime.clone()
    }
}

impl WebSocketRuntimeHandle {
    pub(crate) fn local() -> Self {
        Self::local_with_hub_config(
            32,
            128,
            64,
            Duration::from_secs(2),
            None,
            allocate_node_id(),
        )
    }

    pub(crate) fn local_with_hub_config(
        max_rooms_per_connection: usize,
        max_room_name_bytes: usize,
        broadcast_concurrency: usize,
        broker_operation_timeout: Duration,
        broker: Option<Arc<dyn WsBroker>>,
        node_id: WsNodeId,
    ) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            inner: Arc::new(RuntimeInner {
                next_id: AtomicU64::new(1),
                next_publication_id: AtomicU64::new(1),
                broker_started: AtomicBool::new(false),
                registry: Mutex::new(Registry {
                    accepting: true,
                    connections: HashMap::new(),
                    route_counts: HashMap::new(),
                    ip_counts: HashMap::new(),
                    rooms: HashMap::new(),
                    broker_connected: false,
                    seen_publications: HashSet::new(),
                    seen_publication_order: VecDeque::new(),
                    counters: WebSocketCounters::default(),
                }),
                shutdown_tx,
                empty: Notify::new(),
                registry_changed: Notify::new(),
                observer: RwLock::new(Arc::new(())),
                max_rooms_per_connection,
                max_room_name_bytes,
                broadcast_concurrency,
                broker_operation_timeout,
                broker,
                node_id,
            }),
        }
    }

    /// Returns one coherent snapshot of process-local WebSocket counters.
    pub fn stats(&self) -> WebSocketStats {
        let registry = self.registry();
        WebSocketStats {
            active_connections: registry.connections.len(),
            accepted_connections: registry.counters.accepted_connections,
            rejected_connections: registry.counters.rejected_connections,
            closed_connections: registry.counters.closed_connections,
            messages_received: registry.counters.messages_received,
            messages_sent: registry.counters.messages_sent,
            bytes_received: registry.counters.bytes_received,
            bytes_sent: registry.counters.bytes_sent,
            saturated_sends: registry.counters.saturated_sends,
            heartbeat_timeouts: registry.counters.heartbeat_timeouts,
            active_rooms: registry.rooms.len(),
            room_joins: registry.counters.room_joins,
            room_leaves: registry.counters.room_leaves,
            local_broadcasts: registry.counters.local_broadcasts,
            partial_broadcasts: registry.counters.partial_broadcasts,
            broker_publications: registry.counters.broker_publications,
            broker_errors: registry.counters.broker_errors,
            broker_connected: registry.broker_connected,
            ..WebSocketStats::default()
        }
    }

    /// Returns active process-local connections sorted by ID.
    pub fn connections(&self) -> Vec<WebSocketConnectionSnapshot> {
        let registry = self.registry();
        let mut connections = registry
            .connections
            .iter()
            .map(|(&id, entry)| snapshot(id, entry))
            .collect::<Vec<_>>();
        connections.sort_by_key(|connection| connection.id.0);
        connections
    }

    /// Returns metadata for one active process-local connection.
    pub fn connection(&self, id: WebSocketId) -> Option<WebSocketConnectionSnapshot> {
        let registry = self.registry();
        registry
            .connections
            .get(&id)
            .map(|entry| snapshot(id, entry))
    }

    /// Replaces the process-local metadata observer.
    pub fn set_observer(&self, observer: Arc<dyn WebSocketObserver>) {
        *self
            .inner
            .observer
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = observer;
    }

    pub(crate) fn stop_accepting(&self) {
        self.registry().accepting = false;
    }

    pub(crate) fn admit(
        &self,
        route: &str,
        remote_addr: Option<SocketAddr>,
        protocol: Option<&str>,
        config: &ResolvedWebSocketConfig,
    ) -> Result<ConnectionPermit, AdmissionError> {
        let remote_ip = remote_addr.map(|addr| addr.ip());
        let admission =
            {
                let mut registry = self.registry();
                let rejection =
                    if !registry.accepting {
                        Some(AdmissionError::Shutdown)
                    } else if config
                        .process_max_connections
                        .is_some_and(|limit| registry.connections.len() >= limit)
                    {
                        Some(AdmissionError::ProcessCapacity)
                    } else if config.route_max_connections.is_some_and(|limit| {
                        registry.route_counts.get(route).copied().unwrap_or(0) >= limit
                    }) {
                        Some(AdmissionError::RouteCapacity)
                    } else if remote_ip.zip(config.max_connections_per_ip).is_some_and(
                        |(ip, limit)| registry.ip_counts.get(&ip).copied().unwrap_or(0) >= limit,
                    ) {
                        Some(AdmissionError::IpCapacity)
                    } else {
                        None
                    };

                if let Some(error) = rejection {
                    registry.counters.rejected_connections += 1;
                    Err(error)
                } else {
                    let id = WebSocketId(self.inner.next_id.fetch_add(1, Ordering::Relaxed));
                    let route = route.to_string();
                    registry.connections.insert(
                        id,
                        ConnectionEntry {
                            route: route.clone(),
                            remote_addr,
                            protocol: protocol.map(str::to_string),
                            opened_at: SystemTime::now(),
                            close_timeout: config.close_timeout,
                            max_rooms_per_connection: config
                                .max_rooms_per_connection
                                .min(self.inner.max_rooms_per_connection),
                            max_room_name_bytes: config
                                .max_room_name_bytes
                                .min(self.inner.max_room_name_bytes),
                            rooms: BTreeSet::new(),
                            lifecycle: WebSocketLifecycleState::Connecting,
                            internal_sender: None,
                            driver_abort: None,
                            forced_shutdown_observed: false,
                        },
                    );
                    *registry.route_counts.entry(route).or_default() += 1;
                    if let Some(ip) = remote_ip {
                        *registry.ip_counts.entry(ip).or_default() += 1;
                    }
                    registry.counters.accepted_connections += 1;
                    Ok(id)
                }
            };

        match admission {
            Ok(id) => {
                self.observe(&WebSocketObservation::Accepted { id, route });
                Ok(ConnectionPermit {
                    id,
                    runtime: self.clone(),
                    released: false,
                })
            }
            Err(error) => {
                self.observe(&WebSocketObservation::Rejected {
                    route,
                    reason: error.reason(),
                });
                Err(error)
            }
        }
    }

    pub(crate) fn register_driver(
        &self,
        id: WebSocketId,
        internal_sender: InternalWebSocketSender,
        driver_abort: AbortHandle,
    ) -> bool {
        let registered = {
            let mut registry = self.registry();
            let Some(entry) = registry.connections.get_mut(&id) else {
                return false;
            };
            entry.internal_sender = Some(internal_sender);
            entry.driver_abort = Some(driver_abort);
            true
        };
        self.inner.registry_changed.notify_waiters();
        registered
    }

    pub(crate) fn join(&self, id: WebSocketId, rooms: &[String]) -> Result<(), WsError> {
        let (route, additions) = {
            let mut registry = self.registry();
            let (route, additions) = {
                let entry = registry
                    .connections
                    .get_mut(&id)
                    .ok_or(WsError::ConnectionNotFound(id))?;
                let requested = rooms.iter().cloned().collect::<BTreeSet<_>>();
                for room in &requested {
                    validate_room(room, entry.max_room_name_bytes)?;
                }
                let additions = requested
                    .difference(&entry.rooms)
                    .cloned()
                    .collect::<Vec<_>>();
                if entry.rooms.len() + additions.len() > entry.max_rooms_per_connection {
                    return Err(WsError::RoomLimit);
                }
                for room in &additions {
                    entry.rooms.insert(room.clone());
                }
                (entry.route.clone(), additions)
            };
            for room in &additions {
                registry
                    .rooms
                    .entry((route.clone(), room.clone()))
                    .or_default()
                    .insert(id);
            }
            registry.counters.room_joins = registry
                .counters
                .room_joins
                .saturating_add(u64::try_from(additions.len()).unwrap_or(u64::MAX));
            (route, additions)
        };
        for room in &additions {
            self.observe(&WebSocketObservation::RoomJoined {
                id,
                route: &route,
                room,
            });
        }
        Ok(())
    }

    pub(crate) fn leave(&self, id: WebSocketId, rooms: &[String]) -> Result<(), WsError> {
        let (route, removals) = {
            let mut registry = self.registry();
            let (route, removals) = {
                let entry = registry
                    .connections
                    .get_mut(&id)
                    .ok_or(WsError::ConnectionNotFound(id))?;
                let requested = rooms.iter().cloned().collect::<BTreeSet<_>>();
                let removals = requested
                    .intersection(&entry.rooms)
                    .cloned()
                    .collect::<Vec<_>>();
                for room in &removals {
                    entry.rooms.remove(room);
                }
                (entry.route.clone(), removals)
            };
            remove_room_memberships(&mut registry.rooms, &route, id, removals.clone());
            registry.counters.room_leaves = registry
                .counters
                .room_leaves
                .saturating_add(u64::try_from(removals.len()).unwrap_or(u64::MAX));
            (route, removals)
        };
        self.observe_room_leaves(id, &route, &removals);
        Ok(())
    }

    pub(crate) fn leave_all(&self, id: WebSocketId) -> Result<(), WsError> {
        let (route, rooms) = {
            let mut registry = self.registry();
            let (route, rooms) = {
                let entry = registry
                    .connections
                    .get_mut(&id)
                    .ok_or(WsError::ConnectionNotFound(id))?;
                let rooms = std::mem::take(&mut entry.rooms)
                    .into_iter()
                    .collect::<Vec<_>>();
                (entry.route.clone(), rooms)
            };
            remove_room_memberships(&mut registry.rooms, &route, id, rooms.clone());
            registry.counters.room_leaves = registry
                .counters
                .room_leaves
                .saturating_add(u64::try_from(rooms.len()).unwrap_or(u64::MAX));
            (route, rooms)
        };
        self.observe_room_leaves(id, &route, &rooms);
        Ok(())
    }

    pub(crate) fn rooms(&self, id: WebSocketId) -> Option<Vec<String>> {
        self.registry()
            .connections
            .get(&id)
            .map(|entry| entry.rooms.iter().cloned().collect())
    }

    pub(crate) fn local_room_size(&self, route: &str, room: &str) -> usize {
        self.registry()
            .rooms
            .get(&(route.to_string(), room.to_string()))
            .map_or(0, HashSet::len)
    }

    pub(crate) fn hub_config(&self) -> RuntimeHubConfig {
        RuntimeHubConfig {
            max_rooms_per_connection: self.inner.max_rooms_per_connection,
            max_room_name_bytes: self.inner.max_room_name_bytes,
            broadcast_concurrency: self.inner.broadcast_concurrency,
            broker_operation_timeout: self.inner.broker_operation_timeout,
            broker: self.inner.broker.clone(),
            node_id: self.inner.node_id,
        }
    }

    pub(crate) fn next_publication_id(&self) -> WsPublicationId {
        WsPublicationId::new(
            self.inner
                .next_publication_id
                .fetch_add(1, Ordering::Relaxed),
        )
    }

    pub(crate) async fn start_broker(&self) {
        let Some(broker) = self.inner.broker.clone() else {
            return;
        };
        if self
            .inner
            .broker_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let runtime = self.clone();
        tokio::spawn(async move {
            runtime.supervise_broker(broker).await;
        });
    }

    pub(crate) fn local_socket(&self, id: WebSocketId) -> Option<LocalSocketParts> {
        let registry = self.registry();
        let entry = registry.connections.get(&id)?;
        Some(LocalSocketParts {
            snapshot: snapshot(id, entry),
            sender: entry.internal_sender.clone()?,
        })
    }

    pub(crate) fn select_broadcast_recipients(
        &self,
        route: Option<&str>,
        rooms: &[String],
        all_in_scope: bool,
        excluded: &HashSet<WebSocketId>,
    ) -> Vec<LocalBroadcastRecipient> {
        let registry = self.registry();
        let mut ids = if all_in_scope {
            registry
                .connections
                .iter()
                .filter_map(|(&id, entry)| {
                    route.is_none_or(|route| entry.route == route).then_some(id)
                })
                .collect::<HashSet<_>>()
        } else {
            let Some(route) = route else {
                return Vec::new();
            };
            rooms
                .iter()
                .filter_map(|room| registry.rooms.get(&(route.to_string(), room.clone())))
                .flat_map(|members| members.iter().copied())
                .collect::<HashSet<_>>()
        };
        ids.retain(|id| !excluded.contains(id));
        let mut ids = ids.into_iter().collect::<Vec<_>>();
        ids.sort_by_key(|id| id.0);
        ids.into_iter()
            .map(|id| LocalBroadcastRecipient {
                id,
                sender: registry
                    .connections
                    .get(&id)
                    .and_then(|entry| entry.internal_sender.clone()),
            })
            .collect()
    }

    /// Closes one process-local connection and waits for registry cleanup.
    pub async fn close(&self, id: WebSocketId, code: u16, reason: &str) -> Result<(), WsError> {
        validate_close(code, reason)?;
        let close_timeout = {
            let mut registry = self.registry();
            let entry = registry
                .connections
                .get_mut(&id)
                .ok_or(WsError::ConnectionNotFound(id))?;
            entry.lifecycle = WebSocketLifecycleState::Closing;
            entry.close_timeout
        };

        tokio::time::timeout(close_timeout, async {
            let sender = self.wait_for_sender(id).await?;
            sender.disconnect(code, reason).await?;
            let _ = sender.closed().await;
            self.wait_for_removal(id).await;
            Ok(())
        })
        .await
        .map_err(|_| WsError::Timeout(WebSocketTimeout::Close))?
    }

    /// Stops future WebSocket admission and drains active WebSockets.
    ///
    /// This does not stop the HTTP listener.
    pub async fn shutdown(&self) -> Result<(), WsError> {
        self.begin_shutdown().await;
        let grace = self.shutdown_grace_period();
        match self.drain(grace).await {
            Ok(()) => Ok(()),
            Err(error) => {
                self.abort_remaining();
                self.wait_until_empty().await;
                Err(error)
            }
        }
    }

    pub(crate) async fn begin_shutdown(&self) {
        {
            let mut registry = self.registry();
            registry.accepting = false;
            for entry in registry.connections.values_mut() {
                entry.lifecycle = WebSocketLifecycleState::Closing;
            }
        }
        self.inner.shutdown_tx.send_replace(true);
    }

    pub(crate) fn subscribe_shutdown(&self) -> watch::Receiver<bool> {
        self.inner.shutdown_tx.subscribe()
    }

    pub(crate) fn shutdown_grace_period(&self) -> Duration {
        self.registry()
            .connections
            .values()
            .map(|entry| entry.close_timeout)
            .max()
            .unwrap_or(Duration::ZERO)
    }

    pub(crate) async fn drain(&self, timeout: Duration) -> Result<(), WsError> {
        if self.active_count() == 0 {
            return Ok(());
        }
        tokio::time::timeout(timeout, self.wait_until_empty())
            .await
            .map_err(|_| WsError::Timeout(WebSocketTimeout::Shutdown))
    }

    pub(crate) fn abort_remaining(&self) {
        let remaining = {
            let mut registry = self.registry();
            registry
                .connections
                .iter_mut()
                .filter_map(|(&id, entry)| {
                    let abort_handle = entry.driver_abort.clone()?;
                    if entry.forced_shutdown_observed {
                        return None;
                    }
                    entry.forced_shutdown_observed = true;
                    Some((id, abort_handle))
                })
                .collect::<Vec<_>>()
        };
        for (id, abort_handle) in remaining {
            self.observe(&WebSocketObservation::ForcedShutdown { id });
            abort_handle.abort();
        }
    }

    pub(crate) async fn wait_until_empty(&self) {
        loop {
            let notified = self.inner.empty.notified();
            if self.active_count() == 0 {
                return;
            }
            notified.await;
        }
    }

    async fn supervise_broker(self, broker: Arc<dyn WsBroker>) {
        let mut shutdown = self.subscribe_shutdown();
        let mut retry_delay = Duration::from_millis(100);
        loop {
            if *shutdown.borrow() {
                self.set_broker_disconnected(WsBrokerErrorCategory::SubscriptionClosed);
                return;
            }
            let subscribed = tokio::select! {
                changed = shutdown.changed() => {
                    let _ = changed;
                    self.set_broker_disconnected(WsBrokerErrorCategory::SubscriptionClosed);
                    return;
                }
                result = tokio::time::timeout(
                    self.inner.broker_operation_timeout,
                    broker.subscribe(self.inner.node_id),
                ) => result,
            };
            let mut stream = match subscribed {
                Ok(Ok(stream)) => {
                    self.set_broker_connected();
                    retry_delay = Duration::from_millis(100);
                    stream
                }
                Ok(Err(error)) => {
                    self.record_broker_error(&error);
                    self.set_broker_disconnected(error.category());
                    if !wait_for_broker_retry(&mut shutdown, retry_delay).await {
                        return;
                    }
                    retry_delay = (retry_delay * 2).min(Duration::from_secs(1));
                    continue;
                }
                Err(_) => {
                    let error = WsBrokerError::Timeout;
                    self.record_broker_error(&error);
                    self.set_broker_disconnected(error.category());
                    if !wait_for_broker_retry(&mut shutdown, retry_delay).await {
                        return;
                    }
                    retry_delay = (retry_delay * 2).min(Duration::from_secs(1));
                    continue;
                }
            };

            loop {
                tokio::select! {
                    changed = shutdown.changed() => {
                        let _ = changed;
                        self.set_broker_disconnected(WsBrokerErrorCategory::SubscriptionClosed);
                        return;
                    }
                    item = stream.next() => match item {
                        Some(Ok(publication)) => self.handle_broker_publication(publication).await,
                        Some(Err(error)) => {
                            self.record_broker_error(&error);
                            self.set_broker_disconnected(error.category());
                            break;
                        }
                        None => {
                            self.set_broker_disconnected(
                                WsBrokerErrorCategory::SubscriptionClosed,
                            );
                            break;
                        }
                    }
                }
            }
            if !wait_for_broker_retry(&mut shutdown, retry_delay).await {
                return;
            }
            retry_delay = (retry_delay * 2).min(Duration::from_secs(1));
        }
    }

    async fn handle_broker_publication(&self, publication: WsBrokerPublication) {
        if publication.origin == self.inner.node_id {
            return;
        }
        let hub = super::WsHub::from_runtime(self.clone());
        if hub.validate_broker_publication(&publication).is_err() {
            let error = WsBrokerError::InvalidPublication("publicacion remota rechazada".into());
            self.record_broker_error(&error);
            self.observe(&WebSocketObservation::BrokerInvalidPublication {
                origin: publication.origin,
                publication: publication.id,
            });
            return;
        }
        if !self.mark_publication_seen(publication.origin, publication.id) {
            return;
        }
        hub.deliver_broker_publication(publication).await;
    }

    fn mark_publication_seen(&self, origin: WsNodeId, id: WsPublicationId) -> bool {
        let key = (origin, id);
        let mut registry = self.registry();
        if !registry.seen_publications.insert(key) {
            return false;
        }
        registry.seen_publication_order.push_back(key);
        if registry.seen_publication_order.len() > MAX_SEEN_PUBLICATIONS
            && let Some(expired) = registry.seen_publication_order.pop_front()
        {
            registry.seen_publications.remove(&expired);
        }
        true
    }

    fn set_broker_connected(&self) {
        let changed = {
            let mut registry = self.registry();
            let changed = !registry.broker_connected;
            registry.broker_connected = true;
            changed
        };
        if changed {
            self.observe(&WebSocketObservation::BrokerConnected {
                node: self.inner.node_id,
            });
        }
    }

    fn set_broker_disconnected(&self, reason: WsBrokerErrorCategory) {
        self.registry().broker_connected = false;
        self.observe(&WebSocketObservation::BrokerDisconnected {
            node: self.inner.node_id,
            reason,
        });
    }

    fn active_count(&self) -> usize {
        self.registry().connections.len()
    }

    async fn wait_for_sender(&self, id: WebSocketId) -> Result<InternalWebSocketSender, WsError> {
        loop {
            let changed = self.inner.registry_changed.notified();
            match self.registry().connections.get(&id) {
                Some(entry) => {
                    if let Some(sender) = &entry.internal_sender {
                        return Ok(sender.clone());
                    }
                }
                None => return Err(WsError::ConnectionNotFound(id)),
            }
            changed.await;
        }
    }

    async fn wait_for_removal(&self, id: WebSocketId) {
        loop {
            let changed = self.inner.registry_changed.notified();
            if !self.registry().connections.contains_key(&id) {
                return;
            }
            changed.await;
        }
    }

    pub(crate) fn record_opened(&self, id: WebSocketId) {
        {
            let mut registry = self.registry();
            if let Some(entry) = registry.connections.get_mut(&id) {
                entry.lifecycle = WebSocketLifecycleState::Open;
            }
        }
        self.observe(&WebSocketObservation::Opened { id });
    }

    pub(crate) fn record_closing(&self, id: WebSocketId) {
        let mut registry = self.registry();
        if let Some(entry) = registry.connections.get_mut(&id) {
            entry.lifecycle = WebSocketLifecycleState::Closing;
        }
    }

    pub(crate) fn record_message(
        &self,
        id: WebSocketId,
        outbound: bool,
        bytes: usize,
        message_type: &'static str,
    ) {
        {
            let mut registry = self.registry();
            let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
            if outbound {
                registry.counters.messages_sent = registry.counters.messages_sent.saturating_add(1);
                registry.counters.bytes_sent = registry.counters.bytes_sent.saturating_add(bytes);
            } else {
                registry.counters.messages_received =
                    registry.counters.messages_received.saturating_add(1);
                registry.counters.bytes_received =
                    registry.counters.bytes_received.saturating_add(bytes);
            }
        }
        self.observe(&WebSocketObservation::Message {
            id,
            outbound,
            bytes,
        });
        trace_websocket_message(id, outbound, bytes, message_type);
    }

    pub(crate) fn record_saturated_send(&self, id: WebSocketId, outbound: bool) {
        self.registry().counters.saturated_sends += 1;
        self.observe(&WebSocketObservation::QueueSaturated { id, outbound });
    }

    pub(crate) fn record_heartbeat_timeout(&self, id: WebSocketId) {
        self.registry().counters.heartbeat_timeouts += 1;
        self.observe(&WebSocketObservation::HeartbeatTimeout { id });
    }

    pub(crate) fn record_closed(&self, id: WebSocketId, close_info: &WebSocketCloseInfo) {
        self.observe(&WebSocketObservation::Closed {
            id,
            code: Some(close_info.code),
            clean: close_info.clean,
        });
    }

    pub(crate) fn record_handler_failed(&self, id: WebSocketId) {
        self.observe(&WebSocketObservation::HandlerFailed { id });
    }

    pub(crate) fn record_broadcast(
        &self,
        route: Option<&str>,
        room_count: usize,
        report: &WsBroadcastReport,
    ) {
        {
            let mut registry = self.registry();
            registry.counters.local_broadcasts =
                registry.counters.local_broadcasts.saturating_add(1);
            if report.rejected > 0 || report.disconnected > 0 {
                registry.counters.partial_broadcasts =
                    registry.counters.partial_broadcasts.saturating_add(1);
            }
        }
        self.observe(&WebSocketObservation::Broadcast {
            route,
            room_count,
            matched: report.matched,
            enqueued: report.enqueued,
            rejected: report.rejected,
            disconnected: report.disconnected,
            remote: report.remote,
        });
    }

    pub(crate) fn record_broker_publication(&self) {
        let mut registry = self.registry();
        registry.counters.broker_publications =
            registry.counters.broker_publications.saturating_add(1);
    }

    pub(crate) fn record_broker_error(&self, error: &WsBrokerError) {
        {
            let mut registry = self.registry();
            registry.counters.broker_errors = registry.counters.broker_errors.saturating_add(1);
        }
        if let WsBrokerError::Lagged(skipped) = error {
            self.observe(&WebSocketObservation::BrokerLagged {
                node: self.inner.node_id,
                skipped: *skipped,
            });
        }
    }

    fn observe_room_leaves(&self, id: WebSocketId, route: &str, rooms: &[String]) {
        for room in rooms {
            self.observe(&WebSocketObservation::RoomLeft { id, route, room });
        }
    }

    fn release(&self, id: WebSocketId) {
        let (became_empty, route, rooms) = {
            let mut registry = self.registry();
            let Some(entry) = registry.connections.remove(&id) else {
                return;
            };

            decrement_count(&mut registry.route_counts, &entry.route);
            if let Some(ip) = entry.remote_addr.map(|addr| addr.ip()) {
                decrement_count(&mut registry.ip_counts, &ip);
            }
            let rooms = entry.rooms.into_iter().collect::<Vec<_>>();
            remove_room_memberships(&mut registry.rooms, &entry.route, id, rooms.clone());
            registry.counters.room_leaves = registry
                .counters
                .room_leaves
                .saturating_add(u64::try_from(rooms.len()).unwrap_or(u64::MAX));
            registry.counters.closed_connections += 1;
            (registry.connections.is_empty(), entry.route, rooms)
        };

        self.observe_room_leaves(id, &route, &rooms);
        if became_empty {
            self.inner.empty.notify_waiters();
        }
        self.inner.registry_changed.notify_waiters();
    }

    fn registry(&self) -> MutexGuard<'_, Registry> {
        self.inner
            .registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn observe(&self, event: &WebSocketObservation<'_>) {
        let observer = self
            .inner
            .observer
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let _ = catch_unwind(AssertUnwindSafe(|| observer.observe(event)));
        trace_websocket_observation(event);
    }
}

async fn wait_for_broker_retry(shutdown: &mut watch::Receiver<bool>, delay: Duration) -> bool {
    tokio::select! {
        changed = shutdown.changed() => {
            let _ = changed;
            false
        }
        _ = tokio::time::sleep(delay) => !*shutdown.borrow(),
    }
}

fn validate_room(room: &str, max_room_name_bytes: usize) -> Result<(), WsError> {
    if room.is_empty() || room.contains('\0') || room.len() > max_room_name_bytes {
        return Err(WsError::InvalidRoom(room.to_string()));
    }
    Ok(())
}

fn remove_room_memberships(
    room_index: &mut HashMap<(String, String), HashSet<WebSocketId>>,
    route: &str,
    id: WebSocketId,
    rooms: Vec<String>,
) {
    for room in rooms {
        let key = (route.to_string(), room);
        let remove_key = room_index.get_mut(&key).is_some_and(|members| {
            members.remove(&id);
            members.is_empty()
        });
        if remove_key {
            room_index.remove(&key);
        }
    }
}

#[cfg(feature = "tracing")]
fn trace_websocket_message(
    id: WebSocketId,
    outbound: bool,
    bytes: usize,
    message_type: &'static str,
) {
    tracing::debug!(
        ws.id = %id,
        ws.outbound = outbound,
        ws.message_type = message_type,
        ws.bytes = bytes,
        "websocket message"
    );
}

#[cfg(not(feature = "tracing"))]
fn trace_websocket_message(
    _id: WebSocketId,
    _outbound: bool,
    _bytes: usize,
    _message_type: &'static str,
) {
}

#[cfg(feature = "tracing")]
fn trace_websocket_observation(event: &WebSocketObservation<'_>) {
    match event {
        WebSocketObservation::Accepted { id, route } => {
            tracing::debug!(ws.id = %id, ws.route = *route, "websocket accepted");
        }
        WebSocketObservation::Rejected { route, reason } => {
            tracing::warn!(ws.route = *route, ws.reason = *reason, "websocket rejected");
        }
        WebSocketObservation::Opened { id } => {
            tracing::info!(ws.id = %id, "websocket opened");
        }
        WebSocketObservation::Message { .. } => {}
        WebSocketObservation::QueueSaturated { id, outbound } => {
            tracing::warn!(ws.id = %id, ws.outbound = outbound, "websocket queue saturated");
        }
        WebSocketObservation::HeartbeatTimeout { id } => {
            tracing::warn!(ws.id = %id, "websocket heartbeat timed out");
        }
        WebSocketObservation::Closed { id, code, clean } => {
            tracing::info!(ws.id = %id, ws.code = ?code, ws.clean = clean, "websocket closed");
        }
        WebSocketObservation::HandlerFailed { id } => {
            tracing::error!(ws.id = %id, "websocket handler failed");
        }
        WebSocketObservation::ForcedShutdown { id } => {
            tracing::warn!(ws.id = %id, "websocket force-aborted during shutdown");
        }
        WebSocketObservation::RoomJoined { id, route, room } => {
            tracing::debug!(ws.id = %id, ws.route = *route, ws.room = *room, "websocket joined room");
        }
        WebSocketObservation::RoomLeft { id, route, room } => {
            tracing::debug!(ws.id = %id, ws.route = *route, ws.room = *room, "websocket left room");
        }
        WebSocketObservation::Broadcast {
            route,
            room_count,
            matched,
            enqueued,
            rejected,
            disconnected,
            remote,
        } => {
            tracing::debug!(
                ws.route = ?route,
                ws.room_count = room_count,
                ws.matched = matched,
                ws.enqueued = enqueued,
                ws.rejected = rejected,
                ws.disconnected = disconnected,
                ws.remote = ?remote,
                "websocket broadcast completed"
            );
        }
        WebSocketObservation::BrokerConnected { node } => {
            tracing::info!(ws.node = node.get(), "websocket broker connected");
        }
        WebSocketObservation::BrokerDisconnected { node, reason } => {
            tracing::warn!(ws.node = node.get(), ws.reason = ?reason, "websocket broker disconnected");
        }
        WebSocketObservation::BrokerLagged { node, skipped } => {
            tracing::warn!(
                ws.node = node.get(),
                ws.skipped = skipped,
                "websocket broker lagged"
            );
        }
        WebSocketObservation::BrokerInvalidPublication {
            origin,
            publication,
        } => {
            tracing::warn!(
                ws.origin = origin.get(),
                ws.publication = publication.get(),
                "websocket broker publication rejected"
            );
        }
    }
}

#[cfg(not(feature = "tracing"))]
fn trace_websocket_observation(_event: &WebSocketObservation<'_>) {}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        if !self.released {
            self.released = true;
            self.runtime.release(self.id);
        }
    }
}

fn snapshot(id: WebSocketId, entry: &ConnectionEntry) -> WebSocketConnectionSnapshot {
    WebSocketConnectionSnapshot {
        id,
        route: entry.route.clone(),
        remote_addr: entry.remote_addr,
        protocol: entry.protocol.clone(),
        opened_at: entry.opened_at,
        rooms: entry.rooms.iter().cloned().collect(),
        lifecycle: entry.lifecycle,
    }
}

fn decrement_count<K>(counts: &mut HashMap<K, usize>, key: &K)
where
    K: Eq + std::hash::Hash,
{
    if let Some(count) = counts.get_mut(key) {
        *count -= 1;
        if *count == 0 {
            counts.remove(key);
        }
    }
}
