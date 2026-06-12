use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::{Duration, SystemTime};

use tokio::sync::{Notify, watch};
use tokio::task::AbortHandle;

use super::ResolvedWebSocketConfig;
use super::socket::{InternalWebSocketSender, validate_close};
use super::types::{
    WebSocketConnectionSnapshot, WebSocketId, WebSocketObservation, WebSocketObserver,
    WebSocketStats,
};
use super::{WebSocketTimeout, WsError};

struct RuntimeInner {
    next_id: AtomicU64,
    registry: Mutex<Registry>,
    shutdown_tx: watch::Sender<bool>,
    empty: Notify,
    registry_changed: Notify,
    observer: RwLock<Arc<dyn WebSocketObserver>>,
}

struct Registry {
    accepting: bool,
    connections: HashMap<WebSocketId, ConnectionEntry>,
    route_counts: HashMap<String, usize>,
    ip_counts: HashMap<IpAddr, usize>,
    counters: WebSocketCounters,
}

#[derive(Clone)]
struct ConnectionEntry {
    route: String,
    remote_addr: Option<SocketAddr>,
    protocol: Option<String>,
    opened_at: SystemTime,
    close_timeout: Duration,
    internal_sender: Option<InternalWebSocketSender>,
    driver_abort: Option<AbortHandle>,
}

#[derive(Default)]
struct WebSocketCounters {
    accepted_connections: u64,
    rejected_connections: u64,
    closed_connections: u64,
    saturated_sends: u64,
    heartbeat_timeouts: u64,
}

#[derive(Clone)]
pub struct WebSocketRuntimeHandle {
    inner: Arc<RuntimeInner>,
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
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            inner: Arc::new(RuntimeInner {
                next_id: AtomicU64::new(1),
                registry: Mutex::new(Registry {
                    accepting: true,
                    connections: HashMap::new(),
                    route_counts: HashMap::new(),
                    ip_counts: HashMap::new(),
                    counters: WebSocketCounters::default(),
                }),
                shutdown_tx,
                empty: Notify::new(),
                registry_changed: Notify::new(),
                observer: RwLock::new(Arc::new(())),
            }),
        }
    }

    pub fn stats(&self) -> WebSocketStats {
        let registry = self.registry();
        WebSocketStats {
            active_connections: registry.connections.len(),
            accepted_connections: registry.counters.accepted_connections,
            rejected_connections: registry.counters.rejected_connections,
            closed_connections: registry.counters.closed_connections,
            saturated_sends: registry.counters.saturated_sends,
            heartbeat_timeouts: registry.counters.heartbeat_timeouts,
            ..WebSocketStats::default()
        }
    }

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

    pub fn connection(&self, id: WebSocketId) -> Option<WebSocketConnectionSnapshot> {
        let registry = self.registry();
        registry
            .connections
            .get(&id)
            .map(|entry| snapshot(id, entry))
    }

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
                            internal_sender: None,
                            driver_abort: None,
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

    pub async fn close(&self, id: WebSocketId, code: u16, reason: &str) -> Result<(), WsError> {
        validate_close(code, reason)?;
        let close_timeout = self
            .registry()
            .connections
            .get(&id)
            .map(|entry| entry.close_timeout)
            .ok_or(WsError::ConnectionNotFound(id))?;

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
        self.stop_accepting();
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
        let abort_handles = self
            .registry()
            .connections
            .values()
            .filter_map(|entry| entry.driver_abort.clone())
            .collect::<Vec<_>>();
        for abort_handle in abort_handles {
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

    pub(crate) fn record_saturated_send(&self) {
        self.registry().counters.saturated_sends += 1;
    }

    pub(crate) fn record_heartbeat_timeout(&self) {
        self.registry().counters.heartbeat_timeouts += 1;
    }

    fn release(&self, id: WebSocketId) {
        let became_empty = {
            let mut registry = self.registry();
            let Some(entry) = registry.connections.remove(&id) else {
                return;
            };

            decrement_count(&mut registry.route_counts, &entry.route);
            if let Some(ip) = entry.remote_addr.map(|addr| addr.ip()) {
                decrement_count(&mut registry.ip_counts, &ip);
            }
            registry.counters.closed_connections += 1;
            registry.connections.is_empty()
        };

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
    }
}

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
