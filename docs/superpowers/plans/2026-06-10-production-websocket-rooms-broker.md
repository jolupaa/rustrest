# WebSocket Rooms and Broker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add route-scoped rooms, Socket.IO-style server-side broadcast selectors, local connection administration, explicit delivery reports, and an object-safe external broker boundary without implementing the Socket.IO protocol.

**Architecture:** `WsHub` is a cloneable handle over the existing WebSocket runtime registry. Bidirectional room indexes live beside connection entries so driver cleanup removes memberships atomically. Broadcast selectors snapshot and deduplicate local senders, enqueue with bounded concurrency, then optionally publish a route/room target through `WsBroker`; remote publications are supervised and fan out only to local connections.

**Tech Stack:** Rust collections and synchronization, Tokio bounded channels/broadcast, futures-util boxed streams and bounded concurrency, Hyper `Bytes`, serde for event payloads, existing runtime/observer infrastructure.

---

## File Map

- Create `src/app/websocket/hub.rs`: hub builder, room membership, selectors,
  reports, local administration.
- Create `src/app/websocket/broker.rs`: broker types, trait, in-memory broker,
  subscription supervision.
- Modify `src/app/websocket/runtime.rs`: room indexes, sender snapshots, broker
  lifecycle, cleanup.
- Modify `src/app/websocket/socket.rs`: room APIs and selectors on socket/sender.
- Modify `src/app/websocket/types.rs`: room/broker observations and snapshots.
- Modify `src/app/websocket/error.rs`: room and broadcast errors.
- Modify `src/app/websocket.rs`, `src/app.rs`, `src/lib.rs`: additive exports.
- Modify `src/app/server.rs`: hub installation/startup.
- Modify `tests/websocket_api_compat.rs`: additive API compile fixture.
- Modify `tests/websocket_integration.rs`: real TCP rooms/broadcast behavior.
- Modify `src/app/websocket/tests.rs`: pure registry and broker tests.

### Task 1: Add `WsHub` and Atomic Route-Scoped Room Membership

**Files:**
- Create: `src/app/websocket/hub.rs`
- Modify: `src/app/websocket/runtime.rs`
- Modify: `src/app/websocket/socket.rs`
- Modify: `src/app/websocket/error.rs`
- Modify: `src/app/websocket/types.rs`
- Modify: `src/app/websocket.rs`
- Modify: `src/app/server.rs`
- Modify: `src/app.rs`
- Modify: `src/lib.rs`
- Modify: `src/app/websocket/tests.rs`
- Modify: `tests/websocket_api_compat.rs`

- [ ] **Step 1: Write failing pure room-registry tests**

Add unit tests covering idempotent join/leave, sorted room output,
`join_many` atomicity, route isolation, max-room count, max-name bytes, and
automatic cleanup when the connection permit drops.

Use these key assertions:

```rust
runtime.join(id, &["general", "equipo-7"]).unwrap();
runtime.join(id, &["general"]).unwrap();
assert_eq!(runtime.rooms(id).unwrap(), vec!["equipo-7", "general"]);

assert!(runtime.join(id, &["a", "b", "c"]).is_err());
assert_eq!(runtime.rooms(id).unwrap(), vec!["equipo-7", "general"]);

drop(permit);
assert!(runtime.rooms(id).is_none());
assert_eq!(runtime.local_room_size("/chat/:channel", "general"), 0);
```

- [ ] **Step 2: Run the tests and confirm failure**

Run:

```bash
cargo test websocket_rooms_are_route_scoped
```

Expected: FAIL because no room registry exists.

- [ ] **Step 3: Add bidirectional indexes to the runtime registry**

Extend registry state with:

```rust
type RoomKey = (String, String);

#[derive(Default)]
struct Registry {
    accepting: bool,
    connections: HashMap<WebSocketId, ConnectionEntry>,
    route_counts: HashMap<String, usize>,
    ip_counts: HashMap<IpAddr, usize>,
    rooms: HashMap<RoomKey, HashSet<WebSocketId>>,
    counters: WebSocketCounters,
}

struct ConnectionEntry {
    route: String,
    rooms: BTreeSet<String>,
    sender: Option<InternalWebSocketSender>,
    metadata: ConnectionMetadata,
    task: Option<AbortHandle>,
}
```

Use the normalized fully-mounted route pattern already stored during dispatch.
Do not use the concrete request path or route parameter values as implicit
room namespaces.

- [ ] **Step 4: Implement validation and atomic membership updates**

Room validation must reject empty strings, NUL, and names whose UTF-8 byte
length exceeds the effective limit. `join_many` must deduplicate requested
names, validate the complete result, then update both indexes under one mutex
lock. `leave_many` and `leave_all` update both indexes under one lock and are
idempotent.

Implement runtime methods with these signatures:

```rust
pub(crate) fn join(
    &self,
    id: WebSocketId,
    rooms: &[String],
) -> Result<(), WsError>;

pub(crate) fn leave(
    &self,
    id: WebSocketId,
    rooms: &[String],
) -> Result<(), WsError>;

pub(crate) fn leave_all(&self, id: WebSocketId) -> Result<(), WsError>;
pub(crate) fn rooms(&self, id: WebSocketId) -> Option<Vec<String>>;
pub(crate) fn local_room_size(&self, route: &str, room: &str) -> usize;
```

The existing `release(id)` path must remove the ID from every room and delete
empty room keys before removing the connection entry.

- [ ] **Step 5: Define `WsHub` and builder ownership**

In `hub.rs` add:

```rust
#[derive(Clone)]
pub struct WsHub {
    runtime: WebSocketRuntimeHandle,
    config: Arc<WsHubConfig>,
}

pub struct WsHubBuilder {
    max_rooms_per_connection: usize,
    max_room_name_bytes: usize,
    broadcast_concurrency: usize,
    broker_operation_timeout: Duration,
}
```

Defaults: 32 rooms, 128 bytes/name, 64 concurrent enqueue operations, and a
2-second future broker timeout. `build()` rejects zero values and creates a runtime
configured with these hard ceilings. `WsHub::local()` and `Default` build the
default local hub.

Expose `WsHub::builder() -> WsHubBuilder`, chainable setters for every builder
field, and `WsHubBuilder::build() -> Result<WsHub, WsError>`.

`App::new()` installs a local hub. Add:

```rust
pub fn websocket_hub(&mut self, hub: WsHub) -> &mut Self;
pub fn websocket_hub_handle(&self) -> WsHub;
```

Installing a hub replaces the runtime handle with `hub.runtime()`. Document
and test that this method is used before serving; `App` is not mutable once
consumed by `serve`, so runtime replacement cannot occur after startup.

- [ ] **Step 6: Add room APIs to `WebSocket` and `WebSocketSender`**

Both types delegate through shared connection metadata:

```rust
pub async fn join(&self, room: impl Into<String>) -> Result<(), WsError>;
pub async fn join_many<I, S>(&self, rooms: I) -> Result<(), WsError>
where
    I: IntoIterator<Item = S>,
    S: Into<String>;
pub async fn leave(&self, room: impl Into<String>) -> Result<(), WsError>;
pub async fn leave_many<I, S>(&self, rooms: I) -> Result<(), WsError>
where
    I: IntoIterator<Item = S>,
    S: Into<String>;
pub async fn leave_all(&self) -> Result<(), WsError>;
pub async fn rooms(&self) -> Result<Vec<String>, WsError>;
```

These methods are async for API consistency but must not hold a lock across an
await; the current local implementation completes synchronously inside the
async body.

- [ ] **Step 7: Extend the compile fixture and verify membership**

Add calls for `WsHub::local`, `app.websocket_hub`, socket/sender join/leave,
rooms, and `app.websocket_hub_handle` to `tests/websocket_api_compat.rs`.

Run:

```bash
cargo fmt
cargo test websocket_room
cargo test --test websocket_api_compat
cargo check --all-targets
```

Expected: PASS.

- [ ] **Step 8: Commit hub and room membership**

```bash
git add src/app/websocket src/app/websocket.rs src/app/server.rs src/app.rs src/lib.rs tests/websocket_api_compat.rs
git commit -m "feat: add route scoped websocket rooms"
```

### Task 2: Add Broadcast Selectors, Deduplication, and Explicit Reports

**Files:**
- Modify: `src/app/websocket/hub.rs`
- Modify: `src/app/websocket/runtime.rs`
- Modify: `src/app/websocket/socket.rs`
- Modify: `src/app/websocket/error.rs`
- Modify: `src/app/websocket/types.rs`
- Modify: `tests/websocket_integration.rs`
- Modify: `src/app/websocket/tests.rs`

- [ ] **Step 1: Write failing real TCP broadcast tests**

Connect clients to `/chat/:channel` and `/admin/chat/:channel`. Join overlapping
rooms and assert:

- `socket.to("general")` excludes the sender;
- `to_many(["general", "equipo-7"])` delivers once to a client in both rooms;
- `/chat/:channel` never reaches `/admin/chat/:channel`;
- `hub.route("/chat/:channel").all()` includes the caller unless explicitly
  excluded;
- `hub.all()` crosses route scopes;
- partial enqueue failure produces `rejected > 0` and no silent loss.

- [ ] **Step 2: Run broadcast tests and confirm failure**

Run:

```bash
cargo test --test websocket_integration websocket_room_broadcast
```

Expected: FAIL.

- [ ] **Step 3: Define selectors and report types**

In `hub.rs` define:

```rust
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
```

Define non-exhaustive `WsBroadcastError` with invalid control message and
broker/local report variants. For every returned local report enforce
`matched == enqueued + rejected + disconnected`.

- [ ] **Step 4: Implement deterministic local selection**

Under one registry lock, select connection IDs by:

- route+room union for `to`/`to_many`;
- all entries with matching route for `WsRoute::all`;
- all entries for `WsHub::all`;
- remove every excluded ID;
- deduplicate in a `HashSet`;
- sort IDs before cloning internal senders for deterministic tests.

Release the lock before enqueueing. A missing/closed sender counts as
`disconnected`, queue capacity failure counts as `rejected`, and successful
enqueue counts as `enqueued`.

- [ ] **Step 5: Bound fan-out concurrency**

Use `futures_util::stream::iter(senders)`, map each sender to its existing
bounded enqueue future with a cloned application message, then apply
`.buffer_unordered(limit)` with the hub's nonzero `broadcast_concurrency`.
Each enqueue uses the target connection's configured backpressure policy and
timeout. Do not spawn one untracked task per recipient.

Reject Ping, Pong, and Close in `WsTarget::send`; selectors accept only text
and binary application messages. Add `send_text`, `send_binary`, `send_json`,
and `send_event` conveniences.

- [ ] **Step 6: Add socket and hub selector entry points**

Implement:

```rust
impl WebSocketSender {
    pub fn to(&self, room: impl Into<String>) -> WsTarget;
    pub fn to_many<I, S>(&self, rooms: I) -> WsTarget
    where I: IntoIterator<Item = S>, S: Into<String>;
    pub fn broadcast(&self) -> WsTarget;
}

impl WsHub {
    pub fn route(&self, route: impl Into<String>) -> WsRoute;
    pub fn all(&self) -> WsTarget;
}

impl WsRoute {
    pub fn to(&self, room: impl Into<String>) -> WsTarget;
    pub fn to_many<I, S>(&self, rooms: I) -> WsTarget
    where I: IntoIterator<Item = S>, S: Into<String>;
    pub fn all(&self) -> WsTarget;
    pub async fn local_room_size(&self, room: &str) -> usize;
}

impl WsTarget {
    pub fn except(mut self, id: WebSocketId) -> Self;
}
```

Socket-created targets set `route`, exclude their own ID, and later publish
remotely. Hub-created targets include all matches unless `.except` is called.

- [ ] **Step 7: Verify dedupe, exclusion, and partial reports**

Run:

```bash
cargo fmt
cargo test websocket_broadcast_report
cargo test --test websocket_integration websocket_room_broadcast
cargo test --test websocket_integration websocket_multi_room_deduplicates
```

Expected: PASS.

- [ ] **Step 8: Commit local broadcast selectors**

```bash
git add src/app/websocket tests/websocket_integration.rs
git commit -m "feat: broadcast websocket messages to rooms"
```

### Task 3: Add Local Direct Send, Disconnect, and Runtime Room Snapshots

**Files:**
- Modify: `src/app/websocket/hub.rs`
- Modify: `src/app/websocket/runtime.rs`
- Modify: `src/app/websocket/types.rs`
- Modify: `tests/websocket_integration.rs`

- [ ] **Step 1: Write failing local administration tests**

Capture a connection ID in the handler, then from the test call direct send and
disconnect. Assert the client receives the event, then Close 1008 with the
provided Spanish reason. Assert an unknown ID returns `None`/not-found and a
snapshot lists sorted room names but no payload.

- [ ] **Step 2: Run and confirm failure**

Run:

```bash
cargo test --test websocket_integration websocket_local_administration
```

Expected: FAIL.

- [ ] **Step 3: Expose a restricted local socket handle**

Define `WsLocalSocket` containing connection metadata plus an internal sender.
Expose only outbound message methods, close methods, and metadata; do not expose
an inbound receiver. Implement:

```rust
pub fn local_socket(&self, id: WebSocketId) -> Option<WsLocalSocket>;
pub async fn disconnect_local(
    &self,
    id: WebSocketId,
    code: u16,
    reason: &str,
) -> Result<(), WsError>;
pub fn local_connection_count(&self) -> usize;
```

- [ ] **Step 4: Include rooms in runtime snapshots**

`WebSocketConnectionSnapshot` includes sorted room names, route, protocol,
remote address, opened timestamp, and lifecycle state. It must not include
message contents, cookies, authorization headers, or arbitrary request state.

- [ ] **Step 5: Verify local administration**

Run:

```bash
cargo fmt
cargo test --test websocket_integration websocket_local_administration
cargo test websocket_connection_snapshot
```

Expected: PASS.

- [ ] **Step 6: Commit local administration**

```bash
git add src/app/websocket tests/websocket_integration.rs
git commit -m "feat: administer local websocket connections"
```

### Task 4: Preserve and Clarify the Existing `WsBroadcast` Helper

**Files:**
- Modify: `src/app/websocket.rs`
- Modify: `src/app/websocket/tests.rs`
- Modify: `tests/websocket_api_compat.rs`

- [ ] **Step 1: Add an explicit lag test**

Create a capacity-1 `WsBroadcast`, send two messages without receiving, and
assert the receiver returns `broadcast::error::RecvError::Lagged(1)`. This
locks the existing non-silent lag behavior.

- [ ] **Step 2: Run the lag test**

```bash
cargo test ws_broadcast_reports_lag
```

Expected: PASS with the current helper. If it fails, fix only the helper's
explicit error behavior without adding rooms to it.

- [ ] **Step 3: Document separation from `WsHub` in rustdoc**

State that `WsBroadcast` is a raw local Tokio broadcast channel; it does not
track sockets, routes, rooms, backpressure reports, or brokers. Point room use
cases to `WsHub`.

- [ ] **Step 4: Verify compatibility**

```bash
cargo fmt
cargo test ws_broadcast
cargo test --test websocket_api_compat
```

Expected: PASS.

- [ ] **Step 5: Commit helper clarification**

```bash
git add src/app/websocket.rs src/app/websocket/tests.rs tests/websocket_api_compat.rs
git commit -m "docs: distinguish websocket hub from raw broadcast"
```

### Task 5: Define the Object-Safe Broker Contract and In-Memory Broker

**Files:**
- Create: `src/app/websocket/broker.rs`
- Modify: `src/app/websocket/hub.rs`
- Modify: `src/app/websocket/error.rs`
- Modify: `src/app/websocket/types.rs`
- Modify: `src/app/websocket.rs`
- Modify: `src/app.rs`
- Modify: `src/lib.rs`
- Modify: `src/app/websocket/tests.rs`

- [ ] **Step 1: Write failing broker contract tests**

Test publish/subscribe, lag/error exposure, independent subscribers, origin
node echo suppression input, and stream termination. Use two subscribers to
one `InMemoryWsBroker`.

- [ ] **Step 2: Run and confirm failure**

```bash
cargo test websocket_in_memory_broker
```

Expected: FAIL.

- [ ] **Step 3: Add strongly typed publication identifiers and payloads**

Define:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WsNodeId(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WsPublicationId(u64);

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct WsBrokerPublication {
    pub id: WsPublicationId,
    pub origin: WsNodeId,
    pub target: WsBrokerTarget,
    pub payload: WsBrokerPayload,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum WsBrokerTarget {
    RouteRooms { route: String, rooms: Vec<String> },
    RouteAll { route: String },
    AllRoutes,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum WsBrokerPayload {
    Text(String),
    Binary(Bytes),
}
```

Node IDs are unique within the process and configurable through the hub builder
for external orchestrators. Publication IDs are monotonic per node; the pair
`(origin, id)` is the dedupe key.

Expose lossless construction and inspection for broker adapters:

```rust
impl WsNodeId {
    pub const fn new(value: u64) -> Self;
    pub const fn get(self) -> u64;
}

impl WsPublicationId {
    pub const fn new(value: u64) -> Self;
    pub const fn get(self) -> u64;
}
```

- [ ] **Step 4: Implement the object-safe trait exactly**

```rust
pub type WsBrokerStream = Pin<
    Box<dyn Stream<Item = Result<WsBrokerPublication, WsBrokerError>> + Send>,
>;

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
```

`WsBrokerError` is non-exhaustive, implements `Error`, and includes
`Unavailable`, `Lagged(u64)`, `InvalidPublication(String)`, and
`SubscriptionClosed`.

- [ ] **Step 5: Implement `InMemoryWsBroker` with explicit lag**

Back it with `Mutex<Option<tokio::sync::broadcast::Sender<WsBrokerPublication>>>`.
Expose `close()` for deterministic tests by taking the sender from the option.
Convert
`RecvError::Lagged(n)` to `WsBrokerError::Lagged(n)` in the returned boxed
stream; convert closed to stream termination only after yielding one
`SubscriptionClosed` error. `publish` returns unavailable when no receiver is
present only if the broker itself has been closed; zero current subscribers is
otherwise a valid publish.

- [ ] **Step 6: Add broker configuration to `WsHubBuilder`**

Extend the builder and resolved hub config with:

```rust
broker: Option<Arc<dyn WsBroker>>,
node_id: Option<WsNodeId>,
```

Add:

```rust
pub fn broker(mut self, broker: Arc<dyn WsBroker>) -> Self;
pub fn node_id(mut self, node_id: WsNodeId) -> Self;
```

When no node ID is supplied, allocate one from a process-local atomic counter.
The existing `broker_operation_timeout` setting becomes active here.

- [ ] **Step 7: Export broker types and verify object safety**

Re-export `WsBroker`, all publication/error/ID types, `WsBrokerStream`, and
`InMemoryWsBroker` from `src/app.rs` and `src/lib.rs`.

Add a compile assertion:

```rust
fn assert_object_safe(_: Arc<dyn WsBroker>) {}
assert_object_safe(Arc::new(InMemoryWsBroker::new(64)));
```

Run:

```bash
cargo fmt
cargo test websocket_in_memory_broker
cargo test --test websocket_api_compat
cargo check --all-targets
```

Expected: PASS.

- [ ] **Step 8: Commit the broker boundary**

```bash
git add src/app/websocket src/app/websocket.rs src/app.rs src/lib.rs
git commit -m "feat: define websocket broker contract"
```

### Task 6: Supervise Broker Subscriptions and Broadcast Across Two Nodes

**Files:**
- Modify: `src/app/websocket/broker.rs`
- Modify: `src/app/websocket/hub.rs`
- Modify: `src/app/websocket/runtime.rs`
- Modify: `src/app/websocket/types.rs`
- Modify: `tests/websocket_integration.rs`
- Modify: `src/app/websocket/tests.rs`

- [ ] **Step 1: Write failing two-node tests**

Build two hubs with distinct node IDs and one `InMemoryWsBroker`. Attach one
app/server per hub, connect one client to each, join the same route-scoped
room, publish from node A, and assert:

- A's non-origin local peers receive once;
- A's originating socket is excluded for socket-created targets;
- B's matching peer receives once;
- no client receives a duplicate;
- route isolation remains intact across nodes;
- local delivery still occurs when broker publish fails and the returned error
  contains the completed local report.

- [ ] **Step 2: Run and confirm failure**

```bash
cargo test --test websocket_integration websocket_broker_two_nodes
```

Expected: FAIL.

- [ ] **Step 3: Start broker supervision at server startup**

Do not spawn in `WsHubBuilder::build`, because apps may be constructed outside
a Tokio runtime. Add `runtime.start_broker().await` at the beginning of plain
and TLS serve paths. Make startup idempotent.

The supervisor must:

1. call `subscribe(node_id)` with the configured operation timeout;
2. consume the stream until error/end/shutdown;
3. emit broker state observations;
4. retry with bounded exponential delays 100ms, 200ms, 400ms, 800ms, then 1s;
5. stop immediately on runtime shutdown.

- [ ] **Step 4: Validate and deduplicate incoming publications**

Before fan-out, validate route/room lengths, room count, payload size against
hub ceilings, and target shape. Ignore publications whose origin equals the
local node. Keep a bounded 4,096-entry `(origin, publication_id)` seen set
using `HashSet` plus `VecDeque`; ignore duplicates and evict oldest entries.

Remote fan-out calls the same local selection/enqueue path with
`publish_remote = false`, preventing loops.

- [ ] **Step 5: Publish after local fan-out and preserve partial outcomes**

For socket/hub targets, perform local enqueue first. If no broker is configured,
return `WsRemotePublish::NotConfigured`. Otherwise publish within the broker
timeout and return `Published`. On failure, return
`WsBroadcastError::Broker { source, local_report }`; never hide completed local
delivery.

Remote target mapping is:

- route + rooms -> `RouteRooms`;
- route all -> `RouteAll`;
- hub all -> `AllRoutes`.

Do not serialize connection IDs into broker targets; sender exclusion is only
needed on the origin node.

- [ ] **Step 6: Verify cross-node behavior and retry**

Run:

```bash
cargo fmt
cargo test websocket_broker_subscription_recovers
cargo test --test websocket_integration websocket_broker_two_nodes
cargo test --test websocket_integration websocket_broker_failure_keeps_local_report
```

Expected: PASS.

- [ ] **Step 7: Commit broker supervision and cross-node rooms**

```bash
git add src/app/websocket tests/websocket_integration.rs src/app/server.rs src/app/tls.rs
git commit -m "feat: broadcast websocket rooms across broker nodes"
```

### Task 7: Observe Rooms, Broadcasts, and Broker Health

**Files:**
- Modify: `src/app/websocket/types.rs`
- Modify: `src/app/websocket/runtime.rs`
- Modify: `src/app/websocket/hub.rs`
- Modify: `src/app/websocket/broker.rs`
- Modify: `src/app/websocket/tests.rs`

- [ ] **Step 1: Write failing observation/statistics tests**

Assert observer events for join, leave, broadcast report, broker connected,
broker disconnected, broker lag, and invalid remote publication. Assert stats
track active non-empty room keys and broker health without exposing payload.

- [ ] **Step 2: Run and confirm failure**

```bash
cargo test websocket_room_observation
cargo test websocket_broker_observation
```

Expected: FAIL.

- [ ] **Step 3: Extend observation metadata without payloads**

Add non-exhaustive variants carrying IDs, route, room name, counts, and error
category only. Broadcast observations carry `matched/enqueued/rejected/
disconnected` and remote status, never the text/binary body.

- [ ] **Step 4: Extend stats coherently**

Track active non-empty room keys, total room joins/leaves, local broadcasts,
partial broadcasts, broker publications, broker errors, and broker-connected
state. Update under the same registry lock or atomics used by the core runtime;
`stats()` must remain one coherent snapshot.

- [ ] **Step 5: Verify observation panic isolation still holds**

Run:

```bash
cargo fmt
cargo test websocket_room_observation
cargo test websocket_broker_observation
cargo test websocket_observer_panic_is_isolated
cargo check --features tracing
```

Expected: PASS.

- [ ] **Step 6: Commit room/broker observability**

```bash
git add src/app/websocket
git commit -m "feat: observe websocket rooms and broker health"
```

### Task 8: Rooms and Broker Phase Verification Gate

**Files:**
- Modify only files needed to fix gate failures.

- [ ] **Step 1: Run the complete feature matrix**

```bash
cargo fmt --check
cargo check --all-targets
cargo check --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo test --features tls
cargo test --all-features
cargo doc --no-deps --all-features
```

Expected: every command exits 0.

- [ ] **Step 2: Run focused room/broker tests serially to detect shared-state leaks**

```bash
cargo test websocket_room -- --test-threads=1
cargo test websocket_broker -- --test-threads=1
cargo test --test websocket_integration websocket_room -- --test-threads=1
cargo test --test websocket_integration websocket_broker -- --test-threads=1
```

Expected: PASS.

- [ ] **Step 3: Review invariants manually**

Confirm:

- room membership exists in exactly two synchronized indexes;
- connection release removes all memberships;
- multi-room selection deduplicates IDs;
- no registry lock survives an await;
- fan-out concurrency is bounded;
- every local partial failure is represented in the report;
- broker echo and duplicate publications are suppressed;
- remote publications never republish;
- direct socket control remains local-process only.

- [ ] **Step 4: Commit only verification fixes, if any**

```bash
git add src tests Cargo.toml Cargo.lock
git commit -m "fix: close websocket rooms verification gaps"
```

Skip this commit when no files changed.
