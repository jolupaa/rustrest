# Production WebSocket Design

## Status

Approved design for upgrading RustRest's native WebSocket support while
preserving the public API introduced in v0.2.

## Objective

Make RustRest WebSockets suitable for production workloads on a single
application instance, with explicit extension points for external brokers in
multi-node deployments. The target capacity per instance is 10,000 idle
connections and 1,000 concurrently active connections under a reproducible
load test. The framework also provides native route-scoped rooms and
broadcast selectors comparable to the server-side room primitives in
Socket.IO, while continuing to speak standard WebSocket rather than the
Socket.IO protocol.

"Production-ready" means bounded resource use, deterministic shutdown,
protocol-correct lifecycle handling, security controls before upgrade,
observable behavior, panic isolation, documented deployment constraints, and
no silent message loss.

## Compatibility

The existing APIs remain source-compatible:

- `App::websocket`, `App::ws`, and `App::websocket_with`
- the equivalent `Router` methods
- `Request::websocket` and `Request::websocket_with`
- `WebSocket::recv`, `send`, `send_text`, `send_binary`, JSON/event helpers,
  `ping`, `pong`, `close`, and `protocol`
- `WsBroadcast`
- handlers whose future returns `()`

New capabilities are additive. Existing defaults preserve current behavior
unless retaining it would permit unbounded resource use. Any bounded default
introduced for safety must be large enough for normal existing applications
and documented in the release notes.

Handlers may additionally return `Result<(), WebSocketError>` or
`Result<(), WsError>`. This is implemented through output normalization in
`IntoWebSocketHandler`, using the same marker-based approach as HTTP handlers
where needed to preserve type inference.

## Non-goals

The framework will not implement:

- Socket.IO or its transport protocol
- client-side reconnection
- durable message storage or replay
- delivery acknowledgements beyond WebSocket/TCP semantics
- bundled Redis, NATS, or Kafka clients
- transparent cross-node presence
- Socket.IO namespaces, acknowledgement packets, or client protocol
- WebSocket over HTTP/2 extended CONNECT in this phase
- application-level schemas for event payloads

## Architecture

### Connection runtime

`App` owns a shared `WebSocketRuntime`. Every accepted WebSocket upgrade is
registered with the runtime before its handler starts and removed after the
connection driver finishes. Registration stores only operational metadata:
connection ID, route, remote address, negotiated subprotocol, timestamps, and
current lifecycle state.

The runtime provides:

- connection IDs unique within the running process
- active/accepted/rejected/closed counters
- global and per-route connection accounting
- per-IP connection accounting
- a cancellation signal for server shutdown
- a supervised task registry for connection drivers
- a bounded shutdown deadline followed by forced task abortion
- a metrics observer and lifecycle hooks

The runtime is internal by default. A read-only `WebSocketRuntimeHandle` may be
obtained from `App` state for statistics and administrative shutdown. The
handle cannot access application payloads.

### Connection driver

After Hyper completes the HTTP/1.1 upgrade, one driver task exclusively owns
the `tokio_tungstenite::WebSocketStream`. This avoids concurrent mutable access
to the transport and permits heartbeat, shutdown, inbound traffic, and
outbound traffic to progress independently.

The driver multiplexes:

- incoming frames from the peer
- a bounded outbound `mpsc` queue
- heartbeat timers
- idle and lifetime timers
- the runtime shutdown signal
- handler completion or panic notification

The user-facing `WebSocket` becomes a handle over bounded channels rather than
the direct owner of the transport. Its existing async methods retain their
behavior. The connection driver delivers inbound application messages through
a bounded queue and processes control frames internally.

Control frames receive priority over application messages so that shutdown,
Ping, Pong, and Close cannot remain blocked behind a full application queue.
Control traffic has a separate, very small bounded channel and coalesces
duplicate heartbeat work rather than growing without bound.

### Handler supervision

The handler runs in a supervised task associated with the driver. A panic is
captured at the task boundary, recorded, and causes that connection to close
with code 1011. It never unwinds into the accept loop or affects another
connection.

Dropping the handler's final `WebSocket`/sender handle initiates a normal close
unless the peer or runtime already started closure. Returning
`Result<(), WebSocketError>` or `Result<(), WsError>` records the error and
maps it to an appropriate close reason. Returning `()` retains existing
behavior.

### Split API

`WebSocket::split()` returns:

- a single `WebSocketReceiver` for inbound messages
- a clonable `WebSocketSender` for outbound messages

There remains only one logical receiver. `WebSocketSender::send` waits for
bounded queue capacity, while `try_send` immediately reports `Full` or
`Closed`. This makes fan-out and independent producer tasks possible without
exposing the raw Tungstenite stream.

### Hub, route scopes, and rooms

Every `App` has one `WsHub`, backed by the same connection registry as the
`WebSocketRuntime`. The default hub is local to the process. Applications may
install a hub configured with a `WsBroker` before registering or serving
WebSocket routes.

The normalized, fully mounted route pattern is the room namespace. For
example, room `general` on `/chat/:channel` is distinct from room `general` on
`/admin/chat/:channel`; concrete parameter values do not create implicit
namespaces. Applications that want one room per parameter explicitly join the
parameter value as a room.
`WebSocket::join`, `leave`, `rooms`, `to`, and `broadcast` operate within the
socket's route scope. Administrative broadcasts use
`hub.route("/chat")`. `hub.all()` is the explicit operation that crosses route
scopes.

Room membership is maintained locally by bidirectional indexes:

- route scope + room -> connection IDs
- connection ID -> route-scoped rooms

Joining an existing room is idempotent. Leaving a room that was not joined is
also idempotent. All memberships are removed when the driver exits, including
panic, failed upgrade, forced shutdown, and abrupt transport failure paths.
Room names are non-empty UTF-8 strings with configurable byte-length and
per-connection count limits.

`join_many` validates every room and the resulting membership count before
changing either index, so it is atomic on validation or capacity failure.
`leave_many` is idempotent and removes all requested memberships in one local
registry operation. `rooms()` returns a sorted `Vec<String>` for deterministic
tests and diagnostics.

Broadcast selectors take a snapshot of matching connection senders, dedupe by
connection ID, and then enqueue with bounded concurrency. A connection that
matches multiple selected rooms receives one copy. `socket.to(room)` and
`socket.broadcast()` exclude the originating connection; hub selectors include
all matches unless `.except(id)` is applied.

Messages sent through one `WebSocketSender` retain enqueue order for that
connection. A broadcast does not promise global ordering across different
connections or broker nodes.

Every broadcast returns a `WsBroadcastReport` containing local matched,
enqueued, rejected, and disconnected counts plus remote publication status.
Partial delivery is never reported as full success and no queue overflow is
silently discarded. The report describes enqueueing into each connection's
bounded queue, not receipt
by the remote application; WebSocket itself supplies no application-level
acknowledgement.

`hub.local_socket(id)` deliberately addresses only this process because
connection IDs are process-local. Route-room broadcast may cross nodes through
the broker. Global connection lookup, global room counts, distributed
presence, and direct remote socket control require adapter-specific services
and are not implied by `WsBroker`.

## Configuration

`WebSocketConfig` remains a builder and adds the following optional settings:

- `max_frame_size`
- `max_write_buffer_size`
- `ping_interval`
- `pong_timeout`
- `idle_timeout`
- `max_connection_lifetime`
- `close_timeout`
- `outbound_capacity`
- `inbound_capacity`
- `backpressure_policy`
- `origin_policy`
- `require_protocol`
- `max_connections`
- `max_connections_per_ip`
- `message_rate_limit`
- `max_rooms_per_connection`
- `max_room_name_bytes`

Server-wide defaults live in a WebSocket runtime configuration on `App`.
Route-level `WebSocketConfig` values override those defaults. Limits are
checked in this order: process, route, then IP. Counters are reserved before
returning HTTP 101 and released on every failure path.

Hub room limits are hard process-wide ceilings. Route configuration may lower
`max_rooms_per_connection` and `max_room_name_bytes`, but cannot exceed the hub
ceiling.

All queues and Tungstenite buffers are bounded. Configuration rejects zero
capacities, inconsistent heartbeat values, invalid close durations, and
unsafe write-buffer relationships at route registration time where possible.

### Backpressure

`BackpressurePolicy` supports:

- `Wait`: async `send` waits for capacity up to a configurable send timeout
- `Reject`: the current send returns a capacity error
- `Disconnect`: the driver closes a persistently slow connection with code
  1013

The default is `Wait` with a finite timeout. `try_send` always rejects
immediately when full regardless of the configured policy. No policy silently
drops a message. `WsBroadcast` continues to expose Tokio broadcast lag as an
explicit receiver error; framework examples must handle that error rather than
ignore it.

## Protocol Lifecycle

### Handshake

Before upgrading, RustRest validates:

- method is GET
- HTTP version is HTTP/1.1, which supports the classic upgrade
- `Upgrade: websocket`
- `Connection` contains `upgrade`
- `Sec-WebSocket-Version` is exactly 13
- `Sec-WebSocket-Key` decodes to 16 bytes
- configured origin policy
- connection capacity
- required subprotocol negotiation

Failures return an HTTP response before upgrade. Version mismatch returns 426
with `Sec-WebSocket-Version: 13`. Authentication and authorization remain
normal middleware concerns and run before the route handler initiates the
upgrade.

### Heartbeat and liveness

Heartbeat no longer depends on application calls to `recv()`.

The driver sends Ping after `ping_interval` without inbound traffic and records
the payload token and send instant. A matching Pong clears the pending probe.
If no valid Pong arrives within `pong_timeout`, the driver closes the
connection and records a heartbeat timeout. Incoming Ping receives an
automatic Pong. To preserve existing behavior, Ping, Pong, and Close frames
are also delivered by `recv()` after the driver performs mandatory protocol
handling. If the bounded inbound queue cannot accept them within its deadline,
the connection closes as a slow consumer instead of silently dropping them.

`idle_timeout` measures absence of application data frames. It is separate
from heartbeat liveness. `max_connection_lifetime` limits total connection
duration when configured.

### Closing

The driver implements the RFC 6455 closing handshake:

1. The initiator sends one Close frame with a valid code and UTF-8 reason.
2. On peer Close, the driver echoes a Close if it has not already sent one.
3. It waits up to `close_timeout` for the peer/transport to finish.
4. It then releases runtime accounting and terminates the tasks.

`close_with(code, reason)` validates allowed close codes and the 123-byte
encoded reason limit. `close()` remains shorthand for a normal 1000 close.
The final close code, reason, initiator, and cleanliness are exposed through a
`WebSocketCloseInfo` value and observability hooks.

### Server shutdown

When `serve_with_shutdown` or its TLS equivalent receives the shutdown signal:

1. The listener stops accepting new TCP connections.
2. The runtime rejects pending upgrade attempts with 503.
3. Every active WebSocket receives Close 1001 with reason `server shutdown`.
4. Drivers wait for peers until the configured WebSocket shutdown grace
   period expires.
5. Remaining driver and handler tasks are aborted.
6. HTTP graceful shutdown completes after the WebSocket registry is empty or
   the server's outer deadline expires.

Plain and TLS servers share the same runtime shutdown implementation.

## Security

### Origin policy

`OriginPolicy` supports:

- `Any`, preserving compatibility
- `SameHost`
- `AllowList`
- control over clients that omit `Origin`

Matching normalizes scheme and host, handles default ports, and never uses
substring matching. Documentation recommends `SameHost` or an allowlist for
browser-facing authenticated endpoints. CORS headers are not treated as
WebSocket origin authorization.

### Resource protection

The framework enforces:

- process, route, and IP connection limits
- handshake rate limits through existing middleware or a dedicated hook
- per-connection message rate limits
- frame, message, inbound queue, outbound queue, and write-buffer limits
- finite heartbeat, send, close, and shutdown deadlines
- bounded close reasons and diagnostic strings

Rate-limit and capacity rejections occur before upgrade with 429 or 503 and a
`Retry-After` header where meaningful. Message-rate violations close with
1008. Oversized messages close with 1009.

## Error Model

`WebSocketError` keeps exactly its current public variants, `Protocol` and
`Json`, because adding variants would break downstream exhaustive matches.
Existing methods keep their current return types. New runtime-aware APIs use a
separate non-exhaustive `WsError` with structured variants for:

- protocol/transport failures
- JSON failures
- timeout kind
- capacity/backpressure failure
- invalid configuration
- invalid close frame
- invalid room or room-capacity failure
- shutdown
- handler panic
- channel closed

`WsError` implements `From<WebSocketError>`. Existing `WebSocket::send` and
`recv` map internal channel timeout/closure states to the closest Tungstenite
protocol or I/O error to preserve their signatures. New `WebSocketSender`,
room, and runtime APIs expose the precise `WsError`.
`WebSocketError::category()` is added as a non-breaking helper for stable
classification.

Errors retain their source where applicable and expose a stable category for
metrics. Expected peer disconnects are not logged as server errors. Public
error messages never include payload contents.

## Observability

### Tracing

With the existing `tracing` feature, each connection gets a span containing
connection ID, route, remote address, and negotiated subprotocol. Events cover
acceptance, rejection, handler failure, heartbeat timeout, backpressure,
closure, and forced shutdown. Message payloads are excluded. At most message
type and byte length are recorded.

### Metrics

The core defines a lightweight `WebSocketObserver` trait with no dependency on
a metrics backend. A no-op implementation is the default. Callbacks report:

- accepted, rejected, active, and closed connections
- sent/received message and byte counts by frame class
- close codes and initiators
- protocol, handler, heartbeat, and capacity errors
- outbound queue wait duration and saturation
- connection lifetime
- room joins/leaves, broadcast targets, partial delivery, and broker health

Observer callbacks must be non-blocking and panic-isolated. Prometheus,
OpenTelemetry, or other integrations live in external crates or application
code.

### Hooks

Optional lifecycle hooks cover connect, close, and error. A per-message hook
receives metadata only, not payloads. Hooks cannot mutate protocol state and
must not run on the driver's critical polling path if they can block.

## Broker Extensibility

`WsBroadcast` remains the local-process helper. A new `WsBroker` trait provides
an optional boundary for external fan-out systems. It deals in application
topics and byte payloads, not live socket objects, and exposes subscription
lag/disconnection explicitly.

The core contains an in-memory broker implementation used for tests and small
deployments. Redis/NATS adapters belong in separate crates. Delivery,
ordering, replay, and persistence guarantees are documented by each adapter;
the core does not imply stronger semantics than the adapter provides.

Broker subjects encode both the normalized route scope and room name. Each
node receives remote publications and fans them out only to its local matching
connections. Publications carry an origin node identifier so they are not
echoed twice on the publishing node. Broker payloads contain the serialized
WebSocket application message and targeting metadata, but never connection
handles.

`WsBroker` reports publish failures, subscription loss, and lag explicitly.
The hub applies finite broker operation deadlines. A local enqueue can succeed
while remote publication fails; `WsBroadcastReport` exposes those outcomes
separately. No adapter may claim global room size or presence unless it
implements and documents that additional capability.

## Public API Additions

The additive surface includes:

- `WebSocket::id()`
- `WebSocket::remote_addr()`
- `WebSocket::split()`
- `WebSocket::close_with(code, reason)`
- `WebSocket::closed()`
- `WebSocket::{route, join, leave, leave_all, rooms, to, broadcast}`
- `WebSocketSender::{send, try_send, close, close_with, closed}`
- `WebSocketSender::{join, join_many, leave, leave_many, leave_all, rooms}`
- `WebSocketSender::{to, to_many, broadcast}`
- `WebSocketReceiver::recv()`
- `WebSocketReceiver::closed()`
- `WebSocketCloseInfo`
- `WsError`, `WebSocketErrorCategory`, `WebSocketTimeout`, and
  `WebSocketCapacityError`
- `WebSocketRuntimeHandle::stats()`
- `WebSocketStats`
- `BackpressurePolicy`
- `OriginPolicy`
- `WebSocketObserver`
- `WsHub`, `WsHubBuilder`, `WsRoute`, and `WsTarget`
- `WsBroadcastReport`, `WsRemotePublish`, and `WsBroadcastError`
- `WsBroker`, `WsBrokerPublication`, `WsBrokerTarget`, and `WsBrokerPayload`
- `WsNodeId`, `WsPublicationId`, `WsBrokerError`, and `WsBrokerStream`

Existing direct methods delegate to the same channel-backed implementation as
the split handles.

New public enums and snapshot/report structs are marked `#[non_exhaustive]`
where future extension is expected. The existing `WebSocketError` is the sole
exception because changing its exhaustiveness would itself be a compatibility
break.

## Proposed Public API

This section fixes the intended public shape before the implementation plan.
Exact generic helper traits may differ internally, but the calls shown here
must compile.

### Route registration

Existing registration remains valid on `App`, `Router`, and `Request`:

```rust
app.websocket("/ws", handler);
app.ws("/ws", handler);
app.websocket_with("/ws", config, handler);

router.websocket("/ws", handler);
router.ws("/ws", handler);
router.websocket_with("/ws", config, handler);

req.websocket(handler);
req.websocket_with(config, handler);
```

Handlers may return `()`, `Result<(), WebSocketError>`, or
`Result<(), WsError>`:

```rust
app.websocket("/ws", |mut socket| async move {
    while let Some(message) = socket.recv().await? {
        socket.send(message).await?;
    }
    Ok::<(), WsError>(())
});
```

### App runtime and hub configuration

```rust
let hub = WsHub::builder()
    .max_rooms_per_connection(32)
    .max_room_name_bytes(128)
    .broadcast_concurrency(64)
    .broker_operation_timeout(Duration::from_secs(2))
    .broker(Arc::new(my_broker))
    .build()?;

app.websocket_hub(hub.clone());
app.websocket_defaults(default_config);
app.websocket_observer(Arc::new(observer));

let runtime = app.websocket_runtime();
let hub = app.websocket_hub_handle();
```

`App::new()` installs a local hub automatically. `websocket_hub` replaces it
before serving; changing the hub after the app has begun serving is not
supported.

### Per-route configuration

```rust
let config = WebSocketConfig::new()
    .protocols(&["chat", "graphql-ws"])
    .require_protocol(true)
    .max_message_size(1024 * 1024)
    .max_frame_size(256 * 1024)
    .max_write_buffer_size(2 * 1024 * 1024)
    .inbound_capacity(64)
    .outbound_capacity(64)
    .backpressure_policy(BackpressurePolicy::Wait)
    .send_timeout(Duration::from_secs(5))
    .ping_interval(Duration::from_secs(30))
    .pong_timeout(Duration::from_secs(10))
    .idle_timeout(Duration::from_secs(120))
    .max_connection_lifetime(Duration::from_secs(24 * 60 * 60))
    .close_timeout(Duration::from_secs(5))
    .origin_policy(OriginPolicy::allow([
        "https://app.example.com",
    ]))
    .max_connections(2_000)
    .max_connections_per_ip(20)
    .message_rate_limit(100, Duration::from_secs(1))
    .max_rooms_per_connection(32)
    .max_room_name_bytes(128);

config.validate()?;
```

`OriginPolicy` provides `any`, `same_host`, and `allow` constructors plus a
builder controlling whether a missing `Origin` is accepted.

`listen`, `serve`, and their TLS variants validate every registered WebSocket
configuration before accepting connections and return `io::ErrorKind::InvalidInput`
on failure. Direct `Request::websocket_with` use validates during the request
and returns an HTTP 500 through the configured error handler.

### Socket metadata, messages, and closure

```rust
socket.id();
socket.protocol();
socket.remote_addr();
socket.route();

socket.recv().await?;
socket.recv_json::<T>().await?;
socket.recv_event::<T>().await?;

socket.send(message).await?;
socket.send_text("hola").await?;
socket.send_binary(bytes).await?;
socket.send_json(&value).await?;
socket.send_event("chat:message", &value).await?;

socket.ping(bytes).await?;
socket.pong(bytes).await?;
socket.close().await?;
socket.close_with(1000, "finalizado").await?;
let close_info = socket.closed().await;
```

### Split sender and receiver

```rust
let (mut receiver, sender) = socket.split();
let background_sender = sender.clone();

receiver.recv().await?;
receiver.recv_json::<T>().await?;
receiver.recv_event::<T>().await?;

sender.send(message).await?;
sender.try_send(message)?;
sender.send_text("hola").await?;
sender.send_binary(bytes).await?;
sender.send_json(&value).await?;
sender.send_event("chat:message", &value).await?;
sender.close().await?;
sender.close_with(1000, "finalizado").await?;
```

The receiver is unique and is not clonable. Senders are clonable and all use
the same bounded outbound queue. `WebSocketSender` also retains connection
metadata, room membership, room targeting, and closure-wait methods so
splitting does not remove control capabilities.

```rust
sender.id();
sender.route();
sender.join("general").await?;
sender.leave("general").await?;
sender.rooms().await?;
sender.to("general").send_event("chat:message", &value).await?;
sender.broadcast().send_event("presence:changed", &value).await?;

let close_info = receiver.closed().await;
let close_info = sender.closed().await;
```

### Rooms from a socket

```rust
socket.join("general").await?;
socket.join_many(["general", "equipo-7"]).await?;
socket.leave("general").await?;
socket.leave_many(["general", "equipo-7"]).await?;
socket.leave_all().await?;

let rooms = socket.rooms().await?;

let report = socket
    .to("general")
    .except(other_connection_id)
    .send_text("hola")
    .await?;

let report = socket
    .to_many(["general", "equipo-7"])
    .send_event("chat:message", &value)
    .await?;

let report = socket
    .broadcast()
    .send_event("presence:changed", &value)
    .await?;
```

All three socket selectors stay within `socket.route()` and automatically
exclude `socket.id()`.

### Administrative hub broadcasts

```rust
let chat = hub.route("/chat");

chat.to("general").send(message).await?;
chat.to("general").send_text("hola").await?;
chat.to("general").send_json(&value).await?;
chat.to("general")
    .send_event("chat:message", &value)
    .await?;

chat.to_many(["general", "equipo-7"])
    .except(connection_id)
    .send_event("chat:message", &value)
    .await?;

chat.all().send_event("server:notice", &value).await?;
hub.all().send_event("server:shutdown", &value).await?;

hub.local_socket(connection_id)
    .ok_or(WsBroadcastError::ConnectionNotFound)?
    .send_event("account:changed", &value)
    .await?;

hub.disconnect_local(connection_id, 1008, "no autorizado")
    .await?;

let local_room_size = chat.local_room_size("general").await;
let local_connections = hub.local_connection_count();
```

`WsRoute::all()` means every local or broker-connected socket in that route
scope. `WsHub::all()` explicitly targets every route scope. Direct socket
lookup and disconnect remain process-local.

### Broadcast result

```rust
pub struct WsBroadcastReport {
    // Local-process counts. Remote brokers cannot provide subscriber counts.
    pub matched: usize,
    pub enqueued: usize,
    pub rejected: usize,
    pub disconnected: usize,
    pub remote: WsRemotePublish,
}

pub enum WsRemotePublish {
    NotConfigured,
    Published,
}
```

Broadcast methods return `Result<WsBroadcastReport, WsBroadcastError>`. A
report with `rejected > 0` is a partial delivery result that callers can retry,
record, or surface. Broker failure returns an error carrying the completed
local report so local delivery is never hidden.

For local counts, `matched == enqueued + rejected + disconnected`.
`WsRemotePublish::Published` means the configured broker accepted the
publication; it does not claim remote client receipt. With no broker, the value
is `NotConfigured`. Broker failure returns `WsBroadcastError::Broker` with the
completed local report.

Room broadcasts accept text and binary application messages. Ping, Pong, and
Close are rejected by `WsTarget::send` because control frames must remain
connection-specific. Use runtime close/disconnect methods for administrative
closure.

### Existing local broadcast helper

The existing API remains unchanged for applications that only need a raw local
fan-out channel:

```rust
let broadcast = WsBroadcast::new(64);
let mut receiver = broadcast.subscribe();

broadcast.send(message);
broadcast.send_text("hola");
broadcast.receiver_count();
receiver.recv().await;
```

Unlike `WsHub`, `WsBroadcast` has no connection registry or rooms. Lagging
receivers continue to receive Tokio's explicit `Lagged` error.

### External broker contract

The object-safe broker API uses boxed futures and streams so applications can
install `Arc<dyn WsBroker>` without an async-trait dependency:

```rust
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

pub type WsBrokerStream = Pin<
    Box<dyn Stream<Item = Result<WsBrokerPublication, WsBrokerError>> + Send>,
>;

pub struct WsBrokerPublication {
    pub id: WsPublicationId,
    pub origin: WsNodeId,
    pub target: WsBrokerTarget,
    pub payload: WsBrokerPayload,
}

pub enum WsBrokerTarget {
    RouteRooms { route: String, rooms: Vec<String> },
    RouteAll { route: String },
    AllRoutes,
}

pub enum WsBrokerPayload {
    Text(String),
    Binary(Bytes),
}
```

The framework validates and bounds all decoded publication fields before
fan-out. A broker subscription is supervised with bounded exponential retry;
subscription loss and recovery are observable. Publications received with the
local `WsNodeId` are ignored to prevent local duplicate delivery.

### Close, error, statistics, and observer types

```rust
pub struct WebSocketCloseInfo {
    pub code: Option<u16>,
    pub reason: String,
    pub initiator: WebSocketCloseInitiator,
    pub clean: bool,
}

pub enum WebSocketCloseInitiator {
    Local,
    Peer,
    Runtime,
    Timeout,
    ProtocolError,
    Handler,
}

pub enum WebSocketError {
    Protocol(tungstenite::Error), // Existing variant, unchanged.
    Json(serde_json::Error),      // Existing variant, unchanged.
}

#[non_exhaustive]
pub enum WsError {
    WebSocket(WebSocketError),
    Timeout(WebSocketTimeout),
    Capacity(WebSocketCapacityError),
    InvalidConfiguration(String),
    InvalidClose { code: u16, reason: String },
    InvalidRoom(String),
    RoomLimit,
    Shutdown,
    HandlerPanic,
    Closed,
}

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
    pub broker_connected: bool,
}

pub trait WebSocketObserver: Send + Sync + 'static {
    fn observe(&self, event: &WebSocketObservation<'_>);
}
```

`WebSocketObservation` is a non-exhaustive metadata event enum covering
connection acceptance/rejection, open/close, message direction and byte
length, queue saturation, room join/leave, broadcast result, heartbeat,
handler failure, broker state, and forced shutdown. Payload bytes and JSON
values are never included. Observer panics are caught and do not affect the
driver.

### Runtime administration

```rust
let stats = runtime.stats();
let connections = runtime.connections();
let connection = runtime.connection(connection_id);

runtime.close(connection_id, 1001, "mantenimiento").await?;
runtime.shutdown().await?;
```

Runtime connection snapshots contain metadata and room names but never message
payloads.

## Testing Strategy

### Unit and integration tests

Unit tests cover configuration validation, handshake validation, origin
matching, subprotocol requirements, limit accounting, close-code validation,
error categorization, and observer panic isolation.

Real TCP tests cover:

- text, binary, fragmented, Ping, Pong, and Close frames
- independent heartbeat while the handler is not receiving
- heartbeat timeout against a client that suppresses Pong
- bounded inbound and outbound queues
- each backpressure policy
- oversized frames/messages and message-rate violations
- split sender concurrency
- handler success, error, and panic
- abrupt disconnects and malformed sequences
- connection limits and accounting release
- graceful and forced server shutdown
- API compatibility with existing handlers
- idempotent join/leave and automatic membership cleanup
- route-scoped rooms with no cross-route leakage
- multi-room deduplication and sender exclusion
- bounded-concurrency broadcast and partial delivery reports
- room limits and invalid room names
- local direct-send/disconnect behavior

Broker contract tests cover:

- publication encoding for route rooms, route-wide, and global targets
- origin-node echo suppression
- local success combined with remote publish failure
- subscription loss, retry, and recovery
- invalid or oversized remote publication rejection
- two in-memory nodes broadcasting through the broker without duplicate
  delivery

TLS integration tests repeat handshake, messaging, heartbeat, and shutdown over
`wss://`.

### Concurrency verification

Deterministic Tokio time tests verify timer interactions. Loom is used for any
custom concurrent state machine whose correctness cannot be covered by Tokio
channels and atomics alone. Miri runs against pure state/configuration tests
where supported. No unsafe code is introduced for this work.

### Fuzzing

Cargo-fuzz targets cover handshake header parsing, close frames, event JSON,
room names, broker publications, and generated frame/control sequences. Seed
corpora include malformed keys, invalid UTF-8, fragmented control frames,
oversized reasons, repeated close frames, duplicate rooms, and oversized
broker targets.

### Load and soak tests

A repository load-test tool opens WebSocket connections against a release
build and emits machine-readable results. The production acceptance profile is:

- 10,000 idle concurrent connections
- 1,000 active concurrent connections exchanging bounded messages
- a sustained soak period that demonstrates stable active connection count
  and no continuing memory growth after warm-up
- zero silent outbound drops
- zero server panics
- successful shutdown within configured deadlines

Latency, throughput, resident memory, CPU, queue saturation, and close/error
counts are published as measured results. The design does not prescribe a
universal latency threshold because hardware and CI runners vary; regressions
are detected against a documented reference environment and baseline.

## Documentation and Operations

Documentation includes:

- secure browser origin configuration
- authentication before upgrade
- reverse-proxy upgrade headers and idle timeouts
- TLS and `wss://`
- load balancer draining behavior
- OS file descriptor and TCP tuning for 10,000 connections
- queue and memory sizing
- graceful shutdown expectations
- slow-client and backpressure behavior
- single-node `WsBroadcast` limitations
- route-scoped rooms, sender exclusion, multi-room deduplication, and partial
  broadcast reports
- implementing a Redis/NATS `WsBroker` externally
- the distinction between cross-node room broadcast and distributed presence
- close codes and error handling
- compatibility migration examples

The example application demonstrates authentication middleware, secure origin
policy, bounded queues, split send/receive tasks, graceful shutdown, and
metrics observation without logging payloads.

## Delivery Phases

1. Introduce configuration validation, strict handshake behavior, close types,
   and compatibility tests.
2. Add the channel-backed connection driver and split API while keeping direct
   methods intact.
3. Add heartbeat/Pong timeout, idle/lifetime deadlines, and protocol-correct
   closure.
4. Integrate the runtime registry with plain/TLS graceful shutdown and panic
   isolation.
5. Add capacity, rate, origin, and backpressure controls.
6. Add tracing, observer metrics, lifecycle hooks, and runtime statistics.
7. Add `WsHub`, route-scoped rooms, broadcast selectors, reports, and local
   administration.
8. Add `WsBroker`, update `WsBroadcast`, multinode contract tests, examples,
   and operations docs.
9. Add fuzzing, load/soak tooling, reference benchmarks, and final compatibility
   validation.

Each phase must leave `cargo fmt --check`, `cargo clippy --all-targets`, and the
full test suite passing. Feature-specific tests are written before their
implementation, and each phase is independently reviewable and releasable.

## Acceptance Criteria

The work is complete when:

- all existing WebSocket examples and public calls compile unchanged
- every queue and buffer controlled by RustRest is bounded
- no send path silently drops application messages
- heartbeat operates without application polling and detects missing Pong
- closure follows RFC 6455 and exposes final close information
- handler panics affect only their own connection
- plain and TLS server shutdown closes and drains registered WebSockets within
  configured deadlines
- origin, protocol, connection, message, and size policies are enforced with
  documented HTTP or WebSocket outcomes
- metrics and tracing expose operational state without payload leakage
- route-scoped rooms support idempotent membership, sender exclusion,
  multi-room deduplication, automatic cleanup, and explicit partial delivery
  reports
- room broadcasts work across two in-memory broker nodes without duplicate
  local delivery
- an external broker can be implemented without modifying framework internals
- unit, real-network, TLS, concurrency, and fuzz tests cover the defined
  failure modes
- the reference load profile reaches 10,000 idle and 1,000 active connections
  with stable post-warm-up memory and no panics or silent drops
