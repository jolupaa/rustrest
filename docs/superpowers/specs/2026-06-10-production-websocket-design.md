# Production WebSocket Design

## Status

Approved design for upgrading RustRest's native WebSocket support while
preserving the public API introduced in v0.2.

## Objective

Make RustRest WebSockets suitable for production workloads on a single
application instance, with explicit extension points for external brokers in
multi-node deployments. The target capacity per instance is 10,000 idle
connections and 1,000 concurrently active connections under a reproducible
load test.

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

Handlers may additionally return `Result<(), WebSocketError>`. This is
implemented through output normalization in `IntoWebSocketHandler`, using the
same marker-based approach as HTTP handlers where needed to preserve type
inference.

## Non-goals

The framework will not implement:

- Socket.IO or its transport protocol
- client-side reconnection
- durable message storage or replay
- delivery acknowledgements beyond WebSocket/TCP semantics
- bundled Redis, NATS, or Kafka clients
- transparent cross-node presence
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
`Result<(), WebSocketError>` records the error and maps it to an appropriate
close reason. Returning `()` retains existing behavior.

### Split API

`WebSocket::split()` returns:

- a single `WebSocketReceiver` for inbound messages
- a clonable `WebSocketSender` for outbound messages

There remains only one logical receiver. `WebSocketSender::send` waits for
bounded queue capacity, while `try_send` immediately reports `Full` or
`Closed`. This makes fan-out and independent producer tasks possible without
exposing the raw Tungstenite stream.

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
- `max_messages_per_interval`

Server-wide defaults live in a WebSocket runtime configuration on `App`.
Route-level `WebSocketConfig` values override those defaults. Limits are
checked in this order: process, route, then IP. Counters are reserved before
returning HTTP 101 and released on every failure path.

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

`WebSocketError` gains structured variants for:

- protocol/transport failures
- JSON failures
- timeout kind
- capacity/backpressure failure
- invalid configuration
- invalid close frame
- shutdown
- handler panic
- channel closed

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

Observer callbacks must be non-blocking and panic-isolated. Prometheus, OpenTelemetry,
or other integrations live in external crates or application code.

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

## Public API Additions

The additive surface includes:

- `WebSocket::id()`
- `WebSocket::remote_addr()`
- `WebSocket::split()`
- `WebSocket::close_with(code, reason)`
- `WebSocketSender::{send, try_send, close, close_with}`
- `WebSocketReceiver::recv()`
- `WebSocketCloseInfo`
- `WebSocketRuntimeHandle::stats()`
- `WebSocketStats`
- `BackpressurePolicy`
- `OriginPolicy`
- `WebSocketObserver`
- `WsBroker`

Existing direct methods delegate to the same channel-backed implementation as
the split handles.

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

TLS integration tests repeat handshake, messaging, heartbeat, and shutdown over
`wss://`.

### Concurrency verification

Deterministic Tokio time tests verify timer interactions. Loom is used for any
custom concurrent state machine whose correctness cannot be covered by Tokio
channels and atomics alone. Miri runs against pure state/configuration tests
where supported. No unsafe code is introduced for this work.

### Fuzzing

Cargo-fuzz targets cover handshake header parsing, close frames, event JSON,
and generated frame/control sequences. Seed corpora include malformed keys,
invalid UTF-8, fragmented control frames, oversized reasons, and repeated
close frames.

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
- implementing a Redis/NATS `WsBroker` externally
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
7. Add `WsBroker`, update `WsBroadcast`, examples, and operations docs.
8. Add fuzzing, load/soak tooling, reference benchmarks, and final compatibility
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
- an external broker can be implemented without modifying framework internals
- unit, real-network, TLS, concurrency, and fuzz tests cover the defined
  failure modes
- the reference load profile reaches 10,000 idle and 1,000 active connections
  with stable post-warm-up memory and no panics or silent drops
