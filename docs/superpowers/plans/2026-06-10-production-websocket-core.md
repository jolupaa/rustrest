# Production WebSocket Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the direct-stream WebSocket implementation with a backward-compatible, bounded, supervised runtime that enforces handshake security, lifecycle deadlines, backpressure, limits, observability, and graceful shutdown.

**Architecture:** `App` owns a shared `WebSocketRuntime`; each upgraded connection is admitted before HTTP 101, then a single driver task owns the Tungstenite stream. User-facing socket handles communicate with the driver through bounded Tokio channels, while a watch channel publishes final close information and the runtime registry supervises shutdown and statistics.

**Tech Stack:** Rust 2024, Hyper 1.x upgrades, hyper-util, tokio-tungstenite 0.29, Tokio `mpsc`/`watch`/`Notify`, futures-util panic isolation, serde, optional tracing.

---

## File Map

- Create `src/app/websocket/config.rs`: public configuration, origin policy,
  validation, Tungstenite conversion.
- Create `src/app/websocket/error.rs`: additive `WsError` and classifications;
  preserve `WebSocketError` variants.
- Create `src/app/websocket/types.rs`: IDs, close information, snapshots,
  statistics, observer events.
- Create `src/app/websocket/socket.rs`: `WebSocket`, split sender/receiver,
  JSON/event helpers.
- Create `src/app/websocket/driver.rs`: transport state machine.
- Create `src/app/websocket/runtime.rs`: admission, registry, counters,
  cancellation, drain.
- Modify `src/app/websocket.rs`: facade, handshake, spawn wiring,
  compatibility `WsBroadcast`.
- Modify `src/app/router.rs`: preserve the normalized mounted route pattern in
  dispatch and validate WebSocket route configs.
- Modify `src/app/request.rs`: HTTP version, matched route pattern, runtime
  handle.
- Modify `src/app/server.rs`: runtime ownership, configuration APIs, admission
  context, plain shutdown drain.
- Modify `src/app/tls.rs`: same runtime drain for TLS.
- Modify `src/app.rs` and `src/lib.rs`: additive public re-exports.
- Create `tests/websocket_api_compat.rs`: source-compatibility fixture.
- Create `tests/websocket_integration.rs`: real TCP lifecycle tests.
- Modify `src/app/tests.rs`: focused pure/unit tests.

### Task 1: Lock Current API Compatibility and Split the Module

**Files:**
- Create: `tests/websocket_api_compat.rs`
- Create: `src/app/websocket/config.rs`
- Create: `src/app/websocket/error.rs`
- Create: `src/app/websocket/types.rs`
- Create: `src/app/websocket/socket.rs`
- Create: `src/app/websocket/tests.rs`
- Modify: `src/app/websocket.rs`
- Modify: `src/app/tests.rs`
- Modify: `src/app.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write the compatibility test before moving code**

Create `tests/websocket_api_compat.rs` with calls that must continue compiling:

```rust
use std::time::Duration;

use rustrest::{
    App, Request, Router, WebSocket, WebSocketConfig, WebSocketError,
    WebSocketMessage, WsBroadcast,
};

fn exhaustive_existing_error(error: WebSocketError) -> &'static str {
    match error {
        WebSocketError::Protocol(_) => "protocol",
        WebSocketError::Json(_) => "json",
    }
}

fn accepts_websocket(_: WebSocket) {}

#[test]
fn existing_websocket_surface_still_compiles() {
    let mut app = App::new();
    app.websocket("/ws", |mut socket| async move {
        let _ = socket.protocol();
        let _ = socket.send_text("hola").await;
        let _ = socket.send_binary([1_u8, 2, 3].as_slice()).await;
        let _ = socket.send_json(&serde_json::json!({ "ok": true })).await;
        let _ = socket.send_event("ready", &true).await;
        let _ = socket.ping(Vec::<u8>::new()).await;
        let _ = socket.pong(Vec::<u8>::new()).await;
        let _ = socket.close().await;
    });

    app.ws("/short", |_socket| async move {});
    app.websocket_with(
        "/configured",
        WebSocketConfig::new()
            .protocols(&["chat"])
            .max_message_size(1024)
            .ping_interval(Duration::from_secs(30)),
        |_socket| async move {},
    );

    let mut router = Router::new();
    router.websocket("/nested", |_socket| async move {});
    router.ws("/nested-short", |_socket| async move {});
    router.websocket_with(
        "/nested-configured",
        WebSocketConfig::new(),
        |_socket| async move {},
    );

    let request = Request::builder().path("/ws").build();
    let _response = request.websocket(|_socket| async move {});

    let broadcast = WsBroadcast::new(8);
    let mut receiver = broadcast.subscribe();
    assert_eq!(broadcast.send_text("hola"), 1);
    let _ = broadcast.receiver_count();
    drop(receiver.try_recv());

    let _message = WebSocketMessage::text("hola");
    let _error_match: fn(WebSocketError) -> &'static str = exhaustive_existing_error;
    let _socket_consumer: fn(WebSocket) = accepts_websocket;
}
```

- [ ] **Step 2: Run the compatibility test before refactoring**

Run:

```bash
cargo test --test websocket_api_compat
```

Expected: PASS. This establishes the pre-change source surface.

- [ ] **Step 3: Turn `websocket.rs` into the facade without changing behavior**

Move existing definitions into focused files and leave these declarations and
re-exports in `src/app/websocket.rs`:

```rust
mod config;
mod error;
mod socket;
mod types;
#[cfg(test)]
mod tests;

pub use config::WebSocketConfig;
pub use error::WebSocketError;
pub use socket::{
    IntoWebSocketHandler, WebSocket, WebSocketEvent, WebSocketHandler,
    WebSocketMessage,
};
```

Keep `Request::websocket`, `Request::websocket_with`, `spawn_websocket`, and
`WsBroadcast` in the facade for now. Move code mechanically; do not introduce
new behavior in this step. Move the existing WebSocket handshake,
subprotocol, and `WsBroadcast` unit tests from `src/app/tests.rs` into
`src/app/websocket/tests.rs` so later private subsystem tests remain close to
their implementation. Rebuild their requests with `Request::builder()` rather
than depending on the parent test module's private `dummy_request` helper.

- [ ] **Step 4: Keep crate-root re-exports unchanged**

Verify `src/app.rs` and `src/lib.rs` still export exactly the existing names.
Do not export empty future types yet.

- [ ] **Step 5: Format and verify the mechanical split**

Run:

```bash
cargo fmt
cargo test --test websocket_api_compat
cargo test websocket
cargo check --all-targets
```

Expected: all commands PASS with no behavior change.

- [ ] **Step 6: Commit the compatibility lock and module split**

```bash
git add src/app/websocket.rs src/app/websocket src/app/tests.rs tests/websocket_api_compat.rs src/app.rs src/lib.rs
git commit -m "refactor: split websocket module behind compatible facade"
```

### Task 2: Add Validated Configuration, Origin Policy, and Additive Errors

**Files:**
- Modify: `src/app/websocket/config.rs`
- Modify: `src/app/websocket/error.rs`
- Modify: `src/app/websocket/types.rs`
- Modify: `src/app/websocket.rs`
- Modify: `src/app.rs`
- Modify: `src/lib.rs`
- Modify: `src/app/tests.rs`

- [ ] **Step 1: Write failing configuration and compatibility tests**

Add tests in `src/app/tests.rs`:

```rust
#[test]
fn websocket_config_rejects_unbounded_or_inconsistent_values() {
    assert!(WebSocketConfig::new().outbound_capacity(0).validate().is_err());
    assert!(WebSocketConfig::new().inbound_capacity(0).validate().is_err());
    assert!(
        WebSocketConfig::new()
            .write_buffer_size(1024)
            .max_write_buffer_size(1024)
            .validate()
            .is_err()
    );
    assert!(
        WebSocketConfig::new()
            .ping_interval(Duration::from_secs(30))
            .pong_timeout(Duration::from_secs(30))
            .validate()
            .is_err()
    );
}

#[test]
fn websocket_origin_policy_normalizes_default_ports() {
    let policy = OriginPolicy::allow(["https://app.example.com"]);
    assert!(policy.allows(Some("https://app.example.com:443"), "app.example.com"));
    assert!(!policy.allows(Some("https://evil.example"), "app.example.com"));

    let same_host = OriginPolicy::same_host().allow_missing(false);
    assert!(same_host.allows(Some("http://localhost:3000"), "localhost:3000"));
    assert!(!same_host.allows(None, "localhost:3000"));
}

#[test]
fn existing_websocket_error_remains_exhaustive() {
    fn classify(error: WebSocketError) -> &'static str {
        match error {
            WebSocketError::Protocol(_) => "protocol",
            WebSocketError::Json(_) => "json",
        }
    }
    let _ = classify as fn(WebSocketError) -> &'static str;
}
```

- [ ] **Step 2: Run the focused tests and confirm failure**

Run:

```bash
cargo test websocket_config_rejects_unbounded_or_inconsistent_values
```

Expected: FAIL because the new builders, validation, and `OriginPolicy` do not
exist.

- [ ] **Step 3: Define bounded defaults and validation**

Implement these public shapes in `src/app/websocket/config.rs`:

```rust
use std::time::Duration;

const DEFAULT_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_FRAME_SIZE: usize = 4 * 1024 * 1024;
const DEFAULT_WRITE_BUFFER_SIZE: usize = 128 * 1024;
const DEFAULT_MAX_WRITE_BUFFER_SIZE: usize = 1024 * 1024;
const DEFAULT_CHANNEL_CAPACITY: usize = 64;
const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_PONG_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_MAX_ROOMS: usize = 32;
const DEFAULT_MAX_ROOM_NAME_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum BackpressurePolicy {
    #[default]
    Wait,
    Reject,
    Disconnect,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum OriginPolicy {
    Any { allow_missing: bool },
    SameHost { allow_missing: bool },
    AllowList {
        origins: Vec<String>,
        allow_missing: bool,
    },
}

#[derive(Clone, Debug)]
pub struct MessageRateLimit {
    pub max_messages: u32,
    pub interval: Duration,
}

#[derive(Clone, Debug, Default)]
pub struct WebSocketConfig {
    pub(crate) protocols: Option<Vec<String>>,
    pub(crate) require_protocol: Option<bool>,
    pub(crate) max_message_size: Option<usize>,
    pub(crate) max_frame_size: Option<usize>,
    pub(crate) write_buffer_size: Option<usize>,
    pub(crate) max_write_buffer_size: Option<usize>,
    pub(crate) inbound_capacity: Option<usize>,
    pub(crate) outbound_capacity: Option<usize>,
    pub(crate) backpressure_policy: Option<BackpressurePolicy>,
    pub(crate) send_timeout: Option<Duration>,
    pub(crate) ping_interval: Option<Option<Duration>>,
    pub(crate) pong_timeout: Option<Duration>,
    pub(crate) idle_timeout: Option<Option<Duration>>,
    pub(crate) max_connection_lifetime: Option<Option<Duration>>,
    pub(crate) close_timeout: Option<Duration>,
    pub(crate) origin_policy: Option<OriginPolicy>,
    pub(crate) max_connections: Option<usize>,
    pub(crate) max_connections_per_ip: Option<Option<usize>>,
    pub(crate) message_rate_limit: Option<Option<MessageRateLimit>>,
    pub(crate) max_rooms_per_connection: Option<usize>,
    pub(crate) max_room_name_bytes: Option<usize>,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedWebSocketConfig {
    pub protocols: Vec<String>,
    pub require_protocol: bool,
    pub max_message_size: usize,
    pub max_frame_size: usize,
    pub write_buffer_size: usize,
    pub max_write_buffer_size: usize,
    pub inbound_capacity: usize,
    pub outbound_capacity: usize,
    pub backpressure_policy: BackpressurePolicy,
    pub send_timeout: Duration,
    pub ping_interval: Option<Duration>,
    pub pong_timeout: Duration,
    pub idle_timeout: Option<Duration>,
    pub max_connection_lifetime: Option<Duration>,
    pub close_timeout: Duration,
    pub origin_policy: OriginPolicy,
    pub process_max_connections: Option<usize>,
    pub route_max_connections: Option<usize>,
    pub max_connections_per_ip: Option<usize>,
    pub message_rate_limit: Option<MessageRateLimit>,
    pub max_rooms_per_connection: usize,
    pub max_room_name_bytes: usize,
}
```

`WebSocketConfig::default()` means “no overrides.” Preserve the existing
`new`, `protocols`, `max_message_size`, and `ping_interval` builders by setting
the corresponding `Option`; add builders for every other field shown in the
approved design.

The inheritable optional settings use `Option<Option<T>>`: outer `None` means
inherit, `Some(Some(value))` means override, and `Some(None)` means explicitly
disable an App default. Add `disable_ping`, `disable_idle_timeout`,
`disable_max_connection_lifetime`, `disable_max_connections_per_ip`, and
`disable_message_rate_limit` builders. Existing setters place values in
`Some(Some(value))` and remain source-compatible.

Implement `ResolvedWebSocketConfig::from_layers(app, route)` so route options
win over App options, then built-in constants apply. `process_max_connections`
comes only from `app.max_connections`; `route_max_connections` comes only from
`route.max_connections`; route per-IP limit wins over the App per-IP limit.
Protocols inherit from App only when the route did not call `protocols`.
Built-in origin is `OriginPolicy::Any { allow_missing: true }`; built-in
heartbeat/idle/lifetime/rate/connection limits are disabled.

Implement `validate()` with these exact rules:

```rust
pub fn validate(&self) -> Result<(), WsError> {
    let resolved = ResolvedWebSocketConfig::from_layers(
        &WebSocketConfig::default(),
        self,
    );
    resolved.validate()
}

impl ResolvedWebSocketConfig {
pub(crate) fn validate(&self) -> Result<(), WsError> {
    if self.max_message_size == 0 || self.max_frame_size == 0 {
        return Err(WsError::InvalidConfiguration(
            "los limites de mensaje y frame WebSocket deben ser mayores que cero".into(),
        ));
    }
    if self.inbound_capacity == 0 || self.outbound_capacity == 0 {
        return Err(WsError::InvalidConfiguration(
            "las capacidades de los canales WebSocket deben ser mayores que cero".into(),
        ));
    }
    if self.max_write_buffer_size <= self.write_buffer_size {
        return Err(WsError::InvalidConfiguration(
            "max_write_buffer_size debe ser mayor que write_buffer_size".into(),
        ));
    }
    if self.ping_interval.is_some_and(|interval| interval <= self.pong_timeout) {
        return Err(WsError::InvalidConfiguration(
            "ping_interval debe ser mayor que pong_timeout".into(),
        ));
    }
    if self.send_timeout.is_zero() || self.close_timeout.is_zero() {
        return Err(WsError::InvalidConfiguration(
            "los tiempos limite de envio y cierre WebSocket deben ser mayores que cero".into(),
        ));
    }
    if self.max_rooms_per_connection == 0 || self.max_room_name_bytes == 0 {
        return Err(WsError::InvalidConfiguration(
            "los limites de rooms WebSocket deben ser mayores que cero".into(),
        ));
    }
    if self.process_max_connections == Some(0)
        || self.route_max_connections == Some(0)
        || self.max_connections_per_ip == Some(0)
    {
        return Err(WsError::InvalidConfiguration(
            "los limites de conexiones WebSocket deben ser mayores que cero".into(),
        ));
    }
    if let Some(limit) = &self.message_rate_limit
        && (limit.max_messages == 0 || limit.interval.is_zero())
    {
        return Err(WsError::InvalidConfiguration(
            "los limites de frecuencia de mensajes WebSocket deben ser mayores que cero".into(),
        ));
    }
    Ok(())
}
}
```

Add `ResolvedWebSocketConfig::tungstenite_config()` and set
`write_buffer_size`, `max_write_buffer_size`,
`max_message_size(Some(self.max_message_size))`, and
`max_frame_size(Some(self.max_frame_size))`. Never pass Tungstenite's unbounded
`max_write_buffer_size` default into a production connection.

- [ ] **Step 4: Implement origin normalization without substring matching**

Use `hyper::Uri` and compare normalized `(scheme, host, effective_port)`
tuples. `SameHost` derives the expected host/port from the request `Host`
header and treats `ws/http` as port 80 and `wss/https` as port 443. An invalid
origin is denied. `allow_missing` controls `None` only. Normalize and validate
allowlist entries during `WebSocketConfig::validate`; reject entries without
an HTTP/HTTPS/WS/WSS scheme and authority.

- [ ] **Step 5: Preserve `WebSocketError` and add precise new errors**

Keep the existing enum unchanged in `src/app/websocket/error.rs`, then add:

```rust
#[derive(Debug)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WebSocketTimeout {
    Send,
    Pong,
    Idle,
    Lifetime,
    Close,
    Shutdown,
    Broker,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WebSocketCapacityError {
    InboundQueue,
    OutboundQueue,
    GlobalConnections,
    RouteConnections,
    IpConnections,
    MessageRate,
}
```

Implement `Display`, `Error::source`, and `From<WebSocketError> for WsError`.
Add `WebSocketError::category()` returning a stable
`WebSocketErrorCategory::{Protocol, Json}` without altering enum variants.

- [ ] **Step 6: Export only additive names and run tests**

Re-export `BackpressurePolicy`, `OriginPolicy`, `WsError`, timeout/capacity
types, and `WebSocketErrorCategory` from `src/app.rs` and `src/lib.rs`.

Run:

```bash
cargo fmt
cargo test websocket_config_rejects_unbounded_or_inconsistent_values
cargo test websocket_origin_policy_normalizes_default_ports
cargo test --test websocket_api_compat
cargo clippy --all-targets -- -D warnings
```

Expected: PASS.

- [ ] **Step 7: Commit configuration and error foundations**

```bash
git add src/app/websocket src/app/websocket.rs src/app/tests.rs src/app.rs src/lib.rs
git commit -m "feat: add validated websocket configuration"
```

### Task 3: Preserve Matched Route Context and Enforce a Strict Handshake

**Files:**
- Modify: `src/app/router.rs`
- Modify: `src/app/request.rs`
- Modify: `src/app/server.rs`
- Modify: `src/app/response.rs`
- Modify: `src/app/websocket.rs`
- Modify: `src/app/websocket/config.rs`
- Modify: `src/app/tests.rs`
- Create: `tests/websocket_integration.rs`

- [ ] **Step 1: Write failing route-pattern and handshake tests**

Add a router unit test asserting a mounted WebSocket request records
`/api/chat/:channel`, not `/api/chat/42`. Add real TCP tests covering:

```rust
#[tokio::test]
async fn websocket_rejects_invalid_version_with_426() {
    let (addr, server) = spawn_app(|app| {
        app.websocket("/ws", |_socket| async move {});
    }).await;

    let response = raw_handshake(addr, &[
        ("Sec-WebSocket-Version", "12"),
        ("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ=="),
    ]).await;

    assert!(response.starts_with("HTTP/1.1 426"), "{response}");
    assert!(response.contains("sec-websocket-version: 13"), "{response}");
    server.abort();
}

#[tokio::test]
async fn websocket_rejects_key_that_is_not_sixteen_decoded_bytes() {
    let (addr, server) = spawn_app(|app| {
        app.websocket("/ws", |_socket| async move {});
    }).await;

    let response = raw_handshake(addr, &[
        ("Sec-WebSocket-Version", "13"),
        ("Sec-WebSocket-Key", "YQ=="),
    ]).await;

    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    server.abort();
}
```

The test helper must always send HTTP/1.1, `Host`, `Upgrade: websocket`, and
`Connection: Upgrade`, then read until EOF.

- [ ] **Step 2: Run the new tests and confirm failure**

Run:

```bash
cargo test --test websocket_integration websocket_rejects
```

Expected: at least the version and invalid-key assertions FAIL.

- [ ] **Step 3: Replace the route tuple with a named internal match**

In `src/app/router.rs`, define:

```rust
pub(crate) struct MatchedRoute {
    pub handler: Handler,
    pub middlewares: Vec<Middleware>,
    pub params: HashMap<String, String>,
    pub pattern: String,
}
```

Change `Router::route` to return `Option<MatchedRoute>` and set `pattern` with
`render_pattern(&route.pattern)`. Change `resolve_miss` to return a
`MatchedRoute` whose `pattern` is the concrete miss path, then set the new
private `Request::route_pattern: Option<String>` during dispatch. Add:

```rust
pub(crate) fn route_pattern(&self) -> Option<&str> {
    self.route_pattern.as_deref()
}
```

Initialize the field to `None` in the real request builder, `RequestBuilder`,
and every test request literal.

- [ ] **Step 4: Preserve the HTTP version on `Request`**

Add private `version: hyper::Version`, initialize it from the Hyper request,
default builders to `HTTP_11`, and add a public read-only accessor:

```rust
pub fn version(&self) -> hyper::Version {
    self.version
}
```

Also add private `secure_transport: bool`, public `is_secure()`, and
`RequestBuilder::secure(bool)`. Change the crate-private server handler to
receive a `TransportSecurity::{Plain, Tls}` argument: plain serve paths pass
`Plain`, TLS serve paths pass `Tls`. Origin normalization uses this bit when a
`Host` header omits its port: plain defaults to 80, TLS defaults to 443.

- [ ] **Step 5: Implement one strict handshake validator**

In `src/app/websocket.rs`, define an internal rejection that owns its headers:

```rust
struct HandshakeRejection {
    status: u16,
    message: &'static str,
    headers: Vec<(&'static str, &'static str)>,
}

impl HandshakeRejection {
    fn into_response(self) -> Response {
        self.headers.into_iter().fold(
            Response::send(self.message).status(self.status),
            |response, (name, value)| response.header(name, value),
        )
    }
}
```

`validate_handshake(req, config)` must check, in order: HTTP/1.1, GET,
Upgrade token, Connection token, version 13, base64-decoded key length 16,
origin policy, and required subprotocol overlap. Version mismatch returns 426
plus `Sec-WebSocket-Version: 13`; malformed requests return 400; origin denial
returns 403.

Use this validator from `Request::websocket_with`. Keep
`Response::websocket(&Request) -> Result<Response, HttpError>` source-compatible
by mapping a rejection to `HttpError::new(status, message)` after validation.

- [ ] **Step 6: Ensure Hyper only takes `OnUpgrade` for valid classic upgrades**

Change `is_websocket_upgrade_request` to require `req.version() ==
hyper::Version::HTTP_11` and a valid version/key shape. It may remain a cheaper
transport predicate; the full route config validation still occurs in
`Request::websocket_with`.

- [ ] **Step 7: Verify strict handshake and route context**

Run:

```bash
cargo fmt
cargo test websocket_handshake
cargo test --test websocket_integration websocket_rejects
cargo test --test websocket_api_compat
```

Expected: PASS.

- [ ] **Step 8: Commit strict handshake support**

```bash
git add src/app/router.rs src/app/request.rs src/app/server.rs src/app/response.rs src/app/websocket.rs src/app/tests.rs tests/websocket_integration.rs
git commit -m "feat: validate websocket upgrades strictly"
```

### Task 4: Add Runtime Admission, Registry, Snapshots, and App Ownership

**Files:**
- Create: `src/app/websocket/runtime.rs`
- Modify: `src/app/websocket.rs`
- Modify: `src/app/websocket/types.rs`
- Modify: `src/app/request.rs`
- Modify: `src/app/router.rs`
- Modify: `src/app/server.rs`
- Modify: `src/app.rs`
- Modify: `src/lib.rs`
- Modify: `src/app/tests.rs`

- [ ] **Step 1: Write failing runtime accounting tests**

Add tests that admit two connections, assert active/accepted counts, drop one
permit, assert active decrements, and reject a third permit when global, route,
or IP limits are reached. Use deterministic addresses and route patterns.

The central assertion must be:

```rust
assert_eq!(runtime.stats().active_connections, 2);
drop(first);
assert_eq!(runtime.stats().active_connections, 1);
assert_eq!(runtime.stats().accepted_connections, 2);
```

- [ ] **Step 2: Run the runtime test and confirm failure**

Run:

```bash
cargo test websocket_runtime_accounts_for_permits
```

Expected: FAIL because the runtime does not exist.

- [ ] **Step 3: Implement process-local IDs and registry data**

In `types.rs`, define `WebSocketId(u64)`, `WebSocketConnectionSnapshot`, and
`WebSocketStats`. Derive `Clone`, `Copy`, `Debug`, `Eq`, `Hash`, and display the
ID as its decimal value. Mark public snapshots/stats `#[non_exhaustive]`.

In `runtime.rs`, use:

```rust
struct RuntimeInner {
    next_id: AtomicU64,
    registry: Mutex<Registry>,
    shutdown_tx: watch::Sender<bool>,
    empty: Notify,
    observer: Arc<dyn WebSocketObserver>,
}

#[derive(Default)]
struct Registry {
    accepting: bool,
    connections: HashMap<WebSocketId, ConnectionEntry>,
    route_counts: HashMap<String, usize>,
    ip_counts: HashMap<IpAddr, usize>,
    counters: WebSocketCounters,
}

pub(crate) struct ConnectionPermit {
    id: WebSocketId,
    runtime: WebSocketRuntimeHandle,
    released: bool,
}
```

`ConnectionPermit::drop` must call one idempotent `release(id)` path that
updates every counter and notifies `empty` when the registry reaches zero.
Never await while holding the registry mutex.

Do not rely on `Registry::default()` for startup state: the runtime constructor
must explicitly set `accepting: true`. Shutdown is the only transition to
`false`.

- [ ] **Step 4: Add atomic admission before HTTP 101**

Implement:

```rust
pub(crate) fn admit(
    &self,
    route: &str,
    remote_addr: Option<SocketAddr>,
    protocol: Option<&str>,
    config: &ResolvedWebSocketConfig,
) -> Result<ConnectionPermit, AdmissionError>;
```

Inside one mutex lock: reject shutdown, process limit, route limit, then IP
limit; allocate the ID; insert metadata; increment accepted/active and the
route/IP maps. Map rejections before upgrade to 503 for shutdown/global/route
capacity and 429 plus `Retry-After: 1` for per-IP capacity.

- [ ] **Step 5: Give `App` and every routed request the same runtime**

Add `websocket_runtime: WebSocketRuntimeHandle` and
`websocket_defaults: WebSocketConfig` to `App`. `App::new()` creates a local
runtime and an empty override layer. Add:

```rust
pub fn websocket_runtime(&self) -> WebSocketRuntimeHandle {
    self.websocket_runtime.clone()
}

pub fn websocket_defaults(&mut self, config: WebSocketConfig) -> &mut Self {
    self.websocket_defaults = config;
    self
}

pub fn websocket_observer(
    &mut self,
    observer: Arc<dyn WebSocketObserver>,
) -> &mut Self {
    self.websocket_runtime.set_observer(observer);
    self
}
```

Add a private runtime handle and
`resolved_websocket_config: Option<ResolvedWebSocketConfig>` to `Request`;
initialize builders with a detached local runtime and no resolved route config,
then overwrite the runtime from `App::dispatch`, exactly as state is overwritten
today.

- [ ] **Step 6: Store WebSocket route configuration in router metadata**

Add internal `RouteKind::{Http, WebSocket(WebSocketConfig)}` to `Route`.
`Router::websocket_with` registers `RouteKind::WebSocket(config.clone())` and
the closure still calls `req.websocket_with(config.clone(), handler)`.
Preserve the kind across mount and include it in `MatchedRoute`. During
`App::dispatch`, resolve the route layer against `self.websocket_defaults` and
store the immutable result on the request. Add
`Router::validate_websockets(&app_defaults)` and call it before binding/serving;
convert validation failure to `io::ErrorKind::InvalidInput`.

- [ ] **Step 7: Admit before building the 101 response**

In `Request::into_websocket_response`, after handshake and protocol
negotiation but before `Response::websocket`, call runtime admission with the
matched route pattern, remote address, protocol, and resolved request config.
For direct `Request::websocket_with` calls outside routing, resolve its supplied
config against built-in defaults. Pass the
permit into `spawn_websocket`; if response construction or upgrade extraction
fails, dropping the permit must release all accounting.

- [ ] **Step 8: Verify admission and compatibility**

Run:

```bash
cargo fmt
cargo test websocket_runtime
cargo test websocket_config
cargo test --test websocket_api_compat
cargo check --all-targets
```

Expected: PASS.

- [ ] **Step 9: Commit the runtime registry**

```bash
git add src/app/websocket src/app/websocket.rs src/app/request.rs src/app/router.rs src/app/server.rs src/app/tests.rs src/app.rs src/lib.rs
git commit -m "feat: add websocket runtime admission registry"
```

### Task 5: Introduce the Channel-Backed Driver Without Breaking Direct Methods

**Files:**
- Create: `src/app/websocket/driver.rs`
- Modify: `src/app/websocket/socket.rs`
- Modify: `src/app/websocket/runtime.rs`
- Modify: `src/app/websocket.rs`
- Modify: `tests/websocket_integration.rs`

- [ ] **Step 1: Write failing compatibility and independent-progress tests**

Move the existing real WebSocket tests from `tests/http_integration.rs` into
`tests/websocket_integration.rs` unchanged, then add a test where the handler
sleeps without calling `recv()` while the client still receives a server-sent
message from a cloned sender.

The handler body is:

```rust
app.websocket("/ws", |socket| async move {
    let (_receiver, sender) = socket.split();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(25)).await;
        sender.send_text("background").await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
});
```

- [ ] **Step 2: Run the new test and confirm failure**

Run:

```bash
cargo test --test websocket_integration websocket_sender_progresses_independently
```

Expected: FAIL because `split` and the driver do not exist.

- [ ] **Step 3: Define bounded driver channels**

In `driver.rs` define:

```rust
pub(crate) enum OutboundCommand {
    Message(WebSocketMessage),
}

pub(crate) enum ControlCommand {
    Ping(Bytes),
    Pong(Bytes),
    Close(Option<CloseFrame>),
    Disconnect(Option<CloseFrame>),
}

pub(crate) struct DriverChannels {
    pub inbound_tx: mpsc::Sender<Result<WebSocketMessage, WebSocketError>>,
    pub outbound_rx: mpsc::Receiver<OutboundCommand>,
    pub control_rx: mpsc::Receiver<ControlCommand>,
    pub close_tx: watch::Sender<Option<WebSocketCloseInfo>>,
}
```

Create application channels with configured inbound/outbound capacities and a
control channel capacity of 8. The driver is the only owner of
`WebSocketStream<TokioIo<Upgraded>>`.

- [ ] **Step 4: Replace direct stream ownership in `WebSocket`**

`WebSocket` must contain one receiver plus a clonable sender handle:

```rust
pub struct WebSocket {
    receiver: WebSocketReceiver,
    sender: WebSocketSender,
}

#[derive(Clone)]
pub struct WebSocketSender {
    shared: Arc<SocketShared>,
}

pub struct WebSocketReceiver {
    inbound: mpsc::Receiver<Result<WebSocketMessage, WebSocketError>>,
    close_rx: watch::Receiver<Option<WebSocketCloseInfo>>,
}
```

Keep every existing `WebSocket` method signature. Direct methods delegate to
the handles. Add `id()`, `remote_addr()`, and `route()` to both `WebSocket` and
`WebSocketSender`. `split(self)` returns `(WebSocketReceiver,
WebSocketSender)`.

- [ ] **Step 5: Implement the first driver loop**

Use a biased `tokio::select!` with branches, in order, for
`control_rx.recv()`, `outbound_rx.recv()`, and `stream.next()`. For this task
only:

- control commands write the corresponding frame immediately;
- outbound messages call `stream.send(message).await`;
- incoming messages are sent to the bounded inbound channel;
- transport end or error publishes close information and exits;
- the `ConnectionPermit` remains owned by the driver until exit.

Do not implement heartbeat or lifecycle timers in this task.
The driver owns the handler `JoinHandle`; on every driver exit path it waits for
normal handler completion or aborts that handler before releasing the permit.
Only the driver task's abort handle is stored in the runtime registry, so one
forced abort cannot leave an orphan handler.

- [ ] **Step 6: Register the sender with the runtime after upgrade**

After channels exist, update the runtime entry with a weak/clonable internal
sender used later by shutdown and rooms. If the entry disappeared because
shutdown won the race, send Close 1001 and exit without starting the handler.

- [ ] **Step 7: Verify direct and split APIs**

Run:

```bash
cargo fmt
cargo test --test websocket_integration websocket_routes_exchange_messages_and_events
cargo test --test websocket_integration websocket_sender_progresses_independently
cargo test --test websocket_api_compat
```

Expected: PASS.

- [ ] **Step 8: Commit the driver migration**

```bash
git add src/app/websocket tests/websocket_integration.rs tests/http_integration.rs
git commit -m "feat: drive websockets through bounded channels"
```

### Task 6: Implement Backpressure Policies and Precise Split-Handle Errors

**Files:**
- Modify: `src/app/websocket/socket.rs`
- Modify: `src/app/websocket/driver.rs`
- Modify: `src/app/websocket/error.rs`
- Modify: `tests/websocket_integration.rs`

- [ ] **Step 1: Write one failing test per policy**

Use outbound capacity 1 and a client that stops reading. Assert:

- `try_send` returns `WsError::Capacity(OutboundQueue)` immediately;
- `Wait` returns `WsError::Timeout(Send)` after `send_timeout`;
- `Reject` returns capacity without waiting;
- `Disconnect` causes Close 1013 and increments saturated/disconnected counts.

Use paused Tokio time only for unit-level sender tests; real TCP tests must use
small finite deadlines and `timeout` guards.

- [ ] **Step 2: Run the policy tests and confirm failure**

Run:

```bash
cargo test --test websocket_integration websocket_backpressure
```

Expected: FAIL.

- [ ] **Step 3: Implement precise sender methods**

`WebSocketSender::try_send` uses `try_reserve_owned`; `send` behaves exactly as
configured:

```rust
match self.shared.backpressure_policy {
    BackpressurePolicy::Wait => {
        let permit = tokio::time::timeout(
            self.shared.send_timeout,
            self.shared.outbound.clone().reserve_owned(),
        )
        .await
        .map_err(|_| WsError::Timeout(WebSocketTimeout::Send))?
        .map_err(|_| WsError::Closed)?;
        permit.send(OutboundCommand::Message(message));
        Ok(())
    }
    BackpressurePolicy::Reject => self.try_send(message),
    BackpressurePolicy::Disconnect => match self.try_send(message) {
        Ok(()) => Ok(()),
        Err(WsError::Capacity(_)) => {
            self.disconnect_slow_consumer().await?;
            Err(WsError::Capacity(WebSocketCapacityError::OutboundQueue))
        }
        Err(error) => Err(error),
    },
}
```

Direct `WebSocket::send` keeps returning `WebSocketError`; map `WsError` to a
Tungstenite I/O/protocol error in one private compatibility conversion. Do not
alter `WebSocketError` variants.

- [ ] **Step 4: Add complete split convenience methods**

Add `send_text`, `send_binary`, `send_json`, `send_event`, `ping`, `pong`,
`close`, `close_with`, metadata accessors, and `closed` to sender. Add
`recv_json`, `recv_event`, and `closed` to receiver. Use the same serialization
helpers as direct `WebSocket` methods.

- [ ] **Step 5: Verify bounded queues and no silent drops**

Run:

```bash
cargo fmt
cargo test websocket_backpressure
cargo test --test websocket_integration websocket_backpressure
cargo test --test websocket_api_compat
```

Expected: PASS and every rejected send is visible as an error or close.

- [ ] **Step 6: Commit backpressure support**

```bash
git add src/app/websocket tests/websocket_integration.rs
git commit -m "feat: enforce websocket backpressure policies"
```

### Task 7: Add Heartbeat, Idle/Lifetime Deadlines, and RFC 6455 Closure

**Files:**
- Modify: `src/app/websocket/driver.rs`
- Modify: `src/app/websocket/socket.rs`
- Modify: `src/app/websocket/types.rs`
- Modify: `src/app/websocket/error.rs`
- Modify: `tests/websocket_integration.rs`

- [ ] **Step 1: Write failing lifecycle tests**

Add tests for:

1. Ping arrives while handler never calls `recv()`.
2. Matching Pong prevents closure.
3. A raw client that suppresses Pong receives/causes heartbeat timeout.
4. Idle timeout closes with 1001 after no text/binary frames.
5. Lifetime timeout closes with 1001 even while messages flow.
6. `close_with(1000, "finalizado")` completes a clean handshake.
7. Invalid code or a reason over 123 encoded bytes returns `WsError::InvalidClose`.

- [ ] **Step 2: Run lifecycle tests and confirm failure**

Run:

```bash
cargo test --test websocket_integration websocket_heartbeat
cargo test --test websocket_integration websocket_close_handshake
```

Expected: FAIL.

- [ ] **Step 3: Define close information and legal-code validation**

Add non-exhaustive `WebSocketCloseInfo { code, reason, initiator, clean }` and
`WebSocketCloseInitiator::{Local, Peer, Runtime, Timeout, ProtocolError,
Handler}`. Legal outgoing codes are 1000, 1001, 1002, 1003, 1007, 1008, 1009,
1010, 1011, 1012, 1013, and 3000..=4999. Reject 1004, 1005, 1006, 1015, and
all other reserved/unassigned values. Check UTF-8 encoded reason length, not
character count.

- [ ] **Step 4: Extend the driver state machine**

Track:

```rust
struct DriverState {
    close_sent: bool,
    close_received: bool,
    close_info: Option<WebSocketCloseInfo>,
    pending_ping: Option<(Bytes, Instant)>,
    last_application_message: Instant,
    opened_at: Instant,
}
```

The select loop must independently poll ping, Pong deadline, idle deadline,
lifetime deadline, shutdown, control, outbound, and transport input. Incoming
Ping queues an immediate Pong and is then forwarded to `recv()`. Incoming Pong
clears only a matching pending token and is forwarded. Incoming Close is
forwarded, echoed once, and starts `close_timeout`.

Use an incrementing 8-byte big-endian heartbeat token. A missing matching Pong
closes with 1001 and records `WebSocketTimeout::Pong`. Oversized messages map
to Close 1009; policy/rate violations map to 1008; internal handler failures
map to 1011.

Schedule Ping from the timestamp of the last inbound frame. Any inbound frame
resets the next-idle-Ping timer, but only a matching Pong clears an outstanding
probe; unrelated text/binary/Ping traffic must not mask a missing Pong timeout.

- [ ] **Step 5: Make closure observable from all handles**

Publish exactly one `WebSocketCloseInfo` through the watch channel. `closed()`
returns immediately if already closed, otherwise waits for `changed()`. Driver
cleanup must publish before dropping the runtime permit.

- [ ] **Step 6: Verify lifecycle behavior**

Run:

```bash
cargo fmt
cargo test --test websocket_integration websocket_heartbeat
cargo test --test websocket_integration websocket_close
cargo test websocket_close_code
```

Expected: PASS.

- [ ] **Step 7: Commit lifecycle handling**

```bash
git add src/app/websocket tests/websocket_integration.rs
git commit -m "feat: supervise websocket heartbeat and closure"
```

### Task 8: Normalize Handler Results and Isolate Panics

**Files:**
- Modify: `src/app/websocket/socket.rs`
- Modify: `src/app/websocket/driver.rs`
- Modify: `tests/websocket_api_compat.rs`
- Modify: `tests/websocket_integration.rs`

- [ ] **Step 1: Write failing compile/runtime tests**

Extend the compatibility fixture with handlers returning `()`,
`Result<(), WebSocketError>`, and `Result<(), WsError>`. Add a real TCP test
where a handler panics and assert only that connection closes with 1011 while a
second connection still echoes messages.

- [ ] **Step 2: Run tests and confirm failure**

Run:

```bash
cargo test --test websocket_api_compat
cargo test --test websocket_integration websocket_handler_panic
```

Expected: FAIL for result-returning handlers and panic close behavior.

- [ ] **Step 3: Normalize handler output without changing call syntax**

Define:

```rust
pub trait IntoWebSocketOutput {
    fn into_websocket_output(self) -> Result<(), WsError>;
}

impl IntoWebSocketOutput for () {
    fn into_websocket_output(self) -> Result<(), WsError> { Ok(()) }
}

impl IntoWebSocketOutput for Result<(), WebSocketError> {
    fn into_websocket_output(self) -> Result<(), WsError> { self.map_err(Into::into) }
}

impl IntoWebSocketOutput for Result<(), WsError> {
    fn into_websocket_output(self) -> Result<(), WsError> { self }
}
```

Mark `IntoWebSocketOutput` `#[doc(hidden)]` but re-export it from `app` and the
crate root because it appears in public method bounds; this avoids an
unnameable/private-bound API while keeping normal documentation focused.

Use one blanket `IntoWebSocketHandler` implementation for `Fn(WebSocket) ->
Future<Output = O>` where `O: IntoWebSocketOutput`. Keep the public
`WebSocketHandler` alias source-compatible; use a separate private normalized
handler alias internally.

Update `App::{websocket, ws, websocket_with}` and the equivalent `Router`
methods from `Fut: Future<Output = ()>` to:

```rust
pub fn websocket<F, Fut, O>(&mut self, path: &str, handler: F)
where
    F: Fn(WebSocket) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O> + Send + 'static,
    O: IntoWebSocketOutput + Send + 'static;
```

Apply the same generic output to `ws` and `websocket_with`. Existing `()`
closures infer `O = ()` and remain source-compatible.
Inside router registration, normalize directly to the private result-returning
handler type; do not assign result handlers to the legacy `WebSocketHandler`
alias. A value explicitly typed as the legacy alias still implements
`IntoWebSocketHandler` through the blanket `O = ()` path.

- [ ] **Step 4: Catch sync and async panics at the handler boundary**

Wrap closure invocation with
`catch_unwind(AssertUnwindSafe(|| handler(socket)))`; if it succeeds, wrap the
returned future with `AssertUnwindSafe(future).catch_unwind().await`. Send the
normalized result to the driver through a oneshot channel. Panic maps to
`WsError::HandlerPanic` and Close 1011; normal completion starts Close 1000 if
no sender handles remain and no close already started.

- [ ] **Step 5: Verify isolation and source compatibility**

Run:

```bash
cargo fmt
cargo test --test websocket_api_compat
cargo test --test websocket_integration websocket_handler
```

Expected: PASS.

- [ ] **Step 6: Commit handler normalization**

```bash
git add src/app/websocket tests/websocket_api_compat.rs tests/websocket_integration.rs
git commit -m "feat: isolate websocket handler failures"
```

### Task 9: Enforce Connection, Message-Rate, and Inbound-Consumer Limits

**Files:**
- Modify: `src/app/websocket/runtime.rs`
- Modify: `src/app/websocket/driver.rs`
- Modify: `src/app/websocket/config.rs`
- Modify: `tests/websocket_integration.rs`

- [ ] **Step 1: Write failing limit tests**

Add real TCP tests asserting:

- process/route capacity rejects with 503 before upgrade;
- per-IP capacity rejects with 429 and `Retry-After`;
- a connection slot is reusable after clean and abrupt close;
- message-rate overflow closes with 1008;
- a handler that never receives cannot cause unbounded inbound growth and is
  closed as a slow consumer after the configured inbound deadline.

- [ ] **Step 2: Run limit tests and confirm failure**

Run:

```bash
cargo test --test websocket_integration websocket_connection_limit
cargo test --test websocket_integration websocket_message_rate
```

Expected: FAIL.

- [ ] **Step 3: Verify process defaults and route overrides explicitly**

Add tests proving the resolution introduced in Task 4: route values override
App defaults; an unset route field inherits App; process capacity comes from
App `max_connections`; route capacity comes from route `max_connections`; and
route room limits may lower but never exceed future hub hard ceilings. Also
prove each `disable_*` builder clears the corresponding inherited optional
setting. Ensure each dispatched request carries one immutable resolved snapshot.

- [ ] **Step 4: Add a fixed-window per-connection message limiter**

Count text and binary messages only. At window rollover reset the count. On
overflow, do not enqueue the violating message; close with 1008, record
`WebSocketCapacityError::MessageRate`, and release accounting after close.

- [ ] **Step 5: Bound inbound delivery time**

When the inbound queue is full, wait no longer than `send_timeout` to enqueue
the received message. On timeout, close with 1013 and classify
`InboundQueue`. Control frames still receive mandatory protocol handling and
then use the same bounded delivery rule.

- [ ] **Step 6: Verify limits and slot cleanup**

Run:

```bash
cargo fmt
cargo test --test websocket_integration websocket_connection_limit
cargo test --test websocket_integration websocket_message_rate
cargo test --test websocket_integration websocket_slow_consumer
```

Expected: PASS.

- [ ] **Step 7: Commit resource limits**

```bash
git add src/app/websocket tests/websocket_integration.rs
git commit -m "feat: enforce websocket resource limits"
```

### Task 10: Integrate WebSockets into Plain and TLS Graceful Shutdown

**Files:**
- Modify: `src/app/websocket/runtime.rs`
- Modify: `src/app/server.rs`
- Modify: `src/app/tls.rs`
- Modify: `tests/websocket_integration.rs`
- Modify: `tests/tls_integration.rs`

- [ ] **Step 1: Write failing plain-server shutdown tests**

Add tests with one cooperative client and one raw client that never replies to
Close. Assert shutdown sends 1001 `apagado del servidor`, waits for the configured
grace period, force-aborts the uncooperative task, and returns within the outer
server deadline.

- [ ] **Step 2: Run shutdown tests and confirm failure**

Run:

```bash
cargo test --test websocket_integration websocket_shutdown
```

Expected: FAIL because upgraded tasks are detached from graceful shutdown.

- [ ] **Step 3: Implement runtime shutdown phases**

Add:

```rust
pub(crate) async fn begin_shutdown(&self) {
    self.stop_accepting();
    let _ = self.inner.shutdown_tx.send(true);
}

pub(crate) async fn drain(&self, timeout: Duration) -> Result<(), WsError> {
    if self.active_count() == 0 {
        return Ok(());
    }
    tokio::time::timeout(timeout, self.wait_until_empty())
        .await
        .map_err(|_| WsError::Timeout(WebSocketTimeout::Shutdown))
}
```

Keep `JoinHandle::abort` handles in the registry. After drain timeout, remove
and abort all remaining tasks, then wait until every permit has released.
Every driver holds a shutdown watch receiver; when it observes `true`, it sends
Close 1001 with reason `apagado del servidor` through its priority control path.
This makes shutdown O(1) to signal and allows all connections to close
concurrently.

- [ ] **Step 4: Share one server shutdown helper between plain and TLS paths**

When the external shutdown future resolves:

1. stop accepting TCP;
2. call `runtime.begin_shutdown()`;
3. run Hyper `GracefulShutdown` and runtime drain concurrently;
4. after the configured 10-second outer deadline, abort remaining WebSockets;
5. return `Ok(())` only after registry cleanup.

Do not duplicate the state machine in `tls.rs`; expose a crate-private helper
from `server.rs` or `runtime.rs` and call it from both paths.

- [ ] **Step 5: Add WSS shutdown coverage**

Extend `tests/tls_integration.rs` with a trusted self-signed rustls client,
`tokio_tungstenite::client_async_tls_with_config` over that stream, and the
same cooperative Close 1001 assertion.

- [ ] **Step 6: Expose bounded runtime administration**

Add public methods:

```rust
pub async fn close(
    &self,
    id: WebSocketId,
    code: u16,
    reason: &str,
) -> Result<(), WsError>;

pub async fn shutdown(&self) -> Result<(), WsError>;
```

`close` validates the close frame, addresses only a local registered
connection, and waits no longer than that connection's close timeout.
`shutdown` stops future WebSocket admission and drains WebSockets using the
configured runtime deadline; it does not stop the HTTP listener. Add tests for
unknown IDs, successful close, and admission rejection after administrative
shutdown.

- [ ] **Step 7: Verify plain and TLS shutdown**

Run:

```bash
cargo fmt
cargo test --test websocket_integration websocket_shutdown
cargo test --features tls --test tls_integration websocket
```

Expected: PASS.

- [ ] **Step 8: Commit lifecycle integration**

```bash
git add src/app/websocket src/app/server.rs src/app/tls.rs tests/websocket_integration.rs tests/tls_integration.rs
git commit -m "feat: drain websockets during server shutdown"
```

### Task 11: Add Runtime Statistics, Observer Hooks, and Structured Tracing

**Files:**
- Modify: `src/app/websocket/types.rs`
- Modify: `src/app/websocket/runtime.rs`
- Modify: `src/app/websocket/driver.rs`
- Modify: `src/app/server.rs`
- Modify: `src/app.rs`
- Modify: `src/lib.rs`
- Modify: `src/app/tests.rs`
- Modify: `tests/websocket_integration.rs`

- [ ] **Step 1: Write failing observer and stats tests**

Use a recording observer backed by `Mutex<Vec<String>>`; assert open, message,
queue saturation, close, and heartbeat events. Add an observer that panics and
assert the connection still echoes. Assert runtime snapshots contain IDs,
route pattern, remote address, protocol, timestamps, and no payload.

- [ ] **Step 2: Run focused tests and confirm failure**

Run:

```bash
cargo test websocket_observer
cargo test --test websocket_integration websocket_runtime_stats
```

Expected: FAIL.

- [ ] **Step 3: Define the non-blocking observer contract**

Add:

```rust
pub trait WebSocketObserver: Send + Sync + 'static {
    fn observe(&self, event: &WebSocketObservation<'_>);
}

#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum WebSocketObservation<'a> {
    Accepted { id: WebSocketId, route: &'a str },
    Rejected { route: &'a str, reason: &'a str },
    Opened { id: WebSocketId },
    Message { id: WebSocketId, outbound: bool, bytes: usize },
    QueueSaturated { id: WebSocketId, outbound: bool },
    HeartbeatTimeout { id: WebSocketId },
    Closed { id: WebSocketId, code: Option<u16>, clean: bool },
    HandlerFailed { id: WebSocketId },
    ForcedShutdown { id: WebSocketId },
}
```

Invoke callbacks through `catch_unwind`; never hold registry locks while
calling observers. Treat these observation callbacks as the framework's
lifecycle/message-metadata hooks; do not add a second callback registry.

- [ ] **Step 4: Keep counters in runtime-owned atomics/locked state**

Update counters on accepted/rejected/opened/message bytes/saturation/heartbeat
timeout/closed. `stats()` takes one coherent snapshot. `connections()` sorts
by ID for deterministic output.

- [ ] **Step 5: Emit tracing events only behind the feature**

Create a connection span with `ws.id`, `ws.route`, `ws.remote_addr`, and
`ws.protocol`. Emit events for rejection, open, close, handler failure,
heartbeat timeout, backpressure, and forced shutdown. Record only message type
and byte length; never payloads.

- [ ] **Step 6: Verify observers, tracing build, and no payload leakage**

Run:

```bash
cargo fmt
cargo test websocket_observer
cargo test --test websocket_integration websocket_runtime_stats
cargo check --all-targets --features tracing
cargo clippy --all-targets --features tracing -- -D warnings
```

Expected: PASS.

- [ ] **Step 7: Commit observability**

```bash
git add src/app/websocket src/app/server.rs src/app/tests.rs src/app.rs src/lib.rs tests/websocket_integration.rs
git commit -m "feat: observe websocket runtime state"
```

### Task 12: Core Phase Verification Gate

**Files:**
- Modify only files needed to fix failures found by this gate.

- [ ] **Step 1: Run formatting, compilation, lint, and all tests**

```bash
cargo fmt --check
cargo check --all-targets
cargo check --all-targets --features "tls tracing brotli"
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo test --features tls
cargo test --all-features
cargo doc --no-deps --all-features
```

Expected: every command exits 0.

- [ ] **Step 2: Run the compatibility fixture by itself**

```bash
cargo test --test websocket_api_compat
```

Expected: PASS, including exhaustive matching of the unchanged
`WebSocketError` variants.

- [ ] **Step 3: Review resource ownership manually**

Confirm from the code and tests:

- exactly one task owns each `WebSocketStream`;
- all Tokio channels are bounded;
- every admitted connection releases its route/IP/global counters;
- no mutex guard is held across `.await`;
- observer callbacks run outside locks and are panic-isolated;
- the runtime owns every connection task until clean exit or forced abort;
- plain and TLS shutdown both drain the same registry.

- [ ] **Step 4: Commit only gate fixes, if any**

```bash
git add src tests Cargo.toml Cargo.lock
git commit -m "fix: close websocket core verification gaps"
```

Skip this commit when the gate required no changes.
