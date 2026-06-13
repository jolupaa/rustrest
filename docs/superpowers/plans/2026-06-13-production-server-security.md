# RustRest Production Server and Security Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver RustRest `0.4.0` with a controllable server lifecycle, bounded transport resources, spoof-resistant proxy identity and production security middleware.

**Architecture:** Replace duplicated plain/TLS serve loops with a transport-neutral connection runner configured by `ServerBuilder`. Resolve client identity through an explicit trusted-proxy policy. Move middleware into focused modules with trait-based stores for policies that may be local or distributed.

**Tech Stack:** Tokio networking and synchronization, Hyper/hyper-util server builders, rustls, `ipnet`, `http`, tracing hooks from the following phase, atomic counters and semaphores.

---

## File Map

| Path | Responsibility |
| --- | --- |
| `src/app/server/config.rs` | public server and protocol configuration |
| `src/app/server/handle.rs` | local address, stats and shutdown handle |
| `src/app/server/connection.rs` | shared plain/TLS connection execution |
| `src/app/server/mod.rs` | App serving entry points and compatibility wrappers |
| `src/app/proxy.rs` | trusted CIDRs and forwarded-header resolution |
| `src/app/middleware/request_id.rs` | validated request IDs |
| `src/app/middleware/concurrency.rs` | concurrency limit and load shedding |
| `src/app/middleware/security_headers.rs` | configurable browser security headers |
| `src/app/middleware/cors.rs` | complete CORS policy |
| `src/app/middleware/rate_limit.rs` | rate-limit store contract and local implementation |
| `tests/server_limits.rs` | connection, handshake, timeout and drain tests |
| `tests/trusted_proxy.rs` | proxy spoofing and chain-resolution tests |
| `tests/security_middleware.rs` | security/CORS/rate-limit behavior |

### Task 1: Introduce `ServerBuilder` and `ServerHandle`

**Files:**
- Create: `src/app/server/mod.rs`
- Create: `src/app/server/config.rs`
- Create: `src/app/server/handle.rs`
- Create: `src/app/server/connection.rs`
- Move from: `src/app/server.rs`
- Modify: `src/app.rs`
- Modify: `src/lib.rs`
- Create: `tests/server_limits.rs`

- [ ] **Step 1: Write failing lifecycle tests**

Create `tests/server_limits.rs`:

```rust
#[tokio::test]
async fn server_handle_reports_address_stats_and_shutdown() {
    let mut app = App::new();
    app.get("/health", |_req: Request| Response::send("ok"));

    let handle = app.server().listen("127.0.0.1:0").await.unwrap();
    assert_ne!(handle.local_addr().port(), 0);
    assert_eq!(handle.stats().active_connections, 0);

    handle.shutdown().await.unwrap();
    handle.wait().await.unwrap();
}
```

Also add a compatibility test proving `app.listen(address).await` still blocks until shutdown or
server failure.

- [ ] **Step 2: Verify failure**

Run `cargo test --test server_limits server_handle_reports_address_stats_and_shutdown`.

Expected: compilation fails because `App::server` and `ServerHandle` do not exist.

- [ ] **Step 3: Define public configuration types**

Create `src/app/server/config.rs`:

```rust
#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub max_body_size: usize,
    pub request_timeout: Option<Duration>,
    pub header_read_timeout: Duration,
    pub graceful_shutdown_timeout: Duration,
    pub max_connections: usize,
    pub max_connections_per_ip: usize,
    pub tcp_nodelay: bool,
    pub tcp_keepalive: Option<TcpKeepaliveConfig>,
    pub http1: Http1Config,
    pub http2: Http2Config,
}

#[derive(Clone, Debug)]
pub struct Http1Config {
    pub keep_alive: bool,
    pub half_close: bool,
    pub max_buf_size: usize,
}

#[derive(Clone, Debug)]
pub struct Http2Config {
    pub max_concurrent_streams: u32,
    pub initial_stream_window_size: u32,
    pub initial_connection_window_size: u32,
    pub keep_alive_interval: Option<Duration>,
    pub keep_alive_timeout: Duration,
}
```

Choose bounded defaults and validate all zero/inconsistent values in `ServerConfig::validate`.

- [ ] **Step 4: Implement builder and handle**

Expose:

```rust
impl App {
    pub fn server(self) -> ServerBuilder { ServerBuilder::new(self) }
}

impl ServerBuilder {
    pub fn max_connections(mut self, value: usize) -> Self;
    pub fn max_connections_per_ip(mut self, value: usize) -> Self;
    pub fn graceful_shutdown_timeout(mut self, value: Duration) -> Self;
    pub async fn listen(self, address: impl ToSocketAddrs) -> io::Result<ServerHandle>;
    pub async fn serve(self, listener: TcpListener) -> io::Result<ServerHandle>;
}
```

`ServerHandle` owns a cancellation sender, a shared stats object and a `JoinHandle<io::Result<()>>`.
Make `shutdown(&self)` idempotent; make `wait(self)` return the serve task result exactly once.
Expose `is_alive()` and `is_ready()`: alive means the serve task has not terminated; ready means the
listener is accepting and shutdown has not started.

- [ ] **Step 5: Preserve compatibility wrappers**

Implement current `App::listen`, `serve`, `listen_with_shutdown` and `serve_with_shutdown` in terms
of `ServerBuilder`, then wait on the returned handle. Do not duplicate serving logic.

- [ ] **Step 6: Verify**

Run:

```bash
cargo test --test server_limits server_handle
cargo test serve_with_shutdown
cargo test
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/app/server src/app.rs src/lib.rs tests/server_limits.rs src/app/tests.rs
git rm src/app/server.rs
git commit -m "feat: add configurable server handle"
```

### Task 2: Enforce Connection and Protocol Limits

**Files:**
- Modify: `src/app/server/config.rs`
- Modify: `src/app/server/connection.rs`
- Modify: `src/app/server/handle.rs`
- Modify: `src/app/tls.rs`
- Modify: `tests/server_limits.rs`
- Modify: `tests/tls_integration.rs`

- [ ] **Step 1: Add failing process/per-IP limit tests**

Use `TcpStream` connections that hold requests open:

```rust
#[tokio::test]
async fn server_rejects_connections_above_process_limit() {
    let app = slow_header_app();
    let handle = app.server().max_connections(2).listen("127.0.0.1:0").await.unwrap();

    let first = TcpStream::connect(handle.local_addr()).await.unwrap();
    let second = TcpStream::connect(handle.local_addr()).await.unwrap();
    wait_for(|| handle.stats().active_connections == 2).await;
    let mut third = TcpStream::connect(handle.local_addr()).await.unwrap();

    assert_connection_closed_without_response(&mut third).await;
    assert_eq!(handle.stats().rejected_connections, 1);
    drop((first, second));
    handle.shutdown().await.unwrap();
}
```

Add a per-IP variant and a test proving permits return after disconnect.

- [ ] **Step 2: Verify failure**

Run `cargo test --test server_limits server_rejects_connections_above_process_limit`.

Expected: the third connection remains active or stats are unavailable.

- [ ] **Step 3: Implement admission state**

Use a process `Semaphore` plus a sharded per-IP counter map. Admission must be atomic: either both
limits are reserved or neither is. Store permits in a `ConnectionPermit` whose `Drop` releases the
per-IP count and process permit, including TLS handshake failures and task panics.

Expose:

```rust
#[derive(Clone, Copy, Debug, Default)]
pub struct ServerStats {
    pub accepted_connections: u64,
    pub rejected_connections: u64,
    pub active_connections: u64,
    pub active_requests: u64,
}
```

Use atomics for process counters; do not take a synchronous global mutex on every request.

- [ ] **Step 4: Apply Hyper protocol settings**

Map `Http1Config` and `Http2Config` into `hyper_util::server::conn::auto::Builder`. Set the timer
before header-read and HTTP/2 keepalive settings. Add unit tests that invalid config is rejected
before binding.

Apply TCP_NODELAY and configured socket keepalive immediately after accept. Categorize accept
errors; use exponential backoff capped at one second for file-descriptor/resource exhaustion and a
short fixed delay for transient errors. Reset backoff after the next successful accept.

- [ ] **Step 5: Bound TLS handshakes**

Add `tls_handshake_timeout` and `max_concurrent_tls_handshakes` to TLS configuration. Reserve a TLS
handshake permit before `acceptor.accept`, wrap the future in `tokio::time::timeout`, and release the
permit before serving the established connection.

- [ ] **Step 6: Make drain timeout configurable**

Replace the fixed 10-second constant with `graceful_shutdown_timeout`. At expiry, abort remaining
HTTP and WebSocket tasks, wait for permit counts to reach zero, and return a shutdown report through
the handle rather than printing a string.

- [ ] **Step 7: Verify**

Run:

```bash
cargo test --test server_limits
cargo test --features tls --test tls_integration
cargo test --test websocket_integration websocket_runtime_shutdown
```

Expected: all pass without leaked active-connection counts.

- [ ] **Step 8: Commit**

```bash
git add src/app/server src/app/tls.rs tests/server_limits.rs tests/tls_integration.rs tests/websocket_integration.rs
git commit -m "feat: enforce server transport limits"
```

### Task 3: Add Trusted Proxy Resolution

**Files:**
- Create: `src/app/proxy.rs`
- Modify: `src/app.rs`
- Modify: `src/app/request.rs`
- Modify: `src/app/server/mod.rs`
- Modify: `src/lib.rs`
- Modify: `Cargo.toml`
- Create: `tests/trusted_proxy.rs`

- [ ] **Step 1: Add spoofing and resolution tests**

Create `tests/trusted_proxy.rs`:

```rust
#[test]
fn untrusted_peer_cannot_spoof_forwarded_client() {
    let policy = TrustedProxies::new().allow("10.0.0.0/8".parse().unwrap());
    let result = policy.resolve(
        "203.0.113.10:5000".parse().unwrap(),
        &["for=198.51.100.7;proto=https;host=api.example.com"],
        &[],
    ).unwrap();

    assert_eq!(result.client_addr.ip(), "203.0.113.10".parse::<IpAddr>().unwrap());
    assert_eq!(result.scheme, None);
    assert_eq!(result.host, None);
}

#[test]
fn trusted_chain_is_walked_from_the_nearest_proxy_inward() {
    let policy = TrustedProxies::new()
        .allow("10.0.0.0/8".parse().unwrap())
        .allow("192.168.0.0/16".parse().unwrap());
    let result = policy.resolve(
        "10.1.0.4:5000".parse().unwrap(),
        &["for=198.51.100.7, for=192.168.1.5;proto=https;host=api.example.com"],
        &[],
    ).unwrap();

    assert_eq!(result.client_addr.ip(), "198.51.100.7".parse::<IpAddr>().unwrap());
    assert_eq!(result.scheme.as_deref(), Some("https"));
    assert_eq!(result.host.as_deref(), Some("api.example.com"));
}
```

- [ ] **Step 2: Verify failure**

Run `cargo test --test trusted_proxy`.

Expected: compilation fails because proxy policy types do not exist.

- [ ] **Step 3: Add `ipnet` and implement the trust policy**

Add `ipnet = "2"` to dependencies. Define:

```rust
#[derive(Clone, Default)]
pub struct TrustedProxies {
    networks: Vec<ipnet::IpNet>,
    mode: ForwardedMode,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum ForwardedMode {
    #[default]
    StandardThenXForwarded,
    StandardOnly,
    XForwardedOnly,
}
```

Parse RFC 7239 quoted values, bracketed IPv6, optional ports and comma-separated elements. Reject
control characters, duplicate singleton parameters, invalid IP literals and chains longer than a
configurable maximum.

- [ ] **Step 4: Store peer and resolved identity separately**

Add to `RequestParts`:

```rust
peer_addr: Option<SocketAddr>,
client_addr: Option<SocketAddr>,
original_scheme: Option<String>,
original_host: Option<String>,
```

Expose `peer_addr`, `client_addr`, `original_scheme` and `original_host`. Keep `remote_addr` as a
deprecated alias for `peer_addr` during `0.4`.

- [ ] **Step 5: Resolve identity once in the server pipeline**

After parsing headers and before middleware, call the configured policy only when the direct peer
is trusted. Store the result; downstream middleware must not parse forwarded headers independently.

- [ ] **Step 6: Verify property and integration behavior**

Run:

```bash
cargo test --test trusted_proxy
cargo test request_exposes_client_peer_address
cargo test
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock src/app/proxy.rs src/app.rs src/app/request.rs src/app/server src/lib.rs tests/trusted_proxy.rs tests/http_integration.rs
git commit -m "feat: resolve client identity through trusted proxies"
```

### Task 4: Add Cancellation, Concurrency Limit and Load Shedding Middleware

**Files:**
- Create: `src/app/middleware/concurrency.rs`
- Create: `src/app/middleware/timeout.rs`
- Modify: `src/app/middleware/mod.rs`
- Modify: `src/app/request.rs`
- Modify: `src/app/server/connection.rs`
- Create: `tests/middleware_load.rs`

- [ ] **Step 1: Write failing cancellation and saturation tests**

Create `tests/middleware_load.rs` with:

```rust
#[tokio::test]
async fn concurrency_limit_sheds_without_waiting_when_configured() {
    let gate = Arc::new(Notify::new());
    let mut app = App::new();
    app.layer(ConcurrencyLimit::new(1).load_shed(true));
    app.get("/work", blocking_handler(gate.clone()));

    let client = Arc::new(TestClient::new(app));
    let first = tokio::spawn({ let client = client.clone(); async move { client.get("/work").send().await } });
    wait_until_first_request_started().await;
    let second = client.get("/work").send().await;

    assert_eq!(second.status, 503);
    gate.notify_waiters();
    assert_eq!(first.await.unwrap().status, 200);
}
```

Add a real-network disconnect test where a handler waits on `req.cancelled()` and observes
cancellation after the TCP client closes.

- [ ] **Step 2: Verify failure**

Run `cargo test --test middleware_load`.

Expected: missing concurrency and cancellation APIs.

- [ ] **Step 3: Add request cancellation token**

Store a lightweight cancellation primitive in `RequestParts` and expose:

```rust
pub async fn cancelled(&self) {
    self.parts.cancellation.cancelled().await
}

pub fn is_cancelled(&self) -> bool {
    self.parts.cancellation.is_cancelled()
}
```

Cancel it when the request future is dropped because of client disconnect, timeout or server
shutdown. Add a drop guard around each request service future.

- [ ] **Step 4: Implement `ConcurrencyLimit`**

Use a `Semaphore`. In queued mode, await a permit with cancellation. In load-shed mode, use
`try_acquire_owned` and return structured `503 service_unavailable` with optional `Retry-After`.
Hold the permit until the response body finishes, not only until headers are produced; wrap the
response body with a permit-owning body.

- [ ] **Step 5: Update timeout middleware**

On timeout, cancel the request token before returning `408` or configurable `504`. Ensure inner
handler work cannot continue detached unless the application explicitly spawned it.

- [ ] **Step 6: Verify**

Run:

```bash
cargo test --test middleware_load
cargo test timeout_middleware
cargo test
```

Expected: all pass and no permit remains after a dropped streamed response.

- [ ] **Step 7: Commit**

```bash
git add src/app/middleware/concurrency.rs src/app/middleware/timeout.rs src/app/middleware/mod.rs src/app/request.rs src/app/response.rs src/app/server/connection.rs tests/middleware_load.rs
git commit -m "feat: add request cancellation and load shedding"
```

### Task 5: Harden Request IDs, CORS and Security Headers

**Files:**
- Create: `src/app/middleware/request_id.rs`
- Create: `src/app/middleware/cors.rs`
- Create: `src/app/middleware/security_headers.rs`
- Modify: `src/app/middleware/mod.rs`
- Modify: `Cargo.toml`
- Create: `tests/security_middleware.rs`

- [ ] **Step 1: Add failing table-driven tests**

Test these behaviors:

- request IDs longer than 128 bytes or containing non-visible ASCII are replaced;
- generated IDs use 128 bits of randomness, not timestamp/counter predictability;
- CORS emits `Vary: Origin, Access-Control-Request-Method, Access-Control-Request-Headers` where
  required;
- denied preflight returns `403` rather than an ambiguous empty `204`;
- private-network preflight is denied unless enabled;
- credentials never combine with wildcard origin;
- HSTS is emitted only for secure/originally secure requests;
- default security headers include `X-Content-Type-Options: nosniff` and a referrer policy.

- [ ] **Step 2: Verify failure**

Run `cargo test --test security_middleware`.

Expected: multiple assertions fail under the current middleware.

- [ ] **Step 3: Implement request ID policy**

Add `getrandom = "0.3"` to core dependencies.

Define:

```rust
pub trait RequestIdGenerator: Send + Sync + 'static {
    fn generate(&self) -> HeaderValue;
}

pub struct RequestId {
    header: HeaderName,
    max_len: usize,
    trust_incoming: bool,
    generator: Arc<dyn RequestIdGenerator>,
}
```

Use `getrandom` to generate 16 random bytes encoded as lowercase hex. Put the validated value in
request extensions and response headers.

- [ ] **Step 4: Complete CORS policy**

Represent allowed methods and headers as parsed `Method`/`HeaderName` sets. Validate configuration
at construction. Add exposed headers, private-network permission, wildcard subdomain matching only
when explicitly enabled, and deterministic `Vary` merging.

- [ ] **Step 5: Implement `SecurityHeaders`**

Provide a builder for nosniff, frame options, referrer policy, permissions policy, CSP, COOP/CORP
and HSTS. Defaults should be broadly safe without breaking normal APIs; HSTS and CSP remain opt-in
because deployment/application context matters.

- [ ] **Step 6: Verify**

Run:

```bash
cargo test --test security_middleware
cargo test cors
cargo test request_id
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock src/app/middleware/request_id.rs src/app/middleware/cors.rs src/app/middleware/security_headers.rs src/app/middleware/mod.rs tests/security_middleware.rs src/app/tests.rs
git commit -m "feat: harden HTTP security middleware"
```

### Task 6: Replace the Fixed Local Rate Limiter with a Store Contract

**Files:**
- Create: `src/app/middleware/rate_limit.rs`
- Modify: `src/app/middleware/mod.rs`
- Modify: `src/lib.rs`
- Modify: `tests/security_middleware.rs`

- [ ] **Step 1: Add contract tests**

Create a fake store and verify allowed, rejected and store-error outcomes:

```rust
pub trait RateLimitStore: Send + Sync + 'static {
    fn check(
        &self,
        key: RateLimitKey,
        policy: RateLimitPolicy,
        now: Instant,
    ) -> impl Future<Output = Result<RateLimitDecision, RateLimitError>> + Send;
}
```

Tests must assert `RateLimit-*` headers, `Retry-After`, client-address keying and fail-open/fail-closed
behavior.

- [ ] **Step 2: Verify failure**

Run `cargo test --test security_middleware rate_limit_store`.

Expected: missing contract types.

- [ ] **Step 3: Implement policy and decisions**

Define a token-bucket or GCRA policy with explicit capacity and refill rate. Do not retain a bucket
forever: the in-memory store must evict entries that have fully refilled and remain idle for two
policy windows.

Expose key functions for client IP, header/API key and application-provided extension. Default to
resolved `client_addr`.

- [ ] **Step 4: Implement store failure policy**

```rust
pub enum StoreFailurePolicy {
    FailOpen,
    FailClosed,
}
```

Fail-closed returns structured `503 rate_limit_unavailable`, not `429`. Emit an observer event in
the later observability phase.

- [ ] **Step 5: Verify**

Run:

```bash
cargo test --test security_middleware rate_limit
cargo test rate_limit
cargo test
```

Expected: all pass and tests demonstrate bounded idle-entry cleanup.

- [ ] **Step 6: Commit**

```bash
git add src/app/middleware/rate_limit.rs src/app/middleware/mod.rs src/lib.rs tests/security_middleware.rs src/app/tests.rs
git commit -m "feat: add pluggable rate limiting"
```

### Task 7: Publish the `0.4.0` Migration and Release

**Files:**
- Modify: `Cargo.toml`
- Modify: `README.md`
- Modify: `CHANGELOG.md`
- Create: `docs/migrations/0.3-to-0.4.md`
- Create: `docs/production/server.md`
- Create: `docs/production/reverse-proxy.md`
- Create: `examples/production_server.rs`

- [ ] **Step 1: Document migration and defaults**

Include before/after server startup, `remote_addr` to `peer_addr`/`client_addr`, new rate-limit
configuration and middleware ordering. Document every default numeric limit from `ServerConfig`.

- [ ] **Step 2: Add reverse proxy configurations**

Provide Nginx, Caddy and HAProxy examples that overwrite rather than append untrusted forwarded
headers. State the exact trusted CIDRs used by the Rust application.

- [ ] **Step 3: Add a production server example**

Configure trusted proxies, connection limits, security headers, CORS, tracing feature hooks and
signal shutdown. The example must bind to `127.0.0.1:3000` by default and print only application
startup information.

- [ ] **Step 4: Bump version and changelog**

Set `version = "0.4.0"` and add the actual release date under `## [0.4.0]`.

- [ ] **Step 5: Run release gates**

```bash
cargo fmt --check
cargo check --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo test --all-features
cargo test --features tls --test tls_integration
cargo test --test websocket_integration
cargo doc --no-deps --all-features
cargo check --example production_server --all-features
cargo publish --dry-run
```

Expected: all commands exit `0`.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock README.md CHANGELOG.md docs/migrations/0.3-to-0.4.md docs/production/server.md docs/production/reverse-proxy.md examples/production_server.rs
git commit -m "release: prepare rustrest 0.4.0"
```
