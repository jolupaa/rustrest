# RustRest Observability, Quality and 1.0 Stabilization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver RustRest `0.6.0`, validate the complete framework under production-style failure and load, and stabilize the documented API as `1.0.0`.

**Architecture:** Core runtime events flow through neutral observer and metrics traits. Tracing, OpenTelemetry and Redis are companion adapters, not mandatory core dependencies. CI turns behavior, performance, security and semver expectations into release gates before the final API freeze.

**Tech Stack:** tracing, tracing-core, W3C Trace Context, metrics-style recorder trait, OpenTelemetry companion crate, Redis companion crates, proptest, cargo-fuzz, criterion, cargo-semver-checks, cargo-deny, cargo-audit and GitHub Actions.

---

## File Map

| Path | Responsibility |
| --- | --- |
| `src/app/observe.rs` | neutral server/request/body/panic event hooks |
| `src/app/metrics.rs` | low-cardinality metrics contract and no-op recorder |
| `src/app/middleware/trace_context.rs` | W3C propagation and trace request extensions |
| `src/app/testing/` | cookie-aware client, body tools, SSE and assertions |
| `rustrest-opentelemetry/` | OpenTelemetry observer/metrics adapter |
| `rustrest-session-redis/` | Redis implementation of `SessionStore` |
| `rustrest-rate-limit-redis/` | Redis implementation of `RateLimitStore` |
| `benches/` | routing, middleware, extraction and body benchmarks |
| `fuzz/fuzz_targets/` | HTTP/query/cookie/multipart/forwarded fuzzing |
| `tests/http2_integration.rs` | HTTP/2 protocol behavior |
| `tests/framework_load.rs` | bounded release-mode load smoke |
| `docs/benchmarks/framework-1.0-acceptance.md` | final acceptance evidence |

### Task 1: Add Neutral Runtime Observation and Remove Console Side Effects

**Files:**
- Create: `src/app/observe.rs`
- Modify: `src/app.rs`
- Modify: `src/app/server/connection.rs`
- Modify: `src/app/server/handle.rs`
- Modify: `src/app/handler.rs`
- Modify: `src/app/body.rs`
- Modify: `src/app/response.rs`
- Modify: `src/app/tls.rs`
- Modify: `src/lib.rs`
- Create: `tests/observation.rs`

- [ ] **Step 1: Add failing observer tests**

Create a recording observer and assert ordered events for success, handler error, panic, body error,
accept error and forced shutdown:

```rust
#[derive(Default)]
struct RecordingObserver(Mutex<Vec<ObservedKind>>);

impl ServerObserver for RecordingObserver {
    fn on_event(&self, event: ServerEvent<'_>) {
        self.0.lock().unwrap().push(event.kind());
    }
}

#[tokio::test]
async fn observer_records_request_without_payload_or_secret_headers() {
    let observer = Arc::new(RecordingObserver::default());
    let mut app = App::new();
    app.observer(observer.clone());
    app.get("/users/:id", |_req: Request| Response::send("secret-body"));

    TestClient::new(app)
        .get("/users/42?token=secret")
        .header("authorization", "Bearer secret")
        .send().await;

    let events = observer.events();
    assert!(events.iter().any(|event| event.route() == Some("/users/:id")));
    assert!(events.iter().all(|event| !format!("{event:?}").contains("secret")));
}
```

- [ ] **Step 2: Verify failure**

Run `cargo test --test observation`.

Expected: missing observer API and existing runtime paths still print directly.

- [ ] **Step 3: Define event contracts**

Create a non-exhaustive event enum with borrowed metadata:

```rust
#[non_exhaustive]
pub enum ServerEvent<'a> {
    AcceptError(AcceptErrorEvent<'a>),
    ConnectionOpened(ConnectionEvent),
    ConnectionClosed(ConnectionClosedEvent),
    RequestStarted(RequestStartedEvent<'a>),
    RequestFinished(RequestFinishedEvent<'a>),
    BodyFailed(BodyFailedEvent<'a>),
    HandlerPanicked(PanicEvent<'a>),
    ShutdownStarted,
    ShutdownForced { remaining_connections: u64 },
}

pub trait ServerObserver: Send + Sync + 'static {
    fn on_event(&self, event: ServerEvent<'_>);
}
```

Use stable error categories and low-cardinality matched route patterns. Never include body bytes,
query values, cookies, authorization values or arbitrary panic payload formatting.

- [ ] **Step 4: Isolate observer failures**

Wrap observer calls with `catch_unwind`. A panicking observer must be disabled atomically and must
not affect request handling. Emit one fallback diagnostic through a configurable bootstrap logger;
the default is silent.

- [ ] **Step 5: Replace every framework print**

Remove direct `println!`/`eprintln!` from `src/app/**`. Startup address printing moves to examples and
application binaries. Accept, TLS, body, handler, broker and shutdown failures call the observer.
Keep no-op observation as the default.

- [ ] **Step 6: Verify absence of console calls**

Run:

```bash
rg -n 'println!|eprintln!' src/app
cargo test --test observation
cargo test --all-features
```

Expected: `rg` returns no matches and all tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/app/observe.rs src/app.rs src/app/server src/app/handler.rs src/app/body.rs src/app/response.rs src/app/tls.rs src/lib.rs tests/observation.rs examples src/main.rs
git commit -m "feat: expose structured runtime observation"
```

### Task 2: Add W3C Trace Context and the OpenTelemetry Adapter

**Files:**
- Create: `src/app/middleware/trace_context.rs`
- Modify: `src/app/middleware/mod.rs`
- Modify: `src/app/request.rs`
- Modify: `src/app/observe.rs`
- Modify: `Cargo.toml`
- Create: `rustrest-opentelemetry/Cargo.toml`
- Create: `rustrest-opentelemetry/src/lib.rs`
- Create: `rustrest-opentelemetry/tests/propagation.rs`

- [ ] **Step 1: Add W3C parser tests**

Test valid version `00`, invalid all-zero IDs, invalid lengths/hex, future versions, tracestate
length/member constraints and response propagation. Assert untrusted malformed input creates a new
trace rather than failing the request.

- [ ] **Step 2: Verify failure**

Run `cargo test trace_context`.

Expected: trace context types do not exist.

- [ ] **Step 3: Implement dependency-light trace context**

Define:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraceId([u8; 16]);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpanId([u8; 8]);

#[derive(Clone, Debug)]
pub struct TraceContext {
    pub trace_id: TraceId,
    pub parent_span_id: Option<SpanId>,
    pub span_id: SpanId,
    pub sampled: bool,
    pub tracestate: Option<HeaderValue>,
}
```

Generate IDs with OS randomness. Insert `TraceContext` into request extensions before routing and
make it available to observers and response middleware.

- [ ] **Step 4: Upgrade tracing integration**

Emit a request span using method, matched route, HTTP version, client address class, trace/span ID
and sampling decision. Record status class, latency, request bytes, response bytes and error code on
completion. Do not use raw path as a metric label.

- [ ] **Step 5: Create `rustrest-opentelemetry`**

Add the crate to the workspace. Implement a `ServerObserver` that creates OpenTelemetry spans and a
propagator adapter mapping RustRest `TraceContext` to OpenTelemetry context. Keep all OpenTelemetry
dependencies out of the root crate.

- [ ] **Step 6: Verify**

Run:

```bash
cargo test trace_context
cargo test -p rustrest-opentelemetry
cargo test --workspace --all-features
```

Expected: propagation tests pass and root default dependency tree contains no OpenTelemetry crates.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock src/app/middleware/trace_context.rs src/app/middleware/mod.rs src/app/request.rs src/app/observe.rs rustrest-opentelemetry
git commit -m "feat: propagate W3C trace context"
```

### Task 3: Add a Low-Cardinality Metrics Contract

**Files:**
- Create: `src/app/metrics.rs`
- Modify: `src/app.rs`
- Modify: `src/app/server/connection.rs`
- Modify: `src/app/body.rs`
- Modify: `src/app/middleware/rate_limit.rs`
- Modify: `src/app/sse.rs`
- Modify: `src/app/websocket/runtime.rs`
- Modify: `src/lib.rs`
- Create: `tests/metrics.rs`

- [ ] **Step 1: Add recorder tests**

Create a recording implementation and drive HTTP success, 404, timeout, body overflow, rate-limit,
SSE and WebSocket paths. Assert metric names and bounded labels:

```rust
assert_counter("rustrest_http_requests_total", &[('method', "GET"), ('route', "/users/:id"), ('status_class', "2xx")], 1);
assert_histogram_count("rustrest_http_request_duration_seconds", 1);
assert_gauge("rustrest_connections_active", 0.0);
```

No metric may label raw user ID, query string, room name, error message or client IP by default.

- [ ] **Step 2: Verify failure**

Run `cargo test --test metrics`.

Expected: missing metrics registration API.

- [ ] **Step 3: Define recorder primitives**

```rust
pub trait MetricsRecorder: Send + Sync + 'static {
    fn increment_counter(&self, name: &'static str, labels: &[MetricLabel], value: u64);
    fn set_gauge(&self, name: &'static str, labels: &[MetricLabel], value: f64);
    fn record_histogram(&self, name: &'static str, labels: &[MetricLabel], value: f64);
}

#[derive(Clone)]
pub struct MetricLabel {
    pub key: &'static str,
    pub value: MetricValue,
}
```

Use enums or validated static/owned values for labels. The default recorder is a zero-cost no-op
behind an `Option<Arc<dyn MetricsRecorder>>` check.

- [ ] **Step 4: Instrument lifecycle points**

Record connection admission, active requests, latency, status class, body bytes/failures,
compression, timeout, rate limit, session store errors, SSE and WebSocket aggregate stats. Document
every metric name and unit in `docs/production/metrics.md`.

- [ ] **Step 5: Adapt OpenTelemetry metrics**

Implement `MetricsRecorder` in `rustrest-opentelemetry` using OpenTelemetry instruments. Cache
instruments by static metric name; do not create one per request.

- [ ] **Step 6: Verify**

Run:

```bash
cargo test --test metrics
cargo test -p rustrest-opentelemetry
cargo test --workspace --all-features
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/app/metrics.rs src/app.rs src/app/server src/app/body.rs src/app/middleware/rate_limit.rs src/app/sse.rs src/app/websocket/runtime.rs src/lib.rs rustrest-opentelemetry docs/production/metrics.md tests/metrics.rs
git commit -m "feat: expose framework metrics"
```

### Task 4: Expand the Test Client and Real-Network Harness

**Files:**
- Create: `src/app/testing/mod.rs`
- Create: `src/app/testing/client.rs`
- Create: `src/app/testing/request.rs`
- Create: `src/app/testing/response.rs`
- Create: `src/app/testing/server.rs`
- Move from: `src/app/testing.rs`
- Modify: `src/app.rs`
- Modify: `src/lib.rs`
- Create: `tests/test_client_api.rs`

- [ ] **Step 1: Add public test-helper tests**

Exercise this API:

```rust
let client = TestClient::new(app)
    .with_cookie_store()
    .default_header("x-test", "true")
    .remote_addr("203.0.113.5:4000".parse().unwrap())
    .secure(true);

client.post("/users")
    .json(&CreateUser { name: "Ada".into() })
    .send().await
    .assert_status(201)
    .assert_header("content-type", "application/json")
    .json::<User>().await.unwrap();
```

Also test URL-encoded form, multipart, streaming body, response collection limit, SSE event reads,
forwarded/HTTP version simulation and cookie expiry.

- [ ] **Step 2: Verify failure**

Run `cargo test --test test_client_api`.

Expected: missing builders, cookie store and assertions.

- [ ] **Step 3: Split testing modules and add cookie persistence**

Move existing APIs without changing import paths. Apply response `Set-Cookie` deltas to a client
jar with domain/path/secure/expiry matching; attach matching cookies to later requests.

- [ ] **Step 4: Make response collection fallible and bounded**

Expose:

```rust
impl TestResponse {
    pub async fn bytes(self, limit: usize) -> Result<Bytes, TestBodyError>;
    pub async fn text(self, limit: usize) -> Result<String, TestBodyError>;
    pub async fn json<T: DeserializeOwned>(self) -> Result<T, TestBodyError>;
    pub fn sse(self) -> TestSseStream;
}
```

Never return an empty body when stream collection fails.

- [ ] **Step 5: Add a real-network harness**

`TestServer::spawn(app).await` binds port zero and returns `base_url`, address, `ServerHandle` and
helpers for raw TCP/TLS/HTTP2 tests. Drop initiates bounded shutdown; tests should still call
`shutdown().await` explicitly to assert results.

- [ ] **Step 6: Verify docs and tests**

Run:

```bash
cargo test --test test_client_api
cargo test testing
cargo test --doc
cargo test --all-features
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/app/testing src/app.rs src/lib.rs tests/test_client_api.rs README.md
git rm src/app/testing.rs
git commit -m "feat: expand framework testing tools"
```

### Task 5: Add Redis Companion Stores

**Files:**
- Modify: `Cargo.toml`
- Create: `rustrest-session-redis/Cargo.toml`
- Create: `rustrest-session-redis/src/lib.rs`
- Create: `rustrest-session-redis/tests/redis.rs`
- Create: `rustrest-rate-limit-redis/Cargo.toml`
- Create: `rustrest-rate-limit-redis/src/lib.rs`
- Create: `rustrest-rate-limit-redis/tests/redis.rs`
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Add Redis integration tests**

Use a CI Redis service and a unique key prefix per test. Session tests cover save/load/delete/touch,
TTL, revision conflicts and serialization size. Rate-limit tests cover atomic decisions across two
store instances and expiration cleanup.

- [ ] **Step 2: Implement `rustrest-session-redis`**

Use a connection manager and Lua script for compare-and-save revision semantics. Store a versioned
JSON or MessagePack envelope and Redis TTL equal to the record expiry. Prefix keys with a validated
config value.

Expose:

```rust
pub struct RedisSessionStore {
    manager: ConnectionManager,
    key_prefix: String,
}
```

- [ ] **Step 3: Implement `rustrest-rate-limit-redis`**

Use one Lua script to load state, calculate the GCRA/token-bucket decision, update TTL and return
remaining/reset data atomically. Use Redis server time inside the script to avoid application clock
skew.

- [ ] **Step 4: Add CI Redis service**

Add a job with:

```yaml
services:
  redis:
    image: redis:7-alpine
    ports:
      - 6379:6379
    options: >-
      --health-cmd "redis-cli ping"
      --health-interval 5s
      --health-timeout 3s
      --health-retries 10
```

Run both crates' integration tests with `REDIS_URL=redis://127.0.0.1:6379`.

- [ ] **Step 5: Verify**

Run locally against an available Redis:

```bash
REDIS_URL=redis://127.0.0.1:6379 cargo test -p rustrest-session-redis
REDIS_URL=redis://127.0.0.1:6379 cargo test -p rustrest-rate-limit-redis
cargo test --workspace --all-features --no-run
```

Expected: Redis tests pass and the full workspace compiles.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock rustrest-session-redis rustrest-rate-limit-redis .github/workflows/ci.yml
git commit -m "feat: add Redis state adapters"
```

### Task 6: Expand Fuzzing, Properties and Protocol Tests

**Files:**
- Modify: `fuzz/Cargo.toml`
- Create: `fuzz/fuzz_targets/query.rs`
- Create: `fuzz/fuzz_targets/cookie.rs`
- Create: `fuzz/fuzz_targets/multipart.rs`
- Create: `fuzz/fuzz_targets/forwarded.rs`
- Create: `fuzz/fuzz_targets/route_pattern.rs`
- Create: `tests/http2_integration.rs`
- Create: `tests/property_routing.rs`
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Add fuzz harnesses**

Each target must call only public or deliberately fuzz-exposed parser APIs and assert no panic,
unbounded allocation or invalid accepted state. Cap fuzzer input at 256 KiB for multipart and 16 KiB
for header/query parsers.

- [ ] **Step 2: Add route property tests**

Using `proptest`, generate valid static/parameter/wildcard patterns and paths. Assert:

- static routes outrank params;
- params outrank wildcard;
- URL generation followed by matching preserves parameter values;
- registration validation rejects duplicate names and non-terminal wildcards;
- lookup results do not depend on unrelated route insertion order.

- [ ] **Step 3: Add HTTP/2 protocol tests**

Use `hyper` client connection APIs against `TestServer`. Cover concurrent streams, request/response
streaming, stream reset cancellation, graceful GOAWAY, configured max streams and TLS ALPN.

- [ ] **Step 4: Add deterministic fuzz smoke to CI**

Build every target on nightly and run each parser target for a short fixed duration or fixed corpus
iteration budget. Preserve the longer WebSocket Autobahn/load jobs.

- [ ] **Step 5: Verify**

```bash
cargo test --test property_routing
cargo test --test http2_integration
cargo fuzz build
```

Expected: tests pass and every fuzz target builds.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock fuzz tests/http2_integration.rs tests/property_routing.rs .github/workflows/ci.yml
git commit -m "test: expand HTTP protocol and fuzz coverage"
```

### Task 7: Add Benchmarks and a Framework Load Gate

**Files:**
- Modify: `Cargo.toml`
- Create: `benches/routing.rs`
- Create: `benches/middleware.rs`
- Create: `benches/extraction.rs`
- Create: `benches/body.rs`
- Create: `examples/framework_load.rs`
- Create: `tests/framework_load.rs`
- Create: `scripts/run-framework-reference-profile.sh`
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Add Criterion benchmarks**

Benchmark:

- route lookup with 10, 100, 1,000 and 10,000 routes;
- 0, 1, 5 and 10 no-op middleware layers;
- path/query/JSON extraction at representative sizes;
- request body streaming and buffered collection;
- static file 4 KiB and 1 MiB responses.

Use `black_box`, fixed Tokio runtime setup and separate benchmark groups so regressions are
attributable.

- [ ] **Step 2: Add a release-mode load client**

`examples/framework_load.rs` must support concurrency, duration, endpoint mix and JSON report. It
records successful requests, status errors, transport errors, bytes, throughput and p50/p95/p99
latency without adding an external benchmark binary dependency.

- [ ] **Step 3: Define reference thresholds**

`scripts/run-framework-reference-profile.sh` builds release examples, starts the server, runs the
load client and fails when:

- transport/status error rate exceeds `0.1%`;
- p99 health-route latency exceeds the committed reference threshold by more than `25%`;
- process RSS exceeds the documented ceiling for the selected concurrency;
- active connection/request gauges do not return to zero after drain.

Store reference machine, OS, CPU, Rust version and command in
`docs/benchmarks/framework-reference.md`.

- [ ] **Step 4: Add a smaller CI smoke**

Run 100 concurrent clients for 20 seconds against health, JSON and streamed response endpoints.
Upload report/server logs on failure. Keep reference-profile enforcement manual or scheduled to
avoid noisy shared-runner comparisons.

- [ ] **Step 5: Verify**

```bash
cargo bench --no-run
cargo build --release --example framework_load
cargo test --test framework_load
```

Expected: benchmark/load binaries build and the bounded integration smoke passes.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock benches examples/framework_load.rs tests/framework_load.rs scripts/run-framework-reference-profile.sh docs/benchmarks/framework-reference.md .github/workflows/ci.yml
git commit -m "perf: add framework benchmarks and load gate"
```

### Task 8: Add Security, Documentation and Semver CI Gates

**Files:**
- Create: `deny.toml`
- Create: `.cargo/semver-checks.toml`
- Modify: `.github/workflows/ci.yml`
- Modify: `Cargo.toml`
- Create: `SECURITY.md`
- Create: `docs/security-model.md`
- Create: `docs/api-stability.md`

- [ ] **Step 1: Configure dependency policy**

`deny.toml` must deny known advisories, unknown registries/git sources, copyleft licenses not
approved by the project and duplicate cryptography/runtime crates unless explicitly documented.
Allow MIT, Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, Unicode-3.0 and Zlib.

- [ ] **Step 2: Add security docs**

`SECURITY.md` defines supported versions, private reporting channel and response expectations.
`docs/security-model.md` documents trust boundaries, proxy headers, cookies/sessions, body limits,
filesystem serving, observability redaction, panic isolation and explicit non-goals.

- [ ] **Step 3: Add CI jobs**

Add jobs for:

```bash
cargo audit
cargo deny check
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
cargo semver-checks check-release
cargo test --workspace --doc --all-features
```

Semver checks compare against the latest released baseline and may only be waived by a committed
migration note plus a deliberate major/pre-1.0 minor version change.

- [ ] **Step 4: Verify locally**

Run the commands above. Expected: all exit `0` with no ignored advisory lacking an owner and expiry
date.

- [ ] **Step 5: Commit**

```bash
git add deny.toml .cargo/semver-checks.toml .github/workflows/ci.yml Cargo.toml Cargo.lock SECURITY.md docs/security-model.md docs/api-stability.md
git commit -m "ci: enforce security and API compatibility"
```

### Task 9: Finalize Features and Publish `0.6.0`

**Files:**
- Modify: `Cargo.toml`
- Modify: `README.md`
- Modify: `CHANGELOG.md`
- Create: `docs/migrations/0.5-to-0.6.md`
- Create: `docs/features.md`
- Create: `docs/production/observability.md`

- [ ] **Step 1: Make feature ownership explicit**

Set the intended final feature graph:

```toml
[features]
default = []
compression = ["dep:flate2"]
brotli = ["compression", "dep:brotli"]
multipart = ["dep:multer"]
static-files = ["dep:mime_guess"]
sse = []
websocket = ["dep:base64", "dep:sha1", "dep:tokio-tungstenite"]
openapi = []
openapi-ui = ["openapi"]
sessions = ["dep:hmac", "dep:sha2", "dep:chacha20poly1305"]
tls = ["dep:tokio-rustls", "dep:rustls-pemfile"]
tracing = ["dep:tracing"]
metrics = []
macros = ["dep:rustrest-macros", "openapi"]
full = ["compression", "brotli", "multipart", "static-files", "sse", "websocket", "openapi-ui", "sessions", "tls", "tracing", "metrics", "macros"]
```

Mark dependencies optional where the feature graph requires it. Verify default builds contain only
the HTTP core dependencies.

- [ ] **Step 2: Add feature combination tests**

CI must test default, each individual feature, `full`, `--no-default-features` and representative
pairs that share response/server code. Use `cargo hack` if it remains compatible with MSRV and CI
time; otherwise maintain an explicit matrix.

- [ ] **Step 3: Document the default-feature break**

The migration guide lists exact features users need to add for capabilities that were previously
always compiled. `docs/features.md` includes dependency purpose and API enabled by every feature.

- [ ] **Step 4: Bump and verify `0.6.0`**

Set root and companion versions consistently, update changelog and run:

```bash
cargo fmt --check
cargo check --workspace --all-targets --all-features
cargo check -p rustrest --no-default-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
cargo deny check
cargo audit
cargo bench --no-run
```

Expected: all exit `0`.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock README.md CHANGELOG.md docs/migrations/0.5-to-0.6.md docs/features.md docs/production/observability.md .github/workflows/ci.yml
git commit -m "release: prepare rustrest 0.6.0"
```

### Task 10: Stabilize and Release `1.0.0`

**Files:**
- Modify: all public API modules under `src/`
- Modify: all workspace `Cargo.toml` files
- Modify: `README.md`
- Modify: `CHANGELOG.md`
- Create: `docs/migrations/0.6-to-1.0.md`
- Create: `docs/benchmarks/framework-1.0-acceptance.md`
- Create: `docs/production/deployment-checklist.md`
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Audit the public API**

Generate rustdoc JSON or use `cargo public-api` for every crate. For each public type or method,
either document and stabilize it, hide it, or mark it `#[doc(hidden)]` with an explicit unstable
module contract. Add `#[non_exhaustive]` to public event/config enums expected to grow.

- [ ] **Step 2: Run behavior review against the design**

Check every requirement in
`docs/superpowers/specs/2026-06-13-production-framework-design.md`. Record the implementing tests and
commands in `docs/benchmarks/framework-1.0-acceptance.md`. A requirement without evidence blocks the
release candidate.

- [ ] **Step 3: Execute the complete acceptance matrix**

Run:

```bash
cargo fmt --check
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
cargo semver-checks check-release
cargo deny check
cargo audit
cargo bench --no-run
./scripts/run-framework-reference-profile.sh
./scripts/run-websocket-reference-profile.sh
./scripts/run-autobahn.sh
```

Run Redis integration tests against Redis 7 and the documented minimum supported Redis version.
Expected: every command and acceptance threshold passes.

- [ ] **Step 4: Perform a release-candidate soak**

Run the HTTP and WebSocket reference profiles for at least one hour with the production feature set.
Record CPU, RSS, throughput, p95/p99 latency, transport errors, rejected admissions, active-resource
return-to-zero and graceful shutdown duration. Any leak, panic or protocol error blocks release.

- [ ] **Step 5: Complete migration and deployment docs**

The migration guide covers every deprecation removal since `0.6`. The deployment checklist includes
body/upload limits, connection limits, trusted proxies, TLS, session/rate-limit stores, security
headers, observability, shutdown budget, OS file descriptors and backup/failure behavior.

- [ ] **Step 6: Bump versions and validate packages**

Set published crates to `1.0.0`, update inter-crate dependency versions, and run dry-run publish in
dependency order:

```bash
cargo publish -p rustrest-macros --dry-run
cargo publish -p rustrest --dry-run
cargo publish -p rustrest-opentelemetry --dry-run
cargo publish -p rustrest-session-redis --dry-run
cargo publish -p rustrest-rate-limit-redis --dry-run
```

Expected: every package passes verification.

- [ ] **Step 7: Commit the release candidate**

```bash
git add Cargo.toml Cargo.lock rustrest-macros/Cargo.toml rustrest-opentelemetry/Cargo.toml rustrest-session-redis/Cargo.toml rustrest-rate-limit-redis/Cargo.toml src rustrest-macros/src rustrest-opentelemetry/src rustrest-session-redis/src rustrest-rate-limit-redis/src tests README.md CHANGELOG.md docs/migrations/0.6-to-1.0.md docs/benchmarks/framework-1.0-acceptance.md docs/production/deployment-checklist.md .github/workflows/ci.yml
git commit -m "release: prepare rustrest 1.0.0"
```

Do not create the final tag until CI, the soak report and package dry-runs all pass on the exact
commit.
