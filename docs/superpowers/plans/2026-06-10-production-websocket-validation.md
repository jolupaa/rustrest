# Production WebSocket Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove and document that the completed WebSocket runtime is operationally suitable for production through secure examples, WSS tests, fuzzing, RFC 6455 conformance, reproducible load/soak tooling, CI gates, and a published reference baseline.

**Architecture:** Validation remains outside the runtime's critical path. Real-network tests cover integration, cargo-fuzz attacks public parsing/state boundaries, Autobahn verifies protocol conformance, and a release-mode client drives the target concurrency profile while emitting machine-readable results and external host measurements.

**Tech Stack:** Rust examples/tests, tokio-tungstenite, rustls feature, cargo-fuzz/libFuzzer, Docker Autobahn Testsuite 25.10.1, GitHub Actions, Linux `/usr/bin/time` and `ps` for reference profiling.

---

## File Map

- Modify `examples/websocket.rs`: secure production-oriented rooms example and
  Autobahn echo endpoint.
- Modify `Cargo.toml`: enable Tokio signal handling for graceful example
  shutdown.
- Create `examples/websocket_load.rs`: load/soak client with JSON report.
- Create `docs/websocket-production.md`: deployment and operations guide.
- Modify `README.md`: concise API and production guide links.
- Modify `tests/tls_integration.rs`: WSS messaging, heartbeat, rooms, shutdown.
- Create `fuzz/Cargo.toml`: cargo-fuzz package.
- Create `fuzz/fuzz_targets/handshake.rs`.
- Create `fuzz/fuzz_targets/event_json.rs`.
- Create `fuzz/fuzz_targets/room_target.rs`.
- Create `fuzz/fuzz_targets/broker_publication.rs`.
- Create `autobahn/fuzzingclient.json`: server conformance configuration.
- Create `scripts/run-autobahn.sh`: pinned repeatable conformance command.
- Create `scripts/check-autobahn-report.py`: deterministic JSON result gate.
- Create `scripts/run-websocket-reference-profile.sh`: load and host metrics.
- Create `.github/workflows/ci.yml`: feature matrix, fuzz build, Autobahn quick
  gate.
- Create `docs/benchmarks/websocket-reference.md`: environment and measured
  baseline after running the profile.

### Task 1: Publish the Complete API Through a Secure Example and Operations Guide

**Files:**
- Modify: `examples/websocket.rs`
- Modify: `Cargo.toml`
- Create: `docs/websocket-production.md`
- Modify: `README.md`
- Modify: `tests/websocket_api_compat.rs`

- [ ] **Step 1: Extend the API compile fixture before changing docs**

Add compile-only calls for every promised public family:

```rust
let hub = WsHub::builder()
    .max_rooms_per_connection(32)
    .max_room_name_bytes(128)
    .broadcast_concurrency(64)
    .broker_operation_timeout(Duration::from_secs(2))
    .build()
    .unwrap();

let config = WebSocketConfig::new()
    .protocols(&["chat"])
    .require_protocol(true)
    .max_message_size(1024 * 1024)
    .max_frame_size(256 * 1024)
    .write_buffer_size(128 * 1024)
    .max_write_buffer_size(2 * 1024 * 1024)
    .inbound_capacity(64)
    .outbound_capacity(64)
    .backpressure_policy(BackpressurePolicy::Wait)
    .send_timeout(Duration::from_secs(5))
    .ping_interval(Duration::from_secs(30))
    .pong_timeout(Duration::from_secs(10))
    .idle_timeout(Duration::from_secs(120))
    .max_connection_lifetime(Duration::from_secs(86_400))
    .close_timeout(Duration::from_secs(5))
    .origin_policy(OriginPolicy::same_host().allow_missing(false))
    .max_connections(2_000)
    .max_connections_per_ip(20)
    .message_rate_limit(100, Duration::from_secs(1))
    .max_rooms_per_connection(32)
    .max_room_name_bytes(128);

config.validate().unwrap();

let relaxed_route = WebSocketConfig::new()
    .disable_ping()
    .disable_idle_timeout()
    .disable_max_connection_lifetime()
    .disable_max_connections_per_ip()
    .disable_message_rate_limit();
relaxed_route.validate().unwrap();
```

Also compile `split`, direct send, rooms, socket selectors, hub selectors,
runtime stats/snapshots/close/shutdown, observer installation, broker trait
objects, and exhaustive matching of the unchanged `WebSocketError`.

- [ ] **Step 2: Run the fixture and fix any API drift**

```bash
cargo test --test websocket_api_compat
```

Expected: PASS. If the approved API and implementation differ, change the
implementation or explicitly amend the approved spec before writing docs; do
not document nonexistent calls.

- [ ] **Step 3: Rewrite the WebSocket example around production defaults**

The example must:

- install `WsHub::local()` and a metadata-only observer;
- set App-level WebSocket defaults with a 12,000 process connection ceiling so
  the reference `/load` route can reach its target;
- configure same-host origin, 1 MiB messages, 64-entry queues, heartbeat/Pong
  timeout, close timeout, connection limits, rate limit, and room limits;
- use authentication middleware before the upgrade (a simple signed or fixed
  demo token, clearly marked as example-only);
- join a room derived explicitly from `req.param` or an initial event;
- split sender/receiver and demonstrate background fan-out;
- handle every send/broadcast result instead of `.ok()`;
- use `listen_with_shutdown` and Ctrl-C only if the Tokio `signal` feature is
  added;
- expose `/autobahn` as a raw text/binary echo route with production extras
  disabled so the conformance client sees only RFC 6455 behavior.
- expose `/load` as a raw echo route with process/route capacity of at least
  12,000 and no per-IP limit, specifically for the reference profile.

Add `"signal"` to the existing Tokio feature list in `Cargo.toml`, and use:

```rust
app.listen_with_shutdown("127.0.0.1:3000", async {
    let _ = tokio::signal::ctrl_c().await;
}).await
```

Before registering routes, set the process ceiling:

```rust
app.websocket_defaults(WebSocketConfig::new().max_connections(12_000));
```

The `/autobahn` handler must be:

```rust
app.websocket_with(
    "/autobahn",
    WebSocketConfig::new()
        .max_message_size(64 * 1024 * 1024)
        .max_frame_size(16 * 1024 * 1024)
        .origin_policy(OriginPolicy::any().allow_missing(true))
        .disable_ping()
        .disable_idle_timeout()
        .disable_max_connection_lifetime()
        .disable_max_connections_per_ip()
        .disable_message_rate_limit(),
    |mut socket| async move {
        while let Some(message) = socket.recv().await? {
            if message.is_text() || message.is_binary() {
                socket.send(message).await?;
            } else if message.is_close() {
                break;
            }
        }
        Ok::<(), WebSocketError>(())
    },
);
```

The `/load` handler must use a 12,000 connection route limit, no per-IP limit,
bounded 64-entry queues, and raw text/binary echo:

```rust
app.websocket_with(
    "/load",
    WebSocketConfig::new()
        .max_connections(12_000)
        .inbound_capacity(64)
        .outbound_capacity(64)
        .max_message_size(1024 * 1024)
        .origin_policy(OriginPolicy::any().allow_missing(true))
        .disable_max_connections_per_ip()
        .disable_message_rate_limit(),
    |mut socket| async move {
        while let Some(message) = socket.recv().await? {
            if message.is_text() || message.is_binary() {
                socket.send(message).await?;
            } else if message.is_close() {
                break;
            }
        }
        Ok::<(), WebSocketError>(())
    },
);
```

- [ ] **Step 4: Write `docs/websocket-production.md`**

Use these exact sections:

1. Standard WebSocket vs Socket.IO
2. Secure handshake and `OriginPolicy`
3. Authentication before upgrade
4. Configuration defaults and memory budgeting
5. Direct messages, split handles, and backpressure
6. Rooms, route scopes, sender exclusion, and reports
7. Local administration and runtime statistics
8. Multi-node `WsBroker` integration and non-guarantees
9. Graceful shutdown and close codes
10. Reverse proxy examples for Nginx, Caddy, and HAProxy
11. TLS/WSS
12. OS limits for 10,000 connections
13. Observability and payload privacy
14. Failure handling checklist
15. Load, Autobahn, and fuzz commands

For Linux, document at minimum `ulimit -n 65536`, matching service-manager
limits, proxy idle timeout greater than heartbeat interval, and a memory budget
formula based on two bounded queues plus configured message/write limits. Do
not prescribe kernel tuning without explaining the workload and rollback.

- [ ] **Step 5: Update README without duplicating the full guide**

Keep the existing introductory examples, add the new API surface summary, a
small rooms example, and link to `docs/websocket-production.md`. Explicitly say
that Socket.IO clients remain incompatible.

- [ ] **Step 6: Verify docs and example compile**

```bash
cargo fmt
cargo check --example websocket
cargo test --test websocket_api_compat
cargo test --doc
```

Expected: PASS.

- [ ] **Step 7: Commit API documentation and example**

```bash
git add Cargo.toml Cargo.lock examples/websocket.rs docs/websocket-production.md README.md tests/websocket_api_compat.rs
git commit -m "docs: publish production websocket API"
```

### Task 2: Complete WSS Integration Coverage

**Files:**
- Modify: `tests/tls_integration.rs`
- Modify: `src/app/tls.rs` only if tests expose a lifecycle defect.

- [ ] **Step 1: Add a reusable trusted TLS test setup**

Extract certificate creation, server config, client connector, and temporary
directory cleanup into a test helper struct whose `Drop` removes generated
files. Preserve the existing HTTPS test.

- [ ] **Step 2: Add WSS messaging and subprotocol test**

Connect a TLS stream with the trusted self-signed certificate, build a client
request with `Sec-WebSocket-Protocol: chat`, and use
`tokio_tungstenite::client_async` over the established rustls stream. Assert
subprotocol response, text echo, binary echo, and clean close.

- [ ] **Step 3: Add WSS heartbeat and shutdown tests**

Assert heartbeat Ping/Pong behavior and Close 1001 during
`serve_tls_with_shutdown`, using finite timeouts around every network await.

- [ ] **Step 4: Run TLS tests**

```bash
cargo test --features tls --test tls_integration
```

Expected: PASS.

- [ ] **Step 5: Commit WSS validation**

```bash
git add tests/tls_integration.rs src/app/tls.rs
git commit -m "test: cover websocket behavior over tls"
```

### Task 3: Add Public-Boundary Fuzz Targets

**Files:**
- Create: `fuzz/Cargo.toml`
- Create: `fuzz/fuzz_targets/handshake.rs`
- Create: `fuzz/fuzz_targets/event_json.rs`
- Create: `fuzz/fuzz_targets/room_target.rs`
- Create: `fuzz/fuzz_targets/broker_publication.rs`
- Modify: `.gitignore`

- [ ] **Step 1: Create the standalone fuzz package**

Use:

```toml
[package]
name = "rustrest-fuzz"
version = "0.0.0"
publish = false
edition = "2024"

[package.metadata]
cargo-fuzz = true

[dependencies]
libfuzzer-sys = "0.4"
serde_json = "1"
tokio = { version = "1", features = ["rt", "sync", "time"] }
rustrest = { path = ".." }

[[bin]]
name = "handshake"
path = "fuzz_targets/handshake.rs"
test = false
doc = false
bench = false

[[bin]]
name = "event_json"
path = "fuzz_targets/event_json.rs"
test = false
doc = false
bench = false

[[bin]]
name = "room_target"
path = "fuzz_targets/room_target.rs"
test = false
doc = false
bench = false

[[bin]]
name = "broker_publication"
path = "fuzz_targets/broker_publication.rs"
test = false
doc = false
bench = false
```

Add `fuzz/artifacts/` and `fuzz/corpus/` to `.gitignore`, except checked-in
seed files if later added intentionally.

- [ ] **Step 2: Fuzz handshake header parsing**

Build a `Request` from arbitrary UTF-8-lossy method/path/header chunks and call
`Response::websocket(&request)` inside the fuzz target. Assert only that it
does not panic; both success and rejection are valid.

The target entry is:

```rust
#![no_main]

use libfuzzer_sys::fuzz_target;
use rustrest::{Request, Response};

fuzz_target!(|data: &[u8]| {
    let text = String::from_utf8_lossy(data);
    let mut parts = text.split('\n');
    let key = parts.next().unwrap_or_default();
    let version = parts.next().unwrap_or_default();
    let origin = parts.next().unwrap_or_default();
    let request = Request::builder()
        .method("GET")
        .path("/ws")
        .header("upgrade", "websocket")
        .header("connection", "Upgrade")
        .header("sec-websocket-key", key)
        .header("sec-websocket-version", version)
        .header("origin", origin)
        .build();
    let _ = Response::websocket(&request);
});
```

- [ ] **Step 3: Fuzz event JSON and room selectors**

`event_json` calls `serde_json::from_slice::<WebSocketEvent<Value>>` and, on
success, serializes it again. `room_target` truncates input to 1 KiB, converts
it with `String::from_utf8_lossy`, and attempts `WsHub::builder().build()`. If
the build succeeds, create `hub.route("/chat/:channel").to(lossy_room)` and
call `send_text("x")` inside a current-thread Tokio runtime. Assert only that
the future completes without panic; an empty delivery report or a
room-validation error are both valid.

- [ ] **Step 4: Fuzz broker publication decoding/validation**

Define a fuzz-only serde envelope containing `route`, `room`, and
`payload: Vec<u8>`. Truncate the input and decoded payload to 64 KiB. On
successful decode, construct `Arc::new(InMemoryWsBroker::new(8))` and attempt
`WsHub::builder().broker(broker).build()`. If the build succeeds, call
`hub.route(envelope.route).to(envelope.room).send_binary(Bytes::from(payload))`
inside a current-thread Tokio runtime. Assert only completion without panic;
decode errors, selector validation errors, broker errors, and empty delivery
reports are valid.

- [ ] **Step 5: Build every fuzz target**

```bash
cargo fuzz build handshake
cargo fuzz build event_json
cargo fuzz build room_target
cargo fuzz build broker_publication
```

Expected: all builds exit 0.

- [ ] **Step 6: Run short smoke fuzzing locally**

```bash
cargo fuzz run handshake -- -max_total_time=30
cargo fuzz run event_json -- -max_total_time=30
cargo fuzz run room_target -- -max_total_time=30
cargo fuzz run broker_publication -- -max_total_time=30
```

Expected: no crash, panic, timeout, or sanitizer finding.

- [ ] **Step 7: Commit fuzz infrastructure**

```bash
git add fuzz .gitignore
git commit -m "test: fuzz websocket public boundaries"
```

### Task 4: Add Autobahn RFC 6455 Conformance Testing

**Files:**
- Create: `autobahn/fuzzingclient.json`
- Create: `scripts/run-autobahn.sh`
- Create: `scripts/check-autobahn-report.py`
- Modify: `examples/websocket.rs`
- Modify: `docs/websocket-production.md`

- [ ] **Step 1: Add the pinned server-test configuration**

Create `autobahn/fuzzingclient.json`:

```json
{
  "outdir": "/reports/server",
  "servers": [
    {
      "agent": "rustrest-production-websocket",
      "url": "ws://127.0.0.1:3000/autobahn"
    }
  ],
  "cases": ["*"],
  "exclude-cases": ["9.*", "12.*", "13.*"],
  "exclude-agent-cases": {}
}
```

Cases 9 are excluded from the protocol gate because the repository has a
separate load profile. Cases 12/13 are compression-extension cases and remain
excluded until `permessage-deflate` is explicitly designed and implemented.

- [ ] **Step 2: Create a repeatable runner**

`scripts/run-autobahn.sh` must use `set -euo pipefail`, create
`target/autobahn`, verify Docker and the server endpoint, then run:

```bash
docker run --rm --network host \
  -v "${PWD}/autobahn:/config:ro" \
  -v "${PWD}/target/autobahn:/reports" \
  crossbario/autobahn-testsuite:25.10.1 \
  wstest -m fuzzingclient -s /config/fuzzingclient.json
```

Afterward, invoke `python3 scripts/check-autobahn-report.py
target/autobahn/server`. The checker recursively reads every JSON file, walks
all objects and arrays, and records each object whose `behavior` is exactly
`FAILED` or `UNIMPLEMENTED`. Print its `case`, `caseId`, or `id` field when
present plus the source filename, and exit 1 when any record exists. Exit 2
when no JSON report exists or a report cannot be decoded. Keep the HTML report
under `target/autobahn/server/index.html`; do not commit generated reports.

- [ ] **Step 3: Run the framework example and Autobahn suite**

Terminal 1:

```bash
cargo run --release --example websocket
```

Terminal 2:

```bash
./scripts/run-autobahn.sh
```

Expected: zero failed/unimplemented non-excluded cases.

- [ ] **Step 4: Fix protocol defects with regression tests first**

For each Autobahn failure, add the smallest reproducing unit or TCP integration
test, verify it fails, implement the driver fix, verify it passes, then rerun
the complete suite. Do not special-case Autobahn agent strings or case IDs.

- [ ] **Step 5: Document the pinned image and exclusions**

Add the exact command, image tag, report location, and exclusion rationale to
`docs/websocket-production.md`.

- [ ] **Step 6: Commit conformance infrastructure and fixes**

```bash
git add autobahn scripts/run-autobahn.sh scripts/check-autobahn-report.py examples/websocket.rs docs/websocket-production.md src tests
git commit -m "test: validate websocket RFC 6455 conformance"
```

### Task 5: Build the 10,000-Idle/1,000-Active Load and Soak Tool

**Files:**
- Create: `examples/websocket_load.rs`
- Create: `scripts/run-websocket-reference-profile.sh`
- Create: `docs/benchmarks/websocket-reference.md`
- Modify: `docs/websocket-production.md`

- [ ] **Step 1: Define the CLI and machine-readable report**

Parse arguments without adding Clap. Required options and defaults:

```rust
struct LoadConfig {
    url: String,
    idle: usize,
    active: usize,
    duration: Duration,
    message_bytes: usize,
    connect_concurrency: usize,
    json_out: PathBuf,
}

struct LoadReport {
    requested_idle: usize,
    requested_active: usize,
    connected_idle: usize,
    connected_active: usize,
    connect_failures: usize,
    sent_messages: u64,
    received_messages: u64,
    send_failures: u64,
    receive_failures: u64,
    unexpected_closes: u64,
    p50_round_trip_micros: u64,
    p95_round_trip_micros: u64,
    p99_round_trip_micros: u64,
    elapsed_millis: u128,
}
```

Defaults: URL `ws://127.0.0.1:3000/load`, idle 10,000, active 1,000,
duration 900 seconds, 256-byte messages, connect concurrency 256, output
`target/ws-load-report.json`.

- [ ] **Step 2: Implement bounded connection establishment**

Use a `Semaphore` with `connect_concurrency`, `JoinSet` for connection tasks,
and one shared shutdown watch channel. Establish all idle plus active clients,
classify failures, and abort the run if fewer than requested connections are
ready after a 120-second establishment deadline.

Idle clients continuously read so Ping/Pong and server closure progress but do
not send application messages. Active clients send one unique fixed-size text
message, await the exact echo, record round-trip latency, then repeat with a
small configurable cadence until shutdown.

- [ ] **Step 3: Make measurement bounded and deterministic**

Store latency samples in a bounded 1,000,000-entry reservoir; after the cap,
sample every Nth message rather than growing memory. Use atomics for counters.
On test end, send normal Close to clients, wait 10 seconds, abort remaining
tasks, sort samples, and write pretty JSON.

Exit nonzero when any requested connection was not established, any silent
message mismatch occurred, any unexpected close occurred, or
`received_messages != sent_messages` after drain.

- [ ] **Step 4: Add the reference profiling script**

The Linux script must:

1. require `ulimit -n` of at least 65,536;
2. print `uname -a`, `rustc -Vv`, CPU, memory, and file-descriptor limit;
3. build the server and load client in release mode;
4. launch the server under `/usr/bin/time -v`;
5. sample server RSS/CPU/open-FD count every 10 seconds to CSV;
6. run the load client with 10,000 idle, 1,000 active, and 900 seconds;
7. signal graceful server shutdown;
8. fail on nonzero load exit, panic text, silent drops, or missed deadline;
9. leave JSON, CSV, server logs, and time output under
   `target/ws-reference/`.

- [ ] **Step 5: Run smoke and reference profiles**

Developer smoke test:

```bash
cargo run --release --example websocket_load -- \
  --idle 100 \
  --active 20 \
  --duration-secs 30 \
  --json-out target/ws-load-smoke.json
```

Reference host:

```bash
./scripts/run-websocket-reference-profile.sh
```

Expected reference result: 10,000 idle and 1,000 active established, zero
message mismatch/silent drop/panic, and shutdown within configured deadlines.
For the 900-second reference run, treat RSS as stable when, after the first
300 seconds, the final sample is no more than 5% above the first stabilized
sample and the stabilized maximum is no more than 10% above the stabilized
minimum.

- [ ] **Step 6: Publish the reference environment and measured results**

Write actual values to `docs/benchmarks/websocket-reference.md`: date,
repository commit, OS/kernel, CPU, RAM, Rust version, limits, config, connected
counts, throughput, percentiles, peak/stable RSS, CPU, FD count, errors, and
shutdown duration. Do not write aspirational numbers; if acceptance fails,
keep the task open and document the measured blocker in the working notes, not
as a successful baseline.

- [ ] **Step 7: Commit load tooling and verified baseline**

```bash
git add examples/websocket_load.rs scripts/run-websocket-reference-profile.sh docs/websocket-production.md docs/benchmarks/websocket-reference.md
git commit -m "test: add websocket load and soak profile"
```

### Task 6: Add CI Quality, Feature, Fuzz-Build, and Conformance Gates

**Files:**
- Create: `.github/workflows/ci.yml`
- Modify: `docs/websocket-production.md`

- [ ] **Step 1: Create the standard Rust matrix**

The workflow triggers on push and pull request, uses stable Rust, caches Cargo,
and has jobs for:

```yaml
strategy:
  matrix:
    features:
      - ""
      - "tls"
      - "tracing"
      - "brotli"
      - "tls tracing brotli"
```

Each matrix entry runs `cargo check --all-targets`, `cargo test`, and
`cargo doc --no-deps` with its feature string. Separate jobs run
`cargo fmt --check` and `cargo clippy --all-targets --all-features -- -D warnings`.
Use `actions/checkout@v4`, `dtolnay/rust-toolchain@stable`, and
`Swatinem/rust-cache@v2`; pin permissions to `contents: read`.

- [ ] **Step 2: Add fuzz target compilation**

On Ubuntu, install nightly and cargo-fuzz, then run `cargo fuzz build` for all
four targets. Do not run indefinite fuzzing in ordinary PR CI.

- [ ] **Step 3: Add a small real-network smoke load job**

Start the release example, wait for `/` to respond, run 100 idle + 20 active
for 20 seconds, and upload the JSON/server log artifacts on failure. Ensure the
server is terminated in an `if: always()` step.

- [ ] **Step 4: Add an Autobahn quick job**

Run the release example and the pinned Autobahn script. Upload
`target/autobahn` as an artifact on failure. Keep the full non-performance,
non-compression case set; the Docker image supplies deterministic conformance
coverage.

- [ ] **Step 5: Validate the workflow locally where possible**

Run every underlying command locally:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo fuzz build handshake
cargo fuzz build event_json
cargo fuzz build room_target
cargo fuzz build broker_publication
```

Expected: PASS. Validate YAML syntax with an available local parser or
`ruby -e 'require "yaml"; YAML.load_file(ARGV[0])' .github/workflows/ci.yml`.

- [ ] **Step 6: Commit CI gates**

```bash
git add .github/workflows/ci.yml docs/websocket-production.md
git commit -m "ci: gate production websocket behavior"
```

### Task 7: Final Production Acceptance Gate

**Files:**
- Modify only files required to resolve failures.

- [ ] **Step 1: Run the complete local quality matrix**

```bash
cargo fmt --check
cargo check --all-targets
cargo check --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo test --features tls
cargo test --all-features
cargo test --doc --all-features
cargo doc --no-deps --all-features
```

Expected: all commands exit 0.

- [ ] **Step 2: Run sanitizing/robustness tools**

```bash
cargo fuzz run handshake -- -max_total_time=300
cargo fuzz run event_json -- -max_total_time=300
cargo fuzz run room_target -- -max_total_time=300
cargo fuzz run broker_publication -- -max_total_time=300
```

Run Miri on pure configuration/room tests when the current nightly supports all
dependencies:

```bash
cargo +nightly miri test websocket_config
cargo +nightly miri test websocket_room
```

Expected: no fuzz crash/sanitizer finding; Miri exits 0 or a documented
toolchain incompatibility is filed and does not get described as a pass.

- [ ] **Step 3: Run Autobahn and archive the summary**

```bash
./scripts/run-autobahn.sh
```

Expected: zero failed/unimplemented non-excluded cases. Record the image tag,
run date, and result summary in `docs/benchmarks/websocket-reference.md`.

- [ ] **Step 4: Run the full reference load profile**

```bash
./scripts/run-websocket-reference-profile.sh
```

Expected: 10,000 idle and 1,000 active established, stable post-warm-up memory,
zero silent drops/panics, and graceful shutdown within deadline.

- [ ] **Step 5: Audit the approved design requirement by requirement**

For every acceptance criterion in
`docs/superpowers/specs/2026-06-10-production-websocket-design.md`, point to a
test, conformance result, load result, or documentation section. Add missing
evidence before claiming completion.

- [ ] **Step 6: Review public API and semver compatibility**

Run the compatibility fixture, inspect `cargo doc`, and verify the existing
WebSocket example calls and exhaustive `WebSocketError` match still compile.
Confirm new public enums expected to evolve are `#[non_exhaustive]`.

- [ ] **Step 7: Commit final acceptance fixes and evidence**

```bash
git add src tests examples docs fuzz autobahn scripts .github Cargo.toml Cargo.lock README.md
git commit -m "feat: complete production websocket validation"
```

Skip the commit only when no files changed after the final evidence run.
