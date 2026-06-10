# Production WebSocket Master Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver production-grade standard WebSockets for RustRest, including bounded resources, supervised lifecycle, graceful shutdown, route-scoped rooms, external broker support, and a reproducible 10,000-idle/1,000-active validation profile.

**Architecture:** Preserve the current public WebSocket API while moving transport ownership into a supervised connection driver. A shared runtime owns admission, lifecycle, statistics, and shutdown; `WsHub` shares that registry for route-scoped rooms and bounded fan-out; `WsBroker` adds optional cross-node publication without coupling the core crate to Redis or NATS.

**Tech Stack:** Rust 2024, Tokio bounded channels/watch/notify, Hyper upgrades, tokio-tungstenite 0.29, futures-util, serde/serde_json, optional tracing, cargo-fuzz, real TCP/TLS integration tests.

---

## Execution Order

Execute these plans in order. Do not start a later plan until the previous
plan's final verification gate passes.

1. [Core runtime and lifecycle](2026-06-10-production-websocket-core.md)
2. [Rooms, hub, and external broker](2026-06-10-production-websocket-rooms-broker.md)
3. [Production validation, documentation, and release gate](2026-06-10-production-websocket-validation.md)

The approved design is
[`docs/superpowers/specs/2026-06-10-production-websocket-design.md`](../specs/2026-06-10-production-websocket-design.md).

## Locked Decisions

- Preserve all existing `App`, `Router`, `Request`, `WebSocket`,
  `WebSocketError`, and `WsBroadcast` calls.
- Keep `WebSocketError` exhaustiveness unchanged; new precise failures use
  non-exhaustive `WsError`.
- WebSocket transport remains RFC 6455 over HTTP/1.1 upgrade. This is not the
  Socket.IO protocol.
- All application and control queues are bounded.
- No send or broadcast silently drops a message.
- Existing `recv()` continues to surface Ping, Pong, and Close after mandatory
  protocol processing.
- Room namespaces are normalized, fully-mounted route patterns.
- Direct socket lookup and disconnect are local-process operations.
- Cross-node room broadcast is optional through `WsBroker`; global presence is
  not implied.
- The acceptance load profile is 10,000 idle connections and 1,000 active
  connections per instance.

## File Structure

The final subsystem should have these ownership boundaries:

```text
src/app/websocket.rs                 Public facade and compatibility re-exports
src/app/websocket/config.rs          Config, origin policy, validation
src/app/websocket/error.rs           WsError and stable classifications
src/app/websocket/types.rs           IDs, close info, observations, stats
src/app/websocket/socket.rs          WebSocket, sender, receiver, event helpers
src/app/websocket/driver.rs          Exclusive transport owner and state machine
src/app/websocket/runtime.rs         Registry, admission, shutdown, statistics
src/app/websocket/hub.rs             Rooms, selectors, local fan-out
src/app/websocket/broker.rs          Object-safe broker contract and supervision
src/app/websocket/tests.rs           Pure/unit tests for the subsystem
tests/websocket_integration.rs        Real TCP lifecycle, limits, rooms, broker
tests/tls_integration.rs              Existing HTTPS plus WSS coverage
examples/websocket.rs                 Production-oriented server example
examples/websocket_load.rs            Reproducible load/soak client
fuzz/fuzz_targets/*.rs                Handshake, event JSON, rooms, broker inputs
docs/websocket-production.md          Deployment and operations guide
.github/workflows/ci.yml              Feature matrix and quality gates
```

`src/app/websocket.rs` remains the module entry point so downstream import paths
do not change. Submodules live under `src/app/websocket/`.

## Global Verification Gate

After all three plans:

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

Expected: every command exits 0, existing WebSocket examples compile unchanged,
and no public compatibility fixture fails.

Run the release acceptance profile on a documented Linux reference host:

```bash
cargo build --release --example websocket
cargo run --release --example websocket_load -- \
  --url ws://127.0.0.1:3000/load \
  --idle 10000 \
  --active 1000 \
  --duration-secs 900 \
  --message-bytes 256 \
  --json-out target/ws-load-report.json
```

Expected: 10,000 idle and 1,000 active connections established, zero silent
drops, zero panics, stable post-warm-up memory, and shutdown completes within
the configured deadline. Commit the reference result only after recording OS,
CPU, memory, file-descriptor limit, kernel, and Rust versions.
