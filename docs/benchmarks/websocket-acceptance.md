# WebSocket Production Acceptance

This document maps every acceptance criterion in the approved production
WebSocket design to executable or published evidence.

## Design criteria

| Criterion | Evidence |
| --- | --- |
| Existing calls remain source-compatible | `tests/websocket_api_compat.rs` compiles the original exhaustive `WebSocketError` match, direct APIs, route APIs, split handles, rooms, administration, broker objects, and all accepted handler outputs. `examples/websocket.rs` exercises the documented production API. |
| RustRest-controlled queues and buffers are bounded | `src/app/websocket/socket.rs` creates configured bounded inbound/outbound queues and a fixed-size control queue. `src/app/websocket/config.rs` validates finite capacities and Tungstenite frame/message/write limits. Configuration and backpressure unit tests cover invalid or saturated configurations. |
| No send path silently drops application messages | Backpressure, broadcast-report, and raw-broadcast lag tests require an explicit result. The reference load report requires sent and received counts to match. |
| Heartbeat is independent and detects missing Pong | `websocket_heartbeat_ping_does_not_require_handler_recv`, `websocket_heartbeat_matching_pong_keeps_connection_alive`, `websocket_observer_records_heartbeat_timeout`, and `websocket_tls_heartbeat_keeps_connection_alive`. |
| RFC 6455 closure and final close metadata | Close handshake/code/reason tests, plain/TLS shutdown tests, and the pinned Autobahn suite. |
| Handler panics are isolated | `websocket_observer_records_handler_panic_and_isolates_connection`, `websocket_observer_panic_does_not_break_echo`, and observer panic-isolation unit tests. |
| Plain and TLS shutdown drain registered sockets | Cooperative/uncooperative plain shutdown tests, runtime shutdown rejection tests, and `websocket_tls_shutdown_sends_1001_and_drains_runtime`. |
| Origin, protocol, connection, rate, and size policies are enforced | Handshake/config unit tests plus real-network tests for HTTP 426, invalid keys, duplicate headers, process/route/IP capacity, subprotocol selection, oversized messages, and message-rate close 1008. |
| Metrics and tracing exclude payloads | Runtime stats, tracing metadata, and room observation tests explicitly assert that payload contents are absent. |
| Route-scoped rooms have idempotency, exclusion, dedupe, cleanup, and partial reports | Atomic cleanup/limit unit tests plus route-scope, sender-exclusion, multi-room dedupe, administration, and broadcast-report TCP tests. |
| Two broker nodes deliver once without route leakage | `websocket_broker_two_nodes_delivers_once_and_preserves_route_scope`, origin-publication dedupe, and broker validation/health tests. |
| External brokers require no framework modification | `WsBroker` is object-safe and boxed; the compatibility fixture installs `Arc<dyn WsBroker>`. Failure, recovery, lag, and invalid-publication tests exercise the contract. |
| Unit, TCP, TLS, concurrency, and robustness coverage exists | Unit tests under `src/app/websocket`, real TCP and WSS suites, four cargo-fuzz targets, Miri configuration/room runs, and Autobahn RFC coverage. |
| The 10,000 idle / 1,000 active profile is stable and lossless | `scripts/run-websocket-reference-profile.sh` enforces connection counts, exact echo counts, zero failures/panics, post-300-second RSS limits, and graceful shutdown. Results are in `docs/benchmarks/websocket-reference.md`. |

## Public API and semver audit

- The compatibility fixture covers all legacy registration and socket calls.
- `WebSocketError` retains exactly `Protocol` and `Json` so downstream
  exhaustive matches remain valid.
- Extensible public WebSocket enums and report/snapshot structs are
  `#[non_exhaustive]`; opaque handles/builders keep private fields.
- `WsBroker` remains object-safe and uses boxed futures/streams without an
  async-trait dependency.
- The crate root uses `#![forbid(unsafe_code)]`.
- Rust 1.85.1 compiles all targets and features, matching `rust-version`.

## Operations evidence

`docs/websocket-production.md` documents origin/authentication, bounded
queues, backpressure, rooms, local administration, external brokers, shutdown,
reverse proxies, TLS/WSS, file-descriptor/TCP limits, privacy-safe
observability, failure handling, fuzzing, Autobahn, load testing, and CI.

## Final validation run

The June 12, 2026 acceptance run produced the following results:

- Formatting, default/all-feature checks, all-feature Clippy with warnings
  denied, default/TLS/all-feature tests, doctests, docs, the API fixture, YAML
  parsing, and the Rust 1.85.1 all-target/all-feature build all passed.
- Four parallel 300-second fuzz runs completed without crash artifacts:
  handshake 9,162,299 executions; event JSON 24,951,041; room target
  8,554,118; broker publication 17,348,425.
- Miri passed the three pure configuration unit tests and the atomic room
  membership/room-limit tests. The broad unqualified filter also selected a
  TCP integration test and hit Miri's unsupported macOS `kqueue`; the final
  runs used `--lib` and exact pure-state test names. Room state required
  `-Zmiri-disable-isolation` only for `SystemTime::now`.
- Autobahn 25.10.1 ran 247 configured cases and found zero failed or
  unimplemented behavior records.
- The Linux reference profile established 10,000 idle plus 1,000 active
  connections for 900 seconds, delivered all 67,974,257 echoes, kept
  post-warm-up RSS flat, and shut down in 108 ms.

No custom unsafe or lock-free state machine was introduced, so Loom was not
required; synchronization is built from Tokio channels, semaphores, atomics,
and mutex-protected registries.
