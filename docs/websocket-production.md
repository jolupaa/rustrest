# Production WebSockets

## 1. Standard WebSocket vs Socket.IO

RustRest implements RFC 6455 WebSocket transport. It does not implement the
Socket.IO or Engine.IO protocols, fallback transports, acknowledgements, or a
Socket.IO-compatible client handshake. Use browser `WebSocket`,
`tokio-tungstenite`, or another standard WebSocket client.

RustRest provides server-side conveniences comparable to a subset of
Socket.IO: JSON event envelopes, route-scoped rooms, sender exclusion, local
connection administration, and optional multi-node fan-out through
`WsBroker`. These remain application primitives over standard WebSocket.

## 2. Secure handshake and `OriginPolicy`

Configure an explicit origin policy for browser-facing routes. Same-host is a
reasonable default when the WebSocket and web application share an origin:

```rust
let config = WebSocketConfig::new()
    .origin_policy(OriginPolicy::same_host().allow_missing(false));
```

`Origin` is not an authentication mechanism. Non-browser clients can choose
their own value. Use `OriginPolicy::allow([...])` when trusted frontends use
known origins, and reserve `OriginPolicy::any()` for non-browser endpoints or
controlled validation routes.

## 3. Authentication before upgrade

Authenticate and authorize in middleware before the HTTP upgrade. Rejecting
there returns a normal HTTP status and avoids allocating a WebSocket driver.

```rust
app.layer(|req: Request, next: Next| async move {
    if req.path.starts_with("/chat/") && !valid_session(&req) {
        return Response::send("No autorizado").status(401);
    }
    next(req).await
});
```

Do not put long-lived credentials in URL query strings in production; they
commonly appear in proxy and access logs. Prefer secure cookies or an
`Authorization` header where the client platform permits it.

## 4. Configuration defaults and memory budgeting

Set finite message, frame, queue, connection, and time limits. The
[`examples/websocket.rs`](../examples/websocket.rs) example shows a complete
route configuration.

A conservative per-connection queue budget is approximately:

```text
inbound_capacity * expected_inbound_message_bytes
+ outbound_capacity * expected_outbound_message_bytes
+ max_write_buffer_size
+ protocol, task, registry, TLS, and allocator overhead
```

`max_message_size` is a safety ceiling, not a recommended queue element size.
Multiplying every queue slot by the maximum message size gives the worst-case
admission risk. Choose capacities from measured traffic and process memory,
then set process/route/IP connection ceilings accordingly.

## 5. Direct messages, split handles, and backpressure

`WebSocket::split()` gives one receiver and a cloneable sender. Every sender
uses the same bounded outbound queue. `BackpressurePolicy::Wait` applies a
finite `send_timeout`; `Reject` returns capacity immediately; `Disconnect`
initiates Close 1013 for a slow consumer.

```rust
let (mut receiver, sender) = socket.split();
let background = sender.clone();
tokio::spawn(async move {
    if let Err(error) = background.send_event("job:done", &job).await {
        eprintln!("Fallo enviando job:done: {error}");
    }
});
```

Always inspect send errors. Do not use unbounded application queues in front
of the framework's bounded queue.

## 6. Rooms, route scopes, sender exclusion, and reports

Rooms are scoped by the normalized mounted route pattern. Room `general` on
`/chat/:channel` is separate from the same name on
`/admin/chat/:channel`. Applications explicitly join parameter values when
they want those values to define rooms.

```rust
socket.join_many(["general", "equipo-7"]).await?;
let report = socket
    .to_many(["general", "equipo-7"])
    .send_event("chat:message", &message)
    .await?;
```

Socket-created selectors exclude the sender. Hub selectors include every
match unless `.except(id)` is used. Multi-room selection deduplicates
recipients. For local delivery:

```text
matched == enqueued + rejected + disconnected
```

Treat `rejected` or `disconnected` as a partial result. Broker failure returns
`WsBroadcastError::Broker { source, local_report }`, preserving completed
local delivery.

## 7. Local administration and runtime statistics

`WsHub::local_socket(id)` returns a restricted outbound-only handle with route,
protocol, remote address, rooms, open time, and lifecycle metadata.
`disconnect_local` and `WebSocketRuntimeHandle::close` affect only the current
process. Snapshots never contain message payloads, cookies, authorization
headers, or arbitrary request state.

`runtime.stats()` exposes coherent local counters for connections, bytes,
messages, saturation, rooms, broadcasts, broker health, and broker errors.
It does not claim global presence or global room size.

## 8. Multi-node `WsBroker` integration and non-guarantees

Install a broker before serving:

```rust
let hub = WsHub::builder()
    .broker(Arc::new(my_broker))
    .node_id(WsNodeId::new(7))
    .broker_operation_timeout(Duration::from_secs(2))
    .build()?;
app.websocket_hub(hub);
```

Adapters implement the object-safe `WsBroker` trait. Publications contain a
typed target plus text/binary bytes, origin node, and publication ID. The
runtime suppresses origin echo and keeps a bounded deduplication window.

`Published` means the broker accepted the publication; it does not prove that
another node or client received it. Ordering, persistence, replay, and
delivery guarantees are adapter-specific. The built-in `InMemoryWsBroker` is
process-local and intended for tests or small single-process deployments.

## 9. Graceful shutdown and close codes

Use `listen_with_shutdown`, `serve_with_shutdown`, or the TLS equivalent.
Shutdown stops new WebSocket admission, sends Close 1001 with reason
`apagado del servidor`, waits for each configured close timeout, and aborts
remaining drivers before returning.

Use Close 1000 for normal completion, 1008 for policy/authorization failures,
1009 for oversized messages, 1011 for internal handler failures, and 1013 for
temporary overload. Close reasons are limited to 123 UTF-8 bytes.

## 10. Reverse proxy examples for Nginx, Caddy, and HAProxy

Nginx:

```nginx
location /chat/ {
    proxy_pass http://127.0.0.1:3000;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_read_timeout 180s;
}
```

Caddy:

```caddy
example.com {
    reverse_proxy 127.0.0.1:3000
}
```

HAProxy:

```haproxy
backend rustrest
    option http-server-close
    timeout tunnel 180s
    server app 127.0.0.1:3000 check
```

Set proxy tunnel/idle timeouts above the application heartbeat interval plus
Pong allowance. Preserve `Host` when using same-host origin validation.

## 11. TLS/WSS

Terminate TLS at a trusted reverse proxy or enable the `tls` feature and use
`listen_tls`/`serve_tls_with_shutdown`. `config_from_pem` loads a PEM chain and
private key. Use `wss://` from HTTPS pages; browsers block mixed active
content. Automate certificate renewal and validate that the process reload or
restart path drains existing connections.

## 12. OS limits for 10,000 connections

On Linux, start with:

```bash
ulimit -n 65536
```

Match this in systemd (`LimitNOFILE=65536`), containers, the reverse proxy,
and any supervisor. Account for at least one client and one server FD per
connection during local load tests. Verify ephemeral ports, NAT, proxy limits,
RAM, and CPU on the actual host. Apply kernel tuning only from measured
exhaustion, record the previous value, and define a rollback.

## 13. Observability and payload privacy

Install one `WebSocketObserver` for metadata events. Callbacks are panic
isolated but synchronous; move expensive work to a bounded telemetry queue.
Events and tracing include IDs, routes, room names, byte counts, delivery
counts, close metadata, and broker categories. They never include application
payloads. Room names may still be sensitive, so define an application naming
policy before exporting them.

## 14. Failure handling checklist

- Reject invalid origin/authentication before upgrade.
- Set process, route, IP, message-rate, room, and payload limits.
- Use finite queue capacities, send timeout, heartbeat, lifetime, and close timeout.
- Inspect partial broadcast reports and broker errors.
- Alert on saturation, heartbeat timeouts, forced shutdown, broker lag, and invalid publications.
- Keep proxy idle timeout above heartbeat plus Pong timeout.
- Drain on deploy and verify Close 1001 reaches cooperative clients.
- Test reconnect behavior in the application; RustRest does not reconnect clients.
- Budget broker outages independently from local delivery.
- Avoid logging credentials, payloads, and sensitive room identifiers.

## 15. Load, Autobahn, and fuzz commands

```bash
cargo test --all-features
cargo fuzz build handshake
cargo fuzz run handshake -- -max_total_time=30
cargo run --release --example websocket
./scripts/run-autobahn.sh
cargo run --release --example websocket_load -- \
  --idle 100 --active 20 --duration-secs 30 \
  --json-out target/ws-load-smoke.json
```

The reference profile script and measured baseline live in
`scripts/run-websocket-reference-profile.sh` and
`docs/benchmarks/websocket-reference.md`.

The Autobahn gate uses the pinned image
`crossbario/autobahn-testsuite:25.10.1` and writes its HTML report to
`target/autobahn/server/index.html`. The runner executes:

```bash
docker run --rm --network host \
  -v "${PWD}/autobahn:/config:ro" \
  -v "${PWD}/target/autobahn:/reports" \
  crossbario/autobahn-testsuite:25.10.1 \
  wstest -m fuzzingclient -s /config/fuzzingclient.json
```

Protocol cases `9.*` are excluded because load and soak behavior has a
separate acceptance profile. Cases `12.*` and `13.*` cover WebSocket
compression extensions; RustRest does not currently negotiate
`permessage-deflate`. All other cases must report neither `FAILED` nor
`UNIMPLEMENTED`.

On Docker Desktop, where host networking does not expose the host loopback in
the same way as Linux, bind the example to all interfaces and override only the
runner connection URL:

```bash
RUSTREST_ADDR=0.0.0.0:3001 cargo run --release --example websocket
AUTOBAHN_ENDPOINT_URL=http://127.0.0.1:3001/autobahn \
AUTOBAHN_SERVER_URL=ws://host.docker.internal:3001/autobahn \
  ./scripts/run-autobahn.sh
```

The repository CI repeats default/TLS/tracing/brotli feature checks, all-feature
Clippy, rustfmt, all four fuzz target builds, a 100-idle/20-active network
smoke, and the complete non-performance/non-compression Autobahn gate on every
push and pull request. Failed network jobs upload their JSON, HTML, and server
log artifacts.
