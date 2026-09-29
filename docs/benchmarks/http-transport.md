# HTTP transport benchmark

Measured on 2026-09-29 to compare the transport before and after restoring
HTTP/1.1 persistent connections (with per-request raw-head inspection) and
enabling `TCP_NODELAY` on accepted sockets.

## Setup

- Machine: 32 logical CPUs, Linux 7.2, rustc 1.98.1, release builds. Client
  and server share the machine, so absolute numbers are only comparable
  within this table.
- Server: `examples/bench_server.rs`, no middleware, route `GET /users/:id`
  (`--addr 127.0.0.1:3100`).
- Client: `examples/http_load.rs` (Hyper client), 1 s warm-up + 10 s
  measured, client-perceived latency including any reconnect.
- `base`: commit `4aeea66` (every HTTP/1 connection closed after one
  response, no `TCP_NODELAY`). `new`: this branch.
- Each row is the median of three runs. No run reported errors.

```bash
cargo run --release --example bench_server -- --addr 127.0.0.1:3100 &
cargo run --release --example http_load -- --url http://127.0.0.1:3100/users/42 \
    --mode keepalive --concurrency 64 --duration-secs 10 --warmup-secs 1
# --mode close | h2 (h2 multiplexes 16 streams per connection by default)
```

## Results

| Scenario | base req/s | new req/s | ratio | base p50 | new p50 | base p99 | new p99 | TCP connects (base / new) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| HTTP/1.1 keep-alive client, 1 conn | 44,495 | 123,561 | 2.78× | 19.5 µs | 7.8 µs | 62.6 µs | 10.9 µs | 480,104 / 1 |
| HTTP/1.1 keep-alive client, 64 conns | 197,363 | 726,472 | 3.68× | 306 µs | 75 µs | 683 µs | 319 µs | 2,174,645 / 64 |
| HTTP/1.1 keep-alive client, 256 conns | 202,320 | 905,942 | 4.48× | 620 µs | 253 µs | 1,194 µs | 811 µs | 2,232,202 / 256 |
| HTTP/1.1 new connection per request, 64 | 196,378 | 212,406 | 1.08× | 308 µs | 284 µs | 683 µs | 641 µs | per request |
| HTTP/2 prior knowledge, 64 streams | 120,992 | 341,734 | 2.82× | 151 µs | 182 µs | 597 µs | 326 µs | 4 / 4 |
| HTTP/2 prior knowledge, 256 streams | 382,914 | 971,903 | 2.54× | 200 µs | 247 µs | 40,432 µs | 588 µs | 16 / 16 |

## Interpretation

- **HTTP/1.1 keep-alive.** The base server answered every request with
  `Connection: close`, so keep-alive clients reconnected for each request
  (millions of TCP handshakes per run). Reusing connections gives 2.8–4.5×
  throughput and lower latency. An earlier A/B by the performance review
  measured server CPU at about 16 µs per request before and about 6–7 µs
  after. A variant with keep-alive enabled but *without* the raw-head
  inspector was within run-to-run noise of this branch, so the per-request
  smuggling check costs little.
- **Per-request connections** are unaffected (+8%, within noise).
- **HTTP/2.** Without `TCP_NODELAY`, small multiplexed responses waited for
  the client's delayed ACK: p99 sat at about 40 ms at 256 streams. With it,
  p99 is 0.59 ms and throughput is 2.5–2.8× higher. The median is slightly
  higher because the server now carries far more load.

These figures are a regression reference for this machine, not a
comparison with other frameworks.
