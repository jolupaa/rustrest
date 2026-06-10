# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`rustrest` (v0.2) is a minimal HTTP/web framework written in Rust (edition 2024). It provides a small, hand-written routing layer (`App` / `Router` / `Request` / `Response`) on top of **hyper 1.x** for the HTTP transport (HTTP/1.1 + HTTP/2 via hyper-util's `auto::Builder`), served on `tokio`. It is loosely modeled on Express: route handlers may be **synchronous or asynchronous**, routes support **path parameters** (`/users/:id`) and trailing **wildcards** (`*path`), routers can be defined per-file and **mounted** under a prefix (including nested), and **middleware** runs in an onion (`next`) model at global, router, and per-route scope. Treat this as a framework-building project: prefer extending the hand-written abstractions over pulling in a higher-level framework (axum, actix, warp, etc.) unless explicitly asked.

User-facing strings and console output are in Spanish — match that convention when adding routes or messages. `#![forbid(unsafe_code)]` is set; keep it that way.

## Commands

- `cargo run` — start the demo server; listens on `http://127.0.0.1:3000`
- `cargo build` / `cargo build --release` — compile (debug / optimized). Produces the runnable `target/debug/rustrest`; `cargo test` alone does **not** refresh it.
- `cargo check` — fast type-check without producing a binary
- `cargo clippy --all-targets --all-features` — lint (kept warning-free)
- `cargo fmt` — format
- `cargo test` — unit tests (`src/app/tests.rs`) + integration tests (`tests/`); single test: `cargo test <name>`
- `cargo test --all-features` — also runs the `tls`/`tracing`/`brotli`-gated tests (e.g. the HTTPS integration test)

Edition 2024 requires a Rust 1.85+ stable toolchain.

Cargo features (all off by default): `tls` (rustls HTTPS: `app.listen_tls`, `rustrest::tls::config_from_pem`), `tracing` (`middleware::trace()` spans), `brotli` (preferred encoding in `middleware::compression()`).

## Architecture

Source layout:

- `src/main.rs` — demo entry point (`#[tokio::main]`). Builds the `App`, adds middleware, registers routes, mounts routers, then `app.listen(addr).await` (returns `io::Result<()>`).
- `src/users.rs`, `src/api.rs` — **example** user-land route modules. Each exposes `pub fn router() -> Router`; `api` mounts `users`; `main` mounts `api` under `/api`. This is the pattern for organizing sub-routes across files.
- `src/lib.rs` — crate root re-exporting the public API; `src/app.rs` — module wiring + re-exports.
- `src/app/` — the framework core, one concern per module:
  - `server.rs` — `App`, `ServerConfig` (max_body_size → 413, request_timeout → 408, header_read_timeout, `TrailingSlash` policy), `listen`/`serve`(+`_with_shutdown` graceful drain), `handle`/`run_request`/`dispatch`, `routes()`/`print_routes()`, `openapi()`/`serve_docs()`.
  - `router.rs` — `Router`, `Route` (+ `RouteMeta`), `Segment`, parse/match, `RouteHandle` (per-route `.layer()` + `.summary/.description/.tag`), `RouteInfo`, static files (streaming, ETag/Last-Modified/304, Range/206).
  - `trie.rs` — `RouteIndex`, the segment trie behind `Router::route`/`allowed_methods` (built lazily in a `OnceLock`, invalidated by `add`/`mount`).
  - `request.rs` — `Request` + `RequestBuilder`; `response.rs` — `Response` (`BodyKind` bytes/stream/empty), `IntoResponse`, SSE constructors.
  - `handler.rs` — `Handler`/`Next`/`Middleware` types, `IntoHandler`, `IntoMiddleware`, panic guard, 404/405/OPTIONS handlers.
  - `extract.rs` — `FromRequest`: `Json`, `Form`, `Path` (struct or scalar), `Query`, `State`, `Cookies<T>`, `Headers<T>`, `Bytes`, `String`, `Option`/`Result` wrappers.
  - `middleware.rs` — built-ins: `cors()`/`Cors` builder, `gzip()`, `compression[_with_min_size]()`, `etag()`, `rate_limit()`, `timeout()`, `request_id()`, `tracing()`, cfg-`trace()`.
  - `form.rs` (urlencoded + multipart), `cookie.rs` (builder + HMAC `sign_value`/`verify_value`), `session.rs` (in-memory `Sessions`), `error.rs` (`HttpError`), `state.rs`, `sse.rs` (`SseEvent`, comments), `websocket.rs` (`WebSocket`, `WebSocketConfig`, `WsBroadcast`), `openapi.rs`, `testing.rs` (`TestClient`), cfg-`tls.rs`, `tests.rs`.

Core types:
- `Handler = Arc<dyn Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync>` — `Arc` so handlers can be cloned into the middleware chain. User handlers are normalized via `IntoHandler<Marker>` (the `SyncMarker`/`AsyncMarker` type-parameter trick accepts both sync and async closures; handlers may also return `Result<Response, E: IntoHttpError>`). Panics are caught → 500.
- `Next = Box<dyn FnOnce(Request) -> Pin<Box<...>> + Send>` and `Middleware = Arc<dyn Fn(Request, Next) -> Pin<Box<...>> + Send + Sync>` — the onion model. **Note:** middleware closures must annotate the param as `next: Next` (the type can't be inferred from the body).
- `Request` — public `method`/`path`/`raw_query`/`query`/`headers`/`cookies`/`params` fields; body is a **private `Bytes`** behind `bytes()`/`text()`/`json()`/`form()`/`multipart()`; plus `param`, `query`/`query_all`, `header`/`headers_all` (duplicates preserved), `cookie`, `remote_addr()`, `last_event_id()`, `state`, `extract::<T>()`, `session_id()`, websocket helpers. Build test requests with `Request::builder()`.
- `Response` — public `status`/`content_type`/`headers`; body is private (`body_bytes()`/`body_text()` readers). Constructors: `send`, `bytes`, `json`, `stream`, `sse`/`sse_with_heartbeat`, `redirect[_with_status]`, `websocket`, `not_found`/`bad_request`/`internal_server_error`/`from_error` (these carry an `HttpError` so the registered `error_handler` formats them — including 404/405/timeouts).

Request flow (`App::listen` → `handle` → `run_request` → `dispatch`): connections are served by hyper-util's `auto::Builder` (`serve_connection_with_upgrades`, HTTP/1 + HTTP/2 + WebSocket upgrades) under a `GracefulShutdown` watcher; accept errors are retried, never fatal. `handle` builds the `Request` (size-bounded body → 413/400 on failure), `run_request` applies the request timeout, and `dispatch` applies the trailing-slash policy, looks the route up in the trie (or resolves a miss: auto-HEAD from GET, auto-OPTIONS with `Allow`, 405 with `Allow`, else 404), then builds the onion: innermost = handler, then per-route middlewares, then router-scoped, then global `App` middlewares outermost (each group wrapped last-to-first so the first-registered runs outermost within its group). Global middleware runs for **every** request (including 404s and trailing-slash 404/308s).

### Constraints to know when extending
- Route helpers (`get/post/put/delete/patch/options/head/all`) exist on both `App` and `Router` (delegating to a private `add` that returns `RouteHandle`). Mirror both when adding registration surface; same for `websocket`/`ws`/`websocket_with`.
- **Routing precedence is specificity, not registration order**: static segments beat `:params`, which beat trailing `*wildcards` (with backtracking across branches); an exact-method route beats `all()`; remaining ties go to first-registered. The trie index in `trie.rs` must stay in sync with `match_pattern` semantics — params are still captured by re-running `match_pattern` on the winning route. Any new `Router` mutation must call `self.index.take()`.
- Trailing/duplicate slashes are ignored by the segmenter; the `TrailingSlash` policy (Ignore/Strict/Redirect-308) is enforced in `dispatch`, not in matching.
- `mount` flattens: prefix prepended to each sub-route's pattern, the mounted router's `layer` middlewares baked into each route (outermost-first), routes concatenated (metadata carried along). Nesting composes; params in a prefix are captured too.
- Middleware: call `next(req).await` to continue or return a `Response` to short-circuit. `App::layer` is global; `Router::layer` is router-scoped; `RouteHandle::layer` is per-route. Built-ins that touch the body must skip streams (`body_bytes()` is `None`) and status 101.
- Bodies are fully buffered (`Bytes`, default 64 KB via `Limited`); responses can stream (`BodyKind::Stream`), requests cannot.
- Handlers and middleware must be `Send + Sync + 'static` (closures may only capture `Send + Sync` data), since the `App` is shared across connection tasks via `Arc`.
- Tests: TDD per change; unit tests live in `src/app/tests.rs` (in-process via `app.dispatch`/`TestClient`), wire-level tests in `tests/http_integration.rs` (+ `tests/tls_integration.rs` behind `tls`). Keep `cargo clippy --all-targets --all-features` and `cargo fmt --check` clean.

### Design doc
`docs/superpowers/specs/2026-06-09-rustrest-v0.2-design.md` records the v0.2 roadmap (P0/P1/P2), implementation deviations, and 0.1→0.2 migration notes.
