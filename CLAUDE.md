# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`rustrest` is a minimal HTTP/web framework written in Rust (edition 2024). It provides a small, hand-written routing layer (`App` / `Request` / `Response`) on top of **hyper 1.x** for the HTTP transport, served on `tokio`. Route handlers may be **synchronous or asynchronous** (the framework user picks per route), and routes support **path parameters** (`/users/:id`). Treat this as a framework-building project: prefer extending the hand-written abstractions in `src/app.rs` over pulling in a higher-level framework (axum, actix, warp, etc.) unless explicitly asked.

User-facing strings and console output are in Spanish — match that convention when adding routes or messages.

## Commands

- `cargo run` — start the server; listens on `http://127.0.0.1:3000`
- `cargo build` / `cargo build --release` — compile (debug / optimized). Produces the runnable `target/debug/rustrest`; `cargo test` alone does **not** refresh it.
- `cargo check` — fast type-check without producing a binary
- `cargo clippy --all-targets` — lint
- `cargo fmt` — format
- `cargo test` — run the unit tests in `src/app.rs`; single test: `cargo test <name>`

Edition 2024 requires a Rust 1.85+ stable toolchain.

## Architecture

Two source files:

- `src/main.rs` — entry point (`#[tokio::main]`). Constructs an `App`, registers routes with closures (the demos cover sync/async handlers, a `POST` reading the JSON body, and `GET`/`PUT`/`DELETE` with a `:id` path param), then `app.listen(addr).await`.
- `src/app.rs` — the framework core.

Core types (all in `src/app.rs`):
- `App { routes: Vec<Route> }` — routes are matched in **registration order** (not a hash lookup), so register more specific routes before param routes.
- `Route { method, pattern: Vec<Segment>, handler }` + `enum Segment { Static, Param }` — a path pattern is parsed by `parse_pattern` (`/users/:id` → `[Static("users"), Param("id")]`); `match_pattern` compares it against the request's `path_segments`, capturing params. `App::route(method, path)` returns the first matching handler plus captured params.
- `Handler` — internal type: `Box<dyn Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync>`. Every user handler is normalized into this future-returning shape.
- `IntoHandler<Marker>` — the trait that lets `App::get`/`post`/`put`/`delete` accept **both** sync (`Fn(Request) -> Response`) and async (`Fn(Request) -> impl Future<Output = Response>`) handlers. Two blanket impls coexist via the `Marker` type-parameter trick (`SyncMarker` / `AsyncMarker`); the marker is inferred from the closure's return type, so callers never name it. Sync handlers are wrapped in an immediately-ready future.
- `Request { method, path, query, headers, body, params }` — full request data, plus `req.param("id")` (a captured path param) and `req.json::<T>()` (deserialize the JSON body into `T`).
- `Response { status, body, content_type }` — constructors: `Response::send(text)` (text/plain), `Response::json(&value)` (application/json; serialization failure degrades to 500), `Response::not_found()` (404), `Response::bad_request()` (400). The private `into_hyper(self)` converts it to a `hyper::Response<Full<Bytes>>`.

Request flow (`App::listen` → `App::handle`): binds a `tokio::net::TcpListener` and wraps `self` in an `Arc`. For each accepted connection it wraps the stream in `hyper_util::rt::TokioIo`, clones the `Arc`, and spawns a task serving it with `hyper::server::conn::http1::Builder::serve_connection` plus a `service_fn`. `App::handle` is **async**: it reads method / path / query / headers from the borrowed hyper request, matches the route (capturing path params) via `App::route`, buffers the body (size-bounded — see below), builds a `Request`, **awaits** the matched handler's future (or `Response::not_found()`), and converts the result with `Response::into_hyper()`.

### Dependencies note
hyper 1.x is low-level, so two companion crates are required and used here: `hyper-util` (the `TokioIo` adapter bridging tokio's IO to hyper's traits) and `http-body-util` (`Full<Bytes>` response body, plus `BodyExt`/`Limited` for bounded body collection). `serde` + `serde_json` back `Response::json` and `Request::json`.

### Constraints to know when extending
- `GET`/`POST`/`PUT`/`DELETE` have registration helpers; they all delegate to the private `App::add`. Add other methods by mirroring them (they only differ by the method string).
- Routing matches by path segments with `:name` placeholders. Precedence is **first match in registration order** — there is no static-over-dynamic specificity ranking, so register concrete routes (e.g. `/users/me`) before param routes (`/users/:id`). Trailing/duplicate slashes are ignored; matching requires the same segment count. The raw query string is exposed unparsed in `Request::query`.
- The body is buffered fully into a `String` with a **64 KB cap** (`MAX_BODY_BYTES`, enforced via `http_body_util::Limited`) and decoded UTF-8 lossily; on overflow or read error the handler sees an empty body. No streaming (responses are `Full<Bytes>`).
- Handlers must be `Send + Sync + 'static` (closures may only capture `Send + Sync` data), since the `App` is shared across connection tasks via `Arc`.
