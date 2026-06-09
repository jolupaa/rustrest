# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project

`rustrest` is a minimal HTTP/web framework written in Rust (edition 2024). It provides a small, hand-written routing layer (`App` / `Router` / `Request` / `Response`) on top of **hyper 1.x** for the HTTP transport, served on `tokio`. It is loosely modeled on Express: route handlers may be **synchronous or asynchronous**, routes support **path parameters** (`/users/:id`), routers can be defined per-file and **mounted** under a prefix (including nested), and **middleware** runs in an onion (`next`) model. Treat this as a framework-building project: prefer extending the hand-written abstractions over pulling in a higher-level framework (axum, actix, warp, etc.) unless explicitly asked.

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

Source files:

- `src/main.rs` — entry point (`#[tokio::main]`). Builds the `App`, adds middleware (`app.layer(...)`), registers root routes, mounts routers, then `app.listen(addr).await`.
- `src/app.rs` — the framework core (everything below).
- `src/users.rs`, `src/api.rs` — **example** user-land route modules. Each exposes `pub fn router() -> Router`. `api` mounts `users` (nesting); `main` mounts `api` under `/api`. This is the pattern for organizing routes/sub-routes across files.

Core types (all in `src/app.rs`):
- `App { router: Router, middlewares: Vec<Middleware> }` — owns the root router and the **global** middleware chain. `get/post/put/delete` delegate to the root router; `mount(prefix, router)` adds a sub-router; `layer(mw)` adds global middleware; `listen` runs the server.
- `Router { routes, middlewares }` — a standalone, mountable set of routes with `get/post/put/delete`, `mount` (nest another router), and `layer` (**router-scoped** middleware that applies only to this router's routes). Routes are matched in **registration order** (no specificity ranking).
- `Route { method, pattern: Vec<Segment>, handler, middlewares }` + `enum Segment { Static, Param }` — each route carries the scoped middleware chain baked in at mount time. `parse_pattern` turns `/users/:id` into segments; `match_pattern` matches request segments and captures params; `Router::route` returns the first match (cloned handler + its middleware chain + params).
- `Handler = Arc<dyn Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync>` — `Arc` (not `Box`) so handlers can be cloned into the middleware chain. User handlers are normalized into this via `IntoHandler<Marker>` (the `SyncMarker`/`AsyncMarker` type-parameter trick lets one method accept both sync and async closures; the marker is inferred from the return type).
- `Next = Box<dyn FnOnce(Request) -> Pin<Box<...>> + Send>` and `Middleware = Arc<dyn Fn(Request, Next) -> Pin<Box<...>> + Send + Sync>` — the onion model. `IntoMiddleware` boxes a user `Fn(Request, Next) -> impl Future<Output = Response>`. **Note:** middleware closures must annotate the param as `next: Next` (the type can't be inferred from the body).
- `Request { method, path, query, headers, body, params }` — plus `req.param("id")` and `req.json::<T>()`.
- `Response { status, body, content_type }` — `send` (text/plain), `json` (application/json; failure → 500), `not_found` (404), `bad_request` (400); private `into_hyper`.

Request flow (`App::listen` → `App::handle` → `App::dispatch`): per connection, the tokio stream is wrapped in `TokioIo` and served via `http1::Builder::serve_connection` + a `service_fn`. `handle` builds a `Request` (method/path/query/headers + size-bounded body), then `dispatch` routes it (capturing path params, or a 404 handler if unmatched) and builds the middleware **onion**: innermost = matched handler, then that route's scoped middlewares, then the global `App` middlewares outermost (each group wrapped last-to-first so the first-registered runs outermost within its group). Global middleware runs for **every** request (including 404s); scoped middleware runs only for the routes of the router it was added to.

### Dependencies note
hyper 1.x is low-level, so two companion crates are required and used: `hyper-util` (`TokioIo` adapter) and `http-body-util` (`Full<Bytes>` body, plus `BodyExt`/`Limited` for bounded body collection). `serde` + `serde_json` back `Response::json` and `Request::json`.

### Constraints to know when extending
- `GET`/`POST`/`PUT`/`DELETE` helpers exist on both `App` and `Router` (delegating to a private `add`). Mirror them for other methods.
- Routing matches by path segments with `:name` placeholders. Precedence is **first match in registration order** — register concrete routes (e.g. `/users/me`) before param routes (`/users/:id`). Trailing/duplicate slashes are ignored; matching requires equal segment count. The raw query string is exposed unparsed in `Request::query`.
- `mount` flattens: it prepends the prefix to each sub-route's pattern, **bakes the mounted router's `layer` middlewares into each route** (outermost-first), and concatenates into the parent's `Vec`. Nesting composes; params in a prefix (e.g. `/users/:uid`) are captured too.
- Middleware uses the onion model: call `next(req).await` to continue, or return a `Response` early to short-circuit. `App::layer` is **global**; `Router::layer` is **scoped** to that router's routes (applied when mounted — so a `layer` on a router mounted at `/api` runs only for `/api/*`). There is no per-path middleware without a router.
- The body is buffered fully into a `String` with a **64 KB cap** (`MAX_BODY_BYTES` via `http_body_util::Limited`), UTF-8 lossy; on overflow/error the handler sees an empty body. No streaming (responses are `Full<Bytes>`).
- Handlers and middleware must be `Send + Sync + 'static` (closures may only capture `Send + Sync` data), since the `App` is shared across connection tasks via `Arc`.
