# RustRest

RustRest is a minimal Express-style HTTP framework for Rust, built on top of `hyper` 1.x and `tokio`.

The goal is to provide a small, direct, easy-to-understand API for building HTTP servers and APIs without hiding the transport layer completely. RustRest includes routes, mountable routers, onion-style middleware, typed extractors, shared state, JSON responses, static files, SSE, cookies, redirects, and WebSocket routes.

> Status: `0.3.0`. The API is still evolving. It is best suited for learning, prototyping, and controlled framework development.

## Features

- HTTP/1.1 and HTTP/2 server built on `hyper` 1.x and `tokio`.
- Synchronous and asynchronous handlers.
- Route helpers for `GET`, `POST`, `PUT`, `DELETE`, `PATCH`, `OPTIONS`, and `HEAD`.
- Trie-indexed, non-recursive routing whose lookup work scales with decoded
  path length plus compatible trie branches/candidates explored (worst case
  bounded by the index size), with static > `:params` > `*wildcards`.
- Validated route registration with named routes (`url_for`), strict percent-decoded params, and optional host constraints.
- Automatic `405 Method Not Allowed` (+`Allow`), auto-`HEAD` from `GET`, and auto-`OPTIONS`.
- Mountable `Router` with nested prefixes.
- Route parameters with `:id` and wildcards with `*path`.
- Route introspection (`app.routes()` / `app.print_routes()`) and a configurable trailing-slash policy (ignore/strict/308 redirect).
- OpenAPI 3.0 generation (`app.openapi(...)`) with per-route `.summary/.description/.tag` metadata, plus a Swagger UI route (`app.serve_docs(...)`).
- Global, router-scoped, and per-route onion middleware with `next`.
- Router guards; app and router fallbacks.
- Graceful shutdown (`listen_with_shutdown` / `serve_with_shutdown`), bounded
  connection admission, and a panic-proof accept loop.
- Configurable body, request-target, query, header, connection, and timeout limits.
- Streaming, binary-safe request bodies with async `bytes`, strict `text`, JSON, form, and multipart helpers.
- Client address via `req.remote_addr()`; duplicate headers via `req.headers_all()`.
- Parsed query strings; request and response cookies (plus a `Cookie` builder with `SameSite`/`Secure`/`Max-Age`).
- Signed values (HMAC-SHA256) and a bounded, expiring in-memory `Sessions` middleware.
- `Result<Response, HttpError>` handlers and a global error handler that also formats 404/405.
- Typed shared state.
- Async extractors: `Json<T>`, `Form<T>`, `Path<T>` (structs or scalars),
  `Query<T>`, `State<T>`, `Cookies<T>`, `Headers<T>`, `MatchedPath`,
  `TypedHeader<T>`, `Bytes`, `String`, plus absence-aware `Option` and
  rejection-preserving `Result` wrappers.
- Capability-rooted static files with streaming bodies, weak
  `ETag`/`Last-Modified` (304), and `Range` (206) support.
- Response streaming and Server-Sent Events, with a heartbeat helper (`Response::sse_with_heartbeat`) and `req.last_event_id()` for resumption.
- WebSocket routes with frame send/receive helpers and `{ "event": ..., "data": ... }` JSON envelopes.
- `WebSocketConfig` for subprotocol negotiation, incoming message size limits, and automatic keepalive pings; `WsBroadcast` for fan-out to many sockets.
- Built-in middleware: configurable `Cors` (with preflight), compression negotiation (gzip/deflate, optional brotli), per-IP rate limiting (429 + `Retry-After`), per-route timeouts (408), `ETag`/conditional GET (304), request id, gzip, and tracing.
- An in-process `TestClient` and public `Request::builder()` for testing handlers without TCP.
- Optional cargo features: `tls` (rustls HTTPS), `tracing` (structured spans), `brotli`.
- Unit tests plus real HTTP and HTTPS integration tests.

## Installation

### Local path dependency

```toml
[dependencies]
rustrest = { path = "/path/to/rustrest" }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

### Git dependency

```toml
[dependencies]
rustrest = { git = "https://github.com/your-user/rustrest.git" }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

### crates.io dependency

After the crate is published:

```toml
[dependencies]
rustrest = "0.3"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

RustRest uses Rust edition 2024 and requires Rust `1.85` or newer.

### Cargo features

All optional, disabled by default. Only `tls`, `tracing`, and `brotli` change
what is compiled today. The other names are **reserved** for the planned
modularization: enabling or disabling them currently has no effect, and every
surface they name is always available.

| Feature        | Effect                                                               |
| -------------- | -------------------------------------------------------------------- |
| `tls`          | HTTPS via rustls: `app.listen_tls(...)` + `rustrest::tls::config_from_pem` |
| `tracing`      | `middleware::trace()` spans, and RustRest's own diagnostics go to `tracing` (target `rustrest`) instead of stderr |
| `brotli`       | Brotli support in `middleware::compression()`                        |
| `compression`, `multipart`, `static-files`, `sse`, `websocket`, `openapi`, `sessions`, `metrics` | Reserved; no effect yet |
| `full`         | Enables all of the above                                             |

Without the `tracing` feature, server-side failures (5xx handler errors,
panics, accept errors, unexpected connection errors) are written to stderr.
Routine client behavior—4xx results, disconnects, failed TLS handshakes, and
protocol mismatches—is not logged, so clients cannot flood the logs; enable
`tracing` to see those events at `debug` level.

```toml
rustrest = { version = "0.3", features = ["tls", "tracing"] }
```

## Quick Start

```rust
use rustrest::{App, Request, Response};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut app = App::new();

    app.get("/", |_req: Request| {
        Response::send("Hello from RustRest")
    }).unwrap();

    app.get("/users/:id", |req: Request| {
        let id = req.param("id").unwrap_or("?");
        Response::send(&format!("Requested user: {}", id))
    }).unwrap();

    app.listen("127.0.0.1:3000").await
}
```

Run:

```bash
cargo run
```

Try:

```bash
curl http://127.0.0.1:3000/
curl http://127.0.0.1:3000/users/42
```

## Included Examples

```bash
cargo run --example basic
cargo run --example api
cargo run --example websocket
```

The `basic` example demonstrates simple routes and path parameters. The `api` example demonstrates routers, guards, extractors, shared state, built-in middleware, and errors. The `websocket` example demonstrates browser WebSocket clients, text messages, and JSON event envelopes.

## Mental Model

RustRest is built around four primary types:

```rust
use rustrest::{App, Router, Request, Response};
```

- `App`: the root application. It owns the root router, global middleware, shared state, and the global error handler.
- `Router`: a mountable group of routes. It can have scoped middleware, guards, and a fallback.
- `Request`: normalized request data passed to handlers.
- `Response`: the framework response type, converted internally into a Hyper response.

Request flow:

1. `App::listen` accepts TCP connections.
2. RustRest detects HTTP/1 or HTTP/2 and, for HTTP/1, classifies every raw
   request head before Hyper parses it.
3. Hyper parses the HTTP request.
4. RustRest validates the request head and builds a `Request`.
5. `Router` finds the most specific matching route through a trie index.
6. RustRest builds the middleware chain.
7. The handler returns `Response` or `Result<Response, E>`.
8. `Response` is finalized and converted into a Hyper response.

## Routes

Route registration returns `Result<RouteHandle, RouteError>` so invalid patterns,
duplicate/conflicting routes, duplicate names, and invalid host constraints fail
at startup instead of becoming runtime surprises. `RouteError` converts into
`std::io::Error` (`InvalidInput`), so `?` works in a `main` that returns
`std::io::Result<()>`:

```rust
#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut app = App::new();
    app.get("/", |_req: Request| Response::send("home"))?;
    app.listen("127.0.0.1:3000").await
}
```

```rust
let mut app = App::new();

app.get("/", |_req: Request| Response::send("home")).unwrap();
app.post("/users", |_req: Request| Response::send("create")).unwrap();
app.put("/users/:id", |_req: Request| Response::send("update")).unwrap();
app.patch("/users/:id", |_req: Request| Response::send("patch")).unwrap();
app.delete("/users/:id", |_req: Request| Response::send("delete")).unwrap();
app.options("/users", |_req: Request| Response::send("options")).unwrap();
app.head("/health", |_req: Request| Response::send("ok")).unwrap();
app.all("/any", |_req: Request| Response::send("any ordinary method")).unwrap();
```

`route(Method, ...)` supports extension methods such as `PURGE`. `CONNECT` is
intentionally rejected at registration and returns `501` even through
`all()`: tunneling is unsafe to model as a normal response handler because it
must take ownership of the upgraded transport.

Matching prefers the most specific pattern regardless of registration order: static segments beat `:params`, `:params` beat trailing `*wildcards` (with backtracking across branches), and an exact-method route beats `all()` on the same path. Remaining ties go to the first-registered route.

```rust
app.get("/users/:id", |_req: Request| Response::send("by id")).unwrap();
app.get("/users/me", |_req: Request| Response::send("me")).unwrap(); // still wins for /users/me
```

Request path segments are strict percent-decoded exactly once before trie
lookup, so encoded static segments retain static-route precedence. An encoded
slash such as `%2F` remains data inside one segment rather than changing the
route shape. Static segments in registered patterns are normalized by the same
rules, so `/caf%C3%A9` and `/café` conflict instead of becoming ambiguous.
Malformed/non-UTF-8 percent encoding and decoded control characters are
rejected in both incoming paths and registered static patterns.

Parameter and wildcard names must be non-empty ASCII letters, digits,
underscores, or hyphens; duplicate parameter names are rejected, and a
wildcard must be the final segment. Registration, mounting, and incoming
matching are all capped at 256 path segments. Trie lookup is iterative, so a
deep path within that bound cannot exhaust the call stack. A `GET` route also
answers `HEAD` at its own specificity (RFC 9110 §9.3.2): an explicit `HEAD`
route wins, but a less specific wildcard, `all()` route, fallback, or static
mount never shadows it. Automatic `HEAD` and `Allow` only consider ordinary
HTTP `GET` routes, never a WebSocket-only route.

### Trailing Slashes

By default `/users/` matches `/users`. The policy is configurable:

```rust
use rustrest::TrailingSlash;

app.trailing_slash(TrailingSlash::Strict);   // /users/ -> 404
app.trailing_slash(TrailingSlash::Redirect); // /users/ -> 308 to /users
```

### Route Listings

```rust
app.print_routes();              // "GET     /users/:id" per line
let routes = app.routes();       // Vec<RouteInfo> { method, path, summary, ... }
```

### Path Parameters

```rust
app.get("/users/:id/posts/:post_id", |req: Request| {
    let user_id = req.param("id").unwrap_or("?");
    let post_id = req.param("post_id").unwrap_or("?");
    Response::send(&format!("user={} post={}", user_id, post_id))
}).unwrap();
```

### Wildcards

Patterns such as `*name` capture the rest of the path. They are used internally for static files and fallbacks.

```rust
app.get("/files/*path", |req: Request| {
    Response::send(req.param("path").unwrap_or(""))
}).unwrap();
```

## Routers

Routers let you organize routes by module.

```rust
use rustrest::{Request, Response, Router};

fn users_router() -> Router {
    let mut router = Router::new();

    router.get("/", |_req: Request| Response::send("user list")).unwrap();
    router.get("/:id", |req: Request| {
        Response::send(req.param("id").unwrap_or("?"))
    }).unwrap();

    router
}

let mut app = App::new();
app.mount("/users", users_router()).unwrap();
```

This creates:

- `GET /users`
- `GET /users/:id`

Routers can be mounted inside other routers:

```rust
let mut api = Router::new();
api.mount("/users", users_router()).unwrap();

let mut app = App::new();
app.mount("/api", api).unwrap();
```

Result:

- `GET /api/users`
- `GET /api/users/:id`

## Handlers

A handler can be synchronous:

```rust
app.get("/", |_req: Request| {
    Response::send("sync")
}).unwrap();
```

Or asynchronous:

```rust
app.get("/async", |_req: Request| async move {
    Response::send("async")
}).unwrap();
```

A handler can also return `Result<Response, E>` when `E` implements `IntoHttpError`:

```rust
use rustrest::{HttpError, Request, Response};

app.get("/fallible", |_req: Request| -> Result<Response, HttpError> {
    Err(HttpError::bad_request("Invalid parameters"))
}).unwrap();
```

If a handler panics, RustRest catches it and returns `500`.

## Server limits and shutdown

`App::new()` uses finite transport defaults: a 10-second HTTP/1-header or
HTTP/2-client-preface deadline, a 30-second request/body/handler deadline, a
10-second TLS handshake deadline when TLS is enabled, a 10-second
graceful-shutdown deadline, at most 10,000 admitted connections, an 8 KiB
request target, an 8 KiB query string, 100 request header fields, and 32 KiB of
logical request header data (field-name plus field-value bytes). The HTTP/1
parser's allocation budget also includes bounded framing overhead, and the
configurable header-count limit is hard-capped at 1,024.

HTTP/2 is limited to 100 concurrent streams per connection and a 64 KiB send
buffer. Idle HTTP/2 connections receive a PING every 30 seconds and have 10
seconds to acknowledge it.

```rust
app.header_read_timeout(Duration::from_secs(5))
    .request_timeout(Duration::from_secs(20))
    .http2_keep_alive_interval(Duration::from_secs(20))
    .http2_keep_alive_timeout(Duration::from_secs(5))
    .graceful_shutdown_timeout(Duration::from_secs(15))
    .max_connections(2_000)
    .max_request_target_size(4 * 1024)
    .max_query_string_size(2 * 1024)
    .max_request_header_count(64)
    .max_request_header_bytes(16 * 1024);

// Available with the `tls` feature.
app.tls_handshake_timeout(Duration::from_secs(5));
```

`disable_header_read_timeout()`, `disable_request_timeout()`,
`disable_http2_keep_alive()`, and `disable_connection_limit()` are explicit
escape hatches for endpoints or trusted deployments that assume those budgets
elsewhere. The request deadline ends when the handler returns, so it does not
cap the lifetime of a response stream. When it expires, the handler is
cancelled and a `408` travels back out through the route, router, and global
middleware and the error handler, so CORS and request-id headers are still
applied. Disabling the header-read timeout also removes the bound on idle
HTTP/1 keep-alive connections. `max_connections(0)` is rejected when serving
starts (use `disable_connection_limit()` for no limit); at the limit, new
connections are closed immediately. The connection limit includes TLS
handshakes and upgraded connections. When the graceful deadline expires,
remaining plaintext, TLS-handshake, HTTP, pending-upgrade, and WebSocket tasks
are aborted.

HTTP/1.1 connections are persistent (RFC 9112 §9.3) and pipelined requests
are answered in order. For strict request-smuggling resistance, RustRest parses
every raw HTTP/1 request head on a connection with `httparse`, the parser Hyper
uses, before Hyper reads it. Hyper silently discards a `Content-Length` that
follows `Transfer-Encoding`, so this raw view is the only place the ambiguity
is visible: any request containing both fields, in either order and on any
request of the connection, is rejected with `400 ambiguous_message_framing`
and the connection is closed (RFC 9112 §6.1). The inspector follows
`Content-Length` framing to find the next head; a request whose successor it
does not track—one carrying `Transfer-Encoding` (a chunked upload), `Upgrade`,
or `CONNECT`, or a head it cannot parse—is answered with `Connection: close`,
so Hyper never parses a head the inspector did not see. The header-read
deadline also bounds how long a persistent connection may stay idle between
requests.

Prior-knowledge HTTP/2 must deliver the full client preface and initial
`SETTINGS` frame within the same deadline. Under TLS, ALPN selects one strict
protocol path: negotiated `h2` is never handed to an HTTP/1 parser, and
`http/1.1` (or no ALPN) is never interpreted as HTTP/2.

A successful WebSocket `101 Switching Protocols` keeps the upgraded
connection; any other response to a request carrying `Upgrade` closes the
connection, so it cannot be used to pipeline another HTTP request. HTTP/2
multiplexing is unaffected.

Framing rejections and Hyper parser errors occur before middleware and bypass
the custom error handler; framing rejections use a fixed Spanish problem
response. Once Hyper has parsed
the request, request-target/query/logical-header/authority limits use the
global error renderer but still do not enter middleware. Routing errors such
as invalid percent encoding, and framework-level `Content-Length` validation
errors, do enter global middleware. HTTP/1.1 requests require a valid authority
(normally one `Host` field). HTTP/2 `:authority` is exposed as `Host` when a
physical Host field is absent; duplicate or conflicting authority values are
rejected with `400`. Both sources require strict `host[:port]` syntax: no
userinfo or whitespace, a non-empty host, and an optional decimal port from 0
through 65535. Authority comparison normalizes hostname case, IPv6 spellings,
and scheme-default ports.

## Request

Main public fields:

```rust
pub struct Request {
    pub method: String,
    pub path: String,
    pub raw_query: Option<String>,
    pub query: HashMap<String, Vec<String>>,
    pub params: HashMap<String, String>,
    // headers, cookies, body and connection details are private; use methods
}
```

Useful methods:

```rust
req.param("id");
req.query("page");
req.query_all("tag");
req.header("authorization");
req.headers_all("x-forwarded-for");
req.singleton_header("authorization")?;
req.headers();                   // read-only collapsed view
req.set_header("authorization", "Bearer ...")?;
req.append_header("x-tag", "dos")?;
req.remove_header("authorization");
req.cookie("sid");
req.cookies();                   // read-only parsed view
req.set_cookie("sid", "rotated")?;
req.remove_cookie("sid");
req.bytes().await?;              // collected raw body bytes
req.text().await?;               // strict UTF-8 text
req.text_lossy().await?;         // explicitly lossy UTF-8 text
req.json::<MyType>().await?;
req.form::<MyForm>().await?;
req.multipart().await?;
req.multipart_with_limits(
    MultipartLimits::new()
        .max_parts(32)
        .max_part_bytes(1_048_576),
).await?;
req.state::<Config>();
req.remote_addr();
req.last_event_id();      // SSE reconnection header
req.is_websocket_upgrade();
req.websocket(|socket| async move { ... });
```

`req.path` is the raw request path. Routing percent-decodes segments and
ignores empty ones, so `/%61dmin/users` and `//admin/users` both reach
`/admin/users`. Do not authorize with string checks such as
`req.path.starts_with("/admin")`; scope the check with `Router::guard` or
router middleware, or compare the matched route pattern.

`header()` is a collapsed convenience view. Use `headers_all()` for
list-valued or intentionally repeatable fields, and
`singleton_header()` for fields whose grammar permits at most one occurrence.
The latter returns `400 duplicate_header` instead of silently choosing between
ambiguous duplicates.

The incoming request body is streamed into the handler and is only buffered
when a body helper or body extractor is awaited. Both collection and
`req.take_body_stream()` enforce `app.max_body_size(...)` (64 KiB by default)
or the route-specific limit incrementally. Even
`req.body_mut().collect(larger_limit)` uses the smaller of its argument and
that hard application/route ceiling; callers can tighten the limit, never
raise it. Buffered helpers render oversized bodies as `413`; the raw stream
yields one terminal limit error and then ends, so streaming handlers can map
or log it explicitly.

Multipart parsing is binary-safe and recognizes MIME delimiter lines and
quoted parameters. It rejects folded headers, invalid header names, duplicate
`Content-Disposition`/`Content-Type` fields, and control or DEL characters in
part-header values (horizontal tab is allowed). It is still buffered under the
overall request limit. `MultipartLimits` defaults to 128 parts, 32 headers and
16 KiB of header data per part, and 8 MiB per part; each value can be lowered
independently.

## Typed Extractors

RustRest includes async extractors. Parts-only extractors use `extract_parts`; body-consuming extractors use `extract`. Route handlers can also receive typed extractor arguments directly.

```rust
use rustrest::{Json, Path, Query, Request, Response, State};
use serde::Deserialize;

#[derive(Deserialize)]
struct UserPath {
    id: u32,
}

#[derive(Deserialize)]
struct UserQuery {
    active: Option<bool>,
    tag: Vec<String>,
}

#[derive(Deserialize)]
struct CreateUser {
    name: String,
}

struct Config {
    app_name: &'static str,
}

app.get("/users/:id", |mut req: Request| async move {
    let Path(path) = req.extract_parts::<Path<UserPath>>().await?;
    let Query(query) = req.extract_parts::<Query<UserQuery>>().await?;
    let State(config) = req.extract_parts::<State<Config>>().await?;

    Ok::<_, rustrest::HttpError>(Response::send(&format!(
        "{} id={} active={:?} tags={:?}",
        config.app_name,
        path.id,
        query.active,
        query.tag
    )))
}).unwrap();

app.post("/users", |mut req: Request| async move {
    let Json(user) = req.extract::<Json<CreateUser>>().await?;
    Ok::<_, rustrest::HttpError>(Response::send(&format!("Creating {}", user.name)).status(201))
}).unwrap();

async fn create_user(
    State(config): State<Config>,
    Path(path): Path<UserPath>,
    Json(user): Json<CreateUser>,
) -> Result<Response, rustrest::HttpError> {
    Ok(Response::send(&format!("{}:{}:{}", config.app_name, path.id, user.name)).status(201))
}

app.post("/typed/users/:id", create_user).unwrap();
```

`Option<E>` means “the value may be absent,” not “ignore every rejection.”
It converts an error to `None` only when `E::Rejection` implements
`OptionalRejection` and `is_missing()` returns `true`; malformed input,
unsupported media types, and body-limit failures such as `413` propagate to
the error handler. RustRest's built-in extractors mark only genuine absence:
a missing header, extension, state value, or matched path; no query string at
all for `Query<T>`; and, for `Json<T>`/`Form<T>`, a request with neither a
`Content-Type` nor a body. Build such rejections with `HttpError::missing()`.
Custom rejection types are conservative by
default and should opt in only for a genuine, safe absence. Use `Result<E,
E::Rejection>` when the handler intentionally needs to inspect every
rejection.

## Shared State

Register state by type:

```rust
struct Config {
    database_url: String,
}

let mut app = App::new();
app.state(Config {
    database_url: "postgres://localhost/app".to_string(),
});

app.get("/config", |req: Request| {
    let config = req.state::<Config>().expect("Config registered");
    Response::send(&config.database_url)
}).unwrap();
```

Internally, state is stored in `Arc`, so `req.state::<T>()` returns `Option<Arc<T>>`.

## Response

### Text

```rust
Response::send("hello")
```

Returns `200` with `text/plain; charset=utf-8`.

### JSON

```rust
#[derive(serde::Serialize)]
struct User {
    id: u32,
    name: String,
}

Response::json(&User {
    id: 1,
    name: "Ada".to_string(),
})
```

### Status

```rust
Response::send("created").status(201)
```

### Content-Type

```rust
Response::send("<h1>Hello</h1>").content_type("text/html; charset=utf-8")
```

### Headers

```rust
Response::send("ok")
    .header("x-trace-id", "abc")
    .append_header("vary", "accept-encoding")
```

### Cookies

```rust
Response::send("ok").cookie("sid", "abc123")
```

The `Response::cookie` convenience method delegates to the same `Cookie`
builder as `Response::set_cookie` and generates `Path=/; HttpOnly`. `Cookie`
applies component-specific filtering: names, values, and domains are reduced
to their allowed ASCII characters, while paths drop controls and semicolons,
so control characters cannot become response headers. `SameSite=None`,
`__Secure-`, and `__Host-` cookies are always `Secure`; `__Host-` also forces
`Path=/` and omits `Domain`.

Delete a cookie with the same scope that created it. `clear_cookie(name)` is
the root-path shortcut; `clear_cookie_with` preserves the supplied `Path`,
`Domain`, and security attributes while forcing an empty value and
`Max-Age=0`:

```rust
Response::send("ok").clear_cookie_with(
    Cookie::new("sid", "")
        .path("/admin")
        .domain("example.com")
        .secure(true),
)
```

`sign_value(secret, value)` is a general HMAC helper: it authenticates the
exact original string but does not encode it into the cookie-octet grammar.
When a signed value will be stored in a cookie, encode arbitrary Unicode,
whitespace, commas, semicolons, or other disallowed bytes before signing.
Passing such a raw signed string through `Cookie::new` would sanitize it and
therefore invalidate its signature. The built-in session IDs already use a
cookie-safe representation.

### Sessions

```rust
use rustrest::{SameSite, Sessions};
use std::time::Duration;

let sessions = Sessions::try_new("a-random-secret-with-at-least-32-bytes")?
    .idle_timeout(Duration::from_secs(30 * 60))
    .max_sessions(20_000)
    .max_entries_per_session(32)
    .max_session_key_bytes(128)
    .max_session_value_bytes(8 * 1024)
    .max_session_data_bytes(32 * 1024)
    .same_site(SameSite::Lax)
    .secure_cookies(true);

app.layer(sessions.middleware());
```

`Sessions::try_new` rejects secrets shorter than 32 bytes. Every consuming
builder has a fallible `try_*` counterpart (`try_cookie_name`,
`try_idle_timeout`, `try_max_sessions`, and so on); the infallible variants
panic on invalid configuration. Complete configuration before cloning
`Sessions` or calling `middleware()`. While storage has another live clone or
middleware handle, fallible builders return `SessionConfigError` and
infallible builders panic instead of silently letting handles use incompatible
cookie, expiry, or size policies.

The default store is sharded, expires sessions after 24 hours of inactivity,
and retains at most 10,000 sessions. Each session is also limited to 64 entries,
256-byte keys, 16 KiB values, and 64 KiB of aggregate key-plus-value data by
default. Admission is lazy: middleware exposes a transient `req.session_id()`
to handlers, but a read-only anonymous request neither occupies the store nor
receives a session cookie. The first successful `Sessions::set` persists that
ID. At capacity, new writes return `SessionUnavailable`; live sessions are
never evicted to admit anonymous traffic. Expired capacity is recovered from
an exact ordered expiry frontier per shard in bounded batches, without scanning
the complete store.
`Sessions::set` returns `Result<(), SessionDataError>` and never silently grows
past those bounds:

```rust
sessions.set(session_id, "usuario", "42")?;
```

Rotate the id whenever privilege changes, typically right after login, so an
id planted before authentication (session fixation) becomes useless. Data is
carried over, the old id stops working immediately, and the middleware sends
the new cookie with the same response:

```rust
let id = sessions.regenerate(req.session_id().unwrap())?;
sessions.set(&id, "usuario", "42")?;
```

Access extends the server-side idle deadline. Middleware revalidates and
renews the session when the response is ready, then reissues the signed cookie
with a matching, upward-rounded `Max-Age`. A presented session that expired or
was cleared while the handler ran is not resurrected; the client receives a
deletion cookie instead. An invalid or expired presented cookie is likewise
deleted when the handler does not persist a replacement. An application cookie
suppresses the automatic cookie only when its effective scope is the same
host-only root path, so a same-name
`Domain` or narrower-path cookie cannot accidentally disable session refresh.
Cookies are `HttpOnly`, use `SameSite=Lax` by default, and become `Secure` on
direct TLS requests.
`secure_cookies(true)` is intended for deployments behind a trusted
TLS-terminating proxy.

Requests containing the configured session-cookie name more than once are
rejected with `400 duplicate_session_cookie` before handlers run. This avoids
ambiguous browser/proxy ordering from becoming a session-fixation primitive.

Because a session response sets or refreshes client-specific state, the
middleware also prevents shared-cache reuse. It preserves valid existing
`Cache-Control` fields and appends `private` unless a `private` or `no-store`
directive is already present. An invalid field fails closed as
`Cache-Control: private, no-store`. If sessions are mounted globally, this
policy applies to every response; mount them on a router when public cacheable
routes should remain outside the session scope.

### Redirects

```rust
Response::redirect("/login")
Response::redirect_with_status("/new", 301)
```

Like Express's `res.redirect`, the `Location` value is percent-encoded where
it is not a valid URI reference (spaces, non-ASCII text), while existing
`%XX` escapes are kept: `/café` becomes `/caf%C3%A9`. A status outside
`300..=399` is a construction error rendered as `500`. The value is otherwise
used as given, so never redirect to unvalidated user input (open redirect).

### Common Errors

```rust
Response::not_found()
Response::bad_request()
Response::internal_server_error()
Response::from_error(HttpError::forbidden("Access denied"))
```

### Bytes

```rust
use hyper::body::Bytes;

Response::bytes(Bytes::from_static(b"binary"), "application/octet-stream")
```

### Streaming

```rust
use futures_util::stream;
use hyper::body::Bytes;
use std::convert::Infallible;

let chunks = stream::iter(vec![
    Ok::<_, Infallible>(Bytes::from_static(b"hello ")),
    Ok::<_, Infallible>(Bytes::from_static(b"stream")),
]);

Response::stream(chunks).content_type("text/plain; charset=utf-8")
```

`Response::stream` also accepts fallible streams. Stream errors are propagated to the HTTP body
instead of being swallowed:

```rust
let chunks = stream::iter(vec![
    Ok::<_, std::io::Error>(Bytes::from_static(b"hello")),
    Err(std::io::Error::other("read failed")),
]);

Response::stream(chunks)
```

HTTP trailers can be appended as the final body frame:

```rust
use hyper::HeaderMap;
use hyper::header::HeaderValue;

let mut trailers = HeaderMap::new();
trailers.insert("x-checksum", HeaderValue::from_static("abc"));

Response::stream(stream::iter(vec![Ok::<_, Infallible>(Bytes::from_static(b"data"))]))
    .with_trailers(trailers)
```

Trailer delivery is protocol-dependent and must be treated as best effort.
HTTP/2 carries trailing fields natively. For HTTP/1.1, Hyper emits them only
when the request advertises `TE: trailers`; otherwise the declared late fields
can be discarded, and HTTP/1.0 cannot carry them. Clients must tolerate an
absent trailer rather than making it the only source of correctness-critical
metadata.

For checked response construction, use `try_status`, `try_header`, `try_append_header`, and
`try_with_trailers`. The fluent wrappers still exist; invalid values are recorded and rendered as a
structured `500` at the HTTP boundary instead of panicking or silently disappearing.
Framing, routing, authentication, representation-control, and cookie fields such as
`Content-Length`, `Transfer-Encoding`, `Host`, `Content-Type`, and `Set-Cookie` are forbidden in
trailers. `ETag` and application integrity fields may be emitted late, but
`Keep-Alive` and any field nominated by `Connection` are rejected. Explicit
`Transfer-Encoding` is rejected on all responses; Hyper selects transport framing.

The network server and `TestClient` apply the same finalization rules and
materialize the same implicit `Content-Type` and buffered
`Content-Length` headers. Repeated `Content-Length` values must be valid and
identical, a buffered body's length must match, and trailers cannot be
combined with `Content-Length`.

A final response cannot be an interim `100`–`199` status. RustRest rejects
those statuses except for a valid `101 Switching Protocols`, which must include
the WebSocket `Connection`, `Upgrade`, and one non-empty
`Sec-WebSocket-Accept` field. Bodies and trailers are removed from `204`,
`205`, and `304` responses (a valid `Content-Length` may remain on `304` as
allowed by HTTP semantics). Invalid responses become a structured `500` with
the construction detail retained only as a private error source.

Automatic and explicit `HEAD` responses run the corresponding handler and then
discard its body and trailers. For a buffered body they retain or synthesize
the `Content-Length` that the equivalent `GET` representation would have sent,
and an explicit mismatched length is still rejected. Timeout and framework
error responses follow the same `HEAD` rule.

## Middleware

Middleware receives `Request` and `Next`. A closure passed straight to
`layer` must annotate `next: Next`; `middleware::from_fn` infers both types:

```rust
use rustrest::middleware;

app.layer(middleware::from_fn(|req, next| async move {
    next(req).await.header("x-powered-by", "rustrest")
}));
```

```rust
use rustrest::{Next, Request, Response};

app.layer(|req: Request, next: Next| async move {
    println!("--> {} {}", req.method, req.path);
    let res = next(req).await;
    println!("<-- {}", res.status);
    res
});
```

It can short-circuit the chain without calling `next`:

```rust
app.layer(|_req: Request, _next: Next| async move {
    Response::send("blocked").status(403)
});
```

### Scoped Middleware

```rust
let mut router = Router::new();

router.layer(|req: Request, next: Next| async move {
    println!("[api] {}", req.path);
    next(req).await
});

router.get("/health", |_req: Request| Response::send("ok")).unwrap();
app.mount("/api", router).unwrap();
```

That middleware only runs for routes under `/api`, including the automatic
`OPTIONS` and `405 Method Not Allowed` responses for those routes' paths (so
a router-level `Cors` handles preflights and a router guard cannot be probed
through `405`). It does not run for `404`s under the prefix; register global
middleware for behavior that must cover every request.

### Built-In Middleware

```rust
use rustrest::middleware;
use std::time::Duration;

app.layer(middleware::tracing());
app.layer(middleware::request_id());
app.layer(middleware::cors());
app.layer(middleware::etag());
app.layer(middleware::compression());
app.layer(middleware::rate_limit(100, Duration::from_secs(60)));
app.layer(
    middleware::RateLimit::new(100, Duration::from_secs(60))
        .max_clients(25_000),
);
app.get("/slow", slow_handler)
    .unwrap()
    .layer(middleware::timeout(Duration::from_secs(5)));
```

The first registered middleware is outermost and therefore sees the response
last. Register `etag()` before `compression()` so the validator is calculated
from the final content-coded bytes; compression removes validators that describe
the unencoded representation.

- `tracing`: prints method, path, and status to stdout (a plain logger,
  unrelated to the `tracing` crate); `trace()` (with the `tracing` feature)
  emits structured spans instead.
- `request_id`: propagates exactly one non-empty, printable-ASCII
  `x-request-id` up to 128 bytes; duplicate, control-containing, whitespace,
  non-ASCII, empty, or oversized input is replaced with a generated ID.
- `cors`: adds permissive CORS headers (see the configurable `Cors` builder
  for allowlists, credentials, and preflight). `allow_any_origin()` together
  with `allow_credentials(true)` echoes any requesting origin, which lets every
  site make credentialed requests; prefer an allowlist. The opaque `null`
  origin is never granted in that mode. Configurable responses include
  `Vary: Origin` whenever the result can depend on the origin—even when it is
  denied—and preflights also vary by
  `Access-Control-Request-Method`/`Access-Control-Request-Headers`. `Origin`
  and `Access-Control-Request-Method` are singleton fields; duplicates receive
  `400` instead of being collapsed. Repeated
  `Access-Control-Request-Headers` lines are combined as a list.
- `gzip`: compresses byte responses when the client accepts `gzip`.
- `compression` / `compression_with_min_size`: weighted negotiation for
  gzip/deflate (plus brotli with the `brotli` feature), including `identity`,
  pre-encoded responses, correct `Vary`, and `406` when no acceptable
  representation exists. A transformation removes stale validators,
  digests, length, and range metadata; large buffered bodies are compressed
  on a bounded blocking pool. Repeated `Accept-Encoding` field lines are
  combined as one list, and any repeated `Cache-Control` field containing
  `no-transform` prevents transformation.
- `etag`: strong SHA-256 `ETag` for all buffered 200 responses, including empty
  bodies, with RFC-ordered precondition handling for `GET` and `HEAD`.
  Repeated list-valued `If-Match`/`If-None-Match` fields are combined. Large
  hashes run on a bounded blocking pool. Unsafe-method preconditions must be
  checked by application middleware before the mutating handler; a
  response-side ETag layer cannot safely reject a mutation afterward.
- `rate_limit(max, window)`: bounded fixed-window per-client-IP limiting; over
  the limit returns `429` with `Retry-After` rounded up to the next whole
  second.
- `RateLimit::new(max, window).max_clients(n)`: configures the tracked-client
  bound (10,000 by default); clients beyond it share one bounded overflow
  bucket and expired buckets are swept periodically.
- `timeout(duration)`: cuts off the wrapped handler with `408`; scope it per route or per router.

## Guards

A guard blocks requests before they reach the router's routes.

```rust
let mut api = Router::new();

api.guard(|req: &Request| {
    req.header("x-api-key") == Some("secret")
});

api.get("/private", |_req: Request| Response::send("private")).unwrap();
app.mount("/api", api).unwrap();
```

If the guard fails, RustRest returns `403 Access denied`.

## Fallbacks

Global fallback:

```rust
app.fallback(|_req: Request| {
    Response::send("Not found").status(404)
}).unwrap();
```

Scoped fallback:

```rust
let mut api = Router::new();

api.get("/health", |_req: Request| Response::send("ok")).unwrap();
api.fallback(|_req: Request| {
    Response::send("API route not found").status(404)
}).unwrap();

app.mount("/api", api).unwrap();
```

## Static Files

```rust
use rustrest::{App, Dotfiles, StaticFilesOptions};
use std::time::Duration;

let mut app = App::new();
app.static_files("/assets", "public").unwrap();

// Explicit opt-in when hidden files are intentionally public.
app.static_files_with_options(
    "/public-dotfiles",
    "public",
    StaticFilesOptions::new()
        .dotfiles(Dotfiles::Allow)
        .stream_stall_timeout(Duration::from_secs(20))
        .stream_max_duration(Duration::from_secs(10 * 60)),
).unwrap();
```

Examples:

- `/assets/app.css` serves `public/app.css`.
- `/assets/images/logo.png` serves `public/images/logo.png`.

The root directory is opened and pinned when the route is registered, so a
missing or inaccessible root returns `RouteError` immediately. File lookup is
capability-relative: `..`, absolute paths, symlink escapes, and path-swap races
cannot leave that root. Dotfiles are denied with `404` by default; use
`StaticFilesOptions` and `Dotfiles::Allow` only for an intentional public
mount. Blocking filesystem open/metadata work runs outside Tokio's async
workers under bounded admission: at most 256 static files are open or
streaming across the process, and a request that cannot be admitted within
two seconds receives `503` with `Retry-After` instead of waiting. A supervised, one-chunk producer retains the
file descriptor and admission permit only while the transport is progressing:
the body is lazy, an entirely unpolled body expires after one minute, and the
safe defaults close an active producer after 15 seconds stalled or five
minutes total. `stream_start_timeout`, `stream_stall_timeout`, and
`stream_max_duration` can raise those limits; the explicit `disable_*`
variants should be used only when an edge proxy enforces equivalent
body-start, response-write, and lifetime bounds. File contents remain streamed
with bounded read-ahead. A missing file is `404`;
permission, descriptor, and other non-`NotFound` I/O failures are `500`.
Metadata-derived validators are correctly emitted as weak `ETag` values,
range units such as `BYTES=` are matched case-insensitively, and common
extensions receive a content type. Byte ranges apply only to `GET`; `HEAD`
describes the complete representation. `If-Range` dates must exactly match the
resource's whole-second modification time, and ambiguous duplicate fields
fall back to the full response.

## Error Handling

`HttpError` represents HTTP errors:

```rust
HttpError::bad_request("Invalid request");
HttpError::unauthorized("Unauthenticated");
HttpError::forbidden("Access denied");
HttpError::not_found("Not found");
HttpError::internal_server_error("Internal failure");
```

Handlers with `Result`:

```rust
app.get("/users/:id", |req: Request| -> Result<Response, HttpError> {
    let id = req.param("id").ok_or_else(|| {
        HttpError::bad_request("Missing id")
    })?;

    Ok(Response::send(id))
}).unwrap();
```

Global error handler:

```rust
app.error_handler(|err: HttpError| {
    Response::json(&serde_json::json!({
        "error": err.public_message(),
        "status": err.status().as_u16(),
    }))
    .status(err.status().as_u16())
});
```

Deserializer and internal handler details are retained as private error
sources rather than copied into public problem responses. If the custom error
handler itself panics, RustRest falls back to a generic `500`. Headers added
to an error response after `Response::from_error` (for example
`WWW-Authenticate` on a `401`, `Set-Cookie`, or `Cache-Control`) and an
explicit `.status(..)` override are kept when the error handler renders it.

## OpenAPI & Docs UI

Routes can carry documentation, and the app can describe itself as OpenAPI 3.0:

```rust
app.get("/users", list_users)
    .unwrap()
    .summary("Lista usuarios")
    .description("Devuelve todos los usuarios registrados")
    .tag("users");
app.get("/users/:id", show_user).unwrap().tag("users");

// A serde_json::Value with paths, methods, and path parameters:
let doc = app.openapi("Mi API", "0.3.0");

// Or serve it: GET /docs (Swagger UI) + GET /docs/openapi.json.
// Snapshot semantics: call after registering the routes.
app.serve_docs("/docs", "Mi API", "0.3.0").unwrap();
```

The generated document covers paths, methods, metadata, and
`:param`/`*wildcard` path parameters (typed as strings). Request/response
schemas are not introspected. Methods supported by an OpenAPI Path Item become
ordinary operations. `all()` and extension methods are preserved in the
`x-rustrest-custom-methods` array with their original `method` instead of
being silently dropped.
Swagger UI assets use an exact version and the generated page includes a restrictive CSP whose
script hash authorizes only its escaped initializer, plus a no-referrer policy.
The title and spec URL are escaped for their HTML/JavaScript contexts. If
host-specific routes collapse to the same OpenAPI path and method, the first
registered operation is retained and
`x-rustrest-duplicate-routes` reports the number of variants.

## Server-Sent Events

```rust
use futures_util::stream;
use rustrest::{Response, SseEvent};

app.get("/events", |_req: Request| {
    let events = stream::iter(vec![
        SseEvent::new("hello").event("greeting").id("1"),
        SseEvent::new("goodbye"),
    ]);

    Response::sse(events)
}).unwrap();
```

The response uses `text/event-stream` and `Cache-Control: no-cache`. RustRest
leaves persistence and framing to the server, so it does not emit the
HTTP/2-forbidden `Connection` header.
Invalid SSE fields terminate the response body with a stream error rather than emitting malformed
events. Event data treats bare carriage returns, line feeds, and CRLF pairs as
line boundaries and prefixes every resulting line with `data:`, preventing
embedded CR data from being interpreted as a new SSE field.

For long-lived streams, `sse_with_heartbeat` emits a `: keep-alive` comment whenever the source stream is idle for the given interval, and `req.last_event_id()` exposes the ID browsers resend when they reconnect. A zero heartbeat disables heartbeat generation:

```rust
use std::time::Duration;

app.get("/events", |req: Request| {
    let resume_after = req.last_event_id().map(str::to_string);
    let events = my_event_stream(resume_after);
    Response::sse_with_heartbeat(events, Duration::from_secs(15))
}).unwrap();
```

## WebSocket

RustRest supports native WebSocket routes:

For deployment, security, rooms, brokers, WSS, limits, and validation tooling,
see [Production WebSockets](docs/websocket-production.md).

```rust
use rustrest::{App, WebSocketEvent};
use serde_json::json;

let mut app = App::new();

app.websocket("/ws", |mut socket| async move {
    socket
        .send_event("server:ready", &json!({ "message": "connected" }))
        .await
        .ok();

    while let Ok(Some(message)) = socket.recv().await {
        if message.is_text() {
            let text = message.into_text().unwrap().to_string();
            socket.send_text(&format!("echo:{}", text)).await.ok();
            socket
                .send_event("chat:message", &json!({ "text": text }))
                .await
                .ok();
        } else if message.is_close() {
            break;
        }
    }
}).unwrap();
```

`Router` has the same API:

```rust
let mut router = Router::new();
router.websocket("/ws", |mut socket| async move {
    socket.send_text("hello").await.ok();
}).unwrap();
app.mount("/api", router).unwrap();
```

There is also a short alias:

```rust
app.ws("/ws", |mut socket| async move {
    socket.close().await.ok();
}).unwrap();
```

### WebSocket Methods

```rust
socket.recv().await;
socket.send(WebSocketMessage::text("hello")).await;
socket.send_text("hello").await;
socket.send_binary(bytes).await;
socket.send_json(&value).await;
socket.recv_json::<T>().await;
socket.send_event("event:name", &data).await;
socket.recv_event::<T>().await;
socket.ping(bytes).await;
socket.pong(bytes).await;
socket.close().await;
socket.close_with(1000, "finalizado").await;
socket.closed().await;
socket.join("general").await;
socket.rooms().await;
socket.to("general").send_text("hello").await;
```

`recv()` returns `Result<Option<WebSocketMessage>, WebSocketError>`. `Ok(None)` means the peer closed the stream.

### Event Envelopes

WebSocket itself has no named events. RustRest provides a small JSON convention:

```json
{
  "event": "chat:message",
  "data": {
    "text": "hello"
  }
}
```

Server side:

```rust
socket
    .send_event("chat:message", &serde_json::json!({ "text": "hello" }))
    .await?;

if let Some(event) = socket.recv_event::<serde_json::Value>().await? {
    println!("event={} data={}", event.event, event.data);
}
```

Client side with the browser WebSocket API:

```html
<script>
  const socket = new WebSocket("ws://127.0.0.1:3000/ws");

  socket.addEventListener("open", () => {
    socket.send(JSON.stringify({
      event: "client:hello",
      data: { text: "hello from browser" }
    }));
  });

  socket.addEventListener("message", (message) => {
    const parsed = JSON.parse(message.data);
    switch (parsed.event) {
      case "server:ready":
        console.log("ready", parsed.data);
        break;
      case "chat:message":
        console.log("chat", parsed.data.text);
        break;
      default:
        console.log("raw message", message.data);
    }
  });

  socket.addEventListener("close", () => console.log("closed"));
  socket.addEventListener("error", (error) => console.error(error));
</script>
```

### WebSocket vs Socket.IO

RustRest implements native WebSocket, not the Socket.IO protocol.

Socket.IO adds its own protocol on top of HTTP/WebSocket: named events, acknowledgements, fallback transports, rooms, namespaces, reconnection behavior, heartbeats, and a Socket.IO-specific client. A Socket.IO JavaScript client cannot connect directly to a plain WebSocket server unless the server implements the Socket.IO protocol.

With RustRest, use the browser `WebSocket` API or any WebSocket client. For named events, use `send_event` / `recv_event`, which are plain JSON messages and can be consumed from any language.

### Configuration, Subprotocols, and Keepalive

`websocket_with` accepts a `WebSocketConfig` on `App`, `Router`, and `Request`:

```rust
use rustrest::WebSocketConfig;
use std::time::Duration;

let config = WebSocketConfig::new()
    .protocols(&["superchat", "chat"])         // negotiated + echoed to the client
    .max_message_size(1024 * 1024)             // larger incoming messages error
    .ping_interval(Duration::from_secs(30));   // pings while idle in recv()

app.websocket_with("/ws", config, |mut socket| async move {
    let negotiated = socket.protocol(); // Some("superchat") etc.
    while let Ok(Some(message)) = socket.recv().await {
        // ...
    }
}).unwrap();
```

Unset WebSocket settings resolve to bounded defaults: 1 MiB messages, 256 KiB
frames, 16-slot inbound/outbound queues, 128 KiB/1 MiB write buffers, a
five-second send/write and close timeout, Ping every 30 seconds with a
10-second Pong allowance, a 120-second idle timeout, a 24-hour lifetime,
2,000 process connections, 20 connections per IP, 100 incoming messages per
second, 32 rooms per connection, and 128-byte room names. The default
backpressure policy waits only for the finite send timeout.

The default origin policy is same-host, rejects a missing `Origin`, and
verifies that its scheme matches plaintext versus TLS. Non-browser endpoints
must opt out explicitly when they intentionally omit `Origin`.

The first client-offered subprotocol the server supports is selected and echoed
in `Sec-WebSocket-Protocol`. Subprotocol names are case-sensitive, must be
unique valid HTTP tokens, and malformed client offer lists receive `400`.
Transport writes and flushes, including Close, are
bounded by the configured timeout so a peer that stops reading cannot wedge
shutdown. JSON/event receive helpers transparently process Ping/Pong control
frames rather than reporting them as application end-of-stream.

### Rooms and Managed Broadcasts

`WsHub` and socket selectors provide route-scoped rooms, sender exclusion,
multi-room deduplication, bounded fan-out, and explicit delivery reports:

```rust
app.websocket("/chat/:channel", |mut socket| async move {
    socket.join_many(["general", "equipo-7"]).await?;

    while let Some(event) = socket.recv_event::<serde_json::Value>().await? {
        match socket
            .to("general")
            .send_event("chat:message", &event.data)
            .await
        {
            Ok(report) => println!("matched={} enqueued={}", report.matched, report.enqueued),
            Err(error) => eprintln!("broadcast failed: {error}"),
        }
    }
    Ok::<(), rustrest::WsError>(())
}).unwrap();
```

Rooms are scoped by the normalized route pattern. `socket.to(...)` excludes
the sender; `app.websocket_hub_handle().route(...).all()` includes every local
match unless `.except(id)` is used. An optional `WsBroker` extends room
broadcasts across nodes without making global presence guarantees.

### Raw Tokio Broadcast Helper

`WsBroadcast` is a raw local `tokio::sync::broadcast` channel. It does not
track sockets, routes, rooms, backpressure reports, or brokers:

```rust
use rustrest::{WebSocketMessage, WsBroadcast};

let room = WsBroadcast::new(64);
app.state(room.clone());

app.websocket("/chat", move |mut socket| {
    let room = room.clone();
    async move {
        let mut feed = room.subscribe();
        loop {
            tokio::select! {
                Ok(message) = feed.recv() => {
                    if socket.send(message).await.is_err() { break; }
                }
                received = socket.recv() => match received {
                    Ok(Some(message)) if message.is_text() => { room.send(message); }
                    Ok(Some(_)) => {}
                    _ => break,
                },
            }
        }
    }
}).unwrap();
```

Lagging subscribers receive `RecvError::Lagged` and must handle skipped
messages explicitly. A zero constructor capacity is normalized to one rather
than panicking. Prefer `WsHub` for managed WebSocket fan-out.

### Manual Handshake Helper

`Response::websocket(&req)` is deprecated. A response-only helper cannot own
Hyper's private upgraded transport, so registering it as a network route would
otherwise emit `101` and immediately close the socket. It now refuses real
network upgrades; synthetic requests may still use it temporarily to validate
handshake response construction directly. At the server boundary, even a
well-formed manual `101` is rejected unless the consuming WebSocket API marked
the response as owning the upgrade. Use `app.websocket`, `router.websocket`,
or `req.websocket(handler)`, all of which own the upgraded stream and frame
loop.

## Serving with an Existing TcpListener

Besides `listen`, you can use `serve` for tests or custom bootstrap code, or
`serve_with_shutdown` / `listen_with_shutdown` for graceful shutdown:

```rust
use tokio::net::TcpListener;

let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

// Serve until the process is killed.
app.serve(listener).await?;

// Or stop accepting on a signal and drain in-flight connections:
// app.serve_with_shutdown(listener, async {
//     tokio::signal::ctrl_c().await.ok();
// }).await?;
```

`listen`, `serve`, and the `*_with_shutdown` variants all return
`std::io::Result<()>`.

## Testing

Main commands:

```bash
cargo fmt --check
cargo check
cargo clippy --all-targets
cargo test
```

The project includes:

- Core unit tests in `src/app/tests.rs`.
- HTTP routing, framing/body, semantic, TLS, and WebSocket integration suites
  under `tests/`.
- Five fuzz targets, including route-pattern and path matching.

`Request::builder()` and `TestClient` mirror network normalization closely,
but they run in-process: transport checks (request-target, query, header
count/size limits, `Host` validation, raw framing) only happen on real
connections, so cover those with socket tests.
`RequestBuilder::path("/items?page=2")` splits path and query, while a later
`.path("/items")` clears the previous query. Repeated `Cookie` headers are
processed in arrival order and the last duplicate cookie name wins, as on the
server. `.json(&value)` sets `application/json` and panics if serialization
fails rather than silently sending an empty payload; use
`.try_json(&value)` for fallible construction. `TestClient::send()` finalizes
responses, so assertions see the implicit `Content-Type` and buffered
`Content-Length` that a network client receives.

## Compatibility and releases

See the [changelog](CHANGELOG.md) for notable changes, the [release
policy](docs/releases.md) for compatibility guarantees, and the [migration
guides](docs/migrations/README.md) for breaking upgrades. For this release,
start with [Migrating from 0.3 to 0.4](docs/migrations/0.3-to-0.4.md).

## Publishing Preparation

This repository already includes basic crates.io metadata:

- `description`
- `license`
- `readme`
- `documentation`
- `keywords`
- `categories`
- `rust-version`

Before publishing:

```bash
cargo package --list
cargo publish --dry-run
```

To publish for real:

```bash
cargo login
cargo publish
```

This README documents the process only. It does not publish the crate.

## Project Structure

```text
src/
  lib.rs                 # Public crate API
  main.rs                # Demo server for cargo run
  api.rs                 # Demo router used by main.rs
  users.rs               # Demo router used by main.rs
  app.rs                 # Module wiring + public re-exports
  app/
    server.rs            # App, ServerConfig, listen/serve/dispatch
    router.rs            # Router, RouteHandle, RouteInfo, static files
    trie.rs              # Trie index backing route lookup
    openapi.rs           # OpenAPI document builder + Swagger UI page
    request.rs           # Request + RequestBuilder
    response.rs          # Response + IntoResponse
    handler.rs           # Handler/Next/Middleware plumbing
    extract.rs           # Json, Form, Path, Query, State, Cookies, Headers, ...
    form.rs              # Form bodies + multipart parser
    cookie.rs            # Cookie builder + sign/verify helpers
    session.rs           # Minimal in-memory Sessions middleware
    middleware/          # Built-in middleware (Cors, compression, conditionals, ...)
    error.rs             # HttpError and IntoHttpError
    state.rs             # Type-keyed StateStore
    testing.rs           # In-process TestClient
    tls.rs               # HTTPS via rustls (feature `tls`)
    sse.rs               # SseEvent
    websocket.rs         # WebSocket support
    tests.rs             # Framework unit tests
examples/
  basic.rs               # Minimal example
  api.rs                 # Full API example
  streaming_upload.rs    # Streaming request body upload example
  websocket.rs           # WebSocket and browser client example
tests/
  http_integration.rs    # Real HTTP integration tests
  http_semantics.rs      # Compression/precondition protocol tests
  tls_integration.rs     # Real HTTPS integration test (feature `tls`)
```

## Current Limitations

- Request body helpers and body extractors buffer on demand under configurable limits; multipart
  parsing has per-part limits but does not stream individual parts.
- HTTP/1 requests with a chunked (`Transfer-Encoding`) body are served and then
  close their connection, because the raw-head inspector does not decode
  chunked framing to find the next request. `Content-Length` uploads keep the
  connection persistent.
- HTTP/2 keepalive closes unresponsive peers, but a responsive idle peer can
  retain a global connection permit. Keep the finite connection cap enabled
  and enforce per-IP/idle policy at the edge until the framework can add a
  stream-aware HTTP/2 idle policy without terminating legitimate long-lived
  SSE responses.
- Sessions are bounded and expiring but remain in-memory and single-process; use an external store
  for multi-instance deployments.
- Rate limiting is bounded but remains in-memory and per process.
- OpenAPI output covers paths, methods, and path parameters; request/response schemas are not introspected.

## License

MIT. See `LICENSE`.
