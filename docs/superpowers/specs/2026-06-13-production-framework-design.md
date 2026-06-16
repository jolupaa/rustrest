# RustRest Production Framework Design

- **Status:** Approved
- **Date:** 2026-06-13
- **Branch:** `codex/framework-completeness-roadmap`
- **Current version:** `0.2.0`
- **Target:** staged releases from `0.3.0` to a stable `1.0.0`
- **Scope:** every framework subsystem except the completed WebSocket implementation

## 1. Purpose

RustRest already provides a broad Express-style API over `hyper` and `tokio`: trie routing,
middleware, typed extraction, streaming responses, SSE, static files, TLS, OpenAPI, sessions,
testing helpers and production-grade WebSockets. The remaining work is not primarily adding more
surface area. It is turning the first versions of those features into explicit, composable and
observable production contracts.

The target architecture is:

1. A small, stable HTTP core with few mandatory dependencies.
2. Official optional modules for advanced framework capabilities.
3. Separate integration crates when a capability requires a large dependency tree or an external
   service.
4. A documented compatibility policy that allows applications to upgrade without discovering
   behavioral changes in production.

The framework should remain hand-written on top of `hyper`; adopting Axum, Actix, Warp or another
high-level server framework is outside the design.

## 2. Current Baseline

The audit was performed from commit `38b2112` on `master`. The baseline is healthy:

- 115 unit tests pass.
- 48 integration tests pass, including 41 WebSocket tests.
- 2 doctests pass.
- CI covers default, TLS, tracing, Brotli, all features, MSRV, Clippy, fuzz-target compilation,
  WebSocket load smoke and Autobahn.

The current API already includes:

- HTTP/1.1 and HTTP/2 serving.
- TLS with rustls.
- Trie routing with static, parameter and wildcard precedence.
- Automatic HEAD, OPTIONS and 405 handling.
- Global, router and route middleware.
- Buffered request bodies with a global limit.
- Buffered and streaming responses.
- JSON, forms, multipart, cookies and typed extractors.
- Static files, ranges and conditional requests.
- SSE with heartbeat.
- In-memory sessions and rate limiting.
- Basic OpenAPI generation and Swagger UI.
- In-process testing.
- Graceful HTTP and WebSocket shutdown.

This is a strong `0.2` feature set. The gaps below are about correctness under load, API contracts,
distributed deployment, observability and long-term maintenance.

## 3. Architectural Decision

### 3.1 Crate layout

`rustrest` remains the facade crate. Its default feature set contains only the production HTTP
core:

- application and server lifecycle;
- routing;
- handlers and middleware contracts;
- request and response primitives;
- typed state;
- structured HTTP errors;
- in-process testing primitives.

Official optional features remain in the same crate when they are cohesive and have moderate
dependency cost:

| Feature | Responsibility |
| --- | --- |
| `compression` | gzip and deflate response compression |
| `brotli` | Brotli compression extension |
| `multipart` | streaming multipart parsing |
| `static-files` | conditional and ranged file serving |
| `sse` | Server-Sent Events |
| `websocket` | existing production WebSocket stack |
| `openapi` | route metadata and OpenAPI generation |
| `sessions` | session contracts and in-memory reference store |
| `tls` | rustls transport |
| `tracing` | structured tracing hooks |
| `metrics` | framework metrics interface and built-in recorder |
| `full` | all official features |

Capabilities with heavy or vendor-specific dependencies should live in companion crates:

- `rustrest-macros`: handler arguments and OpenAPI derives.
- `rustrest-session-redis`: distributed Redis session store.
- `rustrest-rate-limit-redis`: distributed rate limiter.
- `rustrest-opentelemetry`: traces and metrics export.

The monorepo conversion is deferred until the first companion crate is implemented. The core
design must not require that conversion prematurely.

### 3.2 Stability policy

Before `1.0`, every breaking release must include a migration section. At `1.0`:

- public behavior is part of the compatibility contract, not only type signatures;
- defaults must be safe for internet-facing services;
- feature flags use additive semantics;
- MSRV changes require a minor release and documentation;
- unstable extension points are explicitly marked rather than silently changed.

### 3.3 Non-goals

The following do not belong in the framework core:

- database ORM or migration system;
- template engine;
- outbound HTTP client;
- application configuration loader;
- JWT/OAuth/OIDC provider implementations;
- dependency injection container;
- background job runtime;
- Socket.IO protocol compatibility.

RustRest should expose enough primitives to integrate these concerns without owning them.

## 4. HTTP Request Pipeline

### Current issue

`App::handle` collects the entire incoming body before routing or middleware. This has several
consequences:

- middleware cannot reject unauthorized large requests before buffering the body;
- upload size is constrained by process memory;
- multipart parsing duplicates the buffered body into per-part buffers;
- backpressure from the application cannot flow to the network body;
- a single global limit is too coarse for JSON, forms and file uploads.

### Target design

`Request` should own a one-shot `RequestBody` abstraction rather than immutable `Bytes`.

```rust
pub struct RequestBody { /* private */ }

impl RequestBody {
    pub async fn collect(self, limit: usize) -> Result<Bytes, BodyError>;
    pub fn into_stream(self) -> BodyStream;
    pub fn size_hint(&self) -> SizeHint;
    pub fn is_end_stream(&self) -> bool;
}

impl Request {
    pub fn body(&self) -> &RequestBody;
    pub fn take_body(&mut self) -> RequestBody;
    pub async fn bytes(&mut self) -> Result<Bytes, HttpError>;
    pub async fn text(&mut self) -> Result<String, HttpError>;
    pub async fn json<T: DeserializeOwned>(&mut self) -> Result<T, HttpError>;
}
```

The framework must preserve convenience through extractors while enforcing one-shot ownership.
Buffered bodies may be cached inside `RequestBody` so multiple read-only extractors can reuse a
previous collection, but streaming and buffered consumption cannot both occur.

Limits must compose in this order:

1. server hard ceiling;
2. router or route body limit;
3. extractor-specific limit;
4. multipart field/file limits.

`Content-Length` above a known limit should be rejected before reading. Chunked bodies must stop
at the same limit while streaming.

### Required behavior

- Body errors distinguish overflow, client disconnect, invalid framing and already-consumed body.
- Request middleware runs before body collection.
- `Expect: 100-continue` is only accepted after routing and early middleware allow the request.
- Tests cover backpressure and cancellation when a handler stops reading.
- Existing buffered helpers remain available through a migration path in `0.3`.

## 5. Responses and HTTP Semantics

### Current issue

Response streams only support `Infallible`, invalid status/header builder inputs are silently
discarded or assumed impossible, and middleware can mutate semantically incompatible responses.
Compression and ETag implementations cover buffered bodies but not the complete HTTP negotiation
rules.

### Target design

`ResponseBody` becomes a public opaque body with fallible streaming and optional trailers.

```rust
pub struct ResponseBody { /* private */ }

impl Response {
    pub fn empty() -> Self;
    pub fn stream<S, E>(stream: S) -> Self
    where
        S: Stream<Item = Result<Bytes, E>> + Send + 'static,
        E: Into<BoxError>;

    pub fn with_trailers(self, trailers: HeaderMap) -> Self;
    pub fn try_status(self, status: u16) -> Result<Self, ResponseBuildError>;
    pub fn try_header(
        self,
        name: impl TryInto<HeaderName>,
        value: impl TryInto<HeaderValue>,
    ) -> Result<Self, ResponseBuildError>;
}
```

The fluent compatibility methods may remain, but they must have a documented failure policy and
must never silently omit security-sensitive headers.

HTTP middleware must implement:

- correct `Accept-Encoding` q-value and wildcard negotiation;
- `406` behavior when identity and all supported encodings are forbidden;
- compression exclusions for no-body statuses, already encoded bodies, ranges and incompressible
  media types;
- weak/strong ETag comparison according to request method;
- `If-Match`, `If-Unmodified-Since`, `If-Range` and precedence rules;
- removal of entity headers where RFC semantics require it;
- correct HEAD metadata without consuming a streaming body.

## 6. Server and Transport

### Current issue

The server has body, request and header-read limits, but production operators also need bounded
connections, handshake deadlines, protocol settings, drain control and observability. Plain and TLS
serve loops duplicate behavior and use direct console output.

### Target design

Introduce a public `ServerConfig` builder and a returned `ServerHandle`:

```rust
let server = app
    .server()
    .max_connections(20_000)
    .max_connections_per_ip(200)
    .http1_keep_alive(true)
    .http2_max_concurrent_streams(256)
    .tls_handshake_timeout(Duration::from_secs(10))
    .graceful_shutdown_timeout(Duration::from_secs(30))
    .listen("0.0.0.0:3000")
    .await?;

server.local_addr();
server.shutdown().await?;
server.stats();
```

Required controls:

- process and per-IP connection limits;
- accept backoff with error categories;
- HTTP/1 keep-alive and half-close controls;
- HTTP/2 stream/window/keepalive controls;
- TLS handshake timeout and concurrency limit;
- configurable graceful shutdown deadline;
- optional TCP_NODELAY and socket keepalive;
- service readiness separate from liveness;
- no internal `println!` or `eprintln!` requirement.

The plain and TLS listeners should share one connection-serving implementation. WebSocket shutdown
continues to integrate through the existing runtime without redesigning its public API.

## 7. Reverse Proxies and Client Identity

### Current issue

`Request::remote_addr` always reports the direct TCP peer. This is correct but insufficient behind
a reverse proxy. Naively trusting forwarded headers would create spoofing and rate-limit bypasses.

### Target design

Add an explicit trusted-proxy policy:

```rust
app.trusted_proxies(
    TrustedProxies::new()
        .allow("10.0.0.0/8".parse()?)
        .allow("192.168.0.0/16".parse()?),
);

req.peer_addr();       // direct socket peer
req.client_addr();     // resolved through trusted proxy chain
req.original_scheme(); // http/https after trusted Forwarded processing
req.original_host();
```

The parser must support RFC 7239 `Forwarded` and common `X-Forwarded-*` headers, reject malformed
chains, define precedence and walk from the trusted edge inward. Rate limiting, secure-cookie
decisions and tracing must use `client_addr`, never raw headers.

## 8. Routing and Route Contracts

### Improvements

1. Validate route definitions at registration time:
   - patterns start with `/`;
   - parameter names are non-empty and unique;
   - wildcard is terminal;
   - duplicate method/pattern registrations are errors or explicit replacements;
   - ambiguous parameter routes are reported.
2. Support arbitrary `http::Method` values through `route(method, path, handler)`.
3. Percent-decode captured parameters once, rejecting invalid percent encoding and encoded path
   separators according to a documented policy.
4. Add route names and reverse URL generation.
5. Add host constraints as an optional routing layer without slowing path-only routes.
6. Preserve deterministic specificity and expose conflicts through startup validation.

Proposed surface:

```rust
app.route(Method::CONNECT, "/tunnel/:id", handler)?;
app.get("/users/:id", show_user)
    .name("users.show")
    .body_limit(32 * 1024)
    .timeout(Duration::from_secs(2));

app.url_for("users.show", [("id", "42")])?;
```

Registration should return `Result<RouteHandle, RouteError>` in a breaking release. A temporary
`*_unchecked` path can ease migration, but silent invalid registration should not reach `1.0`.

## 9. Extraction and Validation

### Current issue

Extractors are synchronous because the whole body is already buffered. They do not validate media
types consistently, cannot expose structured field errors, and handlers always receive the entire
`Request` before manually extracting values.

### Target design

Split extraction into request-parts and body-consuming contracts:

```rust
pub trait FromRequestParts: Sized {
    type Rejection: IntoResponse;
    fn from_parts(parts: &mut RequestParts) -> impl Future<Output = Result<Self, Self::Rejection>> + Send;
}

pub trait FromRequest: Sized {
    type Rejection: IntoResponse;
    fn from_request(req: &mut Request) -> impl Future<Output = Result<Self, Self::Rejection>> + Send;
}
```

Built-in extractors should include:

- `Json<T>` with Content-Type enforcement and configurable limit;
- `Form<T>` and streaming `Multipart`;
- `Path<T>`, `Query<T>`, `TypedHeader<T>` and `CookieJar`;
- `Extension<T>` for middleware-injected request-local values;
- `ConnectInfo`, `MatchedPath`, `OriginalUri`, `Method` and `Version`;
- optional integration with validation crates without forcing one into the core.

`rustrest-macros` may later add ergonomic handler arguments:

```rust
async fn create_user(
    State(db): State<Database>,
    Json(input): Json<CreateUser>,
) -> Result<Created<Json<User>>, ApiError> {
    // application code
}
```

The trait contracts must exist before macros so the macro crate is only syntax sugar.

## 10. Error Model

### Current issue

`HttpError` contains only a numeric status and a message. It cannot carry a stable code, source,
headers, safe public details or request context. Panic responses and internal errors write directly
to stderr.

### Target design

```rust
pub struct HttpError {
    status: StatusCode,
    code: Cow<'static, str>,
    public_message: Cow<'static, str>,
    source: Option<BoxError>,
    headers: HeaderMap,
    extensions: Extensions,
}
```

Requirements:

- distinguish public message from internal source;
- preserve required headers such as `Allow`, `Retry-After` and authentication challenges;
- support RFC 9457 Problem Details JSON;
- route all framework rejections through one error renderer;
- give extractors typed rejection values;
- expose panic and internal-error hooks without leaking sensitive details;
- allow applications to attach stable error codes and correlation IDs.

## 11. Middleware Platform

### Improvements

The onion contract remains, but built-ins should move into focused modules and use explicit config
types. Required production middleware:

- request ID validation/generation with a configurable generator;
- structured trace context propagation;
- trusted-proxy client identity;
- concurrency limit and load shedding;
- configurable timeouts with cancellation semantics;
- body limits per scope;
- security headers (`nosniff`, frame policy, referrer policy, HSTS opt-in, CSP builder);
- CORS with private-network and exposed-header support;
- compression with complete negotiation;
- conditional request middleware;
- rate-limit trait with in-memory reference implementation;
- sensitive-header redaction.

Middleware state must use async-safe synchronization. `std::sync::Mutex` must not guard hot-path
maps that can be sharded or represented with atomics/concurrent maps.

## 12. Cookies and Sessions

### Current issue

Cookie parsing/rendering is hand-written and incomplete. Sessions use process-local `Mutex` state,
have no expiry, no ID rotation, no store trait and no protection against fixation beyond a signed
identifier.

### Target design

Cookies:

- strict parsing with quoted values and duplicate handling;
- `Expires`, prefixes (`__Host-`, `__Secure-`) and partitioned cookies;
- signed and authenticated-encrypted private jars;
- key rotation with current and previous keys;
- explicit secure-cookie policy behind trusted proxies.

Sessions:

```rust
pub trait SessionStore: Send + Sync + 'static {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionRecord>, SessionError>;
    async fn save(&self, record: SessionRecord) -> Result<(), SessionError>;
    async fn delete(&self, id: &SessionId) -> Result<(), SessionError>;
    async fn touch(&self, id: &SessionId, expires_at: SystemTime) -> Result<(), SessionError>;
}
```

Session middleware must support idle and absolute TTL, rolling expiry, regeneration after login,
invalidation after logout, dirty tracking, store-failure policy, maximum serialized size and
concurrent update semantics. The in-memory implementation remains a reference/testing store; Redis
belongs in a companion crate.

## 13. Multipart, Static Files and SSE

### Multipart

Replace the buffered hand-written parser with an incremental parser behind `multipart`:

- per-field, per-file and total limits;
- maximum part count and header size;
- streaming file reads;
- temporary-file helper with cleanup;
- filename sanitization guidance;
- nested multipart rejection unless explicitly enabled;
- clear distinction between malformed input and limit overflow.

### Static files

Move static serving into a configurable service:

```rust
StaticFiles::new("public")
    .index_file("index.html")
    .precompressed(true)
    .cache_control("public, max-age=3600")
    .fallback("index.html")
```

Add MIME detection through a maintained table, symlink policy, precompressed `.br`/`.gz` variants,
If-Range, configurable directory behavior and accurate stream read error reporting. Multi-range
responses are not required for `1.0`; unsupported multi-range requests must have documented
behavior.

### SSE

Add an `Sse` response type and connection lifecycle hooks:

- bounded event channel and backpressure policy;
- heartbeat validation;
- disconnect cancellation signal;
- graceful shutdown integration;
- event serialization errors surfaced rather than erased;
- metrics for open streams, events, bytes and disconnect reasons.

## 14. OpenAPI and API Documentation

### Current issue

The current OpenAPI document contains paths, methods, strings for path parameters and a generic 200
response. It cannot describe real request/response schemas or authentication.

### Target design

Move to OpenAPI 3.1 with typed metadata:

- operation IDs and route names;
- typed path, query, header and cookie parameters;
- request bodies and content types;
- response schemas by status;
- reusable components;
- security schemes and per-operation requirements;
- examples, deprecation and external docs;
- deterministic JSON/YAML generation;
- startup validation for duplicate operation IDs and unresolved references.

The core should define a `ToSchema` contract. Derives belong in `rustrest-macros`. Manual schema
registration remains supported so OpenAPI is not coupled to macros.

Swagger UI assets must not require a third-party CDN by default. The feature may embed pinned
assets or allow applications to supply their own documentation UI.

## 15. Observability

### Target design

Every production event should flow through hooks rather than direct console output.

```rust
pub trait ServerObserver: Send + Sync + 'static {
    fn on_accept_error(&self, event: &AcceptErrorEvent) {}
    fn on_request_start(&self, event: &RequestStartEvent) {}
    fn on_request_end(&self, event: &RequestEndEvent) {}
    fn on_body_error(&self, event: &BodyErrorEvent) {}
    fn on_panic(&self, event: &PanicEvent) {}
}
```

The tracing integration should emit low-cardinality route patterns, status class, protocol,
latency, request/response sizes and error category. It must support W3C `traceparent`/`tracestate`
propagation without requiring OpenTelemetry.

A metrics interface should expose counters, gauges and histograms for:

- accepted, active and rejected connections;
- active requests;
- request latency and status classes;
- body sizes and body failures;
- rate-limit and timeout outcomes;
- SSE streams;
- existing WebSocket runtime statistics through an adapter.

No payload, authorization value, cookie or unbounded raw path may be recorded by default.

## 16. Testing and Developer Experience

### Test client

The in-process client should support:

- persistent cookies;
- default headers;
- form and multipart builders;
- response JSON decoding;
- fallible response body collection;
- SSE stream testing;
- remote address, HTTP version and secure/proxy simulation;
- assertions for status, headers and JSON;
- an optional real-network harness for protocol tests.

### Validation matrix

Add:

- HTTP parser and multipart fuzz targets;
- property tests for routing and forwarded-header resolution;
- HTTP/1 and HTTP/2 protocol integration tests;
- slow-client, disconnect, cancellation and shutdown tests;
- server load smoke with latency/error thresholds;
- criterion benchmarks for route lookup, middleware overhead and extraction;
- Miri for pure modules where practical;
- `cargo-semver-checks`, `cargo-deny`, `cargo-audit` and docs warnings as CI gates.

### Documentation

Before `1.0`, publish:

- a guide organized by user tasks rather than one large README;
- production deployment guidance;
- reverse proxy examples;
- security model and defaults;
- middleware ordering rules;
- migration guides for every breaking release;
- complete examples for REST API, uploads, sessions, SSE, TLS and observability.

## 17. Delivery Sequence

### `0.3.0`: HTTP body and error foundation

- request streaming and per-route limits;
- fallible response bodies;
- structured errors and rejections;
- asynchronous extractor contracts;
- compatibility migration for buffered APIs.

This release is foundational and intentionally breaking.

### `0.4.0`: server, proxy and security

- unified server builder and handle;
- connection/protocol/TLS controls;
- trusted proxies and client identity;
- production middleware modules;
- complete HTTP negotiation and conditional requests.

### `0.5.0`: stateful and documented APIs

- cookie jars and key rotation;
- async session store and TTL semantics;
- streaming multipart and configurable static service;
- OpenAPI 3.1 contracts;
- managed SSE response.

### `0.6.0`: observability and developer tooling

- observer and metrics contracts;
- tracing propagation;
- expanded test client;
- HTTP fuzzing, benchmarks and load gates;
- feature reorganization and companion-crate scaffolding.

### `1.0.0`: stabilization

- API and behavior audit;
- semver report against the final release candidate;
- security review;
- production examples and migration guides;
- documented support and MSRV policy;
- no known production blocker in the acceptance matrix.

## 18. Definition of Production-Ready

RustRest reaches the intended `10/10` state when all of these are true:

1. Untrusted clients cannot force unbounded memory, tasks, connections or state growth using the
   default production profile.
2. Request and response streaming propagate backpressure and cancellation.
3. HTTP behavior is covered by protocol-level tests, not only unit tests.
4. Reverse-proxy trust is explicit and spoof-resistant.
5. Stateful middleware can use external stores without replacing framework internals.
6. Operators can observe errors, latency, load and shutdown without parsing console strings.
7. OpenAPI describes actual request and response contracts.
8. Feature flags keep optional dependency cost out of the default build.
9. Breaking behavior is documented and guarded by semver tooling.
10. Every release phase is independently usable, tested and documented.
