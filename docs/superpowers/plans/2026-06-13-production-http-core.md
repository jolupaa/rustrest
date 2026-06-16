# RustRest Production HTTP Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver RustRest `0.3.0` with streaming request bodies, composable body limits, fallible response streams, structured errors and asynchronous extractor contracts.

**Architecture:** Introduce explicit `RequestBody` and `ResponseBody` types at the transport boundary. Route and middleware resolution happens before request body consumption, while extractors own buffering policy. Replace the string-only error path with typed rejections rendered by one application error renderer.

**Tech Stack:** Hyper `Incoming`, `http-body`/`http-body-util`, Tokio, Bytes, futures-util, Serde, RFC 9457 Problem Details.

---

## File Map

| Path | Responsibility |
| --- | --- |
| `src/app/body.rs` | request body state, streaming, collection and limit errors |
| `src/app/error.rs` | structured `HttpError`, problem details and rejection conversion |
| `src/app/request.rs` | request metadata and one-shot body ownership |
| `src/app/response.rs` | fallible response bodies, trailers and checked builders |
| `src/app/extract.rs` | asynchronous parts/body extractor contracts |
| `src/app/router.rs` | per-route body limit metadata |
| `src/app/server.rs` | early dispatch, 100-continue and transport body wiring |
| `src/app/middleware/compression.rs` | complete content-encoding negotiation |
| `src/app/middleware/conditional.rs` | validators and precondition handling |
| `tests/http_body_integration.rs` | real-network body, cancellation and Expect tests |
| `tests/http_semantics.rs` | compression and conditional request protocol tests |
| `tests/routing_contract.rs` | route validation, custom methods, host matching and URL generation |
| `docs/migrations/0.2-to-0.3.md` | breaking API migration |

### Task 1: Introduce Structured HTTP Errors

**Files:**
- Modify: `src/app/error.rs`
- Modify: `src/app/response.rs`
- Modify: `src/app/server.rs`
- Modify: `src/app/handler.rs`
- Modify: `src/app/tests.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write failing tests for codes, headers and public/internal separation**

Add to `src/app/tests.rs`:

```rust
#[test]
fn http_error_preserves_code_headers_and_private_source() {
    let error = HttpError::new(StatusCode::TOO_MANY_REQUESTS, "rate_limited", "Demasiadas solicitudes")
        .header(RETRY_AFTER, HeaderValue::from_static("30"))
        .with_source(std::io::Error::other("redis unavailable"));

    assert_eq!(error.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(error.code(), "rate_limited");
    assert_eq!(error.public_message(), "Demasiadas solicitudes");
    assert_eq!(error.headers().get(RETRY_AFTER).unwrap(), "30");
    assert_eq!(error.source().unwrap().to_string(), "redis unavailable");
}

#[test]
fn problem_details_never_exposes_internal_source() {
    let response = Response::from_error(
        HttpError::internal_server_error("Error interno")
            .with_source(std::io::Error::other("database password leaked")),
    );
    let json: serde_json::Value = serde_json::from_slice(response.body_bytes().unwrap()).unwrap();

    assert_eq!(json["status"], 500);
    assert_eq!(json["code"], "internal_server_error");
    assert_eq!(json["detail"], "Error interno");
    assert!(!response.body_text().contains("database password"));
}
```

- [ ] **Step 2: Run the tests and verify failure**

Run:

```bash
cargo test http_error_preserves_code_headers_and_private_source
cargo test problem_details_never_exposes_internal_source
```

Expected: compilation fails because the structured constructor and accessors do not exist.

- [ ] **Step 3: Replace the error representation**

Implement in `src/app/error.rs`:

```rust
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub struct HttpError {
    status: hyper::StatusCode,
    code: std::borrow::Cow<'static, str>,
    public_message: std::borrow::Cow<'static, str>,
    source: Option<BoxError>,
    headers: hyper::HeaderMap,
}

impl HttpError {
    pub fn new(
        status: hyper::StatusCode,
        code: impl Into<std::borrow::Cow<'static, str>>,
        public_message: impl Into<std::borrow::Cow<'static, str>>,
    ) -> Self {
        Self {
            status,
            code: code.into(),
            public_message: public_message.into(),
            source: None,
            headers: hyper::HeaderMap::new(),
        }
    }

    pub fn header(mut self, name: hyper::header::HeaderName, value: hyper::HeaderValue) -> Self {
        self.headers.insert(name, value);
        self
    }

    pub fn with_source(mut self, source: impl Into<BoxError>) -> Self {
        self.source = Some(source.into());
        self
    }

    pub fn status(&self) -> hyper::StatusCode { self.status }
    pub fn code(&self) -> &str { &self.code }
    pub fn public_message(&self) -> &str { &self.public_message }
    pub fn headers(&self) -> &hyper::HeaderMap { &self.headers }
    pub fn source(&self) -> Option<&(dyn std::error::Error + Send + Sync + 'static)> {
        self.source.as_deref()
    }
}
```

Retain convenience constructors using stable codes such as `bad_request`, `unauthorized`,
`forbidden`, `not_found`, `payload_too_large`, `request_timeout` and `internal_server_error`.

- [ ] **Step 4: Render RFC 9457-compatible JSON and preserve error headers**

Change `Response::from_error` in `src/app/response.rs` to serialize:

```rust
let body = serde_json::json!({
    "type": format!("about:blank#{}", error.code()),
    "title": error.status().canonical_reason().unwrap_or("HTTP Error"),
    "status": error.status().as_u16(),
    "detail": error.public_message(),
    "code": error.code(),
});

let mut response = Response::json(&body).status(error.status().as_u16());
for (name, value) in error.headers() {
    response.headers.append(name, value.clone());
}
response.error = Some(error);
response
```

- [ ] **Step 5: Route framework failures through the same renderer**

Replace numeric `HttpError::new` calls in `server.rs`, `handler.rs`, `extract.rs`, `form.rs` and
middleware with the named constructors. Ensure 405 keeps `Allow`, 429 keeps `Retry-After`, and 401
can keep `WWW-Authenticate`.

- [ ] **Step 6: Run focused and full tests**

Run:

```bash
cargo test http_error
cargo test problem_details
cargo test
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/app/error.rs src/app/response.rs src/app/server.rs src/app/handler.rs src/app/extract.rs src/app/form.rs src/app/middleware.rs src/app/tests.rs src/lib.rs
git commit -m "feat: add structured HTTP errors"
```

### Task 2: Add a Streaming Request Body Abstraction

**Files:**
- Create: `src/app/body.rs`
- Modify: `src/app.rs`
- Modify: `src/app/request.rs`
- Modify: `src/app/server.rs`
- Modify: `src/app/testing.rs`
- Create: `tests/http_body_integration.rs`

- [ ] **Step 1: Write a real-network test proving middleware runs before body collection**

Create `tests/http_body_integration.rs` with a raw TCP request that declares a large
`Content-Length`, sends headers only, and expects an authorization middleware to return `401`
without waiting for the body:

```rust
#[tokio::test]
async fn middleware_can_reject_before_request_body_is_sent() {
    let mut app = App::new();
    app.layer(|req: Request, next: Next| async move {
        if req.header("authorization").is_none() {
            return Response::from_error(HttpError::unauthorized("Falta autenticación"));
        }
        next(req).await
    });
    app.post("/upload", |mut req: Request| async move {
        let body = req.bytes().await?;
        Ok::<_, HttpError>(Response::send(&body.len().to_string()))
    });

    let (addr, shutdown, server) = spawn_app(app).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(
        b"POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1048576\r\n\r\n",
    ).await.unwrap();

    let response = read_headers(&mut stream).await;
    assert!(response.starts_with("HTTP/1.1 401"));
    shutdown.send(()).unwrap();
    server.await.unwrap().unwrap();
}
```

Define `spawn_app` and `read_headers` in the same test file using `TcpListener`, a oneshot shutdown
channel and a bounded read timeout.

- [ ] **Step 2: Run the test and verify the current server waits for the body**

Run:

```bash
cargo test --test http_body_integration middleware_can_reject_before_request_body_is_sent -- --nocapture
```

Expected: test times out or fails because `App::handle` collects the body before middleware.

- [ ] **Step 3: Implement `RequestBody`**

Create `src/app/body.rs`:

```rust
use std::pin::Pin;
use futures_util::{Stream, StreamExt};
use hyper::body::{Body as _, Bytes, Frame, Incoming, SizeHint};

use super::{BoxError, HttpError};

pub type BodyStream = Pin<Box<dyn Stream<Item = Result<Bytes, BoxError>> + Send>>;

pub struct RequestBody {
    state: BodyState,
    default_limit: usize,
}

enum BodyState {
    Incoming(Incoming),
    Buffered(Bytes),
    Stream(BodyStream),
    Taken,
}

impl RequestBody {
    pub(crate) fn incoming(body: Incoming, default_limit: usize) -> Self {
        Self { state: BodyState::Incoming(body), default_limit }
    }

    pub(crate) fn buffered(body: Bytes, default_limit: usize) -> Self {
        Self { state: BodyState::Buffered(body), default_limit }
    }

    pub fn size_hint(&self) -> SizeHint {
        match &self.state {
            BodyState::Incoming(body) => body.size_hint(),
            BodyState::Buffered(bytes) => SizeHint::with_exact(bytes.len() as u64),
            BodyState::Stream(_) | BodyState::Taken => SizeHint::default(),
        }
    }

    pub async fn collect(&mut self, limit: usize) -> Result<Bytes, HttpError> {
        if let BodyState::Buffered(bytes) = &self.state {
            if bytes.len() > limit { return Err(HttpError::payload_too_large(limit)); }
            return Ok(bytes.clone());
        }
        let mut stream = self.take_stream()?;
        let mut output = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(HttpError::body_read)?;
            if output.len().saturating_add(chunk.len()) > limit {
                return Err(HttpError::payload_too_large(limit));
            }
            output.extend_from_slice(&chunk);
        }
        let bytes = Bytes::from(output);
        self.state = BodyState::Buffered(bytes.clone());
        Ok(bytes)
    }

    pub async fn collect_default(&mut self) -> Result<Bytes, HttpError> {
        self.collect(self.default_limit).await
    }

    pub fn take_stream(&mut self) -> Result<BodyStream, HttpError> {
        let state = std::mem::replace(&mut self.state, BodyState::Taken);
        match state {
            BodyState::Incoming(body) => Ok(Box::pin(body.into_data_stream().map(|item| item.map_err(Into::into)))),
            BodyState::Buffered(bytes) => Ok(Box::pin(futures_util::stream::once(async move { Ok(bytes) }))),
            BodyState::Stream(stream) => Ok(stream),
            BodyState::Taken => Err(HttpError::body_already_consumed()),
        }
    }
}
```

Keep a separate constructor for test streams if `Incoming` cannot be constructed by downstream
tests.

- [ ] **Step 4: Move incoming body ownership into `Request`**

Change `Request` to contain:

```rust
pub(crate) body: RequestBody,
pub(crate) body_limit: usize,
```

and add:

```rust
pub fn body(&self) -> &RequestBody { &self.body }
pub fn body_mut(&mut self) -> &mut RequestBody { &mut self.body }
pub fn take_body_stream(&mut self) -> Result<BodyStream, HttpError> { self.body.take_stream() }
pub async fn bytes(&mut self) -> Result<Bytes, HttpError> { self.body.collect(self.body_limit).await }
pub async fn text(&mut self) -> Result<String, HttpError> {
    String::from_utf8(self.bytes().await?.to_vec())
        .map_err(|error| HttpError::invalid_utf8().with_source(error))
}
pub async fn json<T: DeserializeOwned>(&mut self) -> Result<T, HttpError> {
    serde_json::from_slice(&self.bytes().await?)
        .map_err(|error| HttpError::invalid_json().with_source(error))
}
```

- [ ] **Step 5: Stop collecting in `App::handle`**

In `server.rs`, split the request into parts and body, build routing metadata from the parts, and
construct `RequestBody::incoming(body, self.config.max_body_size)`. `run_request` must receive the
request immediately. Remove `Limited::collect()` from `App::handle`.

- [ ] **Step 6: Adapt `RequestBuilder` and `TestClient`**

Use `RequestBody::buffered` in `RequestBuilder::build`. Keep TestClient's early configured-limit
check so its behavior matches the transport for known buffered bodies.

- [ ] **Step 7: Run body tests**

Run:

```bash
cargo test --test http_body_integration
cargo test request_body
cargo test
```

Expected: middleware rejection is immediate and all existing tests compile after awaiting body
helpers.

- [ ] **Step 8: Commit**

```bash
git add src/app/body.rs src/app.rs src/app/request.rs src/app/server.rs src/app/testing.rs src/app/tests.rs tests/http_body_integration.rs
git commit -m "feat: stream request bodies through handlers"
```

### Task 3: Add Hierarchical Body Limits and Expect Handling

**Files:**
- Modify: `src/app/router.rs`
- Modify: `src/app/server.rs`
- Modify: `src/app/request.rs`
- Modify: `src/app/body.rs`
- Modify: `tests/http_body_integration.rs`

- [ ] **Step 1: Add failing tests for route limits**

Add tests proving `/json` rejects 2 KiB with a 1 KiB route limit while `/upload` accepts the same
body under a 1 MiB limit:

```rust
#[tokio::test]
async fn route_body_limit_overrides_the_server_default() {
    let mut app = App::new();
    app.max_body_size(1024 * 1024);
    app.post("/json", |mut req: Request| async move {
        req.bytes().await.map(|body| Response::send(&body.len().to_string()))
    }).body_limit(1024);
    app.post("/upload", |mut req: Request| async move {
        req.bytes().await.map(|body| Response::send(&body.len().to_string()))
    }).body_limit(1024 * 1024);

    let client = TestClient::new(app);
    assert_eq!(client.post("/json").body(vec![0; 2048]).send().await.status, 413);
    assert_eq!(client.post("/upload").body(vec![0; 2048]).send().await.status, 200);
}
```

- [ ] **Step 2: Verify failure**

Run `cargo test route_body_limit_overrides_the_server_default`.

Expected: compilation fails because `RouteHandle::body_limit` does not exist.

- [ ] **Step 3: Store route transport policy**

Add to `RouteMeta`:

```rust
body_limit: Option<usize>,
```

Add to `RouteHandle`:

```rust
pub fn body_limit(self, bytes: usize) -> Self {
    self.router.routes[self.index].meta.body_limit = Some(bytes);
    self
}

pub fn timeout(self, timeout: Duration) -> Self {
    self.router.routes[self.index]
        .middlewares
        .push(super::middleware::timeout(timeout));
    self
}
```

Carry the resolved values in `MatchedRoute` and assign the effective body limit before middleware
runs.

- [ ] **Step 4: Reject known oversize bodies from Content-Length**

Before invoking middleware, compare a valid singleton `Content-Length` against the effective limit.
Return `HttpError::payload_too_large(limit)` without polling the body. Reject conflicting or invalid
Content-Length values as `400 invalid_content_length`.

- [ ] **Step 5: Verify lazy `Expect: 100-continue` handling**

Keep Hyper's incoming body unpolled until a handler or extractor requests data. Hyper sends
`100 Continue` when the body is first polled; an early middleware response therefore sends only the
final rejection. Add a TCP test that verifies no interim `100` appears for an unauthorized request
and that an authorized request receives `100` before the client sends its body. Do not add a second
framework-level continue mechanism unless the protocol test proves Hyper cannot provide this
behavior.

- [ ] **Step 6: Verify limits and protocol behavior**

Run:

```bash
cargo test route_body_limit
cargo test --test http_body_integration expect_continue
cargo test --test http_body_integration oversized
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/app/router.rs src/app/server.rs src/app/request.rs src/app/body.rs src/app/tests.rs tests/http_body_integration.rs
git commit -m "feat: enforce route body limits before collection"
```

### Task 4: Make Route Registration Validated and Reversible

**Files:**
- Modify: `src/app/router.rs`
- Modify: `src/app/trie.rs`
- Modify: `src/app/server.rs`
- Modify: `src/app/request.rs`
- Modify: `src/app/tests.rs`
- Modify: `Cargo.toml`
- Create: `tests/routing_contract.rs`

- [ ] **Step 1: Write failing registration and URL-generation tests**

Create `tests/routing_contract.rs`:

```rust
fn handler(_req: Request) -> Response {
    Response::send("ok")
}

#[test]
fn invalid_and_duplicate_routes_are_rejected_at_registration() {
    let mut router = Router::new();
    assert_eq!(router.get("users/:id", handler).unwrap_err().kind(), RouteErrorKind::MissingLeadingSlash);
    assert_eq!(router.get("/users/:id/:id", handler).unwrap_err().kind(), RouteErrorKind::DuplicateParameter);
    assert_eq!(router.get("/files/*path/more", handler).unwrap_err().kind(), RouteErrorKind::NonTerminalWildcard);

    router.get("/users/:id", handler).unwrap();
    assert_eq!(router.get("/users/:name", handler).unwrap_err().kind(), RouteErrorKind::ConflictingPattern);
}

#[test]
fn named_route_generates_an_encoded_url() {
    let mut app = App::new();
    app.get("/users/:id/files/*path", handler).unwrap().name("users.file").unwrap();

    let url = app.url_for(
        "users.file",
        [("id", "José / admin"), ("path", "reports/June 2026.pdf")],
    ).unwrap();
    assert_eq!(url, "/users/Jos%C3%A9%20%2F%20admin/files/reports/June%202026.pdf");
}

#[test]
fn custom_method_and_host_constraints_are_routed() {
    let mut router = Router::new();
    router.route(Method::CONNECT, "/tunnel/:id", handler).unwrap()
        .host("api.example.com").unwrap();

    assert!(router.resolve(&Method::CONNECT, "/tunnel/7", Some("api.example.com")).unwrap().is_some());
    assert!(router.resolve(&Method::CONNECT, "/tunnel/7", Some("www.example.com")).unwrap().is_none());
}
```

- [ ] **Step 2: Verify failure**

Run `cargo test --test routing_contract`.

Expected: route methods do not return `Result`, names/hosts/custom methods are unavailable and URL
generation does not exist.

- [ ] **Step 3: Define validated route pattern types**

Add `percent-encoding = "2"` to core dependencies. Replace free-form parsed vectors at registration
with:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RoutePattern {
    rendered: String,
    segments: Vec<Segment>,
    parameter_names: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteErrorKind {
    MissingLeadingSlash,
    EmptyParameter,
    DuplicateParameter,
    NonTerminalWildcard,
    DuplicateRoute,
    ConflictingPattern,
    DuplicateName,
    InvalidHostPattern,
    MissingUrlParameter,
    UnexpectedUrlParameter,
    InvalidPercentEncoding,
}
```

`RoutePattern::parse` must reject invalid patterns and render one canonical slash form. Conflict
keys treat parameter names as equivalent, so `/users/:id` conflicts with `/users/:name` for the
same method and host constraint.

- [ ] **Step 4: Return `Result` from registration**

Add the general method and convert helpers:

```rust
pub fn route<H, M>(
    &mut self,
    method: Method,
    path: &str,
    handler: H,
) -> Result<RouteHandle<'_>, RouteError>
where
    H: IntoHandler<M>;
```

`get`, `post`, `put`, `delete`, `patch`, `options`, `head`, `all`, `websocket` and mount validation
return `Result`. Update examples/tests to use `?` in constructors returning `Result` or `.unwrap()`
inside tests. Registration must not partially mutate the router on failure.

- [ ] **Step 5: Decode captured parameters strictly**

Decode each parameter with `percent_encoding::percent_decode_str(...).decode_utf8()`. Reject invalid
UTF-8 and malformed percent triplets. For normal `:param`, keep encoded `/` as data rather than a
path separator. For `*wildcard`, decode each captured segment separately and join with `/` so URL
generation can preserve hierarchy.

Change resolution to return `Result<Option<MatchedRoute>, RouteMatchError>`; map invalid request path
encoding to structured `400 invalid_path_encoding`.

- [ ] **Step 6: Add route names and URL generation**

Store a unique optional route name. `RouteHandle::name` validates uniqueness and returns
`Result<Self, RouteError>`. `App::url_for` and `Router::url_for` require exactly the named parameters,
percent-encode normal parameters as path segments and encode wildcard values segment-by-segment.

- [ ] **Step 7: Add optional host constraints**

Define `HostPattern::Exact` and `HostPattern::WildcardSubdomain`. Normalize DNS names to lowercase,
strip a valid request port, reject userinfo/control characters and ensure wildcard matching respects
label boundaries. Extend the route index to return ordered path/method candidates, then select the
first candidate whose host constraint matches. Path-only routes retain the current fast path.

- [ ] **Step 8: Verify**

Run:

```bash
cargo test --test routing_contract
cargo test router
cargo test --all-features
```

Expected: all pass, including existing specificity and WebSocket route tests.

- [ ] **Step 9: Commit**

```bash
git add Cargo.toml Cargo.lock src/app/router.rs src/app/trie.rs src/app/server.rs src/app/request.rs src/app/tests.rs tests/routing_contract.rs examples README.md
git commit -m "feat: validate and name route contracts"
```

### Task 5: Replace Synchronous Extractors with Async Contracts

**Files:**
- Modify: `src/app/extract.rs`
- Modify: `src/app/form.rs`
- Modify: `src/app/handler.rs`
- Modify: `src/app/request.rs`
- Modify: `src/app/tests.rs`
- Modify: `examples/api.rs`
- Modify: `Cargo.toml`

- [ ] **Step 1: Write failing extractor tests**

Add tests for media-type validation, a consumed body and a request-parts extractor:

```rust
#[tokio::test]
async fn json_extractor_requires_json_content_type() {
    let mut req = Request::builder().method("POST").body(br#"{"name":"Ada"}"#.to_vec()).build();
    let error = Json::<CreateUser>::from_request(&mut req).await.unwrap_err();
    assert_eq!(error.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(error.code(), "unsupported_media_type");
}

#[tokio::test]
async fn matched_path_extracts_without_consuming_the_body() {
    let mut req = Request::builder().path("/users/42").matched_path("/users/:id").build();
    let MatchedPath(path) = MatchedPath::from_request_parts(req.parts_mut()).await.unwrap();
    assert_eq!(path, "/users/:id");
}
```

- [ ] **Step 2: Verify failure**

Run:

```bash
cargo test json_extractor_requires_json_content_type
cargo test matched_path_extracts_without_consuming_the_body
```

Expected: compilation fails because async extractor traits and `MatchedPath` do not exist.

- [ ] **Step 3: Define parts and body extractor traits**

In `src/app/extract.rs`:

```rust
pub trait FromRequestParts: Sized {
    type Rejection: IntoResponse + Send;
    fn from_request_parts(
        parts: &mut RequestParts,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send;
}

pub trait FromRequest: Sized {
    type Rejection: IntoResponse + Send;
    fn from_request(
        req: &mut Request,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send;
}
```

Define `RequestParts` with method, URI/path, version, headers, params, state, peer/client address,
secure flag, route pattern and request extensions. Do not expose body ownership through this type.

- [ ] **Step 4: Implement built-in extractors**

Add `headers = "0.4"` to core dependencies so typed headers use maintained parsing rules rather
than ad hoc string splitting.

Implement:

```rust
pub struct MatchedPath(pub String);
pub struct OriginalUri(pub hyper::Uri);
pub struct ConnectInfo(pub Option<std::net::SocketAddr>);
pub struct Extension<T>(pub T);
pub struct TypedHeader<T: headers::Header>(pub T);
```

Convert `Path<T>`, `Query<T>`, `State<T>`, `Headers<T>`, `Cookies<T>`, `TypedHeader<T>`, method and
version to `FromRequestParts`. Convert `Json<T>`, `Form<T>`, `Bytes` and `String` to `FromRequest`.
Typed-header rejection must distinguish a missing header from an invalid header value and preserve
authentication challenge headers when the extractor type supplies them.

For JSON, accept `application/json` and `application/*+json`; otherwise return `415`. Deserialize
from `req.bytes().await?` and preserve the Serde source on the rejection.

- [ ] **Step 5: Keep a convenience method on Request**

Add:

```rust
pub async fn extract<E>(&mut self) -> Result<E, E::Rejection>
where
    E: FromRequest,
{
    E::from_request(self).await
}
```

Add `extract_parts` for `FromRequestParts`.

- [ ] **Step 6: Add typed handler arguments without requiring procedural macros**

Extend `IntoHandler` with marker types generated for arities one through eight. Support either all
`FromRequestParts` arguments or parts arguments followed by exactly one body-consuming
`FromRequest` argument. Extraction happens left-to-right; a rejection is converted directly into a
response through `IntoResponse`.

The public result should compile:

```rust
async fn create_user(
    State(db): State<Database>,
    Path(team_id): Path<u64>,
    Json(input): Json<CreateUser>,
) -> Result<Response, HttpError> {
    let user = db.create(team_id, input).await?;
    Ok(Response::json(&user).status(201))
}

app.post("/teams/:team_id/users", create_user)?;
```

Implement the repetitive trait impls with a private declarative macro in `handler.rs`; do not make
users annotate handlers. Add compile tests for 1, 2, 4 and 8 arguments and a compile-fail test for a
body extractor followed by a parts extractor.

- [ ] **Step 7: Update examples and tests**

Change calls from `req.extract::<Json<T>>()?` to `req.extract::<Json<T>>().await?` inside async
handlers. Synchronous handlers that consume a body must become async; parts-only handlers may remain
synchronous by reading existing request accessors.

- [ ] **Step 8: Run extractor tests**

Run:

```bash
cargo test extractor
cargo test --example api
cargo test
```

Expected: all tests and examples compile and pass.

- [ ] **Step 9: Commit**

```bash
git add Cargo.toml Cargo.lock src/app/extract.rs src/app/form.rs src/app/handler.rs src/app/request.rs src/app/tests.rs examples/api.rs README.md
git commit -m "feat: add asynchronous request extractors"
```

### Task 6: Support Fallible Response Streams and Trailers

**Files:**
- Modify: `src/app/response.rs`
- Modify: `src/app/server.rs`
- Modify: `src/app/router.rs`
- Modify: `src/app/sse.rs`
- Modify: `src/app/tests.rs`

- [ ] **Step 1: Add failing tests for stream errors and trailers**

Add unit tests to `src/app/tests.rs`, where crate-private body conversion is available:

```rust
#[tokio::test]
async fn fallible_stream_closes_the_body_after_the_error() {
    let stream = stream::iter(vec![
        Ok::<_, std::io::Error>(Bytes::from_static(b"first")),
        Err(std::io::Error::other("read failed")),
    ]);
    let response = Response::stream(stream);
    let result = response.into_hyper().into_body().collect().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn response_can_emit_http_trailers() {
    let mut trailers = HeaderMap::new();
    trailers.insert("x-checksum", HeaderValue::from_static("abc"));
    let response = Response::stream(stream::once(async { Ok::<_, std::io::Error>(Bytes::from_static(b"data")) }))
        .with_trailers(trailers);
    let collected = response.into_hyper().into_body().collect().await.unwrap();
    assert_eq!(collected.trailers().unwrap().get("x-checksum").unwrap(), "abc");
}
```

- [ ] **Step 2: Verify failure**

Run `cargo test fallible_stream_closes_the_body_after_the_error`.

Expected: compilation fails because streams require `Infallible` and trailers are unsupported.

- [ ] **Step 3: Implement a boxed fallible body**

Change the internal body alias to:

```rust
pub(crate) type ResponseBody = http_body_util::combinators::UnsyncBoxBody<Bytes, BoxError>;
```

Make `Response::stream` generic over an error convertible to `BoxError`:

```rust
pub fn stream<S, E>(stream: S) -> Self
where
    S: Stream<Item = Result<Bytes, E>> + Send + 'static,
    E: Into<BoxError> + 'static,
{
    let frames = stream.map(|item| item.map(Frame::data).map_err(Into::into));
    Self::from_frame_stream(frames)
}
```

Map `Full` and `Empty` `Infallible` errors with `map_err(|never| match never {})`.

- [ ] **Step 4: Append trailers as the final frame**

Store optional trailers in `Response`. In `into_hyper`, chain one final
`Frame::trailers(trailers)` after the data stream. Reject pseudo-headers and invalid trailer names
through `ResponseBuildError`.

- [ ] **Step 5: Add checked response builders**

Implement:

```rust
pub fn try_status(mut self, status: u16) -> Result<Self, ResponseBuildError> {
    self.status = StatusCode::from_u16(status).map_err(ResponseBuildError::invalid_status)?;
    Ok(self)
}

pub fn try_header(
    mut self,
    name: impl TryInto<HeaderName>,
    value: impl TryInto<HeaderValue>,
) -> Result<Self, ResponseBuildError> {
    self.headers.insert(
        name.try_into().map_err(|_| ResponseBuildError::invalid_header_name())?,
        value.try_into().map_err(|_| ResponseBuildError::invalid_header_value())?,
    );
    Ok(self)
}
```

Keep fluent `status`/`header` wrappers deprecated through `0.3`; when conversion fails, store a
`ResponseBuildError` inside the response and render a structured 500 at the transport boundary
rather than silently dropping the invalid value.

- [ ] **Step 6: Propagate file and SSE serialization errors**

Change `file_stream` to yield `std::io::Result<Bytes>` rather than silently ending on read failure.
Make SSE format/serialization failures map to a concrete `SseError` and terminate the response body
with that error.

- [ ] **Step 7: Verify**

Run:

```bash
cargo test fallible_stream
cargo test response_can_emit_http_trailers
cargo test static_files
cargo test sse
cargo test
```

Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add src/app/response.rs src/app/server.rs src/app/router.rs src/app/sse.rs src/app/tests.rs
git commit -m "feat: support fallible response streams"
```

### Task 7: Correct Compression and Conditional Request Semantics

**Files:**
- Create: `src/app/middleware/mod.rs`
- Create: `src/app/middleware/compression.rs`
- Create: `src/app/middleware/conditional.rs`
- Move from: `src/app/middleware.rs`
- Modify: `src/app.rs`
- Create: `tests/http_semantics.rs`

- [ ] **Step 1: Add protocol table tests**

Create table-driven tests covering:

```rust
let cases = [
    ("gzip;q=0.4, deflate;q=0.8", Some("deflate"), 200),
    ("br;q=0, gzip;q=1", Some("gzip"), 200),
    ("identity;q=0, *;q=0", None, 406),
    ("*;q=0.5", Some("gzip"), 200),
];
```

Also test that compression skips `204`, `304`, `206`, `image/png`, responses with
`Cache-Control: no-transform`, and existing `Content-Encoding`.

For conditionals, test `If-Match`, `If-None-Match`, `If-Unmodified-Since`, `If-Modified-Since` and
`If-Range` precedence against buffered responses and static files.

- [ ] **Step 2: Verify current failures**

Run `cargo test --test http_semantics`.

Expected: q-value ordering, wildcard/406 and precondition tests fail.

- [ ] **Step 3: Split middleware by responsibility**

Convert `src/app/middleware.rs` into `src/app/middleware/mod.rs` and move compression and ETag logic
to the new focused files. Re-export the existing function names from `mod.rs` to preserve imports.

- [ ] **Step 4: Implement weighted encoding negotiation**

Parse each coding and `q` value into:

```rust
struct CodingPreference {
    coding: Coding,
    quality: u16,
    client_order: usize,
}
```

Represent quality as an integer from `0` to `1000`; reject malformed values by treating that token
as unacceptable. Choose highest quality, then server preference, then client order. Return a 406
error only when identity and every supported coding have quality zero.

- [ ] **Step 5: Implement precondition evaluation in RFC order**

Create:

```rust
pub(crate) enum PreconditionResult {
    Proceed,
    NotModified,
    Failed,
    IgnoreRange,
}
```

Evaluate `If-Match` and `If-Unmodified-Since` before cache validators. Use strong comparison for
`If-Match`, weak comparison for GET/HEAD `If-None-Match`, and strong validator/date logic for
`If-Range`. Map `Failed` to 412 and `NotModified` to 304 with no body.

- [ ] **Step 6: Verify**

Run:

```bash
cargo test --test http_semantics
cargo test compression
cargo test static_files
cargo test --all-features
```

Expected: all pass for default and Brotli builds.

- [ ] **Step 7: Commit**

```bash
git add src/app/middleware src/app.rs src/app/router.rs src/app/tests.rs tests/http_semantics.rs
git commit -m "fix: implement HTTP content negotiation and preconditions"
```

### Task 8: Publish the `0.3.0` Migration and Release

**Files:**
- Modify: `Cargo.toml`
- Modify: `README.md`
- Modify: `src/lib.rs`
- Modify: `CHANGELOG.md`
- Create: `docs/migrations/0.2-to-0.3.md`
- Create: `examples/streaming_upload.rs`

- [ ] **Step 1: Write the migration guide**

Document exact before/after conversions:

```rust
// 0.2
let Json(input) = req.extract::<Json<CreateUser>>()?;

// 0.3
let Json(input) = req.extract::<Json<CreateUser>>().await?;
```

```rust
// 0.2
let bytes = req.bytes();

// 0.3
let bytes = req.bytes().await?;
```

Explain structured status codes, JSON problem responses, route body limits and fallible streams.

- [ ] **Step 2: Add a compiling streaming upload example**

Create `examples/streaming_upload.rs` with a route that consumes `req.take_body_stream()?`, writes
chunks to a temporary file using `tokio::io::AsyncWriteExt`, enforces an explicit byte count and
removes the partial file on stream error.

- [ ] **Step 3: Reorganize feature flags without changing defaults yet**

Add named `compression`, `multipart`, `static-files`, `sse`, `websocket`, `openapi`, `sessions`,
`metrics` and `full` features. During `0.3`, keep current capabilities enabled as needed for source
compatibility; the final default-feature reduction occurs in the stabilization plan.

- [ ] **Step 4: Bump version and changelog**

Set `version = "0.3.0"` and move relevant Unreleased entries under:

Create the release heading with the execution date obtained from `date +%F`; do not reuse the plan
creation date unless the release is actually performed that day.

- [ ] **Step 5: Run the complete release gate**

```bash
cargo fmt --check
cargo check --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo test --all-features
cargo doc --no-deps --all-features
cargo check --example streaming_upload
cargo publish --dry-run
```

Expected: all commands exit `0`.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock README.md src/lib.rs CHANGELOG.md docs/migrations/0.2-to-0.3.md examples/streaming_upload.rs
git commit -m "release: prepare rustrest 0.3.0"
```
