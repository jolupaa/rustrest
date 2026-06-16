# RustRest Stateful Services, Content and OpenAPI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver RustRest `0.5.0` with secure cookie jars, pluggable expiring sessions, streaming multipart, configurable static files, managed SSE and OpenAPI 3.1 contracts.

**Architecture:** Stateful facilities depend on async store traits, with bounded in-memory reference implementations and vendor adapters kept outside the core. Content handlers operate on request/response streams. OpenAPI metadata is explicit and type-driven; procedural derives live in a separate workspace crate.

**Tech Stack:** Tokio, Hyper bodies, Serde, HMAC-SHA256, ChaCha20-Poly1305, `multer`, MIME guessing, OpenAPI 3.1 JSON Schema, proc-macro2, quote and syn.

---

## File Map

| Path | Responsibility |
| --- | --- |
| `src/app/cookie/mod.rs` | public cookie and jar API |
| `src/app/cookie/key.rs` | signing/encryption keys and rotation |
| `src/app/cookie/jar.rs` | request parsing and response deltas |
| `src/app/session/mod.rs` | middleware and session handle |
| `src/app/session/store.rs` | async store contract and records |
| `src/app/session/memory.rs` | bounded expiring in-memory store |
| `src/app/multipart.rs` | streaming multipart extractor and limits |
| `src/app/static_files.rs` | configurable static-file service |
| `src/app/sse.rs` | managed SSE body, heartbeat and cancellation |
| `src/app/openapi/mod.rs` | OpenAPI document and validation |
| `src/app/openapi/schema.rs` | `ToSchema` and component registry |
| `rustrest-macros/` | optional derive and route metadata macros |
| `tests/session_integration.rs` | TTL, rotation, fixation and failure behavior |
| `tests/content_integration.rs` | multipart, static files and SSE |
| `tests/openapi_contract.rs` | deterministic OpenAPI and validation |

### Task 1: Replace Loose Cookies with `CookieJar` and Key Rotation

**Files:**
- Create: `src/app/cookie/mod.rs`
- Create: `src/app/cookie/key.rs`
- Create: `src/app/cookie/jar.rs`
- Move from: `src/app/cookie.rs`
- Modify: `src/app/request.rs`
- Modify: `src/app/response.rs`
- Modify: `src/app/extract.rs`
- Modify: `src/app.rs`
- Modify: `src/lib.rs`
- Modify: `Cargo.toml`
- Create: `tests/cookie_jar.rs`

- [ ] **Step 1: Write failing cookie contract tests**

Create `tests/cookie_jar.rs`:

```rust
#[test]
fn duplicate_request_cookies_are_preserved_in_arrival_order() {
    let jar = CookieJar::parse("theme=light; sid=old; sid=new").unwrap();
    assert_eq!(jar.get("sid").unwrap().value(), "new");
    assert_eq!(jar.get_all("sid").map(Cookie::value).collect::<Vec<_>>(), vec!["old", "new"]);
}

#[test]
fn host_prefix_enforces_secure_host_only_rules() {
    assert!(Cookie::build("__Host-session", "value")
        .secure(true)
        .path("/")
        .build()
        .is_ok());
    assert!(Cookie::build("__Host-session", "value")
        .domain("example.com")
        .secure(true)
        .path("/")
        .build()
        .is_err());
}

#[test]
fn private_cookie_accepts_previous_key_and_reseals_with_current_key() {
    let old = CookieKey::from([7u8; 64]);
    let current = CookieKey::from([9u8; 64]);
    let ring = CookieKeyRing::new(current).with_previous(old.clone());
    let encoded = PrivateCookieJar::new(CookieKeyRing::new(old)).seal("sid", "abc").unwrap();
    let opened = PrivateCookieJar::new(ring).open("sid", &encoded).unwrap();
    assert_eq!(opened.value(), "abc");
    assert!(opened.needs_rotation());
}
```

- [ ] **Step 2: Verify failure**

Run `cargo test --test cookie_jar`.

Expected: missing jar, key and checked cookie builder APIs.

- [ ] **Step 3: Define strict cookie types**

Implement a builder returning `Result<Cookie, CookieError>`. Validate names, values, domain, path,
`SameSite=None` plus `Secure`, `__Host-` and `__Secure-` prefix constraints. Add `Expires`,
`Partitioned` and deletion preserving path/domain.

Do not sanitize invalid bytes by silently deleting them; return an error.

- [ ] **Step 4: Implement request and response jars**

Define:

```rust
pub struct CookieJar {
    original: Vec<Cookie>,
    delta: Vec<Cookie>,
}

impl CookieJar {
    pub fn get(&self, name: &str) -> Option<&Cookie>;
    pub fn get_all<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a Cookie>;
    pub fn add(&mut self, cookie: Cookie);
    pub fn remove(&mut self, cookie: Cookie);
    pub(crate) fn delta(&self) -> impl Iterator<Item = &Cookie>;
}
```

Add `CookieJar` as a request-parts extractor. Middleware or handlers can place a mutable jar in
request extensions; after the handler, append every delta as a separate `Set-Cookie` header.

- [ ] **Step 5: Add signed and private jars**

Add `chacha20poly1305 = { version = "0.10", optional = true }` under the `sessions` feature.
Derive independent signing and encryption subkeys from each 64-byte `CookieKey`. Use a random
96-bit nonce for every private cookie and bind the cookie name as AEAD associated data. Encode
binary values with URL-safe base64 without padding.

- [ ] **Step 6: Preserve compatibility with deprecations**

Keep `sign_value`, `verify_value`, `Response::set_cookie` and `Request::cookie` as deprecated
wrappers through `0.5`. Their implementation must use the new validated primitives.

- [ ] **Step 7: Verify**

Run:

```bash
cargo test --test cookie_jar
cargo test cookie
cargo test --features sessions
```

Expected: all pass, including tamper and key-rotation tests.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml Cargo.lock src/app/cookie src/app/request.rs src/app/response.rs src/app/extract.rs src/app.rs src/lib.rs tests/cookie_jar.rs src/app/tests.rs
git rm src/app/cookie.rs
git commit -m "feat: add secure cookie jars"
```

### Task 2: Introduce Async Session Stores and Lifecycle Semantics

**Files:**
- Create: `src/app/session/mod.rs`
- Create: `src/app/session/store.rs`
- Create: `src/app/session/memory.rs`
- Move from: `src/app/session.rs`
- Modify: `src/app/request.rs`
- Modify: `src/app.rs`
- Modify: `src/lib.rs`
- Create: `tests/session_integration.rs`

- [ ] **Step 1: Write store contract and lifecycle tests**

Create tests for:

```rust
#[tokio::test]
async fn login_regeneration_invalidates_the_old_session_id() {
    let store = Arc::new(MemorySessionStore::new(MemorySessionConfig::default()));
    let sessions = Sessions::new(store.clone(), CookieKeyRing::new(test_key()));
    let client = TestClient::new(session_app(sessions)).with_cookie_store();

    client.post("/anonymous").send().await.assert_status(200);
    let old = client.cookies().get("rustrest_session").unwrap().value().to_owned();
    client.post("/login").send().await.assert_status(204);
    let new = client.cookies().get("rustrest_session").unwrap().value().to_owned();

    assert_ne!(old, new);
    assert!(store.load_cookie_value(&old).await.unwrap().is_none());
}
```

Also test idle TTL, absolute TTL, rolling expiry, logout deletion, unchanged-session no-op,
maximum data size, store failure fail-closed and concurrent revision conflict.

- [ ] **Step 2: Verify failure**

Run `cargo test --test session_integration`.

Expected: missing async store and session lifecycle APIs.

- [ ] **Step 3: Define store records and errors**

Create `src/app/session/store.rs`:

```rust
#[derive(Clone, Debug)]
pub struct SessionRecord {
    pub id: SessionId,
    pub data: serde_json::Map<String, serde_json::Value>,
    pub created_at: SystemTime,
    pub last_accessed_at: SystemTime,
    pub expires_at: SystemTime,
    pub absolute_expires_at: SystemTime,
    pub revision: u64,
}

pub trait SessionStore: Send + Sync + 'static {
    fn load(&self, id: &SessionId)
        -> impl Future<Output = Result<Option<SessionRecord>, SessionError>> + Send;
    fn save(&self, record: SessionRecord, expected_revision: Option<u64>)
        -> impl Future<Output = Result<(), SessionError>> + Send;
    fn delete(&self, id: &SessionId)
        -> impl Future<Output = Result<(), SessionError>> + Send;
    fn touch(&self, id: &SessionId, expires_at: SystemTime, expected_revision: u64)
        -> impl Future<Output = Result<(), SessionError>> + Send;
}
```

Use an object-safe boxed-future adapter internally so `Sessions` can store `Arc<dyn DynSessionStore>`
while implementers use the ergonomic trait.

- [ ] **Step 4: Implement request-local `Session`**

Expose typed JSON access plus lifecycle operations:

```rust
impl Session {
    pub fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, SessionError>;
    pub fn insert<T: Serialize>(&mut self, key: &str, value: T) -> Result<(), SessionError>;
    pub fn remove(&mut self, key: &str);
    pub fn clear(&mut self);
    pub fn regenerate(&mut self);
    pub fn invalidate(&mut self);
    pub fn id(&self) -> Option<&SessionId>;
}
```

Track `New`, `Loaded`, `Dirty`, `Regenerate` and `Invalidated` states. Only save/touch when required.
Regeneration must save the new record before deleting the old record; on failure, keep the old
session valid and return an error response.

- [ ] **Step 5: Implement bounded memory store**

Use Tokio `RwLock` or a sharded map, lazy expiry on access and a periodic cleanup task owned by the
store. Enforce `max_sessions` and `max_session_bytes`. When full, reject new sessions with a typed
capacity error rather than evicting an active session silently.

- [ ] **Step 6: Wire middleware and secure cookies**

Load the private session cookie before `next`, place `Session` in request extensions, and persist
after the response returns. Configure cookie name, path, domain, secure mode, SameSite, idle TTL,
absolute TTL, rolling expiry and store-failure policy.

- [ ] **Step 7: Verify**

Run:

```bash
cargo test --features sessions --test session_integration
cargo test --features sessions session
cargo test --all-features
```

Expected: all pass and no expired record remains after cleanup settles.

- [ ] **Step 8: Commit**

```bash
git add src/app/session src/app/request.rs src/app.rs src/lib.rs tests/session_integration.rs src/app/tests.rs
git rm src/app/session.rs
git commit -m "feat: add pluggable expiring sessions"
```

### Task 3: Implement Streaming Multipart with Independent Limits

**Files:**
- Create: `src/app/multipart.rs`
- Modify: `src/app/form.rs`
- Modify: `src/app/extract.rs`
- Modify: `src/app.rs`
- Modify: `src/lib.rs`
- Modify: `Cargo.toml`
- Create: `tests/content_integration.rs`

- [ ] **Step 1: Add failing streaming and limit tests**

Test a multipart upload whose total body exceeds the old 64 KiB default but is accepted by a 10 MiB
route limit without collecting into one `Bytes`. Add tests for maximum parts, field bytes, file
bytes, part-header bytes, malformed boundary, nested multipart rejection and client disconnect.

Use the public API:

```rust
let mut multipart = Multipart::from_request(&mut req).await?;
while let Some(mut field) = multipart.next_field().await? {
    if field.is_file() {
        let mut file = tokio::fs::File::create(destination).await?;
        while let Some(chunk) = field.chunk().await? {
            file.write_all(&chunk).await?;
        }
    }
}
```

- [ ] **Step 2: Verify failure**

Run `cargo test --features multipart --test content_integration multipart`.

Expected: current parser buffers the complete body and lacks streaming limits.

- [ ] **Step 3: Add feature-gated parser dependency and config**

Add `multer = { version = "3", optional = true }` and map it from the `multipart` feature. Define:

```rust
#[derive(Clone, Debug)]
pub struct MultipartConfig {
    pub max_parts: usize,
    pub max_field_bytes: usize,
    pub max_file_bytes: usize,
    pub max_header_bytes: usize,
    pub max_total_bytes: usize,
    pub allow_nested: bool,
}
```

Validate non-zero values and ensure each component limit is at most the total limit.

- [ ] **Step 4: Implement `Multipart` and `MultipartField` wrappers**

Map parser errors into stable codes: `invalid_multipart`, `multipart_limit`, `multipart_disconnect`
and `multipart_already_consumed`. Count actual streamed bytes independently of Content-Length.

Expose sanitized metadata separately from the raw filename. Never construct a filesystem path from
the submitted filename.

- [ ] **Step 5: Add temporary-file helper**

Implement `field.persist_temp(directory).await` returning a `TemporaryUpload` that removes the file
on `Drop` unless `persist(path).await` succeeds. Use random names generated from OS randomness and
open with create-new semantics.

- [ ] **Step 6: Deprecate buffered `req.multipart()`**

Retain a compatibility helper that explicitly calls `collect_parts(config)` and enforces an
independent buffered cap. Mark it deprecated and document its memory behavior.

- [ ] **Step 7: Verify**

Run:

```bash
cargo test --features multipart --test content_integration multipart
cargo test --features multipart
cargo test --all-features
```

Expected: all pass, including disconnect cleanup.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml Cargo.lock src/app/multipart.rs src/app/form.rs src/app/extract.rs src/app.rs src/lib.rs tests/content_integration.rs
git commit -m "feat: stream multipart uploads"
```

### Task 4: Extract Static Files into a Configurable Service

**Files:**
- Create: `src/app/static_files.rs`
- Modify: `src/app/router.rs`
- Modify: `src/app.rs`
- Modify: `src/lib.rs`
- Modify: `Cargo.toml`
- Modify: `tests/content_integration.rs`

- [ ] **Step 1: Add static service behavior tests**

Test:

- index file selection;
- explicit directory-listing denial;
- SPA fallback only for requests accepting HTML;
- symlink deny/allow policy;
- `.br`/`.gz` precompressed selection with `Vary` and original media type;
- `If-Range` deciding between 206 and full 200;
- configured cache control;
- read error propagating through the response body.

- [ ] **Step 2: Verify failure**

Run `cargo test --features static-files --test content_integration static_files`.

Expected: configuration APIs and several behaviors are absent.

- [ ] **Step 3: Define the service**

Add `mime_guess = { version = "2", optional = true }` and map it from the `static-files` feature.

Create:

```rust
pub struct StaticFiles {
    root: PathBuf,
    index_file: Option<OsString>,
    fallback: Option<PathBuf>,
    symlinks: SymlinkPolicy,
    precompressed: bool,
    cache_control: Option<HeaderValue>,
}

impl StaticFiles {
    pub fn new(root: impl Into<PathBuf>) -> Self;
    pub fn index_file(self, value: impl Into<OsString>) -> Self;
    pub fn fallback(self, value: impl Into<PathBuf>) -> Self;
    pub fn symlinks(self, policy: SymlinkPolicy) -> Self;
    pub fn precompressed(self, enabled: bool) -> Self;
    pub fn cache_control(self, value: HeaderValue) -> Self;
}
```

Implement it as an `IntoHandler`-compatible service mounted by `Router::serve_dir(prefix, service)`.

- [ ] **Step 4: Enforce filesystem containment**

Canonicalize the root at construction. Under deny policy, use symlink metadata for each path
component and reject any symlink. Under allow-within-root policy, canonicalize the final path and
verify `starts_with(canonical_root)`. Treat containment failure as 404 to avoid revealing layout.

- [ ] **Step 5: Select representation before validators**

Choose `.br`, `.gz` or identity based on complete Accept-Encoding negotiation, then compute metadata,
ETag and range semantics for the selected representation. Set `Content-Encoding`, original
`Content-Type`, `Vary: Accept-Encoding` and selected representation length.

- [ ] **Step 6: Preserve compatibility wrapper**

Implement `app.static_files(prefix, root)` and `router.static_files` by constructing
`StaticFiles::new(root)` with conservative defaults.

- [ ] **Step 7: Verify**

Run:

```bash
cargo test --features static-files --test content_integration static_files
cargo test --features static-files static_files
cargo test --all-features
```

Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml Cargo.lock src/app/static_files.rs src/app/router.rs src/app.rs src/lib.rs tests/content_integration.rs src/app/tests.rs
git commit -m "feat: add configurable static file service"
```

### Task 5: Add Managed SSE Responses

**Files:**
- Modify: `src/app/sse.rs`
- Modify: `src/app/response.rs`
- Modify: `src/app/server/handle.rs`
- Modify: `src/app.rs`
- Modify: `src/lib.rs`
- Modify: `tests/content_integration.rs`

- [ ] **Step 1: Add failing SSE lifecycle tests**

Test bounded queue behavior, heartbeat, Last-Event-ID, cancellation after disconnect, graceful
server shutdown and serialization errors. Use:

```rust
let (sender, response) = Sse::channel(SseConfig::default().capacity(16));
tokio::spawn(async move {
    sender.send(SseEvent::json("user.created", &user)?).await?;
    Ok::<_, SseError>(())
});
response
```

- [ ] **Step 2: Verify failure**

Run `cargo test --features sse --test content_integration sse`.

Expected: `Sse::channel`, bounded sender and lifecycle APIs are absent.

- [ ] **Step 3: Define managed channel types**

```rust
pub struct SseConfig {
    capacity: usize,
    heartbeat: Option<Duration>,
    backpressure: SseBackpressure,
}

pub enum SseBackpressure {
    Wait { timeout: Duration },
    Reject,
    Disconnect,
}

pub struct SseSender { /* bounded channel and cancellation */ }
pub struct Sse { /* response body and registration guard */ }
```

Validate heartbeat and timeout values. Reject event data containing invalid CR/LF sequences after
normalizing each data line according to SSE framing.

- [ ] **Step 4: Integrate cancellation and shutdown**

Close the sender when the client disconnects. Register open SSE streams in shared server lifecycle
state so graceful shutdown stops new streams, sends cancellation and waits within the configured
drain deadline.

- [ ] **Step 5: Preserve stream helpers**

Keep `Response::sse` and `sse_with_heartbeat` as adapters into the managed body without a sender.
Their streams must now surface source errors.

- [ ] **Step 6: Verify**

Run:

```bash
cargo test --features sse --test content_integration sse
cargo test --features sse
cargo test --all-features
```

Expected: all pass with no hanging shutdown test.

- [ ] **Step 7: Commit**

```bash
git add src/app/sse.rs src/app/response.rs src/app/server/handle.rs src/app.rs src/lib.rs tests/content_integration.rs src/app/tests.rs
git commit -m "feat: manage SSE lifecycle and backpressure"
```

### Task 6: Build OpenAPI 3.1 Contracts and Validation

**Files:**
- Create: `src/app/openapi/mod.rs`
- Create: `src/app/openapi/schema.rs`
- Create: `src/app/openapi/operation.rs`
- Move from: `src/app/openapi.rs`
- Modify: `src/app/router.rs`
- Modify: `src/app/extract.rs`
- Modify: `src/app/server/mod.rs`
- Modify: `src/app.rs`
- Modify: `src/lib.rs`
- Modify: `Cargo.toml`
- Create: `tests/openapi_contract.rs`

- [ ] **Step 1: Add a complete contract test**

Create a route whose OpenAPI operation includes operation ID, typed path/query parameters, JSON
request, 201 JSON response, 400 Problem Details and bearer authentication. Assert deterministic
JSON equality against an inline `serde_json::json!` value containing `openapi: "3.1.0"` and reusable
schemas under `components.schemas`.

- [ ] **Step 2: Verify failure**

Run `cargo test --features openapi --test openapi_contract`.

Expected: current output lacks schemas, bodies, errors and security.

- [ ] **Step 3: Define schema registry contracts**

```rust
pub trait ToSchema {
    fn schema(registry: &mut SchemaRegistry) -> SchemaRef;
}

pub struct SchemaRegistry {
    schemas: BTreeMap<String, serde_json::Value>,
}

pub enum SchemaRef {
    Inline(serde_json::Value),
    Component(String),
}
```

Implement primitives, strings, numeric types, booleans, `Option<T>`, `Vec<T>`, maps and
`serde_json::Value`. Use `BTreeMap` for deterministic output.

- [ ] **Step 4: Expand route metadata**

Add chainable methods:

```rust
.operation_id("users.create")
.request_body::<Json<CreateUser>>()
.response::<201, Json<User>>("Created")
.error_response::<400, ProblemDetails>("Invalid input")
.security("bearerAuth", &[])
.deprecated(true)
```

Represent status as `u16` internally if const-generic status aliases are not ergonomic. Registration
must reject duplicate operation IDs and conflicting response definitions.

- [ ] **Step 5: Generate and validate OpenAPI 3.1**

Add `jsonSchemaDialect`, components, security schemes and typed parameters. Validate every path
parameter appears exactly once and is required, every `$ref` resolves, operation IDs are unique and
status keys are valid.

- [ ] **Step 6: Remove mandatory CDN dependence**

Serve the JSON document independently. Put Swagger UI behind a separate `openapi-ui` feature with
pinned embedded assets, or accept an application-supplied HTML renderer. The default `openapi`
feature must not execute third-party CDN JavaScript.

- [ ] **Step 7: Verify**

Run:

```bash
cargo test --features openapi --test openapi_contract
cargo test --features openapi openapi
cargo test --all-features
```

Expected: deterministic documents and startup validation tests pass.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml Cargo.lock src/app/openapi src/app/router.rs src/app/extract.rs src/app/server src/app.rs src/lib.rs tests/openapi_contract.rs src/app/tests.rs
git rm src/app/openapi.rs
git commit -m "feat: generate OpenAPI 3.1 contracts"
```

### Task 7: Add the Optional `rustrest-macros` Crate

**Files:**
- Modify: `Cargo.toml`
- Create: `rustrest-macros/Cargo.toml`
- Create: `rustrest-macros/src/lib.rs`
- Create: `rustrest-macros/tests/schema.rs`
- Modify: `src/lib.rs`
- Modify: `tests/openapi_contract.rs`

- [ ] **Step 1: Convert the repository to a workspace without changing the package**

Add:

```toml
[workspace]
members = [".", "rustrest-macros"]
resolver = "2"
```

The root remains publishable as package `rustrest`.

- [ ] **Step 2: Add macro compile tests**

Test:

```rust
#[derive(Serialize, Deserialize, rustrest::ToSchema)]
struct CreateUser {
    /// Nombre visible del usuario.
    name: String,
    age: Option<u8>,
}
```

Assert the generated schema marks `name` required, `age` nullable/optional and includes field
descriptions. Add `trybuild` compile-fail tests for unsupported unions and duplicate schema names.

- [ ] **Step 3: Configure the proc-macro crate**

Use:

```toml
[lib]
proc-macro = true

[dependencies]
proc-macro2 = "1"
quote = "1"
syn = { version = "2", features = ["full", "extra-traits"] }
```

Add optional root dependency and feature:

```toml
macros = ["dep:rustrest-macros", "openapi"]
```

- [ ] **Step 4: Implement `ToSchema` derive**

Support named structs, tuple newtypes, unit enums, tagged enums, `serde(rename)`,
`serde(rename_all)`, `serde(default)`, `serde(skip)`, doc comments and an explicit
`#[schema(example = ...)]` attribute. Generate calls to the public `rustrest::openapi` traits; do
not depend on private internals.

- [ ] **Step 5: Re-export derives when enabled**

In `src/lib.rs`:

```rust
#[cfg(feature = "macros")]
pub use rustrest_macros::ToSchema;
```

Rust permits the trait and derive macro to share the same name in separate namespaces.

- [ ] **Step 6: Verify**

Run:

```bash
cargo test -p rustrest-macros
cargo test --features macros --test openapi_contract
cargo test --workspace --all-features
```

Expected: runtime and compile-fail tests pass.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock rustrest-macros src/lib.rs tests/openapi_contract.rs
git commit -m "feat: derive OpenAPI schemas"
```

### Task 8: Publish the `0.5.0` Migration and Release

**Files:**
- Modify: `Cargo.toml`
- Modify: `README.md`
- Modify: `CHANGELOG.md`
- Create: `docs/migrations/0.4-to-0.5.md`
- Create: `docs/guides/sessions.md`
- Create: `docs/guides/uploads.md`
- Create: `docs/guides/openapi.md`
- Create: `examples/session_api.rs`
- Create: `examples/upload_api.rs`

- [ ] **Step 1: Document stateful migration**

Explain conversion from `Sessions::new(secret)` and manual IDs to `Sessions::new(store, key_ring)`,
request-local `Session`, TTLs and regeneration. Include an explicit production warning that
`MemorySessionStore` is single-process.

- [ ] **Step 2: Document content and OpenAPI APIs**

Provide complete streaming upload, StaticFiles, managed SSE and OpenAPI route examples. Document
all relevant feature flags and memory/disk limits.

- [ ] **Step 3: Add compiling examples**

`session_api.rs` must demonstrate login regeneration and logout invalidation. `upload_api.rs` must
stream a file to a temporary location, enforce limits and persist it under an application-generated
name.

- [ ] **Step 4: Bump version and changelog**

Set both published crate versions to `0.5.0` where appropriate and record the actual release date.

- [ ] **Step 5: Run release gates**

```bash
cargo fmt --check
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --no-deps --all-features
cargo publish -p rustrest-macros --dry-run
cargo publish -p rustrest --dry-run
```

Expected: all commands exit `0`. Publish order in the eventual release is macros first, then core.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock rustrest-macros/Cargo.toml README.md CHANGELOG.md docs/migrations/0.4-to-0.5.md docs/guides/sessions.md docs/guides/uploads.md docs/guides/openapi.md examples/session_api.rs examples/upload_api.rs
git commit -m "release: prepare rustrest 0.5.0"
```
