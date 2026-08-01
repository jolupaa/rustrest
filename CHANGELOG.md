# Changelog

All notable changes to RustRest are documented here.

The format follows Keep a Changelog and the project uses Semantic Versioning.

## [Unreleased]

### Added

- Server configuration for HTTP/1 header and TLS-handshake deadlines,
  request deadlines, HTTP/2 keepalive, graceful-shutdown deadlines, connection
  admission, request-target/query budgets, and logical request-header
  count/size budgets.
- Configurable `MultipartLimits`, bounded `middleware::RateLimit`, and a
  bounded, sharded, expiring session store.
- `StaticFilesOptions` and `Dotfiles` for explicit opt-in to serving hidden
  files, plus configurable first-poll, stalled-consumer, and total stream
  deadlines.
- `OptionalRejection`, allowing custom extractor rejections to distinguish
  genuinely missing values from malformed or oversized input.
- `Request::singleton_header`, which returns a structured `400` for ambiguous
  duplicate fields instead of choosing one occurrence.
- Synchronized request header/cookie mutation APIs plus read-only `headers()`
  and `cookies()` views.
- Fallible `Sessions::try_*` configuration builders and scoped
  `Response::clear_cookie_with` deletion.
- A router fuzz target plus locked fuzz-workspace checks, and scheduled RustSec
  audits for the application and fuzz dependency graphs.

### Changed

- Ordinary HTTP/1 connections now serve one request and close so raw framing
  validation protects every request. WebSocket candidates use a separate
  upgrade-capable path; unsuccessful upgrades are forced closed. HTTP/2
  remains multiplexed, with finite concurrent-stream/send-buffer defaults and
  keepalive PING deadlines.
- `App::new()` now applies a 30-second request/body/handler deadline by
  default; `disable_request_timeout()` is the explicit opt-out.
- Session HMAC secrets now require at least 32 bytes; session cookies carry a
  matching sliding `Max-Age`, extend a server-side idle deadline, refresh on
  active requests, and support explicit `Secure`/`SameSite` policy.
- Session middleware now makes session-bearing responses private to caches. It
  preserves valid existing `Cache-Control` directives, appends `private`
  unless `private`/`no-store` is already present, and replaces an invalid field
  with `private, no-store`.
- Session data now has configurable per-session entry, key, value, and
  aggregate-byte bounds. `Sessions::set` returns
  `Result<(), SessionDataError>` when a write violates them.
- Anonymous sessions are now admitted lazily on the first successful
  `Sessions::set`. Read-only traffic receives a transient handler-visible id
  but consumes no store capacity and receives no cookie. At capacity, new
  writes return `SessionUnavailable` without evicting a live session.
- Session configuration is immutable while storage is shared, response-time
  renewal keeps server expiry aligned with upward-rounded cookie `Max-Age`,
  and automatic cookie suppression now requires the same host-only root scope.
- Static roots are opened and capability-pinned at route registration,
  bounded blocking file-open work is offloaded, its permit remains attached
  to a supervised bounded producer to cap live descriptors, dotfiles are
  denied by default, and metadata validators are weak `ETag` values.
- The `etag` middleware now uses SHA-256, including empty responses, and
  runs large hashes on a bounded blocking pool. Because it runs after the
  handler, automatic precondition evaluation is limited to `GET`/`HEAD`;
  unsafe methods require a pre-handler guard.
- Swagger UI assets are version-pinned and its generated initializer is
  authorized by a CSP hash with a no-referrer policy; duplicate OpenAPI
  operations preserve the first route and expose a vendor-extension count.
- OpenAPI preserves `all()` and extension-method operations under
  `x-rustrest-custom-methods` instead of dropping them.
- Route patterns and request paths share strict one-time percent
  normalization, decoded control characters are rejected, parameter names are
  validated, and registration/matching are bounded to 256 segments. Trie
  candidate and method traversal is iterative.
- `RouteErrorKind` and `RouteMatchErrorKind` are now non-exhaustive so new
  validation categories can be added without breaking downstream matches.
- The collapsed request-header and parsed-cookie maps are now private.
  Middleware must use the synchronized mutation APIs so raw, duplicate-aware,
  typed, and parsed-cookie access cannot disagree.
- Ordinary HTTP route registration rejects `CONNECT`; dispatch returns `501`
  even through `all()`. A future tunnel API must explicitly own the upgraded
  transport before the framework can safely support this method.
- WebSocket subprotocols are validated as unique HTTP tokens and negotiated
  case-sensitively as required by RFC 6455.
- WebSocket defaults now enforce same-host transport-aware origins, bounded
  messages, frames, buffers, queues, rooms, connection/rate limits, keepalive,
  send/close, idle, and lifetime deadlines, with explicit opt-outs where
  applicable.

### Fixed

- Duplicate configured session cookies now fail with a structured `400`;
  invalid, expired, or cleared presented sessions are deleted client-side
  rather than reissued when no replacement was persisted.
- Stalled or slow-drip static-file consumers can no longer retain every global
  open-file permit indefinitely; a lazy bounded producer drops the file and
  emits a truncated-body error when its configured lease expires. An
  unpolled-body supervisor and metadata-only `HEAD` path close the remaining
  lifecycle and read-amplification gaps.
- Plaintext and TLS request heads with both `Transfer-Encoding` and
  `Content-Length` are rejected at the raw transport boundary in either field
  order, including after accepted leading empty lines, before middleware or
  handlers can run.
- Prior-knowledge HTTP/2 must complete its client preface and initial
  `SETTINGS` under the preface deadline. TLS ALPN now selects a strict
  protocol-specific server path, preventing an `h2`/HTTP/1 mismatch.
- Accepted connection tasks, TLS handshakes, upgraded transports, graceful
  drain, and forced shutdown now share the configured admission/lifecycle
  limits. HTTPS also applies the HTTP/1 header-read deadline.
- Missing, duplicate, malformed, or conflicting `Host`/HTTP/2 `:authority`
  values are rejected. Strict shared `host[:port]` validation rejects userinfo,
  empty hosts, whitespace, and empty, non-decimal, or out-of-range ports even
  when the underlying URI parser accepts them; comparisons normalize hostname
  case, IPv6 spellings, and scheme-default ports. HTTP/2 authority is exposed
  consistently as `Host`.
- WebSocket transport sends, flushes and Close writes now honor a hard timeout;
  the driver uses fair selection with an explicit shutdown preflight.
- WebSocket handler completion now drains already-queued outbound
  messages/commands before synthesizing a normal Close, so a final echo or
  application Close cannot be lost to the completion race.
- Pending WebSocket-upgrade tasks register an abort handle immediately, closing
  the gap in which graceful shutdown could wait indefinitely before a driver
  was registered.
- Public WebSocket-runtime shutdown bounds its post-abort cleanup wait even if
  a malformed lifecycle entry never registered an abort handle.
- Legacy `Response::websocket(&Request)` is deprecated and refuses a real
  network upgrade, preventing a false `101` followed immediately by EOF; use
  a consuming WebSocket route API instead.
- Zero-capacity `WsBroadcast` and `InMemoryWsBroker` constructors no longer
  panic and normalize to a one-slot channel.
- Streaming request bodies and direct `RequestBody::collect` calls now enforce
  the effective application/route hard ceiling; a caller-supplied collection
  limit may only lower it.
- Multipart parsing now follows MIME delimiter-line and quoted-parameter rules
  and enforces part, header, count, and per-part limits. Folded headers,
  duplicate `Content-Disposition`/`Content-Type`, invalid names, and
  control-containing part-header values are rejected.
- `Option<E>` extractors now suppress only a rejection whose
  `OptionalRejection::is_missing()` returns true; malformed input,
  unsupported media types, and body-limit failures propagate.
- Response finalization rejects ambiguous or invalid outbound framing,
  rejects final interim statuses, validates the required WebSocket fields on
  `101`, suppresses bodies for `204`/`205`/`304`, and rejects forbidden
  trailer fields consistently in the network server and `TestClient`.
- `304` representation lengths are validated before the buffered body is
  discarded, including trailer and selected-response framing conflicts.
  `ETag` is allowed as a legitimate trailer, while `Keep-Alive` and fields
  nominated by `Connection` are rejected there.
- `TestClient` responses now materialize the same implicit `Content-Type` and
  buffered `Content-Length` headers as network responses.
- Automatic/explicit `HEAD` responses retain or synthesize the corresponding
  buffered representation length, validate explicit lengths, and remove body
  trailers, including on timeout responses. Finalization now happens before
  body suppression so a replacement `500` also remains bodyless.
- Static-file `Range` processing is limited to `GET`, so `HEAD` always reports
  the complete representation. Duplicate `If-Range` fields fail closed to a
  full response, and date validators require exact whole-second equality.
- SSE no longer emits the HTTP/2-forbidden `Connection` header, a zero
  heartbeat disables heartbeat generation without spinning, and bare CR data
  cannot inject another SSE field.
- Invalid query, cookie, header, path, and URL-encoded form extractors return
  stable Spanish problem details while retaining deserializer errors only as
  private sources. String handler errors no longer disclose their source
  text.
- Compression now honors an explicitly preferred `identity` coding, emits
  correct `Vary` metadata, validates pre-encoded responses, returns `406` when
  identity is forbidden and a transform is impossible, removes stale
  validators/digests/length/range metadata, and bounds large compression work
  off the async runtime. Repeated `Accept-Encoding` and `Cache-Control` field
  lines are evaluated rather than collapsed. Repeated response
  `Content-Encoding` fields are also combined for pre-encoded validation.
  Responses with trailers retain the identity representation so late
  integrity metadata cannot describe pre-transform bytes.
- Conditional parsing handles commas inside quoted entity tags, empty
  responses receive validators, repeated list-valued `If-Match` and
  `If-None-Match` fields are combined, and `412` responses no longer retain a
  stale `Content-Length`, body, or trailers.
- Configurable CORS now emits complete `Vary` metadata for allowed and denied
  origins and for preflight request method/header inputs. Duplicate `Origin`
  or `Access-Control-Request-Method` fields are rejected as ambiguous, while
  repeated `Access-Control-Request-Headers` lines are combined.
- Rate-limit `Retry-After` now rounds fractional remaining windows upward.
- Session admission at capacity now consults only the exact expiry frontier of
  each shard and never evicts a live entry. Scheduled expiry cleanup is
  batched, and rate-limit identity churn uses the bounded overflow bucket
  between scheduled sweeps, removing two serialized
  O(capacity)-per-request denial-of-service paths.
- Multiple `Cookie` request fields and `RequestBuilder` cookies are parsed in
  arrival order with the last duplicate name winning, while automatic `HEAD`
  no longer falls through to WebSocket-only `GET` routes.
- A well-formed manual or error-renderer-generated WebSocket `101` is rejected
  unless the consuming WebSocket path actually owns the upgraded transport.
- CONNECT rejection remains non-successful even when an error renderer or
  outbound middleware attempts to rewrite it to a 2xx tunnel response.
- `RequestBuilder::path` now clears stale query state, and its JSON helpers no
  longer convert serialization failures into an unrelated empty body.
- Static range-unit matching is case-insensitive, missing files remain `404`,
  and non-`NotFound` open failures now produce `500`.
- Supplied request IDs are reused only when exactly one non-empty printable
  ASCII value of at most 128 bytes is present; invalid or duplicate values are
  replaced.
- JSON/event WebSocket receivers now skip Ping/Pong control frames instead of
  reporting them as end-of-stream.
- Panics in a custom global error handler fall back to a generic `500`.
- Zero or unrepresentable HTTP parser/HTTP2/WebSocket deadline values are
  rejected before they can spin, immediately expire, or panic a connection
  task.
- HTTP/2 responses remove `Connection`, `Upgrade`, `Keep-Alive`,
  `Proxy-Connection`, `TE`, and fields nominated by `Connection`.
- The Autobahn runner now selects Docker Desktop's host bridge automatically
  on macOS while retaining host networking on Linux.
- The WebSocket load example now raises the global transport admission limit
  alongside its 12,000-socket WebSocket limits, so the documented 11,000
  connection profile is not capped at the server's production default.

### Security

- Percent-encoded static route segments and registered static pattern segments
  are decoded and validated before trie lookup, preventing encoded paths from
  bypassing static-over-parameter route precedence or producing duplicate
  aliases.
- Capability-relative static-file opens prevent symlink traversal and
  time-of-check/time-of-use path swaps from escaping the configured root.
- Static dotfiles are hidden unless a mount explicitly opts in.
- Trailing-slash redirects cannot emit scheme-relative `Location` values.
- `Response::cookie` and `Response::set_cookie` share the same cookie-octet
  sanitizer; `SameSite=None`, `__Secure-`, and `__Host-` invariants are
  enforced. Session cookies created on direct TLS requests include `Secure`.
- Swagger UI title/spec configuration is escaped for both HTML and inline
  JavaScript contexts.
- Session secrets/cookie names are validated and in-memory session/rate-limit
  storage is bounded against attacker-controlled cardinality.
- TLS PEM loading now uses maintained `rustls-pki-types` instead of the
  unmaintained `rustls-pemfile`; static test certificates also remove the
  vulnerable `time 0.3.45` development dependency (RUSTSEC-2026-0009).

## [0.3.0] - 2026-06-16

### Added

- Structured `HttpError`/problem-details responses with global error-handler support.
- Streaming request body abstraction with binary-safe `bytes`, strict `text`, JSON, form and multipart helpers.
- Route-level body limits, early `Content-Length` validation and `Expect: 100-continue` handling.
- Validated route registration, route names, `url_for`, strict percent-decoded params and optional host constraints.
- Async extractor contracts, `RequestParts`, typed handler arguments, typed headers and request-local extensions.
- Fallible response streams, HTTP trailers and checked response builders.
- Weighted `Accept-Encoding` negotiation and RFC-ordered precondition handling for buffered and static responses.
- `0.2` to `0.3` migration guide and a compiling streaming upload example.
- Named feature flags for future modularization: `compression`, `multipart`, `static-files`, `sse`, `websocket`, `openapi`, `sessions`, `metrics` and `full`.

### Changed

- Body-consuming extractors are now async and must be awaited.
- `Request::extract` is for body extractors; metadata extractors use `Request::extract_parts`.
- Static file and SSE streams now propagate read/format errors through the HTTP body.
- Compression skips untransformable responses and returns `406` when no acceptable coding or identity representation is available.

### Fixed

- Invalid response status/header construction no longer panics or silently drops values at the HTTP boundary.
- Static-file conditionals now respect `If-Match`, `If-Unmodified-Since`, weak `If-None-Match` and `If-Range`.

## [0.2.0] - 2026-06-13

### Added

- Production WebSocket runtime, rooms, broker contract, observability and validation.
- HTTP/2, TLS, typed extraction, static files, SSE, sessions and OpenAPI foundations.
