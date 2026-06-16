# Changelog

All notable changes to RustRest are documented here.

The format follows Keep a Changelog and the project uses Semantic Versioning.

## [Unreleased]

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security

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
