# RustRest Production Framework Roadmap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Evolve RustRest from the current broad `0.2.0` feature set into a modular, observable and compatibility-governed production framework through independently releasable phases.

**Architecture:** Keep `rustrest` as a lightweight facade and HTTP core. Implement advanced capabilities behind additive features, and defer vendor-specific integrations to companion crates. The roadmap is split into four implementation plans because HTTP streaming, server security, stateful services and release engineering have different contracts and can each ship working software.

**Tech Stack:** Rust 2024, Rust 1.85+ MSRV, Tokio, Hyper 1.x, hyper-util, http-body-util, Serde, rustls, tracing, cargo-fuzz, criterion, cargo-semver-checks, cargo-deny.

---

## Plan Set

Execute these documents in order:

1. [`2026-06-13-production-http-core.md`](2026-06-13-production-http-core.md)
   - Target release: `0.3.0`
   - Request streaming, body limits, fallible response streams, structured errors and async extractors.
2. [`2026-06-13-production-server-security.md`](2026-06-13-production-server-security.md)
   - Target release: `0.4.0`
   - Server builder/handle, transport limits, trusted proxies and production middleware.
3. [`2026-06-13-production-state-content-openapi.md`](2026-06-13-production-state-content-openapi.md)
   - Target release: `0.5.0`
   - Cookie jars, async sessions, streaming multipart, static files, managed SSE and OpenAPI 3.1.
4. [`2026-06-13-production-observability-release.md`](2026-06-13-production-observability-release.md)
   - Target releases: `0.6.0`, release candidates and `1.0.0`
   - Observability, test tooling, performance gates, feature organization, documentation and compatibility stabilization.

The approved design and acceptance criteria are in
[`../specs/2026-06-13-production-framework-design.md`](../specs/2026-06-13-production-framework-design.md).

## Specification Coverage

| Design section | Implementation owner |
| --- | --- |
| Architecture and stability | Roadmap Phase 0; observability/release Tasks 8-10 |
| HTTP request pipeline | HTTP core Tasks 2, 3 and 5 |
| Responses and HTTP semantics | HTTP core Tasks 1, 6 and 7 |
| Server and transport | Server/security Tasks 1 and 2 |
| Reverse proxies and identity | Server/security Task 3 |
| Routing and route contracts | HTTP core Task 4 |
| Extraction and validation | HTTP core Task 5 |
| Error model | HTTP core Task 1 |
| Middleware platform | HTTP core Task 7; server/security Tasks 4-6 |
| Cookies and sessions | State/content/OpenAPI Tasks 1-2; observability/release Task 5 |
| Multipart, static files and SSE | State/content/OpenAPI Tasks 3-5 |
| OpenAPI and documentation UI | State/content/OpenAPI Tasks 6-7 |
| Observability | Observability/release Tasks 1-3 |
| Testing and developer experience | Observability/release Tasks 4, 6-8 |
| Feature modularity | HTTP core Task 8; observability/release Task 9 |
| Production acceptance and `1.0` | Observability/release Task 10 |

## Global Working Rules

- [ ] Start every phase from the previous phase's tagged release, in a fresh `codex/` branch.
- [ ] Use TDD for every public behavior change: failing test, minimal implementation, passing test, commit.
- [ ] Preserve unrelated user changes in dirty worktrees.
- [ ] Keep WebSocket public behavior compatible; only adapt it to shared server, error and observability contracts.
- [ ] Add a migration document for every breaking release before merging it.
- [ ] Run the full feature matrix before calling a phase complete.
- [ ] Update `README.md`, crate-level docs and examples in the same commit that changes a public API.
- [ ] Keep default features lightweight; every new optional dependency must map to a named feature.

## Release Gates

Every release must pass:

```bash
cargo fmt --check
cargo check --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo test --all-features
cargo doc --no-deps --all-features
```

Expected result: all commands exit `0` and rustdoc emits no warnings.

From `0.4.0` onward, also run:

```bash
cargo test --test http_integration
cargo test --features tls --test tls_integration
cargo test --test websocket_integration
```

Expected result: all protocol integration tests pass.

From `0.6.0` onward, also run:

```bash
cargo semver-checks check-release
cargo deny check
cargo audit
cargo bench --no-run
```

Expected result: no accidental semver break, denied license/source/advisory issue or benchmark build failure.

## Phase 0: Establish Release Discipline

This task is performed once before implementing `0.3.0`.

**Files:**
- Create: `CHANGELOG.md`
- Create: `docs/migrations/README.md`
- Create: `docs/releases.md`
- Modify: `Cargo.toml`
- Modify: `README.md`

- [ ] **Step 1: Create the changelog contract**

Add `CHANGELOG.md`:

```markdown
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

## [0.2.0] - 2026-06-13

### Added

- Production WebSocket runtime, rooms, broker contract, observability and validation.
- HTTP/2, TLS, typed extraction, static files, SSE, sessions and OpenAPI foundations.
```

- [ ] **Step 2: Document compatibility and release policy**

Add `docs/releases.md` with these enforceable rules:

```markdown
# Release Policy

- RustRest follows Semantic Versioning.
- Public behavior documented in rustdoc, README examples and migration guides is part of the API.
- A breaking pre-1.0 release includes `docs/migrations/<from>-to-<to>.md`.
- Feature flags are additive and must not change the meaning of another enabled feature.
- MSRV increases are documented in the changelog and require a minor release.
- Security fixes may remove unsound or unsafe behavior immediately and must include a security note.
```

Add `docs/migrations/README.md`:

```markdown
# Migration Guides

Breaking releases have a focused guide containing compile fixes, behavior changes and before/after examples.
```

- [ ] **Step 3: Expose repository and issue metadata**

Add to `[package]` in `Cargo.toml`:

```toml
repository = "https://github.com/jolupaa/rustrest"
homepage = "https://github.com/jolupaa/rustrest"
```

- [ ] **Step 4: Link project policy from the README**

Add a `## Compatibility and releases` section to `README.md` linking to `CHANGELOG.md`,
`docs/releases.md` and `docs/migrations/README.md`.

- [ ] **Step 5: Verify package metadata**

Run:

```bash
cargo package --list
cargo metadata --no-deps --format-version 1
```

Expected: both commands exit `0`, and metadata contains the repository and homepage URLs.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml README.md CHANGELOG.md docs/releases.md docs/migrations/README.md
git commit -m "docs: define framework release policy"
```

## Phase Execution

- [ ] Complete every task in `2026-06-13-production-http-core.md` and tag `v0.3.0`.
- [ ] Complete every task in `2026-06-13-production-server-security.md` and tag `v0.4.0`.
- [ ] Complete every task in `2026-06-13-production-state-content-openapi.md` and tag `v0.5.0`.
- [ ] Complete every task in `2026-06-13-production-observability-release.md` and tag `v0.6.0`.
- [ ] Run the `1.0.0` release-candidate gates defined in the final plan.

## Final Acceptance Matrix

Before tagging `1.0.0`, record evidence in `docs/benchmarks/framework-1.0-acceptance.md` for:

| Area | Required evidence |
| --- | --- |
| Memory safety | bounded request, multipart, connection, session and middleware state |
| Backpressure | request and response streams stop reading/writing when consumers stall |
| Cancellation | disconnect and timeout cancel body/handler work |
| HTTP correctness | HTTP/1.1 and HTTP/2 integration suites pass |
| Proxy safety | spoofed forwarded headers are ignored from untrusted peers |
| Security | `cargo deny`, `cargo audit`, security defaults and threat model reviewed |
| Observability | request/connection/body/panic events visible without payload leakage |
| Distributed state | session and rate-limit contracts have external-store reference tests |
| API documentation | OpenAPI contains real inputs, outputs, errors and auth schemes |
| Compatibility | semver check passes against the final release candidate baseline |
| Performance | benchmark and load thresholds pass on the documented reference machine |
| Documentation | quick start, production guide, migrations and complete examples compile |
