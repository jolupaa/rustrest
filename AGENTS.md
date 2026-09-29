# AGENTS.md

Guidance for coding agents working in this repository.

`CLAUDE.md` is the maintained, authoritative description of this project
(architecture, commands, conventions, and extension constraints). Read it
before making changes; this file intentionally does not duplicate it so the
two cannot drift apart again.

Key rules in brief:

- `rustrest` is a hand-written Express-style framework on hyper 1.x/tokio;
  extend its own abstractions rather than adopting axum/actix/warp.
- User-facing strings and console output are in Spanish.
- `#![forbid(unsafe_code)]` stays; MSRV is Rust 1.85 (edition 2024).
- Before finishing: `cargo fmt --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`, and
  `cargo test --all-features` must pass. Behavior changes need a
  `CHANGELOG.md` entry and, when breaking, a note in `docs/migrations/`.
