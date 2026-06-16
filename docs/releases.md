# Release Policy

- RustRest follows Semantic Versioning.
- Public behavior documented in rustdoc, README examples and migration guides is part of the API.
- A breaking pre-1.0 release includes `docs/migrations/<from>-to-<to>.md`.
- Feature flags are additive and must not change the meaning of another enabled feature.
- MSRV increases are documented in the changelog and require a minor release.
- Security fixes may remove unsound or unsafe behavior immediately and must include a security note.
