# beevulyk-queue-contracts

Canonical Rust event structs for BeeVulyk Kafka topics.

Producers and consumers pull this crate in via git submodule and depend on it
as a path/git dependency in `Cargo.toml`. Topic name constants live alongside
their event struct, so every consumer sees a single source of truth for both
the wire format and the topic name.

All events are JSON-serialized on the wire. This crate is deliberately
proto-free — services that only need to publish or consume Kafka events do
not have to pull in `beevulyk-proto` or run gRPC codegen at build time.

## Layout

One module per bounded context:

- `identity::users` — user lifecycle events (`UserRegistered`, ...).

## Adding a new event

1. Create or reuse a module under `src/<domain>/<context>.rs`.
2. Define a `TOPIC_<EVENT>` `&str` constant next to the struct.
3. Derive `Serialize`, `Deserialize`, `Debug`, `Clone` plus `PartialEq` /
   `Eq` where the payload allows.
4. Add a serde round-trip test.
5. Bump the crate `version` and cut a new GitHub release.
