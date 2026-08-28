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

- `identity::profiles` — profile lifecycle events (`ProfileVerificationChanged`,
  `ReviewPublished`).
- `identity::users` — user lifecycle events (`UserRegistered`, ...).
- `marketplace::listings` — listing lifecycle events (`ListingPublished`, ...).
- `marketplace::orders` — order lifecycle events (`OrderCreated`, `OrderConfirmed`,
  `OrderStatusChanged`, `OrderDispatched`, `OrderReplacementRequested`).
- `shipping::shipments` — carrier tracking events (`ShipmentStatusChanged`).

## Adding a new event

1. Create or reuse a module under `src/<domain>/<context>.rs`.
2. Define a `TOPIC_<EVENT>` `&str` constant next to the struct.
3. Derive `Serialize`, `Deserialize`, `Debug`, `Clone` plus `PartialEq` /
   `Eq` where the payload allows.
4. Add a serde round-trip test.
5. Bump the crate `version` and cut a new GitHub release.

## Compatibility notes

### 0.9.0

Two changes, and they are **not** the same kind of change. 0.8.0 was breaking because it
added a **variant** to an existing enum (`NovaPoshta` on `DeliveryMethod`) and serde
matches enum variants closed, so an old consumer DLQs the whole payload. 0.9.0 is a
different and narrower case, and the previous release's lesson does not transfer
unchanged.

- **`identity::profiles::ReviewPublished` is fully additive.** A new struct on a new topic
  (`identity.profiles.review_published`) adds no variant to any existing enum. No consumer
  breaks; nothing needs to move. Pinned by
  `nothing_on_the_previous_tag_breaks_on_the_new_event`.
- **`buyer_id` on `shipping::shipments::ShipmentStatusChanged` is backward-compatible but
  forward-incompatible.** serde ignores unknown fields, so `orders-service` pinned to
  0.8.0 keeps reading 0.9.0 payloads without error and **does not need a bump**. But a
  consumer pinned to 0.9.0 **cannot** read a payload produced by a 0.8.0-pinned producer,
  because the field would be missing and it is not `Option`. Pinned by
  `a_consumer_on_the_previous_tag_cannot_read_a_payload_without_buyer_id`.

**Deployment order is therefore load-bearing:** release 0.9.0, then deploy
`shipping-service` (the producer) **before** `notification-service` begins consuming.
`notification-service`'s new consumer group starts at `latest`, so it only ever sees
messages produced after it joins — which will be new-format ones provided
`shipping-service` went first.
