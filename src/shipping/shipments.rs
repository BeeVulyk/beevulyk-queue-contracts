//! Carrier tracking events.
//!
//! # Publication semantics for every event in this module
//!
//! `shipping-service` polls each carrier on a fixed interval and normalises whatever it
//! finds. It publishes ONLY when the normalised status DIFFERS from the last one recorded
//! for that tracking number — transitions, never polls. A tick that learns nothing new
//! writes to the service's own record and produces no message.
//!
//! Delivery is at-least-once. The service tracks what it has PUBLISHED separately from what
//! it has OBSERVED, so a crash between the database write and the produce republishes on the
//! next tick rather than losing the transition; consumers must therefore be idempotent.
//!
//! # A carrier fact replaces a timer; it never adds a state
//!
//! Nothing on this topic introduces an order state or a lifecycle edge. A `Delivered` fact
//! lets `orders-service` conclude early what its deadline sweeper would otherwise have
//! concluded on a timer, and the timer stays armed regardless — an order must never end up
//! with neither a carrier fact nor a timer.

use serde::{Deserialize, Serialize};

/// Kafka topic on which `ShipmentStatusChanged` events are published.
///
/// Convention: `<domain>.<context>.<event-name>`.
pub const TOPIC_SHIPMENT_STATUS_CHANGED: &str = "shipping.shipments.status_changed";

/// Dead-letter topic for `shipping.shipments.status_changed`.
pub const TOPIC_SHIPMENT_STATUS_CHANGED_DLQ: &str = "shipping.shipments.status_changed.dlq";

/// The postal carrier a shipment was handed to.
///
/// These are exactly the trackable members of
/// [`crate::marketplace::orders::DeliveryMethod`] (and so of the proto enum
/// `marketplace.reference.v1.DeliveryMethod`, minus `UNSPECIFIED`), and they MUST stay in
/// lockstep with it. `SelfPickup` and `CourierAgreed` have no carrier and no tracking
/// number, so they can never appear here — a self-pickup order produces no dispatch event
/// at all, and a privately arranged courier is not something the platform can poll.
///
/// **Adding a variant here is wire-breaking**, for the same reason spelled out at length on
/// [`crate::marketplace::orders::DeliveryMethod`]: serde matches enum variants CLOSED, so a
/// consumer on an older tag that receives an unknown slug fails to deserialise the whole
/// payload and DLQs it. Every consumer must move in the same release as the producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Carrier {
    Ukrposhta,
    NovaPoshta,
}

/// The normalised, publishable outcome of a shipment.
///
/// # This vocabulary is DELIBERATELY three members, and `InTransit` is DELIBERATELY not one
///
/// This is the design decision in this module most likely to be "helpfully" undone, so it is
/// documented at length. **If you are about to add an `InTransit` variant, stop — preventing
/// exactly that is why this type is shaped this way.**
///
/// Both carriers publish dozens of granular statuses: accepted, sorted, in transit, arrived
/// at branch, awaiting collection, and so on. The platform acts on almost none of them.
/// `shipping-service` keeps a WIDER internal vocabulary — `Unknown`, `InTransit`,
/// `Delivered`, `Returned`, `Lost` — normalises every granular status it does not act on to
/// the single internal in-transit value, records it, and does NOT publish it.
///
/// **So the wire is deliberately narrower than the domain.** Making the in-transit value
/// unrepresentable ON THE WIRE turns "publish transitions, not polls" from a rule a producer
/// has to remember into a guarantee the type system enforces: it is not possible to emit a
/// heartbeat on this topic, because no value exists that would express one. A consumer such
/// as `notification-service` therefore cannot later be wired up to tell a buyer their parcel
/// moved every fifteen minutes — not because someone would be told not to, but because the
/// contract gives them nothing to say it with.
///
/// `Unknown` is absent for a related reason: not having learned anything is a fact about our
/// poll, not about the parcel, and it belongs in the service's own record where the
/// never-asked and asked-nothing-new cases stay distinguishable.
///
/// Each of the three members is TERMINAL — the parcel's story with the carrier has ended.
/// That is what makes them worth a message.
///
/// **Adding a variant here is wire-breaking** (serde matches variants CLOSED — see
/// [`crate::marketplace::orders::DeliveryMethod`]), which is a second, independent reason not
/// to reach for one casually.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShipmentStatus {
    /// The carrier handed the parcel to the recipient.
    Delivered,
    /// The parcel came back to the sender.
    Returned,
    /// The carrier declared the parcel lost.
    Lost,
}

/// Emitted by `shipping-service` when a polled shipment's normalised status DIFFERS from the
/// last status recorded for that tracking number.
///
/// Producer: `shipping-service`. This is the platform's only producer that speaks to a
/// third party; every value on this event originates from a carrier's API and is normalised
/// before it is published.
///
/// Consumers: `orders-service` (group `orders-service-shipment-status`), which acts on
/// [`ShipmentStatus::Delivered`] ONLY. `Returned` and `Lost` are carried for observability
/// and for future consumers — today they move no order and add no state. Publishing them is
/// deliberate: they are terminal carrier facts, and a topic that only ever said "delivered"
/// would make a returned parcel indistinguishable from a parcel still in flight.
///
/// Publication semantics: see the module doc — transitions only, at-least-once, published
/// after the shipment row is updated.
///
/// Message key: `order_id`, NOT `tracking_number`. The order is the aggregate, so keying on
/// it keeps a shipment on the same partition as that order's other events and preserves
/// per-order ordering — the same key `OrderStatusChanged` and `OrderDispatched` use.
///
/// # Idempotency — read this before writing a consumer
///
/// Delivery is at-least-once, and the producer itself may legitimately republish after a
/// crash between its database write and its produce.
///
/// A consumer MUST dedupe in a processed-event ledger keyed by `order_id` AND the shipment
/// FACT the event establishes — one of `delivered`, `returned`, `lost` — written in the SAME
/// database transaction as whatever the consumer changes.
///
/// Key on the FACT, not on a poll and not on `observed_at_ms`, for exactly the reason
/// `OrderStatusChanged` keys on the stock fact: `order_id` alone is too narrow, because one
/// order can legitimately establish more than one fact on this topic, and a bare `order_id`
/// ledger would swallow the second as a duplicate.
///
/// # `occurred_at_ms` vs `observed_at_ms`
///
/// These are not interchangeable and the difference is not cosmetic.
///
/// **Consumers MUST use `occurred_at_ms` for anything that becomes a domain fact** — a
/// delivered-at written onto an order, a window that starts counting from delivery. When the
/// parcel was delivered is a fact about the parcel. When we noticed is a fact about our poll
/// interval, and using it would silently shift every such timestamp forward by up to one
/// interval, in a way no test would catch and no user could see.
///
/// `observed_at_ms` exists so that the lag between the two is auditable and a stuck poller is
/// diagnosable. It is NEVER for ordering and never for a domain fact.
///
/// # Late and out-of-order delivery
///
/// A consumer MUST tolerate an event about an order that has ALREADY concluded — the poll
/// interval, a retry, or a DLQ replay can all deliver a carrier fact after the order's
/// deadline sweeper reached the same conclusion on a timer, or after the order closed for
/// some other reason entirely. Such an event must never move an order backwards and must
/// never reopen it. It is an observation that arrived late, not a correction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShipmentStatusChanged {
    /// ULID (26-char Crockford base32) of the order this shipment belongs to. Also the Kafka
    /// message key, and the `order_id` half of the composite `(order_id, fact)` key the
    /// consumer's processed-event ledger MUST be built on.
    pub order_id: String,
    /// The carrier's tracking number (TTN). NEVER empty: a shipment with no tracking number
    /// is never polled and so produces no event at all. This is not `Option` — the absent
    /// case does not reach this topic.
    pub tracking_number: String,
    /// Which carrier the tracking number belongs to.
    pub carrier: Carrier,
    /// The normalised terminal outcome. Only ever one of three values — see
    /// [`ShipmentStatus`] for why in-transit is not among them.
    pub status: ShipmentStatus,
    /// Milliseconds since Unix epoch at which THE CARRIER says the status happened. This is
    /// the timestamp for any domain fact.
    pub occurred_at_ms: i64,
    /// Milliseconds since Unix epoch at which OUR poll saw it. An artefact of our poll
    /// schedule; for auditing lag and diagnosing a stuck poller, never for ordering and
    /// never for a domain fact.
    pub observed_at_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ShipmentStatusChanged {
        ShipmentStatusChanged {
            order_id: "01JABCORDER00000000000000".to_string(),
            tracking_number: "20450000000001".to_string(),
            carrier: Carrier::NovaPoshta,
            status: ShipmentStatus::Delivered,
            occurred_at_ms: 1_780_000_600_000,
            observed_at_ms: 1_780_000_900_000,
        }
    }

    #[test]
    fn shipment_status_changed_roundtrips() {
        let ev = sample();
        let json = serde_json::to_string(&ev).unwrap();
        assert!(json.contains("\"carrier\":\"nova_poshta\""), "{json}");
        assert!(json.contains("\"status\":\"delivered\""), "{json}");
        assert!(
            json.contains("\"tracking_number\":\"20450000000001\""),
            "{json}"
        );
        let back: ShipmentStatusChanged = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ev);
    }

    #[test]
    fn dlq_topic_is_the_topic_plus_suffix() {
        assert_eq!(
            TOPIC_SHIPMENT_STATUS_CHANGED_DLQ,
            format!("{TOPIC_SHIPMENT_STATUS_CHANGED}.dlq")
        );
    }

    #[test]
    fn topic_names_are_pinned() {
        assert_eq!(
            TOPIC_SHIPMENT_STATUS_CHANGED,
            "shipping.shipments.status_changed"
        );
        assert_eq!(
            TOPIC_SHIPMENT_STATUS_CHANGED_DLQ,
            "shipping.shipments.status_changed.dlq"
        );
    }

    #[test]
    fn enum_wire_slugs_are_pinned() {
        for (carrier, slug) in [
            (Carrier::Ukrposhta, "ukrposhta"),
            (Carrier::NovaPoshta, "nova_poshta"),
        ] {
            assert_eq!(
                serde_json::to_string(&carrier).unwrap(),
                format!("\"{slug}\"")
            );
        }

        for (status, slug) in [
            (ShipmentStatus::Delivered, "delivered"),
            (ShipmentStatus::Returned, "returned"),
            (ShipmentStatus::Lost, "lost"),
        ] {
            assert_eq!(
                serde_json::to_string(&status).unwrap(),
                format!("\"{slug}\"")
            );
        }
    }

    /// The wire vocabulary is narrower than the service's internal one, and that narrowness
    /// is the whole design: "publish transitions, not polls" is meant to be enforced by the
    /// type system rather than by a producer's discipline.
    ///
    /// This test pins the guarantee from the OUTSIDE — as a consumer sees it — rather than
    /// restating it in a comment. A producer that tried to emit a heartbeat would have to
    /// invent a slug, and every slug it could invent is rejected here.
    #[test]
    fn no_in_transit_value_is_representable_on_the_wire() {
        for slug in [
            "in_transit",
            "unknown",
            "accepted",
            "sorted",
            "arrived_at_branch",
            "awaiting_collection",
        ] {
            assert!(
                serde_json::from_str::<ShipmentStatus>(&format!("\"{slug}\"")).is_err(),
                "`{slug}` deserialised into ShipmentStatus — the wire vocabulary has been \
                 widened beyond the three terminal facts, which defeats the type-level \
                 guarantee that a heartbeat cannot be published on this topic"
            );
        }

        // And the exhaustive statement of the same thing: exactly three members, all
        // terminal. A new variant makes this match non-exhaustive and fails to compile,
        // which is the point at which someone must come and read the doc on `ShipmentStatus`.
        fn is_terminal(status: ShipmentStatus) -> bool {
            match status {
                ShipmentStatus::Delivered | ShipmentStatus::Returned | ShipmentStatus::Lost => true,
            }
        }
        assert!(is_terminal(ShipmentStatus::Delivered));
        assert!(is_terminal(ShipmentStatus::Returned));
        assert!(is_terminal(ShipmentStatus::Lost));
    }

    /// `occurred_at_ms` and `observed_at_ms` are both `i64` milliseconds, so nothing but a
    /// consumer's attention keeps them apart. They are pinned as two distinct fields, in a
    /// sample where the poll saw a delivery five minutes after it happened — the exact skew
    /// a consumer would silently introduce by reading the wrong one.
    #[test]
    fn both_timestamps_survive_the_wire_as_separate_fields() {
        let ev = sample();
        let json = serde_json::to_string(&ev).unwrap();
        assert!(json.contains("\"occurred_at_ms\":1780000600000"), "{json}");
        assert!(json.contains("\"observed_at_ms\":1780000900000"), "{json}");

        let back: ShipmentStatusChanged = serde_json::from_str(&json).unwrap();
        assert_ne!(back.occurred_at_ms, back.observed_at_ms);
        assert!(back.observed_at_ms > back.occurred_at_ms);
    }
}
