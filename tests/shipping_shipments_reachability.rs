//! Proves the `shipping::shipments` surface added in 0.8.0 is reachable from OUTSIDE the
//! crate.
//!
//! The unit tests in `src/shipping/shipments.rs` use `super::*`, which resolves whether or
//! not the module is registered in `lib.rs` and whether or not each item is `pub`. Because
//! `shipping` is a NEW top-level module in this release, that gap is not hypothetical: every
//! one of those unit tests would pass with `pub mod shipping;` missing from `lib.rs`, and no
//! consumer could import a thing.
//!
//! This file is a separate compilation unit that links against the published crate, so the
//! `use` statement below is exactly what a producer in `shipping-service` and a consumer in
//! `orders-service` will write.

// Exactly the import a producer in `shipping-service` writes.
use beevulyk_queue_contracts::shipping::shipments::{
    Carrier, ShipmentStatus, ShipmentStatusChanged, TOPIC_SHIPMENT_STATUS_CHANGED,
    TOPIC_SHIPMENT_STATUS_CHANGED_DLQ,
};

/// The producer side: build and serialise the event against its topic constant.
#[test]
fn a_producer_can_build_and_serialise_the_event() {
    let ev = ShipmentStatusChanged {
        order_id: "01JABCORDER00000000000000".to_string(),
        tracking_number: "0501234567890".to_string(),
        carrier: Carrier::Ukrposhta,
        status: ShipmentStatus::Delivered,
        occurred_at_ms: 1_780_000_600_000,
        observed_at_ms: 1_780_000_900_000,
    };

    assert_eq!(
        TOPIC_SHIPMENT_STATUS_CHANGED,
        "shipping.shipments.status_changed"
    );
    assert_eq!(
        TOPIC_SHIPMENT_STATUS_CHANGED_DLQ,
        format!("{TOPIC_SHIPMENT_STATUS_CHANGED}.dlq")
    );

    let json = serde_json::to_string(&ev).unwrap();
    assert!(json.contains("\"carrier\":\"ukrposhta\""), "{json}");
    assert!(json.contains("\"status\":\"delivered\""), "{json}");
}

/// The consumer side: deserialise a raw wire payload of the shape `orders-service` will
/// actually receive, and read the fields it acts on.
#[test]
fn the_orders_consumer_can_deserialise_and_read_a_delivered_payload() {
    let wire = r#"{
        "order_id": "01JABCORDER00000000000000",
        "tracking_number": "20450000000001",
        "carrier": "nova_poshta",
        "status": "delivered",
        "occurred_at_ms": 1780000600000,
        "observed_at_ms": 1780000900000
    }"#;

    let ev: ShipmentStatusChanged = serde_json::from_str(wire).unwrap();

    // The `order_id` half of the composite `(order_id, fact)` ledger key; the fact this
    // event establishes is `delivered`. It is also the Kafka message key.
    assert_eq!(ev.order_id, "01JABCORDER00000000000000");
    assert_eq!(ev.status, ShipmentStatus::Delivered);
    assert_eq!(ev.carrier, Carrier::NovaPoshta);

    // The timestamp that becomes a domain fact is the CARRIER's, not our poll's. Reading
    // `observed_at_ms` here would shift the recorded delivery time forward by up to one
    // poll interval — five minutes in this payload.
    assert_eq!(ev.occurred_at_ms, 1_780_000_600_000);
    assert_ne!(ev.occurred_at_ms, ev.observed_at_ms);
}

/// `Returned` and `Lost` are carried for observability: `orders-service` acts on `Delivered`
/// alone today, but both must survive the wire for the consumers that will read them.
#[test]
fn the_non_actionable_terminal_facts_also_survive_the_wire() {
    for (slug, expected) in [
        ("returned", ShipmentStatus::Returned),
        ("lost", ShipmentStatus::Lost),
    ] {
        let wire = format!(
            r#"{{
                "order_id": "01JABCORDER00000000000000",
                "tracking_number": "20450000000001",
                "carrier": "nova_poshta",
                "status": "{slug}",
                "occurred_at_ms": 1780000600000,
                "observed_at_ms": 1780000900000
            }}"#
        );
        let ev: ShipmentStatusChanged = serde_json::from_str(&wire).unwrap();
        assert_eq!(ev.status, expected);
    }
}

/// The wire vocabulary is narrower than `shipping-service`'s internal one, and this is the
/// guarantee that keeps it that way: a poll that learned nothing has no slug to travel under,
/// so "publish transitions, not polls" cannot be violated by a producer's slip.
///
/// Asserted from outside the crate because it is a promise made to CONSUMERS —
/// `notification-service` cannot be turned into a per-poll notifier, because a payload
/// carrying a heartbeat does not deserialise.
#[test]
fn an_in_transit_heartbeat_is_not_representable_on_this_topic() {
    let heartbeat = r#"{
        "order_id": "01JABCORDER00000000000000",
        "tracking_number": "20450000000001",
        "carrier": "nova_poshta",
        "status": "in_transit",
        "occurred_at_ms": 1780000600000,
        "observed_at_ms": 1780000900000
    }"#;

    assert!(
        serde_json::from_str::<ShipmentStatusChanged>(heartbeat).is_err(),
        "an in-transit heartbeat deserialised — the type-level guarantee that this topic \
         carries only terminal facts has been lost"
    );
}
