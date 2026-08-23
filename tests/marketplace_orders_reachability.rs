//! Proves the `marketplace::orders` surface added in 0.6.0 is reachable from OUTSIDE
//! the crate.
//!
//! The unit tests in `src/marketplace/orders.rs` use `super::*`, which resolves whether
//! or not the module is registered in `lib.rs` and whether or not each item is `pub`.
//! This file is a separate compilation unit that links against the published crate, so
//! the `use` statements below are exactly what a producer in `orders-service` and a
//! consumer in `listings-service` will write. If any item were private, or
//! `marketplace/mod.rs` stopped declaring `pub mod orders`, this file would fail to
//! compile.

// Exactly the import a producer in `orders-service` writes.
use beevulyk_queue_contracts::marketplace::orders::{
    ActorType, ClaimReasonCode, OrderConfirmed, OrderReplacementRequested, OrderStatus,
    OrderStatusChanged, OrderStockLine, ReplacementClaimLine, TOPIC_ORDER_CONFIRMED,
    TOPIC_ORDER_CONFIRMED_DLQ, TOPIC_ORDER_REPLACEMENT_REQUESTED,
    TOPIC_ORDER_REPLACEMENT_REQUESTED_DLQ, TOPIC_ORDER_STATUS_CHANGED,
    TOPIC_ORDER_STATUS_CHANGED_DLQ,
};

fn lines() -> Vec<OrderStockLine> {
    vec![OrderStockLine {
        listing_id: "01JABCLISTING0000000000000".to_string(),
        quantity: 3,
    }]
}

/// The producer side: build and serialise each event against its topic constant.
#[test]
fn a_producer_can_build_and_serialise_every_new_event() {
    let confirmed = OrderConfirmed {
        order_id: "01JABCORDER00000000000000".to_string(),
        buyer_id: "01JABCBUYER00000000000000".to_string(),
        seller_id: "01JABCSELLER00000000000000".to_string(),
        items: lines(),
        confirmed_at_ms: 1_780_000_100_000,
    };
    assert_eq!(TOPIC_ORDER_CONFIRMED, "marketplace.orders.confirmed");
    assert!(!serde_json::to_string(&confirmed).unwrap().is_empty());

    let changed = OrderStatusChanged {
        order_id: "01JABCORDER00000000000000".to_string(),
        buyer_id: "01JABCBUYER00000000000000".to_string(),
        seller_id: "01JABCSELLER00000000000000".to_string(),
        from_status: Some(OrderStatus::Confirmed),
        to_status: OrderStatus::Cancelled,
        actor_type: ActorType::Buyer,
        actor_id: Some("01JABCBUYER00000000000000".to_string()),
        reason: Some("buyer_changed_mind".to_string()),
        items: lines(),
        at_ms: 1_780_000_300_000,
    };
    assert_eq!(
        TOPIC_ORDER_STATUS_CHANGED,
        "marketplace.orders.status_changed"
    );
    assert!(!serde_json::to_string(&changed).unwrap().is_empty());

    let requested = OrderReplacementRequested {
        order_id: "01JABCORDER00000000000000".to_string(),
        buyer_id: "01JABCBUYER00000000000000".to_string(),
        seller_id: "01JABCSELLER00000000000000".to_string(),
        claims: vec![ReplacementClaimLine {
            claim_id: "01JABCCLAIM00000000000000".to_string(),
            order_item_id: "01JABCITEM000000000000000".to_string(),
            listing_id: "01JABCLISTING0000000000000".to_string(),
            quantity_claimed: 2,
            reason_code: ClaimReasonCode::ColonyDidNotEstablish,
        }],
        requested_at_ms: 1_780_000_400_000,
    };
    assert_eq!(
        TOPIC_ORDER_REPLACEMENT_REQUESTED,
        "marketplace.orders.replacement_requested"
    );
    assert!(!serde_json::to_string(&requested).unwrap().is_empty());
}

/// The consumer side: deserialise a raw wire payload of the shape `listings-service`
/// will actually receive, and read the fields it needs to adjust its counter.
#[test]
fn the_listings_consumer_can_deserialise_and_read_a_confirmed_payload() {
    let wire = r#"{
        "order_id": "01JABCORDER00000000000000",
        "buyer_id": "01JABCBUYER00000000000000",
        "seller_id": "01JABCSELLER00000000000000",
        "items": [{"listing_id": "01JABCLISTING0000000000000", "quantity": 3}],
        "confirmed_at_ms": 1780000100000
    }"#;

    let ev: OrderConfirmed = serde_json::from_str(wire).unwrap();

    // The dedup key the processed-event ledger must be built on.
    assert_eq!(ev.order_id, "01JABCORDER00000000000000");
    // The delta the consumer applies: quantity_available - 3.
    assert_eq!(ev.items.len(), 1);
    assert_eq!(ev.items[0].listing_id, "01JABCLISTING0000000000000");
    assert_eq!(ev.items[0].quantity, 3);
}

/// `from_status` is what tells the consumer whether stock had ever been taken, so it
/// must survive the wire as an explicit `null` on creation and as a value otherwise.
#[test]
fn the_listings_consumer_can_tell_a_restoring_transition_from_a_non_restoring_one() {
    let never_decremented = r#"{
        "order_id": "01JABCORDER00000000000000",
        "buyer_id": "01JABCBUYER00000000000000",
        "seller_id": "01JABCSELLER00000000000000",
        "from_status": "pending_confirmation",
        "to_status": "rejected",
        "actor_type": "seller",
        "actor_id": "01JABCSELLER00000000000000",
        "reason": "out_of_stock",
        "items": [{"listing_id": "01JABCLISTING0000000000000", "quantity": 3}],
        "at_ms": 1780000300000
    }"#;
    let ev: OrderStatusChanged = serde_json::from_str(never_decremented).unwrap();
    assert_eq!(ev.from_status, Some(OrderStatus::PendingConfirmation));
    assert_eq!(ev.to_status, OrderStatus::Rejected);

    let did_decrement = r#"{
        "order_id": "01JABCORDER00000000000000",
        "buyer_id": "01JABCBUYER00000000000000",
        "seller_id": "01JABCSELLER00000000000000",
        "from_status": "shipped",
        "to_status": "closed_unresolved",
        "actor_type": "system",
        "actor_id": null,
        "reason": "seller_silent",
        "items": [{"listing_id": "01JABCLISTING0000000000000", "quantity": 3}],
        "at_ms": 1780000500000
    }"#;
    let ev: OrderStatusChanged = serde_json::from_str(did_decrement).unwrap();
    assert_eq!(ev.from_status, Some(OrderStatus::Shipped));
    assert_eq!(ev.actor_type, ActorType::System);
    // A system sweep names no account.
    assert_eq!(ev.actor_id, None);
    assert_eq!(ev.reason.as_deref(), Some("seller_silent"));
}

/// Every new DLQ constant is its topic plus `.dlq`, asserted from outside the crate so
/// the pairing is part of the published contract rather than an internal detail.
#[test]
fn every_new_dlq_constant_is_reachable_and_correct() {
    for (topic, dlq) in [
        (TOPIC_ORDER_CONFIRMED, TOPIC_ORDER_CONFIRMED_DLQ),
        (TOPIC_ORDER_STATUS_CHANGED, TOPIC_ORDER_STATUS_CHANGED_DLQ),
        (
            TOPIC_ORDER_REPLACEMENT_REQUESTED,
            TOPIC_ORDER_REPLACEMENT_REQUESTED_DLQ,
        ),
    ] {
        assert_eq!(dlq, format!("{topic}.dlq"));
    }
}
