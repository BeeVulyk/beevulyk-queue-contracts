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
/// will actually receive, and read the fields it needs to move its stock counters.
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

    // The `order_id` half of the composite `(order_id, kind)` ledger key; the `kind`
    // this event establishes is `reserved`.
    assert_eq!(ev.order_id, "01JABCORDER00000000000000");
    // The delta the consumer applies: reserved + 3, which takes the same 3 off what
    // the listing offers.
    assert_eq!(ev.items.len(), 1);
    assert_eq!(ev.items[0].listing_id, "01JABCLISTING0000000000000");
    assert_eq!(ev.items[0].quantity, 3);
}

/// A stock consumer classifies the `(from_status, to_status)` PAIR — reserve, consume,
/// release or nothing — so BOTH halves of the pair have to survive the wire, on every
/// class of edge and not only on the one that moves a counter downwards.
///
/// The three payloads below are one of each class a consumer must tell apart, written
/// as the raw JSON `listings-service` actually receives. **The assertions are
/// deserialisation checks and nothing more**: the classification itself belongs to the
/// consumer, which does not live in this repo, so what is pinned here is that the fields
/// it classifies on arrive intact and with the wire slugs the contract promises.
#[test]
fn every_class_of_stock_edge_survives_the_wire() {
    // NOTHING — before any reservation exists. Nothing was ever taken for this order,
    // so there is nothing to give back.
    let before_any_reservation = r#"{
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
    let ev: OrderStatusChanged = serde_json::from_str(before_any_reservation).unwrap();
    assert_eq!(ev.from_status, Some(OrderStatus::PendingConfirmation));
    assert_eq!(ev.to_status, OrderStatus::Rejected);

    // RELEASE — a terminal state reached before dispatch. The T5 sweep closes a
    // self-pickup order nobody ever collected, and the reservation is given back.
    let release_before_dispatch = r#"{
        "order_id": "01JABCORDER00000000000000",
        "buyer_id": "01JABCBUYER00000000000000",
        "seller_id": "01JABCSELLER00000000000000",
        "from_status": "ready",
        "to_status": "expired",
        "actor_type": "system",
        "actor_id": null,
        "reason": null,
        "items": [{"listing_id": "01JABCLISTING0000000000000", "quantity": 3}],
        "at_ms": 1780000400000
    }"#;
    let ev: OrderStatusChanged = serde_json::from_str(release_before_dispatch).unwrap();
    assert_eq!(ev.from_status, Some(OrderStatus::Ready));
    assert_eq!(ev.to_status, OrderStatus::Expired);
    assert_eq!(ev.actor_type, ActorType::System);
    // A system sweep names no account, and an expiry needs no reason: both `None`
    // arrive as an explicit `null` on the wire.
    assert_eq!(ev.actor_id, None);
    assert_eq!(ev.reason, None);
    // The lines ride along on a release exactly as they do on a reservation, so the
    // consumer can mirror the move without holding a copy of the order.
    assert_eq!(ev.items.len(), 1);
    assert_eq!(ev.items[0].quantity, 3);

    // NOTHING — after dispatch. Non-delivery: the parcel never arrived, but the queens
    // left the apiary when it was handed to the carrier, so they were consumed then and
    // this edge moves nothing. `from_status` is the only thing separating it from an
    // edge that releases, which is why the pair is what a consumer must read.
    let after_dispatch = r#"{
        "order_id": "01JABCORDER00000000000000",
        "buyer_id": "01JABCBUYER00000000000000",
        "seller_id": "01JABCSELLER00000000000000",
        "from_status": "shipped",
        "to_status": "closed_unresolved",
        "actor_type": "buyer",
        "actor_id": "01JABCBUYER00000000000000",
        "reason": "not_delivered",
        "items": [{"listing_id": "01JABCLISTING0000000000000", "quantity": 3}],
        "at_ms": 1780000500000
    }"#;
    let ev: OrderStatusChanged = serde_json::from_str(after_dispatch).unwrap();
    assert_eq!(ev.from_status, Some(OrderStatus::Shipped));
    assert_eq!(ev.to_status, OrderStatus::ClosedUnresolved);
    assert_eq!(ev.actor_type, ActorType::Buyer);
    assert_eq!(ev.reason.as_deref(), Some("not_delivered"));
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
