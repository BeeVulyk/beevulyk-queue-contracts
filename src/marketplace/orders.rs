//! Order lifecycle events.
//!
//! # Publication semantics for every event in this module
//!
//! At-least-once, best-effort direct publish AFTER the database transaction commits.
//! There is no outbox. A produce failure is logged and swallowed; it does NOT fail the
//! RPC, because by that point the transition has already happened and failing the call
//! would tell the caller nothing changed when it did.
//!
//! That trade-off is acceptable ONLY while no consumer failure is user-visible, and it
//! has an explicit expiry: **a transactional outbox in `orders-service` becomes
//! mandatory before `notification-service` ships.** A dropped
//! `marketplace.orders.status_changed` would then mean a buyer is never told their
//! queens shipped — an omission nobody can see and nothing repairs. `listings-service`
//! does not need one: its `quantity_available` counter is an advertisement, not a
//! ledger.
//!
//! # Idempotency is NOT uniform across this crate
//!
//! Every event written before `OrderConfirmed` carries ABSOLUTE state, which makes its
//! consumers idempotent by construction — replaying any prefix of the stream converges.
//! `OrderConfirmed` and `OrderStatusChanged` drive a DELTA on the `listings-service`
//! stock counter, and a delta is not idempotent under at-least-once delivery. See
//! `OrderConfirmed` for the ledger a consumer is required to keep; do NOT carry the
//! commit-on-success pattern of `ProfileVerificationChanged` over to these two.

use serde::{Deserialize, Serialize};

use crate::marketplace::listings::ProductCategory;

/// Kafka topic on which `OrderCreated` events are published.
///
/// Convention: `<domain>.<context>.<event-name>`.
pub const TOPIC_ORDER_CREATED: &str = "marketplace.orders.created";

/// Dead-letter topic for `marketplace.orders.created`.
pub const TOPIC_ORDER_CREATED_DLQ: &str = "marketplace.orders.created.dlq";

/// Lifecycle state of the order. Variants MUST stay in lockstep with the proto enum
/// `marketplace.orders.v1.OrderStatus`, minus `UNSPECIFIED` — that value is never a
/// stored value and is never emitted by the service, so it has no wire slug here.
///
/// Only `PendingConfirmation` is ever emitted in this release: `OrderCreated` fires
/// at creation and nothing transitions an order yet. The remaining variants are
/// present so that the transition work (TASK-48) is an additive change to the
/// producer, not a wire change to this contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    PendingConfirmation,
    Confirmed,
    Ready,
    Shipped,
    Observation,
    ReplacementRequested,
    Completed,
    ClosedReplaced,
    ClosedUnresolved,
    Rejected,
    Cancelled,
    Expired,
}

/// How the order must be fulfilled. Variants MUST stay in lockstep with the proto
/// enum `marketplace.orders.v1.FulfilmentClass`, minus `UNSPECIFIED`.
///
/// Derived ONCE at creation from the item's category and then frozen onto the order;
/// never recomputed. `Live` covers queens, packages and colonies; `Goods` covers
/// everything else.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FulfilmentClass {
    Live,
    Goods,
}

/// How the goods reach the buyer. Variants MUST stay in lockstep with the proto enum
/// `marketplace.reference.v1.DeliveryMethod`, minus `UNSPECIFIED`.
///
/// `NovaPoshta` IS DELIBERATELY ABSENT AND MUST NOT BE ADDED. Nova Poshta prohibits
/// sending insects outright; offering it would route the wedge category (queens) into
/// a carrier that forbids the shipment, voiding any recourse against them and
/// producing disputes the platform has decided not to arbitrate (TASK-67).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMethod {
    Ukrposhta,
    SelfPickup,
    CourierAgreed,
}

/// Unit in which the ordered quantity is counted. Variants MUST stay in lockstep with
/// the proto enum `marketplace.reference.v1.QuantityUnit`, minus `UNSPECIFIED`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuantityUnit {
    Piece,
    Kilogram,
    Litre,
    Set,
}

/// ISO 4217 alphabetic currency code. Variants MUST stay in lockstep with the proto
/// enum `common.money.v1.Currency`, minus `UNSPECIFIED` — the proto states that value
/// is never a stored value, so it has no wire slug here.
///
/// Serialises UPPERCASE (`"UAH"`), matching the ISO-4217 code stored in the
/// `total_currency` / `unit_price_currency` database columns and the code the proto
/// enum spells out.
///
/// Amounts are ALWAYS integer minor units (kopiyky) — never a float, never a decimal
/// string. There is deliberately no combined money struct: an amount and its currency
/// travel as two sibling fields on the owning struct, exactly as in
/// `common/money/v1/money.proto`, which keeps the JSON flat and matches the two
/// separate SQL columns. Introducing `Money { amount, currency }` later would be a
/// wire break on every carrier, so do not add one speculatively.
///
/// This is the first money-carrying type in the crate; no event carried money before,
/// which is why it lives in `marketplace::orders` — the module that first needs it,
/// the same precedent `ProductCategory` set in `marketplace::listings`. If a second,
/// non-marketplace event ever needs a currency, hoist it to a shared module THEN, not
/// speculatively now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Currency {
    Uah,
}

/// Who performed a transition. Variants MUST stay in lockstep with the proto enum
/// `marketplace.orders.v1.ActorType`, minus `UNSPECIFIED`.
///
/// `System` is the deadline sweeper and never a human. An event whose `actor_type` is
/// `System` carries no `actor_id`, because there is no account behind it to name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorType {
    Buyer,
    Seller,
    System,
}

/// What the buyer is claiming. Variants MUST stay in lockstep with the proto enum
/// `marketplace.orders.v1.ClaimReasonCode`, minus `UNSPECIFIED`.
///
/// The commitment recorded here is the SELLER's: that a live queen begins laying within
/// the observation window. Wrong breed, short count and dead-on-arrival are deliberately
/// absent, and there is no catch-all `Other`, so that no claim code exists which cannot
/// lead anywhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimReasonCode {
    QueenNotLaying,
    ColonyDidNotEstablish,
}

/// One ordered line, snapshotted from the listing at creation.
///
/// Every field is a frozen copy taken at order time. The listing it came from may be
/// edited, closed or archived afterwards; this snapshot never changes, because the
/// order records what the parties actually agreed to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderCreatedItem {
    /// ULID (26-char Crockford base32).
    pub order_item_id: String,
    /// ULID. Soft reference to `listings_service.listings` — the listing may be
    /// edited, closed or archived later; this snapshot never changes.
    pub listing_id: String,
    /// Ordered quantity, always >= 1.
    pub quantity: i32,
    /// Snapshot of the listing title at creation.
    pub title: String,
    /// Snapshot of the listing category at creation.
    pub category: ProductCategory,
    /// INTEGER MINOR UNITS (kopiyky). Never a float, never a bare number.
    pub unit_price_amount: i64,
    /// Currency of `unit_price_amount`. Always travels beside its amount.
    pub unit_price_currency: Currency,
    /// Unit `quantity` is counted in.
    pub quantity_unit: QuantityUnit,
}

/// Emitted by `orders-service` after a successful `CreateOrder` RPC — that is, once
/// the order row, its item rows and the frozen fulfilment policy have been committed.
///
/// Producer: `orders-service`.
/// Consumers: none yet. TASK-54's `notification-service` and TASK-48 (order state
/// transitions) will be the first. Do not add a consumer here.
///
/// Publication semantics: at-least-once, best-effort direct publish (no outbox in
/// MVP-1). The event is published AFTER the database transaction commits. A produce
/// failure is logged and swallowed — it does NOT fail `CreateOrder`, because by that
/// point the order is already real and legally binding on the parties, and failing
/// the RPC would tell the buyer nothing happened when it did.
///
/// That trade-off has an explicit expiry: **a transactional outbox becomes mandatory
/// in `orders-service` before `notification-service` ships**, because a dropped event
/// would then mean a seller is never told an order arrived. Not now — today there is
/// no consumer whose absence a user could notice.
///
/// Message key: `order_id`, for partition affinity by aggregate.
///
/// Idempotency: consumers must key on `order_id`. The payload carries absolute state,
/// never a delta.
///
/// This is the ONLY event on the platform that carries item detail, and that width is
/// deliberate. Anything rendering a notification about an order needs the listing
/// titles, categories and prices; a consumer that had to call back into
/// `orders-service` (and through it `listings-service`) for them would turn one event
/// into a fan-out of synchronous reads on the order path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderCreated {
    /// ULID (26-char Crockford base32).
    pub order_id: String,
    /// ULID of the buyer. Soft reference to `users_service.users`.
    pub buyer_id: String,
    /// ULID of the seller. Soft reference to `users_service.users`.
    pub seller_id: String,
    /// Always `PendingConfirmation` in this release.
    pub status: OrderStatus,
    /// Frozen at creation from the item's category; never recomputed.
    pub fulfilment_class: FulfilmentClass,
    /// INTEGER MINOR UNITS (kopiyky). EXCLUDES shipping — the recipient pays the
    /// carrier at the counter, so the platform never quotes or collects it.
    pub total_amount: i64,
    /// Currency of `total_amount`. Always travels beside its amount.
    pub total_currency: Currency,
    /// How the goods reach the buyer, snapshotted from the listing.
    pub delivery_method: DeliveryMethod,
    /// Exactly one element in this release. This is the ONLY event on the platform
    /// carrying item detail, because anything rendering an order notification needs
    /// the titles and would otherwise have to call back into orders-service.
    pub items: Vec<OrderCreatedItem>,
    /// Milliseconds since Unix epoch at which the order row was created.
    pub created_at_ms: i64,
}

/// One order line reduced to what a stock counter needs. Deliberately not
/// `OrderCreatedItem`: a consumer adjusting `quantity_available` needs the listing and
/// the count and nothing else, and shipping the price snapshot to it would invite a
/// consumer to start reasoning about money.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderStockLine {
    /// ULID. Soft reference to `listings_service.listings`.
    pub listing_id: String,
    /// Ordered quantity, always >= 1. The magnitude of the adjustment, never a
    /// resulting total.
    pub quantity: i32,
}

/// One claim line. The observation window is per ORDER — one delivery is one clock —
/// but a claim is per ITEM and per QUANTITY: if three of ten queens never started
/// laying, the claim concerns three queens on one line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplacementClaimLine {
    /// ULID (26-char Crockford base32) of the claim row.
    pub claim_id: String,
    /// ULID of the claimed order line. Soft reference to `orders_service.order_items`.
    pub order_item_id: String,
    /// ULID. Soft reference to `listings_service.listings`.
    pub listing_id: String,
    /// How many units of the line are claimed: >= 1 and <= the line's ordered quantity.
    pub quantity_claimed: i32,
    /// What the buyer asserts about those units.
    pub reason_code: ClaimReasonCode,
}

/// Kafka topic on which `OrderConfirmed` events are published.
///
/// Convention: `<domain>.<context>.<event-name>`.
pub const TOPIC_ORDER_CONFIRMED: &str = "marketplace.orders.confirmed";

/// Dead-letter topic for `marketplace.orders.confirmed`.
pub const TOPIC_ORDER_CONFIRMED_DLQ: &str = "marketplace.orders.confirmed.dlq";

/// Emitted by `orders-service` once a seller has confirmed an order and the
/// `PendingConfirmation` -> `Confirmed` transition has committed.
///
/// Producer: `orders-service`.
/// Consumers: `listings-service` (group `listings-service-order-confirmed`), which
/// DECREMENTS the soft `quantity_available` counter of every listing in `items`.
///
/// This event is separate from `OrderStatusChanged` — which also reports the move to
/// `Confirmed` — because it is the one transition with a FUNCTIONAL consumer rather
/// than an observer. `listings-service` changes stored state in response to it;
/// everything reading `status_changed` merely watches. Keeping them apart lets the
/// functional consumer subscribe to exactly the transition it must act on instead of
/// filtering a firehose, and stops a newly added state from silently changing what it
/// acts on.
///
/// Publication semantics: see the module doc — best-effort publish after commit, no
/// outbox yet.
///
/// Message key: `order_id`, so every event for one order lands on one partition and
/// reaches the consumer in transition order.
///
/// # Idempotency — read this before writing a consumer
///
/// Delivery is at-least-once and the consumer applies a DELTA
/// (`quantity_available - n`), not an absolute value. **A delta is not idempotent.** A
/// redelivered message — a rebalance, a retry, a restart between the write and the
/// offset commit — decrements a second time. The counter is then permanently wrong, no
/// later event corrects it, and nothing surfaces the error: the listing simply
/// advertises fewer queens than the seller has.
///
/// The commit-on-success pattern used by `ProfileVerificationChanged` is **NOT
/// sufficient here.** That handler writes an ABSOLUTE `verification_status`, so
/// replaying it converges; this one does not, and copying it across is the specific
/// mistake this paragraph exists to prevent.
///
/// A consumer MUST keep a processed-event ledger keyed by `order_id`, insert into it IN
/// THE SAME DATABASE TRANSACTION as the decrement, and skip the adjustment when the key
/// is already present. The payload carries `order_id` and every `listing_id` with its
/// `quantity` precisely so a consumer can do that without calling back into
/// `orders-service`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderConfirmed {
    /// ULID (26-char Crockford base32). Also the Kafka message key, and the
    /// deduplication key the consumer's processed-event ledger MUST be built on.
    pub order_id: String,
    /// ULID of the buyer. Soft reference to `users_service.users`.
    pub buyer_id: String,
    /// ULID of the seller. Soft reference to `users_service.users`.
    pub seller_id: String,
    /// Every line of the order, so all decrements can be applied in one transaction.
    pub items: Vec<OrderStockLine>,
    /// Milliseconds since Unix epoch at which the confirmation committed.
    pub confirmed_at_ms: i64,
}

/// Kafka topic on which `OrderStatusChanged` events are published.
///
/// Convention: `<domain>.<context>.<event-name>`.
pub const TOPIC_ORDER_STATUS_CHANGED: &str = "marketplace.orders.status_changed";

/// Dead-letter topic for `marketplace.orders.status_changed`.
pub const TOPIC_ORDER_STATUS_CHANGED_DLQ: &str = "marketplace.orders.status_changed.dlq";

/// Emitted by `orders-service` on EVERY committed lifecycle transition, whoever or
/// whatever caused it.
///
/// Producer: `orders-service`.
/// Consumers: `listings-service` (group `listings-service-order-status-changed`), which
/// RESTORES the soft `quantity_available` counter when an order that had taken stock is
/// abandoned. `notification-service` (TASK-54) will be the second, and is the reason
/// the outbox deadline in the module doc exists.
///
/// ONE topic, not one per state. The meaningful fact is the TRANSITION; the state it
/// landed in is a field. A topic per state would multiply the topic count by twelve,
/// force every observer to subscribe to all of them to follow one order, and turn
/// adding a thirteenth state into a deployment change for every consumer.
///
/// Publication semantics: see the module doc — best-effort publish after commit, no
/// outbox yet. This is the event whose loss `notification-service` will make visible.
///
/// Message key: `order_id`.
///
/// # Why `items` rides on a status event
///
/// `listings-service` must give back the stock it took when a confirmed order is later
/// abandoned, and it holds no copy of the order — it can only do that if the event
/// carries the lines. They use the same `OrderStockLine` shape as `OrderConfirmed`, so
/// the increment is the exact mirror of the decrement.
///
/// # `from_status` decides whether stock is restored
///
/// Stock is taken at `Confirmed` and nowhere else, so only a transition OUT of a state
/// that had already taken it may give it back. A transition out of `PendingConfirmation`
/// — the seller rejected the request, or it expired unanswered — never decremented
/// anything, and restoring on it would inflate `quantity_available` above what the
/// seller actually has. Transitions out of `Confirmed`, `Ready`, `Shipped`,
/// `Observation` and `ReplacementRequested` DID decrement and must restore.
///
/// # Idempotency
///
/// The same delta hazard as `OrderConfirmed`, in the opposite direction: the restore is
/// an INCREMENT, and a redelivered message inflates the counter permanently and
/// silently. A consumer MUST dedupe in a processed-event ledger written in the same
/// transaction as the adjustment — keyed by `order_id` AND `to_status`, because one
/// order legitimately produces many events on this topic and `order_id` alone would
/// swallow every event after the first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderStatusChanged {
    /// ULID (26-char Crockford base32). Also the Kafka message key.
    pub order_id: String,
    /// ULID of the buyer. Soft reference to `users_service.users`.
    pub buyer_id: String,
    /// ULID of the seller. Soft reference to `users_service.users`.
    pub seller_id: String,
    /// The state the order left. `None` ONLY on creation, where there is no prior
    /// state. This is what tells a consumer whether stock had ever been decremented.
    pub from_status: Option<OrderStatus>,
    /// The state the order entered.
    pub to_status: OrderStatus,
    /// Who caused the transition.
    pub actor_type: ActorType,
    /// ULID of the acting user. `None` exactly when `actor_type` is
    /// [`ActorType::System`], because a deadline sweep has no account behind it.
    pub actor_id: Option<String>,
    /// Wire slug of the reason for the transition, or `None` where the transition needs
    /// none. Drawn from whichever proto vocabulary fits the edge:
    /// `marketplace.orders.v1.RejectReasonCode` on a rejection,
    /// `marketplace.orders.v1.CancellationReasonCode` on a cancellation, and
    /// `marketplace.orders.v1.UnresolvedReason` on the move to `ClosedUnresolved`.
    /// A `String` rather than an enum because the three vocabularies are disjoint and
    /// keyed to the edge; the slugs are lowercase snake_case, identical to the gRPC
    /// surface.
    pub reason: Option<String>,
    /// Every line of the order, so a consumer can mirror the stock adjustment. See the
    /// struct doc for why a status event carries them.
    pub items: Vec<OrderStockLine>,
    /// Milliseconds since Unix epoch at which the transition committed.
    pub at_ms: i64,
}

/// Kafka topic on which `OrderReplacementRequested` events are published.
///
/// Convention: `<domain>.<context>.<event-name>`.
pub const TOPIC_ORDER_REPLACEMENT_REQUESTED: &str = "marketplace.orders.replacement_requested";

/// Dead-letter topic for `marketplace.orders.replacement_requested`.
pub const TOPIC_ORDER_REPLACEMENT_REQUESTED_DLQ: &str =
    "marketplace.orders.replacement_requested.dlq";

/// Emitted by `orders-service` when a buyer files replacement claims against a
/// delivered order, once the claim rows have committed.
///
/// Producer: `orders-service`.
/// Consumers: none yet. `notification-service` (TASK-54) will be the first — the seller
/// has a bounded window in which to answer and telling them is that service's job. Do
/// not add a consumer here.
///
/// Publication semantics: see the module doc — best-effort publish after commit, no
/// outbox yet.
///
/// Message key: `order_id`, keeping a claim on the same partition as the order's other
/// events.
///
/// Idempotency: consumers must key on `order_id`, or on `claim_id` for per-claim work.
/// The payload carries absolute state, never a delta, so replaying it converges.
///
/// # What this event deliberately does NOT carry
///
/// The `evidence_keys` and free-text `description` held on the claim row are ABSENT and
/// must not be added. Evidence lives in the private storage root and is reachable only
/// through an authenticated RPC; a claim photograph is not something to broadcast onto
/// a topic any consumer group may subscribe to, and the buyer's written account is
/// equally not a consumer's business. A consumer that genuinely needs either can ask
/// `orders-service`, and that request is auditable.
///
/// What this records is the buyer's assertion against the seller's own commitment.
/// Nothing on this topic adjudicates it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderReplacementRequested {
    /// ULID (26-char Crockford base32). Also the Kafka message key.
    pub order_id: String,
    /// ULID of the buyer. Soft reference to `users_service.users`.
    pub buyer_id: String,
    /// ULID of the seller. Soft reference to `users_service.users`.
    pub seller_id: String,
    /// One entry per claimed order line; never empty.
    pub claims: Vec<ReplacementClaimLine>,
    /// Milliseconds since Unix epoch at which the claims committed.
    pub requested_at_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> OrderCreated {
        OrderCreated {
            order_id: "01JABCORDER00000000000000".to_string(),
            buyer_id: "01JABCBUYER00000000000000".to_string(),
            seller_id: "01JABCSELLER00000000000000".to_string(),
            status: OrderStatus::PendingConfirmation,
            fulfilment_class: FulfilmentClass::Live,
            total_amount: 240_000,
            total_currency: Currency::Uah,
            delivery_method: DeliveryMethod::SelfPickup,
            items: vec![OrderCreatedItem {
                order_item_id: "01JABCITEM000000000000000".to_string(),
                listing_id: "01JABCLISTING0000000000000".to_string(),
                quantity: 2,
                title: "Матка карпатка Ф1".to_string(),
                category: ProductCategory::QueenBee,
                unit_price_amount: 120_000,
                unit_price_currency: Currency::Uah,
                quantity_unit: QuantityUnit::Piece,
            }],
            created_at_ms: 1_780_000_000_000,
        }
    }

    #[test]
    fn order_created_roundtrips() {
        let ev = sample();
        let json = serde_json::to_string(&ev).unwrap();
        let back: OrderCreated = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ev);
    }

    #[test]
    fn dlq_topic_is_the_topic_plus_suffix() {
        assert_eq!(
            TOPIC_ORDER_CREATED_DLQ,
            format!("{TOPIC_ORDER_CREATED}.dlq")
        );
        assert_eq!(
            TOPIC_ORDER_CONFIRMED_DLQ,
            format!("{TOPIC_ORDER_CONFIRMED}.dlq")
        );
        assert_eq!(
            TOPIC_ORDER_STATUS_CHANGED_DLQ,
            format!("{TOPIC_ORDER_STATUS_CHANGED}.dlq")
        );
        assert_eq!(
            TOPIC_ORDER_REPLACEMENT_REQUESTED_DLQ,
            format!("{TOPIC_ORDER_REPLACEMENT_REQUESTED}.dlq")
        );
    }

    #[test]
    fn topic_names_are_pinned() {
        assert_eq!(TOPIC_ORDER_CONFIRMED, "marketplace.orders.confirmed");
        assert_eq!(
            TOPIC_ORDER_STATUS_CHANGED,
            "marketplace.orders.status_changed"
        );
        assert_eq!(
            TOPIC_ORDER_REPLACEMENT_REQUESTED,
            "marketplace.orders.replacement_requested"
        );
    }

    fn stock_lines() -> Vec<OrderStockLine> {
        vec![OrderStockLine {
            listing_id: "01JABCLISTING0000000000000".to_string(),
            quantity: 2,
        }]
    }

    fn confirmed_sample() -> OrderConfirmed {
        OrderConfirmed {
            order_id: "01JABCORDER00000000000000".to_string(),
            buyer_id: "01JABCBUYER00000000000000".to_string(),
            seller_id: "01JABCSELLER00000000000000".to_string(),
            items: stock_lines(),
            confirmed_at_ms: 1_780_000_100_000,
        }
    }

    fn status_changed_sample() -> OrderStatusChanged {
        OrderStatusChanged {
            order_id: "01JABCORDER00000000000000".to_string(),
            buyer_id: "01JABCBUYER00000000000000".to_string(),
            seller_id: "01JABCSELLER00000000000000".to_string(),
            from_status: Some(OrderStatus::PendingConfirmation),
            to_status: OrderStatus::Confirmed,
            actor_type: ActorType::Seller,
            actor_id: Some("01JABCSELLER00000000000000".to_string()),
            reason: None,
            items: stock_lines(),
            at_ms: 1_780_000_100_000,
        }
    }

    fn replacement_sample() -> OrderReplacementRequested {
        OrderReplacementRequested {
            order_id: "01JABCORDER00000000000000".to_string(),
            buyer_id: "01JABCBUYER00000000000000".to_string(),
            seller_id: "01JABCSELLER00000000000000".to_string(),
            claims: vec![ReplacementClaimLine {
                claim_id: "01JABCCLAIM00000000000000".to_string(),
                order_item_id: "01JABCITEM000000000000000".to_string(),
                listing_id: "01JABCLISTING0000000000000".to_string(),
                quantity_claimed: 1,
                reason_code: ClaimReasonCode::QueenNotLaying,
            }],
            requested_at_ms: 1_780_000_200_000,
        }
    }

    #[test]
    fn order_confirmed_roundtrips() {
        let ev = confirmed_sample();
        let json = serde_json::to_string(&ev).unwrap();
        assert!(json.contains("\"listing_id\":\"01JABCLISTING0000000000000\""));
        assert!(json.contains("\"quantity\":2"));
        let back: OrderConfirmed = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ev);
    }

    #[test]
    fn order_status_changed_roundtrips() {
        let ev = status_changed_sample();
        let json = serde_json::to_string(&ev).unwrap();
        let back: OrderStatusChanged = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ev);
    }

    #[test]
    fn order_replacement_requested_roundtrips() {
        let ev = replacement_sample();
        let json = serde_json::to_string(&ev).unwrap();
        let back: OrderReplacementRequested = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ev);
    }

    /// The crate uses no serde attributes on any struct or field, so `Option::None` must
    /// reach the wire as an explicit `null` rather than being skipped. A consumer that
    /// distinguishes "creation" from "transition" reads `from_status` directly.
    #[test]
    fn none_options_serialise_as_explicit_null() {
        let ev = OrderStatusChanged {
            from_status: None,
            actor_type: ActorType::System,
            actor_id: None,
            to_status: OrderStatus::Expired,
            reason: Some("seller_silent".to_string()),
            ..status_changed_sample()
        };
        let json = serde_json::to_string(&ev).unwrap();
        assert!(json.contains("\"from_status\":null"), "{json}");
        assert!(json.contains("\"actor_id\":null"), "{json}");
        assert_eq!(
            serde_json::from_str::<OrderStatusChanged>(&json).unwrap(),
            ev
        );
    }

    #[test]
    fn new_enum_wire_slugs_are_pinned() {
        for (actor, slug) in [
            (ActorType::Buyer, "buyer"),
            (ActorType::Seller, "seller"),
            (ActorType::System, "system"),
        ] {
            assert_eq!(
                serde_json::to_string(&actor).unwrap(),
                format!("\"{slug}\"")
            );
        }

        for (reason, slug) in [
            (ClaimReasonCode::QueenNotLaying, "queen_not_laying"),
            (
                ClaimReasonCode::ColonyDidNotEstablish,
                "colony_did_not_establish",
            ),
        ] {
            assert_eq!(
                serde_json::to_string(&reason).unwrap(),
                format!("\"{slug}\"")
            );
        }

        let json = serde_json::to_string(&status_changed_sample()).unwrap();
        assert!(json.contains("\"from_status\":\"pending_confirmation\""));
        assert!(json.contains("\"to_status\":\"confirmed\""));
        assert!(json.contains("\"actor_type\":\"seller\""));

        let json = serde_json::to_string(&replacement_sample()).unwrap();
        assert!(json.contains("\"reason_code\":\"queen_not_laying\""));
    }

    #[test]
    fn enum_wire_slugs_are_pinned() {
        let json = serde_json::to_string(&sample()).unwrap();
        assert!(json.contains("\"status\":\"pending_confirmation\""));
        assert!(json.contains("\"fulfilment_class\":\"live\""));
        assert!(json.contains("\"delivery_method\":\"self_pickup\""));
        assert!(json.contains("\"total_currency\":\"UAH\""));
        assert!(json.contains("\"unit_price_currency\":\"UAH\""));
        assert!(json.contains("\"quantity_unit\":\"piece\""));
        assert!(json.contains("\"category\":\"queen_bee\""));

        assert_eq!(
            serde_json::to_string(&FulfilmentClass::Goods).unwrap(),
            "\"goods\""
        );
        assert_eq!(
            serde_json::to_string(&DeliveryMethod::CourierAgreed).unwrap(),
            "\"courier_agreed\""
        );
        assert_eq!(
            serde_json::to_string(&OrderStatus::ClosedUnresolved).unwrap(),
            "\"closed_unresolved\""
        );
        assert_eq!(
            serde_json::to_string(&QuantityUnit::Kilogram).unwrap(),
            "\"kilogram\""
        );
    }
}
