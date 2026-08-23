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
