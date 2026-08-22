use serde::{Deserialize, Serialize};

/// Kafka topic on which `ListingPublished` events are published.
///
/// Convention: `<domain>.<context>.<event-name>`.
pub const TOPIC_LISTING_PUBLISHED: &str = "marketplace.listings.published";

/// Dead-letter topic for `marketplace.listings.published`.
pub const TOPIC_LISTING_PUBLISHED_DLQ: &str = "marketplace.listings.published.dlq";

/// Catalogue category of the published listing. Variants MUST stay in lockstep
/// with the proto enum `marketplace.reference.v1.ProductCategory`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductCategory {
    QueenBee,
    BeePackage,
    BeeColony,
    Honey,
    BeeProduct,
    Hive,
    Equipment,
    VetPreparation,
    AgriGoods,
}

/// How the goods are offered. Variants MUST stay in lockstep with the proto enum
/// `marketplace.reference.v1.AvailabilityMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AvailabilityMode {
    InStock,
    PreOrder,
}

/// Emitted by `listings-service` after a listing transitions to `active` — that is,
/// on a successful `PublishListing` or `ResumeListing`.
///
/// Producer: `listings-service`.
/// Consumers: none yet. `#46` (browse/search) may use it for index warming and a
/// future notification-service for follower alerts.
///
/// Publication semantics: at-least-once, best-effort direct publish (no outbox in
/// MVP-1). The event is published AFTER the DB commit; if the produce fails, the RPC
/// still returns success, because the listing is already live and visible. Message
/// key is `listing_id`, for partition affinity by aggregate.
///
/// A listing that is paused and resumed emits the event again. Consumers must be
/// idempotent on `listing_id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListingPublished {
    /// ULID (26-char Crockford base32).
    pub listing_id: String,
    /// ULID of the seller. Soft reference to `users_service.users`.
    pub seller_id: String,
    /// Catalogue category.
    pub category: ProductCategory,
    /// Oblast slug exactly as stored — lowercase snake_case, e.g. `kyiv`,
    /// `ivano_frankivsk`, `kyiv_city`. `None` when the seller stated no oblast.
    ///
    /// A slug rather than a numeric enum discriminant: this payload is JSON and is
    /// read by humans in a DLQ, and the same slug is what the database column holds.
    pub location_region: Option<String>,
    /// How the goods are offered.
    pub availability_mode: AvailabilityMode,
    /// Calendar date the goods are ready, `YYYY-MM-DD`. Present exactly when
    /// `availability_mode` is `PreOrder`.
    ///
    /// A date string, not epoch milliseconds: this is a calendar date, and an
    /// instant encoding would silently shift it by a day for any reader in a
    /// non-UTC zone.
    pub readiness_date: Option<String>,
    /// Milliseconds since Unix epoch at which the listing became active.
    pub published_at_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listing_published_roundtrips() {
        let ev = ListingPublished {
            listing_id: "01JABCLISTING0000000000000".to_string(),
            seller_id: "01JABCSELLER00000000000000".to_string(),
            category: ProductCategory::QueenBee,
            location_region: Some("ivano_frankivsk".to_string()),
            availability_mode: AvailabilityMode::PreOrder,
            readiness_date: Some("2027-05-15".to_string()),
            published_at_ms: 1_780_000_000_000,
        };
        let json = serde_json::to_string(&ev).unwrap();
        assert!(json.contains("\"category\":\"queen_bee\""));
        assert!(json.contains("\"availability_mode\":\"pre_order\""));
        let back: ListingPublished = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ev);
    }

    #[test]
    fn absent_optional_fields_serialise_as_null() {
        let ev = ListingPublished {
            listing_id: "01JABCLISTING0000000000000".to_string(),
            seller_id: "01JABCSELLER00000000000000".to_string(),
            category: ProductCategory::Honey,
            location_region: None,
            availability_mode: AvailabilityMode::InStock,
            readiness_date: None,
            published_at_ms: 1_780_000_000_000,
        };
        let json = serde_json::to_string(&ev).unwrap();
        assert!(json.contains("\"location_region\":null"));
        assert!(json.contains("\"readiness_date\":null"));
        assert_eq!(serde_json::from_str::<ListingPublished>(&json).unwrap(), ev);
    }

    #[test]
    fn dlq_topic_is_the_topic_plus_suffix() {
        assert_eq!(
            TOPIC_LISTING_PUBLISHED_DLQ,
            format!("{TOPIC_LISTING_PUBLISHED}.dlq")
        );
    }
}
