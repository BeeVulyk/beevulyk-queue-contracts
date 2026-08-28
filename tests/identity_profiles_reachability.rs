//! Proves the `identity::profiles` surface — including `ReviewPublished`, added in 0.9.0 —
//! is reachable from OUTSIDE the crate.
//!
//! The unit tests in `src/identity/profiles.rs` use `super::*`, which resolves whether or
//! not each item is `pub` and whether or not `identity/mod.rs` still declares
//! `pub mod profiles`. This file is a separate compilation unit that links against the
//! published crate, so the `use` statement below is exactly what a producer in
//! `profile-service` and a consumer in `notification-service` will write. If any item were
//! private, this file would fail to compile.
//!
//! `identity::profiles` had no such file before 0.9.0 — the module predates the convention
//! `marketplace::orders` and `shipping::shipments` established. It gets one now, because
//! `ReviewPublished` is a brand-new surface with a brand-new consumer.

// Exactly the import a producer in `profile-service` writes.
use beevulyk_queue_contracts::identity::profiles::{
    ProfileVerificationChanged, ReviewPublished, VerificationStatus,
    TOPIC_PROFILE_VERIFICATION_CHANGED, TOPIC_PROFILE_VERIFICATION_CHANGED_DLQ,
    TOPIC_REVIEW_PUBLISHED, TOPIC_REVIEW_PUBLISHED_DLQ,
};

/// The producer side: `profile-service`'s interval worker builds the event from a review
/// row and serialises it against its topic constant.
#[test]
fn a_producer_can_build_and_serialise_review_published() {
    // `became_visible_at_ms` is the row's STORED `visible_from` — read back from the
    // database by the sweeper, never `now()`. That is what makes a republish
    // byte-identical.
    let stored_visible_from_ms = 1_780_000_000_000;

    let ev = ReviewPublished {
        review_id: "01JABCREVIEW00000000000000".to_string(),
        order_id: "01JABCORDER00000000000000".to_string(),
        seller_id: "01JABCSELLER00000000000000".to_string(),
        became_visible_at_ms: stored_visible_from_ms,
    };

    assert_eq!(TOPIC_REVIEW_PUBLISHED, "identity.profiles.review_published");
    assert_eq!(
        TOPIC_REVIEW_PUBLISHED_DLQ,
        format!("{TOPIC_REVIEW_PUBLISHED}.dlq")
    );

    let json = serde_json::to_string(&ev).unwrap();
    assert!(
        json.contains("\"seller_id\":\"01JABCSELLER00000000000000\""),
        "{json}"
    );
    assert!(
        json.contains("\"became_visible_at_ms\":1780000000000"),
        "{json}"
    );
}

/// The consumer side: deserialise a raw wire payload of the shape `notification-service`
/// will actually receive, and read the fields it acts on.
#[test]
fn the_notification_consumer_can_deserialise_and_address_the_seller() {
    let wire = r#"{
        "review_id": "01JABCREVIEW00000000000000",
        "order_id": "01JABCORDER00000000000000",
        "seller_id": "01JABCSELLER00000000000000",
        "became_visible_at_ms": 1780000000000
    }"#;

    let ev: ReviewPublished = serde_json::from_str(wire).unwrap();

    // The recipient, and the Kafka message key.
    assert_eq!(ev.seller_id, "01JABCSELLER00000000000000");
    // The consumer's idempotency key.
    assert_eq!(ev.review_id, "01JABCREVIEW00000000000000");
    assert_eq!(ev.order_id, "01JABCORDER00000000000000");
    assert_eq!(ev.became_visible_at_ms, 1_780_000_000_000);
}

/// `notification-service` dedupes on the SHA-256 of the RAW PAYLOAD BYTES, so an
/// at-least-once republication of one review must produce the same bytes. Every field is a
/// stored value, and `became_visible_at_ms` is the one that would stop being so if someone
/// replaced it with a publication timestamp.
///
/// Pinned from outside the crate because the promise is made to the CONSUMER: it is the
/// consumer's ledger that fails, and a real seller who gets told twice.
#[test]
fn a_republished_review_is_byte_identical_so_the_dedup_ledger_holds() {
    let build = || ReviewPublished {
        review_id: "01JABCREVIEW00000000000000".to_string(),
        order_id: "01JABCORDER00000000000000".to_string(),
        seller_id: "01JABCSELLER00000000000000".to_string(),
        became_visible_at_ms: 1_780_000_000_000,
    };

    assert_eq!(
        serde_json::to_vec(&build()).unwrap(),
        serde_json::to_vec(&build()).unwrap(),
        "two publications of one review must serialise identically, or the payload-hash \
         ledger in notification-service cannot recognise the retry"
    );
}

/// The event says a review EXISTS and where to read it. It carries no review text, and
/// `notification-service` has no message-body column to put one in — its migration forbids
/// one outright.
///
/// Pinned from outside as a wire-level assertion: a future field would have to survive this.
#[test]
fn no_review_text_and_no_reviewer_identity_reaches_the_consumer() {
    let json = serde_json::to_string(&ReviewPublished {
        review_id: "01JABCREVIEW00000000000000".to_string(),
        order_id: "01JABCORDER00000000000000".to_string(),
        seller_id: "01JABCSELLER00000000000000".to_string(),
        became_visible_at_ms: 1_780_000_000_000,
    })
    .unwrap();

    for forbidden in ["body", "comment", "text", "rating", "buyer_id", "reviewer"] {
        assert!(
            !json.contains(&format!("\"{forbidden}\"")),
            "`{forbidden}` reached the wire: {json}"
        );
    }
}

/// `ReviewPublished` is FULLY ADDITIVE — a new struct on a new topic, adding no variant to
/// any existing enum. The pre-existing `identity::profiles` surface is unchanged and still
/// reachable under the same names, which is the part a 0.8.0-pinned consumer depends on.
#[test]
fn the_pre_existing_profiles_surface_is_untouched() {
    assert_eq!(
        TOPIC_PROFILE_VERIFICATION_CHANGED,
        "identity.profiles.verification_changed"
    );
    assert_eq!(
        TOPIC_PROFILE_VERIFICATION_CHANGED_DLQ,
        format!("{TOPIC_PROFILE_VERIFICATION_CHANGED}.dlq")
    );

    let wire = r#"{
        "user_id": "01JABCSELLER00000000000000",
        "verification_status": "verified",
        "changed_at_ms": 1780000000000
    }"#;
    let ev: ProfileVerificationChanged = serde_json::from_str(wire).unwrap();
    assert_eq!(ev.verification_status, VerificationStatus::Verified);
    assert_eq!(ev.user_id, "01JABCSELLER00000000000000");

    // The new topic is a new NAME, so no existing subscription picks it up.
    assert_ne!(TOPIC_REVIEW_PUBLISHED, TOPIC_PROFILE_VERIFICATION_CHANGED);
}
