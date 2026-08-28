use serde::{Deserialize, Serialize};

/// Kafka topic on which `ProfileVerificationChanged` events are published.
///
/// Convention: `<domain>.<context>.<event-name>`.
pub const TOPIC_PROFILE_VERIFICATION_CHANGED: &str = "identity.profiles.verification_changed";

/// Dead-letter topic for `identity.profiles.verification_changed`.
pub const TOPIC_PROFILE_VERIFICATION_CHANGED_DLQ: &str =
    "identity.profiles.verification_changed.dlq";

/// Kafka topic on which `ReviewPublished` events are published.
///
/// Convention: `<domain>.<context>.<event-name>`.
pub const TOPIC_REVIEW_PUBLISHED: &str = "identity.profiles.review_published";

/// Dead-letter topic for `identity.profiles.review_published`.
pub const TOPIC_REVIEW_PUBLISHED_DLQ: &str = "identity.profiles.review_published.dlq";

/// Verification lifecycle of a seller profile. Variants MUST stay in lockstep with
/// the proto enum `identity.profiles.v1.VerificationStatus` and with the
/// `profiles_verification_status_check` CHECK constraint in profile-service, whose
/// stored values are the lowercase slugs `none` / `pending` / `verified` /
/// `rejected` — which is exactly what `rename_all = "snake_case"` produces here.
///
/// There is no `Unspecified` variant: the proto's 0 value is never emitted by
/// profile-service, and an event that cannot say what the status became is not
/// worth publishing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationStatus {
    None,
    Pending,
    Verified,
    Rejected,
}

/// Emitted by `profile-service` whenever a profile's `verification_status` column
/// actually changes value.
///
/// Producer: `profile-service`.
/// Consumers: `listings-service` (group `listings-service-profile-verification`),
/// which refreshes the denormalised `seller_verified` ranking key across every
/// listing of that seller.
///
/// Publication semantics: at-least-once, best-effort direct publish (no outbox).
/// The event is published AFTER the DB commit; if the produce fails the RPC still
/// returns success, because the status has already changed and a lost event
/// degrades ranking rather than corrupting it.
///
/// Emitted ONLY on a real transition. Writing the same status twice must not
/// produce an event, so that a no-op admin action does not churn every listing row
/// of a seller.
///
/// Consumers must be idempotent on `user_id`: the handler writes an ABSOLUTE
/// value, not a delta, so replaying any prefix of the stream converges.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileVerificationChanged {
    /// ULID (26-char Crockford base32) of the profile owner. Also the Kafka
    /// message key, for partition affinity by aggregate.
    pub user_id: String,
    /// The status AFTER the change. Absolute, never a delta.
    pub verification_status: VerificationStatus,
    /// Milliseconds since Unix epoch when the change was committed.
    pub changed_at_ms: i64,
}

/// Emitted by `profile-service` when a review becomes PUBLICLY VISIBLE — that is, when
/// its withdrawal window has closed and it can no longer be taken back.
///
/// Producer: `profile-service`.
/// Consumers: `notification-service` (group `notification-service-review-published`),
/// which tells the seller a review about them is now readable.
///
/// Publication semantics: at-least-once. `profile-service` has no outbox and no
/// scheduling primitive on the review write path — `visible_from` is set once at INSERT
/// to `now + window` and nothing observes the moment it elapses. The event is therefore
/// published by an INTERVAL WORKER that sweeps reviews whose `visible_from` has passed
/// and which have not yet been published, exactly as the deadline sweeper elsewhere in
/// the platform does.
///
/// Emitted on VISIBILITY, not on creation. A review withdrawn inside its window is never
/// published at all, so this topic never announces something a reader cannot go and read.
/// There is correspondingly no withdrawal or retraction event: a notification is only ever
/// sent about a review that has already become permanent.
///
/// Message key: `seller_id`. The seller is the recipient, and per-seller ordering is what
/// matters when two reviews for one seller become visible in the same sweep. Keying on
/// `review_id` would scatter one seller's reviews across partitions for no benefit.
///
/// # This event carries NO review text, and must never gain any
///
/// `notification-service` has no message-body column and its migration forbids one
/// outright: the service composes every message from a template key plus identifiers, and
/// a body arriving on the wire would be an unreviewed string rendered to a real person.
/// The notification says a review EXISTS and where to read it; the text is fetched from
/// `profile-service` by whoever opens it.
///
/// `buyer_id` is deliberately absent for the same reason — the notification does not name
/// the reviewer, so carrying the reviewer's identity would only invite a template that
/// does.
///
/// # Idempotency
///
/// Consumers MUST be idempotent on `review_id`. Delivery is at-least-once and the sweeper
/// may legitimately republish after a crash between its produce and its bookkeeping write.
///
/// A retry produces BYTE-IDENTICAL JSON, because every field on this event is a stored
/// value — see [`ReviewPublished::became_visible_at_ms`], which is the field that makes
/// that true and the field most likely to be "improved" into something that does not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewPublished {
    /// ULID (26-char Crockford base32) of the review row. The consumer's idempotency key.
    pub review_id: String,
    /// ULID of the order the review is attached to. Carried so the notification can point
    /// at the order the review concerns.
    pub order_id: String,
    /// ULID of the seller being reviewed. THE RECIPIENT of the notification, and the Kafka
    /// message key: per-seller ordering is what matters when two reviews land together.
    pub seller_id: String,
    /// The moment the review became public — the row's stored `visible_from`, NEVER the
    /// publisher's wall clock.
    ///
    /// This is load-bearing, not a stylistic preference. `notification-service`
    /// deduplicates on the SHA-256 of the RAW PAYLOAD BYTES. Every other field on this
    /// event is a stored value, so this timestamp is the only thing that could vary
    /// between two publications of one review. Set it to `now()` at publish time and a
    /// republished event hashes differently, walks straight past the processed-event
    /// ledger, and DOUBLE-NOTIFIES A REAL SELLER.
    ///
    /// `visible_from` is written once at INSERT and never updated, so reading it back
    /// makes a retry byte-identical to the original. Do not change this to a send time,
    /// a publication time, or `now()`.
    pub became_visible_at_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_verification_changed_roundtrips() {
        let ev = ProfileVerificationChanged {
            user_id: "01JABCSELLER00000000000000".to_string(),
            verification_status: VerificationStatus::Verified,
            changed_at_ms: 1_780_000_000_000,
        };
        let json = serde_json::to_string(&ev).unwrap();
        assert!(json.contains("\"verification_status\":\"verified\""));
        let back: ProfileVerificationChanged = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ev);
    }

    #[test]
    fn every_status_slug_lands_on_the_wire() {
        for (status, slug) in [
            (VerificationStatus::None, "none"),
            (VerificationStatus::Pending, "pending"),
            (VerificationStatus::Verified, "verified"),
            (VerificationStatus::Rejected, "rejected"),
        ] {
            let ev = ProfileVerificationChanged {
                user_id: "01JABCSELLER00000000000000".to_string(),
                verification_status: status,
                changed_at_ms: 1_780_000_000_000,
            };
            let json = serde_json::to_string(&ev).unwrap();
            assert!(
                json.contains(&format!("\"verification_status\":\"{slug}\"")),
                "expected slug {slug} in {json}"
            );
            assert_eq!(
                serde_json::from_str::<ProfileVerificationChanged>(&json).unwrap(),
                ev
            );
        }
    }

    #[test]
    fn dlq_topic_is_the_topic_plus_suffix() {
        assert_eq!(
            TOPIC_PROFILE_VERIFICATION_CHANGED_DLQ,
            format!("{TOPIC_PROFILE_VERIFICATION_CHANGED}.dlq")
        );
        assert_eq!(
            TOPIC_REVIEW_PUBLISHED_DLQ,
            format!("{TOPIC_REVIEW_PUBLISHED}.dlq")
        );
    }

    #[test]
    fn topic_names_are_pinned() {
        assert_eq!(
            TOPIC_PROFILE_VERIFICATION_CHANGED,
            "identity.profiles.verification_changed"
        );
        assert_eq!(TOPIC_REVIEW_PUBLISHED, "identity.profiles.review_published");
    }

    fn review_sample() -> ReviewPublished {
        ReviewPublished {
            review_id: "01JABCREVIEW00000000000000".to_string(),
            order_id: "01JABCORDER00000000000000".to_string(),
            seller_id: "01JABCSELLER00000000000000".to_string(),
            became_visible_at_ms: 1_780_000_000_000,
        }
    }

    #[test]
    fn review_published_roundtrips() {
        let ev = review_sample();
        let json = serde_json::to_string(&ev).unwrap();
        assert!(
            json.contains("\"review_id\":\"01JABCREVIEW00000000000000\""),
            "{json}"
        );
        assert!(
            json.contains("\"seller_id\":\"01JABCSELLER00000000000000\""),
            "{json}"
        );
        assert!(
            json.contains("\"became_visible_at_ms\":1780000000000"),
            "{json}"
        );
        let back: ReviewPublished = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ev);
    }

    /// The event carries FOUR fields and no review text, and it must stay that way:
    /// `notification-service` has no message-body column and its migration forbids one.
    ///
    /// Pinned as an exhaustive destructure rather than a comment — adding a `body`,
    /// `comment` or `rating` field makes this fail to compile, which is the point at which
    /// someone has to come and read the doc on [`ReviewPublished`].
    #[test]
    fn the_event_carries_no_review_text_and_no_reviewer() {
        let ReviewPublished {
            review_id: _,
            order_id: _,
            seller_id: _,
            became_visible_at_ms: _,
        } = review_sample();

        let json = serde_json::to_string(&review_sample()).unwrap();
        for forbidden in ["body", "comment", "text", "rating", "buyer_id"] {
            assert!(
                !json.contains(&format!("\"{forbidden}\"")),
                "`{forbidden}` reached the wire on ReviewPublished: {json}"
            );
        }
    }

    /// `became_visible_at_ms` is the review's STORED `visible_from`, so republishing the
    /// same review produces byte-identical JSON.
    ///
    /// `notification-service` dedupes on the SHA-256 of the raw payload bytes, so this is
    /// the property the ledger rests on. It is executed rather than asserted in prose
    /// because the failure mode of "improving" the field into a publication timestamp is
    /// silent here and only visible as a real seller being notified twice.
    #[test]
    fn republishing_one_review_produces_byte_identical_json() {
        let first = serde_json::to_vec(&review_sample()).unwrap();
        let second = serde_json::to_vec(&review_sample()).unwrap();
        assert_eq!(
            first, second,
            "two publications of one review must hash identically"
        );

        // And the stability is a property of the VALUE, not of the sample: an event whose
        // timestamp came from a wall clock differs in exactly this way.
        let with_send_time = ReviewPublished {
            became_visible_at_ms: 1_780_000_005_000,
            ..review_sample()
        };
        assert_ne!(
            serde_json::to_vec(&with_send_time).unwrap(),
            first,
            "a five-second difference already defeats a payload-hash ledger"
        );
    }

    /// # Proof that `ReviewPublished` is FULLY ADDITIVE
    ///
    /// The 0.8.0 release was wire-breaking because it added a VARIANT to an existing enum.
    /// 0.9.0's other change (`buyer_id` on `ShipmentStatusChanged`) is forward-incompatible
    /// for that struct. This event is neither, and the claim is executed rather than
    /// written down, because "additive" is asserted about every release and is sometimes
    /// wrong.
    ///
    /// A 0.8.0 consumer is reproduced the only way it can be — two versions of one crate
    /// cannot be linked into a single test binary — as the types that tag actually
    /// contained. It never sees this topic (a consumer subscribes to topics by name, and
    /// this topic did not exist), and none of the types it does link against changed.
    #[test]
    fn nothing_on_the_previous_tag_breaks_on_the_new_event() {
        /// `ProfileVerificationChanged` exactly as 0.8.0 published it — the only struct a
        /// 0.8.0 consumer of `identity.profiles` links against. Unchanged in 0.9.0.
        #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
        struct ProfileVerificationChangedV080 {
            user_id: String,
            verification_status: VerificationStatus,
            changed_at_ms: i64,
        }

        // A payload this crate produces today still deserialises on the old tag, field for
        // field. Nothing about the existing event moved.
        let current = ProfileVerificationChanged {
            user_id: "01JABCSELLER00000000000000".to_string(),
            verification_status: VerificationStatus::Verified,
            changed_at_ms: 1_780_000_000_000,
        };
        let wire = serde_json::to_string(&current).unwrap();
        let old: ProfileVerificationChangedV080 = serde_json::from_str(&wire)
            .expect("0.8.0 must still read a 0.9.0 verification_changed payload");
        assert_eq!(old.user_id, current.user_id);
        assert_eq!(old.verification_status, current.verification_status);
        assert_eq!(old.changed_at_ms, current.changed_at_ms);

        // The new topic is a NEW name, so no existing subscription picks it up. 0.8.0
        // contained no constant with this value.
        assert_ne!(TOPIC_REVIEW_PUBLISHED, TOPIC_PROFILE_VERIFICATION_CHANGED);
        assert_ne!(
            TOPIC_REVIEW_PUBLISHED,
            TOPIC_PROFILE_VERIFICATION_CHANGED_DLQ
        );

        // And a `ReviewPublished` payload is not mistakable for the existing event even if
        // one were misrouted onto the old topic: it shares no required field with it.
        let review_wire = serde_json::to_string(&review_sample()).unwrap();
        assert!(
            serde_json::from_str::<ProfileVerificationChangedV080>(&review_wire).is_err(),
            "a ReviewPublished payload must not silently deserialise as the old event"
        );
    }
}
