use serde::{Deserialize, Serialize};

/// Kafka topic on which `ProfileVerificationChanged` events are published.
///
/// Convention: `<domain>.<context>.<event-name>`.
pub const TOPIC_PROFILE_VERIFICATION_CHANGED: &str = "identity.profiles.verification_changed";

/// Dead-letter topic for `identity.profiles.verification_changed`.
pub const TOPIC_PROFILE_VERIFICATION_CHANGED_DLQ: &str =
    "identity.profiles.verification_changed.dlq";

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
    }
}
