use serde::{Deserialize, Serialize};

/// Kafka topic on which `UserRegistered` events are published.
///
/// Convention: `<domain>.<context>.<event-name>`.
pub const TOPIC_USER_REGISTERED: &str = "identity.users.registered";

/// Dead-letter topic for `identity.users.registered`.
pub const TOPIC_USER_REGISTERED_DLQ: &str = "identity.users.registered.dlq";

/// Role of the registered user. Variants MUST stay in lockstep with the
/// proto enum `identity.users.v1.UserRole`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserRole {
    Beekeeper,
    Agri,
    Vet,
    Other,
}

/// Emitted by `users-service` after a successful `RegisterUser` RPC.
///
/// Producer: `users-service`.
/// Consumers: none yet (feature #9 `notification-service` will consume this).
///
/// Publication semantics: at-least-once, best-effort direct publish (no outbox
/// in MVP-1). The event is published AFTER the DB commit; if the produce
/// fails, the RPC still returns success because the account already exists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserRegistered {
    /// ULID (26-char Crockford base32).
    pub user_id: String,
    /// Normalized lowercase email.
    pub email: String,
    /// E.164 phone.
    pub phone: String,
    /// Role the user registered as.
    pub role: UserRole,
    /// Milliseconds since Unix epoch when the user row was created.
    pub created_at_ms: i64,
}

/// Kafka topic on which `UserLoggedIn` events are published.
pub const TOPIC_USER_LOGGED_IN: &str = "identity.users.logged_in";

/// Dead-letter topic for `identity.users.logged_in`.
pub const TOPIC_USER_LOGGED_IN_DLQ: &str = "identity.users.logged_in.dlq";

/// Emitted by `users-service` after a successful `LoginUser` RPC (i.e. every
/// time a fresh access+refresh pair is issued to a valid credential holder).
///
/// Producer: `users-service`.
/// Consumers: none yet — future fraud/audit and session-analytics services
/// will subscribe.
///
/// Publication semantics: at-least-once, best-effort direct publish (no outbox
/// in MVP-1). Published AFTER the DB commit that inserted the refresh_tokens
/// row; if the produce fails, the RPC still returns success because the login
/// itself succeeded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserLoggedIn {
    /// ULID (26-char Crockford base32).
    pub user_id: String,
    /// Normalized lowercase email.
    pub email: String,
    /// Role of the user at time of login.
    pub role: UserRole,
    /// ULID of the `refresh_tokens` row that was created for this session.
    pub refresh_token_id: String,
    /// Milliseconds since Unix epoch when the login succeeded.
    pub logged_in_at_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_registered_roundtrip() {
        let ev = UserRegistered {
            user_id: "01HN7D8B7Q3V2W4Z5X6Y7A8B9C".into(),
            email: "a@b.co".into(),
            phone: "+380501234567".into(),
            role: UserRole::Beekeeper,
            created_at_ms: 1_700_000_000_000,
        };
        let json = serde_json::to_string(&ev).unwrap();
        assert!(json.contains("\"role\":\"beekeeper\""));
        let back: UserRegistered = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ev);
    }

    #[test]
    fn user_logged_in_roundtrip() {
        let ev = UserLoggedIn {
            user_id: "01HN7D8B7Q3V2W4Z5X6Y7A8B9C".into(),
            email: "a@b.co".into(),
            role: UserRole::Beekeeper,
            refresh_token_id: "01HN7D8C0RXX000000000000ZZ".into(),
            logged_in_at_ms: 1_700_000_000_000,
        };
        let json = serde_json::to_string(&ev).unwrap();
        assert!(json.contains("\"role\":\"beekeeper\""));
        let back: UserLoggedIn = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ev);
    }
}
