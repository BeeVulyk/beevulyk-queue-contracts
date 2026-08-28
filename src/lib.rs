//! Canonical Kafka event contracts for the BeeVulyk platform.
//!
//! One module per bounded context. Topic name constants live next to their
//! struct. All events are JSON-serialized on the wire.

pub mod identity;
pub mod marketplace;
pub mod shipping;
