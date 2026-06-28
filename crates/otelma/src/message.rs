//! The [`Message<T>`] envelope.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A record/replay envelope carrying a user-supplied `payload` of type `T`.
///
/// The envelope fields are venue-agnostic:
/// - `seq`: a monotonically increasing sequence number,
/// - `timestamp`: a UTC instant (conceptually microseconds since epoch),
/// - `payload`: the user's domain event.
///
/// Determinism: downstream consumers replay from `timestamp` and never read the
/// wall clock. Wall-clock time is sampled only at the data-source boundary when
/// a `Message` is first constructed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message<T> {
    /// Monotonically increasing sequence number.
    pub seq: u64,
    /// UTC timestamp of the event.
    pub timestamp: DateTime<Utc>,
    /// User-supplied domain payload.
    pub payload: T,
}

impl<T> Message<T> {
    /// Construct a new [`Message`] from its envelope fields and payload.
    pub fn new(seq: u64, timestamp: DateTime<Utc>, payload: T) -> Self {
        Self {
            seq,
            timestamp,
            payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{decode_payload, encode_payload};

    /// A sample domain payload used to exercise the generic envelope and codec.
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum SampleEvent {
        Tick,
        Book { bid: i64, ask: i64 },
    }

    fn fixed_timestamp() -> DateTime<Utc> {
        DateTime::from_timestamp_micros(1_700_000_000_000_000).expect("valid timestamp")
    }

    #[test]
    fn payload_codec_round_trip() {
        let event = SampleEvent::Book { bid: 49, ask: 51 };
        let blob = encode_payload(&event).expect("encode");
        let decoded: SampleEvent = decode_payload(&blob).expect("decode");
        assert_eq!(event, decoded);
    }

    #[test]
    fn message_msgpack_round_trip() {
        let msg = Message::new(7, fixed_timestamp(), SampleEvent::Tick);
        let blob = encode_payload(&msg).expect("encode");
        let decoded: Message<SampleEvent> = decode_payload(&blob).expect("decode");
        assert_eq!(msg, decoded);
    }

    #[test]
    fn timestamp_is_utc_typed() {
        // Compile-level guarantee: `timestamp` is `DateTime<Utc>`.
        let now: DateTime<Utc> = Utc::now();
        let msg = Message::new(0, now, SampleEvent::Tick);
        let _: DateTime<Utc> = msg.timestamp;
        assert_eq!(msg.seq, 0);
    }
}
