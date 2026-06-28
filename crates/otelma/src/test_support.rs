//! Shared test fixtures for the core crate's unit tests.
//!
//! Gated under `#[cfg(test)]` and `pub(crate)` — these helpers are used by the
//! tests in `message.rs`, `recorder.rs`, `reader.rs`, `replay.rs`, and
//! `monotonic.rs` so the sample payload and stream builders live in one place
//! rather than being copy-pasted across modules.

use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::message::{Message, Payload};
use crate::recorder::Recorder;

/// A sample domain payload used to exercise the generic envelope, codec,
/// recorder, reader, and replay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum SampleEvent {
    /// A trivial tick with no fields.
    Tick,
    /// A two-sided quote.
    Book {
        /// Bid price.
        bid: i64,
        /// Ask price.
        ask: i64,
    },
}

impl Payload for SampleEvent {
    fn type_name(&self) -> &'static str {
        match self {
            SampleEvent::Tick => "Tick",
            SampleEvent::Book { .. } => "Book",
        }
    }
}

/// Parse an RFC 3339 string into a UTC timestamp.
pub(crate) fn ts(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .expect("valid rfc3339")
        .with_timezone(&Utc)
}

/// A UTC timestamp `secs` seconds past the Unix epoch.
pub(crate) fn dt(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(secs, 0).expect("valid")
}

/// A five-message stream that crosses a UTC hour boundary (three messages in
/// hour 10, two in hour 11) so it exercises multi-part recording/reading.
pub(crate) fn sample_stream() -> Vec<Message<SampleEvent>> {
    vec![
        Message::new(0, ts("2026-01-01T10:00:00Z"), SampleEvent::Tick),
        Message::new(
            1,
            ts("2026-01-01T10:30:00.123456Z"),
            SampleEvent::Book { bid: 1, ask: 2 },
        ),
        Message::new(2, ts("2026-01-01T10:59:59Z"), SampleEvent::Tick),
        Message::new(3, ts("2026-01-01T11:00:00Z"), SampleEvent::Tick),
        Message::new(
            4,
            ts("2026-01-01T11:15:00Z"),
            SampleEvent::Book { bid: 3, ask: 4 },
        ),
    ]
}

/// Record `msgs` into `dir` through a real [`Recorder`], closing it cleanly.
pub(crate) fn record_stream(dir: &Path, msgs: &[Message<SampleEvent>]) {
    let mut rec = Recorder::new(dir).expect("recorder");
    for m in msgs {
        rec.record(m).expect("record");
    }
    rec.close().expect("close");
}
