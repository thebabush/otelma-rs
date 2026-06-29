//! `otelma` — a generic, deterministic record/replay library for streaming
//! market data (Polymarket-focused, but the core is venue-agnostic).
//!
//! The central type is [`Message<T>`], an envelope carrying a monotonically
//! increasing `seq`, a UTC `timestamp`, and a user-supplied `payload` of type
//! `T`. The library is generic over `T` and never needs editing when a user
//! adds new payload types. Wall-clock time is read only at the data-source
//! boundary; downstream consumers replay deterministically from
//! `Message.timestamp`. Payloads are serialized as opaque MessagePack blobs via
//! [`encode_payload`] / [`decode_payload`].

mod clock;
mod codec;
mod error;
mod message;
mod monotonic;
mod parts;
mod reader;
mod recorder;
mod replay;

#[cfg(test)]
pub(crate) mod test_support;

pub use clock::{classify_backstep, Backstep};
pub use codec::{decode_payload, encode_payload};
pub use error::Error;
pub use message::{Message, Payload};
pub use parts::{compact_session, part_paths, part_schema};
pub use reader::SessionReader;
pub use recorder::Recorder;
pub use replay::{drive, drive_realtime, PlaybackControl, Sink};
