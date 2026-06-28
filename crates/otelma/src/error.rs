//! Crate error type.

use thiserror::Error;

/// Errors produced by the `otelma` core library.
#[derive(Debug, Error)]
pub enum Error {
    /// A payload failed to encode to MessagePack.
    #[error("failed to encode payload: {0}")]
    Encode(#[from] rmp_serde::encode::Error),

    /// A payload blob failed to decode from MessagePack.
    #[error("failed to decode payload: {0}")]
    Decode(#[from] rmp_serde::decode::Error),

    /// An Arrow operation failed (e.g. building a record batch).
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// A Parquet read/write operation failed.
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// A filesystem operation failed.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The session stream violated its ordering invariant: `seq` must be
    /// strictly increasing and `timestamp` non-decreasing across the whole
    /// session (part boundaries included).
    #[error(
        "monotonicity violation: previous (seq={prev_seq}, ts={prev_ts}) \
         then (seq={seq}, ts={ts})"
    )]
    Monotonicity {
        /// Sequence number of the last accepted message.
        prev_seq: u64,
        /// Timestamp of the last accepted message.
        prev_ts: chrono::DateTime<chrono::Utc>,
        /// Sequence number of the offending message.
        seq: u64,
        /// Timestamp of the offending message.
        ts: chrono::DateTime<chrono::Utc>,
    },

    /// A Parquet column had an unexpected Arrow type (corrupt or foreign file).
    #[error("schema error: {0}")]
    Schema(String),
}
