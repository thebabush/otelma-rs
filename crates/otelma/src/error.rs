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
}
