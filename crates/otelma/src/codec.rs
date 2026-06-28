//! Payload-blob codec.
//!
//! Payloads are stored on disk as opaque binary blobs encoded with MessagePack
//! (`rmp-serde`). These helpers convert any serde-serializable payload to and
//! from that blob representation.

use serde::{de::DeserializeOwned, Serialize};

use crate::error::Error;

/// Encode a payload to its MessagePack blob representation.
pub fn encode_payload<T: Serialize>(payload: &T) -> Result<Vec<u8>, Error> {
    Ok(rmp_serde::to_vec(payload)?)
}

/// Decode a payload from its MessagePack blob representation.
pub fn decode_payload<T: DeserializeOwned>(blob: &[u8]) -> Result<T, Error> {
    Ok(rmp_serde::from_slice(blob)?)
}
