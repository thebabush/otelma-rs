//! Domain newtypes for Polymarket events.
//!
//! These wrap the raw wire scalars so the type system carries meaning:
//! [`Price`] / [`Size`] are validated `Decimal`s, and [`AssetId`] / [`MarketId`]
//! are distinct string ids that work as map keys.
//!
//! Validation happens at construction — the parse boundary (see
//! [`crate::parse_ws_frame`]). The `#[serde(transparent)]` derives mean the
//! on-disk / MessagePack representation is exactly the inner value (the `Decimal`
//! string, lossless and unchanged), and deserialization of an already-recorded
//! value is trusted rather than re-validated.

use std::fmt;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::parser::ParseError;

/// A non-negative price. Zero is allowed (valid at resolution); the upper bound
/// is deliberately not enforced so the recorder captures faithfully.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Price(Decimal);

impl Price {
    /// Construct a price, rejecting negative values.
    pub fn new(d: Decimal) -> Result<Self, ParseError> {
        if d.is_sign_negative() && !d.is_zero() {
            return Err(ParseError::Negative {
                field: "price",
                value: d,
            });
        }
        Ok(Price(d))
    }

    /// The inner decimal value.
    pub fn value(&self) -> Decimal {
        self.0
    }
}

/// A non-negative size. Zero is allowed (an emptied level); the upper bound is
/// deliberately not enforced so the recorder captures faithfully.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Size(Decimal);

impl Size {
    /// Construct a size, rejecting negative values.
    pub fn new(d: Decimal) -> Result<Self, ParseError> {
        if d.is_sign_negative() && !d.is_zero() {
            return Err(ParseError::Negative {
                field: "size",
                value: d,
            });
        }
        Ok(Size(d))
    }

    /// The inner decimal value.
    pub fn value(&self) -> Decimal {
        self.0
    }
}

/// A venue token (asset) id. Distinct from [`MarketId`] in the type system;
/// usable as a map key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AssetId(String);

impl AssetId {
    /// Borrow the inner id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AssetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for AssetId {
    fn from(s: String) -> Self {
        AssetId(s)
    }
}

impl From<&str> for AssetId {
    fn from(s: &str) -> Self {
        AssetId(s.to_string())
    }
}

/// A venue market / condition id. Distinct from [`AssetId`]; usable as a map key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MarketId(String);

impl MarketId {
    /// Borrow the inner id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MarketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for MarketId {
    fn from(s: String) -> Self {
        MarketId(s)
    }
}

impl From<&str> for MarketId {
    fn from(s: &str) -> Self {
        MarketId(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::collections::BTreeMap;

    #[test]
    fn price_rejects_negative_accepts_zero_and_positive() {
        assert!(Price::new(dec!(-0.01)).is_err());
        assert_eq!(Price::new(dec!(0)).expect("zero").value(), dec!(0));
        assert_eq!(Price::new(dec!(0.523)).expect("pos").value(), dec!(0.523));
        // Above 1 is allowed — no venue-opinionated upper clamp.
        assert!(Price::new(dec!(1234.5)).is_ok());
    }

    #[test]
    fn size_rejects_negative_accepts_zero_and_positive() {
        assert!(Size::new(dec!(-1)).is_err());
        assert_eq!(Size::new(dec!(0)).expect("zero").value(), dec!(0));
        assert_eq!(Size::new(dec!(100)).expect("pos").value(), dec!(100));
    }

    #[test]
    fn asset_id_display_as_str_and_map_key() {
        let a = AssetId::from("tok-1");
        assert_eq!(a.as_str(), "tok-1");
        assert_eq!(a.to_string(), "tok-1");
        assert_eq!(AssetId::from("tok-1".to_string()), a);

        let mut m: BTreeMap<AssetId, u32> = BTreeMap::new();
        m.insert(AssetId::from("b"), 2);
        m.insert(AssetId::from("a"), 1);
        let keys: Vec<&str> = m.keys().map(|k| k.as_str()).collect();
        assert_eq!(keys, vec!["a", "b"]); // Ord-sorted
        assert_eq!(m[&AssetId::from("a")], 1);
    }

    #[test]
    fn market_id_basics() {
        let m = MarketId::from("0xabc");
        assert_eq!(m.as_str(), "0xabc");
        assert_eq!(m.to_string(), "0xabc");
    }
}
