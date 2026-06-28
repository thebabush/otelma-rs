//! Pure parser: raw Polymarket CLOB WS text frames → [`PolyEvent`]s.
//!
//! This is the testable core of the venue integration — no networking. The WS
//! client (5b) owns the socket and calls [`parse_ws_frame`] on each text frame.
//!
//! Policy: **skip unknown, crash on corrupt-known.** Event types we don't model
//! (and events missing an `asset_id`) are silently skipped — the venue
//! legitimately sends shapes we don't care about. But a *recognized* event with
//! a structurally bad field (e.g. a non-numeric price on a book level) is
//! corruption and surfaces as an error.

use std::str::FromStr;

use rust_decimal::Decimal;
use serde::Deserialize;
use thiserror::Error;

use crate::event::{BookUpdate, Level, PolyEvent, PriceChange, Side, Trade};
use crate::types::{AssetId, MarketId, Price, Size};

/// Errors from [`parse_ws_frame`].
#[derive(Debug, Error)]
pub enum ParseError {
    /// The frame was not valid JSON, or its overall structure didn't match a
    /// frame (single object or array of objects).
    #[error("invalid Polymarket frame JSON: {0}")]
    Json(#[from] serde_json::Error),

    /// A recognized event carried a structurally bad numeric field (e.g. a
    /// non-numeric price on a book level). Skipping-unknown is fine, but
    /// corrupt-known is surfaced loudly.
    #[error("invalid decimal `{value}` in {field}: {source}")]
    Decimal {
        /// Which field carried the bad value.
        field: &'static str,
        /// The offending raw string.
        value: String,
        /// The underlying parse error.
        source: rust_decimal::Error,
    },

    /// A recognized event carried a negative price or size — corrupt-known.
    #[error("negative {field}: {value}")]
    Negative {
        /// Which field carried the negative value (`price` or `size`).
        field: &'static str,
        /// The offending value.
        value: rust_decimal::Decimal,
    },
}

/// A string-or-number scalar, as Polymarket sometimes quotes numeric fields.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum StrOrNum {
    Str(String),
    Int(i64),
}

/// Raw wire shape of a single CLOB event, before interpretation. Unknown
/// `event_type`s and unmodeled fields are tolerated here; interpretation
/// decides what to keep.
#[derive(Debug, Deserialize)]
struct RawEvent {
    event_type: Option<String>,
    asset_id: Option<String>,
    market: Option<String>,
    timestamp: Option<StrOrNum>,
    #[serde(default)]
    bids: Vec<RawLevel>,
    #[serde(default)]
    asks: Vec<RawLevel>,
    price: Option<String>,
    size: Option<String>,
    side: Option<String>,
}

/// Raw wire shape of a book level (prices/sizes arrive as quoted strings).
#[derive(Debug, Deserialize)]
struct RawLevel {
    price: String,
    size: String,
}

/// One frame element: either a single object or, at the top level, an array.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Frame {
    One(RawEvent),
    Many(Vec<RawEvent>),
}

/// Parse one raw Polymarket CLOB WS text frame into zero or more events.
///
/// A frame is either a single JSON object or an array of objects. Non-JSON
/// control strings (e.g. `PING`/`PONG`) yield zero events; malformed JSON is an
/// [`ParseError::Json`]. Unknown event types and events without an `asset_id`
/// are skipped.
pub fn parse_ws_frame(raw: &str) -> Result<Vec<PolyEvent>, ParseError> {
    let trimmed = raw.trim();
    // Control frames like PING/PONG aren't JSON and carry no events. We only
    // treat genuine JSON (starting with `{` or `[`) as parseable; anything else
    // is a control/keepalive string.
    if !trimmed.starts_with('{') && !trimmed.starts_with('[') {
        return Ok(Vec::new());
    }

    let frame: Frame = serde_json::from_str(trimmed)?;
    let raw_events = match frame {
        Frame::One(e) => vec![e],
        Frame::Many(es) => es,
    };

    let mut events = Vec::with_capacity(raw_events.len());
    for raw in raw_events {
        if let Some(event) = interpret(raw)? {
            events.push(event);
        }
    }
    Ok(events)
}

/// Interpret a raw event into a [`PolyEvent`], or `None` to skip it.
fn interpret(raw: RawEvent) -> Result<Option<PolyEvent>, ParseError> {
    let (Some(event_type), Some(asset_id)) = (raw.event_type.as_deref(), raw.asset_id.clone())
    else {
        // Missing event_type or asset_id → skip.
        return Ok(None);
    };

    match event_type {
        "book" => Ok(Some(PolyEvent::Book(BookUpdate {
            asset_id: AssetId::from(asset_id),
            bids: parse_levels(raw.bids)?,
            asks: parse_levels(raw.asks)?,
            market: raw.market.map(MarketId::from),
            exchange_ts_millis: raw.timestamp.and_then(ts_to_millis),
        }))),
        "last_trade_price" => Ok(Some(PolyEvent::Trade(Trade {
            asset_id: AssetId::from(asset_id),
            price: parse_price_opt(raw.price.as_deref())?,
            size: parse_size_opt(raw.size.as_deref())?,
            side: raw.side.as_deref().and_then(parse_side),
        }))),
        "price_change" => Ok(Some(PolyEvent::PriceChange(PriceChange {
            asset_id: AssetId::from(asset_id),
            price: parse_price_opt(raw.price.as_deref())?,
            size: parse_size_opt(raw.size.as_deref())?,
            side: raw.side.as_deref().and_then(parse_side),
        }))),
        // Unmodeled event type → skip.
        _ => Ok(None),
    }
}

/// Convert recognized book levels; a bad price/size is corruption → error.
fn parse_levels(raw: Vec<RawLevel>) -> Result<Vec<Level>, ParseError> {
    raw.into_iter()
        .map(|l| {
            Ok(Level {
                price: Price::new(parse_decimal(&l.price, "book level price")?)?,
                size: Size::new(parse_decimal(&l.size, "book level size")?)?,
            })
        })
        .collect()
}

/// Parse a required Decimal string; failure on a recognized event is corruption.
fn parse_decimal(s: &str, field: &'static str) -> Result<Decimal, ParseError> {
    Decimal::from_str(s).map_err(|source| ParseError::Decimal {
        field,
        value: s.to_string(),
        source,
    })
}

/// Parse an optional [`Price`] (non-numeric or negative → corrupt-known error).
fn parse_price_opt(s: Option<&str>) -> Result<Option<Price>, ParseError> {
    s.map(|v| Price::new(parse_decimal(v, "price")?))
        .transpose()
}

/// Parse an optional [`Size`] (non-numeric or negative → corrupt-known error).
fn parse_size_opt(s: Option<&str>) -> Result<Option<Size>, ParseError> {
    s.map(|v| Size::new(parse_decimal(v, "size")?)).transpose()
}

/// Parse a side string case-insensitively; unrecognized → `None` (not an error).
fn parse_side(s: &str) -> Option<Side> {
    match s.to_ascii_uppercase().as_str() {
        "BUY" => Some(Side::Buy),
        "SELL" => Some(Side::Sell),
        _ => None,
    }
}

/// Coerce a string-or-number timestamp to millis. A non-numeric string yields
/// `None` rather than a fabricated `0` — consistent with `exchange_ts_millis`
/// already being optional, so garbage surfaces as "absent" not "epoch 0".
fn ts_to_millis(ts: StrOrNum) -> Option<i64> {
    match ts {
        StrOrNum::Int(n) => Some(n),
        StrOrNum::Str(s) => s.parse().ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn price(d: Decimal) -> Price {
        Price::new(d).expect("non-negative price")
    }

    fn size(d: Decimal) -> Size {
        Size::new(d).expect("non-negative size")
    }

    #[test]
    fn parses_single_book_frame() {
        let raw = r#"{
            "event_type":"book",
            "asset_id":"tok-1",
            "market":"0xabc",
            "timestamp":"1700000000000",
            "bids":[{"price":"0.52","size":"100"},{"price":"0.51","size":"200"}],
            "asks":[{"price":"0.55","size":"80"}]
        }"#;
        let events = parse_ws_frame(raw).expect("parse");
        assert_eq!(events.len(), 1);
        let PolyEvent::Book(book) = &events[0] else {
            panic!("expected Book, got {:?}", events[0]);
        };
        assert_eq!(book.asset_id.as_str(), "tok-1");
        assert_eq!(book.market.as_ref().map(|m| m.as_str()), Some("0xabc"));
        assert_eq!(book.exchange_ts_millis, Some(1_700_000_000_000));
        assert_eq!(
            book.bids,
            vec![
                Level {
                    price: price(dec!(0.52)),
                    size: size(dec!(100))
                },
                Level {
                    price: price(dec!(0.51)),
                    size: size(dec!(200))
                },
            ]
        );
        assert_eq!(
            book.asks,
            vec![Level {
                price: price(dec!(0.55)),
                size: size(dec!(80))
            }]
        );
    }

    #[test]
    fn tolerates_numeric_timestamp() {
        let raw =
            r#"{"event_type":"book","asset_id":"t","timestamp":1700000000000,"bids":[],"asks":[]}"#;
        let events = parse_ws_frame(raw).expect("parse");
        let PolyEvent::Book(book) = &events[0] else {
            panic!("expected Book");
        };
        assert_eq!(book.exchange_ts_millis, Some(1_700_000_000_000));
    }

    #[test]
    fn unparseable_timestamp_is_none_not_zero() {
        let raw = r#"{"event_type":"book","asset_id":"t","timestamp":"not-a-number","bids":[],"asks":[]}"#;
        let events = parse_ws_frame(raw).expect("parse");
        let PolyEvent::Book(book) = &events[0] else {
            panic!("expected Book");
        };
        assert_eq!(book.exchange_ts_millis, None);
    }

    #[test]
    fn parses_array_frame_in_order() {
        let raw = r#"[
            {"event_type":"book","asset_id":"a","bids":[],"asks":[]},
            {"event_type":"last_trade_price","asset_id":"b","price":"0.53","size":"12","side":"BUY"}
        ]"#;
        let events = parse_ws_frame(raw).expect("parse");
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], PolyEvent::Book(b) if b.asset_id.as_str() == "a"));
        let PolyEvent::Trade(trade) = &events[1] else {
            panic!("expected Trade");
        };
        assert_eq!(trade.asset_id.as_str(), "b");
        assert_eq!(trade.price, Some(price(dec!(0.53))));
        assert_eq!(trade.size, Some(size(dec!(12))));
        assert_eq!(trade.side, Some(Side::Buy));
    }

    #[test]
    fn parses_price_change_as_its_own_variant() {
        let raw = r#"{"event_type":"price_change","asset_id":"x","price":"0.10","size":"5","side":"sell"}"#;
        let events = parse_ws_frame(raw).expect("parse");
        let PolyEvent::PriceChange(change) = &events[0] else {
            panic!("expected PriceChange, got {:?}", events[0]);
        };
        assert_eq!(change.asset_id.as_str(), "x");
        assert_eq!(change.price, Some(price(dec!(0.10))));
        assert_eq!(change.size, Some(size(dec!(5))));
        assert_eq!(change.side, Some(Side::Sell));
    }

    #[test]
    fn parses_last_trade_price_as_trade_variant() {
        let raw = r#"{"event_type":"last_trade_price","asset_id":"x","price":"0.10","size":"5","side":"buy"}"#;
        let events = parse_ws_frame(raw).expect("parse");
        let PolyEvent::Trade(trade) = &events[0] else {
            panic!("expected Trade, got {:?}", events[0]);
        };
        assert_eq!(trade.side, Some(Side::Buy));
    }

    #[test]
    fn trade_with_missing_fields_is_all_none() {
        let raw = r#"{"event_type":"last_trade_price","asset_id":"y"}"#;
        let events = parse_ws_frame(raw).expect("parse");
        let PolyEvent::Trade(trade) = &events[0] else {
            panic!("expected Trade");
        };
        assert_eq!(trade.price, None);
        assert_eq!(trade.size, None);
        assert_eq!(trade.side, None);
    }

    #[test]
    fn unrecognized_side_is_none_not_error() {
        let raw = r#"{"event_type":"last_trade_price","asset_id":"y","side":"FLIP"}"#;
        let events = parse_ws_frame(raw).expect("parse");
        let PolyEvent::Trade(trade) = &events[0] else {
            panic!("expected Trade");
        };
        assert_eq!(trade.side, None);
    }

    #[test]
    fn unknown_event_type_is_skipped() {
        let raw = r#"{"event_type":"tick_size_change","asset_id":"z"}"#;
        assert_eq!(parse_ws_frame(raw).expect("parse"), vec![]);
    }

    #[test]
    fn event_missing_asset_id_is_skipped() {
        let raw = r#"{"event_type":"book","bids":[],"asks":[]}"#;
        assert_eq!(parse_ws_frame(raw).expect("parse"), vec![]);
    }

    #[test]
    fn pong_control_frame_is_zero_events() {
        assert_eq!(parse_ws_frame("PONG").expect("parse"), vec![]);
        assert_eq!(parse_ws_frame("PING").expect("parse"), vec![]);
    }

    #[test]
    fn malformed_json_is_error() {
        assert!(matches!(parse_ws_frame("{"), Err(ParseError::Json(_))));
    }

    #[test]
    fn corrupt_book_price_is_error() {
        let raw = r#"{"event_type":"book","asset_id":"t","bids":[{"price":"not-a-number","size":"1"}],"asks":[]}"#;
        assert!(matches!(
            parse_ws_frame(raw),
            Err(ParseError::Decimal {
                field: "book level price",
                ..
            })
        ));
    }

    #[test]
    fn negative_book_price_is_error() {
        let raw = r#"{"event_type":"book","asset_id":"t","bids":[{"price":"-0.01","size":"1"}],"asks":[]}"#;
        assert!(matches!(
            parse_ws_frame(raw),
            Err(ParseError::Negative { field: "price", .. })
        ));
    }

    #[test]
    fn negative_trade_size_is_error() {
        let raw = r#"{"event_type":"last_trade_price","asset_id":"t","price":"0.5","size":"-1"}"#;
        assert!(matches!(
            parse_ws_frame(raw),
            Err(ParseError::Negative { field: "size", .. })
        ));
    }

    #[test]
    fn empty_array_frame_is_zero_events() {
        assert_eq!(parse_ws_frame("[]").expect("parse"), vec![]);
    }

    #[test]
    fn null_trade_price_and_size_are_none() {
        // JSON null for an optional scalar deserializes to None — not an error.
        let raw = r#"{"event_type":"last_trade_price","asset_id":"t","price":null,"size":null}"#;
        let events = parse_ws_frame(raw).expect("parse");
        let PolyEvent::Trade(trade) = &events[0] else {
            panic!("expected Trade, got {:?}", events[0]);
        };
        assert_eq!(trade.price, None);
        assert_eq!(trade.size, None);
    }

    /// Duplicate JSON keys in a frame are rejected as a `Json` error — they do
    /// NOT silently take last-wins. The top-level `Frame` is `#[serde(untagged)]`
    /// (single object OR array), and serde's untagged buffering rejects an object
    /// with duplicate keys rather than collapsing it. Pinned because it is a
    /// deterministic, fail-loud behavior worth knowing: a duplicate-key frame is
    /// dropped+logged by the live adapter (handle_frame), not misinterpreted.
    #[test]
    fn duplicate_json_keys_are_rejected_not_silently_collapsed() {
        let raw =
            r#"{"event_type":"book","asset_id":"first","asset_id":"second","bids":[],"asks":[]}"#;
        assert!(matches!(parse_ws_frame(raw), Err(ParseError::Json(_))));
    }

    #[test]
    fn unknown_nested_and_extra_fields_are_ignored() {
        // Extra top-level fields and extra fields inside a level must not break
        // parsing — we model only what we use and ignore the rest.
        let raw = r#"{
            "event_type":"book",
            "asset_id":"tok-1",
            "hash":"0xdeadbeef",
            "extra":{"nested":[1,2,3]},
            "bids":[{"price":"0.52","size":"100","order_id":"abc","flags":7}],
            "asks":[]
        }"#;
        let events = parse_ws_frame(raw).expect("parse");
        let PolyEvent::Book(book) = &events[0] else {
            panic!("expected Book");
        };
        assert_eq!(book.asset_id.as_str(), "tok-1");
        assert_eq!(
            book.bids,
            vec![Level {
                price: price(dec!(0.52)),
                size: size(dec!(100))
            }]
        );
    }
}
