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

use crate::event::{BookUpdate, Level, PolyEvent, Side, Trade};

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
            asset_id,
            bids: parse_levels(raw.bids)?,
            asks: parse_levels(raw.asks)?,
            market: raw.market,
            exchange_ts_millis: raw.timestamp.map(ts_to_millis),
        }))),
        "last_trade_price" | "price_change" => Ok(Some(PolyEvent::Trade(Trade {
            asset_id,
            price: parse_decimal_opt(raw.price.as_deref(), "price")?,
            size: parse_decimal_opt(raw.size.as_deref(), "size")?,
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
                price: parse_decimal(&l.price, "book level price")?,
                size: parse_decimal(&l.size, "book level size")?,
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

/// Parse an optional Decimal string.
fn parse_decimal_opt(s: Option<&str>, field: &'static str) -> Result<Option<Decimal>, ParseError> {
    s.map(|v| parse_decimal(v, field)).transpose()
}

/// Parse a side string case-insensitively; unrecognized → `None` (not an error).
fn parse_side(s: &str) -> Option<Side> {
    match s.to_ascii_uppercase().as_str() {
        "BUY" => Some(Side::Buy),
        "SELL" => Some(Side::Sell),
        _ => None,
    }
}

/// Coerce a string-or-number timestamp to millis. A non-numeric string parses
/// to 0 (the venue's timestamps are always numeric in practice; we tolerate the
/// quoting rather than crash).
fn ts_to_millis(ts: StrOrNum) -> i64 {
    match ts {
        StrOrNum::Int(n) => n,
        StrOrNum::Str(s) => s.parse().unwrap_or(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

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
        assert_eq!(book.asset_id, "tok-1");
        assert_eq!(book.market.as_deref(), Some("0xabc"));
        assert_eq!(book.exchange_ts_millis, Some(1_700_000_000_000));
        assert_eq!(
            book.bids,
            vec![
                Level {
                    price: dec!(0.52),
                    size: dec!(100)
                },
                Level {
                    price: dec!(0.51),
                    size: dec!(200)
                },
            ]
        );
        assert_eq!(
            book.asks,
            vec![Level {
                price: dec!(0.55),
                size: dec!(80)
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
    fn parses_array_frame_in_order() {
        let raw = r#"[
            {"event_type":"book","asset_id":"a","bids":[],"asks":[]},
            {"event_type":"last_trade_price","asset_id":"b","price":"0.53","size":"12","side":"BUY"}
        ]"#;
        let events = parse_ws_frame(raw).expect("parse");
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], PolyEvent::Book(b) if b.asset_id == "a"));
        let PolyEvent::Trade(trade) = &events[1] else {
            panic!("expected Trade");
        };
        assert_eq!(trade.asset_id, "b");
        assert_eq!(trade.price, Some(dec!(0.53)));
        assert_eq!(trade.size, Some(dec!(12)));
        assert_eq!(trade.side, Some(Side::Buy));
    }

    #[test]
    fn parses_price_change_with_lowercase_side() {
        let raw = r#"{"event_type":"price_change","asset_id":"x","price":"0.10","size":"5","side":"sell"}"#;
        let events = parse_ws_frame(raw).expect("parse");
        let PolyEvent::Trade(trade) = &events[0] else {
            panic!("expected Trade");
        };
        assert_eq!(trade.side, Some(Side::Sell));
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
}
