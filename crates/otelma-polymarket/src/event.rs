//! Polymarket CLOB event types — the example payload `T` for the otelma engine.
//!
//! These are venue-generic: events carry the raw Polymarket `asset_id` (token
//! id) and other wire fields verbatim. Mapping asset_ids to domain meaning
//! (outcomes, markets, rate buckets) is the downstream user's job — this crate
//! has no hardcoded token ids or market-specific logic.

use otelma::Payload;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Order side as reported by the venue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    /// A buy / bid.
    Buy,
    /// A sell / ask.
    Sell,
}

/// One price level in an order book.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Level {
    /// Price of the level.
    pub price: Decimal,
    /// Size resting at the level.
    pub size: Decimal,
}

/// A full order-book snapshot for one asset.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BookUpdate {
    /// The venue token id this book is for.
    pub asset_id: String,
    /// Bid levels, in the venue's own ordering (as received on the wire).
    pub bids: Vec<Level>,
    /// Ask levels, in the venue's own ordering (as received on the wire).
    pub asks: Vec<Level>,
    /// The venue's market / condition id, if present.
    pub market: Option<String>,
    /// The venue's own event timestamp in milliseconds, if present.
    pub exchange_ts_millis: Option<i64>,
}

/// A trade / last-price event for one asset.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Trade {
    /// The venue token id this trade is for.
    pub asset_id: String,
    /// Trade price, if reported.
    pub price: Option<Decimal>,
    /// Trade size, if reported.
    pub size: Option<Decimal>,
    /// Aggressor side, if reported and recognized.
    pub side: Option<Side>,
}

/// The Polymarket payload type carried by `otelma::Message`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PolyEvent {
    /// An order-book snapshot.
    Book(BookUpdate),
    /// A trade / last-price update.
    Trade(Trade),
    /// A connection-state change emitted by the WS adapter (not the parser).
    Connection {
        /// Whether the venue connection is up.
        connected: bool,
    },
}

impl Payload for PolyEvent {
    fn type_name(&self) -> &'static str {
        match self {
            PolyEvent::Book(_) => "Book",
            PolyEvent::Trade(_) => "Trade",
            PolyEvent::Connection { .. } => "Connection",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otelma::{decode_payload, encode_payload};
    use rust_decimal_macros::dec;

    #[test]
    fn type_name_tags() {
        let book = PolyEvent::Book(BookUpdate {
            asset_id: "a".into(),
            bids: vec![],
            asks: vec![],
            market: None,
            exchange_ts_millis: None,
        });
        let trade = PolyEvent::Trade(Trade {
            asset_id: "a".into(),
            price: None,
            size: None,
            side: None,
        });
        let conn = PolyEvent::Connection { connected: true };
        assert_eq!(book.type_name(), "Book");
        assert_eq!(trade.type_name(), "Trade");
        assert_eq!(conn.type_name(), "Connection");
    }

    /// The headline serde-config guard: awkward decimals must survive the
    /// MessagePack payload codec exactly. This only holds because `rust_decimal`
    /// is built with `serde-str` (string encoding, not f64).
    #[test]
    fn decimal_round_trip_through_msgpack() {
        let event = PolyEvent::Book(BookUpdate {
            asset_id: "tok".into(),
            bids: vec![
                Level {
                    price: dec!(0.523),
                    size: dec!(0.001),
                },
                Level {
                    price: dec!(1234.5678),
                    size: dec!(100),
                },
            ],
            asks: vec![Level {
                price: dec!(0.99999),
                size: dec!(0.5),
            }],
            market: Some("0xfeed".into()),
            exchange_ts_millis: Some(1_700_000_000_000),
        });

        let blob = encode_payload(&event).expect("encode");
        let decoded: PolyEvent = decode_payload(&blob).expect("decode");
        assert_eq!(decoded, event);
    }
}
