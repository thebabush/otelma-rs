//! Polymarket CLOB event types — the example payload `T` for the otelma engine.
//!
//! These are venue-generic: events carry the raw Polymarket `asset_id` (token
//! id) and other wire fields verbatim. Mapping asset_ids to domain meaning
//! (outcomes, markets, rate buckets) is the downstream user's job — this crate
//! has no hardcoded token ids or market-specific logic.

use otelma::Payload;
use serde::{Deserialize, Serialize};

use crate::types::{AssetId, MarketId, Price, Size};

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
    pub price: Price,
    /// Size resting at the level.
    pub size: Size,
}

/// A full order-book snapshot for one asset.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BookUpdate {
    /// The venue token id this book is for.
    pub asset_id: AssetId,
    /// Bid levels, in the venue's own ordering (as received on the wire).
    pub bids: Vec<Level>,
    /// Ask levels, in the venue's own ordering (as received on the wire).
    pub asks: Vec<Level>,
    /// The venue's market / condition id, if present.
    pub market: Option<MarketId>,
    /// The venue's own event timestamp in milliseconds, if present.
    pub exchange_ts_millis: Option<i64>,
}

impl BookUpdate {
    /// Best bid = the highest bid price, or `None` if there are no bids.
    ///
    /// Computed as the extremum so we never assume which end of the venue's
    /// level vec is top-of-book.
    pub fn best_bid(&self) -> Option<Price> {
        self.bids.iter().map(|l| l.price).max()
    }

    /// Best ask = the lowest ask price, or `None` if there are no asks.
    ///
    /// Computed as the extremum so we never assume which end of the venue's
    /// level vec is top-of-book.
    pub fn best_ask(&self) -> Option<Price> {
        self.asks.iter().map(|l| l.price).min()
    }
}

/// A trade event for one asset (venue `last_trade_price`): a trade occurred.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Trade {
    /// The venue token id this trade is for.
    pub asset_id: AssetId,
    /// Trade price, if reported.
    pub price: Option<Price>,
    /// Trade size, if reported.
    pub size: Option<Size>,
    /// The venue-reported side of the last trade, if present and recognized.
    pub side: Option<Side>,
}

/// A book-change event for one asset (venue `price_change`): a level of the
/// order book changed. This is distinct from a [`Trade`] — no trade necessarily
/// occurred — so it must not be counted or plotted as one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PriceChange {
    /// The venue token id this change is for.
    pub asset_id: AssetId,
    /// The new price of the changed level, if reported.
    pub price: Option<Price>,
    /// The new size of the changed level, if reported.
    pub size: Option<Size>,
    /// The venue-reported side of the book level that changed, if present and
    /// recognized. We intentionally do not interpret this further (e.g. as
    /// bid/ask) without observed data confirming its meaning.
    pub side: Option<Side>,
}

/// Human-readable metadata for one market, captured at record start so a
/// recording is self-contained: a replay can show "Argentina · Yes" instead of
/// an opaque token id without ever calling the Gamma REST API on the replay
/// path. Emitted by the WS adapter (not the parser) as the first messages of a
/// recording, mirroring [`PolyEvent::Connection`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketMeta {
    /// The market's `conditionId`, if known.
    pub market: Option<MarketId>,
    /// The market question, e.g. "Will Argentina win the 2026 FIFA World Cup?".
    pub question: String,
    /// The market's `groupItemTitle`, e.g. "Argentina".
    pub outcome_title: String,
    /// The "Yes" CLOB token id (`clobTokenIds[0]`).
    pub yes_asset_id: AssetId,
    /// The "No" CLOB token id (`clobTokenIds[1]`).
    pub no_asset_id: AssetId,
    /// The parent event's title, e.g. "World Cup Winner", if known.
    pub event_title: Option<String>,
    /// The market slug, if known.
    pub market_slug: Option<String>,
}

/// The Polymarket payload type carried by `otelma::Message`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PolyEvent {
    /// An order-book snapshot.
    Book(BookUpdate),
    /// A trade / last-price update (venue `last_trade_price`).
    Trade(Trade),
    /// A book-level change (venue `price_change`) — not a trade.
    PriceChange(PriceChange),
    /// A connection-state change emitted by the WS adapter (not the parser).
    Connection {
        /// Whether the venue connection is up.
        connected: bool,
    },
    /// Market metadata emitted by the WS adapter at recording start (not the
    /// parser). Lets a replay label assets with human-readable text.
    Market(MarketMeta),
}

impl Payload for PolyEvent {
    fn type_name(&self) -> &'static str {
        match self {
            PolyEvent::Book(_) => "Book",
            PolyEvent::Trade(_) => "Trade",
            PolyEvent::PriceChange(_) => "PriceChange",
            PolyEvent::Connection { .. } => "Connection",
            PolyEvent::Market(_) => "Market",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otelma::{decode_payload, encode_payload};
    use rust_decimal_macros::dec;

    fn price(d: rust_decimal::Decimal) -> Price {
        Price::new(d).expect("non-negative price")
    }

    fn size(d: rust_decimal::Decimal) -> Size {
        Size::new(d).expect("non-negative size")
    }

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
        let price_change = PolyEvent::PriceChange(PriceChange {
            asset_id: "a".into(),
            price: None,
            size: None,
            side: None,
        });
        let conn = PolyEvent::Connection { connected: true };
        let market = PolyEvent::Market(MarketMeta {
            market: Some("0xcond".into()),
            question: "Will Argentina win?".to_string(),
            outcome_title: "Argentina".to_string(),
            yes_asset_id: "yes".into(),
            no_asset_id: "no".into(),
            event_title: Some("World Cup Winner".to_string()),
            market_slug: Some("will-argentina-win".to_string()),
        });
        assert_eq!(book.type_name(), "Book");
        assert_eq!(trade.type_name(), "Trade");
        assert_eq!(price_change.type_name(), "PriceChange");
        assert_eq!(conn.type_name(), "Connection");
        assert_eq!(market.type_name(), "Market");
    }

    #[test]
    fn best_bid_ask_are_extrema_regardless_of_order() {
        let book = BookUpdate {
            asset_id: "a".into(),
            // Levels given out of order — extrema must still be correct.
            bids: vec![
                Level {
                    price: price(dec!(0.50)),
                    size: size(dec!(1)),
                },
                Level {
                    price: price(dec!(0.52)),
                    size: size(dec!(1)),
                },
            ],
            asks: vec![
                Level {
                    price: price(dec!(0.55)),
                    size: size(dec!(1)),
                },
                Level {
                    price: price(dec!(0.54)),
                    size: size(dec!(1)),
                },
            ],
            market: None,
            exchange_ts_millis: None,
        };
        assert_eq!(book.best_bid(), Some(price(dec!(0.52))));
        assert_eq!(book.best_ask(), Some(price(dec!(0.54))));
    }

    #[test]
    fn best_bid_ask_empty_book_is_none() {
        let book = BookUpdate {
            asset_id: "a".into(),
            bids: vec![],
            asks: vec![],
            market: None,
            exchange_ts_millis: None,
        };
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), None);
    }

    /// The headline serde-config guard: awkward decimals must survive the
    /// MessagePack payload codec exactly through the `Price`/`Size` newtypes.
    /// This only holds because `rust_decimal` is built with `serde-str` (string
    /// encoding, not f64) and the newtypes are `#[serde(transparent)]`.
    #[test]
    fn decimal_round_trip_through_msgpack() {
        let event = PolyEvent::Book(BookUpdate {
            asset_id: "tok".into(),
            bids: vec![
                Level {
                    price: price(dec!(0.523)),
                    size: size(dec!(0.001)),
                },
                Level {
                    price: price(dec!(1234.5678)),
                    size: size(dec!(100)),
                },
            ],
            asks: vec![Level {
                price: price(dec!(0.99999)),
                size: size(dec!(0.5)),
            }],
            market: Some("0xfeed".into()),
            exchange_ts_millis: Some(1_700_000_000_000),
        });

        let blob = encode_payload(&event).expect("encode");
        let decoded: PolyEvent = decode_payload(&blob).expect("decode");
        assert_eq!(decoded, event);
        // Confirm the inner decimals survived exactly.
        let PolyEvent::Book(b) = &decoded else {
            panic!("expected Book");
        };
        assert_eq!(b.bids[0].price.value(), dec!(0.523));
        assert_eq!(b.bids[0].size.value(), dec!(0.001));
    }
}
