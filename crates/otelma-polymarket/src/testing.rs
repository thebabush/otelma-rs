//! Shared test builders for [`PolyEvent`] streams.
//!
//! Available within this crate's own tests and, via the `testing` cargo
//! feature, to downstream crates as dev-dependencies — so the `otelma-cli` and
//! `otelma-replay-egui` test suites don't each re-derive the same message
//! builders. Gated under `#[cfg(any(test, feature = "testing"))]`.

use chrono::{DateTime, Utc};
use otelma::Message;
use rust_decimal::Decimal;

use crate::event::{BookUpdate, Level, MarketMeta, PolyEvent, Trade};
use crate::types::{AssetId, MarketId, Price, Size};

/// A UTC timestamp `secs` seconds past the Unix epoch.
pub fn dt(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(secs, 0).expect("valid timestamp")
}

/// A book level from raw decimals (rejecting negatives via the newtypes).
pub fn lvl(price: Decimal, size: Decimal) -> Level {
    Level {
        price: Price::new(price).expect("non-negative price"),
        size: Size::new(size).expect("non-negative size"),
    }
}

/// A `Book` message for `asset` at `secs` with the given levels.
pub fn book_msg(
    seq: u64,
    secs: i64,
    asset: &str,
    bids: Vec<Level>,
    asks: Vec<Level>,
) -> Message<PolyEvent> {
    Message::new(
        seq,
        dt(secs),
        PolyEvent::Book(BookUpdate {
            asset_id: asset.into(),
            bids,
            asks,
            market: None,
            exchange_ts_millis: None,
        }),
    )
}

/// A [`MarketMeta`] value for tests: a market with `outcome_title`, its
/// `(yes, no)` token ids, and an optional `event_title`.
pub fn market_meta(
    outcome_title: &str,
    yes: &str,
    no: &str,
    event_title: Option<&str>,
) -> MarketMeta {
    MarketMeta {
        market: Some(MarketId::from(format!("0x{outcome_title}"))),
        question: format!("Will {outcome_title} win?"),
        outcome_title: outcome_title.to_string(),
        yes_asset_id: AssetId::from(yes),
        no_asset_id: AssetId::from(no),
        event_title: event_title.map(str::to_string),
        market_slug: Some(format!("will-{outcome_title}-win")),
    }
}

/// A [`PolyEvent::Market`] message for `meta` at `secs`.
pub fn market_meta_msg(seq: u64, secs: i64, meta: MarketMeta) -> Message<PolyEvent> {
    Message::new(seq, dt(secs), PolyEvent::Market(meta))
}

/// A `Trade` message for `asset` at `secs`. `price`/`size` are raw decimals
/// (or `None`); `side` is passed through as-is.
pub fn trade_msg(
    seq: u64,
    secs: i64,
    asset: &str,
    price: Option<Decimal>,
    size: Option<Decimal>,
    side: Option<crate::event::Side>,
) -> Message<PolyEvent> {
    Message::new(
        seq,
        dt(secs),
        PolyEvent::Trade(Trade {
            asset_id: asset.into(),
            price: price.map(|p| Price::new(p).expect("non-negative price")),
            size: size.map(|s| Size::new(s).expect("non-negative size")),
            side,
        }),
    )
}
