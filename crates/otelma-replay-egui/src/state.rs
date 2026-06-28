//! Shared replay state and the [`GuiSink`] that fills it.
//!
//! `GuiSink` implements [`otelma::Sink<PolyEvent>`]: the background feeder
//! thread drives a `SessionReader` through it, and the eframe app reads the
//! resulting [`ReplayState`] each frame. The sink only reads `Message`
//! contents — the determinism contract holds; pacing lives in the feeder.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use otelma::{Message, Sink};
use otelma_polymarket::{AssetId, Level, PolyEvent, Price, Side};

/// One sample of an asset's top-of-book over time (plot point).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BookPoint {
    /// Seconds since the session start (plot x-axis).
    pub t_secs: f64,
    /// Best bid (highest bid price) as f64 for plotting.
    pub best_bid: f64,
    /// Best ask (lowest ask price) as f64 for plotting.
    pub best_ask: f64,
    /// Midpoint.
    pub mid: f64,
}

/// One trade marker over time (plot point).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TradePoint {
    /// Seconds since the session start.
    pub t_secs: f64,
    /// Trade price.
    pub price: f64,
    /// Aggressor side, if known.
    pub side: Option<Side>,
}

/// Accumulated state for one asset.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct AssetState {
    /// Top-of-book series built up as replay progresses.
    pub book_series: Vec<BookPoint>,
    /// Trade markers.
    pub trades: Vec<TradePoint>,
    /// Latest full book bid levels (as received).
    pub depth_bids: Vec<(f64, f64)>,
    /// Latest full book ask levels (as received).
    pub depth_asks: Vec<(f64, f64)>,
}

/// All state shared between the feeder thread and the GUI.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ReplayState {
    /// Per-asset accumulated series, keyed by asset id (sorted).
    pub assets: BTreeMap<AssetId, AssetState>,
    /// Most recent `seq` applied.
    pub current_seq: Option<u64>,
    /// Most recent timestamp applied.
    pub current_ts: Option<DateTime<Utc>>,
    /// First timestamp applied (the session start, for relative time).
    pub start_ts: Option<DateTime<Utc>>,
    /// Total messages applied.
    pub message_count: u64,
}

impl ReplayState {
    /// Asset ids seen so far, in sorted order.
    pub fn asset_ids(&self) -> Vec<AssetId> {
        self.assets.keys().cloned().collect()
    }

    /// Reset to empty (used on restart).
    pub fn clear(&mut self) {
        *self = ReplayState::default();
    }
}

/// Best bid = max bid price; best ask = min ask price. Computed as extrema so we
/// never assume which end of the venue's level vec is top-of-book.
fn best_bid_ask(bids: &[Level], asks: &[Level]) -> Option<(f64, f64)> {
    let best_bid: Price = bids.iter().map(|l| l.price).max()?;
    let best_ask: Price = asks.iter().map(|l| l.price).min()?;
    Some((
        decimal_to_f64(best_bid.value()),
        decimal_to_f64(best_ask.value()),
    ))
}

/// Lossy conversion for plotting only (ImPlot/egui work in f64).
fn decimal_to_f64(d: rust_decimal::Decimal) -> f64 {
    use rust_decimal::prelude::ToPrimitive;
    d.to_f64().unwrap_or(f64::NAN)
}

/// A [`Sink`] that accumulates the replayed stream into a shared
/// [`ReplayState`] for the GUI to render.
pub struct GuiSink<'a> {
    state: &'a mut ReplayState,
}

impl<'a> GuiSink<'a> {
    /// Borrow `state` for the duration of a drive.
    pub fn new(state: &'a mut ReplayState) -> Self {
        Self { state }
    }

    /// Seconds from the session start to `ts` (0 before the first message).
    fn t_secs(&self, ts: DateTime<Utc>) -> f64 {
        match self.state.start_ts {
            Some(start) => (ts - start).num_milliseconds() as f64 / 1000.0,
            None => 0.0,
        }
    }
}

impl Sink<PolyEvent> for GuiSink<'_> {
    fn apply(&mut self, msg: &Message<PolyEvent>) {
        if self.state.start_ts.is_none() {
            self.state.start_ts = Some(msg.timestamp);
        }
        self.state.current_seq = Some(msg.seq);
        self.state.current_ts = Some(msg.timestamp);
        self.state.message_count += 1;

        let t_secs = self.t_secs(msg.timestamp);

        match &msg.payload {
            PolyEvent::Book(book) => {
                let asset = self.state.assets.entry(book.asset_id.clone()).or_default();
                if let Some((best_bid, best_ask)) = best_bid_ask(&book.bids, &book.asks) {
                    asset.book_series.push(BookPoint {
                        t_secs,
                        best_bid,
                        best_ask,
                        mid: (best_bid + best_ask) / 2.0,
                    });
                }
                asset.depth_bids = book
                    .bids
                    .iter()
                    .map(|l| {
                        (
                            decimal_to_f64(l.price.value()),
                            decimal_to_f64(l.size.value()),
                        )
                    })
                    .collect();
                asset.depth_asks = book
                    .asks
                    .iter()
                    .map(|l| {
                        (
                            decimal_to_f64(l.price.value()),
                            decimal_to_f64(l.size.value()),
                        )
                    })
                    .collect();
            }
            PolyEvent::Trade(trade) => {
                if let Some(price) = trade.price {
                    let asset = self.state.assets.entry(trade.asset_id.clone()).or_default();
                    asset.trades.push(TradePoint {
                        t_secs,
                        price: decimal_to_f64(price.value()),
                        side: trade.side,
                    });
                }
            }
            PolyEvent::Connection { .. } => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use otelma_polymarket::{BookUpdate, Size, Trade};
    use rust_decimal_macros::dec;

    fn dt(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).single().expect("ts")
    }

    fn book_msg(
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

    fn lvl(price: rust_decimal::Decimal, size: rust_decimal::Decimal) -> Level {
        Level {
            price: Price::new(price).expect("non-negative price"),
            size: Size::new(size).expect("non-negative size"),
        }
    }

    #[test]
    fn sink_accumulates_book_series_with_extrema() {
        let mut state = ReplayState::default();
        {
            let mut sink = GuiSink::new(&mut state);
            sink.apply(&Message::new(
                0,
                dt(100),
                PolyEvent::Connection { connected: true },
            ));
            // Bids/asks given out of order — extrema must still be correct.
            sink.apply(&book_msg(
                1,
                160,
                "A",
                vec![lvl(dec!(0.50), dec!(10)), lvl(dec!(0.52), dec!(5))],
                vec![lvl(dec!(0.55), dec!(8)), lvl(dec!(0.54), dec!(3))],
            ));
        }

        assert_eq!(state.message_count, 2);
        assert_eq!(state.current_seq, Some(1));
        assert_eq!(state.start_ts, Some(dt(100)));
        assert_eq!(state.current_ts, Some(dt(160)));

        let a = &state.assets[&AssetId::from("A")];
        assert_eq!(a.book_series.len(), 1);
        let p = a.book_series[0];
        // start at t=100, this book at t=160 → 60s.
        assert_eq!(p.t_secs, 60.0);
        assert_eq!(p.best_bid, 0.52);
        assert_eq!(p.best_ask, 0.54);
        assert_eq!(p.mid, 0.53);
        // Depth captured as received (two levels each side).
        assert_eq!(a.depth_bids.len(), 2);
        assert_eq!(a.depth_asks.len(), 2);
    }

    #[test]
    fn sink_appends_trade_points() {
        let mut state = ReplayState::default();
        {
            let mut sink = GuiSink::new(&mut state);
            sink.apply(&book_msg(
                0,
                0,
                "A",
                vec![lvl(dec!(0.5), dec!(1))],
                vec![lvl(dec!(0.6), dec!(1))],
            ));
            sink.apply(&Message::new(
                1,
                dt(30),
                PolyEvent::Trade(Trade {
                    asset_id: "A".into(),
                    price: Some(Price::new(dec!(0.55)).expect("price")),
                    size: Some(Size::new(dec!(2)).expect("size")),
                    side: Some(Side::Buy),
                }),
            ));
            // A trade without a price is not plotted.
            sink.apply(&Message::new(
                2,
                dt(31),
                PolyEvent::Trade(Trade {
                    asset_id: "A".into(),
                    price: None,
                    size: None,
                    side: None,
                }),
            ));
        }

        let a = &state.assets[&AssetId::from("A")];
        assert_eq!(a.trades.len(), 1);
        assert_eq!(a.trades[0].price, 0.55);
        assert_eq!(a.trades[0].t_secs, 30.0);
        assert_eq!(a.trades[0].side, Some(Side::Buy));
    }

    #[test]
    fn asset_ids_sorted_and_clear_resets() {
        let mut state = ReplayState::default();
        {
            let mut sink = GuiSink::new(&mut state);
            sink.apply(&book_msg(
                0,
                0,
                "Z",
                vec![lvl(dec!(0.1), dec!(1))],
                vec![lvl(dec!(0.2), dec!(1))],
            ));
            sink.apply(&book_msg(
                1,
                1,
                "A",
                vec![lvl(dec!(0.1), dec!(1))],
                vec![lvl(dec!(0.2), dec!(1))],
            ));
        }
        assert_eq!(
            state.asset_ids(),
            vec![AssetId::from("A"), AssetId::from("Z")]
        );

        state.clear();
        assert_eq!(state, ReplayState::default());
        assert!(state.asset_ids().is_empty());
    }
}
