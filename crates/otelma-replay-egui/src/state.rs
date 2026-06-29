//! Shared replay state and the [`GuiSink`] that fills it.
//!
//! `GuiSink` implements [`otelma::Sink<PolyEvent>`]: the background feeder
//! thread drives a `SessionReader` through it, and the eframe app reads the
//! resulting [`ReplayState`] each frame. The sink only reads `Message`
//! contents — the determinism contract holds; pacing lives in the feeder.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use otelma::{Message, Sink};
use otelma_polymarket::{AssetId, BookUpdate, MarketMeta, PolyEvent, Side, Size};

use crate::series::{SeriesMode, LIVE_WINDOW_SECS};

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
    /// Spread (`best_ask - best_bid`), precomputed for the chart header.
    pub spread: f64,
}

/// One volume-histogram sample over time (volume sub-panel bar).
///
/// "Volume" is the traded/changed size attributed to a single message: the
/// trade `size` for a `Trade`, and the changed level `size` for a `price_change`
/// (Polymarket's `price_change` carries the dense, frequent book churn the
/// prototype's histogram shows; counting both gives the dense look without
/// fabricating data). Messages with no size carry no volume and are skipped.
/// `up` is the tick direction at that moment: `true` when the asset's mid did
/// not fall versus the previous book (`mid >= prev_mid`), `false` otherwise.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VolumePoint {
    /// Seconds since the session start (shared X with the price chart).
    pub t_secs: f64,
    /// Volume for this sample (a non-negative size).
    pub volume: f64,
    /// Tick direction: `true` = up/flat (`mid >= prev`), `false` = down.
    pub up: bool,
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
#[derive(Debug, Clone, PartialEq)]
pub struct AssetState {
    /// Top-of-book series built up as replay progresses.
    pub book_series: Vec<BookPoint>,
    /// Trade markers.
    pub trades: Vec<TradePoint>,
    /// Volume-histogram samples (one per sized trade / price_change).
    pub volume: Vec<VolumePoint>,
    /// Latest full book bid levels (as received).
    pub depth_bids: Vec<(f64, f64)>,
    /// Latest full book ask levels (as received).
    pub depth_asks: Vec<(f64, f64)>,
    /// Mid of the previous top-of-book, for the volume tick-direction (`up`).
    /// `None` until the first top-of-book.
    last_mid: Option<f64>,
    /// Latest tick direction (`mid >= prev_mid`), updated on each book point.
    /// Volume bars inherit this so they colour with the most recent move.
    /// Starts `true` (up) before any move is observed.
    last_dir_up: bool,
}

impl Default for AssetState {
    fn default() -> Self {
        Self {
            book_series: Vec::new(),
            trades: Vec::new(),
            volume: Vec::new(),
            depth_bids: Vec::new(),
            depth_asks: Vec::new(),
            last_mid: None,
            // No move observed yet reads as up (matches "mid >= prev").
            last_dir_up: true,
        }
    }
}

impl AssetState {
    /// The most recent top-of-book sample, if any.
    pub fn last_book(&self) -> Option<&BookPoint> {
        self.book_series.last()
    }

    /// Drop series points older than `min_t` (keep `t_secs >= min_t`). Used in
    /// live mode to bound the stored series to the trailing window. Series are
    /// appended in time order, so this trims a prefix.
    fn trim_before(&mut self, min_t: f64) {
        self.book_series.retain(|p| p.t_secs >= min_t);
        self.trades.retain(|p| p.t_secs >= min_t);
        self.volume.retain(|p| p.t_secs >= min_t);
    }
}

/// All state shared between the feeder thread and the GUI.
///
/// In `--live` mode the per-asset series are bounded to a trailing window of
/// message time (see [`GuiSink::new_live`] / [`crate::series::LIVE_WINDOW_SECS`])
/// so a long capture stays memory-bounded; the on-disk recording is unaffected.
/// In replay the full session is kept (and the GUI clones the whole
/// `ReplayState` each frame) — fine for a bounded recording. Full-session
/// downsampling for scrub-back is deferred.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ReplayState {
    /// Per-asset accumulated series, keyed by asset id (sorted).
    pub assets: BTreeMap<AssetId, AssetState>,
    /// Human-readable label per asset id, from `Market` metadata messages. A
    /// `BTreeMap` (never a `HashMap`) keeps iteration deterministic.
    pub labels: BTreeMap<AssetId, String>,
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

    /// Human-readable label for `asset`, falling back to the raw token id when no
    /// `Market` metadata named it.
    pub fn label_for(&self, asset: &AssetId) -> String {
        self.labels
            .get(asset)
            .cloned()
            .unwrap_or_else(|| asset.to_string())
    }

    /// Reset to empty (used on restart).
    pub fn clear(&mut self) {
        *self = ReplayState::default();
    }
}

/// Top-of-book as `(best_bid, best_ask)` f64s for plotting, or `None` if either
/// side is empty. Extrema come from [`BookUpdate::best_bid`] /
/// [`BookUpdate::best_ask`].
fn best_bid_ask(book: &BookUpdate) -> Option<(f64, f64)> {
    let best_bid = book.best_bid()?;
    let best_ask = book.best_ask()?;
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
    /// Whether to keep the full session (replay) or bound to the trailing live
    /// window. Drives series trimming; everything else is identical.
    mode: SeriesMode,
}

impl<'a> GuiSink<'a> {
    /// Borrow `state` for the duration of a replay drive (full-session series).
    pub fn new(state: &'a mut ReplayState) -> Self {
        Self {
            state,
            mode: SeriesMode::Full,
        }
    }

    /// Borrow `state` for a live drive: the per-asset series are bounded to the
    /// trailing [`LIVE_WINDOW_SECS`] of message time so a long capture stays
    /// memory-bounded. The on-disk recording is unaffected.
    pub fn new_live(state: &'a mut ReplayState) -> Self {
        Self {
            state,
            mode: SeriesMode::Trailing,
        }
    }

    /// Seconds from the session start to `ts` (0 before the first message).
    fn t_secs(&self, ts: DateTime<Utc>) -> f64 {
        match self.state.start_ts {
            Some(start) => (ts - start).num_milliseconds() as f64 / 1000.0,
            None => 0.0,
        }
    }

    /// Append one volume sample for `asset` from a `size`, coloured by the
    /// asset's latest tick direction. A zero/absent size is skipped.
    fn push_volume(asset: &mut AssetState, t_secs: f64, size: Option<Size>) {
        let Some(size) = size else { return };
        let volume = decimal_to_f64(size.value());
        if !volume.is_finite() || volume <= 0.0 {
            return;
        }
        asset.volume.push(VolumePoint {
            t_secs,
            volume,
            up: asset.last_dir_up,
        });
    }

    /// In live mode, drop points older than the trailing window relative to the
    /// just-applied message time. No-op in replay (full session kept).
    fn trim_live(&mut self, now_t: f64) {
        if self.mode != SeriesMode::Trailing {
            return;
        }
        let min_t = now_t - LIVE_WINDOW_SECS;
        for asset in self.state.assets.values_mut() {
            asset.trim_before(min_t);
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
                if let Some((best_bid, best_ask)) = best_bid_ask(book) {
                    let mid = (best_bid + best_ask) / 2.0;
                    // Tick direction vs the previous mid (first move reads up).
                    asset.last_dir_up = match asset.last_mid {
                        Some(prev) => mid >= prev,
                        None => true,
                    };
                    asset.last_mid = Some(mid);
                    asset.book_series.push(BookPoint {
                        t_secs,
                        best_bid,
                        best_ask,
                        mid,
                        spread: best_ask - best_bid,
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
                let asset = self.state.assets.entry(trade.asset_id.clone()).or_default();
                if let Some(price) = trade.price {
                    asset.trades.push(TradePoint {
                        t_secs,
                        price: decimal_to_f64(price.value()),
                        side: trade.side,
                    });
                }
                // The trade's size is real traded volume.
                Self::push_volume(asset, t_secs, trade.size);
            }
            // A price_change is a book-level change, not a trade — it produces no
            // trade marker (top-of-book is driven by PolyEvent::Book). Its
            // changed size is, however, real book churn: count it as volume so
            // the histogram reflects the dense price_change stream.
            PolyEvent::PriceChange(change) => {
                let asset = self
                    .state
                    .assets
                    .entry(change.asset_id.clone())
                    .or_default();
                Self::push_volume(asset, t_secs, change.size);
            }
            PolyEvent::Connection { .. } => {}
            PolyEvent::Market(meta) => {
                self.state
                    .labels
                    .insert(meta.yes_asset_id.clone(), asset_label(meta, "Yes"));
                self.state
                    .labels
                    .insert(meta.no_asset_id.clone(), asset_label(meta, "No"));
            }
        }

        // Live mode: bound every asset's stored series to the trailing window.
        self.trim_live(t_secs);
    }
}

/// Human-readable label for one outcome side of a market, e.g.
/// "Argentina · Yes" (event title prefixed when present).
fn asset_label(meta: &MarketMeta, side: &str) -> String {
    match &meta.event_title {
        Some(event) => format!("{event} · {} · {side}", meta.outcome_title),
        None => format!("{} · {side}", meta.outcome_title),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otelma_polymarket::testing::{book_msg, dt, lvl, trade_msg};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

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
        assert!((p.spread - 0.02).abs() < 1e-9);
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
            sink.apply(&trade_msg(
                1,
                30,
                "A",
                Some(dec!(0.55)),
                Some(dec!(2)),
                Some(Side::Buy),
            ));
            // A trade without a price is not plotted.
            sink.apply(&trade_msg(2, 31, "A", None, None, None));
        }

        let a = &state.assets[&AssetId::from("A")];
        assert_eq!(a.trades.len(), 1);
        assert_eq!(a.trades[0].price, 0.55);
        assert_eq!(a.trades[0].t_secs, 30.0);
        assert_eq!(a.trades[0].side, Some(Side::Buy));
    }

    #[test]
    fn market_meta_populates_label_map() {
        use otelma_polymarket::testing::{market_meta, market_meta_msg};

        let mut state = ReplayState::default();
        {
            let mut sink = GuiSink::new(&mut state);
            sink.apply(&market_meta_msg(
                0,
                0,
                market_meta("Argentina", "yes-arg", "no-arg", Some("World Cup")),
            ));
        }
        assert_eq!(
            state.label_for(&AssetId::from("yes-arg")),
            "World Cup · Argentina · Yes"
        );
        assert_eq!(
            state.label_for(&AssetId::from("no-arg")),
            "World Cup · Argentina · No"
        );
        // Unknown asset falls back to the raw id.
        assert_eq!(state.label_for(&AssetId::from("other")), "other");
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

    /// A `price_change` message for `asset` with a changed `size`.
    fn price_change_msg(seq: u64, secs: i64, asset: &str, size: Decimal) -> Message<PolyEvent> {
        use otelma_polymarket::{PriceChange, Size};
        Message::new(
            seq,
            dt(secs),
            PolyEvent::PriceChange(PriceChange {
                asset_id: asset.into(),
                price: None,
                size: Some(Size::new(size).expect("non-negative size")),
                side: None,
            }),
        )
    }

    #[test]
    fn volume_samples_come_from_trade_and_price_change_size() {
        let mut state = ReplayState::default();
        {
            let mut sink = GuiSink::new(&mut state);
            // A trade with size → one volume sample.
            sink.apply(&trade_msg(
                0,
                0,
                "A",
                Some(dec!(0.55)),
                Some(dec!(7)),
                Some(Side::Buy),
            ));
            // A price_change with size → another volume sample.
            sink.apply(&price_change_msg(1, 1, "A", dec!(3)));
            // A trade without a size → no volume sample.
            sink.apply(&trade_msg(2, 2, "A", Some(dec!(0.55)), None, None));
        }
        let a = &state.assets[&AssetId::from("A")];
        assert_eq!(a.volume.len(), 2);
        assert_eq!(a.volume[0].volume, 7.0);
        assert_eq!(a.volume[1].volume, 3.0);
    }

    #[test]
    fn volume_up_flag_tracks_mid_tick_direction() {
        let mut state = ReplayState::default();
        {
            let mut sink = GuiSink::new(&mut state);
            // First book → mid 0.50; a trade now reads as up (no prior move).
            sink.apply(&book_msg(
                0,
                0,
                "A",
                vec![lvl(dec!(0.49), dec!(1))],
                vec![lvl(dec!(0.51), dec!(1))],
            ));
            sink.apply(&trade_msg(1, 1, "A", None, Some(dec!(1)), None));
            // Mid rises to 0.60 → up.
            sink.apply(&book_msg(
                2,
                2,
                "A",
                vec![lvl(dec!(0.59), dec!(1))],
                vec![lvl(dec!(0.61), dec!(1))],
            ));
            sink.apply(&trade_msg(3, 3, "A", None, Some(dec!(1)), None));
            // Mid falls to 0.40 → down.
            sink.apply(&book_msg(
                4,
                4,
                "A",
                vec![lvl(dec!(0.39), dec!(1))],
                vec![lvl(dec!(0.41), dec!(1))],
            ));
            sink.apply(&trade_msg(5, 5, "A", None, Some(dec!(1)), None));
        }
        let a = &state.assets[&AssetId::from("A")];
        let ups: Vec<bool> = a.volume.iter().map(|v| v.up).collect();
        assert_eq!(ups, vec![true, true, false]);
    }

    #[test]
    fn replay_mode_keeps_full_session_series() {
        let mut state = ReplayState::default();
        {
            let mut sink = GuiSink::new(&mut state);
            // Two books an hour apart — replay must keep both.
            sink.apply(&book_msg(
                0,
                0,
                "A",
                vec![lvl(dec!(0.5), dec!(1))],
                vec![lvl(dec!(0.6), dec!(1))],
            ));
            sink.apply(&book_msg(
                1,
                3600,
                "A",
                vec![lvl(dec!(0.5), dec!(1))],
                vec![lvl(dec!(0.6), dec!(1))],
            ));
        }
        assert_eq!(state.assets[&AssetId::from("A")].book_series.len(), 2);
    }

    #[test]
    fn live_mode_trims_series_to_trailing_window() {
        use crate::series::LIVE_WINDOW_SECS;
        let mut state = ReplayState::default();
        {
            let mut sink = GuiSink::new_live(&mut state);
            // t=0 book (will fall outside the window once t advances far enough).
            sink.apply(&book_msg(
                0,
                0,
                "A",
                vec![lvl(dec!(0.5), dec!(1))],
                vec![lvl(dec!(0.6), dec!(1))],
            ));
            sink.apply(&trade_msg(1, 0, "A", None, Some(dec!(1)), None));
            // A book inside the window: t = WINDOW - 10.
            sink.apply(&book_msg(
                2,
                LIVE_WINDOW_SECS as i64 - 10,
                "A",
                vec![lvl(dec!(0.5), dec!(1))],
                vec![lvl(dec!(0.6), dec!(1))],
            ));
            // Latest message at t = WINDOW + 100 → window floor is t = 100, so
            // the t=0 book + trade fall out; the t = WINDOW-10 (=170) one stays.
            sink.apply(&book_msg(
                3,
                LIVE_WINDOW_SECS as i64 + 100,
                "A",
                vec![lvl(dec!(0.5), dec!(1))],
                vec![lvl(dec!(0.6), dec!(1))],
            ));
        }
        let a = &state.assets[&AssetId::from("A")];
        // The two recent books survive (t=170 and t=280); the t=0 ones are gone.
        assert_eq!(a.book_series.len(), 2);
        assert!(a.book_series.iter().all(|p| p.t_secs >= 100.0));
        assert!(
            a.volume.is_empty(),
            "the t=0 trade volume should be trimmed"
        );
    }
}
