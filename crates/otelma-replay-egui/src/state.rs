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
    /// Structured market metadata, keyed by the market's `yes_asset_id` for
    /// deterministic, deduped iteration (a repeated `Market` for the same market
    /// overwrites in place). This is the source of truth for the CHAIN grid's
    /// YES/NO pairing and outcome/event names; the flat `labels` map above stays
    /// the source for the sidebar. Metadata is not a time series, so live-mode
    /// trimming never touches it. A `BTreeMap` (never a `HashMap`) keeps
    /// iteration deterministic.
    pub markets: BTreeMap<AssetId, MarketMeta>,
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

    /// Reset to empty (used on restart). `markets` is a plain struct field, so
    /// the `*self = default()` below resets it along with everything else.
    pub fn clear(&mut self) {
        *self = ReplayState::default();
    }

    /// The current playhead time in seconds since the session start (mirrors the
    /// chart's `current_t_secs`): `current_ts - start_ts`, or `0` before any
    /// message. This is the `now` against which staleness (`LAST`) is measured.
    pub fn current_t_secs(&self) -> f64 {
        match (self.start_ts, self.current_ts) {
            (Some(start), Some(now)) => (now - start).num_milliseconds() as f64 / 1000.0,
            _ => 0.0,
        }
    }

    /// Current per-side stats for `asset` at playhead time `now_secs`, for one
    /// cell-block of the CHAIN grid. An absent asset (or one with no book yet)
    /// yields an all-`None`/zero [`SideStats`]; see that type for per-field
    /// derivation.
    fn side_stats(&self, asset: &AssetId, now_secs: f64) -> SideStats {
        match self.assets.get(asset) {
            Some(state) => SideStats::from_asset(state, now_secs),
            None => SideStats::default(),
        }
    }

    /// Build the deterministic CHAIN grid view-model: every retained market
    /// (one [`ChainRow`] per [`MarketMeta`]) grouped by event title.
    ///
    /// Each row pairs the market's YES and NO assets, computing a [`SideStats`]
    /// for each from the recorded stream at the current playhead
    /// ([`Self::current_t_secs`]). Rows are grouped by `event_title`, falling
    /// back to the market's `outcome_title` when the event title is `None` (the
    /// same "group under self when ungrouped" spirit as the sidebar). Groups are
    /// sorted by title; rows within a group are sorted by `outcome` then by
    /// `yes_asset` — all deterministic (iteration is over `BTreeMap`s).
    ///
    /// `filter` is an optional case-insensitive substring tested against the
    /// outcome name OR the group/event title (the sidebar's matching style);
    /// empty groups are dropped.
    ///
    /// This is the **data layer only**. The renderer (D3b) owns all formatting
    /// (1-decimal seconds for `LAST`, signed cents for `CHG¢`/`SPR¢`, etc.), the
    /// YES/NO quadrant tints, the trade row-flash, and the resizable outcome
    /// column. D3a derives the numbers; it never formats or styles them.
    // Consumed by the CHAIN renderer in D3b; until then only the unit tests
    // exercise it, so the non-test build sees the whole chain as unused.
    #[allow(dead_code)]
    pub fn chain_view(&self, filter: &str) -> Vec<ChainGroup> {
        let needle = filter.trim().to_lowercase();
        let now_secs = self.current_t_secs();
        // Gather rows per group title in a BTreeMap so groups come out sorted.
        let mut groups: BTreeMap<String, Vec<ChainRow>> = BTreeMap::new();
        // Iterating the markets BTreeMap is itself deterministic.
        for meta in self.markets.values() {
            let title = chain_group_title(meta);
            let outcome = meta.outcome_title.clone();
            if !needle.is_empty() && !row_matches(&needle, &title, &outcome) {
                continue;
            }
            let yes = self.side_stats(&meta.yes_asset_id, now_secs);
            let no = self.side_stats(&meta.no_asset_id, now_secs);
            groups.entry(title).or_default().push(ChainRow {
                outcome,
                yes_asset: meta.yes_asset_id.clone(),
                no_asset: meta.no_asset_id.clone(),
                yes,
                no,
            });
        }
        groups
            .into_iter()
            .map(|(title, mut rows)| {
                // Deterministic row order: by outcome then yes asset id.
                rows.sort_by(|a, b| {
                    a.outcome
                        .cmp(&b.outcome)
                        .then_with(|| a.yes_asset.cmp(&b.yes_asset))
                });
                ChainGroup { title, rows }
            })
            .collect()
    }

    /// Build the deterministic, grouped market list for the left sidebar.
    ///
    /// Rows are derived purely from the recorded assets and their labels — this
    /// is view-model math, egui-free and unit-tested. Each row's `price` is the
    /// asset's current mid (its last [`BookPoint`]'s mid), or `None` before any
    /// top-of-book. Grouping key is the **event title** parsed from the label
    /// (the segment before the first `·`), falling back to the whole label when a
    /// label has no separator (or the raw id when unlabeled). Groups are sorted
    /// by title; rows within a group are sorted by their row label, then asset id
    /// — both deterministic. Iteration over `assets`/`labels` is over `BTreeMap`s,
    /// so the input order is itself deterministic.
    ///
    /// `filter` is an optional case-insensitive substring applied over the group
    /// title and the row label (see [`row_matches`]); empty groups are dropped.
    pub fn market_groups(&self, filter: &str) -> Vec<MarketGroup> {
        let needle = filter.trim().to_lowercase();
        // Gather rows per group in a BTreeMap so groups come out title-sorted.
        let mut groups: BTreeMap<String, Vec<MarketRow>> = BTreeMap::new();
        // List every known asset: those with data (`assets`) and those only named
        // by a Market label (`labels`). A `BTreeSet` keeps the union sorted and
        // deduped deterministically.
        let known: std::collections::BTreeSet<&AssetId> =
            self.assets.keys().chain(self.labels.keys()).collect();
        for asset in known {
            let label = self.label_for(asset);
            let (group, row_label) = split_label(&label);
            if !needle.is_empty() && !row_matches(&needle, &group, &row_label) {
                continue;
            }
            let price = self
                .assets
                .get(asset)
                .and_then(AssetState::last_book)
                .map(|b| b.mid);
            groups.entry(group).or_default().push(MarketRow {
                asset_id: asset.clone(),
                row_label,
                price,
            });
        }
        groups
            .into_iter()
            .map(|(title, mut rows)| {
                // Deterministic row order: by label then asset id.
                rows.sort_by(|a, b| {
                    a.row_label
                        .cmp(&b.row_label)
                        .then_with(|| a.asset_id.cmp(&b.asset_id))
                });
                MarketGroup { title, rows }
            })
            .collect()
    }
}

/// One row in the left market sidebar: an outcome and its current mid price.
#[derive(Debug, Clone, PartialEq)]
pub struct MarketRow {
    /// The asset this row selects when clicked.
    pub asset_id: AssetId,
    /// The outcome label shown on the left (the label minus its event prefix).
    pub row_label: String,
    /// The asset's current mid, or `None` before any top-of-book.
    pub price: Option<f64>,
}

/// A titled group of [`MarketRow`]s (one event), for the sidebar.
#[derive(Debug, Clone, PartialEq)]
pub struct MarketGroup {
    /// The group header (the event title).
    pub title: String,
    /// The rows in this group, in deterministic order.
    pub rows: Vec<MarketRow>,
}

/// Current stats for one side (YES or NO book block) of a CHAIN grid row,
/// derived purely from an [`AssetState`] at the current playhead time. Every
/// field is a raw number — the renderer (D3b) handles formatting, signs, units,
/// and tints. A side with no data (asset absent, or no top-of-book yet) is the
/// `Default`: all `None`, `vol` `0`.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct SideStats {
    /// Current best bid (probability 0..1), from the last [`BookPoint`].
    pub bid: Option<f64>,
    /// Current best ask (probability 0..1), from the last [`BookPoint`].
    pub ask: Option<f64>,
    /// Size resting at the best bid (the `depth_bids` level priced at `bid`).
    pub bid_sz: Option<f64>,
    /// Size resting at the best ask (the `depth_asks` level priced at `ask`).
    pub ask_sz: Option<f64>,
    /// Spread in cents (`spread * 100`), from the last [`BookPoint`].
    pub spr_cents: Option<f64>,
    /// Probability change since session open, in cents:
    /// `(current_mid - first_mid) * 100`, signed.
    pub chg_cents: Option<f64>,
    /// Seconds since this asset's most recent trade (book staleness):
    /// `now_secs - last_trade.t_secs`; `None` if the asset has never traded.
    pub last_secs: Option<f64>,
    /// Cumulative traded/changed volume (sum of every [`VolumePoint::volume`]).
    pub vol: f64,
}

impl SideStats {
    /// Derive one side's current stats from `state` at playhead `now_secs`.
    fn from_asset(state: &AssetState, now_secs: f64) -> Self {
        let last = state.last_book();
        let bid = last.map(|b| b.best_bid);
        let ask = last.map(|b| b.best_ask);
        let spr_cents = last.map(|b| b.spread * 100.0);
        // Size at the best level: the depth entry whose price equals best
        // bid / best ask. `depth_*` carries (price, size) pairs as received.
        let bid_sz = bid.and_then(|p| size_at(&state.depth_bids, p));
        let ask_sz = ask.and_then(|p| size_at(&state.depth_asks, p));
        // Change vs the session open (the asset's first top-of-book mid).
        let chg_cents = match (state.book_series.first(), last) {
            (Some(first), Some(now)) => Some((now.mid - first.mid) * 100.0),
            _ => None,
        };
        // Seconds since the asset's most recent trade.
        let last_secs = state.trades.last().map(|t| now_secs - t.t_secs);
        let vol = state.volume.iter().map(|v| v.volume).sum();
        Self {
            bid,
            ask,
            bid_sz,
            ask_sz,
            spr_cents,
            chg_cents,
            last_secs,
            vol,
        }
    }
}

/// Size resting at price `price` in a `(price, size)` depth ladder, or `None`
/// if no level matches. Prices are f64s already converted from the recorded
/// decimals, so an exact equality is the right match: both the best-bid/ask and
/// the depth levels come from the same `Book` message's identical conversion.
fn size_at(levels: &[(f64, f64)], price: f64) -> Option<f64> {
    levels
        .iter()
        .find(|(p, _)| *p == price)
        .map(|(_, size)| *size)
}

/// One row of the CHAIN grid: an outcome entity with its paired YES / NO books.
#[derive(Debug, Clone, PartialEq)]
pub struct ChainRow {
    /// The outcome entity name (`MarketMeta::outcome_title`), e.g. "Brazil".
    pub outcome: String,
    /// The YES side's CLOB token id.
    pub yes_asset: AssetId,
    /// The NO side's CLOB token id.
    pub no_asset: AssetId,
    /// Current YES-book stats.
    pub yes: SideStats,
    /// Current NO-book stats.
    pub no: SideStats,
}

/// A titled group of [`ChainRow`]s (one event/market section) for the CHAIN grid.
#[derive(Debug, Clone, PartialEq)]
pub struct ChainGroup {
    /// The section header (the event title, or the outcome when ungrouped).
    pub title: String,
    /// The rows in this group, in deterministic order.
    pub rows: Vec<ChainRow>,
}

/// The CHAIN grouping title for a market: its `event_title`, falling back to the
/// `outcome_title` when no event title is known (so an ungrouped market sections
/// under its own name — the sidebar's "group under self" spirit).
fn chain_group_title(meta: &MarketMeta) -> String {
    match &meta.event_title {
        Some(event) => event.clone(),
        None => meta.outcome_title.clone(),
    }
}

/// Split a full asset label into `(group_title, row_label)`. Labels are
/// `"Event · Outcome · Side"` (or `"Outcome · Side"` when unlabeled); the group
/// is the first segment and the row is the remainder. A label with no separator
/// groups under itself.
fn split_label(label: &str) -> (String, String) {
    match label.split_once(" · ") {
        Some((group, rest)) => (group.to_string(), rest.to_string()),
        None => (label.to_string(), label.to_string()),
    }
}

/// Whether a row matches a (pre-lowercased, non-empty) filter needle: a
/// case-insensitive substring of either the group title or the row label.
fn row_matches(needle: &str, group: &str, row_label: &str) -> bool {
    group.to_lowercase().contains(needle) || row_label.to_lowercase().contains(needle)
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
                // Retain the structured metadata for the CHAIN grid. Keyed by
                // the yes asset id so a repeated Market for the same market
                // dedupes (overwrites) deterministically.
                self.state
                    .markets
                    .insert(meta.yes_asset_id.clone(), meta.clone());
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
    fn market_groups_groups_by_event_with_price_and_order() {
        use otelma_polymarket::testing::{market_meta, market_meta_msg};
        let mut state = ReplayState::default();
        {
            let mut sink = GuiSink::new(&mut state);
            // Two markets in "World Cup", one in "US Election".
            sink.apply(&market_meta_msg(
                0,
                0,
                market_meta("Netherlands", "yes-nl", "no-nl", Some("World Cup")),
            ));
            sink.apply(&market_meta_msg(
                1,
                0,
                market_meta("Argentina", "yes-arg", "no-arg", Some("World Cup")),
            ));
            sink.apply(&market_meta_msg(
                2,
                0,
                market_meta("Smith", "yes-smith", "no-smith", Some("US Election")),
            ));
            // Give yes-nl a top-of-book so it has a price; leave others priceless.
            sink.apply(&book_msg(
                3,
                1,
                "yes-nl",
                vec![lvl(dec!(0.40), dec!(1))],
                vec![lvl(dec!(0.42), dec!(1))],
            ));
        }

        let groups = state.market_groups("");
        // Groups sorted by title: "US Election" before "World Cup".
        let titles: Vec<&str> = groups.iter().map(|g| g.title.as_str()).collect();
        assert_eq!(titles, vec!["US Election", "World Cup"]);

        // World Cup rows: Argentina (No, Yes) then Netherlands (No, Yes), sorted
        // by row label.
        let wc = &groups[1];
        let labels: Vec<&str> = wc.rows.iter().map(|r| r.row_label.as_str()).collect();
        assert_eq!(
            labels,
            vec![
                "Argentina · No",
                "Argentina · Yes",
                "Netherlands · No",
                "Netherlands · Yes",
            ]
        );
        // Only yes-nl has a price.
        let yes_nl = wc
            .rows
            .iter()
            .find(|r| r.asset_id == AssetId::from("yes-nl"))
            .expect("yes-nl row");
        assert!((yes_nl.price.expect("priced") - 0.41).abs() < 1e-9);
        let no_nl = wc
            .rows
            .iter()
            .find(|r| r.asset_id == AssetId::from("no-nl"))
            .expect("no-nl row");
        assert_eq!(no_nl.price, None);
    }

    #[test]
    fn market_groups_filter_is_case_insensitive_substring() {
        use otelma_polymarket::testing::{market_meta, market_meta_msg};
        let mut state = ReplayState::default();
        {
            let mut sink = GuiSink::new(&mut state);
            sink.apply(&market_meta_msg(
                0,
                0,
                market_meta("Netherlands", "yes-nl", "no-nl", Some("World Cup")),
            ));
            sink.apply(&market_meta_msg(
                1,
                0,
                market_meta("Smith", "yes-smith", "no-smith", Some("US Election")),
            ));
        }
        // Filter on the outcome name (case-insensitive).
        let groups = state.market_groups("nether");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].title, "World Cup");
        assert_eq!(groups[0].rows.len(), 2);

        // Filter on the group/event title matches the whole group.
        let by_event = state.market_groups("ELECTION");
        assert_eq!(by_event.len(), 1);
        assert_eq!(by_event[0].title, "US Election");

        // A non-matching filter drops every group.
        assert!(state.market_groups("zzz").is_empty());
    }

    #[test]
    fn market_groups_unlabeled_asset_groups_under_its_id() {
        let mut state = ReplayState::default();
        {
            let mut sink = GuiSink::new(&mut state);
            // No Market meta → the label falls back to the raw id.
            sink.apply(&book_msg(
                0,
                0,
                "raw-token",
                vec![lvl(dec!(0.5), dec!(1))],
                vec![lvl(dec!(0.6), dec!(1))],
            ));
        }
        let groups = state.market_groups("");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].title, "raw-token");
        assert_eq!(groups[0].rows[0].row_label, "raw-token");
        assert_eq!(groups[0].rows[0].price, Some(0.55));
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

    #[test]
    fn market_meta_is_retained_and_deduped() {
        use otelma_polymarket::testing::{market_meta, market_meta_msg};
        let mut state = ReplayState::default();
        {
            let mut sink = GuiSink::new(&mut state);
            sink.apply(&market_meta_msg(
                0,
                0,
                market_meta("Argentina", "yes-arg", "no-arg", Some("World Cup")),
            ));
            // A second Market for the same market (same yes id) overwrites in
            // place: dedup keyed by yes_asset_id.
            sink.apply(&market_meta_msg(
                1,
                0,
                market_meta("Argentina", "yes-arg", "no-arg", Some("World Cup")),
            ));
            sink.apply(&market_meta_msg(
                2,
                0,
                market_meta("Brazil", "yes-bra", "no-bra", Some("World Cup")),
            ));
        }
        // Two distinct markets retained, keyed by yes asset id, sorted.
        let keys: Vec<&AssetId> = state.markets.keys().collect();
        assert_eq!(
            keys,
            vec![&AssetId::from("yes-arg"), &AssetId::from("yes-bra")]
        );
        assert_eq!(
            state.markets[&AssetId::from("yes-arg")].outcome_title,
            "Argentina"
        );
        assert_eq!(
            state.markets[&AssetId::from("yes-arg")].no_asset_id,
            AssetId::from("no-arg")
        );

        // Live-mode trimming never drops metadata (it isn't a time series).
        {
            let mut sink = GuiSink::new_live(&mut state);
            sink.apply(&book_msg(
                3,
                LIVE_WINDOW_SECS as i64 + 1000,
                "yes-arg",
                vec![lvl(dec!(0.5), dec!(1))],
                vec![lvl(dec!(0.6), dec!(1))],
            ));
        }
        assert_eq!(state.markets.len(), 2, "metadata survives live trimming");

        // clear() resets the markets map along with everything else.
        state.clear();
        assert!(state.markets.is_empty());
    }

    #[test]
    fn chain_view_groups_pairs_and_derives_stats() {
        use otelma_polymarket::testing::{market_meta, market_meta_msg};
        let mut state = ReplayState::default();
        {
            let mut sink = GuiSink::new(&mut state);
            // Two markets in "World Cup", one in "US Election".
            sink.apply(&market_meta_msg(
                0,
                0,
                market_meta("Netherlands", "yes-nl", "no-nl", Some("World Cup")),
            ));
            sink.apply(&market_meta_msg(
                1,
                0,
                market_meta("Argentina", "yes-arg", "no-arg", Some("World Cup")),
            ));
            sink.apply(&market_meta_msg(
                2,
                0,
                market_meta("Smith", "yes-smith", "no-smith", Some("US Election")),
            ));
            // yes-arg session open: mid 0.50.
            sink.apply(&book_msg(
                3,
                0,
                "yes-arg",
                vec![lvl(dec!(0.49), dec!(11))],
                vec![lvl(dec!(0.51), dec!(7))],
            ));
            // yes-arg trade at t=10.
            sink.apply(&trade_msg(
                4,
                10,
                "yes-arg",
                Some(dec!(0.50)),
                Some(dec!(4)),
                Some(Side::Buy),
            ));
            // yes-arg latest book at t=20: mid moves to 0.60. Depth gives the
            // best-level sizes (bid@0.59 → 12, ask@0.61 → 9). A deeper level is
            // also present to prove we pick the *best*, not just the first.
            sink.apply(&book_msg(
                5,
                20,
                "yes-arg",
                vec![lvl(dec!(0.59), dec!(12)), lvl(dec!(0.58), dec!(99))],
                vec![lvl(dec!(0.61), dec!(9)), lvl(dec!(0.62), dec!(99))],
            ));
        }
        // Playhead is the last applied timestamp (t=20).
        assert_eq!(state.current_t_secs(), 20.0);

        let groups = state.chain_view("");
        // Groups sorted by title: "US Election" before "World Cup".
        let titles: Vec<&str> = groups.iter().map(|g| g.title.as_str()).collect();
        assert_eq!(titles, vec!["US Election", "World Cup"]);

        // World Cup rows sorted by outcome: Argentina then Netherlands.
        let wc = &groups[1];
        let outcomes: Vec<&str> = wc.rows.iter().map(|r| r.outcome.as_str()).collect();
        assert_eq!(outcomes, vec!["Argentina", "Netherlands"]);

        // YES/NO pairing on the Argentina row.
        let arg = &wc.rows[0];
        assert_eq!(arg.yes_asset, AssetId::from("yes-arg"));
        assert_eq!(arg.no_asset, AssetId::from("no-arg"));

        // Argentina YES stats, all from the recorded stream.
        let y = arg.yes;
        assert_eq!(y.bid, Some(0.59));
        assert_eq!(y.ask, Some(0.61));
        assert_eq!(y.bid_sz, Some(12.0)); // best bid size, not the deeper 99
        assert_eq!(y.ask_sz, Some(9.0));
        assert!((y.spr_cents.expect("spr") - 2.0).abs() < 1e-9); // (0.61-0.59)*100
                                                                 // mid 0.50 → 0.60 ⇒ +10c.
        assert!((y.chg_cents.expect("chg") - 10.0).abs() < 1e-9);
        // now=20, last trade t=10 ⇒ 10s since trade.
        assert!((y.last_secs.expect("last") - 10.0).abs() < 1e-9);
        // Volume = trade size only (no price_change) = 4.
        assert!((y.vol - 4.0).abs() < 1e-9);

        // The NO side (no-arg) has no data at all: default stats.
        assert_eq!(arg.no, SideStats::default());
        assert_eq!(arg.no.bid, None);
        assert_eq!(arg.no.last_secs, None);
        assert_eq!(arg.no.vol, 0.0);
    }

    #[test]
    fn chain_view_filter_is_case_insensitive_substring() {
        use otelma_polymarket::testing::{market_meta, market_meta_msg};
        let mut state = ReplayState::default();
        {
            let mut sink = GuiSink::new(&mut state);
            sink.apply(&market_meta_msg(
                0,
                0,
                market_meta("Netherlands", "yes-nl", "no-nl", Some("World Cup")),
            ));
            sink.apply(&market_meta_msg(
                1,
                0,
                market_meta("Smith", "yes-smith", "no-smith", Some("US Election")),
            ));
        }
        // Filter on the outcome name (case-insensitive).
        let by_outcome = state.chain_view("nether");
        assert_eq!(by_outcome.len(), 1);
        assert_eq!(by_outcome[0].title, "World Cup");
        assert_eq!(by_outcome[0].rows.len(), 1);
        assert_eq!(by_outcome[0].rows[0].outcome, "Netherlands");

        // Filter on the group/event title matches the whole group.
        let by_event = state.chain_view("ELECTION");
        assert_eq!(by_event.len(), 1);
        assert_eq!(by_event[0].title, "US Election");

        // A non-matching filter drops every group.
        assert!(state.chain_view("zzz").is_empty());
    }

    #[test]
    fn chain_view_no_event_title_groups_under_outcome() {
        use otelma_polymarket::testing::{market_meta, market_meta_msg};
        let mut state = ReplayState::default();
        {
            let mut sink = GuiSink::new(&mut state);
            // No event title → the market sections under its own outcome name.
            sink.apply(&market_meta_msg(
                0,
                0,
                market_meta("Lonely", "yes-lone", "no-lone", None),
            ));
        }
        let groups = state.chain_view("");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].title, "Lonely");
        assert_eq!(groups[0].rows[0].outcome, "Lonely");
        // No book for either side ⇒ both sides are default (all-None) stats.
        assert_eq!(groups[0].rows[0].yes, SideStats::default());
        assert_eq!(groups[0].rows[0].no, SideStats::default());
    }
}
