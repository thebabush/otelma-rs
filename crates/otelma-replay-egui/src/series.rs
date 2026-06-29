//! Pure, egui-free chart math for the replayer's center column.
//!
//! This is part of the **view-model layer**: every function here is a total,
//! deterministic transform over plain numbers (no egui types, no wall clock, no
//! business state). The renderer (`app.rs` / `ui`) calls these to obtain ranges,
//! windows, and extrema — it never does the math itself. Everything is
//! unit-tested in isolation.

use crate::state::{BookPoint, VolumePoint};

/// Trailing window (seconds of *message* time) kept for the live series and used
/// as the live chart's X span. The series is bounded to this in live mode so a
/// long capture doesn't grow unbounded; replay keeps the full session.
///
/// This is recorded-time, never wall-clock, so it stays on the determinism path.
pub const LIVE_WINDOW_SECS: f64 = 180.0;

/// AUTO Y-range padding: the visible `[min bid, max ask]` is grown by this
/// fraction on each side (25%).
const AUTO_Y_PAD: f64 = 0.25;

/// The chart's Y-axis scaling, toggled by the header `AUTO | 0–1` control.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum YScale {
    /// Fit the visible `[min bid, max ask]` with 25% padding.
    Auto,
    /// Fixed full probability range `0.000 .. 1.000`.
    Full,
}

/// An inclusive numeric range `[min, max]` with `min <= max`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Range {
    /// Lower bound.
    pub min: f64,
    /// Upper bound.
    pub max: f64,
}

impl Range {
    /// Span `max - min` (always `>= 0`).
    pub fn span(self) -> f64 {
        self.max - self.min
    }

    /// Map `v` to `0.0..=1.0` within this range (`0` at `min`, `1` at `max`).
    /// A degenerate (zero-span) range maps everything to the middle, `0.5`.
    pub fn norm(self, v: f64) -> f64 {
        let span = self.span();
        if span <= 0.0 {
            0.5
        } else {
            (v - self.min) / span
        }
    }
}

/// The fixed full probability range `0.000 .. 1.000`.
pub const FULL_RANGE: Range = Range { min: 0.0, max: 1.0 };

/// The chart's Y-range for `scale`, given the book points visible in the current
/// X-window.
///
/// - `Full` is always the fixed `0..1` probability range.
/// - `Auto` fits `[min over visible best_bid, max over visible best_ask]` and
///   pads it by [`AUTO_Y_PAD`] on each side. With no visible points (or a
///   degenerate span) it falls back to `0..1` so the grid still renders.
pub fn auto_y_range(scale: YScale, visible: &[BookPoint]) -> Range {
    match scale {
        YScale::Full => FULL_RANGE,
        YScale::Auto => {
            let mut lo = f64::INFINITY;
            let mut hi = f64::NEG_INFINITY;
            for p in visible {
                lo = lo.min(p.best_bid);
                hi = hi.max(p.best_ask);
            }
            if !lo.is_finite() || !hi.is_finite() || hi <= lo {
                return FULL_RANGE;
            }
            let pad = (hi - lo) * AUTO_Y_PAD;
            Range {
                min: lo - pad,
                max: hi + pad,
            }
        }
    }
}

/// Whether the series is kept whole (replay) or bounded to a trailing window
/// (live). Egui-free so the view-model stays decoupled from the theme/render
/// layer; the app maps its `Mode` onto this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeriesMode {
    /// Keep the full session (replay).
    Full,
    /// Bound the stored series to the trailing [`LIVE_WINDOW_SECS`] (live).
    Trailing,
}

/// The visible X-window (seconds since session start) for the chart, given the
/// current playhead time `current_t` (the latest recorded message's `t_secs`).
///
/// - Replay (`Full`) spans `[0, current_t]` — the whole session up to the
///   playhead.
/// - Live (`Trailing`) spans the trailing `[current_t - window, current_t]`.
///
/// `current_t` of `None` (no data yet) yields a unit `[0, 1]` placeholder so the
/// axis still maps.
pub fn visible_x_window(mode: SeriesMode, current_t: Option<f64>) -> Range {
    let Some(now) = current_t else {
        return Range { min: 0.0, max: 1.0 };
    };
    match mode {
        SeriesMode::Full => Range {
            // A zero-width window (single point at t=0) would not map; widen it.
            min: 0.0,
            max: if now > 0.0 { now } else { 1.0 },
        },
        SeriesMode::Trailing => Range {
            min: now - LIVE_WINDOW_SECS,
            max: now,
        },
    }
}

/// The largest `volume` among the samples whose `t_secs` falls inside `window`
/// (inclusive), or `None` if none do. Used to scale the volume bars; the
/// renderer divides each bar's volume by this so the tallest fills the panel.
pub fn max_visible_volume(window: Range, volume: &[VolumePoint]) -> Option<f64> {
    volume
        .iter()
        .filter(|v| v.t_secs >= window.min && v.t_secs <= window.max)
        .map(|v| v.volume)
        .fold(None, |acc, v| Some(acc.map_or(v, |m: f64| m.max(v))))
}

/// Format a volume for the panel's max-volume label, e.g. `68k`, `1.2M`, `940`.
/// Compact SI-ish suffixes; deterministic and locale-free.
pub fn format_volume(v: f64) -> String {
    let v = v.max(0.0);
    if v >= 1_000_000.0 {
        format!("{:.1}M", v / 1_000_000.0)
    } else if v >= 1_000.0 {
        format!("{:.0}k", v / 1_000.0)
    } else {
        format!("{v:.0}")
    }
}

/// Number of order-book levels shown per side in the ladder.
pub const LADDER_DEPTH: usize = 8;

/// The order-book ladder for one side, prepared for rendering: up to
/// [`LADDER_DEPTH`] `(price, size)` levels plus the max size across them (for
/// depth-bar scaling). Pure view-model math — the renderer only maps these to
/// pixels.
#[derive(Debug, Clone, PartialEq)]
pub struct LadderSide {
    /// Up to [`LADDER_DEPTH`] levels, in the input order (already best-first as
    /// produced by the book).
    pub levels: Vec<(f64, f64)>,
    /// The largest `size` among `levels` (`0.0` when empty), for bar scaling.
    pub max_size: f64,
}

impl LadderSide {
    /// Clamp `levels` to the best [`LADDER_DEPTH`] (the input is best-first), and
    /// compute the max size for depth-bar scaling.
    pub fn from_levels(levels: &[(f64, f64)]) -> Self {
        let clamped: Vec<(f64, f64)> = levels.iter().take(LADDER_DEPTH).copied().collect();
        let max_size = clamped
            .iter()
            .map(|(_, size)| *size)
            .fold(0.0_f64, f64::max);
        Self {
            levels: clamped,
            max_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bp(t: f64, bid: f64, ask: f64) -> BookPoint {
        BookPoint {
            t_secs: t,
            best_bid: bid,
            best_ask: ask,
            mid: (bid + ask) / 2.0,
            spread: ask - bid,
        }
    }

    fn vp(t: f64, volume: f64, up: bool) -> VolumePoint {
        VolumePoint {
            t_secs: t,
            volume,
            up,
        }
    }

    #[test]
    fn range_norm_maps_endpoints_and_handles_degenerate() {
        let r = Range { min: 0.4, max: 0.6 };
        assert_eq!(r.norm(0.4), 0.0);
        assert_eq!(r.norm(0.6), 1.0);
        assert!((r.norm(0.5) - 0.5).abs() < 1e-12);
        // Degenerate span → everything to the middle.
        let d = Range { min: 0.5, max: 0.5 };
        assert_eq!(d.norm(0.5), 0.5);
        assert_eq!(d.norm(9.0), 0.5);
    }

    #[test]
    fn auto_y_range_full_is_fixed_unit() {
        assert_eq!(auto_y_range(YScale::Full, &[]), FULL_RANGE);
        assert_eq!(auto_y_range(YScale::Full, &[bp(0.0, 0.3, 0.4)]), FULL_RANGE);
    }

    #[test]
    fn auto_y_range_fits_extrema_with_25pct_padding() {
        // bids: 0.40, 0.44 → min 0.40; asks: 0.46, 0.50 → max 0.50.
        let pts = [bp(0.0, 0.40, 0.46), bp(1.0, 0.44, 0.50)];
        let r = auto_y_range(YScale::Auto, &pts);
        // span 0.10, pad 0.025 each side.
        assert!((r.min - 0.375).abs() < 1e-9, "min {}", r.min);
        assert!((r.max - 0.525).abs() < 1e-9, "max {}", r.max);
    }

    #[test]
    fn auto_y_range_empty_or_degenerate_falls_back_to_full() {
        assert_eq!(auto_y_range(YScale::Auto, &[]), FULL_RANGE);
        // bid == ask everywhere → zero span → fall back.
        assert_eq!(auto_y_range(YScale::Auto, &[bp(0.0, 0.5, 0.5)]), FULL_RANGE);
    }

    #[test]
    fn visible_x_window_replay_spans_zero_to_now() {
        let r = visible_x_window(SeriesMode::Full, Some(120.0));
        assert_eq!(r.min, 0.0);
        assert_eq!(r.max, 120.0);
    }

    #[test]
    fn visible_x_window_live_is_trailing_180s() {
        let r = visible_x_window(SeriesMode::Trailing, Some(500.0));
        assert_eq!(r.min, 500.0 - LIVE_WINDOW_SECS);
        assert_eq!(r.max, 500.0);
        assert_eq!(r.span(), LIVE_WINDOW_SECS);
    }

    #[test]
    fn visible_x_window_no_data_is_unit_placeholder() {
        assert_eq!(
            visible_x_window(SeriesMode::Full, None),
            Range { min: 0.0, max: 1.0 }
        );
        // A single point at t=0 must still give a mappable (non-zero) window.
        assert_eq!(
            visible_x_window(SeriesMode::Full, Some(0.0)),
            Range { min: 0.0, max: 1.0 }
        );
    }

    #[test]
    fn max_visible_volume_respects_window_and_is_none_when_empty() {
        let vols = [
            vp(0.0, 10.0, true),
            vp(50.0, 99.0, false),
            vp(200.0, 5.0, true),
        ];
        // Window [0,100] excludes the t=200 sample.
        let w = Range {
            min: 0.0,
            max: 100.0,
        };
        assert_eq!(max_visible_volume(w, &vols), Some(99.0));
        // Window with nothing inside → None.
        let empty = Range {
            min: 1000.0,
            max: 2000.0,
        };
        assert_eq!(max_visible_volume(empty, &vols), None);
        assert_eq!(max_visible_volume(w, &[]), None);
    }

    #[test]
    fn ladder_side_clamps_to_eight_and_finds_max_size() {
        // Ten levels, best-first; only the first 8 survive.
        let levels: Vec<(f64, f64)> = (0..10).map(|i| (0.5 - i as f64 * 0.01, i as f64)).collect();
        let side = LadderSide::from_levels(&levels);
        assert_eq!(side.levels.len(), LADDER_DEPTH);
        // The first 8 levels have sizes 0..=7 → max 7.
        assert_eq!(side.max_size, 7.0);
        // Best (input-first) level is preserved at the front.
        assert_eq!(side.levels[0], (0.5, 0.0));
    }

    #[test]
    fn ladder_side_empty_has_zero_max() {
        let side = LadderSide::from_levels(&[]);
        assert!(side.levels.is_empty());
        assert_eq!(side.max_size, 0.0);
    }

    #[test]
    fn format_volume_uses_compact_suffixes() {
        assert_eq!(format_volume(940.0), "940");
        assert_eq!(format_volume(68_000.0), "68k");
        assert_eq!(format_volume(1_200_000.0), "1.2M");
        assert_eq!(format_volume(0.0), "0");
        // Negatives clamp to 0 (defensive; volumes are non-negative).
        assert_eq!(format_volume(-5.0), "0");
    }
}
