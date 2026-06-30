//! Typed design tokens, fonts, and mode-driven accent for the replayer's
//! terminal-style UI.
//!
//! This is the **theme layer**: pure, egui-typed constants plus the small pure
//! functions the renderer leans on (accent-by-mode, mode→label,
//! timezone-aware timestamp formatting, replay/live stats formatting). It holds
//! no business logic and never reads the message stream — it is fed values by
//! the render layer (`app.rs`). The view-model (`state.rs`) stays egui-free; the
//! egui types live here and in `app.rs` only.
//!
//! The palette below intentionally defines the *complete* set of spec tokens
//! up front (so later deliverables — the bespoke chart painter, order-book
//! ladder, and CHAIN grid — draw from named tokens rather than re-deriving hex
//! literals). Tokens not yet referenced are therefore expected.
#![allow(dead_code)]

use chrono::{DateTime, Local, Utc};
use eframe::egui::{Color32, FontData, FontDefinitions, FontFamily};

/// The replayer's mode, fixed at launch by the CLI (`--live`). Drives the
/// accent color, the mode pill, and whether playback controls are enabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Paced replay of a recorded session (playback controls enabled).
    Replay,
    /// Live capture+monitor (playback controls locked).
    Live,
}

/// Which body view is shown: the price chart or the option-chain grid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewMode {
    /// The price/depth chart (default).
    Chart,
    /// The dense option-chain grid.
    Chain,
}

/// Displayed timezone for every timestamp in the UI. Source timestamps are UTC;
/// the default display is the user's local time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Timezone {
    /// The user's local timezone (default display).
    Local,
    /// UTC (matches the on-disk source).
    Utc,
}

impl Timezone {
    /// The label shown on the clickable toggle.
    pub fn label(self) -> &'static str {
        match self {
            Timezone::Local => "LOCAL",
            Timezone::Utc => "UTC",
        }
    }

    /// The other timezone (for the toggle click).
    pub fn toggled(self) -> Timezone {
        match self {
            Timezone::Local => Timezone::Utc,
            Timezone::Utc => Timezone::Local,
        }
    }
}

// ── Background tokens ────────────────────────────────────────────────────────

/// Window background `#0a0b0d`.
pub const BG_WINDOW: Color32 = Color32::from_rgb(0x0a, 0x0b, 0x0d);
/// Title bar background `#111317`.
pub const BG_TITLE: Color32 = Color32::from_rgb(0x11, 0x13, 0x17);
/// Toolbar / footer / header-row background `#0c0e12`.
pub const BG_TOOLBAR: Color32 = Color32::from_rgb(0x0c, 0x0e, 0x12);
/// Input background `#101319`.
pub const BG_INPUT: Color32 = Color32::from_rgb(0x10, 0x13, 0x19);
/// Control-button background `#1c1f26`.
pub const BG_CONTROL: Color32 = Color32::from_rgb(0x1c, 0x1f, 0x26);
/// Chain "off" checkbox background `#14171c`.
pub const BG_CHECK_OFF: Color32 = Color32::from_rgb(0x14, 0x17, 0x1c);

// ── Border tokens ────────────────────────────────────────────────────────────

/// Panel-seam border `#181b21`.
pub const BORDER_SEAM: Color32 = Color32::from_rgb(0x18, 0x1b, 0x21);
/// Stronger border `#20242c`.
pub const BORDER_STRONG: Color32 = Color32::from_rgb(0x20, 0x24, 0x2c);
/// Chain cell-grid border `#1c2128`.
pub const BORDER_CELL: Color32 = Color32::from_rgb(0x1c, 0x21, 0x28);
/// Chain center seam / section accents `#2a2f38`.
pub const BORDER_SEAM_STRONG: Color32 = Color32::from_rgb(0x2a, 0x2f, 0x38);
/// Chain row underline `#14171c`.
pub const BORDER_ROW: Color32 = Color32::from_rgb(0x14, 0x17, 0x1c);
/// Toolbar control border (lighter) `#232830`.
pub const BORDER_CTRL: Color32 = Color32::from_rgb(0x23, 0x28, 0x30);
/// Toolbar control border (stronger) `#2b3039`.
pub const BORDER_CTRL_STRONG: Color32 = Color32::from_rgb(0x2b, 0x30, 0x39);
/// Title decorative dots `#34373d`.
pub const DOT: Color32 = Color32::from_rgb(0x34, 0x37, 0x3d);
/// TZ-toggle dotted underline `#3a4150`.
pub const UNDERLINE: Color32 = Color32::from_rgb(0x3a, 0x41, 0x50);

// ── Text tokens ──────────────────────────────────────────────────────────────

/// Primary text `#c8ccd2`.
pub const TEXT_PRIMARY: Color32 = Color32::from_rgb(0xc8, 0xcc, 0xd2);
/// Bright text `#e6e9ee`.
pub const TEXT_BRIGHT: Color32 = Color32::from_rgb(0xe6, 0xe9, 0xee);
/// Muted text `#9aa3b2`.
pub const TEXT_MUTED: Color32 = Color32::from_rgb(0x9a, 0xa3, 0xb2);
/// Dim text `#7e8794`.
pub const TEXT_DIM: Color32 = Color32::from_rgb(0x7e, 0x87, 0x94);
/// Dimmer text `#5b6473`.
pub const TEXT_DIMMER: Color32 = Color32::from_rgb(0x5b, 0x64, 0x73);
/// Faint text `#4b5360`.
pub const TEXT_FAINT: Color32 = Color32::from_rgb(0x4b, 0x53, 0x60);
/// Label text `#6b7480`.
pub const TEXT_LABEL: Color32 = Color32::from_rgb(0x6b, 0x74, 0x80);
/// Title-name text `#7b8290`.
pub const TEXT_TITLE: Color32 = Color32::from_rgb(0x7b, 0x82, 0x90);

// ── Semantic tokens ──────────────────────────────────────────────────────────

/// Bid / up green `#2ec27e`.
pub const GREEN: Color32 = Color32::from_rgb(0x2e, 0xc2, 0x7e);
/// Ask / down red `#e5484d`.
pub const RED: Color32 = Color32::from_rgb(0xe5, 0x48, 0x4d);
/// Chart grid line `#1b1f27`.
pub const GRID_LINE: Color32 = Color32::from_rgb(0x1b, 0x1f, 0x27);

/// A mode-driven accent and its derived variants. The HTML's `rgba(accent, α)`
/// washes are pre-blended over the window background so they read identically
/// without alpha compositing surprises.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Accent {
    /// Full-strength accent (line, pill text/dot, active tab text).
    pub base: Color32,
    /// Soft fill (~12% over window bg) for active-tab / active-row backgrounds.
    pub soft: Color32,
    /// Band fill (~17%) for the bid/ask chart band.
    pub band: Color32,
    /// Dim accent (~50%) for the dashed playhead.
    pub dim: Color32,
    /// "Ink on accent" — the near-black text drawn on an accent fill.
    pub ink: Color32,
}

/// REPLAY accent: fuchsia `#e845d6` (ink `#160418`).
const FUCHSIA: Color32 = Color32::from_rgb(0xe8, 0x45, 0xd6);
const FUCHSIA_INK: Color32 = Color32::from_rgb(0x16, 0x04, 0x18);
/// LIVE accent: cyan `#22d3ee` (ink `#03161c`).
const CYAN: Color32 = Color32::from_rgb(0x22, 0xd3, 0xee);
const CYAN_INK: Color32 = Color32::from_rgb(0x03, 0x16, 0x1c);

/// Pre-blend `fg` at opacity `alpha` over [`BG_WINDOW`] (opaque result), so a
/// CSS `rgba(accent, alpha)` wash renders as a flat token.
const fn blend_over_window(fg: Color32, alpha: u8) -> Color32 {
    let a = alpha as u16;
    let inv = 255 - a;
    let r = (fg.r() as u16 * a + BG_WINDOW.r() as u16 * inv) / 255;
    let g = (fg.g() as u16 * a + BG_WINDOW.g() as u16 * inv) / 255;
    let b = (fg.b() as u16 * a + BG_WINDOW.b() as u16 * inv) / 255;
    Color32::from_rgb(r as u8, g as u8, b as u8)
}

/// The accent palette for `mode`. REPLAY → fuchsia, LIVE → cyan. This is the
/// single mode→accent mapping the whole UI uses.
pub const fn accent_for(mode: Mode) -> Accent {
    match mode {
        Mode::Replay => Accent {
            base: FUCHSIA,
            // soft .12, band .17, dim .5 — alpha as 0–255.
            soft: blend_over_window(FUCHSIA, 31),
            band: blend_over_window(FUCHSIA, 43),
            dim: blend_over_window(FUCHSIA, 128),
            ink: FUCHSIA_INK,
        },
        Mode::Live => Accent {
            base: CYAN,
            soft: blend_over_window(CYAN, 31),
            band: blend_over_window(CYAN, 43),
            dim: blend_over_window(CYAN, 128),
            ink: CYAN_INK,
        },
    }
}

/// The mode pill's label.
pub const fn mode_label(mode: Mode) -> &'static str {
    match mode {
        Mode::Replay => "REPLAY",
        Mode::Live => "LIVE",
    }
}

/// Pill-dot opacity for the `~1.6s` blink, given seconds elapsed. Matches the
/// prototype's keyframes: opaque for the first 55% of the cycle, then `.18`.
/// Returned as `0.0..=1.0` so the renderer can scale the dot's alpha.
pub fn blink_opacity(elapsed_secs: f64) -> f32 {
    const PERIOD: f64 = 1.6;
    let phase = (elapsed_secs.rem_euclid(PERIOD)) / PERIOD;
    if phase < 0.55 {
        1.0
    } else {
        0.18
    }
}

/// Format a UTC timestamp as `YYYY-MM-DD HH:MM:SS` in the chosen timezone.
pub fn format_timestamp(ts: DateTime<Utc>, tz: Timezone) -> String {
    const FMT: &str = "%Y-%m-%d %H:%M:%S";
    match tz {
        Timezone::Utc => ts.format(FMT).to_string(),
        Timezone::Local => ts.with_timezone(&Local).format(FMT).to_string(),
    }
}

/// Format a UTC timestamp as a clock time `HH:MM:SS` in the chosen timezone
/// (the chart/scrubber axis labels). Respects the same LOCAL/UTC toggle as the
/// full-date [`format_timestamp`].
pub fn format_clock(ts: DateTime<Utc>, tz: Timezone) -> String {
    const FMT: &str = "%H:%M:%S";
    match tz {
        Timezone::Utc => ts.format(FMT).to_string(),
        Timezone::Local => ts.with_timezone(&Local).format(FMT).to_string(),
    }
}

/// Format the `msg` stat. REPLAY shows `cur / total` when the total is known,
/// else a running count; LIVE is always a running count.
pub fn format_msg_stat(mode: Mode, current: u64, total: Option<u64>) -> String {
    match (mode, total) {
        (Mode::Replay, Some(total)) => format!("{current} / {total}"),
        (Mode::Replay, None) | (Mode::Live, _) => current.to_string(),
    }
}

/// Whether the playback controls are interactive for `mode` (REPLAY only).
pub const fn controls_enabled(mode: Mode) -> bool {
    matches!(mode, Mode::Replay)
}

/// Install JetBrains Mono (bundled) as the default proportional *and* monospace
/// family, so the whole terminal-style UI is monospace. Bold is registered as a
/// fallback in both families.
pub fn install_fonts(ctx: &eframe::egui::Context) {
    let mut fonts = FontDefinitions::default();

    fonts.font_data.insert(
        "jetbrains-mono".to_owned(),
        std::sync::Arc::new(FontData::from_static(include_bytes!(
            "../assets/fonts/JetBrainsMono-Regular.ttf"
        ))),
    );
    fonts.font_data.insert(
        "jetbrains-mono-bold".to_owned(),
        std::sync::Arc::new(FontData::from_static(include_bytes!(
            "../assets/fonts/JetBrainsMono-Bold.ttf"
        ))),
    );

    for family in [FontFamily::Proportional, FontFamily::Monospace] {
        let list = fonts.families.entry(family).or_default();
        list.insert(0, "jetbrains-mono".to_owned());
        list.insert(1, "jetbrains-mono-bold".to_owned());
    }

    ctx.set_fonts(fonts);
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn accent_is_fuchsia_for_replay_cyan_for_live() {
        assert_eq!(accent_for(Mode::Replay).base, FUCHSIA);
        assert_eq!(accent_for(Mode::Replay).ink, FUCHSIA_INK);
        assert_eq!(accent_for(Mode::Live).base, CYAN);
        assert_eq!(accent_for(Mode::Live).ink, CYAN_INK);
        // The two modes never share an accent.
        assert_ne!(accent_for(Mode::Replay).base, accent_for(Mode::Live).base);
    }

    #[test]
    fn soft_band_dim_are_blended_between_window_and_base() {
        // A wash sits strictly between the window bg and the full accent.
        let a = accent_for(Mode::Replay);
        for v in [a.soft, a.band, a.dim] {
            assert!(v.r() > BG_WINDOW.r() && v.r() < FUCHSIA.r());
        }
        // Heavier alpha → closer to the base.
        assert!(a.dim.r() > a.band.r());
        assert!(a.band.r() > a.soft.r());
    }

    #[test]
    fn mode_labels_and_controls_gating() {
        assert_eq!(mode_label(Mode::Replay), "REPLAY");
        assert_eq!(mode_label(Mode::Live), "LIVE");
        assert!(controls_enabled(Mode::Replay));
        assert!(!controls_enabled(Mode::Live));
    }

    #[test]
    fn timezone_toggle_round_trips() {
        assert_eq!(Timezone::Local.toggled(), Timezone::Utc);
        assert_eq!(Timezone::Utc.toggled(), Timezone::Local);
        assert_eq!(Timezone::Local.toggled().toggled(), Timezone::Local);
        assert_eq!(Timezone::Local.label(), "LOCAL");
        assert_eq!(Timezone::Utc.label(), "UTC");
    }

    #[test]
    fn format_timestamp_utc_is_exact_and_local_matches_chrono() {
        let ts = Utc.with_ymd_and_hms(2026, 6, 29, 14, 28, 5).unwrap();
        assert_eq!(format_timestamp(ts, Timezone::Utc), "2026-06-29 14:28:05");
        // Local depends on the host tz; assert it equals chrono's own conversion
        // (so the function is correct regardless of where tests run).
        let expected = ts
            .with_timezone(&Local)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        assert_eq!(format_timestamp(ts, Timezone::Local), expected);
    }

    #[test]
    fn format_clock_utc_is_hms_and_local_matches_chrono() {
        let ts = Utc.with_ymd_and_hms(2026, 6, 29, 14, 28, 5).unwrap();
        assert_eq!(format_clock(ts, Timezone::Utc), "14:28:05");
        let expected = ts.with_timezone(&Local).format("%H:%M:%S").to_string();
        assert_eq!(format_clock(ts, Timezone::Local), expected);
    }

    #[test]
    fn msg_stat_shows_cur_total_in_replay_running_in_live() {
        assert_eq!(format_msg_stat(Mode::Replay, 537, Some(540)), "537 / 540");
        assert_eq!(format_msg_stat(Mode::Replay, 537, None), "537");
        // LIVE ignores any total.
        assert_eq!(format_msg_stat(Mode::Live, 537, Some(540)), "537");
        assert_eq!(format_msg_stat(Mode::Live, 537, None), "537");
    }

    #[test]
    fn blink_is_bright_at_start_dim_past_55_percent_and_periodic() {
        assert_eq!(blink_opacity(0.0), 1.0);
        assert_eq!(blink_opacity(0.5), 1.0); // < 0.55 * 1.6 = 0.88
        assert_eq!(blink_opacity(1.2), 0.18); // > 0.88
                                              // Periodic over ~1.6s.
        assert_eq!(blink_opacity(1.6), blink_opacity(0.0));
        assert_eq!(blink_opacity(1.6 + 1.2), blink_opacity(1.2));
    }
}
