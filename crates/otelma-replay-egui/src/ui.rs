//! Bespoke painters for the CHART view's center column: the chart header, the
//! price chart, and the volume sub-panel.
//!
//! Strict layering: this is **render-only** (egui/painter). Every range, window,
//! and extremum comes from the egui-free view-model ([`crate::state`]) and the
//! pure helpers in [`crate::series`]; nothing here does business math beyond
//! mapping already-computed data values to screen pixels.

use chrono::{DateTime, Duration, Utc};
use eframe::egui::{self, Align2, Color32, FontId, Pos2, Rect, Sense, Stroke, Vec2};
use egui::RichText;

use crate::series::{self, Range, SeriesMode, YScale};
use crate::state::AssetState;
use crate::theme::{self, Accent, Timezone};

/// Plot margins (px), per the design spec.
const MARGIN_L: f32 = 50.0;
const MARGIN_R: f32 = 56.0;
const MARGIN_T: f32 = 12.0;
const MARGIN_B: f32 = 22.0;

/// Grid / tick counts.
const Y_GRID_LINES: usize = 5;
const X_TICKS: usize = 5;

/// Chart header height (px).
pub const HEADER_H: f32 = 32.0;
/// Volume sub-panel height (px).
pub const VOLUME_H: f32 = 74.0;

/// Monospace font of `size` (the bundled JetBrains Mono is the default family).
fn mono(size: f32) -> FontId {
    FontId::monospace(size)
}

/// The chart header: market title (left, ellipsis), then the `AUTO | 0–1`
/// toggle, `mid <v>`, and `spread <v>` (right). Returns the (possibly toggled)
/// scale so the caller can store it.
pub fn chart_header(
    ui: &mut egui::Ui,
    accent: Accent,
    title: &str,
    asset: Option<&AssetState>,
    scale: YScale,
) -> YScale {
    let mut chosen = scale;
    ui.horizontal_centered(|ui| {
        ui.add_space(12.0);
        // Right group first (laid out R→L) so the title gets the leftover width.
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            ui.add_space(12.0);
            let (mid, spread) = match asset.and_then(AssetState::last_book) {
                Some(b) => (format!("{:.3}", b.mid), format!("{:.3}", b.spread)),
                None => ("—".to_string(), "—".to_string()),
            };
            ui.label(RichText::new(spread).size(11.0).color(theme::TEXT_PRIMARY));
            ui.label(RichText::new("spread").size(11.0).color(theme::TEXT_DIMMER));
            ui.add_space(10.0);
            ui.label(RichText::new(mid).size(11.0).color(accent.base).strong());
            ui.label(RichText::new("mid").size(11.0).color(theme::TEXT_DIMMER));
            ui.add_space(10.0);
            chosen = scale_toggle(ui, accent, scale);

            // Title fills the leftover space to the left.
            ui.with_layout(egui::Layout::left_to_right(egui::Align::Center), |ui| {
                ui.add(
                    egui::Label::new(RichText::new(title).size(11.0).color(theme::TEXT_PRIMARY))
                        .truncate(),
                );
            });
        });
    });
    chosen
}

/// The `AUTO | 0–1` segmented toggle. Returns the chosen scale.
fn scale_toggle(ui: &mut egui::Ui, accent: Accent, scale: YScale) -> YScale {
    let mut chosen = scale;
    egui::Frame::default()
        .stroke(Stroke::new(1.0, theme::BORDER_STRONG))
        .corner_radius(4.0)
        .inner_margin(egui::Margin::symmetric(1, 1))
        .show(ui, |ui| {
            ui.spacing_mut().item_spacing.x = 0.0;
            for opt in [YScale::Auto, YScale::Full] {
                let active = scale == opt;
                let label = match opt {
                    YScale::Auto => "AUTO",
                    YScale::Full => "0–1",
                };
                let (fg, bg) = if active {
                    (accent.base, accent.soft)
                } else {
                    (theme::TEXT_DIMMER, Color32::TRANSPARENT)
                };
                let resp = egui::Frame::default()
                    .fill(bg)
                    .inner_margin(egui::Margin::symmetric(8, 3))
                    .show(ui, |ui| {
                        ui.label(RichText::new(label).size(9.0).color(fg));
                    })
                    .response
                    .interact(Sense::click());
                if resp.clicked() {
                    chosen = opt;
                }
                if resp.hovered() {
                    ui.ctx().set_cursor_icon(egui::CursorIcon::PointingHand);
                }
            }
        });
    chosen
}

/// Shared X/Y → pixel mapping for one plot rect (already inset by the margins).
struct PlotMap {
    inner: Rect,
    x: Range,
    y: Range,
}

impl PlotMap {
    /// Map a `t_secs` to a pixel x within `inner`.
    fn px(&self, t: f64) -> f32 {
        self.inner.left() + (self.x.norm(t) as f32) * self.inner.width()
    }

    /// Map a value to a pixel y within `inner` (y grows downward → invert).
    fn py(&self, v: f64) -> f32 {
        self.inner.bottom() - (self.y.norm(v) as f32) * self.inner.height()
    }
}

/// Draw the price chart into `rect`. Returns the X mapping so the volume panel
/// can share it. `current_t` is the playhead (latest recorded `t_secs`); only
/// data up to it is drawn.
#[allow(clippy::too_many_arguments)]
pub fn price_chart(
    painter: &egui::Painter,
    rect: Rect,
    accent: Accent,
    asset: Option<&AssetState>,
    scale: YScale,
    mode: SeriesMode,
    tz: Timezone,
    start_ts: Option<DateTime<Utc>>,
    current_t: Option<f64>,
) -> PlotXMap {
    let inner = Rect::from_min_max(
        Pos2::new(rect.left() + MARGIN_L, rect.top() + MARGIN_T),
        Pos2::new(rect.right() - MARGIN_R, rect.bottom() - MARGIN_B),
    );
    let x = series::visible_x_window(mode, current_t);

    // Visible book points: inside the X window and up to the playhead.
    let now = current_t.unwrap_or(f64::INFINITY);
    let visible: Vec<_> = asset
        .map(|a| {
            a.book_series
                .iter()
                .copied()
                .filter(|p| p.t_secs >= x.min && p.t_secs <= x.max && p.t_secs <= now)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let y = series::auto_y_range(scale, &visible);
    let map = PlotMap { inner, x, y };

    // Grid + Y tick labels (3 decimals), top row = y.max.
    for i in 0..Y_GRID_LINES {
        let f = i as f64 / (Y_GRID_LINES - 1) as f64;
        let v = y.max - f * y.span();
        let py = map.py(v);
        painter.line_segment(
            [Pos2::new(inner.left(), py), Pos2::new(inner.right(), py)],
            Stroke::new(1.0, theme::GRID_LINE),
        );
        painter.text(
            Pos2::new(inner.left() - 6.0, py),
            Align2::RIGHT_CENTER,
            format!("{v:.3}"),
            mono(10.0),
            theme::TEXT_DIMMER,
        );
    }

    // X tick labels: clock times across the window.
    if let Some(start) = start_ts {
        for i in 0..X_TICKS {
            let f = i as f64 / (X_TICKS - 1) as f64;
            let t = x.min + f * x.span();
            painter.text(
                Pos2::new(map.px(t), inner.bottom() + 4.0),
                Align2::CENTER_TOP,
                clock_label(start, t, tz),
                mono(10.0),
                theme::TEXT_DIMMER,
            );
        }
    }

    if !visible.is_empty() {
        // Bid/ask band: bid-line (L→R) then ask-line (R→L), filled.
        let mut band: Vec<Pos2> = visible
            .iter()
            .map(|p| Pos2::new(map.px(p.t_secs), map.py(p.best_bid)))
            .collect();
        band.extend(
            visible
                .iter()
                .rev()
                .map(|p| Pos2::new(map.px(p.t_secs), map.py(p.best_ask))),
        );
        painter.add(egui::Shape::convex_polygon(band, accent.band, Stroke::NONE));

        // Mid line (accent, 1.8px, rounded joins via Shape::line).
        let mid: Vec<Pos2> = visible
            .iter()
            .map(|p| Pos2::new(map.px(p.t_secs), map.py(p.mid)))
            .collect();
        painter.add(egui::Shape::line(mid, Stroke::new(1.8, accent.base)));

        // Last-price dot + filled accent tag (mid in accent ink) at right margin.
        if let Some(last) = visible.last() {
            painter.circle_filled(
                Pos2::new(map.px(last.t_secs), map.py(last.mid)),
                2.8,
                accent.base,
            );
            let tag = Rect::from_min_size(
                Pos2::new(inner.right() + 2.0, map.py(last.mid) - 8.0),
                Vec2::new(50.0, 16.0),
            );
            painter.rect_filled(tag, 2.0, accent.base);
            painter.text(
                tag.center(),
                Align2::CENTER_CENTER,
                format!("{:.3}", last.mid),
                mono(10.0),
                accent.ink,
            );
        }
    }

    // Dashed playhead at the current time.
    if let Some(t) = current_t {
        draw_dashed_v(painter, map.px(t), inner.top(), inner.bottom(), accent.dim);
    }

    PlotXMap {
        inner_left: inner.left(),
        inner_width: inner.width(),
        x,
    }
}

/// The shared X mapping handed to the volume panel so its bars line up with the
/// chart's time axis exactly.
pub struct PlotXMap {
    inner_left: f32,
    inner_width: f32,
    x: Range,
}

impl PlotXMap {
    fn px(&self, t: f64) -> f32 {
        self.inner_left + (self.x.norm(t) as f32) * self.inner_width
    }
}

/// Draw the volume sub-panel into `rect`, sharing the chart's X mapping.
pub fn volume_panel(
    painter: &egui::Painter,
    rect: Rect,
    accent: Accent,
    asset: Option<&AssetState>,
    xmap: &PlotXMap,
    current_t: Option<f64>,
) {
    let baseline_y = rect.bottom() - MARGIN_B * 0.5;
    let top_y = rect.top() + 14.0;
    let height = (baseline_y - top_y).max(1.0);

    // Baseline.
    painter.line_segment(
        [
            Pos2::new(xmap.inner_left, baseline_y),
            Pos2::new(xmap.inner_left + xmap.inner_width, baseline_y),
        ],
        Stroke::new(1.0, theme::GRID_LINE),
    );

    // VOLUME caption (top-left).
    painter.text(
        Pos2::new(xmap.inner_left, rect.top() + 2.0),
        Align2::LEFT_TOP,
        "VOLUME",
        mono(9.0),
        theme::TEXT_FAINT,
    );

    let now = current_t.unwrap_or(f64::INFINITY);
    let max_vol = asset
        .and_then(|a| series::max_visible_volume(xmap.x, &a.volume))
        .filter(|m| *m > 0.0);

    if let (Some(a), Some(max_vol)) = (asset, max_vol) {
        // Max-volume label (e.g. `68k`), beside the caption.
        painter.text(
            Pos2::new(xmap.inner_left + 52.0, rect.top() + 2.0),
            Align2::LEFT_TOP,
            series::format_volume(max_vol),
            mono(9.0),
            theme::TEXT_DIMMER,
        );

        for v in &a.volume {
            if v.t_secs < xmap.x.min || v.t_secs > xmap.x.max || v.t_secs > now {
                continue;
            }
            let frac = (v.volume / max_vol).clamp(0.0, 1.0) as f32;
            let h = frac * height;
            let px = xmap.px(v.t_secs);
            let color = if v.up { green_bar() } else { red_bar() };
            painter.rect_filled(
                Rect::from_min_max(
                    Pos2::new(px - 1.0, baseline_y - h),
                    Pos2::new(px + 1.0, baseline_y),
                ),
                0.0,
                color,
            );
        }
    }

    // Same dashed playhead as the chart, so the two panels align.
    if let Some(t) = current_t {
        draw_dashed_v(painter, xmap.px(t), top_y, baseline_y, accent.dim);
    }
}

/// Up bar: green at ~50% over the window bg (`rgba(46,194,126,.5)`).
fn green_bar() -> Color32 {
    blend_over(theme::GREEN, theme::BG_WINDOW, 128)
}

/// Down bar: red at ~50% over the window bg (`rgba(229,72,77,.5)`).
fn red_bar() -> Color32 {
    blend_over(theme::RED, theme::BG_WINDOW, 128)
}

/// Pre-blend `fg` at `alpha`/255 over `bg` (opaque result).
fn blend_over(fg: Color32, bg: Color32, alpha: u8) -> Color32 {
    let a = alpha as u16;
    let inv = 255 - a;
    let mix = |f: u8, b: u8| ((f as u16 * a + b as u16 * inv) / 255) as u8;
    Color32::from_rgb(
        mix(fg.r(), bg.r()),
        mix(fg.g(), bg.g()),
        mix(fg.b(), bg.b()),
    )
}

/// A dashed vertical line (`[2,3]` dash) from `y0` to `y1` at `x`.
fn draw_dashed_v(painter: &egui::Painter, x: f32, y0: f32, y1: f32, color: Color32) {
    const ON: f32 = 2.0;
    const OFF: f32 = 3.0;
    let mut y = y0;
    while y < y1 {
        let seg_end = (y + ON).min(y1);
        painter.line_segment(
            [Pos2::new(x, y), Pos2::new(x, seg_end)],
            Stroke::new(1.0, color),
        );
        y = seg_end + OFF;
    }
}

/// Clock-time label (`HH:MM:SS`) for `t_secs` since `start`, in `tz`.
fn clock_label(start: DateTime<Utc>, t_secs: f64, tz: Timezone) -> String {
    let ts = start + Duration::milliseconds((t_secs * 1000.0) as i64);
    theme::format_clock(ts, tz)
}
