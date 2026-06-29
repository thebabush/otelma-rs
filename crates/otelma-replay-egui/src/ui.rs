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

use otelma_polymarket::AssetId;

use crate::series::{self, LadderSide, Range, SeriesMode, YScale, LADDER_DEPTH};
use crate::state::{AssetState, MarketGroup};
use crate::theme::{self, Accent, Mode, Timezone};

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

// ── Left market sidebar ──────────────────────────────────────────────────────

/// Left market-sidebar width (px).
pub const SIDEBAR_W: f32 = 228.0;

/// Render the grouped market list with a search box at the top. `query` is the
/// live filter text (mutated by the search box); `groups` is the already-filtered
/// view-model list. Returns `Some(asset)` when a row is clicked (selects it).
pub fn market_sidebar(
    ui: &mut egui::Ui,
    accent: Accent,
    query: &mut String,
    groups: &[MarketGroup],
    selected: Option<&AssetId>,
) -> Option<AssetId> {
    let mut clicked: Option<AssetId> = None;

    // Search box (top).
    egui::Frame::default()
        .inner_margin(egui::Margin::symmetric(11, 10))
        .show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(RichText::new("⌕").size(11.0).color(theme::TEXT_FAINT));
                let edit = egui::TextEdit::singleline(query)
                    .hint_text(RichText::new("search markets…").color(theme::TEXT_DIMMER))
                    .desired_width(f32::INFINITY);
                ui.add(edit);
            });
        });

    egui::ScrollArea::vertical()
        .auto_shrink([false, false])
        .show(ui, |ui| {
            ui.spacing_mut().item_spacing.y = 0.0;
            if groups.is_empty() {
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    ui.add_space(13.0);
                    ui.label(
                        RichText::new("no markets")
                            .size(11.0)
                            .color(theme::TEXT_DIMMER),
                    );
                });
            }
            for group in groups {
                group_header(ui, &group.title);
                for row in &group.rows {
                    let active = selected == Some(&row.asset_id);
                    if sidebar_row(ui, accent, row, active) {
                        clicked = Some(row.asset_id.clone());
                    }
                }
            }
        });

    clicked
}

/// A group (event) header row.
fn group_header(ui: &mut egui::Ui, title: &str) {
    egui::Frame::default()
        .inner_margin(egui::Margin {
            left: 13,
            right: 13,
            top: 9,
            bottom: 5,
        })
        .show(ui, |ui| {
            ui.add(
                egui::Label::new(RichText::new(title).size(9.5).color(theme::TEXT_FAINT))
                    .truncate(),
            );
        });
}

/// One outcome row: outcome label (left) + price (right). The active row uses
/// accent text + soft bg + a 2px accent left border. Returns `true` if clicked.
fn sidebar_row(
    ui: &mut egui::Ui,
    accent: Accent,
    row: &crate::state::MarketRow,
    active: bool,
) -> bool {
    let (label_color, price_color, bg) = if active {
        (accent.base, theme::TEXT_PRIMARY, accent.soft)
    } else {
        (theme::TEXT_MUTED, theme::TEXT_DIMMER, Color32::TRANSPARENT)
    };

    let resp = egui::Frame::default()
        .fill(bg)
        .inner_margin(egui::Margin::symmetric(13, 6))
        .show(ui, |ui| {
            ui.set_width(ui.available_width());
            ui.horizontal(|ui| {
                ui.add(
                    egui::Label::new(RichText::new(&row.row_label).size(11.0).color(label_color))
                        .truncate(),
                );
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    let price = match row.price {
                        Some(p) => format!("{p:.3}"),
                        None => "—".to_string(),
                    };
                    ui.label(RichText::new(price).size(11.0).color(price_color));
                });
            });
        })
        .response
        .interact(Sense::click());

    // 2px accent left border on the active row.
    if active {
        let r = resp.rect;
        ui.painter().rect_filled(
            Rect::from_min_max(r.min, Pos2::new(r.left() + 2.0, r.bottom())),
            0.0,
            accent.base,
        );
    }
    if resp.hovered() {
        ui.ctx().set_cursor_icon(egui::CursorIcon::PointingHand);
    }
    resp.clicked()
}

// ── Order-book ladder ────────────────────────────────────────────────────────

/// Order-book ladder width (px).
pub const ORDER_BOOK_W: f32 = 256.0;

/// Order-book row height (px).
const BOOK_ROW_H: f32 = 23.0;
/// Spread row height (px).
const SPREAD_ROW_H: f32 = 30.0;
/// Header height (px).
const BOOK_HEADER_H: f32 = 32.0;

/// Render the order-book ladder for `asset` (asks on top, a spread row, bids
/// below), with depth bars drawn from the right edge ∝ size/maxSize.
pub fn order_book(ui: &mut egui::Ui, asset: Option<&AssetState>) {
    let full = ui.max_rect();
    let painter = ui.painter();

    // Left border seam.
    painter.line_segment(
        [
            Pos2::new(full.left(), full.top()),
            Pos2::new(full.left(), full.bottom()),
        ],
        Stroke::new(1.0, theme::BORDER_SEAM),
    );

    // Header.
    let header = Rect::from_min_size(full.min, Vec2::new(full.width(), BOOK_HEADER_H));
    painter.text(
        Pos2::new(header.left() + 12.0, header.center().y),
        Align2::LEFT_CENTER,
        "ORDER BOOK",
        mono(9.5),
        theme::TEXT_FAINT,
    );
    painter.text(
        Pos2::new(header.right() - 12.0, header.center().y),
        Align2::RIGHT_CENTER,
        "price · size",
        mono(9.5),
        theme::TEXT_FAINT,
    );
    painter.line_segment(
        [
            Pos2::new(full.left(), header.bottom()),
            Pos2::new(full.right(), header.bottom()),
        ],
        Stroke::new(1.0, theme::BORDER_SEAM),
    );

    let Some(a) = asset else { return };
    let asks = LadderSide::from_levels(&a.depth_asks);
    let bids = LadderSide::from_levels(&a.depth_bids);

    // Lay the ladder out centered vertically around the spread row.
    let asks_h = BOOK_ROW_H * LADDER_DEPTH as f32;
    let bids_h = BOOK_ROW_H * LADDER_DEPTH as f32;
    let total = asks_h + SPREAD_ROW_H + bids_h;
    let body_top = header.bottom();
    let body_h = full.bottom() - body_top;
    let top = body_top + ((body_h - total) * 0.5).max(0.0);

    // Asks: highest price at the top, best ask nearest the spread (bottom).
    // `depth_asks` is best-first (ascending price), so render reversed.
    let ask_max = asks.max_size.max(1.0);
    for (i, (price, size)) in asks.levels.iter().rev().enumerate() {
        let y = top + i as f32 * BOOK_ROW_H;
        book_row(
            painter,
            full,
            y,
            *price,
            *size,
            *size / ask_max,
            theme::RED,
            ask_depth_bar(),
        );
    }

    // Spread row (between asks and bids).
    let spread_top = top + asks_h;
    let spread_rect = Rect::from_min_size(
        Pos2::new(full.left(), spread_top),
        Vec2::new(full.width(), SPREAD_ROW_H),
    );
    painter.line_segment(
        [
            Pos2::new(full.left(), spread_rect.top()),
            Pos2::new(full.right(), spread_rect.top()),
        ],
        Stroke::new(1.0, theme::BORDER_SEAM),
    );
    painter.line_segment(
        [
            Pos2::new(full.left(), spread_rect.bottom()),
            Pos2::new(full.right(), spread_rect.bottom()),
        ],
        Stroke::new(1.0, theme::BORDER_SEAM),
    );
    let spread = a.last_book().map(|b| b.spread);
    painter.text(
        Pos2::new(spread_rect.left() + 12.0, spread_rect.center().y),
        Align2::LEFT_CENTER,
        "spread",
        mono(10.0),
        theme::TEXT_DIMMER,
    );
    painter.text(
        Pos2::new(spread_rect.right() - 12.0, spread_rect.center().y),
        Align2::RIGHT_CENTER,
        spread
            .map(|s| format!("{s:.3}"))
            .unwrap_or_else(|| "—".into()),
        mono(13.0),
        theme::TEXT_BRIGHT,
    );

    // Bids: best bid at the top, just below the spread.
    let bids_top = spread_rect.bottom();
    let bid_max = bids.max_size.max(1.0);
    for (i, (price, size)) in bids.levels.iter().enumerate() {
        let y = bids_top + i as f32 * BOOK_ROW_H;
        book_row(
            painter,
            full,
            y,
            *price,
            *size,
            *size / bid_max,
            theme::GREEN,
            bid_depth_bar(),
        );
    }
}

/// One ladder row: a depth bar from the right edge (∝ `frac`), the price (left,
/// colored) and the size (right).
#[allow(clippy::too_many_arguments)]
fn book_row(
    painter: &egui::Painter,
    full: Rect,
    y: f32,
    price: f64,
    size: f64,
    frac: f64,
    price_color: Color32,
    bar_color: Color32,
) {
    let row = Rect::from_min_size(
        Pos2::new(full.left(), y),
        Vec2::new(full.width(), BOOK_ROW_H),
    );
    // Depth bar grows from the right edge.
    let bar_w = (frac.clamp(0.0, 1.0) as f32) * row.width();
    painter.rect_filled(
        Rect::from_min_max(Pos2::new(row.right() - bar_w, row.top()), row.max),
        0.0,
        bar_color,
    );
    painter.text(
        Pos2::new(row.left() + 12.0, row.center().y),
        Align2::LEFT_CENTER,
        format!("{price:.3}"),
        mono(11.0),
        price_color,
    );
    painter.text(
        Pos2::new(row.right() - 12.0, row.center().y),
        Align2::RIGHT_CENTER,
        format!("{size:.0}"),
        mono(11.0),
        theme::TEXT_MUTED,
    );
}

/// Faint red ask depth bar (`rgba(229,72,77,.14)`), pre-blended over the window.
fn ask_depth_bar() -> Color32 {
    blend_over(theme::RED, theme::BG_WINDOW, 36)
}

/// Faint green bid depth bar (`rgba(46,194,126,.14)`), pre-blended.
fn bid_depth_bar() -> Color32 {
    blend_over(theme::GREEN, theme::BG_WINDOW, 36)
}

// ── Scrubber / timeline ──────────────────────────────────────────────────────

/// Scrubber/timeline height (px).
pub const SCRUBBER_H: f32 = 48.0;

/// Render the scrubber. In REPLAY, `bounds` is the session `[start, end]` and a
/// drag maps the x-fraction to a target time, returned as `Some(target)` to seek.
/// In LIVE, `bounds` is `None`: the track pins at 100% and is disabled, the right
/// label reads `LIVE`, and `None` is always returned (no seek).
pub fn scrubber(
    ui: &mut egui::Ui,
    accent: Accent,
    mode: Mode,
    tz: Timezone,
    bounds: Option<(DateTime<Utc>, DateTime<Utc>)>,
    current_ts: Option<DateTime<Utc>>,
) -> Option<DateTime<Utc>> {
    let full = ui.max_rect();
    // Top border seam.
    ui.painter().line_segment(
        [
            Pos2::new(full.left(), full.top()),
            Pos2::new(full.right(), full.top()),
        ],
        Stroke::new(1.0, theme::BORDER_SEAM),
    );

    // Track geometry: inset for the start/end clock labels.
    const LABEL_W: f32 = 64.0;
    let track_left = full.left() + LABEL_W;
    let track_right = full.right() - LABEL_W;
    let track_y = full.center().y;
    let track_w = (track_right - track_left).max(1.0);

    let live = matches!(mode, Mode::Live);

    // Playhead fraction within [start, end] (0..1). LIVE pins to the right.
    let frac = if live {
        1.0
    } else {
        match (bounds, current_ts) {
            (Some((start, end)), Some(now)) => {
                let span = (end - start).num_milliseconds() as f64;
                if span <= 0.0 {
                    0.0
                } else {
                    ((now - start).num_milliseconds() as f64 / span).clamp(0.0, 1.0)
                }
            }
            _ => 0.0,
        }
    } as f32;

    // Interaction: a drag over the track seeks (REPLAY only).
    let mut seek: Option<DateTime<Utc>> = None;
    let track_rect = Rect::from_min_max(
        Pos2::new(track_left, full.top()),
        Pos2::new(track_right, full.bottom()),
    );
    if !live {
        let resp = ui.interact(
            track_rect,
            ui.id().with("scrubber_track"),
            Sense::click_and_drag(),
        );
        if (resp.dragged() || resp.clicked()) && track_w > 0.0 {
            if let (Some((start, end)), Some(pos)) = (bounds, resp.interact_pointer_pos()) {
                let f = ((pos.x - track_left) / track_w).clamp(0.0, 1.0) as f64;
                let span_ms = (end - start).num_milliseconds() as f64;
                seek = Some(start + Duration::milliseconds((f * span_ms) as i64));
            }
        }
        if resp.hovered() {
            ui.ctx().set_cursor_icon(egui::CursorIcon::PointingHand);
        }
    }

    // Dim everything in LIVE (disabled look).
    let dim = |c: Color32| if live { c.gamma_multiply(0.6) } else { c };
    let painter = ui.painter();

    // Track background (4px bar).
    let track_bar = Rect::from_min_max(
        Pos2::new(track_left, track_y - 2.0),
        Pos2::new(track_right, track_y + 2.0),
    );
    painter.rect_filled(track_bar, 2.0, dim(theme::BG_CONTROL));

    // Filled portion (left → playhead) in accent at ~55%.
    let handle_x = track_left + frac * track_w;
    let fill = Rect::from_min_max(
        Pos2::new(track_left, track_y - 2.0),
        Pos2::new(handle_x, track_y + 2.0),
    );
    painter.rect_filled(fill, 2.0, dim(accent.base.gamma_multiply(0.55)));

    // Handle circle.
    painter.circle_filled(Pos2::new(handle_x, track_y), 5.5, dim(accent.base));

    // Time bubble above the handle (playhead clock, in tz).
    if let Some(now) = current_ts {
        let bubble_center = Pos2::new(handle_x, track_y - 22.0);
        let text = theme::format_clock(now, tz);
        let galley = painter.layout_no_wrap(text, mono(9.5), dim(accent.base));
        let pad = Vec2::new(5.0, 2.0);
        let bubble = Rect::from_center_size(bubble_center, galley.size() + pad * 2.0);
        painter.rect_filled(bubble, 3.0, theme::BG_INPUT);
        painter.rect_stroke(
            bubble,
            3.0,
            Stroke::new(1.0, dim(accent.base)),
            egui::StrokeKind::Inside,
        );
        painter.galley(bubble.min + pad, galley, dim(accent.base));
    }

    // Start label (left) and end / LIVE label (right).
    if let Some((start, _)) = bounds {
        painter.text(
            Pos2::new(full.left() + 10.0, track_y),
            Align2::LEFT_CENTER,
            theme::format_clock(start, tz),
            mono(10.0),
            theme::TEXT_DIMMER,
        );
    }
    let right_label = match (live, bounds) {
        (true, _) => "LIVE".to_string(),
        (false, Some((_, end))) => theme::format_clock(end, tz),
        (false, None) => "—".to_string(),
    };
    painter.text(
        Pos2::new(full.right() - 10.0, track_y),
        Align2::RIGHT_CENTER,
        right_label,
        mono(10.0),
        if live {
            accent.base
        } else {
            theme::TEXT_DIMMER
        },
    );

    seek
}
