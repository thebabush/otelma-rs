//! Bespoke painters and widgets for the replayer body: the CHART view (chart
//! header, price chart, volume sub-panel), the left market sidebar, the
//! order-book ladder, the scrubber, and the dense CHAIN grid.
//!
//! Strict layering: this is **render-only** (egui/painter). Every range, window,
//! and extremum comes from the egui-free view-model ([`crate::state`]) and the
//! pure helpers in [`crate::series`]; nothing here does business math beyond
//! mapping already-computed data values to screen pixels.

use chrono::{DateTime, Duration, Utc};
use eframe::egui::{self, Align2, Color32, FontId, Pos2, Rect, Sense, Stroke, Vec2};
use egui::RichText;

use otelma_polymarket::AssetId;

use std::collections::BTreeMap;
use std::time::Instant;

use crate::series::{self, LadderSide, Range, SeriesMode, YScale, LADDER_DEPTH};
use crate::state::{AssetState, ChainGroup, ChainRow, MarketGroup, SideStats};
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

/// Paint a small play triangle centred at `c`. Painted (not a glyph) so it
/// renders regardless of the font's symbol coverage.
pub fn paint_play(painter: &egui::Painter, c: Pos2, color: Color32) {
    let s = 4.5;
    painter.add(egui::Shape::convex_polygon(
        vec![
            Pos2::new(c.x - s * 0.6, c.y - s),
            Pos2::new(c.x - s * 0.6, c.y + s),
            Pos2::new(c.x + s, c.y),
        ],
        color,
        Stroke::NONE,
    ));
}

/// Paint a small pause icon (two bars) centred at `c`.
pub fn paint_pause(painter: &egui::Painter, c: Pos2, color: Color32) {
    let (h, w, gap) = (5.0, 1.7, 1.8);
    painter.rect_filled(
        Rect::from_min_max(
            Pos2::new(c.x - gap - w, c.y - h),
            Pos2::new(c.x - gap, c.y + h),
        ),
        0.0,
        color,
    );
    painter.rect_filled(
        Rect::from_min_max(
            Pos2::new(c.x + gap, c.y - h),
            Pos2::new(c.x + gap + w, c.y + h),
        ),
        0.0,
        color,
    );
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
    preview_t: Option<f64>,
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
        // Bid/ask band: one triangle mesh forming a strip between the bid and ask
        // lines. A single polygon over the whole bid→reversed-ask ribbon is
        // *concave* (it wiggles with the price), so `convex_polygon` would fill
        // its convex hull — a giant wedge fanning from the extremes. Instead each
        // adjacent segment contributes a quad (bid_a, bid_b, ask_b, ask_a → two
        // triangles), so the strip is a thin ribbon hugging the mid line. A single
        // mesh keeps a long-session replay (tens of thousands of points) to one
        // draw, not one shape per segment.
        let mut band = egui::Mesh::default();
        for w in visible.windows(2) {
            let (a, b) = (&w[0], &w[1]);
            let base = band.vertices.len() as u32;
            for pos in [
                Pos2::new(map.px(a.t_secs), map.py(a.best_bid)),
                Pos2::new(map.px(b.t_secs), map.py(b.best_bid)),
                Pos2::new(map.px(b.t_secs), map.py(b.best_ask)),
                Pos2::new(map.px(a.t_secs), map.py(a.best_ask)),
            ] {
                band.colored_vertex(pos, accent.band);
            }
            band.add_triangle(base, base + 1, base + 2);
            band.add_triangle(base, base + 2, base + 3);
        }
        painter.add(egui::Shape::mesh(band));

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

    // Scrub-preview line (solid accent) while dragging the scrubber — shows where
    // a release would seek to, without moving the playhead.
    if let Some(pt) = preview_t {
        if pt >= x.min && pt <= x.max {
            painter.line_segment(
                [
                    Pos2::new(map.px(pt), inner.top()),
                    Pos2::new(map.px(pt), inner.bottom()),
                ],
                Stroke::new(1.5, accent.base),
            );
        }
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
    preview_t: Option<f64>,
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
    // Scrub-preview line, aligned with the chart's.
    if let Some(pt) = preview_t {
        if pt >= xmap.x.min && pt <= xmap.x.max {
            painter.line_segment(
                [
                    Pos2::new(xmap.px(pt), top_y),
                    Pos2::new(xmap.px(pt), baseline_y),
                ],
                Stroke::new(1.5, accent.base),
            );
        }
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
            // `ui.horizontal` keeps the row one line tall; inside it the price is
            // right-aligned (fixed width) and the name fills the remaining width
            // to its left and truncates — otherwise a long name runs under the
            // price (and a bare `with_layout` would balloon the row to full height).
            ui.horizontal(|ui| {
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    let price = match row.price {
                        Some(p) => format!("{p:.3}"),
                        None => "—".to_string(),
                    };
                    ui.label(RichText::new(price).size(11.0).color(price_color));
                    ui.add_space(8.0);
                    ui.with_layout(egui::Layout::left_to_right(egui::Align::Center), |ui| {
                        ui.add(
                            egui::Label::new(
                                RichText::new(&row.row_label).size(11.0).color(label_color),
                            )
                            .truncate(),
                        );
                    });
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

/// What a scrubber interaction wants done. A *drag* only previews (so the plots
/// can show a follow line) and the seek is committed on release; a plain *click*
/// seeks directly.
pub enum ScrubAction {
    /// Dragging to this time — show the preview line; do NOT seek yet.
    Preview(DateTime<Utc>),
    /// Drag released — commit the seek to the last previewed time.
    Release,
    /// A click (no drag) at this time — seek immediately.
    Click(DateTime<Utc>),
}

/// Render the scrubber and report the interaction as a [`ScrubAction`]. In
/// REPLAY, `bounds` is the session `[start, end]`: a drag *previews* a time (the
/// caller draws a follow line and seeks on [`ScrubAction::Release`]), a click
/// seeks immediately, and the time bubble shows only while hovering/dragging. In
/// LIVE, `bounds` is `None`: the track pins at 100%, is disabled, the right label
/// reads `LIVE`, and `None` is always returned (no seek).
pub fn scrubber(
    ui: &mut egui::Ui,
    accent: Accent,
    mode: Mode,
    tz: Timezone,
    bounds: Option<(DateTime<Utc>, DateTime<Utc>)>,
    current_ts: Option<DateTime<Utc>>,
) -> Option<ScrubAction> {
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
    let playhead_x = track_left + frac * track_w;

    // Map a track x back to a recorded time.
    let time_at = |x: f32| -> Option<DateTime<Utc>> {
        let (start, end) = bounds?;
        let f = ((x - track_left) / track_w).clamp(0.0, 1.0) as f64;
        let span_ms = (end - start).num_milliseconds() as f64;
        Some(start + Duration::milliseconds((f * span_ms) as i64))
    };

    // Interaction (REPLAY only). A drag only previews (no seek until release); a
    // click seeks directly. `scrub_x` is the cursor x while hovering or dragging —
    // used for the bubble, and (while dragging) for the handle.
    let mut action = None;
    let mut scrub_x: Option<f32> = None;
    let mut dragging = false;
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
        dragging = resp.dragged();
        if let Some(pos) = resp.interact_pointer_pos().or(resp.hover_pos()) {
            let cx = pos.x.clamp(track_left, track_right);
            scrub_x = Some(cx);
            if resp.dragged() {
                action = time_at(cx).map(ScrubAction::Preview);
            } else if resp.clicked() {
                action = time_at(cx).map(ScrubAction::Click);
            }
        }
        if resp.drag_stopped() {
            action = Some(ScrubAction::Release);
        }
        if resp.hovered() || resp.dragged() {
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

    // Handle follows the cursor while dragging (a preview of where a release would
    // seek); otherwise it sits at the actual playhead.
    let handle_x = if dragging {
        scrub_x.unwrap_or(playhead_x)
    } else {
        playhead_x
    };

    // Filled portion (left → handle) in accent at ~55%.
    let fill = Rect::from_min_max(
        Pos2::new(track_left, track_y - 2.0),
        Pos2::new(handle_x, track_y + 2.0),
    );
    painter.rect_filled(fill, 2.0, dim(accent.base.gamma_multiply(0.55)));

    // Handle circle.
    painter.circle_filled(Pos2::new(handle_x, track_y), 5.5, dim(accent.base));

    // Time bubble — shown ONLY while hovering or dragging the track (the toolbar
    // already shows the playhead clock). It reads the time under the cursor, and
    // floats above the track on a foreground layer so the short panel can't slice
    // it.
    if let Some(cx) = scrub_x {
        if let Some(t) = time_at(cx) {
            let bp = ui.ctx().layer_painter(egui::LayerId::new(
                egui::Order::Foreground,
                ui.id().with("scrubber_bubble"),
            ));
            let text = theme::format_clock(t, tz);
            let galley = bp.layout_no_wrap(text, mono(9.5), accent.base);
            let pad = Vec2::new(5.0, 2.0);
            let bubble =
                Rect::from_center_size(Pos2::new(cx, track_y - 22.0), galley.size() + pad * 2.0);
            bp.rect_filled(bubble, 3.0, theme::BG_INPUT);
            bp.rect_stroke(
                bubble,
                3.0,
                Stroke::new(1.0, accent.base),
                egui::StrokeKind::Inside,
            );
            bp.galley(bubble.min + pad, galley, accent.base);
        }
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

    action
}

// ── CHAIN grid ────────────────────────────────────────────────────────────────

/// CHAIN search-bar height (px).
const CHAIN_SEARCH_H: f32 = 44.0;
/// Sticky YES/NO band height (px).
const CHAIN_BAND_H: f32 = 17.0;
/// Per-market section-header height (px).
const CHAIN_SECTION_H: f32 = 22.0;
/// Data-row height (px).
const CHAIN_ROW_H: f32 = 21.0;
/// Outcome-column resize handle width (px).
const CHAIN_HANDLE_W: f32 = 7.0;
/// Outcome-column clamp bounds (px).
pub const CHAIN_COL_MIN: f32 = 90.0;
pub const CHAIN_COL_MAX: f32 = 340.0;
/// Chain grid line — a faint translucent white so the rules read over BOTH the
/// dark outcome column and the green/red quadrant tints (an opaque dark seam like
/// `BORDER_CELL` vanishes against the tint).
const CHAIN_GRID: Color32 = Color32::from_rgba_premultiplied(22, 22, 22, 22);
/// Zebra: a very mild white overlay applied to every other data row.
const CHAIN_STRIPE: Color32 = Color32::from_rgba_premultiplied(8, 8, 8, 8);
/// Cell horizontal padding (px), per spec (`padding 0×7`).
const CELL_PAD: f32 = 7.0;
/// Row-flash decay duration (s) — a calm ~0.18s ease-out back to the tint.
const FLASH_DECAY_SECS: f32 = 0.18;

/// One column in a YES (or NO) book block: its label and fixed pixel width.
struct ChainCol {
    label: &'static str,
    width: f32,
    kind: CellKind,
}

/// Which datum a cell shows (drives its formatting + colour).
#[derive(Clone, Copy)]
enum CellKind {
    Last,
    Vol,
    Chg,
    Spr,
    BidSz,
    Bid,
    Ask,
    AskSz,
}

/// The YES block columns, outer→center (the quote sits innermost).
const YES_COLS: [ChainCol; 8] = [
    ChainCol {
        label: "LAST",
        width: 54.0,
        kind: CellKind::Last,
    },
    ChainCol {
        label: "VOL",
        width: 60.0,
        kind: CellKind::Vol,
    },
    ChainCol {
        label: "CHG¢",
        width: 46.0,
        kind: CellKind::Chg,
    },
    ChainCol {
        label: "SPR¢",
        width: 44.0,
        kind: CellKind::Spr,
    },
    ChainCol {
        label: "BID SZ",
        width: 62.0,
        kind: CellKind::BidSz,
    },
    ChainCol {
        label: "BID",
        width: 52.0,
        kind: CellKind::Bid,
    },
    ChainCol {
        label: "ASK",
        width: 52.0,
        kind: CellKind::Ask,
    },
    ChainCol {
        label: "ASK SZ",
        width: 62.0,
        kind: CellKind::AskSz,
    },
];

/// The NO block columns mirror YES (center→outer): the quote sits innermost.
const NO_COLS: [ChainCol; 8] = [
    ChainCol {
        label: "BID SZ",
        width: 62.0,
        kind: CellKind::BidSz,
    },
    ChainCol {
        label: "BID",
        width: 52.0,
        kind: CellKind::Bid,
    },
    ChainCol {
        label: "ASK",
        width: 52.0,
        kind: CellKind::Ask,
    },
    ChainCol {
        label: "ASK SZ",
        width: 62.0,
        kind: CellKind::AskSz,
    },
    ChainCol {
        label: "SPR¢",
        width: 44.0,
        kind: CellKind::Spr,
    },
    ChainCol {
        label: "CHG¢",
        width: 46.0,
        kind: CellKind::Chg,
    },
    ChainCol {
        label: "VOL",
        width: 60.0,
        kind: CellKind::Vol,
    },
    ChainCol {
        label: "LAST",
        width: 54.0,
        kind: CellKind::Last,
    },
];

/// Sum of one block's column widths.
fn block_width() -> f32 {
    YES_COLS.iter().map(|c| c.width).sum()
}

/// The horizontal geometry shared by the band, section headers, and rows: where
/// the outcome column ends and where the YES / NO blocks start, so every row's
/// cells line up. The YES|NO book is centered in the space right of the outcome
/// column (a flex spacer on each side).
struct ChainLayout {
    /// Left edge of the whole grid.
    left: f32,
    /// Resizable outcome-column width (right border at `left + col_w`).
    col_w: f32,
    /// Left edge of the YES block.
    yes_left: f32,
    /// Left edge of the NO block (== YES block right edge; the center seam).
    no_left: f32,
    /// One block's total width.
    block_w: f32,
}

impl ChainLayout {
    fn new(full: Rect, col_w: f32) -> Self {
        let block_w = block_width();
        let books_w = block_w * 2.0;
        let region_left = full.left() + col_w;
        // Centre the YES|NO book in the space to the right of the outcome column.
        let spacer = ((full.right() - region_left) - books_w).max(0.0) * 0.5;
        let yes_left = region_left + spacer;
        Self {
            left: full.left(),
            col_w,
            yes_left,
            no_left: yes_left + block_w,
            block_w,
        }
    }

    /// Right edge of the outcome column.
    fn col_right(&self) -> f32 {
        self.left + self.col_w
    }
}

/// Render the whole CHAIN view: the search bar, the sticky YES/NO band, and a
/// scrollable grid of per-market sections (each a repeating header + data rows).
///
/// `query` is the live filter text (mutated by the search box). `groups` is the
/// already-filtered view-model. `col_w` is the app-owned outcome-column width;
/// the returned delta (from the resize handle drag) is added by the caller and
/// re-clamped. `flash` is render-only animation state (per-asset trade-flash
/// start + direction), keyed by the entity's YES asset id. Returns `(clicked,
/// col_delta)`: `clicked` is the YES asset of a clicked row (selects the
/// entity); `col_delta` is the resize-handle drag this frame.
pub fn chain_grid(
    ui: &mut egui::Ui,
    accent: Accent,
    query: &mut String,
    groups: &[ChainGroup],
    selected: Option<&AssetId>,
    col_w: f32,
    flash: &mut BTreeMap<AssetId, RowFlash>,
) -> (Option<AssetId>, f32) {
    let outcomes: usize = groups.iter().map(|g| g.rows.len()).sum();

    // Search bar (top, fixed height, bottom border).
    egui::Panel::top("chain_search")
        .exact_size(CHAIN_SEARCH_H)
        .frame(egui::Frame::default().fill(theme::BG_WINDOW))
        .show(ui, |ui| {
            chain_search_bar(ui, query, outcomes);
        });

    // Sticky YES/NO band (pinned above the scroll area).
    egui::Panel::top("chain_band")
        .exact_size(CHAIN_BAND_H)
        .frame(egui::Frame::default().fill(theme::BG_WINDOW))
        .show(ui, |ui| {
            let layout = ChainLayout::new(ui.max_rect(), col_w);
            chain_band(ui.painter(), ui.max_rect(), &layout);
        });

    // Scrollable grid body.
    let mut clicked: Option<AssetId> = None;
    let mut col_delta = 0.0;
    egui::CentralPanel::default()
        .frame(egui::Frame::default().fill(theme::BG_WINDOW))
        .show(ui, |ui| {
            egui::ScrollArea::vertical()
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    ui.spacing_mut().item_spacing = Vec2::ZERO;
                    let width = ui.available_width();
                    let mut row_i = 0usize; // global parity for zebra striping
                    for group in groups {
                        // Section header (carries the market name + col labels +
                        // a resize handle).
                        let (rect, _) = ui
                            .allocate_exact_size(Vec2::new(width, CHAIN_SECTION_H), Sense::hover());
                        let layout = ChainLayout::new(rect, col_w);
                        chain_section_header(ui.painter(), rect, &layout, &group.title);
                        col_delta += chain_resize_handle(ui, &layout, rect);

                        for row in &group.rows {
                            let striped = row_i % 2 == 1;
                            if chain_data_row(
                                ui, accent, &layout, width, striped, row, selected, flash,
                            ) {
                                clicked = Some(row.yes_asset.clone());
                            }
                            row_i += 1;
                        }
                    }
                });
        });

    (clicked, col_delta)
}

/// The CHAIN search bar: a `⌕` glyph + bounded text input, plus a right-aligned
/// `<N> OUTCOMES` count. Mirrors the sidebar search idiom.
fn chain_search_bar(ui: &mut egui::Ui, query: &mut String, outcomes: usize) {
    // Bottom border seam.
    let full = ui.max_rect();
    ui.painter().line_segment(
        [
            Pos2::new(full.left(), full.bottom()),
            Pos2::new(full.right(), full.bottom()),
        ],
        Stroke::new(1.0, theme::BORDER_STRONG),
    );

    ui.horizontal_centered(|ui| {
        ui.add_space(11.0);
        let edit = egui::TextEdit::singleline(query)
            .hint_text(RichText::new("search markets & outcomes…").color(theme::TEXT_DIMMER))
            .desired_width(300.0);
        ui.add(edit);

        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            ui.add_space(11.0);
            ui.label(
                RichText::new(format!("{outcomes} OUTCOMES"))
                    .size(10.0)
                    .color(theme::TEXT_FAINT),
            );
        });
    });
}

/// The sticky YES/NO band: `YES` centered over the YES block (green), `NO` over
/// the NO block (red); the outcome-column slot is left empty.
fn chain_band(painter: &egui::Painter, rect: Rect, layout: &ChainLayout) {
    let y = rect.center().y;
    painter.text(
        Pos2::new(layout.yes_left + layout.block_w * 0.5, y),
        Align2::CENTER_CENTER,
        "YES",
        mono(8.5),
        theme::GREEN,
    );
    painter.text(
        Pos2::new(layout.no_left + layout.block_w * 0.5, y),
        Align2::CENTER_CENTER,
        "NO",
        mono(8.5),
        theme::RED,
    );
}

/// A per-market section header: the market name in the outcome slot (ellipsis),
/// then center-aligned column labels for the YES and NO blocks. Background +
/// bottom border per spec.
fn chain_section_header(painter: &egui::Painter, rect: Rect, layout: &ChainLayout, title: &str) {
    painter.rect_filled(rect, 0.0, theme::BG_TOOLBAR);
    painter.line_segment(
        [
            Pos2::new(rect.left(), rect.bottom()),
            Pos2::new(rect.right(), rect.bottom()),
        ],
        Stroke::new(1.0, theme::BORDER_STRONG),
    );

    // Market name in the outcome-column slot (clipped to the column).
    let name_rect = Rect::from_min_max(
        Pos2::new(layout.left + CELL_PAD, rect.top()),
        Pos2::new(layout.col_right() - CELL_PAD, rect.bottom()),
    );
    painter.with_clip_rect(name_rect).text(
        Pos2::new(name_rect.left(), rect.center().y),
        Align2::LEFT_CENTER,
        title,
        mono(9.0),
        theme::TEXT_MUTED,
    );

    // Column labels, center-aligned per column, for both blocks.
    let y = rect.center().y;
    let paint_labels = |block_left: f32, cols: &[ChainCol]| {
        let mut x = block_left;
        for col in cols {
            painter.text(
                Pos2::new(x + col.width * 0.5, y),
                Align2::CENTER_CENTER,
                col.label,
                mono(9.0),
                theme::TEXT_DIMMER,
            );
            x += col.width;
        }
    };
    paint_labels(layout.yes_left, &YES_COLS);
    paint_labels(layout.no_left, &NO_COLS);
}

/// The 7px invisible drag handle on the outcome column's right edge. Returns the
/// width delta this frame (drag x-motion), clamped by the caller.
fn chain_resize_handle(ui: &mut egui::Ui, layout: &ChainLayout, rect: Rect) -> f32 {
    let handle = Rect::from_min_max(
        Pos2::new(layout.col_right() - CHAIN_HANDLE_W * 0.5, rect.top()),
        Pos2::new(layout.col_right() + CHAIN_HANDLE_W * 0.5, rect.bottom()),
    );
    let resp = ui.interact(
        handle,
        ui.id().with(("chain_col_handle", rect.top().to_bits())),
        Sense::drag(),
    );
    if resp.hovered() || resp.dragged() {
        ui.ctx().set_cursor_icon(egui::CursorIcon::ResizeHorizontal);
    }
    resp.drag_delta().x
}

/// One CHAIN data row: outcome name (resizable left column, ellipsis), then the
/// YES then NO book cells. Carries the resting quadrant tint, the trade
/// row-flash (decaying over the tint), and the active-row accent marker. Returns
/// `true` if the row was clicked.
#[allow(clippy::too_many_arguments)]
fn chain_data_row(
    ui: &mut egui::Ui,
    accent: Accent,
    layout: &ChainLayout,
    width: f32,
    striped: bool,
    row: &ChainRow,
    selected: Option<&AssetId>,
    flash: &mut BTreeMap<AssetId, RowFlash>,
) -> bool {
    let (rect, _) = ui.allocate_exact_size(Vec2::new(width, CHAIN_ROW_H), Sense::hover());
    let resp = ui.interact(
        rect,
        ui.id().with(("chain_row", row.yes_asset.as_str())),
        Sense::click(),
    );

    // Trade flash: detect a *new* trade for the entity (either side) by comparing
    // the latest trade time against what we last recorded, then start/refresh the
    // flash (wall-clock animation only — not on the data path).
    let trade = latest_trade(&row.yes, &row.no);
    let flash_factor = update_flash(flash, &row.yes_asset, trade);

    let painter = ui.painter();

    // YES / NO resting tints, blended with any active flash colour.
    let yes_tint = blend_over(theme::GREEN, theme::BG_WINDOW, 33); // ~.13
    let no_tint = blend_over(theme::RED, theme::BG_WINDOW, 31); // ~.12
    let (yes_bg, no_bg) = match flash_factor {
        Some((up, f)) => {
            // Flash target ~.34 alpha of the up/down colour, decaying to the tint.
            let flash_col = if up { theme::GREEN } else { theme::RED };
            let target = blend_over(flash_col, theme::BG_WINDOW, 87);
            let a = lerp_color(yes_tint, target, f);
            let b = lerp_color(no_tint, target, f);
            (a, b)
        }
        None => (yes_tint, no_tint),
    };

    // Paint the block backgrounds (the tint/flash quadrants).
    painter.rect_filled(
        Rect::from_min_max(
            Pos2::new(layout.yes_left, rect.top()),
            Pos2::new(layout.no_left, rect.bottom()),
        ),
        0.0,
        yes_bg,
    );
    painter.rect_filled(
        Rect::from_min_max(
            Pos2::new(layout.no_left, rect.top()),
            Pos2::new(layout.no_left + layout.block_w, rect.bottom()),
        ),
        0.0,
        no_bg,
    );

    // Zebra: a very mild light overlay on alternate rows, across the full width
    // (over both the dark outcome column and the tinted blocks).
    if striped {
        painter.rect_filled(rect, 0.0, CHAIN_STRIPE);
    }

    // Outcome name (clipped to the resizable column, ellipsis-like via clip).
    let name_rect = Rect::from_min_max(
        Pos2::new(layout.left + CELL_PAD, rect.top()),
        Pos2::new(layout.col_right() - CELL_PAD, rect.bottom()),
    );
    painter.with_clip_rect(name_rect).text(
        Pos2::new(name_rect.left(), rect.center().y),
        Align2::LEFT_CENTER,
        &row.outcome,
        mono(11.0),
        theme::TEXT_PRIMARY,
    );
    // Outcome-column right border.
    painter.line_segment(
        [
            Pos2::new(layout.col_right(), rect.top()),
            Pos2::new(layout.col_right(), rect.bottom()),
        ],
        Stroke::new(1.0, CHAIN_GRID),
    );

    // YES + NO cells.
    chain_block_cells(painter, rect, layout.yes_left, &YES_COLS, &row.yes);
    chain_block_cells(painter, rect, layout.no_left, &NO_COLS, &row.no);

    // Center seam (stronger) where YES's ASK SZ meets NO's BID SZ.
    painter.line_segment(
        [
            Pos2::new(layout.no_left, rect.top()),
            Pos2::new(layout.no_left, rect.bottom()),
        ],
        Stroke::new(1.0, theme::BORDER_SEAM_STRONG),
    );

    // Horizontal row underline — the missing axis of the grid (the vertical cell
    // seams are drawn per cell above). Full width so the chain reads as a table.
    painter.line_segment(
        [
            Pos2::new(rect.left(), rect.bottom()),
            Pos2::new(rect.right(), rect.bottom()),
        ],
        Stroke::new(1.0, CHAIN_GRID),
    );

    // Active-row marker: a 2px inset accent left border when either side is the
    // selected asset.
    let active = selected == Some(&row.yes_asset) || selected == Some(&row.no_asset);
    if active {
        painter.rect_filled(
            Rect::from_min_max(rect.min, Pos2::new(rect.left() + 2.0, rect.bottom())),
            0.0,
            accent.base,
        );
    }

    if resp.hovered() {
        ui.ctx().set_cursor_icon(egui::CursorIcon::PointingHand);
    }
    resp.clicked()
}

/// Paint one block's eight cells (right-aligned values + 1px right seams).
fn chain_block_cells(
    painter: &egui::Painter,
    rect: Rect,
    block_left: f32,
    cols: &[ChainCol],
    side: &SideStats,
) {
    let mut x = block_left;
    for col in cols {
        let (text, color) = cell_text(col.kind, side);
        painter.text(
            Pos2::new(x + col.width - CELL_PAD, rect.center().y),
            Align2::RIGHT_CENTER,
            text,
            mono(11.0),
            color,
        );
        // 1px right border.
        painter.line_segment(
            [
                Pos2::new(x + col.width, rect.top()),
                Pos2::new(x + col.width, rect.bottom()),
            ],
            Stroke::new(1.0, CHAIN_GRID),
        );
        x += col.width;
    }
}

/// Format + colour one cell from its [`SideStats`] datum. `None`/absent → `—`.
fn cell_text(kind: CellKind, s: &SideStats) -> (String, Color32) {
    match kind {
        // LAST: seconds since last trade, fixed 1-decimal seconds, single unit.
        CellKind::Last => (fmt_secs(s.last_secs), theme::TEXT_DIM),
        CellKind::Vol => (fmt_size(Some(s.vol)), theme::TEXT_DIM),
        CellKind::Chg => fmt_chg(s.chg_cents),
        CellKind::Spr => (fmt_cents(s.spr_cents), theme::TEXT_MUTED),
        CellKind::BidSz => (fmt_size(s.bid_sz), theme::TEXT_DIM),
        CellKind::Bid => (fmt_price(s.bid), theme::GREEN),
        CellKind::Ask => (fmt_price(s.ask), theme::RED),
        CellKind::AskSz => (fmt_size(s.ask_sz), theme::TEXT_DIM),
    }
}

/// Seconds since last trade: fixed 1-decimal seconds, single unit, never minutes.
fn fmt_secs(v: Option<f64>) -> String {
    match v {
        Some(s) if s.is_finite() => format!("{:.1}", s.max(0.0)),
        _ => "—".to_string(),
    }
}

/// Cents, 1 decimal (unsigned) — used for SPR¢.
fn fmt_cents(v: Option<f64>) -> String {
    match v {
        Some(c) if c.is_finite() => format!("{c:.1}"),
        _ => "—".to_string(),
    }
}

/// Signed cents (explicit `+`/`−`) coloured by sign — used for CHG¢.
fn fmt_chg(v: Option<f64>) -> (String, Color32) {
    match v {
        Some(c) if c.is_finite() => {
            // ASCII +/- (JetBrains Mono lacks the U+2212 minus → tofu); colour by
            // sign (flat = green).
            let sign = if c < 0.0 { "-" } else { "+" };
            let color = if c < 0.0 { theme::RED } else { theme::GREEN };
            (format!("{sign}{:.1}", c.abs()), color)
        }
        _ => ("—".to_string(), theme::TEXT_DIMMER),
    }
}

/// A probability price to 3 decimals.
fn fmt_price(v: Option<f64>) -> String {
    match v {
        Some(p) if p.is_finite() => format!("{p:.3}"),
        _ => "—".to_string(),
    }
}

/// A size / volume as a whole number (0 volume still shows as `0`).
fn fmt_size(v: Option<f64>) -> String {
    match v {
        Some(s) if s.is_finite() => format!("{s:.0}"),
        _ => "—".to_string(),
    }
}

/// The more-recent of the two sides' last trades, as `(t, up)`, or `None` if
/// neither side has traded. Drives the row-flash trigger.
fn latest_trade(yes: &SideStats, no: &SideStats) -> Option<(f64, bool)> {
    let pick = |s: &SideStats| s.last_trade_t.map(|t| (t, s.last_trade_up.unwrap_or(true)));
    match (pick(yes), pick(no)) {
        (Some(a), Some(b)) => Some(if a.0 >= b.0 { a } else { b }),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

/// Render-side row-flash state for one entity: when the animation started, its
/// up/down colour, and the trade time that armed it (so a *later* trade re-arms).
/// Wall-clock (`Instant`) here is purely visual — like the pill blink — and never
/// feeds derived data.
pub struct RowFlash {
    /// When the current flash started (wall clock).
    start: Instant,
    /// Flash colour: `true` = up/green, `false` = down/red.
    up: bool,
    /// The trade time (view-model `t_secs`) that armed this flash.
    armed_at_t: f64,
}

/// Update the per-entity flash state for the entity's latest trade and return the
/// active flash `(up, factor)` if one is in progress, where `factor` eases from
/// `1.0` at the trade down to `0.0` over [`FLASH_DECAY_SECS`]. A *later* trade
/// time (vs the armed one) re-arms the animation.
fn update_flash(
    flash: &mut BTreeMap<AssetId, RowFlash>,
    key: &AssetId,
    trade: Option<(f64, bool)>,
) -> Option<(bool, f32)> {
    let (t, up) = trade?;
    let now = Instant::now();
    let entry = flash.entry(key.clone());
    match entry {
        std::collections::btree_map::Entry::Occupied(mut e) => {
            if t != e.get().armed_at_t {
                // The latest trade changed — a new trade printed, or the timeline
                // was rewound/restarted (so `t` went backward). Either way re-arm
                // (using `!=`, not `>`, keeps the flash working after a seek/restart
                // rather than going stale against an old, larger armed time).
                e.insert(RowFlash {
                    start: now,
                    up,
                    armed_at_t: t,
                });
                Some((up, 1.0))
            } else {
                // Same trade as before → keep decaying from its start.
                let f = flash_factor(e.get().start);
                (f > 0.0).then_some((e.get().up, f))
            }
        }
        std::collections::btree_map::Entry::Vacant(e) => {
            e.insert(RowFlash {
                start: now,
                up,
                armed_at_t: t,
            });
            Some((up, 1.0))
        }
    }
}

/// Ease-out flash factor (`1.0`→`0.0`) for an animation started at `start`.
fn flash_factor(start: Instant) -> f32 {
    let dt = start.elapsed().as_secs_f32();
    if dt >= FLASH_DECAY_SECS {
        0.0
    } else {
        let lin = 1.0 - dt / FLASH_DECAY_SECS;
        // Ease-out (quadratic) for a calm decay.
        lin * lin
    }
}

/// Linear blend between two opaque colours by `f` in `0..=1` (`0` = `a`).
fn lerp_color(a: Color32, b: Color32, f: f32) -> Color32 {
    let f = f.clamp(0.0, 1.0);
    let mix = |x: u8, y: u8| (x as f32 + (y as f32 - x as f32) * f).round() as u8;
    Color32::from_rgb(mix(a.r(), b.r()), mix(a.g(), b.g()), mix(a.b(), b.b()))
}
