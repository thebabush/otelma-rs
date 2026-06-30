//! The eframe application: a terminal-style shell (title bar, toolbar, footer)
//! around a flexible body that hosts the price chart (or a CHAIN placeholder).
//!
//! Strict layering: this file is the **render layer** — egui only, no business
//! logic. The view-model lives in [`crate::state`] (egui-free); the typed design
//! tokens, fonts, and the pure mode/tz/stat helpers live in [`crate::theme`].
//!
//! Targets eframe 0.35's wgpu-default `App` API: the required entry point is
//! [`eframe::App::ui`] (a root [`egui::Ui`] with no margin), inside which we lay
//! out panels via `Panel::show(ui, …)`.

use std::path::Path;
use std::time::Instant;

use chrono::{DateTime, Utc};
use eframe::egui::{self, Color32, RichText};

use otelma_polymarket::AssetId;

use crate::feeder::Feeder;
use crate::live::LiveFeeder;
use crate::series::{SeriesMode, YScale};
use crate::state::ReplayState;
use crate::theme::{self, Accent, Mode, Timezone, ViewMode};
use crate::ui;

/// As-fast-as-possible speed.
const FAST_SPEED: f64 = f64::INFINITY;
/// Speed slider bounds (real-time multiplier).
const SPEED_MIN: f64 = 0.5;
const SPEED_MAX: f64 = 1000.0;

/// Fixed bar heights (px), per the design spec.
const TITLE_BAR_H: f32 = 32.0;
const TOOLBAR_H: f32 = 44.0;
const FOOTER_H: f32 = 24.0;

/// The data source the app renders: a paced replay (with playback controls) or
/// a live capture (no pacing — the controls are locked/dimmed).
enum Source {
    /// Replay a recorded session from disk, paced by a [`Feeder`].
    Replay {
        feeder: Feeder,
        /// Last finite speed chosen on the slider (restored when leaving "fast").
        last_finite_speed: f64,
        fast: bool,
    },
    /// Live-capture the venue while recording to disk, via a [`LiveFeeder`].
    Live { feeder: LiveFeeder },
}

impl Source {
    /// The replayer mode this source represents.
    fn mode(&self) -> Mode {
        match self {
            Source::Replay { .. } => Mode::Replay,
            Source::Live { .. } => Mode::Live,
        }
    }

    /// The chart series windowing for this source: replay keeps the full
    /// session; live shows the trailing window.
    fn series_mode(&self) -> SeriesMode {
        match self {
            Source::Replay { .. } => SeriesMode::Full,
            Source::Live { .. } => SeriesMode::Trailing,
        }
    }

    /// Snapshot the shared state under a short lock, whatever the mode.
    fn snapshot(&self) -> ReplayState {
        let state = match self {
            Source::Replay { feeder, .. } => &feeder.state,
            Source::Live { feeder } => &feeder.state,
        };
        state.lock().map(|s| s.clone()).unwrap_or_default()
    }

    /// Stop and join the underlying feeder thread (on window close).
    fn stop_and_join(&mut self) {
        match self {
            Source::Replay { feeder, .. } => feeder.stop_and_join(),
            Source::Live { feeder } => feeder.stop_and_join(),
        }
    }
}

/// The replayer application.
pub struct ReplayApp {
    source: Source,
    selected_asset: Option<AssetId>,
    /// Live search-box text filtering the market sidebar.
    search_query: String,
    /// REPLAY session `[start, end]` bounds for the scrubber, read once from the
    /// recording (cheap Parquet stats). `None` in LIVE (no fixed end) or if the
    /// recording can't be probed.
    bounds: Option<(DateTime<Utc>, DateTime<Utc>)>,
    /// Which body view is shown.
    view: ViewMode,
    /// Displayed timezone for every timestamp (source is UTC; default LOCAL).
    tz: Timezone,
    /// Chart Y-axis scaling (`AUTO` fit vs fixed `0–1`).
    y_scale: YScale,
    /// While dragging the scrubber: the previewed seek time (drawn as a follow
    /// line on the plots). `None` when not dragging; the actual seek happens on
    /// release. Render-only state.
    scrub_preview: Option<DateTime<Utc>>,
    /// While a forward seek is sweeping at max speed: its target time. The `max`
    /// checkbox shows active until the playhead reaches it, then this clears.
    ff_target: Option<DateTime<Utc>>,
    /// Whether fonts/visuals have been installed (done on first frame, where we
    /// have a `Context`).
    styled: bool,
    /// Wall-clock origin for the pill-blink animation (display-only — not on the
    /// determinism path).
    started: Instant,
}

impl ReplayApp {
    /// Build the app over a started replay [`Feeder`].
    pub fn new(feeder: Feeder) -> Self {
        let speed = feeder.control.speed();
        let fast = !speed.is_finite();
        // Probe the recording's time span once for the scrubber (cheap stats).
        let bounds = session_bounds(&feeder.session_dir);
        Self::with_source(
            Source::Replay {
                last_finite_speed: if fast { 1.0 } else { speed },
                fast,
                feeder,
            },
            bounds,
        )
    }

    /// Build the app over a started [`LiveFeeder`] (live capture + monitor).
    pub fn new_live(feeder: LiveFeeder) -> Self {
        Self::with_source(Source::Live { feeder }, None)
    }

    fn with_source(source: Source, bounds: Option<(DateTime<Utc>, DateTime<Utc>)>) -> Self {
        Self {
            source,
            selected_asset: None,
            search_query: String::new(),
            bounds,
            view: ViewMode::Chart,
            tz: Timezone::Local,
            y_scale: YScale::Auto,
            scrub_preview: None,
            ff_target: None,
            styled: false,
            started: Instant::now(),
        }
    }

    /// Snapshot the shared state under a short lock.
    fn snapshot(&self) -> ReplayState {
        self.source.snapshot()
    }

    // ── Title bar ────────────────────────────────────────────────────────────

    fn title_bar_ui(&mut self, ui: &mut egui::Ui, mode: Mode, accent: Accent) {
        ui.horizontal_centered(|ui| {
            ui.add_space(12.0);
            ui.label(
                RichText::new("otelma · replayer")
                    .size(11.0)
                    .color(theme::TEXT_TITLE),
            );

            ui.add_space(14.0);
            self.view_switch_ui(ui, accent);

            // Spacer pushes the mode pill to the far right.
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                ui.add_space(6.0);
                self.mode_pill_ui(ui, mode, accent);
            });
        });
    }

    /// `[ CHART | CHAIN ]` segmented view switch.
    fn view_switch_ui(&mut self, ui: &mut egui::Ui, accent: Accent) {
        egui::Frame::default()
            .stroke(egui::Stroke::new(1.0, theme::BORDER_STRONG))
            .corner_radius(5.0)
            .inner_margin(egui::Margin::symmetric(2, 1))
            .show(ui, |ui| {
                ui.spacing_mut().item_spacing.x = 0.0;
                for (i, mode) in [ViewMode::Chart, ViewMode::Chain].into_iter().enumerate() {
                    if i == 1 {
                        // Divider between segments.
                        let (rect, _) =
                            ui.allocate_exact_size(egui::vec2(1.0, 14.0), egui::Sense::hover());
                        ui.painter().rect_filled(rect, 0.0, theme::BORDER_STRONG);
                    }
                    let active = self.view == mode;
                    let label = match mode {
                        ViewMode::Chart => "CHART",
                        ViewMode::Chain => "CHAIN",
                    };
                    let (text_color, bg) = if active {
                        (accent.base, accent.soft)
                    } else {
                        (theme::TEXT_DIMMER, Color32::TRANSPARENT)
                    };
                    let resp = egui::Frame::default()
                        .fill(bg)
                        .inner_margin(egui::Margin::symmetric(12, 4))
                        .show(ui, |ui| {
                            ui.label(RichText::new(label).size(10.0).color(text_color));
                        })
                        .response
                        .interact(egui::Sense::click());
                    if resp.clicked() {
                        self.view = mode;
                    }
                    if resp.hovered() {
                        ui.ctx().set_cursor_icon(egui::CursorIcon::PointingHand);
                    }
                }
            });
    }

    /// Far-right mode pill (REPLAY fuchsia / LIVE cyan) with a blinking dot.
    fn mode_pill_ui(&self, ui: &mut egui::Ui, mode: Mode, accent: Accent) {
        let opacity = theme::blink_opacity(self.started.elapsed().as_secs_f64());
        // Blend the (blink-faded) accent dot over the pill's soft fill.
        let dot = accent.soft.blend(accent.base.gamma_multiply(opacity));
        egui::Frame::default()
            .fill(accent.soft)
            .stroke(egui::Stroke::new(1.0, theme::BORDER_STRONG))
            .corner_radius(5.0)
            .inner_margin(egui::Margin::symmetric(12, 4))
            .show(ui, |ui| {
                ui.horizontal_centered(|ui| {
                    let (rect, _) =
                        ui.allocate_exact_size(egui::vec2(6.0, 6.0), egui::Sense::hover());
                    ui.painter().circle_filled(rect.center(), 3.0, dot);
                    ui.add_space(6.0);
                    ui.label(
                        RichText::new(theme::mode_label(mode))
                            .size(10.0)
                            .color(accent.base),
                    );
                });
            });
    }

    // ── Toolbar ──────────────────────────────────────────────────────────────

    fn toolbar_ui(&mut self, ui: &mut egui::Ui, state: &ReplayState, mode: Mode, accent: Accent) {
        ui.horizontal_centered(|ui| {
            ui.add_space(8.0);
            // Left group: playback controls (disabled/dimmed in LIVE).
            ui.add_enabled_ui(theme::controls_enabled(mode), |ui| {
                self.playback_controls_ui(ui, accent);
            });

            // Right group: timestamp + TZ toggle + stats (always enabled).
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                ui.add_space(8.0);
                self.stats_ui(ui, state, mode);
                ui.add_space(10.0);
                self.timestamp_ui(ui, state);
            });
        });
    }

    fn playback_controls_ui(&mut self, ui: &mut egui::Ui, accent: Accent) {
        // Only meaningful for Replay; Live renders these disabled via the
        // enclosing `add_enabled_ui`, so just no-op the wiring there.
        let Source::Replay {
            feeder,
            last_finite_speed,
            fast,
        } = &mut self.source
        else {
            // LIVE: draw inert spec'd controls so the disabled state is visible.
            ui.label(RichText::new("Play").size(11.0).color(theme::TEXT_BRIGHT));
            ui.label(RichText::new("Restart").size(11.0).color(theme::TEXT_DIM));
            ui.label(RichText::new("SPEED").size(9.5).color(theme::TEXT_LABEL));
            return;
        };

        let paused = feeder.control.is_paused();
        let label = if paused { "Play" } else { "Pause" };
        let play = ui.button(
            RichText::new(format!("   {label}"))
                .size(11.0)
                .color(theme::TEXT_BRIGHT),
        );
        // Painted glyph (accent-tinted) — renders regardless of font coverage.
        let icon_c = egui::pos2(play.rect.left() + 12.0, play.rect.center().y);
        if paused {
            crate::ui::paint_play(ui.painter(), icon_c, accent.base);
        } else {
            crate::ui::paint_pause(ui.painter(), icon_c, accent.base);
        }
        if play.clicked() {
            if paused {
                feeder.control.resume();
            } else {
                feeder.control.pause();
            }
        }

        if ui
            .button(RichText::new("↺ Restart").size(11.0).color(theme::TEXT_DIM))
            .clicked()
        {
            feeder.restart();
        }

        ui.separator();

        ui.label(RichText::new("SPEED").size(9.5).color(theme::TEXT_LABEL));
        ui.add_enabled_ui(!*fast, |ui| {
            let mut speed = *last_finite_speed;
            let slider = egui::Slider::new(&mut speed, SPEED_MIN..=SPEED_MAX)
                .logarithmic(true)
                .suffix("×");
            if ui.add(slider).changed() {
                *last_finite_speed = speed;
                feeder.control.set_speed(speed);
            }
        });

        // Show `max` active during a forward-seek sweep too (not just steady max).
        let mut want_fast = *fast || self.ff_target.is_some();
        if ui.checkbox(&mut want_fast, "max").changed() {
            *fast = want_fast;
            if want_fast {
                feeder.control.set_speed(FAST_SPEED);
            } else {
                feeder.control.set_speed(*last_finite_speed);
            }
        }
    }

    fn timestamp_ui(&mut self, ui: &mut egui::Ui, state: &ReplayState) {
        // Clickable TZ toggle (drawn first because we lay out right-to-left).
        let tz_resp = ui
            .label(
                RichText::new(self.tz.label())
                    .size(9.0)
                    .color(theme::TEXT_TITLE)
                    .underline(),
            )
            .interact(egui::Sense::click());
        if tz_resp.clicked() {
            self.tz = self.tz.toggled();
        }
        if tz_resp.hovered() {
            ui.ctx().set_cursor_icon(egui::CursorIcon::PointingHand);
        }

        let ts = match state.current_ts {
            Some(ts) => theme::format_timestamp(ts, self.tz),
            None => "—".to_string(),
        };
        ui.label(RichText::new(ts).size(11.0).color(theme::TEXT_PRIMARY));
    }

    fn stats_ui(&self, ui: &mut egui::Ui, state: &ReplayState, mode: Mode) {
        // Laid out right-to-left: render msg, then seq.
        let seq = state
            .current_seq
            .map(|s| s.to_string())
            .unwrap_or_else(|| "—".to_string());
        // Total message count is not cheaply known from the streaming reader yet,
        // so REPLAY falls back to a running count (the `cur / total` form lands
        // with the scrubber in a later deliverable).
        let msg = theme::format_msg_stat(mode, state.message_count, None);

        ui.label(
            RichText::new(&msg)
                .size(11.0)
                .color(theme::TEXT_PRIMARY)
                .strong(),
        );
        ui.label(RichText::new("msg").size(11.0).color(theme::TEXT_DIMMER));
        ui.separator();
        ui.label(
            RichText::new(&seq)
                .size(11.0)
                .color(theme::TEXT_PRIMARY)
                .strong(),
        );
        ui.label(RichText::new("seq").size(11.0).color(theme::TEXT_DIMMER));
    }

    // ── Footer ───────────────────────────────────────────────────────────────

    fn footer_ui(&self, ui: &mut egui::Ui, mode: Mode) {
        ui.horizontal_centered(|ui| {
            ui.add_space(10.0);
            ui.label(
                RichText::new("space play/pause   ← → step frame   R restart")
                    .size(10.0)
                    .color(theme::TEXT_FAINT),
            );
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                ui.add_space(10.0);
                let note = match (mode, &self.source) {
                    (Mode::Replay, _) => String::new(),
                    (Mode::Live, Source::Live { feeder }) => {
                        format!(
                            "recording live · controls locked · {}",
                            feeder.out_dir.display()
                        )
                    }
                    // Mode and source always agree; this arm is unreachable.
                    (Mode::Live, Source::Replay { .. }) => {
                        "recording live · controls locked".to_string()
                    }
                };
                ui.label(RichText::new(note).size(10.0).color(theme::TEXT_FAINT));
            });
        });
    }

    // ── Body ─────────────────────────────────────────────────────────────────

    fn body_ui(&mut self, ui: &mut egui::Ui, state: &ReplayState) {
        match self.view {
            ViewMode::Chart => self.chart_body_ui(ui, state),
            ViewMode::Chain => self.chain_placeholder_ui(ui),
        }
    }

    /// CHART body: the market sidebar (left, 228px), the order-book ladder
    /// (right, 256px), and the center column (chart + volume + scrubber).
    fn chart_body_ui(&mut self, ui: &mut egui::Ui, state: &ReplayState) {
        let accent = theme::accent_for(self.source.mode());

        // Default the selection to the first known asset once data arrives.
        if self.selected_asset.is_none() {
            self.selected_asset = state.asset_ids().first().cloned();
        }

        // Left market sidebar.
        egui::Panel::left("market_sidebar")
            .exact_size(ui::SIDEBAR_W)
            .frame(egui::Frame::default().fill(theme::BG_WINDOW))
            .show(ui, |ui| {
                let groups = state.market_groups(&self.search_query);
                if let Some(clicked) = ui::market_sidebar(
                    ui,
                    accent,
                    &mut self.search_query,
                    &groups,
                    self.selected_asset.as_ref(),
                ) {
                    self.selected_asset = Some(clicked);
                }
            });

        // Right order-book ladder.
        let book_asset = self
            .selected_asset
            .as_ref()
            .and_then(|id| state.assets.get(id))
            .cloned();
        egui::Panel::right("order_book")
            .exact_size(ui::ORDER_BOOK_W)
            .frame(egui::Frame::default().fill(theme::BG_WINDOW))
            .show(ui, |ui| {
                ui::order_book(ui, book_asset.as_ref());
            });

        // Center column.
        egui::CentralPanel::default()
            .frame(egui::Frame::default().fill(theme::BG_WINDOW))
            .show(ui, |ui| {
                self.center_column_ui(ui, state);
            });
    }

    /// The CHART view's center column: header (top), scrubber (bottom), volume
    /// sub-panel (above the scrubber), price chart (fills).
    fn center_column_ui(&mut self, ui: &mut egui::Ui, state: &ReplayState) {
        let mode = self.source.mode();
        let accent = theme::accent_for(mode);
        let series_mode = self.source.series_mode();

        let asset = self
            .selected_asset
            .as_ref()
            .and_then(|id| state.assets.get(id));
        let title = self
            .selected_asset
            .as_ref()
            .map(|id| state.label_for(id))
            .unwrap_or_else(|| "—".to_string());

        // Chart header (~32px).
        egui::Panel::top("chart_header")
            .exact_size(ui::HEADER_H)
            .frame(
                egui::Frame::default()
                    .fill(theme::BG_TOOLBAR)
                    .inner_margin(egui::Margin::ZERO),
            )
            .show(ui, |ui| {
                self.y_scale = ui::chart_header(ui, accent, &title, asset, self.y_scale);
            });

        // Scrubber / timeline (48px, bottom). A drag only *previews* (a follow
        // line on the plots); the seek is committed on release. A click seeks now.
        let mut action = None;
        egui::Panel::bottom("scrubber")
            .exact_size(ui::SCRUBBER_H)
            .frame(
                egui::Frame::default()
                    .fill(theme::BG_WINDOW)
                    .inner_margin(egui::Margin::ZERO),
            )
            .show(ui, |ui| {
                action = ui::scrubber(ui, accent, mode, self.tz, self.bounds, state.current_ts);
            });
        match action {
            Some(ui::ScrubAction::Preview(t)) => self.scrub_preview = Some(t),
            Some(ui::ScrubAction::Release) => {
                if let Some(t) = self.scrub_preview.take() {
                    let forward = state.current_ts.is_some_and(|c| t > c);
                    if let Source::Replay { feeder, .. } = &mut self.source {
                        feeder.seek_to(t);
                    }
                    // A forward seek sweeps at max — show that on the checkbox
                    // until the playhead reaches the target.
                    self.ff_target = forward.then_some(t);
                }
            }
            Some(ui::ScrubAction::Click(t)) => {
                self.scrub_preview = None;
                self.ff_target = None;
                if let Source::Replay { feeder, .. } = &mut self.source {
                    feeder.seek_to(t);
                }
            }
            None => {}
        }

        let current_t = current_t_secs(state);
        // Preview time in chart seconds (the scrubber's follow line while dragging).
        let preview_t = self
            .scrub_preview
            .zip(state.start_ts)
            .map(|(p, start)| (p - start).num_milliseconds() as f64 / 1000.0);

        // The remaining body splits into the price chart (top, fills) and the
        // volume sub-panel (bottom, fixed). They share one painter and one X
        // mapping so the bars and the playhead line up exactly.
        egui::CentralPanel::default()
            .frame(egui::Frame::default().fill(theme::BG_WINDOW))
            .show(ui, |ui| {
                let full = ui.max_rect();
                let split_y = full.bottom() - ui::VOLUME_H;
                let chart_rect =
                    egui::Rect::from_min_max(full.min, egui::pos2(full.right(), split_y));
                let vol_rect = egui::Rect::from_min_max(egui::pos2(full.left(), split_y), full.max);
                let painter = ui.painter();
                let xmap = ui::price_chart(
                    painter,
                    chart_rect,
                    accent,
                    asset,
                    self.y_scale,
                    series_mode,
                    self.tz,
                    state.start_ts,
                    current_t,
                    preview_t,
                );
                ui::volume_panel(
                    painter, vol_rect, accent, asset, &xmap, current_t, preview_t,
                );
            });
    }

    fn chain_placeholder_ui(&self, ui: &mut egui::Ui) {
        ui.centered_and_justified(|ui| {
            ui.label(
                RichText::new("CHAIN view — coming in a later deliverable")
                    .size(11.0)
                    .color(theme::TEXT_DIMMER),
            );
        });
    }
}

/// The playhead time in seconds since the session start: the latest applied
/// message timestamp relative to the first. Derived only from recorded message
/// times (`current_ts`/`start_ts`) — never the wall clock — so it stays on the
/// determinism path. `None` before any data arrives.
fn current_t_secs(state: &ReplayState) -> Option<f64> {
    let start = state.start_ts?;
    let now = state.current_ts?;
    Some((now - start).num_milliseconds() as f64 / 1000.0)
}

/// Probe a recording's `[start, end]` span for the scrubber via the engine's
/// cheap [`otelma::session_time_bounds`]. A probe failure (unreadable dir) is
/// logged and treated as "no bounds" — the scrubber still renders, just without
/// seek/end-clock.
fn session_bounds(session_dir: &Path) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    match otelma::session_time_bounds(session_dir) {
        Ok(bounds) => bounds,
        Err(e) => {
            eprintln!(
                "replay: could not read time bounds of {}: {e}",
                session_dir.display()
            );
            None
        }
    }
}

impl eframe::App for ReplayApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        if !self.styled {
            theme::install_fonts(&ctx);
            let mut visuals = egui::Visuals::dark();
            visuals.panel_fill = theme::BG_WINDOW;
            visuals.window_fill = theme::BG_WINDOW;
            visuals.override_text_color = Some(theme::TEXT_PRIMARY);
            ctx.set_visuals(visuals);
            self.styled = true;
        }

        let mode = self.source.mode();
        let accent = theme::accent_for(mode);
        let state = self.snapshot();

        // A forward-seek sweep is done once the playhead reaches its target; clear
        // the "max" indicator then.
        if let Some(t) = self.ff_target {
            if state.current_ts.is_some_and(|c| c >= t) {
                self.ff_target = None;
            }
        }

        egui::Panel::top("title_bar")
            .exact_size(TITLE_BAR_H)
            .frame(
                egui::Frame::default()
                    .fill(theme::BG_TITLE)
                    .inner_margin(egui::Margin::symmetric(8, 0)),
            )
            .show_separator_line(true)
            .show(ui, |ui| {
                self.title_bar_ui(ui, mode, accent);
            });

        egui::Panel::top("toolbar")
            .exact_size(TOOLBAR_H)
            .frame(
                egui::Frame::default()
                    .fill(theme::BG_TOOLBAR)
                    .inner_margin(egui::Margin::symmetric(4, 0)),
            )
            .show(ui, |ui| {
                self.toolbar_ui(ui, &state, mode, accent);
            });

        egui::Panel::bottom("footer")
            .exact_size(FOOTER_H)
            .frame(egui::Frame::default().fill(theme::BG_TOOLBAR))
            .show(ui, |ui| {
                self.footer_ui(ui, mode);
            });

        egui::CentralPanel::default()
            .frame(egui::Frame::default().fill(theme::BG_WINDOW))
            .show(ui, |ui| {
                self.body_ui(ui, &state);
            });

        // Animate the pill blink and any live/replay progress.
        ctx.request_repaint_after(std::time::Duration::from_millis(33));
    }

    fn on_exit(&mut self) {
        self.source.stop_and_join();
    }
}
