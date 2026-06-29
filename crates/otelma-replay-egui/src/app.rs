//! The eframe application: reads shared [`ReplayState`] each frame and renders
//! price plots, a depth view, and playback controls.
//!
//! Targets eframe 0.35's wgpu-default `App` API: the required entry point is
//! [`eframe::App::ui`] (a root [`egui::Ui`] with no margin), inside which we lay
//! out panels via `Panel::show(ui, …)`.

use eframe::egui;
use egui_plot::{Line, MarkerShape, Plot, Points};

use otelma_polymarket::AssetId;

use crate::feeder::Feeder;
use crate::live::LiveFeeder;
use crate::state::ReplayState;

/// As-fast-as-possible speed.
const FAST_SPEED: f64 = f64::INFINITY;
/// Speed slider bounds (real-time multiplier).
const SPEED_MIN: f64 = 0.5;
const SPEED_MAX: f64 = 1000.0;

/// The data source the app renders: a paced replay (with playback controls) or
/// a live capture (no pacing — the controls are meaningless and hidden).
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
}

impl ReplayApp {
    /// Build the app over a started replay [`Feeder`].
    pub fn new(feeder: Feeder) -> Self {
        let speed = feeder.control.speed();
        let fast = !speed.is_finite();
        Self {
            source: Source::Replay {
                last_finite_speed: if fast { 1.0 } else { speed },
                fast,
                feeder,
            },
            selected_asset: None,
        }
    }

    /// Build the app over a started [`LiveFeeder`] (live capture + monitor).
    pub fn new_live(feeder: LiveFeeder) -> Self {
        Self {
            source: Source::Live { feeder },
            selected_asset: None,
        }
    }

    /// Snapshot the shared state under a short lock.
    fn snapshot(&self) -> ReplayState {
        self.source.snapshot()
    }

    fn controls_ui(&mut self, ui: &mut egui::Ui, state: &ReplayState) {
        // `restart` re-opens the reader; clear the selection afterwards. Tracked
        // here so the closure can borrow `self.source` mutably without also
        // touching `self.selected_asset`.
        let mut restarted = false;
        match &mut self.source {
            Source::Replay {
                feeder,
                last_finite_speed,
                fast,
            } => {
                ui.horizontal(|ui| {
                    let paused = feeder.control.is_paused();
                    if ui
                        .button(if paused { "▶ Play" } else { "⏸ Pause" })
                        .clicked()
                    {
                        if paused {
                            feeder.control.resume();
                        } else {
                            feeder.control.pause();
                        }
                    }
                    if ui.button("⟲ Restart").clicked() {
                        feeder.restart();
                        restarted = true;
                    }

                    ui.separator();

                    let mut want_fast = *fast;
                    if ui.checkbox(&mut want_fast, "As fast as possible").changed() {
                        *fast = want_fast;
                        if want_fast {
                            feeder.control.set_speed(FAST_SPEED);
                        } else {
                            feeder.control.set_speed(*last_finite_speed);
                        }
                    }

                    ui.add_enabled_ui(!*fast, |ui| {
                        let mut speed = *last_finite_speed;
                        let slider = egui::Slider::new(&mut speed, SPEED_MIN..=SPEED_MAX)
                            .logarithmic(true)
                            .text("speed ×");
                        if ui.add(slider).changed() {
                            *last_finite_speed = speed;
                            feeder.control.set_speed(speed);
                        }
                    });
                });
            }
            Source::Live { feeder } => {
                // Live: no pacing/scrubbing/restart — show a recording status line.
                let seq = state
                    .current_seq
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "—".to_string());
                ui.colored_label(
                    egui::Color32::LIGHT_RED,
                    format!(
                        "● LIVE — recording to {} · {} events · seq {seq}",
                        feeder.out_dir.display(),
                        state.message_count,
                    ),
                );
            }
        }
        if restarted {
            self.selected_asset = None;
        }

        ui.horizontal(|ui| {
            let sim_t = match (state.start_ts, state.current_ts) {
                (Some(start), Some(now)) => {
                    format!("{:.1}s", (now - start).num_milliseconds() as f64 / 1000.0)
                }
                _ => "—".to_string(),
            };
            let seq = state
                .current_seq
                .map(|s| s.to_string())
                .unwrap_or_else(|| "—".to_string());
            ui.label(format!("sim time: {sim_t}"));
            ui.separator();
            ui.label(format!("seq: {seq}"));
            ui.separator();
            ui.label(format!("messages: {}", state.message_count));
        });
    }

    fn asset_selector_ui(&mut self, ui: &mut egui::Ui, state: &ReplayState) {
        let assets = state.asset_ids();
        if assets.is_empty() {
            ui.label("waiting for data…");
            return;
        }
        // Default to the first asset once data arrives.
        if self.selected_asset.is_none() {
            self.selected_asset = assets.first().cloned();
        }
        ui.horizontal(|ui| {
            ui.label("asset:");
            for asset in &assets {
                let selected = self.selected_asset.as_ref() == Some(asset);
                if ui
                    .selectable_label(selected, state.label_for(asset))
                    .clicked()
                {
                    self.selected_asset = Some(asset.clone());
                }
            }
        });
    }

    fn price_plot_ui(&self, ui: &mut egui::Ui, state: &ReplayState) {
        let Some(asset) = self.selected_asset.as_ref() else {
            return;
        };
        let Some(a) = state.assets.get(asset) else {
            return;
        };

        ui.label(egui::RichText::new(state.label_for(asset)).strong());

        let bid: Vec<[f64; 2]> = a
            .book_series
            .iter()
            .map(|p| [p.t_secs, p.best_bid])
            .collect();
        let ask: Vec<[f64; 2]> = a
            .book_series
            .iter()
            .map(|p| [p.t_secs, p.best_ask])
            .collect();
        let mid: Vec<[f64; 2]> = a.book_series.iter().map(|p| [p.t_secs, p.mid]).collect();
        let trades: Vec<[f64; 2]> = a.trades.iter().map(|p| [p.t_secs, p.price]).collect();

        Plot::new("price_plot")
            .height(280.0)
            .legend(egui_plot::Legend::default())
            .x_axis_label("t (s)")
            .y_axis_label("price")
            .show(ui, |plot_ui| {
                plot_ui.line(Line::new("best bid", bid));
                plot_ui.line(Line::new("best ask", ask));
                plot_ui.line(Line::new("mid", mid));
                plot_ui.points(
                    Points::new("trades", trades)
                        .radius(3.0)
                        .shape(MarkerShape::Diamond),
                );
            });
    }

    fn depth_ui(&self, ui: &mut egui::Ui, state: &ReplayState) {
        let Some(asset) = self.selected_asset.as_ref() else {
            return;
        };
        let Some(a) = state.assets.get(asset) else {
            return;
        };

        ui.label(egui::RichText::new("current depth").strong());
        egui::Grid::new("depth_grid").striped(true).show(ui, |ui| {
            ui.label("side");
            ui.label("price");
            ui.label("size");
            ui.end_row();
            // Asks high→low above bids, then bids.
            for (price, size) in a.depth_asks.iter().rev() {
                ui.colored_label(egui::Color32::LIGHT_RED, "ask");
                ui.label(format!("{price:.3}"));
                ui.label(format!("{size:.0}"));
                ui.end_row();
            }
            for (price, size) in &a.depth_bids {
                ui.colored_label(egui::Color32::LIGHT_GREEN, "bid");
                ui.label(format!("{price:.3}"));
                ui.label(format!("{size:.0}"));
                ui.end_row();
            }
        });
    }
}

impl eframe::App for ReplayApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        let state = self.snapshot();

        let heading = match self.source {
            Source::Replay { .. } => "otelma replayer",
            Source::Live { .. } => "otelma live monitor",
        };
        egui::Panel::top("controls").show(ui, |ui| {
            ui.heading(heading);
            self.controls_ui(ui, &state);
            self.asset_selector_ui(ui, &state);
        });

        egui::Panel::right("depth").show(ui, |ui| {
            self.depth_ui(ui, &state);
        });

        egui::CentralPanel::default().show(ui, |ui| {
            self.price_plot_ui(ui, &state);
        });

        // Keep animating while replaying.
        ctx.request_repaint_after(std::time::Duration::from_millis(33));
    }

    fn on_exit(&mut self) {
        self.source.stop_and_join();
    }
}
