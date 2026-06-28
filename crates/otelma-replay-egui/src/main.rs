//! `otelma-replay-egui` — a desktop replayer that plots a recorded otelma
//! session as it plays.
//!
//! ```text
//! otelma-replay-egui <SESSION_DIR>
//! ```
//!
//! `SESSION_DIR` must be a real recording produced by `otelma record`. The
//! replayer only ever replays recorded data — it never fabricates a session.

mod app;
mod feeder;
mod state;

use std::path::PathBuf;
use std::process::ExitCode;

use app::ReplayApp;
use feeder::Feeder;

/// Initial playback speed (real-time multiplier).
const INITIAL_SPEED: f64 = 60.0;

fn main() -> ExitCode {
    let Some(session_dir) = std::env::args().nth(1).map(PathBuf::from) else {
        eprintln!("usage: otelma-replay-egui <SESSION_DIR>");
        eprintln!("  SESSION_DIR is a recording produced by `otelma record`.");
        return ExitCode::FAILURE;
    };

    let feeder = Feeder::start(session_dir, INITIAL_SPEED);

    let options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default()
            .with_inner_size([960.0, 640.0])
            .with_title("otelma replayer"),
        ..Default::default()
    };

    match eframe::run_native(
        "otelma replayer",
        options,
        Box::new(|_cc| Ok(Box::new(ReplayApp::new(feeder)))),
    ) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("otelma-replay-egui: {e}");
            ExitCode::FAILURE
        }
    }
}
