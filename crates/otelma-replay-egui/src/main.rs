//! `otelma-replay-egui` — a desktop replayer that plots a recorded otelma
//! session as it plays.
//!
//! ```text
//! otelma-replay-egui [SESSION_DIR]
//! ```
//!
//! With no argument it generates a deterministic synthetic demo session in a
//! temp dir and replays that, so `cargo run -p otelma-replay-egui` shows moving
//! plots with zero setup.

mod app;
mod demo;
mod feeder;
mod state;

use std::path::PathBuf;

use app::ReplayApp;
use feeder::Feeder;

/// Seed for the built-in demo session.
const DEMO_SEED: u64 = 0x0742;
/// Initial playback speed (real-time multiplier).
const INITIAL_SPEED: f64 = 60.0;

fn main() -> eframe::Result<()> {
    // Resolve the session: an explicit dir, or a generated demo in a temp dir.
    // The TempDir is kept alive for the process so the recording isn't deleted.
    let (session_dir, _keepalive) = match std::env::args().nth(1) {
        Some(dir) => (PathBuf::from(dir), None),
        None => {
            let tmp = tempfile::tempdir().expect("create temp dir for demo session");
            let count =
                demo::generate_demo_session(tmp.path(), DEMO_SEED).expect("generate demo session");
            eprintln!(
                "no session dir given; generated {count}-message demo at {}",
                tmp.path().display()
            );
            (tmp.path().to_path_buf(), Some(tmp))
        }
    };

    let feeder = Feeder::start(session_dir, INITIAL_SPEED);

    let options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default()
            .with_inner_size([960.0, 640.0])
            .with_title("otelma replayer"),
        ..Default::default()
    };

    eframe::run_native(
        "otelma replayer",
        options,
        Box::new(|_cc| Ok(Box::new(ReplayApp::new(feeder)))),
    )
}
