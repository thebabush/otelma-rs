//! `otelma-replay-egui` — a desktop replayer that plots an otelma session as it
//! plays, and (with `--live`) a live recorder+monitor that captures Polymarket
//! market data to disk while you watch it.
//!
//! ```text
//! otelma-replay-egui <SESSION_DIR>                            # replay from disk
//! otelma-replay-egui --live --event <slug|url> [--out <DIR>]  # live capture + monitor
//! otelma-replay-egui --live --market <slug|url>
//! otelma-replay-egui --live --asset-id <TOKEN> [--asset-id …]
//! ```
//!
//! In replay mode, `SESSION_DIR` must be a real recording produced by `otelma
//! record`; the replayer never fabricates a session. In `--live` mode it runs
//! the Polymarket WS client itself and **tees** every message to a `Recorder`
//! (rolled Parquet on disk) and the on-screen view — so it captures real data,
//! it never fabricates it.

mod app;
mod feeder;
mod live;
mod series;
mod state;
mod theme;
mod ui;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

use app::ReplayApp;
use feeder::Feeder;
use live::{LiveFeeder, LiveSelectors};

/// Initial playback speed (real-time multiplier) for replay mode.
const INITIAL_SPEED: f64 = 60.0;

/// Replay a recorded otelma session, or (with `--live`) live-capture Polymarket
/// while monitoring it.
#[derive(Debug, Parser)]
#[command(name = "otelma-replay-egui", version, about)]
struct Cli {
    /// Session directory of a recording produced by `otelma record` (replay
    /// mode). Mutually exclusive with `--live`.
    session_dir: Option<PathBuf>,

    /// Live mode: run the Polymarket client and record while monitoring. Requires
    /// at least one of `--event` / `--market` / `--asset-id`.
    #[arg(long)]
    live: bool,

    /// Polymarket event to capture (bare slug or `polymarket.com/event/<slug>`
    /// URL). Repeatable. Live mode only.
    #[arg(long = "event")]
    event: Vec<String>,

    /// Polymarket market to capture (bare slug or URL with `marketSlug=`).
    /// Repeatable. Live mode only.
    #[arg(long = "market")]
    market: Vec<String>,

    /// Raw Polymarket asset (token) id to subscribe to. Repeatable. Live mode only.
    #[arg(long = "asset-id")]
    asset_id: Vec<String>,

    /// Also capture closed/eliminated markets (default: live markets only).
    #[arg(long = "include-closed")]
    include_closed: bool,

    /// Output session directory for live capture. Defaults to
    /// `recordings/<UTC timestamp>/`.
    #[arg(long)]
    out: Option<PathBuf>,
}

/// What the validated CLI resolves to.
#[derive(Debug)]
enum Mode {
    Replay(PathBuf),
    Live(LiveSelectors, PathBuf),
}

impl Cli {
    /// Validate the flag combination into a [`Mode`], or return a usage error.
    fn into_mode(self) -> Result<Mode, String> {
        if self.live {
            if self.session_dir.is_some() {
                return Err("--live takes selectors, not a SESSION_DIR positional".to_string());
            }
            if self.event.is_empty() && self.market.is_empty() && self.asset_id.is_empty() {
                return Err(
                    "--live requires at least one of --event, --market, or --asset-id".to_string(),
                );
            }
            let selectors = LiveSelectors {
                events: self.event,
                markets: self.market,
                asset_ids: self.asset_id,
                include_closed: self.include_closed,
            };
            let out_dir = self
                .out
                .unwrap_or_else(|| otelma::default_session_dir(chrono::Utc::now()));
            Ok(Mode::Live(selectors, out_dir))
        } else {
            // Replay mode: the live-only flags make no sense.
            if !self.event.is_empty() || !self.market.is_empty() || !self.asset_id.is_empty() {
                return Err("--event/--market/--asset-id are only valid with --live".to_string());
            }
            if self.out.is_some() {
                return Err("--out is only valid with --live".to_string());
            }
            match self.session_dir {
                Some(dir) => Ok(Mode::Replay(dir)),
                None => {
                    Err("provide a SESSION_DIR to replay, or --live with selectors".to_string())
                }
            }
        }
    }
}

fn main() -> ExitCode {
    // Surface the adapter's logs (reconnects, malformed-frame skips, a clock
    // backstep abort) — otherwise `--live` capture problems would be invisible.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let mode = match Cli::parse().into_mode() {
        Ok(mode) => mode,
        Err(e) => {
            eprintln!("otelma-replay-egui: {e}");
            return ExitCode::FAILURE;
        }
    };

    let (app, title) = match mode {
        Mode::Replay(session_dir) => {
            let feeder = Feeder::start(session_dir, INITIAL_SPEED);
            (ReplayApp::new(feeder), "otelma replayer")
        }
        Mode::Live(selectors, out_dir) => {
            let feeder = LiveFeeder::start(selectors, out_dir);
            (ReplayApp::new_live(feeder), "otelma live monitor")
        }
    };

    let options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default()
            .with_inner_size([960.0, 640.0])
            .with_title(title),
        ..Default::default()
    };

    match eframe::run_native(title, options, Box::new(|_cc| Ok(Box::new(app)))) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("otelma-replay-egui: {e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse argv (program name prepended) the way `main` does.
    fn parse(args: &[&str]) -> Result<Mode, String> {
        let mut argv = vec!["otelma-replay-egui"];
        argv.extend_from_slice(args);
        Cli::try_parse_from(argv)
            .map_err(|e| e.to_string())
            .and_then(Cli::into_mode)
    }

    #[test]
    fn bare_session_dir_is_replay() {
        match parse(&["recordings/sess"]).expect("replay") {
            Mode::Replay(dir) => assert_eq!(dir, PathBuf::from("recordings/sess")),
            Mode::Live(..) => panic!("expected replay"),
        }
    }

    #[test]
    fn live_with_asset_id_is_live_with_default_out() {
        match parse(&["--live", "--asset-id", "tok"]).expect("live") {
            Mode::Live(sel, out) => {
                assert_eq!(sel.asset_ids, vec!["tok"]);
                assert!(out.starts_with("recordings"));
            }
            Mode::Replay(_) => panic!("expected live"),
        }
    }

    #[test]
    fn live_without_selectors_errors() {
        let err = parse(&["--live"]).expect_err("needs a selector");
        assert!(err.contains("at least one"));
    }

    #[test]
    fn live_with_positional_errors() {
        let err = parse(&["--live", "--asset-id", "t", "somedir"]).expect_err("no positional");
        assert!(err.contains("SESSION_DIR"));
    }

    #[test]
    fn replay_with_live_only_flags_errors() {
        let err = parse(&["somedir", "--asset-id", "t"]).expect_err("flags need --live");
        assert!(err.contains("only valid with --live"));
        let err = parse(&["somedir", "--out", "o"]).expect_err("out needs --live");
        assert!(err.contains("--out"));
    }

    #[test]
    fn nothing_at_all_errors() {
        let err = parse(&[]).expect_err("need a mode");
        assert!(err.contains("SESSION_DIR") || err.contains("--live"));
    }
}
