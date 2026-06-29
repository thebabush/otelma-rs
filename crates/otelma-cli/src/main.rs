//! `otelma` — command-line front door for the otelma record/replay engine.
//!
//! Three subcommands: `record` (live-capture Polymarket to rolled Parquet),
//! `replay` (drive a recorded session through a summary sink, headless or
//! paced), and `compact` (merge a session's parts into one Parquet file).

mod commands;
mod summary;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use otelma::PlaybackControl;
use std::sync::Arc;

/// Record, replay, and compact otelma market-data sessions.
#[derive(Debug, Parser)]
#[command(name = "otelma", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Live-capture Polymarket order books and trades to rolled Parquet parts.
    ///
    /// Specify what to record with any mix of `--event`, `--market`, and
    /// `--asset-id` (all repeatable). `--event`/`--market` accept a bare
    /// Polymarket slug OR a full `polymarket.com` URL and resolve to token ids
    /// via the Gamma REST API. At least one must be given.
    Record {
        /// Polymarket event to record (whole event → every live market's
        /// tokens). Bare slug or `polymarket.com/event/<slug>` URL. Repeatable.
        #[arg(long = "event")]
        event: Vec<String>,
        /// Polymarket market to record (one market → its 2 tokens). Bare slug,
        /// or a URL with a `marketSlug=` query param. Repeatable.
        #[arg(long = "market")]
        market: Vec<String>,
        /// Raw Polymarket asset (token) id to subscribe to. Repeatable.
        #[arg(long = "asset-id")]
        asset_id: Vec<String>,
        /// Also record closed/eliminated markets (default: live markets only).
        #[arg(long = "include-closed")]
        include_closed: bool,
        /// Output session directory. Defaults to `recordings/<UTC timestamp>/`.
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Replay a recorded session through a summary sink.
    Replay {
        /// Session directory of recorded part files (`YYYYMMDDTHHMMSSZ.parquet`).
        session_dir: PathBuf,
        /// Playback speed multiplier (real-time = 1.0, `inf` = as fast as
        /// possible). Omit for headless (fastest) replay.
        #[arg(long)]
        speed: Option<f64>,
        /// Print each message as it is applied (debug view).
        #[arg(long)]
        print: bool,
    },
    /// Merge a session's rolled parts into a single Parquet file.
    Compact {
        /// Session directory to compact.
        session_dir: PathBuf,
        /// Output file. Defaults to `<SESSION_DIR>/compacted.parquet`.
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Record {
            event,
            market,
            asset_id,
            include_closed,
            out,
        } => {
            let sub = commands::resolve_asset_ids(
                otelma_polymarket::DEFAULT_GAMMA_BASE,
                &event,
                &market,
                &asset_id,
                include_closed,
            )
            .await?;
            let out_dir = out.unwrap_or_else(|| commands::default_session_dir(chrono::Utc::now()));
            let count = commands::run_record(sub.token_ids, sub.markets, out_dir.clone()).await?;
            println!("recorded {count} messages → {}", out_dir.display());
        }
        Command::Replay {
            session_dir,
            speed,
            print,
        } => {
            let report = match speed {
                None => commands::run_replay(&session_dir, None, print)?,
                Some(speed) => {
                    // Share one control with the Ctrl+C handler so it can stop
                    // the paced replay promptly.
                    let control = Arc::new(PlaybackControl::new(speed));
                    let sig_control = Arc::clone(&control);
                    tokio::spawn(async move {
                        if tokio::signal::ctrl_c().await.is_ok() {
                            sig_control.stop();
                        }
                    });
                    // drive_realtime blocks (it sleeps); run it off the async
                    // runtime so the signal task can fire.
                    let dir = session_dir.clone();
                    let print_flag = print;
                    tokio::task::spawn_blocking(move || {
                        commands::run_replay(&dir, Some(&control), print_flag)
                    })
                    .await??
                }
            };
            print!("{report}");
        }
        Command::Compact { session_dir, out } => {
            let written = commands::run_compact(&session_dir, out)?;
            println!("compacted → {}", written.display());
        }
    }
    Ok(())
}
