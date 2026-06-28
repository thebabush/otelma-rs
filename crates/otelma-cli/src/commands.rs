//! Subcommand implementations. The core of each command is a plain function so
//! it is testable without going through `clap`/`main`.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use otelma::{compact_session, drive, drive_realtime, PlaybackControl, Recorder, SessionReader};
use otelma_polymarket::{PolyEvent, PolymarketClient};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::summary::{render_line, SummarySink};

/// Default session directory for `record`: `recordings/<UTC timestamp>/`.
pub fn default_session_dir(now: chrono::DateTime<chrono::Utc>) -> PathBuf {
    PathBuf::from("recordings").join(now.format("%Y%m%dT%H%M%SZ").to_string())
}

/// Live-capture Polymarket for `asset_ids` into `out_dir` until Ctrl+C.
///
/// Returns the message count written. The `Recorder` is sync and writes Parquet
/// on hour-roll/close — done inline on the async task, which briefly blocks
/// ~once/hour; acceptable for this tool.
pub async fn run_record(asset_ids: Vec<String>, out_dir: PathBuf) -> Result<u64> {
    std::fs::create_dir_all(&out_dir)
        .with_context(|| format!("creating session dir {}", out_dir.display()))?;
    tracing::info!(dir = %out_dir.display(), assets = asset_ids.len(), "recording");

    let mut recorder = Recorder::new(&out_dir).context("opening recorder")?;
    let (tx, mut rx) = mpsc::channel::<otelma::Message<PolyEvent>>(1024);
    let shutdown = CancellationToken::new();

    let client = PolymarketClient::new(asset_ids);
    let client_shutdown = shutdown.clone();
    let client_task = tokio::spawn(async move { client.run(tx, client_shutdown).await });

    // Trigger shutdown on Ctrl+C.
    let signal_shutdown = shutdown.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::info!("ctrl-c; shutting down");
            signal_shutdown.cancel();
        }
    });

    let mut count: u64 = 0;
    while let Some(msg) = rx.recv().await {
        recorder.record(&msg).context("recording message")?;
        count += 1;
    }
    recorder.close().context("flushing recorder")?;

    // Surface any client error (channel-closed is the only Err it returns).
    match client_task.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(error = %e, "client ended with error"),
        Err(e) => tracing::warn!(error = %e, "client task panicked"),
    }
    Ok(count)
}

/// Replay a session through a [`SummarySink`], returning the rendered report.
///
/// `speed = None` → headless [`drive`] (as fast as possible). `Some(control)` →
/// paced [`drive_realtime`] using the caller's [`PlaybackControl`], so the
/// caller (e.g. a Ctrl+C handler) can `control.stop()` to abort. `print` echoes
/// each applied message as it streams.
pub fn run_replay(
    session_dir: impl AsRef<Path>,
    control: Option<&PlaybackControl>,
    print: bool,
) -> Result<String> {
    let reader = SessionReader::<PolyEvent>::open(&session_dir)
        .with_context(|| format!("opening session {}", session_dir.as_ref().display()))?;

    let mut sink = SummarySink::new();

    // Wrap the reader to optionally echo each message as it streams.
    let printing = reader.inspect(move |item| {
        if print {
            if let Ok(msg) = item {
                println!("{}", render_line(msg));
            }
        }
    });

    match control {
        None => drive(printing, &mut sink).context("replay (headless)")?,
        Some(control) => drive_realtime(printing, &mut sink, control).context("replay (paced)")?,
    }

    Ok(sink.render())
}

/// Compact a session's rolled parts into a single Parquet file. Returns the
/// output path. Thin wrapper over [`otelma::compact_session`] so the command is
/// testable and the default-output policy lives in one place.
pub fn run_compact(session_dir: impl AsRef<Path>, out: Option<PathBuf>) -> Result<PathBuf> {
    let session_dir = session_dir.as_ref();
    let out = out.unwrap_or_else(|| session_dir.join("compacted.parquet"));
    compact_session(session_dir, &out)
        .with_context(|| format!("compacting {}", session_dir.display()))?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use otelma::{Message, Payload};
    use otelma_polymarket::{BookUpdate, Level, Price, Size};
    use rust_decimal_macros::dec;
    use tempfile::tempdir;

    fn lvl(p: rust_decimal::Decimal, s: rust_decimal::Decimal) -> Level {
        Level {
            price: Price::new(p).expect("non-negative"),
            size: Size::new(s).expect("non-negative"),
        }
    }

    fn book_msg(seq: u64, secs: i64, asset: &str) -> Message<PolyEvent> {
        Message::new(
            seq,
            Utc.timestamp_opt(secs, 0).single().expect("ts"),
            PolyEvent::Book(BookUpdate {
                asset_id: asset.into(),
                bids: vec![lvl(dec!(0.5), dec!(1))],
                asks: vec![lvl(dec!(0.6), dec!(1))],
                market: None,
                exchange_ts_millis: None,
            }),
        )
    }

    #[test]
    fn compact_round_trips_two_hour_session() {
        let dir = tempdir().expect("tempdir");
        // Two UTC hours → two parts.
        let original = vec![
            book_msg(0, 36_000, "A"), // 10:00:00
            book_msg(1, 37_800, "A"), // 10:30:00
            book_msg(2, 39_600, "B"), // 11:00:00
            book_msg(3, 40_500, "B"), // 11:15:00
        ];
        let mut rec = Recorder::new(dir.path()).expect("rec");
        for m in &original {
            rec.record(m).expect("record");
        }
        rec.close().expect("close");
        // Sanity: at least two parts exist.
        assert!(otelma::part_paths(dir.path()).expect("paths").len() >= 2);

        let out = dir.path().join("compacted.parquet");
        let written = run_compact(dir.path(), Some(out.clone())).expect("compact");
        assert_eq!(written, out);

        // Read the single compacted file back via a fresh session dir.
        let single_dir = tempdir().expect("tempdir2");
        std::fs::copy(&out, single_dir.path().join("part-0000.parquet")).expect("copy");
        let read: Vec<Message<PolyEvent>> = SessionReader::<PolyEvent>::open(single_dir.path())
            .expect("open")
            .collect::<Result<_, _>>()
            .expect("read");

        assert_eq!(read, original);
    }

    #[test]
    fn replay_headless_produces_summary() {
        let dir = tempdir().expect("tempdir");
        let mut rec = Recorder::new(dir.path()).expect("rec");
        for m in [book_msg(0, 36_000, "A"), book_msg(1, 36_060, "A")] {
            rec.record(&m).expect("record");
        }
        rec.close().expect("close");

        let report = run_replay(dir.path(), None, false).expect("replay");
        assert!(report.contains("messages: 2"));
        assert!(report.contains("Book"));
        // Confirms the payload type tag wiring matches the engine's.
        assert_eq!(book_msg(0, 0, "A").payload.type_name(), "Book");
    }
}
