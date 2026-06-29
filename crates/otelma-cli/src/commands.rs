//! Subcommand implementations. The core of each command is a plain function so
//! it is testable without going through `clap`/`main`.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use otelma::{compact_session, drive, drive_realtime, PlaybackControl, Recorder, SessionReader};
use otelma_polymarket::{
    resolve_event, resolve_market, MarketMeta, PolyEvent, PolymarketClient, Resolution,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::summary::{render_line, SummarySink};

/// Message-time seconds between `record` progress logs (paced by the recorded
/// stream's own timestamps, not the wall clock).
const STATUS_INTERVAL_SECS: i64 = 5;

/// Default session directory for `record`: `recordings/<UTC timestamp>/`.
pub fn default_session_dir(now: chrono::DateTime<chrono::Utc>) -> PathBuf {
    PathBuf::from("recordings").join(now.format("%Y%m%dT%H%M%SZ").to_string())
}

/// What to record: the deterministic, deduplicated token-id subscription set
/// plus the collected per-market metadata to embed at recording start.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedSubscription {
    /// Sorted, deduplicated token ids to subscribe to.
    pub token_ids: Vec<String>,
    /// Per-market metadata, in the resolvers' deterministic order. Raw
    /// `--asset-id`s contribute tokens but no metadata.
    pub markets: Vec<MarketMeta>,
}

/// Merge resolved event/market resolutions with raw asset ids into one sorted,
/// deduplicated token-id list plus the collected market metadata, and report the
/// total skipped-closed count.
///
/// Pure: takes already-fetched [`Resolution`]s plus the raw `--asset-id` values,
/// so the merge + deterministic dedup is unit-testable without the network. The
/// returned token `Vec` is sorted (`BTreeSet`-derived), never hash-iterated; the
/// `markets` preserve each resolution's deterministic (sorted) order.
fn merge_token_ids(
    resolutions: &[Resolution],
    raw_asset_ids: &[String],
) -> (ResolvedSubscription, usize) {
    let mut set: BTreeSet<String> = BTreeSet::new();
    let mut markets: Vec<MarketMeta> = Vec::new();
    let mut seen_markets: BTreeSet<String> = BTreeSet::new();
    let mut skipped_closed = 0usize;
    for r in resolutions {
        skipped_closed += r.skipped_closed;
        for id in &r.token_ids {
            set.insert(id.to_string());
        }
        // Dedup metadata too: a market requested via both --event and --market
        // resolves twice. Keyed on the yes-token id (which is unique per market),
        // keep the first occurrence — preserving each resolution's sorted order —
        // so we never record the same PolyEvent::Market twice.
        for m in &r.markets {
            if seen_markets.insert(m.yes_asset_id.to_string()) {
                markets.push(m.clone());
            }
        }
    }
    for id in raw_asset_ids {
        set.insert(id.clone());
    }
    (
        ResolvedSubscription {
            token_ids: set.into_iter().collect(),
            markets,
        },
        skipped_closed,
    )
}

/// Resolve `--event` / `--market` references (via the Gamma API at `base`) and
/// merge with raw `--asset-id`s into a sorted, deduplicated token-id list.
///
/// Errors if no references of any kind are given (clap can't express
/// "at-least-one-of" across the three flags). Logs how many markets were skipped
/// as closed and the final token count — no silent caps.
pub async fn resolve_asset_ids(
    base: &str,
    events: &[String],
    markets: &[String],
    raw_asset_ids: &[String],
    include_closed: bool,
) -> Result<ResolvedSubscription> {
    if events.is_empty() && markets.is_empty() && raw_asset_ids.is_empty() {
        bail!("nothing to record: pass at least one of --event, --market, or --asset-id");
    }

    let mut resolutions = Vec::new();
    for ev in events {
        let r = resolve_event(base, ev, include_closed)
            .await
            .with_context(|| format!("resolving event {ev:?}"))?;
        tracing::info!(event = %ev, tokens = r.token_ids.len(), markets = r.markets.len(), skipped_closed = r.skipped_closed, "resolved event");
        resolutions.push(r);
    }
    for mk in markets {
        let r = resolve_market(base, mk, include_closed)
            .await
            .with_context(|| format!("resolving market {mk:?}"))?;
        tracing::info!(market = %mk, tokens = r.token_ids.len(), markets = r.markets.len(), skipped_closed = r.skipped_closed, "resolved market");
        resolutions.push(r);
    }

    let (sub, skipped_closed) = merge_token_ids(&resolutions, raw_asset_ids);
    if sub.token_ids.is_empty() {
        bail!("resolved zero token ids — every matched market was closed or had malformed tokens (try --include-closed for closed ones)");
    }
    tracing::info!(
        tokens = sub.token_ids.len(),
        markets = sub.markets.len(),
        skipped_closed,
        "resolved subscription set"
    );
    Ok(sub)
}

/// Live-capture Polymarket for `asset_ids` into `out_dir` until Ctrl+C.
///
/// Returns the message count written. The `Recorder` is sync and writes Parquet
/// on hour-roll/close — done inline on the async task, which briefly blocks
/// ~once/hour; acceptable for this tool.
pub async fn run_record(
    asset_ids: Vec<String>,
    markets: Vec<MarketMeta>,
    out_dir: PathBuf,
) -> Result<u64> {
    std::fs::create_dir_all(&out_dir)
        .with_context(|| format!("creating session dir {}", out_dir.display()))?;
    tracing::info!(dir = %out_dir.display(), assets = asset_ids.len(), markets = markets.len(), "recording");

    let mut recorder = Recorder::new(&out_dir).context("opening recorder")?;
    let (tx, mut rx) = mpsc::channel::<otelma::Message<PolyEvent>>(1024);
    let shutdown = CancellationToken::new();

    let client = PolymarketClient::new(asset_ids).with_markets(markets);
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

    // Periodic progress, paced by *recorded message time* — never the wall clock:
    // log each time the stream's own timestamps cross another STATUS_INTERVAL.
    // (Caveat: a connected-but-silent venue advances no message time, so it logs
    // nothing until the next event; a disconnect still surfaces via the client's
    // Connection marker, which is itself a message.)
    let mut count: u64 = 0;
    let mut last_reported = 0u64;
    let mut next_report: Option<chrono::DateTime<chrono::Utc>> = None;
    let interval = chrono::TimeDelta::seconds(STATUS_INTERVAL_SECS);
    while let Some(msg) = rx.recv().await {
        let ts = msg.timestamp;
        recorder.record(&msg).context("recording message")?;
        count += 1;
        match next_report {
            None => next_report = Some(ts + interval),
            Some(due) if ts >= due => {
                let delta = count - last_reported;
                last_reported = count;
                tracing::info!(events = count, last_interval = delta, "recording");
                next_report = Some(ts + interval);
            }
            _ => {}
        }
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
    use otelma_polymarket::{testing::lvl, BookUpdate};
    use rust_decimal_macros::dec;
    use tempfile::tempdir;

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

    fn res(ids: &[&str], skipped: usize) -> Resolution {
        Resolution {
            token_ids: ids.iter().map(|s| (*s).into()).collect(),
            markets: Vec::new(),
            skipped_closed: skipped,
        }
    }

    fn res_with_markets(ids: &[&str], markets: Vec<MarketMeta>, skipped: usize) -> Resolution {
        Resolution {
            token_ids: ids.iter().map(|s| (*s).into()).collect(),
            markets,
            skipped_closed: skipped,
        }
    }

    #[test]
    fn merge_dedups_and_sorts_across_sources() {
        let resolutions = vec![res(&["500", "100"], 1), res(&["300", "100"], 2)];
        let raw = vec!["100".to_string(), "999".to_string()];
        let (sub, skipped) = merge_token_ids(&resolutions, &raw);
        assert_eq!(sub.token_ids, vec!["100", "300", "500", "999"]);
        assert!(sub.markets.is_empty());
        assert_eq!(skipped, 3);
    }

    #[test]
    fn merge_with_only_raw_asset_ids() {
        let (sub, skipped) = merge_token_ids(&[], &["b".to_string(), "a".to_string()]);
        assert_eq!(sub.token_ids, vec!["a", "b"]);
        // Raw asset ids carry no metadata.
        assert!(sub.markets.is_empty());
        assert_eq!(skipped, 0);
    }

    #[test]
    fn merge_collects_market_metadata_across_resolutions() {
        use otelma_polymarket::testing::market_meta;
        let resolutions = vec![
            res_with_markets(
                &["500", "100"],
                vec![market_meta("Argentina", "500", "100", Some("World Cup"))],
                0,
            ),
            res_with_markets(
                &["300", "200"],
                vec![market_meta("Brazil", "300", "200", Some("World Cup"))],
                0,
            ),
        ];
        let (sub, _) = merge_token_ids(&resolutions, &["raw".to_string()]);
        assert_eq!(sub.token_ids, vec!["100", "200", "300", "500", "raw"]);
        assert_eq!(
            sub.markets
                .iter()
                .map(|m| m.outcome_title.as_str())
                .collect::<Vec<_>>(),
            vec!["Argentina", "Brazil"]
        );
    }

    #[test]
    fn merge_dedups_market_metadata_across_resolutions() {
        use otelma_polymarket::testing::market_meta;
        // The same market resolved twice (e.g. via both --event and --market):
        // same yes-token id → its metadata is recorded exactly once.
        let arg = market_meta("Argentina", "500", "100", Some("World Cup"));
        let resolutions = vec![
            res_with_markets(&["500", "100"], vec![arg.clone()], 0),
            res_with_markets(&["500", "100"], vec![arg.clone()], 0),
        ];
        let (sub, _) = merge_token_ids(&resolutions, &[]);
        assert_eq!(sub.token_ids, vec!["100", "500"]);
        assert_eq!(sub.markets.len(), 1, "duplicate market metadata deduped");
        assert_eq!(sub.markets[0].outcome_title, "Argentina");
    }

    #[tokio::test]
    async fn resolve_asset_ids_errors_when_nothing_given() {
        let err = resolve_asset_ids("http://unused", &[], &[], &[], false)
            .await
            .expect_err("should require at least one ref");
        assert!(err.to_string().contains("at least one"));
    }

    #[tokio::test]
    async fn resolve_asset_ids_passes_through_raw_only_without_network() {
        // No event/market refs → no HTTP; raw ids flow straight through, sorted.
        let sub = resolve_asset_ids("http://unused", &[], &[], &["z".into(), "a".into()], false)
            .await
            .expect("raw-only");
        assert_eq!(sub.token_ids, vec!["a", "z"]);
        assert!(sub.markets.is_empty());
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
        std::fs::copy(&out, single_dir.path().join("20260101T100000Z.parquet")).expect("copy");
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
