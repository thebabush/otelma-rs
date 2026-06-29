//! Live feeder: runs the Polymarket WS client itself and **tees** every message
//! to both a [`Recorder`] (rolled Parquet on disk) and the on-screen
//! [`GuiSink`], so you watch the market live while it's being captured.
//!
//! This mirrors [`crate::feeder::Feeder`]'s ownership shape (`state` +
//! stop/join + `Drop`), but there is NO pacing and NO [`otelma::PlaybackControl`]:
//! live data already arrives in real time. The only wall-clock reader stays the
//! client's `Stamper`; the sink reads only message contents, so the on-disk
//! recording replays later to identical state.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use otelma::{Message, Recorder, Sink};
use otelma_polymarket::{resolve_subscription, PolyEvent, PolymarketClient, DEFAULT_GAMMA_BASE};
use tokio::sync::mpsc::{self, Receiver};
use tokio_util::sync::CancellationToken;

use crate::state::{GuiSink, ReplayState};

/// What to subscribe to in live mode (the parsed selectors).
#[derive(Debug, Clone)]
pub struct LiveSelectors {
    /// `--event` references (slug or URL). Repeatable.
    pub events: Vec<String>,
    /// `--market` references (slug or URL). Repeatable.
    pub markets: Vec<String>,
    /// Raw `--asset-id` token ids. Repeatable.
    pub asset_ids: Vec<String>,
    /// Also subscribe to closed/eliminated markets.
    pub include_closed: bool,
}

/// Owns the live-capture thread (which owns a tokio runtime) and the state it
/// writes into.
pub struct LiveFeeder {
    pub state: Arc<Mutex<ReplayState>>,
    /// The directory recordings are being written to (for the status line).
    pub out_dir: PathBuf,
    shutdown: CancellationToken,
    handle: Option<JoinHandle<()>>,
}

impl LiveFeeder {
    /// Start live capture for `selectors`, writing to `out_dir` (defaulted by the
    /// caller). Spawns ONE std thread that owns a multi-thread tokio runtime;
    /// inside it resolves the subscription, opens the recorder, runs the client,
    /// and tees each message to the recorder + the [`GuiSink`].
    pub fn start(selectors: LiveSelectors, out_dir: PathBuf) -> Self {
        let state = Arc::new(Mutex::new(ReplayState::default()));
        let shutdown = CancellationToken::new();

        let thread_state = Arc::clone(&state);
        let thread_shutdown = shutdown.clone();
        let thread_out = out_dir.clone();
        let handle = std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("live: failed to build tokio runtime: {e}");
                    return;
                }
            };
            rt.block_on(run_live(
                selectors,
                thread_out,
                &thread_state,
                thread_shutdown,
            ));
        });

        Self {
            state,
            out_dir,
            shutdown,
            handle: Some(handle),
        }
    }

    /// Cancel the client and join the capture thread (which flushes the recorder
    /// on the way out).
    pub fn stop_and_join(&mut self) {
        self.shutdown.cancel();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for LiveFeeder {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

/// The async body run inside the capture thread's runtime: resolve, open the
/// recorder, spawn the client, and tee. Errors are logged (the thread can't
/// propagate), never panicked.
async fn run_live(
    selectors: LiveSelectors,
    out_dir: PathBuf,
    state: &Arc<Mutex<ReplayState>>,
    shutdown: CancellationToken,
) {
    let sub = match resolve_subscription(
        DEFAULT_GAMMA_BASE,
        &selectors.events,
        &selectors.markets,
        &selectors.asset_ids,
        selectors.include_closed,
    )
    .await
    {
        Ok(sub) => sub,
        Err(e) => {
            tracing::error!(error = %e, "live: failed to resolve subscription");
            return;
        }
    };

    if let Err(e) = std::fs::create_dir_all(&out_dir) {
        tracing::error!(error = %e, dir = %out_dir.display(), "live: creating session dir");
        return;
    }
    let mut recorder = match Recorder::new(&out_dir) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, dir = %out_dir.display(), "live: opening recorder");
            return;
        }
    };

    tracing::info!(
        tokens = sub.token_ids.len(),
        markets = sub.markets.len(),
        dir = %out_dir.display(),
        "live: capturing"
    );

    let (tx, rx) = mpsc::channel::<Message<PolyEvent>>(1024);
    let client = PolymarketClient::new(sub.token_ids).with_markets(sub.markets);
    let client_shutdown = shutdown.clone();
    let client_task = tokio::spawn(async move { client.run(tx, client_shutdown).await });

    // Tee consumer: records + displays each message until the client drops `tx`.
    match record_and_display(rx, &mut recorder, state).await {
        Ok(count) => tracing::info!(messages = count, "live: capture stopped"),
        Err(e) => tracing::error!(error = %e, "live: recording error"),
    }

    // The consumer loop ended → flush the recorder.
    if let Err(e) = recorder.close() {
        tracing::error!(error = %e, "live: flushing recorder");
    }

    match client_task.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(error = %e, "live: client ended with error"),
        Err(e) => tracing::error!(error = %e, "live: client task panicked"),
    }
}

/// Tee consumer: for each message, record it to `recorder`, then apply it to the
/// shared [`ReplayState`] via [`GuiSink`] under a short-lived lock. Returns the
/// number of messages handled when the channel closes.
///
/// Takes a plain [`Receiver`] (NOT the WS client), so it is unit-testable by
/// sending hand-built messages over a tempdir `Recorder` + a fresh state.
async fn record_and_display(
    mut rx: Receiver<Message<PolyEvent>>,
    recorder: &mut Recorder,
    state: &Arc<Mutex<ReplayState>>,
) -> Result<u64, otelma::Error> {
    let mut count = 0u64;
    while let Some(msg) = rx.recv().await {
        recorder.record(&msg)?;
        count += 1;
        // Poisoned lock means the GUI thread panicked; nothing useful to do.
        // Live: the displayed series are bounded to the trailing window.
        if let Ok(mut s) = state.lock() {
            GuiSink::new_live(&mut s).apply(&msg);
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use otelma::SessionReader;
    use otelma_polymarket::testing::{book_msg, lvl, market_meta, market_meta_msg};
    use otelma_polymarket::AssetId;
    use rust_decimal_macros::dec;
    use tempfile::tempdir;

    #[tokio::test]
    async fn record_and_display_tees_to_state_and_disk() {
        let dir = tempdir().expect("tempdir");
        let mut recorder = Recorder::new(dir.path()).expect("recorder");
        let state = Arc::new(Mutex::new(ReplayState::default()));

        let (tx, rx) = mpsc::channel::<Message<PolyEvent>>(8);

        // A Market (label) then a Book (series) — hand-built, no WS client.
        let market = market_meta_msg(
            0,
            0,
            market_meta("Argentina", "yes-arg", "no-arg", Some("World Cup")),
        );
        let book = book_msg(
            1,
            60,
            "yes-arg",
            vec![lvl(dec!(0.50), dec!(10))],
            vec![lvl(dec!(0.55), dec!(8))],
        );
        let original = vec![market.clone(), book.clone()];
        for m in &original {
            tx.send(m.clone()).await.expect("send");
        }
        drop(tx); // closes the channel so the consumer loop ends

        let count = record_and_display(rx, &mut recorder, &state)
            .await
            .expect("record_and_display");
        assert_eq!(count, 2);
        recorder.close().expect("close");

        // (a) State got the label + the book series.
        {
            let s = state.lock().expect("lock");
            assert_eq!(s.message_count, 2);
            assert_eq!(
                s.label_for(&AssetId::from("yes-arg")),
                "World Cup · Argentina · Yes"
            );
            let a = &s.assets[&AssetId::from("yes-arg")];
            assert_eq!(a.book_series.len(), 1);
            assert_eq!(a.book_series[0].best_bid, 0.50);
            assert_eq!(a.book_series[0].best_ask, 0.55);
        }

        // (b) The recorded session round-trips via SessionReader.
        let read: Vec<Message<PolyEvent>> = SessionReader::<PolyEvent>::open(dir.path())
            .expect("open")
            .collect::<Result<_, _>>()
            .expect("read");
        assert_eq!(read, original);
    }
}
