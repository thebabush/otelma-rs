//! Background feeder: drives a `SessionReader` through a [`GuiSink`] on its own
//! thread, paced by a shared [`PlaybackControl`], writing into shared
//! [`ReplayState`].
//!
//! This mirrors the proven feeder/GUI split: pacing/sleeping happens here; the
//! sink only reads message contents. A [`Feeder`] can be restarted (re-open the
//! reader, reset state) by stopping the current thread and spawning a fresh one.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use chrono::{DateTime, Utc};
use otelma::{drive_realtime, PlaybackControl, SessionReader, Sink};
use otelma_polymarket::PolyEvent;

use crate::state::{GuiSink, ReplayState};

/// Owns the feeder thread and the state it writes into.
pub struct Feeder {
    pub session_dir: PathBuf,
    pub state: Arc<Mutex<ReplayState>>,
    pub control: Arc<PlaybackControl>,
    handle: Option<JoinHandle<()>>,
}

impl Feeder {
    /// Start feeding `session_dir` at `initial_speed`. Spawns the thread.
    pub fn start(session_dir: PathBuf, initial_speed: f64) -> Self {
        let state = Arc::new(Mutex::new(ReplayState::default()));
        let control = Arc::new(PlaybackControl::new(initial_speed));
        let mut feeder = Self {
            session_dir,
            state,
            control,
            handle: None,
        };
        feeder.spawn(None);
        feeder
    }

    /// Spawn a feeder thread for the current session into the current state.
    ///
    /// When `seek_to` is `Some(target)`, the thread first **fast-forwards**: it
    /// pulls messages from the reader and applies them to the sink with NO
    /// pacing until a message's timestamp `>= target`, then hands the *same*
    /// partially-consumed reader to [`drive_realtime`] for paced playback of the
    /// remainder. This keeps the seek out of core's `drive_realtime` (no engine
    /// change): the pre-roll uses the reader's `Iterator` directly. State is
    /// expected to be cleared by the caller, so the chart rebuilds `[0, target]`.
    fn spawn(&mut self, seek_to: Option<DateTime<Utc>>) {
        let session_dir = self.session_dir.clone();
        let state = Arc::clone(&self.state);
        let control = Arc::clone(&self.control);

        let handle = std::thread::spawn(move || {
            let mut reader = match SessionReader::<PolyEvent>::open(&session_dir) {
                Ok(reader) => reader,
                Err(e) => {
                    eprintln!("feeder: failed to open {}: {e}", session_dir.display());
                    return;
                }
            };

            // Apply each message under a short-lived lock so the GUI thread can
            // read a snapshot between messages; pacing sleeps happen inside
            // `drive_realtime`, never while holding the lock.
            let mut locking = LockingSink { state: &state };

            // Pre-roll: fast-forward (no pacing) up to the seek target, applying
            // each message so the chart rebuilds `[0, target]`. We consume the
            // reader by reference, then pass it on so paced playback continues
            // from exactly where the pre-roll left off — no core change needed.
            if let Some(target) = seek_to {
                for msg in reader.by_ref() {
                    let msg = match msg {
                        Ok(msg) => msg,
                        Err(e) => {
                            eprintln!("feeder: seek pre-roll error: {e}");
                            return;
                        }
                    };
                    let reached = msg.timestamp >= target;
                    locking.apply(&msg);
                    if reached {
                        break;
                    }
                    if control.should_stop() {
                        return;
                    }
                }
            }

            if let Err(e) = drive_realtime(reader, &mut locking, &control) {
                eprintln!("feeder: replay error: {e}");
            }
        });
        self.handle = Some(handle);
    }

    /// Restart from the beginning: stop the current thread, reset state and the
    /// control's stop flag, and spawn afresh. Preserves the current speed and
    /// pause state.
    pub fn restart(&mut self) {
        self.respawn(None);
    }

    /// Seek to `target` (recorded message time): like [`restart`](Self::restart),
    /// but the fresh feeder thread fast-forwards (un-paced) until a message's
    /// timestamp reaches `target` before resuming paced playback. Preserves the
    /// current speed and pause state, and rebuilds the chart over `[0, target]`.
    /// Deterministic: `target` is a recorded timestamp, never a wall-clock read.
    pub fn seek_to(&mut self, target: DateTime<Utc>) {
        self.respawn(Some(target));
    }

    /// Stop the current thread, build a fresh control carrying over speed/pause,
    /// clear the state, and spawn a new thread (optionally seeking).
    fn respawn(&mut self, seek_to: Option<DateTime<Utc>>) {
        self.stop_and_join();
        // The control's `stop` is irreversible by design, so build a fresh one
        // carrying over speed and pause.
        let speed = self.control.speed();
        let paused = self.control.is_paused();
        let fresh = PlaybackControl::new(speed);
        if paused {
            fresh.pause();
        }
        self.control = Arc::new(fresh);
        if let Ok(mut s) = self.state.lock() {
            s.clear();
        }
        self.spawn(seek_to);
    }

    /// Signal stop and join the feeder thread.
    pub fn stop_and_join(&mut self) {
        self.control.stop();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for Feeder {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

/// A sink adapter that applies each message under a short-lived lock on the
/// shared state, so the GUI thread can read between messages.
struct LockingSink<'a> {
    state: &'a Arc<Mutex<ReplayState>>,
}

impl otelma::Sink<PolyEvent> for LockingSink<'_> {
    fn apply(&mut self, msg: &otelma::Message<PolyEvent>) {
        // Poisoned lock means the GUI thread panicked; nothing useful to do.
        if let Ok(mut state) = self.state.lock() {
            GuiSink::new(&mut state).apply(msg);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otelma::{Message, Recorder};
    use otelma_polymarket::testing::book_msg;
    use std::time::{Duration, Instant};

    /// Wait until `cond` over a state snapshot holds, or panic after `timeout`.
    fn wait_until(
        feeder: &Feeder,
        timeout: Duration,
        cond: impl Fn(&ReplayState) -> bool,
    ) -> ReplayState {
        let start = Instant::now();
        loop {
            let snap = feeder.state.lock().expect("lock").clone();
            if cond(&snap) {
                return snap;
            }
            assert!(
                start.elapsed() < timeout,
                "condition not met within {timeout:?}"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Record a small session whose messages are seconds apart.
    fn record_session(dir: &std::path::Path) -> Vec<Message<PolyEvent>> {
        use rust_decimal_macros::dec;
        let msgs: Vec<Message<PolyEvent>> = (0..10)
            .map(|i| {
                book_msg(
                    i as u64,
                    i as i64 * 10, // 0s, 10s, 20s … apart
                    "A",
                    vec![lvl(dec!(0.50))],
                    vec![lvl(dec!(0.55))],
                )
            })
            .collect();
        let mut rec = Recorder::new(dir).expect("recorder");
        for m in &msgs {
            rec.record(m).expect("record");
        }
        rec.close().expect("close");
        msgs
    }

    /// One-level bid/ask helper (the testing builder takes price+size).
    fn lvl(price: rust_decimal::Decimal) -> otelma_polymarket::Level {
        otelma_polymarket::testing::lvl(price, rust_decimal_macros::dec!(1))
    }

    /// `seek_to` fast-forwards the pre-roll past the target (un-paced even while
    /// paused), rebuilding the chart over `[0, target]`, then paused playback
    /// holds there. The pre-roll runs without pacing, so it completes promptly.
    #[test]
    fn seek_fast_forwards_to_target_then_holds_when_paused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let msgs = record_session(dir.path());
        // base = first message timestamp (t=0). Target = 45s in → between msg 4
        // (40s) and msg 5 (50s); the pre-roll stops at the first ts >= target,
        // which is msg 5.
        let base = msgs[0].timestamp;
        let target = base + chrono::Duration::seconds(45);

        // Start paused at slow speed so post-seek paced playback can't advance.
        let mut feeder = Feeder::start(dir.path().to_path_buf(), 1.0);
        feeder.control.pause();
        feeder.seek_to(target);

        // The pre-roll applies msgs 0..=5 (six messages), reaching ts 50s >= 45s.
        let snap = wait_until(&feeder, Duration::from_secs(5), |s| s.message_count >= 6);
        assert_eq!(
            snap.message_count, 6,
            "pre-roll stops at the first ts >= target"
        );
        assert_eq!(snap.current_seq, Some(5));
        assert_eq!(snap.current_ts, Some(base + chrono::Duration::seconds(50)));
        assert_eq!(snap.start_ts, Some(base), "chart rebuilt from t=0");

        // Paused: it must NOT advance past the pre-roll.
        std::thread::sleep(Duration::from_millis(50));
        let held = feeder.state.lock().expect("lock").clone();
        assert_eq!(
            held.message_count, 6,
            "paused playback holds at the seek point"
        );

        feeder.stop_and_join();
    }

    /// After seeking, resuming continues paced playback and finishes the rest.
    #[test]
    fn seek_then_resume_plays_to_end() {
        let dir = tempfile::tempdir().expect("tempdir");
        let msgs = record_session(dir.path());
        let base = msgs[0].timestamp;
        let target = base + chrono::Duration::seconds(45);

        // Fast speed so paced playback of the remainder finishes immediately.
        let mut feeder = Feeder::start(dir.path().to_path_buf(), f64::INFINITY);
        feeder.seek_to(target);

        // All 10 messages eventually applied (pre-roll 6 + paced remainder 4).
        let snap = wait_until(&feeder, Duration::from_secs(5), |s| s.message_count == 10);
        assert_eq!(snap.current_seq, Some(9));
        feeder.stop_and_join();
    }
}
