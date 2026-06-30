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

/// A pre-roll request handed to the feeder thread before paced playback resumes.
enum Seek {
    /// State was cleared by the caller; apply `[0, target]` so the chart rebuilds
    /// from the start (a backward seek, the only way to rewind).
    Rebuild { target: DateTime<Utc> },
    /// State is kept; skip messages already applied (`seq <= from_seq`) and apply
    /// only `(from_seq, target]` — a forward seek fast-forwards in place, never
    /// restarting from the start.
    Forward {
        from_seq: u64,
        target: DateTime<Utc>,
    },
}

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
    /// A `Seek` runs an **un-paced pre-roll** before paced playback: it pulls
    /// messages straight from the reader's `Iterator` (no sleeping), then hands
    /// the *same* partially-consumed reader to [`drive_realtime`] for the
    /// remainder — keeping seek out of core's drive (no engine change). The
    /// pre-roll being un-paced is what makes a seek effectively instant ("max
    /// speed"). [`Seek::Rebuild`] applies `[0, target]` (state was cleared);
    /// [`Seek::Forward`] keeps the chart and applies only `(from_seq, target]`.
    fn spawn(&mut self, seek: Option<Seek>) {
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

            // Pre-roll (un-paced) up to the seek target. `skip_after = Some(seq)`
            // skips messages already in the kept state (forward seek); `None`
            // applies everything (rebuild). We consume the reader by reference,
            // then pass it on so paced playback continues from where it left off.
            if let Some(seek) = seek {
                let (skip_after, target) = match seek {
                    Seek::Rebuild { target } => (None, target),
                    Seek::Forward { from_seq, target } => (Some(from_seq), target),
                };
                for item in reader.by_ref() {
                    let msg = match item {
                        Ok(msg) => msg,
                        Err(e) => {
                            eprintln!("feeder: seek pre-roll error: {e}");
                            return;
                        }
                    };
                    if skip_after.is_none_or(|s| msg.seq > s) {
                        locking.apply(&msg);
                    }
                    if msg.timestamp >= target {
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

    /// Restart from the beginning: stop the current thread, clear state, and
    /// spawn afresh. Preserves the current speed and pause state. (The selected
    /// asset is owned by the app and is intentionally *not* reset here.)
    pub fn restart(&mut self) {
        self.stop_and_join();
        self.refresh_control();
        if let Ok(mut s) = self.state.lock() {
            s.clear();
        }
        self.spawn(None);
    }

    /// Seek to `target` (recorded message time). A **forward** seek (past the
    /// current playhead) fast-forwards *in place*: the chart is kept and only
    /// `(playhead, target]` is applied, so it never restarts from the start. A
    /// **backward** seek clears and rebuilds `[0, target]` (the only way to
    /// rewind). Either way the un-paced pre-roll makes the jump effectively
    /// instant, then paced playback resumes at the current speed. Deterministic:
    /// `target` is a recorded timestamp, never a wall-clock read.
    pub fn seek_to(&mut self, target: DateTime<Utc>) {
        let (from_seq, from_ts) = self
            .state
            .lock()
            .map(|s| (s.current_seq, s.current_ts))
            .unwrap_or((None, None));
        self.stop_and_join();
        self.refresh_control();
        if from_ts.is_some_and(|t| target > t) {
            // Forward: keep the chart, fast-forward from the current playhead.
            self.spawn(Some(Seek::Forward {
                from_seq: from_seq.unwrap_or(0),
                target,
            }));
        } else {
            if let Ok(mut s) = self.state.lock() {
                s.clear();
            }
            self.spawn(Some(Seek::Rebuild { target }));
        }
    }

    /// Build a fresh [`PlaybackControl`] carrying over speed and pause (the old
    /// control's `stop` is irreversible by design, so a restart/seek needs a new
    /// one).
    fn refresh_control(&mut self) {
        let speed = self.control.speed();
        let paused = self.control.is_paused();
        let fresh = PlaybackControl::new(speed);
        if paused {
            fresh.pause();
        }
        self.control = Arc::new(fresh);
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

    /// A forward seek (target past the current playhead) fast-forwards *in place*:
    /// it keeps the existing chart and applies only the new messages, rather than
    /// clearing and rebuilding from t=0. Result: the chart still spans from the
    /// start, the playhead reaches the target, and no message is double-applied.
    #[test]
    fn forward_seek_keeps_chart_and_extends_to_target() {
        let dir = tempfile::tempdir().expect("tempdir");
        let msgs = record_session(dir.path());
        let base = msgs[0].timestamp;

        // Paused so paced playback can't advance on its own between the seeks.
        let mut feeder = Feeder::start(dir.path().to_path_buf(), 1.0);
        feeder.control.pause();

        // Establish a playhead at 20s (msg 2): three messages applied.
        feeder.seek_to(base + chrono::Duration::seconds(20));
        let snap = wait_until(&feeder, Duration::from_secs(5), |s| {
            s.current_seq == Some(2)
        });
        assert_eq!(snap.message_count, 3);

        // Forward-seek to 70s (msg 7). This must extend the kept chart to msg 7,
        // not rebuild: total applied = 8 (msgs 0..=7), never more (no re-apply).
        feeder.seek_to(base + chrono::Duration::seconds(70));
        let snap = wait_until(&feeder, Duration::from_secs(5), |s| {
            s.current_seq == Some(7)
        });
        assert_eq!(
            snap.message_count, 8,
            "extended to msg 7, nothing re-applied"
        );
        assert_eq!(snap.current_ts, Some(base + chrono::Duration::seconds(70)));
        assert_eq!(snap.start_ts, Some(base), "chart still spans from t=0");

        feeder.stop_and_join();
    }
}
