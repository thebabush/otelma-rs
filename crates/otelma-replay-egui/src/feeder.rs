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

use otelma::{drive_realtime, PlaybackControl, SessionReader};
use otelma_polymarket::PolyEvent;

use crate::state::{GuiSink, ReplayState};

/// Owns the feeder thread and the state it writes into.
pub struct Feeder {
    session_dir: PathBuf,
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
        feeder.spawn();
        feeder
    }

    /// Spawn a feeder thread for the current session into the current state.
    fn spawn(&mut self) {
        let session_dir = self.session_dir.clone();
        let state = Arc::clone(&self.state);
        let control = Arc::clone(&self.control);

        let handle = std::thread::spawn(move || {
            let reader = match SessionReader::<PolyEvent>::open(&session_dir) {
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
        self.spawn();
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
