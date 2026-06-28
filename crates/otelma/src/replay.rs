//! Replay engine: a [`Sink`] trait plus deterministic and real-time feeders.
//!
//! # Determinism contract
//!
//! A [`Sink`] computes its state purely from [`Message`] contents (`seq`,
//! `timestamp`, `payload`). It **never reads the wall clock**. All notion of
//! time inside a sink flows from `msg.timestamp`.
//!
//! Pacing and sleeping live exclusively in the feeders ([`drive`],
//! [`drive_realtime`]). They change *when* a message is delivered, never *what*
//! the sink computes from it. Consequently, replaying the same recording
//! produces identical sink state at any speed — as fast as possible, real-time,
//! 10×, or paused. [`drive`] and [`drive_realtime`] deliver exactly the same
//! messages in the same order; only the timing differs.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::error::Error;
use crate::message::Message;

/// A consumer of a replayed message stream.
///
/// Implementors must compute purely from message contents and must not read
/// wall-clock time — see the [module-level determinism contract](self).
pub trait Sink<T> {
    /// Apply one message to the consumer's state.
    fn apply(&mut self, msg: &Message<T>);
}

/// Feed every message into `sink` as fast as possible.
///
/// Deterministic: no wall-clock reads, no sleeping. Returns the first reader
/// error encountered (fail-fast), leaving any already-applied messages in the
/// sink.
pub fn drive<T, I, S>(messages: I, sink: &mut S) -> Result<(), Error>
where
    I: IntoIterator<Item = Result<Message<T>, Error>>,
    S: Sink<T>,
{
    for msg in messages {
        sink.apply(&msg?);
    }
    Ok(())
}

/// The longest single sleep slice, so live speed/pause/stop changes are picked
/// up promptly.
const SLEEP_SLICE: Duration = Duration::from_millis(50);

/// Feed messages honoring the real-time gaps between their timestamps, scaled
/// by `control.speed()`, while respecting pause/stop.
///
/// The feeder is permitted wall-clock access (it calls [`thread::sleep`]); the
/// sink is not. Sleeping is split into [`SLEEP_SLICE`] chunks so changes to the
/// control take effect within ~50ms. A non-finite or non-positive speed means
/// "as fast as possible" (no sleeping). Returns early without applying the
/// remaining messages if the control is stopped.
pub fn drive_realtime<T, I, S>(
    messages: I,
    sink: &mut S,
    control: &PlaybackControl,
) -> Result<(), Error>
where
    I: IntoIterator<Item = Result<Message<T>, Error>>,
    S: Sink<T>,
{
    let mut prev_ts: Option<DateTime<Utc>> = None;

    for msg in messages {
        if control.should_stop() {
            return Ok(());
        }
        let msg = msg?;

        if let Some(prev) = prev_ts {
            let gap = msg.timestamp - prev;
            // Negative gaps can't occur on a monotonic stream, but clamp anyway.
            let gap_secs = gap.num_microseconds().unwrap_or(0).max(0) as f64 / 1_000_000.0;
            sleep_scaled(gap_secs, control);
        }

        // Block here while paused (still checking stop), before applying.
        while control.is_paused() {
            if control.should_stop() {
                return Ok(());
            }
            thread::sleep(SLEEP_SLICE);
        }
        if control.should_stop() {
            return Ok(());
        }

        sink.apply(&msg);
        prev_ts = Some(msg.timestamp);
    }
    Ok(())
}

/// Sleep `gap_secs / speed`, in slices, aborting promptly on stop. A non-finite
/// or non-positive speed sleeps not at all (fastest playback).
fn sleep_scaled(gap_secs: f64, control: &PlaybackControl) {
    let speed = control.speed();
    if !speed.is_finite() || speed <= 0.0 || gap_secs <= 0.0 {
        return;
    }
    let mut remaining = Duration::from_secs_f64(gap_secs / speed);
    while remaining > Duration::ZERO {
        if control.should_stop() {
            return;
        }
        let slice = remaining.min(SLEEP_SLICE);
        thread::sleep(slice);
        remaining -= slice;
    }
}

/// Thread-safe, shareable playback control for paced feeders.
///
/// All methods take `&self` (interior mutability), so it can be wrapped in an
/// [`std::sync::Arc`] and shared between a background feeder thread and a UI
/// thread. No GUI dependencies.
pub struct PlaybackControl {
    /// Speed multiplier stored as raw `f64` bits for lock-free access.
    speed_bits: AtomicU64,
    paused: AtomicBool,
    stop: AtomicBool,
    /// Serializes speed updates so concurrent `set_speed` calls can't tear.
    speed_lock: Mutex<()>,
}

impl PlaybackControl {
    /// Create a control with the given initial speed multiplier.
    pub fn new(speed: f64) -> Self {
        Self {
            speed_bits: AtomicU64::new(speed.to_bits()),
            paused: AtomicBool::new(false),
            stop: AtomicBool::new(false),
            speed_lock: Mutex::new(()),
        }
    }

    /// The current speed multiplier.
    pub fn speed(&self) -> f64 {
        f64::from_bits(self.speed_bits.load(Ordering::Relaxed))
    }

    /// Set the speed multiplier.
    pub fn set_speed(&self, speed: f64) {
        let _guard = self
            .speed_lock
            .lock()
            .expect("playback speed lock poisoned");
        self.speed_bits.store(speed.to_bits(), Ordering::Relaxed);
    }

    /// Whether playback is currently paused.
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    /// Pause playback.
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Relaxed);
    }

    /// Resume playback.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::Relaxed);
    }

    /// Request that playback stop. Irreversible.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    /// Whether a stop has been requested.
    pub fn should_stop(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }
}

impl Default for PlaybackControl {
    fn default() -> Self {
        Self::new(1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Payload;
    use crate::reader::SessionReader;
    use crate::recorder::Recorder;
    use serde::{Deserialize, Serialize};
    use std::path::Path;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum SampleEvent {
        Tick,
        Book { bid: i64, ask: i64 },
    }

    impl Payload for SampleEvent {
        fn type_name(&self) -> &str {
            match self {
                SampleEvent::Tick => "Tick",
                SampleEvent::Book { .. } => "Book",
            }
        }
    }

    /// A sink that records every applied message in order.
    #[derive(Default)]
    struct CollectingSink {
        applied: Vec<Message<SampleEvent>>,
    }

    impl Sink<SampleEvent> for CollectingSink {
        fn apply(&mut self, msg: &Message<SampleEvent>) {
            self.applied.push(msg.clone());
        }
    }

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s)
            .expect("valid rfc3339")
            .with_timezone(&Utc)
    }

    fn sample_stream() -> Vec<Message<SampleEvent>> {
        vec![
            Message::new(0, ts("2026-01-01T10:00:00Z"), SampleEvent::Tick),
            Message::new(
                1,
                ts("2026-01-01T10:30:00Z"),
                SampleEvent::Book { bid: 1, ask: 2 },
            ),
            Message::new(2, ts("2026-01-01T11:00:00Z"), SampleEvent::Tick),
        ]
    }

    fn record_stream(dir: &Path, msgs: &[Message<SampleEvent>]) {
        let mut rec = Recorder::new(dir).expect("recorder");
        for m in msgs {
            rec.record(m).expect("record");
        }
        rec.close().expect("close");
    }

    #[test]
    fn drive_applies_all_in_order() {
        let dir = tempdir().expect("tempdir");
        let original = sample_stream();
        record_stream(dir.path(), &original);

        let reader = SessionReader::<SampleEvent>::open(dir.path()).expect("open");
        let mut sink = CollectingSink::default();
        drive(reader, &mut sink).expect("drive");

        assert_eq!(sink.applied, original);
    }

    #[test]
    fn drive_is_deterministic() {
        let dir = tempdir().expect("tempdir");
        record_stream(dir.path(), &sample_stream());

        let mut sink_a = CollectingSink::default();
        drive(
            SessionReader::<SampleEvent>::open(dir.path()).expect("open"),
            &mut sink_a,
        )
        .expect("drive a");

        let mut sink_b = CollectingSink::default();
        drive(
            SessionReader::<SampleEvent>::open(dir.path()).expect("open"),
            &mut sink_b,
        )
        .expect("drive b");

        assert_eq!(sink_a.applied, sink_b.applied);
    }

    #[test]
    fn drive_propagates_reader_error() {
        let good = Message::new(0, ts("2026-01-01T10:00:00Z"), SampleEvent::Tick);
        let items: Vec<Result<Message<SampleEvent>, Error>> = vec![
            Ok(good.clone()),
            Ok(Message::new(
                1,
                ts("2026-01-01T10:01:00Z"),
                SampleEvent::Tick,
            )),
            Err(Error::Schema("boom".to_string())),
            Ok(Message::new(
                2,
                ts("2026-01-01T10:02:00Z"),
                SampleEvent::Tick,
            )),
        ];

        let mut sink = CollectingSink::default();
        let result = drive(items, &mut sink);

        assert!(matches!(result, Err(Error::Schema(_))));
        // Saw exactly the two Oks preceding the error.
        assert_eq!(sink.applied.len(), 2);
        assert_eq!(sink.applied[0], good);
    }

    #[test]
    fn drive_realtime_infinity_applies_all_fast() {
        let original = sample_stream();
        let items: Vec<Result<Message<SampleEvent>, Error>> =
            original.iter().cloned().map(Ok).collect();

        let control = PlaybackControl::new(f64::INFINITY);
        let mut sink = CollectingSink::default();
        drive_realtime(items, &mut sink, &control).expect("realtime");

        assert_eq!(sink.applied, original);
    }

    #[test]
    fn drive_realtime_stop_terminates_early() {
        // Large real-time gaps so the feeder is mid-sleep when we stop it.
        let msgs = [
            Message::new(0, ts("2026-01-01T10:00:00Z"), SampleEvent::Tick),
            Message::new(1, ts("2026-01-01T10:00:30Z"), SampleEvent::Tick),
            Message::new(2, ts("2026-01-01T10:01:00Z"), SampleEvent::Tick),
        ];
        let items: Vec<Result<Message<SampleEvent>, Error>> =
            msgs.iter().cloned().map(Ok).collect();

        // speed 1.0 → 30s gaps; stop should abort well before completion.
        let control = Arc::new(PlaybackControl::new(1.0));
        let feeder_control = Arc::clone(&control);

        let handle = thread::spawn(move || {
            let mut sink = CollectingSink::default();
            drive_realtime(items, &mut sink, &feeder_control).expect("realtime");
            sink.applied.len()
        });

        // Let the first message apply, then stop during the long gap sleep.
        thread::sleep(Duration::from_millis(100));
        control.stop();
        let applied = handle.join().expect("join feeder");

        assert!(applied < 3, "stop should abort before all messages applied");
    }

    #[test]
    fn pause_then_stop_does_not_deadlock() {
        let msgs = sample_stream();
        let items: Vec<Result<Message<SampleEvent>, Error>> =
            msgs.iter().cloned().map(Ok).collect();

        let control = Arc::new(PlaybackControl::new(f64::INFINITY));
        control.pause();
        let feeder_control = Arc::clone(&control);

        let handle = thread::spawn(move || {
            let mut sink = CollectingSink::default();
            drive_realtime(items, &mut sink, &feeder_control).expect("realtime");
        });

        thread::sleep(Duration::from_millis(50));
        control.stop();
        handle
            .join()
            .expect("feeder must not deadlock on pause+stop");
    }
}
