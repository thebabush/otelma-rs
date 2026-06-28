//! [`SessionReader`] — streaming, multi-part reconstruction of a recorded
//! `Message<T>` stream.
//!
//! A session directory is a set of zero-padded `part-*.parquet` files. The
//! reader discovers them, orders them lexically (which matches their numeric
//! order), and iterates rows lazily: it holds one part's
//! [`ParquetRecordBatchReader`] and one decoded batch at a time, so memory is
//! O(batch), not O(session).
//!
//! The reader is the trust boundary for stream ordering: `seq` must be strictly
//! increasing and `timestamp` non-decreasing across the entire session
//! (including part boundaries). A violation yields [`Error::Monotonicity`]
//! rather than silently passing a corrupt recording downstream.

use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use arrow::array::{Array, BinaryArray, TimestampMicrosecondArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use serde::de::DeserializeOwned;

use crate::codec::decode_payload;
use crate::error::Error;
use crate::message::Message;

/// The ordering invariant enforced across the whole session.
///
/// Holds the last accepted `(seq, timestamp)` and rejects any row that is not
/// strictly increasing in `seq` and non-decreasing in `timestamp`.
#[derive(Default)]
struct Monotonicity {
    last: Option<(u64, DateTime<Utc>)>,
}

impl Monotonicity {
    /// Check `(seq, ts)` against the last accepted row, updating state on
    /// success. Returns [`Error::Monotonicity`] on violation.
    fn check(&mut self, seq: u64, ts: DateTime<Utc>) -> Result<(), Error> {
        if let Some((prev_seq, prev_ts)) = self.last {
            if seq <= prev_seq || ts < prev_ts {
                return Err(Error::Monotonicity {
                    prev_seq,
                    prev_ts,
                    seq,
                    ts,
                });
            }
        }
        self.last = Some((seq, ts));
        Ok(())
    }
}

/// A lazily-decoded record batch plus a row cursor.
struct BatchCursor {
    batch: RecordBatch,
    row: usize,
}

impl BatchCursor {
    fn new(batch: RecordBatch) -> Self {
        Self { batch, row: 0 }
    }

    fn is_exhausted(&self) -> bool {
        self.row >= self.batch.num_rows()
    }
}

/// Streaming reader over a session's ordered Parquet parts.
pub struct SessionReader<T> {
    /// Remaining part files, in reverse order so the next part is `pop`ped.
    remaining_parts: Vec<PathBuf>,
    /// Reader for the part currently being consumed.
    current_part: Option<ParquetRecordBatchReader>,
    /// The batch currently being yielded from.
    cursor: Option<BatchCursor>,
    monotonicity: Monotonicity,
    _marker: PhantomData<fn() -> T>,
}

impl<T: DeserializeOwned> SessionReader<T> {
    /// Open a session directory, discovering and ordering its `part-*.parquet`
    /// files. A missing directory is an error; an empty one yields an empty
    /// stream.
    pub fn open(session_dir: impl AsRef<Path>) -> Result<Self, Error> {
        // Discover parts in ascending order, then reverse so `pop` yields them
        // ascending.
        let mut parts = crate::parts::part_paths(session_dir)?;
        parts.reverse();
        Ok(Self {
            remaining_parts: parts,
            current_part: None,
            cursor: None,
            monotonicity: Monotonicity::default(),
            _marker: PhantomData,
        })
    }

    /// Advance to the next non-empty batch, opening subsequent parts as needed.
    /// Returns `Ok(true)` if a batch with rows is ready in `self.cursor`,
    /// `Ok(false)` if the whole session is exhausted.
    fn advance_to_batch(&mut self) -> Result<bool, Error> {
        loop {
            if let Some(cursor) = &self.cursor {
                if !cursor.is_exhausted() {
                    return Ok(true);
                }
                self.cursor = None;
            }

            if self.current_part.is_none() {
                match self.remaining_parts.pop() {
                    Some(path) => {
                        let file = std::fs::File::open(path)?;
                        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
                        self.current_part = Some(reader);
                    }
                    None => return Ok(false),
                }
            }

            // Pull the next batch from the current part.
            let reader = self.current_part.as_mut().expect("current_part set above");
            match reader.next() {
                Some(batch) => {
                    let batch = batch?;
                    if batch.num_rows() > 0 {
                        self.cursor = Some(BatchCursor::new(batch));
                    }
                }
                None => {
                    // Part exhausted; move to the next one.
                    self.current_part = None;
                }
            }
        }
    }

    /// Reconstruct the message at `row` of `batch`, validating ordering.
    fn read_row(&mut self, batch: &RecordBatch, row: usize) -> Result<Message<T>, Error> {
        let seq = column::<UInt64Array>(batch, 0, "seq")?.value(row);

        let ts_micros = column::<TimestampMicrosecondArray>(batch, 1, "timestamp")?.value(row);
        let timestamp = DateTime::<Utc>::from_timestamp_micros(ts_micros)
            .ok_or_else(|| Error::Schema(format!("timestamp micros out of range: {ts_micros}")))?;

        let blob = column::<BinaryArray>(batch, 3, "payload")?.value(row);
        let payload = decode_payload::<T>(blob)?;

        self.monotonicity.check(seq, timestamp)?;
        Ok(Message::new(seq, timestamp, payload))
    }
}

impl<T: DeserializeOwned> Iterator for SessionReader<T> {
    type Item = Result<Message<T>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.advance_to_batch() {
            Ok(false) => return None,
            Ok(true) => {}
            Err(e) => return Some(Err(e)),
        }

        let cursor = self.cursor.as_mut().expect("advance_to_batch ensured rows");
        let row = cursor.row;
        cursor.row += 1;
        // Clone the (small, ref-counted) batch handle so we can borrow `self`
        // mutably for monotonicity bookkeeping while reading the row.
        let batch = cursor.batch.clone();
        Some(self.read_row(&batch, row))
    }
}

/// Downcast column `idx` of `batch` to a concrete Arrow array type, returning a
/// [`Error::Schema`] (not a panic) on type mismatch — the file may be foreign.
fn column<'a, A: Array + 'static>(
    batch: &'a RecordBatch,
    idx: usize,
    name: &str,
) -> Result<&'a A, Error> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| Error::Schema(format!("column `{name}` has unexpected Arrow type")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Payload;
    use crate::recorder::Recorder;
    use serde::{Deserialize, Serialize};
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
                ts("2026-01-01T10:30:00.123456Z"),
                SampleEvent::Book { bid: 1, ask: 2 },
            ),
            Message::new(2, ts("2026-01-01T10:59:59Z"), SampleEvent::Tick),
            Message::new(3, ts("2026-01-01T11:00:00Z"), SampleEvent::Tick),
            Message::new(
                4,
                ts("2026-01-01T11:15:00Z"),
                SampleEvent::Book { bid: 3, ask: 4 },
            ),
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
    fn round_trip_across_parts() {
        let dir = tempdir().expect("tempdir");
        let original = sample_stream();
        record_stream(dir.path(), &original);

        let reader = SessionReader::<SampleEvent>::open(dir.path()).expect("open");
        let read: Vec<Message<SampleEvent>> = reader
            .collect::<Result<Vec<_>, _>>()
            .expect("read without error");

        assert_eq!(read, original);
    }

    #[test]
    fn yields_seq_order_across_parts() {
        let dir = tempdir().expect("tempdir");
        record_stream(dir.path(), &sample_stream());

        let reader = SessionReader::<SampleEvent>::open(dir.path()).expect("open");
        let seqs: Vec<u64> = reader.map(|m| m.expect("message").seq).collect();

        assert_eq!(seqs, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn empty_session_yields_empty_stream() {
        let dir = tempdir().expect("tempdir");
        let reader = SessionReader::<SampleEvent>::open(dir.path()).expect("open");
        let read: Vec<_> = reader.collect::<Result<Vec<_>, _>>().expect("no error");
        assert!(read.is_empty());
    }

    #[test]
    fn missing_dir_is_error() {
        let dir = tempdir().expect("tempdir");
        let missing = dir.path().join("does-not-exist");
        let result = SessionReader::<SampleEvent>::open(missing);
        assert!(matches!(result, Err(Error::Io(_))));
    }

    #[test]
    fn monotonicity_violation_across_parts() {
        let dir = tempdir().expect("tempdir");

        // First recorder: hour 10 → part-0000.
        record_stream(
            dir.path(),
            &[
                Message::new(0, ts("2026-01-01T10:00:00Z"), SampleEvent::Tick),
                Message::new(5, ts("2026-01-01T10:30:00Z"), SampleEvent::Tick),
            ],
        );
        // Second recorder writes into the same dir, but a fresh Recorder restarts
        // its part index at 0 — so it would clobber part-0000. Instead, hand the
        // overlapping-seq part a later name by recording into a sub-session then
        // moving the file. Simplest: record a second session and copy its single
        // part in as part-0001.
        let dir2 = tempdir().expect("tempdir2");
        record_stream(
            dir2.path(),
            &[
                // seq 3 <= previous last seq (5): violates strict-increasing.
                Message::new(3, ts("2026-01-01T11:00:00Z"), SampleEvent::Tick),
            ],
        );
        let src = dir2.path().join("part-0000.parquet");
        let dst = dir.path().join("part-0001.parquet");
        std::fs::copy(&src, &dst).expect("copy part");

        let reader = SessionReader::<SampleEvent>::open(dir.path()).expect("open");
        let results: Vec<Result<Message<SampleEvent>, Error>> = reader.collect();

        // First two rows OK, third is the violation.
        assert!(results[0].is_ok());
        assert!(results[1].is_ok());
        match &results[2] {
            Err(Error::Monotonicity { prev_seq, seq, .. }) => {
                assert_eq!(*prev_seq, 5);
                assert_eq!(*seq, 3);
            }
            other => panic!("expected Monotonicity error, got {other:?}"),
        }
    }
}
