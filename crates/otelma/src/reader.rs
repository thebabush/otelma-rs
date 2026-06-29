//! [`SessionReader`] — streaming, multi-part reconstruction of a recorded
//! `Message<T>` stream.
//!
//! A session directory is a set of UTC-named part files
//! (`YYYYMMDDTHHMMSSZ.parquet`). The reader discovers them, orders them lexically
//! (which, with the fixed-width names, is chronological order), and iterates rows
//! lazily: it holds one part's
//! [`ParquetRecordBatchReader`] and one decoded batch at a time, so memory is
//! O(batch), not O(session).
//!
//! The reader is the trust boundary for stream ordering: `seq` must be strictly
//! increasing and `timestamp` non-decreasing across the entire session
//! (including part boundaries). A violation yields [`Error::Monotonicity`]
//! rather than silently passing a corrupt recording downstream.
//!
//! The iterator is **fused on first error**: once `next()` returns any `Err`
//! (monotonicity, parquet/IO, schema, or payload decode), every subsequent
//! `next()` returns `None`. A single corrupt or unreadable part therefore ends
//! the stream rather than being silently skipped — earlier parts have already
//! been yielded, consistent with the "lose at most the truncated trailing part"
//! recovery story.

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
use crate::monotonic::Monotonicity;

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
    /// Latched on the first error so the iterator fuses (Bug-1 fix): one corrupt
    /// or unreadable part ends the stream rather than silently resuming.
    done: bool,
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
            done: false,
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
            .ok_or(Error::TimestampOutOfRange { micros: ts_micros })?;

        let blob = column::<BinaryArray>(batch, 3, "payload")?.value(row);
        let payload = decode_payload::<T>(blob)?;

        self.monotonicity.check(seq, timestamp)?;
        Ok(Message::new(seq, timestamp, payload))
    }
}

impl<T: DeserializeOwned> Iterator for SessionReader<T> {
    type Item = Result<Message<T>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        // Fused: once any error was returned, the stream is over.
        if self.done {
            return None;
        }

        match self.advance_to_batch() {
            Ok(false) => {
                self.done = true;
                return None;
            }
            Ok(true) => {}
            Err(e) => {
                self.done = true;
                return Some(Err(e));
            }
        }

        let cursor = self.cursor.as_mut().expect("advance_to_batch ensured rows");
        let row = cursor.row;
        // Clone the (small, ref-counted) batch handle so we can borrow `self`
        // mutably for monotonicity bookkeeping while reading the row.
        let batch = cursor.batch.clone();
        let result = self.read_row(&batch, row);
        match result {
            Ok(msg) => {
                // Only advance the row cursor on success, so a fused error never
                // leaves a half-consumed row behind (defensive; we fuse anyway).
                self.cursor.as_mut().expect("cursor present").row += 1;
                Some(Ok(msg))
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

/// Downcast column `idx` of `batch` to a concrete Arrow array type, returning a
/// [`Error::SchemaColumn`] (not a panic) on type mismatch — the file may be
/// foreign.
fn column<'a, A: Array + 'static>(
    batch: &'a RecordBatch,
    idx: usize,
    name: &'static str,
) -> Result<&'a A, Error> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<A>()
        .ok_or(Error::SchemaColumn { column: name })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Payload;
    use crate::test_support::{record_stream, sample_stream, ts, SampleEvent};
    use tempfile::tempdir;

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

    /// Record → read → re-record → read must reproduce the original stream
    /// exactly (seq, microsecond timestamp, payload) — the on-disk format is a
    /// faithful, idempotent round-trip.
    #[test]
    fn record_read_rerecord_is_identity() {
        let dir1 = tempdir().expect("tempdir1");
        let original = sample_stream();
        record_stream(dir1.path(), &original);

        let read1: Vec<Message<SampleEvent>> = SessionReader::<SampleEvent>::open(dir1.path())
            .expect("open1")
            .collect::<Result<_, _>>()
            .expect("read1");
        assert_eq!(read1, original);

        let dir2 = tempdir().expect("tempdir2");
        record_stream(dir2.path(), &read1);

        let read2: Vec<Message<SampleEvent>> = SessionReader::<SampleEvent>::open(dir2.path())
            .expect("open2")
            .collect::<Result<_, _>>()
            .expect("read2");
        assert_eq!(read2, original);
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

        // First recorder: hour 10 → 20260101T100000Z.parquet.
        record_stream(
            dir.path(),
            &[
                Message::new(0, ts("2026-01-01T10:00:00Z"), SampleEvent::Tick),
                Message::new(5, ts("2026-01-01T10:30:00Z"), SampleEvent::Tick),
            ],
        );
        // Record a second session (hour 11, an overlapping seq) and copy its part
        // into the first dir. Its UTC-derived name (20260101T110000Z) sorts after
        // the first part, so the reader chains them in order.
        let dir2 = tempdir().expect("tempdir2");
        record_stream(
            dir2.path(),
            &[
                // seq 3 <= previous last seq (5): violates strict-increasing.
                Message::new(3, ts("2026-01-01T11:00:00Z"), SampleEvent::Tick),
            ],
        );
        let src = dir2.path().join("20260101T110000Z.parquet");
        let dst = dir.path().join("20260101T110000Z.parquet");
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

    /// Write a single part file with arbitrary (possibly non-monotonic) rows,
    /// bypassing the recorder's on-write monotonicity check. Used to fabricate
    /// corrupt sessions for the reader's fuse tests.
    fn write_raw_part(path: &Path, rows: &[(u64, &str, SampleEvent)]) {
        use arrow::array::{
            ArrayRef, BinaryArray, StringArray, TimestampMicrosecondArray, UInt64Array,
        };
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let seq: ArrayRef = Arc::new(UInt64Array::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        ));
        let timestamp: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(
                rows.iter()
                    .map(|r| ts(r.1).timestamp_micros())
                    .collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        );
        let type_name: ArrayRef = Arc::new(StringArray::from(
            rows.iter().map(|r| r.2.type_name()).collect::<Vec<_>>(),
        ));
        let blobs: Vec<Vec<u8>> = rows
            .iter()
            .map(|r| crate::encode_payload(&r.2).expect("encode"))
            .collect();
        let payload: ArrayRef = Arc::new(BinaryArray::from(
            blobs.iter().map(|b| b.as_slice()).collect::<Vec<_>>(),
        ));

        let batch = RecordBatch::try_new(
            crate::part_schema(),
            vec![seq, timestamp, type_name, payload],
        )
        .expect("batch");
        let file = std::fs::File::create(path).expect("create");
        let mut writer = ArrowWriter::try_new(file, crate::part_schema(), None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }

    /// Bug 1(a): an error that is not the last row fuses the iterator — no rows
    /// after the violation are yielded.
    #[test]
    fn fuses_on_mid_part_monotonicity_error() {
        let dir = tempdir().expect("tempdir");
        // Rows 0,5 then 3 (violates after 5) then 4. All in one part / hour.
        write_raw_part(
            &dir.path().join("20260101T100000Z.parquet"),
            &[
                (0, "2026-01-01T10:00:00Z", SampleEvent::Tick),
                (5, "2026-01-01T10:00:01Z", SampleEvent::Tick),
                (3, "2026-01-01T10:00:02Z", SampleEvent::Tick),
                (4, "2026-01-01T10:00:03Z", SampleEvent::Tick),
            ],
        );

        let reader = SessionReader::<SampleEvent>::open(dir.path()).expect("open");
        let results: Vec<Result<Message<SampleEvent>, Error>> = reader.collect();

        // Ok(0), Ok(5), Err(Monotonicity), then NONE — no Ok(4) leaks through.
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().expect("ok").seq, 0);
        assert_eq!(results[1].as_ref().expect("ok").seq, 5);
        assert!(matches!(results[2], Err(Error::Monotonicity { .. })));
    }

    /// Bug 1(b): a corrupt/unreadable middle part fuses the iterator — the reader
    /// does not silently resume into the following part's rows.
    #[test]
    fn fuses_on_corrupt_middle_part() {
        let dir = tempdir().expect("tempdir");
        // First part: valid, one row (hour 10).
        record_stream(
            dir.path(),
            &[Message::new(
                0,
                ts("2026-01-01T10:00:00Z"),
                SampleEvent::Tick,
            )],
        );
        // Middle part: garbage (not a parquet file). Its name sorts between the
        // two valid parts (hour 11).
        std::fs::write(dir.path().join("20260101T110000Z.parquet"), b"not parquet")
            .expect("write garbage");
        // Last part: valid, would-be next rows (hour 12).
        let dir2 = tempdir().expect("tempdir2");
        record_stream(
            dir2.path(),
            &[Message::new(
                9,
                ts("2026-01-01T12:00:00Z"),
                SampleEvent::Tick,
            )],
        );
        std::fs::copy(
            dir2.path().join("20260101T120000Z.parquet"),
            dir.path().join("20260101T120000Z.parquet"),
        )
        .expect("copy");

        let reader = SessionReader::<SampleEvent>::open(dir.path()).expect("open");
        let results: Vec<Result<Message<SampleEvent>, Error>> = reader.collect();

        // Ok(0), Err(open/build of the garbage middle part), then NONE — no Ok(9)
        // from the last part.
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].as_ref().expect("ok").seq, 0);
        assert!(results[1].is_err());
    }

    /// A stored timestamp outside the `DateTime<Utc>` range surfaces as
    /// `TimestampOutOfRange` (not a panic). Hand-write a part with `i64::MAX`
    /// micros — valid Arrow, but unrepresentable as a UTC instant.
    #[test]
    fn out_of_range_timestamp_is_error() {
        use arrow::array::{
            ArrayRef, BinaryArray, StringArray, TimestampMicrosecondArray, UInt64Array,
        };
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let dir = tempdir().expect("tempdir");
        let seq: ArrayRef = Arc::new(UInt64Array::from(vec![0u64]));
        let timestamp: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![i64::MAX]).with_timezone("UTC"));
        let type_name: ArrayRef = Arc::new(StringArray::from(vec!["Tick"]));
        let blob = crate::encode_payload(&SampleEvent::Tick).expect("encode");
        let payload: ArrayRef = Arc::new(BinaryArray::from(vec![blob.as_slice()]));
        let batch = RecordBatch::try_new(
            crate::part_schema(),
            vec![seq, timestamp, type_name, payload],
        )
        .expect("batch");

        let path = dir.path().join("20260101T100000Z.parquet");
        let file = std::fs::File::create(&path).expect("create");
        let mut writer = ArrowWriter::try_new(file, crate::part_schema(), None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");

        let reader = SessionReader::<SampleEvent>::open(dir.path()).expect("open");
        let results: Vec<Result<Message<SampleEvent>, Error>> = reader.collect();
        assert_eq!(results.len(), 1);
        assert!(matches!(
            results[0],
            Err(Error::TimestampOutOfRange { micros }) if micros == i64::MAX
        ));
    }

    /// A part whose `seq` column is the wrong Arrow type (Int64, not UInt64) is
    /// a foreign/corrupt file: the column downcast fails with `SchemaColumn`.
    #[test]
    fn wrong_column_type_is_schema_error() {
        use arrow::array::{
            ArrayRef, BinaryArray, Int64Array, StringArray, TimestampMicrosecondArray,
        };
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        // Same column names/order as the part schema, but seq is Int64.
        let schema = Arc::new(Schema::new(vec![
            Field::new("seq", DataType::Int64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("type_name", DataType::Utf8, false),
            Field::new("payload", DataType::Binary, false),
        ]));

        let dir = tempdir().expect("tempdir");
        let seq: ArrayRef = Arc::new(Int64Array::from(vec![0i64]));
        let timestamp: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![ts("2026-01-01T10:00:00Z").timestamp_micros()])
                .with_timezone("UTC"),
        );
        let type_name: ArrayRef = Arc::new(StringArray::from(vec!["Tick"]));
        let blob = crate::encode_payload(&SampleEvent::Tick).expect("encode");
        let payload: ArrayRef = Arc::new(BinaryArray::from(vec![blob.as_slice()]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![seq, timestamp, type_name, payload],
        )
        .expect("batch");

        let path = dir.path().join("20260101T100000Z.parquet");
        let file = std::fs::File::create(&path).expect("create");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");

        let reader = SessionReader::<SampleEvent>::open(dir.path()).expect("open");
        let results: Vec<Result<Message<SampleEvent>, Error>> = reader.collect();
        assert_eq!(results.len(), 1);
        assert!(matches!(
            results[0],
            Err(Error::SchemaColumn { column: "seq" })
        ));
    }

    /// A part written as two record batches (two writes before close) reads all
    /// rows back in order — the reader iterates batches within a part, not just
    /// the first.
    #[test]
    fn reads_all_rows_across_multiple_batches_in_one_part() {
        use arrow::array::{
            ArrayRef, BinaryArray, StringArray, TimestampMicrosecondArray, UInt64Array,
        };
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let make_batch = |seqs: &[u64], base_sec: i64| {
            let seq: ArrayRef = Arc::new(UInt64Array::from(seqs.to_vec()));
            let timestamp: ArrayRef = Arc::new(
                TimestampMicrosecondArray::from(
                    seqs.iter()
                        .map(|&s| {
                            DateTime::<Utc>::from_timestamp(base_sec + s as i64, 0)
                                .expect("ts")
                                .timestamp_micros()
                        })
                        .collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            );
            let type_name: ArrayRef = Arc::new(StringArray::from(vec!["Tick"; seqs.len()]));
            let blob = crate::encode_payload(&SampleEvent::Tick).expect("encode");
            let payload: ArrayRef = Arc::new(BinaryArray::from(vec![blob.as_slice(); seqs.len()]));
            RecordBatch::try_new(
                crate::part_schema(),
                vec![seq, timestamp, type_name, payload],
            )
            .expect("batch")
        };

        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("20260101T100000Z.parquet");
        let file = std::fs::File::create(&path).expect("create");
        let mut writer = ArrowWriter::try_new(file, crate::part_schema(), None).expect("writer");
        // Two separate batches in the same part file.
        writer.write(&make_batch(&[0, 1], 0)).expect("write b1");
        writer.write(&make_batch(&[2, 3], 100)).expect("write b2");
        writer.close().expect("close");

        let reader = SessionReader::<SampleEvent>::open(dir.path()).expect("open");
        let seqs: Vec<u64> = reader.map(|m| m.expect("ok").seq).collect();
        assert_eq!(seqs, vec![0, 1, 2, 3]);
    }

    /// A legitimately empty (zero-row) middle part is skipped, not treated as an
    /// error: `[part0=rows, part1=empty, part2=rows]` reads the full stream and
    /// the fuse-on-error does not trip on the empty part.
    #[test]
    fn empty_middle_part_is_skipped_not_an_error() {
        let dir = tempdir().expect("tempdir");
        // First part: one row (hour 10).
        write_raw_part(
            &dir.path().join("20260101T100000Z.parquet"),
            &[(0, "2026-01-01T10:00:00Z", SampleEvent::Tick)],
        );
        // Middle part: zero rows (empty batch with the correct schema). Its name
        // sorts between the two non-empty parts.
        write_raw_part(&dir.path().join("20260101T103000Z.parquet"), &[]);
        // Last part: another row, monotonically after the first (hour 11).
        write_raw_part(
            &dir.path().join("20260101T110000Z.parquet"),
            &[(1, "2026-01-01T11:00:00Z", SampleEvent::Tick)],
        );

        let reader = SessionReader::<SampleEvent>::open(dir.path()).expect("open");
        let results: Vec<Result<Message<SampleEvent>, Error>> = reader.collect();
        let seqs: Vec<u64> = results
            .iter()
            .map(|r| r.as_ref().expect("no error on empty middle part").seq)
            .collect();
        assert_eq!(seqs, vec![0, 1]);
    }
}
