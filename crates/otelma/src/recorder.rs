//! [`Recorder`] — writes a message stream to hourly-rolled Parquet part files.
//!
//! Each session is a directory of zero-padded part files (`part-0000.parquet`,
//! `part-0001.parquet`, …). A new part rolls whenever an incoming message falls
//! into a later UTC-hour bucket than the currently open part, producing
//! hour-aligned, deterministic files. Idle hours simply yield no part.
//!
//! Rows for the current part are buffered in memory and written as a single
//! Parquet file (footer and all) when the part rolls or on [`Recorder::close`].
//! This means an in-progress hour is not on disk until it rolls: the accepted
//! crash tradeoff is losing at most the current hour. A configurable safety cap
//! forces an early roll if the buffer grows pathologically large.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, StringArray, TimestampMicrosecondArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, DurationRound, TimeDelta, Utc};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::codec::encode_payload;
use crate::error::Error;
use crate::message::{Message, Payload};
use crate::monotonic::Monotonicity;
use crate::parts::part_schema;

/// Default safety cap on buffered rows before forcing an early roll.
const DEFAULT_MAX_ROWS: usize = 2_000_000;

/// A UTC instant truncated to its hour. Construction is the only way to obtain
/// one (via [`HourBucket::of`]), so an `HourBucket` is always hour-aligned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct HourBucket(DateTime<Utc>);

impl HourBucket {
    /// The hour bucket containing `ts`.
    fn of(ts: DateTime<Utc>) -> Self {
        HourBucket(
            ts.duration_trunc(TimeDelta::hours(1))
                .expect("hour truncation is always representable"),
        )
    }
}

/// Zero-padded, monotonically increasing part-file index. Owns its filename
/// formatting so the on-disk naming convention lives in one place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartIndex(u32);

impl PartIndex {
    /// The first part of a session.
    fn first() -> Self {
        PartIndex(0)
    }

    /// The next part index.
    fn next(self) -> Self {
        PartIndex(self.0 + 1)
    }

    /// The part file name (e.g. `part-0000.parquet`).
    fn file_name(self) -> String {
        format!("part-{:04}.parquet", self.0)
    }
}

/// In-memory column buffers for the part currently being assembled.
#[derive(Default)]
struct PartBuffer {
    seq: Vec<u64>,
    timestamp_micros: Vec<i64>,
    type_name: Vec<String>,
    payload: Vec<Vec<u8>>,
}

impl PartBuffer {
    fn len(&self) -> usize {
        self.seq.len()
    }

    fn is_empty(&self) -> bool {
        self.seq.is_empty()
    }
}

/// Writes a message stream to hourly-rolled Parquet part files (ZSTD).
pub struct Recorder {
    session_dir: PathBuf,
    part_idx: PartIndex,
    /// UTC hour bucket of the currently open part, or `None` before the first
    /// message is recorded.
    current_hour: Option<HourBucket>,
    buffer: PartBuffer,
    props: WriterProperties,
    max_rows: usize,
    /// Enforces the same ordering invariant the reader checks, so any session
    /// the recorder accepts reads back without a mid-stream ordering error.
    monotonicity: Monotonicity,
}

impl Recorder {
    /// Create a recorder writing into `session_dir` (created if absent).
    pub fn new(session_dir: impl Into<PathBuf>) -> Result<Self, Error> {
        Self::with_max_rows(session_dir, DEFAULT_MAX_ROWS)
    }

    /// Create a recorder with an explicit safety cap on buffered rows.
    pub fn with_max_rows(session_dir: impl Into<PathBuf>, max_rows: usize) -> Result<Self, Error> {
        let session_dir = session_dir.into();
        fs::create_dir_all(&session_dir)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();
        Ok(Self {
            session_dir,
            part_idx: PartIndex::first(),
            current_hour: None,
            buffer: PartBuffer::default(),
            props,
            max_rows,
            monotonicity: Monotonicity::default(),
        })
    }

    /// Append a message to the current part, rolling first if its timestamp
    /// crosses into a later UTC hour or the safety cap is hit.
    pub fn record<T: Payload>(&mut self, msg: &Message<T>) -> Result<(), Error> {
        // Enforce the stream invariant on write (before buffering): a regressing
        // seq or timestamp is rejected, so the recording always reads back.
        self.monotonicity.check(msg.seq, msg.timestamp)?;

        let hour = HourBucket::of(msg.timestamp);

        match self.current_hour {
            Some(current) if hour > current => self.roll()?,
            _ => {}
        }
        if self.buffer.len() >= self.max_rows {
            self.roll()?;
        }
        self.current_hour = Some(hour);

        let micros = msg.timestamp.timestamp_micros();
        self.buffer.seq.push(msg.seq);
        self.buffer.timestamp_micros.push(micros);
        self.buffer
            .type_name
            .push(msg.payload.type_name().to_string());
        self.buffer.payload.push(encode_payload(&msg.payload)?);
        Ok(())
    }

    /// Write the buffered part to disk and advance to the next part index.
    fn roll(&mut self) -> Result<(), Error> {
        self.flush_buffer()?;
        self.part_idx = self.part_idx.next();
        self.current_hour = None;
        Ok(())
    }

    /// Write the buffered rows as a single Parquet file. No-op if empty.
    fn flush_buffer(&mut self) -> Result<(), Error> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let buffer = std::mem::take(&mut self.buffer);

        let seq: ArrayRef = Arc::new(UInt64Array::from(buffer.seq));
        let timestamp: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(buffer.timestamp_micros).with_timezone("UTC"));
        let type_name: ArrayRef = Arc::new(StringArray::from(buffer.type_name));
        let payload: ArrayRef = Arc::new(BinaryArray::from(
            buffer
                .payload
                .iter()
                .map(|b| b.as_slice())
                .collect::<Vec<_>>(),
        ));

        let batch = RecordBatch::try_new(part_schema(), vec![seq, timestamp, type_name, payload])?;

        let path = self.session_dir.join(self.part_idx.file_name());
        let file = fs::File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, part_schema(), Some(self.props.clone()))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    /// Flush the final (partial) part and finish.
    pub fn close(mut self) -> Result<(), Error> {
        self.flush_buffer()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::decode_payload;
    use arrow::datatypes::{DataType, TimeUnit};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
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

    fn list_parts(dir: &std::path::Path) -> Vec<PathBuf> {
        let mut parts: Vec<PathBuf> = fs::read_dir(dir)
            .expect("read session dir")
            .map(|e| e.expect("dir entry").path())
            .filter(|p| p.extension().map(|x| x == "parquet").unwrap_or(false))
            .collect();
        parts.sort();
        parts
    }

    #[test]
    fn rolls_on_utc_hour_boundary() {
        let dir = tempdir().expect("tempdir");
        let mut rec = Recorder::new(dir.path()).expect("recorder");

        // Three messages in hour 10, two in hour 11.
        rec.record(&Message::new(
            0,
            ts("2026-01-01T10:00:00Z"),
            SampleEvent::Tick,
        ))
        .expect("record");
        rec.record(&Message::new(
            1,
            ts("2026-01-01T10:30:00Z"),
            SampleEvent::Book { bid: 1, ask: 2 },
        ))
        .expect("record");
        rec.record(&Message::new(
            2,
            ts("2026-01-01T10:59:59Z"),
            SampleEvent::Tick,
        ))
        .expect("record");
        rec.record(&Message::new(
            3,
            ts("2026-01-01T11:00:00Z"),
            SampleEvent::Tick,
        ))
        .expect("record");
        rec.record(&Message::new(
            4,
            ts("2026-01-01T11:15:00Z"),
            SampleEvent::Book { bid: 3, ask: 4 },
        ))
        .expect("record");
        rec.close().expect("close");

        let parts = list_parts(dir.path());
        assert_eq!(parts.len(), 2, "expected exactly two part files");

        let counts: Vec<usize> = parts
            .iter()
            .map(|p| {
                let file = fs::File::open(p).expect("open part");
                let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                    .expect("reader builder")
                    .build()
                    .expect("reader");
                reader.map(|b| b.expect("batch").num_rows()).sum::<usize>()
            })
            .collect();
        assert_eq!(counts, vec![3, 2]);
    }

    #[test]
    fn part_columns_round_trip() {
        let dir = tempdir().expect("tempdir");
        let mut rec = Recorder::new(dir.path()).expect("recorder");

        let msgs = vec![
            Message::new(10, ts("2026-03-02T08:00:00Z"), SampleEvent::Tick),
            Message::new(
                11,
                ts("2026-03-02T08:45:00.123456Z"),
                SampleEvent::Book { bid: 100, ask: 101 },
            ),
        ];
        for m in &msgs {
            rec.record(m).expect("record");
        }
        rec.close().expect("close");

        let parts = list_parts(dir.path());
        assert_eq!(parts.len(), 1);

        let file = fs::File::open(&parts[0]).expect("open");
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("builder");

        // Verify the timestamp column logical type.
        let field = builder
            .schema()
            .field_with_name("timestamp")
            .expect("field");
        assert_eq!(
            field.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );

        let mut reader = builder.build().expect("reader");
        let batch = reader.next().expect("one batch").expect("batch");

        let seq = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("uint64");
        let timestamp = batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp");
        let type_name = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        let payload = batch
            .column(3)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("binary");

        for (i, m) in msgs.iter().enumerate() {
            assert_eq!(seq.value(i), m.seq);
            assert_eq!(timestamp.value(i), m.timestamp.timestamp_micros());
            assert_eq!(type_name.value(i), m.payload.type_name());
            let decoded: SampleEvent = decode_payload(payload.value(i)).expect("decode payload");
            assert_eq!(decoded, m.payload);
        }
    }

    #[test]
    fn close_flushes_trailing_partial_hour() {
        let dir = tempdir().expect("tempdir");
        let mut rec = Recorder::new(dir.path()).expect("recorder");
        rec.record(&Message::new(
            0,
            ts("2026-05-01T12:34:56Z"),
            SampleEvent::Tick,
        ))
        .expect("record");
        rec.close().expect("close");

        let parts = list_parts(dir.path());
        assert_eq!(parts.len(), 1, "trailing partial hour must be flushed");
    }

    #[test]
    fn record_rejects_seq_regression() {
        let dir = tempdir().expect("tempdir");
        let mut rec = Recorder::new(dir.path()).expect("recorder");
        rec.record(&Message::new(
            5,
            ts("2026-05-01T12:00:00Z"),
            SampleEvent::Tick,
        ))
        .expect("first record");
        // seq 3 <= 5 → rejected before buffering.
        let result = rec.record(&Message::new(
            3,
            ts("2026-05-01T12:00:01Z"),
            SampleEvent::Tick,
        ));
        assert!(matches!(
            result,
            Err(Error::Monotonicity {
                prev_seq: 5,
                seq: 3,
                ..
            })
        ));
    }

    #[test]
    fn record_rejects_timestamp_regression() {
        let dir = tempdir().expect("tempdir");
        let mut rec = Recorder::new(dir.path()).expect("recorder");
        rec.record(&Message::new(
            0,
            ts("2026-05-01T12:00:10Z"),
            SampleEvent::Tick,
        ))
        .expect("first record");
        // Earlier timestamp with a larger seq → rejected.
        let result = rec.record(&Message::new(
            1,
            ts("2026-05-01T12:00:09Z"),
            SampleEvent::Tick,
        ));
        assert!(matches!(result, Err(Error::Monotonicity { .. })));
    }

    #[test]
    fn record_accepts_equal_timestamp_increasing_seq() {
        let dir = tempdir().expect("tempdir");
        let mut rec = Recorder::new(dir.path()).expect("recorder");
        rec.record(&Message::new(
            0,
            ts("2026-05-01T12:00:00Z"),
            SampleEvent::Tick,
        ))
        .expect("first");
        // Equal timestamp is allowed as long as seq strictly increases.
        rec.record(&Message::new(
            1,
            ts("2026-05-01T12:00:00Z"),
            SampleEvent::Tick,
        ))
        .expect("second");
        rec.close().expect("close");
    }
}
