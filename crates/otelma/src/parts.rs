//! Low-level part-file utilities: the shared on-disk schema, part discovery,
//! and raw-batch compaction.
//!
//! These operate on the Parquet columns directly (`seq`, `timestamp`,
//! `type_name`, `payload`) without decoding the payload blob, so they are
//! generic over the payload type `T`. The high-level [`crate::SessionReader`]
//! builds on the same part ordering.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, Utc};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::statistics::Statistics;

use crate::error::Error;

/// The `timestamp` column index in the [`part_schema`] (µs-since-epoch, UTC).
const TIMESTAMP_COLUMN: usize = 1;

/// A recording's wall-clock span: `(first message timestamp, last message
/// timestamp)`, both UTC. Returned by [`session_time_bounds`].
pub type TimeBounds = (DateTime<Utc>, DateTime<Utc>);

/// The Arrow schema shared by every part file (and the compacted file).
///
/// Columns: `seq: UInt64`, `timestamp: Timestamp(µs, UTC)`, `type_name: Utf8`,
/// `payload: Binary` (the MessagePack blob).
pub fn part_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("type_name", DataType::Utf8, false),
        Field::new("payload", DataType::Binary, false),
    ]))
}

/// The Parquet writer properties used for all part files: ZSTD compression.
/// Shared by the [`crate::Recorder`] (per-part flushes) and [`compact_session`]
/// so the on-disk codec is defined in exactly one place.
pub(crate) fn zstd_writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build()
}

/// Format a part file name from its start instant: a basic-ISO UTC second
/// timestamp, e.g. `20260628T142311Z.parquet`. Co-located with [`is_part_file`]
/// so the on-disk naming convention (generation *and* recognition) lives in one
/// place. The format is fixed-width and colon-free, so names sort lexically into
/// chronological order and are valid filenames on every platform.
pub(crate) fn part_file_name(start: DateTime<Utc>) -> String {
    format!("{}.parquet", start.format("%Y%m%dT%H%M%SZ"))
}

/// The default session directory for a new recording: `recordings/<UTC start>/`
/// in the same basic-ISO form as the part files. The engine takes any
/// `session_dir`; this is just the convention recording front-ends (the CLI's
/// `record` and the egui `--live` mode) share when no explicit `--out` is given,
/// so it lives in one place rather than being copied per front-end.
pub fn default_session_dir(start: DateTime<Utc>) -> PathBuf {
    PathBuf::from("recordings").join(start.format("%Y%m%dT%H%M%SZ").to_string())
}

/// Discover the part files in `session_dir`, in chronological order (the
/// basic-ISO UTC names are fixed-width, so a lexical sort *is* chronological).
/// Only files matching the part naming convention (see [`part_file_name`]) are
/// included — other parquet files in the directory (e.g. a `compacted.parquet`)
/// are deliberately ignored, so compacting in place doesn't make the reader
/// replay the stream twice. A missing directory is an error; an empty one yields
/// an empty list.
pub fn part_paths(session_dir: impl AsRef<Path>) -> Result<Vec<PathBuf>, Error> {
    let mut parts: Vec<PathBuf> = std::fs::read_dir(session_dir)?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|p| is_part_file(p))
        .collect();
    parts.sort();
    Ok(parts)
}

/// Whether `path` is a session part file, i.e. its name is a basic-ISO UTC
/// second timestamp with a `.parquet` extension (`YYYYMMDDTHHMMSSZ.parquet`).
/// This recognizes exactly what [`part_file_name`] produces and so excludes
/// foreign parquet files such as `compacted.parquet`.
fn is_part_file(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    name.strip_suffix(".parquet")
        .is_some_and(is_basic_iso_utc_second)
}

/// Whether `s` is a basic-ISO UTC second timestamp: `YYYYMMDDTHHMMSSZ` — eight
/// date digits, `T`, six time digits, `Z` (sixteen ASCII bytes). Pure shape
/// check (not a calendar validation); it just distinguishes our part names from
/// other files in the directory.
fn is_basic_iso_utc_second(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 16
        && b[..8].iter().all(u8::is_ascii_digit)
        && b[8] == b'T'
        && b[9..15].iter().all(u8::is_ascii_digit)
        && b[15] == b'Z'
}

/// Merge a session's rolled parts into a single Parquet file at `out`,
/// preserving order and the part schema. Streams raw record batches straight
/// through (no payload decoding), so it is independent of the payload type.
///
/// The result round-trips: reading it back yields the identical message stream.
pub fn compact_session(session_dir: impl AsRef<Path>, out: impl AsRef<Path>) -> Result<(), Error> {
    let parts = part_paths(session_dir)?;
    let file = std::fs::File::create(out)?;
    let mut writer = ArrowWriter::try_new(file, part_schema(), Some(zstd_writer_props()))?;

    for path in parts {
        let part = std::fs::File::open(path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(part)?.build()?;
        for batch in reader {
            writer.write(&batch?)?;
        }
    }
    writer.close()?;
    Ok(())
}

/// A recording's wall-clock span: the first message's timestamp and the last
/// message's timestamp (both UTC). `None` for an empty session (no parts, or
/// parts with no rows).
///
/// This is engine introspection — it reports the recorded timeline, not any UI
/// concept. It is intentionally **cheap**: the earliest timestamp is read from
/// the first part and the latest from the last part. Because the stream is
/// monotonic (the reader enforces non-decreasing timestamps across the whole
/// session), the global minimum lives in the first part and the global maximum
/// in the last. Within a part we prefer the timestamp column's min/max
/// statistics (read from Parquet metadata, no row decode); when a part carries no
/// statistics we fall back to scanning just that part's timestamp column.
pub fn session_time_bounds(session_dir: impl AsRef<Path>) -> Result<Option<TimeBounds>, Error> {
    let parts = part_paths(session_dir)?;
    let Some(first) = parts.first() else {
        return Ok(None);
    };
    let last = parts.last().expect("non-empty parts has a last");

    // Read the first part's span once; its min is the session start, and its max
    // is the fallback session end (single part, or a trailing empty last part).
    let Some((start, first_end)) = part_time_bounds(first)? else {
        return Ok(None);
    };
    // The global max lives in the last part; fall back to the first part's max
    // when the last part is empty or is the first part itself.
    let end = if parts.len() == 1 {
        first_end
    } else {
        part_time_bounds(last)?.map_or(first_end, |(_, end)| end)
    };
    Ok(Some((start, end)))
}

/// The `(min, max)` timestamp of a single part file, or `None` if it has no
/// rows. Prefers the Parquet timestamp-column statistics (metadata only); falls
/// back to scanning the timestamp column when statistics are absent.
fn part_time_bounds(path: &Path) -> Result<Option<TimeBounds>, Error> {
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    if let Some(bounds) = stats_time_bounds(builder.metadata())? {
        return Ok(Some(bounds));
    }

    // No usable statistics: scan just this part's timestamp column.
    let reader = builder.build()?;
    let mut min: Option<i64> = None;
    let mut max: Option<i64> = None;
    for batch in reader {
        let batch = batch?;
        let col = batch
            .column(TIMESTAMP_COLUMN)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or(Error::SchemaColumn {
                column: "timestamp",
            })?;
        for i in 0..col.len() {
            if col.is_null(i) {
                continue;
            }
            let v = col.value(i);
            min = Some(min.map_or(v, |m| m.min(v)));
            max = Some(max.map_or(v, |m| m.max(v)));
        }
    }
    match (min, max) {
        (Some(lo), Some(hi)) => Ok(Some((micros_to_utc(lo)?, micros_to_utc(hi)?))),
        _ => Ok(None),
    }
}

/// Fold the timestamp-column min/max across all row groups from Parquet
/// statistics, without decoding any row. `Ok(None)` when no row group carries
/// usable int64 statistics for that column (caller falls back to a scan).
fn stats_time_bounds(
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> Result<Option<TimeBounds>, Error> {
    let mut min: Option<i64> = None;
    let mut max: Option<i64> = None;
    for rg in metadata.row_groups() {
        let col = rg.column(TIMESTAMP_COLUMN);
        if let Some(Statistics::Int64(stats)) = col.statistics() {
            if let Some(lo) = stats.min_opt() {
                min = Some(min.map_or(*lo, |m| m.min(*lo)));
            }
            if let Some(hi) = stats.max_opt() {
                max = Some(max.map_or(*hi, |m| m.max(*hi)));
            }
        }
    }
    match (min, max) {
        (Some(lo), Some(hi)) => Ok(Some((micros_to_utc(lo)?, micros_to_utc(hi)?))),
        _ => Ok(None),
    }
}

/// Convert microseconds-since-epoch to a UTC instant, mirroring the reader's
/// out-of-range handling.
fn micros_to_utc(micros: i64) -> Result<DateTime<Utc>, Error> {
    DateTime::<Utc>::from_timestamp_micros(micros).ok_or(Error::TimestampOutOfRange { micros })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn part_paths_ignores_non_part_parquet() {
        let dir = tempfile::tempdir().expect("tempdir");
        let touch = |name: &str| std::fs::File::create(dir.path().join(name)).expect("touch");
        touch("20260628T150000Z.parquet");
        touch("20260628T140000Z.parquet");
        touch("compacted.parquet"); // must be ignored
        touch("notes.txt"); // non-parquet, ignored

        let paths = part_paths(dir.path()).expect("paths");
        let names: Vec<&str> = paths
            .iter()
            .map(|p| p.file_name().and_then(|n| n.to_str()).expect("name"))
            .collect();
        // Only part files, in chronological (== lexical) order.
        assert_eq!(
            names,
            vec!["20260628T140000Z.parquet", "20260628T150000Z.parquet"]
        );
    }

    #[test]
    fn is_part_file_matches_basic_iso_utc_names() {
        assert!(is_part_file(Path::new("/x/20260628T142311Z.parquet")));
        assert!(is_part_file(Path::new("/x/20260628T000000Z.parquet")));
        // Foreign / legacy / malformed names are not part files.
        assert!(!is_part_file(Path::new("/x/compacted.parquet")));
        assert!(!is_part_file(Path::new("/x/part-0000.parquet"))); // old scheme
        assert!(!is_part_file(Path::new("/x/20260628T142311Z.txt")));
        assert!(!is_part_file(Path::new("/x/20260628T1423Z.parquet"))); // wrong width
        assert!(!is_part_file(Path::new("/x/2026-06-28T14:23:11Z.parquet"))); // extended ISO
    }

    /// Compacting an empty session directory produces a valid Parquet file that
    /// reads back as a zero-row stream (the part schema, no rows).
    #[test]
    fn compact_empty_session_yields_empty_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let out = dir.path().join("compacted.parquet");
        compact_session(dir.path(), &out).expect("compact");

        let file = std::fs::File::open(&out).expect("open compacted");
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("builder")
            .build()
            .expect("reader");
        let rows: usize = reader.map(|b| b.expect("batch").num_rows()).sum();
        assert_eq!(rows, 0, "empty session compacts to a zero-row file");
    }

    /// Record → compact *in place* → replay must NOT double the stream: the
    /// `compacted.parquet` is ignored by `part_paths`, so SessionReader still
    /// reads only the original parts.
    #[test]
    fn compact_in_place_does_not_double_replay() {
        use crate::test_support::SampleEvent;
        use crate::test_support::{record_stream, sample_stream};
        use crate::SessionReader;

        let dir = tempfile::tempdir().expect("tempdir");
        let original = sample_stream();
        record_stream(dir.path(), &original);

        // Compact into the same directory.
        compact_session(dir.path(), dir.path().join("compacted.parquet")).expect("compact");

        // Replay still yields exactly the original stream, not twice.
        let read: Vec<_> = SessionReader::<SampleEvent>::open(dir.path())
            .expect("open")
            .collect::<Result<Vec<_>, _>>()
            .expect("read");
        assert_eq!(read, original, "compacted file must not be replayed twice");

        // And the compacted file itself round-trips the same row count.
        let file = std::fs::File::open(dir.path().join("compacted.parquet")).expect("open");
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("builder")
            .build()
            .expect("reader");
        let compacted_rows: usize = reader.map(|b| b.expect("batch").num_rows()).sum();
        assert_eq!(compacted_rows, original.len());
    }

    /// An empty session has no time bounds.
    #[test]
    fn session_time_bounds_empty_is_none() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert_eq!(session_time_bounds(dir.path()).expect("bounds"), None);
    }

    /// The bounds are the first message's timestamp and the last message's
    /// timestamp, spanning multiple rolled parts. The sample stream rolls into
    /// several hourly parts, so this exercises the first-part-min /
    /// last-part-max path (via Parquet statistics).
    #[test]
    fn session_time_bounds_spans_first_to_last_message() {
        use crate::message::Message;
        use crate::test_support::{record_stream, ts, SampleEvent};

        let dir = tempfile::tempdir().expect("tempdir");
        let stream = [
            Message::new(0, ts("2026-01-01T10:00:00Z"), SampleEvent::Tick),
            Message::new(1, ts("2026-01-01T10:30:00Z"), SampleEvent::Tick),
            // A later hour → a second rolled part.
            Message::new(2, ts("2026-01-01T12:15:30Z"), SampleEvent::Tick),
        ];
        record_stream(dir.path(), &stream);
        // Two parts were written (hour 10 and hour 12).
        assert_eq!(part_paths(dir.path()).expect("parts").len(), 2);

        let (start, end) = session_time_bounds(dir.path())
            .expect("bounds")
            .expect("non-empty");
        assert_eq!(start, ts("2026-01-01T10:00:00Z"));
        assert_eq!(end, ts("2026-01-01T12:15:30Z"));
    }

    /// A single-part session reports that part's own min/max as the bounds.
    #[test]
    fn session_time_bounds_single_part() {
        use crate::message::Message;
        use crate::test_support::{record_stream, ts, SampleEvent};

        let dir = tempfile::tempdir().expect("tempdir");
        record_stream(
            dir.path(),
            &[
                Message::new(0, ts("2026-01-01T10:00:00Z"), SampleEvent::Tick),
                Message::new(1, ts("2026-01-01T10:05:00Z"), SampleEvent::Tick),
            ],
        );
        assert_eq!(part_paths(dir.path()).expect("parts").len(), 1);

        let (start, end) = session_time_bounds(dir.path())
            .expect("bounds")
            .expect("non-empty");
        assert_eq!(start, ts("2026-01-01T10:00:00Z"));
        assert_eq!(end, ts("2026-01-01T10:05:00Z"));
    }

    /// The column-scan fallback (a part with no statistics) yields the same
    /// bounds as the statistics path. We write a part with statistics disabled
    /// and confirm the scan still finds the right min/max.
    #[test]
    fn session_time_bounds_falls_back_to_scan_without_stats() {
        use crate::message::Payload;
        use crate::test_support::{ts, SampleEvent};
        use arrow::array::{
            ArrayRef, BinaryArray, StringArray, TimestampMicrosecondArray, UInt64Array,
        };
        use arrow::record_batch::RecordBatch;
        use parquet::file::properties::EnabledStatistics;

        let dir = tempfile::tempdir().expect("tempdir");
        let times = ["2026-01-01T10:00:00Z", "2026-01-01T10:09:00Z"];
        let seq: ArrayRef = Arc::new(UInt64Array::from(vec![0u64, 1u64]));
        let timestamp: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(
                times
                    .iter()
                    .map(|t| ts(t).timestamp_micros())
                    .collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        );
        let type_name: ArrayRef =
            Arc::new(StringArray::from(vec![SampleEvent::Tick.type_name(); 2]));
        let blob = crate::encode_payload(&SampleEvent::Tick).expect("encode");
        let payload: ArrayRef = Arc::new(BinaryArray::from(vec![blob.as_slice(); 2]));
        let batch = RecordBatch::try_new(part_schema(), vec![seq, timestamp, type_name, payload])
            .expect("batch");

        let path = dir.path().join("20260101T100000Z.parquet");
        let file = std::fs::File::create(&path).expect("create");
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::None)
            .build();
        let mut writer = ArrowWriter::try_new(file, part_schema(), Some(props)).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");

        // Confirm there really are no usable statistics, so the scan path runs.
        let meta_file = std::fs::File::open(&path).expect("open");
        let meta = ParquetRecordBatchReaderBuilder::try_new(meta_file)
            .expect("builder")
            .metadata()
            .clone();
        assert!(
            stats_time_bounds(&meta).expect("stats").is_none(),
            "statistics should be disabled so the scan fallback is exercised"
        );

        let (start, end) = session_time_bounds(dir.path())
            .expect("bounds")
            .expect("non-empty");
        assert_eq!(start, ts("2026-01-01T10:00:00Z"));
        assert_eq!(end, ts("2026-01-01T10:09:00Z"));
    }
}
