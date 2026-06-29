//! Low-level part-file utilities: the shared on-disk schema, part discovery,
//! and raw-batch compaction.
//!
//! These operate on the Parquet columns directly (`seq`, `timestamp`,
//! `type_name`, `payload`) without decoding the payload blob, so they are
//! generic over the payload type `T`. The high-level [`crate::SessionReader`]
//! builds on the same part ordering.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, Utc};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::error::Error;

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
}
