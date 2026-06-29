# On-disk format

A recorded session is a directory of rolled Parquet **part files**, each named by
the UTC start time of its first message (basic ISO 8601, `YYYYMMDDTHHMMSSZ`):

```
recordings/<session>/
├── 20260628T142311Z.parquet   # recording started 14:23:11
├── 20260628T150000Z.parquet   # rolled at the 15:00 UTC hour
└── ...
```

The names are fixed-width and colon-free, so they sort lexically into
chronological order and are valid filenames on every platform. A new part starts
each UTC hour (see [Rolling](#rolling)); the directory name itself is just a label
and carries no meaning.

## Schema

Every part file (and a compacted file) has the same Arrow schema:

| Column      | Arrow type                          | Meaning |
|-------------|-------------------------------------|---------|
| `seq`       | `UInt64`                            | Strictly-increasing sequence number across the whole session. |
| `timestamp` | `Timestamp(Microsecond, "UTC")`     | Event time, stored as epoch microseconds (not a string). |
| `type_name` | `Utf8`                              | Per-value type tag (e.g. `"Book"`, `"Trade"`, `"Market"`) for filtering without decoding the payload. |
| `payload`   | `Binary`                            | The user payload `T`, encoded as a MessagePack blob. |

Files are compressed with **ZSTD**.

## Payload encoding

The payload is an opaque [MessagePack](https://msgpack.org/) blob produced by
`serde` (`rmp-serde`). This keeps the engine generic over `T`: the on-disk schema
never changes when new event types are added — only the blob's contents do. The
reader reconstructs `Message<T>` by decoding the blob into a caller-chosen `T`.

`Decimal` prices are serialized as strings inside the blob, so values round-trip
losslessly (a numeric encoding would pass through `f64` and lose precision).

For the Polymarket payload, the adapter emits a `Market` variant (carrying
`MarketMeta`: question, outcome title, the Yes/No token ids, event title, slug)
as the **first** messages of a recording. These are ordinary stamped messages, so
a replay can label assets with human-readable text without any API call — the
recording stays self-contained and replay stays deterministic.

## Rolling

A new part rolls whenever an incoming message falls into a later **UTC-hour
bucket** than the currently open part (the bucket is the message timestamp
truncated to the hour). This yields hour-aligned, deterministic part files. Idle
hours simply produce no part. The current part is buffered in memory and written
as one Parquet file when it rolls or on close, so a crash loses at most the
current hour. A safety cap forces an early roll if the buffer grows pathologically
large; should such an early roll split a single second into two parts, the second
part's file name is nudged one second forward so names stay unique and ordered
(the rows keep their true timestamps).

## Monotonicity invariant

Across the whole session (part boundaries included):

- `seq` is **strictly increasing**, and
- `timestamp` is **non-decreasing**.

The WS adapter guarantees this at the source (strictly-increasing seq;
non-decreasing UTC timestamps, with a backwards clock clamped). The reader
**enforces** it on read and errors on any violation, so a corrupt or misordered
recording fails loudly rather than silently feeding bad data downstream.

## Compaction

`otelma compact <session>` merges the rolled parts into a single Parquet file
(default `<session>/compacted.parquet`) with the identical schema, preserving
order. It streams raw record batches straight through without decoding payloads,
so the result round-trips: reading it back yields the identical message stream.
(The reader only chains files whose names match the part convention, so a
`compacted.parquet` left in the session directory is ignored and won't be replayed
twice.)

## Reading from Python / Polars

The columns are standard Parquet, so the envelope is directly readable:

```python
import polars as pl
df = pl.read_parquet("recordings/<session>/20260628T142311Z.parquet")
# df has seq, timestamp (µs UTC), type_name, payload (bytes)
```

To inspect a payload, MessagePack-decode the `payload` bytes (e.g. with the
`msgpack` package). The decoded shape mirrors the Rust `T`'s `serde`
representation.
