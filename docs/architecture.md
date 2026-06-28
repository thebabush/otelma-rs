# Architecture

`otelma` is a small set of composable pieces around one central type,
`Message<T>`. The design goal is **faithful capture and deterministic replay**:
a recording can be replayed to bit-identical consumer state at any speed, forever.

## The pieces

```
  data source ──► adapter ──► Message<T> ──► Recorder ──► Parquet parts
                                  │
                                  ▼
                            SessionReader ──► drive / drive_realtime ──► Sink<T>
```

- **`Message<T>`** — the envelope: `seq` (u64), `timestamp` (`DateTime<Utc>`),
  and a user `payload: T`. Generic over `T`; the engine never needs editing to
  support a new payload type.
- **Adapter** (e.g. `otelma-polymarket`'s WS client) — the only code that reads
  the wall clock. It stamps `seq` and `timestamp` and emits `Message<T>`.
- **`Recorder`** — appends messages to hourly-rolled, ZSTD Parquet part files.
- **`SessionReader<T>`** — streaming reader that discovers and chains parts,
  decodes payloads back into `T`, and enforces the monotonicity invariant.
- **`drive` / `drive_realtime`** — feeders that pull from a reader and push into
  a sink: headless (as fast as possible) and real-time-paced respectively.
- **`Sink<T>`** — the consumer interface; computes only from `Message` contents.

## The adapter is the only wall-clock reader

Every other component — recorder, reader, feeders, sinks, the GUI — derives all
notion of time from `Message.timestamp` (or a `PlaybackControl` for pacing).
Concentrating wall-clock access at the adapter boundary is what makes the rest of
the system replayable. The adapter takes its clock as an injected
`Fn() -> DateTime<Utc>`, so even that single dependency is explicit and testable.

## The determinism contract

A `Sink<T>` must compute its state purely from message contents and must not read
the wall clock. Pacing and sleeping live exclusively in the feeders; they change
*when* a message is delivered, never *what* the sink computes from it.

Consequently `drive` and `drive_realtime` deliver exactly the same messages in
the same order — only the timing differs. Replaying the same recording produces
identical sink state whether run headless, at 10×, in real time, or paused. This
is the property that makes a recording a reliable test fixture: a bug observed in
production can be reproduced exactly by replaying the captured stream.

`PlaybackControl` (speed / pause / stop) is thread-safe via atomics, so a GUI
thread and a background feeder thread can share one over an `Arc`.

## Why these choices

**Parquet columns + opaque payload blob, not a fully columnar layout.** The
envelope fields (`seq`, `timestamp`, `type_name`) are real Parquet columns, so
common filtering and inspection (by time, by type) work with standard Parquet
tooling and no payload decode. The payload itself is one `Binary` column holding
a MessagePack blob. This is the key to genericity: the engine is generic over `T`
and the on-disk schema is fixed, so adding event types never migrates the schema.
A fully columnar layout would either lose that genericity or require per-payload
schema generation and evolution.

**MessagePack for the payload.** Compact, schemaless-on-the-wire, and a direct
`serde` target, so any `Serialize`/`Deserialize` type is recordable with no
codegen. `Decimal` is string-encoded inside the blob to round-trip losslessly.

**Hourly rolling.** The current part is buffered and written whole when it rolls,
so a crash loses at most the current hour rather than corrupting a long-running
file. Hour-aligned boundaries are deterministic (keyed on the message
timestamp's UTC hour, not wall-clock), and the reader chains parts transparently,
so rolling is invisible to consumers. A safety cap forces an early roll to bound
memory on pathological streams.

**Monotonicity enforced on read.** The adapter produces monotonic streams by
construction, but the reader re-checks `seq`/`timestamp` ordering anyway: a
recording is untrusted input, and a silent ordering regression would corrupt
every downstream computation. Crashing loudly on read is the safe default.

## Type-system discipline

Illegal states are made unrepresentable where it's cheap to do so: part-hour
buckets are hour-aligned by construction (constructible only by truncating a
timestamp), part indices own their zero-padded filename formatting, `Side` and
the event payloads are enums, and the venue parser follows parse-don't-validate
at the boundary (skip unknown shapes, crash on corrupt-known ones). Internal code
stays free of `isinstance`-style dispatch; the type system handles it.
