//! Deterministic synthetic demo-session generator.
//!
//! Produces a genuine recording (via the real [`Recorder`]) of ~2 assets whose
//! best bid/ask random-walk over a simulated hour, with occasional trades. It
//! is fully deterministic given the seed and uses only synthetic UTC timestamps
//! (no wall-clock), so `cargo run` shows moving plots with zero setup.

use std::path::Path;

use chrono::{DateTime, TimeZone, Utc};
use otelma::{Message, Recorder};
use otelma_polymarket::{BookUpdate, Level, PolyEvent, Side, Trade};
use rust_decimal::Decimal;

/// The asset ids used by the demo session.
pub const DEMO_ASSETS: [&str; 2] = ["DEMO-YES", "DEMO-NO"];

/// A tiny deterministic PRNG (xorshift64*), so the demo never depends on the
/// `rand` crate or wall-clock.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // Avoid the zero fixed-point.
        Rng(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// A float in `[0, 1)`.
    fn next_unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// A step in `{-1, 0, +1}`.
    fn next_step(&mut self) -> i64 {
        match self.next_u64() % 3 {
            0 => -1,
            1 => 0,
            _ => 1,
        }
    }
}

/// Convert a price expressed in integer ticks (1 tick = 0.001) to a `Decimal`.
fn ticks_to_price(ticks: i64) -> Decimal {
    Decimal::new(ticks, 3)
}

/// Generate a deterministic demo session into `dir` and return the message
/// count written. The session spans one simulated hour starting at
/// `1970-01-01 00:00:00 UTC`, sampling each asset's book once per simulated
/// minute with occasional trades.
pub fn generate_demo_session(dir: impl AsRef<Path>, seed: u64) -> Result<u64, otelma::Error> {
    let messages = build_demo_messages(seed);
    let mut rec = Recorder::new(dir.as_ref())?;
    for msg in &messages {
        rec.record(msg)?;
    }
    rec.close()?;
    Ok(messages.len() as u64)
}

/// Build the demo message stream (pure — no I/O), so it can be unit-tested and
/// reused by the generator. Messages are globally ordered by `seq` and have
/// non-decreasing UTC timestamps.
pub fn build_demo_messages(seed: u64) -> Vec<Message<PolyEvent>> {
    let base: DateTime<Utc> = Utc.timestamp_opt(0, 0).single().expect("epoch");
    let mut rng = Rng::new(seed);

    // Per-asset mid in ticks; start near the middle of the [0,1] range.
    let mut mids: [i64; 2] = [500, 480];
    let half_spread = 5i64; // 0.005

    let mut out: Vec<Message<PolyEvent>> = Vec::new();
    let mut seq: u64 = 0;

    out.push(Message::new(
        seq,
        base,
        PolyEvent::Connection { connected: true },
    ));
    seq += 1;

    // One simulated hour, one sample per minute per asset.
    for minute in 0..60i64 {
        let t = base + chrono::TimeDelta::seconds(minute * 60);

        for (i, asset) in DEMO_ASSETS.iter().enumerate() {
            // Random-walk the mid, clamped into a sane open interval.
            let step = rng.next_step() * 3;
            mids[i] = (mids[i] + step).clamp(50, 950);
            let mid = mids[i];

            let bid = mid - half_spread;
            let ask = mid + half_spread;

            // Two levels each side, depth thinning away from top of book.
            let bids = vec![
                Level {
                    price: ticks_to_price(bid),
                    size: Decimal::new(100, 0),
                },
                Level {
                    price: ticks_to_price(bid - 2),
                    size: Decimal::new(250, 0),
                },
            ];
            let asks = vec![
                Level {
                    price: ticks_to_price(ask),
                    size: Decimal::new(90, 0),
                },
                Level {
                    price: ticks_to_price(ask + 2),
                    size: Decimal::new(220, 0),
                },
            ];

            out.push(Message::new(
                seq,
                t,
                PolyEvent::Book(BookUpdate {
                    asset_id: (*asset).to_string(),
                    bids,
                    asks,
                    market: Some("0xDEMO".to_string()),
                    exchange_ts_millis: Some(t.timestamp_millis()),
                }),
            ));
            seq += 1;

            // ~30% of minutes produce a trade at the mid.
            if rng.next_unit() < 0.30 {
                let side = if rng.next_u64().is_multiple_of(2) {
                    Side::Buy
                } else {
                    Side::Sell
                };
                out.push(Message::new(
                    seq,
                    t,
                    PolyEvent::Trade(Trade {
                        asset_id: (*asset).to_string(),
                        price: Some(ticks_to_price(mid)),
                        size: Some(Decimal::new(10, 0)),
                        side: Some(side),
                    }),
                ));
                seq += 1;
            }
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use otelma::{Payload, SessionReader};
    use tempfile::tempdir;

    #[test]
    fn build_demo_is_deterministic() {
        let a = build_demo_messages(42);
        let b = build_demo_messages(42);
        assert_eq!(a, b);
        // Different seeds diverge.
        let c = build_demo_messages(7);
        assert_ne!(a, c);
    }

    #[test]
    fn build_demo_is_monotonic_and_mixed() {
        let msgs = build_demo_messages(42);
        assert!(msgs.len() > 60, "expected a substantial stream");

        let mut last_seq: Option<u64> = None;
        let mut last_ts: Option<DateTime<Utc>> = None;
        let mut saw_book = false;
        let mut saw_trade = false;

        for m in &msgs {
            if let Some(prev) = last_seq {
                assert!(m.seq > prev, "seq must strictly increase");
            }
            if let Some(prev) = last_ts {
                assert!(m.timestamp >= prev, "timestamps must be non-decreasing");
            }
            last_seq = Some(m.seq);
            last_ts = Some(m.timestamp);
            match m.payload.type_name() {
                "Book" => saw_book = true,
                "Trade" => saw_trade = true,
                _ => {}
            }
        }
        assert!(saw_book && saw_trade, "demo must contain books and trades");
    }

    #[test]
    fn generated_session_reads_back() {
        let dir = tempdir().expect("tempdir");
        let count = generate_demo_session(dir.path(), 42).expect("generate");

        let read: Vec<Message<PolyEvent>> = SessionReader::<PolyEvent>::open(dir.path())
            .expect("open")
            .collect::<Result<_, _>>()
            .expect("read (also asserts the reader's monotonicity guard)");

        assert_eq!(read.len() as u64, count);
        assert_eq!(read, build_demo_messages(42));
    }
}
