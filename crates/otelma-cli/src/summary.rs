//! [`SummarySink`] — an `otelma::Sink<PolyEvent>` that tallies a replayed
//! stream into a human-readable end-of-run report.
//!
//! All logic lives here (pure, no I/O) so it is unit-testable; the `replay`
//! command just constructs it, drives the stream, and prints [`SummarySink::render`].

use std::collections::BTreeMap;
use std::fmt::Write as _;

use chrono::{DateTime, Utc};
use otelma::{Message, Payload, Sink};
use otelma_polymarket::{AssetId, PolyEvent, Price, Side};

/// Per-asset running tally derived from the stream.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct AssetSummary {
    /// Best (highest) bid from the most recent book, if any.
    pub best_bid: Option<Price>,
    /// Best (lowest) ask from the most recent book, if any.
    pub best_ask: Option<Price>,
    /// Number of trade events seen for this asset.
    pub trade_count: u64,
    /// Last trade price seen for this asset, if any carried a price.
    pub last_trade_price: Option<Price>,
}

/// Tallies messages applied during a replay.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct SummarySink {
    total: u64,
    per_type: BTreeMap<String, u64>,
    first_seq: Option<u64>,
    last_seq: Option<u64>,
    first_ts: Option<DateTime<Utc>>,
    last_ts: Option<DateTime<Utc>>,
    per_asset: BTreeMap<AssetId, AssetSummary>,
}

impl SummarySink {
    /// Create an empty sink.
    pub fn new() -> Self {
        Self::default()
    }

    /// First and last `seq` applied, if any.
    fn seq_range(&self) -> Option<(u64, u64)> {
        Some((self.first_seq?, self.last_seq?))
    }

    /// First and last `timestamp` applied, if any.
    fn time_range(&self) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
        Some((self.first_ts?, self.last_ts?))
    }

    /// Render the end-of-run report.
    pub fn render(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "=== otelma replay summary ===");
        let _ = writeln!(out, "messages: {}", self.total);

        if let Some((lo, hi)) = self.seq_range() {
            let _ = writeln!(out, "seq:      {lo} .. {hi}");
        }
        if let Some((start, end)) = self.time_range() {
            let dur = end - start;
            let _ = writeln!(
                out,
                "time:     {start} .. {end}  ({:.3}s)",
                dur.num_milliseconds() as f64 / 1000.0
            );
        }

        let _ = writeln!(out, "by type:");
        for (ty, count) in &self.per_type {
            let _ = writeln!(out, "  {ty:<12} {count}");
        }

        if !self.per_asset.is_empty() {
            let _ = writeln!(out, "by asset:");
            for (asset, a) in &self.per_asset {
                let bid = a
                    .best_bid
                    .map(|p| p.value().to_string())
                    .unwrap_or_else(|| "-".to_string());
                let ask = a
                    .best_ask
                    .map(|p| p.value().to_string())
                    .unwrap_or_else(|| "-".to_string());
                let last = a
                    .last_trade_price
                    .map(|p| p.value().to_string())
                    .unwrap_or_else(|| "-".to_string());
                let _ = writeln!(
                    out,
                    "  {asset}: bid={bid} ask={ask} trades={} last={last}",
                    a.trade_count
                );
            }
        }
        out
    }
}

impl Sink<PolyEvent> for SummarySink {
    fn apply(&mut self, msg: &Message<PolyEvent>) {
        self.total += 1;

        *self
            .per_type
            .entry(msg.payload.type_name().to_string())
            .or_default() += 1;

        if self.first_seq.is_none() {
            self.first_seq = Some(msg.seq);
            self.first_ts = Some(msg.timestamp);
        }
        self.last_seq = Some(msg.seq);
        self.last_ts = Some(msg.timestamp);

        match &msg.payload {
            PolyEvent::Book(book) => {
                let entry = self.per_asset.entry(book.asset_id.clone()).or_default();
                // The venue's level ordering is carried as received; don't assume
                // a side of the vec — compute the extremum: best bid is the max
                // price, best ask the min price.
                entry.best_bid = book.bids.iter().map(|l| l.price).max();
                entry.best_ask = book.asks.iter().map(|l| l.price).min();
            }
            PolyEvent::Trade(trade) => {
                let entry = self.per_asset.entry(trade.asset_id.clone()).or_default();
                entry.trade_count += 1;
                if let Some(price) = trade.price {
                    entry.last_trade_price = Some(price);
                }
                let _ = trade.side; // side not tallied; available on PolyEvent::Trade
            }
            // A price_change is a book-level change, not a trade: it must not
            // drive trade_count / last_trade_price. It still shows up in the
            // by-type tally above.
            PolyEvent::PriceChange(_) => {}
            PolyEvent::Connection { .. } => {}
        }
    }
}

/// Format a venue side for one-line output.
fn fmt_side(side: Option<Side>) -> &'static str {
    match side {
        Some(Side::Buy) => "BUY",
        Some(Side::Sell) => "SELL",
        None => "-",
    }
}

/// One-line debug rendering of a message (for `replay --print`).
pub fn render_line(msg: &Message<PolyEvent>) -> String {
    let (ty, detail) = match &msg.payload {
        PolyEvent::Book(b) => {
            let bid = b.bids.iter().map(|l| l.price).max();
            let ask = b.asks.iter().map(|l| l.price).min();
            (
                "Book",
                format!(
                    "{} bid={} ask={}",
                    b.asset_id,
                    bid.map(|p| p.value().to_string())
                        .unwrap_or_else(|| "-".into()),
                    ask.map(|p| p.value().to_string())
                        .unwrap_or_else(|| "-".into()),
                ),
            )
        }
        PolyEvent::Trade(t) => (
            "Trade",
            format!(
                "{} price={} size={} side={}",
                t.asset_id,
                t.price
                    .map(|p| p.value().to_string())
                    .unwrap_or_else(|| "-".into()),
                t.size
                    .map(|s| s.value().to_string())
                    .unwrap_or_else(|| "-".into()),
                fmt_side(t.side),
            ),
        ),
        PolyEvent::PriceChange(c) => (
            "PriceChange",
            format!(
                "{} price={} size={} side={}",
                c.asset_id,
                c.price
                    .map(|p| p.value().to_string())
                    .unwrap_or_else(|| "-".into()),
                c.size
                    .map(|s| s.value().to_string())
                    .unwrap_or_else(|| "-".into()),
                fmt_side(c.side),
            ),
        ),
        PolyEvent::Connection { connected } => ("Connection", format!("connected={connected}")),
    };
    format!("{:>8}  {}  {:<10}  {}", msg.seq, msg.timestamp, ty, detail)
}

#[cfg(test)]
mod tests {
    use super::*;
    use otelma_polymarket::{BookUpdate, Level, PriceChange, Size, Trade};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn dt(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).expect("valid")
    }

    fn price(d: Decimal) -> Price {
        Price::new(d).expect("non-negative")
    }

    fn lvl(p: Decimal, s: Decimal) -> Level {
        Level {
            price: price(p),
            size: Size::new(s).expect("non-negative"),
        }
    }

    fn asset(id: &str) -> AssetId {
        AssetId::from(id)
    }

    fn book(
        seq: u64,
        secs: i64,
        id: &str,
        bids: Vec<Level>,
        asks: Vec<Level>,
    ) -> Message<PolyEvent> {
        Message::new(
            seq,
            dt(secs),
            PolyEvent::Book(BookUpdate {
                asset_id: id.into(),
                bids,
                asks,
                market: None,
                exchange_ts_millis: None,
            }),
        )
    }

    fn trade(seq: u64, secs: i64, id: &str, p: Option<Decimal>) -> Message<PolyEvent> {
        Message::new(
            seq,
            dt(secs),
            PolyEvent::Trade(Trade {
                asset_id: id.into(),
                price: p.map(price),
                size: Some(Size::new(dec!(1)).expect("non-negative")),
                side: Some(Side::Buy),
            }),
        )
    }

    #[test]
    fn tallies_counts_and_ranges() {
        let mut sink = SummarySink::new();
        let msgs = vec![
            Message::new(0, dt(100), PolyEvent::Connection { connected: true }),
            book(
                1,
                110,
                "A",
                vec![lvl(dec!(0.50), dec!(10)), lvl(dec!(0.52), dec!(5))],
                vec![lvl(dec!(0.55), dec!(8)), lvl(dec!(0.54), dec!(3))],
            ),
            trade(2, 120, "A", Some(dec!(0.53))),
            trade(3, 130, "A", Some(dec!(0.531))),
        ];
        for m in &msgs {
            sink.apply(m);
        }

        assert_eq!(sink.total, 4);
        assert_eq!(sink.per_type["Connection"], 1);
        assert_eq!(sink.per_type["Book"], 1);
        assert_eq!(sink.per_type["Trade"], 2);
        assert_eq!(sink.seq_range(), Some((0, 3)));
        assert_eq!(sink.time_range(), Some((dt(100), dt(130))));

        let a = &sink.per_asset[&asset("A")];
        // best bid = max(0.50, 0.52) = 0.52; best ask = min(0.55, 0.54) = 0.54.
        assert_eq!(a.best_bid, Some(price(dec!(0.52))));
        assert_eq!(a.best_ask, Some(price(dec!(0.54))));
        assert_eq!(a.trade_count, 2);
        assert_eq!(a.last_trade_price, Some(price(dec!(0.531))));
    }

    #[test]
    fn best_bid_ask_ignore_vec_ordering() {
        // Bids/asks given in arbitrary order — extremum must still be correct.
        let mut sink = SummarySink::new();
        sink.apply(&book(
            0,
            1,
            "Z",
            vec![lvl(dec!(0.10), dec!(1)), lvl(dec!(0.90), dec!(1))],
            vec![lvl(dec!(0.99), dec!(1)), lvl(dec!(0.91), dec!(1))],
        ));
        let z = &sink.per_asset[&asset("Z")];
        assert_eq!(z.best_bid, Some(price(dec!(0.90))));
        assert_eq!(z.best_ask, Some(price(dec!(0.91))));
    }

    #[test]
    fn empty_sink_renders_without_panic() {
        let sink = SummarySink::new();
        let report = sink.render();
        assert!(report.contains("messages: 0"));
    }

    fn price_change(seq: u64, secs: i64, id: &str, p: Option<Decimal>) -> Message<PolyEvent> {
        Message::new(
            seq,
            dt(secs),
            PolyEvent::PriceChange(PriceChange {
                asset_id: id.into(),
                price: p.map(price),
                size: Some(Size::new(dec!(1)).expect("non-negative")),
                side: Some(Side::Sell),
            }),
        )
    }

    #[test]
    fn price_change_tallies_by_type_but_is_not_a_trade() {
        let mut sink = SummarySink::new();
        sink.apply(&trade(0, 1, "A", Some(dec!(0.40))));
        sink.apply(&price_change(1, 2, "A", Some(dec!(0.99))));

        // Both events are counted in the by-type tally.
        assert_eq!(sink.per_type["Trade"], 1);
        assert_eq!(sink.per_type["PriceChange"], 1);

        // But only the trade drives trade_count / last_trade_price; the
        // price_change must not touch either.
        let a = &sink.per_asset[&asset("A")];
        assert_eq!(a.trade_count, 1);
        assert_eq!(a.last_trade_price, Some(price(dec!(0.40))));
    }

    #[test]
    fn trade_without_price_keeps_prior_last() {
        let mut sink = SummarySink::new();
        sink.apply(&trade(0, 1, "A", Some(dec!(0.4))));
        sink.apply(&trade(1, 2, "A", None));
        let a = &sink.per_asset[&asset("A")];
        assert_eq!(a.trade_count, 2);
        assert_eq!(a.last_trade_price, Some(price(dec!(0.4))));
    }
}
