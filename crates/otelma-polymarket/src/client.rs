//! Live Polymarket CLOB **market** WebSocket client — the only networking and
//! the only wall-clock-reading code in the crate (it is *the* adapter).
//!
//! The client connects, subscribes to a caller-supplied set of `asset_id`s,
//! parses each inbound frame via [`crate::parse_ws_frame`], stamps every event
//! into an [`otelma::Message`], and emits it on an mpsc channel. It reconnects
//! with exponential backoff and shuts down promptly on a [`CancellationToken`].
//!
//! # Determinism boundary
//!
//! This is the one place that reads the wall clock, and even that is injected
//! (`clock: impl Fn() -> DateTime<Utc>`, default [`chrono::Utc::now`]) so tests
//! are deterministic. The [`Stamper`] guarantees the invariant
//! [`otelma::SessionReader`] enforces: `seq` is strictly increasing across the
//! whole run (reconnects included) and timestamps are non-decreasing — a
//! backwards clock (NTP step-back) is clamped to the last stamp. Recordings
//! this client produces therefore always pass the reader's monotonicity guard.
//! A small backstep is clamped silently and a larger one with a warning, but a
//! backstep beyond the fatal tolerance aborts capture outright (the recorded
//! timeline can no longer be trusted) — the one capture-boundary failure that is
//! deliberately *not* resilient.

use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use futures_util::{SinkExt, StreamExt};
use otelma::{classify_backstep, Backstep, Message};
use serde::Serialize;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_util::sync::CancellationToken;

use crate::event::{MarketMeta, PolyEvent};
use crate::parser::parse_ws_frame;

/// Default Polymarket CLOB market WebSocket URL.
pub const DEFAULT_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// Keepalive interval: send `PING` this often.
const PING_INTERVAL: Duration = Duration::from_secs(10);

/// Reconnect backoff bounds.
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(60);

/// Backward wall-clock jumps larger than this are surfaced with a warning;
/// smaller ones are treated as ordinary jitter and clamped silently.
const CLOCK_BACKSTEP_WARN_SECS: i64 = 1;
/// Backward wall-clock jumps larger than this abort capture: the clock moved so
/// far back that the recorded timeline can no longer be trusted.
const CLOCK_BACKSTEP_FATAL_SECS: i64 = 60;

/// Errors from the WS client.
#[derive(Debug, Error)]
pub enum Error {
    /// The WebSocket transport failed (connect/read/write).
    #[error("websocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    /// The downstream receiver was dropped; nothing left to emit to.
    #[error("emit channel closed")]
    ChannelClosed,

    /// Failed to serialize the subscribe message.
    #[error("subscribe encode error: {0}")]
    Subscribe(#[from] serde_json::Error),

    /// The wall clock stepped backward by more than capture can tolerate; the
    /// recorded timeline can no longer be trusted, so capture is aborted. Unlike
    /// a transport error, this does not trigger a reconnect.
    #[error("wall clock stepped backward by {by} beyond tolerance; aborting capture")]
    ClockBackstep {
        /// How far the clock moved backward.
        by: TimeDelta,
    },
}

/// The market-subscription message sent on connect.
///
/// Note the venue's exact spelling `assets_ids` (not `asset_ids`).
#[derive(Debug, Serialize)]
struct SubscribeMessage<'a> {
    assets_ids: &'a [String],
    #[serde(rename = "type")]
    kind: &'static str,
}

/// Build the JSON subscribe frame for `asset_ids`. Pure and unit-testable.
pub fn subscribe_message(asset_ids: &[String]) -> Result<String, serde_json::Error> {
    serde_json::to_string(&SubscribeMessage {
        assets_ids: asset_ids,
        kind: "market",
    })
}

/// Monotonic message stamper: owns the run-wide `seq` counter, the last stamped
/// timestamp, and the (injectable) clock. Factored out so stamping is testable
/// without a socket.
///
/// Guarantees: `seq` strictly increases; timestamps never decrease (a backwards
/// clock is clamped to the previous timestamp). A backwards step beyond the
/// fatal tolerance is refused outright (see [`Stamper::stamp`]).
pub struct Stamper<C> {
    clock: C,
    seq: u64,
    last_ts: Option<DateTime<Utc>>,
}

impl<C: Fn() -> DateTime<Utc>> Stamper<C> {
    /// Create a stamper using `clock` as its time source.
    pub fn new(clock: C) -> Self {
        Self {
            clock,
            seq: 0,
            last_ts: None,
        }
    }

    /// Wrap `payload` in a stamped [`Message`], advancing seq and the clamped
    /// timestamp.
    ///
    /// A wall clock that steps backward is clamped forward to the previous
    /// instant so the timeline stays non-decreasing. A small backstep (jitter)
    /// is silent; a larger one is warned about; a backstep beyond
    /// [`CLOCK_BACKSTEP_FATAL_SECS`] returns [`Error::ClockBackstep`] without
    /// advancing seq or the timeline — the clock is wrong enough that the
    /// capture can no longer be trusted.
    pub fn stamp(&mut self, payload: PolyEvent) -> Result<Message<PolyEvent>, Error> {
        let raw = (self.clock)();
        match classify_backstep(
            self.last_ts,
            raw,
            TimeDelta::seconds(CLOCK_BACKSTEP_WARN_SECS),
            TimeDelta::seconds(CLOCK_BACKSTEP_FATAL_SECS),
        ) {
            Backstep::None | Backstep::Tolerated { .. } => {}
            Backstep::Notable { by } => {
                tracing::warn!(
                    backstep_ms = by.num_milliseconds(),
                    "wall clock stepped backward; clamping and continuing"
                );
            }
            Backstep::Excessive { by } => {
                tracing::error!(
                    backstep_ms = by.num_milliseconds(),
                    "wall clock stepped backward beyond tolerance; aborting capture"
                );
                return Err(Error::ClockBackstep { by });
            }
        }

        let ts = match self.last_ts {
            Some(prev) if raw < prev => prev,
            _ => raw,
        };
        let seq = self.seq;
        self.seq += 1;
        self.last_ts = Some(ts);
        Ok(Message::new(seq, ts, payload))
    }
}

/// Reconnecting Polymarket market WS client.
pub struct PolymarketClient<C = fn() -> DateTime<Utc>> {
    url: String,
    asset_ids: Vec<String>,
    markets: Vec<MarketMeta>,
    clock: C,
}

impl PolymarketClient {
    /// Create a client for `asset_ids` against the default URL, using
    /// [`chrono::Utc::now`] as the clock.
    pub fn new(asset_ids: Vec<String>) -> Self {
        Self::with_url(DEFAULT_URL.to_string(), asset_ids)
    }

    /// Create a client against an explicit URL (e.g. a local mock server).
    pub fn with_url(url: String, asset_ids: Vec<String>) -> Self {
        Self {
            url,
            asset_ids,
            markets: Vec::new(),
            clock: Utc::now,
        }
    }
}

impl<C: Fn() -> DateTime<Utc>> PolymarketClient<C> {
    /// Replace the clock (e.g. with a deterministic clock in tests).
    pub fn with_clock<C2: Fn() -> DateTime<Utc>>(self, clock: C2) -> PolymarketClient<C2> {
        PolymarketClient {
            url: self.url,
            asset_ids: self.asset_ids,
            markets: self.markets,
            clock,
        }
    }

    /// Attach market metadata to be emitted (as [`PolyEvent::Market`]) as the
    /// very first messages of the recording, before any connection is made.
    pub fn with_markets(mut self, markets: Vec<MarketMeta>) -> Self {
        self.markets = markets;
        self
    }

    /// Run the client: connect, subscribe, emit stamped messages, reconnect on
    /// failure with exponential backoff, until `shutdown` fires.
    ///
    /// Returns `Ok(())` on clean shutdown, or `Err` only if the downstream
    /// channel closes (no point continuing). Transport errors are operational —
    /// they trigger a reconnect, not a return.
    pub async fn run(
        self,
        tx: mpsc::Sender<Message<PolyEvent>>,
        shutdown: CancellationToken,
    ) -> Result<(), Error> {
        let PolymarketClient {
            url,
            asset_ids,
            markets,
            clock,
        } = self;
        let mut stamper = Stamper::new(clock);

        // Emit market metadata first so a recording is self-contained: these get
        // seq 0..N-1, before any connection marker or venue data.
        emit_market_metadata(&mut stamper, &markets, &tx).await?;

        let mut backoff = BACKOFF_MIN;

        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }

            let mut was_connected = false;
            let mut saw_data = false;
            let session = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return Ok(()),
                result = run_session(
                    &url, &asset_ids, &tx, &mut stamper, &shutdown,
                    &mut was_connected, &mut saw_data,
                ) => result,
            };

            match session {
                Ok(SessionEnd::Shutdown) => return Ok(()),
                Ok(SessionEnd::ChannelClosed) => return Err(Error::ChannelClosed),
                Ok(SessionEnd::Disconnected) => {
                    // Only reset the backoff after a *stable* session — one that
                    // actually delivered real data. A server that accepts then
                    // instantly closes (no data) must keep backing off, so a
                    // flapping endpoint isn't hammered at 1 Hz.
                    if saw_data {
                        backoff = BACKOFF_MIN;
                    }
                }
                Err(e) => {
                    // A backwards-clock abort is terminal — the recorded timeline
                    // is untrustworthy, so stop rather than reconnect. Every other
                    // session error is operational: back off and retry.
                    if matches!(e, Error::ClockBackstep { .. }) {
                        return Err(e);
                    }
                    tracing::warn!(error = %e, "polymarket ws session error; reconnecting");
                }
            }

            // Only emit a disconnect marker if the session actually reached the
            // connected state — a failed initial connect (or a retry during a
            // prolonged outage) never emitted `Connection{true}`, so emitting a
            // `false` would be misleading noise.
            if was_connected {
                let marker = stamper.stamp(PolyEvent::Connection { connected: false })?;
                if emit(&tx, marker).await.is_err() {
                    return Err(Error::ChannelClosed);
                }
            }

            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return Ok(()),
                _ = tokio::time::sleep(backoff) => {}
            }
            backoff = backoff.saturating_mul(2).min(BACKOFF_MAX);
        }
    }
}

/// One connect→subscribe→stream cycle. Returns how the session ended; a
/// transport `Err` bubbles up to trigger backoff in [`PolymarketClient::run`].
async fn run_session<C: Fn() -> DateTime<Utc>>(
    url: &str,
    asset_ids: &[String],
    tx: &mpsc::Sender<Message<PolyEvent>>,
    stamper: &mut Stamper<C>,
    shutdown: &CancellationToken,
    was_connected: &mut bool,
    saw_data: &mut bool,
) -> Result<SessionEnd, Error> {
    let (mut ws, _resp) = tokio_tungstenite::connect_async(url).await?;

    // Subscribe, then announce connectivity.
    ws.send(WsMessage::text(subscribe_message(asset_ids)?))
        .await?;
    let connect_marker = stamper.stamp(PolyEvent::Connection { connected: true })?;
    if emit(tx, connect_marker).await.is_err() {
        return Ok(SessionEnd::ChannelClosed);
    }
    *was_connected = true;

    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(SessionEnd::Shutdown),
            _ = ping.tick() => {
                ws.send(WsMessage::text("PING")).await?;
            }
            frame = ws.next() => {
                match frame {
                    None => return Ok(SessionEnd::Disconnected),
                    Some(frame) => {
                        if !handle_frame(frame?, tx, stamper, saw_data).await? {
                            return Ok(SessionEnd::ChannelClosed);
                        }
                    }
                }
            }
        }
    }
}

/// Process one inbound WS frame. Returns `Ok(false)` if the channel closed.
async fn handle_frame<C: Fn() -> DateTime<Utc>>(
    frame: WsMessage,
    tx: &mpsc::Sender<Message<PolyEvent>>,
    stamper: &mut Stamper<C>,
    saw_data: &mut bool,
) -> Result<bool, Error> {
    let text = match frame {
        WsMessage::Text(t) => t.to_string(),
        // Some servers wrap payloads in binary frames.
        WsMessage::Binary(b) => match String::from_utf8(b.to_vec()) {
            Ok(t) => t,
            Err(_) => return Ok(true), // non-text binary: ignore
        },
        WsMessage::Close(_) => return Ok(true),
        // Ping/Pong/Frame: protocol-level, nothing to emit.
        _ => return Ok(true),
    };

    // The parser is strict ("crash on corrupt-known"), but the live adapter is
    // the resilience boundary: a single unparseable frame (mis-modeled or
    // corrupt venue shape) must not tear down the socket and trigger a
    // reconnect storm. Log it and keep capturing everything else.
    let events = match parse_ws_frame(&text) {
        Ok(events) => events,
        Err(parse_err) => {
            tracing::warn!(
                error = %parse_err,
                frame = %truncate_frame(&text),
                "skipping unparseable polymarket frame"
            );
            return Ok(true);
        }
    };

    for event in events {
        // Every event the parser produces is real venue data (Book / Trade /
        // PriceChange — never a synthetic Connection), so reaching here means
        // the session delivered data and is "stable".
        *saw_data = true;
        let msg = stamper.stamp(event)?;
        if emit(tx, msg).await.is_err() {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Truncate a frame to a sane length for logging so a huge frame can't spam.
fn truncate_frame(text: &str) -> String {
    const MAX: usize = 200;
    if text.len() <= MAX {
        text.to_string()
    } else {
        // Respect char boundaries when slicing.
        let end = text
            .char_indices()
            .take_while(|(i, _)| *i < MAX)
            .last()
            .map(|(i, c)| i + c.len_utf8())
            .unwrap_or(0);
        format!("{}…(truncated)", &text[..end])
    }
}

/// How a single session ended.
enum SessionEnd {
    /// Shutdown token fired.
    Shutdown,
    /// Server closed / stream ended; reconnect.
    Disconnected,
    /// Downstream receiver dropped; stop entirely.
    ChannelClosed,
}

/// Emit one [`PolyEvent::Market`] per market via `stamper`, in order, before any
/// other message. These become the recording's first messages (seq 0..N-1).
///
/// `stamp` is fallible (a `ClockBackstep`), so it is propagated with `?`; at
/// startup the stamper's `last_ts` is `None`, so no backstep can occur here.
async fn emit_market_metadata<C: Fn() -> DateTime<Utc>>(
    stamper: &mut Stamper<C>,
    markets: &[MarketMeta],
    tx: &mpsc::Sender<Message<PolyEvent>>,
) -> Result<(), Error> {
    for meta in markets {
        let msg = stamper.stamp(PolyEvent::Market(meta.clone()))?;
        if emit(tx, msg).await.is_err() {
            return Err(Error::ChannelClosed);
        }
    }
    Ok(())
}

/// Send a message, mapping a closed channel to a unit error.
async fn emit(tx: &mpsc::Sender<Message<PolyEvent>>, msg: Message<PolyEvent>) -> Result<(), ()> {
    tx.send(msg).await.map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::BookUpdate;

    fn dt(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).expect("valid timestamp")
    }

    #[test]
    fn subscribe_message_uses_assets_ids_spelling() {
        let ids = vec!["tok-1".to_string(), "tok-2".to_string()];
        let msg = subscribe_message(&ids).expect("encode");
        assert_eq!(msg, r#"{"assets_ids":["tok-1","tok-2"],"type":"market"}"#);
    }

    #[test]
    fn stamper_seq_strictly_increases() {
        let mut stamper = Stamper::new(|| dt(100));
        let a = stamper
            .stamp(PolyEvent::Connection { connected: true })
            .expect("a");
        let b = stamper
            .stamp(PolyEvent::Connection { connected: false })
            .expect("b");
        let c = stamper
            .stamp(PolyEvent::Connection { connected: true })
            .expect("c");
        assert_eq!((a.seq, b.seq, c.seq), (0, 1, 2));
    }

    #[test]
    fn stamper_clamps_backwards_clock() {
        // A clock that steps backwards after the first call.
        let times = std::sync::Mutex::new(vec![dt(100), dt(50), dt(200)].into_iter());
        let clock = || times.lock().expect("lock").next().expect("time");
        let mut stamper = Stamper::new(clock);

        let a = stamper
            .stamp(PolyEvent::Connection { connected: true })
            .expect("a");
        let b = stamper
            .stamp(PolyEvent::Connection { connected: false })
            .expect("b");
        let c = stamper
            .stamp(PolyEvent::Connection { connected: true })
            .expect("c");

        assert_eq!(a.timestamp, dt(100));
        // 50 < 100, a 50s backstep → within the fatal bound, so it warns and
        // clamps to 100 (non-decreasing) rather than aborting.
        assert_eq!(b.timestamp, dt(100));
        // 200 > 100 → accepted.
        assert_eq!(c.timestamp, dt(200));
        // And seq still strictly increases regardless of the clamp.
        assert_eq!((a.seq, b.seq, c.seq), (0, 1, 2));
    }

    #[test]
    fn stamper_aborts_on_excessive_backstep() {
        // First sample, then a clock that jumps back well beyond the fatal bound,
        // then a recovered forward sample.
        let times = std::sync::Mutex::new(vec![dt(10_000), dt(9_400), dt(10_001)].into_iter());
        let clock = || times.lock().expect("lock").next().expect("time");
        let mut stamper = Stamper::new(clock);

        let first = stamper
            .stamp(PolyEvent::Connection { connected: true })
            .expect("first ok");
        assert_eq!((first.seq, first.timestamp), (0, dt(10_000)));

        // A 600s backstep exceeds the fatal bound → error, and the failed stamp
        // must not consume a seq or advance the timeline.
        let err = stamper
            .stamp(PolyEvent::Connection { connected: false })
            .expect_err("excessive backstep must abort");
        assert!(matches!(err, Error::ClockBackstep { .. }));

        // The next valid sample is therefore still seq 1, clamped against the
        // last *accepted* instant (10_000), not the rejected one.
        let next = stamper
            .stamp(PolyEvent::Connection { connected: true })
            .expect("recovered ok");
        assert_eq!((next.seq, next.timestamp), (1, dt(10_001)));
    }

    #[test]
    fn stamper_fixed_clock_is_non_decreasing() {
        let mut stamper = Stamper::new(|| dt(42));
        let msgs: Vec<_> = (0..5)
            .map(|_| {
                stamper
                    .stamp(PolyEvent::Connection { connected: true })
                    .expect("stamp")
            })
            .collect();
        for w in msgs.windows(2) {
            assert!(w[1].seq > w[0].seq);
            assert!(w[1].timestamp >= w[0].timestamp);
        }
    }

    #[test]
    fn stamper_carries_payload() {
        let mut stamper = Stamper::new(|| dt(1));
        let book = PolyEvent::Book(BookUpdate {
            asset_id: "x".into(),
            bids: vec![],
            asks: vec![],
            market: None,
            exchange_ts_millis: None,
        });
        let msg = stamper.stamp(book.clone()).expect("stamp");
        assert_eq!(msg.payload, book);
    }

    #[tokio::test]
    async fn emit_market_metadata_emits_markets_in_order_with_seq_from_zero() {
        use crate::testing::market_meta;

        let markets = vec![
            market_meta("Argentina", "y-arg", "n-arg", Some("World Cup")),
            market_meta("Brazil", "y-bra", "n-bra", Some("World Cup")),
        ];
        let (tx, mut rx) = mpsc::channel(8);
        let mut stamper = Stamper::new(|| dt(1));

        emit_market_metadata(&mut stamper, &markets, &tx)
            .await
            .expect("emit ok");

        // Exactly the markets, in order, as PolyEvent::Market, seq 0..N-1.
        let first = rx.try_recv().expect("first");
        let second = rx.try_recv().expect("second");
        assert!(rx.try_recv().is_err(), "only the two markets are emitted");

        assert_eq!(first.seq, 0);
        assert_eq!(second.seq, 1);
        let PolyEvent::Market(m0) = &first.payload else {
            panic!("expected Market, got {:?}", first.payload);
        };
        let PolyEvent::Market(m1) = &second.payload else {
            panic!("expected Market, got {:?}", second.payload);
        };
        assert_eq!(m0.outcome_title, "Argentina");
        assert_eq!(m1.outcome_title, "Brazil");

        // The stamper's seq counter is left at 2, so subsequent events follow.
        let next = stamper
            .stamp(PolyEvent::Connection { connected: true })
            .expect("next");
        assert_eq!(next.seq, 2);
    }

    #[tokio::test]
    async fn emit_market_metadata_empty_is_noop() {
        let (tx, mut rx) = mpsc::channel(4);
        let mut stamper = Stamper::new(|| dt(1));
        emit_market_metadata(&mut stamper, &[], &tx)
            .await
            .expect("emit ok");
        assert!(rx.try_recv().is_err(), "nothing emitted for empty markets");
    }

    #[tokio::test]
    async fn handle_frame_skips_corrupt_known_frame() {
        // A recognized `book` event with a non-numeric price is corrupt-known:
        // the pure parser errors, but the live adapter must log+skip and keep
        // the session alive (return Ok(true)) without emitting anything.
        let corrupt =
            r#"{"event_type":"book","asset_id":"t","bids":[{"price":"NaN","size":"1"}],"asks":[]}"#;
        let (tx, mut rx) = mpsc::channel(4);
        let mut stamper = Stamper::new(|| dt(1));
        let mut saw_data = false;

        let kept_alive = handle_frame(WsMessage::text(corrupt), &tx, &mut stamper, &mut saw_data)
            .await
            .expect("handle_frame ok");

        assert!(kept_alive, "corrupt frame must not tear down the session");
        // Nothing emitted, and seq counter untouched.
        assert!(rx.try_recv().is_err(), "no message should be emitted");
        // A corrupt-known frame delivers no real data → not a stable session.
        assert!(
            !saw_data,
            "skipped frame must not mark the session as stable"
        );
    }

    #[tokio::test]
    async fn handle_frame_emits_parsed_events() {
        let book =
            r#"{"event_type":"book","asset_id":"t","bids":[{"price":"0.5","size":"1"}],"asks":[]}"#;
        let (tx, mut rx) = mpsc::channel(4);
        let mut stamper = Stamper::new(|| dt(7));
        let mut saw_data = false;

        let kept_alive = handle_frame(WsMessage::text(book), &tx, &mut stamper, &mut saw_data)
            .await
            .expect("handle_frame ok");

        assert!(kept_alive);
        let msg = rx.try_recv().expect("one message");
        assert_eq!(msg.seq, 0);
        assert!(matches!(msg.payload, PolyEvent::Book(_)));
        // Real data delivered → the session counts as stable.
        assert!(saw_data, "an emitted event must mark the session as stable");
    }

    #[tokio::test]
    async fn handle_frame_returns_false_when_channel_closed() {
        // A valid book frame, but the receiver is gone: emit fails, so
        // handle_frame reports the channel is closed (Ok(false)).
        let book =
            r#"{"event_type":"book","asset_id":"t","bids":[{"price":"0.5","size":"1"}],"asks":[]}"#;
        let (tx, rx) = mpsc::channel(4);
        drop(rx);
        let mut stamper = Stamper::new(|| dt(1));
        let mut saw_data = false;

        let kept_alive = handle_frame(WsMessage::text(book), &tx, &mut stamper, &mut saw_data)
            .await
            .expect("handle_frame ok");

        assert!(!kept_alive, "a closed channel must report Ok(false)");
    }

    /// Spin up a local WS server that accepts one connection, expects the
    /// subscribe frame, sends one `book` frame, then closes. Drive the real
    /// client against it and assert it emits Connection{true} then the Book.
    #[tokio::test]
    async fn integration_connect_subscribe_emit() {
        use futures_util::{SinkExt, StreamExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let url = format!("ws://{addr}");

        // Mock server task.
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = tokio_tungstenite::accept_async(stream)
                .await
                .expect("ws handshake");

            // First inbound frame must be the subscribe message.
            let sub = ws.next().await.expect("a frame").expect("ok frame");
            assert_eq!(
                sub.into_text().expect("text").as_str(),
                r#"{"assets_ids":["tok-1"],"type":"market"}"#
            );

            // Send one book frame, then close.
            let book = r#"{"event_type":"book","asset_id":"tok-1","timestamp":"1700000000000","bids":[{"price":"0.52","size":"100"}],"asks":[{"price":"0.55","size":"80"}]}"#;
            ws.send(WsMessage::text(book)).await.expect("send book");
            ws.close(None).await.expect("close");
        });

        let (tx, mut rx) = mpsc::channel(16);
        let shutdown = CancellationToken::new();
        let client_shutdown = shutdown.clone();

        let client = PolymarketClient::with_url(url, vec!["tok-1".to_string()])
            .with_clock(|| dt(1_700_000_000));
        let client_task = tokio::spawn(async move { client.run(tx, client_shutdown).await });

        // First message: Connection{true}.
        let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("no timeout")
            .expect("a message");
        assert_eq!(first.seq, 0);
        assert_eq!(first.timestamp, dt(1_700_000_000));
        assert_eq!(first.payload, PolyEvent::Connection { connected: true });

        // Second message: the Book.
        let second = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("no timeout")
            .expect("a message");
        assert_eq!(second.seq, 1);
        let PolyEvent::Book(book) = &second.payload else {
            panic!("expected Book, got {:?}", second.payload);
        };
        assert_eq!(book.asset_id.as_str(), "tok-1");
        assert_eq!(book.exchange_ts_millis, Some(1_700_000_000_000));

        // Stop the client and clean up.
        shutdown.cancel();
        server.await.expect("server task");
        let _ = tokio::time::timeout(Duration::from_secs(5), client_task)
            .await
            .expect("client stops");
    }

    /// End-to-end reconnect: a mock server serves one `book` then closes, and on
    /// the second accept serves another `book`. The real client must reconnect
    /// and keep `seq` strictly increasing across the gap, with the connection
    /// markers in order:
    /// Connection{true}=0, Book=1, Connection{false}=2, Connection{true}=3,
    /// Book=4.
    #[tokio::test]
    async fn reconnect_keeps_seq_monotonic_end_to_end() {
        use futures_util::{SinkExt, StreamExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let url = format!("ws://{addr}");

        // Mock server: accept twice, each time expect the subscribe frame, send
        // one book, then close so the client reconnects.
        let server = tokio::spawn(async move {
            for asset in ["tok-A", "tok-B"] {
                let (stream, _) = listener.accept().await.expect("accept");
                let mut ws = tokio_tungstenite::accept_async(stream)
                    .await
                    .expect("ws handshake");
                let sub = ws.next().await.expect("a frame").expect("ok frame");
                assert_eq!(
                    sub.into_text().expect("text").as_str(),
                    r#"{"assets_ids":["tok-1"],"type":"market"}"#
                );
                let book = format!(
                    r#"{{"event_type":"book","asset_id":"{asset}","bids":[{{"price":"0.5","size":"1"}}],"asks":[]}}"#
                );
                ws.send(WsMessage::text(book)).await.expect("send book");
                ws.close(None).await.expect("close");
            }
        });

        let (tx, mut rx) = mpsc::channel(16);
        let shutdown = CancellationToken::new();
        let client_shutdown = shutdown.clone();

        // Advancing clock so timestamps are non-decreasing and visible.
        let tick = std::sync::atomic::AtomicI64::new(1_700_000_000);
        let clock = move || {
            let s = tick.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            dt(s)
        };
        let client = PolymarketClient::with_url(url, vec!["tok-1".to_string()]).with_clock(clock);
        let client_task = tokio::spawn(async move { client.run(tx, client_shutdown).await });

        // Collect the five expected messages, each under a timeout.
        let mut msgs = Vec::new();
        for _ in 0..5 {
            let m = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("no timeout")
                .expect("a message");
            msgs.push(m);
        }

        // Markers and books in the right order across the reconnect.
        assert_eq!(msgs[0].payload, PolyEvent::Connection { connected: true });
        assert!(matches!(&msgs[1].payload, PolyEvent::Book(b) if b.asset_id.as_str() == "tok-A"));
        assert_eq!(msgs[2].payload, PolyEvent::Connection { connected: false });
        assert_eq!(msgs[3].payload, PolyEvent::Connection { connected: true });
        assert!(matches!(&msgs[4].payload, PolyEvent::Book(b) if b.asset_id.as_str() == "tok-B"));

        // seq strictly increasing across the whole stream (reconnect included).
        let seqs: Vec<u64> = msgs.iter().map(|m| m.seq).collect();
        assert_eq!(seqs, vec![0, 1, 2, 3, 4]);
        for w in msgs.windows(2) {
            assert!(w[1].seq > w[0].seq, "seq must strictly increase");
            assert!(w[1].timestamp >= w[0].timestamp, "ts non-decreasing");
        }

        shutdown.cancel();
        server.await.expect("server task");
        let _ = tokio::time::timeout(Duration::from_secs(5), client_task)
            .await
            .expect("client stops");
    }
}
