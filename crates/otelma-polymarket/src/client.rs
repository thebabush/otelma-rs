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

use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use otelma::Message;
use serde::Serialize;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_util::sync::CancellationToken;

use crate::event::PolyEvent;
use crate::parser::{parse_ws_frame, ParseError};

/// Default Polymarket CLOB market WebSocket URL.
pub const DEFAULT_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// Keepalive interval: send `PING` this often.
const PING_INTERVAL: Duration = Duration::from_secs(10);

/// Reconnect backoff bounds.
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(60);

/// Errors from the WS client.
#[derive(Debug, Error)]
pub enum Error {
    /// The WebSocket transport failed (connect/read/write).
    #[error("websocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    /// A frame failed to parse (corrupt recognized event).
    #[error("frame parse error: {0}")]
    Parse(#[from] ParseError),

    /// The downstream receiver was dropped; nothing left to emit to.
    #[error("emit channel closed")]
    ChannelClosed,

    /// Failed to serialize the subscribe message.
    #[error("subscribe encode error: {0}")]
    Subscribe(#[from] serde_json::Error),
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
/// clock is clamped to the previous timestamp).
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
    pub fn stamp(&mut self, payload: PolyEvent) -> Message<PolyEvent> {
        let raw = (self.clock)();
        let ts = match self.last_ts {
            Some(prev) if raw < prev => prev,
            _ => raw,
        };
        let seq = self.seq;
        self.seq += 1;
        self.last_ts = Some(ts);
        Message::new(seq, ts, payload)
    }
}

/// Reconnecting Polymarket market WS client.
pub struct PolymarketClient<C = fn() -> DateTime<Utc>> {
    url: String,
    asset_ids: Vec<String>,
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
            clock,
        }
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
            clock,
        } = self;
        let mut stamper = Stamper::new(clock);
        let mut backoff = BACKOFF_MIN;

        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }

            let session = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return Ok(()),
                result = run_session(&url, &asset_ids, &tx, &mut stamper, &shutdown) => result,
            };

            match session {
                Ok(SessionEnd::Shutdown) => return Ok(()),
                Ok(SessionEnd::ChannelClosed) => return Err(Error::ChannelClosed),
                Ok(SessionEnd::Disconnected) => {
                    // Successful connect happened (backoff reset there); loop.
                    backoff = BACKOFF_MIN;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "polymarket ws session error; reconnecting");
                }
            }

            // Emit a disconnect marker (best-effort) and back off.
            if emit(
                &tx,
                stamper.stamp(PolyEvent::Connection { connected: false }),
            )
            .await
            .is_err()
            {
                return Err(Error::ChannelClosed);
            }

            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return Ok(()),
                _ = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(BACKOFF_MAX);
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
) -> Result<SessionEnd, Error> {
    let (mut ws, _resp) = tokio_tungstenite::connect_async(url).await?;

    // Subscribe, then announce connectivity.
    ws.send(WsMessage::text(subscribe_message(asset_ids)?))
        .await?;
    if emit(tx, stamper.stamp(PolyEvent::Connection { connected: true }))
        .await
        .is_err()
    {
        return Ok(SessionEnd::ChannelClosed);
    }

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
                        if !handle_frame(frame?, tx, stamper).await? {
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

    for event in parse_ws_frame(&text)? {
        if emit(tx, stamper.stamp(event)).await.is_err() {
            return Ok(false);
        }
    }
    Ok(true)
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
        let a = stamper.stamp(PolyEvent::Connection { connected: true });
        let b = stamper.stamp(PolyEvent::Connection { connected: false });
        let c = stamper.stamp(PolyEvent::Connection { connected: true });
        assert_eq!((a.seq, b.seq, c.seq), (0, 1, 2));
    }

    #[test]
    fn stamper_clamps_backwards_clock() {
        // A clock that steps backwards after the first call.
        let times = std::sync::Mutex::new(vec![dt(100), dt(50), dt(200)].into_iter());
        let clock = || times.lock().expect("lock").next().expect("time");
        let mut stamper = Stamper::new(clock);

        let a = stamper.stamp(PolyEvent::Connection { connected: true });
        let b = stamper.stamp(PolyEvent::Connection { connected: false });
        let c = stamper.stamp(PolyEvent::Connection { connected: true });

        assert_eq!(a.timestamp, dt(100));
        // 50 < 100 → clamped to 100 (non-decreasing).
        assert_eq!(b.timestamp, dt(100));
        // 200 > 100 → accepted.
        assert_eq!(c.timestamp, dt(200));
        // And seq still strictly increases regardless of the clamp.
        assert_eq!((a.seq, b.seq, c.seq), (0, 1, 2));
    }

    #[test]
    fn stamper_fixed_clock_is_non_decreasing() {
        let mut stamper = Stamper::new(|| dt(42));
        let msgs: Vec<_> = (0..5)
            .map(|_| stamper.stamp(PolyEvent::Connection { connected: true }))
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
        let msg = stamper.stamp(book.clone());
        assert_eq!(msg.payload, book);
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
        assert_eq!(book.asset_id, "tok-1");
        assert_eq!(book.exchange_ts_millis, Some(1_700_000_000_000));

        // Stop the client and clean up.
        shutdown.cancel();
        server.await.expect("server task");
        let _ = tokio::time::timeout(Duration::from_secs(5), client_task)
            .await
            .expect("client stops");
    }
}
