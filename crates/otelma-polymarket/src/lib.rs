//! `otelma-polymarket` — a batteries-included Polymarket CLOB payload for the
//! `otelma` record/replay engine.
//!
//! Provides [`PolyEvent`] (the payload `T`, implementing [`otelma::Payload`])
//! and a pure [`parse_ws_frame`] that turns raw CLOB market WS text frames into
//! events. The crate is generic to Polymarket — it carries raw `asset_id`s and
//! never maps them to domain meaning; that's the downstream user's job. The WS
//! client lives in a separate module (next step); this layer has no networking.

mod event;
mod parser;

pub use event::{BookUpdate, Level, PolyEvent, Side, Trade};
pub use parser::{parse_ws_frame, ParseError};
