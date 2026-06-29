//! `otelma-polymarket` — a batteries-included Polymarket CLOB payload for the
//! `otelma` record/replay engine.
//!
//! Provides [`PolyEvent`] (the payload `T`, implementing [`otelma::Payload`])
//! and a pure [`parse_ws_frame`] that turns raw CLOB market WS text frames into
//! events. The crate is generic to Polymarket — it carries raw `asset_id`s and
//! never maps them to domain meaning; that's the downstream user's job. The WS
//! client lives in a separate module (next step); this layer has no networking.

mod client;
mod event;
mod gamma;
mod parser;
mod types;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use client::{subscribe_message, Error, PolymarketClient, Stamper, DEFAULT_URL};
pub use event::{BookUpdate, Level, MarketMeta, PolyEvent, PriceChange, Side, Trade};
pub use gamma::{
    event_slug_from_ref, market_slug_from_ref, parse_event_token_ids, parse_market_token_ids,
    resolve_event, resolve_market, GammaError, Resolution, DEFAULT_GAMMA_BASE,
};
pub use parser::{parse_ws_frame, ParseError};
pub use types::{AssetId, MarketId, Price, Size};
