//! Polymarket **Gamma REST API** resolver: turn human-friendly event/market
//! references (a bare slug or a full `polymarket.com` URL) into the raw CLOB
//! token (`AssetId`) ids that the WS client subscribes to.
//!
//! The Gamma API responses are huge; we deserialize only the handful of fields
//! we need and ignore the rest. **Pure parsing is kept separate from the
//! network** so the slug/URL extraction, the JSON walk, and the closed-market
//! filtering are all unit-testable against fixture strings without any I/O —
//! [`resolve_event`] / [`resolve_market`] are the only functions that touch the
//! wire, and they are thin wrappers over the pure parsers below.
//!
//! Token ids are deduplicated **deterministically** (collected into a
//! `BTreeSet` and returned sorted), per the project's no-unspecified-ordering
//! rule: the same references always resolve to the same byte-identical id list.

use std::collections::BTreeSet;

use serde::Deserialize;
use thiserror::Error;

use crate::event::MarketMeta;
use crate::types::{AssetId, MarketId};

/// Default Gamma REST API base URL.
pub const DEFAULT_GAMMA_BASE: &str = "https://gamma-api.polymarket.com";

/// Errors from resolving event/market references to token ids.
#[derive(Debug, Error)]
pub enum GammaError {
    /// The HTTP request to the Gamma API failed.
    #[error("gamma http error: {0}")]
    Http(String),
    /// The response body was not valid JSON in the expected shape.
    #[error("gamma json error: {0}")]
    Json(#[from] serde_json::Error),
    /// The Gamma API returned an empty array — no such event/market.
    #[error("no {kind} found for slug {slug:?}")]
    NotFound {
        /// `"event"` or `"market"`.
        kind: &'static str,
        /// The slug that was queried.
        slug: String,
    },
    /// A market's `clobTokenIds` field was missing, not a 2-element array, or
    /// otherwise malformed.
    #[error("malformed clobTokenIds for market {slug:?}: {detail}")]
    MalformedTokens {
        /// The market slug (or question) the bad tokens belong to.
        slug: String,
        /// What was wrong.
        detail: String,
    },
    /// A reference string could not be parsed into a slug.
    #[error("invalid {kind} reference {reference:?}: {detail}")]
    InvalidRef {
        /// `"event"` or `"market"`.
        kind: &'static str,
        /// The offending reference string.
        reference: String,
        /// What was wrong.
        detail: String,
    },
}

/// One market as returned in a Gamma events/markets response. Only the fields we
/// need; everything else is ignored.
#[derive(Debug, Deserialize)]
struct GammaMarket {
    /// JSON-encoded *string* holding a 2-element array `["yesTokenId","noTokenId"]`.
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: Option<String>,
    #[serde(default)]
    active: bool,
    #[serde(default)]
    closed: bool,
    #[serde(default)]
    slug: Option<String>,
    #[serde(default)]
    question: Option<String>,
    #[serde(default, rename = "groupItemTitle")]
    group_item_title: Option<String>,
    #[serde(default, rename = "conditionId")]
    condition_id: Option<String>,
}

/// One event in a Gamma `/events` response.
#[derive(Debug, Deserialize)]
struct GammaEvent {
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    markets: Vec<GammaMarket>,
}

/// Outcome of resolving references: the sorted, deduped token ids, the per-market
/// [`MarketMeta`] (in a deterministic order), and how many markets were skipped
/// as closed (for "no silent caps" reporting).
#[derive(Debug, Clone, PartialEq)]
pub struct Resolution {
    /// Sorted, deduplicated token ids.
    pub token_ids: Vec<AssetId>,
    /// One [`MarketMeta`] per market that contributed tokens, sorted by a stable
    /// key (`yes_asset_id`) so the same input always produces the same order.
    pub markets: Vec<MarketMeta>,
    /// Count of markets filtered out because they were closed/inactive.
    pub skipped_closed: usize,
}

/// A short label used in [`GammaMarket`] errors.
fn market_label(m: &GammaMarket) -> String {
    m.slug
        .clone()
        .or_else(|| m.question.clone())
        .unwrap_or_else(|| "<unknown>".to_string())
}

/// Parse a market's `clobTokenIds` (a JSON-encoded string holding a 2-element
/// array) and return the `(yes, no)` token ids in `clobTokenIds` order.
fn parse_market_tokens(m: &GammaMarket) -> Result<(AssetId, AssetId), GammaError> {
    let raw = m
        .clob_token_ids
        .as_deref()
        .ok_or_else(|| GammaError::MalformedTokens {
            slug: market_label(m),
            detail: "missing clobTokenIds".to_string(),
        })?;
    let ids: Vec<String> = serde_json::from_str(raw).map_err(|e| GammaError::MalformedTokens {
        slug: market_label(m),
        detail: format!("inner JSON not an array of strings: {e}"),
    })?;
    if ids.len() != 2 {
        return Err(GammaError::MalformedTokens {
            slug: market_label(m),
            detail: format!("expected 2 token ids, got {}", ids.len()),
        });
    }
    let mut it = ids.into_iter();
    let yes = AssetId::from(it.next().expect("len checked == 2"));
    let no = AssetId::from(it.next().expect("len checked == 2"));
    Ok((yes, no))
}

/// Build a [`MarketMeta`] from a parsed market and its `(yes, no)` tokens.
fn market_meta(
    m: &GammaMarket,
    event_title: Option<&str>,
    yes: AssetId,
    no: AssetId,
) -> MarketMeta {
    MarketMeta {
        market: m.condition_id.clone().map(MarketId::from),
        question: m.question.clone().unwrap_or_default(),
        outcome_title: m.group_item_title.clone().unwrap_or_default(),
        yes_asset_id: yes,
        no_asset_id: no,
        event_title: event_title.map(str::to_string),
        market_slug: m.slug.clone(),
    }
}

/// Filter markets, collect their tokens and per-market metadata, and tally how
/// many were skipped.
///
/// A single market with malformed `clobTokenIds` must not sink the whole event:
/// it is warned about and skipped (yielding neither a token nor a `MarketMeta`)
/// so the other markets still resolve. (Hence this is infallible — the only hard
/// errors, an empty array or a bad outer response, are caught by the callers.)
///
/// Both outputs are deterministic: `token_ids` is the sorted-deduped set and
/// `markets` is sorted by `yes_asset_id`.
fn resolve_markets(
    event_title: Option<&str>,
    markets: &[GammaMarket],
    include_closed: bool,
) -> Resolution {
    let mut set = BTreeSet::new();
    let mut metas = Vec::new();
    let mut skipped_closed = 0usize;
    for m in markets {
        let live = m.active && !m.closed;
        if !include_closed && !live {
            skipped_closed += 1;
            continue;
        }
        match parse_market_tokens(m) {
            Ok((yes, no)) => {
                set.insert(yes.clone());
                set.insert(no.clone());
                metas.push(market_meta(m, event_title, yes, no));
            }
            Err(e) => {
                tracing::warn!(
                    market = %market_label(m),
                    error = %e,
                    "skipping market with malformed token ids"
                );
            }
        }
    }
    // Deterministic order: sort metadata by its stable yes-token key, mirroring
    // the sorted token set.
    metas.sort_by(|a, b| a.yes_asset_id.cmp(&b.yes_asset_id));
    Resolution {
        token_ids: set.into_iter().collect(),
        markets: metas,
        skipped_closed,
    }
}

/// Parse a Gamma `/events?slug=...` response body into token ids.
///
/// The body is a JSON array; element `[0]` carries the `markets` array. Markets
/// are filtered to `active && !closed` unless `include_closed`.
pub fn parse_event_token_ids(json: &str, include_closed: bool) -> Result<Resolution, GammaError> {
    let events: Vec<GammaEvent> = serde_json::from_str(json)?;
    let event = events
        .into_iter()
        .next()
        .ok_or_else(|| GammaError::NotFound {
            kind: "event",
            slug: String::new(),
        })?;
    Ok(resolve_markets(
        event.title.as_deref(),
        &event.markets,
        include_closed,
    ))
}

/// Parse a Gamma `/markets?slug=...` response body into token ids.
///
/// The body is a JSON array; element `[0]` is the single market.
pub fn parse_market_token_ids(json: &str, include_closed: bool) -> Result<Resolution, GammaError> {
    let markets: Vec<GammaMarket> = serde_json::from_str(json)?;
    if markets.is_empty() {
        return Err(GammaError::NotFound {
            kind: "market",
            slug: String::new(),
        });
    }
    Ok(resolve_markets(None, &markets[..1], include_closed))
}

/// Extract a single trailing path segment after `/event/` (or `/market/`) from a
/// Polymarket URL path, ignoring any query string.
fn path_segment_after<'a>(url: &'a str, marker: &str) -> Option<&'a str> {
    // Strip scheme/query/fragment concerns by splitting on the marker.
    let after = url.split(marker).nth(1)?;
    let seg = after.split(['/', '?', '#']).next()?;
    if seg.is_empty() {
        None
    } else {
        Some(seg)
    }
}

/// Extract the value of a query parameter `key` from a URL, if present.
fn query_param<'a>(url: &'a str, key: &str) -> Option<&'a str> {
    let query = url.split('?').nth(1)?;
    for pair in query.split('&') {
        let mut kv = pair.splitn(2, '=');
        if kv.next() == Some(key) {
            return kv.next();
        }
    }
    None
}

/// True if `s` looks like a URL rather than a bare slug.
fn looks_like_url(s: &str) -> bool {
    s.contains("://") || s.starts_with("polymarket.com") || s.contains('/')
}

/// Resolve an `--event` reference to an event slug.
///
/// Accepts a bare slug (returned verbatim) or a `polymarket.com` URL (the path
/// segment after `/event/` is used; any `marketSlug` query param is ignored —
/// `--event` always means the whole event).
pub fn event_slug_from_ref(s: &str) -> Result<String, GammaError> {
    let s = s.trim();
    if !looks_like_url(s) {
        return Ok(s.to_string());
    }
    path_segment_after(s, "/event/")
        .map(str::to_string)
        .ok_or_else(|| GammaError::InvalidRef {
            kind: "event",
            reference: s.to_string(),
            detail: "could not find an /event/<slug> path segment".to_string(),
        })
}

/// Resolve a `--market` reference to a market slug.
///
/// Accepts a bare slug (returned verbatim) or a `polymarket.com` URL. For a URL,
/// the `marketSlug` query param wins (Polymarket links a specific market via the
/// query string); failing that, a `/market/<slug>` path segment is used.
pub fn market_slug_from_ref(s: &str) -> Result<String, GammaError> {
    let s = s.trim();
    if !looks_like_url(s) {
        return Ok(s.to_string());
    }
    if let Some(slug) = query_param(s, "marketSlug") {
        if !slug.is_empty() {
            return Ok(slug.to_string());
        }
    }
    path_segment_after(s, "/market/")
        .map(str::to_string)
        .ok_or_else(|| GammaError::InvalidRef {
            kind: "market",
            reference: s.to_string(),
            detail: "no marketSlug query param and no /market/<slug> path segment".to_string(),
        })
}

/// Fetch and resolve an event reference (slug or URL) to its token ids.
pub async fn resolve_event(
    base: &str,
    reference: &str,
    include_closed: bool,
) -> Result<Resolution, GammaError> {
    let slug = event_slug_from_ref(reference)?;
    let url = format!("{base}/events?slug={slug}");
    let body = http_get_text(&url).await?;
    let res = parse_event_token_ids(&body, include_closed);
    fill_not_found_slug(res, "event", &slug)
}

/// Fetch and resolve a market reference (slug or URL) to its token ids.
pub async fn resolve_market(
    base: &str,
    reference: &str,
    include_closed: bool,
) -> Result<Resolution, GammaError> {
    let slug = market_slug_from_ref(reference)?;
    let url = format!("{base}/markets?slug={slug}");
    let body = http_get_text(&url).await?;
    let res = parse_market_token_ids(&body, include_closed);
    fill_not_found_slug(res, "market", &slug)
}

/// The pure parsers don't know the queried slug; patch it into a `NotFound` for
/// a useful error message.
fn fill_not_found_slug(
    res: Result<Resolution, GammaError>,
    kind: &'static str,
    slug: &str,
) -> Result<Resolution, GammaError> {
    match res {
        Err(GammaError::NotFound { .. }) => Err(GammaError::NotFound {
            kind,
            slug: slug.to_string(),
        }),
        other => other,
    }
}

/// HTTP GET returning the body text. Minimal reqwest usage (no JSON feature).
async fn http_get_text(url: &str) -> Result<String, GammaError> {
    let resp = reqwest::get(url)
        .await
        .map_err(|e| GammaError::Http(e.to_string()))?;
    let resp = resp
        .error_for_status()
        .map_err(|e| GammaError::Http(e.to_string()))?;
    resp.text()
        .await
        .map_err(|e| GammaError::Http(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Two live markets + one closed market. Token ids chosen so sorting is
    // observable (they are NOT already in sorted order in the fixture).
    const EVENTS_FIXTURE: &str = r#"
    [
      {
        "slug": "world-cup-winner",
        "title": "World Cup Winner",
        "markets": [
          {
            "slug": "will-argentina-win",
            "question": "Will Argentina win?",
            "groupItemTitle": "Argentina",
            "conditionId": "0xargentina",
            "active": true,
            "closed": false,
            "clobTokenIds": "[\"500\", \"100\"]"
          },
          {
            "slug": "will-brazil-win",
            "question": "Will Brazil win?",
            "groupItemTitle": "Brazil",
            "conditionId": "0xbrazil",
            "active": true,
            "closed": false,
            "clobTokenIds": "[\"300\", \"200\"]"
          },
          {
            "slug": "will-elimnated-team-win",
            "question": "Will eliminated team win?",
            "groupItemTitle": "Eliminated",
            "conditionId": "0xelim",
            "active": false,
            "closed": true,
            "clobTokenIds": "[\"900\", \"800\"]"
          }
        ]
      }
    ]
    "#;

    const MARKETS_FIXTURE: &str = r#"
    [
      {
        "slug": "will-argentina-win",
        "question": "Will Argentina win?",
        "groupItemTitle": "Argentina",
        "conditionId": "0xargentina",
        "active": true,
        "closed": false,
        "clobTokenIds": "[\"500\", \"100\"]"
      }
    ]
    "#;

    fn ids(r: &Resolution) -> Vec<&str> {
        r.token_ids.iter().map(AssetId::as_str).collect()
    }

    #[test]
    fn parse_event_filters_closed_by_default_and_sorts() {
        let r = parse_event_token_ids(EVENTS_FIXTURE, false).expect("parse");
        // Closed market's 800/900 excluded; rest sorted & deduped.
        assert_eq!(ids(&r), vec!["100", "200", "300", "500"]);
        assert_eq!(r.skipped_closed, 1);
    }

    #[test]
    fn parse_event_includes_closed_when_requested() {
        let r = parse_event_token_ids(EVENTS_FIXTURE, true).expect("parse");
        assert_eq!(ids(&r), vec!["100", "200", "300", "500", "800", "900"]);
        assert_eq!(r.skipped_closed, 0);
    }

    #[test]
    fn parse_market_resolves_single_market_tokens() {
        let r = parse_market_token_ids(MARKETS_FIXTURE, false).expect("parse");
        assert_eq!(ids(&r), vec!["100", "500"]);
        assert_eq!(r.skipped_closed, 0);
    }

    #[test]
    fn parse_event_empty_array_is_not_found() {
        let err = parse_event_token_ids("[]", false).expect_err("empty");
        assert!(matches!(err, GammaError::NotFound { kind: "event", .. }));
    }

    #[test]
    fn parse_market_empty_array_is_not_found() {
        let err = parse_market_token_ids("[]", false).expect_err("empty");
        assert!(matches!(err, GammaError::NotFound { kind: "market", .. }));
    }

    #[test]
    fn clob_token_ids_inner_json_is_parsed() {
        // Direct check of the inner-JSON-string parsing.
        let json =
            r#"[{"active":true,"closed":false,"slug":"m","clobTokenIds":"[\"abc\",\"def\"]"}]"#;
        let r = parse_market_token_ids(json, false).expect("parse");
        assert_eq!(ids(&r), vec!["abc", "def"]);
    }

    #[test]
    fn malformed_clob_token_ids_are_skipped_not_fatal() {
        // A market with bad tokens is warned about and skipped (empty result),
        // never an error — so it can't sink the rest of an event.
        for bad in [
            r#"[{"active":true,"closed":false,"slug":"m","clobTokenIds":"[\"only-one\"]"}]"#, // wrong arity
            r#"[{"active":true,"closed":false,"slug":"m","clobTokenIds":"not-json"}]"#, // not JSON
            r#"[{"active":true,"closed":false,"slug":"m"}]"#,                           // missing
        ] {
            let r = parse_market_token_ids(bad, false).expect("skips, not errors");
            assert!(
                r.token_ids.is_empty(),
                "should skip malformed market: {bad}"
            );
        }
    }

    #[test]
    fn malformed_market_is_skipped_others_kept() {
        // One bad market alongside a good one: the good one's tokens still resolve.
        let json = r#"
        [{"markets":[
          {"active":true,"closed":false,"slug":"good","clobTokenIds":"[\"100\",\"200\"]"},
          {"active":true,"closed":false,"slug":"bad","clobTokenIds":"oops"}
        ]}]"#;
        let r = parse_event_token_ids(json, false).expect("parse");
        assert_eq!(ids(&r), vec!["100", "200"]);
    }

    #[test]
    fn dedup_is_deterministic_across_markets() {
        // Two markets share a token id (e.g. shared "No" leg); it appears once.
        let json = r#"
        [{"markets":[
          {"active":true,"closed":false,"slug":"a","clobTokenIds":"[\"y1\",\"shared\"]"},
          {"active":true,"closed":false,"slug":"b","clobTokenIds":"[\"y2\",\"shared\"]"}
        ]}]"#;
        let r = parse_event_token_ids(json, false).expect("parse");
        assert_eq!(ids(&r), vec!["shared", "y1", "y2"]);
    }

    #[test]
    fn event_populates_market_meta_with_yes_no_order() {
        let r = parse_event_token_ids(EVENTS_FIXTURE, false).expect("parse");
        // Two live markets → two metas; the closed one yields none.
        assert_eq!(r.markets.len(), 2);
        // Deterministically ordered by yes_asset_id: Brazil's yes="300" sorts
        // before Argentina's yes="500".
        assert_eq!(
            r.markets
                .iter()
                .map(|m| m.outcome_title.as_str())
                .collect::<Vec<_>>(),
            vec!["Brazil", "Argentina"]
        );

        let arg = r
            .markets
            .iter()
            .find(|m| m.outcome_title == "Argentina")
            .expect("argentina meta");
        // clobTokenIds order is [yes, no] → yes="500", no="100".
        assert_eq!(arg.yes_asset_id.as_str(), "500");
        assert_eq!(arg.no_asset_id.as_str(), "100");
        assert_eq!(arg.question, "Will Argentina win?");
        assert_eq!(
            arg.market.as_ref().map(MarketId::as_str),
            Some("0xargentina")
        );
        assert_eq!(arg.event_title.as_deref(), Some("World Cup Winner"));
        assert_eq!(arg.market_slug.as_deref(), Some("will-argentina-win"));
    }

    #[test]
    fn market_endpoint_meta_has_no_event_title() {
        let r = parse_market_token_ids(MARKETS_FIXTURE, false).expect("parse");
        assert_eq!(r.markets.len(), 1);
        let m = &r.markets[0];
        assert_eq!(m.outcome_title, "Argentina");
        assert_eq!(m.yes_asset_id.as_str(), "500");
        assert_eq!(m.no_asset_id.as_str(), "100");
        // The /markets endpoint has no parent event, so no event title.
        assert_eq!(m.event_title, None);
    }

    #[test]
    fn malformed_market_yields_neither_token_nor_meta_siblings_survive() {
        // One bad market alongside a good one: the good one's tokens AND meta
        // still resolve; the bad one contributes neither.
        let json = r#"
        [{"title":"E","markets":[
          {"active":true,"closed":false,"slug":"good","groupItemTitle":"Good","clobTokenIds":"[\"100\",\"200\"]"},
          {"active":true,"closed":false,"slug":"bad","groupItemTitle":"Bad","clobTokenIds":"oops"}
        ]}]"#;
        let r = parse_event_token_ids(json, false).expect("parse");
        assert_eq!(ids(&r), vec!["100", "200"]);
        assert_eq!(r.markets.len(), 1);
        assert_eq!(r.markets[0].outcome_title, "Good");
    }

    #[test]
    fn event_slug_from_bare_slug() {
        assert_eq!(
            event_slug_from_ref("world-cup-winner").unwrap(),
            "world-cup-winner"
        );
        assert_eq!(
            event_slug_from_ref("  world-cup-winner  ").unwrap(),
            "world-cup-winner"
        );
    }

    #[test]
    fn event_slug_from_url_ignores_market_slug() {
        let url = "https://polymarket.com/event/world-cup-winner?marketSlug=will-argentina-win-the-2026-fifa-world-cup-245";
        assert_eq!(event_slug_from_ref(url).unwrap(), "world-cup-winner");
        // No query string.
        assert_eq!(
            event_slug_from_ref("https://polymarket.com/event/world-cup-winner").unwrap(),
            "world-cup-winner"
        );
    }

    #[test]
    fn event_slug_from_url_without_event_path_errors() {
        let err = event_slug_from_ref("https://polymarket.com/foo/bar").expect_err("no /event/");
        assert!(matches!(err, GammaError::InvalidRef { kind: "event", .. }));
    }

    #[test]
    fn market_slug_from_bare_slug() {
        assert_eq!(
            market_slug_from_ref("will-argentina-win-the-2026-fifa-world-cup-245").unwrap(),
            "will-argentina-win-the-2026-fifa-world-cup-245"
        );
    }

    #[test]
    fn market_slug_from_url_prefers_market_slug_query() {
        let url = "https://polymarket.com/event/world-cup-winner?marketSlug=will-argentina-win-the-2026-fifa-world-cup-245";
        assert_eq!(
            market_slug_from_ref(url).unwrap(),
            "will-argentina-win-the-2026-fifa-world-cup-245"
        );
    }

    #[test]
    fn market_slug_from_market_path_url() {
        let url = "https://polymarket.com/market/some-market-slug";
        assert_eq!(market_slug_from_ref(url).unwrap(), "some-market-slug");
    }

    #[test]
    fn market_slug_from_url_without_slug_errors() {
        let err = market_slug_from_ref("https://polymarket.com/event/world-cup-winner")
            .expect_err("no market slug");
        assert!(matches!(err, GammaError::InvalidRef { kind: "market", .. }));
    }
}
