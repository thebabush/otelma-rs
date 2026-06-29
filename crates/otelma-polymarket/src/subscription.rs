//! Shared resolve-and-merge glue: turn a mix of `--event` / `--market` /
//! `--asset-id` selectors into one deterministic subscription set (sorted,
//! deduplicated token ids plus per-market metadata).
//!
//! This lives in `otelma-polymarket` so every front-end — the CLI's `record`
//! and the egui replayer's `--live` mode — shares ONE resolver instead of each
//! re-deriving the merge/dedup. The pure merge (`merge_token_ids`) is unit-
//! tested here without the network; [`resolve_subscription`] is the thin async
//! wrapper that fetches each reference via [`resolve_event`] / [`resolve_market`].

use std::collections::BTreeSet;

use crate::event::MarketMeta;
use crate::gamma::{resolve_event, resolve_market, GammaError, Resolution};

/// What to subscribe to: the deterministic, deduplicated token-id set plus the
/// collected per-market metadata to embed at recording start.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedSubscription {
    /// Sorted, deduplicated token ids to subscribe to.
    pub token_ids: Vec<String>,
    /// Per-market metadata, in the resolvers' deterministic order. Raw
    /// `--asset-id`s contribute tokens but no metadata.
    pub markets: Vec<MarketMeta>,
    /// Total markets skipped as closed across all resolutions.
    pub skipped_closed: usize,
}

/// Merge resolved event/market resolutions with raw asset ids into one sorted,
/// deduplicated token-id list plus the collected market metadata, and report the
/// total skipped-closed count.
///
/// Pure: takes already-fetched [`Resolution`]s plus the raw `--asset-id` values,
/// so the merge + deterministic dedup is unit-testable without the network. The
/// returned token `Vec` is sorted (`BTreeSet`-derived), never hash-iterated; the
/// `markets` preserve each resolution's deterministic (sorted) order.
fn merge_token_ids(resolutions: &[Resolution], raw_asset_ids: &[String]) -> ResolvedSubscription {
    let mut set: BTreeSet<String> = BTreeSet::new();
    let mut markets: Vec<MarketMeta> = Vec::new();
    let mut seen_markets: BTreeSet<String> = BTreeSet::new();
    let mut skipped_closed = 0usize;
    for r in resolutions {
        skipped_closed += r.skipped_closed;
        for id in &r.token_ids {
            set.insert(id.to_string());
        }
        // Dedup metadata too: a market requested via both --event and --market
        // resolves twice. Keyed on the yes-token id (which is unique per market),
        // keep the first occurrence — preserving each resolution's sorted order —
        // so we never record the same PolyEvent::Market twice.
        for m in &r.markets {
            if seen_markets.insert(m.yes_asset_id.to_string()) {
                markets.push(m.clone());
            }
        }
    }
    for id in raw_asset_ids {
        set.insert(id.clone());
    }
    ResolvedSubscription {
        token_ids: set.into_iter().collect(),
        markets,
        skipped_closed,
    }
}

/// Resolve `--event` / `--market` references (via the Gamma API at `base`) and
/// merge with raw `--asset-id`s into a sorted, deduplicated token-id list plus
/// the collected per-market metadata.
///
/// Errors with [`GammaError::NoSelectors`] if no references of any kind are
/// given (clap can't express "at-least-one-of" across the three flags), and with
/// [`GammaError::NoTokens`] if every matched market was closed or malformed.
/// Individual resolutions surface their own [`GammaError`]s.
pub async fn resolve_subscription(
    base: &str,
    events: &[String],
    markets: &[String],
    raw_asset_ids: &[String],
    include_closed: bool,
) -> Result<ResolvedSubscription, GammaError> {
    if events.is_empty() && markets.is_empty() && raw_asset_ids.is_empty() {
        return Err(GammaError::NoSelectors);
    }

    let mut resolutions = Vec::new();
    for ev in events {
        let r = resolve_event(base, ev, include_closed).await?;
        tracing::info!(event = %ev, tokens = r.token_ids.len(), markets = r.markets.len(), skipped_closed = r.skipped_closed, "resolved event");
        resolutions.push(r);
    }
    for mk in markets {
        let r = resolve_market(base, mk, include_closed).await?;
        tracing::info!(market = %mk, tokens = r.token_ids.len(), markets = r.markets.len(), skipped_closed = r.skipped_closed, "resolved market");
        resolutions.push(r);
    }

    let sub = merge_token_ids(&resolutions, raw_asset_ids);
    if sub.token_ids.is_empty() {
        return Err(GammaError::NoTokens);
    }
    tracing::info!(
        tokens = sub.token_ids.len(),
        markets = sub.markets.len(),
        skipped_closed = sub.skipped_closed,
        "resolved subscription set"
    );
    Ok(sub)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::market_meta;

    fn res(ids: &[&str], skipped: usize) -> Resolution {
        Resolution {
            token_ids: ids.iter().map(|s| (*s).into()).collect(),
            markets: Vec::new(),
            skipped_closed: skipped,
        }
    }

    fn res_with_markets(ids: &[&str], markets: Vec<MarketMeta>, skipped: usize) -> Resolution {
        Resolution {
            token_ids: ids.iter().map(|s| (*s).into()).collect(),
            markets,
            skipped_closed: skipped,
        }
    }

    #[test]
    fn merge_dedups_and_sorts_across_sources() {
        let resolutions = vec![res(&["500", "100"], 1), res(&["300", "100"], 2)];
        let raw = vec!["100".to_string(), "999".to_string()];
        let sub = merge_token_ids(&resolutions, &raw);
        assert_eq!(sub.token_ids, vec!["100", "300", "500", "999"]);
        assert!(sub.markets.is_empty());
        assert_eq!(sub.skipped_closed, 3);
    }

    #[test]
    fn merge_with_only_raw_asset_ids() {
        let sub = merge_token_ids(&[], &["b".to_string(), "a".to_string()]);
        assert_eq!(sub.token_ids, vec!["a", "b"]);
        // Raw asset ids carry no metadata.
        assert!(sub.markets.is_empty());
        assert_eq!(sub.skipped_closed, 0);
    }

    #[test]
    fn merge_collects_market_metadata_across_resolutions() {
        let resolutions = vec![
            res_with_markets(
                &["500", "100"],
                vec![market_meta("Argentina", "500", "100", Some("World Cup"))],
                0,
            ),
            res_with_markets(
                &["300", "200"],
                vec![market_meta("Brazil", "300", "200", Some("World Cup"))],
                0,
            ),
        ];
        let sub = merge_token_ids(&resolutions, &["raw".to_string()]);
        assert_eq!(sub.token_ids, vec!["100", "200", "300", "500", "raw"]);
        assert_eq!(
            sub.markets
                .iter()
                .map(|m| m.outcome_title.as_str())
                .collect::<Vec<_>>(),
            vec!["Argentina", "Brazil"]
        );
    }

    #[test]
    fn merge_dedups_market_metadata_across_resolutions() {
        // The same market resolved twice (e.g. via both --event and --market):
        // same yes-token id → its metadata is recorded exactly once.
        let arg = market_meta("Argentina", "500", "100", Some("World Cup"));
        let resolutions = vec![
            res_with_markets(&["500", "100"], vec![arg.clone()], 0),
            res_with_markets(&["500", "100"], vec![arg.clone()], 0),
        ];
        let sub = merge_token_ids(&resolutions, &[]);
        assert_eq!(sub.token_ids, vec!["100", "500"]);
        assert_eq!(sub.markets.len(), 1, "duplicate market metadata deduped");
        assert_eq!(sub.markets[0].outcome_title, "Argentina");
    }

    #[tokio::test]
    async fn resolve_subscription_errors_when_nothing_given() {
        let err = resolve_subscription("http://unused", &[], &[], &[], false)
            .await
            .expect_err("should require at least one ref");
        assert!(matches!(err, GammaError::NoSelectors));
    }

    #[tokio::test]
    async fn resolve_subscription_passes_through_raw_only_without_network() {
        // No event/market refs → no HTTP; raw ids flow straight through, sorted.
        let sub = resolve_subscription("http://unused", &[], &[], &["z".into(), "a".into()], false)
            .await
            .expect("raw-only");
        assert_eq!(sub.token_ids, vec!["a", "z"]);
        assert!(sub.markets.is_empty());
    }
}
