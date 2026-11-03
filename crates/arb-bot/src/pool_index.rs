use crate::{
    config::DiscoveryConfig,
    types::{TokenPair, Venue, USDC_MINT, USDT_MINT, WSOL_MINT},
};
use anyhow::{anyhow, Context, Result};
use futures_util::{stream::FuturesUnordered, StreamExt};
use reqwest::Client;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use tracing::warn;

#[derive(Debug, Clone)]
pub struct IndexedPoolCandidate {
    pub venue: Venue,
    pub address: String,
    pub base_mint: String,
    pub quote_mint: String,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    pub liquidity_usd: f64,
    pub liquidity_quote_ui: Option<f64>,
    pub price_quote_per_base: Option<f64>,
    pub source: String,
}

#[derive(Debug, Clone)]
pub struct PoolIndexIssue {
    pub source: String,
    pub token_mint: String,
    pub error: String,
}

#[derive(Debug, Clone, Default)]
pub struct PoolIndexResult {
    pub candidates: Vec<IndexedPoolCandidate>,
    pub errors: Vec<PoolIndexIssue>,
}

#[derive(Clone)]
pub struct PoolIndexClient {
    config: DiscoveryConfig,
    client: Client,
}

impl PoolIndexClient {
    pub fn new(config: DiscoveryConfig) -> Self {
        Self {
            config,
            client: Client::new(),
        }
    }

    pub async fn fetch_candidates(
        &self,
        targets: &[TokenPair],
        quote_mints: &HashSet<String>,
        enabled_venues: &HashSet<Venue>,
    ) -> PoolIndexResult {
        if !self.config.enabled || !self.config.index_enabled {
            return PoolIndexResult::default();
        }

        let mut base_mints = targets
            .iter()
            .map(|pair| pair.base_mint.clone())
            .collect::<Vec<_>>();
        base_mints.sort();
        base_mints.dedup();

        let mut futures = FuturesUnordered::new();
        for base_mint in base_mints {
            let client = self.clone();
            let targets = targets.to_vec();
            let quote_mints = quote_mints.clone();
            let enabled_venues = enabled_venues.clone();
            futures.push(async move {
                let result = client
                    .fetch_token_candidates(&base_mint, &targets, &quote_mints, &enabled_venues)
                    .await;
                (base_mint, result)
            });
        }

        let mut candidates_by_address: BTreeMap<String, IndexedPoolCandidate> = BTreeMap::new();
        let mut errors = Vec::new();
        while let Some((base_mint, result)) = futures.next().await {
            match result {
                Ok(candidates) => {
                    for candidate in candidates {
                        candidates_by_address
                            .entry(candidate.address.clone())
                            .or_insert(candidate);
                    }
                }
                Err(error) => {
                    warn!(%base_mint, ?error, "pool index lookup failed");
                    errors.push(PoolIndexIssue {
                        source: "dexscreener".to_string(),
                        token_mint: base_mint,
                        error: format!("{error:#}"),
                    });
                }
            }
        }

        PoolIndexResult {
            candidates: candidates_by_address.into_values().collect(),
            errors,
        }
    }

    async fn fetch_token_candidates(
        &self,
        base_mint: &str,
        targets: &[TokenPair],
        quote_mints: &HashSet<String>,
        enabled_venues: &HashSet<Venue>,
    ) -> Result<Vec<IndexedPoolCandidate>> {
        let url = format!(
            "{}/{}",
            self.config.index_url.trim_end_matches('/'),
            base_mint
        );
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("pool index request failed for {base_mint}"))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("pool index http {status}: {body}"));
        }
        let value: Value = response
            .json()
            .await
            .with_context(|| format!("invalid pool index json for {base_mint}"))?;
        Ok(parse_index_candidates(
            &value,
            targets,
            quote_mints,
            enabled_venues,
            self.config.max_index_pairs_per_token,
            self.config.min_index_liquidity_usd,
        ))
    }
}

pub fn parse_index_candidates(
    value: &Value,
    targets: &[TokenPair],
    quote_mints: &HashSet<String>,
    enabled_venues: &HashSet<Venue>,
    max_pairs_per_token: usize,
    min_liquidity_usd: f64,
) -> Vec<IndexedPoolCandidate> {
    let target_decimals = target_decimals(targets);
    let target_base_mints = targets
        .iter()
        .map(|pair| pair.base_mint.clone())
        .collect::<HashSet<_>>();
    let pairs = value
        .get("pairs")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut candidates = pairs
        .iter()
        .filter_map(|pair| {
            if pair.get("chainId").and_then(Value::as_str) != Some("solana") {
                return None;
            }
            let venue = venue_from_dex_id(pair.get("dexId")?.as_str()?)?;
            if !enabled_venues.contains(&venue) {
                return None;
            }

            let pair_address = pair.get("pairAddress")?.as_str()?.to_string();
            let token_a = pair.pointer("/baseToken/address")?.as_str()?;
            let token_b = pair.pointer("/quoteToken/address")?.as_str()?;
            let normalized = normalize_pair(token_a, token_b, &target_base_mints, quote_mints)?;
            let liquidity_usd = pair
                .pointer("/liquidity/usd")
                .and_then(Value::as_f64)
                .unwrap_or_default();
            if liquidity_usd < min_liquidity_usd {
                return None;
            }
            let liquidity_base = pair.pointer("/liquidity/base").and_then(json_f64);
            let liquidity_quote = pair.pointer("/liquidity/quote").and_then(json_f64);
            let liquidity_quote_ui = if normalized.reversed {
                liquidity_base
            } else {
                liquidity_quote
            };
            let price_native = pair
                .get("priceNative")
                .and_then(Value::as_str)
                .and_then(|value| value.parse::<f64>().ok());

            Some(IndexedPoolCandidate {
                venue,
                address: pair_address,
                base_decimals: decimals_for_mint(&normalized.base_mint, &target_decimals),
                quote_decimals: decimals_for_mint(&normalized.quote_mint, &target_decimals),
                base_mint: normalized.base_mint,
                quote_mint: normalized.quote_mint,
                liquidity_usd,
                liquidity_quote_ui,
                price_quote_per_base: price_native.map(|price| {
                    if normalized.reversed && price > 0.0 {
                        1.0 / price
                    } else {
                        price
                    }
                }),
                source: "dexscreener".to_string(),
            })
        })
        .collect::<Vec<_>>();

    candidates.sort_by(|a, b| {
        b.liquidity_usd
            .partial_cmp(&a.liquidity_usd)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    candidates.truncate(max_pairs_per_token);
    candidates
}

fn json_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.parse::<f64>().ok())
}

pub fn venue_from_dex_id(dex_id: &str) -> Option<Venue> {
    let dex_id = dex_id.to_ascii_lowercase();
    if dex_id.contains("orca") {
        Some(Venue::Orca)
    } else if dex_id.contains("meteora") {
        Some(Venue::Meteora)
    } else if dex_id.contains("raydium") {
        Some(Venue::Raydium)
    } else {
        None
    }
}

pub fn decimals_for_mint(mint: &str, target_decimals: &HashMap<String, u8>) -> u8 {
    target_decimals
        .get(mint)
        .copied()
        .or(match mint {
            USDC_MINT | USDT_MINT => Some(6),
            WSOL_MINT => Some(9),
            _ => None,
        })
        .unwrap_or(6)
}

struct NormalizedPair {
    base_mint: String,
    quote_mint: String,
    reversed: bool,
}

fn normalize_pair(
    token_a: &str,
    token_b: &str,
    target_base_mints: &HashSet<String>,
    quote_mints: &HashSet<String>,
) -> Option<NormalizedPair> {
    if target_base_mints.contains(token_a) && quote_mints.contains(token_b) {
        Some(NormalizedPair {
            base_mint: token_a.to_string(),
            quote_mint: token_b.to_string(),
            reversed: false,
        })
    } else if target_base_mints.contains(token_b) && quote_mints.contains(token_a) {
        Some(NormalizedPair {
            base_mint: token_b.to_string(),
            quote_mint: token_a.to_string(),
            reversed: true,
        })
    } else {
        None
    }
}

fn target_decimals(targets: &[TokenPair]) -> HashMap<String, u8> {
    let mut decimals = HashMap::new();
    for pair in targets {
        decimals.insert(pair.base_mint.clone(), pair.base_decimals);
        decimals.insert(pair.quote_mint.clone(), pair.quote_decimals);
    }
    decimals
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{USDC_MINT, WSOL_MINT};
    use serde_json::json;

    fn targets() -> Vec<TokenPair> {
        vec![TokenPair {
            base_mint: "BASE".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 5,
            quote_decimals: 6,
        }]
    }

    #[test]
    fn parses_solana_venue_pools_for_target_quotes() {
        let value = json!({
            "pairs": [
                {
                    "chainId": "solana",
                    "dexId": "orca",
                    "pairAddress": "pool1",
                    "baseToken": {"address": "BASE"},
                    "quoteToken": {"address": WSOL_MINT},
                    "liquidity": {"usd": 1000.0, "quote": "12.5"},
                    "priceNative": "0.25"
                },
                {
                    "chainId": "solana",
                    "dexId": "unrelated",
                    "pairAddress": "pool2",
                    "baseToken": {"address": "BASE"},
                    "quoteToken": {"address": USDC_MINT},
                    "liquidity": {"usd": 1000.0}
                }
            ]
        });
        let quote_mints = [USDC_MINT.to_string(), WSOL_MINT.to_string()]
            .into_iter()
            .collect();
        let venues = [Venue::Orca, Venue::Raydium, Venue::Meteora]
            .into_iter()
            .collect();
        let candidates = parse_index_candidates(&value, &targets(), &quote_mints, &venues, 8, 0.0);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].venue, Venue::Orca);
        assert_eq!(candidates[0].base_mint, "BASE");
        assert_eq!(candidates[0].quote_mint, WSOL_MINT);
        assert_eq!(candidates[0].base_decimals, 5);
        assert_eq!(candidates[0].quote_decimals, 9);
        assert_eq!(candidates[0].liquidity_quote_ui, Some(12.5));
        assert_eq!(candidates[0].price_quote_per_base, Some(0.25));
    }

    #[test]
    fn normalizes_reversed_pool_quote_liquidity_and_price() {
        let value = json!({
            "pairs": [
                {
                    "chainId": "solana",
                    "dexId": "raydium",
                    "pairAddress": "pool1",
                    "baseToken": {"address": USDC_MINT},
                    "quoteToken": {"address": "BASE"},
                    "liquidity": {"usd": 2000.0, "base": "1000.0", "quote": "40.0"},
                    "priceNative": "0.04"
                }
            ]
        });
        let quote_mints = [USDC_MINT.to_string()].into_iter().collect();
        let venues = [Venue::Raydium].into_iter().collect();
        let candidates = parse_index_candidates(&value, &targets(), &quote_mints, &venues, 8, 0.0);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].base_mint, "BASE");
        assert_eq!(candidates[0].quote_mint, USDC_MINT);
        assert_eq!(candidates[0].liquidity_quote_ui, Some(1000.0));
        assert_eq!(candidates[0].price_quote_per_base, Some(25.0));
    }
}
