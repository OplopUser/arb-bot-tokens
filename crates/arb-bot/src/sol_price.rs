use crate::{
    env_runtime::env_value,
    types::{USDC_MINT, WSOL_MINT},
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;
use tracing::warn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolUsdSnapshot {
    pub price_usd: f64,
    pub source: String,
    pub refresh_interval_ms: u64,
    pub updated_unix_ms: Option<u128>,
    pub stale: bool,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
struct SolUsdCache {
    price_usd: f64,
    source: String,
    updated_at: Option<Instant>,
    updated_unix_ms: Option<u128>,
    last_error: Option<String>,
}

#[derive(Clone)]
pub struct SolUsdEstimator {
    client: Client,
    cache: Arc<Mutex<SolUsdCache>>,
}

impl SolUsdEstimator {
    pub fn new(fallback_price_usd: f64) -> Self {
        Self {
            client: Client::new(),
            cache: Arc::new(Mutex::new(SolUsdCache {
                price_usd: sane_fallback(fallback_price_usd),
                source: "config_fallback".to_string(),
                updated_at: None,
                updated_unix_ms: None,
                last_error: None,
            })),
        }
    }

    pub async fn estimate(&self, fallback_price_usd: f64) -> SolUsdSnapshot {
        let refresh_interval = refresh_interval();
        {
            let cache = self.cache.lock().await;
            if cache
                .updated_at
                .map(|updated| updated.elapsed() < refresh_interval)
                .unwrap_or(false)
            {
                return snapshot_from_cache(&cache, refresh_interval, false);
            }
        }

        match self.fetch_jupiter_price().await {
            Ok(price_usd) => {
                let mut cache = self.cache.lock().await;
                cache.price_usd = price_usd;
                cache.source = "jupiter_wsol_usdc_quote".to_string();
                cache.updated_at = Some(Instant::now());
                cache.updated_unix_ms = Some(now_unix_ms());
                cache.last_error = None;
                snapshot_from_cache(&cache, refresh_interval, false)
            }
            Err(error) => {
                let error_text = format!("{error:#}");
                warn!(error = %error_text, "SOL/USD quote refresh failed; using cached or configured estimate");
                let mut cache = self.cache.lock().await;
                if cache.updated_at.is_none() {
                    cache.price_usd = sane_fallback(fallback_price_usd);
                    cache.source = "config_fallback".to_string();
                }
                cache.last_error = Some(error_text);
                snapshot_from_cache(&cache, refresh_interval, cache.updated_at.is_some())
            }
        }
    }

    async fn fetch_jupiter_price(&self) -> anyhow::Result<f64> {
        let amount_lamports = quote_amount_lamports();
        let mut request = self
            .client
            .get(jupiter_quote_url())
            .timeout(request_timeout())
            .query(&[
                ("inputMint", WSOL_MINT),
                ("outputMint", USDC_MINT),
                ("amount", &amount_lamports.to_string()),
                ("slippageBps", "1"),
            ]);
        if let Some(api_key) = env_value("JUPITER_API_KEY") {
            request = request.header("x-api-key", api_key);
        }
        let response = request.send().await?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!("Jupiter SOL/USD quote http {status}: {text}");
        }
        let value: Value = serde_json::from_str(&text)?;
        parse_sol_usd_quote_response(&value, amount_lamports)
    }
}

fn snapshot_from_cache(
    cache: &SolUsdCache,
    refresh_interval: Duration,
    stale: bool,
) -> SolUsdSnapshot {
    SolUsdSnapshot {
        price_usd: cache.price_usd,
        source: cache.source.clone(),
        refresh_interval_ms: refresh_interval.as_millis() as u64,
        updated_unix_ms: cache.updated_unix_ms,
        stale,
        last_error: cache.last_error.clone(),
    }
}

fn parse_sol_usd_quote_response(value: &Value, amount_lamports: u64) -> anyhow::Result<f64> {
    let out_atoms = value
        .get("outAmount")
        .and_then(|value| {
            value
                .as_str()
                .and_then(|text| text.parse::<u128>().ok())
                .or_else(|| value.as_u64().map(u128::from))
        })
        .ok_or_else(|| anyhow::anyhow!("Jupiter SOL/USD quote missing outAmount"))?;
    let sol_amount = amount_lamports as f64 / 1_000_000_000.0;
    if sol_amount <= 0.0 {
        anyhow::bail!("SOL/USD quote amount must be positive");
    }
    let price_usd = out_atoms as f64 / 1_000_000.0 / sol_amount;
    if !(1.0..=1_000.0).contains(&price_usd) {
        anyhow::bail!("SOL/USD quote outside sanity range: {price_usd}");
    }
    Ok(price_usd)
}

fn jupiter_quote_url() -> String {
    let base_url = env_value("JUPITER_API_URL").unwrap_or_else(|| {
        if env_value("JUPITER_API_KEY").is_some() {
            "https://api.jup.ag/swap/v1".to_string()
        } else {
            "https://lite-api.jup.ag/swap/v1".to_string()
        }
    });
    if base_url.ends_with("/quote") {
        base_url
    } else {
        format!("{}/quote", base_url.trim_end_matches('/'))
    }
}

fn refresh_interval() -> Duration {
    Duration::from_millis(
        env_value("SOL_USD_REFRESH_MS")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(60_000)
            .max(5_000),
    )
}

fn request_timeout() -> Duration {
    Duration::from_millis(
        env_value("SOL_USD_TIMEOUT_MS")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(1_500)
            .max(250),
    )
}

fn quote_amount_lamports() -> u64 {
    env_value("SOL_USD_QUOTE_AMOUNT_LAMPORTS")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(1_000_000_000)
        .max(10_000_000)
}

fn sane_fallback(value: f64) -> f64 {
    if value.is_finite() && value > 0.0 {
        value
    } else {
        100.0
    }
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_jupiter_quote_string_out_amount() {
        let price =
            parse_sol_usd_quote_response(&json!({ "outAmount": "154250000" }), 1_000_000_000)
                .unwrap();
        assert!((price - 154.25).abs() < f64::EPSILON);
    }

    #[test]
    fn rejects_implausible_sol_price() {
        let error =
            parse_sol_usd_quote_response(&json!({ "outAmount": "2500000000" }), 1_000_000_000)
                .unwrap_err();
        assert!(error.to_string().contains("sanity range"));
    }
}
