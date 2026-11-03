use crate::{
    config::{
        AggregatorFormat, AggregatorProviderConfig, AggregatorsConfig, JupiterConfig,
        JupiterSeedPair,
    },
    env_runtime::{env_bool as runtime_env_bool, env_value as runtime_env_value},
    types::{
        atoms_to_ui, is_blocklisted_remora_mint, is_pump_like_mint, ui_to_atoms, FeeBreakdown,
        Opportunity, PoolState, Quote, SwapSide, TokenPair, Venue, USDC_MINT, USDT_MINT, WSOL_MINT,
    },
};
use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose, Engine as _};
use futures_util::{stream::FuturesUnordered, SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use reqwest::{header::HeaderName, Client, RequestBuilder, Url};
use rmpv::Value as MsgPackValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{read_keypair_file, Signer},
};
use std::{
    collections::{HashMap, HashSet},
    env, fs,
    hash::{Hash, Hasher},
    io::Cursor,
    str::FromStr,
    sync::{Arc, Mutex as StdMutex, OnceLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, handshake::client::Request, Message},
};
use tracing::{debug, warn};

#[derive(Clone)]
pub struct AggregatorQuoteManager {
    config: AggregatorsConfig,
    client: Client,
    providers: Vec<Arc<ProviderRuntime>>,
    seed_pairs: Vec<TokenPair>,
    dynamic_pairs: Arc<RwLock<Vec<TokenPair>>>,
    last_dynamic_refresh: Arc<Mutex<Option<Instant>>>,
    cache: Arc<RwLock<HashMap<CacheKey, CacheEntry>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatorProviderSnapshot {
    pub name: String,
    pub venue: Venue,
    pub format: AggregatorFormat,
    pub quote_url: String,
    pub requires_api_key: bool,
    pub api_key_env: Option<String>,
    pub missing_auth_envs: Vec<String>,
    pub has_api_key: bool,
    pub min_interval_ms: u64,
    pub max_in_flight: usize,
    pub budget_exhaustion_strikes: u32,
    pub provider_error_strikes: u32,
    pub rate_limit_strikes: u32,
    pub cooldown_reason: Option<String>,
    pub cooldown_remaining_ms: Option<u64>,
    pub next_request_in_ms: Option<u64>,
    pub available: bool,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct AggregatorQuoteRequest {
    pub pair: TokenPair,
    pub side: SwapSide,
    pub in_amount_atoms: u128,
}

impl AggregatorQuoteManager {
    pub fn new(config: AggregatorsConfig, jupiter: JupiterConfig) -> Self {
        let mut providers = Vec::new();
        if jupiter.enabled {
            providers.push(jupiter_provider(&jupiter));
        }
        if config.enabled {
            providers.extend(
                config
                    .providers
                    .iter()
                    .filter(|provider| provider.enabled)
                    .cloned(),
            );
        }

        let mut seed_pairs = HashMap::new();
        for seed in jupiter.seed_pairs.iter().chain(config.seed_pairs.iter()) {
            let pair = seed_pair(seed);
            seed_pairs.entry(pair.key()).or_insert(pair);
        }

        Self {
            config,
            client: Client::new(),
            providers: providers
                .into_iter()
                .map(|config| Arc::new(ProviderRuntime::new(config)))
                .collect(),
            seed_pairs: seed_pairs.into_values().collect(),
            dynamic_pairs: Arc::new(RwLock::new(Vec::new())),
            last_dynamic_refresh: Arc::new(Mutex::new(None)),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn fetch_reference_quotes(
        &self,
        pools: &[PoolState],
        network_fee: &FeeBreakdown,
        trade_size_usd: f64,
        sol_usd_estimate: f64,
        safety_buffer_bps: f64,
    ) -> Vec<Quote> {
        if self.providers.is_empty() {
            return Vec::new();
        }

        let pairs = self.collect_pairs(pools);
        let mut futures = FuturesUnordered::new();
        let mut pending_providers = PendingProviderBudget::default();
        for pair in pairs {
            let in_amount_atoms = ui_to_atoms(
                trade_size_quote_ui(&pair, trade_size_usd, sol_usd_estimate),
                pair.quote_decimals,
            );
            if in_amount_atoms == 0 {
                continue;
            }
            for provider in &self.providers {
                if provider_missing_required_auth(provider) {
                    continue;
                }
                let manager = self.clone();
                let provider = provider.clone();
                let provider_name = provider.config.name.clone();
                let pair = pair.clone();
                let network_fee = network_fee.clone();
                pending_providers.track(&provider_name);
                futures.push(async move {
                    let result = manager
                        .fetch_provider_pair(
                            provider,
                            pair,
                            SwapSide::QuoteToBase,
                            in_amount_atoms,
                            network_fee,
                            safety_buffer_bps,
                        )
                        .await;
                    (provider_name, result)
                });
            }
        }

        let budget_ms = aggregator_quote_budget_ms();
        let deadline = Instant::now() + Duration::from_millis(budget_ms);
        let mut quotes = Vec::new();
        let mut waited_for_reference_result = false;
        while !futures.is_empty() {
            let Some(remaining) = remaining_quote_budget(deadline) else {
                if waited_for_reference_result {
                    cool_down_budget_exhausted_providers(
                        &self.providers,
                        &pending_providers,
                        "aggregator-reference",
                        budget_ms,
                        futures.len(),
                    )
                    .await;
                } else {
                    warn_budget_exhausted_without_cooldown(
                        "aggregator-reference",
                        budget_ms,
                        futures.len(),
                    );
                }
                break;
            };
            waited_for_reference_result = true;
            let Some(result) = (match tokio::time::timeout(remaining, futures.next()).await {
                Ok(result) => result,
                Err(_) => {
                    cool_down_budget_exhausted_providers(
                        &self.providers,
                        &pending_providers,
                        "aggregator-reference",
                        budget_ms,
                        futures.len(),
                    )
                    .await;
                    break;
                }
            }) else {
                break;
            };
            let (provider_name, result) = result;
            pending_providers.complete(&provider_name);
            match result {
                Ok(Some(quote)) => quotes.push(quote),
                Ok(None) => {}
                Err(error) => warn!(?error, "aggregator reference quote failed"),
            }
        }
        quotes
    }

    pub async fn fetch_trade_source_opportunities(
        &self,
        pairs: &[TokenPair],
        network_fee: &FeeBreakdown,
        trade_size_usd: f64,
        sol_usd_estimate: f64,
        safety_buffer_bps: f64,
    ) -> Vec<Opportunity> {
        if self.providers.len() < 2 {
            return Vec::new();
        }

        let mut pairs = pairs.to_vec();
        pairs.truncate(self.config.max_pairs_per_cycle);

        let mut buy_futures = FuturesUnordered::new();
        let mut pending_buy_providers = PendingProviderBudget::default();
        for pair in pairs {
            let input_quote_atoms = ui_to_atoms(
                trade_size_quote_ui(&pair, trade_size_usd, sol_usd_estimate),
                pair.quote_decimals,
            );
            if input_quote_atoms == 0 {
                continue;
            }
            for provider in &self.providers {
                if provider_missing_required_auth(provider) {
                    continue;
                }
                let manager = self.clone();
                let provider = provider.clone();
                let provider_name = provider.config.name.clone();
                let pair = pair.clone();
                let network_fee = network_fee.clone();
                pending_buy_providers.track(&provider_name);
                buy_futures.push(async move {
                    let quote = manager
                        .fetch_provider_pair(
                            provider.clone(),
                            pair,
                            SwapSide::QuoteToBase,
                            input_quote_atoms,
                            network_fee,
                            safety_buffer_bps,
                        )
                        .await;
                    (provider_name, input_quote_atoms, quote)
                });
            }
        }

        let buy_budget_ms = aggregator_quote_budget_ms();
        let buy_deadline = Instant::now() + Duration::from_millis(buy_budget_ms);
        let mut buy_quotes_by_pair = HashMap::<String, Vec<(String, u128, Quote)>>::new();
        let mut waited_for_buy_result = false;
        while !buy_futures.is_empty() {
            let Some(remaining) = remaining_quote_budget(buy_deadline) else {
                if waited_for_buy_result {
                    cool_down_budget_exhausted_providers(
                        &self.providers,
                        &pending_buy_providers,
                        "trade-source-buy",
                        buy_budget_ms,
                        buy_futures.len(),
                    )
                    .await;
                } else {
                    warn_budget_exhausted_without_cooldown(
                        "trade-source-buy",
                        buy_budget_ms,
                        buy_futures.len(),
                    );
                }
                break;
            };
            waited_for_buy_result = true;
            let Some((provider_name, input_quote_atoms, result)) =
                (match tokio::time::timeout(remaining, buy_futures.next()).await {
                    Ok(result) => result,
                    Err(_) => {
                        cool_down_budget_exhausted_providers(
                            &self.providers,
                            &pending_buy_providers,
                            "trade-source-buy",
                            buy_budget_ms,
                            buy_futures.len(),
                        )
                        .await;
                        break;
                    }
                })
            else {
                break;
            };
            pending_buy_providers.complete(&provider_name);
            match result {
                Ok(Some(quote)) => {
                    buy_quotes_by_pair
                        .entry(quote.pair.key())
                        .or_default()
                        .push((provider_name, input_quote_atoms, quote));
                }
                Ok(None) => {}
                Err(error) => warn!(?error, "trade-source buy quote failed"),
            }
        }

        let mut sell_futures = FuturesUnordered::new();
        let mut pending_sell_providers = PendingProviderBudget::default();
        for mut buy_quotes in buy_quotes_by_pair.into_values() {
            buy_quotes.sort_by(|a, b| {
                b.2.out_amount_atoms
                    .cmp(&a.2.out_amount_atoms)
                    .then_with(|| a.0.cmp(&b.0))
            });
            buy_quotes.truncate(max_buy_routes_per_pair());

            for (buy_provider_name, input_quote_atoms, buy_quote) in buy_quotes {
                for sell_provider in &self.providers {
                    if sell_provider.config.name == buy_provider_name {
                        continue;
                    }
                    if provider_missing_required_auth(sell_provider) {
                        continue;
                    }
                    let manager = self.clone();
                    let sell_provider = sell_provider.clone();
                    let sell_provider_name = sell_provider.config.name.clone();
                    let buy_quote = buy_quote.clone();
                    let network_fee = network_fee.clone();
                    pending_sell_providers.track(&sell_provider_name);
                    sell_futures.push(async move {
                        let result = manager
                            .fetch_provider_pair(
                                sell_provider.clone(),
                                buy_quote.pair.clone(),
                                SwapSide::BaseToQuote,
                                buy_quote.out_amount_atoms,
                                network_fee,
                                safety_buffer_bps,
                            )
                            .await;
                        (sell_provider_name, input_quote_atoms, buy_quote, result)
                    });
                }
            }
        }

        let mut opportunities = Vec::new();
        let sell_budget_ms = aggregator_quote_budget_ms();
        let sell_deadline = Instant::now() + Duration::from_millis(sell_budget_ms);
        let mut waited_for_sell_result = false;
        while !sell_futures.is_empty() {
            let Some(remaining) = remaining_quote_budget(sell_deadline) else {
                if waited_for_sell_result {
                    cool_down_budget_exhausted_providers(
                        &self.providers,
                        &pending_sell_providers,
                        "trade-source-sell",
                        sell_budget_ms,
                        sell_futures.len(),
                    )
                    .await;
                } else {
                    warn_budget_exhausted_without_cooldown(
                        "trade-source-sell",
                        sell_budget_ms,
                        sell_futures.len(),
                    );
                }
                break;
            };
            waited_for_sell_result = true;
            let Some((sell_provider_name, input_quote_atoms, buy_quote, result)) =
                (match tokio::time::timeout(remaining, sell_futures.next()).await {
                    Ok(result) => result,
                    Err(_) => {
                        cool_down_budget_exhausted_providers(
                            &self.providers,
                            &pending_sell_providers,
                            "trade-source-sell",
                            sell_budget_ms,
                            sell_futures.len(),
                        )
                        .await;
                        break;
                    }
                })
            else {
                break;
            };
            pending_sell_providers.complete(&sell_provider_name);
            match result {
                Ok(Some(sell_quote)) => {
                    if let Some(opportunity) = quote_roundtrip_opportunity(
                        &buy_quote,
                        &sell_quote,
                        input_quote_atoms,
                        sol_usd_estimate,
                        network_fee,
                    ) {
                        opportunities.push(opportunity);
                    }
                }
                Ok(None) => {}
                Err(error) => warn!(?error, "trade-source sell quote failed"),
            }
        }

        opportunities.sort_by(|a, b| {
            b.net_profit_usd
                .partial_cmp(&a.net_profit_usd)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        opportunities
    }

    pub async fn fetch_explicit_quotes_with_budget(
        &self,
        requests: Vec<AggregatorQuoteRequest>,
        network_fee: &FeeBreakdown,
        safety_buffer_bps: f64,
        budget_ms: Option<u64>,
    ) -> Vec<Quote> {
        if self.providers.is_empty() || requests.is_empty() {
            return Vec::new();
        }

        let mut futures = FuturesUnordered::new();
        let mut pending_providers = PendingProviderBudget::default();
        for request in requests {
            if request.in_amount_atoms == 0 {
                continue;
            }
            for provider in &self.providers {
                if provider_missing_required_auth(provider) {
                    continue;
                }
                let manager = self.clone();
                let provider = provider.clone();
                let provider_name = provider.config.name.clone();
                let network_fee = network_fee.clone();
                let request = request.clone();
                pending_providers.track(&provider_name);
                futures.push(async move {
                    let result = manager
                        .fetch_provider_pair(
                            provider,
                            request.pair,
                            request.side,
                            request.in_amount_atoms,
                            network_fee,
                            safety_buffer_bps,
                        )
                        .await;
                    (provider_name, result)
                });
            }
        }

        let mut quotes = Vec::new();
        let deadline = budget_ms
            .filter(|value| *value > 0)
            .map(|value| Instant::now() + Duration::from_millis(value));
        loop {
            let result = if let Some(deadline) = deadline {
                let now = Instant::now();
                if now >= deadline {
                    cool_down_budget_exhausted_providers(
                        &self.providers,
                        &pending_providers,
                        "explicit-aggregator",
                        budget_ms.unwrap_or_default(),
                        futures.len(),
                    )
                    .await;
                    break;
                }
                match tokio::time::timeout(deadline.duration_since(now), futures.next()).await {
                    Ok(result) => result,
                    Err(_) => {
                        cool_down_budget_exhausted_providers(
                            &self.providers,
                            &pending_providers,
                            "explicit-aggregator",
                            budget_ms.unwrap_or_default(),
                            futures.len(),
                        )
                        .await;
                        break;
                    }
                }
            } else {
                futures.next().await
            };
            let Some(result) = result else {
                break;
            };
            let (provider_name, result) = result;
            pending_providers.complete(&provider_name);
            match result {
                Ok(Some(quote)) => quotes.push(quote),
                Ok(None) => {}
                Err(error) => warn!(?error, "explicit aggregator quote failed"),
            }
        }
        quotes
    }

    pub async fn scan_pairs_dynamic(
        &self,
        pools: &[PoolState],
        quote_mints: &[String],
        denylist: &[String],
    ) -> Vec<TokenPair> {
        self.refresh_dynamic_pairs(quote_mints, denylist).await;
        let dynamic_pairs = self.dynamic_pairs.read().await.clone();
        self.collect_pairs_with_dynamic_unlimited(pools, &dynamic_pairs)
    }

    pub async fn provider_snapshots(&self) -> Vec<AggregatorProviderSnapshot> {
        let mut snapshots = Vec::with_capacity(self.providers.len());
        for provider in &self.providers {
            snapshots.push(provider.snapshot().await);
        }
        snapshots
    }

    fn collect_pairs(&self, pools: &[PoolState]) -> Vec<TokenPair> {
        self.collect_pairs_with_dynamic(pools, &[])
    }

    fn collect_pairs_with_dynamic(
        &self,
        pools: &[PoolState],
        dynamic_pairs: &[TokenPair],
    ) -> Vec<TokenPair> {
        let mut pairs = self.collect_pairs_with_dynamic_unlimited(pools, dynamic_pairs);
        pairs.truncate(self.config.max_pairs_per_cycle);
        pairs
    }

    fn collect_pairs_with_dynamic_unlimited(
        &self,
        pools: &[PoolState],
        dynamic_pairs: &[TokenPair],
    ) -> Vec<TokenPair> {
        let mut seen = std::collections::HashSet::new();
        let mut pairs = Vec::new();

        for pair in dynamic_pairs {
            if !token_pair_is_eligible(pair) {
                continue;
            }
            if seen.insert(pair.key()) {
                pairs.push(pair.clone());
            }
        }
        for pair in &self.seed_pairs {
            if !token_pair_is_eligible(pair) {
                continue;
            }
            if seen.insert(pair.key()) {
                pairs.push(pair.clone());
            }
        }
        for pool in pools {
            if !token_pair_is_eligible(&pool.pair) {
                continue;
            }
            if seen.insert(pool.pair.key()) {
                pairs.push(pool.pair.clone());
            }
        }

        pairs
    }

    async fn refresh_dynamic_pairs(&self, quote_mints: &[String], denylist: &[String]) {
        if !dynamic_token_discovery_enabled(&self.config) {
            return;
        }

        let refresh_interval = Duration::from_millis(dynamic_refresh_interval_ms(&self.config));
        {
            let mut last_refresh = self.last_dynamic_refresh.lock().await;
            if last_refresh
                .map(|last| Instant::now().duration_since(last) < refresh_interval)
                .unwrap_or(false)
            {
                return;
            }
            *last_refresh = Some(Instant::now());
        }

        match self.fetch_dynamic_pairs(quote_mints, denylist).await {
            Ok(pairs) => {
                if !pairs.is_empty() {
                    *self.dynamic_pairs.write().await = pairs;
                }
            }
            Err(error) => warn!(?error, "dynamic Jupiter token discovery failed"),
        }
    }

    async fn fetch_dynamic_pairs(
        &self,
        quote_mints: &[String],
        denylist: &[String],
    ) -> Result<Vec<TokenPair>> {
        let api_key = env::var("JUPITER_API_KEY")
            .ok()
            .filter(|key| !key.trim().is_empty());
        let endpoints = if api_key.is_some() {
            dynamic_token_endpoints()
        } else {
            debug!(
                "JUPITER_API_KEY is not configured; using public GeckoTerminal/DexScreener token discovery"
            );
            Vec::new()
        };
        let mut futures = FuturesUnordered::new();
        if let Some(api_key) = api_key {
            for endpoint in endpoints {
                let client = self.client.clone();
                let api_key = api_key.clone();
                futures.push(async move {
                    let url = format!(
                        "{}/{}",
                        dynamic_token_api_base_url().trim_end_matches('/'),
                        endpoint.trim_start_matches('/')
                    );
                    let response = client
                        .get(url)
                        .header("x-api-key", api_key)
                        .send()
                        .await
                        .context("Jupiter token discovery request failed")?;
                    let status = response.status();
                    if !status.is_success() {
                        let body = response.text().await.unwrap_or_default();
                        return Err(anyhow!("Jupiter token discovery http {status}: {body}"));
                    }
                    response
                        .json::<Value>()
                        .await
                        .context("invalid Jupiter token discovery json")
                });
            }
        }

        let quote_mint_order = dynamic_quote_mints(quote_mints);
        let quote_mints = quote_mint_order
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        let denylist = denylist
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        let mut tokens_by_mint = HashMap::<String, TokenDiscoveryToken>::new();
        let mut successful_sources = 0usize;
        while let Some(result) = futures.next().await {
            let value = match result {
                Ok(value) => value,
                Err(error) => {
                    warn!(?error, "Jupiter token discovery source failed");
                    continue;
                }
            };
            successful_sources = successful_sources.saturating_add(1);
            for token in parse_dynamic_tokens(&value) {
                if !token_is_eligible(&token, &quote_mints, &denylist, &self.config) {
                    continue;
                }
                upsert_discovery_token(&mut tokens_by_mint, token);
            }
        }

        if successful_sources == 0 || tokens_by_mint.is_empty() {
            let gecko_pairs = self
                .fetch_geckoterminal_dynamic_pairs(
                    quote_mint_order.clone(),
                    quote_mints.clone(),
                    denylist.clone(),
                )
                .await?;
            if !gecko_pairs.is_empty() {
                return Ok(gecko_pairs);
            }
            return self
                .fetch_dexscreener_dynamic_pairs(quote_mint_order, quote_mints, denylist)
                .await;
        }

        let mut tokens = tokens_by_mint.into_values().collect::<Vec<_>>();
        tokens.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        tokens.truncate(dynamic_max_tokens(&self.config));

        let mut jupiter_pairs = Vec::new();
        for token in tokens {
            for quote_mint in
                quote_mints_for_discovered_token(&token, &quote_mint_order, &self.config)
            {
                jupiter_pairs.push(TokenPair {
                    base_mint: token.mint.clone(),
                    quote_mint: quote_mint.to_string(),
                    base_decimals: token.decimals,
                    quote_decimals: decimals_for_quote_mint(&quote_mint),
                });
            }
        }

        let mut pairs = self
            .fetch_geckoterminal_dynamic_pairs(
                quote_mint_order.clone(),
                quote_mints.clone(),
                denylist.clone(),
            )
            .await
            .unwrap_or_default();
        let mut seen = pairs
            .iter()
            .map(TokenPair::key)
            .collect::<std::collections::HashSet<_>>();
        for pair in jupiter_pairs {
            if seen.insert(pair.key()) {
                pairs.push(pair);
            }
        }
        if pairs.is_empty() {
            return self
                .fetch_dexscreener_dynamic_pairs(quote_mint_order, quote_mints, denylist)
                .await;
        }
        pairs.truncate(dynamic_max_tokens(&self.config) * dynamic_max_quote_mints(&self.config));
        Ok(pairs)
    }

    async fn fetch_geckoterminal_dynamic_pairs(
        &self,
        quote_mint_order: Vec<String>,
        quote_mints: std::collections::HashSet<String>,
        denylist: std::collections::HashSet<String>,
    ) -> Result<Vec<TokenPair>> {
        let mut futures = FuturesUnordered::new();
        for endpoint in geckoterminal_discovery_endpoints() {
            let client = self.client.clone();
            futures.push(async move {
                let response = client
                    .get(&endpoint)
                    .header("accept", "application/json")
                    .header("user-agent", "arb-bot/0.1")
                    .send()
                    .await
                    .with_context(|| {
                        format!("GeckoTerminal discovery request failed for {endpoint}")
                    })?;
                let status = response.status();
                if !status.is_success() {
                    let body = response.text().await.unwrap_or_default();
                    return Err(anyhow!("GeckoTerminal discovery http {status}: {body}"));
                }
                response
                    .json::<Value>()
                    .await
                    .context("invalid GeckoTerminal discovery json")
            });
        }

        let allowed_dexes = geckoterminal_allowed_dex_ids();
        let mut tokens_by_mint = HashMap::<String, TokenDiscoveryToken>::new();
        while let Some(result) = futures.next().await {
            let value = match result {
                Ok(value) => value,
                Err(error) => {
                    warn!(?error, "GeckoTerminal token discovery source failed");
                    continue;
                }
            };
            for token in parse_geckoterminal_tokens(
                &value,
                &quote_mints,
                &allowed_dexes,
                dynamic_min_organic_score(&self.config),
            ) {
                if !token_is_eligible(&token, &quote_mints, &denylist, &self.config) {
                    continue;
                }
                upsert_discovery_token(&mut tokens_by_mint, token);
            }
        }

        let mut tokens = tokens_by_mint.into_values().collect::<Vec<_>>();
        tokens.retain(|token| {
            token.observed_venue_families.len()
                >= venue_discovery_min_supported_venues(&self.config)
        });
        tokens.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        tokens.truncate(dynamic_max_tokens(&self.config));

        let mut pairs = Vec::new();
        for token in tokens {
            for quote_mint in
                quote_mints_for_discovered_token(&token, &quote_mint_order, &self.config)
            {
                pairs.push(TokenPair {
                    base_mint: token.mint.clone(),
                    quote_mint: quote_mint.to_string(),
                    base_decimals: token.decimals,
                    quote_decimals: decimals_for_quote_mint(&quote_mint),
                });
            }
        }
        Ok(pairs)
    }

    async fn fetch_dexscreener_dynamic_pairs(
        &self,
        quote_mint_order: Vec<String>,
        quote_mints: std::collections::HashSet<String>,
        denylist: std::collections::HashSet<String>,
    ) -> Result<Vec<TokenPair>> {
        let mints = self.fetch_dexscreener_candidate_mints().await?;
        if mints.is_empty() {
            return Ok(Vec::new());
        }

        let candidate_mints = mints
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        let mut futures = FuturesUnordered::new();
        for chunk in mints.chunks(30) {
            let client = self.client.clone();
            let joined = chunk.join(",");
            futures.push(async move {
                let url = format!("https://api.dexscreener.com/tokens/v1/solana/{joined}");
                let response = client
                    .get(url)
                    .send()
                    .await
                    .context("DexScreener token pairs request failed")?;
                let status = response.status();
                if !status.is_success() {
                    let body = response.text().await.unwrap_or_default();
                    return Err(anyhow!("DexScreener token pairs http {status}: {body}"));
                }
                response
                    .json::<Value>()
                    .await
                    .context("invalid DexScreener token pairs json")
            });
        }

        let mut tokens_by_mint = HashMap::<String, TokenDiscoveryToken>::new();
        while let Some(result) = futures.next().await {
            let value = match result {
                Ok(value) => value,
                Err(error) => {
                    warn!(?error, "DexScreener token pairs source failed");
                    continue;
                }
            };
            for token in parse_dexscreener_tokens(
                &value,
                &candidate_mints,
                &quote_mints,
                dynamic_min_organic_score(&self.config),
            ) {
                if !token_is_eligible(&token, &quote_mints, &denylist, &self.config) {
                    continue;
                }
                upsert_discovery_token(&mut tokens_by_mint, token);
            }
        }

        let mut tokens = tokens_by_mint.into_values().collect::<Vec<_>>();
        tokens.retain(|token| {
            token.observed_venue_families.len()
                >= venue_discovery_min_supported_venues(&self.config)
        });
        tokens.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        tokens.truncate(dynamic_max_tokens(&self.config));

        let mut pairs = Vec::new();
        for token in tokens {
            for quote_mint in
                quote_mints_for_discovered_token(&token, &quote_mint_order, &self.config)
            {
                pairs.push(TokenPair {
                    base_mint: token.mint.clone(),
                    quote_mint: quote_mint.to_string(),
                    base_decimals: token.decimals,
                    quote_decimals: decimals_for_quote_mint(&quote_mint),
                });
            }
        }
        Ok(pairs)
    }

    async fn fetch_dexscreener_candidate_mints(&self) -> Result<Vec<String>> {
        let endpoints = [
            "https://api.dexscreener.com/token-boosts/top/v1",
            "https://api.dexscreener.com/token-boosts/latest/v1",
            "https://api.dexscreener.com/token-profiles/latest/v1",
        ];
        let mut futures = FuturesUnordered::new();
        for endpoint in endpoints {
            let client = self.client.clone();
            futures.push(async move {
                let response = client.get(endpoint).send().await.with_context(|| {
                    format!("DexScreener discovery request failed for {endpoint}")
                })?;
                let status = response.status();
                if !status.is_success() {
                    let body = response.text().await.unwrap_or_default();
                    return Err(anyhow!("DexScreener discovery http {status}: {body}"));
                }
                response
                    .json::<Value>()
                    .await
                    .context("invalid DexScreener discovery json")
            });
        }

        let mut mints = Vec::new();
        while let Some(result) = futures.next().await {
            let value = match result {
                Ok(value) => value,
                Err(error) => {
                    warn!(?error, "DexScreener token discovery source failed");
                    continue;
                }
            };
            for item in token_array(&value) {
                if item.get("chainId").and_then(Value::as_str) != Some("solana") {
                    continue;
                }
                if let Some(mint) = item.get("tokenAddress").and_then(Value::as_str) {
                    mints.push(mint.to_string());
                }
            }
        }
        mints.sort();
        mints.dedup();
        Ok(mints)
    }

    async fn fetch_provider_pair(
        &self,
        provider: Arc<ProviderRuntime>,
        pair: TokenPair,
        side: SwapSide,
        in_amount_atoms: u128,
        network_fee: FeeBreakdown,
        safety_buffer_bps: f64,
    ) -> Result<Option<Quote>> {
        if provider.config.requires_api_key && !provider.has_required_auth() {
            debug!(
                provider = provider.config.name,
                "skipping provider without api key"
            );
            return Ok(None);
        }

        let cache_key = CacheKey::new(&provider.config.name, &pair, side, in_amount_atoms);
        if let Some(quote) = self.cached(&cache_key).await {
            return Ok(Some(quote));
        }

        let _permit = provider
            .semaphore
            .acquire()
            .await
            .context("aggregator semaphore closed")?;
        if provider.in_cooldown().await {
            return Ok(None);
        }
        provider.wait_for_turn().await;

        if matches!(provider.config.format, AggregatorFormat::TitanStream) {
            let quote = match self
                .request_titan_quote(
                    &provider.config,
                    provider.api_key(),
                    &pair,
                    side,
                    in_amount_atoms,
                    &network_fee,
                    safety_buffer_bps,
                )
                .await
            {
                Ok(quote) => quote,
                Err(error) => {
                    provider.cool_down_for_provider_error(&error).await;
                    return Err(error);
                }
            };
            if let Some(quote) = &quote {
                self.cache.write().await.insert(
                    cache_key,
                    CacheEntry {
                        quote: quote.clone(),
                        expires_at: Instant::now()
                            + Duration::from_millis(self.config.cache_ttl_ms),
                    },
                );
            }
            return Ok(quote);
        }

        let response_json = match self
            .request_quote(
                &provider.config,
                provider.api_key(),
                &pair,
                side,
                in_amount_atoms,
            )
            .await
        {
            Ok(value) => value,
            Err(error) => {
                provider.cool_down_for_provider_error(&error).await;
                return Err(error);
            }
        };
        let quote = quote_from_json(
            &provider.config,
            &pair,
            side,
            in_amount_atoms,
            &response_json,
            &network_fee,
            safety_buffer_bps,
        );
        if let Some(quote) = &quote {
            self.cache.write().await.insert(
                cache_key,
                CacheEntry {
                    quote: quote.clone(),
                    expires_at: Instant::now() + Duration::from_millis(self.config.cache_ttl_ms),
                },
            );
        }
        Ok(quote)
    }

    async fn cached(&self, cache_key: &CacheKey) -> Option<Quote> {
        let cache = self.cache.read().await;
        let entry = cache.get(cache_key)?;
        if entry.expires_at > Instant::now() {
            return Some(entry.quote.clone());
        }
        None
    }

    async fn request_quote(
        &self,
        provider: &AggregatorProviderConfig,
        api_key: Option<String>,
        pair: &TokenPair,
        side: SwapSide,
        in_amount_atoms: u128,
    ) -> Result<Value> {
        let params = request_params(provider.format, pair, side, in_amount_atoms);
        let timeout_ms = if provider.timeout_ms == 0 {
            self.config.request_timeout_ms
        } else {
            provider.timeout_ms
        };

        let mut request = if matches!(provider.format, AggregatorFormat::Okx) {
            let url = url_with_query(&provider.quote_url, &params)
                .with_context(|| format!("invalid OKX quote url for {}", provider.name))?;
            okx_signed_request(
                self.client
                    .get(url.clone())
                    .timeout(Duration::from_millis(timeout_ms)),
                &url,
                api_key.clone().context("OKX_API_KEY is required")?,
            )?
        } else if matches!(provider.format, AggregatorFormat::Vybe) {
            self.client
                .get(&provider.quote_url)
                .json(&vybe_quote_body(pair, side, in_amount_atoms, None))
                .timeout(Duration::from_millis(timeout_ms))
        } else if matches!(provider.format, AggregatorFormat::Venum) {
            let mut request = self
                .client
                .post(&provider.quote_url)
                .json(&venum_quote_body(pair, side, in_amount_atoms))
                .timeout(Duration::from_millis(timeout_ms));
            if let Some(value) = api_key.clone() {
                request = request.header("X-API-Key", value);
            }
            request
        } else {
            self.client
                .get(&provider.quote_url)
                .query(&params)
                .timeout(Duration::from_millis(timeout_ms))
        };
        if !matches!(
            provider.format,
            AggregatorFormat::Okx | AggregatorFormat::Venum
        ) {
            if let (Some(header), Some(value)) = (provider.api_key_header.as_ref(), api_key) {
                let name = HeaderName::from_bytes(header.as_bytes())
                    .with_context(|| format!("invalid header name for {}", provider.name))?;
                request = request.header(name, value);
            }
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("{} quote request failed", provider.name))?;
        let status = response.status();
        if !status.is_success() {
            let retry_after_seconds = response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok());
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "{} quote http {status}: retry_after_seconds={}: {body}",
                provider.name,
                retry_after_seconds.unwrap_or(0)
            ));
        }
        response
            .json()
            .await
            .with_context(|| format!("invalid {} quote json", provider.name))
    }

    async fn request_titan_quote(
        &self,
        provider: &AggregatorProviderConfig,
        api_key: Option<String>,
        pair: &TokenPair,
        side: SwapSide,
        in_amount_atoms: u128,
        network_fee: &FeeBreakdown,
        safety_buffer_bps: f64,
    ) -> Result<Option<Quote>> {
        let wallet = titan_wallet_pubkey()?;
        let amount_atoms: u64 = in_amount_atoms
            .try_into()
            .context("Titan quote input amount does not fit into u64")?;
        let mut request = provider
            .quote_url
            .as_str()
            .into_client_request()
            .with_context(|| format!("invalid Titan websocket URL for {}", provider.name))?;
        let header = HeaderName::from_bytes(
            provider
                .api_key_header
                .as_deref()
                .unwrap_or("Authorization")
                .as_bytes(),
        )
        .with_context(|| format!("invalid Titan auth header for {}", provider.name))?;
        request.headers_mut().insert(
            header,
            api_key
                .context("TITAN_API_TOKEN is required")?
                .parse()
                .context("invalid Titan auth header value")?,
        );
        apply_titan_ws_protocol(&mut request)?;

        let (mut ws, _) = connect_async(request)
            .await
            .context("Titan websocket connect failed")?;
        let payload = titan_quote_request_msgpack(&wallet, pair, side, amount_atoms)?;
        let mut bytes = Vec::new();
        rmpv::encode::write_value(&mut bytes, &payload)
            .context("failed to encode Titan quote request")?;
        ws.send(Message::Binary(bytes.into()))
            .await
            .context("Titan websocket send failed")?;

        let timeout = Duration::from_millis(provider.timeout_ms.max(1_000));
        let started = Instant::now();
        while started.elapsed() < timeout {
            let remaining = timeout.saturating_sub(started.elapsed());
            let Some(message) = tokio::time::timeout(remaining, ws.next())
                .await
                .context("Titan quote timed out")?
            else {
                break;
            };
            let message = message.context("Titan websocket message failed")?;
            let Message::Binary(data) = message else {
                continue;
            };
            let value = rmpv::decode::read_value(&mut Cursor::new(data))
                .context("failed to decode Titan MessagePack response")?;
            if let Some(route) = titan_best_route(&value) {
                return Ok(titan_quote_from_route(
                    provider,
                    pair,
                    side,
                    in_amount_atoms,
                    route,
                    network_fee,
                    safety_buffer_bps,
                ));
            }
        }

        Ok(None)
    }
}

fn trade_size_quote_ui(pair: &TokenPair, trade_size_usd: f64, sol_usd_estimate: f64) -> f64 {
    if pair.quote_mint == WSOL_MINT {
        trade_size_usd / sol_usd_estimate.max(1.0)
    } else {
        trade_size_usd
    }
}

struct ProviderRuntime {
    config: AggregatorProviderConfig,
    semaphore: Semaphore,
    next_allowed: Mutex<Instant>,
    cooldown_until: Mutex<Option<Instant>>,
    cooldown_reason: Mutex<Option<String>>,
    budget_exhaustion_strikes: Mutex<u32>,
    provider_error_strikes: Mutex<u32>,
    rate_limit_strikes: Mutex<u32>,
}

#[derive(Default)]
struct PendingProviderBudget {
    pending_by_provider: HashMap<String, usize>,
    responded_providers: HashSet<String>,
}

impl PendingProviderBudget {
    fn track(&mut self, provider_name: &str) {
        *self
            .pending_by_provider
            .entry(provider_name.to_string())
            .or_default() += 1;
    }

    fn complete(&mut self, provider_name: &str) {
        self.responded_providers.insert(provider_name.to_string());
        let Some(pending) = self.pending_by_provider.get_mut(provider_name) else {
            return;
        };
        *pending = pending.saturating_sub(1);
        if *pending == 0 {
            self.pending_by_provider.remove(provider_name);
        }
    }

    #[cfg(test)]
    fn unresponsive_pending_provider_names(&self) -> Vec<String> {
        let mut names = self
            .pending_by_provider
            .iter()
            .filter(|&(name, pending)| *pending > 0 && !self.responded_providers.contains(name))
            .map(|(name, _pending)| name.clone())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    fn pending_provider_names(&self) -> Vec<String> {
        let mut names = self
            .pending_by_provider
            .iter()
            .filter(|&(_name, pending)| *pending > 0)
            .map(|(name, _pending)| name.clone())
            .collect::<Vec<_>>();
        names.sort();
        names
    }
}

async fn cool_down_budget_exhausted_providers(
    providers: &[Arc<ProviderRuntime>],
    pending: &PendingProviderBudget,
    phase: &'static str,
    budget_ms: u64,
    pending_futures: usize,
) {
    let provider_names = pending.pending_provider_names();
    if provider_names.is_empty() {
        warn!(
            phase,
            budget_ms,
            pending = pending_futures,
            "aggregator quote budget exhausted; continuing with partial results"
        );
        return;
    }

    let provider_set = provider_names.iter().cloned().collect::<HashSet<_>>();
    let mut cooldowns = HashMap::new();
    for provider in providers {
        if provider_set.contains(&provider.config.name) {
            let cooldown = provider.cool_down_for_budget_exhaustion().await;
            cooldowns.insert(provider.config.name.clone(), cooldown.as_millis() as u64);
        }
    }
    warn!(
        phase,
        budget_ms,
        pending = pending_futures,
        cooldown_ms_by_provider = ?cooldowns,
        providers = ?provider_names,
        "aggregator quote budget exhausted; adaptively cooling unresponsive providers and continuing with partial results"
    );
}

fn warn_budget_exhausted_without_cooldown(
    phase: &'static str,
    budget_ms: u64,
    pending_futures: usize,
) {
    warn!(
        phase,
        budget_ms,
        pending = pending_futures,
        "aggregator quote budget exhausted before provider polling in this phase; continuing with partial results"
    );
}

fn provider_missing_required_auth(provider: &ProviderRuntime) -> bool {
    provider.config.requires_api_key && !provider.has_required_auth()
}

impl ProviderRuntime {
    fn new(config: AggregatorProviderConfig) -> Self {
        let max_in_flight = config.max_in_flight.max(1);
        let cooldown_until = persisted_provider_cooldown_until(&config.name);
        Self {
            config,
            semaphore: Semaphore::new(max_in_flight),
            next_allowed: Mutex::new(Instant::now()),
            cooldown_until: Mutex::new(cooldown_until),
            cooldown_reason: Mutex::new(None),
            budget_exhaustion_strikes: Mutex::new(0),
            provider_error_strikes: Mutex::new(0),
            rate_limit_strikes: Mutex::new(0),
        }
    }

    fn api_key(&self) -> Option<String> {
        let key = provider_api_key(&self.config)?;
        Some(format!(
            "{}{}",
            self.config.api_key_prefix.as_deref().unwrap_or_default(),
            key
        ))
    }

    fn has_required_auth(&self) -> bool {
        self.config.requires_api_key && self.missing_auth_envs().is_empty()
    }

    fn missing_auth_envs(&self) -> Vec<String> {
        missing_provider_auth_envs(&self.config)
    }

    async fn wait_for_turn(&self) {
        let mut next_allowed = self.next_allowed.lock().await;
        let now = Instant::now();
        if *next_allowed > now {
            tokio::time::sleep(*next_allowed - now).await;
        }
        *next_allowed = Instant::now() + Duration::from_millis(self.config.min_interval_ms.max(1));
    }

    async fn in_cooldown(&self) -> bool {
        let cooldown_until = self.cooldown_until.lock().await;
        cooldown_until
            .map(|until| until > Instant::now())
            .unwrap_or(false)
    }

    async fn cool_down(&self, duration: Duration, reason: &'static str) {
        *self.cooldown_until.lock().await = Some(Instant::now() + duration);
        *self.cooldown_reason.lock().await = Some(reason.to_string());
        persist_provider_cooldown(&self.config.name, duration);
    }

    async fn cool_down_for_budget_exhaustion(&self) -> Duration {
        let mut strikes = self.budget_exhaustion_strikes.lock().await;
        *strikes = strikes.saturating_add(1).min(16);
        let cooldown = budget_exhaustion_cooldown_for_strike(*strikes);
        drop(strikes);
        self.cool_down(cooldown, "quote budget miss").await;
        cooldown
    }

    async fn cool_down_for_provider_error(&self, error: &anyhow::Error) -> Option<Duration> {
        let cooldown = if is_rate_limit_error(error) {
            let mut strikes = self.rate_limit_strikes.lock().await;
            *strikes = strikes.saturating_add(1).min(16);
            drop(strikes);
            (
                provider_cooldown_for_error(error).unwrap_or_else(|| rate_limit_cooldown(error)),
                "rate limit",
            )
        } else if is_transient_provider_error(error) {
            let mut strikes = self.provider_error_strikes.lock().await;
            *strikes = strikes.saturating_add(1).min(16);
            let cooldown = transient_error_cooldown_for_strike(*strikes);
            drop(strikes);
            (cooldown, "transient provider error")
        } else {
            return None;
        };
        self.cool_down(cooldown.0, cooldown.1).await;
        Some(cooldown.0)
    }

    async fn budget_exhaustion_strikes(&self) -> u32 {
        *self.budget_exhaustion_strikes.lock().await
    }

    async fn provider_error_strikes(&self) -> u32 {
        *self.provider_error_strikes.lock().await
    }

    async fn rate_limit_strikes(&self) -> u32 {
        *self.rate_limit_strikes.lock().await
    }

    async fn cooldown_reason(&self) -> Option<String> {
        self.cooldown_reason.lock().await.clone()
    }

    async fn cooldown_remaining_ms(&self) -> Option<u64> {
        let until = *self.cooldown_until.lock().await;
        remaining_ms(until)
    }

    async fn next_request_in_ms(&self) -> Option<u64> {
        let until = *self.next_allowed.lock().await;
        remaining_ms(Some(until))
    }

    async fn snapshot(&self) -> AggregatorProviderSnapshot {
        let missing_auth_envs = self.missing_auth_envs();
        let cooldown_remaining_ms = self.cooldown_remaining_ms().await;
        let next_request_in_ms = self.next_request_in_ms().await;
        let budget_exhaustion_strikes = self.budget_exhaustion_strikes().await;
        let provider_error_strikes = self.provider_error_strikes().await;
        let rate_limit_strikes = self.rate_limit_strikes().await;
        let cooldown_reason = self.cooldown_reason().await;
        let auth_ready = !self.config.requires_api_key || missing_auth_envs.is_empty();
        let available = auth_ready && cooldown_remaining_ms.is_none();
        let status = if !auth_ready {
            format!("missing auth: {}", missing_auth_envs.join(", "))
        } else if let Some(ms) = cooldown_remaining_ms {
            let reason = cooldown_reason
                .as_deref()
                .unwrap_or("persisted provider cooldown");
            format!("cooling down after {reason} for {ms}ms")
        } else if let Some(ms) = next_request_in_ms {
            format!("paced; next request allowed in {ms}ms")
        } else {
            "ready".to_string()
        };
        AggregatorProviderSnapshot {
            name: self.config.name.clone(),
            venue: self.config.venue,
            format: self.config.format,
            quote_url: self.config.quote_url.clone(),
            requires_api_key: self.config.requires_api_key,
            api_key_env: self.config.api_key_env.clone(),
            missing_auth_envs,
            has_api_key: self.has_required_auth(),
            min_interval_ms: self.config.min_interval_ms,
            max_in_flight: self.config.max_in_flight.max(1),
            budget_exhaustion_strikes,
            provider_error_strikes,
            rate_limit_strikes,
            cooldown_reason,
            cooldown_remaining_ms,
            next_request_in_ms,
            available,
            status,
        }
    }
}

#[derive(Clone)]
struct CacheEntry {
    quote: Quote,
    expires_at: Instant,
}

#[derive(Clone, Eq)]
struct CacheKey {
    provider: String,
    pair: String,
    side: SwapSide,
    amount: u128,
}

impl CacheKey {
    fn new(provider: &str, pair: &TokenPair, side: SwapSide, amount: u128) -> Self {
        Self {
            provider: provider.to_string(),
            pair: pair.key(),
            side,
            amount,
        }
    }
}

impl PartialEq for CacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.provider == other.provider
            && self.pair == other.pair
            && self.side == other.side
            && self.amount == other.amount
    }
}

impl Hash for CacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.provider.hash(state);
        self.pair.hash(state);
        self.side.hash(state);
        self.amount.hash(state);
    }
}

fn jupiter_provider(config: &JupiterConfig) -> AggregatorProviderConfig {
    AggregatorProviderConfig {
        name: "jupiter".to_string(),
        venue: Venue::Jupiter,
        enabled: true,
        quote_url: config.quote_url.clone(),
        format: AggregatorFormat::Jupiter,
        timeout_ms: config.timeout_ms,
        min_interval_ms: env::var("JUPITER_MIN_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1_000),
        max_in_flight: 1,
        requires_api_key: false,
        api_key_env: Some("JUPITER_API_KEY".to_string()),
        api_key_header: Some("x-api-key".to_string()),
        api_key_prefix: None,
    }
}

pub(crate) fn provider_api_key(config: &AggregatorProviderConfig) -> Option<String> {
    let mut env_names = Vec::new();
    if let Some(env_name) = config
        .api_key_env
        .as_ref()
        .filter(|value| !value.is_empty())
    {
        env_names.push(env_name.as_str());
    }
    match config.name.as_str() {
        "spiderswap" => {
            env_names.push("CARBIUM_API_KEY");
            env_names.push("SPIDERSWAP_API_KEY");
        }
        "dflow" => {
            env_names.push("DFLOW_API_TOKEN");
            env_names.push("DFLOW_AUTH_TOKEN");
        }
        "titan" => {
            env_names.push("TITAN_API_KEY");
            env_names.push("TITAN_BEARER_TOKEN");
        }
        "vybe" => {
            env_names.push("VYBE_API_TOKEN");
        }
        "venum" => {
            env_names.push("VENUM_API_TOKEN");
        }
        _ => {}
    }

    env_names.into_iter().find_map(runtime_env_value)
}

pub(crate) fn missing_provider_auth_envs(config: &AggregatorProviderConfig) -> Vec<String> {
    if !config.requires_api_key {
        return Vec::new();
    }

    let mut missing = Vec::new();
    match config.name.as_str() {
        "spiderswap" => {
            if env_value("SPIDERSWAP_API_KEY").is_none() && env_value("CARBIUM_API_KEY").is_none() {
                missing.push("SPIDERSWAP_API_KEY or CARBIUM_API_KEY".to_string());
            }
        }
        "dflow" => {
            if env_value("DFLOW_API_KEY").is_none()
                && env_value("DFLOW_API_TOKEN").is_none()
                && env_value("DFLOW_AUTH_TOKEN").is_none()
            {
                missing.push("DFLOW_API_KEY, DFLOW_API_TOKEN, or DFLOW_AUTH_TOKEN".to_string());
            }
        }
        "titan" => {
            if env_value("TITAN_API_TOKEN").is_none()
                && env_value("TITAN_API_KEY").is_none()
                && env_value("TITAN_BEARER_TOKEN").is_none()
            {
                missing.push("TITAN_API_TOKEN, TITAN_API_KEY, or TITAN_BEARER_TOKEN".to_string());
            }
        }
        "vybe" => {
            if env_value("VYBE_API_KEY").is_none() && env_value("VYBE_API_TOKEN").is_none() {
                missing.push("VYBE_API_KEY or VYBE_API_TOKEN".to_string());
            }
        }
        "venum" => {
            if env_value("VENUM_API_KEY").is_none() && env_value("VENUM_API_TOKEN").is_none() {
                missing.push("VENUM_API_KEY or VENUM_API_TOKEN".to_string());
            }
        }
        "okx" => {
            if env_value("OKX_API_KEY").is_none() {
                missing.push("OKX_API_KEY".to_string());
            }
            if env_value("OKX_API_SECRET").is_none() && env_value("OKX_SECRET_KEY").is_none() {
                missing.push("OKX_API_SECRET or OKX_SECRET_KEY".to_string());
            }
            if env_value("OKX_PASSPHRASE").is_none() {
                missing.push("OKX_PASSPHRASE".to_string());
            }
            if env_value("OKX_PROJECT_ID").is_none() {
                missing.push("OKX_PROJECT_ID".to_string());
            }
        }
        _ => {
            if config
                .api_key_env
                .as_deref()
                .filter(|env_name| env_value(env_name).is_some())
                .is_none()
            {
                if let Some(env_name) = config.api_key_env.as_ref() {
                    missing.push(env_name.clone());
                }
            }
        }
    }
    missing
}

fn env_value(name: &str) -> Option<String> {
    runtime_env_value(name)
}

fn is_rate_limit_error(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("429") || message.to_ascii_lowercase().contains("too many requests")
}

fn provider_cooldown_for_error(error: &anyhow::Error) -> Option<Duration> {
    if is_rate_limit_error(error) {
        return Some(rate_limit_cooldown(error));
    }
    is_transient_provider_error(error).then(transient_error_cooldown)
}

fn is_transient_provider_error(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    message.contains("timed out")
        || message.contains("timeout")
        || message.contains("temporarily unavailable")
        || message.contains("http 502")
        || message.contains("http 503")
        || message.contains("http 504")
}

fn transient_error_cooldown() -> Duration {
    transient_error_cooldown_from_value(
        runtime_env_value("AGGREGATOR_TRANSIENT_ERROR_COOLDOWN_SECS").as_deref(),
    )
}

fn transient_error_cooldown_for_strike(strikes: u32) -> Duration {
    transient_error_cooldown_for_strike_from_values(
        runtime_env_value("AGGREGATOR_TRANSIENT_ERROR_COOLDOWN_SECS").as_deref(),
        runtime_env_value("AGGREGATOR_TRANSIENT_ERROR_MAX_COOLDOWN_SECS").as_deref(),
        strikes,
    )
}

fn transient_error_cooldown_from_value(value: Option<&str>) -> Duration {
    Duration::from_secs(
        value
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(45)
            .clamp(5, 600),
    )
}

fn transient_error_max_cooldown_from_value(value: Option<&str>) -> Duration {
    Duration::from_secs(
        value
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(600)
            .clamp(30, 3_600),
    )
}

fn transient_error_cooldown_for_strike_from_values(
    base_value: Option<&str>,
    max_value: Option<&str>,
    strikes: u32,
) -> Duration {
    let base = transient_error_cooldown_from_value(base_value);
    let max = transient_error_max_cooldown_from_value(max_value);
    let multiplier = 1u32 << strikes.saturating_sub(1).min(8);
    base.saturating_mul(multiplier).min(max)
}

fn budget_exhaustion_cooldown_for_strike(strikes: u32) -> Duration {
    budget_exhaustion_cooldown_for_strike_from_values(
        runtime_env_value("AGGREGATOR_BUDGET_EXHAUSTED_COOLDOWN_SECS").as_deref(),
        runtime_env_value("AGGREGATOR_BUDGET_EXHAUSTED_MAX_COOLDOWN_SECS").as_deref(),
        strikes,
    )
}

fn budget_exhaustion_cooldown_from_value(value: Option<&str>) -> Duration {
    Duration::from_secs(
        value
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(15)
            .clamp(1, 120),
    )
}

fn budget_exhaustion_max_cooldown_from_value(value: Option<&str>) -> Duration {
    Duration::from_secs(
        value
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(300)
            .clamp(30, 1_800),
    )
}

fn budget_exhaustion_cooldown_for_strike_from_values(
    base_value: Option<&str>,
    max_value: Option<&str>,
    strikes: u32,
) -> Duration {
    let base = budget_exhaustion_cooldown_from_value(base_value);
    let max = budget_exhaustion_max_cooldown_from_value(max_value);
    let multiplier = 1u32 << strikes.saturating_sub(1).min(8);
    base.saturating_mul(multiplier).min(max)
}

fn rate_limit_cooldown(error: &anyhow::Error) -> Duration {
    let message = error.to_string();
    if let Some(seconds) = message
        .split("retry_after_seconds=")
        .nth(1)
        .and_then(|rest| rest.split(|ch: char| !ch.is_ascii_digit()).next())
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
    {
        return Duration::from_secs(seconds.min(3_600));
    }
    Duration::from_secs(
        env::var("AGGREGATOR_RATE_LIMIT_FALLBACK_COOLDOWN_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(300)
            .clamp(30, 3_600),
    )
}

fn persisted_provider_cooldown_until(provider: &str) -> Option<Instant> {
    let _guard = cooldown_state_lock().lock().ok()?;
    let state = read_cooldown_state();
    let expiry_ms = state.get(provider).copied()?;
    let now_ms = unix_now_ms();
    if expiry_ms <= now_ms {
        return None;
    }
    let remaining = expiry_ms.saturating_sub(now_ms).min(u64::MAX as u128) as u64;
    Some(Instant::now() + Duration::from_millis(remaining))
}

fn persist_provider_cooldown(provider: &str, duration: Duration) {
    if duration.is_zero() {
        return;
    }
    let Some(_guard) = cooldown_state_lock().lock().ok() else {
        return;
    };
    let mut state = read_cooldown_state();
    state.insert(
        provider.to_string(),
        unix_now_ms().saturating_add(duration.as_millis()),
    );
    let Ok(text) = serde_json::to_string(&state) else {
        return;
    };
    let path = cooldown_state_path();
    let _ = fs::write(path, text);
}

fn read_cooldown_state() -> HashMap<String, u128> {
    let path = cooldown_state_path();
    fs::read_to_string(path)
        .ok()
        .and_then(|text| serde_json::from_str::<HashMap<String, u128>>(&text).ok())
        .unwrap_or_default()
}

fn cooldown_state_path() -> String {
    env::var("AGGREGATOR_COOLDOWN_STATE_PATH")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "/tmp/arb-bot-aggregator-cooldowns.json".to_string())
}

fn cooldown_state_lock() -> &'static StdMutex<()> {
    static LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| StdMutex::new(()))
}

fn unix_now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn remaining_ms(until: Option<Instant>) -> Option<u64> {
    let until = until?;
    let now = Instant::now();
    (until > now).then(|| until.duration_since(now).as_millis() as u64)
}

fn aggregator_quote_budget_ms() -> u64 {
    aggregator_quote_budget_ms_from_value(
        runtime_env_value("AGGREGATOR_QUOTE_BUDGET_MS").as_deref(),
    )
}

fn aggregator_quote_budget_ms_from_value(value: Option<&str>) -> u64 {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(5_000)
        .clamp(250, 60_000)
}

fn remaining_quote_budget(deadline: Instant) -> Option<Duration> {
    let now = Instant::now();
    (deadline > now).then(|| deadline.duration_since(now))
}

fn max_buy_routes_per_pair() -> usize {
    env_usize_local("TRADE_SOURCE_MAX_BUY_ROUTES_PER_PAIR")
        .unwrap_or(2)
        .max(1)
}

fn seed_pair(seed: &JupiterSeedPair) -> TokenPair {
    TokenPair {
        base_mint: seed.base_mint.clone(),
        quote_mint: seed.quote_mint.clone(),
        base_decimals: seed.base_decimals,
        quote_decimals: seed.quote_decimals,
    }
}

#[derive(Debug, Clone)]
struct TokenDiscoveryToken {
    mint: String,
    decimals: u8,
    observed_quote_mints: Vec<String>,
    observed_venue_families: Vec<String>,
    liquidity_usd: f64,
    mcap_usd: f64,
    organic_score: f64,
    is_verified: bool,
    is_suspicious: bool,
    score: f64,
}

fn dynamic_token_discovery_enabled(config: &AggregatorsConfig) -> bool {
    env_bool_local("TOKEN_DISCOVERY_ENABLED").unwrap_or(config.dynamic_discovery_enabled)
}

fn dynamic_refresh_interval_ms(config: &AggregatorsConfig) -> u64 {
    env_u64_local("TOKEN_DISCOVERY_REFRESH_INTERVAL_MS")
        .unwrap_or(config.dynamic_refresh_interval_ms)
        .max(1_000)
}

fn dynamic_max_tokens(config: &AggregatorsConfig) -> usize {
    env_usize_local("TOKEN_DISCOVERY_MAX_TOKENS")
        .unwrap_or(config.dynamic_max_tokens)
        .max(1)
}

fn dynamic_max_quote_mints(config: &AggregatorsConfig) -> usize {
    env_usize_local("TOKEN_DISCOVERY_MAX_QUOTE_MINTS")
        .unwrap_or(config.dynamic_max_quote_mints)
        .max(1)
}

fn dynamic_quote_mints(default_quote_mints: &[String]) -> Vec<String> {
    env::var("TOKEN_DISCOVERY_QUOTE_MINTS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .filter(|mints| !mints.is_empty())
        .unwrap_or_else(|| {
            let mut ordered = Vec::new();
            for preferred in [USDC_MINT, WSOL_MINT, USDT_MINT] {
                if default_quote_mints.iter().any(|mint| mint == preferred) {
                    ordered.push(preferred.to_string());
                }
            }
            for mint in default_quote_mints {
                if !ordered.iter().any(|existing| existing == mint) {
                    ordered.push(mint.clone());
                }
            }
            ordered
        })
}

fn dynamic_min_liquidity_usd(config: &AggregatorsConfig) -> f64 {
    env_f64_local("TOKEN_DISCOVERY_MIN_LIQUIDITY_USD")
        .unwrap_or(config.dynamic_min_liquidity_usd)
        .max(0.0)
}

fn dynamic_min_organic_score(config: &AggregatorsConfig) -> f64 {
    env_f64_local("TOKEN_DISCOVERY_MIN_ORGANIC_SCORE")
        .unwrap_or(config.dynamic_min_organic_score)
        .max(0.0)
}

fn dynamic_max_mcap_usd(config: &AggregatorsConfig) -> f64 {
    env_f64_local("TOKEN_DISCOVERY_MAX_MCAP_USD").unwrap_or(config.dynamic_max_mcap_usd)
}

fn dynamic_allow_unverified(config: &AggregatorsConfig) -> bool {
    env_bool_local("TOKEN_DISCOVERY_ALLOW_UNVERIFIED").unwrap_or(config.dynamic_allow_unverified)
}

fn dynamic_token_api_base_url() -> String {
    env::var("JUPITER_TOKEN_API_URL").unwrap_or_else(|_| "https://api.jup.ag/tokens/v2".to_string())
}

fn dynamic_token_endpoints() -> Vec<String> {
    env::var("TOKEN_DISCOVERY_ENDPOINTS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .filter(|endpoints| !endpoints.is_empty())
        .unwrap_or_else(|| {
            vec![
                "toptraded/5m?limit=50".to_string(),
                "toptrending/5m?limit=50".to_string(),
                "toporganicscore/1h?limit=50".to_string(),
                "recent".to_string(),
            ]
        })
}

fn geckoterminal_discovery_endpoints() -> Vec<String> {
    env::var("GECKOTERMINAL_DISCOVERY_ENDPOINTS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .filter(|endpoints| !endpoints.is_empty())
        .unwrap_or_else(|| {
            vec![
                "https://api.geckoterminal.com/api/v2/networks/solana/trending_pools?include=dex"
                    .to_string(),
                "https://api.geckoterminal.com/api/v2/networks/solana/pools?page=1&include=dex"
                    .to_string(),
                "https://api.geckoterminal.com/api/v2/networks/solana/pools?page=2&include=dex"
                    .to_string(),
            ]
        })
}

fn geckoterminal_allowed_dex_ids() -> std::collections::HashSet<String> {
    env::var("GECKOTERMINAL_ALLOWED_DEXES")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| value.to_ascii_lowercase())
                .collect::<std::collections::HashSet<_>>()
        })
        .filter(|dexes| !dexes.is_empty())
        .unwrap_or_else(|| {
            [
                "orca",
                "orca-whirlpool",
                "orca-whirlpools",
                "raydium",
                "raydium-clmm",
                "raydium-cpmm",
                "raydium-cp",
                "meteora",
                "meteora-dlmm",
                "meteora-damm",
                "meteora-damm-v2",
            ]
            .into_iter()
            .map(ToOwned::to_owned)
            .collect()
        })
}

fn venue_discovery_min_supported_venues(config: &AggregatorsConfig) -> usize {
    env_usize_local("TOKEN_DISCOVERY_MIN_SUPPORTED_VENUES")
        .unwrap_or(config.dynamic_min_supported_venues)
        .max(1)
}

fn parse_dynamic_tokens(value: &Value) -> Vec<TokenDiscoveryToken> {
    token_array(value)
        .iter()
        .filter_map(|token| {
            let mint = token
                .get("id")
                .or_else(|| token.get("mint"))
                .or_else(|| token.get("address"))?
                .as_str()?
                .to_string();
            let decimals = token
                .get("decimals")
                .and_then(Value::as_u64)
                .and_then(|value| u8::try_from(value).ok())?;
            let liquidity_usd = token
                .get("liquidity")
                .or_else(|| token.get("liquidityUsd"))
                .and_then(Value::as_f64)
                .unwrap_or_default();
            let mcap_usd = token
                .get("mcap")
                .or_else(|| token.get("marketCap"))
                .or_else(|| token.get("fdv"))
                .and_then(Value::as_f64)
                .unwrap_or_default();
            let organic_score = token
                .get("organicScore")
                .and_then(Value::as_f64)
                .unwrap_or_default();
            let is_verified = token
                .get("isVerified")
                .and_then(Value::as_bool)
                .unwrap_or(false)
                || token
                    .get("tags")
                    .and_then(Value::as_array)
                    .map(|tags| tags.iter().any(|tag| tag.as_str() == Some("verified")))
                    .unwrap_or(false);
            let is_suspicious = token
                .pointer("/audit/isSus")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let score = liquidity_usd.log10().max(0.0) * 10.0
                + organic_score
                + if is_verified { 20.0 } else { 0.0 }
                - (mcap_usd / 100_000_000.0).min(20.0);

            Some(TokenDiscoveryToken {
                mint,
                decimals,
                observed_quote_mints: Vec::new(),
                observed_venue_families: Vec::new(),
                liquidity_usd,
                mcap_usd,
                organic_score,
                is_verified,
                is_suspicious,
                score,
            })
        })
        .collect()
}

fn parse_geckoterminal_tokens(
    value: &Value,
    quote_mints: &std::collections::HashSet<String>,
    allowed_dexes: &std::collections::HashSet<String>,
    min_organic_score: f64,
) -> Vec<TokenDiscoveryToken> {
    token_array(value)
        .iter()
        .filter_map(|pool| {
            if pool.get("type").and_then(Value::as_str) != Some("pool") {
                return None;
            }
            let dex_id = pool
                .pointer("/relationships/dex/data/id")
                .and_then(Value::as_str)
                .map(str::to_ascii_lowercase)?;
            if !allowed_dexes.contains(&dex_id) {
                return None;
            }
            let venue_family = supported_dex_family(&dex_id)?;
            let base_mint = strip_geckoterminal_solana_id(
                pool.pointer("/relationships/base_token/data/id")?
                    .as_str()?,
            )?;
            let quote_mint = strip_geckoterminal_solana_id(
                pool.pointer("/relationships/quote_token/data/id")?
                    .as_str()?,
            )?;
            let (mint, observed_quote_mint) = if quote_mints.contains(quote_mint) {
                (base_mint, quote_mint)
            } else if quote_mints.contains(base_mint) {
                (quote_mint, base_mint)
            } else {
                return None;
            };

            let liquidity_usd = first_f64(pool, &["/attributes/reserve_in_usd"]).unwrap_or(0.0);
            let mcap_usd = first_f64(pool, &["/attributes/market_cap_usd", "/attributes/fdv_usd"])
                .unwrap_or_default();
            let volume_h1 = first_f64(pool, &["/attributes/volume_usd/h1"])
                .or_else(|| first_f64(pool, &["/attributes/volume_usd/h6"]))
                .or_else(|| first_f64(pool, &["/attributes/volume_usd/h24"]))
                .unwrap_or_default();
            let txns_h1 = pool
                .pointer("/attributes/transactions/h1/buys")
                .and_then(Value::as_u64)
                .unwrap_or_default()
                .saturating_add(
                    pool.pointer("/attributes/transactions/h1/sells")
                        .and_then(Value::as_u64)
                        .unwrap_or_default(),
                ) as f64;
            let organic_score = min_organic_score;
            let score = liquidity_usd.log10().max(0.0) * 10.0
                + volume_h1.log10().max(0.0) * 12.0
                + txns_h1.min(2_000.0) / 20.0
                - (mcap_usd / 100_000_000.0).min(20.0);

            Some(TokenDiscoveryToken {
                mint: mint.to_string(),
                decimals: 6,
                observed_quote_mints: vec![observed_quote_mint.to_string()],
                observed_venue_families: vec![venue_family.to_string()],
                liquidity_usd,
                mcap_usd,
                organic_score,
                is_verified: false,
                is_suspicious: false,
                score,
            })
        })
        .collect()
}

fn strip_geckoterminal_solana_id(id: &str) -> Option<&str> {
    id.strip_prefix("solana_")
}

fn token_array(value: &Value) -> Vec<Value> {
    if let Some(items) = value.as_array() {
        return items.clone();
    }
    value
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn token_is_eligible(
    token: &TokenDiscoveryToken,
    quote_mints: &std::collections::HashSet<String>,
    denylist: &std::collections::HashSet<String>,
    config: &AggregatorsConfig,
) -> bool {
    if quote_mints.contains(&token.mint) || denylist.contains(&token.mint) {
        return false;
    }
    if is_blocklisted_remora_mint(&token.mint) {
        return false;
    }
    if is_pump_like_mint(&token.mint) {
        return false;
    }
    if token.is_suspicious || token.liquidity_usd < dynamic_min_liquidity_usd(config) {
        return false;
    }
    if dynamic_max_mcap_usd(config) > 0.0 && token.mcap_usd > dynamic_max_mcap_usd(config) {
        return false;
    }
    if !dynamic_allow_unverified(config) && !token.is_verified {
        return false;
    }
    token.is_verified || token.organic_score >= dynamic_min_organic_score(config)
}

fn token_pair_is_eligible(pair: &TokenPair) -> bool {
    !is_blocklisted_remora_mint(&pair.base_mint) && !is_pump_like_mint(&pair.base_mint)
}

fn parse_dexscreener_tokens(
    value: &Value,
    candidate_mints: &std::collections::HashSet<String>,
    quote_mints: &std::collections::HashSet<String>,
    min_organic_score: f64,
) -> Vec<TokenDiscoveryToken> {
    token_array(value)
        .iter()
        .filter_map(|pair| {
            if pair.get("chainId").and_then(Value::as_str) != Some("solana") {
                return None;
            }

            let dex_id = pair.get("dexId").and_then(Value::as_str)?;
            let venue_family = supported_dex_family(dex_id)?;
            let base_mint = pair.pointer("/baseToken/address")?.as_str()?;
            let quote_mint = pair.pointer("/quoteToken/address")?.as_str()?;
            let (mint, observed_quote_mint) =
                if candidate_mints.contains(base_mint) && quote_mints.contains(quote_mint) {
                    (base_mint, quote_mint)
                } else if candidate_mints.contains(quote_mint) && quote_mints.contains(base_mint) {
                    (quote_mint, base_mint)
                } else {
                    return None;
                };

            let liquidity_usd = pair
                .pointer("/liquidity/usd")
                .and_then(Value::as_f64)
                .unwrap_or_default();
            let mcap_usd = pair
                .get("marketCap")
                .or_else(|| pair.get("fdv"))
                .and_then(Value::as_f64)
                .unwrap_or_default();
            let volume_5m = pair
                .pointer("/volume/m5")
                .or_else(|| pair.pointer("/volume/h1"))
                .or_else(|| pair.pointer("/volume/h24"))
                .and_then(Value::as_f64)
                .unwrap_or_default();
            let txns_5m = pair
                .pointer("/txns/m5/buys")
                .and_then(Value::as_u64)
                .unwrap_or_default()
                .saturating_add(
                    pair.pointer("/txns/m5/sells")
                        .and_then(Value::as_u64)
                        .unwrap_or_default(),
                ) as f64;
            let organic_score = min_organic_score;
            let score = liquidity_usd.log10().max(0.0) * 10.0
                + volume_5m.log10().max(0.0) * 8.0
                + txns_5m.min(200.0) / 4.0
                - (mcap_usd / 100_000_000.0).min(20.0);

            Some(TokenDiscoveryToken {
                mint: mint.to_string(),
                decimals: 6,
                observed_quote_mints: vec![observed_quote_mint.to_string()],
                observed_venue_families: vec![venue_family.to_string()],
                liquidity_usd,
                mcap_usd,
                organic_score,
                is_verified: false,
                is_suspicious: false,
                score,
            })
        })
        .collect()
}

fn supported_dex_family(dex_id: &str) -> Option<&'static str> {
    match dex_id.to_ascii_lowercase().as_str() {
        "orca" | "orca-whirlpool" | "orca-whirlpools" => Some("orca"),
        "raydium" | "raydium-clmm" | "raydium-cpmm" | "raydium-cp" => Some("raydium"),
        "meteora" | "meteora-dlmm" | "meteora-damm" | "meteora-damm-v2" => Some("meteora"),
        _ => None,
    }
}

fn upsert_discovery_token(
    tokens_by_mint: &mut HashMap<String, TokenDiscoveryToken>,
    token: TokenDiscoveryToken,
) {
    tokens_by_mint
        .entry(token.mint.clone())
        .and_modify(|existing| {
            let mut observed_quote_mints = existing.observed_quote_mints.clone();
            extend_unique(&mut observed_quote_mints, &token.observed_quote_mints);
            let mut observed_venue_families = existing.observed_venue_families.clone();
            extend_unique(&mut observed_venue_families, &token.observed_venue_families);
            if token.score > existing.score {
                *existing = token.clone();
            }
            extend_unique(&mut existing.observed_quote_mints, &observed_quote_mints);
            extend_unique(
                &mut existing.observed_venue_families,
                &observed_venue_families,
            );
        })
        .or_insert(token);
}

fn extend_unique(target: &mut Vec<String>, values: &[String]) {
    for value in values {
        if !target.iter().any(|existing| existing == value) {
            target.push(value.clone());
        }
    }
}

fn quote_mints_for_discovered_token(
    token: &TokenDiscoveryToken,
    default_quote_mint_order: &[String],
    config: &AggregatorsConfig,
) -> Vec<String> {
    let max_quote_mints = dynamic_max_quote_mints(config);
    let mut selected = Vec::new();
    let mut seen = std::collections::HashSet::new();

    if !token.observed_quote_mints.is_empty() {
        for quote_mint in default_quote_mint_order {
            if token
                .observed_quote_mints
                .iter()
                .any(|observed| observed == quote_mint)
                && seen.insert(quote_mint.clone())
            {
                selected.push(quote_mint.clone());
            }
        }
        for quote_mint in &token.observed_quote_mints {
            if seen.insert(quote_mint.clone()) {
                selected.push(quote_mint.clone());
            }
        }
    } else {
        for quote_mint in default_quote_mint_order {
            if seen.insert(quote_mint.clone()) {
                selected.push(quote_mint.clone());
            }
        }
    }

    selected.truncate(max_quote_mints);
    selected
}

fn decimals_for_quote_mint(mint: &str) -> u8 {
    match mint {
        USDC_MINT | USDT_MINT => 6,
        WSOL_MINT => 9,
        _ => 6,
    }
}

fn env_bool_local(name: &str) -> Option<bool> {
    runtime_env_bool(name)
}

fn env_f64_local(name: &str) -> Option<f64> {
    runtime_env_value(name).and_then(|value| value.parse().ok())
}

fn env_u64_local(name: &str) -> Option<u64> {
    runtime_env_value(name).and_then(|value| value.parse().ok())
}

fn env_usize_local(name: &str) -> Option<usize> {
    runtime_env_value(name).and_then(|value| value.parse().ok())
}

pub(crate) fn request_params(
    format: AggregatorFormat,
    pair: &TokenPair,
    side: SwapSide,
    in_amount_atoms: u128,
) -> Vec<(String, String)> {
    let input_mint = input_mint_for_side(pair, side);
    let output_mint = output_mint_for_side(pair, side);
    let input_decimals = input_decimals_for_side(pair, side);
    let output_decimals = output_decimals_for_side(pair, side);
    match format {
        AggregatorFormat::Jupiter => vec![
            ("inputMint".to_string(), input_mint.to_string()),
            ("outputMint".to_string(), output_mint.to_string()),
            ("amount".to_string(), in_amount_atoms.to_string()),
            ("slippageBps".to_string(), "50".to_string()),
            ("onlyDirectRoutes".to_string(), "false".to_string()),
            ("asLegacyTransaction".to_string(), "false".to_string()),
            ("swapMode".to_string(), "ExactIn".to_string()),
        ],
        AggregatorFormat::RaydiumTradeApi => vec![
            ("inputMint".to_string(), input_mint.to_string()),
            ("outputMint".to_string(), output_mint.to_string()),
            ("amount".to_string(), in_amount_atoms.to_string()),
            ("slippageBps".to_string(), "50".to_string()),
            ("txVersion".to_string(), "V0".to_string()),
        ],
        AggregatorFormat::CarbiumQ1 => vec![
            ("src_mint".to_string(), input_mint.to_string()),
            ("dst_mint".to_string(), output_mint.to_string()),
            ("amount_in".to_string(), in_amount_atoms.to_string()),
            ("slippage_bps".to_string(), "50".to_string()),
        ],
        AggregatorFormat::SpiderSwap => vec![
            ("fromMint".to_string(), input_mint.to_string()),
            ("toMint".to_string(), output_mint.to_string()),
            ("amount".to_string(), in_amount_atoms.to_string()),
            ("slippage".to_string(), "50".to_string()),
            ("fromMintDecimals".to_string(), input_decimals.to_string()),
            ("toMintDecimals".to_string(), output_decimals.to_string()),
        ],
        AggregatorFormat::Okx => vec![
            ("chainIndex".to_string(), "501".to_string()),
            (
                "fromTokenAddress".to_string(),
                okx_token_address(input_mint).to_string(),
            ),
            (
                "toTokenAddress".to_string(),
                okx_token_address(output_mint).to_string(),
            ),
            ("amount".to_string(), in_amount_atoms.to_string()),
            ("swapMode".to_string(), "exactIn".to_string()),
            ("slippagePercent".to_string(), "0.5".to_string()),
            (
                "priceImpactProtectionPercent".to_string(),
                "100".to_string(),
            ),
        ],
        AggregatorFormat::OpenOcean => vec![
            ("inTokenAddress".to_string(), input_mint.to_string()),
            ("outTokenAddress".to_string(), output_mint.to_string()),
            ("amountDecimals".to_string(), in_amount_atoms.to_string()),
            (
                "amount".to_string(),
                trim_float(atoms_to_ui(in_amount_atoms, input_decimals)),
            ),
            ("gasPriceDecimals".to_string(), "0".to_string()),
            ("slippage".to_string(), "0.5".to_string()),
            ("gasPrice".to_string(), "0".to_string()),
        ],
        AggregatorFormat::Dflow => vec![
            ("inputMint".to_string(), input_mint.to_string()),
            ("outputMint".to_string(), output_mint.to_string()),
            ("amount".to_string(), in_amount_atoms.to_string()),
            ("slippageBps".to_string(), "50".to_string()),
        ],
        AggregatorFormat::Vybe => vec![
            (
                "amount".to_string(),
                trim_float(atoms_to_ui(in_amount_atoms, input_decimals)),
            ),
            ("inputMint".to_string(), input_mint.to_string()),
            ("outputMint".to_string(), output_mint.to_string()),
            ("slippage".to_string(), "0.5".to_string()),
        ],
        AggregatorFormat::Generic => vec![
            ("inputMint".to_string(), input_mint.to_string()),
            ("outputMint".to_string(), output_mint.to_string()),
            ("fromMint".to_string(), input_mint.to_string()),
            ("toMint".to_string(), output_mint.to_string()),
            ("amount".to_string(), in_amount_atoms.to_string()),
            ("inAmount".to_string(), in_amount_atoms.to_string()),
            ("slippageBps".to_string(), "50".to_string()),
        ],
        AggregatorFormat::Venum => vec![
            ("inputMint".to_string(), input_mint.to_string()),
            ("outputMint".to_string(), output_mint.to_string()),
            ("amount".to_string(), in_amount_atoms.to_string()),
            ("slippageBps".to_string(), "50".to_string()),
        ],
        AggregatorFormat::TitanStream => Vec::new(),
    }
}

fn okx_token_address(mint: &str) -> &str {
    if mint == WSOL_MINT {
        "11111111111111111111111111111111"
    } else {
        mint
    }
}

fn venum_quote_body(pair: &TokenPair, side: SwapSide, in_amount_atoms: u128) -> Value {
    serde_json::json!({
        "inputMint": input_mint_for_side(pair, side),
        "outputMint": output_mint_for_side(pair, side),
        "amount": in_amount_atoms.to_string(),
        "slippageBps": 50
    })
}

fn vybe_quote_body(
    pair: &TokenPair,
    side: SwapSide,
    in_amount_atoms: u128,
    wallet: Option<&Pubkey>,
) -> Value {
    let input_ui = atoms_to_ui(in_amount_atoms, input_decimals_for_side(pair, side));
    let mut body = serde_json::json!({
        "amount": input_ui,
        "inputMint": input_mint_for_side(pair, side),
        "outputMint": output_mint_for_side(pair, side),
        "slippage": 0.5
    });
    if let Some(wallet) = wallet {
        body["wallet"] = Value::String(wallet.to_string());
    }
    body
}

fn input_mint_for_side(pair: &TokenPair, side: SwapSide) -> &str {
    match side {
        SwapSide::QuoteToBase => &pair.quote_mint,
        SwapSide::BaseToQuote => &pair.base_mint,
    }
}

fn output_mint_for_side(pair: &TokenPair, side: SwapSide) -> &str {
    match side {
        SwapSide::QuoteToBase => &pair.base_mint,
        SwapSide::BaseToQuote => &pair.quote_mint,
    }
}

fn input_decimals_for_side(pair: &TokenPair, side: SwapSide) -> u8 {
    match side {
        SwapSide::QuoteToBase => pair.quote_decimals,
        SwapSide::BaseToQuote => pair.base_decimals,
    }
}

fn output_decimals_for_side(pair: &TokenPair, side: SwapSide) -> u8 {
    match side {
        SwapSide::QuoteToBase => pair.base_decimals,
        SwapSide::BaseToQuote => pair.quote_decimals,
    }
}

fn trim_float(value: f64) -> String {
    let mut text = format!("{value:.12}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    if text.is_empty() {
        "0".to_string()
    } else {
        text
    }
}

pub(crate) fn url_with_query(base_url: &str, params: &[(String, String)]) -> Result<Url> {
    let mut url = Url::parse(base_url)?;
    url.query_pairs_mut().extend_pairs(
        params
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    );
    Ok(url)
}

pub(crate) fn okx_signed_request(
    request: RequestBuilder,
    url: &Url,
    api_key: String,
) -> Result<RequestBuilder> {
    let secret = runtime_env_value("OKX_API_SECRET")
        .or_else(|| runtime_env_value("OKX_SECRET_KEY"))
        .context("OKX_API_SECRET or OKX_SECRET_KEY is required for OKX DEX")?;
    let passphrase = runtime_env_value("OKX_PASSPHRASE").context("OKX_PASSPHRASE is required")?;
    let project_id = runtime_env_value("OKX_PROJECT_ID").context("OKX_PROJECT_ID is required")?;
    let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let request_path = match url.query() {
        Some(query) if !query.is_empty() => format!("{}?{query}", url.path()),
        _ => url.path().to_string(),
    };
    let payload = format!("{timestamp}GET{request_path}");
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .context("failed to initialize OKX signer")?;
    mac.update(payload.as_bytes());
    let signature = general_purpose::STANDARD.encode(mac.finalize().into_bytes());

    Ok(request
        .header("OK-ACCESS-KEY", api_key)
        .header("OK-ACCESS-SIGN", signature)
        .header("OK-ACCESS-TIMESTAMP", timestamp)
        .header("OK-ACCESS-PASSPHRASE", passphrase)
        .header("OK-ACCESS-PROJECT", project_id))
}

fn titan_wallet_pubkey() -> Result<Pubkey> {
    let wallet_path =
        env::var("SOLANA_WALLET_JSON").unwrap_or_else(|_| ".secrets/wallet.json".to_string());
    let wallet = read_keypair_file(&wallet_path)
        .map_err(|error| anyhow!("failed to read wallet keypair {wallet_path}: {error}"))?;
    Ok(wallet.pubkey())
}

fn titan_quote_request_msgpack(
    wallet: &Pubkey,
    pair: &TokenPair,
    side: SwapSide,
    amount_atoms: u64,
) -> Result<MsgPackValue> {
    let input_mint = Pubkey::from_str(input_mint_for_side(pair, side))?;
    let output_mint = Pubkey::from_str(output_mint_for_side(pair, side))?;
    Ok(msgpack_map(vec![
        ("id", MsgPackValue::from(1u32)),
        (
            "data",
            msgpack_map(vec![(
                "NewSwapQuoteStream",
                msgpack_map(vec![
                    (
                        "swap",
                        msgpack_map(vec![
                            (
                                "inputMint",
                                MsgPackValue::Binary(input_mint.to_bytes().to_vec()),
                            ),
                            (
                                "outputMint",
                                MsgPackValue::Binary(output_mint.to_bytes().to_vec()),
                            ),
                            ("amount", MsgPackValue::from(amount_atoms)),
                            ("swapMode", MsgPackValue::from("ExactIn")),
                            ("slippageBps", MsgPackValue::from(titan_slippage_bps())),
                            ("addSizeConstraint", MsgPackValue::from(true)),
                            ("sizeConstraint", MsgPackValue::from(1_180u64)),
                            (
                                "accountsLimitTotal",
                                MsgPackValue::from(titan_accounts_limit()),
                            ),
                        ]),
                    ),
                    (
                        "transaction",
                        msgpack_map(vec![
                            (
                                "userPublicKey",
                                MsgPackValue::Binary(wallet.to_bytes().to_vec()),
                            ),
                            ("createOutputTokenAccount", MsgPackValue::from(true)),
                        ]),
                    ),
                    (
                        "update",
                        msgpack_map(vec![
                            ("intervalMs", MsgPackValue::from(250u64)),
                            ("numQuotes", MsgPackValue::from(1u64)),
                        ]),
                    ),
                ]),
            )]),
        ),
    ]))
}

fn titan_quote_from_route(
    provider: &AggregatorProviderConfig,
    pair: &TokenPair,
    side: SwapSide,
    in_amount_atoms: u128,
    route: &MsgPackValue,
    network_fee: &FeeBreakdown,
    safety_buffer_bps: f64,
) -> Option<Quote> {
    let out_amount_atoms = titan_map_u64(route, "outAmount")? as u128;
    if out_amount_atoms == 0 {
        return None;
    }
    let input_ui = atoms_to_ui(in_amount_atoms, input_decimals_for_side(pair, side));
    let output_ui = atoms_to_ui(out_amount_atoms, output_decimals_for_side(pair, side));
    let (quote_ui, base_ui) = match side {
        SwapSide::QuoteToBase => (input_ui, output_ui),
        SwapSide::BaseToQuote => (output_ui, input_ui),
    };
    if quote_ui == 0.0 || base_ui == 0.0 {
        return None;
    }

    let mut fee = FeeBreakdown {
        lp_fee_atoms: 0,
        protocol_fee_atoms: 0,
        host_fee_atoms: 0,
        base_fee_lamports: 0,
        priority_fee_lamports: 0,
        jito_tip_lamports: 0,
        ata_rent_lamports: 0,
        slippage_bps: titan_map_u64(route, "slippageBps").unwrap_or_default() as f64,
        safety_buffer_bps,
        network_fee_usd: 0.0,
    };
    fee.merge_network(network_fee);

    Some(Quote {
        venue: provider.venue,
        pool: titan_route_label(route),
        pair: pair.clone(),
        side,
        in_amount_atoms,
        out_amount_atoms,
        effective_price: quote_ui / base_ui,
        fee,
        slot: 0,
    })
}

fn msgpack_map(entries: Vec<(&str, MsgPackValue)>) -> MsgPackValue {
    MsgPackValue::Map(
        entries
            .into_iter()
            .map(|(key, value)| (MsgPackValue::from(key), value))
            .collect(),
    )
}

fn titan_best_route(value: &MsgPackValue) -> Option<&MsgPackValue> {
    titan_find_key(value, "quotes")?
        .as_map()?
        .iter()
        .filter_map(|(_, route)| titan_map_u64(route, "outAmount").map(|out| (out, route)))
        .max_by_key(|(out, _)| *out)
        .map(|(_, route)| route)
}

fn titan_find_key<'a>(value: &'a MsgPackValue, key: &str) -> Option<&'a MsgPackValue> {
    match value {
        MsgPackValue::Map(entries) => {
            for (entry_key, entry_value) in entries {
                if entry_key.as_str() == Some(key) {
                    return Some(entry_value);
                }
                if let Some(found) = titan_find_key(entry_value, key) {
                    return Some(found);
                }
            }
            None
        }
        MsgPackValue::Array(items) => items.iter().find_map(|item| titan_find_key(item, key)),
        _ => None,
    }
}

fn titan_map_u64(value: &MsgPackValue, key: &str) -> Option<u64> {
    value.as_map()?.iter().find_map(|(entry_key, entry_value)| {
        (entry_key.as_str() == Some(key)).then(|| {
            entry_value
                .as_u64()
                .or_else(|| entry_value.as_i64().and_then(|value| value.try_into().ok()))
                .or_else(|| entry_value.as_str()?.parse().ok())
        })?
    })
}

fn titan_map_str<'a>(value: &'a MsgPackValue, key: &str) -> Option<&'a str> {
    value.as_map()?.iter().find_map(|(entry_key, entry_value)| {
        (entry_key.as_str() == Some(key))
            .then(|| entry_value.as_str())
            .flatten()
    })
}

fn titan_route_label(route: &MsgPackValue) -> String {
    titan_map_str(route, "label")
        .or_else(|| titan_map_str(route, "name"))
        .map(|label| format!("titan-route:{label}:stream"))
        .unwrap_or_else(|| "titan-route:stream:unknown".to_string())
}

fn titan_slippage_bps() -> u64 {
    env_u64_local("JUPITER_SLIPPAGE_BPS")
        .or_else(|| env_u64_local("MAX_SLIPPAGE_BPS"))
        .unwrap_or(50)
}

fn titan_accounts_limit() -> u64 {
    env_u64_local("JUPITER_MAX_ACCOUNTS").unwrap_or(64)
}

fn apply_titan_ws_protocol(request: &mut Request) -> Result<()> {
    let protocol = titan_ws_protocol();
    if protocol.trim().is_empty() {
        return Ok(());
    }
    request.headers_mut().insert(
        HeaderName::from_static("sec-websocket-protocol"),
        protocol
            .parse()
            .context("invalid Titan websocket protocol header value")?,
    );
    Ok(())
}

fn titan_ws_protocol() -> String {
    runtime_env_value("TITAN_WS_PROTOCOL").unwrap_or_else(|| "v1.api.titan.ag".to_string())
}

pub fn quote_from_json(
    provider: &AggregatorProviderConfig,
    pair: &TokenPair,
    side: SwapSide,
    fallback_in_amount_atoms: u128,
    value: &Value,
    network_fee: &FeeBreakdown,
    safety_buffer_bps: f64,
) -> Option<Quote> {
    let out_amount_atoms = first_u128(
        value,
        &[
            "/outAmount",
            "/data/outAmount",
            "/data/outputAmount",
            "/data/0/outAmount",
            "/data/0/toTokenAmount",
            "/outputAmount",
            "/destAmountOut",
            "/amountOut",
            "/quote/outAmount",
            "/data/quote/outAmount",
            "/data/0/toTokenAmount",
            "/data/0/routerResult/toTokenAmount",
            "/data/dexes/0/swapAmount",
            "/data/0/dexRouterList/0/amountOut",
            "/result/outAmount",
            "/bestRoute/outputAmount",
            "/bestRoute/outAmount",
            "/routes/0/outputAmount",
            "/routes/0/outAmount",
        ],
    )?;
    if out_amount_atoms == 0 {
        return None;
    }

    let in_amount_atoms = first_u128(
        value,
        &[
            "/inAmount",
            "/data/inAmount",
            "/data/inputAmount",
            "/data/0/inAmount",
            "/data/0/fromTokenAmount",
            "/inputAmount",
            "/srcAmountIn",
            "/amountIn",
            "/data/0/fromTokenAmount",
            "/data/0/routerResult/fromTokenAmount",
            "/bestRoute/inputAmount",
            "/bestRoute/inAmount",
            "/routes/0/inputAmount",
            "/routes/0/inAmount",
        ],
    )
    .unwrap_or(fallback_in_amount_atoms);

    let input_ui = atoms_to_ui(in_amount_atoms, input_decimals_for_side(pair, side));
    let output_ui = atoms_to_ui(out_amount_atoms, output_decimals_for_side(pair, side));
    let (quote_ui, base_ui) = match side {
        SwapSide::QuoteToBase => (input_ui, output_ui),
        SwapSide::BaseToQuote => (output_ui, input_ui),
    };
    if quote_ui == 0.0 || base_ui == 0.0 {
        return None;
    }

    let route_fee_atoms = first_u128(
        value,
        &[
            "/routePlan/0/swapInfo/feeAmount",
            "/data/0/feeAmount",
            "/data/routePlan/0/feeAmount",
            "/feeAmount",
            "/fee",
            "/data/fee",
            "/data/dexes/0/route/0/swapInfo/feeAmount",
        ],
    )
    .unwrap_or(0);
    let slippage_bps = first_f64(
        value,
        &[
            "/priceImpactPct",
            "/priceImpact",
            "/data/priceImpactPct",
            "/data/0/priceImpactPct",
            "/data/0/routerResult/priceImpactPct",
            "/data/0/dexRouterList/0/priceImpactPercent",
            "/priceImpactPercent",
            "/bestRoute/priceImpact",
            "/bestRoute/priceImpactPct",
            "/routes/0/priceImpact",
            "/routes/0/priceImpactPct",
        ],
    )
    .map(price_impact_to_bps)
    .unwrap_or(0.0);

    let mut fee = FeeBreakdown {
        lp_fee_atoms: 0,
        protocol_fee_atoms: 0,
        host_fee_atoms: route_fee_atoms,
        base_fee_lamports: 0,
        priority_fee_lamports: 0,
        jito_tip_lamports: 0,
        ata_rent_lamports: 0,
        slippage_bps,
        safety_buffer_bps,
        network_fee_usd: 0.0,
    };
    fee.merge_network(network_fee);

    Some(Quote {
        venue: provider.venue,
        pool: route_label(provider, value),
        pair: pair.clone(),
        side,
        in_amount_atoms,
        out_amount_atoms,
        effective_price: quote_ui / base_ui,
        fee,
        slot: first_u64(value, &["/contextSlot", "/data/contextSlot", "/slot"]).unwrap_or(0),
    })
}

fn quote_roundtrip_opportunity(
    buy: &Quote,
    sell: &Quote,
    input_quote_atoms: u128,
    sol_usd_estimate: f64,
    network_fee: &FeeBreakdown,
) -> Option<Opportunity> {
    if buy.side != SwapSide::QuoteToBase
        || sell.side != SwapSide::BaseToQuote
        || buy.pair.key() != sell.pair.key()
        || buy.venue == sell.venue
        || sell.out_amount_atoms <= input_quote_atoms
    {
        return None;
    }

    let gross_profit_quote_atoms = sell.out_amount_atoms as i128 - input_quote_atoms as i128;
    let input_usd = quote_amount_usd(
        input_quote_atoms,
        buy.pair.quote_decimals,
        &buy.pair.quote_mint,
        sol_usd_estimate,
    )?;
    let gross_profit_usd = quote_amount_usd_signed(
        gross_profit_quote_atoms,
        buy.pair.quote_decimals,
        &buy.pair.quote_mint,
        sol_usd_estimate,
    )?;
    let fee_usd = network_fee.network_fee_usd
        + quote_fee_usd(
            &buy.fee,
            buy.pair.quote_decimals,
            &buy.pair.quote_mint,
            sol_usd_estimate,
        )
        + quote_fee_usd(
            &sell.fee,
            sell.pair.base_decimals,
            &sell.pair.base_mint,
            sol_usd_estimate,
        );
    let net_profit_usd = gross_profit_usd - fee_usd;
    if net_profit_usd <= 0.0 || input_usd <= 0.0 {
        return None;
    }

    let mut fee = buy.fee.clone();
    fee.host_fee_atoms = fee.host_fee_atoms.saturating_add(sell.fee.host_fee_atoms);
    fee.lp_fee_atoms = fee.lp_fee_atoms.saturating_add(sell.fee.lp_fee_atoms);
    fee.protocol_fee_atoms = fee
        .protocol_fee_atoms
        .saturating_add(sell.fee.protocol_fee_atoms);
    fee.slippage_bps += sell.fee.slippage_bps;

    Some(Opportunity {
        pair: buy.pair.clone(),
        buy_venue: buy.venue,
        sell_venue: sell.venue,
        buy_pool: buy.pool.clone(),
        sell_pool: sell.pool.clone(),
        input_quote_atoms,
        base_atoms: buy.out_amount_atoms,
        output_quote_atoms: sell.out_amount_atoms,
        gross_profit_quote_atoms,
        net_profit_usd,
        net_profit_bps: net_profit_usd / input_usd * 10_000.0,
        spread_bps: gross_profit_usd / input_usd * 10_000.0,
        fee,
        detected_slot: buy.slot.max(sell.slot),
        optimizer_decision: None,
    })
}

fn quote_fee_usd(
    fee: &FeeBreakdown,
    input_decimals: u8,
    input_mint: &str,
    sol_usd_estimate: f64,
) -> f64 {
    quote_amount_usd(
        fee.host_fee_atoms
            .saturating_add(fee.lp_fee_atoms)
            .saturating_add(fee.protocol_fee_atoms),
        input_decimals,
        input_mint,
        sol_usd_estimate,
    )
    .unwrap_or(0.0)
}

fn quote_amount_usd(
    amount_atoms: u128,
    decimals: u8,
    mint: &str,
    sol_usd_estimate: f64,
) -> Option<f64> {
    match mint {
        WSOL_MINT => Some(atoms_to_ui(amount_atoms, decimals) * sol_usd_estimate),
        USDC_MINT | USDT_MINT => Some(atoms_to_ui(amount_atoms, decimals)),
        _ => None,
    }
}

fn quote_amount_usd_signed(
    amount_atoms: i128,
    decimals: u8,
    mint: &str,
    sol_usd_estimate: f64,
) -> Option<f64> {
    let sign = if amount_atoms < 0 { -1.0 } else { 1.0 };
    quote_amount_usd(
        amount_atoms.unsigned_abs(),
        decimals,
        mint,
        sol_usd_estimate,
    )
    .map(|amount| amount * sign)
}

fn route_label(provider: &AggregatorProviderConfig, value: &Value) -> String {
    let label = first_str(
        value,
        &[
            "/routePlan/0/swapInfo/label",
            "/routePlan/0/protocol",
            "/routePlan/0/venue",
            "/routePlan/0/swap",
            "/data/routePlan/0/protocol",
            "/data/0/dexProtocol/0/dexName",
            "/data/0/dexRouterList/0/dexProtocol/dexName",
            "/data/dexes/0/dexCode",
            "/data/dexes/0/route/0/swapInfo/label",
            "/bestRoute/dex",
            "/bestRoute/dexName",
            "/bestRoute/protocol",
            "/routes/0/dex",
            "/routes/0/dexName",
            "/routes/0/protocol",
        ],
    )
    .unwrap_or("route");
    let pool = first_str(
        value,
        &[
            "/routePlan/0/swapInfo/ammKey",
            "/routePlan/0/poolAddress",
            "/routePlan/0/marketKey",
            "/data/routePlan/0/poolId",
            "/data/routePlan/0/poolAddress",
            "/data/0/dexProtocol/0/protocol",
            "/data/0/dexRouterList/0/dexProtocol/dexName",
            "/data/dexes/0/route/0/swapInfo/ammKey",
            "/bestRoute/poolAddress",
            "/bestRoute/marketKey",
            "/bestRoute/ammKey",
            "/routes/0/poolAddress",
            "/routes/0/marketKey",
            "/routes/0/ammKey",
        ],
    )
    .unwrap_or("unknown");
    format!("{}-route:{label}:{pool}", provider.name)
}

fn first_u128(value: &Value, paths: &[&str]) -> Option<u128> {
    paths.iter().find_map(|path| {
        let value = value.pointer(path)?;
        value
            .as_u64()
            .map(|value| value as u128)
            .or_else(|| value.as_str()?.parse::<u128>().ok())
    })
}

fn first_u64(value: &Value, paths: &[&str]) -> Option<u64> {
    paths.iter().find_map(|path| {
        let value = value.pointer(path)?;
        value
            .as_u64()
            .or_else(|| value.as_str()?.parse::<u64>().ok())
    })
}

fn first_f64(value: &Value, paths: &[&str]) -> Option<f64> {
    paths.iter().find_map(|path| {
        let value = value.pointer(path)?;
        value
            .as_f64()
            .or_else(|| value.as_str()?.parse::<f64>().ok())
    })
}

fn first_str<'a>(value: &'a Value, paths: &[&str]) -> Option<&'a str> {
    paths.iter().find_map(|path| value.pointer(path)?.as_str())
}

fn price_impact_to_bps(value: f64) -> f64 {
    let value = value.abs();
    if value <= 1.0 {
        value * 10_000.0
    } else {
        value * 100.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::AggregatorProviderConfig,
        types::{USDC_MINT, WSOL_MINT},
    };
    use serde_json::json;

    fn pair() -> TokenPair {
        TokenPair {
            base_mint: "BASE".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 6,
            quote_decimals: 6,
        }
    }

    fn provider(format: AggregatorFormat) -> AggregatorProviderConfig {
        AggregatorProviderConfig {
            name: "jupiter".to_string(),
            venue: Venue::Jupiter,
            enabled: true,
            quote_url: "http://localhost/quote".to_string(),
            format,
            timeout_ms: 100,
            min_interval_ms: 1,
            max_in_flight: 1,
            requires_api_key: false,
            api_key_env: None,
            api_key_header: None,
            api_key_prefix: None,
        }
    }

    #[test]
    fn aggregator_quote_budget_uses_bounded_override() {
        assert_eq!(aggregator_quote_budget_ms_from_value(Some("3500")), 3_500);
        assert_eq!(aggregator_quote_budget_ms_from_value(Some("0")), 250);
        assert_eq!(
            aggregator_quote_budget_ms_from_value(Some("120000")),
            60_000
        );
        assert_eq!(
            aggregator_quote_budget_ms_from_value(Some("not-a-number")),
            5_000
        );
        assert_eq!(aggregator_quote_budget_ms_from_value(None), 5_000);
    }

    #[test]
    fn budget_exhaustion_cooldown_uses_bounded_override() {
        assert_eq!(
            budget_exhaustion_cooldown_from_value(Some("30")),
            Duration::from_secs(30)
        );
        assert_eq!(
            budget_exhaustion_cooldown_from_value(Some("0")),
            Duration::from_secs(1)
        );
        assert_eq!(
            budget_exhaustion_cooldown_from_value(Some("999")),
            Duration::from_secs(120)
        );
        assert_eq!(
            budget_exhaustion_cooldown_from_value(Some("not-a-number")),
            Duration::from_secs(15)
        );
        assert_eq!(
            budget_exhaustion_cooldown_from_value(None),
            Duration::from_secs(15)
        );
    }

    #[test]
    fn budget_exhaustion_backoff_scales_until_maximum() {
        assert_eq!(
            budget_exhaustion_cooldown_for_strike_from_values(Some("10"), Some("300"), 0),
            Duration::from_secs(10)
        );
        assert_eq!(
            budget_exhaustion_cooldown_for_strike_from_values(Some("10"), Some("300"), 1),
            Duration::from_secs(10)
        );
        assert_eq!(
            budget_exhaustion_cooldown_for_strike_from_values(Some("10"), Some("300"), 4),
            Duration::from_secs(80)
        );
        assert_eq!(
            budget_exhaustion_cooldown_for_strike_from_values(Some("10"), Some("300"), 8),
            Duration::from_secs(300)
        );
    }

    #[test]
    fn transient_error_backoff_scales_until_maximum() {
        assert_eq!(
            transient_error_cooldown_for_strike_from_values(Some("5"), Some("120"), 0),
            Duration::from_secs(5)
        );
        assert_eq!(
            transient_error_cooldown_for_strike_from_values(Some("5"), Some("120"), 1),
            Duration::from_secs(5)
        );
        assert_eq!(
            transient_error_cooldown_for_strike_from_values(Some("5"), Some("120"), 5),
            Duration::from_secs(80)
        );
        assert_eq!(
            transient_error_cooldown_for_strike_from_values(Some("5"), Some("120"), 8),
            Duration::from_secs(120)
        );
    }

    #[tokio::test]
    async fn provider_error_cooldown_records_reason_and_strikes() {
        let mut config = provider(AggregatorFormat::Jupiter);
        config.name = "test-provider-error-cooldown".to_string();
        let runtime = ProviderRuntime::new(config);
        let error = anyhow!("openocean quote request failed: operation timed out");

        let first = runtime.cool_down_for_provider_error(&error).await;
        let second = runtime.cool_down_for_provider_error(&error).await;

        assert_eq!(first, Some(Duration::from_secs(45)));
        assert_eq!(second, Some(Duration::from_secs(90)));
        let snapshot = runtime.snapshot().await;
        assert_eq!(snapshot.provider_error_strikes, 2);
        assert_eq!(
            snapshot.cooldown_reason.as_deref(),
            Some("transient provider error")
        );
        assert!(snapshot.cooldown_remaining_ms.is_some());
    }

    #[test]
    fn pending_budget_cools_only_unresponsive_pending_providers() {
        let mut pending = PendingProviderBudget::default();
        pending.track("slow");
        pending.track("slow");
        pending.track("fast");
        pending.complete("fast");

        assert_eq!(
            pending.unresponsive_pending_provider_names(),
            vec!["slow".to_string()]
        );

        pending.complete("slow");
        assert!(pending.unresponsive_pending_provider_names().is_empty());
    }

    #[tokio::test]
    async fn budgeted_explicit_quotes_keep_fast_provider_results() {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buffer = [0u8; 2048];
                    let read = socket.read(&mut buffer).await.unwrap_or_default();
                    let request = String::from_utf8_lossy(&buffer[..read]);
                    if request.contains("/slow") {
                        tokio::time::sleep(Duration::from_millis(1_000)).await;
                    }
                    let body = r#"{"inAmount":"25000000","outAmount":"125000000","routePlan":[{"swapInfo":{"label":"Fast","ammKey":"pool","feeAmount":"0"}}]}"#;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        let mut fast = provider(AggregatorFormat::Generic);
        fast.name = "fast".to_string();
        fast.quote_url = format!("{base_url}/fast");
        fast.timeout_ms = 2_000;
        let mut slow = provider(AggregatorFormat::Generic);
        slow.name = "slow".to_string();
        slow.quote_url = format!("{base_url}/slow");
        slow.timeout_ms = 2_000;
        let manager = AggregatorQuoteManager::new(
            AggregatorsConfig {
                enabled: true,
                request_timeout_ms: 2_000,
                cache_ttl_ms: 1_000,
                max_pairs_per_cycle: 8,
                seed_pairs: Vec::new(),
                providers: vec![fast, slow],
                ..AggregatorsConfig::default()
            },
            JupiterConfig {
                enabled: false,
                ..JupiterConfig::default()
            },
        );

        let quotes = manager
            .fetch_explicit_quotes_with_budget(
                vec![AggregatorQuoteRequest {
                    pair: pair(),
                    side: SwapSide::QuoteToBase,
                    in_amount_atoms: 25_000_000,
                }],
                &FeeBreakdown::default(),
                0.0,
                Some(250),
            )
            .await;
        server.abort();

        assert_eq!(quotes.len(), 1);
        assert!(quotes[0].pool.starts_with("fast-route:Fast:pool"));
    }

    #[test]
    fn jupiter_response_maps_to_quote() {
        let value = json!({
            "inAmount": "25000000",
            "outAmount": "125000000",
            "priceImpactPct": "0.001",
            "contextSlot": 7,
            "routePlan": [{
                "swapInfo": {
                    "label": "Whirlpool",
                    "ammKey": "pool",
                    "feeAmount": "120"
                }
            }]
        });

        let quote = quote_from_json(
            &provider(AggregatorFormat::Jupiter),
            &pair(),
            SwapSide::QuoteToBase,
            25_000_000,
            &value,
            &FeeBreakdown::default(),
            5.0,
        )
        .expect("quote should parse");

        assert_eq!(quote.venue, Venue::Jupiter);
        assert_eq!(quote.out_amount_atoms, 125_000_000);
        assert_eq!(quote.fee.host_fee_atoms, 120);
        assert!(quote.pool.contains("Whirlpool"));
        assert_eq!(quote.slot, 7);
    }

    #[test]
    fn titan_stream_route_maps_to_quote() {
        let mut provider = provider(AggregatorFormat::TitanStream);
        provider.name = "titan".to_string();
        provider.venue = Venue::Titan;
        let route = msgpack_map(vec![
            ("outAmount", MsgPackValue::from(125_000_000u64)),
            ("slippageBps", MsgPackValue::from(42u64)),
            ("label", MsgPackValue::from("JupiterZ")),
        ]);

        let quote = titan_quote_from_route(
            &provider,
            &pair(),
            SwapSide::QuoteToBase,
            25_000_000,
            &route,
            &FeeBreakdown::default(),
            5.0,
        )
        .expect("Titan route should map to a quote");

        assert_eq!(quote.venue, Venue::Titan);
        assert_eq!(quote.out_amount_atoms, 125_000_000);
        assert_eq!(quote.fee.slippage_bps, 42.0);
        assert_eq!(quote.pool, "titan-route:JupiterZ:stream");
    }

    #[test]
    fn nested_provider_response_maps_to_quote() {
        let value = json!({
            "data": [{
                "routerResult": {
                    "fromTokenAmount": "25000000",
                    "toTokenAmount": "100000000",
                    "priceImpactPct": "0.25"
                }
            }]
        });

        let mut provider = provider(AggregatorFormat::Okx);
        provider.name = "okx".to_string();
        provider.venue = Venue::Okx;

        let quote = quote_from_json(
            &provider,
            &pair(),
            SwapSide::QuoteToBase,
            25_000_000,
            &value,
            &FeeBreakdown::default(),
            5.0,
        )
        .expect("quote should parse");

        assert_eq!(quote.venue, Venue::Okx);
        assert_eq!(quote.in_amount_atoms, 25_000_000);
        assert_eq!(quote.out_amount_atoms, 100_000_000);
    }

    #[test]
    fn raydium_trade_api_response_maps_to_quote() {
        let value = json!({
            "success": true,
            "data": {
                "inputAmount": "1000000",
                "outputAmount": "82442",
                "routePlan": [{
                    "poolId": "raydium-pool",
                    "feeAmount": "2500"
                }]
            }
        });

        let mut provider = provider(AggregatorFormat::RaydiumTradeApi);
        provider.name = "raydium_trade_api".to_string();
        provider.venue = Venue::Raydium;

        let quote = quote_from_json(
            &provider,
            &pair(),
            SwapSide::QuoteToBase,
            1_000_000,
            &value,
            &FeeBreakdown::default(),
            5.0,
        )
        .expect("quote should parse");

        assert_eq!(quote.venue, Venue::Raydium);
        assert_eq!(quote.in_amount_atoms, 1_000_000);
        assert_eq!(quote.out_amount_atoms, 82_442);
        assert_eq!(quote.fee.host_fee_atoms, 2_500);
        assert!(quote.pool.contains("raydium-pool"));
    }

    #[test]
    fn base_to_quote_response_maps_to_quote() {
        let value = json!({
            "inAmount": "125000000",
            "outAmount": "26000000",
            "priceImpactPct": "0.001"
        });

        let quote = quote_from_json(
            &provider(AggregatorFormat::Jupiter),
            &pair(),
            SwapSide::BaseToQuote,
            125_000_000,
            &value,
            &FeeBreakdown::default(),
            5.0,
        )
        .expect("quote should parse");

        assert_eq!(quote.side, SwapSide::BaseToQuote);
        assert_eq!(quote.in_amount_atoms, 125_000_000);
        assert_eq!(quote.out_amount_atoms, 26_000_000);
        assert!((quote.effective_price - 0.208).abs() < 0.0001);
    }

    #[test]
    fn openocean_params_use_ui_amount_and_gas_price() {
        let params = request_params(
            AggregatorFormat::OpenOcean,
            &pair(),
            SwapSide::QuoteToBase,
            1_500_000,
        )
        .into_iter()
        .collect::<std::collections::HashMap<_, _>>();

        assert_eq!(
            params.get("inTokenAddress").map(String::as_str),
            Some(USDC_MINT)
        );
        assert_eq!(params.get("amount").map(String::as_str), Some("1.5"));
        assert_eq!(params.get("gasPrice").map(String::as_str), Some("0"));
    }

    #[test]
    fn okx_params_use_native_sol_address_for_wsol() {
        let sol_quoted_pair = TokenPair {
            base_mint: "BASE".to_string(),
            quote_mint: WSOL_MINT.to_string(),
            base_decimals: 6,
            quote_decimals: 9,
        };

        let buy_params = request_params(
            AggregatorFormat::Okx,
            &sol_quoted_pair,
            SwapSide::QuoteToBase,
            1_500_000_000,
        )
        .into_iter()
        .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(
            buy_params.get("fromTokenAddress").map(String::as_str),
            Some("11111111111111111111111111111111")
        );
        assert_eq!(
            buy_params.get("toTokenAddress").map(String::as_str),
            Some("BASE")
        );

        let sell_params = request_params(
            AggregatorFormat::Okx,
            &sol_quoted_pair,
            SwapSide::BaseToQuote,
            1_500_000,
        )
        .into_iter()
        .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(
            sell_params.get("fromTokenAddress").map(String::as_str),
            Some("BASE")
        );
        assert_eq!(
            sell_params.get("toTokenAddress").map(String::as_str),
            Some("11111111111111111111111111111111")
        );
    }

    #[test]
    fn carbium_q1_params_use_current_quote_shape() {
        let params = request_params(
            AggregatorFormat::CarbiumQ1,
            &pair(),
            SwapSide::QuoteToBase,
            1_500_000,
        )
        .into_iter()
        .collect::<std::collections::HashMap<_, _>>();

        assert_eq!(params.get("src_mint").map(String::as_str), Some(USDC_MINT));
        assert_eq!(params.get("dst_mint").map(String::as_str), Some("BASE"));
        assert_eq!(params.get("amount_in").map(String::as_str), Some("1500000"));
        assert_eq!(params.get("slippage_bps").map(String::as_str), Some("50"));
    }

    #[test]
    fn carbium_q1_response_maps_to_quote() {
        let value = json!({
            "srcAmountIn": "1000000",
            "destAmountOut": "1257091",
            "destAmountOutMin": "1250000",
            "priceImpactPct": "0",
            "routePlan": [{"swap": "Raydium", "percent": 100}]
        });

        let mut provider = provider(AggregatorFormat::CarbiumQ1);
        provider.name = "spiderswap".to_string();
        provider.venue = Venue::SpiderSwap;

        let quote = quote_from_json(
            &provider,
            &pair(),
            SwapSide::QuoteToBase,
            1_000_000,
            &value,
            &FeeBreakdown::default(),
            5.0,
        )
        .expect("quote should parse");

        assert_eq!(quote.venue, Venue::SpiderSwap);
        assert_eq!(quote.in_amount_atoms, 1_000_000);
        assert_eq!(quote.out_amount_atoms, 1_257_091);
        assert!(quote.pool.contains("Raydium"));
    }

    #[test]
    fn dflow_quote_response_maps_to_quote() {
        let value = json!({
            "contextSlot": 55,
            "inAmount": "1000000",
            "outAmount": "2000000",
            "priceImpactPct": "0.01",
            "routePlan": [{
                "venue": "Raydium CLMM",
                "marketKey": "market",
                "inAmount": "1000000",
                "outAmount": "2000000"
            }]
        });

        let mut provider = provider(AggregatorFormat::Dflow);
        provider.name = "dflow".to_string();
        provider.venue = Venue::Dflow;

        let quote = quote_from_json(
            &provider,
            &pair(),
            SwapSide::QuoteToBase,
            1_000_000,
            &value,
            &FeeBreakdown::default(),
            5.0,
        )
        .expect("quote should parse");

        assert_eq!(quote.venue, Venue::Dflow);
        assert_eq!(quote.out_amount_atoms, 2_000_000);
        assert_eq!(quote.slot, 55);
        assert!(quote.pool.contains("Raydium CLMM"));
    }

    #[test]
    fn vybe_params_use_ui_amount() {
        let params = request_params(
            AggregatorFormat::Vybe,
            &pair(),
            SwapSide::QuoteToBase,
            1_500_000,
        )
        .into_iter()
        .collect::<std::collections::HashMap<_, _>>();

        assert_eq!(params.get("amount").map(String::as_str), Some("1.5"));
        assert_eq!(params.get("inputMint").map(String::as_str), Some(USDC_MINT));
        assert_eq!(params.get("outputMint").map(String::as_str), Some("BASE"));
    }

    #[test]
    fn vybe_quote_body_uses_json_body_with_ui_amount_and_wallet() {
        let wallet = Pubkey::new_unique();
        let body = vybe_quote_body(&pair(), SwapSide::QuoteToBase, 1_500_000, Some(&wallet));

        assert_eq!(body["amount"], serde_json::json!(1.5));
        assert_eq!(body["inputMint"], USDC_MINT);
        assert_eq!(body["outputMint"], "BASE");
        assert_eq!(body["slippage"], serde_json::json!(0.5));
        assert_eq!(body["wallet"], wallet.to_string());
    }

    #[test]
    fn vybe_response_maps_to_quote() {
        let value = json!({
            "inAmount": "1000000",
            "outAmount": "185234567",
            "priceImpactPct": 0.01,
            "routePlan": [{
                "protocol": "RAYDIUM_CLMM",
                "poolAddress": "pool-address",
                "inAmount": "1000000",
                "outAmount": "185234567"
            }]
        });

        let mut provider = provider(AggregatorFormat::Vybe);
        provider.name = "vybe".to_string();
        provider.venue = Venue::Vybe;

        let quote = quote_from_json(
            &provider,
            &pair(),
            SwapSide::QuoteToBase,
            1_000_000,
            &value,
            &FeeBreakdown::default(),
            5.0,
        )
        .expect("quote should parse");

        assert_eq!(quote.venue, Venue::Vybe);
        assert_eq!(quote.out_amount_atoms, 185_234_567);
        assert!(quote.pool.contains("RAYDIUM_CLMM"));
    }

    #[test]
    fn venum_best_route_response_maps_to_quote() {
        let value = json!({
            "bestRoute": {
                "inputAmount": "25000000",
                "outputAmount": "199001234",
                "priceImpact": 0.004,
                "dex": "Meteora DLMM",
                "poolAddress": "pool-address"
            },
            "routes": [{
                "inputAmount": "25000000",
                "outputAmount": "199001234",
                "dex": "Meteora DLMM",
                "poolAddress": "pool-address"
            }]
        });
        let mut provider = provider(AggregatorFormat::Venum);
        provider.name = "venum".to_string();
        provider.venue = Venue::Venum;

        let quote = quote_from_json(
            &provider,
            &pair(),
            SwapSide::QuoteToBase,
            25_000_000,
            &value,
            &FeeBreakdown::default(),
            5.0,
        )
        .expect("Venum bestRoute quote should parse");

        assert_eq!(quote.venue, Venue::Venum);
        assert_eq!(quote.in_amount_atoms, 25_000_000);
        assert_eq!(quote.out_amount_atoms, 199_001_234);
        assert!((quote.fee.slippage_bps - 40.0).abs() < 0.000001);
        assert_eq!(quote.pool, "venum-route:Meteora DLMM:pool-address");
    }

    #[test]
    fn rate_limit_cooldown_uses_retry_after_header_hint() {
        let error =
            anyhow!("openocean quote http 429 Too Many Requests: retry_after_seconds=3442: limit");
        assert_eq!(rate_limit_cooldown(&error), Duration::from_secs(3442));
    }

    #[test]
    fn rate_limit_cooldown_uses_nonzero_fallback_for_zero_retry_hint() {
        let error =
            anyhow!("raydium_trade_api quote http 429 Too Many Requests: retry_after_seconds=0");
        assert_eq!(rate_limit_cooldown(&error), Duration::from_secs(300));
    }

    #[test]
    fn provider_cooldown_uses_short_backoff_for_transient_timeouts() {
        let error = anyhow!("openocean quote request failed: operation timed out");

        assert_eq!(
            provider_cooldown_for_error(&error),
            Some(Duration::from_secs(45))
        );
    }

    #[test]
    fn provider_cooldown_prefers_retry_after_for_rate_limits() {
        let error = anyhow!("openocean quote http 429 Too Many Requests: retry_after_seconds=120");

        assert_eq!(
            provider_cooldown_for_error(&error),
            Some(Duration::from_secs(120))
        );
    }

    #[test]
    fn trade_source_roundtrip_maps_positive_spread_to_opportunity() {
        let buy = Quote {
            venue: Venue::Raydium,
            pool: "raydium-route".to_string(),
            pair: pair(),
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 100_000_000,
            out_amount_atoms: 50_000_000,
            effective_price: 2.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let sell = Quote {
            venue: Venue::OpenOcean,
            pool: "openocean-route".to_string(),
            pair: pair(),
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 50_000_000,
            out_amount_atoms: 101_000_000,
            effective_price: 2.02,
            fee: FeeBreakdown::default(),
            slot: 2,
        };

        let opportunity =
            quote_roundtrip_opportunity(&buy, &sell, 100_000_000, 100.0, &FeeBreakdown::default())
                .expect("positive spread should become a candidate");

        assert_eq!(opportunity.buy_venue, Venue::Raydium);
        assert_eq!(opportunity.sell_venue, Venue::OpenOcean);
        assert_eq!(opportunity.output_quote_atoms, 101_000_000);
        assert!(opportunity.net_profit_usd > 0.0);
    }

    #[test]
    fn manager_deduplicates_jupiter_and_aggregator_seed_pairs() {
        let manager = AggregatorQuoteManager::new(
            AggregatorsConfig {
                providers: vec![],
                ..AggregatorsConfig::default()
            },
            JupiterConfig::default(),
        );
        let keys = manager
            .seed_pairs
            .iter()
            .map(TokenPair::key)
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(keys.len(), manager.seed_pairs.len());
        assert!(!manager.seed_pairs.is_empty());
    }

    #[test]
    fn target_pair_collection_is_not_capped_by_quote_provider_budget() {
        let manager = AggregatorQuoteManager::new(
            AggregatorsConfig {
                providers: vec![],
                max_pairs_per_cycle: 1,
                ..AggregatorsConfig::default()
            },
            JupiterConfig::default(),
        );

        assert_eq!(manager.collect_pairs(&[]).len(), 1);
        assert!(manager.collect_pairs_with_dynamic_unlimited(&[], &[]).len() > 1);
    }

    #[test]
    fn geckoterminal_pool_response_discovers_quote_pair_token() {
        let value = json!({
            "data": [{
                "type": "pool",
                "attributes": {
                    "reserve_in_usd": "92616.1649",
                    "fdv_usd": "652634.1777",
                    "volume_usd": {"h1": "38100.2886"},
                    "transactions": {"h1": {"buys": 377, "sells": 303}}
                },
                "relationships": {
                    "base_token": {"data": {"id": "solana_BASEMINT"}},
                    "quote_token": {"data": {"id": format!("solana_{USDC_MINT}")}},
                    "dex": {"data": {"id": "raydium-clmm"}}
                }
            }]
        });
        let quote_mints = [USDC_MINT.to_string()]
            .into_iter()
            .collect::<std::collections::HashSet<_>>();
        let allowed_dexes = geckoterminal_allowed_dex_ids();

        let config = AggregatorsConfig::default();
        let tokens = parse_geckoterminal_tokens(
            &value,
            &quote_mints,
            &allowed_dexes,
            dynamic_min_organic_score(&config),
        );

        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].mint, "BASEMINT");
        assert_eq!(tokens[0].observed_quote_mints, vec![USDC_MINT.to_string()]);
        assert_eq!(
            tokens[0].observed_venue_families,
            vec!["raydium".to_string()]
        );
        assert!(tokens[0].liquidity_usd > 90_000.0);
        assert!(tokens[0].score > 0.0);
    }

    #[test]
    fn token_discovery_rejects_pump_like_mints() {
        let token = TokenDiscoveryToken {
            mint: "Ce2gx9KGXJ6C9Mp5b5x1sn9Mg87JwEbrQby4Zqo3pump".to_string(),
            decimals: 6,
            observed_quote_mints: vec![USDC_MINT.to_string()],
            observed_venue_families: vec!["orca".to_string(), "meteora".to_string()],
            liquidity_usd: 250_000.0,
            mcap_usd: 5_000_000.0,
            organic_score: 100.0,
            is_verified: true,
            is_suspicious: false,
            score: 100.0,
        };
        let quote_mints = [USDC_MINT.to_string()]
            .into_iter()
            .collect::<std::collections::HashSet<_>>();
        let denylist = std::collections::HashSet::new();

        let config = AggregatorsConfig::default();

        assert!(!token_is_eligible(&token, &quote_mints, &denylist, &config));
    }

    #[test]
    fn token_discovery_rejects_blacklisted_remora_stock_mints() {
        let token = TokenDiscoveryToken {
            mint: "FJug11111111111111111111111111111111".to_string(),
            decimals: 6,
            observed_quote_mints: vec![USDC_MINT.to_string()],
            observed_venue_families: vec!["orca".to_string(), "meteora".to_string()],
            liquidity_usd: 250_000.0,
            mcap_usd: 5_000_000.0,
            organic_score: 100.0,
            is_verified: true,
            is_suspicious: false,
            score: 100.0,
        };
        let quote_mints = [USDC_MINT.to_string()]
            .into_iter()
            .collect::<std::collections::HashSet<_>>();
        let denylist = std::collections::HashSet::new();

        let config = AggregatorsConfig::default();

        assert!(!token_is_eligible(&token, &quote_mints, &denylist, &config));
    }
}
