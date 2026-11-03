use crate::{
    aggregators::{AggregatorQuoteManager, AggregatorQuoteRequest},
    config::{RwaConfig, RwaMode},
    env_runtime::{env_bool, env_value},
    types::{
        atoms_to_ui, is_blocklisted_remora_mint, is_blocklisted_remora_symbol, ui_to_atoms,
        FeeBreakdown, Quote, SwapSide, TokenPair, Venue, USDC_MINT, USDT_MINT, WSOL_MINT,
    },
};
use anyhow::{anyhow, Context, Result};
use chrono::{
    DateTime, Datelike, Duration as ChronoDuration, NaiveDate, TimeZone, Timelike, Utc, Weekday,
};
use futures_util::{stream::FuturesUnordered, StreamExt};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::Path,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{info, warn};

const PYTH_HERMES_BASE_URL: &str = "https://hermes.pyth.network";
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RwaSnapshot {
    pub enabled: bool,
    pub mode: RwaMode,
    pub directional_enabled: bool,
    pub ondo_primary_enabled: bool,
    pub assets_file: String,
    pub watched_assets: usize,
    pub oracle_prices: usize,
    pub pyth_feed_ids: Vec<String>,
    pub quote_status: String,
    pub quote_request_count: usize,
    pub quote_count: usize,
    pub jupiter_v2_enabled: bool,
    pub jupiter_v2_quote_with_taker: bool,
    pub jupiter_v2_taker_available: bool,
    pub market_session_open: bool,
    pub next_regular_session_open_unix_ms: Option<i64>,
    pub next_regular_session_open_utc: Option<String>,
    pub opportunity_count: usize,
    pub last_error: Option<String>,
    pub assets: Vec<RwaAssetSnapshot>,
    pub opportunities: Vec<RwaOpportunity>,
    pub execution: RwaExecutionSnapshot,
    pub pnl: RwaPnlSnapshot,
}

impl RwaSnapshot {
    pub fn disabled(config: &RwaConfig, reason: impl Into<String>) -> Self {
        Self {
            enabled: false,
            mode: config.mode,
            directional_enabled: config.directional_enabled,
            ondo_primary_enabled: config.ondo_primary_enabled,
            assets_file: config.assets_file.clone(),
            watched_assets: 0,
            oracle_prices: 0,
            pyth_feed_ids: Vec::new(),
            quote_status: "RWA lane disabled".to_string(),
            quote_request_count: 0,
            quote_count: 0,
            jupiter_v2_enabled: false,
            jupiter_v2_quote_with_taker: false,
            jupiter_v2_taker_available: false,
            market_session_open: false,
            next_regular_session_open_unix_ms: None,
            next_regular_session_open_utc: None,
            opportunity_count: 0,
            last_error: Some(reason.into()),
            assets: Vec::new(),
            opportunities: Vec::new(),
            execution: RwaExecutionSnapshot::default(),
            pnl: RwaPnlSnapshot::default(),
        }
    }
}

impl Default for RwaSnapshot {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: RwaMode::Neutral,
            directional_enabled: false,
            ondo_primary_enabled: false,
            assets_file: "rwa_assets.toml".to_string(),
            watched_assets: 0,
            oracle_prices: 0,
            pyth_feed_ids: Vec::new(),
            quote_status: "RWA lane not started".to_string(),
            quote_request_count: 0,
            quote_count: 0,
            jupiter_v2_enabled: false,
            jupiter_v2_quote_with_taker: false,
            jupiter_v2_taker_available: false,
            market_session_open: false,
            next_regular_session_open_unix_ms: None,
            next_regular_session_open_utc: None,
            opportunity_count: 0,
            last_error: None,
            assets: Vec::new(),
            opportunities: Vec::new(),
            execution: RwaExecutionSnapshot::default(),
            pnl: RwaPnlSnapshot::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RwaExecutionSnapshot {
    pub live_requested: bool,
    pub submission_enabled: bool,
    pub attempted_count: u64,
    pub submitted_count: u64,
    pub confirmed_count: u64,
    pub failed_count: u64,
    pub wallet_sol_lamports: u64,
    pub wallet_balances: Vec<RwaWalletBalanceSnapshot>,
    pub wallet_balance_errors: Vec<String>,
    pub stable_available_usd: f64,
    pub quote_funding_available_usd: f64,
    pub configured_trade_usd: f64,
    pub readiness_blockers: Vec<String>,
    pub fundable_direct_candidates: usize,
    pub inventory_blocked_opportunities: usize,
    pub route_gated_opportunities: usize,
    pub best_blocked_required_usd: Option<f64>,
    pub best_blocked_available_usd: Option<f64>,
    pub last_signature: Option<String>,
    pub last_error: Option<String>,
    pub reason: String,
    pub trades: Vec<RwaExecutionRecord>,
}

impl Default for RwaExecutionSnapshot {
    fn default() -> Self {
        Self {
            live_requested: false,
            submission_enabled: false,
            attempted_count: 0,
            submitted_count: 0,
            confirmed_count: 0,
            failed_count: 0,
            wallet_sol_lamports: 0,
            wallet_balances: Vec::new(),
            wallet_balance_errors: Vec::new(),
            stable_available_usd: 0.0,
            quote_funding_available_usd: 0.0,
            configured_trade_usd: 0.0,
            readiness_blockers: Vec::new(),
            fundable_direct_candidates: 0,
            inventory_blocked_opportunities: 0,
            route_gated_opportunities: 0,
            best_blocked_required_usd: None,
            best_blocked_available_usd: None,
            last_signature: None,
            last_error: None,
            reason: "RWA execution has not run yet".to_string(),
            trades: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RwaWalletBalanceSnapshot {
    pub symbol: String,
    pub mint: String,
    pub amount_atoms: u128,
    pub decimals: u8,
    pub ui_amount: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RwaExecutionRecord {
    pub id: u64,
    pub cycle: u64,
    pub submitted_unix_ms: u128,
    pub ticker: String,
    pub kind: RwaOpportunityKind,
    pub from_symbol: String,
    pub to_symbol: String,
    pub input_atoms: u128,
    pub estimated_input_usd: f64,
    pub expected_net_profit_usd: f64,
    pub expected_net_profit_bps: f64,
    pub submitted: bool,
    pub simulation_ok: bool,
    pub signature: Option<String>,
    pub solscan_url: Option<String>,
    pub leg_signatures: Vec<String>,
    pub leg_routes: Vec<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RwaPnlSnapshot {
    pub realized_profit_usd: f64,
    pub mark_to_pyth_profit_usd: f64,
    pub inventory_value_usd: f64,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RwaAssetSnapshot {
    pub ticker: String,
    pub enabled: bool,
    pub pyth_feed_id: String,
    pub xstock_symbol: String,
    pub xstock_mint: String,
    pub ondo_symbol: String,
    pub ondo_mint: String,
    pub ondo_shares_per_token: f64,
    pub oracle: Option<PythOraclePrice>,
    pub xstock_price_usd: Option<f64>,
    pub ondo_price_usd: Option<f64>,
    pub ondo_normalized_price_usd: Option<f64>,
    pub xstock_premium_bps: Option<f64>,
    pub ondo_premium_bps: Option<f64>,
    pub quote_assets: Vec<String>,
    pub quote_count: usize,
    pub direct_route_request_count: usize,
    pub direct_route_count: usize,
    pub primary_route_count: usize,
    pub primary_routes: Vec<RwaPrimaryRouteSnapshot>,
    pub best_rejected_kind: Option<RwaOpportunityKind>,
    pub best_rejected_net_profit_bps: Option<f64>,
    pub best_rejected_route: Option<String>,
    pub best_rejected_reason: Option<String>,
    pub best_rejected_by_kind: Vec<RwaRejectedOpportunitySnapshot>,
    pub no_trade_reason: String,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RwaRejectedOpportunitySnapshot {
    pub kind: RwaOpportunityKind,
    pub net_profit_bps: f64,
    pub net_profit_usd: f64,
    pub route: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RwaPrimaryRouteSnapshot {
    pub provider: String,
    pub symbol: String,
    pub mint: String,
    pub decimals: u8,
    pub shares_per_token: f64,
    pub source: String,
    pub price_usd: f64,
    pub normalized_price_usd: f64,
    pub mint_enabled: bool,
    pub burn_enabled: bool,
    pub rfq_mint_available: bool,
    pub rfq_burn_available: bool,
    pub rfq_mint_price_usd: Option<f64>,
    pub rfq_burn_price_usd: Option<f64>,
    pub rfq_mint_route: Option<String>,
    pub rfq_burn_route: Option<String>,
    pub non_rfq_mint_price_usd: Option<f64>,
    pub non_rfq_burn_price_usd: Option<f64>,
    pub non_rfq_mint_route: Option<String>,
    pub non_rfq_burn_route: Option<String>,
    pub external_builder_configured: bool,
    pub readiness: RwaPrimaryRouteReadiness,
    pub executable: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RwaPrimaryRouteReadiness {
    OracleBlocked,
    RfqReady,
    ExternalBuilderReady,
    WaitingForRfq,
    AuthMissing,
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RwaOpportunity {
    pub ticker: String,
    pub kind: RwaOpportunityKind,
    pub from_symbol: String,
    pub to_symbol: String,
    pub from_mint: String,
    pub to_mint: String,
    pub quote_asset: Option<String>,
    pub pyth_fair_price_usd: f64,
    pub from_price_usd: Option<f64>,
    pub to_price_usd: Option<f64>,
    pub input_atoms: Option<u128>,
    pub input_estimated_usd: Option<f64>,
    pub expected_output_atoms: Option<u128>,
    pub route: String,
    pub gross_edge_bps: f64,
    pub net_profit_bps: f64,
    pub net_profit_usd: f64,
    pub confidence_buffer_bps: f64,
    pub fee: FeeBreakdown,
    pub executable: bool,
    pub execution_legs: Vec<RwaExecutionLeg>,
    pub blocked_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RwaExecutionLeg {
    pub input_mint: String,
    pub output_mint: String,
    pub input_decimals: u8,
    pub output_decimals: u8,
    pub side: SwapSide,
    pub input_atoms: u128,
    pub estimated_input_usd: Option<f64>,
    pub route: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RwaOpportunityKind {
    StableRouteArbitrage,
    NeutralInventoryRebalance,
    DirectIssuerSwap,
    StableFundedDirectIssuerRoundtrip,
    StableFundedDirectIssuerAccumulation,
    DirectionalSell,
    PythDiscountBuy,
    PrimaryMintSell,
    PrimaryBuyBurn,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythOraclePrice {
    pub feed_id: String,
    pub price_usd: f64,
    pub confidence_usd: f64,
    pub confidence_bps: f64,
    pub publish_time: i64,
    pub age_ms: u64,
    pub ema_price_usd: Option<f64>,
    pub status: OracleStatus,
    pub market_session: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OracleStatus {
    Valid,
    Missing,
    MarketClosed,
    Stale,
    WideConfidence,
    InvalidPrice,
}

impl OracleStatus {
    fn label(self) -> &'static str {
        match self {
            OracleStatus::Valid => "valid",
            OracleStatus::Missing => "missing",
            OracleStatus::MarketClosed => "market_closed",
            OracleStatus::Stale => "stale",
            OracleStatus::WideConfidence => "wide_confidence",
            OracleStatus::InvalidPrice => "invalid_price",
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
struct RwaAssetRegistry {
    #[serde(default)]
    assets: BTreeMap<String, RwaAssetConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RwaAssetConfig {
    #[serde(default)]
    pub ticker: String,
    #[serde(default)]
    pub enabled: bool,
    pub pyth_feed_id: String,
    #[serde(default)]
    pub xstock_symbol: String,
    pub xstock_mint: String,
    #[serde(default)]
    pub xstock_decimals: u8,
    #[serde(default)]
    pub xstock_token_program: String,
    #[serde(default)]
    pub ondo_symbol: String,
    pub ondo_mint: String,
    #[serde(default)]
    pub ondo_decimals: u8,
    #[serde(default)]
    pub ondo_token_program: String,
    #[serde(default)]
    pub flash_symbol: String,
    #[serde(default)]
    pub flash_mint: String,
    #[serde(default)]
    pub flash_decimals: u8,
    #[serde(default)]
    pub flash_token_program: String,
    #[serde(default)]
    pub flash_mint_burn_enabled: bool,
    #[serde(default = "default_shares_per_token")]
    pub flash_shares_per_token: f64,
    #[serde(default = "default_ondo_shares_per_token")]
    pub ondo_shares_per_token: f64,
    #[serde(default)]
    pub max_trade_usd: Option<f64>,
    #[serde(default)]
    pub min_net_profit_bps: Option<f64>,
    #[serde(default)]
    pub max_oracle_confidence_bps: Option<f64>,
    #[serde(default)]
    pub max_oracle_age_ms: Option<u64>,
    #[serde(default)]
    pub regular_hours_only: Option<bool>,
    #[serde(default)]
    pub source: String,
}

fn default_ondo_shares_per_token() -> f64 {
    default_shares_per_token()
}

fn default_shares_per_token() -> f64 {
    1.0
}

#[derive(Debug, Clone)]
struct RwaAssetRuntime {
    asset: RwaAssetConfig,
}

#[derive(Debug, Clone)]
struct RwaQuoteAsset {
    symbol: String,
    mint: String,
    decimals: u8,
    usd_per_unit: f64,
}

impl RwaQuoteAsset {
    fn input_atoms_for_usd(&self, usd: f64) -> u128 {
        let units = usd / self.usd_per_unit.max(f64::EPSILON);
        ui_to_atoms(units, self.decimals)
    }

    fn input_usd_for_atoms(&self, atoms: u128) -> f64 {
        atoms_to_ui(atoms, self.decimals) * self.usd_per_unit
    }
}

#[derive(Debug, Clone)]
struct IssuerQuotes {
    symbol: String,
    mint: String,
    decimals: u8,
    shares_per_token: f64,
    quote_usd_per_unit: f64,
    best_buy: Option<Quote>,
    best_sell: Option<Quote>,
    secondary_buy: Option<Quote>,
    secondary_sell: Option<Quote>,
    rfq_buy: Option<Quote>,
    rfq_sell: Option<Quote>,
}

#[derive(Debug, Clone)]
struct ConfiguredIssuer {
    symbol: String,
    mint: String,
    decimals: u8,
    shares_per_token: f64,
}

struct RwaQuotePlan<'a> {
    asset: &'a RwaAssetConfig,
    trade_usd: f64,
    issuers: Vec<ConfiguredIssuer>,
    issuer_atoms: HashMap<String, u128>,
}

impl IssuerQuotes {
    fn buy_price_usd(&self) -> Option<f64> {
        self.best_buy.as_ref().and_then(|quote| {
            stable_price_per_token_usd(quote, self.decimals, self.quote_usd_per_unit)
        })
    }

    fn sell_price_usd(&self) -> Option<f64> {
        self.best_sell.as_ref().and_then(|quote| {
            stable_price_per_token_usd(quote, self.decimals, self.quote_usd_per_unit)
        })
    }

    fn executable_price_usd(&self) -> Option<f64> {
        match (self.buy_price_usd(), self.sell_price_usd()) {
            (Some(buy), Some(sell)) => Some((buy + sell) / 2.0),
            (Some(buy), None) => Some(buy),
            (None, Some(sell)) => Some(sell),
            (None, None) => None,
        }
    }

    fn normalized_buy_price_usd(&self) -> Option<f64> {
        self.buy_price_usd()
            .map(|price| price / self.shares_per_token.max(f64::EPSILON))
    }

    fn normalized_sell_price_usd(&self) -> Option<f64> {
        self.sell_price_usd()
            .map(|price| price / self.shares_per_token.max(f64::EPSILON))
    }

    fn secondary_buy_price_usd(&self) -> Option<f64> {
        self.secondary_buy.as_ref().and_then(|quote| {
            stable_price_per_token_usd(quote, self.decimals, self.quote_usd_per_unit)
        })
    }

    fn secondary_sell_price_usd(&self) -> Option<f64> {
        self.secondary_sell.as_ref().and_then(|quote| {
            stable_price_per_token_usd(quote, self.decimals, self.quote_usd_per_unit)
        })
    }

    fn normalized_secondary_buy_price_usd(&self) -> Option<f64> {
        self.secondary_buy_price_usd()
            .map(|price| price / self.shares_per_token.max(f64::EPSILON))
    }

    fn normalized_secondary_sell_price_usd(&self) -> Option<f64> {
        self.secondary_sell_price_usd()
            .map(|price| price / self.shares_per_token.max(f64::EPSILON))
    }

    fn normalized_executable_price_usd(&self) -> Option<f64> {
        self.executable_price_usd()
            .map(|price| price / self.shares_per_token.max(f64::EPSILON))
    }
}

pub async fn scan_rwa(
    config: &RwaConfig,
    aggregators: &AggregatorQuoteManager,
    network_fee: &FeeBreakdown,
    sol_usd_estimate: f64,
    cycle: u64,
    taker_pubkey: Option<&str>,
    taker_wallet_balances: Option<&HashMap<String, u128>>,
) -> RwaSnapshot {
    if !config.enabled {
        return RwaSnapshot::disabled(config, "rwa.enabled=false");
    }

    let assets = match load_asset_registry(&config.assets_file, config.max_assets_per_cycle).await {
        Ok(assets) => assets,
        Err(error) => return RwaSnapshot::disabled(config, format!("{error:#}")),
    };
    if assets.is_empty() {
        return RwaSnapshot::disabled(config, "no enabled RWA assets in registry");
    }

    let quote_assets = quote_assets_from_config(&config.quote_assets, sol_usd_estimate);
    if quote_assets.is_empty() {
        return RwaSnapshot::disabled(config, "no supported RWA quote assets configured");
    }

    let oracle_client = PythOracleClient::default();
    let oracle_prices = match oracle_client.fetch_latest(&assets, config).await {
        Ok(prices) => prices,
        Err(error) => {
            warn!(?error, "rwa pyth fetch failed");
            HashMap::new()
        }
    };

    let pyth_feed_ids = pyth_feed_ids_for_assets(&assets);
    let quote_ready_assets = assets_with_quote_oracles(&assets, &oracle_prices);
    let fundable_quote_requests = build_quote_requests_for_balances(
        config,
        &quote_ready_assets,
        &quote_assets,
        &oracle_prices,
        taker_wallet_balances,
    );
    let all_quote_requests = build_quote_requests_for_balances(
        config,
        &quote_ready_assets,
        &quote_assets,
        &oracle_prices,
        None,
    );
    let quote_request_count = all_quote_requests.len();
    let jupiter_v2_enabled = rwa_jupiter_v2_enabled();
    let jupiter_v2_quote_with_taker = rwa_jupiter_v2_quote_with_taker();
    let jupiter_v2_taker = rwa_jupiter_v2_quote_taker(taker_pubkey, jupiter_v2_quote_with_taker);
    let jupiter_v2_taker_available = jupiter_v2_taker.is_some();
    let now = Utc::now();
    let market_session_open = us_equity_session_at(now) == UsEquitySession::Regular;
    let next_regular_session_open = (!market_session_open)
        .then(|| next_us_equity_regular_open_at(now))
        .flatten();
    let (mut jupiter_v2_taker_quote_requests, mut jupiter_v2_quote_only_requests) =
        if jupiter_v2_taker_available {
            partition_taker_fundable_quote_requests(&fundable_quote_requests, taker_wallet_balances)
        } else {
            (Vec::new(), fundable_quote_requests.clone())
        };
    retain_jupiter_v2_min_order_requests(
        &mut jupiter_v2_taker_quote_requests,
        &quote_assets,
        &quote_ready_assets,
        &oracle_prices,
    );
    cap_quote_requests_for_cycle(
        &mut jupiter_v2_taker_quote_requests,
        rwa_jupiter_v2_taker_max_requests(),
        cycle,
        0,
    );
    jupiter_v2_quote_only_requests.extend(requests_not_in(
        &all_quote_requests,
        &jupiter_v2_taker_quote_requests,
    ));
    dedupe_quote_requests(&mut jupiter_v2_quote_only_requests);
    retain_jupiter_v2_min_order_requests(
        &mut jupiter_v2_quote_only_requests,
        &quote_assets,
        &quote_ready_assets,
        &oracle_prices,
    );
    let mut aggregator_quote_requests = if taker_wallet_balances.is_some() {
        jupiter_v2_taker_quote_requests.clone()
    } else {
        fundable_quote_requests.clone()
    };
    if rwa_aggregator_quote_only_enabled() {
        aggregator_quote_requests.extend(jupiter_v2_quote_only_requests.clone());
    }
    let aggregator_quotes = fetch_rwa_aggregator_quotes(
        aggregators,
        aggregator_quote_requests,
        network_fee,
        config.fee_safety_buffer_bps,
    );
    cap_quote_requests_for_cycle(
        &mut jupiter_v2_quote_only_requests,
        rwa_jupiter_v2_quote_only_max_requests(),
        cycle,
        1,
    );
    let mut jupiter_v2_rfq_only_requests = if rwa_jupiter_v2_rfq_only_enabled() {
        build_primary_rfq_probe_quote_requests(
            config,
            &quote_ready_assets,
            &quote_assets,
            &oracle_prices,
        )
    } else {
        Vec::new()
    };
    retain_jupiter_v2_min_order_requests(
        &mut jupiter_v2_rfq_only_requests,
        &quote_assets,
        &quote_ready_assets,
        &oracle_prices,
    );
    cap_quote_requests_for_cycle(
        &mut jupiter_v2_rfq_only_requests,
        rwa_jupiter_v2_rfq_only_max_requests(),
        cycle,
        2,
    );
    let jupiter_v2_quotes = async {
        if jupiter_v2_enabled {
            let taker_quotes = fetch_scoped_jupiter_v2_order_quotes(
                jupiter_v2_taker_quote_requests,
                network_fee,
                config.fee_safety_buffer_bps,
                jupiter_v2_taker,
            );
            let quote_only_quotes = fetch_scoped_jupiter_v2_order_quotes(
                jupiter_v2_quote_only_requests,
                network_fee,
                config.fee_safety_buffer_bps,
                None,
            );
            let rfq_only_quotes = fetch_scoped_jupiter_v2_order_quotes_with_router_excludes(
                jupiter_v2_rfq_only_requests,
                network_fee,
                config.fee_safety_buffer_bps,
                None,
                rwa_jupiter_v2_rfq_only_exclude_routers(),
            );
            let (mut taker_quotes, quote_only_quotes, rfq_only_quotes) =
                tokio::join!(taker_quotes, quote_only_quotes, rfq_only_quotes);
            taker_quotes.extend(quote_only_quotes);
            taker_quotes.extend(rfq_only_quotes);
            taker_quotes
        } else {
            Vec::new()
        }
    };
    let (mut quotes, jupiter_v2_quotes) = tokio::join!(aggregator_quotes, jupiter_v2_quotes);
    quotes.extend(jupiter_v2_quotes);
    let mut jupiter_v1_fallback_requests = missing_quote_requests(&all_quote_requests, &quotes);
    cap_quote_requests_for_cycle(
        &mut jupiter_v1_fallback_requests,
        Some(rwa_jupiter_v1_fallback_max_requests()),
        cycle,
        3,
    );
    if !jupiter_v1_fallback_requests.is_empty() {
        quotes.extend(
            fetch_scoped_jupiter_quotes(
                jupiter_v1_fallback_requests,
                network_fee,
                config.fee_safety_buffer_bps,
            )
            .await,
        );
    }
    let fresh_quote_count = quotes.len();
    let cached_quote_count = merge_rwa_quote_cache(&mut quotes);
    dedupe_quotes(&mut quotes);
    let blocked_quote_count = remove_rwa_live_blocked_quotes(&mut quotes);
    info!(
        fresh_quote_count,
        cached_quote_count,
        blocked_quote_count,
        quote_count = quotes.len(),
        quote_sample = quotes
            .iter()
            .take(6)
            .map(|quote| {
                format!(
                    "{}>{}:{}:{}",
                    short_mint(input_mint_for_side(&quote.pair, quote.side)),
                    short_mint(output_mint_for_side(&quote.pair, quote.side)),
                    quote.in_amount_atoms,
                    quote.pool
                )
            })
            .collect::<Vec<_>>()
            .join(","),
        "RWA quote collection complete"
    );
    let quote_status = rwa_quote_status(
        &assets,
        &quote_ready_assets,
        &oracle_prices,
        quote_request_count,
        quotes.len(),
    );
    let quote_index = QuoteIndex::with_quote_assets(quotes.clone(), &quote_assets);

    let mut asset_snapshots = Vec::new();
    let mut opportunities = Vec::new();
    for runtime in assets {
        let (asset_snapshot, mut asset_opportunities) = evaluate_asset(
            config,
            &runtime,
            &quote_assets,
            &oracle_prices,
            &quote_index,
            network_fee,
        );
        asset_snapshots.push(asset_snapshot);
        opportunities.append(&mut asset_opportunities);
    }

    opportunities.sort_by(|a, b| {
        b.net_profit_usd
            .partial_cmp(&a.net_profit_usd)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let note = if config.execution_enabled {
        "RWA live execution is enabled; stable-funded direct issuer round-trips can execute from configured quote assets after valid Pyth/session, route, funding, and fee gates pass."
    } else if config.emit_execution_candidates {
        "RWA execution candidates are visible for monitoring; live submission is disabled until rwa.execution_enabled is true."
    } else {
        "RWA lane is monitoring and scoring executable quotes; live RWA execution candidates are not emitted by default."
    };

    RwaSnapshot {
        enabled: true,
        mode: config.mode,
        directional_enabled: config.directional_enabled,
        ondo_primary_enabled: config.ondo_primary_enabled,
        assets_file: config.assets_file.clone(),
        watched_assets: asset_snapshots.len(),
        oracle_prices: oracle_prices
            .values()
            .filter(|price| price.status == OracleStatus::Valid)
            .count(),
        pyth_feed_ids,
        quote_status,
        quote_request_count,
        quote_count: quotes.len(),
        jupiter_v2_enabled,
        jupiter_v2_quote_with_taker,
        jupiter_v2_taker_available,
        market_session_open,
        next_regular_session_open_unix_ms: next_regular_session_open
            .as_ref()
            .map(DateTime::timestamp_millis),
        next_regular_session_open_utc: next_regular_session_open.as_ref().map(DateTime::to_rfc3339),
        opportunity_count: opportunities.len(),
        last_error: None,
        assets: asset_snapshots,
        opportunities,
        execution: RwaExecutionSnapshot::default(),
        pnl: RwaPnlSnapshot {
            note: note.to_string(),
            ..RwaPnlSnapshot::default()
        },
    }
}

async fn fetch_rwa_aggregator_quotes(
    aggregators: &AggregatorQuoteManager,
    requests: Vec<AggregatorQuoteRequest>,
    network_fee: &FeeBreakdown,
    safety_buffer_bps: f64,
) -> Vec<Quote> {
    let budget_ms = rwa_aggregator_quote_budget_ms();
    aggregators
        .fetch_explicit_quotes_with_budget(
            requests,
            network_fee,
            safety_buffer_bps,
            (budget_ms > 0).then_some(budget_ms),
        )
        .await
}

async fn load_asset_registry(path: &str, max_assets: usize) -> Result<Vec<RwaAssetRuntime>> {
    let path = Path::new(path);
    let text = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("failed to read RWA asset registry {}", path.display()))?;
    let registry: RwaAssetRegistry = toml::from_str(&text)
        .with_context(|| format!("failed to parse RWA asset registry {}", path.display()))?;
    let mut assets = Vec::new();
    for (key, mut asset) in registry.assets {
        if !asset.enabled {
            continue;
        }
        if asset.ticker.is_empty() {
            asset.ticker = key.clone();
        }
        validate_asset(&key, &asset)?;
        assets.push(RwaAssetRuntime { asset });
        if max_assets > 0 && assets.len() >= max_assets {
            break;
        }
    }
    Ok(assets)
}

fn validate_asset(key: &str, asset: &RwaAssetConfig) -> Result<()> {
    if !is_pyth_feed_id(&asset.pyth_feed_id) {
        return Err(anyhow!("{key}: invalid pinned Pyth feed id"));
    }
    let xstock_symbol = asset
        .xstock_symbol
        .clone()
        .if_empty_then(|| format!("{}x", asset.ticker));
    let ondo_symbol = asset
        .ondo_symbol
        .clone()
        .if_empty_then(|| format!("{}on", asset.ticker));
    for (label, symbol, mint) in [
        ("xstock", xstock_symbol.as_str(), asset.xstock_mint.as_str()),
        ("ondo", ondo_symbol.as_str(), asset.ondo_mint.as_str()),
    ] {
        if let Some(reason) = blacklisted_remora_token_reason(label, symbol, mint) {
            return Err(anyhow!("{key}: {reason}"));
        }
    }
    for (label, mint) in [
        ("xstock_mint", &asset.xstock_mint),
        ("ondo_mint", &asset.ondo_mint),
    ] {
        if mint.trim().is_empty() {
            return Err(anyhow!("{key}: {label} is empty"));
        }
        if mint.to_ascii_lowercase().contains("pump") {
            return Err(anyhow!(
                "{key}: {label} looks like a pump token and is rejected"
            ));
        }
        if mint.len() < 32 || mint.len() > 48 {
            return Err(anyhow!("{key}: {label} is not a plausible Solana mint"));
        }
    }
    if !asset.flash_symbol.trim().is_empty()
        || !asset.flash_mint.trim().is_empty()
        || !asset.flash_token_program.trim().is_empty()
        || asset.flash_decimals != 0
        || asset.flash_mint_burn_enabled
        || (asset.flash_shares_per_token - 1.0).abs() > f64::EPSILON
    {
        return Err(anyhow!(
            "{key}: Flash/Remora issuer fields are no longer supported; configure only official xStock and Ondo mints"
        ));
    }
    if asset.xstock_decimals == 0 || asset.ondo_decimals == 0 {
        return Err(anyhow!("{key}: token decimals must be configured"));
    }
    if asset.ondo_shares_per_token <= 0.0 || !asset.ondo_shares_per_token.is_finite() {
        return Err(anyhow!("{key}: ondo_shares_per_token must be positive"));
    }
    Ok(())
}

fn blacklisted_remora_token_reason(label: &str, symbol: &str, mint: &str) -> Option<String> {
    let trimmed_symbol = symbol.trim();
    if is_blocklisted_remora_symbol(trimmed_symbol) {
        return Some(format!(
            "{label}_symbol {trimmed_symbol} is blacklisted because deprecated Remora/rStock markets must not trade"
        ));
    }

    let mint = mint.trim();
    if is_blocklisted_remora_mint(mint) {
        return Some(format!(
            "{label}_mint {prefix}... is blacklisted because it matches a deprecated Remora/rStock mint",
            prefix = &mint[..mint.len().min(6)]
        ));
    }

    None
}

fn is_pyth_feed_id(value: &str) -> bool {
    value.len() == 64 && value.chars().all(|ch| ch.is_ascii_hexdigit())
}

#[derive(Clone)]
struct PythOracleClient {
    client: Client,
    base_url: String,
}

impl Default for PythOracleClient {
    fn default() -> Self {
        Self {
            client: Client::new(),
            base_url: PYTH_HERMES_BASE_URL.to_string(),
        }
    }
}

impl PythOracleClient {
    async fn fetch_latest(
        &self,
        assets: &[RwaAssetRuntime],
        config: &RwaConfig,
    ) -> Result<HashMap<String, PythOraclePrice>> {
        let mut seen = HashSet::new();
        let feed_ids = assets
            .iter()
            .filter_map(|runtime| {
                let id = runtime.asset.pyth_feed_id.trim().to_string();
                seen.insert(id.clone()).then_some(id)
            })
            .collect::<Vec<_>>();
        if feed_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut url = Url::parse(&format!(
            "{}/v2/updates/price/latest",
            self.base_url.trim_end_matches('/')
        ))?;
        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("parsed", "true");
            for feed_id in &feed_ids {
                pairs.append_pair("ids[]", feed_id);
            }
        }

        let response = self
            .client
            .get(url)
            .send()
            .await
            .context("Pyth Hermes latest price request failed")?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Pyth Hermes http {status}: {body}"));
        }

        let payload = response
            .json::<HermesLatestPriceResponse>()
            .await
            .context("invalid Pyth Hermes latest price json")?;
        let mut prices = HashMap::new();
        for parsed in payload.parsed {
            let max_age_ms = assets
                .iter()
                .find(|asset| asset.asset.pyth_feed_id == parsed.id)
                .and_then(|asset| asset.asset.max_oracle_age_ms)
                .unwrap_or(config.max_oracle_age_ms);
            let max_confidence_bps = assets
                .iter()
                .find(|asset| asset.asset.pyth_feed_id == parsed.id)
                .and_then(|asset| asset.asset.max_oracle_confidence_bps)
                .unwrap_or(config.max_oracle_confidence_bps);
            let regular_hours_only = assets
                .iter()
                .find(|asset| asset.asset.pyth_feed_id == parsed.id)
                .and_then(|asset| asset.asset.regular_hours_only)
                .unwrap_or(config.regular_hours_only);
            prices.insert(
                parsed.id.clone(),
                pyth_price_from_update(parsed, max_age_ms, max_confidence_bps, regular_hours_only),
            );
        }
        Ok(prices)
    }
}

#[derive(Debug, Deserialize)]
struct HermesLatestPriceResponse {
    #[serde(default)]
    parsed: Vec<HermesParsedPrice>,
}

#[derive(Debug, Deserialize)]
struct HermesParsedPrice {
    id: String,
    price: HermesPrice,
    #[serde(default)]
    ema_price: Option<HermesPrice>,
    #[serde(default)]
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct HermesPrice {
    price: String,
    conf: String,
    expo: i32,
    publish_time: i64,
}

fn pyth_price_from_update(
    update: HermesParsedPrice,
    max_age_ms: u64,
    max_confidence_bps: f64,
    regular_hours_only: bool,
) -> PythOraclePrice {
    pyth_price_from_update_at(
        update,
        max_age_ms,
        max_confidence_bps,
        regular_hours_only,
        Utc::now(),
    )
}

fn pyth_price_from_update_at(
    update: HermesParsedPrice,
    max_age_ms: u64,
    max_confidence_bps: f64,
    regular_hours_only: bool,
    now: DateTime<Utc>,
) -> PythOraclePrice {
    let price_usd = pyth_fixed_to_f64(&update.price.price, update.price.expo).unwrap_or(0.0);
    let confidence_usd = pyth_fixed_to_f64(&update.price.conf, update.price.expo).unwrap_or(0.0);
    let confidence_bps = if price_usd > 0.0 {
        confidence_usd / price_usd * 10_000.0
    } else {
        f64::INFINITY
    };
    let now_ms = now.timestamp_millis().max(0) as u128;
    let publish_ms = update.price.publish_time.max(0) as u128 * 1_000;
    let age_ms = now_ms.saturating_sub(publish_ms).min(u64::MAX as u128) as u64;
    let us_equity_session = us_equity_session_at(now);
    let status = if price_usd <= 0.0 || !price_usd.is_finite() {
        OracleStatus::InvalidPrice
    } else if regular_hours_only && us_equity_session != UsEquitySession::Regular {
        OracleStatus::MarketClosed
    } else if age_ms > max_age_ms {
        OracleStatus::Stale
    } else if confidence_bps > max_confidence_bps {
        OracleStatus::WideConfidence
    } else {
        OracleStatus::Valid
    };
    let ema_price_usd = update
        .ema_price
        .as_ref()
        .and_then(|price| pyth_fixed_to_f64(&price.price, price.expo));
    let slot = update
        .metadata
        .as_ref()
        .and_then(|value| value.get("slot"))
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let market_session = if regular_hours_only {
        match us_equity_session {
            UsEquitySession::Regular => format!("us_equity_regular_open_slot_{slot}"),
            UsEquitySession::Closed => format!("us_equity_regular_closed_slot_{slot}"),
        }
    } else {
        format!("extended_allowed_slot_{slot}")
    };

    PythOraclePrice {
        feed_id: update.id,
        price_usd,
        confidence_usd,
        confidence_bps,
        publish_time: update.price.publish_time,
        age_ms,
        ema_price_usd,
        status,
        market_session,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UsEquitySession {
    Regular,
    Closed,
}

fn us_equity_session_at(now: DateTime<Utc>) -> UsEquitySession {
    if matches!(now.weekday(), Weekday::Sat | Weekday::Sun)
        || us_equity_market_holiday(now.date_naive())
    {
        return UsEquitySession::Closed;
    }
    let eastern_offset_hours = if us_eastern_dst_at(now) { -4 } else { -5 };
    let open_utc_minutes = 9 * 60 + 30 - eastern_offset_hours * 60;
    let close_utc_minutes = 16 * 60 - eastern_offset_hours * 60;
    let now_minutes = now.hour() as i32 * 60 + now.minute() as i32;
    if now_minutes >= open_utc_minutes && now_minutes < close_utc_minutes {
        UsEquitySession::Regular
    } else {
        UsEquitySession::Closed
    }
}

fn next_us_equity_regular_open_at(now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    for day_offset in 0..=10 {
        let date = now.date_naive() + ChronoDuration::days(day_offset);
        if matches!(date.weekday(), Weekday::Sat | Weekday::Sun) || us_equity_market_holiday(date) {
            continue;
        }
        let noon = Utc
            .with_ymd_and_hms(date.year(), date.month(), date.day(), 12, 0, 0)
            .single()?;
        let eastern_offset_hours = if us_eastern_dst_at(noon) { -4 } else { -5 };
        let open_utc_minutes = 9 * 60 + 30 - eastern_offset_hours * 60;
        let open = Utc
            .with_ymd_and_hms(
                date.year(),
                date.month(),
                date.day(),
                (open_utc_minutes / 60) as u32,
                (open_utc_minutes % 60) as u32,
                0,
            )
            .single()?;
        if open > now {
            return Some(open);
        }
    }
    None
}

fn us_equity_market_holiday(date: NaiveDate) -> bool {
    let year = date.year();
    for fixed_year in [year - 1, year, year + 1] {
        for (month, day) in [(1, 1), (6, 19), (7, 4), (12, 25)] {
            if observed_fixed_holiday_date(fixed_year, month, day) == Some(date) {
                return true;
            }
        }
    }

    nth_weekday_date(year, 1, Weekday::Mon, 3) == Some(date)
        || nth_weekday_date(year, 2, Weekday::Mon, 3) == Some(date)
        || last_weekday_date(year, 5, Weekday::Mon) == Some(date)
        || nth_weekday_date(year, 9, Weekday::Mon, 1) == Some(date)
        || nth_weekday_date(year, 11, Weekday::Thu, 4) == Some(date)
        || good_friday_date(year) == Some(date)
}

fn observed_fixed_holiday_date(year: i32, month: u32, day: u32) -> Option<NaiveDate> {
    let actual = NaiveDate::from_ymd_opt(year, month, day)?;
    match actual.weekday() {
        Weekday::Sat => actual.checked_sub_signed(ChronoDuration::days(1)),
        Weekday::Sun => actual.checked_add_signed(ChronoDuration::days(1)),
        _ => Some(actual),
    }
}

fn nth_weekday_date(year: i32, month: u32, weekday: Weekday, nth: u32) -> Option<NaiveDate> {
    NaiveDate::from_ymd_opt(
        year,
        month,
        nth_weekday_day_of_month(year, month, weekday, nth)?,
    )
}

fn last_weekday_date(year: i32, month: u32, weekday: Weekday) -> Option<NaiveDate> {
    let mut day = last_day_of_month(year, month)?;
    loop {
        let date = NaiveDate::from_ymd_opt(year, month, day)?;
        if date.weekday() == weekday {
            return Some(date);
        }
        day = day.checked_sub(1)?;
    }
}

fn last_day_of_month(year: i32, month: u32) -> Option<u32> {
    let next_month = if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)?
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1)?
    };
    next_month
        .checked_sub_signed(ChronoDuration::days(1))
        .map(|date| date.day())
}

fn good_friday_date(year: i32) -> Option<NaiveDate> {
    easter_sunday_date(year)?.checked_sub_signed(ChronoDuration::days(2))
}

fn easter_sunday_date(year: i32) -> Option<NaiveDate> {
    let a = year % 19;
    let b = year / 100;
    let c = year % 100;
    let d = b / 4;
    let e = b % 4;
    let f = (b + 8) / 25;
    let g = (b - f + 1) / 3;
    let h = (19 * a + b - d - g + 15) % 30;
    let i = c / 4;
    let k = c % 4;
    let l = (32 + 2 * e + 2 * i - h - k) % 7;
    let m = (a + 11 * h + 22 * l) / 451;
    let month = (h + l - 7 * m + 114) / 31;
    let day = ((h + l - 7 * m + 114) % 31) + 1;
    NaiveDate::from_ymd_opt(year, month as u32, day as u32)
}

fn us_eastern_dst_at(now: DateTime<Utc>) -> bool {
    let year = now.year();
    let Some(start_day) = nth_weekday_day_of_month(year, 3, Weekday::Sun, 2) else {
        return false;
    };
    let Some(end_day) = nth_weekday_day_of_month(year, 11, Weekday::Sun, 1) else {
        return false;
    };
    let Some(start) = Utc.with_ymd_and_hms(year, 3, start_day, 7, 0, 0).single() else {
        return false;
    };
    let Some(end) = Utc.with_ymd_and_hms(year, 11, end_day, 6, 0, 0).single() else {
        return false;
    };
    now >= start && now < end
}

fn nth_weekday_day_of_month(year: i32, month: u32, weekday: Weekday, nth: u32) -> Option<u32> {
    let first = NaiveDate::from_ymd_opt(year, month, 1)?;
    let first_weekday = first.weekday().num_days_from_monday() as i32;
    let target_weekday = weekday.num_days_from_monday() as i32;
    let offset = (target_weekday - first_weekday).rem_euclid(7) as u32;
    Some(1 + offset + 7 * nth.saturating_sub(1))
}

fn pyth_fixed_to_f64(value: &str, expo: i32) -> Option<f64> {
    let raw = value.parse::<f64>().ok()?;
    Some(raw * 10f64.powi(expo))
}

fn oracle_rejection_reason(oracle: &PythOraclePrice) -> String {
    match oracle.status {
        OracleStatus::MarketClosed => format!("market_closed ({})", oracle.market_session),
        OracleStatus::Stale => format!(
            "stale age={}s max_age_gate_exceeded ({})",
            oracle.age_ms / 1_000,
            oracle.market_session
        ),
        OracleStatus::WideConfidence => format!(
            "wide_confidence {:.2}bps ({})",
            oracle.confidence_bps, oracle.market_session
        ),
        other => format!("{} ({})", other.label(), oracle.market_session),
    }
}

fn quote_assets_from_config(values: &[String], sol_usd_estimate: f64) -> Vec<RwaQuoteAsset> {
    let mut assets = Vec::new();
    let mut seen = HashSet::new();
    for value in values {
        let normalized = value.trim();
        if normalized.is_empty() {
            continue;
        }
        let upper = normalized.to_ascii_uppercase();
        let asset = match upper.as_str() {
            "USDC" => RwaQuoteAsset {
                symbol: "USDC".to_string(),
                mint: USDC_MINT.to_string(),
                decimals: 6,
                usd_per_unit: 1.0,
            },
            "USDT" => RwaQuoteAsset {
                symbol: "USDT".to_string(),
                mint: USDT_MINT.to_string(),
                decimals: 6,
                usd_per_unit: 1.0,
            },
            "SOL" | "WSOL" => RwaQuoteAsset {
                symbol: "SOL/WSOL".to_string(),
                mint: WSOL_MINT.to_string(),
                decimals: 9,
                usd_per_unit: sol_usd_estimate.max(1.0),
            },
            _ if normalized == WSOL_MINT => RwaQuoteAsset {
                symbol: "SOL/WSOL".to_string(),
                mint: WSOL_MINT.to_string(),
                decimals: 9,
                usd_per_unit: sol_usd_estimate.max(1.0),
            },
            mint => RwaQuoteAsset {
                symbol: short_mint(mint),
                mint: mint.to_string(),
                decimals: 6,
                usd_per_unit: 1.0,
            },
        };
        if seen.insert(asset.mint.clone()) {
            assets.push(asset);
        }
    }
    assets
}

fn pyth_feed_ids_for_assets(assets: &[RwaAssetRuntime]) -> Vec<String> {
    let mut seen = HashSet::new();
    assets
        .iter()
        .filter_map(|runtime| {
            let id = runtime.asset.pyth_feed_id.trim().to_string();
            (!id.is_empty() && seen.insert(id.clone())).then_some(id)
        })
        .collect()
}

#[cfg(test)]
fn assets_with_valid_oracles(
    assets: &[RwaAssetRuntime],
    oracle_prices: &HashMap<String, PythOraclePrice>,
) -> Vec<RwaAssetRuntime> {
    assets
        .iter()
        .filter(|runtime| {
            oracle_prices
                .get(&runtime.asset.pyth_feed_id)
                .is_some_and(|price| price.status == OracleStatus::Valid)
        })
        .cloned()
        .collect()
}

fn assets_with_quote_oracles(
    assets: &[RwaAssetRuntime],
    oracle_prices: &HashMap<String, PythOraclePrice>,
) -> Vec<RwaAssetRuntime> {
    assets_with_quote_oracles_at(
        assets,
        oracle_prices,
        Utc::now(),
        rwa_preopen_quote_warmup_ms(),
    )
}

fn assets_with_quote_oracles_at(
    assets: &[RwaAssetRuntime],
    oracle_prices: &HashMap<String, PythOraclePrice>,
    now: DateTime<Utc>,
    warmup_ms: i64,
) -> Vec<RwaAssetRuntime> {
    assets
        .iter()
        .filter(|runtime| {
            oracle_prices
                .get(&runtime.asset.pyth_feed_id)
                .is_some_and(|price| quote_oracle_can_size_requests(price, now, warmup_ms))
        })
        .cloned()
        .collect()
}

fn quote_oracle_can_size_requests(
    price: &PythOraclePrice,
    now: DateTime<Utc>,
    warmup_ms: i64,
) -> bool {
    if price.status == OracleStatus::Valid {
        return true;
    }
    if price.status != OracleStatus::MarketClosed || warmup_ms <= 0 {
        return false;
    }
    next_us_equity_regular_open_at(now).is_some_and(|open| {
        let until_open_ms = open.signed_duration_since(now).num_milliseconds();
        until_open_ms >= 0 && until_open_ms <= warmup_ms
    })
}

fn rwa_preopen_quote_warmup_ms() -> i64 {
    env_value("RWA_PREOPEN_QUOTE_WARMUP_MS")
        .and_then(|value| value.parse::<i64>().ok())
        .map(|ms| ms.clamp(0, 6 * 60 * 60 * 1_000))
        .unwrap_or(30 * 60 * 1_000)
}

fn rwa_quote_status(
    assets: &[RwaAssetRuntime],
    quote_ready_assets: &[RwaAssetRuntime],
    oracle_prices: &HashMap<String, PythOraclePrice>,
    quote_request_count: usize,
    quote_count: usize,
) -> String {
    let watched = asset_tickers(assets);
    if assets.is_empty() {
        return "No RWA assets are enabled".to_string();
    }
    if quote_ready_assets.is_empty() {
        return append_rwa_jupiter_cooldown_status(format!(
            "Watching {watched}; route quotes paused until a pinned Pyth feed is valid or inside the pre-open quote warmup window"
        ));
    }

    let ready = asset_tickers(quote_ready_assets);
    let valid_ready = quote_ready_assets
        .iter()
        .filter(|runtime| {
            oracle_prices
                .get(&runtime.asset.pyth_feed_id)
                .is_some_and(|price| price.status == OracleStatus::Valid)
        })
        .cloned()
        .collect::<Vec<_>>();
    if valid_ready.is_empty() {
        return append_rwa_jupiter_cooldown_status(format!(
            "Pre-open quote warmup for {ready}; {quote_request_count} request(s), {quote_count} quote(s); trading remains blocked until Pyth is valid/open-session"
        ));
    }

    let status = if quote_ready_assets.len() == assets.len() {
        if valid_ready.len() == quote_ready_assets.len() {
            format!(
                "Quoting all watched assets ({ready}); {quote_request_count} request(s), {quote_count} quote(s)"
            )
        } else {
            let valid = asset_tickers(&valid_ready);
            format!(
                "Quoting open-session assets ({valid}) and warming pre-open assets ({ready}); {quote_request_count} request(s), {quote_count} quote(s)"
            )
        }
    } else {
        let ready_ids = quote_ready_assets
            .iter()
            .map(|runtime| runtime.asset.ticker.as_str())
            .collect::<HashSet<_>>();
        let paused = assets
            .iter()
            .filter_map(|runtime| {
                let ticker = runtime.asset.ticker.as_str();
                (!ready_ids.contains(ticker)).then_some(ticker)
            })
            .collect::<Vec<_>>()
            .join(", ");
        let valid = asset_tickers(&valid_ready);
        format!(
            "Quoting {valid}; warming/ready {ready}; paused {paused} until valid Pyth or pre-open warmup; {quote_request_count} request(s), {quote_count} quote(s)"
        )
    };
    append_rwa_jupiter_cooldown_status(status)
}

fn append_rwa_jupiter_cooldown_status(mut status: String) -> String {
    if let Some(remaining_ms) = rwa_jupiter_rate_limit_cooldown_remaining_ms() {
        status.push_str(&format!(
            "; provider cooling down for {remaining_ms}ms after Jupiter rate limit"
        ));
    }
    status
}

fn asset_tickers(assets: &[RwaAssetRuntime]) -> String {
    let tickers = assets
        .iter()
        .map(|runtime| runtime.asset.ticker.as_str())
        .filter(|ticker| !ticker.is_empty())
        .collect::<Vec<_>>();
    if tickers.is_empty() {
        "--".to_string()
    } else {
        tickers.join(", ")
    }
}

fn configured_issuers(asset: &RwaAssetConfig) -> Vec<ConfiguredIssuer> {
    let mut issuers = vec![
        ConfiguredIssuer {
            symbol: asset
                .xstock_symbol
                .clone()
                .if_empty_then(|| format!("{}x", asset.ticker)),
            mint: asset.xstock_mint.clone(),
            decimals: asset.xstock_decimals,
            shares_per_token: 1.0,
        },
        ConfiguredIssuer {
            symbol: asset
                .ondo_symbol
                .clone()
                .if_empty_then(|| format!("{}on", asset.ticker)),
            mint: asset.ondo_mint.clone(),
            decimals: asset.ondo_decimals,
            shares_per_token: asset.ondo_shares_per_token,
        },
    ];
    issuers.retain(|issuer| {
        !is_blocklisted_remora_symbol(&issuer.symbol) && !is_blocklisted_remora_mint(&issuer.mint)
    });
    issuers
}

fn build_quote_plans<'a>(
    config: &RwaConfig,
    assets: &'a [RwaAssetRuntime],
    oracle_prices: &HashMap<String, PythOraclePrice>,
    trade_ladder_value: Option<&str>,
) -> Vec<RwaQuotePlan<'a>> {
    struct PlanSeed<'a> {
        asset: &'a RwaAssetConfig,
        price: f64,
        issuers: Vec<ConfiguredIssuer>,
        trade_sizes: Vec<f64>,
    }

    let mut seeds = Vec::new();
    let mut max_ladder_len = 0usize;
    for runtime in assets {
        let asset = &runtime.asset;
        let oracle = oracle_prices.get(&asset.pyth_feed_id);
        let price = oracle
            .filter(|oracle| oracle.price_usd > 0.0 && oracle.price_usd.is_finite())
            .map(|oracle| oracle.price_usd)
            .unwrap_or(1.0);
        let trade_sizes = rwa_trade_size_ladder_from_value(config, asset, trade_ladder_value);
        if trade_sizes.is_empty() {
            continue;
        }
        max_ladder_len = max_ladder_len.max(trade_sizes.len());
        seeds.push(PlanSeed {
            asset,
            price,
            issuers: configured_issuers(asset),
            trade_sizes,
        });
    }

    let mut plans = Vec::new();
    for size_index in 0..max_ladder_len {
        for seed in &seeds {
            let Some(trade_usd) = seed.trade_sizes.get(size_index).copied() else {
                continue;
            };
            let issuer_atoms = seed
                .issuers
                .iter()
                .map(|issuer| {
                    (
                        issuer.mint.clone(),
                        ui_to_atoms(
                            trade_usd / (seed.price * issuer.shares_per_token.max(f64::EPSILON)),
                            issuer.decimals,
                        ),
                    )
                })
                .collect::<HashMap<_, _>>();
            plans.push(RwaQuotePlan {
                asset: seed.asset,
                trade_usd,
                issuers: seed.issuers.clone(),
                issuer_atoms,
            });
        }
    }
    plans
}

fn rwa_trade_size_ladder_from_value(
    config: &RwaConfig,
    asset: &RwaAssetConfig,
    value: Option<&str>,
) -> Vec<f64> {
    let max_trade_usd = asset
        .max_trade_usd
        .unwrap_or(config.max_trade_usd_per_asset)
        .max(0.0);
    if max_trade_usd <= 0.0 || !max_trade_usd.is_finite() {
        return Vec::new();
    }

    let mut sizes = value
        .map(|value| {
            value
                .split(',')
                .filter_map(|part| part.trim().parse::<f64>().ok())
                .filter(|size| size.is_finite() && *size > 0.0)
                .filter(|size| *size <= max_trade_usd)
                .collect::<Vec<_>>()
        })
        .filter(|sizes| !sizes.is_empty())
        .unwrap_or_else(|| vec![max_trade_usd]);

    sizes.push(max_trade_usd);
    sizes.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    sizes.dedup_by(|a, b| (*a - *b).abs() < 0.000001);
    sizes
}

#[cfg(test)]
fn build_quote_requests(
    config: &RwaConfig,
    assets: &[RwaAssetRuntime],
    quote_assets: &[RwaQuoteAsset],
    oracle_prices: &HashMap<String, PythOraclePrice>,
) -> Vec<AggregatorQuoteRequest> {
    build_quote_requests_for_balances(config, assets, quote_assets, oracle_prices, None)
}

fn build_quote_requests_for_balances(
    config: &RwaConfig,
    assets: &[RwaAssetRuntime],
    quote_assets: &[RwaQuoteAsset],
    oracle_prices: &HashMap<String, PythOraclePrice>,
    balances: Option<&HashMap<String, u128>>,
) -> Vec<AggregatorQuoteRequest> {
    let ladder = env_value("RWA_TRADE_SIZE_USD_LADDER");
    let plans = build_quote_plans(config, assets, oracle_prices, ladder.as_deref());
    let wsol_usd_per_unit = quote_assets
        .iter()
        .find(|asset| asset.mint == WSOL_MINT)
        .map(|asset| asset.usd_per_unit);

    let mut requests = Vec::new();
    let mut direct_requests_pushed = false;
    for quote_asset in quote_assets {
        for priority in [0, 1, 2] {
            for plan in &plans {
                let mut issuers = plan.issuers.iter().collect::<Vec<_>>();
                issuers.sort_by_key(|issuer| issuer_quote_priority(plan.asset, issuer));
                let quote_atoms = quote_asset.input_atoms_for_usd(plan.trade_usd);
                if !quote_plan_fundable(quote_asset, quote_atoms, balances, wsol_usd_per_unit) {
                    continue;
                }
                for issuer in issuers
                    .into_iter()
                    .filter(|issuer| issuer_quote_priority(plan.asset, issuer) == priority)
                {
                    push_stable_quote_requests(
                        &mut requests,
                        issuer,
                        quote_asset,
                        quote_atoms,
                        plan.issuer_atoms.get(&issuer.mint).copied().unwrap_or(0),
                    );
                }
            }
            if priority == 0 && !direct_requests_pushed {
                for plan in &plans {
                    if !plan_has_fundable_quote_asset(
                        plan,
                        quote_assets,
                        balances,
                        wsol_usd_per_unit,
                    ) {
                        continue;
                    }
                    push_direct_issuer_quote_requests(
                        &mut requests,
                        &plan.issuers,
                        &plan.issuer_atoms,
                    );
                }
                direct_requests_pushed = true;
            }
        }
    }
    requests
}

fn quote_plan_fundable(
    quote_asset: &RwaQuoteAsset,
    quote_atoms: u128,
    balances: Option<&HashMap<String, u128>>,
    wsol_usd_per_unit: Option<f64>,
) -> bool {
    let Some(balances) = balances else {
        return true;
    };
    if balances.get(&quote_asset.mint).copied().unwrap_or_default() >= quote_atoms {
        return true;
    }
    if !rwa_wsol_stable_funding_enabled() || !is_supported_stable_quote_mint(&quote_asset.mint) {
        return false;
    }
    let Some(wsol_usd_per_unit) =
        wsol_usd_per_unit.filter(|value| value.is_finite() && *value > 0.0)
    else {
        return false;
    };
    let required_usd = quote_asset.input_usd_for_atoms(quote_atoms);
    let required_wsol_atoms = ui_to_atoms(required_usd / wsol_usd_per_unit, 9).saturating_add(1);
    balances.get(WSOL_MINT).copied().unwrap_or_default() >= required_wsol_atoms
}

fn plan_has_fundable_quote_asset(
    plan: &RwaQuotePlan<'_>,
    quote_assets: &[RwaQuoteAsset],
    balances: Option<&HashMap<String, u128>>,
    wsol_usd_per_unit: Option<f64>,
) -> bool {
    quote_assets.iter().any(|quote_asset| {
        quote_plan_fundable(
            quote_asset,
            quote_asset.input_atoms_for_usd(plan.trade_usd),
            balances,
            wsol_usd_per_unit,
        )
    })
}

fn rwa_wsol_stable_funding_enabled() -> bool {
    env_bool("RWA_SOL_STABLE_FUNDING_ENABLED").unwrap_or(true)
}

fn rwa_pyth_discount_buy_enabled(config: &RwaConfig) -> bool {
    env_bool("RWA_PYTH_DISCOUNT_BUY_ENABLED").unwrap_or({
        config.directional_enabled && matches!(config.mode, RwaMode::Directional | RwaMode::Both)
    })
}

fn rwa_pyth_discount_buy_min_edge_bps(config: &RwaConfig) -> f64 {
    env_value("RWA_PYTH_DISCOUNT_BUY_MIN_EDGE_BPS")
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0)
        .unwrap_or(100.0)
        .max(config.min_net_profit_bps)
}

fn is_supported_stable_quote_mint(mint: &str) -> bool {
    matches!(mint, USDC_MINT | USDT_MINT)
}

#[cfg(test)]
fn filter_taker_fundable_quote_requests(
    requests: &[AggregatorQuoteRequest],
    balances: Option<&HashMap<String, u128>>,
) -> Vec<AggregatorQuoteRequest> {
    partition_taker_fundable_quote_requests(requests, balances).0
}

fn partition_taker_fundable_quote_requests(
    requests: &[AggregatorQuoteRequest],
    balances: Option<&HashMap<String, u128>>,
) -> (Vec<AggregatorQuoteRequest>, Vec<AggregatorQuoteRequest>) {
    let Some(balances) = balances else {
        return (requests.to_vec(), Vec::new());
    };
    let mut fundable = Vec::new();
    let mut quote_only = Vec::new();
    for request in requests {
        let input_mint = input_mint_for_side(&request.pair, request.side);
        let fundable_from_balance =
            balances.get(input_mint).copied().unwrap_or_default() >= request.in_amount_atoms;
        if fundable_from_balance {
            fundable.push(request.clone());
        } else {
            quote_only.push(request.clone());
        }
    }
    (fundable, quote_only)
}

fn filter_primary_route_quote_requests(
    requests: &[AggregatorQuoteRequest],
    assets: &[RwaAssetRuntime],
) -> Vec<AggregatorQuoteRequest> {
    let mut primary_mints = HashSet::new();
    let mut issuer_mints = HashSet::new();
    for runtime in assets {
        let xstock_mint = runtime.asset.xstock_mint.trim();
        if !xstock_mint.is_empty() {
            issuer_mints.insert(xstock_mint);
        }
        let ondo_mint = runtime.asset.ondo_mint.trim();
        if !ondo_mint.is_empty() {
            primary_mints.insert(ondo_mint);
            issuer_mints.insert(ondo_mint);
        }
    }
    if primary_mints.is_empty() {
        return Vec::new();
    }
    requests
        .iter()
        .filter(|request| {
            primary_mints.contains(request.pair.base_mint.as_str())
                && !issuer_mints.contains(request.pair.quote_mint.as_str())
        })
        .cloned()
        .collect()
}

fn retain_jupiter_v2_min_order_requests(
    requests: &mut Vec<AggregatorQuoteRequest>,
    quote_assets: &[RwaQuoteAsset],
    assets: &[RwaAssetRuntime],
    oracle_prices: &HashMap<String, PythOraclePrice>,
) {
    let min_order_usd = rwa_jupiter_v2_min_order_usd();
    if min_order_usd <= 0.0 {
        return;
    }
    requests.retain(|request| {
        request_input_usd(request, quote_assets, assets, oracle_prices)
            .is_none_or(|input_usd| input_usd >= min_order_usd)
    });
}

fn request_input_usd(
    request: &AggregatorQuoteRequest,
    quote_assets: &[RwaQuoteAsset],
    assets: &[RwaAssetRuntime],
    oracle_prices: &HashMap<String, PythOraclePrice>,
) -> Option<f64> {
    let input_mint = input_mint_for_side(&request.pair, request.side);
    let input_decimals = input_decimals_for_side(&request.pair, request.side);
    if let Some(quote_asset) = quote_assets.iter().find(|asset| asset.mint == input_mint) {
        return Some(
            atoms_to_ui(request.in_amount_atoms, input_decimals) * quote_asset.usd_per_unit,
        );
    }

    assets.iter().find_map(|runtime| {
        let asset = &runtime.asset;
        let shares_per_token = if input_mint == asset.xstock_mint {
            Some(1.0)
        } else if input_mint == asset.ondo_mint {
            Some(asset.ondo_shares_per_token)
        } else {
            None
        }?;
        let oracle = oracle_prices.get(&asset.pyth_feed_id)?;
        (oracle.price_usd.is_finite() && oracle.price_usd > 0.0).then(|| {
            atoms_to_ui(request.in_amount_atoms, input_decimals)
                * shares_per_token
                * oracle.price_usd
        })
    })
}

fn build_primary_rfq_probe_quote_requests(
    config: &RwaConfig,
    assets: &[RwaAssetRuntime],
    quote_assets: &[RwaQuoteAsset],
    oracle_prices: &HashMap<String, PythOraclePrice>,
) -> Vec<AggregatorQuoteRequest> {
    let requests =
        build_quote_requests_for_balances(config, assets, quote_assets, oracle_prices, None);
    filter_primary_route_quote_requests(&requests, assets)
}

#[cfg(test)]
fn cap_quote_requests(requests: &mut Vec<AggregatorQuoteRequest>, max_requests: Option<usize>) {
    cap_quote_requests_with_offset(requests, max_requests, 0);
}

fn cap_quote_requests_for_cycle(
    requests: &mut Vec<AggregatorQuoteRequest>,
    max_requests: Option<usize>,
    cycle: u64,
    salt: usize,
) {
    *requests = retain_rwa_jupiter_tradable_quote_requests(std::mem::take(requests));
    let offset = (cycle as usize).wrapping_mul(salt.saturating_add(1));
    cap_quote_requests_with_offset(requests, max_requests, offset);
}

fn cap_quote_requests_with_offset(
    requests: &mut Vec<AggregatorQuoteRequest>,
    max_requests: Option<usize>,
    offset: usize,
) {
    if let Some(max_requests) = max_requests {
        if requests.len() <= max_requests {
            return;
        }
        if max_requests == 0 {
            requests.clear();
            return;
        }

        let original = std::mem::take(requests);
        let len = original.len();
        let mut selected = Vec::with_capacity(max_requests);
        let offset = offset % len;
        for index in 0..max_requests {
            let original_index = (offset + (index * len / max_requests)) % len;
            selected.push(original[original_index].clone());
        }
        *requests = selected;
    }
}

fn quote_request_sample(requests: &[AggregatorQuoteRequest]) -> String {
    requests
        .iter()
        .take(6)
        .map(|request| {
            format!(
                "{}>{}:{}",
                short_mint(input_mint_for_side(&request.pair, request.side)),
                short_mint(output_mint_for_side(&request.pair, request.side)),
                request.in_amount_atoms
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn requests_not_in(
    requests: &[AggregatorQuoteRequest],
    excluded: &[AggregatorQuoteRequest],
) -> Vec<AggregatorQuoteRequest> {
    requests
        .iter()
        .filter(|request| {
            !excluded
                .iter()
                .any(|excluded| quote_request_eq(request, excluded))
        })
        .cloned()
        .collect()
}

fn dedupe_quote_requests(requests: &mut Vec<AggregatorQuoteRequest>) {
    let mut seen = HashSet::new();
    requests.retain(|request| {
        seen.insert(format!(
            "{}:{}:{:?}:{}",
            request.pair.base_mint, request.pair.quote_mint, request.side, request.in_amount_atoms
        ))
    });
}

fn missing_quote_requests(
    requests: &[AggregatorQuoteRequest],
    quotes: &[Quote],
) -> Vec<AggregatorQuoteRequest> {
    requests
        .iter()
        .filter(|request| {
            !quotes
                .iter()
                .any(|quote| quote_satisfies_request(quote, request))
        })
        .cloned()
        .collect()
}

fn quote_request_eq(left: &AggregatorQuoteRequest, right: &AggregatorQuoteRequest) -> bool {
    left.side == right.side
        && left.pair.base_mint == right.pair.base_mint
        && left.pair.quote_mint == right.pair.quote_mint
        && left.in_amount_atoms == right.in_amount_atoms
}

fn quote_satisfies_request(quote: &Quote, request: &AggregatorQuoteRequest) -> bool {
    quote.side == request.side
        && quote.pair.base_mint == request.pair.base_mint
        && quote.pair.quote_mint == request.pair.quote_mint
        && quote.in_amount_atoms == request.in_amount_atoms
}

fn push_stable_quote_requests(
    requests: &mut Vec<AggregatorQuoteRequest>,
    issuer: &ConfiguredIssuer,
    quote_asset: &RwaQuoteAsset,
    quote_atoms: u128,
    issuer_atoms: u128,
) {
    requests.push(AggregatorQuoteRequest {
        pair: TokenPair {
            base_mint: issuer.mint.clone(),
            quote_mint: quote_asset.mint.clone(),
            base_decimals: issuer.decimals,
            quote_decimals: quote_asset.decimals,
        },
        side: SwapSide::QuoteToBase,
        in_amount_atoms: quote_atoms,
    });
    requests.push(AggregatorQuoteRequest {
        pair: TokenPair {
            base_mint: issuer.mint.clone(),
            quote_mint: quote_asset.mint.clone(),
            base_decimals: issuer.decimals,
            quote_decimals: quote_asset.decimals,
        },
        side: SwapSide::BaseToQuote,
        in_amount_atoms: issuer_atoms,
    });
}

fn push_direct_issuer_quote_requests(
    requests: &mut Vec<AggregatorQuoteRequest>,
    issuers: &[ConfiguredIssuer],
    issuer_atoms: &HashMap<String, u128>,
) {
    for from in issuers {
        for to in issuers {
            if from.mint == to.mint {
                continue;
            }
            requests.push(AggregatorQuoteRequest {
                pair: TokenPair {
                    base_mint: from.mint.clone(),
                    quote_mint: to.mint.clone(),
                    base_decimals: from.decimals,
                    quote_decimals: to.decimals,
                },
                side: SwapSide::BaseToQuote,
                in_amount_atoms: issuer_atoms.get(&from.mint).copied().unwrap_or(0),
            });
        }
    }
}

fn issuer_quote_priority(asset: &RwaAssetConfig, issuer: &ConfiguredIssuer) -> u8 {
    if issuer.mint == asset.xstock_mint {
        0
    } else if issuer.mint == asset.ondo_mint {
        1
    } else {
        2
    }
}

async fn fetch_scoped_jupiter_quotes(
    requests: Vec<AggregatorQuoteRequest>,
    network_fee: &FeeBreakdown,
    safety_buffer_bps: f64,
) -> Vec<Quote> {
    let requests = retain_rwa_jupiter_tradable_quote_requests(requests);
    let request_count = requests.len();
    if requests.is_empty() {
        return Vec::new();
    }

    let client = Client::new();
    let quote_url = jupiter_quote_url();
    let api_key = env_value("JUPITER_API_KEY");
    let mut quotes = Vec::new();
    let max_in_flight = rwa_jupiter_max_in_flight();
    let batch_delay = std::time::Duration::from_millis(rwa_jupiter_batch_delay_ms());
    let budget_ms = rwa_jupiter_quote_budget_ms();
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(budget_ms);
    let mut batches = requests.chunks(max_in_flight).peekable();
    while let Some(batch) = batches.next() {
        if rwa_jupiter_rate_limited() {
            break;
        }
        if rwa_jupiter_quote_budget_remaining(deadline).is_none() {
            warn!(
                budget_ms,
                "scoped RWA Jupiter quote budget exhausted; continuing with partial quotes"
            );
            break;
        }
        let mut futures = FuturesUnordered::new();
        for request in batch.iter().cloned() {
            let client = client.clone();
            let quote_url = quote_url.clone();
            let api_key = api_key.clone();
            let network_fee = network_fee.clone();
            futures.push(async move {
                let _permit = acquire_rwa_jupiter_permit().await;
                let request_for_quote = request.clone();
                let result = scoped_jupiter_quote(
                    &client,
                    &quote_url,
                    api_key.as_deref(),
                    request_for_quote,
                    network_fee,
                    safety_buffer_bps,
                )
                .await;
                (request, result)
            });
        }

        while !futures.is_empty() {
            let Some(remaining) = rwa_jupiter_quote_budget_remaining(deadline) else {
                warn!(
                    budget_ms,
                    pending = futures.len(),
                    "scoped RWA Jupiter quote budget exhausted; continuing with partial quotes"
                );
                break;
            };
            let Some(result) = (match tokio::time::timeout(remaining, futures.next()).await {
                Ok(result) => result,
                Err(_) => {
                    warn!(
                        budget_ms,
                        pending = futures.len(),
                        "scoped RWA Jupiter quote budget exhausted; continuing with partial quotes"
                    );
                    break;
                }
            }) else {
                break;
            };
            let (request, result) = result;
            match result {
                Ok(Some(quote)) => quotes.push(quote),
                Ok(None) => {}
                Err(error) => {
                    record_rwa_jupiter_untradable_token_cooldown(&request, &error.to_string());
                    warn!(?error, "scoped RWA Jupiter quote failed");
                }
            }
        }
        if batches.peek().is_some() && !batch_delay.is_zero() {
            if let Some(remaining) = rwa_jupiter_quote_budget_remaining(deadline) {
                tokio::time::sleep(batch_delay.min(remaining)).await;
            }
        }
    }
    info!(
        request_count,
        quote_count = quotes.len(),
        sample = quote_request_sample(&requests),
        "scoped RWA Jupiter v1 quote batch complete"
    );
    quotes
}

async fn fetch_scoped_jupiter_v2_order_quotes(
    requests: Vec<AggregatorQuoteRequest>,
    network_fee: &FeeBreakdown,
    safety_buffer_bps: f64,
    taker_pubkey: Option<String>,
) -> Vec<Quote> {
    fetch_scoped_jupiter_v2_order_quotes_with_router_excludes(
        requests,
        network_fee,
        safety_buffer_bps,
        taker_pubkey,
        None,
    )
    .await
}

async fn fetch_scoped_jupiter_v2_order_quotes_with_router_excludes(
    requests: Vec<AggregatorQuoteRequest>,
    network_fee: &FeeBreakdown,
    safety_buffer_bps: f64,
    taker_pubkey: Option<String>,
    exclude_routers: Option<String>,
) -> Vec<Quote> {
    let requests = retain_rwa_jupiter_tradable_quote_requests(requests);
    let request_count = requests.len();
    if requests.is_empty() {
        return Vec::new();
    }

    let client = Client::new();
    let quote_url = jupiter_v2_order_url();
    let api_key = env_value("JUPITER_API_KEY");
    if api_key.is_none() {
        return Vec::new();
    }

    let mut quotes = Vec::new();
    let max_in_flight = rwa_jupiter_max_in_flight();
    let batch_delay = std::time::Duration::from_millis(rwa_jupiter_batch_delay_ms());
    let budget_ms = rwa_jupiter_quote_budget_ms();
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(budget_ms);
    let mut batches = requests.chunks(max_in_flight).peekable();
    while let Some(batch) = batches.next() {
        if rwa_jupiter_rate_limited() {
            break;
        }
        if rwa_jupiter_quote_budget_remaining(deadline).is_none() {
            warn!(
                budget_ms,
                "scoped RWA Jupiter v2 order quote budget exhausted; continuing with partial quotes"
            );
            break;
        }
        let mut futures = FuturesUnordered::new();
        for request in batch.iter().cloned() {
            let client = client.clone();
            let quote_url = quote_url.clone();
            let api_key = api_key.clone();
            let network_fee = network_fee.clone();
            let taker_pubkey = taker_pubkey.clone();
            let exclude_routers = exclude_routers.clone();
            futures.push(async move {
                let _permit = acquire_rwa_jupiter_permit().await;
                let request_for_quote = request.clone();
                let result = scoped_jupiter_v2_order_quote(
                    &client,
                    &quote_url,
                    api_key.as_deref(),
                    request_for_quote,
                    network_fee,
                    safety_buffer_bps,
                    taker_pubkey.as_deref(),
                    exclude_routers.as_deref(),
                )
                .await;
                (request, result)
            });
        }

        while !futures.is_empty() {
            let Some(remaining) = rwa_jupiter_quote_budget_remaining(deadline) else {
                warn!(
                    budget_ms,
                    pending = futures.len(),
                    "scoped RWA Jupiter v2 order quote budget exhausted; continuing with partial quotes"
                );
                break;
            };
            let Some(result) = (match tokio::time::timeout(remaining, futures.next()).await {
                Ok(result) => result,
                Err(_) => {
                    warn!(
                        budget_ms,
                        pending = futures.len(),
                        "scoped RWA Jupiter v2 order quote budget exhausted; continuing with partial quotes"
                    );
                    break;
                }
            }) else {
                break;
            };
            let (request, result) = result;
            match result {
                Ok(Some(quote)) => quotes.push(quote),
                Ok(None) => {}
                Err(error) => {
                    record_rwa_jupiter_untradable_token_cooldown(&request, &error.to_string());
                    warn!(?error, "scoped RWA Jupiter v2 order quote failed");
                }
            }
        }
        if batches.peek().is_some() && !batch_delay.is_zero() {
            if let Some(remaining) = rwa_jupiter_quote_budget_remaining(deadline) {
                tokio::time::sleep(batch_delay.min(remaining)).await;
            }
        }
    }
    info!(
        request_count,
        quote_count = quotes.len(),
        sample = quote_request_sample(&requests),
        router_excludes = exclude_routers.as_deref().unwrap_or(""),
        taker = taker_pubkey.is_some(),
        "scoped RWA Jupiter v2 order quote batch complete"
    );
    quotes
}

async fn scoped_jupiter_v2_order_quote(
    client: &Client,
    quote_url: &str,
    api_key: Option<&str>,
    request: AggregatorQuoteRequest,
    network_fee: FeeBreakdown,
    safety_buffer_bps: f64,
    taker_pubkey: Option<&str>,
    exclude_routers: Option<&str>,
) -> Result<Option<Quote>> {
    if request.in_amount_atoms == 0 {
        return Ok(None);
    }

    let input_mint = input_mint_for_side(&request.pair, request.side);
    let output_mint = output_mint_for_side(&request.pair, request.side);
    let params = jupiter_v2_order_params(
        input_mint,
        output_mint,
        request.in_amount_atoms,
        taker_pubkey,
        jupiter_v2_auto_slippage(),
        exclude_routers,
    );
    let mut http_request = client
        .get(quote_url)
        .query(&params)
        .timeout(std::time::Duration::from_millis(rwa_jupiter_v2_timeout_ms()));
    if let Some(api_key) = api_key {
        http_request = http_request.header("x-api-key", api_key);
    }

    let response = match http_request.send().await {
        Ok(response) => response,
        Err(error) if error.is_timeout() => return Ok(None),
        Err(error) => {
            return Err(error).context("scoped RWA Jupiter v2 order request failed");
        }
    };
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        if status.as_u16() == 429 || is_jupiter_rate_limit_text(&body) {
            start_rwa_jupiter_rate_limit_cooldown();
            return Ok(None);
        }
        if is_jupiter_no_route_text(&body) {
            return Ok(None);
        }
        return Err(anyhow!("scoped RWA Jupiter v2 order http {status}: {body}"));
    }
    let value = response
        .json::<Value>()
        .await
        .context("invalid scoped RWA Jupiter v2 order json")?;
    if value.get("error").is_some() || value.get("code").is_some() {
        let error_text = value
            .get("error")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        if is_jupiter_rate_limit_text(error_text) {
            start_rwa_jupiter_rate_limit_cooldown();
            return Ok(None);
        }
        if is_jupiter_no_route_text(error_text) {
            return Ok(None);
        }
        if error_text
            .to_ascii_lowercase()
            .contains("insufficient funds")
        {
            return Ok(None);
        }
        if !error_text.is_empty() {
            return Err(anyhow!("scoped RWA Jupiter v2 order error: {error_text}"));
        }
    }
    let mut quote = quote_from_jupiter_json(&request, &value, &network_fee, safety_buffer_bps);
    if let Some(quote) = quote.as_mut() {
        quote.pool = scoped_jupiter_v2_route_label(&value);
    }
    Ok(quote)
}

async fn scoped_jupiter_quote(
    client: &Client,
    quote_url: &str,
    api_key: Option<&str>,
    request: AggregatorQuoteRequest,
    network_fee: FeeBreakdown,
    safety_buffer_bps: f64,
) -> Result<Option<Quote>> {
    if request.in_amount_atoms == 0 {
        return Ok(None);
    }

    let input_mint = input_mint_for_side(&request.pair, request.side);
    let output_mint = output_mint_for_side(&request.pair, request.side);
    let params = vec![
        ("inputMint".to_string(), input_mint.to_string()),
        ("outputMint".to_string(), output_mint.to_string()),
        ("amount".to_string(), request.in_amount_atoms.to_string()),
        ("slippageBps".to_string(), "50".to_string()),
        ("onlyDirectRoutes".to_string(), "false".to_string()),
        ("asLegacyTransaction".to_string(), "false".to_string()),
        ("swapMode".to_string(), "ExactIn".to_string()),
    ];
    let mut http_request = client
        .get(quote_url)
        .query(&params)
        .timeout(std::time::Duration::from_millis(1_200));
    if let Some(api_key) = api_key {
        http_request = http_request.header("x-api-key", api_key);
    }

    let response = http_request
        .send()
        .await
        .context("scoped RWA Jupiter quote request failed")?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        if status.as_u16() == 429 || is_jupiter_rate_limit_text(&body) {
            start_rwa_jupiter_rate_limit_cooldown();
            return Ok(None);
        }
        if is_jupiter_no_route_text(&body) {
            return Ok(None);
        }
        return Err(anyhow!("scoped RWA Jupiter quote http {status}: {body}"));
    }
    let value = response
        .json::<Value>()
        .await
        .context("invalid scoped RWA Jupiter quote json")?;
    let quote = quote_from_jupiter_json(&request, &value, &network_fee, safety_buffer_bps);
    if quote.is_none() {
        let error_text = value
            .get("error")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        if is_jupiter_rate_limit_text(error_text) {
            start_rwa_jupiter_rate_limit_cooldown();
            return Ok(None);
        }
        if is_jupiter_no_route_text(error_text) {
            return Ok(None);
        }
        if !error_text.is_empty() {
            return Err(anyhow!("scoped RWA Jupiter quote error: {error_text}"));
        }
        return Err(anyhow!(
            "scoped RWA Jupiter quote missing outAmount: {value}"
        ));
    }
    Ok(quote)
}

fn quote_from_jupiter_json(
    request: &AggregatorQuoteRequest,
    value: &Value,
    network_fee: &FeeBreakdown,
    safety_buffer_bps: f64,
) -> Option<Quote> {
    let out_amount_atoms = json_u128(value, &["/outAmount"])?;
    if out_amount_atoms == 0 {
        return None;
    }
    let in_amount_atoms = json_u128(value, &["/inAmount"]).unwrap_or(request.in_amount_atoms);
    let input_ui = atoms_to_ui(
        in_amount_atoms,
        input_decimals_for_side(&request.pair, request.side),
    );
    let output_ui = atoms_to_ui(
        out_amount_atoms,
        output_decimals_for_side(&request.pair, request.side),
    );
    let (quote_ui, base_ui) = match request.side {
        SwapSide::QuoteToBase => (input_ui, output_ui),
        SwapSide::BaseToQuote => (output_ui, input_ui),
    };
    if quote_ui == 0.0 || base_ui == 0.0 {
        return None;
    }

    let route_fee_atoms = json_u128(value, &["/routePlan/0/swapInfo/feeAmount"]).unwrap_or(0);
    let slippage_bps = json_f64(value, &["/priceImpactPct"])
        .map(|value| {
            let value = value.abs();
            if value <= 1.0 {
                value * 10_000.0
            } else {
                value * 100.0
            }
        })
        .unwrap_or(0.0);
    let mut fee = FeeBreakdown {
        host_fee_atoms: route_fee_atoms,
        slippage_bps,
        safety_buffer_bps,
        ..FeeBreakdown::default()
    };
    fee.merge_network(network_fee);

    Some(Quote {
        venue: Venue::Jupiter,
        pool: scoped_jupiter_route_label(value),
        pair: request.pair.clone(),
        side: request.side,
        in_amount_atoms,
        out_amount_atoms,
        effective_price: quote_ui / base_ui,
        fee,
        slot: json_u64(value, &["/contextSlot"]).unwrap_or(0),
    })
}

fn scoped_jupiter_route_label(value: &Value) -> String {
    let label = json_str(value, &["/routePlan/0/swapInfo/label"]).unwrap_or("jupiter");
    let pool = json_str(value, &["/routePlan/0/swapInfo/ammKey"]).unwrap_or("unknown");
    format!("jupiter-route:{label}:{pool}")
}

fn scoped_jupiter_v2_route_label(value: &Value) -> String {
    let router = json_str(value, &["/router"]).unwrap_or("unknown");
    let swap_type = json_str(value, &["/swapType"])
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| {
            if router.eq_ignore_ascii_case("jupiterz")
                || value.get("quoteId").is_some()
                || value.get("maker").is_some()
            {
                "rfq".to_string()
            } else {
                "aggregator".to_string()
            }
        });
    let labels = value
        .get("routePlan")
        .and_then(Value::as_array)
        .map(|routes| {
            routes
                .iter()
                .filter_map(|route| route.pointer("/swapInfo/label").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("+")
        })
        .filter(|labels| !labels.is_empty())
        .unwrap_or_else(|| "unknown".to_string());
    format!("jupiter-v2-order:{swap_type}:{router}:{labels}")
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

fn jupiter_v2_order_url() -> String {
    env_value("JUPITER_V2_ORDER_URL")
        .unwrap_or_else(|| "https://api.jup.ag/swap/v2/order".to_string())
}

fn jupiter_v2_order_params(
    input_mint: &str,
    output_mint: &str,
    amount_atoms: u128,
    taker_pubkey: Option<&str>,
    auto_slippage: bool,
    exclude_routers: Option<&str>,
) -> Vec<(String, String)> {
    let mut params = vec![
        ("inputMint".to_string(), input_mint.to_string()),
        ("outputMint".to_string(), output_mint.to_string()),
        ("amount".to_string(), amount_atoms.to_string()),
        ("swapMode".to_string(), "ExactIn".to_string()),
    ];
    if let Some(taker) = taker_pubkey.filter(|value| !value.trim().is_empty()) {
        params.push(("taker".to_string(), taker.to_string()));
    }
    if !auto_slippage {
        params.push(("slippageBps".to_string(), "50".to_string()));
    }
    if let Some(exclude_routers) = exclude_routers.filter(|value| !value.trim().is_empty()) {
        params.push(("excludeRouters".to_string(), exclude_routers.to_string()));
    }
    params
}

fn rwa_jupiter_v2_enabled() -> bool {
    env_bool("RWA_JUPITER_V2_ENABLED").unwrap_or(true)
}

fn jupiter_v2_auto_slippage() -> bool {
    env_bool("JUPITER_V2_AUTO_SLIPPAGE").unwrap_or(true)
}

fn rwa_jupiter_v2_quote_with_taker() -> bool {
    env_bool("RWA_JUPITER_V2_QUOTE_WITH_TAKER").unwrap_or(true)
}

fn rwa_jupiter_v2_rfq_only_enabled() -> bool {
    env_bool("RWA_JUPITER_V2_RFQ_ONLY_ENABLED").unwrap_or(true)
}

fn rwa_aggregator_quote_only_enabled() -> bool {
    env_bool("RWA_AGGREGATOR_QUOTE_ONLY_ENABLED").unwrap_or(false)
}

fn rwa_jupiter_v2_quote_only_max_requests() -> Option<usize> {
    Some(
        env_value("RWA_JUPITER_V2_QUOTE_ONLY_MAX_REQUESTS")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(18),
    )
}

fn rwa_jupiter_v2_taker_max_requests() -> Option<usize> {
    Some(
        env_value("RWA_JUPITER_V2_TAKER_MAX_REQUESTS")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(6),
    )
}

fn rwa_jupiter_v2_rfq_only_max_requests() -> Option<usize> {
    Some(
        env_value("RWA_JUPITER_V2_RFQ_ONLY_MAX_REQUESTS")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(12),
    )
}

fn rwa_jupiter_v1_fallback_max_requests() -> usize {
    env_value("RWA_JUPITER_V1_FALLBACK_MAX_REQUESTS")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(24)
}

fn rwa_jupiter_v2_rfq_only_exclude_routers() -> Option<String> {
    rwa_jupiter_v2_rfq_only_exclude_routers_from_value(
        env_value("RWA_JUPITER_V2_RFQ_ONLY_EXCLUDE_ROUTERS").as_deref(),
    )
}

fn rwa_jupiter_v2_rfq_only_exclude_routers_from_value(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn rwa_jupiter_v2_quote_taker(
    taker_pubkey: Option<&str>,
    quote_with_taker: bool,
) -> Option<String> {
    if !quote_with_taker {
        return None;
    }
    taker_pubkey
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn rwa_jupiter_max_in_flight() -> usize {
    env_value("RWA_JUPITER_MAX_IN_FLIGHT")
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(2)
}

fn rwa_jupiter_semaphore() -> Arc<Semaphore> {
    static CELL: OnceLock<Arc<Semaphore>> = OnceLock::new();
    CELL.get_or_init(|| Arc::new(Semaphore::new(rwa_jupiter_max_in_flight())))
        .clone()
}

async fn acquire_rwa_jupiter_permit() -> Option<OwnedSemaphorePermit> {
    rwa_jupiter_semaphore().acquire_owned().await.ok()
}

fn rwa_jupiter_v2_timeout_ms() -> u64 {
    env_value("RWA_JUPITER_V2_TIMEOUT_MS")
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(2_000)
}

fn rwa_jupiter_v2_min_order_usd() -> f64 {
    env_value("RWA_JUPITER_V2_MIN_ORDER_USD")
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0)
        .unwrap_or(5.0)
}

fn rwa_jupiter_batch_delay_ms() -> u64 {
    env_value("RWA_JUPITER_BATCH_DELAY_MS")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(250)
}

fn rwa_jupiter_quote_budget_ms() -> u64 {
    rwa_jupiter_quote_budget_ms_from_value(env_value("RWA_JUPITER_QUOTE_BUDGET_MS").as_deref())
}

fn rwa_jupiter_quote_budget_ms_from_value(value: Option<&str>) -> u64 {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(20_000)
        .clamp(1_000, 120_000)
}

fn rwa_jupiter_quote_budget_remaining(deadline: std::time::Instant) -> Option<std::time::Duration> {
    let now = std::time::Instant::now();
    (deadline > now).then(|| deadline.duration_since(now))
}

fn rwa_aggregator_quote_budget_ms() -> u64 {
    env_value("RWA_AGGREGATOR_QUOTE_BUDGET_MS")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(2_500)
}

fn rwa_jupiter_rate_limit_cooldown_ms() -> u64 {
    env_value("RWA_JUPITER_RATE_LIMIT_COOLDOWN_MS")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(60_000)
}

fn rwa_jupiter_rate_limit_cell() -> &'static Mutex<Option<std::time::Instant>> {
    static CELL: OnceLock<Mutex<Option<std::time::Instant>>> = OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

fn rwa_jupiter_rate_limited() -> bool {
    let Ok(mut cooldown) = rwa_jupiter_rate_limit_cell().lock() else {
        return false;
    };
    match *cooldown {
        Some(until) if until > std::time::Instant::now() => true,
        Some(_) => {
            *cooldown = None;
            false
        }
        None => false,
    }
}

fn rwa_jupiter_rate_limit_cooldown_remaining_ms() -> Option<u64> {
    let Ok(mut cooldown) = rwa_jupiter_rate_limit_cell().lock() else {
        return None;
    };
    let now = std::time::Instant::now();
    match *cooldown {
        Some(until) if until > now => {
            let remaining = until.duration_since(now);
            let millis =
                remaining.as_millis() + u128::from(remaining.subsec_nanos() % 1_000_000 != 0);
            Some(millis.min(u64::MAX as u128) as u64)
        }
        Some(_) => {
            *cooldown = None;
            None
        }
        None => None,
    }
}

fn start_rwa_jupiter_rate_limit_cooldown() {
    let cooldown_ms = rwa_jupiter_rate_limit_cooldown_ms();
    let Ok(mut cooldown) = rwa_jupiter_rate_limit_cell().lock() else {
        return;
    };
    *cooldown = Some(std::time::Instant::now() + std::time::Duration::from_millis(cooldown_ms));
    warn!(
        cooldown_ms,
        "RWA Jupiter quote rate limited; starting cooldown"
    );
}

fn is_jupiter_rate_limit_text(value: &str) -> bool {
    value.contains("Too many requests") || value.contains("rate limit")
}

fn is_jupiter_no_route_text(value: &str) -> bool {
    value.contains("NO_ROUTES_FOUND")
        || value.contains("No routes found")
        || value.contains("Quote not available from market maker")
        || value.contains("Failed to get quotes")
}

fn is_jupiter_token_not_tradable_text(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    lower.contains("token_not_tradable") || lower.contains("not tradable")
}

fn jupiter_token_not_tradable_mint_from_text(value: &str) -> Option<String> {
    let marker = "The token ";
    let start = value.find(marker)? + marker.len();
    let rest = &value[start..];
    let end = rest.find(" is not tradable")?;
    let mint = rest[..end].trim();
    (!mint.is_empty()).then(|| mint.to_string())
}

fn rwa_jupiter_untradable_token_cooldown_secs() -> u64 {
    env_value("RWA_JUPITER_TOKEN_NOT_TRADABLE_COOLDOWN_SECS")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(900)
}

fn rwa_jupiter_untradable_token_cooldowns() -> &'static Mutex<HashMap<String, std::time::Instant>> {
    static CELL: OnceLock<Mutex<HashMap<String, std::time::Instant>>> = OnceLock::new();
    CELL.get_or_init(|| Mutex::new(HashMap::new()))
}

fn retain_rwa_jupiter_tradable_quote_requests(
    requests: Vec<AggregatorQuoteRequest>,
) -> Vec<AggregatorQuoteRequest> {
    let Ok(mut cooldowns) = rwa_jupiter_untradable_token_cooldowns().lock() else {
        return requests;
    };
    let now = std::time::Instant::now();
    cooldowns.retain(|_, until| *until > now);
    if cooldowns.is_empty() {
        return requests;
    }
    requests
        .into_iter()
        .filter(|request| {
            !rwa_request_non_quote_mints(request)
                .iter()
                .any(|mint| cooldowns.contains_key(*mint))
        })
        .collect()
}

fn record_rwa_jupiter_untradable_token_cooldown(
    request: &AggregatorQuoteRequest,
    error_text: &str,
) {
    if !is_jupiter_token_not_tradable_text(error_text) {
        return;
    }
    let request_mints = rwa_request_non_quote_mints(request);
    let mints = if let Some(mint) = jupiter_token_not_tradable_mint_from_text(error_text) {
        if request_mints.contains(&mint.as_str()) {
            vec![mint]
        } else {
            Vec::new()
        }
    } else {
        let matched_mints = request_mints
            .iter()
            .copied()
            .filter(|mint| error_text.contains(mint))
            .map(str::to_string)
            .collect::<Vec<_>>();
        if matched_mints.is_empty() {
            request_mints.into_iter().map(str::to_string).collect()
        } else {
            matched_mints
        }
    };
    if mints.is_empty() {
        return;
    }

    let cooldown_secs = rwa_jupiter_untradable_token_cooldown_secs();
    let until = std::time::Instant::now() + std::time::Duration::from_secs(cooldown_secs);
    let Ok(mut cooldowns) = rwa_jupiter_untradable_token_cooldowns().lock() else {
        return;
    };
    for mint in mints {
        cooldowns.insert(mint.clone(), until);
        warn!(
            mint,
            cooldown_secs, "cooling down RWA Jupiter quote mint after TOKEN_NOT_TRADABLE"
        );
    }
}

fn rwa_request_non_quote_mints(request: &AggregatorQuoteRequest) -> Vec<&str> {
    let mut mints = Vec::new();
    for mint in [
        request.pair.base_mint.as_str(),
        request.pair.quote_mint.as_str(),
    ] {
        if is_rwa_quote_funding_mint(mint) || mints.contains(&mint) {
            continue;
        }
        mints.push(mint);
    }
    mints
}

fn is_rwa_quote_funding_mint(mint: &str) -> bool {
    is_supported_stable_quote_mint(mint) || mint == WSOL_MINT
}

#[cfg(test)]
fn clear_rwa_jupiter_untradable_token_cooldowns_for_tests() {
    if let Ok(mut cooldowns) = rwa_jupiter_untradable_token_cooldowns().lock() {
        cooldowns.clear();
    }
}

fn env_flag_default(name: &str, default: bool) -> bool {
    env_bool(name).unwrap_or(default)
}

fn rwa_sequential_roundtrip_enabled() -> bool {
    env_flag_default("RWA_SEQUENTIAL_ROUNDTRIP_ENABLED", true)
}

fn rwa_sequential_rebalance_enabled() -> bool {
    env_flag_default("RWA_SEQUENTIAL_REBALANCE_ENABLED", true)
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

fn json_u128(value: &Value, paths: &[&str]) -> Option<u128> {
    paths.iter().find_map(|path| {
        let value = value.pointer(path)?;
        value
            .as_u64()
            .map(|value| value as u128)
            .or_else(|| value.as_str()?.parse::<u128>().ok())
    })
}

fn json_u64(value: &Value, paths: &[&str]) -> Option<u64> {
    paths.iter().find_map(|path| {
        let value = value.pointer(path)?;
        value
            .as_u64()
            .or_else(|| value.as_str()?.parse::<u64>().ok())
    })
}

fn json_f64(value: &Value, paths: &[&str]) -> Option<f64> {
    paths.iter().find_map(|path| {
        let value = value.pointer(path)?;
        value
            .as_f64()
            .or_else(|| value.as_str()?.parse::<f64>().ok())
    })
}

fn json_str<'a>(value: &'a Value, paths: &[&str]) -> Option<&'a str> {
    paths.iter().find_map(|path| value.pointer(path)?.as_str())
}

fn dedupe_quotes(quotes: &mut Vec<Quote>) {
    let mut seen = HashSet::new();
    quotes.retain(|quote| {
        seen.insert(format!(
            "{}:{}:{:?}:{}:{}:{}",
            quote.venue,
            quote.pair.key(),
            quote.side,
            quote.in_amount_atoms,
            quote.out_amount_atoms,
            quote.pool
        ))
    });
}

struct CachedRwaQuote {
    quote: Quote,
    expires_at: std::time::Instant,
}

fn merge_rwa_quote_cache(quotes: &mut Vec<Quote>) -> usize {
    let ttl_ms = rwa_quote_cache_ttl_ms();
    if ttl_ms == 0 {
        return 0;
    }
    let Ok(mut cache) = rwa_quote_cache().lock() else {
        return 0;
    };
    let now = std::time::Instant::now();
    cache.retain(|_, entry| entry.expires_at > now);

    let fresh_keys = quotes
        .iter()
        .map(rwa_quote_cache_key)
        .collect::<HashSet<_>>();
    let expires_at = now + std::time::Duration::from_millis(ttl_ms);
    for quote in quotes.iter().cloned() {
        cache.insert(
            rwa_quote_cache_key(&quote),
            CachedRwaQuote { quote, expires_at },
        );
    }

    let cached_quotes = cache
        .iter()
        .filter(|(key, _)| !fresh_keys.contains(*key))
        .map(|(_, entry)| entry.quote.clone())
        .collect::<Vec<_>>();
    let cached_count = cached_quotes.len();
    quotes.extend(cached_quotes);
    cached_count
}

fn rwa_quote_cache_key(quote: &Quote) -> String {
    format!(
        "{}:{}:{:?}:{}:{}",
        quote.venue,
        quote.pair.key(),
        quote.side,
        quote.in_amount_atoms,
        quote.pool
    )
}

fn rwa_quote_cache() -> &'static Mutex<HashMap<String, CachedRwaQuote>> {
    static CELL: OnceLock<Mutex<HashMap<String, CachedRwaQuote>>> = OnceLock::new();
    CELL.get_or_init(|| Mutex::new(HashMap::new()))
}

fn rwa_quote_cache_ttl_ms() -> u64 {
    env_value("RWA_QUOTE_CACHE_TTL_MS")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(20_000)
        .min(120_000)
}

fn remove_rwa_live_blocked_quotes(quotes: &mut Vec<Quote>) -> usize {
    if !rwa_block_v2_okx_routes() {
        return 0;
    }
    let before = quotes.len();
    quotes.retain(|quote| !rwa_live_blocked_quote_pool(&quote.pool));
    before.saturating_sub(quotes.len())
}

fn rwa_live_blocked_quote_pool(pool: &str) -> bool {
    pool.to_ascii_lowercase()
        .contains("jupiter-v2-order:aggregator:okx")
}

fn rwa_block_v2_okx_routes() -> bool {
    env_bool("RWA_BLOCK_V2_OKX_ROUTES").unwrap_or(false)
}

fn rwa_block_stable_route_rfq_exit() -> bool {
    env_bool("RWA_BLOCK_STABLE_ROUTE_RFQ_EXIT").unwrap_or(false)
}

#[cfg(test)]
fn clear_rwa_quote_cache_for_tests() {
    if let Ok(mut cache) = rwa_quote_cache().lock() {
        cache.clear();
    }
}

struct QuoteIndex {
    quotes: Vec<Quote>,
    quote_usd_per_unit_by_mint: HashMap<String, f64>,
}

impl QuoteIndex {
    #[cfg(test)]
    fn new(quotes: Vec<Quote>) -> Self {
        Self::with_quote_assets(quotes, &[])
    }

    fn with_quote_assets(quotes: Vec<Quote>, quote_assets: &[RwaQuoteAsset]) -> Self {
        let mut quote_usd_per_unit_by_mint = quote_assets
            .iter()
            .map(|asset| (asset.mint.clone(), asset.usd_per_unit))
            .collect::<HashMap<_, _>>();
        quote_usd_per_unit_by_mint
            .entry(USDC_MINT.to_string())
            .or_insert(1.0);
        quote_usd_per_unit_by_mint
            .entry(USDT_MINT.to_string())
            .or_insert(1.0);
        Self {
            quotes,
            quote_usd_per_unit_by_mint,
        }
    }

    fn quote_usd_per_unit(&self, mint: &str) -> f64 {
        self.quote_usd_per_unit_by_mint
            .get(mint)
            .copied()
            .unwrap_or(1.0)
    }

    fn best_buy(&self, base: &str, quote: &str) -> Option<Quote> {
        self.best_buy_matching(base, quote, |_| true)
    }

    fn best_buy_matching<F>(&self, base: &str, quote: &str, predicate: F) -> Option<Quote>
    where
        F: Fn(&Quote) -> bool,
    {
        self.quotes
            .iter()
            .filter(|item| {
                item.pair.base_mint == base
                    && item.pair.quote_mint == quote
                    && item.side == SwapSide::QuoteToBase
                    && predicate(item)
            })
            .filter_map(|quote| {
                stable_price_per_token_usd(
                    quote,
                    quote.pair.base_decimals,
                    self.quote_usd_per_unit(&quote.pair.quote_mint),
                )
                .map(|price| (price, quote))
            })
            .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(_, quote)| quote.clone())
    }

    fn best_sell(&self, base: &str, quote: &str) -> Option<Quote> {
        self.best_sell_matching(base, quote, |_| true)
    }

    fn best_sell_matching<F>(&self, base: &str, quote: &str, predicate: F) -> Option<Quote>
    where
        F: Fn(&Quote) -> bool,
    {
        self.quotes
            .iter()
            .filter(|item| {
                item.pair.base_mint == base
                    && item.pair.quote_mint == quote
                    && item.side == SwapSide::BaseToQuote
                    && predicate(item)
            })
            .filter_map(|quote| {
                stable_price_per_token_usd(
                    quote,
                    quote.pair.base_decimals,
                    self.quote_usd_per_unit(&quote.pair.quote_mint),
                )
                .map(|price| (price, quote))
            })
            .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(_, quote)| quote.clone())
    }

    fn best_direct_value_gain(
        &self,
        base: &str,
        quote: &str,
        base_shares_per_token: f64,
        quote_shares_per_token: f64,
    ) -> Option<(Quote, f64)> {
        self.quotes
            .iter()
            .filter(|item| {
                item.pair.base_mint == base
                    && item.pair.quote_mint == quote
                    && item.side == SwapSide::BaseToQuote
            })
            .filter_map(|quote| {
                let input = atoms_to_ui(quote.in_amount_atoms, quote.pair.base_decimals)
                    * base_shares_per_token;
                let output = atoms_to_ui(quote.out_amount_atoms, quote.pair.quote_decimals)
                    * quote_shares_per_token;
                if input <= 0.0 {
                    return None;
                }
                Some((quote.clone(), (output / input - 1.0) * 10_000.0))
            })
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
    }
}

fn evaluate_asset(
    config: &RwaConfig,
    runtime: &RwaAssetRuntime,
    quote_assets: &[RwaQuoteAsset],
    oracle_prices: &HashMap<String, PythOraclePrice>,
    quote_index: &QuoteIndex,
    network_fee: &FeeBreakdown,
) -> (RwaAssetSnapshot, Vec<RwaOpportunity>) {
    let asset = &runtime.asset;
    let oracle = oracle_prices.get(&asset.pyth_feed_id).cloned();
    let mut no_trade_reasons = Vec::new();
    if oracle.is_none() {
        no_trade_reasons.push("missing Pyth price".to_string());
    }
    if oracle
        .as_ref()
        .is_some_and(|price| price.status != OracleStatus::Valid)
    {
        if let Some(price) = oracle.as_ref() {
            no_trade_reasons.push(format!(
                "Pyth price rejected: {}",
                oracle_rejection_reason(price)
            ));
        }
    }

    let mut opportunities = Vec::new();
    let mut best_xstock = None;
    let mut best_ondo = None;
    let mut per_quote_asset_issuers: Vec<(RwaQuoteAsset, Vec<IssuerQuotes>)> = Vec::new();
    let mut quote_count = 0usize;
    let configured = configured_issuers(asset);
    for quote_asset in quote_assets {
        let issuers = configured
            .iter()
            .map(|issuer| IssuerQuotes {
                symbol: issuer.symbol.clone(),
                mint: issuer.mint.clone(),
                decimals: issuer.decimals,
                shares_per_token: issuer.shares_per_token,
                quote_usd_per_unit: quote_asset.usd_per_unit,
                best_buy: quote_index.best_buy(&issuer.mint, &quote_asset.mint),
                best_sell: quote_index.best_sell(&issuer.mint, &quote_asset.mint),
                secondary_buy: quote_index.best_buy_matching(
                    &issuer.mint,
                    &quote_asset.mint,
                    |quote| !is_jupiter_v2_rfq_route(&quote.pool),
                ),
                secondary_sell: quote_index.best_sell_matching(
                    &issuer.mint,
                    &quote_asset.mint,
                    |quote| !is_jupiter_v2_rfq_route(&quote.pool),
                ),
                rfq_buy: quote_index.best_buy_matching(&issuer.mint, &quote_asset.mint, |quote| {
                    is_jupiter_v2_rfq_route(&quote.pool)
                }),
                rfq_sell: quote_index.best_sell_matching(
                    &issuer.mint,
                    &quote_asset.mint,
                    |quote| is_jupiter_v2_rfq_route(&quote.pool),
                ),
            })
            .collect::<Vec<_>>();
        quote_count += issuers
            .iter()
            .map(|issuer| issuer.best_buy.is_some() as usize + issuer.best_sell.is_some() as usize)
            .sum::<usize>();

        if let Some(oracle) = oracle
            .as_ref()
            .filter(|price| price.status == OracleStatus::Valid)
        {
            for issuer in &issuers {
                if let (Some(buy_quote), Some(sell_quote), Some(buy_price), Some(sell_price)) = (
                    issuer.best_buy.as_ref(),
                    issuer.best_sell.as_ref(),
                    issuer.buy_price_usd(),
                    issuer.sell_price_usd(),
                ) {
                    opportunities.push(stable_route_arbitrage_opportunity(
                        config,
                        asset,
                        oracle,
                        quote_asset,
                        issuer,
                        buy_quote,
                        sell_quote,
                        buy_price,
                        sell_price,
                        network_fee,
                    ));
                }
                if let (Some(buy_quote), Some(buy_price)) = (
                    issuer.secondary_buy.as_ref(),
                    issuer.normalized_secondary_buy_price_usd(),
                ) {
                    if let Some(opportunity) = pyth_discount_buy_opportunity(
                        config,
                        asset,
                        oracle,
                        quote_asset,
                        issuer,
                        buy_quote,
                        buy_price,
                        network_fee,
                    ) {
                        opportunities.push(opportunity);
                    }
                }
            }
            for rich in &issuers {
                for cheap in &issuers {
                    if rich.mint == cheap.mint {
                        continue;
                    }
                    opportunities.extend(relative_value_opportunities(
                        config,
                        asset,
                        oracle,
                        quote_asset,
                        rich,
                        cheap,
                        network_fee,
                    ));
                }
            }
        }

        for issuer in &issuers {
            if issuer.mint == asset.xstock_mint {
                best_xstock = better_issuer(best_xstock, issuer.clone());
            } else if issuer.mint == asset.ondo_mint {
                best_ondo = better_issuer(best_ondo, issuer.clone());
            }
        }
        per_quote_asset_issuers.push((quote_asset.clone(), issuers));
    }

    let mut direct_route_count = 0usize;
    let direct_route_request_count = configured
        .len()
        .saturating_mul(configured.len().saturating_sub(1));
    let valid_oracle = oracle
        .as_ref()
        .filter(|price| price.status == OracleStatus::Valid);
    for from in &configured {
        for to in &configured {
            if from.mint == to.mint {
                continue;
            }
            if let Some((quote, gross_edge_bps)) = quote_index.best_direct_value_gain(
                &from.mint,
                &to.mint,
                from.shares_per_token,
                to.shares_per_token,
            ) {
                direct_route_count += 1;
                if let Some(oracle) = valid_oracle {
                    opportunities.push(direct_swap_opportunity(
                        config,
                        asset,
                        oracle,
                        &quote,
                        &from.symbol,
                        &to.symbol,
                        gross_edge_bps,
                        network_fee,
                    ));
                    for (quote_asset, issuers) in &per_quote_asset_issuers {
                        let Some(from_issuer) =
                            issuers.iter().find(|issuer| issuer.mint == from.mint)
                        else {
                            continue;
                        };
                        let Some(to_issuer) = issuers.iter().find(|issuer| issuer.mint == to.mint)
                        else {
                            continue;
                        };
                        if let (Some(buy_quote), Some(sell_quote)) =
                            (from_issuer.best_buy.as_ref(), to_issuer.best_sell.as_ref())
                        {
                            if let Some(opportunity) = stable_funded_direct_roundtrip_opportunity(
                                config,
                                asset,
                                oracle,
                                quote_asset,
                                from,
                                to,
                                buy_quote,
                                &quote,
                                sell_quote,
                                network_fee,
                            ) {
                                opportunities.push(opportunity);
                            }
                        }
                        if let Some(buy_quote) = from_issuer.best_buy.as_ref() {
                            if let Some(opportunity) = stable_funded_direct_accumulation_opportunity(
                                config,
                                asset,
                                oracle,
                                quote_asset,
                                from,
                                to,
                                buy_quote,
                                &quote,
                                network_fee,
                            ) {
                                opportunities.push(opportunity);
                            }
                        }
                    }
                }
            }
        }
    }

    if valid_oracle.is_some() && direct_route_count == 0 {
        if direct_route_request_count > 0 {
            no_trade_reasons.push(format!(
                "no direct issuer-to-issuer route returned after {direct_route_request_count} route request(s)"
            ));
        } else {
            no_trade_reasons.push("no direct issuer-to-issuer route configured".to_string());
        }
    }
    if valid_oracle.is_some() && quote_count == 0 {
        no_trade_reasons.push("no stable quote returned for watched token mints".to_string());
    }

    let xstock_price = best_xstock
        .as_ref()
        .and_then(IssuerQuotes::executable_price_usd);
    let ondo_price = best_ondo
        .as_ref()
        .and_then(IssuerQuotes::executable_price_usd);
    let ondo_normalized = best_ondo
        .as_ref()
        .and_then(IssuerQuotes::normalized_executable_price_usd);
    let pyth_price = oracle
        .as_ref()
        .filter(|price| price.status == OracleStatus::Valid)
        .map(|price| price.price_usd);
    let xstock_premium_bps = xstock_price
        .zip(pyth_price)
        .map(|(market, fair)| premium_bps(market, fair));
    let ondo_premium_bps = ondo_normalized
        .zip(pyth_price)
        .map(|(market, fair)| premium_bps(market, fair));

    if valid_oracle.is_some() && xstock_price.is_none() {
        no_trade_reasons.push("no xStock stable quote returned".to_string());
    }
    if valid_oracle.is_some() && ondo_price.is_none() {
        no_trade_reasons.push("no Ondo stable quote returned".to_string());
    }
    let mut primary_routes = primary_routes_for_asset(config, asset, oracle.as_ref());
    let primary_route_issuer_refs = per_quote_asset_issuers
        .iter()
        .flat_map(|(_, issuers)| issuers.iter().map(Some))
        .collect::<Vec<_>>();
    annotate_primary_route_quote_availability(&mut primary_routes, &primary_route_issuer_refs);
    if primary_routes.is_empty() {
        no_trade_reasons.push("no enabled primary mint/burn route".to_string());
    } else if oracle
        .as_ref()
        .is_none_or(|price| price.status != OracleStatus::Valid)
    {
        no_trade_reasons.push(
            "primary mint/burn routes configured but blocked until Pyth price is valid".to_string(),
        );
    }
    if let Some(oracle) = oracle
        .as_ref()
        .filter(|price| price.status == OracleStatus::Valid)
    {
        for (quote_asset, issuers) in &per_quote_asset_issuers {
            let issuer_refs = issuers.iter().map(Some).collect::<Vec<_>>();
            opportunities.extend(primary_route_opportunities(
                config,
                asset,
                oracle,
                quote_asset,
                &issuer_refs,
                &primary_routes,
                network_fee,
            ));
        }
    }
    let rejected_opportunities = opportunities
        .iter()
        .filter(|opportunity| opportunity.net_profit_bps < config.min_net_profit_bps)
        .cloned()
        .collect::<Vec<_>>();
    let best_rejected = rejected_opportunities
        .iter()
        .max_by(|left, right| {
            left.net_profit_bps
                .partial_cmp(&right.net_profit_bps)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .cloned();
    let best_rejected_by_kind = best_rejected_by_kind(&rejected_opportunities, config);
    opportunities.retain(|opportunity| opportunity.net_profit_bps >= config.min_net_profit_bps);
    if opportunities.is_empty() && no_trade_reasons.is_empty() {
        if let Some(rejected) = best_rejected.as_ref() {
            no_trade_reasons.push(format!(
                "quotes valid, but best rejected {:?} route was {:.2} bps below configured {:.2} bps net-profit threshold",
                rejected.kind, rejected.net_profit_bps, config.min_net_profit_bps
            ));
        } else {
            no_trade_reasons.push(
                "quotes valid, but no edge above configured net-profit threshold".to_string(),
            );
        }
    }

    (
        RwaAssetSnapshot {
            ticker: asset.ticker.clone(),
            enabled: asset.enabled,
            pyth_feed_id: asset.pyth_feed_id.clone(),
            xstock_symbol: asset
                .xstock_symbol
                .clone()
                .if_empty_then(|| format!("{}x", asset.ticker)),
            xstock_mint: asset.xstock_mint.clone(),
            ondo_symbol: asset
                .ondo_symbol
                .clone()
                .if_empty_then(|| format!("{}on", asset.ticker)),
            ondo_mint: asset.ondo_mint.clone(),
            ondo_shares_per_token: asset.ondo_shares_per_token,
            oracle,
            xstock_price_usd: xstock_price,
            ondo_price_usd: ondo_price,
            ondo_normalized_price_usd: ondo_normalized,
            xstock_premium_bps,
            ondo_premium_bps,
            quote_assets: quote_assets
                .iter()
                .map(|asset| asset.symbol.clone())
                .collect(),
            quote_count,
            direct_route_request_count,
            direct_route_count,
            primary_route_count: primary_routes.len(),
            primary_routes,
            best_rejected_kind: best_rejected.as_ref().map(|opportunity| opportunity.kind),
            best_rejected_net_profit_bps: best_rejected
                .as_ref()
                .map(|opportunity| opportunity.net_profit_bps),
            best_rejected_route: best_rejected
                .as_ref()
                .map(|opportunity| opportunity.route.clone()),
            best_rejected_reason: best_rejected.as_ref().map(|opportunity| {
                opportunity.blocked_reason.clone().unwrap_or_else(|| {
                    format!(
                        "net profit {:.2} bps is below configured {:.2} bps threshold",
                        opportunity.net_profit_bps, config.min_net_profit_bps
                    )
                })
            }),
            best_rejected_by_kind,
            no_trade_reason: no_trade_reasons.join("; "),
            source: asset.source.clone(),
        },
        opportunities,
    )
}

fn stable_route_arbitrage_opportunity(
    config: &RwaConfig,
    asset: &RwaAssetConfig,
    oracle: &PythOraclePrice,
    quote_asset: &RwaQuoteAsset,
    issuer: &IssuerQuotes,
    buy_quote: &Quote,
    sell_quote: &Quote,
    buy_price: f64,
    sell_price: f64,
    network_fee: &FeeBreakdown,
) -> RwaOpportunity {
    let sell_input_atoms = buy_quote.out_amount_atoms.min(sell_quote.in_amount_atoms);
    let buy_input_atoms = if sell_input_atoms < buy_quote.out_amount_atoms {
        prorate_atoms(
            buy_quote.in_amount_atoms,
            sell_input_atoms,
            buy_quote.out_amount_atoms,
        )
        .unwrap_or(buy_quote.in_amount_atoms)
    } else {
        buy_quote.in_amount_atoms
    };
    let stable_output_atoms = if sell_input_atoms < sell_quote.in_amount_atoms {
        prorate_atoms(
            sell_quote.out_amount_atoms,
            sell_input_atoms,
            sell_quote.in_amount_atoms,
        )
        .unwrap_or(sell_quote.out_amount_atoms)
    } else {
        sell_quote.out_amount_atoms
    };
    let input_usd = quote_asset.input_usd_for_atoms(buy_input_atoms);
    let gross_edge_bps = premium_bps(sell_price, buy_price);
    let net_profit_bps = net_profit_bps(
        gross_edge_bps,
        oracle.confidence_bps,
        config.fee_safety_buffer_bps,
        network_fee.network_fee_usd,
        input_usd,
    );
    let sequential_enabled = rwa_sequential_roundtrip_enabled();
    let execution_legs = vec![
        execution_leg_from_quote(buy_quote, buy_input_atoms, Some(input_usd)),
        execution_leg_from_quote(sell_quote, sell_input_atoms, Some(input_usd)),
    ];
    let route_blocked_reason = merge_blocked_reasons(
        execution_legs_route_blocked_reason(&execution_legs),
        stable_route_rfq_exit_blocked_reason(&execution_legs),
    );
    RwaOpportunity {
        ticker: asset.ticker.clone(),
        kind: RwaOpportunityKind::StableRouteArbitrage,
        from_symbol: quote_asset.symbol.clone(),
        to_symbol: quote_asset.symbol.clone(),
        from_mint: quote_asset.mint.clone(),
        to_mint: quote_asset.mint.clone(),
        quote_asset: Some(quote_asset.symbol.clone()),
        pyth_fair_price_usd: oracle.price_usd,
        from_price_usd: Some(buy_price),
        to_price_usd: Some(sell_price),
        input_atoms: Some(buy_input_atoms),
        input_estimated_usd: Some(input_usd),
        expected_output_atoms: Some(stable_output_atoms),
        route: format!(
            "{} buy via {}; {} sell via {}",
            issuer.symbol, buy_quote.pool, issuer.symbol, sell_quote.pool
        ),
        gross_edge_bps,
        net_profit_bps,
        net_profit_usd: net_profit_bps / 10_000.0 * input_usd,
        confidence_buffer_bps: oracle.confidence_bps,
        fee: sell_quote.fee.clone(),
        executable: config.execution_enabled
            && sequential_enabled
            && route_blocked_reason.is_none(),
        execution_legs,
        blocked_reason: merge_blocked_reasons(
            (!sequential_enabled)
                .then(|| "sequential RWA stable round-trip execution is disabled".to_string()),
            route_blocked_reason,
        ),
    }
}

fn stable_route_rfq_exit_blocked_reason(legs: &[RwaExecutionLeg]) -> Option<String> {
    if !rwa_block_stable_route_rfq_exit() {
        return None;
    }
    let last_leg = legs.last()?;
    if !rwa_jupiterz_rfq_route(&last_leg.route) {
        return None;
    }
    Some(
        "JupiterZ RFQ exit leg is blocked for stable route roundtrips in the live proof profile"
            .to_string(),
    )
}

fn rwa_jupiterz_rfq_route(route: &str) -> bool {
    route
        .to_ascii_lowercase()
        .contains("jupiter-v2-order:rfq:jupiterz")
}

fn best_rejected_by_kind(
    rejected_opportunities: &[RwaOpportunity],
    config: &RwaConfig,
) -> Vec<RwaRejectedOpportunitySnapshot> {
    [
        RwaOpportunityKind::StableRouteArbitrage,
        RwaOpportunityKind::StableFundedDirectIssuerRoundtrip,
        RwaOpportunityKind::StableFundedDirectIssuerAccumulation,
        RwaOpportunityKind::PrimaryMintSell,
        RwaOpportunityKind::PrimaryBuyBurn,
        RwaOpportunityKind::PythDiscountBuy,
        RwaOpportunityKind::DirectIssuerSwap,
        RwaOpportunityKind::NeutralInventoryRebalance,
        RwaOpportunityKind::DirectionalSell,
    ]
    .into_iter()
    .filter_map(|kind| {
        rejected_opportunities
            .iter()
            .filter(|opportunity| opportunity.kind == kind)
            .max_by(|left, right| {
                left.net_profit_bps
                    .partial_cmp(&right.net_profit_bps)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|opportunity| rejected_opportunity_snapshot(opportunity, config))
    })
    .collect()
}

fn rejected_opportunity_snapshot(
    opportunity: &RwaOpportunity,
    config: &RwaConfig,
) -> RwaRejectedOpportunitySnapshot {
    RwaRejectedOpportunitySnapshot {
        kind: opportunity.kind,
        net_profit_bps: opportunity.net_profit_bps,
        net_profit_usd: opportunity.net_profit_usd,
        route: opportunity.route.clone(),
        reason: opportunity.blocked_reason.clone().unwrap_or_else(|| {
            format!(
                "net profit {:.2} bps is below configured {:.2} bps threshold",
                opportunity.net_profit_bps, config.min_net_profit_bps
            )
        }),
    }
}

fn relative_value_opportunities(
    config: &RwaConfig,
    asset: &RwaAssetConfig,
    oracle: &PythOraclePrice,
    quote_asset: &RwaQuoteAsset,
    rich: &IssuerQuotes,
    cheap: &IssuerQuotes,
    network_fee: &FeeBreakdown,
) -> Vec<RwaOpportunity> {
    let mut opportunities = Vec::new();
    if matches!(config.mode, RwaMode::Neutral | RwaMode::Both) {
        if let (Some(rich_sell), Some(cheap_buy)) = (
            rich.normalized_sell_price_usd(),
            cheap.normalized_buy_price_usd(),
        ) {
            opportunities.push(inventory_rebalance_opportunity(
                config,
                asset,
                oracle,
                quote_asset,
                rich,
                cheap,
                rich_sell,
                cheap_buy,
                network_fee,
            ));
        }
    }

    if config.directional_enabled && matches!(config.mode, RwaMode::Directional | RwaMode::Both) {
        if let Some(ema) = oracle.ema_price_usd.filter(|ema| *ema > 0.0) {
            let down_bps = (ema - oracle.price_usd) / ema * 10_000.0;
            if down_bps > config.min_net_profit_bps {
                if let (Some(sell_quote), Some(sell_price)) =
                    (rich.best_sell.as_ref(), rich.normalized_sell_price_usd())
                {
                    let input_estimated_usd =
                        issuer_quote_input_usd(sell_quote, rich, oracle.price_usd).unwrap_or_else(
                            || {
                                asset
                                    .max_trade_usd
                                    .unwrap_or(config.max_trade_usd_per_asset)
                            },
                        );
                    let net = net_profit_bps(
                        down_bps,
                        oracle.confidence_bps,
                        config.fee_safety_buffer_bps,
                        network_fee.network_fee_usd,
                        input_estimated_usd,
                    );
                    let execution_legs = vec![execution_leg_from_quote(
                        sell_quote,
                        sell_quote.in_amount_atoms,
                        Some(input_estimated_usd),
                    )];
                    let route_blocked_reason = execution_legs_route_blocked_reason(&execution_legs);
                    let executable = config.execution_enabled && route_blocked_reason.is_none();
                    opportunities.push(RwaOpportunity {
                        ticker: asset.ticker.clone(),
                        kind: RwaOpportunityKind::DirectionalSell,
                        from_symbol: rich.symbol.clone(),
                        to_symbol: quote_asset.symbol.clone(),
                        from_mint: rich.mint.clone(),
                        to_mint: quote_asset.mint.clone(),
                        quote_asset: Some(quote_asset.symbol.clone()),
                        pyth_fair_price_usd: oracle.price_usd,
                        from_price_usd: Some(sell_price),
                        to_price_usd: Some(1.0),
                        input_atoms: Some(sell_quote.in_amount_atoms),
                        input_estimated_usd: Some(input_estimated_usd),
                        expected_output_atoms: Some(sell_quote.out_amount_atoms),
                        route: sell_quote.pool.clone(),
                        gross_edge_bps: down_bps,
                        net_profit_bps: net,
                        net_profit_usd: net / 10_000.0 * input_estimated_usd,
                        confidence_buffer_bps: oracle.confidence_bps,
                        fee: sell_quote.fee.clone(),
                        executable,
                        execution_legs,
                        blocked_reason: merge_blocked_reasons(
                            (!config.execution_enabled).then(|| {
                                "directional sell is visible but RWA live execution is disabled"
                                    .to_string()
                            }),
                            route_blocked_reason,
                        ),
                    });
                }
            }
        }
    }

    opportunities
}

fn issuer_quote_input_usd(
    quote: &Quote,
    issuer: &IssuerQuotes,
    pyth_price_usd: f64,
) -> Option<f64> {
    if quote.side != SwapSide::BaseToQuote || !pyth_price_usd.is_finite() || pyth_price_usd <= 0.0 {
        return None;
    }
    let input = atoms_to_ui(quote.in_amount_atoms, issuer.decimals)
        * issuer.shares_per_token
        * pyth_price_usd;
    (input.is_finite() && input > 0.0).then_some(input)
}

fn pyth_discount_buy_opportunity(
    config: &RwaConfig,
    asset: &RwaAssetConfig,
    oracle: &PythOraclePrice,
    quote_asset: &RwaQuoteAsset,
    issuer: &IssuerQuotes,
    buy_quote: &Quote,
    buy_price: f64,
    network_fee: &FeeBreakdown,
) -> Option<RwaOpportunity> {
    if !rwa_pyth_discount_buy_enabled(config) {
        return None;
    }
    if buy_quote.side != SwapSide::QuoteToBase
        || buy_quote.pair.base_mint != issuer.mint
        || buy_quote.pair.quote_mint != quote_asset.mint
    {
        return None;
    }
    let input_usd = quote_asset.input_usd_for_atoms(buy_quote.in_amount_atoms);
    if !input_usd.is_finite() || input_usd <= 0.0 || buy_price <= 0.0 || oracle.price_usd <= 0.0 {
        return None;
    }
    let gross_edge_bps = premium_bps(oracle.price_usd, buy_price);
    let net_profit_bps = net_profit_bps(
        gross_edge_bps,
        oracle.confidence_bps,
        config.fee_safety_buffer_bps,
        network_fee.network_fee_usd,
        input_usd,
    );
    let min_edge_bps = rwa_pyth_discount_buy_min_edge_bps(config);
    if net_profit_bps < min_edge_bps {
        return None;
    }
    let output_units =
        atoms_to_ui(buy_quote.out_amount_atoms, issuer.decimals) * issuer.shares_per_token;
    let mark_to_pyth_usd = output_units * oracle.price_usd;
    let net_profit_usd = (mark_to_pyth_usd - input_usd).min(net_profit_bps / 10_000.0 * input_usd);
    let execution_legs = vec![execution_leg_from_quote(
        buy_quote,
        buy_quote.in_amount_atoms,
        Some(input_usd),
    )];
    let route_blocked_reason = execution_legs_route_blocked_reason(&execution_legs);
    let executable = config.execution_enabled && route_blocked_reason.is_none();
    Some(RwaOpportunity {
        ticker: asset.ticker.clone(),
        kind: RwaOpportunityKind::PythDiscountBuy,
        from_symbol: quote_asset.symbol.clone(),
        to_symbol: issuer.symbol.clone(),
        from_mint: quote_asset.mint.clone(),
        to_mint: issuer.mint.clone(),
        quote_asset: Some(quote_asset.symbol.clone()),
        pyth_fair_price_usd: oracle.price_usd,
        from_price_usd: Some(buy_price),
        to_price_usd: Some(oracle.price_usd),
        input_atoms: Some(buy_quote.in_amount_atoms),
        input_estimated_usd: Some(input_usd),
        expected_output_atoms: Some(buy_quote.out_amount_atoms),
        route: format!(
            "{} secondary buy via {} at ${:.6} normalized; mark-to-Pyth ${:.6}",
            issuer.symbol, buy_quote.pool, buy_price, oracle.price_usd
        ),
        gross_edge_bps,
        net_profit_bps,
        net_profit_usd,
        confidence_buffer_bps: oracle.confidence_bps,
        fee: buy_quote.fee.clone(),
        executable,
        execution_legs,
        blocked_reason: merge_blocked_reasons(
            (!config.execution_enabled).then(|| {
                "Pyth-discount accumulation is enabled but RWA live execution is disabled"
                    .to_string()
            }),
            route_blocked_reason,
        ),
    })
}

fn primary_routes_for_asset(
    config: &RwaConfig,
    asset: &RwaAssetConfig,
    oracle: Option<&PythOraclePrice>,
) -> Vec<RwaPrimaryRouteSnapshot> {
    let mut routes = Vec::new();
    let pyth_price_usd = oracle.map(|price| price.price_usd).unwrap_or_default();
    let oracle_gate_reason = match oracle {
        Some(price) if price.status == OracleStatus::Valid => None,
        Some(price) => Some(format!(
            "Pyth oracle {}; primary route is not tradable yet",
            oracle_rejection_reason(price)
        )),
        None => Some("Pyth oracle is missing; primary route is not tradable yet".to_string()),
    };
    if config.ondo_primary_enabled {
        let has_jupiter_key = env_value("JUPITER_API_KEY")
            .or_else(|| env_value("JUPITER_V2_API_KEY"))
            .is_some();
        let rfq_enabled = env_flag_default("RWA_ONDO_JUPITER_RFQ_EXECUTION_ENABLED", true);
        let external_builder = external_primary_builder_configured("ondo_jupiter_rfq");
        routes.push(RwaPrimaryRouteSnapshot {
            provider: "ondo_jupiter_rfq".to_string(),
            symbol: asset
                .ondo_symbol
                .clone()
                .if_empty_then(|| format!("{}on", asset.ticker)),
            mint: asset.ondo_mint.clone(),
            decimals: asset.ondo_decimals,
            shares_per_token: asset.ondo_shares_per_token,
            source: "Ondo GM primary mint/redeem is represented by Jupiter v2 RFQ routes and priced against Pyth/underlying fair value"
                .to_string(),
            price_usd: pyth_price_usd * asset.ondo_shares_per_token,
            normalized_price_usd: pyth_price_usd,
            mint_enabled: true,
            burn_enabled: true,
            rfq_mint_available: false,
            rfq_burn_available: false,
            rfq_mint_price_usd: None,
            rfq_burn_price_usd: None,
            rfq_mint_route: None,
            rfq_burn_route: None,
            non_rfq_mint_price_usd: None,
            non_rfq_burn_price_usd: None,
            non_rfq_mint_route: None,
            non_rfq_burn_route: None,
            external_builder_configured: external_builder,
            readiness: primary_route_readiness(
                oracle_gate_reason.is_none(),
                rfq_enabled,
                has_jupiter_key,
                external_builder,
                false,
            ),
            executable: oracle_gate_reason.is_none() && external_builder,
            reason: if let Some(reason) = oracle_gate_reason.as_ref() {
                format!("{reason}; Ondo Jupiter v2 RFQ mint/burn is configured and will be scored when the oracle is valid")
            } else if rfq_enabled && has_jupiter_key {
                "Jupiter v2 RFQ execution is configured; a live RFQ quote is still required before any Ondo primary mint/burn leg becomes executable"
                    .to_string()
            } else if external_builder {
                "ONDO_PRIMARY_BUILD_URL is configured; direct primary transaction builder can execute when a profitable route is found"
                    .to_string()
            } else if !rfq_enabled {
                "RWA_ONDO_JUPITER_RFQ_EXECUTION_ENABLED=false; Ondo primary mint/burn is visible but execution is disabled"
                    .to_string()
            } else {
                "JUPITER_API_KEY missing; Ondo primary mint/burn is visible but Jupiter v2 RFQ execution cannot run"
                    .to_string()
            },
        });
    }

    routes
}

fn annotate_primary_route_quote_availability(
    routes: &mut [RwaPrimaryRouteSnapshot],
    issuers: &[Option<&IssuerQuotes>],
) {
    for route in routes {
        let matching_issuers = issuers
            .iter()
            .flatten()
            .filter(|issuer| issuer.mint == route.mint)
            .copied()
            .collect::<Vec<_>>();
        if let Some(issuer) = matching_issuers.first() {
            let shares = issuer.shares_per_token.max(f64::EPSILON);
            if route.normalized_price_usd <= 0.0 || !route.normalized_price_usd.is_finite() {
                route.normalized_price_usd = route.price_usd / shares;
            }
        }
        for issuer in matching_issuers {
            let shares = issuer.shares_per_token.max(f64::EPSILON);
            if let Some(quote) = issuer.secondary_buy.as_ref() {
                update_primary_route_quote(
                    &mut route.non_rfq_mint_price_usd,
                    &mut route.non_rfq_mint_route,
                    quote,
                    issuer.decimals,
                    shares,
                    issuer.quote_usd_per_unit,
                    true,
                );
            }
            if let Some(quote) = issuer.secondary_sell.as_ref() {
                update_primary_route_quote(
                    &mut route.non_rfq_burn_price_usd,
                    &mut route.non_rfq_burn_route,
                    quote,
                    issuer.decimals,
                    shares,
                    issuer.quote_usd_per_unit,
                    false,
                );
            }
            if primary_rfq_execution_enabled(&route.provider) {
                if let Some(quote) = issuer.rfq_buy.as_ref() {
                    route.rfq_mint_available = true;
                    update_primary_route_quote(
                        &mut route.rfq_mint_price_usd,
                        &mut route.rfq_mint_route,
                        quote,
                        issuer.decimals,
                        shares,
                        issuer.quote_usd_per_unit,
                        true,
                    );
                }
                if let Some(quote) = issuer.rfq_sell.as_ref() {
                    route.rfq_burn_available = true;
                    update_primary_route_quote(
                        &mut route.rfq_burn_price_usd,
                        &mut route.rfq_burn_route,
                        quote,
                        issuer.decimals,
                        shares,
                        issuer.quote_usd_per_unit,
                        false,
                    );
                }
            }
        }
        route.external_builder_configured = external_primary_builder_configured(&route.provider);
        let has_jupiter_key = env_value("JUPITER_API_KEY")
            .or_else(|| env_value("JUPITER_V2_API_KEY"))
            .is_some();
        let rfq_ready = primary_rfq_execution_enabled(&route.provider)
            && has_jupiter_key
            && (route.rfq_mint_available || route.rfq_burn_available);
        route.readiness = primary_route_readiness(
            primary_route_oracle_gate_open(route),
            primary_rfq_execution_enabled(&route.provider),
            has_jupiter_key,
            route.external_builder_configured,
            route.rfq_mint_available || route.rfq_burn_available,
        );
        route.executable = primary_route_oracle_gate_open(route)
            && (route.external_builder_configured || rfq_ready);
        refresh_primary_route_reason(route, has_jupiter_key);
        annotate_non_rfq_primary_reason(route);
    }
}

fn update_primary_route_quote(
    price_slot: &mut Option<f64>,
    route_slot: &mut Option<String>,
    quote: &Quote,
    decimals: u8,
    shares_per_token: f64,
    quote_usd_per_unit: f64,
    prefer_lower_price: bool,
) {
    let Some(price) = stable_price_per_token_usd(quote, decimals, quote_usd_per_unit)
        .map(|price| price / shares_per_token)
    else {
        return;
    };
    let replace = match *price_slot {
        None => true,
        Some(current) if prefer_lower_price => price < current,
        Some(current) => price > current,
    };
    if replace {
        *price_slot = Some(price);
        *route_slot = Some(quote.pool.clone());
    }
}

fn annotate_non_rfq_primary_reason(route: &mut RwaPrimaryRouteSnapshot) {
    if route.reason.contains("non-RFQ secondary") {
        return;
    }
    let mut sides = Vec::new();
    if let Some(price) = route.non_rfq_mint_price_usd {
        sides.push(format!("mint-like buy at ${price:.6}"));
    }
    if let Some(price) = route.non_rfq_burn_price_usd {
        sides.push(format!("burn-like sell at ${price:.6}"));
    }
    if sides.is_empty() {
        return;
    }
    route.reason = format!(
        "{}; observed non-RFQ secondary route(s) {}, not primary mint/burn execution",
        route.reason,
        sides.join(" and ")
    );
}

fn refresh_primary_route_reason(route: &mut RwaPrimaryRouteSnapshot, has_jupiter_key: bool) {
    if route.readiness == RwaPrimaryRouteReadiness::OracleBlocked {
        return;
    }
    let provider_label = match route.provider.as_str() {
        "ondo_jupiter_rfq" | "ondo_primary" => "Ondo",
        _ => route.provider.as_str(),
    };
    route.reason = match route.readiness {
        RwaPrimaryRouteReadiness::RfqReady => format!(
            "{provider_label} Jupiter v2 RFQ mint/burn is live; primary legs can execute when a profitable route is fundable"
        ),
        RwaPrimaryRouteReadiness::ExternalBuilderReady => format!(
            "{provider_label} direct primary builder is configured; primary legs can execute when a profitable route is found"
        ),
        RwaPrimaryRouteReadiness::WaitingForRfq => format!(
            "Jupiter v2 RFQ execution is configured; a live RFQ quote is still required before any {provider_label} primary mint/burn leg becomes executable"
        ),
        RwaPrimaryRouteReadiness::AuthMissing if has_jupiter_key => format!(
            "{provider_label} primary route is configured but missing required external builder credentials"
        ),
        RwaPrimaryRouteReadiness::AuthMissing => format!(
            "JUPITER_API_KEY missing; {provider_label} primary mint/burn is visible but Jupiter v2 RFQ execution cannot run"
        ),
        RwaPrimaryRouteReadiness::Disabled => format!(
            "{provider_label} primary mint/burn is visible but execution is disabled"
        ),
        RwaPrimaryRouteReadiness::OracleBlocked => route.reason.clone(),
    };
}

fn primary_route_readiness(
    oracle_gate_open: bool,
    rfq_enabled: bool,
    has_jupiter_key: bool,
    external_builder: bool,
    rfq_available: bool,
) -> RwaPrimaryRouteReadiness {
    if !oracle_gate_open {
        return RwaPrimaryRouteReadiness::OracleBlocked;
    }
    if rfq_enabled && has_jupiter_key && rfq_available {
        return RwaPrimaryRouteReadiness::RfqReady;
    }
    if external_builder {
        return RwaPrimaryRouteReadiness::ExternalBuilderReady;
    }
    if rfq_enabled && has_jupiter_key {
        return RwaPrimaryRouteReadiness::WaitingForRfq;
    }
    if rfq_enabled {
        return RwaPrimaryRouteReadiness::AuthMissing;
    }
    RwaPrimaryRouteReadiness::Disabled
}

#[derive(Debug, Clone, Copy)]
struct PrimaryMintSellAmounts {
    quote_input_atoms: u128,
    issuer_sell_input_atoms: u128,
    stable_output_atoms: u128,
}

#[derive(Debug, Clone, Copy)]
struct PrimaryBuyBurnAmounts {
    quote_input_atoms: u128,
    issuer_burn_input_atoms: u128,
    stable_output_atoms: u128,
}

fn primary_route_opportunities(
    config: &RwaConfig,
    asset: &RwaAssetConfig,
    oracle: &PythOraclePrice,
    quote_asset: &RwaQuoteAsset,
    issuers: &[Option<&IssuerQuotes>],
    primary_routes: &[RwaPrimaryRouteSnapshot],
    network_fee: &FeeBreakdown,
) -> Vec<RwaOpportunity> {
    let mut opportunities = Vec::new();
    let trade_usd = asset
        .max_trade_usd
        .unwrap_or(config.max_trade_usd_per_asset);
    for route in primary_routes {
        let Some(issuer) = issuers
            .iter()
            .flatten()
            .find(|issuer| issuer.mint == route.mint)
        else {
            continue;
        };
        let sequential_enabled = rwa_sequential_roundtrip_enabled();
        let quote_symbol = quote_asset.symbol.clone();
        let quote_mint = quote_asset.mint.clone();
        let quote_decimals = quote_asset.decimals;
        let quote_input_atoms = quote_asset.input_atoms_for_usd(trade_usd);
        let rfq_primary_enabled = primary_rfq_execution_enabled(&route.provider);
        let external_primary_enabled = external_primary_builder_configured(&route.provider);

        if route.mint_enabled {
            if let (Some(sell_price), Some(sell_quote)) = (
                issuer.normalized_secondary_sell_price_usd(),
                issuer.secondary_sell.as_ref(),
            ) {
                let mint_quote = issuer.rfq_buy.as_ref().filter(|quote| {
                    rfq_primary_enabled
                        && quote.pair.quote_mint == quote_mint
                        && is_jupiter_v2_rfq_route(&quote.pool)
                });
                let mut input_atoms = quote_input_atoms;
                let mut input_usd = quote_asset.input_usd_for_atoms(input_atoms);
                let mut expected_output_atoms = sell_quote.out_amount_atoms;
                let mint_amounts = primary_mint_sell_amounts(mint_quote, sell_quote);
                let mut execution_legs = if let Some(amounts) = mint_amounts {
                    input_atoms = amounts.quote_input_atoms;
                    input_usd = quote_asset.input_usd_for_atoms(input_atoms);
                    expected_output_atoms = amounts.stable_output_atoms;
                    primary_mint_sell_execution_legs(mint_quote, sell_quote, input_usd)
                } else {
                    Vec::new()
                };
                if execution_legs.is_empty() && external_primary_enabled {
                    execution_legs = external_primary_mint_sell_execution_legs(
                        route,
                        quote_mint.clone(),
                        quote_decimals,
                        quote_input_atoms,
                        sell_quote,
                        input_usd,
                    );
                }
                if let Some(first_leg) = execution_legs.first() {
                    input_atoms = first_leg.input_atoms;
                    input_usd = quote_asset.input_usd_for_atoms(input_atoms);
                }
                let route_blocked_reason = execution_legs_route_blocked_reason(&execution_legs);
                let executable = config.execution_enabled
                    && sequential_enabled
                    && !execution_legs.is_empty()
                    && route_blocked_reason.is_none();
                let base_blocked_reason = if executable {
                    None
                } else if external_primary_enabled {
                    Some(format!(
                        "{}; external primary mint builder did not produce executable legs",
                        route.reason
                    ))
                } else if primary_rfq_supported(&route.provider) && mint_quote.is_none() {
                    Some(format!(
                        "{}; no executable Jupiter v2 RFQ mint leg is currently available for {}",
                        route.reason, quote_symbol
                    ))
                } else {
                    Some(format!(
                        "{}; waiting for a live Jupiter v2 RFQ mint leg for {} or configured external primary builder",
                        route.reason, quote_symbol
                    ))
                };
                let blocked_reason = merge_blocked_reasons(
                    base_blocked_reason,
                    (!sequential_enabled).then(|| {
                        "sequential RWA primary mint/sell execution is disabled".to_string()
                    }),
                );
                let blocked_reason = merge_blocked_reasons(blocked_reason, route_blocked_reason);
                let primary_buy_price = normalized_primary_quote_price(route, issuer, mint_quote);
                let gross_edge_bps = premium_bps(sell_price, primary_buy_price);
                let net_profit_bps = net_profit_bps(
                    gross_edge_bps,
                    oracle.confidence_bps,
                    config.fee_safety_buffer_bps,
                    network_fee.network_fee_usd,
                    input_usd,
                );
                opportunities.push(RwaOpportunity {
                    ticker: asset.ticker.clone(),
                    kind: RwaOpportunityKind::PrimaryMintSell,
                    from_symbol: quote_symbol.clone(),
                    to_symbol: issuer.symbol.clone(),
                    from_mint: quote_mint.clone(),
                    to_mint: issuer.mint.clone(),
                    quote_asset: Some(quote_symbol.clone()),
                    pyth_fair_price_usd: oracle.price_usd,
                    from_price_usd: Some(primary_buy_price),
                    to_price_usd: Some(sell_price),
                    input_atoms: Some(input_atoms),
                    input_estimated_usd: Some(input_usd),
                    expected_output_atoms: Some(expected_output_atoms),
                    route: format!(
                        "{} primary mint at ${:.6} normalized -> secondary sell via {}",
                        route.provider, primary_buy_price, sell_quote.pool
                    ),
                    gross_edge_bps,
                    net_profit_bps,
                    net_profit_usd: net_profit_bps / 10_000.0 * input_usd,
                    confidence_buffer_bps: oracle.confidence_bps,
                    fee: sell_quote.fee.clone(),
                    executable,
                    execution_legs,
                    blocked_reason,
                });
            }
        }

        if route.burn_enabled {
            if let (Some(buy_price), Some(buy_quote)) = (
                issuer.normalized_secondary_buy_price_usd(),
                issuer.secondary_buy.as_ref(),
            ) {
                let burn_quote = issuer.rfq_sell.as_ref().filter(|quote| {
                    rfq_primary_enabled
                        && quote.pair.quote_mint == quote_mint
                        && is_jupiter_v2_rfq_route(&quote.pool)
                });
                let mut input_atoms = buy_quote.in_amount_atoms;
                let mut input_usd = quote_asset.input_usd_for_atoms(input_atoms);
                let mut expected_output_atoms = None;
                let burn_amounts = primary_buy_burn_amounts(buy_quote, burn_quote);
                let mut execution_legs = if let Some(amounts) = burn_amounts {
                    input_atoms = amounts.quote_input_atoms;
                    input_usd = quote_asset.input_usd_for_atoms(input_atoms);
                    expected_output_atoms = Some(amounts.stable_output_atoms);
                    primary_buy_burn_execution_legs(buy_quote, burn_quote, input_usd)
                } else {
                    Vec::new()
                };
                if execution_legs.is_empty() && external_primary_enabled {
                    execution_legs = external_primary_buy_burn_execution_legs(
                        route,
                        buy_quote,
                        quote_mint.clone(),
                        quote_decimals,
                        input_usd,
                    );
                }
                if let Some(first_leg) = execution_legs.first() {
                    input_atoms = first_leg.input_atoms;
                    input_usd = quote_asset.input_usd_for_atoms(input_atoms);
                }
                let route_blocked_reason = execution_legs_route_blocked_reason(&execution_legs);
                let executable = config.execution_enabled
                    && sequential_enabled
                    && !execution_legs.is_empty()
                    && route_blocked_reason.is_none();
                let base_blocked_reason = if executable {
                    None
                } else if external_primary_enabled {
                    Some(format!(
                        "{}; external primary burn builder did not produce executable legs",
                        route.reason
                    ))
                } else if primary_rfq_supported(&route.provider) && burn_quote.is_none() {
                    Some(format!(
                        "{}; no executable Jupiter v2 RFQ burn leg is currently available for {}",
                        route.reason, quote_symbol
                    ))
                } else {
                    Some(format!(
                        "{}; waiting for a live Jupiter v2 RFQ burn leg for {} or configured external primary builder",
                        route.reason, quote_symbol
                    ))
                };
                let blocked_reason = merge_blocked_reasons(
                    base_blocked_reason,
                    (!sequential_enabled).then(|| {
                        "sequential RWA primary buy/burn execution is disabled".to_string()
                    }),
                );
                let blocked_reason = merge_blocked_reasons(blocked_reason, route_blocked_reason);
                let primary_sell_price = normalized_primary_quote_price(route, issuer, burn_quote);
                let gross_edge_bps = premium_bps(primary_sell_price, buy_price);
                let net_profit_bps = net_profit_bps(
                    gross_edge_bps,
                    oracle.confidence_bps,
                    config.fee_safety_buffer_bps,
                    network_fee.network_fee_usd,
                    input_usd,
                );
                opportunities.push(RwaOpportunity {
                    ticker: asset.ticker.clone(),
                    kind: RwaOpportunityKind::PrimaryBuyBurn,
                    from_symbol: quote_symbol.clone(),
                    to_symbol: issuer.symbol.clone(),
                    from_mint: quote_mint.clone(),
                    to_mint: issuer.mint.clone(),
                    quote_asset: Some(quote_symbol.clone()),
                    pyth_fair_price_usd: oracle.price_usd,
                    from_price_usd: Some(buy_price),
                    to_price_usd: Some(primary_sell_price),
                    input_atoms: Some(input_atoms),
                    input_estimated_usd: Some(input_usd),
                    expected_output_atoms,
                    route: format!(
                        "secondary buy via {} -> {} primary burn at ${:.6} normalized",
                        buy_quote.pool, route.provider, primary_sell_price
                    ),
                    gross_edge_bps,
                    net_profit_bps,
                    net_profit_usd: net_profit_bps / 10_000.0 * input_usd,
                    confidence_buffer_bps: oracle.confidence_bps,
                    fee: buy_quote.fee.clone(),
                    executable,
                    execution_legs,
                    blocked_reason,
                });
            }
        }
    }
    opportunities
}

fn normalized_primary_quote_price(
    route: &RwaPrimaryRouteSnapshot,
    issuer: &IssuerQuotes,
    quote: Option<&Quote>,
) -> f64 {
    let shares = issuer.shares_per_token.max(f64::EPSILON);
    quote
        .and_then(|quote| {
            stable_price_per_token_usd(quote, issuer.decimals, issuer.quote_usd_per_unit)
        })
        .map(|price| price / shares)
        .unwrap_or_else(|| {
            if route.normalized_price_usd > 0.0 && route.normalized_price_usd.is_finite() {
                route.normalized_price_usd
            } else {
                route.price_usd / shares
            }
        })
}

fn primary_mint_sell_execution_legs(
    mint_quote: Option<&Quote>,
    sell_quote: &Quote,
    estimated_input_usd: f64,
) -> Vec<RwaExecutionLeg> {
    let Some(mint_quote) = mint_quote else {
        return Vec::new();
    };
    let Some(amounts) = primary_mint_sell_amounts(Some(mint_quote), sell_quote) else {
        return Vec::new();
    };
    vec![
        execution_leg_from_quote(
            mint_quote,
            amounts.quote_input_atoms,
            Some(estimated_input_usd),
        ),
        execution_leg_from_quote(
            sell_quote,
            amounts.issuer_sell_input_atoms,
            Some(estimated_input_usd),
        ),
    ]
}

fn primary_buy_burn_execution_legs(
    buy_quote: &Quote,
    burn_quote: Option<&Quote>,
    estimated_input_usd: f64,
) -> Vec<RwaExecutionLeg> {
    let Some(burn_quote) = burn_quote else {
        return Vec::new();
    };
    let Some(amounts) = primary_buy_burn_amounts(buy_quote, Some(burn_quote)) else {
        return Vec::new();
    };
    vec![
        execution_leg_from_quote(
            buy_quote,
            amounts.quote_input_atoms,
            Some(estimated_input_usd),
        ),
        execution_leg_from_quote(
            burn_quote,
            amounts.issuer_burn_input_atoms,
            Some(estimated_input_usd),
        ),
    ]
}

fn primary_mint_sell_amounts(
    mint_quote: Option<&Quote>,
    sell_quote: &Quote,
) -> Option<PrimaryMintSellAmounts> {
    let mint_quote = mint_quote?;
    let issuer_sell_input_atoms = mint_quote.out_amount_atoms.min(sell_quote.in_amount_atoms);
    let quote_input_atoms = if issuer_sell_input_atoms < mint_quote.out_amount_atoms {
        prorate_atoms(
            mint_quote.in_amount_atoms,
            issuer_sell_input_atoms,
            mint_quote.out_amount_atoms,
        )?
    } else {
        mint_quote.in_amount_atoms
    };
    let stable_output_atoms = if issuer_sell_input_atoms < sell_quote.in_amount_atoms {
        prorate_atoms(
            sell_quote.out_amount_atoms,
            issuer_sell_input_atoms,
            sell_quote.in_amount_atoms,
        )?
    } else {
        sell_quote.out_amount_atoms
    };
    Some(PrimaryMintSellAmounts {
        quote_input_atoms,
        issuer_sell_input_atoms,
        stable_output_atoms,
    })
}

fn primary_buy_burn_amounts(
    buy_quote: &Quote,
    burn_quote: Option<&Quote>,
) -> Option<PrimaryBuyBurnAmounts> {
    let burn_quote = burn_quote?;
    let issuer_burn_input_atoms = buy_quote.out_amount_atoms.min(burn_quote.in_amount_atoms);
    let quote_input_atoms = if issuer_burn_input_atoms < buy_quote.out_amount_atoms {
        prorate_atoms(
            buy_quote.in_amount_atoms,
            issuer_burn_input_atoms,
            buy_quote.out_amount_atoms,
        )?
    } else {
        buy_quote.in_amount_atoms
    };
    let stable_output_atoms = if issuer_burn_input_atoms < burn_quote.in_amount_atoms {
        prorate_atoms(
            burn_quote.out_amount_atoms,
            issuer_burn_input_atoms,
            burn_quote.in_amount_atoms,
        )?
    } else {
        burn_quote.out_amount_atoms
    };
    Some(PrimaryBuyBurnAmounts {
        quote_input_atoms,
        issuer_burn_input_atoms,
        stable_output_atoms,
    })
}

fn external_primary_mint_sell_execution_legs(
    route: &RwaPrimaryRouteSnapshot,
    quote_mint: String,
    quote_decimals: u8,
    quote_input_atoms: u128,
    sell_quote: &Quote,
    trade_usd: f64,
) -> Vec<RwaExecutionLeg> {
    vec![
        RwaExecutionLeg {
            input_mint: quote_mint,
            output_mint: route.mint.clone(),
            input_decimals: quote_decimals,
            output_decimals: route.decimals,
            side: SwapSide::QuoteToBase,
            input_atoms: quote_input_atoms,
            estimated_input_usd: Some(trade_usd),
            route: external_primary_leg_route(&route.provider, "mint"),
        },
        execution_leg_from_quote(sell_quote, sell_quote.in_amount_atoms, Some(trade_usd)),
    ]
}

fn external_primary_buy_burn_execution_legs(
    route: &RwaPrimaryRouteSnapshot,
    buy_quote: &Quote,
    quote_mint: String,
    quote_decimals: u8,
    trade_usd: f64,
) -> Vec<RwaExecutionLeg> {
    vec![
        execution_leg_from_quote(buy_quote, buy_quote.in_amount_atoms, Some(trade_usd)),
        RwaExecutionLeg {
            input_mint: route.mint.clone(),
            output_mint: quote_mint,
            input_decimals: route.decimals,
            output_decimals: quote_decimals,
            side: SwapSide::BaseToQuote,
            input_atoms: buy_quote.out_amount_atoms,
            estimated_input_usd: Some(trade_usd),
            route: external_primary_leg_route(&route.provider, "burn"),
        },
    ]
}

fn execution_leg_from_quote(
    quote: &Quote,
    input_atoms: u128,
    estimated_input_usd: Option<f64>,
) -> RwaExecutionLeg {
    let (input_mint, output_mint, input_decimals, output_decimals) = match quote.side {
        SwapSide::QuoteToBase => (
            quote.pair.quote_mint.clone(),
            quote.pair.base_mint.clone(),
            quote.pair.quote_decimals,
            quote.pair.base_decimals,
        ),
        SwapSide::BaseToQuote => (
            quote.pair.base_mint.clone(),
            quote.pair.quote_mint.clone(),
            quote.pair.base_decimals,
            quote.pair.quote_decimals,
        ),
    };
    RwaExecutionLeg {
        input_mint,
        output_mint,
        input_decimals,
        output_decimals,
        side: quote.side,
        input_atoms,
        estimated_input_usd,
        route: quote.pool.clone(),
    }
}

fn execution_legs_route_blocked_reason(legs: &[RwaExecutionLeg]) -> Option<String> {
    let mut providers = legs
        .iter()
        .filter_map(|leg| quote_only_live_provider(&leg.route))
        .collect::<Vec<_>>();
    providers.sort();
    providers.dedup();
    if providers.is_empty() {
        return None;
    }
    Some(format!(
        "RWA live execution disabled for quote-only route provider(s): {}; enable provider live-build only after swap-build reliability is verified",
        providers.join(", ")
    ))
}

fn quote_only_live_provider(route: &str) -> Option<&'static str> {
    let provider = route.split_once("-route:")?.0;
    match provider {
        "openocean" if !env_flag_default("RWA_OPENOCEAN_LIVE_BUILD_ENABLED", false) => {
            Some("openocean")
        }
        _ => None,
    }
}

fn merge_blocked_reasons(first: Option<String>, second: Option<String>) -> Option<String> {
    match (first, second) {
        (Some(first), Some(second)) if !first.is_empty() && !second.is_empty() => {
            Some(format!("{first}; {second}"))
        }
        (Some(first), _) if !first.is_empty() => Some(first),
        (_, Some(second)) if !second.is_empty() => Some(second),
        _ => None,
    }
}

fn external_primary_leg_route(provider: &str, action: &str) -> String {
    format!("external-primary:{provider}:{action}")
}

fn external_primary_builder_configured(provider: &str) -> bool {
    let (url_env, enabled_env) = match provider {
        "ondo_jupiter_rfq" | "ondo_primary" => {
            ("ONDO_PRIMARY_BUILD_URL", "ONDO_PRIMARY_EXECUTION_ENABLED")
        }
        _ => return false,
    };
    let has_url = env_value(url_env).is_some();
    has_url && env_flag_default(enabled_env, true)
}

fn primary_rfq_supported(provider: &str) -> bool {
    matches!(provider, "ondo_jupiter_rfq")
}

fn primary_route_oracle_gate_open(route: &RwaPrimaryRouteSnapshot) -> bool {
    !route.reason.contains("primary route is not tradable yet")
}

fn primary_rfq_execution_enabled(provider: &str) -> bool {
    match provider {
        "ondo_jupiter_rfq" => env_flag_default("RWA_ONDO_JUPITER_RFQ_EXECUTION_ENABLED", true),
        _ => false,
    }
}

fn is_jupiter_v2_rfq_route(route: &str) -> bool {
    let lower = route.to_ascii_lowercase();
    if !lower.starts_with("jupiter-v2-order:") {
        return false;
    }
    let parts = lower.split(':').collect::<Vec<_>>();
    parts.get(1).is_some_and(|part| part.contains("rfq"))
        || parts.get(2).is_some_and(|part| *part == "jupiterz")
}

fn inventory_rebalance_opportunity(
    config: &RwaConfig,
    asset: &RwaAssetConfig,
    oracle: &PythOraclePrice,
    quote_asset: &RwaQuoteAsset,
    rich: &IssuerQuotes,
    cheap: &IssuerQuotes,
    rich_sell_norm: f64,
    cheap_buy_norm: f64,
    network_fee: &FeeBreakdown,
) -> RwaOpportunity {
    let amounts = match (rich.best_sell.as_ref(), cheap.best_buy.as_ref()) {
        (Some(sell_quote), Some(buy_quote)) => {
            inventory_rebalance_amounts(quote_asset, sell_quote, buy_quote)
        }
        _ => None,
    };
    let input_usd = amounts
        .as_ref()
        .map(|amounts| amounts.input_usd)
        .unwrap_or_else(|| {
            asset
                .max_trade_usd
                .unwrap_or(config.max_trade_usd_per_asset)
        });
    let gross_edge_bps = premium_bps(rich_sell_norm, cheap_buy_norm);
    let net_profit_bps = net_profit_bps(
        gross_edge_bps,
        oracle.confidence_bps,
        config.fee_safety_buffer_bps,
        network_fee.network_fee_usd,
        input_usd,
    );
    let mut execution_legs = Vec::new();
    if let (Some(amounts), Some(sell_quote), Some(buy_quote)) = (
        amounts.as_ref(),
        rich.best_sell.as_ref(),
        cheap.best_buy.as_ref(),
    ) {
        execution_legs.push(execution_leg_from_quote(
            sell_quote,
            amounts.sell_input_atoms,
            Some(amounts.input_usd),
        ));
        execution_legs.push(execution_leg_from_quote(
            buy_quote,
            amounts.buy_input_atoms,
            Some(amounts.input_usd),
        ));
    }
    let sequential_enabled = rwa_sequential_rebalance_enabled();
    let route_blocked_reason = execution_legs_route_blocked_reason(&execution_legs);
    RwaOpportunity {
        ticker: asset.ticker.clone(),
        kind: RwaOpportunityKind::NeutralInventoryRebalance,
        from_symbol: rich.symbol.clone(),
        to_symbol: cheap.symbol.clone(),
        from_mint: rich.mint.clone(),
        to_mint: cheap.mint.clone(),
        quote_asset: Some(quote_asset.symbol.clone()),
        pyth_fair_price_usd: oracle.price_usd,
        from_price_usd: Some(rich_sell_norm),
        to_price_usd: Some(cheap_buy_norm),
        input_atoms: amounts.as_ref().map(|amounts| amounts.sell_input_atoms),
        input_estimated_usd: Some(input_usd),
        expected_output_atoms: amounts
            .as_ref()
            .map(|amounts| amounts.expected_output_atoms),
        route: format!(
            "{} sell via {}; {} buy via {}",
            rich.symbol,
            rich.best_sell
                .as_ref()
                .map(|quote| quote.pool.as_str())
                .unwrap_or("missing"),
            cheap.symbol,
            cheap
                .best_buy
                .as_ref()
                .map(|quote| quote.pool.as_str())
                .unwrap_or("missing")
        ),
        gross_edge_bps,
        net_profit_bps,
        net_profit_usd: net_profit_bps / 10_000.0 * input_usd,
        confidence_buffer_bps: oracle.confidence_bps,
        fee: rich
            .best_sell
            .as_ref()
            .map(|quote| quote.fee.clone())
            .unwrap_or_else(|| network_fee.clone()),
        executable: config.execution_enabled
            && sequential_enabled
            && execution_legs.len() == 2
            && route_blocked_reason.is_none(),
        execution_legs,
        blocked_reason: merge_blocked_reasons(
            (!sequential_enabled)
                .then(|| "sequential RWA inventory rebalance execution is disabled".to_string()),
            route_blocked_reason,
        ),
    }
}

struct InventoryRebalanceAmounts {
    sell_input_atoms: u128,
    buy_input_atoms: u128,
    expected_output_atoms: u128,
    input_usd: f64,
}

fn inventory_rebalance_amounts(
    quote_asset: &RwaQuoteAsset,
    sell_quote: &Quote,
    buy_quote: &Quote,
) -> Option<InventoryRebalanceAmounts> {
    if sell_quote.side != SwapSide::BaseToQuote
        || buy_quote.side != SwapSide::QuoteToBase
        || sell_quote.pair.quote_mint != buy_quote.pair.quote_mint
    {
        return None;
    }
    let buy_input_atoms = sell_quote.out_amount_atoms.min(buy_quote.in_amount_atoms);
    if buy_input_atoms == 0 {
        return None;
    }
    let sell_input_atoms = if buy_input_atoms < sell_quote.out_amount_atoms {
        prorate_atoms(
            sell_quote.in_amount_atoms,
            buy_input_atoms,
            sell_quote.out_amount_atoms,
        )
        .unwrap_or(sell_quote.in_amount_atoms)
    } else {
        sell_quote.in_amount_atoms
    };
    let expected_output_atoms = if buy_input_atoms < buy_quote.in_amount_atoms {
        prorate_atoms(
            buy_quote.out_amount_atoms,
            buy_input_atoms,
            buy_quote.in_amount_atoms,
        )
        .unwrap_or(buy_quote.out_amount_atoms)
    } else {
        buy_quote.out_amount_atoms
    };
    Some(InventoryRebalanceAmounts {
        sell_input_atoms,
        buy_input_atoms,
        expected_output_atoms,
        input_usd: quote_asset.input_usd_for_atoms(buy_input_atoms),
    })
}

fn direct_swap_opportunity(
    config: &RwaConfig,
    asset: &RwaAssetConfig,
    oracle: &PythOraclePrice,
    quote: &Quote,
    from_symbol: &str,
    to_symbol: &str,
    gross_edge_bps: f64,
    network_fee: &FeeBreakdown,
) -> RwaOpportunity {
    let input_shares_per_token = if quote.pair.base_mint == asset.ondo_mint {
        asset.ondo_shares_per_token
    } else {
        1.0
    };
    let input_estimated_usd = atoms_to_ui(quote.in_amount_atoms, quote.pair.base_decimals)
        * input_shares_per_token
        * oracle.price_usd;
    let net_profit_bps = net_profit_bps(
        gross_edge_bps,
        oracle.confidence_bps,
        config.fee_safety_buffer_bps,
        network_fee.network_fee_usd,
        input_estimated_usd,
    );
    let execution_legs = vec![execution_leg_from_quote(
        quote,
        quote.in_amount_atoms,
        Some(input_estimated_usd),
    )];
    let route_blocked_reason = execution_legs_route_blocked_reason(&execution_legs);
    RwaOpportunity {
        ticker: asset.ticker.clone(),
        kind: RwaOpportunityKind::DirectIssuerSwap,
        from_symbol: from_symbol.to_string(),
        to_symbol: to_symbol.to_string(),
        from_mint: quote.pair.base_mint.clone(),
        to_mint: quote.pair.quote_mint.clone(),
        quote_asset: None,
        pyth_fair_price_usd: oracle.price_usd,
        from_price_usd: None,
        to_price_usd: None,
        input_atoms: Some(quote.in_amount_atoms),
        input_estimated_usd: Some(input_estimated_usd),
        expected_output_atoms: Some(quote.out_amount_atoms),
        route: quote.pool.clone(),
        gross_edge_bps,
        net_profit_bps,
        net_profit_usd: net_profit_bps / 10_000.0 * input_estimated_usd,
        confidence_buffer_bps: oracle.confidence_bps,
        fee: quote.fee.clone(),
        executable: (config.emit_execution_candidates || config.execution_enabled)
            && route_blocked_reason.is_none(),
        execution_legs,
        blocked_reason: merge_blocked_reasons(
            (!config.emit_execution_candidates && !config.execution_enabled).then(|| {
                "RWA execution candidates are disabled; enable rwa.execution_enabled for live direct issuer swaps".to_string()
            }),
            route_blocked_reason,
        ),
    }
}

fn stable_funded_direct_roundtrip_opportunity(
    config: &RwaConfig,
    asset: &RwaAssetConfig,
    oracle: &PythOraclePrice,
    quote_asset: &RwaQuoteAsset,
    from_issuer: &ConfiguredIssuer,
    to_issuer: &ConfiguredIssuer,
    buy_quote: &Quote,
    direct_quote: &Quote,
    sell_quote: &Quote,
    network_fee: &FeeBreakdown,
) -> Option<RwaOpportunity> {
    if buy_quote.side != SwapSide::QuoteToBase
        || direct_quote.side != SwapSide::BaseToQuote
        || sell_quote.side != SwapSide::BaseToQuote
        || buy_quote.pair.base_mint != from_issuer.mint
        || buy_quote.pair.quote_mint != quote_asset.mint
        || direct_quote.pair.base_mint != from_issuer.mint
        || direct_quote.pair.quote_mint != to_issuer.mint
        || sell_quote.pair.base_mint != to_issuer.mint
        || sell_quote.pair.quote_mint != quote_asset.mint
    {
        return None;
    }

    let mut direct_input_atoms = buy_quote.out_amount_atoms.min(direct_quote.in_amount_atoms);
    if direct_quote.out_amount_atoms > sell_quote.in_amount_atoms {
        direct_input_atoms = direct_input_atoms.min(prorate_atoms(
            direct_quote.in_amount_atoms,
            sell_quote.in_amount_atoms,
            direct_quote.out_amount_atoms,
        )?);
    }
    if direct_input_atoms == 0 {
        return None;
    }

    let stable_input_atoms = prorate_atoms(
        buy_quote.in_amount_atoms,
        direct_input_atoms,
        buy_quote.out_amount_atoms,
    )?;
    let direct_output_atoms = prorate_atoms(
        direct_quote.out_amount_atoms,
        direct_input_atoms,
        direct_quote.in_amount_atoms,
    )?;
    let sell_input_atoms = direct_output_atoms.min(sell_quote.in_amount_atoms);
    let stable_output_atoms = prorate_atoms(
        sell_quote.out_amount_atoms,
        sell_input_atoms,
        sell_quote.in_amount_atoms,
    )?;
    let input_usd =
        atoms_to_ui(stable_input_atoms, quote_asset.decimals) * quote_asset.usd_per_unit;
    let output_usd =
        atoms_to_ui(stable_output_atoms, quote_asset.decimals) * quote_asset.usd_per_unit;
    if !input_usd.is_finite() || !output_usd.is_finite() || input_usd <= 0.0 || output_usd <= 0.0 {
        return None;
    }

    let gross_edge_bps = premium_bps(output_usd, input_usd);
    let route_network_fee_usd = network_fee.network_fee_usd * 3.0;
    let net_profit_bps = net_profit_bps(
        gross_edge_bps,
        oracle.confidence_bps,
        config.fee_safety_buffer_bps,
        route_network_fee_usd,
        input_usd,
    );
    let sequential_enabled = rwa_sequential_roundtrip_enabled();
    let mut fee = buy_quote.fee.clone();
    merge_quote_fee(&mut fee, &direct_quote.fee);
    merge_quote_fee(&mut fee, &sell_quote.fee);
    fee.network_fee_usd = fee.network_fee_usd.max(route_network_fee_usd);
    let execution_legs = vec![
        execution_leg_from_quote(buy_quote, stable_input_atoms, Some(input_usd)),
        execution_leg_from_quote(direct_quote, direct_input_atoms, Some(input_usd)),
        execution_leg_from_quote(sell_quote, sell_input_atoms, Some(input_usd)),
    ];
    let route_blocked_reason = execution_legs_route_blocked_reason(&execution_legs);

    Some(RwaOpportunity {
        ticker: asset.ticker.clone(),
        kind: RwaOpportunityKind::StableFundedDirectIssuerRoundtrip,
        from_symbol: quote_asset.symbol.clone(),
        to_symbol: quote_asset.symbol.clone(),
        from_mint: quote_asset.mint.clone(),
        to_mint: quote_asset.mint.clone(),
        quote_asset: Some(quote_asset.symbol.clone()),
        pyth_fair_price_usd: oracle.price_usd,
        from_price_usd: stable_price_per_token_usd(
            buy_quote,
            from_issuer.decimals,
            quote_asset.usd_per_unit,
        )
        .map(|price| price / from_issuer.shares_per_token.max(f64::EPSILON)),
        to_price_usd: stable_price_per_token_usd(
            sell_quote,
            to_issuer.decimals,
            quote_asset.usd_per_unit,
        )
        .map(|price| price / to_issuer.shares_per_token.max(f64::EPSILON)),
        input_atoms: Some(stable_input_atoms),
        input_estimated_usd: Some(input_usd),
        expected_output_atoms: Some(stable_output_atoms),
        route: format!(
            "{} buy via {}; {} -> {} via {}; {} sell via {}",
            from_issuer.symbol,
            buy_quote.pool,
            from_issuer.symbol,
            to_issuer.symbol,
            direct_quote.pool,
            to_issuer.symbol,
            sell_quote.pool
        ),
        gross_edge_bps,
        net_profit_bps,
        net_profit_usd: net_profit_bps / 10_000.0 * input_usd,
        confidence_buffer_bps: oracle.confidence_bps,
        fee,
        executable: config.execution_enabled
            && sequential_enabled
            && route_blocked_reason.is_none(),
        execution_legs,
        blocked_reason: merge_blocked_reasons(
            (!sequential_enabled)
                .then(|| "sequential RWA direct round-trip execution is disabled".to_string()),
            route_blocked_reason,
        ),
    })
}

fn stable_funded_direct_accumulation_opportunity(
    config: &RwaConfig,
    asset: &RwaAssetConfig,
    oracle: &PythOraclePrice,
    quote_asset: &RwaQuoteAsset,
    from_issuer: &ConfiguredIssuer,
    to_issuer: &ConfiguredIssuer,
    buy_quote: &Quote,
    direct_quote: &Quote,
    network_fee: &FeeBreakdown,
) -> Option<RwaOpportunity> {
    if !rwa_pyth_discount_buy_enabled(config)
        || buy_quote.side != SwapSide::QuoteToBase
        || direct_quote.side != SwapSide::BaseToQuote
        || buy_quote.pair.base_mint != from_issuer.mint
        || buy_quote.pair.quote_mint != quote_asset.mint
        || direct_quote.pair.base_mint != from_issuer.mint
        || direct_quote.pair.quote_mint != to_issuer.mint
    {
        return None;
    }

    let direct_input_atoms = buy_quote.out_amount_atoms.min(direct_quote.in_amount_atoms);
    if direct_input_atoms == 0 {
        return None;
    }
    let stable_input_atoms = prorate_atoms(
        buy_quote.in_amount_atoms,
        direct_input_atoms,
        buy_quote.out_amount_atoms,
    )?;
    let direct_output_atoms = prorate_atoms(
        direct_quote.out_amount_atoms,
        direct_input_atoms,
        direct_quote.in_amount_atoms,
    )?;
    let input_usd =
        atoms_to_ui(stable_input_atoms, quote_asset.decimals) * quote_asset.usd_per_unit;
    let mark_to_pyth_usd = atoms_to_ui(direct_output_atoms, to_issuer.decimals)
        * to_issuer.shares_per_token
        * oracle.price_usd;
    if !input_usd.is_finite()
        || !mark_to_pyth_usd.is_finite()
        || input_usd <= 0.0
        || mark_to_pyth_usd <= 0.0
    {
        return None;
    }

    let gross_edge_bps = premium_bps(mark_to_pyth_usd, input_usd);
    let route_network_fee_usd = network_fee.network_fee_usd * 2.0;
    let net_profit_bps = net_profit_bps(
        gross_edge_bps,
        oracle.confidence_bps,
        config.fee_safety_buffer_bps,
        route_network_fee_usd,
        input_usd,
    );
    if net_profit_bps < rwa_pyth_discount_buy_min_edge_bps(config) {
        return None;
    }

    let mut fee = buy_quote.fee.clone();
    merge_quote_fee(&mut fee, &direct_quote.fee);
    fee.network_fee_usd = fee.network_fee_usd.max(route_network_fee_usd);
    let execution_legs = vec![
        execution_leg_from_quote(buy_quote, stable_input_atoms, Some(input_usd)),
        execution_leg_from_quote(direct_quote, direct_input_atoms, Some(input_usd)),
    ];
    let route_blocked_reason = execution_legs_route_blocked_reason(&execution_legs);
    Some(RwaOpportunity {
        ticker: asset.ticker.clone(),
        kind: RwaOpportunityKind::StableFundedDirectIssuerAccumulation,
        from_symbol: quote_asset.symbol.clone(),
        to_symbol: to_issuer.symbol.clone(),
        from_mint: quote_asset.mint.clone(),
        to_mint: to_issuer.mint.clone(),
        quote_asset: Some(quote_asset.symbol.clone()),
        pyth_fair_price_usd: oracle.price_usd,
        from_price_usd: stable_price_per_token_usd(
            buy_quote,
            from_issuer.decimals,
            quote_asset.usd_per_unit,
        )
        .map(|price| price / from_issuer.shares_per_token.max(f64::EPSILON)),
        to_price_usd: Some(oracle.price_usd),
        input_atoms: Some(stable_input_atoms),
        input_estimated_usd: Some(input_usd),
        expected_output_atoms: Some(direct_output_atoms),
        route: format!(
            "{} buy via {}; {} -> {} via {}; hold {} mark-to-Pyth",
            from_issuer.symbol,
            buy_quote.pool,
            from_issuer.symbol,
            to_issuer.symbol,
            direct_quote.pool,
            to_issuer.symbol
        ),
        gross_edge_bps,
        net_profit_bps,
        net_profit_usd: net_profit_bps / 10_000.0 * input_usd,
        confidence_buffer_bps: oracle.confidence_bps,
        fee,
        executable: config.execution_enabled && route_blocked_reason.is_none(),
        execution_legs,
        blocked_reason: merge_blocked_reasons(
            (!config.execution_enabled).then(|| {
                "stable-funded direct issuer accumulation is visible but RWA live execution is disabled"
                    .to_string()
            }),
            route_blocked_reason,
        ),
    })
}

fn prorate_atoms(amount: u128, numerator: u128, denominator: u128) -> Option<u128> {
    if amount == 0 || numerator == 0 || denominator == 0 {
        return None;
    }
    amount
        .checked_mul(numerator)
        .map(|value| value / denominator)
        .filter(|value| *value > 0)
}

fn merge_quote_fee(target: &mut FeeBreakdown, extra: &FeeBreakdown) {
    target.lp_fee_atoms = target.lp_fee_atoms.saturating_add(extra.lp_fee_atoms);
    target.protocol_fee_atoms = target
        .protocol_fee_atoms
        .saturating_add(extra.protocol_fee_atoms);
    target.host_fee_atoms = target.host_fee_atoms.saturating_add(extra.host_fee_atoms);
    target.priority_fee_lamports = target
        .priority_fee_lamports
        .saturating_add(extra.priority_fee_lamports);
    target.jito_tip_lamports = target
        .jito_tip_lamports
        .saturating_add(extra.jito_tip_lamports);
    target.ata_rent_lamports = target
        .ata_rent_lamports
        .saturating_add(extra.ata_rent_lamports);
    target.slippage_bps += extra.slippage_bps;
    target.safety_buffer_bps += extra.safety_buffer_bps;
    target.network_fee_usd += extra.network_fee_usd;
}

fn better_issuer(current: Option<IssuerQuotes>, next: IssuerQuotes) -> Option<IssuerQuotes> {
    match current {
        Some(current) => {
            let current_score =
                current.best_buy.is_some() as u8 + current.best_sell.is_some() as u8;
            let next_score = next.best_buy.is_some() as u8 + next.best_sell.is_some() as u8;
            if next_score > current_score {
                Some(next)
            } else {
                Some(current)
            }
        }
        None => Some(next),
    }
}

#[cfg(test)]
fn stable_price_per_token(quote: &Quote, token_decimals: u8) -> Option<f64> {
    stable_price_per_token_usd(quote, token_decimals, 1.0)
}

fn stable_price_per_token_usd(
    quote: &Quote,
    token_decimals: u8,
    quote_usd_per_unit: f64,
) -> Option<f64> {
    match quote.side {
        SwapSide::QuoteToBase => {
            let stable =
                atoms_to_ui(quote.in_amount_atoms, quote.pair.quote_decimals) * quote_usd_per_unit;
            let token = atoms_to_ui(quote.out_amount_atoms, token_decimals);
            (stable > 0.0 && token > 0.0).then_some(stable / token)
        }
        SwapSide::BaseToQuote => {
            let token = atoms_to_ui(quote.in_amount_atoms, token_decimals);
            let stable =
                atoms_to_ui(quote.out_amount_atoms, quote.pair.quote_decimals) * quote_usd_per_unit;
            (stable > 0.0 && token > 0.0).then_some(stable / token)
        }
    }
}

fn premium_bps(market: f64, fair: f64) -> f64 {
    if fair <= 0.0 {
        return 0.0;
    }
    (market / fair - 1.0) * 10_000.0
}

fn net_profit_bps(
    gross_edge_bps: f64,
    confidence_bps: f64,
    safety_buffer_bps: f64,
    network_fee_usd: f64,
    trade_usd: f64,
) -> f64 {
    let network_bps = if trade_usd > 0.0 {
        network_fee_usd / trade_usd * 10_000.0
    } else {
        0.0
    };
    gross_edge_bps - confidence_bps - safety_buffer_bps - network_bps
}

fn short_mint(mint: &str) -> String {
    if mint.len() <= 8 {
        return mint.to_string();
    }
    format!("{}...{}", &mint[..4], &mint[mint.len() - 4..])
}

trait EmptyStringExt {
    fn if_empty_then<F: FnOnce() -> String>(self, fallback: F) -> String;
}

impl EmptyStringExt for String {
    fn if_empty_then<F: FnOnce() -> String>(self, fallback: F) -> String {
        if self.is_empty() {
            fallback()
        } else {
            self
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn test_env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("test env lock poisoned")
    }

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(previous) = &self.previous {
                std::env::set_var(self.key, previous);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }

    fn set_test_env_var(key: &'static str, value: &str) -> EnvVarGuard {
        let previous = std::env::var(key).ok();
        std::env::set_var(key, value);
        EnvVarGuard { key, previous }
    }

    fn test_rwa_quote(
        base_mint: &str,
        quote_mint: &str,
        side: SwapSide,
        in_amount_atoms: u128,
        out_amount_atoms: u128,
        pool: &str,
    ) -> Quote {
        Quote {
            venue: Venue::Jupiter,
            pool: pool.to_string(),
            pair: TokenPair {
                base_mint: base_mint.to_string(),
                quote_mint: quote_mint.to_string(),
                base_decimals: 8,
                quote_decimals: 6,
            },
            side,
            in_amount_atoms,
            out_amount_atoms,
            effective_price: 1.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        }
    }

    fn test_asset(ticker: &str, xstock_mint: &str, ondo_mint: &str) -> RwaAssetRuntime {
        RwaAssetRuntime {
            asset: RwaAssetConfig {
                ticker: ticker.to_string(),
                enabled: true,
                pyth_feed_id: format!("{:0>64}", ticker.len()),
                xstock_symbol: format!("{ticker}x"),
                xstock_mint: xstock_mint.to_string(),
                xstock_decimals: 8,
                xstock_token_program: String::new(),
                ondo_symbol: format!("{ticker}on"),
                ondo_mint: ondo_mint.to_string(),
                ondo_decimals: 9,
                ondo_token_program: String::new(),
                flash_symbol: String::new(),
                flash_mint: String::new(),
                flash_decimals: 0,
                flash_token_program: String::new(),
                flash_mint_burn_enabled: false,
                flash_shares_per_token: 1.0,
                ondo_shares_per_token: 1.0,
                max_trade_usd: Some(25.0),
                min_net_profit_bps: None,
                max_oracle_confidence_bps: None,
                max_oracle_age_ms: None,
                regular_hours_only: None,
                source: String::new(),
            },
        }
    }

    fn rejected_test_opportunity(
        kind: RwaOpportunityKind,
        net_profit_bps: f64,
        route: &str,
    ) -> RwaOpportunity {
        RwaOpportunity {
            ticker: "SPY".to_string(),
            kind,
            from_symbol: "USDC".to_string(),
            to_symbol: "USDC".to_string(),
            from_mint: USDC_MINT.to_string(),
            to_mint: USDC_MINT.to_string(),
            quote_asset: Some("USDC".to_string()),
            pyth_fair_price_usd: 25.0,
            from_price_usd: Some(25.0),
            to_price_usd: Some(25.0),
            input_atoms: Some(25_000_000),
            input_estimated_usd: Some(25.0),
            expected_output_atoms: Some(25_000_000),
            route: route.to_string(),
            gross_edge_bps: net_profit_bps,
            net_profit_bps,
            net_profit_usd: net_profit_bps / 10_000.0 * 25.0,
            confidence_buffer_bps: 0.0,
            fee: FeeBreakdown::default(),
            executable: false,
            execution_legs: Vec::new(),
            blocked_reason: None,
        }
    }

    #[test]
    fn rejected_near_misses_keep_best_route_per_kind() {
        let config = RwaConfig {
            min_net_profit_bps: 50.0,
            ..RwaConfig::default()
        };
        let rejected = vec![
            rejected_test_opportunity(
                RwaOpportunityKind::StableFundedDirectIssuerRoundtrip,
                8.0,
                "low-roundtrip",
            ),
            rejected_test_opportunity(
                RwaOpportunityKind::StableFundedDirectIssuerRoundtrip,
                18.0,
                "best-roundtrip",
            ),
            rejected_test_opportunity(RwaOpportunityKind::PrimaryMintSell, -4.0, "primary"),
        ];

        let by_kind = best_rejected_by_kind(&rejected, &config);
        let roundtrip = by_kind
            .iter()
            .find(|entry| entry.kind == RwaOpportunityKind::StableFundedDirectIssuerRoundtrip)
            .expect("stable-funded roundtrip near miss");
        assert_eq!(roundtrip.net_profit_bps, 18.0);
        assert_eq!(roundtrip.route, "best-roundtrip");
        assert!(roundtrip.reason.contains("below configured 50.00 bps"));

        let primary = by_kind
            .iter()
            .find(|entry| entry.kind == RwaOpportunityKind::PrimaryMintSell)
            .expect("primary near miss");
        assert_eq!(primary.route, "primary");
    }

    #[test]
    fn pyth_fixed_point_conversion_handles_negative_expo() {
        let price = pyth_fixed_to_f64("123456789", -6).unwrap();
        assert!((price - 123.456789).abs() < 0.000001);
    }

    #[test]
    fn next_regular_equity_open_skips_weekend() {
        let saturday = Utc.with_ymd_and_hms(2026, 5, 30, 2, 0, 0).single().unwrap();

        let next_open = next_us_equity_regular_open_at(saturday).unwrap();

        assert_eq!(
            next_open,
            Utc.with_ymd_and_hms(2026, 6, 1, 13, 30, 0)
                .single()
                .unwrap()
        );
    }

    #[test]
    fn equity_session_and_next_open_skip_market_holidays() {
        let memorial_day = Utc
            .with_ymd_and_hms(2026, 5, 25, 15, 0, 0)
            .single()
            .unwrap();

        assert_eq!(us_equity_session_at(memorial_day), UsEquitySession::Closed);
        assert_eq!(
            next_us_equity_regular_open_at(memorial_day).unwrap(),
            Utc.with_ymd_and_hms(2026, 5, 26, 13, 30, 0)
                .single()
                .unwrap()
        );
    }

    #[test]
    fn equity_holiday_calendar_includes_good_friday() {
        let good_friday = Utc.with_ymd_and_hms(2026, 4, 3, 15, 0, 0).single().unwrap();

        assert_eq!(us_equity_session_at(good_friday), UsEquitySession::Closed);
    }

    #[test]
    fn confidence_buffer_rejects_wide_oracle() {
        let now = Utc
            .with_ymd_and_hms(2026, 5, 29, 19, 0, 0)
            .single()
            .unwrap();
        let update = HermesParsedPrice {
            id: "19e09bb805456ada3979a7d1cbb4b6d63babc3a0f8e8a9509f68afa5c4c11cd5".to_string(),
            price: HermesPrice {
                price: "100000000".to_string(),
                conf: "5000000".to_string(),
                expo: -6,
                publish_time: now.timestamp(),
            },
            ema_price: None,
            metadata: None,
        };
        let price = pyth_price_from_update_at(update, 90_000, 10.0, true, now);
        assert_eq!(price.status, OracleStatus::WideConfidence);
    }

    #[test]
    fn pyth_equity_oracle_reports_market_closed_after_regular_session() {
        let now = Utc
            .with_ymd_and_hms(2026, 5, 29, 20, 48, 0)
            .single()
            .unwrap();
        let update = HermesParsedPrice {
            id: "19e09bb805456ada3979a7d1cbb4b6d63babc3a0f8e8a9509f68afa5c4c11cd5".to_string(),
            price: HermesPrice {
                price: "756627710".to_string(),
                conf: "105040".to_string(),
                expo: -6,
                publish_time: now.timestamp(),
            },
            ema_price: None,
            metadata: None,
        };
        let price = pyth_price_from_update_at(update, 90_000, 10.0, true, now);
        assert_eq!(price.status, OracleStatus::MarketClosed);
        assert!(price.market_session.contains("closed"));
    }

    #[test]
    fn rwa_jupiter_quote_budget_uses_bounded_override() {
        assert_eq!(
            rwa_jupiter_quote_budget_ms_from_value(Some("12000")),
            12_000
        );
        assert_eq!(rwa_jupiter_quote_budget_ms_from_value(Some("0")), 1_000);
        assert_eq!(
            rwa_jupiter_quote_budget_ms_from_value(Some("180000")),
            120_000
        );
        assert_eq!(
            rwa_jupiter_quote_budget_ms_from_value(Some("invalid")),
            20_000
        );
        assert_eq!(rwa_jupiter_quote_budget_ms_from_value(None), 20_000);
    }

    #[test]
    fn jupiter_v2_order_params_match_live_execution_shape() {
        let params = jupiter_v2_order_params(
            USDC_MINT,
            "SPYx",
            25_000_000,
            Some("wallet111111111111111111111111111111111111"),
            true,
            None,
        )
        .into_iter()
        .collect::<HashMap<_, _>>();

        assert_eq!(params.get("inputMint").map(String::as_str), Some(USDC_MINT));
        assert_eq!(params.get("outputMint").map(String::as_str), Some("SPYx"));
        assert_eq!(params.get("amount").map(String::as_str), Some("25000000"));
        assert_eq!(
            params.get("taker").map(String::as_str),
            Some("wallet111111111111111111111111111111111111")
        );
        assert_eq!(params.get("swapMode").map(String::as_str), Some("ExactIn"));
        assert!(!params.contains_key("slippageBps"));

        let manual = jupiter_v2_order_params(
            USDC_MINT,
            "SPYx",
            25_000_000,
            None,
            false,
            Some("iris,dflow,okx"),
        )
        .into_iter()
        .collect::<HashMap<_, _>>();
        assert_eq!(manual.get("slippageBps").map(String::as_str), Some("50"));
        assert_eq!(
            manual.get("excludeRouters").map(String::as_str),
            Some("iris,dflow,okx")
        );
        assert!(!manual.contains_key("taker"));
    }

    #[test]
    fn rfq_only_router_excludes_default_to_none_so_all_routers_are_eligible() {
        assert_eq!(
            rwa_jupiter_v2_rfq_only_exclude_routers_from_value(None),
            None
        );
        assert_eq!(
            rwa_jupiter_v2_rfq_only_exclude_routers_from_value(Some("   ")),
            None
        );
        assert_eq!(
            rwa_jupiter_v2_rfq_only_exclude_routers_from_value(Some("iris")),
            Some("iris".to_string())
        );
        assert_eq!(
            rwa_jupiter_v2_rfq_only_exclude_routers_from_value(Some("dflow,okx")),
            Some("dflow,okx".to_string())
        );
    }

    #[test]
    fn jupiter_v2_route_label_treats_jupiterz_router_as_rfq() {
        let label = scoped_jupiter_v2_route_label(&serde_json::json!({
            "router": "jupiterz",
            "quoteId": "rfq-quote",
            "maker": "maker111",
            "routePlan": [
                {"swapInfo": {"label": "JupiterZ"}}
            ]
        }));

        assert_eq!(label, "jupiter-v2-order:rfq:jupiterz:JupiterZ");
        assert!(is_jupiter_v2_rfq_route(&label));
        assert!(is_jupiter_v2_rfq_route(
            "jupiter-v2-order:aggregator+rfq:jupiterz:JupiterZ"
        ));
        assert!(!is_jupiter_v2_rfq_route(
            "jupiter-v2-order:aggregator:dflow:HumidiFi+Manifest"
        ));
    }

    #[test]
    fn rwa_v2_quote_taker_helper_includes_wallet_only_when_enabled() {
        assert_eq!(
            rwa_jupiter_v2_quote_taker(Some("wallet111111111111111111111111111111111111"), false),
            None
        );
        assert_eq!(
            rwa_jupiter_v2_quote_taker(Some("wallet111111111111111111111111111111111111"), true)
                .as_deref(),
            Some("wallet111111111111111111111111111111111111")
        );
        assert_eq!(rwa_jupiter_v2_quote_taker(Some("   "), true), None);
    }

    #[test]
    fn quote_requests_size_with_last_pyth_price_even_when_oracle_is_closed() {
        let asset = RwaAssetRuntime {
            asset: RwaAssetConfig {
                ticker: "SPY".to_string(),
                enabled: true,
                pyth_feed_id: "19e09bb805456ada3979a7d1cbb4b6d63babc3a0f8e8a9509f68afa5c4c11cd5"
                    .to_string(),
                xstock_symbol: "SPYx".to_string(),
                xstock_mint: "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W".to_string(),
                xstock_decimals: 8,
                xstock_token_program: String::new(),
                ondo_symbol: "SPYon".to_string(),
                ondo_mint: "k18WJUULWheRkSpSquYGdNNmtuE2Vbw1hpuUi92ondo".to_string(),
                ondo_decimals: 9,
                ondo_token_program: String::new(),
                flash_symbol: String::new(),
                flash_mint: String::new(),
                flash_decimals: 0,
                flash_token_program: String::new(),
                flash_mint_burn_enabled: false,
                flash_shares_per_token: 1.0,
                ondo_shares_per_token: 1.0,
                max_trade_usd: Some(25.0),
                min_net_profit_bps: None,
                max_oracle_confidence_bps: None,
                max_oracle_age_ms: None,
                regular_hours_only: None,
                source: String::new(),
            },
        };
        let mut oracles = HashMap::new();
        oracles.insert(
            asset.asset.pyth_feed_id.clone(),
            PythOraclePrice {
                feed_id: asset.asset.pyth_feed_id.clone(),
                price_usd: 750.0,
                confidence_usd: 0.1,
                confidence_bps: 1.3,
                publish_time: 1,
                age_ms: 1_000_000,
                ema_price_usd: None,
                status: OracleStatus::MarketClosed,
                market_session: "us_equity_regular_closed_slot_1".to_string(),
            },
        );

        let config = RwaConfig::default();
        let quote_assets = vec![RwaQuoteAsset {
            symbol: "USDC".to_string(),
            mint: USDC_MINT.to_string(),
            decimals: 6,
            usd_per_unit: 1.0,
        }];
        let requests = build_quote_requests(&config, &[asset], &quote_assets, &oracles);
        let sell_spyx = requests
            .iter()
            .filter(|request| {
                request.pair.base_mint == "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W"
                    && request.pair.quote_mint == USDC_MINT
                    && request.side == SwapSide::BaseToQuote
            })
            .max_by_key(|request| request.in_amount_atoms)
            .unwrap();
        assert_eq!(sell_spyx.in_amount_atoms, ui_to_atoms(25.0 / 750.0, 8));
    }

    #[test]
    fn taker_fundable_filter_skips_unfunded_jupiter_v2_inputs() {
        let funded_wsol_request = AggregatorQuoteRequest {
            pair: TokenPair {
                base_mint: "SPYon".to_string(),
                quote_mint: WSOL_MINT.to_string(),
                base_decimals: 9,
                quote_decimals: 9,
            },
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 100,
        };
        let unfunded_usdc_request = AggregatorQuoteRequest {
            pair: TokenPair {
                base_mint: "SPYon".to_string(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 9,
                quote_decimals: 6,
            },
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 25_000_000,
        };
        let unfunded_inventory_request = AggregatorQuoteRequest {
            pair: TokenPair {
                base_mint: "SPYon".to_string(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 9,
                quote_decimals: 6,
            },
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 1_000_000_000,
        };
        let mut balances = HashMap::new();
        balances.insert(WSOL_MINT.to_string(), 101);
        balances.insert(USDC_MINT.to_string(), 81_325);

        let filtered = filter_taker_fundable_quote_requests(
            &[
                funded_wsol_request.clone(),
                unfunded_usdc_request,
                unfunded_inventory_request,
            ],
            Some(&balances),
        );

        assert_eq!(filtered.len(), 1);
        assert_eq!(
            filtered[0].pair.quote_mint,
            funded_wsol_request.pair.quote_mint
        );
        assert_eq!(filtered[0].side, SwapSide::QuoteToBase);
    }

    #[test]
    fn taker_partition_keeps_unfunded_sell_legs_for_quote_only_pricing() {
        let funded_buy = AggregatorQuoteRequest {
            pair: TokenPair {
                base_mint: "TESTr".to_string(),
                quote_mint: WSOL_MINT.to_string(),
                base_decimals: 9,
                quote_decimals: 9,
            },
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 125_000_000,
        };
        let unfunded_sell = AggregatorQuoteRequest {
            pair: funded_buy.pair.clone(),
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 13_000_000,
        };
        let mut balances = HashMap::new();
        balances.insert(WSOL_MINT.to_string(), 200_000_000);

        let (fundable, quote_only) = partition_taker_fundable_quote_requests(
            &[funded_buy.clone(), unfunded_sell.clone()],
            Some(&balances),
        );

        assert_eq!(fundable.len(), 1);
        assert_eq!(fundable[0].pair.base_mint, funded_buy.pair.base_mint);
        assert_eq!(fundable[0].pair.quote_mint, funded_buy.pair.quote_mint);
        assert_eq!(fundable[0].side, funded_buy.side);
        assert_eq!(fundable[0].in_amount_atoms, funded_buy.in_amount_atoms);
        assert_eq!(quote_only.len(), 1);
        assert_eq!(quote_only[0].pair.base_mint, unfunded_sell.pair.base_mint);
        assert_eq!(quote_only[0].pair.quote_mint, unfunded_sell.pair.quote_mint);
        assert_eq!(quote_only[0].side, unfunded_sell.side);
        assert_eq!(quote_only[0].in_amount_atoms, unfunded_sell.in_amount_atoms);
    }

    #[test]
    fn missing_quote_requests_keep_secondary_fallback_after_primary_rfq_quote() {
        let ondo_request = AggregatorQuoteRequest {
            pair: TokenPair {
                base_mint: "SPYon".to_string(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 9,
                quote_decimals: 6,
            },
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 25_000_000,
        };
        let xstock_request = AggregatorQuoteRequest {
            pair: TokenPair {
                base_mint: "SPYx".to_string(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 8,
                quote_decimals: 6,
            },
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 25_000_000,
        };
        let ondo_rfq_quote = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:rfq:jupiterz:JupiterZ".to_string(),
            pair: ondo_request.pair.clone(),
            side: ondo_request.side,
            in_amount_atoms: ondo_request.in_amount_atoms,
            out_amount_atoms: 33_000_000,
            effective_price: 755.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };

        let missing =
            missing_quote_requests(&[ondo_request, xstock_request.clone()], &[ondo_rfq_quote]);

        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].pair.base_mint, xstock_request.pair.base_mint);
        assert_eq!(missing[0].pair.quote_mint, xstock_request.pair.quote_mint);
        assert_eq!(missing[0].side, xstock_request.side);
    }

    #[test]
    fn rfq_only_filter_keeps_primary_stable_legs_not_direct_pairs() {
        let asset = test_asset("SPY", "spy_xstock", "spy_ondo");
        let other_asset = test_asset("QQQ", "qqq_xstock", "qqq_ondo");
        let stable_buy = AggregatorQuoteRequest {
            pair: TokenPair {
                base_mint: "spy_ondo".to_string(),
                quote_mint: WSOL_MINT.to_string(),
                base_decimals: 9,
                quote_decimals: 9,
            },
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 125_000_000,
        };
        let stable_sell = AggregatorQuoteRequest {
            pair: stable_buy.pair.clone(),
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 13_000_000,
        };
        let direct_pair = AggregatorQuoteRequest {
            pair: TokenPair {
                base_mint: "spy_ondo".to_string(),
                quote_mint: "qqq_ondo".to_string(),
                base_decimals: 9,
                quote_decimals: 9,
            },
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 13_000_000,
        };
        let xstock_direct_pair = AggregatorQuoteRequest {
            pair: TokenPair {
                base_mint: "spy_ondo".to_string(),
                quote_mint: "spy_xstock".to_string(),
                base_decimals: 9,
                quote_decimals: 8,
            },
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 13_000_000,
        };

        let filtered = filter_primary_route_quote_requests(
            &[
                stable_buy.clone(),
                stable_sell.clone(),
                direct_pair,
                xstock_direct_pair,
            ],
            &[asset, other_asset],
        );

        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].pair.base_mint, stable_buy.pair.base_mint);
        assert_eq!(filtered[0].pair.quote_mint, stable_buy.pair.quote_mint);
        assert_eq!(filtered[0].side, stable_buy.side);
        assert_eq!(filtered[0].in_amount_atoms, stable_buy.in_amount_atoms);
        assert_eq!(filtered[1].pair.base_mint, stable_sell.pair.base_mint);
        assert_eq!(filtered[1].pair.quote_mint, stable_sell.pair.quote_mint);
        assert_eq!(filtered[1].side, stable_sell.side);
        assert_eq!(filtered[1].in_amount_atoms, stable_sell.in_amount_atoms);
    }

    #[test]
    fn jupiter_v2_min_order_filter_skips_sub_five_dollar_rwa_orders() {
        let asset = test_asset("SPY", "spy_xstock", "spy_ondo");
        let mut oracle_prices = HashMap::new();
        oracle_prices.insert(
            asset.asset.pyth_feed_id.clone(),
            PythOraclePrice {
                feed_id: asset.asset.pyth_feed_id.clone(),
                price_usd: 100.0,
                confidence_usd: 0.1,
                confidence_bps: 1.0,
                publish_time: 1,
                age_ms: 1_000,
                ema_price_usd: None,
                status: OracleStatus::Valid,
                market_session: "us_equity_regular_open_slot_1".to_string(),
            },
        );
        let quote_assets = vec![RwaQuoteAsset {
            symbol: "USDC".to_string(),
            mint: USDC_MINT.to_string(),
            decimals: 6,
            usd_per_unit: 1.0,
        }];
        let small_stable_buy = AggregatorQuoteRequest {
            pair: TokenPair {
                base_mint: asset.asset.xstock_mint.clone(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 8,
                quote_decimals: 6,
            },
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 4_000_000,
        };
        let ok_stable_buy = AggregatorQuoteRequest {
            in_amount_atoms: 5_000_000,
            ..small_stable_buy.clone()
        };
        let small_rwa_sell = AggregatorQuoteRequest {
            pair: small_stable_buy.pair.clone(),
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 4_000_000,
        };
        let ok_rwa_sell = AggregatorQuoteRequest {
            in_amount_atoms: 5_000_000,
            ..small_rwa_sell.clone()
        };
        let mut requests = vec![
            small_stable_buy,
            ok_stable_buy.clone(),
            small_rwa_sell,
            ok_rwa_sell.clone(),
        ];

        retain_jupiter_v2_min_order_requests(
            &mut requests,
            &quote_assets,
            &[asset],
            &oracle_prices,
        );

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].side, ok_stable_buy.side);
        assert_eq!(requests[0].in_amount_atoms, ok_stable_buy.in_amount_atoms);
        assert_eq!(requests[1].side, ok_rwa_sell.side);
        assert_eq!(requests[1].in_amount_atoms, ok_rwa_sell.in_amount_atoms);
    }

    #[test]
    fn quote_requests_with_balances_skip_unfunded_quote_assets_without_bridge_source() {
        let asset = test_asset("SPY", "spy_xstock", "spy_ondo");
        let config = RwaConfig::default();
        let quote_assets = vec![
            RwaQuoteAsset {
                symbol: "USDC".to_string(),
                mint: USDC_MINT.to_string(),
                decimals: 6,
                usd_per_unit: 1.0,
            },
            RwaQuoteAsset {
                symbol: "USDT".to_string(),
                mint: USDT_MINT.to_string(),
                decimals: 6,
                usd_per_unit: 1.0,
            },
        ];
        let mut balances = HashMap::new();
        balances.insert(USDC_MINT.to_string(), 81_325);
        balances.insert(USDT_MINT.to_string(), 25_000_000);

        let requests = build_quote_requests_for_balances(
            &config,
            &[asset],
            &quote_assets,
            &HashMap::new(),
            Some(&balances),
        );

        assert!(requests
            .iter()
            .any(|request| request.pair.quote_mint == USDT_MINT));
        assert!(!requests
            .iter()
            .any(|request| request.pair.quote_mint == USDC_MINT));
        assert!(requests.iter().any(|request| {
            request.pair.base_mint == "spy_xstock"
                && request.pair.quote_mint == "spy_ondo"
                && request.side == SwapSide::BaseToQuote
        }));
    }

    #[test]
    fn quote_requests_with_wsol_bridge_keep_underfunded_stable_quotes_as_quote_only() {
        let asset = test_asset("SPY", "spy_xstock", "spy_ondo");
        let config = RwaConfig::default();
        let quote_assets = vec![
            RwaQuoteAsset {
                symbol: "USDC".to_string(),
                mint: USDC_MINT.to_string(),
                decimals: 6,
                usd_per_unit: 1.0,
            },
            RwaQuoteAsset {
                symbol: "SOL/WSOL".to_string(),
                mint: WSOL_MINT.to_string(),
                decimals: 9,
                usd_per_unit: 100.0,
            },
        ];
        let mut balances = HashMap::new();
        balances.insert(USDC_MINT.to_string(), 81_325);
        balances.insert(WSOL_MINT.to_string(), 251_000_000);

        let requests = build_quote_requests_for_balances(
            &config,
            &[asset],
            &quote_assets,
            &HashMap::new(),
            Some(&balances),
        );
        let (taker_fundable, quote_only) =
            partition_taker_fundable_quote_requests(&requests, Some(&balances));

        assert!(requests
            .iter()
            .any(|request| request.pair.quote_mint == USDC_MINT));
        assert!(!taker_fundable
            .iter()
            .any(|request| request.pair.quote_mint == USDC_MINT));
        assert!(quote_only
            .iter()
            .any(|request| request.pair.quote_mint == USDC_MINT));
    }

    #[test]
    fn primary_rfq_probe_requests_keep_unfunded_stable_primary_routes() {
        let asset = test_asset("SPY", "spy_xstock", "spy_ondo");
        let config = RwaConfig::default();
        let quote_assets = vec![
            RwaQuoteAsset {
                symbol: "USDC".to_string(),
                mint: USDC_MINT.to_string(),
                decimals: 6,
                usd_per_unit: 1.0,
            },
            RwaQuoteAsset {
                symbol: "SOL/WSOL".to_string(),
                mint: WSOL_MINT.to_string(),
                decimals: 9,
                usd_per_unit: 100.0,
            },
        ];
        let mut balances = HashMap::new();
        balances.insert(USDC_MINT.to_string(), 81_325);
        balances.insert(WSOL_MINT.to_string(), 250_000_000);

        let wallet_filtered = build_quote_requests_for_balances(
            &config,
            std::slice::from_ref(&asset),
            &quote_assets,
            &HashMap::new(),
            Some(&balances),
        );
        let rfq_probes = build_primary_rfq_probe_quote_requests(
            &config,
            &[asset],
            &quote_assets,
            &HashMap::new(),
        );

        assert!(!wallet_filtered
            .iter()
            .any(|request| request.pair.quote_mint == USDC_MINT));
        assert!(rfq_probes.iter().any(|request| {
            request.pair.base_mint == "spy_ondo"
                && request.pair.quote_mint == USDC_MINT
                && request.side == SwapSide::QuoteToBase
        }));
        assert!(rfq_probes.iter().any(|request| {
            request.pair.base_mint == "spy_ondo"
                && request.pair.quote_mint == USDC_MINT
                && request.side == SwapSide::BaseToQuote
        }));
    }

    #[test]
    fn rwa_trade_size_ladder_sorts_deduplicates_caps_and_includes_max() {
        let runtime = test_asset("SPY", "spy_xstock", "spy_ondo");
        let config = RwaConfig::default();

        let sizes = rwa_trade_size_ladder_from_value(
            &config,
            &runtime.asset,
            Some("10, 5, 10, 0, -1, nope, 500"),
        );

        assert_eq!(sizes, vec![5.0, 10.0, 25.0]);
    }

    #[test]
    fn quote_plans_include_configured_rwa_trade_size_ladder() {
        let asset = test_asset("SPY", "spy_xstock", "spy_ondo");
        let mut oracles = HashMap::new();
        oracles.insert(
            asset.asset.pyth_feed_id.clone(),
            PythOraclePrice {
                feed_id: asset.asset.pyth_feed_id.clone(),
                price_usd: 50.0,
                confidence_usd: 0.1,
                confidence_bps: 1.0,
                publish_time: 1,
                age_ms: 1_000,
                ema_price_usd: None,
                status: OracleStatus::Valid,
                market_session: "us_equity_regular_open_slot_1".to_string(),
            },
        );

        let assets = vec![asset];
        let plans = build_quote_plans(&RwaConfig::default(), &assets, &oracles, Some("5, 10, 25"));

        let trade_sizes = plans.iter().map(|plan| plan.trade_usd).collect::<Vec<_>>();
        assert_eq!(trade_sizes, vec![5.0, 10.0, 25.0]);
        assert_eq!(
            plans[0].issuer_atoms.get("spy_xstock").copied(),
            Some(ui_to_atoms(5.0 / 50.0, 8))
        );
    }

    #[test]
    fn quote_requests_prioritize_routable_xstocks_across_assets() {
        let assets = vec![
            test_asset("SPY", "spy_xstock", "spy_ondo"),
            test_asset("NVDA", "nvda_xstock", "nvda_ondo"),
        ];
        let config = RwaConfig::default();
        let quote_assets = vec![RwaQuoteAsset {
            symbol: "USDC".to_string(),
            mint: USDC_MINT.to_string(),
            decimals: 6,
            usd_per_unit: 1.0,
        }];
        let requests = build_quote_requests(&config, &assets, &quote_assets, &HashMap::new());

        let first_mints = requests
            .iter()
            .take(4)
            .map(|request| request.pair.base_mint.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            first_mints,
            vec!["spy_xstock", "spy_xstock", "nvda_xstock", "nvda_xstock"]
        );

        let first_ondo = requests
            .iter()
            .position(|request| request.pair.base_mint == "spy_ondo")
            .unwrap();
        let first_xstock = requests
            .iter()
            .position(|request| request.pair.base_mint == "spy_xstock")
            .unwrap();
        assert!(first_xstock < first_ondo);
    }

    #[test]
    fn quote_requests_ignore_legacy_flash_fields_and_prioritize_xstock_before_ondo() {
        let mut asset = test_asset("SPY", "spy_xstock", "spy_ondo");
        asset.asset.flash_symbol = "TESTr".to_string();
        asset.asset.flash_mint = "33333333333333333333333333333333".to_string();
        asset.asset.flash_decimals = 9;
        asset.asset.flash_mint_burn_enabled = true;
        let config = RwaConfig::default();
        let quote_assets = vec![RwaQuoteAsset {
            symbol: "USDC".to_string(),
            mint: USDC_MINT.to_string(),
            decimals: 6,
            usd_per_unit: 1.0,
        }];

        let requests = build_quote_requests(&config, &[asset], &quote_assets, &HashMap::new());

        let first_ondo = requests
            .iter()
            .position(|request| request.pair.base_mint == "spy_ondo")
            .unwrap();
        let xstock_requests_before_ondo = requests
            .iter()
            .take(first_ondo)
            .filter(|request| request.pair.base_mint == "spy_xstock")
            .count();
        let flash_requests = requests
            .iter()
            .filter(|request| request.pair.base_mint == "33333333333333333333333333333333")
            .count();

        assert!(xstock_requests_before_ondo >= 2);
        assert_eq!(flash_requests, 0);
    }

    #[test]
    fn quote_request_cap_spreads_across_full_route_list() {
        let mut requests = Vec::new();
        for index in 0..50 {
            requests.push(AggregatorQuoteRequest {
                pair: TokenPair {
                    base_mint: format!("ONDO{index}"),
                    quote_mint: USDC_MINT.to_string(),
                    base_decimals: 9,
                    quote_decimals: 6,
                },
                side: SwapSide::QuoteToBase,
                in_amount_atoms: 5_000_000,
            });
        }
        for index in 0..50 {
            requests.push(AggregatorQuoteRequest {
                pair: TokenPair {
                    base_mint: format!("XSTOCK{index}"),
                    quote_mint: USDC_MINT.to_string(),
                    base_decimals: 8,
                    quote_decimals: 6,
                },
                side: SwapSide::QuoteToBase,
                in_amount_atoms: 5_000_000,
            });
        }

        cap_quote_requests(&mut requests, Some(10));

        assert_eq!(requests.len(), 10);
        assert!(requests
            .iter()
            .any(|request| request.pair.base_mint.starts_with("ONDO")));
        assert!(requests
            .iter()
            .any(|request| request.pair.base_mint.starts_with("XSTOCK")));
    }

    #[test]
    fn quote_request_cap_rotates_across_cycles() {
        let requests = (0..20)
            .map(|index| AggregatorQuoteRequest {
                pair: TokenPair {
                    base_mint: format!("XSTOCK{index}"),
                    quote_mint: USDC_MINT.to_string(),
                    base_decimals: 8,
                    quote_decimals: 6,
                },
                side: SwapSide::QuoteToBase,
                in_amount_atoms: 5_000_000,
            })
            .collect::<Vec<_>>();
        let mut first_cycle = requests.clone();
        let mut second_cycle = requests;

        cap_quote_requests_for_cycle(&mut first_cycle, Some(5), 1, 0);
        cap_quote_requests_for_cycle(&mut second_cycle, Some(5), 2, 0);

        assert_eq!(first_cycle.len(), 5);
        assert_eq!(second_cycle.len(), 5);
        assert_ne!(
            first_cycle
                .iter()
                .map(|request| request.pair.base_mint.as_str())
                .collect::<Vec<_>>(),
            second_cycle
                .iter()
                .map(|request| request.pair.base_mint.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn rwa_quote_cache_reuses_recent_quotes_between_rotated_scans() {
        let _guard = test_env_lock();
        let _env = set_test_env_var("RWA_QUOTE_CACHE_TTL_MS", "60000");
        clear_rwa_quote_cache_for_tests();
        let mut first_scan = vec![test_rwa_quote(
            "SPYx",
            USDC_MINT,
            SwapSide::QuoteToBase,
            10_000_000,
            1_000_000,
            "jupiter-v2-order:aggregator:metis:buy",
        )];

        assert_eq!(merge_rwa_quote_cache(&mut first_scan), 0);

        let mut second_scan = vec![test_rwa_quote(
            "SPYx",
            USDC_MINT,
            SwapSide::BaseToQuote,
            1_000_000,
            10_010_000,
            "jupiter-v2-order:aggregator:metis:sell",
        )];
        let cached_count = merge_rwa_quote_cache(&mut second_scan);

        assert_eq!(cached_count, 1);
        assert!(second_scan
            .iter()
            .any(|quote| quote.side == SwapSide::QuoteToBase));
        assert!(second_scan
            .iter()
            .any(|quote| quote.side == SwapSide::BaseToQuote));
        clear_rwa_quote_cache_for_tests();
    }

    #[test]
    fn rwa_live_blocked_quote_filter_removes_okx_v2_routes_when_enabled() {
        let _guard = test_env_lock();
        let _env = set_test_env_var("RWA_BLOCK_V2_OKX_ROUTES", "true");
        let mut quotes = vec![
            test_rwa_quote(
                "AAPLx",
                USDC_MINT,
                SwapSide::BaseToQuote,
                1_000_000,
                10_000_000,
                "jupiter-v2-order:aggregator:okx:OKX DEX Router",
            ),
            test_rwa_quote(
                "AAPLx",
                USDC_MINT,
                SwapSide::BaseToQuote,
                1_000_000,
                10_000_000,
                "jupiter-route:Meteora DLMM:sell",
            ),
        ];

        assert_eq!(remove_rwa_live_blocked_quotes(&mut quotes), 1);
        assert_eq!(quotes.len(), 1);
        assert!(quotes[0].pool.contains("Meteora"));
    }

    #[test]
    fn quote_request_cap_keeps_xstock_stable_routes_after_ondo_first_ordering() {
        let _guard = test_env_lock();
        let _env = set_test_env_var("RWA_TRADE_SIZE_USD_LADDER", "1,2,5,10,25");
        let tickers = [
            "SPY", "QQQ", "AAPL", "NVDA", "TSLA", "CRCL", "MSTR", "COIN", "GOOGL", "STRC", "HOOD",
            "GLD", "AMZN", "MSFT", "META", "PLTR", "AVGO", "MCD",
        ];
        let assets = tickers
            .iter()
            .map(|ticker| {
                test_asset(
                    ticker,
                    &format!("{ticker}_xstock"),
                    &format!("{ticker}_ondo"),
                )
            })
            .collect::<Vec<_>>();
        let oracles = assets
            .iter()
            .map(|runtime| {
                (
                    runtime.asset.pyth_feed_id.clone(),
                    PythOraclePrice {
                        feed_id: runtime.asset.pyth_feed_id.clone(),
                        price_usd: 100.0,
                        confidence_usd: 0.0,
                        confidence_bps: 0.0,
                        publish_time: 1,
                        age_ms: 1_000,
                        ema_price_usd: None,
                        status: OracleStatus::Valid,
                        market_session: "us_equity_regular_open_slot_1".to_string(),
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        let quote_assets = vec![
            RwaQuoteAsset {
                symbol: "USDC".to_string(),
                mint: USDC_MINT.to_string(),
                decimals: 6,
                usd_per_unit: 1.0,
            },
            RwaQuoteAsset {
                symbol: "USDT".to_string(),
                mint: USDT_MINT.to_string(),
                decimals: 6,
                usd_per_unit: 1.0,
            },
        ];
        let mut requests =
            build_quote_requests(&RwaConfig::default(), &assets, &quote_assets, &oracles);

        cap_quote_requests(&mut requests, Some(144));

        assert!(requests.iter().any(|request| {
            request.pair.base_mint.ends_with("_xstock")
                && request.pair.quote_mint == USDC_MINT
                && request.side == SwapSide::QuoteToBase
        }));
        assert!(requests.iter().any(|request| {
            request.pair.base_mint.ends_with("_xstock")
                && request.pair.quote_mint == USDC_MINT
                && request.side == SwapSide::BaseToQuote
        }));
    }

    #[test]
    fn quote_request_cycle_cap_skips_untradable_mints_before_selecting_slots() {
        clear_rwa_jupiter_untradable_token_cooldowns_for_tests();
        let cooled_ondo_buy = AggregatorQuoteRequest {
            pair: TokenPair {
                base_mint: "cooled_ondo_mint".to_string(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 9,
                quote_decimals: 6,
            },
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 1_000_000,
        };
        let good_ondo_buy = AggregatorQuoteRequest {
            pair: TokenPair {
                base_mint: "good_ondo_mint".to_string(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 9,
                quote_decimals: 6,
            },
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 1_000_000,
        };

        record_rwa_jupiter_untradable_token_cooldown(
            &cooled_ondo_buy,
            "scoped RWA Jupiter quote http 400 Bad Request: {\"error\":\"The token cooled_ondo_mint is not tradable\",\"errorCode\":\"TOKEN_NOT_TRADABLE\"}",
        );

        let mut requests = vec![cooled_ondo_buy, good_ondo_buy.clone()];
        cap_quote_requests_for_cycle(&mut requests, Some(1), 0, 0);
        let filtered = retain_rwa_jupiter_tradable_quote_requests(requests);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].pair.base_mint, good_ondo_buy.pair.base_mint);
        clear_rwa_jupiter_untradable_token_cooldowns_for_tests();
    }

    #[test]
    fn token_not_tradable_cooldown_suppresses_only_matching_rwa_mint_requests() {
        clear_rwa_jupiter_untradable_token_cooldowns_for_tests();
        let bad_ondo_buy = AggregatorQuoteRequest {
            pair: TokenPair {
                base_mint: "bad_ondo_mint".to_string(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 9,
                quote_decimals: 6,
            },
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 1_000_000,
        };
        let good_xstock_buy = AggregatorQuoteRequest {
            pair: TokenPair {
                base_mint: "good_xstock_mint".to_string(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 8,
                quote_decimals: 6,
            },
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 1_000_000,
        };
        let direct_to_bad_ondo = AggregatorQuoteRequest {
            pair: TokenPair {
                base_mint: "good_xstock_mint".to_string(),
                quote_mint: "bad_ondo_mint".to_string(),
                base_decimals: 8,
                quote_decimals: 9,
            },
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 100_000_000,
        };

        record_rwa_jupiter_untradable_token_cooldown(
            &bad_ondo_buy,
            "scoped RWA Jupiter quote http 400 Bad Request: {\"error\":\"The token bad_ondo_mint is not tradable\",\"errorCode\":\"TOKEN_NOT_TRADABLE\"}",
        );

        let filtered = retain_rwa_jupiter_tradable_quote_requests(vec![
            bad_ondo_buy,
            good_xstock_buy.clone(),
            direct_to_bad_ondo,
        ]);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].pair.base_mint, good_xstock_buy.pair.base_mint);
        assert_eq!(filtered[0].pair.quote_mint, USDC_MINT);
        clear_rwa_jupiter_untradable_token_cooldowns_for_tests();
    }

    #[test]
    fn token_not_tradable_cooldown_does_not_cool_quote_funding_mints() {
        clear_rwa_jupiter_untradable_token_cooldowns_for_tests();
        let request = AggregatorQuoteRequest {
            pair: TokenPair {
                base_mint: "routable_xstock_mint".to_string(),
                quote_mint: USDT_MINT.to_string(),
                base_decimals: 8,
                quote_decimals: 6,
            },
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 1_000_000,
        };

        record_rwa_jupiter_untradable_token_cooldown(
            &request,
            "scoped RWA Jupiter quote http 400 Bad Request: {\"error\":\"The token USDT is not tradable\",\"errorCode\":\"TOKEN_NOT_TRADABLE\"}",
        );

        let filtered = retain_rwa_jupiter_tradable_quote_requests(vec![request.clone()]);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].pair.base_mint, request.pair.base_mint);
        clear_rwa_jupiter_untradable_token_cooldowns_for_tests();
    }

    #[test]
    fn quote_fetching_only_uses_assets_with_valid_pyth_feeds() {
        let assets = vec![
            test_asset("SPY", "spy_xstock", "spy_ondo"),
            test_asset("NVDA", "nvda_xstock", "nvda_ondo"),
        ];
        let mut oracles = HashMap::new();
        oracles.insert(
            assets[0].asset.pyth_feed_id.clone(),
            PythOraclePrice {
                feed_id: assets[0].asset.pyth_feed_id.clone(),
                price_usd: 750.0,
                confidence_usd: 0.1,
                confidence_bps: 1.3,
                publish_time: 1,
                age_ms: 1_000,
                ema_price_usd: None,
                status: OracleStatus::Valid,
                market_session: "us_equity_regular_open_slot_1".to_string(),
            },
        );
        oracles.insert(
            assets[1].asset.pyth_feed_id.clone(),
            PythOraclePrice {
                feed_id: assets[1].asset.pyth_feed_id.clone(),
                price_usd: 200.0,
                confidence_usd: 0.1,
                confidence_bps: 5.0,
                publish_time: 1,
                age_ms: 1_000_000,
                ema_price_usd: None,
                status: OracleStatus::MarketClosed,
                market_session: "us_equity_regular_closed_slot_1".to_string(),
            },
        );

        let ready = assets_with_valid_oracles(&assets, &oracles);
        assert_eq!(asset_tickers(&ready), "SPY");
        assert_eq!(pyth_feed_ids_for_assets(&assets).len(), 2);

        let quote_assets = vec![RwaQuoteAsset {
            symbol: "USDC".to_string(),
            mint: USDC_MINT.to_string(),
            decimals: 6,
            usd_per_unit: 1.0,
        }];
        let requests = build_quote_requests(&RwaConfig::default(), &ready, &quote_assets, &oracles);

        assert!(!requests.is_empty());
        assert!(requests.iter().all(|request| {
            request.pair.base_mint.contains("spy") && !request.pair.base_mint.contains("nvda")
        }));
        let status = rwa_quote_status(&assets, &ready, &oracles, requests.len(), 0);
        assert!(status.contains("Quoting SPY"));
        assert!(status.contains("paused NVDA"));
    }

    #[test]
    fn preopen_warmup_can_quote_closed_oracles_without_marking_them_valid() {
        let assets = vec![test_asset("SPY", "spy_xstock", "spy_ondo")];
        let mut oracles = HashMap::new();
        oracles.insert(
            assets[0].asset.pyth_feed_id.clone(),
            PythOraclePrice {
                feed_id: assets[0].asset.pyth_feed_id.clone(),
                price_usd: 750.0,
                confidence_usd: 0.1,
                confidence_bps: 1.3,
                publish_time: 1,
                age_ms: 1_000_000,
                ema_price_usd: None,
                status: OracleStatus::MarketClosed,
                market_session: "us_equity_regular_closed_slot_1".to_string(),
            },
        );
        let before_warmup = Utc
            .with_ymd_and_hms(2026, 6, 1, 12, 30, 0)
            .single()
            .unwrap();
        let inside_warmup = Utc.with_ymd_and_hms(2026, 6, 1, 13, 5, 0).single().unwrap();

        assert!(
            assets_with_quote_oracles_at(&assets, &oracles, before_warmup, 30 * 60 * 1_000)
                .is_empty()
        );
        let warmed =
            assets_with_quote_oracles_at(&assets, &oracles, inside_warmup, 30 * 60 * 1_000);
        assert_eq!(asset_tickers(&warmed), "SPY");
        assert!(assets_with_valid_oracles(&assets, &oracles).is_empty());

        let status = rwa_quote_status(&assets, &warmed, &oracles, 24, 6);
        assert!(status.contains("Pre-open quote warmup for SPY"));
        assert!(status.contains("trading remains blocked"));
    }

    #[test]
    fn quote_status_reports_active_jupiter_rate_limit_cooldown() {
        let _guard = test_env_lock();
        let _env = set_test_env_var("RWA_JUPITER_RATE_LIMIT_COOLDOWN_MS", "60000");
        if let Ok(mut cooldown) = rwa_jupiter_rate_limit_cell().lock() {
            *cooldown = None;
        }

        let assets = vec![test_asset("SPY", "spy_xstock", "spy_ondo")];
        let mut oracles = HashMap::new();
        oracles.insert(
            assets[0].asset.pyth_feed_id.clone(),
            PythOraclePrice {
                feed_id: assets[0].asset.pyth_feed_id.clone(),
                price_usd: 750.0,
                confidence_usd: 0.1,
                confidence_bps: 1.3,
                publish_time: 1,
                age_ms: 1_000,
                ema_price_usd: None,
                status: OracleStatus::Valid,
                market_session: "us_equity_regular_open_slot_1".to_string(),
            },
        );

        start_rwa_jupiter_rate_limit_cooldown();
        let status = rwa_quote_status(&assets, &assets, &oracles, 24, 0);

        assert!(status.contains("provider cooling down"));
        assert!(status.contains("60000ms"));
        if let Ok(mut cooldown) = rwa_jupiter_rate_limit_cell().lock() {
            *cooldown = None;
        }
    }

    #[test]
    fn quote_requests_interleave_direct_routes_before_secondary_quote_assets() {
        let assets = vec![test_asset("SPY", "spy_xstock", "spy_ondo")];
        let config = RwaConfig::default();
        let quote_assets = vec![
            RwaQuoteAsset {
                symbol: "USDC".to_string(),
                mint: USDC_MINT.to_string(),
                decimals: 6,
                usd_per_unit: 1.0,
            },
            RwaQuoteAsset {
                symbol: "USDT".to_string(),
                mint: USDT_MINT.to_string(),
                decimals: 6,
                usd_per_unit: 1.0,
            },
        ];
        let requests = build_quote_requests(&config, &assets, &quote_assets, &HashMap::new());

        let first_direct = requests
            .iter()
            .position(|request| {
                request.pair.base_mint == "spy_xstock"
                    && request.pair.quote_mint == "spy_ondo"
                    && request.side == SwapSide::BaseToQuote
            })
            .unwrap();
        let first_usdt = requests
            .iter()
            .position(|request| request.pair.quote_mint == USDT_MINT)
            .unwrap();

        assert!(first_direct < first_usdt);
    }

    #[test]
    fn direct_route_count_is_visible_when_oracle_is_closed() {
        let runtime = test_asset("SPY", "spy_xstock", "spy_ondo");
        let asset = &runtime.asset;
        let mut oracle_prices = HashMap::new();
        oracle_prices.insert(
            asset.pyth_feed_id.clone(),
            PythOraclePrice {
                feed_id: asset.pyth_feed_id.clone(),
                price_usd: 25.0,
                confidence_usd: 0.001,
                confidence_bps: 0.4,
                publish_time: 1,
                age_ms: 1_000,
                ema_price_usd: None,
                status: OracleStatus::MarketClosed,
                market_session: "us_equity_regular_closed_slot_1".to_string(),
            },
        );
        let direct_pair = TokenPair {
            base_mint: asset.xstock_mint.clone(),
            quote_mint: asset.ondo_mint.clone(),
            base_decimals: asset.xstock_decimals,
            quote_decimals: asset.ondo_decimals,
        };
        let quote_index = QuoteIndex::new(vec![Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:xstock-ondo".to_string(),
            pair: direct_pair,
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 100_000_000,
            out_amount_atoms: 1_000_000_000,
            effective_price: 1.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        }]);
        let quote_assets = vec![RwaQuoteAsset {
            symbol: "USDC".to_string(),
            mint: USDC_MINT.to_string(),
            decimals: 6,
            usd_per_unit: 1.0,
        }];

        let (snapshot, opportunities) = evaluate_asset(
            &RwaConfig::default(),
            &runtime,
            &quote_assets,
            &oracle_prices,
            &quote_index,
            &FeeBreakdown::default(),
        );

        assert_eq!(snapshot.direct_route_request_count, 2);
        assert_eq!(snapshot.direct_route_count, 1);
        assert!(!snapshot
            .no_trade_reason
            .contains("no direct issuer-to-issuer route returned"));
        assert!(opportunities.is_empty());
    }

    #[test]
    fn stable_funded_direct_roundtrip_uses_direct_issuer_route_without_inventory() {
        let runtime = test_asset("SPY", "spy_xstock", "spy_ondo");
        let asset = &runtime.asset;
        let mut oracle_prices = HashMap::new();
        oracle_prices.insert(
            asset.pyth_feed_id.clone(),
            PythOraclePrice {
                feed_id: asset.pyth_feed_id.clone(),
                price_usd: 25.0,
                confidence_usd: 0.0,
                confidence_bps: 0.0,
                publish_time: 1,
                age_ms: 1_000,
                ema_price_usd: None,
                status: OracleStatus::Valid,
                market_session: "us_equity_regular_open_slot_1".to_string(),
            },
        );
        let quote_assets = vec![RwaQuoteAsset {
            symbol: "USDC".to_string(),
            mint: USDC_MINT.to_string(),
            decimals: 6,
            usd_per_unit: 1.0,
        }];
        let buy_xstock = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:usdc-xstock".to_string(),
            pair: TokenPair {
                base_mint: asset.xstock_mint.clone(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: asset.xstock_decimals,
                quote_decimals: 6,
            },
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 25_000_000,
            out_amount_atoms: 100_000_000,
            effective_price: 25.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let direct_xstock_to_ondo = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:xstock-ondo".to_string(),
            pair: TokenPair {
                base_mint: asset.xstock_mint.clone(),
                quote_mint: asset.ondo_mint.clone(),
                base_decimals: asset.xstock_decimals,
                quote_decimals: asset.ondo_decimals,
            },
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 100_000_000,
            out_amount_atoms: 1_100_000_000,
            effective_price: 1.1,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let sell_ondo = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:ondo-usdc".to_string(),
            pair: TokenPair {
                base_mint: asset.ondo_mint.clone(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: asset.ondo_decimals,
                quote_decimals: 6,
            },
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 1_100_000_000,
            out_amount_atoms: 27_000_000,
            effective_price: 27.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let config = RwaConfig {
            execution_enabled: true,
            min_net_profit_bps: 0.0,
            fee_safety_buffer_bps: 0.0,
            ..RwaConfig::default()
        };

        let (snapshot, opportunities) = evaluate_asset(
            &config,
            &runtime,
            &quote_assets,
            &oracle_prices,
            &QuoteIndex::new(vec![buy_xstock, direct_xstock_to_ondo, sell_ondo]),
            &FeeBreakdown::default(),
        );

        assert_eq!(snapshot.direct_route_count, 1);
        let roundtrip = opportunities
            .iter()
            .find(|opportunity| {
                opportunity.kind == RwaOpportunityKind::StableFundedDirectIssuerRoundtrip
            })
            .unwrap();
        assert_eq!(roundtrip.from_mint, USDC_MINT);
        assert_eq!(roundtrip.input_atoms, Some(25_000_000));
        assert_eq!(roundtrip.expected_output_atoms, Some(27_000_000));
        assert_eq!(roundtrip.execution_legs.len(), 3);
        assert_eq!(roundtrip.execution_legs[0].input_mint, USDC_MINT);
        assert_eq!(roundtrip.execution_legs[1].input_mint, asset.xstock_mint);
        assert_eq!(roundtrip.execution_legs[2].input_mint, asset.ondo_mint);
        assert!(roundtrip.executable);
        assert!(roundtrip.net_profit_bps > 0.0);
    }

    #[test]
    fn stable_funded_direct_accumulation_uses_stable_balance_without_starting_inventory() {
        let runtime = test_asset("SPY", "spy_xstock", "spy_ondo");
        let asset = &runtime.asset;
        let mut oracle_prices = HashMap::new();
        oracle_prices.insert(
            asset.pyth_feed_id.clone(),
            PythOraclePrice {
                feed_id: asset.pyth_feed_id.clone(),
                price_usd: 25.0,
                confidence_usd: 0.0,
                confidence_bps: 0.0,
                publish_time: 1,
                age_ms: 1_000,
                ema_price_usd: None,
                status: OracleStatus::Valid,
                market_session: "us_equity_regular_open_slot_1".to_string(),
            },
        );
        let quote_assets = vec![RwaQuoteAsset {
            symbol: "USDC".to_string(),
            mint: USDC_MINT.to_string(),
            decimals: 6,
            usd_per_unit: 1.0,
        }];
        let buy_xstock = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:usdc-xstock".to_string(),
            pair: TokenPair {
                base_mint: asset.xstock_mint.clone(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: asset.xstock_decimals,
                quote_decimals: 6,
            },
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 25_000_000,
            out_amount_atoms: 100_000_000,
            effective_price: 25.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let direct_xstock_to_ondo = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:xstock-ondo".to_string(),
            pair: TokenPair {
                base_mint: asset.xstock_mint.clone(),
                quote_mint: asset.ondo_mint.clone(),
                base_decimals: asset.xstock_decimals,
                quote_decimals: asset.ondo_decimals,
            },
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 100_000_000,
            out_amount_atoms: 1_100_000_000,
            effective_price: 1.1,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let config = RwaConfig {
            execution_enabled: true,
            mode: RwaMode::Both,
            directional_enabled: true,
            min_net_profit_bps: 0.0,
            fee_safety_buffer_bps: 0.0,
            ..RwaConfig::default()
        };

        let (_snapshot, opportunities) = evaluate_asset(
            &config,
            &runtime,
            &quote_assets,
            &oracle_prices,
            &QuoteIndex::new(vec![buy_xstock, direct_xstock_to_ondo]),
            &FeeBreakdown::default(),
        );
        let accumulation = opportunities
            .iter()
            .find(|opportunity| {
                opportunity.kind == RwaOpportunityKind::StableFundedDirectIssuerAccumulation
            })
            .expect("stable-funded accumulation");

        assert_eq!(accumulation.from_mint, USDC_MINT);
        assert_eq!(accumulation.to_mint, asset.ondo_mint);
        assert_eq!(accumulation.input_atoms, Some(25_000_000));
        assert_eq!(accumulation.expected_output_atoms, Some(1_100_000_000));
        assert_eq!(accumulation.execution_legs.len(), 2);
        assert_eq!(accumulation.execution_legs[0].input_mint, USDC_MINT);
        assert_eq!(accumulation.execution_legs[1].input_mint, asset.xstock_mint);
        assert!(accumulation.executable);
        assert!(accumulation.route.contains("hold SPYon mark-to-Pyth"));
        assert!(accumulation.net_profit_bps > 900.0);
    }

    #[test]
    fn quote_assets_support_wsol_sized_by_sol_usd() {
        let assets = quote_assets_from_config(&["WSOL".to_string()], 100.0);
        assert_eq!(assets.len(), 1);
        assert_eq!(assets[0].symbol, "SOL/WSOL");
        assert_eq!(assets[0].mint, WSOL_MINT);
        assert_eq!(assets[0].decimals, 9);
        assert_eq!(assets[0].input_atoms_for_usd(25.0), 250_000_000);
    }

    #[test]
    fn wsol_rwa_quote_prices_are_converted_to_usd() {
        let quote = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:wsol-stock".to_string(),
            pair: TokenPair {
                base_mint: "TESTr".to_string(),
                quote_mint: WSOL_MINT.to_string(),
                base_decimals: 8,
                quote_decimals: 9,
            },
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 250_000_000,
            out_amount_atoms: 125_000_000,
            effective_price: 0.2,
            fee: FeeBreakdown::default(),
            slot: 1,
        };

        assert_eq!(stable_price_per_token(&quote, 8), Some(0.2));
        assert_eq!(stable_price_per_token_usd(&quote, 8, 100.0), Some(20.0));

        let index = QuoteIndex::with_quote_assets(
            vec![quote],
            &[RwaQuoteAsset {
                symbol: "SOL/WSOL".to_string(),
                mint: WSOL_MINT.to_string(),
                decimals: 9,
                usd_per_unit: 100.0,
            }],
        );
        let best = index.best_buy("TESTr", WSOL_MINT).expect("wsol quote");
        assert_eq!(best.out_amount_atoms, 125_000_000);
    }

    #[test]
    fn directional_pyth_discount_buy_accumulates_discounted_official_exposure() {
        let mut runtime = test_asset("SPY", "spy_xstock", "spy_ondo");
        runtime.asset.flash_symbol = "TESTr".to_string();
        runtime.asset.flash_mint = "spy_flash".to_string();
        runtime.asset.flash_decimals = 9;
        runtime.asset.flash_mint_burn_enabled = true;
        let asset = &runtime.asset;
        let mut oracle_prices = HashMap::new();
        oracle_prices.insert(
            asset.pyth_feed_id.clone(),
            PythOraclePrice {
                feed_id: asset.pyth_feed_id.clone(),
                price_usd: 100.0,
                confidence_usd: 0.1,
                confidence_bps: 10.0,
                publish_time: 1,
                age_ms: 1_000,
                ema_price_usd: None,
                status: OracleStatus::Valid,
                market_session: "us_equity_regular_open_slot_1".to_string(),
            },
        );
        let quote_assets = vec![RwaQuoteAsset {
            symbol: "SOL/WSOL".to_string(),
            mint: WSOL_MINT.to_string(),
            decimals: 9,
            usd_per_unit: 100.0,
        }];
        let xstock_buy = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:discounted-xstock".to_string(),
            pair: TokenPair {
                base_mint: asset.xstock_mint.clone(),
                quote_mint: WSOL_MINT.to_string(),
                base_decimals: asset.xstock_decimals,
                quote_decimals: 9,
            },
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 100_000_000,
            out_amount_atoms: 125_000_000,
            effective_price: 0.8,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let config = RwaConfig {
            execution_enabled: true,
            mode: RwaMode::Both,
            directional_enabled: true,
            min_net_profit_bps: 0.0,
            fee_safety_buffer_bps: 15.0,
            ..RwaConfig::default()
        };

        let (_snapshot, opportunities) = evaluate_asset(
            &config,
            &runtime,
            &quote_assets,
            &oracle_prices,
            &QuoteIndex::new(vec![xstock_buy]),
            &FeeBreakdown::default(),
        );
        let opportunity = opportunities
            .iter()
            .find(|opportunity| opportunity.kind == RwaOpportunityKind::PythDiscountBuy)
            .expect("discount buy opportunity");

        assert_eq!(opportunity.from_mint, WSOL_MINT);
        assert_eq!(opportunity.to_mint, asset.xstock_mint);
        assert_eq!(opportunity.execution_legs.len(), 1);
        assert!(opportunity.executable);
        assert!(opportunity.net_profit_bps > 2_000.0);
        assert!(opportunity.route.contains("mark-to-Pyth"));
    }

    #[test]
    fn directional_sell_builds_live_execution_leg_when_enabled() {
        let runtime = test_asset("SPY", "spy_xstock", "spy_ondo");
        let asset = &runtime.asset;
        let mut oracle_prices = HashMap::new();
        oracle_prices.insert(
            asset.pyth_feed_id.clone(),
            PythOraclePrice {
                feed_id: asset.pyth_feed_id.clone(),
                price_usd: 100.0,
                confidence_usd: 0.1,
                confidence_bps: 10.0,
                publish_time: 1,
                age_ms: 1_000,
                ema_price_usd: Some(110.0),
                status: OracleStatus::Valid,
                market_session: "us_equity_regular_open_slot_1".to_string(),
            },
        );
        let quote_assets = vec![RwaQuoteAsset {
            symbol: "USDC".to_string(),
            mint: USDC_MINT.to_string(),
            decimals: 6,
            usd_per_unit: 1.0,
        }];
        let sell_quote = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:xstock-sell".to_string(),
            pair: TokenPair {
                base_mint: asset.xstock_mint.clone(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: asset.xstock_decimals,
                quote_decimals: 6,
            },
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 125_000_000,
            out_amount_atoms: 12_500_000,
            effective_price: 10.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let config = RwaConfig {
            execution_enabled: true,
            mode: RwaMode::Both,
            directional_enabled: true,
            min_net_profit_bps: 0.0,
            fee_safety_buffer_bps: 15.0,
            ..RwaConfig::default()
        };

        let (_snapshot, opportunities) = evaluate_asset(
            &config,
            &runtime,
            &quote_assets,
            &oracle_prices,
            &QuoteIndex::new(vec![sell_quote]),
            &FeeBreakdown::default(),
        );
        let opportunity = opportunities
            .iter()
            .find(|opportunity| opportunity.kind == RwaOpportunityKind::DirectionalSell)
            .expect("directional sell opportunity");

        assert_eq!(opportunity.from_mint, asset.xstock_mint);
        assert_eq!(opportunity.to_mint, USDC_MINT);
        assert_eq!(opportunity.input_atoms, Some(125_000_000));
        assert_eq!(opportunity.execution_legs.len(), 1);
        assert!(opportunity.executable);
        assert!(opportunity.blocked_reason.is_none());
        assert_eq!(opportunity.execution_legs[0].input_mint, asset.xstock_mint);
        assert_eq!(opportunity.execution_legs[0].output_mint, USDC_MINT);
        assert_eq!(
            opportunity.execution_legs[0].route,
            "jupiter-v2-order:aggregator:metis:xstock-sell"
        );
        assert_eq!(opportunity.input_estimated_usd, Some(125.0));
    }

    #[test]
    fn pyth_discount_buy_can_accumulate_discounted_ondo_exposure() {
        let runtime = test_asset("SPY", "spy_xstock", "spy_ondo");
        let asset = &runtime.asset;
        let mut oracle_prices = HashMap::new();
        oracle_prices.insert(
            asset.pyth_feed_id.clone(),
            PythOraclePrice {
                feed_id: asset.pyth_feed_id.clone(),
                price_usd: 100.0,
                confidence_usd: 0.1,
                confidence_bps: 10.0,
                publish_time: 1,
                age_ms: 1_000,
                ema_price_usd: None,
                status: OracleStatus::Valid,
                market_session: "us_equity_regular_open_slot_1".to_string(),
            },
        );
        let quote_assets = vec![RwaQuoteAsset {
            symbol: "USDC".to_string(),
            mint: USDC_MINT.to_string(),
            decimals: 6,
            usd_per_unit: 1.0,
        }];
        let ondo_buy = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:discounted-ondo".to_string(),
            pair: TokenPair {
                base_mint: asset.ondo_mint.clone(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: asset.ondo_decimals,
                quote_decimals: 6,
            },
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 10_000_000,
            out_amount_atoms: 125_000_000,
            effective_price: 0.8,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let config = RwaConfig {
            execution_enabled: true,
            mode: RwaMode::Both,
            directional_enabled: true,
            min_net_profit_bps: 0.0,
            fee_safety_buffer_bps: 15.0,
            ..RwaConfig::default()
        };

        let (_snapshot, opportunities) = evaluate_asset(
            &config,
            &runtime,
            &quote_assets,
            &oracle_prices,
            &QuoteIndex::new(vec![ondo_buy]),
            &FeeBreakdown::default(),
        );
        let opportunity = opportunities
            .iter()
            .find(|opportunity| {
                opportunity.kind == RwaOpportunityKind::PythDiscountBuy
                    && opportunity.to_mint == asset.ondo_mint
            })
            .expect("discounted Ondo buy opportunity");

        assert_eq!(opportunity.from_mint, USDC_MINT);
        assert_eq!(opportunity.to_mint, asset.ondo_mint);
        assert_eq!(opportunity.execution_legs.len(), 1);
        assert!(opportunity.executable);
        assert!(opportunity.net_profit_bps > 2_000.0);
        assert!(opportunity.route.contains("mark-to-Pyth"));
    }

    #[test]
    fn net_profit_subtracts_confidence_safety_and_network() {
        let net = net_profit_bps(100.0, 8.0, 15.0, 0.25, 25.0);
        assert!((net - (-23.0)).abs() < 0.000001);
    }

    #[test]
    fn primary_route_price_is_normalized_by_share_multiplier() {
        let issuer = IssuerQuotes {
            symbol: "SPYon".to_string(),
            mint: "ONDO".to_string(),
            decimals: 9,
            shares_per_token: 1.0059,
            quote_usd_per_unit: 1.0,
            best_buy: None,
            best_sell: None,
            secondary_buy: None,
            secondary_sell: None,
            rfq_buy: None,
            rfq_sell: None,
        };
        let route = RwaPrimaryRouteSnapshot {
            provider: "ondo_jupiter_rfq".to_string(),
            symbol: "SPYon".to_string(),
            mint: "ONDO".to_string(),
            decimals: 9,
            shares_per_token: 1.0059,
            source: "test".to_string(),
            price_usd: 756.62771 * 1.0059,
            normalized_price_usd: 756.62771,
            mint_enabled: true,
            burn_enabled: true,
            rfq_mint_available: false,
            rfq_burn_available: false,
            rfq_mint_price_usd: None,
            rfq_burn_price_usd: None,
            rfq_mint_route: None,
            rfq_burn_route: None,
            non_rfq_mint_price_usd: None,
            non_rfq_burn_price_usd: None,
            non_rfq_mint_route: None,
            non_rfq_burn_route: None,
            external_builder_configured: false,
            readiness: RwaPrimaryRouteReadiness::WaitingForRfq,
            executable: true,
            reason: "test".to_string(),
        };
        assert!(
            (normalized_primary_quote_price(&route, &issuer, None) - 756.62771).abs() < 0.000001
        );
    }

    #[test]
    fn flash_primary_route_is_unsupported_even_for_non_remora_mint() {
        let mut config = RwaConfig::default();
        config.ondo_primary_enabled = false;
        config.flash_primary_enabled = true;
        let mut asset = test_asset(
            "SPY",
            "11111111111111111111111111111111",
            "22222222222222222222222222222222",
        )
        .asset;
        asset.flash_symbol = "TESTr".to_string();
        asset.flash_mint = "11111111111111111111111111111112".to_string();
        asset.flash_decimals = 9;
        asset.flash_mint_burn_enabled = true;
        asset.flash_shares_per_token = 0.01;
        let oracle = PythOraclePrice {
            feed_id: asset.pyth_feed_id.clone(),
            price_usd: 750.0,
            confidence_usd: 0.1,
            confidence_bps: 1.3,
            publish_time: 1,
            age_ms: 1_000,
            ema_price_usd: None,
            status: OracleStatus::Valid,
            market_session: "us_equity_regular_open_slot_1".to_string(),
        };

        let err = validate_asset("SPY", &asset).unwrap_err();
        assert!(err
            .to_string()
            .contains("Flash/Remora issuer fields are no longer supported"));

        let issuers = configured_issuers(&asset);
        assert!(issuers.iter().all(|issuer| issuer.mint != asset.flash_mint));

        let routes = primary_routes_for_asset(&config, &asset, Some(&oracle));
        assert!(routes.iter().all(|route| route.provider != "flash_primary"));
    }

    #[test]
    fn primary_mint_sell_uses_only_jupiter_v2_rfq_mint_leg() {
        let mint = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:rfq:jupiterz:JupiterZ".to_string(),
            pair: TokenPair {
                base_mint: "ONDO".to_string(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 9,
                quote_decimals: 6,
            },
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 25_000_000,
            out_amount_atoms: 1_000_000_000,
            effective_price: 25.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let sell = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:Whirlpool".to_string(),
            pair: mint.pair.clone(),
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 1_100_000_000,
            out_amount_atoms: 26_000_000,
            effective_price: 26.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };

        let legs = primary_mint_sell_execution_legs(Some(&mint), &sell, 25.0);
        assert_eq!(legs.len(), 2);
        assert_eq!(legs[0].route, mint.pool);
        assert_eq!(legs[1].input_atoms, mint.out_amount_atoms);
        assert!(is_jupiter_v2_rfq_route(&legs[0].route));
    }

    #[test]
    fn primary_mint_sell_prorates_mint_to_secondary_sell_capacity() {
        let mint = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:rfq:jupiterz:JupiterZ".to_string(),
            pair: TokenPair {
                base_mint: "ONDO".to_string(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 9,
                quote_decimals: 6,
            },
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 25_000_000,
            out_amount_atoms: 2_000_000_000,
            effective_price: 12.5,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let sell = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:Whirlpool".to_string(),
            pair: mint.pair.clone(),
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 1_000_000_000,
            out_amount_atoms: 13_000_000,
            effective_price: 13.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };

        let legs = primary_mint_sell_execution_legs(Some(&mint), &sell, 25.0);

        assert_eq!(legs.len(), 2);
        assert_eq!(legs[0].input_atoms, 12_500_000);
        assert_eq!(legs[1].input_atoms, sell.in_amount_atoms);
    }

    #[test]
    fn primary_buy_burn_prorates_buy_to_rfq_burn_capacity() {
        let pair = TokenPair {
            base_mint: "ONDO".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 9,
            quote_decimals: 6,
        };
        let buy = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:Whirlpool".to_string(),
            pair: pair.clone(),
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 25_000_000,
            out_amount_atoms: 2_000_000_000,
            effective_price: 12.5,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let burn = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:rfq:jupiterz:JupiterZ".to_string(),
            pair,
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 1_000_000_000,
            out_amount_atoms: 13_000_000,
            effective_price: 13.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };

        let legs = primary_buy_burn_execution_legs(&buy, Some(&burn), 25.0);

        assert_eq!(legs.len(), 2);
        assert_eq!(legs[0].input_atoms, 12_500_000);
        assert_eq!(legs[1].input_atoms, burn.in_amount_atoms);
    }

    #[test]
    fn primary_mint_sell_opportunity_uses_actual_prorated_quote_input() {
        let runtime = test_asset("SPY", "SPYX", "ONDO");
        let asset = runtime.asset;
        let quote_asset = RwaQuoteAsset {
            symbol: "USDC".to_string(),
            mint: USDC_MINT.to_string(),
            decimals: 6,
            usd_per_unit: 1.0,
        };
        let pair = TokenPair {
            base_mint: "ONDO".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 9,
            quote_decimals: 6,
        };
        let rfq_buy = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:rfq:jupiterz:JupiterZ".to_string(),
            pair: pair.clone(),
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 25_000_000,
            out_amount_atoms: 2_000_000_000,
            effective_price: 12.5,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let secondary_sell = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:sell".to_string(),
            pair,
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 1_000_000_000,
            out_amount_atoms: 13_000_000,
            effective_price: 13.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let issuer = IssuerQuotes {
            symbol: "SPYon".to_string(),
            mint: "ONDO".to_string(),
            decimals: 9,
            shares_per_token: 1.0,
            quote_usd_per_unit: 1.0,
            best_buy: Some(rfq_buy.clone()),
            best_sell: Some(secondary_sell.clone()),
            secondary_buy: None,
            secondary_sell: Some(secondary_sell),
            rfq_buy: Some(rfq_buy),
            rfq_sell: None,
        };
        let route = RwaPrimaryRouteSnapshot {
            provider: "ondo_jupiter_rfq".to_string(),
            symbol: "SPYon".to_string(),
            mint: "ONDO".to_string(),
            decimals: 9,
            shares_per_token: 1.0,
            source: "test".to_string(),
            price_usd: 12.5,
            normalized_price_usd: 12.5,
            mint_enabled: true,
            burn_enabled: false,
            rfq_mint_available: true,
            rfq_burn_available: false,
            rfq_mint_price_usd: Some(12.5),
            rfq_burn_price_usd: None,
            rfq_mint_route: Some("jupiter-v2-order:rfq:jupiterz:JupiterZ".to_string()),
            rfq_burn_route: None,
            non_rfq_mint_price_usd: None,
            non_rfq_burn_price_usd: Some(13.0),
            non_rfq_mint_route: None,
            non_rfq_burn_route: Some("jupiter-v2-order:aggregator:metis:sell".to_string()),
            external_builder_configured: false,
            readiness: RwaPrimaryRouteReadiness::RfqReady,
            executable: true,
            reason: "test".to_string(),
        };
        let oracle = PythOraclePrice {
            feed_id: asset.pyth_feed_id.clone(),
            price_usd: 12.5,
            confidence_usd: 0.0,
            confidence_bps: 0.0,
            publish_time: 1,
            age_ms: 1_000,
            ema_price_usd: None,
            status: OracleStatus::Valid,
            market_session: "us_equity_regular_open_slot_1".to_string(),
        };
        let config = RwaConfig {
            execution_enabled: true,
            min_net_profit_bps: -10_000.0,
            fee_safety_buffer_bps: 0.0,
            ..RwaConfig::default()
        };

        let opportunities = primary_route_opportunities(
            &config,
            &asset,
            &oracle,
            &quote_asset,
            &[Some(&issuer)],
            &[route],
            &FeeBreakdown::default(),
        );

        let opportunity = opportunities
            .iter()
            .find(|opportunity| opportunity.kind == RwaOpportunityKind::PrimaryMintSell)
            .unwrap();
        assert_eq!(opportunity.input_atoms, Some(12_500_000));
        assert_eq!(opportunity.input_estimated_usd, Some(12.5));
        assert_eq!(opportunity.expected_output_atoms, Some(13_000_000));
        assert_eq!(opportunity.execution_legs[0].input_atoms, 12_500_000);
        assert!((opportunity.net_profit_usd - 0.5).abs() < 0.000001);
    }

    #[test]
    fn primary_mint_sell_is_not_executable_when_sequential_roundtrips_disabled() {
        let _guard = test_env_lock();
        let _env = set_test_env_var("RWA_SEQUENTIAL_ROUNDTRIP_ENABLED", "false");
        let runtime = test_asset("SPY", "SPYX", "ONDO");
        let asset = runtime.asset;
        let quote_asset = RwaQuoteAsset {
            symbol: "USDC".to_string(),
            mint: USDC_MINT.to_string(),
            decimals: 6,
            usd_per_unit: 1.0,
        };
        let pair = TokenPair {
            base_mint: "ONDO".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 9,
            quote_decimals: 6,
        };
        let rfq_buy = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:rfq:jupiterz:JupiterZ".to_string(),
            pair: pair.clone(),
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 25_000_000,
            out_amount_atoms: 1_000_000_000,
            effective_price: 25.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let secondary_sell = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:sell".to_string(),
            pair,
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 1_000_000_000,
            out_amount_atoms: 26_000_000,
            effective_price: 26.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let issuer = IssuerQuotes {
            symbol: "SPYon".to_string(),
            mint: "ONDO".to_string(),
            decimals: 9,
            shares_per_token: 1.0,
            quote_usd_per_unit: 1.0,
            best_buy: Some(rfq_buy.clone()),
            best_sell: Some(secondary_sell.clone()),
            secondary_buy: None,
            secondary_sell: Some(secondary_sell),
            rfq_buy: Some(rfq_buy),
            rfq_sell: None,
        };
        let route = RwaPrimaryRouteSnapshot {
            provider: "ondo_jupiter_rfq".to_string(),
            symbol: "SPYon".to_string(),
            mint: "ONDO".to_string(),
            decimals: 9,
            shares_per_token: 1.0,
            source: "test".to_string(),
            price_usd: 25.0,
            normalized_price_usd: 25.0,
            mint_enabled: true,
            burn_enabled: false,
            rfq_mint_available: true,
            rfq_burn_available: false,
            rfq_mint_price_usd: Some(25.0),
            rfq_burn_price_usd: None,
            rfq_mint_route: Some("jupiter-v2-order:rfq:jupiterz:JupiterZ".to_string()),
            rfq_burn_route: None,
            non_rfq_mint_price_usd: None,
            non_rfq_burn_price_usd: Some(26.0),
            non_rfq_mint_route: None,
            non_rfq_burn_route: Some("jupiter-v2-order:aggregator:metis:sell".to_string()),
            external_builder_configured: false,
            readiness: RwaPrimaryRouteReadiness::RfqReady,
            executable: true,
            reason: "test".to_string(),
        };
        let oracle = PythOraclePrice {
            feed_id: asset.pyth_feed_id.clone(),
            price_usd: 25.0,
            confidence_usd: 0.0,
            confidence_bps: 0.0,
            publish_time: 1,
            age_ms: 1_000,
            ema_price_usd: None,
            status: OracleStatus::Valid,
            market_session: "us_equity_regular_open_slot_1".to_string(),
        };
        let config = RwaConfig {
            execution_enabled: true,
            min_net_profit_bps: -10_000.0,
            fee_safety_buffer_bps: 0.0,
            ..RwaConfig::default()
        };

        let opportunities = primary_route_opportunities(
            &config,
            &asset,
            &oracle,
            &quote_asset,
            &[Some(&issuer)],
            &[route],
            &FeeBreakdown::default(),
        );

        let opportunity = opportunities
            .iter()
            .find(|opportunity| opportunity.kind == RwaOpportunityKind::PrimaryMintSell)
            .unwrap();
        assert!(!opportunity.executable);
        assert!(opportunity
            .blocked_reason
            .as_deref()
            .unwrap_or_default()
            .contains("sequential RWA primary mint/sell execution is disabled"));
    }

    #[test]
    fn primary_buy_burn_opportunity_uses_actual_prorated_quote_input() {
        let runtime = test_asset("SPY", "SPYX", "ONDO");
        let asset = runtime.asset;
        let quote_asset = RwaQuoteAsset {
            symbol: "USDC".to_string(),
            mint: USDC_MINT.to_string(),
            decimals: 6,
            usd_per_unit: 1.0,
        };
        let pair = TokenPair {
            base_mint: "ONDO".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 9,
            quote_decimals: 6,
        };
        let secondary_buy = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:buy".to_string(),
            pair: pair.clone(),
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 25_000_000,
            out_amount_atoms: 2_000_000_000,
            effective_price: 12.5,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let rfq_sell = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:rfq:jupiterz:JupiterZ".to_string(),
            pair,
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 1_000_000_000,
            out_amount_atoms: 13_000_000,
            effective_price: 13.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let issuer = IssuerQuotes {
            symbol: "SPYon".to_string(),
            mint: "ONDO".to_string(),
            decimals: 9,
            shares_per_token: 1.0,
            quote_usd_per_unit: 1.0,
            best_buy: Some(secondary_buy.clone()),
            best_sell: Some(rfq_sell.clone()),
            secondary_buy: Some(secondary_buy),
            secondary_sell: None,
            rfq_buy: None,
            rfq_sell: Some(rfq_sell),
        };
        let route = RwaPrimaryRouteSnapshot {
            provider: "ondo_jupiter_rfq".to_string(),
            symbol: "SPYon".to_string(),
            mint: "ONDO".to_string(),
            decimals: 9,
            shares_per_token: 1.0,
            source: "test".to_string(),
            price_usd: 13.0,
            normalized_price_usd: 13.0,
            mint_enabled: false,
            burn_enabled: true,
            rfq_mint_available: false,
            rfq_burn_available: true,
            rfq_mint_price_usd: None,
            rfq_burn_price_usd: Some(13.0),
            rfq_mint_route: None,
            rfq_burn_route: Some("jupiter-v2-order:rfq:jupiterz:JupiterZ".to_string()),
            non_rfq_mint_price_usd: Some(12.5),
            non_rfq_burn_price_usd: None,
            non_rfq_mint_route: Some("jupiter-v2-order:aggregator:metis:buy".to_string()),
            non_rfq_burn_route: None,
            external_builder_configured: false,
            readiness: RwaPrimaryRouteReadiness::RfqReady,
            executable: true,
            reason: "test".to_string(),
        };
        let oracle = PythOraclePrice {
            feed_id: asset.pyth_feed_id.clone(),
            price_usd: 12.75,
            confidence_usd: 0.0,
            confidence_bps: 0.0,
            publish_time: 1,
            age_ms: 1_000,
            ema_price_usd: None,
            status: OracleStatus::Valid,
            market_session: "us_equity_regular_open_slot_1".to_string(),
        };
        let config = RwaConfig {
            execution_enabled: true,
            min_net_profit_bps: -10_000.0,
            fee_safety_buffer_bps: 0.0,
            ..RwaConfig::default()
        };

        let opportunities = primary_route_opportunities(
            &config,
            &asset,
            &oracle,
            &quote_asset,
            &[Some(&issuer)],
            &[route],
            &FeeBreakdown::default(),
        );

        let opportunity = opportunities
            .iter()
            .find(|opportunity| opportunity.kind == RwaOpportunityKind::PrimaryBuyBurn)
            .unwrap();
        assert_eq!(opportunity.input_atoms, Some(12_500_000));
        assert_eq!(opportunity.input_estimated_usd, Some(12.5));
        assert_eq!(opportunity.expected_output_atoms, Some(13_000_000));
        assert_eq!(opportunity.execution_legs[0].input_atoms, 12_500_000);
        assert!((opportunity.net_profit_usd - 0.5).abs() < 0.000001);
    }

    #[test]
    fn primary_buy_burn_is_not_executable_when_sequential_roundtrips_disabled() {
        let _guard = test_env_lock();
        let _env = set_test_env_var("RWA_SEQUENTIAL_ROUNDTRIP_ENABLED", "false");
        let runtime = test_asset("SPY", "SPYX", "ONDO");
        let asset = runtime.asset;
        let quote_asset = RwaQuoteAsset {
            symbol: "USDC".to_string(),
            mint: USDC_MINT.to_string(),
            decimals: 6,
            usd_per_unit: 1.0,
        };
        let pair = TokenPair {
            base_mint: "ONDO".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 9,
            quote_decimals: 6,
        };
        let secondary_buy = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:buy".to_string(),
            pair: pair.clone(),
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 25_000_000,
            out_amount_atoms: 2_000_000_000,
            effective_price: 12.5,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let rfq_sell = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:rfq:jupiterz:JupiterZ".to_string(),
            pair,
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 1_000_000_000,
            out_amount_atoms: 13_000_000,
            effective_price: 13.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let issuer = IssuerQuotes {
            symbol: "SPYon".to_string(),
            mint: "ONDO".to_string(),
            decimals: 9,
            shares_per_token: 1.0,
            quote_usd_per_unit: 1.0,
            best_buy: Some(secondary_buy.clone()),
            best_sell: Some(rfq_sell.clone()),
            secondary_buy: Some(secondary_buy),
            secondary_sell: None,
            rfq_buy: None,
            rfq_sell: Some(rfq_sell),
        };
        let route = RwaPrimaryRouteSnapshot {
            provider: "ondo_jupiter_rfq".to_string(),
            symbol: "SPYon".to_string(),
            mint: "ONDO".to_string(),
            decimals: 9,
            shares_per_token: 1.0,
            source: "test".to_string(),
            price_usd: 13.0,
            normalized_price_usd: 13.0,
            mint_enabled: false,
            burn_enabled: true,
            rfq_mint_available: false,
            rfq_burn_available: true,
            rfq_mint_price_usd: None,
            rfq_burn_price_usd: Some(13.0),
            rfq_mint_route: None,
            rfq_burn_route: Some("jupiter-v2-order:rfq:jupiterz:JupiterZ".to_string()),
            non_rfq_mint_price_usd: Some(12.5),
            non_rfq_burn_price_usd: None,
            non_rfq_mint_route: Some("jupiter-v2-order:aggregator:metis:buy".to_string()),
            non_rfq_burn_route: None,
            external_builder_configured: false,
            readiness: RwaPrimaryRouteReadiness::RfqReady,
            executable: true,
            reason: "test".to_string(),
        };
        let oracle = PythOraclePrice {
            feed_id: asset.pyth_feed_id.clone(),
            price_usd: 12.75,
            confidence_usd: 0.0,
            confidence_bps: 0.0,
            publish_time: 1,
            age_ms: 1_000,
            ema_price_usd: None,
            status: OracleStatus::Valid,
            market_session: "us_equity_regular_open_slot_1".to_string(),
        };
        let config = RwaConfig {
            execution_enabled: true,
            min_net_profit_bps: -10_000.0,
            fee_safety_buffer_bps: 0.0,
            ..RwaConfig::default()
        };

        let opportunities = primary_route_opportunities(
            &config,
            &asset,
            &oracle,
            &quote_asset,
            &[Some(&issuer)],
            &[route],
            &FeeBreakdown::default(),
        );

        let opportunity = opportunities
            .iter()
            .find(|opportunity| opportunity.kind == RwaOpportunityKind::PrimaryBuyBurn)
            .unwrap();
        assert!(!opportunity.executable);
        assert!(opportunity
            .blocked_reason
            .as_deref()
            .unwrap_or_default()
            .contains("sequential RWA primary buy/burn execution is disabled"));
    }

    #[test]
    fn stable_route_arbitrage_prorates_buy_to_sell_capacity() {
        let runtime = test_asset("SPY", "SPYX", "ONDO");
        let asset = runtime.asset;
        let quote_asset = RwaQuoteAsset {
            symbol: "USDC".to_string(),
            mint: USDC_MINT.to_string(),
            decimals: 6,
            usd_per_unit: 1.0,
        };
        let pair = TokenPair {
            base_mint: "ONDO".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 9,
            quote_decimals: 6,
        };
        let buy = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:buy".to_string(),
            pair: pair.clone(),
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 25_000_000,
            out_amount_atoms: 2_000_000_000,
            effective_price: 12.5,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let sell = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:sell".to_string(),
            pair,
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 1_000_000_000,
            out_amount_atoms: 13_000_000,
            effective_price: 13.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let issuer = IssuerQuotes {
            symbol: "SPYon".to_string(),
            mint: "ONDO".to_string(),
            decimals: 9,
            shares_per_token: 1.0,
            quote_usd_per_unit: 1.0,
            best_buy: Some(buy.clone()),
            best_sell: Some(sell.clone()),
            secondary_buy: Some(buy.clone()),
            secondary_sell: Some(sell.clone()),
            rfq_buy: None,
            rfq_sell: None,
        };
        let oracle = PythOraclePrice {
            feed_id: "feed".to_string(),
            price_usd: 12.75,
            confidence_usd: 0.01,
            confidence_bps: 1.0,
            publish_time: 1,
            age_ms: 1_000,
            ema_price_usd: None,
            status: OracleStatus::Valid,
            market_session: "us_equity_regular_open_slot_1".to_string(),
        };

        let opportunity = stable_route_arbitrage_opportunity(
            &RwaConfig {
                execution_enabled: true,
                min_net_profit_bps: -10_000.0,
                ..RwaConfig::default()
            },
            &asset,
            &oracle,
            &quote_asset,
            &issuer,
            &buy,
            &sell,
            12.5,
            13.0,
            &FeeBreakdown::default(),
        );

        assert_eq!(opportunity.input_atoms, Some(12_500_000));
        assert_eq!(opportunity.input_estimated_usd, Some(12.5));
        assert_eq!(opportunity.expected_output_atoms, Some(13_000_000));
        assert_eq!(opportunity.execution_legs[0].input_atoms, 12_500_000);
        assert_eq!(
            opportunity.execution_legs[1].input_atoms,
            sell.in_amount_atoms
        );
    }

    #[test]
    fn stable_route_arbitrage_blocks_rfq_exit_when_enabled() {
        let _lock = test_env_lock();
        let _env = set_test_env_var("RWA_BLOCK_STABLE_ROUTE_RFQ_EXIT", "true");
        let runtime = test_asset("MSFT", "MSFTX", "MSFTON");
        let asset = runtime.asset;
        let quote_asset = RwaQuoteAsset {
            symbol: "USDC".to_string(),
            mint: USDC_MINT.to_string(),
            decimals: 6,
            usd_per_unit: 1.0,
        };
        let pair = TokenPair {
            base_mint: "MSFTX".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 8,
            quote_decimals: 6,
        };
        let buy = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-route:GoonFi V2:test".to_string(),
            pair: pair.clone(),
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 2_000_000,
            out_amount_atoms: 570_000,
            effective_price: 350.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let sell = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:rfq:jupiterz:JupiterZ".to_string(),
            pair,
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 570_000,
            out_amount_atoms: 2_005_000,
            effective_price: 351.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let issuer = IssuerQuotes {
            symbol: "MSFTx".to_string(),
            mint: "MSFTX".to_string(),
            decimals: 8,
            shares_per_token: 1.0,
            quote_usd_per_unit: 1.0,
            best_buy: Some(buy.clone()),
            best_sell: Some(sell.clone()),
            secondary_buy: Some(buy.clone()),
            secondary_sell: None,
            rfq_buy: None,
            rfq_sell: Some(sell.clone()),
        };
        let oracle = PythOraclePrice {
            feed_id: "feed".to_string(),
            price_usd: 350.5,
            confidence_usd: 0.0,
            confidence_bps: 0.0,
            publish_time: 1,
            age_ms: 1_000,
            ema_price_usd: None,
            status: OracleStatus::Valid,
            market_session: "us_equity_regular_open_slot_1".to_string(),
        };

        let opportunity = stable_route_arbitrage_opportunity(
            &RwaConfig {
                execution_enabled: true,
                min_net_profit_bps: -10_000.0,
                ..RwaConfig::default()
            },
            &asset,
            &oracle,
            &quote_asset,
            &issuer,
            &buy,
            &sell,
            350.0,
            351.0,
            &FeeBreakdown::default(),
        );

        assert!(!opportunity.executable);
        assert!(opportunity
            .blocked_reason
            .as_deref()
            .unwrap_or_default()
            .contains("JupiterZ RFQ exit leg is blocked"));
    }

    #[test]
    fn inventory_rebalance_prorates_to_smaller_buy_leg_size() {
        let runtime = test_asset("SPY", "SPYX", "ONDO");
        let asset = runtime.asset;
        let quote_asset = RwaQuoteAsset {
            symbol: "USDC".to_string(),
            mint: USDC_MINT.to_string(),
            decimals: 6,
            usd_per_unit: 1.0,
        };
        let sell_pair = TokenPair {
            base_mint: "SPYX".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 8,
            quote_decimals: 6,
        };
        let buy_pair = TokenPair {
            base_mint: "ONDO".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 9,
            quote_decimals: 6,
        };
        let rich_sell = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-route:Whirlpool:sell".to_string(),
            pair: sell_pair,
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 2_500_000,
            out_amount_atoms: 25_000_000,
            effective_price: 760.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let cheap_buy = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:buy".to_string(),
            pair: buy_pair,
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 4_000_000,
            out_amount_atoms: 5_300_000,
            effective_price: 755.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let rich = IssuerQuotes {
            symbol: "SPYx".to_string(),
            mint: "SPYX".to_string(),
            decimals: 8,
            shares_per_token: 1.0,
            quote_usd_per_unit: 1.0,
            best_buy: None,
            best_sell: Some(rich_sell),
            secondary_buy: None,
            secondary_sell: None,
            rfq_buy: None,
            rfq_sell: None,
        };
        let cheap = IssuerQuotes {
            symbol: "SPYon".to_string(),
            mint: "ONDO".to_string(),
            decimals: 9,
            shares_per_token: 1.0,
            quote_usd_per_unit: 1.0,
            best_buy: Some(cheap_buy),
            best_sell: None,
            secondary_buy: None,
            secondary_sell: None,
            rfq_buy: None,
            rfq_sell: None,
        };
        let oracle = PythOraclePrice {
            feed_id: "feed".to_string(),
            price_usd: 756.0,
            confidence_usd: 0.01,
            confidence_bps: 1.0,
            publish_time: 1,
            age_ms: 1_000,
            ema_price_usd: None,
            status: OracleStatus::Valid,
            market_session: "us_equity_regular_open_slot_1".to_string(),
        };

        let opportunity = inventory_rebalance_opportunity(
            &RwaConfig {
                execution_enabled: true,
                min_net_profit_bps: -10_000.0,
                ..RwaConfig::default()
            },
            &asset,
            &oracle,
            &quote_asset,
            &rich,
            &cheap,
            760.0,
            755.0,
            &FeeBreakdown::default(),
        );

        assert_eq!(opportunity.input_atoms, Some(400_000));
        assert_eq!(opportunity.input_estimated_usd, Some(4.0));
        assert_eq!(opportunity.expected_output_atoms, Some(5_300_000));
        assert_eq!(opportunity.execution_legs[0].input_atoms, 400_000);
        assert_eq!(opportunity.execution_legs[0].estimated_input_usd, Some(4.0));
        assert_eq!(opportunity.execution_legs[1].input_atoms, 4_000_000);
        assert_eq!(opportunity.execution_legs[1].estimated_input_usd, Some(4.0));
    }

    #[test]
    fn openocean_rwa_routes_are_quote_only_until_live_build_enabled() {
        let runtime = test_asset("SPY", "SPYX", "ONDO");
        let asset = runtime.asset;
        let quote = Quote {
            venue: Venue::OpenOcean,
            pool: "openocean-route:Titan:unknown".to_string(),
            pair: TokenPair {
                base_mint: "SPYX".to_string(),
                quote_mint: "ONDO".to_string(),
                base_decimals: 8,
                quote_decimals: 9,
            },
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 528_000,
            out_amount_atoms: 5_300_000,
            effective_price: 755.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let oracle = PythOraclePrice {
            feed_id: "feed".to_string(),
            price_usd: 756.0,
            confidence_usd: 0.01,
            confidence_bps: 1.0,
            publish_time: 1,
            age_ms: 1_000,
            ema_price_usd: None,
            status: OracleStatus::Valid,
            market_session: "us_equity_regular_open_slot_1".to_string(),
        };

        let opportunity = direct_swap_opportunity(
            &RwaConfig {
                execution_enabled: true,
                min_net_profit_bps: -10_000.0,
                ..RwaConfig::default()
            },
            &asset,
            &oracle,
            &quote,
            "SPYx",
            "SPYon",
            100.0,
            &FeeBreakdown::default(),
        );

        assert!(!opportunity.executable);
        assert!(opportunity
            .blocked_reason
            .as_deref()
            .unwrap_or_default()
            .contains("quote-only route provider(s): openocean"));
    }

    #[test]
    fn primary_route_uses_rfq_leg_even_when_not_best_quote() {
        let pair = TokenPair {
            base_mint: "ONDO".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 9,
            quote_decimals: 6,
        };
        let best_buy = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:Whirlpool".to_string(),
            pair: pair.clone(),
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 25_000_000,
            out_amount_atoms: 1_100_000_000,
            effective_price: 22.72,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let rfq_buy = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:rfq:jupiterz:JupiterZ".to_string(),
            pair: pair.clone(),
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 25_000_000,
            out_amount_atoms: 1_000_000_000,
            effective_price: 25.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let best_sell = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:Whirlpool".to_string(),
            pair: pair.clone(),
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 1_000_000_000,
            out_amount_atoms: 26_000_000,
            effective_price: 26.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let rfq_sell = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:rfq:jupiterz:JupiterZ".to_string(),
            pair,
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 1_000_000_000,
            out_amount_atoms: 25_000_000,
            effective_price: 25.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let issuer = IssuerQuotes {
            symbol: "SPYon".to_string(),
            mint: "ONDO".to_string(),
            decimals: 9,
            shares_per_token: 1.0,
            quote_usd_per_unit: 1.0,
            best_buy: Some(best_buy.clone()),
            best_sell: Some(best_sell.clone()),
            secondary_buy: Some(best_buy),
            secondary_sell: Some(best_sell),
            rfq_buy: Some(rfq_buy.clone()),
            rfq_sell: Some(rfq_sell.clone()),
        };
        let config = RwaConfig {
            enabled: true,
            execution_enabled: true,
            assets_file: "rwa_assets.toml".to_string(),
            mode: RwaMode::Neutral,
            directional_enabled: false,
            ondo_primary_enabled: true,
            flash_primary_enabled: false,
            quote_assets: vec!["USDC".to_string()],
            min_net_profit_bps: 0.0,
            fee_safety_buffer_bps: 0.0,
            max_trade_usd_per_asset: 25.0,
            max_oracle_confidence_bps: 10.0,
            max_oracle_age_ms: 90_000,
            regular_hours_only: true,
            emit_execution_candidates: true,
            max_live_trades_per_cycle: 1,
            max_assets_per_cycle: 8,
        };
        let asset = RwaAssetConfig {
            ticker: "SPY".to_string(),
            enabled: true,
            pyth_feed_id: "19e09bb805456ada3979a7d1cbb4b6d63babc3a0f8e8a9509f68afa5c4c11cd5"
                .to_string(),
            xstock_symbol: "SPYx".to_string(),
            xstock_mint: "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W".to_string(),
            xstock_decimals: 8,
            xstock_token_program: String::new(),
            ondo_symbol: "SPYon".to_string(),
            ondo_mint: "ONDO".to_string(),
            ondo_decimals: 9,
            ondo_token_program: String::new(),
            flash_symbol: String::new(),
            flash_mint: String::new(),
            flash_decimals: 0,
            flash_token_program: String::new(),
            flash_mint_burn_enabled: false,
            flash_shares_per_token: 1.0,
            ondo_shares_per_token: 1.0,
            max_trade_usd: Some(25.0),
            min_net_profit_bps: None,
            max_oracle_confidence_bps: None,
            max_oracle_age_ms: None,
            regular_hours_only: None,
            source: String::new(),
        };
        let oracle = PythOraclePrice {
            feed_id: asset.pyth_feed_id.clone(),
            price_usd: 25.0,
            confidence_usd: 0.001,
            confidence_bps: 0.4,
            publish_time: 1,
            age_ms: 1_000,
            ema_price_usd: None,
            status: OracleStatus::Valid,
            market_session: "us_equity_regular_open_slot_1".to_string(),
        };
        let route = RwaPrimaryRouteSnapshot {
            provider: "ondo_jupiter_rfq".to_string(),
            symbol: "SPYon".to_string(),
            mint: "ONDO".to_string(),
            decimals: 9,
            shares_per_token: 1.0,
            source: "test".to_string(),
            price_usd: 25.0,
            normalized_price_usd: 25.0,
            mint_enabled: true,
            burn_enabled: true,
            rfq_mint_available: false,
            rfq_burn_available: false,
            rfq_mint_price_usd: None,
            rfq_burn_price_usd: None,
            rfq_mint_route: None,
            rfq_burn_route: None,
            non_rfq_mint_price_usd: None,
            non_rfq_burn_price_usd: None,
            non_rfq_mint_route: None,
            non_rfq_burn_route: None,
            external_builder_configured: false,
            readiness: RwaPrimaryRouteReadiness::WaitingForRfq,
            executable: true,
            reason: "test".to_string(),
        };
        let quote_asset = RwaQuoteAsset {
            symbol: "USDC".to_string(),
            mint: USDC_MINT.to_string(),
            decimals: 6,
            usd_per_unit: 1.0,
        };
        let mut route_snapshots = vec![route];
        annotate_primary_route_quote_availability(&mut route_snapshots, &[Some(&issuer)]);
        assert!(route_snapshots[0].rfq_mint_available);
        assert!(route_snapshots[0].rfq_burn_available);
        assert_eq!(
            route_snapshots[0].readiness,
            RwaPrimaryRouteReadiness::AuthMissing
        );
        assert_eq!(route_snapshots[0].rfq_mint_price_usd, Some(25.0));
        assert_eq!(route_snapshots[0].rfq_burn_price_usd, Some(25.0));

        let opportunities = primary_route_opportunities(
            &config,
            &asset,
            &oracle,
            &quote_asset,
            &[Some(&issuer)],
            &route_snapshots,
            &FeeBreakdown::default(),
        );
        let mint = opportunities
            .iter()
            .find(|opportunity| opportunity.kind == RwaOpportunityKind::PrimaryMintSell)
            .unwrap();
        assert_eq!(mint.execution_legs[0].route, rfq_buy.pool);
        assert!(mint.execution_legs[1].route.contains("aggregator:metis"));

        let burn = opportunities
            .iter()
            .find(|opportunity| opportunity.kind == RwaOpportunityKind::PrimaryBuyBurn)
            .unwrap();
        assert!(burn.execution_legs[0].route.contains("aggregator:metis"));
        assert_eq!(burn.execution_legs[1].route, rfq_sell.pool);
    }

    #[test]
    fn primary_route_readiness_distinguishes_oracle_auth_and_rfq_gates() {
        assert_eq!(
            primary_route_readiness(false, true, true, false, true),
            RwaPrimaryRouteReadiness::OracleBlocked
        );
        assert_eq!(
            primary_route_readiness(true, true, false, false, true),
            RwaPrimaryRouteReadiness::AuthMissing
        );
        assert_eq!(
            primary_route_readiness(true, true, true, false, true),
            RwaPrimaryRouteReadiness::RfqReady
        );
        assert_eq!(
            primary_route_readiness(true, true, true, false, false),
            RwaPrimaryRouteReadiness::WaitingForRfq
        );
        assert_eq!(
            primary_route_readiness(true, false, false, true, false),
            RwaPrimaryRouteReadiness::ExternalBuilderReady
        );
        assert_eq!(
            primary_route_readiness(true, false, false, false, false),
            RwaPrimaryRouteReadiness::Disabled
        );
    }

    #[test]
    fn primary_route_scores_rfq_on_each_quote_asset() {
        let runtime = test_asset("SPY", "XSTOCK", "ONDO");
        let asset = &runtime.asset;
        let config = RwaConfig {
            enabled: true,
            execution_enabled: true,
            assets_file: "rwa_assets.toml".to_string(),
            mode: RwaMode::Neutral,
            directional_enabled: false,
            ondo_primary_enabled: true,
            flash_primary_enabled: false,
            quote_assets: vec!["USDC".to_string(), "USDT".to_string()],
            min_net_profit_bps: 0.0,
            fee_safety_buffer_bps: 0.0,
            max_trade_usd_per_asset: 25.0,
            max_oracle_confidence_bps: 10.0,
            max_oracle_age_ms: 90_000,
            regular_hours_only: true,
            emit_execution_candidates: true,
            max_live_trades_per_cycle: 1,
            max_assets_per_cycle: 8,
        };
        let oracle = PythOraclePrice {
            feed_id: asset.pyth_feed_id.clone(),
            price_usd: 25.0,
            confidence_usd: 0.001,
            confidence_bps: 0.4,
            publish_time: 1,
            age_ms: 1_000,
            ema_price_usd: None,
            status: OracleStatus::Valid,
            market_session: "us_equity_regular_open_slot_1".to_string(),
        };
        let mut oracle_prices = HashMap::new();
        oracle_prices.insert(asset.pyth_feed_id.clone(), oracle);
        let usdc_pair = TokenPair {
            base_mint: asset.ondo_mint.clone(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: asset.ondo_decimals,
            quote_decimals: 6,
        };
        let usdt_pair = TokenPair {
            base_mint: asset.ondo_mint.clone(),
            quote_mint: USDT_MINT.to_string(),
            base_decimals: asset.ondo_decimals,
            quote_decimals: 6,
        };
        let quotes = vec![
            Quote {
                venue: Venue::Jupiter,
                pool: "jupiter-v2-order:aggregator:metis:USDC".to_string(),
                pair: usdc_pair.clone(),
                side: SwapSide::QuoteToBase,
                in_amount_atoms: 25_000_000,
                out_amount_atoms: 1_000_000_000,
                effective_price: 25.0,
                fee: FeeBreakdown::default(),
                slot: 1,
            },
            Quote {
                venue: Venue::Jupiter,
                pool: "jupiter-v2-order:aggregator:metis:USDC".to_string(),
                pair: usdc_pair,
                side: SwapSide::BaseToQuote,
                in_amount_atoms: 1_000_000_000,
                out_amount_atoms: 24_000_000,
                effective_price: 24.0,
                fee: FeeBreakdown::default(),
                slot: 1,
            },
            Quote {
                venue: Venue::Jupiter,
                pool: "jupiter-v2-order:rfq:jupiterz:USDT".to_string(),
                pair: usdt_pair.clone(),
                side: SwapSide::QuoteToBase,
                in_amount_atoms: 25_000_000,
                out_amount_atoms: 1_000_000_000,
                effective_price: 25.0,
                fee: FeeBreakdown::default(),
                slot: 1,
            },
            Quote {
                venue: Venue::Jupiter,
                pool: "jupiter-v2-order:aggregator:metis:USDT".to_string(),
                pair: usdt_pair,
                side: SwapSide::BaseToQuote,
                in_amount_atoms: 1_000_000_000,
                out_amount_atoms: 26_000_000,
                effective_price: 26.0,
                fee: FeeBreakdown::default(),
                slot: 1,
            },
        ];
        let quote_assets = quote_assets_from_config(&config.quote_assets, 100.0);
        let quote_index = QuoteIndex::new(quotes);
        let (snapshot, opportunities) = evaluate_asset(
            &config,
            &runtime,
            &quote_assets,
            &oracle_prices,
            &quote_index,
            &FeeBreakdown::default(),
        );

        let route = snapshot
            .primary_routes
            .iter()
            .find(|route| route.provider == "ondo_jupiter_rfq")
            .unwrap();
        assert!(route.rfq_mint_available);
        assert!(route.rfq_mint_route.as_ref().unwrap().contains("USDT"));

        let usdt_mint = opportunities
            .iter()
            .find(|opportunity| {
                opportunity.kind == RwaOpportunityKind::PrimaryMintSell
                    && opportunity.quote_asset.as_deref() == Some("USDT")
            })
            .unwrap();
        assert_eq!(usdt_mint.from_mint, USDT_MINT);
        assert!(usdt_mint.execution_legs[0].route.contains("jupiterz"));
        assert!(usdt_mint.execution_legs[1].route.contains("USDT"));
    }

    #[test]
    fn primary_route_without_rfq_leg_waits_for_executable_primary_path() {
        let sell = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:Whirlpool".to_string(),
            pair: TokenPair {
                base_mint: "ONDO".to_string(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 9,
                quote_decimals: 6,
            },
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 1_000_000_000,
            out_amount_atoms: 26_000_000,
            effective_price: 26.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };

        assert!(primary_mint_sell_execution_legs(None, &sell, 25.0).is_empty());
        assert!(!is_jupiter_v2_rfq_route(
            "jupiter-v2-order:aggregator:metis:Whirlpool"
        ));
        assert!(is_jupiter_v2_rfq_route(
            "jupiter-v2-order:rfq:dflow:DFlow RFQ"
        ));
        assert!(is_jupiter_v2_rfq_route(
            "jupiter-v2-order:aggregator:jupiterz:JupiterZ"
        ));
        assert!(is_jupiter_v2_rfq_route(
            "jupiter-v2-order:rfq:jupiterz:JupiterZ"
        ));
        assert!(is_jupiter_v2_rfq_route(
            "jupiter-v2-order:rfq:unknown:JupiterZ"
        ));
    }

    #[test]
    fn primary_route_executable_requires_live_rfq_or_builder() {
        let issuer = IssuerQuotes {
            symbol: "SPYon".to_string(),
            mint: "ONDO".to_string(),
            decimals: 9,
            shares_per_token: 1.0,
            quote_usd_per_unit: 1.0,
            best_buy: None,
            best_sell: None,
            secondary_buy: None,
            secondary_sell: None,
            rfq_buy: None,
            rfq_sell: None,
        };
        let mut route = vec![RwaPrimaryRouteSnapshot {
            provider: "ondo_jupiter_rfq".to_string(),
            symbol: "SPYon".to_string(),
            mint: "ONDO".to_string(),
            decimals: 9,
            shares_per_token: 1.0,
            source: "test".to_string(),
            price_usd: 25.0,
            normalized_price_usd: 25.0,
            mint_enabled: true,
            burn_enabled: true,
            rfq_mint_available: false,
            rfq_burn_available: false,
            rfq_mint_price_usd: None,
            rfq_burn_price_usd: None,
            rfq_mint_route: None,
            rfq_burn_route: None,
            non_rfq_mint_price_usd: None,
            non_rfq_burn_price_usd: None,
            non_rfq_mint_route: None,
            non_rfq_burn_route: None,
            external_builder_configured: false,
            readiness: RwaPrimaryRouteReadiness::WaitingForRfq,
            executable: true,
            reason: "Jupiter v2 RFQ execution is configured; a live RFQ quote is still required before any Ondo primary mint/burn leg becomes executable".to_string(),
        }];

        annotate_primary_route_quote_availability(&mut route, &[Some(&issuer)]);

        assert!(!route[0].rfq_mint_available);
        assert!(!route[0].rfq_burn_available);
        assert!(!route[0].executable);
    }

    #[test]
    fn primary_route_records_non_rfq_secondary_routes_without_enabling_primary() {
        let best_buy = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:Whirlpool+Meteora DLMM".to_string(),
            pair: TokenPair {
                base_mint: "ONDO".to_string(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 9,
                quote_decimals: 6,
            },
            side: SwapSide::QuoteToBase,
            in_amount_atoms: 25_000_000,
            out_amount_atoms: 1_000_000_000,
            effective_price: 25.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let best_sell = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:Meteora DLMM".to_string(),
            pair: TokenPair {
                base_mint: "ONDO".to_string(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 9,
                quote_decimals: 6,
            },
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 1_000_000_000,
            out_amount_atoms: 24_000_000,
            effective_price: 24.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };
        let issuer = IssuerQuotes {
            symbol: "SPYon".to_string(),
            mint: "ONDO".to_string(),
            decimals: 9,
            shares_per_token: 1.0,
            quote_usd_per_unit: 1.0,
            best_buy: Some(best_buy.clone()),
            best_sell: Some(best_sell.clone()),
            secondary_buy: Some(best_buy),
            secondary_sell: Some(best_sell),
            rfq_buy: None,
            rfq_sell: None,
        };
        let mut route = vec![RwaPrimaryRouteSnapshot {
            provider: "ondo_jupiter_rfq".to_string(),
            symbol: "SPYon".to_string(),
            mint: "ONDO".to_string(),
            decimals: 9,
            shares_per_token: 1.0,
            source: "test".to_string(),
            price_usd: 25.0,
            normalized_price_usd: 25.0,
            mint_enabled: true,
            burn_enabled: true,
            rfq_mint_available: false,
            rfq_burn_available: false,
            rfq_mint_price_usd: None,
            rfq_burn_price_usd: None,
            rfq_mint_route: None,
            rfq_burn_route: None,
            non_rfq_mint_price_usd: None,
            non_rfq_burn_price_usd: None,
            non_rfq_mint_route: None,
            non_rfq_burn_route: None,
            external_builder_configured: false,
            readiness: RwaPrimaryRouteReadiness::WaitingForRfq,
            executable: true,
            reason: "Jupiter v2 RFQ execution is configured; a live RFQ quote is still required before any Ondo primary mint/burn leg becomes executable".to_string(),
        }];

        annotate_primary_route_quote_availability(&mut route, &[Some(&issuer)]);

        assert_eq!(route[0].non_rfq_mint_price_usd, Some(25.0));
        assert_eq!(route[0].non_rfq_burn_price_usd, Some(24.0));
        assert!(route[0]
            .non_rfq_mint_route
            .as_ref()
            .unwrap()
            .contains("metis"));
        assert!(route[0]
            .non_rfq_burn_route
            .as_ref()
            .unwrap()
            .contains("metis"));
        assert!(!route[0].rfq_mint_available);
        assert!(!route[0].rfq_burn_available);
        assert!(!route[0].executable);
        assert!(route[0].reason.contains("non-RFQ secondary"));
    }

    #[test]
    fn primary_routes_remain_visible_when_oracle_is_stale() {
        let config = RwaConfig {
            enabled: true,
            execution_enabled: true,
            assets_file: "rwa_assets.toml".to_string(),
            mode: RwaMode::Neutral,
            directional_enabled: false,
            ondo_primary_enabled: true,
            flash_primary_enabled: false,
            quote_assets: vec!["USDC".to_string()],
            min_net_profit_bps: 75.0,
            fee_safety_buffer_bps: 15.0,
            max_trade_usd_per_asset: 25.0,
            max_oracle_confidence_bps: 10.0,
            max_oracle_age_ms: 90_000,
            regular_hours_only: true,
            emit_execution_candidates: true,
            max_live_trades_per_cycle: 0,
            max_assets_per_cycle: 8,
        };
        let asset = RwaAssetConfig {
            ticker: "SPY".to_string(),
            enabled: true,
            pyth_feed_id: "19e09bb805456ada3979a7d1cbb4b6d63babc3a0f8e8a9509f68afa5c4c11cd5"
                .to_string(),
            xstock_symbol: "SPYx".to_string(),
            xstock_mint: "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W".to_string(),
            xstock_decimals: 8,
            xstock_token_program: String::new(),
            ondo_symbol: "SPYon".to_string(),
            ondo_mint: "k18WJUULWheRkSpSquYGdNNmtuE2Vbw1hpuUi92ondo".to_string(),
            ondo_decimals: 9,
            ondo_token_program: String::new(),
            flash_symbol: String::new(),
            flash_mint: String::new(),
            flash_decimals: 0,
            flash_token_program: String::new(),
            flash_mint_burn_enabled: false,
            flash_shares_per_token: 1.0,
            ondo_shares_per_token: 1.0059,
            max_trade_usd: None,
            min_net_profit_bps: None,
            max_oracle_confidence_bps: None,
            max_oracle_age_ms: None,
            regular_hours_only: None,
            source: String::new(),
        };
        let oracle = PythOraclePrice {
            feed_id: asset.pyth_feed_id.clone(),
            price_usd: 750.0,
            confidence_usd: 0.1,
            confidence_bps: 1.3,
            publish_time: 1,
            age_ms: 1_000_000,
            ema_price_usd: None,
            status: OracleStatus::Stale,
            market_session: "regular_feed_slot_1".to_string(),
        };

        let routes = primary_routes_for_asset(&config, &asset, Some(&oracle));
        assert_eq!(routes.len(), 1);
        assert!(routes.iter().all(|route| !route.executable));
        assert!(routes
            .iter()
            .all(|route| route.readiness == RwaPrimaryRouteReadiness::OracleBlocked));
        assert!(routes
            .iter()
            .all(|route| route.reason.contains("Pyth oracle stale")));
    }

    #[test]
    fn external_primary_ondo_leg_uses_builder_route_prefix() {
        let route = RwaPrimaryRouteSnapshot {
            provider: "ondo_jupiter_rfq".to_string(),
            symbol: "SPYon".to_string(),
            mint: "11111111111111111111111111111112".to_string(),
            decimals: 9,
            shares_per_token: 1.0,
            source: "test".to_string(),
            price_usd: 750.0,
            normalized_price_usd: 750.0,
            mint_enabled: true,
            burn_enabled: true,
            rfq_mint_available: false,
            rfq_burn_available: false,
            rfq_mint_price_usd: None,
            rfq_burn_price_usd: None,
            rfq_mint_route: None,
            rfq_burn_route: None,
            non_rfq_mint_price_usd: None,
            non_rfq_burn_price_usd: None,
            non_rfq_mint_route: None,
            non_rfq_burn_route: None,
            external_builder_configured: true,
            readiness: RwaPrimaryRouteReadiness::ExternalBuilderReady,
            executable: true,
            reason: "test".to_string(),
        };
        let sell = Quote {
            venue: Venue::Jupiter,
            pool: "jupiter-v2-order:aggregator:metis:Whirlpool".to_string(),
            pair: TokenPair {
                base_mint: route.mint.clone(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 9,
                quote_decimals: 6,
            },
            side: SwapSide::BaseToQuote,
            in_amount_atoms: 10_000_000,
            out_amount_atoms: 25_000_000,
            effective_price: 25.0,
            fee: FeeBreakdown::default(),
            slot: 1,
        };

        let legs = external_primary_mint_sell_execution_legs(
            &route,
            USDC_MINT.to_string(),
            6,
            25_000_000,
            &sell,
            25.0,
        );

        assert_eq!(legs.len(), 2);
        assert_eq!(legs[0].route, "external-primary:ondo_jupiter_rfq:mint");
        assert_eq!(legs[0].input_atoms, 25_000_000);
        assert_eq!(legs[1].route, sell.pool);
    }

    #[test]
    fn rejects_pump_mint_in_registry_validation() {
        let asset = RwaAssetConfig {
            ticker: "BAD".to_string(),
            enabled: true,
            pyth_feed_id: "19e09bb805456ada3979a7d1cbb4b6d63babc3a0f8e8a9509f68afa5c4c11cd5"
                .to_string(),
            xstock_symbol: "BADx".to_string(),
            xstock_mint: "11111111111111111111111111111111pump".to_string(),
            xstock_decimals: 8,
            xstock_token_program: String::new(),
            ondo_symbol: "BADon".to_string(),
            ondo_mint: "k18WJUULWheRkSpSquYGdNNmtuE2Vbw1hpuUi92ondo".to_string(),
            ondo_decimals: 9,
            ondo_token_program: String::new(),
            flash_symbol: String::new(),
            flash_mint: String::new(),
            flash_decimals: 0,
            flash_token_program: String::new(),
            flash_mint_burn_enabled: false,
            flash_shares_per_token: 1.0,
            ondo_shares_per_token: 1.0,
            max_trade_usd: None,
            min_net_profit_bps: None,
            max_oracle_confidence_bps: None,
            max_oracle_age_ms: None,
            regular_hours_only: None,
            source: String::new(),
        };
        assert!(validate_asset("BAD", &asset).is_err());
    }

    #[test]
    fn rejects_blacklisted_remora_symbol_in_registry_validation() {
        for (ticker, symbol) in [("SPY", "SPYr"), ("NVDA", "NVDAr"), ("TSLA", "TSLAr")] {
            let mut asset = test_asset(
                ticker,
                "11111111111111111111111111111111",
                "22222222222222222222222222222222",
            )
            .asset;
            asset.flash_symbol = symbol.to_string();
            asset.flash_mint = "11111111111111111111111111111112".to_string();
            asset.flash_decimals = 9;
            asset.flash_mint_burn_enabled = true;

            let err = validate_asset(ticker, &asset).expect_err("unsupported wrapper symbol");
            assert!(err
                .to_string()
                .contains("Flash/Remora issuer fields are no longer supported"));
        }
    }

    #[test]
    fn rejects_blacklisted_remora_symbol_in_any_issuer_slot() {
        let mut xstock_asset = test_asset(
            "SPY",
            "11111111111111111111111111111111",
            "22222222222222222222222222222222",
        )
        .asset;
        xstock_asset.xstock_symbol = "SPYr".to_string();
        let err = validate_asset("SPY", &xstock_asset).expect_err("blacklisted xstock symbol");
        assert!(err.to_string().contains("xstock_symbol SPYr"));

        let mut ondo_asset = test_asset(
            "NVDA",
            "11111111111111111111111111111111",
            "22222222222222222222222222222222",
        )
        .asset;
        ondo_asset.ondo_symbol = "NVDAr".to_string();
        let err = validate_asset("NVDA", &ondo_asset).expect_err("blacklisted ondo symbol");
        assert!(err.to_string().contains("ondo_symbol NVDAr"));
    }

    #[test]
    fn rejects_blacklisted_remora_mint_prefix_in_registry_validation() {
        let mut asset = test_asset(
            "FAKE",
            "11111111111111111111111111111111",
            "22222222222222222222222222222222",
        )
        .asset;
        asset.flash_symbol = "FAKEr".to_string();
        asset.flash_mint = "AVw21111111111111111111111111111111".to_string();
        asset.flash_decimals = 9;
        asset.flash_mint_burn_enabled = true;

        let err = validate_asset("FAKE", &asset).expect_err("unsupported wrapper mint");
        assert!(err
            .to_string()
            .contains("Flash/Remora issuer fields are no longer supported"));
    }

    #[test]
    fn rejects_blacklisted_remora_mint_prefix_in_any_issuer_slot() {
        let xstock_asset = test_asset(
            "FAKE",
            "AVw21111111111111111111111111111111",
            "22222222222222222222222222222222",
        )
        .asset;
        let err = validate_asset("FAKE", &xstock_asset).expect_err("blacklisted xstock mint");
        assert!(err.to_string().contains("xstock_mint AVw211"));

        let mut ondo_asset = test_asset(
            "FAKE",
            "11111111111111111111111111111111",
            "FJug11111111111111111111111111111111",
        )
        .asset;
        ondo_asset.ondo_symbol = "FAKEon".to_string();
        let err = validate_asset("FAKE", &ondo_asset).expect_err("blacklisted ondo mint");
        assert!(err.to_string().contains("ondo_mint FJug11"));
    }

    #[test]
    fn skips_blacklisted_remora_issuer_and_primary_route_at_runtime() {
        let mut config = RwaConfig::default();
        config.ondo_primary_enabled = false;
        config.flash_primary_enabled = true;
        let mut asset = test_asset(
            "SPY",
            "11111111111111111111111111111111",
            "22222222222222222222222222222222",
        )
        .asset;
        asset.flash_symbol = "SPYr".to_string();
        asset.flash_mint = "11111111111111111111111111111112".to_string();
        asset.flash_decimals = 9;
        asset.flash_mint_burn_enabled = true;
        let oracle = PythOraclePrice {
            feed_id: asset.pyth_feed_id.clone(),
            price_usd: 750.0,
            confidence_usd: 0.1,
            confidence_bps: 1.3,
            publish_time: 1,
            age_ms: 1_000,
            ema_price_usd: None,
            status: OracleStatus::Valid,
            market_session: "us_equity_regular_open_slot_1".to_string(),
        };

        let issuers = configured_issuers(&asset);
        assert!(issuers.iter().all(|issuer| issuer.mint != asset.flash_mint));

        let routes = primary_routes_for_asset(&config, &asset, Some(&oracle));
        assert!(routes.iter().all(|route| route.provider != "flash_primary"));
    }

    #[test]
    fn configured_issuers_drop_blacklisted_remora_symbols_and_mints() {
        let mut asset = test_asset(
            "SPY",
            "AVw21111111111111111111111111111111",
            "22222222222222222222222222222222",
        )
        .asset;
        asset.xstock_symbol = "SPYr".to_string();
        asset.flash_symbol = "TSLAr".to_string();
        asset.flash_mint = "FJug11111111111111111111111111111111".to_string();
        asset.flash_decimals = 9;
        asset.flash_mint_burn_enabled = true;

        let issuers = configured_issuers(&asset);
        assert!(issuers.iter().all(|issuer| {
            !is_blocklisted_remora_symbol(&issuer.symbol)
                && !is_blocklisted_remora_mint(&issuer.mint)
        }));
        assert_eq!(issuers.len(), 1);
        assert_eq!(issuers[0].symbol, asset.ondo_symbol);
    }
}
