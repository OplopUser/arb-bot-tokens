use crate::types::{USDC_MINT, USDT_MINT, WSOL_MINT};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{env, path::Path};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BotConfig {
    pub rpc: RpcConfig,
    pub wallet: WalletConfig,
    #[serde(default)]
    pub database: DatabaseConfig,
    pub trading: TradingConfig,
    #[serde(default)]
    pub rwa: RwaConfig,
    #[serde(default)]
    pub discovery: DiscoveryConfig,
    pub fees: FeesConfig,
    pub venues: VenuesConfig,
    #[serde(default)]
    pub jupiter: JupiterConfig,
    #[serde(default)]
    pub aggregators: AggregatorsConfig,
    #[serde(default)]
    pub simulation: SimulationConfig,
    #[serde(default)]
    pub execution: ExecutionConfig,
    pub submitters: SubmittersConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcConfig {
    pub primary_http: String,
    pub primary_ws: String,
    pub fallback_http: Vec<String>,
    pub commitment: String,
    #[serde(default = "default_rpc_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletConfig {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_database_path")]
    pub path: String,
    #[serde(default = "default_true")]
    pub migrations_on_startup: bool,
    #[serde(default = "default_database_busy_timeout_ms")]
    pub busy_timeout_ms: u64,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            enabled: env_bool("ARB_DATABASE_ENABLED").unwrap_or(false),
            path: env::var("ARB_DATABASE_PATH").unwrap_or_else(|_| default_database_path()),
            migrations_on_startup: env_bool("ARB_DATABASE_MIGRATIONS_ON_STARTUP").unwrap_or(true),
            busy_timeout_ms: env_u64("ARB_DATABASE_BUSY_TIMEOUT_MS")
                .unwrap_or_else(default_database_busy_timeout_ms),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingConfig {
    pub live: bool,
    pub trade_size_usd: f64,
    pub min_net_profit_bps: f64,
    pub fee_safety_buffer_bps: f64,
    pub max_active_tokens: usize,
    pub scan_interval_ms: u64,
    pub quote_mints: Vec<String>,
    pub major_denylist: Vec<String>,
    pub sol_usd_estimate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationConfig {
    pub enabled: bool,
    pub max_history: usize,
    pub max_trades_per_cycle: usize,
    pub min_interval_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    pub enabled: bool,
    #[serde(default)]
    pub submit_enabled: bool,
    pub max_live_smoke_trade_usd: f64,
    pub max_live_trade_usd: f64,
    #[serde(default)]
    pub min_absolute_profit_usd: f64,
    pub max_live_trades_per_session: u64,
    pub max_live_trades_per_cycle: usize,
    pub max_live_candidates_per_cycle: usize,
    pub min_interval_ms: u64,
    #[serde(default = "default_jupiter_requests_per_window")]
    pub jupiter_requests_per_window: usize,
    #[serde(default = "default_jupiter_window_ms")]
    pub jupiter_window_ms: u64,
    #[serde(default = "default_jupiter_estimated_requests_per_candidate")]
    pub jupiter_estimated_requests_per_candidate: usize,
    #[serde(default = "default_jupiter_rate_limit_cooldown_ms")]
    pub jupiter_rate_limit_cooldown_ms: u64,
    #[serde(default = "default_jupiter_probe_enabled")]
    pub jupiter_probe_enabled: bool,
    pub min_requote_profit_bps: f64,
    pub require_simulation: bool,
    pub confirmation_timeout_ms: u64,
    pub jupiter_swap_url: String,
    pub jupiter_swap_instructions_url: String,
    pub jupiter_only_direct_routes: bool,
    pub jupiter_use_venue_filters: bool,
    pub jupiter_max_accounts: u16,
    pub compute_unit_limit: u32,
    pub compute_unit_price_micro_lamports: u64,
    #[serde(default = "default_stale_candidate_ttl_ms")]
    pub stale_candidate_ttl_ms: u64,
    #[serde(default)]
    pub native_prefunded_hot_accounts: bool,
    #[serde(default)]
    pub address_lookup_tables: Vec<String>,
    #[serde(default)]
    pub execution_profile: String,
    #[serde(default)]
    pub native_price_only_execution_enabled: bool,
    #[serde(default)]
    pub native_scout_min_net_profit_bps: f64,
    #[serde(default)]
    pub jupiter_probe_trade_sizes_usd: Vec<f64>,
    #[serde(default)]
    pub direct_trade_sizes_usd: Vec<f64>,
    #[serde(default)]
    pub native_focus_routes: Vec<NativeFocusRoute>,
    #[serde(default = "default_ws_subscription_budget")]
    pub ws_subscription_budget: usize,
    #[serde(default)]
    pub jito_block_engine_url: String,
    #[serde(default = "default_max_pool_slot_age")]
    pub max_pool_slot_age: u64,
    #[serde(default)]
    pub max_live_fee_loss_usd_per_session: f64,
    #[serde(default)]
    pub max_failed_transactions_per_session: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeFocusRoute {
    pub base_mint: String,
    pub quote_mint: String,
    pub buy_venue: String,
    pub sell_venue: String,
    #[serde(default)]
    pub require_fresh_accounts: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RwaConfig {
    #[serde(default = "default_rwa_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub execution_enabled: bool,
    #[serde(default = "default_rwa_assets_file")]
    pub assets_file: String,
    #[serde(default)]
    pub mode: RwaMode,
    #[serde(default)]
    pub directional_enabled: bool,
    #[serde(default)]
    pub ondo_primary_enabled: bool,
    #[serde(default = "default_rwa_flash_primary_enabled")]
    pub flash_primary_enabled: bool,
    #[serde(default = "default_rwa_quote_assets")]
    pub quote_assets: Vec<String>,
    #[serde(default = "default_rwa_min_net_profit_bps")]
    pub min_net_profit_bps: f64,
    #[serde(default = "default_rwa_fee_safety_buffer_bps")]
    pub fee_safety_buffer_bps: f64,
    #[serde(default = "default_rwa_max_trade_usd_per_asset")]
    pub max_trade_usd_per_asset: f64,
    #[serde(default = "default_rwa_max_oracle_confidence_bps")]
    pub max_oracle_confidence_bps: f64,
    #[serde(default = "default_rwa_max_oracle_age_ms")]
    pub max_oracle_age_ms: u64,
    #[serde(default = "default_true")]
    pub regular_hours_only: bool,
    #[serde(default)]
    pub emit_execution_candidates: bool,
    #[serde(default = "default_rwa_max_live_trades_per_cycle")]
    pub max_live_trades_per_cycle: usize,
    #[serde(default = "default_rwa_max_assets_per_cycle")]
    pub max_assets_per_cycle: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RwaMode {
    Neutral,
    Directional,
    Both,
}

impl Default for RwaMode {
    fn default() -> Self {
        Self::Neutral
    }
}

impl Default for RwaConfig {
    fn default() -> Self {
        Self {
            enabled: default_rwa_enabled(),
            execution_enabled: env_bool("RWA_EXECUTION_ENABLED").unwrap_or(false),
            assets_file: default_rwa_assets_file(),
            mode: default_rwa_mode(),
            directional_enabled: env_bool("RWA_DIRECTIONAL_ENABLED").unwrap_or(false),
            ondo_primary_enabled: env_bool("RWA_ONDO_PRIMARY_ENABLED")
                .or_else(|| env_bool("ONDO_PRIMARY_ENABLED"))
                .unwrap_or(false),
            flash_primary_enabled: default_rwa_flash_primary_enabled(),
            quote_assets: default_rwa_quote_assets(),
            min_net_profit_bps: default_rwa_min_net_profit_bps(),
            fee_safety_buffer_bps: default_rwa_fee_safety_buffer_bps(),
            max_trade_usd_per_asset: default_rwa_max_trade_usd_per_asset(),
            max_oracle_confidence_bps: default_rwa_max_oracle_confidence_bps(),
            max_oracle_age_ms: default_rwa_max_oracle_age_ms(),
            regular_hours_only: true,
            emit_execution_candidates: env_bool("RWA_EMIT_EXECUTION_CANDIDATES").unwrap_or(false),
            max_live_trades_per_cycle: default_rwa_max_live_trades_per_cycle(),
            max_assets_per_cycle: default_rwa_max_assets_per_cycle(),
        }
    }
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            enabled: env::var("LIVE_EXECUTION_ENABLED")
                .map(|value| value == "true" || value == "1")
                .unwrap_or(false),
            submit_enabled: env_bool("LIVE_SUBMIT_ENABLED").unwrap_or(false),
            max_live_smoke_trade_usd: env::var("MAX_LIVE_SMOKE_TRADE_USD")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1.0),
            max_live_trade_usd: env::var("MAX_LIVE_TRADE_USD")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1.0),
            min_absolute_profit_usd: env::var("LIVE_MIN_ABSOLUTE_PROFIT_USD")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(0.0),
            max_live_trades_per_session: env::var("MAX_LIVE_TRADES_PER_SESSION")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1),
            max_live_trades_per_cycle: env::var("MAX_LIVE_TRADES_PER_CYCLE")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1),
            max_live_candidates_per_cycle: env::var("MAX_LIVE_CANDIDATES_PER_CYCLE")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(3),
            min_interval_ms: env::var("LIVE_EXECUTION_MIN_INTERVAL_MS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(0),
            jupiter_requests_per_window: default_jupiter_requests_per_window(),
            jupiter_window_ms: default_jupiter_window_ms(),
            jupiter_estimated_requests_per_candidate:
                default_jupiter_estimated_requests_per_candidate(),
            jupiter_rate_limit_cooldown_ms: default_jupiter_rate_limit_cooldown_ms(),
            jupiter_probe_enabled: default_jupiter_probe_enabled(),
            min_requote_profit_bps: env::var("LIVE_MIN_REQUOTE_PROFIT_BPS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(25.0),
            require_simulation: env::var("LIVE_REQUIRE_SIMULATION")
                .map(|value| value != "false" && value != "0")
                .unwrap_or(true),
            confirmation_timeout_ms: env::var("LIVE_CONFIRMATION_TIMEOUT_MS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(45_000),
            jupiter_swap_url: default_jupiter_swap_url(),
            jupiter_swap_instructions_url: default_jupiter_swap_instructions_url(),
            jupiter_only_direct_routes: env::var("JUPITER_ONLY_DIRECT_ROUTES")
                .map(|value| value != "false" && value != "0")
                .unwrap_or(true),
            jupiter_use_venue_filters: env::var("JUPITER_USE_VENUE_FILTERS")
                .map(|value| value != "false" && value != "0")
                .unwrap_or(true),
            jupiter_max_accounts: env::var("JUPITER_MAX_ACCOUNTS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(64),
            compute_unit_limit: env::var("LIVE_COMPUTE_UNIT_LIMIT")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1_400_000),
            compute_unit_price_micro_lamports: env::var("LIVE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(0),
            stale_candidate_ttl_ms: env::var("LIVE_STALE_CANDIDATE_TTL_MS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or_else(default_stale_candidate_ttl_ms),
            native_prefunded_hot_accounts: env::var("NATIVE_PREFUNDED_HOT_ACCOUNTS")
                .map(|value| value == "true" || value == "1")
                .unwrap_or(false),
            address_lookup_tables: env::var("LIVE_ADDRESS_LOOKUP_TABLES")
                .ok()
                .map(|value| {
                    value
                        .split(',')
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(ToOwned::to_owned)
                        .collect()
                })
                .unwrap_or_default(),
            execution_profile: env::var("LIVE_EXECUTION_PROFILE")
                .unwrap_or_else(|_| "live_normal".to_string()),
            native_price_only_execution_enabled: env::var("NATIVE_PRICE_ONLY_EXECUTION_ENABLED")
                .map(|v| v != "false" && v != "0")
                .unwrap_or(true),
            native_scout_min_net_profit_bps: env::var("NATIVE_SCOUT_MIN_NET_PROFIT_BPS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0),
            jupiter_probe_trade_sizes_usd: env::var("JUPITER_PROBE_TRADE_SIZE_USD_LADDER")
                .ok()
                .map(|v| v.split(',').filter_map(|s| s.trim().parse().ok()).collect())
                .unwrap_or_else(|| vec![5.0, 10.0, 25.0]),
            direct_trade_sizes_usd: env::var("DIRECT_TRADE_SIZE_USD_LADDER")
                .ok()
                .map(|v| v.split(',').filter_map(|s| s.trim().parse().ok()).collect())
                .unwrap_or_else(|| vec![5.0, 10.0, 25.0]),
            native_focus_routes: Vec::new(),
            ws_subscription_budget: env::var("WS_SUBSCRIPTION_BUDGET")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(default_ws_subscription_budget),
            jito_block_engine_url: env::var("JITO_BLOCK_ENGINE_URL").unwrap_or_default(),
            max_pool_slot_age: env::var("MAX_POOL_SLOT_AGE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(default_max_pool_slot_age),
            max_live_fee_loss_usd_per_session: env::var("MAX_LIVE_FEE_LOSS_USD_PER_SESSION")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0),
            max_failed_transactions_per_session: env::var("MAX_FAILED_TRANSACTIONS_PER_SESSION")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
        }
    }
}

impl ExecutionConfig {
    pub fn jupiter_api_pacing_enabled(&self) -> bool {
        self.jupiter_requests_per_window > 0 && self.jupiter_window_ms > 0
    }

    pub fn jupiter_requests_budget_per_cycle(&self, scan_interval_ms: u64) -> usize {
        if !self.jupiter_api_pacing_enabled() {
            return usize::MAX;
        }

        let requests = (self.jupiter_requests_per_window as u128)
            .saturating_mul(scan_interval_ms.max(1) as u128)
            / self.jupiter_window_ms.max(1) as u128;
        requests.max(1).min(usize::MAX as u128) as usize
    }

    pub fn jupiter_candidate_budget_per_cycle(&self, scan_interval_ms: u64) -> usize {
        let requests = self.jupiter_requests_budget_per_cycle(scan_interval_ms);
        if requests == usize::MAX {
            return usize::MAX;
        }

        let requests_per_candidate = self.jupiter_estimated_requests_per_candidate.max(1);
        (requests / requests_per_candidate).max(1)
    }

    pub fn live_candidate_limit(&self, scan_interval_ms: u64, available: usize) -> usize {
        if available == 0 {
            return 0;
        }

        let configured_limit = if self.max_live_candidates_per_cycle == 0 {
            usize::MAX
        } else {
            self.max_live_candidates_per_cycle
        };
        let api_limit = self.jupiter_candidate_budget_per_cycle(scan_interval_ms);

        configured_limit.min(api_limit).min(available)
    }
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            enabled: env::var("SIMULATION_ENABLED")
                .map(|value| value == "true" || value == "1")
                .unwrap_or(true),
            max_history: env::var("SIMULATION_MAX_HISTORY")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(100),
            max_trades_per_cycle: env::var("SIMULATION_MAX_TRADES_PER_CYCLE")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1),
            min_interval_ms: env::var("SIMULATION_MIN_INTERVAL_MS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(10_000),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_discovery_refresh_interval_ms")]
    pub refresh_interval_ms: u64,
    #[serde(default = "default_true")]
    pub index_enabled: bool,
    #[serde(default = "default_pool_index_url")]
    pub index_url: String,
    #[serde(default = "default_max_index_pairs_per_token")]
    pub max_index_pairs_per_token: usize,
    #[serde(default)]
    pub min_index_liquidity_usd: f64,
    #[serde(default)]
    pub broad_rpc_enabled: bool,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            refresh_interval_ms: default_discovery_refresh_interval_ms(),
            index_enabled: true,
            index_url: default_pool_index_url(),
            max_index_pairs_per_token: default_max_index_pairs_per_token(),
            min_index_liquidity_usd: 0.0,
            broad_rpc_enabled: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeesConfig {
    #[serde(default = "default_priority_fee_percentile")]
    pub priority_fee_percentile: f64,
    #[serde(default = "default_max_priority_fee_lamports")]
    pub max_priority_fee_lamports: u64,
    #[serde(default = "default_jito_tip_lamports")]
    pub jito_tip_lamports: u64,
    #[serde(default = "default_max_jito_tip_lamports")]
    pub max_jito_tip_lamports: u64,
    #[serde(default = "default_ata_rent_lamports")]
    pub ata_rent_lamports: u64,
    #[serde(default = "default_base_fee_lamports")]
    pub base_fee_lamports: u64,
}

impl Default for FeesConfig {
    fn default() -> Self {
        Self {
            priority_fee_percentile: default_priority_fee_percentile(),
            max_priority_fee_lamports: default_max_priority_fee_lamports(),
            jito_tip_lamports: default_jito_tip_lamports(),
            max_jito_tip_lamports: default_max_jito_tip_lamports(),
            ata_rent_lamports: default_ata_rent_lamports(),
            base_fee_lamports: default_base_fee_lamports(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenuesConfig {
    pub enabled: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JupiterConfig {
    pub enabled: bool,
    pub quote_url: String,
    pub timeout_ms: u64,
    pub slippage_bps: u16,
    #[serde(default = "default_jupiter_seed_pairs")]
    pub seed_pairs: Vec<JupiterSeedPair>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JupiterSeedPair {
    pub base_mint: String,
    pub quote_mint: String,
    pub base_decimals: u8,
    pub quote_decimals: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatorsConfig {
    pub enabled: bool,
    pub request_timeout_ms: u64,
    pub cache_ttl_ms: u64,
    pub max_pairs_per_cycle: usize,
    #[serde(default = "default_aggregator_dynamic_discovery_enabled")]
    pub dynamic_discovery_enabled: bool,
    #[serde(default = "default_aggregator_dynamic_refresh_interval_ms")]
    pub dynamic_refresh_interval_ms: u64,
    #[serde(default = "default_aggregator_dynamic_max_tokens")]
    pub dynamic_max_tokens: usize,
    #[serde(default = "default_aggregator_dynamic_max_quote_mints")]
    pub dynamic_max_quote_mints: usize,
    #[serde(default = "default_aggregator_dynamic_min_liquidity_usd")]
    pub dynamic_min_liquidity_usd: f64,
    #[serde(default = "default_aggregator_dynamic_min_organic_score")]
    pub dynamic_min_organic_score: f64,
    #[serde(default = "default_aggregator_dynamic_max_mcap_usd")]
    pub dynamic_max_mcap_usd: f64,
    #[serde(default = "default_aggregator_dynamic_allow_unverified")]
    pub dynamic_allow_unverified: bool,
    #[serde(default = "default_aggregator_dynamic_min_supported_venues")]
    pub dynamic_min_supported_venues: usize,
    #[serde(default = "default_aggregator_seed_pairs")]
    pub seed_pairs: Vec<JupiterSeedPair>,
    #[serde(default = "default_aggregator_providers")]
    pub providers: Vec<AggregatorProviderConfig>,
}

impl Default for AggregatorsConfig {
    fn default() -> Self {
        Self {
            enabled: env_bool("AGGREGATORS_ENABLED").unwrap_or(true),
            request_timeout_ms: env_u64("AGGREGATOR_REQUEST_TIMEOUT_MS").unwrap_or(1_200),
            cache_ttl_ms: env_u64("AGGREGATOR_CACHE_TTL_MS").unwrap_or(10_000),
            max_pairs_per_cycle: env_usize("AGGREGATOR_MAX_PAIRS_PER_CYCLE").unwrap_or(64),
            dynamic_discovery_enabled: default_aggregator_dynamic_discovery_enabled(),
            dynamic_refresh_interval_ms: default_aggregator_dynamic_refresh_interval_ms(),
            dynamic_max_tokens: default_aggregator_dynamic_max_tokens(),
            dynamic_max_quote_mints: default_aggregator_dynamic_max_quote_mints(),
            dynamic_min_liquidity_usd: default_aggregator_dynamic_min_liquidity_usd(),
            dynamic_min_organic_score: default_aggregator_dynamic_min_organic_score(),
            dynamic_max_mcap_usd: default_aggregator_dynamic_max_mcap_usd(),
            dynamic_allow_unverified: default_aggregator_dynamic_allow_unverified(),
            dynamic_min_supported_venues: default_aggregator_dynamic_min_supported_venues(),
            seed_pairs: default_aggregator_seed_pairs(),
            providers: default_aggregator_providers(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatorProviderConfig {
    pub name: String,
    pub venue: crate::types::Venue,
    pub enabled: bool,
    pub quote_url: String,
    pub format: AggregatorFormat,
    pub timeout_ms: u64,
    pub min_interval_ms: u64,
    pub max_in_flight: usize,
    pub requires_api_key: bool,
    pub api_key_env: Option<String>,
    pub api_key_header: Option<String>,
    pub api_key_prefix: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregatorFormat {
    Jupiter,
    RaydiumTradeApi,
    CarbiumQ1,
    SpiderSwap,
    Okx,
    OpenOcean,
    Dflow,
    TitanStream,
    Vybe,
    Venum,
    Generic,
}

impl Default for JupiterConfig {
    fn default() -> Self {
        let base_url = default_jupiter_api_url();
        let quote_url = if base_url.ends_with("/quote") {
            base_url
        } else {
            format!("{}/quote", base_url.trim_end_matches('/'))
        };

        Self {
            enabled: env::var("JUPITER_REFERENCE_ENABLED")
                .map(|value| value == "true" || value == "1")
                .unwrap_or(true),
            quote_url,
            timeout_ms: env::var("JUPITER_TIMEOUT_MS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1_200),
            slippage_bps: env::var("JUPITER_SLIPPAGE_BPS")
                .ok()
                .or_else(|| env::var("MAX_SLIPPAGE_BPS").ok())
                .and_then(|value| value.parse().ok())
                .unwrap_or(50),
            seed_pairs: default_jupiter_seed_pairs(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmittersConfig {
    pub enabled: Vec<String>,
}

impl BotConfig {
    pub async fn load(path: impl AsRef<Path>) -> Result<Self> {
        let _ = dotenvy::dotenv();
        let path = path.as_ref();
        if tokio::fs::try_exists(path).await.unwrap_or(false) {
            let text = tokio::fs::read_to_string(path)
                .await
                .with_context(|| format!("failed to read {}", path.display()))?;
            let mut config: Self = toml::from_str(&text)
                .with_context(|| format!("failed to parse {}", path.display()))?;
            config.resolve_env_references();
            config.disable_unsupported_rwa_issuers();
            tracing::info!(
                "loaded config from {} (rwa.enabled={})",
                path.display(),
                config.rwa.enabled
            );
            return Ok(config);
        }

        let config = Self::from_env_defaults();
        tracing::warn!(
            "config file {} not found; using environment defaults (rwa.enabled={}, set RWA_ENABLED=true to opt in)",
            path.display(),
            config.rwa.enabled
        );
        Ok(config)
    }

    pub fn from_env_defaults() -> Self {
        let primary_http = env::var("HELIOS_RPC_URL")
            .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());
        let primary_ws = env::var("HELIOS_WS_URL").unwrap_or_else(|_| http_to_ws(&primary_http));
        let wallet_path =
            env::var("SOLANA_WALLET_JSON").unwrap_or_else(|_| ".secrets/wallet.json".to_string());

        let mut config = Self {
            rpc: RpcConfig {
                primary_http,
                primary_ws,
                fallback_http: vec!["https://api.mainnet-beta.solana.com".to_string()],
                commitment: "processed".to_string(),
                request_timeout_ms: env::var("RPC_REQUEST_TIMEOUT_MS")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or_else(default_rpc_request_timeout_ms),
            },
            wallet: WalletConfig { path: wallet_path },
            database: DatabaseConfig::default(),
            trading: TradingConfig {
                live: env_bool("TRADING_LIVE")
                    .or_else(|| env_bool("LIVE_TRADING"))
                    .or_else(|| env_bool("SIMULATION_ONLY").map(|simulation_only| !simulation_only))
                    .unwrap_or(false),
                trade_size_usd: env_f64("TRADE_SIZE_USD").unwrap_or(25.0),
                min_net_profit_bps: env_f64("MIN_NET_PROFIT_BPS").unwrap_or(15.0),
                fee_safety_buffer_bps: env_f64("FEE_SAFETY_BUFFER_BPS").unwrap_or(5.0),
                max_active_tokens: env_usize("MAX_ACTIVE_TOKENS").unwrap_or(100),
                scan_interval_ms: env_u64("SCAN_INTERVAL_MS")
                    .or_else(|| env_u64("POLL_INTERVAL_MS"))
                    .unwrap_or(5_000),
                quote_mints: vec![
                    USDC_MINT.to_string(),
                    USDT_MINT.to_string(),
                    WSOL_MINT.to_string(),
                ],
                major_denylist: vec![WSOL_MINT.to_string()],
                sol_usd_estimate: 100.0,
            },
            rwa: RwaConfig::default(),
            discovery: DiscoveryConfig::default(),
            fees: FeesConfig::default(),
            venues: VenuesConfig {
                enabled: vec![
                    "orca".to_string(),
                    "meteora".to_string(),
                    "raydium".to_string(),
                ],
            },
            jupiter: JupiterConfig::default(),
            aggregators: AggregatorsConfig::default(),
            simulation: SimulationConfig::default(),
            execution: ExecutionConfig::default(),
            submitters: SubmittersConfig {
                enabled: vec!["primary_rpc".to_string(), "fallback_rpcs".to_string()],
            },
        };
        config.disable_unsupported_rwa_issuers();
        config
    }

    fn disable_unsupported_rwa_issuers(&mut self) {
        self.rwa.flash_primary_enabled = false;
    }

    pub fn rwa_only_mode(&self) -> bool {
        self.rwa.enabled
            && !self.discovery.enabled
            && !self.discovery.index_enabled
            && !self.discovery.broad_rpc_enabled
            && !self.simulation.enabled
            && !self.execution.enabled
            && !self.execution.submit_enabled
            && !self.execution.native_price_only_execution_enabled
            && !self.execution.jupiter_probe_enabled
            && self.execution.native_focus_routes.is_empty()
            && !self.aggregators.dynamic_discovery_enabled
            && self.jupiter.seed_pairs.is_empty()
    }

    fn resolve_env_references(&mut self) {
        self.rpc.primary_http = resolve_env_reference(&self.rpc.primary_http);
        self.rpc.primary_ws = resolve_env_reference(&self.rpc.primary_ws);
        if self.rpc.primary_ws.trim().is_empty() {
            self.rpc.primary_ws = http_to_ws(&self.rpc.primary_http);
        }
        for url in &mut self.rpc.fallback_http {
            *url = resolve_env_reference(url);
        }
        self.wallet.path = resolve_env_reference(&self.wallet.path);
        self.database.path = resolve_env_reference(&self.database.path);
        self.execution.jito_block_engine_url =
            resolve_env_reference(&self.execution.jito_block_engine_url);
    }
}

fn http_to_ws(http: &str) -> String {
    if let Some(rest) = http.strip_prefix("https://") {
        format!("wss://{}", rest)
    } else if let Some(rest) = http.strip_prefix("http://") {
        format!("ws://{}", rest)
    } else {
        http.to_string()
    }
}

fn resolve_env_reference(value: &str) -> String {
    if let Some(name) = value.strip_prefix("env:") {
        env::var(name).unwrap_or_else(|_| value.to_string())
    } else {
        value.to_string()
    }
}

fn default_rpc_request_timeout_ms() -> u64 {
    2_500
}

fn default_database_path() -> String {
    ".arb/arb.db".to_string()
}

fn default_database_busy_timeout_ms() -> u64 {
    5_000
}

fn default_jupiter_requests_per_window() -> usize {
    env_usize("JUPITER_REQUESTS_PER_WINDOW").unwrap_or(90)
}

fn default_jupiter_window_ms() -> u64 {
    env_u64("JUPITER_WINDOW_MS").unwrap_or(60_000)
}

fn default_jupiter_estimated_requests_per_candidate() -> usize {
    env_usize("JUPITER_ESTIMATED_REQUESTS_PER_CANDIDATE").unwrap_or(2)
}

fn default_jupiter_rate_limit_cooldown_ms() -> u64 {
    env_u64("JUPITER_RATE_LIMIT_COOLDOWN_MS").unwrap_or(60_000)
}

fn default_ws_subscription_budget() -> usize {
    env_usize("WS_SUBSCRIPTION_BUDGET").unwrap_or(64)
}

fn default_jupiter_probe_enabled() -> bool {
    env_bool("JUPITER_PROBE_ENABLED").unwrap_or(true)
}

fn default_true() -> bool {
    true
}

fn default_rwa_enabled() -> bool {
    env_bool("RWA_ENABLED").unwrap_or(false)
}

fn default_rwa_assets_file() -> String {
    env::var("RWA_ASSETS_FILE").unwrap_or_else(|_| "rwa_assets.toml".to_string())
}

fn default_rwa_mode() -> RwaMode {
    env::var("RWA_MODE")
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "neutral" => Some(RwaMode::Neutral),
            "directional" => Some(RwaMode::Directional),
            "both" => Some(RwaMode::Both),
            _ => None,
        })
        .unwrap_or_default()
}

fn default_rwa_quote_assets() -> Vec<String> {
    env::var("RWA_QUOTE_ASSETS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .filter(|assets| !assets.is_empty())
        .unwrap_or_else(|| vec!["USDC".to_string(), "USDT".to_string()])
}

fn default_rwa_min_net_profit_bps() -> f64 {
    env_f64("RWA_MIN_NET_PROFIT_BPS").unwrap_or(75.0)
}

fn default_rwa_fee_safety_buffer_bps() -> f64 {
    env_f64("RWA_FEE_SAFETY_BUFFER_BPS").unwrap_or(15.0)
}

fn default_rwa_max_trade_usd_per_asset() -> f64 {
    env_f64("RWA_MAX_TRADE_USD_PER_ASSET").unwrap_or(25.0)
}

fn default_rwa_max_oracle_confidence_bps() -> f64 {
    env_f64("RWA_MAX_ORACLE_CONFIDENCE_BPS").unwrap_or(10.0)
}

fn default_rwa_max_oracle_age_ms() -> u64 {
    env_u64("RWA_MAX_ORACLE_AGE_MS").unwrap_or(90_000)
}

fn default_rwa_flash_primary_enabled() -> bool {
    false
}

fn default_rwa_max_assets_per_cycle() -> usize {
    env_usize("RWA_MAX_ASSETS_PER_CYCLE").unwrap_or(8)
}

fn default_rwa_max_live_trades_per_cycle() -> usize {
    env_usize("RWA_MAX_LIVE_TRADES_PER_CYCLE").unwrap_or(1)
}

fn default_discovery_refresh_interval_ms() -> u64 {
    300_000
}

fn default_priority_fee_percentile() -> f64 {
    env_f64("PRIORITY_FEE_PERCENTILE").unwrap_or(75.0)
}

fn default_max_priority_fee_lamports() -> u64 {
    env_u64("MAX_PRIORITY_FEE_LAMPORTS").unwrap_or(250_000)
}

fn default_jito_tip_lamports() -> u64 {
    env_u64("JITO_TIP_LAMPORTS").unwrap_or(0)
}

fn default_max_jito_tip_lamports() -> u64 {
    env_u64("MAX_JITO_TIP_LAMPORTS").unwrap_or(0)
}

fn default_ata_rent_lamports() -> u64 {
    env_u64("ATA_RENT_LAMPORTS").unwrap_or(2_039_280)
}

fn default_base_fee_lamports() -> u64 {
    env_u64("BASE_FEE_LAMPORTS")
        .or_else(|| env_u64("SOLANA_BASE_SIGNATURE_FEE_LAMPORTS"))
        .unwrap_or(5_000)
}

fn default_stale_candidate_ttl_ms() -> u64 {
    env_u64("LIVE_STALE_CANDIDATE_TTL_MS").unwrap_or(250)
}

fn default_max_pool_slot_age() -> u64 {
    env_u64("MAX_POOL_SLOT_AGE").unwrap_or(50)
}

fn default_pool_index_url() -> String {
    env::var("POOL_INDEX_URL")
        .unwrap_or_else(|_| "https://api.dexscreener.com/latest/dex/tokens".to_string())
}

fn default_jupiter_swap_url() -> String {
    env::var("JUPITER_SWAP_URL").unwrap_or_else(|_| {
        let base_url = default_jupiter_api_url();
        if base_url.ends_with("/swap") {
            base_url
        } else if let Some(prefix) = base_url.strip_suffix("/quote") {
            format!("{prefix}/swap")
        } else {
            format!("{}/swap", base_url.trim_end_matches('/'))
        }
    })
}

fn default_jupiter_swap_instructions_url() -> String {
    env::var("JUPITER_SWAP_INSTRUCTIONS_URL").unwrap_or_else(|_| {
        let base_url = default_jupiter_api_url();
        if base_url.ends_with("/swap-instructions") {
            base_url
        } else if let Some(prefix) = base_url.strip_suffix("/quote") {
            format!("{prefix}/swap-instructions")
        } else if let Some(prefix) = base_url.strip_suffix("/swap") {
            format!("{prefix}/swap-instructions")
        } else {
            format!("{}/swap-instructions", base_url.trim_end_matches('/'))
        }
    })
}

fn default_jupiter_api_url() -> String {
    env::var("JUPITER_API_URL").unwrap_or_else(|_| {
        if env::var("JUPITER_API_KEY")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .is_some()
        {
            "https://api.jup.ag/swap/v1".to_string()
        } else {
            "https://lite-api.jup.ag/swap/v1".to_string()
        }
    })
}

fn default_max_index_pairs_per_token() -> usize {
    env::var("POOL_INDEX_MAX_PAIRS_PER_TOKEN")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(24)
}

fn env_bool(name: &str) -> Option<bool> {
    env::var(name)
        .ok()
        .map(|value| value == "true" || value == "1")
}

fn env_f64(name: &str) -> Option<f64> {
    env::var(name).ok().and_then(|value| value.parse().ok())
}

fn env_u64(name: &str) -> Option<u64> {
    env::var(name).ok().and_then(|value| value.parse().ok())
}

fn env_usize(name: &str) -> Option<usize> {
    env::var(name).ok().and_then(|value| value.parse().ok())
}

fn env_present_any(names: &[&str]) -> bool {
    names.iter().any(|name| {
        env::var(name)
            .ok()
            .is_some_and(|value| !value.trim().is_empty())
    })
}

fn default_jupiter_seed_pairs() -> Vec<JupiterSeedPair> {
    [
        ("DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263", 5), // BONK
        ("EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm", 6), // WIF
        ("HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3", 6), // PYTH
    ]
    .into_iter()
    .map(|(base_mint, base_decimals)| JupiterSeedPair {
        base_mint: base_mint.to_string(),
        quote_mint: USDC_MINT.to_string(),
        base_decimals,
        quote_decimals: 6,
    })
    .collect()
}

fn default_aggregator_seed_pairs() -> Vec<JupiterSeedPair> {
    default_jupiter_seed_pairs()
}

fn default_aggregator_providers() -> Vec<AggregatorProviderConfig> {
    let enable_all =
        env_flag("AGGREGATOR_ENABLE_ALL_SOURCES") || env_flag("ALL_TRADE_SOURCES_ENABLED");
    vec![
        AggregatorProviderConfig {
            name: "raydium_trade_api".to_string(),
            venue: crate::types::Venue::Raydium,
            enabled: enable_all || env_flag("RAYDIUM_TRADE_API_ENABLED"),
            quote_url: env::var("RAYDIUM_TRADE_API_QUOTE_URL").unwrap_or_else(|_| {
                "https://transaction-v1.raydium.io/compute/swap-base-in".to_string()
            }),
            format: AggregatorFormat::RaydiumTradeApi,
            timeout_ms: 900,
            min_interval_ms: env_u64("RAYDIUM_TRADE_API_MIN_INTERVAL_MS").unwrap_or(2_000),
            max_in_flight: env_usize("RAYDIUM_TRADE_API_MAX_IN_FLIGHT").unwrap_or(1),
            requires_api_key: false,
            api_key_env: None,
            api_key_header: None,
            api_key_prefix: None,
        },
        AggregatorProviderConfig {
            name: "spiderswap".to_string(),
            venue: crate::types::Venue::SpiderSwap,
            enabled: enable_all
                || env_flag("SPIDERSWAP_ENABLED")
                || env_flag("CARBIUM_ENABLED")
                || env::var("SPIDERSWAP_API_KEY").is_ok()
                || env::var("CARBIUM_API_KEY").is_ok(),
            quote_url: env::var("SPIDERSWAP_QUOTE_URL")
                .or_else(|_| env::var("CARBIUM_QUOTE_URL"))
                .unwrap_or_else(|_| "https://api.carbium.io/api/v2/quote".to_string()),
            format: AggregatorFormat::CarbiumQ1,
            timeout_ms: 1_200,
            min_interval_ms: 350,
            max_in_flight: 1,
            requires_api_key: true,
            api_key_env: Some("SPIDERSWAP_API_KEY".to_string()),
            api_key_header: Some("X-API-KEY".to_string()),
            api_key_prefix: None,
        },
        AggregatorProviderConfig {
            name: "dflow".to_string(),
            venue: crate::types::Venue::Dflow,
            enabled: enable_all
                || env_flag("DFLOW_ENABLED")
                || env_present_any(&["DFLOW_API_KEY", "DFLOW_API_TOKEN", "DFLOW_AUTH_TOKEN"]),
            quote_url: env::var("DFLOW_QUOTE_URL").unwrap_or_else(|_| {
                env::var("DFLOW_API_URL")
                    .map(|url| format!("{}/order", url.trim_end_matches('/')))
                    .unwrap_or_else(|_| "https://quote-api.dflow.net/order".to_string())
            }),
            format: AggregatorFormat::Dflow,
            timeout_ms: 1_200,
            min_interval_ms: 350,
            max_in_flight: 1,
            requires_api_key: true,
            api_key_env: Some("DFLOW_API_KEY".to_string()),
            api_key_header: Some("x-api-key".to_string()),
            api_key_prefix: None,
        },
        AggregatorProviderConfig {
            name: "okx".to_string(),
            venue: crate::types::Venue::Okx,
            enabled: enable_all || env_flag("OKX_DEX_ENABLED") || env::var("OKX_API_KEY").is_ok(),
            quote_url: env::var("OKX_DEX_QUOTE_URL")
                .unwrap_or_else(|_| "https://web3.okx.com/api/v6/dex/aggregator/quote".to_string()),
            format: AggregatorFormat::Okx,
            timeout_ms: 1_500,
            min_interval_ms: 500,
            max_in_flight: 1,
            requires_api_key: true,
            api_key_env: Some("OKX_API_KEY".to_string()),
            api_key_header: Some("OK-ACCESS-KEY".to_string()),
            api_key_prefix: None,
        },
        AggregatorProviderConfig {
            name: "openocean".to_string(),
            venue: crate::types::Venue::OpenOcean,
            enabled: enable_all
                || env_flag("OPENOCEAN_ENABLED")
                || env::var("OPENOCEAN_API_KEY").is_ok(),
            quote_url: env::var("OPENOCEAN_QUOTE_URL").unwrap_or_else(|_| {
                "https://open-api.openocean.finance/v4/solana/quote".to_string()
            }),
            format: AggregatorFormat::OpenOcean,
            timeout_ms: 1_500,
            min_interval_ms: env_u64("OPENOCEAN_MIN_INTERVAL_MS").unwrap_or(2_500),
            max_in_flight: 1,
            requires_api_key: false,
            api_key_env: Some("OPENOCEAN_API_KEY".to_string()),
            api_key_header: Some("X-API-KEY".to_string()),
            api_key_prefix: None,
        },
        AggregatorProviderConfig {
            name: "titan".to_string(),
            venue: crate::types::Venue::Titan,
            enabled: enable_all
                || env_flag("TITAN_ENABLED")
                || env_present_any(&["TITAN_API_TOKEN", "TITAN_API_KEY", "TITAN_BEARER_TOKEN"]),
            quote_url: env::var("TITAN_WS_URL")
                .unwrap_or_else(|_| "wss://api.titan.ag/api/v1/ws".to_string()),
            format: AggregatorFormat::TitanStream,
            timeout_ms: 1_200,
            min_interval_ms: 250,
            max_in_flight: 1,
            requires_api_key: true,
            api_key_env: Some("TITAN_API_TOKEN".to_string()),
            api_key_header: Some("Authorization".to_string()),
            api_key_prefix: Some("Bearer ".to_string()),
        },
        AggregatorProviderConfig {
            name: "vybe".to_string(),
            venue: crate::types::Venue::Vybe,
            enabled: enable_all
                || env_flag("VYBE_ENABLED")
                || env_present_any(&["VYBE_API_KEY", "VYBE_API_TOKEN"]),
            quote_url: env::var("VYBE_QUOTE_URL")
                .unwrap_or_else(|_| "https://api.vybenetwork.com/trading/swap-quote".to_string()),
            format: AggregatorFormat::Vybe,
            timeout_ms: 1_200,
            min_interval_ms: 500,
            max_in_flight: 1,
            requires_api_key: true,
            api_key_env: Some("VYBE_API_KEY".to_string()),
            api_key_header: Some("X-API-KEY".to_string()),
            api_key_prefix: None,
        },
        AggregatorProviderConfig {
            name: "venum".to_string(),
            venue: crate::types::Venue::Venum,
            enabled: enable_all
                || env_flag("VENUM_ENABLED")
                || env_present_any(&["VENUM_API_KEY", "VENUM_API_TOKEN"]),
            quote_url: env::var("VENUM_QUOTE_URL")
                .unwrap_or_else(|_| "https://api.venum.dev/v1/quote".to_string()),
            format: AggregatorFormat::Venum,
            timeout_ms: 1_200,
            min_interval_ms: 6_000,
            max_in_flight: 1,
            requires_api_key: true,
            api_key_env: Some("VENUM_API_KEY".to_string()),
            api_key_header: Some("X-API-Key".to_string()),
            api_key_prefix: None,
        },
    ]
}

fn default_aggregator_dynamic_discovery_enabled() -> bool {
    env_bool("TOKEN_DISCOVERY_ENABLED").unwrap_or(true)
}

fn default_aggregator_dynamic_refresh_interval_ms() -> u64 {
    env_u64("TOKEN_DISCOVERY_REFRESH_INTERVAL_MS").unwrap_or(300_000)
}

fn default_aggregator_dynamic_max_tokens() -> usize {
    env_usize("TOKEN_DISCOVERY_MAX_TOKENS").unwrap_or(18)
}

fn default_aggregator_dynamic_max_quote_mints() -> usize {
    env_usize("TOKEN_DISCOVERY_MAX_QUOTE_MINTS").unwrap_or(2)
}

fn default_aggregator_dynamic_min_liquidity_usd() -> f64 {
    env_f64("TOKEN_DISCOVERY_MIN_LIQUIDITY_USD").unwrap_or(25_000.0)
}

fn default_aggregator_dynamic_min_organic_score() -> f64 {
    env_f64("TOKEN_DISCOVERY_MIN_ORGANIC_SCORE").unwrap_or(35.0)
}

fn default_aggregator_dynamic_max_mcap_usd() -> f64 {
    env_f64("TOKEN_DISCOVERY_MAX_MCAP_USD").unwrap_or(750_000_000.0)
}

fn default_aggregator_dynamic_allow_unverified() -> bool {
    env_bool("TOKEN_DISCOVERY_ALLOW_UNVERIFIED").unwrap_or(true)
}

fn default_aggregator_dynamic_min_supported_venues() -> usize {
    env_usize("TOKEN_DISCOVERY_MIN_SUPPORTED_VENUES").unwrap_or(2)
}

fn env_flag(name: &str) -> bool {
    env::var(name)
        .map(|value| value == "true" || value == "1")
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_example_parses() {
        let config: BotConfig =
            toml::from_str(include_str!("../../../config.example.toml")).unwrap();
        assert!(config.discovery.refresh_interval_ms >= 60_000);
        assert!(config.aggregators.cache_ttl_ms > 0);
        assert!(config
            .aggregators
            .providers
            .iter()
            .any(|provider| provider.name == "spiderswap"));
    }

    #[test]
    fn active_tokens_config_disables_rwa_and_submit() {
        let config: BotConfig =
            toml::from_str(include_str!("../../../config.tokens.toml")).unwrap();

        assert!(!config.rwa.enabled);
        assert!(!config.rwa.execution_enabled);
        assert!(config.discovery.enabled);
        assert!(config.discovery.index_enabled);
        assert!(!config.discovery.broad_rpc_enabled);
        assert!(!config.simulation.enabled);
        assert!(config.execution.enabled);
        assert!(!config.execution.submit_enabled);
        assert!(config.execution.native_price_only_execution_enabled);
        assert!(config.execution.jupiter_probe_enabled);
        assert!(!config.execution.native_focus_routes.is_empty());
        assert!(config.aggregators.dynamic_discovery_enabled);
        assert!(!config.jupiter.seed_pairs.is_empty());
        assert!(!config.rwa_only_mode());
    }

    #[test]
    fn missing_config_file_falls_back_to_rwa_disabled() {
        let config = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(BotConfig::load("does-not-exist/config.toml"))
            .unwrap();
        if env_bool("RWA_ENABLED").is_some() {
            return;
        }
        assert!(!config.rwa.enabled);
        assert!(!config.rwa_only_mode());
    }

    #[test]
    fn env_defaults_include_rate_limited_aggregator_providers() {
        let config = BotConfig::from_env_defaults();
        assert!(config.aggregators.enabled);
        assert!(config.aggregators.max_pairs_per_cycle > 0);
        assert!(config
            .aggregators
            .providers
            .iter()
            .all(|provider| provider.max_in_flight >= 1));
    }

    #[test]
    fn config_file_reads_dynamic_discovery_controls() {
        let config: AggregatorsConfig = toml::from_str(
            r#"
enabled = true
request_timeout_ms = 900
cache_ttl_ms = 1000
max_pairs_per_cycle = 7
dynamic_discovery_enabled = true
dynamic_refresh_interval_ms = 123456
dynamic_max_tokens = 11
dynamic_max_quote_mints = 3
dynamic_min_liquidity_usd = 50000.0
dynamic_min_organic_score = 42.0
dynamic_max_mcap_usd = 250000000.0
dynamic_allow_unverified = false
dynamic_min_supported_venues = 2
seed_pairs = []
providers = []
"#,
        )
        .expect("aggregator config parses");

        assert!(config.dynamic_discovery_enabled);
        assert_eq!(config.dynamic_refresh_interval_ms, 123_456);
        assert_eq!(config.dynamic_max_tokens, 11);
        assert_eq!(config.dynamic_max_quote_mints, 3);
        assert_eq!(config.dynamic_min_liquidity_usd, 50_000.0);
        assert_eq!(config.dynamic_min_organic_score, 42.0);
        assert_eq!(config.dynamic_max_mcap_usd, 250_000_000.0);
        assert!(!config.dynamic_allow_unverified);
        assert_eq!(config.dynamic_min_supported_venues, 2);
    }

    #[test]
    fn file_config_derives_primary_ws_when_blank() {
        let mut config: BotConfig =
            toml::from_str(include_str!("../../../config.minimal.toml")).unwrap();
        config.rpc.primary_http = "https://example.rpc".to_string();
        config.rpc.primary_ws.clear();

        config.resolve_env_references();

        assert_eq!(config.rpc.primary_ws, "wss://example.rpc");
    }

    #[test]
    fn execution_budget_spreads_jupiter_requests_across_scan_cycles() {
        let mut execution = ExecutionConfig::default();
        execution.max_live_candidates_per_cycle = 0;
        execution.jupiter_requests_per_window = 90;
        execution.jupiter_window_ms = 60_000;
        execution.jupiter_estimated_requests_per_candidate = 2;

        assert_eq!(execution.jupiter_requests_budget_per_cycle(5_000), 7);
        assert_eq!(execution.jupiter_candidate_budget_per_cycle(5_000), 3);
        assert_eq!(execution.live_candidate_limit(5_000, 144), 3);

        execution.max_live_candidates_per_cycle = 2;
        assert_eq!(execution.live_candidate_limit(5_000, 144), 2);
    }

    #[test]
    fn execution_quality_controls_have_safe_defaults() {
        let execution = ExecutionConfig::default();

        assert_eq!(execution.stale_candidate_ttl_ms, 250);
        assert!(!execution.native_prefunded_hot_accounts);
        assert_eq!(execution.min_absolute_profit_usd, 0.0);
        assert!(execution.address_lookup_tables.is_empty());
    }
}
