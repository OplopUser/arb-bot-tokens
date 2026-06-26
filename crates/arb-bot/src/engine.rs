use crate::{
    aggregators::{missing_provider_auth_envs, AggregatorProviderSnapshot, AggregatorQuoteManager},
    api_swap::{route_provider_name as api_route_provider_name, ApiSwapBuilder},
    config::{BotConfig, ExecutionConfig},
    db::{
        ArbDatabase, DatabaseSummary, ExecutionAttemptRecord, LiveSettlementRecord,
        QuoteObservationRecord, RouteAccountFetchTarget, SimulationResultRecord,
    },
    env_runtime::{env_bool, env_value},
    fees::DynamicFeeEngine,
    live_swap::{
        ApiRouteSwapRequest, AtomicArbReport, AtomicArbStatus, ExternalPrimaryTransactionRequest,
        JupiterLiveSwapExecutor, JupiterSwapInstructionChainLeg,
        JupiterSwapInstructionChainRequest, LiveSwapReport, LiveSwapRequest,
    },
    native_swap::NativeSwapBuilder,
    pool_index::{IndexedPoolCandidate, PoolIndexClient},
    pool_subscription::{CachedAccount, PoolSubscriptionManager},
    rpc::RpcClient,
    rwa::{
        scan_rwa, RwaExecutionLeg, RwaExecutionRecord, RwaOpportunity, RwaOpportunityKind,
        RwaPrimaryRouteReadiness, RwaSnapshot, RwaWalletBalanceSnapshot,
    },
    sol_price::{SolUsdEstimator, SolUsdSnapshot},
    types::{
        atoms_to_ui, is_blocklisted_remora_mint, is_pump_like_mint, ui_to_atoms, FeeBreakdown,
        FocusedRouteReadinessSnapshot, FocusedRouteRequoteLatencySnapshot, Opportunity,
        OptimizerDecisionTrace, PoolKind, PoolState, Quote, RawAccount, RouteAccountRequirement,
        RouteReadinessState, SwapSide, TokenPair, Venue, METEORA_DAMM_V2_PROGRAM,
        METEORA_DLMM_PROGRAM, METEORA_POOLS_PROGRAM, RAYDIUM_AMM_V4_PROGRAM, RAYDIUM_CLMM_PROGRAM,
        RAYDIUM_CPMM_PROGRAM, USDC_MINT, USDT_MINT, WSOL_MINT,
    },
    venues::{enabled_adapters, VenueAdapter},
};
use anyhow::{anyhow, Context, Result};
use futures_util::{stream::FuturesUnordered, StreamExt};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use solana_sdk::signature::{read_keypair_file, Signer};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
#[cfg(not(test))]
use std::{
    fs,
    sync::{Mutex as StdMutex, OnceLock},
};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

const FOCUSED_ROUTE_REQUOTE_DEBOUNCE_MS: u64 = 25;
const FOCUSED_ROUTE_ACCOUNT_MAX_AGE_FLOOR_MS: u64 = 5_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BotStatus {
    Running,
    Paused,
    EmergencyStopped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateSnapshot {
    pub status: BotStatus,
    pub live: bool,
    pub cycle: u64,
    pub scan: ScanSnapshot,
    pub execution: ExecutionSnapshot,
    pub simulation: SimulationSnapshot,
    pub rwa: RwaSnapshot,
    pub pools: Vec<PoolState>,
    pub quotes: Vec<Quote>,
    pub opportunities: Vec<Opportunity>,
    pub active_tokens: Vec<String>,
    pub last_error: Option<String>,
    pub last_network_fee: FeeBreakdown,
    pub ws_connected: bool,
    pub ws_last_update_slot: u64,
    pub ws_subscription_count: usize,
    pub ws_desired_subscription_count: usize,
    pub ws_pending_subscription_count: usize,
    pub ws_confirmed_subscription_count: usize,
    pub ws_rejected_subscription_count: usize,
    pub ws_subscription_budget: usize,
    pub ws_subscription_budget_remaining: usize,
    pub ws_cache_size: usize,
    pub focused_route_accounts: Vec<FocusedRouteReadinessSnapshot>,
    pub focused_route_requotes: Vec<FocusedRouteRequoteLatencySnapshot>,
    pub runtime_evidence: RuntimeEvidenceSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeEvidenceSnapshot {
    pub database_enabled: bool,
    pub database_path: Option<String>,
    pub database_error: Option<String>,
    pub quote_observations_delta: u64,
    pub simulation_results_delta: u64,
    pub execution_attempts_delta: u64,
    pub memory_simulation_attempted_count: u64,
    pub memory_execution_attempted_count: u64,
    pub memory_execution_blocked_count: u64,
}

impl Default for RuntimeEvidenceSnapshot {
    fn default() -> Self {
        Self {
            database_enabled: false,
            database_path: None,
            database_error: None,
            quote_observations_delta: 0,
            simulation_results_delta: 0,
            execution_attempts_delta: 0,
            memory_simulation_attempted_count: 0,
            memory_execution_attempted_count: 0,
            memory_execution_blocked_count: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationSnapshot {
    pub enabled: bool,
    pub attempted_count: u64,
    pub profitable_count: u64,
    pub unprofitable_count: u64,
    pub total_net_profit_usd: f64,
    pub last_simulated_unix_ms: Option<u128>,
    pub next_simulation_in_ms: Option<u64>,
    pub reason: String,
    pub trades: Vec<SimulatedTrade>,
}

impl Default for SimulationSnapshot {
    fn default() -> Self {
        Self {
            enabled: false,
            attempted_count: 0,
            profitable_count: 0,
            unprofitable_count: 0,
            total_net_profit_usd: 0.0,
            last_simulated_unix_ms: None,
            next_simulation_in_ms: None,
            reason: "simulator has not run yet".to_string(),
            trades: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulatedTrade {
    pub id: u64,
    pub cycle: u64,
    pub simulated_unix_ms: u128,
    pub pair: TokenPair,
    pub buy_venue: crate::types::Venue,
    pub sell_venue: crate::types::Venue,
    pub buy_pool: String,
    pub sell_pool: String,
    pub input_quote_atoms: u128,
    pub base_atoms: u128,
    pub output_quote_atoms: u128,
    pub expected_net_profit_usd: f64,
    pub simulated_net_profit_usd: f64,
    pub simulated_profit_bps: f64,
    pub profitable: bool,
    pub status: SimulatedTradeStatus,
    pub notes: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SimulatedTradeStatus {
    Profitable,
    Unprofitable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionSnapshot {
    pub live_requested: bool,
    pub submission_enabled: bool,
    pub paths: Vec<ExecutionPathSnapshot>,
    pub candidate_source: String,
    pub candidate_pool_size: usize,
    pub candidate_cooldown_count: usize,
    pub probe_trade_sizes_usd: Vec<f64>,
    pub api_requests_per_window: usize,
    pub api_window_ms: u64,
    pub api_requests_budget_per_cycle: usize,
    pub api_estimated_requests_per_candidate: usize,
    pub api_candidate_budget_per_cycle: usize,
    pub api_candidate_limit_applied: usize,
    pub candidate_start_offset: usize,
    pub api_rate_limit_cooldown_ms: u64,
    pub api_rate_limited: bool,
    pub wallet_quote_balances: Vec<RwaWalletBalanceSnapshot>,
    pub wallet_quote_balance_errors: Vec<String>,
    pub wallet_funding_blocked_count: usize,
    pub best_funding_blocked_required_usd: Option<f64>,
    pub best_funding_blocked_available_usd: Option<f64>,
    pub attempted_count: u64,
    pub blocked_count: u64,
    pub confirmed_count: u64,
    pub failed_count: u64,
    pub last_signature: Option<String>,
    pub last_error: Option<String>,
    pub reason: String,
    pub selected_candidates: Vec<ProbeCandidateSnapshot>,
    pub best_recent_candidates: Vec<ProbeCandidateSnapshot>,
    #[serde(default)]
    pub focused_route_families: Vec<FocusedRouteFamilySnapshot>,
    #[serde(default)]
    pub focused_route_candidate_rejections: Vec<FocusedRouteCandidateRejectionSnapshot>,
    #[serde(default)]
    pub fresh_quote_truth: FreshQuoteTruthSnapshot,
    #[serde(default)]
    pub live_risk: LiveRiskSnapshot,
    pub trades: Vec<LiveExecutionRecord>,
    pub execution_profile: String,
    pub native_focus_route_count: usize,
    pub native_price_only_execution_enabled: bool,
    pub native_simulation_net_profit_atoms: Option<String>,
    pub route_family_count: usize,
    pub route_edge_count: usize,
}

impl Default for ExecutionSnapshot {
    fn default() -> Self {
        Self {
            live_requested: false,
            submission_enabled: false,
            paths: Vec::new(),
            candidate_source: "none".to_string(),
            candidate_pool_size: 0,
            candidate_cooldown_count: 0,
            probe_trade_sizes_usd: Vec::new(),
            api_requests_per_window: 0,
            api_window_ms: 0,
            api_requests_budget_per_cycle: 0,
            api_estimated_requests_per_candidate: 0,
            api_candidate_budget_per_cycle: 0,
            api_candidate_limit_applied: 0,
            candidate_start_offset: 0,
            api_rate_limit_cooldown_ms: 0,
            api_rate_limited: false,
            wallet_quote_balances: Vec::new(),
            wallet_quote_balance_errors: Vec::new(),
            wallet_funding_blocked_count: 0,
            best_funding_blocked_required_usd: None,
            best_funding_blocked_available_usd: None,
            attempted_count: 0,
            blocked_count: 0,
            confirmed_count: 0,
            failed_count: 0,
            last_signature: None,
            last_error: None,
            reason: "scanner has not run yet".to_string(),
            selected_candidates: Vec::new(),
            best_recent_candidates: Vec::new(),
            focused_route_families: Vec::new(),
            focused_route_candidate_rejections: Vec::new(),
            fresh_quote_truth: FreshQuoteTruthSnapshot::default(),
            live_risk: LiveRiskSnapshot::default(),
            trades: Vec::new(),
            execution_profile: String::new(),
            native_focus_route_count: 0,
            native_price_only_execution_enabled: false,
            native_simulation_net_profit_atoms: None,
            route_family_count: 0,
            route_edge_count: 0,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FreshQuoteTruthSnapshot {
    pub fresh_positive_complete: usize,
    pub fresh_positive_blocked: usize,
    pub fresh_negative_complete: usize,
    pub fresh_quote_unavailable: usize,
    pub executable_candidate_count: usize,
    pub rejected_candidate_count: usize,
    pub execution_gate_open: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LiveRiskSnapshot {
    pub gate_open: bool,
    pub blocked_reason: Option<String>,
    pub max_fee_loss_usd_per_session: f64,
    pub max_failed_transactions_per_session: u64,
    pub failed_fee_loss_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FocusedRouteFamilySnapshot {
    pub id: i64,
    pub pair: TokenPair,
    pub buy_venue: String,
    pub sell_venue: String,
    pub enabled: bool,
    pub priority: i64,
    pub total_edges: u64,
    pub readiness_counts: Vec<FocusedRouteReadinessCountSnapshot>,
    pub latest_blockers: Vec<FocusedRouteBlockerSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FocusedRouteCandidateRejectionSnapshot {
    pub route_edge_id: Option<i64>,
    pub pair: TokenPair,
    pub buy_venue: String,
    pub sell_venue: String,
    pub buy_pool: String,
    pub sell_pool: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fresh_quote_reconciliation: Option<FreshQuoteReconciliationSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshQuoteReconciliationSnapshot {
    pub route_edge_id: Option<i64>,
    pub pair: TokenPair,
    pub buy_venue: String,
    pub sell_venue: String,
    pub buy_pool: String,
    pub sell_pool: String,
    pub optimizer_input_quote_atoms: String,
    pub optimizer_buy_output_base_atoms: String,
    pub optimizer_sell_output_quote_atoms: String,
    pub optimizer_net_profit_quote_atoms: String,
    pub optimizer_net_profit_bps: f64,
    pub fresh_buy_output_base_atoms: Option<String>,
    pub fresh_sell_output_quote_atoms: Option<String>,
    pub fresh_net_profit_quote_atoms: Option<String>,
    pub fresh_net_profit_bps: Option<f64>,
    pub execution_cost_quote_atoms: Option<String>,
    pub fresh_requote_complete: Option<bool>,
    pub fresh_blocked_reason: Option<String>,
    pub fresh_quote_status: String,
    pub final_blocker: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_snapshot: Option<FocusedRouteAccountAgeSummarySnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FocusedRouteAccountAgeSummarySnapshot {
    pub ready: bool,
    pub total_accounts: usize,
    pub required_accounts: usize,
    pub ready_accounts: usize,
    pub missing_required_accounts: usize,
    pub stale_required_accounts: usize,
    pub max_wall_clock_age_ms: Option<u64>,
    pub max_slot_age: Option<u64>,
    pub blocker: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FocusedRouteReadinessCountSnapshot {
    pub readiness_state: String,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FocusedRouteBlockerSnapshot {
    pub readiness_state: String,
    pub blocker: String,
    pub count: u64,
    pub latest_updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPathSnapshot {
    pub name: String,
    pub kind: String,
    pub quote_source: String,
    pub swap_source: String,
    pub enabled: bool,
    pub active: bool,
    pub readiness: ExecutionPathReadiness,
    pub requires_api_key: bool,
    pub has_api_key: bool,
    pub decoded_pools: usize,
    pub reserve_backed_pools: usize,
    pub quote_count: usize,
    pub status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionPathReadiness {
    Active,
    AuthMissing,
    CoolingDown,
    Disabled,
    Gated,
    Pacing,
    WaitingForPools,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveExecutionRecord {
    pub id: u64,
    pub cycle: u64,
    pub submitted_unix_ms: u128,
    pub pair: TokenPair,
    pub buy_venue: crate::types::Venue,
    pub sell_venue: crate::types::Venue,
    pub input_quote_atoms: u128,
    pub direct_signal_net_profit_usd: f64,
    pub requote_net_profit_usd: Option<f64>,
    pub requote_net_profit_bps: Option<f64>,
    pub status: AtomicArbStatus,
    pub submitted: bool,
    pub simulation_ok: bool,
    pub signature: Option<String>,
    pub solscan_url: Option<String>,
    pub reason: Option<String>,
    pub jupiter_reference_net_profit_bps: Option<f64>,
    pub jupiter_reference_error: Option<String>,
    pub native_execution_cost_quote_atoms: Option<String>,
    pub native_setup_missing_atas: Option<usize>,
    pub native_simulation_net_profit_atoms: Option<String>,
    pub native_simulation_pre_quote_atoms: Option<String>,
    pub native_simulation_post_quote_atoms: Option<String>,
    pub native_simulation_quote_delta_atoms: Option<String>,
    pub signed_transaction_bytes: Option<usize>,
    #[serde(default)]
    pub latency_trace: Vec<crate::live_swap::ExecutionLatencyMark>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeCandidateSnapshot {
    pub key: String,
    pub pair: TokenPair,
    pub buy_venue: crate::types::Venue,
    pub sell_venue: crate::types::Venue,
    pub input_quote_atoms: u128,
    pub input_usd: f64,
    pub attempts: u64,
    pub blocks: u64,
    pub no_route_blocks: u64,
    pub last_requote_bps: Option<f64>,
    pub best_requote_bps: Option<f64>,
    pub last_reason: Option<String>,
    pub last_checked_unix_ms: Option<u128>,
    pub cooldown_remaining_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optimizer_decision_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optimizer_selected_input_quote_atoms: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optimizer_net_profit_quote_atoms: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optimizer_net_profit_usd: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optimizer_net_profit_bps: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optimizer_route_edge_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optimizer_buy_pool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optimizer_sell_pool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optimizer_buy_venue: Option<crate::types::Venue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optimizer_sell_venue: Option<crate::types::Venue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanSnapshot {
    pub trade_size_usd: f64,
    pub min_net_profit_bps: f64,
    pub sol_usd: SolUsdSnapshot,
    pub direct_venues: Vec<String>,
    pub target_pairs: Vec<ScanTargetPair>,
    pub quote_providers: Vec<AggregatorProviderSnapshot>,
    pub discovery: DiscoverySnapshot,
    pub direct_pool_cache_count: usize,
    pub promoted_pool_count: usize,
    pub quote_count: usize,
    pub quote_venues: Vec<String>,
}

impl Default for ScanSnapshot {
    fn default() -> Self {
        Self {
            trade_size_usd: 0.0,
            min_net_profit_bps: 0.0,
            sol_usd: SolUsdSnapshot {
                price_usd: 100.0,
                source: "default".to_string(),
                refresh_interval_ms: 0,
                updated_unix_ms: None,
                stale: true,
                last_error: None,
            },
            direct_venues: Vec::new(),
            target_pairs: Vec::new(),
            quote_providers: Vec::new(),
            discovery: DiscoverySnapshot::default(),
            direct_pool_cache_count: 0,
            promoted_pool_count: 0,
            quote_count: 0,
            quote_venues: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanTargetPair {
    pub key: String,
    pub base_mint: String,
    pub quote_mint: String,
    pub base_decimals: u8,
    pub quote_decimals: u8,
}

impl From<&TokenPair> for ScanTargetPair {
    fn from(pair: &TokenPair) -> Self {
        Self {
            key: pair.key(),
            base_mint: pair.base_mint.clone(),
            quote_mint: pair.quote_mint.clone(),
            base_decimals: pair.base_decimals,
            quote_decimals: pair.quote_decimals,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoverySnapshot {
    pub enabled: bool,
    pub refresh_interval_ms: u64,
    pub attempted_this_cycle: bool,
    pub last_attempt_cycle: Option<u64>,
    pub last_attempt_unix_ms: Option<u128>,
    pub next_attempt_in_ms: Option<u64>,
    pub jobs: usize,
    pub indexed_candidates: usize,
    pub targeted_accounts: usize,
    pub broad_rpc_enabled: bool,
    pub decoded_pools: usize,
    pub errors: Vec<DiscoveryIssue>,
}

impl Default for DiscoverySnapshot {
    fn default() -> Self {
        Self {
            enabled: false,
            refresh_interval_ms: 0,
            attempted_this_cycle: false,
            last_attempt_cycle: None,
            last_attempt_unix_ms: None,
            next_attempt_in_ms: None,
            jobs: 0,
            indexed_candidates: 0,
            targeted_accounts: 0,
            broad_rpc_enabled: false,
            decoded_pools: 0,
            errors: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryIssue {
    pub kind: DiscoveryIssueKind,
    pub venue: crate::types::Venue,
    pub program_id: String,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryIssueKind {
    Decode,
    Index,
    MissingAccount,
    Rpc,
}

struct DiscoveryRun {
    pools: Vec<PoolState>,
    jobs: usize,
    indexed_candidates: usize,
    targeted_accounts: usize,
    errors: Vec<DiscoveryIssue>,
}

#[derive(Debug, Clone)]
struct CandidateProbeInfo {
    key: String,
    pair: TokenPair,
    buy_venue: Venue,
    sell_venue: Venue,
    input_quote_atoms: u128,
    input_usd: f64,
    optimizer_decision_id: Option<String>,
    optimizer_selected_input_quote_atoms: Option<String>,
    optimizer_net_profit_quote_atoms: Option<String>,
    optimizer_net_profit_usd: Option<f64>,
    optimizer_net_profit_bps: Option<f64>,
    optimizer_route_edge_id: Option<i64>,
    optimizer_buy_pool: Option<String>,
    optimizer_sell_pool: Option<String>,
    optimizer_buy_venue: Option<Venue>,
    optimizer_sell_venue: Option<Venue>,
}

#[derive(Debug, Clone, Default)]
struct CandidateProbeStats {
    info: Option<CandidateProbeInfo>,
    attempts: u64,
    blocks: u64,
    no_route_blocks: u64,
    last_requote_bps: Option<f64>,
    best_requote_bps: Option<f64>,
    last_reason: Option<String>,
    last_checked_unix_ms: Option<u128>,
    cooldown_until: Option<Instant>,
}

struct CandidateSelection {
    indices: Vec<usize>,
    cooldown_count: usize,
    selected_candidates: Vec<ProbeCandidateSnapshot>,
}

#[derive(Debug, Clone)]
struct HotSubscriptionRoute {
    route_edge_id: Option<i64>,
    pair: TokenPair,
    buy_venue: Venue,
    sell_venue: Venue,
    readiness_state: Option<RouteReadinessState>,
    pubkeys: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct HotSubscriptionSelection {
    pubkeys: Vec<String>,
    reasons: Vec<String>,
    desired_count: usize,
    budget: usize,
    budget_remaining: usize,
}

struct RwaSettlementContext {
    wallet_pubkey: String,
    quote_mint: String,
    pre_quote_atoms: u128,
    pre_sol_lamports: u64,
}

impl StateSnapshot {
    pub fn new(live: bool) -> Self {
        Self {
            status: BotStatus::Paused,
            live,
            cycle: 0,
            scan: ScanSnapshot::default(),
            execution: ExecutionSnapshot::default(),
            simulation: SimulationSnapshot::default(),
            rwa: RwaSnapshot::default(),
            pools: Vec::new(),
            quotes: Vec::new(),
            opportunities: Vec::new(),
            active_tokens: Vec::new(),
            last_error: None,
            last_network_fee: FeeBreakdown::default(),
            ws_connected: false,
            ws_last_update_slot: 0,
            ws_subscription_count: 0,
            ws_desired_subscription_count: 0,
            ws_pending_subscription_count: 0,
            ws_confirmed_subscription_count: 0,
            ws_rejected_subscription_count: 0,
            ws_subscription_budget: 0,
            ws_subscription_budget_remaining: 0,
            ws_cache_size: 0,
            focused_route_accounts: Vec::new(),
            focused_route_requotes: Vec::new(),
            runtime_evidence: RuntimeEvidenceSnapshot::default(),
        }
    }
}

#[derive(Clone)]
pub struct BotEngine {
    config: Arc<BotConfig>,
    rpc: RpcClient,
    fee_engine: DynamicFeeEngine,
    aggregators: AggregatorQuoteManager,
    pool_index: PoolIndexClient,
    sol_usd_estimator: SolUsdEstimator,
    state: Arc<RwLock<StateSnapshot>>,
    discovered_cache: Arc<RwLock<Vec<PoolState>>>,
    last_discovery_attempt: Arc<tokio::sync::Mutex<Option<Instant>>>,
    last_simulation_attempt: Arc<tokio::sync::Mutex<Option<Instant>>>,
    last_execution_attempt: Arc<tokio::sync::Mutex<Option<Instant>>>,
    last_jupiter_rate_limit: Arc<tokio::sync::Mutex<Option<Instant>>>,
    live_candidate_cursor: Arc<AtomicU64>,
    candidate_probe_stats: Arc<RwLock<HashMap<String, CandidateProbeStats>>>,
    jupiter_no_route_leg_cooldowns: Arc<RwLock<HashMap<String, Instant>>>,
    simulation_sequence: Arc<AtomicU64>,
    execution_sequence: Arc<AtomicU64>,
    pool_subscription: Arc<PoolSubscriptionManager>,
    database: Option<Arc<std::sync::Mutex<ArbDatabase>>>,
    database_baseline: Option<DatabaseSummary>,
    database_error: Option<String>,
}

impl BotEngine {
    pub fn new(config: BotConfig) -> Self {
        let rpc = RpcClient::new(&config.rpc);
        let state = Arc::new(RwLock::new(StateSnapshot::new(config.trading.live)));
        let fee_engine = DynamicFeeEngine::new(config.fees.clone(), config.trading.clone());
        let aggregators =
            AggregatorQuoteManager::new(config.aggregators.clone(), config.jupiter.clone());
        let pool_index = PoolIndexClient::new(config.discovery.clone());
        let sol_usd_estimator = SolUsdEstimator::new(config.trading.sol_usd_estimate);
        let pool_subscription = Arc::new(PoolSubscriptionManager::new_with_budget(
            config.rpc.primary_ws.clone(),
            config.execution.ws_subscription_budget,
        ));
        pool_subscription.start();
        let (database, database_baseline, database_error) = open_runtime_database(&config);
        Self {
            config: Arc::new(config),
            rpc,
            fee_engine,
            aggregators,
            pool_index,
            sol_usd_estimator,
            state,
            discovered_cache: Arc::new(RwLock::new(Vec::new())),
            last_discovery_attempt: Arc::new(tokio::sync::Mutex::new(None)),
            last_simulation_attempt: Arc::new(tokio::sync::Mutex::new(None)),
            last_execution_attempt: Arc::new(tokio::sync::Mutex::new(None)),
            last_jupiter_rate_limit: Arc::new(tokio::sync::Mutex::new(None)),
            live_candidate_cursor: Arc::new(AtomicU64::new(0)),
            candidate_probe_stats: Arc::new(RwLock::new(HashMap::new())),
            jupiter_no_route_leg_cooldowns: Arc::new(RwLock::new(load_route_cooldown_state())),
            simulation_sequence: Arc::new(AtomicU64::new(1)),
            execution_sequence: Arc::new(AtomicU64::new(1)),
            pool_subscription,
            database,
            database_baseline,
            database_error,
        }
    }

    pub fn state(&self) -> Arc<RwLock<StateSnapshot>> {
        self.state.clone()
    }

    #[cfg(test)]
    fn with_database_for_test(mut self, db: ArbDatabase) -> Self {
        self.database_baseline = db.summary().ok();
        self.database = Some(Arc::new(std::sync::Mutex::new(db)));
        self.database_error = None;
        self
    }

    fn config_with_sol_usd(&self, sol_usd_estimate: f64) -> BotConfig {
        let mut config = (*self.config).clone();
        if sol_usd_estimate.is_finite() && sol_usd_estimate > 0.0 {
            config.trading.sol_usd_estimate = sol_usd_estimate;
        }
        config
    }

    async fn update_hot_subscriptions(&self, candidates: &[Opportunity], promoted: &[PoolState]) {
        let routes = self.hot_subscription_routes(candidates);
        let fallback = promoted
            .iter()
            .map(|pool| pool.address.clone())
            .collect::<Vec<_>>();
        let budget = self.config.execution.ws_subscription_budget;
        let selection = select_hot_subscription_pubkeys(
            &routes,
            &fallback,
            &self.config.execution.native_focus_routes,
            budget,
        );
        self.pool_subscription.set_subscription_budget(budget).await;
        self.pool_subscription
            .set_subscriptions(&selection.pubkeys)
            .await;
        let selected_pubkeys = selection.pubkeys.iter().cloned().collect::<HashSet<_>>();
        for route in &routes {
            let Some(route_edge_id) = route.route_edge_id else {
                continue;
            };
            let requirements = route
                .pubkeys
                .iter()
                .enumerate()
                .filter(|(_, pubkey)| selected_pubkeys.contains(*pubkey))
                .map(|(_, pubkey)| pubkey.clone())
                .collect::<Vec<_>>();
            if !requirements.is_empty() {
                self.pool_subscription
                    .register_focused_route_accounts(
                        route_edge_id,
                        route_account_requirements_from_pubkeys(&requirements),
                        focused_route_account_max_age_ms(
                            self.config.execution.stale_candidate_ttl_ms,
                        ),
                        FOCUSED_ROUTE_REQUOTE_DEBOUNCE_MS,
                    )
                    .await;
            }
        }
        self.warm_hot_subscription_cache(&selection.pubkeys).await;
        if !selection.pubkeys.is_empty() {
            debug!(
                desired = selection.desired_count,
                budget = selection.budget,
                budget_remaining = selection.budget_remaining,
                reasons = ?selection.reasons,
                "updated hot websocket subscription set"
            );
        }
    }

    async fn warm_hot_subscription_cache(&self, pubkeys: &[String]) {
        let mut refresh = Vec::new();
        let max_age_ms =
            focused_route_prequote_refresh_max_age_ms(self.config.execution.stale_candidate_ttl_ms);
        let now = Instant::now();
        for pubkey in pubkeys {
            let cached = self.pool_subscription.get_cached(pubkey).await;
            if hot_cache_entry_needs_refresh(cached.as_ref(), max_age_ms, now) {
                refresh.push(pubkey.clone());
            }
        }
        if refresh.is_empty() {
            return;
        }

        for chunk in refresh.chunks(100) {
            let chunk = chunk.to_vec();
            match self.rpc.get_multiple_accounts(&chunk).await {
                Ok(accounts) => {
                    let warmed = accounts.into_iter().flatten().collect::<Vec<_>>();
                    if !warmed.is_empty() {
                        let warmed_count = warmed.len();
                        self.pool_subscription.warm_raw_accounts(warmed).await;
                        debug!(
                            requested = chunk.len(),
                            warmed = warmed_count,
                            "warmed hot websocket account cache from rpc snapshot"
                        );
                    }
                }
                Err(error) => {
                    debug!(
                        ?error,
                        requested = chunk.len(),
                        "failed to refresh hot websocket account cache"
                    );
                }
            }
        }
    }

    async fn register_focused_route_manifest_accounts(&self, route_edge_id: i64) -> Vec<String> {
        let route_pubkeys = self.focused_route_manifest_pubkeys(route_edge_id);
        if route_pubkeys.is_empty() {
            return route_pubkeys;
        }
        self.pool_subscription
            .register_focused_route_accounts(
                route_edge_id,
                route_account_requirements_from_pubkeys(&route_pubkeys),
                focused_route_account_max_age_ms(self.config.execution.stale_candidate_ttl_ms),
                FOCUSED_ROUTE_REQUOTE_DEBOUNCE_MS,
            )
            .await;
        route_pubkeys
    }

    async fn warm_selected_focused_route_cache(&self, opportunity: &Opportunity) {
        let Some(route_edge_id) = opportunity
            .optimizer_decision
            .as_ref()
            .and_then(|decision| decision.route_edge_id)
        else {
            return;
        };
        let route_pubkeys = self
            .register_focused_route_manifest_accounts(route_edge_id)
            .await;
        if !route_pubkeys.is_empty() {
            self.warm_hot_subscription_cache(&route_pubkeys).await;
        }
    }

    fn focused_route_manifest_pubkeys(&self, route_edge_id: i64) -> Vec<String> {
        self.database
            .as_ref()
            .and_then(|db| db.lock().ok())
            .and_then(|db| db.route_subscription_pubkeys(route_edge_id).ok())
            .unwrap_or_default()
    }

    fn hot_refresh_pubkeys_for_candidate(&self, opportunity: &Opportunity) -> Vec<String> {
        let mut pubkeys = vec![opportunity.buy_pool.clone(), opportunity.sell_pool.clone()];
        if let Some(db) = &self.database {
            if let Ok(db) = db.lock() {
                if let Ok(Some(route_edge_id)) = db.route_edge_id_for_pools(
                    &opportunity.pair,
                    opportunity.buy_venue,
                    opportunity.sell_venue,
                    &opportunity.buy_pool,
                    &opportunity.sell_pool,
                ) {
                    if let Ok(route_pubkeys) = db.route_subscription_pubkeys(route_edge_id) {
                        if !route_pubkeys.is_empty() {
                            pubkeys = route_pubkeys;
                        }
                    }
                }
            }
        }

        pubkeys.push(opportunity.pair.quote_mint.clone());
        pubkeys.push(opportunity.pair.base_mint.clone());

        let mut seen = HashSet::new();
        pubkeys
            .into_iter()
            .filter(|pubkey| is_hot_subscription_pubkey(pubkey))
            .filter(|pubkey| seen.insert(pubkey.clone()))
            .collect()
    }

    fn hot_subscription_routes(&self, candidates: &[Opportunity]) -> Vec<HotSubscriptionRoute> {
        candidates
            .iter()
            .map(|opportunity| {
                let mut readiness_state = None;
                let mut route_edge_id = None;
                let mut pubkeys = vec![opportunity.buy_pool.clone(), opportunity.sell_pool.clone()];
                if let Some(db) = &self.database {
                    if let Ok(db) = db.lock() {
                        match db.route_edge_id_for_pools(
                            &opportunity.pair,
                            opportunity.buy_venue,
                            opportunity.sell_venue,
                            &opportunity.buy_pool,
                            &opportunity.sell_pool,
                        ) {
                            Ok(Some(edge_id)) => {
                                route_edge_id = Some(edge_id);
                                readiness_state = db
                                    .route_edge_readiness_for_pools(
                                        &opportunity.pair,
                                        opportunity.buy_venue,
                                        opportunity.sell_venue,
                                        &opportunity.buy_pool,
                                        &opportunity.sell_pool,
                                    )
                                    .ok()
                                    .flatten();
                                if let Ok(route_pubkeys) = db.route_subscription_pubkeys(edge_id) {
                                    if !route_pubkeys.is_empty() {
                                        pubkeys = route_pubkeys;
                                    }
                                }
                            }
                            Ok(None) => {}
                            Err(error) => {
                                debug!(?error, "route edge lookup failed for subscription hot set");
                            }
                        }
                    }
                }
                HotSubscriptionRoute {
                    route_edge_id,
                    pair: opportunity.pair.clone(),
                    buy_venue: opportunity.buy_venue,
                    sell_venue: opportunity.sell_venue,
                    readiness_state,
                    pubkeys,
                }
            })
            .collect()
    }

    fn runtime_evidence_snapshot(&self, state: &StateSnapshot) -> RuntimeEvidenceSnapshot {
        let mut snapshot = RuntimeEvidenceSnapshot {
            database_enabled: self.config.database.enabled,
            database_path: self
                .config
                .database
                .enabled
                .then(|| self.config.database.path.clone()),
            database_error: self.database_error.clone(),
            memory_simulation_attempted_count: state.simulation.attempted_count,
            memory_execution_attempted_count: state.execution.attempted_count,
            memory_execution_blocked_count: state.execution.blocked_count,
            ..RuntimeEvidenceSnapshot::default()
        };
        let Some(db) = &self.database else {
            return snapshot;
        };
        let Some(baseline) = &self.database_baseline else {
            return snapshot;
        };
        match db.lock().ok().and_then(|db| db.summary().ok()) {
            Some(current) => {
                snapshot.quote_observations_delta = current
                    .quote_observations
                    .saturating_sub(baseline.quote_observations);
                snapshot.simulation_results_delta = current
                    .simulation_results
                    .saturating_sub(baseline.simulation_results);
                snapshot.execution_attempts_delta = current
                    .execution_attempts
                    .saturating_sub(baseline.execution_attempts);
            }
            None => {
                snapshot.database_error =
                    Some("runtime database summary failed during state refresh".to_string());
            }
        }
        snapshot
    }

    fn rwa_live_executor(&self, sol_usd_estimate: f64) -> JupiterLiveSwapExecutor {
        JupiterLiveSwapExecutor::new(self.config_with_sol_usd(sol_usd_estimate))
    }

    fn rwa_quote_executor(&self, sol_usd_estimate: f64) -> JupiterLiveSwapExecutor {
        let mut config = self.config_with_sol_usd(sol_usd_estimate);
        config.execution.require_simulation = false;
        JupiterLiveSwapExecutor::new(config)
    }

    pub async fn pause(&self) {
        self.state.write().await.status = BotStatus::Paused;
    }

    pub async fn resume(&self) {
        self.state.write().await.status = BotStatus::Running;
    }

    pub async fn emergency_stop(&self) {
        self.state.write().await.status = BotStatus::EmergencyStopped;
    }

    pub async fn run_forever(&self) {
        self.resume().await;
        let (ata_mode, setup_cost_info) = if self.config.execution.native_prefunded_hot_accounts {
            (
                "prefunded",
                "ATA rent excluded from cost estimates".to_string(),
            )
        } else {
            (
                "on-demand",
                "ATA rent (~0.002 SOL per ATA) included in cost estimates. Run scripts/prefund-atas.sh and set NATIVE_PREFUNDED_HOT_ACCOUNTS=true to eliminate ATA rent overhead."
                    .to_string(),
            )
        };
        info!("ATA mode: {ata_mode} — {setup_cost_info}");
        info!(
            "Fee config: max priority {} lamports, jito tip {} lamports, min profit floor ${:.4}, min bps {}",
            self.config.fees.max_priority_fee_lamports,
            self.config.fees.jito_tip_lamports,
            self.config.execution.min_absolute_profit_usd,
            self.config.trading.min_net_profit_bps,
        );
        if self.config.execution.max_pool_slot_age > 0 {
            info!(
                "Pool staleness gate: max {} slots age",
                self.config.execution.max_pool_slot_age,
            );
        }
        let mut scan_interval =
            tokio::time::interval(Duration::from_millis(self.config.trading.scan_interval_ms));
        let mut reactive_interval =
            tokio::time::interval(Duration::from_millis(FOCUSED_ROUTE_REQUOTE_DEBOUNCE_MS));
        loop {
            let reactive = tokio::select! {
                _ = scan_interval.tick() => false,
                _ = reactive_interval.tick() => {
                    !self.pool_subscription.pop_due_focused_requote_events().await.is_empty()
                }
            };
            if reactive {
                debug!("reactive focused-route re-quote cycle triggered");
            }
            let status = self.state.read().await.status.clone();
            match status {
                BotStatus::Running => {
                    if let Err(error) = self.run_cycle().await {
                        error!(?error, "scan cycle failed");
                        self.state.write().await.last_error = Some(error.to_string());
                    }
                }
                BotStatus::Paused => {}
                BotStatus::EmergencyStopped => break,
            }
        }
    }

    pub async fn run_cycle(&self) -> Result<()> {
        let adapters = enabled_adapters(&self.config.venues.enabled);
        let next_cycle = self.state.read().await.cycle + 1;
        let sol_usd = self
            .sol_usd_estimator
            .estimate(self.config.trading.sol_usd_estimate)
            .await;
        let sol_usd_estimate = sol_usd.price_usd;
        let network_fee = self
            .fee_engine
            .network_fee_for_accounts_with_sol_usd(&self.rpc, &[], false, sol_usd_estimate)
            .await
            .unwrap_or_else(|_| {
                self.fee_engine
                    .static_network_fee_with_sol_usd(false, sol_usd_estimate)
            });

        let mut discovered = self.discovered_cache.read().await.clone();
        let mut discovery = self.discovery_snapshot(next_cycle).await;
        if self.should_refresh_discovery().await {
            let target_pairs = self
                .aggregators
                .scan_pairs_dynamic(
                    &[],
                    &self.config.trading.quote_mints,
                    &self.config.trading.major_denylist,
                )
                .await;
            let discovery_run = self.discover_parallel(&adapters, &target_pairs).await?;
            discovered = discovery_run.pools;
            *self.discovered_cache.write().await = discovered.clone();
            discovery.attempted_this_cycle = true;
            discovery.last_attempt_cycle = Some(next_cycle);
            discovery.last_attempt_unix_ms = Some(now_unix_ms());
            discovery.jobs = discovery_run.jobs;
            discovery.indexed_candidates = discovery_run.indexed_candidates;
            discovery.targeted_accounts = discovery_run.targeted_accounts;
            discovery.broad_rpc_enabled = self.config.discovery.broad_rpc_enabled;
            discovery.decoded_pools = discovered.len();
            discovery.errors = discovery_run.errors;
            discovery.next_attempt_in_ms = self.discovery_next_attempt_in_ms().await;
        }

        let mut promoted = self.promote_pools(discovered);
        self.append_focused_route_pools_from_route_graph(
            &adapters,
            &mut promoted,
            sol_usd_estimate,
        )
        .await;
        let execution_pairs = self
            .aggregators
            .scan_pairs_dynamic(
                &promoted,
                &self.config.trading.quote_mints,
                &self.config.trading.major_denylist,
            )
            .await;
        let mut quotes = self.compute_quotes(&adapters, &promoted, &network_fee, sol_usd_estimate);
        quotes.extend(
            self.aggregators
                .fetch_reference_quotes(
                    &promoted,
                    &network_fee,
                    self.config.trading.trade_size_usd,
                    sol_usd_estimate,
                    self.config.trading.fee_safety_buffer_bps,
                )
                .await,
        );
        let mut opportunities = self.find_opportunities(&promoted, &network_fee, sol_usd_estimate);
        opportunities.extend(
            self.aggregators
                .fetch_trade_source_opportunities(
                    &execution_pairs,
                    &network_fee,
                    self.config.trading.trade_size_usd,
                    sol_usd_estimate,
                    self.config.trading.fee_safety_buffer_bps,
                )
                .await,
        );
        opportunities.sort_by(|a, b| {
            b.net_profit_usd
                .partial_cmp(&a.net_profit_usd)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let rwa_taker_pubkey = read_keypair_file(&self.config.wallet.path)
            .ok()
            .map(|wallet| wallet.pubkey().to_string());
        let rwa_taker_wallet_balances = if self.config.rwa.enabled && rwa_taker_pubkey.is_some() {
            self.live_wallet_quote_balances()
                .await
                .ok()
                .map(|(balances, _, _)| balances)
        } else {
            None
        };
        let mut rwa = scan_rwa(
            &self.config.rwa,
            &self.aggregators,
            &network_fee,
            sol_usd_estimate,
            next_cycle,
            rwa_taker_pubkey.as_deref(),
            rwa_taker_wallet_balances.as_ref(),
        )
        .await;
        self.execute_rwa_live(&mut rwa, next_cycle, sol_usd_estimate)
            .await;
        let active_tokens = promoted
            .iter()
            .map(|pool| pool.pair.base_mint.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let quote_venues = quotes
            .iter()
            .map(|quote| quote.venue.to_string())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let target_pairs = execution_pairs
            .iter()
            .map(ScanTargetPair::from)
            .collect::<Vec<_>>();
        let execution_quote_balance_caps = if self.config.trading.live
            && self.config.execution.enabled
            && self.config.execution.native_prefunded_hot_accounts
            && !self.config.execution.native_focus_routes.is_empty()
        {
            self.live_wallet_quote_balances()
                .await
                .ok()
                .map(|(balances, _, _)| balances)
        } else {
            None
        };
        let (
            execution_candidates,
            mut execution_candidate_source,
            mut focused_route_candidate_rejections,
        ) = self.execution_candidates_for_cycle(
            &promoted,
            &opportunities,
            &execution_pairs,
            &network_fee,
            sol_usd_estimate,
            execution_quote_balance_caps.as_ref(),
        );
        let focused_candidate_count_before_fresh_quote = execution_candidates.len();
        let execution_candidates = self
            .fresh_quote_focused_native_candidates(
                execution_candidates,
                &mut focused_route_candidate_rejections,
                next_cycle,
                &network_fee,
                sol_usd_estimate,
            )
            .await;
        if execution_candidate_source == "focused native optimized route"
            && focused_candidate_count_before_fresh_quote > 0
            && execution_candidates.is_empty()
        {
            execution_candidate_source = "focused native optimized route unavailable".to_string();
        }
        self.update_hot_subscriptions(&execution_candidates, &promoted)
            .await;
        let scan = ScanSnapshot {
            trade_size_usd: self.config.trading.trade_size_usd,
            min_net_profit_bps: self.config.trading.min_net_profit_bps,
            sol_usd,
            direct_venues: self.config.venues.enabled.clone(),
            target_pairs,
            quote_providers: self.aggregators.provider_snapshots().await,
            discovery,
            direct_pool_cache_count: self.discovered_cache.read().await.len(),
            promoted_pool_count: promoted.len(),
            quote_count: quotes.len(),
            quote_venues,
        };
        let mut execution = self
            .execution_snapshot(
                &execution_candidates,
                next_cycle,
                &execution_candidate_source,
                &promoted,
                &quotes,
                sol_usd_estimate,
            )
            .await;
        execution.focused_route_candidate_rejections = focused_route_candidate_rejections;
        execution.fresh_quote_truth = fresh_quote_truth_from_candidates_and_rejections(
            &execution_candidates,
            &execution.focused_route_candidate_rejections,
        );
        let simulation = self.simulation_snapshot(&opportunities, next_cycle).await;

        let mut state = self.state.write().await;
        state.status = BotStatus::Running;
        state.cycle += 1;
        state.scan = scan;
        state.execution = execution;
        state.simulation = simulation;
        state.rwa = rwa;
        state.pools = promoted;
        state.quotes = quotes;
        state.opportunities = opportunities;
        state.active_tokens = active_tokens;
        state.last_error = None;
        state.last_network_fee = network_fee;
        state.ws_connected = self.pool_subscription.is_connected().await;
        state.ws_last_update_slot = self.pool_subscription.last_update_slot().await;
        state.ws_subscription_count = self.pool_subscription.subscription_count().await;
        let ws_stats = self.pool_subscription.subscription_stats().await;
        state.ws_desired_subscription_count = ws_stats.desired;
        state.ws_pending_subscription_count = ws_stats.pending;
        state.ws_confirmed_subscription_count = ws_stats.confirmed;
        state.ws_rejected_subscription_count = ws_stats.rejected;
        state.ws_subscription_budget = ws_stats.budget;
        state.ws_subscription_budget_remaining = ws_stats.budget_remaining;
        state.ws_cache_size = self.pool_subscription.cache_size().await;
        state.focused_route_accounts = self
            .pool_subscription
            .focused_route_readiness_snapshots()
            .await;
        state.focused_route_requotes = self
            .pool_subscription
            .focused_route_requote_latency_snapshots()
            .await;
        state.runtime_evidence = self.runtime_evidence_snapshot(&state);
        info!(
            cycle = state.cycle,
            pools = state.pools.len(),
            opportunities = state.opportunities.len(),
            "scan cycle complete"
        );
        Ok(())
    }

    async fn should_refresh_discovery(&self) -> bool {
        if !self.config.discovery.enabled {
            return false;
        }

        let mut last_attempt = self.last_discovery_attempt.lock().await;
        let now = Instant::now();
        let should_refresh = last_attempt
            .map(|last| {
                now.duration_since(last)
                    >= Duration::from_millis(self.config.discovery.refresh_interval_ms)
            })
            .unwrap_or(true);
        if should_refresh {
            *last_attempt = Some(now);
        }
        should_refresh
    }

    async fn discovery_snapshot(&self, cycle: u64) -> DiscoverySnapshot {
        let mut discovery = self.state.read().await.scan.discovery.clone();
        discovery.enabled = self.config.discovery.enabled;
        discovery.refresh_interval_ms = self.config.discovery.refresh_interval_ms;
        discovery.broad_rpc_enabled = self.config.discovery.broad_rpc_enabled;
        discovery.attempted_this_cycle = false;
        discovery.next_attempt_in_ms = self.discovery_next_attempt_in_ms().await;
        if discovery.last_attempt_cycle.is_none() && cycle == 1 {
            discovery.errors.clear();
        }
        discovery
    }

    async fn discovery_next_attempt_in_ms(&self) -> Option<u64> {
        if !self.config.discovery.enabled {
            return None;
        }

        let last_attempt = self.last_discovery_attempt.lock().await;
        let last_attempt = (*last_attempt)?;
        let elapsed_ms = Instant::now().duration_since(last_attempt).as_millis() as u64;
        Some(
            self.config
                .discovery
                .refresh_interval_ms
                .saturating_sub(elapsed_ms),
        )
    }

    async fn discover_parallel(
        &self,
        adapters: &[Box<dyn VenueAdapter>],
        target_pairs: &[TokenPair],
    ) -> Result<DiscoveryRun> {
        let mut pools = Vec::new();
        let mut errors = Vec::new();
        let mut futures = FuturesUnordered::new();
        let mut jobs = 0;

        let quote_mints = self
            .config
            .trading
            .quote_mints
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let enabled_venues = adapters
            .iter()
            .map(|adapter| adapter.venue())
            .collect::<HashSet<_>>();
        let index_result = self
            .pool_index
            .fetch_candidates(target_pairs, &quote_mints, &enabled_venues)
            .await;
        let indexed_candidates = index_result.candidates.len();
        errors.extend(index_result.errors.into_iter().map(|issue| DiscoveryIssue {
            kind: DiscoveryIssueKind::Index,
            venue: crate::types::Venue::Custom,
            program_id: issue.source,
            error: format!("{}: {}", issue.token_mint, issue.error),
        }));
        let indexed_run = self
            .decode_indexed_candidates(adapters, index_result.candidates)
            .await;
        let targeted_accounts = indexed_run.targeted_accounts;
        jobs += indexed_run.targeted_accounts;
        pools.extend(indexed_run.pools);
        errors.extend(indexed_run.errors);

        if self.config.discovery.broad_rpc_enabled {
            for adapter in adapters {
                for job in adapter.discovery_jobs() {
                    jobs += 1;
                    let rpc = self.rpc.clone();
                    let venue = adapter.venue();
                    futures.push(async move {
                        debug!(
                            venue = %venue,
                            program_id = job.program_id,
                            data_size = ?job.data_size,
                            "discovering program accounts"
                        );
                        let accounts = rpc
                            .get_program_accounts(job.program_id, job.data_size, None)
                            .await;
                        (venue, job.program_id, accounts)
                    });
                }
            }
        }

        while let Some((venue, program_id, accounts)) = futures.next().await {
            match accounts {
                Ok(accounts) => {
                    if let Some(adapter) = adapters.iter().find(|adapter| adapter.venue() == venue)
                    {
                        let before = pools.len();
                        for account in accounts {
                            if let Some(pool) = adapter.decode_pool(&account) {
                                pools.push(pool);
                            }
                        }
                        debug!(
                            venue = %venue,
                            program_id,
                            decoded = pools.len().saturating_sub(before),
                            "venue discovery finished"
                        );
                    }
                }
                Err(error) => {
                    warn!(venue = %venue, program_id, ?error, "venue discovery failed");
                    errors.push(DiscoveryIssue {
                        kind: DiscoveryIssueKind::Rpc,
                        venue,
                        program_id: program_id.to_string(),
                        error: format!("{error:#}"),
                    });
                }
            }
        }

        Ok(DiscoveryRun {
            pools,
            jobs,
            indexed_candidates,
            targeted_accounts,
            errors,
        })
    }

    async fn execution_snapshot(
        &self,
        candidates: &[Opportunity],
        cycle: u64,
        candidate_source: &str,
        pools: &[PoolState],
        quotes: &[Quote],
        sol_usd_estimate: f64,
    ) -> ExecutionSnapshot {
        let mut snapshot = self.state.read().await.execution.clone();
        snapshot.live_requested = self.config.trading.live;
        snapshot.submission_enabled = false;
        snapshot.paths = self.execution_paths(pools, quotes).await;
        snapshot.candidate_source = candidate_source.to_string();
        snapshot.candidate_pool_size = candidates.len();
        snapshot.candidate_cooldown_count = 0;
        snapshot.probe_trade_sizes_usd =
            probe_trade_sizes_usd(&self.config.execution, self.config.trading.trade_size_usd);
        snapshot.api_requests_per_window = self.config.execution.jupiter_requests_per_window;
        snapshot.api_window_ms = self.config.execution.jupiter_window_ms;
        snapshot.api_requests_budget_per_cycle = self
            .config
            .execution
            .jupiter_requests_budget_per_cycle(self.config.trading.scan_interval_ms);
        snapshot.api_estimated_requests_per_candidate = self
            .config
            .execution
            .jupiter_estimated_requests_per_candidate
            .max(1);
        snapshot.api_candidate_budget_per_cycle = self
            .config
            .execution
            .jupiter_candidate_budget_per_cycle(self.config.trading.scan_interval_ms);
        snapshot.api_candidate_limit_applied = 0;
        snapshot.candidate_start_offset = 0;
        snapshot.api_rate_limit_cooldown_ms = self.config.execution.jupiter_rate_limit_cooldown_ms;
        snapshot.api_rate_limited = false;
        snapshot.wallet_quote_balances.clear();
        snapshot.wallet_quote_balance_errors.clear();
        snapshot.wallet_funding_blocked_count = 0;
        snapshot.best_funding_blocked_required_usd = None;
        snapshot.best_funding_blocked_available_usd = None;
        snapshot.selected_candidates = Vec::new();
        snapshot.best_recent_candidates = self.best_recent_candidate_snapshots(8).await;
        snapshot.focused_route_families = self.focused_route_family_snapshots(12);
        snapshot.route_family_count = snapshot.focused_route_families.len();
        snapshot.route_edge_count = snapshot
            .focused_route_families
            .iter()
            .map(|family| family.total_edges as usize)
            .sum();

        if !self.config.trading.live {
            snapshot.reason =
                "trading.live=false; scanner is monitoring and planning only".to_string();
            return snapshot;
        }

        if !self.config.execution.enabled {
            snapshot.reason = "trading.live=true, but execution.enabled=false".to_string();
            return snapshot;
        }

        snapshot.submission_enabled = self.config.execution.submit_enabled;

        if self.config.execution.submit_enabled {
            if let Some(reason) = live_submit_proof_arming_blocker(&self.config.execution) {
                snapshot.reason = reason;
                return snapshot;
            }
        }

        if let Some(last_rate_limit) = *self.last_jupiter_rate_limit.lock().await {
            let cooldown =
                Duration::from_millis(self.config.execution.jupiter_rate_limit_cooldown_ms);
            let elapsed = Instant::now().duration_since(last_rate_limit);
            if !cooldown.is_zero() && elapsed < cooldown {
                snapshot.api_rate_limited = true;
                snapshot.reason = format!(
                    "Jupiter API cooldown: next live quote window in {}ms after a rate-limit response",
                    (cooldown - elapsed).as_millis()
                );
                return snapshot;
            }
        }

        if candidates.is_empty() {
            snapshot.reason =
                format!("execution enabled; no {candidate_source} candidate(s) this cycle");
            return snapshot;
        }

        let has_session_cap = self.config.execution.max_live_trades_per_session > 0;
        if has_session_cap
            && snapshot.attempted_count >= self.config.execution.max_live_trades_per_session
        {
            snapshot.reason = format!(
                "session live trade cap reached: {}/{}",
                snapshot.attempted_count, self.config.execution.max_live_trades_per_session
            );
            return snapshot;
        }

        if self.config.execution.min_interval_ms > 0 {
            if let Some(last) = *self.last_execution_attempt.lock().await {
                let elapsed = Instant::now().duration_since(last);
                let interval = Duration::from_millis(self.config.execution.min_interval_ms);
                if elapsed < interval {
                    snapshot.reason = format!(
                        "configured live execution pacing: next check in {}ms",
                        (interval - elapsed).as_millis()
                    );
                    return snapshot;
                }
            }
        }

        snapshot.live_risk = self.live_risk_snapshot().await;
        if self.config.execution.submit_enabled && !snapshot.live_risk.gate_open {
            snapshot.reason = snapshot
                .live_risk
                .blocked_reason
                .clone()
                .unwrap_or_else(|| "live submit risk gate is closed".to_string());
            return snapshot;
        }

        let wallet_quote_balances = match self.live_wallet_quote_balances().await {
            Ok((balances, snapshots, balance_errors)) => {
                snapshot.wallet_quote_balances = snapshots;
                snapshot.wallet_quote_balance_errors = balance_errors;
                if !snapshot.wallet_quote_balance_errors.is_empty() {
                    snapshot.last_error = Some(format!(
                        "live wallet funding check had {} partial token-balance error(s)",
                        snapshot.wallet_quote_balance_errors.len()
                    ));
                }
                balances
            }
            Err(error) => {
                snapshot.last_error = Some(format!("live wallet funding check failed: {error:#}"));
                snapshot.reason =
                    "execution enabled, but wallet quote balances could not be checked".to_string();
                return snapshot;
            }
        };
        let (fundable_candidates, funding_blocked) =
            fundable_live_candidates(candidates, &wallet_quote_balances, sol_usd_estimate);
        snapshot.wallet_funding_blocked_count = funding_blocked.blocked_count;
        snapshot.best_funding_blocked_required_usd = funding_blocked.best_required_usd;
        snapshot.best_funding_blocked_available_usd = funding_blocked.best_available_usd;
        if fundable_candidates.is_empty() {
            snapshot.reason = if snapshot.wallet_funding_blocked_count > 0 {
                format!(
                    "execution enabled, but all {} {candidate_source} candidate(s) exceed wallet quote balances",
                    candidates.len()
                )
            } else {
                format!("execution enabled; no fundable {candidate_source} candidate(s) this cycle")
            };
            return snapshot;
        }

        let session_remaining = if has_session_cap {
            self.config
                .execution
                .max_live_trades_per_session
                .saturating_sub(snapshot.attempted_count) as usize
        } else {
            usize::MAX
        };
        let cycle_limit = if self.config.execution.max_live_trades_per_cycle == 0 {
            usize::MAX
        } else {
            self.config.execution.max_live_trades_per_cycle
        };
        let trade_limit = cycle_limit.min(session_remaining);
        let mut ran = 0usize;
        let mut inspected = 0usize;
        let candidate_limit = self.config.execution.live_candidate_limit(
            self.config.trading.scan_interval_ms,
            fundable_candidates.len(),
        );
        snapshot.api_candidate_limit_applied = candidate_limit;
        let candidate_start_offset = if candidate_limit == 0 {
            0
        } else {
            (self
                .live_candidate_cursor
                .fetch_add(candidate_limit as u64, Ordering::Relaxed) as usize)
                % fundable_candidates.len()
        };
        snapshot.candidate_start_offset = candidate_start_offset;
        let selection = self
            .select_live_candidate_indices(
                &fundable_candidates,
                candidate_limit,
                candidate_start_offset,
                sol_usd_estimate,
            )
            .await;
        snapshot.candidate_cooldown_count = selection.cooldown_count;
        snapshot.selected_candidates = selection.selected_candidates;
        let candidate_indices = selection.indices;
        if candidate_indices.is_empty() {
            snapshot.reason = format!(
                "{} {candidate_source} candidate(s) are temporarily cooling down after recent no-route or negative re-quotes",
                fundable_candidates.len()
            );
            return snapshot;
        }
        let mut hit_rate_limit = false;
        let rate_limited = Arc::new(AtomicBool::new(false));
        let trade_limit_remaining = Arc::new(AtomicUsize::new(trade_limit));
        let rpc_semaphore = Arc::new(tokio::sync::Semaphore::new(16));

        let mut warmed_route_edges = HashSet::new();
        let mut ready_candidate_indices = Vec::with_capacity(candidate_indices.len());
        for &candidate_index in &candidate_indices {
            let Some(route_edge_id) = fundable_candidates[candidate_index]
                .optimizer_decision
                .as_ref()
                .and_then(|decision| decision.route_edge_id)
            else {
                ready_candidate_indices.push(candidate_index);
                continue;
            };
            if warmed_route_edges.insert(route_edge_id) {
                self.warm_selected_focused_route_cache(&fundable_candidates[candidate_index])
                    .await;
            }
            if let Err(error) = self
                .pool_subscription
                .ensure_focused_route_ready_for_quote(route_edge_id)
                .await
            {
                let id = self.execution_sequence.fetch_add(1, Ordering::Relaxed);
                let reason = format!("focused route {route_edge_id} preflight failed: {error:#}");
                self.record_live_probe_outcome(
                    &fundable_candidates[candidate_index],
                    None,
                    Some(reason.clone()),
                    true,
                    false,
                    sol_usd_estimate,
                )
                .await;
                snapshot.blocked_count = snapshot.blocked_count.saturating_add(1);
                snapshot.last_error = Some(reason.clone());
                snapshot.trades.insert(
                    0,
                    blocked_live_record_from_opportunity(
                        id,
                        cycle,
                        &fundable_candidates[candidate_index],
                        reason,
                    ),
                );
                continue;
            }
            ready_candidate_indices.push(candidate_index);
        }
        let candidate_indices = ready_candidate_indices;
        if candidate_indices.is_empty() {
            snapshot.reason = format!(
                "execution enabled, but all selected {candidate_source} candidate(s) failed focused route preflight readiness"
            );
            return snapshot;
        }

        let mut futures = FuturesUnordered::new();
        for &candidate_index in &candidate_indices {
            let opportunity = fundable_candidates[candidate_index].clone();
            let refresh_pubkeys = self.hot_refresh_pubkeys_for_candidate(&opportunity);
            let id = self.execution_sequence.fetch_add(1, Ordering::Relaxed);
            let config = self.config_with_sol_usd(sol_usd_estimate);
            let submit_enabled = config.execution.submit_enabled;
            let rate_limited = rate_limited.clone();
            let trade_limit_remaining = trade_limit_remaining.clone();
            let sem = rpc_semaphore.clone();

            let pool_sub = self.pool_subscription.clone();
            futures.push(tokio::spawn(async move {
                let _permit = sem.acquire().await;
                if rate_limited.load(Ordering::Relaxed) {
                    return None;
                }
                let remaining = trade_limit_remaining.load(Ordering::Relaxed);
                if remaining == 0 {
                    return None;
                }
                let route_edge_id = opportunity
                    .optimizer_decision
                    .as_ref()
                    .and_then(|decision| decision.route_edge_id);
                if let Some(route_edge_id) = route_edge_id {
                    if let Err(error) = pool_sub
                        .ensure_focused_route_ready_for_quote(route_edge_id)
                        .await
                    {
                        return Some((id, opportunity, Err(error)));
                    }
                    pool_sub
                        .note_focused_route_quote_started(route_edge_id)
                        .await;
                }
                let executor = JupiterLiveSwapExecutor::new(config)
                    .with_pool_subscription(pool_sub.clone())
                    .with_native_market_refresh_pubkeys(refresh_pubkeys);
                let report = executor
                    .execute_atomic_arbitrage(&opportunity, submit_enabled)
                    .await;
                if let (Some(route_edge_id), Ok(report)) = (route_edge_id, report.as_ref()) {
                    if report.submitted {
                        pool_sub
                            .note_focused_route_submit_started(route_edge_id)
                            .await;
                    }
                }
                Some((id, opportunity, report))
            }));
        }

        while let Some(result) = futures.next().await {
            match result {
                Ok(Some((id, opportunity, Ok(report)))) => {
                    inspected += 1;
                    self.record_live_probe_report(
                        &opportunity,
                        &report,
                        cycle,
                        id,
                        sol_usd_estimate,
                    )
                    .await;
                    if matches!(report.status, AtomicArbStatus::Blocked) {
                        let blocked_reason = report.blocked_reason.clone();
                        snapshot.blocked_count = snapshot.blocked_count.saturating_add(1);
                        snapshot.last_error = report.blocked_reason.clone();
                        snapshot
                            .trades
                            .insert(0, live_record_from_report(id, cycle, &report));
                        if blocked_reason
                            .as_deref()
                            .is_some_and(is_jupiter_rate_limit_error)
                        {
                            *self.last_jupiter_rate_limit.lock().await = Some(Instant::now());
                            snapshot.api_rate_limited = true;
                            hit_rate_limit = true;
                            rate_limited.store(true, Ordering::Relaxed);
                            continue;
                        }
                        continue;
                    }
                    snapshot.attempted_count = snapshot.attempted_count.saturating_add(1);
                    if trade_limit_remaining.fetch_sub(1, Ordering::Relaxed) <= 1 {
                        hit_rate_limit = true;
                    }
                    ran += 1;
                    if matches!(report.status, AtomicArbStatus::Confirmed) {
                        snapshot.confirmed_count = snapshot.confirmed_count.saturating_add(1);
                    }
                    if matches!(report.status, AtomicArbStatus::Failed) {
                        snapshot.failed_count = snapshot.failed_count.saturating_add(1);
                    }
                    if let Some(signature) = &report.signature {
                        snapshot.last_signature = Some(signature.clone());
                    }
                    snapshot.last_error = report
                        .blocked_reason
                        .clone()
                        .or(report.confirmation_error.clone());
                    snapshot
                        .trades
                        .insert(0, live_record_from_report(id, cycle, &report));
                }
                Ok(Some((id, opportunity, Err(error)))) => {
                    inspected += 1;
                    self.record_live_probe_error(
                        &opportunity,
                        &format!("{error:#}"),
                        cycle,
                        id,
                        sol_usd_estimate,
                    )
                    .await;
                    snapshot.attempted_count = snapshot.attempted_count.saturating_add(1);
                    if trade_limit_remaining.fetch_sub(1, Ordering::Relaxed) <= 1 {
                        hit_rate_limit = true;
                    }
                    ran += 1;
                    snapshot.failed_count = snapshot.failed_count.saturating_add(1);
                    let error_text = format!("{error:#}");
                    if is_jupiter_rate_limit_error(&error_text) {
                        *self.last_jupiter_rate_limit.lock().await = Some(Instant::now());
                        snapshot.api_rate_limited = true;
                        hit_rate_limit = true;
                        rate_limited.store(true, Ordering::Relaxed);
                    }
                    snapshot.last_error = Some(error_text);
                    snapshot.trades.insert(
                        0,
                        failed_live_record_from_opportunity(id, cycle, &opportunity, &error),
                    );
                }
                _ => {}
            }
        }
        if inspected > 0 {
            *self.last_execution_attempt.lock().await = Some(Instant::now());
        }

        snapshot.trades.truncate(50);
        snapshot.best_recent_candidates = self.best_recent_candidate_snapshots(8).await;
        snapshot.reason = if hit_rate_limit {
            "Jupiter API rate limit hit; live candidate inspection is cooling down".to_string()
        } else if ran == 0 {
            format!(
                "inspected {inspected}/{} fundable {candidate_source} candidate(s) from offset {candidate_start_offset} within Jupiter API budget; none passed live re-quote gates",
                fundable_candidates.len()
            )
        } else {
            format!(
                "live execution attempted {ran} {candidate_source} candidate(s); inspected {inspected}/{} fundable candidates from offset {candidate_start_offset} within Jupiter API budget",
                fundable_candidates.len()
            )
        };
        snapshot
    }

    fn focused_route_family_snapshots(&self, limit: usize) -> Vec<FocusedRouteFamilySnapshot> {
        let Some(database) = &self.database else {
            return Vec::new();
        };
        let Ok(db) = database.lock() else {
            return Vec::new();
        };
        let Ok(report) = db.route_status_report() else {
            return Vec::new();
        };

        report
            .families
            .into_iter()
            .take(limit)
            .map(|family| FocusedRouteFamilySnapshot {
                id: family.id,
                pair: TokenPair {
                    base_mint: family.base_mint,
                    quote_mint: family.quote_mint,
                    base_decimals: 0,
                    quote_decimals: 0,
                },
                buy_venue: family.buy_venue,
                sell_venue: family.sell_venue,
                enabled: family.enabled,
                priority: family.priority,
                total_edges: family.total_edges,
                readiness_counts: family
                    .readiness_counts
                    .into_iter()
                    .map(|count| FocusedRouteReadinessCountSnapshot {
                        readiness_state: count.readiness_state,
                        count: count.count,
                    })
                    .collect(),
                latest_blockers: family
                    .latest_blockers
                    .into_iter()
                    .map(|blocker| FocusedRouteBlockerSnapshot {
                        readiness_state: blocker.readiness_state,
                        blocker: blocker.blocker,
                        count: blocker.count,
                        latest_updated_at_unix_ms: blocker.latest_updated_at_unix_ms,
                    })
                    .collect(),
            })
            .collect()
    }

    async fn execute_rwa_live(&self, rwa: &mut RwaSnapshot, cycle: u64, sol_usd_estimate: f64) {
        let mut snapshot = self.state.read().await.rwa.execution.clone();
        snapshot.live_requested = self.config.trading.live && self.config.rwa.execution_enabled;
        snapshot.submission_enabled = false;

        if !rwa.enabled {
            snapshot.reason = rwa
                .last_error
                .clone()
                .unwrap_or_else(|| "RWA scanner is disabled".to_string());
            rwa.execution = snapshot;
            return;
        }

        if !self.config.trading.live {
            snapshot.reason =
                "trading.live=false; RWA scanner is monitoring and planning only".to_string();
            rwa.execution = snapshot;
            return;
        }

        if !self.config.rwa.execution_enabled {
            snapshot.reason = "rwa.execution_enabled=false; RWA live execution is off".to_string();
            rwa.execution = snapshot;
            return;
        }

        snapshot.submission_enabled = true;
        snapshot.last_error = None;
        snapshot.wallet_sol_lamports = 0;
        snapshot.wallet_balances.clear();
        snapshot.wallet_balance_errors.clear();
        snapshot.stable_available_usd = 0.0;
        snapshot.quote_funding_available_usd = 0.0;
        snapshot.configured_trade_usd = self.config.rwa.max_trade_usd_per_asset;
        snapshot.readiness_blockers.clear();
        snapshot.fundable_direct_candidates = 0;
        snapshot.inventory_blocked_opportunities = 0;
        snapshot.route_gated_opportunities = 0;
        snapshot.best_blocked_required_usd = None;
        snapshot.best_blocked_available_usd = None;
        let mut tracked_inputs = rwa
            .opportunities
            .iter()
            .filter(|opportunity| opportunity.net_profit_bps >= self.config.rwa.min_net_profit_bps)
            .map(|opportunity| {
                (
                    opportunity.from_mint.clone(),
                    opportunity.from_symbol.clone(),
                )
            })
            .collect::<HashMap<_, _>>();
        for asset in &rwa.assets {
            tracked_inputs.insert(asset.xstock_mint.clone(), asset.xstock_symbol.clone());
            tracked_inputs.insert(asset.ondo_mint.clone(), asset.ondo_symbol.clone());
        }
        for quote_asset in &self.config.rwa.quote_assets {
            let normalized = quote_asset.trim();
            match normalized.to_ascii_uppercase().as_str() {
                "USDC" => {
                    tracked_inputs.insert(USDC_MINT.to_string(), "USDC".to_string());
                }
                "USDT" => {
                    tracked_inputs.insert(USDT_MINT.to_string(), "USDT".to_string());
                }
                "SOL" | "WSOL" => {
                    tracked_inputs.insert(WSOL_MINT.to_string(), "SOL/WSOL".to_string());
                }
                _ => {}
            };
            if normalized == WSOL_MINT {
                tracked_inputs.insert(WSOL_MINT.to_string(), "SOL/WSOL".to_string());
            }
        }
        let wallet_balances = match self.rwa_wallet_token_balances(&tracked_inputs).await {
            Ok((sol_lamports, balances, balance_snapshots, balance_errors)) => {
                snapshot.wallet_sol_lamports = sol_lamports;
                snapshot.wallet_balances = balance_snapshots;
                snapshot.wallet_balance_errors = balance_errors;
                snapshot.stable_available_usd = stable_balance_usd(&balances);
                snapshot.quote_funding_available_usd =
                    quote_funding_balance_usd(&balances, sol_usd_estimate);
                if !snapshot.wallet_balance_errors.is_empty() {
                    snapshot.last_error = Some(format!(
                        "RWA wallet balance check had {} partial token-balance error(s)",
                        snapshot.wallet_balance_errors.len()
                    ));
                }
                balances
            }
            Err(error) => {
                snapshot.last_error = Some(format!("RWA wallet balance check failed: {error:#}"));
                snapshot.reason =
                    "RWA live enabled, but wallet balances could not be checked".to_string();
                rwa.execution = snapshot;
                return;
            }
        };
        snapshot.readiness_blockers =
            self.rwa_readiness_blockers(rwa, snapshot.quote_funding_available_usd);
        let mut best_inventory_blocked_score: Option<f64> = None;
        for opportunity in &mut rwa.opportunities {
            if !matches!(
                opportunity.kind,
                RwaOpportunityKind::StableRouteArbitrage
                    | RwaOpportunityKind::DirectIssuerSwap
                    | RwaOpportunityKind::StableFundedDirectIssuerRoundtrip
                    | RwaOpportunityKind::StableFundedDirectIssuerAccumulation
                    | RwaOpportunityKind::PrimaryMintSell
                    | RwaOpportunityKind::PrimaryBuyBurn
                    | RwaOpportunityKind::PythDiscountBuy
                    | RwaOpportunityKind::NeutralInventoryRebalance
                    | RwaOpportunityKind::DirectionalSell
            ) {
                continue;
            }
            if route_gated_rwa_opportunity(opportunity, self.config.rwa.min_net_profit_bps) {
                snapshot.route_gated_opportunities =
                    snapshot.route_gated_opportunities.saturating_add(1);
                continue;
            }
            if block_pyth_discount_top_up(
                opportunity,
                &wallet_balances,
                rwa_pyth_discount_buy_allow_top_ups(),
            ) {
                snapshot.inventory_blocked_opportunities =
                    snapshot.inventory_blocked_opportunities.saturating_add(1);
                continue;
            }
            let mut required = opportunity.input_atoms.unwrap_or_default();
            let mut available = wallet_balances
                .get(&opportunity.from_mint)
                .copied()
                .unwrap_or_default();
            if available < required
                && maybe_add_wsol_stable_funding_route(
                    opportunity,
                    &wallet_balances,
                    sol_usd_estimate,
                    self.config.rwa.min_net_profit_bps,
                    self.config.rwa.fee_safety_buffer_bps,
                )
            {
                required = opportunity.input_atoms.unwrap_or_default();
                available = wallet_balances
                    .get(&opportunity.from_mint)
                    .copied()
                    .unwrap_or_default();
            }
            if available < required {
                snapshot.inventory_blocked_opportunities =
                    snapshot.inventory_blocked_opportunities.saturating_add(1);
                let available_usd = opportunity
                    .input_estimated_usd
                    .filter(|value| required > 0 && value.is_finite())
                    .map(|input_usd| input_usd * available as f64 / required as f64);
                if best_inventory_blocked_score
                    .map(|score| opportunity.net_profit_usd > score)
                    .unwrap_or(true)
                {
                    best_inventory_blocked_score = Some(opportunity.net_profit_usd);
                    snapshot.best_blocked_required_usd = opportunity.input_estimated_usd;
                    snapshot.best_blocked_available_usd = available_usd;
                }
                opportunity.executable = false;
                let inventory_reason = format!(
                    "wallet balance for {} is {} atom(s), below required {} atom(s)",
                    opportunity.from_symbol, available, required
                );
                opportunity.blocked_reason = Some(match opportunity.blocked_reason.take() {
                    Some(reason) if !reason.is_empty() => {
                        format!("{reason}; {inventory_reason}")
                    }
                    _ => inventory_reason,
                });
            }
        }
        let route_cooldowns = self.jupiter_no_route_leg_cooldowns.read().await;
        let now = Instant::now();
        let mut cooled_down_candidates = 0usize;
        let mut candidates = Vec::new();
        for opportunity in &rwa.opportunities {
            if !(opportunity.executable
                && !opportunity.execution_legs.is_empty()
                && rwa_live_closed_roundtrip_candidate(opportunity)
                && opportunity.input_atoms.is_some()
                && opportunity.input_estimated_usd.is_some()
                && opportunity.net_profit_bps >= self.config.rwa.min_net_profit_bps
                && opportunity.net_profit_bps >= self.config.execution.min_requote_profit_bps)
            {
                continue;
            }
            if rwa_route_leg_cooldown_until(opportunity, &route_cooldowns)
                .is_some_and(|until| until > now)
            {
                cooled_down_candidates = cooled_down_candidates.saturating_add(1);
                continue;
            }
            candidates.push(opportunity.clone());
        }
        drop(route_cooldowns);
        candidates.sort_by(|left, right| {
            right
                .net_profit_usd
                .partial_cmp(&left.net_profit_usd)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        snapshot.fundable_direct_candidates = candidates.len();
        if candidates.is_empty() {
            snapshot.reason = if cooled_down_candidates > 0 {
                format!(
                    "RWA live enabled; {cooled_down_candidates} executable RWA route candidate(s) are cooling down after recent failed live re-quotes"
                )
            } else if snapshot.inventory_blocked_opportunities > 0
                && snapshot.route_gated_opportunities > 0
            {
                "RWA live enabled; profitable opportunities are split between wallet inventory blockers and route/policy gates"
                    .to_string()
            } else if snapshot.inventory_blocked_opportunities > 0 {
                "RWA live enabled; profitable opportunities have executable legs but are blocked by wallet inventory"
                    .to_string()
            } else if snapshot.route_gated_opportunities > 0 {
                "RWA live enabled; profitable signals exist but are blocked by route/policy gates"
                    .to_string()
            } else {
                "RWA live enabled; no profitable executable RWA route candidate this cycle"
                    .to_string()
            };
            rwa.execution = snapshot;
            return;
        }

        let finite_trade_limit = self.config.rwa.max_live_trades_per_cycle != 0;
        let trade_limit = if finite_trade_limit {
            self.config.rwa.max_live_trades_per_cycle
        } else {
            usize::MAX
        };
        let mut ran = 0usize;
        let mut candidate_queue = candidates
            .into_iter()
            .take(trade_limit)
            .collect::<VecDeque<_>>();

        while ran < trade_limit {
            let Some(opportunity) = candidate_queue.pop_front() else {
                break;
            };
            let route_cooldowns = self.jupiter_no_route_leg_cooldowns.read().await;
            let cooled_down = rwa_route_leg_cooldown_until(&opportunity, &route_cooldowns)
                .is_some_and(|until| until > Instant::now());
            drop(route_cooldowns);
            if cooled_down {
                continue;
            }
            let id = self.execution_sequence.fetch_add(1, Ordering::Relaxed);
            snapshot.attempted_count = snapshot.attempted_count.saturating_add(1);
            let record = self
                .execute_rwa_route(id, cycle, &opportunity, sol_usd_estimate)
                .await;
            if let Some(reason) = record.reason.as_deref() {
                self.record_rwa_route_cooldown(&opportunity, reason).await;
            }
            let confirmed = record.submitted && record.reason.is_none();
            if record.submitted {
                snapshot.submitted_count = snapshot.submitted_count.saturating_add(1);
            }
            if confirmed {
                snapshot.confirmed_count = snapshot.confirmed_count.saturating_add(1);
            } else if record.reason.is_some() || !record.submitted {
                snapshot.failed_count = snapshot.failed_count.saturating_add(1);
            }
            if let Some(signature) = &record.signature {
                snapshot.last_signature = Some(signature.clone());
            }
            snapshot.last_error = record.reason.clone();
            snapshot.trades.insert(0, record);
            ran += 1;
            if should_repeat_confirmed_rwa_candidate(
                confirmed,
                ran,
                trade_limit,
                finite_trade_limit,
            ) {
                candidate_queue.push_front(opportunity);
            }
        }

        snapshot.trades.truncate(50);
        snapshot.reason = format!("RWA live attempted {ran} executable RWA route candidate(s)");
        rwa.execution = snapshot;
    }

    async fn live_wallet_quote_balances(
        &self,
    ) -> Result<(
        HashMap<String, u128>,
        Vec<RwaWalletBalanceSnapshot>,
        Vec<String>,
    )> {
        let wallet = read_keypair_file(&self.config.wallet.path).map_err(|error| {
            anyhow!(
                "failed to read wallet keypair {}: {error}",
                self.config.wallet.path
            )
        })?;
        let wallet_pubkey = wallet.pubkey().to_string();
        let sol_lamports = self
            .rpc
            .get_balance_lamports(&wallet_pubkey)
            .await
            .unwrap_or_default();
        let mut balances = HashMap::new();
        let mut snapshots = Vec::new();
        let mut balance_errors = Vec::new();
        let mut seen = HashSet::new();

        for mint in &self.config.trading.quote_mints {
            if !seen.insert(mint.clone()) {
                continue;
            }
            if mint == &WSOL_MINT {
                let prefunded_token_amount = if self.config.execution.native_prefunded_hot_accounts
                {
                    match self.wallet_token_balance_atoms(&wallet_pubkey, mint).await {
                        Ok(amount) => Some(amount),
                        Err(error) => {
                            balance_errors.push(format!("WSOL prefunded ({mint}): {error:#}"));
                            Some(0)
                        }
                    }
                } else {
                    None
                };
                let amount = spendable_wsol_quote_balance_atoms(
                    sol_lamports,
                    prefunded_token_amount,
                    self.config.execution.native_prefunded_hot_accounts,
                );
                balances.insert(mint.clone(), amount);
                snapshots.push(RwaWalletBalanceSnapshot {
                    symbol: if self.config.execution.native_prefunded_hot_accounts {
                        "WSOL prefunded".to_string()
                    } else {
                        "SOL/WSOL".to_string()
                    },
                    mint: mint.clone(),
                    amount_atoms: amount,
                    decimals: 9,
                    ui_amount: atoms_to_ui(amount, 9),
                });
                continue;
            }

            let symbol = match mint.as_str() {
                USDC_MINT => "USDC",
                USDT_MINT => "USDT",
                _ => "quote",
            };
            let amount = match self.wallet_token_balance_atoms(&wallet_pubkey, mint).await {
                Ok(amount) => amount,
                Err(error) => {
                    balance_errors.push(format!("{symbol} ({mint}): {error:#}"));
                    0
                }
            };
            let decimals = decimals_for_quote_mint(mint);
            balances.insert(mint.clone(), amount);
            snapshots.push(RwaWalletBalanceSnapshot {
                symbol: symbol.to_string(),
                mint: mint.clone(),
                amount_atoms: amount,
                decimals,
                ui_amount: atoms_to_ui(amount, decimals),
            });
        }

        snapshots.sort_by(|left, right| left.symbol.cmp(&right.symbol));
        Ok((balances, snapshots, balance_errors))
    }

    async fn rwa_wallet_token_balances(
        &self,
        mints: &HashMap<String, String>,
    ) -> Result<(
        u64,
        HashMap<String, u128>,
        Vec<RwaWalletBalanceSnapshot>,
        Vec<String>,
    )> {
        let wallet = read_keypair_file(&self.config.wallet.path).map_err(|error| {
            anyhow!(
                "failed to read wallet keypair {}: {error}",
                self.config.wallet.path
            )
        })?;
        let wallet_pubkey = wallet.pubkey().to_string();
        let sol_lamports = self
            .rpc
            .get_balance_lamports(&wallet_pubkey)
            .await
            .unwrap_or_default();
        let mut balances = HashMap::new();
        let mut snapshots = Vec::new();
        let mut balance_errors = Vec::new();
        for (mint, symbol) in mints {
            if mint == WSOL_MINT {
                let amount = sol_lamports.saturating_sub(live_sol_fee_reserve_lamports()) as u128;
                balances.insert(mint.clone(), amount);
                snapshots.push(RwaWalletBalanceSnapshot {
                    symbol: symbol.clone(),
                    mint: mint.clone(),
                    amount_atoms: amount,
                    decimals: 9,
                    ui_amount: atoms_to_ui(amount, 9),
                });
                continue;
            }
            let result = match self
                .rpc
                .call(
                    "getTokenAccountsByOwner",
                    json!([
                        wallet_pubkey,
                        { "mint": mint },
                        {
                            "encoding": "jsonParsed",
                            "commitment": "processed"
                        }
                    ]),
                )
                .await
                .with_context(|| format!("failed to fetch wallet token accounts for {mint}"))
            {
                Ok(result) => result,
                Err(error) => {
                    balance_errors.push(format!("{symbol} ({mint}): {error:#}"));
                    balances.insert(mint.clone(), 0);
                    snapshots.push(RwaWalletBalanceSnapshot {
                        symbol: symbol.clone(),
                        mint: mint.clone(),
                        amount_atoms: 0,
                        decimals: token_decimals_for_symbol(symbol),
                        ui_amount: 0.0,
                    });
                    continue;
                }
            };
            let mut decimals = None;
            let amount = result
                .get("value")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|account| {
                    let (amount, account_decimals) = token_account_amount_atoms(account)?;
                    decimals.get_or_insert(account_decimals);
                    Some(amount)
                })
                .sum::<u128>();
            let decimals = decimals.unwrap_or_else(|| token_decimals_for_symbol(symbol));
            balances.insert(mint.clone(), amount);
            snapshots.push(RwaWalletBalanceSnapshot {
                symbol: symbol.clone(),
                mint: mint.clone(),
                amount_atoms: amount,
                decimals,
                ui_amount: atoms_to_ui(amount, decimals),
            });
        }
        snapshots.sort_by(|left, right| left.symbol.cmp(&right.symbol));
        Ok((sol_lamports, balances, snapshots, balance_errors))
    }

    async fn execute_rwa_route(
        &self,
        id: u64,
        cycle: u64,
        opportunity: &RwaOpportunity,
        sol_usd_estimate: f64,
    ) -> RwaExecutionRecord {
        let settlement_context = self.rwa_settlement_context(opportunity).await;
        let record = if opportunity.execution_legs.len() <= 1 {
            self.execute_rwa_direct_swap(id, cycle, opportunity, sol_usd_estimate)
                .await
        } else if rwa_atomic_swap_instruction_route_supported(opportunity) {
            self.execute_rwa_atomic_swap_instruction_route(id, cycle, opportunity, sol_usd_estimate)
                .await
        } else {
            self.execute_rwa_sequential_route(id, cycle, opportunity, sol_usd_estimate)
                .await
        };
        self.persist_rwa_settlement_if_confirmed(
            cycle,
            opportunity,
            &record,
            settlement_context,
            sol_usd_estimate,
        )
        .await;
        record
    }

    async fn rwa_settlement_context(
        &self,
        opportunity: &RwaOpportunity,
    ) -> Option<RwaSettlementContext> {
        if !rwa_live_closed_roundtrip_candidate(opportunity) {
            return None;
        }
        let quote_mint = opportunity.execution_legs.first()?.input_mint.clone();
        let wallet = match read_keypair_file(&self.config.wallet.path) {
            Ok(wallet) => wallet,
            Err(error) => {
                warn!(
                    error = %error,
                    "RWA settlement persistence skipped: wallet keypair could not be read"
                );
                return None;
            }
        };
        let wallet_pubkey = wallet.pubkey().to_string();
        let pre_sol_lamports = match self.rpc.get_balance_lamports(&wallet_pubkey).await {
            Ok(balance) => balance,
            Err(error) => {
                warn!(
                    error = %error,
                    "RWA settlement persistence skipped: pre-trade SOL balance unavailable"
                );
                return None;
            }
        };
        let pre_quote_atoms = match self
            .rwa_settlement_quote_balance_atoms(&wallet_pubkey, &quote_mint, pre_sol_lamports)
            .await
        {
            Ok(balance) => balance,
            Err(error) => {
                warn!(
                    quote_mint = %quote_mint,
                    error = %error,
                    "RWA settlement persistence skipped: pre-trade quote balance unavailable"
                );
                return None;
            }
        };
        Some(RwaSettlementContext {
            wallet_pubkey,
            quote_mint,
            pre_quote_atoms,
            pre_sol_lamports,
        })
    }

    async fn persist_rwa_settlement_if_confirmed(
        &self,
        cycle: u64,
        opportunity: &RwaOpportunity,
        record: &RwaExecutionRecord,
        context: Option<RwaSettlementContext>,
        sol_usd_estimate: f64,
    ) {
        if !record.submitted || record.reason.is_some() {
            return;
        }
        let Some(signature) = record.signature.as_deref() else {
            return;
        };
        let Some(context) = context else {
            return;
        };
        let Some(database) = self.database.as_ref() else {
            return;
        };
        let post_sol_lamports = match self.rpc.get_balance_lamports(&context.wallet_pubkey).await {
            Ok(balance) => balance,
            Err(error) => {
                warn!(
                    error = %error,
                    "RWA settlement persistence skipped: post-trade SOL balance unavailable"
                );
                return;
            }
        };
        let post_quote_atoms = match self
            .rwa_settlement_quote_balance_atoms(
                &context.wallet_pubkey,
                &context.quote_mint,
                post_sol_lamports,
            )
            .await
        {
            Ok(balance) => balance,
            Err(error) => {
                warn!(
                    quote_mint = %context.quote_mint,
                    error = %error,
                    "RWA settlement persistence skipped: post-trade quote balance unavailable"
                );
                return;
            }
        };
        let realized_delta = post_quote_atoms as i128 - context.pre_quote_atoms as i128;
        let fee_lamports = context.pre_sol_lamports.saturating_sub(post_sol_lamports);
        let fee_quote_atoms =
            sol_fee_quote_atoms(fee_lamports, &context.quote_mint, sol_usd_estimate);
        let realized_profit_atoms = realized_delta - fee_quote_atoms;
        let estimated_profit_atoms =
            (record.input_atoms as f64 * record.expected_net_profit_bps / 10_000.0).round() as i128;
        let classification = classify_confirmed_quote_delta(realized_profit_atoms);
        let now_ms = now_unix_ms();
        let attempt_id = format!("rwa-{cycle}-{}", record.id);
        let pre_balances_json = json!({
            "wallet": &context.wallet_pubkey,
            "quote_mint": &context.quote_mint,
            "quote_atoms": context.pre_quote_atoms.to_string(),
            "sol_lamports": context.pre_sol_lamports,
        });
        let post_balances_json = json!({
            "wallet": &context.wallet_pubkey,
            "quote_mint": &context.quote_mint,
            "quote_atoms": post_quote_atoms.to_string(),
            "sol_lamports": post_sol_lamports,
        });
        let meta_json = json!({
            "source": "rwa_execution",
            "ticker": &opportunity.ticker,
            "kind": format!("{:?}", opportunity.kind),
            "from_symbol": &opportunity.from_symbol,
            "to_symbol": &opportunity.to_symbol,
            "expected_net_profit_usd": record.expected_net_profit_usd,
            "expected_net_profit_bps": record.expected_net_profit_bps,
            "leg_signatures": &record.leg_signatures,
            "leg_routes": &record.leg_routes,
            "cycle": cycle,
        });
        let insert_result = database
            .lock()
            .map_err(|error| anyhow!("{error}"))
            .and_then(|db| {
                db.insert_live_settlement(&LiveSettlementRecord {
                    route_edge_id: None,
                    attempt_id: Some(attempt_id),
                    tx_signature: signature.to_string(),
                    status: "confirmed".to_string(),
                    quote_mint: context.quote_mint.clone(),
                    input_atoms: Some(record.input_atoms.to_string()),
                    estimated_profit_atoms: Some(estimated_profit_atoms.to_string()),
                    realized_quote_delta_atoms: Some(realized_delta.to_string()),
                    fee_lamports: Some(fee_lamports),
                    fee_quote_atoms: Some(fee_quote_atoms.to_string()),
                    realized_profit_atoms: Some(realized_profit_atoms.to_string()),
                    classification: classification.to_string(),
                    pre_balances_json: serde_json::to_string(&pre_balances_json)?,
                    post_balances_json: serde_json::to_string(&post_balances_json)?,
                    meta_json: serde_json::to_string(&meta_json)?,
                    settled_at_unix_ms: now_ms,
                })?;
                Ok(())
            });
        if let Err(error) = insert_result {
            warn!(error = %error, "failed to persist RWA live settlement evidence");
        }
    }

    async fn execute_rwa_direct_swap(
        &self,
        id: u64,
        cycle: u64,
        opportunity: &RwaOpportunity,
        sol_usd_estimate: f64,
    ) -> RwaExecutionRecord {
        let input_atoms = opportunity.input_atoms.unwrap_or_default();
        let estimated_input_usd = opportunity.input_estimated_usd.unwrap_or_default();
        let amount_atoms = match u64::try_from(input_atoms) {
            Ok(value) => value,
            Err(error) => {
                return failed_rwa_execution_record(
                    id,
                    cycle,
                    opportunity,
                    input_atoms,
                    estimated_input_usd,
                    format!("RWA input amount does not fit into u64: {error}"),
                );
            }
        };
        let max_usd = if self.config.execution.max_live_trade_usd > 0.0 {
            self.config.execution.max_live_trade_usd
        } else {
            (estimated_input_usd * 1.25).max(estimated_input_usd + 1.0)
        };
        let request = LiveSwapRequest {
            input_mint: opportunity.from_mint.clone(),
            output_mint: opportunity.to_mint.clone(),
            amount_atoms,
            slippage_bps: self.config.jupiter.slippage_bps,
            submit: true,
            max_usd,
            estimated_input_usd_override: Some(estimated_input_usd),
            required_route_label_contains: opportunity
                .execution_legs
                .first()
                .and_then(|leg| jupiter_v2_route_guard(&leg.route)),
            force_swap_instructions: false,
        };
        let executor = self.rwa_live_executor(sol_usd_estimate);
        let report = if let Some(leg) = opportunity.execution_legs.first() {
            execute_rwa_leg(
                &executor,
                opportunity,
                leg,
                amount_atoms,
                max_usd,
                Some(estimated_input_usd),
                true,
                self.config.jupiter.slippage_bps,
            )
            .await
        } else {
            executor.run(request).await
        };
        match report {
            Ok(report) => {
                let confirmed = live_swap_report_success(&report);
                let reason = report
                    .blocked_reason
                    .clone()
                    .or(report.confirmation_error.clone())
                    .or_else(|| {
                        (report.submitted && !confirmed).then(|| {
                            format!(
                                "confirmation status {}",
                                report
                                    .confirmation_status
                                    .as_deref()
                                    .unwrap_or("not returned")
                            )
                        })
                    });
                RwaExecutionRecord {
                    id,
                    cycle,
                    submitted_unix_ms: now_unix_ms(),
                    ticker: opportunity.ticker.clone(),
                    kind: opportunity.kind,
                    from_symbol: opportunity.from_symbol.clone(),
                    to_symbol: opportunity.to_symbol.clone(),
                    input_atoms,
                    estimated_input_usd,
                    expected_net_profit_usd: opportunity.net_profit_usd,
                    expected_net_profit_bps: opportunity.net_profit_bps,
                    submitted: report.submitted,
                    simulation_ok: report.simulation_ok,
                    signature: report.signature.clone(),
                    solscan_url: report.solscan_url.clone(),
                    leg_signatures: report.signature.into_iter().collect(),
                    leg_routes: opportunity
                        .execution_legs
                        .iter()
                        .map(|leg| leg.route.clone())
                        .collect(),
                    reason,
                }
            }
            Err(error) => failed_rwa_execution_record(
                id,
                cycle,
                opportunity,
                input_atoms,
                estimated_input_usd,
                format!("{error:#}"),
            ),
        }
    }

    async fn execute_rwa_sequential_route(
        &self,
        id: u64,
        cycle: u64,
        opportunity: &RwaOpportunity,
        sol_usd_estimate: f64,
    ) -> RwaExecutionRecord {
        let input_atoms = opportunity.input_atoms.unwrap_or_default();
        let estimated_input_usd = opportunity.input_estimated_usd.unwrap_or_default();
        let wallet = match read_keypair_file(&self.config.wallet.path) {
            Ok(wallet) => wallet,
            Err(error) => {
                return failed_rwa_execution_record(
                    id,
                    cycle,
                    opportunity,
                    input_atoms,
                    estimated_input_usd,
                    format!(
                        "failed to read wallet keypair {}: {error}",
                        self.config.wallet.path
                    ),
                );
            }
        };
        let wallet_pubkey = wallet.pubkey().to_string();
        let preflight_executor = self.rwa_quote_executor(sol_usd_estimate);
        let executor = self.rwa_live_executor(sol_usd_estimate);
        if should_preflight_rwa_roundtrip(opportunity) {
            if let Err(reason) = self
                .preflight_rwa_quote_return_route(&preflight_executor, opportunity)
                .await
            {
                return failed_rwa_execution_record(
                    id,
                    cycle,
                    opportunity,
                    input_atoms,
                    estimated_input_usd,
                    reason,
                );
            }
        }
        let mut leg_signatures = Vec::new();
        let mut leg_routes = Vec::new();
        let mut simulation_ok = true;
        let mut next_input_atoms: Option<u128> = None;
        let mut last_signature = None;
        let mut last_solscan_url = None;

        for (index, leg) in opportunity.execution_legs.iter().enumerate() {
            let planned_input = next_input_atoms.unwrap_or(leg.input_atoms);
            let leg_input = if index == 0 {
                planned_input
            } else {
                planned_input.min(leg.input_atoms).max(1)
            };
            let amount_atoms = match u64::try_from(leg_input) {
                Ok(value) if value > 0 => value,
                Ok(_) => {
                    return failed_rwa_execution_record(
                        id,
                        cycle,
                        opportunity,
                        input_atoms,
                        estimated_input_usd,
                        format!("RWA leg {} input amount is zero", index + 1),
                    );
                }
                Err(error) => {
                    return failed_rwa_execution_record(
                        id,
                        cycle,
                        opportunity,
                        input_atoms,
                        estimated_input_usd,
                        format!(
                            "RWA leg {} input amount does not fit into u64: {error}",
                            index + 1
                        ),
                    );
                }
            };
            let estimated_leg_usd = leg.estimated_input_usd.map(|estimated| {
                if leg.input_atoms > 0 {
                    estimated * leg_input as f64 / leg.input_atoms as f64
                } else {
                    estimated
                }
            });
            let max_usd = if self.config.execution.max_live_trade_usd > 0.0 {
                self.config.execution.max_live_trade_usd
            } else {
                let estimated = estimated_leg_usd.unwrap_or(estimated_input_usd);
                (estimated * 1.25).max(estimated + 1.0)
            };
            let output_before = match self
                .wallet_token_balance_atoms(&wallet_pubkey, &leg.output_mint)
                .await
            {
                Ok(balance) => balance,
                Err(error) => {
                    return failed_rwa_execution_record(
                        id,
                        cycle,
                        opportunity,
                        input_atoms,
                        estimated_input_usd,
                        format!(
                            "RWA leg {} output balance check failed: {error:#}",
                            index + 1
                        ),
                    );
                }
            };
            let report = match execute_rwa_leg(
                &executor,
                opportunity,
                leg,
                amount_atoms,
                max_usd,
                estimated_leg_usd,
                true,
                self.config.jupiter.slippage_bps,
            )
            .await
            {
                Ok(report) => report,
                Err(error) => {
                    return failed_rwa_execution_record(
                        id,
                        cycle,
                        opportunity,
                        input_atoms,
                        estimated_input_usd,
                        format!("RWA leg {} execution failed: {error:#}", index + 1),
                    );
                }
            };
            simulation_ok &= report.simulation_ok;
            leg_routes.push(leg.route.clone());
            if let Some(signature) = report.signature.clone() {
                last_signature = Some(signature.clone());
                leg_signatures.push(signature);
            }
            if let Some(solscan_url) = report.solscan_url.clone() {
                last_solscan_url = Some(solscan_url);
            }
            if !live_swap_report_success(&report) {
                return RwaExecutionRecord {
                    id,
                    cycle,
                    submitted_unix_ms: now_unix_ms(),
                    ticker: opportunity.ticker.clone(),
                    kind: opportunity.kind,
                    from_symbol: opportunity.from_symbol.clone(),
                    to_symbol: opportunity.to_symbol.clone(),
                    input_atoms,
                    estimated_input_usd,
                    expected_net_profit_usd: opportunity.net_profit_usd,
                    expected_net_profit_bps: opportunity.net_profit_bps,
                    submitted: !leg_signatures.is_empty(),
                    simulation_ok,
                    signature: last_signature,
                    solscan_url: last_solscan_url,
                    leg_signatures,
                    leg_routes,
                    reason: report
                        .blocked_reason
                        .clone()
                        .or(report.confirmation_error.clone())
                        .or_else(|| {
                            Some(format!(
                                "RWA leg {} confirmation status {}",
                                index + 1,
                                report
                                    .confirmation_status
                                    .as_deref()
                                    .unwrap_or("not returned")
                            ))
                        }),
                };
            }

            let output_after = match self
                .wallet_token_balance_atoms(&wallet_pubkey, &leg.output_mint)
                .await
            {
                Ok(balance) => balance,
                Err(error) => {
                    return RwaExecutionRecord {
                        id,
                        cycle,
                        submitted_unix_ms: now_unix_ms(),
                        ticker: opportunity.ticker.clone(),
                        kind: opportunity.kind,
                        from_symbol: opportunity.from_symbol.clone(),
                        to_symbol: opportunity.to_symbol.clone(),
                        input_atoms,
                        estimated_input_usd,
                        expected_net_profit_usd: opportunity.net_profit_usd,
                        expected_net_profit_bps: opportunity.net_profit_bps,
                        submitted: !leg_signatures.is_empty(),
                        simulation_ok,
                        signature: last_signature,
                        solscan_url: last_solscan_url,
                        leg_signatures,
                        leg_routes,
                        reason: Some(format!(
                            "RWA leg {} post-swap balance check failed: {error:#}",
                            index + 1
                        )),
                    };
                }
            };
            let output_delta = output_after.checked_sub(output_before).unwrap_or_default();
            if index + 1 < opportunity.execution_legs.len() && output_delta == 0 {
                return RwaExecutionRecord {
                    id,
                    cycle,
                    submitted_unix_ms: now_unix_ms(),
                    ticker: opportunity.ticker.clone(),
                    kind: opportunity.kind,
                    from_symbol: opportunity.from_symbol.clone(),
                    to_symbol: opportunity.to_symbol.clone(),
                    input_atoms,
                    estimated_input_usd,
                    expected_net_profit_usd: opportunity.net_profit_usd,
                    expected_net_profit_bps: opportunity.net_profit_bps,
                    submitted: !leg_signatures.is_empty(),
                    simulation_ok,
                    signature: last_signature,
                    solscan_url: last_solscan_url,
                    leg_signatures,
                    leg_routes,
                    reason: Some(format!(
                        "RWA leg {} produced no new output atoms for the next leg",
                        index + 1
                    )),
                };
            }
            next_input_atoms = Some(output_delta);
        }

        RwaExecutionRecord {
            id,
            cycle,
            submitted_unix_ms: now_unix_ms(),
            ticker: opportunity.ticker.clone(),
            kind: opportunity.kind,
            from_symbol: opportunity.from_symbol.clone(),
            to_symbol: opportunity.to_symbol.clone(),
            input_atoms,
            estimated_input_usd,
            expected_net_profit_usd: opportunity.net_profit_usd,
            expected_net_profit_bps: opportunity.net_profit_bps,
            submitted: !leg_signatures.is_empty(),
            simulation_ok,
            signature: last_signature,
            solscan_url: last_solscan_url,
            leg_signatures,
            leg_routes,
            reason: None,
        }
    }

    async fn execute_rwa_atomic_swap_instruction_route(
        &self,
        id: u64,
        cycle: u64,
        opportunity: &RwaOpportunity,
        sol_usd_estimate: f64,
    ) -> RwaExecutionRecord {
        let input_atoms = opportunity.input_atoms.unwrap_or_default();
        let estimated_input_usd = opportunity.input_estimated_usd.unwrap_or_default();
        let amount_atoms = match u64::try_from(input_atoms) {
            Ok(value) if value > 0 => value,
            Ok(_) => {
                return failed_rwa_execution_record(
                    id,
                    cycle,
                    opportunity,
                    input_atoms,
                    estimated_input_usd,
                    "RWA atomic route input amount is zero".to_string(),
                );
            }
            Err(error) => {
                return failed_rwa_execution_record(
                    id,
                    cycle,
                    opportunity,
                    input_atoms,
                    estimated_input_usd,
                    format!("RWA atomic route input amount does not fit into u64: {error}"),
                );
            }
        };
        let max_usd = if self.config.execution.max_live_trade_usd > 0.0 {
            self.config.execution.max_live_trade_usd
        } else {
            (estimated_input_usd * 1.25).max(estimated_input_usd + 1.0)
        };
        let request = JupiterSwapInstructionChainRequest {
            input_mint: opportunity.from_mint.clone(),
            output_mint: opportunity.to_mint.clone(),
            amount_atoms,
            legs: opportunity
                .execution_legs
                .iter()
                .map(|leg| JupiterSwapInstructionChainLeg {
                    input_mint: leg.input_mint.clone(),
                    output_mint: leg.output_mint.clone(),
                    required_route_label_contains: legacy_jupiter_route_label(&leg.route),
                })
                .collect(),
            slippage_bps: self.config.jupiter.slippage_bps,
            submit: true,
            max_usd,
            estimated_input_usd_override: Some(estimated_input_usd),
            min_output_atoms: rwa_roundtrip_min_output_atoms(&self.config, opportunity),
        };
        let executor = self.rwa_live_executor(sol_usd_estimate);
        match executor.run_swap_instruction_chain(request).await {
            Ok(report) => {
                let confirmed = live_swap_report_success(&report);
                let reason = report
                    .blocked_reason
                    .clone()
                    .or(report.confirmation_error.clone())
                    .or_else(|| {
                        (!confirmed).then(|| {
                            format!(
                                "RWA atomic route confirmation status {}",
                                report
                                    .confirmation_status
                                    .as_deref()
                                    .unwrap_or("not returned")
                            )
                        })
                    });
                RwaExecutionRecord {
                    id,
                    cycle,
                    submitted_unix_ms: now_unix_ms(),
                    ticker: opportunity.ticker.clone(),
                    kind: opportunity.kind,
                    from_symbol: opportunity.from_symbol.clone(),
                    to_symbol: opportunity.to_symbol.clone(),
                    input_atoms,
                    estimated_input_usd,
                    expected_net_profit_usd: opportunity.net_profit_usd,
                    expected_net_profit_bps: opportunity.net_profit_bps,
                    submitted: report.submitted,
                    simulation_ok: report.simulation_ok,
                    signature: report.signature.clone(),
                    solscan_url: report.solscan_url.clone(),
                    leg_signatures: report.signature.into_iter().collect(),
                    leg_routes: opportunity
                        .execution_legs
                        .iter()
                        .map(|leg| leg.route.clone())
                        .collect(),
                    reason,
                }
            }
            Err(error) => failed_rwa_execution_record(
                id,
                cycle,
                opportunity,
                input_atoms,
                estimated_input_usd,
                format!("RWA atomic route execution failed: {error:#}"),
            ),
        }
    }

    async fn preflight_rwa_quote_return_route(
        &self,
        executor: &JupiterLiveSwapExecutor,
        opportunity: &RwaOpportunity,
    ) -> std::result::Result<(), String> {
        let legs = &opportunity.execution_legs;
        if legs.len() < 2 {
            return Err("RWA quote-return route requires at least two executable legs".to_string());
        }
        let first_leg = legs.first().expect("checked non-empty legs");
        let last_leg = legs.last().expect("checked non-empty legs");
        if first_leg.input_mint != last_leg.output_mint {
            return Err("RWA quote-return input/output mints do not match".to_string());
        }
        if first_leg.input_decimals != last_leg.output_decimals {
            return Err("RWA quote-return input/output decimals do not match".to_string());
        }
        let input_atoms = opportunity.input_atoms.unwrap_or(legs[0].input_atoms);
        let mut next_amount = u64::try_from(input_atoms)
            .map_err(|error| format!("RWA quote-return input does not fit into u64: {error}"))?;
        let input_usd = opportunity.input_estimated_usd.unwrap_or_default();
        let mut final_out = None;
        for (index, leg) in legs.iter().enumerate() {
            let leg_input = if index == 0 {
                next_amount as u128
            } else {
                (next_amount as u128).min(leg.input_atoms).max(1)
            };
            let amount = u64::try_from(leg_input).map_err(|error| {
                format!(
                    "RWA quote-return leg {} input does not fit into u64: {error}",
                    index + 1
                )
            })?;
            let estimated_usd = leg.estimated_input_usd.unwrap_or(input_usd);
            let report = self
                .preflight_rwa_leg(executor, opportunity, leg, amount, estimated_usd)
                .await?;
            let out = report
                .quote_out_amount
                .as_deref()
                .and_then(|value| value.parse::<u128>().ok())
                .ok_or_else(|| {
                    format!(
                        "RWA quote-return leg {} live re-quote missing outAmount",
                        index + 1
                    )
                })?;
            final_out = Some(out);
            if index + 1 < legs.len() {
                next_amount = u64::try_from(out).map_err(|error| {
                    format!(
                        "RWA quote-return leg {} output does not fit into u64 for next leg: {error}",
                        index + 1
                    )
                })?;
            }
        }
        let final_out = final_out.ok_or_else(|| {
            "RWA quote-return route did not produce a final live re-quote".to_string()
        })?;
        let profit_atoms = final_out as i128 - input_atoms as i128;
        let profit_bps = if input_atoms > 0 {
            profit_atoms as f64 / input_atoms as f64 * 10_000.0
        } else {
            0.0
        };
        let min_bps = self
            .config
            .rwa
            .min_net_profit_bps
            .max(self.config.trading.min_net_profit_bps)
            .max(self.config.execution.min_requote_profit_bps);
        if profit_bps < min_bps {
            return Err(format!(
                "RWA quote-return live re-quote profit {profit_bps:.2} bps is below required {min_bps:.2} bps"
            ));
        }
        Ok(())
    }

    async fn preflight_rwa_leg(
        &self,
        executor: &JupiterLiveSwapExecutor,
        opportunity: &RwaOpportunity,
        leg: &crate::rwa::RwaExecutionLeg,
        amount_atoms: u64,
        estimated_input_usd: f64,
    ) -> std::result::Result<LiveSwapReport, String> {
        let max_usd = if self.config.execution.max_live_trade_usd > 0.0 {
            self.config.execution.max_live_trade_usd
        } else {
            (estimated_input_usd * 1.25).max(estimated_input_usd + 1.0)
        };
        execute_rwa_leg(
            executor,
            opportunity,
            leg,
            amount_atoms,
            max_usd,
            Some(estimated_input_usd),
            false,
            self.config.jupiter.slippage_bps,
        )
        .await
        .map_err(|error| format!("RWA route live re-quote failed: {error:#}"))
    }

    async fn wallet_token_balance_atoms(&self, wallet_pubkey: &str, mint: &str) -> Result<u128> {
        let result = self
            .rpc
            .call(
                "getTokenAccountsByOwner",
                json!([
                    wallet_pubkey,
                    { "mint": mint },
                    {
                        "encoding": "jsonParsed",
                        "commitment": "processed"
                    }
                ]),
            )
            .await
            .with_context(|| format!("failed to fetch wallet token accounts for {mint}"))?;
        Ok(result
            .get("value")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|account| token_account_amount_atoms(account).map(|(amount, _)| amount))
            .sum::<u128>())
    }

    async fn rwa_settlement_quote_balance_atoms(
        &self,
        wallet_pubkey: &str,
        quote_mint: &str,
        sol_lamports: u64,
    ) -> Result<u128> {
        if quote_mint == WSOL_MINT {
            return Ok(settlement_quote_balance_atoms(quote_mint, 0, sol_lamports));
        }
        self.wallet_token_balance_atoms(wallet_pubkey, quote_mint)
            .await
            .map(|token_balance| {
                settlement_quote_balance_atoms(quote_mint, token_balance, sol_lamports)
            })
    }

    fn rwa_readiness_blockers(
        &self,
        rwa: &RwaSnapshot,
        quote_funding_available_usd: f64,
    ) -> Vec<String> {
        let mut blockers = Vec::new();
        if rwa.oracle_prices == 0 {
            blockers.push("no valid Pyth equity price for any watched RWA asset".to_string());
        }
        for asset in &rwa.assets {
            match asset.oracle.as_ref() {
                Some(oracle) if oracle.status == crate::rwa::OracleStatus::MarketClosed => {
                    blockers.push(format!(
                        "{} US equity session closed for Pyth feed ({})",
                        asset.ticker, oracle.market_session
                    ));
                }
                Some(oracle) if oracle.status != crate::rwa::OracleStatus::Valid => {
                    blockers.push(format!(
                        "{} Pyth status is {:?} ({})",
                        asset.ticker, oracle.status, oracle.market_session
                    ));
                }
                None => blockers.push(format!("{} Pyth price missing", asset.ticker)),
                _ => {}
            }
        }

        let trade_usd = configured_rwa_min_trade_usd(self.config.rwa.max_trade_usd_per_asset);
        if trade_usd > 0.0 && quote_funding_available_usd + f64::EPSILON < trade_usd {
            blockers.push(format!(
                "quote funding balance ${quote_funding_available_usd:.6} is below smallest configured RWA trade size ${trade_usd:.2}"
            ));
        }

        for provider in &self.config.aggregators.providers {
            if !self.config.aggregators.enabled || !provider.enabled {
                continue;
            }
            let missing = missing_provider_auth_envs(provider);
            if !missing.is_empty() {
                blockers.push(format!("{} missing {}", provider.name, missing.join(", ")));
            }
        }

        if self.config.rwa.ondo_primary_enabled
            && env_value("ONDO_PRIMARY_BUILD_URL").is_none()
            && !rwa.assets.iter().any(|asset| {
                asset.primary_routes.iter().any(|route| {
                    route.provider == "ondo_jupiter_rfq"
                        && (route.rfq_mint_available || route.rfq_burn_available)
                })
            })
        {
            if primary_routes_all_waiting_for_oracle(rwa, "ondo_jupiter_rfq") {
                blockers.push(
                    "Ondo Jupiter v2 RFQ primary mint/burn is configured; waiting for valid open-session Pyth before RFQ legs are requested and scored"
                        .to_string(),
                );
            } else {
                blockers.push(
                    "ONDO_PRIMARY_BUILD_URL unset and no live Jupiter v2 RFQ primary leg is currently available; Ondo primary mint/burn execution is unavailable this cycle"
                        .to_string(),
                );
            }
        }
        blockers.sort();
        blockers.dedup();
        blockers
    }

    async fn execution_paths(
        &self,
        pools: &[PoolState],
        quotes: &[Quote],
    ) -> Vec<ExecutionPathSnapshot> {
        let mut paths = Vec::new();
        let provider_snapshots = self.aggregators.provider_snapshots().await;
        let provider_snapshots_by_name = provider_snapshots
            .into_iter()
            .map(|snapshot| (snapshot.name.clone(), snapshot))
            .collect::<HashMap<_, _>>();
        let jupiter_active = self.config.trading.live && self.config.execution.enabled;
        let has_jupiter_key = env_value("JUPITER_API_KEY").is_some();
        paths.push(ExecutionPathSnapshot {
            name: "jupiter_swap_instructions".to_string(),
            kind: "router_execution".to_string(),
            quote_source: "Jupiter quote API".to_string(),
            swap_source: "Jupiter swap-instructions API".to_string(),
            enabled: jupiter_active,
            active: jupiter_active,
            readiness: if jupiter_active {
                ExecutionPathReadiness::Active
            } else {
                ExecutionPathReadiness::Disabled
            },
            requires_api_key: false,
            has_api_key: has_jupiter_key,
            decoded_pools: 0,
            reserve_backed_pools: 0,
            quote_count: quotes
                .iter()
                .filter(|quote| quote.venue == Venue::Jupiter)
                .count(),
            status: if jupiter_active {
                "active executor; Orca/Raydium/Meteora route labels are Jupiter dex filters"
                    .to_string()
            } else {
                "inactive; live execution is disabled".to_string()
            },
        });
        let jupiter_v2_enabled =
            env_boolish("JUPITER_V2_EXECUTION_ENABLED") || env_boolish("RWA_JUPITER_V2_ENABLED");
        let jupiter_v2_active = self.config.trading.live
            && self.config.rwa.execution_enabled
            && jupiter_v2_enabled
            && has_jupiter_key;
        let jupiter_v2_readiness = if !jupiter_v2_enabled {
            ExecutionPathReadiness::Disabled
        } else if !has_jupiter_key {
            ExecutionPathReadiness::AuthMissing
        } else if jupiter_v2_active {
            ExecutionPathReadiness::Active
        } else {
            ExecutionPathReadiness::Gated
        };
        paths.push(ExecutionPathSnapshot {
            name: "jupiter_v2_order_rfq".to_string(),
            kind: "router_execution".to_string(),
            quote_source: env_value("JUPITER_V2_ORDER_URL")
                .unwrap_or_else(|| "https://api.jup.ag/swap/v2/order".to_string()),
            swap_source: env_value("JUPITER_V2_EXECUTE_URL")
                .unwrap_or_else(|| "https://api.jup.ag/swap/v2/execute".to_string()),
            enabled: jupiter_v2_enabled,
            active: jupiter_v2_active,
            readiness: jupiter_v2_readiness,
            requires_api_key: true,
            has_api_key: has_jupiter_key,
            decoded_pools: 0,
            reserve_backed_pools: 0,
            quote_count: quotes
                .iter()
                .filter(|quote| {
                    quote.venue == Venue::Jupiter && quote.pool.starts_with("jupiter-v2-order:")
                })
                .count(),
            status: if !jupiter_v2_enabled {
                "provider disabled".to_string()
            } else if !has_jupiter_key {
                "Jupiter API key required for v2 order/RFQ execution".to_string()
            } else if jupiter_v2_active {
                "active Jupiter v2 order/execute path; includes JupiterZ RFQ, Metis, DFlow, OKX, and other routers returned by Jupiter".to_string()
            } else {
                "configured, but RWA live execution is not active".to_string()
            },
        });

        for (name, venue) in [
            ("orca_direct", Venue::Orca),
            ("meteora_direct", Venue::Meteora),
            ("raydium_direct", Venue::Raydium),
        ] {
            let enabled = self
                .config
                .venues
                .enabled
                .iter()
                .any(|configured| live_probe_venue_from_name(configured) == Some(venue));
            let decoded_pools = pools.iter().filter(|pool| pool.venue == venue).count();
            let reserve_backed_pools = pools
                .iter()
                .filter(|pool| {
                    pool.venue == venue
                        && pool.reserve_base_atoms > 0
                        && pool.reserve_quote_atoms > 0
                })
                .count();
            let quote_count = quotes.iter().filter(|quote| quote.venue == venue).count();
            let readiness = if !enabled {
                ExecutionPathReadiness::Disabled
            } else if decoded_pools == 0 {
                ExecutionPathReadiness::WaitingForPools
            } else {
                ExecutionPathReadiness::Active
            };
            let status = if !enabled {
                "disabled in venues.enabled".to_string()
            } else if decoded_pools == 0 {
                "native swap builder wired; no promoted decoded pools".to_string()
            } else if reserve_backed_pools == 0 {
                "native swap builder wired; price-only pools are quote signals only and are excluded from live native submission"
                    .to_string()
            } else {
                "native swap builder wired; direct pool candidates execute without Jupiter"
                    .to_string()
            };
            paths.push(ExecutionPathSnapshot {
                name: name.to_string(),
                kind: "native_venue".to_string(),
                quote_source: "local decoded account state".to_string(),
                swap_source: "native swaps crate instruction builder".to_string(),
                enabled,
                active: enabled && decoded_pools > 0,
                readiness,
                requires_api_key: false,
                has_api_key: false,
                decoded_pools,
                reserve_backed_pools,
                quote_count,
                status,
            });
        }

        for provider in &self.config.aggregators.providers {
            let has_api_key = provider_has_required_auth(provider);
            let missing_auth_envs = missing_provider_auth_envs(provider);
            let runtime = provider_snapshots_by_name.get(&provider.name);
            let cooldown_remaining_ms = runtime.and_then(|snapshot| snapshot.cooldown_remaining_ms);
            let cooldown_reason = runtime
                .and_then(|snapshot| snapshot.cooldown_reason.as_deref())
                .unwrap_or("persisted provider cooldown");
            let next_request_in_ms = runtime.and_then(|snapshot| snapshot.next_request_in_ms);
            let active = self.config.aggregators.enabled
                && provider.enabled
                && (!provider.requires_api_key || has_api_key);
            let readiness = aggregator_path_readiness(
                self.config.aggregators.enabled,
                provider.enabled,
                provider.requires_api_key,
                has_api_key,
                cooldown_remaining_ms,
                next_request_in_ms,
            );
            let base_status = aggregator_path_status(
                &provider.name,
                self.config.aggregators.enabled,
                provider.enabled,
                provider.requires_api_key,
                has_api_key,
                &missing_auth_envs,
            );
            let status = if let Some(ms) = cooldown_remaining_ms {
                format!("{base_status}; provider cooling down for {ms}ms after {cooldown_reason}")
            } else if let Some(ms) = next_request_in_ms {
                format!("{base_status}; paced next request in {ms}ms")
            } else {
                base_status
            };
            paths.push(ExecutionPathSnapshot {
                name: provider.name.clone(),
                kind: "router_or_api".to_string(),
                quote_source: provider.quote_url.clone(),
                swap_source: aggregator_swap_source(&provider.name).to_string(),
                enabled: self.config.aggregators.enabled && provider.enabled,
                active: active && cooldown_remaining_ms.is_none(),
                readiness,
                requires_api_key: provider.requires_api_key,
                has_api_key,
                decoded_pools: 0,
                reserve_backed_pools: 0,
                quote_count: quotes
                    .iter()
                    .filter(|quote| quote.venue == provider.venue)
                    .count(),
                status,
            });
        }

        paths
    }

    async fn select_live_candidate_indices(
        &self,
        candidates: &[Opportunity],
        limit: usize,
        start_offset: usize,
        sol_usd_estimate: f64,
    ) -> CandidateSelection {
        if candidates.is_empty() || limit == 0 {
            return CandidateSelection {
                indices: Vec::new(),
                cooldown_count: 0,
                selected_candidates: Vec::new(),
            };
        }

        let stats = self.candidate_probe_stats.read().await;
        let route_cooldowns = self.jupiter_no_route_leg_cooldowns.read().await;
        let now = Instant::now();
        let mut cooldown_count = 0usize;
        let mut ranked = Vec::new();
        for (ordinal, index) in
            rotated_candidate_indices(candidates.len(), candidates.len(), start_offset)
                .into_iter()
                .enumerate()
        {
            let candidate = &candidates[index];
            let key = live_candidate_key(candidate);
            let candidate_stats = stats.get(&key);
            if candidate_stats
                .and_then(|stats| stats.cooldown_until)
                .is_some_and(|until| until > now)
            {
                cooldown_count = cooldown_count.saturating_add(1);
                continue;
            }
            if structural_route_cooldown_until(candidate, &route_cooldowns)
                .or_else(|| jupiter_route_leg_cooldown_until(candidate, &route_cooldowns))
                .is_some_and(|until| until > now)
            {
                cooldown_count = cooldown_count.saturating_add(1);
                continue;
            }
            ranked.push((
                index,
                probe_candidate_priority(candidate_stats) + direct_signal_priority(candidate),
                ordinal,
                key,
            ));
        }

        ranked.sort_by(|left, right| {
            right
                .1
                .partial_cmp(&left.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.2.cmp(&right.2))
        });

        let selected = diverse_live_probe_selection(ranked, candidates, limit);
        let selected_candidates = selected
            .iter()
            .map(|(index, _, _, key)| {
                probe_snapshot_from_opportunity(
                    &candidates[*index],
                    key.clone(),
                    stats.get(key),
                    now,
                    sol_usd_estimate,
                )
            })
            .collect::<Vec<_>>();

        CandidateSelection {
            indices: selected
                .into_iter()
                .map(|(index, _, _, _)| index)
                .collect::<Vec<_>>(),
            cooldown_count,
            selected_candidates,
        }
    }

    async fn record_live_probe_report(
        &self,
        opportunity: &Opportunity,
        report: &AtomicArbReport,
        cycle: u64,
        id: u64,
        sol_usd_estimate: f64,
    ) {
        if let Some(db) = &self.database {
            if let Ok(db) = db.lock() {
                let attempt_id = runtime_attempt_id("live", cycle, id);
                let config_hash = runtime_config_hash(self.config.as_ref());
                if let Err(error) = persist_live_probe_evidence(
                    &db,
                    cycle,
                    opportunity,
                    Some(report),
                    None,
                    &attempt_id,
                    &config_hash,
                    now_unix_ms(),
                ) {
                    warn!(?error, "failed to persist live probe evidence");
                }
            }
        }
        let reason = report
            .blocked_reason
            .clone()
            .or(report.confirmation_error.clone());
        self.record_live_probe_outcome(
            opportunity,
            report.requote_net_profit_bps,
            reason,
            matches!(report.status, AtomicArbStatus::Blocked),
            matches!(report.status, AtomicArbStatus::Failed),
            sol_usd_estimate,
        )
        .await;
    }

    async fn record_live_probe_error(
        &self,
        opportunity: &Opportunity,
        error: &str,
        cycle: u64,
        id: u64,
        sol_usd_estimate: f64,
    ) {
        if let Some(db) = &self.database {
            if let Ok(db) = db.lock() {
                let attempt_id = runtime_attempt_id("live", cycle, id);
                let config_hash = runtime_config_hash(self.config.as_ref());
                if let Err(error) = persist_live_probe_evidence(
                    &db,
                    cycle,
                    opportunity,
                    None,
                    Some(error),
                    &attempt_id,
                    &config_hash,
                    now_unix_ms(),
                ) {
                    warn!(?error, "failed to persist live probe error evidence");
                }
            }
        }
        self.record_live_probe_outcome(
            opportunity,
            None,
            Some(error.to_string()),
            false,
            true,
            sol_usd_estimate,
        )
        .await;
    }

    async fn record_live_probe_outcome(
        &self,
        opportunity: &Opportunity,
        requote_bps: Option<f64>,
        reason: Option<String>,
        blocked: bool,
        failed: bool,
        sol_usd_estimate: f64,
    ) {
        let key = live_candidate_key(opportunity);
        let info = candidate_probe_info(opportunity, key.clone(), sol_usd_estimate);
        let mut stats_map = self.candidate_probe_stats.write().await;
        let stats = stats_map.entry(key).or_default();
        stats.info = Some(info);
        stats.attempts = stats.attempts.saturating_add(1);
        if blocked {
            stats.blocks = stats.blocks.saturating_add(1);
        }
        if reason.as_deref().is_some_and(is_jupiter_no_route_error) {
            stats.no_route_blocks = stats.no_route_blocks.saturating_add(1);
        }
        stats.last_requote_bps = requote_bps;
        if let Some(bps) = requote_bps {
            stats.best_requote_bps = Some(
                stats
                    .best_requote_bps
                    .map(|best| best.max(bps))
                    .unwrap_or(bps),
            );
        }
        stats.last_reason = reason.clone();
        stats.last_checked_unix_ms = Some(now_unix_ms());
        let cooldown = probe_cooldown_duration(reason.as_deref(), requote_bps, failed);
        stats.cooldown_until = cooldown.and_then(|duration| {
            if duration.is_zero() {
                None
            } else {
                Some(Instant::now() + duration)
            }
        });

        if stats_map.len() > 5_000 {
            let cutoff = now_unix_ms().saturating_sub(60 * 60 * 1_000);
            stats_map.retain(|_, stats| stats.last_checked_unix_ms.unwrap_or(0) >= cutoff);
        }
        drop(stats_map);

        let structural_cooldown_until = reason
            .as_deref()
            .and_then(structural_route_cooldown_duration)
            .map(|duration| Instant::now() + duration);
        let structural_cooldown_keys = structural_cooldown_until
            .map(|_| {
                reason
                    .as_deref()
                    .map(|reason| structural_route_cooldown_keys_for_reason(opportunity, reason))
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        let leg_cooldown_keys = reason
            .as_deref()
            .map(|reason| jupiter_no_route_leg_keys_for_reason(opportunity, reason))
            .unwrap_or_default();
        if !leg_cooldown_keys.is_empty() || !structural_cooldown_keys.is_empty() {
            let mut route_cooldowns = self.jupiter_no_route_leg_cooldowns.write().await;
            if let Some(until) = structural_cooldown_until {
                for key in structural_cooldown_keys {
                    route_cooldowns.insert(key, until);
                }
            }
            let until = Instant::now() + jupiter_no_route_leg_cooldown_duration();
            for key in leg_cooldown_keys {
                route_cooldowns.insert(key, until);
            }
            if route_cooldowns.len() > 5_000 {
                let now = Instant::now();
                route_cooldowns.retain(|_, until| *until > now);
            }
            persist_route_cooldown_state(&route_cooldowns);
        }
    }

    async fn record_rwa_route_cooldown(&self, opportunity: &RwaOpportunity, reason: &str) {
        let Some(duration) = rwa_route_leg_cooldown_duration_for_reason(reason) else {
            return;
        };
        let leg_cooldown_keys = rwa_route_leg_cooldown_keys_for_reason(opportunity, reason);
        if leg_cooldown_keys.is_empty() {
            return;
        }
        let until = Instant::now() + duration;
        let mut route_cooldowns = self.jupiter_no_route_leg_cooldowns.write().await;
        for key in leg_cooldown_keys {
            route_cooldowns.insert(key, until);
        }
        if route_cooldowns.len() > 5_000 {
            let now = Instant::now();
            route_cooldowns.retain(|_, until| *until > now);
        }
        persist_route_cooldown_state(&route_cooldowns);
    }

    async fn best_recent_candidate_snapshots(&self, limit: usize) -> Vec<ProbeCandidateSnapshot> {
        let stats = self.candidate_probe_stats.read().await;
        let now = Instant::now();
        let mut snapshots = stats
            .iter()
            .filter_map(|(key, stats)| {
                let info = stats.info.as_ref()?;
                Some(probe_snapshot_from_info(info, key.clone(), stats, now))
            })
            .collect::<Vec<_>>();
        snapshots.sort_by(|left, right| {
            let left_bps = left
                .best_requote_bps
                .or(left.last_requote_bps)
                .unwrap_or(f64::NEG_INFINITY);
            let right_bps = right
                .best_requote_bps
                .or(right.last_requote_bps)
                .unwrap_or(f64::NEG_INFINITY);
            right_bps
                .partial_cmp(&left_bps)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| right.last_checked_unix_ms.cmp(&left.last_checked_unix_ms))
        });
        snapshots.truncate(limit);
        snapshots
    }

    async fn simulation_snapshot(
        &self,
        opportunities: &[Opportunity],
        cycle: u64,
    ) -> SimulationSnapshot {
        let mut snapshot = self.state.read().await.simulation.clone();
        snapshot.enabled = self.config.simulation.enabled;
        snapshot.next_simulation_in_ms = self.simulation_next_attempt_in_ms().await;

        if !self.config.simulation.enabled {
            snapshot = SimulationSnapshot {
                enabled: false,
                reason: "paper-trade simulation is disabled; live execution only".to_string(),
                ..SimulationSnapshot::default()
            };
            return snapshot;
        }

        if opportunities.is_empty() {
            snapshot.reason = "no candidate opportunities to simulate".to_string();
            return snapshot;
        }

        let mut last_attempt = self.last_simulation_attempt.lock().await;
        let now = Instant::now();
        if let Some(last) = *last_attempt {
            let elapsed = now.duration_since(last);
            let interval = Duration::from_millis(self.config.simulation.min_interval_ms);
            if elapsed < interval {
                snapshot.next_simulation_in_ms = Some(
                    (interval - elapsed)
                        .as_millis()
                        .try_into()
                        .unwrap_or(u64::MAX),
                );
                snapshot.reason = "waiting for next paper-trade interval".to_string();
                return snapshot;
            }
        }

        let now_ms = now_unix_ms();
        let trade_count = self.config.simulation.max_trades_per_cycle.max(1);
        for opportunity in opportunities.iter().take(trade_count) {
            let id = self.simulation_sequence.fetch_add(1, Ordering::Relaxed);
            let trade = simulated_trade_from_opportunity(id, cycle, now_ms, opportunity);
            if let Some(db) = &self.database {
                if let Ok(db) = db.lock() {
                    let attempt_id = runtime_attempt_id("sim", cycle, id);
                    if let Err(error) =
                        persist_simulation_evidence(&db, cycle, opportunity, &attempt_id, now_ms)
                    {
                        warn!(?error, "failed to persist simulation evidence");
                    }
                }
            }
            snapshot.attempted_count = snapshot.attempted_count.saturating_add(1);
            if trade.profitable {
                snapshot.profitable_count = snapshot.profitable_count.saturating_add(1);
            } else {
                snapshot.unprofitable_count = snapshot.unprofitable_count.saturating_add(1);
            }
            snapshot.total_net_profit_usd += trade.simulated_net_profit_usd;
            snapshot.trades.insert(0, trade);
        }

        snapshot
            .trades
            .truncate(self.config.simulation.max_history.max(1));
        snapshot.last_simulated_unix_ms = Some(now_ms);
        snapshot.next_simulation_in_ms = Some(self.config.simulation.min_interval_ms);
        snapshot.reason = format!(
            "simulated {} paper trade(s) from current opportunity candidates",
            trade_count.min(opportunities.len())
        );
        *last_attempt = Some(now);
        snapshot
    }

    async fn simulation_next_attempt_in_ms(&self) -> Option<u64> {
        if !self.config.simulation.enabled {
            return None;
        }

        let last_attempt = self.last_simulation_attempt.lock().await;
        let last_attempt = (*last_attempt)?;
        let elapsed_ms = Instant::now().duration_since(last_attempt).as_millis() as u64;
        Some(
            self.config
                .simulation
                .min_interval_ms
                .saturating_sub(elapsed_ms),
        )
    }

    async fn live_risk_snapshot(&self) -> LiveRiskSnapshot {
        let failed_fee_loss_count = self
            .database
            .as_ref()
            .and_then(|db| db.lock().ok())
            .and_then(|db| db.summary().ok())
            .map(|summary| summary.live_settlement_fee_losses)
            .unwrap_or_default();
        live_risk_snapshot_from_config(&self.config.execution, failed_fee_loss_count)
    }

    async fn decode_indexed_candidates(
        &self,
        adapters: &[Box<dyn VenueAdapter>],
        candidates: Vec<IndexedPoolCandidate>,
    ) -> DiscoveryRun {
        if candidates.is_empty() {
            return DiscoveryRun {
                pools: Vec::new(),
                jobs: 0,
                indexed_candidates: 0,
                targeted_accounts: 0,
                errors: Vec::new(),
            };
        }

        let mut candidates_by_address = HashMap::new();
        let pubkeys = candidates
            .iter()
            .map(|candidate| {
                candidates_by_address.insert(candidate.address.clone(), candidate.clone());
                candidate.address.clone()
            })
            .collect::<Vec<_>>();
        let mut pools = Vec::new();
        let mut errors = Vec::new();

        for chunk in pubkeys.chunks(100) {
            let chunk = chunk.to_vec();
            match self.rpc.get_multiple_accounts(&chunk).await {
                Ok(accounts) => {
                    for (pubkey, account) in chunk.iter().zip(accounts.into_iter()) {
                        let Some(candidate) = candidates_by_address.get(pubkey) else {
                            continue;
                        };
                        let Some(account) = account else {
                            errors.push(DiscoveryIssue {
                                kind: DiscoveryIssueKind::MissingAccount,
                                venue: candidate.venue,
                                program_id: candidate.source.clone(),
                                error: format!("indexed pool account missing: {pubkey}"),
                            });
                            continue;
                        };
                        let Some(adapter) = adapters
                            .iter()
                            .find(|adapter| adapter.venue() == candidate.venue)
                        else {
                            continue;
                        };
                        match adapter
                            .decode_pool(&account)
                            .and_then(|pool| normalize_indexed_pool(pool, candidate))
                            .or_else(|| indexed_price_only_pool(&account, candidate))
                        {
                            Some(pool) => pools.push(pool),
                            None => errors.push(DiscoveryIssue {
                                kind: DiscoveryIssueKind::Decode,
                                venue: candidate.venue,
                                program_id: account.owner,
                                error: format!(
                                    "could not decode indexed {} pool {} for {}/{}",
                                    candidate.source,
                                    pubkey,
                                    short_mint(&candidate.base_mint),
                                    short_mint(&candidate.quote_mint)
                                ),
                            }),
                        }
                    }
                }
                Err(error) => errors.push(DiscoveryIssue {
                    kind: DiscoveryIssueKind::Rpc,
                    venue: crate::types::Venue::Custom,
                    program_id: "getMultipleAccounts".to_string(),
                    error: format!("{error:#}"),
                }),
            }
        }

        DiscoveryRun {
            pools,
            jobs: pubkeys.len(),
            indexed_candidates: candidates_by_address.len(),
            targeted_accounts: pubkeys.len(),
            errors,
        }
    }

    async fn append_focused_route_pools_from_route_graph(
        &self,
        adapters: &[Box<dyn VenueAdapter>],
        promoted: &mut Vec<PoolState>,
        sol_usd_estimate: f64,
    ) {
        if self.config.execution.native_focus_routes.is_empty() {
            return;
        }
        let Some(database) = &self.database else {
            return;
        };
        let targets = match database
            .lock()
            .map_err(|error| anyhow!("database lock poisoned: {error}"))
            .and_then(|db| db.complete_route_account_fetch_targets(512))
        {
            Ok(targets) => targets,
            Err(error) => {
                debug!(?error, "focused route pool target lookup failed");
                return;
            }
        };
        if targets.is_empty() {
            return;
        }

        let existing_addresses = promoted
            .iter()
            .map(|pool| pool.address.clone())
            .collect::<HashSet<_>>();
        let mut fetch_addresses = Vec::new();
        let mut seen = HashSet::new();
        for target in
            focused_route_pool_decode_targets(&targets, &self.config.execution.native_focus_routes)
        {
            if existing_addresses.contains(&target.address) {
                continue;
            }
            if seen.insert(target.address.clone()) {
                fetch_addresses.push(target.address);
            }
        }
        if fetch_addresses.is_empty() {
            return;
        }

        let cached_accounts = self
            .pool_subscription
            .get_cached_raw_accounts(&fetch_addresses)
            .await;
        let mut raw_by_address = HashMap::<String, RawAccount>::new();
        for account in cached_accounts.into_iter().flatten() {
            raw_by_address.insert(account.pubkey.clone(), account);
        }

        let missing = fetch_addresses
            .iter()
            .filter(|address| !raw_by_address.contains_key(*address))
            .cloned()
            .collect::<Vec<_>>();
        for chunk in missing.chunks(100) {
            let chunk = chunk.to_vec();
            match self.rpc.get_multiple_accounts(&chunk).await {
                Ok(accounts) => {
                    let warmed = accounts.iter().flatten().cloned().collect::<Vec<_>>();
                    for account in accounts.into_iter().flatten() {
                        raw_by_address.insert(account.pubkey.clone(), account);
                    }
                    if !warmed.is_empty() {
                        self.pool_subscription.warm_raw_accounts(warmed).await;
                    }
                }
                Err(error) => {
                    debug!(
                        ?error,
                        requested = chunk.len(),
                        "failed to fetch focused route pools from rpc"
                    );
                }
            }
        }

        let decoded_pools =
            focused_route_pool_decode_targets(&targets, &self.config.execution.native_focus_routes)
                .into_iter()
                .filter_map(|target| {
                    raw_by_address
                        .get(&target.address)
                        .and_then(|account| decode_focused_route_pool(adapters, account, &target))
                })
                .collect::<Vec<_>>();
        if decoded_pools.is_empty() {
            debug!(
                target_count = targets.len(),
                requested = fetch_addresses.len(),
                "no focused route pools decoded from route graph targets"
            );
            return;
        }

        let appended = append_focused_route_pools(
            promoted,
            &targets,
            decoded_pools,
            &self.config.execution.native_focus_routes,
            Some((&self.config, sol_usd_estimate)),
        );
        if appended > 0 {
            debug!(
                target_count = targets.len(),
                requested = fetch_addresses.len(),
                appended,
                "appended focused route pools to promoted pool set"
            );
        }
    }

    fn promote_pools(&self, pools: Vec<PoolState>) -> Vec<PoolState> {
        let denylist = self
            .config
            .trading
            .major_denylist
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        let quote_mints = self
            .config
            .trading
            .quote_mints
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();

        let mut by_pair: HashMap<String, Vec<PoolState>> = HashMap::new();
        for pool in pools {
            if denylist.contains(&pool.pair.base_mint) {
                continue;
            }
            if is_blocklisted_remora_mint(&pool.pair.base_mint) {
                continue;
            }
            if is_pump_like_mint(&pool.pair.base_mint) {
                continue;
            }
            if !quote_mints.contains(&pool.pair.quote_mint) {
                continue;
            }
            if pool.price_quote_per_base().is_none() {
                continue;
            }
            by_pair.entry(pool.pair.key()).or_default().push(pool);
        }

        let mut promoted = Vec::new();
        for (_pair, mut pair_pools) in by_pair {
            let venue_count = pair_pools
                .iter()
                .map(|pool| pool.venue)
                .collect::<std::collections::HashSet<_>>()
                .len();
            if venue_count < 2 {
                continue;
            }
            pair_pools.sort_by_key(|pool| pool.venue as u8);
            promoted.extend(pair_pools);
            if promoted.len() >= self.config.trading.max_active_tokens * 3 {
                break;
            }
        }
        promoted
    }

    fn compute_quotes(
        &self,
        adapters: &[Box<dyn VenueAdapter>],
        pools: &[PoolState],
        network_fee: &FeeBreakdown,
        sol_usd_estimate: f64,
    ) -> Vec<Quote> {
        let trade_size = self.config.trading.trade_size_usd;
        let safety_buffer_bps = self.config.trading.fee_safety_buffer_bps;
        pools
            .par_iter()
            .filter_map(|pool| {
                let quote_atoms = ui_to_atoms(
                    trade_size_quote_ui(&pool.pair, trade_size, sol_usd_estimate),
                    pool.pair.quote_decimals,
                );
                let adapter = adapters
                    .iter()
                    .find(|adapter| adapter.venue() == pool.venue);
                adapter.and_then(|adapter| {
                    adapter.quote(
                        pool,
                        SwapSide::QuoteToBase,
                        quote_atoms,
                        network_fee,
                        safety_buffer_bps,
                    )
                })
            })
            .collect()
    }

    fn find_opportunities(
        &self,
        pools: &[PoolState],
        network_fee: &FeeBreakdown,
        sol_usd_estimate: f64,
    ) -> Vec<Opportunity> {
        let mut by_pair: HashMap<String, Vec<&PoolState>> = HashMap::new();
        for pool in pools {
            by_pair.entry(pool.pair.key()).or_default().push(pool);
        }

        let groups = by_pair.into_values().collect::<Vec<_>>();
        let trade_sizes_usd = direct_opportunity_trade_sizes_usd(
            &self.config.execution,
            self.config.trading.trade_size_usd,
        );
        let max_trade_size_usd = trade_sizes_usd.iter().copied().fold(0.0_f64, f64::max);
        let mut opportunities = groups
            .par_iter()
            .flat_map_iter(|pair_pools| {
                let mut pair_opportunities = Vec::new();
                let mut seen_optimizer_pairs: HashSet<(String, String)> = HashSet::new();
                for buy_pool in pair_pools {
                    for sell_pool in pair_pools {
                        if buy_pool.venue == sell_pool.venue
                            || buy_pool.address == sell_pool.address
                        {
                            continue;
                        }
                        for trade_size_usd in &trade_sizes_usd {
                            if let Some(opportunity) = self.evaluate_pair_at_trade_size(
                                buy_pool,
                                sell_pool,
                                *trade_size_usd,
                                network_fee.clone(),
                                sol_usd_estimate,
                            ) {
                                pair_opportunities.push(opportunity);
                            }
                        }
                        if max_trade_size_usd > 0.0
                            && seen_optimizer_pairs
                                .insert((buy_pool.address.clone(), sell_pool.address.clone()))
                        {
                            if let Some(opportunity) = self.evaluate_pair_at_optimized_size(
                                buy_pool,
                                sell_pool,
                                None,
                                network_fee.clone(),
                                sol_usd_estimate,
                                None,
                            ) {
                                pair_opportunities.push(opportunity);
                            }
                        }
                    }
                }
                pair_opportunities
            })
            .collect::<Vec<_>>();

        opportunities.sort_by(|a, b| {
            b.net_profit_usd
                .partial_cmp(&a.net_profit_usd)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        opportunities
    }

    fn execution_candidates_for_cycle(
        &self,
        pools: &[PoolState],
        opportunities: &[Opportunity],
        execution_pairs: &[TokenPair],
        network_fee: &FeeBreakdown,
        sol_usd_estimate: f64,
        quote_balance_caps: Option<&HashMap<String, u128>>,
    ) -> (
        Vec<Opportunity>,
        String,
        Vec<FocusedRouteCandidateRejectionSnapshot>,
    ) {
        if self.config.rwa_only_mode() {
            return (
                Vec::new(),
                "rwa-only mode; broad execution disabled".to_string(),
                Vec::new(),
            );
        }

        let reserve_backed_pool_addresses = reserve_backed_pool_addresses(pools);
        let executable_opportunities = opportunities
            .iter()
            .filter(|opportunity| {
                !NativeSwapBuilder::is_native_candidate(opportunity)
                    || native_opportunity_uses_reserve_backed_pools(
                        opportunity,
                        &reserve_backed_pool_addresses,
                    )
            })
            .cloned()
            .collect::<Vec<_>>();
        let opportunity_candidate_source = if opportunities.iter().any(|opportunity| {
            opportunity.buy_pool.contains("-route:") || opportunity.sell_pool.contains("-route:")
        }) {
            "trade-source opportunity"
        } else {
            "direct pool opportunity"
        };
        if !self.config.execution.native_focus_routes.is_empty() {
            let native_report = self.focused_native_route_edge_candidates(
                pools,
                network_fee,
                sol_usd_estimate,
                quote_balance_caps,
            );
            if !native_report.candidates.is_empty() {
                return (
                    native_report.candidates,
                    "focused native optimized route".to_string(),
                    native_report.rejections,
                );
            }
            return (
                Vec::new(),
                "focused native optimized route unavailable".to_string(),
                native_report.rejections,
            );
        }

        let probe_candidates =
            self.jupiter_probe_candidates(execution_pairs, network_fee, sol_usd_estimate);

        if self.config.execution.native_price_only_execution_enabled && !execution_pairs.is_empty()
        {
            let native_candidates = self.native_direct_candidates(
                execution_pairs,
                pools,
                network_fee,
                sol_usd_estimate,
            );
            if !native_candidates.is_empty() {
                return (
                    native_candidates,
                    "native direct pool opportunity".to_string(),
                    Vec::new(),
                );
            }
        }

        match (
            executable_opportunities.is_empty(),
            probe_candidates.is_empty(),
        ) {
            (true, true) => (
                Vec::new(),
                "no live candidate source".to_string(),
                Vec::new(),
            ),
            (true, false) => (
                probe_candidates,
                "Jupiter venue probe".to_string(),
                Vec::new(),
            ),
            (false, true) => (
                executable_opportunities,
                opportunity_candidate_source.to_string(),
                Vec::new(),
            ),
            (false, false) => {
                let mut candidates =
                    Vec::with_capacity(executable_opportunities.len() + probe_candidates.len());
                candidates.extend(executable_opportunities);
                candidates.extend(probe_candidates);
                (
                    candidates,
                    format!("{opportunity_candidate_source} + Jupiter venue probe fallback"),
                    Vec::new(),
                )
            }
        }
    }

    async fn fresh_quote_focused_native_candidates(
        &self,
        candidates: Vec<Opportunity>,
        rejections: &mut Vec<FocusedRouteCandidateRejectionSnapshot>,
        cycle: u64,
        network_fee: &FeeBreakdown,
        sol_usd_estimate: f64,
    ) -> Vec<Opportunity> {
        if candidates.is_empty()
            || self.config.execution.native_focus_routes.is_empty()
            || !self.config.trading.live
            || !self.config.execution.enabled
        {
            return candidates;
        }
        if !candidates.iter().any(focused_optimizer_candidate) {
            return candidates;
        }

        let wallet = match read_keypair_file(&self.config.wallet.path) {
            Ok(wallet) => wallet,
            Err(error) => {
                warn!(
                    wallet_path = %self.config.wallet.path,
                    ?error,
                    "focused native fresh quote filter skipped because wallet keypair could not be read"
                );
                return candidates;
            }
        };
        let ttl_ms = self.config.execution.stale_candidate_ttl_ms;
        let builder = NativeSwapBuilder::new(self.config_with_sol_usd(sol_usd_estimate))
            .with_pool_subscription(self.pool_subscription.clone())
            .with_strict_fresh_cache(ttl_ms);
        let min_requote_profit_bps = self
            .config
            .execution
            .min_requote_profit_bps
            .max(self.config.trading.min_net_profit_bps);
        let mut filtered = Vec::with_capacity(candidates.len());

        for candidate in candidates {
            let Some(route_edge_id) = candidate
                .optimizer_decision
                .as_ref()
                .and_then(|decision| decision.route_edge_id)
            else {
                filtered.push(candidate);
                continue;
            };
            self.warm_selected_focused_route_cache(&candidate).await;
            if let Err(error) = self
                .pool_subscription
                .ensure_focused_route_ready_for_quote(route_edge_id)
                .await
            {
                let reason = format!("focused route {route_edge_id} preflight failed: {error:#}");
                let account_summary = self
                    .focused_route_account_age_summary_for_route(route_edge_id)
                    .await;
                let reconciliation = fresh_quote_reconciliation_from_opportunity(
                    &candidate,
                    &reason,
                    None,
                    account_summary.clone(),
                );
                self.persist_focused_fresh_quote_rejection(
                    cycle,
                    &candidate,
                    &reason,
                    network_fee,
                    sol_usd_estimate,
                    None,
                    account_summary,
                );
                rejections.push(
                    focused_route_rejection_from_opportunity_with_reconciliation(
                        &candidate,
                        &reason,
                        Some(reconciliation),
                    ),
                );
                continue;
            }
            if let Err(error) = builder
                .warm_static_hot_accounts(&wallet.pubkey(), &candidate)
                .await
            {
                let reason = format!("focused native static account warmup failed: {error:#}");
                let account_summary = self
                    .focused_route_account_age_summary_for_route(route_edge_id)
                    .await;
                let reconciliation = fresh_quote_reconciliation_from_opportunity(
                    &candidate,
                    &reason,
                    None,
                    account_summary.clone(),
                );
                self.persist_focused_fresh_quote_rejection(
                    cycle,
                    &candidate,
                    &reason,
                    network_fee,
                    sol_usd_estimate,
                    None,
                    account_summary,
                );
                rejections.push(
                    focused_route_rejection_from_opportunity_with_reconciliation(
                        &candidate,
                        &reason,
                        Some(reconciliation),
                    ),
                );
                continue;
            }

            let fresh_quote_result = match builder
                .quote_atomic_arbitrage_from_snapshot(&wallet.pubkey(), &candidate)
                .await
            {
                Err(error) => {
                    let first_error = format!("{error:#}");
                    if is_native_strict_cache_stale_error(&first_error) {
                        debug!(
                            route_edge_id,
                            error = %first_error,
                            "retrying focused fresh quote after strict cache stale error"
                        );
                        self.warm_selected_focused_route_cache(&candidate).await;
                        match builder
                            .quote_atomic_arbitrage_from_snapshot(&wallet.pubkey(), &candidate)
                            .await
                        {
                            Ok(result) => Ok(result),
                            Err(retry_error) => Err(anyhow!(
                                "fresh quote retry after strict-cache stale failed: first error: {first_error}; retry error: {retry_error:#}"
                            )),
                        }
                    } else {
                        Err(error)
                    }
                }
                result => result,
            };

            match fresh_quote_result {
                Ok(Some(fresh_quote)) => {
                    match apply_fresh_focused_quote_to_candidate(
                        candidate.clone(),
                        &fresh_quote,
                        min_requote_profit_bps,
                    ) {
                        Ok(fresh_candidate) => filtered.push(fresh_candidate),
                        Err(reason) => {
                            let account_summary = self
                                .focused_route_account_age_summary_for_route(route_edge_id)
                                .await;
                            let reconciliation = fresh_quote_reconciliation_from_opportunity(
                                &candidate,
                                &reason,
                                Some(&fresh_quote),
                                account_summary.clone(),
                            );
                            self.persist_focused_fresh_quote_rejection(
                                cycle,
                                &candidate,
                                &reason,
                                network_fee,
                                sol_usd_estimate,
                                Some(&fresh_quote),
                                account_summary,
                            );
                            rejections.push(
                                focused_route_rejection_from_opportunity_with_reconciliation(
                                    &candidate,
                                    &reason,
                                    Some(reconciliation),
                                ),
                            );
                        }
                    }
                }
                Ok(None) => {
                    let reason = "focused native fresh quote unavailable: not a native candidate"
                        .to_string();
                    let account_summary = self
                        .focused_route_account_age_summary_for_route(route_edge_id)
                        .await;
                    let reconciliation = fresh_quote_reconciliation_from_opportunity(
                        &candidate,
                        &reason,
                        None,
                        account_summary.clone(),
                    );
                    self.persist_focused_fresh_quote_rejection(
                        cycle,
                        &candidate,
                        &reason,
                        network_fee,
                        sol_usd_estimate,
                        None,
                        account_summary,
                    );
                    rejections.push(
                        focused_route_rejection_from_opportunity_with_reconciliation(
                            &candidate,
                            &reason,
                            Some(reconciliation),
                        ),
                    );
                }
                Err(error) => {
                    let reason = format!("focused native fresh quote unavailable: {error:#}");
                    let account_summary = self
                        .focused_route_account_age_summary_for_route(route_edge_id)
                        .await;
                    let reconciliation = fresh_quote_reconciliation_from_opportunity(
                        &candidate,
                        &reason,
                        None,
                        account_summary.clone(),
                    );
                    self.persist_focused_fresh_quote_rejection(
                        cycle,
                        &candidate,
                        &reason,
                        network_fee,
                        sol_usd_estimate,
                        None,
                        account_summary,
                    );
                    rejections.push(
                        focused_route_rejection_from_opportunity_with_reconciliation(
                            &candidate,
                            &reason,
                            Some(reconciliation),
                        ),
                    );
                }
            }
        }

        filtered
    }

    async fn focused_route_account_age_summary_for_route(
        &self,
        route_edge_id: i64,
    ) -> Option<FocusedRouteAccountAgeSummarySnapshot> {
        self.pool_subscription
            .focused_route_readiness(route_edge_id)
            .await
            .map(|snapshot| focused_route_account_age_summary(&snapshot))
    }

    fn persist_focused_fresh_quote_rejection(
        &self,
        cycle: u64,
        candidate: &Opportunity,
        reason: &str,
        network_fee: &FeeBreakdown,
        sol_usd_estimate: f64,
        fresh_quote: Option<&crate::native_swap::NativeFreshQuote>,
        account_summary: Option<FocusedRouteAccountAgeSummarySnapshot>,
    ) {
        let Some(database) = &self.database else {
            return;
        };
        let Some(decision) = &candidate.optimizer_decision else {
            return;
        };
        let (Some(buy_pool), Some(sell_pool)) = (
            decision.buy_pool_snapshot.as_ref(),
            decision.sell_pool_snapshot.as_ref(),
        ) else {
            return;
        };
        let Ok(db) = database.lock() else {
            return;
        };
        let quote_atom_usd_value = quote_mint_usd_value(&buy_pool.pair, sol_usd_estimate)
            / 10f64.powi(buy_pool.pair.quote_decimals as i32);
        let min_absolute_profit_quote_atoms = quote_usd_to_atoms(
            &buy_pool.pair,
            self.config.execution.min_absolute_profit_usd,
            sol_usd_estimate,
        );
        let route_edge_id = decision.route_edge_id;
        let attempt_id = route_edge_id
            .map(|id| format!("focused-fresh-quote-reject-{cycle}-{id}"))
            .unwrap_or_else(|| format!("focused-fresh-quote-reject-{cycle}"));
        let reconciliation = fresh_quote_reconciliation_from_opportunity(
            candidate,
            reason,
            fresh_quote,
            account_summary,
        );
        if let Err(error) = persist_focused_optimizer_rejection_evidence_with_reconciliation(
            &db,
            cycle,
            route_edge_id,
            buy_pool,
            sell_pool,
            candidate.input_quote_atoms,
            decision.max_input_quote_atoms,
            reason,
            &attempt_id,
            now_unix_ms(),
            network_fee.clone(),
            quote_atom_usd_value,
            self.config.trading.fee_safety_buffer_bps,
            self.config.trading.min_net_profit_bps,
            min_absolute_profit_quote_atoms,
            !self.config.execution.native_prefunded_hot_accounts,
            sol_usd_estimate / 1_000_000_000.0,
            Some(&reconciliation),
        ) {
            debug!(
                ?error,
                route_edge_id = ?route_edge_id,
                "failed to persist focused fresh quote rejection evidence"
            );
        }
    }

    fn focused_native_route_edge_candidates(
        &self,
        pools: &[PoolState],
        network_fee: &FeeBreakdown,
        sol_usd_estimate: f64,
        quote_balance_caps: Option<&HashMap<String, u128>>,
    ) -> FocusedNativeCandidateReport {
        let Some(database) = &self.database else {
            return FocusedNativeCandidateReport::default();
        };
        let Ok(db) = database.lock() else {
            return FocusedNativeCandidateReport::default();
        };
        let Ok(targets) = db.complete_route_account_fetch_targets(512) else {
            return FocusedNativeCandidateReport::default();
        };
        drop(db);

        let pool_by_address = pools
            .iter()
            .map(|pool| (pool.address.as_str(), pool))
            .collect::<HashMap<_, _>>();
        let mut candidates = Vec::new();
        let mut rejections = Vec::new();
        let mut seen = HashSet::new();

        for target in targets {
            let Some(buy_venue) = live_probe_venue_from_name(&target.buy_venue) else {
                rejections.push(focused_route_rejection(&target, "unsupported buy venue"));
                continue;
            };
            let Some(sell_venue) = live_probe_venue_from_name(&target.sell_venue) else {
                rejections.push(focused_route_rejection(&target, "unsupported sell venue"));
                continue;
            };
            let pair = TokenPair {
                base_mint: target.base_mint.clone(),
                quote_mint: target.quote_mint.clone(),
                base_decimals: target.base_decimals,
                quote_decimals: target.quote_decimals,
            };
            if !opportunity_matches_focus_parts(
                &pair,
                buy_venue,
                sell_venue,
                &self.config.execution.native_focus_routes,
            ) {
                continue;
            }
            let key = (
                target.route_edge_id,
                target.buy_pool.clone(),
                target.sell_pool.clone(),
            );
            if !seen.insert(key) {
                continue;
            }
            let (Some(buy_pool), Some(sell_pool)) = (
                pool_by_address.get(target.buy_pool.as_str()).copied(),
                pool_by_address.get(target.sell_pool.as_str()).copied(),
            ) else {
                rejections.push(focused_route_rejection(
                    &target,
                    "route pools not present in current promoted pool set",
                ));
                continue;
            };
            if matching_focus_route_requires_fresh_accounts(
                &pair,
                buy_venue,
                sell_venue,
                &self.config.execution.native_focus_routes,
            ) && (!pool_has_focused_route_price_basis(buy_pool)
                || !pool_has_focused_route_price_basis(sell_pool))
            {
                rejections.push(focused_route_rejection(
                    &target,
                    "fresh account route requires decoded reserves or route-graph price snapshots for both pools",
                ));
                continue;
            }
            let max_input_quote_atoms = optimized_route_max_input_quote_atoms(
                &self.config,
                buy_pool,
                sol_usd_estimate,
                quote_balance_caps.and_then(|caps| caps.get(&pair.quote_mint).copied()),
            );
            if let Some(min_executable_input) = dlmm_min_executable_input_quote_atoms(buy_pool) {
                if min_executable_input > max_input_quote_atoms {
                    let reason = format!(
                        "no executable live size: min executable input {min_executable_input} exceeds cap {max_input_quote_atoms}"
                    );
                    if let Some(database) = &self.database {
                        if let Ok(db) = database.lock() {
                            let quote_atom_usd_value =
                                quote_mint_usd_value(&buy_pool.pair, sol_usd_estimate)
                                    / 10f64.powi(buy_pool.pair.quote_decimals as i32);
                            let min_absolute_profit_quote_atoms = quote_usd_to_atoms(
                                &buy_pool.pair,
                                self.config.execution.min_absolute_profit_usd,
                                sol_usd_estimate,
                            );
                            let attempt_id =
                                format!("focused-optimizer-reject-{}", target.route_edge_id);
                            if let Err(error) = persist_focused_optimizer_rejection_evidence(
                                &db,
                                0,
                                Some(target.route_edge_id),
                                buy_pool,
                                sell_pool,
                                min_executable_input,
                                max_input_quote_atoms,
                                &reason,
                                &attempt_id,
                                now_unix_ms(),
                                network_fee.clone(),
                                quote_atom_usd_value,
                                self.config.trading.fee_safety_buffer_bps,
                                self.config.trading.min_net_profit_bps,
                                min_absolute_profit_quote_atoms,
                                !self.config.execution.native_prefunded_hot_accounts,
                                sol_usd_estimate / 1_000_000_000.0,
                            ) {
                                debug!(
                                    ?error,
                                    route_edge_id = target.route_edge_id,
                                    "failed to persist focused optimizer rejection evidence"
                                );
                            }
                        }
                    }
                    rejections.push(focused_route_rejection(&target, &reason));
                    continue;
                }
            }
            if let Some(opportunity) = self.evaluate_pair_at_optimized_size(
                buy_pool,
                sell_pool,
                Some(target.route_edge_id),
                network_fee.clone(),
                sol_usd_estimate,
                quote_balance_caps.and_then(|caps| caps.get(&pair.quote_mint).copied()),
            ) {
                if opportunity.pair.base_mint == pair.base_mint
                    && opportunity.pair.quote_mint == pair.quote_mint
                    && opportunity.buy_venue == buy_venue
                    && opportunity.sell_venue == sell_venue
                {
                    candidates.push(opportunity);
                }
            } else {
                rejections.push(focused_route_rejection(
                    &target,
                    "optimizer produced no profitable focused native candidate",
                ));
            }
        }

        FocusedNativeCandidateReport {
            candidates,
            rejections,
        }
    }

    fn native_direct_candidates(
        &self,
        pairs: &[TokenPair],
        pools: &[PoolState],
        network_fee: &FeeBreakdown,
        sol_usd_estimate: f64,
    ) -> Vec<Opportunity> {
        let venue_pool_map = pools.iter().fold(
            HashMap::new(),
            |mut acc: HashMap<(String, String, Venue), &PoolState>, pool| {
                acc.entry((
                    pool.pair.base_mint.clone(),
                    pool.pair.quote_mint.clone(),
                    pool.venue,
                ))
                .or_insert(pool);
                acc
            },
        );

        let venues: Vec<Venue> = vec![Venue::Meteora, Venue::Raydium];
        let trade_sizes_usd = direct_opportunity_trade_sizes_usd(
            &self.config.execution,
            self.config.trading.trade_size_usd,
        );

        let mut candidates = Vec::new();
        for pair in pairs {
            for trade_size_usd in &trade_sizes_usd {
                let input_quote_atoms = ui_to_atoms(
                    trade_size_quote_ui(pair, *trade_size_usd, sol_usd_estimate),
                    pair.quote_decimals,
                );
                if input_quote_atoms == 0 {
                    continue;
                }

                let base_mint = &pair.base_mint;
                let quote_mint = &pair.quote_mint;
                let pair_pools: Vec<&PoolState> = venues
                    .iter()
                    .filter_map(|v| {
                        venue_pool_map
                            .get(&(base_mint.clone(), quote_mint.clone(), *v))
                            .copied()
                    })
                    .collect();

                for buy_pool in &pair_pools {
                    for sell_pool in &pair_pools {
                        if buy_pool.venue == sell_pool.venue
                            || buy_pool.address == sell_pool.address
                        {
                            continue;
                        }
                        // Try to compute actual profit from pool data, fall back to zero
                        let opportunity = self
                            .evaluate_pair_at_trade_size(
                                buy_pool,
                                sell_pool,
                                *trade_size_usd,
                                network_fee.clone(),
                                sol_usd_estimate,
                            )
                            .unwrap_or_else(|| Opportunity {
                                pair: pair.clone(),
                                buy_venue: buy_pool.venue,
                                sell_venue: sell_pool.venue,
                                buy_pool: buy_pool.address.clone(),
                                sell_pool: sell_pool.address.clone(),
                                input_quote_atoms,
                                base_atoms: 0,
                                output_quote_atoms: input_quote_atoms,
                                gross_profit_quote_atoms: 0,
                                net_profit_usd: 0.0,
                                net_profit_bps: 0.0,
                                spread_bps: 0.0,
                                fee: network_fee.clone(),
                                detected_slot: 0,
                                optimizer_decision: None,
                            });
                        candidates.push(opportunity);
                    }
                }
            }
        }
        candidates
    }

    fn jupiter_probe_candidates(
        &self,
        pairs: &[TokenPair],
        network_fee: &FeeBreakdown,
        sol_usd_estimate: f64,
    ) -> Vec<Opportunity> {
        if !self.config.execution.jupiter_probe_enabled {
            return Vec::new();
        }

        let quote_mints = self
            .config
            .trading
            .quote_mints
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let denylist = self
            .config
            .trading
            .major_denylist
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let venues = self
            .config
            .venues
            .enabled
            .iter()
            .filter_map(|venue| live_probe_venue_from_name(venue))
            .collect::<Vec<_>>();

        if venues.len() < 2 {
            return Vec::new();
        }

        let mut candidates = Vec::new();
        let trade_sizes_usd =
            probe_trade_sizes_usd(&self.config.execution, self.config.trading.trade_size_usd);
        for pair in pairs {
            if denylist.contains(&pair.base_mint)
                || is_blocklisted_remora_mint(&pair.base_mint)
                || is_pump_like_mint(&pair.base_mint)
                || !quote_mints.contains(&pair.quote_mint)
            {
                continue;
            }
            for trade_size_usd in &trade_sizes_usd {
                let input_quote_atoms = ui_to_atoms(
                    trade_size_quote_ui(pair, *trade_size_usd, sol_usd_estimate),
                    pair.quote_decimals,
                );
                if input_quote_atoms == 0 {
                    continue;
                }

                for buy_venue in &venues {
                    for sell_venue in &venues {
                        if buy_venue == sell_venue {
                            continue;
                        }
                        candidates.push(Opportunity {
                            pair: pair.clone(),
                            buy_venue: *buy_venue,
                            sell_venue: *sell_venue,
                            buy_pool: format!("jupiter-probe:{buy_venue}"),
                            sell_pool: format!("jupiter-probe:{sell_venue}"),
                            input_quote_atoms,
                            base_atoms: 0,
                            output_quote_atoms: input_quote_atoms,
                            gross_profit_quote_atoms: 0,
                            net_profit_usd: 0.0,
                            net_profit_bps: 0.0,
                            spread_bps: 0.0,
                            fee: network_fee.clone(),
                            detected_slot: 0,
                            optimizer_decision: None,
                        });
                    }
                }
            }
        }

        candidates
    }

    fn evaluate_pair_at_trade_size(
        &self,
        buy_pool: &PoolState,
        sell_pool: &PoolState,
        trade_size_usd: f64,
        network_fee: FeeBreakdown,
        sol_usd_estimate: f64,
    ) -> Option<Opportunity> {
        if !trade_size_usd.is_finite() || trade_size_usd <= 0.0 {
            return None;
        }
        if self.config.execution.max_pool_slot_age > 0 {
            let max_slot = buy_pool.updated_slot.max(sell_pool.updated_slot);
            let min_slot = buy_pool.updated_slot.min(sell_pool.updated_slot);
            if max_slot > 0
                && max_slot.saturating_sub(min_slot) > self.config.execution.max_pool_slot_age
            {
                return None;
            }
        }
        let input_quote_atoms = ui_to_atoms(
            trade_size_quote_ui(&buy_pool.pair, trade_size_usd, sol_usd_estimate),
            buy_pool.pair.quote_decimals,
        );
        let buy = crate::math::pool_quote(
            buy_pool,
            SwapSide::QuoteToBase,
            input_quote_atoms,
            &network_fee,
            self.config.trading.fee_safety_buffer_bps,
        )?;
        let sell = crate::math::pool_quote(
            sell_pool,
            SwapSide::BaseToQuote,
            buy.out_amount_atoms,
            &network_fee,
            self.config.trading.fee_safety_buffer_bps,
        )?;
        let gross_profit_quote_atoms = sell.out_amount_atoms as i128 - input_quote_atoms as i128;
        let gross_profit_usd = atoms_to_ui(
            gross_profit_quote_atoms.max(0) as u128,
            buy_pool.pair.quote_decimals,
        ) * quote_mint_usd_value(&buy_pool.pair, sol_usd_estimate);
        let safety_buffer_usd =
            trade_size_usd * self.config.trading.fee_safety_buffer_bps / 10_000.0;
        let net_profit_usd = gross_profit_usd - network_fee.network_fee_usd - safety_buffer_usd;
        let net_profit_bps = net_profit_usd / trade_size_usd * 10_000.0;
        if net_profit_bps < self.config.trading.min_net_profit_bps {
            return None;
        }
        if self.config.execution.min_absolute_profit_usd > 0.0
            && net_profit_usd < self.config.execution.min_absolute_profit_usd
        {
            return None;
        }

        let buy_price = buy_pool.price_quote_per_base()?;
        let sell_price = sell_pool.price_quote_per_base()?;
        let spread_bps = (sell_price - buy_price) / buy_price * 10_000.0;

        Some(Opportunity {
            pair: TokenPair {
                base_mint: buy_pool.pair.base_mint.clone(),
                quote_mint: buy_pool.pair.quote_mint.clone(),
                base_decimals: buy_pool.pair.base_decimals,
                quote_decimals: buy_pool.pair.quote_decimals,
            },
            buy_venue: buy_pool.venue,
            sell_venue: sell_pool.venue,
            buy_pool: buy_pool.address.clone(),
            sell_pool: sell_pool.address.clone(),
            input_quote_atoms,
            base_atoms: buy.out_amount_atoms,
            output_quote_atoms: sell.out_amount_atoms,
            gross_profit_quote_atoms,
            net_profit_usd,
            net_profit_bps,
            spread_bps,
            fee: network_fee,
            detected_slot: buy_pool.updated_slot.max(sell_pool.updated_slot),
            optimizer_decision: None,
        })
    }

    fn evaluate_pair_at_optimized_size(
        &self,
        buy_pool: &PoolState,
        sell_pool: &PoolState,
        route_edge_id: Option<i64>,
        network_fee: FeeBreakdown,
        sol_usd_estimate: f64,
        quote_balance_cap_atoms: Option<u128>,
    ) -> Option<Opportunity> {
        let max_input_quote_atoms = optimized_route_max_input_quote_atoms(
            &self.config,
            buy_pool,
            sol_usd_estimate,
            quote_balance_cap_atoms,
        );
        let min_input_quote_atoms =
            optimized_route_min_input_quote_atoms(&buy_pool.pair, sol_usd_estimate);
        let min_input_quote_atoms = dlmm_min_executable_input_quote_atoms(buy_pool)
            .map(|min_executable| min_input_quote_atoms.max(min_executable))
            .unwrap_or(min_input_quote_atoms);
        if min_input_quote_atoms >= max_input_quote_atoms {
            return None;
        }
        let quote_atom_usd_value = quote_mint_usd_value(&buy_pool.pair, sol_usd_estimate)
            / 10f64.powi(buy_pool.pair.quote_decimals as i32);
        let min_absolute_profit_quote_atoms = quote_usd_to_atoms(
            &buy_pool.pair,
            self.config.execution.min_absolute_profit_usd,
            sol_usd_estimate,
        );
        let safety_buffer_bps = self.config.trading.fee_safety_buffer_bps;
        let min_net_profit_bps = self.config.trading.min_net_profit_bps;
        let result =
            crate::math::optimize_two_pool_route_size(crate::math::TwoPoolRouteSizeRequest {
                buy_pool,
                sell_pool,
                min_input_quote_atoms,
                max_input_quote_atoms,
                network_fee: &network_fee,
                safety_buffer_bps,
                min_absolute_profit_quote_atoms,
                include_setup_cost: !self.config.execution.native_prefunded_hot_accounts,
                setup_lamport_usd_value: sol_usd_estimate / 1_000_000_000.0,
                min_net_profit_bps,
                quote_atom_usd_value,
            })?;
        let buy_price = buy_pool.price_quote_per_base()?;
        let sell_price = sell_pool.price_quote_per_base()?;
        let spread_bps = (sell_price - buy_price) / buy_price * 10_000.0;
        let optimizer_decision = optimizer_decision_trace(
            buy_pool,
            sell_pool,
            route_edge_id,
            min_input_quote_atoms,
            max_input_quote_atoms,
            &result,
            network_fee.clone(),
            quote_atom_usd_value,
            safety_buffer_bps,
            min_net_profit_bps,
            min_absolute_profit_quote_atoms,
            !self.config.execution.native_prefunded_hot_accounts,
            sol_usd_estimate / 1_000_000_000.0,
        );

        Some(Opportunity {
            pair: TokenPair {
                base_mint: buy_pool.pair.base_mint.clone(),
                quote_mint: buy_pool.pair.quote_mint.clone(),
                base_decimals: buy_pool.pair.base_decimals,
                quote_decimals: buy_pool.pair.quote_decimals,
            },
            buy_venue: buy_pool.venue,
            sell_venue: sell_pool.venue,
            buy_pool: buy_pool.address.clone(),
            sell_pool: sell_pool.address.clone(),
            input_quote_atoms: result.input_quote_atoms,
            base_atoms: result.base_atoms,
            output_quote_atoms: result.output_quote_atoms,
            gross_profit_quote_atoms: result.gross_profit_quote_atoms,
            net_profit_usd: result.net_profit_usd,
            net_profit_bps: result.net_profit_bps,
            spread_bps,
            fee: network_fee,
            detected_slot: buy_pool.updated_slot.max(sell_pool.updated_slot),
            optimizer_decision: Some(optimizer_decision),
        })
    }
}

#[derive(Debug, Default)]
struct FocusedNativeCandidateReport {
    candidates: Vec<Opportunity>,
    rejections: Vec<FocusedRouteCandidateRejectionSnapshot>,
}

#[derive(Debug, Clone)]
struct FocusedRoutePoolDecodeTarget {
    route_edge_id: i64,
    pair: TokenPair,
    venue: Venue,
    address: String,
    pool_kind: PoolKind,
    liquidity_quote_ui: Option<f64>,
    price_quote_per_base: Option<f64>,
}

fn focused_route_pool_decode_targets(
    targets: &[RouteAccountFetchTarget],
    focus_routes: &[crate::config::NativeFocusRoute],
) -> Vec<FocusedRoutePoolDecodeTarget> {
    let mut decode_targets = Vec::new();
    let mut seen = HashSet::new();
    for target in targets {
        let Some(buy_venue) = live_probe_venue_from_name(&target.buy_venue) else {
            continue;
        };
        let Some(sell_venue) = live_probe_venue_from_name(&target.sell_venue) else {
            continue;
        };
        let pair = TokenPair {
            base_mint: target.base_mint.clone(),
            quote_mint: target.quote_mint.clone(),
            base_decimals: target.base_decimals,
            quote_decimals: target.quote_decimals,
        };
        if !opportunity_matches_focus_parts(&pair, buy_venue, sell_venue, focus_routes) {
            continue;
        }
        for (venue, address, pool_kind, liquidity_quote_ui, price_quote_per_base) in [
            (
                buy_venue,
                target.buy_pool.clone(),
                pool_kind_from_db_name(&target.buy_pool_kind, buy_venue),
                target.buy_pool_liquidity_quote_ui,
                target.buy_pool_price_quote_per_base,
            ),
            (
                sell_venue,
                target.sell_pool.clone(),
                pool_kind_from_db_name(&target.sell_pool_kind, sell_venue),
                target.sell_pool_liquidity_quote_ui,
                target.sell_pool_price_quote_per_base,
            ),
        ] {
            let Some(pool_kind) = pool_kind else {
                continue;
            };
            if !seen.insert(address.clone()) {
                continue;
            }
            decode_targets.push(FocusedRoutePoolDecodeTarget {
                route_edge_id: target.route_edge_id,
                pair: pair.clone(),
                venue,
                address,
                pool_kind,
                liquidity_quote_ui,
                price_quote_per_base,
            });
        }
    }
    decode_targets
}

fn pool_kind_from_db_name(kind: &str, venue: Venue) -> Option<PoolKind> {
    match kind {
        "constant_product" => Some(PoolKind::ConstantProduct),
        "concentrated_liquidity" => Some(PoolKind::ConcentratedLiquidity),
        "dlmm" => Some(PoolKind::Dlmm),
        "raydium_amm_v4" => Some(PoolKind::RaydiumAmmV4),
        "meteora_pools" => Some(PoolKind::MeteoraPools),
        "meteora_damm_v2" => Some(PoolKind::MeteoraDammV2),
        _ if venue == Venue::Raydium => Some(PoolKind::ConstantProduct),
        _ if venue == Venue::Meteora => Some(PoolKind::MeteoraPools),
        _ => None,
    }
}

fn decode_focused_route_pool(
    adapters: &[Box<dyn VenueAdapter>],
    account: &RawAccount,
    target: &FocusedRoutePoolDecodeTarget,
) -> Option<PoolState> {
    let adapter = adapters
        .iter()
        .find(|adapter| adapter.venue() == target.venue)?;
    let candidate = IndexedPoolCandidate {
        venue: target.venue,
        address: target.address.clone(),
        base_mint: target.pair.base_mint.clone(),
        quote_mint: target.pair.quote_mint.clone(),
        base_decimals: target.pair.base_decimals,
        quote_decimals: target.pair.quote_decimals,
        liquidity_usd: 0.0,
        liquidity_quote_ui: None,
        price_quote_per_base: None,
        source: format!("focused_route_edge_{}", target.route_edge_id),
    };
    adapter
        .decode_pool(account)
        .and_then(|pool| normalize_indexed_pool(pool, &candidate))
}

fn append_focused_route_pools(
    promoted: &mut Vec<PoolState>,
    targets: &[RouteAccountFetchTarget],
    decoded_pools: Vec<PoolState>,
    focus_routes: &[crate::config::NativeFocusRoute],
    fallback_sizing: Option<(&BotConfig, f64)>,
) -> usize {
    append_focused_route_pools_with_fallback(
        promoted,
        targets,
        decoded_pools,
        focus_routes,
        fallback_sizing,
    )
}

fn append_focused_route_pools_with_fallback(
    promoted: &mut Vec<PoolState>,
    targets: &[RouteAccountFetchTarget],
    decoded_pools: Vec<PoolState>,
    focus_routes: &[crate::config::NativeFocusRoute],
    fallback_sizing: Option<(&BotConfig, f64)>,
) -> usize {
    let mut decoded_by_address = decoded_pools
        .into_iter()
        .map(|pool| (pool.address.clone(), pool))
        .collect::<HashMap<_, _>>();
    let before = promoted.len();

    for target in focused_route_pool_decode_targets(targets, focus_routes) {
        let fallback_quote_liquidity_atoms = fallback_sizing.and_then(|(config, sol_usd)| {
            focused_route_fallback_liquidity_quote_atoms(config, &target.pair, sol_usd)
        });
        let Some(pool) = decoded_by_address
            .remove(&target.address)
            .or_else(|| materialize_focused_route_pool(&target, fallback_quote_liquidity_atoms))
        else {
            continue;
        };
        if pool.venue != target.venue
            || pool.pair.base_mint != target.pair.base_mint
            || pool.pair.quote_mint != target.pair.quote_mint
        {
            continue;
        }
        if let Some(index) = promoted
            .iter()
            .position(|existing| existing.address == target.address)
        {
            if !pool_has_focused_route_price_basis(&promoted[index])
                && pool_has_focused_route_price_basis(&pool)
            {
                promoted[index] = pool;
            }
            continue;
        }
        promoted.push(pool);
    }

    promoted.len().saturating_sub(before)
}

fn materialize_focused_route_pool(
    target: &FocusedRoutePoolDecodeTarget,
    fallback_quote_liquidity_atoms: Option<u128>,
) -> Option<PoolState> {
    let price = target.price_quote_per_base?;
    if !price.is_finite() || price <= 0.0 {
        return None;
    }
    let liquidity_quote_atoms = match target.liquidity_quote_ui {
        Some(liquidity_quote_ui) if liquidity_quote_ui.is_finite() && liquidity_quote_ui > 0.0 => {
            ui_to_atoms(liquidity_quote_ui, target.pair.quote_decimals)
        }
        _ => fallback_quote_liquidity_atoms?,
    };
    if liquidity_quote_atoms == 0 {
        return None;
    }

    Some(PoolState {
        venue: target.venue,
        kind: target.pool_kind,
        address: target.address.clone(),
        pair: target.pair.clone(),
        reserve_base_atoms: 0,
        reserve_quote_atoms: 0,
        sqrt_price_x64: sqrt_price_x64_from_price(
            price,
            target.pair.base_decimals,
            target.pair.quote_decimals,
        ),
        active_bin_id: None,
        fee_bps: indexed_fallback_fee_bps(target.venue),
        protocol_fee_bps: 0,
        liquidity_quote_atoms,
        updated_slot: 0,
    })
}

fn focused_route_fallback_liquidity_quote_atoms(
    config: &BotConfig,
    pair: &TokenPair,
    sol_usd_estimate: f64,
) -> Option<u128> {
    let max_usd = if config.execution.max_live_trade_usd.is_finite()
        && config.execution.max_live_trade_usd > 0.0
    {
        config.execution.max_live_trade_usd
    } else {
        config
            .execution
            .direct_trade_sizes_usd
            .iter()
            .copied()
            .filter(|size| size.is_finite() && *size > 0.0)
            .fold(config.trading.trade_size_usd.max(0.0), f64::max)
    };
    if !max_usd.is_finite() || max_usd <= 0.0 {
        return None;
    }
    let max_input_quote_atoms = quote_usd_to_atoms(pair, max_usd, sol_usd_estimate).max(
        optimized_route_min_input_quote_atoms(pair, sol_usd_estimate),
    );
    Some(
        max_input_quote_atoms
            .saturating_mul(5)
            .max(max_input_quote_atoms),
    )
}

fn focused_route_rejection(
    target: &crate::db::RouteAccountFetchTarget,
    reason: &str,
) -> FocusedRouteCandidateRejectionSnapshot {
    FocusedRouteCandidateRejectionSnapshot {
        route_edge_id: Some(target.route_edge_id),
        pair: TokenPair {
            base_mint: target.base_mint.clone(),
            quote_mint: target.quote_mint.clone(),
            base_decimals: target.base_decimals,
            quote_decimals: target.quote_decimals,
        },
        buy_venue: target.buy_venue.clone(),
        sell_venue: target.sell_venue.clone(),
        buy_pool: target.buy_pool.clone(),
        sell_pool: target.sell_pool.clone(),
        reason: reason.to_string(),
        fresh_quote_reconciliation: None,
    }
}

fn focused_route_rejection_from_opportunity_with_reconciliation(
    opportunity: &Opportunity,
    reason: &str,
    fresh_quote_reconciliation: Option<FreshQuoteReconciliationSnapshot>,
) -> FocusedRouteCandidateRejectionSnapshot {
    FocusedRouteCandidateRejectionSnapshot {
        route_edge_id: opportunity
            .optimizer_decision
            .as_ref()
            .and_then(|decision| decision.route_edge_id),
        pair: opportunity.pair.clone(),
        buy_venue: opportunity.buy_venue.to_string(),
        sell_venue: opportunity.sell_venue.to_string(),
        buy_pool: opportunity.buy_pool.clone(),
        sell_pool: opportunity.sell_pool.clone(),
        reason: reason.to_string(),
        fresh_quote_reconciliation,
    }
}

fn focused_optimizer_candidate(opportunity: &Opportunity) -> bool {
    opportunity
        .optimizer_decision
        .as_ref()
        .and_then(|decision| decision.route_edge_id)
        .is_some()
        && NativeSwapBuilder::is_native_candidate(opportunity)
}

fn live_risk_snapshot_from_config(
    execution: &ExecutionConfig,
    failed_fee_loss_count: u64,
) -> LiveRiskSnapshot {
    let mut snapshot = LiveRiskSnapshot {
        gate_open: true,
        blocked_reason: None,
        max_fee_loss_usd_per_session: execution.max_live_fee_loss_usd_per_session,
        max_failed_transactions_per_session: execution.max_failed_transactions_per_session,
        failed_fee_loss_count,
    };
    if execution.max_live_fee_loss_usd_per_session <= 0.0 {
        snapshot.gate_open = false;
        snapshot.blocked_reason = Some(
            "live submit risk gate closed: MAX_LIVE_FEE_LOSS_USD_PER_SESSION must be > 0"
                .to_string(),
        );
        return snapshot;
    }
    if execution.max_failed_transactions_per_session == 0 {
        snapshot.gate_open = false;
        snapshot.blocked_reason = Some(
            "live submit risk gate closed: MAX_FAILED_TRANSACTIONS_PER_SESSION must be > 0"
                .to_string(),
        );
        return snapshot;
    }
    if failed_fee_loss_count >= execution.max_failed_transactions_per_session {
        snapshot.gate_open = false;
        snapshot.blocked_reason = Some(format!(
            "live submit risk gate closed: failed fee-loss cap reached {failed_fee_loss_count}/{}",
            execution.max_failed_transactions_per_session
        ));
    }
    snapshot
}

fn live_submit_proof_arming_blocker(execution: &ExecutionConfig) -> Option<String> {
    if execution.require_simulation {
        return Some(
            "live submit proof mode requires LIVE_REQUIRE_SIMULATION=false to avoid edge decay"
                .to_string(),
        );
    }
    if execution.max_live_trades_per_session != 1 {
        return Some(format!(
            "live submit proof mode requires MAX_LIVE_TRADES_PER_SESSION=1, got {}",
            execution.max_live_trades_per_session
        ));
    }
    if execution.max_live_trades_per_cycle != 1 {
        return Some(format!(
            "live submit proof mode requires MAX_LIVE_TRADES_PER_CYCLE=1, got {}",
            execution.max_live_trades_per_cycle
        ));
    }
    if execution.max_live_trade_usd <= 0.0 {
        return Some("live submit proof mode requires MAX_LIVE_TRADE_USD > 0".to_string());
    }
    None
}

fn fresh_quote_truth_from_candidates_and_rejections(
    candidates: &[Opportunity],
    rejections: &[FocusedRouteCandidateRejectionSnapshot],
) -> FreshQuoteTruthSnapshot {
    let mut truth = FreshQuoteTruthSnapshot::default();
    truth.executable_candidate_count = candidates
        .iter()
        .filter(|candidate| {
            candidate
                .optimizer_decision
                .as_ref()
                .and_then(|decision| decision.route_edge_id)
                .is_some()
        })
        .count();
    truth.fresh_positive_complete = truth.executable_candidate_count;
    truth.rejected_candidate_count = rejections.len();

    for rejection in rejections {
        match rejection
            .fresh_quote_reconciliation
            .as_ref()
            .map(|reconciliation| reconciliation.fresh_quote_status.as_str())
        {
            Some("fresh_positive_complete") => {
                truth.fresh_positive_complete = truth.fresh_positive_complete.saturating_add(1);
            }
            Some("fresh_positive_blocked") => {
                truth.fresh_positive_blocked = truth.fresh_positive_blocked.saturating_add(1);
            }
            Some("fresh_negative_complete") => {
                truth.fresh_negative_complete = truth.fresh_negative_complete.saturating_add(1);
            }
            Some("fresh_quote_unavailable") | None => {
                truth.fresh_quote_unavailable = truth.fresh_quote_unavailable.saturating_add(1);
            }
            Some(_) => {
                truth.fresh_quote_unavailable = truth.fresh_quote_unavailable.saturating_add(1);
            }
        }
    }

    truth.execution_gate_open = truth.fresh_positive_complete > 0;
    truth
}

fn apply_fresh_focused_quote_to_candidate(
    mut candidate: Opportunity,
    fresh_quote: &crate::native_swap::NativeFreshQuote,
    min_requote_profit_bps: f64,
) -> std::result::Result<Opportunity, String> {
    if let Some(reason) = &fresh_quote.blocked_reason {
        return Err(reason.clone());
    }
    if !fresh_quote.fresh_requote_complete {
        return Err("focused native fresh quote is incomplete".to_string());
    }
    if fresh_quote.buy_quote_out_atoms == 0 {
        return Err("native direct fresh buy quote produced zero output".to_string());
    }
    if fresh_quote.sell_quote_out_atoms == 0 {
        return Err("native direct fresh sell quote produced zero output".to_string());
    }
    if fresh_quote.requote_net_profit_bps < min_requote_profit_bps {
        return Err(format!(
            "native direct re-quote net profit {:.2} bps is below required {:.2} bps",
            fresh_quote.requote_net_profit_bps, min_requote_profit_bps
        ));
    }

    let input_quote_atoms = i128::try_from(candidate.input_quote_atoms).unwrap_or(i128::MAX);
    let gross_profit_quote_atoms = i128::from(fresh_quote.sell_quote_out_atoms) - input_quote_atoms;
    let net_profit_quote_atoms =
        gross_profit_quote_atoms - i128::from(fresh_quote.execution_cost_quote_atoms);

    candidate.base_atoms = u128::from(fresh_quote.buy_quote_out_atoms);
    candidate.output_quote_atoms = u128::from(fresh_quote.sell_quote_out_atoms);
    candidate.gross_profit_quote_atoms = gross_profit_quote_atoms;
    candidate.net_profit_usd = fresh_quote.requote_net_profit_usd;
    candidate.net_profit_bps = fresh_quote.requote_net_profit_bps;
    if let Some(decision) = candidate.optimizer_decision.as_mut() {
        decision.base_atoms = candidate.base_atoms;
        decision.output_quote_atoms = candidate.output_quote_atoms;
        decision.gross_profit_quote_atoms = gross_profit_quote_atoms;
        decision.net_profit_quote_atoms = net_profit_quote_atoms;
        decision.net_profit_usd = fresh_quote.requote_net_profit_usd;
        decision.net_profit_bps = fresh_quote.requote_net_profit_bps;
    }
    Ok(candidate)
}

fn focused_route_account_age_summary(
    snapshot: &FocusedRouteReadinessSnapshot,
) -> FocusedRouteAccountAgeSummarySnapshot {
    let required_accounts = snapshot
        .accounts
        .iter()
        .filter(|account| account.required)
        .count();
    let ready_accounts = snapshot
        .accounts
        .iter()
        .filter(|account| account.status == crate::types::FocusedRouteAccountStatus::Ready)
        .count();
    let missing_required_accounts = snapshot
        .accounts
        .iter()
        .filter(|account| {
            account.required && account.status == crate::types::FocusedRouteAccountStatus::Missing
        })
        .count();
    let stale_required_accounts = snapshot
        .accounts
        .iter()
        .filter(|account| {
            account.required && account.status == crate::types::FocusedRouteAccountStatus::Stale
        })
        .count();
    FocusedRouteAccountAgeSummarySnapshot {
        ready: snapshot.ready,
        total_accounts: snapshot.accounts.len(),
        required_accounts,
        ready_accounts,
        missing_required_accounts,
        stale_required_accounts,
        max_wall_clock_age_ms: snapshot
            .accounts
            .iter()
            .filter_map(|account| account.wall_clock_age_ms)
            .max(),
        max_slot_age: snapshot
            .accounts
            .iter()
            .filter_map(|account| account.slot_age)
            .max(),
        blocker: snapshot.blocker.clone(),
    }
}

fn fresh_quote_reconciliation_from_opportunity(
    opportunity: &Opportunity,
    final_blocker: &str,
    fresh_quote: Option<&crate::native_swap::NativeFreshQuote>,
    account_snapshot: Option<FocusedRouteAccountAgeSummarySnapshot>,
) -> FreshQuoteReconciliationSnapshot {
    let optimizer = opportunity.optimizer_decision.as_ref();
    let optimizer_input_quote_atoms = optimizer
        .map(|decision| decision.selected_input_quote_atoms)
        .unwrap_or(opportunity.input_quote_atoms);
    let optimizer_buy_output_base_atoms = optimizer
        .map(|decision| decision.base_atoms)
        .unwrap_or(opportunity.base_atoms);
    let optimizer_sell_output_quote_atoms = optimizer
        .map(|decision| decision.output_quote_atoms)
        .unwrap_or(opportunity.output_quote_atoms);
    let optimizer_net_profit_quote_atoms = optimizer
        .map(|decision| decision.net_profit_quote_atoms)
        .unwrap_or_else(|| {
            i128::try_from(opportunity.output_quote_atoms).unwrap_or(i128::MAX)
                - i128::try_from(opportunity.input_quote_atoms).unwrap_or(i128::MAX)
        });
    let fresh_net_profit_quote_atoms = fresh_quote.map(|quote| {
        i128::from(quote.sell_quote_out_atoms)
            - i128::try_from(opportunity.input_quote_atoms).unwrap_or(i128::MAX)
            - i128::from(quote.execution_cost_quote_atoms)
    });
    let fresh_quote_status = fresh_quote_status(fresh_quote, fresh_net_profit_quote_atoms);

    FreshQuoteReconciliationSnapshot {
        route_edge_id: optimizer.and_then(|decision| decision.route_edge_id),
        pair: opportunity.pair.clone(),
        buy_venue: opportunity.buy_venue.to_string(),
        sell_venue: opportunity.sell_venue.to_string(),
        buy_pool: opportunity.buy_pool.clone(),
        sell_pool: opportunity.sell_pool.clone(),
        optimizer_input_quote_atoms: optimizer_input_quote_atoms.to_string(),
        optimizer_buy_output_base_atoms: optimizer_buy_output_base_atoms.to_string(),
        optimizer_sell_output_quote_atoms: optimizer_sell_output_quote_atoms.to_string(),
        optimizer_net_profit_quote_atoms: optimizer_net_profit_quote_atoms.to_string(),
        optimizer_net_profit_bps: optimizer
            .map(|decision| decision.net_profit_bps)
            .unwrap_or(opportunity.net_profit_bps),
        fresh_buy_output_base_atoms: fresh_quote.map(|quote| quote.buy_quote_out_atoms.to_string()),
        fresh_sell_output_quote_atoms: fresh_quote
            .map(|quote| quote.sell_quote_out_atoms.to_string()),
        fresh_net_profit_quote_atoms: fresh_net_profit_quote_atoms.map(|value| value.to_string()),
        fresh_net_profit_bps: fresh_quote.map(|quote| quote.requote_net_profit_bps),
        execution_cost_quote_atoms: fresh_quote
            .map(|quote| quote.execution_cost_quote_atoms.to_string()),
        fresh_requote_complete: fresh_quote.map(|quote| quote.fresh_requote_complete),
        fresh_blocked_reason: fresh_quote.and_then(|quote| quote.blocked_reason.clone()),
        fresh_quote_status,
        final_blocker: final_blocker.to_string(),
        account_snapshot,
    }
}

fn fresh_quote_status(
    fresh_quote: Option<&crate::native_swap::NativeFreshQuote>,
    fresh_net_profit_quote_atoms: Option<i128>,
) -> String {
    let Some(fresh_quote) = fresh_quote else {
        return "fresh_quote_unavailable".to_string();
    };
    let profitable_after_cost = fresh_net_profit_quote_atoms.is_some_and(|atoms| atoms > 0);
    if fresh_quote.blocked_reason.is_some() || !fresh_quote.fresh_requote_complete {
        return if profitable_after_cost {
            "fresh_positive_blocked".to_string()
        } else {
            "fresh_quote_unavailable".to_string()
        };
    }
    if profitable_after_cost {
        "fresh_positive_complete".to_string()
    } else {
        "fresh_negative_complete".to_string()
    }
}

fn optimizer_decision_trace(
    buy_pool: &PoolState,
    sell_pool: &PoolState,
    route_edge_id: Option<i64>,
    min_input_quote_atoms: u128,
    max_input_quote_atoms: u128,
    result: &crate::math::TwoPoolRouteSizeResult,
    network_fee: FeeBreakdown,
    quote_atom_usd_value: f64,
    safety_buffer_bps: f64,
    min_net_profit_bps: f64,
    min_absolute_profit_quote_atoms: u128,
    include_setup_cost: bool,
    setup_lamport_usd_value: f64,
) -> OptimizerDecisionTrace {
    let source = "two_pool_route_size_optimizer".to_string();
    let route_label = format!(
        "{}:{}->{}:{}",
        buy_pool.venue, buy_pool.address, sell_pool.venue, sell_pool.address
    );
    let id_material = json!({
        "source": source,
        "route_label": route_label,
        "base_mint": buy_pool.pair.base_mint,
        "quote_mint": buy_pool.pair.quote_mint,
        "min_input_quote_atoms": min_input_quote_atoms.to_string(),
        "max_input_quote_atoms": max_input_quote_atoms.to_string(),
        "selected_input_quote_atoms": result.input_quote_atoms.to_string(),
        "base_atoms": result.base_atoms.to_string(),
        "output_quote_atoms": result.output_quote_atoms.to_string(),
        "gross_profit_quote_atoms": result.gross_profit_quote_atoms.to_string(),
        "required_profit_quote_atoms": result.required_profit_quote_atoms.to_string(),
        "net_profit_quote_atoms": result.net_profit_quote_atoms.to_string(),
        "quote_atom_usd_value": quote_atom_usd_value,
        "safety_buffer_bps": safety_buffer_bps,
        "min_net_profit_bps": min_net_profit_bps,
        "min_absolute_profit_quote_atoms": min_absolute_profit_quote_atoms.to_string(),
    });
    let optimizer_decision_id = format!("{:x}", Sha256::digest(id_material.to_string()));

    OptimizerDecisionTrace {
        optimizer_decision_id,
        source,
        route_label,
        route_edge_id,
        buy_pool: buy_pool.address.clone(),
        sell_pool: sell_pool.address.clone(),
        buy_venue: Some(buy_pool.venue),
        sell_venue: Some(sell_pool.venue),
        min_input_quote_atoms,
        max_input_quote_atoms,
        selected_input_quote_atoms: result.input_quote_atoms,
        base_atoms: result.base_atoms,
        output_quote_atoms: result.output_quote_atoms,
        gross_profit_quote_atoms: result.gross_profit_quote_atoms,
        required_profit_quote_atoms: result.required_profit_quote_atoms,
        net_profit_quote_atoms: result.net_profit_quote_atoms,
        net_profit_usd: result.net_profit_usd,
        net_profit_bps: result.net_profit_bps,
        quote_atom_usd_value,
        safety_buffer_bps,
        min_net_profit_bps,
        min_absolute_profit_quote_atoms,
        min_executable_input_quote_atoms: None,
        rejection_reason: None,
        network_fee,
        include_setup_cost,
        setup_lamport_usd_value,
        buy_pool_snapshot: Some(buy_pool.clone()),
        sell_pool_snapshot: Some(sell_pool.clone()),
    }
}

#[allow(clippy::too_many_arguments)]
fn optimizer_rejection_trace(
    buy_pool: &PoolState,
    sell_pool: &PoolState,
    route_edge_id: Option<i64>,
    min_executable_input_quote_atoms: u128,
    max_input_quote_atoms: u128,
    reason: &str,
    network_fee: FeeBreakdown,
    quote_atom_usd_value: f64,
    safety_buffer_bps: f64,
    min_net_profit_bps: f64,
    min_absolute_profit_quote_atoms: u128,
    include_setup_cost: bool,
    setup_lamport_usd_value: f64,
) -> OptimizerDecisionTrace {
    let source = "two_pool_route_size_optimizer".to_string();
    let route_label = format!(
        "{}:{}->{}:{}",
        buy_pool.venue, buy_pool.address, sell_pool.venue, sell_pool.address
    );
    let id_material = json!({
        "source": source,
        "route_label": route_label,
        "route_edge_id": route_edge_id,
        "base_mint": buy_pool.pair.base_mint,
        "quote_mint": buy_pool.pair.quote_mint,
        "min_executable_input_quote_atoms": min_executable_input_quote_atoms.to_string(),
        "max_input_quote_atoms": max_input_quote_atoms.to_string(),
        "rejection_reason": reason,
    });
    let optimizer_decision_id = format!("{:x}", Sha256::digest(id_material.to_string()));

    OptimizerDecisionTrace {
        optimizer_decision_id,
        source,
        route_label,
        route_edge_id,
        buy_pool: buy_pool.address.clone(),
        sell_pool: sell_pool.address.clone(),
        buy_venue: Some(buy_pool.venue),
        sell_venue: Some(sell_pool.venue),
        min_input_quote_atoms: min_executable_input_quote_atoms,
        max_input_quote_atoms,
        selected_input_quote_atoms: max_input_quote_atoms,
        base_atoms: 0,
        output_quote_atoms: 0,
        gross_profit_quote_atoms: -i128::try_from(max_input_quote_atoms).unwrap_or(i128::MAX),
        required_profit_quote_atoms: min_absolute_profit_quote_atoms,
        net_profit_quote_atoms: -i128::try_from(max_input_quote_atoms).unwrap_or(i128::MAX),
        net_profit_usd: 0.0,
        net_profit_bps: 0.0,
        quote_atom_usd_value,
        safety_buffer_bps,
        min_net_profit_bps,
        min_absolute_profit_quote_atoms,
        min_executable_input_quote_atoms: Some(min_executable_input_quote_atoms),
        rejection_reason: Some(reason.to_string()),
        network_fee,
        include_setup_cost,
        setup_lamport_usd_value,
        buy_pool_snapshot: Some(buy_pool.clone()),
        sell_pool_snapshot: Some(sell_pool.clone()),
    }
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn runtime_attempt_id(kind: &str, cycle: u64, id: u64) -> String {
    format!("{kind}-{cycle}-{id}")
}

fn runtime_config_hash(config: &BotConfig) -> String {
    let bytes = serde_json::to_vec(config).unwrap_or_default();
    let hash = Sha256::digest(bytes);
    format!("{hash:x}")
}

fn resolve_opportunity_route_edge_id(
    db: &ArbDatabase,
    opportunity: &Opportunity,
) -> Result<Option<i64>> {
    db.route_edge_id_for_pools(
        &opportunity.pair,
        opportunity.buy_venue,
        opportunity.sell_venue,
        &opportunity.buy_pool,
        &opportunity.sell_pool,
    )
}

fn route_context_json(
    opportunity: &Opportunity,
    route_edge_id: Option<i64>,
    mapping_blocker: Option<&str>,
    latency_trace: Option<&[crate::live_swap::ExecutionLatencyMark]>,
    fresh_quote_reconciliation: Option<&FreshQuoteReconciliationSnapshot>,
) -> Result<String> {
    let mut context = json!({
        "pair": {
            "base_mint": opportunity.pair.base_mint,
            "quote_mint": opportunity.pair.quote_mint,
            "base_decimals": opportunity.pair.base_decimals,
            "quote_decimals": opportunity.pair.quote_decimals,
        },
        "buy_venue": opportunity.buy_venue,
        "sell_venue": opportunity.sell_venue,
        "buy_pool": opportunity.buy_pool,
        "sell_pool": opportunity.sell_pool,
        "route_edge_id": route_edge_id,
        "route_edge_mapped": route_edge_id.is_some(),
        "mapping_blocker": mapping_blocker,
    });
    if let Some(trace) = latency_trace.filter(|trace| !trace.is_empty()) {
        context["latency_trace"] = serde_json::to_value(trace)?;
    }
    if let Some(optimizer_decision) = &opportunity.optimizer_decision {
        let mut optimizer_decision = optimizer_decision.clone();
        if optimizer_decision.route_edge_id.is_none() {
            optimizer_decision.route_edge_id = route_edge_id;
        }
        context["optimizer_decision"] = serde_json::to_value(&optimizer_decision)?;
    }
    if let Some(reconciliation) = fresh_quote_reconciliation {
        context["fresh_quote_reconciliation"] = serde_json::to_value(reconciliation)?;
    }
    Ok(serde_json::to_string(&context)?)
}

fn quote_delta_atoms(opportunity: &Opportunity) -> i128 {
    opportunity.output_quote_atoms as i128 - opportunity.input_quote_atoms as i128
}

fn persist_quote_observation_evidence(
    db: &ArbDatabase,
    cycle: u64,
    opportunity: &Opportunity,
    route_edge_id: Option<i64>,
    source: &str,
    blocker: Option<String>,
    latency_trace: Option<&[crate::live_swap::ExecutionLatencyMark]>,
    attempt_id: &str,
    now_ms: u128,
) -> Result<()> {
    persist_quote_observation_evidence_with_reconciliation(
        db,
        cycle,
        opportunity,
        route_edge_id,
        source,
        blocker,
        latency_trace,
        attempt_id,
        now_ms,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn persist_quote_observation_evidence_with_reconciliation(
    db: &ArbDatabase,
    cycle: u64,
    opportunity: &Opportunity,
    route_edge_id: Option<i64>,
    source: &str,
    blocker: Option<String>,
    latency_trace: Option<&[crate::live_swap::ExecutionLatencyMark]>,
    attempt_id: &str,
    now_ms: u128,
    fresh_quote_reconciliation: Option<&FreshQuoteReconciliationSnapshot>,
) -> Result<()> {
    let freshness_json = route_context_json(
        opportunity,
        route_edge_id,
        blocker.as_deref(),
        latency_trace,
        fresh_quote_reconciliation,
    )?;
    db.insert_quote_observation(&QuoteObservationRecord {
        route_edge_id,
        cycle: cycle as i64,
        source: source.to_string(),
        input_atoms: opportunity.input_quote_atoms.to_string(),
        output_atoms: Some(opportunity.output_quote_atoms.to_string()),
        net_profit_atoms: Some(quote_delta_atoms(opportunity).to_string()),
        net_profit_bps: Some(opportunity.net_profit_bps),
        fee_atoms: None,
        slot: Some(opportunity.detected_slot),
        freshness_json,
        blocker,
        observed_at_unix_ms: now_ms,
        attempt_id: Some(attempt_id.to_string()),
    })?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn persist_focused_optimizer_rejection_evidence(
    db: &ArbDatabase,
    cycle: u64,
    route_edge_id: Option<i64>,
    buy_pool: &PoolState,
    sell_pool: &PoolState,
    min_executable_input_quote_atoms: u128,
    max_input_quote_atoms: u128,
    reason: &str,
    attempt_id: &str,
    now_ms: u128,
    network_fee: FeeBreakdown,
    quote_atom_usd_value: f64,
    safety_buffer_bps: f64,
    min_net_profit_bps: f64,
    min_absolute_profit_quote_atoms: u128,
    include_setup_cost: bool,
    setup_lamport_usd_value: f64,
) -> Result<()> {
    persist_focused_optimizer_rejection_evidence_with_reconciliation(
        db,
        cycle,
        route_edge_id,
        buy_pool,
        sell_pool,
        min_executable_input_quote_atoms,
        max_input_quote_atoms,
        reason,
        attempt_id,
        now_ms,
        network_fee,
        quote_atom_usd_value,
        safety_buffer_bps,
        min_net_profit_bps,
        min_absolute_profit_quote_atoms,
        include_setup_cost,
        setup_lamport_usd_value,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn persist_focused_optimizer_rejection_evidence_with_reconciliation(
    db: &ArbDatabase,
    cycle: u64,
    route_edge_id: Option<i64>,
    buy_pool: &PoolState,
    sell_pool: &PoolState,
    min_executable_input_quote_atoms: u128,
    max_input_quote_atoms: u128,
    reason: &str,
    attempt_id: &str,
    now_ms: u128,
    network_fee: FeeBreakdown,
    quote_atom_usd_value: f64,
    safety_buffer_bps: f64,
    min_net_profit_bps: f64,
    min_absolute_profit_quote_atoms: u128,
    include_setup_cost: bool,
    setup_lamport_usd_value: f64,
    fresh_quote_reconciliation: Option<&FreshQuoteReconciliationSnapshot>,
) -> Result<()> {
    let optimizer_decision = optimizer_rejection_trace(
        buy_pool,
        sell_pool,
        route_edge_id,
        min_executable_input_quote_atoms,
        max_input_quote_atoms,
        reason,
        network_fee.clone(),
        quote_atom_usd_value,
        safety_buffer_bps,
        min_net_profit_bps,
        min_absolute_profit_quote_atoms,
        include_setup_cost,
        setup_lamport_usd_value,
    );
    let opportunity = Opportunity {
        pair: TokenPair {
            base_mint: buy_pool.pair.base_mint.clone(),
            quote_mint: buy_pool.pair.quote_mint.clone(),
            base_decimals: buy_pool.pair.base_decimals,
            quote_decimals: buy_pool.pair.quote_decimals,
        },
        buy_venue: buy_pool.venue,
        sell_venue: sell_pool.venue,
        buy_pool: buy_pool.address.clone(),
        sell_pool: sell_pool.address.clone(),
        input_quote_atoms: max_input_quote_atoms,
        base_atoms: 0,
        output_quote_atoms: 0,
        gross_profit_quote_atoms: -i128::try_from(max_input_quote_atoms).unwrap_or(i128::MAX),
        net_profit_usd: 0.0,
        net_profit_bps: 0.0,
        spread_bps: 0.0,
        fee: network_fee,
        detected_slot: buy_pool.updated_slot.max(sell_pool.updated_slot),
        optimizer_decision: Some(optimizer_decision),
    };
    let fallback_reconciliation;
    let reconciliation = match fresh_quote_reconciliation {
        Some(reconciliation) => reconciliation,
        None => {
            fallback_reconciliation =
                fresh_quote_reconciliation_from_opportunity(&opportunity, reason, None, None);
            &fallback_reconciliation
        }
    };
    persist_quote_observation_evidence_with_reconciliation(
        db,
        cycle,
        &opportunity,
        route_edge_id,
        "focused_optimizer_rejection",
        Some(reason.to_string()),
        None,
        attempt_id,
        now_ms,
        Some(reconciliation),
    )
}

fn persist_simulation_evidence(
    db: &ArbDatabase,
    cycle: u64,
    opportunity: &Opportunity,
    attempt_id: &str,
    now_ms: u128,
) -> Result<()> {
    let route_edge_id = resolve_opportunity_route_edge_id(db, opportunity)?;
    let unmapped = route_edge_id.is_none();
    if unmapped {
        return Ok(());
    }
    let blocker =
        unmapped.then(|| "route edge mapping unavailable for paper simulation".to_string());
    persist_quote_observation_evidence(
        db,
        cycle,
        opportunity,
        route_edge_id,
        "paper_simulation",
        blocker.clone(),
        None,
        attempt_id,
        now_ms,
    )?;
    let route_context = serde_json::from_str::<serde_json::Value>(&route_context_json(
        opportunity,
        route_edge_id,
        blocker.as_deref(),
        None,
        None,
    )?)?;
    let mut logs_json = json!({
        "source": "paper_simulation",
        "route_edge_mapped": !unmapped,
        "route_context": route_context,
        "blocker": blocker,
    });
    if let Some(optimizer_decision) = &opportunity.optimizer_decision {
        logs_json["optimizer_decision"] = serde_json::to_value(optimizer_decision)?;
    }
    db.insert_simulation_result(&SimulationResultRecord {
        route_edge_id,
        cycle: cycle as i64,
        input_atoms: opportunity.input_quote_atoms.to_string(),
        pre_quote_atoms: Some(opportunity.input_quote_atoms.to_string()),
        post_quote_atoms: Some(opportunity.output_quote_atoms.to_string()),
        quote_delta_atoms: Some(quote_delta_atoms(opportunity).to_string()),
        net_profit_atoms: Some(quote_delta_atoms(opportunity).to_string()),
        net_profit_bps: Some(opportunity.net_profit_bps),
        tx_bytes: None,
        simulation_ok: !unmapped,
        simulation_error: blocker.clone(),
        logs_json: serde_json::to_string(&logs_json)?,
        simulated_at_unix_ms: now_ms,
        attempt_id: Some(attempt_id.to_string()),
    })?;
    let route_edge_id = route_edge_id.expect("mapped simulation result has route edge");
    if opportunity.net_profit_bps > 0.0 {
        let _ = db.mark_route_simulation_positive(route_edge_id);
    } else {
        let _ =
            db.mark_route_simulation_negative(route_edge_id, "paper simulation was not profitable");
    }
    Ok(())
}

fn persist_live_probe_evidence(
    db: &ArbDatabase,
    cycle: u64,
    opportunity: &Opportunity,
    report: Option<&AtomicArbReport>,
    error: Option<&str>,
    attempt_id: &str,
    config_hash: &str,
    now_ms: u128,
) -> Result<()> {
    let route_edge_id = resolve_opportunity_route_edge_id(db, opportunity)?;
    let unmapped_blocker = route_edge_id
        .is_none()
        .then(|| "route edge mapping unavailable for live probe".to_string());
    let report_reason = report.and_then(|report| {
        report
            .blocked_reason
            .clone()
            .or(report.confirmation_error.clone())
            .or(report.simulation_error.clone())
    });
    let blocker = error
        .map(ToOwned::to_owned)
        .or(report_reason)
        .or(unmapped_blocker.clone());
    persist_quote_observation_evidence(
        db,
        cycle,
        opportunity,
        route_edge_id,
        "live_probe",
        blocker.clone(),
        report
            .map(|report| report.latency_trace.as_slice())
            .filter(|trace| !trace.is_empty()),
        attempt_id,
        now_ms,
    )?;
    let status = match report.map(|report| &report.status) {
        Some(AtomicArbStatus::Blocked) => "blocked_requote",
        Some(AtomicArbStatus::Simulated) => "simulation_passed",
        Some(AtomicArbStatus::Submitted) => "submitted",
        Some(AtomicArbStatus::Confirmed) => "confirmed",
        Some(AtomicArbStatus::Failed) | None => "failed_probe",
    };
    let realized_profit_atoms_str = report
        .and_then(|report| report.requote_net_profit_bps)
        .map(|requote_bps| {
            let atoms =
                (opportunity.input_quote_atoms as f64 * requote_bps / 10_000.0).round() as i128;
            atoms.to_string()
        })
        .unwrap_or_else(|| quote_delta_atoms(opportunity).to_string());
    db.insert_execution_attempt(&ExecutionAttemptRecord {
        route_edge_id,
        cycle: cycle as i64,
        status: status.to_string(),
        tx_signature: report.and_then(|report| report.signature.clone()),
        input_atoms: Some(opportunity.input_quote_atoms.to_string()),
        estimated_profit_atoms: report
            .and_then(|report| report.native_simulation_net_profit_atoms.clone())
            .or_else(|| Some(quote_delta_atoms(opportunity).to_string())),
        realized_profit_atoms: Some(realized_profit_atoms_str),
        blocker,
        config_hash: config_hash.to_string(),
        wallet_pubkey: report.map(|report| report.wallet_pubkey.clone()),
        submitted_at_unix_ms: Some(now_ms),
        confirmed_at_unix_ms: report
            .is_some_and(|report| matches!(report.status, AtomicArbStatus::Confirmed))
            .then_some(now_ms),
        attempt_id: Some(attempt_id.to_string()),
    })?;
    if let Some(report) = report {
        if let Some(signature) = &report.signature {
            persist_live_settlement_evidence(
                db,
                opportunity,
                route_edge_id,
                report,
                signature,
                attempt_id,
                now_ms,
            )?;
        }
    }
    Ok(())
}

fn persist_live_settlement_evidence(
    db: &ArbDatabase,
    opportunity: &Opportunity,
    route_edge_id: Option<i64>,
    report: &AtomicArbReport,
    signature: &str,
    attempt_id: &str,
    now_ms: u128,
) -> Result<()> {
    let estimated_profit_atoms = report
        .native_simulation_net_profit_atoms
        .clone()
        .or_else(|| Some(quote_delta_atoms(opportunity).to_string()));
    let fee_quote_atoms = report.native_execution_cost_quote_atoms.clone();
    let realized_profit_atoms = estimated_realized_profit_atoms_for_report(
        &report.status,
        estimated_profit_atoms.as_deref(),
        fee_quote_atoms.as_deref(),
    );
    let classification =
        classify_live_settlement(&report.status, realized_profit_atoms.as_deref()).to_string();
    let meta_json = json!({
        "source": "live_report",
        "confirmation_status": report.confirmation_status,
        "confirmation_error": report.confirmation_error,
        "blocked_reason": report.blocked_reason,
        "requote_net_profit_bps": report.requote_net_profit_bps,
        "requote_net_profit_usd": report.requote_net_profit_usd,
        "native_execution_cost_quote_atoms": report.native_execution_cost_quote_atoms,
        "native_simulation_net_profit_atoms": report.native_simulation_net_profit_atoms,
        "latency_trace": report.latency_trace,
    });
    db.insert_live_settlement(&LiveSettlementRecord {
        route_edge_id,
        attempt_id: Some(attempt_id.to_string()),
        tx_signature: signature.to_string(),
        status: format!("{:?}", report.status).to_lowercase(),
        quote_mint: opportunity.pair.quote_mint.clone(),
        input_atoms: Some(opportunity.input_quote_atoms.to_string()),
        estimated_profit_atoms,
        realized_quote_delta_atoms: report.native_simulation_quote_delta_atoms.clone(),
        fee_lamports: None,
        fee_quote_atoms,
        realized_profit_atoms,
        classification,
        pre_balances_json: "{}".to_string(),
        post_balances_json: "{}".to_string(),
        meta_json: serde_json::to_string(&meta_json)?,
        settled_at_unix_ms: now_ms,
    })?;
    Ok(())
}

fn estimated_realized_profit_atoms_for_report(
    status: &AtomicArbStatus,
    estimated_profit_atoms: Option<&str>,
    fee_quote_atoms: Option<&str>,
) -> Option<String> {
    if matches!(status, AtomicArbStatus::Failed) {
        let fee = fee_quote_atoms
            .and_then(|value| value.parse::<i128>().ok())
            .unwrap_or_default();
        return Some((-fee).to_string());
    }
    estimated_profit_atoms.map(ToOwned::to_owned)
}

fn classify_live_settlement(
    status: &AtomicArbStatus,
    realized_profit_atoms: Option<&str>,
) -> &'static str {
    if matches!(status, AtomicArbStatus::Failed) {
        return "failed_fee_loss";
    }
    let realized = realized_profit_atoms
        .and_then(|value| value.parse::<i128>().ok())
        .unwrap_or_default();
    if realized > 0 {
        "landed_profit"
    } else if realized < 0 {
        "landed_loss"
    } else if matches!(status, AtomicArbStatus::Confirmed) {
        "break_even"
    } else {
        "pending"
    }
}

fn classify_confirmed_quote_delta(realized_delta_atoms: i128) -> &'static str {
    if realized_delta_atoms > 0 {
        "landed_profit"
    } else if realized_delta_atoms < 0 {
        "landed_loss"
    } else {
        "break_even"
    }
}

fn sol_fee_quote_atoms(fee_lamports: u64, quote_mint: &str, sol_usd_estimate: f64) -> i128 {
    if fee_lamports == 0 {
        return 0;
    }
    if quote_mint == WSOL_MINT {
        return 0;
    }
    let quote_decimals = if quote_mint == USDC_MINT || quote_mint == USDT_MINT {
        6
    } else {
        token_decimals_for_symbol("")
    };
    let quote_units = fee_lamports as f64 / 1_000_000_000.0 * sol_usd_estimate.max(0.0);
    (quote_units * 10f64.powi(quote_decimals as i32)).ceil() as i128
}

fn settlement_quote_balance_atoms(
    quote_mint: &str,
    token_balance_atoms: u128,
    sol_lamports: u64,
) -> u128 {
    if quote_mint == WSOL_MINT {
        sol_lamports.saturating_sub(live_sol_fee_reserve_lamports()) as u128
    } else {
        token_balance_atoms
    }
}

type RuntimeDatabase = Arc<std::sync::Mutex<ArbDatabase>>;

fn open_runtime_database(
    config: &BotConfig,
) -> (
    Option<RuntimeDatabase>,
    Option<DatabaseSummary>,
    Option<String>,
) {
    if !config.database.enabled {
        return (None, None, None);
    }
    let db = match ArbDatabase::open(&config.database.path, config.database.busy_timeout_ms) {
        Ok(db) => db,
        Err(error) => {
            let message = format!("runtime database open failed: {error:#}");
            warn!(error = %message);
            return (None, None, Some(message));
        }
    };
    if config.database.migrations_on_startup {
        if let Err(error) = db.run_migrations() {
            let message = format!("runtime database migration failed: {error:#}");
            warn!(error = %message);
            return (None, None, Some(message));
        }
    }
    let baseline = match db.summary() {
        Ok(summary) => Some(summary),
        Err(error) => {
            let message = format!("runtime database summary failed: {error:#}");
            warn!(error = %message);
            return (
                Some(Arc::new(std::sync::Mutex::new(db))),
                None,
                Some(message),
            );
        }
    };
    (Some(Arc::new(std::sync::Mutex::new(db))), baseline, None)
}

#[cfg(test)]
fn load_route_cooldown_state() -> HashMap<String, Instant> {
    HashMap::new()
}

#[cfg(not(test))]
fn load_route_cooldown_state() -> HashMap<String, Instant> {
    let Some(_guard) = route_cooldown_state_lock().lock().ok() else {
        return HashMap::new();
    };
    let now_ms = now_unix_ms();
    let now = Instant::now();
    fs::read_to_string(route_cooldown_state_path())
        .ok()
        .and_then(|text| serde_json::from_str::<HashMap<String, u128>>(&text).ok())
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(key, until_ms)| {
            if until_ms <= now_ms {
                return None;
            }
            let delta_ms = until_ms.saturating_sub(now_ms).min(u128::from(u64::MAX)) as u64;
            Some((key, now + Duration::from_millis(delta_ms)))
        })
        .collect()
}

#[cfg(test)]
fn persist_route_cooldown_state(_: &HashMap<String, Instant>) {}

#[cfg(not(test))]
fn persist_route_cooldown_state(cooldowns: &HashMap<String, Instant>) {
    let Some(_guard) = route_cooldown_state_lock().lock().ok() else {
        return;
    };
    let now = Instant::now();
    let now_ms = now_unix_ms();
    let state = cooldowns
        .iter()
        .filter_map(|(key, until)| {
            if *until <= now {
                return None;
            }
            Some((
                key.clone(),
                now_ms.saturating_add(until.duration_since(now).as_millis()),
            ))
        })
        .collect::<HashMap<_, _>>();
    let Ok(text) = serde_json::to_string(&state) else {
        return;
    };
    let _ = fs::write(route_cooldown_state_path(), text);
}

#[cfg(not(test))]
fn route_cooldown_state_path() -> String {
    env_value("LIVE_ROUTE_COOLDOWN_STATE_PATH")
        .unwrap_or_else(|| ".arb-live-route-cooldowns.json".to_string())
}

#[cfg(not(test))]
fn route_cooldown_state_lock() -> &'static StdMutex<()> {
    static LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| StdMutex::new(()))
}

fn token_account_amount_atoms(account: &Value) -> Option<(u128, u8)> {
    let token_amount = account.pointer("/account/data/parsed/info/tokenAmount")?;
    let amount = token_amount
        .get("amount")
        .and_then(Value::as_str)
        .and_then(|amount| amount.parse::<u128>().ok())?;
    let decimals = token_amount
        .get("decimals")
        .and_then(Value::as_u64)
        .and_then(|value| value.try_into().ok())
        .unwrap_or_else(|| token_decimals_for_symbol(""));
    Some((amount, decimals))
}

fn token_decimals_for_symbol(symbol: &str) -> u8 {
    match symbol {
        "USDC" | "USDT" => 6,
        "SOL" | "WSOL" => 9,
        symbol if symbol.ends_with('x') => 8,
        symbol if symbol.ends_with("on") => 9,
        symbol if symbol.ends_with('r') => 9,
        _ => 6,
    }
}

fn pool_has_decoded_reserves(pool: &PoolState) -> bool {
    pool.reserve_base_atoms > 0 && pool.reserve_quote_atoms > 0
}

fn pool_has_focused_route_price_basis(pool: &PoolState) -> bool {
    pool_has_decoded_reserves(pool)
        || (pool.reserve_base_atoms == 0
            && pool.reserve_quote_atoms == 0
            && pool.updated_slot == 0
            && pool.sqrt_price_x64.is_some()
            && pool.liquidity_quote_atoms > 0)
}

fn focused_route_account_max_age_ms(stale_candidate_ttl_ms: u64) -> u64 {
    stale_candidate_ttl_ms.max(FOCUSED_ROUTE_ACCOUNT_MAX_AGE_FLOOR_MS)
}

fn focused_route_prequote_refresh_max_age_ms(stale_candidate_ttl_ms: u64) -> u64 {
    if stale_candidate_ttl_ms == 0 {
        return 0;
    }
    let margin = stale_candidate_ttl_ms
        .saturating_mul(4)
        .saturating_div(5)
        .clamp(25, 200)
        .min(stale_candidate_ttl_ms);
    stale_candidate_ttl_ms.saturating_sub(margin)
}

fn hot_cache_entry_needs_refresh(
    cached: Option<&CachedAccount>,
    max_account_age_ms: u64,
    now: Instant,
) -> bool {
    let Some(cached) = cached else {
        return true;
    };
    if max_account_age_ms == 0 {
        return false;
    }
    now.saturating_duration_since(cached.updated_at).as_millis() > u128::from(max_account_age_ms)
}

fn route_account_requirements_from_pubkeys(pubkeys: &[String]) -> Vec<RouteAccountRequirement> {
    pubkeys
        .iter()
        .enumerate()
        .map(|(index, pubkey)| RouteAccountRequirement {
            role: format!("route.account.{index}"),
            pubkey: pubkey.clone(),
            expected_owner: None,
            required: true,
        })
        .collect()
}

fn reserve_backed_pool_addresses(pools: &[PoolState]) -> HashSet<String> {
    pools
        .iter()
        .filter(|pool| pool_has_decoded_reserves(pool))
        .map(|pool| pool.address.clone())
        .collect()
}

fn native_opportunity_uses_reserve_backed_pools(
    opportunity: &Opportunity,
    reserve_backed_pool_addresses: &HashSet<String>,
) -> bool {
    reserve_backed_pool_addresses.contains(&opportunity.buy_pool)
        && reserve_backed_pool_addresses.contains(&opportunity.sell_pool)
}

fn normalize_indexed_pool(
    mut pool: PoolState,
    candidate: &IndexedPoolCandidate,
) -> Option<PoolState> {
    if pool.pair.base_mint == candidate.base_mint && pool.pair.quote_mint == candidate.quote_mint {
        pool.pair.base_decimals = candidate.base_decimals;
        pool.pair.quote_decimals = candidate.quote_decimals;
        apply_index_price_guard(&mut pool, candidate);
        return Some(pool);
    }

    if pool.pair.base_mint == candidate.quote_mint && pool.pair.quote_mint == candidate.base_mint {
        std::mem::swap(&mut pool.reserve_base_atoms, &mut pool.reserve_quote_atoms);
        pool.sqrt_price_x64 = pool.sqrt_price_x64.and_then(invert_sqrt_price_x64);
        pool.pair.base_mint = candidate.base_mint.clone();
        pool.pair.quote_mint = candidate.quote_mint.clone();
        pool.pair.base_decimals = candidate.base_decimals;
        pool.pair.quote_decimals = candidate.quote_decimals;
        pool.liquidity_quote_atoms = pool.reserve_quote_atoms;
        apply_index_price_guard(&mut pool, candidate);
        return Some(pool);
    }

    None
}

fn apply_index_price_guard(pool: &mut PoolState, candidate: &IndexedPoolCandidate) {
    let Some(index_price) = candidate.price_quote_per_base else {
        return;
    };
    if price_is_close(pool.price_quote_per_base(), index_price) {
        return;
    }

    pool.reserve_base_atoms = 0;
    pool.reserve_quote_atoms = 0;
    pool.liquidity_quote_atoms = 0;
    pool.sqrt_price_x64 = sqrt_price_x64_from_price(
        index_price,
        candidate.base_decimals,
        candidate.quote_decimals,
    );
}

fn indexed_price_only_pool(
    account: &crate::types::RawAccount,
    candidate: &IndexedPoolCandidate,
) -> Option<PoolState> {
    let price = candidate.price_quote_per_base?;
    if price <= 0.0 || !price.is_finite() {
        return None;
    }
    let liquidity_quote_atoms = indexed_liquidity_quote_atoms(candidate)?;
    if liquidity_quote_atoms == 0 {
        return None;
    }

    Some(PoolState {
        venue: candidate.venue,
        kind: indexed_pool_kind(candidate.venue, &account.owner),
        address: candidate.address.clone(),
        pair: TokenPair {
            base_mint: candidate.base_mint.clone(),
            quote_mint: candidate.quote_mint.clone(),
            base_decimals: candidate.base_decimals,
            quote_decimals: candidate.quote_decimals,
        },
        reserve_base_atoms: 0,
        reserve_quote_atoms: 0,
        sqrt_price_x64: sqrt_price_x64_from_price(
            price,
            candidate.base_decimals,
            candidate.quote_decimals,
        ),
        active_bin_id: None,
        fee_bps: indexed_fallback_fee_bps(candidate.venue),
        protocol_fee_bps: 0,
        liquidity_quote_atoms,
        updated_slot: account.slot,
    })
}

fn indexed_liquidity_quote_atoms(candidate: &IndexedPoolCandidate) -> Option<u128> {
    let quote_ui = candidate
        .liquidity_quote_ui
        .or_else(|| stable_quote_mint(&candidate.quote_mint).then_some(candidate.liquidity_usd))?;
    if quote_ui <= 0.0 || !quote_ui.is_finite() {
        return None;
    }
    Some(ui_to_atoms(quote_ui, candidate.quote_decimals))
}

fn stable_quote_mint(mint: &str) -> bool {
    mint == USDC_MINT || mint == USDT_MINT
}

fn indexed_pool_kind(venue: Venue, owner: &str) -> PoolKind {
    match venue {
        Venue::Orca => PoolKind::ConcentratedLiquidity,
        Venue::Meteora if owner == METEORA_DLMM_PROGRAM => PoolKind::Dlmm,
        Venue::Meteora if owner == METEORA_DAMM_V2_PROGRAM => PoolKind::MeteoraDammV2,
        Venue::Meteora if owner == METEORA_POOLS_PROGRAM => PoolKind::MeteoraPools,
        Venue::Meteora => PoolKind::MeteoraPools,
        Venue::Raydium if owner == RAYDIUM_CLMM_PROGRAM => PoolKind::ConcentratedLiquidity,
        Venue::Raydium if owner == RAYDIUM_CPMM_PROGRAM => PoolKind::ConstantProduct,
        Venue::Raydium if owner == RAYDIUM_AMM_V4_PROGRAM => PoolKind::RaydiumAmmV4,
        Venue::Raydium => PoolKind::ConstantProduct,
        _ => PoolKind::ConstantProduct,
    }
}

fn indexed_fallback_fee_bps(venue: Venue) -> u16 {
    match venue {
        Venue::Orca => 30,
        Venue::Meteora => 30,
        Venue::Raydium => 25,
        _ => 30,
    }
}

fn price_is_close(decoded_price: Option<f64>, index_price: f64) -> bool {
    let Some(decoded_price) = decoded_price else {
        return false;
    };
    if decoded_price <= 0.0 || index_price <= 0.0 {
        return false;
    }
    ((decoded_price - index_price).abs() / index_price) <= 0.10
}

fn sqrt_price_x64_from_price(
    price_quote_per_base: f64,
    base_decimals: u8,
    quote_decimals: u8,
) -> Option<u128> {
    if price_quote_per_base <= 0.0 || !price_quote_per_base.is_finite() {
        return None;
    }
    let decimal_factor = 10f64.powi(base_decimals as i32 - quote_decimals as i32);
    let raw = price_quote_per_base / decimal_factor;
    if raw <= 0.0 || !raw.is_finite() {
        return None;
    }
    Some((raw.sqrt() * 2f64.powi(64)).floor().max(1.0) as u128)
}

fn invert_sqrt_price_x64(sqrt_price_x64: u128) -> Option<u128> {
    if sqrt_price_x64 == 0 {
        return None;
    }
    let q64 = 2f64.powi(64);
    let sqrt = sqrt_price_x64 as f64 / q64;
    if sqrt == 0.0 || !sqrt.is_finite() {
        return None;
    }
    Some(((1.0 / sqrt) * q64).floor().max(1.0) as u128)
}

fn short_mint(mint: &str) -> String {
    if mint.len() <= 8 {
        return mint.to_string();
    }
    format!("{}...{}", &mint[..4], &mint[mint.len() - 4..])
}

fn trade_size_quote_ui(pair: &TokenPair, trade_size_usd: f64, sol_usd_estimate: f64) -> f64 {
    if pair.quote_mint == WSOL_MINT {
        trade_size_usd / sol_usd_estimate.max(1.0)
    } else {
        trade_size_usd
    }
}

fn quote_usd_to_atoms(pair: &TokenPair, usd_amount: f64, sol_usd_estimate: f64) -> u128 {
    if !usd_amount.is_finite() || usd_amount <= 0.0 {
        return 0;
    }
    ui_to_atoms(
        trade_size_quote_ui(pair, usd_amount, sol_usd_estimate),
        pair.quote_decimals,
    )
}

fn optimized_route_min_input_quote_atoms(pair: &TokenPair, sol_usd_estimate: f64) -> u128 {
    ui_to_atoms(
        trade_size_quote_ui(pair, 0.01, sol_usd_estimate),
        pair.quote_decimals,
    )
    .max(1)
}

fn dlmm_min_executable_input_quote_atoms(pool: &PoolState) -> Option<u128> {
    if pool.kind != PoolKind::Dlmm {
        return None;
    }
    let price_quote_per_base = pool.price_quote_per_base()?;
    if !price_quote_per_base.is_finite() || price_quote_per_base <= 0.0 {
        return None;
    }
    let one_base_atom_in_quote_atoms = price_quote_per_base
        * 10f64.powi(pool.pair.quote_decimals as i32 - pool.pair.base_decimals as i32);
    let before_fee_atoms = ceil_positive_f64_to_u128(one_base_atom_in_quote_atoms)?;
    let total_fee_bps = u128::from(pool.fee_bps.saturating_add(pool.protocol_fee_bps)).min(9_999);
    let fee_denominator = 10_000u128.saturating_sub(total_fee_bps).max(1);
    Some(
        before_fee_atoms
            .saturating_mul(10_000)
            .saturating_add(fee_denominator.saturating_sub(1))
            / fee_denominator,
    )
}

fn ceil_positive_f64_to_u128(value: f64) -> Option<u128> {
    if !value.is_finite() || value <= 0.0 {
        return None;
    }
    if value >= u128::MAX as f64 {
        return Some(u128::MAX);
    }
    Some(value.ceil() as u128)
}

fn optimized_route_max_input_quote_atoms(
    config: &BotConfig,
    buy_pool: &PoolState,
    sol_usd_estimate: f64,
    quote_balance_cap_atoms: Option<u128>,
) -> u128 {
    let pair = &buy_pool.pair;
    let min_atoms = optimized_route_min_input_quote_atoms(pair, sol_usd_estimate);
    let max_usd = if config.execution.max_live_trade_usd.is_finite()
        && config.execution.max_live_trade_usd > 0.0
    {
        Some(config.execution.max_live_trade_usd)
    } else {
        None
    };
    let configured_max = if let Some(max_usd) = max_usd {
        ui_to_atoms(
            trade_size_quote_ui(pair, max_usd, sol_usd_estimate),
            pair.quote_decimals,
        )
        .max(min_atoms)
    } else {
        let reserve_bound = buy_pool.reserve_quote_atoms.saturating_mul(2_000) / 10_000;
        let liquidity_bound = buy_pool.liquidity_quote_atoms.saturating_mul(2_000) / 10_000;
        [reserve_bound, liquidity_bound]
            .into_iter()
            .filter(|bound| *bound > 0)
            .min()
            .unwrap_or(min_atoms)
            .max(min_atoms)
    };

    quote_balance_cap_atoms
        .map(|cap| configured_max.min(cap))
        .unwrap_or(configured_max)
}

fn quote_mint_usd_value(pair: &TokenPair, sol_usd_estimate: f64) -> f64 {
    if pair.quote_mint == WSOL_MINT {
        sol_usd_estimate
    } else {
        1.0
    }
}

fn decimals_for_quote_mint(mint: &str) -> u8 {
    match mint {
        USDC_MINT | USDT_MINT => 6,
        WSOL_MINT => 9,
        _ => 6,
    }
}

fn live_sol_fee_reserve_lamports() -> u64 {
    env_value("LIVE_SOL_FEE_RESERVE_LAMPORTS")
        .and_then(|value| value.parse().ok())
        .unwrap_or(5_000_000)
}

fn spendable_wsol_quote_balance_atoms(
    sol_lamports: u64,
    prefunded_token_atoms: Option<u128>,
    native_prefunded_hot_accounts: bool,
) -> u128 {
    if native_prefunded_hot_accounts {
        prefunded_token_atoms.unwrap_or(0)
    } else {
        sol_lamports.saturating_sub(live_sol_fee_reserve_lamports()) as u128
    }
}

#[derive(Debug, Clone, Default)]
struct FundingBlockedSummary {
    blocked_count: usize,
    best_required_usd: Option<f64>,
    best_available_usd: Option<f64>,
}

fn fundable_live_candidates(
    candidates: &[Opportunity],
    balances: &HashMap<String, u128>,
    sol_usd_estimate: f64,
) -> (Vec<Opportunity>, FundingBlockedSummary) {
    let mut fundable = Vec::new();
    let mut summary = FundingBlockedSummary::default();
    let mut best_required_usd = 0.0;
    for candidate in candidates {
        let available = balances
            .get(&candidate.pair.quote_mint)
            .copied()
            .unwrap_or_default();
        if available >= candidate.input_quote_atoms {
            fundable.push(candidate.clone());
            continue;
        }

        summary.blocked_count = summary.blocked_count.saturating_add(1);
        let required_usd = input_quote_usd(candidate, sol_usd_estimate);
        if required_usd >= best_required_usd {
            best_required_usd = required_usd;
            summary.best_required_usd = Some(required_usd);
            summary.best_available_usd = Some(
                atoms_to_ui(available, candidate.pair.quote_decimals)
                    * quote_mint_usd_value(&candidate.pair, sol_usd_estimate),
            );
        }
    }
    (fundable, summary)
}

fn diverse_live_probe_selection(
    ranked: Vec<(usize, f64, usize, String)>,
    candidates: &[Opportunity],
    limit: usize,
) -> Vec<(usize, f64, usize, String)> {
    if limit == 0 || ranked.is_empty() {
        return Vec::new();
    }
    let per_pair_limit = live_probe_max_candidates_per_pair();
    let mut selected = Vec::new();
    let mut pair_counts = HashMap::<String, usize>::new();
    let mut deferred = Vec::new();

    for item in ranked {
        if selected.len() >= limit {
            break;
        }
        let pair_key = candidates[item.0].pair.key();
        let count = pair_counts.entry(pair_key).or_default();
        if *count < per_pair_limit {
            *count += 1;
            selected.push(item);
        } else {
            deferred.push(item);
        }
    }

    for item in deferred {
        if selected.len() >= limit {
            break;
        }
        selected.push(item);
    }

    selected
}

fn live_probe_max_candidates_per_pair() -> usize {
    env_value("JUPITER_PROBE_MAX_CANDIDATES_PER_PAIR")
        .and_then(|value| value.parse().ok())
        .unwrap_or(1)
        .max(1)
}

fn probe_trade_sizes_usd(config: &ExecutionConfig, default_trade_size_usd: f64) -> Vec<f64> {
    configured_trade_sizes(&config.jupiter_probe_trade_sizes_usd)
        .or_else(|| {
            env_value("JUPITER_PROBE_TRADE_SIZE_USD_LADDER")
                .map(|value| parse_probe_trade_sizes(&value, default_trade_size_usd))
                .filter(|sizes| !sizes.is_empty())
        })
        .unwrap_or_else(|| vec![default_trade_size_usd.max(0.01)])
}

fn parse_probe_trade_sizes(value: &str, default_trade_size_usd: f64) -> Vec<f64> {
    let mut sizes = value
        .split(',')
        .filter_map(|part| part.trim().parse::<f64>().ok())
        .filter(|size| size.is_finite() && *size > 0.0)
        .collect::<Vec<_>>();
    if sizes.is_empty() {
        sizes.push(default_trade_size_usd.max(0.01));
    }
    sizes.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    sizes.dedup_by(|a, b| (*a - *b).abs() < 0.000001);
    sizes
}

fn live_candidate_key(opportunity: &Opportunity) -> String {
    format!(
        "{}|{}|{}|{}|{}",
        opportunity.pair.base_mint,
        opportunity.pair.quote_mint,
        opportunity.buy_venue,
        opportunity.sell_venue,
        opportunity.input_quote_atoms
    )
}

fn uses_jupiter_arbitrage_requote(opportunity: &Opportunity) -> bool {
    !NativeSwapBuilder::is_native_candidate(opportunity)
        && !ApiSwapBuilder::is_api_candidate(opportunity)
}

fn jupiter_requote_buy_leg_key(opportunity: &Opportunity) -> String {
    format!(
        "buy|{}|{}|{}|{}",
        opportunity.pair.quote_mint,
        opportunity.pair.base_mint,
        opportunity.buy_venue,
        opportunity.input_quote_atoms
    )
}

fn jupiter_requote_sell_leg_key(opportunity: &Opportunity) -> String {
    format!(
        "sell|{}|{}|{}|{}",
        opportunity.pair.base_mint,
        opportunity.pair.quote_mint,
        opportunity.sell_venue,
        opportunity.base_atoms
    )
}

fn jupiter_route_leg_cooldown_until(
    opportunity: &Opportunity,
    route_cooldowns: &HashMap<String, Instant>,
) -> Option<Instant> {
    if !uses_jupiter_arbitrage_requote(opportunity) {
        return None;
    }
    [
        jupiter_requote_buy_leg_key(opportunity),
        jupiter_requote_sell_leg_key(opportunity),
    ]
    .into_iter()
    .filter_map(|key| route_cooldowns.get(&key).copied())
    .max()
}

fn structural_route_cooldown_key(opportunity: &Opportunity) -> String {
    format!(
        "structural|{}|{}|{}|{}|{}|{}",
        opportunity.pair.base_mint,
        opportunity.pair.quote_mint,
        opportunity.buy_venue,
        opportunity.sell_venue,
        opportunity.buy_pool,
        opportunity.sell_pool
    )
}

fn structural_pair_venue_cooldown_key(opportunity: &Opportunity) -> String {
    format!(
        "structural-pair-venue|{}|{}|{}|{}",
        opportunity.pair.base_mint,
        opportunity.pair.quote_mint,
        opportunity.buy_venue,
        opportunity.sell_venue,
    )
}

fn structural_pair_cooldown_key(opportunity: &Opportunity) -> String {
    format!(
        "structural-pair|{}|{}",
        opportunity.pair.base_mint, opportunity.pair.quote_mint,
    )
}

fn structural_route_cooldown_keys(opportunity: &Opportunity) -> Vec<String> {
    vec![
        structural_route_cooldown_key(opportunity),
        structural_pair_venue_cooldown_key(opportunity),
        structural_pair_cooldown_key(opportunity),
    ]
}

fn structural_route_cooldown_keys_for_reason(
    opportunity: &Opportunity,
    reason: &str,
) -> Vec<String> {
    if is_native_zero_output_fresh_quote_error(reason) {
        return vec![structural_route_cooldown_key(opportunity)];
    }
    let mut keys = vec![
        structural_route_cooldown_key(opportunity),
        structural_pair_venue_cooldown_key(opportunity),
    ];
    if is_slippage_error(reason) {
        keys.push(structural_pair_cooldown_key(opportunity));
    }
    keys
}

fn structural_route_cooldown_until(
    opportunity: &Opportunity,
    route_cooldowns: &HashMap<String, Instant>,
) -> Option<Instant> {
    structural_route_cooldown_keys(opportunity)
        .into_iter()
        .filter_map(|key| route_cooldowns.get(&key).copied())
        .max()
}

fn structural_route_cooldown_duration(reason: &str) -> Option<Duration> {
    if is_transaction_too_large_error(reason) {
        return Some(Duration::from_secs(180));
    }
    if is_native_zero_output_fresh_quote_error(reason) {
        return Some(Duration::from_secs(10));
    }
    if is_unsupported_native_pool_error(reason) {
        return Some(unsupported_native_pool_cooldown_duration());
    }
    if is_slippage_error(reason) {
        return Some(dex_execution_error_cooldown_duration());
    }
    None
}

fn jupiter_no_route_leg_keys_for_reason(opportunity: &Opportunity, reason: &str) -> Vec<String> {
    if !uses_jupiter_arbitrage_requote(opportunity) || !is_jupiter_no_route_error(reason) {
        return Vec::new();
    }
    let lower = reason.to_ascii_lowercase();
    if lower.contains("buy leg") {
        vec![jupiter_requote_buy_leg_key(opportunity)]
    } else if lower.contains("sell leg") {
        vec![jupiter_requote_sell_leg_key(opportunity)]
    } else {
        vec![
            jupiter_requote_buy_leg_key(opportunity),
            jupiter_requote_sell_leg_key(opportunity),
        ]
    }
}

fn rwa_route_leg_cooldown_key(leg: &RwaExecutionLeg) -> Option<String> {
    let route = leg.route.trim();
    if route.is_empty()
        || !(route.starts_with("jupiter-route:")
            || route.starts_with("jupiter-v2-order:")
            || rwa_api_route_uses_jupiter_fallback(route))
    {
        return None;
    }
    Some(format!(
        "rwa-leg|{}|{}|{}",
        leg.input_mint, leg.output_mint, route
    ))
}

fn rwa_route_leg_cooldown_keys_for_reason(
    opportunity: &RwaOpportunity,
    reason: &str,
) -> Vec<String> {
    if rwa_route_leg_cooldown_duration_for_reason(reason).is_none() {
        return Vec::new();
    }
    opportunity
        .execution_legs
        .iter()
        .filter_map(rwa_route_leg_cooldown_key)
        .collect()
}

fn rwa_route_leg_cooldown_duration_for_reason(reason: &str) -> Option<Duration> {
    if is_jupiter_no_route_error(reason) {
        return Some(jupiter_no_route_leg_cooldown_duration());
    }
    if is_below_required_profit_error(reason) {
        return Some(rwa_negative_requote_cooldown_duration());
    }
    None
}

fn rwa_route_leg_cooldown_until(
    opportunity: &RwaOpportunity,
    route_cooldowns: &HashMap<String, Instant>,
) -> Option<Instant> {
    opportunity
        .execution_legs
        .iter()
        .filter_map(rwa_route_leg_cooldown_key)
        .filter_map(|key| route_cooldowns.get(&key).copied())
        .max()
}

fn jupiter_no_route_leg_cooldown_duration() -> Duration {
    env_value("JUPITER_NO_ROUTE_LEG_COOLDOWN_SECS")
        .and_then(|value| value.parse::<u64>().ok())
        .map(|secs| secs.clamp(10, 3_600))
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(180))
}

fn candidate_probe_info(
    opportunity: &Opportunity,
    key: String,
    sol_usd_estimate: f64,
) -> CandidateProbeInfo {
    let optimizer_decision = opportunity.optimizer_decision.as_ref();
    CandidateProbeInfo {
        key,
        pair: opportunity.pair.clone(),
        buy_venue: opportunity.buy_venue,
        sell_venue: opportunity.sell_venue,
        input_quote_atoms: opportunity.input_quote_atoms,
        input_usd: input_quote_usd(opportunity, sol_usd_estimate),
        optimizer_decision_id: optimizer_decision
            .map(|decision| decision.optimizer_decision_id.clone()),
        optimizer_selected_input_quote_atoms: optimizer_decision
            .map(|decision| decision.selected_input_quote_atoms.to_string()),
        optimizer_net_profit_quote_atoms: optimizer_decision
            .map(|decision| decision.net_profit_quote_atoms.to_string()),
        optimizer_net_profit_usd: optimizer_decision.map(|decision| decision.net_profit_usd),
        optimizer_net_profit_bps: optimizer_decision.map(|decision| decision.net_profit_bps),
        optimizer_route_edge_id: optimizer_decision.and_then(|decision| decision.route_edge_id),
        optimizer_buy_pool: optimizer_decision.map(|decision| decision.buy_pool.clone()),
        optimizer_sell_pool: optimizer_decision.map(|decision| decision.sell_pool.clone()),
        optimizer_buy_venue: optimizer_decision.and_then(|decision| decision.buy_venue),
        optimizer_sell_venue: optimizer_decision.and_then(|decision| decision.sell_venue),
    }
}

fn input_quote_usd(opportunity: &Opportunity, sol_usd_estimate: f64) -> f64 {
    atoms_to_ui(
        opportunity.input_quote_atoms,
        opportunity.pair.quote_decimals,
    ) * quote_mint_usd_value(&opportunity.pair, sol_usd_estimate)
}

fn probe_snapshot_from_opportunity(
    opportunity: &Opportunity,
    key: String,
    stats: Option<&CandidateProbeStats>,
    now: Instant,
    sol_usd_estimate: f64,
) -> ProbeCandidateSnapshot {
    let info = candidate_probe_info(opportunity, key, sol_usd_estimate);
    match stats {
        Some(stats) => probe_snapshot_from_info(&info, info.key.clone(), stats, now),
        None => ProbeCandidateSnapshot {
            key: info.key,
            pair: info.pair,
            buy_venue: info.buy_venue,
            sell_venue: info.sell_venue,
            input_quote_atoms: info.input_quote_atoms,
            input_usd: info.input_usd,
            attempts: 0,
            blocks: 0,
            no_route_blocks: 0,
            last_requote_bps: None,
            best_requote_bps: None,
            last_reason: None,
            last_checked_unix_ms: None,
            cooldown_remaining_ms: None,
            optimizer_decision_id: info.optimizer_decision_id,
            optimizer_selected_input_quote_atoms: info.optimizer_selected_input_quote_atoms,
            optimizer_net_profit_quote_atoms: info.optimizer_net_profit_quote_atoms,
            optimizer_net_profit_usd: info.optimizer_net_profit_usd,
            optimizer_net_profit_bps: info.optimizer_net_profit_bps,
            optimizer_route_edge_id: info.optimizer_route_edge_id,
            optimizer_buy_pool: info.optimizer_buy_pool,
            optimizer_sell_pool: info.optimizer_sell_pool,
            optimizer_buy_venue: info.optimizer_buy_venue,
            optimizer_sell_venue: info.optimizer_sell_venue,
        },
    }
}

fn probe_snapshot_from_info(
    info: &CandidateProbeInfo,
    key: String,
    stats: &CandidateProbeStats,
    now: Instant,
) -> ProbeCandidateSnapshot {
    ProbeCandidateSnapshot {
        key,
        pair: info.pair.clone(),
        buy_venue: info.buy_venue,
        sell_venue: info.sell_venue,
        input_quote_atoms: info.input_quote_atoms,
        input_usd: info.input_usd,
        attempts: stats.attempts,
        blocks: stats.blocks,
        no_route_blocks: stats.no_route_blocks,
        last_requote_bps: stats.last_requote_bps,
        best_requote_bps: stats.best_requote_bps,
        last_reason: stats.last_reason.clone(),
        last_checked_unix_ms: stats.last_checked_unix_ms,
        cooldown_remaining_ms: stats
            .cooldown_until
            .filter(|until| *until > now)
            .map(|until| (until - now).as_millis() as u64),
        optimizer_decision_id: info.optimizer_decision_id.clone(),
        optimizer_selected_input_quote_atoms: info.optimizer_selected_input_quote_atoms.clone(),
        optimizer_net_profit_quote_atoms: info.optimizer_net_profit_quote_atoms.clone(),
        optimizer_net_profit_usd: info.optimizer_net_profit_usd,
        optimizer_net_profit_bps: info.optimizer_net_profit_bps,
        optimizer_route_edge_id: info.optimizer_route_edge_id,
        optimizer_buy_pool: info.optimizer_buy_pool.clone(),
        optimizer_sell_pool: info.optimizer_sell_pool.clone(),
        optimizer_buy_venue: info.optimizer_buy_venue,
        optimizer_sell_venue: info.optimizer_sell_venue,
    }
}

fn probe_candidate_priority(stats: Option<&CandidateProbeStats>) -> f64 {
    let Some(stats) = stats else {
        return 1_000_000.0;
    };

    let bps = stats
        .best_requote_bps
        .or(stats.last_requote_bps)
        .unwrap_or(-500.0)
        .clamp(-1_000.0, 1_000.0);
    10_000.0 + bps * 100.0 - stats.no_route_blocks as f64 * 100.0 - stats.blocks as f64 * 5.0
}

fn direct_signal_priority(opportunity: &Opportunity) -> f64 {
    let profit_bps = opportunity.net_profit_bps.max(0.0).min(10_000.0);
    let profit_usd = opportunity.net_profit_usd.max(0.0).min(1_000.0);
    profit_bps * 10.0 + profit_usd
}

fn direct_opportunity_trade_sizes_usd(
    config: &ExecutionConfig,
    default_trade_size_usd: f64,
) -> Vec<f64> {
    configured_trade_sizes(&config.direct_trade_sizes_usd)
        .or_else(|| {
            env_value("DIRECT_OPPORTUNITY_TRADE_SIZE_USD_LADDER")
                .or_else(|| env_value("LIVE_TRADE_SIZE_USD_LADDER"))
                .or_else(|| env_value("JUPITER_PROBE_TRADE_SIZE_USD_LADDER"))
                .map(|value| parse_probe_trade_sizes(&value, default_trade_size_usd))
                .filter(|sizes| !sizes.is_empty())
        })
        .unwrap_or_else(|| vec![default_trade_size_usd.max(0.01)])
}

fn select_hot_subscription_pubkeys(
    routes: &[HotSubscriptionRoute],
    fallback_pubkeys: &[String],
    focus_routes: &[crate::config::NativeFocusRoute],
    budget: usize,
) -> HotSubscriptionSelection {
    if budget == 0 {
        return HotSubscriptionSelection::default();
    }
    let mut ranked = Vec::<(i64, usize, String, String)>::new();
    let mut ordinal = 0usize;
    for route in routes {
        let focus = hot_route_matches_focus(route, focus_routes);
        let readiness_score = hot_readiness_score(route.readiness_state);
        let focus_score = if focus { 10_000 } else { 0 };
        let reason = if focus {
            format!(
                "focus route; readiness={}",
                route
                    .readiness_state
                    .map(RouteReadinessState::as_str)
                    .unwrap_or("unmapped")
            )
        } else {
            format!(
                "selected candidate; readiness={}",
                route
                    .readiness_state
                    .map(RouteReadinessState::as_str)
                    .unwrap_or("unmapped")
            )
        };
        for pubkey in &route.pubkeys {
            if !is_hot_subscription_pubkey(pubkey) {
                continue;
            }
            ranked.push((
                focus_score + readiness_score,
                ordinal,
                pubkey.clone(),
                reason.clone(),
            ));
            ordinal += 1;
        }
    }
    for pubkey in fallback_pubkeys {
        if !is_hot_subscription_pubkey(pubkey) {
            continue;
        }
        ranked.push((
            0,
            ordinal,
            pubkey.clone(),
            "broad fallback promoted pool".to_string(),
        ));
        ordinal += 1;
    }
    ranked.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));

    let mut seen = HashSet::new();
    let mut selected = HotSubscriptionSelection {
        budget,
        ..HotSubscriptionSelection::default()
    };
    for (_, _, pubkey, reason) in ranked {
        if selected.pubkeys.len() >= budget {
            break;
        }
        if seen.insert(pubkey.clone()) {
            selected.pubkeys.push(pubkey);
            selected.reasons.push(reason);
        }
    }
    selected.desired_count = selected.pubkeys.len();
    selected.budget_remaining = budget.saturating_sub(selected.pubkeys.len());
    selected
}

fn is_hot_subscription_pubkey(pubkey: &str) -> bool {
    !pubkey.starts_with("jupiter-probe:")
}

fn hot_route_matches_focus(
    route: &HotSubscriptionRoute,
    focus_routes: &[crate::config::NativeFocusRoute],
) -> bool {
    focus_routes.iter().any(|focus| {
        focus_route_parts_match(&route.pair, route.buy_venue, route.sell_venue, focus)
            && (!focus.require_fresh_accounts
                || matches!(
                    route.readiness_state,
                    Some(
                        RouteReadinessState::FreshAccountsReady
                            | RouteReadinessState::FreshQuoteReady
                            | RouteReadinessState::SimulationPositive
                            | RouteReadinessState::SubmitEligible
                    )
                ))
    })
}

fn opportunity_matches_focus_parts(
    pair: &TokenPair,
    buy_venue: Venue,
    sell_venue: Venue,
    focus_routes: &[crate::config::NativeFocusRoute],
) -> bool {
    focus_routes
        .iter()
        .any(|focus| focus_route_parts_match(pair, buy_venue, sell_venue, focus))
}

fn matching_focus_route_requires_fresh_accounts(
    pair: &TokenPair,
    buy_venue: Venue,
    sell_venue: Venue,
    focus_routes: &[crate::config::NativeFocusRoute],
) -> bool {
    focus_routes.iter().any(|focus| {
        focus.require_fresh_accounts && focus_route_parts_match(pair, buy_venue, sell_venue, focus)
    })
}

fn focus_route_parts_match(
    pair: &TokenPair,
    buy_venue: Venue,
    sell_venue: Venue,
    focus: &crate::config::NativeFocusRoute,
) -> bool {
    pair.base_mint == focus.base_mint
        && pair.quote_mint == focus.quote_mint
        && buy_venue.to_string().eq_ignore_ascii_case(&focus.buy_venue)
        && sell_venue
            .to_string()
            .eq_ignore_ascii_case(&focus.sell_venue)
}

fn hot_readiness_score(readiness: Option<RouteReadinessState>) -> i64 {
    match readiness {
        Some(RouteReadinessState::SubmitEligible) => 700,
        Some(RouteReadinessState::SimulationPositive) => 650,
        Some(RouteReadinessState::FreshQuoteReady) => 625,
        Some(RouteReadinessState::FreshAccountsReady) => 600,
        Some(RouteReadinessState::GraphComplete) => 300,
        Some(RouteReadinessState::GraphIncomplete) => 100,
        Some(RouteReadinessState::CooledDown) => 50,
        Some(RouteReadinessState::PriceOnly) | None => 25,
    }
}

fn configured_trade_sizes(values: &[f64]) -> Option<Vec<f64>> {
    let mut sizes = values
        .iter()
        .copied()
        .filter(|size| size.is_finite() && *size > 0.0)
        .collect::<Vec<_>>();
    if sizes.is_empty() {
        return None;
    }
    sizes.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    sizes.dedup_by(|a, b| (*a - *b).abs() < 0.000001);
    Some(sizes)
}

fn probe_cooldown_duration(
    reason: Option<&str>,
    requote_bps: Option<f64>,
    failed: bool,
) -> Option<Duration> {
    if reason.is_some_and(is_jupiter_no_route_error) {
        return Some(Duration::from_secs(60));
    }
    if reason.is_some_and(is_transaction_too_large_error) {
        return Some(Duration::from_secs(180));
    }
    if reason.is_some_and(is_focused_route_preflight_error) {
        return Some(Duration::from_secs(30));
    }
    if reason.is_some_and(is_native_fresh_quote_unavailable_error) {
        return Some(Duration::from_secs(10));
    }
    if reason.is_some_and(is_unsupported_native_pool_error) {
        return Some(unsupported_native_pool_cooldown_duration());
    }
    if reason.is_some_and(is_slippage_error) {
        return Some(dex_execution_error_cooldown_duration());
    }
    if reason.is_some_and(is_below_required_profit_error) {
        let bps = requote_bps.unwrap_or_default();
        let secs = if bps < 0.0 {
            negative_requote_cooldown_duration().as_secs()
        } else if bps < 100.0 {
            60
        } else if bps < 250.0 {
            30
        } else {
            10
        };
        return Some(Duration::from_secs(secs));
    }
    if failed {
        return Some(Duration::from_secs(20));
    }
    match requote_bps {
        Some(bps) if bps < -100.0 => Some(Duration::from_secs(45)),
        Some(bps) if bps < -25.0 => Some(Duration::from_secs(25)),
        Some(bps) if bps < 0.0 => Some(Duration::from_secs(10)),
        _ => None,
    }
}

fn dex_execution_error_cooldown_duration() -> Duration {
    env_value("LIVE_DEX_ERROR_COOLDOWN_SECS")
        .and_then(|value| value.parse::<u64>().ok())
        .map(|secs| secs.clamp(60, 3_600))
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(600))
}

fn negative_requote_cooldown_duration() -> Duration {
    env_value("LIVE_NEGATIVE_REQUOTE_COOLDOWN_SECS")
        .and_then(|value| value.parse::<u64>().ok())
        .map(|secs| secs.clamp(30, 1_800))
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(180))
}

fn rwa_negative_requote_cooldown_duration() -> Duration {
    env_value("RWA_NEGATIVE_REQUOTE_COOLDOWN_SECS")
        .and_then(|value| value.parse::<u64>().ok())
        .map(|secs| secs.clamp(5, 120))
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(15))
}

fn should_repeat_confirmed_rwa_candidate(
    confirmed: bool,
    ran_after_attempt: usize,
    trade_limit: usize,
    finite_trade_limit: bool,
) -> bool {
    confirmed && finite_trade_limit && ran_after_attempt < trade_limit
}

fn unsupported_native_pool_cooldown_duration() -> Duration {
    env_value("LIVE_UNSUPPORTED_NATIVE_POOL_COOLDOWN_SECS")
        .and_then(|value| value.parse::<u64>().ok())
        .map(|secs| secs.clamp(300, 86_400))
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(3_600))
}

fn is_jupiter_no_route_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("no_routes_found")
        || lower.contains("no routes found")
        || lower.contains("could not find any route")
        || lower.contains("no route")
}

fn is_focused_route_preflight_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("focused route") && lower.contains("preflight failed")
}

fn is_native_fresh_quote_unavailable_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("native direct fresh")
        && (lower.contains("quote unavailable") || lower.contains("quote produced zero output"))
}

fn is_native_strict_cache_stale_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("native strict ws cache stale account")
        && lower.contains("age")
        && lower.contains("ttl")
}

fn is_native_zero_output_fresh_quote_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("native direct fresh") && lower.contains("quote produced zero output")
}

fn is_transaction_too_large_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("base64 encoded too large")
        || lower.contains("transaction is too large")
        || lower.contains("route is too large")
        || lower.contains("above live max")
}

fn is_unsupported_native_pool_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("unsupported native") && lower.contains("pool owner")
}

fn is_below_required_profit_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("below required")
        && (lower.contains("re-quote")
            || lower.contains("guaranteed net profit")
            || lower.contains("would reduce net profit"))
}

fn is_slippage_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("exceededslippage")
        || lower.contains("exceeded slippage")
        || lower.contains("\"custom\":6002")
        || lower.contains("\"custom\":6004")
        || lower.contains("\"custom\":6005")
        || lower.contains("\"custom\":6038")
        || lower.contains("custom program error: 0x1772")
        || lower.contains("custom program error: 0x1774")
        || lower.contains("custom program error: 0x1775")
        || lower.contains("custom program error: 0x1796")
}

fn simulated_trade_from_opportunity(
    id: u64,
    cycle: u64,
    simulated_unix_ms: u128,
    opportunity: &Opportunity,
) -> SimulatedTrade {
    let profitable = opportunity.net_profit_usd > 0.0;
    SimulatedTrade {
        id,
        cycle,
        simulated_unix_ms,
        pair: opportunity.pair.clone(),
        buy_venue: opportunity.buy_venue,
        sell_venue: opportunity.sell_venue,
        buy_pool: opportunity.buy_pool.clone(),
        sell_pool: opportunity.sell_pool.clone(),
        input_quote_atoms: opportunity.input_quote_atoms,
        base_atoms: opportunity.base_atoms,
        output_quote_atoms: opportunity.output_quote_atoms,
        expected_net_profit_usd: opportunity.net_profit_usd,
        simulated_net_profit_usd: opportunity.net_profit_usd,
        simulated_profit_bps: opportunity.net_profit_bps,
        profitable,
        status: if profitable {
            SimulatedTradeStatus::Profitable
        } else {
            SimulatedTradeStatus::Unprofitable
        },
        notes: "paper trade only; no transaction was signed or submitted".to_string(),
    }
}

fn live_record_from_report(id: u64, cycle: u64, report: &AtomicArbReport) -> LiveExecutionRecord {
    LiveExecutionRecord {
        id,
        cycle,
        submitted_unix_ms: now_unix_ms(),
        pair: report.pair.clone(),
        buy_venue: report.buy_venue,
        sell_venue: report.sell_venue,
        input_quote_atoms: report.input_quote_atoms,
        direct_signal_net_profit_usd: report.direct_signal_net_profit_usd,
        requote_net_profit_usd: report.requote_net_profit_usd,
        requote_net_profit_bps: report.requote_net_profit_bps,
        status: report.status.clone(),
        submitted: report.submitted,
        simulation_ok: report.simulation_ok,
        signature: report.signature.clone(),
        solscan_url: report.solscan_url.clone(),
        reason: report
            .blocked_reason
            .clone()
            .or(report.confirmation_error.clone()),
        jupiter_reference_net_profit_bps: report.jupiter_reference_net_profit_bps,
        jupiter_reference_error: report.jupiter_reference_error.clone(),
        native_execution_cost_quote_atoms: report.native_execution_cost_quote_atoms.clone(),
        native_setup_missing_atas: report.native_setup_missing_atas,
        native_simulation_net_profit_atoms: report.native_simulation_net_profit_atoms.clone(),
        native_simulation_pre_quote_atoms: report.native_simulation_pre_quote_atoms.clone(),
        native_simulation_post_quote_atoms: report.native_simulation_post_quote_atoms.clone(),
        native_simulation_quote_delta_atoms: report.native_simulation_quote_delta_atoms.clone(),
        signed_transaction_bytes: Some(report.signed_transaction_bytes),
        latency_trace: report.latency_trace.clone(),
    }
}

fn failed_live_record_from_opportunity(
    id: u64,
    cycle: u64,
    opportunity: &Opportunity,
    error: &anyhow::Error,
) -> LiveExecutionRecord {
    LiveExecutionRecord {
        id,
        cycle,
        submitted_unix_ms: now_unix_ms(),
        pair: opportunity.pair.clone(),
        buy_venue: opportunity.buy_venue,
        sell_venue: opportunity.sell_venue,
        input_quote_atoms: opportunity.input_quote_atoms,
        direct_signal_net_profit_usd: opportunity.net_profit_usd,
        requote_net_profit_usd: None,
        requote_net_profit_bps: None,
        status: AtomicArbStatus::Failed,
        submitted: false,
        simulation_ok: false,
        signature: None,
        solscan_url: None,
        reason: Some(format!("{error:#}")),
        jupiter_reference_net_profit_bps: None,
        jupiter_reference_error: None,
        native_execution_cost_quote_atoms: None,
        native_setup_missing_atas: None,
        native_simulation_net_profit_atoms: None,
        native_simulation_pre_quote_atoms: None,
        native_simulation_post_quote_atoms: None,
        native_simulation_quote_delta_atoms: None,
        signed_transaction_bytes: None,
        latency_trace: Vec::new(),
    }
}

fn blocked_live_record_from_opportunity(
    id: u64,
    cycle: u64,
    opportunity: &Opportunity,
    reason: String,
) -> LiveExecutionRecord {
    LiveExecutionRecord {
        id,
        cycle,
        submitted_unix_ms: now_unix_ms(),
        pair: opportunity.pair.clone(),
        buy_venue: opportunity.buy_venue,
        sell_venue: opportunity.sell_venue,
        input_quote_atoms: opportunity.input_quote_atoms,
        direct_signal_net_profit_usd: opportunity.net_profit_usd,
        requote_net_profit_usd: None,
        requote_net_profit_bps: None,
        status: AtomicArbStatus::Blocked,
        submitted: false,
        simulation_ok: false,
        signature: None,
        solscan_url: None,
        reason: Some(reason),
        jupiter_reference_net_profit_bps: None,
        jupiter_reference_error: None,
        native_execution_cost_quote_atoms: None,
        native_setup_missing_atas: None,
        native_simulation_net_profit_atoms: None,
        native_simulation_pre_quote_atoms: None,
        native_simulation_post_quote_atoms: None,
        native_simulation_quote_delta_atoms: None,
        signed_transaction_bytes: None,
        latency_trace: Vec::new(),
    }
}

fn failed_rwa_execution_record(
    id: u64,
    cycle: u64,
    opportunity: &RwaOpportunity,
    input_atoms: u128,
    estimated_input_usd: f64,
    reason: String,
) -> RwaExecutionRecord {
    RwaExecutionRecord {
        id,
        cycle,
        submitted_unix_ms: now_unix_ms(),
        ticker: opportunity.ticker.clone(),
        kind: opportunity.kind,
        from_symbol: opportunity.from_symbol.clone(),
        to_symbol: opportunity.to_symbol.clone(),
        input_atoms,
        estimated_input_usd,
        expected_net_profit_usd: opportunity.net_profit_usd,
        expected_net_profit_bps: opportunity.net_profit_bps,
        submitted: false,
        simulation_ok: false,
        signature: None,
        solscan_url: None,
        leg_signatures: Vec::new(),
        leg_routes: opportunity
            .execution_legs
            .iter()
            .map(|leg| leg.route.clone())
            .collect(),
        reason: Some(reason),
    }
}

async fn execute_rwa_leg(
    executor: &JupiterLiveSwapExecutor,
    opportunity: &RwaOpportunity,
    leg: &crate::rwa::RwaExecutionLeg,
    amount_atoms: u64,
    max_usd: f64,
    estimated_input_usd: Option<f64>,
    submit: bool,
    slippage_bps: u16,
) -> Result<LiveSwapReport> {
    if let Some((provider, action)) = external_primary_leg(&leg.route) {
        return executor
            .run_external_primary_transaction(ExternalPrimaryTransactionRequest {
                provider,
                action,
                ticker: opportunity.ticker.clone(),
                input_mint: leg.input_mint.clone(),
                output_mint: leg.output_mint.clone(),
                amount_atoms,
                pyth_fair_price_usd: opportunity.pyth_fair_price_usd,
                pyth_confidence_bps: opportunity.confidence_buffer_bps,
                estimated_input_usd,
                max_usd,
                submit,
            })
            .await;
    }

    if rwa_leg_uses_api_route_transaction(&leg.route) {
        let pair = token_pair_for_rwa_leg(leg);
        return executor
            .run_api_route_transaction(ApiRouteSwapRequest {
                route: leg.route.clone(),
                pair,
                side: leg.side,
                amount_atoms,
                slippage_bps,
                submit,
                max_usd,
                estimated_input_usd_override: estimated_input_usd,
            })
            .await;
    }

    executor
        .run(LiveSwapRequest {
            input_mint: leg.input_mint.clone(),
            output_mint: leg.output_mint.clone(),
            amount_atoms,
            slippage_bps,
            submit,
            max_usd,
            estimated_input_usd_override: estimated_input_usd,
            required_route_label_contains: jupiter_v2_route_guard(&leg.route),
            force_swap_instructions: force_swap_instructions_for_rwa_route(&leg.route),
        })
        .await
}

fn rwa_leg_uses_api_route_transaction(route: &str) -> bool {
    matches!(api_route_provider_name(route), Some(provider) if provider != "jupiter")
        && !rwa_api_route_uses_jupiter_fallback(route)
}

fn force_swap_instructions_for_rwa_route(route: &str) -> bool {
    route.starts_with("jupiter-route:") || rwa_api_route_uses_jupiter_fallback(route)
}

fn rwa_api_route_uses_jupiter_fallback(route: &str) -> bool {
    rwa_api_route_uses_jupiter_fallback_with_flags(
        route,
        env_bool("RWA_OPENOCEAN_JUPITER_FALLBACK_ENABLED"),
        env_bool("RWA_OPENOCEAN_LIVE_BUILD_ENABLED"),
    )
}

fn rwa_api_route_uses_jupiter_fallback_with_flags(
    route: &str,
    openocean_fallback_enabled: Option<bool>,
    openocean_live_build_enabled: Option<bool>,
) -> bool {
    let Some(provider) = api_route_provider_name(route) else {
        return false;
    };
    match provider {
        "openocean" => openocean_fallback_enabled
            .unwrap_or_else(|| !openocean_live_build_enabled.unwrap_or(false)),
        _ => false,
    }
}

fn rwa_atomic_swap_instruction_route_supported(opportunity: &RwaOpportunity) -> bool {
    opportunity.execution_legs.len() > 1
        && opportunity
            .execution_legs
            .iter()
            .all(|leg| rwa_leg_supports_atomic_swap_instruction_route(&leg.route))
}

fn rwa_leg_supports_atomic_swap_instruction_route(route: &str) -> bool {
    route.starts_with("jupiter-route:") || rwa_api_route_uses_jupiter_fallback(route)
}

fn rwa_roundtrip_min_output_atoms(config: &BotConfig, opportunity: &RwaOpportunity) -> Option<u64> {
    let first_leg = opportunity.execution_legs.first()?;
    let last_leg = opportunity.execution_legs.last()?;
    if first_leg.input_mint != last_leg.output_mint
        || first_leg.input_decimals != last_leg.output_decimals
    {
        return None;
    }
    let input_atoms = u64::try_from(opportunity.input_atoms?).ok()?;
    let min_bps = config
        .rwa
        .min_net_profit_bps
        .max(config.trading.min_net_profit_bps)
        .max(0.0);
    let min_output = (input_atoms as f64 * (1.0 + min_bps / 10_000.0)).ceil();
    if !min_output.is_finite() || min_output > u64::MAX as f64 {
        return None;
    }
    Some(min_output as u64)
}

fn token_pair_for_rwa_leg(leg: &crate::rwa::RwaExecutionLeg) -> TokenPair {
    match leg.side {
        SwapSide::QuoteToBase => TokenPair {
            base_mint: leg.output_mint.clone(),
            quote_mint: leg.input_mint.clone(),
            base_decimals: leg.output_decimals,
            quote_decimals: leg.input_decimals,
        },
        SwapSide::BaseToQuote => TokenPair {
            base_mint: leg.input_mint.clone(),
            quote_mint: leg.output_mint.clone(),
            base_decimals: leg.input_decimals,
            quote_decimals: leg.output_decimals,
        },
    }
}

fn external_primary_leg(route: &str) -> Option<(String, String)> {
    let rest = route.strip_prefix("external-primary:")?;
    let mut parts = rest.split(':');
    let provider = parts.next()?.trim();
    let action = parts.next()?.trim();
    if provider.is_empty() || action.is_empty() {
        return None;
    }
    if !matches!(provider, "ondo_jupiter_rfq" | "ondo_primary") {
        return None;
    }
    Some((provider.to_string(), action.to_string()))
}

fn route_gated_rwa_opportunity(opportunity: &RwaOpportunity, min_net_profit_bps: f64) -> bool {
    opportunity.net_profit_bps >= min_net_profit_bps && opportunity.execution_legs.is_empty()
}

fn jupiter_v2_route_guard(route: &str) -> Option<String> {
    let lower = route.to_ascii_lowercase();
    let parts = lower.split(':').collect::<Vec<_>>();
    if let Some(label) = legacy_jupiter_route_label(route) {
        return Some(label);
    }
    if !lower.starts_with("jupiter-v2-order:") {
        return None;
    }
    if parts.get(1).is_some_and(|part| part.contains("rfq"))
        || parts.get(2).is_some_and(|part| *part == "jupiterz")
    {
        return Some("jupiter-v2-rfq".to_string());
    }
    parts
        .get(2)
        .map(|router| router.trim())
        .filter(|router| !router.is_empty() && *router != "unknown")
        .map(|router| format!("router:{router}"))
}

fn legacy_jupiter_route_label(route: &str) -> Option<String> {
    let rest = route.strip_prefix("jupiter-route:")?;
    let label = rest.split(':').next()?.trim();
    (!label.is_empty() && !label.eq_ignore_ascii_case("unknown")).then(|| label.to_string())
}

fn stable_balance_usd(balances: &HashMap<String, u128>) -> f64 {
    atoms_to_ui(balances.get(USDC_MINT).copied().unwrap_or_default(), 6)
        + atoms_to_ui(balances.get(USDT_MINT).copied().unwrap_or_default(), 6)
}

fn quote_funding_balance_usd(balances: &HashMap<String, u128>, sol_usd_estimate: f64) -> f64 {
    stable_balance_usd(balances)
        + atoms_to_ui(balances.get(WSOL_MINT).copied().unwrap_or_default(), 9)
            * sol_usd_estimate.max(1.0)
}

fn live_swap_report_success(report: &LiveSwapReport) -> bool {
    report.confirmation_error.is_none()
        && matches!(
            report.confirmation_status.as_deref(),
            Some("confirmed") | Some("finalized") | Some("Success")
        )
}

fn should_preflight_rwa_roundtrip(opportunity: &RwaOpportunity) -> bool {
    if opportunity.execution_legs.len() < 2 {
        return false;
    }
    if is_wsol_funded_stable_return_route(opportunity) {
        return true;
    }
    match opportunity.kind {
        RwaOpportunityKind::StableFundedDirectIssuerRoundtrip => true,
        RwaOpportunityKind::StableRouteArbitrage
        | RwaOpportunityKind::PrimaryMintSell
        | RwaOpportunityKind::PrimaryBuyBurn => opportunity.execution_legs.len() == 2,
        _ => false,
    }
}

fn rwa_live_closed_roundtrip_candidate(opportunity: &RwaOpportunity) -> bool {
    if opportunity.execution_legs.len() < 2 {
        return false;
    }
    let Some(first_leg) = opportunity.execution_legs.first() else {
        return false;
    };
    let Some(last_leg) = opportunity.execution_legs.last() else {
        return false;
    };
    if first_leg.input_mint != last_leg.output_mint
        || first_leg.input_decimals != last_leg.output_decimals
    {
        return false;
    }
    let stable_funded = first_leg.input_mint == USDC_MINT || first_leg.input_mint == USDT_MINT;
    let wsol_stable_funded = is_wsol_funded_stable_return_route(opportunity);
    if !stable_funded && !wsol_stable_funded {
        return false;
    }
    matches!(
        opportunity.kind,
        RwaOpportunityKind::StableRouteArbitrage
            | RwaOpportunityKind::StableFundedDirectIssuerRoundtrip
            | RwaOpportunityKind::PrimaryMintSell
            | RwaOpportunityKind::PrimaryBuyBurn
    )
}

fn block_pyth_discount_top_up(
    opportunity: &mut RwaOpportunity,
    wallet_balances: &HashMap<String, u128>,
    allow_top_ups: bool,
) -> bool {
    if !matches!(
        opportunity.kind,
        RwaOpportunityKind::PythDiscountBuy
            | RwaOpportunityKind::StableFundedDirectIssuerAccumulation
    ) || allow_top_ups
    {
        return false;
    }
    let held = wallet_balances
        .get(&opportunity.to_mint)
        .copied()
        .unwrap_or_default();
    if held == 0 {
        return false;
    }
    opportunity.executable = false;
    let top_up_reason = format!(
        "existing {} position is {} atom(s); RWA accumulation top-ups disabled",
        opportunity.to_symbol, held
    );
    opportunity.blocked_reason = Some(match opportunity.blocked_reason.take() {
        Some(reason) if !reason.is_empty() => format!("{reason}; {top_up_reason}"),
        _ => top_up_reason,
    });
    true
}

fn maybe_add_wsol_stable_funding_route(
    opportunity: &mut RwaOpportunity,
    wallet_balances: &HashMap<String, u128>,
    sol_usd_estimate: f64,
    min_net_profit_bps: f64,
    fee_safety_buffer_bps: f64,
) -> bool {
    if !rwa_wsol_stable_funding_enabled() || opportunity.execution_legs.len() < 2 {
        return false;
    }
    let Some(stable_mint) = stable_mint_for_funding(opportunity) else {
        return false;
    };
    let required_stable_atoms = opportunity.input_atoms.unwrap_or_default();
    if required_stable_atoms == 0
        || wallet_balances
            .get(stable_mint)
            .copied()
            .unwrap_or_default()
            >= required_stable_atoms
    {
        return false;
    }
    let Some(expected_stable_output_atoms) = opportunity.expected_output_atoms else {
        return false;
    };
    let Some(stable_decimals) = stable_decimals(stable_mint) else {
        return false;
    };
    if !sol_usd_estimate.is_finite() || sol_usd_estimate <= 0.0 {
        return false;
    }

    let buffer_bps = rwa_wsol_stable_funding_buffer_bps(fee_safety_buffer_bps);
    let required_stable_usd = atoms_to_ui(required_stable_atoms, stable_decimals);
    let buffered_input_usd = required_stable_usd * (1.0 + buffer_bps / 10_000.0);
    let funding_input_atoms =
        ui_to_atoms(buffered_input_usd / sol_usd_estimate, 9).saturating_add(1);
    let wsol_available = wallet_balances.get(WSOL_MINT).copied().unwrap_or_default();
    if wsol_available < funding_input_atoms {
        return false;
    }

    let expected_stable_output_usd = atoms_to_ui(expected_stable_output_atoms, stable_decimals);
    let network_buffer_usd = buffered_input_usd * (buffer_bps / 10_000.0);
    let adjusted_profit_usd =
        (expected_stable_output_usd - buffered_input_usd - network_buffer_usd)
            .min(opportunity.net_profit_usd);
    let adjusted_profit_bps = if buffered_input_usd > 0.0 {
        adjusted_profit_usd / buffered_input_usd * 10_000.0
    } else {
        f64::NEG_INFINITY
    };
    if !adjusted_profit_bps.is_finite() || adjusted_profit_bps < min_net_profit_bps {
        opportunity.executable = false;
        let reason = format!(
            "SOL/WSOL funding route would reduce net profit to {adjusted_profit_bps:.2} bps below required {min_net_profit_bps:.2} bps"
        );
        opportunity.blocked_reason = Some(match opportunity.blocked_reason.take() {
            Some(existing) if !existing.is_empty() => format!("{existing}; {reason}"),
            _ => reason,
        });
        return false;
    }

    let stable_symbol = stable_symbol(stable_mint).unwrap_or("stable");
    let mut funded_legs = Vec::with_capacity(opportunity.execution_legs.len() + 2);
    funded_legs.push(RwaExecutionLeg {
        input_mint: WSOL_MINT.to_string(),
        output_mint: stable_mint.to_string(),
        input_decimals: 9,
        output_decimals: stable_decimals,
        side: SwapSide::QuoteToBase,
        input_atoms: funding_input_atoms,
        estimated_input_usd: Some(buffered_input_usd),
        route: format!(
            "jupiter-v2-order:funding:wsol-to-{}",
            stable_symbol.to_ascii_lowercase()
        ),
    });
    funded_legs.extend(opportunity.execution_legs.clone());
    funded_legs.push(RwaExecutionLeg {
        input_mint: stable_mint.to_string(),
        output_mint: WSOL_MINT.to_string(),
        input_decimals: stable_decimals,
        output_decimals: 9,
        side: SwapSide::BaseToQuote,
        input_atoms: expected_stable_output_atoms,
        estimated_input_usd: Some(expected_stable_output_usd),
        route: format!(
            "jupiter-v2-order:funding:{}-to-wsol",
            stable_symbol.to_ascii_lowercase()
        ),
    });

    opportunity.from_symbol = "SOL/WSOL".to_string();
    opportunity.to_symbol = "SOL/WSOL".to_string();
    opportunity.from_mint = WSOL_MINT.to_string();
    opportunity.to_mint = WSOL_MINT.to_string();
    opportunity.input_atoms = Some(funding_input_atoms);
    opportunity.input_estimated_usd = Some(buffered_input_usd);
    opportunity.expected_output_atoms = None;
    opportunity.net_profit_usd = adjusted_profit_usd;
    opportunity.net_profit_bps = adjusted_profit_bps;
    opportunity.route = format!(
        "SOL/WSOL funding to {stable_symbol} -> {} -> {stable_symbol} return to SOL/WSOL",
        opportunity.route
    );
    opportunity.execution_legs = funded_legs;
    opportunity.executable = true;
    opportunity.blocked_reason = None;
    true
}

fn stable_mint_for_funding(opportunity: &RwaOpportunity) -> Option<&str> {
    let first = opportunity.execution_legs.first()?;
    let last = opportunity.execution_legs.last()?;
    if first.input_mint != opportunity.from_mint
        || Some(first.input_atoms) != opportunity.input_atoms
    {
        return None;
    }
    if first.input_mint != last.output_mint || !is_stable_funding_mint(&first.input_mint) {
        return None;
    }
    Some(first.input_mint.as_str())
}

fn is_wsol_funded_stable_return_route(opportunity: &RwaOpportunity) -> bool {
    let legs = &opportunity.execution_legs;
    let Some(first) = legs.first() else {
        return false;
    };
    let Some(last) = legs.last() else {
        return false;
    };
    legs.len() >= 4
        && first.input_mint == WSOL_MINT
        && last.output_mint == WSOL_MINT
        && is_stable_funding_mint(&first.output_mint)
        && first.output_mint == last.input_mint
        && first.route.contains("funding:wsol-to")
        && last.route.contains("funding:")
}

fn is_stable_funding_mint(mint: &str) -> bool {
    matches!(mint, USDC_MINT | USDT_MINT)
}

fn stable_decimals(mint: &str) -> Option<u8> {
    match mint {
        USDC_MINT | USDT_MINT => Some(6),
        _ => None,
    }
}

fn stable_symbol(mint: &str) -> Option<&'static str> {
    match mint {
        USDC_MINT => Some("USDC"),
        USDT_MINT => Some("USDT"),
        _ => None,
    }
}

fn rwa_wsol_stable_funding_enabled() -> bool {
    env_bool("RWA_SOL_STABLE_FUNDING_ENABLED").unwrap_or(true)
}

fn rwa_wsol_stable_funding_buffer_bps(fee_safety_buffer_bps: f64) -> f64 {
    env_value("RWA_SOL_STABLE_FUNDING_BUFFER_BPS")
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0)
        .unwrap_or_else(|| fee_safety_buffer_bps.max(50.0))
        .clamp(0.0, 1_000.0)
}

fn rwa_pyth_discount_buy_allow_top_ups() -> bool {
    env_bool("RWA_PYTH_DISCOUNT_BUY_ALLOW_TOP_UPS").unwrap_or(false)
}

fn configured_rwa_min_trade_usd(max_trade_usd: f64) -> f64 {
    rwa_min_trade_usd_from_ladder(
        max_trade_usd,
        env_value("RWA_TRADE_SIZE_USD_LADDER").as_deref(),
    )
}

fn rwa_min_trade_usd_from_ladder(max_trade_usd: f64, ladder: Option<&str>) -> f64 {
    if !max_trade_usd.is_finite() || max_trade_usd <= 0.0 {
        return 0.0;
    }
    let mut sizes = ladder
        .map(|value| {
            value
                .split(',')
                .filter_map(|part| part.trim().parse::<f64>().ok())
                .filter(|size| size.is_finite() && *size > 0.0 && *size <= max_trade_usd)
                .collect::<Vec<_>>()
        })
        .filter(|sizes| !sizes.is_empty())
        .unwrap_or_else(|| vec![max_trade_usd]);
    sizes.push(max_trade_usd);
    sizes
        .into_iter()
        .min_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap_or(max_trade_usd)
}

fn is_jupiter_rate_limit_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    message.contains("429") || lower.contains("too many requests") || lower.contains("rate limit")
}

fn live_probe_venue_from_name(name: &str) -> Option<Venue> {
    match name.to_ascii_lowercase().as_str() {
        "orca" => Some(Venue::Orca),
        "meteora" => Some(Venue::Meteora),
        "raydium" => Some(Venue::Raydium),
        _ => None,
    }
}

fn aggregator_swap_source(name: &str) -> &'static str {
    match name {
        "raydium_trade_api" => {
            "Raydium transaction API; unsigned transaction decompiled into atomic composer"
        }
        "spiderswap" => {
            "Carbium/SpiderSwap executable quote transaction decompiled into atomic composer"
        }
        "dflow" => "DFlow /order transaction payload decompiled into atomic executor",
        "titan" => {
            "Titan MessagePack websocket instructions/transaction composed into atomic executor"
        }
        "okx" => "OKX Solana swap-instruction API composed into atomic executor",
        "openocean" => "OpenOcean swap transaction endpoint decompiled into atomic composer",
        "venum" => "Venum unsigned transaction build decompiled into atomic composer",
        "vybe" => "Vybe configurable swap build endpoint decompiled into atomic composer",
        _ => "configurable executable swap endpoint composed into atomic executor",
    }
}

fn env_boolish(name: &str) -> bool {
    env_bool(name).unwrap_or(false)
}

fn provider_has_required_auth(provider: &crate::config::AggregatorProviderConfig) -> bool {
    provider.requires_api_key && missing_provider_auth_envs(provider).is_empty()
}

fn primary_routes_all_waiting_for_oracle(rwa: &RwaSnapshot, provider: &str) -> bool {
    let mut matching_routes = rwa.assets.iter().flat_map(|asset| {
        asset
            .primary_routes
            .iter()
            .filter(move |route| route.provider == provider)
    });
    let Some(first) = matching_routes.next() else {
        return false;
    };
    first.readiness == RwaPrimaryRouteReadiness::OracleBlocked
        && matching_routes.all(|route| route.readiness == RwaPrimaryRouteReadiness::OracleBlocked)
}

fn aggregator_path_readiness(
    aggregators_enabled: bool,
    provider_enabled: bool,
    requires_api_key: bool,
    has_api_key: bool,
    cooldown_remaining_ms: Option<u64>,
    next_request_in_ms: Option<u64>,
) -> ExecutionPathReadiness {
    if !aggregators_enabled || !provider_enabled {
        return ExecutionPathReadiness::Disabled;
    }
    if requires_api_key && !has_api_key {
        return ExecutionPathReadiness::AuthMissing;
    }
    if cooldown_remaining_ms.is_some() {
        return ExecutionPathReadiness::CoolingDown;
    }
    if next_request_in_ms.is_some() {
        return ExecutionPathReadiness::Pacing;
    }
    ExecutionPathReadiness::Active
}

fn aggregator_path_status(
    name: &str,
    aggregators_enabled: bool,
    provider_enabled: bool,
    requires_api_key: bool,
    has_api_key: bool,
    missing_auth_envs: &[String],
) -> String {
    if !aggregators_enabled {
        return "aggregator providers disabled globally".to_string();
    }
    if !provider_enabled {
        return "provider disabled".to_string();
    }
    if requires_api_key && !has_api_key {
        return if missing_auth_envs.is_empty() {
            "provider requires authentication before it can be queried".to_string()
        } else {
            format!(
                "provider requires authentication before it can be queried; missing {}",
                missing_auth_envs.join(", ")
            )
        };
    }
    match name {
        "raydium_trade_api" => {
            "active quote provider; Raydium transaction payloads are decompiled for atomic execution".to_string()
        }
        "dflow" => {
            "enabled DFlow /order source; transaction payload execution is wired into the atomic executor".to_string()
        }
        "titan" => {
            "configured; Titan MessagePack websocket execution client is wired for API-token use".to_string()
        }
        "okx" => {
            "active signed OKX source; Solana swap-instruction execution is wired into the atomic executor".to_string()
        }
        "spiderswap" => {
            "active Carbium/SpiderSwap Q1 source; executable txn payloads are wired into the atomic executor".to_string()
        }
        "venum" => {
            "enabled quote/build source; unsigned transaction execution is wired into the atomic executor".to_string()
        }
        _ => "active quote provider; executable swap composer is wired for configured swap endpoint".to_string(),
    }
}

fn rotated_candidate_indices(available: usize, limit: usize, start_offset: usize) -> Vec<usize> {
    if available == 0 || limit == 0 {
        return Vec::new();
    }

    let start = start_offset % available;
    (0..limit.min(available))
        .map(|offset| (start + offset) % available)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{
            AggregatorsConfig, DatabaseConfig, DiscoveryConfig, ExecutionConfig, FeesConfig,
            JupiterConfig, RpcConfig, RwaConfig, SimulationConfig, SubmittersConfig, TradingConfig,
            VenuesConfig, WalletConfig,
        },
        types::{PoolKind, USDC_MINT},
    };
    use std::sync::{Mutex as StdMutex, OnceLock};

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<String>,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let lock = test_env_lock();
            let previous = std::env::var(key).ok();
            std::env::set_var(key, value);
            Self {
                key,
                previous,
                _lock: lock,
            }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(previous) = self.previous.as_ref() {
                std::env::set_var(self.key, previous);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }

    fn test_env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| StdMutex::new(())).lock().unwrap()
    }

    fn test_config() -> BotConfig {
        BotConfig {
            rpc: RpcConfig {
                primary_http: "http://localhost:8899".to_string(),
                primary_ws: "ws://localhost:8900".to_string(),
                fallback_http: vec![],
                commitment: "processed".to_string(),
                request_timeout_ms: 250,
            },
            wallet: WalletConfig {
                path: ".secrets/wallet.json".to_string(),
            },
            database: DatabaseConfig::default(),
            trading: TradingConfig {
                live: true,
                trade_size_usd: 100.0,
                min_net_profit_bps: 1.0,
                fee_safety_buffer_bps: 0.0,
                max_active_tokens: 100,
                scan_interval_ms: 5_000,
                quote_mints: vec![USDC_MINT.to_string()],
                major_denylist: vec![],
                sol_usd_estimate: 100.0,
            },
            rwa: RwaConfig {
                enabled: false,
                ..RwaConfig::default()
            },
            discovery: DiscoveryConfig::default(),
            fees: FeesConfig {
                priority_fee_percentile: 75.0,
                max_priority_fee_lamports: 0,
                jito_tip_lamports: 0,
                max_jito_tip_lamports: 0,
                ata_rent_lamports: 0,
                base_fee_lamports: 0,
            },
            venues: VenuesConfig {
                enabled: vec!["raydium".to_string()],
            },
            jupiter: JupiterConfig {
                enabled: false,
                ..JupiterConfig::default()
            },
            aggregators: AggregatorsConfig {
                enabled: false,
                ..AggregatorsConfig::default()
            },
            simulation: SimulationConfig::default(),
            execution: ExecutionConfig::default(),
            submitters: SubmittersConfig {
                enabled: vec!["primary_rpc".to_string()],
            },
        }
    }

    fn pool(venue: crate::types::Venue, address: &str, reserve_quote_atoms: u128) -> PoolState {
        PoolState {
            venue,
            kind: PoolKind::ConstantProduct,
            address: address.to_string(),
            pair: TokenPair {
                base_mint: "BASE".to_string(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 6,
                quote_decimals: 6,
            },
            reserve_base_atoms: 1_000_000_000_000,
            reserve_quote_atoms,
            sqrt_price_x64: None,
            active_bin_id: None,
            fee_bps: 0,
            protocol_fee_bps: 0,
            liquidity_quote_atoms: reserve_quote_atoms,
            updated_slot: 42,
        }
    }

    fn seed_engine_route_edge(db: &crate::db::ArbDatabase) -> (TokenPair, i64, i64) {
        let pair = TokenPair {
            base_mint: "BASE".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 6,
            quote_decimals: 6,
        };
        for (mint, symbol, decimals) in [
            (pair.base_mint.clone(), "BASE", pair.base_decimals),
            (pair.quote_mint.clone(), "USDC", pair.quote_decimals),
        ] {
            db.upsert_token(&crate::db::TokenGraphRecord {
                mint,
                symbol: symbol.to_string(),
                decimals,
                token_program: None,
                now_unix_ms: 1_000,
            })
            .expect("token");
        }
        for (pool_address, venue) in [
            (
                "MeteoraEnginePool111111111111111111111111".to_string(),
                crate::types::Venue::Meteora,
            ),
            (
                "RaydiumEnginePool111111111111111111111111".to_string(),
                crate::types::Venue::Raydium,
            ),
        ] {
            db.upsert_pool(&crate::db::PoolGraphRecord {
                pool_address,
                venue,
                pool_kind: PoolKind::ConstantProduct,
                program_owner: None,
                pair: pair.clone(),
                liquidity_quote_ui: Some(1.0),
                price_quote_per_base: Some(1.0),
                updated_slot: 1,
                source: "test".to_string(),
                now_unix_ms: 1_000,
            })
            .expect("pool");
        }
        let family_id = db
            .upsert_route_family(&crate::db::RouteFamilyGraphRecord {
                pair: pair.clone(),
                buy_venue: crate::types::Venue::Meteora,
                sell_venue: crate::types::Venue::Raydium,
                enabled: true,
                priority: 1,
                now_unix_ms: 1_000,
            })
            .expect("route family");
        let edge_id = db
            .upsert_route_edge(&crate::db::RouteEdgeGraphRecord {
                route_family_id: family_id,
                buy_pool: "MeteoraEnginePool111111111111111111111111".to_string(),
                sell_pool: "RaydiumEnginePool111111111111111111111111".to_string(),
                buy_pool_kind: PoolKind::ConstantProduct,
                sell_pool_kind: PoolKind::ConstantProduct,
                readiness_state: crate::types::RouteReadinessState::FreshAccountsReady,
                last_blocker: None,
                cooldown_until_unix_ms: None,
                now_unix_ms: 1_000,
            })
            .expect("route edge");
        (pair, family_id, edge_id)
    }

    fn optimized_test_opportunity() -> Opportunity {
        let mut config = test_config();
        config.trading.min_net_profit_bps = 0.0;
        config.execution.max_live_trade_usd = 1_000.0;
        let engine = BotEngine::new(config);
        let buy = pool(
            crate::types::Venue::Meteora,
            "MeteoraEnginePool111111111111111111111111",
            1_000_000_000_000,
        );
        let sell = pool(
            crate::types::Venue::Raydium,
            "RaydiumEnginePool111111111111111111111111",
            1_100_000_000_000,
        );
        engine
            .evaluate_pair_at_optimized_size(
                &buy,
                &sell,
                None,
                FeeBreakdown::default(),
                100.0,
                None,
            )
            .expect("optimized route")
    }

    #[test]
    fn optimized_route_context_includes_optimizer_decision() {
        let opportunity = optimized_test_opportunity();
        let decision = opportunity
            .optimizer_decision
            .as_ref()
            .expect("optimizer decision");

        assert_eq!(decision.source, "two_pool_route_size_optimizer");
        assert_eq!(
            decision.selected_input_quote_atoms,
            opportunity.input_quote_atoms
        );
        assert_eq!(decision.base_atoms, opportunity.base_atoms);
        assert_eq!(decision.output_quote_atoms, opportunity.output_quote_atoms);
        assert_eq!(
            decision.gross_profit_quote_atoms,
            opportunity.gross_profit_quote_atoms
        );
        assert_eq!(decision.net_profit_usd, opportunity.net_profit_usd);
        assert_eq!(decision.net_profit_bps, opportunity.net_profit_bps);
        assert_eq!(decision.route_edge_id, None);
        assert_eq!(decision.buy_pool, opportunity.buy_pool);
        assert_eq!(decision.sell_pool, opportunity.sell_pool);
        assert_eq!(decision.buy_venue, Some(opportunity.buy_venue));
        assert_eq!(decision.sell_venue, Some(opportunity.sell_venue));
        assert!(decision.optimizer_decision_id.len() >= 32);

        let context = route_context_json(&opportunity, Some(77), None, None, None)
            .expect("route context json");
        let context: serde_json::Value =
            serde_json::from_str(&context).expect("valid route context json");
        let json_decision = &context["optimizer_decision"];

        assert_eq!(
            json_decision["optimizer_decision_id"],
            decision.optimizer_decision_id
        );
        assert_eq!(json_decision["route_edge_id"], 77);
        assert_eq!(json_decision["buy_pool"], opportunity.buy_pool);
        assert_eq!(json_decision["sell_pool"], opportunity.sell_pool);
        assert_eq!(json_decision["buy_venue"], "meteora");
        assert_eq!(json_decision["sell_venue"], "raydium");
        assert_eq!(
            json_decision["selected_input_quote_atoms"],
            opportunity.input_quote_atoms.to_string()
        );
        assert_eq!(
            json_decision["net_profit_quote_atoms"],
            decision.net_profit_quote_atoms.to_string()
        );
    }

    #[test]
    fn simulation_evidence_persists_optimizer_decision_in_quote_and_logs_json() {
        let db = crate::db::ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (_pair, _family_id, _edge_id) = seed_engine_route_edge(&db);
        let opportunity = optimized_test_opportunity();
        let decision_id = opportunity
            .optimizer_decision
            .as_ref()
            .expect("optimizer decision")
            .optimizer_decision_id
            .clone();

        persist_simulation_evidence(&db, 35, &opportunity, "attempt-optimizer-sim", 35_000)
            .expect("simulation evidence");

        let quote_context: String = db
            .conn_for_test()
            .query_row(
                "SELECT freshness_json FROM quote_observations WHERE attempt_id = 'attempt-optimizer-sim'",
                [],
                |row| row.get(0),
            )
            .expect("quote context");
        let quote_context: serde_json::Value =
            serde_json::from_str(&quote_context).expect("quote context json");
        assert_eq!(
            quote_context["optimizer_decision"]["optimizer_decision_id"],
            decision_id
        );
        assert_eq!(
            quote_context["optimizer_decision"]["route_edge_id"],
            _edge_id
        );
        assert_eq!(
            quote_context["optimizer_decision"]["buy_pool"],
            "MeteoraEnginePool111111111111111111111111"
        );
        assert_eq!(
            quote_context["optimizer_decision"]["sell_pool"],
            "RaydiumEnginePool111111111111111111111111"
        );

        let logs_json: String = db
            .conn_for_test()
            .query_row(
                "SELECT logs_json FROM simulation_results WHERE attempt_id = 'attempt-optimizer-sim'",
                [],
                |row| row.get(0),
            )
            .expect("simulation logs");
        let logs_json: serde_json::Value =
            serde_json::from_str(&logs_json).expect("simulation logs json");
        assert_eq!(
            logs_json["optimizer_decision"]["optimizer_decision_id"],
            decision_id
        );
        assert_eq!(
            logs_json["route_context"]["optimizer_decision"]["optimizer_decision_id"],
            decision_id
        );
        assert_eq!(
            logs_json["route_context"]["optimizer_decision"]["route_edge_id"],
            _edge_id
        );
    }

    #[test]
    fn live_probe_evidence_persists_optimizer_decision_with_shared_attempt_id() {
        let db = crate::db::ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (_pair, _family_id, edge_id) = seed_engine_route_edge(&db);
        let opportunity = optimized_test_opportunity();
        let decision_id = opportunity
            .optimizer_decision
            .as_ref()
            .expect("optimizer decision")
            .optimizer_decision_id
            .clone();

        persist_live_probe_evidence(
            &db,
            35,
            &opportunity,
            None,
            Some("test gated before submit"),
            "attempt-optimizer-live",
            "test-config",
            35_500,
        )
        .expect("live probe evidence");

        let (quote_attempt_id, quote_context): (String, String) = db
            .conn_for_test()
            .query_row(
                "SELECT attempt_id, freshness_json FROM quote_observations WHERE source = 'live_probe'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("live quote context");
        let execution_attempt_id: String = db
            .conn_for_test()
            .query_row(
                "SELECT attempt_id FROM execution_attempts WHERE route_edge_id = ?1",
                [edge_id],
                |row| row.get(0),
            )
            .expect("execution attempt");
        let quote_context: serde_json::Value =
            serde_json::from_str(&quote_context).expect("quote context json");

        assert_eq!(quote_attempt_id, "attempt-optimizer-live");
        assert_eq!(execution_attempt_id, "attempt-optimizer-live");
        assert_eq!(
            quote_context["optimizer_decision"]["optimizer_decision_id"],
            decision_id
        );
        assert_eq!(
            quote_context["optimizer_decision"]["route_edge_id"],
            edge_id
        );
    }

    #[test]
    fn focused_optimizer_rejection_persists_replayable_quote_observation_only() {
        let db = crate::db::ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (_pair, _family_id, edge_id) = seed_engine_route_edge(&db);
        let buy = pool(
            crate::types::Venue::Meteora,
            "MeteoraEnginePool111111111111111111111111",
            1_000_000_000_000,
        );
        let sell = pool(
            crate::types::Venue::Raydium,
            "RaydiumEnginePool111111111111111111111111",
            1_100_000_000_000,
        );
        let reason = "no executable live size: min executable input 2000000 exceeds cap 1000000";

        persist_focused_optimizer_rejection_evidence(
            &db,
            44,
            Some(edge_id),
            &buy,
            &sell,
            2_000_000,
            1_000_000,
            reason,
            "focused-optimizer-reject-44",
            44_000,
            FeeBreakdown::default(),
            0.000001,
            0.0,
            0.0,
            0,
            false,
            0.0000001,
        )
        .expect("optimizer rejection evidence");

        let execution_attempt_count: i64 = db
            .conn_for_test()
            .query_row("SELECT COUNT(*) FROM execution_attempts", [], |row| {
                row.get(0)
            })
            .expect("execution attempt count");
        assert_eq!(execution_attempt_count, 0);
        let (source, blocker, freshness_json): (String, String, String) = db
            .conn_for_test()
            .query_row(
                "SELECT source, blocker, freshness_json FROM quote_observations WHERE route_edge_id = ?1",
                [edge_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("optimizer rejection quote observation");
        assert_eq!(source, "focused_optimizer_rejection");
        assert_eq!(blocker, reason);
        let context: serde_json::Value =
            serde_json::from_str(&freshness_json).expect("freshness json");
        assert_eq!(context["optimizer_decision"]["rejection_reason"], reason);
        assert_eq!(
            context["optimizer_decision"]["min_executable_input_quote_atoms"],
            "2000000"
        );
        assert_eq!(
            context["fresh_quote_reconciliation"]["optimizer_input_quote_atoms"],
            "1000000"
        );
        assert_eq!(
            context["fresh_quote_reconciliation"]["optimizer_buy_output_base_atoms"],
            "0"
        );
        assert_eq!(
            context["fresh_quote_reconciliation"]["fresh_buy_output_base_atoms"],
            serde_json::Value::Null
        );
        assert_eq!(
            context["fresh_quote_reconciliation"]["final_blocker"],
            reason
        );
    }

    #[test]
    fn focused_native_candidates_expose_optimizer_decision_on_selected_candidate_snapshot() {
        let opportunity = optimized_test_opportunity();
        let decision = opportunity
            .optimizer_decision
            .as_ref()
            .expect("optimizer decision")
            .clone();

        let snapshot = probe_snapshot_from_opportunity(
            &opportunity,
            "test-candidate".to_string(),
            None,
            Instant::now(),
            100.0,
        );

        assert_eq!(
            snapshot.optimizer_decision_id,
            Some(decision.optimizer_decision_id)
        );
        assert_eq!(
            snapshot.optimizer_selected_input_quote_atoms,
            Some(decision.selected_input_quote_atoms.to_string())
        );
        assert_eq!(
            snapshot.optimizer_net_profit_quote_atoms,
            Some(decision.net_profit_quote_atoms.to_string())
        );
        assert_eq!(
            snapshot.optimizer_net_profit_bps,
            Some(decision.net_profit_bps)
        );
        assert_eq!(
            snapshot.optimizer_net_profit_usd,
            Some(decision.net_profit_usd)
        );
        assert_eq!(snapshot.optimizer_buy_pool, Some(decision.buy_pool));
        assert_eq!(snapshot.optimizer_sell_pool, Some(decision.sell_pool));
        assert_eq!(snapshot.optimizer_buy_venue, decision.buy_venue);
        assert_eq!(snapshot.optimizer_sell_venue, decision.sell_venue);
    }

    #[test]
    fn net_profit_filter_accepts_positive_cross_pool_spread() {
        let engine = BotEngine::new(test_config());
        let cheap = pool(crate::types::Venue::Orca, "cheap", 1_000_000_000_000);
        let expensive = pool(crate::types::Venue::Meteora, "expensive", 1_100_000_000_000);
        let opportunity = engine
            .evaluate_pair_at_trade_size(&cheap, &expensive, 100.0, FeeBreakdown::default(), 100.0)
            .expect("spread should survive fees");
        assert!(opportunity.net_profit_usd > 0.0);
        assert!(opportunity.spread_bps > 0.0);
    }

    #[test]
    fn promotion_requires_two_venues_for_same_pair() {
        let engine = BotEngine::new(test_config());
        let promoted = engine.promote_pools(vec![pool(
            crate::types::Venue::Orca,
            "single",
            1_000_000_000_000,
        )]);
        assert!(promoted.is_empty());
    }

    #[test]
    fn hot_set_selector_prefers_focus_ready_route_accounts_over_fallback_pools() {
        let mut focus = test_config().execution.native_focus_routes;
        focus.push(crate::config::NativeFocusRoute {
            base_mint: "BASE".to_string(),
            quote_mint: USDC_MINT.to_string(),
            buy_venue: "meteora".to_string(),
            sell_venue: "raydium".to_string(),
            require_fresh_accounts: true,
        });
        let routes = vec![
            HotSubscriptionRoute {
                route_edge_id: None,
                pair: TokenPair {
                    base_mint: "BASE".to_string(),
                    quote_mint: USDC_MINT.to_string(),
                    base_decimals: 6,
                    quote_decimals: 6,
                },
                buy_venue: crate::types::Venue::Meteora,
                sell_venue: crate::types::Venue::Raydium,
                readiness_state: Some(crate::types::RouteReadinessState::FreshAccountsReady),
                pubkeys: vec![
                    "focus-buy-pool".to_string(),
                    "focus-buy-vault".to_string(),
                    "focus-sell-pool".to_string(),
                ],
            },
            HotSubscriptionRoute {
                route_edge_id: None,
                pair: TokenPair {
                    base_mint: "OTHER".to_string(),
                    quote_mint: USDC_MINT.to_string(),
                    base_decimals: 6,
                    quote_decimals: 6,
                },
                buy_venue: crate::types::Venue::Meteora,
                sell_venue: crate::types::Venue::Raydium,
                readiness_state: Some(crate::types::RouteReadinessState::FreshAccountsReady),
                pubkeys: vec!["other-ready-pool".to_string()],
            },
        ];

        let selected =
            select_hot_subscription_pubkeys(&routes, &["fallback-pool".to_string()], &focus, 3);

        assert_eq!(
            selected.pubkeys,
            vec![
                "focus-buy-pool".to_string(),
                "focus-buy-vault".to_string(),
                "focus-sell-pool".to_string(),
            ]
        );
        assert_eq!(selected.desired_count, 3);
        assert_eq!(selected.budget, 3);
        assert_eq!(selected.budget_remaining, 0);
        assert!(selected
            .reasons
            .iter()
            .all(|reason| reason.contains("focus route")));
    }

    #[test]
    fn hot_set_selector_skips_jupiter_probe_labels() {
        let routes = vec![HotSubscriptionRoute {
            route_edge_id: None,
            pair: TokenPair {
                base_mint: "BASE".to_string(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 6,
                quote_decimals: 6,
            },
            buy_venue: crate::types::Venue::Orca,
            sell_venue: crate::types::Venue::Meteora,
            readiness_state: None,
            pubkeys: vec![
                "jupiter-probe:orca".to_string(),
                "jupiter-probe:meteora".to_string(),
                USDC_MINT.to_string(),
            ],
        }];

        let selected = select_hot_subscription_pubkeys(&routes, &[], &[], 3);

        assert_eq!(selected.pubkeys, vec![USDC_MINT.to_string()]);
        assert_eq!(selected.desired_count, 1);
        assert_eq!(selected.budget_remaining, 2);
    }

    #[test]
    fn selected_candidate_refresh_pubkeys_use_route_manifest_with_core_pools_first() {
        let db = crate::db::ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (_pair, _family_id, _edge_id) = seed_engine_route_edge(&db);
        db.upsert_pool_account(&crate::db::PoolAccountGraphRecord {
            pool_address: "MeteoraEnginePool111111111111111111111111".to_string(),
            role: "vault_a".to_string(),
            pubkey: "AaaEngineVault111111111111111111111111111".to_string(),
            owner: None,
            mint: None,
            token_program: None,
            derivation_kind: None,
            derivation_seed_json: None,
            required: true,
            now_unix_ms: 10_000,
        })
        .expect("pool account");
        let mut config = test_config();
        config.database.enabled = true;
        let engine = BotEngine::new(config).with_database_for_test(db);
        let opportunity = engine
            .evaluate_pair_at_optimized_size(
                &pool(
                    crate::types::Venue::Meteora,
                    "MeteoraEnginePool111111111111111111111111",
                    1_000_000_000_000,
                ),
                &pool(
                    crate::types::Venue::Raydium,
                    "RaydiumEnginePool111111111111111111111111",
                    1_100_000_000_000,
                ),
                None,
                FeeBreakdown::default(),
                100.0,
                None,
            )
            .expect("opportunity");

        let pubkeys = engine.hot_refresh_pubkeys_for_candidate(&opportunity);

        assert_eq!(
            &pubkeys[..2],
            &[
                "MeteoraEnginePool111111111111111111111111".to_string(),
                "RaydiumEnginePool111111111111111111111111".to_string(),
            ]
        );
        assert!(pubkeys.contains(&"AaaEngineVault111111111111111111111111111".to_string()));
        assert!(pubkeys.contains(&opportunity.pair.quote_mint));
        assert!(pubkeys.contains(&opportunity.pair.base_mint));
    }

    #[tokio::test]
    async fn selected_focused_route_registration_expands_budget_limited_hot_set_to_full_manifest() {
        let db = crate::db::ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (_pair, _family_id, edge_id) = seed_engine_route_edge(&db);
        db.upsert_pool_account(&crate::db::PoolAccountGraphRecord {
            pool_address: "MeteoraEnginePool111111111111111111111111".to_string(),
            role: "vault_a".to_string(),
            pubkey: "AaaEngineVault111111111111111111111111111".to_string(),
            owner: None,
            mint: None,
            token_program: None,
            derivation_kind: None,
            derivation_seed_json: None,
            required: true,
            now_unix_ms: 10_000,
        })
        .expect("pool account");
        let mut config = test_config();
        config.database.enabled = true;
        let engine = BotEngine::new(config).with_database_for_test(db);
        let budget_limited_pubkeys = vec![
            "MeteoraEnginePool111111111111111111111111".to_string(),
            "RaydiumEnginePool111111111111111111111111".to_string(),
        ];
        engine
            .pool_subscription
            .register_focused_route_accounts(
                edge_id,
                route_account_requirements_from_pubkeys(&budget_limited_pubkeys),
                focused_route_account_max_age_ms(engine.config.execution.stale_candidate_ttl_ms),
                FOCUSED_ROUTE_REQUOTE_DEBOUNCE_MS,
            )
            .await;
        assert_eq!(
            engine
                .pool_subscription
                .focused_route_readiness(edge_id)
                .await
                .expect("partial route registration")
                .accounts
                .len(),
            2
        );

        let full_manifest_pubkeys = engine
            .register_focused_route_manifest_accounts(edge_id)
            .await;
        let snapshot = engine
            .pool_subscription
            .focused_route_readiness(edge_id)
            .await
            .expect("full route registration");

        assert!(full_manifest_pubkeys
            .contains(&"AaaEngineVault111111111111111111111111111".to_string()));
        assert_eq!(snapshot.accounts.len(), full_manifest_pubkeys.len());
        assert!(snapshot
            .accounts
            .iter()
            .any(|account| account.pubkey == "AaaEngineVault111111111111111111111111111"));
    }

    #[test]
    fn evidence_records_include_mapped_and_unmapped_runtime_attempts() {
        let db = crate::db::ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (pair, family_id, edge_id) = seed_engine_route_edge(&db);
        let mapped = Opportunity {
            pair: pair.clone(),
            buy_venue: crate::types::Venue::Meteora,
            sell_venue: crate::types::Venue::Raydium,
            buy_pool: "MeteoraEnginePool111111111111111111111111".to_string(),
            sell_pool: "RaydiumEnginePool111111111111111111111111".to_string(),
            input_quote_atoms: 1_000_000,
            base_atoms: 2_000_000,
            output_quote_atoms: 1_020_000,
            gross_profit_quote_atoms: 20_000,
            net_profit_usd: 0.02,
            net_profit_bps: 200.0,
            spread_bps: 220.0,
            fee: FeeBreakdown::default(),
            detected_slot: 123,
            optimizer_decision: None,
        };
        let unmapped = Opportunity {
            buy_pool: "UnmappedBuy11111111111111111111111111111".to_string(),
            sell_pool: "UnmappedSell1111111111111111111111111111".to_string(),
            ..mapped.clone()
        };

        persist_simulation_evidence(&db, 9, &mapped, "attempt-sim", 10_000)
            .expect("simulation evidence");
        persist_live_probe_evidence(
            &db,
            9,
            &mapped,
            Some(&AtomicArbReport {
                status: AtomicArbStatus::Blocked,
                submitted: false,
                wallet_pubkey: "wallet1111111111111111111111111111111111".to_string(),
                pair: pair.clone(),
                buy_venue: crate::types::Venue::Meteora,
                sell_venue: crate::types::Venue::Raydium,
                input_quote_atoms: 1_000_000,
                buy_quote_out_amount: Some("2000000".to_string()),
                sell_quote_out_amount: Some("1010000".to_string()),
                direct_signal_net_profit_usd: 0.02,
                requote_net_profit_usd: Some(0.01),
                requote_net_profit_bps: Some(100.0),
                buy_route_labels: Vec::new(),
                sell_route_labels: Vec::new(),
                address_lookup_tables: Vec::new(),
                signed_transaction_bytes: 777,
                simulation_ok: true,
                simulation_error: None,
                simulation_logs: vec!["ok".to_string()],
                signature: None,
                confirmation_status: None,
                confirmation_error: None,
                solscan_url: None,
                blocked_reason: Some("native direct re-quote below required profit".to_string()),
                jupiter_reference_net_profit_bps: None,
                jupiter_reference_buy_quote_out_amount: None,
                jupiter_reference_sell_quote_out_amount: None,
                jupiter_reference_net_profit_usd: None,
                jupiter_reference_error: None,
                native_execution_cost_quote_atoms: None,
                native_setup_missing_atas: None,
                native_simulation_pre_quote_atoms: Some("1000000".to_string()),
                native_simulation_post_quote_atoms: Some("1010000".to_string()),
                native_simulation_quote_delta_atoms: Some("10000".to_string()),
                native_simulation_net_profit_atoms: Some("10000".to_string()),
                latency_trace: vec![crate::live_swap::ExecutionLatencyMark {
                    stage: "before_submit".to_string(),
                    elapsed_ms: 123,
                }],
            }),
            None,
            "attempt-live",
            "test-config",
            10_500,
        )
        .expect("live evidence");
        persist_simulation_evidence(&db, 10, &unmapped, "attempt-unmapped", 11_000)
            .expect("unmapped simulation evidence");

        assert_eq!(family_id > 0, true);
        let summary = db.summary().unwrap();
        assert_eq!(summary.simulation_results, 1);
        assert_eq!(summary.execution_attempts, 1);
        let live_context: String = db
            .conn_for_test()
            .query_row(
                "SELECT freshness_json FROM quote_observations WHERE source = 'live_probe' ORDER BY id DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .expect("live quote context");
        assert!(live_context.contains("latency_trace"));
        assert!(live_context.contains("before_submit"));
        assert!(live_context.contains("123"));
        assert_eq!(
            db.latest_simulation_result(edge_id).unwrap(),
            (
                "1000000".to_string(),
                Some("20000".to_string()),
                Some("20000".to_string()),
                Some(200.0),
                true,
                None,
            )
        );
        assert_eq!(
            db.latest_execution_attempt(edge_id).unwrap(),
            (
                "blocked_requote".to_string(),
                Some("1000000".to_string()),
                Some("10000".to_string()),
                Some("native direct re-quote below required profit".to_string()),
            )
        );
    }

    #[test]
    fn promotion_excludes_pump_like_base_mints() {
        let engine = BotEngine::new(test_config());
        let pump_mint = "Ce2gx9KGXJ6C9Mp5b5x1sn9Mg87JwEbrQby4Zqo3pump";
        let mut orca = pool(crate::types::Venue::Orca, "orca", 1_000_000_000_000);
        let mut meteora = pool(crate::types::Venue::Meteora, "meteora", 1_100_000_000_000);
        orca.pair.base_mint = pump_mint.to_string();
        meteora.pair.base_mint = pump_mint.to_string();

        let promoted = engine.promote_pools(vec![orca, meteora]);

        assert!(promoted.is_empty());
    }

    #[test]
    fn promotion_excludes_blacklisted_remora_stock_mints() {
        let engine = BotEngine::new(test_config());
        let remora_mint = "AVw21111111111111111111111111111111";
        let mut orca = pool(crate::types::Venue::Orca, "orca", 1_000_000_000_000);
        let mut meteora = pool(crate::types::Venue::Meteora, "meteora", 1_100_000_000_000);
        orca.pair.base_mint = remora_mint.to_string();
        meteora.pair.base_mint = remora_mint.to_string();

        let promoted = engine.promote_pools(vec![orca, meteora]);

        assert!(promoted.is_empty());
    }

    #[test]
    fn indexed_price_only_pool_fallback_uses_quote_liquidity_and_price() {
        let candidate = IndexedPoolCandidate {
            venue: Venue::Raydium,
            address: "raydium_pool".to_string(),
            base_mint: "BASE".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 6,
            quote_decimals: 6,
            liquidity_usd: 1_000.0,
            liquidity_quote_ui: Some(250.0),
            price_quote_per_base: Some(2.0),
            source: "dexscreener".to_string(),
        };
        let account = crate::types::RawAccount {
            pubkey: candidate.address.clone(),
            owner: "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK".to_string(),
            lamports: 0,
            slot: 99,
            data: vec![],
        };

        let pool = indexed_price_only_pool(&account, &candidate).expect("fallback pool");

        assert_eq!(pool.venue, Venue::Raydium);
        assert_eq!(pool.kind, PoolKind::ConcentratedLiquidity);
        assert_eq!(pool.pair.base_mint, "BASE");
        assert_eq!(pool.pair.quote_mint, USDC_MINT);
        assert_eq!(pool.liquidity_quote_atoms, 250_000_000);
        assert!(pool.sqrt_price_x64.is_some());
    }

    #[test]
    fn indexed_pool_kind_uses_program_owner_family() {
        assert_eq!(
            indexed_pool_kind(Venue::Raydium, RAYDIUM_AMM_V4_PROGRAM),
            PoolKind::RaydiumAmmV4
        );
        assert_eq!(
            indexed_pool_kind(Venue::Raydium, RAYDIUM_CPMM_PROGRAM),
            PoolKind::ConstantProduct
        );
        assert_eq!(
            indexed_pool_kind(Venue::Raydium, RAYDIUM_CLMM_PROGRAM),
            PoolKind::ConcentratedLiquidity
        );
        assert_eq!(
            indexed_pool_kind(Venue::Meteora, METEORA_POOLS_PROGRAM),
            PoolKind::MeteoraPools
        );
        assert_eq!(
            indexed_pool_kind(Venue::Meteora, METEORA_DLMM_PROGRAM),
            PoolKind::Dlmm
        );
        assert_eq!(
            indexed_pool_kind(Venue::Meteora, METEORA_DAMM_V2_PROGRAM),
            PoolKind::MeteoraDammV2
        );
    }

    #[tokio::test]
    async fn discovery_refresh_respects_slow_interval_even_when_empty() {
        let mut config = test_config();
        config.discovery.refresh_interval_ms = 60_000;
        let engine = BotEngine::new(config);

        assert!(engine.should_refresh_discovery().await);
        assert!(!engine.should_refresh_discovery().await);
    }

    #[test]
    fn live_candidate_rotation_wraps_candidate_list() {
        assert_eq!(rotated_candidate_indices(5, 3, 0), vec![0, 1, 2]);
        assert_eq!(rotated_candidate_indices(5, 3, 3), vec![3, 4, 0]);
        assert_eq!(rotated_candidate_indices(5, 10, 4), vec![4, 0, 1, 2, 3]);
    }

    #[test]
    fn probe_trade_size_parser_sorts_deduplicates_and_filters() {
        assert_eq!(
            parse_probe_trade_sizes("100,10,25,10,bad,0", 50.0),
            vec![10.0, 25.0, 100.0]
        );
        assert_eq!(parse_probe_trade_sizes("bad,0", 50.0), vec![50.0]);
    }

    #[test]
    fn probe_trade_sizes_prefer_configured_ladder_over_default_trade_size() {
        let mut config = ExecutionConfig::default();
        config.jupiter_probe_trade_sizes_usd = vec![0.5, 2.0, 10.0];

        let sizes = probe_trade_sizes_usd(&config, 100.0);

        assert_eq!(sizes, vec![0.5, 2.0, 10.0]);
    }

    #[test]
    fn direct_trade_sizes_prefer_configured_ladder_over_probe_env() {
        let mut config = ExecutionConfig::default();
        config.direct_trade_sizes_usd = vec![1.0, 3.0, 5.0];

        let sizes = direct_opportunity_trade_sizes_usd(&config, 100.0);

        assert_eq!(sizes, vec![1.0, 3.0, 5.0]);
    }

    #[test]
    fn optimized_route_max_input_uses_pool_liquidity_when_live_cap_is_disabled() {
        let mut config = test_config();
        config.execution.max_live_trade_usd = 0.0;
        config.execution.direct_trade_sizes_usd = vec![1.0, 3.0, 5.0];
        let pool = pool(crate::types::Venue::Meteora, "pool", 1_000_000_000);

        let max_atoms = optimized_route_max_input_quote_atoms(&config, &pool, 100.0, None);

        assert_eq!(max_atoms, 200_000_000);
    }

    #[test]
    fn optimized_route_max_input_uses_live_trade_cap_when_configured() {
        let mut config = test_config();
        config.execution.max_live_trade_usd = 25.0;
        config.execution.direct_trade_sizes_usd = vec![1.0, 3.0, 5.0];
        let pool = pool(crate::types::Venue::Meteora, "pool", 1_000_000_000);

        let max_atoms = optimized_route_max_input_quote_atoms(&config, &pool, 100.0, None);

        assert_eq!(max_atoms, 25_000_000);
    }

    #[test]
    fn optimized_route_max_input_uses_prefunded_quote_balance_cap() {
        let mut config = test_config();
        config.execution.max_live_trade_usd = 1_000.0;
        let pool = pool(crate::types::Venue::Meteora, "pool", 1_000_000_000);

        let max_atoms =
            optimized_route_max_input_quote_atoms(&config, &pool, 100.0, Some(250_000_000));

        assert_eq!(max_atoms, 250_000_000);
    }

    #[test]
    fn prefunded_wsol_quote_balance_uses_token_balance_not_sol_lamports() {
        assert_eq!(
            spendable_wsol_quote_balance_atoms(2_000_000_000, Some(250_000_000), true),
            250_000_000
        );
        assert_eq!(
            spendable_wsol_quote_balance_atoms(2_000_000_000, Some(250_000_000), false),
            1_995_000_000
        );
    }

    #[test]
    fn optimized_route_max_input_does_not_use_live_trade_counts_as_size_caps() {
        let mut config = test_config();
        config.execution.max_live_trade_usd = 0.0;
        config.execution.max_live_trades_per_session = 1;
        config.execution.max_live_trades_per_cycle = 1;
        let pool = pool(crate::types::Venue::Meteora, "pool", 1_000_000_000);

        let max_atoms = optimized_route_max_input_quote_atoms(&config, &pool, 100.0, None);

        assert_eq!(max_atoms, 200_000_000);
    }

    #[test]
    fn probe_priority_prefers_unseen_over_negative_history() {
        let mut stats = CandidateProbeStats {
            attempts: 5,
            blocks: 5,
            no_route_blocks: 1,
            last_requote_bps: Some(-50.0),
            best_requote_bps: Some(-25.0),
            ..CandidateProbeStats::default()
        };
        assert!(probe_candidate_priority(None) > probe_candidate_priority(Some(&stats)));
        stats.best_requote_bps = Some(-1.0);
        assert!(probe_candidate_priority(Some(&stats)) > 9_000.0);
    }

    fn test_rwa_leg(input_mint: &str, output_mint: &str) -> crate::rwa::RwaExecutionLeg {
        crate::rwa::RwaExecutionLeg {
            input_mint: input_mint.to_string(),
            output_mint: output_mint.to_string(),
            input_decimals: 6,
            output_decimals: 6,
            side: SwapSide::BaseToQuote,
            input_atoms: 1_000_000,
            estimated_input_usd: Some(1.0),
            route: "jupiter-v2-order:aggregator:metis:test".to_string(),
        }
    }

    fn test_rwa_leg_with_decimals(
        input_mint: &str,
        output_mint: &str,
        input_decimals: u8,
        output_decimals: u8,
        route: &str,
    ) -> crate::rwa::RwaExecutionLeg {
        crate::rwa::RwaExecutionLeg {
            input_mint: input_mint.to_string(),
            output_mint: output_mint.to_string(),
            input_decimals,
            output_decimals,
            side: SwapSide::BaseToQuote,
            input_atoms: 1_000_000,
            estimated_input_usd: Some(1.0),
            route: route.to_string(),
        }
    }

    fn test_rwa_opportunity(
        kind: RwaOpportunityKind,
        execution_legs: Vec<crate::rwa::RwaExecutionLeg>,
    ) -> RwaOpportunity {
        RwaOpportunity {
            ticker: "SPY".to_string(),
            kind,
            from_symbol: "USDC".to_string(),
            to_symbol: "USDC".to_string(),
            from_mint: USDC_MINT.to_string(),
            to_mint: USDC_MINT.to_string(),
            quote_asset: Some("USDC".to_string()),
            pyth_fair_price_usd: 100.0,
            from_price_usd: Some(100.0),
            to_price_usd: Some(101.0),
            input_atoms: Some(1_000_000),
            input_estimated_usd: Some(1.0),
            expected_output_atoms: Some(1_010_000),
            route: "test".to_string(),
            gross_edge_bps: 100.0,
            net_profit_bps: 100.0,
            net_profit_usd: 0.01,
            confidence_buffer_bps: 0.0,
            fee: FeeBreakdown::default(),
            executable: true,
            execution_legs,
            blocked_reason: None,
        }
    }

    fn test_primary_route(
        provider: &str,
        readiness: RwaPrimaryRouteReadiness,
    ) -> crate::rwa::RwaPrimaryRouteSnapshot {
        crate::rwa::RwaPrimaryRouteSnapshot {
            provider: provider.to_string(),
            symbol: "SPYon".to_string(),
            mint: "SPYon".to_string(),
            decimals: 9,
            shares_per_token: 1.0,
            source: "test".to_string(),
            price_usd: 100.0,
            normalized_price_usd: 100.0,
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
            readiness,
            executable: readiness != RwaPrimaryRouteReadiness::OracleBlocked,
            reason: "test".to_string(),
        }
    }

    fn test_rwa_asset_with_route(
        ticker: &str,
        route: crate::rwa::RwaPrimaryRouteSnapshot,
    ) -> crate::rwa::RwaAssetSnapshot {
        crate::rwa::RwaAssetSnapshot {
            ticker: ticker.to_string(),
            enabled: true,
            pyth_feed_id: format!("{:0>64}", ticker.len()),
            xstock_symbol: format!("{ticker}x"),
            xstock_mint: format!("{ticker}x_mint"),
            ondo_symbol: format!("{ticker}on"),
            ondo_mint: format!("{ticker}on_mint"),
            ondo_shares_per_token: 1.0,
            oracle: None,
            xstock_price_usd: None,
            ondo_price_usd: None,
            ondo_normalized_price_usd: None,
            xstock_premium_bps: None,
            ondo_premium_bps: None,
            quote_assets: Vec::new(),
            quote_count: 0,
            direct_route_request_count: 0,
            direct_route_count: 0,
            primary_route_count: 1,
            primary_routes: vec![route],
            best_rejected_kind: None,
            best_rejected_net_profit_bps: None,
            best_rejected_route: None,
            best_rejected_reason: None,
            best_rejected_by_kind: Vec::new(),
            no_trade_reason: String::new(),
            source: "test".to_string(),
        }
    }

    #[test]
    fn primary_route_oracle_waiting_requires_all_provider_routes_blocked() {
        let all_blocked = RwaSnapshot {
            assets: vec![
                test_rwa_asset_with_route(
                    "SPY",
                    test_primary_route("ondo_jupiter_rfq", RwaPrimaryRouteReadiness::OracleBlocked),
                ),
                test_rwa_asset_with_route(
                    "TSLA",
                    test_primary_route("ondo_jupiter_rfq", RwaPrimaryRouteReadiness::OracleBlocked),
                ),
            ],
            ..RwaSnapshot::default()
        };
        assert!(primary_routes_all_waiting_for_oracle(
            &all_blocked,
            "ondo_jupiter_rfq"
        ));

        let mixed_readiness = RwaSnapshot {
            assets: vec![
                test_rwa_asset_with_route(
                    "SPY",
                    test_primary_route("ondo_jupiter_rfq", RwaPrimaryRouteReadiness::WaitingForRfq),
                ),
                test_rwa_asset_with_route(
                    "TSLA",
                    test_primary_route("ondo_jupiter_rfq", RwaPrimaryRouteReadiness::OracleBlocked),
                ),
            ],
            ..RwaSnapshot::default()
        };
        assert!(!primary_routes_all_waiting_for_oracle(
            &mixed_readiness,
            "ondo_jupiter_rfq"
        ));
        assert!(!primary_routes_all_waiting_for_oracle(
            &RwaSnapshot::default(),
            "ondo_jupiter_rfq"
        ));
    }

    #[test]
    fn rwa_preflight_includes_stable_funded_direct_roundtrip() {
        let two_leg = test_rwa_opportunity(
            RwaOpportunityKind::StableRouteArbitrage,
            vec![
                test_rwa_leg(USDC_MINT, "SPYx"),
                test_rwa_leg("SPYx", USDC_MINT),
            ],
        );
        assert!(should_preflight_rwa_roundtrip(&two_leg));

        let three_leg = test_rwa_opportunity(
            RwaOpportunityKind::StableFundedDirectIssuerRoundtrip,
            vec![
                test_rwa_leg(USDC_MINT, "SPYx"),
                test_rwa_leg("SPYx", "SPYon"),
                test_rwa_leg("SPYon", USDC_MINT),
            ],
        );
        assert!(should_preflight_rwa_roundtrip(&three_leg));

        let direct = test_rwa_opportunity(
            RwaOpportunityKind::DirectIssuerSwap,
            vec![test_rwa_leg("SPYx", "SPYon")],
        );
        assert!(!should_preflight_rwa_roundtrip(&direct));

        let malformed_stable = test_rwa_opportunity(
            RwaOpportunityKind::StableRouteArbitrage,
            vec![
                test_rwa_leg(USDC_MINT, "SPYx"),
                test_rwa_leg("SPYx", "SPYon"),
                test_rwa_leg("SPYon", USDC_MINT),
            ],
        );
        assert!(!should_preflight_rwa_roundtrip(&malformed_stable));
    }

    #[test]
    fn rwa_live_candidates_require_closed_stable_roundtrips() {
        let stable = test_rwa_opportunity(
            RwaOpportunityKind::StableRouteArbitrage,
            vec![
                test_rwa_leg(USDC_MINT, "SPYx"),
                test_rwa_leg("SPYx", USDC_MINT),
            ],
        );
        assert!(rwa_live_closed_roundtrip_candidate(&stable));

        let direct_roundtrip = test_rwa_opportunity(
            RwaOpportunityKind::StableFundedDirectIssuerRoundtrip,
            vec![
                test_rwa_leg(USDC_MINT, "SPYx"),
                test_rwa_leg("SPYx", "SPYon"),
                test_rwa_leg("SPYon", USDC_MINT),
            ],
        );
        assert!(rwa_live_closed_roundtrip_candidate(&direct_roundtrip));

        let wsol_funded = test_rwa_opportunity(
            RwaOpportunityKind::StableRouteArbitrage,
            vec![
                test_rwa_leg(WSOL_MINT, USDT_MINT),
                test_rwa_leg(USDT_MINT, "SPYx"),
                test_rwa_leg("SPYx", USDT_MINT),
                test_rwa_leg(USDT_MINT, WSOL_MINT),
            ],
        );
        assert!(!rwa_live_closed_roundtrip_candidate(&wsol_funded));

        let explicit_wsol_stable_funding = test_rwa_opportunity(
            RwaOpportunityKind::StableRouteArbitrage,
            vec![
                test_rwa_leg_with_decimals(
                    WSOL_MINT,
                    USDT_MINT,
                    9,
                    6,
                    "jupiter-v2-order:funding:wsol-to-usdt",
                ),
                test_rwa_leg(USDT_MINT, "SPYx"),
                test_rwa_leg("SPYx", USDT_MINT),
                test_rwa_leg_with_decimals(
                    USDT_MINT,
                    WSOL_MINT,
                    6,
                    9,
                    "jupiter-v2-order:funding:usdt-to-wsol",
                ),
            ],
        );
        assert!(rwa_live_closed_roundtrip_candidate(
            &explicit_wsol_stable_funding
        ));

        let primary_mint_sell = test_rwa_opportunity(
            RwaOpportunityKind::PrimaryMintSell,
            vec![
                test_rwa_leg(USDC_MINT, "SPYon"),
                test_rwa_leg("SPYon", USDC_MINT),
            ],
        );
        assert!(rwa_live_closed_roundtrip_candidate(&primary_mint_sell));

        let primary_buy_burn = test_rwa_opportunity(
            RwaOpportunityKind::PrimaryBuyBurn,
            vec![
                test_rwa_leg(USDC_MINT, "SPYon"),
                test_rwa_leg("SPYon", USDC_MINT),
            ],
        );
        assert!(rwa_live_closed_roundtrip_candidate(&primary_buy_burn));

        let accumulation = test_rwa_opportunity(
            RwaOpportunityKind::StableFundedDirectIssuerAccumulation,
            vec![
                test_rwa_leg(USDC_MINT, "SPYx"),
                test_rwa_leg("SPYx", "SPYon"),
            ],
        );
        assert!(!rwa_live_closed_roundtrip_candidate(&accumulation));

        let rebalance = test_rwa_opportunity(
            RwaOpportunityKind::NeutralInventoryRebalance,
            vec![test_rwa_leg("SPYon", "SPYx")],
        );
        assert!(!rwa_live_closed_roundtrip_candidate(&rebalance));

        let malformed_stable = test_rwa_opportunity(
            RwaOpportunityKind::StableRouteArbitrage,
            vec![
                test_rwa_leg(USDC_MINT, "SPYx"),
                test_rwa_leg("SPYx", "SPYon"),
            ],
        );
        assert!(!rwa_live_closed_roundtrip_candidate(&malformed_stable));
    }

    #[test]
    fn rwa_no_route_cooldown_keys_cover_jupiter_execution_legs() {
        let mut rfq_leg = test_rwa_leg(USDC_MINT, "SPYon");
        rfq_leg.route = "jupiter-v2-order:rfq:jupiterz:JupiterZ".to_string();
        let mut sell_leg = test_rwa_leg("SPYon", USDC_MINT);
        sell_leg.route = "jupiter-route:Meteora DLMM:sell".to_string();
        let opportunity =
            test_rwa_opportunity(RwaOpportunityKind::PrimaryMintSell, vec![rfq_leg, sell_leg]);

        let keys =
            rwa_route_leg_cooldown_keys_for_reason(&opportunity, "No routes found for live leg");
        assert_eq!(keys.len(), 2);
        assert!(keys.iter().any(|key| key.contains("jupiter-v2-order")));
        assert!(keys.iter().any(|key| key.contains("jupiter-route")));
        let negative_requote_keys = rwa_route_leg_cooldown_keys_for_reason(
            &opportunity,
            "RWA quote-return live re-quote profit -468.70 bps is below required 3.00 bps",
        );
        assert_eq!(negative_requote_keys.len(), 2);
        assert!(
            rwa_route_leg_cooldown_keys_for_reason(&opportunity, "wallet unavailable").is_empty()
        );

        let mut cooldowns = HashMap::new();
        cooldowns.insert(keys[0].clone(), Instant::now() + Duration::from_secs(60));
        assert!(rwa_route_leg_cooldown_until(&opportunity, &cooldowns).is_some());
    }

    #[test]
    fn rwa_negative_requote_cooldown_is_short_for_transient_arb_windows() {
        let _guard = EnvVarGuard::set("LIVE_NEGATIVE_REQUOTE_COOLDOWN_SECS", "900");
        let reason =
            "RWA quote-return live re-quote profit -468.70 bps is below required 3.00 bps";

        assert_eq!(
            rwa_route_leg_cooldown_duration_for_reason(reason),
            Some(Duration::from_secs(15))
        );
    }

    #[test]
    fn rwa_atomic_swap_instruction_route_requires_composable_legs() {
        let supported = test_rwa_opportunity(
            RwaOpportunityKind::StableRouteArbitrage,
            vec![
                {
                    let mut leg = test_rwa_leg(USDC_MINT, "SPYx");
                    leg.route = "jupiter-route:Whirlpool:buy".to_string();
                    leg
                },
                {
                    let mut leg = test_rwa_leg("SPYx", USDC_MINT);
                    leg.route = "jupiter-route:Meteora DLMM:sell".to_string();
                    leg
                },
            ],
        );
        assert!(rwa_atomic_swap_instruction_route_supported(&supported));

        let v2_only = test_rwa_opportunity(
            RwaOpportunityKind::StableRouteArbitrage,
            vec![
                test_rwa_leg(USDC_MINT, "SPYx"),
                test_rwa_leg("SPYx", USDC_MINT),
            ],
        );
        assert!(!rwa_atomic_swap_instruction_route_supported(&v2_only));

        let external_primary = test_rwa_opportunity(
            RwaOpportunityKind::PrimaryMintSell,
            vec![
                {
                    let mut leg = test_rwa_leg(USDC_MINT, "SPYon");
                    leg.route = "external-primary:ondo_jupiter_rfq:mint".to_string();
                    leg
                },
                {
                    let mut leg = test_rwa_leg("SPYon", USDC_MINT);
                    leg.route = "jupiter-route:Whirlpool:sell".to_string();
                    leg
                },
            ],
        );
        assert!(!rwa_atomic_swap_instruction_route_supported(
            &external_primary
        ));
    }

    #[test]
    fn rwa_roundtrip_min_output_uses_configured_profit_floor() {
        let mut config = test_config();
        config.trading.min_net_profit_bps = 12.5;
        config.rwa.min_net_profit_bps = 20.0;
        let opportunity = test_rwa_opportunity(
            RwaOpportunityKind::StableRouteArbitrage,
            vec![
                test_rwa_leg(USDC_MINT, "SPYx"),
                test_rwa_leg("SPYx", USDC_MINT),
            ],
        );

        assert_eq!(
            rwa_roundtrip_min_output_atoms(&config, &opportunity),
            Some(1_002_000)
        );

        let non_roundtrip = test_rwa_opportunity(
            RwaOpportunityKind::DirectIssuerSwap,
            vec![
                test_rwa_leg(USDC_MINT, "SPYx"),
                test_rwa_leg("SPYx", "SPYon"),
            ],
        );
        assert_eq!(
            rwa_roundtrip_min_output_atoms(&config, &non_roundtrip),
            None
        );
    }

    #[test]
    fn route_gated_rwa_opportunity_requires_profit_and_no_execution_legs() {
        let no_legs = test_rwa_opportunity(RwaOpportunityKind::DirectionalSell, Vec::new());
        assert!(route_gated_rwa_opportunity(&no_legs, 10.0));

        let mut below_threshold = no_legs.clone();
        below_threshold.net_profit_bps = 9.99;
        assert!(!route_gated_rwa_opportunity(&below_threshold, 10.0));

        let with_live_leg = test_rwa_opportunity(
            RwaOpportunityKind::DirectIssuerSwap,
            vec![test_rwa_leg("SPYx", "SPYon")],
        );
        assert!(!route_gated_rwa_opportunity(&with_live_leg, 10.0));
    }

    #[test]
    fn wsol_funding_route_wraps_underfunded_stable_roundtrip() {
        let mut opportunity = test_rwa_opportunity(
            RwaOpportunityKind::PrimaryMintSell,
            vec![
                test_rwa_leg(USDC_MINT, "SPYon"),
                test_rwa_leg("SPYon", USDC_MINT),
            ],
        );
        opportunity.expected_output_atoms = Some(1_030_000);
        let mut balances = HashMap::new();
        balances.insert(USDC_MINT.to_string(), 81_325);
        balances.insert(WSOL_MINT.to_string(), 20_000_000);

        assert!(maybe_add_wsol_stable_funding_route(
            &mut opportunity,
            &balances,
            100.0,
            0.0,
            15.0,
        ));

        assert_eq!(opportunity.from_mint, WSOL_MINT);
        assert_eq!(opportunity.to_mint, WSOL_MINT);
        assert_eq!(opportunity.execution_legs.len(), 4);
        assert_eq!(opportunity.execution_legs[0].input_mint, WSOL_MINT);
        assert_eq!(opportunity.execution_legs[0].output_mint, USDC_MINT);
        assert_eq!(opportunity.execution_legs[3].input_mint, USDC_MINT);
        assert_eq!(opportunity.execution_legs[3].output_mint, WSOL_MINT);
        assert!(opportunity.route.contains("SOL/WSOL funding to USDC"));
        assert!(should_preflight_rwa_roundtrip(&opportunity));
        assert!(opportunity.net_profit_bps > 0.0);
    }

    #[test]
    fn quote_funding_balance_prices_sol_once() {
        let mut balances = HashMap::new();
        balances.insert(USDC_MINT.to_string(), 10_000_000);
        balances.insert(USDT_MINT.to_string(), 2_500_000);
        balances.insert(WSOL_MINT.to_string(), 500_000_000);

        let available = quote_funding_balance_usd(&balances, 100.0);

        assert_eq!(stable_balance_usd(&balances), 12.5);
        assert!((available - 62.5).abs() < f64::EPSILON);
    }

    #[test]
    fn wsol_settlement_uses_spendable_sol_and_does_not_double_count_fees() {
        let reserve = live_sol_fee_reserve_lamports();
        let pre_sol = reserve + 1_000_000_000;
        let post_sol = reserve + 1_001_000_000;

        assert_eq!(
            spendable_wsol_quote_balance_atoms(pre_sol, None, false),
            1_000_000_000
        );
        assert_eq!(
            spendable_wsol_quote_balance_atoms(post_sol, None, false),
            1_001_000_000
        );
        assert_eq!(sol_fee_quote_atoms(10_000, WSOL_MINT, 100.0), 0);
    }

    #[test]
    fn pyth_discount_buy_blocks_existing_position_unless_top_ups_enabled() {
        let mut opportunity = test_rwa_opportunity(
            RwaOpportunityKind::PythDiscountBuy,
            vec![test_rwa_leg(WSOL_MINT, "TESTr")],
        );
        opportunity.to_symbol = "TESTr".to_string();
        opportunity.to_mint = "TESTr".to_string();

        let mut balances = HashMap::new();
        balances.insert("TESTr".to_string(), 245_311_543);

        assert!(block_pyth_discount_top_up(
            &mut opportunity,
            &balances,
            false,
        ));
        assert!(!opportunity.executable);
        assert!(opportunity
            .blocked_reason
            .as_deref()
            .unwrap_or_default()
            .contains("RWA accumulation top-ups disabled"));

        let mut allowed = opportunity.clone();
        allowed.executable = true;
        allowed.blocked_reason = None;
        assert!(!block_pyth_discount_top_up(&mut allowed, &balances, true));
        assert!(allowed.executable);
        assert!(allowed.blocked_reason.is_none());
    }

    #[test]
    fn stable_funded_direct_accumulation_blocks_existing_position_like_discount_buy() {
        let mut opportunity = test_rwa_opportunity(
            RwaOpportunityKind::StableFundedDirectIssuerAccumulation,
            vec![
                test_rwa_leg(USDC_MINT, "SPYx"),
                test_rwa_leg("SPYx", "SPYon"),
            ],
        );
        opportunity.to_symbol = "SPYon".to_string();
        opportunity.to_mint = "SPYon".to_string();

        let mut balances = HashMap::new();
        balances.insert("SPYon".to_string(), 1);

        assert!(block_pyth_discount_top_up(
            &mut opportunity,
            &balances,
            false,
        ));
        assert!(!opportunity.executable);
        assert!(opportunity
            .blocked_reason
            .as_deref()
            .unwrap_or_default()
            .contains("RWA accumulation top-ups disabled"));

        let mut allowed = opportunity.clone();
        allowed.executable = true;
        allowed.blocked_reason = None;
        assert!(!block_pyth_discount_top_up(&mut allowed, &balances, true));
        assert!(allowed.executable);
    }

    #[test]
    fn rwa_readiness_uses_smallest_configured_trade_ladder_size() {
        assert_eq!(
            rwa_min_trade_usd_from_ladder(25.0, Some("1,2,4,10,25")),
            1.0
        );
        assert_eq!(rwa_min_trade_usd_from_ladder(25.0, Some("50,bad")), 25.0);
        assert_eq!(rwa_min_trade_usd_from_ladder(0.0, Some("1,2")), 0.0);
    }

    #[test]
    fn openocean_rwa_live_leg_falls_back_to_jupiter_unless_live_build_enabled() {
        assert!(rwa_api_route_uses_jupiter_fallback_with_flags(
            "openocean-route:Jupiter:pool",
            None,
            None,
        ));
        assert!(!rwa_api_route_uses_jupiter_fallback_with_flags(
            "openocean-route:Jupiter:pool",
            None,
            Some(true),
        ));
        assert!(rwa_api_route_uses_jupiter_fallback_with_flags(
            "openocean-route:Jupiter:pool",
            Some(true),
            Some(true),
        ));
        assert!(!rwa_api_route_uses_jupiter_fallback_with_flags(
            "dflow-route:route:pool",
            None,
            None,
        ));
    }

    #[test]
    fn rwa_jupiter_route_legs_use_live_swap_not_api_route_builder() {
        assert!(!rwa_leg_uses_api_route_transaction(
            "jupiter-route:Meteora DLMM:Eooam7ShtPZhQzq42ZeX1sAqFTtdhUCMjSYDAff334Hg"
        ));
        assert!(rwa_leg_uses_api_route_transaction("dflow-route:route:pool"));
    }

    #[test]
    fn rwa_jupiter_v2_route_guard_allows_any_jupiter_v2_rfq() {
        assert_eq!(
            jupiter_v2_route_guard("jupiter-v2-order:rfq:jupiterz:JupiterZ").as_deref(),
            Some("jupiter-v2-rfq")
        );
        assert_eq!(
            jupiter_v2_route_guard("jupiter-v2-order:rfq:unknown:JupiterZ").as_deref(),
            Some("jupiter-v2-rfq")
        );
        assert_eq!(
            jupiter_v2_route_guard("jupiter-v2-order:rfq:dflow:DFlow RFQ").as_deref(),
            Some("jupiter-v2-rfq")
        );
        assert_eq!(
            jupiter_v2_route_guard("jupiter-v2-order:aggregator:jupiterz:JupiterZ").as_deref(),
            Some("jupiter-v2-rfq")
        );
        assert_eq!(
            jupiter_v2_route_guard("jupiter-v2-order:aggregator+rfq:jupiterz:JupiterZ").as_deref(),
            Some("jupiter-v2-rfq")
        );
        assert_eq!(
            jupiter_v2_route_guard("jupiter-v2-order:aggregator:dflow:HumidiFi+Manifest"),
            Some("router:dflow".to_string())
        );
        assert_eq!(
            jupiter_v2_route_guard("jupiter-v2-order:aggregator:okx:OKX DEX Router"),
            Some("router:okx".to_string())
        );
        assert_eq!(
            jupiter_v2_route_guard("jupiter-v2-order:aggregator:metis:Whirlpool"),
            Some("router:metis".to_string())
        );
        assert_eq!(
            jupiter_v2_route_guard("jupiter-v2-order:aggregator:unknown:Whirlpool"),
            None
        );
    }

    #[test]
    fn rwa_jupiter_route_guard_preserves_legacy_route_label() {
        assert_eq!(
            jupiter_v2_route_guard("jupiter-route:Meteora DLMM:cNPfiL8eVbyqFMEETRrnUUDbfa"),
            Some("Meteora DLMM".to_string())
        );
        assert_eq!(
            jupiter_v2_route_guard("jupiter-route:Manifest:Cje75S6Z5LN4qRB1"),
            Some("Manifest".to_string())
        );
        assert_eq!(jupiter_v2_route_guard("jupiter-route:unknown:pool"), None);
    }

    #[test]
    fn aggregator_path_readiness_explains_inactive_sources() {
        assert_eq!(
            aggregator_path_readiness(false, true, false, false, None, None),
            ExecutionPathReadiness::Disabled
        );
        assert_eq!(
            aggregator_path_readiness(true, true, true, false, None, None),
            ExecutionPathReadiness::AuthMissing
        );
        assert_eq!(
            aggregator_path_readiness(true, true, false, false, Some(1_500), None),
            ExecutionPathReadiness::CoolingDown
        );
        assert_eq!(
            aggregator_path_readiness(true, true, false, false, None, Some(250)),
            ExecutionPathReadiness::Pacing
        );
        assert_eq!(
            aggregator_path_readiness(true, true, false, false, None, None),
            ExecutionPathReadiness::Active
        );
    }

    #[test]
    fn jupiter_probe_candidates_cover_live_venue_pairs() {
        let mut config = test_config();
        config.venues.enabled = vec![
            "orca".to_string(),
            "meteora".to_string(),
            "raydium".to_string(),
        ];
        config.execution.jupiter_probe_trade_sizes_usd = vec![25.0];
        let engine = BotEngine::new(config);
        let pair = TokenPair {
            base_mint: "BASE".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 6,
            quote_decimals: 6,
        };

        let candidates = engine.jupiter_probe_candidates(&[pair], &FeeBreakdown::default(), 100.0);

        assert_eq!(candidates.len(), 6);
        assert!(candidates
            .iter()
            .all(|candidate| candidate.buy_venue != candidate.sell_venue));
        assert!(candidates
            .iter()
            .all(|candidate| candidate.input_quote_atoms > 0));
    }

    #[test]
    fn jupiter_probe_candidates_exclude_pump_like_base_mints() {
        let mut config = test_config();
        config.venues.enabled = vec!["orca".to_string(), "meteora".to_string()];
        let engine = BotEngine::new(config);
        let pair = TokenPair {
            base_mint: "Ce2gx9KGXJ6C9Mp5b5x1sn9Mg87JwEbrQby4Zqo3pump".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 6,
            quote_decimals: 6,
        };

        let candidates = engine.jupiter_probe_candidates(&[pair], &FeeBreakdown::default(), 100.0);

        assert!(candidates.is_empty());
    }

    #[test]
    fn jupiter_probe_candidates_exclude_blacklisted_remora_stock_mints() {
        let mut config = test_config();
        config.venues.enabled = vec!["orca".to_string(), "meteora".to_string()];
        let engine = BotEngine::new(config);
        let pair = TokenPair {
            base_mint: "ALTP61111111111111111111111111111111".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 6,
            quote_decimals: 6,
        };

        let candidates = engine.jupiter_probe_candidates(&[pair], &FeeBreakdown::default(), 100.0);

        assert!(candidates.is_empty());
    }

    #[test]
    fn execution_candidates_keep_probe_fallback_when_opportunities_exist() {
        let mut config = test_config();
        config.venues.enabled = vec!["orca".to_string(), "meteora".to_string()];
        config.execution.jupiter_probe_enabled = true;
        let engine = BotEngine::new(config);
        let pair = TokenPair {
            base_mint: "BASE".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 6,
            quote_decimals: 6,
        };
        let opportunity = Opportunity {
            pair: pair.clone(),
            buy_venue: Venue::Raydium,
            sell_venue: Venue::Meteora,
            buy_pool: "raydium-route:buy".to_string(),
            sell_pool: "meteora-route:sell".to_string(),
            input_quote_atoms: 100_000_000,
            base_atoms: 0,
            output_quote_atoms: 100_000_000,
            gross_profit_quote_atoms: 0,
            net_profit_usd: 0.0,
            net_profit_bps: 0.0,
            spread_bps: 0.0,
            fee: FeeBreakdown::default(),
            detected_slot: 0,
            optimizer_decision: None,
        };

        let (candidates, source, _) = engine.execution_candidates_for_cycle(
            &[],
            &[opportunity],
            &[pair],
            &FeeBreakdown::default(),
            100.0,
            None,
        );

        assert!(source.contains("Jupiter venue probe fallback"));
        assert!(candidates.len() > 1);
        assert!(candidates
            .iter()
            .any(|candidate| candidate.buy_pool.starts_with("jupiter-probe:")));
    }

    #[test]
    fn execution_candidates_exclude_price_only_native_pool_opportunities() {
        let mut config = test_config();
        config.venues.enabled = vec!["orca".to_string(), "meteora".to_string()];
        config.execution.jupiter_probe_enabled = true;
        let engine = BotEngine::new(config);
        let pair = TokenPair {
            base_mint: "BASE".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 6,
            quote_decimals: 6,
        };
        let opportunity = Opportunity {
            pair: pair.clone(),
            buy_venue: Venue::Orca,
            sell_venue: Venue::Meteora,
            buy_pool: USDT_MINT.to_string(),
            sell_pool: WSOL_MINT.to_string(),
            input_quote_atoms: 100_000_000,
            base_atoms: 100_000_000,
            output_quote_atoms: 102_000_000,
            gross_profit_quote_atoms: 2_000_000,
            net_profit_usd: 2.0,
            net_profit_bps: 200.0,
            spread_bps: 200.0,
            fee: FeeBreakdown::default(),
            detected_slot: 0,
            optimizer_decision: None,
        };
        let mut price_only_buy = pool(Venue::Orca, &opportunity.buy_pool, 1_000_000_000_000);
        price_only_buy.reserve_base_atoms = 0;
        price_only_buy.reserve_quote_atoms = 0;
        let mut price_only_sell = pool(Venue::Meteora, &opportunity.sell_pool, 1_000_000_000_000);
        price_only_sell.reserve_base_atoms = 0;
        price_only_sell.reserve_quote_atoms = 0;

        let (candidates, source, _) = engine.execution_candidates_for_cycle(
            &[price_only_buy, price_only_sell],
            &[opportunity.clone()],
            &[pair.clone()],
            &FeeBreakdown::default(),
            100.0,
            None,
        );

        assert_eq!(source, "Jupiter venue probe");
        assert!(candidates
            .iter()
            .all(|candidate| candidate.buy_pool.starts_with("jupiter-probe:")));

        let reserve_buy = pool(Venue::Orca, &opportunity.buy_pool, 1_000_000_000_000);
        let reserve_sell = pool(Venue::Meteora, &opportunity.sell_pool, 1_100_000_000_000);
        let (candidates, source, _) = engine.execution_candidates_for_cycle(
            &[reserve_buy, reserve_sell],
            &[opportunity],
            &[pair],
            &FeeBreakdown::default(),
            100.0,
            None,
        );

        assert!(source.contains("Jupiter venue probe fallback"));
        assert!(candidates
            .iter()
            .any(|candidate| candidate.buy_pool == USDT_MINT));
    }

    #[test]
    fn rwa_only_mode_suppresses_generic_execution_candidates() {
        let mut config = test_config();
        config.rwa.enabled = true;
        config.rwa.execution_enabled = false;
        config.rwa.emit_execution_candidates = true;
        config.discovery.enabled = false;
        config.discovery.index_enabled = false;
        config.discovery.broad_rpc_enabled = false;
        config.simulation.enabled = false;
        config.execution.enabled = false;
        config.execution.submit_enabled = false;
        config.execution.native_price_only_execution_enabled = false;
        config.execution.jupiter_probe_enabled = false;
        config.execution.native_focus_routes.clear();
        config.aggregators.dynamic_discovery_enabled = false;
        config.jupiter.seed_pairs.clear();
        let engine = BotEngine::new(config);
        let pair = TokenPair {
            base_mint: "BASE".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 6,
            quote_decimals: 6,
        };
        let opportunity = Opportunity {
            pair: pair.clone(),
            buy_venue: Venue::Orca,
            sell_venue: Venue::Meteora,
            buy_pool: "buy-pool".to_string(),
            sell_pool: "sell-pool".to_string(),
            input_quote_atoms: 100_000_000,
            base_atoms: 100_000_000,
            output_quote_atoms: 102_000_000,
            gross_profit_quote_atoms: 2_000_000,
            net_profit_usd: 2.0,
            net_profit_bps: 200.0,
            spread_bps: 200.0,
            fee: FeeBreakdown::default(),
            detected_slot: 0,
            optimizer_decision: None,
        };

        let (candidates, source, rejections) = engine.execution_candidates_for_cycle(
            &[],
            &[opportunity],
            &[pair],
            &FeeBreakdown::default(),
            100.0,
            None,
        );

        assert!(candidates.is_empty());
        assert_eq!(source, "rwa-only mode; broad execution disabled");
        assert!(rejections.is_empty());
    }

    #[test]
    fn focused_native_candidates_make_jupiter_probe_non_critical_when_focus_configured() {
        let mut config = test_config();
        config.venues.enabled = vec!["meteora".to_string(), "raydium".to_string()];
        config.execution.native_price_only_execution_enabled = false;
        config.execution.jupiter_probe_enabled = true;
        config.execution.jupiter_probe_trade_sizes_usd = vec![25.0];
        config.execution.native_focus_routes = vec![crate::config::NativeFocusRoute {
            base_mint: "BASE".to_string(),
            quote_mint: USDC_MINT.to_string(),
            buy_venue: "meteora".to_string(),
            sell_venue: "raydium".to_string(),
            require_fresh_accounts: true,
        }];
        let engine = BotEngine::new(config);
        let pair = TokenPair {
            base_mint: "BASE".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 6,
            quote_decimals: 6,
        };

        let (candidates, source, _) = engine.execution_candidates_for_cycle(
            &[],
            &[],
            &[pair],
            &FeeBreakdown::default(),
            100.0,
            None,
        );

        assert_eq!(source, "focused native optimized route unavailable");
        assert!(candidates.is_empty());
    }

    #[test]
    fn focused_native_candidates_skip_unquoted_route_edge_fallbacks() {
        let db = crate::db::ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (pair, _family_id, edge_id) = seed_engine_route_edge(&db);
        db.insert_account_manifest(&crate::db::AccountManifestGraphRecord {
            route_edge_id: edge_id,
            manifest_version: 1,
            complete: true,
            missing_roles_json: "[]".to_string(),
            account_roles_json: "{}".to_string(),
            refreshed_slot: Some(1),
            refreshed_at_unix_ms: 1_000,
            decode_status: "complete".to_string(),
        })
        .expect("account manifest");
        let mut config = test_config();
        config.database.enabled = true;
        config.execution.native_price_only_execution_enabled = true;
        config.execution.jupiter_probe_enabled = false;
        config.execution.direct_trade_sizes_usd = vec![10.0];
        config.execution.native_focus_routes = vec![crate::config::NativeFocusRoute {
            base_mint: pair.base_mint.clone(),
            quote_mint: pair.quote_mint.clone(),
            buy_venue: "meteora".to_string(),
            sell_venue: "raydium".to_string(),
            require_fresh_accounts: true,
        }];
        let engine = BotEngine::new(config).with_database_for_test(db);
        let mut focus_buy = pool(
            crate::types::Venue::Meteora,
            "MeteoraEnginePool111111111111111111111111",
            1_000_000_000_000,
        );
        focus_buy.reserve_base_atoms = 0;
        focus_buy.reserve_quote_atoms = 0;
        let mut focus_sell = pool(
            crate::types::Venue::Raydium,
            "RaydiumEnginePool111111111111111111111111",
            1_100_000_000_000,
        );
        focus_sell.reserve_base_atoms = 0;
        focus_sell.reserve_quote_atoms = 0;
        let other_pair = TokenPair {
            base_mint: "OTHER".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 6,
            quote_decimals: 6,
        };
        let mut other_buy = pool(crate::types::Venue::Meteora, "OtherMeteoraPool", 1_000_000);
        other_buy.pair = other_pair.clone();
        let mut other_sell = pool(crate::types::Venue::Raydium, "OtherRaydiumPool", 1_100_000);
        other_sell.pair = other_pair.clone();

        let (candidates, source, _) = engine.execution_candidates_for_cycle(
            &[focus_buy, focus_sell, other_buy, other_sell],
            &[],
            &[pair.clone(), other_pair],
            &FeeBreakdown::default(),
            100.0,
            None,
        );

        assert_eq!(source, "focused native optimized route unavailable");
        assert!(candidates.is_empty());

        let (db_only_candidates, db_only_source, _) = engine.execution_candidates_for_cycle(
            &[],
            &[],
            &[pair.clone()],
            &FeeBreakdown::default(),
            100.0,
            None,
        );
        assert_eq!(db_only_source, "focused native optimized route unavailable");
        assert!(db_only_candidates.is_empty());
    }

    #[test]
    fn focused_native_candidates_skip_price_only_pools_when_fresh_accounts_required() {
        let db = crate::db::ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (pair, _family_id, edge_id) = seed_engine_route_edge(&db);
        db.insert_account_manifest(&crate::db::AccountManifestGraphRecord {
            route_edge_id: edge_id,
            manifest_version: 1,
            complete: true,
            missing_roles_json: "[]".to_string(),
            account_roles_json: "{}".to_string(),
            refreshed_slot: Some(1),
            refreshed_at_unix_ms: 1_000,
            decode_status: "complete".to_string(),
        })
        .expect("account manifest");
        let mut config = test_config();
        config.database.enabled = true;
        config.trading.min_net_profit_bps = 0.0;
        config.execution.native_price_only_execution_enabled = true;
        config.execution.jupiter_probe_enabled = false;
        config.execution.max_live_trade_usd = 1_000.0;
        config.execution.direct_trade_sizes_usd = vec![10.0];
        config.execution.native_focus_routes = vec![crate::config::NativeFocusRoute {
            base_mint: pair.base_mint.clone(),
            quote_mint: pair.quote_mint.clone(),
            buy_venue: "meteora".to_string(),
            sell_venue: "raydium".to_string(),
            require_fresh_accounts: true,
        }];
        let engine = BotEngine::new(config).with_database_for_test(db);
        let mut focus_buy = pool(
            crate::types::Venue::Meteora,
            "MeteoraEnginePool111111111111111111111111",
            1_000_000_000_000,
        );
        focus_buy.reserve_base_atoms = 0;
        focus_buy.reserve_quote_atoms = 0;
        focus_buy.sqrt_price_x64 = sqrt_price_x64_from_price(1.0, 6, 6);
        focus_buy.liquidity_quote_atoms = 1_000_000_000_000;
        let mut focus_sell = pool(
            crate::types::Venue::Raydium,
            "RaydiumEnginePool111111111111111111111111",
            1_100_000_000_000,
        );
        focus_sell.reserve_base_atoms = 0;
        focus_sell.reserve_quote_atoms = 0;
        focus_sell.sqrt_price_x64 = sqrt_price_x64_from_price(1.1, 6, 6);
        focus_sell.liquidity_quote_atoms = 1_100_000_000_000;

        let (candidates, source, _) = engine.execution_candidates_for_cycle(
            &[focus_buy, focus_sell],
            &[],
            &[pair],
            &FeeBreakdown::default(),
            100.0,
            None,
        );

        assert_eq!(source, "focused native optimized route unavailable");
        assert!(candidates.is_empty());
    }

    #[test]
    fn focused_native_candidates_promote_one_optimizer_selected_size_per_route_edge() {
        let db = crate::db::ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (pair, _family_id, edge_id) = seed_engine_route_edge(&db);
        db.insert_account_manifest(&crate::db::AccountManifestGraphRecord {
            route_edge_id: edge_id,
            manifest_version: 1,
            complete: true,
            missing_roles_json: "[]".to_string(),
            account_roles_json: "{}".to_string(),
            refreshed_slot: Some(1),
            refreshed_at_unix_ms: 1_000,
            decode_status: "complete".to_string(),
        })
        .expect("account manifest");
        let mut config = test_config();
        config.database.enabled = true;
        config.trading.min_net_profit_bps = 0.0;
        config.execution.native_price_only_execution_enabled = true;
        config.execution.jupiter_probe_enabled = false;
        config.execution.max_live_trade_usd = 1_000.0;
        config.execution.direct_trade_sizes_usd = vec![1.0, 10.0, 50.0];
        config.execution.native_focus_routes = vec![crate::config::NativeFocusRoute {
            base_mint: pair.base_mint.clone(),
            quote_mint: pair.quote_mint.clone(),
            buy_venue: "meteora".to_string(),
            sell_venue: "raydium".to_string(),
            require_fresh_accounts: true,
        }];
        let engine = BotEngine::new(config).with_database_for_test(db);
        let buy = pool(
            crate::types::Venue::Meteora,
            "MeteoraEnginePool111111111111111111111111",
            1_000_000_000_000,
        );
        let sell = pool(
            crate::types::Venue::Raydium,
            "RaydiumEnginePool111111111111111111111111",
            1_100_000_000_000,
        );

        let (candidates, source, _) = engine.execution_candidates_for_cycle(
            &[buy, sell],
            &[],
            &[pair.clone()],
            &FeeBreakdown::default(),
            100.0,
            None,
        );

        assert_eq!(source, "focused native optimized route");
        assert_eq!(candidates.len(), 1);
        let configured_atoms = [1.0, 10.0, 50.0]
            .into_iter()
            .map(|trade_size_usd| {
                ui_to_atoms(
                    trade_size_quote_ui(&pair, trade_size_usd, 100.0),
                    pair.quote_decimals,
                )
            })
            .collect::<Vec<_>>();
        assert!(!configured_atoms.contains(&candidates[0].input_quote_atoms));
        assert!(candidates[0].base_atoms > 0);
        assert!(candidates[0].output_quote_atoms > candidates[0].input_quote_atoms);
        assert!(candidates[0].net_profit_usd > 0.0);
        let decision = candidates[0]
            .optimizer_decision
            .as_ref()
            .expect("focused optimizer decision");
        assert_eq!(decision.route_edge_id, Some(edge_id));
        assert_eq!(
            probe_snapshot_from_opportunity(
                &candidates[0],
                "focused".to_string(),
                None,
                Instant::now(),
                100.0,
            )
            .optimizer_route_edge_id,
            Some(edge_id)
        );
    }

    #[test]
    fn focused_native_candidates_cap_optimizer_to_quote_balance() {
        let db = crate::db::ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (pair, _family_id, edge_id) = seed_engine_route_edge(&db);
        db.insert_account_manifest(&crate::db::AccountManifestGraphRecord {
            route_edge_id: edge_id,
            manifest_version: 1,
            complete: true,
            missing_roles_json: "[]".to_string(),
            account_roles_json: "{}".to_string(),
            refreshed_slot: Some(1),
            refreshed_at_unix_ms: 1_000,
            decode_status: "complete".to_string(),
        })
        .expect("account manifest");
        let mut config = test_config();
        config.database.enabled = true;
        config.trading.min_net_profit_bps = 0.0;
        config.execution.native_price_only_execution_enabled = true;
        config.execution.jupiter_probe_enabled = false;
        config.execution.max_live_trade_usd = 1_000.0;
        config.execution.native_focus_routes = vec![crate::config::NativeFocusRoute {
            base_mint: pair.base_mint.clone(),
            quote_mint: pair.quote_mint.clone(),
            buy_venue: "meteora".to_string(),
            sell_venue: "raydium".to_string(),
            require_fresh_accounts: true,
        }];
        let engine = BotEngine::new(config).with_database_for_test(db);
        let buy = pool(
            crate::types::Venue::Meteora,
            "MeteoraEnginePool111111111111111111111111",
            1_000_000_000_000,
        );
        let sell = pool(
            crate::types::Venue::Raydium,
            "RaydiumEnginePool111111111111111111111111",
            1_100_000_000_000,
        );
        let quote_balance_caps = HashMap::from([(pair.quote_mint.clone(), 250_000_000)]);

        let (candidates, source, _) = engine.execution_candidates_for_cycle(
            &[buy, sell],
            &[],
            &[pair],
            &FeeBreakdown::default(),
            100.0,
            Some(&quote_balance_caps),
        );

        assert_eq!(source, "focused native optimized route");
        assert_eq!(candidates.len(), 1);
        assert!(candidates[0].input_quote_atoms <= 250_000_000);
    }

    #[test]
    fn focused_native_candidates_reject_dlmm_min_executable_input_above_cap() {
        let db = crate::db::ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (pair, _family_id, edge_id) = seed_engine_route_edge(&db);
        db.insert_account_manifest(&crate::db::AccountManifestGraphRecord {
            route_edge_id: edge_id,
            manifest_version: 1,
            complete: true,
            missing_roles_json: "[]".to_string(),
            account_roles_json: "{}".to_string(),
            refreshed_slot: Some(1),
            refreshed_at_unix_ms: 1_000,
            decode_status: "complete".to_string(),
        })
        .expect("account manifest");
        let mut config = test_config();
        config.database.enabled = true;
        config.trading.min_net_profit_bps = 0.0;
        config.execution.native_price_only_execution_enabled = true;
        config.execution.jupiter_probe_enabled = false;
        config.execution.max_live_trade_usd = 1.0;
        config.execution.native_focus_routes = vec![crate::config::NativeFocusRoute {
            base_mint: pair.base_mint.clone(),
            quote_mint: pair.quote_mint.clone(),
            buy_venue: "meteora".to_string(),
            sell_venue: "raydium".to_string(),
            require_fresh_accounts: true,
        }];
        let engine = BotEngine::new(config).with_database_for_test(db);
        let mut buy = pool(
            crate::types::Venue::Meteora,
            "MeteoraEnginePool111111111111111111111111",
            2_000_000_000_000,
        );
        buy.kind = PoolKind::Dlmm;
        buy.reserve_base_atoms = 1;
        let sell = pool(
            crate::types::Venue::Raydium,
            "RaydiumEnginePool111111111111111111111111",
            3_000_000_000_000,
        );

        let (candidates, source, rejections) = engine.execution_candidates_for_cycle(
            &[buy, sell],
            &[],
            &[pair],
            &FeeBreakdown::default(),
            100.0,
            None,
        );

        assert_eq!(source, "focused native optimized route unavailable");
        assert!(candidates.is_empty());
        assert!(rejections.iter().any(|rejection| {
            rejection.reason.contains("min executable input")
                && rejection.reason.contains("exceeds cap")
        }));
    }

    #[test]
    fn focused_native_fresh_quote_rejects_unexecutable_candidate_before_probe() {
        let stale_candidate = Opportunity {
            pair: TokenPair {
                base_mint: "BASE".to_string(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 6,
                quote_decimals: 6,
            },
            buy_venue: Venue::Meteora,
            sell_venue: Venue::Raydium,
            buy_pool: "MeteoraEnginePool111111111111111111111111".to_string(),
            sell_pool: "RaydiumEnginePool111111111111111111111111".to_string(),
            input_quote_atoms: 10_000_000,
            base_atoms: 9_900_000,
            output_quote_atoms: 10_200_000,
            gross_profit_quote_atoms: 200_000,
            net_profit_usd: 0.20,
            net_profit_bps: 200.0,
            spread_bps: 200.0,
            fee: FeeBreakdown::default(),
            detected_slot: 42,
            optimizer_decision: Some(OptimizerDecisionTrace {
                optimizer_decision_id: "decision".to_string(),
                source: "two_pool_route_size_optimizer".to_string(),
                route_label: "route".to_string(),
                route_edge_id: Some(123),
                buy_pool: "MeteoraEnginePool111111111111111111111111".to_string(),
                sell_pool: "RaydiumEnginePool111111111111111111111111".to_string(),
                buy_venue: Some(Venue::Meteora),
                sell_venue: Some(Venue::Raydium),
                min_input_quote_atoms: 1,
                max_input_quote_atoms: 10_000_000,
                selected_input_quote_atoms: 10_000_000,
                base_atoms: 9_900_000,
                output_quote_atoms: 10_200_000,
                gross_profit_quote_atoms: 200_000,
                required_profit_quote_atoms: 0,
                net_profit_quote_atoms: 200_000,
                net_profit_usd: 0.20,
                net_profit_bps: 200.0,
                quote_atom_usd_value: 0.000001,
                safety_buffer_bps: 0.0,
                min_net_profit_bps: 0.0,
                min_absolute_profit_quote_atoms: 0,
                min_executable_input_quote_atoms: None,
                rejection_reason: None,
                network_fee: FeeBreakdown::default(),
                include_setup_cost: false,
                setup_lamport_usd_value: 0.0,
                buy_pool_snapshot: None,
                sell_pool_snapshot: None,
            }),
        };
        let fresh_quote = crate::native_swap::NativeFreshQuote {
            buy_quote_out_atoms: 0,
            sell_quote_out_atoms: 0,
            sell_amount_atoms: 0,
            execution_cost_quote_atoms: 0,
            fresh_requote_complete: false,
            requote_net_profit_bps: 0.0,
            requote_net_profit_usd: 0.0,
            blocked_reason: Some("native direct fresh buy quote produced zero output".to_string()),
        };

        let rejection =
            apply_fresh_focused_quote_to_candidate(stale_candidate.clone(), &fresh_quote, 15.0)
                .expect_err("zero-output fresh quote must reject before live probe");

        assert!(rejection.contains("fresh buy quote produced zero output"));

        let reconciliation = fresh_quote_reconciliation_from_opportunity(
            &stale_candidate,
            &rejection,
            Some(&fresh_quote),
            Some(FocusedRouteAccountAgeSummarySnapshot {
                ready: true,
                total_accounts: 4,
                required_accounts: 4,
                ready_accounts: 4,
                missing_required_accounts: 0,
                stale_required_accounts: 0,
                max_wall_clock_age_ms: Some(125),
                max_slot_age: Some(1),
                blocker: None,
            }),
        );

        assert_eq!(reconciliation.route_edge_id, Some(123));
        assert_eq!(reconciliation.optimizer_input_quote_atoms, "10000000");
        assert_eq!(reconciliation.optimizer_sell_output_quote_atoms, "10200000");
        assert_eq!(
            reconciliation.fresh_buy_output_base_atoms.as_deref(),
            Some("0")
        );
        assert_eq!(
            reconciliation.fresh_sell_output_quote_atoms.as_deref(),
            Some("0")
        );
        assert_eq!(
            reconciliation.fresh_net_profit_quote_atoms.as_deref(),
            Some("-10000000")
        );
        assert_eq!(
            reconciliation.execution_cost_quote_atoms.as_deref(),
            Some("0")
        );
        assert_eq!(reconciliation.final_blocker, rejection);
        assert_eq!(
            reconciliation
                .account_snapshot
                .as_ref()
                .map(|snapshot| snapshot.ready_accounts),
            Some(4)
        );
    }

    #[test]
    fn fresh_reconciliation_marks_positive_bps_blocked_quote_as_not_executable() {
        let stale_candidate = Opportunity {
            pair: TokenPair {
                base_mint: "BASE".to_string(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 6,
                quote_decimals: 6,
            },
            buy_venue: Venue::Meteora,
            sell_venue: Venue::Raydium,
            buy_pool: "MeteoraEnginePool111111111111111111111111".to_string(),
            sell_pool: "RaydiumEnginePool111111111111111111111111".to_string(),
            input_quote_atoms: 100_000_000,
            base_atoms: 50_000_000,
            output_quote_atoms: 120_000_000,
            gross_profit_quote_atoms: 20_000_000,
            net_profit_usd: 20.0,
            net_profit_bps: 2_000.0,
            spread_bps: 2_000.0,
            fee: FeeBreakdown::default(),
            detected_slot: 42,
            optimizer_decision: Some(OptimizerDecisionTrace {
                optimizer_decision_id: "decision".to_string(),
                source: "two_pool_route_size_optimizer".to_string(),
                route_label: "route".to_string(),
                route_edge_id: Some(5),
                buy_pool: "MeteoraEnginePool111111111111111111111111".to_string(),
                sell_pool: "RaydiumEnginePool111111111111111111111111".to_string(),
                buy_venue: Some(Venue::Meteora),
                sell_venue: Some(Venue::Raydium),
                min_input_quote_atoms: 1,
                max_input_quote_atoms: 100_000_000,
                selected_input_quote_atoms: 100_000_000,
                base_atoms: 50_000_000,
                output_quote_atoms: 120_000_000,
                gross_profit_quote_atoms: 20_000_000,
                required_profit_quote_atoms: 0,
                net_profit_quote_atoms: 20_000_000,
                net_profit_usd: 20.0,
                net_profit_bps: 2_000.0,
                quote_atom_usd_value: 0.000001,
                safety_buffer_bps: 0.0,
                min_net_profit_bps: 0.0,
                min_absolute_profit_quote_atoms: 0,
                min_executable_input_quote_atoms: None,
                rejection_reason: None,
                network_fee: FeeBreakdown::default(),
                include_setup_cost: false,
                setup_lamport_usd_value: 0.0,
                buy_pool_snapshot: None,
                sell_pool_snapshot: None,
            }),
        };
        let blocker =
            "native strict WS cache stale account BIN_ARRAY: age 330 ms exceeds ttl 250 ms";
        let fresh_quote = crate::native_swap::NativeFreshQuote {
            buy_quote_out_atoms: 50_000_000,
            sell_quote_out_atoms: 130_000_000,
            sell_amount_atoms: 50_000_000,
            execution_cost_quote_atoms: 1_000_000,
            fresh_requote_complete: false,
            requote_net_profit_bps: 3_000.0,
            requote_net_profit_usd: 30.0,
            blocked_reason: Some(blocker.to_string()),
        };

        let reconciliation = fresh_quote_reconciliation_from_opportunity(
            &stale_candidate,
            blocker,
            Some(&fresh_quote),
            None,
        );

        assert_eq!(reconciliation.fresh_requote_complete, Some(false));
        assert_eq!(
            reconciliation.fresh_blocked_reason.as_deref(),
            Some(blocker)
        );
        assert_eq!(reconciliation.fresh_quote_status, "fresh_positive_blocked");
    }

    #[test]
    fn strict_cache_stale_classifier_matches_focused_fresh_quote_errors() {
        assert!(is_native_strict_cache_stale_error(
            "native sell instruction build failed for raydium EP2i: native strict WS cache stale account EP2i: age 315 ms exceeds ttl 250 ms"
        ));
        assert!(!is_native_strict_cache_stale_error(
            "native strict WS cache missing account EP2i"
        ));
        assert!(!is_native_strict_cache_stale_error(
            "native direct re-quote net profit -10322.03 bps is below required 15.00 bps"
        ));
    }

    #[test]
    fn fresh_quote_truth_counts_complete_positive_separately_from_blocked_rejections() {
        let mut executable = optimized_test_opportunity();
        executable
            .optimizer_decision
            .as_mut()
            .expect("optimizer decision")
            .route_edge_id = Some(77);
        let blocked = focused_route_rejection_from_opportunity_with_reconciliation(
            &executable,
            "strict cache stale",
            Some(FreshQuoteReconciliationSnapshot {
                route_edge_id: executable
                    .optimizer_decision
                    .as_ref()
                    .and_then(|decision| decision.route_edge_id),
                pair: executable.pair.clone(),
                buy_venue: executable.buy_venue.to_string(),
                sell_venue: executable.sell_venue.to_string(),
                buy_pool: executable.buy_pool.clone(),
                sell_pool: executable.sell_pool.clone(),
                optimizer_input_quote_atoms: executable.input_quote_atoms.to_string(),
                optimizer_buy_output_base_atoms: executable.base_atoms.to_string(),
                optimizer_sell_output_quote_atoms: executable.output_quote_atoms.to_string(),
                optimizer_net_profit_quote_atoms: executable.gross_profit_quote_atoms.to_string(),
                optimizer_net_profit_bps: executable.net_profit_bps,
                fresh_buy_output_base_atoms: Some(executable.base_atoms.to_string()),
                fresh_sell_output_quote_atoms: Some(executable.output_quote_atoms.to_string()),
                fresh_net_profit_quote_atoms: Some("250000".to_string()),
                fresh_net_profit_bps: Some(25.0),
                execution_cost_quote_atoms: Some("0".to_string()),
                fresh_requote_complete: Some(false),
                fresh_blocked_reason: Some("strict cache stale".to_string()),
                fresh_quote_status: "fresh_positive_blocked".to_string(),
                final_blocker: "strict cache stale".to_string(),
                account_snapshot: None,
            }),
        );
        let unavailable = focused_route_rejection_from_opportunity_with_reconciliation(
            &executable,
            "fresh quote unavailable",
            None,
        );

        let truth = fresh_quote_truth_from_candidates_and_rejections(
            &[executable],
            &[blocked, unavailable],
        );

        assert_eq!(truth.fresh_positive_complete, 1);
        assert_eq!(truth.fresh_positive_blocked, 1);
        assert_eq!(truth.fresh_quote_unavailable, 1);
        assert_eq!(truth.executable_candidate_count, 1);
        assert!(truth.execution_gate_open);
    }

    #[test]
    fn live_settlement_classification_distinguishes_fee_loss_and_profit() {
        assert_eq!(
            estimated_realized_profit_atoms_for_report(
                &AtomicArbStatus::Failed,
                Some("10000"),
                Some("5000"),
            )
            .as_deref(),
            Some("-5000")
        );
        assert_eq!(
            classify_live_settlement(&AtomicArbStatus::Failed, Some("-5000")),
            "failed_fee_loss"
        );
        assert_eq!(
            classify_live_settlement(&AtomicArbStatus::Confirmed, Some("25000")),
            "landed_profit"
        );
        assert_eq!(
            classify_live_settlement(&AtomicArbStatus::Confirmed, Some("-1")),
            "landed_loss"
        );
    }

    #[test]
    fn live_submit_risk_gate_requires_caps_and_stops_after_failed_fee_losses() {
        let mut execution = ExecutionConfig::default();
        execution.max_live_fee_loss_usd_per_session = 0.0;
        execution.max_failed_transactions_per_session = 1;
        let missing_fee_cap = live_risk_snapshot_from_config(&execution, 0);
        assert!(!missing_fee_cap.gate_open);
        assert!(missing_fee_cap
            .blocked_reason
            .as_deref()
            .unwrap_or_default()
            .contains("MAX_LIVE_FEE_LOSS_USD_PER_SESSION"));

        execution.max_live_fee_loss_usd_per_session = 0.50;
        execution.max_failed_transactions_per_session = 0;
        let missing_failed_cap = live_risk_snapshot_from_config(&execution, 0);
        assert!(!missing_failed_cap.gate_open);
        assert!(missing_failed_cap
            .blocked_reason
            .as_deref()
            .unwrap_or_default()
            .contains("MAX_FAILED_TRANSACTIONS_PER_SESSION"));

        execution.max_failed_transactions_per_session = 1;
        let open = live_risk_snapshot_from_config(&execution, 0);
        assert!(open.gate_open);

        let stopped = live_risk_snapshot_from_config(&execution, 1);
        assert!(!stopped.gate_open);
        assert!(stopped
            .blocked_reason
            .as_deref()
            .unwrap_or_default()
            .contains("failed fee-loss cap reached"));
    }

    #[test]
    fn live_submit_proof_arming_requires_one_trade_no_simulation_caps() {
        let mut execution = ExecutionConfig::default();
        execution.require_simulation = true;
        execution.max_live_trades_per_session = 1;
        execution.max_live_trades_per_cycle = 1;
        execution.max_live_trade_usd = 50.0;
        assert!(live_submit_proof_arming_blocker(&execution)
            .unwrap()
            .contains("LIVE_REQUIRE_SIMULATION=false"));

        execution.require_simulation = false;
        execution.max_live_trades_per_session = 2;
        assert!(live_submit_proof_arming_blocker(&execution)
            .unwrap()
            .contains("MAX_LIVE_TRADES_PER_SESSION=1"));

        execution.max_live_trades_per_session = 1;
        execution.max_live_trades_per_cycle = 2;
        assert!(live_submit_proof_arming_blocker(&execution)
            .unwrap()
            .contains("MAX_LIVE_TRADES_PER_CYCLE=1"));

        execution.max_live_trades_per_cycle = 1;
        assert!(live_submit_proof_arming_blocker(&execution).is_none());
    }

    #[test]
    fn focused_native_fresh_quote_replaces_stale_optimizer_amounts() {
        let mut stale_candidate = Opportunity {
            pair: TokenPair {
                base_mint: "BASE".to_string(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 6,
                quote_decimals: 6,
            },
            buy_venue: Venue::Meteora,
            sell_venue: Venue::Raydium,
            buy_pool: "MeteoraEnginePool111111111111111111111111".to_string(),
            sell_pool: "RaydiumEnginePool111111111111111111111111".to_string(),
            input_quote_atoms: 10_000_000,
            base_atoms: 9_900_000,
            output_quote_atoms: 10_200_000,
            gross_profit_quote_atoms: 200_000,
            net_profit_usd: 0.20,
            net_profit_bps: 200.0,
            spread_bps: 200.0,
            fee: FeeBreakdown::default(),
            detected_slot: 42,
            optimizer_decision: None,
        };
        stale_candidate.optimizer_decision = Some(OptimizerDecisionTrace {
            optimizer_decision_id: "decision".to_string(),
            source: "two_pool_route_size_optimizer".to_string(),
            route_label: "route".to_string(),
            route_edge_id: Some(123),
            buy_pool: stale_candidate.buy_pool.clone(),
            sell_pool: stale_candidate.sell_pool.clone(),
            buy_venue: Some(Venue::Meteora),
            sell_venue: Some(Venue::Raydium),
            min_input_quote_atoms: 1,
            max_input_quote_atoms: 10_000_000,
            selected_input_quote_atoms: 10_000_000,
            base_atoms: stale_candidate.base_atoms,
            output_quote_atoms: stale_candidate.output_quote_atoms,
            gross_profit_quote_atoms: stale_candidate.gross_profit_quote_atoms,
            required_profit_quote_atoms: 0,
            net_profit_quote_atoms: stale_candidate.gross_profit_quote_atoms,
            net_profit_usd: stale_candidate.net_profit_usd,
            net_profit_bps: stale_candidate.net_profit_bps,
            quote_atom_usd_value: 0.000001,
            safety_buffer_bps: 0.0,
            min_net_profit_bps: 0.0,
            min_absolute_profit_quote_atoms: 0,
            min_executable_input_quote_atoms: None,
            rejection_reason: None,
            network_fee: FeeBreakdown::default(),
            include_setup_cost: false,
            setup_lamport_usd_value: 0.0,
            buy_pool_snapshot: None,
            sell_pool_snapshot: None,
        });
        let fresh_quote = crate::native_swap::NativeFreshQuote {
            buy_quote_out_atoms: 11_000_000,
            sell_quote_out_atoms: 10_350_000,
            sell_amount_atoms: 11_000_000,
            execution_cost_quote_atoms: 5_000,
            fresh_requote_complete: true,
            requote_net_profit_bps: 350.0,
            requote_net_profit_usd: 0.35,
            blocked_reason: None,
        };

        let fresh_candidate =
            apply_fresh_focused_quote_to_candidate(stale_candidate, &fresh_quote, 15.0)
                .expect("fresh executable quote should remain a candidate");

        assert_eq!(fresh_candidate.base_atoms, 11_000_000);
        assert_eq!(fresh_candidate.output_quote_atoms, 10_350_000);
        assert_eq!(fresh_candidate.gross_profit_quote_atoms, 350_000);
        assert_eq!(fresh_candidate.net_profit_bps, 350.0);
        let decision = fresh_candidate
            .optimizer_decision
            .expect("optimizer decision remains attached");
        assert_eq!(decision.base_atoms, 11_000_000);
        assert_eq!(decision.output_quote_atoms, 10_350_000);
        assert_eq!(decision.net_profit_quote_atoms, 345_000);
    }

    #[test]
    fn focused_native_candidates_reject_fixed_size_mapped_direct_fallback() {
        let db = crate::db::ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (pair, _family_id, edge_id) = seed_engine_route_edge(&db);
        let mut config = test_config();
        config.database.enabled = true;
        config.trading.min_net_profit_bps = 0.0;
        config.execution.native_price_only_execution_enabled = true;
        config.execution.jupiter_probe_enabled = false;
        config.execution.max_live_trade_usd = 1_000.0;
        config.execution.direct_trade_sizes_usd = vec![1.0, 10.0, 50.0];
        config.execution.native_focus_routes = vec![crate::config::NativeFocusRoute {
            base_mint: pair.base_mint.clone(),
            quote_mint: pair.quote_mint.clone(),
            buy_venue: "meteora".to_string(),
            sell_venue: "raydium".to_string(),
            require_fresh_accounts: true,
        }];
        let engine = BotEngine::new(config).with_database_for_test(db);
        let buy = pool(
            crate::types::Venue::Meteora,
            "MeteoraEnginePool111111111111111111111111",
            1_000_000_000_000,
        );
        let sell = pool(
            crate::types::Venue::Raydium,
            "RaydiumEnginePool111111111111111111111111",
            1_100_000_000_000,
        );
        let opportunity = engine
            .evaluate_pair_at_trade_size(&buy, &sell, 10.0, FeeBreakdown::default(), 100.0)
            .expect("fixed-size direct opportunity");

        let (candidates, source, _) = engine.execution_candidates_for_cycle(
            &[buy, sell],
            &[opportunity],
            &[pair],
            &FeeBreakdown::default(),
            100.0,
            None,
        );

        assert_eq!(source, "focused native optimized route unavailable");
        assert!(
            candidates.is_empty(),
            "focused route mode must not fall back to fixed-size direct opportunities for edge {edge_id}"
        );
    }

    #[test]
    fn focused_route_pools_are_appended_to_promoted_pool_set() {
        let db = crate::db::ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (pair, _family_id, edge_id) = seed_engine_route_edge(&db);
        db.insert_account_manifest(&crate::db::AccountManifestGraphRecord {
            route_edge_id: edge_id,
            manifest_version: 1,
            complete: true,
            missing_roles_json: "[]".to_string(),
            account_roles_json: "{}".to_string(),
            refreshed_slot: Some(1),
            refreshed_at_unix_ms: 1_000,
            decode_status: "complete".to_string(),
        })
        .expect("account manifest");
        let targets = db
            .complete_route_account_fetch_targets(10)
            .expect("focused route targets");
        let buy = pool(
            crate::types::Venue::Meteora,
            "MeteoraEnginePool111111111111111111111111",
            1_000_000_000_000,
        );
        let sell = pool(
            crate::types::Venue::Raydium,
            "RaydiumEnginePool111111111111111111111111",
            1_100_000_000_000,
        );
        let unrelated = pool(
            crate::types::Venue::Orca,
            "UnrelatedPool1111111111111111111111111111",
            1_200_000_000_000,
        );
        let focus_routes = vec![crate::config::NativeFocusRoute {
            base_mint: pair.base_mint.clone(),
            quote_mint: pair.quote_mint.clone(),
            buy_venue: "meteora".to_string(),
            sell_venue: "raydium".to_string(),
            require_fresh_accounts: true,
        }];
        let mut promoted = vec![buy.clone()];

        append_focused_route_pools(
            &mut promoted,
            &targets,
            vec![buy, sell.clone(), unrelated],
            &focus_routes,
            None,
        );

        let promoted_addresses = promoted
            .iter()
            .map(|pool| pool.address.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            promoted_addresses,
            vec![
                "MeteoraEnginePool111111111111111111111111",
                "RaydiumEnginePool111111111111111111111111",
            ]
        );
        assert!(promoted.iter().any(|pool| {
            pool.address == sell.address && pool.venue == crate::types::Venue::Raydium
        }));
    }

    #[test]
    fn focused_route_pools_fall_back_to_db_price_liquidity_when_decode_is_unavailable() {
        let db = crate::db::ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (pair, _family_id, edge_id) = seed_engine_route_edge(&db);
        db.insert_account_manifest(&crate::db::AccountManifestGraphRecord {
            route_edge_id: edge_id,
            manifest_version: 1,
            complete: true,
            missing_roles_json: "[]".to_string(),
            account_roles_json: "{}".to_string(),
            refreshed_slot: Some(1),
            refreshed_at_unix_ms: 1_000,
            decode_status: "complete".to_string(),
        })
        .expect("account manifest");
        let targets = db
            .complete_route_account_fetch_targets(10)
            .expect("focused route targets");
        let focus_routes = vec![crate::config::NativeFocusRoute {
            base_mint: pair.base_mint.clone(),
            quote_mint: pair.quote_mint.clone(),
            buy_venue: "meteora".to_string(),
            sell_venue: "raydium".to_string(),
            require_fresh_accounts: true,
        }];
        let mut promoted = Vec::new();

        append_focused_route_pools(&mut promoted, &targets, Vec::new(), &focus_routes, None);

        assert_eq!(promoted.len(), 2);
        assert!(promoted.iter().any(|pool| {
            pool.address == "MeteoraEnginePool111111111111111111111111"
                && pool.sqrt_price_x64.is_some()
                && pool.liquidity_quote_atoms > 0
        }));
        assert!(promoted.iter().any(|pool| {
            pool.address == "RaydiumEnginePool111111111111111111111111"
                && pool.sqrt_price_x64.is_some()
                && pool.liquidity_quote_atoms > 0
        }));
    }

    #[test]
    fn focused_route_append_replaces_weak_price_only_promoted_snapshot() {
        let db = crate::db::ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (pair, _family_id, edge_id) = seed_engine_route_edge(&db);
        db.insert_account_manifest(&crate::db::AccountManifestGraphRecord {
            route_edge_id: edge_id,
            manifest_version: 1,
            complete: true,
            missing_roles_json: "[]".to_string(),
            account_roles_json: "{}".to_string(),
            refreshed_slot: Some(1),
            refreshed_at_unix_ms: 1_000,
            decode_status: "complete".to_string(),
        })
        .expect("account manifest");
        let targets = db
            .complete_route_account_fetch_targets(10)
            .expect("focused route targets");
        let focus_routes = vec![crate::config::NativeFocusRoute {
            base_mint: pair.base_mint.clone(),
            quote_mint: pair.quote_mint.clone(),
            buy_venue: "meteora".to_string(),
            sell_venue: "raydium".to_string(),
            require_fresh_accounts: true,
        }];
        let mut weak_buy = pool(
            crate::types::Venue::Meteora,
            "MeteoraEnginePool111111111111111111111111",
            1_000_000_000_000,
        );
        weak_buy.reserve_base_atoms = 0;
        weak_buy.reserve_quote_atoms = 0;
        weak_buy.sqrt_price_x64 = sqrt_price_x64_from_price(1.0, 6, 6);
        weak_buy.liquidity_quote_atoms = 1_000_000_000_000;
        weak_buy.updated_slot = 42;
        let mut promoted = vec![weak_buy];

        append_focused_route_pools(&mut promoted, &targets, Vec::new(), &focus_routes, None);

        let buy = promoted
            .iter()
            .find(|pool| pool.address == "MeteoraEnginePool111111111111111111111111")
            .expect("buy pool remains promoted");
        assert_eq!(buy.updated_slot, 0);
        assert!(pool_has_focused_route_price_basis(buy));
        assert!(promoted.iter().any(|pool| {
            pool.address == "RaydiumEnginePool111111111111111111111111"
                && pool.updated_slot == 0
                && pool_has_focused_route_price_basis(pool)
        }));
    }

    #[test]
    fn focused_route_append_materializes_zero_liquidity_price_snapshot_with_execution_cap() {
        let db = crate::db::ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (pair, _family_id, edge_id) = seed_engine_route_edge(&db);
        db.conn_for_test()
            .execute(
                "UPDATE pools SET liquidity_quote_ui = 0.0 WHERE pool_address IN (?1, ?2)",
                [
                    "MeteoraEnginePool111111111111111111111111",
                    "RaydiumEnginePool111111111111111111111111",
                ],
            )
            .expect("zero route graph liquidity");
        db.insert_account_manifest(&crate::db::AccountManifestGraphRecord {
            route_edge_id: edge_id,
            manifest_version: 1,
            complete: true,
            missing_roles_json: "[]".to_string(),
            account_roles_json: "{}".to_string(),
            refreshed_slot: Some(1),
            refreshed_at_unix_ms: 1_000,
            decode_status: "complete".to_string(),
        })
        .expect("account manifest");
        let targets = db
            .complete_route_account_fetch_targets(10)
            .expect("focused route targets");
        let focus_routes = vec![crate::config::NativeFocusRoute {
            base_mint: pair.base_mint.clone(),
            quote_mint: pair.quote_mint.clone(),
            buy_venue: "meteora".to_string(),
            sell_venue: "raydium".to_string(),
            require_fresh_accounts: true,
        }];
        let mut config = test_config();
        config.execution.max_live_trade_usd = 10.0;
        let mut promoted = Vec::new();

        append_focused_route_pools_with_fallback(
            &mut promoted,
            &targets,
            Vec::new(),
            &focus_routes,
            Some((&config, 100.0)),
        );

        assert_eq!(promoted.len(), 2);
        assert!(promoted.iter().all(|pool| {
            pool.updated_slot == 0
                && pool.sqrt_price_x64.is_some()
                && pool.liquidity_quote_atoms > 0
                && pool_has_focused_route_price_basis(pool)
        }));
    }

    #[test]
    fn focused_route_account_cache_age_has_floor_separate_from_candidate_ttl() {
        assert_eq!(focused_route_account_max_age_ms(250), 5_000);
        assert_eq!(focused_route_account_max_age_ms(6_000), 6_000);
    }

    #[test]
    fn selected_route_prequote_refresh_uses_strict_candidate_ttl() {
        assert_eq!(focused_route_prequote_refresh_max_age_ms(250), 50);
        assert_eq!(focused_route_prequote_refresh_max_age_ms(40), 8);
        assert_eq!(focused_route_prequote_refresh_max_age_ms(0), 0);
    }

    #[test]
    fn hot_cache_refreshes_missing_and_stale_selected_accounts() {
        let now = Instant::now();
        let fresh = CachedAccount {
            pubkey: "fresh".to_string(),
            owner: "owner".to_string(),
            lamports: 1,
            data: vec![1],
            slot: 1,
            updated_at: now - Duration::from_millis(100),
        };
        let stale = CachedAccount {
            pubkey: "stale".to_string(),
            owner: "owner".to_string(),
            lamports: 1,
            data: vec![1],
            slot: 1,
            updated_at: now - Duration::from_millis(1_501),
        };

        assert!(hot_cache_entry_needs_refresh(None, 1_500, now));
        assert!(!hot_cache_entry_needs_refresh(Some(&fresh), 1_500, now));
        assert!(hot_cache_entry_needs_refresh(Some(&stale), 1_500, now));
        assert!(!hot_cache_entry_needs_refresh(Some(&stale), 0, now));
    }

    #[test]
    fn unmapped_paper_simulation_evidence_is_not_persisted_as_quote_noise() {
        let db = crate::db::ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let pair = TokenPair {
            base_mint: "UNMAPPED_BASE".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 6,
            quote_decimals: 6,
        };
        let opportunity = Opportunity {
            pair: pair.clone(),
            buy_venue: crate::types::Venue::Meteora,
            sell_venue: crate::types::Venue::Raydium,
            buy_pool: "UnmappedMeteoraPool111111111111111111111".to_string(),
            sell_pool: "UnmappedRaydiumPool111111111111111111111".to_string(),
            input_quote_atoms: 1_000_000,
            base_atoms: 2_000_000,
            output_quote_atoms: 1_020_000,
            gross_profit_quote_atoms: 20_000,
            net_profit_usd: 0.02,
            net_profit_bps: 200.0,
            spread_bps: 220.0,
            fee: FeeBreakdown::default(),
            detected_slot: 123,
            optimizer_decision: None,
        };

        persist_simulation_evidence(&db, 12, &opportunity, "attempt-context", 12_000)
            .expect("simulation evidence");

        let quote_observations: i64 = db
            .conn_for_test()
            .query_row("SELECT COUNT(*) FROM quote_observations", [], |row| {
                row.get(0)
            })
            .expect("quote count");
        assert_eq!(quote_observations, 0);
        let simulation_results: i64 = db
            .conn_for_test()
            .query_row("SELECT COUNT(*) FROM simulation_results", [], |row| {
                row.get(0)
            })
            .expect("simulation count");
        assert_eq!(simulation_results, 0);
    }

    #[tokio::test]
    async fn live_selection_skips_jupiter_buy_leg_no_route_without_blocking_native() {
        let engine = BotEngine::new(test_config());
        let pair = TokenPair {
            base_mint: "BASE".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 6,
            quote_decimals: 6,
        };
        let candidate = |sell_venue: Venue, sell_pool: &str| Opportunity {
            pair: pair.clone(),
            buy_venue: Venue::Orca,
            sell_venue,
            buy_pool: "jupiter-probe:orca".to_string(),
            sell_pool: sell_pool.to_string(),
            input_quote_atoms: 25_000_000,
            base_atoms: 0,
            output_quote_atoms: 25_000_000,
            gross_profit_quote_atoms: 0,
            net_profit_usd: 0.0,
            net_profit_bps: 0.0,
            spread_bps: 0.0,
            fee: FeeBreakdown::default(),
            detected_slot: 0,
            optimizer_decision: None,
        };
        let jupiter_probe_a = candidate(Venue::Meteora, "jupiter-probe:meteora");
        let jupiter_probe_b = candidate(Venue::Raydium, "jupiter-probe:raydium");
        let mut native = candidate(Venue::Meteora, "11111111111111111111111111111111");
        native.buy_pool = "11111111111111111111111111111111".to_string();

        engine.jupiter_no_route_leg_cooldowns.write().await.insert(
            jupiter_requote_buy_leg_key(&jupiter_probe_a),
            Instant::now() + Duration::from_secs(60),
        );

        let selection = engine
            .select_live_candidate_indices(&[jupiter_probe_a, jupiter_probe_b, native], 3, 0, 100.0)
            .await;

        assert_eq!(selection.indices, vec![2]);
        assert_eq!(selection.cooldown_count, 2);
    }

    #[tokio::test]
    async fn live_selection_skips_structurally_oversized_route_across_sizes() {
        let engine = BotEngine::new(test_config());
        let pair = TokenPair {
            base_mint: "BASE".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 6,
            quote_decimals: 6,
        };
        let candidate = |amount, sell_pool: &str| Opportunity {
            pair: pair.clone(),
            buy_venue: Venue::Orca,
            sell_venue: Venue::Meteora,
            buy_pool: "11111111111111111111111111111111".to_string(),
            sell_pool: sell_pool.to_string(),
            input_quote_atoms: amount,
            base_atoms: amount,
            output_quote_atoms: amount,
            gross_profit_quote_atoms: 0,
            net_profit_usd: 0.0,
            net_profit_bps: 0.0,
            spread_bps: 0.0,
            fee: FeeBreakdown::default(),
            detected_slot: 0,
            optimizer_decision: None,
        };
        let small = candidate(2_000_000, "22222222222222222222222222222222");
        let larger_same_route = candidate(3_000_000, "22222222222222222222222222222222");
        let mut other_route = candidate(3_000_000, "33333333333333333333333333333333");
        other_route.sell_venue = Venue::Raydium;

        engine.jupiter_no_route_leg_cooldowns.write().await.insert(
            structural_route_cooldown_key(&small),
            Instant::now() + Duration::from_secs(60),
        );

        let selection = engine
            .select_live_candidate_indices(&[small, larger_same_route, other_route], 3, 0, 100.0)
            .await;

        assert_eq!(selection.indices, vec![2]);
        assert_eq!(selection.cooldown_count, 2);
    }

    #[tokio::test]
    async fn live_selection_skips_structural_pair_venue_family() {
        let engine = BotEngine::new(test_config());
        let pair = TokenPair {
            base_mint: "BASE".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 6,
            quote_decimals: 6,
        };
        let candidate = |sell_pool: &str| Opportunity {
            pair: pair.clone(),
            buy_venue: Venue::Orca,
            sell_venue: Venue::Meteora,
            buy_pool: "11111111111111111111111111111111".to_string(),
            sell_pool: sell_pool.to_string(),
            input_quote_atoms: 2_000_000,
            base_atoms: 2_000_000,
            output_quote_atoms: 2_000_000,
            gross_profit_quote_atoms: 0,
            net_profit_usd: 0.0,
            net_profit_bps: 0.0,
            spread_bps: 0.0,
            fee: FeeBreakdown::default(),
            detected_slot: 0,
            optimizer_decision: None,
        };
        let first = candidate("22222222222222222222222222222222");
        let same_pair_venue_different_pool = candidate("33333333333333333333333333333333");

        engine.jupiter_no_route_leg_cooldowns.write().await.insert(
            structural_pair_venue_cooldown_key(&first),
            Instant::now() + Duration::from_secs(60),
        );

        let selection = engine
            .select_live_candidate_indices(&[first, same_pair_venue_different_pool], 2, 0, 100.0)
            .await;

        assert!(selection.indices.is_empty());
        assert_eq!(selection.cooldown_count, 2);
    }

    #[tokio::test]
    async fn live_selection_skips_whole_pair_after_slippage_failure() {
        let engine = BotEngine::new(test_config());
        let pair = TokenPair {
            base_mint: "BASE".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 6,
            quote_decimals: 6,
        };
        let candidate = |buy_venue: Venue, sell_venue: Venue| Opportunity {
            pair: pair.clone(),
            buy_venue,
            sell_venue,
            buy_pool: format!("buy-{buy_venue:?}"),
            sell_pool: format!("sell-{sell_venue:?}"),
            input_quote_atoms: 2_000_000,
            base_atoms: 2_000_000,
            output_quote_atoms: 2_000_000,
            gross_profit_quote_atoms: 0,
            net_profit_usd: 0.0,
            net_profit_bps: 0.0,
            spread_bps: 0.0,
            fee: FeeBreakdown::default(),
            detected_slot: 0,
            optimizer_decision: None,
        };
        let first = candidate(Venue::Meteora, Venue::Raydium);
        let same_pair_different_venues = candidate(Venue::Raydium, Venue::Orca);

        engine.jupiter_no_route_leg_cooldowns.write().await.insert(
            structural_pair_cooldown_key(&first),
            Instant::now() + Duration::from_secs(60),
        );

        let selection = engine
            .select_live_candidate_indices(&[first, same_pair_different_venues], 2, 0, 100.0)
            .await;

        assert!(selection.indices.is_empty());
        assert_eq!(selection.cooldown_count, 2);
    }

    #[test]
    fn no_route_leg_keys_target_the_failed_requote_side() {
        let opportunity = Opportunity {
            pair: TokenPair {
                base_mint: "BASE".to_string(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 6,
                quote_decimals: 6,
            },
            buy_venue: Venue::Orca,
            sell_venue: Venue::Meteora,
            buy_pool: "jupiter-probe:orca".to_string(),
            sell_pool: "jupiter-probe:meteora".to_string(),
            input_quote_atoms: 25_000_000,
            base_atoms: 12_000_000,
            output_quote_atoms: 25_000_000,
            gross_profit_quote_atoms: 0,
            net_profit_usd: 0.0,
            net_profit_bps: 0.0,
            spread_bps: 0.0,
            fee: FeeBreakdown::default(),
            detected_slot: 0,
            optimizer_decision: None,
        };

        let keys = jupiter_no_route_leg_keys_for_reason(
            &opportunity,
            "buy leg re-quote failed: No routes found",
        );

        assert_eq!(keys, vec![jupiter_requote_buy_leg_key(&opportunity)]);
    }

    #[test]
    fn direct_signal_priority_prefers_profitable_pool_signal() {
        let mut opportunity = Opportunity {
            pair: TokenPair {
                base_mint: "BASE".to_string(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 6,
                quote_decimals: 6,
            },
            buy_venue: Venue::Orca,
            sell_venue: Venue::Meteora,
            buy_pool: "buy".to_string(),
            sell_pool: "sell".to_string(),
            input_quote_atoms: 4_000_000,
            base_atoms: 0,
            output_quote_atoms: 4_000_000,
            gross_profit_quote_atoms: 0,
            net_profit_usd: 0.0,
            net_profit_bps: 0.0,
            spread_bps: 0.0,
            fee: FeeBreakdown::default(),
            detected_slot: 0,
            optimizer_decision: None,
        };

        assert_eq!(direct_signal_priority(&opportunity), 0.0);
        opportunity.net_profit_bps = 25.0;
        opportunity.net_profit_usd = 0.01;
        assert!(direct_signal_priority(&opportunity) > 250.0);
    }

    #[test]
    fn oversized_transaction_errors_get_candidate_cooldown() {
        let cooldown = probe_cooldown_duration(
            Some("signed transaction is 1400 byte(s), above live max 1232"),
            Some(50.0),
            false,
        );
        assert_eq!(cooldown, Some(Duration::from_secs(180)));
    }

    #[test]
    fn focused_preflight_and_fresh_quote_errors_get_short_candidate_cooldowns() {
        let preflight = probe_cooldown_duration(
            Some("focused route 21 preflight failed: focused route 21 is not ready for quote"),
            None,
            false,
        );
        assert_eq!(preflight, Some(Duration::from_secs(30)));

        let fresh_quote = probe_cooldown_duration(
            Some("native direct fresh buy quote unavailable: DLMM bin array quote failed"),
            Some(11_000.0),
            false,
        );
        assert_eq!(fresh_quote, Some(Duration::from_secs(10)));

        let zero_output = probe_cooldown_duration(
            Some("native direct fresh buy quote produced zero output"),
            Some(11_000.0),
            false,
        );
        assert_eq!(zero_output, Some(Duration::from_secs(10)));
    }

    #[test]
    fn zero_output_fresh_quote_errors_get_structural_route_cooldown() {
        let opportunity = Opportunity {
            pair: TokenPair {
                base_mint: "BASE".to_string(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 6,
                quote_decimals: 6,
            },
            buy_venue: crate::types::Venue::Meteora,
            sell_venue: crate::types::Venue::Raydium,
            buy_pool: "MeteoraEnginePool111111111111111111111111".to_string(),
            sell_pool: "RaydiumEnginePool111111111111111111111111".to_string(),
            input_quote_atoms: 1_000_000,
            base_atoms: 0,
            output_quote_atoms: 0,
            gross_profit_quote_atoms: 0,
            net_profit_usd: 0.0,
            net_profit_bps: 0.0,
            spread_bps: 0.0,
            fee: FeeBreakdown::default(),
            detected_slot: 0,
            optimizer_decision: None,
        };
        let reason = "native direct fresh buy quote produced zero output";

        assert_eq!(
            structural_route_cooldown_duration(reason),
            Some(Duration::from_secs(10))
        );
        assert_eq!(
            structural_route_cooldown_keys_for_reason(&opportunity, reason),
            vec![structural_route_cooldown_key(&opportunity)]
        );
    }

    #[test]
    fn below_required_profit_errors_get_short_candidate_cooldown() {
        let cooldown = probe_cooldown_duration(
            Some("native direct re-quote net profit 142.95 bps is below required 350.00 bps"),
            Some(142.95),
            false,
        );
        assert_eq!(cooldown, Some(Duration::from_secs(30)));

        let cooldown = probe_cooldown_duration(
            Some("native direct re-quote net profit 246.13 bps is below required 350.00 bps"),
            Some(246.13),
            false,
        );
        assert_eq!(cooldown, Some(Duration::from_secs(30)));

        let cooldown = probe_cooldown_duration(
            Some("native direct re-quote net profit 333.00 bps is below required 350.00 bps"),
            Some(333.0),
            false,
        );
        assert_eq!(cooldown, Some(Duration::from_secs(10)));

        let cooldown = probe_cooldown_duration(
            Some("native direct re-quote net profit -245.12 bps is below required 350.00 bps"),
            Some(-245.12),
            false,
        );
        assert_eq!(cooldown, Some(Duration::from_secs(180)));
    }

    #[test]
    fn slippage_errors_get_candidate_cooldown() {
        let cooldown = probe_cooldown_duration(
            Some("{\"InstructionError\":[7,{\"Custom\":6005}]}"),
            Some(50.0),
            true,
        );
        assert_eq!(cooldown, Some(Duration::from_secs(600)));

        let cooldown = probe_cooldown_duration(
            Some("{\"InstructionError\":[7,{\"Custom\":6004}]}"),
            Some(500.0),
            true,
        );
        assert_eq!(cooldown, Some(Duration::from_secs(600)));

        let cooldown = probe_cooldown_duration(
            Some("{\"InstructionError\":[4,{\"Custom\":6038}]}"),
            Some(600.0),
            true,
        );
        assert_eq!(cooldown, Some(Duration::from_secs(600)));
    }

    #[test]
    fn unsupported_native_pool_errors_get_structural_cooldown() {
        let reason = "native direct instruction build failed: native sell instruction build failed for orca 3THMPkPmcHJ54JtHRyazhs7UN7HbV5KiNJVLJs6hSPSC: unsupported native orca pool owner 9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP";
        let cooldown = probe_cooldown_duration(Some(reason), None, false);
        assert_eq!(cooldown, Some(Duration::from_secs(3_600)));
        assert_eq!(
            structural_route_cooldown_duration(reason),
            Some(Duration::from_secs(3_600))
        );
    }

    #[test]
    fn structural_route_cooldown_only_applies_to_structural_failures() {
        assert_eq!(
            structural_route_cooldown_duration(
                "signed transaction is 1254 byte(s), above live max 1232"
            ),
            Some(Duration::from_secs(180))
        );
        assert_eq!(
            structural_route_cooldown_duration("{\"InstructionError\":[7,{\"Custom\":6002}]}"),
            Some(Duration::from_secs(600))
        );
        assert_eq!(
            structural_route_cooldown_duration("{\"InstructionError\":[7,{\"Custom\":6004}]}"),
            Some(Duration::from_secs(600))
        );
        assert_eq!(
            structural_route_cooldown_duration("{\"InstructionError\":[4,{\"Custom\":6038}]}"),
            Some(Duration::from_secs(600))
        );
        assert_eq!(
            structural_route_cooldown_duration(
                "native direct instruction build failed: unsupported native raydium pool owner 11111111111111111111111111111111"
            ),
            Some(Duration::from_secs(3_600))
        );
        assert_eq!(
            structural_route_cooldown_duration(
                "native direct re-quote net profit 133.52 bps is below required 500.00 bps"
            ),
            None
        );
    }

    #[test]
    fn fundable_live_candidates_excludes_unfunded_quote_mints() {
        let pair = TokenPair {
            base_mint: "BASE".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 6,
            quote_decimals: 6,
        };
        let small = Opportunity {
            pair: pair.clone(),
            buy_venue: Venue::Orca,
            sell_venue: Venue::Meteora,
            buy_pool: "buy".to_string(),
            sell_pool: "sell".to_string(),
            input_quote_atoms: 50_000,
            base_atoms: 0,
            output_quote_atoms: 50_000,
            gross_profit_quote_atoms: 0,
            net_profit_usd: 0.0,
            net_profit_bps: 0.0,
            spread_bps: 0.0,
            fee: FeeBreakdown::default(),
            detected_slot: 0,
            optimizer_decision: None,
        };
        let mut large = small.clone();
        large.input_quote_atoms = 25_000_000;
        let mut balances = HashMap::new();
        balances.insert(USDC_MINT.to_string(), 81_325u128);

        let (fundable, blocked) = fundable_live_candidates(&[large, small], &balances, 150.0);

        assert_eq!(fundable.len(), 1);
        assert_eq!(fundable[0].input_quote_atoms, 50_000);
        assert_eq!(blocked.blocked_count, 1);
        assert_eq!(blocked.best_required_usd, Some(25.0));
    }

    #[test]
    fn diverse_live_probe_selection_prefers_distinct_pairs() {
        let pair_a = TokenPair {
            base_mint: "BASEA".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 6,
            quote_decimals: 6,
        };
        let pair_b = TokenPair {
            base_mint: "BASEB".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 6,
            quote_decimals: 6,
        };
        let candidate = |pair: TokenPair| Opportunity {
            pair,
            buy_venue: Venue::Orca,
            sell_venue: Venue::Meteora,
            buy_pool: "buy".to_string(),
            sell_pool: "sell".to_string(),
            input_quote_atoms: 1_000_000,
            base_atoms: 0,
            output_quote_atoms: 1_000_000,
            gross_profit_quote_atoms: 0,
            net_profit_usd: 0.0,
            net_profit_bps: 0.0,
            spread_bps: 0.0,
            fee: FeeBreakdown::default(),
            detected_slot: 0,
            optimizer_decision: None,
        };
        let candidates = vec![
            candidate(pair_a.clone()),
            candidate(pair_a),
            candidate(pair_b.clone()),
            candidate(pair_b),
        ];
        let ranked = vec![
            (0, 10_000.0, 0, "a1".to_string()),
            (1, 9_999.0, 1, "a2".to_string()),
            (2, 9_998.0, 2, "b1".to_string()),
            (3, 9_997.0, 3, "b2".to_string()),
        ];

        let selected = diverse_live_probe_selection(ranked, &candidates, 2);

        assert_eq!(
            selected.iter().map(|item| item.0).collect::<Vec<_>>(),
            vec![0, 2]
        );
    }

    #[test]
    fn repeat_confirmed_rwa_candidate_requires_confirmed_and_finite_room() {
        assert!(should_repeat_confirmed_rwa_candidate(true, 1, 4, true));
        assert!(!should_repeat_confirmed_rwa_candidate(false, 1, 4, true));
        assert!(!should_repeat_confirmed_rwa_candidate(true, 4, 4, true));
        assert!(!should_repeat_confirmed_rwa_candidate(
            true,
            1,
            usize::MAX,
            false
        ));
    }
}
