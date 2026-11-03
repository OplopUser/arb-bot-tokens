use crate::{
    api_swap::ApiSwapBuilder,
    config::BotConfig,
    env_runtime::{env_bool, env_value},
    native_swap::NativeSwapBuilder,
    pool_subscription::PoolSubscriptionManager,
    rpc::RpcClient,
    types::{
        atoms_to_ui, Opportunity, SwapSide, TokenPair, Venue, USDC_MINT, USDT_MINT, WSOL_MINT,
    },
};
use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose, Engine as _};
use futures_util::{stream::FuturesUnordered, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use solana_sdk::{
    address_lookup_table::state::AddressLookupTable,
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    message::{v0, AddressLookupTableAccount, Message, VersionedMessage},
    pubkey::Pubkey,
    signature::{read_keypair_file, Signature, Signer},
    transaction::VersionedTransaction,
};
use std::{
    collections::BTreeSet,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::Instant;
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveSwapRequest {
    pub input_mint: String,
    pub output_mint: String,
    pub amount_atoms: u64,
    pub slippage_bps: u16,
    pub submit: bool,
    pub max_usd: f64,
    pub estimated_input_usd_override: Option<f64>,
    pub required_route_label_contains: Option<String>,
    pub force_swap_instructions: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JupiterChainSmokeRequest {
    pub input_mint: String,
    pub output_mints: Vec<String>,
    pub amount_atoms: u64,
    pub slippage_bps: u16,
    pub max_usd: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JupiterChainSmokeLegReport {
    pub index: usize,
    pub input_mint: String,
    pub output_mint: String,
    pub input_atoms: u64,
    pub quote_out_amount: String,
    pub next_input_atoms: Option<u64>,
    pub route_labels: Vec<String>,
    pub setup_count: usize,
    pub cleanup: bool,
    pub lookup_tables: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JupiterChainSmokeReport {
    pub submit_requested: bool,
    pub submitted: bool,
    pub wallet_pubkey: String,
    pub input_mint: String,
    pub output_mints: Vec<String>,
    pub amount_atoms: u64,
    pub max_usd: f64,
    pub legs: Vec<JupiterChainSmokeLegReport>,
    pub address_lookup_tables: Vec<String>,
    pub signed_transaction_bytes: usize,
    pub simulation_ok: bool,
    pub simulation_error: Option<String>,
    pub simulation_logs: Vec<String>,
    pub blocked_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JupiterSwapInstructionChainLeg {
    pub input_mint: String,
    pub output_mint: String,
    pub required_route_label_contains: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JupiterSwapInstructionChainRequest {
    pub input_mint: String,
    pub output_mint: String,
    pub amount_atoms: u64,
    pub legs: Vec<JupiterSwapInstructionChainLeg>,
    pub slippage_bps: u16,
    pub submit: bool,
    pub max_usd: f64,
    pub estimated_input_usd_override: Option<f64>,
    pub min_output_atoms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiRouteSwapRequest {
    pub route: String,
    pub pair: TokenPair,
    pub side: SwapSide,
    pub amount_atoms: u64,
    pub slippage_bps: u16,
    pub submit: bool,
    pub max_usd: f64,
    pub estimated_input_usd_override: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalPrimaryTransactionRequest {
    pub provider: String,
    pub action: String,
    pub ticker: String,
    pub input_mint: String,
    pub output_mint: String,
    pub amount_atoms: u64,
    pub pyth_fair_price_usd: f64,
    pub pyth_confidence_bps: f64,
    pub estimated_input_usd: Option<f64>,
    pub max_usd: f64,
    pub submit: bool,
}

fn external_primary_build_payload(
    request: &ExternalPrimaryTransactionRequest,
    wallet_pubkey: &str,
    slippage_bps: u16,
) -> Value {
    json!({
        "wallet": wallet_pubkey,
        "owner": wallet_pubkey,
        "taker": wallet_pubkey,
        "provider": request.provider,
        "action": request.action,
        "ticker": request.ticker,
        "inputMint": request.input_mint,
        "outputMint": request.output_mint,
        "amount": request.amount_atoms.to_string(),
        "amountAtoms": request.amount_atoms.to_string(),
        "pythFairPriceUsd": request.pyth_fair_price_usd,
        "pyth_fair_price_usd": request.pyth_fair_price_usd,
        "pythConfidenceBps": request.pyth_confidence_bps,
        "pyth_confidence_bps": request.pyth_confidence_bps,
        "slippageBps": slippage_bps,
        "submit": false
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveSwapReport {
    pub submit_requested: bool,
    pub submitted: bool,
    pub wallet_pubkey: String,
    pub input_mint: String,
    pub output_mint: String,
    pub amount_atoms: u64,
    pub estimated_input_usd: Option<f64>,
    pub max_usd: f64,
    pub quote_out_amount: Option<String>,
    pub quote_price_impact_pct: Option<String>,
    pub route_labels: Vec<String>,
    pub last_valid_block_height: Option<u64>,
    pub prioritization_fee_lamports: Option<u64>,
    pub signed_transaction_bytes: usize,
    pub simulation_ok: bool,
    pub simulation_error: Option<String>,
    pub simulation_logs: Vec<String>,
    pub signature: Option<String>,
    pub confirmation_status: Option<String>,
    pub confirmation_error: Option<String>,
    pub solscan_url: Option<String>,
    pub blocked_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AtomicArbStatus {
    Blocked,
    Simulated,
    Submitted,
    Confirmed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtomicArbReport {
    pub status: AtomicArbStatus,
    pub submitted: bool,
    pub wallet_pubkey: String,
    pub pair: crate::types::TokenPair,
    pub buy_venue: Venue,
    pub sell_venue: Venue,
    pub input_quote_atoms: u128,
    pub buy_quote_out_amount: Option<String>,
    pub sell_quote_out_amount: Option<String>,
    pub direct_signal_net_profit_usd: f64,
    pub requote_net_profit_usd: Option<f64>,
    pub requote_net_profit_bps: Option<f64>,
    pub buy_route_labels: Vec<String>,
    pub sell_route_labels: Vec<String>,
    pub address_lookup_tables: Vec<String>,
    pub signed_transaction_bytes: usize,
    pub simulation_ok: bool,
    pub simulation_error: Option<String>,
    pub simulation_logs: Vec<String>,
    pub signature: Option<String>,
    pub confirmation_status: Option<String>,
    pub confirmation_error: Option<String>,
    pub solscan_url: Option<String>,
    pub blocked_reason: Option<String>,
    pub jupiter_reference_net_profit_bps: Option<f64>,
    pub jupiter_reference_buy_quote_out_amount: Option<String>,
    pub jupiter_reference_sell_quote_out_amount: Option<String>,
    pub jupiter_reference_net_profit_usd: Option<f64>,
    pub jupiter_reference_error: Option<String>,
    pub native_execution_cost_quote_atoms: Option<String>,
    pub native_setup_missing_atas: Option<usize>,
    pub native_simulation_pre_quote_atoms: Option<String>,
    pub native_simulation_post_quote_atoms: Option<String>,
    pub native_simulation_quote_delta_atoms: Option<String>,
    pub native_simulation_net_profit_atoms: Option<String>,
    pub latency_trace: Vec<ExecutionLatencyMark>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionLatencyMark {
    pub stage: String,
    pub elapsed_ms: u64,
}

pub struct JupiterLiveSwapExecutor {
    config: BotConfig,
    client: Client,
    rpc: RpcClient,
    use_jito: bool,
    rpc_urls: Vec<String>,
    pool_subscription: Option<Arc<PoolSubscriptionManager>>,
    native_market_refresh_pubkeys: Vec<String>,
    native_session_submit_attempts: Arc<AtomicU64>,
    native_cycle_submit_attempts: Arc<AtomicU64>,
}

struct SimulationOutcome {
    ok: bool,
    error: Option<String>,
    logs: Vec<String>,
}

struct JupiterSwapInstructionChainQuote {
    quote: Value,
    labels: Vec<String>,
}

struct JupiterChainSmokeQuotedLeg {
    input_mint: String,
    output_mint: String,
    input_atoms: u64,
    quote_out_amount: u64,
    next_input_atoms: Option<u64>,
    route_labels: Vec<String>,
    quote: Value,
}

impl JupiterLiveSwapExecutor {
    pub fn new(config: BotConfig) -> Self {
        let rpc = RpcClient::new(&config.rpc);
        let use_jito = config.submitters.enabled.iter().any(|s| s == "jito")
            && !config.execution.jito_block_engine_url.is_empty();
        if use_jito {
            info!(
                "Jito bundle submission enabled: {}",
                config.execution.jito_block_engine_url
            );
        } else {
            info!(
                "Jito disabled (enabled={:?}, url='{}')",
                config.submitters.enabled, config.execution.jito_block_engine_url
            );
        }
        let mut rpc_urls = vec![config.rpc.primary_http.clone()];
        rpc_urls.extend(config.rpc.fallback_http.iter().cloned());
        Self {
            config,
            client: Client::new(),
            rpc,
            use_jito,
            rpc_urls,
            pool_subscription: None,
            native_market_refresh_pubkeys: Vec::new(),
            native_session_submit_attempts: Arc::new(AtomicU64::new(0)),
            native_cycle_submit_attempts: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn with_pool_subscription(mut self, sub: Arc<PoolSubscriptionManager>) -> Self {
        self.pool_subscription = Some(sub);
        self
    }

    pub fn with_native_market_refresh_pubkeys(mut self, pubkeys: Vec<String>) -> Self {
        self.native_market_refresh_pubkeys = pubkeys;
        self
    }

    pub async fn run(&self, request: LiveSwapRequest) -> Result<LiveSwapReport> {
        if !request.force_swap_instructions
            && jupiter_v2_execution_enabled()
            && jupiter_api_key().is_some()
        {
            return self.run_v2_order(request).await;
        }

        let estimated_input_usd = request.estimated_input_usd_override.or_else(|| {
            estimate_input_usd(
                &request.input_mint,
                request.amount_atoms,
                self.config.trading.sol_usd_estimate,
            )
        });
        if let Some(estimated) = estimated_input_usd {
            if estimated > request.max_usd {
                return Err(anyhow!(
                    "requested live smoke input is ${estimated:.6}, above max ${:.6}",
                    request.max_usd
                ));
            }
        } else if request.submit {
            return Err(anyhow!(
                "cannot submit unknown-value input mint {}; use WSOL, USDC, or USDT for live smoke tests",
                request.input_mint
            ));
        }

        let wallet = read_keypair_file(&self.config.wallet.path).map_err(|error| {
            anyhow!(
                "failed to read wallet keypair {}: {error}",
                self.config.wallet.path
            )
        })?;
        let wallet_pubkey = wallet.pubkey().to_string();

        let quote = self.fetch_quote(&request).await?;
        let swap = self.fetch_swap(&quote, &wallet_pubkey).await?;
        let signed_bytes = sign_swap_transaction(&swap.swap_transaction, &wallet)?;
        let signed_base64 = general_purpose::STANDARD.encode(&signed_bytes);

        let simulation = self.maybe_simulate(&signed_base64).await?;

        let mut report = LiveSwapReport {
            submit_requested: request.submit,
            submitted: false,
            wallet_pubkey,
            input_mint: request.input_mint,
            output_mint: request.output_mint,
            amount_atoms: request.amount_atoms,
            estimated_input_usd,
            max_usd: request.max_usd,
            quote_out_amount: quote
                .get("outAmount")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            quote_price_impact_pct: quote
                .get("priceImpactPct")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            route_labels: route_labels(&quote),
            last_valid_block_height: swap.last_valid_block_height,
            prioritization_fee_lamports: swap.prioritization_fee_lamports,
            signed_transaction_bytes: signed_bytes.len(),
            simulation_ok: simulation.ok,
            simulation_error: simulation.error,
            simulation_logs: simulation.logs,
            signature: None,
            confirmation_status: None,
            confirmation_error: None,
            solscan_url: None,
            blocked_reason: None,
        };

        if !report.simulation_ok && self.config.execution.require_simulation {
            report.blocked_reason =
                Some("simulation failed; transaction was not submitted".to_string());
            return Ok(report);
        }

        if !request.submit {
            report.blocked_reason =
                Some("submit=false; transaction was signed and simulated only".to_string());
            return Ok(report);
        }

        let signature = self.send_transaction(&signed_base64).await?;
        report.submitted = true;
        report.solscan_url = Some(format!("https://solscan.io/tx/{signature}"));
        report.signature = Some(signature.clone());

        let confirmation = self.confirm(&signature).await?;
        report.confirmation_status = confirmation.status;
        report.confirmation_error = confirmation.error;
        Ok(report)
    }

    async fn run_v2_order(&self, request: LiveSwapRequest) -> Result<LiveSwapReport> {
        let estimated_input_usd = request.estimated_input_usd_override.or_else(|| {
            estimate_input_usd(
                &request.input_mint,
                request.amount_atoms,
                self.config.trading.sol_usd_estimate,
            )
        });
        if let Some(estimated) = estimated_input_usd {
            if estimated > request.max_usd {
                return Err(anyhow!(
                    "requested live input is ${estimated:.6}, above max ${:.6}",
                    request.max_usd
                ));
            }
        } else if request.submit {
            return Err(anyhow!(
                "cannot submit unknown-value input mint {}; use WSOL, USDC, USDT, or pass an estimated value override",
                request.input_mint
            ));
        }

        let wallet = read_keypair_file(&self.config.wallet.path).map_err(|error| {
            anyhow!(
                "failed to read wallet keypair {}: {error}",
                self.config.wallet.path
            )
        })?;
        let wallet_pubkey = wallet.pubkey().to_string();

        let order = self.fetch_v2_order(&request, &wallet_pubkey).await?;
        let route_labels = v2_route_labels(&order);
        let mut report = LiveSwapReport {
            submit_requested: request.submit,
            submitted: false,
            wallet_pubkey,
            input_mint: request.input_mint.clone(),
            output_mint: request.output_mint.clone(),
            amount_atoms: request.amount_atoms,
            estimated_input_usd,
            max_usd: request.max_usd,
            quote_out_amount: order
                .get("outAmount")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            quote_price_impact_pct: order
                .get("priceImpactPct")
                .or_else(|| order.get("priceImpact"))
                .map(Value::to_string),
            route_labels: route_labels.clone(),
            last_valid_block_height: order.get("lastValidBlockHeight").and_then(Value::as_u64),
            prioritization_fee_lamports: order
                .get("prioritizationFeeLamports")
                .and_then(Value::as_u64),
            signed_transaction_bytes: 0,
            simulation_ok: false,
            simulation_error: None,
            simulation_logs: Vec::new(),
            signature: None,
            confirmation_status: None,
            confirmation_error: None,
            solscan_url: None,
            blocked_reason: None,
        };

        if let Some(required) = request.required_route_label_contains.as_deref() {
            if !v2_order_matches_route_guard(&order, &route_labels, required) {
                report.blocked_reason = Some(format!(
                    "Jupiter v2 route guard mismatch; required route containing '{required}', got {}",
                    route_labels.join(", ")
                ));
                return Ok(report);
            }
        }

        let transaction = order
            .get("transaction")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("Jupiter v2 order did not return a transaction"))?;
        let request_id = order
            .get("requestId")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("Jupiter v2 order did not return requestId"))?
            .to_string();
        let signed_bytes = sign_swap_transaction(transaction, &wallet)?;
        let signed_base64 = general_purpose::STANDARD.encode(&signed_bytes);
        report.signed_transaction_bytes = signed_bytes.len();
        if let Some(reason) = oversized_transaction_reason(signed_bytes.len()) {
            report.blocked_reason = Some(reason);
            return Ok(report);
        }

        let simulation = self.maybe_simulate(&signed_base64).await?;
        report.simulation_ok = simulation.ok;
        report.simulation_error = simulation.error;
        report.simulation_logs = simulation.logs;
        if !report.simulation_ok && self.config.execution.require_simulation {
            report.blocked_reason = Some(
                "Jupiter v2 order simulation failed; transaction was not submitted".to_string(),
            );
            return Ok(report);
        }

        if !request.submit {
            report.blocked_reason =
                Some("submit=false; Jupiter v2 order was signed but not executed".to_string());
            return Ok(report);
        }

        let execute = self
            .execute_v2_order(&request_id, &signed_base64)
            .await
            .context("Jupiter v2 execute failed")?;
        let signature = execute
            .get("signature")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        report.submitted = signature.is_some();
        report.signature = signature.clone();
        report.solscan_url =
            signature.map(|signature| format!("https://solscan.io/tx/{signature}"));
        let status = execute
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("Unknown")
            .to_string();
        report.confirmation_status = Some(status.clone());
        if status != "Success" {
            report.confirmation_error = execute
                .get("error")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .or_else(|| {
                    execute
                        .get("code")
                        .map(|code| format!("Jupiter v2 execute status {status}, code {code}"))
                });
        }
        Ok(report)
    }

    pub async fn run_chain_smoke(
        &self,
        request: JupiterChainSmokeRequest,
    ) -> Result<JupiterChainSmokeReport> {
        if request.output_mints.is_empty() {
            return Err(anyhow!("chain-smoke requires at least one output mint"));
        }
        let estimated_input_usd = estimate_input_usd(
            &request.input_mint,
            request.amount_atoms,
            self.config.trading.sol_usd_estimate,
        )
        .ok_or_else(|| {
            anyhow!(
                "cannot estimate chain-smoke input value for {}; use WSOL, USDC, or USDT input",
                request.input_mint
            )
        })?;
        if estimated_input_usd > request.max_usd {
            return Err(anyhow!(
                "requested chain-smoke input is ${estimated_input_usd:.6}, above max ${:.6}",
                request.max_usd
            ));
        }

        let wallet = read_keypair_file(&self.config.wallet.path).map_err(|error| {
            anyhow!(
                "failed to read wallet keypair {}: {error}",
                self.config.wallet.path
            )
        })?;
        let wallet_pubkey = wallet.pubkey().to_string();
        let mut current_mint = request.input_mint.clone();
        let mut current_amount = request.amount_atoms;
        let mut quoted_legs = Vec::new();
        let chain_buffer_bps =
            atomic_chain_input_buffer_bps(live_arbitrage_slippage_bps(request.slippage_bps));

        for (index, output_mint) in request.output_mints.iter().enumerate() {
            let leg_request = LiveSwapRequest {
                input_mint: current_mint.clone(),
                output_mint: output_mint.clone(),
                amount_atoms: current_amount,
                slippage_bps: request.slippage_bps,
                submit: false,
                max_usd: request.max_usd,
                estimated_input_usd_override: None,
                required_route_label_contains: None,
                force_swap_instructions: true,
            };
            let quote = self.fetch_quote(&leg_request).await?;
            let out_amount = parse_quote_out_amount(&quote)?;
            let next_input_atoms = if index + 1 < request.output_mints.len() {
                let next = atomic_chain_sell_input_atoms(out_amount, chain_buffer_bps);
                if next == 0 {
                    return Err(anyhow!(
                        "chain-smoke leg {} output is too small after chain buffer",
                        index + 1
                    ));
                }
                Some(next)
            } else {
                None
            };
            quoted_legs.push(JupiterChainSmokeQuotedLeg {
                input_mint: current_mint.clone(),
                output_mint: output_mint.clone(),
                input_atoms: current_amount,
                quote_out_amount: out_amount,
                next_input_atoms,
                route_labels: route_labels(&quote),
                quote,
            });
            current_mint = output_mint.clone();
            if let Some(next) = next_input_atoms {
                current_amount = next;
            }
        }

        let quote_refs = quoted_legs
            .iter()
            .map(|quoted_leg| &quoted_leg.quote)
            .collect::<Vec<_>>();
        let instruction_sets = self
            .fetch_swap_instructions_many(&quote_refs, &wallet_pubkey, "chain-smoke leg")
            .await?;
        let legs = quoted_legs
            .iter()
            .zip(instruction_sets.iter())
            .enumerate()
            .map(
                |(index, (quoted_leg, instructions))| JupiterChainSmokeLegReport {
                    index: index + 1,
                    input_mint: quoted_leg.input_mint.clone(),
                    output_mint: quoted_leg.output_mint.clone(),
                    input_atoms: quoted_leg.input_atoms,
                    quote_out_amount: quoted_leg.quote_out_amount.to_string(),
                    next_input_atoms: quoted_leg.next_input_atoms,
                    route_labels: quoted_leg.route_labels.clone(),
                    setup_count: instructions.setup_instructions.len(),
                    cleanup: instructions.cleanup_instruction.is_some(),
                    lookup_tables: instructions.address_lookup_table_addresses.len(),
                },
            )
            .collect::<Vec<_>>();

        let instruction_refs = instruction_sets.iter().collect::<Vec<_>>();
        let lookup_table_addresses = merge_lookup_table_addresses_many(&instruction_refs);
        let instructions = compose_jupiter_chain_instructions(&self.config, &instruction_refs)?;
        let lookup_tables = self
            .fetch_address_lookup_tables(&lookup_table_addresses)
            .await?;
        let signed_bytes = self
            .sign_custom_transaction(&wallet, instructions, &lookup_tables)
            .await?;
        let signed_base64 = general_purpose::STANDARD.encode(&signed_bytes);
        let mut report = JupiterChainSmokeReport {
            submit_requested: false,
            submitted: false,
            wallet_pubkey,
            input_mint: request.input_mint,
            output_mints: request.output_mints,
            amount_atoms: request.amount_atoms,
            max_usd: request.max_usd,
            legs,
            address_lookup_tables: lookup_table_addresses,
            signed_transaction_bytes: signed_bytes.len(),
            simulation_ok: false,
            simulation_error: None,
            simulation_logs: Vec::new(),
            blocked_reason: None,
        };
        if let Some(reason) = oversized_transaction_reason(signed_bytes.len()) {
            report.blocked_reason = Some(reason);
            return Ok(report);
        }
        let simulation = self.simulate(&signed_base64).await?;
        report.simulation_ok = simulation
            .pointer("/value/err")
            .map(Value::is_null)
            .unwrap_or(false);
        report.simulation_error = simulation
            .pointer("/value/err")
            .filter(|value| !value.is_null())
            .map(Value::to_string);
        report.simulation_logs = simulation_logs(&simulation);
        report.blocked_reason = Some(
            "submit=false; chained Jupiter transaction was signed and simulated only".to_string(),
        );
        Ok(report)
    }

    pub async fn run_swap_instruction_chain(
        &self,
        request: JupiterSwapInstructionChainRequest,
    ) -> Result<LiveSwapReport> {
        if request.legs.is_empty() {
            return Err(anyhow!("swap-instruction chain requires at least one leg"));
        }
        validate_swap_instruction_chain(&request)?;
        let estimated_input_usd = request.estimated_input_usd_override.or_else(|| {
            estimate_input_usd(
                &request.input_mint,
                request.amount_atoms,
                self.config.trading.sol_usd_estimate,
            )
        });
        if let Some(estimated) = estimated_input_usd {
            if estimated > request.max_usd {
                return Err(anyhow!(
                    "requested chained input is ${estimated:.6}, above max ${:.6}",
                    request.max_usd
                ));
            }
        } else if request.submit {
            return Err(anyhow!(
                "cannot submit unknown-value chained input mint {}; provide an estimated input value",
                request.input_mint
            ));
        }

        let wallet = read_keypair_file(&self.config.wallet.path).map_err(|error| {
            anyhow!(
                "failed to read wallet keypair {}: {error}",
                self.config.wallet.path
            )
        })?;
        let wallet_pubkey = wallet.pubkey().to_string();
        let mut current_amount = request.amount_atoms;
        let chain_buffer_bps =
            atomic_chain_input_buffer_bps(live_arbitrage_slippage_bps(request.slippage_bps));
        let mut quoted_legs = Vec::with_capacity(request.legs.len());
        let mut final_out_amount = None;

        for (index, leg) in request.legs.iter().enumerate() {
            let leg_request = LiveSwapRequest {
                input_mint: leg.input_mint.clone(),
                output_mint: leg.output_mint.clone(),
                amount_atoms: current_amount,
                slippage_bps: request.slippage_bps,
                submit: false,
                max_usd: request.max_usd,
                estimated_input_usd_override: None,
                required_route_label_contains: None,
                force_swap_instructions: true,
            };
            let quote = self.fetch_quote(&leg_request).await?;
            let labels = route_labels(&quote);
            if let Some(required) = leg.required_route_label_contains.as_deref() {
                if !quote_route_matches_label_guard(&labels, required) {
                    return Err(anyhow!(
                        "chain leg {} route labels {:?} did not match required label {required}",
                        index + 1,
                        labels
                    ));
                }
            }
            let out_amount = parse_quote_out_amount(&quote)?;
            quoted_legs.push(JupiterSwapInstructionChainQuote { quote, labels });
            final_out_amount = Some(out_amount);
            if index + 1 < request.legs.len() {
                current_amount = atomic_chain_sell_input_atoms(out_amount, chain_buffer_bps);
                if current_amount == 0 {
                    return Err(anyhow!(
                        "chain leg {} output is too small after chain buffer",
                        index + 1
                    ));
                }
            }
        }
        if let (Some(final_out_amount), Some(min_output_atoms)) =
            (final_out_amount, request.min_output_atoms)
        {
            if final_out_amount < min_output_atoms {
                return Err(anyhow!(
                    "chained quote output {final_out_amount} atom(s) is below required minimum {min_output_atoms}"
                ));
            }
        }

        let quote_refs = quoted_legs
            .iter()
            .map(|quoted_leg| &quoted_leg.quote)
            .collect::<Vec<_>>();
        let instruction_sets = self
            .fetch_swap_instructions_many(&quote_refs, &wallet_pubkey, "chain leg")
            .await?;

        let instruction_refs = instruction_sets.iter().collect::<Vec<_>>();
        let lookup_table_addresses = merge_lookup_table_addresses_many(&instruction_refs);
        let instructions = compose_jupiter_chain_instructions(&self.config, &instruction_refs)?;
        let lookup_tables = self
            .fetch_address_lookup_tables(&lookup_table_addresses)
            .await?;
        let signed_bytes = self
            .sign_custom_transaction(&wallet, instructions, &lookup_tables)
            .await?;
        let signed_base64 = general_purpose::STANDARD.encode(&signed_bytes);
        let mut report = LiveSwapReport {
            submit_requested: request.submit,
            submitted: false,
            wallet_pubkey,
            input_mint: request.input_mint,
            output_mint: request.output_mint,
            amount_atoms: request.amount_atoms,
            estimated_input_usd,
            max_usd: request.max_usd,
            quote_out_amount: final_out_amount.map(|amount| amount.to_string()),
            quote_price_impact_pct: None,
            route_labels: quoted_legs
                .iter()
                .flat_map(|quoted_leg| quoted_leg.labels.iter().cloned())
                .collect(),
            last_valid_block_height: None,
            prioritization_fee_lamports: None,
            signed_transaction_bytes: signed_bytes.len(),
            simulation_ok: false,
            simulation_error: None,
            simulation_logs: Vec::new(),
            signature: None,
            confirmation_status: None,
            confirmation_error: None,
            solscan_url: None,
            blocked_reason: None,
        };
        if let Some(reason) = oversized_transaction_reason(signed_bytes.len()) {
            report.blocked_reason = Some(reason);
            return Ok(report);
        }

        let simulation = self.maybe_simulate(&signed_base64).await?;
        report.simulation_ok = simulation.ok;
        report.simulation_error = simulation.error;
        report.simulation_logs = simulation.logs;
        if !report.simulation_ok && self.config.execution.require_simulation {
            report.blocked_reason = Some(
                "chained swap-instruction simulation failed; transaction was not submitted"
                    .to_string(),
            );
            return Ok(report);
        }

        if !request.submit {
            report.blocked_reason = Some(
                "submit=false; chained swap-instruction transaction was signed and simulated only"
                    .to_string(),
            );
            return Ok(report);
        }

        let signature = self.send_transaction(&signed_base64).await?;
        report.submitted = true;
        report.solscan_url = Some(format!("https://solscan.io/tx/{signature}"));
        report.signature = Some(signature.clone());
        let confirmation = self.confirm(&signature).await?;
        report.confirmation_status = confirmation.status;
        report.confirmation_error = confirmation.error;
        Ok(report)
    }

    pub async fn run_external_primary_transaction(
        &self,
        request: ExternalPrimaryTransactionRequest,
    ) -> Result<LiveSwapReport> {
        if let Some(estimated) = request.estimated_input_usd {
            if estimated > request.max_usd {
                return Err(anyhow!(
                    "requested external primary input is ${estimated:.6}, above max ${:.6}",
                    request.max_usd
                ));
            }
        } else if request.submit {
            return Err(anyhow!(
                "cannot submit external primary leg without estimated input value"
            ));
        }

        let build_url = external_primary_build_url(&request.provider)
            .ok_or_else(|| anyhow!("{} primary build URL is not configured", request.provider))?;
        let wallet = read_keypair_file(&self.config.wallet.path).map_err(|error| {
            anyhow!(
                "failed to read wallet keypair {}: {error}",
                self.config.wallet.path
            )
        })?;
        let wallet_pubkey = wallet.pubkey().to_string();
        let payload = external_primary_build_payload(
            &request,
            &wallet_pubkey,
            self.config.jupiter.slippage_bps,
        );
        let mut builder = self.client.post(build_url).json(&payload);
        if let Some((header, value)) = external_primary_auth_header(&request.provider) {
            builder = builder.header(header, value);
        }
        let response = builder
            .send()
            .await
            .with_context(|| format!("{} primary transaction build failed", request.provider))?;
        let body = response_json(response, "external primary transaction build").await?;
        let transaction = external_transaction_base64(&body)
            .ok_or_else(|| anyhow!("external primary response did not include a transaction"))?;
        let signed_bytes = sign_swap_transaction(&transaction, &wallet)?;
        let signed_base64 = general_purpose::STANDARD.encode(&signed_bytes);
        let simulation = self.maybe_simulate(&signed_base64).await?;

        let mut report = LiveSwapReport {
            submit_requested: request.submit,
            submitted: false,
            wallet_pubkey,
            input_mint: request.input_mint,
            output_mint: request.output_mint,
            amount_atoms: request.amount_atoms,
            estimated_input_usd: request.estimated_input_usd,
            max_usd: request.max_usd,
            quote_out_amount: external_out_amount(&body),
            quote_price_impact_pct: external_price_impact(&body),
            route_labels: external_route_labels(&request.provider, &request.action, &body),
            last_valid_block_height: body
                .get("lastValidBlockHeight")
                .or_else(|| body.pointer("/data/lastValidBlockHeight"))
                .and_then(Value::as_u64),
            prioritization_fee_lamports: body
                .get("prioritizationFeeLamports")
                .or_else(|| body.pointer("/data/prioritizationFeeLamports"))
                .and_then(Value::as_u64),
            signed_transaction_bytes: signed_bytes.len(),
            simulation_ok: simulation.ok,
            simulation_error: simulation.error,
            simulation_logs: simulation.logs,
            signature: None,
            confirmation_status: None,
            confirmation_error: None,
            solscan_url: None,
            blocked_reason: None,
        };

        if !report.simulation_ok && self.config.execution.require_simulation {
            report.blocked_reason = Some(
                "external primary simulation failed; transaction was not submitted".to_string(),
            );
            return Ok(report);
        }

        if !request.submit {
            report.blocked_reason = Some(
                "submit=false; external primary transaction was signed and simulated only"
                    .to_string(),
            );
            return Ok(report);
        }

        let signature = self.send_transaction(&signed_base64).await?;
        report.submitted = true;
        report.solscan_url = Some(format!("https://solscan.io/tx/{signature}"));
        report.signature = Some(signature.clone());
        let confirmation = self.confirm(&signature).await?;
        report.confirmation_status = confirmation.status;
        report.confirmation_error = confirmation.error;
        Ok(report)
    }

    pub async fn run_api_route_transaction(
        &self,
        request: ApiRouteSwapRequest,
    ) -> Result<LiveSwapReport> {
        let input_mint = api_input_mint_for_side(&request.pair, request.side).to_string();
        let output_mint = api_output_mint_for_side(&request.pair, request.side).to_string();
        let input_decimals = api_input_decimals_for_side(&request.pair, request.side);
        let estimated_input_usd = request.estimated_input_usd_override.or_else(|| {
            quote_amount_usd(
                request.amount_atoms as u128,
                input_decimals,
                &input_mint,
                self.config.trading.sol_usd_estimate,
            )
        });
        if let Some(estimated) = estimated_input_usd {
            if estimated > request.max_usd {
                return Err(anyhow!(
                    "requested API route input is ${estimated:.6}, above max ${:.6}",
                    request.max_usd
                ));
            }
        } else if request.submit {
            return Err(anyhow!(
                "cannot submit API route leg without estimated input value"
            ));
        }

        let wallet = read_keypair_file(&self.config.wallet.path).map_err(|error| {
            anyhow!(
                "failed to read wallet keypair {}: {error}",
                self.config.wallet.path
            )
        })?;
        let wallet_pubkey = wallet.pubkey().to_string();
        let builder = ApiSwapBuilder::new(self.config.clone());
        let plan = builder
            .build_single_leg_plan(
                &wallet.pubkey(),
                &request.route,
                &request.pair,
                request.side,
                request.amount_atoms,
            )
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "route {} is not backed by an enabled API swap provider",
                    request.route
                )
            })?;

        let signed_bytes = self
            .sign_custom_transaction(&wallet, plan.instructions, &plan.lookup_tables)
            .await?;
        let signed_base64 = general_purpose::STANDARD.encode(&signed_bytes);
        let simulation = self.maybe_simulate(&signed_base64).await?;

        let mut report = LiveSwapReport {
            submit_requested: request.submit,
            submitted: false,
            wallet_pubkey,
            input_mint,
            output_mint,
            amount_atoms: request.amount_atoms,
            estimated_input_usd,
            max_usd: request.max_usd,
            quote_out_amount: Some(plan.out_amount_atoms.to_string()),
            quote_price_impact_pct: None,
            route_labels: plan.route_labels,
            last_valid_block_height: None,
            prioritization_fee_lamports: None,
            signed_transaction_bytes: signed_bytes.len(),
            simulation_ok: simulation.ok,
            simulation_error: simulation.error,
            simulation_logs: simulation.logs,
            signature: None,
            confirmation_status: None,
            confirmation_error: None,
            solscan_url: None,
            blocked_reason: None,
        };

        if !report.simulation_ok && self.config.execution.require_simulation {
            report.blocked_reason =
                Some("API route simulation failed; transaction was not submitted".to_string());
            return Ok(report);
        }

        if !request.submit {
            report.blocked_reason = Some(
                "submit=false; API route transaction was signed and simulated only".to_string(),
            );
            return Ok(report);
        }

        let signature = self.send_transaction(&signed_base64).await?;
        report.submitted = true;
        report.solscan_url = Some(format!("https://solscan.io/tx/{signature}"));
        report.signature = Some(signature.clone());
        let confirmation = self.confirm(&signature).await?;
        report.confirmation_status = confirmation.status;
        report.confirmation_error = confirmation.error;
        Ok(report)
    }

    pub async fn execute_atomic_arbitrage(
        &self,
        opportunity: &Opportunity,
        submit: bool,
    ) -> Result<AtomicArbReport> {
        let wallet = read_keypair_file(&self.config.wallet.path).map_err(|error| {
            anyhow!(
                "failed to read wallet keypair {}: {error}",
                self.config.wallet.path
            )
        })?;
        let wallet_pubkey = wallet.pubkey().to_string();
        let input_quote_atoms: u64 = opportunity
            .input_quote_atoms
            .try_into()
            .context("opportunity input amount does not fit into u64")?;
        let estimated_input_usd = quote_amount_usd(
            opportunity.input_quote_atoms,
            opportunity.pair.quote_decimals,
            &opportunity.pair.quote_mint,
            self.config.trading.sol_usd_estimate,
        )
        .ok_or_else(|| anyhow!("unsupported quote mint {}", opportunity.pair.quote_mint))?;

        let mut report = AtomicArbReport {
            status: AtomicArbStatus::Blocked,
            submitted: false,
            wallet_pubkey: wallet_pubkey.clone(),
            pair: opportunity.pair.clone(),
            buy_venue: opportunity.buy_venue,
            sell_venue: opportunity.sell_venue,
            input_quote_atoms: opportunity.input_quote_atoms,
            buy_quote_out_amount: None,
            sell_quote_out_amount: None,
            direct_signal_net_profit_usd: opportunity.net_profit_usd,
            requote_net_profit_usd: None,
            requote_net_profit_bps: None,
            buy_route_labels: Vec::new(),
            sell_route_labels: Vec::new(),
            address_lookup_tables: Vec::new(),
            signed_transaction_bytes: 0,
            simulation_ok: false,
            simulation_error: None,
            simulation_logs: Vec::new(),
            signature: None,
            confirmation_status: None,
            confirmation_error: None,
            solscan_url: None,
            blocked_reason: None,
            jupiter_reference_net_profit_bps: None,
            jupiter_reference_buy_quote_out_amount: None,
            jupiter_reference_sell_quote_out_amount: None,
            jupiter_reference_net_profit_usd: None,
            jupiter_reference_error: None,
            native_execution_cost_quote_atoms: None,
            native_setup_missing_atas: None,
            native_simulation_pre_quote_atoms: None,
            native_simulation_post_quote_atoms: None,
            native_simulation_quote_delta_atoms: None,
            native_simulation_net_profit_atoms: None,
            latency_trace: Vec::new(),
        };

        if self.config.execution.max_live_trade_usd > 0.0
            && estimated_input_usd > self.config.execution.max_live_trade_usd
        {
            report.blocked_reason = Some(format!(
                "opportunity input ${estimated_input_usd:.6} exceeds max live trade ${:.6}",
                self.config.execution.max_live_trade_usd
            ));
            return Ok(report);
        }

        if NativeSwapBuilder::is_native_candidate(opportunity) {
            return self
                .execute_native_atomic_arbitrage(opportunity, submit, wallet, report)
                .await;
        }

        if ApiSwapBuilder::is_api_candidate(opportunity) {
            return self
                .execute_api_atomic_arbitrage(opportunity, submit, wallet, report)
                .await;
        }

        let buy_quote = match self
            .fetch_arbitrage_quote(
                &opportunity.pair.quote_mint,
                &opportunity.pair.base_mint,
                input_quote_atoms,
                opportunity.buy_venue,
            )
            .await
        {
            Ok(quote) => quote,
            Err(error) => {
                report.blocked_reason = Some(format!("buy leg re-quote failed: {error:#}"));
                return Ok(report);
            }
        };
        let buy_out_atoms = parse_quote_out_amount(&buy_quote)?;
        report.buy_quote_out_amount = Some(buy_out_atoms.to_string());
        report.buy_route_labels = route_labels(&buy_quote);
        let sell_input_atoms = atomic_chain_sell_input_atoms(
            buy_out_atoms,
            atomic_chain_input_buffer_bps(live_arbitrage_slippage_bps(
                self.config.jupiter.slippage_bps,
            )),
        );
        if sell_input_atoms == 0 {
            report.blocked_reason =
                Some("buy leg quote output is too small after atomic chain buffer".to_string());
            return Ok(report);
        }

        let sell_quote = match self
            .fetch_arbitrage_quote(
                &opportunity.pair.base_mint,
                &opportunity.pair.quote_mint,
                sell_input_atoms,
                opportunity.sell_venue,
            )
            .await
        {
            Ok(quote) => quote,
            Err(error) => {
                report.blocked_reason = Some(format!("sell leg re-quote failed: {error:#}"));
                return Ok(report);
            }
        };
        let sell_out_atoms = parse_quote_out_amount(&sell_quote)?;
        report.sell_quote_out_amount = Some(sell_out_atoms.to_string());
        report.sell_route_labels = route_labels(&sell_quote);

        let requote_profit_atoms = sell_out_atoms as i128 - input_quote_atoms as i128;
        let requote_profit_usd = quote_amount_usd_signed(
            requote_profit_atoms,
            opportunity.pair.quote_decimals,
            &opportunity.pair.quote_mint,
            self.config.trading.sol_usd_estimate,
        )
        .unwrap_or(0.0)
            - estimated_priority_fee_usd(&self.config);
        let requote_profit_bps = if estimated_input_usd > 0.0 {
            requote_profit_usd / estimated_input_usd * 10_000.0
        } else {
            0.0
        };
        report.requote_net_profit_usd = Some(requote_profit_usd);
        report.requote_net_profit_bps = Some(requote_profit_bps);

        let min_requote_profit_bps = min_live_requote_profit_bps(&self.config);
        if requote_profit_bps < min_requote_profit_bps {
            report.blocked_reason = Some(format!(
                "live Jupiter re-quote net profit {requote_profit_bps:.2} bps is below required {min_requote_profit_bps:.2} bps"
            ));
            return Ok(report);
        }

        let estimated_tx_cost_usd = estimated_priority_fee_usd(&self.config);
        if requote_profit_usd > 0.0 && requote_profit_usd < estimated_tx_cost_usd * 2.0 {
            report.blocked_reason = Some(format!(
                "re-quote net profit ${requote_profit_usd:.4} is below 2x estimated tx cost ${estimated_tx_cost_usd:.4}; execution cost would consume >50% of profit"
            ));
            return Ok(report);
        }

        let (buy_instruction_result, sell_instruction_result) = tokio::join!(
            self.fetch_swap_instructions(&buy_quote, &wallet_pubkey),
            self.fetch_swap_instructions(&sell_quote, &wallet_pubkey)
        );
        let buy_instructions = match buy_instruction_result {
            Ok(instructions) => instructions,
            Err(error) => {
                report.blocked_reason =
                    Some(format!("buy leg instruction build failed: {error:#}"));
                return Ok(report);
            }
        };
        let sell_instructions = match sell_instruction_result {
            Ok(instructions) => instructions,
            Err(error) => {
                report.blocked_reason =
                    Some(format!("sell leg instruction build failed: {error:#}"));
                return Ok(report);
            }
        };
        let address_lookup_table_addresses =
            merge_lookup_table_addresses(&buy_instructions, &sell_instructions);
        report.address_lookup_tables = address_lookup_table_addresses.clone();

        let instructions =
            compose_atomic_instructions(&self.config, &buy_instructions, &sell_instructions)?;
        let lookup_tables = self
            .fetch_address_lookup_tables(&address_lookup_table_addresses)
            .await?;
        let signed_bytes = self
            .sign_custom_transaction(&wallet, instructions, &lookup_tables)
            .await?;
        let signed_base64 = general_purpose::STANDARD.encode(&signed_bytes);
        report.signed_transaction_bytes = signed_bytes.len();
        if let Some(reason) = oversized_transaction_reason(signed_bytes.len()) {
            report.status = AtomicArbStatus::Blocked;
            report.blocked_reason = Some(reason);
            return Ok(report);
        }

        let simulation = self.maybe_simulate(&signed_base64).await?;
        report.simulation_ok = simulation.ok;
        report.simulation_error = simulation.error;
        report.simulation_logs = simulation.logs;

        if !report.simulation_ok && self.config.execution.require_simulation {
            report.status = AtomicArbStatus::Failed;
            report.blocked_reason = Some(
                "atomic arbitrage simulation failed; transaction was not submitted".to_string(),
            );
            return Ok(report);
        }

        if !submit {
            report.status = AtomicArbStatus::Simulated;
            report.blocked_reason =
                Some("submit=false; atomic arbitrage was simulated only".to_string());
            return Ok(report);
        }

        let signature = self.send_transaction(&signed_base64).await?;
        report.submitted = true;
        report.status = AtomicArbStatus::Submitted;
        report.solscan_url = Some(format!("https://solscan.io/tx/{signature}"));
        report.signature = Some(signature.clone());

        let confirmation = self.confirm(&signature).await?;
        report.confirmation_status = confirmation.status;
        report.confirmation_error = confirmation.error;
        report.status = if report.confirmation_error.is_some() {
            AtomicArbStatus::Failed
        } else if matches!(
            report.confirmation_status.as_deref(),
            Some("confirmed") | Some("finalized")
        ) {
            AtomicArbStatus::Confirmed
        } else {
            AtomicArbStatus::Submitted
        };
        Ok(report)
    }

    async fn execute_native_atomic_arbitrage(
        &self,
        opportunity: &Opportunity,
        submit: bool,
        wallet: solana_sdk::signature::Keypair,
        mut report: AtomicArbReport,
    ) -> Result<AtomicArbReport> {
        if let Some(reason) = jito_tip_guard(&self.config) {
            report.status = AtomicArbStatus::Blocked;
            report.blocked_reason = Some(reason);
            return Ok(report);
        }
        let ttl_ms = self.config.execution.stale_candidate_ttl_ms;
        let builder = match &self.pool_subscription {
            Some(sub) => NativeSwapBuilder::new(self.config.clone())
                .with_pool_subscription(sub.clone())
                .with_strict_fresh_cache(ttl_ms),
            None => NativeSwapBuilder::new(self.config.clone()),
        };
        if let Err(error) = builder
            .warm_static_hot_accounts(&wallet.pubkey(), opportunity)
            .await
        {
            report.status = AtomicArbStatus::Blocked;
            report.blocked_reason = Some(format!(
                "native static hot account warmup failed: {error:#}"
            ));
            return Ok(report);
        }
        if let Err(error) = self.refresh_native_market_accounts().await {
            report.status = AtomicArbStatus::Blocked;
            report.blocked_reason = Some(format!(
                "native market account pre-refresh failed: {error:#}"
            ));
            return Ok(report);
        }
        let latency_start = Instant::now();
        push_optimizer_build_latency_mark(
            &mut report.latency_trace,
            opportunity.optimizer_decision.is_some(),
            latency_start,
        );
        let plan = match builder
            .build_exploratory_simulation_plan(&wallet.pubkey(), opportunity)
            .await
        {
            Ok(Some(plan)) => plan,
            Ok(None) => {
                report.blocked_reason =
                    Some("native direct executor does not support this opportunity".to_string());
                return Ok(report);
            }
            Err(error) => {
                let first_error = format!("{error:#}");
                if !is_native_strict_cache_stale_error(&first_error) {
                    report.blocked_reason = Some(format!(
                        "native direct instruction build failed: {first_error}"
                    ));
                    return Ok(report);
                }
                push_latency_mark(
                    &mut report.latency_trace,
                    "native_strict_cache_stale_retry_refresh_started",
                    latency_start,
                );
                if let Err(refresh_error) = self.refresh_native_market_accounts().await {
                    report.status = AtomicArbStatus::Blocked;
                    report.blocked_reason = Some(format!(
                        "native direct instruction build strict-cache retry refresh failed after stale error `{first_error}`: {refresh_error:#}"
                    ));
                    return Ok(report);
                }
                push_latency_mark(
                    &mut report.latency_trace,
                    "native_strict_cache_stale_retry_refreshed",
                    latency_start,
                );
                match builder
                    .build_exploratory_simulation_plan(&wallet.pubkey(), opportunity)
                    .await
                {
                    Ok(Some(plan)) => {
                        push_latency_mark(
                            &mut report.latency_trace,
                            "native_strict_cache_stale_retry_succeeded",
                            latency_start,
                        );
                        plan
                    }
                    Ok(None) => {
                        report.blocked_reason = Some(format!(
                            "native direct executor does not support this opportunity after strict-cache stale retry; first error: {first_error}"
                        ));
                        return Ok(report);
                    }
                    Err(retry_error) => {
                        report.status = AtomicArbStatus::Blocked;
                        report.blocked_reason = Some(format!(
                            "native direct instruction build failed after one strict-cache stale retry; first error: {first_error}; retry error: {retry_error:#}"
                        ));
                        return Ok(report);
                    }
                }
            }
        };
        report
            .latency_trace
            .extend(plan.latency_trace.iter().map(|mark| ExecutionLatencyMark {
                stage: mark.stage.clone(),
                elapsed_ms: mark.elapsed_ms,
            }));
        push_latency_mark(
            &mut report.latency_trace,
            "native_plan_built",
            latency_start,
        );
        if let Some(reason) =
            stale_candidate_blocker("native_plan_built", latency_start.elapsed(), ttl_ms)
        {
            report.status = AtomicArbStatus::Blocked;
            report.blocked_reason = Some(reason);
            return Ok(report);
        }

        report.buy_quote_out_amount = Some(plan.buy_quote_out_atoms.to_string());
        report.sell_quote_out_amount = Some(plan.sell_quote_out_atoms.to_string());
        report.buy_route_labels = plan.dexes.first().cloned().into_iter().collect();
        report.sell_route_labels = plan.dexes.get(1).cloned().into_iter().collect();
        report.requote_net_profit_usd = Some(plan.requote_net_profit_usd);
        report.requote_net_profit_bps = Some(plan.requote_net_profit_bps);
        report.blocked_reason = plan.blocked_reason.clone();
        report.native_execution_cost_quote_atoms =
            Some(plan.execution_cost_quote_atoms.to_string());

        if let Some(reason) = plan.blocked_reason {
            report.status = AtomicArbStatus::Blocked;
            report.blocked_reason = Some(reason);
            return Ok(report);
        }

        let min_requote_profit_bps = min_live_requote_profit_bps(&self.config);
        if plan.requote_net_profit_bps < min_requote_profit_bps {
            report.status = AtomicArbStatus::Blocked;
            report.blocked_reason = Some(format!(
                "native direct re-quote net profit {:.2} bps is below required {:.2} bps",
                plan.requote_net_profit_bps, min_requote_profit_bps
            ));
            return Ok(report);
        }

        let lookup_table_addresses = self.config.execution.address_lookup_tables.clone();
        report.address_lookup_tables = lookup_table_addresses.clone();
        let lookup_tables = match self
            .fetch_address_lookup_tables(&lookup_table_addresses)
            .await
        {
            Ok(lookup_tables) => lookup_tables,
            Err(error) => {
                report.status = AtomicArbStatus::Blocked;
                report.blocked_reason = Some(format!(
                    "native direct address lookup table fetch failed: {error:#}"
                ));
                return Ok(report);
            }
        };
        push_latency_mark(
            &mut report.latency_trace,
            "native_lookup_tables_loaded",
            latency_start,
        );
        if let Some(reason) = stale_candidate_blocker(
            "native_lookup_tables_loaded",
            latency_start.elapsed(),
            ttl_ms,
        ) {
            report.status = AtomicArbStatus::Blocked;
            report.blocked_reason = Some(reason);
            return Ok(report);
        }

        let signed_bytes = self
            .sign_custom_transaction(&wallet, plan.instructions.clone(), &lookup_tables)
            .await?;
        let mut signed_base64 = general_purpose::STANDARD.encode(&signed_bytes);
        report.signed_transaction_bytes = signed_bytes.len();
        push_latency_mark(&mut report.latency_trace, "native_signed", latency_start);
        if let Some(reason) =
            stale_candidate_blocker("native_signed", latency_start.elapsed(), ttl_ms)
        {
            report.status = AtomicArbStatus::Blocked;
            report.blocked_reason = Some(reason);
            return Ok(report);
        }
        if let Some(reason) = native_oversized_transaction_reason_with_lut_context(
            signed_bytes.len(),
            max_live_transaction_bytes(),
            lookup_table_addresses.len(),
            lookup_tables.len(),
        ) {
            if plan.jito_tip_included {
                let instructions_no_tip: Vec<Instruction> = plan
                    .instructions
                    .iter()
                    .filter(|ix| !is_jito_tip_instruction(ix))
                    .cloned()
                    .collect();
                let retry_bytes = self
                    .sign_custom_transaction(&wallet, instructions_no_tip, &lookup_tables)
                    .await?;
                if native_oversized_transaction_reason_with_lut_context(
                    retry_bytes.len(),
                    max_live_transaction_bytes(),
                    lookup_table_addresses.len(),
                    lookup_tables.len(),
                )
                .is_none()
                {
                    signed_base64 = general_purpose::STANDARD.encode(&retry_bytes);
                    report.signed_transaction_bytes = retry_bytes.len();
                    info!(
                        "tx oversized with tip ({} bytes), submitting without tip ({} bytes)",
                        signed_bytes.len(),
                        retry_bytes.len()
                    );
                } else {
                    report.status = AtomicArbStatus::Blocked;
                    report.blocked_reason = Some(reason);
                    return Ok(report);
                }
            } else {
                report.status = AtomicArbStatus::Blocked;
                report.blocked_reason = Some(reason);
                return Ok(report);
            }
        }

        let simulation = self.maybe_simulate(&signed_base64).await?;
        let simulation_blocker = Self::simulation_blocked_reason(
            "native direct atomic arbitrage simulation failed; transaction was not submitted",
            &simulation,
        );
        report.simulation_ok = simulation.ok;
        report.simulation_error = simulation.error;
        report.simulation_logs = simulation.logs;
        push_latency_mark(&mut report.latency_trace, "native_simulated", latency_start);

        if !report.simulation_ok && self.config.execution.require_simulation {
            report.status = AtomicArbStatus::Failed;
            report.blocked_reason = Some(simulation_blocker);
            return Ok(report);
        }

        if let Some(reason) = native_submit_blocker_after_fresh_quote(
            submit,
            plan.fresh_requote_complete,
            plan.requote_net_profit_usd,
            self.config.execution.min_absolute_profit_usd,
            self.native_session_submit_attempts.load(Ordering::Relaxed),
            self.config.execution.max_live_trades_per_session,
            self.native_cycle_submit_attempts.load(Ordering::Relaxed),
            self.config.execution.max_live_trades_per_cycle as u64,
        ) {
            report.status = AtomicArbStatus::Blocked;
            report.blocked_reason = Some(reason);
            return Ok(report);
        }

        if !submit {
            report.status = AtomicArbStatus::Simulated;
            report.blocked_reason =
                Some("submit=false; native direct atomic arbitrage was simulated only".to_string());
            return Ok(report);
        }

        if let Some(reason) =
            stale_candidate_blocker("before_submit", latency_start.elapsed(), ttl_ms)
        {
            report.status = AtomicArbStatus::Blocked;
            report.blocked_reason = Some(reason);
            return Ok(report);
        }

        self.native_session_submit_attempts
            .fetch_add(1, Ordering::Relaxed);
        self.native_cycle_submit_attempts
            .fetch_add(1, Ordering::Relaxed);
        let signature = match self.send_transaction(&signed_base64).await {
            Ok(signature) => signature,
            Err(error) => {
                push_latency_mark(
                    &mut report.latency_trace,
                    "native_submit_failed",
                    latency_start,
                );
                report.status = AtomicArbStatus::Failed;
                report.blocked_reason = Some(format!(
                    "native direct submit failed after bounded attempt: {error:#}"
                ));
                return Ok(report);
            }
        };
        push_latency_mark(&mut report.latency_trace, "native_submitted", latency_start);
        report.submitted = true;
        report.status = AtomicArbStatus::Submitted;
        report.solscan_url = Some(format!("https://solscan.io/tx/{signature}"));
        report.signature = Some(signature.clone());

        let confirmation = match self.confirm(&signature).await {
            Ok(confirmation) => confirmation,
            Err(error) => {
                push_latency_mark(
                    &mut report.latency_trace,
                    "native_confirm_failed",
                    latency_start,
                );
                report.status = AtomicArbStatus::Failed;
                report.confirmation_error = Some(format!("{error:#}"));
                report.blocked_reason = Some(format!(
                    "native direct confirmation failed after submit: {error:#}"
                ));
                return Ok(report);
            }
        };
        push_latency_mark(&mut report.latency_trace, "native_confirmed", latency_start);
        report.confirmation_status = confirmation.status;
        report.confirmation_error = confirmation.error;
        report.status = if report.confirmation_error.is_some() {
            AtomicArbStatus::Failed
        } else if matches!(
            report.confirmation_status.as_deref(),
            Some("confirmed") | Some("finalized")
        ) {
            AtomicArbStatus::Confirmed
        } else {
            AtomicArbStatus::Submitted
        };
        Ok(report)
    }

    async fn refresh_native_market_accounts(&self) -> Result<()> {
        let Some(pool_subscription) = &self.pool_subscription else {
            return Ok(());
        };
        if self.native_market_refresh_pubkeys.is_empty() {
            return Ok(());
        }
        let accounts = self
            .rpc
            .get_multiple_accounts(&self.native_market_refresh_pubkeys)
            .await?;
        let warmed = accounts.into_iter().flatten().collect::<Vec<_>>();
        if !warmed.is_empty() {
            pool_subscription.warm_raw_accounts(warmed).await;
        }
        Ok(())
    }

    async fn execute_api_atomic_arbitrage(
        &self,
        opportunity: &Opportunity,
        submit: bool,
        wallet: solana_sdk::signature::Keypair,
        mut report: AtomicArbReport,
    ) -> Result<AtomicArbReport> {
        let builder = ApiSwapBuilder::new(self.config.clone());
        let plan = match builder
            .build_atomic_plan(&wallet.pubkey(), opportunity)
            .await
        {
            Ok(Some(plan)) => plan,
            Ok(None) => {
                report.blocked_reason =
                    Some("API/direct executor does not support this opportunity".to_string());
                return Ok(report);
            }
            Err(error) => {
                report.blocked_reason =
                    Some(format!("API/direct instruction build failed: {error:#}"));
                return Ok(report);
            }
        };

        report.buy_quote_out_amount = Some(plan.buy_out_atoms.to_string());
        report.sell_quote_out_amount = Some(plan.sell_out_atoms.to_string());
        report.buy_route_labels = plan.buy_route_labels;
        report.sell_route_labels = plan.sell_route_labels;
        report.address_lookup_tables = plan.lookup_table_addresses.clone();
        report.requote_net_profit_usd = Some(plan.requote_net_profit_usd);
        report.requote_net_profit_bps = Some(plan.requote_net_profit_bps);
        report.blocked_reason = None;
        let min_requote_profit_bps = min_live_requote_profit_bps(&self.config);
        if plan.requote_net_profit_bps < min_requote_profit_bps {
            report.status = AtomicArbStatus::Blocked;
            report.blocked_reason = Some(format!(
                "API/direct re-quote net profit {:.2} bps is below required {:.2} bps",
                plan.requote_net_profit_bps, min_requote_profit_bps
            ));
            return Ok(report);
        }

        let signed_bytes = self
            .sign_custom_transaction(&wallet, plan.instructions, &plan.lookup_tables)
            .await?;
        let signed_base64 = general_purpose::STANDARD.encode(&signed_bytes);
        report.signed_transaction_bytes = signed_bytes.len();
        if let Some(reason) = oversized_transaction_reason(signed_bytes.len()) {
            report.status = AtomicArbStatus::Blocked;
            report.blocked_reason = Some(reason);
            return Ok(report);
        }

        let simulation = self.maybe_simulate(&signed_base64).await?;
        report.simulation_ok = simulation.ok;
        report.simulation_error = simulation.error;
        report.simulation_logs = simulation.logs;

        if !report.simulation_ok && self.config.execution.require_simulation {
            report.status = AtomicArbStatus::Failed;
            report.blocked_reason = Some(
                "API/direct atomic arbitrage simulation failed; transaction was not submitted"
                    .to_string(),
            );
            return Ok(report);
        }

        if !submit {
            report.status = AtomicArbStatus::Simulated;
            report.blocked_reason =
                Some("submit=false; API/direct atomic arbitrage was simulated only".to_string());
            return Ok(report);
        }

        let signature = self.send_transaction(&signed_base64).await?;
        report.submitted = true;
        report.status = AtomicArbStatus::Submitted;
        report.solscan_url = Some(format!("https://solscan.io/tx/{signature}"));
        report.signature = Some(signature.clone());

        let confirmation = self.confirm(&signature).await?;
        report.confirmation_status = confirmation.status;
        report.confirmation_error = confirmation.error;
        report.status = if report.confirmation_error.is_some() {
            AtomicArbStatus::Failed
        } else if matches!(
            report.confirmation_status.as_deref(),
            Some("confirmed") | Some("finalized")
        ) {
            AtomicArbStatus::Confirmed
        } else {
            AtomicArbStatus::Submitted
        };
        Ok(report)
    }

    async fn fetch_quote(&self, request: &LiveSwapRequest) -> Result<Value> {
        let request = self.client.get(&self.config.jupiter.quote_url).query(&[
            ("inputMint", request.input_mint.as_str()),
            ("outputMint", request.output_mint.as_str()),
            ("amount", &request.amount_atoms.to_string()),
            ("slippageBps", &request.slippage_bps.to_string()),
            ("swapMode", "ExactIn"),
        ]);
        let response = self
            .with_jupiter_api_key(request)
            .send()
            .await
            .context("jupiter quote request failed")?;
        response_json(response, "jupiter quote").await
    }

    async fn fetch_v2_order(&self, request: &LiveSwapRequest, taker: &str) -> Result<Value> {
        let amount = request.amount_atoms.to_string();
        let mut params = vec![
            ("inputMint", request.input_mint.as_str()),
            ("outputMint", request.output_mint.as_str()),
            ("amount", amount.as_str()),
            ("taker", taker),
            ("swapMode", "ExactIn"),
        ];
        let slippage_bps = request.slippage_bps.to_string();
        if !jupiter_v2_auto_slippage() {
            params.push(("slippageBps", slippage_bps.as_str()));
        }
        let request = self.client.get(jupiter_v2_order_url()).query(&params);
        let response = self
            .with_jupiter_api_key(request)
            .send()
            .await
            .context("Jupiter v2 order request failed")?;
        response_json(response, "Jupiter v2 order").await
    }

    async fn execute_v2_order(&self, request_id: &str, signed_transaction: &str) -> Result<Value> {
        let request = self.client.post(jupiter_v2_execute_url()).json(&json!({
            "requestId": request_id,
            "signedTransaction": signed_transaction,
        }));
        let response = self
            .with_jupiter_api_key(request)
            .send()
            .await
            .context("Jupiter v2 execute request failed")?;
        response_json(response, "Jupiter v2 execute").await
    }

    async fn fetch_arbitrage_quote(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_atoms: u64,
        venue: Venue,
    ) -> Result<Value> {
        let mut params = vec![
            ("inputMint".to_string(), input_mint.to_string()),
            ("outputMint".to_string(), output_mint.to_string()),
            ("amount".to_string(), amount_atoms.to_string()),
            (
                "slippageBps".to_string(),
                live_arbitrage_slippage_bps(self.config.jupiter.slippage_bps).to_string(),
            ),
            ("swapMode".to_string(), "ExactIn".to_string()),
            (
                "onlyDirectRoutes".to_string(),
                self.config.execution.jupiter_only_direct_routes.to_string(),
            ),
            (
                "maxAccounts".to_string(),
                self.config.execution.jupiter_max_accounts.to_string(),
            ),
        ];
        if self.config.execution.jupiter_use_venue_filters {
            if let Some(dexes) = jupiter_dex_filter(venue) {
                params.push(("dexes".to_string(), dexes.to_string()));
            }
        }

        let request = self
            .client
            .get(&self.config.jupiter.quote_url)
            .query(&params);
        let response = self
            .with_jupiter_api_key(request)
            .send()
            .await
            .with_context(|| format!("jupiter arbitrage quote failed for {venue}"))?;
        response_json(response, "jupiter arbitrage quote").await
    }

    async fn fetch_swap(&self, quote: &Value, wallet_pubkey: &str) -> Result<JupiterSwapResponse> {
        let mut request = self
            .client
            .post(&self.config.execution.jupiter_swap_url)
            .json(&json!({
                "quoteResponse": quote,
                "userPublicKey": wallet_pubkey,
                "wrapAndUnwrapSol": true,
                "dynamicComputeUnitLimit": true,
                "prioritizationFeeLamports": {
                    "priorityLevelWithMaxLamports": {
                        "maxLamports": self.config.fees.max_priority_fee_lamports,
                        "global": false,
                        "priorityLevel": priority_level(self.config.fees.priority_fee_percentile)
                    }
                }
            }));
        request = self.with_jupiter_api_key(request);

        let response = request
            .send()
            .await
            .context("jupiter swap request failed")?;
        let value = response_json(response, "jupiter swap").await?;
        serde_json::from_value(value).context("invalid jupiter swap response")
    }

    async fn fetch_swap_instructions(
        &self,
        quote: &Value,
        wallet_pubkey: &str,
    ) -> Result<JupiterSwapInstructionsResponse> {
        let mut request = self
            .client
            .post(&self.config.execution.jupiter_swap_instructions_url)
            .json(&json!({
                "quoteResponse": quote,
                "userPublicKey": wallet_pubkey,
                "payer": wallet_pubkey,
                "wrapAndUnwrapSol": true,
                "useSharedAccounts": true,
                "asLegacyTransaction": false,
                "dynamicComputeUnitLimit": false,
                "skipUserAccountsRpcCalls": false
            }));
        request = self.with_jupiter_api_key(request);

        let response = request
            .send()
            .await
            .context("jupiter swap-instructions request failed")?;
        let value = response_json(response, "jupiter swap-instructions").await?;
        if let Some(error) = value.get("error").and_then(Value::as_str) {
            return Err(anyhow!("jupiter swap-instructions error: {error}"));
        }
        serde_json::from_value(value).context("invalid jupiter swap-instructions response")
    }

    async fn fetch_swap_instructions_many(
        &self,
        quotes: &[&Value],
        wallet_pubkey: &str,
        leg_name: &str,
    ) -> Result<Vec<JupiterSwapInstructionsResponse>> {
        let mut instruction_futures = FuturesUnordered::new();
        for (index, quote) in quotes.iter().copied().enumerate() {
            instruction_futures.push(async move {
                self.fetch_swap_instructions(quote, wallet_pubkey)
                    .await
                    .map(|instructions| (index, instructions))
            });
        }
        let mut instruction_sets = (0..quotes.len()).map(|_| None).collect::<Vec<_>>();
        while let Some(result) = instruction_futures.next().await {
            let (index, instructions) = result?;
            instruction_sets[index] = Some(instructions);
        }
        instruction_sets
            .into_iter()
            .enumerate()
            .map(|(index, instructions)| {
                instructions.ok_or_else(|| {
                    anyhow!(
                        "missing Jupiter swap instructions for {leg_name} {}",
                        index + 1
                    )
                })
            })
            .collect()
    }

    async fn fetch_address_lookup_tables(
        &self,
        addresses: &[String],
    ) -> Result<Vec<AddressLookupTableAccount>> {
        if addresses.is_empty() {
            return Ok(Vec::new());
        }
        let accounts = self
            .rpc
            .get_multiple_accounts(addresses)
            .await
            .context("failed to fetch address lookup table accounts")?;
        let mut lookup_tables = Vec::new();
        for (address, account) in addresses.iter().zip(accounts) {
            let Some(account) = account else {
                return Err(anyhow!("address lookup table account missing: {address}"));
            };
            let key = Pubkey::from_str(address)
                .with_context(|| format!("invalid address lookup table key {address}"))?;
            let table = AddressLookupTable::deserialize(&account.data)
                .map_err(|error| anyhow!("invalid address lookup table {address}: {error:?}"))?;
            lookup_tables.push(AddressLookupTableAccount {
                key,
                addresses: table.addresses.to_vec(),
            });
        }
        Ok(lookup_tables)
    }

    async fn sign_custom_transaction(
        &self,
        wallet: &solana_sdk::signature::Keypair,
        instructions: Vec<Instruction>,
        lookup_tables: &[AddressLookupTableAccount],
    ) -> Result<Vec<u8>> {
        let blockhash = Hash::from_str(&self.rpc.get_latest_blockhash().await?)
            .context("invalid latest blockhash")?;
        sign_custom_transaction_with_blockhash(wallet, instructions, lookup_tables, blockhash)
    }

    fn with_jupiter_api_key(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(api_key) = jupiter_api_key() {
            request.header("x-api-key", api_key)
        } else {
            request
        }
    }

    async fn simulate(&self, signed_base64: &str) -> Result<Value> {
        self.rpc
            .call(
                "simulateTransaction",
                json!([
                    signed_base64,
                    {
                        "encoding": "base64",
                        "sigVerify": true,
                        "replaceRecentBlockhash": false,
                        "commitment": "processed"
                    }
                ]),
            )
            .await
            .context("simulateTransaction failed")
    }

    async fn maybe_simulate(&self, signed_base64: &str) -> Result<SimulationOutcome> {
        if !self.config.execution.require_simulation {
            return Ok(SimulationOutcome {
                ok: true,
                error: None,
                logs: Vec::new(),
            });
        }

        let simulation = self.simulate(signed_base64).await?;
        Ok(SimulationOutcome {
            ok: simulation
                .pointer("/value/err")
                .map(Value::is_null)
                .unwrap_or(false),
            error: simulation
                .pointer("/value/err")
                .filter(|value| !value.is_null())
                .map(Value::to_string),
            logs: simulation_logs(&simulation),
        })
    }

    fn simulation_blocked_reason(prefix: &str, simulation: &SimulationOutcome) -> String {
        let base = simulation
            .error
            .as_ref()
            .filter(|error| !error.trim().is_empty())
            .map(|error| format!("{prefix}: {error}"))
            .unwrap_or_else(|| prefix.to_string());
        let logs = simulation_log_tail(&simulation.logs, 6);
        if logs.is_empty() {
            base
        } else {
            format!("{base} logs: {logs}")
        }
    }

    async fn send_transaction(&self, signed_base64: &str) -> Result<String> {
        static LAST_SUBMIT: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let last = LAST_SUBMIT.load(std::sync::atomic::Ordering::Relaxed);
        let wait_ms = 100i64.saturating_sub(now - last);
        if wait_ms > 0 {
            tokio::time::sleep(Duration::from_millis(wait_ms as u64)).await;
        }
        LAST_SUBMIT.store(now, std::sync::atomic::Ordering::Relaxed);
        self.send_transaction_with_retry(signed_base64).await
    }

    async fn send_transaction_with_retry(&self, signed_base64: &str) -> Result<String> {
        let mut last_error = String::new();
        if self.use_jito {
            match self.send_jito_bundle(signed_base64).await {
                Ok(bundle_id) => {
                    info!("jito bundle accepted: {bundle_id}");
                    if let Some(sig) = extract_signature_from_legacy_tx(signed_base64) {
                        return Ok(sig);
                    }
                }
                Err(e) => {
                    if !e.to_string().contains("429")
                        && !e.to_string().contains("could not be decoded")
                    {
                        info!("jito bundle failed: {e:#}");
                    }
                }
            }
        }
        for attempt in 0..2 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            match self.send_transaction_multi_rpc(signed_base64).await {
                Ok(sig) => return Ok(sig),
                Err(e) => last_error = format!("{e:#}"),
            }
        }
        Err(anyhow!("send transaction failed: {last_error}"))
    }

    async fn send_jito_bundle(&self, signed_base64: &str) -> Result<String> {
        let jito_url = &self.config.execution.jito_block_engine_url;
        send_jito_bundle(signed_base64, jito_url, &self.client).await
    }

    async fn send_transaction_multi_rpc(&self, signed_base64: &str) -> Result<String> {
        let mut futures = FuturesUnordered::new();
        for url in &self.rpc_urls {
            let url = url.clone();
            let signed = signed_base64.to_string();
            futures.push(tokio::spawn(
                async move { send_to_rpc_url(&url, &signed).await },
            ));
        }
        let mut last_error = String::new();
        while let Some(result) = futures.next().await {
            match result {
                Ok(Ok(sig)) => return Ok(sig),
                Ok(Err(e)) => last_error = format!("{e:#}"),
                Err(e) => last_error = format!("{e:#}"),
            }
        }
        Err(anyhow!("multi-rpc send failed: {last_error}"))
    }

    async fn confirm(&self, signature: &str) -> Result<ConfirmationReport> {
        let timeout = Duration::from_millis(self.config.execution.confirmation_timeout_ms);
        let started = Instant::now();
        while started.elapsed() < timeout {
            let result = self
                .rpc
                .call(
                    "getSignatureStatuses",
                    json!([[signature], {"searchTransactionHistory": false}]),
                )
                .await?;
            if let Some(status) = result
                .get("value")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
            {
                if !status.is_null() {
                    let err = status
                        .get("err")
                        .filter(|value| !value.is_null())
                        .map(Value::to_string);
                    let confirmation_status = status
                        .get("confirmationStatus")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned);
                    if err.is_some()
                        || matches!(
                            confirmation_status.as_deref(),
                            Some("confirmed") | Some("finalized")
                        )
                    {
                        return Ok(ConfirmationReport {
                            status: confirmation_status,
                            error: err,
                        });
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(1_000)).await;
        }

        Ok(ConfirmationReport {
            status: Some("timeout".to_string()),
            error: None,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JupiterSwapResponse {
    swap_transaction: String,
    last_valid_block_height: Option<u64>,
    prioritization_fee_lamports: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JupiterSwapInstructionsResponse {
    #[serde(default)]
    other_instructions: Vec<JupiterInstruction>,
    #[serde(default, rename = "computeBudgetInstructions")]
    _compute_budget_instructions: Vec<JupiterInstruction>,
    #[serde(default)]
    setup_instructions: Vec<JupiterInstruction>,
    swap_instruction: JupiterInstruction,
    cleanup_instruction: Option<JupiterInstruction>,
    token_ledger_instruction: Option<JupiterInstruction>,
    #[serde(default)]
    address_lookup_table_addresses: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JupiterInstruction {
    program_id: String,
    #[serde(default)]
    accounts: Vec<JupiterAccountMeta>,
    data: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JupiterAccountMeta {
    pubkey: String,
    is_signer: bool,
    is_writable: bool,
}

struct ConfirmationReport {
    status: Option<String>,
    error: Option<String>,
}

async fn response_json(response: reqwest::Response, label: &str) -> Result<Value> {
    let status = response.status();
    let text = response
        .text()
        .await
        .with_context(|| format!("{label} response body failed"))?;
    if !status.is_success() {
        return Err(anyhow!("{label} http {status}: {text}"));
    }
    serde_json::from_str(&text).with_context(|| format!("invalid {label} json: {text}"))
}

fn jupiter_v2_execution_enabled() -> bool {
    env_bool("JUPITER_V2_EXECUTION_ENABLED").unwrap_or(true)
}

fn jupiter_v2_auto_slippage() -> bool {
    env_bool("JUPITER_V2_AUTO_SLIPPAGE").unwrap_or(true)
}

fn jupiter_v2_order_url() -> String {
    env_value("JUPITER_V2_ORDER_URL")
        .unwrap_or_else(|| "https://api.jup.ag/swap/v2/order".to_string())
}

fn jupiter_v2_execute_url() -> String {
    env_value("JUPITER_V2_EXECUTE_URL")
        .unwrap_or_else(|| "https://api.jup.ag/swap/v2/execute".to_string())
}

fn jupiter_api_key() -> Option<String> {
    env_value("JUPITER_API_KEY")
}

fn external_primary_build_url(provider: &str) -> Option<String> {
    let env_name = match provider {
        "ondo_jupiter_rfq" | "ondo_primary" => "ONDO_PRIMARY_BUILD_URL",
        _ => return None,
    };
    env_value(env_name)
}

fn external_primary_auth_header(provider: &str) -> Option<(String, String)> {
    let (key_env, header_env, prefix_env, default_header, default_prefix) = match provider {
        "ondo_jupiter_rfq" | "ondo_primary" => (
            "ONDO_API_KEY",
            "ONDO_PRIMARY_API_KEY_HEADER",
            "ONDO_PRIMARY_API_KEY_PREFIX",
            "Authorization",
            "Bearer ",
        ),
        _ => return None,
    };
    let key = env_value(key_env)?;
    let header = env_value(header_env).unwrap_or_else(|| default_header.to_string());
    let prefix = env_value(prefix_env).unwrap_or_else(|| default_prefix.to_string());
    Some((header, format!("{prefix}{key}")))
}

fn external_transaction_base64(value: &Value) -> Option<String> {
    const PATHS: &[&str] = &[
        "/transaction",
        "/swapTransaction",
        "/unsignedTransaction",
        "/data/transaction",
        "/data/swapTransaction",
        "/data/unsignedTransaction",
        "/result/transaction",
        "/result/swapTransaction",
    ];
    PATHS
        .iter()
        .find_map(|path| value.pointer(path)?.as_str())
        .filter(|transaction| !transaction.trim().is_empty())
        .map(ToOwned::to_owned)
}

fn external_out_amount(value: &Value) -> Option<String> {
    const PATHS: &[&str] = &[
        "/outAmount",
        "/outputAmount",
        "/expectedOutAmount",
        "/data/outAmount",
        "/data/outputAmount",
        "/data/expectedOutAmount",
        "/result/outAmount",
        "/result/outputAmount",
    ];
    PATHS.iter().find_map(|path| {
        let value = value.pointer(path)?;
        value
            .as_str()
            .map(ToOwned::to_owned)
            .or_else(|| value.as_u64().map(|amount| amount.to_string()))
    })
}

fn external_price_impact(value: &Value) -> Option<String> {
    const PATHS: &[&str] = &[
        "/priceImpactPct",
        "/priceImpact",
        "/data/priceImpactPct",
        "/data/priceImpact",
        "/result/priceImpactPct",
        "/result/priceImpact",
    ];
    PATHS.iter().find_map(|path| {
        let value = value.pointer(path)?;
        value
            .as_str()
            .map(ToOwned::to_owned)
            .or_else(|| value.as_f64().map(|impact| impact.to_string()))
    })
}

fn external_route_labels(provider: &str, action: &str, value: &Value) -> Vec<String> {
    let mut labels = vec![format!("external-primary:{provider}:{action}")];
    if let Some(label) = value
        .get("route")
        .or_else(|| value.get("label"))
        .or_else(|| value.pointer("/data/route"))
        .or_else(|| value.pointer("/data/label"))
        .and_then(Value::as_str)
    {
        labels.push(label.to_string());
    }
    labels
}

fn v2_route_labels(order: &Value) -> Vec<String> {
    let mut labels = Vec::new();
    if let Some(router) = order.get("router").and_then(Value::as_str) {
        labels.push(format!("router:{router}"));
    }
    if let Some(swap_type) = order.get("swapType").and_then(Value::as_str) {
        labels.push(format!("swapType:{swap_type}"));
    }
    if let Some(mode) = order.get("mode").and_then(Value::as_str) {
        labels.push(format!("mode:{mode}"));
    }
    if let Some(plan) = order.get("routePlan").and_then(Value::as_array) {
        labels.extend(plan.iter().filter_map(|route| {
            route
                .pointer("/swapInfo/label")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        }));
    }
    labels
}

fn v2_order_matches_route_guard(order: &Value, labels: &[String], required: &str) -> bool {
    let required = required.trim().to_ascii_lowercase();
    if required.is_empty() {
        return true;
    }
    let mut route_text = labels.join(" ").to_ascii_lowercase();
    for path in ["/router", "/swapType", "/mode"] {
        if let Some(value) = order.pointer(path).and_then(Value::as_str) {
            route_text.push(' ');
            route_text.push_str(&value.to_ascii_lowercase());
        }
    }
    if matches!(
        required.as_str(),
        "jupiter-v2-rfq" | "jupiter_v2_rfq" | "jupiter-rfq" | "jupiter_rfq"
    ) {
        return route_text.contains("rfq");
    }
    if matches!(required.as_str(), "jupiterz-rfq" | "jupiterz_rfq") {
        return route_text.contains("jupiterz") && route_text.contains("rfq");
    }
    route_text.contains(&required)
}

fn sign_swap_transaction(
    swap_transaction_base64: &str,
    wallet: &solana_sdk::signature::Keypair,
) -> Result<Vec<u8>> {
    let tx_bytes = general_purpose::STANDARD
        .decode(swap_transaction_base64)
        .context("invalid base64 swap transaction")?;
    let mut transaction: VersionedTransaction =
        bincode::deserialize(&tx_bytes).context("failed to deserialize swap transaction")?;
    let required_signatures = usize::from(transaction.message.header().num_required_signatures);
    let static_keys = transaction.message.static_account_keys();
    let signer_index = static_keys
        .iter()
        .take(required_signatures)
        .position(|key| key == &wallet.pubkey())
        .ok_or_else(|| anyhow!("wallet pubkey is not a required signer in swap transaction"))?;
    if transaction.signatures.len() < required_signatures {
        transaction
            .signatures
            .resize(required_signatures, Signature::default());
    }
    if transaction.signatures.len() > required_signatures {
        transaction.signatures.truncate(required_signatures);
    }
    transaction.signatures[signer_index] = wallet.sign_message(&transaction.message.serialize());
    bincode::serialize(&transaction).context("failed to serialize signed swap transaction")
}

fn compose_atomic_instructions(
    config: &BotConfig,
    buy: &JupiterSwapInstructionsResponse,
    sell: &JupiterSwapInstructionsResponse,
) -> Result<Vec<Instruction>> {
    compose_jupiter_chain_instructions(config, &[buy, sell])
}

fn compose_jupiter_chain_instructions(
    config: &BotConfig,
    legs: &[&JupiterSwapInstructionsResponse],
) -> Result<Vec<Instruction>> {
    let mut instructions = Vec::new();
    instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(
        config.execution.compute_unit_limit,
    ));
    let compute_unit_price = if config.execution.compute_unit_price_micro_lamports > 0 {
        config.execution.compute_unit_price_micro_lamports
    } else {
        compute_unit_price_from_max_fee(config)
    };
    if compute_unit_price > 0 {
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(
            compute_unit_price,
        ));
    }

    for leg in legs {
        append_jupiter_swap_leg(&mut instructions, leg)?;
    }
    Ok(instructions)
}

fn append_jupiter_swap_leg(
    instructions: &mut Vec<Instruction>,
    leg: &JupiterSwapInstructionsResponse,
) -> Result<()> {
    append_instruction_list(instructions, &leg.other_instructions)?;
    append_instruction_list(instructions, &leg.setup_instructions)?;
    if let Some(token_ledger_instruction) = &leg.token_ledger_instruction {
        instructions.push(jupiter_instruction(token_ledger_instruction)?);
    }
    instructions.push(jupiter_instruction(&leg.swap_instruction)?);
    if let Some(cleanup_instruction) = &leg.cleanup_instruction {
        instructions.push(jupiter_instruction(cleanup_instruction)?);
    }
    Ok(())
}

fn append_instruction_list(
    instructions: &mut Vec<Instruction>,
    values: &[JupiterInstruction],
) -> Result<()> {
    for value in values {
        instructions.push(jupiter_instruction(value)?);
    }
    Ok(())
}

fn jupiter_instruction(value: &JupiterInstruction) -> Result<Instruction> {
    let program_id = Pubkey::from_str(&value.program_id)
        .with_context(|| format!("invalid instruction program id {}", value.program_id))?;
    let accounts = value
        .accounts
        .iter()
        .map(|account| {
            let pubkey = Pubkey::from_str(&account.pubkey)
                .with_context(|| format!("invalid instruction account {}", account.pubkey))?;
            Ok(if account.is_writable {
                AccountMeta::new(pubkey, account.is_signer)
            } else {
                AccountMeta::new_readonly(pubkey, account.is_signer)
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let data = if value.data.is_empty() {
        Vec::new()
    } else {
        general_purpose::STANDARD
            .decode(&value.data)
            .context("invalid instruction data base64")?
    };
    Ok(Instruction {
        program_id,
        accounts,
        data,
    })
}

fn merge_lookup_table_addresses(
    buy: &JupiterSwapInstructionsResponse,
    sell: &JupiterSwapInstructionsResponse,
) -> Vec<String> {
    merge_lookup_table_addresses_many(&[buy, sell])
}

fn merge_lookup_table_addresses_many(legs: &[&JupiterSwapInstructionsResponse]) -> Vec<String> {
    legs.iter()
        .flat_map(|leg| leg.address_lookup_table_addresses.iter())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn parse_quote_out_amount(quote: &Value) -> Result<u64> {
    quote
        .get("outAmount")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("quote response missing outAmount"))?
        .parse()
        .context("quote outAmount was not a u64")
}

fn estimate_input_usd(input_mint: &str, amount_atoms: u64, sol_usd_estimate: f64) -> Option<f64> {
    match input_mint {
        WSOL_MINT => Some(atoms_to_ui(amount_atoms as u128, 9) * sol_usd_estimate),
        USDC_MINT | USDT_MINT => Some(atoms_to_ui(amount_atoms as u128, 6)),
        _ => None,
    }
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

fn api_input_mint_for_side(pair: &TokenPair, side: SwapSide) -> &str {
    match side {
        SwapSide::QuoteToBase => &pair.quote_mint,
        SwapSide::BaseToQuote => &pair.base_mint,
    }
}

fn api_output_mint_for_side(pair: &TokenPair, side: SwapSide) -> &str {
    match side {
        SwapSide::QuoteToBase => &pair.base_mint,
        SwapSide::BaseToQuote => &pair.quote_mint,
    }
}

fn api_input_decimals_for_side(pair: &TokenPair, side: SwapSide) -> u8 {
    match side {
        SwapSide::QuoteToBase => pair.quote_decimals,
        SwapSide::BaseToQuote => pair.base_decimals,
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

fn validate_swap_instruction_chain(request: &JupiterSwapInstructionChainRequest) -> Result<()> {
    let first = request
        .legs
        .first()
        .expect("chain request has at least one leg");
    if first.input_mint != request.input_mint {
        return Err(anyhow!(
            "chain first leg input mint {} does not match request input mint {}",
            first.input_mint,
            request.input_mint
        ));
    }
    let last = request
        .legs
        .last()
        .expect("chain request has at least one leg");
    if last.output_mint != request.output_mint {
        return Err(anyhow!(
            "chain last leg output mint {} does not match request output mint {}",
            last.output_mint,
            request.output_mint
        ));
    }
    for (index, pair) in request.legs.windows(2).enumerate() {
        if pair[0].output_mint != pair[1].input_mint {
            return Err(anyhow!(
                "chain leg {} output mint {} does not match leg {} input mint {}",
                index + 1,
                pair[0].output_mint,
                index + 2,
                pair[1].input_mint
            ));
        }
    }
    Ok(())
}

fn quote_route_matches_label_guard(labels: &[String], required: &str) -> bool {
    let required = required.trim().to_ascii_lowercase();
    if required.is_empty() {
        return true;
    }
    labels
        .iter()
        .any(|label| label.to_ascii_lowercase().contains(&required))
}

fn route_labels(quote: &Value) -> Vec<String> {
    quote
        .get("routePlan")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|leg| leg.pointer("/swapInfo/label").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect()
}

fn simulation_logs(simulation: &Value) -> Vec<String> {
    simulation
        .pointer("/value/logs")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn simulation_log_tail(logs: &[String], max_logs: usize) -> String {
    if max_logs == 0 || logs.is_empty() {
        return String::new();
    }
    let start = logs.len().saturating_sub(max_logs);
    logs[start..].join(" | ")
}

fn priority_level(percentile: f64) -> &'static str {
    if percentile >= 75.0 {
        "veryHigh"
    } else if percentile >= 50.0 {
        "high"
    } else {
        "medium"
    }
}

fn compute_unit_price_from_max_fee(config: &BotConfig) -> u64 {
    if config.execution.compute_unit_limit == 0 {
        return 0;
    }
    config
        .fees
        .max_priority_fee_lamports
        .saturating_mul(1_000_000)
        / u64::from(config.execution.compute_unit_limit)
}

fn estimated_priority_fee_usd(config: &BotConfig) -> f64 {
    let priority_fee_lamports = if config.execution.compute_unit_price_micro_lamports > 0 {
        u64::from(config.execution.compute_unit_limit)
            .saturating_mul(config.execution.compute_unit_price_micro_lamports)
            / 1_000_000
    } else {
        config.fees.max_priority_fee_lamports
    };
    let total_lamports = priority_fee_lamports
        .saturating_add(config.fees.jito_tip_lamports)
        .saturating_add(5_000);
    atoms_to_ui(total_lamports as u128, 9) * config.trading.sol_usd_estimate
}

fn min_live_requote_profit_bps(config: &BotConfig) -> f64 {
    config
        .execution
        .min_requote_profit_bps
        .max(config.trading.min_net_profit_bps)
}

fn max_live_transaction_bytes() -> usize {
    env_value("LIVE_MAX_TRANSACTION_BYTES")
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value >= 512)
        .unwrap_or(1_232)
}

fn oversized_transaction_reason(byte_len: usize) -> Option<String> {
    oversized_transaction_reason_with_limit(byte_len, max_live_transaction_bytes())
}

fn oversized_transaction_reason_with_limit(byte_len: usize, max_bytes: usize) -> Option<String> {
    (byte_len > max_bytes).then(|| {
        format!(
            "signed transaction is {byte_len} byte(s), above live max {max_bytes}; route is too large for RPC packet submission"
        )
    })
}

fn native_oversized_transaction_reason_with_lut_context(
    byte_len: usize,
    max_bytes: usize,
    configured_lookup_tables: usize,
    loaded_lookup_tables: usize,
) -> Option<String> {
    oversized_transaction_reason_with_limit(byte_len, max_bytes).map(|base| {
        let coverage = if configured_lookup_tables == 0 {
            "missing LUT coverage: no address lookup tables are configured for native execution"
                .to_string()
        } else if loaded_lookup_tables < configured_lookup_tables {
            format!(
                "insufficient LUT coverage: configured {configured_lookup_tables}, loaded {loaded_lookup_tables}"
            )
        } else if loaded_lookup_tables == 0 {
            "missing LUT coverage: no address lookup tables were loaded for native execution"
                .to_string()
        } else {
            format!(
                "insufficient LUT coverage: configured {configured_lookup_tables}, loaded {loaded_lookup_tables}; transaction still exceeds the RPC packet limit"
            )
        };
        format!("{base}; {coverage}")
    })
}

fn stale_candidate_blocker(stage: &str, elapsed: Duration, ttl_ms: u64) -> Option<String> {
    if ttl_ms == 0 {
        return None;
    }
    let elapsed_ms = elapsed.as_millis().min(u128::from(u64::MAX)) as u64;
    (elapsed_ms > ttl_ms).then(|| {
        format!(
            "stale live candidate at {stage}: hot path age {elapsed_ms} ms exceeds ttl {ttl_ms} ms"
        )
    })
}

fn push_latency_mark(trace: &mut Vec<ExecutionLatencyMark>, stage: &str, start: Instant) {
    trace.push(ExecutionLatencyMark {
        stage: stage.to_string(),
        elapsed_ms: start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
    });
}

fn push_optimizer_build_latency_mark(
    trace: &mut Vec<ExecutionLatencyMark>,
    has_optimizer_decision: bool,
    start: Instant,
) {
    let stage = if has_optimizer_decision {
        "optimizer_decision_selected"
    } else {
        "native_plan_build_started"
    };
    push_latency_mark(trace, stage, start);
}

fn native_submit_blocker_after_fresh_quote(
    submit_requested: bool,
    fresh_requote_complete: bool,
    net_profit_usd_after_costs: f64,
    min_absolute_profit_usd: f64,
    session_submit_attempts: u64,
    session_submit_cap: u64,
    cycle_submit_attempts: u64,
    cycle_submit_cap: u64,
) -> Option<String> {
    if !submit_requested {
        return None;
    }
    if !fresh_requote_complete {
        return Some(
            "native submit blocked: fresh native quote is incomplete after costs".to_string(),
        );
    }
    if net_profit_usd_after_costs <= 0.0 {
        return Some(format!(
            "negative fresh native quote after costs: ${net_profit_usd_after_costs:.6}; transaction was not submitted"
        ));
    }
    if min_absolute_profit_usd > 0.0 && net_profit_usd_after_costs < min_absolute_profit_usd {
        return Some(format!(
            "fresh native quote profit ${net_profit_usd_after_costs:.6} is below minimum absolute profit ${min_absolute_profit_usd:.6}"
        ));
    }
    if session_submit_cap > 0 && session_submit_attempts >= session_submit_cap {
        return Some(format!(
            "session native submit cap reached: {session_submit_attempts}/{session_submit_cap}"
        ));
    }
    if cycle_submit_cap > 0 && cycle_submit_attempts >= cycle_submit_cap {
        return Some(format!(
            "cycle native submit cap reached: {cycle_submit_attempts}/{cycle_submit_cap}"
        ));
    }
    None
}

fn is_native_strict_cache_stale_error(error: &str) -> bool {
    error.contains("native strict WS cache stale account")
        && error.contains("age")
        && error.contains("exceeds ttl")
}

fn jito_tip_guard(config: &BotConfig) -> Option<String> {
    if config.execution.jito_block_engine_url.trim().is_empty() {
        return None;
    }
    if config.fees.jito_tip_lamports == 0 {
        return Some("Jito block engine is configured but JITO_TIP_LAMPORTS is zero".to_string());
    }
    if config.fees.max_jito_tip_lamports == 0 {
        return Some(
            "Jito block engine is configured but MAX_JITO_TIP_LAMPORTS is zero".to_string(),
        );
    }
    if config.fees.jito_tip_lamports > config.fees.max_jito_tip_lamports {
        return Some(format!(
            "Jito tip {} lamports exceeds max cap {} lamports",
            config.fees.jito_tip_lamports, config.fees.max_jito_tip_lamports
        ));
    }
    None
}

fn sign_custom_transaction_with_blockhash(
    wallet: &solana_sdk::signature::Keypair,
    instructions: Vec<Instruction>,
    lookup_tables: &[AddressLookupTableAccount],
    blockhash: Hash,
) -> Result<Vec<u8>> {
    if lookup_tables.is_empty() {
        let message = Message::new(&instructions, Some(&wallet.pubkey()));
        let mut tx = solana_sdk::transaction::Transaction::new_unsigned(message);
        tx.sign(&[wallet], blockhash);
        return bincode::serialize(&tx).with_context(|| {
            format!(
                "failed to serialize legacy tx ({} instructions, {} accounts)",
                tx.message.instructions.len(),
                tx.message.account_keys.len()
            )
        });
    }

    let message =
        v0::Message::try_compile(&wallet.pubkey(), &instructions, lookup_tables, blockhash)
            .context("failed to compile v0 atomic arbitrage message")?;
    let transaction = VersionedTransaction::try_new(VersionedMessage::V0(message), &[wallet])
        .context("failed to sign atomic arbitrage transaction")?;
    bincode::serialize(&transaction).context("failed to serialize atomic arbitrage transaction")
}

fn atomic_chain_input_buffer_bps(default_slippage_bps: u16) -> u16 {
    env_value("LIVE_ATOMIC_CHAIN_INPUT_BUFFER_BPS")
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(default_slippage_bps)
        .min(9_999)
}

fn live_arbitrage_slippage_bps(default_slippage_bps: u16) -> u16 {
    let raw = env_value("LIVE_ARBITRAGE_SLIPPAGE_BPS")
        .and_then(|value| value.parse::<u16>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default_slippage_bps.max(1));
    if raw > 200 {
        tracing::warn!(
            "LIVE_ARBITRAGE_SLIPPAGE_BPS={raw} capped at 200 bps to prevent excessive Jupiter routing latitude"
        );
        200
    } else {
        raw
    }
}

fn atomic_chain_sell_input_atoms(buy_out_atoms: u64, buffer_bps: u16) -> u64 {
    if buy_out_atoms == 0 {
        return 0;
    }
    let retained_bps = 10_000_u128.saturating_sub(u128::from(buffer_bps.min(9_999)));
    let buffered = u128::from(buy_out_atoms).saturating_mul(retained_bps) / 10_000;
    buffered.try_into().unwrap_or(u64::MAX)
}

fn jupiter_dex_filter(venue: Venue) -> Option<&'static str> {
    match venue {
        Venue::Orca => Some("Orca V1,Orca V2,Whirlpool"),
        Venue::Meteora => Some("Meteora,Meteora DLMM,Meteora DAMM v2"),
        Venue::Raydium => Some("Raydium,Raydium CLMM,Raydium CP"),
        _ => None,
    }
}

async fn send_to_rpc_url(url: &str, signed_base64: &str) -> Result<String> {
    let client = Client::new();
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "sendTransaction",
        "params": [signed_base64, {
            "encoding": "base64",
            "skipPreflight": true,
            "maxRetries": 0,
            "preflightCommitment": "processed"
        }]
    });
    let response = client
        .post(url)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .context("rpc send failed")?;
    let status = response.status();
    let payload: Value = response.json().await.context("rpc json parse failed")?;
    if !status.is_success() {
        return Err(anyhow!("rpc http {status}: {payload}"));
    }
    if let Some(err) = payload.get("error") {
        return Err(anyhow!("rpc error: {err}"));
    }
    payload
        .get("result")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("rpc response missing signature"))
}

async fn send_jito_bundle(signed_base64: &str, jito_url: &str, client: &Client) -> Result<String> {
    let bundle_url = format!("{jito_url}/api/v1/bundles");
    // Jito block engine expects base58-encoded transactions for bundles
    let bytes = general_purpose::STANDARD
        .decode(signed_base64)
        .context("base64 decode failed for bundle")?;
    let b58_tx = bs58::encode(&bytes).into_string();

    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "sendBundle",
        "params": [[b58_tx]]
    });
    let response = client
        .post(&bundle_url)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .context("jito bundle request failed")?;
    let status = response.status();
    let payload_text = response.text().await.unwrap_or_default();
    let payload: Value = serde_json::from_str(&payload_text).unwrap_or_else(|_| json!({}));
    if !status.is_success() {
        let err_msg = payload
            .pointer("/error/message")
            .and_then(Value::as_str)
            .unwrap_or("unknown jito error");
        return Err(anyhow!("jito bundle http {status}: {err_msg}"));
    }
    if let Some(err) = payload.get("error") {
        return Err(anyhow!("jito bundle rpc error: {err}"));
    }
    let bundle_id = payload
        .get("result")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    Ok(bundle_id.to_string())
}

fn extract_signature_from_legacy_tx(signed_base64: &str) -> Option<String> {
    let bytes = general_purpose::STANDARD.decode(signed_base64).ok()?;
    if bytes.len() < 72 {
        return None;
    }
    // bincode format: u64 LE count, then 64-byte signatures
    let count_bytes: [u8; 8] = bytes[0..8].try_into().ok()?;
    let sig_count = u64::from_le_bytes(count_bytes) as usize;
    if sig_count == 0 || bytes.len() < 8 + sig_count * 64 {
        return None;
    }
    let sig_bytes = &bytes[8..8 + 64];
    let sig = solana_sdk::signature::Signature::try_from(sig_bytes).ok()?;
    Some(sig.to_string())
}

fn is_jito_tip_instruction(ix: &Instruction) -> bool {
    let tip_account =
        Pubkey::from_str("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5").unwrap_or_default();
    ix.program_id == Pubkey::from_str("11111111111111111111111111111111").unwrap_or_default()
        && ix.data.first() == Some(&2) // system program transfer instruction
        && ix.accounts.iter().any(|a| a.pubkey == tip_account)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v2_route_guard_accepts_jupiterz_rfq_order() {
        let order = json!({
            "router": "JupiterZ",
            "swapType": "rfq",
            "routePlan": [{"swapInfo": {"label": "JupiterZ"}}]
        });
        let labels = v2_route_labels(&order);
        assert!(v2_order_matches_route_guard(&order, &labels, "jupiterz"));
        assert!(v2_order_matches_route_guard(
            &order,
            &labels,
            "jupiter-v2-rfq"
        ));
        assert!(v2_order_matches_route_guard(
            &order,
            &labels,
            "jupiterz-rfq"
        ));
    }

    #[test]
    fn oversized_transaction_gate_reports_packet_limit() {
        assert!(oversized_transaction_reason_with_limit(1_000, 1_232).is_none());
        let reason = oversized_transaction_reason_with_limit(1_400, 1_232).unwrap();
        assert!(reason.contains("above live max 1232"));
    }

    #[test]
    fn atomic_chain_sell_input_uses_first_leg_slippage_buffer() {
        assert_eq!(atomic_chain_sell_input_atoms(1_000_000, 50), 995_000);
        assert_eq!(atomic_chain_sell_input_atoms(1_000_000, 0), 1_000_000);
        assert_eq!(atomic_chain_sell_input_atoms(1, 50), 0);
        assert_eq!(atomic_chain_sell_input_atoms(1_000_000, 20_000), 100);
    }

    #[test]
    fn live_arbitrage_slippage_uses_positive_bounded_default() {
        assert_eq!(live_arbitrage_slippage_bps(0), 1);
        assert_eq!(live_arbitrage_slippage_bps(50), 50);
    }

    #[test]
    fn min_live_requote_profit_uses_stricter_configured_gate() {
        let mut config = BotConfig::from_env_defaults();
        config.execution.min_requote_profit_bps = 25.0;
        config.trading.min_net_profit_bps = 100.0;
        assert_eq!(min_live_requote_profit_bps(&config), 100.0);
        config.execution.min_requote_profit_bps = 250.0;
        assert_eq!(min_live_requote_profit_bps(&config), 250.0);
    }

    #[test]
    fn stale_candidate_ttl_blocks_slow_hot_path_before_submit() {
        let blocker =
            stale_candidate_blocker("before_submit", Duration::from_millis(251), 250).unwrap();

        assert!(blocker.contains("stale live candidate"));
        assert!(blocker.contains("before_submit"));
        assert!(
            stale_candidate_blocker("before_submit", Duration::from_millis(250), 250).is_none()
        );
        assert!(stale_candidate_blocker("before_submit", Duration::from_secs(10), 0).is_none());
    }

    #[test]
    fn optimizer_latency_mark_precedes_native_plan_built_and_ttl_blocker_still_uses_plan_stage() {
        let start = Instant::now();
        let mut trace = Vec::new();

        push_optimizer_build_latency_mark(&mut trace, true, start);
        push_latency_mark(&mut trace, "native_plan_built", start);

        let stages: Vec<&str> = trace.iter().map(|mark| mark.stage.as_str()).collect();
        assert_eq!(
            stages,
            vec!["optimizer_decision_selected", "native_plan_built"]
        );

        let blocker =
            stale_candidate_blocker("native_plan_built", Duration::from_millis(251), 250).unwrap();
        assert!(blocker.contains("native_plan_built"));
    }

    #[test]
    fn custom_transaction_signing_uses_v0_when_lookup_tables_are_supplied() {
        let wallet = solana_sdk::signature::Keypair::new();
        let recipient = Pubkey::new_unique();
        let instruction = solana_sdk::system_instruction::transfer(&wallet.pubkey(), &recipient, 1);
        let lookup_table = AddressLookupTableAccount {
            key: Pubkey::new_unique(),
            addresses: vec![recipient],
        };

        let bytes = sign_custom_transaction_with_blockhash(
            &wallet,
            vec![instruction],
            &[lookup_table],
            Hash::new_unique(),
        )
        .unwrap();
        let transaction: VersionedTransaction = bincode::deserialize(&bytes).unwrap();

        assert!(matches!(transaction.message, VersionedMessage::V0(_)));
    }

    #[test]
    fn native_oversized_blocker_reports_missing_lut_coverage() {
        let reason = native_oversized_transaction_reason_with_lut_context(1_400, 1_232, 0, 0)
            .expect("oversized transaction is blocked");

        assert!(reason.contains("above live max 1232"));
        assert!(reason.contains("missing LUT coverage"));
    }

    #[test]
    fn native_oversized_blocker_reports_insufficient_lut_coverage() {
        let reason = native_oversized_transaction_reason_with_lut_context(1_400, 1_232, 2, 1)
            .expect("oversized transaction is blocked");

        assert!(reason.contains("above live max 1232"));
        assert!(reason.contains("insufficient LUT coverage"));
        assert!(reason.contains("configured 2"));
        assert!(reason.contains("loaded 1"));
    }

    #[test]
    fn native_submit_gate_blocks_negative_fresh_quote_even_when_submit_requested() {
        let reason = native_submit_blocker_after_fresh_quote(true, true, -0.01, 0.0, 0, 1, 0, 1)
            .expect("negative fresh quote is blocked");

        assert!(reason.contains("negative fresh native quote"));
    }

    #[test]
    fn native_submit_gate_allows_one_positive_fresh_quote_under_caps() {
        assert!(
            native_submit_blocker_after_fresh_quote(true, true, 0.01, 0.0, 0, 1, 0, 1).is_none()
        );
    }

    #[test]
    fn native_submit_gate_blocks_when_session_or_cycle_caps_are_reached() {
        let session_reason =
            native_submit_blocker_after_fresh_quote(true, true, 0.01, 0.0, 1, 1, 0, 1)
                .expect("session cap is blocked");
        let cycle_reason =
            native_submit_blocker_after_fresh_quote(true, true, 0.01, 0.0, 0, 1, 1, 1)
                .expect("cycle cap is blocked");

        assert!(session_reason.contains("session native submit cap reached"));
        assert!(cycle_reason.contains("cycle native submit cap reached"));
    }

    #[test]
    fn strict_cache_stale_classifier_only_matches_strict_stale_errors() {
        assert!(is_native_strict_cache_stale_error(
            "native sell instruction build failed: native strict WS cache stale account ABC: age 273 ms exceeds ttl 250 ms"
        ));
        assert!(!is_native_strict_cache_stale_error(
            "native strict WS cache missing account ABC"
        ));
        assert!(!is_native_strict_cache_stale_error(
            "native direct executor does not support this opportunity"
        ));
    }

    #[test]
    fn jito_tip_guard_requires_positive_tip_and_cap_when_block_engine_configured() {
        let mut config = BotConfig::from_env_defaults();
        assert!(jito_tip_guard(&config).is_none());

        config.execution.jito_block_engine_url = "https://block-engine.example".to_string();
        config.fees.jito_tip_lamports = 0;
        config.fees.max_jito_tip_lamports = 10_000;
        assert!(jito_tip_guard(&config)
            .unwrap()
            .contains("JITO_TIP_LAMPORTS"));

        config.fees.jito_tip_lamports = 5_000;
        config.fees.max_jito_tip_lamports = 0;
        assert!(jito_tip_guard(&config)
            .unwrap()
            .contains("MAX_JITO_TIP_LAMPORTS"));

        config.fees.max_jito_tip_lamports = 1_000;
        assert!(jito_tip_guard(&config).unwrap().contains("exceeds max cap"));

        config.fees.max_jito_tip_lamports = 10_000;
        assert!(jito_tip_guard(&config).is_none());
    }

    #[test]
    fn v2_route_guard_accepts_non_jupiterz_rfq_order() {
        let order = json!({
            "router": "DFlow",
            "swapType": "rfq",
            "routePlan": [{"swapInfo": {"label": "DFlow RFQ"}}]
        });
        let labels = v2_route_labels(&order);

        assert!(v2_order_matches_route_guard(
            &order,
            &labels,
            "jupiter-v2-rfq"
        ));
        assert!(!v2_order_matches_route_guard(
            &order,
            &labels,
            "jupiterz-rfq"
        ));
    }

    #[test]
    fn v2_route_guard_rejects_non_rfq_order() {
        let order = json!({
            "router": "Metis",
            "swapType": "aggregator",
            "routePlan": [{"swapInfo": {"label": "Whirlpool"}}]
        });
        let labels = v2_route_labels(&order);
        assert!(!v2_order_matches_route_guard(&order, &labels, "jupiterz"));
        assert!(!v2_order_matches_route_guard(
            &order,
            &labels,
            "jupiter-v2-rfq"
        ));
        assert!(!v2_order_matches_route_guard(
            &order,
            &labels,
            "jupiterz-rfq"
        ));
    }

    #[test]
    fn v2_route_guard_rejects_jupiterz_without_rfq_for_primary_route() {
        let order = json!({
            "router": "JupiterZ",
            "swapType": "aggregator",
            "routePlan": [{"swapInfo": {"label": "JupiterZ"}}]
        });
        let labels = v2_route_labels(&order);

        assert!(v2_order_matches_route_guard(&order, &labels, "jupiterz"));
        assert!(!v2_order_matches_route_guard(
            &order,
            &labels,
            "jupiter-v2-rfq"
        ));
        assert!(!v2_order_matches_route_guard(
            &order,
            &labels,
            "jupiterz-rfq"
        ));
    }

    #[test]
    fn external_primary_payload_includes_pyth_fair_value() {
        let request = ExternalPrimaryTransactionRequest {
            provider: "ondo_jupiter_rfq".to_string(),
            action: "mint".to_string(),
            ticker: "NVDA".to_string(),
            input_mint: USDC_MINT.to_string(),
            output_mint: "nvda_ondo_mint".to_string(),
            amount_atoms: 1_000_000,
            pyth_fair_price_usd: 211.75,
            pyth_confidence_bps: 8.5,
            estimated_input_usd: Some(1.0),
            max_usd: 2.0,
            submit: true,
        };

        let payload = external_primary_build_payload(&request, "wallet111", 50);

        assert_eq!(payload["wallet"], json!("wallet111"));
        assert_eq!(payload["provider"], json!("ondo_jupiter_rfq"));
        assert_eq!(payload["action"], json!("mint"));
        assert_eq!(payload["ticker"], json!("NVDA"));
        assert_eq!(payload["amount"], json!("1000000"));
        assert_eq!(payload["amountAtoms"], json!("1000000"));
        assert_eq!(payload["pythFairPriceUsd"], json!(211.75));
        assert_eq!(payload["pyth_fair_price_usd"], json!(211.75));
        assert_eq!(payload["pythConfidenceBps"], json!(8.5));
        assert_eq!(payload["pyth_confidence_bps"], json!(8.5));
        assert_eq!(payload["slippageBps"], json!(50));
        assert_eq!(payload["submit"], json!(false));
    }

    #[test]
    fn simulation_blocked_reason_includes_rpc_error() {
        let outcome = SimulationOutcome {
            ok: false,
            error: Some("{\"InstructionError\":[3,{\"Custom\":1}]}".to_string()),
            logs: vec!["Program log: custom error".to_string()],
        };

        assert_eq!(
            JupiterLiveSwapExecutor::simulation_blocked_reason(
                "native simulation failed",
                &outcome
            ),
            "native simulation failed: {\"InstructionError\":[3,{\"Custom\":1}]} logs: Program log: custom error"
        );
    }
}
