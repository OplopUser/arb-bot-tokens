mod aggregators;
mod api_swap;
mod config;
mod dashboard;
mod db;
mod engine;
mod env_runtime;
mod executor;
mod fees;
mod live_swap;
mod math;
mod native_swap;
mod pool_index;
mod pool_subscription;
mod rpc;
mod rwa;
mod sol_price;
mod types;
mod venues;

use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use config::BotConfig;
use dashboard::DashboardState;
use db::{ArbDatabase, PoolAccountBackfillTarget, PoolAccountGraphRecord};
use engine::BotEngine;
use live_swap::{JupiterChainSmokeRequest, JupiterLiveSwapExecutor, LiveSwapRequest};
use rpc::RpcClient;
use serde::Serialize;
use solana_address::Address;
use solana_sdk::pubkey::Pubkey;
use tracing_subscriber::EnvFilter;
use types::{USDC_MINT, WSOL_MINT};

#[derive(Debug, Parser)]
#[command(name = "arb-bot", about = "Direct on-chain Solana arbitrage bot")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Run {
        #[arg(long, default_value = "config.toml")]
        config: String,
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = 8791)]
        port: u16,
    },
    Discover {
        #[arg(long, default_value = "config.toml")]
        config: String,
    },
    DryRun {
        #[arg(long, default_value = "config.toml")]
        config: String,
    },
    LiveSmoke {
        #[arg(long, default_value = "config.toml")]
        config: String,
        #[arg(long, default_value = WSOL_MINT)]
        input_mint: String,
        #[arg(long, default_value = USDC_MINT)]
        output_mint: String,
        #[arg(long, default_value_t = 1_000_000)]
        amount_atoms: u64,
        #[arg(long)]
        slippage_bps: Option<u16>,
        #[arg(long, default_value_t = false)]
        submit: bool,
        #[arg(long)]
        max_usd: Option<f64>,
    },
    ChainSmoke {
        #[arg(long, default_value = "config.toml")]
        config: String,
        #[arg(long)]
        input_mint: String,
        #[arg(long = "output-mint", required = true, value_delimiter = ',')]
        output_mints: Vec<String>,
        #[arg(long)]
        amount_atoms: u64,
        #[arg(long)]
        slippage_bps: Option<u16>,
        #[arg(long, default_value_t = 2.0)]
        max_usd: f64,
    },
    AtomicArb {
        #[arg(long, default_value = "config.toml")]
        config: String,
        #[arg(long, default_value_t = false)]
        submit: bool,
    },
    DbSummary {
        #[arg(long, default_value = "config.toml")]
        config: String,
        #[arg(long, default_value_t = false)]
        route_status: bool,
        #[arg(long, default_value_t = false)]
        repair_indexed_pool_kinds: bool,
        #[arg(long, default_value_t = false)]
        backfill_pool_accounts: bool,
        #[arg(long, default_value_t = 100)]
        backfill_limit: usize,
    },
    QuoteReplay {
        #[arg(long, default_value = "config.toml")]
        config: String,
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("arb_bot=info".parse()?))
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Run { config, host, port } => {
            let config = BotConfig::load(config).await?;
            let engine = BotEngine::new(config);
            let dashboard_state = DashboardState {
                snapshot: engine.state(),
                control: engine.clone(),
            };
            let engine_task = tokio::spawn({
                let engine = engine.clone();
                async move { engine.run_forever().await }
            });
            let dashboard_task = tokio::spawn(dashboard::serve(host, port, dashboard_state));

            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    engine.emergency_stop().await;
                    tracing::info!("received ctrl-c, stopping");
                }
                result = dashboard_task => {
                    result??;
                }
                _ = engine_task => {}
            }
        }
        Command::Discover { config } => {
            let config = BotConfig::load(config).await?;
            let engine = BotEngine::new(config);
            engine.resume().await;
            engine.run_cycle().await?;
            let snapshot = engine.state().read().await.clone();
            println!("{}", serde_json::to_string_pretty(&snapshot)?);
        }
        Command::DryRun { config } => {
            let config = BotConfig::load(config).await?;
            let engine = BotEngine::new(config.clone());
            engine.resume().await;
            engine.run_cycle().await?;
            let snapshot = engine.state().read().await.clone();
            let executor = executor::Executor::new(config);
            if let Some(opportunity) = snapshot.opportunities.first() {
                let plan = executor.build_plan(opportunity);
                println!("{}", serde_json::to_string_pretty(&plan)?);
            } else {
                println!("No opportunity found for dry-run transaction plan.");
            }
        }
        Command::LiveSmoke {
            config,
            input_mint,
            output_mint,
            amount_atoms,
            slippage_bps,
            submit,
            max_usd,
        } => {
            let config = BotConfig::load(config).await?;
            let request = LiveSwapRequest {
                input_mint,
                output_mint,
                amount_atoms,
                slippage_bps: slippage_bps.unwrap_or(config.jupiter.slippage_bps),
                submit,
                max_usd: max_usd.unwrap_or(config.execution.max_live_smoke_trade_usd),
                estimated_input_usd_override: None,
                required_route_label_contains: None,
                force_swap_instructions: false,
            };
            let executor = JupiterLiveSwapExecutor::new(config);
            let report = executor.run(request).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::ChainSmoke {
            config,
            input_mint,
            output_mints,
            amount_atoms,
            slippage_bps,
            max_usd,
        } => {
            let config = BotConfig::load(config).await?;
            let request = JupiterChainSmokeRequest {
                input_mint,
                output_mints,
                amount_atoms,
                slippage_bps: slippage_bps.unwrap_or(config.jupiter.slippage_bps),
                max_usd,
            };
            let executor = JupiterLiveSwapExecutor::new(config);
            let report = executor.run_chain_smoke(request).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::AtomicArb { config, submit } => {
            let config = BotConfig::load(config).await?;
            if submit && (!config.trading.live || !config.execution.enabled) {
                anyhow::bail!(
                    "--submit requires trading.live=true and execution.enabled=true in config"
                );
            }
            let engine = BotEngine::new(config.clone());
            engine.resume().await;
            engine.run_cycle().await?;
            let snapshot = engine.state().read().await.clone();
            if snapshot.opportunities.is_empty() {
                println!("No opportunity found for atomic arbitrage.");
                return Ok(());
            }
            let executor = JupiterLiveSwapExecutor::new(config);
            let mut reports = Vec::new();
            for opportunity in snapshot.opportunities.iter().take(10) {
                let report = executor
                    .execute_atomic_arbitrage(opportunity, submit)
                    .await?;
                let blocked = matches!(report.status, live_swap::AtomicArbStatus::Blocked);
                reports.push(report);
                if !blocked {
                    break;
                }
            }
            println!("{}", serde_json::to_string_pretty(&reports)?);
        }
        Command::DbSummary {
            config,
            route_status,
            repair_indexed_pool_kinds,
            backfill_pool_accounts,
            backfill_limit,
        } => {
            let config = BotConfig::load(config).await?;
            if !config.database.enabled {
                anyhow::bail!("database.enabled=false in config");
            }
            let db = db::ArbDatabase::open(&config.database.path, config.database.busy_timeout_ms)?;
            if config.database.migrations_on_startup {
                db.run_migrations()?;
            }
            let maintenance_requested = repair_indexed_pool_kinds || backfill_pool_accounts;
            if maintenance_requested {
                let now_unix_ms = system_now_unix_ms()?;
                let kind_repair = if repair_indexed_pool_kinds || backfill_pool_accounts {
                    Some(db.repair_indexed_pool_kinds(now_unix_ms)?)
                } else {
                    None
                };
                let pool_account_backfill = if backfill_pool_accounts {
                    let rpc = RpcClient::new(&config.rpc);
                    Some(
                        backfill_pool_accounts_for_db(&db, &rpc, backfill_limit, now_unix_ms)
                            .await?,
                    )
                } else {
                    None
                };
                let manifest_materialization = Some(db.materialize_route_manifests(now_unix_ms)?);
                let report = DbMaintenanceReport {
                    kind_repair,
                    pool_account_backfill,
                    manifest_materialization,
                };
                if route_status {
                    eprintln!("{}", serde_json::to_string_pretty(&report)?);
                } else {
                    println!("{}", serde_json::to_string_pretty(&report)?);
                    return Ok(());
                }
            }
            if route_status {
                let report = RouteStatusWithFreshScores {
                    route_status: db.route_status_report()?,
                    fresh_quote_route_family_audit: db.fresh_quote_route_family_audit(25)?,
                    fresh_quote_route_scores: db.fresh_quote_route_scores(25)?,
                    fresh_quote_blocked_positive_records: db
                        .fresh_quote_blocked_positive_records(25)?,
                    route_family_promotion_decisions: db.route_family_promotion_decisions(25)?,
                };
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("{}", serde_json::to_string_pretty(&db.summary()?)?);
            }
        }
        Command::QuoteReplay { config, limit } => {
            let config = BotConfig::load(config).await?;
            if !config.database.enabled {
                anyhow::bail!("database.enabled=false in config");
            }
            let db = db::ArbDatabase::open_read_only(
                &config.database.path,
                config.database.busy_timeout_ms,
            )?;
            let report = quote_replay_report(&db, limit)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
    }

    Ok(())
}

#[derive(Debug, Serialize)]
struct DbMaintenanceReport {
    kind_repair: Option<db::PoolKindRepairSummary>,
    pool_account_backfill: Option<PoolAccountBackfillReport>,
    manifest_materialization: Option<db::MaterializedRouteManifestSummary>,
}

#[derive(Debug, Serialize)]
struct RouteStatusWithFreshScores {
    route_status: db::RouteStatusReport,
    fresh_quote_route_family_audit: Vec<db::FreshQuoteRouteFamilyAudit>,
    fresh_quote_route_scores: Vec<db::FreshQuoteRouteScore>,
    fresh_quote_blocked_positive_records: Vec<db::FreshQuoteBlockedPositiveRecord>,
    route_family_promotion_decisions: Vec<db::RouteFamilyPromotionDecision>,
}

#[derive(Debug, Default, Serialize)]
struct PoolAccountBackfillReport {
    targets_scanned: u64,
    pools_backfilled: u64,
    accounts_upserted: u64,
    failures: Vec<String>,
}

#[derive(Debug, Serialize)]
struct QuoteReplayReport {
    observations_scanned: usize,
    observations: Vec<QuoteReplayObservationReport>,
}

#[derive(Debug, Serialize)]
struct QuoteReplayObservationReport {
    quote_observation_id: i64,
    route_edge_id: Option<i64>,
    cycle: i64,
    source: String,
    attempt_id: Option<String>,
    replay: Option<math::OptimizerReplayReport>,
    replay_error: Option<String>,
    output_quote_drift: Option<math::QuoteDrift>,
}

fn quote_replay_report(db: &ArbDatabase, limit: usize) -> Result<QuoteReplayReport> {
    let evidence = db.optimizer_decision_evidence(limit)?;
    let mut observations = Vec::with_capacity(evidence.len());

    for row in evidence {
        let decision = optimizer_decision_from_freshness_json(&row.freshness_json);
        let (replay, replay_error, output_quote_drift) = match decision {
            Ok(decision) => {
                let output_quote_drift = math::quote_drift(
                    Some(decision.output_quote_atoms),
                    parse_optional_u128(row.output_atoms.as_deref())?,
                );
                match math::replay_optimizer_decision_trace(&decision) {
                    Ok(replay) => (Some(replay), None, output_quote_drift),
                    Err(error) => (None, Some(error.to_string()), output_quote_drift),
                }
            }
            Err(error) => (None, Some(error.to_string()), None),
        };
        observations.push(QuoteReplayObservationReport {
            quote_observation_id: row.id,
            route_edge_id: row.route_edge_id,
            cycle: row.cycle,
            source: row.source,
            attempt_id: row.attempt_id,
            replay,
            replay_error,
            output_quote_drift,
        });
    }

    Ok(QuoteReplayReport {
        observations_scanned: observations.len(),
        observations,
    })
}

fn optimizer_decision_from_freshness_json(
    freshness_json: &str,
) -> Result<types::OptimizerDecisionTrace> {
    let context: serde_json::Value =
        serde_json::from_str(freshness_json).context("freshness_json is not valid JSON")?;
    let decision = context
        .get("optimizer_decision")
        .cloned()
        .ok_or_else(|| anyhow!("freshness_json is missing optimizer_decision"))?;
    serde_json::from_value(decision).context("optimizer_decision is not a valid trace")
}

fn parse_optional_u128(value: Option<&str>) -> Result<Option<u128>> {
    value
        .map(|value| {
            value
                .parse::<u128>()
                .with_context(|| format!("invalid u128 atom value {value}"))
        })
        .transpose()
}

async fn backfill_pool_accounts_for_db(
    db: &ArbDatabase,
    rpc: &RpcClient,
    limit: usize,
    now_unix_ms: u128,
) -> Result<PoolAccountBackfillReport> {
    let targets = db.pool_account_backfill_targets(limit)?;
    let mut report = PoolAccountBackfillReport {
        targets_scanned: targets.len() as u64,
        ..PoolAccountBackfillReport::default()
    };

    for target in targets {
        match backfill_pool_account_target(db, rpc, &target, now_unix_ms).await {
            Ok(upserted) => {
                if upserted > 0 {
                    report.pools_backfilled += 1;
                    report.accounts_upserted += upserted;
                }
            }
            Err(error) => report.failures.push(format!(
                "{} {}: {error:#}",
                target.pool_kind, target.pool_address
            )),
        }
    }

    Ok(report)
}

async fn backfill_pool_account_target(
    db: &ArbDatabase,
    rpc: &RpcClient,
    target: &PoolAccountBackfillTarget,
    now_unix_ms: u128,
) -> Result<u64> {
    let pool = fetch_account(rpc, &target.pool_address)
        .await?
        .ok_or_else(|| anyhow!("pool account missing"))?;
    if let Some(expected_owner) = &target.program_owner {
        if !expected_owner.is_empty() && pool.owner != *expected_owner {
            return Err(anyhow!(
                "pool owner mismatch: expected {expected_owner}, got {}",
                pool.owner
            ));
        }
    }

    let mut upserted = 0;
    match target.pool_kind.as_str() {
        "raydium_amm_v4" => {
            let market = read_address_at(&pool.data, 528).context("raydium amm v4 market")?;
            let market_account = fetch_account(rpc, &address_to_string(market))
                .await?
                .ok_or_else(|| anyhow!("raydium amm v4 market account missing"))?;
            let state =
                swaps::dex::raydium_amm_v4::AmmState::from_bytes(&pool.data, &market_account.data)?;
            for (role, address) in [
                ("raydium_amm_coin_vault", state.token_coin()),
                ("raydium_amm_pc_vault", state.token_pc()),
                ("raydium_amm_coin_mint", state.coin_mint()),
                ("raydium_amm_pc_mint", state.pc_mint()),
                ("raydium_amm_open_orders", state.open_orders()),
                ("raydium_amm_market", state.market()),
                ("raydium_amm_serum_dex", state.serum_dex()),
                ("raydium_amm_target_orders", state.target_orders()),
                ("raydium_amm_serum_coin_vault", state.serum_coin_vault()),
                ("raydium_amm_serum_pc_vault", state.serum_pc_vault()),
                ("raydium_amm_serum_event_queue", state.serum_event_queue()),
                ("raydium_amm_serum_bids", state.serum_bids()),
                ("raydium_amm_serum_asks", state.serum_asks()),
                (
                    "raydium_amm_serum_vault_signer",
                    swaps::dex::raydium_amm_v4::serum_vault_signer(
                        &state.market(),
                        &state.serum_dex(),
                        state.vault_signer_nonce(),
                    ),
                ),
            ] {
                upserted += upsert_pool_account_role(db, target, role, address, now_unix_ms)?;
            }
        }
        "constant_product" => {
            let state = swaps::dex::raydium_cpmm::PoolState::from_bytes(&pool.data)?;
            for (role, address) in [
                ("raydium_amm_config", state.amm_config()),
                ("raydium_token_0_vault", state.token_0_vault()),
                ("raydium_token_1_vault", state.token_1_vault()),
                ("raydium_token_0_mint", state.token_0_mint()),
                ("raydium_token_1_mint", state.token_1_mint()),
                ("raydium_token_0_program", state.token_0_program()),
                ("raydium_token_1_program", state.token_1_program()),
                ("raydium_observation", state.observation_key()),
            ] {
                upserted += upsert_pool_account_role(db, target, role, address, now_unix_ms)?;
            }
        }
        "concentrated_liquidity" if target.venue == "raydium" => {
            let state = swaps::dex::raydium_clmm::PoolState::from_bytes(&pool.data)?;
            let pool_address = address_from_pubkey_string(&target.pool_address)?;
            let bitmap_extension =
                swaps::dex::raydium_clmm::tick_array_bitmap_extension_pda(&pool_address).0;
            let bitmap_account = fetch_account(rpc, &address_to_string(bitmap_extension))
                .await?
                .ok_or_else(|| anyhow!("raydium clmm bitmap extension missing"))?;
            let extension =
                swaps::dex::raydium_clmm::BitmapExtensionState::from_bytes(&bitmap_account.data)?;
            for (role, address) in [
                ("raydium_amm_config", state.amm_config()),
                ("raydium_token_0_vault", state.token_vault_0()),
                ("raydium_token_1_vault", state.token_vault_1()),
                ("raydium_token_0_mint", state.token_mint_0()),
                ("raydium_token_1_mint", state.token_mint_1()),
                ("raydium_observation", state.observation_key()),
                ("raydium_bitmap_extension", bitmap_extension),
            ] {
                upserted += upsert_pool_account_role(db, target, role, address, now_unix_ms)?;
            }
            let bitmap = state.tick_array_bitmap()?;
            let tick_array_starts = swaps::dex::raydium_clmm::selected_tick_array_starts(
                state.tick_current(),
                state.tick_spacing(),
                &bitmap,
                &extension,
            );
            for start in tick_array_starts.iter().copied() {
                let role = format!("raydium_tick_array_{start}");
                let address = swaps::dex::raydium_clmm::tick_array_pda(&pool_address, start).0;
                upserted += upsert_pool_account_role(db, target, &role, address, now_unix_ms)?;
            }
        }
        "dlmm" => {
            let state = swaps::dex::meteora_dlmm::PoolState::from_bytes(&pool.data)?;
            let pool_address = address_from_pubkey_string(&target.pool_address)?;
            for (role, address) in [
                ("meteora_reserve_x", state.reserve_x()),
                ("meteora_reserve_y", state.reserve_y()),
                ("meteora_token_x_mint", state.token_x_mint()),
                ("meteora_token_y_mint", state.token_y_mint()),
                ("meteora_oracle", state.oracle()),
            ] {
                upserted += upsert_pool_account_role(db, target, role, address, now_unix_ms)?;
            }
            for index in swaps::dex::meteora_dlmm::selected_bin_array_indexes(state.active_id()) {
                let role = format!("meteora_bin_array_{index}");
                let address = swaps::dex::meteora_dlmm::bin_array_pda(&pool_address, index).0;
                let required = fetch_account(rpc, &address_to_string(address))
                    .await?
                    .is_some();
                upserted += upsert_pool_account_role_with_required(
                    db,
                    target,
                    &role,
                    address,
                    required,
                    now_unix_ms,
                )?;
            }
        }
        "meteora_pools" => {
            let state = swaps::dex::meteora_pools::PoolState::from_bytes(&pool.data)?;
            let a_vault_account = fetch_account(rpc, &address_to_string(state.a_vault()))
                .await?
                .ok_or_else(|| anyhow!("meteora pools a vault missing"))?;
            let b_vault_account = fetch_account(rpc, &address_to_string(state.b_vault()))
                .await?
                .ok_or_else(|| anyhow!("meteora pools b vault missing"))?;
            let a_vault_state =
                swaps::dex::meteora_pools::VaultState::from_bytes(&a_vault_account.data)?;
            let b_vault_state =
                swaps::dex::meteora_pools::VaultState::from_bytes(&b_vault_account.data)?;
            for (role, address) in [
                ("meteora_pool_token_a_mint", state.token_a_mint()),
                ("meteora_pool_token_b_mint", state.token_b_mint()),
                ("meteora_pool_a_vault", state.a_vault()),
                ("meteora_pool_b_vault", state.b_vault()),
                ("meteora_pool_a_vault_lp", state.a_vault_lp()),
                ("meteora_pool_b_vault_lp", state.b_vault_lp()),
                ("meteora_pool_a_token_vault", a_vault_state.token_vault()),
                ("meteora_pool_b_token_vault", b_vault_state.token_vault()),
                ("meteora_pool_a_vault_lp_mint", a_vault_state.lp_mint()),
                ("meteora_pool_b_vault_lp_mint", b_vault_state.lp_mint()),
            ] {
                upserted += upsert_pool_account_role(db, target, role, address, now_unix_ms)?;
            }
        }
        "meteora_damm_v2" => {
            let state = swaps::dex::meteora_damm_v2::PoolState::from_bytes(&pool.data)?;
            for (role, address) in [
                ("meteora_damm_v2_token_a_mint", state.token_a_mint()),
                ("meteora_damm_v2_token_b_mint", state.token_b_mint()),
                ("meteora_damm_v2_token_a_vault", state.token_a_vault()),
                ("meteora_damm_v2_token_b_vault", state.token_b_vault()),
            ] {
                upserted += upsert_pool_account_role(db, target, role, address, now_unix_ms)?;
            }
        }
        other => return Err(anyhow!("unsupported pool account backfill kind {other}")),
    }

    Ok(upserted)
}

async fn fetch_account(rpc: &RpcClient, pubkey: &str) -> Result<Option<types::RawAccount>> {
    let mut accounts = rpc.get_multiple_accounts(&[pubkey.to_string()]).await?;
    Ok(accounts.pop().flatten())
}

fn upsert_pool_account_role(
    db: &ArbDatabase,
    target: &PoolAccountBackfillTarget,
    role: &str,
    address: Address,
    now_unix_ms: u128,
) -> Result<u64> {
    upsert_pool_account_role_with_required(db, target, role, address, true, now_unix_ms)
}

fn upsert_pool_account_role_with_required(
    db: &ArbDatabase,
    target: &PoolAccountBackfillTarget,
    role: &str,
    address: Address,
    required: bool,
    now_unix_ms: u128,
) -> Result<u64> {
    if address_is_zero(address) {
        return Ok(0);
    }
    db.upsert_pool_account(&PoolAccountGraphRecord {
        pool_address: target.pool_address.clone(),
        role: role.to_string(),
        pubkey: address_to_string(address),
        owner: None,
        mint: None,
        token_program: None,
        derivation_kind: Some("pool_state".to_string()),
        derivation_seed_json: None,
        required,
        now_unix_ms,
    })?;
    Ok(1)
}

fn read_address_at(data: &[u8], offset: usize) -> Result<Address> {
    let bytes = data
        .get(offset..offset + 32)
        .ok_or_else(|| anyhow!("account data too short for address at offset {offset}"))?;
    let mut out = [0u8; 32];
    out.copy_from_slice(bytes);
    Ok(Address::new_from_array(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_pool_account_role_can_persist_optional_derived_account() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let target = PoolAccountBackfillTarget {
            pool_address: "Pool111111111111111111111111111111111111".to_string(),
            venue: "meteora".to_string(),
            pool_kind: "dlmm".to_string(),
            program_owner: None,
        };
        for (mint, symbol, decimals) in [
            (
                "BaseMint111111111111111111111111111111111".to_string(),
                "BASE",
                6,
            ),
            (WSOL_MINT.to_string(), "WSOL", 9),
        ] {
            db.upsert_token(&db::TokenGraphRecord {
                mint,
                symbol: symbol.to_string(),
                decimals,
                token_program: None,
                now_unix_ms: 1_000,
            })
            .expect("token");
        }
        db.upsert_pool(&db::PoolGraphRecord {
            pool_address: target.pool_address.clone(),
            venue: types::Venue::Meteora,
            pool_kind: types::PoolKind::Dlmm,
            program_owner: None,
            pair: types::TokenPair {
                base_mint: "BaseMint111111111111111111111111111111111".to_string(),
                quote_mint: WSOL_MINT.to_string(),
                base_decimals: 6,
                quote_decimals: 9,
            },
            liquidity_quote_ui: Some(1.0),
            price_quote_per_base: Some(0.002),
            updated_slot: 1,
            source: "test".to_string(),
            now_unix_ms: 1_000,
        })
        .expect("pool");
        let address = Address::new_from_array([1; 32]);

        upsert_pool_account_role_with_required(
            &db,
            &target,
            "meteora_bin_array_0",
            address,
            false,
            1_000,
        )
        .expect("optional account role");

        let required: i64 = db
            .conn_for_test()
            .query_row(
                "SELECT required FROM pool_accounts WHERE pool_address = ?1 AND role = ?2",
                [&target.pool_address, "meteora_bin_array_0"],
                |row| row.get(0),
            )
            .expect("required flag");
        assert_eq!(required, 0);
    }
}

fn address_from_pubkey_string(value: &str) -> Result<Address> {
    let pubkey = value
        .parse::<Pubkey>()
        .with_context(|| format!("invalid pubkey {value}"))?;
    Ok(Address::new_from_array(pubkey.to_bytes()))
}

fn address_to_string(address: Address) -> String {
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(address.as_ref());
    Pubkey::new_from_array(bytes).to_string()
}

fn address_is_zero(address: Address) -> bool {
    address.as_ref().iter().all(|byte| *byte == 0)
}

fn system_now_unix_ms() -> Result<u128> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_millis())
}
