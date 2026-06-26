use crate::types::{
    PoolKind, RouteAccountRequirement, RouteReadinessState, TokenPair, Venue,
    METEORA_DAMM_V2_PROGRAM, METEORA_DLMM_PROGRAM, METEORA_POOLS_PROGRAM, RAYDIUM_AMM_V4_PROGRAM,
    RAYDIUM_CLMM_PROGRAM, RAYDIUM_CPMM_PROGRAM,
};
use anyhow::{anyhow, Context, Result};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const INITIAL_SCHEMA_ID: i64 = 1;
const INITIAL_SCHEMA_NAME: &str = "initial_route_graph_schema";
const INITIAL_SCHEMA_CHECKSUM: &str = "2026-06-16-token-route-graph-v2";

pub struct ArbDatabase {
    conn: Connection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DatabaseSummary {
    pub tokens: u64,
    pub pools: u64,
    pub pool_accounts: u64,
    pub route_families: u64,
    pub route_edges: u64,
    pub account_manifests: u64,
    pub route_account_observations: u64,
    pub quote_observations: u64,
    pub simulation_results: u64,
    pub execution_attempts: u64,
    pub live_settlements: u64,
    pub live_settlement_fee_losses: u64,
    pub live_settlement_profits: u64,
    pub fresh_positive_complete_observations: u64,
    pub fresh_positive_blocked_observations: u64,
    pub fresh_negative_complete_observations: u64,
    pub fresh_quote_unavailable_observations: u64,
    pub simulation_positive_results: u64,
    pub simulation_positive_with_fresh_positive_complete: u64,
    pub route_cooldowns: u64,
    pub applied_migrations: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RouteStatusReport {
    pub summary: DatabaseSummary,
    pub families: Vec<RouteFamilyStatus>,
    pub account_blockers: Vec<RouteAccountBlockerSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RouteFamilyStatus {
    pub id: i64,
    pub base_mint: String,
    pub quote_mint: String,
    pub buy_venue: String,
    pub sell_venue: String,
    pub enabled: bool,
    pub priority: i64,
    pub total_edges: u64,
    pub readiness_counts: Vec<RouteReadinessCount>,
    pub latest_blockers: Vec<RouteBlockerSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RouteReadinessCount {
    pub readiness_state: String,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RouteBlockerSummary {
    pub readiness_state: String,
    pub blocker: String,
    pub count: u64,
    pub latest_updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RouteAccountBlockerSummary {
    pub route_edge_id: i64,
    pub route_family_id: i64,
    pub base_mint: String,
    pub quote_mint: String,
    pub buy_venue: String,
    pub sell_venue: String,
    pub role: String,
    pub pubkey: String,
    pub outcome: String,
    pub reason: String,
    pub observed_at_unix_ms: i64,
    pub next_retry_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FreshQuoteRouteScore {
    pub route_edge_id: i64,
    pub route_family_id: i64,
    pub base_mint: String,
    pub quote_mint: String,
    pub buy_venue: String,
    pub sell_venue: String,
    pub buy_pool: String,
    pub sell_pool: String,
    pub fresh_positive_complete: u64,
    pub fresh_positive_blocked: u64,
    pub fresh_negative_complete: u64,
    pub fresh_quote_unavailable: u64,
    pub best_complete_fresh_bps: Option<f64>,
    pub latest_observed_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FreshQuoteRouteFamilyAudit {
    pub route_family_id: i64,
    pub base_mint: String,
    pub quote_mint: String,
    pub buy_venue: String,
    pub sell_venue: String,
    pub enabled: bool,
    pub priority: i64,
    pub total_edges: u64,
    pub observed_edges: u64,
    pub complete_fresh_edges: u64,
    pub positive_complete_edges: u64,
    pub fresh_positive_complete: u64,
    pub fresh_positive_blocked: u64,
    pub fresh_negative_complete: u64,
    pub fresh_quote_unavailable: u64,
    pub total_fresh_observations: u64,
    pub best_complete_fresh_bps: Option<f64>,
    pub latest_observed_at_unix_ms: Option<i64>,
    pub unavailable_ratio: f64,
    pub recommendation: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FreshQuoteBlockedPositiveRecord {
    pub quote_observation_id: i64,
    pub route_edge_id: i64,
    pub route_family_id: i64,
    pub base_mint: String,
    pub quote_mint: String,
    pub buy_venue: String,
    pub sell_venue: String,
    pub buy_pool: String,
    pub sell_pool: String,
    pub cycle: i64,
    pub source: String,
    pub input_atoms: String,
    pub output_atoms: Option<String>,
    pub net_profit_atoms: Option<String>,
    pub net_profit_bps: Option<f64>,
    pub blocker: Option<String>,
    pub freshness_json: String,
    pub observed_at_unix_ms: i64,
    pub attempt_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RouteFamilyPromotionDecision {
    pub route_family_id: i64,
    pub base_mint: String,
    pub quote_mint: String,
    pub buy_venue: String,
    pub sell_venue: String,
    pub action: String,
    pub score: f64,
    pub hot_path_eligible: bool,
    pub reason: String,
    pub total_edges: u64,
    pub observed_edges: u64,
    pub complete_fresh_edges: u64,
    pub positive_complete_edges: u64,
    pub fresh_positive_complete: u64,
    pub fresh_positive_blocked: u64,
    pub fresh_negative_complete: u64,
    pub fresh_quote_unavailable: u64,
    pub best_complete_fresh_bps: Option<f64>,
    pub latest_observed_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MaterializedRouteManifestSummary {
    pub routes_scanned: u64,
    pub complete_manifests: u64,
    pub incomplete_manifests: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PoolKindRepairSummary {
    pub pools_updated: u64,
    pub buy_edges_updated: u64,
    pub sell_edges_updated: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolAccountBackfillTarget {
    pub pool_address: String,
    pub venue: String,
    pub pool_kind: String,
    pub program_owner: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RouteAccountFetchTarget {
    pub route_edge_id: i64,
    pub route_family_id: i64,
    pub base_mint: String,
    pub quote_mint: String,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    pub buy_venue: String,
    pub sell_venue: String,
    pub buy_pool: String,
    pub sell_pool: String,
    pub buy_pool_kind: String,
    pub sell_pool_kind: String,
    pub buy_pool_liquidity_quote_ui: Option<f64>,
    pub sell_pool_liquidity_quote_ui: Option<f64>,
    pub buy_pool_price_quote_per_base: Option<f64>,
    pub sell_pool_price_quote_per_base: Option<f64>,
    pub requirements: Vec<RouteAccountRequirement>,
}

pub struct TokenGraphRecord {
    pub mint: String,
    pub symbol: String,
    pub decimals: u8,
    pub token_program: Option<String>,
    pub now_unix_ms: u128,
}

pub struct PoolGraphRecord {
    pub pool_address: String,
    pub venue: Venue,
    pub pool_kind: PoolKind,
    pub program_owner: Option<String>,
    pub pair: TokenPair,
    pub liquidity_quote_ui: Option<f64>,
    pub price_quote_per_base: Option<f64>,
    pub updated_slot: u64,
    pub source: String,
    pub now_unix_ms: u128,
}

pub struct PoolAccountGraphRecord {
    pub pool_address: String,
    pub role: String,
    pub pubkey: String,
    pub owner: Option<String>,
    pub mint: Option<String>,
    pub token_program: Option<String>,
    pub derivation_kind: Option<String>,
    pub derivation_seed_json: Option<String>,
    pub required: bool,
    pub now_unix_ms: u128,
}

pub struct RouteFamilyGraphRecord {
    pub pair: TokenPair,
    pub buy_venue: Venue,
    pub sell_venue: Venue,
    pub enabled: bool,
    pub priority: i64,
    pub now_unix_ms: u128,
}

pub struct RouteEdgeGraphRecord {
    pub route_family_id: i64,
    pub buy_pool: String,
    pub sell_pool: String,
    pub buy_pool_kind: PoolKind,
    pub sell_pool_kind: PoolKind,
    pub readiness_state: RouteReadinessState,
    pub last_blocker: Option<String>,
    pub cooldown_until_unix_ms: Option<u128>,
    pub now_unix_ms: u128,
}

#[allow(dead_code)]
pub struct AccountManifestGraphRecord {
    pub route_edge_id: i64,
    pub manifest_version: i64,
    pub complete: bool,
    pub missing_roles_json: String,
    pub account_roles_json: String,
    pub refreshed_slot: Option<u64>,
    pub refreshed_at_unix_ms: u128,
    pub decode_status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum RouteAccountObservationOutcome {
    Fetched,
    Missing,
    OwnerMismatch,
    Stale,
}

impl RouteAccountObservationOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            RouteAccountObservationOutcome::Fetched => "fetched",
            RouteAccountObservationOutcome::Missing => "missing",
            RouteAccountObservationOutcome::OwnerMismatch => "owner_mismatch",
            RouteAccountObservationOutcome::Stale => "stale",
        }
    }
}

pub struct RouteAccountObservationRecord {
    pub route_edge_id: i64,
    pub role: String,
    pub pubkey: String,
    pub outcome: RouteAccountObservationOutcome,
    pub reason: String,
    pub expected_owner: Option<String>,
    pub actual_owner: Option<String>,
    pub slot: Option<u64>,
    pub slot_age: Option<u64>,
    pub age_ms: Option<u128>,
    pub observed_at_unix_ms: u128,
    pub next_retry_unix_ms: Option<u128>,
}

pub struct QuoteObservationRecord {
    pub route_edge_id: Option<i64>,
    pub cycle: i64,
    pub source: String,
    pub input_atoms: String,
    pub output_atoms: Option<String>,
    pub net_profit_atoms: Option<String>,
    pub net_profit_bps: Option<f64>,
    pub fee_atoms: Option<String>,
    pub slot: Option<u64>,
    pub freshness_json: String,
    pub blocker: Option<String>,
    pub observed_at_unix_ms: u128,
    pub attempt_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OptimizerDecisionEvidenceRecord {
    pub id: i64,
    pub route_edge_id: Option<i64>,
    pub cycle: i64,
    pub source: String,
    pub input_atoms: String,
    pub output_atoms: Option<String>,
    pub net_profit_atoms: Option<String>,
    pub net_profit_bps: Option<f64>,
    pub freshness_json: String,
    pub observed_at_unix_ms: u128,
    pub attempt_id: Option<String>,
}

pub struct SimulationResultRecord {
    pub route_edge_id: Option<i64>,
    pub cycle: i64,
    pub input_atoms: String,
    pub pre_quote_atoms: Option<String>,
    pub post_quote_atoms: Option<String>,
    pub quote_delta_atoms: Option<String>,
    pub net_profit_atoms: Option<String>,
    pub net_profit_bps: Option<f64>,
    pub tx_bytes: Option<usize>,
    pub simulation_ok: bool,
    pub simulation_error: Option<String>,
    pub logs_json: String,
    pub simulated_at_unix_ms: u128,
    pub attempt_id: Option<String>,
}

pub struct ExecutionAttemptRecord {
    pub route_edge_id: Option<i64>,
    pub cycle: i64,
    pub status: String,
    pub tx_signature: Option<String>,
    pub input_atoms: Option<String>,
    pub estimated_profit_atoms: Option<String>,
    pub realized_profit_atoms: Option<String>,
    pub blocker: Option<String>,
    pub config_hash: String,
    pub wallet_pubkey: Option<String>,
    pub submitted_at_unix_ms: Option<u128>,
    pub confirmed_at_unix_ms: Option<u128>,
    pub attempt_id: Option<String>,
}

pub struct LiveSettlementRecord {
    pub route_edge_id: Option<i64>,
    pub attempt_id: Option<String>,
    pub tx_signature: String,
    pub status: String,
    pub quote_mint: String,
    pub input_atoms: Option<String>,
    pub estimated_profit_atoms: Option<String>,
    pub realized_quote_delta_atoms: Option<String>,
    pub fee_lamports: Option<u64>,
    pub fee_quote_atoms: Option<String>,
    pub realized_profit_atoms: Option<String>,
    pub classification: String,
    pub pre_balances_json: String,
    pub post_balances_json: String,
    pub meta_json: String,
    pub settled_at_unix_ms: u128,
}

impl ArbDatabase {
    pub fn open(path: impl AsRef<Path>, busy_timeout_ms: u64) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create database dir {}", parent.display()))?;
        }
        let conn = Connection::open(path)
            .with_context(|| format!("failed to open database {}", path.display()))?;
        Self::from_connection(conn, busy_timeout_ms)
    }

    pub fn open_read_only(path: impl AsRef<Path>, busy_timeout_ms: u64) -> Result<Self> {
        let path = path.as_ref();
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .with_context(|| format!("failed to open database read-only {}", path.display()))?;
        conn.busy_timeout(Duration::from_millis(busy_timeout_ms))?;
        conn.pragma_update(None, "query_only", "ON")?;
        Ok(Self { conn })
    }

    #[cfg(test)]
    pub(crate) fn open_in_memory() -> Result<Self> {
        Self::from_connection(Connection::open_in_memory()?, 5_000)
    }

    fn from_connection(conn: Connection, busy_timeout_ms: u64) -> Result<Self> {
        conn.busy_timeout(Duration::from_millis(busy_timeout_ms))?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        Ok(Self { conn })
    }

    pub fn run_migrations(&self) -> Result<()> {
        self.conn.execute_batch(INITIAL_SCHEMA_SQL)?;
        self.conn.execute(
            r#"
            INSERT OR IGNORE INTO schema_migrations (id, name, checksum, applied_at_unix_ms)
            VALUES (?1, ?2, ?3, CAST(strftime('%s','now') AS INTEGER) * 1000)
            "#,
            (
                INITIAL_SCHEMA_ID,
                INITIAL_SCHEMA_NAME,
                INITIAL_SCHEMA_CHECKSUM,
            ),
        )?;
        for alter in ATTEMPT_CHAIN_MIGRATION {
            if let Err(e) = self.conn.execute(alter, []) {
                if !e.to_string().contains("duplicate column") {
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    pub fn summary(&self) -> Result<DatabaseSummary> {
        Ok(DatabaseSummary {
            tokens: self.count_table("tokens")?,
            pools: self.count_table("pools")?,
            pool_accounts: self.count_table("pool_accounts")?,
            route_families: self.count_table("route_families")?,
            route_edges: self.count_table("route_edges")?,
            account_manifests: self.count_table("account_manifests")?,
            route_account_observations: self.count_table("route_account_observations")?,
            quote_observations: self.count_table("quote_observations")?,
            simulation_results: self.count_table("simulation_results")?,
            execution_attempts: self.count_table("execution_attempts")?,
            live_settlements: self.count_table("live_settlements")?,
            live_settlement_fee_losses: self
                .count_live_settlement_classification("failed_fee_loss")?,
            live_settlement_profits: self.count_live_settlement_classification("landed_profit")?,
            fresh_positive_complete_observations: self
                .count_fresh_quote_status("fresh_positive_complete")?,
            fresh_positive_blocked_observations: self
                .count_fresh_quote_status("fresh_positive_blocked")?,
            fresh_negative_complete_observations: self
                .count_fresh_quote_status("fresh_negative_complete")?,
            fresh_quote_unavailable_observations: self
                .count_fresh_quote_status("fresh_quote_unavailable")?,
            simulation_positive_results: self.count_positive_simulation_results()?,
            simulation_positive_with_fresh_positive_complete: self
                .count_positive_simulations_with_fresh_positive_complete()?,
            route_cooldowns: self.count_table("route_cooldowns")?,
            applied_migrations: self.count_table("schema_migrations")?,
        })
    }

    pub fn route_status_report(&self) -> Result<RouteStatusReport> {
        let summary = self.summary()?;
        let mut stmt = self.conn.prepare(
            r#"
            SELECT
                rf.id,
                rf.base_mint,
                rf.quote_mint,
                rf.buy_venue,
                rf.sell_venue,
                rf.enabled,
                rf.priority,
                COUNT(re.id) AS total_edges
            FROM route_families rf
            LEFT JOIN route_edges re ON re.route_family_id = rf.id
            GROUP BY
                rf.id,
                rf.base_mint,
                rf.quote_mint,
                rf.buy_venue,
                rf.sell_venue,
                rf.enabled,
                rf.priority
            ORDER BY rf.priority DESC, rf.id ASC
            "#,
        )?;
        let family_rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, u64>(7)?,
            ))
        })?;

        let mut families = Vec::new();
        for row in family_rows {
            let (id, base_mint, quote_mint, buy_venue, sell_venue, enabled, priority, total_edges) =
                row?;
            families.push(RouteFamilyStatus {
                id,
                base_mint,
                quote_mint,
                buy_venue,
                sell_venue,
                enabled: enabled != 0,
                priority,
                total_edges,
                readiness_counts: self.route_readiness_counts(id)?,
                latest_blockers: self.route_blocker_summaries(id)?,
            });
        }

        Ok(RouteStatusReport {
            summary,
            families,
            account_blockers: self.route_account_blockers(50)?,
        })
    }

    pub fn materialize_route_manifests(
        &self,
        now_unix_ms: u128,
    ) -> Result<MaterializedRouteManifestSummary> {
        let now_unix_ms_i64 = unix_ms_i64(now_unix_ms)?;
        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, buy_pool, sell_pool, buy_pool_kind, sell_pool_kind
            FROM route_edges
            ORDER BY id ASC
            "#,
        )?;
        let route_rows = stmt.query_map([], |row| {
            Ok(RouteEdgeManifestInput {
                id: row.get(0)?,
                buy_pool: row.get(1)?,
                sell_pool: row.get(2)?,
                buy_pool_kind: row.get(3)?,
                sell_pool_kind: row.get(4)?,
            })
        })?;
        let routes = route_rows.collect::<std::result::Result<Vec<_>, _>>()?;

        let mut summary = MaterializedRouteManifestSummary {
            routes_scanned: 0,
            complete_manifests: 0,
            incomplete_manifests: 0,
        };

        for route in routes {
            summary.routes_scanned += 1;
            let buy_accounts = self.pool_account_roles(&route.buy_pool)?;
            let sell_accounts = self.pool_account_roles(&route.sell_pool)?;
            let mut missing_roles = Vec::new();
            let mut account_roles = BTreeMap::new();

            collect_leg_manifest(
                "buy",
                &route.buy_pool_kind,
                &buy_accounts,
                &mut account_roles,
                &mut missing_roles,
            );
            collect_leg_manifest(
                "sell",
                &route.sell_pool_kind,
                &sell_accounts,
                &mut account_roles,
                &mut missing_roles,
            );

            let complete = missing_roles.is_empty();
            let decode_status = if complete {
                "complete".to_string()
            } else {
                format!("missing route accounts: {}", missing_roles.join(", "))
            };
            let missing_roles_json = serde_json::to_string(&missing_roles)?;
            let account_roles_json = serde_json::to_string(&account_roles)?;

            self.insert_account_manifest(&AccountManifestGraphRecord {
                route_edge_id: route.id,
                manifest_version: 1,
                complete,
                missing_roles_json,
                account_roles_json,
                refreshed_slot: None,
                refreshed_at_unix_ms: now_unix_ms,
                decode_status: decode_status.clone(),
            })?;

            let readiness_state = if complete {
                summary.complete_manifests += 1;
                RouteReadinessState::GraphComplete
            } else {
                summary.incomplete_manifests += 1;
                RouteReadinessState::GraphIncomplete
            };
            let blocker = if complete { None } else { Some(decode_status) };
            self.conn.execute(
                r#"
                UPDATE route_edges
                SET readiness_state = ?1,
                    last_blocker = ?2,
                    updated_at_unix_ms = ?3
                WHERE id = ?4
                "#,
                params![readiness_state.as_str(), blocker, now_unix_ms_i64, route.id,],
            )?;
        }

        Ok(summary)
    }

    pub fn repair_indexed_pool_kinds(&self, now_unix_ms: u128) -> Result<PoolKindRepairSummary> {
        let now_unix_ms_i64 = unix_ms_i64(now_unix_ms)?;
        let pools_updated = self.conn.execute(
            r#"
            UPDATE pools
            SET pool_kind = CASE
                    WHEN venue = 'raydium' AND program_owner = ?1 THEN 'raydium_amm_v4'
                    WHEN venue = 'raydium' AND program_owner = ?2 THEN 'concentrated_liquidity'
                    WHEN venue = 'raydium' AND program_owner = ?3 THEN 'constant_product'
                    WHEN venue = 'meteora' AND program_owner = ?4 THEN 'dlmm'
                    WHEN venue = 'meteora' AND program_owner = ?5 THEN 'meteora_damm_v2'
                    WHEN venue = 'meteora' AND program_owner = ?6 THEN 'meteora_pools'
                    ELSE pool_kind
                END,
                last_seen_unix_ms = ?7
            WHERE pool_kind != CASE
                    WHEN venue = 'raydium' AND program_owner = ?1 THEN 'raydium_amm_v4'
                    WHEN venue = 'raydium' AND program_owner = ?2 THEN 'concentrated_liquidity'
                    WHEN venue = 'raydium' AND program_owner = ?3 THEN 'constant_product'
                    WHEN venue = 'meteora' AND program_owner = ?4 THEN 'dlmm'
                    WHEN venue = 'meteora' AND program_owner = ?5 THEN 'meteora_damm_v2'
                    WHEN venue = 'meteora' AND program_owner = ?6 THEN 'meteora_pools'
                    ELSE pool_kind
                END
            "#,
            params![
                RAYDIUM_AMM_V4_PROGRAM,
                RAYDIUM_CLMM_PROGRAM,
                RAYDIUM_CPMM_PROGRAM,
                METEORA_DLMM_PROGRAM,
                METEORA_DAMM_V2_PROGRAM,
                METEORA_POOLS_PROGRAM,
                now_unix_ms_i64,
            ],
        )?;
        let buy_edges_updated = self.conn.execute(
            r#"
            UPDATE route_edges
            SET buy_pool_kind = (
                    SELECT pool_kind
                    FROM pools
                    WHERE pools.pool_address = route_edges.buy_pool
                ),
                updated_at_unix_ms = ?1
            WHERE EXISTS (
                    SELECT 1
                    FROM pools
                    WHERE pools.pool_address = route_edges.buy_pool
                      AND pools.pool_kind != route_edges.buy_pool_kind
                )
            "#,
            [now_unix_ms_i64],
        )?;
        let sell_edges_updated = self.conn.execute(
            r#"
            UPDATE route_edges
            SET sell_pool_kind = (
                    SELECT pool_kind
                    FROM pools
                    WHERE pools.pool_address = route_edges.sell_pool
                ),
                updated_at_unix_ms = ?1
            WHERE EXISTS (
                    SELECT 1
                    FROM pools
                    WHERE pools.pool_address = route_edges.sell_pool
                      AND pools.pool_kind != route_edges.sell_pool_kind
                )
            "#,
            [now_unix_ms_i64],
        )?;

        Ok(PoolKindRepairSummary {
            pools_updated: pools_updated as u64,
            buy_edges_updated: buy_edges_updated as u64,
            sell_edges_updated: sell_edges_updated as u64,
        })
    }

    pub fn pool_account_backfill_targets(
        &self,
        limit: usize,
    ) -> Result<Vec<PoolAccountBackfillTarget>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit_i64 = i64::try_from(limit).context("pool account backfill limit exceeds i64")?;
        let mut stmt = self.conn.prepare(
            r#"
            SELECT p.pool_address, p.venue, p.pool_kind, p.program_owner
            FROM pools p
            WHERE p.pool_kind IN (
                'dlmm',
                'constant_product',
                'concentrated_liquidity',
                'raydium_amm_v4',
                'meteora_pools',
                'meteora_damm_v2'
            )
              AND (
                  EXISTS (
                      SELECT 1
                      FROM route_edges re
                      WHERE re.buy_pool = p.pool_address OR re.sell_pool = p.pool_address
                  )
              )
              AND (
                  NOT EXISTS (
                      SELECT 1
                      FROM pool_accounts pa
                      WHERE pa.pool_address = p.pool_address
                  )
                  OR EXISTS (
                      SELECT 1
                      FROM account_manifests am
                      JOIN route_edges re ON re.id = am.route_edge_id
                      WHERE am.id = (
                          SELECT id
                          FROM account_manifests latest
                          WHERE latest.route_edge_id = re.id
                          ORDER BY id DESC
                          LIMIT 1
                      )
                        AND am.complete = 0
                        AND (re.buy_pool = p.pool_address OR re.sell_pool = p.pool_address)
                  )
              )
            ORDER BY
                COALESCE(p.liquidity_quote_ui, 0.0) DESC,
                p.last_seen_unix_ms DESC,
                p.pool_address ASC
            LIMIT ?1
            "#,
        )?;
        let rows = stmt.query_map([limit_i64], |row| {
            Ok(PoolAccountBackfillTarget {
                pool_address: row.get(0)?,
                venue: row.get(1)?,
                pool_kind: row.get(2)?,
                program_owner: row.get(3)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn complete_route_account_fetch_targets(
        &self,
        limit: usize,
    ) -> Result<Vec<RouteAccountFetchTarget>> {
        self.complete_route_account_fetch_targets_at(limit, system_now_unix_ms()?)
    }

    pub fn complete_route_account_fetch_targets_at(
        &self,
        limit: usize,
        now_unix_ms: u128,
    ) -> Result<Vec<RouteAccountFetchTarget>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit_i64 = i64::try_from(limit).context("route fetch target limit exceeds i64")?;
        let now_unix_ms_i64 = unix_ms_i64(now_unix_ms)?;
        let mut stmt = self.conn.prepare(
            r#"
            SELECT
                re.id,
                re.route_family_id,
                rf.base_mint,
                rf.quote_mint,
                buy_pool.base_decimals,
                buy_pool.quote_decimals,
                buy_pool.venue,
                sell_pool.venue,
                re.buy_pool,
                re.sell_pool,
                re.buy_pool_kind,
                re.sell_pool_kind,
                buy_pool.program_owner,
                sell_pool.program_owner,
                buy_pool.liquidity_quote_ui,
                sell_pool.liquidity_quote_ui,
                buy_pool.price_quote_per_base,
                sell_pool.price_quote_per_base
            FROM route_edges re
            JOIN route_families rf ON rf.id = re.route_family_id
            JOIN pools buy_pool ON buy_pool.pool_address = re.buy_pool
            JOIN pools sell_pool ON sell_pool.pool_address = re.sell_pool
            JOIN account_manifests am ON am.id = (
                SELECT id
                FROM account_manifests
                WHERE route_edge_id = re.id
                ORDER BY id DESC
                LIMIT 1
            )
            WHERE re.readiness_state IN (?1, ?2, ?3, ?4, ?5, ?6)
              AND am.complete != 0
              AND (
                    re.cooldown_until_unix_ms IS NULL
                    OR re.cooldown_until_unix_ms <= ?7
              )
            ORDER BY re.updated_at_unix_ms DESC, re.id ASC
            LIMIT ?8
            "#,
        )?;
        let rows = stmt.query_map(
            params![
                RouteReadinessState::GraphComplete.as_str(),
                RouteReadinessState::FreshAccountsReady.as_str(),
                RouteReadinessState::FreshQuoteReady.as_str(),
                RouteReadinessState::SimulationPositive.as_str(),
                RouteReadinessState::SubmitEligible.as_str(),
                RouteReadinessState::CooledDown.as_str(),
                now_unix_ms_i64,
                limit_i64
            ],
            |row| {
                Ok(RouteAccountFetchTargetInput {
                    route_edge_id: row.get(0)?,
                    route_family_id: row.get(1)?,
                    base_mint: row.get(2)?,
                    quote_mint: row.get(3)?,
                    base_decimals: row.get(4)?,
                    quote_decimals: row.get(5)?,
                    buy_venue: row.get(6)?,
                    sell_venue: row.get(7)?,
                    buy_pool: row.get(8)?,
                    sell_pool: row.get(9)?,
                    buy_pool_kind: row.get(10)?,
                    sell_pool_kind: row.get(11)?,
                    buy_pool_owner: row.get(12)?,
                    sell_pool_owner: row.get(13)?,
                    buy_pool_liquidity_quote_ui: row.get(14)?,
                    sell_pool_liquidity_quote_ui: row.get(15)?,
                    buy_pool_price_quote_per_base: row.get(16)?,
                    sell_pool_price_quote_per_base: row.get(17)?,
                })
            },
        )?;
        let inputs = rows.collect::<std::result::Result<Vec<_>, _>>()?;

        inputs
            .into_iter()
            .map(|input| {
                let mut requirements = vec![
                    RouteAccountRequirement {
                        role: "buy.pool".to_string(),
                        pubkey: input.buy_pool.clone(),
                        expected_owner: input.buy_pool_owner.clone(),
                        required: true,
                    },
                    RouteAccountRequirement {
                        role: "sell.pool".to_string(),
                        pubkey: input.sell_pool.clone(),
                        expected_owner: input.sell_pool_owner.clone(),
                        required: true,
                    },
                ];
                requirements.extend(self.pool_account_requirements(&input.buy_pool, "buy")?);
                requirements.extend(self.pool_account_requirements(&input.sell_pool, "sell")?);
                Ok(RouteAccountFetchTarget {
                    route_edge_id: input.route_edge_id,
                    route_family_id: input.route_family_id,
                    base_mint: input.base_mint,
                    quote_mint: input.quote_mint,
                    base_decimals: input.base_decimals,
                    quote_decimals: input.quote_decimals,
                    buy_venue: input.buy_venue,
                    sell_venue: input.sell_venue,
                    buy_pool: input.buy_pool,
                    sell_pool: input.sell_pool,
                    buy_pool_kind: input.buy_pool_kind,
                    sell_pool_kind: input.sell_pool_kind,
                    buy_pool_liquidity_quote_ui: input.buy_pool_liquidity_quote_ui,
                    sell_pool_liquidity_quote_ui: input.sell_pool_liquidity_quote_ui,
                    buy_pool_price_quote_per_base: input.buy_pool_price_quote_per_base,
                    sell_pool_price_quote_per_base: input.sell_pool_price_quote_per_base,
                    requirements,
                })
            })
            .collect()
    }

    pub fn insert_route_account_observation(
        &self,
        record: &RouteAccountObservationRecord,
    ) -> Result<i64> {
        self.conn.execute(
            r#"
            INSERT INTO route_account_observations (
                route_edge_id, role, pubkey, outcome, reason, expected_owner, actual_owner,
                slot, slot_age, age_ms, observed_at_unix_ms, next_retry_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
            "#,
            params![
                record.route_edge_id,
                record.role,
                record.pubkey,
                record.outcome.as_str(),
                record.reason,
                record.expected_owner,
                record.actual_owner,
                optional_u64_i64(record.slot)?,
                optional_u64_i64(record.slot_age)?,
                optional_u128_i64(record.age_ms)?,
                unix_ms_i64(record.observed_at_unix_ms)?,
                optional_unix_ms_i64(record.next_retry_unix_ms)?,
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn cool_down_route_edge(
        &self,
        route_edge_id: i64,
        blocker: &str,
        cooldown_until_unix_ms: u128,
        now_unix_ms: u128,
    ) -> Result<usize> {
        self.conn
            .execute(
                r#"
                UPDATE route_edges
                SET readiness_state = ?1,
                    last_blocker = ?2,
                    cooldown_until_unix_ms = ?3,
                    updated_at_unix_ms = ?4
                WHERE id = ?5
                "#,
                params![
                    RouteReadinessState::CooledDown.as_str(),
                    blocker,
                    unix_ms_i64(cooldown_until_unix_ms)?,
                    unix_ms_i64(now_unix_ms)?,
                    route_edge_id,
                ],
            )
            .map_err(Into::into)
    }

    pub fn mark_route_accounts_ready(
        &self,
        route_edge_id: i64,
        now_unix_ms: u128,
    ) -> Result<usize> {
        self.conn
            .execute(
                r#"
                UPDATE route_edges
                SET readiness_state = ?1,
                    last_blocker = NULL,
                    cooldown_until_unix_ms = NULL,
                    updated_at_unix_ms = ?2
                WHERE id = ?3
                "#,
                params![
                    RouteReadinessState::FreshAccountsReady.as_str(),
                    unix_ms_i64(now_unix_ms)?,
                    route_edge_id,
                ],
            )
            .map_err(Into::into)
    }

    pub fn mark_route_quote_ready(&self, route_edge_id: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE route_edges SET readiness_state = ?1, updated_at_unix_ms = ?2 WHERE id = ?3",
            params![
                RouteReadinessState::FreshQuoteReady.as_str(),
                unix_ms_i64(system_now_unix_ms()?)?,
                route_edge_id,
            ],
        )?;
        Ok(())
    }

    pub fn mark_route_simulation_positive(&self, route_edge_id: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE route_edges SET readiness_state = ?1, last_blocker = NULL, updated_at_unix_ms = ?2 WHERE id = ?3",
            params![
                RouteReadinessState::SimulationPositive.as_str(),
                unix_ms_i64(system_now_unix_ms()?)?,
                route_edge_id,
            ],
        )?;
        Ok(())
    }

    pub fn mark_route_submit_eligible(&self, route_edge_id: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE route_edges SET readiness_state = ?1, last_blocker = NULL, updated_at_unix_ms = ?2 WHERE id = ?3",
            params![
                RouteReadinessState::SubmitEligible.as_str(),
                unix_ms_i64(system_now_unix_ms()?)?,
                route_edge_id,
            ],
        )?;
        Ok(())
    }

    pub fn mark_route_simulation_negative(&self, route_edge_id: i64, blocker: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE route_edges SET readiness_state = ?1, last_blocker = ?2, updated_at_unix_ms = ?3 WHERE id = ?4",
            params![
                RouteReadinessState::FreshAccountsReady.as_str(),
                blocker,
                unix_ms_i64(system_now_unix_ms()?)?,
                route_edge_id,
            ],
        )?;
        Ok(())
    }

    pub fn insert_quote_observation(&self, record: &QuoteObservationRecord) -> Result<i64> {
        self.conn.execute(
            r#"
            INSERT INTO quote_observations (
                route_edge_id, cycle, source, input_atoms, output_atoms, net_profit_atoms,
                net_profit_bps, fee_atoms, slot, freshness_json, blocker, observed_at_unix_ms, attempt_id
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            "#,
            params![
                record.route_edge_id,
                record.cycle,
                record.source,
                record.input_atoms,
                record.output_atoms,
                record.net_profit_atoms,
                record.net_profit_bps,
                record.fee_atoms,
                optional_u64_i64(record.slot)?,
                record.freshness_json,
                record.blocker,
                unix_ms_i64(record.observed_at_unix_ms)?,
                record.attempt_id,
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn optimizer_decision_evidence(
        &self,
        limit: usize,
    ) -> Result<Vec<OptimizerDecisionEvidenceRecord>> {
        let limit = i64::try_from(limit.max(1)).unwrap_or(i64::MAX);
        let mut stmt = self.conn.prepare(
            r#"
            SELECT id, route_edge_id, cycle, source, input_atoms, output_atoms,
                   net_profit_atoms, net_profit_bps, freshness_json, observed_at_unix_ms, attempt_id
            FROM quote_observations
            WHERE freshness_json LIKE '%"optimizer_decision"%'
            ORDER BY id DESC
            LIMIT ?1
            "#,
        )?;
        let rows = stmt.query_map([limit], |row| {
            Ok(OptimizerDecisionEvidenceRecord {
                id: row.get(0)?,
                route_edge_id: row.get(1)?,
                cycle: row.get(2)?,
                source: row.get(3)?,
                input_atoms: row.get(4)?,
                output_atoms: row.get(5)?,
                net_profit_atoms: row.get(6)?,
                net_profit_bps: row.get(7)?,
                freshness_json: row.get(8)?,
                observed_at_unix_ms: row.get::<_, i64>(9)? as u128,
                attempt_id: row.get(10)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn fresh_quote_route_scores(&self, limit: usize) -> Result<Vec<FreshQuoteRouteScore>> {
        let limit = i64::try_from(limit.max(1)).unwrap_or(i64::MAX);
        let mut stmt = self.conn.prepare(
            r#"
            SELECT
                re.id,
                re.route_family_id,
                rf.base_mint,
                rf.quote_mint,
                rf.buy_venue,
                rf.sell_venue,
                re.buy_pool,
                re.sell_pool,
                SUM(CASE WHEN qo.freshness_json LIKE '%"fresh_quote_status":"fresh_positive_complete"%' THEN 1 ELSE 0 END) AS fresh_positive_complete,
                SUM(CASE WHEN qo.freshness_json LIKE '%"fresh_quote_status":"fresh_positive_blocked"%' THEN 1 ELSE 0 END) AS fresh_positive_blocked,
                SUM(CASE WHEN qo.freshness_json LIKE '%"fresh_quote_status":"fresh_negative_complete"%' THEN 1 ELSE 0 END) AS fresh_negative_complete,
                SUM(CASE WHEN qo.freshness_json LIKE '%"fresh_quote_status":"fresh_quote_unavailable"%' THEN 1 ELSE 0 END) AS fresh_quote_unavailable,
                MAX(CASE WHEN qo.freshness_json LIKE '%"fresh_quote_status":"fresh_positive_complete"%'
                          OR qo.freshness_json LIKE '%"fresh_quote_status":"fresh_negative_complete"%'
                         THEN qo.net_profit_bps ELSE NULL END) AS best_complete_fresh_bps,
                MAX(qo.observed_at_unix_ms) AS latest_observed_at_unix_ms
            FROM route_edges re
            JOIN route_families rf ON rf.id = re.route_family_id
            JOIN quote_observations qo ON qo.route_edge_id = re.id
            WHERE qo.freshness_json LIKE '%"fresh_quote_reconciliation"%'
            GROUP BY re.id
            ORDER BY fresh_positive_complete DESC,
                     best_complete_fresh_bps DESC,
                     latest_observed_at_unix_ms DESC
            LIMIT ?1
            "#,
        )?;
        let rows = stmt.query_map([limit], |row| {
            Ok(FreshQuoteRouteScore {
                route_edge_id: row.get(0)?,
                route_family_id: row.get(1)?,
                base_mint: row.get(2)?,
                quote_mint: row.get(3)?,
                buy_venue: row.get(4)?,
                sell_venue: row.get(5)?,
                buy_pool: row.get(6)?,
                sell_pool: row.get(7)?,
                fresh_positive_complete: row.get(8)?,
                fresh_positive_blocked: row.get(9)?,
                fresh_negative_complete: row.get(10)?,
                fresh_quote_unavailable: row.get(11)?,
                best_complete_fresh_bps: row.get(12)?,
                latest_observed_at_unix_ms: row.get(13)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn fresh_quote_route_family_audit(
        &self,
        limit: usize,
    ) -> Result<Vec<FreshQuoteRouteFamilyAudit>> {
        let limit = i64::try_from(limit.max(1)).unwrap_or(i64::MAX);
        let mut stmt = self.conn.prepare(
            r#"
            WITH edge_scores AS (
                SELECT
                    re.id AS route_edge_id,
                    re.route_family_id,
                    SUM(CASE WHEN qo.freshness_json LIKE '%"fresh_quote_status":"fresh_positive_complete"%' THEN 1 ELSE 0 END) AS fresh_positive_complete,
                    SUM(CASE WHEN qo.freshness_json LIKE '%"fresh_quote_status":"fresh_positive_blocked"%' THEN 1 ELSE 0 END) AS fresh_positive_blocked,
                    SUM(CASE WHEN qo.freshness_json LIKE '%"fresh_quote_status":"fresh_negative_complete"%' THEN 1 ELSE 0 END) AS fresh_negative_complete,
                    SUM(CASE WHEN qo.freshness_json LIKE '%"fresh_quote_status":"fresh_quote_unavailable"%' THEN 1 ELSE 0 END) AS fresh_quote_unavailable,
                    MAX(CASE WHEN qo.freshness_json LIKE '%"fresh_quote_status":"fresh_positive_complete"%'
                              OR qo.freshness_json LIKE '%"fresh_quote_status":"fresh_negative_complete"%'
                             THEN qo.net_profit_bps ELSE NULL END) AS best_complete_fresh_bps,
                    MAX(qo.observed_at_unix_ms) AS latest_observed_at_unix_ms
                FROM route_edges re
                JOIN quote_observations qo ON qo.route_edge_id = re.id
                WHERE qo.freshness_json LIKE '%"fresh_quote_reconciliation"%'
                GROUP BY re.id
            ),
            family_scores AS (
                SELECT
                    rf.id,
                    rf.base_mint,
                    rf.quote_mint,
                    rf.buy_venue,
                    rf.sell_venue,
                    rf.enabled,
                    rf.priority,
                    COUNT(DISTINCT re.id) AS total_edges,
                    COUNT(DISTINCT es.route_edge_id) AS observed_edges,
                    COUNT(DISTINCT CASE WHEN es.fresh_positive_complete > 0 OR es.fresh_negative_complete > 0 THEN es.route_edge_id END) AS complete_fresh_edges,
                    COUNT(DISTINCT CASE WHEN es.fresh_positive_complete > 0 THEN es.route_edge_id END) AS positive_complete_edges,
                    COALESCE(SUM(es.fresh_positive_complete), 0) AS fresh_positive_complete,
                    COALESCE(SUM(es.fresh_positive_blocked), 0) AS fresh_positive_blocked,
                    COALESCE(SUM(es.fresh_negative_complete), 0) AS fresh_negative_complete,
                    COALESCE(SUM(es.fresh_quote_unavailable), 0) AS fresh_quote_unavailable,
                    MAX(es.best_complete_fresh_bps) AS best_complete_fresh_bps,
                    MAX(es.latest_observed_at_unix_ms) AS latest_observed_at_unix_ms
                FROM route_families rf
                JOIN route_edges re ON re.route_family_id = rf.id
                LEFT JOIN edge_scores es ON es.route_edge_id = re.id
                GROUP BY rf.id
            )
            SELECT
                id, base_mint, quote_mint, buy_venue, sell_venue, enabled, priority,
                total_edges, observed_edges, complete_fresh_edges, positive_complete_edges,
                fresh_positive_complete, fresh_positive_blocked, fresh_negative_complete,
                fresh_quote_unavailable, best_complete_fresh_bps, latest_observed_at_unix_ms
            FROM family_scores
            ORDER BY fresh_positive_complete DESC,
                     best_complete_fresh_bps DESC,
                     complete_fresh_edges DESC,
                     latest_observed_at_unix_ms DESC
            LIMIT ?1
            "#,
        )?;
        let rows = stmt.query_map([limit], |row| {
            let fresh_positive_complete: u64 = row.get(11)?;
            let fresh_positive_blocked: u64 = row.get(12)?;
            let fresh_negative_complete: u64 = row.get(13)?;
            let fresh_quote_unavailable: u64 = row.get(14)?;
            let total_fresh_observations = fresh_positive_complete
                .saturating_add(fresh_positive_blocked)
                .saturating_add(fresh_negative_complete)
                .saturating_add(fresh_quote_unavailable);
            let unavailable_ratio = if total_fresh_observations == 0 {
                1.0
            } else {
                fresh_quote_unavailable as f64 / total_fresh_observations as f64
            };
            let best_complete_fresh_bps = row.get(15)?;
            let (recommendation, reason) = fresh_route_family_recommendation(
                fresh_positive_complete,
                fresh_positive_blocked,
                fresh_negative_complete,
                fresh_quote_unavailable,
                best_complete_fresh_bps,
                unavailable_ratio,
            );
            Ok(FreshQuoteRouteFamilyAudit {
                route_family_id: row.get(0)?,
                base_mint: row.get(1)?,
                quote_mint: row.get(2)?,
                buy_venue: row.get(3)?,
                sell_venue: row.get(4)?,
                enabled: row.get::<_, i64>(5)? != 0,
                priority: row.get(6)?,
                total_edges: row.get(7)?,
                observed_edges: row.get(8)?,
                complete_fresh_edges: row.get(9)?,
                positive_complete_edges: row.get(10)?,
                fresh_positive_complete,
                fresh_positive_blocked,
                fresh_negative_complete,
                fresh_quote_unavailable,
                total_fresh_observations,
                best_complete_fresh_bps,
                latest_observed_at_unix_ms: row.get(16)?,
                unavailable_ratio,
                recommendation,
                reason,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn fresh_quote_blocked_positive_records(
        &self,
        limit: usize,
    ) -> Result<Vec<FreshQuoteBlockedPositiveRecord>> {
        let limit = i64::try_from(limit.max(1)).unwrap_or(i64::MAX);
        let mut stmt = self.conn.prepare(
            r#"
            SELECT
                qo.id,
                re.id,
                re.route_family_id,
                rf.base_mint,
                rf.quote_mint,
                rf.buy_venue,
                rf.sell_venue,
                re.buy_pool,
                re.sell_pool,
                qo.cycle,
                qo.source,
                qo.input_atoms,
                qo.output_atoms,
                qo.net_profit_atoms,
                qo.net_profit_bps,
                qo.blocker,
                qo.freshness_json,
                qo.observed_at_unix_ms,
                qo.attempt_id
            FROM quote_observations qo
            JOIN route_edges re ON re.id = qo.route_edge_id
            JOIN route_families rf ON rf.id = re.route_family_id
            WHERE qo.freshness_json LIKE '%"fresh_quote_status":"fresh_positive_blocked"%'
            ORDER BY qo.observed_at_unix_ms DESC, qo.id DESC
            LIMIT ?1
            "#,
        )?;
        let rows = stmt.query_map([limit], |row| {
            Ok(FreshQuoteBlockedPositiveRecord {
                quote_observation_id: row.get(0)?,
                route_edge_id: row.get(1)?,
                route_family_id: row.get(2)?,
                base_mint: row.get(3)?,
                quote_mint: row.get(4)?,
                buy_venue: row.get(5)?,
                sell_venue: row.get(6)?,
                buy_pool: row.get(7)?,
                sell_pool: row.get(8)?,
                cycle: row.get(9)?,
                source: row.get(10)?,
                input_atoms: row.get(11)?,
                output_atoms: row.get(12)?,
                net_profit_atoms: row.get(13)?,
                net_profit_bps: row.get(14)?,
                blocker: row.get(15)?,
                freshness_json: row.get(16)?,
                observed_at_unix_ms: row.get(17)?,
                attempt_id: row.get(18)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn route_family_promotion_decisions(
        &self,
        limit: usize,
    ) -> Result<Vec<RouteFamilyPromotionDecision>> {
        let mut decisions = self
            .fresh_quote_route_family_audit(limit)?
            .into_iter()
            .map(route_family_promotion_decision)
            .collect::<Vec<_>>();
        decisions.sort_by(|left, right| {
            promotion_action_rank(&left.action)
                .cmp(&promotion_action_rank(&right.action))
                .then_with(|| {
                    right
                        .score
                        .partial_cmp(&left.score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| {
                    right
                        .latest_observed_at_unix_ms
                        .cmp(&left.latest_observed_at_unix_ms)
                })
                .then_with(|| left.route_family_id.cmp(&right.route_family_id))
        });
        decisions.truncate(limit.max(1));
        Ok(decisions)
    }

    pub fn insert_simulation_result(&self, record: &SimulationResultRecord) -> Result<i64> {
        self.conn.execute(
            r#"
            INSERT INTO simulation_results (
                route_edge_id, cycle, input_atoms, pre_quote_atoms, post_quote_atoms,
                quote_delta_atoms, net_profit_atoms, net_profit_bps, tx_bytes,
                simulation_ok, simulation_error, logs_json, simulated_at_unix_ms, attempt_id
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
            "#,
            params![
                record.route_edge_id,
                record.cycle,
                record.input_atoms,
                record.pre_quote_atoms,
                record.post_quote_atoms,
                record.quote_delta_atoms,
                record.net_profit_atoms,
                record.net_profit_bps,
                optional_usize_i64(record.tx_bytes)?,
                bool_i64(record.simulation_ok),
                record.simulation_error,
                record.logs_json,
                unix_ms_i64(record.simulated_at_unix_ms)?,
                record.attempt_id,
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn insert_execution_attempt(&self, record: &ExecutionAttemptRecord) -> Result<i64> {
        self.conn.execute(
            r#"
            INSERT INTO execution_attempts (
                route_edge_id, cycle, status, tx_signature, input_atoms,
                estimated_profit_atoms, realized_profit_atoms, blocker, config_hash,
                wallet_pubkey, submitted_at_unix_ms, confirmed_at_unix_ms, attempt_id
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            "#,
            params![
                record.route_edge_id,
                record.cycle,
                record.status,
                record.tx_signature,
                record.input_atoms,
                record.estimated_profit_atoms,
                record.realized_profit_atoms,
                record.blocker,
                record.config_hash,
                record.wallet_pubkey,
                optional_unix_ms_i64(record.submitted_at_unix_ms)?,
                optional_unix_ms_i64(record.confirmed_at_unix_ms)?,
                record.attempt_id,
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn insert_live_settlement(&self, record: &LiveSettlementRecord) -> Result<i64> {
        self.conn.execute(
            r#"
            INSERT INTO live_settlements (
                route_edge_id, attempt_id, tx_signature, status, quote_mint,
                input_atoms, estimated_profit_atoms, realized_quote_delta_atoms,
                fee_lamports, fee_quote_atoms, realized_profit_atoms, classification,
                pre_balances_json, post_balances_json, meta_json, settled_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
            "#,
            params![
                record.route_edge_id,
                record.attempt_id,
                record.tx_signature,
                record.status,
                record.quote_mint,
                record.input_atoms,
                record.estimated_profit_atoms,
                record.realized_quote_delta_atoms,
                optional_u64_i64(record.fee_lamports)?,
                record.fee_quote_atoms,
                record.realized_profit_atoms,
                record.classification,
                record.pre_balances_json,
                record.post_balances_json,
                record.meta_json,
                unix_ms_i64(record.settled_at_unix_ms)?,
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn upsert_token(&self, record: &TokenGraphRecord) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO tokens (
                mint, symbol, decimals, token_program, created_at_unix_ms, updated_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?5)
            ON CONFLICT(mint) DO UPDATE SET
                symbol = excluded.symbol,
                decimals = excluded.decimals,
                token_program = excluded.token_program,
                updated_at_unix_ms = excluded.updated_at_unix_ms
            "#,
            params![
                record.mint,
                record.symbol,
                record.decimals,
                record.token_program,
                unix_ms_i64(record.now_unix_ms)?,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_pool(&self, record: &PoolGraphRecord) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO pools (
                pool_address, venue, pool_kind, program_owner, base_mint, quote_mint,
                base_decimals, quote_decimals, liquidity_quote_ui, price_quote_per_base,
                updated_slot, first_seen_unix_ms, last_seen_unix_ms, source
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?12, ?13)
            ON CONFLICT(pool_address) DO UPDATE SET
                venue = excluded.venue,
                pool_kind = excluded.pool_kind,
                program_owner = excluded.program_owner,
                base_mint = excluded.base_mint,
                quote_mint = excluded.quote_mint,
                base_decimals = excluded.base_decimals,
                quote_decimals = excluded.quote_decimals,
                liquidity_quote_ui = excluded.liquidity_quote_ui,
                price_quote_per_base = excluded.price_quote_per_base,
                updated_slot = excluded.updated_slot,
                last_seen_unix_ms = excluded.last_seen_unix_ms,
                source = excluded.source
            "#,
            params![
                record.pool_address,
                record.venue.to_string(),
                pool_kind_name(record.pool_kind),
                record.program_owner,
                record.pair.base_mint,
                record.pair.quote_mint,
                record.pair.base_decimals,
                record.pair.quote_decimals,
                record.liquidity_quote_ui,
                record.price_quote_per_base,
                u64_i64(record.updated_slot)?,
                unix_ms_i64(record.now_unix_ms)?,
                record.source,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_pool_account(&self, record: &PoolAccountGraphRecord) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO pool_accounts (
                pool_address, role, pubkey, owner, mint, token_program, derivation_kind,
                derivation_seed_json, required, first_seen_unix_ms, last_seen_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10)
            ON CONFLICT(pool_address, role, pubkey) DO UPDATE SET
                owner = excluded.owner,
                mint = excluded.mint,
                token_program = excluded.token_program,
                derivation_kind = excluded.derivation_kind,
                derivation_seed_json = excluded.derivation_seed_json,
                required = excluded.required,
                last_seen_unix_ms = excluded.last_seen_unix_ms
            "#,
            params![
                record.pool_address,
                record.role,
                record.pubkey,
                record.owner,
                record.mint,
                record.token_program,
                record.derivation_kind,
                record.derivation_seed_json,
                bool_i64(record.required),
                unix_ms_i64(record.now_unix_ms)?,
            ],
        )?;
        Ok(())
    }

    pub fn delete_pool_accounts_for_pool(&self, pool_address: &str) -> Result<usize> {
        self.conn
            .execute(
                "DELETE FROM pool_accounts WHERE pool_address = ?1",
                [pool_address],
            )
            .map_err(Into::into)
    }

    pub fn pool_addresses_missing_program_owner(&self, limit: usize) -> Result<Vec<String>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit_i64 = i64::try_from(limit).context("pool owner backfill limit exceeds i64")?;
        let mut stmt = self.conn.prepare(
            r#"
            SELECT pool_address
            FROM pools
            WHERE program_owner IS NULL OR program_owner = ''
            ORDER BY last_seen_unix_ms DESC, pool_address ASC
            LIMIT ?1
            "#,
        )?;
        let rows = stmt.query_map([limit_i64], |row| row.get(0))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn update_pool_program_owner(
        &self,
        pool_address: &str,
        program_owner: &str,
        now_unix_ms: u128,
    ) -> Result<usize> {
        self.conn
            .execute(
                r#"
                UPDATE pools
                SET program_owner = ?2,
                    last_seen_unix_ms = ?3
                WHERE pool_address = ?1
                "#,
                params![pool_address, program_owner, unix_ms_i64(now_unix_ms)?],
            )
            .map_err(Into::into)
    }

    pub fn upsert_route_family(&self, record: &RouteFamilyGraphRecord) -> Result<i64> {
        self.conn.execute(
            r#"
            INSERT INTO route_families (
                base_mint, quote_mint, buy_venue, sell_venue, enabled, priority,
                created_at_unix_ms, updated_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)
            ON CONFLICT(base_mint, quote_mint, buy_venue, sell_venue) DO UPDATE SET
                enabled = excluded.enabled,
                priority = excluded.priority,
                updated_at_unix_ms = excluded.updated_at_unix_ms
            "#,
            params![
                record.pair.base_mint,
                record.pair.quote_mint,
                record.buy_venue.to_string(),
                record.sell_venue.to_string(),
                bool_i64(record.enabled),
                record.priority,
                unix_ms_i64(record.now_unix_ms)?,
            ],
        )?;
        self.route_family_id(&record.pair, record.buy_venue, record.sell_venue)
    }

    pub fn upsert_route_edge(&self, record: &RouteEdgeGraphRecord) -> Result<i64> {
        self.conn.execute(
            r#"
            INSERT INTO route_edges (
                route_family_id, buy_pool, sell_pool, buy_pool_kind, sell_pool_kind,
                readiness_state, last_blocker, cooldown_until_unix_ms, updated_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ON CONFLICT(route_family_id, buy_pool, sell_pool) DO UPDATE SET
                buy_pool_kind = excluded.buy_pool_kind,
                sell_pool_kind = excluded.sell_pool_kind,
                readiness_state = excluded.readiness_state,
                last_blocker = excluded.last_blocker,
                cooldown_until_unix_ms = excluded.cooldown_until_unix_ms,
                updated_at_unix_ms = excluded.updated_at_unix_ms
            "#,
            params![
                record.route_family_id,
                record.buy_pool,
                record.sell_pool,
                pool_kind_name(record.buy_pool_kind),
                pool_kind_name(record.sell_pool_kind),
                record.readiness_state.as_str(),
                record.last_blocker,
                optional_unix_ms_i64(record.cooldown_until_unix_ms)?,
                unix_ms_i64(record.now_unix_ms)?,
            ],
        )?;
        self.route_edge_id(record.route_family_id, &record.buy_pool, &record.sell_pool)
    }

    #[allow(dead_code)]
    pub fn insert_account_manifest(&self, record: &AccountManifestGraphRecord) -> Result<i64> {
        self.conn.execute(
            r#"
            INSERT INTO account_manifests (
                route_edge_id, manifest_version, complete, missing_roles_json,
                account_roles_json, refreshed_slot, refreshed_at_unix_ms, decode_status
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#,
            params![
                record.route_edge_id,
                record.manifest_version,
                bool_i64(record.complete),
                record.missing_roles_json,
                record.account_roles_json,
                optional_u64_i64(record.refreshed_slot)?,
                unix_ms_i64(record.refreshed_at_unix_ms)?,
                record.decode_status,
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn route_family_id(
        &self,
        pair: &TokenPair,
        buy_venue: Venue,
        sell_venue: Venue,
    ) -> Result<i64> {
        self.conn
            .query_row(
                r#"
                SELECT id FROM route_families
                WHERE base_mint = ?1 AND quote_mint = ?2 AND buy_venue = ?3 AND sell_venue = ?4
                "#,
                params![
                    pair.base_mint,
                    pair.quote_mint,
                    buy_venue.to_string(),
                    sell_venue.to_string(),
                ],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    pub fn route_edge_id(
        &self,
        route_family_id: i64,
        buy_pool: &str,
        sell_pool: &str,
    ) -> Result<i64> {
        self.conn
            .query_row(
                r#"
                SELECT id FROM route_edges
                WHERE route_family_id = ?1 AND buy_pool = ?2 AND sell_pool = ?3
                "#,
                params![route_family_id, buy_pool, sell_pool],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    pub fn route_edge_id_for_pools(
        &self,
        pair: &TokenPair,
        buy_venue: Venue,
        sell_venue: Venue,
        buy_pool: &str,
        sell_pool: &str,
    ) -> Result<Option<i64>> {
        self.conn
            .query_row(
                r#"
                SELECT re.id
                FROM route_edges re
                JOIN route_families rf ON rf.id = re.route_family_id
                WHERE rf.base_mint = ?1
                    AND rf.quote_mint = ?2
                    AND rf.buy_venue = ?3
                    AND rf.sell_venue = ?4
                    AND re.buy_pool = ?5
                    AND re.sell_pool = ?6
                "#,
                params![
                    pair.base_mint,
                    pair.quote_mint,
                    buy_venue.to_string(),
                    sell_venue.to_string(),
                    buy_pool,
                    sell_pool,
                ],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn route_edge_readiness_for_pools(
        &self,
        pair: &TokenPair,
        buy_venue: Venue,
        sell_venue: Venue,
        buy_pool: &str,
        sell_pool: &str,
    ) -> Result<Option<RouteReadinessState>> {
        let Some(route_edge_id) =
            self.route_edge_id_for_pools(pair, buy_venue, sell_venue, buy_pool, sell_pool)?
        else {
            return Ok(None);
        };
        self.route_edge_readiness(route_edge_id).map(Some)
    }

    pub fn route_subscription_pubkeys(&self, route_edge_id: i64) -> Result<Vec<String>> {
        let (buy_pool, sell_pool): (String, String) = self.conn.query_row(
            "SELECT buy_pool, sell_pool FROM route_edges WHERE id = ?1",
            [route_edge_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let mut seen = BTreeSet::new();
        let mut pubkeys = Vec::new();
        for pubkey in [&buy_pool, &sell_pool] {
            if seen.insert(pubkey.clone()) {
                pubkeys.push(pubkey.clone());
            }
        }
        let mut manifest_pubkeys = BTreeSet::new();
        for roles in self.pool_account_roles(&buy_pool)?.values() {
            for pubkey in roles {
                manifest_pubkeys.insert(pubkey.clone());
            }
        }
        for roles in self.pool_account_roles(&sell_pool)?.values() {
            for pubkey in roles {
                manifest_pubkeys.insert(pubkey.clone());
            }
        }
        for pubkey in manifest_pubkeys {
            if seen.insert(pubkey.clone()) {
                pubkeys.push(pubkey);
            }
        }
        Ok(pubkeys)
    }

    #[cfg(test)]
    fn row_count(&self, table: &str) -> Result<u64> {
        self.count_table(table)
    }

    #[cfg(test)]
    pub(crate) fn conn_for_test(&self) -> &Connection {
        &self.conn
    }

    fn count_table(&self, table: &str) -> Result<u64> {
        let table = allowed_count_table(table)?;
        self.conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get::<_, u64>(0)
            })
            .map_err(Into::into)
    }

    fn count_fresh_quote_status(&self, status: &str) -> Result<u64> {
        self.conn
            .query_row(
                r#"
                SELECT COUNT(*)
                FROM quote_observations
                WHERE freshness_json LIKE ?1
                "#,
                [format!(r#"%"fresh_quote_status":"{status}"%"#)],
                |row| row.get::<_, u64>(0),
            )
            .map_err(Into::into)
    }

    fn count_positive_simulation_results(&self) -> Result<u64> {
        self.conn
            .query_row(
                r#"
                SELECT COUNT(*)
                FROM simulation_results
                WHERE simulation_ok != 0
                  AND COALESCE(net_profit_bps, 0) > 0
                "#,
                [],
                |row| row.get::<_, u64>(0),
            )
            .map_err(Into::into)
    }

    fn count_positive_simulations_with_fresh_positive_complete(&self) -> Result<u64> {
        self.conn
            .query_row(
                r#"
                SELECT COUNT(*)
                FROM simulation_results sim
                WHERE sim.simulation_ok != 0
                  AND COALESCE(sim.net_profit_bps, 0) > 0
                  AND sim.route_edge_id IS NOT NULL
                  AND EXISTS (
                      SELECT 1
                      FROM quote_observations quote
                      WHERE quote.route_edge_id = sim.route_edge_id
                        AND quote.freshness_json LIKE '%"fresh_quote_status":"fresh_positive_complete"%'
                  )
                "#,
                [],
                |row| row.get::<_, u64>(0),
            )
            .map_err(Into::into)
    }

    fn count_live_settlement_classification(&self, classification: &str) -> Result<u64> {
        self.conn
            .query_row(
                r#"
                SELECT COUNT(*)
                FROM live_settlements
                WHERE classification = ?1
                "#,
                [classification],
                |row| row.get::<_, u64>(0),
            )
            .map_err(Into::into)
    }

    fn route_readiness_counts(&self, route_family_id: i64) -> Result<Vec<RouteReadinessCount>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT readiness_state, COUNT(*)
            FROM route_edges
            WHERE route_family_id = ?1
            GROUP BY readiness_state
            ORDER BY COUNT(*) DESC, readiness_state ASC
            "#,
        )?;
        let rows = stmt.query_map([route_family_id], |row| {
            Ok(RouteReadinessCount {
                readiness_state: row.get(0)?,
                count: row.get(1)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn route_blocker_summaries(&self, route_family_id: i64) -> Result<Vec<RouteBlockerSummary>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT readiness_state, last_blocker, COUNT(*), MAX(updated_at_unix_ms)
            FROM route_edges
            WHERE route_family_id = ?1
              AND last_blocker IS NOT NULL
              AND TRIM(last_blocker) <> ''
            GROUP BY readiness_state, last_blocker
            ORDER BY MAX(updated_at_unix_ms) DESC, COUNT(*) DESC
            LIMIT 10
            "#,
        )?;
        let rows = stmt.query_map([route_family_id], |row| {
            Ok(RouteBlockerSummary {
                readiness_state: row.get(0)?,
                blocker: row.get(1)?,
                count: row.get(2)?,
                latest_updated_at_unix_ms: row.get(3)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn route_account_blockers(&self, limit: usize) -> Result<Vec<RouteAccountBlockerSummary>> {
        let limit_i64 = i64::try_from(limit).context("account blocker limit exceeds i64")?;
        let mut stmt = self.conn.prepare(
            r#"
            SELECT
                latest.route_edge_id,
                re.route_family_id,
                rf.base_mint,
                rf.quote_mint,
                rf.buy_venue,
                rf.sell_venue,
                latest.role,
                latest.pubkey,
                latest.outcome,
                latest.reason,
                latest.observed_at_unix_ms,
                latest.next_retry_unix_ms
            FROM route_account_observations latest
            JOIN route_edges re ON re.id = latest.route_edge_id
            JOIN route_families rf ON rf.id = re.route_family_id
            WHERE latest.id IN (
                SELECT MAX(id)
                FROM route_account_observations
                GROUP BY route_edge_id, role, pubkey
            )
              AND latest.outcome IN ('missing', 'owner_mismatch', 'stale')
              AND latest.observed_at_unix_ms >= re.updated_at_unix_ms
              AND COALESCE((
                    SELECT pa.required
                    FROM pool_accounts pa
                    WHERE pa.pool_address = CASE
                        WHEN latest.role LIKE 'buy.%' THEN re.buy_pool
                        WHEN latest.role LIKE 'sell.%' THEN re.sell_pool
                        ELSE NULL
                    END
                      AND pa.role = SUBSTR(latest.role, INSTR(latest.role, '.') + 1)
                      AND pa.pubkey = latest.pubkey
                    LIMIT 1
              ), 1) != 0
              AND NOT (
                    latest.outcome = 'missing'
                    AND latest.reason LIKE 'missing optional account %'
              )
            ORDER BY latest.observed_at_unix_ms DESC, latest.id DESC
            LIMIT ?1
            "#,
        )?;
        let rows = stmt.query_map([limit_i64], |row| {
            Ok(RouteAccountBlockerSummary {
                route_edge_id: row.get(0)?,
                route_family_id: row.get(1)?,
                base_mint: row.get(2)?,
                quote_mint: row.get(3)?,
                buy_venue: row.get(4)?,
                sell_venue: row.get(5)?,
                role: row.get(6)?,
                pubkey: row.get(7)?,
                outcome: row.get(8)?,
                reason: row.get(9)?,
                observed_at_unix_ms: row.get(10)?,
                next_retry_unix_ms: row.get(11)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn pool_account_roles(&self, pool_address: &str) -> Result<BTreeMap<String, Vec<String>>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT role, pubkey
            FROM pool_accounts
            WHERE pool_address = ?1
              AND required != 0
            ORDER BY role ASC, pubkey ASC
            "#,
        )?;
        let rows = stmt.query_map([pool_address], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut roles = BTreeMap::<String, Vec<String>>::new();
        for row in rows {
            let (role, pubkey) = row?;
            if !route_subscription_role_is_fetchable(&role) {
                continue;
            }
            roles.entry(role).or_default().push(pubkey);
        }
        Ok(roles)
    }

    fn pool_account_requirements(
        &self,
        pool_address: &str,
        leg: &str,
    ) -> Result<Vec<RouteAccountRequirement>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT role, pubkey, owner, required
            FROM pool_accounts
            WHERE pool_address = ?1
            ORDER BY role ASC, pubkey ASC
            "#,
        )?;
        let rows = stmt.query_map([pool_address], |row| {
            let role: String = row.get(0)?;
            Ok(RouteAccountRequirement {
                role: format!("{leg}.{role}"),
                pubkey: row.get(1)?,
                expected_owner: row.get(2)?,
                required: row.get::<_, i64>(3)? != 0,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn route_edge_readiness(&self, route_edge_id: i64) -> Result<RouteReadinessState> {
        let value: String = self.conn.query_row(
            "SELECT readiness_state FROM route_edges WHERE id = ?1",
            [route_edge_id],
            |row| row.get(0),
        )?;
        RouteReadinessState::parse(&value)
            .ok_or_else(|| anyhow!("unknown route readiness state {value}"))
    }

    #[cfg(test)]
    fn latest_manifest_decode_status(&self, route_edge_id: i64) -> Result<String> {
        self.conn
            .query_row(
                r#"
                SELECT decode_status FROM account_manifests
                WHERE route_edge_id = ?1
                ORDER BY id DESC
                LIMIT 1
                "#,
                [route_edge_id],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    #[cfg(test)]
    fn latest_quote_observation(
        &self,
        route_edge_id: i64,
    ) -> Result<(
        String,
        String,
        Option<String>,
        Option<String>,
        Option<f64>,
        Option<String>,
    )> {
        self.conn
            .query_row(
                r#"
                SELECT source, input_atoms, output_atoms, net_profit_atoms, net_profit_bps, blocker
                FROM quote_observations
                WHERE route_edge_id = ?1
                ORDER BY id DESC
                LIMIT 1
                "#,
                [route_edge_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .map_err(Into::into)
    }

    #[cfg(test)]
    pub(crate) fn latest_simulation_result(
        &self,
        route_edge_id: i64,
    ) -> Result<(
        String,
        Option<String>,
        Option<String>,
        Option<f64>,
        bool,
        Option<String>,
    )> {
        self.conn
            .query_row(
                r#"
                SELECT input_atoms, quote_delta_atoms, net_profit_atoms, net_profit_bps,
                       simulation_ok, simulation_error
                FROM simulation_results
                WHERE route_edge_id = ?1
                ORDER BY id DESC
                LIMIT 1
                "#,
                [route_edge_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get::<_, i64>(4)? != 0,
                        row.get(5)?,
                    ))
                },
            )
            .map_err(Into::into)
    }

    #[cfg(test)]
    pub(crate) fn latest_execution_attempt(
        &self,
        route_edge_id: i64,
    ) -> Result<(String, Option<String>, Option<String>, Option<String>)> {
        self.conn
            .query_row(
                r#"
                SELECT status, input_atoms, estimated_profit_atoms, blocker
                FROM execution_attempts
                WHERE route_edge_id = ?1
                ORDER BY id DESC
                LIMIT 1
                "#,
                [route_edge_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .map_err(Into::into)
    }

    #[cfg(test)]
    pub(crate) fn latest_quote_cycle(&self, route_edge_id: i64) -> Result<i64> {
        self.conn
            .query_row(
                r#"
                SELECT cycle
                FROM quote_observations
                WHERE route_edge_id = ?1
                ORDER BY id DESC
                LIMIT 1
                "#,
                [route_edge_id],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    #[cfg(test)]
    fn table_names(&self) -> Result<std::collections::HashSet<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
        )?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect::<std::result::Result<std::collections::HashSet<_>, _>>()
            .map_err(Into::into)
    }

    #[cfg(test)]
    fn applied_migration_count(&self) -> Result<u64> {
        let count = self
            .conn
            .query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
                row.get::<_, u64>(0)
            })?;
        Ok(count)
    }
}

#[cfg(test)]
pub fn canonical_route_key(
    pair: &TokenPair,
    buy_venue: Venue,
    sell_venue: Venue,
    buy_pool: &str,
    sell_pool: &str,
) -> String {
    format!(
        "{}/{}|{}->{}|{}->{}",
        pair.base_mint, pair.quote_mint, buy_venue, sell_venue, buy_pool, sell_pool
    )
}

fn pool_kind_name(kind: PoolKind) -> &'static str {
    match kind {
        PoolKind::ConstantProduct => "constant_product",
        PoolKind::ConcentratedLiquidity => "concentrated_liquidity",
        PoolKind::Dlmm => "dlmm",
        PoolKind::RaydiumAmmV4 => "raydium_amm_v4",
        PoolKind::MeteoraPools => "meteora_pools",
        PoolKind::MeteoraDammV2 => "meteora_damm_v2",
    }
}

fn bool_i64(value: bool) -> i64 {
    if value {
        1
    } else {
        0
    }
}

fn unix_ms_i64(value: u128) -> Result<i64> {
    value
        .try_into()
        .map_err(|_| anyhow!("unix ms value exceeds i64: {value}"))
}

fn optional_unix_ms_i64(value: Option<u128>) -> Result<Option<i64>> {
    value.map(unix_ms_i64).transpose()
}

fn optional_u128_i64(value: Option<u128>) -> Result<Option<i64>> {
    value.map(unix_ms_i64).transpose()
}

fn u64_i64(value: u64) -> Result<i64> {
    value
        .try_into()
        .map_err(|_| anyhow!("u64 value exceeds i64: {value}"))
}

fn usize_i64(value: usize) -> Result<i64> {
    value
        .try_into()
        .map_err(|_| anyhow!("usize value exceeds i64: {value}"))
}

#[allow(dead_code)]
fn optional_u64_i64(value: Option<u64>) -> Result<Option<i64>> {
    value.map(u64_i64).transpose()
}

fn optional_usize_i64(value: Option<usize>) -> Result<Option<i64>> {
    value.map(usize_i64).transpose()
}

fn system_now_unix_ms() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_millis())
}

fn allowed_count_table(table: &str) -> Result<&'static str> {
    match table {
        "schema_migrations" => Ok("schema_migrations"),
        "tokens" => Ok("tokens"),
        "pools" => Ok("pools"),
        "pool_accounts" => Ok("pool_accounts"),
        "route_families" => Ok("route_families"),
        "route_edges" => Ok("route_edges"),
        "account_manifests" => Ok("account_manifests"),
        "route_account_observations" => Ok("route_account_observations"),
        "quote_observations" => Ok("quote_observations"),
        "simulation_results" => Ok("simulation_results"),
        "execution_attempts" => Ok("execution_attempts"),
        "live_settlements" => Ok("live_settlements"),
        "route_cooldowns" => Ok("route_cooldowns"),
        other => Err(anyhow!("unsupported table count {other}")),
    }
}

fn fresh_route_family_recommendation(
    fresh_positive_complete: u64,
    fresh_positive_blocked: u64,
    fresh_negative_complete: u64,
    fresh_quote_unavailable: u64,
    best_complete_fresh_bps: Option<f64>,
    unavailable_ratio: f64,
) -> (String, String) {
    if fresh_positive_complete > 0 {
        return (
            "ready_for_no_submit_audit".to_string(),
            format!(
                "has {fresh_positive_complete} complete fresh-positive observation(s); verify frequency and sizing in a capped no-submit audit"
            ),
        );
    }
    if fresh_positive_blocked > 0 {
        return (
            "fix_blocked_positive_fresh_quote".to_string(),
            format!(
                "has {fresh_positive_blocked} positive-but-blocked fresh observation(s); resolve blocker before live submit"
            ),
        );
    }
    if fresh_negative_complete > 0 {
        let best_bps = best_complete_fresh_bps.unwrap_or(0.0);
        if best_bps >= -100.0 {
            return (
                "watch_only".to_string(),
                format!(
                    "complete fresh quotes exist but are not positive; best complete fresh bps is {best_bps:.2}"
                ),
            );
        }
        return (
            "reject_for_now".to_string(),
            format!(
                "complete fresh quotes are materially negative; best complete fresh bps is {best_bps:.2}"
            ),
        );
    }
    if fresh_quote_unavailable > 0 || unavailable_ratio >= 0.5 {
        return (
            "needs_quote_support".to_string(),
            format!(
                "fresh quote evidence is unavailable for {:.1}% of observations",
                unavailable_ratio * 100.0
            ),
        );
    }
    (
        "needs_evidence".to_string(),
        "no fresh quote reconciliation evidence yet".to_string(),
    )
}

fn route_family_promotion_decision(
    audit: FreshQuoteRouteFamilyAudit,
) -> RouteFamilyPromotionDecision {
    let positive_score = audit.fresh_positive_complete as f64 * 1_000.0
        + audit.positive_complete_edges as f64 * 250.0
        + audit.best_complete_fresh_bps.unwrap_or_default().max(0.0) * 10.0;
    let evidence_score = audit.complete_fresh_edges as f64 * 20.0 + audit.observed_edges as f64;
    let penalty = audit.fresh_positive_blocked as f64 * 100.0
        + audit.fresh_quote_unavailable as f64 * 5.0
        + audit.unavailable_ratio * 100.0
        + audit.fresh_negative_complete as f64 * 0.1;
    let score = positive_score + evidence_score - penalty;

    let (action, hot_path_eligible, reason) = match audit.recommendation.as_str() {
        "ready_for_no_submit_audit" => (
            "hot_candidate".to_string(),
            true,
            format!(
                "{}; promote into the hot no-submit path before any canary submit",
                audit.reason
            ),
        ),
        "fix_blocked_positive_fresh_quote" => (
            "repair_candidate".to_string(),
            false,
            format!(
                "{}; keep watching but do not submit until the execution blocker is fixed",
                audit.reason
            ),
        ),
        "watch_only" => (
            "watch_only".to_string(),
            false,
            format!(
                "{}; keep low-priority evidence collection active",
                audit.reason
            ),
        ),
        "reject_for_now" => (
            "reject_for_now".to_string(),
            false,
            format!(
                "{}; remove from hot execution budget until market changes",
                audit.reason
            ),
        ),
        "needs_quote_support" => (
            "needs_quote_support".to_string(),
            false,
            format!(
                "{}; route graph or quote adapter support is incomplete",
                audit.reason
            ),
        ),
        _ => (
            "needs_evidence".to_string(),
            false,
            format!(
                "{}; collect fresh quote/account evidence first",
                audit.reason
            ),
        ),
    };

    RouteFamilyPromotionDecision {
        route_family_id: audit.route_family_id,
        base_mint: audit.base_mint,
        quote_mint: audit.quote_mint,
        buy_venue: audit.buy_venue,
        sell_venue: audit.sell_venue,
        action,
        score,
        hot_path_eligible,
        reason,
        total_edges: audit.total_edges,
        observed_edges: audit.observed_edges,
        complete_fresh_edges: audit.complete_fresh_edges,
        positive_complete_edges: audit.positive_complete_edges,
        fresh_positive_complete: audit.fresh_positive_complete,
        fresh_positive_blocked: audit.fresh_positive_blocked,
        fresh_negative_complete: audit.fresh_negative_complete,
        fresh_quote_unavailable: audit.fresh_quote_unavailable,
        best_complete_fresh_bps: audit.best_complete_fresh_bps,
        latest_observed_at_unix_ms: audit.latest_observed_at_unix_ms,
    }
}

fn promotion_action_rank(action: &str) -> u8 {
    match action {
        "hot_candidate" => 0,
        "repair_candidate" => 1,
        "watch_only" => 2,
        "needs_quote_support" => 3,
        "needs_evidence" => 4,
        "reject_for_now" => 5,
        _ => 6,
    }
}

struct RouteEdgeManifestInput {
    id: i64,
    buy_pool: String,
    sell_pool: String,
    buy_pool_kind: String,
    sell_pool_kind: String,
}

struct RouteAccountFetchTargetInput {
    route_edge_id: i64,
    route_family_id: i64,
    base_mint: String,
    quote_mint: String,
    base_decimals: u8,
    quote_decimals: u8,
    buy_venue: String,
    sell_venue: String,
    buy_pool: String,
    sell_pool: String,
    buy_pool_kind: String,
    sell_pool_kind: String,
    buy_pool_owner: Option<String>,
    sell_pool_owner: Option<String>,
    buy_pool_liquidity_quote_ui: Option<f64>,
    sell_pool_liquidity_quote_ui: Option<f64>,
    buy_pool_price_quote_per_base: Option<f64>,
    sell_pool_price_quote_per_base: Option<f64>,
}

fn collect_leg_manifest(
    leg: &str,
    pool_kind: &str,
    accounts: &BTreeMap<String, Vec<String>>,
    account_roles: &mut BTreeMap<String, Vec<String>>,
    missing_roles: &mut Vec<String>,
) {
    for (role, pubkeys) in accounts {
        account_roles.insert(format!("{leg}.{role}"), pubkeys.clone());
    }

    let requirements = required_manifest_roles(pool_kind);
    for &role in requirements.exact_roles {
        if !accounts.contains_key(role) {
            missing_roles.push(format!("{leg}.{role}"));
        }
    }
    for &prefix in requirements.prefix_roles {
        if !accounts.keys().any(|role| role.starts_with(prefix)) {
            missing_roles.push(format!("{leg}.{prefix}*"));
        }
    }
    let mut unique = BTreeSet::new();
    missing_roles.retain(|role| unique.insert(role.clone()));
}

struct ManifestRoleRequirements {
    exact_roles: &'static [&'static str],
    prefix_roles: &'static [&'static str],
}

fn required_manifest_roles(pool_kind: &str) -> ManifestRoleRequirements {
    match pool_kind {
        "dlmm" => ManifestRoleRequirements {
            exact_roles: &[
                "meteora_reserve_x",
                "meteora_reserve_y",
                "meteora_token_x_mint",
                "meteora_token_y_mint",
                "meteora_oracle",
            ],
            prefix_roles: &["meteora_bin_array_"],
        },
        "constant_product" => ManifestRoleRequirements {
            exact_roles: &[
                "raydium_amm_config",
                "raydium_token_0_vault",
                "raydium_token_1_vault",
                "raydium_token_0_mint",
                "raydium_token_1_mint",
            ],
            prefix_roles: &[],
        },
        "raydium_amm_v4" => ManifestRoleRequirements {
            exact_roles: &[
                "raydium_amm_coin_vault",
                "raydium_amm_pc_vault",
                "raydium_amm_coin_mint",
                "raydium_amm_pc_mint",
                "raydium_amm_open_orders",
                "raydium_amm_market",
                "raydium_amm_serum_dex",
                "raydium_amm_target_orders",
                "raydium_amm_serum_coin_vault",
                "raydium_amm_serum_pc_vault",
                "raydium_amm_serum_event_queue",
                "raydium_amm_serum_bids",
                "raydium_amm_serum_asks",
                "raydium_amm_serum_vault_signer",
            ],
            prefix_roles: &[],
        },
        "concentrated_liquidity" => ManifestRoleRequirements {
            exact_roles: &[
                "raydium_amm_config",
                "raydium_token_0_vault",
                "raydium_token_1_vault",
                "raydium_token_0_mint",
                "raydium_token_1_mint",
                "raydium_observation",
                "raydium_bitmap_extension",
            ],
            prefix_roles: &["raydium_tick_array_"],
        },
        "meteora_pools" => ManifestRoleRequirements {
            exact_roles: &[
                "meteora_pool_token_a_mint",
                "meteora_pool_token_b_mint",
                "meteora_pool_a_vault",
                "meteora_pool_b_vault",
                "meteora_pool_a_vault_lp",
                "meteora_pool_b_vault_lp",
                "meteora_pool_a_token_vault",
                "meteora_pool_b_token_vault",
                "meteora_pool_a_vault_lp_mint",
                "meteora_pool_b_vault_lp_mint",
            ],
            prefix_roles: &[],
        },
        "meteora_damm_v2" => ManifestRoleRequirements {
            exact_roles: &[
                "meteora_damm_v2_token_a_mint",
                "meteora_damm_v2_token_b_mint",
                "meteora_damm_v2_token_a_vault",
                "meteora_damm_v2_token_b_vault",
            ],
            prefix_roles: &[],
        },
        _ => ManifestRoleRequirements {
            exact_roles: &["unknown_pool_kind_manifest"],
            prefix_roles: &[],
        },
    }
}

fn route_subscription_role_is_fetchable(role: &str) -> bool {
    !role.ends_with("_signer")
}

const INITIAL_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS schema_migrations (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    checksum TEXT NOT NULL,
    applied_at_unix_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tokens (
    mint TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    decimals INTEGER NOT NULL,
    token_program TEXT,
    created_at_unix_ms INTEGER NOT NULL,
    updated_at_unix_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS pools (
    pool_address TEXT PRIMARY KEY,
    venue TEXT NOT NULL,
    pool_kind TEXT NOT NULL,
    program_owner TEXT,
    base_mint TEXT NOT NULL,
    quote_mint TEXT NOT NULL,
    base_decimals INTEGER NOT NULL,
    quote_decimals INTEGER NOT NULL,
    liquidity_quote_ui REAL,
    price_quote_per_base REAL,
    updated_slot INTEGER NOT NULL DEFAULT 0,
    first_seen_unix_ms INTEGER NOT NULL,
    last_seen_unix_ms INTEGER NOT NULL,
    source TEXT NOT NULL,
    FOREIGN KEY(base_mint) REFERENCES tokens(mint),
    FOREIGN KEY(quote_mint) REFERENCES tokens(mint)
);

CREATE TABLE IF NOT EXISTS pool_accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pool_address TEXT NOT NULL,
    role TEXT NOT NULL,
    pubkey TEXT NOT NULL,
    owner TEXT,
    mint TEXT,
    token_program TEXT,
    derivation_kind TEXT,
    derivation_seed_json TEXT,
    required INTEGER NOT NULL DEFAULT 1,
    first_seen_unix_ms INTEGER NOT NULL,
    last_seen_unix_ms INTEGER NOT NULL,
    UNIQUE(pool_address, role, pubkey),
    FOREIGN KEY(pool_address) REFERENCES pools(pool_address)
);

CREATE TABLE IF NOT EXISTS route_families (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    base_mint TEXT NOT NULL,
    quote_mint TEXT NOT NULL,
    buy_venue TEXT NOT NULL,
    sell_venue TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    priority INTEGER NOT NULL DEFAULT 0,
    created_at_unix_ms INTEGER NOT NULL,
    updated_at_unix_ms INTEGER NOT NULL,
    UNIQUE(base_mint, quote_mint, buy_venue, sell_venue)
);

CREATE TABLE IF NOT EXISTS route_edges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    route_family_id INTEGER NOT NULL,
    buy_pool TEXT NOT NULL,
    sell_pool TEXT NOT NULL,
    buy_pool_kind TEXT NOT NULL,
    sell_pool_kind TEXT NOT NULL,
    readiness_state TEXT NOT NULL,
    last_blocker TEXT,
    cooldown_until_unix_ms INTEGER,
    updated_at_unix_ms INTEGER NOT NULL,
    UNIQUE(route_family_id, buy_pool, sell_pool),
    FOREIGN KEY(route_family_id) REFERENCES route_families(id),
    FOREIGN KEY(buy_pool) REFERENCES pools(pool_address),
    FOREIGN KEY(sell_pool) REFERENCES pools(pool_address)
);

CREATE TABLE IF NOT EXISTS account_manifests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    route_edge_id INTEGER NOT NULL,
    manifest_version INTEGER NOT NULL,
    complete INTEGER NOT NULL,
    missing_roles_json TEXT NOT NULL,
    account_roles_json TEXT NOT NULL,
    refreshed_slot INTEGER,
    refreshed_at_unix_ms INTEGER NOT NULL,
    decode_status TEXT NOT NULL,
    FOREIGN KEY(route_edge_id) REFERENCES route_edges(id)
);

CREATE TABLE IF NOT EXISTS route_account_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    route_edge_id INTEGER NOT NULL,
    role TEXT NOT NULL,
    pubkey TEXT NOT NULL,
    outcome TEXT NOT NULL,
    reason TEXT NOT NULL,
    expected_owner TEXT,
    actual_owner TEXT,
    slot INTEGER,
    slot_age INTEGER,
    age_ms INTEGER,
    observed_at_unix_ms INTEGER NOT NULL,
    next_retry_unix_ms INTEGER,
    FOREIGN KEY(route_edge_id) REFERENCES route_edges(id)
);

CREATE TABLE IF NOT EXISTS quote_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    route_edge_id INTEGER,
    cycle INTEGER NOT NULL,
    source TEXT NOT NULL,
    input_atoms TEXT NOT NULL,
    output_atoms TEXT,
    net_profit_atoms TEXT,
    net_profit_bps REAL,
    fee_atoms TEXT,
    slot INTEGER,
    freshness_json TEXT NOT NULL,
    blocker TEXT,
    observed_at_unix_ms INTEGER NOT NULL,
    FOREIGN KEY(route_edge_id) REFERENCES route_edges(id)
);

CREATE TABLE IF NOT EXISTS simulation_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    route_edge_id INTEGER,
    cycle INTEGER NOT NULL,
    input_atoms TEXT NOT NULL,
    pre_quote_atoms TEXT,
    post_quote_atoms TEXT,
    quote_delta_atoms TEXT,
    net_profit_atoms TEXT,
    net_profit_bps REAL,
    tx_bytes INTEGER,
    simulation_ok INTEGER NOT NULL,
    simulation_error TEXT,
    logs_json TEXT NOT NULL,
    simulated_at_unix_ms INTEGER NOT NULL,
    FOREIGN KEY(route_edge_id) REFERENCES route_edges(id)
);

CREATE TABLE IF NOT EXISTS execution_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    route_edge_id INTEGER,
    cycle INTEGER NOT NULL,
    status TEXT NOT NULL,
    tx_signature TEXT,
    input_atoms TEXT,
    estimated_profit_atoms TEXT,
    realized_profit_atoms TEXT,
    blocker TEXT,
    config_hash TEXT NOT NULL,
    wallet_pubkey TEXT,
    submitted_at_unix_ms INTEGER,
    confirmed_at_unix_ms INTEGER,
    FOREIGN KEY(route_edge_id) REFERENCES route_edges(id)
);

CREATE TABLE IF NOT EXISTS live_settlements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    route_edge_id INTEGER,
    attempt_id TEXT,
    tx_signature TEXT NOT NULL,
    status TEXT NOT NULL,
    quote_mint TEXT NOT NULL,
    input_atoms TEXT,
    estimated_profit_atoms TEXT,
    realized_quote_delta_atoms TEXT,
    fee_lamports INTEGER,
    fee_quote_atoms TEXT,
    realized_profit_atoms TEXT,
    classification TEXT NOT NULL,
    pre_balances_json TEXT NOT NULL,
    post_balances_json TEXT NOT NULL,
    meta_json TEXT NOT NULL,
    settled_at_unix_ms INTEGER NOT NULL,
    FOREIGN KEY(route_edge_id) REFERENCES route_edges(id)
);

CREATE TABLE IF NOT EXISTS route_cooldowns (
    route_key TEXT PRIMARY KEY,
    reason TEXT NOT NULL,
    until_unix_ms INTEGER NOT NULL,
    count INTEGER NOT NULL,
    updated_at_unix_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pools_pair_venue_kind
    ON pools(base_mint, quote_mint, venue, pool_kind);
CREATE INDEX IF NOT EXISTS idx_pool_accounts_pool_role
    ON pool_accounts(pool_address, role);
CREATE INDEX IF NOT EXISTS idx_route_families_pair_venues
    ON route_families(base_mint, quote_mint, buy_venue, sell_venue);
CREATE INDEX IF NOT EXISTS idx_route_edges_family_state
    ON route_edges(route_family_id, readiness_state);
CREATE INDEX IF NOT EXISTS idx_route_account_observations_route_time
    ON route_account_observations(route_edge_id, observed_at_unix_ms);
CREATE INDEX IF NOT EXISTS idx_route_account_observations_outcome
    ON route_account_observations(outcome, next_retry_unix_ms);
CREATE INDEX IF NOT EXISTS idx_quote_observations_route_time
    ON quote_observations(route_edge_id, observed_at_unix_ms);
CREATE INDEX IF NOT EXISTS idx_simulation_results_route_time
    ON simulation_results(route_edge_id, simulated_at_unix_ms);
CREATE INDEX IF NOT EXISTS idx_execution_attempts_route_time
    ON execution_attempts(route_edge_id, submitted_at_unix_ms);
CREATE INDEX IF NOT EXISTS idx_live_settlements_signature
    ON live_settlements(tx_signature);
CREATE INDEX IF NOT EXISTS idx_live_settlements_classification
    ON live_settlements(classification, settled_at_unix_ms);
"#;

const ATTEMPT_CHAIN_MIGRATION: &[&str] = &[
    "ALTER TABLE quote_observations ADD COLUMN attempt_id TEXT",
    "ALTER TABLE simulation_results ADD COLUMN attempt_id TEXT",
    "ALTER TABLE execution_attempts ADD COLUMN attempt_id TEXT",
    r#"
    CREATE TABLE IF NOT EXISTS live_settlements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        route_edge_id INTEGER,
        attempt_id TEXT,
        tx_signature TEXT NOT NULL,
        status TEXT NOT NULL,
        quote_mint TEXT NOT NULL,
        input_atoms TEXT,
        estimated_profit_atoms TEXT,
        realized_quote_delta_atoms TEXT,
        fee_lamports INTEGER,
        fee_quote_atoms TEXT,
        realized_profit_atoms TEXT,
        classification TEXT NOT NULL,
        pre_balances_json TEXT NOT NULL,
        post_balances_json TEXT NOT NULL,
        meta_json TEXT NOT NULL,
        settled_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(route_edge_id) REFERENCES route_edges(id)
    )
    "#,
    "CREATE INDEX IF NOT EXISTS idx_live_settlements_signature ON live_settlements(tx_signature)",
    "CREATE INDEX IF NOT EXISTS idx_live_settlements_classification ON live_settlements(classification, settled_at_unix_ms)",
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{AccountFetchPlan, PoolKind, RouteReadinessState, TokenPair, Venue};

    #[test]
    fn migrations_create_core_route_graph_tables_idempotently() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");

        db.run_migrations().expect("first migration run");
        db.run_migrations().expect("second migration run");

        let tables = db.table_names().expect("table names");
        for table in [
            "schema_migrations",
            "tokens",
            "pools",
            "pool_accounts",
            "route_families",
            "route_edges",
            "account_manifests",
            "route_account_observations",
            "quote_observations",
            "simulation_results",
            "execution_attempts",
            "route_cooldowns",
        ] {
            assert!(tables.contains(table), "missing table {table}");
        }

        assert_eq!(db.applied_migration_count().expect("migration count"), 1);
    }

    #[test]
    fn open_read_only_does_not_create_or_write_database() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let db_path = std::env::temp_dir().join(format!(
            "arb-bot-read-only-db-{}-{unique}.sqlite",
            std::process::id()
        ));
        let wal_path = db_path.with_extension("sqlite-wal");
        let shm_path = db_path.with_extension("sqlite-shm");

        {
            let db = ArbDatabase::open(&db_path, 5_000).expect("open writable db");
            db.run_migrations().expect("migrations");
        }

        let read_only = ArbDatabase::open_read_only(&db_path, 5_000).expect("open read-only db");
        assert_eq!(
            read_only
                .row_count("quote_observations")
                .expect("quote observation count"),
            0
        );
        let write_result = read_only.insert_quote_observation(&QuoteObservationRecord {
            route_edge_id: None,
            cycle: 1,
            source: "read-only-test".to_string(),
            input_atoms: "1".to_string(),
            output_atoms: None,
            net_profit_atoms: None,
            net_profit_bps: None,
            fee_atoms: None,
            slot: None,
            freshness_json: "{}".to_string(),
            blocker: None,
            observed_at_unix_ms: 1,
            attempt_id: None,
        });
        assert!(write_result.is_err());

        drop(read_only);
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(&wal_path);
        let _ = std::fs::remove_file(&shm_path);
    }

    #[test]
    fn route_graph_upserts_tokens_pools_families_and_edges_idempotently() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");

        let pair = TokenPair {
            base_mint: "WIFMint111111111111111111111111111111111111".to_string(),
            quote_mint: "So11111111111111111111111111111111111111112".to_string(),
            base_decimals: 6,
            quote_decimals: 9,
        };
        db.upsert_token(&TokenGraphRecord {
            mint: pair.base_mint.clone(),
            symbol: "WIF".to_string(),
            decimals: pair.base_decimals,
            token_program: Some("Tokenkeg1111111111111111111111111111111111".to_string()),
            now_unix_ms: 1_000,
        })
        .expect("insert base token");
        db.upsert_token(&TokenGraphRecord {
            mint: pair.base_mint.clone(),
            symbol: "dogwifhat".to_string(),
            decimals: pair.base_decimals,
            token_program: Some("Tokenkeg1111111111111111111111111111111111".to_string()),
            now_unix_ms: 2_000,
        })
        .expect("update base token");
        db.upsert_token(&TokenGraphRecord {
            mint: pair.quote_mint.clone(),
            symbol: "WSOL".to_string(),
            decimals: pair.quote_decimals,
            token_program: Some("Tokenkeg1111111111111111111111111111111111".to_string()),
            now_unix_ms: 1_000,
        })
        .expect("insert quote token");

        db.upsert_pool(&PoolGraphRecord {
            pool_address: "MeteoraPool111111111111111111111111111111".to_string(),
            venue: Venue::Meteora,
            pool_kind: PoolKind::Dlmm,
            program_owner: Some("MeteoraProgram111111111111111111111111111".to_string()),
            pair: pair.clone(),
            liquidity_quote_ui: Some(100.0),
            price_quote_per_base: Some(0.01),
            updated_slot: 10,
            source: "test".to_string(),
            now_unix_ms: 1_000,
        })
        .expect("insert buy pool");
        db.upsert_pool(&PoolGraphRecord {
            pool_address: "RaydiumPool111111111111111111111111111111".to_string(),
            venue: Venue::Raydium,
            pool_kind: PoolKind::ConstantProduct,
            program_owner: Some("RaydiumProgram111111111111111111111111111".to_string()),
            pair: pair.clone(),
            liquidity_quote_ui: Some(110.0),
            price_quote_per_base: Some(0.011),
            updated_slot: 11,
            source: "test".to_string(),
            now_unix_ms: 1_000,
        })
        .expect("insert sell pool");

        let family_id = db
            .upsert_route_family(&RouteFamilyGraphRecord {
                pair: pair.clone(),
                buy_venue: Venue::Meteora,
                sell_venue: Venue::Raydium,
                enabled: true,
                priority: 10,
                now_unix_ms: 2_000,
            })
            .expect("insert family");
        let same_family_id = db
            .upsert_route_family(&RouteFamilyGraphRecord {
                pair: pair.clone(),
                buy_venue: Venue::Meteora,
                sell_venue: Venue::Raydium,
                enabled: true,
                priority: 20,
                now_unix_ms: 3_000,
            })
            .expect("update family");

        let edge_id = db
            .upsert_route_edge(&RouteEdgeGraphRecord {
                route_family_id: family_id,
                buy_pool: "MeteoraPool111111111111111111111111111111".to_string(),
                sell_pool: "RaydiumPool111111111111111111111111111111".to_string(),
                buy_pool_kind: PoolKind::Dlmm,
                sell_pool_kind: PoolKind::ConstantProduct,
                readiness_state: RouteReadinessState::PriceOnly,
                last_blocker: Some("indexed pool only".to_string()),
                cooldown_until_unix_ms: None,
                now_unix_ms: 3_000,
            })
            .expect("insert edge");
        let same_edge_id = db
            .upsert_route_edge(&RouteEdgeGraphRecord {
                route_family_id: family_id,
                buy_pool: "MeteoraPool111111111111111111111111111111".to_string(),
                sell_pool: "RaydiumPool111111111111111111111111111111".to_string(),
                buy_pool_kind: PoolKind::Dlmm,
                sell_pool_kind: PoolKind::ConstantProduct,
                readiness_state: RouteReadinessState::GraphIncomplete,
                last_blocker: Some("missing meteora active_bin_array".to_string()),
                cooldown_until_unix_ms: None,
                now_unix_ms: 4_000,
            })
            .expect("update edge");

        assert_eq!(family_id, same_family_id);
        assert_eq!(edge_id, same_edge_id);
        assert_eq!(db.row_count("tokens").expect("token count"), 2);
        assert_eq!(db.row_count("pools").expect("pool count"), 2);
        assert_eq!(db.row_count("route_families").expect("family count"), 1);
        assert_eq!(db.row_count("route_edges").expect("edge count"), 1);
        assert_eq!(
            db.route_edge_readiness(edge_id).expect("edge readiness"),
            RouteReadinessState::GraphIncomplete
        );
        assert_eq!(
            canonical_route_key(
                &pair,
                Venue::Meteora,
                Venue::Raydium,
                "MeteoraPool111111111111111111111111111111",
                "RaydiumPool111111111111111111111111111111",
            ),
            "WIFMint111111111111111111111111111111111111/So11111111111111111111111111111111111111112|meteora->raydium|MeteoraPool111111111111111111111111111111->RaydiumPool111111111111111111111111111111"
        );
    }

    #[test]
    fn summary_reports_route_graph_counts() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");

        let summary = db.summary().expect("summary");

        assert_eq!(summary.tokens, 0);
        assert_eq!(summary.pools, 0);
        assert_eq!(summary.route_families, 0);
        assert_eq!(summary.route_edges, 0);
        assert_eq!(summary.account_manifests, 0);
        assert_eq!(summary.route_account_observations, 0);
        assert_eq!(summary.quote_observations, 0);
        assert_eq!(summary.simulation_results, 0);
        assert_eq!(summary.execution_attempts, 0);
    }

    #[test]
    fn pool_accounts_and_account_manifests_are_persisted_idempotently() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (pair, family_id, edge_id) = seed_route_edge(&db);

        db.upsert_pool_account(&PoolAccountGraphRecord {
            pool_address: "RaydiumPool111111111111111111111111111111".to_string(),
            role: "raydium_vault_0".to_string(),
            pubkey: "Vault011111111111111111111111111111111111".to_string(),
            owner: Some("Tokenkeg1111111111111111111111111111111111".to_string()),
            mint: Some(pair.base_mint.clone()),
            token_program: Some("Tokenkeg1111111111111111111111111111111111".to_string()),
            derivation_kind: None,
            derivation_seed_json: None,
            required: true,
            now_unix_ms: 5_000,
        })
        .expect("insert pool account");
        db.upsert_pool_account(&PoolAccountGraphRecord {
            pool_address: "RaydiumPool111111111111111111111111111111".to_string(),
            role: "raydium_vault_0".to_string(),
            pubkey: "Vault011111111111111111111111111111111111".to_string(),
            owner: Some("Tokenkeg1111111111111111111111111111111111".to_string()),
            mint: Some(pair.base_mint.clone()),
            token_program: Some("Tokenkeg1111111111111111111111111111111111".to_string()),
            derivation_kind: None,
            derivation_seed_json: None,
            required: true,
            now_unix_ms: 6_000,
        })
        .expect("update pool account");

        db.insert_account_manifest(&AccountManifestGraphRecord {
            route_edge_id: edge_id,
            manifest_version: 1,
            complete: false,
            missing_roles_json: "[\"raydium_amm_config\"]".to_string(),
            account_roles_json:
                "{\"raydium_vault_0\":\"Vault011111111111111111111111111111111111\"}".to_string(),
            refreshed_slot: Some(99),
            refreshed_at_unix_ms: 6_000,
            decode_status: "missing required accounts".to_string(),
        })
        .expect("insert manifest");

        assert_eq!(family_id, 1);
        assert_eq!(
            db.row_count("pool_accounts").expect("pool account count"),
            1
        );
        assert_eq!(
            db.row_count("account_manifests").expect("manifest count"),
            1
        );
        assert_eq!(
            db.latest_manifest_decode_status(edge_id)
                .expect("manifest status"),
            "missing required accounts"
        );
    }

    #[test]
    fn pool_accounts_can_be_cleared_when_pool_layout_is_no_longer_supported() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        seed_route_edge(&db);

        for role in ["raydium_amm_config", "raydium_token_0_vault"] {
            db.upsert_pool_account(&PoolAccountGraphRecord {
                pool_address: "RaydiumPool111111111111111111111111111111".to_string(),
                role: role.to_string(),
                pubkey: format!("{role}111111111111111111111111111111"),
                owner: None,
                mint: None,
                token_program: None,
                derivation_kind: None,
                derivation_seed_json: None,
                required: true,
                now_unix_ms: 5_000,
            })
            .expect("insert stale pool account");
        }
        assert_eq!(
            db.row_count("pool_accounts").expect("pool account count"),
            2
        );

        let deleted = db
            .delete_pool_accounts_for_pool("RaydiumPool111111111111111111111111111111")
            .expect("delete stale pool accounts");

        assert_eq!(deleted, 2);
        assert_eq!(
            db.row_count("pool_accounts").expect("pool account count"),
            0
        );
    }

    #[test]
    fn missing_pool_program_owners_can_be_listed_and_backfilled() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let pair = TokenPair {
            base_mint: "WIFMint111111111111111111111111111111111111".to_string(),
            quote_mint: "So11111111111111111111111111111111111111112".to_string(),
            base_decimals: 6,
            quote_decimals: 9,
        };
        for (mint, symbol, decimals) in [
            (pair.base_mint.clone(), "WIF", pair.base_decimals),
            (pair.quote_mint.clone(), "WSOL", pair.quote_decimals),
        ] {
            db.upsert_token(&TokenGraphRecord {
                mint,
                symbol: symbol.to_string(),
                decimals,
                token_program: None,
                now_unix_ms: 1_000,
            })
            .expect("token");
        }
        db.upsert_pool(&PoolGraphRecord {
            pool_address: "PoolMissingOwner1111111111111111111111111".to_string(),
            venue: Venue::Meteora,
            pool_kind: PoolKind::Dlmm,
            program_owner: None,
            pair,
            liquidity_quote_ui: Some(1.0),
            price_quote_per_base: Some(1.0),
            updated_slot: 1,
            source: "test".to_string(),
            now_unix_ms: 1_000,
        })
        .expect("pool");

        assert_eq!(
            db.pool_addresses_missing_program_owner(10)
                .expect("missing owners"),
            vec!["PoolMissingOwner1111111111111111111111111".to_string()]
        );

        let updated = db
            .update_pool_program_owner(
                "PoolMissingOwner1111111111111111111111111",
                "Owner111111111111111111111111111111111111111",
                2_000,
            )
            .expect("backfill owner");

        assert_eq!(updated, 1);
        assert!(db
            .pool_addresses_missing_program_owner(10)
            .expect("missing owners")
            .is_empty());
    }

    #[test]
    fn route_status_report_groups_families_readiness_and_blockers() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (_pair, family_id, edge_id) = seed_route_edge(&db);

        db.upsert_route_edge(&RouteEdgeGraphRecord {
            route_family_id: family_id,
            buy_pool: "MeteoraPool111111111111111111111111111111".to_string(),
            sell_pool: "RaydiumPool111111111111111111111111111111".to_string(),
            buy_pool_kind: PoolKind::Dlmm,
            sell_pool_kind: PoolKind::ConstantProduct,
            readiness_state: RouteReadinessState::GraphIncomplete,
            last_blocker: Some("missing meteora active_bin_array".to_string()),
            cooldown_until_unix_ms: None,
            now_unix_ms: 2_000,
        })
        .expect("update blocker");
        db.insert_account_manifest(&AccountManifestGraphRecord {
            route_edge_id: edge_id,
            manifest_version: 1,
            complete: false,
            missing_roles_json: "[\"meteora_active_bin_array\"]".to_string(),
            account_roles_json: "{}".to_string(),
            refreshed_slot: Some(42),
            refreshed_at_unix_ms: 2_000,
            decode_status: "missing meteora active_bin_array".to_string(),
        })
        .expect("insert manifest");

        let report = db.route_status_report().expect("route status report");

        assert_eq!(report.summary.route_families, 1);
        assert_eq!(report.summary.route_edges, 1);
        assert_eq!(report.summary.account_manifests, 1);
        assert_eq!(report.families.len(), 1);

        let family = &report.families[0];
        assert_eq!(family.buy_venue, "meteora");
        assert_eq!(family.sell_venue, "raydium");
        assert_eq!(family.enabled, true);
        assert_eq!(family.total_edges, 1);
        assert_eq!(
            family.readiness_counts,
            vec![RouteReadinessCount {
                readiness_state: "graph incomplete".to_string(),
                count: 1,
            }]
        );
        assert_eq!(
            family.latest_blockers,
            vec![RouteBlockerSummary {
                readiness_state: "graph incomplete".to_string(),
                blocker: "missing meteora active_bin_array".to_string(),
                count: 1,
                latest_updated_at_unix_ms: 2_000,
            }]
        );
    }

    #[test]
    fn materializes_complete_route_manifest_from_pool_account_roles() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (_pair, _family_id, edge_id) = seed_route_edge(&db);

        for (pool_address, role) in [
            (
                "MeteoraPool111111111111111111111111111111",
                "meteora_reserve_x",
            ),
            (
                "MeteoraPool111111111111111111111111111111",
                "meteora_reserve_y",
            ),
            (
                "MeteoraPool111111111111111111111111111111",
                "meteora_token_x_mint",
            ),
            (
                "MeteoraPool111111111111111111111111111111",
                "meteora_token_y_mint",
            ),
            (
                "MeteoraPool111111111111111111111111111111",
                "meteora_oracle",
            ),
            (
                "MeteoraPool111111111111111111111111111111",
                "meteora_bin_array_0",
            ),
            (
                "RaydiumPool111111111111111111111111111111",
                "raydium_amm_config",
            ),
            (
                "RaydiumPool111111111111111111111111111111",
                "raydium_token_0_vault",
            ),
            (
                "RaydiumPool111111111111111111111111111111",
                "raydium_token_1_vault",
            ),
            (
                "RaydiumPool111111111111111111111111111111",
                "raydium_token_0_mint",
            ),
            (
                "RaydiumPool111111111111111111111111111111",
                "raydium_token_1_mint",
            ),
        ] {
            db.upsert_pool_account(&PoolAccountGraphRecord {
                pool_address: pool_address.to_string(),
                role: role.to_string(),
                pubkey: format!("{role}111111111111111111111111111111"),
                owner: None,
                mint: None,
                token_program: None,
                derivation_kind: None,
                derivation_seed_json: None,
                required: true,
                now_unix_ms: 2_000,
            })
            .expect("pool account");
        }

        let summary = db
            .materialize_route_manifests(3_000)
            .expect("materialize manifests");

        assert_eq!(summary.routes_scanned, 1);
        assert_eq!(summary.complete_manifests, 1);
        assert_eq!(summary.incomplete_manifests, 0);
        assert_eq!(
            db.row_count("account_manifests").expect("manifest count"),
            1
        );
        assert_eq!(
            db.route_edge_readiness(edge_id).expect("route readiness"),
            RouteReadinessState::GraphComplete
        );
        assert_eq!(
            db.latest_manifest_decode_status(edge_id)
                .expect("manifest status"),
            "complete"
        );
    }

    #[test]
    fn complete_route_manifest_builds_account_fetch_target() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (_pair, _family_id, _edge_id) = seed_route_edge(&db);

        for (pool_address, role, pubkey, owner) in [
            (
                "MeteoraPool111111111111111111111111111111",
                "meteora_reserve_x",
                "SharedVault1111111111111111111111111111111",
                Some("Tokenkeg1111111111111111111111111111111111"),
            ),
            (
                "MeteoraPool111111111111111111111111111111",
                "meteora_reserve_y",
                "MeteoraReserveY111111111111111111111111111",
                Some("Tokenkeg1111111111111111111111111111111111"),
            ),
            (
                "MeteoraPool111111111111111111111111111111",
                "meteora_token_x_mint",
                "MeteoraTokenX1111111111111111111111111111",
                None,
            ),
            (
                "MeteoraPool111111111111111111111111111111",
                "meteora_token_y_mint",
                "MeteoraTokenY1111111111111111111111111111",
                None,
            ),
            (
                "MeteoraPool111111111111111111111111111111",
                "meteora_oracle",
                "MeteoraOracle11111111111111111111111111111",
                Some("MeteoraProgram111111111111111111111111111"),
            ),
            (
                "MeteoraPool111111111111111111111111111111",
                "meteora_bin_array_0",
                "MeteoraBinArray11111111111111111111111111",
                Some("MeteoraProgram111111111111111111111111111"),
            ),
            (
                "RaydiumPool111111111111111111111111111111",
                "raydium_amm_config",
                "RaydiumAmmConfig111111111111111111111111",
                None,
            ),
            (
                "RaydiumPool111111111111111111111111111111",
                "raydium_token_0_vault",
                "SharedVault1111111111111111111111111111111",
                Some("Tokenkeg1111111111111111111111111111111111"),
            ),
            (
                "RaydiumPool111111111111111111111111111111",
                "raydium_token_1_vault",
                "RaydiumVault11111111111111111111111111111",
                Some("Tokenkeg1111111111111111111111111111111111"),
            ),
            (
                "RaydiumPool111111111111111111111111111111",
                "raydium_token_0_mint",
                "RaydiumMint011111111111111111111111111111",
                None,
            ),
            (
                "RaydiumPool111111111111111111111111111111",
                "raydium_token_1_mint",
                "RaydiumMint111111111111111111111111111111",
                None,
            ),
        ] {
            db.upsert_pool_account(&PoolAccountGraphRecord {
                pool_address: pool_address.to_string(),
                role: role.to_string(),
                pubkey: pubkey.to_string(),
                owner: owner.map(ToOwned::to_owned),
                mint: None,
                token_program: owner.map(ToOwned::to_owned),
                derivation_kind: None,
                derivation_seed_json: None,
                required: true,
                now_unix_ms: 2_000,
            })
            .expect("pool account");
        }

        db.materialize_route_manifests(3_000)
            .expect("materialize manifests");

        let targets = db
            .complete_route_account_fetch_targets(10)
            .expect("fetch targets");

        assert_eq!(targets.len(), 1);
        let target = &targets[0];
        assert_eq!(target.route_edge_id, 1);
        assert_eq!(target.requirements.len(), 13);
        assert!(target.requirements.iter().any(|requirement| {
            requirement.role == "buy.pool"
                && requirement.pubkey == "MeteoraPool111111111111111111111111111111"
        }));
        assert!(target.requirements.iter().any(|requirement| {
            requirement.role == "sell.pool"
                && requirement.pubkey == "RaydiumPool111111111111111111111111111111"
        }));
        assert!(target.requirements.iter().any(|requirement| {
            requirement.role == "buy.meteora_reserve_x"
                && requirement.pubkey == "SharedVault1111111111111111111111111111111"
                && requirement.expected_owner.as_deref()
                    == Some("Tokenkeg1111111111111111111111111111111111")
        }));
        assert!(target.requirements.iter().any(|requirement| {
            requirement.role == "sell.raydium_token_0_vault"
                && requirement.pubkey == "SharedVault1111111111111111111111111111111"
        }));

        let plan = AccountFetchPlan::from_requirements(&target.requirements);

        assert_eq!(plan.pubkeys.len(), 12);
        assert_eq!(
            plan.roles_for_pubkey("SharedVault1111111111111111111111111111111"),
            vec![
                "buy.meteora_reserve_x".to_string(),
                "sell.raydium_token_0_vault".to_string()
            ]
        );
    }

    #[test]
    fn route_account_fetch_targets_include_optional_accounts_without_requiring_them() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (_pair, _family_id, _edge_id) = seed_route_edge(&db);
        seed_complete_route_accounts(&db);
        db.upsert_pool_account(&PoolAccountGraphRecord {
            pool_address: "MeteoraPool111111111111111111111111111111".to_string(),
            role: "meteora_bin_array_-1".to_string(),
            pubkey: "OptionalMeteoraBinArray11111111111111111111".to_string(),
            owner: Some("MeteoraProgram111111111111111111111111111".to_string()),
            mint: None,
            token_program: Some("MeteoraProgram111111111111111111111111111".to_string()),
            derivation_kind: Some("pda".to_string()),
            derivation_seed_json: None,
            required: false,
            now_unix_ms: 2_000,
        })
        .expect("optional pool account");

        db.materialize_route_manifests(3_000)
            .expect("materialize manifests");

        let targets = db
            .complete_route_account_fetch_targets(10)
            .expect("fetch targets");
        let optional = targets[0]
            .requirements
            .iter()
            .find(|requirement| requirement.role == "buy.meteora_bin_array_-1")
            .expect("optional adjacent bin array should be fetched when present");

        assert!(!optional.required);
        assert_eq!(
            optional.pubkey,
            "OptionalMeteoraBinArray11111111111111111111"
        );
        assert_eq!(
            db.route_edge_readiness(1).expect("route readiness"),
            RouteReadinessState::GraphComplete
        );
    }

    #[test]
    fn route_status_omits_missing_optional_accounts_from_blockers() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (_pair, _family_id, edge_id) = seed_route_edge(&db);
        db.upsert_pool_account(&PoolAccountGraphRecord {
            pool_address: "MeteoraPool111111111111111111111111111111".to_string(),
            role: "meteora_bin_array_0".to_string(),
            pubkey: "OptionalMeteoraBinArray11111111111111111111".to_string(),
            owner: Some("MeteoraProgram111111111111111111111111111".to_string()),
            mint: None,
            token_program: Some("MeteoraProgram111111111111111111111111111".to_string()),
            derivation_kind: Some("pda".to_string()),
            derivation_seed_json: None,
            required: false,
            now_unix_ms: 3_000,
        })
        .expect("optional pool account");

        db.insert_route_account_observation(&RouteAccountObservationRecord {
            route_edge_id: edge_id,
            role: "buy.meteora_bin_array_0".to_string(),
            pubkey: "OptionalMeteoraBinArray11111111111111111111".to_string(),
            outcome: RouteAccountObservationOutcome::Missing,
            reason: "missing derived account buy.meteora_bin_array_0 OptionalMeteoraBinArray11111111111111111111".to_string(),
            expected_owner: Some("MeteoraProgram111111111111111111111111111".to_string()),
            actual_owner: None,
            slot: None,
            slot_age: None,
            age_ms: None,
            observed_at_unix_ms: 4_000,
            next_retry_unix_ms: None,
        })
        .expect("insert optional missing observation");

        let report = db.route_status_report().expect("route status report");

        assert!(report.account_blockers.is_empty());
    }

    #[test]
    fn route_account_observations_cool_down_missing_derived_accounts() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (_pair, _family_id, edge_id) = seed_route_edge(&db);
        seed_complete_route_accounts(&db);

        db.materialize_route_manifests(3_000)
            .expect("materialize manifests");
        assert_eq!(
            db.complete_route_account_fetch_targets_at(10, 3_500)
                .expect("targets before cooldown")
                .len(),
            1
        );

        db.insert_route_account_observation(&RouteAccountObservationRecord {
            route_edge_id: edge_id,
            role: "buy.meteora_bin_array_0".to_string(),
            pubkey: "MeteoraBinArray11111111111111111111111111".to_string(),
            outcome: RouteAccountObservationOutcome::Missing,
            reason: "derived account missing on-chain".to_string(),
            expected_owner: Some("MeteoraProgram111111111111111111111111111".to_string()),
            actual_owner: None,
            slot: None,
            slot_age: None,
            age_ms: None,
            observed_at_unix_ms: 4_000,
            next_retry_unix_ms: Some(64_000),
        })
        .expect("insert route account observation");
        db.cool_down_route_edge(
            edge_id,
            "missing derived account buy.meteora_bin_array_0 MeteoraBinArray11111111111111111111111111",
            64_000,
            4_000,
        )
        .expect("cool down route");

        assert_eq!(
            db.row_count("route_account_observations")
                .expect("observation count"),
            1
        );
        assert_eq!(
            db.route_edge_readiness(edge_id).expect("route readiness"),
            RouteReadinessState::CooledDown
        );
        assert!(db
            .complete_route_account_fetch_targets_at(10, 4_001)
            .expect("targets during cooldown")
            .is_empty());
        assert_eq!(
            db.complete_route_account_fetch_targets_at(10, 64_000)
                .expect("targets after cooldown")
                .len(),
            1
        );

        let report = db.route_status_report().expect("route status report");
        assert_eq!(report.account_blockers.len(), 1);
        assert_eq!(report.account_blockers[0].route_edge_id, edge_id);
        assert_eq!(report.account_blockers[0].role, "buy.meteora_bin_array_0");
        assert_eq!(report.account_blockers[0].next_retry_unix_ms, Some(64_000));

        db.insert_route_account_observation(&RouteAccountObservationRecord {
            route_edge_id: edge_id,
            role: "buy.meteora_bin_array_0".to_string(),
            pubkey: "MeteoraBinArray11111111111111111111111111".to_string(),
            outcome: RouteAccountObservationOutcome::Fetched,
            reason:
                "fresh account buy.meteora_bin_array_0 MeteoraBinArray11111111111111111111111111"
                    .to_string(),
            expected_owner: Some("MeteoraProgram111111111111111111111111111".to_string()),
            actual_owner: Some("MeteoraProgram111111111111111111111111111".to_string()),
            slot: Some(42),
            slot_age: Some(0),
            age_ms: Some(0),
            observed_at_unix_ms: 65_000,
            next_retry_unix_ms: None,
        })
        .expect("insert recovered observation");
        assert!(db
            .route_status_report()
            .expect("route status after recovery")
            .account_blockers
            .is_empty());
    }

    #[test]
    fn quote_observations_are_persisted_for_route_evidence() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (_pair, _family_id, edge_id) = seed_route_edge(&db);

        db.insert_quote_observation(&QuoteObservationRecord {
            route_edge_id: Some(edge_id),
            cycle: 42,
            source: "route-quote:native".to_string(),
            input_atoms: "1000000".to_string(),
            output_atoms: Some("1010000".to_string()),
            net_profit_atoms: Some("7500".to_string()),
            net_profit_bps: Some(75.0),
            fee_atoms: Some("2500".to_string()),
            slot: Some(123),
            freshness_json: "{\"fresh\":true}".to_string(),
            blocker: None,
            observed_at_unix_ms: 9_000,
            attempt_id: None,
        })
        .expect("insert quote observation");

        assert_eq!(
            db.row_count("quote_observations")
                .expect("quote observation count"),
            1
        );
        assert_eq!(
            db.latest_quote_observation(edge_id)
                .expect("latest quote observation"),
            (
                "route-quote:native".to_string(),
                "1000000".to_string(),
                Some("1010000".to_string()),
                Some("7500".to_string()),
                Some(75.0),
                None,
            )
        );
    }

    #[test]
    fn optimizer_decision_evidence_selector_is_read_only() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (_pair, _family_id, edge_id) = seed_route_edge(&db);

        db.insert_quote_observation(&QuoteObservationRecord {
            route_edge_id: Some(edge_id),
            cycle: 43,
            source: "live_probe".to_string(),
            input_atoms: "1000000".to_string(),
            output_atoms: Some("1010000".to_string()),
            net_profit_atoms: Some("10000".to_string()),
            net_profit_bps: Some(100.0),
            fee_atoms: None,
            slot: Some(123),
            freshness_json: serde_json::json!({
                "optimizer_decision": {
                    "optimizer_decision_id": "decision-1",
                    "source": "two_pool_route_size_optimizer",
                    "route_label": "test"
                }
            })
            .to_string(),
            blocker: None,
            observed_at_unix_ms: 9_500,
            attempt_id: Some("attempt-selector".to_string()),
        })
        .expect("insert quote observation");

        let evidence = db
            .optimizer_decision_evidence(10)
            .expect("optimizer decision evidence");

        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].route_edge_id, Some(edge_id));
        assert_eq!(evidence[0].attempt_id, Some("attempt-selector".to_string()));
        assert_eq!(
            db.row_count("execution_attempts")
                .expect("execution attempt count"),
            0
        );
    }

    #[test]
    fn simulation_and_execution_attempts_are_persisted_for_route_evidence() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (_pair, _family_id, edge_id) = seed_route_edge(&db);

        db.insert_simulation_result(&SimulationResultRecord {
            route_edge_id: Some(edge_id),
            cycle: 77,
            input_atoms: "1000000".to_string(),
            pre_quote_atoms: Some("1000000".to_string()),
            post_quote_atoms: Some("1008000".to_string()),
            quote_delta_atoms: Some("8000".to_string()),
            net_profit_atoms: Some("6766".to_string()),
            net_profit_bps: Some(67.66),
            tx_bytes: Some(987),
            simulation_ok: true,
            simulation_error: None,
            logs_json: "[\"Program log: ok\"]".to_string(),
            simulated_at_unix_ms: 12_000,
            attempt_id: None,
        })
        .expect("insert simulation result");
        db.insert_execution_attempt(&ExecutionAttemptRecord {
            route_edge_id: Some(edge_id),
            cycle: 77,
            status: "blocked".to_string(),
            tx_signature: None,
            input_atoms: Some("1000000".to_string()),
            estimated_profit_atoms: Some("6766".to_string()),
            realized_profit_atoms: None,
            blocker: Some("simulation positive but submit disabled".to_string()),
            config_hash: "test-config".to_string(),
            wallet_pubkey: Some("wallet1111111111111111111111111111111111".to_string()),
            submitted_at_unix_ms: Some(12_500),
            confirmed_at_unix_ms: None,
            attempt_id: None,
        })
        .expect("insert execution attempt");

        assert_eq!(
            db.row_count("simulation_results")
                .expect("simulation result count"),
            1
        );
        assert_eq!(
            db.row_count("execution_attempts")
                .expect("execution attempt count"),
            1
        );
        assert_eq!(
            db.latest_simulation_result(edge_id)
                .expect("latest simulation result"),
            (
                "1000000".to_string(),
                Some("8000".to_string()),
                Some("6766".to_string()),
                Some(67.66),
                true,
                None,
            )
        );
        assert_eq!(
            db.latest_execution_attempt(edge_id)
                .expect("latest execution attempt"),
            (
                "blocked".to_string(),
                Some("1000000".to_string()),
                Some("6766".to_string()),
                Some("simulation positive but submit disabled".to_string()),
            )
        );
    }

    #[test]
    fn summary_counts_fresh_quote_reconciliation_and_simulation_mapping() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (pair, _family_id, edge_id) = seed_route_edge(&db);
        let now = 1_000;
        let context = serde_json::json!({
            "pair": {
                "base_mint": pair.base_mint,
                "quote_mint": pair.quote_mint,
                "base_decimals": pair.base_decimals,
                "quote_decimals": pair.quote_decimals,
            },
            "fresh_quote_reconciliation": {
                "fresh_quote_status": "fresh_positive_complete"
            }
        });
        db.insert_quote_observation(&QuoteObservationRecord {
            route_edge_id: Some(edge_id),
            cycle: 1,
            source: "focused_fresh_quote".to_string(),
            input_atoms: "1000000".to_string(),
            output_atoms: Some("1010000".to_string()),
            net_profit_atoms: Some("10000".to_string()),
            net_profit_bps: Some(100.0),
            fee_atoms: None,
            slot: Some(1),
            freshness_json: serde_json::to_string(&context).expect("json"),
            blocker: None,
            observed_at_unix_ms: now,
            attempt_id: Some("fresh-1".to_string()),
        })
        .expect("quote observation");
        db.insert_simulation_result(&SimulationResultRecord {
            route_edge_id: Some(edge_id),
            cycle: 1,
            input_atoms: "1000000".to_string(),
            pre_quote_atoms: Some("1000000".to_string()),
            post_quote_atoms: Some("1010000".to_string()),
            quote_delta_atoms: Some("10000".to_string()),
            net_profit_atoms: Some("10000".to_string()),
            net_profit_bps: Some(100.0),
            tx_bytes: None,
            simulation_ok: true,
            simulation_error: None,
            logs_json: "{}".to_string(),
            simulated_at_unix_ms: now,
            attempt_id: Some("sim-1".to_string()),
        })
        .expect("simulation result");

        let summary = db.summary().expect("summary");

        assert_eq!(summary.fresh_positive_complete_observations, 1);
        assert_eq!(summary.fresh_positive_blocked_observations, 0);
        assert_eq!(summary.fresh_negative_complete_observations, 0);
        assert_eq!(summary.fresh_quote_unavailable_observations, 0);
        assert_eq!(summary.simulation_positive_results, 1);
        assert_eq!(summary.simulation_positive_with_fresh_positive_complete, 1);
    }

    #[test]
    fn live_settlement_rows_are_persisted_and_counted() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (_pair, _family_id, edge_id) = seed_route_edge(&db);

        db.insert_live_settlement(&LiveSettlementRecord {
            route_edge_id: Some(edge_id),
            attempt_id: Some("live-1".to_string()),
            tx_signature: "sig1111111111111111111111111111111111111111111111111111111111111111"
                .to_string(),
            status: "failed".to_string(),
            quote_mint: "So11111111111111111111111111111111111111112".to_string(),
            input_atoms: Some("50000000".to_string()),
            estimated_profit_atoms: Some("10000".to_string()),
            realized_quote_delta_atoms: Some("0".to_string()),
            fee_lamports: Some(5000),
            fee_quote_atoms: Some("5000".to_string()),
            realized_profit_atoms: Some("-5000".to_string()),
            classification: "failed_fee_loss".to_string(),
            pre_balances_json: "{}".to_string(),
            post_balances_json: "{}".to_string(),
            meta_json: r#"{"source":"unit"}"#.to_string(),
            settled_at_unix_ms: 2_000,
        })
        .expect("settlement insert");

        let summary = db.summary().expect("summary");

        assert_eq!(summary.live_settlements, 1);
        assert_eq!(summary.live_settlement_fee_losses, 1);
        assert_eq!(summary.live_settlement_profits, 0);
    }

    #[test]
    fn fresh_quote_route_scores_rank_complete_positive_evidence() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (pair, _family_id, edge_id) = seed_route_edge(&db);
        for (attempt_id, status, bps, observed_at) in [
            ("fresh-pos", "fresh_positive_complete", 42.0, 2_000),
            ("fresh-neg", "fresh_negative_complete", -15.0, 1_000),
        ] {
            let context = serde_json::json!({
                "fresh_quote_reconciliation": {
                    "fresh_quote_status": status
                }
            });
            db.insert_quote_observation(&QuoteObservationRecord {
                route_edge_id: Some(edge_id),
                cycle: 1,
                source: "focused_fresh_quote".to_string(),
                input_atoms: "1000000".to_string(),
                output_atoms: Some("1004200".to_string()),
                net_profit_atoms: Some("4200".to_string()),
                net_profit_bps: Some(bps),
                fee_atoms: None,
                slot: Some(1),
                freshness_json: serde_json::to_string(&context).expect("json"),
                blocker: None,
                observed_at_unix_ms: observed_at,
                attempt_id: Some(attempt_id.to_string()),
            })
            .expect("quote observation");
        }

        let scores = db.fresh_quote_route_scores(5).expect("scores");

        assert_eq!(scores.len(), 1);
        assert_eq!(scores[0].route_edge_id, edge_id);
        assert_eq!(scores[0].base_mint, pair.base_mint);
        assert_eq!(scores[0].fresh_positive_complete, 1);
        assert_eq!(scores[0].fresh_negative_complete, 1);
        assert_eq!(scores[0].best_complete_fresh_bps, Some(42.0));
        assert_eq!(scores[0].latest_observed_at_unix_ms, Some(2_000));
    }

    #[test]
    fn fresh_quote_route_family_audit_recommends_positive_complete_families() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (_pair, family_id, edge_id) = seed_route_edge(&db);
        for (attempt_id, status, bps, observed_at) in [
            ("fresh-neg", "fresh_negative_complete", -25.0, 1_000),
            ("fresh-pos", "fresh_positive_complete", 18.0, 2_000),
        ] {
            let context = serde_json::json!({
                "fresh_quote_reconciliation": {
                    "fresh_quote_status": status
                }
            });
            db.insert_quote_observation(&QuoteObservationRecord {
                route_edge_id: Some(edge_id),
                cycle: 1,
                source: "focused_fresh_quote".to_string(),
                input_atoms: "1000000".to_string(),
                output_atoms: Some("1001800".to_string()),
                net_profit_atoms: Some("1800".to_string()),
                net_profit_bps: Some(bps),
                fee_atoms: None,
                slot: Some(1),
                freshness_json: serde_json::to_string(&context).expect("json"),
                blocker: None,
                observed_at_unix_ms: observed_at,
                attempt_id: Some(attempt_id.to_string()),
            })
            .expect("quote observation");
        }

        let audit = db.fresh_quote_route_family_audit(5).expect("audit");

        assert_eq!(audit.len(), 1);
        assert_eq!(audit[0].route_family_id, family_id);
        assert_eq!(audit[0].fresh_positive_complete, 1);
        assert_eq!(audit[0].fresh_negative_complete, 1);
        assert_eq!(audit[0].positive_complete_edges, 1);
        assert_eq!(audit[0].recommendation, "ready_for_no_submit_audit");
        assert!(audit[0].reason.contains("complete fresh-positive"));
    }

    #[test]
    fn blocked_positive_fresh_quote_details_are_reported() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (pair, family_id, edge_id) = seed_route_edge(&db);
        let context = serde_json::json!({
            "fresh_quote_reconciliation": {
                "fresh_quote_status": "fresh_positive_blocked",
                "blocker": "signed transaction is 1252 byte(s), above live max 1200"
            }
        });
        db.insert_quote_observation(&QuoteObservationRecord {
            route_edge_id: Some(edge_id),
            cycle: 7,
            source: "focused_fresh_quote".to_string(),
            input_atoms: "50000000".to_string(),
            output_atoms: Some("50090000".to_string()),
            net_profit_atoms: Some("90000".to_string()),
            net_profit_bps: Some(18.0),
            fee_atoms: None,
            slot: Some(33),
            freshness_json: serde_json::to_string(&context).expect("json"),
            blocker: Some("transaction_too_large".to_string()),
            observed_at_unix_ms: 12_345,
            attempt_id: Some("blocked-positive-1".to_string()),
        })
        .expect("quote observation");

        let blocked = db
            .fresh_quote_blocked_positive_records(5)
            .expect("blocked positive report");

        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].route_edge_id, edge_id);
        assert_eq!(blocked[0].route_family_id, family_id);
        assert_eq!(blocked[0].base_mint, pair.base_mint);
        assert_eq!(blocked[0].quote_mint, pair.quote_mint);
        assert_eq!(blocked[0].cycle, 7);
        assert_eq!(blocked[0].net_profit_bps, Some(18.0));
        assert_eq!(blocked[0].blocker.as_deref(), Some("transaction_too_large"));
        assert_eq!(blocked[0].attempt_id.as_deref(), Some("blocked-positive-1"));
        assert!(blocked[0]
            .freshness_json
            .contains("signed transaction is 1252 byte"));
    }

    #[test]
    fn route_family_promotion_decisions_rank_hot_candidates() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (_pair, family_id, edge_id) = seed_route_edge(&db);
        let context = serde_json::json!({
            "fresh_quote_reconciliation": {
                "fresh_quote_status": "fresh_positive_complete"
            }
        });
        db.insert_quote_observation(&QuoteObservationRecord {
            route_edge_id: Some(edge_id),
            cycle: 9,
            source: "focused_fresh_quote".to_string(),
            input_atoms: "1000000".to_string(),
            output_atoms: Some("1002400".to_string()),
            net_profit_atoms: Some("2400".to_string()),
            net_profit_bps: Some(24.0),
            fee_atoms: None,
            slot: Some(44),
            freshness_json: serde_json::to_string(&context).expect("json"),
            blocker: None,
            observed_at_unix_ms: 22_222,
            attempt_id: Some("hot-family-1".to_string()),
        })
        .expect("quote observation");

        let decisions = db.route_family_promotion_decisions(5).expect("decisions");

        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].route_family_id, family_id);
        assert_eq!(decisions[0].action, "hot_candidate");
        assert!(decisions[0].hot_path_eligible);
        assert!(decisions[0].score > 0.0);
        assert!(decisions[0].reason.contains("complete fresh-positive"));
    }

    #[test]
    fn route_edge_lookup_matches_pair_direction_and_pool_addresses() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (pair, _family_id, edge_id) = seed_route_edge(&db);

        assert_eq!(
            db.route_edge_id_for_pools(
                &pair,
                Venue::Meteora,
                Venue::Raydium,
                "MeteoraPool111111111111111111111111111111",
                "RaydiumPool111111111111111111111111111111",
            )
            .expect("lookup route edge"),
            Some(edge_id)
        );
        assert_eq!(
            db.route_edge_id_for_pools(
                &pair,
                Venue::Raydium,
                Venue::Meteora,
                "RaydiumPool111111111111111111111111111111",
                "MeteoraPool111111111111111111111111111111",
            )
            .expect("lookup reversed route edge"),
            None
        );
    }

    #[test]
    fn route_edge_readiness_lookup_matches_exact_pair_direction_and_pools() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (pair, _family_id, _edge_id) = seed_route_edge(&db);

        assert_eq!(
            db.route_edge_readiness_for_pools(
                &pair,
                Venue::Meteora,
                Venue::Raydium,
                "MeteoraPool111111111111111111111111111111",
                "RaydiumPool111111111111111111111111111111",
            )
            .expect("lookup readiness"),
            Some(RouteReadinessState::GraphIncomplete)
        );
        assert_eq!(
            db.route_edge_readiness_for_pools(
                &pair,
                Venue::Raydium,
                Venue::Meteora,
                "RaydiumPool111111111111111111111111111111",
                "MeteoraPool111111111111111111111111111111",
            )
            .expect("lookup reversed readiness"),
            None
        );
    }

    #[test]
    fn route_subscription_accounts_include_required_manifest_pubkeys() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (_pair, _family_id, edge_id) = seed_route_edge(&db);
        db.upsert_pool_account(&PoolAccountGraphRecord {
            pool_address: "MeteoraPool111111111111111111111111111111".to_string(),
            role: "vault_a".to_string(),
            pubkey: "FocusBuyVault111111111111111111111111111".to_string(),
            owner: None,
            mint: None,
            token_program: None,
            derivation_kind: None,
            derivation_seed_json: None,
            required: true,
            now_unix_ms: 10_000,
        })
        .expect("pool account");
        db.upsert_pool_account(&PoolAccountGraphRecord {
            pool_address: "RaydiumPool111111111111111111111111111111".to_string(),
            role: "optional_observation".to_string(),
            pubkey: "Optional11111111111111111111111111111111".to_string(),
            owner: None,
            mint: None,
            token_program: None,
            derivation_kind: None,
            derivation_seed_json: None,
            required: false,
            now_unix_ms: 10_000,
        })
        .expect("optional pool account");
        db.upsert_pool_account(&PoolAccountGraphRecord {
            pool_address: "RaydiumPool111111111111111111111111111111".to_string(),
            role: "raydium_amm_serum_vault_signer".to_string(),
            pubkey: "SignerPda1111111111111111111111111111111".to_string(),
            owner: None,
            mint: None,
            token_program: None,
            derivation_kind: None,
            derivation_seed_json: None,
            required: true,
            now_unix_ms: 10_000,
        })
        .expect("signer pda pool account");

        let pubkeys = db
            .route_subscription_pubkeys(edge_id)
            .expect("route subscription pubkeys");

        assert!(pubkeys.contains(&"MeteoraPool111111111111111111111111111111".to_string()));
        assert!(pubkeys.contains(&"RaydiumPool111111111111111111111111111111".to_string()));
        assert!(pubkeys.contains(&"FocusBuyVault111111111111111111111111111".to_string()));
        assert!(!pubkeys.contains(&"Optional11111111111111111111111111111111".to_string()));
        assert!(!pubkeys.contains(&"SignerPda1111111111111111111111111111111".to_string()));
    }

    #[test]
    fn route_subscription_accounts_keep_core_pools_first() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let (_pair, _family_id, edge_id) = seed_route_edge(&db);
        db.upsert_pool_account(&PoolAccountGraphRecord {
            pool_address: "MeteoraPool111111111111111111111111111111".to_string(),
            role: "vault_a".to_string(),
            pubkey: "AaaVault1111111111111111111111111111111111".to_string(),
            owner: None,
            mint: None,
            token_program: None,
            derivation_kind: None,
            derivation_seed_json: None,
            required: true,
            now_unix_ms: 10_000,
        })
        .expect("pool account");

        let pubkeys = db
            .route_subscription_pubkeys(edge_id)
            .expect("route subscription pubkeys");

        assert_eq!(
            &pubkeys[..2],
            &[
                "MeteoraPool111111111111111111111111111111".to_string(),
                "RaydiumPool111111111111111111111111111111".to_string(),
            ]
        );
        assert!(pubkeys.contains(&"AaaVault1111111111111111111111111111111111".to_string()));
    }

    fn seed_complete_route_accounts(db: &ArbDatabase) {
        for (pool_address, role, pubkey, owner) in [
            (
                "MeteoraPool111111111111111111111111111111",
                "meteora_reserve_x",
                "SharedVault1111111111111111111111111111111",
                Some("Tokenkeg1111111111111111111111111111111111"),
            ),
            (
                "MeteoraPool111111111111111111111111111111",
                "meteora_reserve_y",
                "MeteoraReserveY111111111111111111111111111",
                Some("Tokenkeg1111111111111111111111111111111111"),
            ),
            (
                "MeteoraPool111111111111111111111111111111",
                "meteora_token_x_mint",
                "MeteoraTokenX1111111111111111111111111111",
                None,
            ),
            (
                "MeteoraPool111111111111111111111111111111",
                "meteora_token_y_mint",
                "MeteoraTokenY1111111111111111111111111111",
                None,
            ),
            (
                "MeteoraPool111111111111111111111111111111",
                "meteora_oracle",
                "MeteoraOracle11111111111111111111111111111",
                Some("MeteoraProgram111111111111111111111111111"),
            ),
            (
                "MeteoraPool111111111111111111111111111111",
                "meteora_bin_array_0",
                "MeteoraBinArray11111111111111111111111111",
                Some("MeteoraProgram111111111111111111111111111"),
            ),
            (
                "RaydiumPool111111111111111111111111111111",
                "raydium_amm_config",
                "RaydiumAmmConfig111111111111111111111111",
                None,
            ),
            (
                "RaydiumPool111111111111111111111111111111",
                "raydium_token_0_vault",
                "SharedVault1111111111111111111111111111111",
                Some("Tokenkeg1111111111111111111111111111111111"),
            ),
            (
                "RaydiumPool111111111111111111111111111111",
                "raydium_token_1_vault",
                "RaydiumVault11111111111111111111111111111",
                Some("Tokenkeg1111111111111111111111111111111111"),
            ),
            (
                "RaydiumPool111111111111111111111111111111",
                "raydium_token_0_mint",
                "RaydiumMint011111111111111111111111111111",
                None,
            ),
            (
                "RaydiumPool111111111111111111111111111111",
                "raydium_token_1_mint",
                "RaydiumMint111111111111111111111111111111",
                None,
            ),
        ] {
            db.upsert_pool_account(&PoolAccountGraphRecord {
                pool_address: pool_address.to_string(),
                role: role.to_string(),
                pubkey: pubkey.to_string(),
                owner: owner.map(ToOwned::to_owned),
                mint: None,
                token_program: owner.map(ToOwned::to_owned),
                derivation_kind: None,
                derivation_seed_json: None,
                required: true,
                now_unix_ms: 2_000,
            })
            .expect("pool account");
        }
    }

    #[test]
    fn repair_indexed_pool_kinds_refreshes_route_manifest_requirements() {
        let db = ArbDatabase::open_in_memory().expect("open sqlite db");
        db.run_migrations().expect("migrations");
        let pair = TokenPair {
            base_mint: "BaseMint111111111111111111111111111111111111".to_string(),
            quote_mint: "So11111111111111111111111111111111111111112".to_string(),
            base_decimals: 6,
            quote_decimals: 9,
        };
        for (mint, symbol, decimals) in [
            (pair.base_mint.clone(), "BASE", pair.base_decimals),
            (pair.quote_mint.clone(), "WSOL", pair.quote_decimals),
        ] {
            db.upsert_token(&TokenGraphRecord {
                mint,
                symbol: symbol.to_string(),
                decimals,
                token_program: None,
                now_unix_ms: 1_000,
            })
            .expect("token");
        }
        db.upsert_pool(&PoolGraphRecord {
            pool_address: "MeteoraPools1111111111111111111111111111".to_string(),
            venue: Venue::Meteora,
            pool_kind: PoolKind::Dlmm,
            program_owner: Some(METEORA_POOLS_PROGRAM.to_string()),
            pair: pair.clone(),
            liquidity_quote_ui: Some(1.0),
            price_quote_per_base: Some(1.0),
            updated_slot: 1,
            source: "test".to_string(),
            now_unix_ms: 1_000,
        })
        .expect("meteora pool");
        db.upsert_pool(&PoolGraphRecord {
            pool_address: "RaydiumAmmV41111111111111111111111111111".to_string(),
            venue: Venue::Raydium,
            pool_kind: PoolKind::ConstantProduct,
            program_owner: Some(RAYDIUM_AMM_V4_PROGRAM.to_string()),
            pair: pair.clone(),
            liquidity_quote_ui: Some(1.0),
            price_quote_per_base: Some(1.0),
            updated_slot: 1,
            source: "test".to_string(),
            now_unix_ms: 1_000,
        })
        .expect("raydium pool");
        let family_id = db
            .upsert_route_family(&RouteFamilyGraphRecord {
                pair,
                buy_venue: Venue::Meteora,
                sell_venue: Venue::Raydium,
                enabled: true,
                priority: 0,
                now_unix_ms: 1_000,
            })
            .expect("family");
        db.upsert_route_edge(&RouteEdgeGraphRecord {
            route_family_id: family_id,
            buy_pool: "MeteoraPools1111111111111111111111111111".to_string(),
            sell_pool: "RaydiumAmmV41111111111111111111111111111".to_string(),
            buy_pool_kind: PoolKind::Dlmm,
            sell_pool_kind: PoolKind::ConstantProduct,
            readiness_state: RouteReadinessState::GraphIncomplete,
            last_blocker: None,
            cooldown_until_unix_ms: None,
            now_unix_ms: 1_000,
        })
        .expect("edge");

        let repair = db
            .repair_indexed_pool_kinds(2_000)
            .expect("repair indexed kinds");
        let materialized = db
            .materialize_route_manifests(2_000)
            .expect("materialize route manifests");
        let report = db.route_status_report().expect("route status");
        let blockers = report.families[0]
            .latest_blockers
            .iter()
            .map(|blocker| blocker.blocker.as_str())
            .collect::<Vec<_>>()
            .join("\n");

        assert_eq!(repair.pools_updated, 2);
        assert_eq!(repair.buy_edges_updated, 1);
        assert_eq!(repair.sell_edges_updated, 1);
        assert_eq!(materialized.incomplete_manifests, 1);
        assert!(blockers.contains("buy.meteora_pool_token_a_mint"));
        assert!(blockers.contains("sell.raydium_amm_coin_vault"));
        assert!(!blockers.contains("buy.meteora_reserve_x"));
        assert!(!blockers.contains("sell.raydium_amm_config"));
    }

    fn seed_route_edge(db: &ArbDatabase) -> (TokenPair, i64, i64) {
        let pair = TokenPair {
            base_mint: "WIFMint111111111111111111111111111111111111".to_string(),
            quote_mint: "So11111111111111111111111111111111111111112".to_string(),
            base_decimals: 6,
            quote_decimals: 9,
        };
        for (mint, symbol, decimals) in [
            (pair.base_mint.clone(), "WIF", pair.base_decimals),
            (pair.quote_mint.clone(), "WSOL", pair.quote_decimals),
        ] {
            db.upsert_token(&TokenGraphRecord {
                mint,
                symbol: symbol.to_string(),
                decimals,
                token_program: None,
                now_unix_ms: 1_000,
            })
            .expect("token");
        }
        for (pool_address, venue, kind) in [
            (
                "MeteoraPool111111111111111111111111111111".to_string(),
                Venue::Meteora,
                PoolKind::Dlmm,
            ),
            (
                "RaydiumPool111111111111111111111111111111".to_string(),
                Venue::Raydium,
                PoolKind::ConstantProduct,
            ),
        ] {
            db.upsert_pool(&PoolGraphRecord {
                pool_address,
                venue,
                pool_kind: kind,
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
            .upsert_route_family(&RouteFamilyGraphRecord {
                pair: pair.clone(),
                buy_venue: Venue::Meteora,
                sell_venue: Venue::Raydium,
                enabled: true,
                priority: 0,
                now_unix_ms: 1_000,
            })
            .expect("family");
        let edge_id = db
            .upsert_route_edge(&RouteEdgeGraphRecord {
                route_family_id: family_id,
                buy_pool: "MeteoraPool111111111111111111111111111111".to_string(),
                sell_pool: "RaydiumPool111111111111111111111111111111".to_string(),
                buy_pool_kind: PoolKind::Dlmm,
                sell_pool_kind: PoolKind::ConstantProduct,
                readiness_state: RouteReadinessState::GraphIncomplete,
                last_blocker: None,
                cooldown_until_unix_ms: None,
                now_unix_ms: 1_000,
            })
            .expect("edge");
        (pair, family_id, edge_id)
    }
}
