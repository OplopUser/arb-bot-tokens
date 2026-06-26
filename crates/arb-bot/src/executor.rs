use crate::{config::BotConfig, live_swap::JupiterLiveSwapExecutor, types::Opportunity};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionPlan {
    pub live_requested: bool,
    pub legs: Vec<SwapLegPlan>,
    pub priority_fee_lamports: u64,
    pub jito_tip_lamports: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapLegPlan {
    pub venue: String,
    pub pool: String,
    pub input_mint: String,
    pub output_mint: String,
    pub input_atoms: u128,
}

pub struct Executor {
    config: BotConfig,
}

impl Executor {
    pub fn new(config: BotConfig) -> Self {
        Self { config }
    }

    pub fn build_plan(&self, opportunity: &Opportunity) -> TransactionPlan {
        TransactionPlan {
            live_requested: self.config.trading.live,
            legs: vec![
                SwapLegPlan {
                    venue: opportunity.buy_venue.to_string(),
                    pool: opportunity.buy_pool.clone(),
                    input_mint: opportunity.pair.quote_mint.clone(),
                    output_mint: opportunity.pair.base_mint.clone(),
                    input_atoms: opportunity.input_quote_atoms,
                },
                SwapLegPlan {
                    venue: opportunity.sell_venue.to_string(),
                    pool: opportunity.sell_pool.clone(),
                    input_mint: opportunity.pair.base_mint.clone(),
                    output_mint: opportunity.pair.quote_mint.clone(),
                    input_atoms: opportunity.base_atoms,
                },
            ],
            priority_fee_lamports: opportunity.fee.priority_fee_lamports,
            jito_tip_lamports: opportunity.fee.jito_tip_lamports,
        }
    }

    #[allow(dead_code)]
    pub async fn execute(&self, opportunity: &Opportunity) -> Result<String> {
        let report = JupiterLiveSwapExecutor::new(self.config.clone())
            .execute_atomic_arbitrage(opportunity, true)
            .await?;
        report.signature.ok_or_else(|| {
            anyhow!(report
                .blocked_reason
                .unwrap_or_else(|| "atomic arbitrage was not submitted".to_string()))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{
            AggregatorsConfig, BotConfig, DatabaseConfig, DiscoveryConfig, ExecutionConfig,
            FeesConfig, JupiterConfig, RpcConfig, RwaConfig, SimulationConfig, SubmittersConfig,
            TradingConfig, VenuesConfig, WalletConfig,
        },
        types::{FeeBreakdown, TokenPair, Venue, USDC_MINT},
    };

    fn config() -> BotConfig {
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
                trade_size_usd: 25.0,
                min_net_profit_bps: 10.0,
                fee_safety_buffer_bps: 5.0,
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
                max_priority_fee_lamports: 250_000,
                jito_tip_lamports: 1_000,
                max_jito_tip_lamports: 1_000,
                ata_rent_lamports: 0,
                base_fee_lamports: 5_000,
            },
            venues: VenuesConfig {
                enabled: vec!["orca".to_string(), "raydium".to_string()],
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

    #[test]
    fn transaction_plan_contains_two_atomic_legs() {
        let executor = Executor::new(config());
        let opportunity = Opportunity {
            pair: TokenPair {
                base_mint: "BASE".to_string(),
                quote_mint: USDC_MINT.to_string(),
                base_decimals: 6,
                quote_decimals: 6,
            },
            buy_venue: Venue::Orca,
            sell_venue: Venue::Raydium,
            buy_pool: "buy".to_string(),
            sell_pool: "sell".to_string(),
            input_quote_atoms: 25_000_000,
            base_atoms: 10_000_000,
            output_quote_atoms: 26_000_000,
            gross_profit_quote_atoms: 1_000_000,
            net_profit_usd: 1.0,
            net_profit_bps: 400.0,
            spread_bps: 500.0,
            fee: FeeBreakdown {
                priority_fee_lamports: 20_000,
                jito_tip_lamports: 1_000,
                ..FeeBreakdown::default()
            },
            detected_slot: 1,
            optimizer_decision: None,
        };

        let plan = executor.build_plan(&opportunity);
        assert!(plan.live_requested);
        assert_eq!(plan.legs.len(), 2);
        assert_eq!(plan.legs[0].venue, "orca");
        assert_eq!(plan.legs[1].venue, "raydium");
        assert_eq!(plan.priority_fee_lamports, 20_000);
    }
}
