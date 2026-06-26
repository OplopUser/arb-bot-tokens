use crate::{
    config::{FeesConfig, TradingConfig},
    rpc::RpcClient,
    types::FeeBreakdown,
};
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct DynamicFeeEngine {
    fees: FeesConfig,
    trading: TradingConfig,
}

impl DynamicFeeEngine {
    pub fn new(fees: FeesConfig, trading: TradingConfig) -> Self {
        Self { fees, trading }
    }

    pub async fn network_fee_for_accounts_with_sol_usd(
        &self,
        rpc: &RpcClient,
        writable_accounts: &[String],
        include_ata_rent: bool,
        sol_usd_estimate: f64,
    ) -> Result<FeeBreakdown> {
        let percentile_fee = rpc
            .priority_fee_percentile(writable_accounts, self.fees.priority_fee_percentile)
            .await
            .unwrap_or(0);
        let priority_fee_lamports =
            cap_priority_fee(percentile_fee, self.fees.max_priority_fee_lamports);
        let ata_rent_lamports = if include_ata_rent {
            self.fees.ata_rent_lamports
        } else {
            0
        };
        let total_lamports = self
            .fees
            .base_fee_lamports
            .saturating_add(priority_fee_lamports)
            .saturating_add(self.fees.jito_tip_lamports)
            .saturating_add(ata_rent_lamports);
        let network_fee_usd = (total_lamports as f64 / 1_000_000_000.0) * sol_usd_estimate;

        Ok(FeeBreakdown {
            lp_fee_atoms: 0,
            protocol_fee_atoms: 0,
            host_fee_atoms: 0,
            base_fee_lamports: self.fees.base_fee_lamports,
            priority_fee_lamports,
            jito_tip_lamports: self.fees.jito_tip_lamports,
            ata_rent_lamports,
            slippage_bps: 0.0,
            safety_buffer_bps: self.trading.fee_safety_buffer_bps,
            network_fee_usd,
        })
    }

    pub fn static_network_fee_with_sol_usd(
        &self,
        include_ata_rent: bool,
        sol_usd_estimate: f64,
    ) -> FeeBreakdown {
        let ata_rent_lamports = if include_ata_rent {
            self.fees.ata_rent_lamports
        } else {
            0
        };
        let total_lamports = self
            .fees
            .base_fee_lamports
            .saturating_add(self.fees.jito_tip_lamports)
            .saturating_add(ata_rent_lamports);
        let network_fee_usd = (total_lamports as f64 / 1_000_000_000.0) * sol_usd_estimate;

        FeeBreakdown {
            lp_fee_atoms: 0,
            protocol_fee_atoms: 0,
            host_fee_atoms: 0,
            base_fee_lamports: self.fees.base_fee_lamports,
            priority_fee_lamports: 0,
            jito_tip_lamports: self.fees.jito_tip_lamports,
            ata_rent_lamports,
            slippage_bps: 0.0,
            safety_buffer_bps: self.trading.fee_safety_buffer_bps,
            network_fee_usd,
        }
    }
}

fn cap_priority_fee(observed_lamports: u64, max_lamports: u64) -> u64 {
    if max_lamports == 0 {
        observed_lamports
    } else {
        observed_lamports.min(max_lamports)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FeesConfig, TradingConfig};

    fn trading() -> TradingConfig {
        TradingConfig {
            live: true,
            trade_size_usd: 25.0,
            min_net_profit_bps: 15.0,
            fee_safety_buffer_bps: 5.0,
            max_active_tokens: 100,
            scan_interval_ms: 5_000,
            quote_mints: vec![],
            major_denylist: vec![],
            sol_usd_estimate: 100.0,
        }
    }

    #[test]
    fn static_network_fee_includes_jito_and_ata_rent() {
        let engine = DynamicFeeEngine::new(
            FeesConfig {
                priority_fee_percentile: 75.0,
                max_priority_fee_lamports: 250_000,
                jito_tip_lamports: 10_000,
                max_jito_tip_lamports: 10_000,
                ata_rent_lamports: 2_000_000,
                base_fee_lamports: 5_000,
            },
            trading(),
        );

        let fee = engine.static_network_fee_with_sol_usd(true, 100.0);
        assert_eq!(fee.base_fee_lamports, 5_000);
        assert_eq!(fee.jito_tip_lamports, 10_000);
        assert_eq!(fee.ata_rent_lamports, 2_000_000);
        assert!(fee.network_fee_usd > 0.0);
    }

    #[test]
    fn zero_priority_fee_cap_means_uncapped() {
        assert_eq!(cap_priority_fee(75_000, 0), 75_000);
        assert_eq!(cap_priority_fee(75_000, 50_000), 50_000);
    }
}
