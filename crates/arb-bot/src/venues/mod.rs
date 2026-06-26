use crate::{
    math::pool_quote,
    types::{FeeBreakdown, PoolState, Quote, RawAccount, SwapSide, Venue},
};

pub mod meteora;
pub mod orca;
pub mod raydium;

#[derive(Debug, Clone, Copy)]
pub struct DiscoveryJob {
    pub program_id: &'static str,
    pub data_size: Option<usize>,
}

pub trait VenueAdapter: Send + Sync {
    fn venue(&self) -> Venue;
    fn program_ids(&self) -> &'static [&'static str];
    fn account_data_size(&self) -> Option<usize>;
    fn decode_pool(&self, account: &RawAccount) -> Option<PoolState>;

    fn discovery_jobs(&self) -> Vec<DiscoveryJob> {
        self.program_ids()
            .iter()
            .map(|program_id| DiscoveryJob {
                program_id,
                data_size: self.account_data_size(),
            })
            .collect()
    }

    fn quote(
        &self,
        pool: &PoolState,
        side: SwapSide,
        in_amount_atoms: u128,
        network_fee: &FeeBreakdown,
        safety_buffer_bps: f64,
    ) -> Option<Quote> {
        pool_quote(pool, side, in_amount_atoms, network_fee, safety_buffer_bps)
    }
}

pub fn enabled_adapters(enabled: &[String]) -> Vec<Box<dyn VenueAdapter>> {
    let enabled = enabled
        .iter()
        .map(|venue| venue.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    let mut adapters: Vec<Box<dyn VenueAdapter>> = Vec::new();

    if enabled.contains("orca") {
        adapters.push(Box::<orca::OrcaAdapter>::default());
    }
    if enabled.contains("meteora") {
        adapters.push(Box::<meteora::MeteoraAdapter>::default());
    }
    if enabled.contains("raydium") {
        adapters.push(Box::<raydium::RaydiumAdapter>::default());
    }

    adapters
}
