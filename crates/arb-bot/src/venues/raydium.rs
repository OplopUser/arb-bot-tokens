use crate::{
    types::{PoolKind, PoolState, RawAccount, TokenPair, Venue},
    venues::{DiscoveryJob, VenueAdapter},
};

const RAYDIUM_AMM_V4_PROGRAM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const RAYDIUM_CLMM_PROGRAM: &str = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";
const RAYDIUM_CPMM_PROGRAM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";

#[derive(Default)]
pub struct RaydiumAdapter;

impl VenueAdapter for RaydiumAdapter {
    fn venue(&self) -> Venue {
        Venue::Raydium
    }

    fn program_ids(&self) -> &'static [&'static str] {
        &[
            RAYDIUM_AMM_V4_PROGRAM,
            RAYDIUM_CLMM_PROGRAM,
            RAYDIUM_CPMM_PROGRAM,
        ]
    }

    fn account_data_size(&self) -> Option<usize> {
        Some(752)
    }

    fn discovery_jobs(&self) -> Vec<DiscoveryJob> {
        vec![
            DiscoveryJob {
                program_id: RAYDIUM_AMM_V4_PROGRAM,
                data_size: Some(752),
            },
            DiscoveryJob {
                program_id: RAYDIUM_CLMM_PROGRAM,
                data_size: Some(1544),
            },
            DiscoveryJob {
                program_id: RAYDIUM_CPMM_PROGRAM,
                data_size: Some(637),
            },
        ]
    }

    fn decode_pool(&self, account: &RawAccount) -> Option<PoolState> {
        decode_cpmm_like(account).or_else(|| decode_clmm_like(account))
    }
}

pub fn decode_cpmm_like(account: &RawAccount) -> Option<PoolState> {
    let data = &account.data;
    if data.len() < 160 {
        return None;
    }

    let base_mint = bs58::encode(data.get(72..104)?).into_string();
    let quote_mint = bs58::encode(data.get(104..136)?).into_string();
    let reserve_base_atoms = read_u64_le(data, 136).unwrap_or_default() as u128;
    let reserve_quote_atoms = read_u64_le(data, 144).unwrap_or_default() as u128;

    Some(PoolState {
        venue: Venue::Raydium,
        kind: PoolKind::ConstantProduct,
        address: account.pubkey.clone(),
        pair: TokenPair {
            base_mint,
            quote_mint,
            base_decimals: 6,
            quote_decimals: 6,
        },
        reserve_base_atoms,
        reserve_quote_atoms,
        sqrt_price_x64: None,
        active_bin_id: None,
        fee_bps: 25,
        protocol_fee_bps: 3,
        liquidity_quote_atoms: reserve_quote_atoms,
        updated_slot: account.slot,
    })
}

pub fn decode_clmm_like(account: &RawAccount) -> Option<PoolState> {
    let data = &account.data;
    if data.len() < 160 {
        return None;
    }

    let base_mint = bs58::encode(data.get(73..105)?).into_string();
    let quote_mint = bs58::encode(data.get(105..137)?).into_string();
    let sqrt_price_x64 = read_u128_le(data, 137).unwrap_or_default();

    Some(PoolState {
        venue: Venue::Raydium,
        kind: PoolKind::ConcentratedLiquidity,
        address: account.pubkey.clone(),
        pair: TokenPair {
            base_mint,
            quote_mint,
            base_decimals: 6,
            quote_decimals: 6,
        },
        reserve_base_atoms: 0,
        reserve_quote_atoms: 0,
        sqrt_price_x64: Some(sqrt_price_x64),
        active_bin_id: None,
        fee_bps: 25,
        protocol_fee_bps: 3,
        liquidity_quote_atoms: 0,
        updated_slot: account.slot,
    })
}

fn read_u64_le(data: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_le_bytes(
        data.get(offset..offset + 8)?.try_into().ok()?,
    ))
}

fn read_u128_le(data: &[u8], offset: usize) -> Option<u128> {
    Some(u128::from_le_bytes(
        data.get(offset..offset + 16)?.try_into().ok()?,
    ))
}
