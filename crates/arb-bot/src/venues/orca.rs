use crate::{
    types::{PoolKind, PoolState, RawAccount, TokenPair, Venue},
    venues::VenueAdapter,
};

const ORCA_WHIRLPOOL_PROGRAM: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const WHIRLPOOL_ACCOUNT_SIZE: usize = 653;

#[derive(Default)]
pub struct OrcaAdapter;

impl VenueAdapter for OrcaAdapter {
    fn venue(&self) -> Venue {
        Venue::Orca
    }

    fn program_ids(&self) -> &'static [&'static str] {
        &[ORCA_WHIRLPOOL_PROGRAM]
    }

    fn account_data_size(&self) -> Option<usize> {
        Some(WHIRLPOOL_ACCOUNT_SIZE)
    }

    fn decode_pool(&self, account: &RawAccount) -> Option<PoolState> {
        decode_whirlpool(account)
    }
}

pub fn decode_whirlpool(account: &RawAccount) -> Option<PoolState> {
    let data = &account.data;
    if data.len() < 213 {
        return None;
    }

    let fee_rate_hundredths_bps = read_u16_le(data, 45)?;
    let fee_bps = ((fee_rate_hundredths_bps as f64) / 100.0).round() as u16;
    let liquidity = read_u128_le(data, 49)?;
    let sqrt_price_x64 = read_u128_le(data, 65)?;
    let token_a = bs58::encode(data.get(101..133)?).into_string();
    let token_b = bs58::encode(data.get(181..213)?).into_string();

    Some(PoolState {
        venue: Venue::Orca,
        kind: PoolKind::ConcentratedLiquidity,
        address: account.pubkey.clone(),
        pair: TokenPair {
            base_mint: token_a,
            quote_mint: token_b,
            base_decimals: 6,
            quote_decimals: 6,
        },
        reserve_base_atoms: 0,
        reserve_quote_atoms: 0,
        sqrt_price_x64: Some(sqrt_price_x64),
        active_bin_id: None,
        fee_bps,
        protocol_fee_bps: 0,
        liquidity_quote_atoms: liquidity,
        updated_slot: account.slot,
    })
}

fn read_u16_le(data: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_le_bytes(
        data.get(offset..offset + 2)?.try_into().ok()?,
    ))
}

fn read_u128_le(data: &[u8], offset: usize) -> Option<u128> {
    Some(u128::from_le_bytes(
        data.get(offset..offset + 16)?.try_into().ok()?,
    ))
}
