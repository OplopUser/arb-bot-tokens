use crate::{
    types::{PoolKind, PoolState, RawAccount, TokenPair, Venue},
    venues::VenueAdapter,
};

const METEORA_DLMM_PROGRAM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
const METEORA_DLMM_PROGRAM_V2: &str = "ME27nBgwYEzHBfxiup6dKF5P7oKSftG5GGJrMej8dQV";
const DLMM_POOL_SIZE: usize = 840;

#[derive(Default)]
pub struct MeteoraAdapter;

impl VenueAdapter for MeteoraAdapter {
    fn venue(&self) -> Venue {
        Venue::Meteora
    }

    fn program_ids(&self) -> &'static [&'static str] {
        &[METEORA_DLMM_PROGRAM, METEORA_DLMM_PROGRAM_V2]
    }

    fn account_data_size(&self) -> Option<usize> {
        Some(DLMM_POOL_SIZE)
    }

    fn decode_pool(&self, account: &RawAccount) -> Option<PoolState> {
        decode_dlmm(account)
    }
}

pub fn decode_dlmm(account: &RawAccount) -> Option<PoolState> {
    let data = &account.data;
    if data.len() < 0x98 {
        return None;
    }

    let bin_step = read_u16_le(data, 0x29)?;
    let active_bin_id = read_i32_le(data, 0x2b)?;
    let fee_bps = read_u64_le(data, 0x44).unwrap_or(bin_step as u64) as u16;
    let token_x = bs58::encode(data.get(0x58..0x58 + 32)?).into_string();
    let token_y = bs58::encode(data.get(0x78..0x78 + 32)?).into_string();
    let reserve_x = read_u64_le(data, 0x98).unwrap_or_default() as u128;
    let reserve_y = read_u64_le(data, 0xa0).unwrap_or_default() as u128;

    Some(PoolState {
        venue: Venue::Meteora,
        kind: PoolKind::Dlmm,
        address: account.pubkey.clone(),
        pair: TokenPair {
            base_mint: token_x,
            quote_mint: token_y,
            base_decimals: 6,
            quote_decimals: 6,
        },
        reserve_base_atoms: reserve_x,
        reserve_quote_atoms: reserve_y,
        sqrt_price_x64: None,
        active_bin_id: Some(active_bin_id),
        fee_bps,
        protocol_fee_bps: 0,
        liquidity_quote_atoms: reserve_y,
        updated_slot: account.slot,
    })
}

fn read_u16_le(data: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_le_bytes(
        data.get(offset..offset + 2)?.try_into().ok()?,
    ))
}

fn read_i32_le(data: &[u8], offset: usize) -> Option<i32> {
    Some(i32::from_le_bytes(
        data.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn read_u64_le(data: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_le_bytes(
        data.get(offset..offset + 8)?.try_into().ok()?,
    ))
}
