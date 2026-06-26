use crate::types::{
    atoms_to_ui, ui_to_atoms, FeeBreakdown, OptimizerDecisionTrace, PoolState, Quote, SwapSide,
};
use serde::{Deserialize, Serialize};

const PRICE_ONLY_MAX_LIQUIDITY_USAGE_BPS: u128 = 2_000;

pub fn pool_quote(
    pool: &PoolState,
    side: SwapSide,
    in_amount_atoms: u128,
    network_fee: &FeeBreakdown,
    safety_buffer_bps: f64,
) -> Option<Quote> {
    constant_product_quote(pool, side, in_amount_atoms, network_fee, safety_buffer_bps)
        .or_else(|| price_based_quote(pool, side, in_amount_atoms, network_fee, safety_buffer_bps))
}

pub fn constant_product_quote(
    pool: &PoolState,
    side: SwapSide,
    in_amount_atoms: u128,
    network_fee: &FeeBreakdown,
    safety_buffer_bps: f64,
) -> Option<Quote> {
    if in_amount_atoms == 0 || pool.reserve_base_atoms == 0 || pool.reserve_quote_atoms == 0 {
        return None;
    }

    let (reserve_in, reserve_out, out_decimals) = match side {
        SwapSide::BaseToQuote => (
            pool.reserve_base_atoms,
            pool.reserve_quote_atoms,
            pool.pair.quote_decimals,
        ),
        SwapSide::QuoteToBase => (
            pool.reserve_quote_atoms,
            pool.reserve_base_atoms,
            pool.pair.base_decimals,
        ),
    };

    let total_fee_bps = pool.fee_bps.saturating_add(pool.protocol_fee_bps) as u128;
    let lp_fee_atoms = in_amount_atoms.saturating_mul(pool.fee_bps as u128) / 10_000;
    let protocol_fee_atoms = in_amount_atoms.saturating_mul(pool.protocol_fee_bps as u128) / 10_000;
    let effective_in =
        in_amount_atoms.saturating_sub(in_amount_atoms.saturating_mul(total_fee_bps) / 10_000);
    if effective_in == 0 {
        return None;
    }

    let numerator = effective_in.saturating_mul(reserve_out);
    let denominator = reserve_in.saturating_add(effective_in);
    if denominator == 0 {
        return None;
    }
    let out_amount_atoms = numerator / denominator;
    if out_amount_atoms == 0 {
        return None;
    }

    let no_slippage_price = reserve_out as f64 / reserve_in as f64;
    let execution_price = out_amount_atoms as f64 / in_amount_atoms as f64;
    let slippage_bps =
        ((no_slippage_price - execution_price) / no_slippage_price * 10_000.0).max(0.0);

    let mut fee = FeeBreakdown {
        lp_fee_atoms,
        protocol_fee_atoms,
        host_fee_atoms: 0,
        base_fee_lamports: 0,
        priority_fee_lamports: 0,
        jito_tip_lamports: 0,
        ata_rent_lamports: 0,
        slippage_bps,
        safety_buffer_bps,
        network_fee_usd: 0.0,
    };
    fee.merge_network(network_fee);

    let effective_price =
        out_amount_atoms as f64 / 10f64.powi(out_decimals as i32) / (in_amount_atoms as f64);

    Some(Quote {
        venue: pool.venue,
        pool: pool.address.clone(),
        pair: pool.pair.clone(),
        side,
        in_amount_atoms,
        out_amount_atoms,
        effective_price,
        fee,
        slot: pool.updated_slot,
    })
}

pub fn price_based_quote(
    pool: &PoolState,
    side: SwapSide,
    in_amount_atoms: u128,
    network_fee: &FeeBreakdown,
    safety_buffer_bps: f64,
) -> Option<Quote> {
    if in_amount_atoms == 0 {
        return None;
    }
    if pool.liquidity_quote_atoms == 0 {
        return None;
    }
    let price_quote_per_base = pool.price_quote_per_base()?;
    if price_quote_per_base <= 0.0 || !price_quote_per_base.is_finite() {
        return None;
    }

    let total_fee_bps = pool.fee_bps.saturating_add(pool.protocol_fee_bps) as u128;
    let lp_fee_atoms = in_amount_atoms.saturating_mul(pool.fee_bps as u128) / 10_000;
    let protocol_fee_atoms = in_amount_atoms.saturating_mul(pool.protocol_fee_bps as u128) / 10_000;
    let effective_in =
        in_amount_atoms.saturating_sub(in_amount_atoms.saturating_mul(total_fee_bps) / 10_000);

    let out_amount_atoms = match side {
        SwapSide::BaseToQuote => {
            let base_in = atoms_to_ui(effective_in, pool.pair.base_decimals);
            ui_to_atoms(base_in * price_quote_per_base, pool.pair.quote_decimals)
        }
        SwapSide::QuoteToBase => {
            let quote_in = atoms_to_ui(effective_in, pool.pair.quote_decimals);
            ui_to_atoms(quote_in / price_quote_per_base, pool.pair.base_decimals)
        }
    };
    if out_amount_atoms == 0 {
        return None;
    }
    if !within_price_only_liquidity(pool, side, in_amount_atoms, out_amount_atoms) {
        return None;
    }

    let effective_price = match side {
        SwapSide::BaseToQuote => {
            let base_in = atoms_to_ui(in_amount_atoms, pool.pair.base_decimals);
            let quote_out = atoms_to_ui(out_amount_atoms, pool.pair.quote_decimals);
            quote_out / base_in
        }
        SwapSide::QuoteToBase => {
            let quote_in = atoms_to_ui(in_amount_atoms, pool.pair.quote_decimals);
            let base_out = atoms_to_ui(out_amount_atoms, pool.pair.base_decimals);
            quote_in / base_out
        }
    };

    let mut fee = FeeBreakdown {
        lp_fee_atoms,
        protocol_fee_atoms,
        host_fee_atoms: 0,
        base_fee_lamports: 0,
        priority_fee_lamports: 0,
        jito_tip_lamports: 0,
        ata_rent_lamports: 0,
        slippage_bps: 0.0,
        safety_buffer_bps,
        network_fee_usd: 0.0,
    };
    fee.merge_network(network_fee);

    Some(Quote {
        venue: pool.venue,
        pool: pool.address.clone(),
        pair: pool.pair.clone(),
        side,
        in_amount_atoms,
        out_amount_atoms,
        effective_price,
        fee,
        slot: pool.updated_slot,
    })
}

#[derive(Debug, Clone, Copy)]
pub struct TwoPoolRouteSizeRequest<'a> {
    pub buy_pool: &'a PoolState,
    pub sell_pool: &'a PoolState,
    pub min_input_quote_atoms: u128,
    pub max_input_quote_atoms: u128,
    pub network_fee: &'a FeeBreakdown,
    pub safety_buffer_bps: f64,
    pub min_absolute_profit_quote_atoms: u128,
    pub include_setup_cost: bool,
    pub setup_lamport_usd_value: f64,
    pub min_net_profit_bps: f64,
    pub quote_atom_usd_value: f64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TwoPoolRouteSizeResult {
    pub input_quote_atoms: u128,
    pub base_atoms: u128,
    pub output_quote_atoms: u128,
    pub gross_profit_quote_atoms: i128,
    pub required_profit_quote_atoms: u128,
    pub net_profit_quote_atoms: i128,
    pub net_profit_usd: f64,
    pub net_profit_bps: f64,
    pub buy_quote: Quote,
    pub sell_quote: Quote,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuoteDrift {
    pub expected_atoms: u128,
    pub observed_atoms: u128,
    pub drift_atoms: i128,
    pub drift_bps: f64,
}

pub fn quote_drift(
    expected_atoms: Option<u128>,
    observed_atoms: Option<u128>,
) -> Option<QuoteDrift> {
    let expected_atoms = expected_atoms?;
    let observed_atoms = observed_atoms?;
    if expected_atoms == 0 {
        return None;
    }
    let drift_atoms =
        u128_to_i128_saturating(observed_atoms) - u128_to_i128_saturating(expected_atoms);
    let drift_bps = drift_atoms as f64 / expected_atoms as f64 * 10_000.0;
    Some(QuoteDrift {
        expected_atoms,
        observed_atoms,
        drift_atoms,
        drift_bps,
    })
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OptimizerReplaySelection {
    pub selected_input_quote_atoms: u128,
    pub base_atoms: u128,
    pub output_quote_atoms: u128,
    pub net_profit_quote_atoms: i128,
    pub net_profit_bps: f64,
}

impl OptimizerReplaySelection {
    fn from_trace(trace: &OptimizerDecisionTrace) -> Self {
        Self {
            selected_input_quote_atoms: trace.selected_input_quote_atoms,
            base_atoms: trace.base_atoms,
            output_quote_atoms: trace.output_quote_atoms,
            net_profit_quote_atoms: trace.net_profit_quote_atoms,
            net_profit_bps: trace.net_profit_bps,
        }
    }

    fn from_result(result: &TwoPoolRouteSizeResult) -> Self {
        Self {
            selected_input_quote_atoms: result.input_quote_atoms,
            base_atoms: result.base_atoms,
            output_quote_atoms: result.output_quote_atoms,
            net_profit_quote_atoms: result.net_profit_quote_atoms,
            net_profit_bps: result.net_profit_bps,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OptimizerReplayReport {
    pub optimizer_decision_id: String,
    pub route_edge_id: Option<i64>,
    pub buy_pool: String,
    pub sell_pool: String,
    pub rejected: bool,
    pub rejection_reason: Option<String>,
    pub original: OptimizerReplaySelection,
    pub replayed: OptimizerReplaySelection,
    pub selected_input_quote_atoms_drift: i128,
    pub base_atoms_drift: i128,
    pub output_quote_atoms_drift: i128,
    pub net_profit_quote_atoms_drift: i128,
    pub net_profit_bps_drift: f64,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum OptimizerReplayError {
    #[error("optimizer decision trace is missing buy_pool_snapshot")]
    MissingBuyPoolSnapshot,
    #[error("optimizer decision trace is missing sell_pool_snapshot")]
    MissingSellPoolSnapshot,
    #[error("optimizer replay produced no profitable result")]
    NoReplayResult,
}

pub fn replay_optimizer_decision_trace(
    trace: &OptimizerDecisionTrace,
) -> Result<OptimizerReplayReport, OptimizerReplayError> {
    if trace.rejection_reason.is_some() {
        let original = OptimizerReplaySelection::from_trace(trace);
        return Ok(OptimizerReplayReport {
            optimizer_decision_id: trace.optimizer_decision_id.clone(),
            route_edge_id: trace.route_edge_id,
            buy_pool: trace.buy_pool.clone(),
            sell_pool: trace.sell_pool.clone(),
            rejected: true,
            rejection_reason: trace.rejection_reason.clone(),
            selected_input_quote_atoms_drift: 0,
            base_atoms_drift: 0,
            output_quote_atoms_drift: 0,
            net_profit_quote_atoms_drift: 0,
            net_profit_bps_drift: 0.0,
            original: original.clone(),
            replayed: original,
        });
    }
    let buy_pool = trace
        .buy_pool_snapshot
        .as_ref()
        .ok_or(OptimizerReplayError::MissingBuyPoolSnapshot)?;
    let sell_pool = trace
        .sell_pool_snapshot
        .as_ref()
        .ok_or(OptimizerReplayError::MissingSellPoolSnapshot)?;
    let replayed_result = optimize_two_pool_route_size(TwoPoolRouteSizeRequest {
        buy_pool,
        sell_pool,
        min_input_quote_atoms: trace.min_input_quote_atoms,
        max_input_quote_atoms: trace.max_input_quote_atoms,
        network_fee: &trace.network_fee,
        safety_buffer_bps: trace.safety_buffer_bps,
        min_absolute_profit_quote_atoms: trace.min_absolute_profit_quote_atoms,
        include_setup_cost: trace.include_setup_cost,
        setup_lamport_usd_value: trace.setup_lamport_usd_value,
        min_net_profit_bps: trace.min_net_profit_bps,
        quote_atom_usd_value: trace.quote_atom_usd_value,
    })
    .ok_or(OptimizerReplayError::NoReplayResult)?;

    let original = OptimizerReplaySelection::from_trace(trace);
    let replayed = OptimizerReplaySelection::from_result(&replayed_result);

    Ok(OptimizerReplayReport {
        optimizer_decision_id: trace.optimizer_decision_id.clone(),
        route_edge_id: trace.route_edge_id,
        buy_pool: trace.buy_pool.clone(),
        sell_pool: trace.sell_pool.clone(),
        rejected: false,
        rejection_reason: None,
        selected_input_quote_atoms_drift: u128_to_i128_saturating(
            replayed.selected_input_quote_atoms,
        ) - u128_to_i128_saturating(
            original.selected_input_quote_atoms,
        ),
        base_atoms_drift: u128_to_i128_saturating(replayed.base_atoms)
            - u128_to_i128_saturating(original.base_atoms),
        output_quote_atoms_drift: u128_to_i128_saturating(replayed.output_quote_atoms)
            - u128_to_i128_saturating(original.output_quote_atoms),
        net_profit_quote_atoms_drift: replayed.net_profit_quote_atoms
            - original.net_profit_quote_atoms,
        net_profit_bps_drift: replayed.net_profit_bps - original.net_profit_bps,
        original,
        replayed,
    })
}

pub fn optimize_two_pool_route_size(
    request: TwoPoolRouteSizeRequest<'_>,
) -> Option<TwoPoolRouteSizeResult> {
    if request.min_input_quote_atoms == 0
        || request.max_input_quote_atoms < request.min_input_quote_atoms
        || !request.quote_atom_usd_value.is_finite()
        || request.quote_atom_usd_value <= 0.0
    {
        return None;
    }
    if request.buy_pool.pair.key() != request.sell_pool.pair.key() {
        return None;
    }

    let cost_model = RouteExecutionCostModel::from_request(&request)?;
    let scorer = |input_atoms: u128| {
        route_size_result_for_input(
            request.buy_pool,
            request.sell_pool,
            input_atoms,
            request.network_fee,
            request.safety_buffer_bps,
            &cost_model,
            request.quote_atom_usd_value,
        )
    };

    let mut best = best_of_inputs(
        [
            request.min_input_quote_atoms,
            request.max_input_quote_atoms,
            request.min_input_quote_atoms
                + (request.max_input_quote_atoms - request.min_input_quote_atoms) / 2,
        ],
        &scorer,
    );

    let mut left = request.min_input_quote_atoms;
    let mut right = request.max_input_quote_atoms;
    while right.saturating_sub(left) > 24 {
        let span = right - left;
        let mid_left = left + span / 3;
        let mid_right = right - span / 3;
        let left_score = scorer(mid_left)
            .map(|result| result.net_profit_quote_atoms)
            .unwrap_or(i128::MIN);
        let right_score = scorer(mid_right)
            .map(|result| result.net_profit_quote_atoms)
            .unwrap_or(i128::MIN);
        if left_score < right_score {
            left = mid_left.saturating_add(1);
        } else {
            right = mid_right.saturating_sub(1).max(left);
        }
        best = best_result(best, scorer(mid_left));
        best = best_result(best, scorer(mid_right));
    }

    let mut input = left;
    while input <= right {
        best = best_result(best, scorer(input));
        if input == u128::MAX {
            break;
        }
        input += 1;
    }

    let result = best?;
    if result.net_profit_quote_atoms <= 0 {
        return None;
    }
    if !cost_model.satisfies_absolute_profit_floor(result.net_profit_quote_atoms) {
        return None;
    }
    if result.net_profit_bps < request.min_net_profit_bps {
        return None;
    }
    Some(result)
}

pub fn route_net_profit_quote_atoms(
    buy_pool: &PoolState,
    sell_pool: &PoolState,
    input_quote_atoms: u128,
    network_fee: &FeeBreakdown,
    safety_buffer_bps: f64,
    extra_cost_quote_atoms: u128,
) -> Option<i128> {
    let cost_model = RouteExecutionCostModel {
        network_fee_quote_atoms: extra_cost_quote_atoms,
        setup_cost_quote_atoms: 0,
        min_absolute_profit_quote_atoms: 0,
    };
    route_size_result_for_input(
        buy_pool,
        sell_pool,
        input_quote_atoms,
        network_fee,
        safety_buffer_bps,
        &cost_model,
        1.0,
    )
    .map(|result| result.net_profit_quote_atoms)
}

fn route_size_result_for_input(
    buy_pool: &PoolState,
    sell_pool: &PoolState,
    input_quote_atoms: u128,
    network_fee: &FeeBreakdown,
    safety_buffer_bps: f64,
    cost_model: &RouteExecutionCostModel,
    quote_atom_usd_value: f64,
) -> Option<TwoPoolRouteSizeResult> {
    let buy_quote = pool_quote(
        buy_pool,
        SwapSide::QuoteToBase,
        input_quote_atoms,
        network_fee,
        safety_buffer_bps,
    )?;
    let sell_quote = pool_quote(
        sell_pool,
        SwapSide::BaseToQuote,
        buy_quote.out_amount_atoms,
        network_fee,
        safety_buffer_bps,
    )?;
    let gross_profit_quote_atoms = u128_to_i128_saturating(sell_quote.out_amount_atoms)
        - u128_to_i128_saturating(input_quote_atoms);
    let safety_cost_atoms = if safety_buffer_bps.is_finite() && safety_buffer_bps > 0.0 {
        ((input_quote_atoms as f64) * safety_buffer_bps / 10_000.0).ceil() as u128
    } else {
        0
    };
    let required_profit_quote_atoms = cost_model
        .network_fee_quote_atoms
        .saturating_add(cost_model.setup_cost_quote_atoms)
        .saturating_add(safety_cost_atoms);
    let net_profit_quote_atoms =
        gross_profit_quote_atoms - u128_to_i128_saturating(required_profit_quote_atoms);
    let net_profit_bps = if input_quote_atoms == 0 {
        0.0
    } else {
        net_profit_quote_atoms as f64 / input_quote_atoms as f64 * 10_000.0
    };

    Some(TwoPoolRouteSizeResult {
        input_quote_atoms,
        base_atoms: buy_quote.out_amount_atoms,
        output_quote_atoms: sell_quote.out_amount_atoms,
        gross_profit_quote_atoms,
        required_profit_quote_atoms,
        net_profit_quote_atoms,
        net_profit_usd: net_profit_quote_atoms as f64 * quote_atom_usd_value,
        net_profit_bps,
        buy_quote,
        sell_quote,
    })
}

fn best_of_inputs<const N: usize, F>(
    inputs: [u128; N],
    scorer: &F,
) -> Option<TwoPoolRouteSizeResult>
where
    F: Fn(u128) -> Option<TwoPoolRouteSizeResult>,
{
    inputs
        .into_iter()
        .fold(None, |best, input| best_result(best, scorer(input)))
}

fn best_result(
    current: Option<TwoPoolRouteSizeResult>,
    candidate: Option<TwoPoolRouteSizeResult>,
) -> Option<TwoPoolRouteSizeResult> {
    match (current, candidate) {
        (None, candidate) => candidate,
        (current, None) => current,
        (Some(current), Some(candidate)) => {
            if candidate.net_profit_quote_atoms > current.net_profit_quote_atoms {
                Some(candidate)
            } else {
                Some(current)
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RouteExecutionCostModel {
    network_fee_quote_atoms: u128,
    setup_cost_quote_atoms: u128,
    min_absolute_profit_quote_atoms: u128,
}

impl RouteExecutionCostModel {
    fn from_request(request: &TwoPoolRouteSizeRequest<'_>) -> Option<Self> {
        let network_fee_quote_atoms = usd_cost_atoms(
            request.network_fee.network_fee_usd,
            request.quote_atom_usd_value,
        )?;
        let setup_cost_quote_atoms = if request.include_setup_cost {
            setup_cost_quote_atoms(
                request.network_fee,
                request.setup_lamport_usd_value,
                request.quote_atom_usd_value,
            )?
        } else {
            0
        };

        Some(Self {
            network_fee_quote_atoms,
            setup_cost_quote_atoms,
            min_absolute_profit_quote_atoms: request.min_absolute_profit_quote_atoms,
        })
    }

    fn satisfies_absolute_profit_floor(&self, net_profit_quote_atoms: i128) -> bool {
        net_profit_quote_atoms >= u128_to_i128_saturating(self.min_absolute_profit_quote_atoms)
    }
}

fn setup_cost_quote_atoms(
    network_fee: &FeeBreakdown,
    setup_lamport_usd_value: f64,
    quote_atom_usd_value: f64,
) -> Option<u128> {
    if network_fee.ata_rent_lamports == 0 {
        return Some(0);
    }
    if !setup_lamport_usd_value.is_finite() || setup_lamport_usd_value <= 0.0 {
        return Some(0);
    }
    usd_cost_atoms(
        network_fee.ata_rent_lamports as f64 * setup_lamport_usd_value,
        quote_atom_usd_value,
    )
}

fn usd_cost_atoms(cost_usd: f64, quote_atom_usd_value: f64) -> Option<u128> {
    if !quote_atom_usd_value.is_finite() || quote_atom_usd_value <= 0.0 {
        return None;
    }
    if !cost_usd.is_finite() || cost_usd <= 0.0 {
        return Some(0);
    }
    Some((cost_usd / quote_atom_usd_value).ceil() as u128)
}

fn u128_to_i128_saturating(value: u128) -> i128 {
    i128::try_from(value).unwrap_or(i128::MAX)
}

fn within_price_only_liquidity(
    pool: &PoolState,
    side: SwapSide,
    in_amount_atoms: u128,
    out_amount_atoms: u128,
) -> bool {
    let max_quote_atoms = pool
        .liquidity_quote_atoms
        .saturating_mul(PRICE_ONLY_MAX_LIQUIDITY_USAGE_BPS)
        / 10_000;
    if max_quote_atoms == 0 {
        return false;
    }

    match side {
        SwapSide::BaseToQuote => out_amount_atoms <= max_quote_atoms,
        SwapSide::QuoteToBase => in_amount_atoms <= max_quote_atoms,
    }
}

#[allow(dead_code)]
pub fn fee_adjusted_amount(amount: u128, fee_bps: u16, protocol_fee_bps: u16) -> u128 {
    let total_fee_bps = fee_bps.saturating_add(protocol_fee_bps) as u128;
    amount.saturating_sub(amount.saturating_mul(total_fee_bps) / 10_000)
}

pub fn percentile(values: &mut [u64], pct: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    let pct = pct.clamp(0.0, 100.0);
    let idx = ((pct / 100.0) * (values.len().saturating_sub(1) as f64)).round() as usize;
    values[idx.min(values.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{OptimizerDecisionTrace, PoolKind, TokenPair, Venue};

    fn test_pool() -> PoolState {
        PoolState {
            venue: Venue::Raydium,
            kind: PoolKind::ConstantProduct,
            address: "pool".to_string(),
            pair: TokenPair {
                base_mint: "BASE".to_string(),
                quote_mint: "QUOTE".to_string(),
                base_decimals: 6,
                quote_decimals: 6,
            },
            reserve_base_atoms: 1_000_000_000,
            reserve_quote_atoms: 2_000_000_000,
            sqrt_price_x64: None,
            active_bin_id: None,
            fee_bps: 25,
            protocol_fee_bps: 5,
            liquidity_quote_atoms: 2_000_000_000,
            updated_slot: 1,
        }
    }

    #[test]
    fn constant_product_quote_accounts_for_dynamic_pool_fee() {
        let quote = constant_product_quote(
            &test_pool(),
            SwapSide::BaseToQuote,
            10_000_000,
            &FeeBreakdown::default(),
            5.0,
        )
        .unwrap();

        assert!(quote.out_amount_atoms > 19_000_000);
        assert_eq!(quote.fee.lp_fee_atoms, 25_000);
        assert_eq!(quote.fee.protocol_fee_atoms, 5_000);
        assert!(quote.fee.slippage_bps > 0.0);
    }

    #[test]
    fn percentile_selects_expected_value() {
        let mut values = vec![10, 20, 30, 40, 50];
        assert_eq!(percentile(&mut values, 75.0), 40);
    }

    #[test]
    fn fee_adjusted_amount_subtracts_lp_and_protocol_fees() {
        assert_eq!(fee_adjusted_amount(1_000_000, 25, 5), 997_000);
    }

    #[test]
    fn price_based_quote_uses_sqrt_price_when_reserves_absent() {
        let mut pool = test_pool();
        pool.reserve_base_atoms = 0;
        pool.reserve_quote_atoms = 0;
        pool.sqrt_price_x64 = Some(2u128.pow(64));
        let quote = pool_quote(
            &pool,
            SwapSide::QuoteToBase,
            1_000_000,
            &FeeBreakdown::default(),
            5.0,
        )
        .unwrap();

        assert!(quote.out_amount_atoms > 990_000);
        assert_eq!(quote.fee.lp_fee_atoms, 2_500);
    }

    #[test]
    fn price_based_quote_rejects_price_only_pool_without_liquidity() {
        let mut pool = test_pool();
        pool.reserve_base_atoms = 0;
        pool.reserve_quote_atoms = 0;
        pool.liquidity_quote_atoms = 0;
        pool.sqrt_price_x64 = Some(2u128.pow(64));

        assert!(pool_quote(
            &pool,
            SwapSide::QuoteToBase,
            1_000_000,
            &FeeBreakdown::default(),
            5.0,
        )
        .is_none());
    }

    #[test]
    fn price_based_quote_caps_price_only_pool_by_liquidity() {
        let mut pool = test_pool();
        pool.reserve_base_atoms = 0;
        pool.reserve_quote_atoms = 0;
        pool.sqrt_price_x64 = Some(2u128.pow(64));

        assert!(pool_quote(
            &pool,
            SwapSide::QuoteToBase,
            500_000_000,
            &FeeBreakdown::default(),
            5.0,
        )
        .is_none());
    }

    #[test]
    fn two_pool_optimizer_selects_interior_size_that_maximizes_net_profit() {
        let mut buy_pool = test_pool();
        buy_pool.reserve_base_atoms = 1_000_000_000_000;
        buy_pool.reserve_quote_atoms = 1_000_000_000_000;
        buy_pool.fee_bps = 0;
        buy_pool.protocol_fee_bps = 0;
        let mut sell_pool = test_pool();
        sell_pool.address = "sell".to_string();
        sell_pool.venue = Venue::Meteora;
        sell_pool.reserve_base_atoms = 1_000_000_000_000;
        sell_pool.reserve_quote_atoms = 1_100_000_000_000;
        sell_pool.fee_bps = 0;
        sell_pool.protocol_fee_bps = 0;

        let result = optimize_two_pool_route_size(TwoPoolRouteSizeRequest {
            buy_pool: &buy_pool,
            sell_pool: &sell_pool,
            min_input_quote_atoms: 1_000_000,
            max_input_quote_atoms: 500_000_000_000,
            network_fee: &FeeBreakdown::default(),
            safety_buffer_bps: 0.0,
            min_absolute_profit_quote_atoms: 0,
            include_setup_cost: true,
            setup_lamport_usd_value: 0.0000001,
            min_net_profit_bps: 0.0,
            quote_atom_usd_value: 0.000001,
        })
        .expect("profitable route");

        assert!(result.input_quote_atoms > 1_000_000);
        assert!(result.input_quote_atoms < 500_000_000_000);
        assert!(result.output_quote_atoms > result.input_quote_atoms);
        assert!(result.net_profit_quote_atoms > 0);
        assert!(
            result.net_profit_quote_atoms
                > route_net_profit_quote_atoms(
                    &buy_pool,
                    &sell_pool,
                    1_000_000,
                    &FeeBreakdown::default(),
                    0.0,
                    0
                )
                .unwrap()
        );
        assert!(
            result.net_profit_quote_atoms
                > route_net_profit_quote_atoms(
                    &buy_pool,
                    &sell_pool,
                    500_000_000_000,
                    &FeeBreakdown::default(),
                    0.0,
                    0
                )
                .unwrap()
        );
    }

    #[test]
    fn two_pool_optimizer_rejects_routes_below_cost_floor() {
        let mut buy_pool = test_pool();
        buy_pool.reserve_base_atoms = 1_000_000_000_000;
        buy_pool.reserve_quote_atoms = 1_000_000_000_000;
        buy_pool.fee_bps = 0;
        buy_pool.protocol_fee_bps = 0;
        let mut sell_pool = buy_pool.clone();
        sell_pool.address = "sell".to_string();
        sell_pool.venue = Venue::Meteora;
        sell_pool.reserve_quote_atoms = 1_001_000_000_000;
        let network_fee = FeeBreakdown {
            network_fee_usd: 50.0,
            ..FeeBreakdown::default()
        };

        assert!(optimize_two_pool_route_size(TwoPoolRouteSizeRequest {
            buy_pool: &buy_pool,
            sell_pool: &sell_pool,
            min_input_quote_atoms: 1_000_000,
            max_input_quote_atoms: 500_000_000,
            network_fee: &network_fee,
            safety_buffer_bps: 0.0,
            min_absolute_profit_quote_atoms: 0,
            include_setup_cost: true,
            setup_lamport_usd_value: 0.0000001,
            min_net_profit_bps: 0.0,
            quote_atom_usd_value: 0.000001,
        })
        .is_none());
    }

    #[test]
    fn two_pool_optimizer_rejects_high_bps_tiny_trade_below_absolute_floor() {
        let mut buy_pool = test_pool();
        buy_pool.reserve_base_atoms = 1_000_000_000_000;
        buy_pool.reserve_quote_atoms = 1_000_000_000_000;
        buy_pool.fee_bps = 0;
        buy_pool.protocol_fee_bps = 0;
        let mut sell_pool = buy_pool.clone();
        sell_pool.address = "sell".to_string();
        sell_pool.venue = Venue::Meteora;
        sell_pool.reserve_quote_atoms = 1_005_000_000_000;

        assert!(optimize_two_pool_route_size(TwoPoolRouteSizeRequest {
            buy_pool: &buy_pool,
            sell_pool: &sell_pool,
            min_input_quote_atoms: 1_000,
            max_input_quote_atoms: 10_000,
            network_fee: &FeeBreakdown::default(),
            safety_buffer_bps: 0.0,
            min_absolute_profit_quote_atoms: 100,
            include_setup_cost: true,
            setup_lamport_usd_value: 0.0000001,
            min_net_profit_bps: 10.0,
            quote_atom_usd_value: 0.000001,
        })
        .is_none());
    }

    #[test]
    fn two_pool_optimizer_prefunded_mode_excludes_setup_cost() {
        let mut buy_pool = test_pool();
        buy_pool.reserve_base_atoms = 1_000_000_000_000;
        buy_pool.reserve_quote_atoms = 1_000_000_000_000;
        buy_pool.fee_bps = 0;
        buy_pool.protocol_fee_bps = 0;
        let mut sell_pool = buy_pool.clone();
        sell_pool.address = "sell".to_string();
        sell_pool.venue = Venue::Meteora;
        sell_pool.reserve_quote_atoms = 1_010_000_000_000;
        let setup_fee = FeeBreakdown {
            network_fee_usd: 0.0,
            ata_rent_lamports: 50_000_000,
            ..FeeBreakdown::default()
        };

        let prefunded = optimize_two_pool_route_size(TwoPoolRouteSizeRequest {
            buy_pool: &buy_pool,
            sell_pool: &sell_pool,
            min_input_quote_atoms: 1_000_000,
            max_input_quote_atoms: 10_000_000,
            network_fee: &setup_fee,
            safety_buffer_bps: 0.0,
            min_absolute_profit_quote_atoms: 0,
            include_setup_cost: false,
            setup_lamport_usd_value: 0.0000001,
            min_net_profit_bps: 0.0,
            quote_atom_usd_value: 0.000001,
        });
        let non_prefunded = optimize_two_pool_route_size(TwoPoolRouteSizeRequest {
            buy_pool: &buy_pool,
            sell_pool: &sell_pool,
            min_input_quote_atoms: 1_000_000,
            max_input_quote_atoms: 10_000_000,
            network_fee: &setup_fee,
            safety_buffer_bps: 0.0,
            min_absolute_profit_quote_atoms: 0,
            include_setup_cost: true,
            setup_lamport_usd_value: 0.0000001,
            min_net_profit_bps: 0.0,
            quote_atom_usd_value: 0.000001,
        });

        assert!(prefunded.is_some());
        assert!(non_prefunded.is_none());
    }

    #[test]
    fn two_pool_optimizer_selects_larger_absolute_profit_over_higher_bps() {
        let mut buy_pool = test_pool();
        buy_pool.reserve_base_atoms = 1_000_000_000_000;
        buy_pool.reserve_quote_atoms = 1_000_000_000_000;
        buy_pool.fee_bps = 0;
        buy_pool.protocol_fee_bps = 0;
        let mut sell_pool = buy_pool.clone();
        sell_pool.address = "sell".to_string();
        sell_pool.venue = Venue::Meteora;
        sell_pool.reserve_quote_atoms = 1_100_000_000_000;

        let result = optimize_two_pool_route_size(TwoPoolRouteSizeRequest {
            buy_pool: &buy_pool,
            sell_pool: &sell_pool,
            min_input_quote_atoms: 1_000_000,
            max_input_quote_atoms: 100_000_000,
            network_fee: &FeeBreakdown::default(),
            safety_buffer_bps: 0.0,
            min_absolute_profit_quote_atoms: 0,
            include_setup_cost: true,
            setup_lamport_usd_value: 0.0000001,
            min_net_profit_bps: 0.0,
            quote_atom_usd_value: 0.000001,
        })
        .expect("profitable route");

        let min_result = route_size_result_for_input(
            &buy_pool,
            &sell_pool,
            1_000_000,
            &FeeBreakdown::default(),
            0.0,
            &RouteExecutionCostModel {
                network_fee_quote_atoms: 0,
                setup_cost_quote_atoms: 0,
                min_absolute_profit_quote_atoms: 0,
            },
            0.000001,
        )
        .unwrap();

        assert!(min_result.net_profit_bps > result.net_profit_bps);
        assert!(result.net_profit_quote_atoms > min_result.net_profit_quote_atoms);
        assert!(result.input_quote_atoms > min_result.input_quote_atoms);
    }

    #[test]
    fn optimizer_trace_replays_after_pool_snapshot_json_roundtrip() {
        let mut buy_pool = test_pool();
        buy_pool.reserve_base_atoms = 1_000_000_000_000;
        buy_pool.reserve_quote_atoms = 1_000_000_000_000;
        buy_pool.fee_bps = 0;
        buy_pool.protocol_fee_bps = 0;
        let mut sell_pool = buy_pool.clone();
        sell_pool.address = "sell".to_string();
        sell_pool.venue = Venue::Meteora;
        sell_pool.reserve_quote_atoms = 1_100_000_000_000;
        let network_fee = FeeBreakdown::default();

        let result = optimize_two_pool_route_size(TwoPoolRouteSizeRequest {
            buy_pool: &buy_pool,
            sell_pool: &sell_pool,
            min_input_quote_atoms: 1_000_000,
            max_input_quote_atoms: 100_000_000,
            network_fee: &network_fee,
            safety_buffer_bps: 0.0,
            min_absolute_profit_quote_atoms: 0,
            include_setup_cost: true,
            setup_lamport_usd_value: 0.0000001,
            min_net_profit_bps: 0.0,
            quote_atom_usd_value: 0.000001,
        })
        .expect("profitable route");

        let trace = OptimizerDecisionTrace {
            optimizer_decision_id: "test-decision".to_string(),
            source: "two_pool_route_size_optimizer".to_string(),
            route_label: "test".to_string(),
            route_edge_id: Some(12),
            buy_pool: "pool".to_string(),
            sell_pool: "sell".to_string(),
            buy_venue: Some(Venue::Raydium),
            sell_venue: Some(Venue::Meteora),
            min_input_quote_atoms: 1_000_000,
            max_input_quote_atoms: 100_000_000,
            selected_input_quote_atoms: result.input_quote_atoms,
            base_atoms: result.base_atoms,
            output_quote_atoms: result.output_quote_atoms,
            gross_profit_quote_atoms: result.gross_profit_quote_atoms,
            required_profit_quote_atoms: result.required_profit_quote_atoms,
            net_profit_quote_atoms: result.net_profit_quote_atoms,
            net_profit_usd: result.net_profit_usd,
            net_profit_bps: result.net_profit_bps,
            quote_atom_usd_value: 0.000001,
            safety_buffer_bps: 0.0,
            min_net_profit_bps: 0.0,
            min_absolute_profit_quote_atoms: 0,
            min_executable_input_quote_atoms: None,
            rejection_reason: None,
            network_fee,
            include_setup_cost: true,
            setup_lamport_usd_value: 0.0000001,
            buy_pool_snapshot: Some(buy_pool),
            sell_pool_snapshot: Some(sell_pool),
        };
        let decoded: OptimizerDecisionTrace =
            serde_json::from_str(&serde_json::to_string(&trace).expect("serialize trace"))
                .expect("deserialize trace");

        let replay = replay_optimizer_decision_trace(&decoded).expect("replay trace");

        assert_eq!(
            replay.original.selected_input_quote_atoms,
            replay.replayed.selected_input_quote_atoms
        );
        assert_eq!(replay.selected_input_quote_atoms_drift, 0);
        assert_eq!(replay.base_atoms_drift, 0);
        assert_eq!(replay.output_quote_atoms_drift, 0);
        assert_eq!(replay.net_profit_quote_atoms_drift, 0);
        assert_eq!(replay.net_profit_bps_drift, 0.0);
    }

    #[test]
    fn optimizer_replay_reports_rejected_no_executable_size_without_drift() {
        let trace = OptimizerDecisionTrace {
            optimizer_decision_id: "rejected-decision".to_string(),
            source: "two_pool_route_size_optimizer".to_string(),
            route_label: "test".to_string(),
            route_edge_id: Some(44),
            buy_pool: "buy".to_string(),
            sell_pool: "sell".to_string(),
            buy_venue: Some(Venue::Meteora),
            sell_venue: Some(Venue::Raydium),
            min_input_quote_atoms: 2_000_000,
            max_input_quote_atoms: 1_000_000,
            selected_input_quote_atoms: 1_000_000,
            base_atoms: 0,
            output_quote_atoms: 0,
            gross_profit_quote_atoms: -1_000_000,
            required_profit_quote_atoms: 0,
            net_profit_quote_atoms: -1_000_000,
            net_profit_usd: 0.0,
            net_profit_bps: 0.0,
            quote_atom_usd_value: 0.000001,
            safety_buffer_bps: 0.0,
            min_net_profit_bps: 0.0,
            min_absolute_profit_quote_atoms: 0,
            min_executable_input_quote_atoms: Some(2_000_000),
            rejection_reason: Some(
                "no executable live size: min executable input 2000000 exceeds cap 1000000"
                    .to_string(),
            ),
            network_fee: FeeBreakdown::default(),
            include_setup_cost: false,
            setup_lamport_usd_value: 0.0000001,
            buy_pool_snapshot: None,
            sell_pool_snapshot: None,
        };

        let replay = replay_optimizer_decision_trace(&trace).expect("rejected replay report");

        assert!(replay.rejected);
        assert_eq!(replay.route_edge_id, Some(44));
        assert_eq!(replay.selected_input_quote_atoms_drift, 0);
        assert_eq!(replay.base_atoms_drift, 0);
        assert_eq!(replay.output_quote_atoms_drift, 0);
        assert_eq!(replay.net_profit_quote_atoms_drift, 0);
        assert_eq!(replay.net_profit_bps_drift, 0.0);
        assert_eq!(replay.original, replay.replayed);
    }

    #[test]
    fn quote_drift_reports_positive_and_negative_drift() {
        let positive = quote_drift(Some(1_000_000), Some(1_001_000)).expect("positive drift");
        assert_eq!(positive.drift_atoms, 1_000);
        assert_eq!(positive.drift_bps, 10.0);

        let negative = quote_drift(Some(1_000_000), Some(998_000)).expect("negative drift");
        assert_eq!(negative.drift_atoms, -2_000);
        assert_eq!(negative.drift_bps, -20.0);

        assert!(quote_drift(None, Some(1_000_000)).is_none());
        assert!(quote_drift(Some(0), Some(1_000_000)).is_none());
    }
}
