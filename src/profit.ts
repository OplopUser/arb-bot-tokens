import { PriceQuote, ArbitrageOpportunity, ArbitrageDirection, Venue } from './types.js';
import { config } from './config.js';

/**
 * Profit Calculator
 *
 * Calculates net profit from arbitrage opportunities accounting for:
 * - DEX-specific fees
 * - Slippage based on trade size vs liquidity
 * - Solana base transaction fee
 * - Jito tip (if using bundles for MEV protection)
 * - Token transfer costs
 *
 * Fee Structure by DEX:
 * - Jupiter: 0 bps (aggregator, charges via route fees)
 * - Raydium AMM: 0.25% (25 bps) split between LP + protocol
 * - Raydium CLMM: 0.04% - 1% depending on pool fee tier
 * - Orca Whirlpools: 0.01% - 1% depending on fee tier (typically 0.03% for stable, 0.3% for volatile)
 * - Meteora DLMM: Dynamic, typically 0.04% - 1%
 * - DFlow: Market-dependent, typically 0.02% - 0.10%
 */

/** DEX fee rates in basis points (bps) */
const DEX_FEE_BPS: Record<Venue, number> = {
  [Venue.Jupiter]: 0,   // Jupiter itself charges 0; route fees are embedded
  [Venue.Raydium]: 25,  // 0.25% standard AMM fee
  [Venue.Orca]: 30,     // 0.30% standard Whirlpool fee for volatile pairs
  [Venue.Meteora]: 25,  // 0.25% typical DLMM fee
  [Venue.DFlow]: 5,     // 0.05% typical DFlow fee
};

/**
 * Calculate net profit for an arbitrage opportunity
 *
 * @param buyQuote - Price quote from the venue where token is cheaper
 * @param sellQuote - Price quote from the venue where token is more expensive
 * @param tradeSizeUsd - Amount of capital to deploy (in USD)
 * @returns ArbitrageOpportunity or null if below minimum profit
 */
export function calculateProfit(
  buyQuote: PriceQuote,
  sellQuote: PriceQuote,
  tradeSizeUsd: number = config.tradeSizeUsd,
): ArbitrageOpportunity | null {
  if (buyQuote.price <= 0 || sellQuote.price <= 0) return null;
  if (buyQuote.price >= sellQuote.price) return null; // No arbitrage

  const pair = buyQuote.pair;
  const grossPriceSpread = (sellQuote.price - buyQuote.price) / buyQuote.price;
  const grossPriceSpreadPct = grossPriceSpread * 100;

  // Calculate gross profit
  const baseAmount = tradeSizeUsd / buyQuote.price;
  const grossProceeds = baseAmount * sellQuote.price;
  const grossProfitUsd = grossProceeds - tradeSizeUsd;

  // DEX fees (applied to the trade on both venues if executing two swaps)
  const buyFeeBps = DEX_FEE_BPS[buyQuote.venue] || 25;
  const sellFeeBps = DEX_FEE_BPS[sellQuote.venue] || 25;

  // Fee in USD: feeBps / 10000 * trade volume
  const buyFeeUsd = (buyFeeBps / 10000) * tradeSizeUsd;
  const sellFeeUsd = (sellFeeBps / 10000) * grossProceeds;
  const dexFeeUsd = buyFeeUsd + sellFeeUsd;

  // Slippage estimation
  // Based on trade size relative to available liquidity
  const buySlippageUsd = estimateSlippageCost(
    tradeSizeUsd,
    buyQuote.liquidity,
    buyQuote.price,
    config.maxSlippageBps,
  );
  const sellSlippageUsd = estimateSlippageCost(
    grossProceeds,
    sellQuote.liquidity,
    sellQuote.price,
    config.maxSlippageBps,
  );
  const slippageCostUsd = buySlippageUsd + sellSlippageUsd;

  // Solana transaction fee
  // Base fee: 5000 lamports per signature (~2 signatures = 10000 lamports)
  // Jito tip (optional): config.jitoTipLamports
  const lamports = 10000 + config.jitoTipLamports; // 2 sigs + tip
  const solPrice = getSolPrice(); // rough estimate of SOL price
  const txFeeUsd = (lamports / 1e9) * solPrice;

  // Net profit
  const netProfitUsd = grossProfitUsd - dexFeeUsd - slippageCostUsd - txFeeUsd;
  const netProfitPct = (netProfitUsd / tradeSizeUsd) * 100;

  const opportunity: ArbitrageOpportunity = {
    pair,
    buyVenue: buyQuote.venue,
    sellVenue: sellQuote.venue,
    buyPrice: buyQuote.price,
    sellPrice: sellQuote.price,
    priceSpreadPct: grossPriceSpreadPct,
    grossProfitUsd,
    txFeeUsd,
    dexFeeUsd,
    slippageCostUsd,
    netProfitUsd,
    netProfitPct,
    direction: ArbitrageDirection.BuyOnVenueA_SellOnVenueB,
    timestamp: Date.now(),
  };

  // Filter out unprofitable opportunities
  if (netProfitUsd < config.minProfitUsd) return null;

  return opportunity;
}

/**
 * Calculate the opportunity in the opposite direction
 * (buy on sellVenue, sell on buyVenue) for triangular detection
 */
export function calculateReverseProfit(
  buyQuote: PriceQuote,
  sellQuote: PriceQuote,
  tradeSizeUsd: number = config.tradeSizeUsd,
): ArbitrageOpportunity | null {
  return calculateProfit(sellQuote, buyQuote, tradeSizeUsd);
}

/**
 * Estimate slippage cost based on trade size vs liquidity
 *
 * Uses a simplified constant-product AMM slippage model:
 * - If trade size is small relative to liquidity, slippage is low
 * - Uses config.maxSlippageBps as the upper bound
 */
function estimateSlippageCost(
  tradeUsd: number,
  liquidityUsd: number,
  _price: number,
  maxSlippageBps: number,
): number {
  if (liquidityUsd <= 0 || tradeUsd <= 0) {
    // No liquidity info, use max slippage as estimate
    return tradeUsd * (maxSlippageBps / 10000);
  }

  // For a constant product AMM: slippage ≈ (tradeSize / liquidity) * price
  // Simplified: slippage_pct = tradeSize / liquidity (in same units)
  const tradeRatio = tradeUsd / liquidityUsd;
  const estimatedSlippageBps = Math.min(
    tradeRatio * 10000, // Convert ratio to bps
    maxSlippageBps,
  );

  return tradeUsd * (estimatedSlippageBps / 10000);
}

/**
 * Get a rough SOL price for fee estimation
 * In production, this would fetch from an oracle or price feed
 */
let cachedSolPrice: number = 150; // Default fallback
let solPriceTimestamp: number = 0;

function getSolPrice(): number {
  if (Date.now() - solPriceTimestamp > 60_000) {
    cachedSolPrice = 150;
    solPriceTimestamp = Date.now();
  }
  return cachedSolPrice;
}

/**
 * Update the cached SOL price
 */
export function updateSolPrice(price: number): void {
  cachedSolPrice = price;
  solPriceTimestamp = Date.now();
}

/**
 * Get total cost breakdown for an opportunity
 */
export function getCostBreakdown(opp: ArbitrageOpportunity): {
  grossProfit: number;
  dexFees: number;
  slippage: number;
  txFee: number;
  netProfit: number;
  totalCosts: number;
} {
  const totalCosts = opp.dexFeeUsd + opp.slippageCostUsd + opp.txFeeUsd;
  return {
    grossProfit: opp.grossProfitUsd,
    dexFees: opp.dexFeeUsd,
    slippage: opp.slippageCostUsd,
    txFee: opp.txFeeUsd,
    netProfit: opp.netProfitUsd,
    totalCosts,
  };
}
