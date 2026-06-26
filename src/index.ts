/**
 * Solana Arbitrage Bot
 *
 * A production-grade arbitrage bot that monitors prices across multiple
 * Solana DEX venues (Jupiter, Raydium, Orca, Meteora, DFlow) and
 * identifies profitable trading opportunities.
 *
 * @packageDocumentation
 */

// ─── Core Types ──────────────────────────────────────────────────────────────
export type {
  TokenPair,
  PriceQuote,
  ArbitrageOpportunity,
  ArbitrageDirection,
  BotConfig,
  TradeExecutionResult,
} from './types.js';

export {
  Venue,
} from './types.js';

// ─── Scanner ─────────────────────────────────────────────────────────────────
export {
  fetchQuotes,
  findOpportunitiesFromQuotes,
  scanPair,
  scanPairs,
  scanPairWithBidAsk,
  getPriceSummary,
} from './scanner.js';

// ─── Profit Calculator ───────────────────────────────────────────────────────
export {
  calculateProfit,
  calculateReverseProfit,
  getCostBreakdown,
  updateSolPrice,
} from './profit.js';

// ─── Price Fetchers ──────────────────────────────────────────────────────────
export { fetchJupiterQuote } from './fetchers/jupiter.js';
export { fetchRaydiumQuote } from './fetchers/raydium.js';
export { fetchOrcaQuote } from './fetchers/orca.js';
export { fetchMeteoraQuote } from './fetchers/meteora.js';
export { fetchDFlowQuote } from './fetchers/dflow.js';

// ─── Config ──────────────────────────────────────────────────────────────────
export { config, KNOWN_TOKENS } from './config.js';
export type { KnownToken } from './config.js';

// ─── Utils ───────────────────────────────────────────────────────────────────
export { logger, formatOpportunity, setLogLevel } from './utils/logger.js';
export { getConnection } from './utils/connection.js';

/**
 * Bot version
 */
export const VERSION = '1.0.0';
