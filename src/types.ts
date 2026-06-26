import { PublicKey } from '@solana/web3.js';

// ─── Enums ───────────────────────────────────────────────────────────────────

/** Supported DEX venues for price fetching */
export enum Venue {
  Jupiter = 'jupiter',
  Raydium = 'raydium',
  Orca = 'orca',
  Meteora = 'meteora',
  DFlow = 'dflow',
}

/** Direction of arbitrage trade */
export enum ArbitrageDirection {
  BuyOnVenueA_SellOnVenueB = 'buy_a_sell_b',
  BuyOnVenueB_SellOnVenueA = 'buy_b_sell_a',
}

// ─── Core Data Structures ────────────────────────────────────────────────────

/** Represents a token pair to trade */
export interface TokenPair {
  /** Symbol string, e.g. "SOL" */
  baseSymbol: string;
  /** Symbol string, e.g. "USDC" */
  quoteSymbol: string;
  /** Base token mint address */
  baseMint: PublicKey;
  /** Quote token mint address */
  quoteMint: PublicKey;
  /** Decimals for base token */
  baseDecimals: number;
  /** Decimals for quote token */
  quoteDecimals: number;
}

/** A single price quote from a DEX */
export interface PriceQuote {
  /** Which venue provided this quote */
  venue: Venue;
  /** The token pair */
  pair: TokenPair;
  /** Price in quote-per-base (e.g. SOL/USDC = 150.5 means 1 SOL = 150.5 USDC) */
  price: number;
  /** Bid price (if available) */
  bidPrice?: number;
  /** Ask price (if available) */
  askPrice?: number;
  /** Liquidity depth at this price (in quote token, e.g. USDC) */
  liquidity: number;
  /** Timestamp when quote was obtained */
  timestamp: number;
  /** Any error during fetch */
  error?: string;
}

/** An arbitrage opportunity detected across venues */
export interface ArbitrageOpportunity {
  /** The token pair involved */
  pair: TokenPair;
  /** Venue where the token is cheaper */
  buyVenue: Venue;
  /** Venue where the token is more expensive */
  sellVenue: Venue;
  /** Price on the buy venue (quote per base) */
  buyPrice: number;
  /** Price on the sell venue (quote per base) */
  sellPrice: number;
  /** Gross price difference in percent */
  priceSpreadPct: number;
  /** Gross profit in USD before costs */
  grossProfitUsd: number;
  /** Estimated transaction fee cost (lamports → USD) */
  txFeeUsd: number;
  /** Estimated DEX fees in USD */
  dexFeeUsd: number;
  /** Estimated slippage cost in USD */
  slippageCostUsd: number;
  /** Net profit in USD after all costs */
  netProfitUsd: number;
  /** Net profit as percentage of capital */
  netProfitPct: number;
  /** Direction of the trade */
  direction: ArbitrageDirection;
  /** Timestamp when opportunity was found */
  timestamp: number;
}

/** Configuration loaded from environment */
export interface BotConfig {
  heliosRpcUrl: string;
  walletJsonPath: string | undefined;
  jupiterApiUrl: string;
  dflowApiUrl: string;
  minProfitUsd: number;
  maxSlippageBps: number;
  simulationOnly: boolean;
  jitoTipLamports: number;
  /** Default trade size in USD */
  tradeSizeUsd: number;
  /** Poll interval in ms for watch mode */
  pollIntervalMs: number;
  /** Enable slow experimental direct on-chain venue fallbacks */
  enableOnchainFallback: boolean;
}

/** Result from executing a trade */
export interface TradeExecutionResult {
  success: boolean;
  opportunity: ArbitrageOpportunity;
  txSignature?: string;
  error?: string;
  actualProfitUsd?: number;
  simulationLogs?: string[];
}
