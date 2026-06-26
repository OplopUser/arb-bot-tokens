import { TokenPair, PriceQuote, ArbitrageOpportunity, Venue } from './types.js';
import { logger } from './utils/logger.js';
import { fetchJupiterQuote, fetchJupiterReverseQuote } from './fetchers/jupiter.js';
import { fetchRaydiumQuote } from './fetchers/raydium.js';
import { fetchOrcaQuote } from './fetchers/orca.js';
import { fetchMeteoraQuote } from './fetchers/meteora.js';
import { fetchDFlowQuote } from './fetchers/dflow.js';
import { calculateProfit } from './profit.js';

/**
 * Opportunity Scanner
 *
 * The core logic: for a given token pair, fetches prices from all configured
 * DEX venues, compares them, and detects arbitrage opportunities.
 *
 * Architecture:
 * 1. Price Fetchers run in parallel (one per venue)
 * 2. All returned quotes are standardized as PriceQuote
 * 3. Quotes are compared pairwise across venues
 * 4. Profitable discrepancies are returned as ArbitrageOpportunity
 */

/** Type for a price fetcher function */
type PriceFetcher = (pair: TokenPair) => Promise<PriceQuote>;

export interface FetchQuoteOptions {
  sequential?: boolean;
  delayMs?: number;
  stopOnRateLimit?: boolean;
}

/** All available price fetchers mapped by venue */
const PRICE_FETCHERS: Record<Venue, PriceFetcher> = {
  [Venue.Jupiter]: fetchJupiterQuote,
  [Venue.Raydium]: fetchRaydiumQuote,
  [Venue.Orca]: fetchOrcaQuote,
  [Venue.Meteora]: fetchMeteoraQuote,
  [Venue.DFlow]: fetchDFlowQuote,
};

/**
 * Fetch quotes from the requested venues.
 */
export async function fetchQuotes(
  pair: TokenPair,
  venues?: Venue[],
  options: FetchQuoteOptions = {},
): Promise<PriceQuote[]> {
  const activeVenues = venues ?? Object.values(Venue);

  if (options.sequential) {
    const quotes: PriceQuote[] = [];
    for (const [index, venue] of activeVenues.entries()) {
      const quote = await fetchVenueQuote(pair, venue);
      if (quote) quotes.push(quote);
      if (quote && options.stopOnRateLimit && isRateLimitError(quote.error)) {
        break;
      }
      if (options.delayMs && options.delayMs > 0 && index < activeVenues.length - 1) {
        await sleep(options.delayMs);
      }
    }
    return quotes;
  }

  const quotes = await Promise.all(
    activeVenues.map((venue) => fetchVenueQuote(pair, venue)),
  );
  return quotes.filter((q): q is PriceQuote => q !== null);
}

async function fetchVenueQuote(
  pair: TokenPair,
  venue: Venue,
): Promise<PriceQuote | null> {
  const fetcher = PRICE_FETCHERS[venue];
  if (!fetcher) {
    logger.warn(`No fetcher for venue: ${venue}`);
    return null;
  }

  return fetcher(pair).catch((err: Error) => {
    logger.error(`Fetcher error for ${venue}: ${err.message}`);
    return null;
  });
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRateLimitError(error: string | undefined): boolean {
  return Boolean(error && /rate limited|429/i.test(error));
}

/**
 * Convert valid quotes into profitable arbitrage opportunities.
 */
export function findOpportunitiesFromQuotes(
  validQuotes: PriceQuote[],
): ArbitrageOpportunity[] {
  const opportunities: ArbitrageOpportunity[] = [];

  for (let i = 0; i < validQuotes.length; i++) {
    for (let j = i + 1; j < validQuotes.length; j++) {
      const quoteA = validQuotes[i];
      const quoteB = validQuotes[j];

      // Direction 1: Buy on A, sell on B
      if (quoteA.price < quoteB.price) {
        const opp = calculateProfit(quoteA, quoteB);
        if (opp) opportunities.push(opp);
      }

      // Direction 2: Buy on B, sell on A
      if (quoteB.price < quoteA.price) {
        const opp = calculateProfit(quoteB, quoteA);
        if (opp) opportunities.push(opp);
      }
    }
  }

  return opportunities.sort((a, b) => b.netProfitUsd - a.netProfitUsd);
}

/**
 * Scan all venues for a single token pair
 *
 * @param pair - The token pair to scan
 * @param venues - Optional subset of venues to scan (default: all)
 * @returns Array of profitable arbitrage opportunities sorted by net profit
 */
export async function scanPair(
  pair: TokenPair,
  venues?: Venue[],
): Promise<ArbitrageOpportunity[]> {
  const activeVenues = venues ?? Object.values(Venue);
  logger.info(`Scanning ${pair.baseSymbol}/${pair.quoteSymbol} across ${activeVenues.length} venues...`);

  // ── Phase 1: Fetch all quotes in parallel ─────────────────────────────
  const quotes = await fetchQuotes(pair, activeVenues);
  const validQuotes = quotes.filter(
    (q): q is PriceQuote => q !== null && q.price > 0,
  );

  logger.info(
    `Got ${validQuotes.length}/${activeVenues.length} valid quotes`,
  );

  if (validQuotes.length < 2) {
    logger.warn('Need at least 2 valid quotes to detect arbitrage');
    return [];
  }

  // ── Phase 2: Pairwise comparison ──────────────────────────────────────
  const opportunities = findOpportunitiesFromQuotes(validQuotes);

  if (opportunities.length > 0) {
    logger.success(
      `Found ${opportunities.length} arbitrage opportunities!`,
      { best: `$${opportunities[0].netProfitUsd.toFixed(4)}`, pair: `${pair.baseSymbol}/${pair.quoteSymbol}` },
    );
  } else {
    logger.info('No profitable opportunities found');
  }

  return opportunities;
}

/**
 * Scan a single pair and also try the reverse direction with Jupiter
 * to get accurate bid-ask spreads
 */
export async function scanPairWithBidAsk(
  pair: TokenPair,
): Promise<ArbitrageOpportunity[]> {
  await fetchJupiterQuote(pair);
  await fetchJupiterReverseQuote(pair);

  return scanPair(pair);
}

/**
 * Scan multiple token pairs
 *
 * @param pairs - Array of token pairs to scan
 * @param venues - Optional venue subset
 * @returns Map of pair key to opportunities array
 */
export async function scanPairs(
  pairs: TokenPair[],
  venues?: Venue[],
): Promise<Map<string, ArbitrageOpportunity[]>> {
  const results = new Map<string, ArbitrageOpportunity[]>();

  for (const pair of pairs) {
    const key = `${pair.baseSymbol}/${pair.quoteSymbol}`;
    try {
      const opps = await scanPair(pair, venues);
      results.set(key, opps);
    } catch (error: unknown) {
      const msg = error instanceof Error ? error.message : String(error);
      logger.error(`Error scanning ${key}: ${msg}`);
      results.set(key, []);
    }
  }

  return results;
}

/**
 * Get a summary of all prices across venues for a pair
 */
export async function getPriceSummary(
  pair: TokenPair,
  venues?: Venue[],
): Promise<Map<Venue, number>> {
  const summary = new Map<Venue, number>();

  const quotes = await fetchQuotes(pair, venues);
  for (const quote of quotes) {
    if (quote.price > 0) {
      summary.set(quote.venue, quote.price);
    }
  }

  return summary;
}
