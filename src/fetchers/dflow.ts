import axios from 'axios';
import { config } from '../config.js';
import { PriceQuote, TokenPair, Venue } from '../types.js';

/**
 * DFlow Price Fetcher
 *
 * DFlow is a decentralized exchange aggregator/order book on Solana.
 * They provide a REST API for price quotes.
 *
 * Integration: REST API
 * - If DFLOW_API_URL is configured, we use their quote endpoint
 * - Fallback: on-chain program query if API is unavailable
 */

/**
 * Fetch a price quote from DFlow API or on-chain
 */
export async function fetchDFlowQuote(pair: TokenPair): Promise<PriceQuote> {
  const timestamp = Date.now();

  try {
    // Try REST API first
    const apiQuote = await fetchDFlowApiQuote(pair);
    if (apiQuote && apiQuote.price > 0) return { ...apiQuote, timestamp };

    // Fallback: try on-chain program
    const onChainQuote = await fetchDFlowOnChainQuote();
    if (onChainQuote && onChainQuote.price > 0) return { ...onChainQuote, timestamp };

    return {
      venue: Venue.DFlow,
      pair,
      price: 0,
      liquidity: 0,
      timestamp,
      error: 'No quote available from DFlow',
    };
  } catch (error: any) {
    return {
      venue: Venue.DFlow,
      pair,
      price: 0,
      liquidity: 0,
      timestamp,
      error: error.message || 'Unknown error',
    };
  }
}

/**
 * Query DFlow REST API for a price quote
 *
 * Typical DFlow API endpoints:
 * - GET /quote?inputMint=...&outputMint=...&amount=...&slippageBps=...
 * - Returns: { inputAmount, outputAmount, price, routes, ... }
 */
async function fetchDFlowApiQuote(
  pair: TokenPair,
): Promise<PriceQuote | null> {
  try {
    // Request quote for 0.01 base token
    const baseAmount = Math.pow(10, pair.baseDecimals - 2);

    const response = await axios.get(`${config.dflowApiUrl}/quote`, {
      params: {
        inputMint: pair.baseMint.toBase58(),
        outputMint: pair.quoteMint.toBase58(),
        amount: baseAmount.toString(),
        slippageBps: config.maxSlippageBps,
      },
      timeout: 10000,
      headers: { Accept: 'application/json' },
    });

    const data = response.data;

    if (!data || !data.outputAmount) {
      return null;
    }

    const outAmount = Number(data.outputAmount) / Math.pow(10, pair.quoteDecimals);
    const inAmount = Number(baseAmount) / Math.pow(10, pair.baseDecimals);
    const price = outAmount / inAmount;

    return {
      venue: Venue.DFlow,
      pair,
      price,
      liquidity: data.liquidity ? Number(data.liquidity) : 0,
      timestamp: Date.now(),
    };
  } catch (error: any) {
    // If API returns 404 or connection error, return null to try fallback
    if (error.response?.status === 404 || error.code === 'ECONNREFUSED') {
      return null;
    }
    throw error;
  }
}

/**
 * Fallback: Query DFlow on-chain state for price estimation
 *
 * DFlow on-chain order book stores:
 * - Market accounts with bid/ask depth
 * - Token mint addresses
 * - Current best bid and ask prices
 */
async function fetchDFlowOnChainQuote(): Promise<PriceQuote | null> {
  // On-chain DFlow integration would go here
  // For now, return null to indicate no quote available
  return null;
}
