import axios from 'axios';
import { config } from '../config.js';
import { PriceQuote, TokenPair, Venue } from '../types.js';
import { quoteBaseAmountUnits } from './quoteAmount.js';

/**
 * Jupiter Price Fetcher
 *
 * Uses the Jupiter Swap quote API to get price quotes for token pairs.
 * Jupiter aggregates liquidity across many Solana DEXs, so this gives us
 * a reference price and execution path.
 *
 * API: GET /quote
 * Docs: https://dev.jup.ag/docs/swap/get-quote
 */
export async function fetchJupiterQuote(pair: TokenPair): Promise<PriceQuote> {
  const timestamp = Date.now();

  try {
    const baseAmount = quoteBaseAmountUnits(pair);

    const response = await axios.get(`${config.jupiterApiUrl}/quote`, {
      params: {
        inputMint: pair.baseMint.toBase58(),
        outputMint: pair.quoteMint.toBase58(),
        amount: baseAmount,
        slippageBps: config.maxSlippageBps,
        onlyDirectRoutes: false,
        asLegacyTransaction: false,
      },
      timeout: 10000,
      headers: { Accept: 'application/json' },
    });

    const data = response.data;

    if (!data || !data.outAmount) {
      return {
        venue: Venue.Jupiter,
        pair,
        price: 0,
        liquidity: 0,
        timestamp,
        error: 'No route found',
      };
    }

    // outAmount is in quote token decimal units
    const outAmount = Number(data.outAmount) / Math.pow(10, pair.quoteDecimals);
    const inAmount = Number(data.inAmount ?? baseAmount) / Math.pow(10, pair.baseDecimals);
    const price = outAmount / inAmount;

    // Route info for liquidity estimation
    const liquidity = estimateLiquidityFromRoutes(data);

    return {
      venue: Venue.Jupiter,
      pair,
      price,
      liquidity,
      timestamp,
    };
  } catch (error: any) {
    return {
      venue: Venue.Jupiter,
      pair,
      price: 0,
      liquidity: 0,
      timestamp,
      error: error.response?.status === 429
        ? 'Jupiter rate limited (429)'
        : error.message || 'Unknown error',
    };
  }
}

/**
 * Estimate liquidity from Jupiter route info
 * This gives us a rough idea of the depth available
 */
function estimateLiquidityFromRoutes(data: any): number {
  try {
    // If we have route info, use the total inAmount as a liquidity proxy
    if (data.routePlan && Array.isArray(data.routePlan)) {
      const totalIn = Number(data.inAmount) || 0;
      return totalIn / 1e9; // Rough USD estimate
    }
  } catch {
    // fall through
  }
  return 0;
}

/**
 * Get the reverse quote (swap quote -> base) to detect spreads
 */
export async function fetchJupiterReverseQuote(
  pair: TokenPair,
): Promise<PriceQuote> {
  const timestamp = Date.now();

  try {
    // Request 1 USD equivalent of quote token
    const quoteAmount = Math.pow(10, pair.quoteDecimals - 2); // 0.01 quote token

    const response = await axios.get(`${config.jupiterApiUrl}/quote`, {
      params: {
        inputMint: pair.quoteMint.toBase58(),
        outputMint: pair.baseMint.toBase58(),
        amount: quoteAmount.toString(),
        slippageBps: config.maxSlippageBps,
        onlyDirectRoutes: false,
      },
      timeout: 10000,
    });

    const data = response.data;

    if (!data || !data.outAmount) {
      return {
        venue: Venue.Jupiter,
        pair,
        price: 0,
        liquidity: 0,
        timestamp,
        error: 'No reverse route found',
      };
    }

    // For reverse quote, we want base/quote price
    const outAmount = Number(data.outAmount) / Math.pow(10, pair.baseDecimals);
    const inAmount = Number(data.inAmount) / Math.pow(10, pair.quoteDecimals);
    const reversePrice = outAmount / inAmount; // base per quote
    const price = 1 / reversePrice; // quote per base

    const liquidity = estimateLiquidityFromRoutes(data);

    return {
      venue: Venue.Jupiter,
      pair,
      price,
      liquidity,
      timestamp,
    };
  } catch (error: any) {
    return {
      venue: Venue.Jupiter,
      pair,
      price: 0,
      liquidity: 0,
      timestamp,
      error: error.message || 'Unknown error',
    };
  }
}
