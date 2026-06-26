import axios from 'axios';
import { config } from '../config.js';
import { PriceQuote, TokenPair, Venue } from '../types.js';
import { quoteBaseAmountUnits } from './quoteAmount.js';

/**
 * Fetch a Jupiter quote constrained to one or more DEX labels.
 */
export async function fetchJupiterDexQuote(
  pair: TokenPair,
  venue: Venue,
  dexLabels: string[],
): Promise<PriceQuote> {
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
        dexes: dexLabels.join(','),
      },
      timeout: 10_000,
      headers: { Accept: 'application/json' },
    });

    const data = response.data;
    if (!data?.outAmount) {
      return emptyQuote(venue, pair, timestamp, 'No route found');
    }

    const outAmount = Number(data.outAmount) / Math.pow(10, pair.quoteDecimals);
    const inAmount = Number(data.inAmount ?? baseAmount) / Math.pow(10, pair.baseDecimals);
    const price = outAmount / inAmount;

    return {
      venue,
      pair,
      price,
      liquidity: estimateLiquidity(data, pair.quoteDecimals),
      timestamp,
    };
  } catch (error: any) {
    return emptyQuote(
      venue,
      pair,
      timestamp,
      error.response?.status === 429
        ? 'Jupiter rate limited (429)'
        : error.message || 'Unknown error',
    );
  }
}

function emptyQuote(
  venue: Venue,
  pair: TokenPair,
  timestamp: number,
  error: string,
): PriceQuote {
  return {
    venue,
    pair,
    price: 0,
    liquidity: 0,
    timestamp,
    error,
  };
}

function estimateLiquidity(data: any, quoteDecimals: number): number {
  const outAmount = Number(data?.outAmount);
  if (Number.isFinite(outAmount) && outAmount > 0) {
    return outAmount / Math.pow(10, quoteDecimals);
  }

  return 0;
}
