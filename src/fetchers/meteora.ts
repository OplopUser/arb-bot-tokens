import { Connection, PublicKey } from '@solana/web3.js';
import { config } from '../config.js';
import { PriceQuote, TokenPair, Venue } from '../types.js';
import { getConnection } from '../utils/connection.js';
import { fetchJupiterDexQuote } from './jupiterDex.js';

/**
 * Meteora Price Fetcher
 *
 * Meteora (formerly known as "Meteora DLMM") provides Dynamic Liquidity
 * Market Maker pools on Solana. Their pools use concentrated liquidity
 * similar to Orca Whirlpools but with a dynamic fee model.
 *
 * Integration approaches:
 * 1. Direct on-chain pool account parsing (implemented here)
 * 2. Using Meteora SDK if available (@meteora-ag/dlmm)
 *
 * Pool Account Layout (DLMM):
 * - tokenXMint, tokenYMint
 * - binStep: u16
 * - activeBinId: i32
 * - sqrtPrice: Q64.64
 * - reserveX, reserveY
 * - fee info
 * - etc.
 */

// Meteora DLMM program ID on mainnet
const METEORA_DLMM_PROGRAM = new PublicKey(
  'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo',
);

// Meteora DLMM program ID (v2 if available)
const METEORA_DLMM_PROGRAM_V2 = new PublicKey(
  'ME27nBgwYEzHBfxiup6dKF5P7oKSftG5GGJrMej8dQV',
);

/**
 * DLMM pool constants
 */
const DLMM_POOL_SIZE = 840; // Typical pool account size in bytes

/**
 * Parse sqrt price from Q64.64 format to decimal price
 */
function sqrtPriceToPrice(
  sqrtPrice: bigint,
  decimalsX: number,
  decimalsY: number,
): number {
  const Q64 = BigInt(1) << BigInt(64);
  const priceFloat = Number(sqrtPrice) / Number(Q64);
  const price = priceFloat * priceFloat;
  // Adjust for decimal differences: price in Y per X
  return price * Math.pow(10, decimalsX - decimalsY);
}

/**
 * Fetch Meteora DLMM price quote
 */
export async function fetchMeteoraQuote(pair: TokenPair): Promise<PriceQuote> {
  const timestamp = Date.now();

  try {
    const routedQuote = await fetchJupiterDexQuote(pair, Venue.Meteora, [
      'Meteora',
      'Meteora DLMM',
      'Meteora DAMM v2',
    ]);
    if (routedQuote.price > 0 || !config.enableOnchainFallback) {
      return routedQuote;
    }

    const connection = getConnection();

    // Try v1 DLMM program
    const v1Quote = await fetchDlmmPoolPrice(connection, METEORA_DLMM_PROGRAM, pair);
    if (v1Quote && v1Quote.price > 0) return { ...v1Quote, timestamp };

    // Try v2 DLMM program
    const v2Quote = await fetchDlmmPoolPrice(connection, METEORA_DLMM_PROGRAM_V2, pair);
    if (v2Quote && v2Quote.price > 0) return { ...v2Quote, timestamp };

    return {
      venue: Venue.Meteora,
      pair,
      price: 0,
      liquidity: 0,
      timestamp,
      error: 'No matching Meteora DLMM pool found',
    };
  } catch (error: any) {
    return {
      venue: Venue.Meteora,
      pair,
      price: 0,
      liquidity: 0,
      timestamp,
      error: error.message || 'Unknown error',
    };
  }
}

/**
 * Query DLMM pools from a specific program to find a matching pair
 *
 * DLMM pool layout (Meteora):
 *   offset | size | field
 *   -------|------|-------
 *   0x000  | 8    | discriminator
 *   0x008  | 32   | lbPair
 *   0x028  | 1    | bump
 *   0x029  | 2    | binStep
 *   0x02b  | 4    | activeBinId
 *   0x02f  | 1    | status
 *   0x030  | 2    | padding
 *   0x032  | 16   | sqrtPrice (Q64.64)
 *   0x042  | 1    | padding
 *   0x043  | 1    | rewardInfosPaused
 *   0x044  | 8    | baseFeeRate
 *   0x04c  | 1    | padding
 *   0x04d  | 1    | protocolShare
 *   0x04e  | 8    | padding
 *   0x056  | 2    | padding
 *   0x058  | 32   | tokenXMint
 *   0x078  | 32   | tokenYMint
 *   0x098  | ...  | reserveX, reserveY, etc.
 */
async function fetchDlmmPoolPrice(
  connection: Connection,
  programId: PublicKey,
  pair: TokenPair,
): Promise<PriceQuote | null> {
  try {
    const accounts = await connection.getProgramAccounts(programId, {
      filters: [{ dataSize: DLMM_POOL_SIZE }],
      dataSlice: { offset: 0x32, length: 102 }, // sqrtPrice + mints + reserves
    });

    for (const { account } of accounts) {
      const data = account.data;
      if (data.length < 102) continue;

      // Parse sqrt price at offset 0 (relative to our slice)
      const sqrtPriceLE = data.readBigUInt64LE(0); // Q64.64 lower
      const sqrtPrice = sqrtPriceLE; // For Q64.64, the value fits in u64

      // Parse mints at offset 0x26 (0x58 - 0x32 = 0x26) from slice start
      const tokenXMint = new PublicKey(data.slice(0x26, 0x26 + 32));
      const tokenYMint = new PublicKey(data.slice(0x26 + 32, 0x26 + 64));

      if (sqrtPrice === 0n) continue;

      // Check if this pool matches (tokenX = base, tokenY = quote) or reversed
      const matchesXY =
        tokenXMint.equals(pair.baseMint) && tokenYMint.equals(pair.quoteMint);
      const matchesYX =
        tokenXMint.equals(pair.quoteMint) && tokenYMint.equals(pair.baseMint);

      if (!matchesXY && !matchesYX) continue;

      let price: number;
      if (matchesXY) {
        price = sqrtPriceToPrice(sqrtPrice, pair.baseDecimals, pair.quoteDecimals);
      } else {
        price = 1 / sqrtPriceToPrice(sqrtPrice, pair.quoteDecimals, pair.baseDecimals);
      }

      // Parse reserve amounts (at offset ~0x66 from slice start)
      const reserveX = data.readBigUInt64LE(0x66);
      const reserveY = data.readBigUInt64LE(0x6e);

      // Liquidity estimate in quote terms
      const liquidity = matchesXY
        ? Number(reserveY) / Math.pow(10, pair.quoteDecimals)
        : Number(reserveX) / Math.pow(10, pair.baseDecimals) * price;

      return {
        venue: Venue.Meteora,
        pair,
        price,
        liquidity,
        timestamp: Date.now(),
      };
    }

    return null;
  } catch {
    return null;
  }
}
