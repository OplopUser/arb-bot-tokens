import { Connection, PublicKey } from '@solana/web3.js';
import { config } from '../config.js';
import { PriceQuote, TokenPair, Venue } from '../types.js';
import { getConnection } from '../utils/connection.js';
import { fetchJupiterDexQuote } from './jupiterDex.js';

/**
 * Orca Price Fetcher
 *
 * Orca is a Solana DEX with both:
 * - Standard AMM pools (Whirlpools - concentrated liquidity)
 * - Legacy pools (Aquafarms)
 *
 * We use the @orca-so/whirlpools-sdk when available, with fallback
 * to direct on-chain queries for the core Whirlpool Accounts.
 *
 * Whirlpool account structure (simplified):
 * - tokenMintA, tokenMintB: token mints
 * - sqrtPrice: current √price as Q128.64
 * - liquidity: active liquidity
 * - tickCurrentIndex, tickSpacing, etc.
 */

// Orca Whirlpools program ID
const ORCA_WHIRLPOOL_PROGRAM = new PublicKey(
  'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
);

// Orca legacy pool program IDs
const ORCA_SWAP_PROGRAM = new PublicKey(
  '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP',
);

/**
 * Parse sqrt price from Q128.64 format to decimal price
 * Price = (sqrtPrice / 2^64)^2
 */
function sqrtPriceToPrice(sqrtPrice: bigint, decimalsA: number, decimalsB: number): number {
  const Q64 = BigInt(1) << BigInt(64);
  const priceFloat = Number(sqrtPrice) / Number(Q64);
  const price = priceFloat * priceFloat;
  // Adjust for decimal differences
  return price * Math.pow(10, decimalsA - decimalsB);
}

/**
 * Fetch Orca Whirlpool price for a given token pair
 */
export async function fetchOrcaQuote(pair: TokenPair): Promise<PriceQuote> {
  const timestamp = Date.now();

  try {
    const routedQuote = await fetchJupiterDexQuote(pair, Venue.Orca, [
      'Orca V1',
      'Orca V2',
      'XOrca',
    ]);
    if (routedQuote.price > 0 || !config.enableOnchainFallback) {
      return routedQuote;
    }

    const connection = getConnection();

    // Try Whirlpools first (concentrated liquidity)
    const whirlpoolQuote = await fetchWhirlpoolQuote(connection, pair);
    if (whirlpoolQuote && whirlpoolQuote.price > 0) {
      return { ...whirlpoolQuote, timestamp };
    }

    // Fallback: legacy Orca pools
    const legacyQuote = await fetchLegacyOrcaQuote(connection, pair);
    if (legacyQuote && legacyQuote.price > 0) {
      return { ...legacyQuote, timestamp };
    }

    return {
      venue: Venue.Orca,
      pair,
      price: 0,
      liquidity: 0,
      timestamp,
      error: 'No matching Orca pool found',
    };
  } catch (error: any) {
    return {
      venue: Venue.Orca,
      pair,
      price: 0,
      liquidity: 0,
      timestamp,
      error: error.message || 'Unknown error',
    };
  }
}

/**
 * Query Whirlpool accounts to find price for a token pair
 *
 * Whirlpool accounts are ~300 bytes.
 * Layout (offsets):
 *  0x00: 8  bytes - discriminator (WhirlpoolAccount::IDENT)
 *  0x08: 32 bytes - whirlpoolsConfig
 *  0x28: 2  bytes - tokenMintA (actually 32, skip to 0x48)
 *  0x48: 32 bytes - tokenMintA
 *  0x68: 32 bytes - tokenMintB
 *  0x88: 32 bytes - tokenVaultA
 *  0xa8: 32 bytes - tokenVaultB
 *  0xc8: 16 bytes - sqrtPrice (Q128.64)
 *  0xd8: 4  bytes - tickCurrentIndex
 *  0xdc: 2  bytes - tickSpacing
 *  ...
 */
async function fetchWhirlpoolQuote(
  connection: Connection,
  pair: TokenPair,
): Promise<PriceQuote | null> {
  try {
    // Fetch all whirlpool accounts (filtered by size)
    // In production, you'd want to cache pool addresses and use getMultipleAccounts
    const accounts = await connection.getProgramAccounts(
      ORCA_WHIRLPOOL_PROGRAM,
      {
        filters: [{ dataSize: 300 }],
        dataSlice: { offset: 0x48, length: 144 }, // mints + sqrtPrice + tick
      },
    );

    for (const { account } of accounts) {
      const data = account.data;
      if (data.length < 144) continue;

      const tokenMintA = new PublicKey(data.slice(0, 32));
      const tokenMintB = new PublicKey(data.slice(32, 64));
      const sqrtPriceBytes = data.slice(64, 80); // Q128.64

      // Check if this pool matches our pair (in either direction)
      const matchesAB =
        tokenMintA.equals(pair.baseMint) && tokenMintB.equals(pair.quoteMint);
      const matchesBA =
        tokenMintA.equals(pair.quoteMint) && tokenMintB.equals(pair.baseMint);

      if (!matchesAB && !matchesBA) continue;

      // Parse sqrt price as 128-bit big-endian (but Solana/Orca uses LE)
      const sqrtPrice = Buffer.from(sqrtPriceBytes).readBigUInt64LE(0);
      if (sqrtPrice === 0n) continue;

      let price: number;
      if (matchesAB) {
        price = sqrtPriceToPrice(sqrtPrice, pair.baseDecimals, pair.quoteDecimals);
      } else {
        price = 1 / sqrtPriceToPrice(sqrtPrice, pair.quoteDecimals, pair.baseDecimals);
      }

      return {
        venue: Venue.Orca,
        pair,
        price,
        liquidity: 0, // Would need to compute from tick data
        timestamp: Date.now(),
      };
    }

    return null;
  } catch {
    return null;
  }
}

/**
 * Legacy Orca pools (Aquafarms / token-swap pools)
 */
async function fetchLegacyOrcaQuote(
  connection: Connection,
  pair: TokenPair,
): Promise<PriceQuote | null> {
  try {
    // Legacy pools are typically ~250 bytes
    const accounts = await connection.getProgramAccounts(
      ORCA_SWAP_PROGRAM,
      {
        filters: [{ dataSize: 250 }],
        dataSlice: { offset: 0, length: 64 }, // enough for mints
      },
    );

    for (const { account } of accounts) {
      const data = account.data;
      if (data.length < 64) continue;

      // Layout varies by version; skip detailed parsing here
      // In production, use the Orca SDK for this
      const tokenA = new PublicKey(data.slice(0, 32));
      const tokenB = new PublicKey(data.slice(32, 64));

      if (
        (tokenA.equals(pair.baseMint) && tokenB.equals(pair.quoteMint)) ||
        (tokenA.equals(pair.quoteMint) && tokenB.equals(pair.baseMint))
      ) {
        return {
          venue: Venue.Orca,
          pair,
          price: 0,
          liquidity: 0,
          timestamp: Date.now(),
          error: 'Legacy pool found, need SDK for reserves',
        };
      }
    }

    return null;
  } catch {
    return null;
  }
}
