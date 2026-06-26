import { Connection, PublicKey } from '@solana/web3.js';
import { config } from '../config.js';
import { PriceQuote, TokenPair, Venue } from '../types.js';
import { getConnection } from '../utils/connection.js';
import { fetchJupiterDexQuote } from './jupiterDex.js';

/**
 * Raydium Price Fetcher
 *
 * Queries Raydium AMM and CLMM pool accounts on-chain via RPC.
 * Raydium is the largest DEX on Solana by volume.
 *
 * Integration approaches:
 * 1. Using @raydium-io/raydium-sdk-v2 SDK (preferred)
 * 2. Fallback: direct on-chain pool account parsing
 *
 * Pool types:
 * - AMM (Constant Product) v4: Standard CP pools
 * - CLMM (Concentrated Liquidity): Range-order pools
 * - CPMM (Constant Product Market Maker): Newer CP pools
 */

// Raydium program IDs on mainnet
const RAYDIUM_AMM_V4_PROGRAM = new PublicKey(
  '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
);
const RAYDIUM_CLMM_PROGRAM = new PublicKey(
  'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK',
);
const RAYDIUM_CPMM_PROGRAM = new PublicKey(
  'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C',
);

/**
 * Fetch Raydium AMM pool price by finding matching pool on-chain
 * Uses web3.js to read pool accounts via getProgramAccounts
 */
export async function fetchRaydiumQuote(pair: TokenPair): Promise<PriceQuote> {
  const timestamp = Date.now();

  try {
    const routedQuote = await fetchJupiterDexQuote(pair, Venue.Raydium, [
      'Raydium',
      'Raydium CLMM',
      'Raydium CP',
      'Raydium Launchlab',
    ]);
    if (routedQuote.price > 0 || !config.enableOnchainFallback) {
      return routedQuote;
    }

    const connection = getConnection();

    // Try AMM v4 pools first
    const ammQuote = await fetchAmmV4Quote(connection, pair);
    if (ammQuote && ammQuote.price > 0) return { ...ammQuote, timestamp };

    // Fallback: try CLMM pools
    const clmmQuote = await fetchClmmQuote(connection, pair);
    if (clmmQuote && clmmQuote.price > 0) return { ...clmmQuote, timestamp };

    // Fallback: try CPMM pools
    const cpmmQuote = await fetchCpmmQuote(connection, pair);
    if (cpmmQuote && cpmmQuote.price > 0) return { ...cpmmQuote, timestamp };

    return {
      venue: Venue.Raydium,
      pair,
      price: 0,
      liquidity: 0,
      timestamp,
      error: 'No matching Raydium pool found',
    };
  } catch (error: any) {
    return {
      venue: Venue.Raydium,
      pair,
      price: 0,
      liquidity: 0,
      timestamp,
      error: error.message || 'Unknown error',
    };
  }
}

/**
 * Query AMM v4 pools to find one matching our token pair
 * Uses getProgramAccounts with dataSize and memcmp filters
 */
async function fetchAmmV4Quote(
  connection: Connection,
  pair: TokenPair,
): Promise<PriceQuote | null> {
  try {
    // AMM v4 pool accounts are ~752 bytes
    // We filter by account data size and search for matching token mints
    const accounts = await connection.getProgramAccounts(
      RAYDIUM_AMM_V4_PROGRAM,
      {
        filters: [{ dataSize: 752 }],
        dataSlice: { offset: 0x28, length: 64 }, // enough for both mints
      },
    );

    for (const { account } of accounts) {
      const data = account.data;
      if (data.length < 64) continue;

      const poolBaseMint = new PublicKey(data.slice(0, 32));
      const poolQuoteMint = new PublicKey(data.slice(32, 64));

      if (
        poolBaseMint.equals(pair.baseMint) &&
        poolQuoteMint.equals(pair.quoteMint)
      ) {
        // We found the pool — now we need to fetch the full account to get reserves
        // This requires additional RPC calls to vault accounts
        // For now, estimate price via the SDK if available, or return placeholder
        return {
          venue: Venue.Raydium,
          pair,
          price: 0, // Will be filled by detailed pool fetch
          liquidity: 0,
          timestamp: Date.now(),
          error: 'Pool found, need detailed fetch',
        };
      }
    }

    return null;
  } catch {
    return null;
  }
}

/**
 * Fallback: Query CLMM (Concentrated Liquidity) pools
 */
async function fetchClmmQuote(
  connection: Connection,
  pair: TokenPair,
): Promise<PriceQuote | null> {
  try {
    // CLMM pool accounts are ~450 bytes
    const accounts = await connection.getProgramAccounts(
      RAYDIUM_CLMM_PROGRAM,
      {
        filters: [{ dataSize: 450 }],
        dataSlice: { offset: 0, length: 64 },
      },
    );

    for (const { account } of accounts) {
      const data = account.data;
      if (data.length < 64) continue;

      const poolBaseMint = new PublicKey(data.slice(0, 32));
      const poolQuoteMint = new PublicKey(data.slice(32, 64));

      if (
        poolBaseMint.equals(pair.baseMint) &&
        poolQuoteMint.equals(pair.quoteMint)
      ) {
        return {
          venue: Venue.Raydium,
          pair,
          price: 0,
          liquidity: 0,
          timestamp: Date.now(),
          error: 'CLMM pool found, need detailed fetch',
        };
      }
    }

    return null;
  } catch {
    return null;
  }
}

/**
 * Fallback: CPMM pools
 */
async function fetchCpmmQuote(
  connection: Connection,
  pair: TokenPair,
): Promise<PriceQuote | null> {
  try {
    const accounts = await connection.getProgramAccounts(
      RAYDIUM_CPMM_PROGRAM,
      {
        filters: [{ dataSize: 400 }],
        dataSlice: { offset: 0, length: 64 },
      },
    );

    for (const { account } of accounts) {
      const data = account.data;
      if (data.length < 64) continue;

      const poolBaseMint = new PublicKey(data.slice(0, 32));
      const poolQuoteMint = new PublicKey(data.slice(32, 64));

      if (
        poolBaseMint.equals(pair.baseMint) &&
        poolQuoteMint.equals(pair.quoteMint)
      ) {
        return {
          venue: Venue.Raydium,
          pair,
          price: 0,
          liquidity: 0,
          timestamp: Date.now(),
          error: 'CPMM pool found, need detailed fetch',
        };
      }
    }

    return null;
  } catch {
    return null;
  }
}
