import { Connection } from '@solana/web3.js';
import { config } from '../config.js';

/** Singleton connection pool for Solana RPC */
let connection: Connection | null = null;

/**
 * Get or create a web3.js Connection instance
 * Uses Helius RPC URL from config for optimized performance
 */
export function getConnection(): Connection {
  if (!connection) {
    connection = new Connection(config.heliosRpcUrl, {
      commitment: 'confirmed',
      confirmTransactionInitialTimeout: 60_000,
      disableRetryOnRateLimit: false,
    });
  }
  return connection;
}

/**
 * Reset connection (useful on RPC errors to force reconnect)
 */
export function resetConnection(): void {
  connection = null;
}
