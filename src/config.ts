import dotenv from 'dotenv';
import { z } from 'zod';
import { BotConfig } from './types.js';

dotenv.config();

// ─── Schema ──────────────────────────────────────────────────────────────────

const envSchema = z.object({
  HELIOS_RPC_URL: z
    .string()
    .url()
    .default('https://api.mainnet-beta.solana.com'),
  SOLANA_WALLET_JSON: z.string().optional(),
  JUPITER_API_URL: z
    .string()
    .url()
    .default('https://lite-api.jup.ag/swap/v1'),
  DFLOW_API_URL: z
    .string()
    .url()
    .default('https://api.dflow.trade/v1'),
  MIN_PROFIT_USD: z
    .string()
    .transform(Number)
    .pipe(z.number().positive())
    .default('0.05'),
  MAX_SLIPPAGE_BPS: z
    .string()
    .transform(Number)
    .pipe(z.number().int().min(1).max(1000))
    .default('50'),
  SIMULATION_ONLY: z
    .string()
    .transform((v) => v === 'true' || v === '1')
    .default('true'),
  JITO_TIP_LAMPORTS: z
    .string()
    .transform(Number)
    .pipe(z.number().int().min(0))
    .default('5000'),
  TRADE_SIZE_USD: z
    .string()
    .transform(Number)
    .pipe(z.number().positive())
    .default('100'),
  POLL_INTERVAL_MS: z
    .string()
    .transform(Number)
    .pipe(z.number().int().min(1000).max(60000))
    .default('5000'),
  ENABLE_ONCHAIN_FALLBACK: z
    .string()
    .transform((v) => v === 'true' || v === '1')
    .default('false'),
});

// ─── Parse ───────────────────────────────────────────────────────────────────

function loadConfig(): BotConfig {
  const parsed = envSchema.safeParse(process.env);

  if (!parsed.success) {
    console.error('❌ Invalid environment configuration:');
    for (const issue of parsed.error.issues) {
      console.error(`  - ${issue.path.join('.')}: ${issue.message}`);
    }
    console.error('\n⚠️  Using defaults for missing/incorrect values.');
  }

  const env = parsed.success ? parsed.data : envSchema.parse({});

  return {
    heliosRpcUrl: env.HELIOS_RPC_URL,
    walletJsonPath: env.SOLANA_WALLET_JSON || undefined,
    jupiterApiUrl: env.JUPITER_API_URL,
    dflowApiUrl: env.DFLOW_API_URL,
    minProfitUsd: env.MIN_PROFIT_USD,
    maxSlippageBps: env.MAX_SLIPPAGE_BPS,
    simulationOnly: env.SIMULATION_ONLY,
    jitoTipLamports: env.JITO_TIP_LAMPORTS,
    tradeSizeUsd: env.TRADE_SIZE_USD,
    pollIntervalMs: env.POLL_INTERVAL_MS,
    enableOnchainFallback: env.ENABLE_ONCHAIN_FALLBACK,
  };
}

/** Singleton config instance */
export const config: BotConfig = loadConfig();

/** Well-known token addresses on Solana mainnet */
export const KNOWN_TOKENS = [
  {
    symbol: 'SOL',
    name: 'Solana',
    mint: 'So11111111111111111111111111111111111111112',
    decimals: 9,
  },
  {
    symbol: 'USDC',
    name: 'USD Coin',
    mint: 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
    decimals: 6,
  },
  {
    symbol: 'USDT',
    name: 'Tether USD',
    mint: 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
    decimals: 6,
  },
  {
    symbol: 'JUP',
    name: 'Jupiter',
    mint: 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN',
    decimals: 6,
  },
  {
    symbol: 'RAY',
    name: 'Raydium',
    mint: '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R',
    decimals: 6,
  },
  {
    symbol: 'ORCA',
    name: 'Orca',
    mint: 'orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE',
    decimals: 6,
  },
  {
    symbol: 'BONK',
    name: 'Bonk',
    mint: 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263',
    decimals: 5,
  },
  {
    symbol: 'WIF',
    name: 'dogwifhat',
    mint: 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm',
    decimals: 6,
  },
  {
    symbol: 'PYTH',
    name: 'Pyth Network',
    mint: 'HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3',
    decimals: 6,
  },
] as const;

export type KnownToken = (typeof KNOWN_TOKENS)[number];
