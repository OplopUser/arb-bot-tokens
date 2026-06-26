import { TokenPair } from '../types.js';

const SAMPLE_BASE_AMOUNTS: Record<string, number> = {
  SOL: 0.01,
  JUP: 10,
  RAY: 5,
  ORCA: 1,
  WIF: 1,
  BONK: 100_000,
  PYTH: 10,
};

/**
 * Use a token-aware quote size so low-priced assets do not request dust routes.
 */
export function quoteBaseAmountUnits(pair: TokenPair): string {
  const tokenAmount = SAMPLE_BASE_AMOUNTS[pair.baseSymbol.toUpperCase()] ?? 1;
  const atomicAmount = Math.floor(tokenAmount * Math.pow(10, pair.baseDecimals));
  return Math.max(1, atomicAmount).toString();
}
