#!/usr/bin/env node

import { Command } from 'commander';
import chalk from 'chalk';
import { PublicKey } from '@solana/web3.js';
import { config, KNOWN_TOKENS } from './config.js';
import { TokenPair, Venue, ArbitrageOpportunity } from './types.js';
import { scanPair, scanPairs, scanPairWithBidAsk, getPriceSummary } from './scanner.js';
import { formatOpportunity, logger } from './utils/logger.js';

// ─── Types ───────────────────────────────────────────────────────────────────

interface ScanOptions {
  pair: string;
  venue?: string[];
  all?: boolean;
  bidAsk?: boolean;
}

interface WatchOptions {
  pair: string;
  interval?: string;
  venue?: string[];
}

// ─── CLI Setup ───────────────────────────────────────────────────────────────

const program = new Command();

program
  .name('sol-arb')
  .description('Solana Arbitrage Bot - Multi-DEX opportunity scanner & trader')
  .version('1.0.0');

// ─── Helper Functions ────────────────────────────────────────────────────────

/**
 * Create a TokenPair from "SOL-USDC" format string
 */
function parsePairString(pairStr: string): TokenPair {
  const [baseSymbol, quoteSymbol] = pairStr.split(/[-/]/).map((s) => s.trim().toUpperCase());

  if (!baseSymbol || !quoteSymbol) {
    throw new Error(
      `Invalid pair format: "${pairStr}". Use format like "SOL-USDC" or "SOL/USDC"`,
    );
  }

  const baseToken = KNOWN_TOKENS.find((t) => t.symbol === baseSymbol);
  const quoteToken = KNOWN_TOKENS.find((t) => t.symbol === quoteSymbol);

  if (!baseToken) {
    throw new Error(`Unknown token: ${baseSymbol}. Use 'sol-arb tokens' to list known tokens.`);
  }
  if (!quoteToken) {
    throw new Error(`Unknown token: ${quoteSymbol}. Use 'sol-arb tokens' to list known tokens.`);
  }

  return {
    baseSymbol: baseToken.symbol,
    quoteSymbol: quoteToken.symbol,
    baseMint: new PublicKey(baseToken.mint),
    quoteMint: new PublicKey(quoteToken.mint),
    baseDecimals: baseToken.decimals,
    quoteDecimals: quoteToken.decimals,
  };
}

/**
 * Parse venue names from CLI args
 */
function parseVenues(venueStrs?: string[]): Venue[] | undefined {
  if (!venueStrs || venueStrs.length === 0) return undefined;

  return venueStrs.map((v) => {
    const lower = v.toLowerCase().trim();
    const match = Object.values(Venue).find((vn) => vn === lower);
    if (!match) {
      throw new Error(
        `Unknown venue: "${v}". Valid: ${Object.values(Venue).join(', ')}`,
      );
    }
    return match;
  });
}

/**
 * Print a price summary table for a pair
 */
async function printPriceSummary(
  pair: TokenPair,
  venues?: Venue[],
): Promise<void> {
  logger.info(`\n📊 Price Summary for ${pair.baseSymbol}/${pair.quoteSymbol}`);
  logger.info(`${'─'.repeat(50)}`);

  const summary = await getPriceSummary(pair, venues);

  if (summary.size === 0) {
    logger.warn('No prices available from any venue');
    return;
  }

  // Find min and max for highlighting
  const entries = Array.from(summary.entries());
  const prices = entries.map(([, p]) => p);
  const minPrice = Math.min(...prices);
  const maxPrice = Math.max(...prices);

  for (const [venue, price] of entries) {
    const marker = price === minPrice
      ? chalk.green(' ← LOWEST')
      : price === maxPrice
        ? chalk.red(' ← HIGHEST')
        : '';
    const priceStr = price.toFixed(6);
    console.log(`  ${chalk.cyan(venue.padEnd(10))} $${priceStr}${marker}`);
  }

  if (maxPrice > minPrice) {
    const spread = ((maxPrice - minPrice) / minPrice) * 100;
    console.log(`\n  ${chalk.yellow(`Max spread: ${spread.toFixed(4)}%`)}`);
  }
  console.log();
}

/**
 * Display opportunities in a formatted way
 */
function displayOpportunities(
  opportunities: ArbitrageOpportunity[],
  pairLabel: string,
): void {
  if (opportunities.length === 0) {
    logger.info(`No profitable arbitrage for ${pairLabel}`);
    return;
  }

  logger.success(
    `Found ${opportunities.length} arbitrage opportunity(ies) for ${pairLabel}`,
  );

  opportunities.forEach((opp, i) => {
    console.log(formatOpportunity(opp, i + 1));
  });
}

// ─── Scan Command ────────────────────────────────────────────────────────────

program
  .command('scan')
  .description('Scan a token pair for arbitrage opportunities')
  .requiredOption('-p, --pair <pair>', 'Token pair (e.g., SOL-USDC)')
  .option('-v, --venue <venues...>', 'Venues to scan (default: all)')
  .option('--bid-ask', 'Use Jupiter bid/ask spread analysis')
  .action(async (options: ScanOptions) => {
    try {
      const pair = parsePairString(options.pair);
      const venues = parseVenues(options.venue);

      await printPriceSummary(pair, venues);

      const opportunities = options.bidAsk
        ? await scanPairWithBidAsk(pair)
        : await scanPair(pair, venues);

      displayOpportunities(opportunities, `${pair.baseSymbol}/${pair.quoteSymbol}`);

      if (config.simulationOnly) {
        logger.info('🛡️  SIMULATION MODE - No transactions executed');
        logger.info('   Set SIMULATION_ONLY=false to enable execution');
      }
    } catch (error: any) {
      logger.error(error.message);
      process.exit(1);
    }
  });

// ─── Watch Command ───────────────────────────────────────────────────────────

program
  .command('watch')
  .description('Continuously monitor a pair for arbitrage opportunities')
  .requiredOption('-p, --pair <pair>', 'Token pair (e.g., SOL-USDC)')
  .option('-i, --interval <ms>', 'Poll interval in ms', config.pollIntervalMs.toString())
  .option('-v, --venue <venues...>', 'Venues to scan (default: all)')
  .action(async (options: WatchOptions) => {
    try {
      const pair = parsePairString(options.pair);
      const venues = parseVenues(options.venue);
      const interval = parseInt(options.interval || config.pollIntervalMs.toString(), 10);

      logger.info(`🎯 Watching ${pair.baseSymbol}/${pair.quoteSymbol} every ${interval}ms`);
      logger.info(`   Press Ctrl+C to stop\n`);

      if (config.simulationOnly) {
        logger.info('🛡️  SIMULATION MODE');
      }

      let scanCount = 0;

      // Continuous loop
      const runScan = async () => {
        scanCount++;
        const timestamp = new Date().toISOString();
        logger.info(`Scan #${scanCount} at ${timestamp}`);

        try {
          const opportunities = await scanPair(pair, venues);

          if (opportunities.length > 0) {
            displayOpportunities(opportunities, `${pair.baseSymbol}/${pair.quoteSymbol}`);
          }
        } catch (err: any) {
          logger.error(`Scan error: ${err.message}`);
        }
      };

      // Run immediately, then on interval
      await runScan();
      setInterval(runScan, interval);
    } catch (error: any) {
      logger.error(error.message);
      process.exit(1);
    }
  });

// ─── Scan-All Command ────────────────────────────────────────────────────────

program
  .command('scan-all')
  .description('Scan all known token pairs for arbitrage')
  .option('-v, --venue <venues...>', 'Venues to scan (default: all)')
  .option('--limit <n>', 'Limit number of pairs', '10')
  .action(async (options: { venue?: string[]; limit?: string }) => {
    try {
      const venues = parseVenues(options.venue);
      const limit = parseInt(options.limit || '10', 10);

      // Build pairs from known tokens (base = SOL paired with all quote tokens)
      const solToken = KNOWN_TOKENS.find((t) => t.symbol === 'SOL')!;
      const pairs: TokenPair[] = KNOWN_TOKENS
        .filter((t) => t.symbol !== 'SOL')
        .slice(0, limit)
        .map((quote) => ({
          baseSymbol: solToken.symbol,
          quoteSymbol: quote.symbol,
          baseMint: new PublicKey(solToken.mint),
          quoteMint: new PublicKey(quote.mint),
          baseDecimals: solToken.decimals,
          quoteDecimals: quote.decimals,
        }));

      logger.info(`Scanning ${pairs.length} token pairs for arbitrage...`);
      logger.info('');

      const results = await scanPairs(pairs, venues);

      let totalOpps = 0;
      for (const [key, opps] of results) {
        if (opps.length > 0) {
          console.log(chalk.bold(`\n${'═'.repeat(55)}`));
          displayOpportunities(opps, key);
          totalOpps += opps.length;
        } else {
          console.log(`  ${chalk.gray('✗')} ${key}: ${chalk.gray('no opportunities')}`);
        }
      }

      console.log(chalk.bold(`\n${'═'.repeat(55)}`));
      logger.success(`Scan complete: ${totalOpps} total opportunity(ies) found`);

      if (config.simulationOnly) {
        logger.info('🛡️  SIMULATION MODE');
      }
    } catch (error: any) {
      logger.error(error.message);
      process.exit(1);
    }
  });

// ─── Tokens Command ──────────────────────────────────────────────────────────

program
  .command('tokens')
  .description('List known token addresses')
  .option('--json', 'Output as JSON')
  .action((options: { json?: boolean }) => {
    if (options.json) {
      console.log(JSON.stringify(KNOWN_TOKENS, null, 2));
      return;
    }

    console.log(chalk.bold('\n📋 Known Tokens'));
    console.log(`${'─'.repeat(70)}`);
    console.log(
      `  ${chalk.cyan('Symbol'.padEnd(8))} ${chalk.cyan('Name'.padEnd(20))} ${chalk.cyan('Mint Address')}`,
    );
    console.log(`${'─'.repeat(70)}`);

    for (const token of KNOWN_TOKENS) {
      const mintShort = token.mint.slice(0, 4) + '...' + token.mint.slice(-4);
      console.log(
        `  ${chalk.yellow(token.symbol.padEnd(8))} ${token.name.padEnd(20)} ${mintShort}`,
      );
    }
    console.log(`${'─'.repeat(70)}`);
    console.log(`  Total: ${KNOWN_TOKENS.length} tokens\n`);
  });

// ─── Config Command ──────────────────────────────────────────────────────────

program
  .command('config')
  .description('Show current configuration')
  .action(() => {
    console.log(chalk.bold('\n⚙️  Configuration'));
    console.log(`${'─'.repeat(40)}`);
    console.log(`  ${chalk.cyan('HELIOS_RPC_URL')}:     ${config.heliosRpcUrl}`);
    console.log(`  ${chalk.cyan('MIN_PROFIT_USD')}:      $${config.minProfitUsd}`);
    console.log(`  ${chalk.cyan('MAX_SLIPPAGE_BPS')}:    ${config.maxSlippageBps}`);
    console.log(`  ${chalk.cyan('SIMULATION_ONLY')}:     ${config.simulationOnly}`);
    console.log(`  ${chalk.cyan('TRADE_SIZE_USD')}:      $${config.tradeSizeUsd}`);
    console.log(`  ${chalk.cyan('JITO_TIP_LAMPORTS')}:   ${config.jitoTipLamports}`);
    console.log(`  ${chalk.cyan('POLL_INTERVAL_MS')}:    ${config.pollIntervalMs}ms`);
    console.log(`  ${chalk.cyan('WALLET_CONFIGURED')}:   ${config.walletJsonPath ? chalk.green('yes') : chalk.red('no')}`);
    console.log();
  });

// ─── Main ────────────────────────────────────────────────────────────────────

program.addHelpText(
  'after',
  `
${chalk.bold('Examples:')}
  ${chalk.gray('# Scan SOL-USDC for arbitrage')}
  $ sol-arb scan -p SOL-USDC

  ${chalk.gray('# Scan with specific venues')}
  $ sol-arb scan -p SOL-USDC -v jupiter raydium orca

  ${chalk.gray('# Watch mode (continuous monitoring)')}
  $ sol-arb watch -p SOL-USDC -i 3000

  ${chalk.gray('# Scan all pairs')}
  $ sol-arb scan-all --limit 5

  ${chalk.gray('# Use with bid-ask spread analysis')}
  $ sol-arb scan -p SOL-USDC --bid-ask

  ${chalk.gray('# List available tokens')}
  $ sol-arb tokens

${chalk.bold('Environment:')}
  Copy .env.example to .env and configure:
  $ cp .env.example .env
  # Edit .env with your RPC URL and settings
`,
);

// ─── Parse ───────────────────────────────────────────────────────────────────

program.parseAsync(process.argv).catch((err) => {
  logger.error(err.message);
  process.exit(1);
});
