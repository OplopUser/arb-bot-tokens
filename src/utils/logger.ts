import chalk from 'chalk';

/** Log levels */
export type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'success';

/** Current log level filter */
let currentLogLevel: LogLevel = 'info';

const LOG_ORDER: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  success: 1,
  warn: 2,
  error: 3,
};

/**
 * Set the minimum log level
 */
export function setLogLevel(level: LogLevel): void {
  currentLogLevel = level;
}

/**
 * Format and output a log message with color
 */
function log(
  level: LogLevel,
  message: string,
  data?: Record<string, unknown>,
): void {
  if (LOG_ORDER[level] < LOG_ORDER[currentLogLevel]) return;

  const timestamp = new Date().toISOString().slice(11, 19);
  const prefix = getPrefix(level);
  const formattedData = data ? ' ' + JSON.stringify(data) : '';

  console.log(`${chalk.gray(`[${timestamp}]`)} ${prefix} ${message}${chalk.gray(formattedData)}`);
}

function getPrefix(level: LogLevel): string {
  switch (level) {
    case 'debug':
      return chalk.gray('🔍 DEBUG');
    case 'info':
      return chalk.blue('ℹ️  INFO');
    case 'success':
      return chalk.green('✅ DONE');
    case 'warn':
      return chalk.yellow('⚠️  WARN');
    case 'error':
      return chalk.red('❌ ERROR');
  }
}

export const logger = {
  debug: (msg: string, data?: Record<string, unknown>) => log('debug', msg, data),
  info: (msg: string, data?: Record<string, unknown>) => log('info', msg, data),
  success: (msg: string, data?: Record<string, unknown>) => log('success', msg, data),
  warn: (msg: string, data?: Record<string, unknown>) => log('warn', msg, data),
  error: (msg: string, data?: Record<string, unknown>) => log('error', msg, data),
};

/**
 * Pretty-print an arbitrage opportunity
 */
export function formatOpportunity(
  opp: {
    pair: { baseSymbol: string; quoteSymbol: string };
    buyVenue: string;
    sellVenue: string;
    buyPrice: number;
    sellPrice: number;
    priceSpreadPct: number;
    grossProfitUsd: number;
    netProfitUsd: number;
    netProfitPct: number;
  },
  index?: number,
): string {
  const header = index !== undefined
    ? chalk.bold(`\n📈 Opportunity #${index}`)
    : chalk.bold('\n📈 Arbitrage Opportunity');

  return [
    header,
    `  ${chalk.cyan(opp.pair.baseSymbol)}/${chalk.cyan(opp.pair.quoteSymbol)}`,
    `  Buy:  ${chalk.green(opp.buyVenue)} @ $${opp.buyPrice.toFixed(6)}`,
    `  Sell: ${chalk.red(opp.sellVenue)} @ $${opp.sellPrice.toFixed(6)}`,
    `  Spread: ${chalk.yellow(opp.priceSpreadPct.toFixed(3) + '%')}`,
    `  Gross Profit: ${chalk.green('$' + opp.grossProfitUsd.toFixed(4))}`,
    `  Net Profit:   ${opp.netProfitUsd > 0 ? chalk.green('$' + opp.netProfitUsd.toFixed(4)) : chalk.red('$' + opp.netProfitUsd.toFixed(4))}`,
    `  Net ROI: ${opp.netProfitPct > 0 ? chalk.green(opp.netProfitPct.toFixed(3) + '%') : chalk.red(opp.netProfitPct.toFixed(3) + '%')}`,
    '',
  ].join('\n');
}
