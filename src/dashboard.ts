#!/usr/bin/env node

import { createServer, IncomingMessage, ServerResponse } from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { Keypair, LAMPORTS_PER_SOL, PublicKey } from '@solana/web3.js';
import { config, KNOWN_TOKENS } from './config.js';
import { fetchQuotes, findOpportunitiesFromQuotes } from './scanner.js';
import { ArbitrageOpportunity, PriceQuote, TokenPair, Venue } from './types.js';
import { getConnection } from './utils/connection.js';
import { setLogLevel } from './utils/logger.js';

type Status = 'running' | 'paused' | 'scanning' | 'error';
type ActivityLevel = 'info' | 'success' | 'warn' | 'error';

interface ActivityItem {
  id: number;
  level: ActivityLevel;
  message: string;
  timestamp: string;
}

interface SerializableQuote {
  pair: string;
  venue: Venue;
  price: number;
  liquidity: number;
  timestamp: number;
  error: string | null;
}

interface SerializableOpportunity {
  pair: string;
  buyVenue: Venue;
  sellVenue: Venue;
  buyPrice: number;
  sellPrice: number;
  priceSpreadPct: number;
  grossProfitUsd: number;
  txFeeUsd: number;
  dexFeeUsd: number;
  slippageCostUsd: number;
  netProfitUsd: number;
  netProfitPct: number;
  timestamp: number;
}

interface WalletSnapshot {
  configured: boolean;
  publicKey: string | null;
  balanceSol: number | null;
  error: string | null;
}

interface SparkPoint {
  timestamp: number;
  maxSpreadPct: number;
  bestNetProfitUsd: number;
  validQuotes: number;
}

interface PairScanResult {
  pair: string;
  validQuotes: number;
  quoteCount: number;
  opportunityCount: number;
  bestNetProfitUsd: number | null;
  maxSpreadPct: number | null;
  lastError: string | null;
  quotes: SerializableQuote[];
  opportunities: SerializableOpportunity[];
}

interface DashboardSnapshot {
  status: Status;
  pair: string;
  pairs: string[];
  venues: Venue[];
  intervalMs: number;
  simulationOnly: boolean;
  executionStatus: 'read_only' | 'not_implemented';
  wallet: WalletSnapshot;
  scan: {
    cycle: number;
    inFlight: boolean;
    lastStartedAt: string | null;
    lastFinishedAt: string | null;
    lastError: string | null;
    validQuotes: number;
    quoteCount: number;
    opportunityCount: number;
    bestNetProfitUsd: number | null;
    maxSpreadPct: number | null;
  };
  quotes: SerializableQuote[];
  opportunities: SerializableOpportunity[];
  pairResults: PairScanResult[];
  activity: ActivityItem[];
  sparkline: SparkPoint[];
  cumulative: {
    pnlUsd: number;
    totalOpps: number;
    totalCycles: number;
  };
  tokens: Array<{ symbol: string; name: string }>;
}

const DEFAULT_PORT = 8787;
const DEFAULT_HOST = '127.0.0.1';
const DEFAULT_PAIRS = process.env.DASHBOARD_PAIRS
  || process.env.DASHBOARD_PAIR
  || 'SOL-USDC,JUP-USDC,RAY-USDC,ORCA-USDC,HYPE-USDC,TRUMP-USDC,FARTCOIN-USDC,WIF-USDC,BONK-USDC';
const DEFAULT_INTERVAL_MS = clampInt(
  process.env.DASHBOARD_INTERVAL_MS,
  60_000,
  15_000,
  300_000,
);
const DEFAULT_PAIR_CONCURRENCY = clampInt(
  process.env.DASHBOARD_PAIR_CONCURRENCY,
  1,
  1,
  4,
);
const DEFAULT_QUOTE_DELAY_MS = clampInt(
  process.env.DASHBOARD_QUOTE_DELAY_MS,
  1_000,
  0,
  10_000,
);

let pairLabels = parsePairList(DEFAULT_PAIRS);
let venues = parseVenueList(
  process.env.DASHBOARD_VENUES || 'jupiter,raydium,orca,meteora',
);
let intervalMs = DEFAULT_INTERVAL_MS;
let status: Status = 'paused';
let inFlight = false;
let timer: NodeJS.Timeout | null = null;
let activityId = 0;
let cycle = 0;
let lastStartedAt: string | null = null;
let lastFinishedAt: string | null = null;
let lastError: string | null = null;
let quotes: SerializableQuote[] = [];
let opportunities: SerializableOpportunity[] = [];
let pairResults: PairScanResult[] = [];
let wallet: WalletSnapshot = {
  configured: Boolean(config.walletJsonPath),
  publicKey: null,
  balanceSol: null,
  error: null,
};
let activity: ActivityItem[] = [];
let sparkline: SparkPoint[] = [];
let cumulativePnlUsd = 0;
let cumulativeOpps = 0;

setLogLevel('error');

function clampInt(
  raw: string | undefined,
  fallback: number,
  min: number,
  max: number,
): number {
  const parsed = raw ? Number.parseInt(raw, 10) : fallback;
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

function nowIso(): string {
  return new Date().toISOString();
}

function addActivity(level: ActivityLevel, message: string): void {
  activity = [
    {
      id: ++activityId,
      level,
      message,
      timestamp: nowIso(),
    },
    ...activity,
  ].slice(0, 80);
}

function parsePairString(pairStr: string): TokenPair {
  const [baseSymbol, quoteSymbol] = pairStr
    .split(/[-/]/)
    .map((s) => s.trim().toUpperCase());

  if (!baseSymbol || !quoteSymbol) {
    throw new Error(`Invalid pair: ${pairStr}`);
  }

  const baseToken = KNOWN_TOKENS.find((t) => t.symbol === baseSymbol);
  const quoteToken = KNOWN_TOKENS.find((t) => t.symbol === quoteSymbol);

  if (!baseToken) throw new Error(`Unknown base token: ${baseSymbol}`);
  if (!quoteToken) throw new Error(`Unknown quote token: ${quoteSymbol}`);

  return {
    baseSymbol: baseToken.symbol,
    quoteSymbol: quoteToken.symbol,
    baseMint: new PublicKey(baseToken.mint),
    quoteMint: new PublicKey(quoteToken.mint),
    baseDecimals: baseToken.decimals,
    quoteDecimals: quoteToken.decimals,
  };
}

function parsePairList(raw: string | string[] | undefined): string[] {
  const values = Array.isArray(raw)
    ? raw
    : String(raw || '')
      .split(/[\n,]+/)
      .map((pair) => pair.trim().toUpperCase())
      .filter(Boolean);

  if (values.length === 0) throw new Error('At least one pair is required');

  const uniquePairs = Array.from(new Set(values));
  for (const pair of uniquePairs) {
    parsePairString(pair);
  }

  return uniquePairs;
}

function parseVenueList(raw: string | string[] | undefined): Venue[] {
  const values = Array.isArray(raw)
    ? raw
    : String(raw || '')
      .split(',')
      .map((v) => v.trim())
      .filter(Boolean);

  if (values.length === 0) return [Venue.Jupiter];

  const parsed = values.map((value) => {
    const lower = value.toLowerCase();
    const match = Object.values(Venue).find((venueValue) => venueValue === lower);
    if (!match) throw new Error(`Unknown venue: ${value}`);
    return match;
  });

  return Array.from(new Set(parsed));
}

function serializeQuote(quote: PriceQuote): SerializableQuote {
  return {
    pair: `${quote.pair.baseSymbol}/${quote.pair.quoteSymbol}`,
    venue: quote.venue,
    price: quote.price,
    liquidity: quote.liquidity,
    timestamp: quote.timestamp,
    error: quote.error ?? null,
  };
}

function serializeOpportunity(opp: ArbitrageOpportunity): SerializableOpportunity {
  return {
    pair: `${opp.pair.baseSymbol}/${opp.pair.quoteSymbol}`,
    buyVenue: opp.buyVenue,
    sellVenue: opp.sellVenue,
    buyPrice: opp.buyPrice,
    sellPrice: opp.sellPrice,
    priceSpreadPct: opp.priceSpreadPct,
    grossProfitUsd: opp.grossProfitUsd,
    txFeeUsd: opp.txFeeUsd,
    dexFeeUsd: opp.dexFeeUsd,
    slippageCostUsd: opp.slippageCostUsd,
    netProfitUsd: opp.netProfitUsd,
    netProfitPct: opp.netProfitPct,
    timestamp: opp.timestamp,
  };
}

function resolveWalletPath(): string | null {
  if (!config.walletJsonPath) return null;
  return path.isAbsolute(config.walletJsonPath)
    ? config.walletJsonPath
    : path.resolve(process.cwd(), config.walletJsonPath);
}

function readWalletPublicKey(): string | null {
  const walletPath = resolveWalletPath();
  if (!walletPath) return null;

  const raw = JSON.parse(fs.readFileSync(walletPath, 'utf8')) as unknown;
  if (!Array.isArray(raw) || raw.length !== 64) {
    throw new Error('Wallet JSON is not a 64-byte keypair array');
  }

  const secret = Uint8Array.from(raw.map((value) => Number(value)));
  return Keypair.fromSecretKey(secret).publicKey.toBase58();
}

async function refreshWallet(): Promise<void> {
  try {
    const publicKey = readWalletPublicKey();
    if (!publicKey) {
      wallet = {
        configured: false,
        publicKey: null,
        balanceSol: null,
        error: null,
      };
      return;
    }

    const balanceLamports = await getConnection().getBalance(new PublicKey(publicKey));
    wallet = {
      configured: true,
      publicKey,
      balanceSol: balanceLamports / LAMPORTS_PER_SOL,
      error: null,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    wallet = {
      configured: Boolean(config.walletJsonPath),
      publicKey: wallet.publicKey,
      balanceSol: wallet.balanceSol,
      error: message,
    };
  }
}

function maxSpreadPct(validQuotes: SerializableQuote[]): number | null {
  const prices = validQuotes.map((quote) => quote.price).filter((price) => price > 0);
  if (prices.length < 2) return null;

  const minPrice = Math.min(...prices);
  const maxPrice = Math.max(...prices);
  return ((maxPrice - minPrice) / minPrice) * 100;
}

function maxPairSpreadPct(results: PairScanResult[]): number | null {
  const spreads = results
    .map((result) => result.maxSpreadPct)
    .filter((spread): spread is number => spread !== null);
  return spreads.length > 0 ? Math.max(...spreads) : null;
}

async function mapWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  worker: (item: T) => Promise<R>,
): Promise<R[]> {
  const results: R[] = [];
  let nextIndex = 0;

  async function runWorker(): Promise<void> {
    while (nextIndex < items.length) {
      const index = nextIndex++;
      results[index] = await worker(items[index]);
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(concurrency, items.length) }, () => runWorker()),
  );
  return results;
}

async function scanPairLabel(label: string): Promise<PairScanResult> {
  try {
    const pair = parsePairString(label);
    const rawQuotes = await fetchQuotes(pair, venues, {
      sequential: true,
      delayMs: DEFAULT_QUOTE_DELAY_MS,
      stopOnRateLimit: true,
    });
    const validQuotes = rawQuotes.filter((quote) => quote.price > 0);
    const rawOpportunities = findOpportunitiesFromQuotes(validQuotes);
    const pairQuotes = rawQuotes.map(serializeQuote);
    const pairOpportunities = rawOpportunities.map(serializeOpportunity);

    return {
      pair: `${pair.baseSymbol}/${pair.quoteSymbol}`,
      validQuotes: validQuotes.length,
      quoteCount: rawQuotes.length,
      opportunityCount: pairOpportunities.length,
      bestNetProfitUsd: pairOpportunities[0]?.netProfitUsd ?? null,
      maxSpreadPct: maxSpreadPct(pairQuotes.filter((quote) => quote.price > 0)),
      lastError: null,
      quotes: pairQuotes,
      opportunities: pairOpportunities,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return {
      pair: label,
      validQuotes: 0,
      quoteCount: 0,
      opportunityCount: 0,
      bestNetProfitUsd: null,
      maxSpreadPct: null,
      lastError: message,
      quotes: [],
      opportunities: [],
    };
  }
}

function skippedPairResult(label: string, message: string): PairScanResult {
  return {
    pair: label,
    validQuotes: 0,
    quoteCount: 0,
    opportunityCount: 0,
    bestNetProfitUsd: null,
    maxSpreadPct: null,
    lastError: message,
    quotes: [],
    opportunities: [],
  };
}

function isRateLimitedResult(result: PairScanResult): boolean {
  return result.quotes.some((quote) => isRateLimitError(quote.error))
    || isRateLimitError(result.lastError);
}

function isRateLimitError(error: string | null | undefined): boolean {
  return Boolean(error && /rate limited|429/i.test(error));
}

async function runScan(trigger: 'manual' | 'scheduled' | 'startup'): Promise<void> {
  if (inFlight) return;

  const previousStatus = status;
  inFlight = true;
  status = 'scanning';
  lastStartedAt = nowIso();
  lastError = null;

  try {
    const nextResults = DEFAULT_PAIR_CONCURRENCY === 1
      ? await scanPairsSequentially()
      : await mapWithConcurrency(pairLabels, DEFAULT_PAIR_CONCURRENCY, scanPairLabel);
    const nextQuotes = nextResults.flatMap((result) => result.quotes);
    const nextOpportunities = nextResults.flatMap((result) => result.opportunities);

    for (const result of nextResults) {
      if (result.lastError && !result.lastError.startsWith('Skipped:')) {
        addActivity('error', `${result.pair}: ${result.lastError}`);
      }
    }

    nextOpportunities.sort((a, b) => b.netProfitUsd - a.netProfitUsd);
    quotes = nextQuotes;
    opportunities = nextOpportunities;
    pairResults = nextResults;
    cycle += 1;
    cumulativePnlUsd += Math.max(0, opportunities[0]?.netProfitUsd ?? 0);
    cumulativeOpps += opportunities.length;
    lastFinishedAt = nowIso();

    const validQuoteCount = quotes.filter((quote) => quote.price > 0).length;
    const spread = maxPairSpreadPct(nextResults);
    const bestNetProfitUsd = opportunities[0]?.netProfitUsd ?? 0;

    sparkline = [
      ...sparkline,
      {
        timestamp: Date.now(),
        maxSpreadPct: spread ?? 0,
        bestNetProfitUsd,
        validQuotes: validQuoteCount,
      },
    ].slice(-80);

    await refreshWallet();

    if (opportunities.length > 0) {
      addActivity(
        'success',
        `${opportunities.length} opportunity found across ${pairLabels.length} pairs`,
      );
    } else if (validQuoteCount < 2) {
      addActivity(
        trigger === 'manual' ? 'warn' : 'info',
        `${validQuoteCount}/${pairLabels.length * venues.length} valid quotes`,
      );
    } else {
      addActivity('info', `No profitable spreads across ${pairLabels.length} pairs`);
    }

    status = previousStatus === 'paused' && trigger !== 'startup' ? 'paused' : 'running';
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    lastError = message;
    status = 'error';
    addActivity('error', message);
  } finally {
    inFlight = false;
  }
}

async function scanPairsSequentially(): Promise<PairScanResult[]> {
  const results: PairScanResult[] = [];

  for (const label of pairLabels) {
    const result = await scanPairLabel(label);
    results.push(result);

    if (isRateLimitedResult(result)) {
      const skipped = pairLabels.slice(results.length);
      results.push(
        ...skipped.map((pair) => skippedPairResult(pair, 'Skipped: Jupiter rate limited')),
      );
      addActivity('warn', 'Jupiter rate limited; stopped this scan cycle early');
      break;
    }
  }

  return results;
}

function startScanner(): void {
  if (timer) clearTimeout(timer);
  status = 'running';
  void runScan('manual').finally(scheduleNextScan);
  addActivity('info', 'Scanner started');
}

function scheduleNextScan(): void {
  if (status !== 'running') return;
  timer = setTimeout(() => {
    void runScan('scheduled').finally(scheduleNextScan);
  }, intervalMs);
}

function stopScanner(): void {
  if (timer) clearTimeout(timer);
  timer = null;
  status = 'paused';
  addActivity('info', 'Scanner paused');
}

function snapshot(): DashboardSnapshot {
  const validQuotes = quotes.filter((quote) => quote.price > 0);
  const bestNetProfitUsd = opportunities[0]?.netProfitUsd ?? null;

  return {
    status,
    pair: pairLabels[0] ?? '',
    pairs: pairLabels,
    venues,
    intervalMs,
    simulationOnly: config.simulationOnly,
    executionStatus: 'not_implemented',
    wallet,
    scan: {
      cycle,
      inFlight,
      lastStartedAt,
      lastFinishedAt,
      lastError,
      validQuotes: validQuotes.length,
      quoteCount: quotes.length,
      opportunityCount: opportunities.length,
      bestNetProfitUsd,
      maxSpreadPct: maxPairSpreadPct(pairResults),
    },
    quotes,
    opportunities,
    pairResults,
    activity,
    sparkline,
    cumulative: {
      pnlUsd: cumulativePnlUsd,
      totalOpps: cumulativeOpps,
      totalCycles: cycle,
    },
    tokens: KNOWN_TOKENS.map((token) => ({
      symbol: token.symbol,
      name: token.name,
    })),
  };
}

async function readJson(req: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  const body = Buffer.concat(chunks).toString('utf8').trim();
  return body ? JSON.parse(body) : {};
}

function sendJson(res: ServerResponse, body: unknown, statusCode = 200): void {
  res.writeHead(statusCode, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
  });
  res.end(JSON.stringify(body));
}

function sendHtml(res: ServerResponse): void {
  res.writeHead(200, {
    'Content-Type': 'text/html; charset=utf-8',
    'Cache-Control': 'no-store',
  });
  res.end(renderHtml());
}

function badRequest(res: ServerResponse, error: unknown): void {
  const message = error instanceof Error ? error.message : String(error);
  sendJson(res, { error: message }, 400);
}

async function handleApi(
  req: IncomingMessage,
  res: ServerResponse,
  pathname: string,
): Promise<void> {
  if (req.method === 'GET' && pathname === '/api/state') {
    sendJson(res, snapshot());
    return;
  }

  if (req.method === 'POST' && pathname === '/api/start') {
    startScanner();
    sendJson(res, snapshot());
    return;
  }

  if (req.method === 'POST' && pathname === '/api/stop') {
    stopScanner();
    sendJson(res, snapshot());
    return;
  }

  if (req.method === 'POST' && pathname === '/api/scan') {
    void runScan('manual');
    sendJson(res, snapshot());
    return;
  }

  if (req.method === 'POST' && pathname === '/api/settings') {
    try {
      const body = await readJson(req) as {
        pair?: string;
        pairs?: string[] | string;
        venues?: string[];
        intervalMs?: number;
      };

      const nextPairs = parsePairList(body.pairs || body.pair || pairLabels);
      const nextVenues = parseVenueList(body.venues || venues);
      const nextInterval = clampInt(
        body.intervalMs ? String(body.intervalMs) : undefined,
        intervalMs,
        15_000,
        300_000,
      );

      pairLabels = nextPairs;
      venues = nextVenues;
      intervalMs = nextInterval;
      addActivity('info', `Settings updated: ${pairLabels.length} pairs, ${venues.join(', ')}`);

      if (status === 'running') {
        if (timer) clearTimeout(timer);
        void runScan('manual').finally(scheduleNextScan);
      }

      sendJson(res, snapshot());
    } catch (error) {
      badRequest(res, error);
    }
    return;
  }

  sendJson(res, { error: 'Not found' }, 404);
}

function formatHostForUrl(host: string): string {
  return host === '0.0.0.0' ? '127.0.0.1' : host;
}

function parsePort(): number {
  const portArgIndex = process.argv.findIndex((arg) => arg === '--port' || arg === '-p');
  const raw = portArgIndex >= 0
    ? process.argv[portArgIndex + 1]
    : process.env.DASHBOARD_PORT;
  return clampInt(raw, DEFAULT_PORT, 1_024, 65_535);
}

function parseHost(): string {
  const hostArgIndex = process.argv.findIndex((arg) => arg === '--host' || arg === '-h');
  return hostArgIndex >= 0
    ? process.argv[hostArgIndex + 1] || DEFAULT_HOST
    : process.env.DASHBOARD_HOST || DEFAULT_HOST;
}

function renderHtml(): string {
  const venueOptions = Object.values(Venue)
    .map(function (v) { return '<label class="check"><input type="checkbox" name="venue" value="' + v + '"> ' + v + '</label>'; })
    .join('');

  return '<!doctype html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n<meta name="viewport" content="width=device-width,initial-scale=1">\n<title>Arbitrage Dashboard</title>\n<style>\n*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}\n:root{--bg:#f1f5f9;--surface:#fff;--border:#e2e8f0;--text:#0f172a;--text2:#64748b;--text3:#94a3b8;--green:#059669;--green-bg:#ecfdf5;--green-brd:#a7f3d0;--red:#dc2626;--red-bg:#fef2f2;--red-brd:#fecaca;--amber:#d97706;--amber-bg:#fffbeb;--blue:#2563eb;--blue-bg:#eff6ff;--ink:#0f172a;--r:8px}\nbody{font:13px/1.5 system-ui,-apple-system,sans-serif;background:var(--bg);color:var(--text)}\n.header{background:var(--ink);color:#f8fafc;position:sticky;top:0;z-index:100;box-shadow:0 2px 8px rgba(0,0,0,.12)}\n.header-inner{max-width:1400px;margin:0 auto;padding:10px 20px;display:flex;align-items:center;gap:20px;flex-wrap:wrap}\n.header-title{font-size:15px;font-weight:700;display:flex;align-items:center;gap:8px;white-space:nowrap}\n.dot{width:9px;height:9px;border-radius:50%;display:inline-block;flex-shrink:0}\n.dot.running{background:#22c55e}\n.dot.scanning{background:#22c55e;animation:blink 1s infinite}\n.dot.paused{background:#94a3b8}\n.dot.error{background:#ef4444}\n@keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}\n.header-line{font-size:11px;color:#94a3b8;display:flex;align-items:center;gap:12px;flex-wrap:wrap}\n.hstat{text-align:center;font-size:12px}\n.hstat-label{font-size:10px;text-transform:uppercase;color:#64748b;letter-spacing:.04em}\n.hstat-value{font-weight:700}\n.hstat-value.up{color:#34d399}\n.hstat-value.down{color:#fca5a5}\n.badge{padding:3px 10px;border-radius:20px;font-size:10px;font-weight:800;letter-spacing:.04em;text-transform:uppercase}\n.badge.sim{background:rgba(5,150,105,.2);color:#6ee7b7}\n.badge.live{background:rgba(220,38,38,.2);color:#fca5a5}\n.main{max-width:1400px;margin:0 auto;padding:16px 20px;display:flex;gap:16px;align-items:flex-start}\n.sidebar{width:280px;flex-shrink:0;display:flex;flex-direction:column;gap:12px}\n.card{background:var(--surface);border:1px solid var(--border);border-radius:var(--r);box-shadow:0 1px 3px rgba(0,0,0,.05)}\n.card-hd{padding:12px 14px 0;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.04em;color:var(--text2)}\n.card-bd{padding:10px 14px 14px}\n.field{margin-bottom:10px}\n.field:last-child{margin-bottom:0}\n.field-label{display:block;font-size:11px;font-weight:600;color:var(--text2);margin-bottom:3px}\n.input,.textarea{width:100%;padding:7px 9px;border:1px solid var(--border);border-radius:6px;font:inherit;font-size:12px;color:var(--text);background:#fff;transition:border-color .15s}\n.input:focus,.textarea:focus{border-color:var(--blue);outline:none}\n.textarea{min-height:110px;resize:vertical;line-height:1.5;font-family:ui-monospace,Menlo,monospace;font-size:11px}\n.venue-checks{display:grid;grid-template-columns:1fr 1fr;gap:5px}\n.check{display:flex;align-items:center;gap:5px;padding:5px 7px;border:1px solid var(--border);border-radius:5px;background:#f8fafc;font-size:11px;cursor:pointer;user-select:none}\n.check input{accent-color:var(--blue)}\n.btn{padding:7px 12px;border:1px solid var(--border);border-radius:6px;font:inherit;font-size:12px;font-weight:600;cursor:pointer;background:#fff;color:var(--text);transition:all .15s}\n.btn:hover{background:#f8fafc}\n.btn:disabled{opacity:.45;cursor:not-allowed}\n.btn-primary{background:var(--green);color:#fff;border-color:var(--green)}\n.btn-primary:hover{background:#047857}\n.btn-danger{color:var(--red);border-color:#fecaca}\n.btn-danger:hover{background:var(--red-bg)}\n.btn-scan{border-color:var(--blue);color:var(--blue)}\n.btn-scan:hover{background:var(--blue-bg)}\n.btn-group{display:grid;grid-template-columns:1fr 1fr;gap:6px}\n.wallet-key{font:11px/ui-monospace,Menlo,monospace;color:var(--text2);word-break:break-all}\n.wallet-bal{font-size:18px;font-weight:800;margin-top:2px}\n.workspace{flex:1;min-width:0;display:flex;flex-direction:column;gap:12px}\n.kpi-row{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}\n.kpi{background:var(--surface);border:1px solid var(--border);border-radius:var(--r);padding:14px;box-shadow:0 1px 3px rgba(0,0,0,.05)}\n.kpi.pnl-up{background:var(--green-bg);border-color:var(--green-brd)}\n.kpi.pnl-down{background:var(--red-bg);border-color:var(--red-brd)}\n.kpi-label{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.04em;color:var(--text2);margin-bottom:2px}\n.kpi-value{font-size:22px;font-weight:800;line-height:1.15}\n.kpi-sub{font-size:11px;color:var(--text2);margin-top:2px}\n.tab-bar{display:flex;border-bottom:2px solid var(--border)}\n.tab-btn{padding:8px 14px;border:none;background:none;font:inherit;font-size:12px;font-weight:600;color:var(--text2);cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-2px;transition:all .15s}\n.tab-btn:hover{color:var(--text)}\n.tab-btn.active{color:var(--blue);border-bottom-color:var(--blue)}\n.tab-panel{display:none}\n.tab-panel.active{display:block}\n.table-wrap{overflow-x:auto;padding:10px 0 0}\ntable{width:100%;border-collapse:collapse;font-size:12px}\nth{padding:7px 9px;text-align:left;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.03em;color:var(--text2);background:#f8fafc;border-bottom:2px solid var(--border);white-space:nowrap}\ntd{padding:7px 9px;border-bottom:1px solid var(--border);white-space:nowrap}\ntr:hover td{background:#f8fafc}\n.num{font-variant-numeric:tabular-nums;text-align:right}\n.good{color:var(--green);font-weight:700}\n.bad{color:var(--red);font-weight:700}\n.warn{color:var(--amber);font-weight:700}\n.muted{color:var(--text2)}\n.tag{display:inline-block;padding:1px 6px;border-radius:4px;font-weight:700;font-size:10px;text-transform:capitalize;background:#f1f5f9}\n.activity-feed{max-height:280px;overflow-y:auto}\n.act-item{padding:7px 10px;border-left:3px solid var(--blue);background:#f8fafc;margin-bottom:3px;border-radius:0 4px 4px 0;font-size:12px}\n.act-item.success{border-left-color:var(--green)}\n.act-item.warn{border-left-color:var(--amber)}\n.act-item.error{border-left-color:var(--red)}\n.act-time{color:var(--text2);font-size:10px}\n.chart-wrap{border:1px solid var(--border);border-radius:6px;overflow:hidden;height:180px;background:#fbfdfe}\ncanvas{width:100%;height:100%;display:block}\n.empty{padding:32px 16px;text-align:center;color:var(--text3)}\n.bottom-row{display:grid;grid-template-columns:1.2fr .8fr;gap:12px}\n@media(max-width:1100px){.main{flex-direction:column}.sidebar{width:100%}.bottom-row{grid-template-columns:1fr}}\n@media(max-width:680px){.kpi-row{grid-template-columns:repeat(2,1fr)}.header-inner{gap:10px}}\n</style>\n</head>\n<body>\n<header class="header">\n<div class="header-inner">\n<div class="header-title"><span class="dot paused" id="statusDot"></span>Arbitrage</div>\n<div class="header-line" id="headerLine">\n<span class="hstat"><span class="hstat-label">PnL</span> <span class="hstat-value" id="hdrPnl">--</span></span>\n<span class="hstat"><span class="hstat-label">Wallet</span> <span class="hstat-value" id="hdrWallet">--</span></span>\n<span class="hstat"><span class="hstat-label">Cycle</span> <span class="hstat-value" id="hdrCycle">--</span></span>\n<span class="hstat"><span class="hstat-label">Opps</span> <span class="hstat-value" id="hdrOpps">--</span></span>\n</div>\n<span class="badge sim" id="modeBadge">SIM</span>\n</div>\n</header>\n<main class="main">\n<aside class="sidebar">\n<div class="card"><div class="card-hd">Scanner</div><div class="card-bd">\n<label class="field"><span class="field-label">Watchlist (one per line)</span><textarea class="textarea" id="pairInput" autocomplete="off" rows="6">SOL-USDC\nJUP-USDC\nRAY-USDC\nORCA-USDC\nHYPE-USDC\nTRUMP-USDC\nFARTCOIN-USDC\nWIF-USDC\nBONK-USDC</textarea></label>\n<label class="field"><span class="field-label">Interval (ms)</span><input type="number" class="input" id="intervalInput" min="15000" max="300000" step="5000" value="60000"></label>\n<div class="field"><span class="field-label">Venues</span><div class="venue-checks" id="venueChecks">' + venueOptions + '</div></div>\n<div class="btn-group">\n<button class="btn btn-primary" id="saveButton">Save</button>\n<button class="btn btn-scan" id="scanButton">Scan</button>\n<button class="btn btn-primary" id="startButton">Start</button>\n<button class="btn btn-danger" id="stopButton">Stop</button>\n</div>\n</div></div>\n<div class="card"><div class="card-hd">Wallet</div><div class="card-bd">\n<div class="wallet-key" id="walletPubkey">--</div>\n<div class="wallet-bal" id="walletBalance">--</div>\n<div id="walletError" style="font-size:11px;color:var(--red);margin-top:4px"></div>\n</div></div>\n</aside>\n<section class="workspace">\n<div class="kpi-row">\n<div class="kpi"><div class="kpi-label">Status</div><div class="kpi-value" id="kpiStatus">--</div><div class="kpi-sub" id="kpiStatusSub"></div></div>\n<div class="kpi" id="kpiPnlCard"><div class="kpi-label">Cumulative PnL</div><div class="kpi-value" id="kpiPnl">--</div><div class="kpi-sub" id="kpiPnlSub"></div></div>\n<div class="kpi"><div class="kpi-label">Best Spread</div><div class="kpi-value" id="kpiSpread">--</div><div class="kpi-sub" id="kpiSpreadSub"></div></div>\n<div class="kpi"><div class="kpi-label">Quotes</div><div class="kpi-value" id="kpiQuotes">--</div><div class="kpi-sub" id="kpiQuotesSub"></div></div>\n</div>\n<div class="card">\n<div class="tab-bar">\n<button class="tab-btn active" data-tab="quotes">Live Quotes</button>\n<button class="tab-btn" data-tab="pairs">Pair Coverage</button>\n<button class="tab-btn" data-tab="opps">Trade Candidates</button>\n</div>\n<div class="tab-panel active" id="panelQuotes"><div class="table-wrap"><div class="empty">No quotes yet.</div></div></div>\n<div class="tab-panel" id="panelPairs"><div class="table-wrap"><div class="empty">No scans yet.</div></div></div>\n<div class="tab-panel" id="panelOpps"><div class="table-wrap"><div class="empty">No opportunities yet.</div></div></div>\n</div>\n<div class="bottom-row">\n<div class="card"><div class="card-hd">Spread History</div><div class="card-bd" style="padding:0"><div class="chart-wrap"><canvas id="sparkline" width="800" height="180"></canvas></div></div></div>\n<div class="card"><div class="card-hd">Activity</div><div class="card-bd" style="padding:8px 14px 12px"><div class="activity-feed" id="activityFeed"><div class="empty">No activity yet.</div></div></div></div>\n</div>\n</section>\n</main>\n<script>\n(function(){\nvar fmtUsd = new Intl.NumberFormat("en-US",{style:"currency",currency:"USD",maximumFractionDigits:4});\nvar fmtNum = new Intl.NumberFormat("en-US",{maximumFractionDigits:6});\nvar currentState = null;\nvar currentTab = "quotes";\n\nfunction esc(v){return String(v||"").replace(/[&<>"\'\\n]/g,function(c){return{"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;","\'":"&#39;"}[c]||c})}\nfunction shortKey(v){if(!v)return"--";return v.length>14?v.slice(0,6)+"..."+v.slice(-6):v}\nfunction cls(v){return v>=0?"good":"bad"}\n\nasync function api(path,body){\nvar init=body===undefined?{}:{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)};\nvar r=await fetch(path,init);\nvar d=await r.json();\nif(!r.ok)throw new Error(d.error||r.statusText);\nreturn d\n}\n\nfunction selectedVenues(){\nvar els=document.querySelectorAll("input[name=\\"venue\\"]:checked");\nreturn Array.from(els).map(function(e){return e.value})\n}\n\nfunction syncControls(s){\ndocument.getElementById("pairInput").value=s.pairs.join("\\n");\ndocument.getElementById("intervalInput").value=s.intervalMs;\ndocument.querySelectorAll("input[name=\\"venue\\"]").forEach(function(e){e.checked=s.venues.includes(e.value)})\n}\n\nfunction tableHtml(headers,rows){\nvar h="<table><thead><tr>";\nheaders.forEach(function(hdr){h+="<th>"+hdr+"</th>"});\nh+="</tr></thead><tbody>"+rows+"</tbody></table>";\nreturn h\n}\n\nfunction renderQuotes(quotes){\nif(quotes.length===0){document.getElementById("panelQuotes").innerHTML=\'<div class="table-wrap"><div class="empty">No quotes yet.</div></div>\';return}\nvar rows="";\nquotes.forEach(function(q){\nvar ok=q.price>0;\nrows+="<tr><td>"+esc(q.pair)+"</td><td><span class=\\"tag\\">"+esc(q.venue)+"</span></td><td class=\\"num\\">"+(ok?"$"+fmtNum.format(q.price):"--")+"</td><td class=\\"num\\">"+(q.liquidity>0?fmtNum.format(q.liquidity):"--")+"</td><td class=\\""+(ok?"good":"warn")+"\\">"+esc(ok?"valid":(q.error||"no quote"))+"</td></tr>"\n});\ndocument.getElementById("panelQuotes").innerHTML=\'<div class="table-wrap">\'+tableHtml(["Pair","Venue","Price","Liquidity","Status"],rows)+"</div>"\n}\n\nfunction renderPairs(results){\nif(results.length===0){document.getElementById("panelPairs").innerHTML=\'<div class="table-wrap"><div class="empty">No scans yet.</div></div>\';return}\nvar rows="";\nresults.forEach(function(r){\nvar complete=r.validQuotes>=2;\nvar pnlClass=r.bestNetProfitUsd===null?"muted":cls(r.bestNetProfitUsd);\nrows+="<tr><td>"+esc(r.pair)+"</td><td class=\\"num\\">"+r.validQuotes+"/"+r.quoteCount+"</td><td class=\\"num\\">"+(r.maxSpreadPct===null?"--":r.maxSpreadPct.toFixed(4)+"%")+"</td><td class=\\"num "+pnlClass+"\\">"+(r.bestNetProfitUsd===null?"--":fmtUsd.format(r.bestNetProfitUsd))+"</td><td class=\\""+(r.lastError?"bad":complete?"good":"warn")+"\\">"+esc(r.lastError||(complete?"ready":"need 2+ quotes"))+"</td></tr>"\n});\ndocument.getElementById("panelPairs").innerHTML=\'<div class="table-wrap">\'+tableHtml(["Pair","Quotes","Spread","Best Net","Status"],rows)+"</div>"\n}\n\nfunction renderOpps(opps){\nif(opps.length===0){document.getElementById("panelOpps").innerHTML=\'<div class="table-wrap"><div class="empty">No trade candidates found.</div></div>\';return}\nvar rows="";\nopps.forEach(function(o){\nvar pnlClass=cls(o.netProfitUsd);\nrows+="<tr><td>"+esc(o.pair)+"</td><td><span class=\\"tag\\">"+esc(o.buyVenue)+"</span> <span class=\\"muted\\">$"+fmtNum.format(o.buyPrice)+"</span></td><td><span class=\\"tag\\">"+esc(o.sellVenue)+"</span> <span class=\\"muted\\">$"+fmtNum.format(o.sellPrice)+"</span></td><td class=\\"num good\\">"+o.priceSpreadPct.toFixed(4)+"%</td><td class=\\"num "+pnlClass+"\\">"+fmtUsd.format(o.netProfitUsd)+"</td><td class=\\"num muted\\">"+fmtUsd.format(o.dexFeeUsd+o.slippageCostUsd+o.txFeeUsd)+"</td></tr>"\n});\ndocument.getElementById("panelOpps").innerHTML=\'<div class="table-wrap">\'+tableHtml(["Pair","Buy","Sell","Spread","Net","Costs"],rows)+"</div>"\n}\n\nfunction renderActivity(items){\nif(items.length===0){document.getElementById("activityFeed").innerHTML=\'<div class="empty">No activity yet.</div>\';return}\nvar h="";\nitems.forEach(function(a){\nvar t=new Date(a.timestamp).toLocaleTimeString();\nh+="<div class=\\"act-item "+a.level+"\\"><div class=\\"act-time\\">"+esc(t)+"</div><div>"+esc(a.message)+"</div></div>"\n});\ndocument.getElementById("activityFeed").innerHTML=h\n}\n\nfunction drawSparkline(points){\nvar c=document.getElementById("sparkline");\nvar ctx=c.getContext("2d");\nvar w=c.width,h=c.height;\nctx.clearRect(0,0,w,h);\nctx.fillStyle="#fbfdfe";\nctx.fillRect(0,0,w,h);\nctx.strokeStyle="#e2e8f0";\nctx.lineWidth=1;\nfor(var i=1;i<4;i++){var y=(h/4)*i;ctx.beginPath();ctx.moveTo(0,y);ctx.lineTo(w,y);ctx.stroke()}\nif(points.length<2)return;\nvar pad=16;\nvar maxSpread=Math.max(0.0001,Math.max.apply(null,points.map(function(p){return p.maxSpreadPct})));\nvar step=(w-pad*2)/Math.max(1,points.length-1);\nctx.strokeStyle="'+ '#2563eb' + '";\nctx.lineWidth=2;\nctx.beginPath();\npoints.forEach(function(p,i){\nvar x=pad+step*i;\nvar y=h-pad-(p.maxSpreadPct/maxSpread)*(h-pad*2);\nif(i===0)ctx.moveTo(x,y);else ctx.lineTo(x,y)\n});\nctx.stroke();\nvar last=points[points.length-1];\nctx.fillStyle="#0f172a";\nctx.font="11px ui-monospace,Menlo,monospace";\nctx.fillText("spread "+last.maxSpreadPct.toFixed(4)+"%",12,18);\nctx.fillText("quotes "+last.validQuotes,12,34)\n}\n\nfunction switchTab(name){\ncurrentTab=name;\ndocument.querySelectorAll(".tab-btn").forEach(function(b){b.classList.toggle("active",b.dataset.tab===name)});\ndocument.querySelectorAll(".tab-panel").forEach(function(p){p.classList.toggle("active",p.id==="panel"+name.charAt(0).toUpperCase()+name.slice(1))})\n}\n\nfunction render(s){\ncurrentState=s;\nvar pairs=s.pairs.length;var venues=s.venues.length;\ndocument.getElementById("headerLine").innerHTML=\n\'<span class="hstat"><span class="hstat-label">PnL</span> <span class="hstat-value \\"\'+(s.cumulative.pnlUsd>=0?"up":"down")+\'">\'+fmtUsd.format(s.cumulative.pnlUsd)+\'</span></span>\'+\n\'<span class="hstat"><span class="hstat-label">Wallet</span> <span class="hstat-value">\'+(s.wallet.balanceSol===null?"--":s.wallet.balanceSol.toFixed(3)+" SOL")+\'</span></span>\'+\n\'<span class="hstat"><span class="hstat-label">Cycle</span> <span class="hstat-value">\'+s.scan.cycle+\'</span></span>\'+\n\'<span class="hstat"><span class="hstat-label">Opps</span> <span class="hstat-value">\'+s.cumulative.totalOpps+\'</span></span>\';\n\nvar dot=document.getElementById("statusDot");\ndot.className="dot "+s.status;\n\nvar badge=document.getElementById("modeBadge");\nbadge.textContent=s.simulationOnly?"SIM":"LIVE";\nbadge.className="badge "+(s.simulationOnly?"sim":"live");\n\ndocument.getElementById("kpiStatus").textContent=s.status.toUpperCase();\ndocument.getElementById("kpiStatusSub").textContent=s.scan.inFlight?"scanning":"cycle "+s.scan.cycle;\n\ndocument.getElementById("kpiPnl").textContent=fmtUsd.format(s.cumulative.pnlUsd);\ndocument.getElementById("kpiPnlSub").textContent=s.cumulative.totalOpps+" opps in "+s.cumulative.totalCycles+" cycles";\nvar pnlCard=document.getElementById("kpiPnlCard");\npnlCard.className="kpi "+(s.cumulative.pnlUsd>=0?"pnl-up":"pnl-down");\n\ndocument.getElementById("kpiSpread").textContent=s.scan.maxSpreadPct===null?"0%":s.scan.maxSpreadPct.toFixed(4)+"%";\ndocument.getElementById("kpiSpreadSub").textContent="best profit "+fmtUsd.format(s.scan.bestNetProfitUsd||0);\n\ndocument.getElementById("kpiQuotes").textContent=s.scan.validQuotes+"/"+(pairs*venues);\ndocument.getElementById("kpiQuotesSub").textContent=s.scan.opportunityCount+" opportunities found";\n\ndocument.getElementById("walletPubkey").textContent=s.wallet.error?s.wallet.error:shortKey(s.wallet.publicKey);\ndocument.getElementById("walletBalance").textContent=s.wallet.balanceSol===null?"--":s.wallet.balanceSol.toFixed(4)+" SOL";\ndocument.getElementById("walletError").textContent="";\n\nrenderQuotes(s.quotes);\nrenderPairs(s.pairResults);\nrenderOpps(s.opportunities);\nrenderActivity(s.activity);\ndrawSparkline(s.sparkline);\nswitchTab(currentTab);\n\ndocument.getElementById("startButton").disabled=s.status==="running"||s.status==="scanning";\ndocument.getElementById("stopButton").disabled=s.status==="paused"\n}\n\nasync function loadState(){\ntry{\nvar state=await api("/api/state");\nif(!currentState)syncControls(state);\nrender(state)\n}catch(e){console.error(e)}\n}\n\nasync function saveSettings(){\ntry{\nvar body={pairs:document.getElementById("pairInput").value,intervalMs:Number(document.getElementById("intervalInput").value),venues:selectedVenues()};\nvar s=await api("/api/settings",body);\nrender(s)\n}catch(e){alert(e.message)}\n}\n\ndocument.getElementById("saveButton").addEventListener("click",function(){saveSettings().catch(alert)});\ndocument.getElementById("scanButton").addEventListener("click",function(){api("/api/scan",{}).then(render).catch(alert)});\ndocument.getElementById("startButton").addEventListener("click",function(){api("/api/start",{}).then(render).catch(alert)});\ndocument.getElementById("stopButton").addEventListener("click",function(){api("/api/stop",{}).then(render).catch(alert)});\ndocument.querySelectorAll(".tab-btn").forEach(function(btn){btn.addEventListener("click",function(){switchTab(btn.dataset.tab)})});\n\nloadState().catch(alert);\nsetInterval(function(){loadState().catch(console.error)},2000)\n})();\n</script>\n</body>\n</html>';
}


export function startDashboard(): void {
  const host = parseHost();
  const port = parsePort();
  const server = createServer((req, res) => {
    const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);

    if (url.pathname === '/') {
      sendHtml(res);
      return;
    }

    if (url.pathname.startsWith('/api/')) {
      void handleApi(req, res, url.pathname).catch((error) => {
        badRequest(res, error);
      });
      return;
    }

    sendJson(res, { error: 'Not found' }, 404);
  });

  server.listen(port, host, () => {
    const displayUrl = `http://${formatHostForUrl(host)}:${port}`;
    console.log(`Dashboard running at ${displayUrl}`);
    void refreshWallet();
    startScanner();
  });
}

startDashboard();
