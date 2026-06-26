use crate::{
    engine::{ScanSnapshot, SimulationSnapshot, StateSnapshot},
    rwa::RwaSnapshot,
    types::{Opportunity, Quote},
};
use axum::{
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct DashboardState {
    pub snapshot: Arc<RwLock<StateSnapshot>>,
    pub control: crate::engine::BotEngine,
}

pub async fn serve(host: String, port: u16, state: DashboardState) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/", get(index))
        .route("/health", get(get_health))
        .route("/state", get(get_state))
        .route("/scan", get(get_scan))
        .route("/opportunities", get(get_opportunities))
        .route("/quotes", get(get_quotes))
        .route("/simulations", get(get_simulations))
        .route("/fees", get(get_fees))
        .route("/pools", get(get_pools))
        .route("/rwa/state", get(get_rwa_state))
        .route("/rwa/opportunities", get(get_rwa_opportunities))
        .route("/rwa/pnl", get(get_rwa_pnl))
        .route("/rwa/assets", get(get_rwa_assets))
        .route("/pause", post(pause))
        .route("/resume", post(resume))
        .route("/emergency-stop", post(emergency_stop))
        .with_state(state);

    let addr: SocketAddr = format!("{host}:{port}").parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("Rust dashboard running at http://{addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(
        r#"<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Rust Solana Arbitrage Bot</title>
  <style>
    body{font-family:system-ui,-apple-system,sans-serif;margin:24px;background:#f6f8fb;color:#121820}
    main{max-width:1100px;margin:auto}
    .grid{display:grid;grid-template-columns:repeat(8,minmax(0,1fr));gap:12px}
    .card{background:white;border:1px solid #d9e0ea;border-radius:8px;padding:14px}
    h1{margin:0 0 16px}
    h2{margin:24px 0 8px}
    .value{font-size:24px;font-weight:800}
    table{border-collapse:collapse;width:100%;background:white;margin-top:12px}
    td,th{border-bottom:1px solid #d9e0ea;padding:8px;text-align:left}
    th{font-size:12px;text-transform:uppercase;color:#5b6675}
    code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}
    .muted{color:#5b6675}
    .warn{color:#9a3412}
    .ok{color:#166534}
    .section{margin-top:16px}
    button{padding:10px 14px;margin-right:8px}
    @media(max-width:900px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
  </style>
</head>
<body>
<main>
  <h1>Rust Solana Arbitrage Bot</h1>
  <div class="grid">
    <div class="card"><div>Status</div><div id="status" class="value">--</div></div>
    <div class="card"><div>Cycle</div><div id="cycle" class="value">--</div></div>
    <div class="card"><div>Pools</div><div id="pools" class="value">--</div></div>
    <div class="card"><div>Quotes</div><div id="quotes" class="value">--</div></div>
    <div class="card"><div>Targets</div><div id="targets" class="value">--</div></div>
    <div class="card"><div>Opportunities</div><div id="opps" class="value">--</div></div>
    <div class="card"><div>Execution</div><div id="execution" class="value">--</div></div>
    <div class="card"><div>Sim PnL</div><div id="sim-pnl" class="value">--</div></div>
    <div class="card"><div>RWA</div><div id="rwa-count" class="value">--</div></div>
  </div>
  <p>
    <button onclick="post('/resume')">Resume</button>
    <button onclick="post('/pause')">Pause</button>
    <button onclick="post('/emergency-stop')">Emergency Stop</button>
  </p>
  <h2>Health</h2>
  <div id="health" class="card"></div>
  <h2>RWA Stocks</h2>
  <div id="rwa-detail" class="card"></div>
  <div id="rwa-execution" class="card"></div>
  <div id="rwa-assets"></div>
  <div id="rwa-opportunities"></div>
  <h2>Execution</h2>
  <div id="execution-detail" class="card"></div>
  <div id="path-table"></div>
  <h2>Focused Native Route Readiness</h2>
  <div id="focused-route-table"></div>
  <h2>Jupiter Reference Probes</h2>
  <div id="probe-table"></div>
  <div id="wallet-funding-table"></div>
  <h2>Execution Attempts</h2>
  <div id="execution-table"></div>
  <h2>Simulated Trades</h2>
  <div id="simulation-detail" class="card"></div>
  <div id="simulation-table"></div>
  <h2>Scan Targets</h2>
  <div id="target-table"></div>
  <h2>Quote Providers</h2>
  <div id="provider-table"></div>
  <h2>Quotes</h2>
  <div id="quote-table"></div>
  <h2>Broad Scan Opportunities</h2>
  <div id="opportunity-table"></div>
  <h2>Discovery</h2>
  <div id="discovery"></div>
  <h2>Fees</h2>
  <pre id="fees"></pre>
</main>
<script>
async function post(path){ await fetch(path,{method:'POST'}); await load(); }
async function load(){
 const state = await (await fetch('/state')).json();
 document.getElementById('status').textContent = state.status;
 document.getElementById('cycle').textContent = state.cycle;
 document.getElementById('pools').textContent = state.pools.length;
 document.getElementById('quotes').textContent = state.quotes.length;
 document.getElementById('targets').textContent = (state.scan?.target_pairs || []).length;
 document.getElementById('opps').textContent = state.opportunities.length;
 const broadExecutionOn = !!state.execution?.submission_enabled;
 const rwaExecutionOn = !!state.rwa?.execution?.submission_enabled;
 document.getElementById('execution').textContent = broadExecutionOn ? 'on' : (rwaExecutionOn ? 'rwa on' : 'off');
 document.getElementById('sim-pnl').textContent = state.simulation?.enabled ? `$${Number(state.simulation?.total_net_profit_usd || 0).toFixed(2)}` : 'off';
 document.getElementById('rwa-count').textContent = state.rwa?.enabled ? `${state.rwa.watched_assets || 0}/${state.rwa.opportunity_count || 0}` : 'off';
 document.getElementById('fees').textContent = JSON.stringify(state.last_network_fee,null,2);
 health(state);
 rwa(state.rwa || {});
 execution(state.execution || {});
 simulation(state.simulation || {});
 targetTable(state.scan?.target_pairs || []);
 providerTable(state.scan?.quote_providers || []);
 quoteTable(state.quotes || []);
 opportunityTable(state.opportunities);
 discovery(state.scan?.discovery || {});
}
function short(value){ return value ? `${value.slice(0,4)}...${value.slice(-4)}` : '--'; }
function shortSource(value){ return value && value.length > 56 ? `${value.slice(0,53)}...` : (value || '--'); }
function limit(value){ return Number(value || 0) > 9000000000000000 ? 'uncapped' : value; }
function age(value){
 const ms = Number(value || 0);
 if (!Number.isFinite(ms) || ms <= 0) return '--';
 if (ms < 120000) return `${Math.round(ms / 1000)}s`;
 if (ms < 7200000) return `${Math.round(ms / 60000)}m`;
 return `${(ms / 3600000).toFixed(1)}h`;
}
function nonzeroAtoms(value){ return String(value || '0') !== '0'; }
function issueCounts(errors){
	 return (errors || []).reduce((acc,e)=>{ const k=e.kind || 'unknown'; acc[k]=(acc[k]||0)+1; return acc; },{});
}
function readinessLabel(value){ return String(value || 'inactive').replaceAll('_',' '); }
function readinessClass(value){
 const key = String(value || '');
 if (key === 'active') return 'ok';
 if (key === 'pacing') return 'muted';
 return 'warn';
}
function health(state){
 const scan = state.scan || {};
 const discovery = scan.discovery || {};
 const counts = issueCounts(discovery.errors || []);
 const reserveBacked = (state.pools || []).filter(p=>nonzeroAtoms(p.reserve_base_atoms) && nonzeroAtoms(p.reserve_quote_atoms)).length;
 const priceOnlyWithLiquidity = (state.pools || []).filter(p=>(!nonzeroAtoms(p.reserve_base_atoms) || !nonzeroAtoms(p.reserve_quote_atoms)) && nonzeroAtoms(p.liquidity_quote_atoms)).length;
 const priceOnlyNoLiquidity = (state.pools || []).filter(p=>(!nonzeroAtoms(p.reserve_base_atoms) || !nonzeroAtoms(p.reserve_quote_atoms)) && !nonzeroAtoms(p.liquidity_quote_atoms)).length;
 let rows = [];
 if (state.pools.length === 0 && state.quotes.length > 0) {
   rows.push('<div class="warn">Reference quotes are working, but direct executable pools are still zero.</div>');
 }
 if (counts.rpc) {
   rows.push(`<div class="warn">Last discovery attempt had ${counts.rpc} RPC failure(s).</div>`);
 }
 const skipped = (counts.decode || 0) + (counts.missing_account || 0) + (counts.index || 0);
 if (skipped) {
   rows.push(`<div class="muted">Discovery skipped ${skipped} indexed candidate(s); ${discovery.decoded_pools || 0} direct pools are usable.</div>`);
 }
 if (state.pools.length > 0) {
   rows.push(`<div class="${reserveBacked ? 'ok' : 'warn'}">Pool quality: ${reserveBacked} reserve-backed, ${priceOnlyWithLiquidity} price-only with liquidity, ${priceOnlyNoLiquidity} price-only without liquidity.</div>`);
 }
 if (state.pools.length > 0 && reserveBacked === 0) {
   rows.push('<div class="warn">No reserve-backed direct pools are decoded; phantom price-only spreads are excluded from execution.</div>');
 }
 if (!state.execution?.submission_enabled && !state.rwa?.execution?.submission_enabled) {
   rows.push(`<div class="warn">Transaction execution is off: ${state.execution?.reason || 'not configured'}.</div>`);
 } else if (!state.execution?.submission_enabled && state.rwa?.execution?.submission_enabled) {
   rows.push(`<div class="ok">RWA live execution is on; broad crypto scanner execution remains off.</div>`);
 }
 if (state.simulation?.enabled) {
   rows.push(`<div class="ok">Paper-trade simulation is on; ${state.simulation.attempted_count || 0} simulated trade(s) recorded.</div>`);
 } else {
   rows.push('<div class="muted">Paper-trade simulation is off; dashboard execution records are live-only.</div>');
 }
 rows.push(`<div class="muted">Direct venues: ${(scan.direct_venues || []).join(', ') || '--'}</div>`);
 rows.push(`<div class="muted">Quote venues: ${(scan.quote_venues || []).join(', ') || '--'}</div>`);
 const sol = scan.sol_usd || {};
 rows.push(`<div class="${sol.last_error ? 'warn' : 'muted'}">SOL/USD sizing: $${Number(sol.price_usd || 0).toFixed(4)} via ${sol.source || '--'}${sol.stale ? ' (stale fallback)' : ''}${sol.last_error ? ` | ${sol.last_error}` : ''}</div>`);
 rows.push(`<div class="muted">Trade size: $${Number(scan.trade_size_usd || 0).toFixed(2)} | Min net profit: ${Number(scan.min_net_profit_bps || 0).toFixed(2)} bps</div>`);
 document.getElementById('health').innerHTML = rows.join('');
}
function rwa(r){
 const exec = r.execution || {};
 const nextOpen = r.next_regular_session_open_utc || (r.next_regular_session_open_unix_ms == null ? '--' : new Date(r.next_regular_session_open_unix_ms).toISOString());
 const html = [
	  `<div>Enabled: ${r.enabled ? 'yes' : 'no'} | Mode: ${r.mode || '--'} | Assets: ${r.watched_assets || 0} | Pyth: ${r.oracle_prices || 0} | Quote requests: ${r.quote_request_count || 0} | Quotes: ${r.quote_count || 0} | Opportunities: ${r.opportunity_count || 0}</div>`,
	  `<div>Directional: ${r.directional_enabled ? 'on' : 'off'} | Ondo primary: ${r.ondo_primary_enabled ? 'on' : 'off'} | Registry: <code>${r.assets_file || '--'}</code></div>`,
	  `<div>Jupiter v2/RFQ: ${r.jupiter_v2_enabled ? 'on' : 'off'} | quote with taker: ${r.jupiter_v2_quote_with_taker ? 'yes' : 'no'} | taker wallet: ${r.jupiter_v2_taker_available ? 'available' : 'missing'}</div>`,
	  `<div>US equity session: ${r.market_session_open ? 'open' : 'closed'} | next regular open: ${nextOpen}</div>`,
	  `<div class="muted">Pyth feeds: ${(r.pyth_feed_ids || []).map(short).join(', ') || '--'} | ${r.quote_status || '--'}</div>`,
	  `<div class="muted">${r.pnl?.note || ''}</div>`,
	  r.last_error ? `<div class="warn"><code>${r.last_error}</code></div>` : ''
	 ].join('');
 document.getElementById('rwa-detail').innerHTML = html;
 const execRows = [
 `<div>Live requested: ${exec.live_requested ? 'yes' : 'no'} | Submission enabled: ${exec.submission_enabled ? 'yes' : 'no'}</div>`,
  `<div>Attempts: ${exec.attempted_count || 0} | Submitted: ${exec.submitted_count || 0} | Confirmed: ${exec.confirmed_count || 0} | Failed: ${exec.failed_count || 0}</div>`,
  `<div>Stable available: $${Number(exec.stable_available_usd || 0).toFixed(6)} | Quote funding: $${Number(exec.quote_funding_available_usd || 0).toFixed(6)} | Configured RWA trade size: $${Number(exec.configured_trade_usd || 0).toFixed(2)}</div>`,
  `<div>Fundable executable candidates: ${exec.fundable_direct_candidates || 0} | Inventory-blocked executable opportunities: ${exec.inventory_blocked_opportunities || 0} | Route/policy-gated signals: ${exec.route_gated_opportunities || 0}${exec.best_blocked_required_usd == null ? '' : ` | Best inventory-blocked has $${Number(exec.best_blocked_available_usd || 0).toFixed(4)} available of $${Number(exec.best_blocked_required_usd || 0).toFixed(4)} required`}</div>`,
  `<div class="${exec.submission_enabled ? 'ok' : 'warn'}">${exec.reason || '--'}</div>`,
  (exec.wallet_balance_errors || []).length ? `<div class="warn">Wallet balance warnings:<br>${(exec.wallet_balance_errors || []).map(v=>`<code>${v}</code>`).join('<br>')}</div>` : '',
  (exec.readiness_blockers || []).length ? `<div class="warn">Readiness blockers:<br>${(exec.readiness_blockers || []).map(b=>`<code>${b}</code>`).join('<br>')}</div>` : '<div class="ok">No readiness blockers reported.</div>',
 exec.last_signature ? `<div>Last tx: <a href="https://solscan.io/tx/${exec.last_signature}" target="_blank" rel="noreferrer"><code>${short(exec.last_signature)}</code></a></div>` : '',
  exec.last_error ? `<div class="warn"><code>${exec.last_error}</code></div>` : ''
 ];
 const balances = exec.wallet_balances || [];
 const balanceTable = balances.length ? '<table><thead><tr><th>Wallet input</th><th>Balance</th><th>Mint</th></tr></thead><tbody>'
  + balances.map(b=>`<tr><td>${b.symbol}</td><td>${Number(b.ui_amount || 0).toFixed(8)}<br><code>${b.amount_atoms || 0} atoms</code></td><td><code>${short(b.mint)}</code></td></tr>`).join('')
  + `</tbody></table><div class="muted">SOL: ${Number((exec.wallet_sol_lamports || 0) / 1_000_000_000).toFixed(6)}</div>` : '';
 const trades = exec.trades || [];
 const execTable = trades.length ? '<table><thead><tr><th>ID</th><th>Ticker</th><th>Route</th><th>Expected</th><th>Submitted</th><th>Reason</th></tr></thead><tbody>'
  + trades.slice(0,20).map(t=>`<tr><td>${t.id}</td><td>${t.ticker}</td><td>${t.from_symbol} -> ${t.to_symbol}</td><td>$${Number(t.expected_net_profit_usd || 0).toFixed(6)} / ${Number(t.expected_net_profit_bps || 0).toFixed(2)} bps</td><td>${t.submitted ? 'yes' : 'no'}${t.signature ? `<br><a href="${t.solscan_url || ('https://solscan.io/tx/' + t.signature)}" target="_blank" rel="noreferrer"><code>${short(t.signature)}</code></a>` : ''}</td><td><code>${t.reason || ''}</code></td></tr>`).join('')
  + '</tbody></table>' : '<p class="muted">No live RWA execution records yet.</p>';
 document.getElementById('rwa-execution').innerHTML = execRows.join('') + balanceTable + execTable;
 const assets = r.assets || [];
 const assetTable = assets.length ? '<table><thead><tr><th>Ticker</th><th>Pyth</th><th>xStock</th><th>Ondo</th><th>Primary</th><th>Premiums</th><th>Routes</th><th>No trade reason</th></tr></thead><tbody>'
  + assets.map(a=>{
    const oracle = a.oracle || {};
    const pyth = oracle.price_usd == null ? '--' : `$${Number(oracle.price_usd).toFixed(4)} / ${Number(oracle.confidence_bps || 0).toFixed(2)}bps / ${oracle.status}<br><span class="muted">${oracle.market_session || '--'} / age ${age(oracle.age_ms)}</span>`;
    const x = a.xstock_price_usd == null ? '--' : `$${Number(a.xstock_price_usd).toFixed(4)}`;
    const o = a.ondo_normalized_price_usd == null ? '--' : `$${Number(a.ondo_normalized_price_usd).toFixed(4)} norm (${Number(a.ondo_shares_per_token || 1).toFixed(6)}x)`;
    const primary = (a.primary_routes || []).length ? (a.primary_routes || []).map(p=>{
      const norm = p.normalized_price_usd == null ? '--' : `$${Number(p.normalized_price_usd || 0).toFixed(4)} norm`;
      const token = p.price_usd == null ? '--' : `$${Number(p.price_usd || 0).toFixed(4)} token`;
      const shares = Number(p.shares_per_token || 1).toFixed(6);
      const mint = p.rfq_mint_price_usd == null ? (p.rfq_mint_available ? 'yes' : 'no') : `$${Number(p.rfq_mint_price_usd || 0).toFixed(4)}`;
      const burn = p.rfq_burn_price_usd == null ? (p.rfq_burn_available ? 'yes' : 'no') : `$${Number(p.rfq_burn_price_usd || 0).toFixed(4)}`;
      const mintRoute = p.rfq_mint_route ? ` via ${shortSource(p.rfq_mint_route)}` : '';
	      const burnRoute = p.rfq_burn_route ? ` via ${shortSource(p.rfq_burn_route)}` : '';
	      const nonRfqMint = p.non_rfq_mint_price_usd == null ? '--' : `$${Number(p.non_rfq_mint_price_usd || 0).toFixed(4)}${p.non_rfq_mint_route ? ` via ${shortSource(p.non_rfq_mint_route)}` : ''}`;
	      const nonRfqBurn = p.non_rfq_burn_price_usd == null ? '--' : `$${Number(p.non_rfq_burn_price_usd || 0).toFixed(4)}${p.non_rfq_burn_route ? ` via ${shortSource(p.non_rfq_burn_route)}` : ''}`;
	      return `${p.provider}: ${norm} / ${token} (${shares}x) <span class="${readinessClass(p.readiness)}">${readinessLabel(p.readiness)}</span> ${p.executable ? 'exec' : 'gated'}<br><span class="muted">RFQ mint ${mint}${mintRoute} / burn ${burn}${burnRoute} / builder ${p.external_builder_configured ? 'yes' : 'no'}</span><br><span class="muted">non-RFQ secondary mint ${nonRfqMint} / burn ${nonRfqBurn}</span><br><span class="muted">${p.reason || ''}</span>`;
	    }).join('<br>') : '--';
    const prem = `${a.xstock_premium_bps == null ? '--' : Number(a.xstock_premium_bps).toFixed(2)} / ${a.ondo_premium_bps == null ? '--' : Number(a.ondo_premium_bps).toFixed(2)} bps`;
    const nearMiss = a.best_rejected_net_profit_bps == null ? '' : `<br><span class="muted">Best rejected ${a.best_rejected_kind || '--'}: ${Number(a.best_rejected_net_profit_bps || 0).toFixed(2)} bps via ${shortSource(a.best_rejected_route || '')}</span><br><code>${a.best_rejected_reason || ''}</code>`;
    const rejectedByKind = (a.best_rejected_by_kind || []).length ? '<br><span class="muted">Near misses by route:</span><br>' + (a.best_rejected_by_kind || []).map(r=>`<span class="muted">${r.kind}: ${Number(r.net_profit_bps || 0).toFixed(2)} bps / $${Number(r.net_profit_usd || 0).toFixed(6)} via ${shortSource(r.route || '')}</span><br><code>${r.reason || ''}</code>`).join('<br>') : '';
    return `<tr><td>${a.ticker}<br><code>${a.xstock_symbol}/${a.ondo_symbol}</code></td><td>${pyth}</td><td>${x}<br><code>${short(a.xstock_mint)}</code></td><td>${o}<br><code>${short(a.ondo_mint)}</code></td><td>${primary}</td><td>${prem}</td><td>${a.quote_count || 0} stable / ${a.direct_route_count || 0}/${a.direct_route_request_count || 0} direct / ${a.primary_route_count || 0} primary</td><td><code>${a.no_trade_reason || ''}</code>${nearMiss}${rejectedByKind}</td></tr>`;
  }).join('')
  + '</tbody></table>' : '<p class="muted">No RWA assets enabled.</p>';
 document.getElementById('rwa-assets').innerHTML = assetTable;
 const opps = r.opportunities || [];
 const oppTable = opps.length ? '<table><thead><tr><th>Ticker</th><th>Kind</th><th>Route</th><th>Edge</th><th>Net</th><th>Executable</th><th>Block</th></tr></thead><tbody>'
  + opps.slice(0,30).map(o=>`<tr><td>${o.ticker}</td><td>${o.kind}</td><td>${o.from_symbol} -> ${o.to_symbol}<br><code>${shortSource(o.route)}</code><br><span class="muted">${(o.execution_legs || []).length} executable leg(s)</span></td><td>${Number(o.gross_edge_bps || 0).toFixed(2)} bps</td><td class="${Number(o.net_profit_usd || 0) >= 0 ? 'ok' : 'warn'}">$${Number(o.net_profit_usd || 0).toFixed(6)} / ${Number(o.net_profit_bps || 0).toFixed(2)} bps</td><td>${o.executable ? 'yes' : 'no'}</td><td><code>${o.blocked_reason || ''}</code></td></tr>`).join('')
  + '</tbody></table>' : '<p class="muted">No RWA opportunities above threshold.</p>';
 document.getElementById('rwa-opportunities').innerHTML = oppTable;
}
function execution(e){
 const apiWindow = e.api_requests_per_window ? `${e.api_requests_per_window}/${Math.ceil((e.api_window_ms || 0) / 1000)}s` : 'uncapped';
 const html = [
  `<div>Live requested: ${e.live_requested ? 'yes' : 'no'}</div>`,
  `<div>Submission enabled: ${e.submission_enabled ? 'yes' : 'no'}</div>`,
  `<div>Candidate source: ${e.candidate_source || '--'}</div>`,
  `<div>Candidate pool: ${e.candidate_pool_size || 0} | Cooling down: ${e.candidate_cooldown_count || 0} | Probe sizes: ${(e.probe_trade_sizes_usd || []).map(v=>`$${Number(v).toFixed(2)}`).join(', ') || '--'}</div>`,
  `<div>Wallet funding blocked: ${e.wallet_funding_blocked_count || 0}${e.best_funding_blocked_required_usd == null ? '' : ` | Best blocked has $${Number(e.best_funding_blocked_available_usd || 0).toFixed(4)} available of $${Number(e.best_funding_blocked_required_usd || 0).toFixed(4)} required`}</div>`,
  `<div>Jupiter API budget: ${apiWindow} | ${limit(e.api_requests_budget_per_cycle)} request(s)/scan | ${limit(e.api_candidate_budget_per_cycle)} candidate(s)/scan | selected ${e.api_candidate_limit_applied || 0} from offset ${e.candidate_start_offset || 0}</div>`,
  `<div>Request estimate: ${e.api_estimated_requests_per_candidate || 0} per candidate | Rate-limit cooldown: ${e.api_rate_limit_cooldown_ms || 0}ms | Rate limited: ${e.api_rate_limited ? 'yes' : 'no'}</div>`,
  `<div>Attempts: ${e.attempted_count || 0} | Blocked: ${e.blocked_count || 0} | Confirmed: ${e.confirmed_count || 0} | Failed: ${e.failed_count || 0}</div>`,
  `<div class="${e.submission_enabled ? 'ok' : 'warn'}">${e.reason || '--'}</div>`,
  (e.wallet_quote_balance_errors || []).length ? `<div class="warn">Wallet balance warnings:<br>${(e.wallet_quote_balance_errors || []).map(v=>`<code>${v}</code>`).join('<br>')}</div>` : '',
  e.last_signature ? `<div>Last signature: <code>${e.last_signature}</code></div>` : '',
  e.last_error ? `<div class="warn">Last error: <code>${e.last_error}</code></div>` : ''
 ].join('');
 document.getElementById('execution-detail').innerHTML = html;
 pathTable(e.paths || []);
 focusedRouteTable(e);
 probeTable(e);
 walletQuoteTable(e.wallet_quote_balances || []);
 const rows = e.trades || [];
 const table = rows.length ? '<table><thead><tr><th>ID</th><th>Cycle</th><th>Pair</th><th>Route</th><th>Signal USD</th><th>Requote bps</th><th>Status</th><th>Tx</th><th>Reason</th></tr></thead><tbody>'
  + rows.slice(0,30).map(t=>`<tr><td>${t.id}</td><td>${t.cycle}</td><td><code>${short(t.pair.base_mint)}/${short(t.pair.quote_mint)}</code></td><td>${t.buy_venue} -> ${t.sell_venue}</td><td>${Number(t.direct_signal_net_profit_usd || 0).toFixed(6)}</td><td>${t.requote_net_profit_bps == null ? '--' : Number(t.requote_net_profit_bps).toFixed(2)}</td><td class="${t.status === 'confirmed' ? 'ok' : 'warn'}">${t.status}</td><td>${t.solscan_url ? `<a href="${t.solscan_url}" target="_blank">open</a>` : '--'}</td><td><code>${t.reason || ''}</code></td></tr>`).join('')
 + '</tbody></table>' : '<p class="muted">No live execution attempts yet.</p>';
 document.getElementById('execution-table').innerHTML = table;
}
function walletQuoteTable(rows){
 const html = rows.length ? '<table><thead><tr><th>Live input</th><th>Balance</th><th>Mint</th></tr></thead><tbody>'
  + rows.map(b=>`<tr><td>${b.symbol}</td><td>${Number(b.ui_amount || 0).toFixed(8)}<br><code>${b.amount_atoms || 0} atoms</code></td><td><code>${short(b.mint)}</code></td></tr>`).join('')
  + '</tbody></table>' : '<p class="muted">No live quote-mint wallet balances reported yet.</p>';
 document.getElementById('wallet-funding-table').innerHTML = '<div class="section"><div class="muted">Live funding balances</div>' + html + '</div>';
}
function pathTable(rows){
	 const html = rows.length ? '<table><thead><tr><th>Path</th><th>Kind</th><th>Quote Source</th><th>Swap Source</th><th>Ready</th><th>Active</th><th>Pools</th><th>Quotes</th><th>Status</th></tr></thead><tbody>'
	  + rows.map(p=>`<tr><td><code>${p.name}</code></td><td>${p.kind}</td><td><code>${shortSource(p.quote_source)}</code></td><td>${p.swap_source}</td><td class="${readinessClass(p.readiness)}">${readinessLabel(p.readiness)}</td><td class="${p.active ? 'ok' : 'warn'}">${p.active ? 'yes' : 'no'}${p.requires_api_key ? ` / key ${p.has_api_key ? 'present' : 'missing'}` : ''}</td><td>${p.decoded_pools || 0} decoded / ${p.reserve_backed_pools || 0} reserve</td><td>${p.quote_count || 0}</td><td><code>${p.status || ''}</code></td></tr>`).join('')
	  + '</tbody></table>' : '<p class="muted">No execution paths reported yet.</p>';
	 document.getElementById('path-table').innerHTML = '<div class="section"><div class="muted">Execution and quote paths</div>' + html + '</div>';
}
function samePair(a,b){ return a && b && a.base_mint === b.base_mint && a.quote_mint === b.quote_mint; }
function latencyStages(trace){
 const rows = trace || [];
 return rows.length ? rows.map(mark=>`${mark.stage}:${mark.elapsed_ms}ms`).join('<br>') : '--';
}
function readinessCounts(counts){
 return (counts || []).length ? (counts || []).map(c=>`${String(c.readiness_state || '').replaceAll('_',' ')} ${c.count || 0}`).join('<br>') : '--';
}
function latestBlocker(blockers){
 const blocker = (blockers || [])[0];
 return blocker ? `${String(blocker.readiness_state || '').replaceAll('_',' ')} x${blocker.count || 0}<br><code>${blocker.blocker || ''}</code>` : '<span class="ok">No blocker reported.</span>';
}
function focusedRouteTable(e){
 const families = e.focused_route_families || [];
 const trades = e.trades || [];
 const html = families.length ? '<table><thead><tr><th>Family</th><th>Account readiness</th><th>Quote readiness</th><th>Native net bps</th><th>Latency stages</th><th>Submit state</th><th>Current blocker</th></tr></thead><tbody>'
  + families.map(f=>{
    const latest = trades.find(t=>samePair(t.pair, f.pair) && String(t.buy_venue) === String(f.buy_venue) && String(t.sell_venue) === String(f.sell_venue));
    const native_net_bps = latest?.requote_net_profit_bps == null ? '--' : Number(latest.requote_net_profit_bps).toFixed(2);
    const submit_state = latest ? `${latest.status || '--'} / submitted=${latest.submitted ? 'yes' : 'no'} / sim=${latest.simulation_ok ? 'ok' : 'blocked'}` : '--';
    const quoteReady = latest ? `${latest.jupiter_reference_error ? 'reference blocked' : 'reference available'}${latest.jupiter_reference_net_profit_bps == null ? '' : `<br><span class="muted">jup ref ${Number(latest.jupiter_reference_net_profit_bps).toFixed(2)}bps</span>`}` : readinessCounts(f.readiness_counts);
    return `<tr><td><code>#${f.id}</code> ${short(f.pair?.base_mint)}/${short(f.pair?.quote_mint)}<br>${f.buy_venue} -> ${f.sell_venue}<br><span class="muted">${f.total_edges || 0} edge(s), priority ${f.priority || 0}, ${f.enabled ? 'enabled' : 'disabled'}</span></td><td>${readinessCounts(f.readiness_counts)}</td><td>${quoteReady}</td><td>${native_net_bps}</td><td>${latencyStages(latest?.latency_trace)}</td><td>${submit_state}</td><td>${latest?.reason ? `<code>${latest.reason}</code>` : latestBlocker(f.latest_blockers)}</td></tr>`;
  }).join('')
  + '</tbody></table>' : '<p class="muted">No focused native route families reported yet.</p>';
 document.getElementById('focused-route-table').innerHTML = html;
}
function probeTable(e){
 const selected = e.selected_candidates || [];
 const best = e.best_recent_candidates || [];
 const render = rows => rows.length ? '<table><thead><tr><th>Pair</th><th>Route</th><th>Input</th><th>Optimizer</th><th>Attempts</th><th>Last bps</th><th>Best bps</th><th>Cooldown</th><th>Last reason</th></tr></thead><tbody>'
  + rows.map(c=>{
    const optimizer = c.optimizer_decision_id ? `<code>${short(c.optimizer_decision_id)}</code><br><span class="${Number(c.optimizer_net_profit_usd || 0) >= 0 ? 'ok' : 'warn'}">$${Number(c.optimizer_net_profit_usd || 0).toFixed(6)} / ${Number(c.optimizer_net_profit_bps || 0).toFixed(2)}bps</span><br><span class="muted">in ${c.optimizer_selected_input_quote_atoms || '--'} net ${c.optimizer_net_profit_quote_atoms || '--'}</span>` : '--';
    return `<tr><td><code>${short(c.pair.base_mint)}/${short(c.pair.quote_mint)}</code></td><td>${c.buy_venue} -> ${c.sell_venue}</td><td>$${Number(c.input_usd || 0).toFixed(2)}</td><td>${optimizer}</td><td>${c.attempts || 0}</td><td>${c.last_requote_bps == null ? '--' : Number(c.last_requote_bps).toFixed(2)}</td><td>${c.best_requote_bps == null ? '--' : Number(c.best_requote_bps).toFixed(2)}</td><td>${c.cooldown_remaining_ms ? `${Math.ceil(c.cooldown_remaining_ms/1000)}s` : '--'}</td><td><code>${c.last_reason || ''}</code></td></tr>`;
  }).join('')
  + '</tbody></table>' : '<p class="muted">No probe candidates selected yet.</p>';
 const bestTable = best.length ? '<div class="section"><div class="muted">Best recent probes</div>' + render(best) + '</div>' : '';
 document.getElementById('probe-table').innerHTML = '<div class="section"><div class="muted">Selected this cycle</div>' + render(selected) + '</div>' + bestTable;
}
function simulation(s){
 const pnl = Number(s.total_net_profit_usd || 0);
 const html = [
  `<div>Enabled: ${s.enabled ? 'yes' : 'no'} | Attempts: ${s.attempted_count || 0}</div>`,
  `<div>Profitable: ${s.profitable_count || 0} | Unprofitable: ${s.unprofitable_count || 0}</div>`,
  `<div class="${pnl >= 0 ? 'ok' : 'warn'}">Total simulated net PnL: $${pnl.toFixed(6)}</div>`,
  `<div class="muted">${s.reason || '--'}</div>`
 ].join('');
 document.getElementById('simulation-detail').innerHTML = html;
 const rows = s.trades || [];
 const table = rows.length ? '<table><thead><tr><th>ID</th><th>Cycle</th><th>Pair</th><th>Route</th><th>Net USD</th><th>Bps</th><th>Status</th></tr></thead><tbody>'
  + rows.slice(0,30).map(t=>`<tr><td>${t.id}</td><td>${t.cycle}</td><td><code>${short(t.pair.base_mint)}/${short(t.pair.quote_mint)}</code></td><td>${t.buy_venue} -> ${t.sell_venue}</td><td>${Number(t.simulated_net_profit_usd).toFixed(6)}</td><td>${Number(t.simulated_profit_bps).toFixed(2)}</td><td class="${t.profitable ? 'ok' : 'warn'}">${t.status}</td></tr>`).join('')
  + '</tbody></table>' : '<p class="muted">No simulated trades yet.</p>';
 document.getElementById('simulation-table').innerHTML = table;
}
function targetTable(rows){
 const html = rows.length ? '<table><thead><tr><th>Pair</th><th>Base Mint</th><th>Quote Mint</th><th>Decimals</th></tr></thead><tbody>'
  + rows.map(p=>`<tr><td><code>${short(p.base_mint)}/${short(p.quote_mint)}</code></td><td><code>${p.base_mint}</code></td><td><code>${p.quote_mint}</code></td><td>${p.base_decimals}/${p.quote_decimals}</td></tr>`).join('')
  + '</tbody></table>' : '<p class="muted">No scan targets.</p>';
 document.getElementById('target-table').innerHTML = html;
}
function providerTable(rows){
 const html = rows.length ? '<table><thead><tr><th>Name</th><th>Venue</th><th>Format</th><th>Key</th><th>Pacing</th><th>Backoff</th><th>Status</th></tr></thead><tbody>'
  + rows.map(p=>{
    const cooldown = p.cooldown_remaining_ms ? `<br><span class="warn">cooldown ${Math.ceil(Number(p.cooldown_remaining_ms || 0)/1000)}s</span>` : '';
    const next = p.next_request_in_ms ? `<br><span class="muted">next ${Math.ceil(Number(p.next_request_in_ms || 0)/1000)}s</span>` : '';
    const key = p.requires_api_key ? `${p.has_api_key ? 'present' : 'missing'}${p.api_key_env ? `<br><code>${p.api_key_env}</code>` : ''}${(p.missing_auth_envs || []).length ? `<br><span class="warn">missing ${(p.missing_auth_envs || []).join(', ')}</span>` : ''}` : 'not required';
    const budgetStrikes = Number(p.budget_exhaustion_strikes || 0);
    const errorStrikes = Number(p.provider_error_strikes || 0);
    const rateStrikes = Number(p.rate_limit_strikes || 0);
    const backoff = [
      budgetStrikes ? `${budgetStrikes} budget miss${budgetStrikes === 1 ? '' : 'es'}` : '',
      errorStrikes ? `${errorStrikes} timeout/error${errorStrikes === 1 ? '' : 's'}` : '',
      rateStrikes ? `${rateStrikes} rate limit${rateStrikes === 1 ? '' : 's'}` : '',
      p.cooldown_reason ? `reason: ${p.cooldown_reason}` : ''
    ].filter(Boolean).join('<br>') || '--';
    const backoffClass = budgetStrikes || errorStrikes || rateStrikes ? 'warn' : 'muted';
    return `<tr><td>${p.name}</td><td>${p.venue}</td><td>${p.format}</td><td>${key}</td><td>${p.min_interval_ms}ms / ${p.max_in_flight} in flight${next}${cooldown}</td><td class="${backoffClass}">${backoff}</td><td class="${p.available ? 'ok' : 'warn'}">${p.status || '--'}</td></tr>`;
  }).join('')
  + '</tbody></table>' : '<p class="muted">No active quote providers.</p>';
 document.getElementById('provider-table').innerHTML = html;
}
function quoteTable(rows){
 const html = rows.length ? '<table><thead><tr><th>Venue</th><th>Pair</th><th>Input</th><th>Output</th><th>Price</th><th>Route</th></tr></thead><tbody>'
  + rows.slice(0,20).map(q=>`<tr><td>${q.venue}</td><td><code>${short(q.pair.base_mint)}/${short(q.pair.quote_mint)}</code></td><td>${q.in_amount_atoms}</td><td>${q.out_amount_atoms}</td><td>${Number(q.effective_price).toPrecision(8)}</td><td><code>${q.pool}</code></td></tr>`).join('')
  + '</tbody></table>' : '<p class="muted">No quotes.</p>';
 document.getElementById('quote-table').innerHTML = html;
}
function opportunityTable(rows){
 const html = rows.length ? '<table><thead><tr><th>Pair</th><th>Buy</th><th>Sell</th><th>Net USD</th><th>Spread bps</th></tr></thead><tbody>'
  + rows.map(o=>`<tr><td>${o.pair.base_mint.slice(0,4)}.../${o.pair.quote_mint.slice(0,4)}...</td><td>${o.buy_venue}</td><td>${o.sell_venue}</td><td>${o.net_profit_usd.toFixed(6)}</td><td>${o.spread_bps.toFixed(2)}</td></tr>`).join('')
  + '</tbody></table>' : '<p>No opportunities.</p>';
 document.getElementById('opportunity-table').innerHTML = html;
}
function discovery(d){
 const errors = d.errors || [];
 const next = d.next_attempt_in_ms == null ? '--' : `${Math.ceil(d.next_attempt_in_ms / 1000)}s`;
 const summary = `<div class="card"><div>Enabled: ${d.enabled ? 'yes' : 'no'} | Attempted this cycle: ${d.attempted_this_cycle ? 'yes' : 'no'} | Indexed candidates: ${d.indexed_candidates || 0} | Targeted accounts: ${d.targeted_accounts || 0} | Decoded pools: ${d.decoded_pools || 0} | Broad RPC: ${d.broad_rpc_enabled ? 'on' : 'off'} | Next attempt: ${next}</div></div>`;
 const table = errors.length ? '<table><thead><tr><th>Kind</th><th>Venue</th><th>Program</th><th>Error</th></tr></thead><tbody>'
  + errors.map(e=>`<tr><td>${e.kind || 'unknown'}</td><td>${e.venue}</td><td><code>${e.program_id}</code></td><td><code>${String(e.error).split('\n')[0]}</code></td></tr>`).join('')
  + '</tbody></table>' : '<p class="muted">No discovery errors recorded.</p>';
 document.getElementById('discovery').innerHTML = summary + table;
}
load(); setInterval(load, 2000);
</script>
</body>
</html>"#,
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthPhase {
    Starting,
    Running,
    RunningWithWarnings,
    Paused,
    EmergencyStopped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthSnapshot {
    pub ok: bool,
    pub phase: HealthPhase,
    pub status: crate::engine::BotStatus,
    pub runtime_mode: String,
    pub rwa_only_mode: bool,
    pub rwa_monitoring_ready: bool,
    pub live: bool,
    pub cycle: u64,
    pub pools: usize,
    pub quotes: usize,
    pub opportunities: usize,
    pub broad_execution_enabled: bool,
    pub rwa_execution_enabled: bool,
    pub rwa_watched_assets: usize,
    pub rwa_valid_oracles: usize,
    pub rwa_quote_status: String,
    pub rwa_quotes: usize,
    pub rwa_opportunities: usize,
    pub discovery_rpc_errors: usize,
    pub missing_provider_keys: Vec<String>,
    pub readiness_blockers: Vec<String>,
    pub warnings: Vec<String>,
    pub last_error: Option<String>,
    pub execution_last_error: Option<String>,
    pub rwa_last_error: Option<String>,
    pub paths: Vec<HealthPathSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthPathSnapshot {
    pub name: String,
    pub enabled: bool,
    pub active: bool,
    pub readiness: crate::engine::ExecutionPathReadiness,
    pub requires_api_key: bool,
    pub has_api_key: bool,
    pub status: String,
}

fn health_snapshot(state: &StateSnapshot) -> HealthSnapshot {
    let discovery_rpc_errors = state
        .scan
        .discovery
        .errors
        .iter()
        .filter(|issue| matches!(issue.kind, crate::engine::DiscoveryIssueKind::Rpc))
        .count();
    let missing_provider_keys: Vec<String> = state
        .execution
        .paths
        .iter()
        .filter(|path| path.enabled && path.requires_api_key && !path.has_api_key)
        .map(|path| path.name.clone())
        .collect();
    let cooling_paths = state
        .execution
        .paths
        .iter()
        .filter(|path| {
            matches!(
                path.readiness,
                crate::engine::ExecutionPathReadiness::CoolingDown
            )
        })
        .count();
    let readiness_blockers = state.rwa.execution.readiness_blockers.clone();

    let mut warnings = Vec::new();
    if state.cycle == 0 {
        warnings.push("scanner has not completed its first cycle".to_string());
    }
    if state.pools.is_empty() {
        warnings.push("no pools are currently decoded".to_string());
    }
    if discovery_rpc_errors > 0 {
        warnings.push(format!(
            "last discovery attempt had {discovery_rpc_errors} RPC failure(s)"
        ));
    }
    if state.execution.submission_enabled && state.execution.last_error.is_some() {
        warnings.push("broad execution has a recent gated candidate".to_string());
    }
    if !state.execution.wallet_quote_balance_errors.is_empty() {
        warnings.push(format!(
            "broad execution has {} partial wallet balance check warning(s)",
            state.execution.wallet_quote_balance_errors.len()
        ));
    }
    if !state.rwa.execution.wallet_balance_errors.is_empty() {
        warnings.push(format!(
            "RWA execution has {} partial wallet balance check warning(s)",
            state.rwa.execution.wallet_balance_errors.len()
        ));
    }
    if !missing_provider_keys.is_empty() {
        warnings.push(format!(
            "{} configured route provider(s) are waiting for API keys",
            missing_provider_keys.len()
        ));
    }
    if cooling_paths > 0 {
        warnings.push(format!(
            "{cooling_paths} configured route provider(s) are cooling down after timeout or rate limit"
        ));
    }
    if state.rwa.enabled && state.rwa.oracle_prices == 0 {
        warnings.push("no valid RWA oracle prices are currently tradable".to_string());
    }
    if !state.execution.submission_enabled && !state.rwa.execution.submission_enabled {
        warnings.push("all transaction submission lanes are disabled".to_string());
    }

    let rwa_only_mode = state.rwa.enabled
        && !state.execution.submission_enabled
        && state.execution.candidate_source == "rwa-only mode; broad execution disabled";
    let runtime_mode = if rwa_only_mode {
        "rwa_only"
    } else if state.rwa.enabled {
        "mixed_rwa"
    } else {
        "broad"
    }
    .to_string();
    let rwa_monitoring_ready = matches!(state.status, crate::engine::BotStatus::Running)
        && state.cycle > 0
        && state.rwa.enabled
        && state.last_error.is_none()
        && state.rwa.last_error.is_none()
        && !state
            .rwa
            .quote_status
            .to_ascii_lowercase()
            .contains("disabled");

    let has_active_submission =
        state.execution.submission_enabled || state.rwa.execution.submission_enabled;
    let ok = matches!(state.status, crate::engine::BotStatus::Running)
        && state.cycle > 0
        && has_active_submission
        && state.last_error.is_none();

    let phase = match state.status {
        crate::engine::BotStatus::EmergencyStopped => HealthPhase::EmergencyStopped,
        crate::engine::BotStatus::Paused => HealthPhase::Paused,
        crate::engine::BotStatus::Running if state.cycle == 0 => HealthPhase::Starting,
        crate::engine::BotStatus::Running if warnings.is_empty() => HealthPhase::Running,
        crate::engine::BotStatus::Running => HealthPhase::RunningWithWarnings,
    };

    HealthSnapshot {
        ok,
        phase,
        status: state.status.clone(),
        runtime_mode,
        rwa_only_mode,
        rwa_monitoring_ready,
        live: state.live,
        cycle: state.cycle,
        pools: state.pools.len(),
        quotes: state.quotes.len(),
        opportunities: state.opportunities.len(),
        broad_execution_enabled: state.execution.submission_enabled,
        rwa_execution_enabled: state.rwa.execution.submission_enabled,
        rwa_watched_assets: state.rwa.watched_assets,
        rwa_valid_oracles: state.rwa.oracle_prices,
        rwa_quote_status: state.rwa.quote_status.clone(),
        rwa_quotes: state.rwa.quote_count,
        rwa_opportunities: state.rwa.opportunity_count,
        discovery_rpc_errors,
        missing_provider_keys,
        readiness_blockers,
        warnings,
        last_error: state.last_error.clone(),
        execution_last_error: state.execution.last_error.clone(),
        rwa_last_error: state
            .rwa
            .execution
            .last_error
            .clone()
            .or_else(|| state.rwa.last_error.clone()),
        paths: state
            .execution
            .paths
            .iter()
            .map(|path| HealthPathSnapshot {
                name: path.name.clone(),
                enabled: path.enabled,
                active: path.active,
                readiness: path.readiness.clone(),
                requires_api_key: path.requires_api_key,
                has_api_key: path.has_api_key,
                status: path.status.clone(),
            })
            .collect(),
    }
}

async fn get_health(State(state): State<DashboardState>) -> impl IntoResponse {
    let snapshot = state.snapshot.read().await;
    Json(health_snapshot(&snapshot))
}

async fn get_state(State(state): State<DashboardState>) -> impl IntoResponse {
    Json(state.snapshot.read().await.clone())
}

async fn get_scan(State(state): State<DashboardState>) -> impl IntoResponse {
    let scan: ScanSnapshot = state.snapshot.read().await.scan.clone();
    Json(scan)
}

async fn get_opportunities(State(state): State<DashboardState>) -> impl IntoResponse {
    let opportunities: Vec<Opportunity> = state.snapshot.read().await.opportunities.clone();
    Json(opportunities)
}

async fn get_quotes(State(state): State<DashboardState>) -> impl IntoResponse {
    let quotes: Vec<Quote> = state.snapshot.read().await.quotes.clone();
    Json(quotes)
}

async fn get_simulations(State(state): State<DashboardState>) -> impl IntoResponse {
    let simulations: SimulationSnapshot = state.snapshot.read().await.simulation.clone();
    Json(simulations)
}

async fn get_fees(State(state): State<DashboardState>) -> impl IntoResponse {
    Json(state.snapshot.read().await.last_network_fee.clone())
}

async fn get_pools(State(state): State<DashboardState>) -> impl IntoResponse {
    Json(state.snapshot.read().await.pools.clone())
}

async fn get_rwa_state(State(state): State<DashboardState>) -> impl IntoResponse {
    let rwa: RwaSnapshot = state.snapshot.read().await.rwa.clone();
    Json(rwa)
}

async fn get_rwa_opportunities(State(state): State<DashboardState>) -> impl IntoResponse {
    Json(state.snapshot.read().await.rwa.opportunities.clone())
}

async fn get_rwa_pnl(State(state): State<DashboardState>) -> impl IntoResponse {
    Json(state.snapshot.read().await.rwa.pnl.clone())
}

async fn get_rwa_assets(State(state): State<DashboardState>) -> impl IntoResponse {
    Json(state.snapshot.read().await.rwa.assets.clone())
}

async fn pause(State(state): State<DashboardState>) -> impl IntoResponse {
    state.control.pause().await;
    (
        StatusCode::OK,
        Json(json!({"ok": true, "status": "paused"})),
    )
}

async fn resume(State(state): State<DashboardState>) -> impl IntoResponse {
    state.control.resume().await;
    (
        StatusCode::OK,
        Json(json!({"ok": true, "status": "running"})),
    )
}

async fn emergency_stop(State(state): State<DashboardState>) -> impl IntoResponse {
    state.control.emergency_stop().await;
    (
        StatusCode::OK,
        Json(json!({"ok": true, "status": "emergency_stopped"})),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{BotStatus, ExecutionPathSnapshot, StateSnapshot};
    use crate::types::{PoolKind, PoolState, TokenPair, Venue, WSOL_MINT};

    fn minimal_pool() -> PoolState {
        PoolState {
            venue: Venue::Orca,
            kind: PoolKind::ConstantProduct,
            address: "pool111111111111111111111111111111111111111".to_string(),
            pair: TokenPair {
                base_mint: "base111111111111111111111111111111111111111".to_string(),
                quote_mint: WSOL_MINT.to_string(),
                base_decimals: 6,
                quote_decimals: 9,
            },
            reserve_base_atoms: 1_000_000,
            reserve_quote_atoms: 1_000_000_000,
            sqrt_price_x64: None,
            active_bin_id: None,
            liquidity_quote_atoms: 1_000_000_000,
            fee_bps: 30,
            protocol_fee_bps: 0,
            updated_slot: 1,
        }
    }

    #[test]
    fn health_snapshot_reports_starting_before_first_cycle() {
        let mut state = StateSnapshot::new(true);
        state.status = BotStatus::Running;
        state.execution.submission_enabled = true;

        let health = health_snapshot(&state);

        assert!(!health.ok);
        assert_eq!(health.phase, HealthPhase::Starting);
        assert!(health
            .warnings
            .iter()
            .any(|warning| warning.contains("first cycle")));
    }

    #[test]
    fn health_snapshot_surfaces_route_and_rwa_blockers() {
        let mut state = StateSnapshot::new(true);
        state.status = BotStatus::Running;
        state.cycle = 3;
        state.pools = vec![minimal_pool()];
        state.execution.submission_enabled = true;
        state.execution.paths.push(ExecutionPathSnapshot {
            name: "titan".to_string(),
            kind: "api".to_string(),
            quote_source: "wss://api.titan.ag/api/v1/ws".to_string(),
            swap_source: "titan".to_string(),
            enabled: true,
            active: false,
            readiness: crate::engine::ExecutionPathReadiness::AuthMissing,
            requires_api_key: true,
            has_api_key: false,
            decoded_pools: 0,
            reserve_backed_pools: 0,
            quote_count: 0,
            status: "provider requires authentication before it can be queried".to_string(),
        });
        state.rwa.enabled = true;
        state.rwa.execution.submission_enabled = true;
        state
            .rwa
            .execution
            .readiness_blockers
            .push("NVDA US equity session closed for Pyth feed".to_string());

        let health = health_snapshot(&state);

        assert!(health.ok);
        assert_eq!(health.phase, HealthPhase::RunningWithWarnings);
        assert_eq!(health.missing_provider_keys, vec!["titan"]);
        assert_eq!(
            health.readiness_blockers,
            vec!["NVDA US equity session closed for Pyth feed"]
        );
        assert_eq!(health.paths.len(), 1);
        assert_eq!(
            health.paths[0].readiness,
            crate::engine::ExecutionPathReadiness::AuthMissing
        );
    }

    #[test]
    fn health_snapshot_reports_rwa_only_monitoring_readiness() {
        let mut state = StateSnapshot::new(true);
        state.status = BotStatus::Running;
        state.cycle = 4;
        state.rwa.enabled = true;
        state.rwa.watched_assets = 3;
        state.rwa.oracle_prices = 3;
        state.rwa.quote_count = 8;
        state.rwa.opportunity_count = 1;
        state.rwa.quote_status = "RWA execution candidates are visible for monitoring".to_string();
        state.execution.candidate_source = "rwa-only mode; broad execution disabled".to_string();
        state.execution.reason = "trading.live=true, but execution.enabled=false".to_string();
        state.rwa.execution.reason =
            "rwa.execution_enabled=false; RWA live execution is off".to_string();

        let health = health_snapshot(&state);

        assert!(health.rwa_only_mode);
        assert_eq!(health.runtime_mode, "rwa_only");
        assert!(health.rwa_monitoring_ready);
        assert!(!health.rwa_execution_enabled);
        assert!(health
            .warnings
            .iter()
            .any(|warning| warning.contains("submission lanes are disabled")));
    }

    #[test]
    fn index_separates_focused_native_route_from_reference_probes_and_execution_attempts() {
        let html = tokio::runtime::Runtime::new()
            .expect("tokio runtime")
            .block_on(index())
            .0;

        assert!(html.contains("Focused Native Route Readiness"));
        assert!(html.contains("focused-route-table"));
        assert!(html.contains("Jupiter Reference Probes"));
        assert!(html.contains("Broad Scan Opportunities"));
        assert!(html.contains("Execution Attempts"));
        assert!(html.contains("function focusedRouteTable"));
        assert!(html.contains("native_net_bps"));
        assert!(html.contains("latency_trace"));
        assert!(html.contains("submit_state"));
    }
}
