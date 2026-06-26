#!/usr/bin/env node

const dashboardUrl = process.env.ARB_DASHBOARD_URL || "http://127.0.0.1:8791";
const stateUrl = new URL("/state", dashboardUrl);

const SYMBOLS_BY_MINT = new Map([
  ["EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "USDC"],
  ["So11111111111111111111111111111111111111112", "WSOL"],
  ["98sMhvDwXj1RQi5c5Mndm3vPe9cBqPrbLaufMXFNMh5g", "HYPE"],
  ["6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN", "TRUMP"],
  ["9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump", "FARTCOIN"],
  ["EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm", "WIF"],
  ["Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", "USDT"],
  ["XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W", "SPYx"],
  ["k18WJUULWheRkSpSquYGdNNmtuE2Vbw1hpuUi92ondo", "SPYon"],
  ["Xsc9qvGR1efVDFGLrVsmkzv3qi45LTBjeUKSPmx9qEh", "NVDAx"],
  ["gEGtLTPNQ7jcg25zTetkbmF7teoDLcrfTnQfmn2ondo", "NVDAon"],
  ["XsDoVfqeBukxuZHWhdvWHBhgEHjGNst4MLodqsJHzoB", "TSLAx"],
  ["KeGv7bsfR4MheC1CkmnAVceoApjrkvBhHYjWb67ondo", "TSLAon"],
  ["Xs8S1uUs1zvS2p7iwtsG3b6fkhpvmwz4GYU3gWAmWHZ", "QQQx"],
  ["HrYNm6jTQ71LoFphjVKBTdAE4uja7WsmLG8VxB8ondo", "QQQon"],
  ["XsbEhLAtcf6HdfpFZ5xEMdqW8nfAvcsP5bdudRLJzJp", "AAPLx"],
  ["123mYEnRLM2LLYsJW3K6oyYh8uP1fngj732iG638ondo", "AAPLon"],
  ["XsueG8BtpquVJX9LVLLEGuViXUungE6WmK5YZ3p3bd1", "CRCLx"],
  ["6xHEyem9hmkGtVq6XGCiQUGpPsHBaoYuYdFNZa5ondo", "CRCLon"],
  ["XsP7xzNPvEHS1m6qfanPUGjNmdnmsLKEoNAnHjdxxyZ", "MSTRx"],
  ["FSz4ouiqXpHuGPcpacZfTzbMjScoj5FfzHkiyu2ondo", "MSTRon"],
  ["Xs7ZdzSHLU9ftNJsii5fCeJhoRWSC32SQGzGQtePxNu", "COINx"],
  ["5u6KDiNJXxX4rGMfYT4BApZQC5CuDNrG6MHkwp1ondo", "COINon"],
  ["XsCPL9dNWBMvFtTmwcCA5v3xWPSMEBCszbQdiLLq6aN", "GOOGLx"],
  ["bbahNA5vT9WJeYft8tALrH1LXWffjwqVoUbqYa1ondo", "GOOGLon"],
  ["Xs78JED6PFZxWc2wCEPspZW9kL3Se5J7L5TChKgsidH", "STRCx"],
  ["y6kSRF4i9tfMMjZziPHtQE1PeUS6bWEHTzZMFgXondo", "STRCon"],
  ["XsvNBAYkrDRNhA7wPHQfX3ZUXZyZLdnCQDfHZ56bzpg", "HOODx"],
  ["BVdXGvmgi6A9oAiwWvBvP76fyTqcCNRJMM7zMN6ondo", "HOODon"],
  ["Xsv9hRk1z5ystj9MhnA7Lq4vjSsLwzL2nxrwmwtD3re", "GLDx"],
  ["hWfiw4mcxT8rnNFkk6fsCQSxoxgZ9yVhB6tyeVcondo", "GLDon"],
  ["Xs3eBt7uRfJX8QUs4suhyU8p2M6DoUDrJyWBa8LLZsg", "AMZNx"],
  ["14Tqdo8V1FhzKsE3W2pFsZCzYPQxxupXRcqw9jv6ondo", "AMZNon"],
  ["XspzcW1PRtgf6Wj92HCiZdjzKCyFekVD8P5Ueh3dRMX", "MSFTx"],
  ["FRmH6iRkMr33DLG6zVLR7EM4LojBFAuq6NtFzG6ondo", "MSFTon"],
  ["Xsa62P5mvPszXL1krVUnU5ar38bBSVcWAB6fmPCo5Zu", "METAx"],
  ["fDxs5y12E7x7jBwCKBXGqt71uJmCWsAQ3Srkte6ondo", "METAon"],
  ["XsoBhf2ufR8fTyNSjqfU71DYGaE6Z3SUGAidpzriAA4", "PLTRx"],
  ["HfsnTS5qtdStwec9DfBrunRqnAMYMMz1kjv9Hu9ondo", "PLTRon"],
  ["XsgSaSvNSqLTtFuyWPBhK9196Xb9Bbdyjj4fH3cPJGo", "AVGOx"],
  ["1FWZtdWN7y38BSXGzbs8D6Shk88oL9atDNgbVz9ondo", "AVGOon"],
  ["XshPgPdXFRWB8tP1j82rebb2Q9rPgGX37RuqzohmArM", "INTCx"],
  ["cJpUMp5R7rZ6fGeLHbHhrRuJzK9mkyKDjZqNpT3ondo", "INTCon"],
  ["XsqE9cRRpzxcGKDXj1BJ7Xmg4GRhZoyY1KpmGSxAWT2", "MCDx"],
  ["EUbJjmDt8JA222M91bVLZs211siZ2jzbFArH9N3ondo", "MCDon"],
]);

function fmt(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "--";
  return Number(value).toFixed(digits);
}

function short(value) {
  if (!value) return "--";
  const text = String(value);
  if (text.length <= 12) return text;
  return `${text.slice(0, 4)}...${text.slice(-4)}`;
}

function symbol(mint) {
  return SYMBOLS_BY_MINT.get(mint) || short(mint);
}

function pairLabel(pair) {
  if (!pair) return "--";
  return `${symbol(pair.base_mint)}/${symbol(pair.quote_mint)}`;
}

function tokenList(tokens) {
  return (tokens || []).map(symbol).join(",") || "--";
}

function routeLabel(item) {
  return `${item?.buy_venue || "?"}->${item?.sell_venue || "?"}`;
}

function msLabel(ms) {
  if (!ms) return "--";
  if (ms < 1_000) return `${ms}ms`;
  return `${Math.ceil(ms / 1_000)}s`;
}

function optimizerLabel(candidate) {
  if (!candidate?.optimizer_decision_id) return "optimizer=--";
  return `optimizer=${short(candidate.optimizer_decision_id)} ` +
    `selected=${candidate.optimizer_selected_input_quote_atoms || "--"} ` +
    `net=${candidate.optimizer_net_profit_quote_atoms || "--"}atoms/` +
    `$${fmt(candidate.optimizer_net_profit_usd, 6)}/` +
    `${fmt(candidate.optimizer_net_profit_bps)}bps`;
}

function readinessCounts(counts) {
  if (!counts?.length) return "--";
  return counts
    .map((count) => `${count.readiness_state || "unknown"}:${count.count || 0}`)
    .join(",");
}

function latestBlocker(blockers) {
  const blocker = blockers?.[0];
  if (!blocker) return "--";
  return `${blocker.readiness_state || "unknown"}:${blocker.blocker || "--"}x${blocker.count || 0}`;
}

function latencyLabel(trace) {
  if (!trace?.length) return "--";
  return trace.map((mark) => `${mark.stage}:${mark.elapsed_ms}ms`).join(",");
}

function rwaModeLabel(state, execution, rwa) {
  if (rwa?.enabled && execution?.candidate_source === "rwa-only mode; broad execution disabled") {
    return "rwa_only";
  }
  if (rwa?.enabled) return "mixed_rwa";
  return "broad";
}

function reconciliationLabel(reconciliation) {
  if (!reconciliation) return "";
  const account = reconciliation.account_snapshot;
  const accountLabel = account
    ? ` accounts=${account.ready_accounts || 0}/${account.total_accounts || 0}` +
      ` req=${account.required_accounts || 0}` +
      ` age=${msLabel(account.max_wall_clock_age_ms)}` +
      ` slot_age=${account.max_slot_age ?? "--"}` +
      ` account_blocker=${account.blocker || "--"}`
    : "";
  return ` recon_opt=${reconciliation.optimizer_input_quote_atoms || "--"}` +
    `->${reconciliation.optimizer_buy_output_base_atoms || "--"}` +
    `->${reconciliation.optimizer_sell_output_quote_atoms || "--"}` +
    `/${fmt(reconciliation.optimizer_net_profit_bps)}bps` +
    ` status=${reconciliation.fresh_quote_status || "--"}` +
    ` complete=${reconciliation.fresh_requote_complete ?? "--"}` +
    ` recon_fresh=${reconciliation.fresh_buy_output_base_atoms || "--"}` +
    `->${reconciliation.fresh_sell_output_quote_atoms || "--"}` +
    `/${fmt(reconciliation.fresh_net_profit_bps)}bps` +
    ` fresh_net=${reconciliation.fresh_net_profit_quote_atoms || "--"}atoms` +
    ` cost=${reconciliation.execution_cost_quote_atoms || "--"}atoms` +
    `${reconciliation.fresh_blocked_reason ? ` fresh_blocker=${reconciliation.fresh_blocked_reason}` : ""}` +
    `${accountLabel}`;
}

function samePair(a, b) {
  return a?.base_mint === b?.base_mint && a?.quote_mint === b?.quote_mint;
}

function matchingTradeForFamily(trades, family) {
  return (trades || []).find((trade) =>
    samePair(trade.pair, family.pair) &&
    String(trade.buy_venue) === String(family.buy_venue) &&
    String(trade.sell_venue) === String(family.sell_venue),
  );
}

function printRows(title, rows, render) {
  console.log(`\n${title}`);
  if (!rows?.length) {
    console.log("  --");
    return;
  }
  for (const row of rows) {
    console.log(`  ${render(row)}`);
  }
}

async function main() {
  const response = await fetch(stateUrl);
  if (!response.ok) {
    throw new Error(`GET ${stateUrl} failed with HTTP ${response.status}`);
  }
  const state = await response.json();
  const execution = state.execution || {};
  const simulation = state.simulation || {};
  const scan = state.scan || {};
  const rwa = state.rwa || {};
  const rwaExecution = rwa.execution || {};
  const mode = rwaModeLabel(state, execution, rwa);

  console.log(`dashboard: ${stateUrl}`);
  console.log(
    `bot: mode=${mode} status=${state.status} live=${state.live} cycle=${state.cycle} ` +
      `tokens=${tokenList(state.active_tokens)}`,
  );
  console.log(
    `rwa: enabled=${!!rwa.enabled} watched=${rwa.watched_assets || 0} ` +
      `oracles=${rwa.oracle_prices || 0} quotes=${rwa.quote_count || 0} ` +
      `opps=${rwa.opportunity_count || 0} market_open=${rwa.market_session_open ?? "--"} ` +
      `jup_v2=${!!rwa.jupiter_v2_enabled} taker=${!!rwa.jupiter_v2_taker_available}`,
  );
  console.log(
    `rwa_execution: live_requested=${!!rwaExecution.live_requested} ` +
      `submission_enabled=${!!rwaExecution.submission_enabled} ` +
      `configured_trade=$${fmt(rwaExecution.configured_trade_usd, 2)} ` +
      `stable=$${fmt(rwaExecution.stable_available_usd, 6)} ` +
      `quote_funding=$${fmt(rwaExecution.quote_funding_available_usd, 6)} ` +
      `fundable=${rwaExecution.fundable_direct_candidates || 0} ` +
      `inventory_blocked=${rwaExecution.inventory_blocked_opportunities || 0} ` +
      `route_gated=${rwaExecution.route_gated_opportunities || 0} ` +
      `attempts=${rwaExecution.attempted_count || 0} submitted=${rwaExecution.submitted_count || 0} ` +
      `confirmed=${rwaExecution.confirmed_count || 0} failed=${rwaExecution.failed_count || 0}`,
  );
  console.log(`rwa_quote_status: ${rwa.quote_status || "--"}`);
  console.log(`rwa_execution_reason: ${rwaExecution.reason || "--"}`);
  if (rwa.last_error || rwaExecution.last_error) {
    console.log(`rwa_error: ${rwaExecution.last_error || rwa.last_error}`);
  }
  if (rwaExecution.last_signature) {
    console.log(`rwa_last_signature: ${rwaExecution.last_signature}`);
  }
  console.log(
    `scan: targets=${scan.target_pairs?.length || 0} pools=${state.pools?.length || 0} ` +
      `quotes=${state.quotes?.length || 0} opps=${state.opportunities?.length || 0} ` +
      `min=${fmt(scan.min_net_profit_bps)}bps trade=$${fmt(scan.trade_size_usd, 2)}`,
  );
  console.log(
    `execution: live_requested=${execution.live_requested} ` +
      `submission_enabled=${execution.submission_enabled} ` +
      `route_families=${execution.route_family_count || 0} route_edges=${execution.route_edge_count || 0} ` +
      `native_focus_routes=${execution.native_focus_route_count || 0} ` +
      `attempts=${execution.attempted_count || 0} blocked=${execution.blocked_count || 0} ` +
      `confirmed=${execution.confirmed_count || 0} failed=${execution.failed_count || 0}`,
  );
  const freshTruth = execution.fresh_quote_truth || {};
  console.log(
    `fresh_quote_truth: gate_open=${freshTruth.execution_gate_open || false} ` +
      `complete_pos=${freshTruth.fresh_positive_complete || 0} ` +
      `blocked_pos=${freshTruth.fresh_positive_blocked || 0} ` +
      `complete_neg=${freshTruth.fresh_negative_complete || 0} ` +
      `unavailable=${freshTruth.fresh_quote_unavailable || 0} ` +
      `exec_candidates=${freshTruth.executable_candidate_count || 0} ` +
      `rejected=${freshTruth.rejected_candidate_count || 0}`,
  );
  const liveRisk = execution.live_risk || {};
  console.log(
    `live_risk: gate_open=${liveRisk.gate_open || false} ` +
      `failed_fee_losses=${liveRisk.failed_fee_loss_count || 0}/${liveRisk.max_failed_transactions_per_session || 0} ` +
      `fee_loss_cap_usd=$${fmt(liveRisk.max_fee_loss_usd_per_session, 2)} ` +
      `blocker=${liveRisk.blocked_reason || "--"}`,
  );
  console.log(`execution_reason: ${execution.reason || "--"}`);
  if (execution.last_signature) console.log(`last_signature: ${execution.last_signature}`);
  if (execution.last_error) console.log(`last_error: ${execution.last_error}`);
  console.log(
    `simulation: label=scouting_reference enabled=${simulation.enabled} attempts=${simulation.attempted_count || 0} ` +
      `profitable=${simulation.profitable_count || 0} ` +
      `unprofitable=${simulation.unprofitable_count || 0} pnl=$${fmt(simulation.total_net_profit_usd, 6)}`,
  );

  printRows("rwa_assets", rwa.assets || [], (asset) =>
    `${asset.ticker} enabled=${asset.enabled} oracle=${asset.oracle?.status || "--"}` +
    `/age=${msLabel(asset.oracle?.age_ms)} pyth=$${fmt(asset.oracle?.price_usd, 4)} ` +
    `${asset.xstock_symbol}=$${fmt(asset.xstock_price_usd, 4)} ` +
    `${asset.ondo_symbol}=$${fmt(asset.ondo_normalized_price_usd, 4)}norm ` +
    `prem=${fmt(asset.xstock_premium_bps)}/${fmt(asset.ondo_premium_bps)}bps ` +
    `quotes=${asset.quote_count || 0} direct=${asset.direct_route_count || 0}/${asset.direct_route_request_count || 0} ` +
    `primary=${asset.primary_route_count || 0} blocker=${asset.no_trade_reason || "--"}`,
  );

  printRows("rwa_opportunities", rwa.opportunities || [], (opp) =>
    `${opp.ticker} ${opp.kind} ${opp.from_symbol}->${opp.to_symbol} ` +
    `input=$${fmt(opp.input_estimated_usd, 4)} fair=$${fmt(opp.pyth_fair_price_usd, 4)} ` +
    `gross=${fmt(opp.gross_edge_bps)}bps net=${fmt(opp.net_profit_bps)}bps/$${fmt(opp.net_profit_usd, 6)} ` +
    `executable=${!!opp.executable} legs=${opp.execution_legs?.length || 0} ` +
    `route=${opp.route || "--"} blocker=${opp.blocked_reason || "--"}`,
  );

  printRows("rwa_execution_records", rwaExecution.trades || [], (trade) =>
    `#${trade.id} cycle=${trade.cycle} ${trade.ticker} ${trade.kind} ` +
    `${trade.from_symbol}->${trade.to_symbol} input=$${fmt(trade.estimated_input_usd, 4)} ` +
    `expected=$${fmt(trade.expected_net_profit_usd, 6)}/${fmt(trade.expected_net_profit_bps)}bps ` +
    `submitted=${!!trade.submitted} sim_ok=${!!trade.simulation_ok} sig=${short(trade.signature)} ` +
    `routes=${(trade.leg_routes || []).join(">") || "--"} reason=${trade.reason || "--"}`,
  );

  printRows("quote_providers", scan.quote_providers || [], (provider) => {
    const backoff = [
      provider.budget_exhaustion_strikes ? `${provider.budget_exhaustion_strikes} budget` : "",
      provider.provider_error_strikes ? `${provider.provider_error_strikes} errors` : "",
      provider.rate_limit_strikes ? `${provider.rate_limit_strikes} rate` : "",
    ].filter(Boolean).join(", ");
    return `${provider.name} ${provider.venue}/${provider.format} ` +
      `available=${provider.available} cooldown=${msLabel(provider.cooldown_remaining_ms)} ` +
      `next=${msLabel(provider.next_request_in_ms)} ${backoff || ""} status=${provider.status || "--"}`;
  });

  printRows("wallet_quote_balances", execution.wallet_quote_balances || [], (balance) =>
    `${balance.symbol || symbol(balance.mint)} ${fmt(balance.ui_amount, 8)} ` +
    `atoms=${balance.amount_atoms || "--"} mint=${symbol(balance.mint)}`,
  );

  printRows("focused_route_families", execution.focused_route_families || [], (family) => {
    const latest = matchingTradeForFamily(execution.trades, family);
    const nativeNetBps = fmt(latest?.requote_net_profit_bps);
    const submitState = latest
      ? `${latest.status || "--"}/submitted=${latest.submitted}/simulation_ok=${latest.simulation_ok}`
      : "--";
    return `#${family.id} ${pairLabel(family.pair)} ${family.buy_venue}->${family.sell_venue} ` +
      `enabled=${family.enabled} edges=${family.total_edges || 0} priority=${family.priority || 0} ` +
      `account_readiness=${readinessCounts(family.readiness_counts)} ` +
      `quote_readiness=${latest?.jupiter_reference_error ? `blocked:${latest.jupiter_reference_error}` : "latest_or_family"} ` +
      `native_net_bps=${nativeNetBps} latency=${latencyLabel(latest?.latency_trace)} ` +
      `submit_state=${submitState} blocker=${latest?.reason || latestBlocker(family.latest_blockers)}`;
  });

  printRows("focused_route_candidate_rejections", execution.focused_route_candidate_rejections || [], (rejection) =>
    `#${rejection.route_edge_id || "--"} ${pairLabel(rejection.pair)} ` +
    `${symbol(rejection.buy_pool)} -> ${symbol(rejection.sell_pool)} ` +
    `${rejection.buy_venue || "--"}->${rejection.sell_venue || "--"} ` +
    `reason=${rejection.reason || "--"}` +
    reconciliationLabel(rejection.fresh_quote_reconciliation),
  );

  printRows("jupiter_reference_probes", execution.selected_candidates || [], (candidate) =>
    `${pairLabel(candidate.pair)} ${routeLabel(candidate)} key=${candidate.key || "--"} ` +
    `input=$${fmt(candidate.input_usd, 2)} ` +
    `${optimizerLabel(candidate)} ` +
    `attempts=${candidate.attempts || 0} signal=${fmt(candidate.last_direct_signal_bps)}bps ` +
    `last_exec=${fmt(candidate.last_requote_bps)}bps best_exec=${fmt(candidate.best_requote_bps)}bps ` +
    `jup_ref=${fmt(candidate.last_jupiter_reference_bps)}bps ` +
    `floor=${fmt(candidate.learned_min_signal_bps)}bps cooldown=${msLabel(candidate.cooldown_remaining_ms)} ` +
    `reason=${candidate.last_reason || "--"}`,
  );

  printRows("best_recent_jupiter_reference_probes", execution.best_recent_candidates || [], (candidate) =>
    `${pairLabel(candidate.pair)} ${routeLabel(candidate)} key=${candidate.key || "--"} ` +
    `input=$${fmt(candidate.input_usd, 2)} ` +
    `${optimizerLabel(candidate)} ` +
    `attempts=${candidate.attempts || 0} signal=${fmt(candidate.last_direct_signal_bps)}bps ` +
    `last_exec=${fmt(candidate.last_requote_bps)}bps best_exec=${fmt(candidate.best_requote_bps)}bps ` +
    `jup_ref=${fmt(candidate.last_jupiter_reference_bps)}bps ` +
    `floor=${fmt(candidate.learned_min_signal_bps)}bps cooldown=${msLabel(candidate.cooldown_remaining_ms)} ` +
    `reason=${candidate.last_reason || "--"}`,
  );

  printRows("recent_execution_records", execution.trades || [], (trade) =>
    `#${trade.id} cycle=${trade.cycle} ${pairLabel(trade.pair)} ${routeLabel(trade)} ` +
    `signal=$${fmt(trade.direct_signal_net_profit_usd, 6)} ` +
    `native=${fmt(trade.requote_net_profit_bps)}bps ` +
    `buy=${trade.buy_quote_out_amount || "--"} sell=${trade.sell_quote_out_amount || "--"} ` +
    `jup_ref=${fmt(trade.jupiter_reference_net_profit_bps)}bps ` +
    `${trade.jupiter_reference_error ? `jup_err=${trade.jupiter_reference_error} ` : ""}` +
    `setup=${trade.native_execution_cost_quote_atoms || "--"}atoms/${trade.native_setup_missing_atas ?? "--"}ata ` +
    `sim_delta=${trade.native_simulation_quote_delta_atoms || "--"} ` +
    `sim_net=${trade.native_simulation_net_profit_atoms || "--"} ` +
    `sim_bal=${trade.native_simulation_pre_quote_atoms || "--"}->${trade.native_simulation_post_quote_atoms || "--"} ` +
    `latency=${latencyLabel(trade.latency_trace)} ` +
    `bytes=${trade.signed_transaction_bytes || "--"} ` +
    `status=${trade.status} ` +
    `submitted=${trade.submitted} sim_ok=${trade.simulation_ok} sig=${short(trade.signature)} ` +
    `reason=${trade.reason || "--"}`,
  );
}

main().catch((error) => {
  console.error(error?.stack || error?.message || String(error));
  process.exitCode = 1;
});
