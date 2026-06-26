use crate::types::{
    FocusedRouteAccountSnapshot, FocusedRouteAccountStatus, FocusedRouteReadinessSnapshot,
    FocusedRouteRequoteEvent, FocusedRouteRequoteLatencySnapshot, RawAccount,
    RouteAccountRequirement,
};
use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::{interval, sleep};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
pub struct CachedAccount {
    pub pubkey: String,
    pub owner: String,
    pub lamports: u64,
    pub data: Vec<u8>,
    pub slot: u64,
    pub updated_at: Instant,
}

#[derive(Debug, Clone, Default)]
pub struct SubscriptionStats {
    pub desired: usize,
    pub pending: usize,
    pub confirmed: usize,
    pub rejected: usize,
    pub budget: usize,
    pub budget_remaining: usize,
}

#[derive(Debug, Clone)]
struct RejectedSubscription {
    rejected_at: Instant,
}

#[derive(Debug, Clone)]
struct FocusedRouteRegistration {
    requirements: Vec<RouteAccountRequirement>,
    max_account_age_ms: u64,
    debounce_ms: u64,
}

#[derive(Debug, Clone)]
struct FocusedRouteRequoteState {
    last_trigger_pubkey: String,
    last_update_slot: u64,
    last_update_at: Instant,
    due_at: Option<Instant>,
    quote_started_at: Option<Instant>,
    submit_started_at: Option<Instant>,
    update_to_quote_latency_ms: Option<u64>,
    quote_to_submit_latency_ms: Option<u64>,
}

pub struct PoolSubscriptionManager {
    ws_url: String,
    cache: Arc<RwLock<HashMap<String, CachedAccount>>>,
    subscribed: Arc<RwLock<Vec<String>>>,
    rejected: Arc<RwLock<HashMap<String, RejectedSubscription>>>,
    next_id: Arc<AtomicU64>,
    connected: Arc<RwLock<bool>>,
    last_update_slot: Arc<RwLock<u64>>,
    subscription_count: Arc<RwLock<usize>>,
    subscription_stats: Arc<RwLock<SubscriptionStats>>,
    subscription_budget: Arc<RwLock<usize>>,
    shutdown: Arc<RwLock<bool>>,
    plan_keys_cache: Arc<RwLock<HashMap<String, Vec<String>>>>,
    focused_routes: Arc<RwLock<HashMap<i64, FocusedRouteRegistration>>>,
    focused_account_routes: Arc<RwLock<HashMap<String, HashSet<i64>>>>,
    focused_requote_state: Arc<RwLock<HashMap<i64, FocusedRouteRequoteState>>>,
}

#[derive(Debug, Clone)]
pub struct PoolPlanKeys {
    pub pool_address: String,
    pub initial_keys: Vec<String>,
    pub followup_keys: Vec<String>,
}

impl PoolPlanKeys {
    pub fn all_keys(&self) -> Vec<String> {
        let mut keys = vec![self.pool_address.clone()];
        keys.extend(self.initial_keys.clone());
        keys.extend(self.followup_keys.clone());
        keys
    }
}

impl PoolSubscriptionManager {
    pub fn new(ws_url: String) -> Self {
        Self::new_with_budget(ws_url, usize::MAX)
    }

    pub fn new_with_budget(ws_url: String, budget: usize) -> Self {
        Self {
            ws_url,
            cache: Arc::new(RwLock::new(HashMap::new())),
            subscribed: Arc::new(RwLock::new(Vec::new())),
            rejected: Arc::new(RwLock::new(HashMap::new())),
            next_id: Arc::new(AtomicU64::new(1)),
            connected: Arc::new(RwLock::new(false)),
            last_update_slot: Arc::new(RwLock::new(0)),
            subscription_count: Arc::new(RwLock::new(0)),
            subscription_stats: Arc::new(RwLock::new(SubscriptionStats {
                budget,
                budget_remaining: budget,
                ..SubscriptionStats::default()
            })),
            subscription_budget: Arc::new(RwLock::new(budget)),
            shutdown: Arc::new(RwLock::new(false)),
            plan_keys_cache: Arc::new(RwLock::new(HashMap::new())),
            focused_routes: Arc::new(RwLock::new(HashMap::new())),
            focused_account_routes: Arc::new(RwLock::new(HashMap::new())),
            focused_requote_state: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn subscribe(&self, pubkeys: &[String]) {
        if pubkeys.is_empty() {
            return;
        }
        {
            let mut list = self.subscribed.write().await;
            for key in pubkeys {
                if !list.contains(key) {
                    list.push(key.clone());
                }
            }
        }
        self.refresh_stats_without_socket().await;
    }

    pub async fn set_subscriptions(&self, pubkeys: &[String]) {
        let mut unique = Vec::new();
        for key in pubkeys {
            if !unique.contains(key) {
                unique.push(key.clone());
            }
        }
        {
            *self.subscribed.write().await = unique;
        }
        self.refresh_stats_without_socket().await;
    }

    pub async fn unsubscribe(&self, pubkeys: &[String]) {
        {
            let mut list = self.subscribed.write().await;
            list.retain(|k| !pubkeys.contains(k));
        }
        self.refresh_stats_without_socket().await;
    }

    pub async fn get_cached(&self, pubkey: &str) -> Option<CachedAccount> {
        self.cache.read().await.get(pubkey).cloned()
    }

    pub async fn warm_raw_accounts(&self, accounts: Vec<RawAccount>) {
        if accounts.is_empty() {
            return;
        }
        let now = Instant::now();
        let mut max_slot = 0u64;
        {
            let mut cache = self.cache.write().await;
            for account in accounts {
                max_slot = max_slot.max(account.slot);
                cache.insert(
                    account.pubkey.clone(),
                    CachedAccount {
                        pubkey: account.pubkey,
                        owner: account.owner,
                        lamports: account.lamports,
                        data: account.data,
                        slot: account.slot,
                        updated_at: now,
                    },
                );
            }
        }
        if max_slot > 0 {
            let mut last_slot = self.last_update_slot.write().await;
            if max_slot > *last_slot {
                *last_slot = max_slot;
            }
        }
    }

    pub async fn get_cached_data(&self, pubkeys: &[String]) -> Vec<Option<Vec<u8>>> {
        let cache = self.cache.read().await;
        pubkeys
            .iter()
            .map(|k| cache.get(k).map(|a| a.data.clone()))
            .collect()
    }

    pub async fn get_cached_raw_accounts(&self, pubkeys: &[String]) -> Vec<Option<RawAccount>> {
        let cache = self.cache.read().await;
        pubkeys
            .iter()
            .map(|k| {
                cache.get(k).map(|a| RawAccount {
                    pubkey: a.pubkey.clone(),
                    owner: a.owner.clone(),
                    lamports: a.lamports,
                    slot: a.slot,
                    data: a.data.clone(),
                })
            })
            .collect()
    }

    pub async fn is_connected(&self) -> bool {
        *self.connected.read().await
    }

    pub async fn last_update_slot(&self) -> u64 {
        *self.last_update_slot.read().await
    }

    pub async fn subscription_count(&self) -> usize {
        *self.subscription_count.read().await
    }

    pub async fn subscription_stats(&self) -> SubscriptionStats {
        let mut stats = self.subscription_stats.read().await.clone();
        let desired = self.subscribed.read().await.len();
        let budget = *self.subscription_budget.read().await;
        let active = stats.pending.saturating_add(stats.confirmed);
        stats.desired = desired;
        stats.budget = budget;
        stats.budget_remaining = budget.saturating_sub(active);
        stats.rejected = self.live_rejected_count().await;
        stats
    }

    pub async fn set_subscription_budget(&self, budget: usize) {
        *self.subscription_budget.write().await = budget;
        self.refresh_stats_without_socket().await;
    }

    pub async fn cache_size(&self) -> usize {
        self.cache.read().await.len()
    }

    pub fn cache_arc(&self) -> Arc<RwLock<HashMap<String, CachedAccount>>> {
        self.cache.clone()
    }

    pub async fn cache_plan_keys(&self, pool_address: &str, plan_keys: PoolPlanKeys) {
        let key = pool_address.to_string();
        let all = plan_keys.all_keys();
        self.plan_keys_cache.write().await.insert(key, all);
    }

    pub async fn get_plan_keys(&self, pool_address: &str) -> Option<Vec<String>> {
        self.plan_keys_cache.read().await.get(pool_address).cloned()
    }

    pub async fn register_focused_route_accounts(
        &self,
        route_edge_id: i64,
        requirements: Vec<RouteAccountRequirement>,
        max_account_age_ms: u64,
        debounce_ms: u64,
    ) {
        {
            let mut routes = self.focused_routes.write().await;
            routes.insert(
                route_edge_id,
                FocusedRouteRegistration {
                    requirements: requirements.clone(),
                    max_account_age_ms,
                    debounce_ms,
                },
            );
        }

        let mut account_routes = self.focused_account_routes.write().await;
        for routes in account_routes.values_mut() {
            routes.remove(&route_edge_id);
        }
        account_routes.retain(|_, routes| !routes.is_empty());
        for requirement in requirements {
            account_routes
                .entry(requirement.pubkey)
                .or_default()
                .insert(route_edge_id);
        }
    }

    pub async fn focused_route_readiness(
        &self,
        route_edge_id: i64,
    ) -> Option<FocusedRouteReadinessSnapshot> {
        let registration = self
            .focused_routes
            .read()
            .await
            .get(&route_edge_id)
            .cloned()?;
        let cache = self.cache.read().await;
        let current_slot = cache
            .values()
            .map(|account| account.slot)
            .max()
            .unwrap_or_default();
        Some(build_focused_route_readiness(
            route_edge_id,
            &registration,
            &cache,
            current_slot,
        ))
    }

    pub async fn focused_route_readiness_snapshots(&self) -> Vec<FocusedRouteReadinessSnapshot> {
        let route_ids = self
            .focused_routes
            .read()
            .await
            .keys()
            .copied()
            .collect::<Vec<_>>();
        let mut snapshots = Vec::with_capacity(route_ids.len());
        for route_id in route_ids {
            if let Some(snapshot) = self.focused_route_readiness(route_id).await {
                snapshots.push(snapshot);
            }
        }
        snapshots.sort_by_key(|snapshot| snapshot.route_edge_id);
        snapshots
    }

    pub async fn ensure_focused_route_ready_for_quote(
        &self,
        route_edge_id: i64,
    ) -> Result<FocusedRouteReadinessSnapshot> {
        let snapshot = self
            .focused_route_readiness(route_edge_id)
            .await
            .ok_or_else(|| anyhow!("focused route {route_edge_id} is not registered"))?;
        if snapshot.ready {
            Ok(snapshot)
        } else {
            Err(anyhow!(
                "focused route {route_edge_id} is not ready for quote: {}",
                snapshot
                    .blocker
                    .clone()
                    .unwrap_or_else(|| "unknown blocker".to_string())
            ))
        }
    }

    pub async fn ingest_account_notification(
        &self,
        val: &Value,
        active_ids: &HashMap<u64, String>,
    ) {
        if let Some(account) = account_from_notification(val, active_ids) {
            self.apply_account_update(account).await;
        }
    }

    pub async fn pop_due_focused_requote_events(&self) -> Vec<FocusedRouteRequoteEvent> {
        let now = Instant::now();
        let mut states = self.focused_requote_state.write().await;
        let mut due = Vec::new();
        for (route_edge_id, state) in states.iter_mut() {
            if state.due_at.is_some_and(|due_at| due_at <= now) {
                state.due_at = None;
                due.push(FocusedRouteRequoteEvent {
                    route_edge_id: *route_edge_id,
                    trigger_pubkey: state.last_trigger_pubkey.clone(),
                    update_slot: state.last_update_slot,
                    update_to_quote_latency_ms: state.update_to_quote_latency_ms,
                    quote_to_submit_latency_ms: state.quote_to_submit_latency_ms,
                });
            }
        }
        due.sort_by_key(|event| event.route_edge_id);
        due
    }

    pub async fn note_focused_route_quote_started(&self, route_edge_id: i64) {
        let mut states = self.focused_requote_state.write().await;
        if let Some(state) = states.get_mut(&route_edge_id) {
            let now = Instant::now();
            state.quote_started_at = Some(now);
            state.update_to_quote_latency_ms =
                Some(duration_ms(now.duration_since(state.last_update_at)));
        }
    }

    pub async fn note_focused_route_submit_started(&self, route_edge_id: i64) {
        let mut states = self.focused_requote_state.write().await;
        if let Some(state) = states.get_mut(&route_edge_id) {
            let now = Instant::now();
            state.submit_started_at = Some(now);
            if let Some(quote_started_at) = state.quote_started_at {
                state.quote_to_submit_latency_ms =
                    Some(duration_ms(now.duration_since(quote_started_at)));
            }
        }
    }

    pub async fn focused_route_requote_latency(
        &self,
        route_edge_id: i64,
    ) -> Option<FocusedRouteRequoteLatencySnapshot> {
        let states = self.focused_requote_state.read().await;
        let state = states.get(&route_edge_id)?;
        Some(FocusedRouteRequoteLatencySnapshot {
            route_edge_id,
            last_trigger_pubkey: Some(state.last_trigger_pubkey.clone()),
            last_update_slot: Some(state.last_update_slot),
            last_update_to_quote_latency_ms: state.update_to_quote_latency_ms,
            last_quote_to_submit_latency_ms: state.quote_to_submit_latency_ms,
        })
    }

    pub async fn focused_route_requote_latency_snapshots(
        &self,
    ) -> Vec<FocusedRouteRequoteLatencySnapshot> {
        let states = self.focused_requote_state.read().await;
        let mut snapshots = states
            .iter()
            .map(
                |(route_edge_id, state)| FocusedRouteRequoteLatencySnapshot {
                    route_edge_id: *route_edge_id,
                    last_trigger_pubkey: Some(state.last_trigger_pubkey.clone()),
                    last_update_slot: Some(state.last_update_slot),
                    last_update_to_quote_latency_ms: state.update_to_quote_latency_ms,
                    last_quote_to_submit_latency_ms: state.quote_to_submit_latency_ms,
                },
            )
            .collect::<Vec<_>>();
        snapshots.sort_by_key(|snapshot| snapshot.route_edge_id);
        snapshots
    }

    async fn apply_account_update(&self, account: CachedAccount) {
        let pubkey = account.pubkey.clone();
        let slot = account.slot;
        {
            self.cache.write().await.insert(pubkey.clone(), account);
        }
        {
            let mut slot_w = self.last_update_slot.write().await;
            if slot > *slot_w {
                *slot_w = slot;
            }
        }
        self.schedule_focused_requotes_for_account(&pubkey, slot)
            .await;
    }

    async fn schedule_focused_requotes_for_account(&self, pubkey: &str, slot: u64) {
        let route_ids = self
            .focused_account_routes
            .read()
            .await
            .get(pubkey)
            .cloned()
            .unwrap_or_default();
        if route_ids.is_empty() {
            return;
        }
        let routes = self.focused_routes.read().await;
        let now = Instant::now();
        let mut states = self.focused_requote_state.write().await;
        for route_edge_id in route_ids {
            let debounce_ms = routes
                .get(&route_edge_id)
                .map(|route| route.debounce_ms)
                .unwrap_or_default();
            states.insert(
                route_edge_id,
                FocusedRouteRequoteState {
                    last_trigger_pubkey: pubkey.to_string(),
                    last_update_slot: slot,
                    last_update_at: now,
                    due_at: Some(now + Duration::from_millis(debounce_ms)),
                    quote_started_at: None,
                    submit_started_at: None,
                    update_to_quote_latency_ms: None,
                    quote_to_submit_latency_ms: None,
                },
            );
        }
    }

    pub async fn subscribe_with_plan(&self, pool_address: &str) {
        if let Some(keys) = self.get_plan_keys(pool_address).await {
            self.subscribe(&keys).await;
        } else {
            self.subscribe(&[pool_address.to_string()]).await;
        }
    }

    pub fn start(&self) {
        let ws_url = self.ws_url.clone();
        let cache = self.cache.clone();
        let subscribed = self.subscribed.clone();
        let rejected = self.rejected.clone();
        let connected = self.connected.clone();
        let last_update_slot = self.last_update_slot.clone();
        let sub_count = self.subscription_count.clone();
        let sub_stats = self.subscription_stats.clone();
        let sub_budget = self.subscription_budget.clone();
        let next_id = self.next_id.clone();
        let shutdown = self.shutdown.clone();
        let focused_routes = self.focused_routes.clone();
        let focused_account_routes = self.focused_account_routes.clone();
        let focused_requote_state = self.focused_requote_state.clone();

        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            warn!("pool subscription manager not started; no Tokio runtime is active");
            return;
        };

        handle.spawn(async move {
            let mut backoff_ms: u64 = 500;
            loop {
                {
                    let s = shutdown.read().await;
                    if *s {
                        info!("pool subscription manager shutting down");
                        break;
                    }
                }
                match run_ws_loop(
                    &ws_url,
                    &cache,
                    &subscribed,
                    &rejected,
                    &connected,
                    &last_update_slot,
                    &sub_count,
                    &sub_stats,
                    &sub_budget,
                    &next_id,
                    &shutdown,
                    &focused_routes,
                    &focused_account_routes,
                    &focused_requote_state,
                )
                .await
                {
                    Ok(()) => {
                        backoff_ms = 500;
                    }
                    Err(e) => {
                        warn!(
                            backoff_ms,
                            "pool ws disconnected, reconnecting in {}ms: {e}", backoff_ms
                        );
                    }
                }
                {
                    *connected.write().await = false;
                }
                sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(30_000);
            }
        });
    }

    pub async fn shutdown(&self) {
        *self.shutdown.write().await = true;
    }

    async fn refresh_stats_without_socket(&self) {
        let desired = self.subscribed.read().await.len();
        let budget = *self.subscription_budget.read().await;
        let rejected = self.live_rejected_count().await;
        let mut stats = self.subscription_stats.write().await;
        stats.desired = desired;
        stats.rejected = rejected;
        stats.budget = budget;
        stats.budget_remaining = budget.saturating_sub(stats.pending + stats.confirmed);
    }

    async fn live_rejected_count(&self) -> usize {
        let mut rejected = self.rejected.write().await;
        prune_rejected(&mut rejected);
        rejected.len()
    }
}

async fn run_ws_loop(
    ws_url: &str,
    cache: &Arc<RwLock<HashMap<String, CachedAccount>>>,
    subscribed: &Arc<RwLock<Vec<String>>>,
    rejected: &Arc<RwLock<HashMap<String, RejectedSubscription>>>,
    connected: &Arc<RwLock<bool>>,
    last_update_slot: &Arc<RwLock<u64>>,
    sub_count: &Arc<RwLock<usize>>,
    sub_stats: &Arc<RwLock<SubscriptionStats>>,
    sub_budget: &Arc<RwLock<usize>>,
    next_id: &Arc<AtomicU64>,
    shutdown: &Arc<RwLock<bool>>,
    focused_routes: &Arc<RwLock<HashMap<i64, FocusedRouteRegistration>>>,
    focused_account_routes: &Arc<RwLock<HashMap<String, HashSet<i64>>>>,
    focused_requote_state: &Arc<RwLock<HashMap<i64, FocusedRouteRequoteState>>>,
) -> Result<()> {
    let (mut ws, _) = connect_async(ws_url)
        .await
        .context("pool ws connect failed")?;
    info!(
        ws_url = %redacted_ws_url(ws_url),
        "pool subscription ws connected"
    );

    *connected.write().await = true;
    let mut pending_ids: HashMap<u64, String> = HashMap::new();
    let mut confirmed_ids: HashMap<u64, String> = HashMap::new();

    send_pending_subscriptions(
        &mut ws,
        subscribed,
        rejected,
        &mut pending_ids,
        &confirmed_ids,
        sub_count,
        sub_stats,
        sub_budget,
        next_id,
    )
    .await?;

    let mut refresh = interval(Duration::from_millis(100));
    loop {
        tokio::select! {
            _ = refresh.tick() => {
                {
                    let s = shutdown.read().await;
                    if *s {
                        break;
                    }
                }
                send_pending_subscriptions(
                    &mut ws,
                    subscribed,
                    rejected,
                    &mut pending_ids,
                    &confirmed_ids,
                    sub_count,
                    sub_stats,
                    sub_budget,
                    next_id,
                ).await?;
            }
            message = ws.next() => {
                {
                    let s = shutdown.read().await;
                    if *s {
                        break;
                    }
                }
                let Some(message) = message else {
                    break;
                };
                let msg = message.context("pool ws read failed")?;
                let text = match msg {
                    Message::Text(t) => t.to_string(),
                    Message::Ping(_) => {
                        continue;
                    }
                    Message::Close(_) => break,
                    _ => continue,
                };

                let val: Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                if let Some(method) = val.get("method").and_then(|m| m.as_str()) {
                    if method == "accountNotification" {
                        handle_notification(
                            &val,
                            &confirmed_ids,
                            cache,
                            last_update_slot,
                            focused_routes,
                            focused_account_routes,
                            focused_requote_state,
                        ).await;
                        continue;
                    }
                }

                if let Some(result) = val.get("result") {
                    if let Some(id) = val.get("id").and_then(|i| i.as_u64()) {
                        if !result.is_null() {
                            let sub_id = result.as_u64().unwrap_or(0);
                            if let Some(key) = pending_ids.remove(&id) {
                                confirmed_ids.insert(sub_id, key);
                                update_subscription_counts(
                                    subscribed,
                                    rejected,
                                    &pending_ids,
                                    &confirmed_ids,
                                    sub_count,
                                    sub_stats,
                                    sub_budget,
                                ).await;
                                debug!(sub_id, "pool subscription confirmed");
                            }
                        }
                    }
                }

                if let Some(error) = val.get("error") {
                    if let Some(id) = val.get("id").and_then(|i| i.as_u64()) {
                        if let Some(key) = pending_ids.remove(&id) {
                            let reason = error
                                .get("message")
                                .and_then(|message| message.as_str())
                                .unwrap_or("subscription rejected")
                                .to_string();
                            rejected.write().await.insert(
                                key.clone(),
                                RejectedSubscription {
                                    rejected_at: Instant::now(),
                                },
                            );
                            update_subscription_counts(
                                subscribed,
                                rejected,
                                &pending_ids,
                                &confirmed_ids,
                                sub_count,
                                sub_stats,
                                sub_budget,
                            ).await;
                            warn!(pubkey = %key, reason, ?error, "pool ws subscription rejected");
                            continue;
                        }
                    }
                    warn!(?error, "pool ws rpc error");
                }
            }
        }
    }

    Ok(())
}

fn redacted_ws_url(ws_url: &str) -> String {
    match ws_url.split_once('?') {
        Some((base, _)) => format!("{base}?<redacted>"),
        None => ws_url.to_string(),
    }
}

async fn send_pending_subscriptions<S>(
    ws: &mut tokio_tungstenite::WebSocketStream<S>,
    subscribed: &Arc<RwLock<Vec<String>>>,
    rejected: &Arc<RwLock<HashMap<String, RejectedSubscription>>>,
    pending_ids: &mut HashMap<u64, String>,
    confirmed_ids: &HashMap<u64, String>,
    sub_count: &Arc<RwLock<usize>>,
    sub_stats: &Arc<RwLock<SubscriptionStats>>,
    sub_budget: &Arc<RwLock<usize>>,
    next_id: &Arc<AtomicU64>,
) -> Result<usize>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let current_subs = subscribed.read().await.clone();
    {
        let mut rejected_map = rejected.write().await;
        prune_rejected(&mut rejected_map);
    }
    let rejected_pubkeys: HashSet<String> = rejected.read().await.keys().cloned().collect();
    let active_pubkeys: HashSet<String> = pending_ids
        .values()
        .chain(confirmed_ids.values())
        .cloned()
        .collect();
    let budget = *sub_budget.read().await;
    let mut sent = 0usize;

    for key in current_subs {
        if active_pubkeys.contains(&key) {
            continue;
        }
        if rejected_pubkeys.contains(&key) {
            continue;
        }
        if pending_ids.len().saturating_add(confirmed_ids.len()) >= budget {
            break;
        }
        let id = next_id.fetch_add(1, Ordering::Relaxed);
        let req = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "accountSubscribe",
            "params": [
                key,
                {
                    "encoding": "base64",
                    "commitment": "processed"
                }
            ]
        });
        let msg = serde_json::to_string(&req)?;
        ws.send(Message::Text(msg.into())).await?;
        pending_ids.insert(id, key);
        sent += 1;
    }

    update_subscription_counts(
        subscribed,
        rejected,
        pending_ids,
        confirmed_ids,
        sub_count,
        sub_stats,
        sub_budget,
    )
    .await;

    if sent > 0 {
        debug!(
            sent,
            count = pending_ids.len() + confirmed_ids.len(),
            "pool subscription requests sent"
        );
    }

    Ok(sent)
}

async fn update_subscription_counts(
    subscribed: &Arc<RwLock<Vec<String>>>,
    rejected: &Arc<RwLock<HashMap<String, RejectedSubscription>>>,
    pending_ids: &HashMap<u64, String>,
    confirmed_ids: &HashMap<u64, String>,
    sub_count: &Arc<RwLock<usize>>,
    sub_stats: &Arc<RwLock<SubscriptionStats>>,
    sub_budget: &Arc<RwLock<usize>>,
) {
    let active = pending_ids.len().saturating_add(confirmed_ids.len());
    *sub_count.write().await = active;
    let desired = subscribed.read().await.len();
    let rejected_count = {
        let mut rejected_map = rejected.write().await;
        prune_rejected(&mut rejected_map);
        rejected_map.len()
    };
    let budget = *sub_budget.read().await;
    *sub_stats.write().await = SubscriptionStats {
        desired,
        pending: pending_ids.len(),
        confirmed: confirmed_ids.len(),
        rejected: rejected_count,
        budget,
        budget_remaining: budget.saturating_sub(active),
    };
}

fn prune_rejected(rejected: &mut HashMap<String, RejectedSubscription>) {
    let now = Instant::now();
    rejected.retain(|_, rejection| {
        now.duration_since(rejection.rejected_at) < rejected_subscription_cooldown()
    });
}

fn rejected_subscription_cooldown() -> Duration {
    Duration::from_secs(60)
}

fn build_focused_route_readiness(
    route_edge_id: i64,
    registration: &FocusedRouteRegistration,
    cache: &HashMap<String, CachedAccount>,
    current_slot: u64,
) -> FocusedRouteReadinessSnapshot {
    let accounts = registration
        .requirements
        .iter()
        .map(|requirement| {
            let cached = cache.get(&requirement.pubkey);
            let actual_owner = cached.map(|account| account.owner.clone());
            let status =
                focused_account_status(requirement, cached, registration.max_account_age_ms);
            let reason = focused_account_reason(requirement, cached, &status);
            FocusedRouteAccountSnapshot {
                route_edge_id,
                role: requirement.role.clone(),
                pubkey: requirement.pubkey.clone(),
                required: requirement.required,
                expected_owner: requirement.expected_owner.clone(),
                actual_owner,
                status,
                slot: cached.map(|account| account.slot),
                slot_age: cached.map(|account| current_slot.saturating_sub(account.slot)),
                wall_clock_age_ms: cached.map(|account| duration_ms(account.updated_at.elapsed())),
                lamports: cached.map(|account| account.lamports),
                data_hash: cached.map(|account| data_hash_hex(&account.data)),
                reason,
            }
        })
        .collect::<Vec<_>>();

    let blocker = accounts
        .iter()
        .find(|account| account.required && account.status != FocusedRouteAccountStatus::Ready)
        .map(|account| {
            account
                .reason
                .clone()
                .unwrap_or_else(|| format!("{} {} is not ready", account.role, account.pubkey))
        });
    FocusedRouteReadinessSnapshot {
        route_edge_id,
        ready: blocker.is_none(),
        blocker,
        accounts,
    }
}

fn focused_account_status(
    requirement: &RouteAccountRequirement,
    cached: Option<&CachedAccount>,
    max_account_age_ms: u64,
) -> FocusedRouteAccountStatus {
    let Some(account) = cached else {
        return FocusedRouteAccountStatus::Missing;
    };
    if requirement
        .expected_owner
        .as_ref()
        .is_some_and(|expected| expected != &account.owner)
    {
        return FocusedRouteAccountStatus::OwnerMismatch;
    }
    if max_account_age_ms > 0 && duration_ms(account.updated_at.elapsed()) > max_account_age_ms {
        return FocusedRouteAccountStatus::Stale;
    }
    if account.data.is_empty() {
        return FocusedRouteAccountStatus::DecodeFailed;
    }
    FocusedRouteAccountStatus::Ready
}

fn focused_account_reason(
    requirement: &RouteAccountRequirement,
    cached: Option<&CachedAccount>,
    status: &FocusedRouteAccountStatus,
) -> Option<String> {
    match status {
        FocusedRouteAccountStatus::Ready => None,
        FocusedRouteAccountStatus::Missing => Some(format!(
            "{} account {} is missing from focused route cache",
            requirement.role, requirement.pubkey
        )),
        FocusedRouteAccountStatus::Stale => Some(format!(
            "{} account {} is stale in focused route cache",
            requirement.role, requirement.pubkey
        )),
        FocusedRouteAccountStatus::OwnerMismatch => {
            let actual = cached
                .map(|account| account.owner.as_str())
                .unwrap_or("missing");
            Some(format!(
                "{} account {} owner mismatch: expected {}, got {}",
                requirement.role,
                requirement.pubkey,
                requirement.expected_owner.as_deref().unwrap_or("unknown"),
                actual
            ))
        }
        FocusedRouteAccountStatus::DecodeFailed => Some(format!(
            "{} account {} could not be decoded from focused route cache",
            requirement.role, requirement.pubkey
        )),
    }
}

fn data_hash_hex(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

async fn handle_notification(
    val: &Value,
    active_ids: &HashMap<u64, String>,
    cache: &Arc<RwLock<HashMap<String, CachedAccount>>>,
    last_update_slot: &Arc<RwLock<u64>>,
    focused_routes: &Arc<RwLock<HashMap<i64, FocusedRouteRegistration>>>,
    focused_account_routes: &Arc<RwLock<HashMap<String, HashSet<i64>>>>,
    focused_requote_state: &Arc<RwLock<HashMap<i64, FocusedRouteRequoteState>>>,
) {
    let Some(account) = account_from_notification(val, active_ids) else {
        return;
    };
    let pubkey = account.pubkey.clone();
    let slot = account.slot;
    cache.write().await.insert(pubkey.clone(), account);
    let mut slot_w = last_update_slot.write().await;
    if slot > *slot_w {
        *slot_w = slot;
    }
    drop(slot_w);
    schedule_focused_requotes_for_account_parts(
        &pubkey,
        slot,
        focused_routes,
        focused_account_routes,
        focused_requote_state,
    )
    .await;
}

fn account_from_notification(
    val: &Value,
    active_ids: &HashMap<u64, String>,
) -> Option<CachedAccount> {
    let params = match val.get("params") {
        Some(p) => p,
        None => return None,
    };
    let result = match params.get("result") {
        Some(r) => r,
        None => return None,
    };
    let context = match result.get("context") {
        Some(c) => c,
        None => return None,
    };
    let slot = context.get("slot").and_then(|s| s.as_u64()).unwrap_or(0);
    let value = match result.get("value") {
        Some(v) => v,
        None => return None,
    };
    let owner = match value.get("owner").and_then(|o| o.as_str()) {
        Some(o) => o.to_string(),
        None => return None,
    };
    let lamports = value.get("lamports").and_then(|l| l.as_u64()).unwrap_or(0);
    let data = match value.get("data") {
        Some(d) => {
            if let Some(arr) = d.as_array() {
                if let (Some(data_str), Some(_encoding)) = (
                    arr.first().and_then(|v| v.as_str()),
                    arr.get(1).and_then(|v| v.as_str()),
                ) {
                    match base64::Engine::decode(
                        &base64::engine::general_purpose::STANDARD,
                        data_str,
                    ) {
                        Ok(decoded) => decoded,
                        Err(_) => return None,
                    }
                } else {
                    return None;
                }
            } else {
                return None;
            }
        }
        None => return None,
    };

    let sub_id = params.get("subscription").and_then(|s| s.as_u64());
    let pubkey = sub_id.and_then(|sid| active_ids.get(&sid).cloned());

    pubkey.map(|pk| CachedAccount {
        pubkey: pk,
        owner,
        lamports,
        data,
        slot,
        updated_at: Instant::now(),
    })
}

async fn schedule_focused_requotes_for_account_parts(
    pubkey: &str,
    slot: u64,
    focused_routes: &Arc<RwLock<HashMap<i64, FocusedRouteRegistration>>>,
    focused_account_routes: &Arc<RwLock<HashMap<String, HashSet<i64>>>>,
    focused_requote_state: &Arc<RwLock<HashMap<i64, FocusedRouteRequoteState>>>,
) {
    let route_ids = focused_account_routes
        .read()
        .await
        .get(pubkey)
        .cloned()
        .unwrap_or_default();
    if route_ids.is_empty() {
        return;
    }
    let routes = focused_routes.read().await;
    let now = Instant::now();
    let mut states = focused_requote_state.write().await;
    for route_edge_id in route_ids {
        let debounce_ms = routes
            .get(&route_edge_id)
            .map(|route| route.debounce_ms)
            .unwrap_or_default();
        states.insert(
            route_edge_id,
            FocusedRouteRequoteState {
                last_trigger_pubkey: pubkey.to_string(),
                last_update_slot: slot,
                last_update_at: now,
                due_at: Some(now + Duration::from_millis(debounce_ms)),
                quote_started_at: None,
                submit_started_at: None,
                update_to_quote_latency_ms: None,
                quote_to_submit_latency_ms: None,
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio::time::timeout;
    use tokio_tungstenite::accept_async;

    #[test]
    fn manager_new_initializes_empty() {
        let mgr = PoolSubscriptionManager::new("ws://localhost:8900".to_string());
        assert!(!tokio_test_helpers::block_on(mgr.is_connected()));
    }

    mod tokio_test_helpers {
        use std::future::Future;
        pub fn block_on<F: Future>(f: F) -> F::Output {
            tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap()
                .block_on(f)
        }
    }

    #[test]
    fn subscribe_adds_pubkeys() {
        let mgr = PoolSubscriptionManager::new("ws://localhost:8900".to_string());
        tokio_test_helpers::block_on(async {
            mgr.subscribe(&["key1".to_string(), "key2".to_string()])
                .await;
            let subs = mgr.subscribed.read().await.clone();
            assert!(subs.contains(&"key1".to_string()));
            assert!(subs.contains(&"key2".to_string()));
        });
    }

    #[test]
    fn unsubscribe_removes_pubkeys() {
        let mgr = PoolSubscriptionManager::new("ws://localhost:8900".to_string());
        tokio_test_helpers::block_on(async {
            mgr.subscribe(&["key1".to_string(), "key2".to_string(), "key3".to_string()])
                .await;
            mgr.unsubscribe(&["key2".to_string()]).await;
            let subs = mgr.subscribed.read().await.clone();
            assert!(subs.contains(&"key1".to_string()));
            assert!(!subs.contains(&"key2".to_string()));
            assert!(subs.contains(&"key3".to_string()));
        });
    }

    #[test]
    fn warm_raw_accounts_populates_cache_for_strict_reads() {
        let mgr = PoolSubscriptionManager::new("ws://localhost:8900".to_string());
        tokio_test_helpers::block_on(async {
            mgr.warm_raw_accounts(vec![RawAccount {
                pubkey: "pool-account".to_string(),
                owner: "owner-program".to_string(),
                lamports: 42,
                slot: 99,
                data: vec![1, 2, 3],
            }])
            .await;

            let cached = mgr
                .get_cached("pool-account")
                .await
                .expect("cached account");
            assert_eq!(cached.owner, "owner-program");
            assert_eq!(cached.lamports, 42);
            assert_eq!(cached.slot, 99);
            assert_eq!(cached.data, vec![1, 2, 3]);
            assert_eq!(mgr.cache_size().await, 1);
            assert_eq!(mgr.last_update_slot().await, 99);
        });
    }

    #[test]
    fn focused_route_readiness_fails_closed_and_tracks_account_metadata() {
        let mgr = PoolSubscriptionManager::new("ws://localhost:8900".to_string());
        tokio_test_helpers::block_on(async {
            mgr.register_focused_route_accounts(
                42,
                vec![
                    crate::types::RouteAccountRequirement {
                        role: "buy.pool".to_string(),
                        pubkey: "pool-a".to_string(),
                        expected_owner: Some("owner-program".to_string()),
                        required: true,
                    },
                    crate::types::RouteAccountRequirement {
                        role: "sell.pool".to_string(),
                        pubkey: "pool-b".to_string(),
                        expected_owner: Some("owner-program".to_string()),
                        required: true,
                    },
                    crate::types::RouteAccountRequirement {
                        role: "buy.vault".to_string(),
                        pubkey: "vault-a".to_string(),
                        expected_owner: Some("owner-program".to_string()),
                        required: true,
                    },
                ],
                1_000,
                0,
            )
            .await;

            mgr.warm_raw_accounts(vec![
                RawAccount {
                    pubkey: "pool-a".to_string(),
                    owner: "owner-program".to_string(),
                    lamports: 42,
                    slot: 100,
                    data: vec![1, 2, 3],
                },
                RawAccount {
                    pubkey: "pool-b".to_string(),
                    owner: "wrong-owner".to_string(),
                    lamports: 43,
                    slot: 99,
                    data: vec![4, 5, 6],
                },
                RawAccount {
                    pubkey: "vault-a".to_string(),
                    owner: "owner-program".to_string(),
                    lamports: 44,
                    slot: 98,
                    data: Vec::new(),
                },
            ])
            .await;

            let snapshot = mgr
                .focused_route_readiness(42)
                .await
                .expect("focused route snapshot");
            assert!(!snapshot.ready);
            assert!(mgr.ensure_focused_route_ready_for_quote(42).await.is_err());

            let pool_a = snapshot
                .accounts
                .iter()
                .find(|account| account.pubkey == "pool-a")
                .expect("pool-a readiness");
            assert_eq!(
                pool_a.status,
                crate::types::FocusedRouteAccountStatus::Ready
            );
            assert_eq!(pool_a.slot_age, Some(0));
            assert!(pool_a.wall_clock_age_ms.is_some());
            assert_eq!(pool_a.data_hash.as_deref().map(str::len), Some(64));

            let pool_b = snapshot
                .accounts
                .iter()
                .find(|account| account.pubkey == "pool-b")
                .expect("pool-b readiness");
            assert_eq!(
                pool_b.status,
                crate::types::FocusedRouteAccountStatus::OwnerMismatch
            );
            assert_eq!(pool_b.actual_owner.as_deref(), Some("wrong-owner"));

            let vault_a = snapshot
                .accounts
                .iter()
                .find(|account| account.pubkey == "vault-a")
                .expect("vault-a readiness");
            assert_eq!(
                vault_a.status,
                crate::types::FocusedRouteAccountStatus::DecodeFailed
            );
        });
    }

    #[tokio::test]
    async fn websocket_updates_schedule_debounced_requote_and_record_latencies() {
        let mgr = PoolSubscriptionManager::new("ws://localhost:8900".to_string());
        mgr.register_focused_route_accounts(
            77,
            vec![crate::types::RouteAccountRequirement {
                role: "buy.pool".to_string(),
                pubkey: "pool-a".to_string(),
                expected_owner: Some("owner-program".to_string()),
                required: true,
            }],
            1_000,
            0,
        )
        .await;

        let active_ids = HashMap::from([(9, "pool-a".to_string())]);
        let notification = json!({
            "method": "accountNotification",
            "params": {
                "subscription": 9,
                "result": {
                    "context": { "slot": 123 },
                    "value": {
                        "owner": "owner-program",
                        "lamports": 55,
                        "data": [base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [9, 8, 7]), "base64"]
                    }
                }
            }
        });

        mgr.ingest_account_notification(&notification, &active_ids)
            .await;
        let due = mgr.pop_due_focused_requote_events().await;
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].route_edge_id, 77);
        assert_eq!(due[0].trigger_pubkey, "pool-a");
        assert_eq!(due[0].update_slot, 123);
        assert!(due[0].update_to_quote_latency_ms.is_none());

        mgr.note_focused_route_quote_started(77).await;
        let after_quote = mgr
            .focused_route_requote_latency(77)
            .await
            .expect("route latency after quote");
        assert_eq!(after_quote.last_update_slot, Some(123));
        assert!(after_quote.last_update_to_quote_latency_ms.is_some());

        mgr.note_focused_route_submit_started(77).await;
        let after_submit = mgr
            .focused_route_requote_latency(77)
            .await
            .expect("route latency after submit");
        assert!(after_submit.last_quote_to_submit_latency_ms.is_some());
    }

    #[tokio::test]
    async fn subscribe_after_connect_sends_account_subscribe_without_reconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (ready_tx, ready_rx) = oneshot::channel();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();

            let initial = timeout(Duration::from_millis(150), ws.next()).await;
            assert!(
                initial.is_err(),
                "expected no initial subscription before dynamic subscribe"
            );
            let _ = ready_tx.send(());

            while let Some(message) = ws.next().await {
                let message = message.unwrap();
                if !message.is_text() {
                    continue;
                }
                let payload: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
                if payload.get("method").and_then(|method| method.as_str())
                    != Some("accountSubscribe")
                {
                    continue;
                }
                if payload
                    .get("params")
                    .and_then(|params| params.as_array())
                    .and_then(|params| params.first())
                    .and_then(|pubkey| pubkey.as_str())
                    == Some("dynamic-key")
                {
                    return true;
                }
            }
            false
        });

        let mgr = PoolSubscriptionManager::new(format!("ws://{addr}"));
        mgr.start();
        ready_rx.await.unwrap();

        mgr.subscribe(&["dynamic-key".to_string()]).await;

        let received = timeout(Duration::from_millis(500), server)
            .await
            .expect("dynamic subscription request was not sent")
            .unwrap();
        mgr.shutdown().await;
        assert!(received);
    }

    #[tokio::test]
    async fn too_many_subscriptions_response_marks_pubkey_rejected_without_immediate_retry() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (received_tx, received_rx) = oneshot::channel();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            let mut first_request_id = None;
            let mut request_count = 0usize;

            while let Ok(Some(message)) = timeout(Duration::from_millis(500), ws.next()).await {
                let message = message.unwrap();
                if !message.is_text() {
                    continue;
                }
                let payload: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
                if payload.get("method").and_then(|method| method.as_str())
                    != Some("accountSubscribe")
                {
                    continue;
                }
                request_count += 1;
                if first_request_id.is_none() {
                    let id = payload.get("id").and_then(|id| id.as_u64()).unwrap();
                    first_request_id = Some(id);
                    ws.send(Message::Text(
                        json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "error": {
                                "code": -32005,
                                "message": "Too many subscriptions"
                            }
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .unwrap();
                }
            }

            let _ = received_tx.send(request_count);
        });

        let mgr = PoolSubscriptionManager::new(format!("ws://{addr}"));
        mgr.start();
        mgr.subscribe(&["too-many-key".to_string()]).await;

        let request_count = received_rx.await.unwrap();
        server.await.unwrap();
        mgr.shutdown().await;

        let stats = mgr.subscription_stats().await;
        assert_eq!(request_count, 1, "rejected key was retried immediately");
        assert_eq!(stats.desired, 1);
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.confirmed, 0);
        assert_eq!(stats.rejected, 1);
    }
}
