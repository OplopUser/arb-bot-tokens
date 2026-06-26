use crate::{
    config::BotConfig,
    env_runtime::env_value,
    pool_subscription::{CachedAccount, PoolPlanKeys, PoolSubscriptionManager},
    rpc::RpcClient,
    types::{Opportunity, SwapSide, Venue, WSOL_MINT},
};
use anyhow::{anyhow, Context, Result};
use solana_address::Address;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    str::FromStr,
    sync::Arc,
    time::Instant,
};
use swaps::{dex::DexKind, InstructionBuf, SwapRequest};
use tokio::sync::RwLock;
use tracing::debug;

const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";
const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const ASSOCIATED_TOKEN_PROGRAM: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
const METEORA_POOLS_QUOTE_FEE_BPS: u64 = 100;
const METEORA_DLMM_BIN_ARRAY_HEADER_LEN: usize = 56;
const METEORA_DLMM_BIN_LEN: usize = 144;

#[derive(Debug, Clone)]
pub struct NativeAtomicPlan {
    pub instructions: Vec<Instruction>,
    pub buy_min_out_atoms: u64,
    pub sell_min_out_atoms: u64,
    pub dexes: Vec<String>,
    pub buy_quote_out_atoms: u64,
    pub sell_quote_out_atoms: u64,
    pub sell_amount_atoms: u64,
    pub execution_cost_quote_atoms: u64,
    pub quote_account: String,
    pub quote_account_before_atoms: u64,
    pub blocked_reason: Option<String>,
    pub simulation_priced: bool,
    pub fresh_requote_complete: bool,
    pub requote_net_profit_bps: f64,
    pub requote_net_profit_usd: f64,
    pub exploratory_simulation: bool,
    pub jito_tip_included: bool,
    pub latency_trace: Vec<NativeLatencyMark>,
}

#[derive(Debug, Clone)]
pub struct NativeFreshQuote {
    pub buy_quote_out_atoms: u64,
    pub sell_quote_out_atoms: u64,
    pub sell_amount_atoms: u64,
    pub execution_cost_quote_atoms: u64,
    pub fresh_requote_complete: bool,
    pub requote_net_profit_bps: f64,
    pub requote_net_profit_usd: f64,
    pub blocked_reason: Option<String>,
}

impl From<NativeAtomicPlan> for NativeFreshQuote {
    fn from(plan: NativeAtomicPlan) -> Self {
        Self {
            buy_quote_out_atoms: plan.buy_quote_out_atoms,
            sell_quote_out_atoms: plan.sell_quote_out_atoms,
            sell_amount_atoms: plan.sell_amount_atoms,
            execution_cost_quote_atoms: plan.execution_cost_quote_atoms,
            fresh_requote_complete: plan.fresh_requote_complete,
            requote_net_profit_bps: plan.requote_net_profit_bps,
            requote_net_profit_usd: plan.requote_net_profit_usd,
            blocked_reason: plan.blocked_reason,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NativeLatencyMark {
    pub stage: String,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativePlanBuildMode {
    Strict,
    ExploratorySimulation,
    PriceOnly,
}

#[derive(Clone)]
pub struct NativeSwapBuilder {
    config: BotConfig,
    rpc: RpcClient,
    pool_subscription: Option<Arc<PoolSubscriptionManager>>,
    strict_fresh_cache_ttl_ms: Option<u64>,
    token_program_cache: Arc<RwLock<HashMap<String, Pubkey>>>,
    static_account_cache: Arc<RwLock<HashMap<String, CachedAccount>>>,
    instruction_template_cache: Arc<RwLock<HashMap<String, NativeInstructionTemplate>>>,
}

impl NativeSwapBuilder {
    pub fn new(config: BotConfig) -> Self {
        let rpc = RpcClient::new(&config.rpc);
        Self {
            config,
            rpc,
            pool_subscription: None,
            strict_fresh_cache_ttl_ms: None,
            token_program_cache: Arc::new(RwLock::new(HashMap::new())),
            static_account_cache: Arc::new(RwLock::new(HashMap::new())),
            instruction_template_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn with_pool_subscription(mut self, sub: Arc<PoolSubscriptionManager>) -> Self {
        self.pool_subscription = Some(sub);
        self
    }

    pub fn with_strict_fresh_cache(mut self, ttl_ms: u64) -> Self {
        self.strict_fresh_cache_ttl_ms = Some(ttl_ms);
        self
    }

    pub async fn warm_static_hot_accounts(
        &self,
        wallet: &Pubkey,
        opportunity: &Opportunity,
    ) -> Result<()> {
        if self.pool_subscription.is_none() || self.strict_fresh_cache_ttl_ms.is_none() {
            return Ok(());
        }
        let quote_mint = pubkey_from_str(&opportunity.pair.quote_mint)
            .context("invalid opportunity quote mint")?;
        let base_mint = pubkey_from_str(&opportunity.pair.base_mint)
            .context("invalid opportunity base mint")?;
        let (quote_token_program, base_token_program) = tokio::join!(
            self.token_program_for_mint(&quote_mint),
            self.token_program_for_mint(&base_mint),
        );
        let quote_token_program = quote_token_program?;
        let base_token_program = base_token_program?;
        if self.config.execution.native_prefunded_hot_accounts {
            let quote_ata = associated_token_address(wallet, &quote_mint, &quote_token_program)?;
            let base_ata = associated_token_address(wallet, &base_mint, &base_token_program)?;
            let _ = self
                .hot_cached_accounts(&[quote_ata.to_string(), base_ata.to_string()])
                .await?;
        }
        Ok(())
    }

    pub fn is_native_candidate(opportunity: &Opportunity) -> bool {
        native_pool_candidate(opportunity.buy_venue, &opportunity.buy_pool)
            && native_pool_candidate(opportunity.sell_venue, &opportunity.sell_pool)
    }

    pub async fn build_atomic_plan(
        &self,
        wallet: &Pubkey,
        opportunity: &Opportunity,
    ) -> Result<Option<NativeAtomicPlan>> {
        if !Self::is_native_candidate(opportunity) {
            return Ok(None);
        }

        let wallet_address = address_from_pubkey(wallet);
        let quote_mint = pubkey_from_str(&opportunity.pair.quote_mint)
            .context("invalid opportunity quote mint")?;
        let base_mint = pubkey_from_str(&opportunity.pair.base_mint)
            .context("invalid opportunity base mint")?;
        let quote_token_program = self.token_program_for_mint(&quote_mint).await?;
        let base_token_program = self.token_program_for_mint(&base_mint).await?;

        let buy_amount_in: u64 = opportunity
            .input_quote_atoms
            .try_into()
            .context("native buy input amount does not fit u64")?;
        let quoted_base_out: u64 = opportunity
            .base_atoms
            .try_into()
            .context("native buy output amount does not fit u64")?;
        if buy_amount_in == 0 || quoted_base_out == 0 {
            return Ok(None);
        }

        let slippage_bps = u64::from(live_native_slippage_bps(self.config.jupiter.slippage_bps))
            + self.config.trading.fee_safety_buffer_bps.ceil().max(0.0) as u64;
        let buy_min_out = slippage_floor(quoted_base_out, slippage_bps).max(1);
        let sell_amount = buy_min_out;
        let estimated_sell_out_for_sell_amount =
            scale_amount(opportunity.output_quote_atoms, sell_amount, quoted_base_out)?;
        let slippage_sell_min = slippage_floor(estimated_sell_out_for_sell_amount, slippage_bps);
        let required_profit_atoms = required_profit_atoms(
            opportunity.input_quote_atoms,
            self.config
                .execution
                .min_requote_profit_bps
                .max(self.config.trading.min_net_profit_bps),
        );
        let no_loss_min = opportunity
            .input_quote_atoms
            .saturating_add(required_profit_atoms)
            .min(u128::from(u64::MAX)) as u64;
        let sell_min_out = slippage_sell_min.max(no_loss_min).max(1);

        let buy = self
            .build_swap_instruction(
                opportunity.buy_venue,
                &opportunity.buy_pool,
                wallet_address,
                address_from_pubkey(&quote_mint),
                address_from_pubkey(&base_mint),
                address_from_pubkey(&quote_token_program),
                address_from_pubkey(&base_token_program),
                buy_amount_in,
                buy_min_out,
            )
            .await
            .with_context(|| {
                format!(
                    "native buy instruction build failed for {} {}",
                    opportunity.buy_venue, opportunity.buy_pool
                )
            })?;
        let sell = self
            .build_swap_instruction(
                opportunity.sell_venue,
                &opportunity.sell_pool,
                wallet_address,
                address_from_pubkey(&base_mint),
                address_from_pubkey(&quote_mint),
                address_from_pubkey(&base_token_program),
                address_from_pubkey(&quote_token_program),
                sell_amount,
                sell_min_out,
            )
            .await
            .with_context(|| {
                format!(
                    "native sell instruction build failed for {} {}",
                    opportunity.sell_venue, opportunity.sell_pool
                )
            })?;

        let mut instructions = Vec::new();
        instructions.extend(native_compute_budget_instructions(&self.config));
        if self.config.fees.jito_tip_lamports > 0 {
            instructions.push(jito_tip_ix(wallet, self.config.fees.jito_tip_lamports)?);
        }
        if let Some(blocker) = self
            .prefunded_hot_account_blocker(
                wallet,
                &quote_mint,
                &base_mint,
                &quote_token_program,
                &base_token_program,
                buy_amount_in,
            )
            .await?
        {
            return Ok(Some(NativeAtomicPlan {
                instructions: Vec::new(),
                buy_min_out_atoms: buy_min_out,
                sell_min_out_atoms: sell_min_out,
                dexes: vec![buy.dex_name, sell.dex_name],
                buy_quote_out_atoms: quoted_base_out,
                sell_quote_out_atoms: estimated_sell_out_for_sell_amount,
                sell_amount_atoms: sell_amount,
                execution_cost_quote_atoms: 0,
                quote_account: String::new(),
                quote_account_before_atoms: 0,
                blocked_reason: Some(blocker),
                simulation_priced: false,
                fresh_requote_complete: false,
                requote_net_profit_bps: opportunity.net_profit_bps,
                requote_net_profit_usd: opportunity.net_profit_usd,
                exploratory_simulation: false,
                jito_tip_included: false,
                latency_trace: Vec::new(),
            }));
        }
        instructions.extend(native_setup_prefix_instructions(
            &self.config,
            wallet,
            &quote_mint,
            &base_mint,
            &quote_token_program,
            &base_token_program,
            buy_amount_in,
        )?);
        instructions.push(buy.instruction);
        instructions.push(sell.instruction);
        instructions.extend(native_setup_suffix_instructions(
            &self.config,
            wallet,
            &quote_mint,
            &quote_token_program,
        )?);
        Ok(Some(NativeAtomicPlan {
            instructions,
            buy_min_out_atoms: buy_min_out,
            sell_min_out_atoms: sell_min_out,
            dexes: vec![buy.dex_name, sell.dex_name],
            buy_quote_out_atoms: quoted_base_out,
            sell_quote_out_atoms: estimated_sell_out_for_sell_amount,
            sell_amount_atoms: sell_amount,
            execution_cost_quote_atoms: 0,
            quote_account: String::new(),
            quote_account_before_atoms: 0,
            blocked_reason: None,
            simulation_priced: false,
            fresh_requote_complete: false,
            requote_net_profit_bps: opportunity.net_profit_bps,
            requote_net_profit_usd: opportunity.net_profit_usd,
            exploratory_simulation: false,
            jito_tip_included: false,
            latency_trace: Vec::new(),
        }))
    }

    async fn build_swap_instruction(
        &self,
        venue: Venue,
        pool: &str,
        user: Address,
        token_in: Address,
        token_out: Address,
        token_in_program: Address,
        token_out_program: Address,
        amount_in: u64,
        min_amount_out: u64,
    ) -> Result<NativeSwapInstruction> {
        let pool_key = pubkey_from_str(pool).context("invalid native pool address")?;
        let pool_address = address_from_pubkey(&pool_key);
        let pool_account = self
            .rpc
            .get_multiple_accounts(&[pool.to_string()])
            .await?
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| anyhow!("native pool account missing: {pool}"))?;
        let owner = pubkey_from_str(&pool_account.owner)
            .with_context(|| format!("invalid owner for native pool {pool}"))?;
        let dex = dex_for_pool_owner(venue, &owner)
            .ok_or_else(|| anyhow!("unsupported native {venue} pool owner {owner}"))?;

        let req = SwapRequest::new(
            dex.kind,
            user,
            token_in,
            token_out,
            amount_in,
            min_amount_out,
        )
        .with_pool(pool_address)
        .with_token_programs(token_in_program, token_out_program);
        let initial_plan = swaps::fetch_plan(&req);
        let initial_keys = initial_plan
            .addresses()
            .map(address_to_string)
            .collect::<Vec<_>>();
        let initial_data = self.fetch_account_data(&initial_keys).await?;
        let followup_data = match swaps::followup_keys(&req, &initial_data)? {
            Some(plan) => {
                let keys = plan.addresses().map(address_to_string).collect::<Vec<_>>();
                self.fetch_account_data(&keys).await?
            }
            None => Vec::new(),
        };
        let snap = swaps::decode_snapshot(&req, &initial_data, &followup_data)?;
        let mut buf = InstructionBuf::new();
        swaps::build_swap(&req, &snap, &mut buf)?;

        Ok(NativeSwapInstruction {
            instruction: instruction_from_swaps(buf.as_view()),
            dex_name: dex.name.to_string(),
        })
    }

    async fn fetch_account_data(&self, keys: &[String]) -> Result<Vec<Vec<u8>>> {
        if let Some(ref sub) = self.pool_subscription {
            if let Some(ttl_ms) = self.strict_fresh_cache_ttl_ms {
                return refresh_optional_strict_cached_accounts(&self.rpc, sub, keys, ttl_ms)
                    .await?
                    .into_iter()
                    .enumerate()
                    .map(|(index, account)| {
                        account
                            .map(|account| account.data)
                            .ok_or_else(|| anyhow!("native swap account missing: {}", keys[index]))
                    })
                    .collect();
            }
            let cached = sub.get_cached_data(keys).await;
            let mut result = Vec::with_capacity(keys.len());
            let mut missing_indices = Vec::new();
            let mut missing_keys = Vec::new();
            for (i, key) in keys.iter().enumerate() {
                match &cached[i] {
                    Some(data) => result.push(data.clone()),
                    None => {
                        result.push(Vec::new());
                        missing_indices.push(i);
                        missing_keys.push(key.clone());
                    }
                }
            }
            if !missing_keys.is_empty() {
                let accounts = self.rpc.get_multiple_accounts(&missing_keys).await?;
                for (j, account) in accounts.into_iter().enumerate() {
                    let i = missing_indices[j];
                    let data = account
                        .ok_or_else(|| anyhow!("native swap account missing: {}", missing_keys[j]))?
                        .data;
                    result[i] = data;
                }
            }
            return Ok(result);
        }
        let accounts = self.rpc.get_multiple_accounts(keys).await?;
        accounts
            .into_iter()
            .enumerate()
            .map(|(index, account)| {
                account
                    .map(|account| account.data)
                    .ok_or_else(|| anyhow!("native swap account missing: {}", keys[index]))
            })
            .collect()
    }

    async fn fetch_dlmm_bin_array_data(
        &self,
        targets: &[NativeDlmmBinArrayFetchTarget],
    ) -> Result<Vec<Vec<u8>>> {
        if let Some(ref sub) = self.pool_subscription {
            if let Some(ttl_ms) = self.strict_fresh_cache_ttl_ms {
                let keys = targets
                    .iter()
                    .map(|target| target.key.clone())
                    .collect::<Vec<_>>();
                let accounts =
                    refresh_optional_strict_cached_accounts(&self.rpc, sub, &keys, ttl_ms).await?;
                let mut data = Vec::with_capacity(targets.len());
                for (target, account) in targets.iter().zip(accounts.into_iter()) {
                    match account {
                        Some(account) => data.push(account.data),
                        None if !target.required => {
                            debug!(
                                pubkey = target.key.as_str(),
                                "skipping optional DLMM bin array for fresh quote"
                            );
                        }
                        None => {
                            return Err(anyhow!("native swap account missing: {}", target.key));
                        }
                    }
                }
                return Ok(data);
            }
        }

        let keys = targets
            .iter()
            .map(|target| target.key.clone())
            .collect::<Vec<_>>();
        let accounts = self.rpc.get_multiple_accounts(&keys).await?;
        let mut data = Vec::with_capacity(targets.len());
        for (target, account) in targets.iter().zip(accounts.into_iter()) {
            match account {
                Some(account) => data.push(account.data),
                None if !target.required => {
                    debug!(
                        pubkey = target.key.as_str(),
                        "skipping missing optional DLMM bin array for fresh quote"
                    );
                }
                None => return Err(anyhow!("native swap account missing: {}", target.key)),
            }
        }
        Ok(data)
    }

    async fn token_program_for_mint(&self, mint: &Pubkey) -> Result<Pubkey> {
        let mint_text = mint.to_string();
        if let Some(token_program) = self
            .token_program_cache
            .read()
            .await
            .get(&mint_text)
            .cloned()
        {
            return Ok(token_program);
        }
        if mint_text == WSOL_MINT {
            let token_program = pubkey_from_str(TOKEN_PROGRAM)?;
            self.token_program_cache
                .write()
                .await
                .insert(mint_text, token_program);
            return pubkey_from_str(TOKEN_PROGRAM);
        }
        let account = self
            .hot_cached_account(&mint_text)
            .await?
            .ok_or_else(|| anyhow!("mint account missing: {mint_text}"))?;
        let owner = pubkey_from_str(&account.owner)?;
        let token_program = pubkey_from_str(TOKEN_PROGRAM)?;
        let token_2022_program = pubkey_from_str(TOKEN_2022_PROGRAM)?;
        if owner == token_program || owner == token_2022_program {
            self.token_program_cache
                .write()
                .await
                .insert(mint_text, owner);
            Ok(owner)
        } else {
            Err(anyhow!(
                "mint {mint_text} owner {owner} is not SPL Token or Token-2022"
            ))
        }
    }

    async fn prefunded_hot_account_blocker(
        &self,
        wallet: &Pubkey,
        quote_mint: &Pubkey,
        base_mint: &Pubkey,
        quote_token_program: &Pubkey,
        base_token_program: &Pubkey,
        required_quote_atoms: u64,
    ) -> Result<Option<String>> {
        if !self.config.execution.native_prefunded_hot_accounts {
            return Ok(None);
        }

        let quote_ata = associated_token_address(wallet, quote_mint, quote_token_program)?;
        let base_ata = associated_token_address(wallet, base_mint, base_token_program)?;
        let quote_ata_text = quote_ata.to_string();
        let base_ata_text = base_ata.to_string();
        let accounts = self
            .hot_cached_accounts(&[quote_ata_text.clone(), base_ata_text.clone()])
            .await?;
        let Some(quote_account) = accounts.first().and_then(|account| account.as_ref()) else {
            return Ok(Some(format!(
                "native prefunded hot account missing quote ATA {quote_ata_text}"
            )));
        };
        if accounts
            .get(1)
            .and_then(|account| account.as_ref())
            .is_none()
        {
            return Ok(Some(format!(
                "native prefunded hot account missing base ATA {base_ata_text}"
            )));
        }
        let quote_amount = token_account_amount(&quote_account.data).unwrap_or(0);
        if quote_amount < required_quote_atoms {
            return Ok(Some(format!(
                "native prefunded hot quote ATA balance {quote_amount} is below required {required_quote_atoms}"
            )));
        }
        Ok(None)
    }

    async fn hot_cached_account(&self, pubkey: &str) -> Result<Option<CachedAccount>> {
        let accounts = self.hot_cached_accounts(&[pubkey.to_string()]).await?;
        Ok(accounts.into_iter().next().flatten())
    }

    async fn hot_cached_accounts(&self, pubkeys: &[String]) -> Result<Vec<Option<CachedAccount>>> {
        let mut result = Vec::with_capacity(pubkeys.len());
        let mut missing_indices = Vec::new();
        let mut missing_keys = Vec::new();
        {
            let static_cache = self.static_account_cache.read().await;
            for (index, pubkey) in pubkeys.iter().enumerate() {
                match static_cache.get(pubkey) {
                    Some(account) => result.push(Some(account.clone())),
                    None => {
                        result.push(None);
                        missing_indices.push(index);
                        missing_keys.push(pubkey.clone());
                    }
                }
            }
        }
        if missing_keys.is_empty() {
            return Ok(result);
        }
        if let Some(ref sub) = self.pool_subscription {
            if let Some(ttl_ms) = self.strict_fresh_cache_ttl_ms {
                let fetched =
                    refresh_optional_strict_cached_accounts(&self.rpc, sub, &missing_keys, ttl_ms)
                        .await?;
                self.remember_static_accounts(fetched.iter().flatten())
                    .await;
                for (fetched_index, account) in fetched.into_iter().enumerate() {
                    result[missing_indices[fetched_index]] = account;
                }
                return Ok(result);
            }
            if !missing_keys.is_empty() {
                let accounts = self.rpc.get_multiple_accounts(&missing_keys).await?;
                let mut warmed = Vec::new();
                for (missing_index, account) in accounts.into_iter().enumerate() {
                    if let Some(account) = account {
                        warmed.push(account.clone());
                        result[missing_indices[missing_index]] = Some(CachedAccount {
                            pubkey: account.pubkey,
                            owner: account.owner,
                            lamports: account.lamports,
                            data: account.data,
                            slot: account.slot,
                            updated_at: std::time::Instant::now(),
                        });
                    }
                }
                sub.warm_raw_accounts(warmed).await;
            }
            self.remember_static_accounts(result.iter().flatten()).await;
            return Ok(result);
        }
        let accounts = self.rpc.get_multiple_accounts(&missing_keys).await?;
        for (fetched_index, account) in accounts.into_iter().enumerate() {
            if let Some(account) = account {
                result[missing_indices[fetched_index]] = Some(CachedAccount {
                    pubkey: account.pubkey,
                    owner: account.owner,
                    lamports: account.lamports,
                    data: account.data,
                    slot: account.slot,
                    updated_at: std::time::Instant::now(),
                });
            }
        }
        self.remember_static_accounts(result.iter().flatten()).await;
        Ok(result)
    }

    async fn remember_static_accounts<'a>(
        &self,
        accounts: impl IntoIterator<Item = &'a CachedAccount>,
    ) {
        let mut static_cache = self.static_account_cache.write().await;
        for account in accounts {
            static_cache.insert(account.pubkey.clone(), account.clone());
        }
    }

    pub async fn build_exploratory_simulation_plan(
        &self,
        wallet: &Pubkey,
        opportunity: &Opportunity,
    ) -> Result<Option<NativeAtomicPlan>> {
        self.build_atomic_plan_with_mode(
            wallet,
            opportunity,
            NativePlanBuildMode::ExploratorySimulation,
            exploratory_sell_amount_cap(opportunity),
        )
        .await
    }

    pub async fn build_exploratory_simulation_plan_with_sell_amount(
        &self,
        wallet: &Pubkey,
        opportunity: &Opportunity,
        sell_amount: u64,
    ) -> Result<Option<NativeAtomicPlan>> {
        self.build_atomic_plan_with_mode(
            wallet,
            opportunity,
            NativePlanBuildMode::ExploratorySimulation,
            sell_amount,
        )
        .await
    }

    pub async fn build_atomic_plan_with_sell_amount(
        &self,
        wallet: &Pubkey,
        opportunity: &Opportunity,
        sell_amount: u64,
    ) -> Result<Option<NativeAtomicPlan>> {
        self.build_atomic_plan_with_mode(
            wallet,
            opportunity,
            NativePlanBuildMode::Strict,
            sell_amount,
        )
        .await
    }

    pub async fn quote_atomic_arbitrage_from_snapshot(
        &self,
        wallet: &Pubkey,
        opportunity: &Opportunity,
    ) -> Result<Option<NativeFreshQuote>> {
        Ok(self
            .build_atomic_plan_with_mode(
                wallet,
                opportunity,
                NativePlanBuildMode::PriceOnly,
                exploratory_sell_amount_cap(opportunity),
            )
            .await?
            .map(NativeFreshQuote::from))
    }

    pub async fn build_atomic_plan_with_mode(
        &self,
        wallet: &Pubkey,
        opportunity: &Opportunity,
        mode: NativePlanBuildMode,
        sell_amount: u64,
    ) -> Result<Option<NativeAtomicPlan>> {
        let latency_start = Instant::now();
        let mut latency_trace = Vec::new();
        if !Self::is_native_candidate(opportunity) {
            return Ok(None);
        }
        let wallet_address = address_from_pubkey(wallet);
        let quote_mint = pubkey_from_str(&opportunity.pair.quote_mint)
            .context("invalid opportunity quote mint")?;
        let base_mint = pubkey_from_str(&opportunity.pair.base_mint)
            .context("invalid opportunity base mint")?;
        // Parallel RPC: resolve token programs for both mints simultaneously
        let (quote_token_program, base_token_program) = tokio::join!(
            self.token_program_for_mint(&quote_mint),
            self.token_program_for_mint(&base_mint),
        );
        let quote_token_program = quote_token_program?;
        let base_token_program = base_token_program?;
        push_native_latency_mark(
            &mut latency_trace,
            "native_token_programs_resolved",
            latency_start,
        );

        let buy_amount_in: u64 = opportunity
            .input_quote_atoms
            .try_into()
            .context("native buy input amount does not fit u64")?;
        if buy_amount_in == 0 {
            return Ok(None);
        }
        let slippage_bps = u64::from(live_native_slippage_bps(self.config.jupiter.slippage_bps))
            + self.config.trading.fee_safety_buffer_bps.ceil().max(0.0) as u64;
        let initial_buy_min_out =
            native_buy_min_out_for_mode(mode, sell_amount, buy_amount_in, slippage_bps);

        let (mut buy, mut buy_snapshot) = self
            .build_swap_instruction_with_snapshot(
                opportunity.buy_venue,
                &opportunity.buy_pool,
                wallet_address,
                address_from_pubkey(&quote_mint),
                address_from_pubkey(&base_mint),
                address_from_pubkey(&quote_token_program),
                address_from_pubkey(&base_token_program),
                buy_amount_in,
                initial_buy_min_out,
            )
            .await
            .with_context(|| {
                format!(
                    "native buy instruction build failed for {} {}",
                    opportunity.buy_venue, opportunity.buy_pool
                )
            })?;
        push_native_latency_mark(
            &mut latency_trace,
            "native_buy_instruction_built",
            latency_start,
        );
        let mut fresh_buy_quote_result = self
            .fresh_quote_from_snapshot(
                &buy_snapshot,
                opportunity.buy_venue,
                &address_from_pubkey(&quote_mint),
                &address_from_pubkey(&base_mint),
                buy_amount_in,
                SwapSide::QuoteToBase,
            )
            .await;
        let mut fresh_buy_quote = fresh_buy_quote_result.as_ref().ok().copied();
        let buy_min_out = native_buy_min_out_after_fresh_quote(
            mode,
            initial_buy_min_out,
            sell_amount,
            fresh_buy_quote,
            slippage_bps,
        );
        if buy_min_out != initial_buy_min_out {
            let rebuilt = self
                .build_swap_instruction_with_snapshot(
                    opportunity.buy_venue,
                    &opportunity.buy_pool,
                    wallet_address,
                    address_from_pubkey(&quote_mint),
                    address_from_pubkey(&base_mint),
                    address_from_pubkey(&quote_token_program),
                    address_from_pubkey(&base_token_program),
                    buy_amount_in,
                    buy_min_out,
                )
                .await
                .with_context(|| {
                    format!(
                        "native buy instruction rebuild failed for {} {}",
                        opportunity.buy_venue, opportunity.buy_pool
                    )
                })?;
            buy = rebuilt.0;
            buy_snapshot = rebuilt.1;
            push_native_latency_mark(
                &mut latency_trace,
                "native_buy_instruction_rebuilt",
                latency_start,
            );
            fresh_buy_quote_result = self
                .fresh_quote_from_snapshot(
                    &buy_snapshot,
                    opportunity.buy_venue,
                    &address_from_pubkey(&quote_mint),
                    &address_from_pubkey(&base_mint),
                    buy_amount_in,
                    SwapSide::QuoteToBase,
                )
                .await;
            fresh_buy_quote = fresh_buy_quote_result.as_ref().ok().copied();
        }
        let buy_quote_out = fresh_buy_quote.filter(|&v| v > 0).unwrap_or_else(|| {
            opportunity
                .base_atoms
                .try_into()
                .unwrap_or(buy_min_out)
                .max(buy_min_out)
        });
        let sell_base_amount =
            native_sell_amount_for_mode(mode, sell_amount, buy_quote_out, buy_min_out);

        let estimated_sell_out = opportunity.output_quote_atoms.min(u64::MAX as u128) as u64;
        let sell_min_out = native_sell_min_out_for_mode(
            mode,
            false,
            estimated_sell_out,
            estimated_sell_out,
            slippage_bps,
        );

        let (sell, sell_snapshot) = self
            .build_swap_instruction_with_snapshot(
                opportunity.sell_venue,
                &opportunity.sell_pool,
                wallet_address,
                address_from_pubkey(&base_mint),
                address_from_pubkey(&quote_mint),
                address_from_pubkey(&base_token_program),
                address_from_pubkey(&quote_token_program),
                sell_base_amount,
                sell_min_out,
            )
            .await
            .with_context(|| {
                format!(
                    "native sell instruction build failed for {} {}",
                    opportunity.sell_venue, opportunity.sell_pool
                )
            })?;
        push_native_latency_mark(
            &mut latency_trace,
            "native_sell_instruction_built",
            latency_start,
        );

        // Parallel: fresh quotes from cached pool data (no RPC needed)
        let fresh_sell_quote_result = self
            .fresh_quote_from_snapshot(
                &sell_snapshot,
                opportunity.sell_venue,
                &address_from_pubkey(&base_mint),
                &address_from_pubkey(&quote_mint),
                sell_base_amount,
                SwapSide::BaseToQuote,
            )
            .await;
        let fresh_sell_quote = fresh_sell_quote_result.as_ref().ok().copied();
        let sell_quote_out = fresh_sell_quote
            .filter(|&v| v > 0)
            .unwrap_or_else(|| opportunity.output_quote_atoms.min(u64::MAX as u128) as u64);
        push_native_latency_mark(
            &mut latency_trace,
            "native_fresh_quotes_priced",
            latency_start,
        );

        let execution_cost_lamports = self
            .config
            .fees
            .max_priority_fee_lamports
            .saturating_add(self.config.fees.base_fee_lamports)
            .saturating_add(self.config.fees.ata_rent_lamports * 2);
        let execution_cost_atoms = native_execution_cost_quote_atoms(
            execution_cost_lamports,
            opportunity.pair.quote_decimals,
            &opportunity.pair.quote_mint,
            self.config.trading.sol_usd_estimate,
        )
        .ok_or_else(|| {
            anyhow!(
                "cannot price native execution cost in quote mint {}",
                opportunity.pair.quote_mint
            )
        })?;
        let fresh_requote_complete = fresh_buy_quote.is_some() && fresh_sell_quote.is_some();
        let requote_net_profit_atoms = if fresh_requote_complete {
            i128::from(sell_quote_out)
                - i128::from(buy_amount_in)
                - saturating_i128(execution_cost_atoms)
        } else {
            (opportunity.net_profit_bps / 10_000.0 * buy_amount_in as f64) as i128
        };
        let requote_net_profit_bps = requote_profit_bps(requote_net_profit_atoms, buy_amount_in);
        let requote_net_profit_usd = quote_profit_usd_signed(
            requote_net_profit_atoms,
            opportunity.pair.quote_decimals,
            &opportunity.pair.quote_mint,
            self.config.trading.sol_usd_estimate,
        )
        .unwrap_or(0.0);
        let simulation_priced = !fresh_requote_complete && self.config.execution.require_simulation;
        let final_sell_min_out = native_sell_min_out_for_mode(
            mode,
            simulation_priced,
            sell_quote_out,
            sell_quote_out,
            slippage_bps,
        );
        let blocked_reason = native_fresh_quote_blocked_reason(
            fresh_buy_quote.is_some(),
            fresh_sell_quote.is_some(),
            sell_quote_out,
            final_sell_min_out,
            requote_net_profit_bps,
            simulation_priced || !self.config.execution.require_simulation,
            fresh_buy_quote_result.as_ref().err().map(String::as_str),
            fresh_sell_quote_result.as_ref().err().map(String::as_str),
        );
        if blocked_reason.is_some() && mode != NativePlanBuildMode::ExploratorySimulation {
            return Ok(Some(NativeAtomicPlan {
                instructions: Vec::new(),
                buy_min_out_atoms: buy_min_out,
                sell_min_out_atoms: final_sell_min_out,
                dexes: vec![buy.dex_name, sell.dex_name],
                buy_quote_out_atoms: buy_quote_out,
                sell_quote_out_atoms: sell_quote_out,
                sell_amount_atoms: sell_base_amount,
                execution_cost_quote_atoms: execution_cost_atoms,
                quote_account: String::new(),
                quote_account_before_atoms: 0,
                blocked_reason,
                simulation_priced,
                fresh_requote_complete,
                requote_net_profit_bps,
                requote_net_profit_usd,
                exploratory_simulation: mode == NativePlanBuildMode::ExploratorySimulation,
                jito_tip_included: false,
                latency_trace,
            }));
        }

        let mut instructions = Vec::new();
        instructions.extend(native_compute_budget_instructions(&self.config));
        let tip_lamports = self.config.fees.jito_tip_lamports;
        let mut tip_included = false;
        if tip_lamports > 0 {
            instructions.push(jito_tip_ix(wallet, tip_lamports)?);
            tip_included = true;
        }
        if let Some(blocker) = self
            .prefunded_hot_account_blocker(
                wallet,
                &quote_mint,
                &base_mint,
                &quote_token_program,
                &base_token_program,
                buy_amount_in,
            )
            .await?
        {
            push_native_latency_mark(
                &mut latency_trace,
                "native_prefunded_accounts_checked",
                latency_start,
            );
            return Ok(Some(NativeAtomicPlan {
                instructions: Vec::new(),
                buy_min_out_atoms: buy_min_out,
                sell_min_out_atoms: final_sell_min_out,
                dexes: vec![buy.dex_name, sell.dex_name],
                buy_quote_out_atoms: buy_quote_out,
                sell_quote_out_atoms: sell_quote_out,
                sell_amount_atoms: sell_base_amount,
                execution_cost_quote_atoms: execution_cost_atoms,
                quote_account: String::new(),
                quote_account_before_atoms: 0,
                blocked_reason: Some(blocker),
                simulation_priced,
                fresh_requote_complete,
                requote_net_profit_bps,
                requote_net_profit_usd,
                exploratory_simulation: mode == NativePlanBuildMode::ExploratorySimulation,
                jito_tip_included: tip_included,
                latency_trace,
            }));
        }
        push_native_latency_mark(
            &mut latency_trace,
            "native_prefunded_accounts_checked",
            latency_start,
        );
        instructions.extend(native_setup_prefix_instructions(
            &self.config,
            wallet,
            &quote_mint,
            &base_mint,
            &quote_token_program,
            &base_token_program,
            buy_amount_in,
        )?);
        instructions.push(buy.instruction);
        instructions.push(sell.instruction);
        instructions.extend(native_setup_suffix_instructions(
            &self.config,
            wallet,
            &quote_mint,
            &quote_token_program,
        )?);
        push_native_latency_mark(
            &mut latency_trace,
            "native_instructions_assembled",
            latency_start,
        );

        Ok(Some(NativeAtomicPlan {
            instructions,
            buy_min_out_atoms: buy_min_out,
            sell_min_out_atoms: final_sell_min_out,
            dexes: vec![buy.dex_name, sell.dex_name],
            buy_quote_out_atoms: buy_quote_out,
            sell_quote_out_atoms: sell_quote_out,
            sell_amount_atoms: sell_base_amount,
            execution_cost_quote_atoms: execution_cost_atoms,
            quote_account: String::new(),
            quote_account_before_atoms: 0,
            blocked_reason,
            simulation_priced,
            fresh_requote_complete,
            requote_net_profit_bps,
            requote_net_profit_usd,
            exploratory_simulation: mode == NativePlanBuildMode::ExploratorySimulation,
            jito_tip_included: tip_included,
            latency_trace,
        }))
    }

    async fn build_swap_instruction_with_snapshot(
        &self,
        venue: Venue,
        pool: &str,
        user: Address,
        token_in: Address,
        token_out: Address,
        token_in_program: Address,
        token_out_program: Address,
        amount_in: u64,
        min_amount_out: u64,
    ) -> Result<(NativeSwapInstruction, PoolSnapshot)> {
        let pool_key = pubkey_from_str(pool).context("invalid native pool address")?;
        let pool_address = address_from_pubkey(&pool_key);
        let (pool_data, pool_owner) = if let Some(ref sub) = self.pool_subscription {
            if let Some(ttl_ms) = self.strict_fresh_cache_ttl_ms {
                let cached = strict_fresh_cached_account(sub, pool, ttl_ms).await?;
                (cached.data, cached.owner)
            } else if let Some(cached) = sub.get_cached(pool).await {
                (cached.data, cached.owner)
            } else {
                let account = self
                    .rpc
                    .get_multiple_accounts(&[pool.to_string()])
                    .await?
                    .into_iter()
                    .next()
                    .flatten()
                    .ok_or_else(|| anyhow!("native pool account missing: {pool}"))?;
                (account.data, account.owner)
            }
        } else {
            let account = self
                .rpc
                .get_multiple_accounts(&[pool.to_string()])
                .await?
                .into_iter()
                .next()
                .flatten()
                .ok_or_else(|| anyhow!("native pool account missing: {pool}"))?;
            (account.data, account.owner)
        };
        let owner = pubkey_from_str(&pool_owner)
            .with_context(|| format!("invalid owner for native pool {pool}"))?;
        let dex = dex_for_pool_owner(venue, &owner)
            .ok_or_else(|| anyhow!("unsupported native {venue} pool owner {owner}"))?;

        let req = SwapRequest::new(
            dex.kind,
            user,
            token_in,
            token_out,
            amount_in,
            min_amount_out,
        )
        .with_pool(pool_address)
        .with_token_programs(token_in_program, token_out_program);
        let initial_plan = swaps::fetch_plan(&req);
        let initial_keys = initial_plan
            .addresses()
            .map(address_to_string)
            .collect::<Vec<_>>();
        let initial_data = self.fetch_account_data(&initial_keys).await?;
        let (followup_data, followup_keys_list) = match swaps::followup_keys(&req, &initial_data)? {
            Some(plan) => {
                let keys: Vec<String> = plan.addresses().map(address_to_string).collect();
                let data = self.fetch_account_data(&keys).await?;
                (data, keys)
            }
            None => (Vec::new(), Vec::new()),
        };
        let pool_snapshot = PoolSnapshot {
            dex,
            pool_data: pool_data,
            pool_address: address_to_string(&pool_address),
            additional_data: initial_data.clone(),
            followup_data: followup_data.clone(),
        };
        let snap = swaps::decode_snapshot(&req, &initial_data, &followup_data)?;
        let mut buf = InstructionBuf::new();
        swaps::build_swap(&req, &snap, &mut buf)?;
        let fresh_instruction = instruction_from_swaps(buf.as_view());
        let template_key = native_instruction_template_key(
            venue,
            pool,
            user,
            token_in,
            token_out,
            token_in_program,
            token_out_program,
        );
        let instruction = self
            .fill_or_refresh_instruction_template(template_key, fresh_instruction)
            .await;

        if let Some(ref sub) = self.pool_subscription {
            let plan_keys = PoolPlanKeys {
                pool_address: pool.to_string(),
                initial_keys: initial_keys.clone(),
                followup_keys: followup_keys_list,
            };
            let pool_key = pool.to_string();
            sub.cache_plan_keys(&pool_key, plan_keys).await;
            sub.subscribe_with_plan(&pool_key).await;
        }

        Ok((
            NativeSwapInstruction {
                instruction,
                dex_name: dex.name.to_string(),
            },
            pool_snapshot,
        ))
    }

    async fn fill_or_refresh_instruction_template(
        &self,
        key: String,
        fresh_instruction: Instruction,
    ) -> Instruction {
        let cached_template = {
            let templates = self.instruction_template_cache.read().await;
            templates.get(&key).cloned()
        };
        if let Some(template) = cached_template {
            if template.matches_static_layout(&fresh_instruction) {
                return template.fill(fresh_instruction.data);
            }
        }

        let template = NativeInstructionTemplate::from_instruction(&fresh_instruction);
        self.instruction_template_cache
            .write()
            .await
            .insert(key, template);
        fresh_instruction
    }

    async fn fresh_quote_from_snapshot(
        &self,
        snapshot: &PoolSnapshot,
        _venue: Venue,
        token_in: &Address,
        token_out: &Address,
        amount_in: u64,
        side: SwapSide,
    ) -> std::result::Result<u64, String> {
        match snapshot.dex.kind {
            DexKind::MeteoraDlmm => {
                let state = swaps::dex::meteora_dlmm::PoolState::from_bytes(&snapshot.pool_data)
                    .map_err(|error| format!("DLMM pool state decode failed: {error:?}"))?;
                let pool_address = pubkey_from_str(&snapshot.pool_address)
                    .map_err(|error| format!("DLMM pool address parse failed: {error}"))?;
                let pool_address = address_from_pubkey(&pool_address);
                let bin_array_targets =
                    native_dlmm_bin_array_fetch_targets(&pool_address, state.active_id());
                let bin_array_keys = bin_array_targets
                    .iter()
                    .map(|target| target.key.clone())
                    .collect::<Vec<_>>();
                let bin_array_data = self
                    .fetch_dlmm_bin_array_data(&bin_array_targets)
                    .await
                    .map_err(|error| {
                        format!(
                            "DLMM bin array fetch failed for [{}]: {error:#}",
                            bin_array_keys.join(",")
                        )
                    })?;
                native_dlmm_fresh_quote_from_bin_arrays_for_tokens(
                    &snapshot.pool_data,
                    amount_in,
                    token_in,
                    token_out,
                    &bin_array_data,
                )
                .map(|quote| quote.amount_out)
                .map_err(|error| {
                    format!(
                        "DLMM bin array quote failed for [{}]: {error:?}",
                        bin_array_keys.join(",")
                    )
                })
            }
            DexKind::MeteoraPools => {
                let a_vault = swaps::dex::meteora_pools::VaultState::from_bytes(
                    snapshot
                        .followup_data
                        .first()
                        .ok_or_else(|| "Meteora pools missing first vault state".to_string())?
                        .as_slice(),
                )
                .map_err(|error| format!("Meteora pools first vault decode failed: {error:?}"))?;
                let b_vault = swaps::dex::meteora_pools::VaultState::from_bytes(
                    snapshot
                        .followup_data
                        .get(1)
                        .ok_or_else(|| "Meteora pools missing second vault state".to_string())?
                        .as_slice(),
                )
                .map_err(|error| format!("Meteora pools second vault decode failed: {error:?}"))?;
                let token_vault_addresses = [a_vault.token_vault(), b_vault.token_vault()];
                let token_vault_keys = token_vault_addresses
                    .iter()
                    .map(address_to_string)
                    .collect::<Vec<_>>();
                let token_vault_data =
                    self.fetch_account_data(&token_vault_keys)
                        .await
                        .map_err(|error| {
                            format!(
                                "Meteora pools vault fetch failed for [{}]: {error:#}",
                                token_vault_keys.join(",")
                            )
                        })?;
                let token_vaults = [
                    (
                        *token_vault_addresses.first().ok_or_else(|| {
                            "Meteora pools missing first vault address".to_string()
                        })?,
                        token_vault_data
                            .first()
                            .ok_or_else(|| "Meteora pools missing first vault data".to_string())?
                            .as_slice(),
                    ),
                    (
                        *token_vault_addresses.get(1).ok_or_else(|| {
                            "Meteora pools missing second vault address".to_string()
                        })?,
                        token_vault_data
                            .get(1)
                            .ok_or_else(|| "Meteora pools missing second vault data".to_string())?
                            .as_slice(),
                    ),
                ];
                meteora_pools_fresh_quote_from_vaults(
                    &snapshot.pool_data,
                    snapshot
                        .followup_data
                        .first()
                        .ok_or_else(|| "Meteora pools missing first followup".to_string())?
                        .as_slice(),
                    snapshot
                        .followup_data
                        .get(1)
                        .ok_or_else(|| "Meteora pools missing second followup".to_string())?
                        .as_slice(),
                    token_in,
                    token_out,
                    amount_in,
                    &token_vaults,
                )
                .ok_or_else(|| "Meteora pools fresh quote returned no output".to_string())
            }
            DexKind::RaydiumCpmm => {
                let state = swaps::dex::raydium_cpmm::PoolState::from_bytes(&snapshot.pool_data)
                    .map_err(|error| format!("Raydium CPMM pool state decode failed: {error:?}"))?;
                let vault_addresses = [state.token_0_vault(), state.token_1_vault()];
                let vault_keys = vault_addresses
                    .iter()
                    .map(address_to_string)
                    .collect::<Vec<_>>();
                let vault_data = self
                    .fetch_account_data(&vault_keys)
                    .await
                    .map_err(|error| {
                        format!(
                            "Raydium CPMM vault fetch failed for [{}]: {error:#}",
                            vault_keys.join(",")
                        )
                    })?;
                let vaults = [
                    (
                        *vault_addresses
                            .first()
                            .ok_or_else(|| "Raydium CPMM missing first vault".to_string())?,
                        vault_data
                            .first()
                            .ok_or_else(|| "Raydium CPMM missing first vault data".to_string())?
                            .as_slice(),
                    ),
                    (
                        *vault_addresses
                            .get(1)
                            .ok_or_else(|| "Raydium CPMM missing second vault".to_string())?,
                        vault_data
                            .get(1)
                            .ok_or_else(|| "Raydium CPMM missing second vault data".to_string())?
                            .as_slice(),
                    ),
                ];
                raydium_cpmm_fresh_quote_from_vaults(
                    &snapshot.pool_data,
                    token_in,
                    token_out,
                    amount_in,
                    &vaults,
                )
                .ok_or_else(|| "Raydium CPMM fresh quote returned no output".to_string())
            }
            DexKind::OrcaWhirlpool | DexKind::RaydiumClmm => {
                let vault_data = snapshot
                    .additional_data
                    .iter()
                    .map(|data| data.clone())
                    .collect::<Vec<_>>();
                native_clmm_fresh_quote(&snapshot.pool_data, &vault_data, amount_in, side)
                    .ok_or_else(|| "CLMM fresh quote returned no output".to_string())
            }
            _ => {
                // For constant-product pools, estimate from pool data
                fresh_quote_constant_product(&snapshot.pool_data, amount_in, side)
                    .ok_or_else(|| "constant product fresh quote returned no output".to_string())
            }
        }
    }
}

struct PoolSnapshot {
    dex: NativeDex,
    pool_data: Vec<u8>,
    pool_address: String,
    additional_data: Vec<Vec<u8>>,
    followup_data: Vec<Vec<u8>>,
}

fn native_execution_cost_quote_atoms(
    lamports: u64,
    quote_decimals: u8,
    quote_mint: &str,
    sol_usd_estimate: f64,
) -> Option<u64> {
    if quote_mint == WSOL_MINT {
        return Some(lamports);
    }
    let sol_price = sol_usd_estimate.max(1.0);
    let quote_to_sol = if quote_mint == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" {
        1.0 / sol_price
    } else {
        1.0 / sol_price
    };
    let cost_in_quote =
        (lamports as f64 * quote_to_sol * 10f64.powi(quote_decimals as i32)) / 1_000_000_000.0;
    if cost_in_quote.is_finite() && cost_in_quote > 0.0 {
        Some(cost_in_quote as u64)
    } else {
        None
    }
}

fn native_sell_min_out_for_mode(
    mode: NativePlanBuildMode,
    simulation_priced: bool,
    sell_quote_out: u64,
    no_loss_min: u64,
    slippage_bps: u64,
) -> u64 {
    if mode == NativePlanBuildMode::ExploratorySimulation && simulation_priced {
        no_loss_min.max(1)
    } else {
        slippage_floor(sell_quote_out.max(no_loss_min), slippage_bps).max(1)
    }
}

fn native_buy_min_out_for_mode(
    mode: NativePlanBuildMode,
    sell_amount_cap: u64,
    buy_amount_in: u64,
    slippage_bps: u64,
) -> u64 {
    if mode == NativePlanBuildMode::ExploratorySimulation && sell_amount_cap == u64::MAX {
        1
    } else {
        slippage_floor(sell_amount_cap.min(buy_amount_in), slippage_bps).max(1)
    }
}

fn native_buy_min_out_after_fresh_quote(
    mode: NativePlanBuildMode,
    initial_buy_min_out: u64,
    sell_amount_cap: u64,
    fresh_buy_quote: Option<u64>,
    slippage_bps: u64,
) -> u64 {
    if mode == NativePlanBuildMode::ExploratorySimulation && sell_amount_cap == u64::MAX {
        fresh_buy_quote
            .filter(|amount| *amount > 0)
            .map(|amount| slippage_floor(amount, slippage_bps).max(1))
            .unwrap_or(initial_buy_min_out)
    } else {
        initial_buy_min_out
    }
}

fn native_sell_amount_for_mode(
    mode: NativePlanBuildMode,
    sell_amount_cap: u64,
    buy_quote_out: u64,
    guaranteed_buy_out: u64,
) -> u64 {
    if mode == NativePlanBuildMode::ExploratorySimulation && sell_amount_cap == u64::MAX {
        guaranteed_buy_out.max(1)
    } else {
        buy_quote_out.min(sell_amount_cap).max(1)
    }
}

fn exploratory_sell_amount_cap(opportunity: &Opportunity) -> u64 {
    u64::try_from(opportunity.base_atoms)
        .ok()
        .filter(|amount| *amount > 0)
        .unwrap_or(u64::MAX)
}

fn native_fresh_quote_blocked_reason(
    has_fresh_buy: bool,
    has_fresh_sell: bool,
    sell_quote_out: u64,
    sell_min_out: u64,
    _requote_net_profit_bps: f64,
    _allow_simulation_priced: bool,
    fresh_buy_error: Option<&str>,
    fresh_sell_error: Option<&str>,
) -> Option<String> {
    if !has_fresh_buy {
        return Some(native_fresh_quote_missing_reason("buy", fresh_buy_error));
    }
    if !has_fresh_sell {
        return Some(native_fresh_quote_missing_reason("sell", fresh_sell_error));
    }
    if sell_quote_out < sell_min_out {
        return Some(format!(
            "native direct sell quote out {sell_quote_out} < min out {sell_min_out}"
        ));
    }
    None
}

fn native_fresh_quote_missing_reason(side: &str, error: Option<&str>) -> String {
    if error.is_some_and(|message| message.contains("ZeroOutput")) {
        return format!("native direct fresh {side} quote produced zero output");
    }
    match error {
        Some(error) => format!("native direct fresh {side} quote unavailable: {error}"),
        None => format!("native direct fresh {side} quote unavailable"),
    }
}

fn native_direct_requote_gate_reason(
    fresh_requote_complete: bool,
    requote_net_profit_bps: f64,
    min_requote_profit_bps: f64,
    fallback_profit_bps: f64,
) -> Option<String> {
    if !fresh_requote_complete {
        return Some(
            "native direct fresh requote is incomplete; fallback quotes may be stale".to_string(),
        );
    }
    let effective_bps = if requote_net_profit_bps.is_finite() {
        requote_net_profit_bps
    } else {
        fallback_profit_bps
    };
    if effective_bps < min_requote_profit_bps {
        return Some(format!(
            "native direct re-quote net profit {requote_net_profit_bps:.2} bps is below required {min_requote_profit_bps:.2} bps"
        ));
    }
    None
}

fn requote_profit_bps(profit_atoms: i128, input_atoms: u64) -> f64 {
    if input_atoms == 0 {
        return f64::NEG_INFINITY;
    }
    let bps = (profit_atoms as f64 / input_atoms as f64) * 10_000.0;
    bps.clamp(-100_000.0, 100_000.0)
}

fn quote_profit_usd_signed(
    profit_atoms: i128,
    quote_decimals: u8,
    quote_mint: &str,
    sol_usd_estimate: f64,
) -> Option<f64> {
    let quote_ui = profit_atoms as f64 / 10f64.powi(quote_decimals as i32);
    if quote_mint == WSOL_MINT {
        Some(quote_ui * sol_usd_estimate)
    } else {
        Some(quote_ui)
    }
}

fn saturating_i128(value: u64) -> i128 {
    i128::from(value)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NativeDlmmFreshQuote {
    amount_out: u64,
    fee_amount: u64,
    bins_crossed: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NativeDlmmQuoteBlocker {
    InvalidPool,
    PoolDisabled,
    InvalidAmount,
    UnsupportedPair,
    InvalidBinArray { index: i64 },
    MissingBinArray { index: i64 },
    InvalidBinPrice { bin_id: i32 },
    ZeroOutput,
    InsufficientLiquidity { remaining_input: u64 },
    ArithmeticOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativeDlmmSwapDirection {
    XToY,
    YToX,
}

impl NativeDlmmSwapDirection {
    fn from_side(side: SwapSide) -> Self {
        match side {
            SwapSide::QuoteToBase => Self::YToX,
            SwapSide::BaseToQuote => Self::XToY,
        }
    }

    fn bin_step(self) -> i32 {
        match self {
            Self::XToY => -1,
            Self::YToX => 1,
        }
    }
}

struct NativeDlmmBinArray<'a> {
    index: i64,
    raw: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NativeDlmmBinArrayFetchTarget {
    key: String,
    index: i64,
    required: bool,
}

#[derive(Debug, Clone, Copy)]
struct NativeDlmmBin {
    amount_x: u64,
    amount_y: u64,
    price: u128,
}

fn native_dlmm_bin_array_fetch_targets(
    pool_address: &Address,
    active_id: i32,
) -> Vec<NativeDlmmBinArrayFetchTarget> {
    let active_index = swaps::dex::meteora_dlmm::bin_id_to_bin_array_index(active_id);
    swaps::dex::meteora_dlmm::selected_bin_array_indexes(active_id)
        .iter()
        .map(|index| NativeDlmmBinArrayFetchTarget {
            key: address_to_string(
                &swaps::dex::meteora_dlmm::bin_array_pda(pool_address, *index).0,
            ),
            index: *index,
            required: *index == active_index,
        })
        .collect()
}

fn native_dlmm_fresh_quote_from_bin_arrays(
    pool_data: &[u8],
    amount_in: u64,
    side: SwapSide,
    bin_array_data: &[Vec<u8>],
) -> Result<NativeDlmmFreshQuote, NativeDlmmQuoteBlocker> {
    native_dlmm_fresh_quote_from_bin_arrays_with_direction(
        pool_data,
        amount_in,
        NativeDlmmSwapDirection::from_side(side),
        bin_array_data,
    )
}

fn native_dlmm_fresh_quote_from_bin_arrays_for_tokens(
    pool_data: &[u8],
    amount_in: u64,
    token_in: &Address,
    token_out: &Address,
    bin_array_data: &[Vec<u8>],
) -> Result<NativeDlmmFreshQuote, NativeDlmmQuoteBlocker> {
    let pool = swaps::dex::meteora_dlmm::PoolState::from_bytes(pool_data)
        .map_err(|_| NativeDlmmQuoteBlocker::InvalidPool)?;
    let direction = if *token_in == pool.token_x_mint() && *token_out == pool.token_y_mint() {
        NativeDlmmSwapDirection::XToY
    } else if *token_in == pool.token_y_mint() && *token_out == pool.token_x_mint() {
        NativeDlmmSwapDirection::YToX
    } else {
        return Err(NativeDlmmQuoteBlocker::UnsupportedPair);
    };
    native_dlmm_fresh_quote_from_bin_arrays_with_direction(
        pool_data,
        amount_in,
        direction,
        bin_array_data,
    )
}

fn native_dlmm_fresh_quote_from_bin_arrays_with_direction(
    pool_data: &[u8],
    amount_in: u64,
    direction: NativeDlmmSwapDirection,
    bin_array_data: &[Vec<u8>],
) -> Result<NativeDlmmFreshQuote, NativeDlmmQuoteBlocker> {
    if amount_in == 0 {
        return Err(NativeDlmmQuoteBlocker::InvalidAmount);
    }
    let pool = swaps::dex::meteora_dlmm::PoolState::from_bytes(pool_data)
        .map_err(|_| NativeDlmmQuoteBlocker::InvalidPool)?;
    if !pool.enabled() {
        return Err(NativeDlmmQuoteBlocker::PoolDisabled);
    }
    let fee_bps = read_u16_at(pool_data, 8)
        .ok_or(NativeDlmmQuoteBlocker::InvalidPool)?
        .min(9_999) as u64;
    let fee_amount = amount_in.saturating_mul(fee_bps) / 10_000;
    let mut remaining_input = amount_in.saturating_sub(fee_amount);
    if remaining_input == 0 {
        return Err(NativeDlmmQuoteBlocker::InvalidAmount);
    }

    let mut arrays = BTreeMap::new();
    for data in bin_array_data {
        let array = decode_native_dlmm_bin_array(data)?;
        arrays.insert(array.index, array);
    }

    let mut amount_out = 0u64;
    let mut bins_crossed = 0u16;
    let mut bin_id = pool.active_id();
    let max_steps = arrays
        .len()
        .saturating_mul(swaps::dex::meteora_dlmm::BIN_ARRAY_SIZE as usize);
    for _ in 0..=max_steps {
        let array_index = swaps::dex::meteora_dlmm::bin_id_to_bin_array_index(bin_id);
        let array = arrays
            .get(&array_index)
            .ok_or(NativeDlmmQuoteBlocker::MissingBinArray { index: array_index })?;
        let bin = array.bin(bin_id)?;
        let available_out = match direction {
            NativeDlmmSwapDirection::YToX => bin.amount_x,
            NativeDlmmSwapDirection::XToY => bin.amount_y,
        };
        if bin.price == 0 {
            if available_out == 0 {
                bin_id = bin_id
                    .checked_add(direction.bin_step())
                    .ok_or(NativeDlmmQuoteBlocker::ArithmeticOverflow)?;
                continue;
            }
            return Err(NativeDlmmQuoteBlocker::InvalidBinPrice { bin_id });
        }
        if available_out > 0 {
            let input_to_clear_bin =
                native_dlmm_input_for_output(available_out, bin.price, direction)?;
            if remaining_input <= input_to_clear_bin {
                let partial_out =
                    native_dlmm_output_for_input(remaining_input, bin.price, direction)?;
                let partial_out = partial_out.min(available_out);
                amount_out = amount_out
                    .checked_add(partial_out)
                    .ok_or(NativeDlmmQuoteBlocker::ArithmeticOverflow)?;
                remaining_input = 0;
                break;
            }

            amount_out = amount_out
                .checked_add(available_out)
                .ok_or(NativeDlmmQuoteBlocker::ArithmeticOverflow)?;
            remaining_input = remaining_input.saturating_sub(input_to_clear_bin);
            bins_crossed = bins_crossed.saturating_add(1);
        }
        bin_id = bin_id
            .checked_add(direction.bin_step())
            .ok_or(NativeDlmmQuoteBlocker::ArithmeticOverflow)?;
    }

    if amount_out == 0 {
        return Err(NativeDlmmQuoteBlocker::ZeroOutput);
    }
    if remaining_input > 0 {
        return Err(NativeDlmmQuoteBlocker::InsufficientLiquidity { remaining_input });
    }

    Ok(NativeDlmmFreshQuote {
        amount_out,
        fee_amount,
        bins_crossed,
    })
}

fn decode_native_dlmm_bin_array(
    data: &[u8],
) -> Result<NativeDlmmBinArray<'_>, NativeDlmmQuoteBlocker> {
    let index = read_i64_at(data, 8).ok_or(NativeDlmmQuoteBlocker::InvalidBinArray { index: 0 })?;
    if data.len()
        < METEORA_DLMM_BIN_ARRAY_HEADER_LEN
            + swaps::dex::meteora_dlmm::BIN_ARRAY_SIZE as usize * METEORA_DLMM_BIN_LEN
        || data.get(..8) != Some(&swaps::dex::meteora_dlmm::BIN_ARRAY_DISCRIMINATOR)
    {
        return Err(NativeDlmmQuoteBlocker::InvalidBinArray { index });
    }
    Ok(NativeDlmmBinArray { index, raw: data })
}

impl NativeDlmmBinArray<'_> {
    fn bin(&self, bin_id: i32) -> Result<NativeDlmmBin, NativeDlmmQuoteBlocker> {
        let offset_in_array = i64::from(bin_id) - self.index * 70;
        if !(0..70).contains(&offset_in_array) {
            return Err(NativeDlmmQuoteBlocker::MissingBinArray { index: self.index });
        }
        let offset =
            METEORA_DLMM_BIN_ARRAY_HEADER_LEN + offset_in_array as usize * METEORA_DLMM_BIN_LEN;
        Ok(NativeDlmmBin {
            amount_x: read_u64_at(self.raw, offset)
                .ok_or(NativeDlmmQuoteBlocker::InvalidBinArray { index: self.index })?,
            amount_y: read_u64_at(self.raw, offset + 8)
                .ok_or(NativeDlmmQuoteBlocker::InvalidBinArray { index: self.index })?,
            price: read_u128_at(self.raw, offset + 16)
                .ok_or(NativeDlmmQuoteBlocker::InvalidBinArray { index: self.index })?,
        })
    }
}

fn native_dlmm_output_for_input(
    amount_in: u64,
    price_q64: u128,
    direction: NativeDlmmSwapDirection,
) -> Result<u64, NativeDlmmQuoteBlocker> {
    const Q64: u128 = 1u128 << 64;
    if price_q64 == 0 {
        return Err(NativeDlmmQuoteBlocker::InvalidBinPrice { bin_id: 0 });
    }
    Ok(match direction {
        NativeDlmmSwapDirection::YToX => {
            mul_div_floor_u64_saturating(u128::from(amount_in), Q64, price_q64)
        }
        NativeDlmmSwapDirection::XToY => {
            mul_div_floor_u64_saturating(u128::from(amount_in), price_q64, Q64)
        }
    })
}

fn mul_div_floor_u64_saturating(numerator: u128, multiplier: u128, denominator: u128) -> u64 {
    let Some(product) = numerator.checked_mul(multiplier) else {
        return u64::MAX;
    };
    u64::try_from(product / denominator).unwrap_or(u64::MAX)
}

fn native_dlmm_input_for_output(
    amount_out: u64,
    price_q64: u128,
    direction: NativeDlmmSwapDirection,
) -> Result<u64, NativeDlmmQuoteBlocker> {
    const Q64: u128 = 1u128 << 64;
    if price_q64 == 0 {
        return Err(NativeDlmmQuoteBlocker::InvalidBinPrice { bin_id: 0 });
    }
    Ok(match direction {
        NativeDlmmSwapDirection::YToX => {
            mul_div_ceil_u64_saturating(u128::from(amount_out), price_q64, Q64)
        }
        NativeDlmmSwapDirection::XToY => {
            mul_div_ceil_u64_saturating(u128::from(amount_out), Q64, price_q64)
        }
    })
}

fn mul_div_ceil_u64_saturating(numerator: u128, multiplier: u128, denominator: u128) -> u64 {
    let Some(product) = numerator.checked_mul(multiplier) else {
        return u64::MAX;
    };
    u64::try_from(div_ceil_u128(product, denominator)).unwrap_or(u64::MAX)
}

fn div_ceil_u128(numerator: u128, denominator: u128) -> u128 {
    numerator / denominator + u128::from(numerator % denominator != 0)
}

fn read_u16_at(data: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_le_bytes(
        data.get(offset..offset + 2)?.try_into().ok()?,
    ))
}

fn read_u64_at(data: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_le_bytes(
        data.get(offset..offset + 8)?.try_into().ok()?,
    ))
}

fn read_i64_at(data: &[u8], offset: usize) -> Option<i64> {
    Some(i64::from_le_bytes(
        data.get(offset..offset + 8)?.try_into().ok()?,
    ))
}

fn read_u128_at(data: &[u8], offset: usize) -> Option<u128> {
    Some(u128::from_le_bytes(
        data.get(offset..offset + 16)?.try_into().ok()?,
    ))
}

fn native_dlmm_fresh_quote(pool_data: &[u8], amount_in: u64, side: SwapSide) -> Option<u64> {
    if pool_data.len() < 688 {
        return None;
    }
    let token_x_reserve = u64::from_le_bytes(pool_data[72..80].try_into().ok()?);
    let token_y_reserve = u64::from_le_bytes(pool_data[80..88].try_into().ok()?);
    let (reserve_in, reserve_out) = match side {
        SwapSide::QuoteToBase => (token_y_reserve, token_x_reserve),
        SwapSide::BaseToQuote => (token_x_reserve, token_y_reserve),
    };
    if reserve_in == 0 || reserve_out == 0 {
        return None;
    }
    let fee_bps = 25u64;
    let amount_in_after_fee = amount_in.saturating_mul(10_000 - fee_bps) / 10_000;
    let amount_out = ((amount_in_after_fee as u128).saturating_mul(reserve_out as u128)
        / (reserve_in as u128 + amount_in_after_fee as u128)) as u64;
    if amount_out == 0 {
        None
    } else {
        Some(amount_out)
    }
}

fn native_clmm_fresh_quote(
    pool_data: &[u8],
    _vault_data: &[Vec<u8>],
    amount_in: u64,
    side: SwapSide,
) -> Option<u64> {
    if pool_data.len() < 260 {
        return None;
    }
    fresh_quote_constant_product(pool_data, amount_in, side)
}

fn raydium_cpmm_fresh_quote_from_vaults(
    pool_data: &[u8],
    token_in: &Address,
    token_out: &Address,
    amount_in: u64,
    vaults: &[(Address, &[u8])],
) -> Option<u64> {
    let state = swaps::dex::raydium_cpmm::PoolState::from_bytes(pool_data).ok()?;
    let mint_0 = state.token_0_mint();
    let mint_1 = state.token_1_mint();
    let vault_0 = state.token_0_vault();
    let vault_1 = state.token_1_vault();

    let reserve_0 = token_account_amount(vault_data(vaults, &vault_0)?)?;
    let reserve_1 = token_account_amount(vault_data(vaults, &vault_1)?)?;
    let (reserve_in, reserve_out) = if *token_in == mint_0 && *token_out == mint_1 {
        (reserve_0, reserve_1)
    } else if *token_in == mint_1 && *token_out == mint_0 {
        (reserve_1, reserve_0)
    } else {
        return None;
    };
    constant_product_amount_out(amount_in, reserve_in, reserve_out, 28)
}

fn meteora_pools_fresh_quote_from_vaults(
    pool_data: &[u8],
    a_vault_data: &[u8],
    b_vault_data: &[u8],
    token_in: &Address,
    token_out: &Address,
    amount_in: u64,
    token_vaults: &[(Address, &[u8])],
) -> Option<u64> {
    let state = swaps::dex::meteora_pools::PoolState::from_bytes(pool_data).ok()?;
    if !state.enabled() || state.curve_type_tag() != 0 {
        return None;
    }
    let a_vault = swaps::dex::meteora_pools::VaultState::from_bytes(a_vault_data).ok()?;
    let b_vault = swaps::dex::meteora_pools::VaultState::from_bytes(b_vault_data).ok()?;
    if !a_vault.enabled()
        || !b_vault.enabled()
        || a_vault.token_mint() != state.token_a_mint()
        || b_vault.token_mint() != state.token_b_mint()
    {
        return None;
    }

    let reserve_a = token_account_amount(vault_data(token_vaults, &a_vault.token_vault())?)?;
    let reserve_b = token_account_amount(vault_data(token_vaults, &b_vault.token_vault())?)?;
    let token_a = state.token_a_mint();
    let token_b = state.token_b_mint();
    let (reserve_in, reserve_out) = if *token_in == token_a && *token_out == token_b {
        (reserve_a, reserve_b)
    } else if *token_in == token_b && *token_out == token_a {
        (reserve_b, reserve_a)
    } else {
        return None;
    };

    constant_product_amount_out(
        amount_in,
        reserve_in,
        reserve_out,
        METEORA_POOLS_QUOTE_FEE_BPS,
    )
}

fn fresh_quote_constant_product(pool_data: &[u8], amount_in: u64, side: SwapSide) -> Option<u64> {
    if pool_data.len() < 144 {
        return None;
    }
    let token_x_reserve = u64::from_le_bytes(pool_data[72..80].try_into().ok()?);
    let token_y_reserve = u64::from_le_bytes(pool_data[144..152].try_into().ok()?);
    let (reserve_in, reserve_out) = match side {
        SwapSide::QuoteToBase => (token_y_reserve, token_x_reserve),
        SwapSide::BaseToQuote => (token_x_reserve, token_y_reserve),
    };
    if reserve_in == 0 || reserve_out == 0 {
        return None;
    }
    constant_product_amount_out(amount_in, reserve_in, reserve_out, 30)
}

fn constant_product_amount_out(
    amount_in: u64,
    reserve_in: u64,
    reserve_out: u64,
    fee_bps: u64,
) -> Option<u64> {
    if amount_in == 0 || reserve_in == 0 || reserve_out == 0 || fee_bps >= 10_000 {
        return None;
    }
    let amount_in_after_fee = amount_in.saturating_mul(10_000 - fee_bps) / 10_000;
    let amount_out = ((amount_in_after_fee as u128).saturating_mul(reserve_out as u128)
        / (reserve_in as u128 + amount_in_after_fee as u128)) as u64;
    if amount_out == 0 {
        None
    } else {
        Some(amount_out)
    }
}

fn vault_data<'a>(vaults: &'a [(Address, &[u8])], vault: &Address) -> Option<&'a [u8]> {
    vaults
        .iter()
        .find(|(address, _)| address == vault)
        .map(|(_, data)| *data)
}

fn token_account_amount(data: &[u8]) -> Option<u64> {
    Some(u64::from_le_bytes(data.get(64..72)?.try_into().ok()?))
}

fn push_native_latency_mark(trace: &mut Vec<NativeLatencyMark>, stage: &str, start: Instant) {
    trace.push(NativeLatencyMark {
        stage: stage.to_string(),
        elapsed_ms: start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
    });
}

async fn strict_fresh_cached_account(
    sub: &PoolSubscriptionManager,
    pubkey: &str,
    ttl_ms: u64,
) -> Result<CachedAccount> {
    let account = sub
        .get_cached(pubkey)
        .await
        .ok_or_else(|| anyhow!("native strict WS cache missing account: {pubkey}"))?;
    if ttl_ms == 0 {
        return Ok(account);
    }
    let age_ms = account
        .updated_at
        .elapsed()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    if age_ms > ttl_ms {
        return Err(anyhow!(
            "native strict WS cache stale account {pubkey}: age {age_ms} ms exceeds ttl {ttl_ms} ms"
        ));
    }
    Ok(account)
}

async fn strict_fresh_cached_accounts(
    sub: &PoolSubscriptionManager,
    pubkeys: &[String],
    ttl_ms: u64,
) -> Result<Vec<CachedAccount>> {
    let mut accounts = Vec::with_capacity(pubkeys.len());
    for pubkey in pubkeys {
        accounts.push(strict_fresh_cached_account(sub, pubkey, ttl_ms).await?);
    }
    Ok(accounts)
}

fn cached_account_age_ms(account: &CachedAccount) -> u64 {
    account
        .updated_at
        .elapsed()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn strict_refresh_margin_ms(ttl_ms: u64) -> u64 {
    if ttl_ms == 0 {
        return 0;
    }
    ttl_ms
        .saturating_mul(4)
        .saturating_div(5)
        .clamp(25, 200)
        .min(ttl_ms)
}

fn strict_refresh_cutoff_ms(ttl_ms: u64) -> u64 {
    ttl_ms.saturating_sub(strict_refresh_margin_ms(ttl_ms))
}

async fn refresh_optional_strict_cached_accounts(
    rpc: &RpcClient,
    sub: &PoolSubscriptionManager,
    pubkeys: &[String],
    ttl_ms: u64,
) -> Result<Vec<Option<CachedAccount>>> {
    let refresh_keys = strict_refresh_needed_keys(sub, pubkeys, ttl_ms).await;
    let refresh_keys = strict_refresh_request_keys(pubkeys, &refresh_keys);
    let mut missing_after_refresh = HashSet::new();
    if !refresh_keys.is_empty() {
        let accounts = rpc.get_multiple_accounts(&refresh_keys).await?;
        let mut warmed = Vec::new();
        for (index, account) in accounts.into_iter().enumerate() {
            if let Some(account) = account {
                warmed.push(account);
            } else {
                missing_after_refresh.insert(refresh_keys[index].clone());
            }
        }
        sub.warm_raw_accounts(warmed).await;
    }

    let mut refreshed = Vec::with_capacity(pubkeys.len());
    for pubkey in pubkeys {
        if missing_after_refresh.contains(pubkey) {
            refreshed.push(None);
        } else {
            refreshed.push(Some(
                strict_fresh_cached_account(sub, pubkey, ttl_ms).await?,
            ));
        }
    }
    Ok(refreshed)
}

fn strict_refresh_request_keys(pubkeys: &[String], refresh_keys: &[String]) -> Vec<String> {
    if refresh_keys.is_empty() {
        return Vec::new();
    }
    let mut seen = HashSet::new();
    pubkeys
        .iter()
        .filter(|pubkey| seen.insert((*pubkey).clone()))
        .cloned()
        .collect()
}

async fn strict_refresh_needed_keys(
    sub: &PoolSubscriptionManager,
    pubkeys: &[String],
    ttl_ms: u64,
) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut refresh_keys = Vec::new();
    for pubkey in pubkeys {
        if !seen.insert(pubkey.clone()) {
            continue;
        }
        let Some(account) = sub.get_cached(pubkey).await else {
            refresh_keys.push(pubkey.clone());
            continue;
        };
        if ttl_ms > 0 && cached_account_age_ms(&account) >= strict_refresh_cutoff_ms(ttl_ms) {
            refresh_keys.push(pubkey.clone());
        }
    }
    refresh_keys
}

struct NativeSwapInstruction {
    instruction: Instruction,
    dex_name: String,
}

#[derive(Clone)]
struct NativeInstructionTemplate {
    program_id: Pubkey,
    accounts: Vec<AccountMeta>,
}

impl NativeInstructionTemplate {
    fn from_instruction(instruction: &Instruction) -> Self {
        Self {
            program_id: instruction.program_id,
            accounts: instruction.accounts.clone(),
        }
    }

    fn matches_static_layout(&self, instruction: &Instruction) -> bool {
        self.program_id == instruction.program_id && self.accounts == instruction.accounts
    }

    fn fill(&self, data: Vec<u8>) -> Instruction {
        Instruction {
            program_id: self.program_id,
            accounts: self.accounts.clone(),
            data,
        }
    }
}

#[derive(Clone, Copy)]
struct NativeDex {
    kind: DexKind,
    name: &'static str,
}

fn native_pool_candidate(venue: Venue, pool: &str) -> bool {
    matches!(venue, Venue::Orca | Venue::Meteora | Venue::Raydium)
        && !pool.contains(':')
        && Pubkey::from_str(pool).is_ok()
}

fn native_instruction_template_key(
    venue: Venue,
    pool: &str,
    user: Address,
    token_in: Address,
    token_out: Address,
    token_in_program: Address,
    token_out_program: Address,
) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}:{}",
        venue,
        pool,
        address_to_string(&user),
        address_to_string(&token_in),
        address_to_string(&token_out),
        address_to_string(&token_in_program),
        address_to_string(&token_out_program)
    )
}

fn dex_for_pool_owner(venue: Venue, owner: &Pubkey) -> Option<NativeDex> {
    let owner_address = address_from_pubkey(owner);
    match venue {
        Venue::Orca if owner_address == swaps::dex::orca_whirlpool::PROGRAM_ID => Some(NativeDex {
            kind: DexKind::OrcaWhirlpool,
            name: "orca_whirlpool",
        }),
        Venue::Meteora if owner_address == swaps::dex::meteora_dlmm::PROGRAM_ID => {
            Some(NativeDex {
                kind: DexKind::MeteoraDlmm,
                name: "meteora_dlmm",
            })
        }
        Venue::Meteora if owner_address == swaps::dex::meteora_damm_v2::PROGRAM_ID => {
            Some(NativeDex {
                kind: DexKind::MeteoraDammV2,
                name: "meteora_damm_v2",
            })
        }
        Venue::Meteora if owner_address == swaps::dex::meteora_pools::PROGRAM_ID => {
            Some(NativeDex {
                kind: DexKind::MeteoraPools,
                name: "meteora_pools",
            })
        }
        Venue::Raydium if owner_address == swaps::dex::raydium_cpmm::PROGRAM_ID => {
            Some(NativeDex {
                kind: DexKind::RaydiumCpmm,
                name: "raydium_cpmm",
            })
        }
        Venue::Raydium if owner_address == swaps::dex::raydium_clmm::PROGRAM_ID => {
            Some(NativeDex {
                kind: DexKind::RaydiumClmm,
                name: "raydium_clmm",
            })
        }
        Venue::Raydium if owner_address == swaps::dex::raydium_amm_v4::PROGRAM_ID => {
            Some(NativeDex {
                kind: DexKind::RaydiumAmmV4,
                name: "raydium_amm_v4",
            })
        }
        _ => None,
    }
}

fn native_compute_budget_instructions(config: &BotConfig) -> Vec<Instruction> {
    let mut instructions = vec![
        solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(
            config.execution.compute_unit_limit,
        ),
    ];
    let compute_unit_price = if config.execution.compute_unit_price_micro_lamports > 0 {
        config.execution.compute_unit_price_micro_lamports
    } else if config.execution.compute_unit_limit > 0 {
        config
            .fees
            .max_priority_fee_lamports
            .saturating_mul(1_000_000)
            / u64::from(config.execution.compute_unit_limit)
    } else {
        0
    };
    if compute_unit_price > 0 {
        instructions.push(
            solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_price(
                compute_unit_price,
            ),
        );
    }
    instructions
}

fn jito_tip_ix(wallet: &Pubkey, lamports: u64) -> Result<Instruction> {
    let tip_account = pubkey_from_str("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5")?;
    system_transfer_ix(wallet, &tip_account, lamports)
}

fn instruction_from_swaps(ix: swaps::InstructionView<'_>) -> Instruction {
    Instruction {
        program_id: pubkey_from_address(&ix.program_id),
        accounts: ix
            .accounts
            .iter()
            .map(|account| {
                if account.is_writable {
                    AccountMeta::new(pubkey_from_address(&account.address), account.is_signer)
                } else {
                    AccountMeta::new_readonly(
                        pubkey_from_address(&account.address),
                        account.is_signer,
                    )
                }
            })
            .collect(),
        data: ix.data.to_vec(),
    }
}

fn wrap_wsol_ixs(user: &Pubkey, lamports: u64) -> Result<Vec<Instruction>> {
    let token_program = pubkey_from_str(TOKEN_PROGRAM)?;
    let wsol_mint = pubkey_from_str(WSOL_MINT)?;
    let wsol_ata = associated_token_address(user, &wsol_mint, &token_program)?;
    Ok(vec![
        create_ata_idempotent_ix(user, user, &wsol_mint, &token_program)?,
        system_transfer_ix(user, &wsol_ata, lamports)?,
        sync_native_ix(&wsol_ata, &token_program),
    ])
}

fn native_setup_prefix_instructions(
    config: &BotConfig,
    wallet: &Pubkey,
    quote_mint: &Pubkey,
    base_mint: &Pubkey,
    quote_token_program: &Pubkey,
    base_token_program: &Pubkey,
    buy_amount_in: u64,
) -> Result<Vec<Instruction>> {
    if config.execution.native_prefunded_hot_accounts {
        return Ok(Vec::new());
    }

    let mut instructions = vec![
        create_ata_idempotent_ix(wallet, wallet, quote_mint, quote_token_program)?,
        create_ata_idempotent_ix(wallet, wallet, base_mint, base_token_program)?,
    ];
    if quote_mint.to_string() == WSOL_MINT {
        instructions.extend(wrap_wsol_ixs(wallet, buy_amount_in)?);
    }
    Ok(instructions)
}

fn native_setup_suffix_instructions(
    config: &BotConfig,
    wallet: &Pubkey,
    quote_mint: &Pubkey,
    quote_token_program: &Pubkey,
) -> Result<Vec<Instruction>> {
    if config.execution.native_prefunded_hot_accounts || quote_mint.to_string() != WSOL_MINT {
        return Ok(Vec::new());
    }
    Ok(vec![close_account_ix(
        &associated_token_address(wallet, quote_mint, quote_token_program)?,
        wallet,
        wallet,
        quote_token_program,
    )?])
}

fn create_ata_idempotent_ix(
    funder: &Pubkey,
    owner: &Pubkey,
    mint: &Pubkey,
    token_program: &Pubkey,
) -> Result<Instruction> {
    let ata = associated_token_address(owner, mint, token_program)?;
    Ok(Instruction {
        program_id: pubkey_from_str(ASSOCIATED_TOKEN_PROGRAM)?,
        accounts: vec![
            AccountMeta::new(*funder, true),
            AccountMeta::new(ata, false),
            AccountMeta::new_readonly(*owner, false),
            AccountMeta::new_readonly(*mint, false),
            AccountMeta::new_readonly(pubkey_from_str(SYSTEM_PROGRAM)?, false),
            AccountMeta::new_readonly(*token_program, false),
        ],
        data: vec![1],
    })
}

fn sync_native_ix(wsol_ata: &Pubkey, token_program: &Pubkey) -> Instruction {
    Instruction {
        program_id: *token_program,
        accounts: vec![AccountMeta::new(*wsol_ata, false)],
        data: vec![17],
    }
}

fn close_account_ix(
    account: &Pubkey,
    destination: &Pubkey,
    authority: &Pubkey,
    token_program: &Pubkey,
) -> Result<Instruction> {
    Ok(Instruction {
        program_id: *token_program,
        accounts: vec![
            AccountMeta::new(*account, false),
            AccountMeta::new(*destination, false),
            AccountMeta::new_readonly(*authority, true),
        ],
        data: vec![9],
    })
}

fn system_transfer_ix(from: &Pubkey, to: &Pubkey, lamports: u64) -> Result<Instruction> {
    let mut data = Vec::with_capacity(12);
    data.extend_from_slice(&2u32.to_le_bytes());
    data.extend_from_slice(&lamports.to_le_bytes());
    Ok(Instruction {
        program_id: pubkey_from_str(SYSTEM_PROGRAM)?,
        accounts: vec![AccountMeta::new(*from, true), AccountMeta::new(*to, false)],
        data,
    })
}

fn associated_token_address(
    owner: &Pubkey,
    mint: &Pubkey,
    token_program: &Pubkey,
) -> Result<Pubkey> {
    let address = swaps::pda::associated_token_address(
        &address_from_pubkey(owner),
        &address_from_pubkey(mint),
        &address_from_pubkey(token_program),
    );
    Ok(pubkey_from_address(&address))
}

fn scale_amount(amount: u128, numerator: u64, denominator: u64) -> Result<u64> {
    if denominator == 0 {
        return Err(anyhow!("cannot scale amount with zero denominator"));
    }
    Ok(amount
        .saturating_mul(u128::from(numerator))
        .checked_div(u128::from(denominator))
        .unwrap_or_default()
        .min(u128::from(u64::MAX)) as u64)
}

fn slippage_floor(amount: u64, slippage_bps: u64) -> u64 {
    let retained_bps = 10_000u64.saturating_sub(slippage_bps.min(9_999));
    ((u128::from(amount) * u128::from(retained_bps)) / 10_000) as u64
}

fn live_native_slippage_bps(default_slippage_bps: u16) -> u16 {
    env_value("LIVE_ARBITRAGE_SLIPPAGE_BPS")
        .and_then(|value| value.parse::<u16>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default_slippage_bps.max(1))
        .min(2_000)
}

fn required_profit_atoms(input_atoms: u128, min_profit_bps: f64) -> u128 {
    if min_profit_bps <= 0.0 || !min_profit_bps.is_finite() {
        return 0;
    }
    ((input_atoms as f64) * min_profit_bps / 10_000.0).ceil() as u128
}

fn pubkey_from_str(value: &str) -> Result<Pubkey> {
    Pubkey::from_str(value).with_context(|| format!("invalid pubkey {value}"))
}

fn address_from_pubkey(pubkey: &Pubkey) -> Address {
    Address::new_from_array(pubkey.to_bytes())
}

fn pubkey_from_address(address: &Address) -> Pubkey {
    Pubkey::new_from_array(address.to_bytes())
}

fn address_to_string(address: &Address) -> String {
    pubkey_from_address(address).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slippage_floor_caps_to_nonzero_retained_bps() {
        assert_eq!(slippage_floor(1_000_000, 50), 995_000);
        assert_eq!(slippage_floor(1_000_000, 50_000), 100);
    }

    #[test]
    fn live_native_slippage_uses_positive_default() {
        assert_eq!(live_native_slippage_bps(0), 1);
        assert_eq!(live_native_slippage_bps(50), 50);
    }

    #[test]
    fn native_candidate_requires_plain_pool_address() {
        assert!(!native_pool_candidate(Venue::Jupiter, "route"));
        assert!(!native_pool_candidate(Venue::Orca, "jupiter-probe:orca"));
        assert!(native_pool_candidate(
            Venue::Orca,
            "11111111111111111111111111111111"
        ));
    }

    #[test]
    fn native_instruction_template_reuses_static_accounts_and_fills_fresh_data() {
        let program_id = Pubkey::new_unique();
        let account = Pubkey::new_unique();
        let original = Instruction {
            program_id,
            accounts: vec![AccountMeta::new(account, false)],
            data: vec![1, 2, 3],
        };
        let template = NativeInstructionTemplate::from_instruction(&original);
        let fresh_same_layout = Instruction {
            data: vec![9, 8, 7],
            ..original.clone()
        };

        assert!(template.matches_static_layout(&fresh_same_layout));
        let filled = template.fill(fresh_same_layout.data.clone());
        assert_eq!(filled.program_id, original.program_id);
        assert_eq!(filled.accounts, original.accounts);
        assert_eq!(filled.data, vec![9, 8, 7]);

        let changed_layout = Instruction {
            accounts: vec![AccountMeta::new_readonly(account, false)],
            ..original
        };
        assert!(!template.matches_static_layout(&changed_layout));
    }

    #[test]
    fn missing_fresh_native_quote_blocks_before_zero_bps_profit_gate() {
        assert_eq!(
            native_fresh_quote_blocked_reason(false, true, 1_000, 1, 0.0, true, None, None),
            Some("native direct fresh buy quote unavailable".to_string())
        );
        assert_eq!(
            native_fresh_quote_blocked_reason(true, false, 1_000, 1, 0.0, true, None, None),
            Some("native direct fresh sell quote unavailable".to_string())
        );
        assert_eq!(
            native_fresh_quote_blocked_reason(
                false,
                true,
                1_000,
                1,
                0.0,
                true,
                Some("DLMM bin array fetch failed for [bin-a]: native swap account missing: bin-a"),
                None,
            ),
            Some(
                "native direct fresh buy quote unavailable: DLMM bin array fetch failed for [bin-a]: native swap account missing: bin-a"
                    .to_string()
            )
        );
        assert_eq!(
            native_fresh_quote_blocked_reason(
                false,
                true,
                1_000,
                1,
                0.0,
                true,
                Some("DLMM bin array quote failed for [bin-a]: ZeroOutput"),
                None,
            ),
            Some("native direct fresh buy quote produced zero output".to_string())
        );
    }

    #[test]
    fn raydium_cpmm_sell_quote_uses_vault_balances_and_mint_order() {
        let wsol = pubkey_from_str(WSOL_MINT).unwrap();
        let wif = pubkey_from_str("EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm").unwrap();
        let wsol_vault = pubkey_from_str("GVPPvur2666HU4qqQB3VUH1Upd8ZKDnJXQMTGJaZatF8").unwrap();
        let wif_vault = pubkey_from_str("Aj18sAdxm7QSUiT6iaxtqCVHfe9ch48M3woM8gU7XJ9S").unwrap();

        let mut pool = vec![0; swaps::dex::raydium_cpmm::POOL_STATE_MIN_LEN];
        pool[..8].copy_from_slice(&swaps::dex::raydium_cpmm::POOL_STATE_DISCRIMINATOR);
        pool[72..104].copy_from_slice(&address_from_pubkey(&wsol_vault).to_bytes());
        pool[104..136].copy_from_slice(&address_from_pubkey(&wif_vault).to_bytes());
        pool[168..200].copy_from_slice(&address_from_pubkey(&wsol).to_bytes());
        pool[200..232].copy_from_slice(&address_from_pubkey(&wif).to_bytes());

        let wsol_vault_data = token_account_data_with_amount(8_000_000_000);
        let wif_vault_data = token_account_data_with_amount(200_000_000_000);
        let out = raydium_cpmm_fresh_quote_from_vaults(
            &pool,
            &address_from_pubkey(&wif),
            &address_from_pubkey(&wsol),
            1_000_000,
            &[
                (address_from_pubkey(&wsol_vault), wsol_vault_data.as_slice()),
                (address_from_pubkey(&wif_vault), wif_vault_data.as_slice()),
            ],
        )
        .expect("fresh cpmm quote");

        assert!(out > 35_000);
        assert!(out < 45_000);
    }

    #[test]
    fn meteora_pools_quote_uses_token_vault_balances_and_mint_order() {
        let wsol = pubkey_from_str(WSOL_MINT).unwrap();
        let wif = pubkey_from_str("EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm").unwrap();
        let a_vault = pubkey_from_str("GkYAvt7ecy7juVHyPBssmJsw6DQLZ5r1YTzf58DMLFxu").unwrap();
        let b_vault = pubkey_from_str("anQHGFzjoUf6F663ZVzC1irEgvXdgHdgp35knPg3wNj").unwrap();
        let a_token_vault =
            pubkey_from_str("ATUMydDvNcELzNk9GP1Ky7i2Mgx2t2ej5aNPMhA6F2VH").unwrap();
        let b_token_vault =
            pubkey_from_str("ChcWkmUbWDbBspDjPX6ZXi7Hb9kZ7VTbNUf6nMtWF1YH").unwrap();
        let a_lp_mint = pubkey_from_str("D4FPEruKEHrG5TenZ2mpDGEfu1iUvTiqBxvpU8HLBvC2").unwrap();
        let b_lp_mint = pubkey_from_str("2uiwPb51HMsNPfk6C2MF7mx9vcjN2coDGct1cYM96nCt").unwrap();

        let mut pool = vec![0; swaps::dex::meteora_pools::POOL_STATE_MIN_LEN];
        pool[..8].copy_from_slice(&swaps::dex::meteora_pools::POOL_DISCRIMINATOR);
        write_test_address(&mut pool, 40, &wsol);
        write_test_address(&mut pool, 72, &wif);
        write_test_address(&mut pool, 104, &a_vault);
        write_test_address(&mut pool, 136, &b_vault);
        pool[233] = 1;

        let mut a_vault_data = vec![0; swaps::dex::meteora_pools::VAULT_STATE_MIN_LEN];
        a_vault_data[..8].copy_from_slice(&swaps::dex::meteora_pools::VAULT_DISCRIMINATOR);
        a_vault_data[8] = 1;
        write_test_address(&mut a_vault_data, 19, &a_token_vault);
        write_test_address(&mut a_vault_data, 83, &wsol);
        write_test_address(&mut a_vault_data, 115, &a_lp_mint);

        let mut b_vault_data = vec![0; swaps::dex::meteora_pools::VAULT_STATE_MIN_LEN];
        b_vault_data[..8].copy_from_slice(&swaps::dex::meteora_pools::VAULT_DISCRIMINATOR);
        b_vault_data[8] = 1;
        write_test_address(&mut b_vault_data, 19, &b_token_vault);
        write_test_address(&mut b_vault_data, 83, &wif);
        write_test_address(&mut b_vault_data, 115, &b_lp_mint);

        let wsol_vault_data = token_account_data_with_amount(8_000_000_000);
        let wif_vault_data = token_account_data_with_amount(200_000_000_000);
        let out = meteora_pools_fresh_quote_from_vaults(
            &pool,
            &a_vault_data,
            &b_vault_data,
            &address_from_pubkey(&wsol),
            &address_from_pubkey(&wif),
            10_000_000,
            &[
                (
                    address_from_pubkey(&b_token_vault),
                    wif_vault_data.as_slice(),
                ),
                (
                    address_from_pubkey(&a_token_vault),
                    wsol_vault_data.as_slice(),
                ),
            ],
        )
        .expect("fresh meteora pools quote");

        assert!(out > 245_000_000);
        assert!(out < 250_000_000);
    }

    #[test]
    fn meteora_pools_quote_rejects_unpriced_stable_curve() {
        let wsol = pubkey_from_str(WSOL_MINT).unwrap();
        let wif = pubkey_from_str("EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm").unwrap();
        let token_vault = pubkey_from_str("ATUMydDvNcELzNk9GP1Ky7i2Mgx2t2ej5aNPMhA6F2VH").unwrap();
        let lp_mint = pubkey_from_str("D4FPEruKEHrG5TenZ2mpDGEfu1iUvTiqBxvpU8HLBvC2").unwrap();

        let mut pool = vec![0; swaps::dex::meteora_pools::POOL_STATE_MIN_LEN];
        pool[..8].copy_from_slice(&swaps::dex::meteora_pools::POOL_DISCRIMINATOR);
        write_test_address(&mut pool, 40, &wsol);
        write_test_address(&mut pool, 72, &wif);
        pool[233] = 1;
        pool[874] = 1;

        let mut vault_data = vec![0; swaps::dex::meteora_pools::VAULT_STATE_MIN_LEN];
        vault_data[..8].copy_from_slice(&swaps::dex::meteora_pools::VAULT_DISCRIMINATOR);
        vault_data[8] = 1;
        write_test_address(&mut vault_data, 19, &token_vault);
        write_test_address(&mut vault_data, 83, &wsol);
        write_test_address(&mut vault_data, 115, &lp_mint);

        assert!(meteora_pools_fresh_quote_from_vaults(
            &pool,
            &vault_data,
            &vault_data,
            &address_from_pubkey(&wsol),
            &address_from_pubkey(&wif),
            10_000_000,
            &[(address_from_pubkey(&token_vault), [0u8; 165].as_slice())],
        )
        .is_none());
    }

    #[test]
    fn meteora_dlmm_quote_uses_active_bin_liquidity() {
        let pool = test_dlmm_pool_data(0, 25);
        let bin_array = test_dlmm_bin_array(0, &[(0, 5_000, 50_000, Q64)]);

        let quote = native_dlmm_fresh_quote_from_bin_arrays(
            &pool,
            1_000,
            SwapSide::QuoteToBase,
            &[bin_array],
        )
        .expect("one-bin dlmm quote");

        assert_eq!(quote.fee_amount, 2);
        assert_eq!(quote.amount_out, 998);
    }

    #[test]
    fn meteora_dlmm_bin_array_fetch_targets_require_only_active_array() {
        let pool_address = address_from_pubkey(&Pubkey::new_unique());
        let targets = native_dlmm_bin_array_fetch_targets(&pool_address, 70);

        assert_eq!(
            targets
                .iter()
                .map(|target| (target.index, target.required))
                .collect::<Vec<_>>(),
            vec![(0, false), (1, true), (2, false)]
        );
    }

    #[test]
    fn meteora_dlmm_quote_crosses_to_next_bin_array() {
        let pool = test_dlmm_pool_data(69, 0);
        let current = test_dlmm_bin_array(0, &[(69, 500, 50_000, Q64)]);
        let next = test_dlmm_bin_array(1, &[(70, 900, 50_000, Q64)]);

        let out = native_dlmm_fresh_quote_from_bin_arrays(
            &pool,
            1_200,
            SwapSide::QuoteToBase,
            &[current, next],
        )
        .expect("multi-bin dlmm quote")
        .amount_out;

        assert_eq!(out, 1_200);
    }

    #[test]
    fn meteora_dlmm_quote_blocks_when_required_bin_array_missing() {
        let pool = test_dlmm_pool_data(69, 0);
        let current = test_dlmm_bin_array(0, &[(69, 500, 50_000, Q64)]);

        let err = native_dlmm_fresh_quote_from_bin_arrays(
            &pool,
            1_200,
            SwapSide::QuoteToBase,
            &[current],
        )
        .expect_err("next bin array is required");

        assert_eq!(err, NativeDlmmQuoteBlocker::MissingBinArray { index: 1 });
    }

    #[test]
    fn meteora_dlmm_quote_skips_empty_zero_price_bins() {
        let pool = test_dlmm_pool_data(117, 0);
        let array = test_dlmm_bin_array(1, &[(118, 1_000, 10_000, Q64)]);

        let quote =
            native_dlmm_fresh_quote_from_bin_arrays(&pool, 500, SwapSide::QuoteToBase, &[array])
                .expect("empty active bin should not block next priced bin");

        assert_eq!(quote.amount_out, 500);
    }

    #[test]
    fn meteora_dlmm_quote_handles_large_q64_price_without_overflow() {
        let pool = test_dlmm_pool_data(0, 0);
        let large_price = Q64 * 1_000_000_000_000u128;
        let bin_array = test_dlmm_bin_array(0, &[(0, 1_000_000, u64::MAX, large_price)]);

        let quote = native_dlmm_fresh_quote_from_bin_arrays(
            &pool,
            1_000_000_000_000,
            SwapSide::QuoteToBase,
            &[bin_array],
        )
        .expect("large price should use checked mul/div math, not overflow");

        assert_eq!(quote.amount_out, 1);
    }

    #[test]
    fn meteora_dlmm_quote_saturates_bin_clear_math_for_large_price() {
        let pool = test_dlmm_pool_data(0, 0);
        let bin_array = test_dlmm_bin_array(0, &[(0, u64::MAX, 10_000, Q64 * 2)]);

        let quote = native_dlmm_fresh_quote_from_bin_arrays(
            &pool,
            1_000,
            SwapSide::QuoteToBase,
            &[bin_array],
        )
        .expect("small partial fill should not require exact full-bin clear math");

        assert_eq!(quote.amount_out, 500);
    }

    #[test]
    fn meteora_dlmm_quote_reports_zero_output_after_rounding() {
        let pool = test_dlmm_pool_data(0, 0);
        let bin_array = test_dlmm_bin_array(0, &[(0, u64::MAX, 10_000, Q64 * 2)]);

        let err =
            native_dlmm_fresh_quote_from_bin_arrays(&pool, 1, SwapSide::QuoteToBase, &[bin_array])
                .expect_err("input too small for one output atom at live bin price");

        assert_eq!(err, NativeDlmmQuoteBlocker::ZeroOutput);
    }

    #[test]
    fn meteora_dlmm_quote_uses_pool_token_orientation() {
        let token_x = pubkey_from_str("EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm").unwrap();
        let token_y = pubkey_from_str(WSOL_MINT).unwrap();
        let pool = test_dlmm_pool_data_with_mints(0, 0, &token_x, &token_y);
        let bin_array = test_dlmm_bin_array(0, &[(0, 1_000, 50_000, Q64 * 2)]);

        let y_to_x = native_dlmm_fresh_quote_from_bin_arrays_for_tokens(
            &pool,
            2_000,
            &address_from_pubkey(&token_y),
            &address_from_pubkey(&token_x),
            &[bin_array.clone()],
        )
        .expect("token Y to token X quote");
        assert_eq!(y_to_x.amount_out, 1_000);

        let x_to_y = native_dlmm_fresh_quote_from_bin_arrays_for_tokens(
            &pool,
            500,
            &address_from_pubkey(&token_x),
            &address_from_pubkey(&token_y),
            &[bin_array],
        )
        .expect("token X to token Y quote");
        assert_eq!(x_to_y.amount_out, 1_000);
    }

    #[test]
    fn exploratory_sell_amount_cap_is_unbounded_when_base_atoms_are_missing() {
        let mut opportunity = test_native_opportunity();
        opportunity.base_atoms = 0;

        assert_eq!(exploratory_sell_amount_cap(&opportunity), u64::MAX);

        opportunity.base_atoms = 123_456;
        assert_eq!(exploratory_sell_amount_cap(&opportunity), 123_456);
    }

    #[test]
    fn exploratory_buy_min_out_is_permissive_when_base_atoms_are_missing() {
        assert_eq!(
            native_buy_min_out_for_mode(
                NativePlanBuildMode::ExploratorySimulation,
                u64::MAX,
                7_000_000,
                50,
            ),
            1
        );
        assert_eq!(
            native_buy_min_out_for_mode(
                NativePlanBuildMode::ExploratorySimulation,
                100_000,
                7_000_000,
                50
            ),
            99_500
        );
        assert_eq!(
            native_buy_min_out_for_mode(NativePlanBuildMode::Strict, u64::MAX, 7_000_000, 50),
            6_965_000
        );
    }

    #[test]
    fn exploratory_buy_min_out_uses_fresh_quote_when_base_atoms_are_missing() {
        assert_eq!(
            native_buy_min_out_after_fresh_quote(
                NativePlanBuildMode::ExploratorySimulation,
                1,
                u64::MAX,
                Some(16_808_607),
                50,
            ),
            16_724_563
        );
        assert_eq!(
            native_buy_min_out_after_fresh_quote(
                NativePlanBuildMode::ExploratorySimulation,
                99_500,
                100_000,
                Some(16_808_607),
                50,
            ),
            99_500
        );
        assert_eq!(
            native_buy_min_out_after_fresh_quote(
                NativePlanBuildMode::Strict,
                1,
                u64::MAX,
                Some(16_808_607),
                50
            ),
            1
        );
    }

    #[test]
    fn exploratory_sell_amount_uses_slippage_buffer_when_base_atoms_are_missing() {
        assert_eq!(
            native_sell_amount_for_mode(
                NativePlanBuildMode::ExploratorySimulation,
                u64::MAX,
                16_808_607,
                16_724_563,
            ),
            16_724_563
        );
        assert_eq!(
            native_sell_amount_for_mode(
                NativePlanBuildMode::ExploratorySimulation,
                12_000_000,
                16_808_607,
                16_724_563,
            ),
            12_000_000
        );
        assert_eq!(
            native_sell_amount_for_mode(NativePlanBuildMode::Strict, u64::MAX, 16_808_607, 1),
            16_808_607
        );
    }

    #[test]
    fn token_2022_program_matches_rwa_registry_program() {
        assert_eq!(
            TOKEN_2022_PROGRAM,
            "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
        );
        assert!(pubkey_from_str(TOKEN_2022_PROGRAM).is_ok());
    }

    #[test]
    fn prefunded_hot_account_mode_skips_setup_and_cleanup_instructions() {
        let mut config = BotConfig::from_env_defaults();
        config.execution.native_prefunded_hot_accounts = true;
        let wallet = Pubkey::new_unique();
        let quote_mint = pubkey_from_str(WSOL_MINT).unwrap();
        let base_mint = Pubkey::new_unique();
        let token_program = pubkey_from_str(TOKEN_PROGRAM).unwrap();

        let setup = native_setup_prefix_instructions(
            &config,
            &wallet,
            &quote_mint,
            &base_mint,
            &token_program,
            &token_program,
            1_000_000,
        )
        .unwrap();
        let cleanup =
            native_setup_suffix_instructions(&config, &wallet, &quote_mint, &token_program)
                .unwrap();

        assert!(setup.is_empty());
        assert!(cleanup.is_empty());
    }

    #[tokio::test]
    async fn strict_fresh_cached_account_rejects_missing_and_stale_accounts() {
        let sub = Arc::new(PoolSubscriptionManager::new(
            "ws://localhost:8900".to_string(),
        ));
        let missing = strict_fresh_cached_account(&sub, "missing-account", 250)
            .await
            .unwrap_err()
            .to_string();
        assert!(missing.contains("missing-account"));
        assert!(missing.contains("missing"));

        sub.cache_arc().write().await.insert(
            "stale-account".to_string(),
            crate::pool_subscription::CachedAccount {
                pubkey: "stale-account".to_string(),
                owner: SYSTEM_PROGRAM.to_string(),
                lamports: 0,
                data: vec![1, 2, 3],
                slot: 1,
                updated_at: std::time::Instant::now() - std::time::Duration::from_millis(251),
            },
        );

        let stale = strict_fresh_cached_account(&sub, "stale-account", 250)
            .await
            .unwrap_err()
            .to_string();
        assert!(stale.contains("stale-account"));
        assert!(stale.contains("stale"));
        assert!(stale.contains("ttl 250 ms"));
    }

    #[tokio::test]
    async fn strict_fresh_cached_account_accepts_fresh_account() {
        let sub = Arc::new(PoolSubscriptionManager::new(
            "ws://localhost:8900".to_string(),
        ));
        sub.cache_arc().write().await.insert(
            "fresh-account".to_string(),
            crate::pool_subscription::CachedAccount {
                pubkey: "fresh-account".to_string(),
                owner: SYSTEM_PROGRAM.to_string(),
                lamports: 5,
                data: vec![9, 8, 7],
                slot: 42,
                updated_at: std::time::Instant::now(),
            },
        );

        let account = strict_fresh_cached_account(&sub, "fresh-account", 250)
            .await
            .unwrap();
        assert_eq!(account.pubkey, "fresh-account");
        assert_eq!(account.data, vec![9, 8, 7]);
        assert_eq!(account.slot, 42);
    }

    #[tokio::test]
    async fn strict_refresh_needed_keys_dedupes_and_skips_fresh_accounts() {
        let sub = Arc::new(PoolSubscriptionManager::new(
            "ws://localhost:8900".to_string(),
        ));
        {
            let cache_arc = sub.cache_arc();
            let mut cache = cache_arc.write().await;
            cache.insert(
                "fresh-account".to_string(),
                crate::pool_subscription::CachedAccount {
                    pubkey: "fresh-account".to_string(),
                    owner: SYSTEM_PROGRAM.to_string(),
                    lamports: 1,
                    data: vec![1],
                    slot: 10,
                    updated_at: std::time::Instant::now(),
                },
            );
            cache.insert(
                "stale-account".to_string(),
                crate::pool_subscription::CachedAccount {
                    pubkey: "stale-account".to_string(),
                    owner: SYSTEM_PROGRAM.to_string(),
                    lamports: 1,
                    data: vec![2],
                    slot: 10,
                    updated_at: std::time::Instant::now() - std::time::Duration::from_millis(251),
                },
            );
        }
        let keys = vec![
            "fresh-account".to_string(),
            "stale-account".to_string(),
            "missing-account".to_string(),
            "stale-account".to_string(),
        ];

        let needed = strict_refresh_needed_keys(&sub, &keys, 250).await;

        assert_eq!(
            needed,
            vec!["stale-account".to_string(), "missing-account".to_string()]
        );
    }

    #[tokio::test]
    async fn strict_refresh_needed_keys_refreshes_accounts_near_ttl_edge() {
        let sub = Arc::new(PoolSubscriptionManager::new(
            "ws://localhost:8900".to_string(),
        ));
        {
            let cache_arc = sub.cache_arc();
            let mut cache = cache_arc.write().await;
            cache.insert(
                "fresh-account".to_string(),
                crate::pool_subscription::CachedAccount {
                    pubkey: "fresh-account".to_string(),
                    owner: SYSTEM_PROGRAM.to_string(),
                    lamports: 1,
                    data: vec![1],
                    slot: 10,
                    updated_at: std::time::Instant::now() - std::time::Duration::from_millis(50),
                },
            );
            cache.insert(
                "near-edge-account".to_string(),
                crate::pool_subscription::CachedAccount {
                    pubkey: "near-edge-account".to_string(),
                    owner: SYSTEM_PROGRAM.to_string(),
                    lamports: 1,
                    data: vec![2],
                    slot: 10,
                    updated_at: std::time::Instant::now() - std::time::Duration::from_millis(175),
                },
            );
        }
        let keys = vec!["fresh-account".to_string(), "near-edge-account".to_string()];

        let needed = strict_refresh_needed_keys(&sub, &keys, 250).await;

        assert_eq!(
            needed,
            vec!["fresh-account".to_string(), "near-edge-account".to_string()]
        );
    }

    #[tokio::test]
    async fn strict_refresh_needed_keys_refreshes_mid_life_accounts_for_tight_ttl() {
        let sub = Arc::new(PoolSubscriptionManager::new(
            "ws://localhost:8900".to_string(),
        ));
        sub.cache_arc().write().await.insert(
            "mid-life-account".to_string(),
            crate::pool_subscription::CachedAccount {
                pubkey: "mid-life-account".to_string(),
                owner: SYSTEM_PROGRAM.to_string(),
                lamports: 1,
                data: vec![1],
                slot: 10,
                updated_at: std::time::Instant::now() - std::time::Duration::from_millis(75),
            },
        );
        let keys = vec!["mid-life-account".to_string()];

        let needed = strict_refresh_needed_keys(&sub, &keys, 250).await;

        assert_eq!(needed, vec!["mid-life-account".to_string()]);
    }

    #[test]
    fn strict_refresh_request_keys_expands_to_full_candidate_set() {
        let keys = vec![
            "fresh-account".to_string(),
            "near-edge-account".to_string(),
            "fresh-account".to_string(),
            "optional-bin-array".to_string(),
        ];
        let needed = vec!["near-edge-account".to_string()];

        let refresh_keys = strict_refresh_request_keys(&keys, &needed);

        assert_eq!(
            refresh_keys,
            vec![
                "fresh-account".to_string(),
                "near-edge-account".to_string(),
                "optional-bin-array".to_string(),
            ]
        );
        assert!(strict_refresh_request_keys(&keys, &[]).is_empty());
    }

    #[test]
    fn default_hot_account_mode_keeps_setup_and_wsol_cleanup_instructions() {
        let config = BotConfig::from_env_defaults();
        let wallet = Pubkey::new_unique();
        let quote_mint = pubkey_from_str(WSOL_MINT).unwrap();
        let base_mint = Pubkey::new_unique();
        let token_program = pubkey_from_str(TOKEN_PROGRAM).unwrap();
        let associated_token_program = pubkey_from_str(ASSOCIATED_TOKEN_PROGRAM).unwrap();

        let setup = native_setup_prefix_instructions(
            &config,
            &wallet,
            &quote_mint,
            &base_mint,
            &token_program,
            &token_program,
            1_000_000,
        )
        .unwrap();
        let cleanup =
            native_setup_suffix_instructions(&config, &wallet, &quote_mint, &token_program)
                .unwrap();

        assert!(setup
            .iter()
            .any(|instruction| instruction.program_id == associated_token_program));
        assert!(setup
            .iter()
            .any(|instruction| instruction.program_id == pubkey_from_str(SYSTEM_PROGRAM).unwrap()));
        assert!(setup
            .iter()
            .any(|instruction| instruction.data.first() == Some(&17)));
        assert_eq!(cleanup.len(), 1);
        assert_eq!(cleanup[0].data, vec![9]);
    }

    fn token_account_data_with_amount(amount: u64) -> Vec<u8> {
        let mut data = vec![0; 165];
        data[64..72].copy_from_slice(&amount.to_le_bytes());
        data
    }

    const Q64: u128 = 1u128 << 64;

    fn test_dlmm_pool_data(active_id: i32, fee_bps: u16) -> Vec<u8> {
        let mut data = vec![0; swaps::dex::meteora_dlmm::LB_PAIR_MIN_LEN];
        data[..8].copy_from_slice(&swaps::dex::meteora_dlmm::LB_PAIR_DISCRIMINATOR);
        data[8..10].copy_from_slice(&fee_bps.to_le_bytes());
        data[76..80].copy_from_slice(&active_id.to_le_bytes());
        data[80..82].copy_from_slice(&1u16.to_le_bytes());
        data[82] = 0;
        data
    }

    fn test_dlmm_pool_data_with_mints(
        active_id: i32,
        fee_bps: u16,
        token_x: &Pubkey,
        token_y: &Pubkey,
    ) -> Vec<u8> {
        let mut data = test_dlmm_pool_data(active_id, fee_bps);
        write_test_address(&mut data, 0x58, token_x);
        write_test_address(&mut data, 0x78, token_y);
        data
    }

    fn test_dlmm_bin_array(index: i64, bins: &[(i32, u64, u64, u128)]) -> Vec<u8> {
        let mut data = vec![0; METEORA_DLMM_BIN_ARRAY_HEADER_LEN + 70 * METEORA_DLMM_BIN_LEN];
        data[..8].copy_from_slice(&swaps::dex::meteora_dlmm::BIN_ARRAY_DISCRIMINATOR);
        data[8..16].copy_from_slice(&index.to_le_bytes());
        for (bin_id, amount_x, amount_y, price) in bins {
            let offset = METEORA_DLMM_BIN_ARRAY_HEADER_LEN
                + ((*bin_id as i64 - index * 70) as usize) * METEORA_DLMM_BIN_LEN;
            data[offset..offset + 8].copy_from_slice(&amount_x.to_le_bytes());
            data[offset + 8..offset + 16].copy_from_slice(&amount_y.to_le_bytes());
            data[offset + 16..offset + 32].copy_from_slice(&price.to_le_bytes());
        }
        data
    }

    fn write_test_address(data: &mut [u8], offset: usize, key: &Pubkey) {
        data[offset..offset + 32].copy_from_slice(&address_from_pubkey(key).to_bytes());
    }

    fn test_native_opportunity() -> Opportunity {
        Opportunity {
            pair: crate::types::TokenPair {
                base_mint: "BASE1111111111111111111111111111111111111".to_string(),
                quote_mint: WSOL_MINT.to_string(),
                base_decimals: 6,
                quote_decimals: 9,
            },
            buy_venue: Venue::Meteora,
            sell_venue: Venue::Raydium,
            buy_pool: "11111111111111111111111111111111".to_string(),
            sell_pool: "11111111111111111111111111111111".to_string(),
            input_quote_atoms: 1_000_000,
            base_atoms: 1_000,
            output_quote_atoms: 1_010_000,
            gross_profit_quote_atoms: 10_000,
            net_profit_usd: 0.01,
            net_profit_bps: 100.0,
            spread_bps: 100.0,
            fee: crate::types::FeeBreakdown::default(),
            detected_slot: 1,
            optimizer_decision: None,
        }
    }
}
