use crate::{
    aggregators::{okx_signed_request, provider_api_key, request_params, url_with_query},
    config::{AggregatorFormat, AggregatorProviderConfig, BotConfig},
    env_runtime::env_value,
    rpc::RpcClient,
    types::{
        atoms_to_ui, Opportunity, SwapSide, TokenPair, Venue, USDC_MINT, USDT_MINT, WSOL_MINT,
    },
};
use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose, Engine as _};
use futures_util::{SinkExt, StreamExt};
use reqwest::{header::HeaderName, Client, RequestBuilder};
use rmpv::Value as MsgPackValue;
use serde::Deserialize;
use serde_json::{json, Value};
use solana_sdk::{
    address_lookup_table::state::AddressLookupTable,
    compute_budget::ComputeBudgetInstruction,
    instruction::{AccountMeta, Instruction},
    message::{AddressLookupTableAccount, VersionedMessage},
    pubkey::Pubkey,
    transaction::VersionedTransaction,
};
use std::{collections::BTreeMap, io::Cursor, str::FromStr, time::Duration};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, handshake::client::Request, Message},
};

const COMPUTE_BUDGET_PROGRAM: &str = "ComputeBudget111111111111111111111111111111";

#[derive(Debug, Clone)]
pub struct ApiAtomicPlan {
    pub instructions: Vec<Instruction>,
    pub lookup_tables: Vec<AddressLookupTableAccount>,
    pub lookup_table_addresses: Vec<String>,
    pub buy_out_atoms: u64,
    pub sell_out_atoms: u64,
    pub requote_net_profit_usd: f64,
    pub requote_net_profit_bps: f64,
    pub buy_route_labels: Vec<String>,
    pub sell_route_labels: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ApiSingleLegPlan {
    pub instructions: Vec<Instruction>,
    pub lookup_tables: Vec<AddressLookupTableAccount>,
    pub out_amount_atoms: u64,
    pub route_labels: Vec<String>,
}

#[derive(Clone)]
pub struct ApiSwapBuilder {
    config: BotConfig,
    rpc: RpcClient,
    client: Client,
}

impl ApiSwapBuilder {
    pub fn new(config: BotConfig) -> Self {
        let rpc = RpcClient::new(&config.rpc);
        Self {
            config,
            rpc,
            client: Client::new(),
        }
    }

    pub fn is_api_candidate(opportunity: &Opportunity) -> bool {
        route_provider_name(&opportunity.buy_pool).is_some()
            || route_provider_name(&opportunity.sell_pool).is_some()
            || api_venue(opportunity.buy_venue)
            || api_venue(opportunity.sell_venue)
    }

    pub async fn build_atomic_plan(
        &self,
        wallet: &Pubkey,
        opportunity: &Opportunity,
    ) -> Result<Option<ApiAtomicPlan>> {
        if !Self::is_api_candidate(opportunity) {
            return Ok(None);
        }

        let buy_provider = self
            .provider_for_leg(opportunity.buy_venue, &opportunity.buy_pool)
            .with_context(|| {
                format!(
                    "no API swap provider configured for buy leg {} {}",
                    opportunity.buy_venue, opportunity.buy_pool
                )
            })?;
        let sell_provider = self
            .provider_for_leg(opportunity.sell_venue, &opportunity.sell_pool)
            .with_context(|| {
                format!(
                    "no API swap provider configured for sell leg {} {}",
                    opportunity.sell_venue, opportunity.sell_pool
                )
            })?;

        let buy_amount: u64 = opportunity
            .input_quote_atoms
            .try_into()
            .context("API buy input amount does not fit u64")?;
        if matches!(buy_provider.format(), Some(AggregatorFormat::TitanStream))
            || matches!(sell_provider.format(), Some(AggregatorFormat::TitanStream))
        {
            let buy = self
                .build_leg(
                    wallet,
                    &buy_provider,
                    &opportunity.pair,
                    SwapSide::QuoteToBase,
                    buy_amount,
                )
                .await
                .with_context(|| format!("{} buy leg build failed", buy_provider.name()))?;
            let sell_amount = buy.min_out_atoms.max(1);
            let sell = self
                .build_leg(
                    wallet,
                    &sell_provider,
                    &opportunity.pair,
                    SwapSide::BaseToQuote,
                    sell_amount,
                )
                .await
                .with_context(|| format!("{} sell leg build failed", sell_provider.name()))?;
            let (requote_net_profit_usd, requote_net_profit_bps) =
                self.api_requote_metrics(opportunity, buy_amount, sell.min_out_atoms)?;
            self.ensure_api_requote_profit(requote_net_profit_bps)?;
            return Ok(Some(self.api_atomic_plan_from_legs(
                buy,
                sell,
                requote_net_profit_usd,
                requote_net_profit_bps,
            )));
        }

        let buy_quote = self
            .quote_leg(
                wallet,
                &buy_provider,
                &opportunity.pair,
                SwapSide::QuoteToBase,
                buy_amount,
            )
            .await
            .with_context(|| format!("{} buy leg quote failed", buy_provider.name()))?;
        let sell_amount = buy_quote.min_out_atoms.max(1);
        let sell_quote = self
            .quote_leg(
                wallet,
                &sell_provider,
                &opportunity.pair,
                SwapSide::BaseToQuote,
                sell_amount,
            )
            .await
            .with_context(|| format!("{} sell leg quote failed", sell_provider.name()))?;
        let (requote_net_profit_usd, requote_net_profit_bps) =
            self.api_requote_metrics(opportunity, buy_amount, sell_quote.min_out_atoms)?;
        self.ensure_api_requote_profit(requote_net_profit_bps)?;

        let (buy_executable, sell_executable) = tokio::join!(
            self.fetch_executable(
                &buy_provider,
                &opportunity.pair,
                SwapSide::QuoteToBase,
                buy_amount,
                &buy_quote.quote,
                wallet,
            ),
            self.fetch_executable(
                &sell_provider,
                &opportunity.pair,
                SwapSide::BaseToQuote,
                sell_amount,
                &sell_quote.quote,
                wallet,
            )
        );
        let buy_executable = buy_executable
            .with_context(|| format!("{} buy executable build failed", buy_provider.name()))?;
        let sell_executable = sell_executable
            .with_context(|| format!("{} sell executable build failed", sell_provider.name()))?;
        let buy = buy_quote.into_plan(buy_executable);
        let sell = sell_quote.into_plan(sell_executable);

        Ok(Some(self.api_atomic_plan_from_legs(
            buy,
            sell,
            requote_net_profit_usd,
            requote_net_profit_bps,
        )))
    }

    fn api_atomic_plan_from_legs(
        &self,
        buy: ApiLegPlan,
        sell: ApiLegPlan,
        requote_net_profit_usd: f64,
        requote_net_profit_bps: f64,
    ) -> ApiAtomicPlan {
        let mut instructions = api_compute_budget_instructions(&self.config);
        instructions.extend(
            buy.instructions
                .into_iter()
                .filter(|ix| !is_compute_budget_instruction(ix)),
        );
        instructions.extend(
            sell.instructions
                .into_iter()
                .filter(|ix| !is_compute_budget_instruction(ix)),
        );

        let (lookup_tables, lookup_table_addresses) =
            merge_lookup_tables(buy.lookup_tables, sell.lookup_tables);

        ApiAtomicPlan {
            instructions,
            lookup_tables,
            lookup_table_addresses,
            buy_out_atoms: buy.out_amount_atoms,
            sell_out_atoms: sell.out_amount_atoms,
            requote_net_profit_usd,
            requote_net_profit_bps,
            buy_route_labels: buy.route_labels,
            sell_route_labels: sell.route_labels,
        }
    }

    fn api_requote_metrics(
        &self,
        opportunity: &Opportunity,
        buy_amount: u64,
        sell_min_out_atoms: u64,
    ) -> Result<(f64, f64)> {
        let guaranteed_profit_atoms = sell_min_out_atoms as i128 - buy_amount as i128;
        let estimated_input_usd = quote_amount_usd(
            opportunity.input_quote_atoms,
            opportunity.pair.quote_decimals,
            &opportunity.pair.quote_mint,
            self.config.trading.sol_usd_estimate,
        )
        .ok_or_else(|| anyhow!("unsupported quote mint {}", opportunity.pair.quote_mint))?;
        let requote_net_profit_usd = quote_amount_usd_signed(
            guaranteed_profit_atoms,
            opportunity.pair.quote_decimals,
            &opportunity.pair.quote_mint,
            self.config.trading.sol_usd_estimate,
        )
        .unwrap_or(0.0)
            - estimated_priority_fee_usd(&self.config);
        let requote_net_profit_bps = if estimated_input_usd > 0.0 {
            requote_net_profit_usd / estimated_input_usd * 10_000.0
        } else {
            0.0
        };
        Ok((requote_net_profit_usd, requote_net_profit_bps))
    }

    fn ensure_api_requote_profit(&self, requote_net_profit_bps: f64) -> Result<()> {
        let min_requote_profit_bps = self
            .config
            .execution
            .min_requote_profit_bps
            .max(self.config.trading.min_net_profit_bps);
        if requote_net_profit_bps < min_requote_profit_bps {
            return Err(anyhow!(
                "API route guaranteed net profit {requote_net_profit_bps:.2} bps is below required {min_requote_profit_bps:.2} bps"
            ));
        }
        Ok(())
    }

    pub async fn build_single_leg_plan(
        &self,
        wallet: &Pubkey,
        route: &str,
        pair: &TokenPair,
        side: SwapSide,
        amount_atoms: u64,
    ) -> Result<Option<ApiSingleLegPlan>> {
        let Some(provider) = self.provider_for_leg(Venue::Custom, route) else {
            return Ok(None);
        };
        let leg = self
            .build_leg(wallet, &provider, pair, side, amount_atoms)
            .await
            .with_context(|| format!("{} route leg build failed", provider.name()))?;

        Ok(Some(ApiSingleLegPlan {
            instructions: leg.instructions,
            lookup_tables: leg.lookup_tables,
            out_amount_atoms: leg.out_amount_atoms,
            route_labels: leg.route_labels,
        }))
    }

    fn provider_for_leg(&self, venue: Venue, pool: &str) -> Option<ApiProvider> {
        if let Some(name) = route_provider_name(pool) {
            if name == "jupiter" {
                return Some(ApiProvider::Jupiter { venue_filter: None });
            }
            return self
                .config
                .aggregators
                .providers
                .iter()
                .find(|provider| provider.enabled && provider.name == name)
                .cloned()
                .map(ApiProvider::Aggregator);
        }

        if venue == Venue::Jupiter {
            return Some(ApiProvider::Jupiter { venue_filter: None });
        }
        self.config
            .aggregators
            .providers
            .iter()
            .find(|provider| provider.enabled && provider.venue == venue)
            .cloned()
            .map(ApiProvider::Aggregator)
            .or_else(|| {
                jupiter_dex_filter(venue).map(|_| ApiProvider::Jupiter {
                    venue_filter: Some(venue),
                })
            })
    }

    async fn build_leg(
        &self,
        wallet: &Pubkey,
        provider: &ApiProvider,
        pair: &TokenPair,
        side: SwapSide,
        amount_atoms: u64,
    ) -> Result<ApiLegPlan> {
        if matches!(provider.format(), Some(AggregatorFormat::TitanStream)) {
            return self
                .build_titan_leg(wallet, provider, pair, side, amount_atoms)
                .await;
        }

        let quoted = self
            .quote_leg(wallet, provider, pair, side, amount_atoms)
            .await?;
        let executable = self
            .fetch_executable(provider, pair, side, amount_atoms, &quoted.quote, wallet)
            .await?;

        Ok(quoted.into_plan(executable))
    }

    async fn quote_leg(
        &self,
        wallet: &Pubkey,
        provider: &ApiProvider,
        pair: &TokenPair,
        side: SwapSide,
        amount_atoms: u64,
    ) -> Result<ApiLegQuote> {
        let quote = self
            .fetch_quote(provider, pair, side, amount_atoms, Some(wallet))
            .await?;
        let out_amount_atoms = parse_out_amount(&quote)
            .with_context(|| format!("{} quote missing output amount", provider.name()))?;
        let min_out_atoms = parse_min_out_amount(&quote).unwrap_or_else(|| {
            slippage_floor(
                out_amount_atoms,
                u64::from(self.config.jupiter.slippage_bps)
                    + self.config.trading.fee_safety_buffer_bps.ceil().max(0.0) as u64,
            )
            .max(1)
        });
        let route_labels = route_labels(provider.name(), &quote);
        Ok(ApiLegQuote {
            quote,
            out_amount_atoms,
            min_out_atoms,
            route_labels,
        })
    }

    async fn fetch_quote(
        &self,
        provider: &ApiProvider,
        pair: &TokenPair,
        side: SwapSide,
        amount_atoms: u64,
        wallet: Option<&Pubkey>,
    ) -> Result<Value> {
        match provider {
            ApiProvider::Jupiter { venue_filter } => {
                let params = jupiter_params(pair, side, amount_atoms, &self.config, *venue_filter);
                let request = self
                    .client
                    .get(&self.config.jupiter.quote_url)
                    .query(&params)
                    .timeout(Duration::from_millis(self.config.jupiter.timeout_ms));
                let request = with_env_api_key(request, "JUPITER_API_KEY", "x-api-key", None)?;
                send_json(request, "jupiter quote").await
            }
            ApiProvider::Aggregator(config) => {
                ensure_provider_auth(config)?;
                let mut params = request_params(config.format, pair, side, amount_atoms as u128);
                if matches!(
                    config.format,
                    AggregatorFormat::CarbiumQ1 | AggregatorFormat::SpiderSwap
                ) {
                    if let Some(wallet) = wallet {
                        params.push(("user_account".to_string(), wallet.to_string()));
                    }
                }
                if matches!(config.format, AggregatorFormat::Dflow) {
                    add_dflow_order_execution_params(
                        &mut params,
                        wallet,
                        self.config.fees.max_priority_fee_lamports,
                        self.config.execution.compute_unit_price_micro_lamports,
                    );
                }
                let request = if matches!(config.format, AggregatorFormat::Vybe) {
                    provider_get_json_request(
                        &self.client,
                        config,
                        &config.quote_url,
                        &vybe_quote_body(
                            pair,
                            side,
                            amount_atoms,
                            wallet,
                            self.config.jupiter.slippage_bps,
                        ),
                        config.timeout_ms,
                    )?
                } else if matches!(config.format, AggregatorFormat::Venum) {
                    provider_post_json_request(
                        &self.client,
                        config,
                        &config.quote_url,
                        &venum_quote_body(pair, side, amount_atoms),
                        config.timeout_ms,
                    )?
                } else {
                    provider_get_request(
                        &self.client,
                        config,
                        &config.quote_url,
                        &params,
                        config.timeout_ms,
                    )?
                };
                send_json(request, &format!("{} quote", config.name)).await
            }
        }
    }

    async fn fetch_executable(
        &self,
        provider: &ApiProvider,
        pair: &TokenPair,
        side: SwapSide,
        amount_atoms: u64,
        quote: &Value,
        wallet: &Pubkey,
    ) -> Result<ExecutablePayload> {
        let value = match provider {
            ApiProvider::Jupiter { .. } => {
                let mut request = self
                    .client
                    .post(&self.config.execution.jupiter_swap_instructions_url)
                    .json(&json!({
                        "quoteResponse": quote,
                        "userPublicKey": wallet.to_string(),
                        "payer": wallet.to_string(),
                        "wrapAndUnwrapSol": true,
                        "useSharedAccounts": true,
                        "asLegacyTransaction": false,
                        "dynamicComputeUnitLimit": false,
                        "skipUserAccountsRpcCalls": false
                    }));
                request = with_env_api_key(request, "JUPITER_API_KEY", "x-api-key", None)?;
                send_json(request, "jupiter swap-instructions").await?
            }
            ApiProvider::Aggregator(config) => match config.format {
                AggregatorFormat::RaydiumTradeApi => {
                    let swap_url = provider_swap_url(config);
                    let body = json!({
                        "swapResponse": quote,
                        "wallet": wallet.to_string(),
                        "txVersion": "V0",
                        "wrapSol": input_mint_for_side(pair, side) == WSOL_MINT,
                        "unwrapSol": output_mint_for_side(pair, side) == WSOL_MINT,
                        "computeUnitPriceMicroLamports": compute_unit_price_micro_lamports(&self.config).to_string()
                    });
                    let request = self
                        .client
                        .post(&swap_url)
                        .json(&body)
                        .timeout(Duration::from_millis(config.timeout_ms));
                    send_json(request, "raydium transaction").await?
                }
                AggregatorFormat::CarbiumQ1 | AggregatorFormat::SpiderSwap => {
                    if has_transaction_payload(quote) {
                        quote.clone()
                    } else {
                        let mut params =
                            request_params(config.format, pair, side, amount_atoms as u128);
                        params.push(("user_account".to_string(), wallet.to_string()));
                        let request = provider_get_request(
                            &self.client,
                            config,
                            &config.quote_url,
                            &params,
                            config.timeout_ms,
                        )?;
                        send_json(request, &format!("{} executable quote", config.name)).await?
                    }
                }
                AggregatorFormat::Okx => {
                    let swap_url = provider_swap_url(config);
                    let mut params =
                        request_params(config.format, pair, side, amount_atoms as u128);
                    params.push(("userWalletAddress".to_string(), wallet.to_string()));
                    params.push(("useTokenLedger".to_string(), "true".to_string()));
                    let request = provider_get_request(
                        &self.client,
                        config,
                        &swap_url,
                        &params,
                        config.timeout_ms,
                    )?;
                    send_json(request, "okx swap-instruction").await?
                }
                AggregatorFormat::Dflow => {
                    let swap_url = provider_swap_url(config);
                    if has_transaction_payload(quote) {
                        quote.clone()
                    } else {
                        let mut params =
                            request_params(config.format, pair, side, amount_atoms as u128);
                        add_dflow_order_execution_params(
                            &mut params,
                            Some(wallet),
                            self.config.fees.max_priority_fee_lamports,
                            self.config.execution.compute_unit_price_micro_lamports,
                        );
                        let request = provider_get_request(
                            &self.client,
                            config,
                            &swap_url,
                            &params,
                            config.timeout_ms,
                        )?;
                        send_json(request, "dflow order").await?
                    }
                }
                AggregatorFormat::OpenOcean => {
                    let swap_url = provider_swap_url(config);
                    let mut params =
                        request_params(config.format, pair, side, amount_atoms as u128);
                    params.push(("account".to_string(), wallet.to_string()));
                    let request = provider_get_request(
                        &self.client,
                        config,
                        &swap_url,
                        &params,
                        config.timeout_ms,
                    )?;
                    send_json(request, "openocean swap").await?
                }
                AggregatorFormat::Venum => {
                    let swap_url = provider_swap_url(config);
                    let body = json!({
                        "inputMint": input_mint_for_side(pair, side),
                        "outputMint": output_mint_for_side(pair, side),
                        "amount": amount_atoms.to_string(),
                        "slippageBps": self.config.jupiter.slippage_bps,
                        "userPublicKey": wallet.to_string()
                    });
                    let request = provider_post_json_request(
                        &self.client,
                        config,
                        &swap_url,
                        &body,
                        config.timeout_ms,
                    )?;
                    send_json(request, "venum swap build").await?
                }
                AggregatorFormat::Vybe => {
                    let swap_url = provider_swap_url(config);
                    let body = vybe_swap_body(
                        pair,
                        side,
                        amount_atoms,
                        wallet,
                        self.config.jupiter.slippage_bps,
                    );
                    let request = provider_post_json_request(
                        &self.client,
                        config,
                        &swap_url,
                        &body,
                        config.timeout_ms,
                    )?;
                    send_json(request, "vybe swap build").await?
                }
                AggregatorFormat::Generic => {
                    let swap_url = provider_swap_url(config);
                    let body = json!({
                        "quoteResponse": quote,
                        "inputMint": input_mint_for_side(pair, side),
                        "outputMint": output_mint_for_side(pair, side),
                        "amount": amount_atoms.to_string(),
                        "slippageBps": self.config.jupiter.slippage_bps,
                        "userPublicKey": wallet.to_string(),
                        "wallet": wallet.to_string()
                    });
                    let request = provider_post_json_request(
                        &self.client,
                        config,
                        &swap_url,
                        &body,
                        config.timeout_ms,
                    )?;
                    send_json(request, &format!("{} swap build", config.name)).await?
                }
                AggregatorFormat::Jupiter | AggregatorFormat::TitanStream => {
                    return Err(anyhow!(
                        "{} is handled by a dedicated executor",
                        config.name
                    ));
                }
            },
        };

        self.executable_from_json(&value).await
    }

    async fn executable_from_json(&self, value: &Value) -> Result<ExecutablePayload> {
        if let Some(instructions) = instructions_from_json(value)? {
            let lookup_table_addresses = lookup_table_addresses_from_json(value);
            let lookup_tables = self.fetch_lookup_tables(&lookup_table_addresses).await?;
            return Ok(ExecutablePayload {
                instructions,
                lookup_tables,
            });
        }

        let tx = transaction_payload_from_json(value)
            .ok_or_else(|| anyhow!("response did not contain instructions or a transaction"))?;
        self.executable_from_transaction_bytes(&tx).await
    }

    async fn executable_from_transaction_bytes(&self, tx: &[u8]) -> Result<ExecutablePayload> {
        let transaction: VersionedTransaction =
            bincode::deserialize(tx).context("failed to deserialize provider transaction")?;
        let (account_keys, lookup_tables) = self.resolve_message_keys(&transaction.message).await?;
        let instructions = match &transaction.message {
            VersionedMessage::Legacy(message) => message
                .instructions
                .iter()
                .map(|compiled| {
                    instruction_from_compiled_message(
                        &transaction.message,
                        &account_keys,
                        compiled.program_id_index,
                        &compiled.accounts,
                        &compiled.data,
                    )
                })
                .collect::<Result<Vec<_>>>()?,
            VersionedMessage::V0(message) => message
                .instructions
                .iter()
                .map(|compiled| {
                    instruction_from_compiled_message(
                        &transaction.message,
                        &account_keys,
                        compiled.program_id_index,
                        &compiled.accounts,
                        &compiled.data,
                    )
                })
                .collect::<Result<Vec<_>>>()?,
        };
        Ok(ExecutablePayload {
            instructions,
            lookup_tables,
        })
    }

    async fn resolve_message_keys(
        &self,
        message: &VersionedMessage,
    ) -> Result<(Vec<Pubkey>, Vec<AddressLookupTableAccount>)> {
        let mut keys = message.static_account_keys().to_vec();
        let Some(lookups) = message.address_table_lookups() else {
            return Ok((keys, Vec::new()));
        };
        let lookup_addresses = lookups
            .iter()
            .map(|lookup| lookup.account_key.to_string())
            .collect::<Vec<_>>();
        let lookup_tables = self.fetch_lookup_tables(&lookup_addresses).await?;
        for (lookup, table) in lookups.iter().zip(lookup_tables.iter()) {
            for index in &lookup.writable_indexes {
                keys.push(
                    *table
                        .addresses
                        .get(usize::from(*index))
                        .ok_or_else(|| anyhow!("lookup table writable index out of range"))?,
                );
            }
            for index in &lookup.readonly_indexes {
                keys.push(
                    *table
                        .addresses
                        .get(usize::from(*index))
                        .ok_or_else(|| anyhow!("lookup table readonly index out of range"))?,
                );
            }
        }
        Ok((keys, lookup_tables))
    }

    async fn fetch_lookup_tables(
        &self,
        addresses: &[String],
    ) -> Result<Vec<AddressLookupTableAccount>> {
        let mut unique = addresses.to_vec();
        unique.sort();
        unique.dedup();
        if unique.is_empty() {
            return Ok(Vec::new());
        }
        let accounts = self.rpc.get_multiple_accounts(&unique).await?;
        unique
            .into_iter()
            .zip(accounts)
            .map(|(address, account)| {
                let account = account
                    .ok_or_else(|| anyhow!("address lookup table account missing: {address}"))?;
                let key = Pubkey::from_str(&address)
                    .with_context(|| format!("invalid address lookup table key {address}"))?;
                let table = AddressLookupTable::deserialize(&account.data).map_err(|error| {
                    anyhow!("invalid address lookup table {address}: {error:?}")
                })?;
                Ok(AddressLookupTableAccount {
                    key,
                    addresses: table.addresses.to_vec(),
                })
            })
            .collect()
    }

    async fn build_titan_leg(
        &self,
        wallet: &Pubkey,
        provider: &ApiProvider,
        pair: &TokenPair,
        side: SwapSide,
        amount_atoms: u64,
    ) -> Result<ApiLegPlan> {
        let ApiProvider::Aggregator(config) = provider else {
            return Err(anyhow!("titan provider was not an aggregator config"));
        };
        ensure_provider_auth(config)?;
        let api_key =
            provider_api_key_header_value(config).context("TITAN_API_TOKEN is required")?;
        let mut request = config
            .quote_url
            .as_str()
            .into_client_request()
            .context("invalid Titan websocket URL")?;
        let header = HeaderName::from_bytes(
            config
                .api_key_header
                .as_deref()
                .unwrap_or("Authorization")
                .as_bytes(),
        )
        .context("invalid Titan auth header")?;
        request.headers_mut().insert(
            header,
            api_key.parse().context("invalid Titan auth header value")?,
        );
        apply_titan_ws_protocol(&mut request)?;

        let (mut ws, _) = connect_async(request)
            .await
            .context("Titan websocket connect failed")?;
        let payload = titan_quote_request(wallet, pair, side, amount_atoms, &self.config)?;
        let mut bytes = Vec::new();
        rmpv::encode::write_value(&mut bytes, &payload)
            .context("failed to encode Titan request")?;
        ws.send(Message::Binary(bytes.into()))
            .await
            .context("Titan websocket send failed")?;

        let started = tokio::time::Instant::now();
        let timeout = Duration::from_millis(config.timeout_ms.max(1_000));
        while started.elapsed() < timeout {
            let remaining = timeout.saturating_sub(started.elapsed());
            let Some(message) = tokio::time::timeout(remaining, ws.next())
                .await
                .context("Titan quote timed out")?
            else {
                break;
            };
            let message = message.context("Titan websocket message failed")?;
            let Message::Binary(data) = message else {
                continue;
            };
            let value = rmpv::decode::read_value(&mut Cursor::new(data))
                .context("failed to decode Titan MessagePack response")?;
            if let Some(route) = self.titan_route_from_message(&value).await? {
                return Ok(ApiLegPlan {
                    instructions: route.payload.instructions,
                    lookup_tables: route.payload.lookup_tables,
                    out_amount_atoms: route.out_amount_atoms,
                    min_out_atoms: route.min_out_atoms,
                    route_labels: route.route_labels,
                });
            }
        }

        Err(anyhow!(
            "Titan did not return an executable route before timeout"
        ))
    }

    async fn titan_route_from_message(
        &self,
        value: &MsgPackValue,
    ) -> Result<Option<TitanRoutePayload>> {
        let Some(route) = find_best_titan_route(value) else {
            return Ok(None);
        };
        let out_amount_atoms = mp_map_u64(route, "outAmount").unwrap_or_default();
        if out_amount_atoms == 0 {
            return Ok(None);
        }
        let slippage_bps = mp_map_u64(route, "slippageBps").unwrap_or(0);
        let min_out_atoms = slippage_floor(out_amount_atoms, slippage_bps).max(1);
        let route_labels = titan_route_labels(route);
        if let Some(instructions) = titan_instructions(route)? {
            let lookup_table_addresses = titan_lookup_tables(route);
            let lookup_tables = self.fetch_lookup_tables(&lookup_table_addresses).await?;
            return Ok(Some(TitanRoutePayload {
                payload: ExecutablePayload {
                    instructions,
                    lookup_tables,
                },
                out_amount_atoms,
                min_out_atoms,
                route_labels,
            }));
        }
        if let Some(transaction) = titan_transaction(route) {
            let payload = self.executable_from_transaction_bytes(&transaction).await?;
            return Ok(Some(TitanRoutePayload {
                payload,
                out_amount_atoms,
                min_out_atoms,
                route_labels,
            }));
        }
        Ok(None)
    }
}

#[derive(Debug, Clone)]
enum ApiProvider {
    Jupiter { venue_filter: Option<Venue> },
    Aggregator(AggregatorProviderConfig),
}

impl ApiProvider {
    fn name(&self) -> &str {
        match self {
            ApiProvider::Jupiter { .. } => "jupiter",
            ApiProvider::Aggregator(provider) => &provider.name,
        }
    }

    fn format(&self) -> Option<AggregatorFormat> {
        match self {
            ApiProvider::Jupiter { .. } => None,
            ApiProvider::Aggregator(provider) => Some(provider.format),
        }
    }
}

struct ApiLegPlan {
    instructions: Vec<Instruction>,
    lookup_tables: Vec<AddressLookupTableAccount>,
    out_amount_atoms: u64,
    min_out_atoms: u64,
    route_labels: Vec<String>,
}

struct ApiLegQuote {
    quote: Value,
    out_amount_atoms: u64,
    min_out_atoms: u64,
    route_labels: Vec<String>,
}

impl ApiLegQuote {
    fn into_plan(self, executable: ExecutablePayload) -> ApiLegPlan {
        ApiLegPlan {
            instructions: executable.instructions,
            lookup_tables: executable.lookup_tables,
            out_amount_atoms: self.out_amount_atoms,
            min_out_atoms: self.min_out_atoms,
            route_labels: self.route_labels,
        }
    }
}

struct ExecutablePayload {
    instructions: Vec<Instruction>,
    lookup_tables: Vec<AddressLookupTableAccount>,
}

struct TitanRoutePayload {
    payload: ExecutablePayload,
    out_amount_atoms: u64,
    min_out_atoms: u64,
    route_labels: Vec<String>,
}

fn api_venue(venue: Venue) -> bool {
    matches!(
        venue,
        Venue::Jupiter
            | Venue::Dflow
            | Venue::Titan
            | Venue::Okx
            | Venue::SpiderSwap
            | Venue::OpenOcean
            | Venue::Vybe
            | Venue::Venum
    )
}

pub fn route_provider_name(pool: &str) -> Option<&str> {
    pool.split_once("-route:").map(|(name, _)| name)
}

fn ensure_provider_auth(config: &AggregatorProviderConfig) -> Result<()> {
    if !config.requires_api_key {
        return Ok(());
    }
    provider_api_key(config).with_context(|| {
        format!(
            "{} requires {}",
            config.name,
            config.api_key_env.as_deref().unwrap_or("an API key")
        )
    })?;
    if matches!(config.format, AggregatorFormat::Okx) {
        env_value("OKX_API_SECRET")
            .or_else(|| env_value("OKX_SECRET_KEY"))
            .context("OKX_API_SECRET or OKX_SECRET_KEY is required for OKX DEX")?;
        env_value("OKX_PASSPHRASE").context("OKX_PASSPHRASE is required")?;
        env_value("OKX_PROJECT_ID").context("OKX_PROJECT_ID is required")?;
    }
    Ok(())
}

fn jupiter_params(
    pair: &TokenPair,
    side: SwapSide,
    amount_atoms: u64,
    config: &BotConfig,
    venue_filter: Option<Venue>,
) -> Vec<(String, String)> {
    let mut params = vec![
        (
            "inputMint".to_string(),
            input_mint_for_side(pair, side).to_string(),
        ),
        (
            "outputMint".to_string(),
            output_mint_for_side(pair, side).to_string(),
        ),
        ("amount".to_string(), amount_atoms.to_string()),
        (
            "slippageBps".to_string(),
            config.jupiter.slippage_bps.to_string(),
        ),
        ("swapMode".to_string(), "ExactIn".to_string()),
        (
            "onlyDirectRoutes".to_string(),
            config.execution.jupiter_only_direct_routes.to_string(),
        ),
        (
            "maxAccounts".to_string(),
            config.execution.jupiter_max_accounts.to_string(),
        ),
    ];
    if config.execution.jupiter_use_venue_filters {
        if let Some(dexes) = venue_filter.and_then(jupiter_dex_filter) {
            params.push(("dexes".to_string(), dexes.to_string()));
        }
    }
    params.push(("asLegacyTransaction".to_string(), "false".to_string()));
    params
}

fn jupiter_dex_filter(venue: Venue) -> Option<&'static str> {
    match venue {
        Venue::Orca => Some("Orca V1,Orca V2,Whirlpool"),
        Venue::Meteora => Some("Meteora,Meteora DLMM,Meteora DAMM v2"),
        Venue::Raydium => Some("Raydium,Raydium CLMM,Raydium CP"),
        _ => None,
    }
}

fn input_mint_for_side(pair: &TokenPair, side: SwapSide) -> &str {
    match side {
        SwapSide::QuoteToBase => &pair.quote_mint,
        SwapSide::BaseToQuote => &pair.base_mint,
    }
}

fn output_mint_for_side(pair: &TokenPair, side: SwapSide) -> &str {
    match side {
        SwapSide::QuoteToBase => &pair.base_mint,
        SwapSide::BaseToQuote => &pair.quote_mint,
    }
}

fn input_decimals_for_side(pair: &TokenPair, side: SwapSide) -> u8 {
    match side {
        SwapSide::QuoteToBase => pair.quote_decimals,
        SwapSide::BaseToQuote => pair.base_decimals,
    }
}

fn vybe_swap_body(
    pair: &TokenPair,
    side: SwapSide,
    amount_atoms: u64,
    wallet: &Pubkey,
    slippage_bps: u16,
) -> Value {
    let input_ui = atoms_to_ui(
        u128::from(amount_atoms),
        input_decimals_for_side(pair, side),
    );
    let slippage_percent = f64::from(slippage_bps) / 100.0;
    let router = env_value("VYBE_ROUTER").unwrap_or_else(|| "vybe".to_string());
    let mut body = json!({
        "wallet": wallet.to_string(),
        "amount": input_ui,
        "inputMint": input_mint_for_side(pair, side),
        "outputMint": output_mint_for_side(pair, side),
        "router": router,
        "slippage": slippage_percent,
    });

    if env_truthy("VYBE_ONLY") {
        body["vybeOnly"] = Value::Bool(true);
    }
    if env_truthy("VYBE_PRIVY") {
        body["privy"] = Value::Bool(true);
    }
    body
}

fn vybe_quote_body(
    pair: &TokenPair,
    side: SwapSide,
    amount_atoms: u64,
    wallet: Option<&Pubkey>,
    slippage_bps: u16,
) -> Value {
    let input_ui = atoms_to_ui(
        u128::from(amount_atoms),
        input_decimals_for_side(pair, side),
    );
    let slippage_percent = f64::from(slippage_bps) / 100.0;
    let mut body = json!({
        "amount": input_ui,
        "inputMint": input_mint_for_side(pair, side),
        "outputMint": output_mint_for_side(pair, side),
        "slippage": slippage_percent,
    });
    if let Some(wallet) = wallet {
        body["wallet"] = Value::String(wallet.to_string());
    }
    body
}

fn venum_quote_body(pair: &TokenPair, side: SwapSide, amount_atoms: u64) -> Value {
    json!({
        "inputMint": input_mint_for_side(pair, side),
        "outputMint": output_mint_for_side(pair, side),
        "amount": amount_atoms.to_string(),
        "slippageBps": 50,
    })
}

fn env_truthy(name: &str) -> bool {
    env_value(name)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

fn provider_get_request(
    client: &Client,
    config: &AggregatorProviderConfig,
    url: &str,
    params: &[(String, String)],
    timeout_ms: u64,
) -> Result<RequestBuilder> {
    if matches!(config.format, AggregatorFormat::Okx) {
        let url = url_with_query(url, params)?;
        okx_signed_request(
            client
                .get(url.clone())
                .timeout(Duration::from_millis(timeout_ms)),
            &url,
            provider_api_key(config).context("OKX_API_KEY is required")?,
        )
    } else {
        let request = client
            .get(url)
            .query(params)
            .timeout(Duration::from_millis(timeout_ms));
        with_provider_api_key(request, config)
    }
}

fn provider_get_json_request(
    client: &Client,
    config: &AggregatorProviderConfig,
    url: &str,
    body: &Value,
    timeout_ms: u64,
) -> Result<RequestBuilder> {
    let request = client
        .get(url)
        .json(body)
        .timeout(Duration::from_millis(timeout_ms));
    with_provider_api_key(request, config)
}

fn provider_post_json_request(
    client: &Client,
    config: &AggregatorProviderConfig,
    url: &str,
    body: &Value,
    timeout_ms: u64,
) -> Result<RequestBuilder> {
    let request = client
        .post(url)
        .json(body)
        .timeout(Duration::from_millis(timeout_ms));
    with_provider_api_key(request, config)
}

fn with_provider_api_key(
    request: RequestBuilder,
    config: &AggregatorProviderConfig,
) -> Result<RequestBuilder> {
    let Some(value) = provider_api_key_header_value(config) else {
        return Ok(request);
    };
    let Some(header) = config.api_key_header.as_ref() else {
        return Ok(request);
    };
    let name = HeaderName::from_bytes(header.as_bytes())
        .with_context(|| format!("invalid header name for {}", config.name))?;
    Ok(request.header(name, value))
}

fn provider_api_key_header_value(config: &AggregatorProviderConfig) -> Option<String> {
    provider_api_key(config).map(|key| {
        format!(
            "{}{key}",
            config.api_key_prefix.as_deref().unwrap_or_default()
        )
    })
}

fn with_env_api_key(
    request: RequestBuilder,
    env_name: &str,
    header_name: &str,
    prefix: Option<&str>,
) -> Result<RequestBuilder> {
    let Some(value) = env_value(env_name) else {
        return Ok(request);
    };
    let header = HeaderName::from_bytes(header_name.as_bytes())?;
    Ok(request.header(header, format!("{}{}", prefix.unwrap_or_default(), value)))
}

async fn send_json(request: RequestBuilder, label: &str) -> Result<Value> {
    let response = request
        .send()
        .await
        .with_context(|| format!("{label} request failed"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .with_context(|| format!("{label} response body failed"))?;
    if !status.is_success() {
        return Err(anyhow!("{label} http {status}: {text}"));
    }
    serde_json::from_str(&text).with_context(|| format!("invalid {label} json: {text}"))
}

fn provider_swap_url(config: &AggregatorProviderConfig) -> String {
    let upper = config.name.to_ascii_uppercase();
    let env_name = format!("{}_SWAP_URL", upper);
    if let Some(value) = env_value(&env_name).or_else(|| match config.name.as_str() {
        "spiderswap" => env_value("CARBIUM_SWAP_URL"),
        "okx" => {
            env_value("OKX_DEX_SWAP_INSTRUCTIONS_URL").or_else(|| env_value("OKX_DEX_SWAP_URL"))
        }
        _ => None,
    }) {
        return value;
    }

    match config.format {
        AggregatorFormat::RaydiumTradeApi => config
            .quote_url
            .replace("/compute/swap-base-in", "/transaction/swap-base-in")
            .replace("/compute/swap-base-out", "/transaction/swap-base-out"),
        AggregatorFormat::CarbiumQ1 | AggregatorFormat::SpiderSwap => config.quote_url.clone(),
        AggregatorFormat::Okx => config
            .quote_url
            .replace(
                "/api/v6/dex/aggregator/quote",
                "/api/v6/dex/aggregator/swap-instruction",
            )
            .replace(
                "/api/v5/dex/aggregator/quote",
                "/api/v5/dex/aggregator/swap-instruction",
            ),
        AggregatorFormat::OpenOcean => config.quote_url.replace("/quote", "/swap"),
        AggregatorFormat::Dflow => dflow_order_url(&config.quote_url),
        AggregatorFormat::Venum => config.quote_url.replace("/v1/quote", "/v1/swap/build"),
        AggregatorFormat::Vybe => config.quote_url.replace("swap-quote", "swap"),
        AggregatorFormat::Generic => config.quote_url.replace("quote", "swap"),
        AggregatorFormat::Jupiter | AggregatorFormat::TitanStream => config.quote_url.clone(),
    }
}

fn dflow_order_url(url: &str) -> String {
    if let Some(base) = url.strip_suffix("/swap-instructions") {
        format!("{base}/order")
    } else if let Some(base) = url.strip_suffix("/quote") {
        format!("{base}/order")
    } else {
        url.to_string()
    }
}

fn add_dflow_order_execution_params(
    params: &mut Vec<(String, String)>,
    wallet: Option<&Pubkey>,
    max_priority_fee_lamports: u64,
    compute_unit_price_micro_lamports: u64,
) {
    if let Some(wallet) = wallet {
        params.push(("userPublicKey".to_string(), wallet.to_string()));
    }
    params.push(("wrapAndUnwrapSol".to_string(), "true".to_string()));
    params.push(("perLegSlippage".to_string(), "true".to_string()));
    params.push(("allowSyncExec".to_string(), "true".to_string()));
    params.push(("allowAsyncExec".to_string(), "false".to_string()));
    params.push(("dynamicComputeUnitLimit".to_string(), "false".to_string()));
    params.push((
        "prioritizationFeeMaxLamports".to_string(),
        max_priority_fee_lamports.to_string(),
    ));
    if compute_unit_price_micro_lamports > 0 {
        params.push((
            "computeUnitPriceMicroLamports".to_string(),
            compute_unit_price_micro_lamports.to_string(),
        ));
    } else {
        params.push(("prioritizationFeeLamports".to_string(), "auto".to_string()));
    }
}

fn apply_titan_ws_protocol(request: &mut Request) -> Result<()> {
    let protocol = titan_ws_protocol();
    if protocol.trim().is_empty() {
        return Ok(());
    }
    request.headers_mut().insert(
        HeaderName::from_static("sec-websocket-protocol"),
        protocol
            .parse()
            .context("invalid Titan websocket protocol header value")?,
    );
    Ok(())
}

fn titan_ws_protocol() -> String {
    env_value("TITAN_WS_PROTOCOL").unwrap_or_else(|| "v1.api.titan.ag".to_string())
}

fn has_transaction_payload(value: &Value) -> bool {
    transaction_payload_from_json(value).is_some()
}

fn parse_out_amount(value: &Value) -> Option<u64> {
    first_u128(
        value,
        &[
            "/outAmount",
            "/data/outAmount",
            "/data/outputAmount",
            "/data/0/outAmount",
            "/data/0/toTokenAmount",
            "/outputAmount",
            "/destAmountOut",
            "/amountOut",
            "/quote/outAmount",
            "/data/quote/outAmount",
            "/data/0/routerResult/toTokenAmount",
            "/data/dexes/0/swapAmount",
            "/result/outAmount",
            "/estimatedOutput",
            "/bestRoute/outputAmount",
            "/bestRoute/outAmount",
            "/routes/0/outputAmount",
            "/routes/0/outAmount",
        ],
    )
    .and_then(|value| value.try_into().ok())
}

fn parse_min_out_amount(value: &Value) -> Option<u64> {
    first_u128(
        value,
        &[
            "/otherAmountThreshold",
            "/minOutAmount",
            "/minOutput",
            "/destAmountOutMin",
            "/data/minReceiveAmount",
            "/data/0/minReceiveAmount",
            "/data/0/routerResult/minReceiveAmount",
            "/data/0/toTokenAmountMin",
            "/bestRoute/minOutputAmount",
            "/bestRoute/minOutAmount",
            "/routes/0/minOutputAmount",
            "/routes/0/minOutAmount",
        ],
    )
    .and_then(|value| value.try_into().ok())
}

fn route_labels(provider_name: &str, value: &Value) -> Vec<String> {
    let mut labels = Vec::new();
    collect_route_labels(value, &mut labels);
    labels.sort();
    labels.dedup();
    if labels.is_empty() {
        vec![provider_name.to_string()]
    } else {
        labels
            .into_iter()
            .map(|label| format!("{provider_name}:{label}"))
            .collect()
    }
}

fn collect_route_labels(value: &Value, labels: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for key in ["label", "venue", "dex", "dexName", "swap", "protocol"] {
                if let Some(label) = map.get(key).and_then(Value::as_str) {
                    labels.push(label.to_string());
                }
            }
            for child in map.values() {
                collect_route_labels(child, labels);
            }
        }
        Value::Array(values) => {
            for child in values {
                collect_route_labels(child, labels);
            }
        }
        _ => {}
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiInstruction {
    #[serde(alias = "program_id")]
    program_id: String,
    #[serde(default)]
    accounts: Vec<ApiAccountMeta>,
    #[serde(default)]
    data: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiAccountMeta {
    #[serde(alias = "pubKey")]
    pubkey: String,
    #[serde(default, alias = "is_signer")]
    is_signer: bool,
    #[serde(default, alias = "is_writable")]
    is_writable: bool,
}

fn instructions_from_json(value: &Value) -> Result<Option<Vec<Instruction>>> {
    let mut instructions = Vec::new();

    for path in [
        "/instructionLists",
        "/data/instructionLists",
        "/data/0/instructionLists",
        "/instructions",
        "/data/instructions",
        "/data/0/instructions",
    ] {
        if let Some(values) = value.pointer(path).and_then(Value::as_array) {
            append_json_instruction_array(&mut instructions, values)?;
        }
    }

    for path in [
        "/otherInstructions",
        "/data/otherInstructions",
        "/data/0/otherInstructions",
        "/setupInstructions",
        "/data/setupInstructions",
        "/data/0/setupInstructions",
        "/computeBudgetInstructions",
        "/data/computeBudgetInstructions",
        "/data/0/computeBudgetInstructions",
    ] {
        if let Some(values) = value.pointer(path).and_then(Value::as_array) {
            append_json_instruction_array(&mut instructions, values)?;
        }
    }

    for path in [
        "/swapInstruction",
        "/data/swapInstruction",
        "/data/0/swapInstruction",
    ] {
        if let Some(value) = value.pointer(path) {
            instructions.push(json_instruction(value.clone())?);
        }
    }

    for path in [
        "/cleanupInstructions",
        "/data/cleanupInstructions",
        "/data/0/cleanupInstructions",
    ] {
        if let Some(values) = value.pointer(path).and_then(Value::as_array) {
            append_json_instruction_array(&mut instructions, values)?;
        }
    }

    Ok((!instructions.is_empty()).then_some(instructions))
}

fn append_json_instruction_array(
    instructions: &mut Vec<Instruction>,
    values: &[Value],
) -> Result<()> {
    for value in values {
        instructions.push(json_instruction(value.clone())?);
    }
    Ok(())
}

fn json_instruction(value: Value) -> Result<Instruction> {
    let value: ApiInstruction = serde_json::from_value(value).context("invalid API instruction")?;
    let program_id = Pubkey::from_str(&value.program_id)
        .with_context(|| format!("invalid instruction program id {}", value.program_id))?;
    let accounts = value
        .accounts
        .into_iter()
        .map(|account| {
            let pubkey = Pubkey::from_str(&account.pubkey)
                .with_context(|| format!("invalid instruction account {}", account.pubkey))?;
            Ok(if account.is_writable {
                AccountMeta::new(pubkey, account.is_signer)
            } else {
                AccountMeta::new_readonly(pubkey, account.is_signer)
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Instruction {
        program_id,
        accounts,
        data: decode_payload_string(&value.data)?,
    })
}

fn lookup_table_addresses_from_json(value: &Value) -> Vec<String> {
    let mut addresses = Vec::new();
    for path in [
        "/addressLookupTableAddresses",
        "/addressLookupTableAccount",
        "/addressLookupTables",
        "/data/addressLookupTableAddresses",
        "/data/addressLookupTableAccount",
        "/data/addressLookupTables",
        "/data/0/addressLookupTableAddresses",
        "/data/0/addressLookupTableAccount",
        "/data/0/addressLookupTables",
    ] {
        if let Some(values) = value.pointer(path).and_then(Value::as_array) {
            addresses.extend(values.iter().filter_map(lookup_table_address_value));
        }
    }
    addresses.sort();
    addresses.dedup();
    addresses
}

fn lookup_table_address_value(value: &Value) -> Option<String> {
    if let Some(address) = value.as_str() {
        return Some(address.to_owned());
    }
    for key in [
        "address",
        "pubkey",
        "pubKey",
        "key",
        "lookupTableAddress",
        "lookupTable",
    ] {
        if let Some(address) = value.get(key).and_then(Value::as_str) {
            return Some(address.to_owned());
        }
    }
    None
}

fn transaction_payload_from_json(value: &Value) -> Option<Vec<u8>> {
    for path in [
        "/txn",
        "/transaction",
        "/swapTransaction",
        "/data/txn",
        "/data/transaction",
        "/data/swapTransaction",
        "/data/tx",
        "/data/tx/data",
        "/data/0/txn",
        "/data/0/transaction",
        "/data/0/swapTransaction",
        "/data/0/tx",
        "/data/0/tx/data",
        "/result/transaction",
        "/transactions/0",
        "/data/transactions/0",
        "/data/transactions/0/transaction",
    ] {
        if let Some(text) = value.pointer(path).and_then(Value::as_str) {
            if let Ok(bytes) = decode_payload_string(text) {
                return Some(bytes);
            }
        }
    }
    None
}

fn decode_payload_string(value: &str) -> Result<Vec<u8>> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    general_purpose::STANDARD
        .decode(value)
        .or_else(|_| general_purpose::URL_SAFE.decode(value))
        .or_else(|_| bs58::decode(value).into_vec())
        .context("payload is not base64/base58")
}

fn instruction_from_compiled_message(
    message: &VersionedMessage,
    account_keys: &[Pubkey],
    program_id_index: u8,
    accounts: &[u8],
    data: &[u8],
) -> Result<Instruction> {
    let program_id = *account_keys
        .get(usize::from(program_id_index))
        .ok_or_else(|| anyhow!("compiled instruction program id index out of range"))?;
    let metas = accounts
        .iter()
        .map(|index| {
            let index = usize::from(*index);
            let pubkey = *account_keys
                .get(index)
                .ok_or_else(|| anyhow!("compiled instruction account index out of range"))?;
            Ok(if message.is_maybe_writable(index, None) {
                AccountMeta::new(pubkey, message.is_signer(index))
            } else {
                AccountMeta::new_readonly(pubkey, message.is_signer(index))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Instruction {
        program_id,
        accounts: metas,
        data: data.to_vec(),
    })
}

fn api_compute_budget_instructions(config: &BotConfig) -> Vec<Instruction> {
    let mut instructions = vec![ComputeBudgetInstruction::set_compute_unit_limit(
        config.execution.compute_unit_limit,
    )];
    let price = compute_unit_price_micro_lamports(config);
    if price > 0 {
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(price));
    }
    instructions
}

fn compute_unit_price_micro_lamports(config: &BotConfig) -> u64 {
    if config.execution.compute_unit_price_micro_lamports > 0 {
        config.execution.compute_unit_price_micro_lamports
    } else if config.execution.compute_unit_limit > 0 {
        config
            .fees
            .max_priority_fee_lamports
            .saturating_mul(1_000_000)
            / u64::from(config.execution.compute_unit_limit)
    } else {
        0
    }
}

fn is_compute_budget_instruction(ix: &Instruction) -> bool {
    ix.program_id.to_string() == COMPUTE_BUDGET_PROGRAM
}

fn merge_lookup_tables(
    first: Vec<AddressLookupTableAccount>,
    second: Vec<AddressLookupTableAccount>,
) -> (Vec<AddressLookupTableAccount>, Vec<String>) {
    let mut by_key = BTreeMap::<String, AddressLookupTableAccount>::new();
    for table in first.into_iter().chain(second) {
        by_key.entry(table.key.to_string()).or_insert(table);
    }
    let addresses = by_key.keys().cloned().collect::<Vec<_>>();
    (by_key.into_values().collect(), addresses)
}

fn first_u128(value: &Value, paths: &[&str]) -> Option<u128> {
    paths.iter().find_map(|path| {
        let value = value.pointer(path)?;
        value
            .as_u64()
            .map(|value| value as u128)
            .or_else(|| value.as_str()?.parse::<u128>().ok())
    })
}

fn quote_amount_usd(
    amount_atoms: u128,
    decimals: u8,
    mint: &str,
    sol_usd_estimate: f64,
) -> Option<f64> {
    match mint {
        WSOL_MINT => Some(atoms_to_ui(amount_atoms, decimals) * sol_usd_estimate),
        USDC_MINT | USDT_MINT => Some(atoms_to_ui(amount_atoms, decimals)),
        _ => None,
    }
}

fn quote_amount_usd_signed(
    amount_atoms: i128,
    decimals: u8,
    mint: &str,
    sol_usd_estimate: f64,
) -> Option<f64> {
    let sign = if amount_atoms < 0 { -1.0 } else { 1.0 };
    quote_amount_usd(
        amount_atoms.unsigned_abs(),
        decimals,
        mint,
        sol_usd_estimate,
    )
    .map(|amount| amount * sign)
}

fn estimated_priority_fee_usd(config: &BotConfig) -> f64 {
    let total_lamports = config
        .fees
        .max_priority_fee_lamports
        .saturating_add(config.fees.jito_tip_lamports)
        .saturating_add(5_000);
    atoms_to_ui(total_lamports as u128, 9) * config.trading.sol_usd_estimate
}

fn slippage_floor(amount: u64, slippage_bps: u64) -> u64 {
    let retained_bps = 10_000u64.saturating_sub(slippage_bps.min(9_999));
    ((u128::from(amount) * u128::from(retained_bps)) / 10_000) as u64
}

fn titan_quote_request(
    wallet: &Pubkey,
    pair: &TokenPair,
    side: SwapSide,
    amount_atoms: u64,
    config: &BotConfig,
) -> Result<MsgPackValue> {
    let input_mint = Pubkey::from_str(input_mint_for_side(pair, side))?;
    let output_mint = Pubkey::from_str(output_mint_for_side(pair, side))?;
    Ok(mp_map(vec![
        ("id", MsgPackValue::from(1u32)),
        (
            "data",
            mp_map(vec![(
                "NewSwapQuoteStream",
                mp_map(vec![
                    (
                        "swap",
                        mp_map(vec![
                            (
                                "inputMint",
                                MsgPackValue::Binary(input_mint.to_bytes().to_vec()),
                            ),
                            (
                                "outputMint",
                                MsgPackValue::Binary(output_mint.to_bytes().to_vec()),
                            ),
                            ("amount", MsgPackValue::from(amount_atoms)),
                            ("swapMode", MsgPackValue::from("ExactIn")),
                            (
                                "slippageBps",
                                MsgPackValue::from(u64::from(config.jupiter.slippage_bps)),
                            ),
                            ("addSizeConstraint", MsgPackValue::from(true)),
                            ("sizeConstraint", MsgPackValue::from(1_180u64)),
                            (
                                "accountsLimitTotal",
                                MsgPackValue::from(u64::from(
                                    config.execution.jupiter_max_accounts,
                                )),
                            ),
                        ]),
                    ),
                    (
                        "transaction",
                        mp_map(vec![
                            (
                                "userPublicKey",
                                MsgPackValue::Binary(wallet.to_bytes().to_vec()),
                            ),
                            ("createOutputTokenAccount", MsgPackValue::from(true)),
                        ]),
                    ),
                    (
                        "update",
                        mp_map(vec![
                            ("intervalMs", MsgPackValue::from(250u64)),
                            ("numQuotes", MsgPackValue::from(1u64)),
                        ]),
                    ),
                ]),
            )]),
        ),
    ]))
}

fn mp_map(entries: Vec<(&str, MsgPackValue)>) -> MsgPackValue {
    MsgPackValue::Map(
        entries
            .into_iter()
            .map(|(key, value)| (MsgPackValue::from(key), value))
            .collect(),
    )
}

fn find_best_titan_route(value: &MsgPackValue) -> Option<&MsgPackValue> {
    let quotes_map = find_mp_key(value, "quotes")?.as_map()?;
    quotes_map
        .iter()
        .filter_map(|(_, route)| mp_map_u64(route, "outAmount").map(|out| (out, route)))
        .max_by_key(|(out, _)| *out)
        .map(|(_, route)| route)
}

fn titan_instructions(route: &MsgPackValue) -> Result<Option<Vec<Instruction>>> {
    let Some(values) = mp_map_value(route, "instructions").and_then(MsgPackValue::as_array) else {
        return Ok(None);
    };
    if values.is_empty() {
        return Ok(None);
    }
    values
        .iter()
        .map(titan_instruction)
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

fn titan_instruction(value: &MsgPackValue) -> Result<Instruction> {
    let program_id = mp_map_pubkey_any(value, &["p", "programId", "program_id"])
        .context("Titan instruction missing program id")?;
    let accounts = mp_map_value_any(value, &["a", "accounts"])
        .and_then(MsgPackValue::as_array)
        .unwrap_or(&Vec::new())
        .iter()
        .map(|account| {
            let pubkey = mp_map_pubkey_any(account, &["p", "pubkey", "pubKey"])
                .context("Titan account missing pubkey")?;
            let is_signer =
                mp_map_bool_any(account, &["s", "isSigner", "is_signer"]).unwrap_or(false);
            let is_writable =
                mp_map_bool_any(account, &["w", "isWritable", "is_writable"]).unwrap_or(false);
            Ok(if is_writable {
                AccountMeta::new(pubkey, is_signer)
            } else {
                AccountMeta::new_readonly(pubkey, is_signer)
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let data = mp_map_bytes_any(value, &["d", "data"]).unwrap_or_default();
    Ok(Instruction {
        program_id,
        accounts,
        data,
    })
}

fn titan_lookup_tables(route: &MsgPackValue) -> Vec<String> {
    mp_map_value_any(
        route,
        &[
            "addressLookupTables",
            "addressLookupTableAddresses",
            "lookupTables",
        ],
    )
    .and_then(MsgPackValue::as_array)
    .into_iter()
    .flatten()
    .filter_map(|value| mp_value_pubkey(value).ok())
    .map(|pubkey| pubkey.to_string())
    .collect()
}

fn titan_transaction(route: &MsgPackValue) -> Option<Vec<u8>> {
    mp_map_bytes_any(route, &["transaction", "swapTransaction", "txn"])
}

fn titan_route_labels(route: &MsgPackValue) -> Vec<String> {
    mp_map_value(route, "steps")
        .and_then(MsgPackValue::as_array)
        .into_iter()
        .flatten()
        .filter_map(|step| {
            mp_map_value(step, "label")
                .and_then(MsgPackValue::as_str)
                .map(|label| format!("titan:{label}"))
        })
        .collect::<Vec<_>>()
}

fn find_mp_key<'a>(value: &'a MsgPackValue, key: &str) -> Option<&'a MsgPackValue> {
    if let Some(map) = value.as_map() {
        for (candidate, child) in map {
            if candidate.as_str() == Some(key) {
                return Some(child);
            }
            if let Some(found) = find_mp_key(child, key) {
                return Some(found);
            }
        }
    }
    if let Some(values) = value.as_array() {
        for child in values {
            if let Some(found) = find_mp_key(child, key) {
                return Some(found);
            }
        }
    }
    None
}

fn mp_map_value<'a>(value: &'a MsgPackValue, key: &str) -> Option<&'a MsgPackValue> {
    value
        .as_map()?
        .iter()
        .find_map(|(candidate, child)| (candidate.as_str() == Some(key)).then_some(child))
}

fn mp_map_value_any<'a>(value: &'a MsgPackValue, keys: &[&str]) -> Option<&'a MsgPackValue> {
    keys.iter().find_map(|key| mp_map_value(value, key))
}

fn mp_map_u64(value: &MsgPackValue, key: &str) -> Option<u64> {
    let value = mp_map_value(value, key)?;
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|value| value.try_into().ok()))
        .or_else(|| value.as_str()?.parse().ok())
}

fn mp_map_bool(value: &MsgPackValue, key: &str) -> Option<bool> {
    mp_map_value(value, key)?.as_bool()
}

fn mp_map_bool_any(value: &MsgPackValue, keys: &[&str]) -> Option<bool> {
    keys.iter().find_map(|key| mp_map_bool(value, key))
}

fn mp_map_bytes(value: &MsgPackValue, key: &str) -> Option<Vec<u8>> {
    mp_map_value(value, key).and_then(mp_value_bytes)
}

fn mp_map_bytes_any(value: &MsgPackValue, keys: &[&str]) -> Option<Vec<u8>> {
    keys.iter().find_map(|key| mp_map_bytes(value, key))
}

fn mp_map_pubkey_any(value: &MsgPackValue, keys: &[&str]) -> Result<Pubkey> {
    mp_value_pubkey(mp_map_value_any(value, keys).context("missing pubkey")?)
}

fn mp_value_pubkey(value: &MsgPackValue) -> Result<Pubkey> {
    if let Some(text) = value.as_str() {
        return Pubkey::from_str(text).with_context(|| format!("invalid pubkey {text}"));
    }
    let bytes = mp_value_bytes(value).context("pubkey value is not binary")?;
    let array: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow!("pubkey binary value was not 32 bytes"))?;
    Ok(Pubkey::new_from_array(array))
}

fn mp_value_bytes(value: &MsgPackValue) -> Option<Vec<u8>> {
    match value {
        MsgPackValue::Binary(bytes) => Some(bytes.clone()),
        MsgPackValue::Array(values) => values
            .iter()
            .map(|value| value.as_u64().and_then(|byte| u8::try_from(byte).ok()))
            .collect::<Option<Vec<_>>>(),
        MsgPackValue::String(text) => text
            .as_str()
            .and_then(|value| decode_payload_string(value).ok()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_generic_transaction_paths() {
        let value = json!({"txn": general_purpose::STANDARD.encode([1u8, 2, 3])});
        assert_eq!(
            transaction_payload_from_json(&value).unwrap(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn parses_dflow_style_instruction_response() {
        let value = json!({
            "addressLookupTableAddresses": ["11111111111111111111111111111111"],
            "setupInstructions": [],
            "swapInstruction": {
                "programId": "11111111111111111111111111111111",
                "accounts": [{"pubkey": "11111111111111111111111111111111", "isSigner": false, "isWritable": true}],
                "data": general_purpose::STANDARD.encode([7u8, 8, 9])
            }
        });
        let instructions = instructions_from_json(&value).unwrap().unwrap();
        assert_eq!(instructions.len(), 1);
        assert_eq!(instructions[0].data, vec![7, 8, 9]);
        assert_eq!(lookup_table_addresses_from_json(&value).len(), 1);
    }

    #[test]
    fn parses_lookup_table_object_entries_from_provider_responses() {
        let table_one = Pubkey::new_unique().to_string();
        let table_two = Pubkey::new_unique().to_string();
        let value = json!({
            "data": {
                "addressLookupTables": [
                    {"address": table_one},
                    {"pubkey": table_two},
                    {"pubKey": table_two},
                    table_one
                ]
            }
        });

        let addresses = lookup_table_addresses_from_json(&value);
        assert_eq!(addresses.len(), 2);
        assert!(addresses.contains(&table_one));
        assert!(addresses.contains(&table_two));
    }

    #[test]
    fn route_provider_prefix_is_extracted() {
        assert_eq!(route_provider_name("dflow-route:route:pool"), Some("dflow"));
        assert_eq!(route_provider_name("plain-pool"), None);
    }

    #[test]
    fn native_venue_leg_falls_back_to_jupiter_with_dex_filter() {
        let mut config = BotConfig::from_env_defaults();
        config.aggregators.providers.clear();
        let builder = ApiSwapBuilder::new(config);

        let provider = builder
            .provider_for_leg(Venue::Orca, "plain-orca-pool")
            .expect("native Orca leg should be executable through Jupiter fallback");

        match provider {
            ApiProvider::Jupiter { venue_filter } => assert_eq!(venue_filter, Some(Venue::Orca)),
            ApiProvider::Aggregator(_) => panic!("expected Jupiter fallback provider"),
        }
    }

    #[test]
    fn jupiter_params_include_native_venue_dex_filter() {
        let mut config = BotConfig::from_env_defaults();
        config.execution.jupiter_use_venue_filters = true;
        let pair = TokenPair {
            base_mint: "BaseMint11111111111111111111111111111111111".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 8,
            quote_decimals: 6,
        };
        let params = jupiter_params(
            &pair,
            SwapSide::QuoteToBase,
            1_000_000,
            &config,
            Some(Venue::Meteora),
        )
        .into_iter()
        .collect::<std::collections::HashMap<_, _>>();

        assert_eq!(
            params.get("dexes").map(String::as_str),
            Some("Meteora,Meteora DLMM,Meteora DAMM v2")
        );
    }

    #[test]
    fn dflow_order_url_replaces_legacy_paths() {
        let mut provider = AggregatorProviderConfig {
            name: "dflow".to_string(),
            venue: Venue::Dflow,
            enabled: true,
            quote_url: "https://quote-api.dflow.net/quote".to_string(),
            format: AggregatorFormat::Dflow,
            timeout_ms: 100,
            min_interval_ms: 1,
            max_in_flight: 1,
            requires_api_key: true,
            api_key_env: Some("DFLOW_API_KEY".to_string()),
            api_key_header: Some("x-api-key".to_string()),
            api_key_prefix: None,
        };
        assert_eq!(
            provider_swap_url(&provider),
            "https://quote-api.dflow.net/order"
        );

        provider.quote_url = "https://quote-api.dflow.net/swap-instructions".to_string();
        assert_eq!(
            provider_swap_url(&provider),
            "https://quote-api.dflow.net/order"
        );
    }

    #[test]
    fn dflow_order_params_include_wallet_and_execution_controls() {
        let wallet = Pubkey::new_unique();
        let mut params = vec![("inputMint".to_string(), "in".to_string())];
        add_dflow_order_execution_params(&mut params, Some(&wallet), 250_000, 0);
        let params = params
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        let wallet_string = wallet.to_string();

        assert_eq!(
            params.get("userPublicKey").map(String::as_str),
            Some(wallet_string.as_str())
        );
        assert_eq!(
            params.get("allowSyncExec").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            params.get("allowAsyncExec").map(String::as_str),
            Some("false")
        );
        assert_eq!(
            params.get("prioritizationFeeLamports").map(String::as_str),
            Some("auto")
        );
        assert_eq!(
            params
                .get("prioritizationFeeMaxLamports")
                .map(String::as_str),
            Some("250000")
        );
    }

    #[test]
    fn vybe_quote_body_uses_get_json_shape_with_wallet() {
        let pair = TokenPair {
            base_mint: "BaseMint11111111111111111111111111111111111".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 8,
            quote_decimals: 6,
        };
        let wallet = Pubkey::new_unique();

        let body = vybe_quote_body(&pair, SwapSide::QuoteToBase, 1_500_000, Some(&wallet), 75);

        assert_eq!(body["wallet"], wallet.to_string());
        assert_eq!(body["amount"], json!(1.5));
        assert_eq!(body["inputMint"], USDC_MINT);
        assert_eq!(body["outputMint"], pair.base_mint);
        assert_eq!(body["slippage"], json!(0.75));
    }

    #[test]
    fn vybe_swap_body_uses_ui_amount_router_and_percent_slippage() {
        let pair = TokenPair {
            base_mint: "BaseMint11111111111111111111111111111111111".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 8,
            quote_decimals: 6,
        };
        let wallet = Pubkey::new_unique();

        let body = vybe_swap_body(&pair, SwapSide::QuoteToBase, 1_500_000, &wallet, 75);

        assert_eq!(body["wallet"], wallet.to_string());
        assert_eq!(body["amount"], json!(1.5));
        assert_eq!(body["inputMint"], USDC_MINT);
        assert_eq!(body["outputMint"], pair.base_mint);
        assert_eq!(body["router"], "vybe");
        assert_eq!(body["slippage"], json!(0.75));
        assert!(body.get("quoteResponse").is_none());
    }

    #[test]
    fn venum_quote_body_uses_post_json_quote_shape() {
        let pair = TokenPair {
            base_mint: "BaseMint11111111111111111111111111111111111".to_string(),
            quote_mint: USDC_MINT.to_string(),
            base_decimals: 8,
            quote_decimals: 6,
        };

        let body = venum_quote_body(&pair, SwapSide::BaseToQuote, 25_000_000);

        assert_eq!(body["inputMint"], pair.base_mint);
        assert_eq!(body["outputMint"], USDC_MINT);
        assert_eq!(body["amount"], "25000000");
        assert_eq!(body["slippageBps"], json!(50));
    }

    #[test]
    fn venum_best_route_amounts_are_executable_quote_compatible() {
        let value = json!({
            "bestRoute": {
                "inputAmount": "25000000",
                "outputAmount": "199001234",
                "minOutputAmount": "197000000"
            },
            "routes": [{
                "inputAmount": "25000000",
                "outputAmount": "199001234",
                "minOutputAmount": "197000000"
            }]
        });

        assert_eq!(parse_out_amount(&value), Some(199_001_234));
        assert_eq!(parse_min_out_amount(&value), Some(197_000_000));
    }

    #[test]
    fn titan_websocket_request_uses_v1_protocol_header() {
        let mut request = "wss://api.titan.ag/api/v1/ws"
            .into_client_request()
            .expect("request should parse");
        let protocol = titan_ws_protocol();
        apply_titan_ws_protocol(&mut request).expect("protocol header should be valid");
        assert_eq!(
            request
                .headers()
                .get("sec-websocket-protocol")
                .and_then(|value| value.to_str().ok()),
            Some(protocol.as_str())
        );
    }

    #[test]
    fn titan_route_parser_accepts_string_amounts_and_full_instruction_keys() {
        let program_id = Pubkey::new_unique();
        let account = Pubkey::new_unique();
        let lookup_table = Pubkey::new_unique();
        let instruction = mp_map(vec![
            ("programId", MsgPackValue::from(program_id.to_string())),
            (
                "accounts",
                MsgPackValue::Array(vec![mp_map(vec![
                    ("pubkey", MsgPackValue::from(account.to_string())),
                    ("isSigner", MsgPackValue::from(true)),
                    ("isWritable", MsgPackValue::from(false)),
                ])]),
            ),
            (
                "data",
                MsgPackValue::from(general_purpose::STANDARD.encode([7u8, 8, 9])),
            ),
        ]);
        let route = mp_map(vec![
            ("outAmount", MsgPackValue::from("125000000")),
            ("slippageBps", MsgPackValue::from("42")),
            ("instructions", MsgPackValue::Array(vec![instruction])),
            (
                "addressLookupTableAddresses",
                MsgPackValue::Array(vec![MsgPackValue::from(lookup_table.to_string())]),
            ),
        ]);
        let message = mp_map(vec![(
            "quotes",
            MsgPackValue::Map(vec![(MsgPackValue::from("best"), route)]),
        )]);

        let route = find_best_titan_route(&message).expect("best Titan route");
        assert_eq!(mp_map_u64(route, "outAmount"), Some(125_000_000));

        let instructions = titan_instructions(route).unwrap().unwrap();
        assert_eq!(instructions.len(), 1);
        assert_eq!(instructions[0].program_id, program_id);
        assert_eq!(instructions[0].accounts[0].pubkey, account);
        assert!(instructions[0].accounts[0].is_signer);
        assert!(!instructions[0].accounts[0].is_writable);
        assert_eq!(instructions[0].data, vec![7, 8, 9]);
        assert_eq!(titan_lookup_tables(route), vec![lookup_table.to_string()]);
    }

    #[test]
    fn provider_api_key_header_value_includes_configured_prefix() {
        let env_name = "ARB_BOT_TEST_TITAN_API_TOKEN";
        std::env::set_var(env_name, "test-token");
        let provider = AggregatorProviderConfig {
            name: "titan".to_string(),
            venue: Venue::Titan,
            enabled: true,
            quote_url: "wss://api.titan.ag/api/v1/ws".to_string(),
            format: AggregatorFormat::TitanStream,
            timeout_ms: 100,
            min_interval_ms: 1,
            max_in_flight: 1,
            requires_api_key: true,
            api_key_env: Some(env_name.to_string()),
            api_key_header: Some("Authorization".to_string()),
            api_key_prefix: Some("Bearer ".to_string()),
        };

        assert_eq!(
            provider_api_key_header_value(&provider).as_deref(),
            Some("Bearer test-token")
        );
        std::env::remove_var(env_name);
    }
}
