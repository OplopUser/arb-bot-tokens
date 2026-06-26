use crate::{config::RpcConfig, math::percentile, types::RawAccount};
use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose, Engine as _};
use reqwest::Client;
use serde_json::{json, Value};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

#[derive(Clone)]
pub struct RpcClient {
    http_url: String,
    client: Client,
    id: std::sync::Arc<AtomicU64>,
}

impl RpcClient {
    pub fn new(config: &RpcConfig) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .build()
            .unwrap_or_else(|_| Client::new());
        Self {
            http_url: config.primary_http.clone(),
            client,
            id: std::sync::Arc::new(AtomicU64::new(1)),
        }
    }

    #[allow(dead_code)]
    pub fn with_url(http_url: impl Into<String>) -> Self {
        Self {
            http_url: http_url.into(),
            client: Client::new(),
            id: std::sync::Arc::new(AtomicU64::new(1)),
        }
    }

    pub async fn call(&self, method: &str, params: Value) -> Result<Value> {
        let id = self.id.fetch_add(1, Ordering::Relaxed);
        let body = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });

        let response = self
            .client
            .post(&self.http_url)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("rpc request failed: {method}"))?;
        let status = response.status();
        let payload: Value = response
            .json()
            .await
            .with_context(|| format!("invalid rpc json: {method}"))?;
        if !status.is_success() {
            return Err(anyhow!("rpc http {status}: {payload}"));
        }
        if let Some(error) = payload.get("error") {
            return Err(anyhow!("rpc {method} error: {error}"));
        }
        payload
            .get("result")
            .cloned()
            .ok_or_else(|| anyhow!("rpc {method} response missing result"))
    }

    #[allow(dead_code)]
    pub async fn get_balance_lamports(&self, pubkey: &str) -> Result<u64> {
        let result = self.call("getBalance", json!([pubkey])).await?;
        result
            .get("value")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("getBalance response missing value"))
    }

    #[allow(dead_code)]
    pub async fn get_latest_blockhash(&self) -> Result<String> {
        let result = self
            .call("getLatestBlockhash", json!([{"commitment": "processed"}]))
            .await?;
        result
            .pointer("/value/blockhash")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow!("getLatestBlockhash response missing blockhash"))
    }

    pub async fn recent_priority_fees(&self, account_keys: &[String]) -> Result<Vec<u64>> {
        let params = if account_keys.is_empty() {
            json!([])
        } else {
            json!([account_keys])
        };
        let result = self.call("getRecentPrioritizationFees", params).await?;
        let values = result
            .as_array()
            .ok_or_else(|| anyhow!("getRecentPrioritizationFees did not return array"))?
            .iter()
            .filter_map(|entry| entry.get("prioritizationFee").and_then(Value::as_u64))
            .collect();
        Ok(values)
    }

    pub async fn priority_fee_percentile(&self, account_keys: &[String], pct: f64) -> Result<u64> {
        let mut values = self.recent_priority_fees(account_keys).await?;
        Ok(percentile(&mut values, pct))
    }

    pub async fn get_program_accounts(
        &self,
        program_id: &str,
        data_size: Option<usize>,
        data_slice: Option<(usize, usize)>,
    ) -> Result<Vec<RawAccount>> {
        let mut filters = Vec::new();
        if let Some(size) = data_size {
            filters.push(json!({ "dataSize": size }));
        }
        let mut config = json!({
            "encoding": "base64",
            "commitment": "processed",
            "filters": filters,
        });
        if let Some((offset, length)) = data_slice {
            config["dataSlice"] = json!({ "offset": offset, "length": length });
        }

        let result = self
            .call("getProgramAccounts", json!([program_id, config]))
            .await?;
        let accounts = result
            .as_array()
            .ok_or_else(|| anyhow!("getProgramAccounts did not return array"))?;
        accounts.iter().map(parse_program_account).collect()
    }

    pub async fn get_multiple_accounts(
        &self,
        pubkeys: &[String],
    ) -> Result<Vec<Option<RawAccount>>> {
        if pubkeys.is_empty() {
            return Ok(Vec::new());
        }

        let result = self
            .call(
                "getMultipleAccounts",
                json!([
                    pubkeys,
                    {
                        "encoding": "base64",
                        "commitment": "processed"
                    }
                ]),
            )
            .await?;
        let slot = result
            .pointer("/context/slot")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let values = result
            .get("value")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("getMultipleAccounts did not return value array"))?;

        values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                if value.is_null() {
                    return Ok(None);
                }
                parse_account_value(&pubkeys[index], value, slot).map(Some)
            })
            .collect()
    }
}

fn parse_program_account(value: &Value) -> Result<RawAccount> {
    let pubkey = value
        .get("pubkey")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("program account missing pubkey"))?
        .to_string();
    let account = value
        .get("account")
        .ok_or_else(|| anyhow!("program account missing account"))?;
    parse_account_value(&pubkey, account, 0)
}

fn parse_account_value(pubkey: &str, account: &Value, slot: u64) -> Result<RawAccount> {
    let owner = account
        .get("owner")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let lamports = account
        .get("lamports")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let data = account
        .get("data")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("program account missing base64 data"))?;
    let data = general_purpose::STANDARD
        .decode(data)
        .with_context(|| format!("invalid base64 data for account {pubkey}"))?;

    Ok(RawAccount {
        pubkey: pubkey.to_string(),
        owner,
        lamports,
        slot,
        data,
    })
}
