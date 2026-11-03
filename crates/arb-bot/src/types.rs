use serde::{de::Error as DeError, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
#[cfg(test)]
use std::collections::BTreeMap;
use std::fmt;

pub const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub const USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
pub const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
pub const BLACKLISTED_REMORA_SYMBOLS: &[&str] = &["SPYr", "NVDAr", "TSLAr"];
pub const BLACKLISTED_REMORA_MINT_PREFIXES: &[&str] = &["AVw2", "ALTP6", "FJug"];

pub fn is_pump_like_mint(mint: &str) -> bool {
    let lower = mint.trim().to_ascii_lowercase();
    lower.starts_with("pump") || lower.ends_with("pump") || lower.contains("pumpswap")
}

pub fn is_blocklisted_remora_symbol(symbol: &str) -> bool {
    let trimmed = symbol.trim();
    BLACKLISTED_REMORA_SYMBOLS
        .iter()
        .any(|blocked| trimmed.eq_ignore_ascii_case(blocked))
}

pub fn is_blocklisted_remora_mint(mint: &str) -> bool {
    let trimmed = mint.trim();
    BLACKLISTED_REMORA_MINT_PREFIXES
        .iter()
        .any(|prefix| trimmed.starts_with(prefix))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Venue {
    Orca,
    Meteora,
    Raydium,
    Jupiter,
    Dflow,
    Titan,
    Okx,
    SpiderSwap,
    OpenOcean,
    Vybe,
    Venum,
    Mobula,
    Custom,
}

impl fmt::Display for Venue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Venue::Orca => write!(f, "orca"),
            Venue::Meteora => write!(f, "meteora"),
            Venue::Raydium => write!(f, "raydium"),
            Venue::Jupiter => write!(f, "jupiter"),
            Venue::Dflow => write!(f, "dflow"),
            Venue::Titan => write!(f, "titan"),
            Venue::Okx => write!(f, "okx"),
            Venue::SpiderSwap => write!(f, "spiderswap"),
            Venue::OpenOcean => write!(f, "openocean"),
            Venue::Vybe => write!(f, "vybe"),
            Venue::Venum => write!(f, "venum"),
            Venue::Mobula => write!(f, "mobula"),
            Venue::Custom => write!(f, "custom"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PoolKind {
    ConstantProduct,
    ConcentratedLiquidity,
    Dlmm,
    RaydiumAmmV4,
    MeteoraPools,
    MeteoraDammV2,
}

pub const RAYDIUM_AMM_V4_PROGRAM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
pub const RAYDIUM_CLMM_PROGRAM: &str = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";
pub const RAYDIUM_CPMM_PROGRAM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
pub const METEORA_DLMM_PROGRAM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
pub const METEORA_DAMM_V2_PROGRAM: &str = "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG";
pub const METEORA_POOLS_PROGRAM: &str = "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SwapSide {
    BaseToQuote,
    QuoteToBase,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TokenPair {
    pub base_mint: String,
    pub quote_mint: String,
    pub base_decimals: u8,
    pub quote_decimals: u8,
}

impl TokenPair {
    pub fn key(&self) -> String {
        format!("{}/{}", self.base_mint, self.quote_mint)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolState {
    pub venue: Venue,
    pub kind: PoolKind,
    pub address: String,
    pub pair: TokenPair,
    #[serde(
        serialize_with = "serialize_u128_as_string",
        deserialize_with = "deserialize_u128_from_string_or_number"
    )]
    pub reserve_base_atoms: u128,
    #[serde(
        serialize_with = "serialize_u128_as_string",
        deserialize_with = "deserialize_u128_from_string_or_number"
    )]
    pub reserve_quote_atoms: u128,
    #[serde(
        default,
        serialize_with = "serialize_option_u128_as_string",
        deserialize_with = "deserialize_option_u128_from_string_or_number",
        skip_serializing_if = "Option::is_none"
    )]
    pub sqrt_price_x64: Option<u128>,
    pub active_bin_id: Option<i32>,
    pub fee_bps: u16,
    pub protocol_fee_bps: u16,
    #[serde(
        serialize_with = "serialize_u128_as_string",
        deserialize_with = "deserialize_u128_from_string_or_number"
    )]
    pub liquidity_quote_atoms: u128,
    pub updated_slot: u64,
}

impl PoolState {
    pub fn price_quote_per_base(&self) -> Option<f64> {
        if self.reserve_base_atoms > 0 && self.reserve_quote_atoms > 0 {
            let base = atoms_to_ui(self.reserve_base_atoms, self.pair.base_decimals);
            let quote = atoms_to_ui(self.reserve_quote_atoms, self.pair.quote_decimals);
            return Some(quote / base);
        }

        self.sqrt_price_x64.map(|sqrt_price| {
            price_from_sqrt_x64(
                sqrt_price,
                self.pair.base_decimals,
                self.pair.quote_decimals,
            )
        })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FeeBreakdown {
    pub lp_fee_atoms: u128,
    pub protocol_fee_atoms: u128,
    pub host_fee_atoms: u128,
    pub base_fee_lamports: u64,
    pub priority_fee_lamports: u64,
    pub jito_tip_lamports: u64,
    pub ata_rent_lamports: u64,
    pub slippage_bps: f64,
    pub safety_buffer_bps: f64,
    pub network_fee_usd: f64,
}

impl FeeBreakdown {
    pub fn merge_network(&mut self, other: &FeeBreakdown) {
        self.base_fee_lamports = self
            .base_fee_lamports
            .saturating_add(other.base_fee_lamports);
        self.priority_fee_lamports = self
            .priority_fee_lamports
            .saturating_add(other.priority_fee_lamports);
        self.jito_tip_lamports = self
            .jito_tip_lamports
            .saturating_add(other.jito_tip_lamports);
        self.ata_rent_lamports = self
            .ata_rent_lamports
            .saturating_add(other.ata_rent_lamports);
        self.network_fee_usd += other.network_fee_usd;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quote {
    pub venue: Venue,
    pub pool: String,
    pub pair: TokenPair,
    pub side: SwapSide,
    pub in_amount_atoms: u128,
    pub out_amount_atoms: u128,
    pub effective_price: f64,
    pub fee: FeeBreakdown,
    pub slot: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizerDecisionTrace {
    pub optimizer_decision_id: String,
    pub source: String,
    pub route_label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_edge_id: Option<i64>,
    #[serde(default)]
    pub buy_pool: String,
    #[serde(default)]
    pub sell_pool: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buy_venue: Option<Venue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sell_venue: Option<Venue>,
    #[serde(
        serialize_with = "serialize_u128_as_string",
        deserialize_with = "deserialize_u128_from_string_or_number"
    )]
    pub min_input_quote_atoms: u128,
    #[serde(
        serialize_with = "serialize_u128_as_string",
        deserialize_with = "deserialize_u128_from_string_or_number"
    )]
    pub max_input_quote_atoms: u128,
    #[serde(
        serialize_with = "serialize_u128_as_string",
        deserialize_with = "deserialize_u128_from_string_or_number"
    )]
    pub selected_input_quote_atoms: u128,
    #[serde(
        serialize_with = "serialize_u128_as_string",
        deserialize_with = "deserialize_u128_from_string_or_number"
    )]
    pub base_atoms: u128,
    #[serde(
        serialize_with = "serialize_u128_as_string",
        deserialize_with = "deserialize_u128_from_string_or_number"
    )]
    pub output_quote_atoms: u128,
    #[serde(
        serialize_with = "serialize_i128_as_string",
        deserialize_with = "deserialize_i128_from_string_or_number"
    )]
    pub gross_profit_quote_atoms: i128,
    #[serde(
        serialize_with = "serialize_u128_as_string",
        deserialize_with = "deserialize_u128_from_string_or_number"
    )]
    pub required_profit_quote_atoms: u128,
    #[serde(
        serialize_with = "serialize_i128_as_string",
        deserialize_with = "deserialize_i128_from_string_or_number"
    )]
    pub net_profit_quote_atoms: i128,
    pub net_profit_usd: f64,
    pub net_profit_bps: f64,
    pub quote_atom_usd_value: f64,
    pub safety_buffer_bps: f64,
    pub min_net_profit_bps: f64,
    #[serde(
        serialize_with = "serialize_u128_as_string",
        deserialize_with = "deserialize_u128_from_string_or_number"
    )]
    pub min_absolute_profit_quote_atoms: u128,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_option_u128_as_string",
        deserialize_with = "deserialize_option_u128_from_string_or_number"
    )]
    pub min_executable_input_quote_atoms: Option<u128>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejection_reason: Option<String>,
    #[serde(default)]
    pub network_fee: FeeBreakdown,
    #[serde(default = "default_include_setup_cost")]
    pub include_setup_cost: bool,
    #[serde(default)]
    pub setup_lamport_usd_value: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buy_pool_snapshot: Option<PoolState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sell_pool_snapshot: Option<PoolState>,
}

fn default_include_setup_cost() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Opportunity {
    pub pair: TokenPair,
    pub buy_venue: Venue,
    pub sell_venue: Venue,
    pub buy_pool: String,
    pub sell_pool: String,
    pub input_quote_atoms: u128,
    pub base_atoms: u128,
    pub output_quote_atoms: u128,
    pub gross_profit_quote_atoms: i128,
    pub net_profit_usd: f64,
    pub net_profit_bps: f64,
    pub spread_bps: f64,
    pub fee: FeeBreakdown,
    pub detected_slot: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optimizer_decision: Option<OptimizerDecisionTrace>,
}

fn serialize_u128_as_string<S>(value: &u128, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

fn serialize_i128_as_string<S>(value: &i128, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

fn serialize_option_u128_as_string<S>(
    value: &Option<u128>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(value) => serializer.serialize_some(&value.to_string()),
        None => serializer.serialize_none(),
    }
}

fn deserialize_u128_from_string_or_number<'de, D>(deserializer: D) -> Result<u128, D::Error>
where
    D: Deserializer<'de>,
{
    match Value::deserialize(deserializer)? {
        Value::String(value) => value.parse::<u128>().map_err(D::Error::custom),
        Value::Number(value) => value
            .as_u64()
            .map(u128::from)
            .ok_or_else(|| D::Error::custom("expected unsigned integer")),
        _ => Err(D::Error::custom("expected unsigned integer or string")),
    }
}

fn deserialize_option_u128_from_string_or_number<'de, D>(
    deserializer: D,
) -> Result<Option<u128>, D::Error>
where
    D: Deserializer<'de>,
{
    match Option::<Value>::deserialize(deserializer)? {
        Some(Value::String(value)) => value.parse::<u128>().map(Some).map_err(D::Error::custom),
        Some(Value::Number(value)) => value
            .as_u64()
            .map(u128::from)
            .map(Some)
            .ok_or_else(|| D::Error::custom("expected unsigned integer")),
        Some(_) => Err(D::Error::custom(
            "expected unsigned integer, string, or null",
        )),
        None => Ok(None),
    }
}

fn deserialize_i128_from_string_or_number<'de, D>(deserializer: D) -> Result<i128, D::Error>
where
    D: Deserializer<'de>,
{
    match Value::deserialize(deserializer)? {
        Value::String(value) => value.parse::<i128>().map_err(D::Error::custom),
        Value::Number(value) => value
            .as_i64()
            .map(i128::from)
            .ok_or_else(|| D::Error::custom("expected signed integer")),
        _ => Err(D::Error::custom("expected signed integer or string")),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawAccount {
    pub pubkey: String,
    pub owner: String,
    pub lamports: u64,
    pub slot: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteAccountRequirement {
    pub role: String,
    pub pubkey: String,
    pub expected_owner: Option<String>,
    pub required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FocusedRouteAccountStatus {
    Ready,
    Missing,
    Stale,
    OwnerMismatch,
    DecodeFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FocusedRouteAccountSnapshot {
    pub route_edge_id: i64,
    pub role: String,
    pub pubkey: String,
    pub required: bool,
    pub expected_owner: Option<String>,
    pub actual_owner: Option<String>,
    pub status: FocusedRouteAccountStatus,
    pub slot: Option<u64>,
    pub slot_age: Option<u64>,
    pub wall_clock_age_ms: Option<u64>,
    pub lamports: Option<u64>,
    pub data_hash: Option<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FocusedRouteReadinessSnapshot {
    pub route_edge_id: i64,
    pub ready: bool,
    pub blocker: Option<String>,
    pub accounts: Vec<FocusedRouteAccountSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FocusedRouteRequoteEvent {
    pub route_edge_id: i64,
    pub trigger_pubkey: String,
    pub update_slot: u64,
    pub update_to_quote_latency_ms: Option<u64>,
    pub quote_to_submit_latency_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FocusedRouteRequoteLatencySnapshot {
    pub route_edge_id: i64,
    pub last_trigger_pubkey: Option<String>,
    pub last_update_slot: Option<u64>,
    pub last_update_to_quote_latency_ms: Option<u64>,
    pub last_quote_to_submit_latency_ms: Option<u64>,
}

#[cfg(test)]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountFetchPlan {
    pub pubkeys: Vec<String>,
    roles_by_pubkey: BTreeMap<String, Vec<String>>,
}

#[cfg(test)]
impl AccountFetchPlan {
    pub fn from_requirements(requirements: &[RouteAccountRequirement]) -> Self {
        let mut pubkeys = Vec::new();
        let mut roles_by_pubkey = BTreeMap::<String, Vec<String>>::new();
        for requirement in requirements {
            if !roles_by_pubkey.contains_key(&requirement.pubkey) {
                pubkeys.push(requirement.pubkey.clone());
            }
            roles_by_pubkey
                .entry(requirement.pubkey.clone())
                .or_default()
                .push(requirement.role.clone());
        }
        Self {
            pubkeys,
            roles_by_pubkey,
        }
    }

    pub fn roles_for_pubkey(&self, pubkey: &str) -> Vec<String> {
        self.roles_by_pubkey
            .get(pubkey)
            .cloned()
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteReadinessState {
    PriceOnly,
    GraphIncomplete,
    GraphComplete,
    FreshAccountsReady,
    FreshQuoteReady,
    SimulationPositive,
    SubmitEligible,
    CooledDown,
}

impl RouteReadinessState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PriceOnly => "price-only",
            Self::GraphIncomplete => "graph incomplete",
            Self::GraphComplete => "graph complete",
            Self::FreshAccountsReady => "fresh accounts ready",
            Self::FreshQuoteReady => "fresh quote ready",
            Self::SimulationPositive => "simulation positive",
            Self::SubmitEligible => "submit eligible",
            Self::CooledDown => "cooled down",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "price-only" | "price_only" => Some(Self::PriceOnly),
            "graph incomplete" | "graph_incomplete" => Some(Self::GraphIncomplete),
            "graph complete" | "graph_complete" => Some(Self::GraphComplete),
            "fresh accounts ready" | "fresh_accounts_ready" => Some(Self::FreshAccountsReady),
            "fresh quote ready" | "fresh_quote_ready" => Some(Self::FreshQuoteReady),
            "simulation positive" | "simulation_positive" => Some(Self::SimulationPositive),
            "submit eligible" | "submit_eligible" => Some(Self::SubmitEligible),
            "cooled down" | "cooled_down" => Some(Self::CooledDown),
            _ => None,
        }
    }
}

pub fn atoms_to_ui(amount: u128, decimals: u8) -> f64 {
    amount as f64 / 10f64.powi(decimals as i32)
}

pub fn ui_to_atoms(amount: f64, decimals: u8) -> u128 {
    (amount * 10f64.powi(decimals as i32)).floor().max(0.0) as u128
}

pub fn price_from_sqrt_x64(sqrt_price_x64: u128, base_decimals: u8, quote_decimals: u8) -> f64 {
    let q64 = 2f64.powi(64);
    let sqrt = sqrt_price_x64 as f64 / q64;
    let raw = sqrt * sqrt;
    raw * 10f64.powi(base_decimals as i32 - quote_decimals as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_point_sqrt_price_converts_to_decimal_price() {
        let sqrt_two_x64 = ((2.0_f64.sqrt()) * 2f64.powi(64)) as u128;
        let price = price_from_sqrt_x64(sqrt_two_x64, 6, 6);
        assert!((price - 2.0).abs() < 0.000001);
    }

    #[test]
    fn pool_state_large_atoms_serialize_as_strings_for_json_evidence() {
        let pool = PoolState {
            venue: Venue::Meteora,
            kind: PoolKind::Dlmm,
            address: "Pool111111111111111111111111111111111111111".to_string(),
            pair: TokenPair {
                base_mint: "Base111111111111111111111111111111111111111".to_string(),
                quote_mint: WSOL_MINT.to_string(),
                base_decimals: 6,
                quote_decimals: 9,
            },
            reserve_base_atoms: u64::MAX as u128 + 42,
            reserve_quote_atoms: u64::MAX as u128 + 43,
            sqrt_price_x64: Some(u64::MAX as u128 + 44),
            active_bin_id: Some(12),
            fee_bps: 25,
            protocol_fee_bps: 5,
            liquidity_quote_atoms: u64::MAX as u128 + 45,
            updated_slot: 123,
        };

        let json = serde_json::to_value(&pool).expect("pool json");
        assert_eq!(
            json["reserve_base_atoms"],
            (u64::MAX as u128 + 42).to_string()
        );
        assert_eq!(
            json["reserve_quote_atoms"],
            (u64::MAX as u128 + 43).to_string()
        );
        assert_eq!(json["sqrt_price_x64"], (u64::MAX as u128 + 44).to_string());
        assert_eq!(
            json["liquidity_quote_atoms"],
            (u64::MAX as u128 + 45).to_string()
        );

        let decoded: PoolState = serde_json::from_value(json).expect("pool roundtrip");
        assert_eq!(decoded.reserve_base_atoms, pool.reserve_base_atoms);
        assert_eq!(decoded.reserve_quote_atoms, pool.reserve_quote_atoms);
        assert_eq!(decoded.sqrt_price_x64, pool.sqrt_price_x64);
        assert_eq!(decoded.liquidity_quote_atoms, pool.liquidity_quote_atoms);
    }

    #[test]
    fn pump_like_mint_detector_catches_pump_suffix() {
        assert!(is_pump_like_mint(
            "Ce2gx9KGXJ6C9Mp5b5x1sn9Mg87JwEbrQby4Zqo3pump"
        ));
        assert!(is_pump_like_mint(
            "pumpCmXqMfrsAkQ5r49WcJnRayYRqmXz6ae8H7H9Dfn"
        ));
        assert!(!is_pump_like_mint(USDC_MINT));
    }

    #[test]
    fn remora_stock_blocklist_catches_dead_symbols_and_mint_prefixes() {
        for symbol in ["SPYr", "NVDAr", "TSLAr", "spyr", "nvdar", "tslar"] {
            assert!(is_blocklisted_remora_symbol(symbol));
        }
        for mint in [
            "AVw21111111111111111111111111111111",
            "ALTP61111111111111111111111111111111",
            "FJug11111111111111111111111111111111",
        ] {
            assert!(is_blocklisted_remora_mint(mint));
        }
        assert!(!is_blocklisted_remora_mint(USDC_MINT));
    }
}
