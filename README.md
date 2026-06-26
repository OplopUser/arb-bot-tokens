# Arb Bot Tokens

Rust-first Solana token arbitrage engine for direct Orca, Raydium, and Meteora pool routes. This repository is the token/native-pool version of the bot. The RWA operating profile and RWA asset registry are intentionally not part of this repo.

## Default Profile

The committed token profile is [config.tokens.toml](config.tokens.toml).

- `rwa.enabled = false`
- `execution.enabled = true`
- `execution.submit_enabled = false`
- `native_price_only_execution_enabled = true`
- Direct and Jupiter probe size ladders include `$50` and `$100`
- Focused WIF/WSOL and FARTCOIN/WSOL route families are configured for fresh account-backed quotes
- HYPE, TRUMP, FARTCOIN, and WIF are seeded against USDC and WSOL
- Dynamic token discovery is enabled with liquidity, organic-score, and multi-venue filters

With `submit_enabled = false`, the bot can inspect candidates and run simulations, but it will not send transactions.

## Run

```bash
npm install
cargo test -p arb-bot
npm run lint
npm run tokens:run
```

For real runs, add a private RPC endpoint and wallet locally:

```bash
mkdir -p .secrets
# Put the keypair JSON at .secrets/wallet.json, or edit [wallet].path.
# Edit [rpc].primary_http in config.tokens.toml or copy it to ignored config.toml.
```

The dashboard/API listens on `127.0.0.1:8791` by default:

```bash
curl http://127.0.0.1:8791/state
curl http://127.0.0.1:8791/opportunities
curl http://127.0.0.1:8791/pools
```

## Architecture

```text
arb-bot-tokens/
├── Cargo.toml
├── config.tokens.toml
├── crates/
│   └── arb-bot/
│       ├── src/main.rs
│       ├── src/engine.rs
│       ├── src/native_swap.rs
│       ├── src/pool_index.rs
│       ├── src/pool_subscription.rs
│       ├── src/live_swap.rs
│       └── src/venues/
└── src/
    └── legacy TypeScript scanner/dashboard
```

The native Rust path discovers pools, indexes candidate token pairs, computes local quotes, optimizes direct route sizing, applies fee and freshness checks, and optionally builds/simulates the two-leg route. The TypeScript implementation is still present as legacy support during the Rust migration.

## Safety Defaults

Keep these defaults unless intentionally arming live trading:

```toml
[execution]
submit_enabled = false
max_live_fee_loss_usd_per_session = 0.0
max_failed_transactions_per_session = 0
```

Turning on transaction submission should be a separate change with fresh verification, small trade caps, and explicit risk limits.
