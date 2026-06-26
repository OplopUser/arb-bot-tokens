# Bot Optimization Tracking

> Created: 2026-06-17 | Last updated: 2026-06-17 12:50

## Current State
- **Transactions landing**: ✅ (transactions submit and confirm on-chain via Jito TxnForwarder)
- **Execution success**: ❌ (0 profitable — all ExceededSlippage or pool state drift)
- **Cycle time**: ~3s
- **Pairs**: 15 seed pairs, 58 focus routes, ~475 pools
- **Priority fee**: 95th percentile, max 1M lamports
- **Slippage**: 200 bps (was 50)

## Task List

### HIGH IMPACT

- [x] **1. Raise priority fee to 95th percentile** ✅
  - `config.toml`: `priority_fee_percentile = 95.0`
  - Faster block inclusion, less state drift

- [x] **2. Widen arbitrage slippage tolerance** ✅
  - `.env`: `LIVE_ARBITRAGE_SLIPPAGE_BPS=200` (was 150)
  - Fewer ExceededSlippage errors expected

- [x] **3. Pre-fund ATAs for seed tokens** ✅
  - `scripts/prefund-atas.sh` created
  - Run manually: saves $0.15 ATA rent + 1 instruction per first trade

### MEDIUM IMPACT

- [x] **4. Add RPC fallback URLs** ✅
  - Added `api.mainnet-beta.solana.com`, `solana-mainnet.g.alchemy.com/v2/demo`
  - Multiple RPCs for race-to-submit

- [x] **5. Increase compute unit limit to 2M** ✅
  - `config.toml`: `compute_unit_limit = 2000000` (was 1.4M)

- [x] **6. Remove non-tradable tokens (KMNO, DRIFT)** ✅
  - Removed seed pairs, kept native focus routes

## RESULTS LOG

| Timestamp | Change | Before | After | Notes |
|---|---|---|---|---|
| 2026-06-17 12:50 | All 6 tasks deployed | 6004 errors, 0% profit | 1 submission, bot cooling down routes | Slippage still too wide for market |
| 2026-06-17 12:33 | TxnForwarder switch | 0/14 landed | 4/4 landed | Tx confirm on-chain ✅ |
| 2026-06-17 12:28 | Added seed pairs | 6 tokens | 19 tokens | 5x more attempts |
