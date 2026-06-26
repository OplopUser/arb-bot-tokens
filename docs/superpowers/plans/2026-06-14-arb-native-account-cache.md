# Arb Native Account Cache and Focused Route Execution

## Goal

Make the bot stop treating selected Meteora -> Raydium price-only indexed pools as executable signals until it has the real executable account state, while starting a persistent pool metadata layer that can warm future cycles without refetching every pool address.

## Scope

- Add explicit native focus route config for selected route families.
- Gate focused routes so price-only indexed pools remain reference/discovery data, not live native execution candidates.
- Keep Jupiter probes available as reference/calibration when a focused route is not executable.
- Persist discovered pool metadata to a local state file and warm the in-memory cache from that state on restart.
- Wire WIF/WSOL and FARTCOIN/WSOL Meteora -> Raydium as the current focused routes in the live token config.

## Out Of Scope For This Slice

- Full Meteora DLMM/Raydium CLMM account graph reconstruction.
- Fresh tick-array/bin-array account fetching.
- Sending a native transaction for a focused route before fresh native quote support exists.

## Verification

- Unit tests for focused price-only route gating.
- Unit tests for persistent pool metadata snapshot dedupe/roundtrip.
- `cargo fmt`
- `cargo test -p arb-bot`
- `cargo build -p arb-bot`
