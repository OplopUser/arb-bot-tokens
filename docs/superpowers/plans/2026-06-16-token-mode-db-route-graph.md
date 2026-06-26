# Token Mode DB Route Graph Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the foundational DB-backed route graph and freshness boundary needed before native token-mode arb routes can be treated as executable.

**Architecture:** Add a focused SQLite persistence module for slow-changing graph facts and audit observations. Keep fresh account bytes in runtime-only structs so DB data can speed up discovery but can never become quote/simulation/submit truth.

**Tech Stack:** Rust 2021, `rusqlite`, existing `arb-bot` crate, SQLite WAL, existing dashboard/CLI/config modules.

---

### Task 1: SQLite Config and Migration Runner

**Files:**
- Modify: `crates/arb-bot/Cargo.toml`
- Modify: `crates/arb-bot/src/main.rs`
- Modify: `crates/arb-bot/src/config.rs`
- Create: `crates/arb-bot/src/db.rs`
- Modify: `config.example.toml`
- Modify: `config.tokens.toml`

- [ ] Write failing tests for default database config and migration idempotence.
- [ ] Run focused tests and confirm they fail because `DatabaseConfig` and `db` module do not exist.
- [ ] Add `rusqlite` dependency.
- [ ] Add `DatabaseConfig` to `BotConfig` with `enabled`, `path`, `migrations_on_startup`, and `busy_timeout_ms`.
- [ ] Add `db.rs` with `ArbDatabase::open`, `run_migrations`, `schema_migrations`, and table creation.
- [ ] Add `db-summary` CLI command that opens DB, runs migrations when configured, and prints compact JSON summary.
- [ ] Run focused tests and full Rust tests.

### Task 2: Route Graph Repository

**Files:**
- Modify: `crates/arb-bot/src/db.rs`
- Modify: `crates/arb-bot/src/types.rs`

- [ ] Write failing tests for token/pool/route-family/route-edge upsert idempotence.
- [ ] Implement repository structs and upsert/read methods.
- [ ] Add canonical route key behavior.
- [ ] Verify duplicate discovery updates rows rather than inserting duplicates.

### Task 3: Runtime Freshness Boundary

**Files:**
- Modify: `crates/arb-bot/src/types.rs`
- Modify: `crates/arb-bot/src/db.rs`

- [ ] Write failing tests proving a DB manifest alone is not fresh execution truth.
- [ ] Add `RuntimeAccountSnapshot`, `AccountFreshness`, and stale/missing/owner-mismatch validation helpers.
- [ ] Ensure DB stores manifest metadata, not live account bytes by default.

### Task 4: Engine Bootstrap Writes

**Files:**
- Modify: `crates/arb-bot/src/engine.rs`
- Modify: `crates/arb-bot/src/db.rs`

- [ ] Write failing tests for persisting discovered pools and configured focus route families.
- [ ] Wire DB initialization into `BotEngine::new` or cycle startup without blocking tests.
- [ ] Persist discovered pool graph after discovery refresh.
- [ ] Preserve JSON fallback during transition.

### Task 5: Dashboard and Status Surface

**Files:**
- Modify: `crates/arb-bot/src/dashboard.rs`
- Modify: `scripts/read-dashboard-state.mjs`

- [ ] Write failing serialization tests for route graph readiness summary.
- [ ] Add a compact route graph/readiness summary to dashboard state.
- [ ] Update helper script output.

### Verification

- [ ] `cargo fmt`
- [ ] `cargo test -p arb-bot`
- [ ] `cargo build -p arb-bot`
- [ ] `git diff --check`
