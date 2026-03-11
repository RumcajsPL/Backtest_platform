# CTP Evolution Roadmap
**Complementary Trading Platform — Strategic Summary**
**Date**: 2026-03-10 | Version 1.2 (V2 architecture blueprint + V3 vision added)
---
## Platform Vision
A Python-powered framework for strategy research, validation, and live trading support. Four integrated components covering the full strategy lifecycle from design to live execution.
| Component | Current maturity | Notes |
|-----------|-----------------|-------|
| Strategy Builder | Active development | WBWSStrategy implemented; modular contract design ready for additional signal triggers |
| Backtesting Engine | **V1 Production** | 8-stage pipeline declared production 2026-03-08 |
| Broker Connectivity | Early — connection confirmed | eToro API working; 4 bugs to fix; 5-step development plan defined |
| Analytical Intelligence | Planned | MC Deep + sensitivity embedded in backtester; standalone analytics layer not yet built |
---
## What We Have Today
**Strategy Builder** is more complete than it might appear. The architecture already separates the strategy contract from the signal trigger. The existing WBWSStrategy implementation is the full framework — entry/exit logic, multi-filter management, historical data handling, trade management, and analytics hooks. Adding a new signal trigger is a small development effort. A large library of candidate signal indicators is already available.
**Backtesting Engine V1** is production-declared. The 8-stage pipeline (random search → GA optimisation → WFO → MC Deep → sensitivity → verdict) is fully validated on DAX 5-minute data across 6 consecutive production runs. Two paper trade candidates are identified: `c4f0aea11a3e` (primary, exploration zone) and `da38ecc0ddc6` (secondary, safe zone). Zero ruin probability across all MC evaluations.
**Broker Connectivity** has a confirmed working eToro API connection. The `broker_support` Python package has correct architecture and four specific bugs to fix. A 5-step plan to reach automated paper trading is fully scoped.
---
## Revised Phase Order (from initial roadmap)
The original roadmap sequenced Backtesting V2 before broker connectivity. **This order has been revised** based on the following reasoning:
The most valuable unknown right now is not "can we backtest better?" — it is "does the edge hold in live conditions?" V1 backtesting has already identified tradeable candidates. V2 improvements would produce better candidates, but we don't yet know if the current candidates work live. That question is worth more than better backtest machinery, and only live data answers it.
Additionally, live paper trading results will directly inform V2 priorities: if the edge breaks at specific times of day → time window analysis becomes P0; if a filter is clearly not contributing live → filter discovery mode is P0; if live and backtest match well → multi-asset expansion (RAR normalisation) becomes the priority. V2 built before this data exists is speculative. V2 built after is evidence-based.
---
## Five Phases — Revised Order
### Phase 0 — Broker Connectivity Foundation *(now, parallel to full-history calibration run)*
**Objective**: Fix the four known bugs in `broker_support`, confirm the demo history endpoint behaviour, resolve the InstrumentID for DAX. This work is already partially done and can proceed while the calibration run is in progress.
**Deliverables**:
- All four `broker_support` bugs fixed (wrong endpoint, orphaned function, wrong date param, field alias mismatch)
- Empirical test: does `/api/v1/trading/info/trade/history` return demo trades?
- DAX InstrumentID confirmed and added to constants
- One successful manual tracker cycle recording a closed trade with correct data
**Gate**: one full position lifecycle (open → close → journal entry with correct P&L) confirmed via API.
---
### Phase 1 — Backtesting Full History + V1.1 Calibration *(parallel to Phase 0)*
**Objective**: Validate the V1 candidates across the full 38-month dataset (3 distinct DAX regimes: 2023 recovery, 2024 choppy rate cycle, 2025 range-bound). Confirm the V1 edge is not a short-window artefact.
**Deliverables**:
- Two code changes applied: `_MAX_EXPECTED_DRAWDOWN = 2_500.0`, `scenario.py ct.get()` fix
- Calibration v2 run completed; new `_SIGMOID_SCALE = 310.0` confirmed (N=231, stdev=620.09)
- Full pipeline calibration run completed; at least 1 candidate with full verdict (Stage 7)
- Production run completed overnight on `backtest_V1_01.yaml`
- V1 candidates evaluated across 13 WFO windows including the 2024 stress period (W05–W08)
**Gate**: at least 3 auto_go candidates surviving W05–W08 (2024 stress windows).
---
### Phase 2 — Automated Paper Trading via eToro Demo *(requires Phase 0 gate + Phase 1 gate)*
**Objective**: Replace manual paper trading with automated signal execution on the eToro demo account. Close the loop between the backtester output and live market data.
**Deliverables**:
- Signal bridge: reads `c4f0aea11a3e` and `da38ecc0ddc6` strategy YAMLs, maps parameters to eToro API orders (ATR multiplier → SL rate, RR target → TP rate, risk percentile → position size)
- Reliable tracker loop: 5-minute polling during DAX trading hours, automatic journal recording
- Live journal: entry/exit price, P&L, slippage vs backtest expectancy, per trade
- Execution quality monitor: flag systematic slippage deviations
**Gate**: 20+ trades executed automatically without manual intervention; journal populated with correct P&L data.
---
### Phase 3 — Backtesting V2 *(informed by Phase 2 live data)*
**Objective**: Redesign the backtesting engine based on what live paper trading reveals, and lay the architectural foundation for V3 (Strategy Setup Builder). V2 should be V3-ready: every architectural decision in V2 must not require refactoring to support the V3 meta-optimiser.
#### V2 Architecture Redesign — Single Responsibility + Shared Data
**Current V1 violation of single responsibility:**
V1 `DataLoader` both loads raw files and slices windows. V1 `StrategyOrchestrator` re-runs raw signal generation whilst signal are not changing during E2E strategy/backtester (they are calculate on the base of strategy TF and HTF only so both are not changing and can be calculated once only). FilterPipeline is different but can also be optimized it is calculated on every candidate evaluation, even though signals are deterministic functions of OHLCV data and do not vary per candidate. For 33 candidates × 7 windows = 231 evaluations, the RSI, ATR, and Bollinger series are recomputed 231 times on identical data. This is architecturally wrong and is the root cause of the OOM issues encountered in full-history WFO runs (B9O-006 through B9O-008).
**V2 target architecture — four dedicated modules:**
```
┌─────────────────────────────────────────────────────────────┐
│  RawDataStore  (replaces DataLoader — load responsibility)  │
│  • Called ONCE at pipeline start                            │
│  • Loads all Parquet files (strategy, HTF, LTF) to memory   │
│  • ARTF loaded separately (never sliced — full range only)  │
│  • Exposes: get_raw(file_type) → DataFrame                  │
│  • Releases: nothing — holds raw data for WindowSlicer      │
└──────────────────────────┬──────────────────────────────────┘
                           │ raw DataFrames (full range)
┌──────────────────────────▼──────────────────────────────────┐
│  WindowSlicer  (new — slice responsibility)                 │
│  • Called ONCE per pipeline run after RawDataStore loads    │
│  • Slices all window date ranges for all file types         │
│  • Warmup bars (200) prepended to each window slice         │
│  • Stores slices in shared memory (multiprocessing.         │
│    shared_memory) — zero-copy access across worker procs    │
│  • Releases: raw DataFrames after all windows sliced        │
│  • Exposes: get_slice(window_id, file_type) → shm_handle   │
└──────────────────────────┬──────────────────────────────────┘
                           │ shared memory handles (not copies)
┌──────────────────────────▼──────────────────────────────────┐
│  SignalCache  (new — signal generation responsibility)      │
│  • Called ONCE per window per UNIQUE parameter combination  │
│  • Generates RSI, ATR, Bollinger for the window slice       │
│  • Cache key: (window_id, rsi_period, bollinger_length,     │
│    atr_length) — only indicator-shaping params              │
│  • Stores in shared memory alongside OHLCV slices           │
│  • Workers read signals from shm — no recomputation         │
│  • Note: per-candidate params (rsi_overbought, atr_         │
│    multiplier, rr_target) do NOT affect signal generation   │
│    — they affect TradeSimulator thresholds only             │
│  • Exposes: get_signals(window_id, params) → shm_handle     │
└──────────────────────────┬──────────────────────────────────┘
                           │ signal shm handles
┌──────────────────────────▼──────────────────────────────────┐
│  TradeSimulator  (unchanged — evaluation responsibility)    │
│  • Called per candidate per window (231× for 33×7)          │
│  • Receives: ohlcv_slice + signal_slice (shm reads ~0ms)    │
│  • Applies: candidate thresholds (overbought, multiplier,   │
│    rr_target, risk_percentile)                              │
│  • Pure logic — no I/O, no signal recomputation             │
│  • RiskManager receives warmup-buffered ohlcv_slice only    │
└─────────────────────────────────────────────────────────────┘
```
**Memory impact of V2 architecture:**

| Metric | V1 (current) | V2 (target) |
|--------|-------------|-------------|
| Raw file loads per run | Up to 231 (cold cache) | 1 |
| Signal computations per run | 231 | ~10–20 (unique param combos) |
| Peak RAM per worker (cold) | 897MB (read_parquet) | ~20MB (shm slice read) |
| Workers safe at 8GB RAM | 2 | 6+ |
| `max_workers` constraint | Hard limit 2 | Removed |
**Shared memory design (Windows spawn-safe):**
Python `multiprocessing.shared_memory.SharedMemory` is the correct mechanism on Windows (spawn mode — no fork inheritance). The pattern:
1. Parent process: `RawDataStore` loads files → `WindowSlicer` creates named `SharedMemory` blocks per (window, file_type) pair
2. Worker receives: shared memory block name + array shape + dtype (all serialisable primitives)
3. Worker reconstructs: `np.ndarray` from `SharedMemory` handle → wraps in `pd.DataFrame` → slices if needed
4. Parent cleanup: releases all `SharedMemory` blocks in `finally` after pool closes
Named blocks survive the spawn boundary. Workers never copy the data — they map the same physical pages. At 6 workers × 20MB slice = 120MB total vs V1's 6 × 897MB = 5.38GB.
**Signal cache design caveat:**
Signals (RSI, ATR, Bollinger) are functions of OHLCV data AND indicator period parameters (`rsi_period`, `bollinger_length`, `atr_length`). These vary per candidate. However the search space is discrete and bounded (e.g. `rsi_period` ∈ [8..24], step 1 = 17 values). For 60 candidates, the number of unique `(rsi_period, bollinger_length, atr_length)` combinations is far smaller than 60. The `SignalCache` keyed on these indicator-shaping params eliminates most recomputation. Threshold params (`rsi_overbought`, `atr_multiplier`, `rr_target`, `risk_percentile`) are applied only in `TradeSimulator` — they never touch signal generation.
**Inteligent, dynamic cache and memory share management - to consider during V2 design phase**
Inteligent cache manager can ensure to keep in cache only data still required by the backtester pipeline. Data won't be reused will be removed. By anticipation it can see what data keep in disk cache and what charge to memory.
Avoid calculation repetition cache data will be reuse by pipeline. Full computation optional, fisrt check if cache data available then if not exist -> full computation.
Above feature should be done on purpose but not contrproductive. I we are not winning at least of E2E pipeline perf then abanndon.    
#### V2 Functional Deliverables (in addition to architecture)
- **V2-RAR**: Replace DAX-specific normalisation constants with dimensionless Rolling Annual Range fractions. Enables multi-asset backtesting without per-instrument recalibration.
- **RSI removal**: Remove `rsi_period`, `rsi_overbought`, `rsi_oversold` from search space (RSI-SENS-2 — 6 consecutive zero-delta runs confirmed).
- **Time window analysis**: Identify intra-day periods where the strategy performs best/worst. Backtested findings cross-validated against live paper trade times.
- **Filter discovery mode**: Report which filter combinations are consistently active across all auto_go candidates. Answers "is RSI helping?" systematically.
- **Dynamic WFO windows**: Replace hardcoded window list with `data_range + window_size` parameters. Required for multi-asset support and V3 meta-optimiser.
- **Backtester as callable function**: `run_backtest(config: BacktestConfig) → BacktestResult` — pure function interface with no side effects except DB write. Required for V3 outer loop.
- **B9N-001**: Systematic `scenario.py` constraint loader fix — all fields use `ct.get()` with documented defaults.
- **CAL-01**: Raise `normalisation_freq_ref_trades_per_week` to 50.0.
**Gate**: V2 produces auto_go candidates on a second instrument (DAX + one other) using the same pipeline without instrument-specific recalibration. `max_workers` constraint removed — 6+ workers stable on 8GB RAM.
- **Time session as configurable setting**: Currently strategy (and backtester) has a time filter fixed for whole pipeline. What we have today: strategy pipeline is collecting and presenting a following breakdowns:
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
London	760	14.1%	-313.7	-0.41	+114.4	-70.6
NY	316	11.1%	-795.2	-2.52	+103.1	-38.5

Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
08:00	64	12.5%	-148.0	-2.31
09:00	100	16.0%	+184.0	+1.84
10:00	106	15.1%	+59.4	+0.56
11:00	101	10.9%	-249.7	-2.47
12:00	119	15.1%	-63.4	-0.53
13:00	93	19.4%	+414.1	+4.45
14:00	83	14.5%	+4.8	+0.06
15:00	94	8.5%	-514.8	-5.48
16:00	60	13.3%	+0.5	+0.01
17:00	64	15.6%	-20.9	-0.33
18:00	67	7.5%	-316.9	-4.73
19:00	80	8.8%	-372.3	-4.65
20:00	45	11.1%	-85.7	-1.90

Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	205	15.6%	+359.9	+1.76
Tuesday	236	13.1%	-339.9	-1.44
Wednesday	215	15.3%	+28.0	+0.13
Thursday	214	13.6%	-153.0	-0.71
Friday	206	8.3%	-1003.9	-4.87

V2 design phase should decide what can be done as part of V2 and what in V3
---
### Phase 4 — Backtesting V3: Strategy Setup Builder *(requires Phase 3 gate)*
**Objective**: Build a meta-optimiser that treats the V2 backtester as a black box and answers the question: *does this strategy configuration have tradeable potential at all?* V3 is a backtester of backtests.
**Concept:**
V2 optimises *within* a fixed strategy configuration (given timeframe, filter set, risk setup — find the best parameters). V3 optimises *across* strategy configurations — it searches the space of setups to discover which combinations of timeframe, HTF, filter sequences, and risk structure produce strategies worth optimising.
```
V2 search space:  parameters     (rsi_period, atr_multiplier, rr_target, ...)
V3 search space:  configurations (timeframe, HTF, filter_set, risk_structure, ...)
```
**V3 two-phase execution:**
**Phase A — Discovery (broad, unconstrained):**
- Runs V2 backtester many times with different strategy configurations
- Uses `discovery` zone only — no constraints or minimal constraints (confirm trades exist)
- Objective: identify which configurations produce net-positive results across at least 3 of 7 WFO windows
- Configurations that pass Phase A are *potentially tradeable* — the strategy architecture has signal for this setup
**Phase B — Confirmation (constrained, calibrated):**
- Takes Phase A survivors and runs full V2 pipeline with proper constraints
- Objective: confirm profitability is stable, find optimal parameter settings, validate across time
- Adds a temporal stability check: last N months of data must produce results not materially inferior to the full-history average (market evolution guard)
- Configurations that pass Phase B are promoted to paper trading candidates
**Temporal stability check (new concept for V3):**
A configuration passes temporal stability if:
```
median_net_pnl(last_N_months_windows) >= α × median_net_pnl(all_windows)
```
where `α = 0.70` (last period must be at least 70% as good as historical average). This guards against strategies whose edge has decayed as market regimes evolve. `N` and `α` are configurable.
**Architectural requirements on V2 (V3-readiness):**
These V2 decisions must be made with V3 in mind:
1. **Backtester as callable function** (`run_backtest(config) → result`) — V3 outer loop calls V2 as a library, not a subprocess. No CLI-only interface in V2.
2. **Config as a first-class object** — `BacktestConfig` dataclass must be fully serialisable and constructable programmatically (not only from YAML). V3 generates configs in code.
3. **Dynamic WFO windows** — V3 needs to vary window size and count across configurations. Hardcoded window lists cannot be used.
4. **Stateless evaluation** — each `run_backtest()` call must be fully independent. No shared mutable state between runs. V3 runs many backtests in parallel.
5. **Structured result contract** — `BacktestResult` must expose all metrics V3 needs to score a configuration: stage pass rates, WFO window results, MC ruin probability, per-window net_pnl distribution. No hidden state in DB only.
**V3 Deliverables**:
- `StrategySetupBuilder` — outer loop over configuration space
- `ConfigurationSpace` — defines which setup dimensions to search (timeframe options, HTF options, filter set combinations, risk structure presets)
- `PhaseARunner` — broad discovery pass, no constraints, scores configurations by WFO window survival rate
- `PhaseBRunner` — full V2 pipeline pass on Phase A survivors, adds temporal stability check
- `SetupReport` — ranks configurations by Phase B score, highlights which setup dimensions matter most
**Gate**: V3 identifies at least one new tradeable strategy configuration on a different timeframe or instrument without operator guidance on parameter values.
---
### Phase 5 — Multi-Strategy and Multi-Asset *(requires Phase 4 gate)*
**Objective**: Exercise the Strategy Builder's modularity. Add a second signal trigger using the existing framework contract. Validate that the backtesting engine handles multiple strategies without pipeline changes.
**Deliverables**:
- Second strategy with a different signal trigger (candidate from existing indicator library)
- Backtesting engine confirms: only a calibration pass is required per new instrument/timeframe — no code changes
- At least one auto_go candidate on a second strategy
**Gate**: second strategy produces at least one paper-tradeable candidate via the same pipeline.
---
### Phase 6 — Analytical Intelligence and Live Trading *(requires Phase 5 gate + 60 days paper trading)*
**Objective**: Build the analytics layer and transition from demo to controlled live account deployment.
**Deliverables**:
- Live vs backtest comparison engine: per-candidate tracking of win rate, expectancy, drawdown against backtest MC distributions
- Regime detection: real-time classification using the same Choppiness/DPO logic embedded in the strategies
- Trading journal UI: structured log queryable by candidate, regime, session, date range
- Drawdown circuit breaker: automated signal pause when live drawdown exceeds MC worst_dd threshold
- Real account deployment at 10% of target risk, scaling up in 25% increments at 30-day intervals with operator sign-off
**Gate**: 30-day live Sharpe ratio > 0; circuit breaker tested and validated before enabling on real account.
---
## Key Parallel Tracks
Two workstreams can proceed simultaneously right now without blocking each other:
**Track A** (running now): Full-history calibration sequence — applying code changes, running calibration_v2, calculating new `_SIGMOID_SCALE = 310.0`, running full pipeline calibration, then overnight production run.
**Track B** (starting now): `broker_support` bug fixes → empirical demo history test → InstrumentID lookup → clean manual tracker cycle.
These tracks converge at Phase 2 (automated paper trading), which requires both V1 candidates confirmed on full history (Track A gate) and a working signal bridge (Track B completion).
---
## Open Items Feeding Into Phase 3 (V2 Backlog)
| ID | Description | Priority |
|----|-------------|----------|
| V2-ARCH | RawDataStore + WindowSlicer + SignalCache architecture redesign | P0 |
| V2-SHM | Shared memory implementation (Windows spawn-safe, named blocks) | P0 |
| V2-CALLABLE | `run_backtest(config) → result` pure function interface for V3 | P0 |
| V2-RAR | Dimensionless normalisation via Rolling Annual Range | P1 |
| V2-DYN-WFO | Dynamic window generation from `data_range + window_size` | P1 |
| RSI-SENS-2 | Remove RSI from search space — 6 runs confirmed zero signal | P2 |
| Time-WIN | Intra-day time window analysis (best/worst performance periods) | P2 |
| FILTER-DISC | Filter discovery mode — which combinations survive across auto_go candidates | P2 |
| B9N-001 | scenario.py systematic ct.get() fix for all constraint fields | P3 |
| CAL-01 | normalisation_freq_ref_trades_per_week 20.0 → 50.0 | P3 |
| RR-CEILING-2 | Revert safe zone rr_target.max 8.5 → 7.0 in next YAML | P3 |
| B8C-002/003 | report_generator.py cosmetic HTML issues | P3 |
---
## Decision Log
| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-03-08 | Freeze V2, prioritise broker integration and paper trading | Live data is more valuable than better backtest machinery at this stage. V2 priorities should be evidence-based, not speculative. |
| 2026-03-08 | Phase 0 (broker fixes) runs parallel to Phase 1 (full-history) | Independent workstreams with no shared dependencies until Phase 2. |
| 2026-03-08 | Time window analysis and filter discovery added to V2 scope | Both are better specified and validated with live paper trading data available. |
| 2026-03-08 | eToro demo/real symmetry confirmed | No architectural separation between paper trading and live trading code. One config flag at go-live. |
| 2026-03-10 | V2 architecture redesign scoped: RawDataStore + WindowSlicer + SignalCache | V1 DataLoader violates single responsibility (loads + slices). V1 recomputes signals 231× per run on identical data. V2 redesign eliminates OOM constraint (max_workers: 2) and reduces signal computation by ~90%. |
| 2026-03-10 | V2 must be V3-ready: backtester as callable function, config as dataclass, stateless evaluation | V3 meta-optimiser requires programmatic backtester invocation. V2 decisions made now must not require refactoring for V3. |
| 2026-03-10 | V3 = Strategy Setup Builder (meta-optimiser over configuration space) | V2 optimises parameters within a fixed setup. V3 optimises setups — answers "is this strategy architecture tradeable at all?" before parameter optimisation. Two-phase: broad discovery (unconstrained) then confirmation (full pipeline + temporal stability check). |