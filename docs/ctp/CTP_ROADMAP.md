# CTP Evolution Roadmap
**Complementary Trading Platform — Strategic Summary**
**Date**: 2026-03-08 | Version 1.1 (phase order revised from initial document)

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
- Calibration v2 run completed; new `_SIGMOID_SCALE` calculated
- Full-history production run completed overnight
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
**Objective**: Redesign the backtesting engine based on what live paper trading reveals. V2 priorities are determined by live evidence, not speculation.

**Expected deliverables** (may be revised based on paper trading results):
- **V2-RAR**: Replace DAX-specific normalisation constants with dimensionless Rolling Annual Range fractions. Enables multi-asset backtesting without per-instrument recalibration.
- **RSI removal**: Remove `rsi_period`, `rsi_overbought`, `rsi_oversold` from search space (RSI-SENS-2 — 6 consecutive zero-delta runs confirmed)
- **Time window analysis**: Identify intra-day periods where the strategy performs best/worst. Backtested findings cross-validated against live paper trade times.
- **Filter discovery mode**: Report which filter combinations are consistently active across all auto_go candidates. Answers "is RSI helping?" systematically rather than by inspection.
- **Dynamic WFO windows**: Replace hardcoded window list with `data_range + window_size` parameters. Required for multi-asset support.
- **B9N-001**: Systematic `scenario.py` constraint loader fix — all fields use `ct.get()` with documented defaults.
- **CAL-01**: Raise `normalisation_freq_ref_trades_per_week` to 50.0.

**Gate**: V2 produces auto_go candidates on a second instrument (DAX + one other) using the same pipeline without instrument-specific recalibration.

---

### Phase 4 — Multi-Strategy and Multi-Asset *(requires Phase 3 gate)*
**Objective**: Exercise the Strategy Builder's modularity. Add a second signal trigger using the existing framework contract. Validate that the backtesting engine handles multiple strategies without pipeline changes.

**Deliverables**:
- Second strategy with a different signal trigger (candidate from existing indicator library)
- Backtesting engine confirms: only a calibration pass is required per new instrument/timeframe — no code changes
- At least one auto_go candidate on a second strategy

**Gate**: second strategy produces at least one paper-tradeable candidate via the same pipeline.

---

### Phase 5 — Analytical Intelligence and Live Trading *(requires Phase 4 gate + 60 days paper trading)*
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

**Track A** (running now): Full-history calibration sequence — applying code changes, running calibration_v2, calculating new `_SIGMOID_SCALE`, running full-history production overnight.

**Track B** (starting now): `broker_support` bug fixes → empirical demo history test → InstrumentID lookup → clean manual tracker cycle.

These tracks converge at Phase 2 (automated paper trading), which requires both V1 candidates confirmed on full history (Track A gate) and a working signal bridge (Track B completion).

---

## Open Items Feeding Into Phase 3 (V2 Backlog)

| ID | Description | Priority |
|----|-------------|----------|
| V2-RAR | Dimensionless normalisation via Rolling Annual Range | P1 |
| RSI-SENS-2 | Remove RSI from search space — 6 runs confirmed zero signal | P2 |
| DYN-WFO | Dynamic window generation from data_range + window_size | P2 |
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