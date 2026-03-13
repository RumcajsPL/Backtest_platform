# CTP_ROADMAP.md — Complementary Trading Platform Strategic Roadmap
# Updated: 2026-03-13 | Version 1.3
---
## Phase Status
| Phase | Description | Status |
|-------|-------------|--------|
| 0 | Broker connectivity foundation | ✅ COMPLETE |
| 1 | Backtesting full history + V1.1 calibration | ✅ COMPLETE |
| 2 | Automated paper trading via eToro demo | 🔄 IN PROGRESS |
| 3 | Backtesting V2 (informed by Phase 2 live data) | 🔲 PLANNED |
| 4 | Backtesting V3: Strategy Setup Builder | 🔲 PLANNED |
| 5 | Multi-strategy and multi-asset | 🔲 PLANNED |
| 6 | Analytical intelligence and live trading | 🔲 PLANNED |
---
## Phase 0 — Broker Connectivity Foundation ✅
All broker_support bugs fixed. Demo history endpoint confirmed. DAX instrument confirmed (GER40, id=32). One full position lifecycle confirmed via API.
## Phase 1 — Backtesting Full History ✅
`_SIGMOID_SCALE=310.0` confirmed. Production run on `backtest_V1_01.yaml` complete. Primary candidate `c424a0e04327` confirmed across 13 WFO windows including 2024 stress period.
## Phase 2 — Automated Paper Trading 🔄
**Gate:** 20+ trades executed automatically; journal populated with correct P&L data.
Progress:
- ✅ Full pipeline architecture delivered (v3), 90/90 unit + 63/63 integration tests
- ✅ Stage 1 dry-run confirmed on live API (2026-03-13)
- ✅ Stage 2 --place-order path confirmed (pipeline + guards)
- ✅ run_signal_loop.py operational
- 🔲 First live demo order placed
- 🔲 Tracker loop confirms close and journal entry
- 🔲 20-trade gate
## Phase 3 — Backtesting V2 🔲
*(Start after Phase 2 data available — V2 priorities must be evidence-based)*
Architecture redesign: RawDataStore + WindowSlicer + SignalCache eliminates V1's 231× signal recomputation and OOM constraint (max_workers: 2 → 6+).
Key deliverables: V2-RAR (dimensionless normalisation), RSI removal from search space, dynamic WFO windows, `run_backtest(config) → result` callable interface (V3-readiness), break-even + trailing stop as optional backtestable strategy features.
**Gate:** V2 produces auto_go candidates on a second instrument without per-instrument recalibration.
## Phase 4 — V3 Strategy Setup Builder 🔲
*(Requires Phase 3 gate)*
Meta-optimiser over configuration space (timeframes, filter sets, risk structures). Two-phase: broad discovery → confirmation with temporal stability check (last N months ≥ 70% of full-history average).
**Gate:** V3 identifies a tradeable configuration on a different timeframe without operator guidance.
## Phase 5 — Multi-Strategy and Multi-Asset 🔲
*(Requires Phase 4 gate)*
Second signal trigger via existing framework contract. At least one auto_go candidate on a second strategy.
## Phase 6 — Live Trading 🔲
*(Requires Phase 5 gate + 60 days paper trading)*
Live vs backtest comparison engine, drawdown circuit breaker, real account deployment at 10% risk scaling in 25% increments with operator sign-off.
---
## Open Backlog
### Phase 2
| ID | Description | Priority |
|----|-------------|----------|
| PHASE-2-STAGE2 | First live demo order confirmed | P0 |
| RESOLVER-FIELDS | InstrumentResolver missing 'fields' param + exact-match | P1 |
### Phase 3 (V2)
| ID | Description | Priority |
|----|-------------|----------|
| V2-ARCH | RawDataStore + WindowSlicer + SignalCache redesign | P0 |
| V2-SHM | Shared memory (Windows spawn-safe named blocks) | P0 |
| V2-CALLABLE | `run_backtest(config) → result` interface | P0 |
| V2-RAR | Dimensionless normalisation via Rolling Annual Range | P1 |
| V2-DYN-WFO | Dynamic window generation | P1 |
| RSI-SENS-2 | Remove RSI from search space | P2 |
| Time-WIN | Intra-day time window analysis | P2 |
| FILTER-DISC | Filter discovery mode | P2 |
---
## Decision Log
| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-03-08 | Freeze V2, prioritise broker integration | Live data answers "does the edge hold?" — more valuable than better backtest machinery now. |
| 2026-03-08 | eToro demo/real symmetry confirmed | One config flag at go-live. No separate code paths. |
| 2026-03-10 | V2 architecture redesign scoped | V1 recomputes signals 231× on identical data. V2 eliminates OOM, reduces computation ~90%. |
| 2026-03-10 | V2 must be V3-ready | V3 meta-optimiser requires programmatic backtester invocation — decisions made now must not require V3 refactoring. |
| 2026-03-13 | Integration test suite before first dry-run | 63 integration tests confirmed pipeline seams before touching live API. |
| 2026-03-13 | run_signal_loop.py: one-shot loop for Stage 2 | Simplest supervised path to first order. Full automation loop deferred until Stage 2 confirmed. |