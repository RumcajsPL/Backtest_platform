# CTP_ROADMAP.md — Complementary Trading Platform Strategic Roadmap
# Scope: Platform master roadmap — all sub-projects and phases
# Owner: Claude.ai + Owner | Version: 2.0 | Date: 2026-04-18
# Environment: Production (E:\Trading\Backtest_platform\)
---

## PLATFORM VISION

The Complementary Trading Platform (CTP) automates the full lifecycle of a trading strategy:
from idea capture through development, backtesting, paper trading, and live deployment,
with continuous evaluation and a disciplined retirement/rejection protocol.

The workflow runs as parallel loops — multiple strategies can be at different lifecycle
stages simultaneously:

```
[Strategy Forgery]   → Idea capture, concept validation, strategy coding (Python, backtest-ready)
      ↓
[Backtester]         → Heavy backtesting (search, optimisation, WFO, MC, sensitivity)
      ↓                  If not relevant → Rejected Strategy Repository
[Paper Trading]      → Automated demo trading, performance vs backtest comparison
      ↓                  If not relevant → back to Dev/Backtest loop OR Rejected Repository
[Live Trading]       → Strictly controlled deployment, continuous evaluation
      ↓                  If not relevant → back to Dev/Backtest loop OR Rejected Repository
[Strategy Repository]→ Accepted (live) | Rejected (archived with reason)
```

**Parallel operation**: Multiple strategies traverse this pipeline simultaneously.
Each stage has its own automation layer. The Backtester is the central analytical engine.

---

## DOCUMENT HIERARCHY

```
CTP_ROADMAP.md                                ← This document. Platform master.
  └── docs/backtesting/V2/VX_ROADMAP.md       ← Backtester master. All V2+ roadmap items.
        └── docs/backtesting/V2/V2.1_PLAN.md  ← Sprint plan (V2.1 scope only).
        └── docs/backtesting/V2/V2.2_PLAN.md  ← (future)
```

When detail is needed, navigate down. When direction is unclear, navigate up.
No content is duplicated between levels. Each document owns its scope exclusively.

---

## SUB-PROJECTS

| Sub-project | Scope | Status | Repository |
|-------------|-------|--------|------------|
| CTP Core | Broker integration, paper trading, live trading pipeline | Active | Backtest_platform |
| Backtester | V1 complete, V2 complete, VX roadmap active | Active | Backtest_platform |
| Strategy Forgery | Idea capture, concept validation, strategy coding | Early development | pine_works |

**Strategy Forgery** (pine_works sub-project): Handles the first stage of the platform
lifecycle — capturing new strategy ideas, validating concepts, and producing Python strategy
code ready for backtesting. Not yet integrated with CTP. Keyword registered for future
integration planning. When integrated, Strategy Forgery will be the entry point to the
full lifecycle loop above.

---

## PHASE STATUS

| Phase | Description | Status |
|-------|-------------|--------|
| 0 | Broker connectivity foundation | ✅ COMPLETE |
| 1 | Backtesting full history + V1.1 calibration | ✅ COMPLETE |
| 2 | Automated paper trading via eToro demo | 🔄 IN PROGRESS |
| 3 | Backtesting V2 — multi-instrument, no recalibration | ✅ COMPLETE (Session 16) |
| 4 | Backtesting VX — agentic pipeline + strategy builder | 🔲 PLANNED |
| 5 | Multi-strategy and multi-asset | 🔲 PLANNED |
| 6 | Platform integration — full lifecycle automation | 🔲 PLANNED |
| 7 | Live trading — controlled deployment | 🔲 PLANNED |

---

## PHASE 0 — BROKER CONNECTIVITY ✅
All broker_support bugs fixed. Demo history endpoint confirmed. DAX instrument confirmed
(GER40, id=32). One full position lifecycle confirmed via API.

---

## PHASE 1 — BACKTESTING V1 ✅
`_SIGMOID_SCALE=310.0` confirmed. Production run on `backtest_V1_01.yaml` complete.
Primary candidate `c424a0e04327` confirmed across 13 WFO windows including 2024 stress period.

---

## PHASE 2 — AUTOMATED PAPER TRADING 🔄
**Gate:** 20+ trades executed automatically; journal populated with correct P&L data.

Progress:
- ✅ Full pipeline architecture delivered (v3), 90/90 unit + 63/63 integration tests
- ✅ Stage 1 dry-run confirmed on live API (2026-03-13)
- ✅ Stage 2 --place-order path confirmed (pipeline + guards)
- ✅ run_signal_loop.py operational
- 🔲 First live demo order placed
- 🔲 Tracker loop confirms close and journal entry
- 🔲 20-trade gate

---

## PHASE 3 — BACKTESTING V2 ✅ COMPLETE (Session 16, promoted Session 28)
**Gate met:** V2 produces auto_go candidates on a second instrument (NASDAQ, VAL-001)
without per-instrument recalibration. V2 promoted to production.

Key deliverables: RawDataStore + WindowSlicer + SignalCache, V2-RAR normalisation,
RSI removal, full 7-stage pipeline, OPT-02 shared memory, RUN-005 validation (8 auto_go).

Full V2 roadmap items and post-V2 intelligence: see VX_ROADMAP.md.

---

## PHASE 4 — BACKTESTING VX — AGENTIC PIPELINE + STRATEGY BUILDER 🔲
*(Phase 3 gate met)*

**Vision**: The backtester becomes a fully agentic analytical engine with a component-based
strategy builder:
- Agentic scenario-based processing: automated run sequencing, result evaluation, paper trading push
- Strategy builder: plug-in indicator architecture with configurable optional components
  (session filter, entry filters, SL/TP, trailing stop, break-even, partial closure)
- Cross-run analytics database: all runs append to shared persistent backtest.db
- Meta-optimiser: programmatic outer loop over BacktestConfig parameter space
- Selective component execution: stages use precision appropriate to their measurement type

**Gate:** Backtester autonomously identifies a tradeable configuration on a new instrument
or timeframe without operator guidance and pushes the candidate to paper trading.

Full roadmap items, version packaging, and sprint plans: see VX_ROADMAP.md → V2.1_PLAN.md.

---

## PHASE 5 — MULTI-STRATEGY AND MULTI-ASSET 🔲
*(Requires Phase 4 gate)*

Second signal trigger via existing framework contract. Cross-instrument portfolio analysis.
Multi-strategy simultaneous paper trading. At least one auto_go on a second strategy type.

---

## PHASE 6 — PLATFORM INTEGRATION — FULL LIFECYCLE AUTOMATION 🔲
*(Requires Phase 4 gate + Strategy Forgery integration ready)*

Integration of Strategy Forgery → Backtester → Paper Trading as an automated loop.
Strategy lifecycle state machine: dev → backtest → paper → live → retired/rejected.
Strategy Repository (accepted and rejected with metadata and reason codes).
Advanced analytics across all lifecycle stages.
Parallel operation: multiple strategies at different stages simultaneously.

---

## PHASE 7 — LIVE TRADING 🔲
*(Requires Phase 5 gate + 60 days paper trading)*

Live vs backtest comparison engine. Drawdown circuit breaker. Real account deployment
at 10% risk scaling in 25% increments with operator sign-off.

---

## OPEN BACKLOG

### Phase 2
| ID | Description | Priority |
|----|-------------|----------|
| PHASE-2-STAGE2 | First live demo order confirmed | P0 |
| RESOLVER-FIELDS | InstrumentResolver missing 'fields' param + exact-match | P1 |

### Phase 4 (Backtester VX) — summary only
Full item list in VX_ROADMAP.md. Sprint scope in V2.1_PLAN.md.
| Level | Items |
|-------|-------|
| V2.1 (no contract changes) | Diagnostics, config cleanup, query tools, tech debt |
| V3 (contract changes) | Dynamic normalisation, sensitivity span fix, strategy builder, trade sim extensions |
| V3+ / Platform | Meta-optimiser, live feedback loop, multi-instrument simultaneous |

---

## DECISION LOG

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-03-08 | Freeze V2, prioritise broker integration | Live data answers "does the edge hold?" — more valuable than better backtest machinery now. |
| 2026-03-08 | eToro demo/real symmetry confirmed | One config flag at go-live. No separate code paths. |
| 2026-03-10 | V2 architecture redesign scoped | V1 recomputes signals 231× on identical data. V2 eliminates OOM, reduces computation ~90%. |
| 2026-03-10 | V2 must be V3-ready | V3 meta-optimiser requires programmatic backtester invocation. |
| 2026-03-13 | Integration test suite before first dry-run | 63 integration tests confirmed pipeline seams before touching live API. |
| 2026-03-13 | run_signal_loop.py: one-shot loop for Stage 2 | Simplest supervised path to first order. Full automation loop deferred until Stage 2 confirmed. |
| 2026-04-18 | Roadmap hierarchy established | CTP_ROADMAP (platform master) → VX_ROADMAP (backtester master) → V2.x_PLAN (sprint). Prevents loading irrelevant documents each session. |
| 2026-04-18 | Strategy Forgery registered as CTP sub-project | pine_works project covers lifecycle stage 1 (idea capture → strategy code). Integration planned Phase 6. |
| 2026-04-18 | Phase 4 framed as agentic backtester + strategy builder | Backtester becomes autonomous analytical engine; strategy becomes a component-based builder. Detail in VX_ROADMAP. |
| 2026-04-18 | LTF precision policy established | 1s LTF precision preserved as default. Stage-appropriate precision allowed (e.g. sensitivity may omit LTF if memory requires and relative delta is preserved). Tick-level precision a future option. |
