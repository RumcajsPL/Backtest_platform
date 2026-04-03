# CONTEXT.md — CTP V2 Backtester
# Session continuity document — updated every session close
# Owner: Claude.ai | Version: 0.1 (project initialisation) | Date: 2026-04-02
---

## CURRENT PROJECT STATE

**Phase**: M0 confirmed — V2 code work may begin
**Active sprint**: None — Session 1 to be planned
**Last session**: 2026-04-02 — Project documentation initialisation (no code changes)
**Next session goal**: Session 1 — V1 due diligence (Agent C) + environment setup

**Phase 2 status**: 30+ backtester runs completed. 4 candidates in paper trading
(2+ weeks running). 3 candidates in observation. Gate confirmed.

---

## ENVIRONMENT STATUS

```
Production:  E:\Trading\Backtest_platform\          [LIVE — 4× paper trading loops running]
Staging:     E:\Trading\Backtest_platform_staging\  [Not yet initialised for V2]
Sandbox:     E:\Trading\Backtest_platform_sandbox\  [Not yet initialised for V2]
Data:        data/processed/ohlcv/ — read-only, shared; primary consumer is strategy pipeline
V1 tests:    tests\backtesting — inventory pending (Session 1 task)
```

---

## BACKLOG — CURRENT PRIORITY ORDER

*(Full backlog with rationale in PLAN.md — this section: current sprint + immediate next)*

### Immediate — Session 1 (due diligence first, then environment)
```
DD-001   [P0]  Agent C: audit DataLoader — which module owns it (strategy vs backtester),
               exact slicing/caching additions made ad-hoc, what backtester reads directly
DD-002   [P0]  Agent C: audit consistency_scorer.py — all hardcoded constants
               (_SIGMOID_SCALE etc.), how many times recalibrated, where changed in source
DD-003   [P0]  Agent C: audit max_workers — where set, what constraints it; confirm whether
               4→6 gives measurable gain under current V1 architecture
DD-004   [P0]  Agent C: audit OHLCV data flow — processed/ folder structure, what path
               data takes from file to backtester evaluation
ENV-001  [P0]  Owner: initialise staging (xcopy from production)
ENV-002  [P0]  Owner: initialise sandbox (src + tests + configs only)
TEST-001 [P0]  Agent C: V1 test inventory — list all files, classify REUSE/ADAPT/RETIRE
```

### After due diligence output received by Claude.ai
```
DEC-007  Resolve DataLoader boundary decision (informed by DD-001)
DEC-008  Resolve max_workers target decision (informed by DD-003)
TEST-002 Claude.ai: produce V2 test plan from TEST-001 output
ARCH-001 Agent A: RawDataStore design (after DEC-007 resolved)
ARCH-002 Agent A: WindowSlicer design (after DEC-007 resolved)
```

---

## OPEN DECISIONS

*Decisions pending — to be resolved before implementation of affected modules*

| ID | Question | Affects | Status |
|----|----------|---------|--------|
| DEC-001 | SignalCache eviction: fixed size? LRU? All-or-nothing per run? | SignalCache | Open |
| DEC-002 | Intelligent cache manager scope: V2 or V3? (measurable E2E gain required) | SignalCache | Open |
| DEC-003 | Break-even and trailing stop: include in V2 or defer to V3? | TradeSimulator | Open |
| DEC-004 | Time session as configurable setting: V2 scope or V3? | Config, Strategy | Open |
| DEC-005 | Avg P&L vs trade count as constraint: change in V2? | Scenario, Fitness | Open |
| DEC-006 | ~~RSI removal confirmed~~ | SearchSpace | Resolved |
| DEC-007 | DataLoader boundary: V2 own data layer or refactor existing? (after DD-001) | ARCH-001/002 | Open |
| DEC-008 | max_workers target: 6 confirmed safe under shm? Profile to verify gain over 4 (after DD-003) | ARCH-002 | Open |

---

## ARCHITECTURE STATE

*What has been decided vs what is still V1 (to be migrated or replaced)*

### Confirmed V2 decisions
- RSI (`rsi_period`, `rsi_overbought`, `rsi_oversold`) removed from search space
- DAX-specific normalisation constants replaced by V2-RAR (Rolling Annual Range)
- `max_workers` constraint removed (shared memory architecture)
- Single Responsibility refactor: `DataLoader` → `RawDataStore` + `WindowSlicer`
- Signal computation extracted to `SignalCache`
- `run_backtest(config) → result` callable interface required (V3-readiness)
- Dynamic WFO window generation (replaces hardcoded window list)

### Still V1 (unchanged in V2 unless explicitly decided)
- All contracts in `contracts.py` (frozen dataclasses) — extend, don't replace
- `CandidateStore` (SQLite WAL, single-writer queue) — reuse architecture
- GA engine, fitness evaluation, verdict logic — reuse with minimal changes
- MC engine, sensitivity analysis, report generator — reuse

### V2 module inventory (target)
```
RawDataStore       → NEW (replaces DataLoader load responsibility)
WindowSlicer       → NEW (slice responsibility, shared memory)
SignalCache        → NEW (signal generation responsibility, cached)
TradeSimulator     → UNCHANGED from V1 (evaluation only)
BacktestConfig     → NEW contract (programmatic config)
BacktestResult     → NEW contract (structured result for V3)
DynamicWindowGen   → NEW (replaces hardcoded window list)
V2-RAR normaliser  → NEW (replaces DAX constants)
run_backtest()     → NEW callable entry point
```

---

## AGENT PERFORMANCE NOTES

*Updated each session — basis for periodic role review*

| Agent | Last evaluated | Status | Notes |
|-------|---------------|--------|-------|
| Agent A (Claude Code) | Not yet | — | Not yet started |
| Agent B (Codex) | Not yet | — | Not yet started |
| Agent C (Qwen) | Not yet | — | Not yet started |

---

## RISKS AND BLOCKERS

| ID | Risk | Impact | Mitigation | Status |
|----|------|--------|------------|--------|
| R-001 | Phase 2 gate delayed — V2 cannot start | High | Monitor Phase 2 progress; prep documentation in interim | Active |
| R-002 | V1 tests require significant rework | Medium | Test inventory in Session 1 sizes the effort | Open |
| R-003 | Shared memory complexity on Windows spawn | Medium | Proven pattern documented in ARCHITECTURE.md §13; prototype early | Open |
| R-004 | SignalCache eviction strategy unclear | Low | DEC-001 to resolve before SignalCache implementation | Open |

---

## KEY REFERENCE — DO NOT SEARCH AGAIN

*Facts confirmed from documents — avoid re-looking these up each session*

- Phase 2 gate: CONFIRMED. 30+ runs, 4 candidates in paper trading, 3 in observation.
- V1 `_SIGMOID_SCALE`: NOT a fixed value. Was recalibrated multiple times across runs;
  each recalibration required modifying `src/backtesting/wfo/consistency_scorer.py` directly.
  The value 310.0 appeared in one configuration but was not stable. V2-RAR eliminates this.
- V1 `max_workers`: confirmed stable at 2 and 4. 6 tested without issues but no measurable
  perf difference between 2 and 4 observed yet. To be profiled under shm architecture (DEC-008).
- V1 signal recomputation: 231× for 33 candidates × 7 windows on identical data
- V2 memory target: ~120MB peak (6 workers × ~20MB) vs V1 ~5.38GB
- DAX instrument: GER40, id=32
- Data path: `data/processed/ohlcv/` — CSV/Parquet files, read-only, shared staging/production
- `DataLoader` is a strategy pipeline module, adapted ad-hoc for backtester use — boundary TBD (DEC-007)
- Production has 4× `run_demo_trading.py` instances — never interrupted by V2 work
- `trade.pnl_points` is correct; `trade.pnl` does not exist
- Both `strategy_runner._PARAM_KEY_MAP` and `yaml_generator._PARAM_MAP` must always be updated together

---

## NEXT SESSION PLAN (Session 1)

**Goal**: V1 due diligence + environment setup

```
Task 1 [Agent C — due diligence, runs in parallel]:
  DD-001: Read DataLoader source file. Report:
    - Which package/module owns it (strategy or backtester?)
    - Exact methods added ad-hoc for backtester (slicing, caching)
    - What files does the backtester read directly vs via DataLoader?
  DD-002: Read consistency_scorer.py. Report:
    - All hardcoded constants (_SIGMOID_SCALE, _MAX_EXPECTED_DRAWDOWN, _MAX_EXPECTED_VARIANCE)
    - Current values and any comments indicating recalibration history
    - Any other hardcoded instrument-specific values in the file
  DD-003: Search codebase for max_workers. Report:
    - Every file where max_workers is set or read
    - What constraint currently limits it (comment, config, OOM guard?)
  DD-004: Read data loading path. Report:
    - What files live in data/processed/ohlcv/ (instrument names, TFs, formats)
    - Exact import chain from OHLCV file → backtester evaluation (file by file)
  Output for all: structured report → Owner → Claude.ai

Task 2 [Owner — manual]:
  ENV-001: xcopy E:\Trading\Backtest_platform\ E:\Trading\Backtest_platform_staging\ /E /I /H
  ENV-002: xcopy src\ + tests\ + configs\ to sandbox only

Task 3 [Agent C — after ENV-001]:
  TEST-001: Inventory all test files in staging:
    - Each file path and test count
    - Classify each: REUSE / ADAPT / RETIRE for V2
    - Flag any test importing from modules being replaced
  Output: structured list → Owner → Claude.ai

Task 4 [Claude.ai — after DD output + TEST-001 received]:
  - Resolve DEC-007 (DataLoader boundary) from DD-001
  - Resolve DEC-008 (max_workers target) from DD-003
  - Produce V2 test plan (TEST-002) from TEST-001
  - Author ARCH-001 instruction for Agent A (RawDataStore design)
```