# PLAN.md — CTP V2 Backtester — Project Plan
# Scope: Backlog, milestones, open decisions, task detail
# Status and session state: see CONTEXT.md
# Owner: Claude.ai | Version: 1.0 | Date: 2026-04-02
---

## PROJECT GATE

**V2 start gate (M0)**: Phase 2 gate — CONFIRMED.
30+ backtester runs completed under a strict plan (different strategies, TFs, filter configs).
4 candidates in paper trading (running 2+ weeks). 3 candidates in observation.
V2 code work may begin.

**V2 complete gate**: V2 produces auto_go candidates on a second instrument without
per-instrument recalibration.

---

## MILESTONES

| ID | Milestone | Depends on | Status |
|----|-----------|------------|--------|
| M0 | Phase 2 gate confirmed | Phase 2 completion | **CONFIRMED** |
| M-DD | V1 due diligence complete (architecture audit via Agent C) | M0 | Not started |
| M1 | Environment setup + V1 test inventory complete | M-DD | Not started |
| M2 | V2 contracts defined (BacktestConfig, BacktestResult, shm contracts) | M1 | Not started |
| M3 | RawDataStore + WindowSlicer implemented + tested | M2 | Not started |
| M4 | SignalCache implemented + tested | M3 | Not started |
| M5 | TradeSimulator adapted for shm input | M4 | Not started |
| M6 | `run_backtest()` callable interface integrated | M5 | Not started |
| M7 | Dynamic WFO window generator implemented | M2 | Not started |
| M8 | V2-RAR normalisation implemented (replaces hardcoded constants) | M2 | Not started |
| M9 | RSI removal from search space | M2 | Not started |
| M10 | Full pipeline integration test on DAX | M6, M7, M8, M9 | Not started |
| M11 | Second instrument validation (V2 gate) | M10 | Not started |
| M12 | Open backlog items resolved (P2 items) | M10 | Not started |

---

## BACKLOG

### P0 — Critical path (blocks milestone progress)

| ID | Description | Milestone | Agent | Status |
|----|-------------|-----------|-------|--------|
| DD-001 | V1 due diligence: audit DataLoader and its relationship to strategy pipeline vs backtester | M-DD | C | Not started |
| DD-002 | V1 due diligence: audit consistency_scorer.py — all hardcoded constants, recalibration points | M-DD | C | Not started |
| DD-003 | V1 due diligence: audit max_workers usage — where set, what limits it, profiling data if any | M-DD | C | Not started |
| DD-004 | V1 due diligence: audit OHLCV data pipeline — processed/ folder structure, what backtester reads directly vs via strategy | M-DD | C | Not started |
| ENV-001 | Initialise staging environment (xcopy from production) | M1 | Owner | Not started |
| ENV-002 | Initialise sandbox (src + tests + configs only) | M1 | Owner | Not started |
| TEST-001 | V1 test inventory: list all test files, classify REUSE/ADAPT/RETIRE | M1 | C | Not started |
| ARCH-001 | RawDataStore: contract definition + module design doc | M2 | A | Not started |
| ARCH-002 | WindowSlicer: contract definition + shared memory design | M2 | A | Not started |
| ARCH-003 | SignalCache: contract definition + cache key design | M2 | A | Not started |
| ARCH-004 | BacktestConfig + BacktestResult contracts (V3-readiness) | M2 | A | Not started |
| IMPL-001 | RawDataStore implementation + unit tests | M3 | A | Not started |
| IMPL-002 | WindowSlicer implementation + shared memory + unit tests | M3 | A | Not started |
| IMPL-003 | SignalCache implementation + unit tests | M4 | A | Not started |
| IMPL-004 | TradeSimulator adaptation for shm input | M5 | A | Not started |
| IMPL-005 | `run_backtest(config) → result` integration | M6 | A | Not started |
| INT-001 | Full pipeline integration test on DAX | M10 | A+C | Not started |
| VAL-001 | Second instrument validation run | M11 | A+C | Not started |

**Note on DataLoader**: `DataLoader` is a module of the strategy pipeline, not the
backtester. It was adapted ad-hoc to serve the backtester (slicing, caching). DD-001
must clarify the exact boundary before V2 design freezes — specifically whether V2
should introduce its own data access layer or extend/refactor the existing one.

### P1 — Important (not on immediate critical path)

| ID | Description | Milestone | Agent | Status |
|----|-------------|-----------|-------|--------|
| ARCH-005 | Dynamic WFO window generator design | M7 | A | Not started |
| IMPL-006 | Dynamic WFO window generator implementation | M7 | A | Not started |
| V2-RAR-1 | V2-RAR design: Rolling Annual Range normalisation spec | M8 | A | Not started |
| V2-RAR-2 | V2-RAR implementation (replaces _SIGMOID_SCALE etc.) | M8 | A | Not started |
| RSI-001 | Remove RSI from search space + parameter_space + config | M9 | A | Not started |
| TEST-002 | New V2 test plan (output of TEST-001 + Claude.ai review) | M1 | Claude.ai | Not started |
| TEST-003 | Port/adapt reusable V1 tests for V2 modules | M3+ | A | Not started |

### P2 — Planned (post-core architecture)

| ID | Description | Source | Status |
|----|-------------|--------|--------|
| B9N-001 | scenario.py: systematic ct.get() fix for all constraint fields | V1 backlog | Not started |
| CAL-01 | normalisation_freq_ref_trades_per_week 20.0 → 50.0 | V1 backlog | Not started |
| RR-CEILING-2 | Revert safe zone rr_target.max 8.5 → 7.0 | V1 backlog | Not started |
| B8C-002/003 | report_generator.py cosmetic HTML issues | V1 backlog | Not started |
| Time-WIN | Intra-day time window analysis | EVOLUTION_PIPELINE | Not started |
| FILTER-DISC | Filter discovery mode | EVOLUTION_PIPELINE | Not started |

### P3 — Under consideration (requires decision before scoping)

| ID | Description | Decision needed | Status |
|----|-------------|-----------------|--------|
| CACHE-MGR | Intelligent cache manager (anticipatory eviction) | DEC-001, DEC-002 | Open |
| BREAK-EVEN | Break-even mechanism design | DEC-003 | Open |
| TRAIL-STOP | Trailing stop mechanism design | DEC-003 | Open |
| TIME-SESSION | Time session as configurable setting | DEC-004 | Open |
| TRADE-CONSTRAINT | Avg P&L vs trade count as primary constraint | DEC-005 | Open |

---

## OPEN DECISIONS

Decisions that must be resolved before the affected backlog items can be implemented.
Claude.ai resolves these with Owner input, informed by due diligence output where needed.

| ID | Question | Affects | Target resolution |
|----|----------|---------|------------------|
| DEC-001 | SignalCache eviction: fixed size? LRU? All-or-nothing per run? | CACHE-MGR, ARCH-003 | Before IMPL-003 |
| DEC-002 | Intelligent cache manager scope: V2 or V3? Rule: measurable E2E perf gain required | CACHE-MGR | Before ARCH-003 finalised |
| DEC-003 | Break-even + trailing stop: V2 scope or V3? | BREAK-EVEN, TRAIL-STOP | Before M6 |
| DEC-004 | Time session as configurable setting: V2 scope or V3? | TIME-SESSION | Before M10 |
| DEC-005 | Avg P&L vs trade count as primary constraint: change in V2? | Trade-CONSTRAINT | Before INT-001 |
| DEC-006 | ~~RSI removal~~ | ~~Confirmed~~ | Resolved: removed |
| DEC-007 | DataLoader boundary: does V2 introduce its own data access layer, or refactor/extend existing? Informed by DD-001 | ARCH-001, ARCH-002 | After DD-001 |
| DEC-008 | max_workers target for V2: 6 confirmed safe? Profile under shared memory to verify perf gain over 4. Informed by DD-003 | ARCH-002 | After DD-003 |

---

## CHANGE MANAGEMENT PROCESS

### Principle
Every change to project scope must have a precisely defined expected outcome that delivers
measurable added value to the platform. If that outcome cannot be confirmed during the
design phase, the change is abandoned.

### Process

**Step 1 — Change proposal**: Owner or Claude.ai identifies a potential change.
Recorded as a new backlog item (P3 initially) with: description, expected outcome,
measurability criterion (how we confirm the outcome), estimated effort category (S/M/L).

**Step 2 — Impact assessment**: Agent C searches codebase for affected modules and
call sites. Claude.ai assesses: does this change conflict with confirmed architecture
decisions? Does it require a new open decision?

**Step 3 — Decision gate**: Claude.ai proposes: INCLUDE / DEFER / ABANDON with rationale.
Owner confirms. Criteria for each:
- **INCLUDE**: Expected outcome is measurable, aligns with V2 or V3 gate, effort justified
- **DEFER**: Outcome valid but not measurable until more data available, or belongs in V3
- **ABANDON**: Outcome cannot be defined precisely, or E2E benefit not confirmable

**Step 4 — Promotion**: Approved changes move from P3 to appropriate priority.
New CHANGELOG.md entry authored. PLAN.md backlog updated.

**Step 5 — Completion**: On implementation, outcome is verified against the criterion
defined in Step 1. If outcome not confirmed: revert or reclassify as DEFER.

### Examples of measurable outcomes
- "max_workers=6 stable at 8GB RAM" — confirmed by profiling run, no OOM
- "E2E pipeline time reduced by ≥30% on 7-window DAX run" — confirmed by benchmark
- "auto_go candidate on second instrument without recalibration" — confirmed by validation run
- "SignalCache reduces signal computation calls by ≥80%" — confirmed by call counter

### Examples of non-measurable outcomes (abandon or defer)
- "better architecture" without specific metric
- "more flexible" without a concrete use case
- "might help V3" without V3 requirement being formally specified

Agent C will produce this list in Session 1. Classification criteria:

**REUSE**: Test exercises a module/contract that is unchanged in V2 (e.g. fitness.py,
verdict.py, GA logic, MC engine, ranker). Can be run as-is after staging refresh.

**ADAPT**: Test exercises a module being refactored (e.g. DataLoader → RawDataStore).
Needs import path changes and possibly fixture changes. Logic mostly preserved.

**RETIRE**: Test exercises a module being replaced entirely (e.g. V1 normalisation
constants, RSI search space, hardcoded window list). New test written from scratch.

Flag any test that imports from: `DataLoader`, `_SIGMOID_SCALE`, `_MAX_EXPECTED_DRAWDOWN`,
`_MAX_EXPECTED_VARIANCE`, RSI parameters — these are all RETIRE candidates.

---

## ARCHITECTURE DECISIONS LOG

*Confirmed decisions that drove backlog items. Full detail in CHANGELOG.md.*

| Date | Decision | Rationale | Backlog items |
|------|----------|-----------|---------------|
| Pre-V2 | RSI removed from search space | 6 zero-delta sensitivity runs | RSI-001 |
| Pre-V2 | DataLoader replaced by RawDataStore + WindowSlicer | Single responsibility; OOM root cause | ARCH-001, ARCH-002, IMPL-001, IMPL-002 |
| Pre-V2 | SignalCache introduced | 231× signal recomputation eliminated | ARCH-003, IMPL-003 |
| Pre-V2 | max_workers constraint removed | Shared memory makes 6+ workers safe | ARCH-002 |
| Pre-V2 | DAX normalisation constants → V2-RAR | Multi-asset without recalibration | V2-RAR-1, V2-RAR-2 |
| Pre-V2 | run_backtest() callable interface | V3 meta-optimiser readiness | IMPL-005, ARCH-004 |
| Pre-V2 | Dynamic WFO windows | Multi-asset + V3 meta-optimiser | ARCH-005, IMPL-006 |

---

## NOTES ON V3 READINESS

Every V2 architecture decision must not require refactoring for V3. V3 requirements that
drive V2 decisions:

1. **`run_backtest(config) → result`** callable: V3 outer loop calls V2 as a library.
   No CLI-only interface survives into V2 final.
2. **`BacktestConfig` dataclass**: fully serialisable, constructable programmatically.
   V3 generates configs in code — YAML-only construction not acceptable.
3. **Dynamic WFO windows**: V3 varies window size and count per configuration.
4. **Stateless evaluation**: each `run_backtest()` call fully independent.
   No shared mutable state. V3 runs many backtests in parallel.
5. **`BacktestResult` contract**: exposes all metrics V3 needs. No hidden state in DB only.