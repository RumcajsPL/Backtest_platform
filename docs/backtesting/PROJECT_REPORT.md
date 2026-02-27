# PROJECT_REPORT.md
## Backtesting & Optimization Framework — Progress Report

**Last updated**: 2026-02-27 | **Current phase**: Pre-Design (Planning complete)
**Overall status**: 🟡 On Track

---

## Phase Summary

| Phase | Name | Status | Sessions | Key Deliverable |
|---|---|---|---|---|
| 0 | Planning & Requirements | ✅ Complete | 1 | `BACKTESTER_PLAN.md` v1.1 |
| 1 | Design | ⬜ Not started | — | Contracts, SQLite schema, YAML spec |
| 2 | Core Infrastructure | ⬜ Not started | — | CandidateStore, StrategyRunner, Orchestrator skeleton |
| 3 | Optimization Engines | ⬜ Not started | — | GA, WFO (both modes), MC pre-filter |
| 4 | Monte Carlo Deep & Verdict | ⬜ Not started | — | MC deep, Sensitivity map, Verdict engine |
| 5 | Output Layer | ⬜ Not started | — | HTML report, SQLite exports, trading YAML |
| 6 | Hardening & Delivery | ⬜ Not started | — | Performance validation, full test suite |

**Status key**: ✅ Complete | 🔵 In Progress | 🟡 On Track / Blocked | ⬜ Not Started | 🔴 At Risk

---

## Phase 0 — Planning & Requirements ✅

**Objective**: Define what to build before any design decisions.
**Completed**: 2026-02-27

### Deliverables
| Deliverable | Status | Notes |
|---|---|---|
| Requirements Q&A session | ✅ | 9 question clusters, all answers confirmed |
| `BACKTESTER_PLAN.md` v1.0 | ✅ | 14 sections, 47 requirements |
| Pipeline design (revised) | ✅ | 8-stage sequence with rationale |
| Scenario system defined | ✅ | Section 2.4 + 4.10, 8 requirements |
| Future platform context | ✅ | Section 1b — eToro roadmap recorded |
| `CONTEXT.md` | ✅ | Handoff ignition key |
| `CHANGE_LOG.md` | ✅ | Living history initialized |
| `PROJECT_REPORT.md` | ✅ | This file |
| `PROJECT_SKILL.md` | ✅ | Claude coding session skill |

### Key Decisions Made
- Evidence pillars: MC robustness + WFO temporal consistency only
- Pipeline: Random → MC Pre-Filter → GA (WFO-aware) → Full WFO → MC Deep → Sensitivity → Report
- Output: HTML + JSON/Parquet + SQLite + trading YAML
- Verdict: hybrid (auto-go / borderline / auto-reject)
- Scenario: intention-driven, YAML-configurable

### Open Items Carried to Phase 1
All 10 open decisions (D-01 through D-10) — see BACKTESTER_PLAN.md Section 12

---

## Phase 1 — Design ⬜

**Objective**: Resolve all open decisions. Define every contract. Design SQLite schema. Specify full YAML config.
**Target**: Before any implementation begins.

### Deliverables (planned)
| Deliverable | Status | Notes |
|---|---|---|
| Functional specification | ⬜ | Module behaviours in plain language |
| Technical specification | ⬜ | Contract definitions, module interfaces |
| All 10 contracts as frozen dataclasses | ⬜ | See CONTEXT.md contract checklist |
| SQLite schema | ⬜ | All tables, columns, foreign keys, indexes — ML-ready |
| `backtest_template.yaml` full spec | ⬜ | All valid keys, types, defaults, constraints |
| Integration mode decision + benchmark | ⬜ | D-01 — requires timing prototype |
| SQLite concurrency prototype | ⬜ | D-02 — WAL mode under 6 writers |
| Scenario profiles defined | ⬜ | capital_accumulation, swing_trading, conservative |

### Blocking Decisions
- **D-01** (integration mode) and **D-02** (SQLite concurrency) require prototype benchmarks before any implementation begins

---

## Phase 2 — Core Infrastructure ⬜

**Objective**: Build the backbone. Everything else depends on CandidateStore and StrategyRunner.
**Planned deliverables**: `candidate_store.py`, `parameter_space.py`, `sampler.py`, `scenario.py`, `strategy_runner.py`, `fitness.py`, `ranker.py`, `orchestrator.py` (skeleton)

**Key milestone**: Single candidate full round-trip — parameter set in → MetricsReport + TradeResult out → stored in SQLite with correct stage label.

---

## Phase 3 — Optimization Engines ⬜

**Objective**: GA, WFO, and MC pre-filter.
**Critical dependency**: WFO evaluator must be built before GA (GA needs WFO for fitness).
**Key milestone**: Random → MC Pre-Filter → GA → Full WFO end-to-end, all results queryable in SQLite.

---

## Phase 4 — Monte Carlo Deep & Verdict ⬜

**Objective**: Full stress testing and go/no-go verdict.
**Key milestone**: Full 8-stage pipeline on real WBWS data. Verdict produced for top candidates.

---

## Phase 5 — Output Layer ⬜

**Objective**: All output formats produced correctly.
**Key milestone**: Full pipeline run on real data. HTML report opens. SQLite query suite passes. Trading YAML validates against `StrategyConfig` schema.

---

## Phase 6 — Hardening & Delivery ⬜

**Objective**: Performance, reliability, documentation.
**Key milestone**: Full pipeline within 4-hour target. Resume from each of 8 checkpoints validated. Verdict thresholds calibrated against first real run.

---

## Risk Tracker

| ID | Risk | Status | Notes |
|---|---|---|---|
| R-01 | Integration mode too slow | 🟡 Open | Benchmark in Phase 1 |
| R-02 | SQLite write contention | 🟡 Open | Prototype in Phase 2 |
| R-03 | ProcessPoolExecutor spawn overhead | 🟡 Open | Measure in Phase 2 |
| R-04 | GA invalid strategy runs | 🟡 Open | strategy_runner.py isolation — Phase 3 |
| R-05 | GA WFO-aware fitness over 4hr budget | 🔴 Watch | **Highest risk.** Profile in Phase 3 before accepting design |
| R-06 | Full pipeline over 4hr target | 🟡 Open | Levers available in YAML |
| R-07 | Verdict thresholds miscalibrated | 🟡 Open | Calibrate Phase 6 against real run |
| R-08 | SQLite schema insufficient for ML | 🟡 Open | ML schema review in Phase 1 |

---

## Deferred Concerns Log
*Full details in CHANGE_LOG.md. Summary here.*

| ID | Type | Description | Target |
|---|---|---|---|
| DC-01 | Concern | GA WFO-aware fitness runtime cost unknown | Phase 3 profiling |
| DC-02 | Concern | SQLite WAL mode on Windows — untested | Phase 2 prototype |
| DC-03 | Opportunity | Market regime tagging for WFO windows | Phase 1 schema design |
| DC-04 | Opportunity | MC pre-filter as standalone health check | Post-v1 |
| DC-05 | Concern | eToro API stability unknown for live layer | Monitor, not v1 |

---
*Updated automatically at end of each session. Do not edit manually mid-session.*