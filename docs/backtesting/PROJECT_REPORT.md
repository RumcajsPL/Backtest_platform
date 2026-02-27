# PROJECT_REPORT.md
## Backtesting & Optimization Framework — Progress Report

**Last updated**: 2026-02-27 | **Current phase**: Phase 2 — Core Infrastructure (ready to start)
**Overall status**: 🟡 On Track

---

## Phase Summary

| Phase | Name | Status | Sessions | Key Deliverable |
|---|---|---|---|---|
| 0 | Planning & Requirements | ✅ Complete | 1 | `BACKTESTER_PLAN.md` v1.1 |
| 1 | Design | ✅ Complete | 1 | Contracts, SQLite schema, YAML spec, all decisions resolved |
| 2 | Core Infrastructure | ⬜ Not started | — | CandidateStore, StrategyRunner, Orchestrator skeleton |
| 3 | Optimization Engines | ⬜ Not started | — | GA, WFO (both modes), MC pre-filter |
| 4 | Monte Carlo Deep & Verdict | ⬜ Not started | — | MC deep, Sensitivity map, Verdict engine |
| 5 | Output Layer | ⬜ Not started | — | HTML report, SQLite exports, trading YAML |
| 6 | Hardening & Delivery | ⬜ Not started | — | Adversarial suite, performance validation, full test suite |

**Status key**: ✅ Complete | 🔵 In Progress | 🟡 On Track | ⬜ Not Started | 🔴 At Risk

---

## Phase 0 — Planning & Requirements ✅

**Objective**: Define what to build before any design decisions.
**Completed**: 2026-02-27 | **Sessions**: 1

### Deliverables
| Deliverable | Status | Notes |
|---|---|---|
| Requirements Q&A session | ✅ | 9 question clusters, all answers confirmed |
| `BACKTESTER_PLAN.md` v1.0 | ✅ | 14 sections, 47 requirements |
| Pipeline design (revised) | ✅ | 8-stage sequence with rationale |
| Scenario system defined | ✅ | Section 2.4 + 4.10, 8 requirements |
| Future platform context | ✅ | Section 1b — eToro roadmap recorded |
| `CONTEXT.md` | ✅ | Session handoff ignition key |
| `CHANGE_LOG.md` | ✅ | Living history initialised |
| `PROJECT_REPORT.md` | ✅ | This file |
| `backtester-project` skill | ✅ | Claude coding session skill |

### Key Decisions Made
- Evidence pillars: MC robustness + WFO temporal consistency only
- Pipeline: Random → MC Pre-Filter → GA (WFO-aware) → Full WFO → MC Deep → Sensitivity → Report
- Output: HTML + JSON/Parquet + SQLite + trading YAML
- Verdict: hybrid (auto-go / borderline / auto-reject)
- Scenario: intention-driven, YAML-configurable

### Open Items Carried to Phase 1
10 open decisions (D-01 through D-10) — all resolved in Phase 1.

---

## Phase 1 — Design ✅

**Objective**: Resolve all open decisions. Define every contract. Design SQLite schema. Specify full YAML config.
**Completed**: 2026-02-27 | **Sessions**: 1

### Deliverables
| Deliverable | Status | Notes |
|---|---|---|
| Independent opinion review | ✅ | Adversarial review evaluated; 10 items accepted/modified/rejected with rationale |
| `BACKTESTER_PLAN.md` v1.2 | ✅ | +Section 4.11 adversarial requirements, +Principle 10, all accepted review changes |
| `FUNCTIONAL_SPEC.md` v1.0 | ✅ | All 8 stages in plain language, cross-cutting behaviours, verdict logic, deployment gate |
| `TECHNICAL_SPEC.md` v1.0 | ✅ | All 12 decisions resolved, all 11 contracts as frozen dataclasses, module signatures, YAML schema |
| `SQLITE_SCHEMA.md` v1.0 | ✅ | 9 tables, CREATE TABLE statements, indexes, 10 query examples (incl. ML feature matrix) |
| All 12 open decisions resolved | ✅ | D-01 through D-12, all with rationale |
| All 11 contracts defined | ✅ | Production-ready frozen dataclasses with __post_init__ validation |
| 3 scenario profiles with concrete values | ✅ | capital_accumulation, swing_trading, conservative — all thresholds specified |
| `backtest_template.yaml` schema | ✅ | All valid keys, types, defaults, constraints documented |

### Key Decisions Made This Phase
| Decision | Resolution |
|---|---|
| D-01 Integration mode | Direct Python call in worker process |
| D-02 SQLite concurrency | WAL mode + single-writer queue |
| D-03 Temp YAML lifecycle | Per-candidate, named by hash, deleted in `finally` |
| D-04 GA seeding | Top-N by fitness; diversity handled by penalty |
| D-05 GA WFO windows | Random sample 2 per generation from full list |
| D-06 Stage counts | 200/zone → 120 → pop60/30gen → 30 → 10 → 5 |
| D-07 Verdict thresholds | WFO go ≥0.65; MC go ≤5% (scenario-specific variants defined) |
| D-08 Sensitivity scope | All parameters |
| D-09 Output formats | Both JSON + Parquet |
| D-10 Report generator | Build new |
| D-11 Diversity metric | Hybrid Euclidean/Hamming |
| D-12 IS/OOS gate default | Off by default; opt-in via YAML |

### New Items Added This Phase
- GA-06: Random WFO window sampling per generation
- GA-07: Diversity penalty (Should-Have)
- WF-04: WFO consistency score redefined as 4-metric composite
- WF-09: Post-Stage-1 statistical adequacy warning
- Section 4.11: Adversarial validation requirements (AV-01 to AV-05)
- Architecture Principle 10: Immutable Run Artifacts
- CS-07: Config hash + seeds + perturbation profile stored immutably
- `VerdictResult.deployment_status`: PAPER_TRADE_REQUIRED / LIVE_APPROVED
- New modules: `ga/diversity.py`, `wfo/consistency_scorer.py`
- 2 new risks: R-09 (diversity penalty), R-10 (adversarial suite late detection)

---

## Phase 2 — Core Infrastructure ⬜

**Objective**: Build the backbone. Everything else depends on CandidateStore and StrategyRunner.
**Prerequisite**: Read `TECHNICAL_SPEC.md` and `SQLITE_SCHEMA.md` before writing any code.

### Planned Deliverables
| Deliverable | Status | Notes |
|---|---|---|
| D-01 benchmark: 50 candidates, direct-call mode | ⬜ | Must complete before full StrategyRunner implementation |
| D-02 benchmark: 500 writes, 6-worker load | ⬜ | Must complete before full CandidateStore implementation |
| `candidate_store.py` | ⬜ | SQLite WAL + writer queue. First module — everything depends on it. |
| `parameter_space.py` | ⬜ | Zone expansion, boundary validation |
| `sampler.py` | ⬜ | LHS + random sampling |
| `scenario.py` | ⬜ | ScenarioProfile loader and validator |
| `strategy_runner.py` | ⬜ | Single candidate evaluation, significance guard, never raises |
| `fitness.py` | ⬜ | Stateless constraint check + weighted score |
| `ranker.py` | ⬜ | Stateless query → ranked list |
| `orchestrator.py` (skeleton) | ⬜ | 8 stage stubs + checkpoint/resume logic |
| Integration test | ⬜ | Single candidate full round-trip → stored in SQLite with correct stage label |

### Implementation Order
1. `candidate_store.py` + D-02 benchmark
2. `parameter_space.py` + `sampler.py`
3. `scenario.py`
4. `strategy_runner.py` + D-01 benchmark
5. `fitness.py`
6. `ranker.py`
7. `orchestrator.py` skeleton
8. Integration test

---

## Phase 3 — Optimization Engines ⬜

**Objective**: GA (with random window sampling + diversity), WFO (both modes), MC pre-filter.
**Critical dependency**: `wfo/wfo_evaluator.py` must be built before GA (GA needs WFO for per-generation fitness).

### Planned Deliverables
| Deliverable | Status | Notes |
|---|---|---|
| `wfo/window_generator.py` | ⬜ | |
| `wfo/wfo_evaluator.py` | ⬜ | Build first |
| `wfo/wfo_engine.py` | ⬜ | Lightweight + full modes |
| `wfo/consistency_scorer.py` | ⬜ | 4 temporal metrics → composite score |
| `ga/population.py` | ⬜ | |
| `ga/selection.py` | ⬜ | |
| `ga/crossover.py` | ⬜ | |
| `ga/mutation.py` | ⬜ | |
| `ga/diversity.py` | ⬜ | Hybrid Euclidean/Hamming penalty |
| `ga/ga_engine.py` | ⬜ | Random window sampling per generation |
| `monte_carlo/perturbation.py` | ⬜ | Named versioned profiles |
| `monte_carlo/equity_simulator.py` | ⬜ | |
| `monte_carlo/mc_metrics.py` | ⬜ | |
| `monte_carlo/mc_engine.py` (pre-filter mode) | ⬜ | |
| Integration test: Random → MC Pre-filter → GA → Full WFO | ⬜ | All results in SQLite |

### Key Validation Tasks
- GA random window sampling: confirm window selection is independent per generation
- GA diversity penalty: confirm population does not collapse to narrow cluster over 100+ generations
- R-05 profiling: measure GA WFO-aware fitness runtime vs. 4-hour budget

---

## Phase 4 — Monte Carlo Deep & Verdict ⬜

**Objective**: Full stress testing and go/no-go verdict.

### Planned Deliverables
| Deliverable | Status | Notes |
|---|---|---|
| `monte_carlo/mc_engine.py` (deep mode) | ⬜ | Full iterations, all perturbation types |
| `evaluation/sensitivity.py` | ⬜ | ±1/±2 step perturbation, fitness delta map |
| `evaluation/verdict.py` | ⬜ | Two-pillar logic + sensitivity modifier + optional IS/OOS gate |
| AV-01 smoke test | ⬜ | Random-signal baseline — run before output layer |
| Integration test: full 8-stage pipeline | ⬜ | Verdict produced for top candidates on real data |

---

## Phase 5 — Output Layer ⬜

**Objective**: All output formats produced correctly.

### Planned Deliverables
| Deliverable | Status | Notes |
|---|---|---|
| `report_generator.py` | ⬜ | Scenario-framed HTML, borderline checklist |
| `yaml_generator.py` | ⬜ | Trading YAML with deployment_status metadata |
| JSON/Parquet export | ⬜ | One file per candidate |
| SQLite query validation suite | ⬜ | 10 queries from SQLITE_SCHEMA.md must pass |
| End-to-end system test | ⬜ | Full pipeline on real WBWS data, all outputs validated |

---

## Phase 6 — Hardening & Delivery ⬜

**Objective**: Performance, reliability, adversarial validation, documentation.

### Planned Deliverables
| Deliverable | Status | Notes |
|---|---|---|
| AV-01 Random-signal baseline (automated) | ⬜ | Required for delivery |
| AV-02 Overfit-injection test (automated) | ⬜ | Required for delivery |
| AV-03 Meta-config stability test | ⬜ | >80% verdict stability under seed perturbation |
| Performance validation: ≤4hr full pipeline | ⬜ | On target hardware with 6 workers |
| Resume validation: all 8 checkpoints | ⬜ | |
| Worker isolation test | ⬜ | Kill one worker mid-run, pipeline continues |
| Verdict threshold calibration | ⬜ | D-07 calibration against first real run |
| Full documentation suite | ⬜ | Module ref, YAML guide, scenario guide, SQLite cookbook, paper trading protocol |

---

## Risk Tracker

| ID | Risk | Status | Notes |
|---|---|---|---|
| R-01 | Integration mode too slow | 🟡 Benchmark pending | Decision made (direct call). Benchmark in Phase 2. |
| R-02 | SQLite write contention | 🟡 Benchmark pending | Decision made (WAL + queue). Benchmark in Phase 2. |
| R-03 | ProcessPoolExecutor spawn overhead | 🟡 Open | Measure in Phase 2 |
| R-04 | GA invalid strategy runs | 🟡 Open | strategy_runner.py isolation — Phase 3 |
| R-05 | GA WFO-aware fitness over 4hr budget | 🔴 Watch | **Highest risk.** Profile in Phase 3. Random window sampling mitigates vs. fixed pair. |
| R-06 | Full pipeline over 4hr target | 🟡 Open | All levers are YAML-configurable |
| R-07 | Verdict thresholds miscalibrated | 🟡 Open | Starting values defined in TECHNICAL_SPEC.md. Calibrate Phase 6. |
| R-08 | SQLite schema insufficient for ML | ✅ Resolved | ML-ready schema confirmed in Phase 1 design. |
| R-09 | GA diversity penalty miscalibration | 🟡 Open | Weight YAML-configurable. Monitor population spread in Phase 3. |
| R-10 | Adversarial suite finding flaw late | 🟡 Open | Mitigation: AV-01 smoke test at end of Phase 4. |

---

## Deferred Concerns Log

| ID | Type | Description | Target |
|---|---|---|---|
| DC-01 | Concern | GA WFO-aware fitness runtime cost | Phase 3 profiling (R-05) |
| DC-02 | Concern | SQLite WAL mode on Windows — untested | Phase 2 benchmark (R-02) |
| DC-03 | Opportunity | Market regime tagging for WFO windows | Post-v1 |
| DC-04 | Opportunity | MC pre-filter as standalone health check | Post-v1 |
| DC-05 | Concern | eToro API stability for live layer | Monitor, not v1 |
| DC-06 | Future enhancement | Regime-aware MC perturbation profiles | v2 |
| DC-07 | Future enhancement | True global parameter sensitivity random-walk | v2 |
| DC-08 | Concern | D-01 benchmark: strategy integration speed | Phase 2 — first benchmark task |
| DC-09 | Concern | D-02 benchmark: SQLite writer queue under load | Phase 2 — second benchmark task |

---
*Updated automatically at end of each session. Do not edit manually mid-session.*