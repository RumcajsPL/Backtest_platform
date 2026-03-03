# BACKTESTER_PLAN.md
## Backtesting & Optimization Framework
**Project Charter · Requirements · High-Level Plan**
**Version**: 1.3.0
**Date**: 2026-03-03
**Status**: Phase 6 Complete — Ready for Block 7 (OPT-01 + OPT-02)
---
## Table of Contents
1. [Project Charter](#1-project-charter)
2. [Vision and Success Definition](#2-vision-and-success-definition)
3. [Scope and Boundaries](#3-scope-and-boundaries)
4. [Confirmed Requirements](#4-confirmed-requirements)
5. [Architecture Principles](#5-architecture-principles)
6. [System Overview](#6-system-overview)
7. [Pipeline Design](#7-pipeline-design)
8. [Module Responsibilities](#8-module-responsibilities)
9. [Integration with Strategy Architecture](#9-integration-with-strategy-architecture)
10. [Output and Analytics Layer](#10-output-and-analytics-layer)
11. [Non-Functional Requirements](#11-non-functional-requirements)
12. [Open Decisions for Design Phase](#12-open-decisions-for-design-phase)
13. [High-Level Project Plan](#13-high-level-project-plan)
14. [Risk Register](#14-risk-register)
15. [Lessons Learned](#15-lessons-learned)

---
## 1. Project Charter
### 1.1 Purpose
The Backtesting & Optimization Framework is a fully automated, end-to-end system that answers two questions about any strategy built on the WBWSStrategy architecture:
1. **Does this strategy have real trading potential?** (go / borderline / no-go verdict)
2. **If yes, what are the optimal and robust parameter ranges?** (trading-ready configuration)
The framework replaces manual trial-and-error optimization with a systematic, multi-stage validation pipeline that controls for overfitting, execution bias, and random performance effects.
### 1.2 Context and Origin
This project begins immediately after the successful completion of the WBWSStrategy production architecture (v3.2.0). The strategy architecture was deliberately designed with a `core` execution mode and a `CacheManager` to support exactly this backtesting use case. The backtester is the natural next layer.
A legacy backtesting prototype exists but is being decommissioned. It informs the requirements vocabulary only — no code or design is carried forward.
### 1.3 Stakeholder
Single operator / quantitative analyst. The system must be runnable autonomously on a Windows 10 development machine without manual intervention once started.
### 1.4 Success Criteria
A successful v1 delivery means:
- A full pipeline run (Random → MC Pre-Filter → GA → WFO → MC Deep → Sensitivity → Report) completes autonomously within 4 hours on a single Windows PC
- The output is sufficient to make a documented go/no-go trading decision without running additional manual tests
- All candidate results and intermediate data are persisted and resumable after interruption
- The data schema is ready to support a future ML/AI analytics layer without structural refactoring
- The pipeline passes the Phase 6 adversarial challenge suite before any live capital allocation

---
## 1b. Future Platform Context
This section records the long-term platform vision to ensure architectural decisions made in v1 do not block future development. It is not a v1 requirement.
### Roadmap Beyond the Backtester
The backtester is the second of four planned platform layers:
| Layer | Description | Status |
|---|---|---|
| 1. Strategy Builder | WBWSStrategy architecture, signal generation, trade simulation, analytics | **Complete — v3.2.0** |
| 2. Backtesting Framework | This project — systematic optimization and validation | **Complete — v1.0** |
| 3. Live Signal Platform | Strategy setup → demo account → signal/alert management, trading journal | Future |
| 4. Algorithmic Trading | eToro API integration → automated order execution | Future (depends on broker API maturity) |
### Architectural Implication for v1
The trading-ready strategy YAML produced by the backtester's `yaml_generator.py` is not merely a convenience output. It is the **handoff contract** between the backtesting world and the live trading world. When Layer 3 is built, it will consume exactly this YAML. This means:
- The YAML generator must produce a fully valid, self-contained `StrategyConfig`-compatible file — not a diff or patch
- The YAML schema must be stable and versioned from the start
- The `VerdictResult` contract should include the YAML path as a field so Layer 3 can locate it programmatically
- The `VerdictResult` contract includes a `deployment_status` field (default: `PAPER_TRADE_REQUIRED`) that must be manually updated to `LIVE_APPROVED` after the required paper trading period. This field is embedded in the trading-ready YAML metadata and serves as the operational gate between validation and capital allocation.

No other v1 component is materially affected by this future vision. The pipeline, optimization logic, and analytics layer are internal to the backtester.

---
## 2. Vision and Success Definition
### 2.1 What "Real Trading Potential" Means
A strategy is considered to have real trading potential when **both** of the following evidence types are present in the pipeline output:
| Evidence Type | What It Proves | Pipeline Stage That Produces It |
|---|---|---|
| Results stable across random trade permutations | Not dependent on trade order or a lucky sequence | Monte Carlo |
| Performance consistent across multiple time windows | Not a single lucky period — the edge is repeatable | Walk-Forward multi-window |
These are the two mandatory trust pillars. The backtester must produce structured, quantified evidence for both and combine them into a composite verdict.
**Implications for pipeline design:**
- WFO is required and must produce multi-window consistency evidence, but its primary role is **temporal robustness** (does it work across periods), not IS/OOS train-test gap measurement
- Monte Carlo is required and its primary role is **randomness robustness** (does it survive permutation and noise)
- Out-of-sample performance gap and parameter region width are informational outputs but are **not** gating criteria for the go/no-go verdict by default. An optional `walk_forward.enforce_oos_gate: true` flag is available in `backtest_template.yaml`. When enabled, severe IS/OOS degradation (>50%) triggers a borderline flag (not auto-reject) for human review.
### 2.2 Verdict Model
The system produces a **hybrid verdict**:
- **Automatic rejection** for candidates that fail hard constraints (drawdown, win rate, minimum trades, expectancy, profit factor) — no human review needed, logged and closed
- **Automatic approval** for candidates that pass both trust pillars (MC robustness + multi-period WFO consistency) with high confidence scores
- **Borderline flag** for candidates where one pillar is inconclusive, confidence is marginal, a sensitivity spike is detected, or (optionally) IS/OOS degradation exceeds the configured threshold

The human analyst makes the final call on borderline cases only. The analyst does not re-run analysis — the report contains everything needed. Borderline candidates require a documented adversarial checklist sign-off before any live deployment.

The `VerdictResult` includes a `deployment_status` field:
- `PAPER_TRADE_REQUIRED` — default for all go/borderline candidates. Paper trading required before any capital allocation.
- `LIVE_APPROVED` — manually set by operator after completing the required paper trading period.

### 2.3 WFO Consistency Score — Composite Definition
The WFO consistency score is **not** a single number from a single formula. It is a composite of orthogonal temporal metrics:
- **Median window return** — central tendency of performance across windows
- **Window-to-window return variance** — how stable the edge is across time
- **Worst-window drawdown** — floor protection; a strategy that works 9 of 10 windows but blows up in one is borderline
- **Fraction of positive windows** — simple pass/fail proportion across all windows

These four metrics are combined into the consistency score using scenario-weighted aggregation. All four are exposed individually in the HTML report and SQLite schema. The composite score is the verdict gate; the individual metrics support human review of borderline cases.

### 2.4 Final Deliverable of a Pipeline Run
At the end of a successful full pipeline run, the system produces:
- A ranked shortlist of 3–5 validated parameter configurations with their risk profiles
- A comprehensive analytics report (HTML + JSON/Parquet + SQLite) covering all pipeline stages
- A trading-ready strategy YAML configuration for the top-ranked candidate, deployable directly into the live strategy runner, with `deployment_status: PAPER_TRADE_REQUIRED` embedded in metadata

### 2.5 Scenario-Based Backtesting
The backtester is **intention-driven**. Before a run begins, the operator selects a named trading scenario that defines the objective. The scenario reshapes fitness weights, constraint thresholds, and report framing automatically — the operator states what they are trying to achieve, and the system calibrates itself accordingly.
**Example scenarios:**
| Scenario | Objective | Fitness Emphasis | Constraint Emphasis |
|---|---|---|---|
| `capital_accumulation` | Steadily grow account balance with controlled risk | Win rate, trade frequency, consistency | Tight drawdown, min trades/week, losing streak |
| `swing_trading` | Maximise R:R on high-quality directional signals | R:R ratio, profit factor, avg win size | Min expectancy, fewer but larger wins acceptable |
| `conservative` | Preserve capital, avoid ruin above all | MC ruin probability, max drawdown | Very tight drawdown, high win rate floor |
The scenario does not change the pipeline — it changes the **lens** through which results are evaluated and reported. The report for a `capital_accumulation` run leads with consistency and frequency metrics. The report for a `swing_trading` run leads with R:R and profit factor.
**Architectural implication:** `backtest_template.yaml` gains a `scenario:` top-level key. `FitnessEvaluator`, `Ranker`, and `BacktestReportGenerator` all read the active scenario profile. Adding a new scenario requires only a YAML addition — no pipeline code changes.
The backtester's core question becomes: *"Given this scenario objective, does this strategy have edge — and if yes, what setup best serves this objective?"*

---
## 3. Scope and Boundaries
### 3.1 In Scope — v1
- Full optimization pipeline: Random Search → MC Pre-Filter → Genetic Algorithm (WFO-aware) → Walk-Forward Optimization → Monte Carlo Deep → Parameter Sensitivity → Report
- Parallel execution of independent candidates (up to 6 workers on Windows)
- Persistent candidate store with resume-after-interruption capability
- All four output formats: HTML report, JSON/Parquet files, SQLite database, strategy YAML
- Data schema designed to support future ML/AI analytics
- Loose coupling to strategy: parameter space defined in YAML, not hardcoded
- Windows 10 compatibility throughout
- Config freeze and immutable run artifact enforcement (config hash, seeds stored in SQLite)
- Phase 6 adversarial challenge suite (automated tests gating v1 delivery)
### 3.2 Out of Scope — v1
- ML/AI analytics layer (schema is designed for it; implementation is future work)
- Cloud burst execution or distributed computing
- Live trading integration or order management
- Strategy code modifications (the strategy architecture is fixed input)
- Support for strategies other than those built on the WBWSStrategy architecture
- Regime-aware MC perturbation profiles (future enhancement — logged for v2)
- True global parameter sensitivity random-walk (future enhancement — logged for v2)
- Pre-run statistical power analysis as a gating mechanism (a post-Stage-1 adequacy warning log is in scope)
### 3.3 Fixed Inputs (Not Designed Here)
- Strategy pipeline: `DataLoader → SignalGenerator → FilterPipeline → TradeSimulator → MetricsCalculator`
- Strategy configuration schema: `StrategyConfig` and `strategy_template.yaml`
- Metrics contract: `MetricsReport` from `MetricsCalculator`
- Cache management: `CacheManager` with `clear_all_caches()`

---
## 4. Confirmed Requirements
Requirements are recorded as facts from the design session. Each has a category and a priority.
### 4.1 Pipeline Requirements
| ID | Requirement | Priority |
|---|---|---|
| PL-01 | The pipeline runs all seven stages (Random, MC Pre-Filter, GA, WFO, MC Deep, Sensitivity, Report) in sequence without manual intervention | Must Have |
| PL-02 | Each stage receives its inputs from the previous stage via typed contracts — no raw dicts between stages | Must Have |
| PL-03 | A candidate that fails (runtime error, bad config, data gap) is skipped and logged; the pipeline continues | Must Have |
| PL-04 | The pipeline can resume from the last completed checkpoint after interruption | Must Have |
| PL-05 | Stage failure handling (skip vs. abort vs. retry) is configurable per stage in the YAML | Must Have |
| PL-06 | The pipeline runs in `backtest.mode: full_pipeline` or individual stages can be enabled/disabled | Must Have |
### 4.2 Parameter Space Requirements
| ID | Requirement | Priority |
|---|---|---|
| PS-01 | Parameter space is defined entirely in `backtest_template.yaml` — no hardcoded parameter names in code | Must Have |
| PS-02 | Parameter space supports named zones (safe / exploration / discovery) with independent enable/disable | Must Have |
| PS-03 | Each zone defines ranges, steps, and discrete choices for each optimizable parameter | Must Have |
| PS-04 | The sampler generates a temporary `strategy_template.yaml` for each candidate — the existing strategy runner receives a valid YAML | Must Have |
| PS-05 | Parameters currently optimizable: Session Filter, Technical Filters (ADX, RSI, MACD etc.), Strategy TF, HTF timeframe, ATR length, ATR multiplier, risk percentile, RR target, session windows | Must Have |
| PS-06 | Adding a new optimizable parameter requires only a YAML change and a sampler update — no pipeline code changes | Should Have |
### 4.3 Fitness and Evaluation Requirements
| ID | Requirement | Priority |
|---|---|---|
| FE-01 | Hard constraints are evaluated first (drawdown, win rate, losing streak, trades/week, expectancy, profit factor) — failed candidates are rejected before fitness scoring | Must Have |
| FE-02 | Fitness is a weighted composite score computed from metrics in `MetricsReport` | Must Have |
| FE-03 | Fitness weights are configurable in the YAML | Must Have |
| FE-04 | All constraint thresholds are configurable in the YAML | Must Have |
| FE-05 | A candidate's full `MetricsReport` is persisted alongside its fitness score and parameter set | Must Have |
### 4.4 Candidate Storage Requirements
| ID | Requirement | Priority |
|---|---|---|
| CS-01 | All candidates (pass and fail) are persisted to the candidate store after evaluation | Must Have |
| CS-02 | The store is append-safe — concurrent writes from parallel workers do not corrupt data | Must Have |
| CS-03 | The store persists: parameter set, zone, fitness score, constraint results, full MetricsReport, run metadata | Must Have |
| CS-04 | The store is the checkpoint mechanism — on resume, already-evaluated candidates are not re-run | Must Have |
| CS-05 | The store supports efficient querying for ranking, GA selection, and WFO input | Must Have |
| CS-06 | The store primary format is SQLite (enables ad-hoc SQL what-if analysis post-run) | Must Have |
| CS-07 | The store persists the config hash, all random seeds, and the perturbation profile name used in each run. Any post-run config change must create a new run record — not overwrite the prior one. | Must Have |
### 4.5 Genetic Algorithm Requirements
| ID | Requirement | Priority |
|---|---|---|
| GA-01 | GA operates on the candidate store output from MC Pre-Filter — it evolves, not brute-forces | Must Have |
| GA-02 | GA respects zone boundaries — mutations and crossovers produce only valid parameter combinations | Must Have |
| GA-03 | GA configuration is fully in YAML: population size, generations, mutation rate, crossover rate, elite fraction, tournament size | Must Have |
| GA-04 | Each GA generation produces candidates that are evaluated, stored, and ranked before the next generation | Must Have |
| GA-05 | Elitism is implemented: the top N candidates survive unchanged to the next generation | Must Have |
| GA-06 | The 2 lightweight WFO windows used for GA per-generation fitness are **randomly sampled** from the full WFO window list at the start of each generation. This prevents the GA from overfitting to a fixed pair of windows. | Must Have |
| GA-07 | GA fitness includes a **diversity penalty** that discourages candidates whose parameter vector is too close (within a configurable distance threshold) to existing elites. Weight of the diversity penalty is configurable in YAML. | Should Have |
### 4.6 Walk-Forward Optimization Requirements
| ID | Requirement | Priority |
|---|---|---|
| WF-01 | WFO runs on the top candidates from GA — not the full Random Search population | Must Have |
| WF-02 | Train/test windows are defined as fixed date pairs in the YAML | Must Have |
| WF-03 | Each candidate is evaluated independently on each window | Must Have |
| WF-04 | WFO produces per-candidate **temporal consistency metrics**: median window return, window-to-window return variance, worst-window drawdown, fraction of positive windows. These four metrics are combined into a composite WFO consistency score. | Must Have |
| WF-05 | A candidate that performs well in some windows but collapses in others is flagged for human review (borderline), not automatically rejected | Must Have |
| WF-06 | IS/OOS delta is computed and reported as **informational** by default — it is not a gating criterion. When `walk_forward.enforce_oos_gate: true` is set in the YAML, IS/OOS degradation > 50% triggers a borderline flag (not auto-reject). | Must Have |
| WF-07 | Parameter region width is computed and reported as **informational** — it is not a gating criterion for the verdict | Should Have |
| WF-08 | WFO results are stored in the candidate store alongside Random/GA results | Must Have |
| WF-09 | After Stage 1 (Random Search) completes, the orchestrator logs a **statistical adequacy warning** if the configured MC iteration count or WFO window count is likely insufficient given observed trade frequency and return variance. This is a warning log only — it does not gate the pipeline. | Should Have |
### 4.7 Monte Carlo Requirements
| ID | Requirement | Priority |
|---|---|---|
| MC-01 | Monte Carlo runs in two modes: lightweight pre-filter (Stage 2) and deep stress test (Stage 5) | Must Have |
| MC-02 | Monte Carlo methods: trade shuffling, return resampling, spread noise, risk perturbation, equity path simulation | Must Have |
| MC-03 | MC configuration is fully in YAML: iterations, noise parameters, slippage ranges, shuffling options. Pre-filter and deep modes have independent configuration sections. | Must Have |
| MC-04 | MC produces: avg final equity, worst drawdown across paths, ruin probability, 5th percentile final equity | Must Have |
| MC-05 | MC results are stored in the candidate store | Must Have |
| MC-06 | Each MC run references a named perturbation profile stored in the YAML and recorded in SQLite with the run metadata. Perturbation profile names are versioned. | Should Have |
### 4.8 Output Requirements
| ID | Requirement | Priority |
|---|---|---|
| OP-01 | HTML report: self-contained, covers all pipeline stages, matches the quality standard of the existing strategy analytics report | Must Have |
| OP-02 | JSON/Parquet files: one file per candidate with full pipeline results, structured for notebook / programmatic analysis | Must Have |
| OP-03 | SQLite database: single DB per run, queryable for ad-hoc what-if analysis | Must Have |
| OP-04 | Trading-ready strategy YAML: generated for the top-ranked candidate, deployable directly into the live strategy runner. Metadata includes scenario name and `deployment_status: PAPER_TRADE_REQUIRED`. | Must Have |
| OP-05 | Data schema is ML-ready: designed to support future feature engineering and model training without structural changes | Must Have |
| OP-06 | All intermediate outputs (equity curves, trade logs, candidate configs, run YAMLs) are optionally saved, configurable per output type | Should Have |
### 4.9 Resilience Requirements
| ID | Requirement | Priority |
|---|---|---|
| RS-01 | Individual candidate failures are isolated — one bad candidate does not stop the pipeline | Must Have |
| RS-02 | On resume after interruption, the orchestrator reads the candidate store and skips already-completed work | Must Have |
| RS-03 | Each pipeline stage completion is checkpointed before the next stage begins | Must Have |
| RS-04 | Run metadata (start time, config hash, scenario, random seeds, stage completion status) is persisted at run start | Must Have |
| RS-05 | All failures are logged with full context: candidate ID, parameter set, error message, stack trace | Must Have |
### 4.10 Scenario Requirements
| ID | Requirement | Priority |
|---|---|---|
| SC-01 | A named scenario is selected once per run via a `scenario:` key in `backtest_template.yaml` | Must Have |
| SC-02 | Each scenario defines: fitness weights, constraint thresholds, report emphasis, and a human-readable objective description | Must Have |
| SC-03 | Built-in scenarios for v1: `capital_accumulation`, `swing_trading`, `conservative` | Must Have |
| SC-04 | Custom scenarios can be defined entirely in YAML — no code changes required | Must Have |
| SC-05 | `FitnessEvaluator` and `Ranker` read the active scenario — identical pipeline, different evaluation lens | Must Have |
| SC-06 | The HTML report titles itself with the scenario objective and leads with the metrics most relevant to that scenario | Must Have |
| SC-07 | The verdict engine applies scenario-specific thresholds for go / borderline / no-go | Must Have |
| SC-08 | The trading-ready YAML output includes the scenario name in its metadata | Should Have |
### 4.11 Adversarial Validation Requirements
| ID | Requirement | Priority |
|---|---|---|
| AV-01 | **Random-signal baseline**: replace strategy signals with coin flips; the pipeline must return no-go for all top candidates from this run. This test must be automated and runnable as part of the Phase 6 acceptance suite. | Must Have |
| AV-02 | **Overfit-injection test**: a strategy configuration hand-crafted to curve-fit a single WFO window must be flagged as borderline or auto-rejected by the pipeline. | Must Have |
| AV-03 | **Meta-config stability test**: randomly perturb validation hyperparameters (WFO window count, MC iteration count, GA random seeds) and verify that verdict outcomes are stable — defined as >80% identical go/no-go results for known-robust candidates. | Should Have |
| AV-04 | **Borderline escalation workflow**: borderline candidates require a documented adversarial checklist and human sign-off before any live deployment. The checklist template is produced by the report generator alongside the HTML report. | Must Have |
| AV-05 | All adversarial tests are automated and runnable in CI. Phase 6 does not close until AV-01 and AV-02 pass. | Must Have |

---
## 5. Architecture Principles
These principles are carried directly from the WBWSStrategy architecture and apply without modification to the backtesting framework. They are reproduced here as a first-class project constraint — not a reference.
**1. Single Responsibility** — One module, one concern. The orchestrator orchestrates. The sampler samples. The fitness evaluator evaluates fitness. No module reaches into another's domain.
**2. Contracts Are the Interface** — Every inter-module communication uses typed, frozen dataclasses. No raw dicts between pipeline stages. If data needs to cross a boundary, it goes into the contract.
**3. Immutability** — All contracts use `frozen=True`. Derived fields computed at construction time use `object.__setattr__` in `__post_init__` — the only acceptable use.
**4. Explicit Over Implicit** — Stage-gated behaviour (which stages run, which candidates proceed) is explicit at every call site. No hidden promotion logic.
**5. Vectorisation First** — Hot paths (fitness scoring across populations, MC path simulation) use numpy/pandas vectorised operations.
**6. Fail Fast** — Invalid YAML configuration raises immediately at construction. Invalid parameter combinations abort candidate generation, not silently produce bad candidates.
**7. Single Source of Truth** — All configuration flows from `backtest_template.yaml`. No module loads its own configuration. The parameter space is defined once and consumed everywhere.
**8. Cache Lifecycle Management** — The existing `CacheManager` is used and `clear_all_caches()` is called between candidate runs. No new caching mechanism is introduced.
**9. Code Hygiene** — No debug flags, no print statements, no commented-out blocks, no MagicMocks in production code. Tests are developed in parallel with implementation.
**10. Immutable Run Artifacts** — Config hash, all random seeds, perturbation profile name, and generated YAMLs are stored in SQLite at run start. Any post-run config change must create a new run record. This principle prevents meta-overfitting through post-hoc tuning of validation parameters.

---
## 6. System Overview
```
backtest_template.yaml
        │
        ▼
BacktestOrchestrator
        │
        ├── ParameterSampler ──────────────────► CandidateStore (SQLite)
        │         │                                      │
        │         ▼                                      │
        │   [Random Search Pool]                         │
        │         │                                      │
        │         ▼                                      │
        │   StrategyRunner × N (parallel)                │
        │         │                                      │
        │         ▼                                      │
        │   FitnessEvaluator ──────────────────────────► │
        │                                                │
        ├── MCEngine (pre-filter mode) ───────────────── │
        │         │                                      │
        │         ▼                                      │
        │   [MC Pre-Filter Survivors]                    │
        │                                                │
        ├── GAEngine ─────────────────────────────────── │
        │         │  (WFO-aware: randomly sampled        │
        │         │   2 windows per generation +         │
        │         │   diversity penalty)                 │
        │         ▼                                      │
        │   [Evolved Candidates]                         │
        │         │                                      │
        │         ▼                                      │
        │   StrategyRunner × N (parallel)                │
        │         │                                      │
        │         ▼                                      │
        │   FitnessEvaluator ──────────────────────────► │
        │                                                │
        ├── WFOEngine ────────────────────────────────── │
        │         │                                      │
        │         ▼                                      │
        │   [WFO-validated Candidates]                   │
        │                                                │
        ├── MCEngine (deep mode) ─────────────────────── │
        │         │                                      │
        │         ▼                                      │
        │   [MC-stress-tested Candidates]                │
        │                                                │
        ├── SensitivityEvaluator ─────────────────────── │
        │         │                                      │
        │         ▼                                      │
        │   [Sensitivity-mapped Candidates]              │
        │                                                │
        └── BacktestReportGenerator
                  │
                  ▼
        ┌─────────────────────────────────────┐
        │ HTML Report (scenario-framed)       │
        │ Borderline Adversarial Checklist    │
        │ JSON/Parquet (per candidate)        │
        │ SQLite (full run, queryable)        │
        │ Trading-ready strategy YAML         │
        │   (deployment_status: PAPER_TRADE_REQUIRED) │
        └─────────────────────────────────────┘
```
The `CandidateStore` (SQLite) is the backbone of the system. Every module writes to it. The orchestrator reads from it for stage transitions and resume. The report generator reads from it for final output.

---
## 7. Pipeline Design
*(unchanged from v1.2 — see that version for full stage detail)*

### 7.1 Pipeline Sequence
```
Stage 0:  Validation & Initialisation
Stage 1:  Random Search           (broad exploration, single-run fitness, significance guard)
Stage 2:  MC Pre-Filter           (cheap early elimination of fragile candidates)
Stage 3:  Genetic Algorithm       (WFO-aware fitness — randomly sampled windows per generation, diversity penalty)
Stage 4:  Full Walk-Forward       (definitive temporal consistency evidence on GA survivors)
Stage 5:  Monte Carlo Deep        (full stress test on WFO-validated candidates only)
Stage 6:  Parameter Sensitivity   (sensitivity map — flat = robust, spike = borderline flag)
Stage 7:  Final Report & Output
```

---
## 8–11. Module Responsibilities / Integration / Output Layer / Non-Functional Requirements
*(unchanged from v1.2)*

---
## 12. Open Decisions for Design Phase
All decisions D-01 through D-12 are resolved. See TECHNICAL_SPEC.md Section 1 for full resolutions and rationale.

| ID | Decision | Status |
|---|---|---|
| D-01 | Strategy integration mode | ✅ Direct Python call |
| D-02 | SQLite write concurrency | ✅ WAL + single-writer queue |
| D-03 | Temporary YAML lifecycle | ✅ Named by hash, deleted in finally |
| D-04 | GA population seeding | ✅ Top-N from MC_PREFILTER_PASS |
| D-05 | GA WFO window selection | ✅ Random sample 2 per generation |
| D-06 | Stage transition candidate counts | ✅ Defaults in YAML, profiled Phase 3/6 |
| D-07 | Composite verdict thresholds | ✅ Confirmed boundary operators; starting values in YAML |
| D-08 | Sensitivity map scope | ✅ All optimizable parameters |
| D-09 | Parquet vs JSON | ✅ Both, configurable |
| D-10 | HTML report generator | ✅ New report_generator.py |
| D-11 | GA diversity distance metric | ✅ Hybrid Euclidean/Hamming |
| D-12 | IS/OOS gate default | ✅ Off by default |

---
## 13. High-Level Project Plan

| Phase | Name | Status | Key Deliverable |
|---|---|---|---|
| 0 | Planning & Requirements | ✅ Complete | BACKTESTER_PLAN.md v1.2 |
| 1 | Design | ✅ Complete | Contracts, schema, all 12 decisions resolved |
| 2 | Core Infrastructure | ✅ Complete | CandidateStore, StrategyRunner, Orchestrator skeleton |
| 3 | Optimization Engines | ✅ Complete | GA, WFO (both modes), MC pre-filter |
| 4 | Monte Carlo Deep & Verdict | ✅ Complete | MC deep, Sensitivity, Verdict engine |
| 5 | Output Layer | ✅ Complete | report_generator.py, yaml_generator.py, all formats |
| 6 | Hardening & Delivery | ✅ Complete | 233 tests green, adversarial suite, performance baseline, documentation |
| 7 | Performance Optimisation | 🔵 Next | OPT-01 (pool reuse) + OPT-02 (batching) in sensitivity.py |

### Phase 6 — Hardening & Delivery ✅ Complete (2026-03-03)

**Completed deliverables:**
- Block 0: E2E test on real WBWS data (13 tests — `test_e2e_wbws_real_data.py`)
- Block 1: User guide (`BACKTESTER_USER_GUIDE.md`)
- Block 2: Adversarial suite — AV-02 overfit-injection confirmed no_go; AV-03 position stability 100% across 3 seeds (8 tests — `test_adversarial_suite.py`)
- Block 3: Performance baseline locked — Total=337s, Stage6=333s, 2.3% of daily budget (7 tests — `test_performance.py`)
- Block 4: Resume validation at all 8 checkpoints; worker isolation confirmed; Windows spawn mock patching constraint documented (12 tests — `test_robustness.py`)
- Block 5: Verdict threshold calibration — full 22-test grid against e2e_test scenario; boundary operators confirmed >= / <= inclusive (22 tests — `test_threshold_calibration.py`)
- Block 6: Final documentation — ARCHITECTURE.md v1.2, TECHNICAL_SPEC.md v1.1, FUNCTIONAL_SPEC.md v1.1, BACKTESTER_PLAN.md v1.3, PROJECT_REPORT.md updated, OPERATOR_RUNBOOK.md created

**Total tests green**: 233

---
## 14. Risk Register

| ID | Risk | Status | Notes |
|---|---|---|---|
| R-01 | Integration mode too slow | ✅ Resolved | Benchmark passed Phase 2 |
| R-02 | SQLite write contention | ✅ Resolved | WAL + single-writer queue confirmed Phase 2 |
| R-03 | ProcessPoolExecutor spawn overhead | ✅ Resolved | Structural bottleneck confirmed and documented (Stage 6, ~66–89s/candidate). OPT-01 planned Block 7. |
| R-04 | GA invalid strategy runs | ✅ Resolved | strategy_runner.py never raises |
| R-05 | GA WFO-aware fitness over 4hr budget | ✅ Resolved | Total run 337–457s (2.3–3.2% of daily budget) |
| R-06 | Full pipeline over 4hr target | ✅ Resolved | Well within budget on 3-month slice |
| R-07 | Verdict thresholds miscalibrated | 🟡 Open | Starting values in YAML; recalibrate after first real run (D-07) |
| R-08 | SQLite schema insufficient for ML | ✅ Resolved | ML-ready schema confirmed Phase 1 |
| R-09 | GA diversity penalty miscalibration | 🟡 Open | Weight YAML-configurable; monitor population spread in first real run |
| R-10 | Adversarial suite finding flaw late | ✅ Resolved | AV-01 passed Phase 4; AV-02/03 passed Phase 6 Block 2 |

---
## 15. Lessons Learned

Recorded at Phase 6 completion. These represent confirmed implementation constraints that future developers must be aware of.

**L-01 — Windows spawn mode: mock patches do not cross the worker boundary**

`unittest.mock.patch` decorates objects in the parent process. On Windows `ProcessPoolExecutor` spawn mode, child processes are fresh Python interpreters — they re-import modules from scratch and do not inherit parent-process patches. Patching a worker function from a test has no effect on the worker; the original function runs instead.

The correct isolation point for integration tests that exercise Stage 6 loop behaviour (continue on failure, write profile, advance checkpoint) is to patch at the orchestrator level — specifically `src.backtesting.orchestrator.evaluate_sensitivity` — not the worker function `_evaluate_perturbation`. The unit under test is the stage's orchestration behaviour, not the worker's internal logic. Worker-level tests belong in the sensitivity module's own unit tests where `ProcessPoolExecutor` is not involved.

Confirmed failure mode (Block 4, ROB-09): `Can't pickle <class 'unittest.mock.MagicMock'>` — the mock object itself failed pickling when the executor attempted to transmit it to a worker process.

**L-02 — Verdict boundary operators must be >= / <= (inclusive) at go thresholds**

The go thresholds in the verdict engine use inclusive operators: `wfo_composite >= go_wfo_floor` and `ruin_prob <= go_mc_ruin_ceiling`. Using strict `>` / `<` would incorrectly classify candidates scoring exactly at the go threshold as BORDERLINE rather than AUTO_GO. The go threshold is intended as a pass line — meeting the standard exactly is sufficient.

The no-go boundaries use strict operators in the opposite direction: `wfo_composite < borderline_wfo_floor` and `ruin_prob > borderline_mc_ruin_ceiling`. This creates a well-defined three-zone partition with no ambiguous boundary cases.

Confirmed and locked from `verdict.py` source review (Block 5). Do not change to strict inequality at go thresholds.

**L-03 — Stage 6 is the dominant runtime cost; pool reuse is the highest-value optimisation**

The performance baseline (Block 3) confirmed that Stage 6 (Sensitivity) accounts for 332–446s of a 337–457s total run — over 97% of end-to-end time. The root cause is Windows `ProcessPoolExecutor` spawn mode: each candidate's perturbation batch creates a new pool, paying per-worker startup cost (a fresh Python interpreter spawn) for every candidate.

Pool reuse across candidates (OPT-01) eliminates the per-candidate startup cost and is expected to reduce Stage 6 by 40–60%. This is the single highest-value optimisation available. Stage 5 (MC Deep) at 0.3–2.5s for fully vectorised 3000-iteration simulation is not a bottleneck and requires no optimisation until `input_count > 50`.

OPT-01 is planned for Block 7. Do not start it until Block 6 documentation is fully closed.

**L-04 — Config fixture shape for tests must match load_scenario() nested structure**

`scenario.py`'s `load_scenario()` reads a nested config dict: `config["scenarios"][name]["fitness_weights"]`, `config["scenarios"][name]["constraints"]`, etc. Tests that construct config fixtures as flat dicts (e.g. `{"fitness_weights": {...}}` at the top level) fail with `KeyError` at the nested key access.

The correct fixture shape wraps the scenario data under `scenarios → name → subsection`, matching the structure of `backtest_template.yaml`. This constraint applies to any test that calls `load_scenario()` directly or exercises code that calls it transitively (orchestrator init, verdict calibration tests).

Confirmed across multiple test failures during Phase 6 Blocks 0 and 5.

---
*v1.0 — Initial version produced 2026-02-27.*
*v1.1 — Updated 2026-02-27: corrected evidence pillars, added scenario-based backtesting, revised pipeline sequence, added platform context.*
*v1.2 — Updated 2026-02-27: incorporated accepted items from adversarial review. D-05 resolved, GA diversity penalty, WFO consistency composite, VerdictResult deployment_status, AV requirements, Architecture Principle 10, CS-07, WF-06 updated, WF-09 added, D-11/D-12 added.*
*v1.3 — Updated 2026-03-03: Phase 6 marked complete. Section 13 plan table updated. All open decisions summary table added (Section 12). R-03/R-05/R-06/R-10 closed in risk register. Section 15 Lessons Learned added (L-01 through L-04). Sections 7–11 reference v1.2 for unchanged content.*