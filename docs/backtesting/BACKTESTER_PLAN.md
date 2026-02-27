# BACKTESTER_PLAN.md
## Backtesting & Optimization Framework
**Project Charter · Requirements · High-Level Plan**
**Version**: 1.0.0
**Date**: 2026-02-27
**Status**: Requirements Complete — Ready for Design Phase
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
- A full pipeline run (Random → GA → WFO → Monte Carlo) completes autonomously within 4 hours on a single Windows PC
- The output is sufficient to make a documented go/no-go trading decision without running additional manual tests
- All candidate results and intermediate data are persisted and resumable after interruption
- The data schema is ready to support a future ML/AI analytics layer without structural refactoring
---
## 1b. Future Platform Context
This section records the long-term platform vision to ensure architectural decisions made in v1 do not block future development. It is not a v1 requirement.
### Roadmap Beyond the Backtester
The backtester is the second of four planned platform layers:
| Layer | Description | Status |
|---|---|---|
| 1. Strategy Builder | WBWSStrategy architecture, signal generation, trade simulation, analytics | **Complete — v3.2.0** |
| 2. Backtesting Framework | This project — systematic optimization and validation | **In design** |
| 3. Live Signal Platform | Strategy setup → demo account → signal/alert management, trading journal | Future |
| 4. Algorithmic Trading | eToro API integration → automated order execution | Future (depends on broker API maturity) |
### Architectural Implication for v1
The trading-ready strategy YAML produced by the backtester's `yaml_generator.py` is not merely a convenience output. It is the **handoff contract** between the backtesting world and the live trading world. When Layer 3 is built, it will consume exactly this YAML. This means:
- The YAML generator must produce a fully valid, self-contained `StrategyConfig`-compatible file — not a diff or patch
- The YAML schema must be stable and versioned from the start
- The `VerdictResult` contract should include the YAML path as a field so Layer 3 can locate it programmatically
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
- Out-of-sample performance gap and parameter region width are informational outputs but are **not** gating criteria for the go/no-go verdict
### 2.2 Verdict Model
The system produces a **hybrid verdict**:
- **Automatic rejection** for candidates that fail hard constraints (drawdown, win rate, minimum trades, expectancy, profit factor) — no human review needed, logged and closed
- **Automatic approval** for candidates that pass both trust pillars (MC robustness + multi-period WFO consistency) with high confidence scores
- **Borderline flag** for candidates where one pillar is inconclusive or confidence is marginal — the structured evidence report is surfaced for human review
The human analyst makes the final call on borderline cases only. The analyst does not re-run analysis — the report contains everything needed. The system does not produce a binary pass/fail for borderline — it produces a structured evidence summary that supports a documented human decision.
### 2.3 Final Deliverable of a Pipeline Run
At the end of a successful full pipeline run, the system produces:
- A ranked shortlist of 3–5 validated parameter configurations with their risk profiles
- A comprehensive analytics report (HTML + JSON/Parquet + SQLite) covering all pipeline stages
- A trading-ready strategy YAML configuration for the top-ranked candidate, deployable directly into the live strategy runner
### 2.4 Scenario-Based Backtesting
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
- Full optimization pipeline: Random Search → Genetic Algorithm → Walk-Forward Optimization → Monte Carlo
- Parallel execution of independent candidates (up to 6 workers on Windows)
- Persistent candidate store with resume-after-interruption capability
- All four output formats: HTML report, JSON/Parquet files, SQLite database, strategy YAML
- Data schema designed to support future ML/AI analytics
- Loose coupling to strategy: parameter space defined in YAML, not hardcoded
- Windows 10 compatibility throughout
### 3.2 Out of Scope — v1
- ML/AI analytics layer (schema is designed for it; implementation is future work)
- Cloud burst execution or distributed computing
- Live trading integration or order management
- Strategy code modifications (the strategy architecture is fixed input)
- Support for strategies other than those built on the WBWSStrategy architecture
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
| PL-01 | The pipeline runs all four stages (Random, GA, WFO, Monte Carlo) in sequence without manual intervention | Must Have |
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
| PS-05 | Parameters currently optimizable: Session Filter, Techical Filters (ADX, RSI, MACD etc.), Strategy TF, HTF timeframe, ATR length, ATR multiplier, risk percentile, RR target, session windows | Must Have |
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
### 4.5 Genetic Algorithm Requirements
| ID | Requirement | Priority |
|---|---|---|
| GA-01 | GA operates on the candidate store output from Random Search — it evolves, not brute-forces | Must Have |
| GA-02 | GA respects zone boundaries — mutations and crossovers produce only valid parameter combinations | Must Have |
| GA-03 | GA configuration is fully in YAML: population size, generations, mutation rate, crossover rate, elite fraction, tournament size | Must Have |
| GA-04 | Each GA generation produces candidates that are evaluated, stored, and ranked before the next generation | Must Have |
| GA-05 | Elitism is implemented: the top N candidates survive unchanged to the next generation | Must Have |
### 4.6 Walk-Forward Optimization Requirements
| ID | Requirement | Priority |
|---|---|---|
| WF-01 | WFO runs on the top candidates from GA — not the full Random Search population | Must Have |
| WF-02 | Train/test windows are defined as fixed date pairs in the YAML | Must Have |
| WF-03 | Each candidate is evaluated independently on each window | Must Have |
| WF-04 | WFO produces per-candidate **temporal consistency metrics**: performance per window, window-to-window variance, consistency score | Must Have |
| WF-05 | A candidate that performs well in some windows but collapses in others is flagged for human review (borderline), not automatically rejected | Must Have |
| WF-06 | IS/OOS delta is computed and reported as **informational** — it is not a gating criterion for the verdict | Must Have |
| WF-07 | Parameter region width is computed and reported as **informational** — it is not a gating criterion for the verdict | Should Have |
| WF-08 | WFO results are stored in the candidate store alongside Random/GA results | Must Have |
### 4.7 Monte Carlo Requirements
| ID | Requirement | Priority |
|---|---|---|
| MC-01 | Monte Carlo runs on the top candidates from WFO | Must Have |
| MC-02 | Monte Carlo methods: trade shuffling, return resampling, spread noise, risk perturbation, equity path simulation | Must Have |
| MC-03 | MC configuration is fully in YAML: iterations, noise parameters, slippage ranges, shuffling options | Must Have |
| MC-04 | MC produces: avg final equity, worst drawdown across paths, ruin probability | Must Have |
| MC-05 | MC results are stored in the candidate store | Must Have |
### 4.8 Output Requirements
| ID | Requirement | Priority |
|---|---|---|
| OP-01 | HTML report: self-contained, covers all pipeline stages, matches the quality standard of the existing strategy analytics report | Must Have |
| OP-02 | JSON/Parquet files: one file per candidate with full pipeline results, structured for notebook / programmatic analysis | Must Have |
| OP-03 | SQLite database: single DB per run, queryable for ad-hoc what-if analysis | Must Have |
| OP-04 | Trading-ready strategy YAML: generated for the top-ranked candidate, deployable directly into the live strategy runner | Must Have |
| OP-05 | Data schema is ML-ready: designed to support future feature engineering and model training without structural changes | Must Have |
| OP-06 | All intermediate outputs (equity curves, trade logs, candidate configs, run YAMLs) are optionally saved, configurable per output type | Should Have |
### 4.9 Resilience Requirements
| ID | Requirement | Priority |
|---|---|---|
| RS-01 | Individual candidate failures are isolated — one bad candidate does not stop the pipeline | Must Have |
| RS-02 | On resume after interruption, the orchestrator reads the candidate store and skips already-completed work | Must Have |
| RS-03 | Each pipeline stage completion is checkpointed before the next stage begins | Must Have |
| RS-04 | Run metadata (start time, config hash, stage completion status) is persisted at run start | Must Have |
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
        ├── GAEngine ─────────────────────────────────── │
        │         │                                      │
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
        ├── MonteCarloEngine ─────────────────────────── │
        │         │                                      │
        │         ▼                                      │
        │   [MC-stress-tested Candidates]                │
        │                                                │
        └── BacktestReportGenerator
                  │
                  ▼
        ┌─────────────────────────────────────┐
        │ HTML Report                         │
        │ JSON/Parquet (per candidate)        │
        │ SQLite (full run, queryable)        │
        │ Trading-ready strategy YAML         │
        └─────────────────────────────────────┘
```
The `CandidateStore` (SQLite) is the backbone of the system. Every module writes to it. The orchestrator reads from it for stage transitions and resume. The report generator reads from it for final output.
---
## 7. Pipeline Design
### 7.1 Pipeline Sequence — Revised Design
The original naive sequence (Random → GA → WFO → MC) has been revised based on industry practice analysis. Two structural problems with the naive order:
- GA evolves against single-run fitness → produces overfit candidates that collapse on WFO windows. Generations are wasted evolving configurations that WFO will reject.
- MC running only at the end means structurally fragile candidates consume expensive GA cycles before elimination.
**Revised sequence:**
```
Stage 0:  Validation & Initialisation
Stage 1:  Random Search           (broad exploration, single-run fitness, significance guard)
Stage 2:  MC Pre-Filter           (cheap early elimination of fragile candidates)
Stage 3:  Genetic Algorithm       (WFO-aware fitness — evolved candidates robust by construction)
Stage 4:  Full Walk-Forward       (definitive temporal consistency evidence on GA survivors)
Stage 5:  Monte Carlo Deep        (full stress test on WFO-validated candidates only)
Stage 6:  Parameter Sensitivity   (sensitivity map — flat = robust, spike = borderline flag)
Stage 7:  Final Report & Output
```
**Why this order:**
| Change from naive | Rationale |
|---|---|
| MC Pre-Filter before GA | Eliminates structurally fragile candidates cheaply before expensive GA cycles. Low iterations, 2 perturbation types. A 30% ruin-probability candidate is not worth evolving. |
| GA uses lightweight WFO fitness | Each candidate's GA fitness = consistency across 2 fast WFO windows, not single-run P&L. Evolved candidates are temporally robust by construction. Fewer wasted generations. |
| Full WFO after GA | Confirmation of temporal consistency across all configured windows. Most fragile candidates already gone. This stage is evidence collection, not discovery. |
| MC Deep after WFO | Full stress test (all iterations, all perturbation types) applied to a small already-robust population. Maximum information per CPU-hour at this stage. |
| Parameter Sensitivity last | For each top candidate, perturb each parameter ±1 and ±2 steps. Flat fitness landscape = robust deployment. Sharp spike = borderline flag added regardless of other scores. |
### 7.2 Stage Detail
```
Stage 0: Validation & Initialisation
  └─ Validate backtest_template.yaml (schema, scenario key, zone definitions)
  └─ Validate strategy data files exist and are readable
  └─ Validate WFO window date ranges are within data bounds and non-overlapping
  └─ Initialise CandidateStore, write run metadata (config hash, scenario, timestamp)
  └─ Checkpoint: RUN_INITIALISED
Stage 1: Random Search
  └─ ParameterSampler expands zones → candidate parameter sets
  └─ LHS or random sampling selects N candidates per zone
  └─ StrategyRunner evaluates each candidate (parallel, core mode)
  └─ Statistical significance guard: < min_trades → REJECTED_INSUFFICIENT_TRADES (before fitness)
  └─ Hard constraint filter (scenario thresholds): failed → REJECTED_CONSTRAINTS
  └─ FitnessEvaluator scores passing candidates (scenario-weighted)
  └─ All candidates written to CandidateStore with stage=RANDOM
  └─ Checkpoint: RANDOM_SEARCH_COMPLETE
Stage 2: MC Pre-Filter
  └─ Ranker selects top N from RANDOM stage
  └─ MCEngine runs lightweight screen (low iterations, spread_noise + shuffle_trades only)
  └─ Candidates with ruin_probability > scenario.mc_prefilter_threshold → MC_PREFILTER_FAIL
  └─ Survivors written to CandidateStore with stage=MC_PREFILTER_PASS
  └─ Checkpoint: MC_PREFILTER_COMPLETE
Stage 3: Genetic Algorithm (WFO-aware fitness)
  └─ GAEngine seeds initial population from MC_PREFILTER_PASS candidates
  └─ Per-generation fitness: each candidate evaluated on 2 lightweight WFO windows
  └─ Generation fitness = weighted(single_run_score, 2-window WFO consistency)
  └─ Elites (top elite_fraction) preserved unchanged each generation
  └─ Crossover + mutation produce valid CandidateParameterSets within zone boundaries
  └─ All GA candidates written to CandidateStore with stage=GA, generation=N
  └─ Checkpoint: GA_COMPLETE
Stage 4: Full Walk-Forward Optimization
  └─ Ranker selects top M candidates from (RANDOM + GA) pool combined
  └─ WFOEngine evaluates each candidate on all configured date windows
  └─ WFOEvaluator computes: per-window fitness, window-to-window variance, consistency score
  └─ IS/OOS delta computed and stored as informational metric (not a verdict gate)
  └─ Window-collapse pattern → borderline flag (not auto-rejection)
  └─ WFO results written to CandidateStore with stage=WFO
  └─ Checkpoint: WFO_COMPLETE
Stage 5: Monte Carlo Deep
  └─ Ranker selects top K candidates ranked by WFO consistency score
  └─ MCEngine runs full stress test (full iterations, all perturbation types)
  └─ MCMetrics computes: avg final equity, worst drawdown across paths, ruin probability
  └─ MC results written to CandidateStore with stage=MC_DEEP
  └─ Checkpoint: MONTE_CARLO_COMPLETE
Stage 6: Parameter Sensitivity Map
  └─ For each top candidate: hold all parameters fixed, vary each ±1 step, ±2 steps
  └─ StrategyRunner evaluates each perturbation (parallel)
  └─ FitnessDelta computed per parameter per step — produces per-candidate sensitivity profile
  └─ Flat profile → robustness confirmed. Sharp spike → borderline flag added
  └─ Sensitivity results written to CandidateStore with stage=SENSITIVITY
  └─ Checkpoint: SENSITIVITY_COMPLETE
Stage 7: Final Report & Output
  └─ Verdict engine applies two-pillar verdict (WFO consistency + MC ruin probability)
  └─ Sensitivity spike flags applied (spike → borderline even if both pillars pass)
  └─ Scenario-framed composite ranking produced
  └─ BacktestReportGenerator produces all output formats
  └─ Top candidate trading-ready YAML generated and validated against StrategyConfig schema
  └─ Checkpoint: COMPLETE
```
### 7.3 Statistical Significance Guard
Before fitness scoring or constraint evaluation, every candidate evaluation must pass a minimum trade count check. This is a **data quality gate**, not a business constraint — it prevents the fitness function from operating on statistically meaningless results. Configurable independently from `min_trades_per_week`. Rejections recorded as `REJECTED_INSUFFICIENT_TRADES` in the CandidateStore.
### 7.4 Resume Logic
On startup, the orchestrator reads `run_metadata` from the CandidateStore. If a prior run is found and is not in `COMPLETE` state, resume is offered. Resume skips all completed stages (by checkpoint) and restarts from the last incomplete stage. Within a stage, already-stored candidates (by parameter hash) are not re-evaluated.
### 7.5 Parallelism Model
Independent candidate evaluations within Stages 1, 2, 3, and 6 are parallelised using `ProcessPoolExecutor` (Windows `spawn` mode — no `fork`-dependent code). Each worker receives a `CandidateParameterSet`, builds a temporary strategy YAML, calls the strategy pipeline in `core` mode, and returns a result contract. The CandidateStore write is serialised to prevent corruption.
Stage 4 (WFO): parallelised per candidate — each candidate's windows run in a worker pool.
Stage 5 (MC Deep): parallelised per candidate — each candidate's iterations run vectorised within a single worker.
---
## 8. Module Responsibilities
### `orchestrator.py`
Loads config and active scenario, initialises the CandidateStore, runs 8 stages in sequence, handles resume logic, reads/writes checkpoints, calls stage modules. Does not evaluate candidates. Does not compute metrics.
### `parameter_space.py`
Reads zone definitions from YAML. Expands ranges to discrete candidate parameter sets. Validates all generated combinations are within zone boundaries. Knows nothing about strategy internals or scenario.
### `sampler.py`
Receives the expanded parameter space. Applies Random or Latin Hypercube Sampling. Returns a list of `CandidateParameterSet` contracts. Does not call the strategy.
### `strategy_runner.py`
Receives a `CandidateParameterSet`. Builds a temporary strategy YAML. Calls the strategy pipeline in `core` mode. Applies the statistical significance guard (minimum trades check) before returning. Returns a `CandidateResult` contract containing the `MetricsReport` and `TradeResult`. Handles per-candidate errors — logs and returns a failed result contract, never raises to the caller.
### `fitness.py`
Receives a `MetricsReport` and the active `ScenarioProfile`. Applies scenario-specific hard constraints first. If passed, computes the scenario-weighted fitness score. Returns a `FitnessResult` contract. Stateless.
### `scenario.py`
Loads and validates scenario definitions from YAML. Returns a `ScenarioProfile` contract (fitness weights, constraint thresholds, report emphasis). Built once at run start, passed to `fitness.py`, `ranker.py`, `verdict.py`, and `report_generator.py`.
### `candidate_store.py`
SQLite-backed persistent store. WAL mode for concurrent writes. Accepts `CandidateRecord` writes. Provides efficient reads for ranking and stage transitions. Provides run metadata read/write including checkpoint state. Single source of truth for all pipeline state.
### `ranker.py`
Reads from `CandidateStore`. Sorts, filters, and returns ranked candidate lists for GA seeding, WFO input, MC input, sensitivity input, and final report. Stateless — receives a query spec, returns a ranked list.
### `ga/ga_engine.py`
Orchestrates the GA evolution loop. Per-generation fitness uses `wfo/wfo_evaluator.py` on 2 lightweight windows (not single-run fitness). Calls `population.py`, `selection.py`, `crossover.py`, `mutation.py` in sequence per generation. Does not evaluate candidates directly.
### `ga/population.py`, `selection.py`, `crossover.py`, `mutation.py`
Each handles exactly one GA operation. All respect zone boundaries. All produce valid `CandidateParameterSet` contracts.
### `wfo/wfo_engine.py`
Orchestrates WFO across candidate list and window list. Calls `wfo_evaluator.py` per candidate-window pair. Used in full mode (Stage 4) and lightweight mode (Stage 3 GA fitness).
### `wfo/window_generator.py`
Reads fixed window definitions from YAML. Returns a list of `WFOWindow` contracts.
### `wfo/wfo_evaluator.py`
Evaluates one candidate on one WFO window. Calls `strategy_runner.py` for the window period. Computes per-window fitness and IS/OOS delta. Returns `WFOWindowResult` contract.
### `monte_carlo/mc_engine.py`
Orchestrates MC in two modes: lightweight pre-filter (Stage 2, low iterations, 2 perturbation types) and deep stress test (Stage 5, full iterations, all perturbation types). Mode is passed as a parameter.
### `monte_carlo/equity_simulator.py`
Simulates equity paths from a `TradeResult` trade list. Applies perturbations from `perturbation.py`.
### `monte_carlo/perturbation.py`
Applies configured noise to trade results: spread noise, risk noise, slippage, execution delay, trade shuffling, return resampling.
### `monte_carlo/mc_metrics.py`
Computes MC summary metrics: avg final equity, worst drawdown across paths, ruin probability. Returns `MCResult` contract.
### `evaluation/sensitivity.py`
For each top candidate, perturbs each parameter ±1 step and ±2 steps while holding all others fixed. Calls `strategy_runner.py` for each perturbation. Computes fitness delta per parameter per step. Returns `SensitivityProfile` contract. Flags candidates with sharp fitness spikes as borderline.
### `evaluation/verdict.py`
Reads final pipeline results per candidate. Applies two-pillar verdict: (1) WFO consistency score, (2) MC deep ruin probability. Sensitivity spike flags applied as a third modifier. IS/OOS delta attached as informational evidence only. Returns `VerdictResult` contract with full evidence summary and scenario-framed assessment.
### `report_generator.py`
Reads from `CandidateStore` across all stages. Scenario-framed output: report leads with the metrics most relevant to the active scenario. Produces HTML report, JSON/Parquet files per candidate, and final ranking table. Distinct from the existing strategy `ReportGenerator`.
### `yaml_generator.py`
Takes the top-ranked candidate's `CandidateParameterSet`. Merges parameter values into the base `strategy_template.yaml`. Validates the output against `StrategyConfig` schema. Embeds scenario name in YAML metadata. Writes the trading-ready YAML — the handoff artifact for the future live trading layer.
---
## 9. Integration with Strategy Architecture
### 9.1 Integration Mode Decision
The exact mechanism by which the backtester calls the strategy (direct orchestrator call vs. subprocess vs. module-level) is deferred to the design phase. This is a significant architectural decision with performance, isolation, and maintainability trade-offs that require benchmarking and analysis of the Windows process model before committing.
### 9.2 What Is Fixed (Not Redesigned)
The following strategy architecture components are consumed as-is, without modification:
- `StrategyConfig.from_yaml()` — config loading and validation
- `CacheManager` and `clear_all_caches()` — cache management between runs
- `MetricsReport` — the primary fitness input (all fields are available)
- `TradeResult` — available for Monte Carlo (raw trade list for shuffling/resampling)
- Execution mode `core` — mandatory for all backtester runs (analytics mode is not used inside the backtester loop)
### 9.3 Parameter Space to StrategyConfig Mapping
The backtester's parameter space YAML maps to `StrategyConfig` fields. The `strategy_runner.py` is responsible for this mapping — it reads a `CandidateParameterSet` and writes valid field values into a temporary strategy YAML. This is the only place where strategy config field names appear in the backtester code.
---
## 10. Output and Analytics Layer
### 10.1 Output Formats
| Format | Content | Primary Use |
|---|---|---|
| HTML Report | Full pipeline summary, all stages, candidate rankings, charts | Human review, go/no-go decision |
| JSON/Parquet | One file per candidate, full pipeline data | Notebook analysis, programmatic what-if |
| SQLite | All candidates, all stages, all metrics, queryable | Ad-hoc SQL analysis, future ML feature store |
| Strategy YAML | Top candidate config, deployable | Live trading |
### 10.2 SQLite Schema Design Principles (ML-Ready)
The SQLite schema must be designed at the start of implementation — not retrofitted. Design principles:
- **One row per candidate per stage** — not denormalised blobs. Each stage result is a separate table row with foreign key to the candidate.
- **All numeric metrics are individual columns** — not JSON-serialised. This allows direct `SELECT`, `GROUP BY`, `WHERE` queries without parsing.
- **Parameter values are individual columns** — one column per optimizable parameter. Enables `WHERE rsi_overbought > 70 AND atr_multiplier < 2.0` queries.
- **Timestamps on all rows** — enables time-series analysis of the optimization run itself.
- **No information destroyed** — if `MetricsReport` has 17 fields, all 17 are columns. The ML layer will decide which are features.
### 10.3 Future ML Hook
The SQLite database is the ML data source. No ML code is implemented in v1. The schema is designed so that:
- A future ML model can `SELECT * FROM candidates WHERE stage = 'MC' AND verdict != 'REJECTED'` and immediately have a feature matrix
- Feature engineering (ratios, normalizations) can be done in a view or at query time — no schema changes needed
- A future AI assistant can query the database with natural language → SQL and get answers about any historical run
---
## 11. Non-Functional Requirements
### 11.1 Performance
| Requirement | Target |
|---|---|
| Full pipeline runtime | ≤ 4 hours on Windows PC with up to 6 parallel workers |
| Single candidate evaluation (core mode) | Consistent with current strategy `core` mode performance |
| Candidate store write throughput | Sufficient for 6 parallel workers without lock contention |
| Resume overhead | < 30 seconds to reconstruct state and resume |
### 11.2 Reliability
- Individual candidate failures do not propagate to the pipeline (isolated worker processes)
- All failures are logged with full diagnostic context before the candidate is marked as failed
- The pipeline does not produce partial output — each stage either completes fully or checkpoints cleanly
### 11.3 Windows Compatibility
- `ProcessPoolExecutor` with `spawn` start method (Windows default) — no `fork`-dependent code
- All file paths use `pathlib.Path` — no hardcoded Unix separators
- All file writes use explicit encoding (`utf-8`) — no platform-default encoding assumptions
- Temporary files written to a configurable temp directory, cleaned up after each candidate
### 11.4 Observability
- Structured logging (reusing `structured_logger.py` from the strategy architecture)
- Log levels configurable in YAML per component
- Progress reporting: current stage, candidates completed / total, estimated time remaining
- On run completion: summary table printed to console and log (stage, candidates evaluated, time taken, top fitness score)
---
## 12. Open Decisions for Design Phase
These questions are explicitly deferred. They must be answered and documented in the functional/technical specification before implementation begins.
| ID | Decision | Options | Implication |
|---|---|---|---|
| D-01 | Strategy integration mode | Direct orchestrator call / subprocess / module-level | Performance vs. isolation vs. Windows process model; requires timing prototype in Phase 1 |
| D-02 | Candidate store write concurrency | SQLite WAL mode / single-writer queue / per-candidate file then merge | Throughput and reliability under 6 parallel workers; prototype required in Phase 2 |
| D-03 | Temporary YAML lifecycle | One per run (overwritten) / one per candidate (named by hash) | Debuggability vs. disk usage |
| D-04 | GA population seeding | Top-N from MC_PREFILTER_PASS only / top-N + diversity-selected | Genetic diversity vs. convergence speed |
| D-05 | GA lightweight WFO window selection | Fixed 2 windows from config / automatically selected (most recent + most diverse) | Stability of GA fitness signal |
| D-06 | Candidate counts at each stage transition | How many proceed from Random→MC Pre-filter, MC Pre-filter→GA, GA→Full WFO, WFO→MC Deep, MC Deep→Sensitivity | Runtime budget allocation; must sum to ≤ 4 hours |
| D-07 | Composite verdict thresholds | What WFO consistency score constitutes go vs. borderline; what ruin probability ceiling triggers borderline vs. fail | Central to the go/no-go logic; calibrate against first real run in Phase 6 |
| D-08 | Sensitivity map scope | All optimizable parameters / top-3 by fitness impact only | Runtime cost of Stage 6 vs. coverage |
| D-09 | Parquet vs. JSON per candidate | Both / one or the other / configurable | Storage size vs. compatibility |
| D-10 | HTML report generator | Extend existing `ReportGenerator` / build new | Code reuse vs. clean separation; the backtester report is structurally different (multi-candidate, multi-stage) |
---
## 13. High-Level Project Plan
### Phase 1 — Design
**Deliverables**: Functional specification, technical specification, all inter-module contracts as frozen dataclasses, SQLite schema, integration mode decision (with benchmark data)
**Key activities**:
- Resolve all open decisions (Section 12) — D-01 and D-02 require prototype benchmarks
- Define all contracts: `CandidateParameterSet`, `CandidateResult`, `FitnessResult`, `ScenarioProfile`, `WFOWindow`, `WFOWindowResult`, `MCResult`, `SensitivityProfile`, `VerdictResult`
- Design SQLite schema (all tables, columns, foreign keys, indexes) — ML-ready from day one
- Define scenario profile structure and built-in scenarios (capital_accumulation, swing_trading, conservative)
- Write `backtest_template.yaml` full specification (all valid keys, types, defaults, constraints)
- Benchmark strategy integration options and select one
### Phase 2 — Core Infrastructure
**Deliverables**: `candidate_store.py`, `parameter_space.py`, `sampler.py`, `scenario.py`, `strategy_runner.py`, `fitness.py`, `ranker.py`, `orchestrator.py` (skeleton with all 8 stage stubs)
**Key activities**:
- Implement and test CandidateStore first — everything else depends on it
- Implement parameter expansion, LHS sampling, and zone validation
- Implement scenario loading and profile building
- Implement strategy integration (single candidate, end-to-end, with significance guard)
- Implement fitness scoring with scenario weights and scenario-specific constraints
- Implement orchestrator skeleton with stage sequencing, checkpointing, and resume logic
- Integration test: single candidate, full round trip, stored in SQLite with correct stage label
### Phase 3 — Optimization Engines
**Deliverables**: `ga/` package (5 modules), `wfo/` package (3 modules), MC pre-filter mode in `monte_carlo/mc_engine.py`
**Key activities**:
- Implement WFO engine first (required by GA for WFO-aware fitness)
- Implement GA engine with WFO-aware per-generation fitness
- Implement MC pre-filter mode (lightweight, 2 perturbation types)
- Validate GA produces only zone-valid candidates across 100+ generations
- Validate WFO consistency score is stable and meaningful against known reference data
- Integration test: Random → MC Pre-filter → GA → Full WFO end-to-end, all results in SQLite
### Phase 4 — Monte Carlo Deep and Verdict
**Deliverables**: `monte_carlo/` complete package (4 modules, both modes), `evaluation/sensitivity.py`, `evaluation/verdict.py`
**Key activities**:
- Implement full MC stress test (all perturbation types, vectorised equity path simulation)
- Implement parameter sensitivity map (Stage 6 — perturb, evaluate, compute fitness delta)
- Implement composite verdict engine with two-pillar logic and sensitivity spike modifier
- Integration test: full 8-stage pipeline on real data, verdict produced for top candidates
### Phase 5 — Output Layer
**Deliverables**: `report_generator.py`, `yaml_generator.py`, HTML report (scenario-framed), JSON/Parquet output, SQLite query validation suite
**Key activities**:
- Implement scenario-framed HTML report (leads with scenario-relevant metrics)
- Implement JSON/Parquet per-candidate export
- Validate SQLite schema supports required ad-hoc queries (write and run a query suite)
- Implement trading-ready YAML generation with scenario metadata and StrategyConfig validation
- End-to-end system test: full pipeline run on real WBWS data, all outputs produced and validated
### Phase 6 — Hardening and Delivery
**Deliverables**: Full test suite, performance validation report, Windows compatibility certification, complete documentation
**Key activities**:
- Validate full pipeline completes within 4-hour target on target hardware
- Profile and resolve bottlenecks if over budget (tuning levers: sample counts, MC iterations, stage transition candidate counts)
- Validate resume-after-interruption at each of the 8 checkpoints
- Validate parallel worker isolation: kill one worker mid-run, confirm pipeline continues
- Calibrate verdict thresholds against first real run results (D-07)
- Final documentation: module reference, YAML configuration guide, scenario authoring guide, output format guide, SQLite query cookbook
---
## 14. Risk Register
| ID | Risk | Probability | Impact | Mitigation |
|---|---|---|---|---|
| R-01 | Strategy integration mode chosen in Design phase proves too slow for 4-hour target | Medium | High | Benchmark all three options in Phase 1 before committing; design phase must include a timing prototype |
| R-02 | SQLite write contention under 6 parallel workers causes corruption or slowdown | Medium | High | Prototype SQLite WAL mode with 6 concurrent writers in Phase 2 before full implementation |
| R-03 | Windows `ProcessPoolExecutor` spawn overhead is prohibitive for large candidate counts | Low | High | Measure worker spawn cost in Phase 2; consider process pool reuse pattern if needed |
| R-04 | GA evolves into parameter regions that produce valid YAMLs but invalid strategy runs | Medium | Medium | `strategy_runner.py` must treat all strategy failures as candidate failures — never propagates |
| R-05 | GA WFO-aware fitness (2 windows per candidate per generation) pushes total runtime over 4-hour target | Medium | High | This is the highest new risk from the revised pipeline. Mitigate: reduce GA generations, reduce GA population size, use fastest 2 windows in config. Must be profiled in Phase 3 before accepting. |
| R-06 | Full pipeline runtime exceeds 4-hour target | Medium | Medium | Phase 6 profiling; primary levers: reduce sample counts, GA generations, MC iterations, stage transition candidate counts. All configurable in YAML — no code changes needed. |
| R-07 | Verdict thresholds (D-07) are set incorrectly on first production use | Medium | Medium | Thresholds are YAML-configurable, not hardcoded. Phase 6 calibration run on real data. Document recommended starting values in configuration guide. |
| R-08 | SQLite schema designed in Phase 1 is insufficient for ML use cases discovered later | Low | Medium | ML schema review in Phase 1 design; adding columns is safe, removing is not. All numeric fields as individual columns from day one. |
---
*Document produced from design session on 2026-02-27.*
*v1.1 — Updated 2026-02-27: corrected evidence pillars (MC robustness + multi-period WFO only), added scenario-based backtesting (Section 2.4, Section 4.10), revised pipeline sequence to Random → MC Pre-Filter → GA (WFO-aware) → Full WFO → MC Deep → Sensitivity → Report, added long-term platform context (Section 1b), updated all dependent sections.*
*Next step: Design Phase — resolve open decisions D-01 through D-10, define contracts, produce functional and technical specification.*