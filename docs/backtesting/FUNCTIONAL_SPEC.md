# FUNCTIONAL_SPEC.md
## Backtesting & Optimization Framework — Functional Specification
**Version**: 1.0.0
**Date**: 2026-02-27
**Phase**: Phase 1 — Design
**Status**: Complete

---

## Table of Contents
1. [Purpose and Scope](#1-purpose-and-scope)
2. [System Inputs](#2-system-inputs)
3. [Stage 0 — Validation and Initialisation](#3-stage-0--validation-and-initialisation)
4. [Stage 1 — Random Search](#4-stage-1--random-search)
5. [Stage 2 — MC Pre-Filter](#5-stage-2--mc-pre-filter)
6. [Stage 3 — Genetic Algorithm](#6-stage-3--genetic-algorithm)
7. [Stage 4 — Full Walk-Forward Optimisation](#7-stage-4--full-walk-forward-optimisation)
8. [Stage 5 — Monte Carlo Deep](#8-stage-5--monte-carlo-deep)
9. [Stage 6 — Parameter Sensitivity](#9-stage-6--parameter-sensitivity)
10. [Stage 7 — Final Report and Output](#10-stage-7--final-report-and-output)
11. [Cross-Cutting Behaviours](#11-cross-cutting-behaviours)
12. [Scenario System](#12-scenario-system)
13. [Verdict Logic](#13-verdict-logic)
14. [Deployment Gate](#14-deployment-gate)

---

## 1. Purpose and Scope

The Backtesting & Optimization Framework is a fully automated pipeline that systematically evaluates whether the WBWSStrategy has real trading potential and, if so, what its optimal parameter configuration is.

The system takes a configuration file and a parameter search space as input. It runs a sequence of seven substantive stages, each building on the results of the previous. At the end, it produces a structured verdict and a set of output artifacts that together support a documented go/no-go trading decision — without any additional manual analysis.

The operator's role is limited to: selecting a scenario, starting the run, and reviewing borderline cases if any appear. Everything else is automated.

---

## 2. System Inputs

**Primary input**: `backtest_template.yaml`

This file is the single source of truth for the entire run. It contains:
- The active scenario name (e.g. `capital_accumulation`)
- All parameter zones with ranges, steps, and discrete choices
- All GA configuration (population size, generations, mutation/crossover rates, diversity penalty weight)
- All WFO window definitions (fixed date pairs)
- All MC configuration for pre-filter and deep modes (iterations, perturbation profiles)
- All fitness weights and constraint thresholds (or references to the active scenario profile)
- Output format settings and parallel execution settings

**Secondary input**: Strategy data files (price data, session data) — paths referenced in the YAML. Their existence and readability is validated in Stage 0.

**Derived input**: `strategy_template.yaml` — the base strategy configuration file. The `strategy_runner` merges candidate parameters into a copy of this file for each evaluation. This file is never modified by the backtester.

---

## 3. Stage 0 — Validation and Initialisation

**Purpose**: Catch all configuration errors before any compute is spent. Either the run starts clean or it does not start at all.

**What happens**:

The orchestrator reads `backtest_template.yaml` and validates it against the full schema. Every key is checked for presence, type, and valid range. Any violation raises immediately with a descriptive error message. There are no silent defaults for required fields.

The orchestrator then validates the WFO window definitions. Each window must have a start date and end date within the bounds of the available data. Windows must not overlap each other. The total window count must be at least 3 — this is the minimum required for GA random window sampling to function correctly. If fewer than 3 windows are defined, the run aborts with a clear error message explaining the requirement.

The orchestrator validates that all referenced data files exist and are readable. It does not validate data quality beyond file existence at this stage — that is the responsibility of the strategy runner during evaluation.

If all validations pass, the orchestrator initialises the `CandidateStore`. It writes a `RunMetadata` record containing: a unique run ID (UUID), a SHA-256 hash of the full `backtest_template.yaml` content, the active scenario name, the start timestamp, all random seeds (one per stage that uses randomness), the perturbation profile name for MC, and the initial checkpoint state `RUN_INITIALISED`.

On startup, before any validation, the orchestrator checks the `CandidateStore` for a prior incomplete run. If one exists (checkpoint state is not `COMPLETE`), the operator is asked whether to resume or start fresh. If resume is selected, Stage 0 validation is still re-run to confirm the config has not changed (the stored config hash is compared to the current file hash). If the config has changed, resume is rejected and a new run must be started.

**Checkpoint written**: `RUN_INITIALISED`

**Failure behaviour**: Any validation error aborts the run immediately with a descriptive log message. The `CandidateStore` is not written to if validation fails.

---

## 4. Stage 1 — Random Search

**Purpose**: Broadly explore the parameter space to find candidates that are not immediately disqualifiable. This is discovery, not optimisation — the goal is to populate the `CandidateStore` with a diverse set of evaluated candidates that the later stages can work from.

**What happens**:

The `ParameterSampler` reads the zone definitions from the YAML and expands each zone's ranges and discrete choices into a full discrete parameter space. It then applies Latin Hypercube Sampling (or uniform random sampling, configurable in YAML) to select N candidates per zone. The total candidate count is configurable per zone. All generated combinations are validated against zone boundaries before evaluation begins — no invalid combination reaches the strategy runner.

Each candidate is represented as a `CandidateParameterSet`. The `StrategyRunner` receives one `CandidateParameterSet`, builds a temporary `strategy_template.yaml` by merging the candidate's parameters into the base template, and calls the strategy pipeline in `core` mode. This happens in parallel across up to 6 worker processes using `ProcessPoolExecutor` in Windows spawn mode.

Before any fitness or constraint evaluation, the strategy runner checks the total trade count. If the result has fewer trades than the configured `min_significant_trades` threshold, the candidate is immediately recorded as `REJECTED_INSUFFICIENT_TRADES` and no further evaluation is performed. This is a data quality gate, not a business constraint.

For candidates that pass the significance guard, the `FitnessEvaluator` applies scenario-specific hard constraints. Constraint evaluation is ordered: drawdown first (cheapest rejection), then win rate, then losing streak, then minimum trades per week, then expectancy, then profit factor. Candidates failing any constraint are recorded as `REJECTED_CONSTRAINTS` with the specific failing constraint and its value logged.

Candidates passing all constraints are scored by the weighted fitness function. The fitness function uses the scenario's configured weights applied to metrics from the `MetricsReport`. The resulting `FitnessResult` is stored alongside the full `MetricsReport` in the `CandidateStore` with stage label `RANDOM`.

After all Random Search candidates are evaluated, the orchestrator runs a statistical adequacy check. It examines the average trade count and return variance across passing candidates and compares them against the configured MC iteration count and WFO window count. If the configuration appears likely to produce statistically weak results (e.g. very low trade counts combined with low MC iterations), a warning is logged. The pipeline continues regardless — this is information for the operator, not a gate.

**Checkpoint written**: `RANDOM_SEARCH_COMPLETE`

**Failure behaviour**: Individual candidate failures are isolated. A worker process that crashes returns a failed `CandidateResult` with the error logged. The candidate is stored as `EVALUATION_ERROR`. The pipeline continues with all other candidates. A stage-level failure (e.g. the sampler itself crashes) aborts the stage and logs the error, but the checkpoint is not written, enabling clean resume.

---

## 5. Stage 2 — MC Pre-Filter

**Purpose**: Eliminate structurally fragile candidates before spending GA compute on them. A candidate whose trade sequence is so fragile that random shuffling produces ruin in 30%+ of paths is not worth evolving.

**What happens**:

The `Ranker` reads the `CandidateStore` and returns the top N candidates from the `RANDOM` stage, ranked by fitness score. N is configurable in the YAML.

The `MCEngine` runs in lightweight pre-filter mode on each of these candidates. Pre-filter mode uses a low iteration count (configurable, typically 200–500) and applies only two perturbation types: trade sequence shuffling and spread noise. This is deliberately cheap — the goal is structural fragility elimination, not statistical precision.

For each candidate, the engine simulates the configured number of equity paths. Each path applies the two perturbation types to the candidate's trade list and replays the resulting sequence. The `MCMetrics` module computes the ruin probability across all paths — the fraction of paths where the account equity falls below the configured ruin threshold (e.g. 20% of starting equity).

Candidates whose ruin probability exceeds the scenario's `mc_prefilter_threshold` are recorded as `MC_PREFILTER_FAIL`. Survivors are recorded as `MC_PREFILTER_PASS`. The perturbation profile name and iteration count used are stored in the result record for audit purposes.

**Checkpoint written**: `MC_PREFILTER_COMPLETE`

**Failure behaviour**: Individual candidate MC failures are isolated. The pipeline continues. A candidate that cannot be evaluated (e.g. insufficient trades for MC simulation) is recorded as `MC_PREFILTER_FAIL` with the specific error logged — conservative treatment.

---

## 6. Stage 3 — Genetic Algorithm

**Purpose**: Evolve better-performing candidates from the MC Pre-Filter survivors. The key design principle is that GA fitness is WFO-aware — every candidate is evaluated across multiple time windows during evolution, not just against a single-run fitness score. This produces candidates that are temporally robust by construction, not by luck.

**What happens**:

The `GAEngine` seeds its initial population from the `MC_PREFILTER_PASS` candidates in the `CandidateStore`. Population seeding strategy (top-N only vs. top-N plus diversity-selected) is resolved in D-04.

At the start of each generation, the engine randomly samples 2 windows from the full WFO window list. This sampling is independent per generation — each generation sees a different window pair. This prevents the GA from evolving candidates that are merely good on two specific time periods.

Each candidate in the population is evaluated on the 2 sampled windows. The `WFOEngine` (in lightweight mode) calls the `StrategyRunner` for each candidate-window pair. The `WFOEvaluator` returns a `WFOWindowResult` for each. The `ConsistencyScorer` combines the two window results into a lightweight consistency score for that generation.

The generation fitness for each candidate is a weighted combination of:
- Its single-run fitness score (from Stage 1 or prior GA evaluation)
- Its 2-window WFO consistency score for the current generation
- A diversity penalty computed by `ga/diversity.py` — candidates whose parameter vector is within a configurable distance of current elites receive a downward penalty on their fitness score

Elite candidates (top `elite_fraction` of the population) are preserved unchanged into the next generation. The remaining slots are filled by selection, crossover, and mutation. All three operators are zone-aware — they produce only valid `CandidateParameterSet` instances within the zone boundaries of their parents.

All candidates produced in each generation are written to the `CandidateStore` with stage `GA` and the generation number. This enables full lineage tracking — any candidate can be traced back through its ancestor chain.

The GA runs for the configured number of generations. If the population's maximum fitness does not improve for a configurable number of consecutive generations (stagnation threshold), the GA terminates early and logs the reason.

**Checkpoint written**: `GA_COMPLETE`

**Failure behaviour**: Individual candidate evaluation failures within a generation are isolated. Failed candidates are assigned the lowest possible fitness score (not removed) — the generation continues with the surviving evaluations. If an entire generation produces zero valid evaluations, the GA logs an error and terminates early; the stage checkpoint is not written.

---

## 7. Stage 4 — Full Walk-Forward Optimisation

**Purpose**: Produce definitive temporal consistency evidence. By this stage, most fragile candidates have been eliminated. This stage is evidence collection — confirming that the GA survivors' edge is genuinely repeatable across time, not just across the 2 randomly sampled GA fitness windows.

**What happens**:

The `Ranker` selects the top M candidates from the combined `RANDOM + GA` pool in the `CandidateStore`, ranked by GA fitness score. M is configurable in the YAML.

The `WFOEngine` (in full mode) evaluates each candidate against every configured WFO window. Each candidate-window pair is a single `wfo_evaluator.py` call, which runs the strategy in core mode for the window's date range and returns a `WFOWindowResult` containing the per-window fitness score, trade count, and key metric snapshot.

Once all windows for a candidate are evaluated, the `ConsistencyScorer` computes the four temporal consistency metrics:
- **Median window return**: the central tendency of per-window P&L or fitness, not skewed by outliers
- **Window-to-window return variance**: how stable the edge is across time periods
- **Worst-window drawdown**: the floor protection metric — a strategy that passes 9 of 10 windows but blows up in one is borderline
- **Fraction of positive windows**: the simple pass/fail proportion across all configured windows

These four metrics are combined into the composite `WFO consistency score` using the scenario's configured temporal weights. All four sub-metrics are stored as individual columns in the `wfo_results` SQLite table alongside the composite score.

IS/OOS delta is also computed for each candidate (performance on the test portion of each window relative to its training portion) and stored as an informational metric. If `walk_forward.enforce_oos_gate: true` is set in the YAML, candidates with IS/OOS degradation greater than 50% receive a borderline flag. This flag is informational to the verdict engine — it cannot auto-reject; only the two mandatory pillars produce auto-reject outcomes.

Candidates that perform well in most windows but collapse in one or more windows are flagged as `WFO_INCONSISTENT` in addition to their normal result record. This flag is an input to the verdict engine's borderline determination.

All WFO results are written to the `CandidateStore` with stage `WFO`.

**Checkpoint written**: `WFO_COMPLETE`

**Failure behaviour**: Window-level failures are isolated per candidate. If a candidate fails on one or more windows (strategy error, insufficient data), those windows are recorded as failed and the consistency score is computed over the remaining windows. If a candidate fails on more than half its windows, it is recorded as `WFO_INSUFFICIENT_WINDOWS` and excluded from further stages.

---

## 8. Stage 5 — Monte Carlo Deep

**Purpose**: Full probabilistic stress testing on the small population of WFO-validated candidates. By this point, the population is already temporally robust. This stage asks: is the edge also robust to execution noise, spread variation, and trade sequence randomness at full statistical precision?

**What happens**:

The `Ranker` selects the top K candidates ranked by WFO consistency score. K is configurable in the YAML.

The `MCEngine` runs in deep mode. Deep mode uses the full iteration count (configurable, typically 2,000–5,000) and applies all configured perturbation types:
- **Trade sequence shuffling**: randomise the order of trades to test whether order dependency exists
- **Return resampling**: resample trade returns with replacement (bootstrap) to test distribution sensitivity
- **Spread noise**: add random spread variation within the configured historical range
- **Risk perturbation**: vary position sizing within a configurable noise band
- **Slippage simulation**: apply configurable entry/exit slippage to each trade

For each candidate, the engine generates the configured number of equity paths. All paths are computed using vectorised numpy operations — no Python loops over individual paths. Each path applies independently sampled perturbations.

The `MCMetrics` module computes:
- **Average final equity**: the expected outcome across all paths
- **Worst drawdown across paths**: the worst equity drawdown encountered across the full simulation ensemble
- **Ruin probability**: the fraction of paths where equity falls below the ruin threshold
- **5th percentile final equity**: the downside tail — what the bottom 5% of outcomes look like

All MC results are written to the `CandidateStore` with stage `MC_DEEP`. The perturbation profile name is stored with each record.

**Checkpoint written**: `MONTE_CARLO_COMPLETE`

**Failure behaviour**: Individual candidate MC failures are isolated. A candidate that fails MC simulation (e.g. no trades, degenerate equity path) is recorded as `MC_DEEP_FAIL` with the error logged. It is excluded from Stage 6 and the final report's ranked shortlist, but appears in the full pipeline report with its failure reason.

---

## 9. Stage 6 — Parameter Sensitivity

**Purpose**: Determine whether the top candidates' fitness is contingent on precise parameter values (a fragility indicator) or robust across a neighbourhood of the parameter space (a robustness indicator). A strategy that requires exact parameters to perform is not safe to deploy — small natural parameter drift in live conditions would collapse performance.

**What happens**:

For each of the top candidates (after MC Deep, typically 3–5), the `SensitivityEvaluator` holds all parameters fixed and varies each optimizable parameter independently, one at a time, at ±1 step and ±2 steps from its current value. The step size for each parameter is the same as defined in the YAML parameter space.

Each perturbation produces a new `CandidateParameterSet`. The `StrategyRunner` evaluates each perturbed candidate in parallel. The fitness delta for each parameter at each step is computed: `delta = perturbed_fitness - baseline_fitness`.

The resulting `SensitivityProfile` shows, for each parameter, how much fitness changes as that parameter moves away from its optimised value. A flat profile (small deltas across all parameters and steps) indicates robustness. A sharp spike (one or two parameters where ±1 step produces large fitness collapse) indicates a parameter cliff — the candidate is sensitive to that parameter's exact value.

Any candidate whose sensitivity profile shows a spike above the configured `spike_threshold` receives a borderline flag, regardless of how well it performed in the two mandatory pillars. A strategy sitting on a parameter cliff is not safe to deploy even if its WFO and MC results are excellent.

All sensitivity results are written to the `CandidateStore` with stage `SENSITIVITY`, with one row per candidate per parameter per step.

**Checkpoint written**: `SENSITIVITY_COMPLETE`

**Failure behaviour**: Individual perturbation evaluation failures are isolated. If a specific parameter's perturbation fails to evaluate (e.g. a ±2 step pushes the parameter outside a valid strategy range), that data point is recorded as missing and excluded from the spike calculation for that parameter. If more than half of a candidate's perturbations fail, the entire sensitivity profile is marked as `INCOMPLETE` and the candidate receives a borderline flag by default.

---

## 10. Stage 7 — Final Report and Output

**Purpose**: Translate all pipeline evidence into structured, human-readable output that supports a documented go/no-go trading decision.

**What happens**:

The `VerdictEngine` reads all stage results for each candidate from the `CandidateStore` and applies the verdict logic (described in Section 13). Each candidate receives a `VerdictResult` with verdict (`go` / `borderline` / `no_go`), both pillar scores, all modifier flags, and a plain-language evidence summary.

The `BacktestReportGenerator` produces the following outputs:

**HTML Report**: A self-contained, single-file report covering all pipeline stages. The report is scenario-framed — it leads with the metrics most relevant to the active scenario. For `capital_accumulation`, this means consistency and frequency metrics appear first. For `swing_trading`, R:R and profit factor lead. The report includes: the run summary (scenario, configuration hash, total candidates evaluated, runtime), the full pipeline funnel (how many candidates passed each stage), the ranked shortlist of top candidates with all metrics, per-candidate charts (equity curve, WFO window performance, MC path distribution, sensitivity profile), and the full verdict with evidence summary for each candidate.

**Adversarial Checklist**: For each borderline candidate, a separate document is generated. This is a structured checklist that the operator must complete and sign off before any live deployment of that candidate. The checklist captures: which pillar was inconclusive and why, which specific windows showed collapse, the sensitivity spike parameters and their deltas, and a sign-off field. No borderline candidate may have `deployment_status` changed to `LIVE_APPROVED` without a completed checklist.

**JSON/Parquet files**: One file per candidate containing the full pipeline results across all stages. Structured for programmatic access, notebook analysis, and future ML ingestion.

**SQLite database**: The same `CandidateStore` used throughout the run — all tables, all stages, all metrics — is the final deliverable database. No data is transformed or summarised into a separate file. The database is the complete record of the run.

**Trading-ready strategy YAML**: Generated for the top-ranked candidate (or the top-ranked `go` verdict candidate if the top candidate is `borderline`). This file is a fully valid, self-contained `StrategyConfig`-compatible YAML that can be passed directly to the live strategy runner. Its metadata section includes: the scenario name, run ID, config hash, generation timestamp, and `deployment_status: PAPER_TRADE_REQUIRED`.

**Checkpoint written**: `COMPLETE`

---

## 11. Cross-Cutting Behaviours

### Logging
All logging uses `structured_logger.py` from the strategy architecture. Log levels are configurable per component in the YAML. Every log entry includes: timestamp, component name, stage, candidate ID (where applicable), and the message. Stack traces are included for all error-level entries.

### Progress Reporting
The orchestrator maintains a progress state that is updated after each candidate evaluation and after each stage completion. This state is logged at regular intervals (configurable) and printed to console as a simple progress line: current stage, candidates completed / total, estimated time remaining.

### Resume
Resume is idempotent. Running the orchestrator against a completed (`COMPLETE`) run is a no-op — it logs that the run is already complete and exits. Running against an incomplete run offers resume. Within a stage, candidates already in the `CandidateStore` (matched by parameter hash) are skipped. A candidate is only re-evaluated if it is absent from the store.

### Parallelism
All parallel execution uses `ProcessPoolExecutor` with the `spawn` start method. No code anywhere in the backtester uses `multiprocessing.Pool` or `fork`-dependent constructs. Each worker is a clean process that receives a serialised `CandidateParameterSet` and returns a serialised result contract. The strategy's `CacheManager.clear_all_caches()` is called in the `finally` block of every worker evaluation.

### Temporary Files
Each candidate evaluation produces a temporary `strategy_template.yaml` named by the candidate's parameter hash. The lifecycle of these files (cleanup timing and location) is resolved in D-03.

### Immutable Run Artifacts
Once a run is initialised, its config hash, seeds, and perturbation profile name cannot be changed. Any modification to `backtest_template.yaml` after run start is detected by hash comparison on resume and causes resume rejection. The operator must start a new run with the new config.

---

## 12. Scenario System

A scenario is a named evaluation profile that reshapes how candidates are evaluated and reported — without changing the pipeline structure or stages.

Each scenario defines:
- **Fitness weights**: the relative importance of each metric in the composite fitness score
- **Constraint thresholds**: the minimum/maximum values each metric must satisfy
- **MC pre-filter threshold**: the maximum acceptable ruin probability in Stage 2
- **Temporal weights**: how the four WFO consistency sub-metrics are combined into the composite score
- **Report emphasis**: which metrics appear prominently in the HTML report
- **Objective description**: a plain-language statement of what this scenario is optimising for

Three built-in scenarios are defined (concrete values in Section 5 of NEXT_SESSION_PLAN.md and in `backtest_template.yaml`): `capital_accumulation`, `swing_trading`, `conservative`.

Custom scenarios can be added by appending a new entry to the `scenarios:` section of `backtest_template.yaml`. No code changes are required. The system validates the new scenario against the scenario schema at Stage 0.

The active scenario is selected via the top-level `scenario: name` key in `backtest_template.yaml`. Only one scenario is active per run. The scenario name is stored in `RunMetadata` and appears in all output artifacts.

---

## 13. Verdict Logic

Each candidate receives exactly one verdict from the following three outcomes:

**auto_go**: The candidate passes both mandatory pillars with scores above the configured `go` thresholds (D-07), has no sensitivity spike flag, and no active modifier flags. No human review required.

**borderline**: The candidate passes the constraint phase (otherwise it would never reach verdicting) but at least one of the following is true:
- WFO consistency score is below the `go` threshold but above the `borderline` floor
- MC deep ruin probability is above the `go` ceiling but below the `borderline` ceiling
- A sensitivity spike flag is set (regardless of pillar scores)
- `enforce_oos_gate: true` is set and IS/OOS degradation exceeds 50%
- The `WFO_INCONSISTENT` flag is set (window collapse pattern)
- The sensitivity profile is marked `INCOMPLETE`

Borderline candidates require human review using the generated adversarial checklist. They cannot be deployed without operator sign-off.

**no_go**: Either of the following:
- A hard constraint failure at any stage (recorded as `REJECTED_CONSTRAINTS`)
- MC deep ruin probability above the `no_go` ceiling (above the `borderline` ceiling)
- WFO consistency score below the `borderline` floor
- `MC_PREFILTER_FAIL` (ruin probability exceeded in Stage 2)

**Pillar independence**: The two pillars are independent. A candidate can fail on either pillar alone and still receive `no_go`. Both pillars must pass at or above their respective `go` thresholds for an `auto_go` verdict.

**Threshold configuration**: All verdict thresholds are configurable in the YAML and scenario-specific. Initial values are calibrated in Phase 6 against the first real run. They are never hardcoded.

---

## 14. Deployment Gate

The `deployment_status` field on `VerdictResult` and in the trading-ready YAML metadata implements the operational gate between pipeline output and live capital.

**`PAPER_TRADE_REQUIRED`**: The default status for all `go` and `borderline` verdicts. The trading-ready YAML is valid and deployable to a paper/demo account. It must not be used with real capital.

**`LIVE_APPROVED`**: Set manually by the operator after the paper trading period is completed. The paper trading period is: minimum 3 months or 500 trades, whichever comes later, with live slippage and P&L within pre-specified tolerances of the backtested values. This field change is the operator's documented sign-off.

The `deployment_status` field is stored in the `verdicts` table of the SQLite database. The YAML file itself is regenerated with the updated status when the operator promotes a candidate. The prior YAML (with `PAPER_TRADE_REQUIRED`) is retained in the run artifacts for audit.