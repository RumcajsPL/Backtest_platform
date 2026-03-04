# ARCHITECTURE.md — Backtesting & Optimization Framework
**Version**: 2.0.0
**Date**: 2026-03-04
**Status**: Block 8A complete. §4–6 pending Block 8B.
**Audience**: Any developer working on any aspect of the backtester pipeline.
**Promise**: This document describes what the code **actually does**, verified against source.
No aspirational content. Every claim was confirmed by code review during Block 8.
---
## §1 — What This System Does
An 8-stage automated parameter optimization pipeline for the WBWSStrategy. Given a parameter
space definition and a strategy base config, it searches for parameter combinations that are:
- **Robust across time** — Walk-Forward Optimization (WFO) across multiple non-overlapping windows
- **Robust under market noise** — Monte Carlo simulation of trade-sequence perturbations
- **Not fragile to small changes** — Parameter sensitivity analysis (±1/±2 grid steps)
It produces a **verdict** (`auto_go` / `borderline` / `no_go`) for each surviving candidate
and — for `auto_go` and `borderline` verdicts — a trading-ready strategy YAML file.
**One run = one config hash.** Every run is identified by its `config_hash` (SHA-256 of
`backtest_template.yaml`). Resumable at any of 8 checkpoints. All state lives in SQLite WAL.
---
## §2 — Repository Layout
```
src/backtesting/
├── orchestrator.py          ← Pipeline entry point. Sequences all 8 stages.
│                              Stages 0, 5, 6, 7 fully implemented.
│                              Stages 1–4 are stubs (pending Phase 4).
├── contracts.py             ← ALL inter-module contracts (frozen dataclasses + enums).
│                              Single import for all shared types. Never raw dicts.
├── candidate_store.py       ← SQLite WAL store. Single-writer queue. Thread-safe.
│                              All writes: non-blocking enqueue. All reads: direct.
├── parameter_space.py       ← Expands YAML zone definitions → discrete parameter grids.
├── sampler.py               ← LHS / random sampling over expanded parameter space.
├── scenario.py              ← Loads ScenarioProfile from config dict.
├── strategy_runner.py       ← Single candidate evaluation. Writes temp YAML. Never raises.
│                              _PARAM_KEY_MAP is the ONLY file that knows strategy YAML keys.
├── fitness.py               ← Stateless. MetricsReport + ScenarioProfile → FitnessResult.
├── ranker.py                ← Stateless. Query spec → ranked CandidateRecord list.
├── report_generator.py      ← Self-contained HTML + JSON + Parquet. Reads from store.
├── yaml_generator.py        ← Merges params into base YAML. Embeds backtester metadata.
├── ga/
│   ├── population.py        ← Init from MC_PREFILTER_PASS. Elite extraction.
│   ├── selection.py         ← Tournament selection.
│   ├── crossover.py         ← Uniform crossover (zone_name from parent_a).
│   ├── mutation.py          ← Gaussian on step grid, clamped to zone bounds.
│   ├── diversity.py         ← Hybrid Euclidean/Hamming distance penalty.
│   └── ga_engine.py         ← Full evolution loop. Writes all candidates to store.
├── wfo/
│   ├── window_generator.py  ← YAML → sorted WFOWindow list (min 3, no overlaps).
│   ├── wfo_evaluator.py     ← One candidate × one window → WFOWindowResult. Never raises.
│   ├── wfo_engine.py        ← "lightweight" (GA) + "full" (Stage 4) modes.
│   └── consistency_scorer.py← WFOWindowResults → 4 sub-metrics → composite [0,1].
├── monte_carlo/
│   ├── perturbation.py      ← Named perturbation profiles from YAML.
│   ├── equity_simulator.py  ← Vectorised np.cumsum. No Python loops over paths.
│   ├── mc_metrics.py        ← avg_equity, worst_dd, ruin_prob, p5_equity. Vectorised.
│   └── mc_engine.py         ← pre-filter + deep dispatch. Never raises.
└── evaluation/
    ├── sensitivity.py       ← ±1/±2 step perturbation. ProcessPoolExecutor workers.
    │                          OPT-01: single pool shared across all candidates (Block 7C).
    └── verdict.py           ← Two-pillar + modifier flags → VerdictResult. Never raises.

configs/backtesting/
└── backtest_template.yaml   ← Single source of truth for all pipeline configuration.
                               Changing this file creates a new config_hash → new run.
```
---
## §3 — Stage Execution Model
### Pipeline overview
```
Stage 0: Validation & Init       → Checkpoint.RUN_INITIALISED
Stage 1: Random Search           → Checkpoint.RANDOM_SEARCH_COMPLETE  [STUB]
Stage 2: MC Pre-Filter           → Checkpoint.MC_PREFILTER_COMPLETE    [STUB]
Stage 3: GA Evolution            → Checkpoint.GA_COMPLETE              [STUB]
Stage 4: Full WFO                → Checkpoint.WFO_COMPLETE             [STUB]
Stage 5: MC Deep                 → Checkpoint.MONTE_CARLO_COMPLETE
Stage 6: Parameter Sensitivity   → Checkpoint.SENSITIVITY_COMPLETE
Stage 7: Report & Output         → Checkpoint.COMPLETE
```
**IMPORTANT**: Stages 1–4 are currently stubs. They log "not yet implemented" and advance
their checkpoint without producing any output. When the pipeline runs today, Stages 5–7
consume data that was loaded into the DB manually (from test fixtures or a prior partial run).
This is a temporary development state. See OPERATOR_RUNBOOK §3 for current operating procedure.
### Checkpoint system
After each stage completes, `store.set_checkpoint(run_id, Checkpoint.X)` is called.
On resume, `_execute_pipeline` compares `store.get_checkpoint(run_id).value` against
each stage's target checkpoint value using `<`. Stages with `.value ≤` current checkpoint
are skipped. This makes every stage boundary a safe interruption point.
Checkpoints are stored in the `runs` table. `set_checkpoint()` uses an SQL `UPDATE` — it
only advances the checkpoint, never retreats it. This is enforced implicitly by the
monotonic `<` comparison in `_execute_pipeline`.
### Stage 0 — Validation & Init
Validates in this order:
1. `load_scenario()` — scenario name exists in config, all required fields present.
2. `_validate_wfo_windows()` — min 3 windows, unique IDs, valid ISO dates, start < end.
3. `_validate_parameter_names()` — all enabled zone parameter names exist in `_PARAM_KEY_MAP`
   (M-05 fix, Block 7B). Catches typos before any evaluation begins.
4. `enabled_zones` check — at least one zone is enabled.
The ordering is significant: parameter name validation (step 3) occurs before the enabled
zone count check (step 4) so that errors surface with the most useful error message.
### Stage 5 — MC Deep
Reads: `store.rank_by_wfo(run_id, top_n=input_count)` → top candidates by WFO score.
For each: calls `run_mc(..., mode=MCMode.DEEP, ...)` → `MCResult`.
Writes: `store.write_mc_result(result, run_id)` — even on error (`result.ruin_probability=None`).
On error: logs WARNING and continues. `None` ruin probability → `NO_GO` in Stage 7 verdict.
### Stage 6 — Parameter Sensitivity (OPT-01)
Reads: `store.rank_by_wfo(run_id, top_n=input_count)` → top candidates.
Opens ONE `ProcessPoolExecutor` for all candidates (pool reuse, Block 7C OPT-01).
For each: calls `evaluate_sensitivity(..., pool=pool)` → `SensitivityProfile`.
Writes: `store.write_sensitivity_profile(profile, run_id)`.
Pool is closed once after all candidates are processed — spawn overhead paid once.
**Windows spawn constraint**: On Windows, `ProcessPoolExecutor` uses spawn mode.
Child processes are fresh Python interpreters — they do not inherit parent-process patches.
`unittest.mock.patch` decorators on worker functions have no effect in child processes.
For integration tests of Stage 6 behaviour, patch at the orchestrator level
(`src.backtesting.orchestrator.evaluate_sensitivity`), not at the worker level.
### Stage 7 — Report & Output
For each top candidate:
1. Fetches `WFOConsistencyScore`, `MCResult` (DEEP mode), `SensitivityProfile` from store.
2. Calls `compute_verdict()` → `VerdictResult`.
3. For `AUTO_GO` / `BORDERLINE`: calls `generate_trading_yaml()` → trading-ready YAML file.
4. Rebuilds `VerdictResult` with `yaml_output_path` populated (frozen dataclass, new instance).
5. Writes `VerdictResult` to store.
Then calls `generate_report()` → HTML + JSON + Parquet output.
**Known deferred fields** (always `None` in current production state):
- `VerdictResult.parameter_region_width` — deferred to Block 8 ML layer (WF-07).
- `VerdictResult.median_oos_delta` — always `None` due to B8-001 (persistence gap in store).
  Fixed in Block 8A: see `BLOCK8_AUDIT_REPORT.md` B8-001.
---
*Pending Block 8B analysis. Will cover `fitness.py`, `wfo/`, and `monte_carlo/` modules.*
---
## §4 — Evaluation Data Flow
### Fitness evaluation
```
CandidateResult  ──►  fitness.evaluate_fitness(result, scenario)  ──►  FitnessResult
                            │
                            ▼
                    _CONSTRAINT_CHECKS (6 checks, cheapest first, fail-fast)
                            │
                    All pass?  ──► _compute_weighted_score(metrics, scenario)
                                        │
                                        ▼
                                   fitness_score ∈ [0, 1]
```
**Constraint boundary semantics**: All lower-bound constraints use `op.lt` (reject when
`actual < threshold`), so a value exactly equal to the threshold is **accepted**. All
upper-bound constraints use `op.gt`, so a value exactly at the threshold is accepted.
This implements `>=` for minimums and `<=` for maximums throughout.
**NaN handling** (B8B-001, fixed Block 8B): A NaN metric value would previously silently
pass all constraint checks due to IEEE 754 comparison semantics (`NaN < x` is always
`False`). An explicit NaN guard is now applied before the constraint loop.
**Normalisation constants**: All fitness scoring constants are scenario-configurable
after the M-02 fix (Block 7B). Exception: expectancy scale (3.0) is still hardcoded
(B8B-003, deferred to Block 9).
### WFO evaluation
```
[Stage 4]
candidates × windows  ──►  ProcessPoolExecutor
                                │
                          evaluate_window(candidate, window, ...)  [wfo_evaluator]
                                │  never raises
                                ▼
                          WFOWindowResult (fitness_score, net_pnl, max_drawdown, oos_delta=None*)
                                │
                          store.write_wfo_window_result()
                                │
                    [all windows done for candidate]
                                │
                          compute_consistency(window_results, ...)  [consistency_scorer]
                                │
                          WFOConsistencyScore (composite_score, median_oos_delta*)
                                │
                          store.write_wfo_consistency_score()
```
`*` **OOS gate limitation** (B8B-005): `oos_delta` is always `None` in the current
implementation. `evaluate_window` returns `oos_delta=None` and `wfo_engine` has no code
to populate it. Consequently: `oos_gate_triggered` is always `False`, `median_oos_delta`
is always `None` from real data, and `enforce_oos_gate: true` has no effect. Full
IS/OOS delta computation deferred to Block 9. See OPERATOR_RUNBOOK §9.
**Sigmoid scale limitation** (B8B-012): The `median_return_norm` sub-metric uses a
sigmoid with `scale=0.10`, calibrated for unit returns (fractions). With real strategy
data where `net_pnl` is in currency points, this sub-metric becomes effectively binary.
Calibration required before first production run. See OPERATOR_RUNBOOK §9.
**WFO modes**:
- `lightweight` (GA, Stage 3): 2 random windows per generation. Results used in-memory
  for GA fitness only. Not written as consistency scores to store.
- `full` (Stage 4): All configured windows. Results and scores written to store.
  `flag_candidate_wfo_insufficient()` called for candidates failing >50% of windows.
---
## §5 — WFO Window Model
```
backtest_template.yaml
  walk_forward.windows:
    - id: W01  start: 2025-09-15  end: 2025-10-03
    - id: W02  start: 2025-10-06  end: 2025-10-24
    ...
          │
          ▼
    window_generator.py  ──►  List[WFOWindow]  (sorted, validated, min 3)
          │
          ├── GA (lightweight): rng.sample(windows, k=2)  per generation
          │
          └── Stage 4 (full): all windows
```
**Window constraints** (all enforced at Stage 0):
- Minimum 3 windows (required for GA random sampling to have meaningful diversity)
- Unique window IDs
- `start_date < end_date` for each window
- No overlapping date ranges
**Date types**: `WFOWindow.start_date` and `end_date` are Python `date` objects (not
`datetime`). `evaluate_window` passes them directly to `strategy_runner.evaluate()` as
`date_start`/`date_end` keyword arguments.
**IS/OOS split**: Currently not implemented. Each WFO window is evaluated as a single
date range. The "IS" vs "OOS" distinction exists in config/intent but not in execution.
`oos_delta` will remain `None` until Block 9 implements the split. (B8B-005)
---
## §6 — MC Path Model
```
CandidateResult.trades  ──►  extract_trade_returns()  ──►  trade_returns: np.ndarray
                                                                  │
simulate_paths(trade_returns, n_iterations, profile, seed, ...)   │
      │                                                            │
      ▼                                                            │
equity_paths: shape (n_iterations, n_trades+1)  ◄─────────────────┘
      │
      ▼
compute_metrics(equity_paths, starting_equity, ruin_threshold)
      │
      ├── avg_final_equity         = mean(equity_paths[:, -1])
      ├── ruin_probability         = fraction of paths where min(path) <= ruin_floor
      ├── worst_drawdown           = max per-path (running_max - equity) / running_max
      │                              [M-04: ruined paths clamped to 1.0]
      └── p5_final_equity          = 5th percentile of final equity  [reporting only]
```
**Seed model**: A single seed is passed to all candidate MC runs within a stage.
This is intentional — identical random perturbations across candidates makes their
MC results directly comparable (same random shocks applied to each trade history).
Reproducibility is guaranteed: same seed → same `equity_paths` for same `trade_returns`.
**Ruin threshold**: Read from config dict in `mc_engine`. `ScenarioProfile` also carries
`mc_prefilter_ruin_threshold` (loaded but currently not consumed by `mc_engine`).
Both sources must agree in `backtest_template.yaml`. (B8B-013, deferred B9)
**Perturbation profiles**: Named profiles loaded from config by `perturbation.py`.
Pre-filter mode uses a subset of perturbation types (shuffle + spread noise).
Deep mode uses all configured types.
**p5_final_equity**: Computed and stored for reporting enrichment. Not used in
verdict logic. `VerdictResult` has no `p5` field. (B8B-017)
---
## §7 — Contract Catalogue
All contracts are **frozen dataclasses** in `src/backtesting/contracts.py`.
Never pass raw dicts between modules. Always use `CandidateParameterSet.create()` factory.
| Contract | Produced by | Consumed by | Key fields | None-path notes |
|---|---|---|---|---|
| `RunMetadata` | `orchestrator` | store, `yaml_generator` | `run_id`, `config_hash`, `wfo_window_ids`, `checkpoint` | No Optional fields except `completed_at` (not in dataclass — DB only) |
| `ScenarioProfile` | `scenario` | `fitness`, `wfo_evaluator`, `consistency_scorer`, `verdict`, `report_generator` | fitness weights, constraint thresholds, verdict floors, M-02/03 normalisation constants | No Optional fields |
| `CandidateParameterSet` | `sampler`, `ga_engine` | `strategy_runner`, `wfo_evaluator`, `sensitivity` | `candidate_id` (SHA-256 of params), `zone_name`, `parameters` | `generation`: None for Random Search |
| `CandidateResult` | `strategy_runner` | `fitness`, `mc_engine` | `metrics`, `trades`, `total_trades`, `error` | `metrics`, `trades`, `total_trades`: all None on error |
| `FitnessResult` | `fitness` | `orchestrator` (→ store) | `fitness_score`, `passed_constraints`, `actual_*` | `fitness_score`: None when constraints failed; `failing_value`: None for non-numeric failures |
| `WFOWindow` | `window_generator` | `wfo_evaluator`, `ga_engine` | `window_id`, `start_date`, `end_date` | No Optional fields |
| `WFOWindowResult` | `wfo_evaluator` | `consistency_scorer`, store | `fitness_score`, `net_pnl`, `win_rate`, `oos_delta` | All metrics: None on error; `oos_delta`: None when OOS gate disabled |
| `WFOConsistencyScore` | `consistency_scorer` | store, `verdict`, ranker | `composite_score`, `fraction_positive_windows`, `oos_gate_triggered`, `window_collapse_flag`, `median_oos_delta` | `median_oos_delta`: None when no windows carry oos_delta; persisted correctly after B8-001 fix |
| `MCResult` | `mc_engine` | store, `verdict` | `ruin_probability`, `avg_final_equity`, `p5_final_equity`, `error` | All metrics: None on error; `ruin_probability=None` → `NO_GO` in verdict |
| `SensitivityProfile` | `sensitivity` | store, `verdict` | `spike_detected`, `spike_parameters`, `profile_complete` | `profile_complete=False` → `sensitivity_profile_incomplete` modifier flag → BORDERLINE demotion |
| `VerdictResult` | `verdict` | store, `yaml_generator`, `report_generator` | `verdict` (enum), `deployment_status`, `evidence_summary` | `yaml_output_path`: None for NO_GO; `parameter_region_width`: always None (WF-07 deferred); `median_oos_delta`: None until B8-001 fixed |
| `CandidateRecord` | `orchestrator` | store | All stage data flattened to primitives for SQLite | Most fields Optional; `wfo_median_oos_delta` always None from `query_candidates()` until B8-002 fixed |
---
## §8 — CandidateStore Threading Model
**Write path**: All writes are non-blocking. Callers call `write_*` methods which
`queue.put()` a `(method_name, payload)` tuple. One daemon writer thread (`_drain_queue`)
pulls items and dispatches via `getattr(self, method_name)(payload)`. All SQLite
`INSERT`/`UPDATE` operations happen on this single thread — eliminating write contention.
**Read path**: Direct synchronous queries. WAL mode allows concurrent readers while
the writer thread is active.
**Flush semantics**: `store.flush()` calls `queue.join()` — blocks until the queue
is empty. Called after each batch of writes in orchestrator stages.
**Close semantics**: `store.close()` flushes, sends `_STOP_SENTINEL` to stop the writer
thread, joins the thread, closes the SQLite connection. Always called in `orchestrator.run()`
`finally` block — prevents data loss on exception.
**Writer dispatch safety**: All 9 dispatch method names are verified to exist as class
methods (Block 8A audit). A typo in a new `write_*` method's dispatch string would cause
silent write loss — the writer thread catches `AttributeError`, logs it, and continues.
See B8-004 for documentation of this risk.
---
## §9 — Verdict Logic — Two-Pillar Model
Two mandatory pillars must both pass for `AUTO_GO`. Either failing → `NO_GO` or `BORDERLINE`.
```python
# Pillar 1 — WFO composite score (from consistency_scorer)
wfo_pillar_go    = wfo_composite >= wfo_go_floor        # INCLUSIVE at go threshold
wfo_pillar_no_go = wfo_composite < wfo_borderline_floor  # strictly less than
# Pillar 2 — MC deep ruin probability
mc_pillar_go    = ruin_prob <= mc_go_ceiling             # INCLUSIVE at go threshold
mc_pillar_no_go = ruin_prob > mc_borderline_ceiling      # strictly greater than
# ruin_prob is None → mc_pillar_no_go = True → NO_GO  (MC failure = conservative NO_GO)
```
**Modifier flags** — any one present demotes `AUTO_GO` → `BORDERLINE`. Cannot override `NO_GO`.
| Flag | Condition |
|---|---|
| `sensitivity_spike` | `sensitivity.spike_detected == True` |
| `oos_gate_triggered` | `oos_gate_enabled == True` AND `wfo_score.oos_gate_triggered == True` |
| `window_collapse_flag` | `wfo_score.window_collapse_flag == True` |
| `sensitivity_profile_incomplete` | `sensitivity.profile_complete == False` |
**Verdict decision tree**:
```python
if wfo_pillar_no_go or mc_pillar_no_go:
    verdict = NO_GO
elif wfo_pillar_go and mc_pillar_go and not any_modifier_flag:
    verdict = AUTO_GO
else:
    verdict = BORDERLINE
```
**Deployment status**: Always `PAPER_TRADE_REQUIRED` for `AUTO_GO` and `BORDERLINE`.
`VerdictResult.__post_init__` raises `ValueError` if `LIVE_APPROVED` is set in code —
operator must manually promote via DB update after paper trading validation.
---
## §10 — Known Deferral Decisions
| Decision | Rationale | Expected resolution |
|---|---|---|
| `parameter_region_width` always None | Requires ML density estimation layer not yet built | B9 / v2 |
| Stages 1–4 as stubs | Phase 4 implementation not yet started | B9 / Phase 4 sprint |
| `_PARAM_KEY_MAP` dot-path staleness check | Requires strategy package in test environment | B9 |
| `CandidateStore.find_resumable_run()` abstraction | Raw SQL in orchestrator is read-only and low-risk | B9 |
| Writer dispatch map at class level | No current missing handlers; risk is documented | B9 |
| Stages 1–4 timing | Will be added when stubs are replaced | Phase 4 sprint |
---
## §11 — Architecture Principles Quick Reference
| # | Principle | Current compliance |
|---|---|---|
| P1 | Single Responsibility | ✓ Each module has one domain. `strategy_runner` is the only module that calls into `src/strategies/`. `candidate_store` is the only module that writes SQLite. |
| P2 | Contracts Are the Interface | ✓ All inter-module types are frozen dataclasses. One violation: raw SQL in `_resume_or_start` (B8-009, P3, deferred). |
| P3 | Immutability | ✓ All contracts are `frozen=True`. No `field(default_factory=...)` patterns. |
| P4 | Explicit Over Implicit | ✓ Stage gating is explicit checkpoint comparison. One P4 note: stub stages advance checkpoints without work (B8-007, documented). |
| P5 | Vectorisation First | ✓ MC path simulation uses `np.cumsum`. Verified in `equity_simulator.py`. Full audit pending 8B. |
| P6 | Fail Fast | ✓ Stage 0 validates scenario, windows, parameter names, zone count. Gap: `min_significant_trades=0` not validated (B8-005, fixed). |
| P7 | Single Source of Truth | ✓ `backtest_template.yaml` is the config source. `_PARAM_KEY_MAP` dot-paths are a second source of truth for strategy schema (B8-006, documented). |
| P8 | Cache Lifecycle | ✓ `clear_all_caches()` in every `strategy_runner.evaluate()` finally block. Full audit pending 8B (wfo_engine window boundary). |
| P9 | Code Hygiene | ✓ No `print()` in production code. All logging via `structured_logger` / stdlib logger. |
| P10 | Immutable Run Artifacts | ✓ `INSERT OR IGNORE` for run records. No update path for config_hash or seeds. Checkpoints only advance. |
---
## §12 — Changelog
| Version | Date | Change |
|---|---|---|
| 1.0.0 | 2026-03-02 | Initial — Phase 6. Full module map, data flow, contract table, verdict logic, store threading model. |
| 1.1.0 | 2026-03-03 | Block 4: Windows spawn mock patching constraint. Verdict grid from Block 5. Performance baseline. OPT table. |
| 1.2.0 | 2026-03-03 | Block 6: Stage counts updated. Verdict grid updated to capital_accumulation thresholds. |
| 2.0.0 | 2026-03-04 | Block 8A: Full rewrite for production readiness. §1–3 (module map, contract catalogue, stage execution model), §7–11 (store threading, verdict logic, deferral decisions, principles). §4–6 pending 8B. |