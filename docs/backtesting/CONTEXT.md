# PROJECT CONTEXT — Backtesting & Optimization Framework
## Identity
**Project**: Backtesting & Optimization Framework for WBWSStrategy
**Operator**: Single quantitative retail trader, Windows 10, eToro broker
**Stage**: Phase 5 — Orchestrator Final Wiring + Live Integration
**Last session ended**: 2026-03-01 — Completed Phase 4: All evaluation modules implemented, 68 tests pass (61 unit + 4 AV-01 smoke + 3 integration). AV-01 passed: 0 AUTO_GO verdicts on random-signal baseline.
---
## Non-Negotiables (Architecture — never override these)
1. **Contracts are the interface** — frozen dataclasses between every module. No raw dicts.
2. **Single responsibility** — one module, one concern. Orchestrator orchestrates only.
3. **Fail fast** — invalid config raises at construction. No silent fallbacks.
4. **Single source of truth** — all config from `backtest_template.yaml`. No module self-loads config.
5. **Immutability** — `frozen=True` on all contracts. `object.__setattr__` in `__post_init__` only.
6. **Windows compatibility** — `pathlib.Path`, `ProcessPoolExecutor` spawn mode, explicit `utf-8` encoding.
7. **Code hygiene** — no print statements, no debug flags, no MagicMocks, no commented-out blocks.
8. **CacheManager** — reuse existing `CacheManager` from strategy architecture. `clear_all_caches()` between runs.
9. **Immutable run artifacts** — config hash, all seeds, perturbation profile name stored immutably.
---
## Project Reference Files
| File | Purpose | Location |
|---|---|---|
| `BACKTESTER_PLAN.md` | Master requirements, architecture, pipeline design (v1.2) | `docs/backtesting/` |
| `FUNCTIONAL_SPEC.md` | Plain-language specification of all 8 stages | `docs/backtesting/` |
| `TECHNICAL_SPEC.md` | All contracts, resolved decisions, module signatures, YAML schema | `docs/backtesting/` |
| `SQLITE_SCHEMA.md` | All 9 tables with CREATE TABLE, indexes, query examples | `docs/backtesting/` |
| `CHANGE_LOG.md` | All changes + session handoff blocks | `docs/backtesting/` |
| `PROJECT_REPORT.md` | Phase progress tracker | `docs/backtesting/` |
| `ARCHITECTURE.md` | Strategy architecture (fixed input, do not modify) | `docs/strategies/architecture/` |
| `backtest_template.yaml` | Backtester config template | `configs/backtesting/` |
---
## Pipeline (DO NOT REORDER without explicit instruction)
```
Stage 0: Validation & Init     (min 3 WFO windows required — validated here for GA random sampling)
Stage 1: Random Search         (LHS, significance guard, constraint filter, single-run fitness)
                               [post-stage: log statistical adequacy warning if MC/WFO config weak]
Stage 2: MC Pre-Filter         (cheap — 2 perturbation types, ruin probability screen)
Stage 3: GA                    (WFO-aware fitness: randomly sample 2 windows per generation from
                               full window list + diversity penalty — NOT fixed window pair)
Stage 4: Full WFO              (all configured windows, 4-metric composite consistency score)
Stage 5: MC Deep               (full iterations, all perturbation types, WFO survivors only)
Stage 6: Parameter Sensitivity (±1/±2 step per parameter, fitness delta map, spike = borderline)
Stage 7: Report & Output       (HTML + borderline checklist + JSON/Parquet + SQLite + YAML)
```
---
## Verdict Model
**Two mandatory pillars**:
1. WFO temporal consistency score — composite of four metrics:
   - median_window_return, window_return_variance, worst_window_drawdown, fraction_positive_windows
2. MC deep ruin probability
**Three outcomes**: auto_go | borderline (human review) | no_go
Sensitivity spike → borderline flag even if both pillars pass.
IS/OOS delta → informational by default. `enforce_oos_gate: true` makes >50% degradation = borderline flag.
`VerdictResult.deployment_status` → always `PAPER_TRADE_REQUIRED` for go/borderline. Operator sets `LIVE_APPROVED` after paper trading.
---
## Scenario System
Each run has one active scenario (`capital_accumulation` | `swing_trading` | `conservative` | custom).
Scenario defines: fitness weights, constraint thresholds, WFO temporal weights, verdict thresholds, report framing.
All three built-in scenarios fully specified in TECHNICAL_SPEC.md Section 5 and backtest_template.yaml schema.
---
## Current Phase Status
```
PHASE:        Phase 5 — Orchestrator Final Wiring + Live Integration
COMPLETED:    - Phase 4: All evaluation modules implemented and tested.
                61 unit tests pass. 4 AV-01 smoke tests pass. 3 integration tests pass.
                AV-01: 0 AUTO_GO on 100 random-signal candidates. Pipeline thresholds validated.
              - Phase 3: All 14 optimization engine modules implemented (wfo/*, ga/*, monte_carlo/*).
                53 tests pass. Key validations: GA window sampling independence, diversity penalty
                effectiveness, MC vectorised equity simulation, consistency scorer correctness.
              - Phase 2: Both benchmarks passed, all 8 core modules implemented, contracts defined,
                all unit tests passed, integration test passed.
              - Phase 1: All specs produced, decisions resolved, contracts defined, schema designed.
   IN PROGRESS:  - CHANGE_LOG.md SESSION 5 block (append to file)
                 - PROJECT_REPORT.md update (operator handles)
                 - CONTEXT.md (this file — updated)
   BLOCKED ON:   Nothing currently blocking.
   NEXT TASK:    Phase 5 — Orchestrator Final Wiring Audit, Live Integration and Phase 5 — Output Layer 
   Start with:   orchestrator.py and candidate_store.py completness audit
```
---
## Open Decisions — ALL RESOLVED
(All 12 decisions D-01 through D-12 resolved. See TECHNICAL_SPEC.md Section 1 for full details.)

---
## Key Contracts — ALL DEFINED (TECHNICAL_SPEC.md)
- [x] `RunMetadata` — run_id, config_hash, scenario, seeds (all 5), window IDs, checkpoint, version
- [x] `ScenarioProfile` — fitness weights (6), constraint thresholds (6), MC threshold, WFO weights (4), verdict thresholds (5), report emphasis
- [x] `CandidateParameterSet` — zone_name, parameters dict, candidate_id (SHA-256 hash), generation. Use `.create()` factory.
- [x] `CandidateResult` — candidate_id, evaluated_at, metrics, trades, total_trades, error
- [x] `FitnessResult` — candidate_id, scenario_name, fitness_score, passed_constraints, rejection details, constraint actuals
- [x] `WFOWindow` — window_id, start_date, end_date
- [x] `WFOWindowResult` — candidate_id, window_id, evaluated_at, fitness_score, key metrics, oos_delta, error
- [x] `WFOConsistencyScore` — candidate_id, 4 sub-metrics, composite_score, windows_evaluated, flags
- [x] `MCResult` — candidate_id, mode, profile_name, iterations, avg_final_equity, worst_drawdown, ruin_probability, p5_final_equity, error
- [x] `SensitivityProfile` — candidate_id, baseline_fitness, ParameterSensitivity tuple, spike_detected, spike_parameters, profile_complete
- [x] `VerdictResult` — candidate_id, verdict, deployment_status (PAPER_TRADE_REQUIRED), pillar scores, flags, evidence_summary
- [x] `CandidateRecord` — flattened SQLite row with all stage fields as primitives
- [x] `ParameterSensitivity` — sub-contract for individual parameter step result
- NOTE: `Candidate` is NOT a defined contract. Use `CandidateParameterSet` for candidate objects.
---
## SQLite Schema — 9 Tables (SQLITE_SCHEMA.md)
- [x] `runs` — one row per pipeline run, immutable artifacts
- [x] `candidates` — one row per unique candidate
- [x] `candidate_parameters` — all parameter values as individual columns + JSON backup
- [x] `evaluations` — one row per candidate per stage, all constraint actuals + fitness
- [x] `wfo_window_results` — one row per candidate per window (GA lightweight + full WFO)
- [x] `wfo_consistency_scores` — four sub-metrics + composite score per candidate
- [x] `mc_results` — pre-filter and deep as separate rows per candidate
- [x] `sensitivity_results` — one row per candidate per parameter per step
- [x] `sensitivity_profiles` — summary (spike_detected, spike_parameters) per candidate
- [x] `verdicts` — final verdict + all evidence per candidate
---
## Scenario Profiles — All 3 Defined (TECHNICAL_SPEC.md Section 5)
- [x] `capital_accumulation` — win_rate + consistency focus. max_dd 15%, min_wr 45%, go WFO ≥0.65, go MC ≤5%
- [x] `swing_trading` — expectancy + profit_factor focus. min_expectancy 0.8, go WFO ≥0.60, go MC ≤7%
- [x] `conservative` — drawdown + win_rate + ruin focus. max_dd 10%, min_wr 52%, go WFO ≥0.70, go MC ≤3%
---
## Modules Implemented (Phase 2 + Phase 3 + Phase 4)
```
Phase 2 (core infrastructure):
  src/backtesting/candidate_store.py       ✓
  src/backtesting/parameter_space.py       ✓
  src/backtesting/sampler.py               ✓
  src/backtesting/scenario.py              ✓
  src/backtesting/strategy_runner.py       ✓
  src/backtesting/fitness.py               ✓
  src/backtesting/ranker.py                ✓
  src/backtesting/orchestrator.py          ✓ 

Phase 3 (optimization engines):
  src/backtesting/wfo/window_generator.py  ✓
  src/backtesting/wfo/wfo_evaluator.py     ✓
  src/backtesting/wfo/wfo_engine.py        ✓
  src/backtesting/wfo/consistency_scorer.py ✓
  src/backtesting/ga/population.py         ✓
  src/backtesting/ga/selection.py          ✓
  src/backtesting/ga/crossover.py          ✓
  src/backtesting/ga/mutation.py           ✓
  src/backtesting/ga/diversity.py          ✓
  src/backtesting/ga/ga_engine.py          ✓
  src/backtesting/monte_carlo/perturbation.py     ✓
  src/backtesting/monte_carlo/equity_simulator.py ✓
  src/backtesting/monte_carlo/mc_metrics.py       ✓
  src/backtesting/monte_carlo/mc_engine.py        ✓

Phase 4 (evaluation layer):
  src/backtesting/evaluation/sensitivity.py   ✓
  src/backtesting/evaluation/verdict.py       ✓
  src/backtesting/yaml_generator.py           ✓
  src/backtesting/report_generator.py         ✓

Phase 5 (to build):
  src/backtesting/orchestrator.py             
  tests/backtesting/integration/test_live_pipeline.py ← Live SQLite + real (or realistic) runner
```
---
## What NOT To Do
- Do not modify `ARCHITECTURE.md` or any file under `src/strategies/` — strategy architecture is frozen
- Do not invent new open decisions without logging them in `CHANGE_LOG.md`
- Do not use `analytics` mode inside the backtester loop — `core` mode only
- Do not build the ML/AI analytics layer — schema design only in v1
- Do not implement eToro API integration — future project, not this one
- Do not implement regime-aware MC perturbation profiles — v2 scope
- Do not implement true global parameter sensitivity random-walk — v2 scope
- Do not set `deployment_status = LIVE_APPROVED` anywhere in code — operator-only action
- Do not use `datetime.utcnow()` in new code — use `datetime.now(UTC)` (Python 3.12+ compatible)
- Do not import or use `Candidate` — it is not a defined contract. Use `CandidateParameterSet`.
- When testing ProcessPoolExecutor-based code: patch the worker function itself (`_evaluate_perturbation`), not functions the worker calls internally (patches do not cross process boundaries).
---
## Phase 5 Starting Point
When Phase 5 begins, the task is audit `orchestrator.py` Stages 5/6/7 full implementations.

**Pre-coding reads required:**
- `src/backtesting/orchestrator.py` — audit existing
- `src/backtesting/candidate_store.py` — audit existing
(**other on demand**)
**Known integration items to confirm before coding:**
1. `strategy_runner.py` accepts `date_start` and `date_end` kwargs — confirm exists or implement
2. `candidate_store.py` exposes: `write_wfo_window_result()`, `write_wfo_consistency_score()`, `flag_candidate_wfo_insufficient()`, `write_mc_result()`, `write_sensitivity_profile()`, `write_verdict()`, `query_verdicts()`, `query_sensitivity_results()`, `query_wfo_window_results()` — confirm all exist
3. `CandidateStore.close()` called in `finally` block of `orchestrator.run()`
4. `datetime.utcnow()` cleanup — schedule single pass migrating all Phase 2/3 occurrences to `datetime.now(UTC)`
## Platform / Environment Notes (for all future sessions)
- **Timezone**: All platform data (OHLCV, strategy signals) is in **CET/CEST**. Internal pipeline timestamps use UTC. Any module computing or displaying wall-clock timestamps visible to the operator should note this distinction.
- **`datetime.utcnow()` deprecation**: Phase 2/3 modules use `datetime.utcnow()`. Python 3.12+ emits `DeprecationWarning`. Phase 4 modules all use `datetime.now(UTC)`. Schedule cleanup of Phase 2/3 modules in Phase 5.
- **Path resolution**: Always use `src/utils/paths.py` for all path construction. Never hardcode separators or roots.
- **ProcessPoolExecutor tests**: Always patch the worker function submitted to the executor, not the functions it calls internally. Module-level patches in the parent process do not propagate to spawned worker processes.
## To take into account: file location and path resolution to be always solved by src\utils\paths.py
### path.py content
```python
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIGS_DIR = PROJECT_ROOT / "configs"
DATA_DIR = PROJECT_ROOT / "data"
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
SRC_DIR = PROJECT_ROOT / "src"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
BACKTEST_OUTPUT_DIR = OUTPUTS_DIR / "backtests"
LOGS_DIR = OUTPUTS_DIR / "logs"
REPORTS_DIR = OUTPUTS_DIR / "reports"
STRATEGIES_OUTPUTS_DIR = OUTPUTS_DIR / "strategies"
STRATEGIES_LOGS_DIR = STRATEGIES_OUTPUTS_DIR / "logs"
STRATEGIES_REPORTS_DIR = STRATEGIES_OUTPUTS_DIR / "reports"
RUNNERS_DIR = SCRIPTS_DIR / "runners"
STRATEGIES_DIR = SRC_DIR / "strategies"
CONTRACTS_DIR = STRATEGIES_DIR / "contracts"
CORE_STRATEGIES_ = STRATEGIES_DIR / "core"
FILTERS_DIR = STRATEGIES_DIR / "filters"
BACKTEST_DIR = SRC_DIR / "backtesting"
UTILS_DIR = SRC_DIR / "utils"
TESTS_DIR = PROJECT_ROOT / "tests"
STRATEGIES_TESTS_DIR = TESTS_DIR / "strategies"
BACKTESTING_TESTS_DIR = TESTS_DIR / "backtesting"
UNIT_TESTS_DIR = STRATEGIES_TESTS_DIR / "unit"
CONTRACT_TEST_DIR = UNIT_TESTS_DIR / "contracts"
FILTERS_TEST_DIR = UNIT_TESTS_DIR / "filters"
RUNNER_TESTS_DIR = STRATEGIES_TESTS_DIR / "runners"
REPORT_TESTS_DIR = STRATEGIES_TESTS_DIR / "reports"
DIAG_TESTS_DIR = STRATEGIES_TESTS_DIR / "diagnostic"
BCST_BENCH_TEST_DIR = BACKTESTING_TESTS_DIR / "benchmarks"
BCST_INEGR_TEST_DIR = BACKTESTING_TESTS_DIR / "integration"
BCST_UNIT_TEST_DIR = BACKTESTING_TESTS_DIR / "unit"
```
<!-- END OF CONTEXT.md -->