# PROJECT CONTEXT — Backtesting & Optimization Framework
<!-- PASTE THIS ENTIRE FILE AS YOUR FIRST MESSAGE IN EVERY NEW CHAT SESSION -->
<!-- After pasting, describe what you need in the same message. -->
<!-- Then ask Claude to confirm it has read and understood before proceeding. -->
## Identity
**Project**: Backtesting & Optimization Framework for WBWSStrategy
**Operator**: Single quantitative retail trader, Windows 10, eToro broker
**Stage**: Phase 4 — Evaluation Layer | Session 1
**Last session ended**: 2026-03-01 — Completed Phase 3: All 14 optimization engine modules implemented, 53 tests pass (3 unit + 1 integration).
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
PHASE:        Phase 4 — Evaluation Layer
COMPLETED:    - Phase 3: All 14 optimization engine modules implemented (wfo/*, ga/*, monte_carlo/*)
                53 tests pass. Key validations: GA window sampling independence, diversity penalty
                effectiveness, MC vectorised equity simulation, consistency scorer correctness.
              - Phase 2: Both benchmarks passed, all 8 core modules implemented, contracts defined,
                all unit tests passed, integration test passed.
              - Phase 1: All specs produced, decisions resolved, contracts defined, schema designed.
   IN PROGRESS:  - CHANGE_LOG.md SESSION 4 block (written — append to file)
                 - PROJECT_REPORT.md update (operator handles)
                 - CONTEXT.md (this file — updated)
   BLOCKED ON:   strategy_runner.py date windowing — see Known Issues below
   NEXT TASK:    Phase 4 — Evaluation Layer
   Start with:   evaluation/sensitivity.py
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
## Modules Implemented (Phase 2 + Phase 3)
```
Phase 2 (core infrastructure):
  src/backtesting/candidate_store.py       ✓
  src/backtesting/parameter_space.py       ✓
  src/backtesting/sampler.py               ✓
  src/backtesting/scenario.py              ✓
  src/backtesting/strategy_runner.py       ✓
  src/backtesting/fitness.py               ✓
  src/backtesting/ranker.py                ✓
  src/backtesting/orchestrator.py          ✓ (skeleton — Stages 5/6/7 stubs remain)

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

Phase 4 (to build):
  src/backtesting/evaluation/sensitivity.py   ← START HERE
  src/backtesting/evaluation/verdict.py
  src/backtesting/yaml_generator.py
  src/backtesting/report_generator.py
  src/backtesting/orchestrator.py             (final wiring of Stages 5/6/7)
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

---
## Phase 4 Starting Point
When Phase 4 begins, the first implementation task is `evaluation/sensitivity.py`.
Read TECHNICAL_SPEC.md (contracts) and FUNCTIONAL_SPEC.md (Stage 6 and Stage 7 details) and SQLITE_SCHEMA.md (sensitivity + verdict tables) before writing any code.

**Known integration bridge required before live testing:**
- `strategy_runner.py` must accept `date_start` and `date_end` keyword arguments to scope evaluation to a WFO window date range. `wfo_evaluator.evaluate_window()` already calls this interface. Confirm or implement in Phase 4 orchestrator wiring block.
- `candidate_store.py` must expose: `write_wfo_window_result()`, `write_wfo_consistency_score()`, `flag_candidate_wfo_insufficient()`. Confirm these exist or add them.

## Platform / Environment Notes (for all future sessions)
- **Timezone**: All platform data (OHLCV, strategy signals) is in **CET/CEST**. Internal pipeline timestamps use UTC. Any module computing or displaying wall-clock timestamps visible to the operator should note this distinction.
- **`datetime.utcnow()` deprecation**: All Phase 3 modules use `datetime.utcnow()`. Python 3.12+ emits `DeprecationWarning`. Do not fix piecemeal — schedule a single cleanup pass migrating all occurrences to `datetime.now(datetime.UTC)` when convenient (not blocking).
- **Path resolution**: Always use `src/utils/paths.py` for all path construction. Never hardcode separators or roots.

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