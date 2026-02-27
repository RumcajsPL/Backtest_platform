# PROJECT CONTEXT — Backtesting & Optimization Framework
<!-- PASTE THIS ENTIRE FILE AS YOUR FIRST MESSAGE IN EVERY NEW CHAT SESSION -->
<!-- After pasting, describe what you need in the same message. -->
<!-- Then ask Claude to confirm it has read and understood before proceeding. -->
## Identity
**Project**: Backtesting & Optimization Framework for WBWSStrategy
**Operator**: Single quantitative retail trader, Windows 10, eToro broker
**Stage**: Phase 1 — Design | Session 2
**Last session ended**: 2026-02-27 — Produced FUNCTIONAL_SPEC.md, TECHNICAL_SPEC.md, SQLITE_SCHEMA.md. Resolved all 12 open decisions. Defined all 11 contracts. Designed full SQLite schema. Defined 3 scenario profiles with concrete values. Specified backtest_template.yaml schema.
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
9. **Immutable run artifacts** — config hash, all seeds, perturbation profile name written at run start. Post-run config changes create a new run — never overwrite.
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
| `ARCHITECTURE.md` | Strategy architecture (fixed input, do not modify) | `docs/architecture/` |
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
PHASE:        Phase 1 — Design
COMPLETED:    - Independent opinion reviewed and decisions made (all accepted/rejected/modified)
              - BACKTESTER_PLAN.md updated to v1.2 (all accepted changes incorporated)
              - All 12 open decisions resolved (D-01 through D-12)
              - All 11 inter-module contracts defined as frozen dataclasses (TECHNICAL_SPEC.md)
              - SQLite schema: all 9 tables with CREATE TABLE statements and indexes (SQLITE_SCHEMA.md)
              - backtest_template.yaml full schema specified (TECHNICAL_SPEC.md Section 5)
              - All 3 scenario profiles defined with concrete values (TECHNICAL_SPEC.md Section 5)
              - FUNCTIONAL_SPEC.md: all 8 stages in plain language
IN PROGRESS:  - CHANGE_LOG.md SESSION 2 block (to be written at end of session)
              - PROJECT_REPORT.md update
              - CONTEXT.md update (this file)
BLOCKED ON:   Nothing — all decisions resolved
NEXT TASK:    Phase 2 — Core Infrastructure
              Start with: candidate_store.py (CandidateStore implementation)
              Then: parameter_space.py, sampler.py, scenario.py, strategy_runner.py, fitness.py
```
---
## Open Decisions — ALL RESOLVED
~~D-01~~: **RESOLVED** — Direct Python call (import StrategyOrchestrator in worker process). Benchmark required in Phase 2.
~~D-02~~: **RESOLVED** — SQLite WAL mode + single-writer queue (workers submit to queue; one writer thread drains). Benchmark required in Phase 2.
~~D-03~~: **RESOLVED** — Per-candidate temp YAML named by parameter hash. Deleted in `finally`. Optional `retain_temp_yamls: true` for debugging.
~~D-04~~: **RESOLVED** — Top-N by fitness from MC_PREFILTER_PASS. Diversity handled by GA diversity penalty during evolution.
~~D-05~~: **RESOLVED** — Randomly sample 2 windows per GA generation from full window list. Min 3 windows required.
~~D-06~~: **RESOLVED** — Default counts: 200/zone Random, top 120 MC Pre-filter, pop 60 GA, 30 gen, top 30 Full WFO, top 10 MC Deep, top 5 Sensitivity.
~~D-07~~: **RESOLVED** — Starting thresholds: WFO go ≥0.65, borderline 0.40–0.65, no_go <0.40; MC go ≤5%, borderline 5–15%, no_go >15%. Scenario-specific values in TECHNICAL_SPEC.md.
~~D-08~~: **RESOLVED** — All optimizable parameters. 5 candidates × ~15 params × 4 steps ≈ 300 evaluations, ~200s at 6 workers.
~~D-09~~: **RESOLVED** — Both JSON and Parquet, both enabled by default. Configurable via `output.formats`.
~~D-10~~: **RESOLVED** — Build new `report_generator.py`. Structurally too different from existing single-run generator to extend.
~~D-11~~: **RESOLVED** — Hybrid: normalised Euclidean for continuous params, Hamming for discrete params, weighted average.
~~D-12~~: **RESOLVED** — `enforce_oos_gate: false` by default. When enabled: >50% IS/OOS degradation = borderline flag (never auto-reject).
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
- [x] `VerdictResult` — candidate_id, verdict, deployment_status (PAPER_TRADE_REQUIRED), pillar scores, flags, evidence_summary, yaml_output_path
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
## What NOT To Do
- Do not modify `ARCHITECTURE.md` or any file under `src/strategies/` — strategy architecture is frozen
- Do not invent new open decisions without logging them in `CHANGE_LOG.md`
- Do not use `analytics` mode inside the backtester loop — `core` mode only
- Do not build the ML/AI analytics layer — schema design only in v1
- Do not implement eToro API integration — future project, not this one
- Do not implement regime-aware MC perturbation profiles — v2 scope
- Do not implement true global parameter sensitivity random-walk — v2 scope
---
## Phase 2 Starting Point
When Phase 2 begins, the first implementation task is `candidate_store.py`. Read TECHNICAL_SPEC.md
and SQLITE_SCHEMA.md before writing any code. The store is the foundation — everything else depends on it.
Key Phase 2 benchmarks (must complete before full implementation):
1. Strategy integration benchmark: 50 candidates in direct-call mode — confirm fits Stage 1 time budget
2. SQLite WAL + writer queue benchmark: 500 concurrent writes from 6 workers — confirm no corruption
<!-- END OF CONTEXT.md -->