---
name: backtester-project
description: >
  Use this skill whenever working on the Backtesting & Optimization Framework project.
  Triggers: any mention of backtester, backtest pipeline, CandidateStore, GA engine,
  WFO evaluator, Monte Carlo engine, fitness evaluator, scenario profile, backtest_template.yaml,
  sensitivity evaluator, verdict engine, report generator, or any module from src/backtesting/.
  Read this SKILL.md before writing any code, creating any file, or making any design
  decision for this project.
---
# Backtesting Framework — Project Skill
## What This Project Is
A fully automated 8-stage optimization pipeline for the WBWSStrategy trading strategy.
Produces go/borderline/no-go verdicts based on two mandatory evidence pillars.

**Current status (2026-03-01)**: Phase 5 closed. 155 tests green. 
---
## Pipeline (in order — do not reorder)
```
Stage 0: Validation & Init     (min 3 WFO windows — validated here for GA random sampling)
Stage 1: Random Search         (LHS, significance guard, constraint filter, single-run fitness)
Stage 2: MC Pre-Filter         (cheap — 2 perturbation types, ruin screen)
Stage 3: GA                    (WFO-aware: random 2 windows/generation + diversity penalty)
Stage 4: Full WFO              (all windows, 4-metric composite consistency score)
Stage 5: MC Deep               (full iterations, all perturbation types, WFO survivors only)
Stage 6: Parameter Sensitivity (±1/±2 step, fitness delta map, spike = borderline)
Stage 7: Report & Output       (HTML + checklist + JSON/Parquet + SQLite + YAML)
```
---
## Verdict Model
**Two mandatory pillars**: (1) WFO composite score, (2) MC deep ruin probability.
**Three outcomes**: auto_go | borderline | no_go
Exact logic:
- `AUTO_GO`: both pillars pass go thresholds AND no modifier flags
- `BORDERLINE`: either pillar in borderline zone OR any modifier flag
- `NO_GO`: either pillar in no_go zone — modifier flags cannot override

Modifier flags (any → borderline): `sensitivity_spike`, `oos_gate_triggered` (only when `enforce_oos_gate: true` AND WFO triggered), `window_collapse_flag`, `sensitivity_profile_incomplete`.

`deployment_status`: always `PAPER_TRADE_REQUIRED` for go/borderline. `__post_init__` raises if `LIVE_APPROVED` set. Operator-only promotion after paper trading.

---
## Scenario System
One active scenario per run (`capital_accumulation` | `swing_trading` | `conservative` | custom).
Controls: fitness weights, constraint thresholds, WFO temporal weights, verdict thresholds, report framing.
Full values: `TECHNICAL_SPEC.md` Section 5.

---
## Architecture Rules (non-negotiable)
```python
# Contracts: always frozen dataclasses, never raw dicts
# Fail fast: invalid config raises at construction, no silent fallbacks
# Datetime: datetime.now(UTC) — never datetime.utcnow() (deprecated Python 3.12+)
# Paths: pathlib.Path + src/utils/paths.py — never hardcoded separators
# Concurrency: ProcessPoolExecutor spawn mode — never multiprocessing fork
# Candidate ID: always CandidateParameterSet.create() factory — never construct directly
# "Candidate" is NOT a contract — use CandidateParameterSet
# LIVE_APPROVED: never set in code — operator-only manual action
```
---
## Module Map
### Phase 2 — Core Infrastructure ✓
```
orchestrator.py       — sequences stages, checkpoints, resume. Stages 5/6/7 fully wired.
                        close() in finally guaranteed.
parameter_space.py    — expands YAML zones. No strategy knowledge.
sampler.py            — LHS or random selection. No evaluation.
scenario.py           — loads ScenarioProfile from YAML.
strategy_runner.py    — single candidate eval. Accepts date_start/date_end. Never raises.
fitness.py            — stateless. MetricsReport + ScenarioProfile → FitnessResult.
candidate_store.py    — SQLite WAL + single-writer queue. Thread-safe.
                        Write: write_candidate(), write_wfo_window_result(),
                               write_wfo_consistency_score(), flag_candidate_wfo_insufficient(),
                               write_mc_result(), write_sensitivity_profile(), write_verdict().
                        Read:  get_checkpoint(), set_checkpoint(), get_wfo_consistency_score(),
                               get_mc_result(), get_sensitivity_profile(), get_candidate_result(),
                               get_fitness_score(), rank_by_wfo(), query_candidates(),
                               query_verdicts(), query_wfo_consistency_scores(),
                               query_mc_results(), query_sensitivity_profiles(),
                               query_wfo_window_results(), query_sensitivity_results(), close().
ranker.py             — stateless. Query spec in → ranked list out.
```
### Phase 3 — Optimization Engines ✓
```
wfo/window_generator.py    — YAML → sorted WFOWindow list. Min 3, no overlaps.
wfo/wfo_evaluator.py       — one candidate, one window → WFOWindowResult. Never raises.
wfo/wfo_engine.py          — "lightweight" (GA) + "full" (Stage 4) modes.
wfo/consistency_scorer.py  — WFOWindowResults → 4 metrics → composite [0,1].
ga/population.py           — init from MC_PREFILTER_PASS. Elite extraction.
ga/selection.py            — tournament selection.
ga/crossover.py            — uniform crossover. zone_name from parent_a.
ga/mutation.py             — Gaussian on step grid. Strictly clamped to zone bounds.
ga/diversity.py            — hybrid Euclidean/Hamming distance penalty.
ga/ga_engine.py            — full evolution. rng.sample(windows, k=2) per generation.
monte_carlo/perturbation.py    — named profiles from YAML.
monte_carlo/equity_simulator.py — vectorised np.cumsum. No Python loops over paths.
monte_carlo/mc_metrics.py      — avg_equity, worst_dd, ruin_prob, p5_equity. Vectorised.
monte_carlo/mc_engine.py       — pre-filter + deep dispatch. Never raises.
```
### Phase 4 — Evaluation Layer ✓
```
evaluation/sensitivity.py — ±1/±2 steps. Parallel via ProcessPoolExecutor.
                            Patch _evaluate_perturbation (worker), not what it calls.
                            profile_complete=False if >50% failed.
evaluation/verdict.py     — two-pillar + modifier flags. Never sets LIVE_APPROVED.
yaml_generator.py         — merges params into base YAML. Embeds backtester_metadata.
                            build_output_path(): {out}/trading_yamls/{run[:8]}_{cid[:12]}_strategy.yaml
report_generator.py       — self-contained HTML (no Jinja2). Inline charts (matplotlib Agg → base64).
                            Scenario-framed. Adversarial checklist per borderline candidate.
                            JSON per candidate → json/. Parquet → parquet/ (pandas).
                            BUG: _collect_report_data() must include "_store": store in return dict
                            for chart functions. Fix: add "_store": store before closing brace.
```

### Phase 5 — Wiring ✓
```
orchestrator.py  — all 8 stages wired. 
```
---
## Critical Patch Targets
```python
# run_mc is a LOCAL import inside _run_stage_5_mc_deep — NOT on orchestrator namespace
# CORRECT:
patch("src.backtesting.monte_carlo.mc_engine.run_mc", ...)
# WRONG — AttributeError:
patch("src.backtesting.orchestrator.run_mc", ...)

# ProcessPoolExecutor worker — patch the worker itself
# CORRECT:
patch("src.backtesting.evaluation.sensitivity._evaluate_perturbation", ...)
# WRONG — parent-process patch doesn't cross spawn boundary:
patch("src.backtesting.evaluation.sensitivity.runner_evaluate", ...)
```
---
## SQLite Schema — 9 Tables
```
runs                   — immutable: config_hash, 5 seeds, perturbation_profile_name, checkpoint
candidates             — one row per unique candidate_id
candidate_parameters   — individual columns per parameter + parameters_json backup
evaluations            — one row per candidate per stage, all constraint actuals + fitness
wfo_window_results     — one row per candidate per window (is_ga_fitness_window flag)
wfo_consistency_scores — 4 sub-metrics + composite score per candidate
mc_results             — pre_filter and deep as separate rows per candidate
sensitivity_results    — one row per candidate per parameter per step
sensitivity_profiles   — summary: spike_detected, spike_parameters, profile_complete
verdicts               — final verdict + evidence + deployment_status per candidate
```
---
## Test Counts
| Phase | Tests | Status |
|---|---|---|
| Phase 2–4 | 123 | ✅ Green |
| test_live_pipeline.py (Phase 5) | 17 | ✅ Green |
| test_sqlite_queries.py (Phase 5) | 12 | ✅ Green |
| test_report_yaml.py (Phase 5) | 19 | ✅ Green |
| **Total green** | **155** | ✅ Green|

---
## Adversarial Suite
- **AV-01** ✅ PASSED (Phase 4): 0 AUTO_GO on 100 random-signal candidates.
- **AV-02** ⬜ Phase 6: overfit-injection → must fail at WFO.
- **AV-03** ⬜ Phase 6: >80% verdict stability under seed perturbation.
- **AV-04** ✅ Implemented: adversarial checklist HTML per borderline candidate.
---
## What NOT To Do
- Do not modify `src/strategies/` — strategy architecture is frozen
- Do not use `analytics` mode — `core` mode only
- Do not add `print()` statements — use structured_logger
- Do not implement ML/AI layer, eToro API, regime-aware MC, or global sensitivity random-walk (all v2+)
- Do not re-open D-01 through D-12
- Do not set `deployment_status = LIVE_APPROVED` in code
- Do not use `datetime.utcnow()` in new code
- Do not use `Candidate` type — use `CandidateParameterSet`
- Do not patch functions called inside ProcessPoolExecutor workers
---
## Platform Notes
- **OS**: Windows 10, Python 3.13.12
- **Timezone**: OHLCV/signals CET/CEST; pipeline timestamps UTC
- **Paths**: always `src/utils/paths.py`
- **DB**: `data/db/backtest.db` (prod); `tmp_path` in tests