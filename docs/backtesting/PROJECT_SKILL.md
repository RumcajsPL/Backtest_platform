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
A fully automated 8-stage optimization pipeline for the WBWSStrategy. Given a parameter
space definition and a strategy base config, it searches for robust parameter combinations
and produces a verdict (auto_go / borderline / no_go) per candidate.

**Current status (2026-03-02)**: Phase 6 in progress. 155 + 7 tests (7 pass, 6 skip on E2E).
E2E test pipeline runs clean but WFO survivors = 0 — constraint rejection bug under investigation.

---
## Pipeline (in order — do not reorder)
```
Stage 0: Validation & Init     (min 3 WFO windows — validated here for GA random sampling)
Stage 1: Random Search         (LHS/random, significance guard, constraint filter)
Stage 2: MC Pre-Filter         (cheap — 2 perturbation types, ruin screen)
Stage 3: GA                    (WFO-aware: random 2 windows/generation + diversity penalty)
Stage 4: Full WFO              (all windows, 4-metric composite consistency score)
Stage 5: MC Deep               (full iterations, all perturbation types, WFO survivors only)
Stage 6: Parameter Sensitivity (±1/±2 step, fitness delta map, spike = borderline)
Stage 7: Report & Output       (HTML + checklist + JSON/Parquet + SQLite + YAML)
```
Stages 1–4 are currently stubs in orchestrator.py. E2E test seeds store directly and
sets checkpoint to WFO_COMPLETE to exercise Stages 5–7 on real data.

---
## Verdict Model
**Two mandatory pillars**: (1) WFO composite score, (2) MC deep ruin probability.
**Three outcomes**: auto_go | borderline | no_go
- `AUTO_GO`: both pillars pass go thresholds AND no modifier flags
- `BORDERLINE`: either pillar in borderline zone OR any modifier flag
- `NO_GO`: either pillar in no_go zone — modifier flags cannot override

Modifier flags (any → borderline): `sensitivity_spike`, `oos_gate_triggered`
(only when `enforce_oos_gate: true` AND WFO triggered), `window_collapse_flag`,
`sensitivity_profile_incomplete`.

`deployment_status`: always `PAPER_TRADE_REQUIRED` for go/borderline.
`__post_init__` raises if `LIVE_APPROVED` set. Operator-only promotion after paper trading.

---
## Scenario System
One active scenario per run. Four defined:
- `capital_accumulation` — grow account, controlled risk
- `swing_trading` — maximize R:R on directional signals
- `conservative` — preserve capital above all else
- `e2e_test` — **pipeline validation only**, NOT for trading. Loose constraints
  calibrated to pass real strategy output (13% win rate, negative expectancy).
  DO NOT use for production optimization runs.

Full values: `TECHNICAL_SPEC.md` Section 5 and `backtest_template.yaml`.

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
# strategy_runner.run(): mode_override="core" — NOT mode="core"
```

---
## Module Map
### Phase 2 — Core Infrastructure ✓
```
orchestrator.py       — sequences stages, checkpoints, resume. Stages 5/6/7 fully wired.
                        Stages 1–4 are stubs pending implementation.
                        close() in finally guaranteed.
parameter_space.py    — expands YAML zones. No strategy knowledge.
sampler.py            — LHS or random selection. No evaluation.
scenario.py           — loads ScenarioProfile from YAML.
strategy_runner.py    — single candidate eval. Accepts date_start/date_end. Never raises.
                        CRITICAL: _PARAM_KEY_MAP maps zone param names → strategy YAML paths.
                        run() kwarg is mode_override="core", NOT mode="core".
fitness.py            — stateless. MetricsReport + ScenarioProfile → FitnessResult.
candidate_store.py    — SQLite WAL + single-writer queue. Thread-safe.
                        Write: write_candidate(), write_wfo_consistency_score(),
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
report_generator.py       — self-contained HTML. Inline charts (matplotlib Agg → base64).
                            Scenario-framed. Adversarial checklist per borderline candidate.
                            JSON per candidate → json/. Parquet → parquet/ (pandas).
                            BUG NOTE: _collect_report_data() must include "_store": store.
```
### Phase 5 — Wiring ✓
```
orchestrator.py  — all 8 stages wired. Stages 1–4 are stubs.
```

---
## strategy_runner._PARAM_KEY_MAP — Current State (verified 2026-03-02)
```python
_PARAM_KEY_MAP = {
    "rsi_period":           "filters.technical_filters.rsi_filter.length",
    "rsi_overbought":       "filters.technical_filters.rsi_filter.overbought",
    "rsi_oversold":         "filters.technical_filters.rsi_filter.oversold",
    "adx_threshold":        "filters.technical_filters.adx_filter.threshold",
    "atr_length":           "trade_management.risk.atr_length",
    "atr_multiplier":       "trade_management.risk.atr_multiplier_sl",
    "rr_target":            "trade_management.risk.risk_to_reward_ratio",
    "risk_percentile":      "trade_management.risk.max_risk_percentile",
    "bollinger_length":     "filters.technical_filters.bollinger_filter.length",
    "bollinger_multiplier": "filters.technical_filters.bollinger_filter.filter_multiplier",
    "strategy_tf":          "data.strategy_timeframe",   # path unverified
    "htf_tf":               "data.htf_timeframe",        # path unverified
    "session_filter":       "filters.time.session",      # path unverified
}
# Remaining 7 filters NOT yet mapped (Block 1 task):
# choppiness, supertrend, cci, macd, ma, pivot, dpo
# Filter enabled flags NOT mapped (Block 1 decision needed)
# Filter sequence order NOT mapped (Block 1 decision needed)
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
| test_e2e_wbws_real_data.py (Phase 6) | 13 | ⚠️ 7 pass / 6 skip |
| **Total** | **184** | ⚠️ 162 green, 6 skipped |

---
## Adversarial Suite
- **AV-01** ✅ PASSED (Phase 4): 0 AUTO_GO on 100 random-signal candidates.
- **AV-02** ⬜ Phase 6 Block 2: overfit-injection → must fail at WFO.
- **AV-03** ⬜ Phase 6 Block 2: >80% verdict stability under seed perturbation.
- **AV-04** ✅ Implemented: adversarial checklist HTML per borderline candidate.

---
## Open Issues (Phase 6)
1. **E2E WFO survivors = 0** — strategy evaluates without error but candidates fail
   fitness constraints. Likely: fitness.py metric extraction mismatch or drawdown
   unit mismatch (points vs fraction). Fix in next session.
2. **_PARAM_KEY_MAP incomplete** — 7 filters + filter sequence + enabled flags not mapped.
   Full audit in Block 1.
3. **strategy_tf / htf_tf / session_filter paths** — in map but not verified against
   actual strategy_template.yaml structure.

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
- Do not use `mode="core"` — use `mode_override="core"`
- Do not use `e2e_test` scenario for production optimization runs

---
## Platform Notes
- **OS**: Windows 10, Python 3.13.12
- **Timezone**: OHLCV/signals CET/CEST; pipeline timestamps UTC
- **Paths**: always `src/utils/paths.py`
- **DB**: `data/db/backtest.db` (prod); `tmp_path` in tests