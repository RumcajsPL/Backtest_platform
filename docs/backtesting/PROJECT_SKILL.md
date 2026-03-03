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
**Current status (2026-03-03)**: Phase 6 in progress. Blocks 0–3 done. Block 4 next.
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
Stages 1–4 are currently stubs in orchestrator.py. E2E test and performance test seed
the store directly and set checkpoint to WFO_COMPLETE to exercise Stages 5–7.
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
# Timing instrumentation: logger.info only — never print(), never debug flags
```
---
## CandidateStore Write API (critical — verified from source 2026-03-03)
```python
# write_candidate takes ONE argument: a CandidateRecord
# Fitness data is EMBEDDED in CandidateRecord fields — not written separately
store.write_candidate(record: CandidateRecord)     # NOT (candidate, run_id)
# query_mc_results mode is a plain string, NOT MCMode enum
store.query_mc_results(run_id, "deep")             # NOT mode=MCMode.DEEP
# Full write API:
store.write_candidate(record: CandidateRecord)
store.write_wfo_consistency_score(score: WFOConsistencyScore, run_id: str)
store.write_mc_result(result: MCResult, run_id: str)
store.write_sensitivity_profile(profile: SensitivityProfile, run_id: str)
store.write_verdict(verdict: VerdictResult, run_id: str)
store.initialise_run(run_metadata: RunMetadata)
store.set_checkpoint(run_id: str, checkpoint: Checkpoint)
store.flush()
store.close()
# There is NO write_fitness_result() method
```
---
## Test Import Convention (CRITICAL — violating causes circular import at collection)
```python
# 1. sys.path FIRST — before any project imports
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
# 2. path anchor
from src.utils.paths import PROJECT_ROOT
# 3. contracts BEFORE candidate_store
from src.backtesting.contracts import (...)
# 4. candidate_store AFTER contracts
from src.backtesting.candidate_store import CandidateStore
```
---
## Module Map
### Phase 2 — Core Infrastructure ✓
```
orchestrator.py       — sequences stages, checkpoints, resume. Stages 5/6/7 fully wired.
                        Stages 1–4 are stubs pending implementation.
                        Per-stage time.perf_counter() timing added (Block 3).
                        close() in finally guaranteed.
parameter_space.py    — expands YAML zones. No strategy knowledge.
sampler.py            — LHS or random selection. No evaluation.
scenario.py           — loads ScenarioProfile from YAML.
strategy_runner.py    — single candidate eval. Accepts date_start/date_end. Never raises.
                        CRITICAL: _PARAM_KEY_MAP maps zone param names → strategy YAML paths.
                        run() kwarg is mode_override="core", NOT mode="core".
fitness.py            — stateless. MetricsReport + ScenarioProfile → FitnessResult.
candidate_store.py    — SQLite WAL + single-writer queue. Thread-safe.
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
report_generator.py       — self-contained HTML. Inline charts. JSON + Parquet output.
                            BUG NOTE: _collect_report_data() must include "_store": store.
```
---
## Critical Patch Targets
```python
# run_mc is a LOCAL import inside _run_stage_5_mc_deep — NOT on orchestrator namespace
patch("src.backtesting.monte_carlo.mc_engine.run_mc", ...)       # CORRECT
patch("src.backtesting.orchestrator.run_mc", ...)                # WRONG — AttributeError

# ProcessPoolExecutor worker — patch the worker function itself
patch("src.backtesting.evaluation.sensitivity._evaluate_perturbation", ...)  # CORRECT
```
---
## Block 3 Performance Baseline (LOCKED 2026-03-03)
```
Config: mc.deep.iterations=3000, mc.deep.input_count=10,
        sens.input_count=5, sens.max_steps=2, max_workers=6, 20 WFO survivors
Run 2 (canonical): Total=337.2s  Stage5=0.3s  Stage6=332.6s  Stage7=4.4s
Budget: 14,400s  →  2.3% consumed  ✅
Stage 5 (MC Deep):   <3s — fully vectorised, NEVER the bottleneck.
Stage 6 (Sensitivity): ~333–446s — structural bottleneck on Windows spawn mode.
Stage 7 (Report):    4–8s — fine.
PERF-06 ceiling: 99% (Stage 6 structural dominance is expected, not a bug).
```
## Performance Optimisation Opportunities (Block 7)
```
OPT-01 [HIGH]:   Pool reuse in evaluate_sensitivity() — 40–60% Stage 6 reduction
                 File: src/backtesting/evaluation/sensitivity.py
OPT-02 [MEDIUM]: Batch all perturbations per worker task — further 15–25%
                 File: src/backtesting/evaluation/sensitivity.py
OPT-03 [LOW]:    sensitivity.input_count: 5→3, YAML only, saves ~130–180s
OPT-04 [NEGLIGIBLE]: Stage 5 needs no action until input_count > 50
```
---
## Test Counts
| Phase | Tests | Status |
|---|---|---|
| Phase 2–4 | 123 | ✅ Green |
| test_live_pipeline.py (Phase 5) | 17 | ✅ Green |
| test_sqlite_queries.py (Phase 5) | 12 | ✅ Green |
| test_report_yaml.py (Phase 5) | 19 | ✅ Green |
| test_e2e_wbws_real_data.py (Phase 6 Block 0) | 13 | ✅ Green |
| test_adversarial_suite.py (Phase 6 Block 2) | 8 | ✅ Green |
| test_performance.py (Phase 6 Block 3) | 7 | ✅ Green |
| **Total green** | **199** | ✅ |
---
## Adversarial Suite (results locked)
- **AV-02**: overfit candidate → no_go. Two-pillar rejection confirmed.
- **AV-03**: 5/5 positions stable (100%) across seeds [42, 137, 9871]. All: no_go.
---
## Phase 6 Remaining Blocks
```
Block 4 (NEXT): Robustness — resume-after-interruption + worker isolation
                ~11 ROB criteria, test_robustness.py
                Upload: orchestrator.py + evaluation/sensitivity.py
Block 5: Threshold calibration after first real Stages 1–4 run
Block 6: Final documentation
Block 7: OPT-01 + OPT-02 implementation
```
---
## What NOT To Do
- Do not modify `src/strategies/` — strategy architecture is frozen
- Do not use `analytics` mode — `core` mode only (`mode_override="core"`)
- Do not add `print()` — use `logger.info`
- Do not implement ML/AI, eToro API, regime-aware MC, or global sensitivity random-walk (v2+)
- Do not re-open D-01 through D-12
- Do not set `deployment_status = LIVE_APPROVED` in code
- Do not use `datetime.utcnow()` in new code
- Do not use `Candidate` type — use `CandidateParameterSet`
- Do not patch functions called inside ProcessPoolExecutor workers
- Do not use `e2e_test` scenario for production optimization runs
---
## Session Deliverables (end of every session)
- Updated `docs/backtesting/CONTEXT.md`
- Appendix for `docs/backtesting/CHANGE_LOG.md`
- New `docs/backtesting/NEXT_SESSION_PLAN.md`
- Updated `docs/backtesting/PROJECT_SKILL.md` (this file)
---
## Platform Notes
- **OS**: Windows 10, Python 3.13.12
- **Timezone**: OHLCV/signals CET/CEST; pipeline timestamps UTC
- **Paths**: always `src/utils/paths.py`
- **DB**: `data/db/backtest.db` (prod); `tmp_path` in tests