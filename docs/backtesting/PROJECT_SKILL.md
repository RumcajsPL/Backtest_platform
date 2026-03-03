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
**Current status (2026-03-03)**: Phase 6 complete. All stages fully wired. 233 tests green.
Block 7 next: audit remediation + OPT-01/02 pool reuse.
---
## Pipeline (in order — do not reorder)
```
Stage 0: Validation & Init     (min 3 WFO windows; param name validation vs _PARAM_KEY_MAP)
Stage 1: Random Search         (LHS/random, significance guard, constraint filter)
Stage 2: MC Pre-Filter         (cheap — 2 perturbation types, ruin screen)
Stage 3: GA                    (WFO-aware: random 2 windows/generation + diversity penalty)
Stage 4: Full WFO              (all windows, 4-metric composite consistency score)
Stage 5: MC Deep               (full iterations, all perturbation types, WFO survivors only)
Stage 6: Parameter Sensitivity (±1/±2 step, fitness delta map, spike = borderline)
Stage 7: Report & Output       (HTML + checklist + JSON/Parquet + SQLite + YAML)
```
All stages fully wired in orchestrator.py. No stubs remain.
---
## Verdict Model
**Two mandatory pillars**: (1) WFO composite score, (2) MC deep ruin probability.
**Three outcomes**: auto_go | borderline | no_go
**Confirmed boundary operators** (from verdict.py source, Block 5 — DO NOT change):
```python
wfo_pillar_go    = wfo_composite >= go_wfo_floor        # >= INCLUSIVE at go threshold
wfo_pillar_no_go = wfo_composite < borderline_wfo_floor  # < strictly less than
mc_pillar_go    = ruin_prob <= go_mc_ruin_ceiling        # <= INCLUSIVE at go threshold
mc_pillar_no_go = ruin_prob > borderline_mc_ruin_ceiling  # > strictly greater than
ruin_prob = None → mc_pillar_no_go = True → NO_GO       # MC failure = conservative NO_GO
oos_gate_triggered = oos_gate_enabled AND wfo_score.oos_gate_triggered  # BOTH required
```
Modifier flags (any → borderline, cannot override NO_GO):
`sensitivity_spike`, `oos_gate_triggered` (when enforce_oos_gate=True AND wfo triggered),
`window_collapse_flag`, `sensitivity_profile_incomplete` (profile_complete=False).
`deployment_status`: always `PAPER_TRADE_REQUIRED` for go/borderline.
`__post_init__` raises if `LIVE_APPROVED` is set. Operator-only manual promotion.
---
## Scenario System
One active scenario per run. Four defined:
- `capital_accumulation` — grow account, controlled risk (default production scenario)
- `swing_trading` — maximize R:R on directional signals
- `conservative` — preserve capital above all else
- `e2e_test` — **pipeline validation only — NEVER for trading.** Loose constraints calibrated
  to pass real strategy output (13% win rate, negative expectancy). Do not use for optimization.
D-07 starting values for capital_accumulation (recalibrate after first real run):
  go_wfo_floor=0.65, borderline_wfo_floor=0.40, go_mc_ruin_ceiling=0.05, borderline_mc_ruin_ceiling=0.15
Full values: TECHNICAL_SPEC.md §5 and backtest_template.yaml.
---
## Architecture Rules (non-negotiable)
```python
# Contracts: always frozen dataclasses, never raw dicts
# Fail fast: invalid config raises at construction, no silent fallbacks
# Datetime: datetime.now(timezone.utc) — NEVER datetime.utcnow() (deprecated Python 3.12+)
# Paths: pathlib.Path + src/utils/paths.py — never hardcoded separators
# Concurrency: ProcessPoolExecutor spawn mode — never multiprocessing fork
# Candidate ID: always CandidateParameterSet.create() factory — never construct directly
# "Candidate" is NOT a contract — use CandidateParameterSet
# LIVE_APPROVED: never set in code — operator-only manual action
# strategy_runner.run(): mode_override="core" — NOT mode="core"
# Timing: logger.info only — never print(), never debug flags
# store.close(): always in finally block
```
---
## CandidateStore Write API (verified from source 2026-03-03)
```python
# write_candidate takes ONE argument: a CandidateRecord
# Fitness data is EMBEDDED in CandidateRecord fields — not written separately
store.write_candidate(record: CandidateRecord)
store.write_wfo_consistency_score(score: WFOConsistencyScore, run_id: str)
store.write_mc_result(result: MCResult, run_id: str)
store.write_sensitivity_profile(profile: SensitivityProfile, run_id: str)
store.write_verdict(verdict: VerdictResult, run_id: str)
store.initialise_run(run_metadata: RunMetadata)
store.set_checkpoint(run_id: str, checkpoint: Checkpoint)  # orchestrator ONLY
store.flush()
store.close()
# There is NO write_fitness_result() method
# query_mc_results: mode is plain string "deep"/"pre_filter" — NOT MCMode enum
# BLOCK 7 NOTE: verify write_wfo_window_result exists (H-02 audit finding)
#   If missing: add before other Block 7 work
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
orchestrator.py       — sequences all 8 stages. Fully wired. Per-stage timing. close() in finally.
parameter_space.py    — expands YAML zones. No strategy knowledge.
sampler.py            — LHS or random selection. No evaluation.
scenario.py           — loads ScenarioProfile from YAML.
strategy_runner.py    — single candidate eval. Accepts date_start/date_end. Never raises.
                        CRITICAL: _PARAM_KEY_MAP maps zone param names → strategy YAML paths.
                        run() kwarg is mode_override="core", NOT mode="core".
fitness.py            — stateless. MetricsReport + ScenarioProfile → FitnessResult.
                        NOTE (M-02): normalisation constants may be hardcoded — verify Block 7.
candidate_store.py    — SQLite WAL + single-writer queue. Thread-safe.
                        NOTE (H-02): verify write_wfo_window_result exists — Block 7.
ranker.py             — stateless. Query spec in → ranked list out.
```
### Phase 3 — Optimization Engines ✓
```
wfo/window_generator.py    — YAML → sorted WFOWindow list. Min 3, no overlaps.
wfo/wfo_evaluator.py       — one candidate, one window → WFOWindowResult. Never raises.
                              Passes window.start_date/end_date to strategy_runner. (H-03 confirm)
wfo/wfo_engine.py          — "lightweight" (GA) + "full" (Stage 4) modes.
wfo/consistency_scorer.py  — WFOWindowResults → 4 metrics → composite [0,1].
                              NOTE (M-03): collapse threshold 0.40 may be hardcoded — verify Block 7.
ga/population.py           — init from MC_PREFILTER_PASS. Elite extraction.
ga/selection.py            — tournament selection.
ga/crossover.py            — uniform crossover. zone_name from parent_a.
ga/mutation.py             — Gaussian on step grid. Strictly clamped to zone bounds.
                              NOTE (M-06): mutation std dev 2 steps may be hardcoded — verify Block 7.
ga/diversity.py            — hybrid Euclidean/Hamming distance penalty.
ga/ga_engine.py            — full evolution. rng.sample(windows, k=2) per generation.
monte_carlo/perturbation.py    — named profiles from YAML.
monte_carlo/equity_simulator.py — vectorised np.cumsum. No Python loops over paths.
monte_carlo/mc_metrics.py      — avg_equity, worst_dd, ruin_prob, p5_equity. Vectorised.
                                  NOTE (M-04): zero-equity paths may understate drawdown — fix Block 7.
monte_carlo/mc_engine.py       — pre-filter + deep dispatch. Never raises.
```
### Phase 4 — Evaluation Layer ✓
```
evaluation/sensitivity.py — ±1/±2 steps. Parallel via ProcessPoolExecutor.
                            profile_complete=False if >50% failed → BORDERLINE modifier.
                            OPTIMIZATION TARGET: OPT-01 pool reuse (Block 7 sub-block 7C).
evaluation/verdict.py     — two-pillar + modifier flags. Never sets LIVE_APPROVED.
yaml_generator.py         — merges params into base YAML. Embeds backtester_metadata.
report_generator.py       — self-contained HTML. Inline charts. JSON + Parquet output.
```
---
## Critical Patch Targets
```python
# Stage 6 integration tests: patch at orchestrator level, NOT at worker function
patch("src.backtesting.orchestrator.evaluate_sensitivity", ...)   # CORRECT for integration tests
# DO NOT: patch("src.backtesting.evaluation.sensitivity._evaluate_perturbation", ...)
#   Reason: spawn mode — mock does not cross process boundary. Causes pickle error (ROB-09).

# Stage 5: run_mc is a LOCAL import inside _run_stage_5_mc_deep
patch("src.backtesting.monte_carlo.mc_engine.run_mc", ...)       # CORRECT
# DO NOT: patch("src.backtesting.orchestrator.run_mc", ...)       # AttributeError
```
---
## Block 3 Performance Baseline (LOCKED 2026-03-03)
```
Config: mc.deep.iterations=3000, mc.deep.input_count=10,
        sens.input_count=5, sens.max_steps=2, max_workers=6, 20 WFO survivors
Run 2 (canonical): Total=337.2s  Stage5=0.3s  Stage6=332.6s  Stage7=4.4s
Budget: 14,400s  →  2.3% consumed  ✅
Stage 5 (MC Deep):    <3s — fully vectorised, NEVER the bottleneck.
Stage 6 (Sensitivity): ~333–446s — structural bottleneck on Windows spawn mode.
Stage 7 (Report):     4–8s — fine.
PERF-06 ceiling: 99% (Stage 6 structural dominance is expected, not a bug).
OPT-01 target: Stage 6 ≤ 200s (40% reduction via pool reuse across candidates).
```
## Performance Optimisation (Block 7 sub-block 7C)
```
OPT-01 [HIGH]:   Pool reuse in evaluate_sensitivity() — 40–60% Stage 6 reduction
                 Change: one ProcessPoolExecutor wrapping ALL candidates, not one per candidate
                 File: src/backtesting/evaluation/sensitivity.py
OPT-02 [MEDIUM]: Batch all perturbations per candidate into one worker task — further 15–25%
                 File: src/backtesting/evaluation/sensitivity.py
OPT-03 [LOW]:    sensitivity.input_count 5→3, YAML only, saves ~130–180s
OPT-04 [NEGLIGIBLE]: Stage 5 — no action until input_count > 50
OPT-05 [CLEANUP]: max_workers param cleanup after OPT-01
```
---
## Test Counts
| File | Count | Phase | Status |
|---|---|---|---|
| unit/ (Phases 2–4) | 123 | 2–4 | ✅ |
| test_live_pipeline.py | 17 | 5 | ✅ |
| test_sqlite_queries.py | 12 | 5 | ✅ |
| test_report_yaml.py | 19 | 5 | ✅ |
| test_e2e_wbws_real_data.py | 13 | 6 Blk 0 | ✅ |
| test_adversarial_suite.py | 8 | 6 Blk 2 | ✅ |
| test_performance.py | 7 | 6 Blk 3 | ✅ |
| test_robustness.py | 12 | 6 Blk 4 | ✅ |
| test_threshold_calibration.py | 22 | 6 Blk 5 | ✅ |
| **Total** | **233** | | **✅ All green** |
---
## Adversarial Suite (results locked)
- **AV-01**: Random signal baseline → no_go. Confirmed Phase 4.
- **AV-02**: overfit candidate → no_go. Two-pillar rejection confirmed. Block 2.
- **AV-03**: 5/5 positions stable (100%) across seeds [42, 137, 9871]. All: no_go. Block 2.
---
## Lessons Learned (Phase 6 — locked)
```
L-01: Windows spawn mode — mock patches don't cross worker boundary.
      For Stage 6 integration tests: patch at orchestrator level.
      Failure mode: "Can't pickle <class 'unittest.mock.MagicMock'>" (ROB-09).

L-02: Verdict boundary operators must be >= / <= (inclusive) at go thresholds.
      Using > / < incorrectly classifies boundary-exact scores as BORDERLINE.
      Confirmed from verdict.py source, Block 5.

L-03: Stage 6 is the dominant runtime (333–446s = 98.7% of total).
      Root cause: Windows spawn mode creates a fresh pool per candidate.
      OPT-01 (pool reuse) is the highest-value optimisation available.

L-04: Config fixture shape for tests must match load_scenario() nested structure.
      Correct: config["scenarios"][name]["fitness_weights"][...].
      Wrong (KeyError): config["fitness_weights"][...] at top level.
```
---
## Block 7 Plan (Next)
```
First: upload source files (candidate_store.py, wfo_evaluator.py, sensitivity.py,
       test_performance.py, fitness.py, consistency_scorer.py, mc_metrics.py, orchestrator.py)

Sub-Block 7A: SKILL.md update + H-02/H-03 verification + datetime.utcnow() fix
Sub-Block 7B: Audit M P1/P2 — M-05 (Stage 0 param validation), M-04 (zero-equity DD),
              M-03 (collapse threshold), M-02 (fitness normalisation)
Sub-Block 7C: OPT-01 pool reuse + OPT-02 batching → Stage 6 ≤ 200s
Sub-Block 7D: M P3+ (M-01 median_oos_delta, M-06 mutation std) + WF-07/WF-09 + docs
```
---
## Independent Audit Summary (2026-03-03)
H-01: FALSE POSITIVE — strategy_runner already accepts date_start/date_end.
H-02: UNRESOLVED — write_wfo_window_result absent from SKILL.md API list; verify source in 7A.
H-03: LIKELY FALSE POSITIVE — contingent on H-01; confirm wfo_evaluator passes window dates.
M-01 to M-07: All accepted. None affect correctness. Prioritised for Block 7 7B/7D.
E-01 to E-11: All accepted as future roadmap. No v1 action.
I-07 (datetime.utcnow()): Promoted to 7A action item.
Parameter mapping §6: All 34 confirmed correct. 3 unmapped are v2 scope.
---
## What NOT To Do
- Do not modify `src/strategies/` — strategy architecture is frozen
- Do not use `analytics` mode — `core` mode only (`mode_override="core"`)
- Do not add `print()` — use `logger.info`
- Do not implement ML/AI, eToro API, regime-aware MC, global sensitivity random-walk (v2+)
- Do not re-open D-01 through D-12 (all resolved)
- Do not set `deployment_status = LIVE_APPROVED` in code
- Do not use `datetime.utcnow()` in any new or modified code
- Do not use `Candidate` type — use `CandidateParameterSet`
- Do not patch functions called inside ProcessPoolExecutor workers (spawn boundary)
- Do not use `e2e_test` scenario for production optimization runs
- Do not start OPT-01 before 7A verification is complete
---
## Platform Notes
- **OS**: Windows 10, Python 3.13.12
- **Timezone**: OHLCV/signals CET/CEST; pipeline timestamps UTC
- **Paths**: always `src/utils/paths.py`
- **DB**: `data/db/backtest.db` (prod); `tmp_path` in tests
---
## Session Deliverables (end of every session)
- Updated `docs/backtesting/CONTEXT.md`
- Appendix entry for `docs/backtesting/CHANGE_LOG.md`
- New `docs/backtesting/NEXT_SESSION_PLAN.md`
- Updated `docs/backtesting/PROJECT_SKILL.md` (this file)