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
**Current status (2026-03-04)**: Block 8B in progress. 2 tests fails, 1 test skipped, 11 tests green 
(SKILL to be updated to th current status - to do with next session)
---
## Pipeline (in order — do not reorder)
```
Stage 0: Validation & Init     (min 3 WFO windows; param name validation vs _PARAM_KEY_MAP — M-05, 7B)
Stage 1: Random Search         (LHS/random, significance guard, constraint filter)
Stage 2: MC Pre-Filter         (cheap — 2 perturbation types, ruin screen)
Stage 3: GA                    (WFO-aware: random 2 windows/generation + diversity penalty)
Stage 4: Full WFO              (all windows, 4-metric composite consistency score)
Stage 5: MC Deep               (full iterations, all perturbation types, WFO survivors only)
Stage 6: Parameter Sensitivity (±1/±2 step, fitness delta map, spike = borderline)
Stage 7: Report & Output       (HTML + checklist + JSON/Parquet + SQLite + YAML)
```
All stages fully wired in orchestrator.py. Stages 1–4 are currently stubs pending
their respective phase implementations. Stages 0, 5, 6, 7 fully implemented.
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
## CandidateStore Write API (verified from source 2026-03-04)
```python
# write_candidate takes ONE argument: a CandidateRecord
# Fitness data is EMBEDDED in CandidateRecord fields — not written separately
store.write_candidate(record: CandidateRecord)
store.write_wfo_window_result(result: WFOWindowResult, run_id: str)   # H-02 FIXED 7A
store.flag_candidate_wfo_insufficient(candidate_id: str, run_id: str) # H-02 FIXED 7A
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
# write_wfo_window_result: non-blocking (enqueue). is_ga_fitness_window=0, ga_generation=None always.
# flag_candidate_wfo_insufficient: non-blocking. INSERT OR IGNORE — idempotent.
#   Writes sentinel wfo_consistency_scores row: windows_evaluated=0, window_collapse_flag=1.
```
---
## Audit Finding Dispositions (2026-03-04 — FINAL)
```
H-01: FALSE POSITIVE — confirmed from wfo_evaluator.py source.
      strategy_runner.evaluate() DOES accept date_start/date_end.
      wfo_evaluator.evaluate_window() passes window.start_date/end_date explicitly.
      Audit read simplified TECHNICAL_SPEC signature, not source. No action needed.
H-02: REAL BUG — FIXED in sub-block 7A.
      write_wfo_window_result and flag_candidate_wfo_insufficient were absent from
      candidate_store.py. wfo_engine.py called both — writer thread silently logged
      AttributeError and discarded all wfo_window_results rows.
      Fix: both public methods + internal handlers added. 2 regression tests added.
H-03: FALSE POSITIVE — confirmed from wfo_evaluator.py source (same read as H-01).
      evaluate_window() passes date_start=window.start_date, date_end=window.end_date.
      No action needed.
I-07: FIXED in sub-block 7A.
      wfo_evaluator.py had 3× datetime.utcnow() calls (all in WFOWindowResult construction).
      Replaced with datetime.now(UTC). UTC imported from datetime. No new tests needed.
M-01 to M-07: Accepted. Prioritised for 7B/7D — see sub-block plan below.
E-01 to E-11: Accepted as future roadmap. No v1 action.
WF-07 (parameter_region_width): Uncertain — verdict.py not yet uploaded. Defer to 7D.
WF-09 (post-Stage-1 adequacy warning): Absent (Stage 1 is a stub). Defer to 7D.
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
orchestrator.py       — sequences all 8 stages. Fully wired (Stages 0,5,6,7). 1-4 stubs.
                        Per-stage timing. close() in finally.
parameter_space.py    — expands YAML zones. No strategy knowledge.
sampler.py            — LHS or random selection. No evaluation.
scenario.py           — loads ScenarioProfile from YAML.
strategy_runner.py    — single candidate eval. Accepts date_start/date_end (H-01 confirmed).
                        Never raises. CRITICAL: _PARAM_KEY_MAP maps zone param names → strategy YAML paths.
                        run() kwarg is mode_override="core", NOT mode="core".
fitness.py            — stateless. MetricsReport + ScenarioProfile → FitnessResult.
                        NOTE (M-02): normalisation constants hardcoded — fix in 7B.
candidate_store.py    — SQLite WAL + single-writer queue. Thread-safe.
                        H-02 FIXED: write_wfo_window_result + flag_candidate_wfo_insufficient added.
ranker.py             — stateless. Query spec in → ranked list out.
```
### Phase 3 — Optimization Engines ✓
```
wfo/window_generator.py    — YAML → sorted WFOWindow list. Min 3, no overlaps.
wfo/wfo_evaluator.py       — one candidate, one window → WFOWindowResult. Never raises.
                              I-07 FIXED: datetime.now(UTC) throughout (was utcnow()).
                              H-03 CONFIRMED: passes window.start_date/end_date to strategy_runner.
wfo/wfo_engine.py          — "lightweight" (GA) + "full" (Stage 4) modes.
                              Calls write_wfo_window_result (now fixed) and
                              flag_candidate_wfo_insufficient (now fixed).
wfo/consistency_scorer.py  — WFOWindowResults → 4 metrics → composite [0,1].
                              NOTE (M-03): collapse threshold 0.40 hardcoded — fix in 7B.
ga/population.py           — init from MC_PREFILTER_PASS. Elite extraction.
ga/selection.py            — tournament selection.
ga/crossover.py            — uniform crossover. zone_name from parent_a.
ga/mutation.py             — Gaussian on step grid. Strictly clamped to zone bounds.
                              NOTE (M-06): mutation std dev 2 steps hardcoded — fix in 7D.
ga/diversity.py            — hybrid Euclidean/Hamming distance penalty.
ga/ga_engine.py            — full evolution. rng.sample(windows, k=2) per generation.
monte_carlo/perturbation.py    — named profiles from YAML.
monte_carlo/equity_simulator.py — vectorised np.cumsum. No Python loops over paths.
monte_carlo/mc_metrics.py      — avg_equity, worst_dd, ruin_prob, p5_equity. Vectorised.
                                  NOTE (M-04): zero-equity paths understate drawdown — fix in 7B.
monte_carlo/mc_engine.py       — pre-filter + deep dispatch. Never raises.
```
### Phase 4 — Evaluation Layer ✓
```
evaluation/sensitivity.py — ±1/±2 steps. Parallel via ProcessPoolExecutor.
                            profile_complete=False if >50% failed → BORDERLINE modifier.
                            OPTIMIZATION TARGET: OPT-01 pool reuse (sub-block 7C).
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
OPT-01 target (not achived): Stage 6 ≤ 200s (40% reduction via pool reuse across candidates).
```
## Performance Optimisation (sub-block 7C)
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
| test_h02_wfo_window_writes.py | 2 | 7 Blk 7A | ✅ |
| **Total** | **235** | | **✅ All green** |
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
L-05: Silent write loss from missing store methods. H-02 (write_wfo_window_result,
      flag_candidate_wfo_insufficient absent) meant wfo_engine.py enqueued writes
      that the writer thread dispatched via getattr() — AttributeError was caught,
      logged, and silently discarded. The wfo_window_results table was always empty.
      Lesson: always verify store write API completeness against all call sites
      before trusting DB output.
```
---
## Block 7 Sub-Block Status
```
7A — SKILL.md + H-02 fix + H-03 confirm + I-07 fix   ✅ COMPLETE
     - H-01: FALSE POSITIVE confirmed (wfo_evaluator.py passes window dates)
     - H-02: REAL BUG fixed (2 methods added, 2 tests, 233→235 tests)
     - H-03: FALSE POSITIVE confirmed (same source read)
     - I-07: FIXED (3× datetime.utcnow() → datetime.now(UTC) in wfo_evaluator.py)
     - SKILL.md updated to current state
7B — Audit M P1/P2 — M-05, M-04, M-03, M-02                    ✅COMPLETE
     Files to modify: orchestrator.py, mc_metrics.py, consistency_scorer.py,
                      fitness.py, contracts.py (ScenarioProfile), scenario YAMLs
7C — OPT-01 pool reuse + OPT-02 batching → Stage 6 ≤ 200s      🟡 Stage 6 > 300s
     File to modify: evaluation/sensitivity.py
7D — M-01, M-06, WF-07/WF-09 + full documentation update    ✅ COMPLETE
8A - ✅ COMPLETE
8B - In Progress 🟡 Test lailed
8C - to start yet
...
```
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
- Do not start OPT-01 before 7A verification is complete (7A is now complete 🟡 objective not achieved)
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