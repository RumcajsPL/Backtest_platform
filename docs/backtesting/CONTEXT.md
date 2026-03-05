# CONTEXT.md — Block 9D Handoff
**Generated**: 2026-03-05 (end of Block 9C session)
**From**: Block 9C — Supporting Modules Audit (complete)
**To**: Block 9D — P3 Prerequisite Fixes + Stage 1–3 Implementation
---
## Current State
### Test Suite
```
pytest tests/backtesting/ → 345 passed, 0 failed, 0 skipped
Breakdown:
  unit/                                    123 tests
  integration/test_live_pipeline            17 tests
  integration/test_sqlite_queries           12 tests
  integration/test_report_yaml              19 tests
  integration/test_e2e_wbws                 13 tests
  integration/test_adversarial               8 tests
  integration/test_performance               7 tests
  integration/test_robustness               12 tests
  integration/test_threshold_calibration    22 tests
  integration/test_h02_wfo                   2 tests
  integration/test_block8a_foundation       12 tests
  integration/test_block8b_engines          14 tests
  integration/test_block8c_verdict          11 tests
  integration/test_block9a_orchestrator      7 tests
  integration/test_block9b_ga               28 tests
  integration/test_block9c_supporting       42 tests  ← NEW (Block 9C)
```
### Pipeline Stage Status
- Stage 0: ✅ Implemented (validation + init)
- Stage 1: 🟡 Stub — checkpoint advanced correctly (B9A-002 fixed)
- Stage 2: 🟡 Stub — checkpoint advanced correctly
- Stage 3: 🟡 Stub — checkpoint advanced correctly
- Stage 4: 🟡 Stub — checkpoint advanced correctly
- Stage 5: ✅ Implemented (MC Deep)
- Stage 6: ✅ Implemented (Sensitivity, OPT-01 applied)
- Stage 7: ✅ Implemented (Report + YAML output)
---
## Block 9C — What Was Done
Six supporting modules fully audited against source: `ranker.py`, `scenario.py`,
`wfo_engine.py`, `parameter_space.py`, `sampler.py`, `yaml_generator.py`.
**Three pre-existing bugs confirmed resolved at this layer (NOT in these files):**
- B9A-001 root: `ranker.rank_by_wfo()` returns `List[CandidateRecord]` correctly ✅ — bug is orchestrator-only inline version
- B9A-003 root: `scenario.load_scenario()` is clean ✅ — bug is orchestrator Stage 6 reads from config dict instead of ScenarioProfile
- B8B-005 root: `wfo_engine.run_wfo()` correctly passes OOS flags ✅ — bug is in `wfo_evaluator.py` / `consistency_scorer.py`
**New findings (9): B9C-001 through B9C-009.** Critical one is B9C-007 (LHS sort key).
**B8-006 scope expanded**: `yaml_generator._STRATEGY_PARAM_KEY_MAP` is a second YAML key map — both files must be updated together.
**42 tests added** — all passing. Key lesson: `test_run_wfo_writes_window_result_to_store`
required mocking `ProcessPoolExecutor` + `as_completed` directly (not just `evaluate_window`)
because `write_wfo_window_result` is called inside the `as_completed` loop, which only
executes when a real Future resolves. Patching the worker function alone doesn't work
(spawn boundary — L-01).
---
## Block 9D — Immediate Action Plan
### STEP 1: Fix prerequisites before any Stage 1 work
#### B9C-007 in `sampler.py` → `_lhs_sample()` (CRITICAL — fix first)
```python
# FIND (approximate location):
param_value_universe[name] = sorted(seen, key=lambda x: (str(type(x)), str(x)))
# REPLACE WITH:
try:
    param_value_universe[name] = sorted(seen, key=lambda x: float(x))
except (TypeError, ValueError):
    param_value_universe[name] = sorted(seen, key=lambda x: str(x))
```
#### B9C-006 in `sampler.py` → `sample_random()` docstring
Change "with replacement" → "without replacement" (implementation is correct, only docstring wrong).
#### B9A-003 in `orchestrator.py` → `_run_stage_6_sensitivity()`
```python
# CHANGE FROM:
spike_threshold = config.get("sensitivity", {}).get("spike_threshold", 0.15)
# TO:
spike_threshold = scenario.verdict_sensitivity_spike_threshold
```
#### B9A-001 in `orchestrator.py` → Stages 5, 6, 7
Replace orchestrator's inline `rank_by_wfo()` (returns `List[Dict]`) with
`ranker.rank_by_wfo(store, run_id, top_n)` (returns `List[CandidateRecord]`).
Then change dict-key access `record['candidate_id']` → attribute `record.candidate_id`.
### STEP 2: Quick P3 fixes (same session, time permitting)
**B9C-004** — `wfo_engine.py` top of `run_wfo()`:
```python
if not candidates:
    logger.warning("run_wfo called with empty candidates list — returning {}")
    return {}
```
**B9C-005** — `parameter_space.py` `_range_values()`: replace `str(step)` → `Decimal(str(step))` for robust scale detection.
**B8-006** — Add warning comment above both `_PARAM_KEY_MAP` in `strategy_runner.py` and `_STRATEGY_PARAM_KEY_MAP` in `yaml_generator.py`:
```python
# WARNING: Twin key map exists in [other_file.py].
# Both files MUST be updated together when adding/renaming strategy parameters.
```
### STEP 3: Stage 1 — Random Search
File: `orchestrator.py`, function `_run_stage_1_random_search()`
Logic:
1. `parameter_space.expand_zones(config)` → `Dict[str, List[CandidateParameterSet]]`
2. For each zone: `sampler.sample_lhs(zone_grid, n=config["random_search"]["samples_per_zone"], seed=run_metadata.random_search_seed)`
3. For each candidate: `strategy_runner.evaluate()` → `fitness.evaluate_fitness()` → build `CandidateRecord` with `stage=CandidateStage.RANDOM.value` → `store.write_candidate()`
4. `store.set_checkpoint(run_id, Checkpoint.RANDOM_SEARCH_COMPLETE)`
Key: `CandidateRecord.stage` is a **string** (`.value`), not the enum.
### STEP 4: Stage 2 — MC Pre-Filter
File: `orchestrator.py`, function `_run_stage_2_mc_prefilter()`
Logic:
1. Query `RANDOM` stage candidates that passed constraints, sort by fitness, take top N
2. For each: `mc_engine.run_mc(..., MCMode.PRE_FILTER, ..., seed=run_metadata.mc_prefilter_seed)` → `store.write_mc_result(result, run_id)`
3. Update candidate stage to `MC_PREFILTER_PASS` / `MC_PREFILTER_FAIL` based on ruin_probability vs ruin_threshold
4. `store.set_checkpoint(run_id, Checkpoint.MC_PREFILTER_COMPLETE)`
### STEP 5: Stage 3 — GA
File: `orchestrator.py`, function `_run_stage_3_ga()`
Critical — inject `_base_yaml_path` (B9B-003):
```python
config = dict(config)  # shallow copy
config['_base_yaml_path'] = str(base_yaml_path)
ga_engine.run_ga(store, run_id, scenario, windows, config, seed=run_metadata.ga_seed)
store.set_checkpoint(run_id, Checkpoint.GA_COMPLETE)
```
---
## Files Needed For Block 9D
### Required uploads
```
src/backtesting/orchestrator.py          ← main target: fixes + Stages 1–3
src/backtesting/sampler.py               ← B9C-006, B9C-007 fixes
```
### Optional (for quick fixes)
```
src/backtesting/wfo/wfo_engine.py        ← B9C-004 empty guard
src/backtesting/parameter_space.py       ← B9C-005 Decimal fix
src/backtesting/strategy_runner.py       ← B8-006 comment
src/backtesting/yaml_generator.py        ← B8-006 comment
configs/backtesting/backtest_template.yaml ← B8-003 verification
```
---
## Contract Field Reference (verified — do NOT deviate)
```python
# CandidateStage enum values (exact):
CandidateStage.RANDOM              # NOT RANDOM_SEARCH
CandidateStage.MC_PREFILTER_PASS
CandidateStage.MC_PREFILTER_FAIL
CandidateStage.GA
CandidateStage.WFO
CandidateStage.MC_DEEP
CandidateStage.SENSITIVITY
# CandidateRecord.stage: str = CandidateStage.X.value  (string, not enum)
# WFOWindow: window_id, start_date: date, end_date: date
# NO is_start, oos_start, is_end, oos_end fields
# WFOConsistencyScore fields (exact names):
#   composite_score               (NOT wfo_consistency_score)
#   median_window_return          (NOT median_oos_return)
#   window_return_variance        (NOT oos_return_variance)
#   worst_window_drawdown         (NOT worst_oos_drawdown)
#   oos_gate_triggered: bool      (NOT Optional — always False currently)
#   window_collapse_flag: bool    (NOT Optional)
#   median_oos_delta: Optional[float] = None
# VerdictResult — all 14 fields required:
#   candidate_id, scenario_name, verdict, deployment_status,
#   wfo_consistency_score, mc_deep_ruin_probability, sensitivity_spike,
#   oos_gate_triggered, window_collapse_flag, sensitivity_profile_incomplete,
#   median_oos_delta, parameter_region_width, yaml_output_path, evidence_summary
# RunMetadata — constraints:
#   config_hash: exactly 64 chars (SHA-256 hex)
#   wfo_window_ids: List[str], minimum 3 elements
#   started_at: datetime (required)
#   backtester_version: str (required)
#   perturbation_profile_name: str (required)
#   checkpoint: Checkpoint (required)
#   All 5 seeds required: random_search_seed, mc_prefilter_seed, ga_seed,
#                          mc_deep_seed, sensitivity_seed
```
---
## Critical Non-Negotiables
```python
# strategy_runner: mode_override="core" (NOT mode="core")
# store.query_mc_results: mode = "deep" or "pre_filter" (plain string, NOT MCMode enum)
# datetime: datetime.now(timezone.utc) ONLY — never datetime.utcnow()
# spawn boundary: mock patches don't cross ProcessPoolExecutor workers on Windows
# For wfo_engine write tests: mock ProcessPoolExecutor + as_completed at module level
# LIVE_APPROVED: never set in code — operator-only
# Snap-then-clamp in mutation (not clamp-then-snap)
# CandidateRecord.stage: str (.value) not enum
# Both _PARAM_KEY_MAP files updated together (strategy_runner + yaml_generator)
```
---
## Test Import Template (prevents circular import)
```python
import sys
from pathlib import Path
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
from src.utils.paths import PROJECT_ROOT
from src.backtesting.contracts import (...)   # BEFORE candidate_store
from src.backtesting.candidate_store import CandidateStore
```
---
## Open Findings Quick Reference
### Must fix in Block 9D
| ID | File | Change |
|---|---|---|
| B9C-007 ⚠️ | sampler.py | Fix `_lhs_sample` sort key to `float(x)` |
| B9C-006 | sampler.py | Fix `sample_random` docstring |
| B9A-003 | orchestrator.py | Stage 6 spike_threshold → ScenarioProfile |
| B9A-001 | orchestrator.py | Stages 5–7 rank_by_wfo → typed return |
| B9C-004 | wfo_engine.py | Add empty candidates guard |
| B9C-005 | parameter_space.py | Decimal(str(step)) |
| B8-006 | strategy_runner + yaml_generator | Cross-reference comment |
### P2 open (Block 9E)
| ID | File | Description |
|---|---|---|
| B8B-005 | wfo_evaluator + consistency_scorer | OOS gate — oos_delta always None |
### Pre-prod blocker (Block 9F)
| ID | Description |
|---|---|
| B8B-012 | WFO sigmoid scale=0.10 — calibrate after first real run |
### Deferred P3
B8-009, B8-003, B8B-003, B8B-011, B8B-013, B8C-002, B8C-003, B9B-001, B9C-008
---
## Block Roadmap
```
Block 9D (now):  Fix B9C-007/B9A-003/B9A-001 → Stage 1 (Random Search) → Stage 2 (MC Pre-Filter) → Stage 3 (GA)
Block 9E:        OOS gate implementation — IS/OOS split in wfo_evaluator + consistency_scorer (B8B-005)
Block 9F:        First real pipeline run → calibrate sigmoid scale (B8B-012), expectancy norm (B8B-003)
Stage 4 (TBD):   Full WFO implementation stub → real implementation
```