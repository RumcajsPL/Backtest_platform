# BLOCK8_AUDIT_REPORT.md — Full Audit Finding Registry
**Started**: Block 8 (2026-03-04) through Block 9C (2026-03-05)
**Last updated**: 2026-03-05 (Block 9C complete)
**Purpose**: Canonical finding registry for all audit findings across Blocks 8–9C.
             Findings not cleared here are tracked in SKILL.md and CONTEXT.md for planning.
---
## Status Legend
- ✅ **FIXED** — Code change applied and tested
- 📋 **DOCUMENTED** — Comment/runbook note added; no code change required
- 🔍 **CONFIRMED** — Finding investigated; root cause confirmed in different file than suspected
- ⚠️ **OPEN** — Active finding, not yet resolved
- ❌ **CLOSED** — False positive or no longer applicable
---
## Block 8A — Foundation Audit
### B8-001 ✅ FIXED
**Severity**: P2 | **File**: `candidate_store.py` + `contracts.py`
**Finding**: `median_oos_delta` field missing from `wfo_consistency_scores` schema, INSERT,
and SELECT. M-01 fix computed the value but never persisted it. `VerdictResult.median_oos_delta`
always `None` regardless of OOS gate state.
**Fix**: Added `median_oos_delta REAL` column to `_SCHEMA_SQL`; extended
`_write_wfo_consistency_score()` INSERT and `get_wfo_consistency_score()` SELECT.
**Test**: `test_block8a_foundation.py::test_wfo_median_oos_delta_persisted`
---
### B8-002 ✅ FIXED (alongside B8-001)
**Severity**: P3 | **File**: `candidate_store.py`
**Finding**: `CandidateRecord.wfo_median_oos_delta` hardcoded to `None` in `query_candidates()`
reconstruction — never joined from `wfo_consistency_scores`.
**Fix**: Extended `query_candidates()` SELECT to join `wfo_consistency_scores.median_oos_delta`.
**Test**: Covered by `test_block8a_foundation.py::test_wfo_median_oos_delta_persisted`
---
### B8-003 ⚠️ OPEN (P3)
**File**: `configs/backtesting/backtest_template.yaml`
**Finding**: Four new `ScenarioProfile` fields added in Blocks 7B/7D are not documented in
`backtest_template.yaml` with their defaults and calibration guidance:
- `wfo_collapse_drawdown_threshold` (default 0.40)
- `normalisation_drawdown_ref_points` (default 10,000.0)
- `normalisation_pnl_ref_points` (default 5,000.0)
- `normalisation_freq_ref_trades_per_week` (default 20.0)
**Target**: Block 9D — verify YAML and add calibration comments.
---
### B8-004 ⚠️ OPEN (P4)
**File**: `candidate_store.py`
**Finding**: `_drain_queue` dispatches via `getattr(self, method_name)`. No compile-time
or startup-time guard that all dispatch string keys correspond to existing `_write_*` methods.
A typo silently discards writes (same failure mode as H-02).
**Target**: Deferred — no current missing handler. Consider `_WRITE_DISPATCH` dict with
`assert` in `__init__`. Block 9D or later.
---
### B8-005 ✅ FIXED
**Severity**: P2 | **File**: `orchestrator.py` (`_run_stage_0_init`)
**Finding**: `min_significant_trades` never validated at Stage 0 — a value of `0` makes the
significance guard always `False`. `spike_threshold` also unvalidated — `0.0` or `> 1.0`
produces silent incorrect behaviour.
**Fix**: Added validation in `_run_stage_0_init`:
```python
if min_significant_trades < 1:
    raise ValueError(f"min_significant_trades must be >= 1; got {min_significant_trades}")
if not (0.0 < spike_threshold < 1.0):
    raise ValueError(f"sensitivity.spike_threshold must be in (0, 1); got {spike_threshold}")
```
**Test**: `test_block8a_foundation.py::test_stage0_rejects_zero_min_trades`,
`test_block8a_foundation.py::test_stage0_rejects_invalid_spike_threshold`
---
### B8-006 ⚠️ OPEN (P3) — scope expanded Block 9C
**File**: `strategy_runner.py` + `yaml_generator.py`
**Finding**: Two files define the strategy parameter → YAML path mapping:
- `strategy_runner._PARAM_KEY_MAP`
- `yaml_generator._STRATEGY_PARAM_KEY_MAP`
Both must be updated in sync when strategy parameters are added or renamed. Stale dot-paths
cause `_deep_set()` to silently create new keys rather than overwriting intended keys.
**Block 9C update**: B8-006 scope expanded — originally only `strategy_runner.py`; audit
confirmed `yaml_generator.py` carries a second independent copy of the same mapping.
**Target**: Add cross-reference comment in both files. Operator checklist updated (§1.7).
Block 9D.
---
### B8-007 ⚠️ OPEN (P4)
**File**: `orchestrator.py`
**Finding**: Stage 1–4 stub call sites have no comment explaining that these stages produce
no output and that Stages 5–7 may be consuming data from a prior run.
**Target**: Add comment block in `_execute_pipeline`. Block 9D when stubs are implemented.
---
### B8-008 ⚠️ OPEN (P4)
**File**: `orchestrator.py`
**Finding**: `perf_counter` timing instrumentation covers only Stages 5–7. `TIMING SUMMARY`
log line is incomplete when Stages 1–4 are active.
**Target**: Extend timing to all stages when Stages 1–4 are implemented. Block 9D.
---
### B8-009 ⚠️ OPEN (P3)
**File**: `orchestrator.py` (`_resume_or_start`)
**Finding**: Opens raw `sqlite3.connect()` directly, bypassing `CandidateStore`. Schema
changes to `runs` table must be duplicated. Architecturally incorrect though functionally safe
(read-only, properly closed).
**Target**: Add `CandidateStore.find_resumable_run()` and `find_conflicting_run()` read
methods. Deferred — low risk. Block 9D or later.
---
## Block 8B — Evaluation Engines Audit
### B8B-001 ✅ FIXED
**Severity**: P2 | **File**: `fitness.py`
**Finding**: NaN metric values silently passed all constraint checks (`NaN < x` is `False`
under IEEE 754). Candidate could reach `_compute_weighted_score` with NaN inputs, producing
an unhandled `ValueError` from `FitnessResult.__post_init__` rather than a clean rejection.
**Fix**: Explicit NaN guard in `evaluate_fitness()` before constraint loop — short-circuits
with `EVALUATION_ERROR` rejection.
**Test**: `test_block8b_engines.py::test_nan_win_rate_rejected_not_passed`
---
### B8B-002 📋 DOCUMENTED (P4)
**File**: `fitness.py`
**Finding**: Constraint boundary semantics (`op.lt` / `op.gt`) produce inclusive boundaries
(value exactly at threshold is accepted) but this is not documented anywhere in source.
**Resolution**: Documented in ARCHITECTURE.md §7 (fitness evaluation). Comment to be added
above `_CONSTRAINT_CHECKS` in source. Code is correct.
**Test**: `test_block8b_engines.py::test_constraint_boundary_win_rate_exact`
---
### B8B-003 ⚠️ OPEN (P3)
**File**: `fitness.py`
**Finding**: Expectancy normalisation `scale=3.0` hardcoded — not lifted into `ScenarioProfile`
like the M-02 fields. Comment in source flagged this as deferred to Block 8.
**Target**: Add `normalisation_expectancy_ref_points: float = 3.0` to `ScenarioProfile`.
Deferred until first real-run calibration of all normalisation constants.
---
### B8B-005 ⚠️ OPEN (P2)
**Files**: `wfo_evaluator.py`, `consistency_scorer.py`
**Finding**: `oos_delta` is always `None` — never computed. `wfo_engine.run_wfo()` passes
flags correctly (confirmed Block 9C); the non-functionality lives in evaluator/scorer.
Cascading effects:
- `oos_gate_triggered` always `False` in every `WFOConsistencyScore`
- `median_oos_delta` always `None`
- `VerdictResult.oos_gate_triggered` modifier permanently silent
**Status**: `enforce_oos_gate: true` has no effect. See OPERATOR_RUNBOOK §11.1.
**Target**: Block 9E — requires structural IS/OOS window split design decision.
---
### B8B-011 ⚠️ OPEN (P3)
**File**: `consistency_scorer.py`
**Finding**: Single valid window gives `variance_raw = 0.0` → `variance_norm = 1.0` — the
best possible score. A single data point cannot demonstrate temporal consistency.
**Target**: Return `variance_norm = 0.5` (neutral) for `windows_evaluated == 1`. Deferred.
---
### B8B-012 ⚠️ OPEN (Pre-production blocker)
**File**: `consistency_scorer.py`
**Finding**: `_sigmoid_normalise` uses `scale=0.10`. With real metrics in pips/points,
all positive net_pnl maps to ≈1.0 and negative to ≈0.0 — sigmoid becomes binary. Two
candidates earning 10 pts/window vs 10,000 pts/window receive identical `median_return_norm`.
Also: `_MAX_EXPECTED_VARIANCE = 0.10` has the same calibration mismatch.
**Status**: OPEN — calibration requires first real run. See OPERATOR_RUNBOOK §11.2.
**Target**: Measure per-window net_pnl distribution from first real run; set
`wfo_sigmoid_scale ≈ 10% of median expected per-window pip value` in ScenarioProfile.
---
### B8B-013 ⚠️ OPEN (P3)
**File**: `mc_engine.py`
**Finding**: `ruin_threshold` dual-source — read from config dict AND in
`ScenarioProfile.mc_prefilter_ruin_threshold`. `mc_engine` does not receive ScenarioProfile;
the two values must agree manually in `backtest_template.yaml`.
**Target**: `mc_engine` should eventually receive `ScenarioProfile` and read
`mc_prefilter_ruin_threshold` from it. Add comment to `_run_mc_internal()`. Deferred.
---
### B8B-017 📋 DOCUMENTED (P4)
**File**: `mc_metrics.py`
**Finding**: `p5_final_equity` is computed and stored but not used in any verdict decision.
Risk of future removal as "apparently unused".
**Resolution**: Comment added clarifying it is a **reporting metric only**, not a verdict
input. No code change required.
---
### B8B-018 ✅ FIXED
**Severity**: P2 | **File**: `wfo_evaluator.py`
**Finding**: Used `net_pnl` attribute name when `MetricsReport` defines the field as
`total_pnl_points`. Same mismatch for `expectancy` vs `expectancy_points`. `net_pnl` and
`expectancy` always returned `None`, making `fraction_positive_windows` always 0.0 and
`median_return_raw` always 0.0.
**Fix**: Changed `_safe_float(m, "net_pnl")` → `_safe_float(m, "total_pnl_points")` and
`_safe_float(m, "expectancy")` → `_safe_float(m, "expectancy_points")`.
**Test**: `test_block8b_engines.py::test_net_pnl_field_name_matches_metrics_report`
---
## Block 8C — Verdict, Sensitivity, Report Audit
### B8C-001 ✅ FIXED
**File**: `contracts.py` (`ScenarioProfile.__post_init__`)
**Finding**: `report_emphasis` accepted as scalar string (e.g. `"balanced"`), which iterates
as individual characters in `report_generator._render_scenario_metrics()`.
**Fix**: Added `isinstance(self.report_emphasis, (list, tuple)) and len > 0` guard.
**Test**: `test_block8c_verdict_sensitivity.py`
---
### B8C-002 ⚠️ OPEN (P3)
**File**: `report_generator.py`
**Finding**: Chart `figsize` is hardcoded (e.g. `figsize=(12, 6)`). Should be configurable
or responsive to data volume.
**Target**: Deferred — cosmetic impact only.
---
### B8C-003 ⚠️ OPEN (P3)
**File**: `report_generator.py`
**Finding**: `query_wfo_window_results()` missing `run_id` filter — could return results
from multiple runs if the DB contains data from prior runs.
**Target**: Add `WHERE run_id = ?` filter. Block 9D or later.
---
### B8C-004 ⚠️ OPEN (P4)
**File**: `sensitivity.py`
**Finding**: Worker crash log line missing `candidate_id` — makes debugging difficult when
multiple candidates are processed concurrently.
**Target**: Add `candidate_id` to the exception log in the worker. Low priority.
---
### B8C-006 ⚠️ OPEN (P4)
**File**: `verdict.py`
**Finding**: `NO_GO` branch sets `deployment_status = DeploymentStatus.PAPER_TRADE_REQUIRED`
in two separate places — a duplicate branch that could diverge if the status value changes.
**Target**: Consolidate to single assignment. Low priority.
---
### B8C-007 ❌ CLOSED (False positive)
**Finding**: Stage 7 missing None guards for `wfo_score` / `mc_result` before calling
`compute_verdict()`.
**Resolution**: Code review confirmed Stage 7 correctly guards `None` values and skips
with WARNING log. No bug.
---
## Block 9A — Orchestrator Audit
### B9A-001 ⚠️ OPEN (P3)
**File**: `orchestrator.py`
**Finding**: Orchestrator's inline `rank_by_wfo()` returns `List[Dict]` — raw dicts, not
`List[CandidateRecord]`. Used in Stages 5, 6, 7 with dict-key access.
**Block 9C clarification**: `ranker.rank_by_wfo()` (the module-level function) is correct
and returns typed records. Bug is orchestrator-only. Stages 5–7 should use
`ranker.rank_by_wfo()` directly when refactored.
**Target**: Block 9D — use `ranker.rank_by_wfo()` in orchestrator Stages 5–7.
---
### B9A-002 ✅ FIXED
**File**: `orchestrator.py`
**Finding**: Stage 1 stub did not advance `RANDOM_SEARCH_COMPLETE` checkpoint — Stage 1
re-ran on every resume.
**Fix**: Added `store.set_checkpoint(run_id, Checkpoint.RANDOM_SEARCH_COMPLETE)` at end
of `_run_stage_1_random_search()`. Confirmed all 4 stubs now advance their checkpoints.
**Test**: `test_block9a_orchestrator.py`
---
### B9A-003 ⚠️ OPEN (P3)
**File**: `orchestrator.py` (Stage 6)
**Finding**: `spike_threshold` dual-source — Stage 6 reads from
`config["sensitivity"]["spike_threshold"]` while `verdict.py` reads from
`ScenarioProfile.verdict_sensitivity_spike_threshold`. Both must be kept in sync.
**Block 9C clarification**: `scenario.load_scenario()` is correct — it loads the field into
ScenarioProfile. Bug is orchestrator Stage 6 only.
**Fix**: Stage 6 should read `scenario.verdict_sensitivity_spike_threshold` directly.
**Target**: Block 9D.
---
### B9A-004 ⚠️ OPEN (P4)
**File**: `orchestrator.py`
**Finding**: Stage 6 calls `load_scenario()` internally rather than using the scenario
already loaded in Stage 0. Violates P1 SRP.
**Target**: Pass scenario object from Stage 0 through to Stage 6. Block 9D.
---
### B9A-005 ⚠️ OPEN (P4)
**File**: `orchestrator.py`
**Finding**: Stage 0 validates `spike_threshold` from config dict. Once B9A-003 is fixed
(Stage 6 reads from ScenarioProfile), this validation becomes redundant (ScenarioProfile
already validates the field).
**Target**: Remove Stage 0 spike_threshold validation after B9A-003 fix. Block 9D.
---
### B9A-006 ❌ CLOSED (False alarm)
**Finding**: Concern that `CandidateParameterSet.candidate_id` might not be stable when
reconstructing from `rank_by_wfo()` dict records.
**Resolution**: `candidate_id` is SHA-256 of `parameters` dict — fully deterministic.
Reconstructing from same params always gives same ID. Safe.
---
## Block 9B — GA Package Audit
### B9B-001 ⚠️ OPEN (P3)
**File**: `crossover.py`
**Finding**: No zone-name assertion for cross-zone parents. If two parents from different
zones are crossed (should not happen given GA pool structure, but not prevented), a
mixed-zone child is produced silently. Downstream mutation clamping enforces `parent_a`'s
zone bounds but logs no warning.
**Target**: Add assertion or log warning when `parent_a.zone_name != parent_b.zone_name`.
Deferred.
---
### B9B-002 ⚠️ OPEN (P4)
**File**: `diversity.py`
**Finding**: Degenerate parameter (`min == max`) is silently skipped with no log warning.
**Target**: Add `logger.debug` when a degenerate parameter is skipped. Low priority.
---
### B9B-003 ⚠️ OPEN (P3)
**File**: `ga_engine.py`
**Finding**: `config['_base_yaml_path']` is a private injected key — not present in
`backtest_template.yaml`. When Stage 3 is implemented, orchestrator must inject:
```python
config['_base_yaml_path'] = str(_resolve_base_yaml(config))
```
**Target**: Fix when Stage 3 is implemented. Block 9D.
---
### B9B-004 ⚠️ OPEN (P4)
**File**: `ga_engine.py`
**Finding**: Diversity penalty elites use prev-generation fitness scores rather than
re-evaluating. This is standard GA behaviour, but the intent is not documented.
**Target**: Add comment documenting intent. Low priority.
---
## Block 9C — Supporting Modules Audit
### B9C-001 ⚠️ OPEN (P4)
**File**: `ranker.py`
**Finding**: `rank_by_wfo()` has no `stage` filter — cross-stage query by design (WFO scores
span all stages). Docstring does not clarify this intent.
**Target**: Add docstring clarification. Low priority.
---
### B9C-002 ⚠️ OPEN (P4)
**File**: `ranker.py`
**Finding**: `rank_combined()` sorts by `fitness_score or 0.0`. A `None` fitness_score
is silently ranked at 0.0 (should not reach here given constraint filtering, but possible).
**Target**: Add explicit None handling or assertion. Low priority.
---
### B9C-003 ⚠️ OPEN (P4)
**File**: `scenario.py`
**Finding**: Direct `s["key"]` access raises `KeyError`, not `ValueError` — inconsistent
with project's fail-fast `ValueError` convention for config errors.
**Target**: Wrap key access in try/except and raise `ValueError`. Low priority.
---
### B9C-004 ⚠️ OPEN (P3)
**File**: `wfo_engine.py`
**Finding**: No early-exit guard for empty `candidates` list before `ProcessPoolExecutor`
entry. An empty list produces no tasks, no futures, and returns an empty dict — correct
but unguarded and silent.
**Target**: Add `if not candidates: return {}` with warning log. Block 9D.
---
### B9C-005 ⚠️ OPEN (P3)
**File**: `parameter_space.py`
**Finding**: `str(step)` for scale detection is fragile for floats with non-canonical
representation. `Decimal(str(step))` would be safer.
**Target**: Replace `str(step)` scale detection with `Decimal(str(step))`. Block 9D.
---
### B9C-006 ⚠️ OPEN (P3)
**File**: `sampler.py`
**Finding**: `sample_random()` docstring says "Uniform random sampling **with replacement**"
but implementation uses `rng.sample()` (without replacement). Implementation is correct.
**Target**: Update docstring only. Block 9D (trivial fix).
---
### B9C-007 ⚠️ OPEN (P3) — FIX BEFORE STAGE 1
**File**: `sampler.py`
**Finding**: `_lhs_sample()` sorts parameter value universe by `(str(type), str(val))`.
For numeric parameters, this is lexicographic: `[9, 10, 11]` sorts as `[10, 11, 9]`.
Breaks the LHS space-filling property for any numeric parameter with values ≥ 10.
**Fix**:
```python
try:
    param_value_universe[name] = sorted(seen, key=lambda x: float(x))
except (TypeError, ValueError):
    param_value_universe[name] = sorted(seen, key=lambda x: str(x))
```
**Target**: Block 9D — must be fixed before Stage 1 implementation.
**Test**: `test_block9c_supporting.py::TestSampler::test_lhs_sample_internal_numeric_sort_preserves_space_filling`
---
### B9C-008 ⚠️ OPEN (P3)
**File**: `yaml_generator.py`
**Finding**: `_structural_validate()` fallback (when `StrategyConfig` is not importable)
only checks that required sections exist — does not check field types. A structurally
valid but type-invalid config passes. Acceptable as documented best-effort.
**Target**: Document explicitly as best-effort. Low priority.
---
### B9C-009 ⚠️ OPEN (P4)
**File**: `wfo_engine.py`
**Finding**: No defensive warning log when `candidates` list is empty. Correct behaviour
(returns empty dict) but silent.
**Target**: Add `logger.warning` for empty candidates. Low priority.
---
## Summary Tables
### All Open Findings by Severity
#### Pre-Production Blocker
| ID | File | Description |
|---|---|---|
| B8B-012 | consistency_scorer.py | WFO sigmoid scale=0.10 — calibrate after first real run |
#### P2 — Fix This Block (Block 9D priority)
| ID | File | Description |
|---|---|---|
| B8B-005 | wfo_evaluator.py / consistency_scorer.py | OOS gate non-functional — oos_delta always None |
#### P3 — Tracked
| ID | File | Description | Target |
|---|---|---|---|
| B8-003 | backtest_template.yaml | M-02/M-03 fields not documented | 9D |
| B8-006 | strategy_runner.py + yaml_generator.py | Dual _PARAM_KEY_MAP files | 9D |
| B8-009 | orchestrator.py | Raw sqlite3 in _resume_or_start | Deferred |
| B8B-003 | fitness.py | expectancy_norm scale=3.0 hardcoded | Deferred |
| B8B-011 | consistency_scorer.py | Single-window variance optimistic | Deferred |
| B8B-013 | mc_engine.py | ruin_threshold dual-source | Deferred |
| B8C-002 | report_generator.py | Chart figsize hardcoded | Deferred |
| B8C-003 | report_generator.py | query_wfo_window_results missing run_id filter | Deferred |
| B9A-001 | orchestrator.py | rank_by_wfo() returns List[Dict] | 9D |
| B9A-003 | orchestrator.py | spike_threshold dual-source | 9D |
| B9B-001 | crossover.py | No zone-name guard for cross-zone parents | Deferred |
| B9B-003 | ga_engine.py | config['_base_yaml_path'] injected key | Fix with Stage 3 |
| B9C-004 | wfo_engine.py | No guard for empty candidates list | 9D |
| B9C-005 | parameter_space.py | str(step) fragile float repr | 9D |
| B9C-006 | sampler.py | sample_random docstring wrong | 9D |
| **B9C-007** | **sampler.py** | **_lhs_sample lexicographic sort — FIX BEFORE STAGE 1** | **9D** |
| B9C-008 | yaml_generator.py | _structural_validate type-blind | Deferred |
#### P4 — Cosmetic / Noted
| ID | File | Description |
|---|---|---|
| B8-004 | candidate_store.py | No compile-time guard on dispatch map |
| B8-007 | orchestrator.py | Stub call sites need comment |
| B8-008 | orchestrator.py | Timing covers Stages 5–7 only |
| B8B-002 | fitness.py | Constraint boundary semantics undocumented (now in ARCHITECTURE) |
| B8B-017 | mc_metrics.py | p5_final_equity reporting-only not documented |
| B8C-004 | sensitivity.py | Worker crash log missing candidate_id |
| B8C-006 | verdict.py | NO_GO deployment_status duplicate branch |
| B9A-004 | orchestrator.py | Stage 6 calls load_scenario() internally |
| B9A-005 | orchestrator.py | Stage 0 spike_threshold validation becomes dead code after B9A-003 fix |
| B9B-002 | diversity.py | Degenerate param skipped silently |
| B9B-004 | ga_engine.py | Diversity elites use prev-gen fitness — document intent |
| B9C-001 | ranker.py | rank_by_wfo() cross-stage query — needs docstring |
| B9C-002 | ranker.py | rank_combined() None fitness ranked at 0.0 |
| B9C-003 | scenario.py | KeyError not ValueError for missing config keys |
| B9C-009 | wfo_engine.py | No warning for empty candidates list |
### Fixed / Closed Findings
| ID | Status | Description |
|---|---|---|
| B8-001 | ✅ FIXED | median_oos_delta persistence in DB |
| B8-002 | ✅ FIXED | CandidateRecord.wfo_median_oos_delta populated |
| B8-005 | ✅ FIXED | Stage 0: min_significant_trades + spike_threshold validation |
| B8B-001 | ✅ FIXED | NaN guard in fitness.py |
| B8B-018 | ✅ FIXED | wfo_evaluator field names: total_pnl_points, expectancy_points |
| B8C-001 | ✅ FIXED | report_emphasis scalar string guard in ScenarioProfile |
| B8C-007 | ❌ CLOSED | False positive — Stage 7 None guards confirmed present |
| B9A-002 | ✅ FIXED | Stage 1 stub advances RANDOM_SEARCH_COMPLETE checkpoint |
| B9A-006 | ❌ CLOSED | False alarm — candidate_id SHA-256 is deterministic |
---
## Block Planning
### Block 9D — Priority Fixes + Stage 1–3 Implementation
**Prerequisites before starting**:
- Fix B9C-007 (`_lhs_sample` sort key) — required for Stage 1 correctness
- Fix B9C-006 (`sample_random` docstring)
- Fix B9A-003 (orchestrator Stage 6 spike_threshold → ScenarioProfile)
- Fix B9A-001 (orchestrator rank_by_wfo → use ranker.rank_by_wfo())
**Main scope**:
- Implement Stage 1 (Random Search): `parameter_space.expand_zones()` + `sampler.sample_lhs()` + `strategy_runner.evaluate()` + `fitness.evaluate_fitness()` + `write_candidate()` loop
- Implement Stage 2 (MC Pre-Filter): `run_mc(..., mode=PRE_FILTER)` + `write_mc_result()` loop
- Implement Stage 3 (GA): `ga_engine.run_ga()` with `config['_base_yaml_path']` injection (B9B-003)
- Address P3 quick fixes: B9C-004, B9C-005, B8-003, B8-006 comment
**Files to upload for 9D**:
```
src/backtesting/orchestrator.py
configs/backtesting/backtest_template.yaml
```
### Block 9E — OOS Gate Implementation (B8B-005)
Requires structural design decision on IS/OOS window splitting within each WFO window.
Files: `wfo_evaluator.py`, `consistency_scorer.py`, `wfo_engine.py`, `contracts.py`
Prerequisite: Stages 1–4 implemented (Block 9D).
### Block 9F — Calibration Run + Normalisation Fixes
After first real pipeline run (Stages 1–4 working):
- Measure per-window net_pnl distribution → calibrate `wfo_sigmoid_scale` (B8B-012)
- Calibrate `normalisation_expectancy_ref_points` (B8B-003)
- Calibrate M-02 fields in ScenarioProfile
- Verify/document M-02/M-03 fields in backtest_template.yaml (B8-003)
### Deferred (no target block yet)
B8-009, B8B-011, B8B-013, B8C-002, B8C-003, B9B-001, B9C-008 — low risk, cosmetic,
or require design decisions not yet made.