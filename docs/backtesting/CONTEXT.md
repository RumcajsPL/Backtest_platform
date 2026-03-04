# CONTEXT.md — Backtesting & Optimization Framework
**Updated**: 2026-03-04 (Block 7 COMPLETE — all sub-blocks 7A through 7D done)
---
## Current State
**Phase 6 complete. Block 7 complete. 246 tests green.**
**Next: Block 8 — Full E2E code analysis and hardening.**
No outstanding audit findings. All M-series, H-series, I-series, WF-series items resolved or dispositioned.
---
## Block 7 Sub-Block Status (FINAL)
| Sub-Block | Status | Tests added | Files changed |
|---|---|---|---|
| 7A — H-02 fix + H-03/I-07 + SKILL.md | ✅ COMPLETE | +2 (235 total) | candidate_store.py, wfo_evaluator.py |
tests\backtesting\integration\test_h02_wfo_window_writes.py [100%] - 2 passed in 0.75s
| 7B — Audit M P1/P2 | ✅ COMPLETE | +7 (242 total) | contracts.py, mc_metrics.py, consistency_scorer.py, fitness.py, orchestrator.py |
tests\backtesting\integration\test_7b_audit_m_series.py [100%] - 7 passed in 5.41s 
| 7C — OPT-01 pool reuse | ✅ COMPLETE | 0 | sensitivity.py, orchestrator.py |
PERFORMANCE SUMMARY — run_id=39295701
  Config (production values, no smoke overrides):
    MC iterations          : 3000
    MC input candidates    : 10
    Sensitivity input      : 5
    Sensitivity max_steps  : 2
    Max workers            : 6
  Candidates processed:
    WFO survivors injected : 20
    Stage 5 MC processed   : 10
    Stage 6 Sens processed : 5
  Stage 5 MC Deep         : 0.3s  (0.0s/candidate avg)
  Stage 6 Sensitivity     : 297.8s  (59.6s/candidate avg)
  Stage 7 Report + Output : 3.9s
  Total                   : 302.0s  
  Budget                  : 14400s
  Status                  : PASS ✅
  Bottleneck              : Stage 6 (98.6% of total)
  Exception               : None
  Yet to analyze during Block 8
| 7D — M-01, M-06, WF-07/WF-09 | ✅ COMPLETE | +4 (246 total) | contracts.py, consistency_scorer.py, verdict.py, mutation.py |
tests\backtesting\integration\test_7d_audit_m01_m06.py [100%] 4 passed in 0.50s  
---
## All Audit Finding Dispositions (COMPLETE)
| ID | Finding | Verdict | Action |
|---|---|---|---|
| H-01 | `strategy_runner.evaluate()` date range | FALSE POSITIVE | Confirmed in source |
| H-02 | `CandidateStore` missing write methods | FIXED 7A | Both methods + 2 handlers added |
| H-03 | WFO date range not passed | FALSE POSITIVE | Same source read as H-01 |
| M-01 | `median_oos_delta` always None | FIXED 7D | Computed in consistency_scorer, propagated |
| M-02 | Hardcoded fitness normalisation constants | FIXED 7B | 3 fields added to ScenarioProfile |
| M-03 | Hardcoded WFO collapse threshold | FIXED 7B | `wfo_collapse_drawdown_threshold` added |
| M-04 | MC zero-equity drawdown understatement | FIXED 7B | Ruined paths clamped to 1.0 |
| M-05 | No Stage 0 param name validation | FIXED 7B | `_validate_parameter_names()` in Stage 0 |
| M-06 | Hardcoded mutation std dev | FIXED 7D | `mutation_std_steps` kwarg added |
| M-07 | Hardcoded chart dimensions | DEFERRED B8 | P4 — Block 8 analysis phase |
| I-07 | `datetime.utcnow()` | FIXED 7A | 3x replaced with `datetime.now(UTC)` |
| WF-07 | `parameter_region_width` always None | DOCUMENTED | Explicit deferred comment in verdict.py |
| WF-09 | Post-Stage-1 adequacy warning | DOCUMENTED | Intent in FUNCTIONAL_SPEC; Stage 1 still stub |
---
## Block 7D — What Changed

### contracts.py
**WFOConsistencyScore** — new field appended at end with default:
```python
median_oos_delta: Optional[float] = None  # M-01
```
**CandidateRecord** — new WFO field:
```python
wfo_median_oos_delta: Optional[float]     # M-01
```
### consistency_scorer.py — M-01
Computes median_oos_delta while valid_results are in scope. No extra DB query.
Passed as `median_oos_delta=median_oos_delta` to `WFOConsistencyScore`.
`None` when all windows have `oos_delta=None` (gate disabled or pre-OOS run).
### verdict.py — M-01
Dead `_compute_median_oos_delta()` helper removed (had always returned `None`
with a placeholder docstring). Replaced with:
```python
median_oos_delta: Optional[float] = wfo_score.median_oos_delta
```
Added to `logger.info()` call for observability.

### mutation.py — M-06
`mutation_std_steps: float = 2.0` kwarg added to `mutate()` and threaded through
to `_mutate_int()` and `_mutate_float()`. Default 2.0 exactly preserves prior behaviour.
Correctly kept as a YAML/config-level parameter, not added to `ScenarioProfile`
(mutation is a GA process parameter, not an evaluation-lens parameter).
---
## Test Inventory
| File | Count | Phase/Block | Status |
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
| test_7b_audit_m_series.py | 7 | 7 Blk 7B | ✅ |
| test_7d_audit_m01_m06.py | 4 | 7 Blk 7D | ✅ |
| **Total** | **246** | | **✅** |
---
## Performance Baseline
```
Windows 10, 6 workers, 3-month WBWS data slice
Locked at Block 3: Total=337s  Stage6=333s
OPT-01 (Block 7C): Stage 6 target <= 200s — verify on local machine with test_performance.py
```
---
## Architecture Constraints (Non-Negotiable)
```python
# Contracts: frozen dataclasses — never raw dicts between modules
# CandidateParameterSet.create()  — always use factory
# strategy_runner, run_mc, evaluate_sensitivity — never raise to caller
# datetime.now(timezone.utc)      — never datetime.utcnow()
# pathlib.Path + src/utils/paths.py — never hardcoded separators
# ProcessPoolExecutor spawn mode  — no fork-dependent code
# LIVE_APPROVED                   — never set in code, operator-only
# store.close()                   — always in finally block
# mode_override="core"            — not mode="core"
# mutation_std_steps              — YAML/config only, NOT ScenarioProfile
# parameter_region_width          — always None until Block 8 ML layer
# Stage 1                         — still a stub; adequacy warning deferred
```
---
## Block 8 — E2E Code Analysis and Hardening
Block 8 shifts from feature work to treating the pipeline as a production system.
This is a deep analytical pass: find everything that could silently misbehave on
a real multi-month run before real capital is involved.
### Phase 1 — Static Analysis
Run `mypy --strict`, `ruff`, `vulture` across the full `src/backtesting/` tree.
Every finding triaged: fix, document with justification, or reject with reason.
No suppressions added without a written rationale comment.
Expected findings: Optional chaining gaps, unreachable branches, unused imports.
### Phase 2 — Contract Completeness Audit
Every public function: does return type match what all callers actually use?
Every `Optional` field in every contract: enumerate every code path where it is
`None` and confirm every consumer handles `None` correctly (no silent `0.0` defaults).
Every frozen dataclass: are all fields actually written and read, or are any stubs?
`parameter_region_width`, `yaml_output_path`: both `None` — document expected population path.
### Phase 3 — Edge Case and Boundary Hardening
Targeted inputs the current test suite does not cover:
- Empty candidate list at Stage 6 entry
- Single-window WFO (below minimum of 3 — should fail fast at Stage 0)
- Zero-trade history (total_trades=0, expectancy undefined)
- Parameter space with step > range (degenerate zone definition)
- All candidates identical parameter hash (GA crossover degenerate case)
- ProcessPoolExecutor pool exhaustion under memory pressure
### Phase 4 — Production Run Preparation
- Upload full-date-range OHLCV data
- Extend WFO windows to cover full slice
- Implement Stage 1 (Random Search) — replace stub
- Calibrate `normalisation_drawdown_ref_points` and `normalisation_pnl_ref_points`
  from actual first-run metrics distribution
- Paper trading setup for any AUTO_GO candidates
### Files to upload at Block 8 start
Full `src/backtesting/` tree for static analysis.
At minimum: `orchestrator.py`, `strategy_runner.py`, `ga/ga_engine.py`,
`monte_carlo/mc_engine.py`, `wfo/wfo_engine.py`, `report_generator.py`,
`candidate_store.py`, all `evaluation/` files.