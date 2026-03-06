# CONTEXT.md — Block 9F Handoff
**Generated**: 2026-03-06 (end of Block 9F session)
**From**: Block 9F — First Real Pipeline Run + Bug Fixes
**To**: Block 9G — First Clean Pipeline Run + Calibration (B8B-012, B8B-003)
---
## Current Pipeline State

### Stage Implementation Status
| Stage | Name | Status |
|---|---|---|
| 0 | Validation & Init | ✅ Implemented |
| 1 | Random Search | ✅ Implemented |
| 2 | MC Pre-Filter | ✅ Implemented (B9F-003 fixed) |
| 3 | Genetic Algorithm | ✅ Implemented (B9F-002 fixed) |
| 4 | Full WFO | 🟡 Stub (logs + advances checkpoint only) |
| 5 | MC Deep | ✅ Implemented |
| 6 | Parameter Sensitivity | ✅ Implemented |
| 7 | Report & Output | ✅ Implemented |
### Block 9F Fixes Applied
| ID | File | Description |
|---|---|---|
| B9F-002 ✅ | orchestrator.py | Stage 3 graceful skip when no MC_PREFILTER_PASS candidates |
| B9F-003 ✅ | orchestrator.py | Stage 2 re-evaluates candidate via evaluate() instead of store reconstruction |
| B9F-004 ✅ | equity_simulator.py | extract_trade_returns uses trade.pnl_points (not trade.pnl) |
| B9F-005 ✅ | strategy_runner.py | evaluate() accepts date_start/date_end; _write_temp_yaml injects data.date_range |
### Critical Skill Correction
**H-01 was INCORRECTLY marked as FALSE POSITIVE in the skill.**
strategy_runner.evaluate() did NOT accept date_start/date_end before B9F-005.
wfo_evaluator.py was always correct in passing them — the runner was missing
the parameters. H-01 must be re-marked as FIXED (B9F-005) in the skill.
### Run Status
- Scenario `e2e_test` used for pipeline validation (loose constraints).
- B9F-005 fix (strategy_runner date_start/date_end) NOT yet run-tested.
- Next session: delete DB, run pipeline with e2e_test, confirm Stages 1–3 complete.
- Then run with capital_accumulation (calibrated constraints) for real calibration data.
### OOS Gate
- Off by default (enforce_oos_gate: false). Do not enable until calibrated.
### Test Suite
345 passing (Block 9C baseline). Blocks 9D/9E/9F add no new tests.
---
## Block 9G — Operator Preparation
### STEP 1 — Deploy all Block 9F output files
```
src/backtesting/orchestrator.py          ← _run_stage_3_ga (B9F-002)
                                          ← _run_stage_2_mc_prefilter (B9F-003)
src/backtesting/monte_carlo/equity_simulator.py  ← extract_trade_returns (B9F-004)
src/backtesting/strategy_runner.py       ← evaluate() + _write_temp_yaml (B9F-005)
configs/backtesting/backtest_1st_run.yaml ← min_win_rate eased to 0.15
```
### STEP 2 — Delete the stale DB
```bash
del outputs\backtesting\backtester.db
```
Every run attempt this session left partial checkpoints. Start fresh.
### STEP 3 — Run with e2e_test scenario first
In backtest_1st_run.yaml, set:
```yaml
scenario: "e2e_test"
```
Run and confirm all stages 1–3 complete without error.
Expected log pattern:
```
Stage 1: Random Search complete — evaluated=50 passed=50 failed=0
Stage 2: MC Pre-Filter complete — pass=P fail=F total=30
Stage 3: Genetic Algorithm — 5 windows, seed=44
GA starting: pop=N gens=5 ...
GA complete: final_best=X.XXXX
Stage 3: Genetic Algorithm complete
Stage 4: Full WFO — stub, not yet implemented
Stage 5: No candidates with WFO scores — skipping MC Deep
Stage 6: No candidates with WFO scores — skipping Sensitivity
Stage 7: No candidates available — generating empty report
Pipeline complete
```
### STEP 4 — Run calibration queries (from CONTEXT.md Block 9F)
After a successful e2e_test run, restore `scenario: "capital_accumulation"` with
the calibration data queries from the previous CONTEXT.md (Queries 3–7).
These require actual fitness scores and WFO data — only available after a
real (non-e2e_test) run with candidates passing Stage 1.
---
## Block 9G — Calibration Actions (done by Claude)
### B8B-012 — Sigmoid scale calibration (PRE-PROD BLOCKER)
Requires Query 3 (net_pnl distribution) from a capital_accumulation run.
File: consistency_scorer.py, function _sigmoid_normalise()
Current: scale=0.10
### B8B-003 — Expectancy normalisation ceiling
Requires Query 4 (expectancy distribution) from a capital_accumulation run.
File: fitness.py, function _compute_weighted_score()
Current: expectancy_norm = clamp(expectancy / 3.0, 0, 1)
### Constraint re-calibration
After first capital_accumulation run with passing candidates:
- Query actual win_rate distribution to set min_win_rate correctly
- Current temporary value: 0.15 (eased from 0.45 — run 1 showed max ~11.7%)
- Strategy is producing ~10% win rates — may indicate strategy config issue
  OR the parameter space (safe zone) doesn't include profitable configurations.
  If capital_accumulation run still shows 0 passing at 0.15 → investigate
  strategy data coverage and base YAML filter configuration.
---
## Open Findings
### Active
| ID | Priority | File | Description |
|---|---|---|---|
| B9F-001 | **P1 BLOCKER** | parameter_space.py | expand_zones() Cartesian product OOM for exploration zone. Workaround: exploration.enabled: false. Fix: refactor to per-param value lists. |
| B8B-012 | PRE-PROD | consistency_scorer.py | sigmoid scale=0.10 — calibrate after first real run |
| B8B-003 | P3 | fitness.py | expectancy /3.0 hardcoded — calibrate after first real run |
| B8-009 | P3 | orchestrator.py | raw sqlite3 in _resume_or_start |
| B9B-001 | P3 | crossover.py | no zone-name guard |
| B8B-013 | P3 | mc_engine.py | ruin_threshold dual-source |
| B8B-011 | P3 | consistency_scorer.py | fraction_positive_windows floor |
| B8C-002/003 | P3 | report_generator.py | deferred |
| B9C-008 | P3 | sampler.py | deferred |
### Resolved This Session (Block 9F)
| ID | Fix |
|---|---|
| B9F-002 ✅ | orchestrator.py Stage 3 graceful skip |
| B9F-003 ✅ | orchestrator.py Stage 2 re-evaluate instead of store reconstruct |
| B9F-004 ✅ | equity_simulator.py pnl_points attribute |
| B9F-005 ✅ | strategy_runner.py date_start/date_end for WFO window scoping |
---
## Contract Field Reference (verified — do NOT deviate)
```python
# CandidateStage enum values (exact):
CandidateStage.RANDOM
CandidateStage.MC_PREFILTER_PASS / MC_PREFILTER_FAIL
CandidateStage.GA / WFO / MC_DEEP / SENSITIVITY
# Trade contract (B9F-004 verified):
Trade.pnl_points        → Optional[float] (None if trade is open / no exit)
Trade.exit              → Optional[TradeExit]
TradeExit.pnl_points    → float
TradeResult.trades      → List[Trade]  (unwrap with trades.trades if TradeResult)
# strategy_runner.evaluate() full signature (B9F-005):
evaluate(candidate, base_yaml_path, temp_dir,
         min_significant_trades=30, retain_temp_yamls=False,
         date_start=None, date_end=None) -> CandidateResult
# date_start/date_end: date or datetime, optional
# date → "YYYY-MM-DD 00:00:00" (start) / "YYYY-MM-DD 23:59:59" (end)
# datetime → formatted as-is
# YAML date_range keys (B9F-005 verified from strategy_template.yaml):
data.date_range.start   "YYYY-MM-DD HH:MM:SS"
data.date_range.end     "YYYY-MM-DD HH:MM:SS"
# All other contracts unchanged from Block 9E — see previous CONTEXT.md
```
---
## Critical Non-Negotiables (unchanged + additions)
```python
# All Block 9E non-negotiables still apply (see previous CONTEXT.md)
# Additions from Block 9F:
# extract_trade_returns: use trade.pnl_points, skip None (open trades)
# Stage 2: re-evaluate via evaluate() — never reconstruct from store
# strategy_runner.evaluate(): date_start/date_end override data.date_range
# H-01 is FIXED (B9F-005) — not a false positive
```
---
## Block Roadmap
```
Block 9D (done):  Prerequisite fixes + Stages 1–3 implemented
Block 9E (done):  B8B-005 OOS gate
Block 9F (done):  B9F-002/003/004/005 bug fixes; pipeline reaches Stage 3
Block 9G (next):  1. Confirm clean e2e_test run (Stages 1–3 complete)
                  2. capital_accumulation run with calibration queries
                  3. Calibrate B8B-012 + B8B-003
                  4. Re-assess min_win_rate for capital_accumulation
                  5. B9F-001 fix (expand_zones refactor) if exploration zone needed
Block 9H (TBD):   Stage 4 Full WFO implementation
Block 9I (TBD):   OOS gate threshold calibration + enable
Production:       Stage 4 complete, B8B-012/003 calibrated, exploration zone enabled
```