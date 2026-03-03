# CONTEXT.md — Backtesting & Optimization Framework
**Updated**: 2026-03-03 (end of Phase 6 Block 6 / start of Block 7)
---
## Current State
**Phase 6 complete. All 6 blocks done. 233 tests green.**
An independent audit report was received and fully analysed this session. Block 7 scope is expanded beyond the original OPT-01/02 plan to include audit remediation. Sub-block sequence and file upload order are in NEXT_SESSION_PLAN.md.
**Next action**: Block 7 — upload source files in the order listed below, then work through 7A → 7D.
---
## Phase 6 — What Was Completed (All Blocks)
| Block | Deliverable | Tests added |
|---|---|---|
| Block 0 | E2E test on real WBWS data | +13 (`test_e2e_wbws_real_data.py`) |
| Block 1 | `BACKTESTER_USER_GUIDE.md` | 0 |
| Block 2 | Adversarial suite — AV-02 no_go confirmed; AV-03 100% stable | +8 (`test_adversarial_suite.py`) |
| Block 3 | Performance baseline locked — Total=337s, Stage6=333s | +7 (`test_performance.py`) |
| Block 4 | Resume at all 8 checkpoints; worker isolation confirmed | +12 (`test_robustness.py`) |
| Block 5 | Verdict threshold calibration; boundary operators confirmed ≥/≤ | +22 (`test_threshold_calibration.py`) |
| Block 6 | Final documentation (6 docs updated + OPERATOR_RUNBOOK.md created) | 0 |
---
## This Session (Block 6) — Documents Produced
| Document | Version | Key changes |
|---|---|---|
| ARCHITECTURE.md | 1.2.0 | Section 3 stage counts from YAML; Section 8 capital_accumulation production verdict grid |
| TECHNICAL_SPEC.md | 1.1.0 | D-07 boundary operators confirmed (≥/≤ inclusive at go thresholds); Windows spawn patch constraint §1a |
| FUNCTIONAL_SPEC.md | 1.1.0 | Stage 5 never-raises; Stage 6 profile_complete=False path; Stage 7 ruin=None→NO_GO; Stage 0 resume coverage; e2e_test warning |
| BACKTESTER_PLAN.md | 1.3.0 | Phase 6 complete; §12 all decisions resolved; §15 Lessons Learned L-01–L-04 |
| PROJECT_REPORT.md | — | Phase 6 complete; 233 tests; Block 7 preview |
| OPERATOR_RUNBOOK.md | 1.0.0 | New — 8-section operator guide covering pre-run, monitoring, verdict, promotion, resume, tuning |
**Note**: SKILL.md was NOT updated during Block 4 and Block 5 sessions. It still reads "199 tests, Block 4 next, Stages 1–4 stubs." SKILL.md update is the **first task of sub-block 7A**.
---
## Audit Analysis Summary (Backtesting_Framework_Audit_Report.md, 2026-03-03)
### HIGH Findings
| ID | Finding | Verdict | Rationale |
|---|---|---|---|
| H-01 | `strategy_runner.evaluate()` missing `date_start`/`date_end` | **FALSE POSITIVE** | SKILL.md (Block 3 state): "Accepts date_start/date_end." Audit read TECHNICAL_SPEC simplified signature, not source. |
| H-02 | `CandidateStore` missing `write_wfo_window_result` / `flag_candidate_wfo_insufficient` | **UNRESOLVED** | Absent from SKILL.md store API list. Audit quotes specific line numbers in wfo_engine.py. SKILL.md may just be stale on this point. **First source file to upload in Block 7.** If missing, HIGH priority fix before other work. |
| H-03 | WFO date range not passed to strategy runner | **LIKELY FALSE POSITIVE** | Contingent on H-01. `wfo_evaluator.py` receives `WFOWindow`; if it passes `window.start_date`/`end_date` to `strategy_runner.evaluate()`, finding is resolved. Confirm by uploading `wfo_evaluator.py`. |
### MEDIUM Findings — All Accepted, Prioritised
| ID | Finding | P | Action in Block 7 |
|---|---|---|---|
| M-05 | No Stage 0 param name validation vs `_PARAM_KEY_MAP` | 1 | Add `_validate_parameter_names()` to orchestrator Stage 0 |
| M-04 | MC zero-equity drawdown understatement | 2 | Fix numpy path in `mc_metrics.py` |
| M-03 | Hardcoded WFO collapse threshold (0.40) | 2 | Add to `ScenarioProfile`, update `consistency_scorer.py` |
| M-02 | Hardcoded fitness normalisation constants | 2 | Add to `ScenarioProfile`, update `fitness.py` |
| M-01 | `median_oos_delta` always None | 3 | Compute in `consistency_scorer.py`, propagate to `VerdictResult` |
| M-06 | Hardcoded mutation std dev (2 steps) | 3 | Add `mutation_std_steps` to YAML + `mutation.py` |
| M-07 | Hardcoded chart dimensions | 4 | Make configurable or use responsive sizing |
### LOW/Evolution (E-01–E-11) — All accepted as future roadmap. No v1 action.
### I-07 — `datetime.utcnow()` in Phase 2/3 modules — fix in Block 7 sub-block 7A.
---
## Completeness Check Against BACKTESTER_PLAN
All Must-Have requirements confirmed implemented. Two Should-Have items with uncertain implementation status — verify by uploading source:
| Item | Requirement | Status |
|---|---|---|
| WF-07 | `parameter_region_width` actually computed | Uncertain — field in contract, may always be None |
| WF-09 | Post-Stage-1 statistical adequacy warning | Uncertain — not mentioned in SKILL.md or test list |
---
## Performance Baseline (LOCKED — Block 3)
```
Windows 10, 6 workers, 3-month WBWS data slice
Run 2 (canonical): Total=337.2s  Stage5=0.3s  Stage6=332.6s  Stage7=4.4s
Daily budget: 14,400s → 2.3% consumed
Stage 6 dominates (98.7%). Root cause: Windows spawn mode per-worker pool startup.
OPT-01 target: Stage 6 ≤ 200s via pool reuse across candidates.
```
---
## Test Inventory
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
| **Total** | **233** | | **✅** |
---
## Files to Upload at Start of Block 7 (in order)
| # | File | Why first |
|---|---|---|
| 1 | `src/backtesting/candidate_store.py` | H-02 verification — does `write_wfo_window_result` exist? |
| 2 | `src/backtesting/wfo/wfo_evaluator.py` | H-03 verification — does it pass window dates? |
| 3 | `src/backtesting/evaluation/sensitivity.py` | OPT-01/02 implementation target |
| 4 | `tests/backtesting/integration/test_performance.py` | Regression guard — run before/after OPT |
| 5 | `src/backtesting/fitness.py` | M-02 normalisation |
| 6 | `src/backtesting/wfo/consistency_scorer.py` | M-03 collapse threshold |
| 7 | `src/backtesting/monte_carlo/mc_metrics.py` | M-04 zero-equity drawdown |
| 8 | `src/backtesting/orchestrator.py` | WF-07/WF-09 verification; M-05 Stage 0 validation |
---
## Architecture Constraints (Non-Negotiable)

```python
# Contracts: frozen dataclasses — never raw dicts between modules
# CandidateParameterSet.create()  — always use factory, never construct directly
# strategy_runner, run_mc, evaluate_sensitivity — never raise to caller
# datetime.now(timezone.utc)      — never datetime.utcnow() (deprecated)
# pathlib.Path + src/utils/paths.py — never hardcoded separators
# ProcessPoolExecutor spawn mode  — no fork-dependent code
# LIVE_APPROVED                   — never set in code, operator-only
# store.close()                   — always in finally block
# mode_override="core"            — not mode="core"
# Stage 6 integration test patch  — patch at orchestrator boundary, not inside worker
# e2e_test scenario               — never for production optimization runs
```