# NEXT SESSION PLAN — Block 4: Robustness
## Status entering this session
- Phase 6, Blocks 0–3 complete. 199 tests green.
- Block 4 is the next task.
---
## Goal
Verify the pipeline survives interruption at every possible checkpoint and that
a single failing sensitivity worker does not abort the remaining candidates.
---
## Files to Upload at Session Start
Upload both before writing any code:
1. `src/backtesting/orchestrator.py` — to verify resume logic per checkpoint
2. `src/backtesting/evaluation/sensitivity.py` — to verify worker error handling
---
## Block 4 Test File
`tests/backtesting/integration/test_robustness.py`
---
## ROB Criteria (11 total)
### Resume-after-interruption (8 criteria — one per Checkpoint value)
For each checkpoint in `[NOT_STARTED, RUN_INITIALISED, RANDOM_SEARCH_COMPLETE,
MC_PREFILTER_COMPLETE, GA_COMPLETE, WFO_COMPLETE, MONTE_CARLO_COMPLETE,
SENSITIVITY_COMPLETE]`:
Seed the store with the state that checkpoint implies (e.g. `WFO_COMPLETE` →
inject WFO consistency scores). Set the checkpoint. Invoke the orchestrator.
Assert the pipeline completes without exception and reaches `COMPLETE`.
| ID | Checkpoint at interruption | Expected resumed stage |
|---|---|---|
| ROB-01 | NOT_STARTED | Stage 0 (re-init) |
| ROB-02 | RUN_INITIALISED | Stage 1 (random search — stub, skip to next) |
| ROB-03 | RANDOM_SEARCH_COMPLETE | Stage 2 (MC prefilter — stub) |
| ROB-04 | MC_PREFILTER_COMPLETE | Stage 3 (GA — stub) |
| ROB-05 | GA_COMPLETE | Stage 4 (full WFO — stub) |
| ROB-06 | WFO_COMPLETE | Stage 5 (MC Deep) |
| ROB-07 | MONTE_CARLO_COMPLETE | Stage 6 (Sensitivity) |
| ROB-08 | SENSITIVITY_COMPLETE | Stage 7 (Report) |
### Worker isolation (3 criteria)
| ID | Scenario | Expected outcome |
|---|---|---|
| ROB-09 | One sensitivity worker raises an unhandled exception | Remaining candidates complete; failing candidate has `sensitivity_profile_complete=False` |
| ROB-10 | All sensitivity workers fail | Stage 6 completes without exception; Stage 7 runs; report notes zero sensitivity profiles |
| ROB-11 | MC worker raises for one candidate | Remaining candidates complete; failing candidate has `mc_result.error` set |
---
## Fixture Design Notes
- Use `module` scope for the store + run setup; `function` scope per ROB test where
  interruption state differs.
- For ROB-06 through ROB-08: seed the store identically to the `perf_run` fixture
  in `test_performance.py` (20 `CandidateRecord` + `WFOConsistencyScore` rows).
- For ROB-09 / ROB-10: patch `_evaluate_perturbation` to raise on demand.
  ```python
  patch("src.backtesting.evaluation.sensitivity._evaluate_perturbation",
        side_effect=RuntimeError("injected worker failure"))
  ```
- For ROB-11: patch `src.backtesting.monte_carlo.mc_engine.run_mc` similarly.
- Stubs (Stages 1–4): orchestrator skips them and advances checkpoint automatically.
  Verify the skip path does not raise.
---
## Pass Criteria Summary
All 11 ROB criteria must pass. Informational `test_z_robustness_summary` (never fails)
prints the checkpoint-to-resume mapping and any worker-error counts observed.
---
## After Block 4 Passes
1. Update CONTEXT.md: Block 4 done, Block 5 next.
2. Append to CHANGE_LOG.md.
3. Write NEXT_SESSION_PLAN.md for Block 5 (threshold calibration).
4. Update PROJECT_SKILL.md test counts to 210.
---
## Block 5 Preview (if time allows)
Threshold calibration: after the first real Stages 1–4 run produces actual WFO
composite scores and MC ruin probabilities, validate that the
`verdict_go_wfo_floor`, `verdict_borderline_wfo_floor`,
`verdict_go_mc_ruin_ceiling`, `verdict_borderline_mc_ruin_ceiling` thresholds
in `backtest_template.yaml` produce an appropriate verdict distribution.
This is decision D-07 from TECHNICAL_SPEC.md.
---
## Performance Optimisation Opportunities (Block 7 — do NOT start yet)
Identified from Block 3 profiling. Context for planning:
```
OPT-01 [HIGH]: Pool reuse in evaluate_sensitivity() — 40–60% Stage 6 reduction
OPT-02 [MEDIUM]: Batch perturbations per worker task — further 15–25% reduction
OPT-03 [LOW]: sensitivity.input_count: 5 → 3 (YAML only, saves ~130–180s)
OPT-04 [NEGLIGIBLE]: Stage 5 needs no action at current scale
```