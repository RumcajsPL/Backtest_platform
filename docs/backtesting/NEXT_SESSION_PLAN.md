# NEXT_SESSION_PLAN.md — Block 7
**Entering state**: Phase 6 complete. 233 tests green. Independent audit reviewed.
**Block 7 scope**: Audit remediation (H-02 verification + M-series) + OPT-01/02 performance + SKILL.md update.
---
## Upload Order — Do This First
Read each file before acting on it. Do not write any code until all verification files are read.
| # | File | Purpose |
|---|---|---|
| 1 | `src/backtesting/candidate_store.py` | **H-02 verdict** — does `write_wfo_window_result` exist? |
| 2 | `src/backtesting/wfo/wfo_evaluator.py` | **H-03 verdict** — does it pass `window.start_date`/`end_date`? |
| 3 | `src/backtesting/evaluation/sensitivity.py` | OPT-01/02 implementation target |
| 4 | `tests/backtesting/integration/test_performance.py` | Baseline regression guard |
| 5 | `src/backtesting/fitness.py` | M-02 normalisation constants |
| 6 | `src/backtesting/wfo/consistency_scorer.py` | M-03 collapse threshold |
| 7 | `src/backtesting/monte_carlo/mc_metrics.py` | M-04 zero-equity path |
| 8 | `src/backtesting/orchestrator.py` | WF-07/WF-09 status; M-05 Stage 0 |
---

## Audit Report — Authoritative Finding Dispositions
These verdicts are fixed. Do not re-litigate them.
### H-01 — FALSE POSITIVE (no action)
`strategy_runner.evaluate()` does accept `date_start`/`date_end`. Audit read the simplified TECHNICAL_SPEC signature, not the source. SKILL.md (Block 3) explicitly records: *"Accepts date_start/date_end."*
### H-02 — VERIFY FIRST, then act
SKILL.md's store write API list does not include `write_wfo_window_result` or `flag_candidate_wfo_insufficient`. The audit quotes specific call sites in `wfo_engine.py` (lines 98, 142, 181). Two outcomes:
**If methods exist in `candidate_store.py`**: H-02 is a SKILL.md documentation gap. Update SKILL.md store API section. No code change needed.
**If methods are missing**: H-02 is a real bug. Add them before any other Block 7 work:
```python
def write_wfo_window_result(self, result: WFOWindowResult, run_id: str) -> None:
    """Enqueue a WFOWindowResult write. Non-blocking."""
    self._queue.put(("_write_wfo_window_result", (result, run_id)))

def flag_candidate_wfo_insufficient(self, candidate_id: str, run_id: str) -> None:
    """Mark candidate as WFO_INSUFFICIENT_WINDOWS."""
    self._queue.put(("_flag_wfo_insufficient", (candidate_id, run_id)))
```
Also implement the writer thread handlers. Write 2 tests. Run full 233 suite.
### H-03 — LIKELY FALSE POSITIVE (confirm from wfo_evaluator.py)
If `wfo_evaluator.evaluate_window` passes `window.start_date` and `window.end_date` to `strategy_runner.evaluate()`, H-03 is resolved. Document in SKILL.md if confirmed.
---
## Sub-Block Structure
Run sub-blocks in order. All 233 tests must remain green after each sub-block.
---
### Sub-Block 7A — SKILL.md Update + Verification + datetime Fix
**Objective**: Bring SKILL.md current, confirm H-02/H-03, fix outstanding deferred item.
**Steps**:
1. **Update SKILL.md** — this is the first code action. The file is stale (last updated Block 3). Updates needed:
   - Status line: "Phase 6 complete. 233 tests green. Block 7 in progress."
   - Remove "Stages 1–4 are currently stubs" — all stages fully wired
   - Test count: 199 → 233, add test_robustness.py and test_threshold_calibration.py rows
   - Add Block 4 lessons: spawn patch boundary note (currently shows wrong patch target for Stage 6 — fix to match TECHNICAL_SPEC §1a)
   - Performance block: add OPT-01–05 with expected gains
   - Add Block 7 plan section
   - Add L-01 through L-04 lessons
2. **Verify H-02** from `candidate_store.py` — act per outcome above
3. **Verify H-03** from `wfo_evaluator.py` — document result
4. **Verify WF-07** from `orchestrator.py` Stage 7 — is `parameter_region_width` computed or always None?
5. **Verify WF-09** from `orchestrator.py` Stage 1 — does post-Stage-1 adequacy warning log exist?
6. **Fix `datetime.utcnow()`** — grep all Phase 2/3 modules, replace with `datetime.now(timezone.utc)`. No new tests needed — 233 suite catches regressions.
**Pass criteria**: 233 tests green. SKILL.md current. H-02/H-03 resolved.
---
### Sub-Block 7B — Audit M-Series P1/P2 Fixes
**Objective**: Fix the four accepted medium findings that affect metric quality or fast-fail behaviour.
**M-05 — Stage 0 parameter name validation** (P1 — do first, cheapest, highest payoff)
Add to `orchestrator._run_stage_0_init()`:
```python
def _validate_parameter_names(config: dict) -> None:
    from src.backtesting.strategy_runner import _PARAM_KEY_MAP
    enabled_params = {
        p
        for zone in config["zones"].values()
        if zone.get("enabled", True)
        for p in zone["parameters"]
    }
    unknown = enabled_params - set(_PARAM_KEY_MAP.keys())
    if unknown:
        raise ValueError(f"Zone parameters not in _PARAM_KEY_MAP: {sorted(unknown)}")
```
Tests: 2 — one passing (valid params), one failing (unknown param name → ValueError at Stage 0).
**M-04 — MC zero-equity drawdown** (P2)
In `mc_metrics.py`, after computing `worst_drawdown_per_path`:
```python
# Paths that hit ruin should report drawdown = 1.0, not an underestimated value
ruined_paths = path_minimums <= ruin_floor
worst_drawdown_per_path[ruined_paths] = 1.0
```
Test: 1 — path that hits zero equity reports `worst_drawdown_across_paths = 1.0`.
**M-03 — Scenario-configurable WFO collapse threshold** (P2)
Add `wfo_collapse_drawdown_threshold: float` to `ScenarioProfile` contract (default 0.40 preserves current behaviour). Update all scenario YAML definitions. Update `consistency_scorer.py` to read from scenario instead of hardcoded constant.
Tests: 2 — verify conservative scenario (threshold 0.20) flags a collapse that capital_accumulation (0.40) does not.
Contract change: `ScenarioProfile` gains one new field. Update TECHNICAL_SPEC.md contracts section.
**M-02 — Scenario-configurable fitness normalisation** (P2)
Add three normalisation fields to `ScenarioProfile`:
- `normalisation_drawdown_ref_points: float` (default 10000.0)
- `normalisation_pnl_ref_points: float` (default 5000.0)
- `normalisation_freq_ref_trades_per_week: float` (default 20.0)
Update all scenario YAML definitions with defaults that reproduce current fitness behaviour. Update `fitness.py` to read from `ScenarioProfile` instead of module-level constants.
Tests: 2 — verify normalisation values flow from scenario into fitness score computation.
Contract change: `ScenarioProfile` gains three new fields. Update TECHNICAL_SPEC.md.
**Pass criteria**: All 233 + new tests green (~7 new tests). Performance baseline unchanged — run `test_performance.py` after.
---
### Sub-Block 7C — OPT-01 + OPT-02: Pool Reuse
**Objective**: Reduce Stage 6 (Sensitivity) runtime from ~333s to ≤200s.
**Pre-condition**: Run `test_performance.py` and record baseline before touching `sensitivity.py`. The Block 3 canonical (Stage6=332.6s) must be reproducible before OPT work begins.
**OPT-01 — Pool reuse across candidates**
Current pattern (one pool per candidate — pays spawn cost N=5 times):
```python
for candidate in candidates:
    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_evaluate_perturbation, ...) for p in perturbations]
        results = [f.result() for f in futures]
```
Target pattern (one pool for all candidates — pays spawn cost once):
```python
with ProcessPoolExecutor(max_workers=max_workers) as pool:
    for candidate in candidates:
        futures = [pool.submit(_evaluate_perturbation, ...) for p in perturbations]
        results = [f.result() for f in futures]
        # collect all results before moving to next candidate
        # store profile, then continue loop
```
Key constraint: results for candidate N must be fully collected before candidate N+1's perturbations are submitted. The pool stays warm across candidates but each candidate's batch is synchronous within the loop.
**OPT-02 — Batch all perturbations per candidate into a single task** (implement only if OPT-01 alone doesn't reach ≤200s)
Instead of one `pool.submit()` per perturbation (N_params × 4 futures), submit all perturbations for a candidate as a single task returning a list of `ParameterSensitivity`. Reduces future creation overhead and result collection loop.
**OPT-05 — Clean up max_workers parameter** (after OPT-01)
If `evaluate_sensitivity()` accepted `max_workers` as a kwarg only to pass into the pool construction, and the pool is now shared/managed at a higher level, clean up the signature.
**Test procedure**:
1. Run `test_performance.py` → record pre-OPT Stage 6 time
2. Implement OPT-01
3. Run `test_performance.py` → Stage 6 must be ≤ 200s (40% reduction)
4. If still > 200s, implement OPT-02
5. Run full 233 suite — all green
6. Update performance block in SKILL.md and PROJECT_REPORT.md with new Stage 6 time
**Pass criteria**: Stage 6 ≤ 200s. All 233 tests still green.
---
### Sub-Block 7D — Remaining M-Series + WF-07/WF-09 + Documentation
**Objective**: Close remaining accepted audit findings and update all session documents.
**WF-07 — parameter_region_width** (from 7A verification)
If always None: document explicitly in FUNCTIONAL_SPEC.md §7 as "informational field, not yet computed — deferred to Block 8 or first real run analysis."
If already computed: confirm correct, document in SKILL.md.
**WF-09 — Post-Stage-1 adequacy warning** (from 7A verification)
If absent: implement as a single `logger.warning()` call after Stage 1 completes, examining average trade count vs. configured MC iterations. Log only, no gate. No new test required.
If present: confirm and document.
**M-01 — `median_oos_delta` population** (P3)
`WFOWindowResult.oos_delta` already exists per window. Compute median in `consistency_scorer.py`:
- Add `median_oos_delta: Optional[float]` to `WFOConsistencyScore` contract
- Compute as `np.median([r.oos_delta for r in valid_results if r.oos_delta is not None])` or `None` if no values
- Propagate to `VerdictResult.median_oos_delta` in `verdict.py`
Test: 1 — verify median is computed correctly from window results.
Contract change: `WFOConsistencyScore` gains one field. Update TECHNICAL_SPEC.md.
**M-06 — Configurable mutation std dev** (P3)
Add to YAML:
```yaml
genetic:
  mutation_std_steps: 2.0   # Standard deviation in steps — default preserves current behaviour
```
Update `mutation.py` to read from config dict passed to GA engine.
Test: 1 — verify different `mutation_std_steps` values produce different mutation amplitude distributions.
**M-07 — Chart dimensions** (P4, only if time permits)
Extract `figsize` constants from `report_generator.py` to an `output.chart_size` YAML key or use relative sizing. No test required — visual only.
**Documentation updates**:
- TECHNICAL_SPEC.md — update `ScenarioProfile` contract (M-02, M-03 new fields), `WFOConsistencyScore` contract (M-01)
- FUNCTIONAL_SPEC.md — update WF-07/WF-09 status, Stage 0 to mention param name validation
- ARCHITECTURE.md — no changes expected
- SKILL.md — finalise with Block 7 completion state, new test count
- PROJECT_REPORT.md — add Block 7 complete, updated test count
- CHANGE_LOG.md appendix, CONTEXT.md, NEXT_SESSION_PLAN.md for Block 8
---
## Sub-Block Priority Summary
| Sub-Block | Must complete? | Estimated effort |
|---|---|---|
| 7A — Verification + SKILL.md + datetime | ✅ Yes — unblocks everything | ~1h |
| 7B — Audit M P1/P2 | ✅ Yes — quality + correctness | ~2–3h |
| 7C — OPT-01/02 | ✅ Yes — performance goal | ~2–3h |
| 7D — Remaining M + docs | 🔵 Best-effort | ~1–2h |
**Order matters**: 7A → 7B → 7C → 7D. If time is constrained, 7C (performance) and 7B are independent after 7A completes — do whichever is more urgent.
---
## Block 7 Pass Criteria
- All 233 existing tests still green after every sub-block
- H-02 resolved (verified from source or fixed)
- H-03 resolved (verified from source)
- SKILL.md updated to current state
- `datetime.utcnow()` removed from all production modules
- Stage 6 runtime ≤ 200s (OPT-01 target)
- All P1 and P2 M-series items implemented
- Session documents complete: CONTEXT.md, CHANGE_LOG.md appendix, NEXT_SESSION_PLAN.md for Block 8, SKILL.md
---
## After Block 7 — Block 8 Preview (First Real Production Run)
Upload full-date-range OHLCV data. Run full pipeline with `capital_accumulation` scenario. Objectives:
- Extend WFO windows to cover full data slice (currently calibrated for 3-month test slice)
- Observe real WFO score and ruin probability distributions
- Calibrate D-07 verdict thresholds based on first-run observations
- Document calibrated values in `backtest_template.yaml` as production defaults (replacing Block 5 starting values)
- Paper trading setup for any AUTO_GO candidates