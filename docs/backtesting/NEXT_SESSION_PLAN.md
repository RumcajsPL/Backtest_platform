# NEXT_SESSION_PLAN.md — Phase 6 Continuation
**Prepared**: 2026-03-02
**Session goal**: Block 3 — Performance validation. Measure full-pipeline wall-clock time,
profile any bottleneck, tune within 4-hour budget.

---
## How to Start
1. Open new chat, paste `CONTEXT.md` as first message
2. Add: *"Phase 6 in progress, follow NEXT_SESSION_PLAN.md"*
3. **Upload these files** (Claude needs to read them to plan timing instrumentation):
   - `src/backtesting/orchestrator.py`  ← stage entry/exit points for timing hooks
   - `configs/backtesting/backtest_template.yaml`  ← production sample counts and MC params
4. Confirm skill read, CONTEXT.md understood

---
## DONE: Blocks 0–2
```
Block 0: E2E real data test       — 13/13 green
Block 1: Parameter mapping audit  — _PARAM_KEY_MAP frozen V1
Block 2: Adversarial suite        — 8/8 green
          AV-02: overfit → no_go ✅
          AV-03: 5/5 positions stable at 100% across seeds [42, 137, 9871] ✅
          Total pipeline time for 5 candidates (smoke): 769s (12m49s)
```

---
## START FROM HERE: Block 3 — Performance Validation

### Goal
A full production-config pipeline run (all 8 stages, real data, realistic candidate counts)
must complete within a **4-hour (14,400s)** wall-clock budget on the operator's hardware.
If over budget, profile bottlenecks and tune until within budget.

### Critical context: what "full pipeline" means for Block 3
Stages 1–4 are still stubs (pass-through, near-zero cost). The real cost is in:
- **Stage 5 MC Deep** — main cost centre: N candidates × iterations × perturbation types × workers
- **Stage 6 Sensitivity** — second cost centre: N candidates × parameters × steps × workers
- **Stage 7 Report** — third cost centre: HTML render + JSON + Parquet write

Block 3 exercises Stages 5–7 with production candidate counts and production iteration counts.

### Known baseline from Block 2
```
5 candidates, SMOKE_MC_ITERATIONS=50, sensitivity_steps=1 → 769s (12m49s total)
Per-candidate average (smoke):  769 / 5 = ~154s
```
Use this to estimate production time before running (Step 3).

---
### Step 1 — Read orchestrator.py: confirm stage timing hook locations
Claude reads `orchestrator.py` and identifies:
- Where each stage (_run_stage_5, _run_stage_6, _run_stage_7) is called
- Whether `time.perf_counter()` or equivalent timing already wraps stage calls
- Whether a timing summary is already logged at end of run

If timing hooks are absent → add them (Step 4). If present → confirm format and proceed.

---
### Step 2 — Read backtest_template.yaml: extract production tuning levers
Extract and document:
```
monte_carlo.deep.iterations    → [value]    # MC paths per candidate
monte_carlo.deep.input_count   → [value]    # max candidates entering Stage 5
sensitivity.input_count        → [value]    # max candidates entering Stage 6
sensitivity.max_steps          → [value]    # 1 = ±1 only, 2 = ±1 and ±2
run.max_workers                → [value]    # parallel ProcessPoolExecutor workers
```
These are the tuning levers for Step 7 if the budget is exceeded.

---
### Step 3 — Estimate full-run time BEFORE running
Using the smoke baseline and production values from Step 2:

```
# Rough estimate — use to decide if a full run is safe to launch
estimated_stage5_s = (prod_candidates / 5) × (prod_mc_iters / 50) × stage5_smoke_s
estimated_stage6_s = (prod_candidates / 5) × (prod_steps / 1) × stage6_smoke_s
estimated_total_s  = estimated_stage5_s + estimated_stage6_s + ~30s (Stage 7)
```

If estimated total > 14400s → tune YAML first (Step 7) before launching the test.
Document the estimate in the session log.

---
### Step 4 — Add per-stage timing instrumentation to orchestrator.py
If not already present, add `time.perf_counter()` timing around each stage call and
log a summary at the end of the run. Minimal, non-breaking addition:

```python
import time

# In the main run body, around each stage call:
_t5 = time.perf_counter()
_run_stage_5_mc_deep(config, store, run_metadata)
_elapsed_5 = time.perf_counter() - _t5
logger.info("TIMING stage_5_mc_deep elapsed=%.1fs", _elapsed_5)

_t6 = time.perf_counter()
_run_stage_6_sensitivity(config, store, run_metadata)
_elapsed_6 = time.perf_counter() - _t6
logger.info("TIMING stage_6_sensitivity elapsed=%.1fs", _elapsed_6)

_t7 = time.perf_counter()
_run_stage_7_report(config, store, run_metadata)
_elapsed_7 = time.perf_counter() - _t7
logger.info("TIMING stage_7_report elapsed=%.1fs", _elapsed_7)

logger.info(
    "TIMING SUMMARY  stage5=%.1fs  stage6=%.1fs  stage7=%.1fs  total=%.1fs  budget=14400s  %s",
    _elapsed_5, _elapsed_6, _elapsed_7,
    _elapsed_5 + _elapsed_6 + _elapsed_7,
    "PASS" if (_elapsed_5 + _elapsed_6 + _elapsed_7) <= 14400 else "OVER BUDGET",
)
```

Architecture rule: use `logger.info`, never `print`. No debug flags. No commented-out blocks.

---
### Step 5 — Write test_performance.py
New file: `tests/backtesting/integration/test_performance.py`

**Fixture design** (`perf_run`, module-scoped):
- `PERF_N_CANDIDATES = 20` — realistic Stage 5/6 load without requiring Stages 1–4
- Same injection pattern as E2E test: evaluate real candidates, inject WFO scores,
  set checkpoint to WFO_COMPLETE, run Stages 5–7
- **No smoke overrides** — use production `monte_carlo.deep.iterations` and `sensitivity.max_steps`
  directly from `backtest_template.yaml`
- Capture per-stage elapsed times via `time.perf_counter()` in the fixture
- Set `config["scenario"] = "e2e_test"` (loose constraints — same as E2E test)

**Pass criteria**:
```
PERF-01  Pipeline completes without exception
PERF-02  Total elapsed ≤ 14400s (4-hour hard budget)
PERF-03  Per-candidate Stage 5 average ≤ 300s (MC Deep sanity bound)
PERF-04  Per-candidate Stage 6 average ≤ 120s (Sensitivity sanity bound)
PERF-05  Stage 7 (report generation) ≤ 60s total
PERF-06  No single stage consumes > 85% of total elapsed (balanced pipeline)
```

**Informational summary** (like test_z_summary in E2E test, never fails):
```
PERFORMANCE SUMMARY — run_id=…
  Candidates (WFO survivors)  : N
  Stage 5 MC Deep             : Xs  (Xs/candidate avg)
  Stage 6 Sensitivity         : Xs  (Xs/candidate avg)
  Stage 7 Report + Output     : Xs
  Total                       : Xs
  Budget                      : 14400s
  Status                      : PASS / OVER BUDGET
  Bottleneck                  : Stage X (Y% of total)
```

---
### Step 6 — Run and record
```
pytest tests/backtesting/integration/test_performance.py -v -s --tb=short
```
Record the per-stage times from the summary output. These become the Block 3 baseline
and inform Block 5 threshold calibration.

---
### Step 7 — If PERF-02 fails: tune using these levers (in order of preference)
| Lever | Effect | Minimum safe value |
|---|---|---|
| `monte_carlo.deep.iterations` ↓ | Linear Stage 5 reduction | 200 (below → ruin estimate too noisy) |
| `monte_carlo.deep.input_count` ↓ | Fewer Stage 5 candidates | Keep ≥ 5 |
| `sensitivity.max_steps` 2→1 | Halves Stage 6 cost | 1 (±1 step only) |
| `sensitivity.input_count` ↓ | Fewer Stage 6 candidates | Keep ≥ 3 |
| `run.max_workers` ↑ | More parallel workers | Diminishing returns above CPU count |

After tuning, update `backtest_template.yaml` with new values + comment explaining
the budget rationale. Re-run until PERF-02 passes.

---
### Step 8 — Close Block 3
Block 3 is closed when:
- [ ] `test_performance.py` passes all 6 criteria including PERF-02
- [ ] Per-stage timing is logged and matches the informational summary
- [ ] `backtest_template.yaml` reflects the tuned production values with budget comments
- [ ] Session-end documents updated (CONTEXT.md, CHANGE_LOG, NEXT_SESSION_PLAN, SKILL)

---
## After Block 3 is closed: Block 4 — Robustness

### Block 4 goal
Validate that the pipeline survives interruption at any checkpoint and that parallel
worker failures are isolated (one crash does not abort or corrupt the run).

### Block 4 tasks
1. **Resume-after-interruption** at each of the 8 `Checkpoint` enum values:
   - For each checkpoint C: set store checkpoint to C, call orchestrator with resume=True,
     verify pipeline resumes from the correct stage
   - Verify no stage is re-executed if already completed
   - Verify DB has no duplicate rows after resume
2. **Parallel worker isolation**:
   - Inject one candidate that causes `_evaluate_perturbation` to raise an exception
   - Verify remaining candidates complete normally
   - Verify the failing candidate gets `sensitivity_profile_complete=False`
3. Write `tests/backtesting/integration/test_robustness.py`

**Files to upload for Block 4**:
- `src/backtesting/orchestrator.py` (if not already uploaded this session)
- `src/backtesting/evaluation/sensitivity.py` (worker isolation test needs this)

---
## Output Documents This Session
| Document | Action |
|---|---|
| `src/backtesting/orchestrator.py` | Add per-stage timing if not present |
| `configs/backtesting/backtest_template.yaml` | Update production counts if tuning needed |
| `tests/backtesting/integration/test_performance.py` | Create + all criteria green |
| `docs/backtesting/CHANGE_LOG.md` | Append SESSION 10 block |
| `docs/backtesting/CONTEXT.md` | Update phase status + Block 3 timing results |
| `docs/backtesting/NEXT_SESSION_PLAN.md` | Update for Block 4 |
| `docs/backtesting/PROJECT_SKILL.md` | Update test counts, Block 3 status |