# PROJECT CONTEXT — Backtesting & Optimization Framework
## Identity
**Project**: Backtesting & Optimization Framework for WBWSStrategy
**Operator**: Single quantitative retail trader, Windows 10, eToro broker
**Stage**: Phase 6 in progress — Block 3 complete (7/7 green). Start Block 4.
**Last session ended**: 2026-03-03 — Block 3 closed. All 7 PERF criteria green. 199 total tests.
---
## Non-Negotiables (Architecture — never override)
1. **Contracts are the interface** — frozen dataclasses between every module. No raw dicts.
2. **Single responsibility** — one module, one concern. Orchestrator orchestrates only.
3. **Fail fast** — invalid config raises at construction. No silent fallbacks.
4. **Single source of truth** — all config from `backtest_template.yaml`. No module self-loads config.
5. **Immutability** — `frozen=True` on all contracts.
6. **Windows compatibility** — `pathlib.Path`, `ProcessPoolExecutor` spawn mode, explicit `utf-8`.
7. **Code hygiene** — no print statements, no debug flags, no MagicMocks in production.
8. **CacheManager** — reuse existing. `clear_all_caches()` between runs.
9. **Immutable run artifacts** — config hash, all seeds, perturbation profile name stored immutably.
---
## Project Reference Files
| File | Purpose | Location |
|---|---|---|
| `BACKTESTER_PLAN.md` | Master requirements v1.2 | `docs/backtesting/` |
| `FUNCTIONAL_SPEC.md` | Plain-language 8-stage spec | `docs/backtesting/` |
| `TECHNICAL_SPEC.md` | Contracts, decisions, module signatures, YAML schema | `docs/backtesting/` |
| `SQLITE_SCHEMA.md` | 9 tables, CREATE TABLE, indexes, 10 query examples | `docs/backtesting/` |
| `CHANGE_LOG.md` | All changes + session handoff blocks | `docs/backtesting/` |
| `PROJECT_REPORT.md` | Phase progress tracker | `docs/backtesting/` |
| `ARCHITECTURE.md` | Backtester architecture | `docs/backtesting/` |
| `backtest_template.yaml` | Backtester config template | `configs/backtesting/` |
---
## Pipeline (DO NOT REORDER)
```
Stage 0: Validation & Init
Stage 1: Random Search         (LHS, significance guard, constraint filter)
Stage 2: MC Pre-Filter         (cheap — 2 perturbation types, ruin screen)
Stage 3: GA                    (WFO-aware: random 2 windows/generation + diversity penalty)
Stage 4: Full WFO              (all windows, 4-metric composite consistency score)
Stage 5: MC Deep               (full iterations, all perturbation types, WFO survivors only)
Stage 6: Parameter Sensitivity (±1/±2 step, fitness delta map, spike = borderline)
Stage 7: Report & Output       (HTML + checklist + JSON/Parquet + SQLite + YAML)
```
Stages 1–4 are stubs. E2E and performance tests seed the store directly and set
checkpoint to WFO_COMPLETE to exercise Stages 5–7.
---
## Test Counts
| Scope | Tests | Status |
|---|---|---|
| Phase 2–4 cumulative | 123 | ✅ All green |
| test_live_pipeline.py | 17 | ✅ All green |
| test_sqlite_queries.py | 12 | ✅ All green |
| test_report_yaml.py | 19 | ✅ All green |
| test_e2e_wbws_real_data.py | 13 | ✅ All green |
| test_adversarial_suite.py | 8 | ✅ All green |
| test_performance.py | 7 | ✅ All green |
| **Total green** | **199** | ✅ |
---
## Current Phase Status
```
PHASE:      Phase 6 — Hardening & Delivery
COMPLETED:  Blocks 0, 1, 2, 3
NEXT:       Block 4 — Robustness
```
---
## Block 3 Performance Baseline (LOCKED — 2026-03-03)
```
Hardware:  Windows 10, 6 workers
Config:    mc.deep.iterations=3000, mc.deep.input_count=10,
           sens.input_count=5, sens.max_steps=2, max_workers=6
Run 1:  Total=457.2s  Stage5=2.5s   Stage6=446.3s  Stage7=8.3s
Run 2:  Total=337.2s  Stage5=0.3s   Stage6=332.6s  Stage7=4.4s
        (Run 2 faster — warm pool / OS cache effects)
Key findings:
  Stage 5 MC Deep:     0.3–2.5s for 10 cands × 3000 iters
                       Fully vectorised (np.cumsum). NEVER the bottleneck.
  Stage 6 Sensitivity: 333–446s for 5 cands, 66–89s/cand avg
                       Structural bottleneck. Windows ProcessPoolExecutor
                       spawn mode pays per-worker startup cost per candidate.
                       5 cands × 9 params × 4 perturbations ≈ 180 evals.
  Stage 7 Report:      4–8s. Fine.
Budget consumed:  337–457s of 14,400s (2.3–3.2%) ✅
PERF-06 ceiling:  99% — Stage 6 dominance is expected (not a bug).
```
---
## Performance Optimisation Opportunities
Not blocking delivery. Planned for Block 7.
```
OPT-01  [HIGH IMPACT]  Pool reuse across candidates in Stage 6
        ProcessPoolExecutor created fresh per candidate in evaluate_sensitivity().
        On Windows spawn, each creation pays startup overhead × 6 workers.
        Fix: create pool ONCE for the Stage 6 run, pass it in.
        Expected: 40–60% reduction in Stage 6 elapsed.
        File: src/backtesting/evaluation/sensitivity.py
OPT-02  [MEDIUM IMPACT]  Batch perturbations per worker task
        Each perturbation is a separate dispatch → fine-grained IPC on Windows.
        Fix: one worker task processes ALL perturbations for one candidate.
        File: src/backtesting/evaluation/sensitivity.py
OPT-03  [LOW IMPACT, trivial]  sensitivity.input_count: 3 (down from 5)
        Each candidate costs 66–89s. Top 3 by WFO score are all that matters.
        Saves ~130–180s. YAML-only change. Risk: none.
OPT-04  [NEGLIGIBLE]  Stage 5 at current scale
        <3s for 3000 iters on 10 candidates. No action until input_count > 50.
```
---
## Key Files Modified This Session (2026-03-03)
| File | Change |
|---|---|
| `src/backtesting/orchestrator.py` | Added `time.perf_counter()` per-stage timing |
| `tests/backtesting/integration/test_performance.py` | Created — 7/7 PERF criteria green |
| `src/backtesting/contracts.py` | Restored after accidental corruption (content unchanged) |
---
## Adversarial Suite Results (Block 2 — locked)
```
AV-02: overfit candidate → no_go. Two-pillar rejection confirmed.
AV-03: 5/5 positions stable (100%) across seeds [42, 137, 9871]. All: no_go.
```
---
## Critical Patch Targets
```python
# run_mc is a LOCAL import inside _run_stage_5_mc_deep — NOT on orchestrator namespace
patch("src.backtesting.monte_carlo.mc_engine.run_mc", ...)          # CORRECT
patch("src.backtesting.orchestrator.run_mc", ...)                   # WRONG — AttributeError
patch("src.backtesting.evaluation.sensitivity._evaluate_perturbation", ...)  # worker patch
```
---
## Test Import Convention (CRITICAL)
Violating this causes circular import errors at pytest collection time.
```python
# 1. sys.path FIRST
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
## CandidateStore Write API (critical for test fixtures)
```python
store.write_candidate(record: CandidateRecord)   # ONE arg — fitness embedded in record
store.query_mc_results(run_id, "deep")           # mode is str not MCMode enum
# There is NO write_fitness_result() method
```
---
## Platform Notes
- Windows 10, Python 3.13.12
- `pathlib.Path`, spawn mode, `utf-8` explicit everywhere
- `strategy_runner.run()` kwarg: `mode_override="core"` NOT `mode="core"`
- Timestamps: pipeline UTC, OHLCV/signals CET/CEST
- DB: `data/db/backtest.db` (prod), `tmp_path` in tests
---
## Phase 6 Blocks
```
Block 0 (done):  E2E real data — 13/13 ✅
Block 1 (done):  _PARAM_KEY_MAP audit — frozen V1 ✅
Block 2 (done):  Adversarial suite — 8/8 ✅
Block 3 (done):  Performance — 7/7 ✅
                 Baseline locked. OPT-01–04 identified. Block 7 planned.
Block 4 (NEXT):  Robustness
                 Resume-after-interruption at each of 8 Checkpoint values.
                 Worker isolation: one crash → remaining complete;
                 failing candidate gets sensitivity_profile_complete=False.
                 File: tests/backtesting/integration/test_robustness.py
                 Upload before starting: orchestrator.py + evaluation/sensitivity.py
Block 5:  Threshold calibration after first real Stages 1–4 run
Block 6:  Final documentation
Block 7:  OPT-01 + OPT-02 — expected 40–60% Stage 6 reduction
```
<!-- END CONTEXT.md -->