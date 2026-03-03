# PROJECT CONTEXT — Backtesting & Optimization Framework
## Identity
**Project**: Backtesting & Optimization Framework for WBWSStrategy
**Operator**: Single quantitative retail trader, Windows 10, eToro broker
**Stage**: Phase 6 in progress — Blocks 0–5 complete (233 green). Start Block 6.
**Last session ended**: 2026-03-03 — Blocks 4 and 5 closed. ARCHITECTURE.md v1.1.0 updated.
---
## Non-Negotiables (Architecture — never override)
1. **Contracts are the interface** — frozen dataclasses between every module. No raw dicts.
2. **Single responsibility** — one module, one concern. Orchestrator orchestrates only.
3. **Fail fast** — invalid config raises at construction. No silent fallbacks.
4. **Single source of truth** — all config from `backtest_template.yaml`.
5. **Immutability** — `frozen=True` on all contracts.
6. **Windows compatibility** — `pathlib.Path`, `ProcessPoolExecutor` spawn mode, `utf-8`.
7. **Code hygiene** — no print statements, no debug flags, no MagicMocks in production.
8. **CacheManager** — reuse existing. `clear_all_caches()` between runs.
9. **Immutable run artifacts** — config hash, all seeds, perturbation profile name stored.
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
| `ARCHITECTURE.md` | Architecture v1.1.0 — updated this session | `docs/backtesting/architecture/` |
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
| test_robustness.py | 12 | ✅ All green |
| test_threshold_calibration.py | 22 | ✅ All green |
| **Total green** | **233** | ✅ |
---
## Current Phase Status
```
PHASE:      Phase 6 — Hardening & Delivery
COMPLETED:  Blocks 0, 1, 2, 3, 4, 5
NEXT:       Block 6 — Final Documentation
```
---
## Block 3 Performance Baseline (LOCKED — 2026-03-03)
```
Hardware:  Windows 10, 6 workers
Config:    mc.deep.iterations=3000, mc.deep.input_count=10,
           sens.input_count=5, sens.max_steps=2, max_workers=6
Run 1:  Total=457.2s  Stage5=2.5s   Stage6=446.3s  Stage7=8.3s
Run 2:  Total=337.2s  Stage5=0.3s   Stage6=332.6s  Stage7=4.4s
Budget: 337–457s of 14,400s (2.3–3.2%) ✅
PERF-06: Stage 6 dominance is expected (not a bug).
```
---
## Performance Optimisation Opportunities (Block 7)
```
OPT-01  [HIGH]      Pool reuse across candidates in Stage 6 — 40–60% reduction
OPT-02  [MEDIUM]    Batch perturbations per worker task — further 15–25%
OPT-03  [LOW]       sensitivity.input_count: 5→3, YAML only, saves ~130–180s
OPT-04  [NEGLIGIBLE] Stage 5 no action until input_count > 50
OPT-05  [LOW]       Clean up evaluate_sensitivity max_workers when OPT-01 lands
All files: src/backtesting/evaluation/sensitivity.py
```
---
## Verdict Engine Logic (LOCKED — confirmed Block 5, 2026-03-03)
```python
wfo_pillar_go    = wfo_composite >= wfo_go_floor        # >= INCLUSIVE
wfo_pillar_no_go = wfo_composite < wfo_borderline_floor  # < strictly less
mc_pillar_go    = ruin_prob <= mc_go_ceiling             # <= INCLUSIVE
mc_pillar_no_go = ruin_prob > mc_borderline_ceiling      # > strictly greater
# ruin_prob is None → mc_pillar_no_go=True → NO_GO always
# verdict.py logs WARNING for None — expected behaviour, not a bug
# oos_gate_triggered = oos_gate_enabled AND wfo_score.oos_gate_triggered
# Either condition alone does NOT trigger the flag
if wfo_pillar_no_go OR mc_pillar_no_go:          → NO_GO
elif wfo_pillar_go AND mc_pillar_go AND no flags: → AUTO_GO
else:                                             → BORDERLINE
```
## Verdict Grid (LOCKED — Block 5, e2e_test thresholds)
```
go_wfo>=0.55  borderline_wfo>=0.40  go_mc<=0.10  borderline_mc<=0.25
           MC<go    MC=go   MC=bdr   MC>bdr  MC=None
WFO>go:   AUTO_GO  AUTO_GO  BORDER   NO_GO   NO_GO
WFO=go:   AUTO_GO  AUTO_GO  BORDER   NO_GO   NO_GO
WFO=bdr:  BORDER   BORDER   BORDER   NO_GO   NO_GO
WFO=ng:   NO_GO    NO_GO    NO_GO    NO_GO   NO_GO
Modifier demotion (AUTO_GO base):
  spike / collapse / incomplete / oos_gate(both) → BORDERLINE
  All flags on NO_GO base → NO_GO (cannot override)
```
---
## Windows Spawn Mode — Mock Patching Constraint (CRITICAL — confirmed Block 4)
```
unittest.mock patches DO NOT cross ProcessPoolExecutor spawn boundary on Windows.
Consequence for tests:
  - Never patch _evaluate_perturbation via unittest.mock.patch
  - Patch evaluate_sensitivity at: src.backtesting.orchestrator.evaluate_sensitivity
  - This is the correct isolation boundary for Stage 6 orchestration tests
Confirmed by Block 4 ROB-09:
  ERROR: Can't pickle <class 'unittest.mock.MagicMock'>
```
---
## Critical Patch Targets
```python
patch("src.backtesting.monte_carlo.mc_engine.run_mc", ...)          # Stage 5 CORRECT
patch("src.backtesting.orchestrator.run_mc", ...)                   # WRONG — AttributeError
patch("src.backtesting.orchestrator.evaluate_sensitivity", ...)     # Stage 6 CORRECT
patch("src.backtesting.evaluation.sensitivity._evaluate_perturbation", ...)  # WRONG Windows
```
---
## Test Import Convention (CRITICAL)
```python
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
from src.utils.paths import PROJECT_ROOT
from src.backtesting.contracts import (...)      # contracts BEFORE candidate_store
from src.backtesting.candidate_store import CandidateStore
```
---
## CandidateStore Write API
```python
store.write_candidate(record: CandidateRecord)   # ONE arg — fitness embedded in record
store.query_mc_results(run_id, "deep")           # mode is str not MCMode enum
# NO write_fitness_result() method
```
---
## CandidateRecord Constructor
```python
# parameters_json (str) not parameters (dict)
# stage = CandidateStage.RANDOM.value (str)
# recorded_at = datetime.now(UTC)
# All 30+ fields explicit — no defaults except verdict group
# NO fields: win_rate, profit_factor, total_trades, expectancy, max_drawdown
```
---
## MCResult Constructor
```python
# Required: perturbation_profile_name (str), evaluated_at (datetime)
# Field: worst_drawdown_across_paths — NOT worst_drawdown
# error=None if valid; error="..." and ruin_probability=None if failed
```
---
## Config Shape for Tests
```python
# load_scenario() requires nested sub-dicts — flat dict raises KeyError
"scenarios": { "e2e_test": {
    "description": "...",
    "fitness_weights": { "net_pnl":0.25, "expectancy":0.25, "max_drawdown":0.20,
                         "win_rate":0.15, "trade_frequency":0.10, "profit_factor":0.05 },
    "constraints": { "min_win_rate":0.0, "max_drawdown":1.0, "max_losing_streak":9999,
                     "min_trades_per_week":0.0, "min_expectancy":-9999.0,
                     "min_profit_factor":0.0 },
    "mc_prefilter_ruin_threshold": 1.0,
    "wfo_temporal_weights": { "median_return":0.40, "variance":0.20,
                               "worst_drawdown":0.20, "fraction_positive":0.20 },
    "verdict_thresholds": { "go_wfo_floor":0.55, "borderline_wfo_floor":0.40,
                            "go_mc_ruin_ceiling":0.10, "borderline_mc_ruin_ceiling":0.25,
                            "sensitivity_spike_threshold":0.15 },
    "report_emphasis": [],
}}
# fitness weights sum == 1.0; wfo_temporal weights sum == 1.0
# borderline_wfo_floor < go_wfo_floor
# go_mc_ruin_ceiling < borderline_mc_ruin_ceiling
```
---
## Platform Notes
- Windows 10, Python 3.13.12
- `pathlib.Path`, spawn mode, `utf-8` explicit everywhere
- `strategy_runner.run()` kwarg: `mode_override="core"` NOT `mode="core"`
- Timestamps: pipeline UTC, OHLCV/signals CET/CEST
---
## Phase 6 Blocks
```
Block 0 (done):  E2E real data — 13/13 ✅
Block 1 (done):  _PARAM_KEY_MAP audit — frozen V1 ✅
Block 2 (done):  Adversarial suite — 8/8 ✅
Block 3 (done):  Performance — 7/7 ✅ Baseline locked. OPT-01–05 identified.
Block 4 (done):  Robustness — 12/12 ✅ Windows spawn constraint documented.
Block 5 (done):  Threshold calibration — 22/22 ✅ Verdict grid locked.
Block 6 (NEXT):  Final documentation
                 Upload first: backtest_template.yaml (for ARCHITECTURE.md
                 production threshold values + stage counts in Section 3)
Block 7:         OPT-01 + OPT-02 — expected 40–60% Stage 6 reduction
```
<!-- END CONTEXT.md -->