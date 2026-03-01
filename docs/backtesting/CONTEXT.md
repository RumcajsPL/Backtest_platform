# PROJECT CONTEXT — Backtesting & Optimization Framework
## Identity
**Project**: Backtesting & Optimization Framework for WBWSStrategy
**Operator**: Single quantitative retail trader, Windows 10, eToro broker
**Stage**: Phase 5 closed — Phase 6 E2E wbws real data tests
**Last session ended**: 2026-03-01 — Phase 5 complete. 29 new tests added (19 live pipeline + 12 SQLite queries = all green). All passed on green!

---
## Non-Negotiables (Architecture — never override)
1. **Contracts are the interface** — frozen dataclasses between every module. No raw dicts.
2. **Single responsibility** — one module, one concern. Orchestrator orchestrates only.
3. **Fail fast** — invalid config raises at construction. No silent fallbacks.
4. **Single source of truth** — all config from `backtest_template.yaml`. No module self-loads config.
5. **Immutability** — `frozen=True` on all contracts. `object.__setattr__` in `__post_init__` only.
6. **Windows compatibility** — `pathlib.Path`, `ProcessPoolExecutor` spawn mode, explicit `utf-8` encoding.
7. **Code hygiene** — no print statements, no debug flags, no MagicMocks in production, no commented-out blocks.
8. **CacheManager** — reuse existing from strategy architecture. `clear_all_caches()` between runs.
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
| `ARCHITECTURE.md` | Strategy architecture (frozen — do not modify) | `docs/strategies/architecture/` |
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
## Modules Implemented
```
Phase 2 (core):        candidate_store.py, parameter_space.py, sampler.py, scenario.py,
                       strategy_runner.py, fitness.py, ranker.py, orchestrator.py ✓
Phase 3 (engines):     wfo/{window_generator,wfo_evaluator,wfo_engine,consistency_scorer}.py
                       ga/{population,selection,crossover,mutation,diversity,ga_engine}.py
                       monte_carlo/{perturbation,equity_simulator,mc_metrics,mc_engine}.py ✓
Phase 4 (evaluation):  evaluation/{sensitivity,verdict}.py, yaml_generator.py,
                       report_generator.py ✓
Phase 5 (wiring):      orchestrator.py Stages 5/6/7 fully wired ✓
```

---
## Test Counts
| Scope | Tests | Status |
|---|---|---|
| Phase 2–4 cumulative | 123 | ✅ All green |
| test_live_pipeline.py | 17 | ✅ All green |
| test_sqlite_queries.py | 12 | ✅ All green |
| test_report_yaml.py | 19 | ✅ All green |
| **Total green** | **155** | |

---
## Current Phase Status
```
PHASE:        Phase 5 — Output Layer
COMPLETED:    - orchestrator.py Stages 5/6/7 fully wired and audited
              - CandidateStore all required methods confirmed present
              - test_live_pipeline.py: 17/17 green
              - test_sqlite_queries.py: 12/12 green (all 10 SQLITE_SCHEMA.md queries validated)
              - report_generator.py and yaml_generator.py: fully implemented (Phase 4)
              - test_report_yaml.py: 19/19 green           
BLOCKED ON:   Nothing.
NEXT TASK:    E2E real data test to create and pass.
              Then: Phase 6 — Hardening & Delivery.
```

---
## Known Issues
1. **`write_wfo_window_result()` / `flag_candidate_wfo_insufficient()`** — not in uploaded CandidateStore snapshot. Must exist (Phase 3 tests pass). Confirm before Phase 6. (to confirm but probably fixed)
---
## Open Decisions — ALL RESOLVED (D-01 through D-12)
See TECHNICAL_SPEC.md Section 1.
---
## Key Patching Rule (critical for tests)
`run_mc` is imported **locally** inside `_run_stage_5_mc_deep()` — it is NOT on the orchestrator module namespace.
- ✅ CORRECT: `patch("src.backtesting.monte_carlo.mc_engine.run_mc", ...)`
- ❌ WRONG: `patch("src.backtesting.orchestrator.run_mc", ...)` → AttributeError
General rule: patch where the name is looked up at call time, not where it is defined.
ProcessPoolExecutor workers: always patch the worker function itself (`_evaluate_perturbation`), never functions it calls internally.

---
## Platform / Environment Notes
- **OS**: Windows 10. `pathlib.Path`, `ProcessPoolExecutor` spawn mode, `utf-8` explicit.
- **Python**: 3.13.12
- **Timezone**: OHLCV/signals in CET/CEST. Pipeline timestamps in UTC.
- **Path resolution**: Always use `src/utils/paths.py`.
- **DB**: `data/db/backtest.db` (production). Tests use `tmp_path` fixtures.

---
## Phase 6 Starting Point (when Phase 5 complete)
**Tasks**:
1. E2E test on real wbws data 
2. AV-02 overfit-injection test
3. AV-03 meta-config stability (>80% verdict stability under seed perturbation)
4. Performance validation: ≤4hr full pipeline on real WBWS data
5. Resume validation: all 8 checkpoints
6. `datetime.utcnow()` cleanup (can be done end of Phase 5)
7. Verdict threshold calibration (D-07) against first real run
<!-- END CONTEXT.md -->