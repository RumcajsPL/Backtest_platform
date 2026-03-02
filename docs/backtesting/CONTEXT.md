# PROJECT CONTEXT — Backtesting & Optimization Framework
## Identity
**Project**: Backtesting & Optimization Framework for WBWSStrategy
**Operator**: Single quantitative retail trader, Windows 10, eToro broker
**Stage**: Phase 6 in progress — E2E real data test not yet fully green
**Last session ended**: 2026-03-02 — Phase 6 Block 0 in progress. E2E test scaffolding complete, pipeline executes without errors, but candidates still not passing constraints (0 WFO survivors). Root cause not yet confirmed — likely fitness.py constraint evaluation issue. Troubleshooting deferred to next session.

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
| `ARCHITECTURE.md` | Backtester architecture — module map, data flow, Mermaid diagrams | `docs/backtesting/` |
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
Phase 6 (hardening):   In progress — see Current Phase Status below
```

---
## Test Counts
| Scope | Tests | Status |
|---|---|---|
| Phase 2–4 cumulative | 123 | ✅ All green |
| test_live_pipeline.py | 17 | ✅ All green |
| test_sqlite_queries.py | 12 | ✅ All green |
| test_report_yaml.py | 19 | ✅ All green |
| test_e2e_wbws_real_data.py | 13 | ⚠️ 7 pass, 6 skip (WFO survivors = 0) |
| **Total green** | **155 + 7** | |

---
## Current Phase Status
```
PHASE:        Phase 6 — Hardening & Delivery (Block 0 in progress)
COMPLETED:    - backtest_template.yaml: production-ready, created from scratch
                  - 3 production scenarios (capital_accumulation, swing_trading, conservative)
                  - e2e_test scenario (loose constraints for pipeline validation)
                  - WFO windows calibrated for 3-month WBWS data slice
                  - Parameter zones: safe + exploration (discovery disabled)
              - ARCHITECTURE.md: created for docs/backtesting/
                  - Full module map, Mermaid diagrams, data flow, contract table
              - strategy_runner.py: fixed
                  - _PARAM_KEY_MAP: corrected all YAML paths to match strategy_template.yaml
                  - Added bollinger_length + bollinger_multiplier entries
                  - Fixed datetime.utcnow() → datetime.now(UTC)
                  - Fixed orchestrator.run(mode=) → run(mode_override=)
              - test_e2e_wbws_real_data.py: created, pipeline executes cleanly
                  - 5 real strategy evaluations complete without error
                  - 0 evaluation errors
                  - Pipeline error: none, writer errors: none
                  - P-01, P-01b, P-02, P-02b, P-05b, P-06 all PASS

BLOCKED ON:   WFO survivors = 0 — candidates evaluated successfully but all fail
              constraints even with e2e_test scenario (very loose thresholds).
              Root cause not confirmed. Likely candidate: fitness.py constraint
              evaluation not reading the e2e_test scenario correctly, OR
              fitness.py has a bug where actual metrics are computed differently
              than expected. Needs debug logging to confirm.

NEXT TASK:    1. Add diagnostic prints to test fixture to surface actual metric
                 values per candidate (win_rate, trades_per_week, etc.)
              2. Confirm which constraint is rejecting and why
              3. Fix root cause (fitness.py, scenario loading, or metric extraction)
              4. Get all 13 E2E tests green
              5. Then: Phase 6 full hardening blocks
```

---
## Known Issues
1. **E2E test: WFO survivors = 0** — candidates pass strategy evaluation (no errors) but
   fail constraints in fitness.py even with e2e_test scenario loose thresholds.
   Next session: add diagnostic output to isolate which constraint fails and what
   actual values are being checked against.
2. **Stages 1–4 are stubs** in orchestrator.py — E2E test seeds store directly,
   sets checkpoint to WFO_COMPLETE. TODO marker in test for when stages are wired.
3. **strategy_runner._PARAM_KEY_MAP** — old entries (`indicators.rsi.period` style paths)
   were wrong; fixed this session to match actual strategy_template.yaml paths.
   Verify all mappings are correct when new strategy parameters are added.

---
## Key Files Modified This Session
| File | Change |
|---|---|
| `configs/backtesting/backtest_template.yaml` | Created from scratch — production ready |
| `src/backtesting/strategy_runner.py` | Fixed _PARAM_KEY_MAP, datetime, run() kwarg |
| `docs/backtesting/ARCHITECTURE.md` | Created — full module/data flow documentation |
| `tests/backtesting/integration/test_e2e_wbws_real_data.py` | Created — E2E test |

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
- **Path resolution**: Always use `src/utils/paths.py`
- **DB**: `data/db/backtest.db` (production). Tests use `tmp_path` fixtures.
- **strategy_runner.run() kwarg**: `mode_override="core"` — NOT `mode="core"`

---
## strategy_runner._PARAM_KEY_MAP — Current State (verified this session)
```python
_PARAM_KEY_MAP = {
    "rsi_period":           "filters.technical_filters.rsi_filter.length",
    "rsi_overbought":       "filters.technical_filters.rsi_filter.overbought",
    "rsi_oversold":         "filters.technical_filters.rsi_filter.oversold",
    "adx_threshold":        "filters.technical_filters.adx_filter.threshold",
    "atr_length":           "trade_management.risk.atr_length",
    "atr_multiplier":       "trade_management.risk.atr_multiplier_sl",
    "rr_target":            "trade_management.risk.risk_to_reward_ratio",
    "risk_percentile":      "trade_management.risk.max_risk_percentile",
    "bollinger_length":     "filters.technical_filters.bollinger_filter.length",
    "bollinger_multiplier": "filters.technical_filters.bollinger_filter.filter_multiplier",
    "strategy_tf":          "data.strategy_timeframe",
    "htf_tf":               "data.htf_timeframe",
    "session_filter":       "filters.time.session",
}
```

---
## Phase 6 Blocks (organized this session)
```
Block 0:  E2E real data test — IN PROGRESS (pipeline runs, survivors=0 blocking)
Block 1:  strategy_runner parameter mapping audit
          - Validate ALL parametrable strategy features are covered in _PARAM_KEY_MAP
          - Strategy has 10 technical filters (rsi, bollinger, choppiness, supertrend,
            cci, adx, macd, ma, pivot, dpo) — each with enabled flag + parameters
          - Filter sequence order is parametrable (filter_sequence list in YAML)
          - filter enabled/disabled flags are parametrable
          - Strategy TF (strategy_tf) and HTF (htf_tf) are parametrable
          - Session filter (time_filter) is parametrable
          - Full mapping audit → confirm all are in _PARAM_KEY_MAP or document why not
Block 2:  Adversarial suite
          - AV-02: Overfit-injection → must fail at WFO
          - AV-03: >80% verdict stability under seed perturbation
Block 3:  Performance validation
          - Full pipeline on real data within 4-hour target
          - Profile + resolve bottlenecks if over budget
Block 4:  Robustness
          - Resume-after-interruption at each of 8 checkpoints
          - Parallel worker isolation
Block 5:  Threshold calibration (D-07)
          - Recalibrate verdict thresholds after first real run results
Block 6:  Final documentation
          - Module reference, YAML config guide, scenario authoring guide,
            output format guide, SQLite query cookbook, paper trading protocol
```
<!-- END CONTEXT.md -->