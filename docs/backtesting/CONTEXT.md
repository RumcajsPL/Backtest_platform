# PROJECT CONTEXT — Backtesting & Optimization Framework
## Identity
**Project**: Backtesting & Optimization Framework for WBWSStrategy
**Operator**: Single quantitative retail trader, Windows 10, eToro broker
**Stage**: Phase 6 in progress — Block 2 fully green. Start Block 3.
**Last session ended**: 2026-03-02 — Block 2 adversarial suite 8/8 green. Start Block 3.

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
Phase 6 (hardening):   In progress — Block 2 code written, awaiting first run
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
| **Total green** | **192** | ✅ |

---
## Current Phase Status
```
PHASE:        Phase 6 — Hardening & Delivery
COMPLETED:    Block 0, Block 1, Block 2
NEXT TASK:    Block 3 — Performance validation (4-hour budget)
```
---
## Key Files Modified This Session
| File | Change |
|---|---|
| `tests/backtesting/integration/test_adversarial_suite.py` | Created + all 8 green |

---
## AV-02 / AV-03 Results (Block 2 — locked, do not reopen)
```
AV-02: overfit candidate (fitness=0.97, WFO composite=0.18, window_collapse_flag=True)
       → verdict = no_go. Two-pillar rejection confirmed. Pipeline correctly rejects
         high in-sample fitness when WFO shows cross-window collapse.
AV-03: same 5 candidates under seeds [42, 137, 9871]
       → 5/5 positions stable (100%). All positions: no_go across all seeds.
       → Verdict is signal-driven, not noise-driven (confirmed at SMOKE_MC_ITERATIONS=50).
```

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
## strategy_runner._PARAM_KEY_MAP — Current State frozen state of V1
```python
# ── Parameter name mapping: backtester name → StrategyConfig YAML key ─────────
_PARAM_KEY_MAP: Dict[str, str] = {
    "rsi_period":               "filters.technical_filters.rsi_filter.length",
    "rsi_overbought":           "filters.technical_filters.rsi_filter.overbought",
    "rsi_oversold":             "filters.technical_filters.rsi_filter.oversold",
    "bollinger_length":         "filters.technical_filters.bollinger_filter.length",
    "bollinger_multiplier":     "filters.technical_filters.bollinger_filter.filter_multiplier",
    "bollinger_width_ma":       "filters.technical_filters.bollinger_filter.width_ma_length",
    "adx_enabled":              "filters.technical_filters.adx_filter.enabled",
    "adx_length":               "filters.technical_filters.adx_filter.adx_length",
    "adx_threshold":            "filters.technical_filters.adx_filter.threshold",
    "choppiness_enabled":       "filters.technical_filters.choppiness_filter.enabled",
    "choppiness_length":        "filters.technical_filters.choppiness_filter.length",
    "choppiness_threshold":     "filters.technical_filters.choppiness_filter.threshold",
    "supertrend_enabled":       "filters.technical_filters.supertrend_filter.enabled",
    "supertrend_atr_length":    "filters.technical_filters.supertrend_filter.atr_length",
    "supertrend_factor":        "filters.technical_filters.supertrend_filter.factor",
    "cci_enabled":              "filters.technical_filters.cci_filter.enabled",
    "cci_length":               "filters.technical_filters.cci_filter.length",
    "cci_overbought":           "filters.technical_filters.cci_filter.overbought",
    "cci_oversold":             "filters.technical_filters.cci_filter.oversold",
    "macd_enabled":             "filters.technical_filters.macd_filter.enabled",
    "macd_fast":                "filters.technical_filters.macd_filter.fast_length",
    "macd_slow":                "filters.technical_filters.macd_filter.slow_length",
    "macd_signal":              "filters.technical_filters.macd_filter.signal_length",
    "ma_enabled":               "filters.technical_filters.ma_filter.enabled",
    "ma_length":                "filters.technical_filters.ma_filter.length",
    "ma_slope_length":          "filters.technical_filters.ma_filter.slope_length",
    "pivot_enabled":            "filters.technical_filters.pivot_filter.enabled",
    "pivot_reversal_pct":       "filters.technical_filters.pivot_filter.reversal_percent",
    "pivot_order":              "filters.technical_filters.pivot_filter.order",
    "dpo_enabled":              "filters.technical_filters.dpo_filter.enabled",
    "dpo_length":               "filters.technical_filters.dpo_filter.length",
    "dpo_smooth":               "filters.technical_filters.dpo_filter.smooth",
    "dpo_threshold":            "filters.technical_filters.dpo_filter.threshold",
    "atr_length":               "trade_management.risk.atr_length",
    "atr_multiplier":           "trade_management.risk.atr_multiplier_sl",
    "rr_target":                "trade_management.risk.risk_to_reward_ratio",
    "risk_percentile":          "trade_management.risk.max_risk_percentile",
    # EXCLUDED (v2+): strategy_tf, htf_tf, session_filter, filter_sequence, ma_type
}
```
---
## Phase 6 Blocks
```
Block 0 (done):  E2E real data test — 13/13 green
Block 1 (done):  strategy_runner parameter mapping audit — _PARAM_KEY_MAP frozen V1
Block 2 (done):  Adversarial suite — 8/8 green
          AV-02: overfit → no_go ✅  (769s / 12m49s on operator hardware)
          AV-03: 5/5 positions stable at 100% across seeds [42, 137, 9871] ✅
Block 3 (NEXT):  Performance validation — 4-hour wall-clock budget
          See NEXT_SESSION_PLAN.md for full task breakdown and timing instrumentation plan
Block 4:  Robustness
          - Resume-after-interruption at each of 8 checkpoints
          - Parallel worker isolation (kill one worker mid-run, confirm pipeline continues)
Block 5:  Threshold calibration (D-07)
          - Recalibrate verdict thresholds after first real run results
Block 6:  Final documentation
          - Module reference, YAML config guide, scenario authoring guide,
            output format guide, SQLite query cookbook, paper trading protocol
```

<!-- END CONTEXT.md -->