# PROJECT CONTEXT — Backtesting & Optimization Framework
## Identity
**Project**: Backtesting & Optimization Framework for WBWSStrategy
**Operator**: Single quantitative retail trader, Windows 10, eToro broker
**Stage**: Phase 6 in progress — E2E real data test fully green!
**Last session ended (interrupted by Claude technical issues)**: 2026-03-02 — Phase 6 Block 0 and Block 1 done. Discussion of Block 1 audit findings

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
| test_e2e_wbws_real_data.py | 13 | ✅ All green |
| **Total green** | **155 + 13** | |

---
## Current Phase Status
```
PHASE:        Phase 6 — Hardening & Delivery (Block 0 in progress)
COMPLETED:    - backtest_template.yaml: production-ready, created from scratch
                  - 3 production scenarios (capital_accumulation, swing_trading, conservative)
                  - e2e_test scenario created and passed
                  - WFO windows calibrated for 3-month WBWS data slice
                  - Parameter zones: safe + exploration (discovery disabled)
              - ARCHITECTURE.md: created for docs/backtesting/
                  - Full module map, Mermaid diagrams, data flow, contract table
              - strategy_runner.py: fixed
                  - _PARAM_KEY_MAP: corrected all YAML paths to match strategy_template.yaml
                  - Added all possible filters
                  - Fixed datetime.utcnow() → datetime.now(UTC)
                  - Fixed orchestrator.run(mode=) → run(mode_override=)
              - test_e2e_wbws_real_data.py: created, pipeline executes cleanly - all pass - green
                

NEXT TASK:    1. Finalize post Block 1 discussion 
              2. Then: Phase 6 full hardening blocks
```
---
## Key Files Modified This Session
| File | Change |
|---|---|
| `configs/backtesting/backtest_template.yaml` | Created from scratch and updated — production ready |
| `src/backtesting/strategy_runner.py` | Fixed _PARAM_KEY_MAP, datetime, run() kwarg |
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
# This dict is the ONLY place in the backtester that knows strategy config keys.
# Update here when the strategy YAML schema changes.
_PARAM_KEY_MAP: Dict[str, str] = {
    # ── RSI filter (always enabled in safe/exploration zones) ────────────────
    "rsi_period":               "filters.technical_filters.rsi_filter.length",
    "rsi_overbought":           "filters.technical_filters.rsi_filter.overbought",
    "rsi_oversold":             "filters.technical_filters.rsi_filter.oversold",

    # ── Bollinger filter (always enabled in safe/exploration zones) ──────────
    "bollinger_length":         "filters.technical_filters.bollinger_filter.length",
    "bollinger_multiplier":     "filters.technical_filters.bollinger_filter.filter_multiplier",
    "bollinger_width_ma":       "filters.technical_filters.bollinger_filter.width_ma_length",

    # ── ADX filter ───────────────────────────────────────────────────────────
    "adx_enabled":              "filters.technical_filters.adx_filter.enabled",
    "adx_length":               "filters.technical_filters.adx_filter.adx_length",
    "adx_threshold":            "filters.technical_filters.adx_filter.threshold",

    # ── Choppiness filter ────────────────────────────────────────────────────
    "choppiness_enabled":       "filters.technical_filters.choppiness_filter.enabled",
    "choppiness_length":        "filters.technical_filters.choppiness_filter.length",
    "choppiness_threshold":     "filters.technical_filters.choppiness_filter.threshold",

    # ── Supertrend filter ────────────────────────────────────────────────────
    "supertrend_enabled":       "filters.technical_filters.supertrend_filter.enabled",
    "supertrend_atr_length":    "filters.technical_filters.supertrend_filter.atr_length",
    "supertrend_factor":        "filters.technical_filters.supertrend_filter.factor",

    # ── CCI filter ───────────────────────────────────────────────────────────
    "cci_enabled":              "filters.technical_filters.cci_filter.enabled",
    "cci_length":               "filters.technical_filters.cci_filter.length",
    "cci_overbought":           "filters.technical_filters.cci_filter.overbought",
    "cci_oversold":             "filters.technical_filters.cci_filter.oversold",

    # ── MACD filter ──────────────────────────────────────────────────────────
    "macd_enabled":             "filters.technical_filters.macd_filter.enabled",
    "macd_fast":                "filters.technical_filters.macd_filter.fast_length",
    "macd_slow":                "filters.technical_filters.macd_filter.slow_length",
    "macd_signal":              "filters.technical_filters.macd_filter.signal_length",

    # ── MA filter ────────────────────────────────────────────────────────────
    "ma_enabled":               "filters.technical_filters.ma_filter.enabled",
    "ma_length":                "filters.technical_filters.ma_filter.length",
    "ma_slope_length":          "filters.technical_filters.ma_filter.slope_length",
    # ma_type excluded: high interaction effects; add as choice param in dedicated zone (v2+)

    # ── Pivot filter ─────────────────────────────────────────────────────────
    "pivot_enabled":            "filters.technical_filters.pivot_filter.enabled",
    "pivot_reversal_pct":       "filters.technical_filters.pivot_filter.reversal_percent",
    "pivot_order":              "filters.technical_filters.pivot_filter.order",

    # ── DPO filter ───────────────────────────────────────────────────────────
    "dpo_enabled":              "filters.technical_filters.dpo_filter.enabled",
    "dpo_length":               "filters.technical_filters.dpo_filter.length",
    "dpo_smooth":               "filters.technical_filters.dpo_filter.smooth",
    "dpo_threshold":            "filters.technical_filters.dpo_filter.threshold",

    # ── Trade management — risk ───────────────────────────────────────────────
    "atr_length":               "trade_management.risk.atr_length",
    "atr_multiplier":           "trade_management.risk.atr_multiplier_sl",
    "rr_target":                "trade_management.risk.risk_to_reward_ratio",
    "risk_percentile":          "trade_management.risk.max_risk_percentile",

    # EXCLUDED (v2+):
    #   strategy_tf    — data.paths.strategy_ohlcv is a full file path, not a TF field.
    #                    Requires path construction + file existence validation.
    #   htf_tf         — same issue; data.htf_period also needs a matching file path.
    #   session_filter — session_start/end are nested {hour, minute} dicts, not scalars.
    #   filter_sequence — list of 10 names; 10! orderings, no fitness gradient. v2+.
    #   ma_type        — choice param with high interaction effects. Dedicated zone only.
}
```

---
## Phase 6 Blocks (organized this session)
```
Block 0:  E2E real data test — corrected, closed, 13 test pass green
Block 1:  strategy_runner parameter mapping audit (done)
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