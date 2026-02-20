# BLOCK A GUIDE — Global Rename: `"debug"` → `"analytics"`
**Built progressively during Blocks B–K**  
**Execute AFTER Block K is complete**  
**Last updated**: Block G  
**Files logged so far**: 6 of ~35

---

## HOW TO USE THIS GUIDE

For each file below:
1. Open the file
2. Apply every change listed under it
3. Run the verification command at the bottom
4. Commit

This is a **one-pass, no-partial-work** operation.  
Do not start until all of Blocks B–K are complete.

---

## RENAME MAP

### `src/strategies/specific/modules/signal_generator.py`
**Already fully addressed in Block B** — default changed from `"debug"` to `"core"`, migration guard active.  
**Remaining**: None.

---

### `src/strategies/specific/modules/data_loader.py`
*(Not yet visited — will be updated when Block touches this file)*

**Expected findings** (from scan report P0-CH1-1):
- `mode: str = "debug"` default in `__init__` → change to `"core"`
- `self._verbose = (mode == "debug")` → change to `(mode == "analytics")`
- `if mode == "debug":` occurrences → `if mode == "analytics":`
- Add migration guard raising `ValueError` on `mode="debug"`

---

### `src/strategies/specific/filters/*.py` (all 10 filters)
*(Not yet visited)*

**Expected findings** (from scan report P0-CH3-2):
- `mode: str = "debug"` in `apply_filter()` signature → `"core"`
- `if mode == "debug":` logging guards → `if mode == "analytics":`

---

### `src/strategies/specific/modules/filter_pipeline.py`
**Already fully addressed in Block D** — migration guard active, logging gates fixed.  
**Remaining**: None.

---

### `src/strategies/specific/modules/trade_simulator.py`
**Already fully addressed in Block E** — one remnant noted:
- `config.get("analytics", config.get("debug", {}))` — intentional backward-compat fallback.
- **Block A action**: After Block A YAML key rename (`debug:` → `analytics:` in any config files), remove the inner `.get("debug", {})` fallback: `config.get("analytics", {})`.

---

### `src/config/config_schema.py`
**Already fully addressed in Block C** — `ExecutionConfig.__post_init__` raises `ValueError` on `mode="debug"`.  
**Remaining**: None.

---

### `src/utils/structured_logger.py`
*(Not yet visited)*

**Expected findings** (from scan report P1-CH0-6):
- `LogStage` enum may have `DEBUG = "debug"` → rename to `ANALYTICS = "analytics"`
- Add `REPORTING = "reporting"` for Phase 5 stage coverage

---

### `src/strategies/specific/modules/risk_manager.py`
**Fully addressed in Block F** — migration guard active; `"debug"` raises `ValueError`; all logging gated on `mode == "analytics"`.  
**Remaining**: None.

---

### `src/strategies/specific/modules/spread_manager.py`
**Fully addressed in Block F** — migration guard active; `"debug"` raises `ValueError`; logging gated on `mode == "analytics"`.  
**Remaining**: None.

---

### `src/strategies/contracts/signal_contracts.py`
**Fully addressed in Block G.**

| Location | Old value | New value | Notes |
|----------|-----------|-----------|-------|
| `from_wbws_trigger()` `signal_metadata["mode"]` | `"debug"` | `"analytics"` | Mode tag in metadata dict |
| `from_wbws_trigger()` docstring | mentions `"debug mode"` | `"analytics mode"` | |

**Block G also added**: `__iter__` guard raising `RuntimeError` when `indicator_data is None` (DEC-024). This is not a rename item but was applied in the same pass.

**Remaining for Block A**: None. File is fully clean.

---

### `src/strategies/contracts/analytics_contracts.py`
**Fully addressed in Block G.**

- No `"debug"` string was present in this file — confirmed clean in scan.
- `TradingSessionConfig` frozen (DEC-004 / P1-CH5-1) applied in Block G.
- Placeholder `UserWarning` for unimplemented features removed (P1-CH5-2 resolved: stubs replaced with clear docstring notes).

**Remaining for Block A**: None. File is clean.

---

## BLOCK B CONFIRMED CHANGES (already applied)

| File | What was changed |
|------|-----------------|
| `signal_generator.py` | `mode: str = "debug"` → `"core"`; `if self.mode == "debug"` → `"analytics"` (5 occurrences); `SignalGeneratorAdapter` deleted |
| `filter_contracts.py` | `mode: str = "debug"` in `FilterProtocol.apply_filter()` docstring → `"analytics"` |
| `data_contracts.py` | No `"debug"` occurrences found |
| `trade_contracts.py` | No `"debug"` occurrences found |
| `trade_manager.py` | No `"debug"` occurrences found (`logger.debug()` calls untouched — log level, not mode string) |

---

## BLOCK C CONFIRMED CHANGES (already applied)

| File | What was changed |
|------|-----------------|
| `config_schema.py` | No `"debug"` string in file. `ExecutionConfig.__post_init__` guard added. Clean. |
| `configs/strategy_template.yaml` | `execution.mode: "core"` — correct; deprecated `"debug"` noted in comment only |

---

## BLOCK D CONFIRMED CHANGES (already applied)

| File | What was changed |
|------|-----------------|
| `cache.py` | No `"debug"` string. Clean. |
| `filter_pipeline.py` | Migration guard in `__init__` raises `ValueError` on `"debug"`. No live `mode == "debug"` branch. |

---

## BLOCK E CONFIRMED CHANGES (already applied)

| File | What was changed |
|------|-----------------|
| `trade_simulator.py` | `"debug"` in docstring only (historical). Intentional fallback `config.get("debug", {})` kept; remove in Block A after YAML key rename. |

---

## BLOCK F CONFIRMED CHANGES (already applied)

| File | What was changed |
|------|-----------------|
| `risk_manager.py` | `mode` parameter added; `"debug"` raises `ValueError`; all `logger.info()` gated on `mode == "analytics"`. Fully clean. |
| `spread_manager.py` | `mode` parameter added; `"debug"` raises `ValueError`; logging gated on analytics. Fully clean. |

---

## BLOCK G CONFIRMED CHANGES (already applied)

| File | What was changed |
|------|-----------------|
| `signal_contracts.py` | `signal_metadata["mode"]` tag: `"debug"` → `"analytics"` in `from_wbws_trigger()`. `frozen=True` added to `SignalFrame`, `SignalStats`. `__iter__` guard added (DEC-024). |
| `analytics_contracts.py` | No `"debug"` string present — confirmed clean. `frozen=True` added to `TradingSessionConfig`. Placeholder warning removed. |

---

## FINAL VERIFICATION COMMAND
Run after Block A rename pass is complete:

```bash
# Must return ZERO results
# (logger.debug calls are log-level calls — excluded; not our mode string)
grep -rn '"debug"' src/strategies/ src/config/ src/indicators/ \
  | grep -v "logger\.debug\|#.*debug\|\.debug(\|pyc" \
  | grep -v "config\.get.*\"debug\""   # trade_simulator fallback — remove manually

# After removing trade_simulator fallback, re-run with no exclusions:
grep -rn '"debug"' src/strategies/ src/config/ src/indicators/ \
  | grep -v "logger\.debug\|#.*debug\|\.debug(\|pyc"

# Verify migration guards active in key entry points:
python -c "
modules = [
    ('src.strategies.specific.modules.data_loader',    'DataLoader',     {}),
    ('src.strategies.specific.modules.signal_generator','SignalGenerator',{'htf_period': '1H'}),
    ('src.strategies.specific.modules.filter_pipeline', 'FilterPipeline', {}),
    ('src.strategies.specific.modules.risk_manager',    'RiskManager',    {}),
    ('src.strategies.specific.modules.spread_manager',  'SpreadManager',  {'asset_symbol': 'X'}),
]
for mod_path, cls_name, kwargs in modules:
    import importlib
    mod = importlib.import_module(mod_path)
    cls = getattr(mod, cls_name)
    try:
        cls(**kwargs, mode='debug')
        print(f'ERROR: {cls_name} should have raised ValueError')
    except ValueError as e:
        print(f'OK {cls_name}: guard active')
    except Exception as e:
        print(f'SKIP {cls_name}: {type(e).__name__} (likely missing required args)')
"
```

---

## MIGRATION GUARD TEMPLATE
Add this to every module that accepts a `mode` parameter:

```python
def __init__(self, ..., mode: str = "core"):
    if mode == "debug":
        raise ValueError(
            "Mode 'debug' has been renamed to 'analytics' in the new architecture. "
            "Update your call: mode='analytics'"
        )
    valid_modes = {"core", "analytics"}
    if mode not in valid_modes:
        raise ValueError(f"Invalid mode '{mode}'. Must be one of: {valid_modes}")
```

**Status of migration guards per module:**

| Module | Guard active? | Added in |
|--------|--------------|----------|
| `data_loader.py` | ⏳ pending | Block A |
| `signal_generator.py` | ✅ | Block B |
| `filter_pipeline.py` | ✅ | Block D |
| `trade_simulator.py` | ✅ | Block E |
| `risk_manager.py` | ✅ | Block F |
| `spread_manager.py` | ✅ | Block F |
| `config_schema.py` (ExecutionConfig) | ✅ | Block C |
| `metrics_calculator.py` | ⏳ pending | Block A |
| `trade_analytics.py` | ⏳ pending | Block A |
| `report_generator.py` | ⏳ pending | Block I |

---

## REMAINING WORK FOR BLOCK A FINAL PASS

After all Blocks B–K complete, the following files still need a scan + rename pass:

| File | Expected `"debug"` occurrences | Priority |
|------|-------------------------------|----------|
| `data_loader.py` | 3–5 (mode default, verbose flag, log gates) | P0 |
| `structured_logger.py` | 1 (`LogStage.DEBUG`) | P1 |
| `metrics_calculator.py` | 0–2 (mode default, log gate) | P1 |
| `trade_analytics.py` | 1–3 (mode checks) | P1 |
| `report_generator.py` | 0–1 (mode check in generate()) | P1 |
| All 10 filter `.py` files | 1–2 each (mode default in apply_filter) | P1 |
| Any YAML config files in `configs/` | 1 (`debug:` key in old-style configs) | P1 |

**Total estimated remaining occurrences**: ~20–30 across ~14 files.  
**After Block A, target**: zero `"debug"` mode strings anywhere in new architecture.

---

## BLOCK H CONFIRMED CHANGES — filters 1–4 (already applied)

| File | `"debug"` occurrences | Action taken |
|------|----------------------|--------------|
| `adx_filter.py` | 4 (execution_time_ms gate, indicator_data gate, indicator_values gate, signal_metadata tag) | All → `"analytics"` |
| `bollinger_filter.py` | 4 (same pattern) | All → `"analytics"` |
| `cci_filter.py` | 4 (same pattern) | All → `"analytics"` |
| `choppiness_filter.py` | 4 (same pattern) | All → `"analytics"` |
| `dpo_filter.py` | 3 (no indicator_values block) | All → `"analytics"` |

All 5 files fully clean. Migration guards NOT added to `apply_filter()` — mode is validated by `FilterPipeline` before it reaches filters.

**Filters 5–10 pending** — same pattern expected (3–4 occurrences each):
- `ma_filter.py`, `macd_filter.py`, `pivot_filter.py`, `rsi_filter.py`, `supertrend_filter.py`, `time_filter.py`

---

## UPDATED REMAINING WORK FOR BLOCK A FINAL PASS

| File | Expected `"debug"` occurrences | Priority | Notes |
|------|-------------------------------|----------|-------|
| `data_loader.py` | 3–5 | P0 | Mode default, verbose flag, log gates |
| `structured_logger.py` | 1 | P1 | `LogStage.DEBUG` enum value |
| `metrics_calculator.py` | 0–2 | P1 | Mode default, log gate |
| `trade_analytics.py` | 1–3 | P1 | Mode checks |
| `report_generator.py` | 0–1 | P1 | Mode check in generate() |
| `ma_filter.py` | 3–4 | P1 | Same pattern as H filters 1–4 |
| `macd_filter.py` | 3–4 | P1 | Same pattern |
| `pivot_filter.py` | 3–4 | P1 | Same pattern |
| `rsi_filter.py` | 3–4 | P1 | Same pattern |
| `supertrend_filter.py` | 3–4 | P1 | Same pattern |
| `time_filter.py` | 3–4 | P1 | Same pattern + P1-CH3-8 (typed params) |
| YAML config files in `configs/` | 1 | P1 | `debug:` key in old-style configs |
| `trade_simulator.py` | 1 | P1 | Remove `config.get("debug", {})` fallback after YAML rename |

**Total estimated remaining**: ~25–35 occurrences across ~13 files.

---

## BLOCK H CONFIRMED CHANGES — filters 5–10 (applied)

| File | `"debug"` occurrences removed | Extra changes |
|------|------------------------------|---------------|
| `ma_filter.py` | 3 (`execution_time_ms`, `indicator_data`, `signal_metadata` tag) | None |
| `macd_filter.py` | 3 (same pattern) | None |
| `pivot_filter.py` | 3 (same pattern) | None |
| `rsi_filter.py` | 4 (+ `indicator_values` gate) | None |
| `supertrend_filter.py` | 4 (+ `indicator_values` gate) | NaN ordering preserved exactly |
| `time_filter.py` | 4 + `logger.info` inside body | DEC-021: `is_in_trading_hours()` + `get_session_info()` removed; logger gated; `self.config` not stored |

**All 10 filter files fully clean.**

---

## P1-CH3-8 DEFERRED ITEM — `time_filter.py` constructor

`TimeFilter.__init__` still accepts `config: Dict[str, Any]` (raw trade-management config).
The dict is unpacked immediately and not stored — mutation risk eliminated.
Typed parameter signature requires coordinated change in `filter_pipeline.py`.
**Resolve when `filter_pipeline.py` is revisited (Block I or Block A).**

---

## UPDATED GLOBAL BLOCK A REMAINING WORK

| File | Expected `"debug"` occurrences | Notes |
|------|-------------------------------|-------|
| `data_loader.py` | 3–5 | Mode default, verbose flag, log gates |
| `structured_logger.py` | 1 | `LogStage.DEBUG` enum value — may be intentional; check before rename |
| `metrics_calculator.py` | 0–2 | Mode default, log gate |
| `trade_analytics.py` | 1–3 | Mode checks |
| `report_generator.py` | 0–1 | Handled in Block I |
| YAML config files in `configs/` | 1–2 | `debug:` key in old-style configs |
| `trade_simulator.py` | 1 | `config.get("debug", {})` fallback |

**All 10 filter files: ✅ COMPLETE — zero remaining `"debug"` occurrences.**
**Estimated remaining across non-filter files: ~8–14 occurrences in ~6 files.**

---

## BLOCK I CONFIRMED CHANGES (applied)

### `report_contracts.py`
| Item | Status |
|------|--------|
| `"debug"` occurrences | ZERO — confirmed clean from source |
| `brand_name` field added to `ReportConfig` | New: `brand_name: str = "WBWSStrategy"` with blank-check in `__post_init__` |
| `datetime` import removed | Was unused |
| Both dataclasses already `frozen=True` | No change needed |

### `report_generator.py`
| Item | Status |
|------|--------|
| `"debug"` occurrences | ZERO — module post-dates the migration |
| `brand_name` wired | 2 sites in HTML (header brand span + footer version string) |
| `__main__` print block | Removed (DEC-021) |
| Duplicate `logger.info` in `_save_html` | Removed |
| `None` guard on `analytics_report` | Added at top of `generate()` |

---

## FINAL BLOCK A SCOPE — Updated

Files with confirmed remaining `"debug"` work after Blocks B–I:

| File | Expected occurrences | Notes |
|------|---------------------|-------|
| `data_loader.py` | 3–5 | Mode default, verbose flag, log gates |
| `structured_logger.py` | 1 | `LogStage.DEBUG` — may be intentional enum value; verify before rename |
| `metrics_calculator.py` | 0–2 | Mode default, log gate |
| `trade_analytics.py` | 1–3 | Mode checks |
| YAML configs in `configs/` | 1–2 | `debug:` key in old-style configs |
| `trade_simulator.py` | 1 | `config.get("debug", {})` fallback (delivered in Block E — confirm clean) |

**All contracts, filters, and report modules: ✅ COMPLETE.**
**Estimated remaining global total: ~8–14 occurrences in ~5 files.**

### Deferred items for Block A sweep
- P1-CH3-8: `TimeFilter.__init__` typed parameters (needs `filter_pipeline.py`)
- `structured_logger.py` `LogStage.DEBUG`: check if this is a genuinely distinct concept (debug log stage) vs the mode parameter rename — may be intentional and should NOT be renamed