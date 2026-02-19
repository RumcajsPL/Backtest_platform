# BLOCK A GUIDE — Global Rename: `"debug"` → `"analytics"`
**Built progressively during Blocks B–K**  
**Execute AFTER Block K is complete**  
**Last updated**: Block B  
**Files logged so far**: 4 of ~35

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
**Already partially addressed in Block B** — default changed from `"debug"` to `"core"`, invalid mode message updated.  
**Remaining**: None. Block B already cleaned this file completely.

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
*(Not yet visited)*

**Expected findings** (from scan report):
- Mode checks for logging gates — currently `"debug"`, need `"analytics"`

---

### `src/strategies/specific/modules/trade_simulator.py`
*(Not yet visited)*

**Expected findings** (from scan report P0-CH4-1):
- `verbose: bool = False` flag — will be replaced with `mode: str = "core"` in Block E
- No direct `"debug"` string, but mode-gating logic needs updating

---

### `src/config/config_schema.py`
*(Not yet visited)*

**Expected findings** (from scan report P1-CH0-7):
- Any `"debug"` in mode validation or defaults

---

### `src/utils/structured_logger.py`
*(Not yet visited)*

**Expected findings** (from scan report P1-CH0-6):
- `LogStage` enum may have `DEBUG = "debug"` or similar → rename to `ANALYTICS = "analytics"`

---

## BLOCK B CONFIRMED CHANGES (already applied)

| File | What was changed |
|------|-----------------|
| `signal_generator.py` | `mode: str = "debug"` → `"core"`; `"debug"` in validation → replaced; `if self.mode == "debug"` → `"analytics"` (5 occurrences); `SignalGeneratorAdapter` deleted (it hardcoded `mode="debug"`) |
| `filter_contracts.py` | `mode: str = "debug"` in `FilterProtocol.apply_filter()` docstring → `"analytics"` |
| `data_contracts.py` | No `"debug"` occurrences found |
| `trade_contracts.py` | No `"debug"` occurrences found |
| `trade_manager.py` | No `"debug"` occurrences found (uses `logger.debug()` — that's a log level, not our mode string, leave it) |

---

## FINAL VERIFICATION COMMAND
Run after Block A rename pass is complete:

```bash
# Must return ZERO results (log levels like logger.debug are excluded)
grep -rn '"debug"' src/strategies/ src/config/ src/indicators/ \
  | grep -v "logger.debug\|#.*debug\|\.debug(" \
  | grep -v ".pyc"

# Also verify migration guard works
python -c "
from src.strategies.specific.modules.signal_generator import SignalGenerator
try:
    SignalGenerator(htf_period='1H', mode='debug')
    print('ERROR: Should have raised ValueError')
except ValueError as e:
    print(f'OK: {e}')
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

**Modules that need this guard** (add as Block A pass proceeds):
- [ ] `data_loader.py`
- [ ] `signal_generator.py` — already has the guard (added Block B)
- [ ] `filter_pipeline.py`
- [ ] `trade_simulator.py`
- [ ] `risk_manager.py` (if it accepts mode)
- [ ] `spread_manager.py` (if it accepts mode)

---

## BLOCK C CONFIRMED CHANGES (already applied)

| File | What was changed |
|------|-----------------|
| `config_schema.py` | No `"debug"` string found in the file itself. Added `ExecutionConfig` with migration guard raising `ValueError` on `mode="debug"` — this is the canonical guard that all YAML-loading goes through. The guard is active. |
| `configs/strategy_template.yaml` | `execution.mode: "core"` — correct. Comment block mentions `"debug"` as deprecated with explicit note. No live code reference. |

**Block C scan result for Block A**: `config_schema.py` is already clean. The `ExecutionConfig.__post_init__` guard is the primary defence for YAML-based mode injection. No additional rename needed here.


---

## BLOCK D CONFIRMED CHANGES (already applied)

| File | What was changed |
|------|-----------------|
| `cache.py` | No `"debug"` string. Clean. |
| `filter_pipeline.py` | `"debug"` appears in 3 lines — all inside migration guard (`__init__` raises `ValueError`). No live `mode == "debug"` branch exists. |

**Block D scan result for Block A**: `filter_pipeline.py` migration guard active. No rename work needed.

---

## BLOCK E CONFIRMED CHANGES (already applied)

| File | What was changed |
|------|-----------------|
| `trade_simulator.py` | `"debug"` appears in module docstring (historical reference) and in analytics key fallback: `config.get("analytics", config.get("debug", {}))` — the fallback is intentional for backward compat during transition, not a live mode string. No `mode == "debug"` branch. Class and method docstrings reference old behaviour only. |

**Block E scan result for Block A:**  
- `config.get("analytics", config.get("debug", {}))` — keep as-is. The fallback reads old `"debug"` config keys from YAML files that haven't been updated yet. When Block A renames those YAML keys, remove the fallback: `config.get("analytics", {})`.
- No rename needed in `trade_simulator.py` itself beyond removing the fallback after Block A YAML updates.