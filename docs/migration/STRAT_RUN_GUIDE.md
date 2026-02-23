# E2E Pipeline Validation — `run_strategy.py`

## Prerequisites

```bash
# From project root, with venv active
cd E:\Trading\Backtest_platform
.venv\Scripts\activate
```

---

## Basic Usage

```bash
# 1. Default run (uses strategy_template.yaml)
python scripts/runners/run_strategy.py

# 2. Run your WBWS config
python scripts/runners/run_strategy.py --config configs/strategies/wbws/wbws_strategy_v2.yaml

# 3. Force analytics mode (overrides YAML without modifying it)
python scripts/runners/run_strategy.py --config configs/strategies/wbws/wbws_strategy_v2.yaml --mode analytics

# 4. Debug — full log output
python scripts/runners/run_strategy.py --config configs/strategies/wbws/wbws_strategy_v2.yaml --mode analytics --log-level DEBUG
```

---

## Expected Output (success)

```
============================================================
RESULT SUMMARY
============================================================
  Mode          : analytics
  Total trades  : 142
  Win rate      : 61.3%
  Total PnL     : +1842.5 pts
  Expectancy    : +12.97 pts/trade
  Profit factor : 2.14
  Max drawdown  : -312.0 pts

  Stage timing:
    data          :    234.1 ms
    signals       :     87.3 ms
    filters       :    412.6 ms
    risk          :    156.2 ms
    simulation    :     98.4 ms
    metrics       :     12.1 ms
    TOTAL         :   1000.7 ms
============================================================
```

---

## What to Validate E2E

Work through these in order — each confirms a deeper layer is wired correctly.

| Step | Command | Confirms |
|------|---------|----------|
| 1 | `--log-level DEBUG` (no `--mode`) | Config loads, YAML parses, DataLoader finds files |
| 2 | `--mode core` | Full pipeline runs, trades generated, metrics calculated |
| 3 | `--mode analytics` | Filter metadata, spread, annual range all flow through |
| 4 | Compare core vs analytics PnL | Should be identical — mode only adds metadata |
| 5 | Change one YAML param, rerun | Result changes as expected — config is being read |

---

## Common Failures and What They Mean

| Error | Cause | Fix |
|-------|-------|-----|
| `Config file not found` | Wrong path or cwd | Check `--config` path is relative to project root |
| `Configuration error: ...` | YAML schema violation | Check YAML against `StrategyConfig` fields |
| `File not found` inside pipeline | DataLoader can't find OHLCV files | Check `data.file_path` in YAML |
| `Invalid mode 'debug'` | Old YAML has `mode: debug` | Change to `core` or `analytics` in YAML |
| `asset.symbol is blank` | Spread enabled but no symbol | Add `asset.symbol` to YAML or disable spread |
| Pipeline runs but 0 trades | Filters too aggressive or date range too narrow | Run `--mode analytics --log-level DEBUG` to see filter rejection counts |

---

## Reading the Debug Log

Key lines to look for with `--log-level DEBUG`:

```
# Config loaded cleanly
INFO | StrategyOrchestrator | Config loaded: wbws_strategy_v2.yaml

# Data found and sliced
INFO | DataLoader | Loaded 48320 bars (2020-01-02 → 2023-12-29)

# Filters wired correctly
INFO | FilterPipeline | time_filter=enabled, technical_filters=3, cfg_hash=a3f8c2...
INFO | FilterPipeline | Time filter: 08:30 – 20:30

# ATR cache behaviour (first run = miss, subsequent = hit)
INFO | RiskManager | ATR computed and cached (Wilder RMA, length=14, key=4a2b1c...)

# Filter pass rates
INFO | FilterPipeline | rsi_filter: 340 → 187 (153 rejected)
INFO | FilterPipeline | FilterPipeline complete: 340 → 112 signals (32.9% pass rate)
```

If you see a stage missing from the log entirely, that module is not being reached — the error will be in the stage before it.