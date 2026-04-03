# Agent C — DataLoader Audit Report

**Date:** 2026-04-03
**Scope:** `src/strategies/core/data_loader.py` + `src/backtesting/`
**Agent:** C
**Env:** Production (read-only)

---

## 1. Which package/module owns DataLoader?

**File path:** `E:\Trading\Backtest_platform\src\strategies\core\data_loader.py`
**Class definition:** Line 104 — `class DataLoader:`
**Top-level package:** `src.strategies.core` (the `strategies` package, `core` submodule)

The module is part of the **strategies** package — not the backtesting package. It lives under `src/strategies/core/`, alongside other strategy-related core modules.

---

## 2. Methods added specifically for backtester use

| # | Method | Signature | Description | Flags |
|---|--------|-----------|-------------|-------|
| 1 | `load_data` | `(self) -> DataBundle` | Main entry point. Loads all data files (strategy, HTF, LTF, ARTF), applies date-range slicing for WFO windows, returns a typed `DataBundle`. | **Date slicing**, **caching**, **returns subsets of data** |
| 2 | `_load_file_with_cache` | `(self, file_config: DataFileConfig, data_type: str, apply_date_range: bool = True) -> pd.DataFrame` | Loads a single Parquet/CSV file with pickle-based disk caching. Slices to date_range **before** saving to cache (B9O-006). Slices to date_range **before** sort_index to avoid OOM (B9O-008). | **Date slicing**, **caching** |
| 3 | `_get_cache_key` | `(self, file_path: Path, date_range: Optional[DateRange] = None, use_content_hash: bool = False) -> Optional[str]` | Generates an MD5 cache key from file path, size, mtime, version tag (`v3.3`), and optional date range. | **Date slicing**, **caching** |
| 4 | `_load_cached_data` | `(self, cache_key: str) -> Optional[pd.DataFrame]` | Loads a pickled DataFrame from `~/.wbws_data_cache/{cache_key}.pkl`. Returns `None` on miss or corruption. | **Caching** |
| 5 | `_save_to_cache` | `(self, cache_key: str, df: pd.DataFrame)` | Saves a DataFrame as a pickle file to the cache directory. | **Caching** |
| 6 | `_get_sliced_cache_key` | `(self, file_path: str, date_range_str: str) -> str` | (B9O-001) Generates a separate cache key for sliced strategy file loads. | **Date slicing**, **caching** |
| 7 | `_load_sliced_strategy_cache` | `(self, file_path: str, date_range_str: str) -> Optional[pd.DataFrame]` | (B9O-001) Loads a pre-sliced strategy DataFrame for a specific date range. | **Date slicing**, **caching**, **returns subsets of data** |
| 8 | `_save_sliced_strategy_cache` | `(self, df: pd.DataFrame, file_path: str, date_range_str: str) -> None` | (B9O-001) Saves a sliced strategy DataFrame to a separate cache key. | **Date slicing**, **caching** |
| 9 | `_build_data_config` | `(self) -> DataConfig` | Converts the `StrategyConfig` into a `DataConfig` with `DataFileConfig` objects and optional `DateRange`. | — |
| 10 | `_sanitize_df` | `(self, df: pd.DataFrame, name: str) -> pd.DataFrame` | Replaces inf with NaN, forward/backward fills NaNs. | — |
| 11 | `_validate_dataframe` | `(self, df: pd.DataFrame, name: str) -> DataValidationResult` | Validates OHLC columns, positive prices, high>=low, open/close within range. | — |
| 12 | `cache_stats` (property) | `(self) -> Optional[CacheStats]` | Returns cache hit/miss stats, file count, and total cache size. | **Caching** |

### Flagged methods (date slicing / caching / window iteration / returning subsets)

- `load_data` — date slicing, caching, returns subsets
- `_load_file_with_cache` — date slicing, caching
- `_get_cache_key` — date slicing, caching
- `_load_cached_data` — caching
- `_save_to_cache` — caching
- `_get_sliced_cache_key` — date slicing, caching
- `_load_sliced_strategy_cache` — date slicing, caching, returns subsets
- `_save_sliced_strategy_cache` — date slicing, caching
- `cache_stats` — caching

---

## 3. What the backtester calls directly vs. what goes through DataLoader

### Direct file reads in `src/backtesting/` — NO market data reads

**Zero** calls to `pd.read_parquet()`, `pd.read_csv()`, `pd.read_json()`, or any other market data loading function exist in `src/backtesting/`.

The backtesting module's own file I/O is limited to **config files only**:

| # | File | Line | Call | Purpose |
|---|------|------|------|---------|
| 1 | `src/backtesting/orchestrator.py` | 107 | `open(config_path, "r", encoding="utf-8")` | Read `backtest_template.yaml` |
| 2 | `src/backtesting/orchestrator.py` | 108 | `yaml.safe_load(f)` | Parse YAML config |
| 3 | `src/backtesting/orchestrator.py` | 128 | `config_path.read_bytes()` | SHA-256 hash of config for run identity |
| 4 | `src/backtesting/yaml_generator.py` | 184 | `base_strategy_yaml_path.open("r", encoding="utf-8")` | Read base strategy YAML template |
| 5 | `src/backtesting/yaml_generator.py` | 185 | `yaml.safe_load(fh)` | Parse base YAML |
| 6 | `src/backtesting/yaml_generator.py` | 231 | `output_path.open("w", encoding="utf-8")` | Write generated YAML |
| 7 | `src/backtesting/strategy_runner.py` | 268 | `open(base_yaml_path, "r", encoding="utf-8")` | Read base YAML for parameter injection |
| 8 | `src/backtesting/strategy_runner.py` | 269 | `yaml.safe_load(f)` | Parse YAML |
| 9 | `src/backtesting/strategy_runner.py` | 291 | `open(output_path, "w", encoding="utf-8")` | Write output YAML |
| 10 | `src/backtesting/candidate_store.py` | 293 | `sqlite3.connect(str(db_path), ...)` | Open SQLite DB (not market data) |

### DataLoader usage — NOT in `src/backtesting/`

**Zero** imports or uses of `DataLoader` exist anywhere in `src/backtesting/`.

DataLoader is imported and instantiated in exactly **one** file outside backtesting:

| # | File | Line | Call | Purpose |
|---|------|------|------|---------|
| 1 | `src/strategies/orchestrator.py` | 18 | `from src.strategies.core.data_loader import DataLoader` | Import |
| 2 | `src/strategies/orchestrator.py` | 315 | `DataLoader(config, mode=...)` | Instantiation |

### How data actually flows to the backtester

The backtesting module receives data **indirectly** through the `DataBundle` returned by `DataLoader.load_data()`. The call chain is:

1. `src/strategies/orchestrator.py` (line 315) → instantiates `DataLoader`
2. `DataLoader.load_data()` (line 468) → returns `DataBundle`
3. `DataBundle` is passed downstream to strategy runners, signal generators, and trade simulators — all within the `src/strategies/` package
4. `src/backtesting/` orchestrates the overall workflow (YAML generation, candidate storage, GA/WFO engines) but **never touches raw data files**

### Summary

- **All OHLCV data file I/O** (`pd.read_parquet` at line 299, `pd.read_csv` at line 293, `pickle.load`/`pickle.dump` at lines 207/217/241/249) is exclusively in `DataLoader` (`src/strategies/core/data_loader.py`).
- **`src/backtesting/` has zero direct market data file reads.** It reads only YAML configs and a SQLite candidate store.
- **`src/backtesting/` has zero imports or uses of `DataLoader`.** The DataLoader lives in `src/strategies/` and is consumed by `src/strategies/orchestrator.py`, not by the backtesting orchestrator.

---

# Agent C — Consistency Scorer Audit Report

**Date:** 2026-04-03
**Scope:** `src/backtesting/wfo/consistency_scorer.py`
**Agent:** C
**Env:** Production (read-only)

---

## 1. Hardcoded numeric constants in the file

### Module-level named constants

| # | Name | Value | Line | Usage context | Comments present |
|---|------|-------|------|---------------|------------------|
| 1 | `_SIGMOID_SCALE` | `313.0` | 63 | Default scale for `_sigmoid_normalise()`. Controls sensitivity of `median_return_norm` to net_pnl values. `value=0 → 0.5`, `value=+scale → ~0.731`, `value=-scale → ~0.269`. | Extensive. Module docstring (lines 30–38) documents recalibration history: `0.10 → 131.0` for DAX points. Inline comment (line 63): `# After calibration standard: 310.0; 128 1min; 283 15min`. Recalibration formula: `scale = stdev(net_pnl WHERE is_ga_fitness_window=0) * 0.5`. |
| 2 | `_MAX_EXPECTED_VARIANCE` | `100_000.0` | 64 | Ceiling for per-window net_pnl variance (pts²). Used to invert variance into [0,1] score: `1.0 - (variance_raw / _MAX_EXPECTED_VARIANCE)`. | Module docstring (lines 30–38): prior value `0.10 → 100_000.0` for DAX. `stdev≈262 → variance≈68,000 → ceiling = 100_000 pts²`. |
| 3 | `_MAX_EXPECTED_DRAWDOWN` | `2_500.0` | 65 | Ceiling for per-window max_drawdown (pts). Used to invert drawdown into [0,1] score: `1.0 - (abs(raw) / _MAX_EXPECTED_DRAWDOWN)`. | Module docstring (lines 30–38): prior value `0.50 → 600.0`. Inline comment says `Observed range in calibration run: 282–676 pts → ceiling = 1_000 pts (conservative)` — but actual value is `2_500.0`, indicating a later recalibration not documented in the comment. |
| 4 | `oos_degradation_threshold` (param default) | `0.50` | 73 | Default threshold for IS/OOS degradation fraction. Passed to `_check_oos_gate()`. | No comment explaining origin. |

### Hardcoded literals inside function bodies

| # | Value | Line | Usage context | Comments present |
|---|-------|------|---------------|------------------|
| 5 | `0.0` (composite default) | 99 | Returned as `composite_score` when no valid windows exist. | — |
| 6 | `1.0` (worst_drawdown default) | 100 | Returned as `worst_window_drawdown` (normalised) when no valid windows — penalises fully. | — |
| 7 | `0.0` (median fallback) | 110 | Fallback for `median_return_raw` when `net_pnls` is empty. | — |
| 8 | `0.0` (variance fallback) | 117 | Fallback for `variance_raw` when fewer than 2 net_pnl values. | — |
| 9 | `0.0` / `1.0` (variance clamp) | 120 | `max(0.0, min(1.0, ...))` — clamps variance_norm to [0,1]. | — |
| 10 | `0.0` (drawdown fallback) | 125 | Fallback for `worst_drawdown_raw` when `drawdowns` is empty. | — |
| 11 | `0.0` / `1.0` (drawdown clamp) | 127 | `max(0.0, min(1.0, ...))` — clamps worst_dd_norm to [0,1]. | — |
| 12 | `0.0` (fraction_positive fallback) | 131 | Fallback when `net_pnls` is empty. | — |
| 13 | `0.0` / `1.0` (composite clamp) | 141 | `max(0.0, min(1.0, composite))` — clamps composite score to [0,1]. | Comment: `# Clamp to [0, 1] — rounding safety` |
| 14 | `0.0` (sigmoid overflow fallback) | 179 | Returns `0.0` on OverflowError when `value < 0`. | — |
| 15 | `1.0` (sigmoid overflow fallback) | 179 | Returns `1.0` on OverflowError when `value >= 0`. | — |

---

## 2. Instrument-specific or timeframe-specific values hardcoded elsewhere in the file

**Yes.** The three module-level normalisation constants are all instrument-specific (calibrated for DAX point-denominated returns):

| Line | Value | Instrument/timeframe specificity |
|------|-------|----------------------------------|
| 63 | `_SIGMOID_SCALE: float = 313.0` | Comment: `# After calibration standard: 310.0; 128 1min; 283 15min` — explicitly references two timeframe-specific calibrations (128-bar 1min, 283-bar 15min). Calibrated from DAX point-denominated returns. |
| 64 | `_MAX_EXPECTED_VARIANCE: float = 100_000.0` | Calibrated for DAX points (pts²). Module docstring: "Prior values were calibrated for fractional returns (0–1 range) and produced degenerate outputs for DAX point-denominated returns." |
| 65 | `_MAX_EXPECTED_DRAWDOWN: float = 2_500.0` | Calibrated for DAX points. Module docstring notes observed range `282–676 pts` from calibration run. |

No other instrument-specific or timeframe-specific literals appear inside function bodies beyond these three module-level constants.

---

## 3. Is `_SIGMOID_SCALE` accepted as a parameter anywhere, or is it always read from module-level state?

**`_SIGMOID_SCALE` is always read from module-level state.** It is never accepted as a parameter through any public API.

The mechanism:

1. **Module-level constant** (line 63): `_SIGMOID_SCALE: float = 313.0`

2. **`_sigmoid_normalise()` private helper** (line 167): Has a default parameter `scale: float = _SIGMOID_SCALE`. This means the function *can* accept a different scale if called explicitly with one, but its default pulls from module-level state.

3. **`compute_consistency()` public function** (line 113): Calls `_sigmoid_normalise(median_return_raw, scale=_SIGMOID_SCALE)` — explicitly passes the module-level constant. There is **no parameter** on `compute_consistency()` to override this.

4. **`ScenarioProfile`** (the config object passed to `compute_consistency()`): Does **not** contain a field for sigmoid scale. The scenario profile provides `wfo_weight_median_return`, `wfo_weight_variance`, `wfo_weight_worst_drawdown`, `wfo_weight_fraction_positive`, and `wfo_collapse_drawdown_threshold` — but no sigmoid scale override.

**Conclusion:** `_SIGMOID_SCALE` is effectively a global constant. The only way to change it is to edit line 63 of the source file. It is not configurable per-candidate, per-scenario, per-instrument, or per-timeframe at runtime.

---

# Agent C — max_workers Audit Report

**Date:** 2026-04-03
**Scope:** Entire codebase — all occurrences of `max_workers`
**Agent:** C
**Env:** Production (read-only)

---

## 1. Every file where max_workers is set, read, or referenced

### A. Production source code (`src/`)

#### `src/backtesting/orchestrator.py`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 527 | **Reader** | `max_workers: int = config.get("run", {}).get("max_workers", 6)` | Stage 3 (Random Search) — reads from config, defaults to 6 |
| 876 | **Reader** | `max_workers: int = config.get("run", {}).get("max_workers", 6)` | Stage 5 (WFO) — reads from config, defaults to 6 |
| 942 | **Reader** | `max_workers=max_workers,` | Passes to `run_wfo()` |
| 1097 | **Reader** | `max_workers: int = config.get("run", {}).get("max_workers", 6)` | Stage 6 (Sensitivity) — reads from config, defaults to 6 |
| 1122 | **Reader** | `with ProcessPoolExecutor(max_workers=max_workers) as pool:` | Creates shared pool for all sensitivity candidates |
| 1143 | **Reader** | `max_workers=max_workers,` | Passes to `evaluate_sensitivity()` |

#### `src/backtesting/wfo/wfo_engine.py`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 57 | **Setter** (param default) | `max_workers: int = 6,` | Function parameter default on `run_wfo()` |
| 74 | **Doc** | `max_workers:              ProcessPoolExecutor worker count.` | Docstring |
| 115 | **Reader** | `with ProcessPoolExecutor(max_workers=max_workers) as pool:` | Spawns pool for WFO window evaluation |

#### `src/backtesting/ga/ga_engine.py`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 23 | **Doc** | `Also hardened config["run"]["max_workers"] and` | Comment |
| 119 | **Reader** | `max_workers: int = config.get("run", {}).get("max_workers", 6)` | Reads from config, defaults to 6 |
| 170 | **Reader** | `max_workers=max_workers,` | Passes to `_evaluate_generation()` |
| 260 | **Setter** (param) | `max_workers: int,` | Function parameter on `_evaluate_generation()` |
| 299 | **Reader** | `with ProcessPoolExecutor(max_workers=max_workers) as pool:` | Spawns pool for GA generation evaluation |

#### `src/backtesting/evaluation/sensitivity.py`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 18 | **Setter** (param default) | `max_workers=6, min_significant_trades=30,` | Function parameter default on `run_sensitivity()` (inner function) |
| 166 | **Setter** (param default) | `max_workers: int = 6,` | Function parameter default on `evaluate_sensitivity()` |
| 184 | **Doc** | `max_workers          : ProcessPoolExecutor worker count. Used only when pool=None.` | Docstring |
| 245 | **Reader** | `else ProcessPoolExecutor(max_workers=max_workers)` | Creates pool when no shared pool provided |

#### `src/data/update_raw_ticks.py`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 13 | **Setter** | `MAX_CONCURRENT_WORKERS = 50` | Module-level constant |
| 124 | **Setter** (param default) | `def update_raw_bi5_files(instrument: str, base_dir: str, max_workers: int = 50):` | Function parameter default |
| 162 | **Reader** | `session = get_requests_session(pool_size=max_workers)` | Passes to HTTP session pool sizing |
| 166 | **Reader** | `with ThreadPoolExecutor(max_workers=max_workers) as executor:` | ThreadPoolExecutor for concurrent downloads |
| 203 | **Reader** | `max_workers=MAX_CONCURRENT_WORKERS` | Call site — passes module constant |

#### `src/data/download_raw_ticks.py`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 22 | **Doc** | `# OPTIMIZATION: Match pool_connections and pool_maxsize to your max_workers` | Comment |
| 85 | **Setter** (param default) | `def download_and_save_bi5_files(..., max_workers: int = 50):` | Function parameter default |
| 106 | **Doc** | `# Pass max_workers to session to size the pool correctly` | Comment |
| 107 | **Reader** | `session = get_requests_session(pool_size=max_workers)` | Passes to HTTP session pool sizing |
| 114 | **Reader** | `with ThreadPoolExecutor(max_workers=max_workers) as executor:` | ThreadPoolExecutor for concurrent downloads |
| 131 | **Reader** | `print(f"Starting download for {files_to_download} new files with {max_workers} workers...")` | Log message |
| 152 | **Setter** | `MAX_CONCURRENT_WORKERS = 50` | Module-level constant |
| 159 | **Reader** | `max_workers=MAX_CONCURRENT_WORKERS` | Call site — passes module constant |

#### `src/strategies/core/data_loader.py`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 14 | **Doc** | `Peak per worker: ~1.9 GB. With max_workers=2: ~3.8 GB → OOM.` | Comment in module docstring |
| 46 | **Doc** | `max_workers=2 → ~3.4GB peak → OOM under normal system load.` | Comment in module docstring |
| 57 | **Doc** | `max_workers can be restored to 6.` | Comment in module docstring |
| 66 | **Doc** | `With max_workers=6 and cold cache: 6 × 856MB = 5.1GB → OOM → system crash.` | Comment in module docstring |
| 74 | **Doc** | `per-worker peak memory is now ~40MB regardless of max_workers.` | Comment in module docstring |
| 471 | **Doc** | `Memory per worker: ~850MB → ~1MB. max_workers can be restored to 6.` | Comment in docstring |

### B. Configuration files (`configs/`)

#### `configs/backtesting/backtest_template.yaml`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 38 | **Setter** | `max_workers: 6                    # ProcessPoolExecutor workers (Windows spawn mode)` | Template default — production baseline |

#### `configs/backtesting/backtest_V1_01.yaml`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 8 | **Doc** | `#   FIX 1: max_workers: 6 → 2  (CRITICAL — OOM fix)` | Comment |
| 49 | **Doc** | `# RUNTIME ESTIMATE: 14-20 hours at max_workers=2` | Comment |
| 78 | **Setter** | `max_workers: 2       # MANDATORY — OOM at 6 confirmed. Do not raise until B9O-009 (shared memory).` | Hardcoded override — OOM guard |

#### `configs/backtesting/backtest_V1_5min.yaml`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 25 | **Setter** | `max_workers: 2` | Hardcoded override |

#### `configs/backtesting/backtest_V1_1min.yaml`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 19 | **Doc** | `#   [ ] Confirm max_workers = 2 in run config` | Comment (checklist) |
| 124 | **Doc** | `# RUNTIME ESTIMATE: 11–15 hours at max_workers=2` | Comment |
| 153 | **Setter** | `max_workers: 2` | Hardcoded override |

#### `configs/backtesting/backtest_V1_15min.yaml`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 47 | **Doc** | `# RUNTIME ESTIMATE: 7–10 hours at max_workers=4 (slightly longer due to more samples)` | Comment |
| 174 | **Doc** | `# RUNTIME ESTIMATE: 6–9 hours at max_workers=4` | Comment |
| 195 | **Setter** | `max_workers: 2` | Hardcoded override |

#### `configs/backtesting/backtest_V1_10min.yaml`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 97 | **Doc** | `# RUNTIME ESTIMATE: 6–9 hours at max_workers=4 (fewer windows, but longer each)` | Comment |
| 128 | **Setter** | `max_workers: 4` | Hardcoded override — only config using 4 |

#### `configs/backtesting/backtest_1st_run.yaml`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 19 | **Doc** | `#   run.max_workers:                 6  → 4        [was: first-run conservative]` | Comment (changelog) |
| 35 | **Doc** | `#   run.max_workers:                  4  → 6       [restored to production value]` | Comment (changelog) |
| 66 | **Setter** | `max_workers: 6                    # Restored to production value (was 4 for e2e_test)` | Hardcoded override |

### C. Test files (`tests/`)

#### `tests/backtesting/unit/test_sensitivity.py`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 144 | **Setter** | `max_workers=1,` | Test isolation — forces single-threaded |

#### `tests/backtesting/integration/test_block9b_ga.py`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 436 | **Setter** | `"run": {"max_workers": 1, "temp_dir": "/tmp"},` | Test isolation in config dict |

#### `tests/backtesting/integration/test_block9a_orchestrator.py`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 96 | **Setter** | `"run": {"output_dir": "/tmp/test", "max_workers": 1},` | Test isolation |
| 138 | **Setter** | `"run": {"output_dir": "/tmp/test", "max_workers": 1},` | Test isolation |
| 171 | **Setter** | `"run": {"output_dir": "/tmp/test", "max_workers": 1},` | Test isolation |

#### `tests/backtesting/integration/test_robustness.py`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 92 | **Setter** | `"max_workers": 2,` | E2E test config |
| 461 | **Setter** (param default) | `max_steps=2, max_workers=6, min_significant_trades=30):` | Function parameter default |
| 527 | **Setter** (param default) | `max_steps=2, max_workers=6, min_significant_trades=30):` | Function parameter default |

#### `tests/backtesting/integration/test_performance.py`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 229 | **Reader** | `max_workers = config.get("run", {}).get("max_workers", 6)` | Reads from config, defaults to 6 |
| 233 | **Reader** | `mc_iters, mc_input, sens_input, sens_steps, max_workers,` | Passes to perf test function |
| 310 | **Reader** | `"max_workers": max_workers,` | Stores in results dict |
| 433 | **Reader** | `f"    Max workers            : {r['max_workers']}\n"` | Prints in report |

#### `tests/backtesting/integration/test_adversarial_suite.py`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 98 | **Setter** | `AV_MAX_WORKERS = 2` | Module-level constant for adversarial tests |
| 178 | **Setter** | `config["run"]["max_workers"] = AV_MAX_WORKERS` | Overrides config for test run |

#### `tests/backtesting/integration/test_live_pipeline.py`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 111 | **Setter** | `"max_workers": 1,` | Base test config — single-threaded |
| 638 | **Doc** | `spike_threshold, max_steps, max_workers, and min_significant_trades.` | Docstring |
| 655 | **Reader** | `assert captured_kwargs["max_workers"] == 1` | Assertion — verifies correct value passed |
| 918 | **Setter** | `"max_workers": 1,` | Test config override |

#### `tests/backtesting/integration/test_e2e_wbws_real_data.py`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 338 | **Setter** | `config["run"]["max_workers"] = 2   # Limit workers for test isolation` | Overrides config for test |

#### `tests/backtesting/benchmarks/bench_d02_sqlite_wal.py`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 154 | **Reader** | `with ProcessPoolExecutor(max_workers=n_workers) as pool:` | Benchmark — `n_workers` is the loop variable |

### D. Documentation (`docs/`)

#### `docs/ctp/CTP_ROADMAP.md`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 31 | **Doc** | `Architecture redesign: RawDataStore + WindowSlicer + SignalCache eliminates V1's 231× signal recomputation and OOM constraint (max_workers: 2 → 6+).` | Roadmap comment |

#### `docs/backtesting/V2/V2_SKILL.md`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 34 | **Doc** | `without per-instrument recalibration. \`max_workers\` constraint removed — 6+ workers stable.` | V2 claim |
| 212 | **Doc** | `- \`max_workers\` constraint removed (shared memory eliminates OOM at 8GB RAM)` | V2 claim |

#### `docs/backtesting/V2/V2_SESSION_LOG.md`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 27 | **Doc** | `- max_workers: stable at 2 and 4; 6 tested without OOM but no confirmed perf gain yet — DEC-008 to resolve` | Session note |

#### `docs/backtesting/V2/V2_PLAN.md`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 48 | **Doc** | `\| DD-003 \| V1 due diligence: audit max_workers usage — where set, what limits it, profiling data if any \| M-DD \| C \| Not started \|` | Plan entry |
| 119 | **Doc** | `\| DEC-008 \| max_workers target for V2: 6 confirmed safe? Profile under shared memory to verify perf gain over 4. Informed by DD-003 \| ARCH-002 \| After DD-003 \|` | Plan entry |
| 153 | **Doc** | `- "max_workers=6 stable at 8GB RAM" — confirmed by profiling run, no OOM` | Acceptance criterion |
| 188 | **Doc** | `\| Pre-V2 \| max_workers constraint removed \| Shared memory makes 6+ workers safe \| ARCH-002 \|` | Plan entry |

#### `docs/backtesting/V2/V2_CONTEXT.md`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 40 | **Doc** | `DD-003   [P0]  Agent C: audit max_workers — where set, what constraints it; confirm whether` | Task description |
| 52 | **Doc** | `DEC-008  Resolve max_workers target decision (informed by DD-003)` | Decision entry |
| 73 | **Doc** | `\| DEC-008 \| max_workers target: 6 confirmed safe under shm? Profile to verify gain over 4 (after DD-003) \| ARCH-002 \| Open \|` | Decision tracker |
| 84 | **Doc** | `- \`max_workers\` constraint removed (shared memory architecture)` | V2 claim |
| 142 | **Doc** | `- V1 \`max_workers\`: confirmed stable at 2 and 4. 6 tested without issues but no measurable` | Status note |
| 169 | **Doc** | `DD-003: Search codebase for max_workers. Report:` | Task spec |
| 170 | **Doc** | `- Every file where max_workers is set or read` | Task spec |
| 190 | **Doc** | `- Resolve DEC-008 (max_workers target) from DD-003` | Task spec |

#### `docs/backtesting/V2/V2_CHANGELOG.md`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 37 | **Doc** | `reloaded repeatedly, causing peak RAM of ~897MB per worker and forcing \`max_workers: 2\`` | Changelog entry |
| 70 | **Doc** | `#### V2-ARCH-003 — max_workers constraint under investigation` | Architecture decision header |
| 72 | **Doc** | `**Rationale**: V1 ran stably at \`max_workers=2\` and \`max_workers=4\`. \`max_workers=6\`` | Rationale |
| 78 | **Doc** | `**Decision**: The hard \`max_workers=2\` constraint is removed as an OOM guard. The` | Decision |
| 83 | **Doc** | `**Breaks**: Config entries relying on \`max_workers=2\` as a safety cap — these become` | Breaking change note |

#### `docs/backtesting/OPERATOR_RUNBOOK.md`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 78 | **Setter** | `max_workers: 2` | YAML example in runbook |
| 81 | **Doc** | `For full-history runs (data range > 6 months), **\`max_workers: 2\` is a hard limit**. Each worker loads the full LTF dataset into memory for trade simulation. On an 8GB RAM machine, more than 2 workers causes out-of-memory failures. For short (3-month) runs, up to 6 workers is safe.` | Operator guidance |
| 158 | **Setter** | `max_workers: 2                   # hard limit 2 for full-history; up to 6 for 3-month` | YAML example with comment |

#### `docs/backtesting/BACKTESTING_TRACKER.md`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 164 | **Setter** | `max_workers: 2` | Tracker entry (YAML snippet) |
| 227 | **Setter** | `max_workers: 4` | Tracker entry (YAML snippet) |

#### `scripts/diagnostics/test_b9o007_memory.py`

| Line | Type | Code | Notes |
|------|------|------|-------|
| 135 | **Doc** | `print("Restore max_workers: 6 in calibration YAML.")` | Diagnostic script message |

---

## 2. Comments, guard conditions, or config keys explaining the max_workers limit

### Primary OOM guard comment (most authoritative)

**File:** `configs/backtesting/backtest_V1_01.yaml`, line 78:
```yaml
max_workers: 2       # MANDATORY — OOM at 6 confirmed. Do not raise until B9O-009 (shared memory).
```

### Operator runbook explanation

**File:** `docs/backtesting/OPERATOR_RUNBOOK.md`, line 81:
```
For full-history runs (data range > 6 months), **`max_workers: 2` is a hard limit**.
Each worker loads the full LTF dataset into memory for trade simulation. On an 8GB
RAM machine, more than 2 workers causes out-of-memory failures. For short (3-month)
runs, up to 6 workers is safe.
```

### DataLoader module docstring (root cause documentation)

**File:** `src/strategies/core/data_loader.py`, lines 14, 46, 57, 66, 74, 471:
```
Peak per worker: ~1.9 GB. With max_workers=2: ~3.8 GB → OOM.       (line 14)
max_workers=2 → ~3.4GB peak → OOM under normal system load.         (line 46)
max_workers can be restored to 6.                                    (line 57)
With max_workers=6 and cold cache: 6 × 856MB = 5.1GB → OOM → crash. (line 66)
per-worker peak memory is now ~40MB regardless of max_workers.       (line 74)
Memory per worker: ~850MB → ~1MB. max_workers can be restored to 6. (line 471)
```

### V2 changelog decision

**File:** `docs/backtesting/V2/V2_CHANGELOG.md`, lines 70–83:
```
#### V2-ARCH-003 — max_workers constraint under investigation
Rationale: V1 ran stably at max_workers=2 and max_workers=4. max_workers=6
tested without issues but no measurable perf gain confirmed.
Decision: The hard max_workers=2 constraint is removed as an OOM guard.
```

### GA engine comment

**File:** `src/backtesting/ga/ga_engine.py`, line 23:
```
Also hardened config["run"]["max_workers"] and
```
(Refers to reading max_workers from config rather than relying on function defaults.)

### No guard conditions in code

There are **no** `if` statements, `assert` guards, or runtime checks that enforce a maximum value for `max_workers`. The constraint is purely documented in comments and enforced by the operator setting lower values in config files.

---

## 3. Is max_workers read from config, hardcoded, or both?

### Read from config (with hardcoded default of 6)

These locations read `max_workers` from `config["run"]["max_workers"]` with a fallback default of `6`:

| File | Line | Default | Mechanism |
|------|------|---------|-----------|
| `src/backtesting/orchestrator.py` | 527 | `6` | `config.get("run", {}).get("max_workers", 6)` |
| `src/backtesting/orchestrator.py` | 876 | `6` | `config.get("run", {}).get("max_workers", 6)` |
| `src/backtesting/orchestrator.py` | 1097 | `6` | `config.get("run", {}).get("max_workers", 6)` |
| `src/backtesting/ga/ga_engine.py` | 119 | `6` | `config.get("run", {}).get("max_workers", 6)` |
| `tests/backtesting/integration/test_performance.py` | 229 | `6` | `config.get("run", {}).get("max_workers", 6)` |

These locations accept `max_workers` as a **function parameter** with a hardcoded default of `6`:

| File | Line | Default | Mechanism |
|------|------|---------|-----------|
| `src/backtesting/wfo/wfo_engine.py` | 57 | `6` | `max_workers: int = 6` |
| `src/backtesting/evaluation/sensitivity.py` | 166 | `6` | `max_workers: int = 6` |
| `src/backtesting/evaluation/sensitivity.py` | 18 | `6` | `max_workers=6` (inner function) |

### Hardcoded in config files (YAML overrides)

These config files set explicit values that override the default of 6:

| File | Line | Value | Reason |
|------|------|-------|--------|
| `configs/backtesting/backtest_V1_01.yaml` | 78 | `2` | OOM guard — "MANDATORY" |
| `configs/backtesting/backtest_V1_5min.yaml` | 25 | `2` | Full-history run |
| `configs/backtesting/backtest_V1_1min.yaml` | 153 | `2` | Full-history run |
| `configs/backtesting/backtest_V1_15min.yaml` | 195 | `2` | Full-history run |
| `configs/backtesting/backtest_V1_10min.yaml` | 128 | `4` | Compromise — longer windows |
| `configs/backtesting/backtest_1st_run.yaml` | 66 | `6` | Production baseline restored |
| `configs/backtesting/backtest_template.yaml` | 38 | `6` | Template default |

### Hardcoded in data download scripts

| File | Line | Value | Purpose |
|------|------|-------|---------|
| `src/data/update_raw_ticks.py` | 13 | `MAX_CONCURRENT_WORKERS = 50` | Dukascopy tick download concurrency |
| `src/data/download_raw_ticks.py` | 152 | `MAX_CONCURRENT_WORKERS = 50` | Dukascopy tick download concurrency |

### Hardcoded in tests (isolation)

| File | Line | Value | Purpose |
|------|------|-------|---------|
| `tests/backtesting/unit/test_sensitivity.py` | 144 | `1` | Single-threaded unit test |
| `tests/backtesting/integration/test_block9b_ga.py` | 436 | `1` | Single-threaded integration test |
| `tests/backtesting/integration/test_block9a_orchestrator.py` | 96, 138, 171 | `1` | Single-threaded integration tests |
| `tests/backtesting/integration/test_robustness.py` | 92 | `2` | E2E test |
| `tests/backtesting/integration/test_robustness.py` | 461, 527 | `6` | Function param defaults |
| `tests/backtesting/integration/test_adversarial_suite.py` | 98 | `2` (`AV_MAX_WORKERS`) | Adversarial test smoke config |
| `tests/backtesting/integration/test_live_pipeline.py` | 111, 918 | `1` | Single-threaded test |
| `tests/backtesting/integration/test_e2e_wbws_real_data.py` | 338 | `2` | Test isolation |

### Summary

| Mechanism | Count of occurrences | Where |
|-----------|---------------------|-------|
| **Read from config** (with default `6`) | 5 | orchestrator.py (×3), ga_engine.py, test_performance.py |
| **Function param default** (`6`) | 4 | wfo_engine.py, sensitivity.py (×2), test_robustness.py (×2) |
| **Hardcoded in YAML configs** | 8 | backtest_V1_*.yaml (×5), backtest_template.yaml, backtest_1st_run.yaml |
| **Hardcoded in data scripts** | 2 | update_raw_ticks.py, download_raw_ticks.py (`= 50`) |
| **Hardcoded in tests** | 11 | Various test files (`1`, `2`, or `6`) |
| **Documentation only** | 22+ | docs/, comments in source |

**Answer:** `max_workers` is **both** read from config and hardcoded. The production code path reads it from `config["run"]["max_workers"]` with a fallback default of `6`. Individual YAML config files override this to `2` or `4` as OOM guards. The data download scripts use a separate hardcoded constant of `50`. Tests hardcode values of `1` or `2` for isolation.

---

# Agent C — OHLCV Data Import Chain Report

**Date:** 2026-04-03
**Scope:** `data/processed/ohlcv/` → `strategy_runner.evaluate()`
**Agent:** C
**Env:** Production (read-only)

---

## 1. Files in data/processed/ohlcv/

All files are `.parquet` format for instrument `DEUIDXEUR` (DAX) across multiple timeframes:

| File | Timeframe | Date Range |
|------|-----------|------------|
| `DEUIDXEUR_1s_20230101_20260301.parquet` | 1 second | 2023-01-01 → 2026-03-01 |
| `DEUIDXEUR_1min_20221201_20260301.parquet` | 1 minute | 2022-12-01 → 2026-03-01 |
| `DEUIDXEUR_5min_20221201_20260301.parquet` | 5 minute | 2022-12-01 → 2026-03-01 |
| `DEUIDXEUR_10min_20221201_20260301.parquet` | 10 minute | 2022-12-01 → 2026-03-01 |
| `DEUIDXEUR_15min_20221201_20260301.parquet` | 15 minute | 2022-12-01 → 2026-03-01 |
| `DEUIDXEUR_30min_20221201_20260301.parquet` | 30 minute | 2022-12-01 → 2026-03-01 |
| `DEUIDXEUR_1h_20221201_20260301.parquet` | 1 hour | 2022-12-01 → 2026-03-01 |
| `DEUIDXEUR_4h_20221201_20260301.parquet` | 4 hour | 2022-12-01 → 2026-03-01 |
| `DEUIDXEUR_1d_20221201_20260301.parquet` | 1 day | 2022-12-01 → 2026-03-01 |
| `DEUIDXEUR_1ME_20210101_20260301.parquet` | 1 month | 2021-01-01 → 2026-03-01 |

**No subdirectories.** All 10 files are flat in the root of `data/processed/ohlcv/`.

---

## 2. Import chain from OHLCV file on disk to backtester evaluation

### Step-by-step chain

1. **`scripts/runners/run_backtester.py`** → `main()` → reads config YAML via `yaml.safe_load()` → calls `run_pipeline(config_path)`
   - **What is passed:** `config_path` (Path to config YAML, e.g. `configs/backtesting/backtest_template.yaml`)

2. **`src/backtesting/orchestrator.py`** → `run(config_path)` → calls `_load_and_validate_config(config_path)` → calls `_execute_pipeline(config, store, run_metadata)`
   - **What is passed:** parsed `config` dict, `CandidateStore`, `RunMetadata`

3. **`src/backtesting/orchestrator.py`** → `_run_stage_3_random_search(config, store, run_metadata)` (line 517) → imports `from src.backtesting.strategy_runner import evaluate`
   - **What is passed:** `config` dict, `store`, `run_metadata`, `base_yaml_path`, `temp_dir`, `scenario`

4. **`src/backtesting/orchestrator.py`** → `_run_stage_3_random_search()` (line 567) → calls `evaluate(candidate, base_yaml_path, temp_dir, min_significant_trades, retain_temp_yamls)`
   - **What is passed:** `CandidateParameterSet`, base YAML Path, temp dir Path, trade threshold, retain flag

5. **`src/backtesting/strategy_runner.py`** → `evaluate()` (line 115) → calls `_write_temp_yaml(candidate, base_yaml_path, output_path)`
   - **What is passed:** candidate parameters written into a new YAML at `temp/candidate_{id}.yaml`

6. **`src/backtesting/strategy_runner.py`** → `evaluate()` (line 169) → calls `StrategyConfig.from_yaml(yaml_path)`
   - **What is passed:** `yaml_path` (temp candidate YAML)
   - **What happens:** `StrategyConfig.from_yaml()` (in `src/strategies/config/config_schema.py`) parses the YAML, resolves `data.paths.strategy_ohlcv`, `data.paths.ltf_ohlcv`, `data.paths.htf_ohlcv`, `data.paths.artf_ohlcv` as `Path` objects. These paths point to files in `data/processed/ohlcv/` (e.g. `data/processed/ohlcv/DEUIDXEUR_1min_20221201_20260301.parquet`). Returns a frozen `StrategyConfig` dataclass.

7. **`src/backtesting/strategy_runner.py`** → `evaluate()` (line 172) → calls `StrategyOrchestrator(strategy_config, cache_manager=cache_manager)` → constructor stores config and cache manager
   - **What is passed:** `StrategyConfig` (with resolved OHLCV paths), `CacheManager`

8. **`src/backtesting/strategy_runner.py`** → `evaluate()` (line 173) → calls `orchestrator.run(mode_override="core")`
   - **What is passed:** mode string `"core"`

9. **`src/strategies/orchestrator.py`** → `StrategyOrchestrator.run()` (line ~200) → calls `self._load_data(effective_mode)`
   - **What is passed:** mode string `"core"`

10. **`src/strategies/orchestrator.py`** → `_load_data(mode)` (line 308) → instantiates `DataLoader(config=self._config, mode=mode)` → calls `loader.load_data()`
    - **What is passed:** `StrategyConfig` (contains `data.paths.*` with resolved OHLCV file paths), mode string

11. **`src/strategies/core/data_loader.py`** → `DataLoader.__init__()` (line 106) → calls `self._build_data_config()` → converts `StrategyConfig.data.paths` into `DataFileConfig` objects with `Path` references to actual `.parquet` files
    - **What is produced:** `DataConfig` with `DataFileConfig(path=Path("data/processed/ohlcv/DEUIDXEUR_*.parquet"), format="parquet")`

12. **`src/strategies/core/data_loader.py`** → `DataLoader.load_data()` (line 468) → calls `self._load_file_with_cache(self.data_config.strategy_data, "strategy", apply_date_range=False)`
    - **What is passed:** `DataFileConfig` pointing to the strategy OHLCV parquet file

13. **`src/strategies/core/data_loader.py`** → `_load_file_with_cache()` (line 256) → checks disk cache (`~/.wbws_data_cache/{cache_key}.pkl`) → on miss, calls `pd.read_parquet(file_path)` (line 299) or `pd.read_csv(file_path)` (line 293)
    - **What is passed:** `file_path` = absolute path to `.parquet` file in `data/processed/ohlcv/`
    - **What is produced:** `pd.DataFrame` with OHLCV data, indexed by timestamp

14. **`src/strategies/core/data_loader.py`** → `_load_file_with_cache()` → applies date-range slicing if configured → saves sliced result to cache via `_save_to_cache()` → returns `pd.DataFrame`

15. **`src/strategies/core/data_loader.py`** → `load_data()` → repeats steps 12-14 for HTF, LTF, and ARTF files → assembles `DataBundle(full=df_full, strategy=df_strategy, htf=df_htf, ltf=df_ltf, artf=df_artf, info=..., validation=..., config=...)`
    - **What is produced:** `DataBundle` — a typed container with all OHLCV DataFrames

16. **`src/strategies/orchestrator.py`** → `_load_data()` (line 318) → returns `DataBundle` to `run()` method

17. **`src/strategies/orchestrator.py`** → `run()` → calls `self._generate_signals(data_bundle, mode)` → `self._run_filters(filter_result, data_bundle, mode)` → `self._simulate_trades(filter_result, data_bundle, mode)` → `calculate_metrics(trade_result)`
    - **What is passed:** `DataBundle` flows through each stage; `TradeSimulator` receives `data_bundle.full` and `data_bundle.artf`

18. **`src/strategies/orchestrator.py`** → `run()` → returns `OrchestratorResult(metrics=MetricsReport, trade_result=TradeResult, ...)`

19. **`src/backtesting/strategy_runner.py`** → `evaluate()` (line 175) → extracts `metrics = result.metrics`, `trades = result.trade_result`, `total_trades = metrics.total_trades` → applies significance guard → returns `CandidateResult(candidate_id, metrics, trades, total_trades)`
    - **What is produced:** `CandidateResult` — the final evaluation result consumed by the backtester

### Summary chain (condensed)

```
[scripts/runners/run_backtester.py] main()
  → [src/backtesting/orchestrator.py] run(config_path)
    → [src/backtesting/orchestrator.py] _run_stage_3_random_search()
      → [src/backtesting/strategy_runner.py] evaluate(candidate, ...)
        → [src/backtesting/strategy_runner.py] _write_temp_yaml() → writes temp YAML with OHLCV paths
        → [src/strategies/config/config_schema.py] StrategyConfig.from_yaml() → resolves OHLCV Paths
        → [src/strategies/orchestrator.py] StrategyOrchestrator.run()
          → [src/strategies/orchestrator.py] _load_data()
            → [src/strategies/core/data_loader.py] DataLoader.__init__() → builds DataFileConfig
            → [src/strategies/core/data_loader.py] _load_file_with_cache() → pd.read_parquet()
            → [src/strategies/core/data_loader.py] load_data() → returns DataBundle
          → [src/strategies/orchestrator.py] _generate_signals() → _run_filters() → _simulate_trades() → calculate_metrics()
          → returns OrchestratorResult
        → [src/backtesting/strategy_runner.py] evaluate() → returns CandidateResult
      → [src/backtesting/orchestrator.py] stores result in CandidateStore
```

---

## 3. Caching of loaded data between runs and between candidates

### Yes — two layers of caching exist.

### Layer 1: DataLoader disk cache (`~/.wbws_data_cache/`)

**Where:** `src/strategies/core/data_loader.py` — `_load_file_with_cache()`, `_load_cached_data()`, `_save_to_cache()`

**What is cached:**
- Full or sliced `pd.DataFrame` objects (OHLCV data) serialized as `.pkl` (pickle) files
- Cache key is an MD5 hash of: file path, file size, file mtime, version tag (`v3.3`), and optional date range
- Separate cache keys for sliced strategy files (B9O-001): `_get_sliced_cache_key()` uses file path + mtime + date_range_str

**Cache location:** `~/.wbws_data_cache/{cache_key}.pkl`

**Eviction mechanism:**
- **No automatic eviction.** Cache files persist indefinitely on disk.
- **Manual clean:** The runner's pre-run clean (`scripts/runners/run_backtester.py`, via `src/utils/run_cleaner.py`) deletes all `~/.wbws_data_cache/*.pkl` files before each pipeline run (unless `--no-clean` is passed). This is documented as B9O-002 — prevents OOM from stale full-file pickle entries.
- **Version invalidation:** The cache key includes a version tag (`v3.3`). Bumping this tag invalidates all prior entries. This was used for B9O-006 (v3.1 → v3.3) to discard old full-file cache entries that stored unsliced DataFrames.
- **Corruption handling:** `_load_cached_data()` (line 207) catches `Exception` on `pickle.load()`, deletes the corrupted file, and returns `None`.

**Scope:** Shared across all candidates within a run and across runs (until cleaned). If two candidates use the same OHLCV file and date range, they share the same cached DataFrame.

### Layer 2: CacheManager (in-memory, per-orchestrator)

**Where:** `src/strategies/core/cache_manager.py`

**What is cached:**
- `_atr_cache`: ATR series (pd.Series) keyed by stable fingerprint — used by `RiskManager`
- `_annual_range_cache`: Annual range series (pd.Series) keyed by stable fingerprint — used by `RiskManager`
- `_spread_config_cache`: Spread configuration dicts keyed by stable fingerprint — used by `SpreadManager`

**Eviction mechanism:**
- `cache_manager.clear_all_caches()` is called in the `finally` block of every `strategy_runner.evaluate()` call (line 207 of `strategy_runner.py`).
- This clears all three in-memory caches and resets statistics to zero.
- **Scope:** A single `CacheManager` instance is created per `evaluate()` call in `strategy_runner.py` (line 158: `cache_manager = CacheManager()`). It is passed to `StrategyOrchestrator` and shared across all pipeline stages within one candidate evaluation. It is cleared after each candidate evaluation completes.

### Summary

| Cache | Location | What | Scope | Eviction |
|-------|----------|------|-------|----------|
| **DataLoader disk cache** | `~/.wbws_data_cache/*.pkl` | Full/sliced OHLCV DataFrames | Cross-candidate, cross-run | Manual clean via `run_cleaner.clean_environment()` (B9O-002); version tag bump; corruption auto-delete |
| **CacheManager (in-memory)** | `CacheManager` instance | ATR series, annual range series, spread configs | Single candidate evaluation | `clear_all_caches()` called in `evaluate()` finally block after every candidate |

**Key finding:** The DataLoader disk cache is the only cache that persists **between runs**. It is cleaned by the runner's pre-run clean (B9O-002). The CacheManager is ephemeral — created and destroyed per candidate evaluation.

---

# Agent C — Phantom Verdict Gate Impact Report

**Date:** 2026-04-03
**Scope:** `src/backtesting/evaluation/verdict.py`, `src/backtesting/contracts.py`, `src/backtesting/report_generator.py`
**Agent:** C
**Env:** Production (read-only)

---

## 1. Where is the verdict decision made in verdict.py?

**Function:** `compute_verdict()`
**File:** `src/backtesting/evaluation/verdict.py`
**Line:** 57
**Signature:**
```python
def compute_verdict(
    candidate_id: str,
    wfo_score: WFOConsistencyScore,
    mc_result: MCResult,
    sensitivity: SensitivityProfile,
    scenario: ScenarioProfile,
    oos_gate_enabled: bool,
) -> VerdictResult:
```

**Fields of `WFOConsistencyScore` currently read:**

| Field | Line | Usage |
|-------|------|-------|
| `composite_score` | 82 | Extracted as `wfo_composite` — Pillar 1 value |
| `oos_gate_triggered` | 113 | Extracted as modifier flag: `oos_gate_triggered = oos_gate_enabled and wfo_score.oos_gate_triggered` |
| `window_collapse_flag` | 114 | Extracted as modifier flag |
| `median_oos_delta` | 156 | Read for informational field on `VerdictResult` |

**Fields of `WFOConsistencyScore` NOT currently read:**
- `windows_evaluated` — present but never accessed
- `windows_total` — present but never accessed
- `median_window_return` — present but never accessed
- `window_return_variance` — present but never accessed
- `worst_window_drawdown` — present but never accessed
- `fraction_positive_windows` — present but never accessed

**Verdict decision logic** (lines 118–124):
```python
if wfo_pillar_no_go or mc_pillar_no_go:
    verdict = Verdict.NO_GO
elif wfo_pillar_go and mc_pillar_go and not any_modifier_flag:
    verdict = Verdict.AUTO_GO
else:
    verdict = Verdict.BORDERLINE
```

There is **no check** on `wfo_score.windows_evaluated` anywhere in the function.

---

## 2. Does WFOConsistencyScore contain a windows_evaluated field?

**Yes.**

**File:** `src/backtesting/contracts.py`
**Line:** 440
**Field definition:**
```python
windows_evaluated: int
```

It is the second field in the dataclass (after `candidate_id`). It is validated in `__post_init__` (line 456) to ensure it is in `[0, windows_total]`.

**All fields present on `WFOConsistencyScore`** (lines 439–453):

| Field | Type | Line |
|-------|------|------|
| `candidate_id` | `str` | 439 |
| `windows_evaluated` | `int` | 440 |
| `windows_total` | `int` | 441 |
| `median_window_return` | `float` | 442 |
| `window_return_variance` | `float` | 443 |
| `worst_window_drawdown` | `float` | 444 |
| `fraction_positive_windows` | `float` | 445 |
| `composite_score` | `float` | 446 |
| `oos_gate_triggered` | `bool` | 447 |
| `window_collapse_flag` | `bool` | 448 |
| `median_oos_delta` | `Optional[float]` | 453 |

---

## 3. Does VerdictResult contain windows_evaluated or insufficient_coverage?

**`windows_evaluated`:** No. `VerdictResult` does not contain this field.

**`insufficient_coverage`:** No. This term does not appear anywhere in `VerdictResult` or the `Verdict` enum.

**All fields present on `VerdictResult`** (lines 541–560):

| Field | Type | Line |
|-------|------|------|
| `candidate_id` | `str` | 542 |
| `scenario_name` | `str` | 543 |
| `verdict` | `Verdict` | 544 |
| `deployment_status` | `DeploymentStatus` | 545 |
| `wfo_consistency_score` | `Optional[float]` | 547 |
| `mc_deep_ruin_probability` | `Optional[float]` | 548 |
| `sensitivity_spike` | `bool` | 550 |
| `oos_gate_triggered` | `bool` | 551 |
| `window_collapse_flag` | `bool` | 552 |
| `sensitivity_profile_incomplete` | `bool` | 553 |
| `median_oos_delta` | `Optional[float]` | 555 |
| `parameter_region_width` | `Optional[float]` | 556 |
| `yaml_output_path` | `Optional[str]` | 557 |
| `evidence_summary` | `str` | 559 |

---

## 4. All call sites of compute_verdict() in src/backtesting/

**Single call site:**

| File | Line | Arguments passed |
|------|------|-----------------|
| `src/backtesting/orchestrator.py` | 1239 | `candidate_id=candidate_id`, `wfo_score=wfo_score`, `mc_result=mc_result`, `sensitivity=sensitivity`, `scenario=scenario`, `oos_gate_enabled=oos_gate_enabled` |

**Context** (lines 1221–1246): The call is inside `_run_stage_7_verdict()`. It is guarded by three `None` checks:
- Line 1222: `if wfo_score is None: continue`
- Line 1227: `if mc_result is None: continue`
- Line 1232: `if sensitivity is None: sensitivity = _neutral_sensitivity(candidate_id)`

There is **no guard** on `wfo_score.windows_evaluated` before the call.

**Import site:**
| File | Line | Code |
|------|------|------|
| `src/backtesting/orchestrator.py` | 63 | `from src.backtesting.evaluation.verdict import compute_verdict` |

---

## 5. Where verdict results are rendered in report_generator.py

### Verdict enum value rendered to HTML

| Line | Context | Code |
|------|---------|------|
| 223 | Counting AUTO_GO candidates | `if _get(v, "verdict") == Verdict.AUTO_GO.value` |
| 224 | Counting BORDERLINE candidates | `if _get(v, "verdict") == Verdict.BORDERLINE.value` |
| 225 | Counting NO_GO candidates | `if _get(v, "verdict") == Verdict.NO_GO.value` |
| 369 | Verdict table — badge class | `verdict_val = _get(v, "verdict", "")` |
| 370 | Verdict table — badge class | `badge_cls = f"badge-{verdict_val}"` |
| 378 | Verdict table — badge text | `<span class="badge {badge_cls}">{verdict_val}</span>` |
| 402 | Candidate detail card — verdict value | `verdict_val = _get(v, "verdict", "no_go")` |
| 403 | Candidate detail card — card class | `card_cls = f"verdict-{verdict_val}"` |
| 404 | Candidate detail card — badge class | `badge_cls = f"badge-{verdict_val}"` |
| 429 | Candidate detail card — badge text | `<span class="badge {badge_cls}">{verdict_val}</span>` |

### WFO score rendered to HTML

| Line | Context | Code |
|------|---------|------|
| 361 | Verdict table — WFO score extraction | `wfo = _get(v, "wfo_consistency_score") or 0.0` |
| 371 | Verdict table — WFO score display | `{f"{wfo:.3f}" if wfo is not None else "—"}` |
| 406 | Candidate detail card — WFO score extraction | `wfo = _get(v, "wfo_consistency_score")` |

### CSS styling for verdict types

| Line | CSS class | Color |
|------|-----------|-------|
| 250 | `.verdict-auto_go` | `#27ae60` (green left border) |
| 251 | `.verdict-borderline` | `#f39c12` (orange left border) |
| 252 | `.verdict-no_go` | `#e74c3c` (red left border) |
| 255 | `.badge-auto_go` | `#27ae60` (green background) |
| 256 | `.badge-borderline` | `#f39c12` (orange background) |
| 257 | `.badge-no_go` | `#e74c3c` (red background) |

**Note:** The verdict value is rendered dynamically via `f"badge-{verdict_val}"` and `f"verdict-{verdict_val}"`. A new verdict type like `insufficient_coverage` would require corresponding CSS classes (`.badge-insufficient_coverage`, `.verdict-insufficient_coverage`) to be added.

---

## 6. Is INSUFFICIENT_COVERAGE already present in the Verdict enum?

**No.**

**File:** `src/backtesting/contracts.py`
**Lines:** 56–59
**Current enum members:**
```python
class Verdict(Enum):
    AUTO_GO    = "auto_go"
    BORDERLINE = "borderline"
    NO_GO      = "no_go"
```

The term `INSUFFICIENT_COVERAGE` (or `insufficient_coverage`) does not appear anywhere in `src/backtesting/`. A grep search across the entire `src/backtesting/` directory returned zero matches.
