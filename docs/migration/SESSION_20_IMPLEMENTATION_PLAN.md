# SESSION 20 — IMPLEMENTATION PLAN
**Date**: 2026-02-19 | **Phase**: 8 — Hardening & Polish  
**Constraint**: Chat window size limits — each work block must be self-contained  
**Goal**: Execute all P0 + P1 fixes. Reach ~302 tests. Resolve performance inversion.

---

## THE GOLDEN RULE FOR THIS SESSION

> Every work block produces **committed, runnable code** before moving to the next.  
> Partial fixes are worse than no fixes — a half-renamed `"debug"` is a runtime bomb.  
> When context is running low, **finish the current block, write the handoff, stop**.

---

## CONSTRAINT MANAGEMENT STRATEGY

Each chat window has finite context. The plan below is divided into **self-contained work blocks**.  
Each block:
- Has a clear start state ("what files are clean going in")
- Has a clear end state ("what tests prove it's done")
- Produces a **mini-handoff note** at its end (copy this to the next chat's first message)

**If a session ends mid-block**: Write `STATUS: PARTIAL — stopped at [step X]` in the handoff.  
**Never start block N+1 if block N is partial.**

---

## WORK BLOCKS — EXECUTION ORDER

```
Block A  │ Global rename "debug" → "analytics"          │ ~45 min │ P0
Block B  │ Delete all legacy adapters                    │ ~30 min │ P0+P1
Block C  │ Create strategy_template.yaml                 │ ~45 min │ P0
Block D  │ Fix filter pipeline (logging + cache key)     │ ~90 min │ P0
Block E  │ Fix core mode performance (TradeSimulator)    │ ~90 min │ P0
Block F  │ Add caching (RiskManager + SpreadManager)     │ ~60 min │ P0
Block G  │ Fix config validation + freeze contracts      │ ~60 min │ P0+P1
Block H  │ Performance optimizations (filters)           │ ~45 min │ P1
Block I  │ ReportGenerator polish                        │ ~60 min │ P1
Block J  │ Write ~30 tests                               │ ~90 min │ coverage
Block K  │ Update all four architecture docs             │ ~30 min │ docs
```

**Total estimated effort**: ~9.5 hours across multiple chats  
**Minimum viable session**: Blocks A–F (core correctness + performance)

---

## BLOCK A — Global Rename: "debug" → "analytics"

**Why first**: Every subsequent block touches mode strings. A partial rename creates inconsistency bugs.  
**What it is**: Search-and-replace + migration guard. One pass touches all 35 files.

### Files to touch
```
src/strategies/specific/modules/data_loader.py
src/strategies/specific/modules/signal_generator.py
src/strategies/specific/modules/filter_pipeline.py
src/strategies/specific/modules/trade_simulator.py
src/strategies/specific/modules/trade_manager.py
src/strategies/specific/modules/risk_manager.py
src/strategies/specific/modules/spread_manager.py
src/strategies/specific/modules/metrics_calculator.py
src/strategies/specific/modules/trade_analytics.py
src/strategies/specific/modules/report_generator.py
src/strategies/specific/filters/*.py  (all 10 filters)
src/strategies/contracts/*.py          (all contracts)
src/config/config_schema.py
src/utils/structured_logger.py
src/indicators/wbws_trigger.py
```

### Exact changes

**In every module** — rename the constant:
```python
# BEFORE
DEFAULT_MODE = "debug"
mode: str = "debug"
if mode == "debug":

# AFTER
DEFAULT_MODE = "analytics"
mode: str = "analytics"
if mode == "analytics":
```

**Add migration guard in DataLoader and SignalGenerator**:
```python
def __init__(self, ..., mode: str = "analytics"):
    if mode == "debug":
        raise ValueError(
            "Mode 'debug' has been renamed to 'analytics' in the new architecture. "
            "Update your config: execution.mode: analytics"
        )
    valid_modes = {"core", "analytics"}
    if mode not in valid_modes:
        raise ValueError(f"Invalid mode '{mode}'. Must be one of: {valid_modes}")
    self._mode = mode
```

**Update LogStage enum** in `structured_logger.py`:
```python
class LogStage(Enum):
    # ... existing stages ...
    ANALYTICS = "analytics"     # was DEBUG
    REPORTING = "reporting"     # add missing Phase 5 stage
```

### Done when
- `grep -r '"debug"' src/strategies/` returns zero results
- `grep -r '"debug"' src/config/` returns zero results  
- `python -c "from src.strategies.specific.modules.data_loader import DataLoader"` succeeds
- Instantiating with `mode="debug"` raises `ValueError` with migration message

---

## BLOCK B — Delete All Legacy Adapters

**Why second**: Clean codebase before building on it. Adapters create confusion about which code path is authoritative.

### Deletions (DEC-021 enforcement)

| File | Delete |
|------|--------|
| `filter_contracts.py` | `pipeline_result_to_old_format()` + `old_format_to_pipeline_result()` |
| `signal_generator.py` | Entire `SignalGeneratorAdapter` class |
| `data_contracts.py` | `DataConfig.from_yaml_config()` classmethod |
| `trade_contracts.py` | `RejectedSignal.to_legacy_trade_dict()` method |
| `trade_manager.py` | `handle_signal_legacy()` + `compute_trade_parameters_legacy()` |

### Verify no callers remain
```bash
grep -r "pipeline_result_to_old_format\|old_format_to_pipeline_result" src/ tests/
grep -r "SignalGeneratorAdapter" src/ tests/
grep -r "from_yaml_config" src/ tests/
grep -r "to_legacy_trade_dict" src/ tests/
grep -r "handle_signal_legacy\|compute_trade_parameters_legacy" src/ tests/
```
All should return zero.

### Also: Remove `self.signals_df` from WBWSTrigger (DEC-025)
```python
# wbws_trigger.py
# REMOVE: self.signals_df = result
# REMOVE: def get_signals(self) -> pd.DataFrame: ...
# Keep: calculate_signals() returns result directly
```

### Done when
- All grep searches return zero
- `pytest tests/migration/ -x -q` still passes (no broken imports from deleted code)

---

## BLOCK C — Create `configs/strategy_template.yaml`

**Resolves**: P0-CH0-1 — `StrategyConfig` never tested end-to-end.

### File location
`configs/strategy_template.yaml`

### Structure (derived from DEC-023 + config_schema.py analysis)
```yaml
# ============================================================
# WBWSStrategy — New Architecture Config Template
# Version: 1.0.0 | Created: Session 20
# Generic template — copy and customize per strategy
# ============================================================

# --- Data ---
data:
  paths:
    strategy_ohlcv: "data/processed/ohlcv/INSTRUMENT_TF.parquet"
    htf_ohlcv: null          # Optional: "data/processed/ohlcv/INSTRUMENT_HTF.parquet"
    ltf_ohlcv: null          # Optional: "data/processed/ohlcv/INSTRUMENT_LTF.parquet"
    artf_ohlcv: null         # Optional: monthly bars
  date_range:
    start: "2023-01-01 00:00:00"
    end: "2024-12-31 23:59:59"
  timezone: "CET"            # Informational only — data is not converted (DEC-035)
  validate_on_load: false    # Set true in tests; false in production (DEC-003)

# --- Execution ---
execution:
  mode: "analytics"          # "core" = max speed | "analytics" = full pipeline
  # mode: "core" for multi-run backtester; never use "debug" (deprecated)

# --- Trade Management ---
trade_management:
  risk:
    atr_length: 14
    max_risk_percentile: 0.5   # % of annual range; valid 0 < value <= 5.0
    min_rr_ratio: 1.5
  spread:
    enabled: false
    spread_value: 0.0          # In points; must be > 0 when enabled: true
  position:
    max_concurrent_trades: 1
    
# --- Signal (Strategy-Specific — fill per strategy) ---
signal:
  # WBWS-specific parameters go here
  # Example: wbws_length: 20
  _placeholder: "Replace with strategy signal parameters"

# --- Filters ---
filters:
  time:
    enabled: true
    sessions:
      london: { start: "07:00", end: "16:00", enabled: true }
      new_york: { start: "13:00", end: "21:00", enabled: true }
      asia: { start: "00:00", end: "08:00", enabled: false }
    excluded_days: []         # e.g. ["Saturday", "Sunday"]
  pipeline:
    filter_sequence: []       # Order of technical filters; empty = config order
    filters: {}               # Technical filter configs (adx, rsi, etc.)

# --- Output ---
output:
  reports:
    enabled: true
    output_dir: "outputs/strategies/reports"
    theme: "dark"             # "dark" | "light"
    chart_height_px: 300
    brand_name: "Strategy"   # Shown in report header
    include_raw_data: true
  logging:
    level: "INFO"             # DEBUG | INFO | WARNING | ERROR
    output_dir: "outputs/strategies/logs"
```

### Also: Fix `max_risk_percentile` validation in `config_schema.py`
```python
# BEFORE: 0 < value <= 100
# AFTER:
if not (0 < value <= 5.0):
    raise ValueError(
        f"max_risk_percentile must be between 0 and 5.0, got {value}. "
        f"This represents a percentage of annual range (typical values: 0.1–1.0)."
    )
if value > 1.0:
    logger.warning(
        f"max_risk_percentile={value} is unusually high (>1.0% of annual range). "
        f"Verify this is intentional."
    )
```

### Done when
- `StrategyConfig.from_yaml(Path("configs/strategy_template.yaml"))` succeeds
- `StrategyConfig.from_yaml(bad_path)` raises with clear message
- `max_risk_percentile=150` raises `ValueError`
- `max_risk_percentile=2.0` logs warning

---

## BLOCK D — Fix Filter Pipeline: Logging + Cache Key

**Resolves**: P0-CH3-2 (unconditional logging), P0-E2 (50% cache hit rate)

### Part 1 — Gate all logging on analytics mode

In `filter_pipeline.py`, every `logger.info()` call must be gated:
```python
# BEFORE
logger.info(f"FilterPipeline: processed {n} signals in {ms:.1f}ms")

# AFTER
if self._mode == "analytics":
    logger.info(f"FilterPipeline: processed {n} signals in {ms:.1f}ms")
```

Also fix the broken final log (currently logs empty string in core mode):
```python
# Find the final log at pipeline exit — ensure it logs the actual result summary
if self._mode == "analytics":
    logger.info(
        f"FilterPipeline complete: {result.final_count}/{result.raw_count} signals "
        f"passed in {result.execution_time_ms:.1f}ms"
    )
```

### Part 2 — Fix cache key to include filter config fingerprint (DEC-026)

In `FilterPipeline.__init__`:
```python
def __init__(self, config: StrategyConfig, mode: str = "analytics"):
    # ... existing init ...
    self._filter_cfg_hash = self._compute_filter_config_hash(config)

def _compute_filter_config_hash(self, config: StrategyConfig) -> str:
    """Stable hash of filter configuration for cache key uniqueness."""
    import hashlib, json
    # Include: which filters are enabled, and their parameters
    filter_state = {
        name: {"enabled": fcfg.enabled, "params": fcfg.config}
        for name, fcfg in config.filters.filters.items()
    }
    # Sort keys for stability
    serialized = json.dumps(filter_state, sort_keys=True, default=str)
    return hashlib.md5(serialized.encode()).hexdigest()[:12]
```

In `cache.py`, update `compute_cache_id()`:
```python
def compute_cache_id(
    self, 
    data_bundle: DataBundle, 
    filter_cfg_hash: str  # NEW parameter
) -> str:
    # Existing: mtime + size + version
    # Add: filter_cfg_hash to prevent cross-config collisions
    base = f"{data_mtime}_{data_size}_{self._version}_{filter_cfg_hash}"
    return hashlib.md5(base.encode()).hexdigest()
```

### Done when
- In core mode: `logger.info` calls = 0 (verify via log capture in test)
- Second pipeline call with same config = cache HIT (verify cache.hits == 1)
- Second pipeline call with different config = cache MISS (new hash)
- `pytest tests/migration/test_filter_pipeline.py -v` passes

---

## BLOCK E — Fix Core Mode Performance (TradeSimulator)

**Resolves**: P0-E1 (core 26% slower than analytics), P0-CH4-1, P0-CH4-2  
**Target**: Core mode total < 12,000ms (from 42,680ms)

### Root cause
LTF (Lower TimeFrame) tick data is preloaded and processed unconditionally in `trade_simulator.py`, even in core mode where LTF execution is not needed.

### Fix: Add `mode` parameter, gate LTF on analytics mode

```python
# trade_simulator.py
def simulate_trades(
    self,
    signal_frame: SignalFrame, 
    data_bundle: DataBundle,
    mode: str = "core",        # NEW — was: verbose: bool = False
) -> TradeResult:
    
    if mode == "debug":
        raise ValueError("Mode 'debug' is deprecated. Use 'analytics'.")
    
    start = perf_counter()
    
    # Gate expensive LTF precomputation
    ltf_data = None
    if mode == "analytics" and data_bundle.has_ltf():
        ltf_data = self._preprocess_ltf(data_bundle.ltf)
        # Previously ran unconditionally — this alone removes ~15s in core mode
    
    # Gate progressive tracking
    use_progressive = (mode == "analytics")
    
    # Gate signal_id lookups (not needed in core)
    use_signal_ids = (mode == "analytics")
    
    results = self._run_simulation(
        signal_frame=signal_frame,
        data_bundle=data_bundle,
        ltf_data=ltf_data,
        use_progressive=use_progressive,
        use_signal_ids=use_signal_ids,
    )
    
    return TradeResult(
        ...,
        execution_mode=mode,
        execution_time_ms=(perf_counter() - start) * 1000,
    )
```

### Expected impact breakdown
| Fix | Estimated Savings |
|-----|------------------|
| Gate LTF precomputation | ~15,000ms |
| Gate progressive tracking | ~8,000ms |
| Gate signal_id lookups | ~3,000ms |
| Remove unconditional logging | ~1,000ms |
| **Total expected** | **~27,000ms** |
| **Expected core mode** | **~15,000ms** |

> Note: Target is <12,000ms. If 15,000ms is the result after E, carry remaining optimization to Block F (ATR caching).

### Done when
- Core mode total duration < 20,000ms (immediate check — further gains from Block F)
- Analytics mode still produces identical trade results
- `TradeResult.trades` counts match between modes on same data

---

## BLOCK F — Add Caching (RiskManager + SpreadManager)

**Resolves**: P0-CH4-3 (ATR repeats every run), P0-CH4-4 (YAML loaded every time)

### RiskManager — ATR cache

```python
# risk_manager.py
class RiskManager:
    _atr_cache: ClassVar[Dict[str, np.ndarray]] = {}   # class-level
    
    def _get_atr(self, prices: pd.DataFrame, length: int) -> np.ndarray:
        cache_key = f"{id(prices)}_{length}_{len(prices)}"
        if cache_key not in RiskManager._atr_cache:
            RiskManager._atr_cache[cache_key] = self._compute_atr(prices, length)
        return RiskManager._atr_cache[cache_key]
    
    @classmethod
    def clear_cache(cls) -> None:
        """Call between strategy runs in multi-run backtester."""
        cls._atr_cache.clear()
```

### SpreadManager — Config cache

```python
# spread_manager.py
class SpreadManager:
    _config_cache: ClassVar[Optional[Dict]] = None
    _config_path_cached: ClassVar[Optional[str]] = None
    
    def _load_spread_config(self, path: str) -> Dict:
        if (SpreadManager._config_cache is not None 
                and SpreadManager._config_path_cached == path):
            return SpreadManager._config_cache
        
        with open(path) as f:
            config = yaml.safe_load(f)
        SpreadManager._config_cache = config
        SpreadManager._config_path_cached = path
        return config
```

### Expected impact
- ATR caching: saves ~5,000ms on first multi-run set; 0 on single run (but correct architecture)
- Spread config: saves ~2,000ms per run (YAML parse eliminated)

### Done when
- Second `RiskManager` instance with same prices + ATR length = cache hit
- `SpreadManager` on second call with same path = no file I/O
- Core mode total < 12,000ms (combined with Block E)

---

## BLOCK G — Fix Config Validation + Freeze All Contracts

**Resolves**: P1-CH0-1, P1-CH1-1, P1-CH2-1, P1-CH5-1 (freeze violations)

### Freeze target list
```python
# config_schema.py — add frozen=True to:
@dataclass(frozen=True)
class DateRangeConfig: ...

@dataclass(frozen=True)  
class DataPathsConfig: ...

@dataclass(frozen=True)
class RiskConfig: ...

@dataclass(frozen=True)
class SpreadConfig: ...

# data_contracts.py
@dataclass(frozen=True)  # was missing
class DataBundle: ...

@dataclass(frozen=True)
class DataInfo: ...

@dataclass(frozen=True)
class DataValidationResult: ...

# signal_contracts.py
@dataclass(frozen=True)
class SignalFrame: ...

@dataclass(frozen=True)
class SignalStats: ...

# analytics_contracts.py
@dataclass(frozen=True)
class TradingSessionConfig: ...
```

### Fix: `object.__setattr__` workarounds in frozen dataclasses
When a frozen dataclass needs to compute derived fields in `__post_init__`:
```python
# PATTERN: Use __post_init__ with object.__setattr__ ONLY when field
# is derived from other fields at construction time
@dataclass(frozen=True)
class DataPathsConfig:
    strategy_ohlcv: Path
    # If path needs to be resolved:
    def __post_init__(self):
        # This is the ONE acceptable use of object.__setattr__ in frozen DC
        object.__setattr__(self, 'strategy_ohlcv', Path(self.strategy_ohlcv).resolve())
```

### Add `__iter__` guard to SignalFrame (DEC-024)
```python
# signal_contracts.py
def __iter__(self):
    if self.indicator_data is None:
        raise RuntimeError(
            "SignalFrame.__iter__ requires indicator_data (analytics mode only). "
            "In core mode, use iter_raw() which returns (timestamp, signal_code) tuples."
        )
    # ... existing iterator logic ...
```

### Done when
- All listed contracts have `frozen=True`
- `signal_frame.__iter__()` with `indicator_data=None` raises `RuntimeError`
- `pytest tests/migration/ -x -q` passes (no mutation-related test failures)

---

## BLOCK H — Performance Optimizations (Filters)

**Resolves**: P1-CH3-3, P1-CH3-5

### Replace `count_by_type()` in all 10 filter hot paths
```python
# In each filter's apply() method
# BEFORE (calls SignalFrame method, creates dict, two lookups):
total = signal_frame.count_by_type()["total"]

# AFTER (direct numpy, single operation):
total = int(np.sum(signal_frame.signals.values != 0))
```

### Remove 6 unused Bollinger indicator arrays
In `bollinger_filter.py`, identify and remove arrays that are computed but never used in the filter logic. Typical unused arrays: intermediate computation buffers that were kept from legacy code.

```python
# BEFORE: 6 arrays computed, only 2 used
ind["bb_upper"] = ...    # used
ind["bb_lower"] = ...    # used  
ind["bb_mid"] = ...      # NOT used in filter logic — remove
ind["bb_width"] = ...    # NOT used — remove
ind["bb_pct_b"] = ...    # NOT used — remove
ind["bb_std"] = ...      # NOT used — remove

# AFTER: only compute what's needed
ind["bb_upper"] = ...
ind["bb_lower"] = ...
```

### Done when
- `grep -r "count_by_type" src/strategies/specific/filters/` returns zero
- Bollinger memory footprint reduced (verify array count in test)
- Filter pipeline total runtime < 30ms (from baseline 65ms)

---

## BLOCK I — ReportGenerator Polish

**Resolves**: P1-CH6-1 through P1-CH6-5

### Add `brand_name` to `ReportConfig` (DEC-033)
```python
# report_contracts.py
@dataclass(frozen=True)
class ReportConfig:
    title: str = "Strategy Performance Report"
    output_dir: Path = Path("outputs/reports")
    include_raw_data: bool = True
    theme: str = "dark"
    chart_height_px: int = 300
    subtitle: Optional[str] = None
    brand_name: str = "Strategy"          # NEW — DEC-033
    timezone: str = "CET"                 # NEW — documentation only (DEC-035)
    offline_chart_fallback: bool = False  # NEW — P1-CH6-2
```

### Add consistency validation (DEC-034)
```python
# report_generator.py
@staticmethod
def generate(
    analytics_report: AnalyticsReport,
    trade_result: Optional[TradeResult] = None,
    config: Optional[ReportConfig] = None,
) -> GeneratedReport:
    
    config = config or ReportConfig()
    
    # Validate consistency
    if trade_result is not None:
        expected = analytics_report.input_metrics.total_trades
        actual = len(trade_result.trades)
        if expected != actual:
            logger.warning(
                f"ReportGenerator: trade_result has {actual} trades but "
                f"analytics_report expects {expected}. "
                f"Equity curve will be skipped to prevent misleading output."
            )
            trade_result = None  # Skip equity curve (P1-CH6-5)
    
    # Use brand_name in HTML header
    # Replace hardcoded "WBWSStrategy" with config.brand_name
```

### Update HTML template to use `config.brand_name`
In `_build_html()` and `_build_layer1_executive()`, replace:
```python
# BEFORE
html_header = "<h1>WBWSStrategy Performance Report</h1>"

# AFTER  
html_header = f"<h1>{config.brand_name} Performance Report</h1>"
```

### Done when
- `ReportConfig(brand_name="EURUSD")` produces HTML with "EURUSD" in h1
- Mismatched trade_result triggers warning and skips equity curve
- `ReportConfig(timezone="UTC")` accepted (informational only)

---

## BLOCK J — Write New Unit Tests

**File organization** — add to existing test files or create new ones:

```
tests/migration/test_config_schema.py         (7 new tests) - update
tests/migration/test_data_loader_s20.py           (4 new tests) - create
tests/migration/test_signal_contracts_s20.py      (4 new tests) - create
tests/migration/test_filter_pipeline_s20.py       (5 new tests) - create
```

### Test list (from SESSION_20_HANDOFF.md + additions)

**Config (7)**:
```python
test_mode_debug_raises_migration_error()
test_mode_analytics_accepted()
test_mode_core_accepted()
test_max_risk_percentile_above_5_raises()
test_max_risk_percentile_above_1_warns()
test_config_dataclasses_are_frozen()
test_filter_sequence_in_pipeline_config()
```
**Data Loader (4)**:
```python
test_load_config_does_not_override_mode()
test_cache_dir_uses_paths_module()
test_from_yaml_config_removed()
test_data_bundle_is_frozen()
```
**Signal Contracts (4)**:
```python
test_signal_frame_is_frozen()
test_signal_frame_iter_raises_in_core_mode()
test_signal_adapter_removed()
test_wbws_trigger_stateless()
```
**Filter Pipeline (5)**:
```python
test_cache_hit_rate_100_on_second_call_same_config()
test_cache_miss_on_different_filter_config()
test_core_mode_no_logger_info_calls()
test_bollinger_indicator_cache_size()
test_count_by_type_not_called_in_hot_path()
```
```

### Done when
- `pytest tests/migration/ -v --tb=short` shows >= all tests passing
- No MagicMock used in new tests (use real dataclasses)
- All new tests use `@pytest.mark.unit` decorator for easy filtering
---
## BLOCK K — Update Architecture Docs
### Files to update
**1. `docs/migration/DECISION_LOG.md`**  
Append DEC-036 through DEC-039 (already drafted in SESSION_19 handoff — copy them in).
**2. `docs/migration/PHASE8_SCAN_REPORT.md`**  
Mark all resolved P0/P1 items as ✅ with session reference:
```
✅ [P0-CH0-1] Fixed Session 20 — strategy_template.yaml created
✅ [P0-E1] Fixed Session 20 — LTF gated on analytics mode
```
**3. `docs/migration/SESSION_21_HANDOFF.md`** (NEW — create this)  
Template is in the next section of this document.
**4. `docs/architecture/ARCHITECTURE.md`**  
Add to Integration Guide section:
- Mode parameter in `simulate_trades()` call signature
- `ReportConfig.brand_name` and `ReportConfig.timezone` fields
- Remove any remaining references to `"debug"` mode
- Update version to 2.2.0
---
## SESSION 21 HANDOFF TEMPLATE

> Copy this block as the opening context of Session 21.

```
SESSION 21 CONTEXT
==================
Session 20 completed: [DATE]
All P0 issues resolved. All P1 issues resolved.
Test count: [N] (target was ~302)
Performance: Core mode [Xms] (target <12,000ms)

Files modified in Session 20:
- [list all modified files]

Session 21 focus: P2 + Observability
1. Add per-stage timing to TradeAnalytics output
2. Make insight thresholds configurable via AnalyticsConfig (DEC-032)
3. Add cache statistics to RiskManager and FilterPipeline
4. Add logging of chart data failures in ReportGenerator
5. Refine strategy_template.yaml with any remaining findings
6. Target: all tests in place

Carry-forward from Session 20:
- [list any partial items with STATUS: PARTIAL note]
- DEC-020: MagicMock cleanup still deferred to Session 22
Read before starting:
1. docs/migration/SESSION_21_HANDOFF.md
2. docs/migration/DECISION_LOG.md (DEC-036 to DEC-039)
3. docs/migration/PHASE8_SCAN_REPORT.md (check ✅ resolved items)
```
---

## PERFORMANCE TARGET TRACKER

Use this table to track progress during Session 20:

| Metric | Baseline | After Block E | After Block F | Target |
|--------|----------|--------------|--------------|--------|
| Core mode total | 42,680ms | ? | ? | <12,000ms |
| Analytics mode total | 31,663ms | ? | ? | <12,000ms |
| Trade sim (core) | 41,052ms | ? | ? | <10,000ms |
| Filter pipeline | 65ms | — | — | <30ms |
| Cache hit rate | 50% | — | ? | 100% |
| Test count | 272 | — | — | ~302 |

---

## RISK LOG

| Risk | Mitigation |
|------|------------|
| Freezing `DataBundle` breaks existing tests | Run full suite after Block G; fix one test at a time |
| ATR cache returns stale data across runs | Add `clear_cache()` call in backtester loop; document in template |
| LTF gating changes trade results | Assert trade counts identical between modes on same seed data |
| `"debug"` rename breaks legacy test fixtures | Legacy tests are in `tests/migration/` — update fixtures in same PR |
| Chat window ends mid-block | Write STATUS: PARTIAL in handoff; never leave half-renamed files |
---
*Plan version: 1.0 | Created: Session 20 | Owner: Session lead*