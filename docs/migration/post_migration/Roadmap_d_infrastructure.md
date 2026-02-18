# ROADMAP Chapter D — Infrastructure
## CLI, logging, MagicMock cleanup, IndicatorStore, config schema

**Parent**: `POST_MIGRATION_ROADMAP.md`  
**Priority**: 🔴 High (items D4.2, D4.5, D4.6) / 🟡 Medium (rest)  
**Load when**: Working on production hardening or technical debt

---

## D4.1 CLI Integration

**Goal**: Command-line entry points for backtesting and reporting.

```bash
python -m wbwsstrategy backtest \
    --config config.yaml \
    --report html,excel \
    --output-dir outputs/

python -m wbwsstrategy report \
    --input outputs/analytics_20260217.json \
    --format pdf \
    --theme light
```

**Implementation**: `argparse` or `click`. Wire to existing pipeline.

**Estimated effort**: 1 session

---

## D4.2 MagicMock Cleanup (DEC-020) 🔴

**Goal**: Replace `MagicMock` with real dataclass instances in 4 test files.

**Affected files**:
```
tests/migration/test_analytics_contracts.py
tests/migration/test_trade_analytics_session15.py
tests/migration/test_trade_analytics_session16.py
tests/migration/test_report_generator_session17.py
```

**What MagicMock is covering**:
- `MetricsReport` — replace with real `MetricsReport(total_trades=500, win_rate=18.5, ...)`
- `TradeResult` — replace with fixture from a small synthetic trade list

**Approach**:
1. Create `tests/conftest.py` with shared fixtures: `real_metrics_report()`, `real_trade_result()`
2. Replace each `MagicMock(spec=MetricsReport)` → `real_metrics_report()`
3. Run tests, fix any failures caused by the mock having been too permissive
4. Delete `from unittest.mock import MagicMock` from all 4 files

**Estimated effort**: ~2 hours  
**Do first** — unblocks realistic test failures that MagicMock was hiding.

---

## D4.3 Legacy Code Removal

**Goal**: Delete `src/strategies/core/` (the old dict-based system).

**When**: After Session 22 integration tests confirm 100% parity end-to-end.

**Steps**:
1. `grep -r "from src.strategies.core" .` — confirm zero imports
2. `mv src/strategies/core/ archive/legacy_strategies_core/`
3. Update any documentation references
4. Run full test suite

**Estimated effort**: ~1 hour  
**Risk**: Low — if no imports remain, deletion is safe.

---

## D4.4 IndicatorStore Refactoring (DEC-007) 🟡

**Goal**: Replace mutable `indicators: Dict[str, pd.Series]` + `ind_np: Dict[str, np.ndarray]`
with an encapsulated `IndicatorStore` class.

```python
# Before (current)
indicators: Dict[str, pd.Series] = {}
ind_np: Dict[str, np.ndarray] = {}
filter.compute_indicators(df, indicators, ind_np)

# After
store = IndicatorStore()
filter.compute_indicators(df, store)
rsi = store.get_numpy("rsi")          # returns cached np array
rsi_series = store.get_series("rsi")  # returns pd.Series
```

**`IndicatorStore` interface**:
```python
class IndicatorStore:
    def add(self, key: str, series: pd.Series) -> None: ...
    def get_series(self, key: str) -> pd.Series: ...
    def get_numpy(self, key: str) -> np.ndarray: ...  # cached
    def has(self, key: str) -> bool: ...
    def keys(self) -> List[str]: ...
```

**Estimated effort**: ~3 hours (implement store, update FilterPipeline, update all 11 filters, tests)

---

## D4.5 Config Schema Validation 🔴

**Goal**: Replace dict-based strategy config with typed `StrategyConfig` dataclass.

```python
# Current (fragile)
spread_enabled = config['trade_management']['spread']['enabled']

# Target (type-safe)
@dataclass(frozen=True)
class SpreadConfig:
    enabled: bool = False
    points: float = 0.5

@dataclass(frozen=True)
class StrategyConfig:
    spread: SpreadConfig
    risk: RiskConfig
    sessions: SessionConfig
    # ...

if config.spread.enabled:  # IDE autocomplete, mypy validated
```

**Implementation steps**:
1. Create `src/strategies/contracts/config_contracts.py`
2. Add `__post_init__` validation (ranges, required fields, dependencies)
3. Add migration helper: `StrategyConfig.from_dict(legacy_dict)`
4. Update all modules to accept typed config

**Estimated effort**: ~3 hours

---

## D4.6 Timezone Handling Verification 🔴

**Goal**: Verify and enforce UTC throughout the entire pipeline. Currently assumed everywhere but not enforced.

```python
# Target: DataBundle.__post_init__ enforces UTC or raises
@dataclass(frozen=True)
class DataBundle:
    def __post_init__(self):
        if self.strategy.index.tz is None:
            raise ValueError("DataBundle: index must be timezone-aware (UTC)")
        if str(self.strategy.index.tz) != "UTC":
            raise ValueError(f"DataBundle: index must be UTC, got {self.strategy.index.tz}")
```

**Audit scope**:
- `DataBundle.__post_init__` — add UTC enforcement
- All timestamp comparisons in `FilterPipeline` (session boundaries in UTC?)
- `TradeEntry.entry_time` / `TradeExit.exit_time` — are they UTC-aware?
- `TradeAnalytics._classify_session()` — does it convert to UTC before comparing hours?

**Estimated effort**: ~2 hours  
**Risk**: Medium — timezone bugs are silent and catastrophic in backtesting.

---

## D4.7 Structured Logging Foundation 🔴

**Goal**: Replace scattered `print()` / ad-hoc `logger.info()` with structured
JSON logging throughout the pipeline.

```python
# utils/structured_logger.py
class StructuredLogger:
    def log_stage(self, stage: str, input_count: int, output_count: int,
                  duration_ms: float, metadata: dict = None):
        entry = {
            "ts": datetime.utcnow().isoformat(),
            "stage": stage,
            "in": input_count,
            "out": output_count,
            "ms": round(duration_ms, 2),
            **(metadata or {})
        }
        logger.info(json.dumps(entry))
```

**Add to each pipeline stage**:
- `DataLoader`: log bars loaded, date range, duration
- `SignalGenerator`: log signals generated (buy/sell counts)
- `FilterPipeline`: log input/output per filter, rejection reasons
- `TradeSimulator`: log trades opened, closed, rejected, duration
- `MetricsCalculator`: log key metrics snapshot
- `TradeAnalytics`: log insights generated, grade, duration
- `ReportGenerator`: log file path, size, duration

**Estimated effort**: ~2 hours

---

## D4.8 Two-Phase TradeManager Decision

**Goal**: Avoid wasted RiskManager calls on signals TradeManager will reject
(pyramiding limits, opposite signal logic).

```python
# Current: always call RiskManager first (0.8% overhead measured)
params = risk_mgr.compute_trade_parameters(...)
result = tm.handle_signal(..., entry_price=params.entry_price, ...)

# Proposed: quick pre-check before expensive RiskManager call
if tm.can_accept_signal(signal_type):   # O(1), checks pyramiding/opposite only
    params = risk_mgr.compute_trade_parameters(...)
    result = tm.handle_signal_full(..., params)
else:
    result = TradeDecision(decision_type=DecisionType.REJECT, ...)
```

**Expected gain**: 5–10% on high-rejection strategies (>30% rejection rate).  
**When to implement**: Only if profiling confirms RiskManager is a bottleneck.  
**Estimated effort**: 2–3 hours

---

## D4.9 Async Report Generation

**Goal**: `async` version of `ReportGenerator.generate()` for non-blocking use
in web service contexts.

```python
generated = await ReportGenerator.generate_async(analytics, config)
# internally: await asyncio.to_thread(ReportGenerator.generate, analytics, config)
```

**Estimated effort**: ~1 hour  
**When**: Only if ReportGenerator is called from an async web framework (FastAPI etc.)