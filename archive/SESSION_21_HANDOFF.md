# SESSION 21 — HANDOFF & OPENING BRIEF
**Created**: 2026-02-20 (end of Session 20)
**Updated**: 2026-02-20 (Session 21 — assumptions resolved, files delivered, migration plan added)
**Phase**: 9 — Integration & Orchestration
**Session focus**: Wire the orchestrator skeleton to real data and achieve a passing E2E run

---

## STATUS: PARTIAL — Stopped after assumption verification + file delivery + migration planning. Next: Step 1 (deploy files) → E2E run.

---

## What to Read Before Starting

Three documents only — in this order:

1. **This file** — complete context for Session 21
2. **`docs/architecture/ARCHITECTURE.md`** (v2.2.0) — module signatures, contract types, import paths
3. **`src/strategies/orchestrator.py`** (v1.2.0) — all assumptions resolved; read the correction comments

---

## State of the System

Phase 8 is closed. Every pipeline module is production-hardened.

| Module | File | State |
|--------|------|-------|
| DataLoader | `src/strategies/specific/modules/data_loader.py` | ✅ Frozen contracts, mode-clean — **not yet StrategyConfig-integrated** |
| SignalGenerator | `src/strategies/specific/modules/signal_generator.py` | ✅ Stateless — **not yet StrategyConfig-integrated** |
| FilterPipeline | `src/strategies/specific/modules/filter_pipeline.py` | ✅ **Already StrategyConfig-integrated** (Session 20 Block D) |
| TradeSimulator | `src/strategies/specific/modules/trade_simulator.py` | ✅ Mode-gated LTF — **not yet StrategyConfig-integrated** |
| RiskManager | `src/strategies/specific/modules/risk_manager.py` | ✅ `clear_cache()` available |
| SpreadManager | `src/strategies/specific/modules/spread_manager.py` | ✅ Class-level config cache |
| MetricsCalculator | `src/strategies/specific/modules/metrics_calculator.py` | ✅ `calculate_metrics(trade_result)` |
| TradeAnalytics | `src/strategies/specific/modules/trade_analytics.py` | ✅ Not wired in orchestrator v1.2 |
| ReportGenerator | `src/strategies/specific/modules/report_generator.py` | ✅ Not wired in orchestrator v1.2 |
| Orchestrator | `src/strategies/orchestrator.py` | ✅ v1.2.0 — all assumptions resolved, workarounds documented |
| Runner script | `scripts/runners/run_strategy.py` | ✅ mode_override wired, clean output |
| `wbws_strategy_v2.yaml` | `configs/strategies/wbws/wbws_strategy_v2.yaml` | ✅ Delivered — deploy and verify load |
| Test count | — | ~302 |

**Architecture is locked**: any contract or interface change requires a DEC entry in `DECISION_LOG.md` before implementation.

---

## Assumption Verification Results (Session 21)

All five assumptions from the v1.0.0 skeleton were verified against source files. Four of five were wrong.

| # | Assumption | Result | Root cause |
|---|---|---|---|
| 1 | `config.execution.mode` | ✅ Correct | — |
| 2 | `DataLoader(config).load()` | ❌ Wrong | Pre-migration module; takes path string + raw YAML |
| 3 | `SignalGenerator(config).generate()` | ❌ Wrong | Pre-migration module; takes `htf_period: str` only |
| 4 | `FilterPipeline(config).run()` | ❌ Partially wrong | Method is `.apply_filters(sf, df, mode)`; `pass_rate` already 0–100 |
| 5 | `TradeSimulator(config).simulate_trades(sf, bundle, mode)` | ❌ Wrong | Complete mismatch; takes raw dict + df_full; different signature |

The orchestrator v1.2.0 corrects all five with documented workarounds. The workarounds are **temporary scaffolding** — they are the subject of the migration plan below.

---

## Steps to First Green E2E Run (Resume Here)

### Step 1 — Deploy the three delivered files

```
src/strategies/orchestrator.py          ← replace with v1.2.0
scripts/runners/run_strategy.py         ← replace with corrected version
configs/strategies/wbws/wbws_strategy_v2.yaml  ← new file, create directory if needed
```

Verify the YAML loads cleanly before running anything:
```python
from pathlib import Path
from src.config.config_schema import StrategyConfig
config = StrategyConfig.from_yaml(Path("configs/strategies/wbws/wbws_strategy_v2.yaml"))
print(config.execution.mode)   # → "analytics"
print(config.data.paths.strategy_ohlcv)
```

**Update the data paths in the YAML** to match your actual processed data directory before this will pass.

### Step 2 — Run core mode first
```bash
python scripts/runners/run_strategy.py \
    --config configs/strategies/wbws/wbws_strategy_v2.yaml \
    --mode core \
    --log-level INFO
```

### Step 3 — Expected output shape
```
============================================================
RESULT SUMMARY
============================================================
  Mode          : core
  Total trades  : ~4379
  Win rate      : ~35.8%
  Total PnL     : ~-10476.0 pts
  Expectancy    : ~-2.39 pts/trade
  Profit factor : ~0.87
  Max drawdown  : -XXX.X pts

  Stage timing:
    data           XXXX.X ms   ← target <500ms
    signals          XX.X ms   ← target <50ms
    filters          XX.X ms   ← target <30ms
    trades         XXXX.X ms   ← target <10,000ms
    metrics           X.X ms   ← target <5ms
    TOTAL          XXXX.X ms   ← target <12,000ms
============================================================
```

### Step 4 — Run analytics mode and confirm identical trade counts
```bash
python scripts/runners/run_strategy.py \
    --config configs/strategies/wbws/wbws_strategy_v2.yaml \
    --mode analytics \
    --log-level INFO
```

### Step 5 — Lock the performance baseline
Create `docs/migration/PERFORMANCE_BASELINE_S21.md`:
```
Date: [date]
Dataset: [instrument] | [bar count] bars | [start] → [end]

Core mode:      data Xms | signals Xms | filters Xms | trades Xms | metrics Xms | TOTAL Xms
Analytics mode: data Xms | signals Xms | filters Xms | trades Xms | metrics Xms | TOTAL Xms

Non-regression rule: >5% degradation on any stage = P0 blocker.
```

---

## StrategyConfig Integration Migration Plan

### Background

`FilterPipeline` was migrated to accept `StrategyConfig` in Session 20 Block D. Three modules were not migrated and still use pre-architecture interfaces. This creates two concrete problems in the current orchestrator:

1. **`_build_simulator_config()`** — manually translates `StrategyConfig` back into a raw dict for `TradeSimulator`. If a field is added to `StrategyConfig` and not mirrored here, the simulator silently uses stale values with no validation error.

2. **`_read_htf_period()`** — re-reads the YAML file from disk at runtime to extract one string that `SignalGenerator` needs. The YAML has already been parsed once into `StrategyConfig`; parsing it again is redundant and fragile.

The three migrations below eliminate both workarounds. They are sequenced so each one is a self-contained, testable change.

---

### DEC-033 — Add missing fields to `DataConfig` and migrate `DataLoader`

**Priority**: P1
**Effort**: Medium — two-file change
**Files**: `src/config/config_schema.py`, `src/strategies/specific/modules/data_loader.py`
**Blocks**: DEC-034 (indirectly — htf_period lives here)

#### Why

`DataLoader` currently takes a YAML file path and re-parses it internally via `load_config()`. `StrategyConfig` already contains all the data path and date range information. The re-parsing is redundant and means the same YAML is validated twice by two different parsers, with no guarantee they agree.

Additionally, `DataConfig` is missing three fields that `DataLoader` currently reads from the raw YAML:
- `ltf_timeframe` (e.g. `"1s"`) — bar frequency of the LTF data file
- `artf_timeframe` (e.g. `"1ME"`) — bar frequency of the ARTF data file
- `htf_period` (e.g. `"1H"`) — HTF resampling period, needed by `SignalGenerator`

`htf_period` is added to `DataConfig` here (not in a separate DEC) because it is fundamentally a data configuration concern — it describes the HTF data file — and co-locating it with `DataPathsConfig` is the right home.

#### Schema changes — `config_schema.py`

**Step 1**: Add `htf_period`, `ltf_timeframe`, `artf_timeframe` to `DataConfig`:

```python
@dataclass(frozen=True)
class DataConfig:
    paths: DataPathsConfig
    date_range: DateRangeConfig
    timezone: str = "CET"
    htf_period: str = "1H"       # ADD — HTF bar frequency (e.g. "1H", "4H")
    ltf_timeframe: str = "1s"    # ADD — LTF bar frequency (e.g. "1s", "1min")
    artf_timeframe: str = "1ME"  # ADD — ARTF bar frequency (e.g. "1ME")

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'DataConfig':
        return cls(
            paths=DataPathsConfig.from_dict(d.get('paths', {})),
            date_range=DateRangeConfig.from_dict(d.get('date_range', {})),
            timezone=str(d.get('timezone', 'CET')),
            htf_period=str(d.get('htf_period', '1H')),       # ADD
            ltf_timeframe=str(d.get('ltf_timeframe', '1s')), # ADD
            artf_timeframe=str(d.get('artf_timeframe', '1ME')), # ADD
        )
```

No `__post_init__` validation needed for these three — they are informational strings used by downstream modules, not validated ranges.

**Step 2**: Update `wbws_strategy_v2.yaml` — the three new fields are already present under `data:` in the delivered YAML (`htf_period: "1H"`). Add `ltf_timeframe` and `artf_timeframe` if needed.

#### Module changes — `data_loader.py`

**Step 3**: Change `DataLoader.__init__` to accept `StrategyConfig`:

```python
# BEFORE
def __init__(self, config_path: str, project_root=None, mode: str = "core"):
    self.config_path = Path(config_path).resolve()
    self.raw_config = None
    self.data_config = None

# AFTER
def __init__(self, config: StrategyConfig, mode: str = "core", project_root=None):
    self._strategy_config = config
    self.project_root = project_root or PROJECT_ROOT
    self.mode = mode
    self._verbose = (mode == "analytics")
    self.cache_dir = Path.home() / ".wbws_data_cache"
    self.cache_dir.mkdir(exist_ok=True)
    self._cache_hits = 0
    self._cache_misses = 0
```

**Step 4**: Replace `load_config()` with a private `_build_data_config()` that reads directly from `StrategyConfig`. `load_data()` calls this instead of `self.load_config()`.

```python
def _build_data_config(self) -> DataConfig:
    """Build DataConfig from the typed StrategyConfig — no YAML re-parse."""
    cfg = self._strategy_config.data
    return DataConfig(
        strategy_data=DataFileConfig(
            path=cfg.paths.strategy_ohlcv,
            format=cfg.paths.strategy_ohlcv.suffix.lstrip("."),
        ),
        htf_data=DataFileConfig(
            path=cfg.paths.htf_ohlcv,
            format=cfg.paths.htf_ohlcv.suffix.lstrip("."),
        ) if cfg.paths.htf_ohlcv else None,
        ltf_data=DataFileConfig(
            path=cfg.paths.ltf_ohlcv,
            format=cfg.paths.ltf_ohlcv.suffix.lstrip("."),
        ) if cfg.paths.ltf_ohlcv else None,
        artf_data=DataFileConfig(
            path=cfg.paths.artf_ohlcv,
            format=cfg.paths.artf_ohlcv.suffix.lstrip("."),
        ) if cfg.paths.artf_ohlcv else None,
        date_range=DateRange(
            start=pd.Timestamp(cfg.date_range.start).to_pydatetime(),
            end=pd.Timestamp(cfg.date_range.end).to_pydatetime(),
        ),
    )
```

**Step 5**: Replace the two `raw_config` reads in `load_data()`:
```python
# BEFORE
ltf_timeframe = self.raw_config["data"].get("ltf_timeframe", "1s")
artf_timeframe = self.raw_config["data"].get("artf_timeframe", "1ME")

# AFTER
ltf_timeframe = self._strategy_config.data.ltf_timeframe
artf_timeframe = self._strategy_config.data.artf_timeframe
```

**Step 6**: Remove `load_config()` entirely. It is replaced by `_build_data_config()`.

#### Orchestrator update after DEC-033

Remove `_read_htf_period()` and the `config_path` parameter from `__init__`. Update `_load_data()`:

```python
# BEFORE
loader = DataLoader(config_path=str(self._config_path), mode=mode)

# AFTER
loader = DataLoader(config=self._config, mode=mode)
```

Update `from_yaml()` — it no longer needs to pass `path` to `__init__`:
```python
@classmethod
def from_yaml(cls, path: Path) -> "StrategyOrchestrator":
    config = StrategyConfig.from_yaml(path)
    return cls(config)   # path no longer needed
```

`OrchestratorResult` loses `config_path` field (it was only needed to pass to `DataLoader`).

#### Tests to write after DEC-033

- `test_data_config_htf_period_default_is_1H()`
- `test_data_config_ltf_artf_timeframe_defaults()`
- `test_data_loader_accepts_strategy_config()`
- `test_data_loader_does_not_reparse_yaml()`
- `test_orchestrator_init_no_longer_requires_config_path()`

---

### DEC-034 — Migrate `SignalGenerator` to accept `StrategyConfig`

**Priority**: P1
**Effort**: Small — one-file change
**Files**: `src/strategies/specific/modules/signal_generator.py`
**Depends on**: DEC-033 (needs `config.data.htf_period` to exist)

#### Why

`SignalGenerator.__init__` currently takes `htf_period: str` as a bare string with no type context. Every call site must independently know to pass this value. After DEC-033 adds `htf_period` to `DataConfig`, `SignalGenerator` should read it from the typed config directly.

#### Module changes — `signal_generator.py`

**Step 1**: Change `SignalGenerator.__init__` to accept `StrategyConfig`:

```python
# BEFORE
def __init__(self, htf_period: str, mode: str = "core"):
    if not htf_period:
        raise ValueError("htf_period configuration is missing.")
    self.htf_period = htf_period
    self.mode = mode
    self.trigger = WBWSTrigger(htf_period=self.htf_period)

# AFTER
def __init__(self, config: StrategyConfig, mode: str = "core"):
    htf_period = config.data.htf_period
    if not htf_period:
        raise ValueError(
            "data.htf_period is required in strategy config. "
            "Add htf_period to the data: section of your YAML."
        )
    self.htf_period = htf_period
    self.mode = mode
    self.trigger = WBWSTrigger(htf_period=self.htf_period)
```

**Step 2**: Rename `generate_signals()` to `generate()` to match the architecture doc contract. Keep `generate_signals()` as a deprecated alias that logs a warning and delegates, for one session, before removal.

```python
def generate(self, data_bundle: DataBundle) -> SignalFrame:
    """Primary entry point — replaces generate_signals()."""
    return self.generate_signals(data_bundle)   # delegates during transition

def generate_signals(self, data_bundle: DataBundle) -> SignalFrame:
    """Deprecated — use generate(). Will be removed in Session 22."""
    # existing implementation unchanged
```

#### Orchestrator update after DEC-034

`_generate_signals()` simplifies to:
```python
def _generate_signals(self, data_bundle: DataBundle, mode: str) -> SignalFrame:
    generator = SignalGenerator(config=self._config, mode=mode)
    frame = generator.generate(data_bundle)
    ...
```

Remove `self._htf_period` and `_read_htf_period()` from the orchestrator entirely.

#### Tests to write after DEC-034

- `test_signal_generator_accepts_strategy_config()`
- `test_signal_generator_reads_htf_period_from_config()`
- `test_signal_generator_raises_if_htf_period_empty()`
- `test_generate_alias_delegates_to_generate_signals()`

---

### DEC-035 — Migrate `TradeSimulator` to accept `StrategyConfig`

**Priority**: P1
**Effort**: Large — multi-file change (TradeSimulator + SpreadManager + RiskManager + TradeManager)
**Files**: `src/strategies/specific/modules/trade_simulator.py`,  `src/strategies/specific/modules/spread_manager.py`, `src/strategies/specific/modules/risk_manager.py`, `src/strategies/specific/modules/trade_manager.py`
**Depends on**: None (can be done independently of DEC-033/034)

#### Why

`TradeSimulator.__init__(config: Dict, df_full)` is the largest remaining pre-migration interface. The `_build_simulator_config()` adapter in the orchestrator is the most brittle piece of the current system — a silent failure risk every time `StrategyConfig` gains a new field. `TradeSimulator` also has two fields that `StrategyConfig` does not yet expose: `asset.symbol` (for `SpreadManager`) and `spread.config_path`. These must be resolved as part of this migration.

#### Gap resolution before coding

Two fields are needed that `StrategyConfig` does not have:

**Gap A — `asset.symbol`**: `SpreadManager` is constructed with `SpreadManager(asset_symbol, config_path)`. The symbol (e.g. `"EURUSD"`) is not in `StrategyConfig`. It belongs in a new `AssetConfig` frozen dataclass.

**Gap B — `spread.config_path`**: `SpreadManager` accepts an optional path to the broker spreads YAML. This belongs in `SpreadConfig` as an optional field.

**Step 0 — Schema additions** (add to `config_schema.py` before touching the simulator):

```python
@dataclass(frozen=True)
class AssetConfig:
    symbol: str
    pip_size: float = 0.0001
    point_size: float = 0.00001

    def __post_init__(self):
        if not self.symbol.strip():
            raise ValueError("asset.symbol cannot be blank")

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'AssetConfig':
        return cls(
            symbol=str(d.get('symbol', '')),
            pip_size=float(d.get('pip_size', 0.0001)),
            point_size=float(d.get('point_size', 0.00001)),
        )
```

Add `asset: AssetConfig` to `StrategyConfig` and add an optional `config_path` field to `SpreadConfig`:

```python
@dataclass(frozen=True)
class SpreadConfig:
    enabled: bool
    spread_type: str
    spread_value: float
    config_path: Optional[Path] = None   # ADD — path to broker_spreads.yaml

    @classmethod
    def from_dict(cls, d):
        return cls(
            ...existing fields...,
            config_path=Path(d['config_path']) if d.get('config_path') else None,
        )
```

Add to `wbws_strategy_v2.yaml`:
```yaml
asset:
  symbol: "EURUSD"
  pip_size: 0.0001
  point_size: 0.00001

trade_management:
  spread:
    config_path: "configs/spreads/broker_spreads.yaml"   # optional
```

#### Module changes — `trade_simulator.py`

**Step 1**: Change `TradeSimulator.__init__` to accept `StrategyConfig`:

```python
# BEFORE
def __init__(self, config: Dict, df_full: pd.DataFrame):
    self.config = config
    ...
    analytics_cfg = config.get("analytics", config.get("debug", {}))
    self.profile_enabled = analytics_cfg.get("profile_simulator", False)

# AFTER
def __init__(self, config: StrategyConfig, df_full: pd.DataFrame):
    self._strategy_config = config
    self.df_full = df_full
    self.profile_enabled = False   # profiler not exposed in StrategyConfig by design
    ...
```

**Step 2**: Update `initialize_managers()` to read from typed config:

```python
def initialize_managers(self) -> None:
    self.trade_manager = TradeManager(self._strategy_config)   # see below

    spread_cfg = self._strategy_config.trade_management.spread
    if spread_cfg.enabled:
        self.spread_manager = SpreadManager(
            asset_symbol=self._strategy_config.asset.symbol,
            config_path=spread_cfg.config_path,               # Optional[Path]
        )
```

**Step 3**: Update `simulate_trades()` signature to accept `SignalFrame` directly and extract the Series internally — removing the signal code translation from the orchestrator:

```python
# BEFORE
def simulate_trades(self, df_strategy, filtered_signals: pd.Series, verbose, ...):

# AFTER
def simulate_trades(self, df_strategy, filtered_signals: pd.Series, verbose, ...):
    # signature unchanged externally; the orchestrator translation stays
    # until SignalFrame iteration is added here in a follow-up (CF-6)
```

Note: full `SignalFrame` → `pd.Series` translation can remain in the orchestrator for now as CF-6, since it touches signal contract conventions and should be its own DEC. The priority here is the config migration.

#### Sub-manager changes

Each sub-manager needs its constructor updated in parallel. These are coordinated changes — do all four files atomically.

**`RiskManager`**:
```python
# BEFORE
def __init__(self, config: Dict, df_full: pd.DataFrame):
    risk_cfg = config.get("trade_management", {}).get("risk", {})
    self.atr_length = risk_cfg.get("atr_length", 14)
    ...

# AFTER
def __init__(self, config: StrategyConfig, df_full: pd.DataFrame):
    risk_cfg = config.trade_management.risk
    self.atr_length = risk_cfg.atr_length
    self.atr_multiplier_sl = risk_cfg.atr_multiplier_sl
    self.atr_multiplier_tp = risk_cfg.atr_multiplier_tp
    self.max_risk_percentile = risk_cfg.max_risk_percentile
```

**`SpreadManager`**:
```python
# BEFORE
def __init__(self, asset_symbol: str, config_path=None):

# AFTER — no signature change needed; called with typed values from TradeSimulator
```

**`TradeManager`**:
```python
# BEFORE
def __init__(self, config: Dict):
    tm_cfg = config.get("trade_management", {})
    pc_cfg = tm_cfg.get("position_control", {})
    self.max_positions = pc_cfg.get("max_positions", 1)
    ...

# AFTER
def __init__(self, config: StrategyConfig):
    pc_cfg = config.trade_management.position_control
    self.max_positions = pc_cfg.max_positions
    self.pyramiding_enabled = pc_cfg.pyramiding_enabled
    self.close_on_opposite = pc_cfg.close_on_opposite
```

#### Orchestrator update after DEC-035

Remove `_build_simulator_config()` entirely. `_simulate_trades()` simplifies to:

```python
def _simulate_trades(self, filter_result, data_bundle, mode):
    simulator = TradeSimulator(
        config=self._config,
        df_full=data_bundle.full,
    )
    filtered_signals = (
        filter_result.final_signals.signals
        .map(_SIGNAL_CODE_TO_STR)
        .dropna()
    )
    result = simulator.simulate_trades(
        df_strategy=data_bundle.strategy,
        filtered_signals=filtered_signals,
        verbose=(mode == "analytics"),
        df_ltf=data_bundle.ltf,
    )
    ...
```

#### Tests to write after DEC-035

- `test_trade_simulator_accepts_strategy_config()`
- `test_risk_manager_accepts_strategy_config()`
- `test_trade_manager_accepts_strategy_config()`
- `test_spread_manager_receives_symbol_from_asset_config()`
- `test_asset_config_validates_blank_symbol()`
- `test_spread_config_optional_config_path()`
- `test_orchestrator_has_no_build_simulator_config()`

---

### Migration Sequence Summary

The three DECs are independent enough to be worked in any order, but this sequence minimises orchestrator churn:

```
DEC-033  →  DEC-034  →  DEC-035
 (data)    (signals)    (trades)

After each DEC:
  1. Update orchestrator workaround for that module only
  2. Run E2E test — numbers must match PERFORMANCE_BASELINE_S21
  3. Run full test suite — count must be >= previous baseline
  4. Commit atomically
```

**State of orchestrator workarounds after each DEC**:

| After | `_read_htf_period()` | `config_path` in init | `_build_simulator_config()` |
|-------|---------------------|----------------------|----------------------------|
| Now (v1.2.0) | ⚠️ exists | ⚠️ required | ⚠️ exists |
| DEC-033 | ⚠️ exists | ✅ removed | ⚠️ exists |
| DEC-034 | ✅ removed | ✅ removed | ⚠️ exists |
| DEC-035 | ✅ removed | ✅ removed | ✅ removed |

After DEC-035 the orchestrator is clean: every stage method is a direct call with `self._config` — no translation, no re-parsing, no adapter methods.

---

## Feature Restoration Plan — Three Lost Features from Legacy Architecture

Three features present in the legacy solution are missing or broken in the new
architecture. Each is traced to its root cause below, then a precise DEC is
proposed. All three DECs depend on DEC-035 being merged first (it adds
`AssetConfig` and `SpreadConfig.config_path` to the schema).

---

### FEATURE 1 — Broker-Configured Spread (Currently Broken)

#### What should happen

`SpreadManager` reads spread configuration from `broker_spreads.yaml` — a
file that holds per-asset broker spread values. For `DEUIDXEUR` the spread
might be `1.0 points`. This file is the single source of truth for spread
values — it is not duplicated in the strategy YAML.

#### What actually happens now

Two independent problems prevent this from working:

**Problem A — `asset.symbol` is not wired.**
`RiskManager.__init__` tries to read:
```python
asset_symbol = config.get("asset", {}).get("symbol", "")
```
There is no `asset:` section in `strategy_template.yaml` and no `AssetConfig`
in `StrategyConfig`. The result is `asset_symbol = ""`. `SpreadManager` is
constructed with an empty symbol. It will either load the wrong spread entry
from the YAML or (more likely) log a warning that the asset was not found and
silently apply zero spread. All trades execute as if spread is disabled.

**Problem B — `spread.config_path` is not wired.**
`SpreadManager.__init__` calls `_resolve_config_path(None)` which navigates
four parent directories from its own file location and constructs the default
path:
```
<project_root>/configs/spreads/broker_spreads.yaml
```
This path is hardcoded inside `SpreadManager`. It is not configurable from the
strategy YAML. If the file does not exist at that exact path, `SpreadManager`
raises `FileNotFoundError` immediately. If it exists but contains stale values
for a different broker, wrong spreads are applied with no warning.

**Problem C — Template `spread_value` is misleading.**
`strategy_template.yaml` shows:
```yaml
trade_management:
  spread:
    spread_value: 0.5
    spread_type: "points"
```
These values are **never read by `SpreadManager`**. `SpreadManager` reads
exclusively from `broker_spreads.yaml`. The template value exists because
`StrategyConfig` validates it via `SpreadConfig`, but `RiskManager` ignores it
and reads from the broker file. This creates a false impression that editing
`spread_value` in the strategy YAML changes the spread. **It does not.**

#### Root causes

| Problem | Root cause | Resolves with |
|---------|------------|---------------|
| A — missing symbol | `AssetConfig` not in `StrategyConfig` | DEC-035 |
| B — hardcoded path | `spread.config_path` not in `SpreadConfig` | DEC-035 |
| C — misleading template | `spread_value` unused by `SpreadManager` | DEC-036 (see below) |

#### DEC-036 — Unify spread configuration source of truth

**Priority**: P1
**Effort**: Small — schema + template + SpreadManager `_resolve_config_path` tweak
**Depends on**: DEC-035 (adds `AssetConfig` and `SpreadConfig.config_path`)

**Decision**: After DEC-035, `SpreadConfig.config_path` is the canonical path
to `broker_spreads.yaml`. The strategy YAML `spread_value` / `spread_type`
fields continue to exist in `SpreadConfig` for validation only (they define
the fallback when no broker file is provided). Their relationship to the broker
file must be explicit, not implicit.

**Step 1** — Remove the hardcoded default from `SpreadManager._resolve_config_path`:
```python
# BEFORE (hardcoded fallback — silent wrong spread risk)
@staticmethod
def _resolve_config_path(spread_config_path: Optional[str]) -> Path:
    if spread_config_path is None:
        project_root = Path(__file__).resolve().parents[4]
        return project_root / "configs" / "spreads" / "broker_spreads.yaml"
    return Path(spread_config_path)

# AFTER (fail-fast — see also SM-2 in Fail-Fast Audit)
@staticmethod
def _resolve_config_path(spread_config_path: Optional[Path]) -> Path:
    if spread_config_path is None:
        raise ValueError(
            "SpreadManager requires an explicit config_path. "
            "Set trade_management.spread.config_path in your strategy YAML, "
            "or set trade_management.spread.enabled: false."
        )
    path = Path(spread_config_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Broker spread config not found: {path}. "
            f"Verify trade_management.spread.config_path in your strategy YAML."
        )
    return path
```

**Step 2** — Update `wbws_strategy_v2.yaml` to add `asset:` and `spread.config_path`:
```yaml
asset:
  symbol: "DEUIDXEUR"    # must match key in broker_spreads.yaml exactly
  pip_size: 0.1          # DAX — 0.1 pt minimum tick
  point_size: 1.0        # DAX — 1 pt = 1.0 index point

trade_management:
  spread:
    enabled: true
    config_path: "configs/spreads/broker_spreads.yaml"
    # spread_value / spread_type below are the fallback used when broker file
    # does not contain an entry for this asset. They do NOT override the broker file.
    # For DEUIDXEUR the broker file has spread_type: percentage, spread_value: 0.015
    # (≈3.0 pts at DAX 20000). The values below are placeholders only.
    spread_type: "percentage"   # updated to match broker file type
    spread_value: 0.015         # updated to match broker file value
```

**Step 3** — Add a comment to `SpreadConfig.from_dict` clarifying precedence:
```python
# NOTE: spread_value and spread_type are fallback values only.
# When config_path is set, SpreadManager reads values from broker_spreads.yaml
# and these fields are not used for execution — only for schema validation.
```

**Step 4** — `broker_spreads.yaml` confirmed. The file exists at
`configs/spreads/broker_spreads.yaml` and already contains a correct entry for `DEUIDXEUR`:
```yaml
spreads:
  DEUIDXEUR:    # DAX40
    spread_value: 0.015
    spread_type: "percentage"
    display_name: "DAX 40"
    asset_class: "index"
```
This means DAX spread at a bid of 20 000 = `0.015/100 × 20 000 = 3.0 pts`.
The strategy template placeholder `spread_value: 0.5 points` is therefore
off by a factor of ~6 at current DAX levels — another reason the template value
must never be used for execution. `SpreadManager.get_spread_in_points()`
already handles `"percentage"` type correctly.

**Step 5** — Wire the broker `settings` section into `SpreadManager`.
The broker file has a `settings:` block that `SpreadManager` currently ignores:
```yaml
settings:
  apply_to_long: true
  apply_to_short: true
  application_method: "entry_only"
```
`application_method: "entry_only"` confirms the BID-data spread model:
LONG pays spread at open (entry price increases); SHORT pays spread at close
(buy-back at Ask). This is exactly the model implemented in `SpreadManager`
and `RiskManager`. The settings are consistent with the code.

However, `apply_to_long` and `apply_to_short` are currently read from the
**strategy YAML** by `RiskManager` (`spread_config.get("apply_to_long", True)`)
— not from the broker file. If the strategy YAML and broker file ever
disagree, the strategy YAML wins silently.

Add `_load_global_settings()` to `SpreadManager` to read these at load time:
```python
def _load_global_settings(self) -> None:
    """Read global broker settings from spread config."""
    settings = self.spread_config.get("settings", {})
    self.apply_to_long = settings.get("apply_to_long", True)
    self.apply_to_short = settings.get("apply_to_short", True)
    self.application_method = settings.get("application_method", "entry_only")
    if self.application_method not in {"entry_only", "entry_and_exit"}:
        raise ValueError(
            f"broker_spreads.yaml settings.application_method="
            f"'{self.application_method}' is not recognised. "
            f"Valid values: 'entry_only', 'entry_and_exit'."
        )
```
Expose `apply_to_long` / `apply_to_short` via `get_spread_info()` so
`RiskManager` can read them from `SpreadManager` instead of from the strategy
YAML. This removes a second source of truth for broker behaviour.

**Tests to write**:
- `test_spread_manager_raises_when_config_path_is_none()`
- `test_spread_manager_raises_when_config_path_not_found()`
- `test_spread_manager_loads_asset_from_broker_yaml()`
- `test_spread_manager_ignores_strategy_yaml_spread_value()`
- `test_spread_manager_deuidxeur_is_percentage_type()`
- `test_spread_manager_deuidxeur_at_20000_gives_3_points()`
- `test_spread_manager_loads_global_settings_from_broker_file()`
- `test_spread_manager_raises_on_unknown_application_method()`
- `test_wbws_yaml_has_asset_symbol_and_spread_config_path()`

---

### FEATURE 2 — Fixed Risk-to-Reward Ratio as Default TP Mode (Currently Broken)

#### What should happen

The legacy strategy used `risk_to_reward_ratio: 5.7` as the primary TP
mechanism. For each trade: `TP = entry ± (ATR × sl_multiplier × rr_ratio)`.
This is intentionally different from specifying `atr_multiplier_tp` directly
— the user thinks in terms of risk-reward, not raw ATR multiples.

The `rr_ratio` mode should be the **default**. The `atr_multiplier_tp` mode
should be an opt-in alternative for when the user wants to set TP directly as
a fixed multiple of ATR regardless of SL size.

#### What actually happens now

**Three separate problems** prevent this from working:

**Problem A — Config key path diverged.**
`StrategyConfig.RiskConfig` stores:
```
trade_management.risk.atr_multiplier_sl   (new schema)
trade_management.risk.atr_multiplier_tp   (new schema)
```
`RiskManager.__init__` reads:
```python
tm_config = config.get("trade_management", {})
self.sl_tp_config = tm_config.get("sl_tp", {})           # looks for 'sl_tp'
self.risk_config  = tm_config.get("risk_management", {}) # looks for 'risk_management'
```
Neither `"sl_tp"` nor `"risk_management"` exist in the new schema. Both
return empty dicts. `RiskManager` silently uses its hardcoded defaults:
`sl_multiplier=1.4`, `risk_to_reward_ratio=2.0`, `atr_length=14`.
**No config value from `StrategyConfig` is ever applied to risk calculations.**

**Problem B — No `tp_mode` field exists.**
There is no field in `RiskConfig` (or anywhere in `StrategyConfig`) that
selects between:
- `"rr_ratio"` — TP = entry ± risk_distance × rr_ratio (legacy default)
- `"atr_multiplier"` — TP = entry ± ATR × atr_multiplier_tp (new option)

Without this field, the mode is implicit and cannot be changed without editing
code.

**Problem C — `atr_multiplier_tp: 7.98` encodes the R:R silently.**
The template comment says `# sl * rr = 1.4 * 5.7 = 7.98`. This means the
author pre-computed the product and stored it as a single multiplier, hiding
the R:R concept. If the user changes `sl_multiplier` to 1.6, the encoded
`atr_multiplier_tp: 7.98` no longer represents a 5.7 R:R ratio. The R:R
concept is invisible at the config level.

#### Root causes

| Problem | Root cause | Resolution |
|---------|------------|------------|
| A — wrong key path | `RiskManager` was not updated when schema renamed fields | DEC-037 step 1 |
| B — no mode selector | `RiskConfig` never got `tp_mode` field | DEC-037 step 2 |
| C — encoded R:R | Template chose to store product not ratio | DEC-037 step 3 |

#### DEC-037 — Restore `risk_to_reward_ratio` as first-class config with `tp_mode` selector

**Priority**: P1
**Effort**: Medium — schema + RiskManager + template + TradeParameters contract
**Depends on**: DEC-035 (RiskManager will accept StrategyConfig after DEC-035)

**Step 1** — Fix `RiskConfig` field naming and add missing fields:

```python
# CURRENT (config_schema.py)
@dataclass(frozen=True)
class RiskConfig:
    atr_length: int
    atr_multiplier_sl: float
    atr_multiplier_tp: float
    max_risk_percentile: float

# AFTER DEC-037
@dataclass(frozen=True)
class RiskConfig:
    atr_length: int
    atr_multiplier_sl: float         # SL = entry ± ATR × atr_multiplier_sl
    atr_multiplier_tp: float         # TP when tp_mode = "atr_multiplier"
    max_risk_percentile: float
    tp_mode: str = "rr_ratio"        # ADD — "rr_ratio" | "atr_multiplier"
    risk_to_reward_ratio: float = 5.7  # ADD — TP multiplier when tp_mode = "rr_ratio"

    def __post_init__(self):
        _VALID_TP_MODES = {"rr_ratio", "atr_multiplier"}
        if self.tp_mode not in _VALID_TP_MODES:
            raise ValueError(
                f"risk.tp_mode='{self.tp_mode}' is invalid. "
                f"Valid values: {sorted(_VALID_TP_MODES)}. "
                f"Use 'rr_ratio' for fixed R:R (e.g. 5.7) or "
                f"'atr_multiplier' for direct ATR multiple (e.g. 7.98)."
            )
        if self.tp_mode == "rr_ratio" and self.risk_to_reward_ratio <= 0:
            raise ValueError(
                f"risk.risk_to_reward_ratio must be > 0 when tp_mode='rr_ratio'. "
                f"Got: {self.risk_to_reward_ratio}"
            )
        if self.tp_mode == "atr_multiplier" and self.atr_multiplier_tp <= 0:
            raise ValueError(
                f"risk.atr_multiplier_tp must be > 0 when tp_mode='atr_multiplier'. "
                f"Got: {self.atr_multiplier_tp}"
            )
        # existing validations unchanged
```

**Step 2** — Update `RiskManager` to read from `StrategyConfig` (as part of DEC-035),
and implement the `tp_mode` branch in `compute_trade_parameters()`:

```python
# In RiskManager.__init__ (after DEC-035 migration):
risk_cfg = config.trade_management.risk
self.atr_length = risk_cfg.atr_length
self.sl_multiplier = risk_cfg.atr_multiplier_sl
self.tp_mode = risk_cfg.tp_mode
self.rr_ratio = risk_cfg.risk_to_reward_ratio          # used when tp_mode = "rr_ratio"
self.atr_multiplier_tp = risk_cfg.atr_multiplier_tp   # used when tp_mode = "atr_multiplier"
```

```python
# In compute_trade_parameters() — TP calculation branch:

# ---- SL (unchanged) -----------------------------------------------
risk_distance = atr_val * self.sl_multiplier
# ... SL validation as before ...

# ---- TP (new branching logic) -------------------------------------
if self.tp_mode == "rr_ratio":
    tp_distance = risk_distance * self.rr_ratio
    # TP = entry ± (ATR × sl_mult × rr_ratio)
    # e.g. ATR=5.0, sl_mult=1.4, rr=5.7 → TP distance = 5.0×1.4×5.7 = 39.9 pts
else:  # "atr_multiplier"
    tp_distance = atr_val * self.atr_multiplier_tp
    # TP = entry ± (ATR × atr_multiplier_tp)
    # e.g. ATR=5.0, atr_mult_tp=7.98 → TP distance = 5.0×7.98 = 39.9 pts

tp = (
    executed_entry + tp_distance if is_long
    else executed_entry - tp_distance
)
```

Note: when `sl_mult=1.4` and `rr_ratio=5.7`, both modes produce identical TP
distances (`7.98 × ATR`). The difference is only in how they respond to config
changes: `rr_ratio` mode scales TP automatically when `atr_multiplier_sl`
changes; `atr_multiplier` mode does not.

**Step 3** — Update `wbws_strategy_v2.yaml` to make `tp_mode` and `risk_to_reward_ratio`
explicit:

```yaml
trade_management:
  risk:
    atr_length: 14
    atr_multiplier_sl: 1.4
    tp_mode: "rr_ratio"           # "rr_ratio" (default) | "atr_multiplier"
    risk_to_reward_ratio: 5.7     # used when tp_mode = "rr_ratio"
    atr_multiplier_tp: 7.98       # used when tp_mode = "atr_multiplier" (ignored otherwise)
    max_risk_percentile: 0.1
```

**Step 4** — Update `TradeParameters` contract to record which TP mode was used:

```python
# In trade_contracts.py — add two fields to TradeParameters:
tp_mode: str            # "rr_ratio" | "atr_multiplier"
tp_distance: float      # absolute distance from entry to TP in points
```

This makes the mode auditable in analytics — every trade record carries which
calculation produced its TP.

**Tests to write**:
- `test_risk_config_tp_mode_defaults_to_rr_ratio()`
- `test_risk_config_rejects_unknown_tp_mode()`
- `test_risk_config_rr_ratio_must_be_positive()`
- `test_risk_manager_rr_ratio_mode_produces_correct_tp()`
- `test_risk_manager_atr_multiplier_mode_produces_correct_tp()`
- `test_both_modes_produce_same_tp_when_params_equivalent()`
- `test_tp_mode_recorded_in_trade_parameters()`

---

### FEATURE 3 — Correct BID Price Handling for All Trade Legs (Partially Broken)

#### What is already correct

`SpreadManager` and `RiskManager` already implement BID-price conventions
correctly for entry and SL. The docstring states this explicitly:

| Leg | Direction | Calculation | Status |
|-----|-----------|-------------|--------|
| Entry | LONG | `bid + spread` (buy at Ask) | ✅ correct |
| Entry | SHORT | `bid` (sell at Bid) | ✅ correct |
| SL trigger | LONG | `sl_bid - spread` (exit at Bid, trigger below SL) | ✅ correct |
| SL trigger | SHORT | `sl_bid + spread` (exit at Ask, trigger above SL) | ✅ correct |

#### What is broken — SHORT TP exit spread not deducted

When a SHORT TP is hit, the position is **closed by buying** at Ask price:
```
SHORT TP actual close price = TP_bid + spread
SHORT TP actual P&L = (entry_bid - tp_bid) - spread
```
The current code sets `tp` as a BID level and records P&L as if the close is
at that exact BID price. The spread cost at TP close is never deducted.

For every SHORT winning trade, P&L is overstated by exactly one spread.
With `DEUIDXEUR` at 1.0 point spread, every SHORT win is 1.0 point too
optimistic. On a dataset with, say, 500 SHORT wins, total P&L is overstated
by 500 points — a material error.

**LONG TP** does not have this problem. The spread is already embedded in the
entry price (`executed_entry = bid + spread`). The LONG TP target is set above
this higher entry, so the spread is effectively pre-paid at entry. When the
LONG TP is hit, the exit is at Bid (sell) — no additional spread at exit.

#### The precise gap

`TradeParameters` has `stop_loss_trigger` (spread-adjusted SL level) but has
no corresponding `take_profit_trigger` field. The `TradeSimulator` uses `tp`
directly as the exit price without applying a spread adjustment for SHORT
direction.

#### DEC-038 — Add `take_profit_trigger` for BID-price SHORT TP exits

**Priority**: P1 — produces incorrect P&L on every SHORT winning trade
**Effort**: Small — three-file change
**Depends on**: DEC-035 (RiskManager StrategyConfig migration), DEC-037 (TP calculation)

**Step 1** — Add `take_profit_trigger` to `TradeParameters`:

```python
# In trade_contracts.py:
@dataclass(frozen=True)
class TradeParameters:
    ...
    stop_loss_trigger: float        # existing — SL level adjusted for spread
    take_profit: float              # existing — TP at BID price level
    take_profit_trigger: float      # ADD — TP exit price adjusted for spread
    ...
```

**Step 2** — Compute `take_profit_trigger` in `RiskManager.compute_trade_parameters()`:

```python
# After TP calculation, add:

# TP trigger: the actual market price at which the close is executed
# LONG:  TP hit when bid rises to tp level → we sell at Bid (no spread on exit)
#        take_profit_trigger = tp  (same as tp — LONG exit is at Bid)
# SHORT: TP hit when bid falls to tp level → we buy at Ask (pay spread on exit)
#        take_profit_trigger = tp + spread_for_this

take_profit_trigger = (
    tp if is_long
    else tp + spread_for_this
)
```

**Step 3** — Update `TradeSimulator` to use `take_profit_trigger` as the level
that triggers TP exit, and record the spread cost at TP close:

```python
# In TradeSimulator — wherever TP hit is checked:

# BEFORE
if current_price <= trade.params.take_profit:   # SHORT TP hit check

# AFTER
if current_price <= trade.params.take_profit_trigger:  # SHORT TP hit check
# (the trigger is lower than the BID TP, accounting for spread widening the gap)
```

**Step 4** — Update P&L calculation at SHORT TP exit:

```python
# When SHORT TP is hit:
# exit_price = take_profit_trigger = tp_bid + spread
# P&L = entry_bid - exit_price = entry_bid - (tp_bid + spread)
#      = (entry_bid - tp_bid) - spread
# This is automatically correct if exit_price = take_profit_trigger
```

**Step 5** — Add `spread_at_tp_exit` field to `TradeExit` contract for analytics:

```python
@dataclass(frozen=True)
class TradeExit:
    ...
    spread_at_tp_exit: Optional[float] = None  # ADD — spread cost at TP close (SHORT only)
```

**Step 6** — Update `TradeAnalytics` spread cost reporting to include TP exit cost.
Currently only entry spread cost is tracked. Total spread cost per trade is:
- LONG: `spread` (at entry) + 0 (at TP/SL exit, both at Bid)
- SHORT: 0 (at entry, at Bid) + `spread` (at SL exit, buy at Ask) + `spread` (at TP exit, buy at Ask)

This means SHORT trades pay spread **twice** (once at SL, once at TP). This is correct
broker behaviour for BID data — document it explicitly in `SpreadManager` docstring.

**Tests to write**:
- `test_long_tp_trigger_equals_tp_price()`
- `test_short_tp_trigger_equals_tp_plus_spread()`
- `test_short_winning_trade_pnl_deducts_tp_spread()`
- `test_long_winning_trade_pnl_unchanged()`
- `test_short_trade_pays_spread_twice_sl_and_tp()`
- `test_short_trade_pays_spread_once_on_sl_exit()`

---

### Dependency and Sequencing — All Three Features

These three DECs build on each other and on DEC-035. The correct order is:

```
DEC-035 (TradeSimulator → StrategyConfig + AssetConfig schema)
  ↓
DEC-036 (Spread config path wiring — removes hardcoded default)
  ↓
DEC-037 (TP mode selector + R:R ratio restoration — fixes config key path)
  ↓
DEC-038 (SHORT TP exit spread — adds take_profit_trigger)
```

DEC-036 and DEC-037 can be worked in parallel after DEC-035. DEC-038 depends
on DEC-037 (needs the TP calculation to be correct before adjusting the trigger).

**Expected P&L delta when all four are merged**: P&L numbers will change from
the PERFORMANCE_BASELINE_S21 because:
- DEC-036: spread is now actually applied (was silently zero before)
- DEC-037: R:R ratio 5.7 is now applied (was 2.0 default before) → larger TPs → fewer TP hits → changed trade outcome distribution
- DEC-038: SHORT TP wins reduced by one spread each

**Action**: after DEC-038 is merged, establish `PERFORMANCE_BASELINE_S22.md`
as the new non-regression baseline. The S21 baseline becomes historically
invalid once these features are live.

---

### Impact on `strategy_template.yaml`

The template needs three additions to support all restored features. Apply
these after the corresponding DECs are merged:

```yaml
# Add at top level (after DEC-035/DEC-036):
asset:
  symbol: "DEUIDXEUR"    # must match key in broker_spreads.yaml exactly
  pip_size: 0.1          # DAX — verify with broker
  point_size: 1.0        # DAX — verify with broker

# Update trade_management.spread (after DEC-036):
trade_management:
  spread:
    enabled: true
    config_path: "configs/spreads/broker_spreads.yaml"
    # IMPORTANT: spread_type and spread_value below are FALLBACK ONLY.
    # SpreadManager reads actual values from broker_spreads.yaml.
    # For DEUIDXEUR: spread_type=percentage, spread_value=0.015 (≈3.0 pts at DAX 20000).
    # Update these to match the broker file entry for documentation consistency.
    spread_type: "percentage"
    spread_value: 0.015

  # Update trade_management.risk (after DEC-037):
  risk:
    atr_length: 14
    atr_multiplier_sl: 1.4
    tp_mode: "rr_ratio"           # DEFAULT — legacy mode; express TP as R:R
    risk_to_reward_ratio: 5.7     # TP = entry ± ATR × atr_multiplier_sl × rr_ratio
    atr_multiplier_tp: 7.98       # alternative — used only if tp_mode: "atr_multiplier"
                                  # note: 1.4 × 5.7 = 7.98 (equivalent when sl_mult=1.4)
    max_risk_percentile: 0.1
```

---



A scan of `DataLoader`, `SignalGenerator`, `TradeSimulator`, and their sub-managers
(`RiskManager`, `TradeManager`, `SpreadManager`) found **10 violations** of the
fail-fast principle. They are grouped by module and tagged with their resolution path.

**Convention used below**:
- 🔴 **High** — can corrupt results silently (wrong prices, wrong metrics, wrong trade counts)
- 🟡 **Medium** — incorrect behaviour but recoverable or clearly visible at runtime
- 🟢 **Low** — edge case, non-critical path, acceptable as-is

---

### DataLoader (`data_loader.py`) — 5 issues

**DL-1 🔴 Mode silently overridden mid-run**
```python
# CURRENT — load_config()
if self.mode == "analytics" and config_mode == "core":
    self.mode = config_mode   # caller has no idea
    self._verbose = False
```
If the caller passes `mode="analytics"` but the YAML says `execution.mode: core`,
`DataLoader` silently switches to core mode. The caller's intent is overwritten
with no warning. The orchestrator logs `mode=analytics` while the loader
runs in core mode — the log is a lie.

**Fix**: Remove the override block entirely. Mode is the caller's responsibility.
If there is a conflict, raise:
```python
if self.mode != config_mode:
    raise ValueError(
        f"DataLoader was constructed with mode='{self.mode}' but "
        f"config YAML specifies execution.mode='{config_mode}'. "
        f"Pass the same mode to both, or use StrategyOrchestrator which "
        f"sets mode consistently across all modules."
    )
```
**Resolution**: Auto-resolves with DEC-033 — `load_config()` is deleted.
Until then: add the ValueError above as a pre-migration guard.

---

**DL-2 🟡 Missing `execution` section silently defaults mode to `"analytics"`**
```python
execution_cfg = self.raw_config.get("execution", {})
config_mode = execution_cfg.get("mode", "analytics").lower()
```
If the `execution:` key is absent from the YAML, mode defaults to `"analytics"`.
A user with a stripped-down YAML will run analytics mode without knowing it.

**Fix**: Require the key explicitly:
```python
if "execution" not in self.raw_config:
    raise ValueError(
        "YAML config is missing the required 'execution:' section. "
        "Add: execution:\n  mode: core  # or analytics"
    )
```
**Resolution**: Auto-resolves with DEC-033.

---

**DL-3 🟡 Missing `data` section silently gives empty dict → confusing downstream error**
```python
data_section = self.raw_config.get("data", {})
self.data_config = DataConfig.from_yaml_config(data_section, self.project_root)
```
If `data:` is absent, `DataConfig.from_yaml_config({})` is called. It will fail
eventually, but deep inside with a `KeyError` or `AttributeError` that points at
the wrong line, making it hard to diagnose.

**Fix**: Fail at the boundary:
```python
if "data" not in self.raw_config or not self.raw_config["data"]:
    raise ValueError(
        "YAML config is missing the required 'data:' section. "
        "At minimum, data.paths.strategy_ohlcv and data.date_range must be set."
    )
```
**Resolution**: Auto-resolves with DEC-033.

---

**DL-4 🔴 `ltf_timeframe` silently defaults to `"1s"` when LTF data is configured**
```python
ltf_timeframe = self.raw_config["data"].get("ltf_timeframe", "1s")
```
If `ltf_ohlcv` is configured but `ltf_timeframe` is omitted, `"1s"` is assumed.
If the actual LTF file contains 5-second bars, `DataInfo.ltf_timeframe` says `"1s"`,
downstream analytics use the wrong frequency, and resampling produces wrong results.
No error is ever raised.

**Fix** (applied inside DEC-033 when `ltf_timeframe` becomes a typed `DataConfig` field):
```python
# In DataConfig.__post_init__ after DEC-033:
if self.paths.ltf_ohlcv is not None and not self.ltf_timeframe.strip():
    raise ValueError(
        "data.ltf_timeframe is required when data.paths.ltf_ohlcv is set. "
        "Example: ltf_timeframe: '1s'"
    )
```
**Resolution**: Implement this validation as part of DEC-033 `DataConfig.__post_init__`.

---

**DL-5 🔴 `artf_timeframe` silently defaults to `"1ME"` when ARTF data is configured**
Identical problem to DL-4 for the ARTF path.
```python
artf_timeframe = self.raw_config["data"].get("artf_timeframe", "1ME")
```
**Fix**: Same pattern as DL-4 — add `__post_init__` validation in DEC-033:
```python
if self.paths.artf_ohlcv is not None and not self.artf_timeframe.strip():
    raise ValueError(
        "data.artf_timeframe is required when data.paths.artf_ohlcv is set. "
        "Example: artf_timeframe: '1ME'"
    )
```
**Resolution**: Implement as part of DEC-033 `DataConfig.__post_init__`.

---

### SignalGenerator (`signal_generator.py`) — 2 issues

**SG-1 🟡 `htf_period` validated for emptiness only — not for content**
```python
if not htf_period:
    raise ValueError("htf_period configuration is missing.")
```
`"  "` (whitespace), `"INVALID"`, or `"999X"` all pass this guard.
`WBWSTrigger` will then call `df.resample("999X")` which raises a confusing
pandas `ValueError` deep in indicator computation — not at the config boundary.

**Fix**: Strip and validate against known pandas offset aliases:
```python
_VALID_HTF_PERIODS = {"1min", "5min", "15min", "30min", "1H", "2H", "4H", "1D", "1W"}

htf_period = htf_period.strip()
if not htf_period:
    raise ValueError(
        "data.htf_period cannot be blank. "
        f"Valid values: {sorted(_VALID_HTF_PERIODS)}"
    )
if htf_period not in _VALID_HTF_PERIODS:
    raise ValueError(
        f"data.htf_period='{htf_period}' is not a recognised period. "
        f"Valid values: {sorted(_VALID_HTF_PERIODS)}"
    )
```
**Resolution**: Implement inside DEC-034 when `SignalGenerator` accepts `StrategyConfig`.
Add `_VALID_HTF_PERIODS` as a module-level constant.

---

**SG-2 🔴 Empty HTF DataFrame not caught — `None` check is insufficient**
```python
if data_bundle.strategy is None or data_bundle.htf is None:
    raise ValueError("data_bundle.strategy and data_bundle.htf are required")
```
`data_bundle.htf` being an empty DataFrame (`len == 0`) passes this guard.
`WBWSTrigger` then calls `df_htf.resample(...)` on an empty DataFrame and
produces an empty signal Series — zero signals generated, no error, no log.
The run completes silently with 0 trades. Very hard to diagnose.

**Fix**: Check both `None` and empty:
```python
if data_bundle.strategy is None or data_bundle.strategy.empty:
    raise ValueError(
        "data_bundle.strategy is missing or empty. "
        "Verify data.paths.strategy_ohlcv points to a valid file with data "
        "in the configured date_range."
    )
if data_bundle.htf is None or data_bundle.htf.empty:
    raise ValueError(
        "data_bundle.htf is missing or empty. "
        "Verify data.paths.htf_ohlcv exists and covers the configured date_range. "
        "htf data is required by SignalGenerator — it cannot be omitted."
    )
```
**Resolution**: Implement inside DEC-034 at the top of `generate_signals()` /
`generate()`. This guard stays regardless of config migration.

---

### TradeSimulator (`trade_simulator.py`) — 2 issues

**TS-1 🔴 Empty `asset.symbol` silently accepted when spread is enabled**
```python
asset_symbol = self.config.get("asset", {}).get("symbol", "")
if spread_config.get("enabled", False):
    self.spread_manager = SpreadManager(asset_symbol, config_path)
```
If `asset:` is missing from the config dict, `asset_symbol=""`. `SpreadManager`
is constructed with an empty symbol. Depending on `SpreadManager` internals,
it either returns 0 spread silently (all trades execute with no spread cost,
overstating P&L) or loads the wrong spread entry.

**Fix**: Guard before constructing `SpreadManager`:
```python
if spread_config.get("enabled", False):
    asset_symbol = self.config.get("asset", {}).get("symbol", "").strip()
    if not asset_symbol:
        raise ValueError(
            "trade_management.spread.enabled is True but asset.symbol is missing "
            "or blank in config. SpreadManager requires a non-empty asset symbol. "
            "Add: asset:\n  symbol: 'EURUSD'"
        )
    self.spread_manager = SpreadManager(asset_symbol, config_path)
```
**Resolution**: Implement now as a pre-DEC-035 guard. Auto-tightens after DEC-035
when `AssetConfig.__post_init__` validates `symbol` at construction time.

---

**TS-2 🔴 Unknown exit reason silently corrupted to `END_OF_DATA`**
```python
except KeyError:
    logger.warning(f"Unknown exit reason '{exit_reason}', using END_OF_DATA")
    exit_reason_enum = ExitReason.END_OF_DATA
```
An unknown exit reason string means there is a bug in the calling code —
a string was passed that is not a member of `ExitReason`. Silently treating it
as `END_OF_DATA` corrupts `exits_by_reason` statistics: a stop-loss exit is
counted as end-of-data. `MetricsCalculator` then produces wrong drawdown and
exit distribution figures. The warning is easy to miss in a long log.

**Fix**: Raise — this is a programming error, not a runtime condition:
```python
try:
    exit_reason_enum = ExitReason[exit_reason]
except KeyError:
    raise ValueError(
        f"Unknown exit reason '{exit_reason}'. "
        f"Valid values: {[e.name for e in ExitReason]}. "
        f"This is a code defect — exit_reason must always be a valid ExitReason name."
    ) from None
```
**Resolution**: Implement now — this fix is independent of DEC-035 and takes 5 minutes.
It is the highest-priority standalone fix in this audit.

---

### Sub-managers — 1 issue confirmed, 1 flagged for verification

**SM-1 🔴 `SpreadManager` likely accepts blank symbol silently** *(inferred — source not uploaded)*

From `TradeSimulator.initialize_managers()` we know `SpreadManager(asset_symbol, config_path)`
is called where `asset_symbol` can be `""`. Whether `SpreadManager.__init__` guards
against this is unknown without its source. **High probability it does not**, given the
pattern seen in every other pre-migration module.

**Action**: When uploading `spread_manager.py` for DEC-035 review, verify:
```python
# SpreadManager.__init__ should have:
if not asset_symbol or not asset_symbol.strip():
    raise ValueError(
        f"SpreadManager requires a non-empty asset_symbol. Got: '{asset_symbol}'"
    )
```
If this guard is absent, add it as part of DEC-035.

---

**SM-2 🟡 `SpreadManager` likely uses hardcoded fallback path when `config_path=None`** *(inferred)*

`config_path = spread_config.get("config_path")` returns `None` if absent.
If `SpreadManager` then falls back to a hardcoded path like
`"configs/spreads/broker_spreads.yaml"`, trades execute with whatever spreads
that file contains — which may be wrong for the configured asset.

**Action**: Verify in `spread_manager.py` source. If a fallback path exists:
```python
# Replace:
self.spread_config_path = config_path or "configs/spreads/broker_spreads.yaml"

# With:
if config_path is None:
    raise ValueError(
        "SpreadManager requires an explicit config_path when spread is enabled. "
        "Add trade_management.spread.config_path to your strategy YAML, "
        "or set spread.enabled: false."
    )
self.spread_config_path = Path(config_path)
if not self.spread_config_path.exists():
    raise FileNotFoundError(
        f"Spread config not found: {self.spread_config_path}. "
        f"Verify trade_management.spread.config_path in your strategy YAML."
    )
```

---

### Fail-Fast Audit Summary

| ID | Module | Severity | Status | Resolution |
|----|--------|----------|--------|------------|
| DL-1 | DataLoader | 🟡 Medium | Pre-migration defect | Auto-resolves: DEC-033 |
| DL-2 | DataLoader | 🟡 Medium | Pre-migration defect | Auto-resolves: DEC-033 |
| DL-3 | DataLoader | 🟡 Medium | Pre-migration defect | Auto-resolves: DEC-033 |
| DL-4 | DataLoader | 🔴 High | Fix in DEC-033 | `DataConfig.__post_init__` validation |
| DL-5 | DataLoader | 🔴 High | Fix in DEC-033 | `DataConfig.__post_init__` validation |
| SG-1 | SignalGenerator | 🟡 Medium | Fix in DEC-034 | Validate `htf_period` against known offsets |
| SG-2 | SignalGenerator | 🔴 High | Fix in DEC-034 | Add `.empty` check alongside `is None` |
| TS-1 | TradeSimulator | 🔴 High | Fix NOW (5 min) | Guard `asset_symbol` before SpreadManager |
| TS-2 | TradeSimulator | 🔴 High | Fix NOW (5 min) | Raise on unknown exit reason — not warn |
| SM-1 | SpreadManager | 🔴 High | Verify in DEC-035 | Check/add blank symbol guard |
| SM-2 | SpreadManager | 🟡 Medium | Verify in DEC-035 | Check/remove hardcoded fallback path |

**Two fixes should be applied before the first E2E run** (TS-1 and TS-2) —
they are standalone, take minutes each, and prevent silent result corruption
that would make the E2E baseline numbers unreliable.

---

## All Carry-Forward Items

Do not start any of these until the E2E run is green and PERFORMANCE_BASELINE_S21 is locked.

**CF-1 — AnalyticsConfig contract (DEC-032)**
Add `AnalyticsConfig` frozen dataclass. Update `TradeAnalytics.analyze()` to accept it. Replace 4 hardcoded threshold constants. Default values must match current hardcodes — behaviour unchanged.

**CF-2 — TimeFilter typed parameters (P1-CH3-8)**
`TimeFilter.__init__` currently accepts `config: Dict`. Replace with `TimeFilterConfig` frozen dataclass. Two-file atomic change: `time_filter.py` + `filter_pipeline.py`.

**CF-3 — New tests (post CF-1 and CF-2)**
- `test_analytics_config_defaults_match_legacy_constants()`
- `test_analytics_config_custom_thresholds_used_in_insights()`
- `test_wbws_v2_yaml_loads()`
- `test_time_filter_rejects_raw_dict()`
- `test_orchestrator_core_mode_returns_metrics_report()`
- `test_orchestrator_clears_risk_manager_cache_between_runs()`

**CF-6 — Move signal translation into `TradeSimulator`**
The `_SIGNAL_CODE_TO_STR` map and `.map().dropna()` in the orchestrator is a smell — the orchestrator should not know about int8 signal codes. After DEC-035, add a `from_signal_frame(signal_frame: SignalFrame) -> pd.Series` classmethod to `TradeSimulator` or a contract helper, and remove the translation from the orchestrator.

**CF-7 — Risk rejection vs position rejection metrics separation**
`TradeSimulator` currently increments `position_rejected_count` for both risk-rejected and position-rejected signals. In `OrchestratorResult` the distinction is lost. After DEC-035, add a separate `risk_rejected_count` counter. This is a metrics accuracy issue, not a correctness issue.

**Target test count after Session 21 E2E run**: ~310
**Target test count after DEC-033 + DEC-034 + DEC-035**: ~325
**Target test count after DEC-036 + DEC-037 + DEC-038**: ~345

---

## Files Required Before Implementing Each DEC

The table below lists every source file that must be read before starting each DEC.
Files marked ✅ are already in context (uploaded this or prior sessions).
Files marked ❌ must be uploaded at the start of the session implementing that DEC.

| DEC | Files required | Status |
|-----|---------------|--------|
| DEC-033 | `src/config/config_schema.py`, `src/strategies/specific/modules/data_loader.py` | ❌ both needed |
| DEC-034 | `src/strategies/specific/modules/signal_generator.py` | ❌ needed |
| DEC-035 | `src/config/config_schema.py`, `src/strategies/specific/modules/trade_simulator.py`, `src/strategies/specific/modules/trade_manager.py`, `src/strategies/contracts/trade_contracts.py` | ❌ all needed |
| DEC-036 | `src/strategies/specific/modules/spread_manager.py` | ✅ in context |
| DEC-037 | `src/config/config_schema.py`, `src/strategies/specific/modules/risk_manager.py`, `src/strategies/contracts/trade_contracts.py` | `risk_manager.py` ✅, others ❌ |
| DEC-038 | `src/strategies/specific/modules/risk_manager.py`, `src/strategies/specific/modules/trade_simulator.py`, `src/strategies/contracts/trade_contracts.py` | `risk_manager.py` ✅, others ❌ |

**Modified files deliverable this session** (full source available):

Two files can be produced now because their full source is in context:
- `spread_manager.py` — DEC-036 changes (Steps 1 and 5: fail-fast path, `_load_global_settings`)
- `risk_manager.py` — DEC-037 + DEC-038 changes (TP mode branch, `take_profit_trigger`)
- `strategy_template.yaml` — all template additions (asset, spread.config_path, tp_mode, rr_ratio)

Three files require upload before modification:
- `src/config/config_schema.py` — needed for DEC-033, DEC-035, DEC-037 schema changes
- `src/strategies/contracts/trade_contracts.py` — needed for DEC-037/DEC-038 (`TradeParameters`)
- `src/strategies/specific/modules/trade_simulator.py` — needed for DEC-035/DEC-038

---



- **Architecture locked**: no contract or interface change without a DEC entry first.
- **Non-regression rule**: once PERFORMANCE_BASELINE_S21 is locked, any stage >5% slower = P0 blocker.
- **Atomic commits**: each DEC is one commit. Never commit a half-migrated module.
- **No MagicMock in any new test**: real dataclasses only.
- **STATUS rule**: update the STATUS line at the top of this file if the session ends before the green E2E run.

---

## Key Contract Quick Reference

**StrategyConfig** (`src/config/config_schema.py`)
Sub-configs: `.data`, `.execution`, `.trade_management`, `.filters`, `.output`.
Mode: `config.execution.mode` → `"core"` or `"analytics"`.

**DataBundle** (`src/strategies/contracts/data_contracts.py`)
Key: `.strategy` (date-sliced DataFrame), `.info` (DataInfo), `.has_htf` (bool property), `.has_ltf` (bool property).
DataInfo fields: `.strategy_bars`, `.total_bars`, `.htf_bars`, `.ltf_bars`, `.cache_hit`.

**SignalFrame** (`src/strategies/contracts/signal_contracts.py`)
Key: `.signals` (int8 Series: 1=BUY, 2=SELL, 0=none). Methods: `.count_by_type()`, `.iter_raw()`.
Do not call `__iter__` in core mode — use `iter_raw()`.

**FilterPipelineResult** (`src/strategies/contracts/filter_contracts.py`)
Key: `.final_signals` (SignalFrame), `.raw_count`, `.final_count`, `.pass_rate` (0–100 already).

**TradeResult** (`src/strategies/contracts/trade_contracts.py`)
Key: `.trades` (List[Trade]), `.win_count`, `.loss_count`, `.total_pnl_points`, `.execution_mode`.
⚠️ No `.total_trades` field — use `MetricsReport.total_trades` via `OrchestratorResult.total_trades`.

**MetricsReport** (`src/strategies/contracts/metrics_contracts.py`)
Key: `.total_trades`, `.win_rate`, `.total_pnl_points`, `.expectancy_points`, `.profit_factor`, `.max_drawdown`.

**OrchestratorResult** (`src/strategies/orchestrator.py`)
Key: all stage outputs + `.stage_durations_ms` + `.total_duration_ms` + `.mode`.
Convenience: `.total_trades`, `.win_rate`, `.total_pnl_points`, `.summary()`.