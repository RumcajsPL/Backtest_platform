# WBWSStrategy System Architecture
**Version**: 2.2.0  
**Date**: 2026-02-20  

---

## Table of Contents
1. [Who Should Read This](#who-should-read-this)
2. [System Overview](#system-overview)
3. [Architecture Principles](#architecture-principles)
4. [Execution Modes](#execution-modes)
5. [Module Responsibilities](#module-responsibilities)
6. [Contract Reference](#contract-reference)
7. [Data Flow](#data-flow)
8. [File Organisation](#file-organisation)
9. [Path Resolution](#path-resolution)
10. [Integration Guide](#integration-guide)
11. [Design Patterns](#design-patterns)
12. [Extension Points](#extension-points)

---

## Who Should Read This

This document serves three developer profiles. Find yours and use it as a reading guide.

**Modifying an existing component** — Read [Architecture Principles](#architecture-principles), the relevant section in [Contract Reference](#contract-reference), and [Design Patterns](#design-patterns). Every module has a single responsibility and communicates only through typed contracts. Understand the contract before touching any module.

**Building a new strategy on this architecture** — Read [Execution Modes](#execution-modes), [Integration Guide](#integration-guide), and [Extension Points](#extension-points). The pipeline is strategy-agnostic from `FilterPipeline` onward; only `DataLoader`, `SignalGenerator`, and strategy-specific filters need to be implemented or swapped.

**Building a backtesting environment** — Read [Execution Modes](#execution-modes) carefully — the `core` mode exists specifically for multi-run backtesting. Then read [Module Responsibilities](#module-responsibilities) and [Contract Reference](#contract-reference) to understand what each stage produces and consumes. The pipeline is designed to be called in a loop with `clear_cache()` between runs.

---

## System Overview

WBWSStrategy is a **contract-based backtesting engine** with **analytics** and **HTML reporting** for systematic trading strategies. It processes market data through a typed pipeline, generating trade signals, simulating realistic execution with configurable spread and risk management, and producing actionable insights and self-contained HTML reports.

### Key Characteristics

- **Contract-based**: End-to-end typed, frozen dataclasses. No dict-based communication between modules.
- **Dual execution modes**: `core` for maximum throughput in multi-run backtesting; `analytics` for full insight and reporting pipeline.
- **Performance**: Vectorised hot paths throughout. LTF tick processing is gated to `analytics` mode only.
- **Type safe**: 100% type hints, strict mypy.
- **Modular**: Each module has exactly one responsibility. The pipeline is: data → signals → filters → trades → analytics → reports.

### Pipeline at a Glance

```
Market Data (CSV / Parquet)
        │
        ▼
   DataLoader          →  DataBundle
        │
        ▼
 SignalGenerator        →  SignalFrame
        │
        ▼
  FilterPipeline        →  FilterPipelineResult
   ├─ TimeFilter
   └─ Technical Filters (ADX, RSI, Bollinger, …)
        │
        ▼
  TradeSimulator        →  TradeResult
   ├─ SpreadManager
   ├─ RiskManager
   └─ TradeManager
        │
        ├──────────────────────────┐
        ▼                          ▼
MetricsCalculator        TradeAnalytics          (analytics mode only)
        │                          │
        ▼                          ▼
  MetricsReport           AnalyticsReport
                                   │
                                   ▼
                          ReportGenerator         (analytics mode only)
                                   │
                                   ▼
                            HTML Report
```

---

## Architecture Principles

### 1. Single Responsibility
One module, one concern. `DataLoader` only loads data. `SignalGenerator` only generates signals. `MetricsCalculator` only computes metrics. No module reaches into another module's domain.

### 2. Contracts Are the Interface
Every module accepts and returns typed, frozen dataclasses. There are no raw dicts, no shared state, no global variables passed between modules. If you need to add information that crosses a module boundary, add a field to the relevant contract — do not bypass the contract.

### 3. Immutability
All contracts use `frozen=True`. Any module that needs to derive a field at construction time uses `object.__setattr__` in `__post_init__` — that is the only acceptable use. After construction, contracts are read-only.

### 4. Explicit Over Implicit
No hidden defaults buried in logic. Mode-gated behaviour (`core` vs `analytics`) is explicit at every call site. Expensive operations (LTF precomputation, progressive tracking, signal ID lookups) run only when the mode requires them.

### 5. Vectorisation First
Hot paths use numpy/pandas vectorised operations. Python loops appear only where the logic cannot be vectorised (e.g. stateful trade management). ATR computation and spread config loading are cached at class level — call `RiskManager.clear_cache()` between backtester runs.

### 6. Fail Fast
Invalid configuration raises immediately at construction via `__post_init__` validation. There are no silent fallbacks, no auto-corrections of bad input, no "legacy compatibility" adapters. If a value is wrong, the system tells you before any computation begins.

---

## Execution Modes

The pipeline has two execution modes, selected via `execution.mode` in the strategy config YAML.

| Mode | Purpose | What runs | Typical use |
|------|---------|-----------|-------------|
| `core` | Maximum throughput | Data load, signal gen, filter, trade sim, MetricsCalculator | Multi-run parameter sweep / backtester loop |
| `analytics` | Full pipeline | Everything in `core` + TradeAnalytics + ReportGenerator + LTF execution | Single-run analysis, report generation |

**Important**: The string `"debug"` is not a valid mode. Passing `mode="debug"` raises a `ValueError` with a migration message. Always use `"analytics"`.

### Mode in Config YAML
```yaml
execution:
  mode: "analytics"   # or "core"
```

### Mode in Code
```python
trade_result = simulator.simulate_trades(
    signal_frame=signal_frame,
    data_bundle=data_bundle,
    mode=config.execution.mode,   # pass through from config — never hardcode
)
```

### Multi-run Backtesting Pattern
```python
for params in parameter_grid:
    config = build_config(params)
    RiskManager.clear_cache()          # mandatory between runs
    result = run_pipeline(config, mode="core")
    results.append(result)
```

---

## Module Responsibilities

### DataLoader
**File**: `src/strategies/specific/modules/data_loader.py`  
**Input**: File paths + `DataConfig`  
**Output**: `DataBundle`

Loads OHLCV data for the strategy timeframe, and optionally HTF, LTF, and ARTF (monthly bars). Validates all DataFrames (DatetimeIndex, OHLC columns present). Applies Parquet optimisation sequence: timestamp floor → sort index → lazy duplicate check. Caches loaded data by file mtime + size + version string.

ARTF data is never date-sliced (monthly bars span the full file). All other DataFrames are sliced to the `date_range` in config.

### SignalGenerator
**File**: `src/strategies/specific/modules/signal_generator.py`  
**Input**: `DataBundle`  
**Output**: `SignalFrame`

Generates BUY/SELL signals by delegating to a strategy-specific indicator (e.g. `WBWSTrigger`). Signals are stored as `int8` (1=BUY, 2=SELL, 0=none) for memory efficiency. HTF alignment uses `shift(1)` — no lookahead. `indicator_data` (full indicator DataFrame) is only populated in `analytics` mode; in `core` mode it is `None`.

### FilterPipeline
**File**: `src/strategies/specific/modules/filter_pipeline.py`  
**Input**: `SignalFrame` + `StrategyConfig`  
**Output**: `FilterPipelineResult`

Runs signals through a two-stage filter: time filters first (session, day-of-week), then technical filters (ADX, RSI, Bollinger, CCI, Choppiness, DPO, MA, MACD, Pivot, Supertrend). Filter results are cached by a key that includes the data fingerprint and a hash of the filter configuration. The cache hash is computed once at `__init__` — changing filter parameters between runs requires a new `FilterPipeline` instance.

Logging is gated to `analytics` mode. In `core` mode the pipeline runs with zero logging overhead.

### TradeSimulator
**File**: `src/strategies/specific/modules/trade_simulator.py`  
**Sub-modules**: `SpreadManager`, `RiskManager`, `TradeManager`  
**Input**: `FilterPipelineResult` + `DataBundle`  
**Output**: `TradeResult`

Simulates trade execution bar by bar. LTF tick data precomputation, progressive tracking, and signal ID lookups are gated to `analytics` mode. In `core` mode the simulator runs the minimum path.

**SpreadManager**: Loads spread config from YAML once (cached at class level). Applies spread to entry prices.  
**RiskManager**: Computes ATR-based stop-loss and take-profit. ATR arrays are cached at class level by `(data_id, atr_length)`. Call `RiskManager.clear_cache()` between backtester runs.  
**TradeManager**: Manages open positions, handles entry/exit logic, enforces max concurrent trades.

### MetricsCalculator
**File**: `src/strategies/specific/modules/metrics_calculator.py`  
**Input**: `TradeResult`  
**Output**: `MetricsReport`

Computes 17 core performance metrics (win rate, profit factor, expectancy, drawdown, streaks, trades per week/day, etc.). Runs in both modes. Typical runtime: ~2ms for 1000 trades.

```python
from src.strategies.specific.modules.metrics_calculator import calculate_metrics
metrics = calculate_metrics(trade_result)
```

### TradeAnalytics
**File**: `src/strategies/specific/modules/trade_analytics.py`  
**Input**: `TradeResult` + `StrategyConfig` (+ optional `MetricsReport`)  
**Output**: `AnalyticsReport`  
**Mode**: `analytics` only

Generates AI-like insights across four dimensions: time performance (by session, hour, day), trade quality (win/loss distribution, duration analysis), risk-adjusted metrics (return over max drawdown, consistency score, recovery factor), and an executive summary with a performance grade (A+ through F).

```python
from src.strategies.specific.modules.trade_analytics import TradeAnalytics
report = TradeAnalytics.analyze(trade_result, config)             # auto-computes metrics
report = TradeAnalytics.analyze(trade_result, config, metrics=m)  # reuse pre-computed
```

### ReportGenerator
**File**: `src/strategies/specific/modules/report_generator.py`  
**Input**: `AnalyticsReport` + optional `TradeResult` + `ReportConfig`  
**Output**: `GeneratedReport` (HTML file + content string)  
**Mode**: `analytics` only

Produces a single self-contained HTML file (~32KB). Features: three tabs (Executive / Analytical / Raw Data), four Chart.js charts (equity curve, session bar, win/loss distribution, duration doughnut), dark/light theme, mobile-responsive layout, lazy chart initialisation, CDN failure handler, `<noscript>` fallback.

If `trade_result` is provided but its trade count does not match `analytics_report.input_metrics.total_trades`, the equity curve is skipped and a warning is logged. This prevents misleading charts from mismatched inputs.

---

## Contract Reference

### Data Layer

#### DataBundle
```python
@dataclass(frozen=True)
class DataBundle:
    full: pd.DataFrame           # Complete dataset (no date slicing)
    strategy: pd.DataFrame       # Date-sliced to config.date_range
    htf: Optional[pd.DataFrame]  # Higher timeframe (e.g. 1H)
    ltf: Optional[pd.DataFrame]  # Lower timeframe (e.g. 1s tick)
    artf: Optional[pd.DataFrame] # Monthly bars (never date-sliced)
    info: DataInfo               # Bar counts, date range, load duration
    validation: DataValidationResult
    config: Optional[DataConfig]
```
**Key methods**: `has_htf()`, `has_ltf()`, `has_artf()`  
**Validation**: All DataFrames must have `DatetimeIndex` and OHLC columns.

---

### Signal Layer

#### SignalFrame
```python
@dataclass(frozen=True)
class SignalFrame:
    signals: pd.Series           # int8: 1=BUY, 2=SELL, 0=none
    indicator_data: Optional[pd.DataFrame]  # None in core mode
    signal_metadata: Dict[str, Any]
```
**Key methods**:
- `count_by_type()` → `{"buy": int, "sell": int, "total": int}`
- `iter_raw()` → fast iterator yielding `(timestamp, code)` — use this in hot paths
- `buy_signals`, `sell_signals` properties

**Important**: `__iter__` requires `indicator_data` to be populated (analytics mode). In core mode use `iter_raw()`.

#### SignalType
```python
class SignalType(Enum):
    BUY = auto()   # Code: 1
    SELL = auto()  # Code: 2
```
`SignalType.from_code(1)` → `SignalType.BUY`

---

### Filter Layer

#### FilterPipelineResult
```python
@dataclass(frozen=True)
class FilterPipelineResult:
    final_signals: SignalFrame
    raw_count: int
    time_filtered_count: int
    technical_filtered_count: int
    final_count: int
    filter_results: List[FilterMetadata]
    rejection_reasons: Dict[str, int]
    execution_time_ms: Optional[float]
```
**Key properties**: `pass_rate`, `total_rejection_count`, `get_stats_summary()`

#### FilterResult
```python
@dataclass(frozen=True)
class FilterResult:
    passed: bool
    signal_frame: SignalFrame
    metadata: FilterMetadata
```

#### FilterStatus
```python
class FilterStatus(Enum):
    PASSED = auto()
    REJECTED = auto()
    SKIPPED = auto()
    ERROR = auto()
```

---

### Trade Layer

#### TradeResult
```python
@dataclass(frozen=True)
class TradeResult:
    trades: List[Trade]
    rejected_signals: List[RejectedSignal]
    total_entries: int
    total_opened: int
    total_closed: int
    total_rejected: int
    currently_open: int
    exits_by_reason: Dict[str, int]
    risk_approved: int
    risk_rejected: int
    risk_adjusted: int
    position_rejected: Dict[str, int]
    win_count: int
    loss_count: int
    win_rate: float
    total_pnl_points: float
    execution_mode: str            # "core" or "analytics"
    execution_time_ms: Optional[float]
```

#### Trade
```python
@dataclass(frozen=True)
class Trade:
    entry: TradeEntry
    exit: Optional[TradeExit] = None
```
**Key properties**: `is_open`, `is_closed`, `pnl_points`, `is_win`, `is_loss`

#### TradeEntry
```python
@dataclass(frozen=True)
class TradeEntry:
    entry_id: str
    trade_manager_id: Optional[int]
    signal_id: Optional[int]       # None in core mode
    entry_time: pd.Timestamp
    direction: TradeDirection      # LONG = 1 | SHORT = -1
    entry_price: float
    stop_loss: float
    take_profit: float
    position_size: float
    sl_distance: float
    tp_distance: float
    risk_reward_ratio: float
    atr_value: Optional[float]
    spread_enabled: bool
    spread_points: Optional[float]
    sl_adjusted: bool
    comment: Optional[str]
```

#### TradeExit
```python
@dataclass(frozen=True)
class TradeExit:
    exit_id: str
    entry_id: str
    exit_time: pd.Timestamp
    duration_bars: int
    duration_minutes: float
    exit_price: float
    exit_reason: ExitReason
    pnl_points: float
    pnl_percent: float
    is_win: bool
    is_loss: bool
    exit_bar_high: Optional[float]
    exit_bar_low: Optional[float]
    ltf_execution: bool            # True only in analytics mode with LTF data
```

#### ExitReason
```python
class ExitReason(Enum):
    STOP_LOSS = auto()
    TAKE_PROFIT = auto()
    OPPOSITE_SIGNAL = auto()
    END_OF_DATA = auto()
    MANUAL = auto()
    TIME_EXIT = auto()
```

#### RejectedSignal
```python
@dataclass(frozen=True)
class RejectedSignal:
    rejection_id: str
    signal_id: Optional[int]
    rejection_time: pd.Timestamp
    direction: str
    rejection_stage: str
    rejection_reason: str
    current_price: Optional[float]
    meta: Dict[str, Any]
```

#### TradeParameters
```python
@dataclass(frozen=True)
class TradeParameters:
    entry_price_mid: float
    entry_price_executed: float
    stop_loss_raw: float
    stop_loss_trigger: float
    take_profit: float
    position_size: float
    atr_value: Optional[float]
    atr_length: Optional[int]
    sl_distance: Optional[float]
    tp_distance: Optional[float]
    risk_reward_ratio: Optional[float]
    annual_range_value: Optional[float]
    risk_percentile_calculated: Optional[float]
    max_risk_percentile: Optional[float]
    risk_percentile_passed: bool
    spread_enabled: bool
    spread_applied: bool
    spread_points: Optional[float]
    sl_adjusted: bool
```

---

### Analytics Layer

#### MetricsReport
```python
@dataclass(frozen=True)
class MetricsReport:
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float              # 0–100
    total_pnl_points: float
    expectancy_points: float
    profit_factor: float
    avg_pnl_points: float
    largest_win: float
    largest_loss: float
    max_drawdown: float          # Negative value
    losing_streak: int
    winning_streak: int
    trades_per_week: float
    trades_per_day: float
    execution_duration_ms: float
    execution_date: str
```
**Key methods**: `to_dict()`, `to_json()`, `to_flat_dict()`

#### AnalyticsReport
```python
@dataclass(frozen=True)
class AnalyticsReport:
    executive_summary: ExecutiveSummary
    time_performance: TimePerformanceBreakdown
    trade_quality: TradeQualityAnalysis
    risk_adjusted: RiskAdjustedMetrics
    comparative: Optional[ComparativeContext]
    input_metrics: MetricsReport
    analysis_timestamp: str
    analysis_duration_ms: float
```
**Key methods**: `to_dict()`, `to_json()`, `get_all_insights()`, `get_critical_insights_only()`, `get_insights_by_category(category)`

#### ExecutiveSummary
```python
@dataclass(frozen=True)
class ExecutiveSummary:
    performance_grade: str          # "A+" through "F"
    grade_reasoning: str
    critical_insights: List[Insight]  # Up to 7
    key_strengths: List[str]
    improvement_areas: List[str]
    overall_assessment: str
```

**Grading algorithm** (100 points total, four dimensions of 25 each):

| Dimension | 25 pts | 20 pts | 15 pts | 10 pts | 5 pts |
|-----------|--------|--------|--------|--------|-------|
| Win rate | ≥20% | — | ≥15% | ≥10% | — |
| Profit factor | ≥2.0 | ≥1.5 | — | ≥1.2 | — |
| Drawdown vs profit | DD < 20% | — | DD < 50% | — | DD < 100% |
| Consistency | score ≥70 | — | score ≥50 | — | score ≥30 |

Score → Grade: 90+ = A+, 85 = A, 80 = A−, 75 = B+, 70 = B, 65 = B−, 60 = C+, 55 = C, 50 = C−, 40 = D+, 30 = D, <30 = F

#### Insight
```python
@dataclass(frozen=True)
class Insight:
    message: str
    recommendation: str
    confidence: str          # "High" | "Medium" | "Low"
    impact_estimate: Optional[str]
    category: str            # "time" | "quality" | "risk" | "general"
    severity: str            # "critical" | "warning" | "info" | "success"
```

#### TimePerformanceBreakdown
```python
@dataclass(frozen=True)
class TimePerformanceBreakdown:
    by_session: Dict[str, SessionMetrics]   # "Asia" | "London" | "NewYork"
    by_hour: Dict[int, SessionMetrics]      # 0–23 (zero-trade hours excluded)
    by_day: Dict[str, SessionMetrics]       # "Monday"–"Sunday"
    best_session: str
    worst_session: str
    insights: List[Insight]
```

#### TradeQualityAnalysis
```python
@dataclass(frozen=True)
class TradeQualityAnalysis:
    win_distribution: TradeDistribution     # small/medium/large buckets
    loss_distribution: TradeDistribution
    duration_analysis: DurationAnalysis
    avg_bars_to_profit: Optional[float]
    avg_bars_to_loss: Optional[float]
    premature_exit_estimate: str
    insights: List[Insight]
```

#### RiskAdjustedMetrics
```python
@dataclass(frozen=True)
class RiskAdjustedMetrics:
    return_over_max_dd: float       # total PnL / max drawdown
    avg_win_over_avg_loss: float    # realised risk/reward
    expectancy_per_trade: float
    consistency_score: float        # 0–100, CV-based
    recovery_factor: float          # total PnL / gross losses
    insights: List[Insight]
```

---

### Reporting Layer

#### ReportConfig
```python
@dataclass(frozen=True)
class ReportConfig:
    title: str = "Strategy Performance Report"
    output_dir: Path = Path("outputs/reports")
    include_raw_data: bool = True
    theme: str = "dark"              # "dark" | "light"
    chart_height_px: int = 300       # 100–800
    subtitle: Optional[str] = None
    brand_name: str = "WBWSStrategy" # Shown in report header and footer
    timezone: str = "CET"            # Informational only — data is not converted
```
**Validation**: `theme` must be `"dark"` or `"light"`. `chart_height_px` must be 100–800. `brand_name` must not be blank.

#### GeneratedReport
```python
@dataclass(frozen=True)
class GeneratedReport:
    html_path: Path                   # Absolute path to saved file
    html_content: str                 # Full HTML string (use for tests)
    generation_duration_ms: float
    analytics_report: AnalyticsReport
    layers_included: List[str]        # ["executive", "analytical"] or + "raw"
```
**Key methods**: `to_dict()`, `to_json()`

---

## Data Flow

### Full Analytics Run (step by step)

```python
# 1. Load data
bundle = DataLoader(config).load()                          # → DataBundle

# 2. Generate signals
signals = SignalGenerator(config).generate(bundle)          # → SignalFrame

# 3. Filter signals
filtered = FilterPipeline(config).run(signals)              # → FilterPipelineResult

# 4. Simulate trades
result = TradeSimulator(config).simulate_trades(
    signal_frame=filtered.final_signals,
    data_bundle=bundle,
    mode="analytics",
)                                                           # → TradeResult

# 5. Compute metrics (auto-computed inside analyze(), or do it explicitly)
metrics = calculate_metrics(result)                         # → MetricsReport

# 6. Generate insights
analytics = TradeAnalytics.analyze(result, config, metrics=metrics)  # → AnalyticsReport

# 7. Generate HTML report
generated = ReportGenerator.generate(
    analytics,
    trade_result=result,
    config=ReportConfig(
        title="My Strategy Report",
        brand_name="MyStrategy",
        output_dir=Path("outputs/strategies/reports"),
        theme="dark",
    ),
)                                                           # → GeneratedReport
```

### Core Run (backtester loop)

```python
results = []
for params in parameter_grid:
    config = build_config(params)
    RiskManager.clear_cache()           # clear ATR cache between runs

    bundle  = DataLoader(config).load()
    signals = SignalGenerator(config).generate(bundle)
    filtered = FilterPipeline(config).run(signals)
    result  = TradeSimulator(config).simulate_trades(
        signal_frame=filtered.final_signals,
        data_bundle=bundle,
        mode="core",                    # no LTF, no analytics, no reporting
    )
    metrics = calculate_metrics(result)
    results.append((params, metrics))
```

---

## File Organisation

```
project_root/
├── configs/
│   ├── spreads/
│   │   └── broker_spreads.yaml          # Centralised broker spread config
│   ├── data/
│   │   └── data_aggregator.yaml         # Settings for OHLCV parquet generation
│   └── strategies/
│       └── strategy_template.yaml       # Generic strategy config template
│
├── data/
│   ├── raw/
│   │   └── dukascopy_bi5/               # Tick data (.bi5), organised by instrument/date
│   └── processed/
│       └── ohlcv/                       # OHLCV parquet/CSV files (all instruments, all TFs)
│
├── outputs/
│   └── strategies/
│       ├── logs/
│       │   └── wbws/
│       └── reports/
│           └── wbws/
│
├── scripts/
│   └── validation/
│       └── validate_strategy_data.py
│
└── src/
    └── strategies/
        ├── contracts/
        │   ├── data_contracts.py        # DataBundle, DataInfo
        │   ├── signal_contracts.py      # SignalFrame, SignalType
        │   ├── filter_contracts.py      # FilterResult, FilterPipelineResult
        │   ├── trade_contracts.py       # Trade, RejectedSignal, TradeResult
        │   ├── market_contracts.py      # MarketFrame
        │   ├── position_contracts.py    # Position
        │   ├── metrics_contracts.py     # MetricsReport
        │   ├── analytics_contracts.py   # AnalyticsReport and sub-contracts
        │   ├── report_contracts.py      # ReportConfig, GeneratedReport
        │   └── cache.py                 # FilterPipelineCache
        │
        ├── config/
        │   └── config_schema.py         # StrategyConfig and all sub-configs
        │
        ├── specific/
        │   ├── modules/
        │   │   ├── data_loader.py
        │   │   ├── signal_generator.py
        │   │   ├── filter_pipeline.py
        │   │   ├── trade_simulator.py
        │   │   ├── spread_manager.py
        │   │   ├── risk_manager.py
        │   │   ├── trade_manager.py
        │   │   ├── metrics_calculator.py
        │   │   ├── trade_analytics.py
        │   │   └── report_generator.py
        │   └── filters/
        │       ├── adx_filter.py
        │       ├── bollinger_filter.py
        │       ├── cci_filter.py
        │       ├── choppiness_filter.py
        │       ├── dpo_filter.py
        │       ├── ma_filter.py
        │       ├── macd_filter.py
        │       ├── pivot_filter.py
        │       ├── rsi_filter.py
        │       ├── supertrend_filter.py
        │       └── time_filter.py
        │
        └── utils/
            ├── paths.py                 # Project-wide path constants
            └── structured_logger.py    # Typed, stage-aware logger
```

---

## Path Resolution

All path constants are defined in `src/utils/paths.py`. Import from there — never construct paths relative to `__file__` in module code.

```python
from pathlib import Path

PROJECT_ROOT        = Path(__file__).resolve().parents[2]

# Top-level
CONFIGS_DIR         = PROJECT_ROOT / "configs"
DATA_DIR            = PROJECT_ROOT / "data"
OUTPUTS_DIR         = PROJECT_ROOT / "outputs"
SCRIPTS_DIR         = PROJECT_ROOT / "scripts"
SRC_DIR             = PROJECT_ROOT / "src"

# Data
RAW_DATA_DIR        = DATA_DIR / "raw"
PROCESSED_DATA_DIR  = DATA_DIR / "processed"
EXPORTS_DATA_DIR    = DATA_DIR / "exports"

# Outputs
BACKTEST_OUTPUT_DIR = OUTPUTS_DIR / "backtests"
LOGS_DIR            = OUTPUTS_DIR / "logs"
REPORTS_DIR         = OUTPUTS_DIR / "reports"
SIGNALS_DIR         = OUTPUTS_DIR / "signals"

# Source structure
STRATEGIES_DIR      = SRC_DIR / "strategies"
CONTRACTS_DIR       = STRATEGIES_DIR / "contracts"
SPECIFIC_DIR        = STRATEGIES_DIR / "specific"
MODULES_DIR         = SPECIFIC_DIR / "modules"
FILTERS_DIR         = SPECIFIC_DIR / "filters"

# Tests
TESTS_DIR           = PROJECT_ROOT / "tests"
```

---

## Integration Guide

### Complete Imports

```python
from src.strategies.specific.modules.data_loader      import DataLoader
from src.strategies.specific.modules.signal_generator import SignalGenerator
from src.strategies.specific.modules.filter_pipeline  import FilterPipeline
from src.strategies.specific.modules.spread_manager   import SpreadManager
from src.strategies.specific.modules.risk_manager     import RiskManager
from src.strategies.specific.modules.trade_manager    import TradeManager
from src.strategies.specific.modules.trade_simulator  import TradeSimulator
from src.strategies.specific.modules.metrics_calculator import calculate_metrics
from src.strategies.specific.modules.trade_analytics  import TradeAnalytics
from src.strategies.specific.modules.report_generator import ReportGenerator
from src.strategies.contracts.report_contracts        import ReportConfig
from src.config.config_schema                         import StrategyConfig
from pathlib import Path
```

### Loading Config

```python
config = StrategyConfig.from_yaml(Path("configs/strategies/my_strategy.yaml"))
```

The config template at `configs/strategies/strategy_template.yaml` documents every available key. Copy it as the starting point for a new strategy config.

### Testing the Report Without Hitting the Filesystem

```python
generated = ReportGenerator.generate(analytics, config=ReportConfig(output_dir=tmp_path))
assert "B+" in generated.html_content   # grade present in HTML
assert "chart-equity" in generated.html_content
```

`html_content` contains the full HTML string regardless of whether the file was written. Use this in unit tests to avoid filesystem dependencies.

---

## Design Patterns

### Immutable Contracts

All contracts are `frozen=True` dataclasses. Derived fields computed at construction use `object.__setattr__` in `__post_init__` — that is the only place this pattern is acceptable.

```python
@dataclass(frozen=True)
class DataPathsConfig:
    strategy_ohlcv: Path

    def __post_init__(self):
        # Resolve path once at construction — immutable thereafter
        object.__setattr__(self, "strategy_ohlcv", Path(self.strategy_ohlcv).resolve())
```

### Optional Parameters for Flexible Pipeline Composition

```python
# MetricsReport is computed automatically if not provided
analytics = TradeAnalytics.analyze(trade_result, config)

# Pass an existing MetricsReport to avoid recomputing (e.g. in a reporting-only context)
analytics = TradeAnalytics.analyze(trade_result, config, metrics=pre_computed_metrics)
```

### Validation in `__post_init__`

All validation happens at construction, not at use. If a contract is in memory, it is valid.

```python
def __post_init__(self):
    if self.theme not in {"dark", "light"}:
        raise ValueError(f"theme must be 'dark' or 'light', got '{self.theme}'")
    if not (100 <= self.chart_height_px <= 800):
        raise ValueError(f"chart_height_px must be 100–800, got {self.chart_height_px}")
    if not self.brand_name.strip():
        raise ValueError("brand_name must not be blank")
```

### Structured Serialisation

All analytics and report contracts expose `to_dict()` and `to_json()` for downstream consumers (logging, storage, API responses).

### Mode-Gated Behaviour

Expensive operations are gated explicitly. The pattern is consistent across all modules:

```python
if mode == "analytics":
    ltf_data = self._preprocess_ltf(data_bundle.ltf)
    logger.info("LTF precomputed: %d ticks", len(ltf_data))
# In core mode: ltf_data is None, no logging, no precomputation
```

### Class-Level Caching for Multi-Run Backtesting

```python
class RiskManager:
    _atr_cache: ClassVar[Dict[str, np.ndarray]] = {}

    def _get_atr(self, prices: pd.DataFrame, length: int) -> np.ndarray:
        key = f"{id(prices)}_{length}_{len(prices)}"
        if key not in RiskManager._atr_cache:
            RiskManager._atr_cache[key] = self._compute_atr(prices, length)
        return RiskManager._atr_cache[key]

    @classmethod
    def clear_cache(cls) -> None:
        """Call between runs in a backtester loop to release stale ATR arrays."""
        cls._atr_cache.clear()
```

---

## Extension Points

### Adding a New Technical Filter

1. Create `src/strategies/specific/filters/my_filter.py` implementing the filter interface (see any existing filter as reference — `adx_filter.py` is the simplest).
2. The filter must accept a `SignalFrame` and return a `FilterResult`.
3. Register the filter in `filter_pipeline.py` by adding it to the filter registry.
4. Add its configuration key to `FilterPipelineConfig` in `config_schema.py`.
5. Add the filter name to `filter_sequence` in the strategy YAML.

All filter hot paths must use `np.sum(signal_frame.signals.values != 0)` for signal counting — do not call `signal_frame.count_by_type()` in performance-critical paths.

### Building a New Strategy

The pipeline is strategy-agnostic from `FilterPipeline` onward. To build a new strategy:

1. Implement a signal generator (replace or extend `WBWSTrigger` in `src/indicators/`). It must return a `SignalFrame` with `int8` signal codes.
2. Create a `SignalGenerator` subclass or replace the indicator reference in the existing one.
3. Copy `configs/strategies/strategy_template.yaml` and fill in strategy-specific signal parameters.
4. Select which technical filters to enable in the YAML `filters.pipeline.filter_sequence`.
5. The `TradeSimulator`, `MetricsCalculator`, `TradeAnalytics`, and `ReportGenerator` require no changes.

### Extending the Analytics Layer

To add a new insight dimension to `AnalyticsReport`:

1. Add a new `@dataclass(frozen=True)` contract to `analytics_contracts.py`.
2. Add a field for it in `AnalyticsReport`.
3. Implement the analysis method in `trade_analytics.py`.
4. Add the new insights to `get_all_insights()` so they surface in reports automatically.

### Extending the Report

`ReportGenerator` builds HTML through four internal methods: `_build_layer1_executive`, `_build_layer2_analytical`, `_build_layer3_raw`, and `_build_chart_data`. Each returns a self-contained HTML string that is assembled in `_build_html`. Add a new section by adding a method that returns an HTML string and inserting its output into the assembly in `_build_html`. Chart data for Chart.js is built in `_build_chart_data` — add new datasets there.

Do not add state to `ReportGenerator`. It is a stateless static class; `generate()` is the only public entry point.

---

*Last updated: 2026-02-20 | Version 2.2.0*