# CONTRACTS QUICK REFERENCE
**Session 18 | Version 6.0 | 2026-02-17**

## 📋 TABLE OF CONTENTS
- [Phase 1: Data Layer](#data-layer-phase-1-)
- [Phase 2: Signal Layer](#signal-layer-phase-2-)
- [Phase 3: Filter Layer](#filter-layer-phase-3-)
- [Phase 4: Trade Layer](#trade-layer-phase-4-)
- [Phase 5: Metrics & Analytics](#metrics--analytics-phase-5-)
- [Phase 5: Reporting](#reporting-phase-5-)
- [Contract Organization](#contract-organization)
- [Migration Status](#migration-status)

---

## DATA LAYER (Phase 1 ✅)
### DataBundle
```python
@dataclass
class DataBundle:
    full: pd.DataFrame           # Complete dataset
    strategy: pd.DataFrame       # Date-sliced data
    htf: Optional[pd.DataFrame]  # Higher timeframe (e.g., 1H)
    ltf: Optional[pd.DataFrame]  # Lower timeframe (e.g., 1s)
    artf: Optional[pd.DataFrame] # Monthly bars
    info: DataInfo               # Metadata (bar counts, date range)
    validation: DataValidationResult
    config: Optional[DataConfig]
```
**Key Methods**: `has_htf`, `has_ltf`, `has_artf`  
**Validation**: All DataFrames must have DatetimeIndex + OHLC columns

---

## SIGNAL LAYER (Phase 2 ✅)
### SignalFrame - OPTIMIZED v2.2
```python
@dataclass
class SignalFrame:
    signals: pd.Series           # int8: 1=BUY, 2=SELL, 0=none
    indicator_data: Optional[pd.DataFrame]  # Lazy (debug mode only)
    signal_metadata: Dict[str, Any]
```
**Key Methods**:
- `count_by_type()` → `{"buy": int, "sell": int, "total": int}`
- `iter_raw()` → Fast iterator: `(timestamp, code)`
- `buy_signals`, `sell_signals` properties

**Performance**: int8 storage (not Enum objects) for 5-10% speedup

### SignalType Enum
```python
class SignalType(Enum):
    BUY = auto()   # Code: 1
    SELL = auto()  # Code: 2
```
**Conversion**: `SignalType.from_code(1)` → `SignalType.BUY`

---

## FILTER LAYER (Phase 3 ✅)
### FilterStatus Enum
```python
class FilterStatus(Enum):
    PASSED = auto()
    REJECTED = auto()
    SKIPPED = auto()
    ERROR = auto()
```

### FilterResult
```python
@dataclass(frozen=True)
class FilterResult:
    passed: bool
    signal_frame: SignalFrame
    metadata: FilterMetadata
```

### FilterPipelineResult
```python
@dataclass(frozen=True)
class FilterPipelineResult:
    final_signals: SignalFrame
    raw_count: int
    time_filtered_count: int
    technical_filtered_count: int
    final_count: int
    filter_results: list[FilterMetadata]
    rejection_reasons: Dict[str, int]
    execution_time_ms: Optional[float]
```
**Key Properties**: `pass_rate`, `total_rejection_count`, `get_stats_summary()`

---

## TRADE LAYER (Phase 4 ✅)
### TradeDirection Enum
```python
class TradeDirection(Enum):
    LONG = 1
    SHORT = -1
```

### ExitReason Enum
```python
class ExitReason(Enum):
    STOP_LOSS = auto()
    TAKE_PROFIT = auto()
    OPPOSITE_SIGNAL = auto()
    END_OF_DATA = auto()
    MANUAL = auto()
    TIME_EXIT = auto()
```

### TradeParameters
```python
@dataclass(frozen=True)
class TradeParameters:
    entry_price_mid: float
    entry_price_executed: float
    stop_loss_raw: float
    stop_loss_trigger: float
    take_profit: float
    position_size: float = 1.0
    atr_value: Optional[float]
    atr_length: Optional[int]
    sl_distance: Optional[float]
    tp_distance: Optional[float]
    risk_reward_ratio: Optional[float]
    annual_range_value: Optional[float]
    risk_percentile_calculated: Optional[float]
    max_risk_percentile: Optional[float]
    risk_percentile_passed: bool = True
    spread_enabled: bool = False
    spread_applied: bool = False
    spread_points: Optional[float]
    sl_adjusted: bool = False
```

### TradeEntry
```python
@dataclass(frozen=True)
class TradeEntry:
    entry_id: str
    trade_manager_id: Optional[int]
    signal_id: Optional[int]
    entry_time: pd.Timestamp
    direction: TradeDirection
    entry_price: float
    stop_loss: float
    take_profit: float
    position_size: float = 1.0
    sl_distance: float
    tp_distance: float
    risk_reward_ratio: float
    atr_value: Optional[float]
    spread_enabled: bool = False
    spread_points: Optional[float]
    sl_adjusted: bool = False
    comment: Optional[str]
```

### TradeExit
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
    ltf_execution: bool = False
```

### Trade (Entry + Exit)
```python
@dataclass(frozen=True)
class Trade:
    entry: TradeEntry
    exit: Optional[TradeExit] = None
```
**Key Properties**: `is_open`, `is_closed`, `pnl_points`, `is_win`, `is_loss`

### RejectedSignal
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

### TradeResult (Pipeline Output)
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
    execution_mode: str
    execution_time_ms: Optional[float]
```

---

## METRICS & ANALYTICS (Phase 5 ✅)

### MetricsReport (Session 13)
```python
@dataclass(frozen=True)
class MetricsReport:
    # Performance metrics (13 fields)
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float                 # Percentage (0-100)
    total_pnl_points: float
    expectancy_points: float
    profit_factor: float
    avg_pnl_points: float
    largest_win: float
    largest_loss: float
    max_drawdown: float             # Negative value
    losing_streak: int
    winning_streak: int
    # Trade summary (2 fields)
    trades_per_week: float
    trades_per_day: float
    # Metadata (2 fields)
    execution_duration_ms: float
    execution_date: str
```
**Key Methods**: `to_dict()`, `to_json()`, `to_flat_dict()`  
**Performance**: 1.72ms for 1000 trades (5.8x faster than target!)

```python
from src.strategies.specific.modules.metrics_calculator import calculate_metrics
metrics = calculate_metrics(trade_result)
```

---

### AnalyticsReport (Sessions 14-16) ✅

#### Insight (Core Building Block)
```python
@dataclass(frozen=True)
class Insight:
    message: str                  # Observation
    recommendation: str           # Action
    confidence: str               # "High" | "Medium" | "Low"
    impact_estimate: Optional[str]
    category: str                 # "time" | "quality" | "risk" | "general"
    severity: str                 # "critical" | "warning" | "info" | "success"
```

#### SessionMetrics
```python
@dataclass(frozen=True)
class SessionMetrics:
    session_name: str             # "London", "Monday", "14"
    trades: int
    winning_trades: int
    win_rate: float
    total_pnl: float
    avg_pnl: float
    largest_win: float
    largest_loss: float
```

#### TimePerformanceBreakdown
```python
@dataclass(frozen=True)
class TimePerformanceBreakdown:
    by_session: Dict[str, SessionMetrics]   # Asia/London/NY
    by_hour: Dict[int, SessionMetrics]      # 0-23
    by_day: Dict[str, SessionMetrics]       # Mon-Sun
    best_session: str
    worst_session: str
    insights: List[Insight]
```

#### TradeDistribution
```python
@dataclass(frozen=True)
class TradeDistribution:
    small_count: int    # < 3 points
    medium_count: int   # 3-7 points
    large_count: int    # > 7 points
    small_pct: float
    medium_pct: float
    large_pct: float
```

#### DurationAnalysis
```python
@dataclass(frozen=True)
class DurationAnalysis:
    avg_bars: float
    median_bars: int
    fast_exits_count: int       # < 3 bars
    normal_exits_count: int     # 3-10 bars
    prolonged_exits_count: int  # > 10 bars
    fast_exits_pct: float
    insights: List[str]
```

#### TradeQualityAnalysis
```python
@dataclass(frozen=True)
class TradeQualityAnalysis:
    win_distribution: TradeDistribution
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
    return_over_max_dd: float       # Total PnL / Max DD
    avg_win_over_avg_loss: float    # Risk/reward ratio
    expectancy_per_trade: float
    consistency_score: float        # 0-100 (CV-based)
    recovery_factor: float          # Total PnL / gross losses
    insights: List[Insight]
```

#### ExecutiveSummary
```python
@dataclass(frozen=True)
class ExecutiveSummary:
    performance_grade: str          # "A+" to "F"
    grade_reasoning: str
    critical_insights: List[Insight]  # Top 3-5 (max 7)
    key_strengths: List[str]
    improvement_areas: List[str]
    overall_assessment: str
```

**Grading algorithm** (4 × 25 pts):
1. Win rate: ≥20% = 25, ≥15% = 20, ≥10% = 10
2. Profit factor: ≥2.0 = 25, ≥1.5 = 20, ≥1.2 = 10
3. Drawdown: DD < 20% of profit = 25, <50% = 15, <100% = 5
4. Consistency: ≥70 = 25, ≥50 = 15, ≥30 = 5

Score → Grade: 90+=A+, 85=A, 80=A-, 75=B+, 70=B, 65=B-, 60=C+, 55=C, 50=C-, 40=D+, 30=D, <30=F

#### AnalyticsReport (main)
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
**Key Methods**: `to_dict()`, `to_json()`, `get_all_insights()`,
`get_critical_insights_only()`, `get_insights_by_category(category)`

**Usage**:
```python
from src.strategies.specific.modules.trade_analytics import TradeAnalytics
report = TradeAnalytics.analyze(result, config)               # auto-metrics
report = TradeAnalytics.analyze(result, config, metrics=m)    # explicit
```

---

## REPORTING (Phase 5 ✅)

### ReportConfig (Session 17)
```python
@dataclass(frozen=True)
class ReportConfig:
    title: str = "Strategy Performance Report"
    output_dir: Path = Path("outputs/reports")
    include_raw_data: bool = True    # Layer 3 toggle
    theme: str = "dark"              # "dark" | "light"
    chart_height_px: int = 300       # 100-800
    subtitle: Optional[str] = None
```

**Validation**:
- `theme` must be `"dark"` or `"light"` (raises `ValueError` otherwise)
- `chart_height_px` must be 100–800

---

### GeneratedReport (Session 17)
```python
@dataclass(frozen=True)
class GeneratedReport:
    html_path: Path                  # Absolute path to saved file
    html_content: str                # Full HTML string (for tests / inspection)
    generation_duration_ms: float    # Wall-clock time to generate
    analytics_report: AnalyticsReport  # Source data reference
    layers_included: List[str]       # ["executive", "analytical"] or + "raw"
```
**Key Methods**: `to_dict()`, `to_json()`

---

### ReportGenerator (Sessions 17-18) ✅

**Entry point**:
```python
@staticmethod
def generate(
    analytics_report: AnalyticsReport,
    trade_result: Optional[TradeResult] = None,  # enables equity curve
    config: Optional[ReportConfig] = None,
) -> GeneratedReport
```

**Internal methods**:
```python
_build_html(analytics_report, trade_result, config) -> str
_build_layer1_executive(report, colours) -> str
_build_layer2_analytical(report, colours, config, chart_data) -> str
_build_layer3_raw(report, colours) -> str
_build_chart_data(trade_result, report) -> Dict     # Chart.js datasets
_build_insights_accordion(insights, colours) -> str
_build_css(colours, config) -> str
_build_js(chart_data, colours, config) -> str
_save_html(html, config) -> Path
```

**HTML report features**:
- Single self-contained `.html` file (~32KB)
- Three tabs: Executive | Analytical | Raw Data
- 4 Chart.js charts: equity curve, session bar, win/loss dist, duration doughnut
- Dark/light theme via `ReportConfig.theme`
- Mobile-responsive: 6→3 cols @900px, 3→2 cols @480px
- Lazy chart initialisation (Executive tab loads instantly)
- CDN failure handler + `<noscript>` fallback (v1.1)
- First critical insight auto-opens in accordion (v1.1)
- Zero-trade hours filtered from hour table (v1.1)

**Full pipeline usage**:
```python
from src.strategies.specific.modules.report_generator import ReportGenerator
from src.strategies.contracts.report_contracts import ReportConfig
from pathlib import Path

analytics = TradeAnalytics.analyze(trade_result, strategy_config)
generated = ReportGenerator.generate(
    analytics,
    trade_result=trade_result,
    config=ReportConfig(
        title="WBWSStrategy Performance Report",
        subtitle="Q1 2026 Backtest",
        output_dir=Path("outputs/reports"),
        theme="dark",
        chart_height_px=300,
    )
)
# generated.html_path   → Path to file (open in browser)
# generated.html_content → Full HTML string (for tests)
# generated.layers_included → ["executive", "analytical", "raw"]
```

**Status**: ✅ v1.1 COMPLETE (Sessions 17-18, 131 tests)  
**Future formats**: Excel, PDF — see `POST_MIGRATION_ROADMAP.md`

---

## CONTRACT ORGANIZATION (v6.0)

```
src/strategies/contracts/
├── data_contracts.py           # Phase 1: DataBundle, DataInfo ✅
├── signal_contracts.py         # Phase 2: SignalFrame, SignalType ✅
├── filter_contracts.py         # Phase 3: FilterResult, FilterPipelineResult ✅
├── trade_contracts.py          # Phase 4: Trade, RejectedSignal, TradeResult ✅
├── market_contracts.py         # Phase 4: MarketFrame ✅
├── position_contracts.py       # Phase 4: Position ✅
├── metrics_contracts.py        # Phase 5: MetricsReport (Session 13) ✅
├── analytics_contracts.py      # Phase 5: AnalyticsReport (Sessions 14-16) ✅
├── report_contracts.py         # Phase 5: ReportConfig, GeneratedReport (Sessions 17-18) ✅
└── cache.py                    # Phase 3: FilterPipelineCache ✅
```

---

## KEY DESIGN PATTERNS

### 1. Immutability
All Phase 4+ contracts use `frozen=True`.

### 2. Optional Parameters for Flexibility (Session 14)
```python
def analyze(
    trade_result: TradeResult,
    config: StrategyConfig,
    metrics: Optional[MetricsReport] = None,  # auto-calculate if None
    trade_result: Optional[TradeResult] = None,  # equity curve if provided
) -> AnalyticsReport
```

### 3. Validation in `__post_init__`
```python
def __post_init__(self):
    if self.theme not in {"dark", "light"}:
        raise ValueError(f"Theme must be 'dark' or 'light', got '{self.theme}'")
```

### 4. Structured Serialization
All contracts expose `to_dict()` and `to_json()` for downstream consumers.

### 5. html_content in GeneratedReport (Session 17)
```python
# Test without touching the filesystem
result = ReportGenerator.generate(analytics, config=cfg)
assert "B+" in result.html_content    # Grade in HTML
assert "chart-equity" in result.html_content
```

---

## MIGRATION STATUS (v6.0)

| Phase | Module | Sessions | Tests | Status |
|-------|--------|----------|-------|--------|
| 1 | DataBundle | 1-4 | — | ✅ |
| 2 | SignalFrame | 5-7 | — | ✅ |
| 3 | FilterResult | 8-10 | — | ✅ |
| 4 | TradeResult | 11-13 | — | ✅ |
| 5.1 | Infrastructure | 12 | — | ✅ |
| 5.2 | MetricsCalculator | 13 | — | ✅ |
| 5.3 | TradeAnalytics | 14-16 | 141 | ✅ |
| 5.4 | ReportGenerator | 17-18 | 131 | ✅ |
| 6 | Infrastructure Polish | 19-22 | — | ⏳ |

**Total Phase 5 tests**: 374 ✅  
**Overall progress**: ~82% complete

---

## PERFORMANCE BENCHMARKS

| Module | Target | Actual |
|--------|--------|--------|
| MetricsCalculator | <10ms / 1000 trades | **1.72ms** 🚀 |
| TradeAnalytics | <200ms (info) | ~50-150ms ✅ |
| ReportGenerator | no constraint | ~4-10ms ✅ |
| Trade Simulation | baseline | **92.6% faster** 🚀 |

---

## TEST COVERAGE

| Test File | Tests | Session |
|-----------|-------|---------|
| `test_analytics_contracts.py` | 34 | 14 |
| `test_trade_analytics_session15.py` | 124 | 15 |
| `test_trade_analytics_session16.py` | 85 | 16 |
| `test_report_generator_session17.py` | 131 | 17-18 |
| **Total Phase 5** | **374** | |

---

**Last Updated**: 2026-02-17 Session 18  
**File Location**: `docs/migration/CONTRACTS_REFERENCE.md`  
**Version**: 6.0  
**Status**: ReportGenerator v1.1 Complete ✅