# CONTRACTS QUICK REFERENCE
**Session 14 | Version 5.0 | 2026-02-16**

## 📋 TABLE OF CONTENTS
- [Phase 1: Data Layer](#data-layer-phase-1-)
- [Phase 2: Signal Layer](#signal-layer-phase-2-)
- [Phase 3: Filter Layer](#filter-layer-phase-3-)
- [Phase 4: Trade Layer](#trade-layer-phase-4-)
- [Phase 5: Metrics & Analytics](#metrics--analytics-phase-5-)
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
    PASSED = auto()     # Signals passed filter criteria
    REJECTED = auto()   # Signals failed filter criteria
    SKIPPED = auto()    # Filter was disabled or not applicable
    ERROR = auto()      # Filter execution encountered an error
```

### FilterResult
```python
@dataclass(frozen=True)
class FilterResult:
    passed: bool                 # Did signals pass this filter?
    signal_frame: SignalFrame    # Filtered signals (subset)
    metadata: FilterMetadata     # Execution details
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
    LONG = 1    # Buy position
    SHORT = -1  # Sell position
```
**Key Methods**:
- `from_string("BUY")` → `TradeDirection.LONG`
- `to_string()` → "BUY" or "SELL"
- Properties: `is_long`, `is_short`

### ExitReason Enum
```python
class ExitReason(Enum):
    STOP_LOSS = auto()
    TAKE_PROFIT = auto()
    OPPOSITE_SIGNAL = auto()
    END_OF_DATA = auto()
    MANUAL = auto()              # Reserved for future
    TIME_EXIT = auto()           # Reserved for future
```

### TradeParameters
```python
@dataclass(frozen=True)
class TradeParameters:
    # Core execution prices
    entry_price_mid: float
    entry_price_executed: float
    stop_loss_raw: float
    stop_loss_trigger: float
    take_profit: float
    position_size: float = 1.0
   
    # Risk metrics
    atr_value: Optional[float]
    atr_length: Optional[int]
    sl_distance: Optional[float]
    tp_distance: Optional[float]
    risk_reward_ratio: Optional[float]
    
    # Annual range validation
    annual_range_value: Optional[float]
    risk_percentile_calculated: Optional[float]
    max_risk_percentile: Optional[float]
    risk_percentile_passed: bool = True
    
    # Spread details
    spread_enabled: bool = False
    spread_applied: bool = False
    spread_points: Optional[float]
    
    # Adjustments
    sl_adjusted: bool = False
```

### TradeEntry
```python
@dataclass(frozen=True)
class TradeEntry:
    # Identity
    entry_id: str
    trade_manager_id: Optional[int]
    signal_id: Optional[int]
    
    # Timing
    entry_time: pd.Timestamp
    
    # Trade details
    direction: TradeDirection
    entry_price: float
    stop_loss: float
    take_profit: float
    position_size: float = 1.0
    
    # Risk metrics
    sl_distance: float
    tp_distance: float
    risk_reward_ratio: float
    atr_value: Optional[float]
    
    # Execution details
    spread_enabled: bool = False
    spread_points: Optional[float]
    sl_adjusted: bool = False
    
    # Metadata
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
    direction: str                           # "BUY" or "SELL"
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
    win_rate: float                          # Percentage (0-100)
    total_pnl_points: float
    expectancy_points: float                 # Average expected return
    profit_factor: float                     # Gross profit / gross loss
    avg_pnl_points: float
    largest_win: float
    largest_loss: float
    max_drawdown: float                      # Negative value
    losing_streak: int                       # Consecutive losses
    winning_streak: int                      # Consecutive wins
    
    # Trade summary (2 fields)
    trades_per_week: float
    trades_per_day: float
    
    # Metadata (2 fields)
    execution_duration_ms: float
    execution_date: str
```

**Key Methods**:
- `to_dict()` → Dictionary format
- `to_json()` → JSON string
- `to_flat_dict()` → Flat structure (for databases)

**Performance**: <2ms for 1000 trades (5.8x faster than target!)

**Usage**:
```python
from src.strategies.specific.modules.metrics_calculator import calculate_metrics

result: TradeResult = simulator.simulate_trades(...)
metrics: MetricsReport = calculate_metrics(result)
print(f"Win Rate: {metrics.win_rate:.1f}%")
```

---

### AnalyticsReport (Session 14 - Design Complete)

#### Configuration Contracts

**TradingSessionConfig**
```python
@dataclass
class TradingSessionConfig:
    sessions: Dict[str, Tuple[int, int]] = {
        "Asia": (0, 8),      # 00:00 - 08:00 UTC
        "London": (8, 16),   # 08:00 - 16:00 UTC
        "NY": (16, 24)       # 16:00 - 24:00 UTC
    }
```

**Insight** (Core Building Block)
```python
@dataclass(frozen=True)
class Insight:
    message: str                             # Observation
    recommendation: str                      # Action
    confidence: str                          # "High" | "Medium" | "Low"
    impact_estimate: Optional[str]           # Expected benefit
    category: str                            # "time" | "quality" | "risk" | "general"
    severity: str                            # "critical" | "warning" | "info" | "success"
```

**Insight Generation Philosophy**: AI-like recommendations with confidence levels

**Example**:
```python
Insight(
    message="Asia session losing -45pts across 234 trades",
    recommendation="Consider excluding Asia session",
    confidence="High",
    impact_estimate="Potential +45pts improvement",
    category="time",
    severity="critical"
)
```

---

#### Time Performance Contracts

**SessionMetrics**
```python
@dataclass(frozen=True)
class SessionMetrics:
    session_name: str                        # "London", "Monday", "14:00"
    trades: int
    winning_trades: int
    win_rate: float
    total_pnl: float
    avg_pnl: float
    largest_win: float
    largest_loss: float
```

**TimePerformanceBreakdown**
```python
@dataclass(frozen=True)
class TimePerformanceBreakdown:
    by_session: Dict[str, SessionMetrics]    # Asia/London/NY
    by_hour: Dict[int, SessionMetrics]       # 0-23
    by_day: Dict[str, SessionMetrics]        # Mon-Sun
    best_session: str
    worst_session: str
    insights: List[Insight]                  # Time-related insights
```

---

#### Trade Quality Contracts

**TradeDistribution**
```python
@dataclass(frozen=True)
class TradeDistribution:
    small_count: int                         # < 3 points
    medium_count: int                        # 3-7 points
    large_count: int                         # > 7 points
    small_pct: float
    medium_pct: float
    large_pct: float
```

**DurationAnalysis**
```python
@dataclass(frozen=True)
class DurationAnalysis:
    avg_bars: float
    median_bars: int
    fast_exits_count: int                    # < 3 bars
    normal_exits_count: int                  # 3-10 bars
    prolonged_exits_count: int               # > 10 bars
    fast_exits_pct: float
    insights: List[str]
```

**TradeQualityAnalysis**
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

---

#### Risk-Adjusted Contracts

**RiskAdjustedMetrics**
```python
@dataclass(frozen=True)
class RiskAdjustedMetrics:
    return_over_max_dd: float                # Total PnL / Max DD
    avg_win_over_avg_loss: float             # Risk/reward ratio
    expectancy_per_trade: float              # Average expected return
    consistency_score: float                 # 0-100 (volatility-adjusted)
    recovery_factor: float                   # Total PnL / total losses
    insights: List[Insight]
```

---

#### Executive Summary Contracts

**ExecutiveSummary**
```python
@dataclass(frozen=True)
class ExecutiveSummary:
    performance_grade: str                   # "A+" to "D-"
    grade_reasoning: str
    critical_insights: List[Insight]         # Top 3-5 most important
    key_strengths: List[str]
    improvement_areas: List[str]
    overall_assessment: str                  # 2-3 sentence summary
```

**Performance Grading Algorithm**:
- Win rate (0-25 pts)
- Profit factor (0-25 pts)
- Drawdown management (0-25 pts)
- Consistency (0-25 pts)
- Total score → Grade (A+ to F)

---

#### Main Analytics Report

**AnalyticsReport**
```python
@dataclass(frozen=True)
class AnalyticsReport:
    # Core analytics
    executive_summary: ExecutiveSummary
    time_performance: TimePerformanceBreakdown
    trade_quality: TradeQualityAnalysis
    risk_adjusted: RiskAdjustedMetrics
    comparative: Optional[ComparativeContext]
    
    # Reference data
    input_metrics: MetricsReport             # Base metrics
    analysis_timestamp: str
    analysis_duration_ms: float
```

**Key Methods**:
- `to_dict()` → Complete structured data
- `to_json()` → JSON export
- `get_executive_summary_markdown()` → Human-readable report
- `get_all_insights()` → All insights from all domains
- `get_critical_insights_only()` → Only critical severity

**Output Formats**:
1. **Primary**: Markdown executive summary (consulting report style)
2. **Secondary**: Structured JSON (for ReportGenerator)

---

#### Usage Patterns (Session 14 Decision)

**Pattern 1: Auto-Calculate Metrics (Convenient)**
```python
from src.strategies.specific.modules.trade_analytics import analyze_trades

result = simulator.simulate_trades(...)
report = analyze_trades(result, config)  # Auto-calculates metrics
```

**Pattern 2: Use Pre-Calculated Metrics (Explicit)**
```python
metrics = calculate_metrics(result)
report = analyze_trades(result, config, metrics=metrics)
```

**Pattern 3: Backtester (Efficient Reuse)**
```python
metrics = calculate_metrics(result)
save_to_db(metrics)  # Store for backtester
analytics = analyze_trades(result, config, metrics=metrics)  # Reuse
```

**Architectural Decision**: TradeAnalytics aggregates MetricsReport + adds insights
- Metrics parameter is **OPTIONAL** (auto-calculates if None)
- Supports all three usage patterns
- No code duplication

---

#### Example Output

**Markdown Format**:
```markdown
=== STRATEGY PERFORMANCE ANALYSIS ===
Period: 2024-10-01 to 2024-12-31
Total Trades: 1,151 | Win Rate: 16.85% | Total P&L: +245 points

🎯 KEY INSIGHTS:
1. ⚠️  Asia session losing -45pts - Consider excluding
2. ✅ London session drives 73% of profits - Maintain focus
3. ⚠️  73% trades exit within 2 bars - Review stop placement

📈 STRENGTHS:
- Excellent directional edge in London session
- Outstanding risk management

⚠️  IMPROVEMENT AREAS:
- Asia session drag (-45pts)
- Premature exits

📊 PERFORMANCE GRADE: B+ (Good, with clear optimization paths)
```

---

## CONTRACT ORGANIZATION

```
src/strategies/contracts/
├── data_contracts.py           # Phase 1: DataBundle, DataInfo
├── signal_contracts.py         # Phase 2: SignalFrame, SignalType
├── filter_contracts.py         # Phase 3: FilterResult, FilterPipelineResult
├── trade_contracts.py          # Phase 4: Trade, RejectedSignal, TradeResult
├── market_contracts.py         # Phase 4: MarketFrame
├── position_contracts.py       # Phase 4: Position
├── metrics_contracts.py        # Phase 5: MetricsReport (Session 13) ✅
├── analytics_contracts.py      # Phase 5: AnalyticsReport (Session 14) ✅
└── cache.py                    # Phase 3: FilterPipelineCache
```

---

## KEY DESIGN PATTERNS

### 1. Immutability (Phase 4+)
```python
@dataclass(frozen=True)
class Trade:
    entry: TradeEntry
    exit: Optional[TradeExit] = None
```

### 2. Type Safety
```python
direction: TradeDirection  # Not str
exit_reason: ExitReason    # Not str
confidence: str            # Validated in __post_init__
```

### 3. Validation
```python
def __post_init__(self):
    if self.confidence not in {"High", "Medium", "Low"}:
        raise ValueError(f"Invalid confidence: {self.confidence}")
```

### 4. Rich Properties
```python
@property
def is_long(self) -> bool:
    return self.direction == TradeDirection.LONG
```

### 5. Serialization
```python
def to_dict(self) -> Dict:
    """Convert to dictionary for storage/transport"""
    
def to_json(self) -> str:
    """Convert to JSON string"""
```

### 6. Optional Parameters for Flexibility (Session 14)
```python
def analyze(
    trade_result: TradeResult,
    config: StrategyConfig,
    metrics: Optional[MetricsReport] = None,  # Auto-calculate if None
    ...
) -> AnalyticsReport
```

---

## MIGRATION STATUS

**Phase 1 (Data)**: ✅ Complete - DataBundle  
**Phase 2 (Signals)**: ✅ Complete - SignalFrame  
**Phase 3 (Filters)**: ✅ Complete - FilterResult  
**Phase 4 (Trades)**: ✅ Complete - Trade, RejectedSignal, TradeResult  
**Phase 5.1 (Infrastructure)**: ✅ Complete - Foundation (Session 12)  
**Phase 5.2 (Metrics)**: ✅ Complete - MetricsCalculator (Session 13)  
**Phase 5.3 (Analytics)**: ✅ Design Complete - TradeAnalytics (Session 14)  
**Phase 5.3 (Analytics)**: ⏳ Implementation - TradeAnalytics (Sessions 15-16)  
**Phase 5.4 (Reporting)**: 📋 Planned - ReportGenerator (Sessions 17-20)

---

## PERFORMANCE BENCHMARKS

**MetricsCalculator** (Session 13):
- Target: <10ms for 1000 trades
- Actual: **1.72ms** for 1000 trades
- Result: **5.8x faster than target!** 🚀

**TradeAnalytics** (Session 14 Design):
- Target: <200ms for 1000 trades (informational)
- Philosophy: Accuracy over speed
- No hard performance constraints

---

## TEST COVERAGE

**Analytics Contracts** (Session 14):
- **34 tests** - All passing ✅
- **Coverage**: All 13 contracts validated
- **Test Time**: 0.49 seconds
- **Quality**: Production-ready

**Test Categories**:
- Configuration validation
- Insight structure and validation
- Performance breakdown contracts
- Quality analysis contracts
- Risk-adjusted metrics
- Executive summary
- Integration tests

---

**Last Updated**: 2026-02-16 Session 14  
**File Location**: `docs/migration/CONTRACTS_REFERENCE.md`  
**Phase**: 5 - Metrics & Analytics Infrastructure  
**Status**: Design Phase Complete ✅  
**Next**: Session 15 - TradeAnalytics Implementation