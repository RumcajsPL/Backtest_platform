Executive Summary
The migration is complete and production-ready with exceptional results:

✅ 100% parity with legacy system

✅ 92.6% faster on realistic datasets (88k bars, 9.6k signals)

✅ Clean contract-based architecture end-to-end

✅ No legacy dependencies - truly autonomous new system

The focus now shifts to two parallel tracks:

Hardening & Cleanup (4-6 weeks) - This roadmap

Reporting Modules (4-5 weeks) - Phase 7 from MIGRATION_PLAN.md

🎯 Prioritization Framework
Based on your requirements and current state:

Priority	Criteria	Weight
🔴 Critical	Must-do before production/backtester integration	40%
🟡 High	Significant value, low risk, quick wins	30%
🟢 Medium	Clean architecture, technical debt	20%
⚪ Low	Nice-to-have, defer if needed	10%
🔴 PHASE A: Critical Infrastructure (2-3 sessions)
A1. Config Schema Validation
Effort: 3 hours | Risk: Low | Value: High

Replace dict-based configs with typed dataclasses:

python
# Current (fragile)
spread_enabled = config['trade_management']['spread']['enabled']

# Target (type-safe)
if config.trade_management.spread.enabled:
Implementation Steps:

Create config_contracts.py with dataclasses for all config sections

Add validation logic (ranges, required fields, dependencies)

Update all modules to accept typed configs

Add migration helper for existing JSON/YAML configs

Why Critical: Prevents runtime errors, enables IDE support, documents configuration

A2. Timezone Handling Verification
Effort: 2 hours | Risk: Medium | Value: High

Current assumption: UTC everywhere. Verify and enforce:

python
@dataclass(frozen=True)
class DataBundle:
    df: pd.DataFrame
    metadata: DataMetadata
    
    def __post_init__(self):
        # Enforce UTC or raise clear error
        if self.df.index.tz != pytz.UTC:
            raise TimezoneError(f"Data must be UTC, got {self.df.index.tz}")
Implementation Steps:

Add timezone validation in DataBundle

Review all timestamp operations for timezone assumptions

Add clear documentation about timezone requirements

Why Critical: Timezone bugs are subtle and catastrophic

A3. Structured Logging Foundation
Effort: 2 hours | Risk: Low | Value: High

Replace scattered print/logger statements with structured JSON logging:

python
# utils/structured_logger.py
class StructuredLogger:
    def log_decision(self, stage: str, decision: str, metadata: dict):
        entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "stage": stage,
            "decision": decision,
            **metadata
        }
        logger.info(json.dumps(entry))
Implementation Steps:

Create StructuredLogger utility

Add to core modules (FilterPipeline, TradeSimulator)

Log key decisions: filter rejections, risk adjustments, trade exits

Why Critical: Audit trail for production, debugging without debug mode

🟡 PHASE B: Quick Wins & Hardening (2-3 sessions)
B1. Pivot Filter Split Completion
Effort: 1 hour | Risk: Low | Value: Medium

Verify both pivot filters are properly separated and documented:

PivotStructureFilter (legacy logic - swing highs/lows)

PivotLevelFilter (new logic - daily pivot levels)

Implementation Steps:

Ensure both filters are in filters/ directory

Add clear docstrings explaining use cases

Update filter registry/documentation

Why Quick Win: Already implemented, just needs verification

B2. NaN/Inf Handling Audit
Effort: 3 hours | Risk: Low | Value: High

Ensure consistent handling across all modules:

python
# utils/validation.py
def validate_array(arr: np.ndarray, name: str) -> np.ndarray:
    if np.any(np.isnan(arr)) or np.any(np.isinf(arr)):
        logger.warning(f"NaN/Inf detected in {name}, filling with 0/forward fill")
        # Consistent handling strategy
    return arr
Implementation Steps:

Audit all filter compute_indicators() methods

Create central validation utility

Apply consistently across modules

Why Quick Win: Prevents silent failures, improves debuggability

B3. Type Hint Completion
Effort: 4 hours | Risk: Low | Value: Medium

Achieve 100% type coverage:

bash
mypy src/strategies/specific/ --strict --ignore-missing-imports
Focus Areas:

Filter implementations (11 filters)

Helper functions in modules

Return types on all public methods

Why Quick Win: Improved IDE support, catches bugs early

B4. IndicatorStore Refactoring (TD-1)
Effort: 3 hours | Risk: Low | Value: Medium

Replace mutable dicts with encapsulated IndicatorStore:

python
# Before
indicators: Dict[str, pd.Series] = {}
ind_np: Dict[str, np.ndarray] = {}

# After
store = IndicatorStore()
store.add("rsi", rsi_series)
rsi = store.get_numpy("rsi")  # Returns cached numpy array
Implementation Steps:

Create IndicatorStore class

Update FilterPipeline to use it

Update all filters to accept store

Add tests for store behavior

Why Quick Win: Cleaner API, enables future caching optimizations

🟢 PHASE C: Architecture Cleanup (3-4 sessions)
C1. Legacy Code Removal
Effort: 2 hours | Risk: Low | Value: Medium

Remove the parallel legacy structure:

text
src/strategies/
├── core/              # DELETE - fully replaced
├── specific/          # KEEP - new architecture
└── contracts/         # KEEP - shared types
Implementation Steps:

Verify no imports from core/ remain

Archive core/ to archive/legacy_strategies/

Update any documentation references

Why Important: Eliminates confusion, enforces new architecture

C2. Error Handling Strategy (TD-2)
Effort: 2 hours | Risk: Low | Value: Medium

Implement configurable error handling:

python
class ErrorStrategy(Enum):
    FAIL_FAST = "fail_fast"      # Development
    PASS_THROUGH = "pass_through" # Production default
    REJECT_ALL = "reject_all"     # Conservative
    
class FilterPipeline:
    def __init__(self, error_strategy=ErrorStrategy.PASS_THROUGH):
        self.error_strategy = error_strategy
Implementation Steps:

Add ErrorStrategy enum

Update pipeline error handling

Add to configuration

Why Important: Flexibility for different deployment scenarios

C3. Configuration Naming Cleanup
Effort: 1 hour | Risk: Low | Value: Medium

Standardize configuration keys across all modules:

Use snake_case consistently

Remove legacy parameter aliases

Update examples/templates

Why Important: Developer experience, consistency

⚪ PHASE D: Performance & Advanced (Optional, 3-5 sessions)
D1. HTF Indicator Caching
Effort: 4 hours | Risk: Medium | Value: High (for optimization)

Cache Higher Timeframe indicators to avoid recomputation:

python
class IndicatorCache:
    def __init__(self):
        self._cache: Dict[str, Tuple[pd.Series, int]] = {}
    
    def get_or_compute(self, key: str, df: pd.DataFrame, compute_func):
        cache_key = f"{key}_{hash(df.index[-1])}"
        if cache_key in self._cache:
            return self._cache[cache_key][0]
        result = compute_func(df)
        self._cache[cache_key] = (result, len(df))
        return result
When to Implement: If multi-strategy optimization becomes bottleneck

D2. Filter Dependency Graph
Effort: 6 hours | Risk: Medium | Value: Medium

Enable intelligent filter ordering and parallel execution:

python
class FilterGraph:
    def add_filter(self, filter_name: str, dependencies: List[str]):
        # Build DAG for optimal execution
        pass
    
    def execute_parallel(self, filters, data):
        # Execute independent filters in parallel
        pass
When to Implement: If filter count grows significantly (>20)

D3. Pydantic Integration
Effort: 4 hours | Risk: Low | Value: Medium

Replace manual validation with Pydantic:

python
from pydantic import BaseModel, Field

class TradeParameters(BaseModel):
    direction: TradeDirection
    entry_price: float = Field(gt=0)
    sl_distance: float = Field(ge=0)
    tp_distance: Optional[float] = Field(None, ge=0)
When to Implement: When validation complexity increases

📊 Roadmap Visualization
text
WEEK 1                WEEK 2                WEEK 3                WEEK 4
─────────────────────────────────────────────────────────────────────────────
🔴 PHASE A
├── A1 Config Schema  ─── A2 Timezone  ─── A3 Structured Logging
└──────────────────────┘

🟡 PHASE B
                       ├── B1 Pivot Split  ─── B2 NaN Audit
                       ├── B3 Type Hints   ─── B4 IndicatorStore
                       └───────────────────┘

🟢 PHASE C
                                          ├── C1 Legacy Removal
                                          ├── C2 Error Strategy
                                          └── C3 Config Cleanup

⚪ PHASE D (Optional)
                                             ├── D1 HTF Cache
                                             └── D2 Filter Graph

     PARALLEL TRACK: Reporting Modules (Phase 7, 4-5 weeks)
🎯 Decision Matrix for Session 12
Given your backtester project timeline (4-6 weeks), here's the recommended sequence:

Must Do Before Backtester Integration
Config Schema Validation (A1) - Prevents config errors

Timezone Handling (A2) - Prevents data misalignment

Structured Logging (A3) - Enables debugging

Should Do Before Backtester Integration
NaN/Inf Audit (B2) - Ensures numerical stability

IndicatorStore (B4) - Clean API for filters

Legacy Removal (C1) - Simplifies codebase

Can Defer Post-Backtester
Performance optimizations (D1, D2) - Only if needed

Pydantic (D3) - Nice-to-have validation

📈 Success Metrics
Metric	Target	Measurement
Type coverage	100%	mypy --strict
Config errors	0 runtime	Validation tests
NaN/Inf handling	Consistent	Audit + tests
Performance	≤ current	Benchmark suite
Documentation	Complete	Review
🚨 Risk Assessment
Risk	Probability	Mitigation
Config changes break existing users	Low	Migration helper, clear docs
Performance regression	Low	Benchmark after each change
Timezone bugs in production	Medium	Validation + tests
Scope creep	Medium	Strict prioritization
✅ Session 12 Handoff
Recommended Session 12 Tasks (4-5 hours):

Hour 1: Config Schema Validation (A1)

Create config_contracts.py

Implement core config sections

Hour 2: Timezone Verification (A2)

Add DataBundle validation

Review timestamp operations

Hour 3-4: Structured Logging (A3)

Create StructuredLogger

Add to FilterPipeline + TradeSimulator

Hour 5: Quick Win - Pivot Split (B1)

Verify both filters exist

Update documentation

Outcome: Solid foundation for remaining work

📝 Summary
The post-migration roadmap prioritizes:

Critical Infrastructure (Phase A) - Non-negotiable before production

Quick Wins (Phase B) - High value, low effort

Architecture Cleanup (Phase C) - Technical debt reduction

Performance (Phase D) - Optional, measure first

The system is already production-ready with exceptional performance. This roadmap ensures it's production-hardened and maintainable for the upcoming backtester project.

# POST-MIGRATION ROADMAP — WBWSStrategy

**Status**: Living document — updated each session  
**Audience**: Developer / project owner  
**Purpose**: Capture enhancements that are explicitly out of scope for the migration
project but should be built once the core system is in production.

---

## Context

The migration project (Sessions 1–24) delivers a production-ready backtesting engine
with contract-based architecture, intelligent analytics, and an HTML reporting layer.

Everything in this document is **deliberately deferred** — it is not needed to reach
production, but represents natural next steps once the foundation is solid.

---

## 1. ReportGenerator v2 — Additional Output Formats

### 1.1 Excel Output (Track B — deferred from Session 18)

**Why deferred**: HTML polish (Track A) was completed first to bring v1.0 to production
quality. Excel adds a new output format but does not fix existing issues.
See `DECISION_LOG.md` DEC-009.

**Goal**: `ReportGenerator.generate_excel(analytics_report, config)` → `.xlsx` file

**Technology**: `openpyxl` (already in most Python environments). Read
`/mnt/skills/public/xlsx/SKILL.md` before implementation.

**Sheet structure**:

```
Sheet 1 — Executive Summary
  Row 1-2: Title + subtitle (merged cells, bold)
  Row 3:   Grade badge (cell background = grade colour)
  Row 5+:  KPI table (Metric | Value | Trend)
  Row 15+: Top insights table (Icon | Message | Recommendation | Confidence | Severity)
  Row 30+: Strengths list (green background)
  Row 40+: Improvement areas list (yellow background)

Sheet 2 — Session Analysis
  Session metrics table (all fields, colour-coded by P&L sign)
  Hour-by-hour table (filtered to hours with trades > 0)
  Day-of-week table
  Embedded bar chart: session P&L (openpyxl BarChart)

Sheet 3 — Trade Quality
  Win distribution table (small / medium / large counts + %)
  Loss distribution table
  Duration breakdown table
  Premature exit assessment (italic text)

Sheet 4 — Risk Metrics
  Risk-adjusted metrics table (5 rows)
  All risk insights (colour-coded by severity)

Sheet 5 — All Insights
  Flat table: Message | Recommendation | Severity | Confidence | Category | Impact
  Severity colour coding:
    critical → light red (#FFD6D6)
    warning  → light yellow (#FFF3CD)
    success  → light green (#D6FFD6)
    info     → light blue (#D6E8FF)
```

**API design**:
```python
# Option A — standalone method (simpler)
result = ReportGenerator.generate_excel(analytics_report, config)

# Option B — extend generate() with formats list (cleaner long-term)
result = ReportGenerator.generate(
    analytics_report,
    config=ReportConfig(formats=["html", "excel"])
)
# result.html_path    → Path | None
# result.excel_path   → Path | None
```

**Recommended**: Start with Option A (new method), migrate to Option B when PDF is
also added so all formats share one entry point.

**Contract changes needed**:
```python
@dataclass(frozen=True)
class GeneratedReport:
    html_path: Optional[Path]        # None if HTML not generated
    excel_path: Optional[Path]       # None if Excel not generated  ← NEW
    pdf_path: Optional[Path]         # None if PDF not generated    ← NEW (v3)
    html_content: Optional[str]
    generation_duration_ms: float
    analytics_report: AnalyticsReport
    layers_included: List[str]
    formats_generated: List[str]     # ["html", "excel"]            ← NEW
```

**Estimated effort**: 1 session (~90 min) — read xlsx skill, implement 5 sheets,
colour coding, embedded chart, 40+ tests.

---

### 1.2 PDF Output (v3)

**Goal**: `ReportGenerator.generate_pdf()` → `.pdf` file from the HTML report.

**Technology options** (evaluate at implementation time):
- `weasyprint` — renders HTML/CSS to PDF, no JS execution
- `pdfkit` + `wkhtmltopdf` — headless browser rendering, better fidelity
- `reportlab` — programmatic PDF, most control, most code

**Recommended approach**: `weasyprint` first (fewest dependencies). If Chart.js charts
don't render (no JS), pre-render chart images server-side with `matplotlib` and
embed as `<img>` tags in a PDF-specific HTML template.

**Estimated effort**: 1–2 sessions.

---

### 1.3 CSV Data Export

**Goal**: Export all tabular data from `AnalyticsReport` to CSV files in one call.

```python
ReportGenerator.export_csv(analytics_report, output_dir)
# Produces:
#   sessions.csv, hours.csv, days.csv,
#   win_distribution.csv, insights.csv, risk_metrics.csv
```

**Effort**: ~30 min (pure `csv` module, no new dependencies).

---

## 2. ReportGenerator v2 — HTML Enhancements

### 2.1 Theme Toggle Button

Add a JS button in the report header that switches between dark and light theme
at runtime (without regenerating the file). Both theme colour sets would be embedded
in the HTML as CSS variables.

**Effort**: ~1 hour (CSS variable approach, small JS toggle).

---

### 2.2 Hour-of-Day Heatmap

Replace or supplement the hour-by-hour raw table with a 24-column heatmap
(green = profitable, red = losing, intensity = |P&L|). Similar to a GitHub
contribution grid.

**Technology**: Pure CSS grid or Chart.js matrix plugin.

**Effort**: ~2 hours.

---

### 2.3 Drawdown Chart

Add a drawdown-over-time chart alongside the equity curve, showing the running
maximum drawdown at each point. Helps visualise recovery periods.

**Data needed**: Requires `trade_result` (same as equity curve — already available).

**Effort**: ~30 min (Chart.js, data computed in `_build_chart_data`).

---

### 2.4 Day × Hour Performance Matrix

A 7×24 heatmap showing average P&L at each (day, hour) intersection. The most
granular time-based view — identifies e.g. "Monday 09:00 is the best slot".

**Effort**: ~2 hours (data aggregation + Chart.js matrix or CSS grid).

---

### 2.5 Interactive Filters

Add a JS filter panel on the Analytical tab to let users filter the insights
accordion by severity and category without regenerating the report.

**Effort**: ~1 hour.

---

## 3. TradeAnalytics Enhancements

### 3.1 Baseline Comparison (ComparativeContext v2)

`ComparativeContext` has `vs_baseline: Optional[Dict]` which is currently `None`.
Implement comparison against a stored baseline (e.g., previous backtest run):

```python
# Planned API
report = TradeAnalytics.analyze(
    result, config,
    baseline=MetricsReport.load("outputs/baseline_2025Q4.json")
)
# report.comparative.vs_baseline → {delta_win_rate, delta_pnl, ...}
```

**Effort**: 1 session.

---

### 3.2 Historical Percentile Ranking

`ComparativeContext.percentile_rank` is currently `None`. Implement by maintaining
a local SQLite or JSON database of past backtest results, then rank the current
run against history.

**Effort**: 1–2 sessions.

---

### 3.3 ML-Based Insight Generation

Replace hand-coded insight rules with a lightweight regression or classification model
trained on backtest outcomes. The model would learn which metric combinations
correlate with real-world performance degradation.

**Effort**: 3–5 sessions (data collection phase required first).

---

## 4. Infrastructure

### 4.1 CLI Integration

```bash
python -m wbwsstrategy backtest --report html,excel --output-dir reports/
python -m wbwsstrategy report   --input analytics_20260217.json --format pdf
```

**Effort**: 1 session.

---

### 4.2 MagicMock Cleanup (carry-forward from migration)

Replace `MagicMock` in test files with real `Trade` / `MetricsReport` dataclass
instances. Tracked in `DECISION_LOG.md` DEC-010.

**Files**:
- `tests/migration/test_trade_analytics_session15.py`
- `tests/migration/test_trade_analytics_session16.py`
- `tests/migration/test_analytics_contracts.py`
- `tests/migration/test_report_generator_session17.py`

**Effort**: ~2 hours (mechanical substitution, verify no test regressions).

---

### 4.3 Async Report Generation

For large datasets (10k+ trades), `ReportGenerator.generate()` could be made
`async` to avoid blocking the event loop in a web service context.

**Effort**: ~1 hour (wrap in `asyncio.to_thread`).

---

## 5. Priority Order (Recommended)

Once the migration project is complete (Session ~24), tackle in this order:

| Priority | Item | Effort | Value |
|----------|------|--------|-------|
| 1 | MagicMock cleanup | 2h | Code quality |
| 2 | CSV export | 30min | Quick win |
| 3 | Excel output (Track B) | 1 session | High user value |
| 4 | Drawdown chart | 30min | Quick win |
| 5 | Theme toggle | 1h | Polish |
| 6 | PDF output | 1-2 sessions | Sharing |
| 7 | Baseline comparison | 1 session | Analytics depth |
| 8 | Hour heatmap | 2h | Visual depth |
| 9 | Day × Hour matrix | 2h | Visual depth |
| 10 | CLI integration | 1 session | Usability |

---

## 6. Out of Scope (Permanently)

These items were considered and explicitly rejected for this project:

- **React/Vue front-end**: Requires a server, breaks the single-file portability goal.
- **Real-time streaming**: This is a batch backtesting system by design.
- **Multi-strategy portfolio analytics**: Different system, different contracts.
- **Broker API integration**: Out of scope for a backtesting engine.

---

**Last updated**: 2026-02-17 (Session 18)  
**Next review**: After migration project completion (~Session 24)