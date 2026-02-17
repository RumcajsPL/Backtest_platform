# SESSION 17 HANDOFF — ReportGenerator Design + HTML v1.0

**Date**: 2026-02-17  
**Phase**: 5.4 — ReportGenerator  
**Prerequisite**: TradeAnalytics ✅ COMPLETE (Sessions 14-16)

---

## 📋 FILES TO BRING INTO SESSION 17 (minimum list)

```
src/strategies/contracts/analytics_contracts.py      ← AnalyticsReport + all sub-contracts
src/strategies/specific/modules/trade_analytics.py   ← completed analytics module
docs/migration/SESSION_17_HANDOFF.md                 ← THIS FILE
```

Optional but useful:
```
src/strategies/contracts/metrics_contracts.py        ← MetricsReport definition
```

---

## ✅ PROJECT STATUS SNAPSHOT

### What is in production (do not touch)
| Module | File | Tests |
|--------|------|-------|
| MetricsCalculator | `metrics_calculator.py` | prod ✅ |
| TradeAnalytics | `trade_analytics.py` | 56 pytest + 85 dev = 141 ✅ |
| Contracts | `analytics_contracts.py` | prod ✅ |

### TradeAnalytics — all 19 methods complete
- **Time**: `_analyze_time_performance()` + 7 insight rules
- **Quality**: `_analyze_trade_quality()` + 5 insight rules  
- **Risk**: `_analyze_risk_adjusted()` + 7 insight rules
- **Executive**: `_generate_executive_summary()`, `_calculate_performance_grade()` (A+ to F)
- **Output**: `format_markdown_report()`, `_save_report()` (JSON + MD)

### Deferred cleanup (do NOT do yet)
Replace MagicMock in test files with real Trade dataclasses — **after ReportGenerator is in production**.

---

## 🎯 SESSION 17 GOAL

Build `ReportGenerator` — the visualisation layer that sits **after** `TradeAnalytics`.

**Input**: `AnalyticsReport` (already fully computed)  
**Output**: HTML report file (+ later: Excel, PDF)

---

## 🏗️ ARCHITECTURE DECISION (LOCKED)

```
TradeResult + MetricsReport + Config
        ↓
TradeAnalytics.analyze()          ← Session 14-16 ✅ DONE
        ↓
AnalyticsReport                   ← fully typed, in memory
        ↓
ReportGenerator.generate()        ← Session 17 (BUILD THIS)
        ↓
HTML file  (+ JSON already saved by TradeAnalytics)
```

**ReportGenerator is called manually** by the developer/orchestrator after `analyze()` returns. It is NOT auto-triggered inside TradeAnalytics. Clean separation.

---

## 📐 THREE-LAYER HTML REPORT DESIGN (DECIDED)

```
Layer 1 — EXECUTIVE  (AI-like, human-friendly, instant read)
  ├── Performance grade badge (A+ to F, colour-coded)
  ├── 2-3 sentence overall assessment
  ├── Top 3-5 insights (icons: 🚨⚠️✅ℹ️) with recommendations
  ├── Strengths list
  └── Improvement areas list

Layer 2 — ANALYTICAL  (breakdown, charts, insight detail)
  ├── Equity curve chart  ← cumulative P&L over time
  ├── Session performance bar chart  ← P&L by Asia/London/NY
  ├── Win/loss distribution  ← small/medium/large breakdown
  ├── Trade duration breakdown  ← fast/normal/prolonged
  └── Full insight detail (all 19 rules, with confidence badges)

Layer 3 — RAW DATA  (tables, numbers, collapsed by default)
  ├── Session metrics table (all fields)
  ├── Hour-by-hour table
  ├── Day-of-week table
  └── Risk metrics table
```

**v1.0 focus**: Layer 1 fully styled + Layer 2 charts + Layer 3 tables.  
Charts: all 4 (equity curve, session bar, win/loss dist, duration).  
Technology: **single self-contained HTML file** (inline CSS + JS, no external deps at runtime). Use Chart.js from CDN for charts.

---

## 📦 FILES SESSION 17 WILL CREATE

```
src/strategies/specific/modules/report_generator.py  ← NEW main module
src/strategies/contracts/report_contracts.py         ← NEW contracts
tests/migration/test_report_generator_session17.py   ← NEW tests
docs/migration/SESSION_17_HANDOFF.md                 ← update with results
```

---

## 🗂️ REPORT CONTRACTS TO DESIGN (Session 17 step 1)

Sketch — validate + refine at session start:

```python
@dataclass(frozen=True)
class ReportConfig:
    title: str = "Strategy Performance Report"
    output_dir: Path = Path("outputs/reports")
    include_raw_data: bool = True        # Layer 3 toggle
    theme: str = "dark"                  # "dark" | "light"
    chart_height_px: int = 300

@dataclass(frozen=True)  
class GeneratedReport:
    html_path: Path                      # Where file was saved
    html_content: str                    # Full HTML string (for tests)
    generation_duration_ms: float
    analytics_report: AnalyticsReport   # Source data reference
    layers_included: List[str]           # ["executive","analytical","raw"]

    def to_dict(self) -> dict: ...
```

---

## 🔑 KEY DESIGN DECISIONS (LOCKED)

| Decision | Choice | Reason |
|----------|--------|--------|
| Single file HTML | ✅ Yes | No server needed, portable |
| Charts library | Chart.js (CDN) | Lightweight, no build step |
| CSS framework | Inline styles only | Self-contained, no CDN dependency |
| Layer toggle | JS show/hide sections | User controls depth |
| Entry point | `ReportGenerator.generate(report, config)` | Clean API |
| Auto-trigger | ❌ No | Called manually after `analyze()` |
| v1.0 formats | HTML only | PDF/Excel = v2.0 |

---

## ⚙️ REPORTGENERATOR API SKETCH

```python
class ReportGenerator:

    @staticmethod
    def generate(
        analytics_report: AnalyticsReport,
        trade_result: "TradeResult",       # Needed for equity curve raw data
        config: ReportConfig = None,
    ) -> GeneratedReport:
        """Main entry point. Returns GeneratedReport with saved HTML path."""

    @staticmethod
    def _build_html(
        analytics_report: AnalyticsReport,
        trade_result: "TradeResult",
        config: ReportConfig,
    ) -> str:
        """Assembles full HTML string from all layers."""

    @staticmethod
    def _build_layer1_executive(report: AnalyticsReport) -> str:
        """Grade badge, assessment, top insights, strengths, improvements."""

    @staticmethod
    def _build_layer2_analytical(
        report: AnalyticsReport,
        trade_result: "TradeResult",
    ) -> str:
        """Charts + full insight detail with confidence badges."""

    @staticmethod
    def _build_layer3_raw(report: AnalyticsReport) -> str:
        """Collapsible tables — session/hour/day/risk data."""

    @staticmethod
    def _build_chart_data(
        trade_result: "TradeResult",
        report: AnalyticsReport,
    ) -> dict:
        """Prepare all Chart.js datasets (equity curve, sessions, distributions)."""

    @staticmethod
    def _save_html(html: str, config: ReportConfig) -> Path:
        """Write file, return path."""
```

---

## 🎨 VISUAL DESIGN REFERENCE

Colour scheme (dark theme):
```
Background:   #0d1117
Card bg:      #161b22
Border:       #30363d
Text:         #e6edf3
Accent:       #58a6ff (blue)

Grade colours:
  A+/A/A- → #3fb950  (green)
  B+/B/B- → #58a6ff  (blue)
  C+/C/C- → #e3b341  (yellow)
  D+/D/D- → #f85149  (red/orange)
  F        → #f85149  (red)

Severity icons + colours:
  critical → 🚨 #f85149
  warning  → ⚠️  #e3b341
  success  → ✅  #3fb950
  info     → ℹ️   #58a6ff
```

---

## ⚠️ DEFERRED CLEANUP (carry forward every session)

**MagicMock cleanup** — after ReportGenerator is in production:
```
tests/migration/test_trade_analytics_session15.py
tests/migration/test_trade_analytics_session16.py
tests/migration/test_analytics_contracts.py
```
Replace MagicMock fakes with real Trade dataclass instances.

---

## 🚀 SESSION 17 EXECUTION ORDER

1. Read this handoff + `analytics_contracts.py` (understand AnalyticsReport shape)
2. Design `report_contracts.py` (ReportConfig + GeneratedReport)
3. Write `report_generator.py` skeleton with all method stubs
4. Implement `_build_layer1_executive()` — full styling, grade badge, insights
5. Implement `_build_layer2_analytical()` — Chart.js charts
6. Implement `_build_layer3_raw()` — collapsible tables
7. Implement `generate()` + `_save_html()`
8. Write test suite (aim 50+ tests)
9. Verify HTML opens correctly in browser
10. Write SESSION_18_HANDOFF.md

**Time estimate**: Layer 1 (~60 min) + Layer 2 charts (~90 min) + Layer 3 (~30 min) + tests (~40 min)

---

**TradeAnalytics**: ✅ COMPLETE (Sessions 14-16, 141 tests)  
**ReportGenerator**: ⏳ Session 17 — HTML Layer 1 focus  
**MagicMock cleanup**: ⏳ After ReportGenerator in production