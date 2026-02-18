# ROADMAP Chapter A — Reporting Formats
## Excel, PDF, CSV Output

**Parent**: `POST_MIGRATION_ROADMAP.md`  
**Priority**: 🔴 High  
**Load when**: Implementing additional report output formats

---

## A1. Excel Output (.xlsx) — Deferred from Session 18 (DEC-019)

**Goal**: `ReportGenerator.generate_excel(analytics_report, config)` → `.xlsx` file

**Technology**: `openpyxl`. Read `/mnt/skills/public/xlsx/SKILL.md` before starting.

**Sheet structure**:
```
Sheet 1 — Executive Summary
  Row 1-2:  Title + subtitle (merged cells, bold, branded colour)
  Row 3:    Grade pill (cell background = grade colour: green/blue/yellow/red)
  Row 5+:   KPI table  │ Metric │ Value │
  Row 15+:  Top insights │ Icon │ Message │ Recommendation │ Confidence │ Severity │
  Row 30+:  Strengths list (green fill rows)
  Row 40+:  Improvement areas (yellow fill rows)

Sheet 2 — Session Analysis
  Session metrics table (all SessionMetrics fields, P&L cells colour-coded: + green / - red)
  Hour-by-hour table (hours with trades > 0 only)
  Day-of-week table
  Embedded BarChart: session P&L (openpyxl BarChart)

Sheet 3 — Trade Quality
  Win distribution table  (small / medium / large counts + %)
  Loss distribution table
  Duration breakdown table
  Premature exit assessment (italic text row)

Sheet 4 — Risk Metrics
  Risk-adjusted metrics table (5 rows)
  All risk insights (colour-coded by severity)

Sheet 5 — All Insights
  │ Icon │ Message │ Recommendation │ Severity │ Confidence │ Category │ Impact │
  Severity row fills:
    critical → #FFD6D6  warning → #FFF3CD  success → #D6FFD6  info → #D6E8FF
```

**API design** (prefer Option A first, migrate to B when PDF added):
```python
# Option A — new standalone method
generated = ReportGenerator.generate_excel(analytics_report, config)
# generated.excel_path → Path

# Option B — formats list (future, when HTML+Excel+PDF coexist)
generated = ReportGenerator.generate(analytics_report,
    config=ReportConfig(formats=["html", "excel"]))
```

**Contract changes needed** (when adding any new format):
```python
@dataclass(frozen=True)
class GeneratedReport:
    html_path: Optional[Path]       # None if html not requested
    excel_path: Optional[Path]      # None if excel not requested
    pdf_path: Optional[Path]        # None if pdf not requested
    html_content: Optional[str]
    generation_duration_ms: float
    analytics_report: AnalyticsReport
    layers_included: List[str]
    formats_generated: List[str]    # ["html", "excel"]
```

**Estimated effort**: 1 session (~90 min)  
**Tests target**: 40+ tests covering all 5 sheets, colour coding, embedded chart

---

## A2. PDF Output

**Goal**: `ReportGenerator.generate_pdf()` → `.pdf` file

**Technology options** (evaluate at implementation time):
- `weasyprint` — renders HTML/CSS to PDF, no JS. **Recommended first.**
- `pdfkit` + `wkhtmltopdf` — headless browser, better fidelity, heavier dependency
- `reportlab` — programmatic PDF, most control, most code

**Approach**: If Chart.js charts don't render (weasyprint has no JS), pre-render
chart images server-side with `matplotlib` and embed as `<img>` in a PDF-specific
HTML template alongside the normal CSS.

**Estimated effort**: 1–2 sessions

---

## A3. CSV Data Export

**Goal**: Export all tabular data from `AnalyticsReport` to CSV files.

```python
ReportGenerator.export_csv(analytics_report, output_dir=Path("outputs/csv"))
# Produces:
#   sessions.csv       — SessionMetrics for each session
#   hours.csv          — SessionMetrics for each active hour
#   days.csv           — SessionMetrics for each day
#   win_distribution.csv
#   loss_distribution.csv
#   insights.csv       — all insights flat
#   risk_metrics.csv
```

**Estimated effort**: ~30 min (pure `csv` module, no new dependencies)  
**Do first** — quick win, unblocks data pipeline users before Excel is ready.