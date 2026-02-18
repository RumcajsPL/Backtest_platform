# ROADMAP Chapter B — HTML Report Enhancements
## Theme toggle, heatmaps, additional charts

**Parent**: `POST_MIGRATION_ROADMAP.md`  
**Priority**: 🟡 Medium  
**Load when**: Enhancing the v1.1 HTML report

---

## B1. Theme Toggle Button

**Goal**: JS button in report header toggles dark↔light at runtime (no regen).

**Approach**: Embed both theme colour sets as CSS variables. Toggle swaps `data-theme`
attribute on `<html>`. No page reload required.

```css
:root { --bg: #0d1117; --card: #161b22; /* dark defaults */ }
[data-theme="light"] { --bg: #f6f8fa; --card: #ffffff; }
```

**Estimated effort**: ~1 hour

---

## B2. Hour-of-Day Heatmap

**Goal**: Visual 24-column heatmap replacing or supplementing the hour raw table.
Green = profitable, red = losing, colour intensity = |P&L|. Similar to GitHub
contribution grid.

**Technology**: Pure CSS grid (no extra JS library needed).

**Data**: Already available in `TimePerformanceBreakdown.by_hour`.

**Estimated effort**: ~2 hours

---

## B3. Drawdown Chart

**Goal**: Running maximum drawdown chart alongside the equity curve in Layer 2.
Shows recovery periods visually — "how deep and how long."

**Data needed**: Requires `trade_result` (same condition as equity curve — already
gated on `trade_result is not None`).

**Implementation**: Compute in `_build_chart_data()`:
```python
running_max = np.maximum.accumulate(equity_values)
drawdown = [(eq - mx) for eq, mx in zip(eq_values, running_max)]
data["drawdown_values"] = drawdown
```

New Chart.js line chart (red fill, below zero).

**Estimated effort**: ~30 min — genuine quick win.

---

## B4. Day × Hour Performance Matrix

**Goal**: 7-row × 24-col heatmap. Each cell = average P&L at that (day, hour)
intersection. Identifies e.g. "Monday 09:00 is the single best slot."

**Data**: `TimePerformanceBreakdown.by_hour` + `by_day` exist but cross-tabulation
requires raw `TradeResult` (list of Trade objects with timestamps).

**Consideration**: Requires `trade_result` to be passed to `ReportGenerator.generate()`.
Currently optional — this feature makes it more valuable.

**Estimated effort**: ~2 hours (aggregation + CSS grid render)

---

## B5. Interactive Insight Filters

**Goal**: JS filter panel on Analytical tab — filter the insights accordion by
severity (`critical`, `warning`, `info`, `success`) and category (`time`, `quality`,
`risk`) without regenerating the report.

**Implementation**: Add `data-severity` and `data-category` attributes to each
`.accordion-item`. JS filters toggle `display: none` on items not matching.

**Estimated effort**: ~1 hour