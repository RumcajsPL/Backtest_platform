"""ReportGenerator — AnalyticsReport → self-contained HTML report.
Philosophy: single file, no external runtime dependencies, production-grade.

Architecture:
    TradeAnalytics.analyze()
        → AnalyticsReport
            → ReportGenerator.generate()
                → GeneratedReport (HTML file + in-memory content)
Three-layer report structure:
    Layer 1 — EXECUTIVE  : Grade badge, assessment, top insights
    Layer 2 — ANALYTICAL : Chart.js charts + full insight detail
    Layer 3 — RAW DATA   : Collapsible tables (toggleable via ReportConfig)
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional

from src.strategies.contracts.analytics_contracts import (
    AnalyticsReport,
    Insight,
    SessionMetrics,
)
from src.strategies.contracts.report_contracts import GeneratedReport, ReportConfig

if TYPE_CHECKING:
    from src.strategies.contracts.trade_contracts import TradeResult

logger = logging.getLogger(__name__)

# ============================================================
# COLOUR / ICON CONSTANTS
# ============================================================

DARK_THEME = {
    "bg":        "#0d1117",
    "card":      "#161b22",
    "card2":     "#1c2128",
    "border":    "#30363d",
    "text":      "#e6edf3",
    "muted":     "#8b949e",
    "accent":    "#58a6ff",
    "green":     "#3fb950",
    "yellow":    "#e3b341",
    "red":       "#f85149",
    "orange":    "#ff7b72",
    "purple":    "#d2a8ff",
    "font":      "'JetBrains Mono', 'Fira Code', 'Courier New', monospace",
    "font_body": "'IBM Plex Sans', 'Segoe UI', system-ui, sans-serif",
}

LIGHT_THEME = {
    "bg":        "#f6f8fa",
    "card":      "#ffffff",
    "card2":     "#f0f2f5",
    "border":    "#d0d7de",
    "text":      "#1f2328",
    "muted":     "#57606a",
    "accent":    "#0969da",
    "green":     "#1a7f37",
    "yellow":    "#9a6700",
    "red":       "#cf222e",
    "orange":    "#bc4c00",
    "purple":    "#8250df",
    "font":      "'JetBrains Mono', 'Fira Code', 'Courier New', monospace",
    "font_body": "'IBM Plex Sans', 'Segoe UI', system-ui, sans-serif",
}

GRADE_COLOURS_DARK = {
    "A+": "#3fb950", "A": "#3fb950", "A-": "#3fb950",
    "B+": "#58a6ff", "B": "#58a6ff", "B-": "#58a6ff",
    "C+": "#e3b341", "C": "#e3b341", "C-": "#e3b341",
    "D+": "#f85149", "D": "#f85149", "D-": "#f85149",
    "F":  "#f85149",
}

SEVERITY_ICON = {
    "critical": "🚨",
    "warning":  "⚠️",
    "success":  "✅",
    "info":     "ℹ️",
}

SEVERITY_CSS_CLASS = {
    "critical": "sev-critical",
    "warning":  "sev-warning",
    "success":  "sev-success",
    "info":     "sev-info",
}

# ============================================================
# MAIN CLASS
# ============================================================

class ReportGenerator:
    """Visualisation layer: AnalyticsReport → self-contained HTML file.

    Usage::

        report    = TradeAnalytics.analyze(result, config)
        generated = ReportGenerator.generate(report, trade_result)
        print(f"Report saved: {generated.html_path}")
    """

    # ──────────────────────────────────────────────────────────
    # PUBLIC API
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def generate(
        analytics_report: AnalyticsReport,
        trade_result: Optional["TradeResult"] = None,
        config: Optional[ReportConfig] = None,
    ) -> GeneratedReport:
        """Build HTML report and save to disk."""
        if analytics_report is None:
            raise ValueError(
                "analytics_report must not be None — "
                "call TradeAnalytics.analyze() first."
            )

        start = time.perf_counter()

        if config is None:
            config = ReportConfig()

        logger.info("ReportGenerator: building HTML report…")

        html      = ReportGenerator._build_html(analytics_report, trade_result, config)
        html_path = ReportGenerator._save_html(html, config)

        duration_ms = (time.perf_counter() - start) * 1000

        layers = ["executive", "analytical"]
        if config.include_raw_data:
            layers.append("raw")

        logger.info(
            "ReportGenerator: done in %.1fms → %s", duration_ms, html_path
        )

        return GeneratedReport(
            html_path=html_path,
            html_content=html,
            generation_duration_ms=duration_ms,
            analytics_report=analytics_report,
            layers_included=layers,
        )

    # ──────────────────────────────────────────────────────────
    # HTML ASSEMBLY
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _build_html(
        analytics_report: AnalyticsReport,
        trade_result: Optional["TradeResult"],
        config: ReportConfig,
    ) -> str:
        colours    = DARK_THEME if config.theme == "dark" else LIGHT_THEME
        chart_data = ReportGenerator._build_chart_data(trade_result, analytics_report)

        layer1 = ReportGenerator._build_layer1_executive(analytics_report, colours)
        layer2 = ReportGenerator._build_layer2_analytical(
            analytics_report, colours, config, chart_data
        )
        layer3 = (
            ReportGenerator._build_layer3_raw(analytics_report, colours)
            if config.include_raw_data
            else ""
        )

        css = ReportGenerator._build_css(colours, config)
        js  = ReportGenerator._build_js(chart_data, colours, config)

        es = analytics_report.executive_summary
        m  = analytics_report.input_metrics
        ts = analytics_report.analysis_timestamp[:10]

        subtitle_html = (
            f'<p class="header-subtitle">{config.subtitle}</p>'
            if config.subtitle
            else ""
        )
        grade_colour = GRADE_COLOURS_DARK.get(es.performance_grade, colours["accent"])

        raw_tab_btn = (
            "<button class='tab-btn' onclick=\"showTab('raw')\">Raw Data</button>"
            if config.include_raw_data
            else ""
        )
        raw_tab_pane = (
            f"<div id='tab-raw' class='tab-pane hidden'>{layer3}</div>"
            if config.include_raw_data
            else ""
        )

        brand = config.brand_name

        return f"""<!DOCTYPE html>
<html lang="en" data-theme="{config.theme}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{config.title}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js" onerror="window._chartJsFailed=true"></script>
<noscript><style>.tab-btn[onclick*="analytical"]{{opacity:0.4;pointer-events:none}}</style></noscript>
<style>
{css}
</style>
</head>
<body>

<!-- ═══ SITE HEADER ═══════════════════════════════════════ -->
<header class="site-header">
  <div class="header-inner">
    <div class="header-brand">
      <span class="brand-dot" style="background:{grade_colour}"></span>
      <span class="brand-name">{brand}</span>
      <span class="brand-sep">/</span>
      <span class="brand-module">Analytics</span>
    </div>
    <div class="header-meta">
      <span class="meta-item">{ts}</span>
      <span class="meta-sep">·</span>
      <span class="meta-item">{m.total_trades:,} trades</span>
      <span class="meta-sep">·</span>
      <span class="grade-pill" style="background:{grade_colour}20; color:{grade_colour}; border-color:{grade_colour}40">{es.performance_grade}</span>
    </div>
  </div>
</header>

<!-- ═══ MAIN TITLE ════════════════════════════════════════ -->
<div class="page-title-wrap">
  <h1 class="page-title">{config.title}</h1>
  {subtitle_html}
</div>

<!-- ═══ NAVIGATION TABS ══════════════════════════════════ -->
<nav class="tab-nav" id="tab-nav">
  <button class="tab-btn active" onclick="showTab('executive')">Executive</button>
  <button class="tab-btn" onclick="showTab('analytical')">Analytical</button>
  {raw_tab_btn}
</nav>

<!-- ═══ LAYERS ════════════════════════════════════════════ -->
<main class="main-content">
  <div id="tab-executive" class="tab-pane active">
{layer1}
  </div>
  <div id="tab-analytical" class="tab-pane hidden">
{layer2}
  </div>
  {raw_tab_pane}
</main>

<!-- ═══ FOOTER ════════════════════════════════════════════ -->
<footer class="site-footer">
  <span>Generated {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</span>
  <span class="meta-sep">·</span>
  <span>Analysis took {analytics_report.analysis_duration_ms:.1f}ms</span>
  <span class="meta-sep">·</span>
  <span>{brand} ReportGenerator v1.2</span>
</footer>

<script>
{js}
</script>
</body>
</html>"""

    # ──────────────────────────────────────────────────────────
    # LAYER 1 — EXECUTIVE
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _build_layer1_executive(
        report: AnalyticsReport,
        colours: Dict[str, str],
    ) -> str:
        es = report.executive_summary
        m  = report.input_metrics
        ra = report.risk_adjusted

        grade_colour = GRADE_COLOURS_DARK.get(es.performance_grade, colours["accent"])

        pnl_colour = colours["green"] if m.total_pnl_points >= 0 else colours["red"]
        pf_colour  = (
            colours["green"]  if m.profit_factor >= 1.5 else
            colours["yellow"] if m.profit_factor >= 1.0 else
            colours["red"]
        )

        kpis = [
            ("Total P&L",     f"{m.total_pnl_points:+.1f} pts", pnl_colour),
            ("Win Rate",       f"{m.win_rate:.1f}%",             colours["accent"]),
            ("Total Trades",   f"{m.total_trades:,}",            colours["text"]),
            ("Profit Factor",  f"{m.profit_factor:.2f}",         pf_colour),
            ("Max Drawdown",   f"{m.max_drawdown:.1f} pts",      colours["red"]),
            ("Expectancy",     f"{ra.expectancy_per_trade:+.3f} pts", pnl_colour),
        ]
        kpi_html = "".join(
            f'<div class="kpi-card">'
            f'  <div class="kpi-value" style="color:{c}">{v}</div>'
            f'  <div class="kpi-label">{lbl}</div>'
            f'</div>'
            for lbl, v, c in kpis
        )

        insights_html = ""
        for insight in es.critical_insights:
            icon      = SEVERITY_ICON.get(insight.severity, "•")
            css_class = SEVERITY_CSS_CLASS.get(insight.severity, "sev-info")
            impact    = (
                f'<div class="insight-impact">💡 {insight.impact_estimate}</div>'
                if insight.impact_estimate
                else ""
            )
            conf_colour = {
                "High":   colours["green"],
                "Medium": colours["yellow"],
                "Low":    colours["muted"],
            }.get(insight.confidence, colours["muted"])
            insights_html += f"""
      <div class="insight-card {css_class}">
        <div class="insight-header">
          <span class="insight-icon">{icon}</span>
          <span class="insight-message">{insight.message}</span>
          <span class="insight-badge" style="color:{conf_colour}">{insight.confidence}</span>
        </div>
        <div class="insight-rec">→ {insight.recommendation}</div>
        {impact}
      </div>"""

        if not insights_html:
            insights_html = '<div class="no-data">No critical insights generated.</div>'

        strengths_html = "".join(
            f'<li class="strength-item"><span class="str-icon">✓</span>{s}</li>'
            for s in (es.key_strengths or ["No clear strengths identified"])
        )
        improve_html = "".join(
            f'<li class="improve-item"><span class="imp-icon">→</span>{a}</li>'
            for a in (es.improvement_areas or ["No critical improvement areas"])
        )

        return f"""    <!-- LAYER 1: EXECUTIVE -->
    <section class="exec-section">

      <div class="grade-hero">
        <div class="grade-ring" style="border-color:{grade_colour}; box-shadow:0 0 40px {grade_colour}30">
          <div class="grade-letter" style="color:{grade_colour}">{es.performance_grade}</div>
          <div class="grade-label">Performance</div>
        </div>
        <div class="grade-details">
          <p class="grade-assessment">{es.overall_assessment}</p>
          <p class="grade-reasoning">{es.grade_reasoning}</p>
        </div>
      </div>

      <div class="kpi-strip">{kpi_html}</div>

      <div class="section-block">
        <h2 class="section-heading">🎯 Key Insights</h2>
        <div class="insights-grid">{insights_html}</div>
      </div>

      <div class="two-col">
        <div class="card">
          <h3 class="card-heading green-head">📈 Strengths</h3>
          <ul class="strengths-list">{strengths_html}</ul>
        </div>
        <div class="card">
          <h3 class="card-heading yellow-head">⚠️ Improvement Areas</h3>
          <ul class="improve-list">{improve_html}</ul>
        </div>
      </div>

    </section>"""

    # ──────────────────────────────────────────────────────────
    # LAYER 2 — ANALYTICAL
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _build_layer2_analytical(
        report: AnalyticsReport,
        colours: Dict[str, str],
        config: ReportConfig,
        chart_data: Dict,
    ) -> str:
        tp = report.time_performance
        tq = report.trade_quality
        ra = report.risk_adjusted
        h  = config.chart_height_px

        if chart_data.get("equity_labels"):
            equity_section = f"""
      <div class="card chart-card">
        <h3 class="card-heading">Equity Curve</h3>
        <div class="chart-wrap" style="height:{h}px">
          <canvas id="chart-equity"></canvas>
        </div>
      </div>"""
        else:
            equity_section = f"""
      <div class="card chart-card chart-placeholder">
        <h3 class="card-heading">Equity Curve</h3>
        <div class="placeholder-body" style="height:{h}px">
          <span class="placeholder-icon">📈</span>
          <p class="placeholder-msg">Pass <code>trade_result</code> to
          <code>ReportGenerator.generate()</code> to enable the equity curve.</p>
        </div>
      </div>"""

        all_insights = list(tp.insights) + list(tq.insights) + list(ra.insights)
        insights_detail = ReportGenerator._build_insights_accordion(all_insights, colours)

        risk_rows = [
            ("Return / Max DD",    f"{ra.return_over_max_dd:.2f}"),
            ("Avg Win / Avg Loss", f"{ra.avg_win_over_avg_loss:.2f}"),
            ("Expectancy/trade",   f"{ra.expectancy_per_trade:+.4f} pts"),
            ("Consistency score",  f"{ra.consistency_score:.1f} / 100"),
            ("Recovery factor",    f"{ra.recovery_factor:.2f}"),
        ]
        risk_table = ReportGenerator._build_simple_table(
            ["Metric", "Value"], risk_rows, colours
        )

        dur = tq.duration_analysis
        dur_rows: List = [
            ("Average",            f"{dur.avg_bars:.1f} bars"),
            ("Median",             f"{dur.median_bars} bars"),
            ("Fast (<3 bars)",     f"{dur.fast_exits_count} ({dur.fast_exits_pct:.1f}%)"),
            ("Normal (3–10)",      f"{dur.normal_exits_count}"),
            ("Prolonged (>10)",    f"{dur.prolonged_exits_count}"),
        ]
        if tq.avg_bars_to_profit is not None:
            dur_rows.append(("Avg bars to profit", f"{tq.avg_bars_to_profit:.1f}"))
        if tq.avg_bars_to_loss is not None:
            dur_rows.append(("Avg bars to loss",   f"{tq.avg_bars_to_loss:.1f}"))

        dur_table = ReportGenerator._build_simple_table(
            ["Field", "Value"], dur_rows, colours
        )

        return f"""    <!-- LAYER 2: ANALYTICAL -->
    <section class="analytical-section">

      <div class="charts-grid">
        {equity_section}
        <div class="card chart-card">
          <h3 class="card-heading">Session Performance</h3>
          <div class="chart-wrap" style="height:{h}px">
            <canvas id="chart-sessions"></canvas>
          </div>
        </div>
        <div class="card chart-card">
          <h3 class="card-heading">Win / Loss Distribution</h3>
          <div class="chart-wrap" style="height:{h}px">
            <canvas id="chart-winloss"></canvas>
          </div>
        </div>
        <div class="card chart-card">
          <h3 class="card-heading">Trade Duration</h3>
          <div class="chart-wrap" style="height:{h}px">
            <canvas id="chart-duration"></canvas>
          </div>
        </div>
      </div>

      <div class="two-col">
        <div class="card">
          <h3 class="card-heading">Risk-Adjusted Metrics</h3>
          {risk_table}
        </div>
        <div class="card">
          <h3 class="card-heading">Duration Breakdown</h3>
          {dur_table}
          <p class="premature-note">{tq.premature_exit_estimate}</p>
        </div>
      </div>

      <div class="section-block">
        <h2 class="section-heading">All Insights ({len(all_insights)})</h2>
        {insights_detail}
      </div>

    </section>"""

    # ──────────────────────────────────────────────────────────
    # LAYER 3 — RAW DATA
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _build_layer3_raw(
        report: AnalyticsReport,
        colours: Dict[str, str],
    ) -> str:
        tp = report.time_performance
        ra = report.risk_adjusted
        m  = report.input_metrics

        session_rows = [
            (
                sm.session_name, str(sm.trades),
                f"{sm.win_rate:.1f}%", f"{sm.total_pnl:+.1f}",
                f"{sm.avg_pnl:+.2f}", f"{sm.largest_win:+.1f}", f"{sm.largest_loss:+.1f}",
            )
            for sm in sorted(
                tp.by_session.values(), key=lambda s: s.total_pnl, reverse=True
            )
        ]
        session_tbl = ReportGenerator._build_data_table(
            ["Session", "Trades", "Win Rate", "Total P&L",
             "Avg P&L", "Largest Win", "Largest Loss"],
            session_rows, colours,
        )

        hour_rows = [
            (f"{h:02d}:00", str(sm.trades), f"{sm.win_rate:.1f}%",
             f"{sm.total_pnl:+.1f}", f"{sm.avg_pnl:+.2f}")
            for h, sm in sorted(tp.by_hour.items())
            if sm.trades > 0
        ]
        hour_tbl = ReportGenerator._build_data_table(
            ["Hour (UTC)", "Trades", "Win Rate", "Total P&L", "Avg P&L"],
            hour_rows, colours,
        )

        day_order = [
            "Monday", "Tuesday", "Wednesday", "Thursday",
            "Friday", "Saturday", "Sunday",
        ]
        day_rows = [
            (
                day, str(tp.by_day[day].trades),
                f"{tp.by_day[day].win_rate:.1f}%",
                f"{tp.by_day[day].total_pnl:+.1f}",
                f"{tp.by_day[day].avg_pnl:+.2f}",
            )
            for day in day_order
            if day in tp.by_day
        ]
        day_tbl = ReportGenerator._build_data_table(
            ["Day", "Trades", "Win Rate", "Total P&L", "Avg P&L"],
            day_rows, colours,
        )

        base_rows = [
            ("Total Trades",  str(m.total_trades)),
            ("Win Rate",      f"{m.win_rate:.2f}%"),
            ("Total P&L",     f"{m.total_pnl_points:+.2f} pts"),
            ("Profit Factor", f"{m.profit_factor:.4f}"),
            ("Max Drawdown",  f"{m.max_drawdown:.2f} pts"),
            ("Largest Win",   f"{m.largest_win:.2f} pts"),
            ("Largest Loss",  f"{m.largest_loss:.2f} pts"),
        ]
        base_tbl = ReportGenerator._build_data_table(
            ["Metric", "Value"], base_rows, colours
        )

        def collapsible(title: str, content: str, open_: bool = False) -> str:
            attr = " open" if open_ else ""
            return (
                f'<details class="raw-section"{attr}>'
                f'<summary class="raw-summary">{title}</summary>'
                f'<div class="raw-content">{content}</div>'
                f"</details>"
            )

        return f"""    <!-- LAYER 3: RAW DATA -->
    <section class="raw-section-wrap">
      <p class="raw-intro">All underlying data. Click sections to expand.</p>
      {collapsible("📊 Base Metrics", base_tbl, open_=True)}
      {collapsible("🌐 Session Breakdown", session_tbl)}
      {collapsible("🕐 Hour-by-Hour Breakdown", hour_tbl)}
      {collapsible("📅 Day-of-Week Breakdown", day_tbl)}
    </section>"""

    # ──────────────────────────────────────────────────────────
    # CHART DATA
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _build_chart_data(
        trade_result: Optional["TradeResult"],
        report: AnalyticsReport,
    ) -> Dict:
        data: Dict = {}

        if trade_result is not None:
            closed = sorted(
                [t for t in trade_result.trades if t.exit is not None],
                key=lambda t: t.entry.entry_time,
            )
            cumulative = 0.0
            eq_labels:  List[str]   = []
            eq_values:  List[float] = []
            for t in closed:
                cumulative += t.exit.pnl_points
                eq_labels.append(t.entry.entry_time.strftime("%Y-%m-%d"))
                eq_values.append(round(cumulative, 2))
            data["equity_labels"] = eq_labels
            data["equity_values"] = eq_values
        else:
            data["equity_labels"] = []
            data["equity_values"] = []

        tp = report.time_performance
        session_names = list(tp.by_session.keys())
        data["session_labels"] = session_names
        data["session_pnl"]    = [tp.by_session[s].total_pnl  for s in session_names]
        data["session_wr"]     = [tp.by_session[s].win_rate   for s in session_names]

        wd = report.trade_quality.win_distribution
        ld = report.trade_quality.loss_distribution
        data["dist_labels"] = ["Small (<3 pts)", "Medium (3–7 pts)", "Large (>7 pts)"]
        data["win_dist"]    = [wd.small_count, wd.medium_count, wd.large_count]
        data["loss_dist"]   = [ld.small_count, ld.medium_count, ld.large_count]

        dur = report.trade_quality.duration_analysis
        data["dur_labels"] = ["Fast (<3 bars)", "Normal (3–10)", "Prolonged (>10)"]
        data["dur_values"] = [
            dur.fast_exits_count,
            dur.normal_exits_count,
            dur.prolonged_exits_count,
        ]

        return data

    # ──────────────────────────────────────────────────────────
    # HTML HELPERS
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _build_insights_accordion(insights: List[Insight], colours: Dict[str, str]) -> str:
        if not insights:
            return '<div class="no-data">No insights generated.</div>'

        first_critical_opened = False
        items = ""
        for ins in insights:
            icon      = SEVERITY_ICON.get(ins.severity, "•")
            css_class = SEVERITY_CSS_CLASS.get(ins.severity, "sev-info")
            impact    = (
                f'<div class="insight-impact">💡 {ins.impact_estimate}</div>'
                if ins.impact_estimate
                else ""
            )
            conf_colour = {
                "High":   colours["green"],
                "Medium": colours["yellow"],
                "Low":    colours["muted"],
            }.get(ins.confidence, colours["muted"])
            cat_badge = (
                f'<span class="cat-badge cat-{ins.category}">{ins.category}</span>'
            )

            auto_open = ""
            if ins.severity == "critical" and not first_critical_opened:
                auto_open = " open"
                first_critical_opened = True

            items += f"""
    <div class="accordion-item {css_class}{auto_open}">
      <div class="acc-header" onclick="toggleAcc(this)">
        <span class="acc-icon">{icon}</span>
        <span class="acc-msg">{ins.message}</span>
        <span class="acc-meta">
          {cat_badge}
          <span class="conf-badge" style="color:{conf_colour}">{ins.confidence}</span>
          <span class="acc-chevron">▾</span>
        </span>
      </div>
      <div class="acc-body">
        <p class="acc-rec">→ {ins.recommendation}</p>
        {impact}
      </div>
    </div>"""
        return f'<div class="accordion">{items}</div>'

    @staticmethod
    def _build_simple_table(
        headers: List[str], rows: List[tuple], colours: Dict[str, str]
    ) -> str:
        th  = "".join(f"<th>{h}</th>" for h in headers)
        trs = "".join(
            "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>"
            for row in rows
        )
        return (
            f'<table class="data-table simple-table">'
            f"<thead><tr>{th}</tr></thead>"
            f"<tbody>{trs}</tbody>"
            f"</table>"
        )

    @staticmethod
    def _build_data_table(
        headers: List[str], rows: List[tuple], colours: Dict[str, str]
    ) -> str:
        return ReportGenerator._build_simple_table(headers, rows, colours)

    # ──────────────────────────────────────────────────────────
    # CSS
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _build_css(colours: Dict[str, str], config: ReportConfig) -> str:
        c = colours
        h = config.chart_height_px
        return f"""
/* ── Reset & base ────────────────────────────────────────── */
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

body {{
  background: {c["bg"]};
  color: {c["text"]};
  font-family: {c["font_body"]};
  font-size: 14px;
  line-height: 1.6;
  min-height: 100vh;
}}

/* ── Site Header ────────────────────────────────────────── */
.site-header {{
  background: {c["card"]};
  border-bottom: 1px solid {c["border"]};
  position: sticky;
  top: 0;
  z-index: 100;
  backdrop-filter: blur(12px);
}}
.header-inner {{
  max-width: 1400px;
  margin: 0 auto;
  padding: 0 24px;
  height: 52px;
  display: flex;
  align-items: center;
  justify-content: space-between;
}}
.header-brand {{
  display: flex;
  align-items: center;
  gap: 8px;
  font-family: {c["font"]};
  font-size: 13px;
  font-weight: 500;
}}
.brand-dot {{
  width: 8px; height: 8px;
  border-radius: 50%;
  animation: pulse 2s infinite;
}}
@keyframes pulse {{
  0%, 100% {{ opacity: 1; transform: scale(1); }}
  50%       {{ opacity: 0.6; transform: scale(0.8); }}
}}
.brand-sep {{ color: {c["muted"]}; }}
.brand-module {{ color: {c["muted"]}; }}
.header-meta {{
  display: flex;
  align-items: center;
  gap: 10px;
  font-size: 12px;
  color: {c["muted"]};
  font-family: {c["font"]};
}}
.meta-sep {{ color: {c["border"]}; }}
.grade-pill {{
  padding: 2px 10px;
  border-radius: 999px;
  border: 1px solid;
  font-weight: 700;
  font-size: 12px;
  font-family: {c["font"]};
}}

/* ── Page title ─────────────────────────────────────────── */
.page-title-wrap {{
  max-width: 1400px;
  margin: 32px auto 0;
  padding: 0 24px;
}}
.page-title {{
  font-size: clamp(22px, 3vw, 32px);
  font-weight: 600;
  letter-spacing: -0.03em;
  color: {c["text"]};
}}
.header-subtitle {{
  color: {c["muted"]};
  margin-top: 4px;
  font-size: 13px;
}}

/* ── Tab navigation ─────────────────────────────────────── */
.tab-nav {{
  max-width: 1400px;
  margin: 24px auto 0;
  padding: 0 24px;
  display: flex;
  gap: 4px;
  border-bottom: 1px solid {c["border"]};
}}
.tab-btn {{
  background: none;
  border: none;
  color: {c["muted"]};
  font: 500 13px {c["font_body"]};
  padding: 10px 18px;
  cursor: pointer;
  border-bottom: 2px solid transparent;
  margin-bottom: -1px;
  transition: color 0.15s, border-color 0.15s;
}}
.tab-btn:hover {{ color: {c["text"]}; }}
.tab-btn.active {{
  color: {c["accent"]};
  border-bottom-color: {c["accent"]};
}}

/* ── Tab panes ──────────────────────────────────────────── */
.tab-pane {{ display: block; }}
.tab-pane.hidden {{ display: none; }}
.main-content {{
  max-width: 1400px;
  margin: 0 auto;
  padding: 28px 24px 60px;
}}

/* ── Card ────────────────────────────────────────────────── */
.card {{
  background: {c["card"]};
  border: 1px solid {c["border"]};
  border-radius: 10px;
  padding: 20px 22px;
}}
.card-heading {{
  font-size: 13px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: {c["muted"]};
  margin-bottom: 14px;
  font-family: {c["font"]};
}}
.green-head  {{ color: {c["green"]}; }}
.yellow-head {{ color: {c["yellow"]}; }}

/* ── Section ─────────────────────────────────────────────── */
.section-block {{ margin-top: 28px; }}
.section-heading {{
  font-size: 15px;
  font-weight: 600;
  color: {c["text"]};
  margin-bottom: 14px;
}}
.two-col {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
  margin-top: 16px;
}}
@media (max-width: 768px) {{
  .two-col {{ grid-template-columns: 1fr; }}
  .grade-hero {{ flex-direction: column; align-items: center; text-align: center; }}
}}
@media (max-width: 900px) {{
  .kpi-strip {{ grid-template-columns: repeat(3, 1fr); }}
}}
@media (max-width: 480px) {{
  .kpi-strip {{ grid-template-columns: repeat(2, 1fr); }}
  .charts-grid {{ grid-template-columns: 1fr; }}
}}

/* ── Layer 1: Executive ──────────────────────────────────── */
.grade-hero {{
  display: flex;
  align-items: flex-start;
  gap: 28px;
  padding: 28px;
  background: {c["card"]};
  border: 1px solid {c["border"]};
  border-radius: 12px;
  margin-bottom: 20px;
  animation: fadeSlideIn 0.4s ease;
}}
@keyframes fadeSlideIn {{
  from {{ opacity: 0; transform: translateY(12px); }}
  to   {{ opacity: 1; transform: translateY(0); }}
}}
.grade-ring {{
  width: 110px; height: 110px;
  border-radius: 50%;
  border: 4px solid;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  transition: box-shadow 0.3s;
}}
.grade-letter {{
  font-family: {c["font"]};
  font-size: 38px;
  font-weight: 700;
  line-height: 1;
}}
.grade-label {{
  font-size: 10px;
  color: {c["muted"]};
  text-transform: uppercase;
  letter-spacing: 0.1em;
  margin-top: 4px;
}}
.grade-details {{ flex: 1; }}
.grade-assessment {{
  font-size: 14px;
  line-height: 1.7;
  color: {c["text"]};
  margin-bottom: 10px;
}}
.grade-reasoning {{
  font-family: {c["font"]};
  font-size: 11px;
  color: {c["muted"]};
  background: {c["card2"]};
  padding: 8px 12px;
  border-radius: 6px;
  border-left: 3px solid {c["border"]};
}}
.kpi-strip {{
  display: grid;
  grid-template-columns: repeat(6, 1fr);
  gap: 12px;
  margin-bottom: 20px;
}}
.kpi-card {{
  background: {c["card"]};
  border: 1px solid {c["border"]};
  border-radius: 8px;
  padding: 14px 16px;
  text-align: center;
  transition: transform 0.15s, border-color 0.15s;
}}
.kpi-card:hover {{
  transform: translateY(-2px);
  border-color: {c["accent"]}60;
}}
.kpi-value {{
  font-family: {c["font"]};
  font-size: 18px;
  font-weight: 700;
  line-height: 1;
  margin-bottom: 5px;
}}
.kpi-label {{
  font-size: 11px;
  color: {c["muted"]};
  text-transform: uppercase;
  letter-spacing: 0.07em;
}}
.insights-grid {{ display: flex; flex-direction: column; gap: 10px; }}
.insight-card {{
  border-radius: 8px;
  padding: 14px 16px;
  border-left: 4px solid;
  background: {c["card"]};
  border-color: {c["border"]};
  animation: fadeSlideIn 0.3s ease;
}}
.insight-card.sev-critical {{ border-left-color: {c["red"]};    background: {c["red"]}0a; }}
.insight-card.sev-warning  {{ border-left-color: {c["yellow"]}; background: {c["yellow"]}0a; }}
.insight-card.sev-success  {{ border-left-color: {c["green"]};  background: {c["green"]}0a; }}
.insight-card.sev-info     {{ border-left-color: {c["accent"]}; background: {c["accent"]}0a; }}
.insight-header {{
  display: flex;
  align-items: flex-start;
  gap: 10px;
  margin-bottom: 6px;
}}
.insight-icon    {{ font-size: 16px; flex-shrink: 0; margin-top: 1px; }}
.insight-message {{ flex: 1; font-weight: 500; font-size: 13px; line-height: 1.4; }}
.insight-badge   {{ font-family: {c["font"]}; font-size: 10px; font-weight: 600; flex-shrink: 0; text-transform: uppercase; }}
.insight-rec     {{ font-size: 12px; color: {c["muted"]}; padding-left: 26px; }}
.insight-impact  {{ font-size: 12px; color: {c["yellow"]}; padding-left: 26px; margin-top: 4px; font-style: italic; }}
.strengths-list, .improve-list {{
  list-style: none;
  display: flex;
  flex-direction: column;
  gap: 8px;
}}
.strength-item, .improve-item {{
  display: flex;
  align-items: flex-start;
  gap: 8px;
  font-size: 13px;
  line-height: 1.5;
}}
.str-icon {{ color: {c["green"]};  font-weight: 700; flex-shrink: 0; }}
.imp-icon {{ color: {c["yellow"]}; font-weight: 700; flex-shrink: 0; }}

/* ── Layer 2: Analytical ─────────────────────────────────── */
.charts-grid {{
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 16px;
  margin-bottom: 16px;
}}
@media (max-width: 900px) {{ .charts-grid {{ grid-template-columns: 1fr; }} }}

/* [C1] chart-wrap: hard height container — Chart.js sizes to this, not to
   the card. Without a fixed-height wrapper the canvas has no height anchor
   and Chart.js's ResizeObserver triggers an infinite grow loop when the
   Analytical tab becomes visible. overflow:hidden prevents any bleed. */
.chart-wrap {{
  position: relative;
  width: 100%;
  height: {h}px;        /* fixed height — must not be auto or % */
  overflow: hidden;
}}
.chart-wrap canvas {{
  position: absolute;
  top: 0; left: 0;
  width: 100% !important;
  height: 100% !important;  /* fills wrap exactly — no resize loop */
}}

.chart-placeholder {{ display: flex; flex-direction: column; }}
.placeholder-body {{
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 10px;
  padding: 24px 16px;
  border: 1px dashed {c["border"]};
  border-radius: 8px;
  overflow: hidden;
}}
.placeholder-icon {{ font-size: 28px; opacity: 0.4; }}
.placeholder-msg  {{
  font-size: 12px;
  color: {c["muted"]};
  text-align: center;
  line-height: 1.6;
}}
.placeholder-msg code {{
  font-family: {c["font"]};
  background: {c["card2"]};
  padding: 1px 5px;
  border-radius: 3px;
  font-size: 11px;
}}
.accordion          {{ display: flex; flex-direction: column; gap: 8px; }}
.accordion-item     {{ border-radius: 8px; border: 1px solid {c["border"]}; background: {c["card"]}; overflow: hidden; }}
.accordion-item.sev-critical {{ border-left: 3px solid {c["red"]};    }}
.accordion-item.sev-warning  {{ border-left: 3px solid {c["yellow"]}; }}
.accordion-item.sev-success  {{ border-left: 3px solid {c["green"]};  }}
.accordion-item.sev-info     {{ border-left: 3px solid {c["accent"]}; }}
.acc-header {{
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 12px 16px;
  cursor: pointer;
  user-select: none;
}}
.acc-header:hover {{ background: {c["card2"]}; }}
.acc-icon {{ font-size: 14px; flex-shrink: 0; }}
.acc-msg  {{ flex: 1; font-size: 13px; font-weight: 500; }}
.acc-meta {{ display: flex; align-items: center; gap: 8px; flex-shrink: 0; }}
.cat-badge {{
  font-family: {c["font"]};
  font-size: 9px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  padding: 2px 7px;
  border-radius: 4px;
  border: 1px solid currentColor;
}}
.cat-time    {{ color: {c["accent"]}; }}
.cat-quality {{ color: {c["purple"]}; }}
.cat-risk    {{ color: {c["orange"]}; }}
.cat-general {{ color: {c["muted"]}; }}
.conf-badge  {{ font-family: {c["font"]}; font-size: 10px; font-weight: 600; }}
.acc-chevron {{ color: {c["muted"]}; font-size: 12px; transition: transform 0.2s; }}
.accordion-item.open .acc-chevron {{ transform: rotate(180deg); }}
.acc-body {{
  display: none;
  padding: 0 16px 14px 40px;
  border-top: 1px solid {c["border"]};
}}
.accordion-item.open .acc-body {{ display: block; }}
.acc-rec {{ font-size: 12px; color: {c["muted"]}; margin-top: 10px; }}
.premature-note {{
  font-size: 12px;
  color: {c["muted"]};
  font-style: italic;
  margin-top: 12px;
  padding-top: 12px;
  border-top: 1px solid {c["border"]};
}}

/* ── Layer 3: Raw Data ───────────────────────────────────── */
.raw-intro {{ font-size: 13px; color: {c["muted"]}; margin-bottom: 16px; }}
.raw-section {{
  background: {c["card"]};
  border: 1px solid {c["border"]};
  border-radius: 10px;
  margin-bottom: 12px;
  overflow: hidden;
}}
.raw-summary {{
  padding: 14px 18px;
  font-weight: 600;
  font-size: 13px;
  cursor: pointer;
  user-select: none;
  list-style: none;
  display: flex;
  align-items: center;
  gap: 8px;
}}
.raw-summary:hover {{ background: {c["card2"]}; }}
.raw-content {{ padding: 0 18px 18px; overflow-x: auto; }}
.data-table {{
  width: 100%;
  border-collapse: collapse;
  font-family: {c["font"]};
  font-size: 12px;
  margin-top: 12px;
}}
.data-table th {{
  background: {c["card2"]};
  color: {c["muted"]};
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 0.07em;
  padding: 8px 12px;
  text-align: left;
  border-bottom: 1px solid {c["border"]};
}}
.data-table td {{
  padding: 8px 12px;
  border-bottom: 1px solid {c["border"]}50;
  color: {c["text"]};
}}
.data-table tr:last-child td  {{ border-bottom: none; }}
.data-table tr:hover td       {{ background: {c["card2"]}; }}
.no-data {{
  color: {c["muted"]};
  font-style: italic;
  font-size: 13px;
  padding: 12px 0;
}}
.simple-table .data-table th {{ background: none; }}

/* ── Footer ─────────────────────────────────────────────── */
.site-footer {{
  max-width: 1400px;
  margin: 0 auto;
  padding: 20px 24px;
  display: flex;
  gap: 10px;
  align-items: center;
  font-family: {c["font"]};
  font-size: 11px;
  color: {c["muted"]};
  border-top: 1px solid {c["border"]};
}}
"""

    # ──────────────────────────────────────────────────────────
    # JAVASCRIPT  [explicit sizing]
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _build_js(chart_data: Dict, colours: Dict[str, str], config: ReportConfig) -> str:
        c  = colours
        cd = json.dumps(chart_data)
        return f"""
// ── CDN failure handler ────────────────────────────────────
(function() {{
  function showChartFallback() {{
    document.querySelectorAll('.chart-wrap').forEach(wrap => {{
      const card = wrap.closest('.chart-card');
      if (!card) return;
      wrap.style.display = 'none';
      const fb = document.createElement('div');
      fb.className = 'placeholder-body';
      fb.style.height = wrap.style.height || '300px';
      fb.innerHTML = '<span class="placeholder-icon">⚠️</span><p class="placeholder-msg">Charts unavailable — Chart.js CDN could not be loaded.<br>Check your internet connection or view the Raw Data tab.</p>';
      card.appendChild(fb);
    }});
  }}
  window.addEventListener('DOMContentLoaded', () => {{
    if (window._chartJsFailed || typeof Chart === 'undefined') {{
      showChartFallback();
    }}
  }});
}})();

const CD = {cd};

const COLOURS = {{
  green:  '{c["green"]}',
  red:    '{c["red"]}',
  accent: '{c["accent"]}',
  yellow: '{c["yellow"]}',
  muted:  '{c["muted"]}',
  orange: '{c["orange"]}',
  purple: '{c["purple"]}',
}};

Chart.defaults.color       = '{c["muted"]}';
Chart.defaults.font.family = "{c['font']}";
Chart.defaults.font.size   = 11;

Chart.defaults.animation = false;

const gridOpts = {{ color: '{c["border"]}', drawBorder: false }};

const BASE_OPTS = {{
  responsive: true,
  maintainAspectRatio: false,
  animation: false,
}};

// ── Tab switching ──────────────────────────────────────────
function showTab(name) {{
  document.querySelectorAll('.tab-pane').forEach(p => p.classList.add('hidden'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  const pane = document.getElementById('tab-' + name);
  if (pane) pane.classList.remove('hidden');
  document.querySelectorAll('.tab-btn').forEach(b => {{
    if (b.textContent.toLowerCase().startsWith(name)) b.classList.add('active');
  }});
  if (name === 'analytical' && !window._chartsInit) {{
    window._chartsInit = true;
    initCharts();
  }}
}}

// ── Accordion ──────────────────────────────────────────────
function toggleAcc(header) {{
  header.closest('.accordion-item').classList.toggle('open');
}}

// ── Chart initialisation ───────────────────────────────────
function initCharts() {{

  // Equity curve
  if (CD.equity_labels && CD.equity_labels.length > 0) {{
    const ctx = document.getElementById('chart-equity');
    if (ctx) new Chart(ctx, {{
      type: 'line',
      data: {{
        labels: CD.equity_labels,
        datasets: [{{
          label: 'Cumulative P&L (pts)',
          data: CD.equity_values,
          borderColor: COLOURS.accent,
          backgroundColor: COLOURS.accent + '20',
          borderWidth: 2,
          pointRadius: 0,
          fill: true,
          tension: 0.1,
        }}]
      }},
      options: {{
        ...BASE_OPTS,
        plugins: {{ legend: {{ display: false }}, tooltip: {{ mode: 'index', intersect: false }} }},
        scales: {{
          x: {{ grid: gridOpts, ticks: {{ maxTicksLimit: 8, maxRotation: 0 }} }},
          y: {{ grid: gridOpts, ticks: {{ callback: v => v + ' pts' }} }},
        }}
      }}
    }});
  }}

  // Session bar chart
  const sessCtx = document.getElementById('chart-sessions');
  if (sessCtx && CD.session_labels) {{
    const barColours = CD.session_pnl.map(v => v >= 0 ? COLOURS.green + 'cc' : COLOURS.red + 'cc');
    new Chart(sessCtx, {{
      type: 'bar',
      data: {{
        labels: CD.session_labels,
        datasets: [{{
          label: 'P&L (pts)',
          data: CD.session_pnl,
          backgroundColor: barColours,
          borderRadius: 5,
        }}, {{
          label: 'Win Rate %',
          data: CD.session_wr,
          backgroundColor: COLOURS.accent + '50',
          borderColor: COLOURS.accent,
          borderWidth: 1,
          borderRadius: 5,
          type: 'bar',
          yAxisID: 'y2',
        }}]
      }},
      options: {{
        ...BASE_OPTS,
        plugins: {{ legend: {{ position: 'bottom' }} }},
        scales: {{
          x:  {{ grid: gridOpts }},
          y:  {{ grid: gridOpts, ticks: {{ callback: v => v + ' pts' }} }},
          y2: {{
            position: 'right',
            grid: {{ display: false }},
            ticks: {{ callback: v => v + '%', color: COLOURS.accent }},
            min: 0, max: 100,
          }}
        }}
      }}
    }});
  }}

  // Win/loss distribution
  const wlCtx = document.getElementById('chart-winloss');
  if (wlCtx && CD.dist_labels) {{
    new Chart(wlCtx, {{
      type: 'bar',
      data: {{
        labels: CD.dist_labels,
        datasets: [{{
          label: 'Wins',
          data: CD.win_dist,
          backgroundColor: COLOURS.green + 'cc',
          borderRadius: 4,
        }}, {{
          label: 'Losses',
          data: CD.loss_dist,
          backgroundColor: COLOURS.red + 'cc',
          borderRadius: 4,
        }}]
      }},
      options: {{
        ...BASE_OPTS,
        plugins: {{ legend: {{ position: 'bottom' }} }},
        scales: {{
          x: {{ grid: gridOpts }},
          y: {{ grid: gridOpts, ticks: {{ stepSize: 1 }} }},
        }}
      }}
    }});
  }}

  // Duration doughnut
  const durCtx = document.getElementById('chart-duration');
  if (durCtx && CD.dur_labels) {{
    new Chart(durCtx, {{
      type: 'doughnut',
      data: {{
        labels: CD.dur_labels,
        datasets: [{{
          data: CD.dur_values,
          backgroundColor: [
            COLOURS.orange + 'cc',
            COLOURS.green  + 'cc',
            COLOURS.purple + 'cc',
          ],
          borderWidth: 0,
        }}]
      }},
      options: {{
        ...BASE_OPTS,
        plugins: {{
          legend: {{ position: 'bottom' }},
          tooltip: {{ callbacks: {{ label: ctx => ctx.label + ': ' + ctx.parsed + ' trades' }} }}
        }}
      }}
    }});
  }}
}}

// ── KPI entrance animation ─────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {{
  document.querySelectorAll('.kpi-card').forEach((el, i) => {{
    el.style.opacity   = '0';
    el.style.transform = 'translateY(16px)';
    setTimeout(() => {{
      el.style.transition = 'opacity 0.3s ease, transform 0.3s ease';
      el.style.opacity    = '1';
      el.style.transform  = 'none';
    }}, 60 + i * 50);
  }});
}});
"""

    # ──────────────────────────────────────────────────────────
    # FILE I/O
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _save_html(html: str, config: ReportConfig) -> Path:
        output_dir = Path(config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = output_dir / f"report_{timestamp}.html"
        path.write_text(html, encoding="utf-8")
        return path


# ============================================================
# MODULE METADATA
# ============================================================

__all__ = ["ReportGenerator"]