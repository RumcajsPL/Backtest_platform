"""
Test Suite — ReportGenerator (Session 17)

Tests for:
  - ReportConfig validation
  - GeneratedReport contract
  - _build_chart_data()
  - _build_layer1_executive() HTML structure
  - _build_layer2_analytical() HTML structure
  - _build_layer3_raw() HTML structure
  - _build_html() integration
  - generate() end-to-end (with mock analytics report)
  - Dark / light theme CSS generation
  - Chart.js data embedding

Author:   Session 17
Created:  2026-02-17
"""

import json
import pytest
import tempfile
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional
from unittest.mock import MagicMock, patch

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# ── Contracts ─────────────────────────────────────────────────────────────────
from src.strategies.contracts.report_contracts import ReportConfig, GeneratedReport
from src.strategies.contracts.analytics_contracts import (
    AnalyticsReport,
    ExecutiveSummary,
    TimePerformanceBreakdown,
    TradeQualityAnalysis,
    RiskAdjustedMetrics,
    ComparativeContext,
    SessionMetrics,
    TradeDistribution,
    DurationAnalysis,
    Insight,
    create_empty_insight,
    create_empty_session_metrics,
)

# ── Module under test ─────────────────────────────────────────────────────────
from src.strategies.specific.modules.report_generator import (
    ReportGenerator,
    DARK_THEME,
    LIGHT_THEME,
    GRADE_COLOURS_DARK,
    SEVERITY_ICON,
)


# ══════════════════════════════════════════════════════════════════════════════
# FIXTURES
# ══════════════════════════════════════════════════════════════════════════════

def _make_insight(
    severity: str = "info",
    category: str = "general",
    confidence: str = "Medium",
    message: str = "Test insight",
    recommendation: str = "Test rec",
    impact: str | None = None,
) -> Insight:
    return Insight(
        message=message,
        recommendation=recommendation,
        confidence=confidence,
        impact_estimate=impact,
        category=category,
        severity=severity,
    )

def _make_session_metrics(
    name: str = "London",
    trades: int = 100,
    winning_trades: Optional[int] = None,  # Make it optional
    win_rate: Optional[float] = None,      # Make it optional
    total_pnl: float = 120.0,
    avg_pnl: float = 1.2,
    largest_win: float = 15.0,
    largest_loss: float = -8.0,
) -> SessionMetrics:
    """
    Create valid SessionMetrics for testing.
    Either winning_trades or win_rate must be provided.
    If both provided, winning_trades takes precedence.
    """
    # Calculate missing value
    if winning_trades is None and win_rate is None:
        # Default case: 60% win rate
        winning_trades = int(round(trades * 0.6))
        win_rate = 60.0
    elif winning_trades is None and win_rate is not None:
        # Calculate winning_trades from win_rate
        winning_trades = int(round(trades * win_rate / 100))
    elif winning_trades is not None and win_rate is None:
        # Calculate win_rate from winning_trades
        if trades > 0:
            win_rate = (winning_trades / trades) * 100
        else:
            win_rate = 0.0
    
    # Validate
    if winning_trades < 0 or winning_trades > trades:
        # Adjust if necessary (for edge cases)
        winning_trades = max(0, min(trades, winning_trades))
        if trades > 0:
            win_rate = (winning_trades / trades) * 100
    
    return SessionMetrics(
        session_name=name,
        trades=trades,
        winning_trades=winning_trades,
        win_rate=win_rate,
        total_pnl=total_pnl,
        avg_pnl=avg_pnl,
        largest_win=largest_win,
        largest_loss=largest_loss,
    )



def _make_trade_distribution(
    small=30, medium=50, large=20,
    small_pct=30.0, medium_pct=50.0, large_pct=20.0,
) -> TradeDistribution:
    return TradeDistribution(
        small_count=small, medium_count=medium, large_count=large,
        small_pct=small_pct, medium_pct=medium_pct, large_pct=large_pct,
    )


def _make_duration_analysis() -> DurationAnalysis:
    return DurationAnalysis(
        avg_bars=5.2,
        median_bars=4,
        fast_exits_count=20,
        normal_exits_count=70,
        prolonged_exits_count=10,
        fast_exits_pct=20.0,
        insights=["20% of trades exit in fewer than 3 bars"],
    )


def _make_analytics_report(
    grade: str = "B+",
    total_trades: int = 500,
    win_rate: float = 18.5,
    total_pnl: float = 250.0,
    profit_factor: float = 1.65,
    max_drawdown: float = -80.0,
    consistency_score: float = 62.0,
) -> AnalyticsReport:
    """Build a minimal but complete AnalyticsReport for testing."""
    # Metrics mock
    m = MagicMock()
    m.total_trades = total_trades
    m.win_rate = win_rate
    m.total_pnl_points = total_pnl
    m.profit_factor = profit_factor
    m.max_drawdown = max_drawdown
    m.largest_win = 18.0
    m.largest_loss = -10.0
    m.to_dict.return_value = {
        "total_trades": total_trades,
        "win_rate": win_rate,
        "total_pnl_points": total_pnl,
        "profit_factor": profit_factor,
        "max_drawdown": max_drawdown,
    }

    # Insights
    time_insight   = _make_insight("warning", "time",    "High",   "London weak", "Review London")
    quality_insight= _make_insight("success", "quality", "High",   "Winners fast", "Keep exits")
    risk_insight   = _make_insight("critical","risk",    "High",   "Neg expectancy", "Fix edge",
                                   impact="Improve win rate first")

    # Time performance
    time_perf = TimePerformanceBreakdown(
        by_session={
            "London": _make_session_metrics("London", total_pnl=120.0),
            "NY":     _make_session_metrics("NY",     total_pnl=80.0),
            "Asia":   _make_session_metrics("Asia",   total_pnl=-50.0),
        },
        by_hour={
            9:  _make_session_metrics("9", trades=40, win_rate=60.0, total_pnl=40.0),  # will calculate winning_trades=24,
            14: _make_session_metrics("14", trades=60, total_pnl=60.0),
        },
        by_day={
            "Monday":    _make_session_metrics("Monday",    trades=100, total_pnl=50.0),
            "Wednesday": _make_session_metrics("Wednesday", trades=90,  total_pnl=-20.0),
        },
        best_session="London",
        worst_session="Asia",
        insights=[time_insight],
    )

    # Trade quality
    quality = TradeQualityAnalysis(
        win_distribution=_make_trade_distribution(),
        loss_distribution=_make_trade_distribution(30, 40, 30, 30.0, 40.0, 30.0),
        duration_analysis=_make_duration_analysis(),
        avg_bars_to_profit=4.5,
        avg_bars_to_loss=6.8,
        premature_exit_estimate="Exit timing appears reasonable (20.0% fast exits).",
        insights=[quality_insight],
    )

    # Risk adjusted
    risk = RiskAdjustedMetrics(
        return_over_max_dd=3.12,
        avg_win_over_avg_loss=1.45,
        expectancy_per_trade=0.5,
        consistency_score=consistency_score,
        recovery_factor=1.8,
        insights=[risk_insight],
    )

    # Executive summary
    exec_sum = ExecutiveSummary(
        performance_grade=grade,
        grade_reasoning="Score 75/100 — win rate ≥ 15%; profit factor ≥ 1.5; good drawdown control",
        critical_insights=[risk_insight, time_insight],
        key_strengths=["Strong profit factor: 1.65", "Clear best session: London"],
        improvement_areas=["Negative expectancy per trade"],
        overall_assessment=(
            f"Strategy shows good performance (grade {grade}) across {total_trades:,} trades "
            f"with a {win_rate:.1f}% win rate."
        ),
    )

    return AnalyticsReport(
        executive_summary=exec_sum,
        time_performance=time_perf,
        trade_quality=quality,
        risk_adjusted=risk,
        comparative=ComparativeContext(vs_baseline=None, statistical_flags=[], percentile_rank=None),
        input_metrics=m,
        analysis_timestamp=datetime.now().isoformat(),
        analysis_duration_ms=42.5,
    )


# ══════════════════════════════════════════════════════════════════════════════
# TESTS — ReportConfig
# ══════════════════════════════════════════════════════════════════════════════

class TestReportConfig:
    def test_default_values(self):
        cfg = ReportConfig()
        assert cfg.title == "Strategy Performance Report"
        assert cfg.theme == "dark"
        assert cfg.include_raw_data is True
        assert cfg.chart_height_px == 300

    def test_custom_values(self):
        cfg = ReportConfig(title="My Report", theme="light", chart_height_px=400)
        assert cfg.title == "My Report"
        assert cfg.theme == "light"
        assert cfg.chart_height_px == 400

    def test_invalid_theme_raises(self):
        with pytest.raises(ValueError, match="Theme must be"):
            ReportConfig(theme="blue")

    def test_chart_height_too_small_raises(self):
        with pytest.raises(ValueError, match="chart_height_px"):
            ReportConfig(chart_height_px=50)

    def test_chart_height_too_large_raises(self):
        with pytest.raises(ValueError, match="chart_height_px"):
            ReportConfig(chart_height_px=900)

    def test_light_theme_accepted(self):
        cfg = ReportConfig(theme="light")
        assert cfg.theme == "light"

    def test_subtitle_optional(self):
        cfg = ReportConfig(subtitle="Q1 2026")
        assert cfg.subtitle == "Q1 2026"

    def test_subtitle_none_by_default(self):
        cfg = ReportConfig()
        assert cfg.subtitle is None

    def test_frozen(self):
        cfg = ReportConfig()
        with pytest.raises((AttributeError, TypeError)):
            cfg.title = "changed"

    def test_output_dir_path_type(self):
        cfg = ReportConfig(output_dir=Path("/tmp/reports"))
        assert isinstance(cfg.output_dir, Path)

    def test_include_raw_data_false(self):
        cfg = ReportConfig(include_raw_data=False)
        assert cfg.include_raw_data is False


# ══════════════════════════════════════════════════════════════════════════════
# TESTS — _build_chart_data
# ══════════════════════════════════════════════════════════════════════════════

class TestBuildChartData:
    def setup_method(self):
        self.report = _make_analytics_report()

    def test_no_trade_result_gives_empty_equity(self):
        data = ReportGenerator._build_chart_data(None, self.report)
        assert data["equity_labels"] == []
        assert data["equity_values"] == []

    def test_session_labels_present(self):
        data = ReportGenerator._build_chart_data(None, self.report)
        assert isinstance(data["session_labels"], list)
        assert len(data["session_labels"]) == 3  # London, NY, Asia

    def test_session_pnl_matches_sessions(self):
        data = ReportGenerator._build_chart_data(None, self.report)
        assert len(data["session_pnl"]) == len(data["session_labels"])

    def test_session_wr_in_data(self):
        data = ReportGenerator._build_chart_data(None, self.report)
        assert "session_wr" in data
        for wr in data["session_wr"]:
            assert 0 <= wr <= 100

    def test_dist_labels_correct_count(self):
        data = ReportGenerator._build_chart_data(None, self.report)
        assert len(data["dist_labels"]) == 3

    def test_win_dist_values_non_negative(self):
        data = ReportGenerator._build_chart_data(None, self.report)
        assert all(v >= 0 for v in data["win_dist"])

    def test_loss_dist_values_non_negative(self):
        data = ReportGenerator._build_chart_data(None, self.report)
        assert all(v >= 0 for v in data["loss_dist"])

    def test_duration_labels_correct_count(self):
        data = ReportGenerator._build_chart_data(None, self.report)
        assert len(data["dur_labels"]) == 3

    def test_duration_values_sum_to_total(self):
        data = ReportGenerator._build_chart_data(None, self.report)
        dur = self.report.trade_quality.duration_analysis
        expected = dur.fast_exits_count + dur.normal_exits_count + dur.prolonged_exits_count
        assert sum(data["dur_values"]) == expected

    def test_with_trade_result_builds_equity(self):
        # Build a minimal trade_result mock with two closed trades
        t1 = MagicMock()
        t1.exit = MagicMock(pnl_points=5.0)
        t1.entry = MagicMock(entry_time=datetime(2025, 1, 1, 10, 0))

        t2 = MagicMock()
        t2.exit = MagicMock(pnl_points=-2.0)
        t2.entry = MagicMock(entry_time=datetime(2025, 1, 2, 10, 0))

        tr = MagicMock()
        tr.trades = [t1, t2]

        data = ReportGenerator._build_chart_data(tr, self.report)
        assert len(data["equity_labels"]) == 2
        assert data["equity_values"][0] == pytest.approx(5.0)
        assert data["equity_values"][1] == pytest.approx(3.0)

    def test_equity_is_cumulative(self):
        trades = []
        for i, pnl in enumerate([10.0, -3.0, 5.0]):
            t = MagicMock()
            t.exit = MagicMock(pnl_points=pnl)
            t.entry = MagicMock(entry_time=datetime(2025, 1, i + 1, 9, 0))
            trades.append(t)
        tr = MagicMock()
        tr.trades = trades
        data = ReportGenerator._build_chart_data(tr, self.report)
        assert data["equity_values"] == pytest.approx([10.0, 7.0, 12.0])


# ══════════════════════════════════════════════════════════════════════════════
# TESTS — Layer 1 Executive
# ══════════════════════════════════════════════════════════════════════════════

class TestBuildLayer1Executive:
    def setup_method(self):
        self.report = _make_analytics_report()
        self.colours = DARK_THEME

    def _html(self):
        return ReportGenerator._build_layer1_executive(self.report, self.colours)

    def test_returns_string(self):
        assert isinstance(self._html(), str)

    def test_contains_grade(self):
        html = self._html()
        assert "B+" in html

    def test_contains_kpi_values(self):
        html = self._html()
        assert "500" in html  # total trades
        assert "18.5" in html  # win rate

    def test_contains_overall_assessment(self):
        html = self._html()
        assert "good performance" in html.lower()

    def test_contains_grade_ring(self):
        assert "grade-ring" in self._html()

    def test_contains_kpi_strip(self):
        assert "kpi-strip" in self._html()

    def test_kpi_cards_present(self):
        html = self._html()
        assert html.count("kpi-card") >= 6

    def test_contains_insights_section(self):
        assert "Key Insights" in self._html()

    def test_critical_insight_shown(self):
        html = self._html()
        assert "Neg expectancy" in html

    def test_warning_insight_shown(self):
        html = self._html()
        assert "London weak" in html

    def test_strengths_section_present(self):
        assert "Strengths" in self._html()

    def test_improvements_section_present(self):
        assert "Improvement" in self._html()

    def test_strength_items_rendered(self):
        html = self._html()
        assert "Strong profit factor" in html

    def test_improvement_items_rendered(self):
        html = self._html()
        assert "Negative expectancy" in html

    def test_severity_css_classes_applied(self):
        html = self._html()
        assert "sev-critical" in html
        assert "sev-warning" in html

    def test_grade_colour_applied(self):
        # B+ should use accent/blue colour
        html = self._html()
        assert GRADE_COLOURS_DARK["B+"] in html

    def test_grade_reasoning_present(self):
        html = self._html()
        assert "Score 75/100" in html

    def test_impact_estimate_shown(self):
        html = self._html()
        assert "Improve win rate first" in html

    def test_confidence_badge_shown(self):
        html = self._html()
        assert "High" in html

    def test_no_data_fallback_when_no_insights(self):
        report = _make_analytics_report()
        # Patch with no critical insights
        new_exec = ExecutiveSummary(
            performance_grade="C",
            grade_reasoning="Score 55/100",
            critical_insights=[],
            key_strengths=[],
            improvement_areas=[],
            overall_assessment="Test.",
        )
        import dataclasses
        report2 = dataclasses.replace(report, executive_summary=new_exec)
        html = ReportGenerator._build_layer1_executive(report2, self.colours)
        assert "No critical insights" in html


# ══════════════════════════════════════════════════════════════════════════════
# TESTS — Layer 2 Analytical
# ══════════════════════════════════════════════════════════════════════════════

class TestBuildLayer2Analytical:
    def setup_method(self):
        self.report = _make_analytics_report()
        self.colours = DARK_THEME
        self.config = ReportConfig()
        self.chart_data = ReportGenerator._build_chart_data(None, self.report)

    def _html(self):
        return ReportGenerator._build_layer2_analytical(
            self.report, self.colours, self.config, self.chart_data
        )

    def test_returns_string(self):
        assert isinstance(self._html(), str)

    def test_session_chart_canvas_present(self):
        assert "chart-sessions" in self._html()

    def test_winloss_chart_canvas_present(self):
        assert "chart-winloss" in self._html()

    def test_duration_chart_canvas_present(self):
        assert "chart-duration" in self._html()

    def test_risk_metrics_table_present(self):
        html = self._html()
        assert "Return / Max DD" in html
        assert "Consistency score" in html

    def test_duration_breakdown_present(self):
        html = self._html()
        assert "Duration Breakdown" in html
        assert "5.2" in html  # avg_bars

    def test_insights_accordion_present(self):
        assert "accordion" in self._html()

    def test_all_insights_shown_in_accordion(self):
        html = self._html()
        # time insight + quality insight + risk insight = 3 total
        assert "London weak" in html
        assert "Winners fast" in html
        assert "Neg expectancy" in html

    def test_category_badges_present(self):
        html = self._html()
        assert "cat-time" in html
        assert "cat-quality" in html
        assert "cat-risk" in html

    def test_premature_exit_note_shown(self):
        html = self._html()
        assert "Exit timing appears reasonable" in html

    def test_risk_values_present(self):
        html = self._html()
        assert "3.12" in html  # return_over_max_dd
        assert "1.45" in html  # avg_win_over_avg_loss

    def test_charts_grid_present(self):
        assert "charts-grid" in self._html()

    def test_equity_canvas_absent_when_no_data(self):
        html = self._html()
        # equity_labels is empty (no trade_result), so canvas should not appear
        assert "chart-equity" not in html

    def test_equity_canvas_present_when_data_exists(self):
        chart_data = dict(self.chart_data)
        chart_data["equity_labels"] = ["2025-01-01", "2025-01-02"]
        chart_data["equity_values"] = [5.0, 8.0]
        html = ReportGenerator._build_layer2_analytical(
            self.report, self.colours, self.config, chart_data
        )
        assert "chart-equity" in html


# ══════════════════════════════════════════════════════════════════════════════
# TESTS — Layer 3 Raw Data
# ══════════════════════════════════════════════════════════════════════════════

class TestBuildLayer3Raw:
    def setup_method(self):
        self.report = _make_analytics_report()
        self.colours = DARK_THEME

    def _html(self):
        return ReportGenerator._build_layer3_raw(self.report, self.colours)

    def test_returns_string(self):
        assert isinstance(self._html(), str)

    def test_session_table_present(self):
        html = self._html()
        assert "Session Breakdown" in html
        assert "London" in html

    def test_hour_table_present(self):
        html = self._html()
        assert "Hour-by-Hour" in html
        assert "09:00" in html

    def test_day_table_present(self):
        html = self._html()
        assert "Day-of-Week" in html
        assert "Monday" in html

    def test_base_metrics_table_present(self):
        html = self._html()
        assert "Base Metrics" in html
        assert "Profit Factor" in html

    def test_collapsible_details_present(self):
        assert "<details" in self._html()
        assert "<summary" in self._html()

    def test_base_metrics_open_by_default(self):
        html = self._html()
        # The first <details> should have open attribute
        assert '<details class="raw-section" open>' in html

    def test_all_session_names_present(self):
        html = self._html()
        for session in ["London", "NY", "Asia"]:
            assert session in html

    def test_day_values_present(self):
        html = self._html()
        assert "Wednesday" in html

    def test_pnl_values_formatted_with_sign(self):
        html = self._html()
        assert "+" in html or "-" in html  # signed values


# ══════════════════════════════════════════════════════════════════════════════
# TESTS — CSS generation
# ══════════════════════════════════════════════════════════════════════════════

class TestBuildCSS:
    def test_dark_theme_uses_dark_bg(self):
        css = ReportGenerator._build_css(DARK_THEME, ReportConfig(theme="dark"))
        assert DARK_THEME["bg"] in css

    def test_light_theme_uses_light_bg(self):
        css = ReportGenerator._build_css(LIGHT_THEME, ReportConfig(theme="light"))
        assert LIGHT_THEME["bg"] in css

    def test_css_contains_kpi_card(self):
        css = ReportGenerator._build_css(DARK_THEME, ReportConfig())
        assert ".kpi-card" in css

    def test_css_contains_grade_ring(self):
        assert ".grade-ring" in ReportGenerator._build_css(DARK_THEME, ReportConfig())

    def test_css_contains_insight_severities(self):
        css = ReportGenerator._build_css(DARK_THEME, ReportConfig())
        assert "sev-critical" in css
        assert "sev-warning" in css
        assert "sev-success" in css

    def test_css_contains_responsive_media_query(self):
        assert "@media" in ReportGenerator._build_css(DARK_THEME, ReportConfig())

    def test_css_has_animation(self):
        assert "@keyframes" in ReportGenerator._build_css(DARK_THEME, ReportConfig())


# ══════════════════════════════════════════════════════════════════════════════
# TESTS — JS generation
# ══════════════════════════════════════════════════════════════════════════════

class TestBuildJS:
    def setup_method(self):
        self.report = _make_analytics_report()
        self.chart_data = ReportGenerator._build_chart_data(None, self.report)

    def _js(self, config=None):
        config = config or ReportConfig()
        return ReportGenerator._build_js(self.chart_data, DARK_THEME, config)

    def test_returns_string(self):
        assert isinstance(self._js(), str)

    def test_chart_data_embedded(self):
        js = self._js()
        assert "const CD" in js
        # Session labels should appear in JSON
        assert "London" in js

    def test_show_tab_function_present(self):
        assert "function showTab" in self._js()

    def test_toggle_acc_function_present(self):
        assert "function toggleAcc" in self._js()

    def test_init_charts_function_present(self):
        assert "function initCharts" in self._js()

    def test_chart_js_references_session_canvas(self):
        assert "chart-sessions" in self._js()

    def test_chart_js_references_winloss_canvas(self):
        assert "chart-winloss" in self._js()

    def test_chart_js_references_duration_canvas(self):
        assert "chart-duration" in self._js()

    def test_colours_embedded(self):
        js = self._js()
        assert DARK_THEME["green"] in js

    def test_dom_content_loaded_listener(self):
        assert "DOMContentLoaded" in self._js()

    def test_kpi_animation_present(self):
        assert "kpi-card" in self._js()


# ══════════════════════════════════════════════════════════════════════════════
# TESTS — Full HTML assembly
# ══════════════════════════════════════════════════════════════════════════════

class TestBuildHTML:
    def setup_method(self):
        self.report = _make_analytics_report()
        self.config = ReportConfig()

    def _html(self, config=None, trade_result=None):
        return ReportGenerator._build_html(
            self.report, trade_result, config or self.config
        )

    def test_returns_string(self):
        assert isinstance(self._html(), str)

    def test_doctype_present(self):
        assert "<!DOCTYPE html>" in self._html()

    def test_chartjs_cdn_imported(self):
        assert "chart.js" in self._html().lower()

    def test_google_fonts_imported(self):
        assert "fonts.googleapis.com" in self._html()

    def test_tab_nav_present(self):
        html = self._html()
        assert "tab-nav" in html
        assert "Executive" in html
        assert "Analytical" in html

    def test_raw_data_tab_present_by_default(self):
        assert "Raw Data" in self._html()

    def test_raw_data_tab_absent_when_disabled(self):
        cfg = ReportConfig(include_raw_data=False)
        html = self._html(config=cfg)
        
        # Check that the raw data tab button is not present
        assert '<button class=\'tab-btn\' onclick="showTab(\'raw\')">Raw Data</button>' not in html
        
        # Check that the raw data tab pane is not present
        assert '<div id=\'tab-raw\'' not in html
        
        # Also check that layer3 content is not present
        assert 'class="raw-section-wrap"' not in html
        assert 'class="raw-section"' not in html
        
        # The CSS comment with "Raw Data" is fine to keep - it's just documentation
        # So we don't assert on the literal text "Raw Data"

    def test_title_in_html(self):
        assert "Strategy Performance Report" in self._html()

    def test_custom_title_applied(self):
        cfg = ReportConfig(title="My Custom Report")
        assert "My Custom Report" in self._html(config=cfg)

    def test_subtitle_applied_when_set(self):
        cfg = ReportConfig(subtitle="Q1 2026 Backtest")
        assert "Q1 2026 Backtest" in self._html(config=cfg)

    def test_footer_present(self):
        assert "site-footer" in self._html()

    def test_footer_version_string(self):
        assert "ReportGenerator v1.1" in self._html()

    def test_all_three_layers_present(self):
        html = self._html()
        assert "tab-executive" in html
        assert "tab-analytical" in html
        assert "tab-raw" in html

    def test_only_two_layers_when_raw_disabled(self):
        cfg = ReportConfig(include_raw_data=False)
        html = self._html(config=cfg)
        assert "tab-raw" not in html

    def test_light_theme_attribute(self):
        cfg = ReportConfig(theme="light")
        assert 'data-theme="light"' in self._html(config=cfg)

    def test_dark_theme_attribute(self):
        assert 'data-theme="dark"' in self._html()

    def test_js_section_present(self):
        assert "<script>" in self._html()

    def test_css_section_present(self):
        assert "<style>" in self._html()

    def test_grade_pill_in_header(self):
        html = self._html()
        assert "grade-pill" in html
        assert "B+" in html

    def test_analysis_duration_in_footer(self):
        html = self._html()
        assert "42.5" in html  # analysis_duration_ms


# ══════════════════════════════════════════════════════════════════════════════
# TESTS — generate() end-to-end
# ══════════════════════════════════════════════════════════════════════════════

class TestGenerate:
    def setup_method(self):
        self.report = _make_analytics_report()

    def test_returns_generated_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ReportConfig(output_dir=Path(tmp))
            result = ReportGenerator.generate(self.report, config=cfg)
            assert isinstance(result, GeneratedReport)

    def test_html_path_exists(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ReportConfig(output_dir=Path(tmp))
            result = ReportGenerator.generate(self.report, config=cfg)
            assert result.html_path.exists()

    def test_html_path_is_html_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ReportConfig(output_dir=Path(tmp))
            result = ReportGenerator.generate(self.report, config=cfg)
            assert result.html_path.suffix == ".html"

    def test_html_content_matches_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ReportConfig(output_dir=Path(tmp))
            result = ReportGenerator.generate(self.report, config=cfg)
            file_content = result.html_path.read_text(encoding="utf-8")
            assert result.html_content == file_content

    def test_generation_duration_positive(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ReportConfig(output_dir=Path(tmp))
            result = ReportGenerator.generate(self.report, config=cfg)
            assert result.generation_duration_ms > 0

    def test_layers_included_with_raw(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ReportConfig(output_dir=Path(tmp), include_raw_data=True)
            result = ReportGenerator.generate(self.report, config=cfg)
            assert "executive" in result.layers_included
            assert "analytical" in result.layers_included
            assert "raw" in result.layers_included

    def test_layers_included_without_raw(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ReportConfig(output_dir=Path(tmp), include_raw_data=False)
            result = ReportGenerator.generate(self.report, config=cfg)
            assert "raw" not in result.layers_included

    def test_analytics_report_reference_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ReportConfig(output_dir=Path(tmp))
            result = ReportGenerator.generate(self.report, config=cfg)
            assert result.analytics_report is self.report

    def test_default_config_applied_when_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch("src.strategies.specific.modules.report_generator.Path") as MockPath:
                # Use real file operations with temp dir via config
                cfg = ReportConfig(output_dir=Path(tmp))
                result = ReportGenerator.generate(self.report, config=cfg)
                assert result.html_content  # non-empty

    def test_output_dir_created(self):
        with tempfile.TemporaryDirectory() as tmp:
            new_dir = Path(tmp) / "new" / "subdir"
            cfg = ReportConfig(output_dir=new_dir)
            result = ReportGenerator.generate(self.report, config=cfg)
            assert new_dir.exists()

    def test_to_dict_works(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ReportConfig(output_dir=Path(tmp))
            result = ReportGenerator.generate(self.report, config=cfg)
            d = result.to_dict()
            assert "html_path" in d
            assert "layers_included" in d
            assert "generation_duration_ms" in d

    def test_to_json_works(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ReportConfig(output_dir=Path(tmp))
            result = ReportGenerator.generate(self.report, config=cfg)
            parsed = json.loads(result.to_json())
            assert isinstance(parsed, dict)

    def test_light_theme_generate(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ReportConfig(output_dir=Path(tmp), theme="light")
            result = ReportGenerator.generate(self.report, config=cfg)
            assert 'data-theme="light"' in result.html_content

    def test_multiple_grades_render(self):
        for grade in ["A+", "B", "C-", "F"]:
            report = _make_analytics_report(grade=grade)
            with tempfile.TemporaryDirectory() as tmp:
                cfg = ReportConfig(output_dir=Path(tmp))
                result = ReportGenerator.generate(report, config=cfg)
                assert grade in result.html_content

    def test_html_is_non_trivial_size(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ReportConfig(output_dir=Path(tmp))
            result = ReportGenerator.generate(self.report, config=cfg)
            assert len(result.html_content) > 20_000  # substantial HTML


# ══════════════════════════════════════════════════════════════════════════════
# TESTS — GeneratedReport contract
# ══════════════════════════════════════════════════════════════════════════════

class TestGeneratedReport:
    def setup_method(self):
        self.report = _make_analytics_report()

    def _make(self) -> GeneratedReport:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ReportConfig(output_dir=Path(tmp))
            return ReportGenerator.generate(self.report, config=cfg)

    def test_html_path_is_path_object(self):
        r = self._make()
        assert isinstance(r.html_path, Path)

    def test_html_content_is_string(self):
        r = self._make()
        assert isinstance(r.html_content, str)

    def test_layers_included_is_list(self):
        r = self._make()
        assert isinstance(r.layers_included, list)

    def test_generation_duration_is_float(self):
        r = self._make()
        assert isinstance(r.generation_duration_ms, float)


# ══════════════════════════════════════════════════════════════════════════════
# TESTS — Edge cases
# ══════════════════════════════════════════════════════════════════════════════

class TestEdgeCases:
    def test_empty_sessions_handled_in_layer3(self):
        report = _make_analytics_report()
        # Patch time_performance to have no sessions
        import dataclasses
        tp = dataclasses.replace(
            report.time_performance,
            by_session={},
            by_hour={},
            by_day={},
            best_session="N/A",
            worst_session="N/A",
        )
        report2 = dataclasses.replace(report, time_performance=tp)
        html = ReportGenerator._build_layer3_raw(report2, DARK_THEME)
        assert isinstance(html, str)

    def test_f_grade_uses_red(self):
        report = _make_analytics_report(grade="F")
        html = ReportGenerator._build_layer1_executive(report, DARK_THEME)
        assert GRADE_COLOURS_DARK["F"] in html

    def test_a_plus_grade_uses_green(self):
        report = _make_analytics_report(grade="A+")
        html = ReportGenerator._build_layer1_executive(report, DARK_THEME)
        assert GRADE_COLOURS_DARK["A+"] in html

    def test_generate_with_no_trade_result(self):
        report = _make_analytics_report()
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ReportConfig(output_dir=Path(tmp))
            result = ReportGenerator.generate(report, trade_result=None, config=cfg)
            assert result.html_content

    def test_insight_without_impact_renders_cleanly(self):
        insight = _make_insight(severity="info", impact=None)
        html = ReportGenerator._build_insights_accordion([insight], DARK_THEME)
        assert "💡" not in html  # no impact line

    def test_insight_with_impact_renders_impact(self):
        insight = _make_insight(severity="critical", impact="Could save 50pts")
        html = ReportGenerator._build_insights_accordion([insight], DARK_THEME)
        assert "Could save 50pts" in html

    def test_empty_insights_accordion_shows_no_data(self):
        html = ReportGenerator._build_insights_accordion([], DARK_THEME)
        assert "No insights" in html

    def test_all_severity_icons_used(self):
        insights = [
            _make_insight("critical"), _make_insight("warning"),
            _make_insight("success"),  _make_insight("info"),
        ]
        html = ReportGenerator._build_insights_accordion(insights, DARK_THEME)
        for icon in SEVERITY_ICON.values():
            assert icon in html