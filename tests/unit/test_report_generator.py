"""
Unit Tests for ReportGenerator
================================
Tests HTML generation, Chart.js integration, and theme handling.
"""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch

from src.strategies.specific.modules.report_generator import (
    ReportGenerator,
    DARK_THEME,
    LIGHT_THEME,
    GRADE_COLOURS_DARK
)
from src.strategies.contracts.analytics_contracts import (
    AnalyticsReport,
    ExecutiveSummary,
    TimePerformanceBreakdown,
    TradeQualityAnalysis,
    RiskAdjustedMetrics,
    SessionMetrics
)
from src.strategies.contracts.report_contracts import GeneratedReport, ReportConfig
from src.strategies.contracts.metrics_contracts import MetricsReport


class TestReportGenerator:
    """Tests for ReportGenerator class."""

    @pytest.fixture
    def sample_metrics(self):
        """Create sample metrics for testing."""
        return MetricsReport(
            total_trades=150,
            winning_trades=90,
            losing_trades=60,
            win_rate=60.0,
            total_pnl_points=450.0,
            expectancy_points=3.0,
            profit_factor=2.5,
            avg_pnl_points=3.0,
            largest_win=15.0,
            largest_loss=-8.0,
            max_drawdown=-25.0,
            losing_streak=4,
            winning_streak=7,
            trades_per_week=12.5,
            trades_per_day=1.8,
            execution_duration_ms=150.0
        )

    @pytest.fixture
    def sample_analytics_report(self, sample_metrics):
        """Create a sample AnalyticsReport."""
        # Create minimal required objects
        exec_summary = ExecutiveSummary(
            performance_grade="B+",
            grade_reasoning="Score 78/100 - good performance",
            critical_insights=[],
            key_strengths=["Strong win rate", "Good profit factor"],
            improvement_areas=["High drawdown", "Losing streak"],
            overall_assessment="Strategy shows good performance with room for improvement."
        )
        
        time_perf = TimePerformanceBreakdown(
            by_session={
                "London": SessionMetrics(
                    session_name="London",
                    trades=80,
                    winning_trades=50,
                    win_rate=62.5,
                    total_pnl=300.0,
                    avg_pnl=3.75,
                    largest_win=15.0,
                    largest_loss=-5.0
                ),
                "New York": SessionMetrics(
                    session_name="New York",
                    trades=70,
                    winning_trades=40,
                    win_rate=57.1,
                    total_pnl=150.0,
                    avg_pnl=2.14,
                    largest_win=12.0,
                    largest_loss=-8.0
                )
            },
            by_hour={10: SessionMetrics(...)},
            by_day={"Monday": SessionMetrics(...)},
            best_session="London",
            worst_session="New York",
            insights=[]
        )
        
        quality = TradeQualityAnalysis(
            win_distribution=None,
            loss_distribution=None,
            duration_analysis=None,
            avg_bars_to_profit=2.5,
            avg_bars_to_loss=4.0,
            premature_exit_estimate="Exit timing appears reasonable",
            insights=[]
        )
        
        risk = RiskAdjustedMetrics(
            return_over_max_dd=18.0,
            avg_win_over_avg_loss=1.8,
            expectancy_per_trade=3.0,
            consistency_score=72.5,
            recovery_factor=2.2,
            insights=[]
        )
        
        return AnalyticsReport(
            executive_summary=exec_summary,
            time_performance=time_perf,
            trade_quality=quality,
            risk_adjusted=risk,
            comparative=None,
            input_metrics=sample_metrics,
            analysis_timestamp="2025-01-01T12:00:00",
            analysis_duration_ms=250.0
        )

    @pytest.fixture
    def sample_trade_result(self):
        """Create a mock TradeResult for equity curve."""
        class MockTrade:
            def __init__(self, entry_time, pnl):
                self.entry = type('obj', (), {'entry_time': entry_time})
                self.exit = type('obj', (), {'pnl_points': pnl})
        
        class MockTradeResult:
            def __init__(self):
                import pandas as pd
                base = pd.Timestamp("2025-01-01")
                self.trades = [
                    MockTrade(base, 10.0),
                    MockTrade(base + pd.Timedelta(days=1), 5.0),
                    MockTrade(base + pd.Timedelta(days=2), -3.0),
                    MockTrade(base + pd.Timedelta(days=3), 8.0),
                    MockTrade(base + pd.Timedelta(days=4), -2.0)
                ]
        
        return MockTradeResult()

    def test_generate_with_valid_report(self, sample_analytics_report, sample_trade_result):
        """Test report generation with valid inputs."""
        config = ReportConfig(
            title="Test Strategy Report",
            brand_name="TestStrategy",
            theme="dark",
            output_dir=Path("/tmp/test_reports")
        )
        
        with patch('pathlib.Path.mkdir'), patch('pathlib.Path.write_text'):
            generated = ReportGenerator.generate(
                analytics_report=sample_analytics_report,
                trade_result=sample_trade_result,
                config=config
            )
        
        assert isinstance(generated, GeneratedReport)
        assert generated.html_path is not None
        assert generated.html_content is not None
        assert generated.generation_duration_ms > 0
        assert generated.analytics_report == sample_analytics_report
        assert "executive" in generated.layers_included

    def test_generate_with_none_report(self):
        """Test that None analytics_report raises error."""
        with pytest.raises(ValueError, match="analytics_report must not be None"):
            ReportGenerator.generate(analytics_report=None)

    def test_generate_with_default_config(self, sample_analytics_report, sample_trade_result):
        """Test report generation with default config."""
        with patch('pathlib.Path.mkdir'), patch('pathlib.Path.write_text'):
            generated = ReportGenerator.generate(
                analytics_report=sample_analytics_report,
                trade_result=sample_trade_result,
                config=None  # Use default
            )
        
        assert isinstance(generated, GeneratedReport)

    def test_generate_without_trade_result(self, sample_analytics_report):
        """Test report generation without trade_result (equity curve placeholder)."""
        config = ReportConfig(
            title="Test Report",
            brand_name="TestStrategy"
        )
        
        with patch('pathlib.Path.mkdir'), patch('pathlib.Path.write_text'):
            generated = ReportGenerator.generate(
                analytics_report=sample_analytics_report,
                trade_result=None,
                config=config
            )
        
        # Should still generate, with placeholder
        assert generated.html_content is not None
        assert "placeholder" in generated.html_content.lower()

    def test_generate_with_raw_data(self, sample_analytics_report, sample_trade_result):
        """Test report generation with raw data included."""
        config = ReportConfig(
            title="Test Report",
            brand_name="TestStrategy",
            include_raw_data=True
        )
        
        with patch('pathlib.Path.mkdir'), patch('pathlib.Path.write_text'):
            generated = ReportGenerator.generate(
                analytics_report=sample_analytics_report,
                trade_result=sample_trade_result,
                config=config
            )
        
        assert "raw" in generated.layers_included
        assert "Raw Data" in generated.html_content

    def test_build_chart_data_with_trade_result(self, sample_analytics_report, sample_trade_result):
        """Test chart data building with trade result."""
        chart_data = ReportGenerator._build_chart_data(
            trade_result=sample_trade_result,
            report=sample_analytics_report
        )
        
        assert "equity_labels" in chart_data
        assert "equity_values" in chart_data
        assert len(chart_data["equity_labels"]) > 0
        assert len(chart_data["equity_values"]) > 0

    def test_build_chart_data_without_trade_result(self, sample_analytics_report):
        """Test chart data building without trade result."""
        chart_data = ReportGenerator._build_chart_data(
            trade_result=None,
            report=sample_analytics_report
        )
        
        assert chart_data["equity_labels"] == []
        assert chart_data["equity_values"] == []

    def test_build_layer1_executive(self, sample_analytics_report):
        """Test executive layer HTML generation."""
        html = ReportGenerator._build_layer1_executive(
            report=sample_analytics_report,
            colours=DARK_THEME
        )
        
        assert isinstance(html, str)
        assert "grade-hero" in html
        assert sample_analytics_report.executive_summary.performance_grade in html
        assert "kpi-strip" in html
        assert "insights-grid" in html

    def test_build_layer2_analytical(self, sample_analytics_report):
        """Test analytical layer HTML generation."""
        chart_data = {
            "equity_labels": ["Day1", "Day2"],
            "equity_values": [10, 15],
            "session_labels": ["London", "NY"],
            "session_pnl": [300, 150],
            "session_wr": [62.5, 57.1],
            "dist_labels": ["Small", "Medium", "Large"],
            "win_dist": [5, 3, 2],
            "loss_dist": [8, 1, 1],
            "dur_labels": ["Fast", "Normal", "Prolonged"],
            "dur_values": [5, 4, 1]
        }
        
        config = ReportConfig(chart_height_px=300)
        
        html = ReportGenerator._build_layer2_analytical(
            report=sample_analytics_report,
            colours=DARK_THEME,
            config=config,
            chart_data=chart_data
        )
        
        assert isinstance(html, str)
        assert "analytical-section" in html
        assert "charts-grid" in html
        assert "canvas" in html

    def test_build_layer3_raw(self, sample_analytics_report):
        """Test raw data layer HTML generation."""
        html = ReportGenerator._build_layer3_raw(
            report=sample_analytics_report,
            colours=DARK_THEME
        )
        
        assert isinstance(html, str)
        assert "raw-section-wrap" in html
        assert "data-table" in html
        assert "collapsible" in html or "details" in html

    def test_build_insights_accordion(self):
        """Test insights accordion HTML generation."""
        from src.strategies.contracts.analytics_contracts import Insight
        
        insights = [
            Insight(
                message="Critical insight 1",
                recommendation="Fix it",
                confidence="High",
                category="risk",
                severity="critical",
                impact_estimate="+50 pts"
            ),
            Insight(
                message="Warning insight",
                recommendation="Be careful",
                confidence="Medium",
                category="time",
                severity="warning"
            ),
            Insight(
                message="Info insight",
                recommendation="Note this",
                confidence="Low",
                category="quality",
                severity="info"
            )
        ]
        
        html = ReportGenerator._build_insights_accordion(
            insights=insights,
            colours=DARK_THEME
        )
        
        assert isinstance(html, str)
        assert "accordion" in html
        for insight in insights:
            assert insight.message in html
            assert insight.recommendation in html

    def test_build_simple_table(self):
        """Test simple table HTML generation."""
        headers = ["Metric", "Value"]
        rows = [
            ("Total Trades", "150"),
            ("Win Rate", "60.0%"),
            ("Total P&L", "+450.0")
        ]
        
        html = ReportGenerator._build_simple_table(
            headers=headers,
            rows=rows,
            colours=DARK_THEME
        )
        
        assert "<table" in html
        assert "<th>Metric</th>" in html
        assert "<th>Value</th>" in html
        assert "<td>150</td>" in html
        assert "<td>60.0%</td>" in html

    def test_build_data_table(self):
        """Test data table HTML generation."""
        headers = ["Day", "Trades", "Win Rate"]
        rows = [
            ("Monday", "50", "62.0%"),
            ("Tuesday", "45", "55.0%")
        ]
        
        html = ReportGenerator._build_data_table(
            headers=headers,
            rows=rows,
            colours=DARK_THEME
        )
        
        assert "<table" in html
        assert "data-table" in html

    def test_build_css_dark_theme(self):
        """Test CSS generation with dark theme."""
        css = ReportGenerator._build_css(
            colours=DARK_THEME,
            config=ReportConfig()
        )
        
        assert isinstance(css, str)
        assert DARK_THEME["bg"] in css
        assert DARK_THEME["text"] in css
        assert "@media" in css  # Responsive styles

    def test_build_css_light_theme(self):
        """Test CSS generation with light theme."""
        css = ReportGenerator._build_css(
            colours=LIGHT_THEME,
            config=ReportConfig()
        )
        
        assert isinstance(css, str)
        assert LIGHT_THEME["bg"] in css
        assert LIGHT_THEME["text"] in css

    def test_build_js(self):
        """Test JavaScript generation."""
        chart_data = {
            "equity_labels": ["Day1", "Day2"],
            "equity_values": [10, 15],
            "session_labels": ["London", "NY"],
            "session_pnl": [300, 150],
            "session_wr": [62.5, 57.1],
            "dist_labels": ["Small", "Medium", "Large"],
            "win_dist": [5, 3, 2],
            "loss_dist": [8, 1, 1],
            "dur_labels": ["Fast", "Normal", "Prolonged"],
            "dur_values": [5, 4, 1]
        }
        
        js = ReportGenerator._build_js(
            chart_data=chart_data,
            colours=DARK_THEME,
            config=ReportConfig(chart_height_px=300)
        )
        
        assert isinstance(js, str)
        assert "function showTab" in js
        assert "function toggleAcc" in js
        assert "function initCharts" in js
        assert "Chart.defaults" in js

    def test_save_html(self, tmp_path):
        """Test saving HTML to file."""
        html_content = "<html><body>Test Report</body></html>"
        config = ReportConfig(output_dir=str(tmp_path))
        
        path = ReportGenerator._save_html(html_content, config)
        
        assert path.exists()
        assert path.read_text(encoding="utf-8") == html_content
        
        # Check that timestamp is in filename
        assert "report_" in path.name
        assert path.suffix == ".html"

    def test_grade_colours_mapping(self):
        """Test grade to colour mapping."""
        assert GRADE_COLOURS_DARK["A+"] == DARK_THEME["green"]
        assert GRADE_COLOURS_DARK["B"] == DARK_THEME["accent"]
        assert GRADE_COLOURS_DARK["D-"] == DARK_THEME["red"]
        assert GRADE_COLOURS_DARK["F"] == DARK_THEME["red"]

    def test_brand_name_in_html(self, sample_analytics_report, sample_trade_result):
        """Test that brand name appears in HTML."""
        config = ReportConfig(
            title="Test Report",
            brand_name="CustomBrand123",
            output_dir=Path("/tmp/test_reports")
        )
        
        with patch('pathlib.Path.mkdir'), patch('pathlib.Path.write_text') as mock_write:
            ReportGenerator.generate(
                analytics_report=sample_analytics_report,
                trade_result=sample_trade_result,
                config=config
            )
            
            # Check that brand name was written
            written_html = mock_write.call_args[0][0]
            assert "CustomBrand123" in written_html

    def test_subtitle_in_html(self, sample_analytics_report, sample_trade_result):
        """Test that subtitle appears in HTML."""
        config = ReportConfig(
            title="Test Report",
            subtitle="Custom Subtitle for Testing",
            brand_name="TestStrategy"
        )
        
        with patch('pathlib.Path.mkdir'), patch('pathlib.Path.write_text') as mock_write:
            ReportGenerator.generate(
                analytics_report=sample_analytics_report,
                trade_result=sample_trade_result,
                config=config
            )
            
            written_html = mock_write.call_args[0][0]
            assert "Custom Subtitle for Testing" in written_html

    def test_chart_height_config(self, sample_analytics_report, sample_trade_result):
        """Test that chart height config is respected."""
        config = ReportConfig(
            title="Test Report",
            brand_name="TestStrategy",
            chart_height_px=500
        )
        
        with patch('pathlib.Path.mkdir'), patch('pathlib.Path.write_text') as mock_write:
            ReportGenerator.generate(
                analytics_report=sample_analytics_report,
                trade_result=sample_trade_result,
                config=config
            )
            
            written_html = mock_write.call_args[0][0]
            assert 'height="500"' in written_html

    def test_theme_switching(self, sample_analytics_report, sample_trade_result):
        """Test that theme switching works."""
        # Dark theme
        config_dark = ReportConfig(theme="dark", brand_name="Test")
        with patch('pathlib.Path.mkdir'), patch('pathlib.Path.write_text') as mock_write:
            ReportGenerator.generate(
                analytics_report=sample_analytics_report,
                trade_result=sample_trade_result,
                config=config_dark
            )
            dark_html = mock_write.call_args[0][0]
        
        # Light theme
        config_light = ReportConfig(theme="light", brand_name="Test")
        with patch('pathlib.Path.mkdir'), patch('pathlib.Path.write_text') as mock_write:
            ReportGenerator.generate(
                analytics_report=sample_analytics_report,
                trade_result=sample_trade_result,
                config=config_light
            )
            light_html = mock_write.call_args[0][0]
        
        # Should be different
        assert dark_html != light_html
        assert 'data-theme="dark"' in dark_html
        assert 'data-theme="light"' in light_html