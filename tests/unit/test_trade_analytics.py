"""
Unit Tests for TradeAnalytics
===============================
Tests insight generation, performance grading, and markdown formatting.
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from src.strategies.specific.modules.trade_analytics import TradeAnalytics
from src.strategies.contracts.analytics_contracts import (
    AnalyticsReport,
    Insight,
    SessionMetrics,
    TradingSessionConfig
)
from src.strategies.contracts.trade_contracts import (
    Trade, TradeEntry, TradeExit, ExitReason, TradeDirection
)
from src.strategies.contracts.metrics_contracts import MetricsReport


class TestTradeAnalytics:
    """Tests for TradeAnalytics class."""

    @pytest.fixture
    def sample_trades(self):
        """Generate sample trades for testing analytics."""
        trades = []
        base_time = pd.Timestamp("2025-01-01 10:00:00")
        
        # Create trades with various properties for comprehensive testing
        
        # Winning trades - various sizes
        win_sizes = [2.5, 4.0, 8.0, 3.5, 6.0, 2.0, 5.5, 9.0, 3.0, 7.5]
        for i, size in enumerate(win_sizes):
            entry = TradeEntry(
                entry_id=f"WE{i}",
                trade_manager_id=i,
                entry_time=base_time + timedelta(hours=i),
                direction=TradeDirection.LONG if i % 2 == 0 else TradeDirection.SHORT,
                entry_price=100.0,
                stop_loss=99.0 if i % 2 == 0 else 101.0,
                take_profit=105.0 if i % 2 == 0 else 95.0,
                position_size=1.0
            )
            exit = TradeExit(
                entry=entry,
                exit_time=entry.entry_time + timedelta(minutes=15 * (i % 3 + 1)),
                exit_price=entry.entry_price + size if entry.is_long else entry.entry_price - size,
                exit_reason=ExitReason.TAKE_PROFIT
            )
            trades.append(Trade(entry=entry, exit=exit))
        
        # Losing trades - various sizes
        loss_sizes = [1.0, 2.5, 4.0, 1.5, 3.0]
        for i, size in enumerate(loss_sizes):
            entry = TradeEntry(
                entry_id=f"LE{i}",
                trade_manager_id=i+20,
                entry_time=base_time + timedelta(hours=i+10),
                direction=TradeDirection.LONG if i % 2 == 0 else TradeDirection.SHORT,
                entry_price=100.0,
                stop_loss=99.0 if i % 2 == 0 else 101.0,
                take_profit=105.0 if i % 2 == 0 else 95.0,
                position_size=1.0
            )
            exit = TradeExit(
                entry=entry,
                exit_time=entry.entry_time + timedelta(minutes=5),
                exit_price=entry.entry_price - size if entry.is_long else entry.entry_price + size,
                exit_reason=ExitReason.STOP_LOSS
            )
            trades.append(Trade(entry=entry, exit=exit))
        
        return trades

    @pytest.fixture
    def sample_metrics(self, sample_trades):
        """Create sample MetricsReport."""
        from src.strategies.contracts.metrics_contracts import MetricsReport
        
        # Calculate basic metrics manually
        closed = [t for t in sample_trades if t.exit]
        wins = [t for t in closed if t.exit.is_win]
        losses = [t for t in closed if t.exit.is_loss]
        
        total_pnl = sum(t.exit.pnl_points for t in closed)
        gross_profit = sum(t.exit.pnl_points for t in wins)
        gross_loss = abs(sum(t.exit.pnl_points for t in losses))
        
        return MetricsReport(
            total_trades=len(closed),
            winning_trades=len(wins),
            losing_trades=len(losses),
            win_rate=len(wins)/len(closed)*100 if closed else 0.0,
            total_pnl_points=total_pnl,
            expectancy_points=total_pnl/len(closed) if closed else 0.0,
            profit_factor=gross_profit/gross_loss if gross_loss else 0.0,
            avg_pnl_points=total_pnl/len(closed) if closed else 0.0,
            largest_win=max((t.exit.pnl_points for t in wins), default=0.0),
            largest_loss=min((t.exit.pnl_points for t in losses), default=0.0),
            max_drawdown=-5.0,
            losing_streak=3,
            winning_streak=5,
            trades_per_week=10.5,
            trades_per_day=1.5,
            execution_duration_ms=100.0
        )

    @pytest.fixture
    def sample_trade_result(self, sample_trades):
        """Create a mock TradeResult."""
        class MockTradeResult:
            def __init__(self, trades):
                self.trades = trades
                self.closed_trades = [t for t in trades if t.exit]
        
        return MockTradeResult(sample_trades)

    def test_analyze_with_auto_metrics(self, sample_trade_result, test_config):
        """Test analyze with auto-calculated metrics."""
        report = TradeAnalytics.analyze(
            trade_result=sample_trade_result,
            config=test_config
        )
        
        assert isinstance(report, AnalyticsReport)
        assert report.input_metrics is not None
        assert report.executive_summary is not None
        assert report.time_performance is not None
        assert report.trade_quality is not None
        assert report.risk_adjusted is not None

    def test_analyze_with_explicit_metrics(self, sample_trade_result, test_config, sample_metrics):
        """Test analyze with explicitly provided metrics."""
        report = TradeAnalytics.analyze(
            trade_result=sample_trade_result,
            config=test_config,
            metrics=sample_metrics
        )
        
        assert isinstance(report, AnalyticsReport)
        assert report.input_metrics == sample_metrics

    def test_analyze_no_closed_trades(self, test_config):
        """Test analyze with no closed trades."""
        class MockEmptyResult:
            def __init__(self):
                self.trades = []
                self.closed_trades = []
        
        report = TradeAnalytics.analyze(
            trade_result=MockEmptyResult(),
            config=test_config
        )
        
        assert isinstance(report, AnalyticsReport)
        assert report.time_performance.by_session == {}
        assert report.trade_quality.avg_bars_to_profit is None

    def test_time_performance_analysis(self, sample_trade_result, test_config, sample_metrics):
        """Test time performance analysis."""
        time_perf = TradeAnalytics._analyze_time_performance(
            trade_result=sample_trade_result,
            metrics=sample_metrics,
            session_config=TradingSessionConfig()
        )
        
        assert time_perf.by_session is not None
        assert time_perf.by_hour is not None
        assert time_perf.by_day is not None
        assert isinstance(time_perf.insights, list)

    def test_get_session_for_hour(self):
        """Test hour to session mapping."""
        config = TradingSessionConfig(
            sessions={
                "Asia": (0, 8),
                "London": (8, 16),
                "New York": (16, 24)
            }
        )
        
        # Test various hours
        assert TradeAnalytics._get_session_for_hour(3, config) == "Asia"
        assert TradeAnalytics._get_session_for_hour(10, config) == "London"
        assert TradeAnalytics._get_session_for_hour(20, config) == "New York"
        assert TradeAnalytics._get_session_for_hour(25, config) == "Other"

    def test_calculate_session_metrics(self, sample_trades):
        """Test session metrics calculation."""
        metrics = TradeAnalytics._calculate_session_metrics(
            trades=sample_trades,
            session_name="Test Session"
        )
        
        assert isinstance(metrics, SessionMetrics)
        assert metrics.session_name == "Test Session"
        assert metrics.trades == len(sample_trades)
        assert metrics.winning_trades >= 0
        assert 0 <= metrics.win_rate <= 100

    def test_generate_time_insights(self, sample_metrics):
        """Test time-based insight generation."""
        # Create sample session data
        by_session = {
            "Asia": SessionMetrics(
                session_name="Asia",
                trades=30,
                winning_trades=10,
                win_rate=33.3,
                total_pnl=-50.0,
                avg_pnl=-1.67,
                largest_win=5.0,
                largest_loss=-10.0
            ),
            "London": SessionMetrics(
                session_name="London",
                trades=50,
                winning_trades=30,
                win_rate=60.0,
                total_pnl=150.0,
                avg_pnl=3.0,
                largest_win=8.0,
                largest_loss=-4.0
            )
        }
        
        insights = TradeAnalytics._generate_time_insights(
            by_session=by_session,
            by_hour={10: by_session["London"]},
            by_day={"Monday": by_session["London"]},
            overall_metrics=sample_metrics
        )
        
        assert isinstance(insights, list)
        if insights:
            assert all(isinstance(i, Insight) for i in insights)

    def test_trade_quality_analysis(self, sample_trade_result, sample_metrics):
        """Test trade quality analysis."""
        quality = TradeAnalytics._analyze_trade_quality(
            trade_result=sample_trade_result,
            metrics=sample_metrics
        )
        
        assert quality.win_distribution is not None
        assert quality.loss_distribution is not None
        assert quality.duration_analysis is not None
        assert quality.premature_exit_estimate is not None
        assert isinstance(quality.insights, list)

    def test_calculate_trade_distribution(self, sample_trades):
        """Test trade distribution calculation."""
        wins = [t for t in sample_trades if t.exit and t.exit.is_win]
        losses = [t for t in sample_trades if t.exit and t.exit.is_loss]
        
        win_dist = TradeAnalytics._calculate_trade_distribution(wins, is_wins=True)
        loss_dist = TradeAnalytics._calculate_trade_distribution(losses, is_wins=False)
        
        assert win_dist.small_count + win_dist.medium_count + win_dist.large_count == len(wins)
        assert loss_dist.small_count + loss_dist.medium_count + loss_dist.large_count == len(losses)
        
        # Percentages should sum to ~100
        assert abs(win_dist.small_pct + win_dist.medium_pct + win_dist.large_pct - 100) < 0.1
        assert abs(loss_dist.small_pct + loss_dist.medium_pct + loss_dist.large_pct - 100) < 0.1

    def test_analyze_duration_patterns(self, sample_trades):
        """Test duration pattern analysis."""
        duration = TradeAnalytics._analyze_duration_patterns(sample_trades)
        
        assert duration.avg_bars > 0
        assert duration.median_bars >= 0
        assert duration.fast_exits_count >= 0
        assert duration.normal_exits_count >= 0
        assert duration.prolonged_exits_count >= 0
        assert 0 <= duration.fast_exits_pct <= 100
        assert isinstance(duration.insights, list)

    def test_build_premature_exit_narrative(self):
        """Test premature exit narrative generation."""
        from src.strategies.contracts.analytics_contracts import DurationAnalysis
        
        duration = DurationAnalysis(
            avg_bars=2.5,
            median_bars=2,
            fast_exits_count=8,
            normal_exits_count=2,
            prolonged_exits_count=0,
            fast_exits_pct=80.0,
            insights=[]
        )
        
        narrative = TradeAnalytics._build_premature_exit_narrative(
            duration=duration,
            avg_bars_to_profit=1.5,
            avg_bars_to_loss=3.0
        )
        
        assert isinstance(narrative, str)
        assert "80%" in narrative or "fast" in narrative

    def test_generate_quality_insights(self, sample_metrics):
        """Test quality insight generation."""
        from src.strategies.contracts.analytics_contracts import (
            TradeDistribution, DurationAnalysis
        )
        
        win_dist = TradeDistribution(
            small_count=5, medium_count=3, large_count=2,
            small_pct=50.0, medium_pct=30.0, large_pct=20.0
        )
        loss_dist = TradeDistribution(
            small_count=8, medium_count=1, large_count=1,
            small_pct=80.0, medium_pct=10.0, large_pct=10.0
        )
        duration = DurationAnalysis(
            avg_bars=3.0,
            median_bars=3,
            fast_exits_count=5,
            normal_exits_count=4,
            prolonged_exits_count=1,
            fast_exits_pct=50.0,
            insights=[]
        )
        
        insights = TradeAnalytics._generate_quality_insights(
            win_dist=win_dist,
            loss_dist=loss_dist,
            duration=duration,
            metrics=sample_metrics,
            avg_bars_to_profit=2.0,
            avg_bars_to_loss=4.0
        )
        
        assert isinstance(insights, list)
        if insights:
            assert all(i.category == "quality" for i in insights)

    def test_risk_adjusted_analysis(self, sample_trade_result, sample_metrics):
        """Test risk-adjusted metrics analysis."""
        risk = TradeAnalytics._analyze_risk_adjusted(
            trade_result=sample_trade_result,
            metrics=sample_metrics
        )
        
        assert risk.return_over_max_dd is not None
        assert risk.avg_win_over_avg_loss is not None
        assert risk.expectancy_per_trade is not None
        assert 0 <= risk.consistency_score <= 100
        assert risk.recovery_factor is not None
        assert isinstance(risk.insights, list)

    def test_calculate_consistency_score(self, sample_trades):
        """Test consistency score calculation."""
        score = TradeAnalytics._calculate_consistency_score(sample_trades)
        
        assert 0 <= score <= 100
        
        # Test with identical P&L (perfect consistency)
        class MockTrade:
            def __init__(self, pnl):
                self.exit = type('obj', (), {'pnl_points': pnl})
        
        identical_trades = [MockTrade(5.0) for _ in range(10)]
        perfect_score = TradeAnalytics._calculate_consistency_score(identical_trades)
        assert perfect_score > 90  # Should be high
        
        # Test with highly variable P&L
        variable_trades = [MockTrade(100.0), MockTrade(-100.0), MockTrade(50.0), MockTrade(-50.0)]
        low_score = TradeAnalytics._calculate_consistency_score(variable_trades)
        assert low_score < 50  # Should be low

    def test_generate_risk_insights(self, sample_metrics):
        """Test risk insight generation."""
        from src.strategies.contracts.analytics_contracts import RiskAdjustedMetrics
        
        risk_metrics = RiskAdjustedMetrics(
            return_over_max_dd=0.5,
            avg_win_over_avg_loss=0.8,  # Bad ratio
            expectancy_per_trade=-0.1,   # Negative expectancy
            consistency_score=25.0,       # Low consistency
            recovery_factor=0.3,          # Weak recovery
            insights=[]
        )
        
        insights = TradeAnalytics._generate_risk_insights(
            risk_metrics=risk_metrics,
            base_metrics=sample_metrics
        )
        
        assert isinstance(insights, list)
        # Should generate multiple insights (critical/warning)
        assert len(insights) >= 3

    def test_calculate_performance_grade(self, sample_metrics):
        """Test performance grade calculation."""
        from src.strategies.contracts.analytics_contracts import RiskAdjustedMetrics
        
        # Create risk metrics with good scores
        risk_good = RiskAdjustedMetrics(
            return_over_max_dd=3.0,
            avg_win_over_avg_loss=2.0,
            expectancy_per_trade=0.5,
            consistency_score=85.0,
            recovery_factor=2.5,
            insights=[]
        )
        
        grade, reasoning = TradeAnalytics._calculate_performance_grade(
            metrics=sample_metrics,
            risk_metrics=risk_good
        )
        
        assert grade in ["A+", "A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D+", "D", "F"]
        assert isinstance(reasoning, str)
        
        # Test with poor metrics
        poor_metrics = MetricsReport(
            total_trades=10,
            winning_trades=2,
            losing_trades=8,
            win_rate=20.0,
            total_pnl_points=-100.0,
            expectancy_points=-10.0,
            profit_factor=0.2,
            avg_pnl_points=-10.0,
            largest_win=5.0,
            largest_loss=-50.0,
            max_drawdown=-80.0,
            losing_streak=5,
            winning_streak=1,
            trades_per_week=2.0,
            trades_per_day=0.3,
            execution_duration_ms=100.0
        )
        
        risk_poor = RiskAdjustedMetrics(
            return_over_max_dd=0.1,
            avg_win_over_avg_loss=0.3,
            expectancy_per_trade=-10.0,
            consistency_score=15.0,
            recovery_factor=0.1,
            insights=[]
        )
        
        poor_grade, _ = TradeAnalytics._calculate_performance_grade(
            metrics=poor_metrics,
            risk_metrics=risk_poor
        )
        
        assert poor_grade == "F" or poor_grade.startswith("D")

    def test_collect_critical_insights(self):
        """Test critical insights collection and prioritization."""
        insights = [
            Insight(
                message="Critical issue 1",
                recommendation="Fix it",
                confidence="High",
                category="risk",
                severity="critical"
            ),
            Insight(
                message="Warning issue",
                recommendation="Be careful",
                confidence="Medium",
                category="time",
                severity="warning"
            ),
            Insight(
                message="Info message",
                recommendation="Note this",
                confidence="Low",
                category="quality",
                severity="info"
            ),
            Insight(
                message="Success message",
                recommendation="Keep doing",
                confidence="High",
                category="risk",
                severity="success"
            ),
            Insight(
                message="Critical issue 2",
                recommendation="Fix it too",
                confidence="High",
                category="time",
                severity="critical"
            ),
            Insight(
                message="Critical issue 3",
                recommendation="Fix it three",
                confidence="High",
                category="quality",
                severity="critical"
            )
        ]
        
        # Mock the required structures
        class MockTimePerf:
            def __init__(self):
                self.insights = insights[:2]
        
        class MockQuality:
            def __init__(self):
                self.insights = insights[2:4]
        
        class MockRisk:
            def __init__(self):
                self.insights = insights[4:]
        
        critical = TradeAnalytics._collect_critical_insights(
            time_perf=MockTimePerf(),
            quality=MockQuality(),
            risk=MockRisk()
        )
        
        # Should return top 5 by priority
        assert len(critical) <= 5
        # First insight should be critical severity
        if critical:
            assert critical[0].severity == "critical"

    def test_generate_executive_summary(self, sample_metrics):
        """Test executive summary generation."""
        from src.strategies.contracts.analytics_contracts import (
            TimePerformanceBreakdown, TradeQualityAnalysis, RiskAdjustedMetrics
        )
        
        # Create minimal required objects
        time_perf = TimePerformanceBreakdown(
            by_session={},
            by_hour={},
            by_day={},
            best_session="N/A",
            worst_session="N/A",
            insights=[]
        )
        
        quality = TradeQualityAnalysis(
            win_distribution=None,
            loss_distribution=None,
            duration_analysis=None,
            avg_bars_to_profit=None,
            avg_bars_to_loss=None,
            premature_exit_estimate="",
            insights=[]
        )
        
        risk = RiskAdjustedMetrics(
            return_over_max_dd=2.0,
            avg_win_over_avg_loss=1.5,
            expectancy_per_trade=0.3,
            consistency_score=75.0,
            recovery_factor=2.0,
            insights=[]
        )
        
        summary = TradeAnalytics._generate_executive_summary(
            metrics=sample_metrics,
            time_perf=time_perf,
            quality=quality,
            risk=risk,
            comparative=None
        )
        
        assert summary.performance_grade is not None
        assert summary.grade_reasoning is not None
        assert isinstance(summary.critical_insights, list)
        assert isinstance(summary.key_strengths, list)
        assert isinstance(summary.improvement_areas, list)
        assert summary.overall_assessment is not None

    def test_format_markdown_report(self, sample_trade_result, test_config, sample_metrics):
        """Test markdown report formatting."""
        report = TradeAnalytics.analyze(
            trade_result=sample_trade_result,
            config=test_config,
            metrics=sample_metrics
        )
        
        markdown = TradeAnalytics.format_markdown_report(report)
        
        assert isinstance(markdown, str)
        assert "STRATEGY PERFORMANCE ANALYSIS" in markdown
        assert "OVERALL ASSESSMENT" in markdown
        assert "KEY INSIGHTS" in markdown
        assert "STRENGTHS" in markdown
        assert "IMPROVEMENT AREAS" in markdown
        assert "TIME-BASED PERFORMANCE" in markdown
        assert "TRADE QUALITY" in markdown
        assert "RISK-ADJUSTED METRICS" in markdown
        assert "PERFORMANCE GRADE" in markdown

    def test_analyze_trades_convenience_function(self, sample_trade_result, test_config):
        """Test the convenience analyze_trades function."""
        from src.strategies.specific.modules.trade_analytics import analyze_trades
        
        report = analyze_trades(
            trade_result=sample_trade_result,
            config=test_config
        )
        
        assert isinstance(report, AnalyticsReport)

    def test_save_report(self, sample_trade_result, test_config, tmp_path):
        """Test saving report to files."""
        report = TradeAnalytics.analyze(
            trade_result=sample_trade_result,
            config=test_config
        )
        
        # Test saving
        TradeAnalytics._save_report(report, output_dir=tmp_path)
        
        # Check that files were created
        json_files = list(tmp_path.glob("analytics_*.json"))
        md_files = list(tmp_path.glob("analytics_*.md"))
        
        assert len(json_files) >= 1
        assert len(md_files) >= 1