"""
Unit Tests for Analytics Contracts
====================================
Tests all analytics dataclasses: TradingSessionConfig, Insight, SessionMetrics,
TimePerformanceBreakdown, TradeDistribution, DurationAnalysis, TradeQualityAnalysis,
RiskAdjustedMetrics, ComparativeContext, ExecutiveSummary, AnalyticsReport.
"""

import pytest
import json
from datetime import datetime

from src.strategies.contracts.analytics_contracts import (
    TradingSessionConfig,
    Insight,
    SessionMetrics,
    TimePerformanceBreakdown,
    TradeDistribution,
    DurationAnalysis,
    TradeQualityAnalysis,
    RiskAdjustedMetrics,
    ComparativeContext,
    ExecutiveSummary,
    AnalyticsReport,
    create_empty_insight,
    create_empty_session_metrics
)


class TestTradingSessionConfig:
    """Tests for TradingSessionConfig contract."""

    def test_default_config(self):
        """Test default trading session configuration."""
        config = TradingSessionConfig()
        
        assert "Asia" in config.sessions
        assert "London" in config.sessions
        assert "NY" in config.sessions
        assert config.sessions["Asia"] == (0, 8)
        assert config.sessions["London"] == (8, 16)
        assert config.sessions["NY"] == (16, 24)

    def test_custom_config(self):
        """Test custom session configuration."""
        config = TradingSessionConfig(
            sessions={
                "Morning": (9, 12),
                "Afternoon": (12, 17)
            }
        )
        
        assert "Morning" in config.sessions
        assert "Afternoon" in config.sessions
        assert config.sessions["Morning"] == (9, 12)
        assert config.sessions["Afternoon"] == (12, 17)

    def test_empty_sessions_raises(self):
        """Test that empty sessions dict raises error."""
        with pytest.raises(ValueError, match="at least one session must be defined"):
            TradingSessionConfig(sessions={})

    def test_invalid_session_bounds(self):
        """Test validation of session hour bounds."""
        # Wrong number of bounds
        with pytest.raises(ValueError, match="must have exactly.*start, end"):
            TradingSessionConfig(sessions={"Test": (0, 8, 16)})
        
        # Start out of range
        with pytest.raises(ValueError, match="hours out of range"):
            TradingSessionConfig(sessions={"Test": (-1, 8)})
        
        # End out of range
        with pytest.raises(ValueError, match="hours out of range"):
            TradingSessionConfig(sessions={"Test": (0, 25)})
        
        # Start >= end
        with pytest.raises(ValueError, match="must be less than end"):
            TradingSessionConfig(sessions={"Test": (8, 8)})
        
        with pytest.raises(ValueError, match="must be less than end"):
            TradingSessionConfig(sessions={"Test": (10, 8)})

    def test_frozen(self):
        """Test that config is immutable."""
        config = TradingSessionConfig()
        
        with pytest.raises(Exception):  # FrozenInstanceError
            config.sessions = {"New": (0, 24)}


class TestInsight:
    """Tests for Insight contract."""

    def test_valid_insight(self):
        """Test creating a valid insight."""
        insight = Insight(
            message="Test insight",
            recommendation="Test recommendation",
            confidence="High",
            impact_estimate="+50 pts",
            category="risk",
            severity="critical"
        )
        
        assert insight.message == "Test insight"
        assert insight.recommendation == "Test recommendation"
        assert insight.confidence == "High"
        assert insight.impact_estimate == "+50 pts"
        assert insight.category == "risk"
        assert insight.severity == "critical"

    def test_impact_estimate_optional(self):
        """Test insight without impact estimate."""
        insight = Insight(
            message="Test insight",
            recommendation="Test recommendation",
            confidence="Medium",
            impact_estimate=None,
            category="time",
            severity="warning"
        )
        
        assert insight.impact_estimate is None

    def test_invalid_confidence(self):
        """Test validation of confidence field."""
        with pytest.raises(ValueError, match="confidence.*must be one of"):
            Insight(
                message="Test",
                recommendation="Test",
                confidence="VeryHigh",
                impact_estimate=None,
                category="general",
                severity="info"
            )

    def test_invalid_category(self):
        """Test validation of category field."""
        with pytest.raises(ValueError, match="category.*must be one of"):
            Insight(
                message="Test",
                recommendation="Test",
                confidence="High",
                impact_estimate=None,
                category="invalid",
                severity="info"
            )

    @pytest.mark.parametrize("severity", ["critical", "warning", "info", "success"])
    def test_valid_severities(self, severity):
        """Test all valid severity values."""
        insight = Insight(
            message="Test",
            recommendation="Test",
            confidence="High",
            impact_estimate=None,
            category="general",
            severity=severity
        )
        assert insight.severity == severity

    def test_invalid_severity(self):
        """Test validation of severity field."""
        with pytest.raises(ValueError, match="severity.*must be one of"):
            Insight(
                message="Test",
                recommendation="Test",
                confidence="High",
                impact_estimate=None,
                category="general",
                severity="invalid"
            )

    def test_to_dict(self):
        """Test serialization to dict."""
        insight = Insight(
            message="Test insight",
            recommendation="Test recommendation",
            confidence="High",
            impact_estimate="+50 pts",
            category="risk",
            severity="critical"
        )
        
        d = insight.to_dict()
        
        assert d["message"] == "Test insight"
        assert d["recommendation"] == "Test recommendation"
        assert d["confidence"] == "High"
        assert d["impact_estimate"] == "+50 pts"
        assert d["category"] == "risk"
        assert d["severity"] == "critical"


class TestSessionMetrics:
    """Tests for SessionMetrics contract."""

    def test_valid_metrics(self):
        """Test creating valid session metrics."""
        metrics = SessionMetrics(
            session_name="London",
            trades=100,
            winning_trades=60,
            win_rate=60.0,
            total_pnl=1500.5,
            avg_pnl=15.005,
            largest_win=45.0,
            largest_loss=-12.5
        )
        
        assert metrics.session_name == "London"
        assert metrics.trades == 100
        assert metrics.winning_trades == 60
        assert metrics.win_rate == 60.0
        assert metrics.total_pnl == 1500.5
        assert metrics.avg_pnl == 15.005
        assert metrics.largest_win == 45.0
        assert metrics.largest_loss == -12.5

    def test_negative_trades_raises(self):
        """Test that negative trades count raises error."""
        with pytest.raises(ValueError, match="trades cannot be negative"):
            SessionMetrics(
                session_name="Test",
                trades=-5,
                winning_trades=0,
                win_rate=0.0,
                total_pnl=0.0,
                avg_pnl=0.0,
                largest_win=0.0,
                largest_loss=0.0
            )

    def test_winning_trades_out_of_range(self):
        """Test that winning_trades > trades raises error."""
        with pytest.raises(ValueError, match="winning_trades.*must be 0–"):
            SessionMetrics(
                session_name="Test",
                trades=10,
                winning_trades=15,
                win_rate=150.0,
                total_pnl=0.0,
                avg_pnl=0.0,
                largest_win=0.0,
                largest_loss=0.0
            )

    @pytest.mark.parametrize("win_rate", [-10, 110])
    def test_win_rate_out_of_range(self, win_rate):
        """Test that win_rate outside 0-100 raises error."""
        with pytest.raises(ValueError, match="win_rate must be 0–100"):
            SessionMetrics(
                session_name="Test",
                trades=10,
                winning_trades=5,
                win_rate=win_rate,
                total_pnl=0.0,
                avg_pnl=0.0,
                largest_win=0.0,
                largest_loss=0.0
            )

    def test_to_dict(self):
        """Test serialization to dict."""
        metrics = SessionMetrics(
            session_name="London",
            trades=100,
            winning_trades=60,
            win_rate=60.0,
            total_pnl=1500.5,
            avg_pnl=15.005,
            largest_win=45.0,
            largest_loss=-12.5
        )
        
        d = metrics.to_dict()
        
        assert d["session_name"] == "London"
        assert d["trades"] == 100
        assert d["winning_trades"] == 60
        assert d["win_rate"] == 60.0
        assert d["total_pnl"] == 1500.5
        assert d["avg_pnl"] == 15.01  # Rounded
        assert d["largest_win"] == 45.0
        assert d["largest_loss"] == -12.5


class TestTimePerformanceBreakdown:
    """Tests for TimePerformanceBreakdown contract."""

    @pytest.fixture
    def sample_session_metrics(self):
        """Sample session metrics for testing."""
        return {
            "London": SessionMetrics(
                session_name="London",
                trades=50,
                winning_trades=30,
                win_rate=60.0,
                total_pnl=500.0,
                avg_pnl=10.0,
                largest_win=20.0,
                largest_loss=-5.0
            ),
            "NY": SessionMetrics(
                session_name="NY",
                trades=40,
                winning_trades=20,
                win_rate=50.0,
                total_pnl=200.0,
                avg_pnl=5.0,
                largest_win=15.0,
                largest_loss=-3.0
            )
        }

    @pytest.fixture
    def sample_hour_metrics(self):
        """Sample hour metrics for testing."""
        return {
            10: SessionMetrics(
                session_name="10:00",
                trades=20,
                winning_trades=12,
                win_rate=60.0,
                total_pnl=150.0,
                avg_pnl=7.5,
                largest_win=10.0,
                largest_loss=-2.0
            ),
            14: SessionMetrics(
                session_name="14:00",
                trades=15,
                winning_trades=8,
                win_rate=53.33,
                total_pnl=80.0,
                avg_pnl=5.33,
                largest_win=8.0,
                largest_loss=-1.5
            )
        }

    @pytest.fixture
    def sample_day_metrics(self):
        """Sample day metrics for testing."""
        return {
            "Monday": SessionMetrics(
                session_name="Monday",
                trades=25,
                winning_trades=15,
                win_rate=60.0,
                total_pnl=200.0,
                avg_pnl=8.0,
                largest_win=12.0,
                largest_loss=-3.0
            ),
            "Tuesday": SessionMetrics(
                session_name="Tuesday",
                trades=20,
                winning_trades=10,
                win_rate=50.0,
                total_pnl=100.0,
                avg_pnl=5.0,
                largest_win=10.0,
                largest_loss=-2.0
            )
        }

    @pytest.fixture
    def sample_insights(self):
        """Sample insights for testing."""
        return [
            Insight(
                message="London session performing well",
                recommendation="Maintain current approach",
                confidence="High",
                impact_estimate=None,
                category="time",
                severity="success"
            )
        ]

    def test_valid_breakdown(self, sample_session_metrics, sample_hour_metrics,
                            sample_day_metrics, sample_insights):
        """Test creating valid time performance breakdown."""
        breakdown = TimePerformanceBreakdown(
            by_session=sample_session_metrics,
            by_hour=sample_hour_metrics,
            by_day=sample_day_metrics,
            best_session="London",
            worst_session="NY",
            insights=sample_insights
        )
        
        assert breakdown.by_session == sample_session_metrics
        assert breakdown.by_hour == sample_hour_metrics
        assert breakdown.by_day == sample_day_metrics
        assert breakdown.best_session == "London"
        assert breakdown.worst_session == "NY"
        assert breakdown.insights == sample_insights

    def test_invalid_hour_key(self, sample_session_metrics, sample_day_metrics,
                              sample_insights):
        """Test that hour keys must be 0-23."""
        with pytest.raises(ValueError, match="Hour key must be 0–23"):
            TimePerformanceBreakdown(
                by_session=sample_session_metrics,
                by_hour={24: sample_session_metrics["London"]},  # Invalid hour
                by_day=sample_day_metrics,
                best_session="London",
                worst_session="NY",
                insights=sample_insights
            )

    def test_invalid_day_key(self, sample_session_metrics, sample_hour_metrics,
                             sample_insights):
        """Test that day keys must be valid weekday names."""
        with pytest.raises(ValueError, match="Invalid weekday key"):
            TimePerformanceBreakdown(
                by_session=sample_session_metrics,
                by_hour=sample_hour_metrics,
                by_day={"Funday": sample_session_metrics["London"]},
                best_session="London",
                worst_session="NY",
                insights=sample_insights
            )

    def test_to_dict(self, sample_session_metrics, sample_hour_metrics,
                     sample_day_metrics, sample_insights):
        """Test serialization to dict."""
        breakdown = TimePerformanceBreakdown(
            by_session=sample_session_metrics,
            by_hour=sample_hour_metrics,
            by_day=sample_day_metrics,
            best_session="London",
            worst_session="NY",
            insights=sample_insights
        )
        
        d = breakdown.to_dict()
        
        assert "by_session" in d
        assert "by_hour" in d
        assert "by_day" in d
        assert d["best_session"] == "London"
        assert d["worst_session"] == "NY"
        assert len(d["insights"]) == 1


class TestTradeDistribution:
    """Tests for TradeDistribution contract."""

    def test_valid_distribution(self):
        """Test creating valid trade distribution."""
        dist = TradeDistribution(
            small_count=10,
            medium_count=5,
            large_count=2,
            small_pct=58.82,
            medium_pct=29.41,
            large_pct=11.77
        )
        
        assert dist.small_count == 10
        assert dist.medium_count == 5
        assert dist.large_count == 2
        assert dist.small_pct == 58.82
        assert dist.medium_pct == 29.41
        assert dist.large_pct == 11.77

    def test_percentage_sum_validation(self):
        """Test that percentages must sum to ~100."""
        with pytest.raises(ValueError, match="percentages must sum to 100"):
            TradeDistribution(
                small_count=10,
                medium_count=5,
                large_count=2,
                small_pct=50.0,
                medium_pct=30.0,
                large_pct=30.0  # Sums to 110
            )

    def test_zero_trades_allowed(self):
        """Test distribution with zero trades."""
        dist = TradeDistribution(
            small_count=0,
            medium_count=0,
            large_count=0,
            small_pct=0.0,
            medium_pct=0.0,
            large_pct=0.0
        )
        
        assert dist.small_count == 0
        # No validation error because total is 0

    def test_to_dict(self):
        """Test serialization to dict."""
        dist = TradeDistribution(
            small_count=10,
            medium_count=5,
            large_count=2,
            small_pct=58.82,
            medium_pct=29.41,
            large_pct=11.77
        )
        
        d = dist.to_dict()
        
        assert d["small_count"] == 10
        assert d["medium_count"] == 5
        assert d["large_count"] == 2
        assert d["small_pct"] == 58.82
        assert d["medium_pct"] == 29.41
        assert d["large_pct"] == 11.77


class TestDurationAnalysis:
    """Tests for DurationAnalysis contract."""

    def test_valid_duration(self):
        """Test creating valid duration analysis."""
        duration = DurationAnalysis(
            avg_bars=5.5,
            median_bars=4,
            fast_exits_count=10,
            normal_exits_count=20,
            prolonged_exits_count=5,
            fast_exits_pct=28.57,
            insights=["Fast exits may indicate tight stops"]
        )
        
        assert duration.avg_bars == 5.5
        assert duration.median_bars == 4
        assert duration.fast_exits_count == 10
        assert duration.normal_exits_count == 20
        assert duration.prolonged_exits_count == 5
        assert duration.fast_exits_pct == 28.57
        assert len(duration.insights) == 1

    def test_negative_avg_bars_raises(self):
        """Test that negative avg_bars raises error."""
        with pytest.raises(ValueError, match="avg_bars cannot be negative"):
            DurationAnalysis(
                avg_bars=-1.0,
                median_bars=4,
                fast_exits_count=10,
                normal_exits_count=20,
                prolonged_exits_count=5,
                fast_exits_pct=28.57,
                insights=[]
            )

    def test_negative_median_bars_raises(self):
        """Test that negative median_bars raises error."""
        with pytest.raises(ValueError, match="median_bars cannot be negative"):
            DurationAnalysis(
                avg_bars=5.5,
                median_bars=-1,
                fast_exits_count=10,
                normal_exits_count=20,
                prolonged_exits_count=5,
                fast_exits_pct=28.57,
                insights=[]
            )

    @pytest.mark.parametrize("fast_pct", [-10, 110])
    def test_fast_exits_pct_out_of_range(self, fast_pct):
        """Test that fast_exits_pct outside 0-100 raises error."""
        with pytest.raises(ValueError, match="fast_exits_pct must be 0–100"):
            DurationAnalysis(
                avg_bars=5.5,
                median_bars=4,
                fast_exits_count=10,
                normal_exits_count=20,
                prolonged_exits_count=5,
                fast_exits_pct=fast_pct,
                insights=[]
            )

    def test_to_dict(self):
        """Test serialization to dict."""
        duration = DurationAnalysis(
            avg_bars=5.5,
            median_bars=4,
            fast_exits_count=10,
            normal_exits_count=20,
            prolonged_exits_count=5,
            fast_exits_pct=28.57,
            insights=["Test insight"]
        )
        
        d = duration.to_dict()
        
        assert d["avg_bars"] == 5.5
        assert d["median_bars"] == 4
        assert d["fast_exits_count"] == 10
        assert d["normal_exits_count"] == 20
        assert d["prolonged_exits_count"] == 5
        assert d["fast_exits_pct"] == 28.57
        assert d["insights"] == ["Test insight"]


class TestRiskAdjustedMetrics:
    """Tests for RiskAdjustedMetrics contract."""

    def test_valid_metrics(self):
        """Test creating valid risk-adjusted metrics."""
        metrics = RiskAdjustedMetrics(
            return_over_max_dd=5.2,
            avg_win_over_avg_loss=2.1,
            expectancy_per_trade=0.05,
            consistency_score=75.5,
            recovery_factor=3.0,
            insights=[]
        )
        
        assert metrics.return_over_max_dd == 5.2
        assert metrics.avg_win_over_avg_loss == 2.1
        assert metrics.expectancy_per_trade == 0.05
        assert metrics.consistency_score == 75.5
        assert metrics.recovery_factor == 3.0

    @pytest.mark.parametrize("score", [-10, 110])
    def test_consistency_score_range(self, score):
        """Test that consistency_score must be 0-100."""
        with pytest.raises(ValueError, match="consistency_score must be 0–100"):
            RiskAdjustedMetrics(
                return_over_max_dd=5.2,
                avg_win_over_avg_loss=2.1,
                expectancy_per_trade=0.05,
                consistency_score=score,
                recovery_factor=3.0,
                insights=[]
            )

    def test_to_dict(self):
        """Test serialization to dict."""
        metrics = RiskAdjustedMetrics(
            return_over_max_dd=5.2,
            avg_win_over_avg_loss=2.1,
            expectancy_per_trade=0.05123,
            consistency_score=75.5,
            recovery_factor=3.0,
            insights=[]
        )
        
        d = metrics.to_dict()
        
        assert d["return_over_max_dd"] == 5.2
        assert d["avg_win_over_avg_loss"] == 2.1
        assert d["expectancy_per_trade"] == 0.0512  # Rounded to 4 decimals
        assert d["consistency_score"] == 75.5
        assert d["recovery_factor"] == 3.0


class TestComparativeContext:
    """Tests for ComparativeContext contract."""

    def test_valid_context(self):
        """Test creating valid comparative context."""
        context = ComparativeContext(
            vs_baseline={"sharpe": 1.5, "sortino": 2.0},
            statistical_flags=["positive_skew", "low_correlation"],
            percentile_rank=85.5
        )
        
        assert context.vs_baseline == {"sharpe": 1.5, "sortino": 2.0}
        assert context.statistical_flags == ["positive_skew", "low_correlation"]
        assert context.percentile_rank == 85.5

    def test_optional_fields(self):
        """Test context with optional fields as None."""
        context = ComparativeContext(
            vs_baseline=None,
            statistical_flags=[],
            percentile_rank=None
        )
        
        assert context.vs_baseline is None
        assert context.statistical_flags == []
        assert context.percentile_rank is None

    def test_to_dict(self):
        """Test serialization to dict."""
        context = ComparativeContext(
            vs_baseline={"sharpe": 1.5},
            statistical_flags=["positive_skew"],
            percentile_rank=85.5
        )
        
        d = context.to_dict()
        
        assert d["vs_baseline"] == {"sharpe": 1.5}
        assert d["statistical_flags"] == ["positive_skew"]
        assert d["percentile_rank"] == 85.5


class TestExecutiveSummary:
    """Tests for ExecutiveSummary contract."""

    @pytest.fixture
    def sample_insights(self):
        """Sample insights for testing."""
        return [
            Insight(
                message="Critical insight",
                recommendation="Fix it",
                confidence="High",
                impact_estimate="+100 pts",
                category="risk",
                severity="critical"
            )
        ]

    @pytest.mark.parametrize("grade", ["A+", "A", "A-", "B+", "B", "B-",
                                       "C+", "C", "C-", "D+", "D", "D-", "F"])
    def test_valid_grades(self, grade, sample_insights):
        """Test all valid grade values."""
        summary = ExecutiveSummary(
            performance_grade=grade,
            grade_reasoning="Test reasoning",
            critical_insights=sample_insights,
            key_strengths=["Good win rate"],
            improvement_areas=["High drawdown"],
            overall_assessment="Strategy shows promise"
        )
        
        assert summary.performance_grade == grade

    def test_invalid_grade(self, sample_insights):
        """Test that invalid grade raises error."""
        with pytest.raises(ValueError, match="Invalid grade"):
            ExecutiveSummary(
                performance_grade="Z",
                grade_reasoning="Test reasoning",
                critical_insights=sample_insights,
                key_strengths=["Good"],
                improvement_areas=["Bad"],
                overall_assessment="Test"
            )

    def test_too_many_insights(self):
        """Test that >7 insights raises error."""
        insights = []
        for i in range(8):
            insights.append(create_empty_insight(message=f"Insight {i}"))
        
        with pytest.raises(ValueError, match="critical_insights must be ≤ 7"):
            ExecutiveSummary(
                performance_grade="A",
                grade_reasoning="Test",
                critical_insights=insights,
                key_strengths=[],
                improvement_areas=[],
                overall_assessment="Test"
            )

    def test_to_dict(self, sample_insights):
        """Test serialization to dict."""
        summary = ExecutiveSummary(
            performance_grade="A-",
            grade_reasoning="Strong performance with minor issues",
            critical_insights=sample_insights,
            key_strengths=["Good win rate", "Strong profit factor"],
            improvement_areas=["High drawdown"],
            overall_assessment="Strategy shows good potential"
        )
        
        d = summary.to_dict()
        
        assert d["performance_grade"] == "A-"
        assert d["grade_reasoning"] == "Strong performance with minor issues"
        assert len(d["critical_insights"]) == 1
        assert d["key_strengths"] == ["Good win rate", "Strong profit factor"]
        assert d["improvement_areas"] == ["High drawdown"]
        assert d["overall_assessment"] == "Strategy shows good potential"


class TestAnalyticsReport:
    """Tests for AnalyticsReport contract."""

    @pytest.fixture
    def mock_metrics_report(self):
        """Mock MetricsReport for testing."""
        class MockMetricsReport:
            def to_dict(self):
                return {"total_trades": 100, "win_rate": 60.0}
        
        return MockMetricsReport()

    @pytest.fixture
    def sample_insights(self):
        """Sample insights for testing."""
        return [
            Insight(
                message="Test insight",
                recommendation="Test",
                confidence="High",
                impact_estimate=None,
                category="time",
                severity="info"
            ),
            Insight(
                message="Critical insight",
                recommendation="Fix it",
                confidence="High",
                impact_estimate="+50",
                category="risk",
                severity="critical"
            )
        ]

    @pytest.fixture
    def sample_executive_summary(self, sample_insights):
        """Sample executive summary for testing."""
        return ExecutiveSummary(
            performance_grade="B+",
            grade_reasoning="Good performance",
            critical_insights=[sample_insights[1]],  # Only critical
            key_strengths=["Good"],
            improvement_areas=["Bad"],
            overall_assessment="Test"
        )

    @pytest.fixture
    def sample_session_metrics(self):
        """Sample session metrics for testing."""
        return SessionMetrics(
            session_name="London",
            trades=50,
            winning_trades=30,
            win_rate=60.0,
            total_pnl=500.0,
            avg_pnl=10.0,
            largest_win=20.0,
            largest_loss=-5.0
        )

    @pytest.fixture
    def sample_time_performance(self, sample_session_metrics, sample_insights):
        """Sample time performance for testing."""
        return TimePerformanceBreakdown(
            by_session={"London": sample_session_metrics},
            by_hour={10: sample_session_metrics},
            by_day={"Monday": sample_session_metrics},
            best_session="London",
            worst_session="London",
            insights=[sample_insights[0]]
        )

    @pytest.fixture
    def sample_trade_quality(self):
        """Sample trade quality for testing."""
        win_dist = TradeDistribution(10, 5, 2, 58.82, 29.41, 11.77)
        loss_dist = TradeDistribution(8, 4, 1, 61.54, 30.77, 7.69)
        duration = DurationAnalysis(5.5, 4, 10, 20, 5, 28.57, [])
        
        return TradeQualityAnalysis(
            win_distribution=win_dist,
            loss_distribution=loss_dist,
            duration_analysis=duration,
            avg_bars_to_profit=4.0,
            avg_bars_to_loss=6.0,
            premature_exit_estimate="Reasonable timing",
            insights=[]
        )

    @pytest.fixture
    def sample_risk_metrics(self):
        """Sample risk metrics for testing."""
        return RiskAdjustedMetrics(
            return_over_max_dd=5.2,
            avg_win_over_avg_loss=2.1,
            expectancy_per_trade=0.05,
            consistency_score=75.5,
            recovery_factor=3.0,
            insights=[]
        )

    def test_valid_report(self, sample_executive_summary, sample_time_performance,
                         sample_trade_quality, sample_risk_metrics,
                         mock_metrics_report):
        """Test creating valid analytics report."""
        report = AnalyticsReport(
            executive_summary=sample_executive_summary,
            time_performance=sample_time_performance,
            trade_quality=sample_trade_quality,
            risk_adjusted=sample_risk_metrics,
            comparative=None,
            input_metrics=mock_metrics_report,
            analysis_timestamp="2025-01-01T12:00:00",
            analysis_duration_ms=250.5
        )
        
        assert report.executive_summary == sample_executive_summary
        assert report.time_performance == sample_time_performance
        assert report.trade_quality == sample_trade_quality
        assert report.risk_adjusted == sample_risk_metrics
        assert report.comparative is None
        assert report.analysis_timestamp == "2025-01-01T12:00:00"
        assert report.analysis_duration_ms == 250.5

    def test_to_dict(self, sample_executive_summary, sample_time_performance,
                    sample_trade_quality, sample_risk_metrics,
                    mock_metrics_report):
        """Test serialization to dict."""
        report = AnalyticsReport(
            executive_summary=sample_executive_summary,
            time_performance=sample_time_performance,
            trade_quality=sample_trade_quality,
            risk_adjusted=sample_risk_metrics,
            comparative=None,
            input_metrics=mock_metrics_report,
            analysis_timestamp="2025-01-01T12:00:00",
            analysis_duration_ms=250.5
        )
        
        d = report.to_dict()
        
        assert "executive_summary" in d
        assert "time_performance" in d
        assert "trade_quality" in d
        assert "risk_adjusted" in d
        assert "comparative" in d
        assert "input_metrics" in d
        assert "metadata" in d

    def test_to_json(self, sample_executive_summary, sample_time_performance,
                    sample_trade_quality, sample_risk_metrics,
                    mock_metrics_report):
        """Test JSON serialization."""
        report = AnalyticsReport(
            executive_summary=sample_executive_summary,
            time_performance=sample_time_performance,
            trade_quality=sample_trade_quality,
            risk_adjusted=sample_risk_metrics,
            comparative=None,
            input_metrics=mock_metrics_report,
            analysis_timestamp="2025-01-01T12:00:00",
            analysis_duration_ms=250.5
        )
        
        json_str = report.to_json()
        parsed = json.loads(json_str)
        
        assert "executive_summary" in parsed
        assert "metadata" in parsed

    def test_get_all_insights(self, sample_executive_summary, sample_time_performance,
                              sample_trade_quality, sample_risk_metrics,
                              mock_metrics_report, sample_insights):
        """Test getting all insights."""
        report = AnalyticsReport(
            executive_summary=sample_executive_summary,
            time_performance=sample_time_performance,
            trade_quality=sample_trade_quality,
            risk_adjusted=sample_risk_metrics,
            comparative=None,
            input_metrics=mock_metrics_report,
            analysis_timestamp="2025-01-01T12:00:00",
            analysis_duration_ms=250.5
        )
        
        all_insights = report.get_all_insights()
        
        # Should include insights from all sections
        assert len(all_insights) >= 2  # At least the ones we added

    def test_get_critical_insights_only(self, sample_executive_summary,
                                         sample_time_performance,
                                         sample_trade_quality,
                                         sample_risk_metrics,
                                         mock_metrics_report):
        """Test getting only critical insights."""
        report = AnalyticsReport(
            executive_summary=sample_executive_summary,
            time_performance=sample_time_performance,
            trade_quality=sample_trade_quality,
            risk_adjusted=sample_risk_metrics,
            comparative=None,
            input_metrics=mock_metrics_report,
            analysis_timestamp="2025-01-01T12:00:00",
            analysis_duration_ms=250.5
        )
        
        critical = report.get_critical_insights_only()
        
        # Should include the critical insight from executive summary
        assert len(critical) >= 1
        for insight in critical:
            assert insight.severity == "critical"

    def test_get_insights_by_category(self, sample_executive_summary,
                                       sample_time_performance,
                                       sample_trade_quality,
                                       sample_risk_metrics,
                                       mock_metrics_report):
        """Test filtering insights by category."""
        report = AnalyticsReport(
            executive_summary=sample_executive_summary,
            time_performance=sample_time_performance,
            trade_quality=sample_trade_quality,
            risk_adjusted=sample_risk_metrics,
            comparative=None,
            input_metrics=mock_metrics_report,
            analysis_timestamp="2025-01-01T12:00:00",
            analysis_duration_ms=250.5
        )
        
        time_insights = report.get_insights_by_category("time")
        risk_insights = report.get_insights_by_category("risk")
        
        assert len(time_insights) >= 1
        assert len(risk_insights) >= 1


class TestFactoryFunctions:
    """Tests for factory functions."""

    def test_create_empty_insight(self):
        """Test creating empty insight."""
        insight = create_empty_insight()
        
        assert insight.message == "No insight"
        assert insight.recommendation == "No recommendation"
        assert insight.confidence == "Low"
        assert insight.impact_estimate is None
        assert insight.category == "general"
        assert insight.severity == "info"

    def test_create_empty_insight_custom(self):
        """Test creating empty insight with custom values."""
        insight = create_empty_insight(
            message="Custom message",
            recommendation="Custom rec",
            confidence="High",
            category="risk",
            severity="warning"
        )
        
        assert insight.message == "Custom message"
        assert insight.recommendation == "Custom rec"
        assert insight.confidence == "High"
        assert insight.category == "risk"
        assert insight.severity == "warning"

    def test_create_empty_session_metrics(self):
        """Test creating empty session metrics."""
        metrics = create_empty_session_metrics("Test Session")
        
        assert metrics.session_name == "Test Session"
        assert metrics.trades == 0
        assert metrics.winning_trades == 0
        assert metrics.win_rate == 0.0
        assert metrics.total_pnl == 0.0
        assert metrics.avg_pnl == 0.0
        assert metrics.largest_win == 0.0
        assert metrics.largest_loss == 0.0

    def test_create_empty_session_metrics_default(self):
        """Test creating empty session metrics with default name."""
        metrics = create_empty_session_metrics()
        
        assert metrics.session_name == "Unknown"