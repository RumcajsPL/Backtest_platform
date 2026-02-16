"""
Test Suite for Analytics Contracts

Validates all analytics dataclasses and their behaviors.
Tests contract creation, validation, serialization, and edge cases.

Created: 2026-02-16 (Session 14)
"""

import sys
from pathlib import Path
import pytest
import json

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.strategies.contracts.analytics_contracts import (
    # Configuration
    TradingSessionConfig,
    # Core contracts
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
    # Factory functions
    create_empty_insight,
    create_empty_session_metrics
)


# ============================================================
# CONFIGURATION TESTS
# ============================================================

class TestTradingSessionConfig:
    """Test TradingSessionConfig contract"""
    
    def test_default_sessions(self):
        """Test default session configuration"""
        config = TradingSessionConfig()
        
        assert "Asia" in config.sessions
        assert "London" in config.sessions
        assert "NY" in config.sessions
        
        assert config.sessions["Asia"] == (0, 8)
        assert config.sessions["London"] == (8, 16)
        assert config.sessions["NY"] == (16, 24)
    
    def test_custom_sessions(self):
        """Test custom session configuration"""
        custom = TradingSessionConfig(
            sessions={"Morning": (8, 12), "Afternoon": (12, 16)}
        )
        
        assert len(custom.sessions) == 2
        assert custom.sessions["Morning"] == (8, 12)
    
    def test_invalid_hour_range(self):
        """Test validation of invalid hour ranges"""
        with pytest.raises(ValueError, match="invalid hours"):
            TradingSessionConfig(sessions={"Bad": (-1, 8)})
        
        with pytest.raises(ValueError, match="invalid hours"):
            TradingSessionConfig(sessions={"Bad": (0, 25)})
    
    def test_start_after_end(self):
        """Test validation when start >= end"""
        with pytest.raises(ValueError, match="start must be before end"):
            TradingSessionConfig(sessions={"Bad": (16, 8)})
    
    def test_empty_sessions(self):
        """Test validation of empty sessions"""
        with pytest.raises(ValueError, match="At least one session"):
            TradingSessionConfig(sessions={})


# ============================================================
# INSIGHT TESTS
# ============================================================

class TestInsight:
    """Test Insight contract"""
    
    def test_valid_insight(self):
        """Test creating valid insight"""
        insight = Insight(
            message="Asia session losing -45pts",
            recommendation="Consider excluding Asia session",
            confidence="High",
            impact_estimate="Potential +45pts improvement",
            category="time",
            severity="critical"
        )
        
        assert insight.message == "Asia session losing -45pts"
        assert insight.confidence == "High"
        assert insight.severity == "critical"
    
    def test_invalid_confidence(self):
        """Test validation of confidence levels"""
        with pytest.raises(ValueError, match="Confidence must be"):
            Insight(
                message="Test",
                recommendation="Test",
                confidence="VeryHigh",  # Invalid
                impact_estimate=None,
                category="time",
                severity="info"
            )
    
    def test_invalid_category(self):
        """Test validation of categories"""
        with pytest.raises(ValueError, match="Category must be"):
            Insight(
                message="Test",
                recommendation="Test",
                confidence="High",
                impact_estimate=None,
                category="invalid",  # Invalid
                severity="info"
            )
    
    def test_invalid_severity(self):
        """Test validation of severity levels"""
        with pytest.raises(ValueError, match="Severity must be"):
            Insight(
                message="Test",
                recommendation="Test",
                confidence="High",
                impact_estimate=None,
                category="time",
                severity="urgent"  # Invalid
            )
    
    def test_insight_serialization(self):
        """Test insight to_dict() method"""
        insight = create_empty_insight(
            message="Test message",
            recommendation="Test action",
            confidence="Medium"
        )
        
        data = insight.to_dict()
        
        assert data["message"] == "Test message"
        assert data["confidence"] == "Medium"
        assert "recommendation" in data


# ============================================================
# SESSION METRICS TESTS
# ============================================================

class TestSessionMetrics:
    """Test SessionMetrics contract"""
    
    def test_valid_session_metrics(self):
        """Test creating valid session metrics"""
        metrics = SessionMetrics(
            session_name="London",
            trades=100,
            winning_trades=20,
            win_rate=20.0,
            total_pnl=50.5,
            avg_pnl=0.505,
            largest_win=10.0,
            largest_loss=-5.0
        )
        
        assert metrics.session_name == "London"
        assert metrics.trades == 100
        assert metrics.win_rate == 20.0
    
    def test_negative_trades(self):
        """Test validation of negative trades"""
        with pytest.raises(ValueError, match="cannot be negative"):
            SessionMetrics(
                session_name="Test",
                trades=-1,
                winning_trades=0,
                win_rate=0.0,
                total_pnl=0.0,
                avg_pnl=0.0,
                largest_win=0.0,
                largest_loss=0.0
            )
    
    def test_winning_trades_exceeds_total(self):
        """Test validation when winning > total trades"""
        with pytest.raises(ValueError, match="must be 0-"):
            SessionMetrics(
                session_name="Test",
                trades=10,
                winning_trades=15,  # More than total
                win_rate=150.0,
                total_pnl=0.0,
                avg_pnl=0.0,
                largest_win=0.0,
                largest_loss=0.0
            )
    
    def test_win_rate_out_of_range(self):
        """Test validation of win rate percentage"""
        with pytest.raises(ValueError, match="must be 0-100"):
            SessionMetrics(
                session_name="Test",
                trades=10,
                winning_trades=5,
                win_rate=150.0,  # Invalid
                total_pnl=0.0,
                avg_pnl=0.0,
                largest_win=0.0,
                largest_loss=0.0
            )
    
    def test_session_metrics_serialization(self):
        """Test session metrics to_dict() method"""
        metrics = create_empty_session_metrics("Test")
        data = metrics.to_dict()
        
        assert data["session_name"] == "Test"
        assert data["trades"] == 0
        assert "win_rate" in data


# ============================================================
# TRADE DISTRIBUTION TESTS
# ============================================================

class TestTradeDistribution:
    """Test TradeDistribution contract"""
    
    def test_valid_distribution(self):
        """Test creating valid distribution"""
        dist = TradeDistribution(
            small_count=50,
            medium_count=30,
            large_count=20,
            small_pct=50.0,
            medium_pct=30.0,
            large_pct=20.0
        )
        
        assert dist.small_count == 50
        assert dist.large_pct == 20.0
    
    def test_percentage_validation(self):
        """Test validation of percentage sum"""
        with pytest.raises(ValueError, match="must sum to 100"):
            TradeDistribution(
                small_count=50,
                medium_count=30,
                large_count=20,
                small_pct=50.0,
                medium_pct=30.0,
                large_pct=30.0  # Sum = 110%
            )
    
    def test_empty_distribution(self):
        """Test empty distribution (no trades)"""
        dist = TradeDistribution(
            small_count=0,
            medium_count=0,
            large_count=0,
            small_pct=0.0,
            medium_pct=0.0,
            large_pct=0.0
        )
        
        assert dist.small_count == 0
        # Should not raise error for empty distribution


# ============================================================
# DURATION ANALYSIS TESTS
# ============================================================

class TestDurationAnalysis:
    """Test DurationAnalysis contract"""
    
    def test_valid_duration_analysis(self):
        """Test creating valid duration analysis"""
        duration = DurationAnalysis(
            avg_bars=5.2,
            median_bars=4,
            fast_exits_count=30,
            normal_exits_count=50,
            prolonged_exits_count=20,
            fast_exits_pct=30.0,
            insights=["Fast exits dominate"]
        )
        
        assert duration.avg_bars == 5.2
        assert duration.median_bars == 4
        assert len(duration.insights) == 1
    
    def test_negative_bars(self):
        """Test validation of negative bar counts"""
        with pytest.raises(ValueError, match="cannot be negative"):
            DurationAnalysis(
                avg_bars=-1.0,
                median_bars=0,
                fast_exits_count=0,
                normal_exits_count=0,
                prolonged_exits_count=0,
                fast_exits_pct=0.0,
                insights=[]
            )
    
    def test_invalid_percentage(self):
        """Test validation of fast exits percentage"""
        with pytest.raises(ValueError, match="must be 0-100"):
            DurationAnalysis(
                avg_bars=5.0,
                median_bars=5,
                fast_exits_count=0,
                normal_exits_count=0,
                prolonged_exits_count=0,
                fast_exits_pct=150.0,  # Invalid
                insights=[]
            )


# ============================================================
# RISK-ADJUSTED METRICS TESTS
# ============================================================

class TestRiskAdjustedMetrics:
    """Test RiskAdjustedMetrics contract"""
    
    def test_valid_risk_metrics(self):
        """Test creating valid risk-adjusted metrics"""
        risk = RiskAdjustedMetrics(
            return_over_max_dd=5.0,
            avg_win_over_avg_loss=2.5,
            expectancy_per_trade=0.25,
            consistency_score=75.0,
            recovery_factor=3.0,
            insights=[]
        )
        
        assert risk.return_over_max_dd == 5.0
        assert risk.consistency_score == 75.0
    
    def test_consistency_score_validation(self):
        """Test validation of consistency score range"""
        with pytest.raises(ValueError, match="must be 0-100"):
            RiskAdjustedMetrics(
                return_over_max_dd=5.0,
                avg_win_over_avg_loss=2.5,
                expectancy_per_trade=0.25,
                consistency_score=150.0,  # Invalid
                recovery_factor=3.0,
                insights=[]
            )


# ============================================================
# EXECUTIVE SUMMARY TESTS
# ============================================================

class TestExecutiveSummary:
    """Test ExecutiveSummary contract"""
    
    def test_valid_executive_summary(self):
        """Test creating valid executive summary"""
        summary = ExecutiveSummary(
            performance_grade="B+",
            grade_reasoning="Good performance with optimization potential",
            critical_insights=[
                create_empty_insight(message="Test 1"),
                create_empty_insight(message="Test 2")
            ],
            key_strengths=["Strong London session", "Good risk management"],
            improvement_areas=["Asia session drag", "Premature exits"],
            overall_assessment="Solid strategy with clear improvement paths"
        )
        
        assert summary.performance_grade == "B+"
        assert len(summary.critical_insights) == 2
        assert len(summary.key_strengths) == 2
    
    def test_invalid_grade(self):
        """Test validation of performance grade"""
        with pytest.raises(ValueError, match="Invalid grade"):
            ExecutiveSummary(
                performance_grade="Z",  # Invalid
                grade_reasoning="Test",
                critical_insights=[],
                key_strengths=[],
                improvement_areas=[],
                overall_assessment="Test"
            )
    
    def test_too_many_critical_insights(self):
        """Test validation of critical insights count"""
        with pytest.raises(ValueError, match="Too many critical insights"):
            ExecutiveSummary(
                performance_grade="B",
                grade_reasoning="Test",
                critical_insights=[create_empty_insight() for _ in range(10)],  # Too many
                key_strengths=[],
                improvement_areas=[],
                overall_assessment="Test"
            )
    
    def test_valid_grades(self):
        """Test all valid grade values"""
        valid_grades = ["A+", "A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D+", "D", "D-", "F"]
        
        for grade in valid_grades:
            summary = ExecutiveSummary(
                performance_grade=grade,
                grade_reasoning="Test",
                critical_insights=[],
                key_strengths=[],
                improvement_areas=[],
                overall_assessment="Test"
            )
            assert summary.performance_grade == grade


# ============================================================
# ANALYTICS REPORT TESTS
# ============================================================

class TestAnalyticsReport:
    """Test AnalyticsReport contract"""
    
    @pytest.fixture
    def mock_metrics_report(self):
        """Mock MetricsReport for testing"""
        class MockMetricsReport:
            def __init__(self):
                self.total_trades = 100
                self.win_rate = 20.0
                self.total_pnl_points = 50.0
            
            def to_dict(self):
                return {"total_trades": self.total_trades}
        
        return MockMetricsReport()
    
    def test_valid_analytics_report(self, mock_metrics_report):
        """Test creating valid analytics report"""
        # Create minimal valid components
        exec_summary = ExecutiveSummary(
            performance_grade="B",
            grade_reasoning="Test",
            critical_insights=[],
            key_strengths=[],
            improvement_areas=[],
            overall_assessment="Test"
        )
        
        time_perf = TimePerformanceBreakdown(
            by_session={},
            by_hour={},
            by_day={},
            best_session="Unknown",
            worst_session="Unknown",
            insights=[]
        )
        
        empty_dist = TradeDistribution(
            small_count=0, medium_count=0, large_count=0,
            small_pct=0.0, medium_pct=0.0, large_pct=0.0
        )
        empty_duration = DurationAnalysis(
            avg_bars=0.0, median_bars=0,
            fast_exits_count=0, normal_exits_count=0, prolonged_exits_count=0,
            fast_exits_pct=0.0, insights=[]
        )
        
        quality = TradeQualityAnalysis(
            win_distribution=empty_dist,
            loss_distribution=empty_dist,
            duration_analysis=empty_duration,
            avg_bars_to_profit=None,
            avg_bars_to_loss=None,
            premature_exit_estimate="Unknown",
            insights=[]
        )
        
        risk = RiskAdjustedMetrics(
            return_over_max_dd=0.0,
            avg_win_over_avg_loss=0.0,
            expectancy_per_trade=0.0,
            consistency_score=50.0,
            recovery_factor=0.0,
            insights=[]
        )
        
        comparative = ComparativeContext(
            vs_baseline=None,
            statistical_flags=[],
            percentile_rank=None
        )
        
        # Create report
        report = AnalyticsReport(
            executive_summary=exec_summary,
            time_performance=time_perf,
            trade_quality=quality,
            risk_adjusted=risk,
            comparative=comparative,
            input_metrics=mock_metrics_report,
            analysis_timestamp="2026-02-16T12:00:00",
            analysis_duration_ms=150.5
        )
        
        assert report.executive_summary.performance_grade == "B"
        assert report.analysis_duration_ms == 150.5
    
    def test_report_serialization(self, mock_metrics_report):
        """Test report to_dict() and to_json() methods"""
        # Use same minimal report structure as above
        exec_summary = ExecutiveSummary(
            performance_grade="B",
            grade_reasoning="Test",
            critical_insights=[],
            key_strengths=[],
            improvement_areas=[],
            overall_assessment="Test"
        )
        
        time_perf = TimePerformanceBreakdown(
            by_session={},
            by_hour={},
            by_day={},
            best_session="Unknown",
            worst_session="Unknown",
            insights=[]
        )
        
        empty_dist = TradeDistribution(
            small_count=0, medium_count=0, large_count=0,
            small_pct=0.0, medium_pct=0.0, large_pct=0.0
        )
        empty_duration = DurationAnalysis(
            avg_bars=0.0, median_bars=0,
            fast_exits_count=0, normal_exits_count=0, prolonged_exits_count=0,
            fast_exits_pct=0.0, insights=[]
        )
        
        quality = TradeQualityAnalysis(
            win_distribution=empty_dist,
            loss_distribution=empty_dist,
            duration_analysis=empty_duration,
            avg_bars_to_profit=None,
            avg_bars_to_loss=None,
            premature_exit_estimate="Unknown",
            insights=[]
        )
        
        risk = RiskAdjustedMetrics(
            return_over_max_dd=0.0,
            avg_win_over_avg_loss=0.0,
            expectancy_per_trade=0.0,
            consistency_score=50.0,
            recovery_factor=0.0,
            insights=[]
        )
        
        comparative = ComparativeContext(
            vs_baseline=None,
            statistical_flags=[],
            percentile_rank=None
        )
        
        report = AnalyticsReport(
            executive_summary=exec_summary,
            time_performance=time_perf,
            trade_quality=quality,
            risk_adjusted=risk,
            comparative=comparative,
            input_metrics=mock_metrics_report,
            analysis_timestamp="2026-02-16T12:00:00",
            analysis_duration_ms=150.5
        )
        
        # Test to_dict()
        data = report.to_dict()
        assert "executive_summary" in data
        assert "time_performance" in data
        assert "metadata" in data
        assert data["metadata"]["analysis_duration_ms"] == 150.5
        
        # Test to_json()
        json_str = report.to_json()
        parsed = json.loads(json_str)
        assert parsed["metadata"]["analysis_duration_ms"] == 150.5
    
    def test_get_all_insights(self, mock_metrics_report):
        """Test collecting all insights from report"""
        insight1 = create_empty_insight(message="Insight 1")
        insight2 = create_empty_insight(message="Insight 2")
        insight3 = create_empty_insight(message="Insight 3")
        
        exec_summary = ExecutiveSummary(
            performance_grade="B",
            grade_reasoning="Test",
            critical_insights=[insight1],
            key_strengths=[],
            improvement_areas=[],
            overall_assessment="Test"
        )
        
        time_perf = TimePerformanceBreakdown(
            by_session={},
            by_hour={},
            by_day={},
            best_session="Unknown",
            worst_session="Unknown",
            insights=[insight2]
        )
        
        empty_dist = TradeDistribution(
            small_count=0, medium_count=0, large_count=0,
            small_pct=0.0, medium_pct=0.0, large_pct=0.0
        )
        empty_duration = DurationAnalysis(
            avg_bars=0.0, median_bars=0,
            fast_exits_count=0, normal_exits_count=0, prolonged_exits_count=0,
            fast_exits_pct=0.0, insights=[]
        )
        
        quality = TradeQualityAnalysis(
            win_distribution=empty_dist,
            loss_distribution=empty_dist,
            duration_analysis=empty_duration,
            avg_bars_to_profit=None,
            avg_bars_to_loss=None,
            premature_exit_estimate="Unknown",
            insights=[insight3]
        )
        
        risk = RiskAdjustedMetrics(
            return_over_max_dd=0.0,
            avg_win_over_avg_loss=0.0,
            expectancy_per_trade=0.0,
            consistency_score=50.0,
            recovery_factor=0.0,
            insights=[]
        )
        
        comparative = ComparativeContext(
            vs_baseline=None,
            statistical_flags=[],
            percentile_rank=None
        )
        
        report = AnalyticsReport(
            executive_summary=exec_summary,
            time_performance=time_perf,
            trade_quality=quality,
            risk_adjusted=risk,
            comparative=comparative,
            input_metrics=mock_metrics_report,
            analysis_timestamp="2026-02-16T12:00:00",
            analysis_duration_ms=150.5
        )
        
        all_insights = report.get_all_insights()
        assert len(all_insights) == 3
        assert insight1 in all_insights
        assert insight2 in all_insights
        assert insight3 in all_insights


# ============================================================
# FACTORY FUNCTION TESTS
# ============================================================

class TestFactoryFunctions:
    """Test factory helper functions"""
    
    def test_create_empty_insight(self):
        """Test empty insight creation"""
        insight = create_empty_insight()
        
        assert insight.message == "No insight"
        assert insight.confidence == "Low"
        assert insight.severity == "info"
    
    def test_create_empty_insight_with_params(self):
        """Test empty insight with custom parameters"""
        insight = create_empty_insight(
            message="Custom message",
            confidence="High",
            severity="critical"
        )
        
        assert insight.message == "Custom message"
        assert insight.confidence == "High"
        assert insight.severity == "critical"
    
    def test_create_empty_session_metrics(self):
        """Test empty session metrics creation"""
        metrics = create_empty_session_metrics("TestSession")
        
        assert metrics.session_name == "TestSession"
        assert metrics.trades == 0
        assert metrics.win_rate == 0.0


# ============================================================
# INTEGRATION TESTS
# ============================================================

class TestContractIntegration:
    """Test contracts working together"""
    
    def test_nested_serialization(self):
        """Test serialization of nested structures"""
        insight = create_empty_insight(message="Test")
        session = create_empty_session_metrics("Test")
        
        # Create nested structure
        time_perf = TimePerformanceBreakdown(
            by_session={"Test": session},
            by_hour={},
            by_day={},
            best_session="Test",
            worst_session="Test",
            insights=[insight]
        )
        
        # Serialize
        data = time_perf.to_dict()
        
        # Verify nested structure preserved
        assert "by_session" in data
        assert "Test" in data["by_session"]
        assert data["by_session"]["Test"]["session_name"] == "Test"
        assert len(data["insights"]) == 1
        assert data["insights"][0]["message"] == "Test"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])