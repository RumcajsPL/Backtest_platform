"""
Analytics Contracts for WBWSStrategy Migration Project

Comprehensive analytics framework for intelligent trade performance analysis.
Provides actionable insights, time-based breakdowns, quality metrics, and risk-adjusted performance.

Created: 2026-02-16 (Session 14)
Philosophy: Intelligence over speed, insights over raw data
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, TYPE_CHECKING
from datetime import datetime
import json

# Import MetricsReport for type reference
if TYPE_CHECKING:
    from src.strategies.contracts.metrics_contracts import MetricsReport


# ============================================================
# CONFIGURATION CONTRACTS
# ============================================================

@dataclass
class TradingSessionConfig:
    """
    Configuration for trading session definitions
    
    Allows customization of time segments for performance analysis.
    Default: Standard forex sessions (Asia/London/NY in UTC)
    """
    sessions: Dict[str, tuple[int, int]] = field(default_factory=lambda: {
        "Asia": (0, 8),      # 00:00 - 08:00 UTC
        "London": (8, 16),   # 08:00 - 16:00 UTC  
        "NY": (16, 24)       # 16:00 - 24:00 UTC
    })
    
    def __post_init__(self):
        """Validate session configuration"""
        if not self.sessions:
            raise ValueError("At least one session must be defined")
        
        for name, (start, end) in self.sessions.items():
            if not (0 <= start < 24 and 0 <= end <= 24):
                raise ValueError(f"Session '{name}' has invalid hours: {start}-{end}")
            if start >= end:
                raise ValueError(f"Session '{name}' start must be before end: {start}-{end}")


# ============================================================
# INSIGHT CONTRACT
# ============================================================

@dataclass(frozen=True)
class Insight:
    """
    Single actionable insight with confidence and impact assessment
    
    Core building block for AI-like recommendations throughout analytics.
    Each insight includes:
    - Clear message (what was observed)
    - Actionable recommendation (what to do about it)
    - Confidence level (how sure we are)
    - Impact estimate (expected benefit if acted upon)
    """
    message: str                        # Observation: "Asia session losing -45pts"
    recommendation: str                 # Action: "Consider excluding Asia session"
    confidence: str                     # "High" | "Medium" | "Low"
    impact_estimate: Optional[str]      # "Potential +45pts improvement"
    category: str                       # "time" | "quality" | "risk" | "general"
    severity: str                       # "critical" | "warning" | "info" | "success"
    
    def __post_init__(self):
        """Validate insight fields"""
        # Validate confidence
        valid_confidence = {"High", "Medium", "Low"}
        if self.confidence not in valid_confidence:
            raise ValueError(f"Confidence must be one of {valid_confidence}, got '{self.confidence}'")
        
        # Validate category
        valid_categories = {"time", "quality", "risk", "general"}
        if self.category not in valid_categories:
            raise ValueError(f"Category must be one of {valid_categories}, got '{self.category}'")
        
        # Validate severity
        valid_severities = {"critical", "warning", "info", "success"}
        if self.severity not in valid_severities:
            raise ValueError(f"Severity must be one of {valid_severities}, got '{self.severity}'")
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "message": self.message,
            "recommendation": self.recommendation,
            "confidence": self.confidence,
            "impact_estimate": self.impact_estimate,
            "category": self.category,
            "severity": self.severity
        }


# ============================================================
# TIME PERFORMANCE CONTRACTS
# ============================================================

@dataclass(frozen=True)
class SessionMetrics:
    """
    Performance metrics for a specific time segment
    
    Used for sessions, hours, or days - any time-based grouping.
    Provides complete P&L picture for the segment.
    """
    session_name: str                   # "Asia" | "Monday" | "14:00"
    trades: int                         # Total trades in segment
    winning_trades: int                 # Number of wins
    win_rate: float                     # Win rate percentage
    total_pnl: float                    # Total P&L in points
    avg_pnl: float                      # Average P&L per trade
    largest_win: float                  # Best single trade
    largest_loss: float                 # Worst single trade
    
    def __post_init__(self):
        """Validate session metrics"""
        if self.trades < 0:
            raise ValueError(f"Trades cannot be negative: {self.trades}")
        if self.winning_trades < 0 or self.winning_trades > self.trades:
            raise ValueError(f"Winning trades ({self.winning_trades}) must be 0-{self.trades}")
        if not (0 <= self.win_rate <= 100):
            raise ValueError(f"Win rate must be 0-100%: {self.win_rate}")
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "session_name": self.session_name,
            "trades": self.trades,
            "winning_trades": self.winning_trades,
            "win_rate": round(self.win_rate, 2),
            "total_pnl": round(self.total_pnl, 2),
            "avg_pnl": round(self.avg_pnl, 2),
            "largest_win": round(self.largest_win, 2),
            "largest_loss": round(self.largest_loss, 2)
        }


@dataclass(frozen=True)
class TimePerformanceBreakdown:
    """
    Comprehensive time-based performance analysis
    
    Analyzes performance across multiple time dimensions:
    - By session (Asia/London/NY)
    - By hour of day (0-23)
    - By day of week (Mon-Sun)
    
    Identifies best/worst performing time segments with insights.
    """
    by_session: Dict[str, SessionMetrics]       # Performance by trading session
    by_hour: Dict[int, SessionMetrics]          # Performance by hour (0-23)
    by_day: Dict[str, SessionMetrics]           # Performance by weekday
    best_session: str                           # Name of best performing session
    worst_session: str                          # Name of worst performing session
    insights: List[Insight]                     # Time-related insights
    
    def __post_init__(self):
        """Validate time performance breakdown"""
        # Validate hour keys
        for hour in self.by_hour.keys():
            if not (0 <= hour <= 23):
                raise ValueError(f"Hour must be 0-23: {hour}")
        
        # Validate day keys
        valid_days = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"}
        for day in self.by_day.keys():
            if day not in valid_days:
                raise ValueError(f"Invalid day: {day}")
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "by_session": {k: v.to_dict() for k, v in self.by_session.items()},
            "by_hour": {str(k): v.to_dict() for k, v in self.by_hour.items()},
            "by_day": {k: v.to_dict() for k, v in self.by_day.items()},
            "best_session": self.best_session,
            "worst_session": self.worst_session,
            "insights": [i.to_dict() for i in self.insights]
        }


# ============================================================
# TRADE QUALITY CONTRACTS
# ============================================================

@dataclass(frozen=True)
class TradeDistribution:
    """
    Distribution analysis for trade sizes (wins or losses)
    
    Categorizes trades by size:
    - Small: < 3 points
    - Medium: 3-7 points
    - Large: > 7 points
    
    Helps identify if strategy relies on rare large winners.
    """
    small_count: int                    # Trades < 3 points
    medium_count: int                   # Trades 3-7 points
    large_count: int                    # Trades > 7 points
    small_pct: float                    # Percentage small
    medium_pct: float                   # Percentage medium
    large_pct: float                    # Percentage large
    
    def __post_init__(self):
        """Validate distribution"""
        total = self.small_count + self.medium_count + self.large_count
        if total > 0:
            calculated_pct = self.small_pct + self.medium_pct + self.large_pct
            if not (99.9 <= calculated_pct <= 100.1):  # Allow rounding error
                raise ValueError(f"Percentages must sum to 100: {calculated_pct}")
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "small_count": self.small_count,
            "medium_count": self.medium_count,
            "large_count": self.large_count,
            "small_pct": round(self.small_pct, 2),
            "medium_pct": round(self.medium_pct, 2),
            "large_pct": round(self.large_pct, 2)
        }


@dataclass(frozen=True)
class DurationAnalysis:
    """
    Trade duration pattern analysis
    
    Analyzes how long trades stay open:
    - Fast exits: < 3 bars (potential premature exits)
    - Normal exits: 3-10 bars
    - Prolonged exits: > 10 bars
    
    Helps identify if stops are too tight or too loose.
    """
    avg_bars: float                     # Average duration in bars
    median_bars: int                    # Median duration
    fast_exits_count: int               # < 3 bars
    normal_exits_count: int             # 3-10 bars
    prolonged_exits_count: int          # > 10 bars
    fast_exits_pct: float               # % fast exits
    insights: List[str]                 # Duration-related observations
    
    def __post_init__(self):
        """Validate duration analysis"""
        if self.avg_bars < 0:
            raise ValueError(f"Average bars cannot be negative: {self.avg_bars}")
        if self.median_bars < 0:
            raise ValueError(f"Median bars cannot be negative: {self.median_bars}")
        if not (0 <= self.fast_exits_pct <= 100):
            raise ValueError(f"Fast exits % must be 0-100: {self.fast_exits_pct}")
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "avg_bars": round(self.avg_bars, 2),
            "median_bars": self.median_bars,
            "fast_exits_count": self.fast_exits_count,
            "normal_exits_count": self.normal_exits_count,
            "prolonged_exits_count": self.prolonged_exits_count,
            "fast_exits_pct": round(self.fast_exits_pct, 2),
            "insights": self.insights
        }


@dataclass(frozen=True)
class TradeQualityAnalysis:
    """
    Deep dive into trade execution quality
    
    Comprehensive analysis of how well trades are executed:
    - Win/loss size distribution
    - Trade duration patterns
    - Time to profit vs time to loss
    - Premature exit estimation
    
    Helps optimize entry/exit management.
    """
    win_distribution: TradeDistribution         # Win size breakdown
    loss_distribution: TradeDistribution        # Loss size breakdown
    duration_analysis: DurationAnalysis         # Duration patterns
    avg_bars_to_profit: Optional[float]         # Avg bars winners stay open
    avg_bars_to_loss: Optional[float]           # Avg bars losers stay open
    premature_exit_estimate: str                # Narrative assessment
    insights: List[Insight]                     # Quality-related insights
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "win_distribution": self.win_distribution.to_dict(),
            "loss_distribution": self.loss_distribution.to_dict(),
            "duration_analysis": self.duration_analysis.to_dict(),
            "avg_bars_to_profit": round(self.avg_bars_to_profit, 2) if self.avg_bars_to_profit else None,
            "avg_bars_to_loss": round(self.avg_bars_to_loss, 2) if self.avg_bars_to_loss else None,
            "premature_exit_estimate": self.premature_exit_estimate,
            "insights": [i.to_dict() for i in self.insights]
        }


# ============================================================
# RISK-ADJUSTED PERFORMANCE CONTRACTS
# ============================================================

@dataclass(frozen=True)
class RiskAdjustedMetrics:
    """
    Risk-adjusted performance measures
    
    Goes beyond raw P&L to assess quality of returns:
    - Return over max drawdown (efficiency)
    - Win/loss ratio (risk/reward balance)
    - Expectancy per trade (edge magnitude)
    - Consistency score (volatility-adjusted)
    - Recovery factor (profit relative to losses)
    
    Essential for comparing different strategy variants.
    """
    return_over_max_dd: float                   # Total PnL / Max DD
    avg_win_over_avg_loss: float                # Risk/reward ratio
    expectancy_per_trade: float                 # Average expected return
    consistency_score: float                    # 0-100 (volatility-adjusted)
    recovery_factor: float                      # Total PnL / total losses
    insights: List[Insight]                     # Risk-related insights
    
    def __post_init__(self):
        """Validate risk-adjusted metrics"""
        if not (0 <= self.consistency_score <= 100):
            raise ValueError(f"Consistency score must be 0-100: {self.consistency_score}")
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "return_over_max_dd": round(self.return_over_max_dd, 2),
            "avg_win_over_avg_loss": round(self.avg_win_over_avg_loss, 2),
            "expectancy_per_trade": round(self.expectancy_per_trade, 4),
            "consistency_score": round(self.consistency_score, 2),
            "recovery_factor": round(self.recovery_factor, 2),
            "insights": [i.to_dict() for i in self.insights]
        }


# ============================================================
# COMPARATIVE CONTEXT CONTRACT
# ============================================================

@dataclass(frozen=True)
class ComparativeContext:
    """
    Comparative analysis and statistical flags
    
    v1.0: Statistical anomaly detection only
    v2.0+: Will include baseline comparison, historical percentiles
    
    Helps identify unusual patterns that need investigation.
    """
    vs_baseline: Optional[Dict]                 # Future: comparison to baseline
    statistical_flags: List[str]                # Unusual patterns detected
    percentile_rank: Optional[float]            # Future: historical percentile
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "vs_baseline": self.vs_baseline,
            "statistical_flags": self.statistical_flags,
            "percentile_rank": self.percentile_rank
        }


# ============================================================
# EXECUTIVE SUMMARY CONTRACT
# ============================================================

@dataclass(frozen=True)
class ExecutiveSummary:
    """
    Top-level insights and strategic assessment
    
    The "elevator pitch" of strategy performance.
    Provides:
    - Performance grade (A+ to D-)
    - Grade reasoning
    - Critical insights (top 3-5 most important)
    - Key strengths (what's working)
    - Improvement areas (what needs attention)
    - Overall assessment (2-3 sentence summary)
    
    Primary deliverable for decision-making.
    """
    performance_grade: str                      # "A+" to "D-"
    grade_reasoning: str                        # Why this grade
    critical_insights: List[Insight]            # Top 3-5 most important
    key_strengths: List[str]                    # What's working well
    improvement_areas: List[str]                # What needs attention
    overall_assessment: str                     # 2-3 sentence summary
    
    def __post_init__(self):
        """Validate executive summary"""
        # Validate grade format
        valid_grades = {
            "A+", "A", "A-", "B+", "B", "B-", 
            "C+", "C", "C-", "D+", "D", "D-", "F"
        }
        if self.performance_grade not in valid_grades:
            raise ValueError(f"Invalid grade: {self.performance_grade}")
        
        # Validate critical insights count
        if len(self.critical_insights) > 7:
            raise ValueError(f"Too many critical insights (max 7): {len(self.critical_insights)}")
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "performance_grade": self.performance_grade,
            "grade_reasoning": self.grade_reasoning,
            "critical_insights": [i.to_dict() for i in self.critical_insights],
            "key_strengths": self.key_strengths,
            "improvement_areas": self.improvement_areas,
            "overall_assessment": self.overall_assessment
        }


# ============================================================
# MAIN ANALYTICS REPORT CONTRACT
# ============================================================

@dataclass(frozen=True)
class AnalyticsReport:
    """
    Complete analytics report with intelligent insights
    
    Primary output of TradeAnalytics module.
    Combines all analysis dimensions into comprehensive assessment.
    
    Primary format: Markdown executive summary (human-readable)
    Secondary format: Structured data (for ReportGenerator)
    
    Usage:
        report = analyze_trades(result, metrics, config)
        print(report.get_executive_summary_markdown())
        insights = report.get_critical_insights_only()
    """
    # Core analytics components
    executive_summary: ExecutiveSummary
    time_performance: TimePerformanceBreakdown
    trade_quality: TradeQualityAnalysis
    risk_adjusted: RiskAdjustedMetrics
    comparative: Optional[ComparativeContext]
    
    # Reference data
    input_metrics: 'MetricsReport'              # Base metrics (from MetricsCalculator)
    analysis_timestamp: str                     # When analysis was performed
    analysis_duration_ms: float                 # How long analysis took
    
    def to_dict(self) -> Dict:
        """
        Convert entire report to dictionary
        
        Returns nested structure preserving all data.
        Suitable for JSON serialization or ReportGenerator input.
        """
        return {
            "executive_summary": self.executive_summary.to_dict(),
            "time_performance": self.time_performance.to_dict(),
            "trade_quality": self.trade_quality.to_dict(),
            "risk_adjusted": self.risk_adjusted.to_dict(),
            "comparative": self.comparative.to_dict() if self.comparative else None,
            "input_metrics": self.input_metrics.to_dict(),
            "metadata": {
                "analysis_timestamp": self.analysis_timestamp,
                "analysis_duration_ms": round(self.analysis_duration_ms, 2)
            }
        }
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), indent=2)
    
    def get_executive_summary_markdown(self) -> str:
        """
        Generate markdown-formatted executive summary
        
        Primary human-readable output.
        Formatted as consulting report with clear sections:
        - Header with key metrics
        - Critical insights
        - Strengths
        - Improvement areas
        - Overall assessment
        - Performance grade
        
        Returns:
            Markdown string ready for display or file save
        """
        # Will be implemented in trade_analytics.py
        # Placeholder for contract definition
        return "# Executive Summary\n\n(Generated by TradeAnalytics.format_markdown_report())"
    
    def get_all_insights(self) -> List[Insight]:
        """
        Collect all insights from all analysis components
        
        Returns:
            Flat list of all insights (critical + time + quality + risk)
        """
        all_insights = []
        all_insights.extend(self.executive_summary.critical_insights)
        all_insights.extend(self.time_performance.insights)
        all_insights.extend(self.trade_quality.insights)
        all_insights.extend(self.risk_adjusted.insights)
        return all_insights
    
    def get_critical_insights_only(self) -> List[Insight]:
        """
        Get only critical severity insights
        
        Filters all insights to show only critical items.
        Useful for alerting or highlighting most important issues.
        
        Returns:
            List of insights with severity="critical"
        """
        return [i for i in self.get_all_insights() if i.severity == "critical"]
    
    def get_insights_by_category(self, category: str) -> List[Insight]:
        """
        Get insights for specific category
        
        Args:
            category: "time" | "quality" | "risk" | "general"
        
        Returns:
            List of insights matching category
        """
        return [i for i in self.get_all_insights() if i.category == category]


# ============================================================
# FACTORY FUNCTIONS
# ============================================================

def create_empty_insight(
    message: str = "No insight",
    recommendation: str = "No recommendation",
    confidence: str = "Low",
    category: str = "general",
    severity: str = "info"
) -> Insight:
    """
    Create minimal insight for testing or placeholders
    
    Args:
        message: Insight message
        recommendation: Action recommendation
        confidence: "High" | "Medium" | "Low"
        category: "time" | "quality" | "risk" | "general"
        severity: "critical" | "warning" | "info" | "success"
    
    Returns:
        Valid Insight instance
    """
    return Insight(
        message=message,
        recommendation=recommendation,
        confidence=confidence,
        impact_estimate=None,
        category=category,
        severity=severity
    )


def create_empty_session_metrics(session_name: str = "Unknown") -> SessionMetrics:
    """
    Create empty session metrics for testing
    
    Args:
        session_name: Name of session/segment
    
    Returns:
        Valid SessionMetrics with zero values
    """
    return SessionMetrics(
        session_name=session_name,
        trades=0,
        winning_trades=0,
        win_rate=0.0,
        total_pnl=0.0,
        avg_pnl=0.0,
        largest_win=0.0,
        largest_loss=0.0
    )


# ============================================================
# MODULE METADATA
# ============================================================

__all__ = [
    # Configuration
    "TradingSessionConfig",
    
    # Core contracts
    "Insight",
    "SessionMetrics",
    "TimePerformanceBreakdown",
    "TradeDistribution",
    "DurationAnalysis",
    "TradeQualityAnalysis",
    "RiskAdjustedMetrics",
    "ComparativeContext",
    "ExecutiveSummary",
    "AnalyticsReport",
    
    # Factory functions
    "create_empty_insight",
    "create_empty_session_metrics"
]


if __name__ == "__main__":
    print("Analytics Contracts Module")
    print("=" * 50)
    print(f"Total contracts defined: {len(__all__)}")
    print("\nContract categories:")
    print("  - Configuration: 1")
    print("  - Insights: 1")
    print("  - Time Performance: 2")
    print("  - Trade Quality: 3")
    print("  - Risk Adjusted: 1")
    print("  - Comparative: 1")
    print("  - Executive: 1")
    print("  - Main Report: 1")
    print("  - Factories: 2")
    print("\nReady for TradeAnalytics implementation! 🚀")