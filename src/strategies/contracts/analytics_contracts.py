"""Analytics contracts for Strategy Builder.
Comprehensive analytics framework: actionable insights, time-based breakdowns,
quality metrics, and risk-adjusted performance.
Philosophy: Intelligence over speed, insights over raw data.
"""
from __future__ import annotations
import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional
if TYPE_CHECKING:
    from src.strategies.contracts.metrics_contracts import MetricsReport

# ============================================================
# CONFIGURATION CONTRACTS
# ============================================================
@dataclass(frozen=True)
class TradingSessionConfig:
    """Configuration for trading session definitions used in time-performance analysis.

    Default sessions are standard forex UTC windows (Asia / London / NY).
    Override to match your instrument's actual active hours.

    Session 20 change: ``frozen=True`` added (DEC-004 / P1-CH5-1).

    Notes
    -----
    ``sessions`` is a ``dict`` whose *reference* is frozen; the dict contents
    are mutable, but this is a read-only configuration object and should never
    be mutated after construction.
    """

    sessions: Dict[str, tuple] = field(
        default_factory=lambda: {
            "Asia":   (0,  8),   # 00:00–08:00 UTC
            "London": (8,  16),  # 08:00–16:00 UTC
            "NY":     (16, 24),  # 16:00–24:00 UTC
        }
    )

    def __post_init__(self) -> None:
        if not self.sessions:
            raise ValueError("TradingSessionConfig: at least one session must be defined.")
        for name, bounds in self.sessions.items():
            if len(bounds) != 2:
                raise ValueError(
                    f"Session '{name}' must have exactly (start, end) hours, got {bounds}."
                )
            start, end = bounds
            if not (0 <= start < 24 and 0 < end <= 24):
                raise ValueError(
                    f"Session '{name}' hours out of range: start={start}, end={end}. "
                    f"Expected 0 ≤ start < 24 and 0 < end ≤ 24."
                )
            if start >= end:
                raise ValueError(
                    f"Session '{name}' start ({start}) must be less than end ({end})."
                )

# ============================================================
# INSIGHT CONTRACT
# ============================================================

@dataclass(frozen=True)
class Insight:
    """Single actionable insight with confidence and impact assessment.

    Every analytical observation is wrapped in an ``Insight`` so that
    downstream consumers can filter by ``severity``, sort by ``confidence``,
    and render recommendations in a consistent format.

    Fields
    ------
    message:
        The observed fact: e.g. "Asia session losing −45 pts".
    recommendation:
        The suggested action: e.g. "Consider excluding Asia session".
    confidence:
        ``"High"`` | ``"Medium"`` | ``"Low"``
    category:
        ``"time"`` | ``"quality"`` | ``"risk"`` | ``"general"``
    severity:
        ``"critical"`` | ``"warning"`` | ``"info"`` | ``"success"``
    impact_estimate:
        Optional projected benefit: e.g. "Potential +45 pts improvement".
        Defaults to ``None`` — not all insights have a quantifiable impact.

    Notes
    -----
    ``impact_estimate`` is the last field and the only one with a default
    (``None``).  This satisfies the frozen-dataclass rule that fields with
    defaults must follow all fields without defaults.  Callers that omit
    ``impact_estimate`` receive ``None`` automatically; callers that pass it
    as a keyword argument are unaffected by its position.
    """
    message: str
    recommendation: str
    confidence: str
    category: str
    severity: str
    impact_estimate: Optional[str] = None

    def __post_init__(self) -> None:
        _valid("confidence", self.confidence, {"High", "Medium", "Low"})
        _valid("category",   self.category,   {"time", "quality", "risk", "general"})
        _valid("severity",   self.severity,   {"critical", "warning", "info", "success"})

    def to_dict(self) -> Dict:
        """Serialise to dict (JSON-safe)."""
        return {
            "message":         self.message,
            "recommendation":  self.recommendation,
            "confidence":      self.confidence,
            "impact_estimate": self.impact_estimate,
            "category":        self.category,
            "severity":        self.severity,
        }

# ============================================================
# TIME PERFORMANCE CONTRACTS
# ============================================================

@dataclass(frozen=True)
class SessionMetrics:
    """Performance metrics for a specific time segment (session, hour, or day)."""

    session_name: str
    trades: int
    winning_trades: int
    win_rate: float     # 0–100
    total_pnl: float
    avg_pnl: float
    largest_win: float
    largest_loss: float

    def __post_init__(self) -> None:
        if self.trades < 0:
            raise ValueError(f"SessionMetrics.trades cannot be negative: {self.trades}")
        if not (0 <= self.winning_trades <= self.trades):
            raise ValueError(
                f"winning_trades ({self.winning_trades}) must be 0–{self.trades}."
            )
        if not (0 <= self.win_rate <= 100):
            raise ValueError(f"win_rate must be 0–100, got {self.win_rate}.")

    def to_dict(self) -> Dict:
        return {
            "session_name":   self.session_name,
            "trades":         self.trades,
            "winning_trades": self.winning_trades,
            "win_rate":       round(self.win_rate, 2),
            "total_pnl":      round(self.total_pnl, 2),
            "avg_pnl":        round(self.avg_pnl, 2),
            "largest_win":    round(self.largest_win, 2),
            "largest_loss":   round(self.largest_loss, 2),
        }

@dataclass(frozen=True)
class TimePerformanceBreakdown:
    """Performance analysis across sessions, hours, and weekdays.

    ``by_hour`` keys must be integers 0–23.
    ``by_day`` keys must be full English weekday names (Monday … Sunday).
    """

    by_session: Dict[str, SessionMetrics]
    by_hour:    Dict[int, SessionMetrics]
    by_day:     Dict[str, SessionMetrics]
    best_session:  str
    worst_session: str
    insights: List[Insight]

    _VALID_DAYS = frozenset(
        {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"}
    )

    def __post_init__(self) -> None:
        for hour in self.by_hour:
            if not (0 <= hour <= 23):
                raise ValueError(f"Hour key must be 0–23, got {hour}.")
        for day in self.by_day:
            if day not in self._VALID_DAYS:
                raise ValueError(f"Invalid weekday key: '{day}'.")

    def to_dict(self) -> Dict:
        return {
            "by_session":    {k: v.to_dict() for k, v in self.by_session.items()},
            "by_hour":       {str(k): v.to_dict() for k, v in self.by_hour.items()},
            "by_day":        {k: v.to_dict() for k, v in self.by_day.items()},
            "best_session":  self.best_session,
            "worst_session": self.worst_session,
            "insights":      [i.to_dict() for i in self.insights],
        }

# ============================================================
# TRADE QUALITY CONTRACTS
# ============================================================

@dataclass(frozen=True)
class TradeDistribution:
    """Win or loss size distribution (small / medium / large).

    Thresholds: small < 3 pts, medium 3–7 pts, large > 7 pts.
    Helps identify if strategy relies on rare large winners.
    """

    small_count:  int
    medium_count: int
    large_count:  int
    small_pct:    float
    medium_pct:   float
    large_pct:    float

    def __post_init__(self) -> None:
        total = self.small_count + self.medium_count + self.large_count
        if total > 0:
            pct_sum = self.small_pct + self.medium_pct + self.large_pct
            if not (99.9 <= pct_sum <= 100.1):
                raise ValueError(f"TradeDistribution percentages must sum to 100, got {pct_sum:.2f}.")

    def to_dict(self) -> Dict:
        return {
            "small_count":  self.small_count,
            "medium_count": self.medium_count,
            "large_count":  self.large_count,
            "small_pct":    round(self.small_pct,  2),
            "medium_pct":   round(self.medium_pct, 2),
            "large_pct":    round(self.large_pct,  2),
        }

@dataclass(frozen=True)
class DurationAnalysis:
    """Trade duration pattern analysis.

    Thresholds: fast < 3 bars, normal 3–10 bars, prolonged > 10 bars.
    High ``fast_exits_pct`` suggests stops may be too tight.
    """

    avg_bars:               float
    median_bars:            int
    fast_exits_count:       int
    normal_exits_count:     int
    prolonged_exits_count:  int
    fast_exits_pct:         float
    insights:               List[str]

    def __post_init__(self) -> None:
        if self.avg_bars < 0:
            raise ValueError(f"avg_bars cannot be negative: {self.avg_bars}.")
        if self.median_bars < 0:
            raise ValueError(f"median_bars cannot be negative: {self.median_bars}.")
        if not (0 <= self.fast_exits_pct <= 100):
            raise ValueError(f"fast_exits_pct must be 0–100, got {self.fast_exits_pct}.")

    def to_dict(self) -> Dict:
        return {
            "avg_bars":               round(self.avg_bars, 2),
            "median_bars":            self.median_bars,
            "fast_exits_count":       self.fast_exits_count,
            "normal_exits_count":     self.normal_exits_count,
            "prolonged_exits_count":  self.prolonged_exits_count,
            "fast_exits_pct":         round(self.fast_exits_pct, 2),
            "insights":               list(self.insights),
        }

@dataclass(frozen=True)
class TradeQualityAnalysis:
    """Deep-dive into trade execution quality.

    Combines win/loss size distributions, duration patterns, and
    time-to-profit vs time-to-loss to surface premature-exit estimates.
    """

    win_distribution:         TradeDistribution
    loss_distribution:        TradeDistribution
    duration_analysis:        DurationAnalysis
    avg_bars_to_profit:       Optional[float]
    avg_bars_to_loss:         Optional[float]
    premature_exit_estimate:  str
    insights:                 List[Insight]

    def to_dict(self) -> Dict:
        return {
            "win_distribution":        self.win_distribution.to_dict(),
            "loss_distribution":       self.loss_distribution.to_dict(),
            "duration_analysis":       self.duration_analysis.to_dict(),
            "avg_bars_to_profit":      round(self.avg_bars_to_profit, 2) if self.avg_bars_to_profit is not None else None,
            "avg_bars_to_loss":        round(self.avg_bars_to_loss,   2) if self.avg_bars_to_loss   is not None else None,
            "premature_exit_estimate": self.premature_exit_estimate,
            "insights":                [i.to_dict() for i in self.insights],
        }

# ============================================================
# RISK-ADJUSTED PERFORMANCE CONTRACTS
# ============================================================

@dataclass(frozen=True)
class RiskAdjustedMetrics:
    """Risk-adjusted performance measures.

    Essential for comparing strategy variants — raw P&L alone is insufficient.

    Fields
    ------
    return_over_max_dd:
        Total P&L / |Max drawdown|.  Higher = more efficient use of capital at risk.
    avg_win_over_avg_loss:
        Average win / |Average loss|.  The effective risk:reward ratio.
    expectancy_per_trade:
        Statistical edge per trade (signed, in points).
    consistency_score:
        0–100 score derived from coefficient-of-variation; higher = more consistent.
    recovery_factor:
        Total P&L / |Gross losses|.  Measures how quickly losses are recovered.
    """

    return_over_max_dd:     float
    avg_win_over_avg_loss:  float
    expectancy_per_trade:   float
    consistency_score:      float   # 0–100
    recovery_factor:        float
    insights:               List[Insight]

    def __post_init__(self) -> None:
        if not (0 <= self.consistency_score <= 100):
            raise ValueError(f"consistency_score must be 0–100, got {self.consistency_score}.")

    def to_dict(self) -> Dict:
        return {
            "return_over_max_dd":    round(self.return_over_max_dd,    2),
            "avg_win_over_avg_loss": round(self.avg_win_over_avg_loss, 2),
            "expectancy_per_trade":  round(self.expectancy_per_trade,  4),
            "consistency_score":     round(self.consistency_score,     2),
            "recovery_factor":       round(self.recovery_factor,       2),
            "insights":              [i.to_dict() for i in self.insights],
        }

# ============================================================
# COMPARATIVE CONTEXT CONTRACT
# ============================================================

@dataclass(frozen=True)
class ComparativeContext:
    """Statistical anomaly detection and (future) baseline comparison.

    v1.0: ``statistical_flags`` only.
    v2.0+: ``vs_baseline`` and ``percentile_rank`` will be populated.
    """

    vs_baseline:       Optional[Dict]
    statistical_flags: List[str]
    percentile_rank:   Optional[float]

    def to_dict(self) -> Dict:
        return {
            "vs_baseline":       self.vs_baseline,
            "statistical_flags": list(self.statistical_flags),
            "percentile_rank":   self.percentile_rank,
        }

# ============================================================
# EXECUTIVE SUMMARY CONTRACT
# ============================================================

@dataclass(frozen=True)
class ExecutiveSummary:
    """Top-level strategic assessment — the "elevator pitch" of performance.

    Grading algorithm (4 × 25 pts → 100 pt scale)
    -----------------------------------------------
    1. Win rate:     ≥20 % = 25, ≥15 % = 20, ≥10 % = 10
    2. Profit factor: ≥2.0 = 25, ≥1.5 = 20, ≥1.2 = 10
    3. Drawdown:     DD < 20 % of profit = 25, < 50 % = 15, < 100 % = 5
    4. Consistency:  ≥70 = 25, ≥50 = 15, ≥30 = 5

    Score → Grade: 90+ = A+, 85 = A, 80 = A−, 75 = B+, 70 = B, 65 = B−,
                   60 = C+, 55 = C, 50 = C−, 40 = D+, 30 = D, < 30 = F
    """

    performance_grade:   str            # "A+" … "F"
    grade_reasoning:     str
    critical_insights:   List[Insight]  # Top 3–7
    key_strengths:       List[str]
    improvement_areas:   List[str]
    overall_assessment:  str            # 2–3 sentences

    _VALID_GRADES = frozenset({
        "A+", "A", "A-",
        "B+", "B", "B-",
        "C+", "C", "C-",
        "D+", "D", "D-",
        "F",
    })

    def __post_init__(self) -> None:
        if self.performance_grade not in self._VALID_GRADES:
            raise ValueError(f"Invalid grade: '{self.performance_grade}'.")
        if len(self.critical_insights) > 7:
            raise ValueError(
                f"critical_insights must be ≤ 7, got {len(self.critical_insights)}."
            )

    def to_dict(self) -> Dict:
        return {
            "performance_grade":  self.performance_grade,
            "grade_reasoning":    self.grade_reasoning,
            "critical_insights":  [i.to_dict() for i in self.critical_insights],
            "key_strengths":      list(self.key_strengths),
            "improvement_areas":  list(self.improvement_areas),
            "overall_assessment": self.overall_assessment,
        }

# ============================================================
# MAIN ANALYTICS REPORT CONTRACT
# ============================================================

@dataclass(frozen=True)
class AnalyticsReport:
    """Complete analytics report with intelligent insights.

    Primary output of ``TradeAnalytics.analyze()``.
    All sub-reports are frozen; the full structure is immutable once built.

    Serialisation
    -------------
    * ``to_dict()``  — nested dict, suitable for JSON or ``ReportGenerator``.
    * ``to_json()``  — pretty-printed JSON string.

    Insight helpers
    ---------------
    * ``get_all_insights()``           — flat list of every insight.
    * ``get_critical_insights_only()`` — severity == "critical" only.
    * ``get_insights_by_category(c)``  — filter by category string.
    """

    executive_summary: ExecutiveSummary
    time_performance:  TimePerformanceBreakdown
    trade_quality:     TradeQualityAnalysis
    risk_adjusted:     RiskAdjustedMetrics
    comparative:       Optional[ComparativeContext]

    # Reference data (typed via TYPE_CHECKING import)
    input_metrics:         "MetricsReport"
    analysis_timestamp:    str
    analysis_duration_ms:  float

    # ------------------------------------------------------------------
    def to_dict(self) -> Dict:
        """Nested dict — JSON-safe, suitable for ReportGenerator."""
        return {
            "executive_summary": self.executive_summary.to_dict(),
            "time_performance":  self.time_performance.to_dict(),
            "trade_quality":     self.trade_quality.to_dict(),
            "risk_adjusted":     self.risk_adjusted.to_dict(),
            "comparative":       self.comparative.to_dict() if self.comparative else None,
            "input_metrics":     self.input_metrics.to_dict(),
            "metadata": {
                "analysis_timestamp":   self.analysis_timestamp,
                "analysis_duration_ms": round(self.analysis_duration_ms, 2),
            },
        }

    def to_json(self) -> str:
        """Pretty-printed JSON string."""
        return json.dumps(self.to_dict(), indent=2)

    def get_all_insights(self) -> List[Insight]:
        """Flat list of every insight across all analysis dimensions."""
        return [
            *self.executive_summary.critical_insights,
            *self.time_performance.insights,
            *self.trade_quality.insights,
            *self.risk_adjusted.insights,
        ]

    def get_critical_insights_only(self) -> List[Insight]:
        """Insights with ``severity == "critical"`` across all dimensions."""
        return [i for i in self.get_all_insights() if i.severity == "critical"]

    def get_insights_by_category(self, category: str) -> List[Insight]:
        """Insights matching ``category`` (time / quality / risk / general)."""
        return [i for i in self.get_all_insights() if i.category == category]

    def get_executive_summary_markdown(self) -> str:
        """Markdown-formatted executive summary (delegated to TradeAnalytics)."""
        # Full implementation lives in trade_analytics.py; this stub satisfies
        # contract consumers that call the method before the formatter runs.
        return "# Executive Summary\n\n(Generated by TradeAnalytics.format_markdown_report())"

# ============================================================
# FACTORY FUNCTIONS
# ============================================================

def create_empty_insight(
    message:        str = "No insight",
    recommendation: str = "No recommendation",
    confidence:     str = "Low",
    category:       str = "general",
    severity:       str = "info",
) -> Insight:
    """Minimal ``Insight`` for testing or placeholder use.

    ``impact_estimate`` defaults to ``None`` and is intentionally omitted here
    — callers that need it should construct ``Insight`` directly.
    """
    return Insight(
        message=message,
        recommendation=recommendation,
        confidence=confidence,
        category=category,
        severity=severity,
        # impact_estimate omitted — uses default None
    )

def create_empty_session_metrics(session_name: str = "Unknown") -> SessionMetrics:
    """Zero-value ``SessionMetrics`` for testing."""
    return SessionMetrics(
        session_name=session_name,
        trades=0,
        winning_trades=0,
        win_rate=0.0,
        total_pnl=0.0,
        avg_pnl=0.0,
        largest_win=0.0,
        largest_loss=0.0,
    )
# ============================================================
# INTERNAL HELPERS
# ============================================================

def _valid(field_name: str, value: str, allowed: frozenset) -> None:
    """Raise ``ValueError`` when ``value`` is not in ``allowed``."""
    if value not in allowed:
        raise ValueError(
            f"Insight.{field_name} must be one of {sorted(allowed)}, got '{value}'."
        )

# ============================================================
# PUBLIC API
# ============================================================

__all__ = [
    "TradingSessionConfig",
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
    "create_empty_insight",
    "create_empty_session_metrics",
]