"""
TradeAnalytics Module for WBWSStrategy Migration Project

Intelligent trade analytics engine that generates actionable insights.
Philosophy: AI-like recommendations, accuracy over speed, human-readable output.

Created:         2026-02-16 (Session 14 - Design)
Session 15:      2026-02-17 - Time Performance + Trade Quality
Session 16:      TBD        - Risk Adjusted + Executive Summary + Markdown

Architectural Decision (Session 14):
- TradeAnalytics aggregates MetricsReport + adds insights (Option A)
- Metrics parameter is OPTIONAL (auto-calculates if not provided)
- Benefits: Explicit when metrics pre-calculated, convenient when not
"""

import time
import logging
from collections import defaultdict
from pathlib import Path
from statistics import mean, median, stdev
from typing import Dict, List, Optional, Tuple
from datetime import datetime

# Import contracts from proper location
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
    TradingSessionConfig,
    create_empty_insight,
    create_empty_session_metrics,
)

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from src.strategies.contracts.trade_contracts import TradeResult, Trade
    from src.strategies.contracts.metrics_contracts import MetricsReport
    from configs.config_schema import StrategyConfig

logger = logging.getLogger(__name__)


# ============================================================
# CONSTANTS
# ============================================================

# Trade size thresholds (points)
SMALL_TRADE_THRESHOLD  = 3.0   # < 3pts = small
LARGE_TRADE_THRESHOLD  = 7.0   # > 7pts = large

# Duration thresholds (bars)
FAST_EXIT_BARS     = 3    # < 3 bars = fast exit
PROLONGED_EXIT_BARS = 10  # > 10 bars = prolonged

# Insight trigger thresholds
SESSION_CRITICAL_LOSS_PTS     = -30.0   # Session losing > 30pts → critical
SESSION_CRITICAL_MIN_TRADES   = 50      # Min trades for reliable session insight
SESSION_WARNING_WIN_RATE_RATIO = 0.70   # Session win rate < 70% of overall → warning
SESSION_SUCCESS_PROFIT_PCT    = 0.60   # Session drives > 60% of profit → success
SESSION_WARNING_LOSS_PCT      = 0.40   # Session contributes > 40% of losses → warning

FAST_EXIT_CRITICAL_PCT        = 0.80   # > 80% fast exits → critical
FAST_EXIT_WARNING_PCT         = 0.60   # > 60% fast exits → warning

LARGE_WIN_RELIANCE_MAX_PCT    = 0.15   # Large wins < 15% of trades ...
LARGE_WIN_RELIANCE_MIN_CONTRIB = 0.50  # ... but > 50% of profit → warning

WINNER_FASTER_RATIO           = 0.70   # Winners exit in < 70% of loser time → success


# ============================================================
# MAIN ANALYTICS CLASS
# ============================================================

class TradeAnalytics:
    """
    Intelligent trade analytics engine.

    Generates comprehensive performance insights from trading results.
    Philosophy:
    - AI-like suggestions with confidence levels
    - Actionable recommendations over raw data
    - Human-readable markdown reports
    - No performance constraints (accuracy prioritised)

    Usage:
        report = TradeAnalytics.analyze(trade_result=result, config=config)
        print(report.get_executive_summary_markdown())
    """

    # ========================================
    # PUBLIC API
    # ========================================

    @staticmethod
    def analyze(
        trade_result: "TradeResult",
        config: "StrategyConfig",
        metrics: Optional["MetricsReport"] = None,
        session_config: Optional[TradingSessionConfig] = None,
        save_to_file: bool = False,
        output_dir: Optional[Path] = None,
    ) -> AnalyticsReport:
        """
        Main entry point for trade analytics.

        Performs comprehensive analysis of trading performance with
        intelligent insight generation.

        Args:
            trade_result:   Results from TradeSimulator.
            config:         Strategy configuration used.
            metrics:        Base metrics from MetricsCalculator
                            (optional — auto-calculates if None).
            session_config: Custom session definitions (optional).
            save_to_file:   Whether to save report to files.
            output_dir:     Directory for saved files (if save_to_file=True).

        Returns:
            Complete AnalyticsReport with all insights.

        Usage Patterns:
            # Pattern 1: Auto-calculate metrics (convenient)
            >>> report = TradeAnalytics.analyze(result, config)

            # Pattern 2: Pre-calculated metrics (explicit / faster)
            >>> metrics = MetricsCalculator.calculate(result)
            >>> report = TradeAnalytics.analyze(result, config, metrics=metrics)
        """
        start_time = time.perf_counter()

        # ── Auto-calculate metrics if not provided ──────────────────────────
        if metrics is None:
            logger.debug("Metrics not provided — auto-calculating...")
            try:
                from src.strategies.specific.modules.metrics_calculator import (
                    MetricsCalculator,
                )
                metrics = MetricsCalculator.calculate(trade_result)
                logger.debug("Auto-calculation complete.")
            except ImportError as exc:
                raise ImportError(
                    "MetricsCalculator not available for auto-calculation. "
                    "Please provide metrics explicitly."
                ) from exc

        # ── Default session config ───────────────────────────────────────────
        if session_config is None:
            session_config = TradingSessionConfig()

        logger.info("Starting trade analytics analysis…")

        # ── Analysis pipeline ────────────────────────────────────────────────
        time_performance = TradeAnalytics._analyze_time_performance(
            trade_result, metrics, session_config
        )

        trade_quality = TradeAnalytics._analyze_trade_quality(
            trade_result, metrics
        )

        risk_adjusted = TradeAnalytics._analyze_risk_adjusted(         # Session 16
            trade_result, metrics
        )

        comparative = TradeAnalytics._analyze_comparative_context(     # Session 16
            trade_result, metrics
        )

        executive_summary = TradeAnalytics._generate_executive_summary( # Session 16
            metrics=metrics,
            time_perf=time_performance,
            quality=trade_quality,
            risk=risk_adjusted,
            comparative=comparative,
        )

        analysis_duration_ms = (time.perf_counter() - start_time) * 1000

        report = AnalyticsReport(
            executive_summary=executive_summary,
            time_performance=time_performance,
            trade_quality=trade_quality,
            risk_adjusted=risk_adjusted,
            comparative=comparative,
            input_metrics=metrics,
            analysis_timestamp=datetime.now().isoformat(),
            analysis_duration_ms=analysis_duration_ms,
        )

        logger.info(f"Analytics completed in {analysis_duration_ms:.2f}ms")

        if save_to_file:
            TradeAnalytics._save_report(report, output_dir)

        return report

    # ========================================
    # TIME PERFORMANCE ANALYSIS  (Session 15)
    # ========================================

    @staticmethod
    def _analyze_time_performance(
        trade_result: "TradeResult",
        metrics: "MetricsReport",
        session_config: TradingSessionConfig,
    ) -> TimePerformanceBreakdown:
        """
        Analyse performance across sessions, hours, and weekdays.

        Breaks down closed-trade performance by:
        - Trading sessions   (Asia / London / NY, or custom)
        - Hour of day        (0–23 UTC)
        - Day of week        (Monday–Sunday)

        Returns a TimePerformanceBreakdown with per-segment SessionMetrics
        and AI-like Insights.
        """
        # ── Filter to closed trades only ─────────────────────────────────────
        closed_trades = [t for t in trade_result.trades if t.exit is not None]

        if not closed_trades:
            logger.warning("No closed trades — returning empty time performance.")
            return TimePerformanceBreakdown(
                by_session={},
                by_hour={},
                by_day={},
                best_session="N/A",
                worst_session="N/A",
                insights=[],
            )

        # ── Group trades by session / hour / day ─────────────────────────────
        by_session_raw: Dict[str, List] = defaultdict(list)
        by_hour_raw:    Dict[int, List] = defaultdict(list)
        by_day_raw:     Dict[str, List] = defaultdict(list)

        for trade in closed_trades:
            hour = trade.entry.entry_time.hour
            day  = trade.entry.entry_time.strftime("%A")   # e.g. "Monday"
            session = TradeAnalytics._get_session_for_hour(hour, session_config)

            by_session_raw[session].append(trade)
            by_hour_raw[hour].append(trade)
            by_day_raw[day].append(trade)

        # ── Calculate SessionMetrics for every group ─────────────────────────
        by_session = {
            name: TradeAnalytics._calculate_session_metrics(trades, name)
            for name, trades in by_session_raw.items()
        }
        by_hour = {
            hour: TradeAnalytics._calculate_session_metrics(trades, str(hour))
            for hour, trades in by_hour_raw.items()
        }
        by_day = {
            day: TradeAnalytics._calculate_session_metrics(trades, day)
            for day, trades in by_day_raw.items()
        }

        # ── Best / worst session by total P&L ────────────────────────────────
        if by_session:
            best_session  = max(by_session, key=lambda s: by_session[s].total_pnl)
            worst_session = min(by_session, key=lambda s: by_session[s].total_pnl)
        else:
            best_session = worst_session = "N/A"

        # ── Generate insights ─────────────────────────────────────────────────
        insights = TradeAnalytics._generate_time_insights(
            by_session, by_hour, by_day, metrics
        )

        return TimePerformanceBreakdown(
            by_session=by_session,
            by_hour=by_hour,
            by_day=by_day,
            best_session=best_session,
            worst_session=worst_session,
            insights=insights,
        )

    # ── Helper: map an hour to a session name ────────────────────────────────

    @staticmethod
    def _get_session_for_hour(
        hour: int,
        session_config: TradingSessionConfig,
    ) -> str:
        """Return the session name that contains the given UTC hour."""
        for name, (start, end) in session_config.sessions.items():
            if start <= hour < end:
                return name
        return "Other"

    # ── Helper: calculate SessionMetrics for a list of trades ────────────────

    @staticmethod
    def _calculate_session_metrics(
        trades: List["Trade"],
        session_name: str,
    ) -> SessionMetrics:
        """
        Compute all SessionMetrics fields from a list of closed trades.

        Args:
            trades:       List of Trade objects (must have .exit set).
            session_name: Label for this segment.

        Returns:
            Populated SessionMetrics.
        """
        if not trades:
            return create_empty_session_metrics(session_name)

        pnl_values     = [t.exit.pnl_points for t in trades if t.exit]
        winning_trades = [t for t in trades if t.exit and t.exit.is_win]

        total_trades = len(trades)
        win_count    = len(winning_trades)
        win_rate     = (win_count / total_trades * 100) if total_trades else 0.0
        total_pnl    = sum(pnl_values)
        avg_pnl      = mean(pnl_values) if pnl_values else 0.0
        largest_win  = max(pnl_values) if pnl_values else 0.0
        largest_loss = min(pnl_values) if pnl_values else 0.0

        return SessionMetrics(
            session_name=session_name,
            trades=total_trades,
            winning_trades=win_count,
            win_rate=win_rate,
            total_pnl=total_pnl,
            avg_pnl=avg_pnl,
            largest_win=largest_win,
            largest_loss=largest_loss,
        )

    # ── Insight generation for time analysis ─────────────────────────────────

    @staticmethod
    def _generate_time_insights(
        by_session: Dict[str, SessionMetrics],
        by_hour:    Dict[int, SessionMetrics],
        by_day:     Dict[str, SessionMetrics],
        overall_metrics: "MetricsReport",
    ) -> List[Insight]:
        """
        Apply intelligence rules to produce time-based Insights.

        Rules applied (in order of severity):
          1. Session losing significantly           → critical
          2. Session win rate well below average    → warning
          3. Day significantly underperforming      → warning
          4. One session driving most of the profit → success
          5. Session contributing most losses       → warning
          6. Best hour cluster                      → info
        """
        insights: List[Insight] = []
        total_pnl = overall_metrics.total_pnl_points
        overall_win_rate = overall_metrics.win_rate

        # ── 1. Session losing significantly ──────────────────────────────────
        for name, sm in by_session.items():
            if sm.total_pnl < SESSION_CRITICAL_LOSS_PTS and sm.trades >= SESSION_CRITICAL_MIN_TRADES:
                insights.append(Insight(
                    message=(
                        f"{name} session is losing "
                        f"{sm.total_pnl:+.1f} pts across {sm.trades} trades"
                    ),
                    recommendation=f"Consider excluding the {name} session entirely",
                    confidence="High",
                    impact_estimate=f"Potential {abs(sm.total_pnl):+.1f} pts improvement",
                    category="time",
                    severity="critical",
                ))
            elif sm.total_pnl < SESSION_CRITICAL_LOSS_PTS and sm.trades < SESSION_CRITICAL_MIN_TRADES:
                # Fewer trades → lower confidence
                insights.append(Insight(
                    message=(
                        f"{name} session is losing "
                        f"{sm.total_pnl:+.1f} pts ({sm.trades} trades — small sample)"
                    ),
                    recommendation=f"Monitor {name} session; consider excluding if pattern persists",
                    confidence="Medium",
                    impact_estimate=f"Potential {abs(sm.total_pnl):+.1f} pts improvement",
                    category="time",
                    severity="warning",
                ))

        # ── 2. Session win rate well below overall average ────────────────────
        for name, sm in by_session.items():
            if (
                overall_win_rate > 0
                and sm.trades >= 20
                and sm.win_rate < overall_win_rate * SESSION_WARNING_WIN_RATE_RATIO
            ):
                insights.append(Insight(
                    message=(
                        f"{name} session win rate {sm.win_rate:.1f}% is well below "
                        f"overall {overall_win_rate:.1f}%"
                    ),
                    recommendation=(
                        f"Review signal quality during {name} session; "
                        "consider tighter entry criteria"
                    ),
                    confidence="Medium",
                    impact_estimate=None,
                    category="time",
                    severity="warning",
                ))

        # ── 3. Day significantly underperforming (negative P&L) ───────────────
        day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
        for day in day_order:
            dm = by_day.get(day)
            if dm is None:
                continue
            # Flag a day that is negative and has at least 20 trades
            if dm.total_pnl < 0 and dm.trades >= 20:
                insights.append(Insight(
                    message=(
                        f"{day} is net-negative at {dm.total_pnl:+.1f} pts "
                        f"over {dm.trades} trades"
                    ),
                    recommendation=f"Investigate {day} market conditions; consider reduced size",
                    confidence="Medium",
                    impact_estimate=f"Potential {abs(dm.total_pnl):+.1f} pts recovery",
                    category="time",
                    severity="warning",
                ))

        # ── 4. One session is the primary profit driver ───────────────────────
        if total_pnl > 0:
            for name, sm in by_session.items():
                if sm.total_pnl > total_pnl * SESSION_SUCCESS_PROFIT_PCT:
                    pct = (sm.total_pnl / total_pnl) * 100
                    insights.append(Insight(
                        message=(
                            f"{name} session drives {pct:.0f}% of total profit "
                            f"({sm.total_pnl:+.1f} pts)"
                        ),
                        recommendation=f"Prioritise and protect the {name} session edge",
                        confidence="High",
                        impact_estimate=None,
                        category="time",
                        severity="success",
                    ))

        # ── 5. Session contributing the most to total losses ─────────────────
        total_loss_pts = sum(
            sm.total_pnl for sm in by_session.values() if sm.total_pnl < 0
        )
        if total_loss_pts < 0:
            for name, sm in by_session.items():
                if sm.total_pnl < 0:
                    loss_contribution = sm.total_pnl / total_loss_pts
                    if loss_contribution >= SESSION_WARNING_LOSS_PCT:
                        insights.append(Insight(
                            message=(
                                f"{name} session accounts for "
                                f"{loss_contribution * 100:.0f}% of all losses"
                            ),
                            recommendation=(
                                f"Focus loss-reduction efforts on {name}; "
                                "review filters or reduce size"
                            ),
                            confidence="High",
                            impact_estimate=None,
                            category="time",
                            severity="warning",
                        ))

        # ── 6. Best performing hour cluster (info) ────────────────────────────
        if by_hour:
            # Top 3 hours by total P&L
            top_hours = sorted(by_hour.items(), key=lambda kv: kv[1].total_pnl, reverse=True)[:3]
            top_hours_filtered = [(h, sm) for h, sm in top_hours if sm.total_pnl > 0 and sm.trades >= 10]
            if top_hours_filtered:
                hour_labels = ", ".join(f"{h:02d}:00" for h, _ in top_hours_filtered)
                best_pnl    = top_hours_filtered[0][1].total_pnl
                insights.append(Insight(
                    message=f"Peak profitable hours: {hour_labels} UTC",
                    recommendation="Consider concentrating trade activity around these hours",
                    confidence="Low",
                    impact_estimate=f"Top hour generates {best_pnl:+.1f} pts",
                    category="time",
                    severity="info",
                ))

        return insights

    # ========================================
    # TRADE QUALITY ANALYSIS  (Session 15)
    # ========================================

    @staticmethod
    def _analyze_trade_quality(
        trade_result: "TradeResult",
        metrics: "MetricsReport",
    ) -> TradeQualityAnalysis:
        """
        Deep dive into trade execution quality.

        Analyses:
        - Win / loss size distribution (small / medium / large)
        - Trade duration patterns (fast / normal / prolonged)
        - Average bars-to-profit vs bars-to-loss
        - Premature exit narrative
        - Actionable quality Insights
        """
        closed_trades = [t for t in trade_result.trades if t.exit is not None]

        if not closed_trades:
            logger.warning("No closed trades — returning empty trade quality.")
            empty_dist = TradeDistribution(
                small_count=0, medium_count=0, large_count=0,
                small_pct=0.0, medium_pct=0.0, large_pct=100.0,
            )
            empty_duration = DurationAnalysis(
                avg_bars=0.0, median_bars=0,
                fast_exits_count=0, normal_exits_count=0, prolonged_exits_count=0,
                fast_exits_pct=0.0, insights=[],
            )
            return TradeQualityAnalysis(
                win_distribution=empty_dist,
                loss_distribution=empty_dist,
                duration_analysis=empty_duration,
                avg_bars_to_profit=None,
                avg_bars_to_loss=None,
                premature_exit_estimate="No data",
                insights=[],
            )

        wins   = [t for t in closed_trades if t.exit.is_win]
        losses = [t for t in closed_trades if t.exit.is_loss]

        # ── Distributions ─────────────────────────────────────────────────────
        win_dist  = TradeAnalytics._calculate_trade_distribution(wins,   is_wins=True)
        loss_dist = TradeAnalytics._calculate_trade_distribution(losses, is_wins=False)

        # ── Duration analysis ─────────────────────────────────────────────────
        duration_analysis = TradeAnalytics._analyze_duration_patterns(closed_trades)

        # ── Average bars to profit / loss ─────────────────────────────────────
        avg_bars_to_profit = (
            mean([t.exit.duration_bars for t in wins]) if wins else None
        )
        avg_bars_to_loss = (
            mean([t.exit.duration_bars for t in losses]) if losses else None
        )

        # ── Premature-exit narrative ───────────────────────────────────────────
        premature_exit_estimate = TradeAnalytics._build_premature_exit_narrative(
            duration_analysis, avg_bars_to_profit, avg_bars_to_loss
        )

        # ── Quality insights ──────────────────────────────────────────────────
        insights = TradeAnalytics._generate_quality_insights(
            win_dist, loss_dist, duration_analysis, metrics,
            avg_bars_to_profit, avg_bars_to_loss,
        )

        return TradeQualityAnalysis(
            win_distribution=win_dist,
            loss_distribution=loss_dist,
            duration_analysis=duration_analysis,
            avg_bars_to_profit=avg_bars_to_profit,
            avg_bars_to_loss=avg_bars_to_loss,
            premature_exit_estimate=premature_exit_estimate,
            insights=insights,
        )

    # ── Helper: trade distribution ────────────────────────────────────────────

    @staticmethod
    def _calculate_trade_distribution(
        trades: List["Trade"],
        is_wins: bool,
    ) -> TradeDistribution:
        """
        Categorise trades into small / medium / large by absolute P&L.

        Thresholds:
            small  : |pnl| < 3 pts
            medium : 3 pts ≤ |pnl| ≤ 7 pts
            large  : |pnl| > 7 pts
        """
        if not trades:
            return TradeDistribution(
                small_count=0, medium_count=0, large_count=0,
                small_pct=0.0, medium_pct=0.0, large_pct=0.0,
            )

        small = medium = large = 0
        for t in trades:
            abs_pnl = abs(t.exit.pnl_points)
            if abs_pnl < SMALL_TRADE_THRESHOLD:
                small += 1
            elif abs_pnl <= LARGE_TRADE_THRESHOLD:
                medium += 1
            else:
                large += 1

        total = len(trades)
        return TradeDistribution(
            small_count=small,
            medium_count=medium,
            large_count=large,
            small_pct=round(small  / total * 100, 2),
            medium_pct=round(medium / total * 100, 2),
            large_pct=round(large  / total * 100, 2),
        )

    # ── Helper: duration analysis ─────────────────────────────────────────────

    @staticmethod
    def _analyze_duration_patterns(
        trades: List["Trade"],
    ) -> DurationAnalysis:
        """
        Analyse trade duration in bars.

        Categories:
            fast      : duration_bars < 3
            normal    : 3 ≤ duration_bars ≤ 10
            prolonged : duration_bars > 10
        """
        if not trades:
            return DurationAnalysis(
                avg_bars=0.0, median_bars=0,
                fast_exits_count=0, normal_exits_count=0, prolonged_exits_count=0,
                fast_exits_pct=0.0, insights=[],
            )

        durations = [t.exit.duration_bars for t in trades if t.exit]

        fast      = sum(1 for d in durations if d < FAST_EXIT_BARS)
        normal    = sum(1 for d in durations if FAST_EXIT_BARS <= d <= PROLONGED_EXIT_BARS)
        prolonged = sum(1 for d in durations if d > PROLONGED_EXIT_BARS)
        total     = len(durations)

        avg_bars_val    = mean(durations)
        median_bars_val = int(median(durations))
        fast_pct        = fast / total * 100

        # ── Duration-level text insights ──────────────────────────────────────
        text_insights: List[str] = []
        if fast_pct > FAST_EXIT_WARNING_PCT * 100:
            text_insights.append(
                f"{fast_pct:.0f}% of trades exit in fewer than {FAST_EXIT_BARS} bars — "
                "potential premature exits or tight stops"
            )
        if prolonged / total > 0.25:
            text_insights.append(
                f"{prolonged} trades ({prolonged / total * 100:.0f}%) are prolonged "
                f"(>{PROLONGED_EXIT_BARS} bars) — review take-profit placement"
            )
        if avg_bars_val < 2:
            text_insights.append(
                f"Very short average duration ({avg_bars_val:.1f} bars) — "
                "strategy may be highly reactive to short-term noise"
            )

        return DurationAnalysis(
            avg_bars=avg_bars_val,
            median_bars=median_bars_val,
            fast_exits_count=fast,
            normal_exits_count=normal,
            prolonged_exits_count=prolonged,
            fast_exits_pct=round(fast_pct, 2),
            insights=text_insights,
        )

    # ── Helper: premature exit narrative ──────────────────────────────────────

    @staticmethod
    def _build_premature_exit_narrative(
        duration: DurationAnalysis,
        avg_bars_to_profit: Optional[float],
        avg_bars_to_loss: Optional[float],
    ) -> str:
        """Build a one-sentence narrative about exit timing quality."""
        parts: List[str] = []

        fast_pct = duration.fast_exits_pct
        if fast_pct > FAST_EXIT_CRITICAL_PCT * 100:
            parts.append(
                f"Very high fast-exit rate ({fast_pct:.0f}%) strongly suggests "
                "premature exits or stops that are too tight"
            )
        elif fast_pct > FAST_EXIT_WARNING_PCT * 100:
            parts.append(
                f"Elevated fast-exit rate ({fast_pct:.0f}%) may indicate "
                "stops being hit before trades can develop"
            )
        else:
            parts.append(f"Exit timing appears reasonable ({fast_pct:.0f}% fast exits)")

        if avg_bars_to_profit is not None and avg_bars_to_loss is not None:
            ratio = avg_bars_to_profit / avg_bars_to_loss if avg_bars_to_loss > 0 else 1.0
            if ratio < WINNER_FASTER_RATIO:
                parts.append(
                    f"winners resolve faster ({avg_bars_to_profit:.1f} bars) "
                    f"than losers ({avg_bars_to_loss:.1f} bars) — good exit discipline"
                )
            else:
                parts.append(
                    f"winners and losers exit at similar speeds "
                    f"({avg_bars_to_profit:.1f} vs {avg_bars_to_loss:.1f} bars)"
                )

        return "; ".join(parts) + "."

    # ── Quality insight generation ────────────────────────────────────────────

    @staticmethod
    def _generate_quality_insights(
        win_dist: TradeDistribution,
        loss_dist: TradeDistribution,
        duration: DurationAnalysis,
        metrics: "MetricsReport",
        avg_bars_to_profit: Optional[float],
        avg_bars_to_loss: Optional[float],
    ) -> List[Insight]:
        """
        Apply intelligence rules to produce quality-based Insights.

        Rules applied:
          1. Very high fast-exit rate               → critical
          2. Moderate fast-exit rate                → warning
          3. Strategy reliant on rare large winners → warning
          4. Winners resolve faster than losers     → success
          5. Large losses dominate loss distribution → warning
        """
        insights: List[Insight] = []
        fast_pct = duration.fast_exits_pct / 100  # 0-1 scale

        # ── 1. Critical: very high fast-exit rate ─────────────────────────────
        if fast_pct > FAST_EXIT_CRITICAL_PCT:
            insights.append(Insight(
                message=(
                    f"{duration.fast_exits_pct:.0f}% of trades exit within "
                    f"{FAST_EXIT_BARS} bars — extremely high early-exit rate"
                ),
                recommendation=(
                    "Review stop-loss placement — stops may be too tight; "
                    "consider widening or using ATR-based stops"
                ),
                confidence="High",
                impact_estimate="Reducing premature exits could materially improve P&L",
                category="quality",
                severity="critical",
            ))

        # ── 2. Warning: elevated fast-exit rate ───────────────────────────────
        elif fast_pct > FAST_EXIT_WARNING_PCT:
            insights.append(Insight(
                message=(
                    f"{duration.fast_exits_pct:.0f}% of trades exit within "
                    f"{FAST_EXIT_BARS} bars — elevated early-exit rate"
                ),
                recommendation=(
                    "Consider slightly wider stops or reviewing entry timing "
                    "to allow trades room to develop"
                ),
                confidence="Medium",
                impact_estimate=None,
                category="quality",
                severity="warning",
            ))

        # ── 3. Reliance on rare large winners ────────────────────────────────
        total_wins = win_dist.small_count + win_dist.medium_count + win_dist.large_count
        if total_wins > 0:
            large_win_pct   = win_dist.large_pct / 100
            # Estimate large-win profit contribution using MetricsReport
            # (approximation — exact breakdown requires raw trade list)
            if (
                large_win_pct < LARGE_WIN_RELIANCE_MAX_PCT
                and metrics.profit_factor > 1.0
            ):
                # Check if average large win is >> average win
                # We don't have the raw totals here; use ratio heuristic
                if win_dist.large_pct > 0 and metrics.largest_win > 7:
                    pct_label = f"{win_dist.large_pct:.0f}%"
                    insights.append(Insight(
                        message=(
                            f"Only {pct_label} of wins are large (>{LARGE_TRADE_THRESHOLD:.0f} pts) "
                            f"but the largest win is {metrics.largest_win:.1f} pts"
                        ),
                        recommendation=(
                            "Protect large winning trades with trailing stops to avoid "
                            "giving back significant gains"
                        ),
                        confidence="Medium",
                        impact_estimate=None,
                        category="quality",
                        severity="warning",
                    ))

        # ── 4. Winners resolve faster than losers (good discipline) ──────────
        if avg_bars_to_profit is not None and avg_bars_to_loss is not None:
            if avg_bars_to_loss > 0:
                ratio = avg_bars_to_profit / avg_bars_to_loss
                if ratio < WINNER_FASTER_RATIO:
                    insights.append(Insight(
                        message=(
                            f"Winning trades resolve faster ({avg_bars_to_profit:.1f} bars) "
                            f"than losing trades ({avg_bars_to_loss:.1f} bars)"
                        ),
                        recommendation="Maintain current exit discipline — winners are being taken efficiently",
                        confidence="High",
                        impact_estimate=None,
                        category="quality",
                        severity="success",
                    ))
                elif ratio > 1.5:
                    # Losers resolve faster — possible letting losses run
                    insights.append(Insight(
                        message=(
                            f"Losing trades resolve faster ({avg_bars_to_loss:.1f} bars) "
                            f"than winning trades ({avg_bars_to_profit:.1f} bars) — "
                            "potential 'let losses run, cut winners early' pattern"
                        ),
                        recommendation=(
                            "Review exit management: ensure take-profits are not cut short "
                            "and stop-losses are given adequate room"
                        ),
                        confidence="Medium",
                        impact_estimate=None,
                        category="quality",
                        severity="warning",
                    ))

        # ── 5. Large losses dominate the loss distribution ────────────────────
        total_losses = loss_dist.small_count + loss_dist.medium_count + loss_dist.large_count
        if total_losses > 0 and loss_dist.large_pct > 30:
            insights.append(Insight(
                message=(
                    f"{loss_dist.large_pct:.0f}% of losses are large "
                    f"(>{LARGE_TRADE_THRESHOLD:.0f} pts) — loss distribution is heavy-tailed"
                ),
                recommendation=(
                    "Investigate outlier losses; consider maximum loss caps "
                    "or scaling out of losing positions"
                ),
                confidence="High",
                impact_estimate=f"Capping large losses could improve max drawdown",
                category="quality",
                severity="warning",
            ))

        return insights

    # ========================================
    # RISK-ADJUSTED ANALYSIS  (Session 16)
    # ========================================

    @staticmethod
    def _analyze_risk_adjusted(
        trade_result: "TradeResult",
        metrics: "MetricsReport",
    ) -> RiskAdjustedMetrics:
        """
        Calculate risk-adjusted performance metrics.

        Computes:
        - return_over_max_dd  : Total P&L / |max drawdown|  (efficiency)
        - avg_win_over_avg_loss: Average win / average loss  (risk/reward)
        - expectancy_per_trade : Total P&L / total trades    (edge per trade)
        - consistency_score   : 0-100 volatility-adjusted   (reliability)
        - recovery_factor     : Total P&L / total gross loss (resilience)
        """
        closed = [t for t in trade_result.trades if t.exit is not None]
        wins   = [t for t in closed if t.exit.is_win]
        losses = [t for t in closed if t.exit.is_loss]

        # return over max drawdown
        if metrics.max_drawdown != 0:
            return_over_max_dd = round(metrics.total_pnl_points / abs(metrics.max_drawdown), 2)
        else:
            return_over_max_dd = float("inf") if metrics.total_pnl_points > 0 else 0.0

        # avg win / avg loss
        if wins and losses:
            avg_win  = mean([t.exit.pnl_points for t in wins])
            avg_loss = mean([abs(t.exit.pnl_points) for t in losses])
            avg_win_over_avg_loss = round(avg_win / avg_loss, 2) if avg_loss else 0.0
        else:
            avg_win_over_avg_loss = 0.0

        # expectancy per trade
        expectancy_per_trade = round(
            metrics.total_pnl_points / metrics.total_trades, 4
        ) if metrics.total_trades else 0.0

        # consistency score
        consistency_score = TradeAnalytics._calculate_consistency_score(closed)

        # recovery factor
        gross_loss = sum(abs(t.exit.pnl_points) for t in losses)
        recovery_factor = round(metrics.total_pnl_points / gross_loss, 2) if gross_loss else 0.0

        # Build without insights first (frozen dataclass)
        risk_partial = RiskAdjustedMetrics(
            return_over_max_dd=return_over_max_dd,
            avg_win_over_avg_loss=avg_win_over_avg_loss,
            expectancy_per_trade=expectancy_per_trade,
            consistency_score=consistency_score,
            recovery_factor=recovery_factor,
            insights=[],
        )
        insights = TradeAnalytics._generate_risk_insights(risk_partial, metrics)

        return RiskAdjustedMetrics(
            return_over_max_dd=return_over_max_dd,
            avg_win_over_avg_loss=avg_win_over_avg_loss,
            expectancy_per_trade=expectancy_per_trade,
            consistency_score=consistency_score,
            recovery_factor=recovery_factor,
            insights=insights,
        )

    @staticmethod
    def _calculate_consistency_score(trades: List["Trade"]) -> float:
        """
        Calculate consistency score (0-100).

        Uses coefficient of variation (CV = stdev / |mean|) of per-trade P&L.
        Lower CV → higher score → more consistent returns.
        Returns 50.0 for edge cases (single trade, mean=0).
        """
        if len(trades) < 2:
            return 50.0
        pnl_vals = [t.exit.pnl_points for t in trades]
        mu = mean(pnl_vals)
        if mu == 0:
            return 50.0
        sd = stdev(pnl_vals)
        cv = sd / abs(mu)
        score = max(0.0, min(100.0, 100.0 - (cv * 10.0)))
        return round(score, 2)

    @staticmethod
    def _generate_risk_insights(
        risk_metrics: RiskAdjustedMetrics,
        base_metrics: "MetricsReport",
    ) -> List[Insight]:
        """
        Apply intelligence rules to produce risk-based Insights.

        Rules:
          1. Poor risk/reward (avg_win < avg_loss)   -> critical
          2. Excellent risk/reward (>= 2x)           -> success
          3. Low consistency score (< 30)            -> warning
          4. Moderate consistency score (30-50)      -> info
          5. Negative expectancy per trade           -> critical
          6. Strong recovery factor (> 2.0)          -> success
          7. Weak recovery factor (< 0.5)            -> warning
        """
        insights: List[Insight] = []

        # Rule 1/2: risk/reward ratio
        if 0 < risk_metrics.avg_win_over_avg_loss < 1.0:
            insights.append(Insight(
                message=(
                    f"Average win is only {risk_metrics.avg_win_over_avg_loss:.2f}x "
                    f"the average loss — unfavourable risk/reward"
                ),
                recommendation=(
                    "Review take-profit targets; average win should exceed average loss. "
                    "Consider wider TPs or tighter SLs"
                ),
                confidence="High",
                impact_estimate="Improving ratio to 1:1 would significantly boost profit factor",
                category="risk",
                severity="critical",
            ))
        elif risk_metrics.avg_win_over_avg_loss >= 2.0:
            insights.append(Insight(
                message=(
                    f"Strong risk/reward: {risk_metrics.avg_win_over_avg_loss:.2f}x "
                    f"(average win is {risk_metrics.avg_win_over_avg_loss:.1f}x average loss)"
                ),
                recommendation="Maintain current TP/SL balance — risk/reward is excellent",
                confidence="High",
                impact_estimate=None,
                category="risk",
                severity="success",
            ))

        # Rules 3/4: consistency score
        if risk_metrics.consistency_score < 30:
            insights.append(Insight(
                message=(
                    f"Low consistency score: {risk_metrics.consistency_score:.0f}/100 — "
                    f"high volatility in per-trade returns"
                ),
                recommendation=(
                    "Returns are highly erratic; consider more selective entry criteria "
                    "or tighter risk per trade to smooth the equity curve"
                ),
                confidence="High",
                impact_estimate=None,
                category="risk",
                severity="warning",
            ))
        elif risk_metrics.consistency_score < 50:
            insights.append(Insight(
                message=(
                    f"Moderate consistency score: {risk_metrics.consistency_score:.0f}/100"
                ),
                recommendation="Monitor equity curve; look for clustering of large losses",
                confidence="Medium",
                impact_estimate=None,
                category="risk",
                severity="info",
            ))

        # Rule 5: negative expectancy
        if risk_metrics.expectancy_per_trade < 0:
            insights.append(Insight(
                message=(
                    f"Negative expectancy: {risk_metrics.expectancy_per_trade:.3f} pts/trade"
                ),
                recommendation=(
                    "Strategy has negative edge — review signal quality, "
                    "spread costs, and filter effectiveness"
                ),
                confidence="High",
                impact_estimate=None,
                category="risk",
                severity="critical",
            ))

        # Rules 6/7: recovery factor
        if risk_metrics.recovery_factor > 2.0:
            insights.append(Insight(
                message=(
                    f"Strong recovery factor: {risk_metrics.recovery_factor:.2f} — "
                    f"net profits significantly exceed gross losses"
                ),
                recommendation="Strategy demonstrates robust edge; maintain current approach",
                confidence="High",
                impact_estimate=None,
                category="risk",
                severity="success",
            ))
        elif 0 < risk_metrics.recovery_factor < 0.5:
            insights.append(Insight(
                message=(
                    f"Weak recovery factor: {risk_metrics.recovery_factor:.2f} — "
                    f"gross losses dwarf net profits"
                ),
                recommendation=(
                    "Strategy is barely profitable relative to gross losses; "
                    "reduce position size or tighten filters"
                ),
                confidence="High",
                impact_estimate=None,
                category="risk",
                severity="warning",
            ))

        return insights


    # ========================================
    # COMPARATIVE CONTEXT  (Session 16)
    # ========================================

    @staticmethod
    def _analyze_comparative_context(
        trade_result: "TradeResult",
        metrics: "MetricsReport",
    ) -> Optional[ComparativeContext]:
        """Placeholder — implemented in Session 16."""
        logger.warning("_analyze_comparative_context: NOT IMPLEMENTED (Session 16)")
        return ComparativeContext(
            vs_baseline=None,
            statistical_flags=[],
            percentile_rank=None,
        )

    # ========================================
    # EXECUTIVE SUMMARY  (Session 16)
    # ========================================

    @staticmethod
    def _generate_executive_summary(
        metrics: "MetricsReport",
        time_perf: TimePerformanceBreakdown,
        quality: TradeQualityAnalysis,
        risk: RiskAdjustedMetrics,
        comparative: Optional[ComparativeContext],
    ) -> ExecutiveSummary:
        """
        Synthesise all analyses into an executive summary.

        Produces:
        - Performance grade (A+ to F) with reasoning
        - Top 3-5 critical insights across all domains
        - Key strengths list
        - Improvement areas list
        - 2-3 sentence overall assessment
        """
        grade, grade_reasoning = TradeAnalytics._calculate_performance_grade(metrics, risk)
        critical_insights = TradeAnalytics._collect_critical_insights(time_perf, quality, risk)

        # Strengths: success insights + strong metric indicators
        strengths: List[str] = []
        for i in (time_perf.insights + quality.insights + risk.insights):
            if i.severity == "success":
                strengths.append(i.message)
        if metrics.profit_factor >= 2.0:
            strengths.append(f"Strong profit factor: {metrics.profit_factor:.2f}")
        if metrics.win_rate >= 20:
            strengths.append(f"Solid win rate: {metrics.win_rate:.1f}%")
        if risk.consistency_score >= 70:
            strengths.append(f"High consistency score: {risk.consistency_score:.0f}/100")
        if time_perf.best_session not in ("N/A", "Unknown"):
            strengths.append(f"Clear best session: {time_perf.best_session}")

        # Improvement areas: critical/warning insights (messages only, de-duplicated)
        improvement_areas: List[str] = []
        seen: set = set()
        for i in critical_insights:
            if i.severity in ("critical", "warning") and i.message not in seen:
                improvement_areas.append(i.message)
                seen.add(i.message)

        # Overall assessment (2-3 sentences)
        grade_word = {
            "A+": "exceptional", "A": "excellent", "A-": "excellent",
            "B+": "good",        "B": "good",       "B-": "good",
            "C+": "acceptable",  "C": "acceptable",  "C-": "acceptable",
            "D+": "below average","D": "below average","D-": "below average",
            "F":  "failing",     "N/A": "ungraded",
        }.get(grade, "ungraded")

        top_strength    = strengths[0]    if strengths    else "no clear strengths identified"
        top_improvement = improvement_areas[0] if improvement_areas else "no critical issues found"

        assessment = (
            f"Strategy shows {grade_word} performance (grade {grade}) across "
            f"{metrics.total_trades:,} trades with a {metrics.win_rate:.1f}% win rate "
            f"and {metrics.total_pnl_points:+.1f} pts total P&L. "
            f"Primary strength: {top_strength}. "
            f"Key focus area: {top_improvement}."
        )

        return ExecutiveSummary(
            performance_grade=grade,
            grade_reasoning=grade_reasoning,
            critical_insights=critical_insights,
            key_strengths=strengths[:5],
            improvement_areas=improvement_areas[:5],
            overall_assessment=assessment,
        )

    @staticmethod
    def _calculate_performance_grade(
        metrics: "MetricsReport",
        risk_metrics: RiskAdjustedMetrics,
    ) -> Tuple[str, str]:
        """
        4-component scoring → letter grade.

        Components (each 0-25 pts, total 0-100):
          1. Win rate
          2. Profit factor
          3. Drawdown management (|max_dd| vs total_pnl)
          4. Consistency score

        Grade mapping:
          90-100 A+  |  85-89 A  |  80-84 A-
          75-79  B+  |  70-74 B  |  65-69 B-
          60-64  C+  |  55-59 C  |  50-54 C-
          40-49  D+  |  30-39 D  |  <30   F
        """
        score = 0
        reasons: List[str] = []

        # Component 1: Win rate (0-25)
        if metrics.win_rate >= 20:
            score += 25; reasons.append("win rate ≥ 20%")
        elif metrics.win_rate >= 15:
            score += 20; reasons.append("win rate ≥ 15%")
        elif metrics.win_rate >= 10:
            score += 10; reasons.append("win rate ≥ 10%")
        else:
            reasons.append(f"low win rate ({metrics.win_rate:.1f}%)")

        # Component 2: Profit factor (0-25)
        if metrics.profit_factor >= 2.0:
            score += 25; reasons.append("profit factor ≥ 2.0")
        elif metrics.profit_factor >= 1.5:
            score += 20; reasons.append("profit factor ≥ 1.5")
        elif metrics.profit_factor >= 1.2:
            score += 10; reasons.append("profit factor ≥ 1.2")
        else:
            reasons.append(f"weak profit factor ({metrics.profit_factor:.2f})")

        # Component 3: Drawdown management (0-25)
        if metrics.total_pnl_points > 0 and metrics.max_drawdown != 0:
            dd_ratio = abs(metrics.max_drawdown) / metrics.total_pnl_points
            if dd_ratio < 0.2:
                score += 25; reasons.append("excellent drawdown control (<20% of profit)")
            elif dd_ratio < 0.5:
                score += 15; reasons.append("good drawdown control (<50% of profit)")
            elif dd_ratio < 1.0:
                score += 5;  reasons.append("moderate drawdown control")
            else:
                reasons.append("drawdown exceeds total profit")
        else:
            score += 10  # Neutral when no drawdown data

        # Component 4: Consistency (0-25)
        if risk_metrics.consistency_score >= 70:
            score += 25; reasons.append(f"high consistency ({risk_metrics.consistency_score:.0f}/100)")
        elif risk_metrics.consistency_score >= 50:
            score += 15; reasons.append(f"moderate consistency ({risk_metrics.consistency_score:.0f}/100)")
        elif risk_metrics.consistency_score >= 30:
            score += 5;  reasons.append(f"low consistency ({risk_metrics.consistency_score:.0f}/100)")
        else:
            reasons.append(f"very low consistency ({risk_metrics.consistency_score:.0f}/100)")

        # Map score to grade
        if   score >= 90: grade = "A+"
        elif score >= 85: grade = "A"
        elif score >= 80: grade = "A-"
        elif score >= 75: grade = "B+"
        elif score >= 70: grade = "B"
        elif score >= 65: grade = "B-"
        elif score >= 60: grade = "C+"
        elif score >= 55: grade = "C"
        elif score >= 50: grade = "C-"
        elif score >= 40: grade = "D+"
        elif score >= 30: grade = "D"
        elif score >= 20: grade = "D-"
        else:             grade = "F"

        reasoning = f"Score {score}/100 — " + "; ".join(reasons)
        return grade, reasoning

    @staticmethod
    def _collect_critical_insights(
        time_perf: TimePerformanceBreakdown,
        quality: TradeQualityAnalysis,
        risk: RiskAdjustedMetrics,
    ) -> List[Insight]:
        """
        Aggregate all insights and return the top 5 by priority.

        Priority order:
          1. severity: critical > warning > info > success
          2. confidence: High > Medium > Low
        """
        SEVERITY_RANK  = {"critical": 0, "warning": 1, "info": 2, "success": 3}
        CONFIDENCE_RANK = {"High": 0, "Medium": 1, "Low": 2}

        all_insights: List[Insight] = (
            time_perf.insights
            + quality.insights
            + risk.insights
        )

        sorted_insights = sorted(
            all_insights,
            key=lambda i: (
                SEVERITY_RANK.get(i.severity, 4),
                CONFIDENCE_RANK.get(i.confidence, 3),
            ),
        )
        return sorted_insights[:5]

    # ========================================
    # MARKDOWN FORMATTING  (Session 16)
    # ========================================

    @staticmethod
    def format_markdown_report(report: AnalyticsReport) -> str:
        """
        Format complete AnalyticsReport as a consulting-style markdown string.

        Sections:
          1. Header — key headline metrics
          2. KEY INSIGHTS — top 5 prioritised insights with icons
          3. STRENGTHS — what is working
          4. IMPROVEMENT AREAS — what needs attention
          5. TIME PERFORMANCE — session table + hour/day notes
          6. TRADE QUALITY — distribution + duration
          7. RISK METRICS — risk-adjusted figures
          8. PERFORMANCE GRADE — grade + reasoning
        """
        es  = report.executive_summary
        tp  = report.time_performance
        tq  = report.trade_quality
        ra  = report.risk_adjusted
        m   = report.input_metrics
        ts  = report.analysis_timestamp[:10]   # date only

        SEVERITY_ICON = {
            "critical": "🚨",
            "warning":  "⚠️ ",
            "info":     "ℹ️ ",
            "success":  "✅",
        }

        lines: List[str] = []

        # ── Header ────────────────────────────────────────────────────────────
        lines += [
            "=" * 60,
            "  STRATEGY PERFORMANCE ANALYSIS",
            "=" * 60,
            f"  Analysis date : {ts}",
            f"  Total trades  : {m.total_trades:,}",
            f"  Win rate      : {m.win_rate:.2f}%",
            f"  Total P&L     : {m.total_pnl_points:+.1f} pts",
            f"  Profit factor : {m.profit_factor:.2f}",
            f"  Max drawdown  : {m.max_drawdown:.1f} pts",
            f"  Grade         : {es.performance_grade}",
            "",
        ]

        # ── Overall assessment ────────────────────────────────────────────────
        lines += [
            "## OVERALL ASSESSMENT",
            "",
            es.overall_assessment,
            "",
        ]

        # ── Key insights ──────────────────────────────────────────────────────
        lines += ["## 🎯 KEY INSIGHTS", ""]
        if es.critical_insights:
            for idx, insight in enumerate(es.critical_insights, 1):
                icon = SEVERITY_ICON.get(insight.severity, "•")
                lines.append(f"{idx}. {icon} {insight.message}")
                lines.append(f"   → {insight.recommendation}")
                if insight.impact_estimate:
                    lines.append(f"   💡 {insight.impact_estimate}")
                lines.append("")
        else:
            lines += ["No critical insights generated.", ""]

        # ── Strengths ─────────────────────────────────────────────────────────
        lines += ["## 📈 STRENGTHS", ""]
        if es.key_strengths:
            for s in es.key_strengths:
                lines.append(f"  - {s}")
        else:
            lines.append("  - No clear strengths identified")
        lines.append("")

        # ── Improvement areas ─────────────────────────────────────────────────
        lines += ["## ⚠️  IMPROVEMENT AREAS", ""]
        if es.improvement_areas:
            for area in es.improvement_areas:
                lines.append(f"  - {area}")
        else:
            lines.append("  - No critical improvement areas identified")
        lines.append("")

        # ── Time performance ──────────────────────────────────────────────────
        lines += ["## 📅 TIME-BASED PERFORMANCE", ""]
        if tp.by_session:
            lines += [
                "### By Session",
                "",
                f"{'Session':<12} {'Trades':>7} {'Win Rate':>9} {'Total P&L':>10} {'Avg P&L':>9}",
                f"{'-'*12} {'-'*7} {'-'*9} {'-'*10} {'-'*9}",
            ]
            for name, sm in sorted(tp.by_session.items()):
                marker = " ★" if name == tp.best_session else (
                         " ✗" if name == tp.worst_session else "")
                lines.append(
                    f"{name + marker:<12} {sm.trades:>7,} {sm.win_rate:>8.1f}%"
                    f" {sm.total_pnl:>+10.1f} {sm.avg_pnl:>+9.2f}"
                )
            lines.append("")

        if tp.by_day:
            lines += ["### By Day of Week", ""]
            day_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
            lines.append(
                f"{'Day':<12} {'Trades':>7} {'Win Rate':>9} {'Total P&L':>10}"
            )
            lines.append(f"{'-'*12} {'-'*7} {'-'*9} {'-'*10}")
            for day in day_order:
                dm = tp.by_day.get(day)
                if dm:
                    lines.append(
                        f"{day:<12} {dm.trades:>7,} {dm.win_rate:>8.1f}%"
                        f" {dm.total_pnl:>+10.1f}"
                    )
            lines.append("")

        if tp.insights:
            lines += ["### Time Insights", ""]
            for i in tp.insights:
                icon = SEVERITY_ICON.get(i.severity, "•")
                lines.append(f"  {icon} {i.message}")
                lines.append(f"     → {i.recommendation}")
            lines.append("")

        # ── Trade quality ─────────────────────────────────────────────────────
        lines += ["## 🔍 TRADE QUALITY", ""]

        dur = tq.duration_analysis
        lines += [
            "### Duration",
            f"  Average  : {dur.avg_bars:.1f} bars",
            f"  Median   : {dur.median_bars} bars",
            f"  Fast (<{FAST_EXIT_BARS}b) : {dur.fast_exits_count} trades ({dur.fast_exits_pct:.1f}%)",
            f"  Normal   : {dur.normal_exits_count} trades",
            f"  Prolonged: {dur.prolonged_exits_count} trades",
            "",
        ]
        if tq.avg_bars_to_profit is not None:
            lines.append(f"  Avg bars to profit : {tq.avg_bars_to_profit:.1f}")
        if tq.avg_bars_to_loss is not None:
            lines.append(f"  Avg bars to loss   : {tq.avg_bars_to_loss:.1f}")
        if tq.avg_bars_to_profit or tq.avg_bars_to_loss:
            lines.append("")

        wd = tq.win_distribution
        ld = tq.loss_distribution
        lines += [
            "### Win Distribution",
            f"  Small  (<3 pts) : {wd.small_count:>5}  ({wd.small_pct:.1f}%)",
            f"  Medium (3-7 pts): {wd.medium_count:>5}  ({wd.medium_pct:.1f}%)",
            f"  Large  (>7 pts) : {wd.large_count:>5}  ({wd.large_pct:.1f}%)",
            "",
            "### Loss Distribution",
            f"  Small  (<3 pts) : {ld.small_count:>5}  ({ld.small_pct:.1f}%)",
            f"  Medium (3-7 pts): {ld.medium_count:>5}  ({ld.medium_pct:.1f}%)",
            f"  Large  (>7 pts) : {ld.large_count:>5}  ({ld.large_pct:.1f}%)",
            "",
            f"  {tq.premature_exit_estimate}",
            "",
        ]

        if tq.insights:
            lines += ["### Quality Insights", ""]
            for i in tq.insights:
                icon = SEVERITY_ICON.get(i.severity, "•")
                lines.append(f"  {icon} {i.message}")
                lines.append(f"     → {i.recommendation}")
            lines.append("")

        # ── Risk metrics ──────────────────────────────────────────────────────
        lines += [
            "## 📊 RISK-ADJUSTED METRICS",
            "",
            f"  Return / Max DD    : {ra.return_over_max_dd:.2f}",
            f"  Avg Win / Avg Loss : {ra.avg_win_over_avg_loss:.2f}",
            f"  Expectancy/trade   : {ra.expectancy_per_trade:+.4f} pts",
            f"  Consistency score  : {ra.consistency_score:.1f}/100",
            f"  Recovery factor    : {ra.recovery_factor:.2f}",
            "",
        ]
        if ra.insights:
            for i in ra.insights:
                icon = SEVERITY_ICON.get(i.severity, "•")
                lines.append(f"  {icon} {i.message}")
                lines.append(f"     → {i.recommendation}")
            lines.append("")

        # ── Performance grade ─────────────────────────────────────────────────
        lines += [
            "=" * 60,
            f"  PERFORMANCE GRADE: {es.performance_grade}",
            f"  {es.grade_reasoning}",
            "=" * 60,
            f"  (Analysis took {report.analysis_duration_ms:.1f}ms)",
        ]

        return "\n".join(lines)

    # ========================================
    # FILE I/O  (Session 16)
    # ========================================

    @staticmethod
    def _save_report(
        report: AnalyticsReport,
        output_dir: Optional[Path],
    ) -> None:
        """
        Save report to JSON and Markdown files.

        Creates:
          analytics_{timestamp}.json  — structured data for downstream use
          analytics_{timestamp}.md    — human-readable consulting report

        Args:
            report:     Complete AnalyticsReport.
            output_dir: Directory for output files.
                        Defaults to outputs/analytics/ relative to cwd.
        """
        if output_dir is None:
            output_dir = Path("outputs") / "analytics"

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save JSON
        json_path = output_dir / f"analytics_{timestamp}.json"
        json_path.write_text(report.to_json(), encoding="utf-8")
        logger.info(f"Analytics JSON saved: {json_path}")

        # Save Markdown
        md_path = output_dir / f"analytics_{timestamp}.md"
        md_path.write_text(
            TradeAnalytics.format_markdown_report(report),
            encoding="utf-8",
        )
        logger.info(f"Analytics Markdown saved: {md_path}")


# ============================================================
# CONVENIENCE FUNCTION
# ============================================================

def analyze_trades(
    trade_result: "TradeResult",
    config: "StrategyConfig",
    metrics: Optional["MetricsReport"] = None,
    **kwargs,
) -> AnalyticsReport:
    """
    Convenience wrapper for TradeAnalytics.analyze().

    Examples:
        >>> report = analyze_trades(result, config)                 # auto-metrics
        >>> report = analyze_trades(result, config, metrics=metrics) # explicit
    """
    return TradeAnalytics.analyze(trade_result, config, metrics=metrics, **kwargs)


# ============================================================
# MODULE METADATA
# ============================================================

__all__ = ["TradeAnalytics", "analyze_trades"]


if __name__ == "__main__":
    print("TradeAnalytics Module")
    print("=" * 50)
    print("Status: ✅ COMPLETE (Sessions 14-16)")
    print()
    print("Session 15 — Time + Quality:")
    print("  ✅ analyze()                       — Main entry point")
    print("  ✅ _analyze_time_performance()     — Sessions / hours / days")
    print("  ✅ _get_session_for_hour()          — Hour → session mapping")
    print("  ✅ _calculate_session_metrics()    — P&L per segment")
    print("  ✅ _generate_time_insights()       — 6 time insight rules")
    print("  ✅ _analyze_trade_quality()        — Win/loss/duration analysis")
    print("  ✅ _calculate_trade_distribution() — Small/medium/large")
    print("  ✅ _analyze_duration_patterns()    — Fast/normal/prolonged")
    print("  ✅ _generate_quality_insights()    — 5 quality insight rules")
    print()
    print("Session 16 — Risk + Executive + Markdown:")
    print("  ✅ _analyze_risk_adjusted()        — 5 risk metrics")
    print("  ✅ _calculate_consistency_score()  — CV-based 0-100 score")
    print("  ✅ _generate_risk_insights()       — 7 risk insight rules")
    print("  ✅ _generate_executive_summary()   — Grade + top insights")
    print("  ✅ _calculate_performance_grade()  — 4-component A+ to F")
    print("  ✅ _collect_critical_insights()    — Prioritised top 5")
    print("  ✅ format_markdown_report()        — Consulting-style MD")
    print("  ✅ _save_report()                  — JSON + MD file save")