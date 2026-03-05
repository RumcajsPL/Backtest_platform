"""
wfo/wfo_evaluator.py
--------------------
Evaluates a single candidate on a single WFO window.

Single responsibility: one candidate × one window → WFOWindowResult.
This module is called by wfo_engine.py in both lightweight (GA) and full (Stage 4) modes.
It is also the innermost callable dispatched to worker processes — it must NEVER raise.
All failures surface as WFOWindowResult with error set and fitness_score=None.

Block 8 fix (B8B-018): Two _safe_float field name mismatches corrected:
  - "net_pnl"   → "total_pnl_points"  (MetricsReport has no 'net_pnl' attribute)
  - "expectancy" → "expectancy_points" (MetricsReport has no 'expectancy' attribute)
Both fields were silently None on every window evaluation, causing:
  - WFO median_return_norm and fraction_positive_windows permanently zeroed
  - WFO composite scores systematically understated on those two sub-metrics

Block 9E fix (B8B-005): IS/OOS split implemented.
  - Each window is split 70/30 (IS/OOS) by calendar days.
  - IS evaluation covers [window.start_date, is_end_date).
  - OOS evaluation covers [is_end_date, window.end_date].
  - oos_delta = oos_fitness_score - is_fitness_score  (both in [0, 1]).
    Negative = OOS underperforms IS.
  - If either IS or OOS evaluation fails the significance guard or returns an error,
    oos_delta is set to None (gate cannot fire on incomplete data — safe default).
  - Splitting is only performed when oos_gate_enabled context is available.
    Without it (lightweight GA mode), the full window is evaluated as before and
    oos_delta remains None — the gate never fires in GA mode by design.
  - The main fitness_score and all metrics fields are taken from the FULL window
    evaluation (not IS or OOS alone) so that GA and WFO consistency scoring use
    the same metric basis as the full-pipeline evaluation.
"""
from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Optional

from src.backtesting.contracts import (
    CandidateParameterSet,
    ScenarioProfile,
    WFOWindow,
    WFOWindowResult,
)
from src.backtesting.strategy_runner import evaluate as _evaluate_candidate
from src.backtesting.fitness import evaluate_fitness

logger = logging.getLogger(__name__)

# IS/OOS split ratio — fraction of window assigned to in-sample period.
# Standard WFO convention: 70% IS, 30% OOS.
# Applied to calendar-day span of the window (not trading days, to avoid
# requiring a trading-day calendar).
_IS_FRACTION: float = 0.70


def evaluate_window(
    candidate: CandidateParameterSet,
    window: WFOWindow,
    base_yaml_path: Path,
    temp_dir: Path,
    scenario: ScenarioProfile,
    min_significant_trades: int = 30,
    oos_gate_enabled: bool = False,
) -> WFOWindowResult:
    """
    Evaluate one candidate on one WFO window.

    Injects the window's date range into the candidate evaluation by passing
    date_override params to strategy_runner. The runner builds a temp YAML
    scoped to [window.start_date, window.end_date].

    When oos_gate_enabled=True, additionally evaluates IS and OOS sub-periods
    and populates oos_delta = oos_fitness - is_fitness.

    Args:
        candidate:               The candidate to evaluate.
        window:                  The WFO window (date range) to evaluate within.
        base_yaml_path:          Path to base strategy_template.yaml.
        temp_dir:                Directory for temp per-candidate YAMLs.
        scenario:                Active scenario profile (for fitness computation).
        min_significant_trades:  Significance guard threshold.
        oos_gate_enabled:        When True, run IS+OOS sub-evaluations to populate
                                 oos_delta. When False (default), oos_delta=None.

    Returns:
        WFOWindowResult — always. fitness_score is None and error is set on failure.
    """
    try:
        # ── Full-window evaluation ─────────────────────────────────────────────
        # fitness_score and all metrics use the full window so that GA lightweight
        # mode and full WFO mode share the same metric basis.
        candidate_result = _evaluate_candidate(
            candidate=candidate,
            base_yaml_path=base_yaml_path,
            temp_dir=temp_dir,
            min_significant_trades=min_significant_trades,
            date_start=window.start_date,
            date_end=window.end_date,
        )

        if not candidate_result.is_valid:
            return WFOWindowResult(
                candidate_id=candidate.candidate_id,
                window_id=window.window_id,
                evaluated_at=datetime.now(UTC),
                fitness_score=None,
                total_trades=candidate_result.total_trades,
                net_pnl=None,
                max_drawdown=None,
                win_rate=None,
                expectancy=None,
                profit_factor=None,
                oos_delta=None,
                error=candidate_result.error,
            )

        fitness_result = evaluate_fitness(candidate_result, scenario)
        m = candidate_result.metrics

        # ── IS/OOS delta (B8B-005) ─────────────────────────────────────────────
        oos_delta: Optional[float] = None
        if oos_gate_enabled:
            oos_delta = _compute_oos_delta(
                candidate=candidate,
                window=window,
                base_yaml_path=base_yaml_path,
                temp_dir=temp_dir,
                scenario=scenario,
                min_significant_trades=min_significant_trades,
            )

        return WFOWindowResult(
            candidate_id=candidate.candidate_id,
            window_id=window.window_id,
            evaluated_at=datetime.now(UTC),
            fitness_score=fitness_result.fitness_score,
            total_trades=candidate_result.total_trades,
            net_pnl=_safe_float(m, "total_pnl_points"),   # B8B-018: was "net_pnl"
            max_drawdown=_safe_float(m, "max_drawdown"),
            win_rate=_safe_float(m, "win_rate"),
            expectancy=_safe_float(m, "expectancy_points"),  # B8B-018: was "expectancy"
            profit_factor=_safe_float(m, "profit_factor"),
            oos_delta=oos_delta,
            error=None,
        )

    except Exception as exc:
        logger.error(
            "WFO window evaluation failed: candidate=%s window=%s error=%s",
            candidate.candidate_id[:12],
            window.window_id,
            exc,
            exc_info=True,
        )
        return WFOWindowResult(
            candidate_id=candidate.candidate_id,
            window_id=window.window_id,
            evaluated_at=datetime.now(UTC),
            fitness_score=None,
            total_trades=None,
            net_pnl=None,
            max_drawdown=None,
            win_rate=None,
            expectancy=None,
            profit_factor=None,
            oos_delta=None,
            error=str(exc),
        )


# ── Private helpers ────────────────────────────────────────────────────────────

def _compute_oos_delta(
    candidate: CandidateParameterSet,
    window: WFOWindow,
    base_yaml_path: Path,
    temp_dir: Path,
    scenario: ScenarioProfile,
    min_significant_trades: int,
) -> Optional[float]:
    """
    Compute oos_delta = oos_fitness - is_fitness for a single window.

    Splits the window into IS (first 70%) and OOS (last 30%) by calendar days.
    Evaluates the candidate on each sub-period independently.

    Returns None if either sub-evaluation fails (error set, significance guard
    triggered, or metrics unavailable). None is the safe default — a missing
    oos_delta cannot cause a false gate trigger.

    The IS period covers [window.start_date, is_end_date).
    The OOS period covers [is_end_date, window.end_date].
    Note: is_end_date is inclusive for the OOS start (OOS starts the day after IS ends)
    to ensure no calendar-day overlap. The strategy runner's date_start/date_end
    filtering handles boundary semantics.
    """
    total_days = (window.end_date - window.start_date).days
    if total_days < 2:
        # Window too short to split meaningfully — cannot compute delta
        logger.debug(
            "Window %s too short to split (%d days) — oos_delta=None",
            window.window_id, total_days,
        )
        return None

    is_days = max(1, int(total_days * _IS_FRACTION))
    # Guard: OOS must have at least 1 day
    if is_days >= total_days:
        is_days = total_days - 1

    is_end_date = window.start_date + timedelta(days=is_days)

    # ── IS evaluation ─────────────────────────────────────────────────────────
    is_result = _evaluate_candidate(
        candidate=candidate,
        base_yaml_path=base_yaml_path,
        temp_dir=temp_dir,
        min_significant_trades=min_significant_trades,
        date_start=window.start_date,
        date_end=is_end_date,
    )

    if not is_result.is_valid:
        logger.debug(
            "IS evaluation failed for candidate=%s window=%s — oos_delta=None: %s",
            candidate.candidate_id[:12], window.window_id, is_result.error,
        )
        return None

    is_fitness_result = evaluate_fitness(is_result, scenario)
    if not is_fitness_result.passed_constraints or is_fitness_result.fitness_score is None:
        logger.debug(
            "IS failed constraints for candidate=%s window=%s — oos_delta=None",
            candidate.candidate_id[:12], window.window_id,
        )
        return None

    # ── OOS evaluation ────────────────────────────────────────────────────────
    oos_result = _evaluate_candidate(
        candidate=candidate,
        base_yaml_path=base_yaml_path,
        temp_dir=temp_dir,
        min_significant_trades=min_significant_trades,
        date_start=is_end_date,
        date_end=window.end_date,
    )

    if not oos_result.is_valid:
        logger.debug(
            "OOS evaluation failed for candidate=%s window=%s — oos_delta=None: %s",
            candidate.candidate_id[:12], window.window_id, oos_result.error,
        )
        return None

    oos_fitness_result = evaluate_fitness(oos_result, scenario)
    if oos_fitness_result.fitness_score is None:
        # OOS failed constraints — fitness_score is None. This is informative
        # (OOS degraded enough to fail) but we need a numeric delta, not None.
        # Treat a constraint-failing OOS as fitness_score=0.0 (floor).
        oos_fitness = 0.0
        logger.debug(
            "OOS failed constraints for candidate=%s window=%s — using oos_fitness=0.0",
            candidate.candidate_id[:12], window.window_id,
        )
    else:
        oos_fitness = oos_fitness_result.fitness_score

    # oos_delta: negative = OOS underperforms IS
    delta = oos_fitness - is_fitness_result.fitness_score
    logger.debug(
        "IS/OOS delta: candidate=%s window=%s is_fitness=%.4f oos_fitness=%.4f delta=%.4f",
        candidate.candidate_id[:12],
        window.window_id,
        is_fitness_result.fitness_score,
        oos_fitness,
        delta,
    )
    return delta


def _safe_float(metrics_obj: object, attr: str) -> Optional[float]:
    """Return float attribute from metrics, or None if absent/None."""
    val = getattr(metrics_obj, attr, None)
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None