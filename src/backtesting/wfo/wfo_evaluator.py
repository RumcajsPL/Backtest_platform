"""
wfo/wfo_evaluator.py
--------------------
Evaluates a single candidate on a single WFO window.

Single responsibility: one candidate × one window → WFOWindowResult.
This module is called by wfo_engine.py in both lightweight (GA) and full (Stage 4) modes.
It is also the innermost callable dispatched to worker processes — it must NEVER raise.
All failures surface as WFOWindowResult with error set and fitness_score=None.
"""
from __future__ import annotations

import logging
from datetime import UTC, datetime
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


def evaluate_window(
    candidate: CandidateParameterSet,
    window: WFOWindow,
    base_yaml_path: Path,
    temp_dir: Path,
    scenario: ScenarioProfile,
    min_significant_trades: int = 30,
) -> WFOWindowResult:
    """
    Evaluate one candidate on one WFO window.

    Injects the window's date range into the candidate evaluation by passing
    date_override params to strategy_runner. The runner builds a temp YAML
    scoped to [window.start_date, window.end_date].

    Args:
        candidate:               The candidate to evaluate.
        window:                  The WFO window (date range) to evaluate within.
        base_yaml_path:          Path to base strategy_template.yaml.
        temp_dir:                Directory for temp per-candidate YAMLs.
        scenario:                Active scenario profile (for fitness computation).
        min_significant_trades:  Significance guard threshold.

    Returns:
        WFOWindowResult — always. fitness_score is None and error is set on failure.
    """
    try:
        # Evaluate the candidate restricted to this window's date range
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

        return WFOWindowResult(
            candidate_id=candidate.candidate_id,
            window_id=window.window_id,
            evaluated_at=datetime.now(UTC),
            fitness_score=fitness_result.fitness_score,
            total_trades=candidate_result.total_trades,
            net_pnl=_safe_float(m, "net_pnl"),
            max_drawdown=_safe_float(m, "max_drawdown"),
            win_rate=_safe_float(m, "win_rate"),
            expectancy=_safe_float(m, "expectancy"),
            profit_factor=_safe_float(m, "profit_factor"),
            oos_delta=None,  # Populated by wfo_engine if IS/OOS gate is enabled
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

def _safe_float(metrics_obj: object, attr: str) -> Optional[float]:
    """Return float attribute from metrics, or None if absent/None."""
    val = getattr(metrics_obj, attr, None)
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None