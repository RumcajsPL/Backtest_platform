"""
fitness.py — Stateless fitness evaluation.

Applies constraint checks (cheapest/most-likely-to-fail first), then computes
the weighted composite fitness score. Receives CandidateResult + ScenarioProfile,
returns FitnessResult. No side effects. No state.

Constraint order (fail fast — cheapest rejection first):
1. max_drawdown   — often fails; single float comparison
2. win_rate       — often fails; single float comparison
3. losing_streak  — integer comparison
4. trades_per_week — float comparison
5. expectancy     — float comparison
6. profit_factor  — float comparison
"""
from __future__ import annotations

import operator as op
from typing import Optional, Tuple

from src.backtesting.contracts import (
    CandidateResult,
    FitnessResult,
    RejectionReason,
    ScenarioProfile,
)

# ── Constraint check table ────────────────────────────────────────────────────
# (field_label, metrics_attr, scenario_threshold_attr, comparator)
# Ordered cheapest/most-rejecting first.

_CONSTRAINT_CHECKS: Tuple = (
    ("max_drawdown",     "max_drawdown",      "max_drawdown",        op.gt),
    ("win_rate",         "win_rate",          "min_win_rate",        op.lt),
    ("losing_streak",    "max_losing_streak", "max_losing_streak",   op.gt),
    ("trades_per_week",  "trades_per_week",   "min_trades_per_week", op.lt),
    ("expectancy",       "expectancy",        "min_expectancy",      op.lt),
    ("profit_factor",    "profit_factor",     "min_profit_factor",   op.lt),
)


def evaluate_fitness(
    result: CandidateResult,
    scenario: ScenarioProfile,
) -> FitnessResult:
    """
    Evaluate fitness for a single candidate result against a scenario profile.

    Stateless: calling twice with identical inputs returns identical outputs.

    Returns FitnessResult with:
    - passed_constraints=False + rejection_reason if any constraint fails or
      the candidate result is invalid.
    - passed_constraints=True + fitness_score in [0, 1] if all constraints pass.
    """
    # Guard: invalid CandidateResult (evaluation failed in strategy_runner)
    if not result.is_valid:
        return FitnessResult(
            candidate_id=result.candidate_id,
            scenario_name=scenario.name,
            fitness_score=None,
            passed_constraints=False,
            rejection_reason=result.error or RejectionReason.EVALUATION_ERROR.value,
            failing_constraint=None,
            failing_value=None,
            actual_win_rate=None,
            actual_max_drawdown=None,
            actual_losing_streak=None,
            actual_trades_per_week=None,
            actual_expectancy=None,
            actual_profit_factor=None,
        )

    m = result.metrics

    # Extract constraint actuals (always populated for valid results)
    actual_win_rate         = _get(m, "win_rate")
    actual_max_drawdown     = _get(m, "max_drawdown")
    actual_losing_streak    = _get(m, "max_losing_streak")
    actual_trades_per_week  = _get(m, "trades_per_week")
    actual_expectancy       = _get(m, "expectancy")
    actual_profit_factor    = _get(m, "profit_factor")

    # Evaluate constraints in order — return on first failure
    for label, metric_attr, threshold_attr, comparator in _CONSTRAINT_CHECKS:
        actual = _get(m, metric_attr)
        threshold = getattr(scenario, threshold_attr)
        if actual is None or comparator(actual, threshold):
            return FitnessResult(
                candidate_id=result.candidate_id,
                scenario_name=scenario.name,
                fitness_score=None,
                passed_constraints=False,
                rejection_reason=RejectionReason.REJECTED_CONSTRAINTS.value,
                failing_constraint=label,
                failing_value=float(actual) if actual is not None else None,
                actual_win_rate=actual_win_rate,
                actual_max_drawdown=actual_max_drawdown,
                actual_losing_streak=actual_losing_streak,
                actual_trades_per_week=actual_trades_per_week,
                actual_expectancy=actual_expectancy,
                actual_profit_factor=actual_profit_factor,
            )

    # All constraints passed — compute weighted fitness score
    fitness_score = _compute_weighted_score(m, scenario)

    return FitnessResult(
        candidate_id=result.candidate_id,
        scenario_name=scenario.name,
        fitness_score=fitness_score,
        passed_constraints=True,
        rejection_reason=None,
        failing_constraint=None,
        failing_value=None,
        actual_win_rate=actual_win_rate,
        actual_max_drawdown=actual_max_drawdown,
        actual_losing_streak=actual_losing_streak,
        actual_trades_per_week=actual_trades_per_week,
        actual_expectancy=actual_expectancy,
        actual_profit_factor=actual_profit_factor,
    )


# ── Internal helpers ───────────────────────────────────────────────────────────

def _get(metrics, attr: str):
    """Safely retrieve a metric attribute; return None if absent."""
    return getattr(metrics, attr, None)


def _compute_weighted_score(metrics, scenario: ScenarioProfile) -> float:
    """
    Compute the composite fitness score in [0, 1] by normalising each metric
    and combining with scenario weights.

    Normalisation bounds are conservative defaults; the pipeline will calibrate
    these in Phase 6. For now they are chosen to keep the score in [0, 1] for
    realistic strategy outputs.

    Drawdown is inverted: lower drawdown → higher contribution.
    """
    # Normalise each metric to [0, 1]
    win_rate_norm         = _clamp(_get(metrics, "win_rate") or 0.0, 0.0, 1.0)
    expectancy_norm       = _clamp((_get(metrics, "expectancy") or 0.0) / 3.0, 0.0, 1.0)
    profit_factor_norm    = _clamp(((_get(metrics, "profit_factor") or 1.0) - 1.0) / 4.0, 0.0, 1.0)
    drawdown_norm         = _clamp(1.0 - (_get(metrics, "max_drawdown") or 0.0), 0.0, 1.0)
    net_pnl_raw           = _get(metrics, "total_pnl_points") or _get(metrics, "net_pnl") or 0.0
    net_pnl_norm          = _clamp(net_pnl_raw / 5000.0, 0.0, 1.0)   # ~5000 pts as "excellent"
    freq_raw              = _get(metrics, "trades_per_week") or 0.0
    trade_freq_norm       = _clamp(freq_raw / 20.0, 0.0, 1.0)         # ~20 trades/week as ceiling

    score = (
        scenario.weight_net_pnl         * net_pnl_norm
        + scenario.weight_expectancy    * expectancy_norm
        + scenario.weight_max_drawdown  * drawdown_norm
        + scenario.weight_win_rate      * win_rate_norm
        + scenario.weight_trade_frequency * trade_freq_norm
        + scenario.weight_profit_factor * profit_factor_norm
    )

    return _clamp(score, 0.0, 1.0)


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))