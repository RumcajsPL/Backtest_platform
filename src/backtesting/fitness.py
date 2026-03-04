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

Unit normalisation (MetricsReport → scenario threshold space):
- win_rate:    MetricsReport stores 0–100 (percent). Scenarios use 0–1 (fraction).
               Normalised by dividing by 100 before comparison and storage.
- max_drawdown: MetricsReport stores negative points (e.g. -1490.0).
               Scenarios use positive fractions (e.g. 0.15 = 15% drawdown).
               Normalised by abs(points) / scenario.normalisation_drawdown_ref_points.
               This reference constant is now scenario-configurable (M-02).
               Default: 10_000 pts (conservative — errs toward passing the
               constraint rather than rejecting valid candidates).

Block 7B change (M-02): Fitness normalisation constants previously hardcoded
as module-level constants are now read from ScenarioProfile:
  - MAX_DRAWDOWN_REF_POINTS     → scenario.normalisation_drawdown_ref_points
  - 5000.0 (PnL ceiling)        → scenario.normalisation_pnl_ref_points
  - 20.0   (freq ceiling)       → scenario.normalisation_freq_ref_trades_per_week
The module-level MAX_DRAWDOWN_REF_POINTS constant is retained for the
_normalise_max_drawdown helper used in the constraint check table, where
the scenario is not yet available. The scoring path uses scenario fields.

Block 8B change (B8B-001): NaN guard added before constraint loop.
IEEE 754 semantics mean op.lt(NaN, x) and op.gt(NaN, x) both return False,
causing NaN metric values to silently pass all constraints. The explicit
math.isnan check short-circuits with a clean EVALUATION_ERROR rejection.
"""
from __future__ import annotations

import math
import operator as op
from typing import Optional, Tuple

from src.backtesting.contracts import (
    CandidateResult,
    FitnessResult,
    RejectionReason,
    ScenarioProfile,
)

# ── Drawdown normalisation reference (constraint path only) ──────────────────
# Used in _CONSTRAINT_CHECKS to normalise MetricsReport.max_drawdown (negative
# points) to a fraction in [0, 1] for threshold comparison.
# The fitness scoring path reads scenario.normalisation_drawdown_ref_points.
# This module-level constant is intentionally kept for the constraint check
# table which is built at import time without a scenario instance.
# Value must stay in sync with ScenarioProfile.normalisation_drawdown_ref_points default.
_CONSTRAINT_DRAWDOWN_REF_POINTS: float = 10_000.0


def _normalise_win_rate(win_rate_pct: Optional[float]) -> Optional[float]:
    """Convert win_rate from 0–100 percent to 0–1 fraction. None-safe."""
    if win_rate_pct is None:
        return None
    return win_rate_pct / 100.0


def _normalise_max_drawdown_constraint(max_drawdown_points: Optional[float]) -> Optional[float]:
    """
    Convert max_drawdown from negative points to positive fraction in [0, 1].
    Uses the module-level constraint reference constant.
    Used in constraint checks only — not in fitness scoring.
    None-safe.
    """
    if max_drawdown_points is None:
        return None
    return min(1.0, abs(max_drawdown_points) / _CONSTRAINT_DRAWDOWN_REF_POINTS)


# ── Constraint check table ────────────────────────────────────────────────────
# (field_label, metrics_attr, scenario_threshold_attr, comparator, normaliser)
# Ordered cheapest/most-rejecting first.
# normaliser: optional callable applied to the raw metric value before comparison.
#
# Boundary semantics: all lower-bound constraints use op.lt (reject when
# actual < threshold), so a value exactly equal to the threshold is ACCEPTED
# (implements >= semantics). All upper-bound constraints use op.gt (implements <=).

_CONSTRAINT_CHECKS: Tuple = (
    ("max_drawdown",    "max_drawdown",      "max_drawdown",        op.gt, _normalise_max_drawdown_constraint),
    ("win_rate",        "win_rate",          "min_win_rate",        op.lt, _normalise_win_rate),
    ("losing_streak",   "losing_streak",     "max_losing_streak",   op.gt, None),
    ("trades_per_week", "trades_per_week",   "min_trades_per_week", op.lt, None),
    ("expectancy",      "expectancy_points", "min_expectancy",      op.lt, None),
    ("profit_factor",   "profit_factor",     "min_profit_factor",   op.lt, None),
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

    # Extract and normalise constraint actuals.
    # Stored in normalised units (0–1 fractions) to match scenario thresholds.
    actual_win_rate         = _normalise_win_rate(_get(m, "win_rate"))
    actual_max_drawdown     = _normalise_max_drawdown_constraint(_get(m, "max_drawdown"))
    actual_losing_streak    = _get(m, "losing_streak")
    actual_trades_per_week  = _get(m, "trades_per_week")
    actual_expectancy       = _get(m, "expectancy_points")
    actual_profit_factor    = _get(m, "profit_factor")

    # B8B-001: NaN guard — must precede constraint loop.
    # IEEE 754 semantics: op.lt(NaN, x) and op.gt(NaN, x) are both False, so a NaN
    # actual value passes every constraint silently. An explicit math.isnan check
    # short-circuits here with a clean EVALUATION_ERROR rejection before NaN can
    # propagate into _compute_weighted_score or trigger ValueError in __post_init__.
    _nan_actuals = [actual_win_rate, actual_max_drawdown, actual_losing_streak,
                    actual_trades_per_week, actual_expectancy, actual_profit_factor]
    if any(isinstance(v, float) and math.isnan(v) for v in _nan_actuals if v is not None):
        return FitnessResult(
            candidate_id=result.candidate_id,
            scenario_name=scenario.name,
            fitness_score=None,
            passed_constraints=False,
            rejection_reason=RejectionReason.EVALUATION_ERROR.value,
            failing_constraint="nan_metric",
            failing_value=None,
            actual_win_rate=actual_win_rate,
            actual_max_drawdown=actual_max_drawdown,
            actual_losing_streak=actual_losing_streak,
            actual_trades_per_week=actual_trades_per_week,
            actual_expectancy=actual_expectancy,
            actual_profit_factor=actual_profit_factor,
        )

    # Evaluate constraints in order — return on first failure.
    for label, metric_attr, threshold_attr, comparator, normaliser in _CONSTRAINT_CHECKS:
        raw = _get(m, metric_attr)
        actual = normaliser(raw) if (normaliser is not None) else raw
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

    All normalisation reference constants are read from ScenarioProfile (M-02).
    Defaults on ScenarioProfile reproduce the prior hardcoded behaviour exactly.

    Drawdown is inverted: lower drawdown → higher contribution.
    """
    # win_rate: normalise from 0–100 → 0–1
    win_rate_norm = _clamp(_normalise_win_rate(_get(metrics, "win_rate")) or 0.0, 0.0, 1.0)

    # expectancy_points: normalise to [0, 1] with fixed scale of 3.0 pts per unit
    # (not yet scenario-configurable — deferred to Block 9 calibration; B8B-003)
    expectancy_norm = _clamp((_get(metrics, "expectancy_points") or 0.0) / 3.0, 0.0, 1.0)

    profit_factor_norm = _clamp(((_get(metrics, "profit_factor") or 1.0) - 1.0) / 4.0, 0.0, 1.0)

    # max_drawdown: normalise from points → fraction using scenario ref, then invert
    dd_points = _get(metrics, "max_drawdown")
    drawdown_frac = (
        min(1.0, abs(dd_points) / scenario.normalisation_drawdown_ref_points)
        if dd_points is not None else 0.0
    )
    drawdown_norm = _clamp(1.0 - drawdown_frac, 0.0, 1.0)

    # net P&L: normalise using scenario ref (M-02)
    net_pnl_raw = _get(metrics, "total_pnl_points") or 0.0
    net_pnl_norm = _clamp(net_pnl_raw / scenario.normalisation_pnl_ref_points, 0.0, 1.0)

    # trade frequency: normalise using scenario ref (M-02)
    freq_raw = _get(metrics, "trades_per_week") or 0.0
    trade_freq_norm = _clamp(freq_raw / scenario.normalisation_freq_ref_trades_per_week, 0.0, 1.0)

    score = (
        scenario.weight_net_pnl           * net_pnl_norm
        + scenario.weight_expectancy      * expectancy_norm
        + scenario.weight_max_drawdown    * drawdown_norm
        + scenario.weight_win_rate        * win_rate_norm
        + scenario.weight_trade_frequency * trade_freq_norm
        + scenario.weight_profit_factor   * profit_factor_norm
    )

    return _clamp(score, 0.0, 1.0)


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))