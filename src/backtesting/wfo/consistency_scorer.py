"""
wfo/consistency_scorer.py
--------------------------
Aggregates a list of WFOWindowResults for a single candidate into four temporal
consistency metrics and a scenario-weighted composite WFOConsistencyScore.

Single responsibility: List[WFOWindowResult] → WFOConsistencyScore.

The four metrics (per TECHNICAL_SPEC.md and FUNCTIONAL_SPEC.md Stage 4):
  1. median_window_return      — median per-window net P&L across valid windows
  2. window_return_variance    — variance of per-window returns (lower = more consistent)
  3. worst_window_drawdown     — max drawdown seen in the worst-performing window
  4. fraction_positive_windows — fraction of windows with positive net_pnl

Each metric is normalised to [0, 1] before weighting (higher = better for all four).
For variance and worst_drawdown, normalisation inverts the raw value.
"""
from __future__ import annotations

import logging
import math
import statistics
from typing import List, Optional

from src.backtesting.contracts import (
    ScenarioProfile,
    WFOConsistencyScore,
    WFOWindowResult,
)

logger = logging.getLogger(__name__)

# Clamp thresholds for normalisation — calibrated conservatively.
# These define the "worst plausible" values for variance and drawdown normalisation.
_MAX_EXPECTED_VARIANCE: float = 0.10   # variance of net_pnl values (in return space)
_MAX_EXPECTED_DRAWDOWN: float = 0.50   # 50% drawdown = worst floor


def compute_consistency(
    window_results: List[WFOWindowResult],
    windows_total: int,
    scenario: ScenarioProfile,
    oos_gate_enabled: bool = False,
    oos_degradation_threshold: float = 0.50,
) -> WFOConsistencyScore:
    """
    Aggregate WFO window results into a composite consistency score.

    Args:
        window_results:            All WFOWindowResult instances for this candidate.
        windows_total:             Total number of configured windows (including failed).
        scenario:                  Active scenario profile (provides temporal weights).
        oos_gate_enabled:          Whether IS/OOS gate is active.
        oos_degradation_threshold: IS/OOS degradation fraction above which gate triggers.

    Returns:
        WFOConsistencyScore with all four sub-metrics and composite_score in [0, 1].
    """
    candidate_id = window_results[0].candidate_id if window_results else ""

    valid_results: List[WFOWindowResult] = [r for r in window_results if r.is_valid]
    windows_evaluated = len(valid_results)

    if windows_evaluated == 0:
        logger.warning(
            "No valid window results for candidate %s — returning zero consistency score",
            candidate_id[:12],
        )
        return WFOConsistencyScore(
            candidate_id=candidate_id,
            windows_evaluated=0,
            windows_total=windows_total,
            median_window_return=0.0,
            window_return_variance=0.0,
            worst_window_drawdown=1.0,
            fraction_positive_windows=0.0,
            composite_score=0.0,
            oos_gate_triggered=False,
            window_collapse_flag=True,
        )

    net_pnls: List[float] = [r.net_pnl for r in valid_results if r.net_pnl is not None]
    drawdowns: List[float] = [r.max_drawdown for r in valid_results if r.max_drawdown is not None]

    # ── Sub-metric 1: Median window return ──────────────────────────────────
    median_return_raw: float = statistics.median(net_pnls) if net_pnls else 0.0
    # Normalise to [0, 1]: sigmoid-like scaling. 0 return → 0.5, positive → above 0.5
    median_return_norm: float = _sigmoid_normalise(median_return_raw, scale=0.10)

    # ── Sub-metric 2: Window-to-window return variance (inverted) ───────────
    if len(net_pnls) >= 2:
        variance_raw: float = statistics.variance(net_pnls)
    else:
        variance_raw = 0.0
    # Invert: lower variance = higher score
    variance_norm: float = max(0.0, 1.0 - (variance_raw / _MAX_EXPECTED_VARIANCE))
    variance_norm = min(1.0, variance_norm)

    # ── Sub-metric 3: Worst window drawdown (inverted) ───────────────────────
    worst_drawdown_raw: float = max(drawdowns) if drawdowns else 0.0
    # Invert: lower drawdown = higher score
    worst_dd_norm: float = max(0.0, 1.0 - (worst_drawdown_raw / _MAX_EXPECTED_DRAWDOWN))
    worst_dd_norm = min(1.0, worst_dd_norm)

    # ── Sub-metric 4: Fraction of positive windows ───────────────────────────
    positive_count = sum(1 for p in net_pnls if p > 0.0)
    fraction_positive: float = positive_count / len(net_pnls) if net_pnls else 0.0

    # ── Composite score ──────────────────────────────────────────────────────
    composite = (
        scenario.wfo_weight_median_return     * median_return_norm
        + scenario.wfo_weight_variance        * variance_norm
        + scenario.wfo_weight_worst_drawdown  * worst_dd_norm
        + scenario.wfo_weight_fraction_positive * fraction_positive
    )
    # Clamp to [0, 1] — rounding safety
    composite = max(0.0, min(1.0, composite))

    # ── IS/OOS gate ──────────────────────────────────────────────────────────
    oos_gate_triggered = False
    if oos_gate_enabled:
        oos_gate_triggered = _check_oos_gate(valid_results, oos_degradation_threshold)

    # ── Window collapse flag ─────────────────────────────────────────────────
    # Flag if any valid window shows drawdown ≥ 40% (severe collapse)
    window_collapse_flag = any(
        (r.max_drawdown or 0.0) >= 0.40 for r in valid_results
    )

    logger.debug(
        "Consistency score candidate=%s windows=%d/%d composite=%.3f "
        "(median_ret=%.3f var=%.3f worst_dd=%.3f frac_pos=%.3f)",
        candidate_id[:12],
        windows_evaluated,
        windows_total,
        composite,
        median_return_norm,
        variance_norm,
        worst_dd_norm,
        fraction_positive,
    )

    return WFOConsistencyScore(
        candidate_id=candidate_id,
        windows_evaluated=windows_evaluated,
        windows_total=windows_total,
        median_window_return=float(median_return_raw),
        window_return_variance=float(variance_raw),
        worst_window_drawdown=float(worst_drawdown_raw),
        fraction_positive_windows=float(fraction_positive),
        composite_score=float(composite),
        oos_gate_triggered=oos_gate_triggered,
        window_collapse_flag=window_collapse_flag,
    )


# ── Private helpers ────────────────────────────────────────────────────────────

def _sigmoid_normalise(value: float, scale: float = 0.10) -> float:
    """
    Map any real return value to (0, 1) via a sigmoid.
    value=0 → 0.5, positive values → above 0.5, negative → below 0.5.
    scale controls sensitivity (smaller scale = more sensitive to small returns).
    """
    try:
        return 1.0 / (1.0 + math.exp(-value / scale))
    except OverflowError:
        return 0.0 if value < 0 else 1.0


def _check_oos_gate(
    valid_results: List[WFOWindowResult],
    threshold: float,
) -> bool:
    """
    Return True if median IS/OOS degradation across windows exceeds the threshold.
    Only meaningful if oos_delta values are populated by wfo_engine.
    """
    deltas = [r.oos_delta for r in valid_results if r.oos_delta is not None]
    if not deltas:
        return False
    median_delta = statistics.median(deltas)
    # oos_delta is expected to be negative when OOS underperforms IS
    # Degradation = how much OOS falls below IS, expressed as a fraction
    # If delta is already a fraction (e.g. -0.55 = 55% degradation), compare directly
    return abs(median_delta) > threshold