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

Block 7B change (M-03): window_collapse_flag threshold is now read from
ScenarioProfile.wfo_collapse_drawdown_threshold instead of hardcoded 0.40.
Default on ScenarioProfile is 0.40, preserving prior behaviour for all existing
scenarios that do not set this field explicitly.

Block 9I change (B8B-012): Three normalisation constants recalibrated for DAX
point-denominated returns. Prior values were calibrated for fractional returns
(0–1 range) and produced degenerate outputs for DAX point values:
  - _SIGMOID_SCALE:          0.10   → 131.0  (stdev=261.98 from run 87712cab × 0.5)
  - _MAX_EXPECTED_VARIANCE:  0.10   → 100_000.0  (pts² ceiling; stdev≈262 → var≈68k)
  - _MAX_EXPECTED_DRAWDOWN:  0.50   → 600.0  (pts ceiling; observed range -282 to -493)
Prior values caused all three sub-metrics to produce 0.0 or identical outputs,
making all WFO composite scores identical (0.7000 in calibration run 87712cab).
Recalibrate _SIGMOID_SCALE after each significant data range change:
  scale = stdev(wfo_window_results.net_pnl WHERE is_ga_fitness_window=0) * 0.5
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

# ── Normalisation constants — calibrated for DAX point-denominated returns ────
# Recalibrate after significant data range or instrument changes.
# Source: run 87712cab (capital_accumulation, 3-month DAX, 2026-03-07)
#
# _SIGMOID_SCALE: controls sensitivity of median_return_norm to net_pnl values.
#   value=0 → 0.5, value=+scale → ~0.731, value=-scale → ~0.269
#   = stdev(wfo_window_results.net_pnl) * 0.5
#   stdev from calibration run: 261.98 pts → scale = 131.0
#
# _MAX_EXPECTED_VARIANCE: "worst plausible" variance of per-window net_pnl (pts²).
#   Used to invert variance into a [0,1] score (lower variance = higher score).
#   Set conservatively above observed values to avoid clamping at 0.
#   stdev≈262 → variance≈68,000 → ceiling = 100_000 pts²
#
# _MAX_EXPECTED_DRAWDOWN: "worst plausible" per-window max_drawdown (pts, positive).
#   worst_window_drawdown is stored as raw negative pts (e.g. -416.81).
#   Inversion: 1.0 - (abs(raw) / ceiling). Ceiling must be > max observed abs value.
#   Observed range in calibration run: 282–676 pts → ceiling = 1_000 pts (conservative)
_SIGMOID_SCALE: float = 163.0 # After calibration standard: 310.0; 128 1min; 283 15min
_MAX_EXPECTED_VARIANCE: float = 100_000.0
_MAX_EXPECTED_DRAWDOWN: float = 2_500.0


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
        scenario:                  Active scenario profile (provides temporal weights and
                                   wfo_collapse_drawdown_threshold).
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
    # Normalise to [0, 1] via sigmoid. value=0 → 0.5, positive → above 0.5.
    # B8B-012: scale=131.0 (calibrated from DAX point stdev, run 87712cab).
    median_return_norm: float = _sigmoid_normalise(median_return_raw, scale=_SIGMOID_SCALE)

    # ── Sub-metric 2: Window-to-window return variance (inverted) ───────────
    if len(net_pnls) >= 2:
        variance_raw: float = statistics.variance(net_pnls)
    else:
        variance_raw = 0.0
    # Invert: lower variance = higher score.
    # B8B-012: _MAX_EXPECTED_VARIANCE=100_000 pts² (calibrated for DAX points).
    variance_norm: float = max(0.0, min(1.0, 1.0 - (variance_raw / _MAX_EXPECTED_VARIANCE)))

    # ── Sub-metric 3: Worst window drawdown (inverted) ───────────────────────
    # worst_window_drawdown is stored as raw negative pts (e.g. -416.81).
    # Take abs() before normalising so inversion works correctly.
    # B8B-012: _MAX_EXPECTED_DRAWDOWN=1_000 pts (calibrated for DAX points).
    worst_drawdown_raw: float = max(drawdowns) if drawdowns else 0.0
    worst_dd_abs: float = abs(worst_drawdown_raw)
    worst_dd_norm: float = max(0.0, min(1.0, 1.0 - (worst_dd_abs / _MAX_EXPECTED_DRAWDOWN)))

    # ── Sub-metric 4: Fraction of positive windows ───────────────────────────
    positive_count = sum(1 for p in net_pnls if p > 0.0)
    fraction_positive: float = positive_count / len(net_pnls) if net_pnls else 0.0

    # ── Composite score ──────────────────────────────────────────────────────
    composite = (
        scenario.wfo_weight_median_return       * median_return_norm
        + scenario.wfo_weight_variance          * variance_norm
        + scenario.wfo_weight_worst_drawdown    * worst_dd_norm
        + scenario.wfo_weight_fraction_positive * fraction_positive
    )
    # Clamp to [0, 1] — rounding safety
    composite = max(0.0, min(1.0, composite))

    # ── IS/OOS gate ──────────────────────────────────────────────────────────
    oos_gate_triggered = False
    if oos_gate_enabled:
        oos_gate_triggered = _check_oos_gate(valid_results, oos_degradation_threshold)

    # ── Median OOS delta (M-01) ───────────────────────────────────────────────
    # Compute here while valid_results are in scope. verdict.py reads directly
    # from wfo_score.median_oos_delta — no second query to the store required.
    oos_deltas = [r.oos_delta for r in valid_results if r.oos_delta is not None]
    median_oos_delta: Optional[float] = (
        statistics.median(oos_deltas) if oos_deltas else None
    )

    # ── Window collapse flag (M-03) ──────────────────────────────────────────
    # Flag if any valid window shows drawdown >= scenario threshold.
    # Previously hardcoded as 0.40. Now reads from ScenarioProfile so that
    # conservative scenario (threshold 0.20) can flag collapses that
    # capital_accumulation (threshold 0.40) would not.
    # B8B-012: worst_window_drawdown is in raw pts — compare abs() vs threshold.
    # ScenarioProfile.wfo_collapse_drawdown_threshold is also in pts for DAX runs.
    # NOTE: if the scenario threshold was set as a fraction (e.g. 0.40), it will
    # never trigger for DAX point values. Operator must set threshold in pts
    # (e.g. 400.0) for point-denominated instruments after this calibration.
    collapse_threshold: float = scenario.wfo_collapse_drawdown_threshold
    window_collapse_flag = any(
        abs(r.max_drawdown or 0.0) >= collapse_threshold for r in valid_results
    )

    logger.debug(
        "Consistency score candidate=%s windows=%d/%d composite=%.3f "
        "(median_ret=%.3f var=%.3f worst_dd=%.3f frac_pos=%.3f collapse_threshold=%.2f "
        "median_oos_delta=%s)",
        candidate_id[:12],
        windows_evaluated,
        windows_total,
        composite,
        median_return_norm,
        variance_norm,
        worst_dd_norm,
        fraction_positive,
        collapse_threshold,
        f"{median_oos_delta:.4f}" if median_oos_delta is not None else "None",
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
        median_oos_delta=median_oos_delta,
    )


# ── Private helpers ────────────────────────────────────────────────────────────

def _sigmoid_normalise(value: float, scale: float = _SIGMOID_SCALE) -> float:
    """
    Map any real return value to (0, 1) via a sigmoid.
    value=0 → 0.5, positive values → above 0.5, negative → below 0.5.
    scale controls sensitivity: value=+scale → ~0.731, value=-scale → ~0.269.
    B8B-012: default scale updated from 0.10 to _SIGMOID_SCALE (131.0).
    Recalibrate when instrument or data range changes:
      scale = stdev(wfo_window_results.net_pnl WHERE is_ga_fitness_window=0) * 0.5
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