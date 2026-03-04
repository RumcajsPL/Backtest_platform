"""
tests/backtesting/unit/test_7d_audit_m01_m06.py
------------------------------------------------
Sub-block 7D regression tests — M-01 (median_oos_delta) and M-06 (mutation_std_steps).

Tests:
  M01-01  median_oos_delta computed correctly from window oos_delta values
          (mix of positive and negative, including None entries filtered out)
  M01-02  median_oos_delta=None when no windows carry oos_delta data
  M06-01  larger mutation_std_steps produces larger average absolute perturbation
  M06-02  mutation_std_steps≈0 produces no-op mutation for int/float parameters
"""
from __future__ import annotations

import random
import statistics
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import List, Optional
from unittest.mock import MagicMock

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.backtesting.contracts import (  # noqa: E402
    CandidateParameterSet,
    ScenarioProfile,
    WFOWindowResult,
)
from src.backtesting.wfo.consistency_scorer import compute_consistency  # noqa: E402
from src.backtesting.ga.mutation import mutate  # noqa: E402


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_scenario() -> ScenarioProfile:
    return ScenarioProfile(
        name="test",
        description="test",
        weight_net_pnl=0.30,
        weight_expectancy=0.20,
        weight_max_drawdown=0.20,
        weight_win_rate=0.10,
        weight_trade_frequency=0.10,
        weight_profit_factor=0.10,
        min_win_rate=0.05,
        max_drawdown=0.99,
        max_losing_streak=100,
        min_trades_per_week=0.0,
        min_expectancy=-100.0,
        min_profit_factor=0.01,
        mc_prefilter_ruin_threshold=0.50,
        wfo_weight_median_return=0.40,
        wfo_weight_variance=0.20,
        wfo_weight_worst_drawdown=0.20,
        wfo_weight_fraction_positive=0.20,
        verdict_go_wfo_floor=0.65,
        verdict_borderline_wfo_floor=0.40,
        verdict_go_mc_ruin_ceiling=0.05,
        verdict_borderline_mc_ruin_ceiling=0.15,
        verdict_sensitivity_spike_threshold=0.15,
        report_emphasis=("wfo_score",),
    )


def _make_window(
    candidate_id: str,
    window_id: str,
    oos_delta: Optional[float],
) -> WFOWindowResult:
    return WFOWindowResult(
        candidate_id=candidate_id,
        window_id=window_id,
        evaluated_at=datetime.now(UTC),
        fitness_score=0.60,
        total_trades=30,
        net_pnl=100.0,
        max_drawdown=0.10,
        win_rate=55.0,
        expectancy=2.0,
        profit_factor=1.5,
        oos_delta=oos_delta,
        error=None,
    )


def _make_candidate(params: dict) -> CandidateParameterSet:
    return CandidateParameterSet.create(
        zone_name="scalp",
        parameters=params,
    )


def _int_zone_def() -> dict:
    return {
        "parameters": {
            "rsi_period": {"type": "int", "min": 5, "max": 30, "step": 1},
            "ema_fast": {"type": "int", "min": 5, "max": 50, "step": 1},
        }
    }


def _float_zone_def() -> dict:
    return {
        "parameters": {
            "threshold": {"type": "float", "min": 0.0, "max": 1.0, "step": 0.01},
        }
    }


# ─────────────────────────────────────────────────────────────────────────────
# M-01 Tests — median_oos_delta
# ─────────────────────────────────────────────────────────────────────────────

class TestM01MedianOosDelta:

    def test_m01_01_median_oos_delta_computed_correctly(self):
        """
        M01-01: median_oos_delta is the statistical median of all non-None
        oos_delta values across valid windows. None values are excluded.

        Setup: 5 windows with oos_delta = [-0.30, -0.10, 0.05, None, -0.20].
        Filtered deltas: [-0.30, -0.10, 0.05, -0.20].
        Median of sorted [-0.30, -0.20, -0.10, 0.05] = (-0.20 + -0.10) / 2 = -0.15.
        """
        cid = "cand_m01_01"
        scenario = _make_scenario()

        oos_deltas = [-0.30, -0.10, 0.05, None, -0.20]
        windows = [
            _make_window(cid, f"w{i+1}", d)
            for i, d in enumerate(oos_deltas)
        ]

        score = compute_consistency(windows, 5, scenario)

        expected_deltas = [-0.30, -0.10, 0.05, -0.20]
        expected_median = statistics.median(expected_deltas)

        assert score.median_oos_delta is not None, (
            "M01-01 FAIL: median_oos_delta should not be None when oos_delta values present"
        )
        assert abs(score.median_oos_delta - expected_median) < 1e-9, (
            f"M01-01 FAIL: expected median_oos_delta={expected_median:.6f}, "
            f"got {score.median_oos_delta:.6f}"
        )

    def test_m01_02_median_oos_delta_none_when_no_deltas(self):
        """
        M01-02: median_oos_delta=None when all windows have oos_delta=None.
        This is the common case when oos_gate is disabled or wfo_engine
        does not populate oos_delta.
        """
        cid = "cand_m01_02"
        scenario = _make_scenario()

        windows = [
            _make_window(cid, f"w{i+1}", None)  # all None
            for i in range(4)
        ]

        score = compute_consistency(windows, 4, scenario)

        assert score.median_oos_delta is None, (
            f"M01-02 FAIL: expected median_oos_delta=None when all oos_delta=None; "
            f"got {score.median_oos_delta}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# M-06 Tests — mutation_std_steps
# ─────────────────────────────────────────────────────────────────────────────

class TestM06MutationStdSteps:

    def test_m06_01_larger_std_steps_produces_larger_perturbation(self):
        """
        M06-01: mutation_std_steps controls perturbation amplitude.
        With mutation_rate=1.0 (mutate every param), running 200 trials:
        average |delta| with std_steps=4.0 must exceed average |delta| with std_steps=0.5.
        """
        zone_def = _int_zone_def()
        params = {"rsi_period": 14, "ema_fast": 20}
        candidate = _make_candidate(params)

        N_TRIALS = 200
        rng_tight = random.Random(42)
        rng_loose = random.Random(42)

        deltas_tight = []
        deltas_loose = []

        for _ in range(N_TRIALS):
            mutated_tight = mutate(
                candidate, mutation_rate=1.0,
                parameter_space_def=zone_def,
                rng=rng_tight, mutation_std_steps=0.5,
            )
            mutated_loose = mutate(
                candidate, mutation_rate=1.0,
                parameter_space_def=zone_def,
                rng=rng_loose, mutation_std_steps=4.0,
            )
            for key in params:
                deltas_tight.append(abs(mutated_tight.parameters[key] - params[key]))
                deltas_loose.append(abs(mutated_loose.parameters[key] - params[key]))

        avg_tight = sum(deltas_tight) / len(deltas_tight)
        avg_loose = sum(deltas_loose) / len(deltas_loose)

        assert avg_loose > avg_tight, (
            f"M06-01 FAIL: std_steps=4.0 should give larger average |delta| than std_steps=0.5. "
            f"avg_loose={avg_loose:.3f}  avg_tight={avg_tight:.3f}"
        )

    def test_m06_02_near_zero_std_steps_produces_no_op_mutation(self):
        """
        M06-02: mutation_std_steps≈0 means Gaussian noise ≈ 0 at every step.
        With std_steps=0.001, all int perturbations round to 0 steps → no change.
        float perturbations: noise = 0.001 * 0.01 (step) = 1e-5, snapped back to 0 offset.
        100% of mutations should leave parameters unchanged.
        """
        int_zone = _int_zone_def()
        float_zone = _float_zone_def()

        int_params = {"rsi_period": 14, "ema_fast": 20}
        float_params = {"threshold": 0.50}

        int_candidate = _make_candidate(int_params)
        float_candidate = _make_candidate(float_params)

        rng = random.Random(99)
        N = 100

        # All int mutations with std=0.001 should produce no change
        int_unchanged = sum(
            1 for _ in range(N)
            if mutate(
                int_candidate, mutation_rate=1.0,
                parameter_space_def=int_zone, rng=rng,
                mutation_std_steps=0.001,
            ).parameters == int_params
        )

        rng2 = random.Random(99)
        float_unchanged = sum(
            1 for _ in range(N)
            if mutate(
                float_candidate, mutation_rate=1.0,
                parameter_space_def=float_zone, rng=rng2,
                mutation_std_steps=0.001,
            ).parameters == float_params
        )

        # Allow 5% tolerance for any edge-case floating point rounding
        assert int_unchanged >= N * 0.95, (
            f"M06-02 FAIL: Expected ≥95% no-op int mutations with std_steps=0.001; "
            f"got {int_unchanged}/{N} unchanged"
        )
        assert float_unchanged >= N * 0.95, (
            f"M06-02 FAIL: Expected ≥95% no-op float mutations with std_steps=0.001; "
            f"got {float_unchanged}/{N} unchanged"
        )