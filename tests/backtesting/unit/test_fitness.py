"""
Unit tests for fitness.py.
"""
from __future__ import annotations

import unittest
from datetime import datetime

from src.backtesting.contracts import (
    CandidateParameterSet,
    CandidateResult,
    RejectionReason,
    ScenarioProfile,
)
from src.backtesting.fitness import evaluate_fitness


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_scenario() -> ScenarioProfile:
    return ScenarioProfile(
        name="capital_accumulation",
        description="Test scenario",
        weight_net_pnl=0.20,
        weight_expectancy=0.25,
        weight_max_drawdown=0.20,
        weight_win_rate=0.15,
        weight_trade_frequency=0.10,
        weight_profit_factor=0.10,
        min_win_rate=0.45,
        max_drawdown=0.15,
        max_losing_streak=7,
        min_trades_per_week=3.0,
        min_expectancy=0.4,
        min_profit_factor=1.3,
        mc_prefilter_ruin_threshold=0.25,
        wfo_weight_median_return=0.30,
        wfo_weight_variance=0.30,
        wfo_weight_worst_drawdown=0.20,
        wfo_weight_fraction_positive=0.20,
        verdict_go_wfo_floor=0.65,
        verdict_borderline_wfo_floor=0.40,
        verdict_go_mc_ruin_ceiling=0.05,
        verdict_borderline_mc_ruin_ceiling=0.15,
        verdict_sensitivity_spike_threshold=0.15,
        report_emphasis=("wfo_consistency_score",),
    )


class _FakeMetrics:
    """Fake MetricsReport with all fields evaluate_fitness needs."""
    def __init__(
        self,
        win_rate=0.55,
        max_drawdown=0.08,
        max_losing_streak=3,
        trades_per_week=5.0,
        expectancy=0.6,
        profit_factor=1.8,
        total_pnl_points=1000.0,
    ):
        self.win_rate = win_rate
        self.max_drawdown = max_drawdown
        self.max_losing_streak = max_losing_streak
        self.trades_per_week = trades_per_week
        self.expectancy = expectancy
        self.profit_factor = profit_factor
        self.total_pnl_points = total_pnl_points


def _make_valid_result(candidate_id="test_cid", **kwargs) -> CandidateResult:
    return CandidateResult(
        candidate_id=candidate_id,
        evaluated_at=datetime(2026, 1, 1),
        metrics=_FakeMetrics(**kwargs),
        trades=object(),
        total_trades=50,
    )


def _make_invalid_result(candidate_id="test_cid", error="some error") -> CandidateResult:
    return CandidateResult(
        candidate_id=candidate_id,
        evaluated_at=datetime(2026, 1, 1),
        metrics=None,
        trades=None,
        total_trades=None,
        error=error,
    )


class TestFitnessConstraints(unittest.TestCase):
    def setUp(self):
        self.scenario = _make_scenario()

    def test_constraint_order_drawdown_first(self):
        """Candidate failing drawdown first → failing_constraint == 'max_drawdown'."""
        result = _make_valid_result(
            max_drawdown=0.20,  # fails (> 0.15 threshold)
            win_rate=0.30,      # also fails but should not be reached
        )
        fitness = evaluate_fitness(result, self.scenario)
        self.assertFalse(fitness.passed_constraints)
        self.assertEqual(fitness.failing_constraint, "max_drawdown")
        self.assertAlmostEqual(fitness.failing_value, 0.20, places=5)

    def test_win_rate_constraint(self):
        result = _make_valid_result(win_rate=0.30)  # fails < 0.45
        fitness = evaluate_fitness(result, self.scenario)
        self.assertFalse(fitness.passed_constraints)
        self.assertEqual(fitness.failing_constraint, "win_rate")

    def test_losing_streak_constraint(self):
        result = _make_valid_result(max_losing_streak=10)  # fails > 7
        fitness = evaluate_fitness(result, self.scenario)
        self.assertFalse(fitness.passed_constraints)
        self.assertEqual(fitness.failing_constraint, "losing_streak")

    def test_trades_per_week_constraint(self):
        result = _make_valid_result(trades_per_week=1.0)  # fails < 3.0
        fitness = evaluate_fitness(result, self.scenario)
        self.assertFalse(fitness.passed_constraints)
        self.assertEqual(fitness.failing_constraint, "trades_per_week")

    def test_expectancy_constraint(self):
        result = _make_valid_result(expectancy=0.1)  # fails < 0.4
        fitness = evaluate_fitness(result, self.scenario)
        self.assertFalse(fitness.passed_constraints)
        self.assertEqual(fitness.failing_constraint, "expectancy")

    def test_profit_factor_constraint(self):
        result = _make_valid_result(profit_factor=1.1)  # fails < 1.3
        fitness = evaluate_fitness(result, self.scenario)
        self.assertFalse(fitness.passed_constraints)
        self.assertEqual(fitness.failing_constraint, "profit_factor")

    def test_all_constraints_pass(self):
        result = _make_valid_result()
        fitness = evaluate_fitness(result, self.scenario)
        self.assertTrue(fitness.passed_constraints)
        self.assertIsNone(fitness.failing_constraint)
        self.assertIsNone(fitness.rejection_reason)


class TestFitnessScore(unittest.TestCase):
    def setUp(self):
        self.scenario = _make_scenario()

    def test_fitness_range(self):
        """Valid candidate → fitness_score in [0.0, 1.0]."""
        result = _make_valid_result()
        fitness = evaluate_fitness(result, self.scenario)
        self.assertIsNotNone(fitness.fitness_score)
        self.assertGreaterEqual(fitness.fitness_score, 0.0)
        self.assertLessEqual(fitness.fitness_score, 1.0)

    def test_stateless(self):
        """Calling twice with same inputs returns identical result."""
        result = _make_valid_result()
        f1 = evaluate_fitness(result, self.scenario)
        f2 = evaluate_fitness(result, self.scenario)
        self.assertEqual(f1.fitness_score, f2.fitness_score)
        self.assertEqual(f1.passed_constraints, f2.passed_constraints)


class TestFitnessInvalidResult(unittest.TestCase):
    def setUp(self):
        self.scenario = _make_scenario()

    def test_invalid_result_rejected(self):
        """CandidateResult with error set → FitnessResult with rejection."""
        result = _make_invalid_result(error="some evaluation error")
        fitness = evaluate_fitness(result, self.scenario)
        self.assertFalse(fitness.passed_constraints)
        self.assertIsNone(fitness.fitness_score)
        self.assertEqual(fitness.rejection_reason, "some evaluation error")

    def test_rejected_insufficient_trades_propagated(self):
        result = _make_invalid_result(
            error=RejectionReason.REJECTED_INSUFFICIENT_TRADES.value
        )
        fitness = evaluate_fitness(result, self.scenario)
        self.assertFalse(fitness.passed_constraints)
        self.assertEqual(
            fitness.rejection_reason,
            RejectionReason.REJECTED_INSUFFICIENT_TRADES.value,
        )

    def test_actuals_none_for_invalid_result(self):
        result = _make_invalid_result()
        fitness = evaluate_fitness(result, self.scenario)
        self.assertIsNone(fitness.actual_win_rate)
        self.assertIsNone(fitness.actual_max_drawdown)


if __name__ == "__main__":
    unittest.main(verbosity=2)