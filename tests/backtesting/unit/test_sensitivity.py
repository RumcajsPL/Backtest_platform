"""
tests/backtesting/unit/test_sensitivity.py
──────────────────────────────────────────
Unit tests for evaluation/sensitivity.py (Stage 6).

All evaluate_sensitivity() tests patch _evaluate_perturbation directly so that
ProcessPoolExecutor workers never touch the filesystem or call the strategy runner.
"""

from __future__ import annotations

import sys
from concurrent.futures import Future
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from src.backtesting.contracts import (
    CandidateParameterSet,
    ScenarioProfile,
    SensitivityProfile,
)
from src.backtesting.evaluation.sensitivity import (
    _perturb_value,
    _step_offsets,
    evaluate_sensitivity,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def scenario() -> ScenarioProfile:
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
        report_emphasis=(
            "wfo_consistency_score", "fraction_positive_windows",
            "actual_trades_per_week", "mc_deep_ruin_probability", "actual_max_drawdown"
        ),
    )


@pytest.fixture
def parameter_space_def() -> dict:
    return {
        "safe": {
            "parameters": {
                "rsi_period":     {"type": "int",    "min": 10, "max": 20, "step": 2},
                "atr_multiplier": {"type": "float",  "min": 1.5, "max": 2.5, "step": 0.25},
                "session_filter": {"type": "choice", "choices": ["london", "new_york", "london_new_york"]},
            }
        }
    }


@pytest.fixture
def candidate() -> CandidateParameterSet:
    return CandidateParameterSet.create(
        zone_name="safe",
        parameters={
            "rsi_period": 14,
            "atr_multiplier": 2.0,
            "session_filter": "new_york",
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# Shared helper — synchronous fake executor
# ─────────────────────────────────────────────────────────────────────────────

def _run_with_fake_executor(
    fake_evaluate_perturbation,
    candidate,
    parameter_space_def,
    scenario,
    tmp_path,
    spike_threshold=0.15,
    max_steps=2,
    baseline=0.60,
) -> SensitivityProfile:
    """
    Patch _evaluate_perturbation and ProcessPoolExecutor so tests run
    synchronously with no filesystem access and no real strategy calls.
    """
    with patch(
        "src.backtesting.evaluation.sensitivity._evaluate_perturbation",
        side_effect=fake_evaluate_perturbation,
    ), patch(
        "src.backtesting.evaluation.sensitivity.ProcessPoolExecutor"
    ) as mock_pool_cls:

        mock_pool = MagicMock()
        mock_pool_cls.return_value.__enter__ = lambda s: mock_pool
        mock_pool_cls.return_value.__exit__ = MagicMock(return_value=False)

        def fake_submit(fn, *args, **kwargs):
            f = Future()
            try:
                f.set_result(fn(*args, **kwargs))
            except Exception as exc:
                f.set_exception(exc)
            return f

        mock_pool.submit = fake_submit

        return evaluate_sensitivity(
            candidate=candidate,
            baseline_fitness=baseline,
            parameter_space_def=parameter_space_def,
            base_yaml_path=tmp_path / "base.yaml",  # does not need to exist
            temp_dir=tmp_path,
            scenario=scenario,
            spike_threshold=spike_threshold,
            max_steps=max_steps,
            max_workers=1,
        )


# ─────────────────────────────────────────────────────────────────────────────
# _perturb_value tests
# ─────────────────────────────────────────────────────────────────────────────

class TestPerturbValue:
    def test_int_positive_step(self):
        pd = {"type": "int", "min": 10, "max": 20, "step": 2}
        assert _perturb_value(14, +1, pd) == 16

    def test_int_negative_step(self):
        pd = {"type": "int", "min": 10, "max": 20, "step": 2}
        assert _perturb_value(14, -1, pd) == 12

    def test_int_out_of_bounds_returns_none(self):
        pd = {"type": "int", "min": 10, "max": 20, "step": 2}
        assert _perturb_value(10, -1, pd) is None
        assert _perturb_value(20, +1, pd) is None

    def test_float_positive_step(self):
        pd = {"type": "float", "min": 1.5, "max": 2.5, "step": 0.25}
        assert _perturb_value(2.0, +1, pd) == pytest.approx(2.25)

    def test_float_negative_step(self):
        pd = {"type": "float", "min": 1.5, "max": 2.5, "step": 0.25}
        assert _perturb_value(2.0, -1, pd) == pytest.approx(1.75)

    def test_float_out_of_bounds_returns_none(self):
        pd = {"type": "float", "min": 1.5, "max": 2.5, "step": 0.25}
        assert _perturb_value(1.5, -1, pd) is None
        assert _perturb_value(2.5, +1, pd) is None

    def test_choice_positive_step(self):
        pd = {"type": "choice", "choices": ["london", "new_york", "london_new_york"]}
        assert _perturb_value("new_york", +1, pd) == "london_new_york"

    def test_choice_negative_step(self):
        pd = {"type": "choice", "choices": ["london", "new_york", "london_new_york"]}
        assert _perturb_value("new_york", -1, pd) == "london"

    def test_choice_out_of_bounds_returns_none(self):
        pd = {"type": "choice", "choices": ["london", "new_york", "london_new_york"]}
        assert _perturb_value("london", -1, pd) is None
        assert _perturb_value("london_new_york", +1, pd) is None

    def test_choice_two_steps(self):
        pd = {"type": "choice", "choices": ["london", "new_york", "london_new_york"]}
        assert _perturb_value("london", +2, pd) == "london_new_york"
        assert _perturb_value("london_new_york", -2, pd) == "london"


class TestStepOffsets:
    def test_default_two_steps(self):
        assert _step_offsets(2) == [-2, -1, 1, 2]

    def test_one_step(self):
        assert _step_offsets(1) == [-1, 1]

    def test_three_steps(self):
        assert _step_offsets(3) == [-3, -2, -1, 1, 2, 3]


# ─────────────────────────────────────────────────────────────────────────────
# evaluate_sensitivity tests
# ─────────────────────────────────────────────────────────────────────────────

class TestEvaluateSensitivity:

    def test_flat_profile_no_spike(self, candidate, scenario, parameter_space_def, tmp_path):
        """Uniform fitness → spike_detected=False, profile_complete=True, all deltas=0."""
        baseline = 0.60

        def fake_eval(base_cand, param_name, perturbed_value, base_yaml, temp_dir, scen, min_trades):
            return (param_name, perturbed_value, baseline, None)   # same fitness → delta = 0

        profile = _run_with_fake_executor(
            fake_eval, candidate, parameter_space_def, scenario, tmp_path, baseline=baseline
        )

        assert profile.candidate_id == candidate.candidate_id
        assert profile.baseline_fitness == pytest.approx(baseline)
        assert profile.spike_detected is False
        assert len(profile.spike_parameters) == 0
        assert profile.profile_complete is True
        for ps in profile.parameter_sensitivities:
            assert ps.fitness_delta == pytest.approx(0.0, abs=1e-9)

    def test_spike_detection(self, candidate, scenario, parameter_space_def, tmp_path):
        """atr_multiplier returns 0.30 (delta=-0.30 > threshold=0.15) → spike on atr_multiplier only."""
        baseline = 0.60

        def fake_eval(base_cand, param_name, perturbed_value, base_yaml, temp_dir, scen, min_trades):
            if param_name == "atr_multiplier":
                return (param_name, perturbed_value, 0.30, None)   # large drop → spike
            return (param_name, perturbed_value, 0.60, None)

        profile = _run_with_fake_executor(
            fake_eval, candidate, parameter_space_def, scenario, tmp_path,
            spike_threshold=0.15, baseline=baseline
        )

        assert profile.spike_detected is True
        assert "atr_multiplier" in profile.spike_parameters
        assert "rsi_period" not in profile.spike_parameters
        assert "session_filter" not in profile.spike_parameters

    def test_profile_incomplete_flag(self, candidate, scenario, parameter_space_def, tmp_path):
        """>50% of evaluations fail → profile_complete=False."""
        baseline = 0.60

        def fake_eval(base_cand, param_name, perturbed_value, base_yaml, temp_dir, scen, min_trades):
            return (param_name, perturbed_value, None, "evaluation_failed")   # all fail

        profile = _run_with_fake_executor(
            fake_eval, candidate, parameter_space_def, scenario, tmp_path, baseline=baseline
        )

        assert profile.profile_complete is False

    def test_step_bounds_never_out_of_zone(self, parameter_space_def):
        """Boundary values: ±steps that exceed zone bounds return None."""
        int_def = {"type": "int", "min": 10, "max": 20, "step": 2}
        assert _perturb_value(10, -1, int_def) is None
        assert _perturb_value(10, -2, int_def) is None
        assert _perturb_value(20, +1, int_def) is None
        assert _perturb_value(20, +2, int_def) is None

        float_def = {"type": "float", "min": 1.5, "max": 2.5, "step": 0.25}
        assert _perturb_value(1.5, -1, float_def) is None
        assert _perturb_value(2.5, +1, float_def) is None

        choice_def = {"type": "choice", "choices": ["a", "b", "c"]}
        assert _perturb_value("a", -1, choice_def) is None
        assert _perturb_value("a", -2, choice_def) is None
        assert _perturb_value("c", +1, choice_def) is None
        assert _perturb_value("c", +2, choice_def) is None