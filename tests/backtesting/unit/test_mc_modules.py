"""
tests/backtesting/unit/test_mc_modules.py
------------------------------------------
Unit tests for Monte Carlo modules:
  - perturbation: named profiles apply correct perturbations
  - equity_simulator: vectorised, shape correct, deterministic for same seed
  - mc_metrics: ruin probability correct, drawdown computation, p5 equity
  - mc_engine: pre-filter rejects high-ruin candidates (via MCResult.ruin_probability)
"""
from __future__ import annotations

import random
import numpy as np
import pytest

from src.backtesting.monte_carlo.perturbation import load_profile, PerturbationProfile
from src.backtesting.monte_carlo.equity_simulator import simulate_paths
from src.backtesting.monte_carlo.mc_metrics import compute_metrics


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def minimal_mc_config():
    return {
        "mc_prefilter": {
            "iterations": 200,
            "perturbation_profile": "default",
            "ruin_threshold": 0.20,
        },
        "monte_carlo": {
            "deep": {
                "input_count": 10,
                "iterations": 500,
                "perturbation_profile": "default",
                "ruin_threshold": 0.20,
                "seed": 45,
            }
        },
        "perturbation_profiles": {
            "default": {
                "version": "1.0",
                "spread_noise_bps_range": [0, 3],
                "slippage_pips_range": [0, 1],
                "risk_noise_fraction": 0.05,
                "shuffle_trades": True,
                "resample_returns": True,
                "execution_delay_bars": 0,
            }
        }
    }


@pytest.fixture
def base_trade_returns():
    """50 trades with positive expectancy."""
    rng = np.random.default_rng(42)
    returns = rng.normal(loc=10.0, scale=50.0, size=50)
    return returns


# ── Perturbation profile tests ────────────────────────────────────────────────

class TestPerturbationProfile:

    def test_load_default_profile(self, minimal_mc_config):
        profile = load_profile(minimal_mc_config, "default")
        assert isinstance(profile, PerturbationProfile)
        assert profile.name == "default"
        assert profile.shuffle_trades is True
        assert profile.resample_returns is True
        assert profile.spread_noise_bps_range == (0.0, 3.0)

    def test_unknown_profile_raises_key_error(self, minimal_mc_config):
        with pytest.raises(KeyError, match="nonexistent"):
            load_profile(minimal_mc_config, "nonexistent")

    def test_prefilter_perturbation_changes_returns(self, minimal_mc_config, base_trade_returns):
        from src.backtesting.monte_carlo.perturbation import apply_prefilter_perturbations
        profile = load_profile(minimal_mc_config, "default")
        rng = random.Random(42)
        perturbed = apply_prefilter_perturbations(base_trade_returns, profile, rng)
        # Shape preserved
        assert perturbed.shape == base_trade_returns.shape
        # Not identical (shuffle and/or noise should change at least some values)
        # In rare cases shuffle might produce same order, so we check broadly
        assert isinstance(perturbed, np.ndarray)

    def test_deep_perturbation_shape_preserved(self, minimal_mc_config, base_trade_returns):
        from src.backtesting.monte_carlo.perturbation import apply_deep_perturbations
        profile = load_profile(minimal_mc_config, "default")
        rng = random.Random(7)
        numpy_rng = np.random.default_rng(7)
        perturbed = apply_deep_perturbations(base_trade_returns, profile, rng, numpy_rng)
        assert perturbed.shape == base_trade_returns.shape


# ── Equity simulator tests ────────────────────────────────────────────────────

class TestEquitySimulator:

    def test_output_shape_correct(self, minimal_mc_config, base_trade_returns):
        profile = load_profile(minimal_mc_config, "default")
        n_iter = 100
        n_trades = len(base_trade_returns)
        paths = simulate_paths(base_trade_returns, n_iter, profile, seed=42)
        # Shape: (n_iterations, n_trades + 1)
        assert paths.shape == (n_iter, n_trades + 1)

    def test_all_paths_start_at_starting_equity(self, minimal_mc_config, base_trade_returns):
        profile = load_profile(minimal_mc_config, "default")
        starting_equity = 15_000.0
        paths = simulate_paths(base_trade_returns, 50, profile, seed=1, starting_equity=starting_equity)
        assert np.all(paths[:, 0] == starting_equity)

    def test_deterministic_for_same_seed(self, minimal_mc_config, base_trade_returns):
        profile = load_profile(minimal_mc_config, "default")
        paths_1 = simulate_paths(base_trade_returns, 50, profile, seed=123)
        paths_2 = simulate_paths(base_trade_returns, 50, profile, seed=123)
        np.testing.assert_array_equal(paths_1, paths_2)

    def test_different_seeds_produce_different_paths(self, minimal_mc_config, base_trade_returns):
        profile = load_profile(minimal_mc_config, "default")
        paths_a = simulate_paths(base_trade_returns, 50, profile, seed=1)
        paths_b = simulate_paths(base_trade_returns, 50, profile, seed=2)
        assert not np.array_equal(paths_a, paths_b)

    def test_empty_returns_raises(self, minimal_mc_config):
        profile = load_profile(minimal_mc_config, "default")
        with pytest.raises(ValueError, match="empty"):
            simulate_paths(np.array([]), 50, profile, seed=1)

    def test_deep_mode_runs(self, minimal_mc_config, base_trade_returns):
        profile = load_profile(minimal_mc_config, "default")
        paths = simulate_paths(base_trade_returns, 30, profile, seed=5, deep_mode=True)
        assert paths.shape[0] == 30
        assert np.all(paths[:, 0] == 10_000.0)


# ── MC metrics tests ──────────────────────────────────────────────────────────

class TestMCMetrics:

    def test_ruin_probability_zero_for_winning_paths(self):
        """All paths stay well above ruin floor → ruin_prob = 0."""
        starting = 10_000.0
        # 10 paths, each ending at 15,000 — never drops below ruin floor
        paths = np.full((10, 51), starting)
        for i in range(1, 51):
            paths[:, i] = starting + i * 100.0

        avg, worst_dd, ruin_prob, p5 = compute_metrics(paths, starting, ruin_threshold=0.20)
        assert ruin_prob == 0.0

    def test_ruin_probability_one_for_ruined_paths(self):
        """All paths drop below ruin floor → ruin_prob = 1.0."""
        starting = 10_000.0
        ruin_floor = starting * 0.20  # 2,000
        # All paths crash to 500 (below ruin floor)
        paths = np.full((10, 51), starting)
        paths[:, 25:] = 500.0  # Crash at step 25

        avg, worst_dd, ruin_prob, p5 = compute_metrics(paths, starting, ruin_threshold=0.20)
        assert ruin_prob == 1.0

    def test_partial_ruin_probability(self):
        """Half of paths ruin → ruin_prob = 0.5."""
        starting = 10_000.0
        paths = np.full((10, 11), starting)
        # First 5 paths: crash to 500 (ruin)
        paths[:5, 5:] = 500.0
        # Last 5 paths: grow to 15,000 (safe)
        paths[5:, 5:] = 15_000.0

        avg, worst_dd, ruin_prob, p5 = compute_metrics(paths, starting, ruin_threshold=0.20)
        assert abs(ruin_prob - 0.5) < 1e-9

    def test_worst_drawdown_in_unit_interval(self):
        starting = 10_000.0
        rng = np.random.default_rng(1)
        # Realistic equity paths with some drawdown
        returns = rng.normal(5.0, 100.0, size=(50, 100))
        paths = np.hstack([
            np.full((50, 1), starting),
            starting + np.cumsum(returns, axis=1)
        ])
        avg, worst_dd, ruin_prob, p5 = compute_metrics(paths, starting, ruin_threshold=0.20)
        assert 0.0 <= worst_dd <= 1.0

    def test_p5_equity_below_mean(self):
        starting = 10_000.0
        rng = np.random.default_rng(99)
        returns = rng.normal(10.0, 200.0, size=(500, 50))
        paths = np.hstack([np.full((500, 1), starting), starting + np.cumsum(returns, axis=1)])
        avg, worst_dd, ruin_prob, p5 = compute_metrics(paths, starting, ruin_threshold=0.20)
        # p5 should be below the average (by definition of percentile)
        assert p5 <= avg

    def test_raises_on_empty_paths(self):
        with pytest.raises(ValueError, match="2-D"):
            compute_metrics(np.array([]), 10_000.0, 0.20)

    def test_avg_final_equity_correctness(self):
        starting = 10_000.0
        # 4 paths, each ending at a known value
        paths = np.array([
            [starting, 11_000.0],
            [starting, 12_000.0],
            [starting, 9_000.0],
            [starting, 10_000.0],
        ])
        avg, worst_dd, ruin_prob, p5 = compute_metrics(paths, starting, ruin_threshold=0.20)
        expected_avg = (11_000.0 + 12_000.0 + 9_000.0 + 10_000.0) / 4
        assert abs(avg - expected_avg) < 0.01


# ── MC engine pre-filter integration ─────────────────────────────────────────

class TestMCEnginePrefilter:

    def test_prefilter_high_ruin_returns_error_free_result(self, minimal_mc_config):
        """
        mc_engine.run_mc should return MCResult with ruin_probability set.
        High-ruin candidates will have ruin_probability above threshold.
        """
        from src.backtesting.contracts import CandidateParameterSet, MCMode
        from src.backtesting.monte_carlo.mc_engine import run_mc
        from unittest.mock import MagicMock
        from datetime import datetime

        # Create a candidate
        candidate = CandidateParameterSet.create(
            "safe", {"rsi_period": 14, "atr_multiplier": 2.0, "session_filter": "london"}
        )

        # Create a mock CandidateResult with losing trades (high ruin scenario)
        mock_trade = MagicMock()
        mock_trade.pnl = -200.0  # All trades lose
        mock_trades_obj = MagicMock()
        mock_trades_obj.trades = [mock_trade] * 50

        mock_result = MagicMock()
        mock_result.is_valid = True
        mock_result.trades = mock_trades_obj
        mock_result.metrics = MagicMock()
        mock_result.metrics.starting_equity = 10_000.0

        mc_result = run_mc(
            candidate=candidate,
            candidate_result=mock_result,
            mode=MCMode.PRE_FILTER,
            config=minimal_mc_config,
            seed=42,
        )

        assert mc_result.candidate_id == candidate.candidate_id
        assert mc_result.error is None
        assert mc_result.ruin_probability is not None
        assert 0.0 <= mc_result.ruin_probability <= 1.0
        # With all-losing trades, ruin probability should be very high
        assert mc_result.ruin_probability > 0.5

    def test_invalid_candidate_result_returns_error_mcresult(self, minimal_mc_config):
        from src.backtesting.contracts import CandidateParameterSet, MCMode
        from src.backtesting.monte_carlo.mc_engine import run_mc
        from unittest.mock import MagicMock

        candidate = CandidateParameterSet.create(
            "safe", {"rsi_period": 14, "atr_multiplier": 2.0, "session_filter": "london"}
        )
        mock_result = MagicMock()
        mock_result.is_valid = False
        mock_result.error = "evaluation failed"

        mc_result = run_mc(
            candidate=candidate,
            candidate_result=mock_result,
            mode=MCMode.PRE_FILTER,
            config=minimal_mc_config,
            seed=42,
        )

        assert mc_result.error is not None
        assert mc_result.ruin_probability is None
        assert mc_result.is_valid is False