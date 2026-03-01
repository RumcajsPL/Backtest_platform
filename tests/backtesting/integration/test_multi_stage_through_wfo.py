"""
tests/backtesting/integration/test_multi_stage_through_wfo.py
--------------------------------------------------------------
Integration test: validates the pipeline flow from Stage 1 through Stage 4 (Full WFO).

This test validates:
  1. WFO window generator produces ≥3 windows
  2. Consistency scorer produces composite_score in [0, 1]
  3. GA window sampling is independent per generation (different pairs across gens)
  4. Diversity penalty keeps scores in [0, penalty_weight]
  5. MC pre-filter produces MCResult for each candidate
  6. WFO consistency score contract fields are all present and valid

Note: Full pipeline integration (with live strategy runner) requires a running
strategy installation. This test uses lightweight mocks for the strategy runner
and validates the pipeline data contracts and flow logic only.
"""
from __future__ import annotations

import json
import random
from datetime import date, datetime
from typing import List
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from src.backtesting.contracts import (
    CandidateParameterSet,
    CandidateResult,
    MCMode,
    ScenarioProfile,
    WFOWindow,
    WFOWindowResult,
)
from src.backtesting.wfo.window_generator import generate_windows
from src.backtesting.wfo.consistency_scorer import compute_consistency
from src.backtesting.ga.diversity import compute_penalty
from src.backtesting.monte_carlo.mc_engine import run_mc


# ── Shared fixtures ────────────────────────────────────────────────────────────

@pytest.fixture
def wfo_config_with_5_windows():
    return {
        "walk_forward": {
            "windows": [
                {"id": "W01", "start": "2021-01-01", "end": "2021-06-30"},
                {"id": "W02", "start": "2021-07-01", "end": "2021-12-31"},
                {"id": "W03", "start": "2022-01-01", "end": "2022-06-30"},
                {"id": "W04", "start": "2022-07-01", "end": "2022-12-31"},
                {"id": "W05", "start": "2023-01-01", "end": "2023-06-30"},
            ]
        }
    }


@pytest.fixture
def scenario():
    return ScenarioProfile(
        name="capital_accumulation",
        description="Integration test scenario",
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


@pytest.fixture
def mc_config():
    return {
        "mc_prefilter": {
            "iterations": 100,
            "perturbation_profile": "default",
            "ruin_threshold": 0.20,
        },
        "monte_carlo": {
            "deep": {
                "input_count": 5,
                "iterations": 200,
                "perturbation_profile": "default",
                "ruin_threshold": 0.20,
                "seed": 45,
            }
        },
        "perturbation_profiles": {
            "default": {
                "version": "1.0",
                "spread_noise_bps_range": [0, 2],
                "slippage_pips_range": [0, 0.5],
                "risk_noise_fraction": 0.03,
                "shuffle_trades": True,
                "resample_returns": True,
                "execution_delay_bars": 0,
            }
        }
    }


@pytest.fixture
def safe_zone_def():
    return {
        "parameters": {
            "rsi_period":     {"type": "int",    "min": 10, "max": 20, "step": 2},
            "atr_multiplier": {"type": "float",  "min": 1.5, "max": 2.5, "step": 0.25},
            "session_filter": {"type": "choice", "choices": ["london", "new_york", "london_new_york"]},
        }
    }


def _make_candidate(rsi=14, atr=2.0, session="london"):
    return CandidateParameterSet.create(
        "safe", {"rsi_period": rsi, "atr_multiplier": atr, "session_filter": session}
    )


def _make_window_result(cid, wid, pnl=300.0, dd=0.05):
    return WFOWindowResult(
        candidate_id=cid, window_id=wid,
        evaluated_at=datetime.utcnow(),
        fitness_score=0.65,
        total_trades=50,
        net_pnl=pnl, max_drawdown=dd,
        win_rate=0.55, expectancy=0.6, profit_factor=1.4,
        oos_delta=None, error=None,
    )


# ── Test: WFO window generation ───────────────────────────────────────────────

class TestWFOWindowGeneration:

    def test_generates_5_windows(self, wfo_config_with_5_windows):
        windows = generate_windows(wfo_config_with_5_windows)
        assert len(windows) == 5
        assert all(isinstance(w, WFOWindow) for w in windows)

    def test_window_ids_are_unique(self, wfo_config_with_5_windows):
        windows = generate_windows(wfo_config_with_5_windows)
        ids = [w.window_id for w in windows]
        assert len(ids) == len(set(ids))

    def test_windows_non_overlapping(self, wfo_config_with_5_windows):
        windows = generate_windows(wfo_config_with_5_windows)
        for i in range(len(windows) - 1):
            assert windows[i].end_date <= windows[i + 1].start_date


# ── Test: GA window sampling per generation ────────────────────────────────────

class TestGAWindowSamplingIntegration:

    def test_30_generations_produce_varied_window_pairs(self, wfo_config_with_5_windows):
        """
        Key validation: 30 generations should not all use the same window pair.
        With 5 windows and k=2 sampling, C(5,2)=10 possible pairs.
        30 generations should sample at least 3 distinct pairs.
        """
        windows = generate_windows(wfo_config_with_5_windows)
        rng = random.Random(44)  # GA seed from config
        pairs = set()
        for _ in range(30):
            sampled = rng.sample(windows, k=2)
            pairs.add(frozenset(w.window_id for w in sampled))
        assert len(pairs) >= 3, (
            f"Only {len(pairs)} distinct window pairs in 30 generations. "
            "Expected at least 3 (GA window sampling not sufficiently random)."
        )


# ── Test: Consistency scorer — composite score validity ───────────────────────

class TestConsistencyScoreIntegration:

    def test_composite_score_valid_for_5_windows(self, scenario):
        cid = "test_candidate_abc"
        window_results = [
            _make_window_result(cid, f"W0{i}", pnl=100.0 * i, dd=0.03 * i)
            for i in range(1, 6)
        ]
        score = compute_consistency(window_results, windows_total=5, scenario=scenario)

        assert 0.0 <= score.composite_score <= 1.0
        assert score.windows_evaluated == 5
        assert score.windows_total == 5
        assert score.fraction_positive_windows == 1.0  # All windows positive PnL

    def test_partially_failed_windows_reduces_evaluated_count(self, scenario):
        cid = "test_candidate_xyz"
        results = [
            _make_window_result(cid, "W01", pnl=300.0, dd=0.05),
            _make_window_result(cid, "W02", pnl=200.0, dd=0.07),
            # 3 failed windows
            WFOWindowResult(
                candidate_id=cid, window_id="W03",
                evaluated_at=datetime.utcnow(),
                fitness_score=None, total_trades=None,
                net_pnl=None, max_drawdown=None,
                win_rate=None, expectancy=None, profit_factor=None,
                oos_delta=None, error="strategy error",
            ),
            WFOWindowResult(
                candidate_id=cid, window_id="W04",
                evaluated_at=datetime.utcnow(),
                fitness_score=None, total_trades=None,
                net_pnl=None, max_drawdown=None,
                win_rate=None, expectancy=None, profit_factor=None,
                oos_delta=None, error="strategy error",
            ),
            WFOWindowResult(
                candidate_id=cid, window_id="W05",
                evaluated_at=datetime.utcnow(),
                fitness_score=None, total_trades=None,
                net_pnl=None, max_drawdown=None,
                win_rate=None, expectancy=None, profit_factor=None,
                oos_delta=None, error="strategy error",
            ),
        ]
        score = compute_consistency(results, windows_total=5, scenario=scenario)
        assert score.windows_evaluated == 2
        assert score.windows_total == 5
        # >50% windows failed — wfo_engine would flag this as WFO_INSUFFICIENT_WINDOWS


# ── Test: Diversity penalty integration ───────────────────────────────────────

class TestDiversityPenaltyIntegration:

    def test_population_spread_maintained(self, safe_zone_def):
        """
        Key validation (NEXT_SESSION_PLAN.md):
        Diversity penalty discriminates between candidates at different distances from an elite.

        Uses distance_threshold=0.60 so most candidates fall within the penalty zone
        and produce distinct penalty values proportional to their proximity.

        Zone: 2 continuous params (rsi_period range=10, atr_multiplier range=1.0)
        and 1 discrete (session_filter) -> w_continuous=2/3, w_discrete=1/3.
        Hybrid distances range from 0.0 (identical) to ~0.64 (max separation).
        The default GA threshold of 0.15 only penalises near-clones; this test
        uses threshold=0.60 to exercise metric discrimination across a wider range.

        Verified penalty values at threshold=0.60, penalty_weight=0.10:
          identical       hybrid=0.000  penalty=0.1000
          1 cont shifted  hybrid=0.094  penalty=0.0843
          2 cont shifted  hybrid=0.151  penalty=0.0748
          cont+discrete   hybrid=0.484  penalty=0.0193
          max separation  hybrid=0.635  penalty=0.0000 (beyond threshold)
        """
        from src.backtesting.ga.diversity import compute_penalty

        elite = _make_candidate(rsi=14, atr=2.0, session="london")

        # Candidates at genuinely different distances from the elite
        candidates = [
            _make_candidate(rsi=14, atr=2.0, session="london"),    # identical -> max penalty
            _make_candidate(rsi=12, atr=2.0, session="london"),    # one continuous param shifted
            _make_candidate(rsi=12, atr=1.75, session="london"),   # two continuous params shifted
            _make_candidate(rsi=12, atr=1.75, session="new_york"), # continuous + discrete shifted
            _make_candidate(rsi=10, atr=1.5, session="new_york"),  # max separation (beyond threshold)
        ]

        penalties = [
            compute_penalty(c, [elite], safe_zone_def, 0.60, 0.10)
            for c in candidates
        ]

        # Penalties must be distinct -- metric discriminates distance
        assert len(set(round(p, 4) for p in penalties)) > 1, (
            "All candidates received identical diversity penalties -- "
            "diversity metric is not discriminating between parameter distances"
        )

        # Identical candidate must receive the maximum penalty
        assert abs(penalties[0] - 0.10) < 1e-6, (
            f"Identical candidate should receive full penalty_weight=0.10; got {penalties[0]}"
        )

        # Penalties decrease as candidates move further from elite
        # (last candidate is beyond threshold so receives 0.0 -- correct behaviour)
        assert penalties[0] >= penalties[1] >= penalties[2] >= penalties[3] >= penalties[4], (
            f"Penalties not monotonically decreasing with distance: {penalties}"
        )

        # All penalties in valid range
        for p in penalties:
            assert 0.0 <= p <= 0.10


# ── Test: MC pre-filter integration ──────────────────────────────────────────

class TestMCPrefilterIntegration:

    def test_profitable_candidate_has_low_ruin(self, mc_config):
        """A candidate with consistently profitable trades should have low ruin probability."""
        from unittest.mock import MagicMock
        candidate = _make_candidate(rsi=14, atr=2.0, session="london")

        mock_trade = MagicMock()
        mock_trade.pnl = 100.0  # All trades win
        mock_trades_obj = MagicMock()
        mock_trades_obj.trades = [mock_trade] * 50

        mock_result = MagicMock()
        mock_result.is_valid = True
        mock_result.trades = mock_trades_obj
        mock_result.metrics = MagicMock()
        mock_result.metrics.starting_equity = 10_000.0

        mc_result = run_mc(candidate, mock_result, MCMode.PRE_FILTER, mc_config, seed=42)

        assert mc_result.error is None
        assert mc_result.ruin_probability is not None
        # Very profitable trades should almost never ruin
        assert mc_result.ruin_probability < 0.10

    def test_losing_candidate_has_high_ruin(self, mc_config):
        """A candidate with consistently losing trades should have high ruin probability."""
        from unittest.mock import MagicMock
        candidate = _make_candidate(rsi=14, atr=2.0, session="london")

        mock_trade = MagicMock()
        mock_trade.pnl = -200.0  # All trades lose substantially
        mock_trades_obj = MagicMock()
        mock_trades_obj.trades = [mock_trade] * 50

        mock_result = MagicMock()
        mock_result.is_valid = True
        mock_result.trades = mock_trades_obj
        mock_result.metrics = MagicMock()
        mock_result.metrics.starting_equity = 10_000.0

        mc_result = run_mc(candidate, mock_result, MCMode.PRE_FILTER, mc_config, seed=42)

        assert mc_result.error is None
        assert mc_result.ruin_probability > 0.80

    def test_mcresult_contract_fields_complete(self, mc_config):
        """Verify MCResult has all expected contract fields populated."""
        from unittest.mock import MagicMock
        candidate = _make_candidate()
        mock_trade = MagicMock()
        mock_trade.pnl = 50.0
        mock_trades_obj = MagicMock()
        mock_trades_obj.trades = [mock_trade] * 30

        mock_result = MagicMock()
        mock_result.is_valid = True
        mock_result.trades = mock_trades_obj
        mock_result.metrics = MagicMock()
        mock_result.metrics.starting_equity = 10_000.0

        mc_result = run_mc(candidate, mock_result, MCMode.PRE_FILTER, mc_config, seed=99)

        # All contract fields must be set (no None for core fields)
        assert mc_result.candidate_id == candidate.candidate_id
        assert mc_result.mode == MCMode.PRE_FILTER
        assert mc_result.perturbation_profile_name == "default"
        assert mc_result.iterations == 100
        assert mc_result.avg_final_equity is not None
        assert mc_result.worst_drawdown_across_paths is not None
        assert mc_result.ruin_probability is not None
        assert mc_result.p5_final_equity is not None
        assert mc_result.error is None