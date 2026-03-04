"""
tests/backtesting/unit/test_7b_audit_m_series.py
-------------------------------------------------
Sub-block 7B regression tests — audit M-series P1/P2 fixes.

Tests:
  M05-01  Valid parameter names → Stage 0 passes
  M05-02  Unknown parameter name → Stage 0 raises ValueError (fast-fail)
  M04-01  Path that hits ruin reports worst_drawdown = 1.0
  M03-01  conservative scenario (threshold 0.20) flags collapse that
          capital_accumulation (threshold 0.40) does not
  M03-02  window_collapse_flag=False when all windows below threshold
  M02-01  normalisation_pnl_ref_points flows from ScenarioProfile into fitness score
  M02-02  normalisation_freq_ref_trades_per_week flows from ScenarioProfile into fitness score
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

# ── sys.path anchor ───────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.utils.paths import PROJECT_ROOT  # noqa: E402

import numpy as np  # noqa: E402

from src.backtesting.contracts import (  # noqa: E402
    CandidateResult,
    ScenarioProfile,
    WFOWindowResult,
)
from src.backtesting.monte_carlo.mc_metrics import compute_metrics  # noqa: E402
from src.backtesting.wfo.consistency_scorer import compute_consistency  # noqa: E402
from src.backtesting.fitness import evaluate_fitness  # noqa: E402
from datetime import UTC, datetime  # noqa: E402


# ── Shared helpers ─────────────────────────────────────────────────────────────

def _make_scenario(
    wfo_collapse_threshold: float = 0.40,
    normalisation_pnl_ref_points: float = 5_000.0,
    normalisation_freq_ref_trades_per_week: float = 20.0,
    normalisation_drawdown_ref_points: float = 10_000.0,
) -> ScenarioProfile:
    """Build a minimal valid ScenarioProfile with configurable M-02/M-03 fields."""
    return ScenarioProfile(
        name="test",
        description="test scenario",
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
        report_emphasis=("wfo_score", "ruin_probability"),
        wfo_collapse_drawdown_threshold=wfo_collapse_threshold,
        normalisation_drawdown_ref_points=normalisation_drawdown_ref_points,
        normalisation_pnl_ref_points=normalisation_pnl_ref_points,
        normalisation_freq_ref_trades_per_week=normalisation_freq_ref_trades_per_week,
    )


def _make_window_result(
    candidate_id: str,
    window_id: str,
    net_pnl: float,
    max_drawdown: float,
) -> WFOWindowResult:
    return WFOWindowResult(
        candidate_id=candidate_id,
        window_id=window_id,
        evaluated_at=datetime.now(UTC),
        fitness_score=0.60,
        total_trades=30,
        net_pnl=net_pnl,
        max_drawdown=max_drawdown,
        win_rate=55.0,
        expectancy=2.0,
        profit_factor=1.5,
        oos_delta=None,
        error=None,
    )


def _make_passing_metrics():
    """Return a mock MetricsReport that passes all loose test constraints."""
    m = MagicMock()
    m.win_rate = 55.0          # 55% → 0.55 fraction, above min 0.05
    m.max_drawdown = -500.0    # -500 pts / 10000 = 0.05 fraction, below max 0.99
    m.losing_streak = 3
    m.trades_per_week = 10.0
    m.expectancy_points = 2.0
    m.profit_factor = 1.5
    m.total_pnl_points = 2500.0
    return m


# ── M-05 Tests ────────────────────────────────────────────────────────────────

class TestM05ParameterNameValidation:

    def test_m05_01_valid_parameter_names_pass(self):
        """
        M05-01: _validate_parameter_names raises nothing when all enabled zone
        parameter names are present in strategy_runner._PARAM_KEY_MAP.
        """
        from src.backtesting.orchestrator import _validate_parameter_names
        from src.backtesting.strategy_runner import _PARAM_KEY_MAP

        # Build a config whose enabled zone uses only known param names
        known_params = list(_PARAM_KEY_MAP.keys())[:3]  # pick first 3 known names
        config = {
            "zones": {
                "safe": {
                    "enabled": True,
                    "parameters": {k: {} for k in known_params},
                }
            }
        }
        # Should not raise
        _validate_parameter_names(config)

    def test_m05_02_unknown_parameter_name_raises(self):
        """
        M05-02: _validate_parameter_names raises ValueError containing the
        unknown parameter name when an enabled zone has an unrecognised param.
        """
        from src.backtesting.orchestrator import _validate_parameter_names

        config = {
            "zones": {
                "safe": {
                    "enabled": True,
                    "parameters": {
                        "rsi_period": {},          # valid
                        "completely_fake_param": {}, # invalid — not in _PARAM_KEY_MAP
                    },
                }
            }
        }
        with pytest.raises(ValueError) as exc_info:
            _validate_parameter_names(config)

        assert "completely_fake_param" in str(exc_info.value), (
            f"M05-02 FAIL: expected 'completely_fake_param' in error message; "
            f"got: {exc_info.value}"
        )


# ── M-04 Tests ────────────────────────────────────────────────────────────────

class TestM04ZeroEquityDrawdown:

    def test_m04_01_ruined_path_reports_drawdown_1_0(self):
        """
        M04-01: A path that hits ruin (equity ≤ ruin_floor) must report
        worst_drawdown_across_paths = 1.0, not an underestimated value.

        Scenario: path starts at 10000, rises to 12000 (running max), then
        crashes to 0 (ruin). Without the fix, running-max calculation gives
        drawdown = (12000 - 0) / 12000 = 1.0 in this trivial case, but a more
        subtle variant where the path briefly recovers mid-crash can produce < 1.0.

        We test the direct clamp: inject one non-ruined path and one ruined path
        where the ruined path's geometric drawdown via running-max would be < 1.0
        (equity goes up to 15000 then crashes to ruin_floor - 1).
        """
        starting_equity = 10_000.0
        ruin_threshold = 0.10   # ruin_floor = 1000.0

        # Path 0 (non-ruined): steady growth
        path_0 = np.array([10000, 10500, 11000, 11500, 12000], dtype=float)

        # Path 1 (ruined): rises to 15000 before crashing to 999 (below ruin_floor=1000)
        # Without M-04 fix: running_max[-1]=15000, equity[-1]=999 but path minimum
        # could give drawdown = (15000-999)/15000 = 0.9334 — NOT 1.0
        # With M-04 fix: ruined_paths[1]=True → worst_drawdown_per_path[1] = 1.0
        path_1 = np.array([10000, 12000, 15000, 8000, 999], dtype=float)

        equity_paths = np.array([path_0, path_1])

        _, worst_dd, ruin_prob, _ = compute_metrics(
            equity_paths=equity_paths,
            starting_equity=starting_equity,
            ruin_threshold=ruin_threshold,
        )

        assert ruin_prob > 0.0, "M04-01 FAIL: path_1 should have triggered ruin"
        assert worst_dd == 1.0, (
            f"M04-01 FAIL: worst_drawdown_across_paths should be 1.0 for ruined path; "
            f"got {worst_dd:.6f}"
        )


# ── M-03 Tests ────────────────────────────────────────────────────────────────

class TestM03CollapseThreshold:

    def test_m03_01_conservative_flags_collapse_that_capital_accum_does_not(self):
        """
        M03-01: A window with max_drawdown=0.25 triggers window_collapse_flag
        for conservative scenario (threshold=0.20) but NOT for capital_accumulation
        scenario (threshold=0.40). Verifies M-03 threshold flows from ScenarioProfile.
        """
        conservative = _make_scenario(wfo_collapse_threshold=0.20)
        capital_accum = _make_scenario(wfo_collapse_threshold=0.40)

        cid = "cand_0001"
        # Window with 25% drawdown — above 0.20 but below 0.40
        results = [
            _make_window_result(cid, "w1", net_pnl=100.0, max_drawdown=0.25),
            _make_window_result(cid, "w2", net_pnl=80.0,  max_drawdown=0.10),
            _make_window_result(cid, "w3", net_pnl=120.0, max_drawdown=0.08),
        ]

        score_conservative = compute_consistency(results, 3, conservative)
        score_capital = compute_consistency(results, 3, capital_accum)

        assert score_conservative.window_collapse_flag is True, (
            "M03-01 FAIL: conservative scenario (threshold=0.20) should flag "
            f"window with drawdown=0.25; window_collapse_flag={score_conservative.window_collapse_flag}"
        )
        assert score_capital.window_collapse_flag is False, (
            "M03-01 FAIL: capital_accumulation scenario (threshold=0.40) should NOT flag "
            f"window with drawdown=0.25; window_collapse_flag={score_capital.window_collapse_flag}"
        )

    def test_m03_02_no_collapse_when_all_windows_below_threshold(self):
        """
        M03-02: window_collapse_flag=False when all windows have drawdown
        strictly below the configured threshold.
        """
        scenario = _make_scenario(wfo_collapse_threshold=0.40)
        cid = "cand_0002"
        results = [
            _make_window_result(cid, "w1", net_pnl=50.0,  max_drawdown=0.10),
            _make_window_result(cid, "w2", net_pnl=75.0,  max_drawdown=0.15),
            _make_window_result(cid, "w3", net_pnl=60.0,  max_drawdown=0.20),
        ]
        score = compute_consistency(results, 3, scenario)
        assert score.window_collapse_flag is False, (
            f"M03-02 FAIL: No window exceeds threshold 0.40; expected False, "
            f"got {score.window_collapse_flag}"
        )


# ── M-02 Tests ────────────────────────────────────────────────────────────────

class TestM02FitnessNormalisation:

    def _make_candidate_result(self, metrics_mock) -> CandidateResult:
        r = MagicMock(spec=CandidateResult)
        r.candidate_id = "cand_m02_test"
        r.is_valid = True
        r.metrics = metrics_mock
        r.trades = MagicMock()
        r.error = None
        return r

    def test_m02_01_pnl_ref_points_flows_into_fitness_score(self):
        """
        M02-01: A candidate with net_pnl=2500 pts scores higher fitness when
        normalisation_pnl_ref_points=2500 (pnl_norm=1.0) than when it is 5000
        (pnl_norm=0.5). Confirms the ref point flows from ScenarioProfile into
        _compute_weighted_score, not from a hardcoded constant.
        """
        metrics = _make_passing_metrics()
        metrics.total_pnl_points = 2500.0

        result = self._make_candidate_result(metrics)

        scenario_tight = _make_scenario(normalisation_pnl_ref_points=2500.0)
        scenario_loose = _make_scenario(normalisation_pnl_ref_points=5000.0)

        fr_tight = evaluate_fitness(result, scenario_tight)
        fr_loose = evaluate_fitness(result, scenario_loose)

        assert fr_tight.passed_constraints, "M02-01 FAIL: candidate should pass constraints"
        assert fr_loose.passed_constraints, "M02-01 FAIL: candidate should pass constraints"
        assert fr_tight.fitness_score > fr_loose.fitness_score, (
            f"M02-01 FAIL: tighter pnl ref (2500) should give higher fitness than loose (5000). "
            f"tight={fr_tight.fitness_score:.4f}  loose={fr_loose.fitness_score:.4f}"
        )

    def test_m02_02_freq_ref_trades_per_week_flows_into_fitness_score(self):
        """
        M02-02: A candidate with trades_per_week=10 scores higher fitness when
        normalisation_freq_ref_trades_per_week=10 (freq_norm=1.0) than when it
        is 20 (freq_norm=0.5). Confirms the ref point flows from ScenarioProfile.
        """
        metrics = _make_passing_metrics()
        metrics.trades_per_week = 10.0

        result = self._make_candidate_result(metrics)

        scenario_tight = _make_scenario(normalisation_freq_ref_trades_per_week=10.0)
        scenario_loose = _make_scenario(normalisation_freq_ref_trades_per_week=20.0)

        fr_tight = evaluate_fitness(result, scenario_tight)
        fr_loose = evaluate_fitness(result, scenario_loose)

        assert fr_tight.passed_constraints, "M02-02 FAIL: candidate should pass constraints"
        assert fr_loose.passed_constraints, "M02-02 FAIL: candidate should pass constraints"
        assert fr_tight.fitness_score > fr_loose.fitness_score, (
            f"M02-02 FAIL: tighter freq ref (10) should give higher fitness than loose (20). "
            f"tight={fr_tight.fitness_score:.4f}  loose={fr_loose.fitness_score:.4f}"
        )