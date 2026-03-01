"""
tests/backtesting/unit/test_wfo_modules.py
------------------------------------------
Unit tests for WFO modules:
  - window_generator: min windows validation, no overlaps, date ordering
  - consistency_scorer: composite score in [0,1], weights sum, zero valid windows
  - wfo_evaluator: not tested here (requires strategy runner — integration test)
"""
from __future__ import annotations

import pytest
from datetime import date
from unittest.mock import MagicMock

from src.utils.paths import PROJECT_ROOT, config_path

from src.backtesting.contracts import (
    ScenarioProfile,
    WFOWindow,
    WFOWindowResult,
)
from src.backtesting.wfo.window_generator import generate_windows, extract_window_ids
from src.backtesting.wfo.consistency_scorer import compute_consistency


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def minimal_valid_config():
    return {
        "walk_forward": {
            "windows": [
                {"id": "W01", "start": "2022-01-01", "end": "2022-06-30"},
                {"id": "W02", "start": "2022-07-01", "end": "2022-12-31"},
                {"id": "W03", "start": "2023-01-01", "end": "2023-06-30"},
            ]
        }
    }


@pytest.fixture
def capital_accumulation_scenario():
    """Minimal ScenarioProfile matching capital_accumulation from TECHNICAL_SPEC.md"""
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


def _make_window_result(
    candidate_id: str,
    window_id: str,
    net_pnl: float,
    max_drawdown: float,
    fitness_score: float,
) -> WFOWindowResult:
    from datetime import datetime
    return WFOWindowResult(
        candidate_id=candidate_id,
        window_id=window_id,
        evaluated_at=datetime.utcnow(),
        fitness_score=fitness_score,
        total_trades=50,
        net_pnl=net_pnl,
        max_drawdown=max_drawdown,
        win_rate=0.55,
        expectancy=0.6,
        profit_factor=1.4,
        oos_delta=None,
        error=None,
    )


# ── window_generator tests ────────────────────────────────────────────────────

class TestWindowGenerator:

    def test_generates_correct_windows(self, minimal_valid_config):
        windows = generate_windows(minimal_valid_config)
        assert len(windows) == 3
        assert all(isinstance(w, WFOWindow) for w in windows)

    def test_windows_sorted_by_start_date(self, minimal_valid_config):
        # Shuffle the config order to verify sorting
        cfg = {
            "walk_forward": {
                "windows": [
                    {"id": "W03", "start": "2023-01-01", "end": "2023-06-30"},
                    {"id": "W01", "start": "2022-01-01", "end": "2022-06-30"},
                    {"id": "W02", "start": "2022-07-01", "end": "2022-12-31"},
                ]
            }
        }
        windows = generate_windows(cfg)
        starts = [w.start_date for w in windows]
        assert starts == sorted(starts)

    def test_min_windows_raises_on_fewer_than_3(self):
        """GA random sampling requires minimum 3 windows — validated here."""
        cfg = {
            "walk_forward": {
                "windows": [
                    {"id": "W01", "start": "2022-01-01", "end": "2022-06-30"},
                    {"id": "W02", "start": "2022-07-01", "end": "2022-12-31"},
                ]
            }
        }
        with pytest.raises(ValueError, match="Minimum 3 WFO windows"):
            generate_windows(cfg)

    def test_zero_windows_raises(self):
        cfg = {"walk_forward": {"windows": []}}
        with pytest.raises(ValueError, match="Minimum 3 WFO windows"):
            generate_windows(cfg)

    def test_overlapping_windows_raises(self):
        cfg = {
            "walk_forward": {
                "windows": [
                    {"id": "W01", "start": "2022-01-01", "end": "2022-07-15"},  # Ends mid-July
                    {"id": "W02", "start": "2022-07-01", "end": "2022-12-31"},  # Starts July 1 — overlaps
                    {"id": "W03", "start": "2023-01-01", "end": "2023-06-30"},
                ]
            }
        }
        with pytest.raises(ValueError, match="overlap"):
            generate_windows(cfg)

    def test_invalid_date_format_raises(self):
        cfg = {
            "walk_forward": {
                "windows": [
                    {"id": "W01", "start": "01-01-2022", "end": "2022-06-30"},  # Wrong format
                    {"id": "W02", "start": "2022-07-01", "end": "2022-12-31"},
                    {"id": "W03", "start": "2023-01-01", "end": "2023-06-30"},
                ]
            }
        }
        with pytest.raises(ValueError, match="invalid date"):
            generate_windows(cfg)

    def test_extract_window_ids(self, minimal_valid_config):
        windows = generate_windows(minimal_valid_config)
        ids = extract_window_ids(windows)
        assert isinstance(ids, tuple)
        assert len(ids) == 3
        assert "W01" in ids


# ── consistency_scorer tests ──────────────────────────────────────────────────

class TestConsistencyScorer:

    def test_composite_score_in_unit_interval(self, capital_accumulation_scenario):
        results = [
            _make_window_result("cand_1", "W01", net_pnl=500.0, max_drawdown=0.05, fitness_score=0.7),
            _make_window_result("cand_1", "W02", net_pnl=300.0, max_drawdown=0.08, fitness_score=0.65),
            _make_window_result("cand_1", "W03", net_pnl=200.0, max_drawdown=0.06, fitness_score=0.60),
        ]
        score = compute_consistency(results, windows_total=3, scenario=capital_accumulation_scenario)
        assert 0.0 <= score.composite_score <= 1.0

    def test_zero_valid_windows_returns_zero_score(self, capital_accumulation_scenario):
        """No valid windows → composite_score=0, window_collapse_flag=True."""
        from datetime import datetime
        failed = [
            WFOWindowResult(
                candidate_id="cand_1", window_id=f"W0{i}",
                evaluated_at=datetime.utcnow(),
                fitness_score=None, total_trades=None,
                net_pnl=None, max_drawdown=None,
                win_rate=None, expectancy=None, profit_factor=None,
                oos_delta=None, error="strategy crash",
            )
            for i in range(1, 4)
        ]
        score = compute_consistency(
            failed, windows_total=3, scenario=capital_accumulation_scenario
        )
        assert score.composite_score == 0.0
        assert score.window_collapse_flag is True
        assert score.windows_evaluated == 0

    def test_all_positive_windows_has_fraction_1(self, capital_accumulation_scenario):
        results = [
            _make_window_result("cand_1", f"W0{i}", net_pnl=100.0 * i, max_drawdown=0.05, fitness_score=0.6)
            for i in range(1, 4)
        ]
        score = compute_consistency(results, windows_total=3, scenario=capital_accumulation_scenario)
        assert score.fraction_positive_windows == 1.0

    def test_mixed_positive_negative_windows(self, capital_accumulation_scenario):
        results = [
            _make_window_result("cand_1", "W01", net_pnl=500.0, max_drawdown=0.05, fitness_score=0.7),
            _make_window_result("cand_1", "W02", net_pnl=-100.0, max_drawdown=0.15, fitness_score=0.3),
            _make_window_result("cand_1", "W03", net_pnl=200.0, max_drawdown=0.08, fitness_score=0.6),
        ]
        score = compute_consistency(results, windows_total=3, scenario=capital_accumulation_scenario)
        # 2 out of 3 windows are positive
        assert abs(score.fraction_positive_windows - 2 / 3) < 0.001
        assert 0.0 <= score.composite_score <= 1.0

    def test_windows_evaluated_excludes_failures(self, capital_accumulation_scenario):
        from datetime import datetime
        valid = _make_window_result("cand_1", "W01", net_pnl=300.0, max_drawdown=0.05, fitness_score=0.65)
        failed = WFOWindowResult(
            candidate_id="cand_1", window_id="W02",
            evaluated_at=datetime.utcnow(),
            fitness_score=None, total_trades=None,
            net_pnl=None, max_drawdown=None,
            win_rate=None, expectancy=None, profit_factor=None,
            oos_delta=None, error="error",
        )
        valid2 = _make_window_result("cand_1", "W03", net_pnl=100.0, max_drawdown=0.07, fitness_score=0.60)
        score = compute_consistency(
            [valid, failed, valid2], windows_total=3, scenario=capital_accumulation_scenario
        )
        assert score.windows_evaluated == 2
        assert score.windows_total == 3

    def test_high_variance_yields_lower_score_than_low_variance(self, capital_accumulation_scenario):
        """Higher variance should produce a lower composite score."""
        low_var_results = [
            _make_window_result("c1", f"W0{i}", net_pnl=300.0, max_drawdown=0.05, fitness_score=0.65)
            for i in range(1, 4)
        ]
        high_var_results = [
            _make_window_result("c2", "W01", net_pnl=1500.0, max_drawdown=0.03, fitness_score=0.8),
            _make_window_result("c2", "W02", net_pnl=-800.0, max_drawdown=0.30, fitness_score=0.2),
            _make_window_result("c2", "W03", net_pnl=600.0, max_drawdown=0.08, fitness_score=0.7),
        ]
        low_var_score = compute_consistency(
            low_var_results, windows_total=3, scenario=capital_accumulation_scenario
        )
        high_var_score = compute_consistency(
            high_var_results, windows_total=3, scenario=capital_accumulation_scenario
        )
        assert low_var_score.composite_score > high_var_score.composite_score