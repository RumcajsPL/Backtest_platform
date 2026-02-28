"""
Unit tests for scenario.py.
"""
from __future__ import annotations

import copy
import unittest

from src.backtesting.scenario import load_scenario


# ── Minimal config fixture matching TECHNICAL_SPEC.md Section 5 ───────────────

_BASE_CONFIG = {
    "scenario": "capital_accumulation",
    "scenarios": {
        "capital_accumulation": {
            "description": "Steadily grow account balance with controlled risk",
            "fitness_weights": {
                "net_pnl": 0.20,
                "expectancy": 0.25,
                "max_drawdown": 0.20,
                "win_rate": 0.15,
                "trade_frequency": 0.10,
                "profit_factor": 0.10,
            },
            "constraints": {
                "min_win_rate": 0.45,
                "max_drawdown": 0.15,
                "max_losing_streak": 7,
                "min_trades_per_week": 3.0,
                "min_expectancy": 0.4,
                "min_profit_factor": 1.3,
            },
            "mc_prefilter_ruin_threshold": 0.25,
            "wfo_temporal_weights": {
                "median_return": 0.30,
                "variance": 0.30,
                "worst_drawdown": 0.20,
                "fraction_positive": 0.20,
            },
            "verdict_thresholds": {
                "go_wfo_floor": 0.65,
                "borderline_wfo_floor": 0.40,
                "go_mc_ruin_ceiling": 0.05,
                "borderline_mc_ruin_ceiling": 0.15,
                "sensitivity_spike_threshold": 0.15,
            },
            "report_emphasis": [
                "wfo_consistency_score",
                "fraction_positive_windows",
            ],
        },
        "conservative": {
            "description": "Preserve capital; avoid ruin above all else",
            "fitness_weights": {
                "net_pnl": 0.10,
                "expectancy": 0.15,
                "max_drawdown": 0.35,
                "win_rate": 0.25,
                "trade_frequency": 0.05,
                "profit_factor": 0.10,
            },
            "constraints": {
                "min_win_rate": 0.52,
                "max_drawdown": 0.10,
                "max_losing_streak": 5,
                "min_trades_per_week": 2.0,
                "min_expectancy": 0.3,
                "min_profit_factor": 1.2,
            },
            "mc_prefilter_ruin_threshold": 0.15,
            "wfo_temporal_weights": {
                "median_return": 0.20,
                "variance": 0.35,
                "worst_drawdown": 0.30,
                "fraction_positive": 0.15,
            },
            "verdict_thresholds": {
                "go_wfo_floor": 0.70,
                "borderline_wfo_floor": 0.50,
                "go_mc_ruin_ceiling": 0.03,
                "borderline_mc_ruin_ceiling": 0.10,
                "sensitivity_spike_threshold": 0.12,
            },
            "report_emphasis": ["mc_deep_ruin_probability"],
        },
    },
}


class TestLoadScenario(unittest.TestCase):
    def test_load_capital_accumulation(self):
        """Fitness weights sum to 1.0, all thresholds are present."""
        profile = load_scenario(_BASE_CONFIG)
        self.assertEqual(profile.name, "capital_accumulation")
        total_fw = (
            profile.weight_net_pnl + profile.weight_expectancy
            + profile.weight_max_drawdown + profile.weight_win_rate
            + profile.weight_trade_frequency + profile.weight_profit_factor
        )
        self.assertAlmostEqual(total_fw, 1.0, places=4)
        self.assertAlmostEqual(profile.min_win_rate, 0.45, places=4)
        self.assertAlmostEqual(profile.max_drawdown, 0.15, places=4)
        self.assertEqual(profile.max_losing_streak, 7)
        self.assertAlmostEqual(profile.verdict_go_wfo_floor, 0.65, places=4)

    def test_load_conservative(self):
        """WFO temporal weights sum to 1.0."""
        cfg = copy.deepcopy(_BASE_CONFIG)
        cfg["scenario"] = "conservative"
        profile = load_scenario(cfg)
        self.assertEqual(profile.name, "conservative")
        total_wfw = (
            profile.wfo_weight_median_return + profile.wfo_weight_variance
            + profile.wfo_weight_worst_drawdown + profile.wfo_weight_fraction_positive
        )
        self.assertAlmostEqual(total_wfw, 1.0, places=4)

    def test_unknown_scenario_raises(self):
        """Unknown scenario name raises ValueError immediately."""
        cfg = copy.deepcopy(_BASE_CONFIG)
        cfg["scenario"] = "nonexistent"
        with self.assertRaises(ValueError) as ctx:
            load_scenario(cfg)
        self.assertIn("nonexistent", str(ctx.exception))
        self.assertIn("capital_accumulation", str(ctx.exception))

    def test_weights_validated_in_post_init(self):
        """Tampered weights (sum ≠ 1.0) raise in ScenarioProfile.__post_init__."""
        cfg = copy.deepcopy(_BASE_CONFIG)
        cfg["scenarios"]["capital_accumulation"]["fitness_weights"]["net_pnl"] = 0.99
        with self.assertRaises(ValueError) as ctx:
            load_scenario(cfg)
        self.assertIn("sum to 1.0", str(ctx.exception))

    def test_missing_scenario_key_raises(self):
        """config missing 'scenario' key raises ValueError."""
        cfg = copy.deepcopy(_BASE_CONFIG)
        del cfg["scenario"]
        with self.assertRaises(ValueError):
            load_scenario(cfg)

    def test_report_emphasis_is_tuple(self):
        profile = load_scenario(_BASE_CONFIG)
        self.assertIsInstance(profile.report_emphasis, tuple)
        self.assertIn("wfo_consistency_score", profile.report_emphasis)

    def test_thresholds_ordering_validated(self):
        """borderline_wfo_floor >= go_wfo_floor raises in __post_init__."""
        cfg = copy.deepcopy(_BASE_CONFIG)
        cfg["scenarios"]["capital_accumulation"]["verdict_thresholds"]["borderline_wfo_floor"] = 0.80
        with self.assertRaises(ValueError):
            load_scenario(cfg)


if __name__ == "__main__":
    unittest.main(verbosity=2)