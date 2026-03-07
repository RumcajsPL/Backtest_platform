"""
scenario.py — Loads the active ScenarioProfile from the backtest config dict.

Reads config['scenario'] to find the active scenario name, then reads
config['scenarios'][name] to build a ScenarioProfile. All validation
is performed by ScenarioProfile.__post_init__ (fail fast at construction).

No strategy knowledge. No file I/O. Config dict is the only input.
"""
from __future__ import annotations

from typing import Tuple

from src.backtesting.contracts import ScenarioProfile


def load_scenario(config: dict) -> ScenarioProfile:
    """
    Build and return the active ScenarioProfile from the config dict.

    Raises:
        ValueError: if the active scenario name is missing, unknown, or
                    if the profile fails validation (weights don't sum to 1.0,
                    thresholds out of range, etc.)
        KeyError:   if required config keys are absent.
    """
    scenario_name = config.get("scenario")
    if not scenario_name:
        raise ValueError("config['scenario'] is missing or empty")

    scenarios_def = config.get("scenarios")
    if not scenarios_def:
        raise ValueError("config['scenarios'] is missing or empty")

    if scenario_name not in scenarios_def:
        available = sorted(scenarios_def.keys())
        raise ValueError(
            f"Unknown scenario '{scenario_name}'. "
            f"Available scenarios: {available}"
        )

    s = scenarios_def[scenario_name]

    fw = s["fitness_weights"]
    ct = s["constraints"]
    wfw = s["wfo_temporal_weights"]
    vt = s["verdict_thresholds"]

    return ScenarioProfile(
        name=scenario_name,
        description=s["description"],

        weight_net_pnl=float(fw["net_pnl"]),
        weight_expectancy=float(fw["expectancy"]),
        weight_max_drawdown=float(fw["max_drawdown"]),
        weight_win_rate=float(fw["win_rate"]),
        weight_trade_frequency=float(fw["trade_frequency"]),
        weight_profit_factor=float(fw["profit_factor"]),

        min_win_rate=float(ct["min_win_rate"]),
        max_drawdown=float(ct["max_drawdown"]),
        max_losing_streak=int(ct["max_losing_streak"]),
        min_trades_per_week=float(ct["min_trades_per_week"]),
        min_expectancy=float(ct["min_expectancy"]),
        min_profit_factor=float(ct["min_profit_factor"]),

        mc_prefilter_ruin_threshold=float(s["mc_prefilter_ruin_threshold"]),
        wfo_collapse_drawdown_threshold=float(s.get("wfo_collapse_drawdown_threshold", 400.0)),

        wfo_weight_median_return=float(wfw["median_return"]),
        wfo_weight_variance=float(wfw["variance"]),
        wfo_weight_worst_drawdown=float(wfw["worst_drawdown"]),
        wfo_weight_fraction_positive=float(wfw["fraction_positive"]),

        verdict_go_wfo_floor=float(vt["go_wfo_floor"]),
        verdict_borderline_wfo_floor=float(vt["borderline_wfo_floor"]),
        verdict_go_mc_ruin_ceiling=float(vt["go_mc_ruin_ceiling"]),
        verdict_borderline_mc_ruin_ceiling=float(vt["borderline_mc_ruin_ceiling"]),
        verdict_sensitivity_spike_threshold=float(vt["sensitivity_spike_threshold"]),

        report_emphasis=tuple(s.get("report_emphasis", [])),
    )