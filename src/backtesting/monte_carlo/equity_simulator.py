"""
monte_carlo/equity_simulator.py
--------------------------------
Equity path simulation for Monte Carlo analysis.

Key design constraint: all equity paths are computed using vectorised numpy
operations. No Python loops over individual paths. A single call to
`simulate_paths` produces all N equity paths at once as a 2-D numpy array.

Each equity path applies independently sampled perturbations via the perturbation
module, then replays the resulting trade sequence as a cumulative equity curve.
"""
from __future__ import annotations

import random
from typing import Optional

import numpy as np

from src.backtesting.monte_carlo.perturbation import (
    PerturbationProfile,
    apply_deep_perturbations,
    apply_prefilter_perturbations,
)


def simulate_paths(
    trade_returns: np.ndarray,
    n_iterations: int,
    profile: PerturbationProfile,
    seed: int,
    starting_equity: float = 10_000.0,
    deep_mode: bool = False,
) -> np.ndarray:
    """
    Simulate N equity paths from a base trade sequence.

    Each path independently applies perturbations then replays the trade sequence
    as a cumulative equity curve starting at `starting_equity`.

    Args:
        trade_returns:   1-D array of per-trade P&L values (in currency units).
        n_iterations:    Number of equity paths to simulate.
        profile:         Perturbation profile to apply.
        seed:            Master seed — each path gets a derived seed for reproducibility.
        starting_equity: Starting account equity for all paths.
        deep_mode:       If True, apply full deep perturbations; else pre-filter perturbations.

    Returns:
        2-D numpy array of shape (n_iterations, n_trades + 1).
        Row i = equity path i, starting at starting_equity.
        paths[:, 0] == starting_equity for all paths.
        paths[:, j] = equity after j-th trade.

    Raises:
        ValueError: If trade_returns is empty or n_iterations <= 0.
    """
    if len(trade_returns) == 0:
        raise ValueError("trade_returns must not be empty")
    if n_iterations <= 0:
        raise ValueError(f"n_iterations must be positive; got {n_iterations}")

    n_trades = len(trade_returns)

    # Pre-allocate output: shape (n_iterations, n_trades + 1)
    # Column 0 = starting equity for all paths
    all_returns = np.empty((n_iterations, n_trades), dtype=np.float64)

    # Master Python RNG for shuffle operations (path-specific derived seeds)
    master_rng = random.Random(seed)
    # Master numpy RNG for vectorised noise in deep mode
    numpy_master_rng = np.random.default_rng(seed)

    for i in range(n_iterations):
        # Each path gets its own derived seed for reproducibility
        path_seed = master_rng.randint(0, 2**31 - 1)
        path_rng = random.Random(path_seed)
        path_numpy_rng = np.random.default_rng(path_seed)

        if deep_mode:
            perturbed = apply_deep_perturbations(
                trade_returns, profile, path_rng, path_numpy_rng
            )
        else:
            perturbed = apply_prefilter_perturbations(
                trade_returns, profile, path_rng
            )

        all_returns[i] = perturbed

    # Vectorised cumulative sum → equity paths
    # Shape: (n_iterations, n_trades)
    cumulative_returns = np.cumsum(all_returns, axis=1)

    # Prepend starting equity column: shape (n_iterations, n_trades + 1)
    starting_col = np.full((n_iterations, 1), starting_equity, dtype=np.float64)
    equity_paths = np.hstack([starting_col, starting_equity + cumulative_returns])

    return equity_paths


def extract_trade_returns(candidate_result_trades: object) -> np.ndarray:
    """
    Extract per-trade P&L values from a CandidateResult's trades object
    into a 1-D numpy array suitable for MC simulation.

    The trades object is the TradeResult from the strategy architecture.
    Attribute resolution order (B9F-004):
      1. trade.pnl_points  — Trade.pnl_points property (TradeExit.pnl_points via exit)
      2. trade.pnl         — legacy fallback attribute name
      3. dict["pnl_points"] — dict representation fallback
      4. dict["pnl"]        — legacy dict fallback

    Open trades (exit is None) return pnl_points=None and are skipped.
    Only closed trades contribute to MC simulation.

    Args:
        candidate_result_trades: TradeResult from strategy (list of Trade objects
                                 or list of dicts with pnl_points/pnl key).

    Returns:
        1-D numpy array of float64 P&L values, one per closed trade.

    Raises:
        ValueError: If no closed trades found or P&L values cannot be extracted.
    """
    if candidate_result_trades is None:
        raise ValueError("trades object is None — cannot extract returns for MC simulation")

    # Handle both list-of-objects and list-of-dicts
    trades = candidate_result_trades
    if hasattr(trades, "trades"):
        trades = trades.trades  # Unwrap TradeResult container if needed

    if not trades:
        raise ValueError("Trade list is empty — cannot run MC simulation with zero trades")

    pnl_values = []
    for trade in trades:
        # B9F-004: Trade contract uses pnl_points (via Trade.pnl_points property).
        # Trade.pnl_points returns None for open trades (no exit) — skip those.
        if hasattr(trade, "pnl_points"):
            val = trade.pnl_points
            if val is None:
                continue  # Open trade — no exit yet, skip
            pnl_values.append(float(val))
        elif hasattr(trade, "pnl"):
            pnl_values.append(float(trade.pnl))
        elif isinstance(trade, dict) and "pnl_points" in trade:
            val = trade["pnl_points"]
            if val is None:
                continue
            pnl_values.append(float(val))
        elif isinstance(trade, dict) and "pnl" in trade:
            pnl_values.append(float(trade["pnl"]))
        else:
            raise ValueError(
                f"Cannot extract pnl from trade object type {type(trade).__name__}. "
                "Expected attribute 'pnl_points', 'pnl', or dict key 'pnl_points'/'pnl'."
            )

    if not pnl_values:
        raise ValueError(
            "No closed trades available for MC simulation — "
            "all trades are open (no exit recorded)"
        )

    return np.array(pnl_values, dtype=np.float64)