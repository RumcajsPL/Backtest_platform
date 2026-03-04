"""
monte_carlo/mc_metrics.py
--------------------------
Compute Monte Carlo summary metrics from a 2-D equity paths array.

All computations are vectorised numpy — no Python loops.

Single responsibility: np.ndarray (equity paths) → scalar metrics.

Block 7B change (M-04): Paths that hit ruin (equity ≤ ruin_floor) report
worst_drawdown = 1.0. Previously, running-max drawdown computation could
understate drawdown on ruined paths because the equity curve might have
briefly recovered before crashing, causing the running maximum to grow
and the reported drawdown fraction to be < 1.0 even though total ruin
was reached. Fix: after computing worst_drawdown_per_path, clamp all
ruined paths to 1.0 before taking the ensemble maximum.
"""
from __future__ import annotations

from typing import Tuple

import numpy as np


def compute_metrics(
    equity_paths: np.ndarray,
    starting_equity: float,
    ruin_threshold: float,
) -> Tuple[float, float, float, float]:
    """
    Compute four MC summary metrics from the equity paths array.

    Args:
        equity_paths:     2-D array shape (n_paths, n_steps+1).
                          paths[:, 0] == starting_equity.
        starting_equity:  Initial equity value.
        ruin_threshold:   Fraction of starting_equity at which ruin is declared.
                          e.g. 0.20 → ruin if equity falls below 20% of starting.

    Returns:
        Tuple of:
          avg_final_equity         — mean of final equity across all paths
          worst_drawdown_across_paths — maximum drawdown seen across entire ensemble
          ruin_probability         — fraction of paths that hit ruin threshold [0, 1]
          p5_final_equity          — 5th percentile of final equity values

    Raises:
        ValueError: If equity_paths is empty or has wrong shape.
    """
    if equity_paths.ndim != 2 or equity_paths.shape[0] == 0:
        raise ValueError(
            f"equity_paths must be 2-D with at least one row; got shape {equity_paths.shape}"
        )

    n_paths = equity_paths.shape[0]
    final_equities = equity_paths[:, -1]

    # ── Average final equity ─────────────────────────────────────────────────
    avg_final_equity: float = float(np.mean(final_equities))

    # ── Ruin probability ─────────────────────────────────────────────────────
    ruin_floor: float = starting_equity * ruin_threshold
    # A path is "ruined" if its equity ever fell at or below ruin_floor
    path_minimums = np.min(equity_paths, axis=1)
    ruined_paths: np.ndarray = path_minimums <= ruin_floor
    ruin_count: int = int(np.sum(ruined_paths))
    ruin_probability: float = ruin_count / n_paths

    # ── Worst drawdown across all paths ──────────────────────────────────────
    # Compute running maximum per path, then drawdown at each step.
    # running_max shape: (n_paths, n_steps+1)
    running_max = np.maximum.accumulate(equity_paths, axis=1)
    # Drawdown at each step: (running_max - current) / running_max
    # Avoid division by zero if running_max is 0 (degenerate)
    safe_running_max = np.where(running_max > 0, running_max, 1.0)
    drawdown_matrix = (running_max - equity_paths) / safe_running_max
    # Per-path worst drawdown
    worst_drawdown_per_path: np.ndarray = np.max(drawdown_matrix, axis=1)

    # M-04 fix: Paths that hit ruin must report drawdown = 1.0.
    # The running-max calculation can understate drawdown on ruined paths:
    # if equity briefly recovered before crashing to ruin, the running max
    # grows, and the reported peak-to-trough fraction may be < 1.0 even
    # though the path ended in total ruin. Clamp all ruined paths to 1.0.
    worst_drawdown_per_path[ruined_paths] = 1.0

    worst_drawdown_across_paths: float = float(np.max(worst_drawdown_per_path))

    # ── 5th percentile final equity ──────────────────────────────────────────
    p5_final_equity: float = float(np.percentile(final_equities, 5))

    return avg_final_equity, worst_drawdown_across_paths, ruin_probability, p5_final_equity