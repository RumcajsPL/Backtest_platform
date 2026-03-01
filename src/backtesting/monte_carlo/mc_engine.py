"""
monte_carlo/mc_engine.py
-------------------------
Monte Carlo engine — orchestrates simulation in pre-filter or deep mode.

Pre-filter mode (Stage 2):
  - Low iteration count (configurable, typically 200–500)
  - 2 perturbation types only (shuffle + spread noise)
  - Purpose: structural fragility screen

Deep mode (Stage 5):
  - High iteration count (configurable, typically 2,000–5,000)
  - All configured perturbation types
  - Purpose: full probabilistic stress testing

Single responsibility: CandidateResult → MCResult.
Never raises — all failures surface as MCResult with error set.
"""
from __future__ import annotations

import logging
from datetime import datetime

from src.backtesting.contracts import (
    CandidateParameterSet,
    CandidateResult,
    MCMode,
    MCResult,
)
from src.backtesting.monte_carlo.perturbation import load_profile, PerturbationProfile
from src.backtesting.monte_carlo.equity_simulator import (
    extract_trade_returns,
    simulate_paths,
)
from src.backtesting.monte_carlo.mc_metrics import compute_metrics

logger = logging.getLogger(__name__)


def run_mc(
    candidate: CandidateParameterSet,
    candidate_result: CandidateResult,
    mode: MCMode,
    config: dict,
    seed: int,
) -> MCResult:
    """
    Run Monte Carlo simulation on a candidate's trade history.

    Args:
        candidate:        The candidate parameter set (for logging).
        candidate_result: The CandidateResult containing trade history.
        mode:             MCMode.PRE_FILTER or MCMode.DEEP.
        config:           Full backtest config dict.
        seed:             RNG seed for this MC run.

    Returns:
        MCResult — always. On failure, ruin_probability is None and error is set.
    """
    try:
        return _run_mc_internal(candidate, candidate_result, mode, config, seed)
    except Exception as exc:
        logger.error(
            "MC simulation failed: candidate=%s mode=%s error=%s",
            candidate.candidate_id[:12],
            mode.value,
            exc,
            exc_info=True,
        )
        return MCResult(
            candidate_id=candidate.candidate_id,
            mode=mode,
            perturbation_profile_name=_get_profile_name(config, mode),
            iterations=1,  # Minimum valid value — actual iterations unknown in error path
            evaluated_at=datetime.utcnow(),
            avg_final_equity=None,
            worst_drawdown_across_paths=None,
            ruin_probability=None,
            p5_final_equity=None,
            error=str(exc),
        )


# ── Private implementation ─────────────────────────────────────────────────────

def _run_mc_internal(
    candidate: CandidateParameterSet,
    candidate_result: CandidateResult,
    mode: MCMode,
    config: dict,
    seed: int,
) -> MCResult:
    """Internal implementation — may raise. Caller wraps in try/except."""
    if not candidate_result.is_valid:
        raise ValueError(
            f"CandidateResult is invalid (error: {candidate_result.error}) — "
            "cannot run MC simulation without valid trade history"
        )

    # Load mode-specific config
    if mode == MCMode.PRE_FILTER:
        mc_cfg = config["mc_prefilter"]
        deep_mode = False
    else:
        mc_cfg = config["monte_carlo"]["deep"]
        deep_mode = True

    iterations: int = mc_cfg["iterations"]
    profile_name: str = mc_cfg["perturbation_profile"]
    ruin_threshold: float = mc_cfg.get("ruin_threshold", config["monte_carlo"]["deep"].get("ruin_threshold", 0.20))

    profile: PerturbationProfile = load_profile(config, profile_name)

    # Extract trade returns from candidate result
    trade_returns = extract_trade_returns(candidate_result.trades)

    if len(trade_returns) == 0:
        raise ValueError("No trades available for MC simulation")

    # Determine starting equity from metrics if available, else default
    starting_equity = _get_starting_equity(candidate_result)

    # Simulate equity paths
    equity_paths = simulate_paths(
        trade_returns=trade_returns,
        n_iterations=iterations,
        profile=profile,
        seed=seed,
        starting_equity=starting_equity,
        deep_mode=deep_mode,
    )

    # Compute summary metrics
    avg_final_equity, worst_drawdown, ruin_probability, p5_final_equity = compute_metrics(
        equity_paths=equity_paths,
        starting_equity=starting_equity,
        ruin_threshold=ruin_threshold,
    )

    logger.debug(
        "MC %s complete: candidate=%s iterations=%d ruin=%.3f avg_equity=%.2f",
        mode.value,
        candidate.candidate_id[:12],
        iterations,
        ruin_probability,
        avg_final_equity,
    )

    return MCResult(
        candidate_id=candidate.candidate_id,
        mode=mode,
        perturbation_profile_name=profile_name,
        iterations=iterations,
        evaluated_at=datetime.utcnow(),
        avg_final_equity=avg_final_equity,
        worst_drawdown_across_paths=worst_drawdown,
        ruin_probability=ruin_probability,
        p5_final_equity=p5_final_equity,
        error=None,
    )


def _get_profile_name(config: dict, mode: MCMode) -> str:
    """Extract profile name from config without raising."""
    try:
        if mode == MCMode.PRE_FILTER:
            return config["mc_prefilter"]["perturbation_profile"]
        return config["monte_carlo"]["deep"]["perturbation_profile"]
    except (KeyError, TypeError):
        return "unknown"


def _get_starting_equity(candidate_result: CandidateResult) -> float:
    """
    Attempt to extract starting equity from metrics.
    Falls back to 10,000 if not available.
    """
    if candidate_result.metrics is None:
        return 10_000.0
    equity = getattr(candidate_result.metrics, "starting_equity", None)
    if equity is not None:
        return float(equity)
    return 10_000.0