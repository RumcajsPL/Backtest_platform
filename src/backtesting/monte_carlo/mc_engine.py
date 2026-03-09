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

B9O-003: Fix KeyError on config["mc_prefilter"] when the top-level mc_prefilter
  config block is absent from the YAML (calibration YAMLs omit it because
  mc_prefilter stage is disabled). Changed to config.get("mc_prefilter", {})
  with safe fallbacks for all sub-keys. Same pattern as B9N-001 (ct.get()).
  Also fixed _get_profile_name() to use .get() consistently.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

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

# Safe defaults used when the YAML config block is absent (e.g. calibration runs
# with mc_prefilter stage disabled — the config block may be omitted entirely).
_MC_PREFILTER_DEFAULTS = {
    "iterations": 300,
    "perturbation_profile": "default",
    "ruin_threshold": 0.25,
}
_MC_DEEP_DEFAULTS = {
    "iterations": 3000,
    "perturbation_profile": "default",
    "ruin_threshold": 0.20,
}


def run_mc(
    candidate: CandidateParameterSet,
    candidate_result: CandidateResult,
    mode: MCMode,
    config: dict,
    seed: int,
    ruin_threshold: Optional[float] = None,
) -> MCResult:
    """
    Run Monte Carlo simulation on a candidate's trade history.

    Args:
        candidate:        The candidate parameter set (for logging).
        candidate_result: The CandidateResult containing trade history.
        mode:             MCMode.PRE_FILTER or MCMode.DEEP.
        config:           Full backtest config dict.
        seed:             RNG seed for this MC run.
        ruin_threshold:   Optional override from scenario profile.

    Returns:
        MCResult — always. On failure, ruin_probability is None and error is set.
    """
    try:
        return _run_mc_internal(candidate, candidate_result, mode, config, seed, ruin_threshold)
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
            iterations=1,
            evaluated_at=datetime.now(timezone.utc),
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
    ruin_threshold_override: Optional[float] = None,
) -> MCResult:
    """Internal implementation — may raise. Caller wraps in try/except."""
    if not candidate_result.is_valid:
        raise ValueError(
            f"CandidateResult is invalid (error: {candidate_result.error}) — "
            "cannot run MC simulation without valid trade history"
        )

    # B9O-003: Use .get() with safe defaults — config block may be absent when
    # the stage is disabled in the YAML (calibration runs omit the mc_prefilter
    # top-level block). Hard dict access config["mc_prefilter"] raises KeyError.
    if mode == MCMode.PRE_FILTER:
        mc_cfg = {**_MC_PREFILTER_DEFAULTS, **config.get("mc_prefilter", {})}
        deep_mode = False
    else:
        deep_block = config.get("monte_carlo", {}).get("deep", {})
        mc_cfg = {**_MC_DEEP_DEFAULTS, **deep_block}
        deep_mode = True

    iterations: int = mc_cfg["iterations"]
    profile_name: str = mc_cfg["perturbation_profile"]

    # B8B-013: ruin_threshold resolved from caller override first (scenario value),
    # then YAML config block, then hardcoded default.
    # ruin_threshold_override is set by orchestrator from scenario.mc_prefilter_ruin_threshold
    # for pre-filter mode, ensuring scenario-specific thresholds are respected.
    if ruin_threshold_override is not None:
        ruin_threshold: float = ruin_threshold_override
    else:
        ruin_threshold = mc_cfg.get("ruin_threshold", 0.20)

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
        evaluated_at=datetime.now(timezone.utc),
        avg_final_equity=avg_final_equity,
        worst_drawdown_across_paths=worst_drawdown,
        ruin_probability=ruin_probability,
        p5_final_equity=p5_final_equity,
        error=None,
    )


def _get_profile_name(config: dict, mode: MCMode) -> str:
    """Extract profile name from config without raising. B9O-003: use .get() throughout."""
    try:
        if mode == MCMode.PRE_FILTER:
            return config.get("mc_prefilter", {}).get(
                "perturbation_profile",
                _MC_PREFILTER_DEFAULTS["perturbation_profile"],
            )
        return config.get("monte_carlo", {}).get("deep", {}).get(
            "perturbation_profile",
            _MC_DEEP_DEFAULTS["perturbation_profile"],
        )
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