"""
monte_carlo/perturbation.py
----------------------------
Named, versioned perturbation profiles for Monte Carlo simulation.

Pre-filter mode uses a subset of perturbation types (cheap screen):
  - trade sequence shuffling
  - spread noise

Deep mode uses all configured perturbation types:
  - trade sequence shuffling
  - return resampling (bootstrap)
  - spread noise
  - risk noise (position sizing)
  - slippage simulation

Single responsibility: load named perturbation profile from config, apply to trade data.
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from typing import List, Optional, Tuple

import numpy as np


@dataclass
class PerturbationProfile:
    """Loaded perturbation profile parameters. Immutable after construction."""
    name: str
    version: str
    spread_noise_bps_range: Tuple[float, float]  # e.g. (0, 3)
    slippage_pips_range: Tuple[float, float]     # e.g. (0, 1)
    risk_noise_fraction: float                   # e.g. 0.05 = ±5%
    shuffle_trades: bool
    resample_returns: bool
    execution_delay_bars: int


def load_profile(config: dict, profile_name: str) -> PerturbationProfile:
    """
    Load a named perturbation profile from the backtester config dict.

    Args:
        config:       Full backtest config dict.
        profile_name: Name of the profile (e.g. "default").

    Returns:
        PerturbationProfile with all parameters loaded.

    Raises:
        KeyError:   If profile_name not found in config.
        ValueError: If profile parameters are invalid.
    """
    profiles: dict = config.get("perturbation_profiles", {})
    if profile_name not in profiles:
        raise KeyError(
            f"Perturbation profile '{profile_name}' not found. "
            f"Available: {list(profiles.keys())}"
        )

    p: dict = profiles[profile_name]
    spread_range = tuple(p["spread_noise_bps_range"])
    slippage_range = tuple(p["slippage_pips_range"])

    if spread_range[0] > spread_range[1]:
        raise ValueError(f"spread_noise_bps_range must be [low, high]; got {spread_range}")
    if slippage_range[0] > slippage_range[1]:
        raise ValueError(f"slippage_pips_range must be [low, high]; got {slippage_range}")

    return PerturbationProfile(
        name=profile_name,
        version=str(p.get("version", "1.0")),
        spread_noise_bps_range=(float(spread_range[0]), float(spread_range[1])),
        slippage_pips_range=(float(slippage_range[0]), float(slippage_range[1])),
        risk_noise_fraction=float(p.get("risk_noise_fraction", 0.0)),
        shuffle_trades=bool(p.get("shuffle_trades", True)),
        resample_returns=bool(p.get("resample_returns", True)),
        execution_delay_bars=int(p.get("execution_delay_bars", 0)),
    )


def apply_prefilter_perturbations(
    trade_returns: np.ndarray,
    profile: PerturbationProfile,
    rng: random.Random,
) -> np.ndarray:
    """
    Apply lightweight pre-filter perturbations (2 types only):
      1. Trade sequence shuffle
      2. Spread noise

    Args:
        trade_returns: 1-D array of per-trade P&L values (in account currency or R-multiples).
        profile:       Perturbation profile with configured ranges.
        rng:           Seeded Python Random instance for shuffle operations.

    Returns:
        Perturbed copy of trade_returns (numpy array).
    """
    perturbed = trade_returns.copy()

    if profile.shuffle_trades:
        perturbed = _shuffle_returns(perturbed, rng)

    if profile.spread_noise_bps_range[1] > 0:
        perturbed = _apply_spread_noise(perturbed, profile.spread_noise_bps_range, rng)

    return perturbed


def apply_deep_perturbations(
    trade_returns: np.ndarray,
    profile: PerturbationProfile,
    rng: random.Random,
    numpy_rng: np.random.Generator,
) -> np.ndarray:
    """
    Apply full deep-mode perturbations (all configured types):
      1. Return resampling (bootstrap)
      2. Trade sequence shuffle
      3. Spread noise
      4. Risk noise (position sizing)
      5. Slippage simulation

    Args:
        trade_returns: 1-D array of per-trade P&L values.
        profile:       Perturbation profile.
        rng:           Seeded Python Random (for shuffle).
        numpy_rng:     Seeded numpy Generator (for vectorised noise).

    Returns:
        Perturbed copy of trade_returns.
    """
    perturbed = trade_returns.copy()

    # 1. Return resampling: bootstrap (with replacement)
    if profile.resample_returns:
        idx = numpy_rng.integers(0, len(perturbed), size=len(perturbed))
        perturbed = perturbed[idx]

    # 2. Sequence shuffle
    if profile.shuffle_trades:
        perturbed = _shuffle_returns(perturbed, rng)

    # 3. Spread noise
    if profile.spread_noise_bps_range[1] > 0:
        perturbed = _apply_spread_noise_vectorised(
            perturbed, profile.spread_noise_bps_range, numpy_rng
        )

    # 4. Risk noise: scale each trade by (1 ± risk_noise_fraction)
    if profile.risk_noise_fraction > 0:
        noise = numpy_rng.uniform(
            1.0 - profile.risk_noise_fraction,
            1.0 + profile.risk_noise_fraction,
            size=len(perturbed),
        )
        perturbed = perturbed * noise

    # 5. Slippage: subtract random slippage from each trade
    if profile.slippage_pips_range[1] > 0:
        slippage = numpy_rng.uniform(
            profile.slippage_pips_range[0],
            profile.slippage_pips_range[1],
            size=len(perturbed),
        )
        # Slippage reduces return — assume pip value is small relative to trade return
        # Subtract as absolute value (sign convention: slippage is always a cost)
        perturbed = perturbed - slippage

    return perturbed


# ── Private helpers ────────────────────────────────────────────────────────────

def _shuffle_returns(returns: np.ndarray, rng: random.Random) -> np.ndarray:
    """Shuffle returns in-place copy using Python rng for shuffle."""
    arr = returns.copy()
    lst = arr.tolist()
    rng.shuffle(lst)
    return np.array(lst, dtype=returns.dtype)


def _apply_spread_noise(
    returns: np.ndarray,
    bps_range: Tuple[float, float],
    rng: random.Random,
) -> np.ndarray:
    """Apply per-trade spread noise using Python rng."""
    noise = np.array(
        [rng.uniform(bps_range[0], bps_range[1]) for _ in range(len(returns))],
        dtype=np.float64,
    )
    return returns - noise


def _apply_spread_noise_vectorised(
    returns: np.ndarray,
    bps_range: Tuple[float, float],
    numpy_rng: np.random.Generator,
) -> np.ndarray:
    """Apply per-trade spread noise using vectorised numpy for deep mode."""
    noise = numpy_rng.uniform(bps_range[0], bps_range[1], size=len(returns))
    return returns - noise