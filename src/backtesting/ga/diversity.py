"""
ga/diversity.py
---------------
Diversity penalty computation for the GA fitness function.

Implements the hybrid Euclidean/Hamming distance metric (D-11):
  - Continuous parameters (int/float): normalised Euclidean distance in [0, 1] per parameter
  - Discrete parameters (choice):      Hamming distance (0 = same, 1 = different)
  - Overall distance: weighted average proportional to count of each type

If a candidate's distance to the nearest elite is below `distance_threshold`,
it receives a downward penalty on its fitness score scaled by `penalty_weight`.

Single responsibility: candidate + elites → penalty scalar.
"""
from __future__ import annotations

import math
from typing import List

from src.backtesting.contracts import CandidateParameterSet


def compute_penalty(
    candidate: CandidateParameterSet,
    elites: List[CandidateParameterSet],
    parameter_space_def: dict,
    distance_threshold: float,
    penalty_weight: float,
) -> float:
    """
    Compute the diversity penalty for a candidate relative to current elites.

    Args:
        candidate:            The candidate being evaluated.
        elites:               Current elite population members (parameter sets only).
        parameter_space_def:  Zone parameter definitions (for normalisation ranges).
        distance_threshold:   Normalised distance below which penalty applies.
        penalty_weight:       Maximum penalty scalar (subtracted from fitness).

    Returns:
        Penalty in [0, penalty_weight]. 0 if candidate is sufficiently diverse.
    """
    if not elites:
        return 0.0

    zone_params_def: dict = parameter_space_def.get("parameters", {})

    # Classify parameters into continuous (int/float) and discrete (choice)
    continuous_params = [
        p for p, d in zone_params_def.items()
        if d.get("type") in ("int", "float")
    ]
    discrete_params = [
        p for p, d in zone_params_def.items()
        if d.get("type") == "choice"
    ]
    total_params = len(continuous_params) + len(discrete_params)
    if total_params == 0:
        return 0.0

    # Weight each type by its fraction of all parameters
    w_continuous = len(continuous_params) / total_params
    w_discrete = len(discrete_params) / total_params

    # Compute distance from candidate to each elite; take the minimum
    min_distance = min(
        _hybrid_distance(
            candidate,
            elite,
            continuous_params,
            discrete_params,
            zone_params_def,
            w_continuous,
            w_discrete,
        )
        for elite in elites
    )

    if min_distance >= distance_threshold:
        return 0.0  # Candidate is sufficiently diverse — no penalty

    # Linear penalty: closer to an elite → higher penalty
    # At distance=0: full penalty_weight. At distance=threshold: 0 penalty.
    penalty_fraction = 1.0 - (min_distance / distance_threshold)
    return penalty_weight * penalty_fraction


# ── Private helpers ────────────────────────────────────────────────────────────

def _hybrid_distance(
    a: CandidateParameterSet,
    b: CandidateParameterSet,
    continuous_params: List[str],
    discrete_params: List[str],
    zone_params_def: dict,
    w_continuous: float,
    w_discrete: float,
) -> float:
    """
    Compute hybrid Euclidean/Hamming distance between two candidates.
    Returns a scalar in [0, 1].
    """
    euclidean_dist = 0.0
    if continuous_params:
        euclidean_dist = _normalised_euclidean(a, b, continuous_params, zone_params_def)

    hamming_dist = 0.0
    if discrete_params:
        hamming_dist = _hamming(a, b, discrete_params)

    return w_continuous * euclidean_dist + w_discrete * hamming_dist


def _normalised_euclidean(
    a: CandidateParameterSet,
    b: CandidateParameterSet,
    params: List[str],
    zone_params_def: dict,
) -> float:
    """
    Normalised Euclidean distance across continuous parameters.
    Each parameter is normalised to [0, 1] by its zone range before distance computation.
    Returns the RMS distance across all continuous parameters (in [0, 1]).
    """
    squared_sum = 0.0
    count = 0

    for param_name in params:
        param_def = zone_params_def.get(param_name)
        if param_def is None:
            continue

        val_a = a.parameters.get(param_name)
        val_b = b.parameters.get(param_name)
        if val_a is None or val_b is None:
            continue

        low = param_def.get("min", 0)
        high = param_def.get("max", 1)
        param_range = high - low

        if param_range <= 0:
            continue

        norm_a = (float(val_a) - low) / param_range
        norm_b = (float(val_b) - low) / param_range
        squared_sum += (norm_a - norm_b) ** 2
        count += 1

    if count == 0:
        return 0.0

    # RMS — keeps result in [0, 1] even when many parameters differ maximally
    return math.sqrt(squared_sum / count)


def _hamming(
    a: CandidateParameterSet,
    b: CandidateParameterSet,
    params: List[str],
) -> float:
    """
    Hamming distance across discrete parameters.
    Returns fraction of differing discrete parameters in [0, 1].
    """
    if not params:
        return 0.0
    differing = sum(
        1 for p in params
        if a.parameters.get(p) != b.parameters.get(p)
    )
    return differing / len(params)