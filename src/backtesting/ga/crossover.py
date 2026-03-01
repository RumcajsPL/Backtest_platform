"""
ga/crossover.py
---------------
Crossover operator for the GA.

Uses uniform crossover: for each parameter, independently choose which parent
to inherit from with probability 0.5. This is more exploratory than single-point
crossover and handles both continuous and discrete parameters uniformly.

Zone-aware: both parents must be from the same zone (or zone is inherited from
parent_a). The offspring is always a valid CandidateParameterSet within zone boundaries.
"""
from __future__ import annotations

import random
from typing import Optional

from src.backtesting.contracts import CandidateParameterSet


def crossover(
    parent_a: CandidateParameterSet,
    parent_b: CandidateParameterSet,
    crossover_rate: float,
    rng: random.Random,
    generation: Optional[int] = None,
) -> CandidateParameterSet:
    """
    Produce one offspring via uniform crossover.

    If a uniform random draw >= crossover_rate, return parent_a unchanged
    (no crossover occurs). Otherwise, apply uniform crossover across all parameters.

    The offspring inherits zone_name from parent_a. If parents are from different
    zones, parameter ranges from parent_a's zone apply — this is handled by mutation
    clamping downstream, not here (this module does not validate zone membership).

    Args:
        parent_a:       First parent (zone_name inherited by offspring).
        parent_b:       Second parent.
        crossover_rate: Probability that crossover occurs (vs. passing parent_a through).
        rng:            Seeded Random instance.
        generation:     GA generation number for the offspring.

    Returns:
        A new CandidateParameterSet (offspring or copy of parent_a).
    """
    if rng.random() >= crossover_rate:
        # No crossover — return parent_a as-is (with updated generation)
        return CandidateParameterSet.create(
            zone_name=parent_a.zone_name,
            parameters=dict(parent_a.parameters),
            generation=generation,
        )

    # Uniform crossover: each parameter independently from one parent
    child_params = {}
    all_keys = set(parent_a.parameters) | set(parent_b.parameters)

    for key in all_keys:
        if key not in parent_a.parameters:
            child_params[key] = parent_b.parameters[key]
        elif key not in parent_b.parameters:
            child_params[key] = parent_a.parameters[key]
        else:
            # Inherit from parent_a with p=0.5, parent_b with p=0.5
            child_params[key] = (
                parent_a.parameters[key] if rng.random() < 0.5
                else parent_b.parameters[key]
            )

    return CandidateParameterSet.create(
        zone_name=parent_a.zone_name,
        parameters=child_params,
        generation=generation,
    )