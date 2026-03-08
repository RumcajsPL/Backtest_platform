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
    # B9B-001: Guard against cross-zone crossover.
    # If parents are from different zones, return parent_a unchanged.
    # Without this guard, the offspring inherits parent_a's zone_name but
    # potentially parameter values only valid in parent_b's zone.
    # Mutation clamping downstream cannot correct this — parent_b values
    # may be completely outside parent_a's zone bounds.
    if parent_a.zone_name != parent_b.zone_name:
        return CandidateParameterSet.create(
            zone_name=parent_a.zone_name,
            parameters=dict(parent_a.parameters),
            generation=generation,
        )

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
            child_params[key] = (
                parent_a.parameters[key] if rng.random() < 0.5
                else parent_b.parameters[key]
            )

    return CandidateParameterSet.create(
        zone_name=parent_a.zone_name,
        parameters=child_params,
        generation=generation,
    )