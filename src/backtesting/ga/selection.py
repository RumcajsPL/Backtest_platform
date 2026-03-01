"""
ga/selection.py
---------------
Tournament selection for the GA.

Single responsibility: given a population with fitness scores, select a parent
candidate for crossover/reproduction. Uses tournament selection — configurable
tournament size.

Zone-aware: selection operates over the full population regardless of zone.
Crossover is zone-aware (handled in crossover.py).
"""
from __future__ import annotations

import random
from typing import List, Tuple

from src.backtesting.contracts import CandidateParameterSet
from src.backtesting.ga.population import PopulationMember


def tournament_select(
    population: List[PopulationMember],
    tournament_size: int,
    rng: random.Random,
) -> CandidateParameterSet:
    """
    Select one parent via tournament selection.

    Randomly samples `tournament_size` candidates from the population without
    replacement (or with replacement if population is smaller) and returns
    the one with the highest fitness score.

    Args:
        population:      Current population as (candidate, fitness) tuples.
        tournament_size: Number of candidates in each tournament.
        rng:             Seeded Random instance for reproducibility.

    Returns:
        The winning CandidateParameterSet.

    Raises:
        ValueError: If population is empty.
    """
    if not population:
        raise ValueError("Cannot run tournament selection on an empty population")

    k = min(tournament_size, len(population))
    contestants: List[PopulationMember] = rng.sample(population, k=k)
    winner = max(contestants, key=lambda m: m[1])
    return winner[0]


def select_parents(
    population: List[PopulationMember],
    n_pairs: int,
    tournament_size: int,
    rng: random.Random,
) -> List[Tuple[CandidateParameterSet, CandidateParameterSet]]:
    """
    Select `n_pairs` parent pairs for crossover.

    Each pair is selected independently — the same individual can appear
    in multiple pairs (standard GA behaviour). Parents within a pair may
    be the same individual in small populations; crossover handles this gracefully.

    Args:
        population:      Current population.
        n_pairs:         Number of parent pairs to produce.
        tournament_size: Tournament size for each selection.
        rng:             Seeded Random instance.

    Returns:
        List of (parent_a, parent_b) tuples.
    """
    return [
        (
            tournament_select(population, tournament_size, rng),
            tournament_select(population, tournament_size, rng),
        )
        for _ in range(n_pairs)
    ]