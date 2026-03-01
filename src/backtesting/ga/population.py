"""
ga/population.py
----------------
GA population initialisation and management.

Single responsibility: build and maintain the population data structure used
during GA evolution. The population is a list of (CandidateParameterSet, float)
tuples — the float is the current generation fitness score for that candidate.

No selection, crossover, or mutation here — those are separate modules.
"""
from __future__ import annotations

import logging
from typing import List, Optional, Tuple

from src.backtesting.contracts import CandidateParameterSet, CandidateRecord

logger = logging.getLogger(__name__)

# Type alias for a population member: (candidate, fitness_score)
PopulationMember = Tuple[CandidateParameterSet, float]


def initialise_population(
    seed_records: List[CandidateRecord],
    population_size: int,
) -> List[PopulationMember]:
    """
    Build the initial GA population from MC_PREFILTER_PASS candidates.

    Per D-04: top-N by fitness score from MC_PREFILTER_PASS.
    If seed_records has fewer than population_size, use all available seeds.

    Args:
        seed_records:    CandidateRecords from MC_PREFILTER_PASS stage,
                         already ranked by fitness_score DESC by the Ranker.
        population_size: Target population size.

    Returns:
        List of PopulationMember tuples, ordered by descending fitness.

    Raises:
        ValueError: If seed_records is empty.
    """
    if not seed_records:
        raise ValueError(
            "Cannot initialise GA population: no MC_PREFILTER_PASS candidates available. "
            "Stage 2 produced no survivors."
        )

    if len(seed_records) < population_size:
        logger.warning(
            "Fewer seed candidates (%d) than population_size (%d); "
            "using all available seeds.",
            len(seed_records),
            population_size,
        )

    population: List[PopulationMember] = []
    for record in seed_records[:population_size]:
        candidate = _record_to_candidate(record)
        fitness = record.fitness_score if record.fitness_score is not None else 0.0
        population.append((candidate, fitness))

    logger.info(
        "GA population initialised: %d members (target=%d)",
        len(population),
        population_size,
    )
    return population


def update_fitness(
    population: List[PopulationMember],
    fitness_map: dict,
) -> List[PopulationMember]:
    """
    Return a new population list with updated fitness scores from fitness_map.

    Args:
        population:  Current population.
        fitness_map: Dict of candidate_id → new fitness score.

    Returns:
        Updated population (new list — does not mutate input).
    """
    updated: List[PopulationMember] = []
    for candidate, old_fitness in population:
        new_fitness = fitness_map.get(candidate.candidate_id, old_fitness)
        updated.append((candidate, new_fitness))
    return updated


def get_elites(
    population: List[PopulationMember],
    elite_fraction: float,
) -> List[PopulationMember]:
    """
    Return the top elite_fraction of the population by fitness score.

    Args:
        population:     Current population (any order).
        elite_fraction: Fraction [0, 1] of population to preserve as elites.

    Returns:
        Sorted (desc) elite subset of the population.
    """
    if not (0.0 <= elite_fraction <= 1.0):
        raise ValueError(f"elite_fraction must be in [0, 1]; got {elite_fraction}")
    n_elites = max(1, int(len(population) * elite_fraction))
    return sorted(population, key=lambda m: m[1], reverse=True)[:n_elites]


def sort_population(population: List[PopulationMember]) -> List[PopulationMember]:
    """Return population sorted by fitness descending."""
    return sorted(population, key=lambda m: m[1], reverse=True)


# ── Private helpers ────────────────────────────────────────────────────────────

def _record_to_candidate(record: CandidateRecord) -> CandidateParameterSet:
    """
    Reconstruct a CandidateParameterSet from a CandidateRecord.
    Uses the parameters_json backup field for reconstruction.
    """
    import json
    parameters = json.loads(record.parameters_json)
    return CandidateParameterSet.create(
        zone_name=record.zone_name,
        parameters=parameters,
        generation=None,  # Will be set when candidate is re-evaluated in GA
    )