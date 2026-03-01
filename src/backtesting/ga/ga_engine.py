"""
ga/ga_engine.py
---------------
Genetic Algorithm evolution loop.

Core design principles (from TECHNICAL_SPEC.md, FUNCTIONAL_SPEC.md Stage 3):
  - WFO-aware fitness: each generation samples 2 random windows from the full
    WFO window list (D-05). Sampling is independent per generation — prevents
    over-fitting to specific time windows.
  - Diversity penalty: applied to fitness scores before selection (D-11).
  - Elite preservation: top elite_fraction of population passes unchanged.
  - Early stopping: if max fitness does not improve for stagnation_generations
    consecutive generations, the GA terminates.
  - All candidate evaluations written to CandidateStore with stage=GA and generation number.

Single responsibility: GA evolution loop. Does not own strategy evaluation —
that is delegated to wfo_evaluator.evaluate_window.
"""
from __future__ import annotations

import logging
import random
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from src.backtesting.contracts import (
    CandidateParameterSet,
    CandidateRecord,
    CandidateStage,
    ScenarioProfile,
    WFOWindow,
    WFOWindowResult,
)
from src.backtesting.candidate_store import CandidateStore
from src.backtesting.ranker import rank
from src.backtesting.wfo.wfo_evaluator import evaluate_window
from src.backtesting.wfo.consistency_scorer import compute_consistency
from src.backtesting.ga.population import (
    PopulationMember,
    get_elites,
    initialise_population,
    sort_population,
    update_fitness,
)
from src.backtesting.ga.selection import select_parents
from src.backtesting.ga.crossover import crossover
from src.backtesting.ga.mutation import mutate
from src.backtesting.ga.diversity import compute_penalty

logger = logging.getLogger(__name__)


def run_ga(
    store: CandidateStore,
    run_id: str,
    scenario: ScenarioProfile,
    wfo_windows: List[WFOWindow],
    config: dict,
    seed: int,
) -> None:
    """
    Run the full GA evolution loop.

    Reads MC_PREFILTER_PASS candidates from store as initial population.
    Evolves for configured generations. Writes all candidate evaluations to store.

    Args:
        store:       CandidateStore for reading seeds and writing results.
        run_id:      Current pipeline run ID.
        scenario:    Active scenario profile.
        wfo_windows: Full list of configured WFO windows (min 3 required).
        config:      Full backtest config dict.
        seed:        RNG seed for GA operations.

    Raises:
        ValueError: If fewer than 3 WFO windows provided.
        RuntimeError: If first generation produces zero valid evaluations.
    """
    if len(wfo_windows) < 3:
        raise ValueError(
            f"GA requires minimum 3 WFO windows for random sampling; got {len(wfo_windows)}"
        )

    ga_config: dict = config["genetic"]
    population_size: int = ga_config["population_size"]
    generations: int = ga_config["generations"]
    elite_fraction: float = ga_config["elite_fraction"]
    mutation_rate: float = ga_config["mutation_rate"]
    crossover_rate: float = ga_config["crossover_rate"]
    tournament_size: int = ga_config["tournament_size"]
    stagnation_limit: int = ga_config["stagnation_generations"]
    diversity_penalty_weight: float = ga_config["diversity_penalty_weight"]
    diversity_distance_threshold: float = ga_config["diversity_distance_threshold"]
    max_workers: int = config["run"]["max_workers"]
    min_significant_trades: int = config["random_search"]["min_significant_trades"]

    base_yaml_path = Path(config["_base_yaml_path"])
    temp_dir = Path(config["run"]["temp_dir"])

    # Get zone parameter definitions for mutation and diversity computation
    zones_def: dict = config.get("zones", {})

    rng = random.Random(seed)

    # ── Seed population from MC_PREFILTER_PASS ───────────────────────────────
    seed_records: List[CandidateRecord] = rank(
        store=store,
        run_id=run_id,
        stage=CandidateStage.MC_PREFILTER_PASS,
        top_n=population_size,
    )
    population: List[PopulationMember] = initialise_population(seed_records, population_size)

    best_fitness: float = max(f for _, f in population) if population else 0.0
    stagnation_count: int = 0

    logger.info(
        "GA starting: pop=%d gens=%d elites=%.0f%% mut=%.2f xo=%.2f seed=%d",
        len(population),
        generations,
        elite_fraction * 100,
        mutation_rate,
        crossover_rate,
        seed,
    )

    # ── Evolution loop ────────────────────────────────────────────────────────
    for gen_num in range(1, generations + 1):

        # Sample 2 random windows for this generation (independent per generation — D-05)
        gen_windows: List[WFOWindow] = rng.sample(wfo_windows, k=2)
        logger.debug(
            "Gen %d: sampled windows %s",
            gen_num,
            [w.window_id for w in gen_windows],
        )

        # Evaluate all population members on the 2 sampled windows
        wfo_fitness_map: Dict[str, float] = _evaluate_generation(
            population=[c for c, _ in population],
            windows=gen_windows,
            scenario=scenario,
            base_yaml_path=base_yaml_path,
            temp_dir=temp_dir,
            max_workers=max_workers,
            min_significant_trades=min_significant_trades,
            run_id=run_id,
            gen_num=gen_num,
            store=store,
        )

        if not wfo_fitness_map:
            logger.error("Generation %d produced zero valid evaluations — aborting GA", gen_num)
            raise RuntimeError(f"GA generation {gen_num} produced no valid evaluations")

        # Apply diversity penalty to fitness scores
        elite_candidates = [c for c, _ in get_elites(population, elite_fraction)]
        penalised_map: Dict[str, float] = {}
        for candidate, _ in population:
            cid = candidate.candidate_id
            base_fitness = wfo_fitness_map.get(cid, 0.0)
            zone_def = zones_def.get(candidate.zone_name, {})
            penalty = compute_penalty(
                candidate=candidate,
                elites=elite_candidates,
                parameter_space_def=zone_def,
                distance_threshold=diversity_distance_threshold,
                penalty_weight=diversity_penalty_weight,
            )
            penalised_map[cid] = max(0.0, base_fitness - penalty)

        # Update population fitness with penalised scores
        population = update_fitness(population, penalised_map)
        population = sort_population(population)

        gen_best = population[0][1] if population else 0.0
        logger.info(
            "Gen %d/%d: best_fitness=%.4f window_pair=%s",
            gen_num,
            generations,
            gen_best,
            [w.window_id for w in gen_windows],
        )

        # Check stagnation
        if gen_best > best_fitness + 1e-6:
            best_fitness = gen_best
            stagnation_count = 0
        else:
            stagnation_count += 1
            if stagnation_count >= stagnation_limit:
                logger.info(
                    "GA early stop: no improvement for %d consecutive generations",
                    stagnation_limit,
                )
                break

        # ── Produce next generation ────────────────────────────────────────
        elites = get_elites(population, elite_fraction)
        next_population: List[PopulationMember] = list(elites)  # Elites preserved unchanged

        # Fill remaining slots via selection → crossover → mutation
        n_offspring = population_size - len(elites)
        parent_pairs = select_parents(
            population=population,
            n_pairs=n_offspring,
            tournament_size=tournament_size,
            rng=rng,
        )

        for parent_a, parent_b in parent_pairs:
            offspring = crossover(parent_a, parent_b, crossover_rate, rng, generation=gen_num)
            zone_def = zones_def.get(offspring.zone_name, {})
            offspring = mutate(offspring, mutation_rate, zone_def, rng, generation=gen_num)
            # Assign placeholder fitness (will be evaluated next generation)
            next_population.append((offspring, 0.0))

        population = next_population[:population_size]

    logger.info(
        "GA complete: final_best=%.4f stagnation_stops=%d",
        best_fitness,
        stagnation_count,
    )


# ── Private helpers ────────────────────────────────────────────────────────────

def _evaluate_generation(
    population: List[CandidateParameterSet],
    windows: List[WFOWindow],
    scenario: ScenarioProfile,
    base_yaml_path: Path,
    temp_dir: Path,
    max_workers: int,
    min_significant_trades: int,
    run_id: str,
    gen_num: int,
    store: CandidateStore,
) -> Dict[str, float]:
    """
    Evaluate all population candidates across the 2 sampled windows in parallel.
    Returns a map of candidate_id → WFO-weighted fitness for this generation.
    """
    tasks: List[Tuple[CandidateParameterSet, WFOWindow]] = [
        (candidate, window)
        for candidate in population
        for window in windows
    ]

    results_by_candidate: Dict[str, List[WFOWindowResult]] = {
        c.candidate_id: [] for c in population
    }

    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        future_map = {
            pool.submit(
                evaluate_window,
                candidate,
                window,
                base_yaml_path,
                temp_dir,
                scenario,
                min_significant_trades,
            ): candidate.candidate_id
            for candidate, window in tasks
        }

        for future in as_completed(future_map):
            cid = future_map[future]
            try:
                result: WFOWindowResult = future.result()
                results_by_candidate[cid].append(result)
                store.write_wfo_window_result(result, run_id)
            except Exception as exc:
                logger.error(
                    "GA worker exception: candidate=%s gen=%d error=%s",
                    cid[:12], gen_num, exc, exc_info=True,
                )

    # Compute lightweight consistency score per candidate → return as fitness
    fitness_map: Dict[str, float] = {}
    for candidate in population:
        cid = candidate.candidate_id
        window_results = results_by_candidate.get(cid, [])
        if not window_results:
            fitness_map[cid] = 0.0
            continue
        consistency = compute_consistency(
            window_results=window_results,
            windows_total=len(windows),
            scenario=scenario,
            oos_gate_enabled=False,  # IS/OOS gate not applied in GA lightweight mode
        )
        fitness_map[cid] = consistency.composite_score

    return fitness_map