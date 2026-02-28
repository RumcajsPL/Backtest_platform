"""
sampler.py — Latin Hypercube and random sampling from the expanded parameter space.

No strategy knowledge. No evaluation. Returns CandidateParameterSet instances.
"""
from __future__ import annotations

import random as stdlib_random
from typing import Dict, List

from src.backtesting.contracts import CandidateParameterSet


def sample_lhs(
    expanded_space: Dict[str, List[Dict[str, object]]],
    n_per_zone: int,
    seed: int,
) -> List[CandidateParameterSet]:
    """
    Latin Hypercube Sampling across all zones.

    For each zone, treat each parameter position as one 'dimension'. Divide
    the n_per_zone strata evenly, pick one value per stratum per parameter
    (without replacement within that stratum), then combine parameter vectors
    into candidate sets.

    Returns one CandidateParameterSet per sampled point.
    The total returned count is n_per_zone × len(enabled_zones), subject to
    the zone having at least n_per_zone distinct combinations.
    """
    rng = stdlib_random.Random(seed)
    results: List[CandidateParameterSet] = []

    for zone_name, combinations in expanded_space.items():
        if not combinations:
            continue
        actual_n = min(n_per_zone, len(combinations))
        sampled = _lhs_sample(combinations, actual_n, rng)
        for params in sampled:
            results.append(
                CandidateParameterSet.create(
                    zone_name=zone_name,
                    parameters=params,
                    generation=None,
                )
            )

    return results


def sample_random(
    expanded_space: Dict[str, List[Dict[str, object]]],
    n_per_zone: int,
    seed: int,
) -> List[CandidateParameterSet]:
    """
    Uniform random sampling with replacement from each zone's expanded space.
    """
    rng = stdlib_random.Random(seed)
    results: List[CandidateParameterSet] = []

    for zone_name, combinations in expanded_space.items():
        if not combinations:
            continue
        actual_n = min(n_per_zone, len(combinations))
        # Sample without replacement (unique combinations only)
        sampled = rng.sample(combinations, actual_n)
        for params in sampled:
            results.append(
                CandidateParameterSet.create(
                    zone_name=zone_name,
                    parameters=params,
                    generation=None,
                )
            )

    return results


# ── Internal helper ───────────────────────────────────────────────────────────

def _lhs_sample(
    combinations: List[Dict[str, object]],
    n: int,
    rng: stdlib_random.Random,
) -> List[Dict[str, object]]:
    """
    Latin Hypercube Sampling over a list of pre-expanded parameter combinations.

    Treats each parameter independently. For each parameter, divides the full
    set of distinct values into n equal strata. Picks one value from each
    stratum. Combines the stratified samples per parameter into n candidate
    dicts (by random column-wise permutation), ensuring each parameter value
    stratum appears exactly once.

    This correctly implements the LHS 'space-filling' property: every region
    of the parameter space is represented exactly once per dimension.
    """
    if n >= len(combinations):
        return list(rng.sample(combinations, len(combinations)))

    if not combinations:
        return []

    param_names = list(combinations[0].keys())

    # Build sorted distinct value list per parameter
    param_value_universe: Dict[str, list] = {}
    for name in param_names:
        seen = []
        seen_set = set()
        for combo in combinations:
            v = combo[name]
            key = str(v)
            if key not in seen_set:
                seen_set.add(key)
                seen.append(v)
        param_value_universe[name] = sorted(seen, key=lambda x: (str(type(x)), str(x)))

    # For each parameter, assign stratified samples
    # Divide the value list into n equal strata, pick one from each
    stratified_values: Dict[str, list] = {}
    for name in param_names:
        universe = param_value_universe[name]
        n_vals = len(universe)
        stratum_size = max(1, n_vals / n)
        picks = []
        for stratum_idx in range(n):
            lo = int(stratum_idx * stratum_size)
            hi = int((stratum_idx + 1) * stratum_size)
            hi = min(hi, n_vals)
            lo = min(lo, n_vals - 1)
            hi = max(hi, lo + 1)
            pick = rng.choice(universe[lo:hi])
            picks.append(pick)
        # Shuffle so parameter combinations aren't correlated across dimensions
        rng.shuffle(picks)
        stratified_values[name] = picks

    # Assemble n candidate dicts
    result = []
    for i in range(n):
        candidate = {name: stratified_values[name][i] for name in param_names}
        result.append(candidate)

    return result