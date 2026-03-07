"""
sampler.py — Latin Hypercube and random sampling from the parameter space.

No strategy knowledge. No evaluation. Returns CandidateParameterSet instances.

B9F-001: Updated to accept the new expand_zones() output format:
    Dict[zone_name, Dict[param_name, List[value]]]
instead of the previous:
    Dict[zone_name, List[Dict[param_name, value]]]  (full Cartesian product)

_lhs_sample() now receives per-param value lists directly, eliminating the
former distinct-value extraction loop (param_value_universe construction) which
was redundant — the value lists from expand_zones() are already the universe.

sample_random() previously called rng.sample(combinations, n) over a full
pre-expanded list. It now draws n values independently per parameter from each
parameter's value list, then assembles candidate dicts. This is equivalent
space-filling behaviour without requiring the full Cartesian product.

B9I-001: Removed min_universe_size hard cap from _lhs_sample().
    The cap `actual_n = min(n, min_universe_size)` was silently clamping
    samples_per_zone=200 to 4 (the size of the smallest parameter universe
    in the safe zone). LHS can correctly handle n > universe_size by cycling
    strata — repeated values per parameter dimension are acceptable when the
    requested sample count exceeds the discrete grid size. The hard cap is
    removed. The stratum calculation now uses modulo-based cycling so that
    all n strata are filled regardless of n_vals.
"""
from __future__ import annotations

import random as stdlib_random
from typing import Dict, List

from src.backtesting.contracts import CandidateParameterSet


def sample_lhs(
    expanded_space: Dict[str, Dict[str, List]],
    n_per_zone: int,
    seed: int,
) -> List[CandidateParameterSet]:
    """
    Latin Hypercube Sampling across all zones.

    For each zone, divides each parameter's value list into n_per_zone equal
    strata, picks one value per stratum per parameter (without replacement
    within that stratum), shuffles per-parameter picks independently, then
    combines parameter vectors into candidate sets. This correctly implements
    the LHS 'space-filling' property: every region of the parameter space is
    represented exactly once per dimension.

    When n_per_zone > universe_size for any parameter, strata cycle through
    the available values (sampling with replacement across cycles). This is
    correct behaviour for discrete parameter grids where the requested sample
    count may exceed the number of distinct values in a given dimension.

    expanded_space: Dict[zone_name, Dict[param_name, List[value]]]
        Output of parameter_space.expand_zones(). Per-param value lists,
        not a pre-expanded Cartesian product (B9F-001).

    Returns one CandidateParameterSet per sampled point.
    Total returned count is n_per_zone × len(enabled_zones).
    """
    rng = stdlib_random.Random(seed)
    results: List[CandidateParameterSet] = []

    for zone_name, param_value_lists in expanded_space.items():
        if not param_value_lists:
            continue
        sampled = _lhs_sample(param_value_lists, n_per_zone, rng)
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
    expanded_space: Dict[str, Dict[str, List]],
    n_per_zone: int,
    seed: int,
) -> List[CandidateParameterSet]:
    """
    Uniform random sampling from each zone's parameter space.

    Draws n_per_zone samples per zone by sampling each parameter's value list
    independently (with replacement). Assembles candidate dicts by zipping the
    per-parameter draws.

    This is equivalent to random sampling over the joint distribution without
    requiring the full Cartesian product to be materialised (B9F-001).

    expanded_space: Dict[zone_name, Dict[param_name, List[value]]]
        Output of parameter_space.expand_zones(). Per-param value lists.
    """
    rng = stdlib_random.Random(seed)
    results: List[CandidateParameterSet] = []

    for zone_name, param_value_lists in expanded_space.items():
        if not param_value_lists:
            continue
        param_names = list(param_value_lists.keys())
        for _ in range(n_per_zone):
            params = {
                name: rng.choice(param_value_lists[name])
                for name in param_names
            }
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
    param_value_lists: Dict[str, List],
    n: int,
    rng: stdlib_random.Random,
) -> List[Dict[str, object]]:
    """
    Latin Hypercube Sampling over per-parameter value lists.

    For each parameter, sorts its value list numerically (or lexicographically
    for choice strings), divides into n equal strata, picks one value from each
    stratum, then shuffles the picks independently per parameter. Combines
    per-parameter picks into n candidate dicts.

    param_value_lists: Dict[param_name, List[value]]
        Per-param value lists as returned by expand_zones() (B9F-001).
        Replaces the former List[Dict] Cartesian product input.

    The distinct-value extraction loop (param_value_universe) from the prior
    implementation is removed: the value lists from expand_zones() are already
    the universe of valid discrete values per parameter.

    B9I-001: No hard cap on n. When n > universe_size for a parameter, strata
    are computed modulo universe_size so all n picks are filled. Each stratum
    maps to a position in the (cycled) universe, and one value is drawn from
    the corresponding window. This allows samples_per_zone=200 to produce 200
    candidates even when the smallest parameter has only 4 discrete values.
    """
    if not param_value_lists:
        return []

    param_names = list(param_value_lists.keys())

    # Sort each parameter's value list so LHS strata respect actual ordering.
    # B9C-007: float() sort key for numeric types; str() fallback for choice strings.
    sorted_values: Dict[str, List] = {}
    for name in param_names:
        universe = param_value_lists[name]
        try:
            sorted_values[name] = sorted(universe, key=lambda x: float(x))
        except (TypeError, ValueError):
            sorted_values[name] = sorted(universe, key=lambda x: str(x))

    # B9I-001: min_universe_size hard cap REMOVED.
    # actual_n = n always. When n > universe_size, strata cycle through the
    # available values using modulo indexing. This is correct for discrete grids.

    # For each parameter, assign stratified samples.
    stratified_values: Dict[str, List] = {}
    for name in param_names:
        universe = sorted_values[name]
        n_vals = len(universe)
        picks = []
        for stratum_idx in range(n):
            # Map stratum_idx into the available universe cyclically.
            # When n <= n_vals: standard LHS — each stratum covers n_vals/n values.
            # When n > n_vals: strata cycle — stratum_idx mod n_vals determines
            # the base position, and a window of size 1 is used (single value).
            if n <= n_vals:
                stratum_size = n_vals / n
                lo = int(stratum_idx * stratum_size)
                hi = int((stratum_idx + 1) * stratum_size)
                hi = min(hi, n_vals)
                lo = min(lo, n_vals - 1)
                hi = max(hi, lo + 1)
                pick = rng.choice(universe[lo:hi])
            else:
                # n > universe_size: cycle through universe values.
                # Each position in the cycle maps to one universe value.
                cycled_idx = stratum_idx % n_vals
                pick = universe[cycled_idx]
            picks.append(pick)
        # Shuffle so parameter combinations aren't correlated across dimensions.
        rng.shuffle(picks)
        stratified_values[name] = picks

    # Assemble n candidate dicts.
    return [
        {name: stratified_values[name][i] for name in param_names}
        for i in range(n)
    ]