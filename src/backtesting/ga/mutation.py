"""
ga/mutation.py
--------------
Mutation operator for the GA.

Strategy per parameter type (from D-11 and FUNCTIONAL_SPEC.md Stage 3):
  - int/float (continuous): Gaussian perturbation, clamped and snapped to step grid
  - choice (discrete):       Random flip to a different value from the choices list

Zone boundaries are strictly enforced — no mutated parameter ever leaves its valid range.
Mutation rate applies per-parameter (not per-candidate).

Block 7D change (M-06): mutation_std_steps parameter added to mutate() and threaded
through to _mutate_int() and _mutate_float(). Previously hardcoded as 2.0. Default
value 2.0 preserves all existing behaviour. ga_engine reads from
config["genetic"]["mutation_std_steps"] and passes the value here.
This is a YAML/config-level parameter, not a ScenarioProfile field —
mutation is a GA process parameter, not an evaluation-lens parameter.
"""
from __future__ import annotations

import random
from typing import Any, Dict, Optional

from src.backtesting.contracts import CandidateParameterSet


def mutate(
    candidate: CandidateParameterSet,
    mutation_rate: float,
    parameter_space_def: dict,
    rng: random.Random,
    generation: Optional[int] = None,
    mutation_std_steps: float = 2.0,
) -> CandidateParameterSet:
    """
    Apply per-parameter mutation to produce a new CandidateParameterSet.

    Args:
        candidate:           Candidate to mutate.
        mutation_rate:       Probability that each individual parameter is mutated.
        parameter_space_def: Zone definition dict for this candidate's zone.
                             Must contain 'parameters' key with parameter specs.
        rng:                 Seeded Random instance.
        generation:          GA generation for the offspring.
        mutation_std_steps:  Standard deviation of Gaussian noise in step units for
                             int/float parameters. Default 2.0 (prior hardcoded value).
                             Read from config["genetic"]["mutation_std_steps"] by ga_engine.
                             Larger values → more aggressive exploration. Use lower values
                             (e.g. 0.5) for fine-tuning near a known good region.

    Returns:
        A new CandidateParameterSet (mutated or identical if no mutations triggered).
    """
    zone_params_def: dict = parameter_space_def.get("parameters", {})
    mutated: Dict[str, Any] = dict(candidate.parameters)

    for param_name, current_value in candidate.parameters.items():
        if rng.random() >= mutation_rate:
            continue  # This parameter not mutated this generation

        param_def = zone_params_def.get(param_name)
        if param_def is None:
            # Parameter not in zone definition — leave unchanged
            continue

        param_type = param_def.get("type", "float")

        if param_type == "choice":
            mutated[param_name] = _mutate_choice(current_value, param_def, rng)
        elif param_type == "int":
            mutated[param_name] = _mutate_int(current_value, param_def, rng, mutation_std_steps)
        elif param_type == "float":
            mutated[param_name] = _mutate_float(current_value, param_def, rng, mutation_std_steps)

    return CandidateParameterSet.create(
        zone_name=candidate.zone_name,
        parameters=mutated,
        generation=generation,
    )


# ── Parameter-type mutation implementations ────────────────────────────────────

def _mutate_choice(current_value: Any, param_def: dict, rng: random.Random) -> Any:
    """Random flip to a different choice. If only one choice exists, return unchanged."""
    choices = param_def.get("choices", [])
    alternatives = [c for c in choices if c != current_value]
    if not alternatives:
        return current_value
    return rng.choice(alternatives)


def _mutate_int(
    current_value: int,
    param_def: dict,
    rng: random.Random,
    mutation_std_steps: float,
) -> int:
    """
    Gaussian perturbation on the step grid for integer parameters.
    Standard deviation = mutation_std_steps (in step units).
    """
    low: int = param_def["min"]
    high: int = param_def["max"]
    step: int = param_def.get("step", 1)

    # Gaussian noise in step units, rounded to nearest step
    noise_steps = rng.gauss(0, mutation_std_steps)
    noise = int(round(noise_steps)) * step

    new_value = current_value + noise
    # Snap to step grid relative to min
    snapped = _snap_to_grid_int(new_value, low, step)
    return max(low, min(high, snapped))


def _mutate_float(
    current_value: float,
    param_def: dict,
    rng: random.Random,
    mutation_std_steps: float,
) -> float:
    """
    Gaussian perturbation on the step grid for float parameters.
    Standard deviation = mutation_std_steps (in step units).
    """
    low: float = param_def["min"]
    high: float = param_def["max"]
    step: float = param_def.get("step", 0.1)

    noise_steps = rng.gauss(0, mutation_std_steps)
    noise = noise_steps * step

    new_value = current_value + noise
    # Snap to nearest step grid point
    snapped = _snap_to_grid_float(new_value, low, step)
    # Clamp to [low, high]
    return max(low, min(high, round(snapped, 6)))


def _snap_to_grid_int(value: int, grid_start: int, step: int) -> int:
    """Snap value to the nearest grid point starting at grid_start with given step."""
    if step <= 0:
        return value
    offset = value - grid_start
    snapped_offset = round(offset / step) * step
    return grid_start + snapped_offset


def _snap_to_grid_float(value: float, grid_start: float, step: float) -> float:
    """Snap value to the nearest grid point starting at grid_start with given step."""
    if step <= 0.0:
        return value
    offset = value - grid_start
    snapped_offset = round(offset / step) * step
    return grid_start + snapped_offset