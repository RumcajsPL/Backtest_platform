"""
evaluation/sensitivity.py
─────────────────────────
Stage 6 — Parameter Sensitivity Evaluator.

For each optimizable parameter in the candidate's zone, perturbs the value at
±1 and ±2 steps from its current value, evaluates fitness at each perturbation,
and computes the fitness delta (perturbed - baseline).

A spike is detected when |fitness_delta| > spike_threshold for any perturbation.
profile_complete is False when >50% of perturbation evaluations fail.

Public interface
────────────────
    evaluate_sensitivity(
        candidate, baseline_fitness, parameter_space_def,
        base_yaml_path, temp_dir, scenario, spike_threshold, max_steps=2,
        max_workers=6, min_significant_trades=30,
        pool=None,
    ) -> SensitivityProfile

Block 7C change (OPT-01): Optional `pool` parameter added.
- If pool is None (default, standalone/test call): function creates and owns its
  own ProcessPoolExecutor — identical behaviour to pre-OPT-01.
- If pool is provided (from orchestrator Stage 6): function uses the shared pool
  and does NOT close it. The pool stays warm across all candidates in the loop,
  paying the Windows spawn cost only once instead of once per candidate.
  With N=5 candidates and ~9 parameters ×4 steps per candidate, this eliminates
  4 of 5 pool startup cycles — expected 40–60% Stage 6 reduction.
Existing tests require no changes — they call without pool kwarg (pool=None path).
"""

from __future__ import annotations

import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import contextmanager, nullcontext
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from src.backtesting.contracts import (
    CandidateParameterSet,
    CandidateResult,
    FitnessResult,
    ParameterSensitivity,
    ScenarioProfile,
    SensitivityProfile,
)
from src.backtesting.fitness import evaluate_fitness
from src.backtesting.strategy_runner import evaluate as runner_evaluate

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Parameter step helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_param_def(parameter_name: str, zone_def: dict) -> Optional[dict]:
    """Return the parameter definition dict for parameter_name in zone_def, or None."""
    return zone_def.get("parameters", {}).get(parameter_name)


def _perturb_value(
    current_value: object,
    step_offset: int,
    param_def: dict,
) -> Optional[object]:
    """
    Return the perturbed value for a parameter at the given step_offset (e.g. -2, -1, +1, +2).
    Returns None if the perturbed value would fall outside the zone min/max.
    Handles int, float, and choice parameter types.
    """
    param_type = param_def.get("type")

    if param_type == "int":
        step_size = param_def["step"]
        new_val = int(current_value) + step_offset * step_size
        if new_val < param_def["min"] or new_val > param_def["max"]:
            return None
        return new_val

    if param_type == "float":
        step_size = param_def["step"]
        new_val = round(float(current_value) + step_offset * step_size, 10)
        if new_val < param_def["min"] - 1e-9 or new_val > param_def["max"] + 1e-9:
            return None
        # Round to the step precision to avoid floating-point drift
        precision = len(str(step_size).rstrip("0").split(".")[-1]) if "." in str(step_size) else 0
        return round(new_val, precision)

    if param_type == "choice":
        choices = param_def["choices"]
        try:
            current_idx = choices.index(current_value)
        except ValueError:
            return None
        new_idx = current_idx + step_offset
        if new_idx < 0 or new_idx >= len(choices):
            return None
        return choices[new_idx]

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Worker — runs in a spawned process
# ─────────────────────────────────────────────────────────────────────────────

def _evaluate_perturbation(
    base_candidate: CandidateParameterSet,
    parameter_name: str,
    perturbed_value: object,
    base_yaml_path: Path,
    temp_dir: Path,
    scenario: ScenarioProfile,
    min_significant_trades: int,
) -> Tuple[str, object, Optional[float], Optional[str]]:
    """
    Worker function: evaluate a single perturbed candidate.

    Returns:
        (parameter_name, perturbed_value, fitness_score_or_None, error_or_None)
    """
    perturbed_params = dict(base_candidate.parameters)
    perturbed_params[parameter_name] = perturbed_value
    perturbed_candidate = CandidateParameterSet.create(
        zone_name=base_candidate.zone_name,
        parameters=perturbed_params,
        generation=base_candidate.generation,
    )

    result: CandidateResult = runner_evaluate(
        candidate=perturbed_candidate,
        base_yaml_path=base_yaml_path,
        temp_dir=temp_dir,
        min_significant_trades=min_significant_trades,
    )

    if not result.is_valid:
        return (parameter_name, perturbed_value, None, result.error)

    fitness_result: FitnessResult = evaluate_fitness(result, scenario)
    fitness_score = fitness_result.fitness_score  # None if constraints failed

    if fitness_score is None:
        error = f"constraints_failed:{fitness_result.rejection_reason}"
        return (parameter_name, perturbed_value, None, error)

    return (parameter_name, perturbed_value, fitness_score, None)


# ─────────────────────────────────────────────────────────────────────────────
# Public interface
# ─────────────────────────────────────────────────────────────────────────────

def evaluate_sensitivity(
    candidate: CandidateParameterSet,
    baseline_fitness: float,
    parameter_space_def: dict,
    base_yaml_path: Path,
    temp_dir: Path,
    scenario: ScenarioProfile,
    spike_threshold: float,
    max_steps: int = 2,
    max_workers: int = 6,
    min_significant_trades: int = 30,
    pool: Optional[ProcessPoolExecutor] = None,
) -> SensitivityProfile:
    """
    Perturbs each parameter in the candidate's zone at ±1..max_steps steps.
    Evaluates fitness at each perturbation using parallel workers.

    Parameters
    ──────────
    candidate            : The candidate whose parameters are being perturbed.
    baseline_fitness     : The candidate's fitness score on the full dataset (Stage 1 result).
    parameter_space_def  : The full zones dict from backtest_template.yaml.
    base_yaml_path       : Path to the base strategy YAML.
    temp_dir             : Temp directory for per-worker YAMLs.
    scenario             : Active ScenarioProfile (for constraint + fitness evaluation).
    spike_threshold      : |fitness_delta| above this value triggers a spike flag.
    max_steps            : Maximum number of steps to perturb in each direction (default 2).
    max_workers          : ProcessPoolExecutor worker count. Used only when pool=None.
    min_significant_trades: Minimum trade count to accept an evaluation.
    pool                 : Optional shared ProcessPoolExecutor. If provided, the pool is
                           used as-is and NOT shut down on return — the caller owns the
                           pool lifecycle. If None, a new pool is created and managed
                           internally (backward-compatible default).

    Returns
    ───────
    SensitivityProfile
    """
    zone_def = parameter_space_def.get(candidate.zone_name, {})
    current_params: dict = dict(candidate.parameters)

    # Build the list of (parameter_name, step_offset, perturbed_value) to evaluate
    perturbation_plan: List[Tuple[str, int, object]] = []
    for param_name, current_value in current_params.items():
        param_def = _get_param_def(param_name, zone_def)
        if param_def is None:
            logger.debug(
                "Parameter '%s' not found in zone '%s' definition — skipping sensitivity.",
                param_name, candidate.zone_name,
            )
            continue

        for step_offset in _step_offsets(max_steps):
            perturbed = _perturb_value(current_value, step_offset, param_def)
            if perturbed is None:
                logger.debug(
                    "Parameter '%s' step %+d out of bounds — skipping.",
                    param_name, step_offset,
                )
                continue
            perturbation_plan.append((param_name, step_offset, perturbed))

    total_perturbations = len(perturbation_plan)
    logger.info(
        "Sensitivity: candidate %s — %d perturbations planned across %d parameters.",
        candidate.candidate_id[:12],
        total_perturbations,
        len(set(p[0] for p in perturbation_plan)),
    )

    if total_perturbations == 0:
        return SensitivityProfile(
            candidate_id=candidate.candidate_id,
            baseline_fitness=baseline_fitness,
            parameter_sensitivities=(),
            spike_detected=False,
            spike_parameters=(),
            profile_complete=True,
        )

    # ── Execute perturbations in parallel ────────────────────────────────────
    # OPT-01: if a shared pool is provided, use it directly (no context manager).
    # If no pool is provided, create and own a local pool (original behaviour).
    results_by_key: Dict[Tuple[str, int], ParameterSensitivity] = {}

    pool_cm = (
        nullcontext(pool)
        if pool is not None
        else ProcessPoolExecutor(max_workers=max_workers)
    )

    with pool_cm as active_pool:
        future_map = {
            active_pool.submit(
                _evaluate_perturbation,
                candidate,
                param_name,
                perturbed_value,
                base_yaml_path,
                temp_dir,
                scenario,
                min_significant_trades,
            ): (param_name, step_offset, perturbed_value)
            for (param_name, step_offset, perturbed_value) in perturbation_plan
        }

        for future in as_completed(future_map):
            param_name, step_offset, perturbed_value = future_map[future]
            try:
                _, _, fitness_score, error = future.result()
            except Exception as exc:
                fitness_score = None
                error = str(exc)
                logger.error(
                    "Sensitivity worker raised for param '%s' step %+d: %s",
                    param_name, step_offset, exc,
                )

            fitness_delta = (
                (fitness_score - baseline_fitness) if fitness_score is not None else None
            )

            results_by_key[(param_name, step_offset)] = ParameterSensitivity(
                parameter_name=param_name,
                step=step_offset,
                perturbed_value=perturbed_value,
                fitness_delta=fitness_delta,
                evaluation_error=error,
            )

    # ── Assemble in deterministic order ──────────────────────────────────────
    parameter_sensitivities: Tuple[ParameterSensitivity, ...] = tuple(
        results_by_key[(param_name, step_offset)]
        for (param_name, step_offset, _) in perturbation_plan
        if (param_name, step_offset) in results_by_key
    )

    # ── Completeness check ────────────────────────────────────────────────────
    failed_count = sum(
        1 for ps in parameter_sensitivities if ps.fitness_delta is None
    )
    total_evaluated = len(parameter_sensitivities)
    profile_complete = (failed_count / total_evaluated) <= 0.50 if total_evaluated > 0 else True

    if not profile_complete:
        logger.warning(
            "Sensitivity profile incomplete for candidate %s: %d/%d perturbations failed.",
            candidate.candidate_id[:12], failed_count, total_evaluated,
        )

    # ── Spike detection ───────────────────────────────────────────────────────
    spike_param_names: List[str] = []
    for ps in parameter_sensitivities:
        if ps.fitness_delta is not None and abs(ps.fitness_delta) > spike_threshold:
            if ps.parameter_name not in spike_param_names:
                spike_param_names.append(ps.parameter_name)

    spike_detected = len(spike_param_names) > 0

    if spike_detected:
        logger.warning(
            "Sensitivity spike detected for candidate %s: parameters %s exceed threshold %.3f.",
            candidate.candidate_id[:12], spike_param_names, spike_threshold,
        )

    return SensitivityProfile(
        candidate_id=candidate.candidate_id,
        baseline_fitness=baseline_fitness,
        parameter_sensitivities=parameter_sensitivities,
        spike_detected=spike_detected,
        spike_parameters=tuple(spike_param_names),
        profile_complete=profile_complete,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _step_offsets(max_steps: int) -> List[int]:
    """Return step offsets in order: -max_steps, ..., -1, +1, ..., +max_steps."""
    offsets = list(range(-max_steps, 0)) + list(range(1, max_steps + 1))
    return offsets