"""
orchestrator.py — Pipeline entry point and stage sequencer.

Responsibilities (orchestrate only):
- Load and validate config
- Resume existing run or initialise new one
- Execute stages in order with checkpoint skip logic
- Write immutable run artifacts at start (config hash, seeds, perturbation profile)

Stages 1–3 are now fully implemented.
Stage 4 remains a stub pending its respective phase implementation.
Stages 0, 5, 6, and 7 are fully implemented.

Block 9A fixes applied:
  B9A-002: Stage 1 stub now advances checkpoint to RANDOM_SEARCH_COMPLETE.
           Previously the checkpoint was never advanced, causing Stage 1 to
           re-run on every pipeline resume.

Block 9D fixes applied:
  B9A-001: Stages 5, 6, 7 now use ranker.rank_by_wfo() (returns List[CandidateRecord])
           instead of the orchestrator's former inline rank_by_wfo() (returned
           List[Dict]). All record access uses typed attributes, not dict keys.
  B9A-003: Stage 6 spike_threshold now reads from
           scenario.verdict_sensitivity_spike_threshold instead of
           config["sensitivity"]["spike_threshold"]. Stage 0 validation block
           for spike_threshold removed (ScenarioProfile.__post_init__ owns it).
  B9C-007/B9C-006: sampler.py fixes applied upstream.
  B9C-004: wfo_engine.py empty guard applied upstream.
  B9C-005: parameter_space.py Decimal fix applied upstream.
  B8-006: twin key map comments added to strategy_runner.py + yaml_generator.py.
"""
from __future__ import annotations

import hashlib
import logging
from concurrent.futures import ProcessPoolExecutor
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from src.backtesting.candidate_store import CandidateStore
from src.backtesting.contracts import (
    CandidateParameterSet,
    CandidateRecord,
    CandidateResult,
    CandidateStage,
    Checkpoint,
    MCMode,
    MCResult,
    RunMetadata,
    ScenarioProfile,
    SensitivityProfile,
    Verdict,
    VerdictResult,
    WFOConsistencyScore,
    WFOWindow,
)
from src.backtesting.evaluation.sensitivity import evaluate_sensitivity
from src.backtesting.evaluation.verdict import compute_verdict
from src.backtesting.report_generator import generate_report
from src.backtesting.scenario import load_scenario
from src.backtesting.yaml_generator import build_output_path, generate_trading_yaml

logger = logging.getLogger(__name__)

BACKTESTER_VERSION = "1.0.0"


# ── Public entry point ─────────────────────────────────────────────────────────

def run(config_path: Path) -> None:
    """
    Main pipeline entry point. Loads config, opens the store, resumes or starts
    a fresh run, and executes all enabled stages in order.
    CandidateStore.close() is guaranteed via finally — drains write queue and
    closes the SQLite connection cleanly even on exception.
    """
    config = _load_and_validate_config(config_path)
    output_dir = Path(config["run"]["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = output_dir / "backtester.db"

    store = CandidateStore(db_path)
    try:
        run_metadata = _resume_or_start(store, config, config_path)
        _execute_pipeline(config, store, run_metadata)
        store.set_checkpoint(run_metadata.run_id, Checkpoint.COMPLETE)
        logger.info("Pipeline complete — run_id=%s", run_metadata.run_id)
    finally:
        store.close()   # drains write queue, joins writer thread, closes connection


# ── Config loading ─────────────────────────────────────────────────────────────

def _load_and_validate_config(config_path: Path) -> dict:
    """
    Load backtest_template.yaml and validate required top-level keys.
    Raises ValueError for missing required keys.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if config is None:
        raise ValueError(f"Config file is empty: {config_path}")

    _require_keys(config, ["backtester_version", "scenario", "run", "scenarios", "zones", "walk_forward"])
    _require_keys(config["run"], ["output_dir", "temp_dir"])

    windows = config.get("walk_forward", {}).get("windows", [])
    if len(windows) < 3:
        raise ValueError(
            f"backtest_template.yaml requires at least 3 WFO windows; "
            f"found {len(windows)}. Add more windows under walk_forward.windows."
        )

    return config


def _compute_config_hash(config_path: Path) -> str:
    """SHA-256 of the raw YAML file content."""
    return hashlib.sha256(config_path.read_bytes()).hexdigest()


# ── Resume or start ────────────────────────────────────────────────────────────

def _resume_or_start(
    store: CandidateStore,
    config: dict,
    config_path: Path,
) -> RunMetadata:
    """
    Check for an existing run with the same config hash. If found and checkpoint
    is not COMPLETE, resume it. If config hash has changed, refuse to resume and
    raise. If no existing run found, start fresh.

    B8-009: Uses CandidateStore read API (get_incomplete_run / get_any_incomplete_run)
    instead of opening a raw sqlite3 connection directly. The store already holds
    an open WAL-mode connection — a second connection was architecturally wrong
    even if safe in practice.
    """
    current_hash = _compute_config_hash(config_path)

    # Check for resumable run with matching config hash
    run_id = store.get_incomplete_run(current_hash)
    if run_id is not None:
        logger.info("Resuming existing run %s", run_id)
        return store.get_run_metadata(run_id)

    # Check for config hash conflict (different incomplete run exists)
    conflict = store.get_any_incomplete_run()
    if conflict is not None:
        existing_run_id, existing_hash = conflict
        raise ValueError(
            f"Config hash mismatch. An incomplete run ({existing_run_id}) "
            f"exists with hash {existing_hash[:12]}… but current config hash "
            f"is {current_hash[:12]}…. Delete or complete the existing run first."
        )

    return _initialise_run(store, config, config_path, current_hash)

def _initialise_run(
    store: CandidateStore,
    config: dict,
    config_path: Path,
    config_hash: str,
) -> RunMetadata:
    """Create and persist a new RunMetadata row."""
    run_id = str(uuid.uuid4())
    wfo_window_ids = tuple(w["id"] for w in config["walk_forward"]["windows"])

    run_metadata = RunMetadata(
        run_id=run_id,
        config_hash=config_hash,
        scenario_name=config["scenario"],
        started_at=datetime.now(UTC),
        perturbation_profile_name=_extract_perturbation_profile(config),
        random_search_seed=config.get("random_search", {}).get("seed", 42),
        ga_seed=config.get("genetic", {}).get("seed", 43),
        mc_prefilter_seed=config.get("mc_prefilter", {}).get("seed", 44),
        mc_deep_seed=config.get("monte_carlo", {}).get("deep", {}).get("seed", 45),
        sensitivity_seed=config.get("sensitivity", {}).get("seed", 46),
        wfo_window_ids=wfo_window_ids,
        checkpoint=Checkpoint.NOT_STARTED,
        backtester_version=BACKTESTER_VERSION,
    )
    store.initialise_run(run_metadata)
    logger.info("New run initialised: run_id=%s", run_id)
    return run_metadata


# ── Pipeline stage execution ───────────────────────────────────────────────────

def _execute_pipeline(
    config: dict,
    store: CandidateStore,
    run_metadata: RunMetadata,
) -> None:
    """Execute all pipeline stages in order with checkpoint skip logic."""
    run_id = run_metadata.run_id

    # ── Stage 0: Validation & Init ────────────────────────────────────────────
    if store.get_checkpoint(run_id).value < Checkpoint.RUN_INITIALISED.value:
        _run_stage_0_init(config, store, run_metadata)
        store.set_checkpoint(run_id, Checkpoint.RUN_INITIALISED)
    else:
        logger.info("Stage 0 already complete — skipping")

    # ── Stage 1: Random Search ────────────────────────────────────────────────
    if store.get_checkpoint(run_id).value < Checkpoint.RANDOM_SEARCH_COMPLETE.value:
        _run_stage_1_random_search(config, store, run_metadata)
        store.set_checkpoint(run_id, Checkpoint.RANDOM_SEARCH_COMPLETE)
    else:
        logger.info("Stage 1 (Random Search) already complete — skipping")

    # ── Stage 2: MC Pre-Filter ────────────────────────────────────────────────
    if store.get_checkpoint(run_id).value < Checkpoint.MC_PREFILTER_COMPLETE.value:
        _run_stage_2_mc_prefilter(config, store, run_metadata)
        store.set_checkpoint(run_id, Checkpoint.MC_PREFILTER_COMPLETE)
    else:
        logger.info("Stage 2 (MC Pre-Filter) already complete — skipping")

    # ── Stage 3: Genetic Algorithm ────────────────────────────────────────────
    if store.get_checkpoint(run_id).value < Checkpoint.GA_COMPLETE.value:
        _run_stage_3_ga(config, store, run_metadata)
        store.set_checkpoint(run_id, Checkpoint.GA_COMPLETE)
    else:
        logger.info("Stage 3 (GA) already complete — skipping")

    # ── Stage 4: Full WFO ─────────────────────────────────────────────────────
    if store.get_checkpoint(run_id).value < Checkpoint.WFO_COMPLETE.value:
        _run_stage_4_wfo(config, store, run_metadata)
        store.set_checkpoint(run_id, Checkpoint.WFO_COMPLETE)
    else:
        logger.info("Stage 4 (Full WFO) already complete — skipping")

    # ── Stage 5: MC Deep ──────────────────────────────────────────────────────
    _t5 = time.perf_counter()
    if store.get_checkpoint(run_id).value < Checkpoint.MONTE_CARLO_COMPLETE.value:
        _run_stage_5_mc_deep(config, store, run_metadata)
        store.set_checkpoint(run_id, Checkpoint.MONTE_CARLO_COMPLETE)
    else:
        logger.info("Stage 5 (MC Deep) already complete — skipping")
    _elapsed_5 = time.perf_counter() - _t5

    # ── Stage 6: Parameter Sensitivity ───────────────────────────────────────
    _t6 = time.perf_counter()
    if store.get_checkpoint(run_id).value < Checkpoint.SENSITIVITY_COMPLETE.value:
        _run_stage_6_sensitivity(config, store, run_metadata)
        store.set_checkpoint(run_id, Checkpoint.SENSITIVITY_COMPLETE)
    else:
        logger.info("Stage 6 (Sensitivity) already complete — skipping")
    _elapsed_6 = time.perf_counter() - _t6

    # ── Stage 7: Report & Output ──────────────────────────────────────────────
    _t7 = time.perf_counter()
    if store.get_checkpoint(run_id).value < Checkpoint.COMPLETE.value:
        _run_stage_7_report(config, store, run_metadata)
    else:
        logger.info("Stage 7 (Report) already complete — skipping")
    _elapsed_7 = time.perf_counter() - _t7

    _elapsed_total = _elapsed_5 + _elapsed_6 + _elapsed_7
    _budget = 14400.0
    # NOTE (B8-008): Timing covers stages 5–7 only. When Stage 1–4 stubs are
    # replaced, move _t_total to the top of this function and sum all stage durations.
    logger.info("TIMING stage_5_mc_deep elapsed=%.1fs", _elapsed_5)
    logger.info("TIMING stage_6_sensitivity elapsed=%.1fs", _elapsed_6)
    logger.info("TIMING stage_7_report elapsed=%.1fs", _elapsed_7)
    logger.info(
        "TIMING SUMMARY  stage5=%.1fs  stage6=%.1fs  stage7=%.1fs  total=%.1fs  budget=%.0fs  %s",
        _elapsed_5, _elapsed_6, _elapsed_7, _elapsed_total, _budget,
        "PASS" if _elapsed_total <= _budget else "OVER BUDGET",
    )


# ── Stage 0: Validation & Init ────────────────────────────────────────────────

def _run_stage_0_init(
    config,
    store,
    run_metadata,
) -> None:
    """
    Validate configuration, scenario, WFO windows, parameter names, and enabled zones.
    Raises ValueError on any validation failure.
    """
    logger.info("Stage 0: Validation & Init — run_id=%s", run_metadata.run_id)

    from src.backtesting.scenario import load_scenario
    try:
        scenario = load_scenario(config)
        logger.debug("Scenario '%s' loaded successfully", scenario.name)
    except (ValueError, KeyError) as exc:
        raise ValueError(f"Stage 0: Scenario validation failed: {exc}") from exc

    _validate_wfo_windows(config.get("walk_forward", {}).get("windows", []))

    # M-05: Validate that all enabled zone parameter names exist in _PARAM_KEY_MAP.
    _validate_parameter_names(config)

    # B8-005: Validate min_significant_trades >= 1.
    min_significant_trades = config.get("random_search", {}).get("min_significant_trades", 30)
    if min_significant_trades < 1:
        raise ValueError(
            f"Stage 0: random_search.min_significant_trades must be >= 1; "
            f"got {min_significant_trades}. A value of 0 disables the significance guard."
        )

    # NOTE (B9A-003 applied): spike_threshold Stage 0 validation removed.
    # ScenarioProfile.__post_init__ owns the guard via
    # verdict_sensitivity_spike_threshold. Stage 6 now reads from
    # scenario.verdict_sensitivity_spike_threshold directly.

    zones = config.get("zones", {})
    enabled_zones = [name for name, zdef in zones.items() if zdef.get("enabled", True)]
    if not enabled_zones:
        raise ValueError(
            "Stage 0: No parameter zones are enabled. "
            "Enable at least one zone under 'zones' in the config."
        )
    logger.debug("Enabled parameter zones: %s", enabled_zones)
    logger.info(
        "Stage 0: All validations passed — %d WFO windows, %d enabled zones",
        len(config.get("walk_forward", {}).get("windows", [])),
        len(enabled_zones),
    )


def _validate_wfo_windows(windows_config: list) -> None:
    """Validate WFO window definitions — min 3, unique IDs, valid dates, start < end."""
    from datetime import date

    if len(windows_config) < 3:
        raise ValueError(
            f"Minimum 3 WFO windows required for GA random sampling; "
            f"found {len(windows_config)}."
        )
    seen_ids: set = set()
    for w in windows_config:
        window_id = w.get("id")
        if not window_id:
            raise ValueError("WFO window missing 'id' field")
        if window_id in seen_ids:
            raise ValueError(f"Duplicate WFO window id: '{window_id}'")
        seen_ids.add(window_id)
        try:
            start = date.fromisoformat(str(w["start"]))
            end = date.fromisoformat(str(w["end"]))
        except (KeyError, ValueError) as exc:
            raise ValueError(
                f"WFO window '{window_id}': invalid date. Error: {exc}"
            ) from exc
        if start >= end:
            raise ValueError(
                f"WFO window '{window_id}': start ({start}) must be before end ({end})"
            )


def _validate_parameter_names(config: dict) -> None:
    """
    Validate that all parameter names in enabled zones exist in strategy_runner._PARAM_KEY_MAP.

    Raises ValueError listing all unknown parameter names (sorted, for deterministic
    test assertions) if any are found. Only enabled zones are checked.

    Called from _run_stage_0_init (M-05 audit remediation).
    """
    from src.backtesting.strategy_runner import _PARAM_KEY_MAP

    zones = config.get("zones", {})
    enabled_param_names = {
        param_name
        for zone_name, zone_def in zones.items()
        if zone_def.get("enabled", True)
        for param_name in zone_def.get("parameters", {})
    }

    unknown = enabled_param_names - set(_PARAM_KEY_MAP.keys())
    if unknown:
        raise ValueError(
            f"Stage 0: Zone parameters not in strategy_runner._PARAM_KEY_MAP: "
            f"{sorted(unknown)}. "
            f"Check for typos or add mappings to _PARAM_KEY_MAP before running."
        )

    logger.debug(
        "Parameter name validation passed — %d unique params checked against _PARAM_KEY_MAP",
        len(enabled_param_names),
    )


# ── Stage 1: Random Search ────────────────────────────────────────────────────

def _run_stage_1_random_search(
    config: dict, store: CandidateStore, run_metadata: RunMetadata
) -> None:
    """
    Stage 1: Random Search.

    Expands enabled parameter zones into discrete grids, samples candidates
    via LHS (or uniform random), evaluates each candidate with the full
    strategy runner + fitness evaluator, and writes passing and failing
    CandidateRecords to the store.

    All candidates (pass and fail) are written so Stage 0 audit queries work.
    """
    from src.backtesting.parameter_space import expand_zones
    from src.backtesting.sampler import sample_lhs, sample_random
    from src.backtesting.strategy_runner import evaluate
    from src.backtesting.fitness import evaluate_fitness
    from src.backtesting.scenario import load_scenario

    run_id = run_metadata.run_id
    rs_config = config.get("random_search", {})
    method: str = rs_config.get("method", "lhs")
    samples_per_zone: int = rs_config.get("samples_per_zone", 200)
    min_significant_trades: int = rs_config.get("min_significant_trades", 30)
    retain_temp: bool = config.get("run", {}).get("retain_temp_yamls", False)
    max_workers: int = config.get("run", {}).get("max_workers", 6)

    base_yaml_path = _resolve_base_yaml(config)
    temp_dir = Path(config["run"]["temp_dir"])
    temp_dir.mkdir(parents=True, exist_ok=True)

    scenario = load_scenario(config)

    logger.info(
        "Stage 1: Random Search — method=%s samples_per_zone=%d",
        method, samples_per_zone,
    )

    # ── Expand zones → sample candidates ─────────────────────────────────────
    expanded = expand_zones(config)
    if not expanded:
        logger.warning("Stage 1: No enabled zones with combinations — nothing to sample")
        return

    if method == "lhs":
        candidates: List[CandidateParameterSet] = sample_lhs(
            expanded_space=expanded,
            n_per_zone=samples_per_zone,
            seed=run_metadata.random_search_seed,
        )
    else:
        candidates = sample_random(
            expanded_space=expanded,
            n_per_zone=samples_per_zone,
            seed=run_metadata.random_search_seed,
        )

    logger.info("Stage 1: %d candidates sampled across %d zones", len(candidates), len(expanded))

    # ── Evaluate each candidate ───────────────────────────────────────────────
    evaluated = 0
    passed = 0

    for candidate in candidates:
        # Evaluate strategy
        result: CandidateResult = evaluate(
            candidate=candidate,
            base_yaml_path=base_yaml_path,
            temp_dir=temp_dir,
            min_significant_trades=min_significant_trades,
            retain_temp_yamls=retain_temp,
        )

        # Evaluate fitness + constraints
        fitness_result = evaluate_fitness(result=result, scenario=scenario)

        # Build CandidateRecord and persist
        record = _build_candidate_record(
            candidate=candidate,
            fitness_result=fitness_result,
            run_id=run_id,
            stage=CandidateStage.RANDOM,
        )
        store.write_candidate(record)
        evaluated += 1

        if fitness_result.passed_constraints:
            passed += 1
            logger.debug(
                "Stage 1: candidate %s PASS  fitness=%.4f",
                candidate.candidate_id[:12], fitness_result.fitness_score,
            )
        else:
            logger.debug(
                "Stage 1: candidate %s FAIL  reason=%s",
                candidate.candidate_id[:12], fitness_result.rejection_reason,
            )

    store.flush()
    logger.info(
        "Stage 1: Random Search complete — evaluated=%d passed=%d failed=%d",
        evaluated, passed, evaluated - passed,
    )


# ── Stage 2: MC Pre-Filter ────────────────────────────────────────────────────

def _run_stage_2_mc_prefilter(
    config: dict, store: CandidateStore, run_metadata: RunMetadata
) -> None:
    """
    Stage 2: Monte Carlo Pre-Filter.

    Queries top-N RANDOM-stage constraint-passing candidates by fitness score,
    runs a cheap MC simulation on each (low iteration count, 2 perturbation
    types), and updates each candidate's stage to MC_PREFILTER_PASS or
    MC_PREFILTER_FAIL based on ruin_probability vs mc_prefilter_ruin_threshold.

    mc_prefilter_ruin_threshold is read from ScenarioProfile (not config dict)
    so scenario-specific thresholds are respected.

    B9F-003: CandidateResult is re-evaluated via strategy_runner.evaluate()
    instead of reconstructing from store. The store only persists aggregate
    metrics — trades and metrics objects are never persisted, so
    store.get_candidate_result() always returns trades=None / metrics=None,
    causing CandidateResult.is_valid=False and MC failure for every candidate.
    Re-evaluating is the correct fix: it produces a live CandidateResult with
    real trades, consistent with how WFO evaluates candidates in Stage 4.
    """
    from src.backtesting.monte_carlo.mc_engine import run_mc
    from src.backtesting.ranker import rank
    from src.backtesting.scenario import load_scenario
    from src.backtesting.strategy_runner import evaluate

    run_id = run_metadata.run_id
    prefilter_config = config.get("mc_prefilter", {})
    input_count: int = prefilter_config.get("input_count", 120)

    scenario = load_scenario(config)
    ruin_threshold: float = scenario.mc_prefilter_ruin_threshold

    base_yaml_path = _resolve_base_yaml(config)
    temp_dir = Path(config["run"]["temp_dir"])
    temp_dir.mkdir(parents=True, exist_ok=True)
    retain_temp: bool = config.get("run", {}).get("retain_temp_yamls", False)
    min_significant_trades: int = config.get("random_search", {}).get("min_significant_trades", 30)

    logger.info(
        "Stage 2: MC Pre-Filter — top %d candidates, ruin_threshold=%.2f",
        input_count, ruin_threshold,
    )

    # Query top-N RANDOM-pass candidates by fitness
    seed_records: List[CandidateRecord] = rank(
        store=store,
        run_id=run_id,
        stage=CandidateStage.RANDOM,
        top_n=input_count,
    )

    if not seed_records:
        logger.warning("Stage 2: No RANDOM-pass candidates available — skipping MC Pre-Filter")
        return

    passed = 0
    failed = 0

    for record in seed_records:
        candidate = _record_to_candidate_from_record(record)

        # ── B9F-003: Re-evaluate to get live CandidateResult with trades ─────
        # store.get_candidate_result() reconstructs from DB with trades=None —
        # CandidateResult.is_valid is always False on reconstructed objects.
        # Re-evaluating is the correct fix: consistent with WFO stage behaviour.
        candidate_result: CandidateResult = evaluate(
            candidate=candidate,
            base_yaml_path=base_yaml_path,
            temp_dir=temp_dir,
            min_significant_trades=min_significant_trades,
            retain_temp_yamls=retain_temp,
        )

        if not candidate_result.is_valid:
            logger.warning(
                "Stage 2: Re-evaluation failed for candidate %s (error: %s) — skipping MC",
                candidate.candidate_id[:12],
                candidate_result.error,
            )
            new_stage = CandidateStage.MC_PREFILTER_FAIL
            failed += 1
            updated_record = _build_candidate_record_from_existing(
                existing=record,
                run_id=run_id,
                new_stage=new_stage,
            )
            store.write_candidate(updated_record)
            continue
        # ── end B9F-003 ────────────────────────────────────────────────────────

        mc_result: MCResult = run_mc(
            candidate=candidate,
            candidate_result=candidate_result,
            mode=MCMode.PRE_FILTER,
            config=config,
            seed=run_metadata.mc_prefilter_seed,
            ruin_threshold=ruin_threshold,  # B8B-013: pass scenario value
        )
        store.write_mc_result(mc_result, run_id)

        # Determine pass/fail based on ruin probability
        if mc_result.error or mc_result.ruin_probability is None:
            new_stage = CandidateStage.MC_PREFILTER_FAIL
            logger.warning(
                "Stage 2: MC error for candidate %s: %s — marking FAIL",
                candidate.candidate_id[:12], mc_result.error,
            )
        elif mc_result.ruin_probability > ruin_threshold:
            new_stage = CandidateStage.MC_PREFILTER_FAIL
            logger.debug(
                "Stage 2: candidate %s FAIL  ruin=%.4f > threshold=%.2f",
                candidate.candidate_id[:12], mc_result.ruin_probability, ruin_threshold,
            )
        else:
            new_stage = CandidateStage.MC_PREFILTER_PASS
            logger.debug(
                "Stage 2: candidate %s PASS  ruin=%.4f <= threshold=%.2f",
                candidate.candidate_id[:12], mc_result.ruin_probability, ruin_threshold,
            )

        # Write updated stage record — reuses the fitness from the RANDOM evaluation
        updated_record = _build_candidate_record_from_existing(
            existing=record,
            run_id=run_id,
            new_stage=new_stage,
        )
        store.write_candidate(updated_record)

        if new_stage == CandidateStage.MC_PREFILTER_PASS:
            passed += 1
        else:
            failed += 1

    store.flush()
    logger.info(
        "Stage 2: MC Pre-Filter complete — pass=%d fail=%d total=%d",
        passed, failed, passed + failed,
    )
    
# ── Stage 3: Genetic Algorithm ────────────────────────────────────────────────

def _run_stage_3_ga(
    config: dict, store: CandidateStore, run_metadata: RunMetadata
) -> None:
    """
    Stage 3: Genetic Algorithm.

    Builds WFOWindow objects from config, injects the base YAML path as the
    private '_base_yaml_path' key (B9B-003 contract), and delegates to
    ga_engine.run_ga() for the full evolution loop.

    ga_engine writes all GA candidate evaluations to the store directly with
    stage=GA. No additional store writes needed here.

    B9F-002: Guard added — if Stage 2 produced no MC_PREFILTER_PASS candidates,
    logs a warning and returns early instead of crashing in initialise_population().
    Checkpoint is advanced to GA_COMPLETE by the caller regardless, so the
    pipeline continues to Stages 4–7.
    """
    from src.backtesting.ga.ga_engine import run_ga
    from src.backtesting.scenario import load_scenario
    from src.backtesting.ranker import rank
    from datetime import date

    run_id = run_metadata.run_id

    # ── B9F-002: Guard — skip GA gracefully if Stage 2 had no survivors ──────
    prefilter_pass = rank(
        store=store,
        run_id=run_id,
        stage=CandidateStage.MC_PREFILTER_PASS,
        top_n=1,
    )
    if not prefilter_pass:
        logger.warning(
            "Stage 3: No MC_PREFILTER_PASS candidates available — skipping GA"
        )
        return
    # ── end B9F-002 ────────────────────────────────────────────────────────────

    scenario = load_scenario(config)
    base_yaml_path = _resolve_base_yaml(config)

    # Build typed WFOWindow objects from YAML config dicts
    wfo_windows: List[WFOWindow] = [
        WFOWindow(
            window_id=w["id"],
            start_date=date.fromisoformat(str(w["start"])),
            end_date=date.fromisoformat(str(w["end"])),
        )
        for w in config["walk_forward"]["windows"]
    ]

    # B9B-003: ga_engine.run_ga() reads config['_base_yaml_path'] as a private
    # injected key. This key is NOT in backtest_template.yaml — it is the
    # orchestrator's responsibility to inject it here before calling run_ga().
    # A shallow copy prevents the injected key from leaking back to the caller.
    ga_config = dict(config)
    ga_config["_base_yaml_path"] = str(base_yaml_path)

    logger.info(
        "Stage 3: Genetic Algorithm — %d windows, seed=%d",
        len(wfo_windows), run_metadata.ga_seed,
    )

    run_ga(
        store=store,
        run_id=run_id,
        scenario=scenario,
        wfo_windows=wfo_windows,
        config=ga_config,
        seed=run_metadata.ga_seed,
    )

    store.flush()
    logger.info("Stage 3: Genetic Algorithm complete")

# ── Stage 4: Full WFO ─────────────────────────────────────────────────────────

# ── PATCH TARGET: src/backtesting/orchestrator.py ─────────────────────────────
#
# Replace the entire _run_stage_4_wfo function (currently a one-line stub):
#
#   def _run_stage_4_wfo(config, store, run_metadata) -> None:
#       logger.info("Stage 4: Full WFO — stub, not yet implemented")
#
# with the implementation below.
# No other changes to orchestrator.py are required.
# ──────────────────────────────────────────────────────────────────────────────


def _run_stage_4_wfo(
    config: dict,
    store: "CandidateStore",
    run_metadata: "RunMetadata",
) -> None:
    """
    Stage 4: Full Walk-Forward Optimisation.

    Per FUNCTIONAL_SPEC §7 and TECHNICAL_SPEC D-06:
      - Input: top M candidates from the combined RANDOM + GA pool, ranked by
        fitness score. M = walk_forward.input_count (default 30).
      - Evaluation: every candidate × every configured WFO window, via
        wfo_engine.run_wfo(mode="full").
      - Output: WFOConsistencyScore per candidate written to store by wfo_engine.
        Candidates that fail >50% of windows are flagged WFO_INSUFFICIENT_WINDOWS
        and excluded from Stages 5+.

    wfo_engine.run_wfo() in full mode:
      - Dispatches all candidate-window pairs to ProcessPoolExecutor workers.
      - Calls store.write_wfo_window_result() for every WFOWindowResult.
      - Calls store.write_wfo_consistency_score() for every candidate.
      - Calls store.flag_candidate_wfo_insufficient() for collapse cases.
    All store writes are handled by wfo_engine — no additional writes here.
    """
    from src.backtesting.wfo.wfo_engine import run_wfo
    from src.backtesting.ranker import rank_combined
    from src.backtesting.scenario import load_scenario
    from datetime import date

    run_id = run_metadata.run_id
    wfo_config = config.get("walk_forward", {})
    input_count: int = wfo_config.get("input_count", 30)
    oos_gate_enabled: bool = wfo_config.get("enforce_oos_gate", False)
    oos_degradation_threshold: float = wfo_config.get("oos_degradation_threshold", 0.50)
    max_workers: int = config.get("run", {}).get("max_workers", 6)
    min_significant_trades: int = config.get("random_search", {}).get("min_significant_trades", 30)

    base_yaml_path = _resolve_base_yaml(config)
    temp_dir = Path(config["run"]["temp_dir"])
    temp_dir.mkdir(parents=True, exist_ok=True)

    scenario = load_scenario(config)

    # Build typed WFOWindow objects from YAML config dicts
    wfo_windows: List[WFOWindow] = [
        WFOWindow(
            window_id=w["id"],
            start_date=date.fromisoformat(str(w["start"])),
            end_date=date.fromisoformat(str(w["end"])),
        )
        for w in config["walk_forward"]["windows"]
    ]

    logger.info(
        "Stage 4: Full WFO — top %d candidates (RANDOM+GA), %d windows, oos_gate=%s",
        input_count,
        len(wfo_windows),
        oos_gate_enabled,
    )

    # Pull top-M from combined RANDOM + GA pool by fitness score (FUNCTIONAL_SPEC §7)
    top_records: List[CandidateRecord] = rank_combined(
        store=store,
        run_id=run_id,
        stages=[CandidateStage.RANDOM, CandidateStage.MC_PREFILTER_PASS, CandidateStage.GA],
        top_n=input_count,
    )

    if not top_records:
        logger.warning("Stage 4: No candidates available for Full WFO — skipping")
        return

    # Reconstruct CandidateParameterSet for each record
    candidates: List[CandidateParameterSet] = [
        _record_to_candidate_from_record(r) for r in top_records
    ]

    logger.info("Stage 4: %d candidates selected for Full WFO", len(candidates))

    # Ensure all candidates have a stub row in the candidates table before
    # wfo_engine writes window results (FK constraint guard — same pattern as Stage 3)
    for candidate in candidates:
        store.write_candidate_stub(
            candidate=candidate,
            run_id=run_id,
            stage="WFO",
            generation=None,
        )
    store.flush()

    # wfo_engine full mode: evaluates all windows, writes all results to store
    consistency_scores = run_wfo(
        candidates=candidates,
        windows=wfo_windows,
        store=store,
        run_id=run_id,
        scenario=scenario,
        base_yaml_path=base_yaml_path,
        temp_dir=temp_dir,
        mode="full",
        max_workers=max_workers,
        min_significant_trades=min_significant_trades,
        oos_gate_enabled=oos_gate_enabled,
        oos_degradation_threshold=oos_degradation_threshold,
    )

    store.flush()
    logger.info(
        "Stage 4: Full WFO complete — %d/%d candidates scored",
        len(consistency_scores),
        len(candidates),
    )

# ── Stage 5: MC Deep ──────────────────────────────────────────────────────────

# ═══════════════════════════════════════════════════════════════════════════════
# PATCH 1 — src/backtesting/orchestrator.py
# Replace _run_stage_5_mc_deep entirely.
# Root cause: store.get_candidate_result() always returns trades=None/metrics=None
# (L-15 — known, documented). Stage 5 must re-evaluate like Stage 2 (B9F-003).
# ═══════════════════════════════════════════════════════════════════════════════

def _run_stage_5_mc_deep(
    config: dict,
    store: CandidateStore,
    run_metadata: RunMetadata,
) -> None:
    """
    Stage 5: Monte Carlo Deep simulation.

    Takes the top-N candidates by WFO consistency score, re-evaluates each
    via strategy_runner.evaluate() to get a live CandidateResult with real
    trades (store.get_candidate_result() always returns trades=None — L-15),
    runs a full MC Deep simulation on each, and writes each MCResult to store.

    B9G-002: Re-evaluate via strategy_runner.evaluate() instead of
    store.get_candidate_result(). Identical fix to B9F-003 (Stage 2).
    store.get_candidate_result() reconstructs from DB with trades=None /
    metrics=None — CandidateResult.is_valid is always False, causing
    run_mc() to raise "CandidateResult is invalid". Re-evaluating produces
    a live CandidateResult with real trades, consistent with Stage 2 and
    Stage 4 behaviour.

    On re-evaluation failure: writes MCResult(error=..., ruin_probability=None)
    and continues — ruin_probability=None → NO_GO in Stage 7 (correct).
    On MC failure: same — writes result with error, logs WARNING, continues.
    """
    from src.backtesting.monte_carlo.mc_engine import run_mc
    from src.backtesting.ranker import rank_by_wfo
    from src.backtesting.strategy_runner import evaluate

    run_id = run_metadata.run_id
    mc_config = config.get("monte_carlo", {}).get("deep", {})
    input_count: int = mc_config.get("input_count", 10)

    base_yaml_path = _resolve_base_yaml(config)
    temp_dir = Path(config["run"]["temp_dir"])
    temp_dir.mkdir(parents=True, exist_ok=True)
    retain_temp: bool = config.get("run", {}).get("retain_temp_yamls", False)
    min_significant_trades: int = config.get("random_search", {}).get("min_significant_trades", 30)

    logger.info("Stage 5: MC Deep — top %d candidates by WFO score", input_count)

    top_records: List[CandidateRecord] = rank_by_wfo(store, run_id, top_n=input_count)
    if not top_records:
        logger.warning("Stage 5: No candidates with WFO scores — skipping MC Deep")
        return

    processed = 0
    for record in top_records:
        candidate = _record_to_candidate_from_record(record)

        # ── B9G-002: Re-evaluate to get live CandidateResult with trades ──────
        # store.get_candidate_result() always returns trades=None (L-15).
        # run_mc() raises if CandidateResult.is_valid is False.
        # Re-evaluating is the correct fix — same pattern as B9F-003 (Stage 2).
        candidate_result: CandidateResult = evaluate(
            candidate=candidate,
            base_yaml_path=base_yaml_path,
            temp_dir=temp_dir,
            min_significant_trades=min_significant_trades,
            retain_temp_yamls=retain_temp,
        )

        if not candidate_result.is_valid:
            logger.warning(
                "Stage 5: Re-evaluation failed for candidate %s (error: %s) — MC will record NO_GO",
                candidate.candidate_id[:12],
                candidate_result.error,
            )
            # Still run_mc so it records MCResult(error=..., ruin_probability=None)
            # → NO_GO in Stage 7. Consistent with "never raises" MC contract.
        # ── end B9G-002 ────────────────────────────────────────────────────────

        mc_result: MCResult = run_mc(
            candidate=candidate,
            candidate_result=candidate_result,
            mode=MCMode.DEEP,
            config=config,
            seed=run_metadata.mc_deep_seed,
        )

        store.write_mc_result(mc_result, run_id)
        processed += 1

        if mc_result.error:
            logger.warning(
                "Stage 5: MC Deep failed for candidate %s: %s",
                candidate.candidate_id[:12], mc_result.error,
            )
        else:
            logger.debug(
                "Stage 5: candidate %s — ruin=%.4f  p5=%.2f",
                candidate.candidate_id[:12],
                mc_result.ruin_probability,
                mc_result.p5_final_equity or 0.0,
            )

    store.flush()
    logger.info("Stage 5: MC Deep complete — %d/%d candidates processed",
                processed, len(top_records))

# ── Stage 6: Parameter Sensitivity ───────────────────────────────────────────

def _run_stage_6_sensitivity(
    config: dict,
    store,
    run_metadata,
) -> None:
    """
    Stage 6: Parameter sensitivity analysis.

    Perturbs each parameter ±1 and ±2 steps (via ProcessPoolExecutor workers),
    evaluates fitness at each perturbation, and writes a SensitivityProfile
    per candidate. profile_complete=False auto-triggers the
    sensitivity_profile_incomplete modifier flag in Stage 7 verdict computation.

    OPT-01: A single ProcessPoolExecutor is opened for all candidates and passed
    to evaluate_sensitivity() via the pool= kwarg. The pool stays warm between
    candidates — spawn overhead is paid once instead of once per candidate.

    B9A-003 applied: spike_threshold now reads from
    scenario.verdict_sensitivity_spike_threshold instead of
    config["sensitivity"]["spike_threshold"]. The two sources must always be
    identical — reading from ScenarioProfile is the single source of truth.
    """
    from src.backtesting.evaluation.sensitivity import evaluate_sensitivity
    from src.backtesting.scenario import load_scenario
    from src.backtesting.contracts import SensitivityProfile
    from src.backtesting.ranker import rank_by_wfo

    run_id = run_metadata.run_id
    sens_config = config.get("sensitivity", {})
    input_count: int = sens_config.get("input_count", 5)
    max_steps: int = sens_config.get("max_steps", 2)
    max_workers: int = config.get("run", {}).get("max_workers", 6)
    min_significant_trades: int = config.get("random_search", {}).get("min_significant_trades", 30)

    base_yaml_path = _resolve_base_yaml(config)
    temp_dir = Path(config["run"]["temp_dir"])
    temp_dir.mkdir(parents=True, exist_ok=True)

    scenario = load_scenario(config)
    # B9A-003: read spike_threshold from ScenarioProfile — single source of truth.
    spike_threshold: float = scenario.verdict_sensitivity_spike_threshold
    parameter_space_def: dict = config.get("zones", {})

    logger.info(
        "Stage 6: Sensitivity — top %d candidates, spike_threshold=%.2f, max_steps=%d",
        input_count, spike_threshold, max_steps,
    )

    top_records: List[CandidateRecord] = rank_by_wfo(store, run_id, top_n=input_count)
    if not top_records:
        logger.warning("Stage 6: No candidates with WFO scores — skipping Sensitivity")
        return

    processed = 0

    # OPT-01: Open one shared pool for all candidates.
    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        for record in top_records:
            candidate = _record_to_candidate_from_record(record)
            baseline_fitness = store.get_fitness_score(candidate.candidate_id)

            if baseline_fitness is None:
                logger.warning(
                    "Stage 6: No baseline fitness for candidate %s — skipping",
                    candidate.candidate_id[:12],
                )
                continue

            sensitivity: SensitivityProfile = evaluate_sensitivity(
                candidate=candidate,
                baseline_fitness=baseline_fitness,
                parameter_space_def=parameter_space_def,
                base_yaml_path=base_yaml_path,
                temp_dir=temp_dir,
                scenario=scenario,
                spike_threshold=spike_threshold,
                max_steps=max_steps,
                max_workers=max_workers,
                min_significant_trades=min_significant_trades,
                pool=pool,   # OPT-01: shared pool — evaluate_sensitivity does NOT close it
            )

            store.write_sensitivity_profile(sensitivity, run_id)
            processed += 1

            logger.debug(
                "Stage 6: candidate %s — spike=%s  complete=%s  params_tested=%d",
                candidate.candidate_id[:12],
                sensitivity.spike_detected,
                sensitivity.profile_complete,
                len(sensitivity.parameter_sensitivities),
            )

    store.flush()
    logger.info("Stage 6: Sensitivity complete — %d/%d candidates processed",
                processed, len(top_records))


# ── Stage 7: Report & Output ──────────────────────────────────────────────────

def _run_stage_7_report(
    config: dict,
    store: CandidateStore,
    run_metadata: RunMetadata,
) -> None:
    """
    Stage 7: Verdict computation, trading YAML generation, and report output.

    For each top candidate (ranked by WFO score):
      1. Fetch WFO consistency score, MC deep result, and sensitivity profile
      2. Compute verdict via two-pillar logic + modifier flags
      3. Write VerdictResult to store
      4. For AUTO_GO and BORDERLINE: generate trading YAML with embedded metadata

    Then generate:
      - Self-contained HTML report (scenario-framed, inline charts)
      - Per-borderline adversarial checklist HTML
      - JSON per-candidate records
      - Parquet per-candidate records (if pandas available)

    B9A-001 applied: uses ranker.rank_by_wfo() (List[CandidateRecord], typed
    attribute access) instead of the former inline rank_by_wfo() (List[Dict],
    dict-key access).
    """
    from src.backtesting.ranker import rank_by_wfo

    run_id = run_metadata.run_id
    sens_config = config.get("sensitivity", {})
    input_count: int = sens_config.get("input_count", 5)
    oos_gate_enabled: bool = config.get("walk_forward", {}).get("enforce_oos_gate", False)

    output_dir = Path(config["run"]["output_dir"])
    base_yaml_path = _resolve_base_yaml(config)
    scenario: ScenarioProfile = load_scenario(config)
    formats: dict = config.get("output", {}).get("formats", {
        "html": True, "json": True, "parquet": True,
    })

    logger.info(
        "Stage 7: Report & Output — top %d candidates, oos_gate=%s",
        input_count, oos_gate_enabled,
    )

    top_records: List[CandidateRecord] = rank_by_wfo(store, run_id, top_n=input_count)
    if not top_records:
        logger.warning("Stage 7: No candidates available — generating empty report")

    # ── 7a: Verdicts ──────────────────────────────────────────────────────────
    verdicts_written = 0
    for record in top_records:
        candidate_id: str = record.candidate_id  # B9A-001: typed attribute access

        wfo_score: Optional[WFOConsistencyScore] = store.get_wfo_consistency_score(candidate_id)
        mc_result: Optional[MCResult] = store.get_mc_result(candidate_id, mode=MCMode.DEEP)
        sensitivity: Optional[SensitivityProfile] = store.get_sensitivity_profile(candidate_id)

        if wfo_score is None:
            logger.warning(
                "Stage 7: No WFO score for %s — cannot compute verdict", candidate_id[:12]
            )
            continue
        if mc_result is None:
            logger.warning(
                "Stage 7: No MC Deep result for %s — cannot compute verdict", candidate_id[:12]
            )
            continue
        if sensitivity is None:
            logger.warning(
                "Stage 7: No sensitivity profile for %s — using neutral profile",
                candidate_id[:12],
            )
            sensitivity = _neutral_sensitivity(candidate_id)

        verdict: VerdictResult = compute_verdict(
            candidate_id=candidate_id,
            wfo_score=wfo_score,
            mc_result=mc_result,
            sensitivity=sensitivity,
            scenario=scenario,
            oos_gate_enabled=oos_gate_enabled,
        )

        # ── 7b: Trading YAML for go/borderline ────────────────────────────────
        yaml_output_path: Optional[str] = None
        if verdict.verdict in (Verdict.AUTO_GO, Verdict.BORDERLINE):
            candidate = _record_to_candidate_from_record(record)
            out_path = build_output_path(output_dir, run_id, candidate_id)
            try:
                yaml_path = generate_trading_yaml(
                    candidate=candidate,
                    verdict=verdict,
                    run_metadata=run_metadata,
                    base_strategy_yaml_path=base_yaml_path,
                    output_path=out_path,
                )
                yaml_output_path = str(yaml_path)
                logger.info(
                    "Stage 7: Trading YAML written — candidate=%s  file=%s",
                    candidate_id[:12], yaml_path.name,
                )
            except Exception as exc:
                logger.error(
                    "Stage 7: Failed to write trading YAML for candidate %s: %s",
                    candidate_id[:12], exc,
                )

        # Rebuild VerdictResult with yaml_output_path populated (frozen dataclass)
        final_verdict = VerdictResult(
            candidate_id=verdict.candidate_id,
            scenario_name=verdict.scenario_name,
            verdict=verdict.verdict,
            deployment_status=verdict.deployment_status,
            wfo_consistency_score=verdict.wfo_consistency_score,
            mc_deep_ruin_probability=verdict.mc_deep_ruin_probability,
            sensitivity_spike=verdict.sensitivity_spike,
            oos_gate_triggered=verdict.oos_gate_triggered,
            window_collapse_flag=verdict.window_collapse_flag,
            sensitivity_profile_incomplete=verdict.sensitivity_profile_incomplete,
            median_oos_delta=verdict.median_oos_delta,
            parameter_region_width=verdict.parameter_region_width,
            yaml_output_path=yaml_output_path,
            evidence_summary=verdict.evidence_summary,
        )

        store.write_verdict(final_verdict, run_id)
        verdicts_written += 1

        logger.info(
            "Stage 7: verdict=%s  candidate=%s  WFO=%.3f  ruin=%s",
            final_verdict.verdict.value,
            candidate_id[:12],
            final_verdict.wfo_consistency_score or 0.0,
            f"{final_verdict.mc_deep_ruin_probability:.4f}"
            if final_verdict.mc_deep_ruin_probability is not None else "N/A",
        )

    store.flush()
    logger.info("Stage 7: %d verdicts written", verdicts_written)

    # ── 7c: HTML report + JSON/Parquet ────────────────────────────────────────
    generate_report(
        store=store,
        run_id=run_id,
        scenario=scenario,
        output_dir=output_dir,
        formats=formats,
    )

    logger.info("Stage 7: Report & Output complete — run_id=%s", run_id)


# ── Stage helper utilities ─────────────────────────────────────────────────────

def _record_to_candidate_from_record(record: CandidateRecord) -> CandidateParameterSet:
    """
    Reconstruct a CandidateParameterSet from a typed CandidateRecord.
    Parses parameters_json to restore the parameters dict.
    candidate_id is deterministic (SHA-256 of parameters dict) — reconstructing
    from the same parameters always yields the same ID as stored in the DB.
    """
    import json
    params = json.loads(record.parameters_json) if record.parameters_json else {}
    return CandidateParameterSet.create(
        zone_name=record.zone_name,
        parameters=params,
        generation=record.generation,
    )


def _record_to_candidate(record: Dict[str, Any]) -> CandidateParameterSet:
    """
    Reconstruct a CandidateParameterSet from a raw dict (legacy path).
    Retained for any code paths that still receive raw dicts.
    Supports both 'parameters' (dict) and 'parameters_json' (JSON string) keys.
    """
    import json

    zone_name: str = record.get("zone_name", "unknown")
    params = record.get("parameters")

    if not params:
        params_json = record.get("parameters_json", "{}")
        params = json.loads(params_json) if isinstance(params_json, str) else {}

    if isinstance(params, str):
        params = json.loads(params)

    return CandidateParameterSet.create(
        zone_name=zone_name,
        parameters=params,
        generation=record.get("generation"),
    )


def _build_candidate_record(
    candidate: CandidateParameterSet,
    fitness_result,
    run_id: str,
    stage: CandidateStage,
) -> CandidateRecord:
    """
    Build a CandidateRecord from a CandidateParameterSet + FitnessResult.
    Used in Stage 1 to persist new evaluations.
    """
    import json
    return CandidateRecord(
        run_id=run_id,
        candidate_id=candidate.candidate_id,
        zone_name=candidate.zone_name,
        stage=stage.value,
        generation=candidate.generation,
        recorded_at=datetime.now(UTC),
        parameters_json=json.dumps(candidate.parameters, sort_keys=True, default=str),
        fitness_score=fitness_result.fitness_score,
        passed_constraints=fitness_result.passed_constraints,
        rejection_reason=fitness_result.rejection_reason,
        failing_constraint=fitness_result.failing_constraint,
        failing_value=fitness_result.failing_value,
        actual_win_rate=fitness_result.actual_win_rate,
        actual_max_drawdown=fitness_result.actual_max_drawdown,
        actual_losing_streak=fitness_result.actual_losing_streak,
        actual_trades_per_week=fitness_result.actual_trades_per_week,
        actual_expectancy=fitness_result.actual_expectancy,
        actual_profit_factor=fitness_result.actual_profit_factor,
        # WFO fields not yet populated at Stage 1
        wfo_median_window_return=None,
        wfo_window_return_variance=None,
        wfo_worst_window_drawdown=None,
        wfo_fraction_positive_windows=None,
        wfo_consistency_score=None,
        wfo_windows_evaluated=None,
        wfo_oos_gate_triggered=None,
        wfo_window_collapse_flag=None,
        wfo_median_oos_delta=None,
        # MC fields not yet populated at Stage 1
        mc_prefilter_ruin_probability=None,
        mc_prefilter_avg_final_equity=None,
        mc_prefilter_iterations=None,
        mc_deep_ruin_probability=None,
        mc_deep_avg_final_equity=None,
        mc_deep_worst_drawdown=None,
        mc_deep_p5_final_equity=None,
        mc_deep_iterations=None,
        # Sensitivity / verdict not yet populated
        sensitivity_spike_detected=None,
        sensitivity_spike_parameters=None,
        sensitivity_profile_complete=None,
        verdict=None,
        deployment_status=None,
        evidence_summary=None,
    )


def _build_candidate_record_from_existing(
    existing: CandidateRecord,
    run_id: str,
    new_stage: CandidateStage,
) -> CandidateRecord:
    """
    Build a CandidateRecord for Stage 2 by cloning an existing RANDOM record
    with an updated stage. Preserves all fitness/constraint actuals from the
    original RANDOM evaluation — Stage 2 does not re-evaluate fitness.
    """
    return CandidateRecord(
        run_id=run_id,
        candidate_id=existing.candidate_id,
        zone_name=existing.zone_name,
        stage=new_stage.value,
        generation=existing.generation,
        recorded_at=datetime.now(UTC),
        parameters_json=existing.parameters_json,
        fitness_score=existing.fitness_score,
        passed_constraints=existing.passed_constraints,
        rejection_reason=existing.rejection_reason,
        failing_constraint=existing.failing_constraint,
        failing_value=existing.failing_value,
        actual_win_rate=existing.actual_win_rate,
        actual_max_drawdown=existing.actual_max_drawdown,
        actual_losing_streak=existing.actual_losing_streak,
        actual_trades_per_week=existing.actual_trades_per_week,
        actual_expectancy=existing.actual_expectancy,
        actual_profit_factor=existing.actual_profit_factor,
        wfo_median_window_return=None,
        wfo_window_return_variance=None,
        wfo_worst_window_drawdown=None,
        wfo_fraction_positive_windows=None,
        wfo_consistency_score=None,
        wfo_windows_evaluated=None,
        wfo_oos_gate_triggered=None,
        wfo_window_collapse_flag=None,
        wfo_median_oos_delta=None,
        mc_prefilter_ruin_probability=None,
        mc_prefilter_avg_final_equity=None,
        mc_prefilter_iterations=None,
        mc_deep_ruin_probability=None,
        mc_deep_avg_final_equity=None,
        mc_deep_worst_drawdown=None,
        mc_deep_p5_final_equity=None,
        mc_deep_iterations=None,
        sensitivity_spike_detected=None,
        sensitivity_spike_parameters=None,
        sensitivity_profile_complete=None,
        verdict=None,
        deployment_status=None,
        evidence_summary=None,
    )


def _neutral_sensitivity(candidate_id: str) -> SensitivityProfile:
    """
    Return a neutral SensitivityProfile for candidates where sensitivity
    evaluation was skipped. profile_complete=False → triggers the
    sensitivity_profile_incomplete modifier flag in the verdict engine.
    """
    return SensitivityProfile(
        candidate_id=candidate_id,
        baseline_fitness=0.0,
        parameter_sensitivities=(),
        spike_detected=False,
        spike_parameters=(),
        profile_complete=False,
    )


def _resolve_base_yaml(config: dict) -> Path:
    """
    Resolve the base strategy YAML path. Reads config['strategy']['base_yaml_path']
    if present; falls back to the default template location via paths.py.
    """
    from src.utils.paths import CONFIGS_DIR
    configured = config.get("strategy", {}).get("base_yaml_path")
    if configured:
        return Path(configured)
    return CONFIGS_DIR / "strategies" / "strategy_template.yaml"


# ── Internal utilities ─────────────────────────────────────────────────────────

def _require_keys(d: dict, keys: list) -> None:
    missing = [k for k in keys if k not in d]
    if missing:
        raise ValueError(f"Config missing required keys: {missing}")


def _extract_perturbation_profile(config: dict) -> str:
    """Extract the perturbation profile name from config. Defaults to 'default'."""
    return (
        config.get("mc_prefilter", {}).get("perturbation_profile")
        or config.get("monte_carlo", {}).get("deep", {}).get("perturbation_profile")
        or "default"
    )