"""
orchestrator.py — Pipeline entry point and stage sequencer.

Responsibilities (orchestrate only):
- Load and validate config
- Resume existing run or initialise new one
- Execute stages in order with checkpoint skip logic
- Write immutable run artifacts at start (config hash, seeds, perturbation profile)

Stages 1–4 remain stubs pending their respective phase implementations.
Stages 0, 5, 6, and 7 are fully implemented.

Block 9A fixes applied:
  B9A-002: Stage 1 stub now advances checkpoint to RANDOM_SEARCH_COMPLETE.
           Previously the checkpoint was never advanced, causing Stage 1 to
           re-run on every pipeline resume.
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
    CandidateResult,
    Checkpoint,
    MCMode,
    MCResult,
    RunMetadata,
    ScenarioProfile,
    SensitivityProfile,
    Verdict,
    VerdictResult,
    WFOConsistencyScore,
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
    """
    import sqlite3

    current_hash = _compute_config_hash(config_path)

    conn = sqlite3.connect(str(store._db_path))
    try:
        row = conn.execute(
            "SELECT run_id FROM runs WHERE config_hash = ? AND checkpoint != ? "
            "ORDER BY started_at DESC LIMIT 1",
            (current_hash, Checkpoint.COMPLETE.name),
        ).fetchone()

        if row is not None:
            run_id = row[0]
            logger.info("Resuming existing run %s", run_id)
            return store.get_run_metadata(run_id)

        conflict = conn.execute(
            "SELECT run_id, config_hash FROM runs WHERE checkpoint != ? LIMIT 1",
            (Checkpoint.COMPLETE.name,),
        ).fetchone()
    finally:
        conn.close()

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
    # NOTE (B8-007): Stages 1–4 are stubs — they advance checkpoints without
    # producing output. Stages 5–7 will consume data from a prior run loaded
    # into the DB. This is a known temporary state. See OPERATOR_RUNBOOK §3.
    if store.get_checkpoint(run_id).value < Checkpoint.RANDOM_SEARCH_COMPLETE.value:
        _run_stage_1_random_search(config, store, run_metadata)
        store.set_checkpoint(run_id, Checkpoint.RANDOM_SEARCH_COMPLETE)  # B9A-002
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
    # NOTE (B8-008): Timing covers stages 5–7 only. Stages 1–4 are stubs and
    # contribute negligible elapsed time. When stubs are replaced, move
    # _t_total to the top of this function and sum all stage durations.
    logger.info(
        "TIMING stage_5_mc_deep elapsed=%.1fs", _elapsed_5,
    )
    logger.info(
        "TIMING stage_6_sensitivity elapsed=%.1fs", _elapsed_6,
    )
    logger.info(
        "TIMING stage_7_report elapsed=%.1fs", _elapsed_7,
    )
    logger.info(
        "TIMING SUMMARY  stage5=%.1fs  stage6=%.1fs  stage7=%.1fs  total=%.1fs  budget=%.0fs  %s",
        _elapsed_5,
        _elapsed_6,
        _elapsed_7,
        _elapsed_total,
        _budget,
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
    import logging
    logger = logging.getLogger(__name__)
    logger.info("Stage 0: Validation & Init — run_id=%s", run_metadata.run_id)

    from src.backtesting.scenario import load_scenario
    try:
        scenario = load_scenario(config)
        logger.debug("Scenario '%s' loaded successfully", scenario.name)
    except (ValueError, KeyError) as exc:
        raise ValueError(f"Stage 0: Scenario validation failed: {exc}") from exc

    _validate_wfo_windows(config.get("walk_forward", {}).get("windows", []))

    # M-05: Validate that all enabled zone parameter names exist in _PARAM_KEY_MAP.
    # Catches typos and unsupported parameter names before any evaluation begins.
    _validate_parameter_names(config)

    # B8-005: Validate min_significant_trades >= 1. A value of 0 disables the
    # significance guard in strategy_runner.evaluate() silently, allowing zero-trade
    # candidates with undefined metrics to enter the pipeline.
    min_significant_trades = config.get("random_search", {}).get("min_significant_trades", 30)
    if min_significant_trades < 1:
        raise ValueError(
            f"Stage 0: random_search.min_significant_trades must be >= 1; "
            f"got {min_significant_trades}. A value of 0 disables the significance guard."
        )

    # NOTE (B9A-003, B9A-005): spike_threshold is currently validated here from the
    # config dict. Once B9A-003 is applied (Stage 6 reads spike_threshold from
    # ScenarioProfile.verdict_sensitivity_spike_threshold instead), this validation
    # becomes redundant — ScenarioProfile.__post_init__ owns the guard.
    # Remove this block when B9A-003 is applied.
    spike_threshold_val = config.get("sensitivity", {}).get("spike_threshold", 0.15)
    if not (0.0 < spike_threshold_val < 1.0):
        raise ValueError(
            f"Stage 0: sensitivity.spike_threshold must be in (0, 1); "
            f"got {spike_threshold_val}."
        )

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
    test assertions) if any are found. Only enabled zones are checked — disabled zones
    may contain experimental parameters that are not yet mapped.

    Called from _run_stage_0_init (M-05 audit remediation).
    """
    import logging
    from src.backtesting.strategy_runner import _PARAM_KEY_MAP
    logger = logging.getLogger(__name__)

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

# ── Stages 1–4: Stubs ─────────────────────────────────────────────────────────

def _run_stage_1_random_search(
    config: dict, store: CandidateStore, run_metadata: RunMetadata
) -> None:
    logger.info("Stage 1: Random Search — stub, not yet implemented")


def _run_stage_2_mc_prefilter(
    config: dict, store: CandidateStore, run_metadata: RunMetadata
) -> None:
    logger.info("Stage 2: MC Pre-Filter — stub, not yet implemented")


def _run_stage_3_ga(
    config: dict, store: CandidateStore, run_metadata: RunMetadata
) -> None:
    logger.info("Stage 3: Genetic Algorithm — stub, not yet implemented")


def _run_stage_4_wfo(
    config: dict, store: CandidateStore, run_metadata: RunMetadata
) -> None:
    logger.info("Stage 4: Full WFO — stub, not yet implemented")


# ── Stage 5: MC Deep ──────────────────────────────────────────────────────────

def _run_stage_5_mc_deep(
    config: dict,
    store: CandidateStore,
    run_metadata: RunMetadata,
) -> None:
    """
    Stage 5: Monte Carlo Deep simulation.

    Takes the top-N candidates by WFO consistency score, runs a full MC Deep
    simulation on each (all perturbation types, configured iteration count),
    and writes each MCResult to the store.

    On MC failure (result.error set): writes the result anyway (ruin_probability=None)
    and logs a warning. The None ruin_probability path produces NO_GO in Stage 7.
    """
    from src.backtesting.monte_carlo.mc_engine import run_mc

    run_id = run_metadata.run_id
    mc_config = config.get("monte_carlo", {}).get("deep", {})
    input_count: int = mc_config.get("input_count", 10)

    logger.info("Stage 5: MC Deep — top %d candidates by WFO score", input_count)

    top_records = store.rank_by_wfo(run_id, top_n=input_count)
    if not top_records:
        logger.warning("Stage 5: No candidates with WFO scores — skipping MC Deep")
        return

    processed = 0
    for record in top_records:
        candidate = _record_to_candidate(record)
        candidate_result = store.get_candidate_result(candidate.candidate_id)

        if candidate_result is None:
            logger.warning(
                "Stage 5: No full-dataset result for candidate %s — skipping",
                candidate.candidate_id[:12],
            )
            continue

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

    NOTE (B9A-003): spike_threshold is currently read from config dict
    (sensitivity.spike_threshold). It should instead be read from
    scenario.verdict_sensitivity_spike_threshold so that the detection threshold
    and the verdict flagging threshold are always identical. Deferred to Block 9B.
    When fixed, also remove the Stage 0 spike_threshold validation (B9A-005).

    NOTE (B9A-004): load_scenario() is called here rather than receiving the
    already-loaded ScenarioProfile as a parameter. Minor SRP friction — deferred.
    """
    from src.backtesting.evaluation.sensitivity import evaluate_sensitivity
    from src.backtesting.scenario import load_scenario
    from src.backtesting.contracts import SensitivityProfile

    run_id = run_metadata.run_id
    sens_config = config.get("sensitivity", {})
    input_count: int = sens_config.get("input_count", 5)
    spike_threshold: float = sens_config.get("spike_threshold", 0.15)  # B9A-003: see NOTE above
    max_steps: int = sens_config.get("max_steps", 2)
    max_workers: int = config.get("run", {}).get("max_workers", 6)
    min_significant_trades: int = config.get("random_search", {}).get("min_significant_trades", 30)

    from src.utils.paths import CONFIGS_DIR
    base_yaml_path = _resolve_base_yaml(config)
    temp_dir = Path(config["run"]["temp_dir"])
    temp_dir.mkdir(parents=True, exist_ok=True)

    scenario = load_scenario(config)  # B9A-004: should be passed as parameter
    parameter_space_def: dict = config.get("zones", {})

    logger.info(
        "Stage 6: Sensitivity — top %d candidates, spike_threshold=%.2f, max_steps=%d",
        input_count, spike_threshold, max_steps,
    )

    top_records = store.rank_by_wfo(run_id, top_n=input_count)
    if not top_records:
        logger.warning("Stage 6: No candidates with WFO scores — skipping Sensitivity")
        return

    processed = 0

    # OPT-01: Open one shared pool for all candidates.
    # Pool stays warm across the loop — spawn cost paid once, not per candidate.
    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        for record in top_records:
            candidate = _record_to_candidate(record)
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

    NOTE (B9A-001): rank_by_wfo() returns List[Dict] — raw dicts crossing the
    store↔orchestrator boundary. record['candidate_id'] is untyped dict-key access.
    Fix: rank_by_wfo() should return List[CandidateRecord]. Deferred to Block 9B.
    """
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

    top_records = store.rank_by_wfo(run_id, top_n=input_count)
    if not top_records:
        logger.warning("Stage 7: No candidates available — generating empty report")

    # ── 7a: Verdicts ──────────────────────────────────────────────────────────
    verdicts_written = 0
    for record in top_records:
        candidate_id: str = record["candidate_id"]  # B9A-001: dict access — see NOTE above

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
            # Sensitivity may be missing if the candidate was excluded at Stage 6.
            # profile_complete=False → sensitivity_profile_incomplete modifier flag.
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
            candidate = _record_to_candidate(record)
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

def _record_to_candidate(record: Dict[str, Any]) -> CandidateParameterSet:
    """
    Reconstruct a CandidateParameterSet from a rank_by_wfo record dict.
    Supports both 'parameters' (dict) and 'parameters_json' (JSON string) keys.

    NOTE: candidate_id is deterministic (SHA-256 of parameters dict) — reconstructing
    from the same parameters always yields the same ID as stored in the DB.
    No candidate_id field needs to be passed explicitly.
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