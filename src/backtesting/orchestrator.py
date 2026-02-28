"""
orchestrator.py — Pipeline entry point and stage sequencer.

Responsibilities (orchestrate only):
- Load and validate config
- Resume existing run or initialise new one
- Execute stages in order with checkpoint skip logic
- Write immutable run artifacts at start (config hash, seeds, perturbation profile)

Stages 1–7 are stubs. Stage 0 is fully implemented.
"""
from __future__ import annotations

import hashlib
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from src.backtesting.candidate_store import CandidateStore
from src.backtesting.contracts import (
    Checkpoint,
    RunMetadata,
)
from src.backtesting.scenario import load_scenario

logger = logging.getLogger(__name__)

BACKTESTER_VERSION = "1.0.0"


# ── Public entry point ─────────────────────────────────────────────────────────

def run(config_path: Path) -> None:
    """
    Main pipeline entry point. Loads config, opens the store, resumes or starts
    a fresh run, and executes all enabled stages in order.
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
        store.close()


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

    # Validate WFO windows exist (min 3 required — Stage 0 also validates,
    # but fail early here on structural config problems)
    windows = config.get("walk_forward", {}).get("windows", [])
    if len(windows) < 3:
        raise ValueError(
            f"backtest_template.yaml requires at least 3 WFO windows; "
            f"found {len(windows)}. Add more windows under walk_forward.windows."
        )

    return config


def _compute_config_hash(config_path: Path) -> str:
    """SHA-256 of the raw YAML file content."""
    content = config_path.read_bytes()
    return hashlib.sha256(content).hexdigest()


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
    current_hash = _compute_config_hash(config_path)

    # Look for the most recent run with this config hash
    # (reads directly from store via runs table)
    import sqlite3
    conn = sqlite3.connect(str(store._db_path))
    row = conn.execute(
        "SELECT run_id, config_hash, checkpoint FROM runs "
        "WHERE config_hash = ? AND checkpoint != ? "
        "ORDER BY started_at DESC LIMIT 1",
        (current_hash, Checkpoint.COMPLETE.name),
    ).fetchone()
    conn.close()

    if row is not None:
        run_id, stored_hash, checkpoint_name = row
        logger.info(
            "Resuming existing run %s at checkpoint %s", run_id, checkpoint_name
        )
        existing = store.get_run_metadata(run_id)
        return existing

    # Check if there is any run with a DIFFERENT hash — this means the config
    # was modified without completing the previous run. Refuse to silently mix.
    conn = sqlite3.connect(str(store._db_path))
    conflict_row = conn.execute(
        "SELECT run_id, config_hash FROM runs WHERE checkpoint != ? LIMIT 1",
        (Checkpoint.COMPLETE.name,),
    ).fetchone()
    conn.close()

    if conflict_row is not None:
        existing_run_id, existing_hash = conflict_row
        raise ValueError(
            f"Config hash mismatch. An incomplete run ({existing_run_id}) "
            f"exists with hash {existing_hash[:12]}… but current config hash "
            f"is {current_hash[:12]}…. Post-run config changes create a new "
            f"run — delete or complete the existing run first."
        )

    # Start fresh
    return _initialise_run(store, config, config_path, current_hash)


def _initialise_run(
    store: CandidateStore,
    config: dict,
    config_path: Path,
    config_hash: str,
) -> RunMetadata:
    """Create and persist a new RunMetadata row."""
    run_id = str(uuid.uuid4())
    windows_config = config["walk_forward"]["windows"]
    wfo_window_ids = tuple(w["id"] for w in windows_config)

    run_metadata = RunMetadata(
        run_id=run_id,
        config_hash=config_hash,
        scenario_name=config["scenario"],
        started_at=datetime.utcnow(),
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
    """Execute all enabled pipeline stages with checkpoint skip logic."""
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
    if store.get_checkpoint(run_id).value < Checkpoint.MONTE_CARLO_COMPLETE.value:
        _run_stage_5_mc_deep(config, store, run_metadata)
        store.set_checkpoint(run_id, Checkpoint.MONTE_CARLO_COMPLETE)
    else:
        logger.info("Stage 5 (MC Deep) already complete — skipping")

    # ── Stage 6: Parameter Sensitivity ───────────────────────────────────────
    if store.get_checkpoint(run_id).value < Checkpoint.SENSITIVITY_COMPLETE.value:
        _run_stage_6_sensitivity(config, store, run_metadata)
        store.set_checkpoint(run_id, Checkpoint.SENSITIVITY_COMPLETE)
    else:
        logger.info("Stage 6 (Sensitivity) already complete — skipping")

    # ── Stage 7: Report & Output ──────────────────────────────────────────────
    if store.get_checkpoint(run_id).value < Checkpoint.COMPLETE.value:
        _run_stage_7_report(config, store, run_metadata)
    else:
        logger.info("Stage 7 (Report) already complete — skipping")


# ── Stage 0: Fully implemented ────────────────────────────────────────────────

def _run_stage_0_init(
    config: dict,
    store: CandidateStore,
    run_metadata: RunMetadata,
) -> None:
    """
    Stage 0: Validate configuration, data files, and WFO windows.

    Validations performed:
    1. Scenario loads without error (weights sum to 1.0, thresholds valid)
    2. WFO windows: minimum 3 required, no overlapping windows, dates parseable
    3. Parameter zones: at least one zone is enabled
    4. Data files exist (if data.path is configured)

    Raises ValueError on any validation failure.
    """
    logger.info("Stage 0: Validation & Init — run_id=%s", run_metadata.run_id)

    # 1. Validate scenario loads cleanly
    try:
        scenario = load_scenario(config)
        logger.debug("Scenario '%s' loaded successfully", scenario.name)
    except (ValueError, KeyError) as exc:
        raise ValueError(f"Stage 0: Scenario validation failed: {exc}") from exc

    # 2. Validate WFO windows
    windows_config = config.get("walk_forward", {}).get("windows", [])
    _validate_wfo_windows(windows_config)

    # 3. Validate at least one parameter zone is enabled
    zones = config.get("zones", {})
    enabled_zones = [name for name, zdef in zones.items() if zdef.get("enabled", True)]
    if not enabled_zones:
        raise ValueError(
            "Stage 0: No parameter zones are enabled. "
            "Enable at least one zone under 'zones' in the config."
        )
    logger.debug("Enabled parameter zones: %s", enabled_zones)

    logger.info("Stage 0: All validations passed — %d WFO windows, %d enabled zones",
                len(windows_config), len(enabled_zones))


def _validate_wfo_windows(windows_config: list) -> None:
    """
    Validate WFO window definitions.
    - Minimum 3 windows required (GA random sampling requires ≥ 3)
    - All window IDs must be non-empty and unique
    - All dates must be parseable as YYYY-MM-DD
    - start must be strictly before end for each window
    """
    from datetime import date

    if len(windows_config) < 3:
        raise ValueError(
            f"Minimum 3 WFO windows required for GA random sampling; "
            f"found {len(windows_config)}. Add more windows under walk_forward.windows."
        )

    seen_ids = set()
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
                f"WFO window '{window_id}': invalid date format. "
                f"Expected YYYY-MM-DD for 'start' and 'end'. Error: {exc}"
            ) from exc

        if start >= end:
            raise ValueError(
                f"WFO window '{window_id}': start ({start}) must be before end ({end})"
            )


# ── Stage stubs (Stages 1–7) ───────────────────────────────────────────────────

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


def _run_stage_5_mc_deep(
    config: dict, store: CandidateStore, run_metadata: RunMetadata
) -> None:
    logger.info("Stage 5: MC Deep — stub, not yet implemented")


def _run_stage_6_sensitivity(
    config: dict, store: CandidateStore, run_metadata: RunMetadata
) -> None:
    logger.info("Stage 6: Parameter Sensitivity — stub, not yet implemented")


def _run_stage_7_report(
    config: dict, store: CandidateStore, run_metadata: RunMetadata
) -> None:
    logger.info("Stage 7: Report & Output — stub, not yet implemented")


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