"""
wfo/wfo_engine.py
-----------------
Orchestrates WFO evaluation for a list of candidates across WFO windows.

Two modes:
  "lightweight" — used by GA engine: evaluates on a pre-selected subset of windows
                  (the 2 randomly sampled for this generation). Returns WFOConsistencyScore
                  per candidate for GA fitness computation.
  "full"        — Stage 4: evaluates on ALL configured windows. Returns WFOConsistencyScore
                  per candidate for the mandatory WFO pillar evidence.

Single responsibility: dispatch candidate-window pairs to wfo_evaluator workers,
collect results, invoke consistency_scorer, write to store.

Parallelism: ProcessPoolExecutor (Windows spawn-safe). Each worker call is
`evaluate_window` from wfo_evaluator.py — it never raises.
"""
from __future__ import annotations

import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional

from src.backtesting.contracts import (
    CandidateParameterSet,
    CandidateStage,
    ScenarioProfile,
    WFOConsistencyScore,
    WFOWindow,
    WFOWindowResult,
)
from src.backtesting.candidate_store import CandidateStore
from src.backtesting.wfo.wfo_evaluator import evaluate_window
from src.backtesting.wfo.consistency_scorer import compute_consistency

logger = logging.getLogger(__name__)


def run_wfo(
    candidates: List[CandidateParameterSet],
    windows: List[WFOWindow],
    store: CandidateStore,
    run_id: str,
    scenario: ScenarioProfile,
    base_yaml_path: Path,
    temp_dir: Path,
    mode: str,
    max_workers: int = 6,
    min_significant_trades: int = 30,
    oos_gate_enabled: bool = False,
    oos_degradation_threshold: float = 0.50,
) -> Dict[str, WFOConsistencyScore]:
    """
    Evaluate all candidates on the given windows and return consistency scores.

    Args:
        candidates:               Candidates to evaluate.
        windows:                  Windows to evaluate on (pre-filtered for lightweight mode).
        store:                    CandidateStore for writing WFOWindowResult rows.
        run_id:                   Current run ID.
        scenario:                 Active scenario profile.
        base_yaml_path:           Base strategy YAML path.
        temp_dir:                 Temp directory for candidate YAMLs.
        mode:                     "full" or "lightweight".
        max_workers:              ProcessPoolExecutor worker count.
        min_significant_trades:   Significance guard (passed to evaluator).
        oos_gate_enabled:         Whether IS/OOS gate check is active.
        oos_degradation_threshold: IS/OOS degradation threshold.

    Returns:
        Dict mapping candidate_id → WFOConsistencyScore.
    """
    if mode not in ("full", "lightweight"):
        raise ValueError(f"WFO mode must be 'full' or 'lightweight'; got '{mode}'")

    windows_total = len(windows)
    logger.info(
        "WFO run starting: mode=%s candidates=%d windows=%d",
        mode,
        len(candidates),
        windows_total,
    )

    # Build all (candidate, window) pairs for parallel dispatch
    tasks: List[tuple] = [
        (candidate, window)
        for candidate in candidates
        for window in windows
    ]

    # Collect results grouped by candidate_id
    results_by_candidate: Dict[str, List[WFOWindowResult]] = {
        c.candidate_id: [] for c in candidates
    }

    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        future_to_task = {
            pool.submit(
                evaluate_window,
                candidate,
                window,
                base_yaml_path,
                temp_dir,
                scenario,
                min_significant_trades,
            ): (candidate.candidate_id, window.window_id)
            for candidate, window in tasks
        }

        for future in as_completed(future_to_task):
            cid, wid = future_to_task[future]
            try:
                window_result: WFOWindowResult = future.result()
            except Exception as exc:
                # Evaluator should never raise, but guard defensively
                logger.error(
                    "Unexpected exception from WFO worker: candidate=%s window=%s error=%s",
                    cid[:12],
                    wid,
                    exc,
                    exc_info=True,
                )
                continue

            results_by_candidate[cid].append(window_result)

            # Write window result to store for audit / SQL queryability
            store.write_wfo_window_result(window_result, run_id)

    # Compute consistency scores for each candidate
    consistency_scores: Dict[str, WFOConsistencyScore] = {}

    for candidate in candidates:
        cid = candidate.candidate_id
        window_results = results_by_candidate.get(cid, [])

        consistency = compute_consistency(
            window_results=window_results,
            windows_total=windows_total,
            scenario=scenario,
            oos_gate_enabled=oos_gate_enabled,
            oos_degradation_threshold=oos_degradation_threshold,
        )
        consistency_scores[cid] = consistency

        # In full mode, check for WFO_INSUFFICIENT_WINDOWS
        if mode == "full":
            _check_window_sufficiency(consistency, candidate, store, run_id)
            store.write_wfo_consistency_score(consistency, run_id)

    logger.info(
        "WFO run complete: mode=%s scored=%d candidates",
        mode,
        len(consistency_scores),
    )
    return consistency_scores


# ── Private helpers ────────────────────────────────────────────────────────────

def _check_window_sufficiency(
    consistency: WFOConsistencyScore,
    candidate: CandidateParameterSet,
    store: CandidateStore,
    run_id: str,
) -> None:
    """
    Flag candidates that failed on more than half their windows as
    WFO_INSUFFICIENT_WINDOWS. These are excluded from Stage 5+.
    """
    if consistency.windows_total == 0:
        return
    fail_fraction = 1.0 - (consistency.windows_evaluated / consistency.windows_total)
    if fail_fraction > 0.50:
        logger.warning(
            "Candidate %s failed >50%% of WFO windows (%d/%d valid) — flagging WFO_INSUFFICIENT_WINDOWS",
            candidate.candidate_id[:12],
            consistency.windows_evaluated,
            consistency.windows_total,
        )
        store.flag_candidate_wfo_insufficient(candidate.candidate_id, run_id)