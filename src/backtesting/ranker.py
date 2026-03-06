"""
ranker.py — Stateless ranking queries against CandidateStore.

All functions are pure queries: they receive a store and query spec, return
a ranked list of CandidateRecord objects. No writes. No state.
"""
from __future__ import annotations

from typing import List

from src.backtesting.candidate_store import CandidateStore
from src.backtesting.contracts import CandidateRecord, CandidateStage


def rank(
    store: CandidateStore,
    run_id: str,
    stage: CandidateStage,
    top_n: int,
) -> List[CandidateRecord]:
    """
    Return the top N candidates from the given stage, ranked by fitness_score
    descending. Only candidates that passed constraints are returned.
    """
    all_candidates = store.query_candidates(
        run_id=run_id,
        stage=stage,
        limit=None,
        order_by="e.fitness_score DESC",
    )
    passed = [r for r in all_candidates if r.passed_constraints is True]
    return passed[:top_n]


def rank_by_wfo(
    store: CandidateStore,
    run_id: str,
    top_n: int,
) -> List[CandidateRecord]:
    """
    Rank by wfo_consistency_score descending.
    Used as input for MC Deep (Stage 5) and Sensitivity (Stage 6).
    Returns up to top_n records that have a WFO consistency score.

    B9G-003: Deduplicate by candidate_id before applying top_n limit.
    query_candidates joins evaluations — candidates evaluated in multiple
    stages (e.g. RANDOM + MC_PREFILTER_PASS) produce multiple rows with
    the same candidate_id. Without deduplication, the same candidate
    appears multiple times in Stage 5/6/7, producing duplicate MC results,
    sensitivity profiles, and verdict writes.
    Deduplication keeps the first occurrence (highest wfo_consistency_score
    after ORDER BY) and discards subsequent duplicates.
    """
    all_candidates = store.query_candidates(
        run_id=run_id,
        limit=None,
        order_by="wcs.wfo_consistency_score DESC",
    )

    # Deduplicate by candidate_id — keep first (highest score) occurrence
    seen_ids: set = set()
    with_wfo: List[CandidateRecord] = []
    for r in all_candidates:
        if r.wfo_consistency_score is not None and r.candidate_id not in seen_ids:
            seen_ids.add(r.candidate_id)
            with_wfo.append(r)

    return with_wfo[:top_n]

def rank_combined(
    store: CandidateStore,
    run_id: str,
    stages: List[CandidateStage],
    top_n: int,
) -> List[CandidateRecord]:
    """
    Return the top N candidates from a combined pool of multiple stages,
    ranked by fitness_score descending. Deduplicates by candidate_id.

    Used for Stage 4 (Full WFO) input: combined Random + GA pool.
    """
    seen_ids: set = set()
    combined: List[CandidateRecord] = []

    for stage in stages:
        candidates = store.query_candidates(
            run_id=run_id,
            stage=stage,
            limit=None,
            order_by="e.fitness_score DESC",
        )
        for r in candidates:
            if r.candidate_id not in seen_ids and r.passed_constraints is True:
                seen_ids.add(r.candidate_id)
                combined.append(r)

    # Re-sort the combined deduplicated list by fitness descending
    combined.sort(key=lambda r: r.fitness_score or 0.0, reverse=True)
    return combined[:top_n]