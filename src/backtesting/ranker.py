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
    """
    all_candidates = store.query_candidates(
        run_id=run_id,
        limit=None,
        order_by="wcs.wfo_consistency_score DESC",
    )
    with_wfo = [r for r in all_candidates if r.wfo_consistency_score is not None]
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