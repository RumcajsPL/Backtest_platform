"""
tests/backtesting/unit/test_h02_wfo_window_writes.py
-----------------------------------------------------
H-02 regression tests — verify write_wfo_window_result and
flag_candidate_wfo_insufficient are present and functional.

Two tests:
  H02-01  write_wfo_window_result persists a WFOWindowResult row that is
          readable via query_wfo_window_results.
  H02-02  flag_candidate_wfo_insufficient writes a sentinel
          wfo_consistency_scores row (windows_evaluated=0,
          window_collapse_flag=1) and is idempotent on a second call.
"""
from __future__ import annotations

import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest

# ── sys.path anchor (must precede all project imports) ────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.utils.paths import PROJECT_ROOT  # noqa: E402
from src.backtesting.contracts import (   # noqa: E402
    Checkpoint,
    RunMetadata,
    WFOWindowResult,
)
from src.backtesting.candidate_store import CandidateStore  # noqa: E402


# ── Shared fixtures ────────────────────────────────────────────────────────────

def _make_run_metadata(run_id: str) -> RunMetadata:
    return RunMetadata(
        run_id=run_id,
        config_hash="a" * 64,
        scenario_name="e2e_test",
        started_at=datetime.now(UTC),
        perturbation_profile_name="default",
        random_search_seed=42,
        ga_seed=43,
        mc_prefilter_seed=44,
        mc_deep_seed=45,
        sensitivity_seed=46,
        wfo_window_ids=("w1", "w2", "w3"),
        checkpoint=Checkpoint.NOT_STARTED,
        backtester_version="1.0.0",
    )


def _make_window_result(candidate_id: str, window_id: str) -> WFOWindowResult:
    return WFOWindowResult(
        candidate_id=candidate_id,
        window_id=window_id,
        evaluated_at=datetime.now(UTC),
        fitness_score=0.72,
        total_trades=45,
        net_pnl=1250.0,
        max_drawdown=-320.0,
        win_rate=58.0,
        expectancy=2.5,
        profit_factor=1.8,
        oos_delta=0.05,
        error=None,
    )


# ── Tests ──────────────────────────────────────────────────────────────────────

class TestH02WFOWindowWrites:

    def test_h02_01_write_wfo_window_result_persists_row(self, tmp_path):
        """
        H02-01: write_wfo_window_result enqueues a WFOWindowResult write.
        After flush(), the row is readable via query_wfo_window_results with
        correct candidate_id, window_id, fitness_score, and oos_delta.
        """
        db_path = tmp_path / "h02_01.db"
        store = CandidateStore(db_path)
        run_id = str(uuid.uuid4())
        candidate_id = str(uuid.uuid4())

        try:
            store.initialise_run(_make_run_metadata(run_id))

            # Seed the candidates table so the FK constraint is satisfied
            store._conn.execute(
                "INSERT INTO candidates (candidate_id, run_id, zone_name, origin_stage, created_at) "
                "VALUES (?, ?, 'safe', 'RANDOM', ?)",
                (candidate_id, run_id, datetime.now(UTC).isoformat()),
            )
            store._conn.commit()

            result = _make_window_result(candidate_id, "w1")
            store.write_wfo_window_result(result, run_id)
            store.flush()

            rows = store.query_wfo_window_results(candidate_id)

            assert len(rows) == 1, (
                f"H02-01 FAIL: Expected 1 wfo_window_results row, got {len(rows)}"
            )
            row = rows[0]
            assert row["window_id"] == "w1", (
                f"H02-01 FAIL: window_id mismatch: {row['window_id']!r}"
            )
            assert abs(row["fitness_score"] - 0.72) < 1e-6, (
                f"H02-01 FAIL: fitness_score mismatch: {row['fitness_score']}"
            )
            assert abs(row["oos_delta"] - 0.05) < 1e-6, (
                f"H02-01 FAIL: oos_delta mismatch: {row['oos_delta']}"
            )
            assert row["evaluation_error"] is None, (
                f"H02-01 FAIL: unexpected error: {row['evaluation_error']}"
            )

        finally:
            store.close()

    def test_h02_02_flag_candidate_wfo_insufficient_sentinel_and_idempotent(self, tmp_path):
        """
        H02-02: flag_candidate_wfo_insufficient writes a sentinel
        wfo_consistency_scores row with windows_evaluated=0 and
        window_collapse_flag=1. A second call is idempotent (INSERT OR IGNORE
        — existing row is not overwritten).
        """
        db_path = tmp_path / "h02_02.db"
        store = CandidateStore(db_path)
        run_id = str(uuid.uuid4())
        candidate_id = str(uuid.uuid4())

        try:
            store.initialise_run(_make_run_metadata(run_id))

            # Seed candidates table
            store._conn.execute(
                "INSERT INTO candidates (candidate_id, run_id, zone_name, origin_stage, created_at) "
                "VALUES (?, ?, 'safe', 'RANDOM', ?)",
                (candidate_id, run_id, datetime.now(UTC).isoformat()),
            )
            store._conn.commit()

            # First call — writes sentinel row
            store.flag_candidate_wfo_insufficient(candidate_id, run_id)
            store.flush()

            score = store.get_wfo_consistency_score(candidate_id)
            assert score is not None, (
                "H02-02 FAIL: No wfo_consistency_scores row after flag_candidate_wfo_insufficient"
            )
            assert score.windows_evaluated == 0, (
                f"H02-02 FAIL: windows_evaluated should be 0, got {score.windows_evaluated}"
            )
            assert score.window_collapse_flag is True, (
                f"H02-02 FAIL: window_collapse_flag should be True, got {score.window_collapse_flag}"
            )
            assert score.composite_score == 0.0, (
                f"H02-02 FAIL: composite_score should be 0.0, got {score.composite_score}"
            )

            # Second call — idempotent, should not raise or corrupt the row
            store.flag_candidate_wfo_insufficient(candidate_id, run_id)
            store.flush()

            score_after = store.get_wfo_consistency_score(candidate_id)
            assert score_after is not None, (
                "H02-02 FAIL: wfo_consistency_scores row missing after second flag call"
            )
            assert score_after.windows_evaluated == 0, (
                "H02-02 FAIL: second flag call corrupted windows_evaluated"
            )

        finally:
            store.close()