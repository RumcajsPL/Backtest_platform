"""
Unit tests for candidate_store.py — using unittest (no pytest dependency).
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
import uuid
from datetime import datetime
from pathlib import Path

from src.backtesting.candidate_store import CandidateStore
from src.backtesting.contracts import (
    CandidateRecord,
    CandidateStage,
    Checkpoint,
    RunMetadata,
)


def _make_run_metadata(wfo_window_ids=("W01", "W02", "W03"), checkpoint=Checkpoint.RUN_INITIALISED):
    return RunMetadata(
        run_id=str(uuid.uuid4()), config_hash="a" * 64,
        scenario_name="capital_accumulation",
        started_at=datetime(2026, 1, 1, 12, 0, 0),
        perturbation_profile_name="default",
        random_search_seed=42, ga_seed=43, mc_prefilter_seed=44,
        mc_deep_seed=45, sensitivity_seed=46,
        wfo_window_ids=tuple(wfo_window_ids), checkpoint=checkpoint,
        backtester_version="1.0.0",
    )


def _make_candidate_record(run_id, candidate_id=None):
    if candidate_id is None:
        candidate_id = str(uuid.uuid4())
    params = {"rsi_period": 14, "atr_multiplier": 2.0, "session_filter": "london"}
    return CandidateRecord(
        run_id=run_id, candidate_id=candidate_id, zone_name="safe",
        stage=CandidateStage.RANDOM.value, generation=None,
        recorded_at=datetime(2026, 1, 1, 12, 0, 0),
        parameters_json=json.dumps(params),
        fitness_score=0.72, passed_constraints=True,
        rejection_reason=None, failing_constraint=None, failing_value=None,
        actual_win_rate=0.55, actual_max_drawdown=0.08, actual_losing_streak=3,
        actual_trades_per_week=4.5, actual_expectancy=0.6, actual_profit_factor=1.8,
        wfo_median_window_return=None, wfo_window_return_variance=None,
        wfo_worst_window_drawdown=None, wfo_fraction_positive_windows=None,
        wfo_consistency_score=None, wfo_windows_evaluated=None,
        wfo_oos_gate_triggered=None, wfo_window_collapse_flag=None,
        mc_prefilter_ruin_probability=None, mc_prefilter_avg_final_equity=None,
        mc_prefilter_iterations=None, mc_deep_ruin_probability=None,
        mc_deep_avg_final_equity=None, mc_deep_worst_drawdown=None,
        mc_deep_p5_final_equity=None, mc_deep_iterations=None,
        sensitivity_spike_detected=None, sensitivity_spike_parameters=None,
        sensitivity_profile_complete=None,
        verdict=None, deployment_status=None, evidence_summary=None,
    )


class TestStoreSchema(unittest.TestCase):
    def test_store_creates_schema(self):
        """Fresh DB has all 9 required tables."""
        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(Path(tmp) / "test.db")
            conn = sqlite3.connect(str(store._db_path))
            tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            conn.close()
            store.close()
            expected = {
                "runs","candidates","candidate_parameters","evaluations",
                "wfo_window_results","wfo_consistency_scores","mc_results",
                "sensitivity_results","sensitivity_profiles","verdicts",
            }
            self.assertEqual(expected - tables, set(), f"Missing: {expected - tables}")


class TestStoreReadWrite(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.store = CandidateStore(Path(self._tmp.name) / "test.db")
        self.run_meta = _make_run_metadata()
        self.store.initialise_run(self.run_meta)

    def tearDown(self):
        self.store.close()
        self._tmp.cleanup()

    def test_write_and_read_candidate(self):
        record = _make_candidate_record(self.run_meta.run_id)
        self.store.write_candidate(record)
        self.store._queue.join()
        results = self.store.query_candidates(self.run_meta.run_id, stage=CandidateStage.RANDOM)
        self.assertEqual(len(results), 1)
        r = results[0]
        self.assertEqual(r.candidate_id, record.candidate_id)
        self.assertAlmostEqual(r.fitness_score, 0.72, places=5)
        self.assertTrue(r.passed_constraints)
        self.assertAlmostEqual(r.actual_win_rate, 0.55, places=5)

    def test_concurrent_writes(self):
        """6 threads × 100 records = 600 rows, zero missing."""
        lock = threading.Lock()
        all_ids = []
        def write_batch():
            for _ in range(100):
                cid = str(uuid.uuid4())
                with lock: all_ids.append(cid)
                self.store.write_candidate(_make_candidate_record(self.run_meta.run_id, cid))
        threads = [threading.Thread(target=write_batch) for _ in range(6)]
        for t in threads: t.start()
        for t in threads: t.join()
        self.store._queue.join()
        conn = sqlite3.connect(str(self.store._db_path))
        count = conn.execute("SELECT COUNT(*) FROM evaluations WHERE run_id=?", (self.run_meta.run_id,)).fetchone()[0]
        conn.close()
        self.assertEqual(count, 600)

    def test_checkpoint_round_trip(self):
        for cp in [Checkpoint.RANDOM_SEARCH_COMPLETE, Checkpoint.GA_COMPLETE, Checkpoint.COMPLETE]:
            self.store.set_checkpoint(self.run_meta.run_id, cp)
            self.assertEqual(self.store.get_checkpoint(self.run_meta.run_id), cp)

    def test_resume_detection(self):
        self.store.set_checkpoint(self.run_meta.run_id, Checkpoint.MC_PREFILTER_COMPLETE)
        retrieved = self.store.get_run_metadata(self.run_meta.run_id)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.run_id, self.run_meta.run_id)
        self.assertEqual(self.store.get_checkpoint(self.run_meta.run_id), Checkpoint.MC_PREFILTER_COMPLETE)

    def test_query_excludes_other_runs(self):
        run_b = _make_run_metadata()
        self.store.initialise_run(run_b)
        self.store.write_candidate(_make_candidate_record(self.run_meta.run_id))
        self.store.write_candidate(_make_candidate_record(self.run_meta.run_id))
        self.store.write_candidate(_make_candidate_record(run_b.run_id))
        self.store._queue.join()
        self.assertEqual(len(self.store.query_candidates(self.run_meta.run_id)), 2)
        self.assertEqual(len(self.store.query_candidates(run_b.run_id)), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)