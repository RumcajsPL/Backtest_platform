"""
Unit tests for ranker.py.
"""
from __future__ import annotations

import json
import tempfile
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
from src.backtesting.ranker import rank, rank_by_wfo, rank_combined


def _make_run_metadata():
    return RunMetadata(
        run_id=str(uuid.uuid4()), config_hash="b" * 64,
        scenario_name="capital_accumulation",
        started_at=datetime(2026, 1, 1), perturbation_profile_name="default",
        random_search_seed=1, ga_seed=2, mc_prefilter_seed=3,
        mc_deep_seed=4, sensitivity_seed=5,
        wfo_window_ids=("W01", "W02", "W03"),
        checkpoint=Checkpoint.RUN_INITIALISED, backtester_version="1.0.0",
    )


def _make_record(run_id, fitness, passed=True, stage=CandidateStage.RANDOM) -> CandidateRecord:
    return CandidateRecord(
        run_id=run_id, candidate_id=str(uuid.uuid4()),
        zone_name="safe", stage=stage.value,
        generation=None, recorded_at=datetime(2026, 1, 1),
        parameters_json=json.dumps({"rsi_period": 14, "atr_multiplier": 2.0, "session_filter": "london"}),
        fitness_score=fitness if passed else None,
        passed_constraints=passed,
        rejection_reason=None if passed else "REJECTED_CONSTRAINTS",
        failing_constraint=None, failing_value=None,
        actual_win_rate=0.5, actual_max_drawdown=0.05, actual_losing_streak=2,
        actual_trades_per_week=4.0, actual_expectancy=0.5, actual_profit_factor=1.5,
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


class TestRanker(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.store = CandidateStore(Path(self._tmp.name) / "test.db")
        self.run_meta = _make_run_metadata()
        self.store.initialise_run(self.run_meta)

    def tearDown(self):
        self.store.close()
        self._tmp.cleanup()

    def _write_all(self, records):
        for r in records:
            self.store.write_candidate(r)
        self.store._queue.join()

    def test_rank_returns_top_n(self):
        """50 candidates stored, rank(top_n=10) returns exactly 10."""
        records = [_make_record(self.run_meta.run_id, fitness=i / 50.0) for i in range(50)]
        self._write_all(records)
        result = rank(self.store, self.run_meta.run_id, CandidateStage.RANDOM, top_n=10)
        self.assertEqual(len(result), 10)

    def test_rank_excludes_failed(self):
        """Candidates with passed_constraints=False are excluded."""
        records = [
            _make_record(self.run_meta.run_id, fitness=0.8, passed=True),
            _make_record(self.run_meta.run_id, fitness=0.9, passed=False),
            _make_record(self.run_meta.run_id, fitness=0.7, passed=True),
        ]
        self._write_all(records)
        result = rank(self.store, self.run_meta.run_id, CandidateStage.RANDOM, top_n=10)
        self.assertEqual(len(result), 2)
        for r in result:
            self.assertTrue(r.passed_constraints)

    def test_rank_ordering(self):
        """Returned candidates are ordered by fitness descending."""
        records = [
            _make_record(self.run_meta.run_id, fitness=0.3),
            _make_record(self.run_meta.run_id, fitness=0.9),
            _make_record(self.run_meta.run_id, fitness=0.5),
        ]
        self._write_all(records)
        result = rank(self.store, self.run_meta.run_id, CandidateStage.RANDOM, top_n=3)
        scores = [r.fitness_score for r in result]
        self.assertEqual(scores, sorted(scores, reverse=True))

    def test_rank_combined_deduplicates(self):
        """rank_combined removes duplicate candidate_ids across stages."""
        record = _make_record(self.run_meta.run_id, fitness=0.75, stage=CandidateStage.RANDOM)
        self._write_all([record])
        # Same candidate in both stages would be deduped
        result = rank_combined(
            self.store, self.run_meta.run_id,
            [CandidateStage.RANDOM, CandidateStage.GA],
            top_n=10,
        )
        ids = [r.candidate_id for r in result]
        self.assertEqual(len(ids), len(set(ids)))

    def test_rank_combined_top_n(self):
        records = [_make_record(self.run_meta.run_id, fitness=i / 20.0) for i in range(20)]
        self._write_all(records)
        result = rank_combined(
            self.store, self.run_meta.run_id, [CandidateStage.RANDOM], top_n=5
        )
        self.assertEqual(len(result), 5)


if __name__ == "__main__":
    unittest.main(verbosity=2)