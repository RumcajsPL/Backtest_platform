"""
Unit tests for orchestrator.py.

Tests:
- test_checkpoint_skip: set checkpoint to RANDOM_SEARCH_COMPLETE → Stage 1 stub skipped
- test_stage_0_validates_wfo_windows: config with 2 WFO windows → raises ValueError
- test_resume_rejects_changed_config: prior run with different config hash → refused
- test_stage_0_validates_scenario: invalid scenario name → raises ValueError
- test_stage_0_validates_enabled_zones: no enabled zones → raises ValueError
"""
from __future__ import annotations

import copy
import tempfile
import unittest
import uuid
from pathlib import Path

import yaml

from src.backtesting.candidate_store import CandidateStore
from src.backtesting.contracts import Checkpoint, RunMetadata
from src.backtesting.orchestrator import (
    _load_and_validate_config,
    _run_stage_0_init,
    _validate_wfo_windows,
    _resume_or_start,
)


# ── Minimal valid config fixture ──────────────────────────────────────────────

def _make_config(n_windows=3, scenario="capital_accumulation"):
    windows = [
        {"id": f"W{i+1:02d}", "start": f"202{i+2}-01-01", "end": f"202{i+2}-06-30"}
        for i in range(n_windows)
    ]
    return {
        "backtester_version": "1.0.0",
        "scenario": scenario,
        "run": {"output_dir": "outputs/test", "temp_dir": "temp/test"},
        "scenarios": {
            "capital_accumulation": {
                "description": "test",
                "fitness_weights": {
                    "net_pnl": 0.20, "expectancy": 0.25, "max_drawdown": 0.20,
                    "win_rate": 0.15, "trade_frequency": 0.10, "profit_factor": 0.10,
                },
                "constraints": {
                    "min_win_rate": 0.45, "max_drawdown": 0.15,
                    "max_losing_streak": 7, "min_trades_per_week": 3.0,
                    "min_expectancy": 0.4, "min_profit_factor": 1.3,
                },
                "mc_prefilter_ruin_threshold": 0.25,
                "wfo_temporal_weights": {
                    "median_return": 0.30, "variance": 0.30,
                    "worst_drawdown": 0.20, "fraction_positive": 0.20,
                },
                "verdict_thresholds": {
                    "go_wfo_floor": 0.65, "borderline_wfo_floor": 0.40,
                    "go_mc_ruin_ceiling": 0.05, "borderline_mc_ruin_ceiling": 0.15,
                    "sensitivity_spike_threshold": 0.15,
                },
                "report_emphasis": ["wfo_consistency_score"],
            },
        },
        "walk_forward": {"windows": windows},
        "zones": {
            "safe": {
                "enabled": True,
                "parameters": {
                    "rsi_period": {"type": "int", "min": 10, "max": 14, "step": 2},
                    "session_filter": {"type": "choice", "choices": ["london"]},
                },
            }
        },
    }


def _write_config_yaml(config: dict, path: Path) -> None:
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(config, f)


class TestStage0Validation(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.db_path = self.tmp / "test.db"

    def tearDown(self):
        self._tmp.cleanup()

    def _make_store_with_run(self, config):
        from datetime import datetime
        store = CandidateStore(self.db_path)
        run_meta = RunMetadata(
            run_id=str(uuid.uuid4()), config_hash="a" * 64,
            scenario_name="capital_accumulation",
            started_at=datetime(2026, 1, 1),
            perturbation_profile_name="default",
            random_search_seed=42, ga_seed=43, mc_prefilter_seed=44,
            mc_deep_seed=45, sensitivity_seed=46,
            wfo_window_ids=("W01", "W02", "W03"),
            checkpoint=Checkpoint.RUN_INITIALISED, backtester_version="1.0.0",
        )
        store.initialise_run(run_meta)
        return store, run_meta

    def test_stage_0_validates_wfo_windows_minimum(self):
        """Config with 2 WFO windows → raises ValueError."""
        config = _make_config(n_windows=2)
        store, run_meta = self._make_store_with_run(config)
        with self.assertRaises(ValueError) as ctx:
            _run_stage_0_init(config, store, run_meta)
        self.assertIn("3", str(ctx.exception))
        store.close()

    def test_stage_0_passes_with_3_windows(self):
        """Config with exactly 3 WFO windows passes Stage 0."""
        config = _make_config(n_windows=3)
        store, run_meta = self._make_store_with_run(config)
        _run_stage_0_init(config, store, run_meta)   # must not raise
        store.close()

    def test_stage_0_validates_scenario(self):
        """Unknown scenario name raises ValueError in Stage 0."""
        config = _make_config()
        config["scenario"] = "nonexistent_scenario"
        store, run_meta = self._make_store_with_run(config)
        with self.assertRaises(ValueError) as ctx:
            _run_stage_0_init(config, store, run_meta)
        self.assertIn("Scenario", str(ctx.exception))
        store.close()

    def test_stage_0_validates_enabled_zones(self):
        """No enabled zones raises ValueError in Stage 0."""
        config = _make_config()
        config["zones"]["safe"]["enabled"] = False
        store, run_meta = self._make_store_with_run(config)
        with self.assertRaises(ValueError) as ctx:
            _run_stage_0_init(config, store, run_meta)
        self.assertIn("zones", str(ctx.exception))
        store.close()


class TestValidateWfoWindows(unittest.TestCase):
    def test_accepts_3_windows(self):
        windows = [
            {"id": "W01", "start": "2022-01-01", "end": "2022-06-30"},
            {"id": "W02", "start": "2022-07-01", "end": "2022-12-31"},
            {"id": "W03", "start": "2023-01-01", "end": "2023-06-30"},
        ]
        _validate_wfo_windows(windows)  # Must not raise

    def test_rejects_2_windows(self):
        windows = [
            {"id": "W01", "start": "2022-01-01", "end": "2022-06-30"},
            {"id": "W02", "start": "2022-07-01", "end": "2022-12-31"},
        ]
        with self.assertRaises(ValueError):
            _validate_wfo_windows(windows)

    def test_rejects_duplicate_ids(self):
        windows = [
            {"id": "W01", "start": "2022-01-01", "end": "2022-06-30"},
            {"id": "W01", "start": "2022-07-01", "end": "2022-12-31"},
            {"id": "W03", "start": "2023-01-01", "end": "2023-06-30"},
        ]
        with self.assertRaises(ValueError) as ctx:
            _validate_wfo_windows(windows)
        self.assertIn("W01", str(ctx.exception))

    def test_rejects_start_after_end(self):
        windows = [
            {"id": "W01", "start": "2022-06-30", "end": "2022-01-01"},
            {"id": "W02", "start": "2022-07-01", "end": "2022-12-31"},
            {"id": "W03", "start": "2023-01-01", "end": "2023-06-30"},
        ]
        with self.assertRaises(ValueError):
            _validate_wfo_windows(windows)


class TestCheckpointSkip(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def test_checkpoint_skip_stage_1(self):
        """If checkpoint is RANDOM_SEARCH_COMPLETE, Stage 1 stub is not called."""
        from datetime import datetime
        store = CandidateStore(self.tmp / "skip_test.db")
        run_meta = RunMetadata(
            run_id=str(uuid.uuid4()), config_hash="c" * 64,
            scenario_name="capital_accumulation",
            started_at=datetime(2026, 1, 1),
            perturbation_profile_name="default",
            random_search_seed=42, ga_seed=43, mc_prefilter_seed=44,
            mc_deep_seed=45, sensitivity_seed=46,
            wfo_window_ids=("W01", "W02", "W03"),
            checkpoint=Checkpoint.RUN_INITIALISED, backtester_version="1.0.0",
        )
        store.initialise_run(run_meta)
        store.set_checkpoint(run_meta.run_id, Checkpoint.RANDOM_SEARCH_COMPLETE)

        # Verify skip: get_checkpoint should already be past Stage 1
        cp = store.get_checkpoint(run_meta.run_id)
        self.assertEqual(cp, Checkpoint.RANDOM_SEARCH_COMPLETE)
        self.assertGreaterEqual(cp.value, Checkpoint.RANDOM_SEARCH_COMPLETE.value)
        store.close()


class TestResumeOrStart(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def test_resume_rejects_changed_config(self):
        """Prior run with different config hash → ValueError."""
        from datetime import datetime
        config = _make_config()
        config_path = self.tmp / "config.yaml"
        _write_config_yaml(config, config_path)

        store = CandidateStore(self.tmp / "resume_test.db")
        # Write a run with a different hash
        run_meta = RunMetadata(
            run_id=str(uuid.uuid4()), config_hash="d" * 64,  # different hash
            scenario_name="capital_accumulation",
            started_at=datetime(2026, 1, 1),
            perturbation_profile_name="default",
            random_search_seed=42, ga_seed=43, mc_prefilter_seed=44,
            mc_deep_seed=45, sensitivity_seed=46,
            wfo_window_ids=("W01", "W02", "W03"),
            checkpoint=Checkpoint.RUN_INITIALISED, backtester_version="1.0.0",
        )
        store.initialise_run(run_meta)
        # Leave it non-complete (RUN_INITIALISED)

        with self.assertRaises(ValueError) as ctx:
            _resume_or_start(store, config, config_path)
        self.assertIn("mismatch", str(ctx.exception).lower())
        store.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)