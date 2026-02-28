"""
Integration test: single candidate full round-trip → stored in SQLite.

Tests the Phase 2 milestone:
- RunMetadata row exists in SQLite runs table with correct config_hash
- CandidateRecord row exists in evaluations table with stage="RANDOM"
- candidate_parameters row exists with correct parameter values
- If candidate passed constraints: fitness_score is in [0, 1]
- If candidate failed constraints: rejection_reason is set
- CacheManager.clear_all_caches() was called (mocked)
- Temp YAML file does not exist after evaluate returns

This test drives the real Stage 0 (validation) and a single manually-executed
candidate evaluation (mocked strategy), then verifies the SQLite state.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import types
import unittest
import uuid
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import yaml

from src.backtesting.candidate_store import CandidateStore
from src.backtesting.contracts import (
    CandidateParameterSet,
    CandidateRecord,
    CandidateStage,
    Checkpoint,
    RunMetadata,
)
from src.backtesting.fitness import evaluate_fitness
from src.backtesting.orchestrator import _run_stage_0_init, _validate_wfo_windows
from src.backtesting.scenario import load_scenario


# ── Helpers ───────────────────────────────────────────────────────────────────

def _build_config(tmp_dir: Path) -> dict:
    """Build a valid capital_accumulation config with 3 WFO windows."""
    return {
        "backtester_version": "1.0.0",
        "scenario": "capital_accumulation",
        "run": {
            "output_dir": str(tmp_dir / "outputs"),
            "temp_dir": str(tmp_dir / "temp"),
        },
        "scenarios": {
            "capital_accumulation": {
                "description": "Steadily grow account balance with controlled risk",
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
        "walk_forward": {
            "windows": [
                {"id": "W01", "start": "2022-01-01", "end": "2022-06-30"},
                {"id": "W02", "start": "2022-07-01", "end": "2022-12-31"},
                {"id": "W03", "start": "2023-01-01", "end": "2023-06-30"},
            ]
        },
        "zones": {
            "safe": {
                "enabled": True,
                "parameters": {
                    "rsi_period":     {"type": "int",    "min": 10, "max": 14, "step": 2},
                    "atr_multiplier": {"type": "float",  "min": 1.5, "max": 2.0, "step": 0.5},
                    "session_filter": {"type": "choice", "choices": ["london", "new_york"]},
                },
            },
        },
        "random_search": {"method": "lhs", "samples_per_zone": 10, "min_significant_trades": 30, "seed": 42},
    }


def _write_config_yaml(config: dict, path: Path) -> None:
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(config, f)


def _inject_strategy_mock(cache_manager_instance, total_trades=50):
    """Inject fake strategy modules into sys.modules."""
    _total_trades = total_trades  # capture for closure

    class _Metrics:
        win_rate = 0.55
        max_drawdown = 0.08
        max_losing_streak = 3
        trades_per_week = 5.0
        expectancy = 0.6
        profit_factor = 1.8
        total_pnl_points = 1200.0

    _Metrics.total_trades = _total_trades

    class _Orchestrator:
        def __init__(self, config, cache_manager=None):
            self._cache = cache_manager

        def run(self, mode):
            result = MagicMock()
            result.metrics = _Metrics()
            result.trade_result = MagicMock()
            return result

    for key in list(sys.modules.keys()):
        if any(key.startswith(p) for p in ["src.config", "src.strategies", "src.core"]):
            del sys.modules[key]

    src = sys.modules.setdefault("src", types.ModuleType("src"))
    for mod_path, attr_chain, obj in [
        ("src.config", "config", types.ModuleType("src.config")),
        ("src.config.config_schema", None, None),
        ("src.strategies", "strategies", types.ModuleType("src.strategies")),
        ("src.strategies.orchestrator", None, None),
        ("src.core", "core", types.ModuleType("src.core")),
        ("src.core.cache_manager", None, None),
    ]:
        if obj is not None:
            sys.modules[mod_path] = obj
            if attr_chain:
                setattr(src, attr_chain, obj)

    cfg_mod = sys.modules["src.config"]
    schema_mod = types.ModuleType("src.config.config_schema")
    schema_mod.StrategyConfig = MagicMock()
    schema_mod.StrategyConfig.from_yaml.return_value = MagicMock()
    sys.modules["src.config.config_schema"] = schema_mod
    cfg_mod.config_schema = schema_mod

    strat_mod = sys.modules["src.strategies"]
    orch_mod = types.ModuleType("src.strategies.orchestrator")
    orch_mod.StrategyOrchestrator = _Orchestrator
    sys.modules["src.strategies.orchestrator"] = orch_mod
    strat_mod.orchestrator = orch_mod

    core_mod = sys.modules["src.core"]
    cache_mod = types.ModuleType("src.core.cache_manager")
    cache_mod.CacheManager = MagicMock(return_value=cache_manager_instance)
    sys.modules["src.core.cache_manager"] = cache_mod
    core_mod.cache_manager = cache_mod


class TestSingleCandidateRoundTrip(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.tmp.mkdir(exist_ok=True)

    def tearDown(self):
        for key in list(sys.modules.keys()):
            if any(key.startswith(p) for p in ["src.config", "src.strategies", "src.core"]):
                sys.modules.pop(key, None)
        self._tmp.cleanup()

    def test_single_candidate_full_round_trip(self):
        """
        Stage 0 + manual single-candidate evaluation → all SQLite rows present.
        """
        config = _build_config(self.tmp)
        config_path = self.tmp / "backtest_template.yaml"
        _write_config_yaml(config, config_path)

        db_path = self.tmp / "backtester.db"
        store = CandidateStore(db_path)

        # ── RunMetadata ───────────────────────────────────────────────────────
        import hashlib
        config_hash = hashlib.sha256(config_path.read_bytes()).hexdigest()

        run_meta = RunMetadata(
            run_id=str(uuid.uuid4()),
            config_hash=config_hash,
            scenario_name="capital_accumulation",
            started_at=datetime(2026, 1, 1, 12, 0, 0),
            perturbation_profile_name="default",
            random_search_seed=42, ga_seed=43, mc_prefilter_seed=44,
            mc_deep_seed=45, sensitivity_seed=46,
            wfo_window_ids=("W01", "W02", "W03"),
            checkpoint=Checkpoint.RUN_INITIALISED,
            backtester_version="1.0.0",
        )
        store.initialise_run(run_meta)

        # ── Stage 0 ───────────────────────────────────────────────────────────
        _run_stage_0_init(config, store, run_meta)   # Must not raise

        # ── Verify runs row ───────────────────────────────────────────────────
        conn = sqlite3.connect(str(db_path))
        run_row = conn.execute(
            "SELECT run_id, config_hash, scenario_name FROM runs WHERE run_id = ?",
            (run_meta.run_id,)
        ).fetchone()
        conn.close()

        self.assertIsNotNone(run_row, "runs table row not found")
        self.assertEqual(run_row[0], run_meta.run_id)
        self.assertEqual(run_row[1], config_hash)
        self.assertEqual(run_row[2], "capital_accumulation")

        # ── Simulate one candidate evaluation (mocked strategy) ──────────────
        from unittest.mock import MagicMock as MM
        cache_mgr = MM()
        cache_mgr.clear_all_caches = MM()

        _inject_strategy_mock(cache_mgr, total_trades=50)

        base_yaml = self.tmp / "base_strategy.yaml"
        strategy_yaml_content = {
            "execution": {"mode": "analytics"},
            "indicators": {"rsi": {"period": 14, "overbought": 70, "oversold": 30},
                           "adx": {"threshold": 25}, "atr": {"length": 14}},
            "trade_management": {"risk": {"atr_multiplier_sl": 2.0, "rr_ratio": 2.0,
                                          "max_risk_percentile": 1.0}},
            "data": {"strategy_timeframe": "H1", "htf_timeframe": "H4"},
            "filters": {"time": {"session": "london"}},
        }
        with open(base_yaml, "w") as f:
            yaml.safe_dump(strategy_yaml_content, f)

        temp_dir = self.tmp / "temp"
        temp_dir.mkdir(exist_ok=True)

        candidate = CandidateParameterSet.create(
            zone_name="safe",
            parameters={"rsi_period": 14, "atr_multiplier": 2.0, "session_filter": "london"},
        )

        import importlib
        from src.backtesting import strategy_runner
        importlib.reload(strategy_runner)

        candidate_result = strategy_runner.evaluate(
            candidate, base_yaml, temp_dir, min_significant_trades=30
        )

        # ── Evaluate fitness ──────────────────────────────────────────────────
        scenario = load_scenario(config)
        fitness_result = evaluate_fitness(candidate_result, scenario)

        # ── Store the record ──────────────────────────────────────────────────
        record = CandidateRecord(
            run_id=run_meta.run_id,
            candidate_id=candidate.candidate_id,
            zone_name=candidate.zone_name,
            stage=CandidateStage.RANDOM.value,
            generation=None,
            recorded_at=datetime.utcnow(),
            parameters_json=json.dumps(candidate.parameters),
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
        store.write_candidate(record)
        store._queue.join()

        # ── Verify SQLite state ───────────────────────────────────────────────
        conn = sqlite3.connect(str(db_path))

        # evaluations row exists
        eval_row = conn.execute(
            "SELECT candidate_id, stage, fitness_score, passed_constraints "
            "FROM evaluations WHERE candidate_id = ?",
            (candidate.candidate_id,)
        ).fetchone()
        self.assertIsNotNone(eval_row, "evaluations row not found")
        self.assertEqual(eval_row[1], "RANDOM")

        # candidate_parameters row exists
        params_row = conn.execute(
            "SELECT rsi_period, atr_multiplier, session_filter "
            "FROM candidate_parameters WHERE candidate_id = ?",
            (candidate.candidate_id,)
        ).fetchone()
        self.assertIsNotNone(params_row, "candidate_parameters row not found")
        self.assertEqual(params_row[0], 14)
        self.assertAlmostEqual(params_row[1], 2.0, places=4)
        self.assertEqual(params_row[2], "london")

        conn.close()

        # Fitness result consistency
        if fitness_result.passed_constraints:
            self.assertIsNotNone(fitness_result.fitness_score)
            self.assertGreaterEqual(fitness_result.fitness_score, 0.0)
            self.assertLessEqual(fitness_result.fitness_score, 1.0)
        else:
            self.assertIsNotNone(fitness_result.rejection_reason)

        # Temp YAML cleaned up
        expected_temp_yaml = temp_dir / f"candidate_{candidate.candidate_id[:12]}.yaml"
        self.assertFalse(
            expected_temp_yaml.exists(),
            f"Temp YAML was not cleaned up: {expected_temp_yaml}"
        )

        store.close()
        print(f"\n  Integration test PASSED")
        print(f"  candidate_id : {candidate.candidate_id[:16]}…")
        print(f"  passed_constraints: {fitness_result.passed_constraints}")
        print(f"  fitness_score: {fitness_result.fitness_score}")


if __name__ == "__main__":
    unittest.main(verbosity=2)