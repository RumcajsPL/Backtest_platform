"""
Unit tests for strategy_runner.py.

All tests mock the strategy package — strategy_runner itself gracefully handles
import failures, so we test via monkey-patching sys.modules to inject
controllable fakes.

Tests:
- test_evaluate_returns_result_not_raises: force exception → CandidateResult with error
- test_significance_guard: mock returning 5 trades → REJECTED_INSUFFICIENT_TRADES
- test_cache_cleared_on_success: CacheManager.clear_all_caches called on success
- test_cache_cleared_on_failure: CacheManager.clear_all_caches called on exception
- test_temp_yaml_deleted: temp YAML does not exist after evaluate returns
- test_temp_yaml_retained_when_flag_set: temp YAML exists if retain_temp_yamls=True
"""
from __future__ import annotations

import sys
import tempfile
import types
import unittest
import uuid
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.backtesting.contracts import CandidateParameterSet, RejectionReason


def _make_candidate():
    return CandidateParameterSet.create(
        zone_name="safe",
        parameters={"rsi_period": 14, "atr_multiplier": 2.0, "session_filter": "london"},
        generation=None,
    )


def _make_base_yaml(tmp_dir: Path) -> Path:
    """Write a minimal strategy YAML to tmp_dir for temp-YAML write tests."""
    import yaml
    content = {
        "execution": {"mode": "analytics"},
        "indicators": {"rsi": {"period": 10, "overbought": 70, "oversold": 30}, "adx": {"threshold": 25}, "atr": {"length": 14}},
        "trade_management": {"risk": {"atr_multiplier_sl": 1.5, "rr_ratio": 2.0, "max_risk_percentile": 1.0}},
        "data": {"strategy_timeframe": "H1", "htf_timeframe": "H4"},
        "filters": {"time": {"session": "london"}},
    }
    path = tmp_dir / "base.yaml"
    with open(path, "w", encoding="utf-8") as f:
        import yaml
        yaml.safe_dump(content, f)
    return path


class _FakeCacheManager:
    """Minimal fake CacheManager that records clear_all_caches calls."""
    def __init__(self):
        self.clear_count = 0

    def clear_all_caches(self):
        self.clear_count += 1


class _FakeMetrics:
    total_trades = 50


class _FakeTradeResult:
    pass


class _FakeOrchestratorResult:
    def __init__(self, total_trades=50):
        self.metrics = _FakeMetrics()
        self.metrics.total_trades = total_trades
        self.trade_result = _FakeTradeResult()


def _inject_fake_strategy_modules(cache_manager_instance, orchestrator_run_fn):
    """
    Inject fake src.config.config_schema, src.strategies.orchestrator,
    and src.core.cache_manager into sys.modules so strategy_runner's
    imports succeed without the real strategy package.
    """
    # src package
    src_mod = types.ModuleType("src")
    sys.modules.setdefault("src", src_mod)

    # src.config
    config_mod = types.ModuleType("src.config")
    sys.modules["src.config"] = config_mod
    src_mod.config = config_mod

    # src.config.config_schema
    schema_mod = types.ModuleType("src.config.config_schema")
    fake_config_cls = MagicMock()
    fake_config_cls.from_yaml = MagicMock(return_value=MagicMock())
    schema_mod.StrategyConfig = fake_config_cls
    sys.modules["src.config.config_schema"] = schema_mod
    config_mod.config_schema = schema_mod

    # src.strategies
    strat_mod = types.ModuleType("src.strategies")
    sys.modules["src.strategies"] = strat_mod
    src_mod.strategies = strat_mod

    # src.strategies.orchestrator
    orch_mod = types.ModuleType("src.strategies.orchestrator")
    fake_orch_cls = MagicMock()
    fake_orch_cls.return_value.run = orchestrator_run_fn
    orch_mod.StrategyOrchestrator = fake_orch_cls
    sys.modules["src.strategies.orchestrator"] = orch_mod
    strat_mod.orchestrator = orch_mod

    # src.core
    core_mod = types.ModuleType("src.core")
    sys.modules["src.core"] = core_mod
    src_mod.core = core_mod

    # src.core.cache_manager
    cache_mod = types.ModuleType("src.core.cache_manager")
    fake_cm_cls = MagicMock(return_value=cache_manager_instance)
    cache_mod.CacheManager = fake_cm_cls
    sys.modules["src.core.cache_manager"] = cache_mod
    core_mod.cache_manager = cache_mod


def _remove_fake_strategy_modules():
    for key in list(sys.modules.keys()):
        if key.startswith("src.config") or key.startswith("src.strategies") or key.startswith("src.core"):
            del sys.modules[key]


class TestStrategyRunnerNeverRaises(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self):
        _remove_fake_strategy_modules()
        self._tmp.cleanup()

    def test_evaluate_returns_result_not_raises(self):
        """Force orchestrator to raise — evaluate must return CandidateResult with error."""
        cache_mgr = _FakeCacheManager()

        def crash_run(mode):
            raise RuntimeError("simulated strategy crash")

        _inject_fake_strategy_modules(cache_mgr, crash_run)

        from src.backtesting import strategy_runner
        import importlib
        importlib.reload(strategy_runner)

        base_yaml = _make_base_yaml(self.tmp)
        candidate = _make_candidate()

        result = strategy_runner.evaluate(candidate, base_yaml, self.tmp)

        self.assertIsNotNone(result)
        self.assertEqual(result.candidate_id, candidate.candidate_id)
        self.assertIsNone(result.metrics)
        self.assertIsNotNone(result.error)
        self.assertIn("simulated strategy crash", result.error)

    def test_significance_guard(self):
        """Orchestrator returning 5 trades → REJECTED_INSUFFICIENT_TRADES."""
        cache_mgr = _FakeCacheManager()
        _inject_fake_strategy_modules(cache_mgr, lambda mode: _FakeOrchestratorResult(total_trades=5))

        from src.backtesting import strategy_runner
        import importlib
        importlib.reload(strategy_runner)

        base_yaml = _make_base_yaml(self.tmp)
        result = strategy_runner.evaluate(_make_candidate(), base_yaml, self.tmp, min_significant_trades=30)

        self.assertFalse(result.is_valid)
        self.assertEqual(result.error, RejectionReason.REJECTED_INSUFFICIENT_TRADES.value)
        self.assertEqual(result.total_trades, 5)

    def test_cache_cleared_on_success(self):
        """CacheManager.clear_all_caches called exactly once on successful evaluation."""
        cache_mgr = _FakeCacheManager()
        _inject_fake_strategy_modules(cache_mgr, lambda mode: _FakeOrchestratorResult(total_trades=50))

        from src.backtesting import strategy_runner
        import importlib
        importlib.reload(strategy_runner)

        base_yaml = _make_base_yaml(self.tmp)
        strategy_runner.evaluate(_make_candidate(), base_yaml, self.tmp, min_significant_trades=30)

        self.assertEqual(cache_mgr.clear_count, 1)

    def test_cache_cleared_on_failure(self):
        """CacheManager.clear_all_caches called even when orchestrator raises."""
        cache_mgr = _FakeCacheManager()

        def crash_run(mode):
            raise ValueError("crash")

        _inject_fake_strategy_modules(cache_mgr, crash_run)

        from src.backtesting import strategy_runner
        import importlib
        importlib.reload(strategy_runner)

        base_yaml = _make_base_yaml(self.tmp)
        strategy_runner.evaluate(_make_candidate(), base_yaml, self.tmp)

        self.assertEqual(cache_mgr.clear_count, 1)

    def test_temp_yaml_deleted(self):
        """Temp YAML does not exist after evaluate returns (retain_temp_yamls=False)."""
        cache_mgr = _FakeCacheManager()
        _inject_fake_strategy_modules(cache_mgr, lambda mode: _FakeOrchestratorResult(total_trades=50))

        from src.backtesting import strategy_runner
        import importlib
        importlib.reload(strategy_runner)

        base_yaml = _make_base_yaml(self.tmp)
        candidate = _make_candidate()
        expected_yaml = self.tmp / f"candidate_{candidate.candidate_id[:12]}.yaml"

        strategy_runner.evaluate(candidate, base_yaml, self.tmp, retain_temp_yamls=False)

        self.assertFalse(expected_yaml.exists(), f"Temp YAML was not deleted: {expected_yaml}")

    def test_temp_yaml_retained_when_flag_set(self):
        """Temp YAML exists when retain_temp_yamls=True."""
        cache_mgr = _FakeCacheManager()
        _inject_fake_strategy_modules(cache_mgr, lambda mode: _FakeOrchestratorResult(total_trades=50))

        from src.backtesting import strategy_runner
        import importlib
        importlib.reload(strategy_runner)

        base_yaml = _make_base_yaml(self.tmp)
        candidate = _make_candidate()
        expected_yaml = self.tmp / f"candidate_{candidate.candidate_id[:12]}.yaml"

        strategy_runner.evaluate(candidate, base_yaml, self.tmp, retain_temp_yamls=True)

        self.assertTrue(expected_yaml.exists(), f"Temp YAML should exist when retain_temp_yamls=True")


if __name__ == "__main__":
    unittest.main(verbosity=2)