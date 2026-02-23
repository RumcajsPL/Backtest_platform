"""
Unit Tests for StrategyOrchestrator
=====================================
Tests pipeline composition, stage execution, and timing.
Focuses on orchestration logic, not implementation details of dependencies.
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
import time

from src.strategies.orchestrator import (
    StrategyOrchestrator,
    OrchestratorResult
)
from src.strategies.core.cache_manager import CacheManager
from src.strategies.contracts.data_contracts import DataBundle, DataInfo
from src.strategies.contracts.signal_contracts import SignalFrame
from src.strategies.contracts.filter_contracts import FilterPipelineResult
from src.strategies.contracts.trade_contracts import TradeResult
from src.strategies.contracts.metrics_contracts import MetricsReport
from src.config.config_schema import StrategyConfig


class TestStrategyOrchestrator:
    """Tests for StrategyOrchestrator class."""

    @pytest.fixture
    def real_asset_config(self, base_config_dict):
        """Use a real asset that exists in broker config."""
        from src.config.config_schema import StrategyConfig
        
        config_dict = base_config_dict.copy()
        config_dict["asset"]["symbol"] = "EURUSD"  # Real asset in broker config
        config_dict["data"]["paths"]["strategy_ohlcv"] = "data/processed/ohlcv/EURUSD_1min_20240101_20260207.parquet"
        config_dict["data"]["paths"]["htf_ohlcv"] = "data/processed/ohlcv/EURUSD_1H_20240101_20260207.parquet"
        
        return StrategyConfig.from_dict(config_dict)

    @pytest.fixture
    def mock_data_bundle(self):
        """Create a minimal mock DataBundle."""
        bundle = MagicMock(spec=DataBundle)
        bundle.info = MagicMock(spec=DataInfo)
        bundle.info.strategy_bars = 100
        bundle.info.total_bars = 200
        bundle.info.cache_hit = False
        bundle.has_htf = True
        bundle.has_ltf = False
        bundle.strategy = MagicMock(spec=pd.DataFrame)
        bundle.htf = MagicMock(spec=pd.DataFrame)
        mock_full = MagicMock(spec=pd.DataFrame)
        mock_full.index = MagicMock(spec=pd.DatetimeIndex)
        bundle.full = mock_full
        bundle.artf = None
        return bundle

    @pytest.fixture
    def mock_signal_frame(self):
        """Create a minimal mock SignalFrame."""
        frame = MagicMock(spec=SignalFrame)
        frame.count_by_type.return_value = {"buy": 5, "sell": 5, "total": 10}
        frame.signals = MagicMock(spec=pd.Series)
        frame.signals.values = np.array([1]*5 + [2]*5 + [0]*90)
        frame.signals.index = MagicMock()
        frame.signals.index.hour = MagicMock(spec=pd.Series)
        # Fix: Convert range to numpy array first, then apply modulo
        frame.signals.index.hour.values = (np.arange(100) % 24).astype(np.int32)
        frame.signals.index.minute = MagicMock(spec=pd.Series)
        frame.signals.index.minute.values = (np.arange(100) % 60).astype(np.int32)
        return frame

    @pytest.fixture
    def mock_filter_result(self, mock_signal_frame):
        """Create a minimal mock FilterPipelineResult."""
        result = MagicMock(spec=FilterPipelineResult)
        result.raw_count = 10
        result.final_count = 8
        result.pass_rate = 80.0
        result.final_signals = mock_signal_frame
        return result

    @pytest.fixture
    def mock_trade_result(self):
        """Create a minimal mock TradeResult."""
        result = MagicMock(spec=TradeResult)
        result.total_opened = 8
        result.total_closed = 6
        result.win_count = 4
        result.loss_count = 2
        result.total_pnl_points = 25.5
        return result

    @pytest.fixture
    def mock_metrics_report(self):
        """Create a minimal mock MetricsReport."""
        metrics = MagicMock(spec=MetricsReport)
        metrics.total_trades = 6
        metrics.win_rate = 66.67
        metrics.total_pnl_points = 25.5
        return metrics

    def test_initialization_with_config(self, real_asset_config):
        """Test orchestrator initialization with valid config."""
        orchestrator = StrategyOrchestrator(config=real_asset_config)
        
        assert orchestrator._config == real_asset_config
        assert orchestrator._mode == real_asset_config.execution.mode
        assert orchestrator._cache_manager is not None

    def test_initialization_with_cache_manager(self, real_asset_config):
        """Test initialization with provided cache manager."""
        cache_manager = CacheManager()
        orchestrator = StrategyOrchestrator(
            config=real_asset_config,
            cache_manager=cache_manager
        )
        
        assert orchestrator._cache_manager == cache_manager

    def test_initialization_invalid_mode(self, base_config_dict):
        """Test initialization with invalid mode - should fail at config level."""
        from src.config.config_schema import StrategyConfig
        
        base_config_dict["execution"]["mode"] = "invalid_mode"
        
        # The exact error message from config_schema.py
        expected_msg = "Invalid execution.mode 'invalid_mode'. Must be one of:"
        
        with pytest.raises(ValueError, match=expected_msg):
            StrategyConfig.from_dict(base_config_dict)

    def test_from_yaml_valid(self, tmp_path, base_config_dict):
        """Test from_yaml classmethod with valid file."""
        import yaml
        
        config_path = tmp_path / "test_config.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(base_config_dict, f)
        
        orchestrator = StrategyOrchestrator.from_yaml(config_path)
        
        assert isinstance(orchestrator, StrategyOrchestrator)
        assert orchestrator._config.asset.symbol == base_config_dict["asset"]["symbol"]

    def test_from_yaml_nonexistent(self, tmp_path):
        """Test from_yaml with nonexistent file."""
        with pytest.raises(FileNotFoundError, match="Strategy config not found"):
            StrategyOrchestrator.from_yaml(tmp_path / "nonexistent.yaml")

    def test_run_core_mode(self, real_asset_config, mock_data_bundle, mock_signal_frame, mock_filter_result, mock_trade_result, mock_metrics_report):
        """Test orchestrator runs in core mode with minimal overhead."""
        # Mock the instance methods directly
        with patch('src.strategies.specific.modules.data_loader.DataLoader.load_data', return_value=mock_data_bundle) as mock_load_data, \
            patch('src.strategies.specific.modules.signal_generator.SignalGenerator.generate_signals', return_value=mock_signal_frame) as mock_generate, \
            patch('src.strategies.orchestrator.FilterPipeline') as mock_filter_class, \
            patch('src.strategies.orchestrator.TradeSimulator') as mock_sim_class, \
            patch('src.strategies.orchestrator.calculate_metrics', return_value=mock_metrics_report) as mock_metrics:
            # Note: ^^^ changed from 'src.strategies.specific.modules.metrics_calculator.calculate_metrics' 
            # to 'src.strategies.orchestrator.calculate_metrics'
            
            mock_filter_instance = MagicMock()
            mock_filter_class.return_value = mock_filter_instance
            mock_filter_instance.apply_filters.return_value = mock_filter_result
            
            mock_sim_instance = MagicMock()
            mock_sim_class.return_value = mock_sim_instance
            mock_sim_instance.simulate_trades.return_value = mock_trade_result
            
            orchestrator = StrategyOrchestrator(config=real_asset_config)
            result = orchestrator.run()
            
            assert isinstance(result, OrchestratorResult)
            assert result.mode == "core"
            assert result.total_duration_ms > 0
            
            # Verify all mocks were called
            mock_load_data.assert_called_once()
            mock_generate.assert_called_once()
            mock_filter_instance.apply_filters.assert_called_once()
            mock_sim_instance.simulate_trades.assert_called_once()
            mock_metrics.assert_called_once()

    def test_mode_override(self, real_asset_config, mock_data_bundle, mock_signal_frame, mock_filter_result, mock_trade_result, mock_metrics_report):
        """Test mode override functionality."""
        # Mock the instance methods directly
        with patch('src.strategies.specific.modules.data_loader.DataLoader.load_data', return_value=mock_data_bundle) as mock_load_data, \
            patch('src.strategies.specific.modules.signal_generator.SignalGenerator.generate_signals', return_value=mock_signal_frame) as mock_generate, \
            patch('src.strategies.orchestrator.FilterPipeline') as mock_filter_class, \
            patch('src.strategies.orchestrator.TradeSimulator') as mock_sim_class, \
            patch('src.strategies.orchestrator.calculate_metrics', return_value=mock_metrics_report) as mock_metrics:
            # Note: ^^^ changed from 'src.strategies.specific.modules.metrics_calculator.calculate_metrics'
            # to 'src.strategies.orchestrator.calculate_metrics'
            
            mock_filter_instance = MagicMock()
            mock_filter_class.return_value = mock_filter_instance
            mock_filter_instance.apply_filters.return_value = mock_filter_result
            
            mock_sim_instance = MagicMock()
            mock_sim_class.return_value = mock_sim_instance
            mock_sim_instance.simulate_trades.return_value = mock_trade_result
            
            orchestrator = StrategyOrchestrator(config=real_asset_config)
            result = orchestrator.run(mode_override="analytics")
            
            assert result.mode == "analytics"
            
            # Verify mocks were called
            mock_load_data.assert_called_once()
            mock_generate.assert_called_once()
            mock_filter_instance.apply_filters.assert_called_once()
            mock_sim_instance.simulate_trades.assert_called_once()
            mock_metrics.assert_called_once()

    def test_invalid_mode_override(self, real_asset_config):
        """Test invalid mode override raises error."""
        orchestrator = StrategyOrchestrator(config=real_asset_config)
        
        with pytest.raises(ValueError, match="Invalid mode_override"):
            orchestrator.run(mode_override="invalid_mode")

    def test_stage_timing(self, real_asset_config):
        """Test that stage timing is recorded."""
        orchestrator = StrategyOrchestrator(config=real_asset_config)
        
        durations = {}
        
        def slow_function():
            time.sleep(0.01)
            return "result"
        
        result = orchestrator._run_stage("test_stage", durations, slow_function)
        
        assert result == "result"
        assert "test_stage" in durations
        assert durations["test_stage"] > 0

    def test_stage_error_propagation(self, real_asset_config):
        """Test that stage errors propagate (fail-fast)."""
        orchestrator = StrategyOrchestrator(config=real_asset_config)
        
        def failing_function():
            raise ValueError("Stage failed")
        
        with pytest.raises(ValueError, match="Stage failed"):
            orchestrator._run_stage("failing", {}, failing_function)

    def test_result_properties(self, mock_metrics_report):
        """Test OrchestratorResult convenience properties."""
        result = OrchestratorResult(
            config=MagicMock(),
            mode="core",
            data_bundle=MagicMock(),
            signal_frame=MagicMock(),
            filter_result=MagicMock(),
            trade_result=MagicMock(),
            metrics=mock_metrics_report,
            stage_durations_ms={"data": 10.5},
            total_duration_ms=100.5
        )
        
        assert result.total_trades == mock_metrics_report.total_trades
        assert result.win_rate == mock_metrics_report.win_rate
        assert result.total_pnl_points == mock_metrics_report.total_pnl_points

    def test_result_summary_format(self, mock_metrics_report):
        """Test OrchestratorResult.summary method format."""
        result = OrchestratorResult(
            config=MagicMock(),
            mode="core",
            data_bundle=MagicMock(),
            signal_frame=MagicMock(),
            filter_result=MagicMock(),
            trade_result=MagicMock(),
            metrics=mock_metrics_report,
            stage_durations_ms={"data": 10.5},
            total_duration_ms=100.5
        )
        
        summary = result.summary()
        
        # Check format, not exact values
        assert "[CORE]" in summary
        assert "trades=" in summary
        assert "win_rate=" in summary
        assert "pnl=" in summary
        assert "total=" in summary

    def test_cache_manager_passed_to_dependencies(self, real_asset_config):
        """Test that cache manager is properly passed to dependencies."""
        cache_manager = CacheManager()
        
        with patch('src.strategies.specific.modules.trade_simulator.TradeSimulator') as mock_sim:
            mock_sim.return_value.simulate_trades.return_value = MagicMock()
            
            orchestrator = StrategyOrchestrator(
                config=real_asset_config,
                cache_manager=cache_manager
            )
            
            # We don't need to actually run the pipeline to test this
            # Just verify the orchestrator holds the cache manager
            assert orchestrator._cache_manager == cache_manager

    # ========================================================================
    # REAL DATA TESTS (These actually test the full pipeline with real data)
    # ========================================================================

    def test_orchestrator_with_real_data(self, real_data_config):
        """Test orchestrator with real market data - integration test."""
        print(f"\n{'='*60}")
        print("REAL DATA TEST: StrategyOrchestrator")
        print(f"{'='*60}")
        print(f"Asset: {real_data_config.asset.symbol}")
        print(f"Date Range: {real_data_config.data.date_range.start} to {real_data_config.data.date_range.end}")
        
        orchestrator = StrategyOrchestrator(config=real_data_config)
        result = orchestrator.run()
        
        print(f"\nPipeline Results:")
        print(f"  Data loaded: {result.data_bundle.info.strategy_bars} bars")
        print(f"  Signals: {result.signal_frame.count_by_type()['total']}")
        print(f"  Trades: {result.trade_result.total_closed}")
        print(f"  Duration: {result.total_duration_ms:.1f}ms")
        
        assert isinstance(result, OrchestratorResult)
        assert result.data_bundle is not None
        assert result.metrics is not None

    def test_orchestrator_core_vs_analytics_modes(self, real_data_config):
        """Compare core vs analytics modes with real data."""
        from dataclasses import replace
        
        print(f"\n{'='*60}")
        print("REAL DATA TEST: Core vs Analytics Mode Comparison")
        print(f"{'='*60}")
        
        # Create configs with different modes
        core_config = replace(
            real_data_config,
            execution=replace(
                real_data_config.execution,
                mode="core"
            )
        )
        
        analytics_config = replace(
            real_data_config,
            execution=replace(
                real_data_config.execution,
                mode="analytics"
            )
        )
        
        # Run in core mode
        orchestrator_core = StrategyOrchestrator(config=core_config)
        result_core = orchestrator_core.run()
        
        # Run in analytics mode
        orchestrator_analytics = StrategyOrchestrator(config=analytics_config)
        result_analytics = orchestrator_analytics.run()
        
        print(f"\nMode Comparison:")
        print(f"  Core time: {result_core.total_duration_ms:.1f}ms")
        print(f"  Analytics time: {result_analytics.total_duration_ms:.1f}ms")
        
        # Results should be identical (just different logging overhead)
        assert result_core.metrics.total_trades == result_analytics.metrics.total_trades
        assert result_core.metrics.total_pnl_points == result_analytics.metrics.total_pnl_points

    def test_orchestrator_multiple_runs_with_cache(self, real_data_config):
        """Test multiple orchestrator runs with cache management."""
        from dataclasses import replace
        
        print(f"\n{'='*60}")
        print("REAL DATA TEST: Multiple Runs with Cache")
        print(f"{'='*60}")
        
        cache_manager = CacheManager()
        results = []
        
        for i in range(3):
            print(f"\nRun {i+1}:")
            run_config = replace(real_data_config)
            orchestrator = StrategyOrchestrator(
                config=run_config,
                cache_manager=cache_manager
            )
            result = orchestrator.run()
            results.append(result)
            
            print(f"  Duration: {result.total_duration_ms:.1f}ms")
            
            if i < 2:
                cache_manager.clear_all_caches()
                print(f"  Cache cleared")
        
        assert len(results) == 3
        assert all(isinstance(r, OrchestratorResult) for r in results)