"""
Unit Tests for StrategyOrchestrator
=====================================
Tests pipeline composition, stage execution, and timing.
Includes real data tests using actual market data.
"""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
import time

from src.strategies.orchestrator import (  # Correct import path
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
    def mock_data_bundle(self):
        """Create a mock DataBundle with proper structure."""
        bundle = MagicMock(spec=DataBundle)
        
        # Create a mock DataInfo
        mock_info = MagicMock(spec=DataInfo)
        mock_info.strategy_bars = 1000
        mock_info.total_bars = 2000
        mock_info.cache_hit = False
        
        # Set up bundle attributes
        bundle.info = mock_info
        bundle.has_htf = True
        bundle.has_ltf = True
        bundle.strategy = MagicMock()
        bundle.full = MagicMock()
        bundle.ltf = MagicMock()
        
        return bundle

    @pytest.fixture
    def mock_signal_frame(self):
        """Create a mock SignalFrame."""
        frame = MagicMock(spec=SignalFrame)
        counts = {"buy": 10, "sell": 8, "total": 18}
        frame.count_by_type.return_value = counts
        return frame

    @pytest.fixture
    def mock_filter_result(self):
        """Create a mock FilterPipelineResult."""
        result = MagicMock(spec=FilterPipelineResult)
        result.raw_count = 18
        result.final_count = 12
        result.pass_rate = 66.7
        result.final_signals = MagicMock()
        return result

    @pytest.fixture
    def mock_trade_result(self):
        """Create a mock TradeResult."""
        result = MagicMock(spec=TradeResult)
        result.total_opened = 12
        result.total_closed = 10
        result.win_count = 6
        result.loss_count = 4
        result.total_pnl_points = 45.5
        result.execution_mode = "LTF_OHLC_V5"
        return result

    @pytest.fixture
    def mock_metrics_report(self):
        """Create a mock MetricsReport."""
        metrics = MagicMock(spec=MetricsReport)
        metrics.total_trades = 10
        metrics.win_rate = 60.0
        metrics.total_pnl_points = 45.5
        return metrics

    def test_initialization_with_config(self, test_config):
        """Test initializing orchestrator with config."""
        orchestrator = StrategyOrchestrator(config=test_config)
        
        assert orchestrator._config == test_config
        assert orchestrator._mode == test_config.execution.mode
        assert orchestrator._cache_manager is not None

    def test_initialization_with_cache_manager(self, test_config):
        """Test initialization with provided cache manager."""
        cache_manager = CacheManager()
        orchestrator = StrategyOrchestrator(
            config=test_config,
            cache_manager=cache_manager
        )
        
        assert orchestrator._cache_manager == cache_manager

    def test_initialization_invalid_mode(self, base_config_dict):
        """Test initialization with invalid mode."""
        from src.config.config_schema import StrategyConfig
        
        base_config_dict["execution"]["mode"] = "invalid_mode"
        
        # The error message comes from ExecutionConfig validation
        expected_msg = "Invalid execution.mode 'invalid_mode'. Must be one of: {'core', 'analytics'}."
        
        with pytest.raises(ValueError, match=expected_msg):
            config = StrategyConfig.from_dict(base_config_dict)
            StrategyOrchestrator(config=config)

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

    @patch('src.strategies.specific.modules.data_loader.DataLoader')
    @patch('src.strategies.specific.modules.signal_generator.SignalGenerator')
    @patch('src.strategies.specific.modules.filter_pipeline.FilterPipeline')
    @patch('src.strategies.specific.modules.trade_simulator.TradeSimulator')
    @patch('src.strategies.specific.modules.metrics_calculator.calculate_metrics')
    def test_run_full_pipeline(
        self,
        mock_calc_metrics,
        mock_trade_sim,
        mock_filter_pipeline,
        mock_signal_gen,
        mock_data_loader,
        test_config,
        mock_data_bundle,
        mock_signal_frame,
        mock_filter_result,
        mock_trade_result,
        mock_metrics_report
    ):
        """Test running full pipeline."""
        # Setup mocks
        mock_loader_instance = Mock()
        mock_loader_instance.load_data.return_value = mock_data_bundle
        mock_data_loader.return_value = mock_loader_instance
        
        mock_gen_instance = Mock()
        mock_gen_instance.generate_signals.return_value = mock_signal_frame
        mock_signal_gen.return_value = mock_gen_instance
        
        mock_filter_instance = Mock()
        mock_filter_instance.apply_filters.return_value = mock_filter_result
        mock_filter_pipeline.return_value = mock_filter_instance
        
        mock_sim_instance = Mock()
        mock_sim_instance.simulate_trades.return_value = mock_trade_result
        mock_trade_sim.return_value = mock_sim_instance
        
        mock_calc_metrics.return_value = mock_metrics_report
        
        orchestrator = StrategyOrchestrator(config=test_config)
        result = orchestrator.run()
        
        assert isinstance(result, OrchestratorResult)
        assert result.config == test_config
        assert result.mode == test_config.execution.mode
        assert result.data_bundle == mock_data_bundle
        assert result.signal_frame == mock_signal_frame
        assert result.filter_result == mock_filter_result
        assert result.trade_result == mock_trade_result
        assert result.metrics == mock_metrics_report
        assert result.total_duration_ms > 0
        assert "data" in result.stage_durations_ms
        assert "signals" in result.stage_durations_ms
        assert "filters" in result.stage_durations_ms
        assert "trades" in result.stage_durations_ms
        assert "metrics" in result.stage_durations_ms

    @patch('src.strategies.specific.modules.data_loader.DataLoader')
    def test_run_with_mode_override(
        self,
        mock_data_loader,
        test_config,
        mock_data_bundle
    ):
        """Test running with mode override."""
        # Setup mocks
        mock_loader_instance = Mock()
        mock_loader_instance.load_data.return_value = mock_data_bundle
        mock_data_loader.return_value = mock_loader_instance
        
        # Mock other stages to return quickly
        with patch('src.strategies.specific.modules.signal_generator.SignalGenerator') as mock_gen, \
             patch('src.strategies.specific.modules.filter_pipeline.FilterPipeline') as mock_filter, \
             patch('src.strategies.specific.modules.trade_simulator.TradeSimulator') as mock_sim, \
             patch('src.strategies.specific.modules.metrics_calculator.calculate_metrics') as mock_metrics:
            
            mock_gen.return_value.generate_signals.return_value = MagicMock()
            mock_filter.return_value.apply_filters.return_value = MagicMock()
            mock_sim.return_value.simulate_trades.return_value = MagicMock()
            mock_metrics.return_value = MagicMock()
            
            orchestrator = StrategyOrchestrator(config=test_config)
            result = orchestrator.run(mode_override="analytics")
            
            assert result.mode == "analytics"

    def test_run_with_invalid_mode_override(self, test_config):
        """Test running with invalid mode override."""
        orchestrator = StrategyOrchestrator(config=test_config)
        
        with pytest.raises(ValueError, match="Invalid mode_override"):
            orchestrator.run(mode_override="invalid_mode")

    def test_stage_timing(self, test_config):
        """Test that stage timing is recorded."""
        orchestrator = StrategyOrchestrator(config=test_config)
        
        durations = {}
        
        # Test _run_stage method directly
        def slow_function():
            time.sleep(0.01)
            return "result"
        
        result = orchestrator._run_stage("test_stage", durations, slow_function)
        
        assert result == "result"
        assert "test_stage" in durations
        assert durations["test_stage"] > 0

    def test_stage_error_propagation(self, test_config):
        """Test that stage errors propagate."""
        orchestrator = StrategyOrchestrator(config=test_config)
        
        def failing_function():
            raise ValueError("Stage failed")
        
        with pytest.raises(ValueError, match="Stage failed"):
            orchestrator._run_stage("failing", {}, failing_function)

    def test_result_properties(self, mock_metrics_report):
        """Test OrchestratorResult properties."""
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

    def test_result_summary(self, mock_metrics_report):
        """Test OrchestratorResult.summary method."""
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
        
        assert "[CORE]" in summary
        assert "trades=10" in summary
        assert "win_rate=60.0%" in summary
        assert "pnl=+45.5pts" in summary
        # Check that total is either 100ms or 101ms (due to rounding)
        assert "total=100ms" in summary or "total=101ms" in summary

    @patch('src.strategies.specific.modules.data_loader.DataLoader')
    def test_load_data_stage(
        self,
        mock_data_loader,
        test_config,
        mock_data_bundle
    ):
        """Test the _load_data stage method."""
        mock_loader_instance = Mock()
        mock_loader_instance.load_data.return_value = mock_data_bundle
        mock_data_loader.return_value = mock_loader_instance
        
        orchestrator = StrategyOrchestrator(config=test_config)
        
        bundle = orchestrator._load_data("core")
        
        assert bundle == mock_data_bundle

    @patch('src.strategies.specific.modules.signal_generator.SignalGenerator')
    def test_generate_signals_stage(
        self,
        mock_signal_gen,
        test_config,
        mock_data_bundle,
        mock_signal_frame
    ):
        """Test the _generate_signals stage method."""
        mock_gen_instance = Mock()
        mock_gen_instance.generate_signals.return_value = mock_signal_frame
        mock_signal_gen.return_value = mock_gen_instance
        
        orchestrator = StrategyOrchestrator(config=test_config)
        
        frame = orchestrator._generate_signals(mock_data_bundle, "core")
        
        assert frame == mock_signal_frame
        mock_gen_instance.generate_signals.assert_called_once_with(mock_data_bundle)

    @patch('src.strategies.specific.modules.filter_pipeline.FilterPipeline')
    def test_run_filters_stage(
        self,
        mock_filter_pipeline,
        test_config,
        mock_data_bundle,
        mock_signal_frame,
        mock_filter_result
    ):
        """Test the _run_filters stage method."""
        mock_filter_instance = Mock()
        mock_filter_instance.apply_filters.return_value = mock_filter_result
        mock_filter_pipeline.return_value = mock_filter_instance
        
        orchestrator = StrategyOrchestrator(config=test_config)
        
        result = orchestrator._run_filters(
            signal_frame=mock_signal_frame,
            data_bundle=mock_data_bundle,
            mode="core"
        )
        
        assert result == mock_filter_result
        mock_filter_instance.apply_filters.assert_called_once_with(
            signal_frame=mock_signal_frame,
            df=mock_data_bundle.strategy,
            mode="core"
        )

    @patch('src.strategies.specific.modules.trade_simulator.TradeSimulator')
    def test_simulate_trades_stage(
        self,
        mock_trade_sim,
        test_config,
        mock_data_bundle,
        mock_filter_result,
        mock_trade_result
    ):
        """Test the _simulate_trades stage method."""
        mock_sim_instance = Mock()
        mock_sim_instance.simulate_trades.return_value = mock_trade_result
        mock_trade_sim.return_value = mock_sim_instance
        
        # Create proper DataFrames with DatetimeIndex for RiskManager
        import pandas as pd
        dates = pd.date_range(start="2025-01-01", periods=100, freq="1min")
        df = pd.DataFrame({
            "open": [100.0] * 100,
            "high": [101.0] * 100,
            "low": [99.0] * 100,
            "close": [100.5] * 100,
            "volume": [1000] * 100
        }, index=dates)
        
        # Use the mock_data_bundle fixture but override its DataFrames
        mock_data_bundle.strategy = df
        mock_data_bundle.full = df
        mock_data_bundle.ltf = df
        
        orchestrator = StrategyOrchestrator(config=test_config)
        
        result = orchestrator._simulate_trades(
            filter_result=mock_filter_result,
            data_bundle=mock_data_bundle,
            mode="analytics"
        )
        
        assert result == mock_trade_result
        mock_sim_instance.simulate_trades.assert_called_once_with(
            df_strategy=mock_data_bundle.strategy,
            signal_frame=mock_filter_result.final_signals,
            verbose=True,  # analytics mode
            progressive_tracker=None,
            signal_id_map=None,
            df_ltf=mock_data_bundle.ltf
        )

    def test_cache_manager_passed_to_trade_simulator(self, test_config, mock_data_bundle):
        """Test that cache manager is passed to TradeSimulator."""
        cache_manager = CacheManager()
        orchestrator = StrategyOrchestrator(
            config=test_config,
            cache_manager=cache_manager
        )
        
        # Create proper DataFrames with DatetimeIndex for RiskManager
        import pandas as pd
        dates = pd.date_range(start="2025-01-01", periods=100, freq="1min")
        df = pd.DataFrame({
            "open": [100.0] * 100,
            "high": [101.0] * 100,
            "low": [99.0] * 100,
            "close": [100.5] * 100,
            "volume": [1000] * 100
        }, index=dates)
        
        # Configure the mock_data_bundle with proper DataFrames
        mock_data_bundle.strategy = df
        mock_data_bundle.full = df
        mock_data_bundle.ltf = df
        
        # Create mock filter result with final_signals
        mock_filter = MagicMock()
        mock_filter.final_signals = MagicMock()
        
        with patch('src.strategies.specific.modules.trade_simulator.TradeSimulator') as mock_sim:
            mock_sim.return_value.simulate_trades.return_value = MagicMock()
            
            orchestrator._simulate_trades(
                filter_result=mock_filter,
                data_bundle=mock_data_bundle,
                mode="core"
            )
            
            # Check that TradeSimulator was called with cache_manager
            mock_sim.assert_called_once()
            args, kwargs = mock_sim.call_args
            assert kwargs.get('cache_manager') == cache_manager

    # ========================================================================
    # REAL DATA TESTS
    # ========================================================================

    def test_orchestrator_with_real_data(self, real_data_config):
        """Test orchestrator with real market data."""
        print(f"\n{'='*60}")
        print("REAL DATA TEST: StrategyOrchestrator")
        print(f"{'='*60}")
        print(f"Asset: {real_data_config.asset.symbol}")
        print(f"Date Range: {real_data_config.data.date_range.start} to {real_data_config.data.date_range.end}")
        print(f"Mode: {real_data_config.execution.mode}")
        
        # Create orchestrator
        orchestrator = StrategyOrchestrator(config=real_data_config)
        
        # Run the pipeline
        result = orchestrator.run()
        
        print(f"\nPipeline Results:")
        print(f"  Data loaded: {result.data_bundle.info.strategy_bars} bars")
        print(f"  Cache hit: {result.data_bundle.info.cache_hit}")
        
        # Signal stats
        signal_counts = result.signal_frame.count_by_type()
        print(f"  Signals generated: {signal_counts['total']} total ({signal_counts['buy']} BUY, {signal_counts['sell']} SELL)")
        
        # Filter stats
        print(f"  Filters applied: {result.filter_result.raw_count} → {result.filter_result.final_count} signals")
        print(f"  Pass rate: {result.filter_result.pass_rate:.1f}%")
        
        # Trade stats
        print(f"  Trades simulated: {result.trade_result.total_opened} opened, {result.trade_result.total_closed} closed")
        print(f"  Win rate: {result.metrics.win_rate:.1f}%")
        print(f"  Total P&L: {result.metrics.total_pnl_points:+.2f} pts")
        print(f"  Profit factor: {result.metrics.profit_factor:.2f}")
        print(f"  Max drawdown: {result.metrics.max_drawdown:.2f} pts")
        
        # Stage timings
        print(f"\nStage Timings:")
        for stage, duration in result.stage_durations_ms.items():
            print(f"  {stage:10s}: {duration:6.1f}ms")
        print(f"  {'total':10s}: {result.total_duration_ms:6.1f}ms")
        
        # Basic assertions
        assert isinstance(result, OrchestratorResult)
        assert result.data_bundle is not None
        assert result.signal_frame is not None
        assert result.filter_result is not None
        assert result.trade_result is not None
        assert result.metrics is not None
        assert result.total_duration_ms > 0

    def test_orchestrator_core_vs_analytics_modes(self, real_data_config):
        """Compare orchestrator performance between core and analytics modes."""
        from dataclasses import replace
        
        print(f"\n{'='*60}")
        print("REAL DATA TEST: Core vs Analytics Mode Comparison")
        print(f"{'='*60}")
        
        # Create configs with different modes using replace (avoid frozen instance issue)
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
        print(f"{'Metric':20} | {'Core':>12} | {'Analytics':>12} | {'Diff':>10}")
        print(f"{'-'*20}-+-{'-'*12}-+-{'-'*12}-+-{'-'*10}")
        
        # Compare key metrics
        metrics_to_compare = [
            ("Total Time", 
             lambda r: r.total_duration_ms, 
             "{:.1f}ms"),
            ("Data Time", 
             lambda r: r.stage_durations_ms.get('data', 0), 
             "{:.1f}ms"),
            ("Signal Time", 
             lambda r: r.stage_durations_ms.get('signals', 0), 
             "{:.1f}ms"),
            ("Filter Time", 
             lambda r: r.stage_durations_ms.get('filters', 0), 
             "{:.1f}ms"),
            ("Trade Time", 
             lambda r: r.stage_durations_ms.get('trades', 0), 
             "{:.1f}ms"),
            ("Metrics Time", 
             lambda r: r.stage_durations_ms.get('metrics', 0), 
             "{:.1f}ms"),
            ("Total Trades", 
             lambda r: r.metrics.total_trades, 
             "{:d}"),
            ("Win Rate", 
             lambda r: r.metrics.win_rate, 
             "{:.1f}%"),
            ("Total P&L", 
             lambda r: r.metrics.total_pnl_points, 
             "{:+.1f}"),
        ]
        
        for name, func, fmt in metrics_to_compare:
            val1 = func(result_core)
            val2 = func(result_analytics)
            if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
                diff = val2 - val1
                print(f"{name:20} | {fmt.format(val1):>12} | {fmt.format(val2):>12} | {diff:>+10.1f}")
            else:
                print(f"{name:20} | {str(val1):>12} | {str(val2):>12} | {'N/A':>10}")
        
        # Basic assertions
        assert result_core.metrics.total_trades == result_analytics.metrics.total_trades
        assert result_core.metrics.total_pnl_points == result_analytics.metrics.total_pnl_points

    def test_orchestrator_multiple_runs_with_cache(self, real_data_config):
        """Test multiple orchestrator runs with cache clearing."""
        from dataclasses import replace
        
        print(f"\n{'='*60}")
        print("REAL DATA TEST: Multiple Runs with Cache")
        print(f"{'='*60}")
        
        cache_manager = CacheManager()
        results = []
        
        # Run multiple times
        for i in range(3):
            print(f"\nRun {i+1}:")
            # Create fresh config each time (avoid frozen instance issues)
            run_config = replace(real_data_config)
            orchestrator = StrategyOrchestrator(
                config=run_config,
                cache_manager=cache_manager
            )
            result = orchestrator.run()
            results.append(result)
            
            print(f"  Duration: {result.total_duration_ms:.1f}ms")
            print(f"  Cache stats: {cache_manager}")
            
            # Clear cache between runs (except last)
            if i < 2:
                cache_manager.clear_all_caches()
                print(f"  Cache cleared")
        
        # First run should be slower (cache miss), subsequent runs faster (cache hit)
        # But with cache clearing between runs, they should all be similar
        print(f"\nCache Statistics:")
        stats = cache_manager.get_stats()
        for cache_name, cache_stats in stats.items():
            print(f"  {cache_name}: {cache_stats['hits']} hits, {cache_stats['misses']} misses")
        
        assert len(results) == 3
        assert all(isinstance(r, OrchestratorResult) for r in results)

    def test_orchestrator_error_handling_missing_ltf(self, real_data_config):
        """Test orchestrator error handling when LTF data is missing."""
        from dataclasses import replace
        
        print(f"\n{'='*60}")
        print("REAL DATA TEST: Missing LTF Error Handling")
        print(f"{'='*60}")
        
        # Create a config with LTF path set to None
        modified_config = replace(
            real_data_config,
            data=replace(
                real_data_config.data,
                paths=replace(
                    real_data_config.data.paths,
                    ltf_ohlcv=None
                )
            )
        )
        
        orchestrator = StrategyOrchestrator(config=modified_config)
        
        # Should raise error about missing LTF data
        with pytest.raises(ValueError, match="LTF execution data missing"):
            orchestrator.run()
        
        print(f"✓ Correctly raised error for missing LTF data")