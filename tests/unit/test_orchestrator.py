"""
Unit Tests for StrategyOrchestrator
=====================================
Tests pipeline composition, stage execution, and timing.
"""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import time

from src.strategies.specific.modules.orchestrator import (
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
        """Create a mock DataBundle."""
        bundle = MagicMock(spec=DataBundle)
        bundle.info.strategy_bars = 1000
        bundle.info.total_bars = 2000
        bundle.info.cache_hit = False
        bundle.has_htf = True
        bundle.has_ltf = True
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
        
        with pytest.raises(ValueError, match="Invalid execution mode"):
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

    @patch('src.strategies.specific.modules.orchestrator.DataLoader')
    @patch('src.strategies.specific.modules.orchestrator.SignalGenerator')
    @patch('src.strategies.specific.modules.orchestrator.FilterPipeline')
    @patch('src.strategies.specific.modules.orchestrator.TradeSimulator')
    @patch('src.strategies.specific.modules.orchestrator.calculate_metrics')
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

    @patch('src.strategies.specific.modules.orchestrator.DataLoader')
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
        with patch('src.strategies.specific.modules.orchestrator.SignalGenerator') as mock_gen, \
             patch('src.strategies.specific.modules.orchestrator.FilterPipeline') as mock_filter, \
             patch('src.strategies.specific.modules.orchestrator.TradeSimulator') as mock_sim, \
             patch('src.strategies.specific.modules.orchestrator.calculate_metrics') as mock_metrics:
            
            mock_gen.return_value.generate_signals.return_value = MagicMock()
            mock_filter.return_value.apply_filters.return_value = MagicMock()
            mock_sim.return_value.simulate_trades.return_value = MagicMock()
            mock_metrics.return_value = MagicMock()
            
            orchestrator = StrategyOrchestrator(config=test_config)
            result = orchestrator.run(mode_override="analytics")
            
            assert result.mode == "analytics"

    @patch('src.strategies.specific.modules.orchestrator.DataLoader')
    def test_run_with_invalid_mode_override(
        self,
        mock_data_loader,
        test_config
    ):
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
        assert "total=101ms" in summary  # rounded

    @patch('src.strategies.specific.modules.orchestrator.DataLoader')
    @patch('src.strategies.specific.modules.orchestrator.SignalGenerator')
    def test_load_data_stage(
        self,
        mock_signal_gen,
        mock_data_loader,
        test_config,
        mock_data_bundle
    ):
        """Test the _load_data stage method."""
        mock_loader_instance = Mock()
        mock_loader_instance.load_data.return_value = mock_data_bundle
        mock_data_loader.return_value = mock_loader_instance
        
        orchestrator = StrategyOrchestrator(config=test_config)
        
        # Replace with patched version
        with patch.object(orchestrator, '_load_data', wraps=orchestrator._load_data) as wrapped:
            bundle = orchestrator._load_data("core")
            
            assert bundle == mock_data_bundle

    @patch('src.strategies.specific.modules.orchestrator.SignalGenerator')
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

    @patch('src.strategies.specific.modules.orchestrator.FilterPipeline')
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

    @patch('src.strategies.specific.modules.orchestrator.TradeSimulator')
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

    def test_cache_manager_passed_to_trade_simulator(self, test_config):
        """Test that cache manager is passed to TradeSimulator."""
        cache_manager = CacheManager()
        orchestrator = StrategyOrchestrator(
            config=test_config,
            cache_manager=cache_manager
        )
        
        with patch('src.strategies.specific.modules.orchestrator.TradeSimulator') as mock_sim:
            mock_sim.return_value.simulate_trades.return_value = MagicMock()
            
            orchestrator._simulate_trades(
                filter_result=MagicMock(),
                data_bundle=MagicMock(),
                mode="core"
            )
            
            # Check that TradeSimulator was called with cache_manager
            mock_sim.assert_called_once()
            args, kwargs = mock_sim.call_args
            assert kwargs.get('cache_manager') == cache_manager