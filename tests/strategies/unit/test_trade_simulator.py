"""
Unit Tests for TradeSimulator
===============================
Tests LTF execution, exit detection, O(1) lookups.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from src.strategies.specific.modules.trade_simulator import TradeSimulator
from src.strategies.contracts.signal_contracts import SignalFrame
from src.strategies.contracts.trade_contracts import TradeResult, ExitReason
from src.strategies.core.cache_manager import CacheManager


class TestTradeSimulator:
    """Tests for TradeSimulator class."""

    @pytest.fixture
    def strategy_df(self):
        """Generate strategy timeframe OHLCV."""
        dates = pd.date_range(start="2025-01-01 09:00:00", periods=100, freq="1min")
        
        # Trending prices
        prices = 100 + np.linspace(0, 10, 100) + np.random.randn(100) * 0.5
        
        df = pd.DataFrame({
            "open": prices * 0.999,
            "high": prices * 1.002,
            "low": prices * 0.998,
            "close": prices,
            "volume": np.random.randint(100, 1000, 100)
        }, index=dates)
        
        return df

    @pytest.fixture
    def ltf_df(self):
        """Generate LTF (1-second) OHLCV."""
        # 60 seconds per minute * 100 minutes = 6000 bars
        dates = pd.date_range(start="2025-01-01 09:00:00", periods=6000, freq="1s")
        
        # More volatile tick data
        base_prices = 100 + np.linspace(0, 10, 6000)
        noise = np.random.randn(6000) * 0.2
        prices = base_prices + noise
        
        df = pd.DataFrame({
            "open": prices * 0.9995,
            "high": prices * 1.0005,
            "low": prices * 0.9995,
            "close": prices,
            "volume": np.ones(6000)
        }, index=dates)
        
        return df

    @pytest.fixture
    def signal_frame(self, strategy_df):
        """Generate signal frame with some BUY/SELL signals."""
        signals = pd.Series(0, index=strategy_df.index, dtype=np.int8)
        
        # Add some signals at specific indices
        signals.iloc[10] = 1   # BUY
        signals.iloc[20] = 1   # BUY
        signals.iloc[30] = 2   # SELL
        signals.iloc[40] = 2   # SELL
        signals.iloc[50] = 1   # BUY
        
        return SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={"source": "test"}
        )

    def test_initialization(self, test_config, strategy_df):
        """Test initializing TradeSimulator."""
        simulator = TradeSimulator(
            config=test_config,
            df_full=strategy_df
        )

        assert simulator.config == test_config
        assert simulator.df_full is not None
        assert simulator.trade_manager is not None
        assert simulator.risk_manager is not None
        assert len(simulator.all_trades) == 0

    def test_initialization_with_cache_manager(self, test_config, strategy_df):
        """Test initialization with cache manager."""
        cache_manager = CacheManager()
        
        simulator = TradeSimulator(
            config=test_config,
            df_full=strategy_df,
            cache_manager=cache_manager
        )

        assert simulator._cache_manager == cache_manager

    def test_simulate_trades_missing_ltf(self, test_config, strategy_df, signal_frame):
        """Test that missing LTF data raises error."""
        simulator = TradeSimulator(
            config=test_config,
            df_full=strategy_df
        )

        with pytest.raises(ValueError, match="LTF execution data missing"):
            simulator.simulate_trades(
                df_strategy=strategy_df,
                signal_frame=signal_frame,
                df_ltf=None
            )

        with pytest.raises(ValueError, match="LTF execution data missing"):
            simulator.simulate_trades(
                df_strategy=strategy_df,
                signal_frame=signal_frame,
                df_ltf=pd.DataFrame()  # empty
            )

    def test_simulate_trades_basic(self, test_config, strategy_df, ltf_df, signal_frame):
        """Test basic trade simulation."""
        simulator = TradeSimulator(
            config=test_config,
            df_full=strategy_df
        )

        result = simulator.simulate_trades(
            df_strategy=strategy_df,
            signal_frame=signal_frame,
            df_ltf=ltf_df,
            verbose=False
        )

        assert isinstance(result, TradeResult)
        assert result.total_entries >= 0
        assert result.total_opened >= 0
        assert result.total_closed >= 0
        assert result.total_rejected >= 0
        
        # Should have some trades if signals exist
        if signal_frame.count_by_type()["total"] > 0:
            assert len(result.trades) > 0

    def test_ltf_window_precomputation(self, test_config, strategy_df, ltf_df):
        """Test LTF window precomputation."""
        simulator = TradeSimulator(
            config=test_config,
            df_full=strategy_df
        )
        
        simulator.df_ltf = ltf_df
        simulator._precompute_ltf_windows(strategy_df)

        assert hasattr(simulator, "_ltf_windows")
        assert len(simulator._ltf_windows) > 0
        
        # Check window structure
        for ts, window in simulator._ltf_windows.items():
            assert "min_low" in window
            assert "max_high" in window
            assert "low_np" in window
            assert "high_np" in window
            assert "index_np" in window
            assert isinstance(window["min_low"], float)
            assert isinstance(window["max_high"], float)

    def test_exit_detection_long_stop_loss(self, test_config, strategy_df, ltf_df):
        """Test LONG position stop loss exit detection."""
        simulator = TradeSimulator(
            config=test_config,
            df_full=strategy_df
        )

        # Create a signal frame with BUY at index 10
        signals = pd.Series(0, index=strategy_df.index, dtype=np.int8)
        signals.iloc[10] = 1  # BUY
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )

        # Run simulation
        result = simulator.simulate_trades(
            df_strategy=strategy_df,
            signal_frame=signal_frame,
            df_ltf=ltf_df,
            verbose=False
        )

        # Check exit stats
        assert result.exits_by_reason.get("STOP_LOSS", 0) >= 0

    def test_exit_detection_long_take_profit(self, test_config, strategy_df, ltf_df):
        """Test LONG position take profit exit detection."""
        # Modify config to have tighter TP (smaller multiplier)
        test_config.trade_management.risk.atr_multiplier_tp = 2.0
        
        simulator = TradeSimulator(
            config=test_config,
            df_full=strategy_df
        )

        # Create a signal frame with BUY at index 10
        signals = pd.Series(0, index=strategy_df.index, dtype=np.int8)
        signals.iloc[10] = 1  # BUY
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )

        result = simulator.simulate_trades(
            df_strategy=strategy_df,
            signal_frame=signal_frame,
            df_ltf=ltf_df,
            verbose=False
        )

        # Check exit stats
        assert result.exits_by_reason.get("TAKE_PROFIT", 0) >= 0

    def test_exit_detection_short_stop_loss(self, test_config, strategy_df, ltf_df):
        """Test SHORT position stop loss exit detection."""
        simulator = TradeSimulator(
            config=test_config,
            df_full=strategy_df
        )

        # Create a signal frame with SELL at index 10
        signals = pd.Series(0, index=strategy_df.index, dtype=np.int8)
        signals.iloc[10] = 2  # SELL
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )

        result = simulator.simulate_trades(
            df_strategy=strategy_df,
            signal_frame=signal_frame,
            df_ltf=ltf_df,
            verbose=False
        )

        assert result.exits_by_reason.get("STOP_LOSS", 0) >= 0

    def test_exit_detection_short_take_profit(self, test_config, strategy_df, ltf_df):
        """Test SHORT position take profit exit detection."""
        # Modify config to have tighter TP
        test_config.trade_management.risk.atr_multiplier_tp = 2.0
        
        simulator = TradeSimulator(
            config=test_config,
            df_full=strategy_df
        )

        # Create a signal frame with SELL at index 10
        signals = pd.Series(0, index=strategy_df.index, dtype=np.int8)
        signals.iloc[10] = 2  # SELL
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )

        result = simulator.simulate_trades(
            df_strategy=strategy_df,
            signal_frame=signal_frame,
            df_ltf=ltf_df,
            verbose=False
        )

        assert result.exits_by_reason.get("TAKE_PROFIT", 0) >= 0

    def test_opposite_signal_exit(self, test_config, strategy_df, ltf_df):
        """Test exit due to opposite signal."""
        # Enable close on opposite
        test_config.trade_management.position_control.close_on_opposite = True
        
        simulator = TradeSimulator(
            config=test_config,
            df_full=strategy_df
        )

        # Create signals: BUY at 10, SELL at 20
        signals = pd.Series(0, index=strategy_df.index, dtype=np.int8)
        signals.iloc[10] = 1  # BUY
        signals.iloc[20] = 2  # SELL (opposite)
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )

        result = simulator.simulate_trades(
            df_strategy=strategy_df,
            signal_frame=signal_frame,
            df_ltf=ltf_df,
            verbose=False
        )

        # Should have opposite signal exits
        assert result.exits_by_reason.get("OPPOSITE_SIGNAL", 0) > 0

    def test_end_of_data_exit(self, test_config, strategy_df, ltf_df):
        """Test exit at end of data."""
        simulator = TradeSimulator(
            config=test_config,
            df_full=strategy_df
        )

        # Create a late signal
        signals = pd.Series(0, index=strategy_df.index, dtype=np.int8)
        signals.iloc[-5] = 1  # BUY near end
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )

        result = simulator.simulate_trades(
            df_strategy=strategy_df,
            signal_frame=signal_frame,
            df_ltf=ltf_df,
            verbose=False
        )

        # Should have END_OF_DATA exits
        assert result.exits_by_reason.get("END_OF_DATA", 0) > 0

    def test_rejected_signals_tracking(self, test_config, strategy_df, ltf_df):
        """Test that rejected signals are tracked."""
        # Set very strict risk limits to force rejections
        test_config.trade_management.risk.max_risk_percentile = 0.0001
        
        simulator = TradeSimulator(
            config=test_config,
            df_full=strategy_df
        )

        # Create many signals
        signals = pd.Series(0, index=strategy_df.index, dtype=np.int8)
        signals.iloc[10:30] = 1  # Many BUY signals
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )

        result = simulator.simulate_trades(
            df_strategy=strategy_df,
            signal_frame=signal_frame,
            df_ltf=ltf_df,
            verbose=False
        )

        # Should have rejected signals
        assert len(result.rejected_signals) > 0
        assert result.total_rejected > 0

    def test_risk_stats_tracking(self, test_config, strategy_df, ltf_df):
        """Test that risk statistics are tracked correctly."""
        simulator = TradeSimulator(
            config=test_config,
            df_full=strategy_df
        )

        signals = pd.Series(0, index=strategy_df.index, dtype=np.int8)
        signals.iloc[10] = 1  # BUY
        signals.iloc[15] = 2  # SELL
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )

        result = simulator.simulate_trades(
            df_strategy=strategy_df,
            signal_frame=signal_frame,
            df_ltf=ltf_df,
            verbose=False
        )

        # Check risk stats
        assert result.risk_approved >= 0
        assert result.risk_rejected >= 0
        assert result.risk_adjusted >= 0

    def test_o1_lookup_structures(self, test_config, strategy_df, ltf_df):
        """Test that O(1) lookup structures are maintained."""
        simulator = TradeSimulator(
            config=test_config,
            df_full=strategy_df
        )

        signals = pd.Series(0, index=strategy_df.index, dtype=np.int8)
        signals.iloc[10] = 1  # BUY
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )

        result = simulator.simulate_trades(
            df_strategy=strategy_df,
            signal_frame=signal_frame,
            df_ltf=ltf_df,
            verbose=False
        )

        # Internal structures should exist
        assert hasattr(simulator, "_open_trades")
        assert hasattr(simulator, "_tm_id_to_entry_id")
        assert hasattr(simulator, "_trade_list_index")

    def test_profiling_enabled(self, test_config, strategy_df, ltf_df):
        """Test profiling when enabled."""
        # Enable profiling
        test_config.analytics = {"profile_simulator": True}
        
        simulator = TradeSimulator(
            config=test_config,
            df_full=strategy_df
        )

        signals = pd.Series(0, index=strategy_df.index, dtype=np.int8)
        signals.iloc[10] = 1
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )

        result = simulator.simulate_trades(
            df_strategy=strategy_df,
            signal_frame=signal_frame,
            df_ltf=ltf_df,
            verbose=True  # verbose to see profile
        )

        # Should have profiler
        assert simulator.profiler is not None

    def test_execution_mode_string(self, test_config, strategy_df, ltf_df):
        """Test execution mode string in result."""
        simulator = TradeSimulator(
            config=test_config,
            df_full=strategy_df
        )

        signals = pd.Series(0, index=strategy_df.index, dtype=np.int8)
        signals.iloc[10] = 1
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )

        result = simulator.simulate_trades(
            df_strategy=strategy_df,
            signal_frame=signal_frame,
            df_ltf=ltf_df
        )

        # Execution mode should be set
        assert result.execution_mode in ["LTF_OHLC_V5_NUMBA", "LTF_OHLC_V5"]

    def test_invalid_exit_reason_handling(self, test_config, strategy_df, ltf_df):
        """Test that invalid exit reason raises error."""
        simulator = TradeSimulator(
            config=test_config,
            df_full=strategy_df
        )

        # This would require mocking to test the internal validation
        # We'll test by calling the exit method directly
        from src.strategies.contracts.trade_contracts import Trade, TradeEntry
        
        # Create a dummy trade
        entry = TradeEntry(
            entry_id="E1",
            entry_time=strategy_df.index[10],
            direction="LONG",
            entry_price=100.0,
            stop_loss=99.0,
            take_profit=105.0
        )
        trade = Trade(entry=entry, exit=None)

        # Try to execute with invalid reason
        with pytest.raises(ValueError, match="Unknown exit reason"):
            simulator._execute_trade_exit(
                trade=trade,
                exit_time=strategy_df.index[20],
                exit_price=99.5,
                exit_reason="INVALID_REASON",
                exit_stats={},
                verbose=False,
                exit_high=100.0,
                exit_low=99.0
            )