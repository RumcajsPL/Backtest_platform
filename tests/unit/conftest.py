"""
pytest Configuration and Shared Fixtures
=========================================
Provides fixtures for all unit tests including:
- Test data generation
- Mock strategy configurations
- Sample DataFrames
- Temporary directories
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any

from src.config.config_schema import (
    StrategyConfig, AssetConfig, DataConfig, DataPathsConfig,
    DateRangeConfig, ExecutionConfig, TradeManagementConfig,
    SpreadConfig, RiskConfig, PositionControlConfig,
    FilterPipelineConfig, FilterConfig, OutputConfig
)
from src.strategies.contracts.data_contracts import DataBundle, DataInfo
from src.strategies.contracts.signal_contracts import SignalFrame
from src.strategies.core.cache_manager import CacheManager
from src.strategies.contracts.filter_contracts import FilterMetadata, FilterResult, FilterStatus
from src.strategies.contracts.signal_contracts import SignalFrame
from src.config.config_schema import TimeFilterConfig


# ============================================================================
# Test Data Generation
# ============================================================================

@pytest.fixture
def sample_ohlcv_data() -> pd.DataFrame:
    """Generate sample OHLCV data for testing."""
    dates = pd.date_range(start="2025-01-01", periods=1000, freq="1min")
    
    # Generate realistic price movements
    np.random.seed(42)
    returns = np.random.randn(1000) * 0.0001
    prices = 100 * (1 + np.cumsum(returns))
    
    df = pd.DataFrame({
        "open": prices * (1 + np.random.randn(1000) * 0.001),
        "high": prices * (1 + np.abs(np.random.randn(1000)) * 0.002),
        "low": prices * (1 - np.abs(np.random.randn(1000)) * 0.002),
        "close": prices,
        "volume": np.random.randint(100, 1000, 1000)
    }, index=dates)
    
    # Ensure OHLC integrity
    df["high"] = df[["open", "high", "close"]].max(axis=1)
    df["low"] = df[["open", "low", "close"]].min(axis=1)
    
    return df


@pytest.fixture
def sample_htf_data() -> pd.DataFrame:
    """Generate sample higher timeframe data."""
    dates = pd.date_range(start="2025-01-01", periods=200, freq="1H")
    
    prices = 100 + np.cumsum(np.random.randn(200) * 0.001)
    
    return pd.DataFrame({
        "open": prices * (1 + np.random.randn(200) * 0.001),
        "high": prices * (1 + np.abs(np.random.randn(200)) * 0.002),
        "low": prices * (1 - np.abs(np.random.randn(200)) * 0.002),
        "close": prices,
        "volume": np.random.randint(1000, 10000, 200)
    }, index=dates)


@pytest.fixture
def sample_ltf_data() -> pd.DataFrame:
    """Generate sample lower timeframe (1s) data."""
    dates = pd.date_range(start="2025-01-01 09:00:00", periods=5000, freq="1s")
    
    # Generate more volatile tick data
    prices = 100 + np.cumsum(np.random.randn(5000) * 0.0005)
    
    df = pd.DataFrame({
        "open": prices * (1 + np.random.randn(5000) * 0.0005),
        "high": prices * (1 + np.abs(np.random.randn(5000)) * 0.001),
        "low": prices * (1 - np.abs(np.random.randn(5000)) * 0.001),
        "close": prices,
        "volume": np.ones(5000)
    }, index=dates)
    
    return df


@pytest.fixture
def sample_artf_data() -> pd.DataFrame:
    """Generate sample monthly (ARTF) data."""
    dates = pd.date_range(start="2020-01-01", periods=60, freq="ME")
    
    prices = 100 + np.cumsum(np.random.randn(60) * 1.0)
    
    return pd.DataFrame({
        "open": prices * (1 + np.random.randn(60) * 0.01),
        "high": prices * (1 + np.abs(np.random.randn(60)) * 0.02),
        "low": prices * (1 - np.abs(np.random.randn(60)) * 0.02),
        "close": prices,
        "volume": np.random.randint(100000, 1000000, 60)
    }, index=dates)


# ============================================================================
# Test DataBundle Fixtures
# ============================================================================

@pytest.fixture
def sample_data_bundle(sample_ohlcv_data, sample_htf_data, sample_ltf_data, sample_artf_data) -> DataBundle:
    """Create a complete DataBundle with sample data."""
    info = DataInfo(
        total_bars=len(sample_ohlcv_data),
        strategy_bars=len(sample_ohlcv_data),
        htf_bars=len(sample_htf_data),
        ltf_bars=len(sample_ltf_data),
        artf_bars=len(sample_artf_data),
        date_range=(
            sample_ohlcv_data.index[0].to_pydatetime(),
            sample_ohlcv_data.index[-1].to_pydatetime()
        ),
        ltf_timeframe="1s",
        artf_timeframe="1ME",
        cache_hit=False
    )
    
    return DataBundle(
        full=sample_ohlcv_data,
        strategy=sample_ohlcv_data,
        htf=sample_htf_data,
        ltf=sample_ltf_data,
        artf=sample_artf_data,
        info=info
    )


@pytest.fixture
def sample_signal_frame() -> SignalFrame:
    """Create a sample SignalFrame with mixed signals."""
    dates = pd.date_range(start="2025-01-01", periods=100, freq="1min")
    
    # Generate random signals (1=BUY, 2=SELL, 0=none)
    np.random.seed(42)
    signals = np.zeros(100, dtype=np.int8)
    buy_indices = np.random.choice(100, 10, replace=False)
    sell_indices = np.random.choice(100, 10, replace=False)
    signals[buy_indices] = 1
    signals[sell_indices] = 2
    
    return SignalFrame(
        signals=pd.Series(signals, index=dates, dtype="int8"),
        indicator_data=None,
        signal_metadata={"source": "test", "mode": "core"}
    )


# ============================================================================
# Test Configuration Fixtures
# ============================================================================

@pytest.fixture
def base_config_dict() -> Dict[str, Any]:
    """Base configuration dictionary for testing."""
    return {
        "asset": {
            "symbol": "TEST",
            "pip_size": 0.0001,
            "point_size": 0.00001
        },
        "data": {
            "paths": {
                "strategy_ohlcv": "data/test/strategy.parquet",
                "htf_ohlcv": "data/test/htf.parquet",
                "ltf_ohlcv": "data/test/ltf.parquet",
                "artf_ohlcv": "data/test/artf.parquet"
            },
            "date_range": {
                "start": "2025-01-01 00:00:00",
                "end": "2025-01-31 23:59:59"
            },
            "timezone": "CET",
            "htf_period": "1H",
            "ltf_timeframe": "1s",
            "artf_timeframe": "1ME"
        },
        "execution": {
            "mode": "core"
        },
        "trade_management": {
            "spread": {
                "enabled": True,
                "config_path": "configs/spreads/broker_spreads.yaml"
            },
            "risk": {
                "atr_length": 14,
                "atr_multiplier_sl": 1.4,
                "atr_multiplier_tp": 7.98,
                "max_risk_percentile": 0.1,
                "tp_mode": "rr_ratio",
                "risk_to_reward_ratio": 5.7
            },
            "position_control": {
                "pyramiding_enabled": False,
                "close_on_opposite": False,
                "max_positions": 1
            }
        },
        "filters": {
            "time_filters": {
                "time_filter": {
                    "enabled": True,
                    "session_start": {"hour": 8, "minute": 30},
                    "session_end": {"hour": 20, "minute": 30},
                    "excluded_days": []
                }
            },
            "filter_sequence": ["rsi_filter"],
            "technical_filters": {
                "rsi_filter": {
                    "enabled": True,
                    "length": 14,
                    "overbought": 70,
                    "oversold": 30
                }
            },
            "default_error_strategy": "pass_through"
        },
        "output": {
            "reports": {
                "enabled": True,
                "output_dir": "tests/reports",
                "theme": "dark",
                "brand_name": "TestStrategy"
            },
            "logging": {
                "level": "INFO",
                "output_dir": "tests/diagnostic_output"
            }
        }
    }


@pytest.fixture
def test_config(base_config_dict) -> StrategyConfig:
    """Create a valid StrategyConfig for testing."""
    return StrategyConfig.from_dict(base_config_dict)


@pytest.fixture
def cache_manager() -> CacheManager:
    """Create a fresh CacheManager for testing."""
    return CacheManager()


# ============================================================================
# Temporary Directory Fixtures
# ============================================================================

@pytest.fixture
def tmp_test_dir(tmp_path) -> Path:
    """Create a temporary directory for test outputs."""
    test_dir = tmp_path / "test_output"
    test_dir.mkdir()
    return test_dir


@pytest.fixture
def tmp_cache_dir(tmp_path) -> Path:
    """Create a temporary cache directory."""
    cache_dir = tmp_path / "test_cache"
    cache_dir.mkdir()
    return cache_dir

# ============================================================================
# Filter Test Fixtures
# ============================================================================

@pytest.fixture
def sample_signal_frame_with_mixed_signals(sample_ohlcv_data):
    """Create SignalFrame with mix of BUY/SELL signals at specific positions."""
    signals = pd.Series(0, index=sample_ohlcv_data.index, dtype=np.int8)
    
    # Add signals at various positions
    signals.iloc[10] = 1   # BUY
    signals.iloc[20] = 1   # BUY
    signals.iloc[30] = 2   # SELL
    signals.iloc[40] = 2   # SELL
    signals.iloc[50] = 1   # BUY
    signals.iloc[60] = 2   # SELL
    signals.iloc[70] = 1   # BUY
    signals.iloc[80] = 2   # SELL
    
    return SignalFrame(
        signals=signals,
        indicator_data=None,
        signal_metadata={"source": "test"}
    )


@pytest.fixture
def sample_signal_frame_all_buy(sample_ohlcv_data):
    """Create SignalFrame with only BUY signals."""
    signals = pd.Series(0, index=sample_ohlcv_data.index, dtype=np.int8)
    signals.iloc[10:20] = 1
    return SignalFrame(
        signals=signals,
        indicator_data=None,
        signal_metadata={"source": "test"}
    )


@pytest.fixture
def sample_signal_frame_all_sell(sample_ohlcv_data):
    """Create SignalFrame with only SELL signals."""
    signals = pd.Series(0, index=sample_ohlcv_data.index, dtype=np.int8)
    signals.iloc[30:40] = 2
    return SignalFrame(
        signals=signals,
        indicator_data=None,
        signal_metadata={"source": "test"}
    )


@pytest.fixture
def sample_signal_frame_no_signals(sample_ohlcv_data):
    """Create SignalFrame with no signals."""
    signals = pd.Series(0, index=sample_ohlcv_data.index, dtype=np.int8)
    return SignalFrame(
        signals=signals,
        indicator_data=None,
        signal_metadata={"source": "test"}
    )


@pytest.fixture
def indicators_dict():
    """Empty indicators dictionary for testing."""
    return {}


@pytest.fixture
def ind_np_dict():
    """Empty numpy indicators dictionary for testing."""
    return {}


@pytest.fixture
def filter_test_df():
    """Create a DataFrame with engineered values for filter testing."""
    dates = pd.date_range(start="2025-01-01", periods=200, freq="1min")
    np.random.seed(42)
    
    # Create trending then ranging data
    trend = np.concatenate([
        np.linspace(100, 110, 100),  # Uptrend
        np.linspace(110, 105, 50),    # Down trend
        np.linspace(105, 106, 50)     # Sideways
    ])
    
    df = pd.DataFrame({
        "open": trend * 0.999,
        "high": trend * 1.002,
        "low": trend * 0.998,
        "close": trend,
        "volume": np.random.randint(100, 1000, 200)
    }, index=dates)
    
    return df


# ============================================================================
# Filter Test Base Class
# ============================================================================

class FilterTestBase:
    """Base class for filter tests with common test patterns."""
    
    filter_class = None
    default_params = {}
    
    def create_filter(self, **kwargs):
        """Create filter instance with default params overridden."""
        params = self.default_params.copy()
        params.update(kwargs)
        return self.filter_class(**params)
    
    def test_initialization_default_params(self):
        """Test filter initialization with default parameters."""
        filter_instance = self.create_filter()
        assert filter_instance.name is not None
        assert hasattr(filter_instance, 'enabled')
        
    def test_initialization_custom_params(self):
        """Test filter initialization with custom parameters."""
        # Override in subclass if needed
        pass
    
    def test_initialization_invalid_params(self):
        """Test that invalid parameters raise errors."""
        # Override in subclass
        pass
    
    def test_disabled_filter(self, sample_signal_frame_with_mixed_signals, filter_test_df):
        """Test that disabled filter passes all signals through."""
        filter_instance = self.create_filter(enabled=False)
        
        result = filter_instance.apply_filter(
            signal_frame=sample_signal_frame_with_mixed_signals,
            df=filter_test_df,
            indicators={},
            ind_np={},
            mode="core"
        )
        
        assert result.passed is True
        assert result.metadata.status == FilterStatus.SKIPPED
        assert result.metadata.signals_in == result.metadata.signals_out
        assert result.metadata.signals_rejected == 0
        assert result.signal_frame is sample_signal_frame_with_mixed_signals
    
    def test_no_input_signals(self, sample_signal_frame_no_signals, filter_test_df):
        """Test behavior when no signals are provided."""
        filter_instance = self.create_filter()
        
        result = filter_instance.apply_filter(
            signal_frame=sample_signal_frame_no_signals,
            df=filter_test_df,
            indicators={},
            ind_np={},
            mode="core"
        )
        
        assert result.metadata.signals_in == 0
        assert result.metadata.signals_out == 0
        assert result.metadata.status == FilterStatus.SKIPPED
    
    def test_missing_indicator(self, sample_signal_frame_with_mixed_signals, filter_test_df):
        """Test handling of missing indicator in cache."""
        filter_instance = self.create_filter()
        
        result = filter_instance.apply_filter(
            signal_frame=sample_signal_frame_with_mixed_signals,
            df=filter_test_df,
            indicators={},
            ind_np={},  # Empty numpy cache - indicator missing
            mode="core"
        )
        
        assert result.metadata.status == FilterStatus.ERROR
        assert "indicator not found" in result.metadata.reason.lower()
    
    def test_analytics_mode_metadata(self, sample_signal_frame_with_mixed_signals, filter_test_df):
        """Test that analytics mode includes additional metadata."""
        filter_instance = self.create_filter()
        
        # Need to compute indicators first
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        result = filter_instance.apply_filter(
            signal_frame=sample_signal_frame_with_mixed_signals,
            df=filter_test_df,
            indicators=indicators,
            ind_np=ind_np,
            mode="analytics"
        )
        
        # Check metadata includes source and params
        assert result.signal_frame.signal_metadata["source"] == filter_instance.name
        assert result.signal_frame.signal_metadata["mode"] == "analytics"
        
        # Check indicator_values if available
        if result.metadata.indicator_values:
            assert isinstance(result.metadata.indicator_values, dict)
    
    def test_timing_collected(self, sample_signal_frame_with_mixed_signals, filter_test_df):
        """Test that execution time is always collected (DEC-027)."""
        filter_instance = self.create_filter()
        
        # Compute indicators
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        result = filter_instance.apply_filter(
            signal_frame=sample_signal_frame_with_mixed_signals,
            df=filter_test_df,
            indicators=indicators,
            ind_np=ind_np,
            mode="core"
        )
        
        assert result.metadata.execution_time_ms is not None
        assert result.metadata.execution_time_ms > 0
    
    def test_compute_indicators_stores_correctly(self, filter_test_df):
        """Test that compute_indicators stores indicators in both dicts."""
        filter_instance = self.create_filter()
        
        indicators = {}
        ind_np = {}
        
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        # Should store at least one indicator
        assert len(indicators) > 0
        assert len(ind_np) > 0
        
        # Check that numpy arrays match series
        for key in indicators:
            assert key in ind_np
            assert len(indicators[key]) == len(ind_np[key])
    
    def test_compute_indicators_short_data(self, filter_test_df):
        """Test compute_indicators with insufficient data."""
        filter_instance = self.create_filter()
        
        # Use first few rows only
        short_df = filter_test_df.iloc[:2]
        
        indicators = {}
        ind_np = {}
        
        # Should not crash
        filter_instance.compute_indicators(short_df, indicators, ind_np)
        
        # Should still produce some output (zeros or NaNs)
        assert len(indicators) > 0
    
    # ========================================================================
    # Real Data Tests
    # ========================================================================
    
    def test_with_real_data(self, real_data_bundle):
        """Test filter on real market data."""
        filter_instance = self.create_filter()
        
        # Compute indicators on real data
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(real_data_bundle.strategy, indicators, ind_np)
        
        # Create a signal frame with some signals (at regular intervals)
        signals = pd.Series(0, index=real_data_bundle.strategy.index, dtype=np.int8)
        for i in range(0, len(signals), 50):  # Every 50th bar
            signals.iloc[i] = 1 if i % 100 == 0 else 2
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )
        
        # Apply filter
        result = filter_instance.apply_filter(
            signal_frame=signal_frame,
            df=real_data_bundle.strategy,
            indicators=indicators,
            ind_np=ind_np,
            mode="analytics"
        )
        
        # Basic validation
        assert result.metadata.signals_in >= result.metadata.signals_out
        assert result.metadata.signals_rejected >= 0
        
        # Log results for inspection
        print(f"\n{filter_instance.name} Real Data Test:")
        print(f"  Signals in: {result.metadata.signals_in}")
        print(f"  Signals out: {result.metadata.signals_out}")
        print(f"  Rejected: {result.metadata.signals_rejected}")
        print(f"  Pass rate: {result.metadata.signals_out/result.metadata.signals_in*100:.1f}%")