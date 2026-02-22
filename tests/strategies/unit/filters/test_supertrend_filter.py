"""
Unit Tests for SupertrendFilter
=================================
Tests Supertrend directional trend filtering.
"""

import pytest
import numpy as np
import pandas as pd

from src.strategies.contracts.signal_contracts import SignalFrame
from src.strategies.specific.filters.supertrend_filter import SupertrendFilter
from .test_filters_base import TechnicalFilterTestBase


class TestSupertrendFilter(TechnicalFilterTestBase):
    """Tests for SupertrendFilter class."""
    
    filter_class = SupertrendFilter
    default_params = {
        "atr_length": 10,
        "factor": 3.0,
        "name": "supertrend_filter"
    }
    
    def test_initialization_custom_params(self):
        """Test initialization with custom parameters."""
        filter_instance = self.create_filter(
            atr_length=14,
            factor=2.5
        )
        
        assert filter_instance.atr_length == 14
        assert filter_instance.factor == 2.5
    
    def test_initialization_invalid_params(self):
        """Test that invalid parameters raise errors."""
        with pytest.raises(ValueError, match="ATR length must be >= 1"):
            self.create_filter(atr_length=0)
        
        with pytest.raises(ValueError, match="Factor must be > 0"):
            self.create_filter(factor=0)
        
        with pytest.raises(ValueError, match="Factor must be > 0"):
            self.create_filter(factor=-1)
    
    def test_compute_indicators(self, filter_test_df):
        """Test Supertrend computation."""
        filter_instance = self.create_filter()
        
        indicators = {}
        ind_np = {}
        
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        assert "supertrend_price" in indicators
        assert "supertrend_dir" in indicators
        assert "supertrend_price" in ind_np
        assert "supertrend_dir" in ind_np
        
        # Direction should be -1 or 1
        dir_values = indicators["supertrend_dir"].dropna()
        if len(dir_values) > 0:
            assert set(dir_values.unique()).issubset({-1.0, 1.0})
    
    def test_filter_buy_signals(self, filter_test_df):
        """Test BUY signals: dir == 1 AND close > supertrend_price."""
        filter_instance = self.create_filter()
        
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        # Manipulate Supertrend values
        st_price = np.ones(len(filter_test_df)) * 100
        st_dir = np.ones(len(filter_test_df)) * 1  # Bullish
        close = np.ones(len(filter_test_df)) * 101
        
        # Index 10: dir=1, close=101 > 100 -> PASS
        # Index 20: dir=1, close=99 < 100 -> REJECT
        # Index 30: dir=-1, close=101 > 100 -> REJECT (wrong dir)
        # Index 40: NaN -> REJECT (NaN pass)
        st_dir[30] = -1
        close[20] = 99
        st_price[40] = np.nan
        
        ind_np["supertrend_price"] = st_price
        ind_np["supertrend_dir"] = st_dir
        df_with_close = filter_test_df.copy()
        df_with_close["close"] = close
        
        signals = pd.Series(0, index=filter_test_df.index, dtype=np.int8)
        signals.iloc[10] = 1  # Should pass
        signals.iloc[20] = 1  # Should reject
        signals.iloc[30] = 1  # Should reject
        signals.iloc[40] = 1  # Should reject (NaN)
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )
        
        result = filter_instance.apply_filter(
            signal_frame=signal_frame,
            df=df_with_close,
            indicators=indicators,
            ind_np=ind_np,
            mode="analytics"
        )
        
        assert result.metadata.signals_out == 1
        assert result.metadata.signals_rejected == 3
    
    def test_filter_sell_signals(self, filter_test_df):
        """Test SELL signals: dir == -1 AND close < supertrend_price."""
        filter_instance = self.create_filter()
        
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        st_price = np.ones(len(filter_test_df)) * 100
        st_dir = np.ones(len(filter_test_df)) * -1  # Bearish
        close = np.ones(len(filter_test_df)) * 99
        
        # Index 10: dir=-1, close=99 < 100 -> PASS
        # Index 20: dir=-1, close=101 > 100 -> REJECT
        # Index 30: dir=1, close=99 < 100 -> REJECT (wrong dir)
        st_dir[30] = 1
        close[20] = 101
        
        ind_np["supertrend_price"] = st_price
        ind_np["supertrend_dir"] = st_dir
        df_with_close = filter_test_df.copy()
        df_with_close["close"] = close
        
        signals = pd.Series(0, index=filter_test_df.index, dtype=np.int8)
        signals.iloc[10] = 2  # Should pass
        signals.iloc[20] = 2  # Should reject
        signals.iloc[30] = 2  # Should reject
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )
        
        result = filter_instance.apply_filter(
            signal_frame=signal_frame,
            df=df_with_close,
            indicators=indicators,
            ind_np=ind_np,
            mode="analytics"
        )
        
        assert result.metadata.signals_out == 1
        assert result.metadata.signals_rejected == 2