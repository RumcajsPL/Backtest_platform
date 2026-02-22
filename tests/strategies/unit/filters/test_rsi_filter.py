"""
Unit Tests for RSIFilter
=========================
Tests RSI overbought/oversold filtering.
"""

import pytest
import numpy as np
import pandas as pd

from src.strategies.contracts.signal_contracts import SignalFrame
from src.strategies.specific.filters.rsi_filter import RSIFilter
from src.strategies.contracts.filter_contracts import FilterStatus
from .test_filters_base import TechnicalFilterTestBase


class TestRSIFilter(TechnicalFilterTestBase):
    """Tests for RSIFilter class."""
    
    filter_class = RSIFilter
    default_params = {
        "length": 14,
        "overbought": 70.0,
        "oversold": 30.0,
        "name": "rsi_filter"
    }
    
    def test_initialization_custom_params(self):
        """Test initialization with custom parameters."""
        filter_instance = self.create_filter(
            length=10,
            overbought=75.0,
            oversold=25.0
        )
        
        assert filter_instance.length == 10
        assert filter_instance.overbought == 75.0
        assert filter_instance.oversold == 25.0
    
    def test_initialization_invalid_params(self):
        """Test that invalid parameters raise errors."""
        # Length too small
        with pytest.raises(ValueError, match="RSI length must be >= 2"):
            self.create_filter(length=1)
        
        # Oversold >= overbought
        with pytest.raises(ValueError, match="oversold.*must be < overbought"):
            self.create_filter(oversold=70, overbought=70)
        
        with pytest.raises(ValueError, match="oversold.*must be < overbought"):
            self.create_filter(oversold=80, overbought=70)
    
    def test_compute_indicators(self, filter_test_df):
        """Test RSI computation."""
        filter_instance = self.create_filter()
        
        indicators = {}
        ind_np = {}
        
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        assert "rsi" in indicators
        assert "rsi" in ind_np
        assert len(indicators["rsi"]) == len(filter_test_df)
        assert 0 <= indicators["rsi"].min() <= 100
        assert 0 <= indicators["rsi"].max() <= 100
    
    def test_filter_buy_signals(self, filter_test_df):
        """Test filtering of BUY signals based on RSI."""
        filter_instance = self.create_filter(overbought=70, oversold=30)
        
        # Compute indicators with engineered RSI values
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        # Manipulate RSI values for testing
        rsi_values = np.ones(len(filter_test_df)) * 50  # Default neutral
        rsi_values[10] = 80  # Overbought - should reject BUY
        rsi_values[20] = 20  # Oversold - should accept BUY (not overbought)
        rsi_values[30] = 60  # Neutral - should accept BUY
        ind_np["rsi"] = rsi_values
        
        # Create signal frame with BUY signals at specific positions
        signals = pd.Series(0, index=filter_test_df.index, dtype=np.int8)
        signals.iloc[10] = 1  # Overbought - should reject
        signals.iloc[20] = 1  # Oversold - should accept
        signals.iloc[30] = 1  # Neutral - should accept
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )
        
        result = filter_instance.apply_filter(
            signal_frame=signal_frame,
            df=filter_test_df,
            indicators=indicators,
            ind_np=ind_np,
            mode="analytics"
        )
        
        # Check results
        assert result.metadata.signals_in == 3
        assert result.metadata.signals_out == 2  # Positions 20 and 30 should pass
        assert result.metadata.signals_rejected == 1  # Position 10 rejected
        
        # Verify which signals passed
        filtered_signals = result.signal_frame.signals
        assert filtered_signals.iloc[10] == 0  # Rejected
        assert filtered_signals.iloc[20] == 1  # Passed
        assert filtered_signals.iloc[30] == 1  # Passed
    
    def test_filter_sell_signals(self, filter_test_df):
        """Test filtering of SELL signals based on RSI."""
        filter_instance = self.create_filter(overbought=70, oversold=30)
        
        # Compute indicators with engineered RSI values
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        # Manipulate RSI values
        rsi_values = np.ones(len(filter_test_df)) * 50
        rsi_values[10] = 20  # Oversold - should reject SELL
        rsi_values[20] = 80  # Overbought - should accept SELL
        rsi_values[30] = 60  # Neutral - should accept SELL
        ind_np["rsi"] = rsi_values
        
        # Create signal frame with SELL signals
        signals = pd.Series(0, index=filter_test_df.index, dtype=np.int8)
        signals.iloc[10] = 2  # Oversold - should reject
        signals.iloc[20] = 2  # Overbought - should accept
        signals.iloc[30] = 2  # Neutral - should accept
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )
        
        result = filter_instance.apply_filter(
            signal_frame=signal_frame,
            df=filter_test_df,
            indicators=indicators,
            ind_np=ind_np,
            mode="analytics"
        )
        
        assert result.metadata.signals_out == 2  # Positions 20 and 30 pass
        assert result.metadata.signals_rejected == 1  # Position 10 rejected
    
    def test_mixed_signals(self, filter_test_df):
        """Test filtering of mixed BUY/SELL signals."""
        filter_instance = self.create_filter(overbought=70, oversold=30)
        
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        # RSI values
        rsi_values = np.ones(len(filter_test_df)) * 50
        rsi_values[10] = 80  # Overbought
        rsi_values[20] = 20  # Oversold
        rsi_values[30] = 80  # Overbought
        rsi_values[40] = 20  # Oversold
        ind_np["rsi"] = rsi_values
        
        # BUY at 10 (overbought - reject), SELL at 20 (oversold - reject)
        # BUY at 30 (overbought - reject), SELL at 40 (oversold - reject)
        signals = pd.Series(0, index=filter_test_df.index, dtype=np.int8)
        signals.iloc[10] = 1  # BUY at overbought - REJECT
        signals.iloc[20] = 2  # SELL at oversold - REJECT
        signals.iloc[30] = 1  # BUY at overbought - REJECT
        signals.iloc[40] = 2  # SELL at oversold - REJECT
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )
        
        result = filter_instance.apply_filter(
            signal_frame=signal_frame,
            df=filter_test_df,
            indicators=indicators,
            ind_np=ind_np,
            mode="analytics"
        )
        
        assert result.metadata.signals_out == 0
        assert result.metadata.signals_rejected == 4
    
    def test_analytics_mode_indicator_values(self, filter_test_df):
        """Test that analytics mode includes indicator values."""
        filter_instance = self.create_filter()
        
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        # Add some signals that will pass
        signals = pd.Series(0, index=filter_test_df.index, dtype=np.int8)
        signals.iloc[50] = 1
        signals.iloc[60] = 2
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )
        
        result = filter_instance.apply_filter(
            signal_frame=signal_frame,
            df=filter_test_df,
            indicators=indicators,
            ind_np=ind_np,
            mode="analytics"
        )
        
        if result.metadata.signals_out > 0:
            assert result.metadata.indicator_values is not None
            assert "rsi_mean" in result.metadata.indicator_values