"""
Unit Tests for MACDFilter
==========================
Tests MACD histogram filtering.
"""

import pytest
import numpy as np
import pandas as pd

from src.strategies.contracts.signal_contracts import SignalFrame
from src.strategies.specific.filters.macd_filter import MACDFilter
from .test_filters_base import TechnicalFilterTestBase


class TestMACDFilter(TechnicalFilterTestBase):
    """Tests for MACDFilter class."""
    
    filter_class = MACDFilter
    default_params = {
        "fast_length": 12,
        "slow_length": 26,
        "signal_length": 9,
        "name": "macd_filter"
    }
    
    def test_initialization_custom_params(self):
        """Test initialization with custom parameters."""
        filter_instance = self.create_filter(
            fast_length=8,
            slow_length=17,
            signal_length=5
        )
        
        assert filter_instance.fast_length == 8
        assert filter_instance.slow_length == 17
        assert filter_instance.signal_length == 5
    
    def test_initialization_invalid_params(self):
        """Test that invalid parameters raise errors."""
        with pytest.raises(ValueError, match="fast_length.*must be < slow_length"):
            self.create_filter(fast_length=20, slow_length=10)
    
    def test_compute_indicators(self, filter_test_df):
        """Test MACD computation - only histogram stored."""
        filter_instance = self.create_filter()
        
        indicators = {}
        ind_np = {}
        
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        assert "macd_histogram" in indicators
        assert "macd_histogram" in ind_np
    
    def test_filter_buy_signals(self, filter_test_df):
        """Test BUY signals: histogram > 0."""
        filter_instance = self.create_filter()
        
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        # Manipulate histogram values
        hist = np.zeros(len(filter_test_df))
        hist[10] = 0.5   # Positive - pass
        hist[20] = -0.3  # Negative - reject
        hist[30] = 0.1   # Positive - pass
        hist[40] = 0.0   # Zero - reject (strict)
        ind_np["macd_histogram"] = hist
        
        signals = pd.Series(0, index=filter_test_df.index, dtype=np.int8)
        signals.iloc[10] = 1  # Should pass
        signals.iloc[20] = 1  # Should reject
        signals.iloc[30] = 1  # Should pass
        signals.iloc[40] = 1  # Should reject
        
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
        
        assert result.metadata.signals_out == 2
        assert result.metadata.signals_rejected == 2
    
    def test_filter_sell_signals(self, filter_test_df):
        """Test SELL signals: histogram < 0."""
        filter_instance = self.create_filter()
        
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        hist = np.zeros(len(filter_test_df))
        hist[10] = -0.5  # Negative - pass
        hist[20] = 0.3   # Positive - reject
        hist[30] = -0.1  # Negative - pass
        hist[40] = 0.0   # Zero - reject
        ind_np["macd_histogram"] = hist
        
        signals = pd.Series(0, index=filter_test_df.index, dtype=np.int8)
        signals.iloc[10] = 2  # Should pass
        signals.iloc[20] = 2  # Should reject
        signals.iloc[30] = 2  # Should pass
        signals.iloc[40] = 2  # Should reject
        
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
        
        assert result.metadata.signals_out == 2
        assert result.metadata.signals_rejected == 2