"""
Unit Tests for CCIFilter
=========================
Tests CCI overbought/oversold filtering.
"""

import pytest
import numpy as np
import pandas as pd

from src.strategies.specific.filters.cci_filter import CCIFilter
from tests.unit.test_filters_base import TechnicalFilterTestBase


class TestCCIFilter(TechnicalFilterTestBase):
    """Tests for CCIFilter class."""
    
    filter_class = CCIFilter
    default_params = {
        "length": 20,
        "overbought": 100,
        "oversold": -100,
        "name": "cci_filter"
    }
    
    def test_initialization_custom_params(self):
        """Test initialization with custom parameters."""
        filter_instance = self.create_filter(
            length=14,
            overbought=150,
            oversold=-150
        )
        
        assert filter_instance.length == 14
        assert filter_instance.overbought == 150
        assert filter_instance.oversold == -150
    
    def test_initialization_invalid_params(self):
        """Test that invalid parameters raise errors."""
        # Length too small
        with pytest.raises(ValueError, match="CCI length must be >= 3"):
            self.create_filter(length=2)
        
        # Oversold >= overbought
        with pytest.raises(ValueError, match="oversold.*must be < overbought"):
            self.create_filter(oversold=100, overbought=100)
        
        with pytest.raises(ValueError, match="oversold.*must be < overbought"):
            self.create_filter(oversold=50, overbought=0)
    
    def test_compute_indicators(self, filter_test_df):
        """Test CCI computation."""
        filter_instance = self.create_filter()
        
        indicators = {}
        ind_np = {}
        
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        assert "cci" in indicators
        assert "cci" in ind_np
    
    def test_filter_buy_signals(self, filter_test_df):
        """Test filtering of BUY signals based on CCI."""
        filter_instance = self.create_filter(overbought=100, oversold=-100)
        
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        # Manipulate CCI values
        cci_values = np.zeros(len(filter_test_df))
        cci_values[10] = 150   # Overbought - should reject BUY
        cci_values[20] = -150  # Oversold - should accept BUY
        cci_values[30] = 50    # Neutral - should accept BUY
        ind_np["cci"] = cci_values
        
        signals = pd.Series(0, index=filter_test_df.index, dtype=np.int8)
        signals.iloc[10] = 1  # Overbought - reject
        signals.iloc[20] = 1  # Oversold - accept
        signals.iloc[30] = 1  # Neutral - accept
        
        signal_frame = create_signal_frame(signals)
        
        result = filter_instance.apply_filter(
            signal_frame=signal_frame,
            df=filter_test_df,
            indicators=indicators,
            ind_np=ind_np,
            mode="analytics"
        )
        
        assert result.metadata.signals_out == 2
        assert result.metadata.signals_rejected == 1
    
    def test_filter_sell_signals(self, filter_test_df):
        """Test filtering of SELL signals based on CCI."""
        filter_instance = self.create_filter(overbought=100, oversold=-100)
        
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        cci_values = np.zeros(len(filter_test_df))
        cci_values[10] = 150   # Overbought - should accept SELL
        cci_values[20] = -150  # Oversold - should reject SELL
        cci_values[30] = 50    # Neutral - should accept SELL
        ind_np["cci"] = cci_values
        
        signals = pd.Series(0, index=filter_test_df.index, dtype=np.int8)
        signals.iloc[10] = 2  # Overbought - accept
        signals.iloc[20] = 2  # Oversold - reject
        signals.iloc[30] = 2  # Neutral - accept
        
        signal_frame = create_signal_frame(signals)
        
        result = filter_instance.apply_filter(
            signal_frame=signal_frame,
            df=filter_test_df,
            indicators=indicators,
            ind_np=ind_np,
            mode="analytics"
        )
        
        assert result.metadata.signals_out == 2
        assert result.metadata.signals_rejected == 1