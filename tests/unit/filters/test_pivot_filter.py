"""
Unit Tests for PivotFilter
===========================
Tests pivot structure bias filtering.
"""

import pytest
import numpy as np
import pandas as pd

from src.strategies.specific.filters.pivot_filter import PivotFilter
from tests.unit.test_filters_base import TechnicalFilterTestBase


class TestPivotFilter(TechnicalFilterTestBase):
    """Tests for PivotFilter class."""
    
    filter_class = PivotFilter
    default_params = {
        "reversal_percent": 0.2,
        "order": 5,
        "name": "pivot_filter"
    }
    
    def test_initialization_custom_params(self):
        """Test initialization with custom parameters."""
        filter_instance = self.create_filter(
            reversal_percent=0.5,
            order=3
        )
        
        assert filter_instance.reversal_percent == 0.005  # Converted to decimal
        assert filter_instance.order == 3
    
    def test_initialization_invalid_params(self):
        """Test that invalid parameters raise errors."""
        with pytest.raises(ValueError, match="reversal_percent must be > 0"):
            self.create_filter(reversal_percent=0)
        
        with pytest.raises(ValueError, match="order must be >= 1"):
            self.create_filter(order=0)
    
    def test_compute_indicators(self, filter_test_df):
        """Test pivot bias computation."""
        filter_instance = self.create_filter()
        
        indicators = {}
        ind_np = {}
        
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        assert "pivot_bias" in indicators
        assert "pivot_bias" in ind_np
        
        # Bias should be -1, 0, or 1
        bias = indicators["pivot_bias"]
        assert bias.dtype == np.int8
        assert set(bias.unique()).issubset({-1, 0, 1})
    
    def test_filter_buy_signals(self, filter_test_df):
        """Test BUY signals: bias == 1."""
        filter_instance = self.create_filter()
        
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        # Manipulate bias values
        bias = np.zeros(len(filter_test_df), dtype=np.int8)
        bias[10] = 1   # Bullish - pass
        bias[20] = -1  # Bearish - reject
        bias[30] = 0   # Neutral - reject
        ind_np["pivot_bias"] = bias
        
        signals = pd.Series(0, index=filter_test_df.index, dtype=np.int8)
        signals.iloc[10] = 1  # Should pass
        signals.iloc[20] = 1  # Should reject
        signals.iloc[30] = 1  # Should reject
        
        signal_frame = create_signal_frame(signals)
        
        result = filter_instance.apply_filter(
            signal_frame=signal_frame,
            df=filter_test_df,
            indicators=indicators,
            ind_np=ind_np,
            mode="analytics"
        )
        
        assert result.metadata.signals_out == 1
        assert result.metadata.signals_rejected == 2
    
    def test_filter_sell_signals(self, filter_test_df):
        """Test SELL signals: bias == -1."""
        filter_instance = self.create_filter()
        
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        bias = np.zeros(len(filter_test_df), dtype=np.int8)
        bias[10] = -1  # Bearish - pass
        bias[20] = 1   # Bullish - reject
        bias[30] = 0   # Neutral - reject
        ind_np["pivot_bias"] = bias
        
        signals = pd.Series(0, index=filter_test_df.index, dtype=np.int8)
        signals.iloc[10] = 2  # Should pass
        signals.iloc[20] = 2  # Should reject
        signals.iloc[30] = 2  # Should reject
        
        signal_frame = create_signal_frame(signals)
        
        result = filter_instance.apply_filter(
            signal_frame=signal_frame,
            df=filter_test_df,
            indicators=indicators,
            ind_np=ind_np,
            mode="analytics"
        )
        
        assert result.metadata.signals_out == 1
        assert result.metadata.signals_rejected == 2