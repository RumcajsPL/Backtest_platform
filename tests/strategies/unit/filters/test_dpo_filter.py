"""
Unit Tests for DPOFilter
=========================
Tests Detrended Price Oscillator filtering.
"""

import pytest
import numpy as np
import pandas as pd

from src.strategies.contracts.signal_contracts import SignalFrame
from src.strategies.specific.filters.dpo_filter import DPOFilter
from .test_filters_base import TechnicalFilterTestBase

class TestDPOFilter(TechnicalFilterTestBase):
    """Tests for DPOFilter class."""
    
    filter_class = DPOFilter
    default_params = {
        "length": 20,
        "smooth": 3,
        "threshold": 0.2,
        "centered": False,
        "name": "dpo_filter"
    }
    
    def test_initialization_custom_params(self):
        """Test initialization with custom parameters."""
        filter_instance = self.create_filter(
            length=15,
            smooth=5,
            threshold=0.3,
            centered=True
        )
        
        assert filter_instance.length == 15
        assert filter_instance.smooth == 5
        assert filter_instance.threshold == 0.3
        assert filter_instance.centered is True
    
    def test_initialization_invalid_params(self):
        """Test that invalid parameters raise errors."""
        with pytest.raises(ValueError, match="DPO length must be >= 3"):
            self.create_filter(length=2)
    
    def test_compute_indicators(self, filter_test_df):
        """Test DPO computation."""
        filter_instance = self.create_filter()
        
        indicators = {}
        ind_np = {}
        
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        assert "dpo_norm" in indicators
        assert "dpo_norm" in ind_np
    
    def test_filter_buy_signals(self, filter_test_df):
        """Test BUY signal filtering: -threshold < dpo < 0."""
        filter_instance = self.create_filter(threshold=0.2)
        
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        # Manipulate DPO values
        dpo_values = np.zeros(len(filter_test_df))
        dpo_values[10] = -0.1  # Within (-0.2, 0) - PASS
        dpo_values[20] = 0.15  # Positive - REJECT
        dpo_values[30] = -0.3  # < -0.2 - REJECT
        dpo_values[40] = 0.05  # Positive - REJECT
        ind_np["dpo_norm"] = dpo_values
        
        signals = pd.Series(0, index=filter_test_df.index, dtype=np.int8)
        signals.iloc[10] = 1  # Should pass
        signals.iloc[20] = 1  # Should reject
        signals.iloc[30] = 1  # Should reject
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
        
        assert result.metadata.signals_out == 1
        assert result.metadata.signals_rejected == 3
    
    def test_filter_sell_signals(self, filter_test_df):
        """Test SELL signal filtering: 0 < dpo < threshold."""
        filter_instance = self.create_filter(threshold=0.2)
        
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        dpo_values = np.zeros(len(filter_test_df))
        dpo_values[10] = 0.1   # Within (0, 0.2) - PASS
        dpo_values[20] = -0.1  # Negative - REJECT
        dpo_values[30] = 0.3   # > 0.2 - REJECT
        dpo_values[40] = 0.15  # Within - PASS
        ind_np["dpo_norm"] = dpo_values
        
        signals = pd.Series(0, index=filter_test_df.index, dtype=np.int8)
        signals.iloc[10] = 2  # Should pass
        signals.iloc[20] = 2  # Should reject
        signals.iloc[30] = 2  # Should reject
        signals.iloc[40] = 2  # Should pass
        
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