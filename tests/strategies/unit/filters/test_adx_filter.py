"""
Unit Tests for ADXFilter
=========================
Tests ADX trend strength filtering.
"""

import pytest
import numpy as np
import pandas as pd

from src.strategies.specific.filters.adx_filter import ADXFilter
from src.strategies.contracts.filter_contracts import FilterStatus
from tests.unit.test_filters_base import TechnicalFilterTestBase


class TestADXFilter(TechnicalFilterTestBase):
    """Tests for ADXFilter class."""
    
    filter_class = ADXFilter
    default_params = {
        "adx_length": 14,
        "threshold": 18.0,
        "name": "adx_filter"
    }
    
    def test_initialization_custom_params(self):
        """Test initialization with custom parameters."""
        filter_instance = self.create_filter(
            adx_length=10,
            threshold=20.0
        )
        
        assert filter_instance.adx_length == 10
        assert filter_instance.threshold == 20.0
    
    def test_initialization_invalid_params(self):
        """Test that invalid parameters raise errors."""
        with pytest.raises(ValueError, match="ADX length must be >= 2"):
            self.create_filter(adx_length=1)
    
    def test_compute_indicators(self, filter_test_df):
        """Test ADX computation."""
        filter_instance = self.create_filter()
        
        indicators = {}
        ind_np = {}
        
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        assert "adx" in indicators
        assert "adx" in ind_np
        assert len(indicators["adx"]) == len(filter_test_df)
    
    def test_filter_buy_signals(self, filter_test_df):
        """Test filtering of BUY signals based on ADX threshold."""
        filter_instance = self.create_filter(threshold=25)
        
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        # Manipulate ADX values
        adx_values = np.ones(len(filter_test_df)) * 20  # Below threshold
        adx_values[10] = 30  # Above threshold - should pass
        adx_values[20] = 15  # Below threshold - should reject
        adx_values[30] = 40  # Above threshold - should pass
        ind_np["adx"] = adx_values
        
        # Create signals
        signals = pd.Series(0, index=filter_test_df.index, dtype=np.int8)
        signals.iloc[10] = 1  # ADX 30 - pass
        signals.iloc[20] = 1  # ADX 15 - reject
        signals.iloc[30] = 1  # ADX 40 - pass
        
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
        
        assert result.metadata.signals_out == 2  # Positions 10 and 30 pass
        assert result.metadata.signals_rejected == 1  # Position 20 rejected
    
    def test_filter_sell_signals(self, filter_test_df):
        """Test filtering of SELL signals based on ADX threshold."""
        filter_instance = self.create_filter(threshold=25)
        
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        adx_values = np.ones(len(filter_test_df)) * 20
        adx_values[10] = 30  # Above threshold - pass
        adx_values[20] = 15  # Below threshold - reject
        ind_np["adx"] = adx_values
        
        signals = pd.Series(0, index=filter_test_df.index, dtype=np.int8)
        signals.iloc[10] = 2  # SELL with ADX 30 - pass
        signals.iloc[20] = 2  # SELL with ADX 15 - reject
        
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
        assert result.metadata.signals_rejected == 1