"""
Unit Tests for MAFilter
========================
Tests Moving Average slope filtering.
"""

import pytest
import numpy as np
import pandas as pd

from src.strategies.contracts.signal_contracts import SignalFrame
from src.strategies.specific.filters.ma_filter import MAFilter
from .test_filters_base import TechnicalFilterTestBase


class TestMAFilter(TechnicalFilterTestBase):
    """Tests for MAFilter class."""
    
    filter_class = MAFilter
    default_params = {
        "ma_type": "TEMA",
        "length": 25,
        "slope_length": 10,
        "name": "ma_filter"
    }
    
    def test_initialization_custom_params(self):
        """Test initialization with custom parameters."""
        filter_instance = self.create_filter(
            ma_type="SMA",
            length=20,
            slope_length=5
        )
        
        assert filter_instance.ma_type == "SMA"
        assert filter_instance.length == 20
        assert filter_instance.slope_length == 5
    
    @pytest.mark.parametrize("ma_type", ["SMA", "EMA", "WMA", "HMA", "DEMA", "TEMA", "KAMA", "TRIMA", "LSMA"])
    def test_all_ma_types(self, ma_type):
        """Test all valid MA types."""
        filter_instance = self.create_filter(ma_type=ma_type)
        assert filter_instance.ma_type == ma_type
    
    def test_initialization_invalid_params(self):
        """Test that invalid parameters raise errors."""
        # Invalid MA type
        with pytest.raises(ValueError, match="MA type must be one of"):
            self.create_filter(ma_type="INVALID")
        
        # Length too small
        with pytest.raises(ValueError, match="MA length must be >= 2"):
            self.create_filter(length=1)
        
        # Slope length too small
        with pytest.raises(ValueError, match="slope_length must be >= 1"):
            self.create_filter(slope_length=0)
    
    def test_compute_indicators(self, filter_test_df):
        """Test MA computation."""
        filter_instance = self.create_filter()
        
        indicators = {}
        ind_np = {}
        
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        assert "ma" in indicators
        assert "ma_ago" in indicators
        assert "ma" in ind_np
        assert "ma_ago" in ind_np
    
    def test_filter_buy_signals(self, filter_test_df):
        """Test BUY signals: ma > ma_ago."""
        filter_instance = self.create_filter()
        
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        # Manipulate MA values
        ma = np.ones(len(filter_test_df)) * 100
        ma_ago = np.ones(len(filter_test_df)) * 100
        
        # At index 10: ma=102 > ma_ago=100 -> pass
        # At index 20: ma=98 < ma_ago=100 -> reject
        # At index 30: ma=100 = ma_ago=100 -> reject (strict)
        ma[10] = 102
        ma[20] = 98
        ma_ago[20] = 100
        ma_ago[30] = 100
        
        ind_np["ma"] = ma
        ind_np["ma_ago"] = ma_ago
        
        signals = pd.Series(0, index=filter_test_df.index, dtype=np.int8)
        signals.iloc[10] = 1  # Should pass
        signals.iloc[20] = 1  # Should reject
        signals.iloc[30] = 1  # Should reject
        
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
        assert result.metadata.signals_rejected == 2
    
    def test_filter_sell_signals(self, filter_test_df):
        """Test SELL signals: ma < ma_ago."""
        filter_instance = self.create_filter()
        
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        ma = np.ones(len(filter_test_df)) * 100
        ma_ago = np.ones(len(filter_test_df)) * 100
        
        # At index 10: ma=98 < ma_ago=100 -> pass
        # At index 20: ma=102 > ma_ago=100 -> reject
        # At index 30: ma=100 = ma_ago=100 -> reject
        ma[10] = 98
        ma[20] = 102
        ma_ago[10] = 100
        ma_ago[20] = 100
        ma_ago[30] = 100
        
        ind_np["ma"] = ma
        ind_np["ma_ago"] = ma_ago
        
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
            df=filter_test_df,
            indicators=indicators,
            ind_np=ind_np,
            mode="analytics"
        )
        
        assert result.metadata.signals_out == 1
        assert result.metadata.signals_rejected == 2