"""
Unit Tests for BollingerFilter
===============================
Tests Bollinger Bands volatility regime filtering.
"""

import pytest
import numpy as np
import pandas as pd

from src.strategies.specific.filters.bollinger_filter import BollingerFilter
from tests.unit.test_filters_base import TechnicalFilterTestBase


class TestBollingerFilter(TechnicalFilterTestBase):
    """Tests for BollingerFilter class."""
    
    filter_class = BollingerFilter
    default_params = {
        "length": 14,
        "width_ma_length": 30,
        "filter_multiplier": 0.5,
        "std_dev": 2.0,
        "name": "bollinger_filter"
    }
    
    def test_initialization_custom_params(self):
        """Test initialization with custom parameters."""
        filter_instance = self.create_filter(
            length=20,
            width_ma_length=40,
            filter_multiplier=0.7,
            std_dev=2.5
        )
        
        assert filter_instance.length == 20
        assert filter_instance.width_ma_length == 40
        assert filter_instance.filter_multiplier == 0.7
        assert filter_instance.std_dev == 2.5
    
    def test_initialization_invalid_params(self):
        """Test that invalid parameters raise errors."""
        with pytest.raises(ValueError, match="Bollinger length must be >= 2"):
            self.create_filter(length=1)
        
        with pytest.raises(ValueError, match="std_dev must be > 0"):
            self.create_filter(std_dev=0)
        
        with pytest.raises(ValueError, match="std_dev must be > 0"):
            self.create_filter(std_dev=-1)
    
    def test_compute_indicators(self, filter_test_df):
        """Test Bollinger Bands computation - only bandwidth stored."""
        filter_instance = self.create_filter()
        
        indicators = {}
        ind_np = {}
        
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        # Should store only bandwidth and bandwidth_ma (P1-CH3-5)
        assert "bb_bandwidth" in indicators
        assert "bb_bandwidth_ma" in indicators
        assert len(indicators) == 2  # No other indicators
        
        assert "bb_bandwidth" in ind_np
        assert "bb_bandwidth_ma" in ind_np
        
        # Values should be non-negative
        assert (indicators["bb_bandwidth"] >= 0).all()
    
    def test_filter_signals(self, filter_test_df):
        """Test filtering based on bandwidth > bandwidth_ma * multiplier."""
        filter_instance = self.create_filter(filter_multiplier=0.5)
        
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        # Manipulate bandwidth values
        bandwidth = np.ones(len(filter_test_df)) * 2.0
        bandwidth_ma = np.ones(len(filter_test_df)) * 2.0
        threshold = bandwidth_ma * 0.5  # = 1.0
        
        # At index 10: bandwidth=3.0 > 1.0 -> pass
        # At index 20: bandwidth=0.5 < 1.0 -> reject
        bandwidth[10] = 3.0
        bandwidth[20] = 0.5
        ind_np["bb_bandwidth"] = bandwidth
        ind_np["bb_bandwidth_ma"] = bandwidth_ma
        
        signals = pd.Series(0, index=filter_test_df.index, dtype=np.int8)
        signals.iloc[10] = 1  # High bandwidth - pass
        signals.iloc[20] = 1  # Low bandwidth - reject
        
        signal_frame = create_signal_frame(signals)
        
        result = filter_instance.apply_filter(
            signal_frame=signal_frame,
            df=filter_test_df,
            indicators=indicators,
            ind_np=ind_np,
            mode="analytics"
        )
        
        assert result.metadata.signals_out == 1
        assert result.metadata.signals_rejected == 1