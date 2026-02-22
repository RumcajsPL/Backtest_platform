"""
Unit Tests for ChoppinessFilter
================================
Tests Choppiness Index market regime filtering.
"""

import pytest
import numpy as np
import pandas as pd

from src.strategies.contracts.signal_contracts import SignalFrame
from src.strategies.specific.filters.choppiness_filter import ChoppinessFilter
from .test_filters_base import TechnicalFilterTestBase


class TestChoppinessFilter(TechnicalFilterTestBase):
    """Tests for ChoppinessFilter class."""
    
    filter_class = ChoppinessFilter
    default_params = {
        "length": 14,
        "threshold": 61.8,
        "name": "choppiness_filter"
    }
    
    def test_initialization_custom_params(self):
        """Test initialization with custom parameters."""
        filter_instance = self.create_filter(
            length=20,
            threshold=50.0
        )
        
        assert filter_instance.length == 20
        assert filter_instance.threshold == 50.0
    
    def test_initialization_invalid_params(self):
        """Test that invalid parameters raise errors."""
        with pytest.raises(ValueError, match="Choppiness length must be >= 2"):
            self.create_filter(length=1)
        
        with pytest.raises(ValueError, match="Threshold must be 0–100"):
            self.create_filter(threshold=101)
        
        with pytest.raises(ValueError, match="Threshold must be 0–100"):
            self.create_filter(threshold=-1)
    
    def test_compute_indicators(self, filter_test_df):
        """Test Choppiness Index computation."""
        filter_instance = self.create_filter()
        
        indicators = {}
        ind_np = {}
        
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        assert "choppiness" in indicators
        assert "choppiness" in ind_np
        
        # CI should be between 0 and 100
        ci = indicators["choppiness"]
        assert (ci >= 0).all()
        assert (ci <= 100).all()
    
    def test_filter_signals(self, filter_test_df):
        """Test filtering based on CI <= threshold."""
        filter_instance = self.create_filter(threshold=50)
        
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        # Manipulate CI values
        ci_values = np.ones(len(filter_test_df)) * 60  # > threshold
        ci_values[10] = 40  # Trending - should pass
        ci_values[20] = 30  # Trending - should pass
        ci_values[30] = 70  # Choppy - should reject
        ind_np["choppiness"] = ci_values
        
        signals = pd.Series(0, index=filter_test_df.index, dtype=np.int8)
        signals.iloc[10] = 1  # Trending - pass
        signals.iloc[20] = 2  # Trending - pass
        signals.iloc[30] = 1  # Choppy - reject
        
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
        assert result.metadata.signals_rejected == 1