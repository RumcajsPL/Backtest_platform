"""
Base Classes for Filter Tests
===============================
Provides common test patterns for all technical filters.
"""

import pytest
import pandas as pd
import numpy as np

from src.strategies.contracts.filter_contracts import FilterStatus
from src.strategies.contracts.signal_contracts import SignalFrame


class TechnicalFilterTestBase:
    """Base class for technical filter tests."""
    
    filter_class = None
    default_params = {}
    
    def create_filter(self, **kwargs):
        """Create filter instance with default params overridden."""
        params = self.default_params.copy()
        params.update(kwargs)
        return self.filter_class(**params)
    
    def test_initialization_default_params(self):
        """Test filter initialization with default parameters."""
        filter_instance = self.create_filter()
        assert filter_instance.name is not None
        assert filter_instance.enabled is True
        
    def test_disabled_filter(self, sample_signal_frame_with_mixed_signals, filter_test_df):
        """Test that disabled filter passes all signals through."""
        filter_instance = self.create_filter(enabled=False)
        
        result = filter_instance.apply_filter(
            signal_frame=sample_signal_frame_with_mixed_signals,
            df=filter_test_df,
            indicators={},
            ind_np={},
            mode="core"
        )
        
        assert result.passed is True
        assert result.metadata.status == FilterStatus.SKIPPED
        assert result.metadata.signals_in == result.metadata.signals_out
        assert result.metadata.signals_rejected == 0
    
    def test_no_input_signals(self, sample_signal_frame_no_signals, filter_test_df):
        """Test behavior when no signals are provided."""
        filter_instance = self.create_filter()
        
        result = filter_instance.apply_filter(
            signal_frame=sample_signal_frame_no_signals,
            df=filter_test_df,
            indicators={},
            ind_np={},
            mode="core"
        )
        
        assert result.metadata.signals_in == 0
        assert result.metadata.signals_out == 0
        assert result.metadata.status == FilterStatus.SKIPPED
    
    def test_timing_collected(self, sample_signal_frame_with_mixed_signals, filter_test_df):
        """Test that execution time is always collected (DEC-027)."""
        filter_instance = self.create_filter()
        
        # Compute indicators
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(filter_test_df, indicators, ind_np)
        
        result = filter_instance.apply_filter(
            signal_frame=sample_signal_frame_with_mixed_signals,
            df=filter_test_df,
            indicators=indicators,
            ind_np=ind_np,
            mode="core"
        )
        
        assert result.metadata.execution_time_ms is not None
        assert result.metadata.execution_time_ms > 0
    
    def test_compute_indicators_short_data(self, filter_test_df):
        """Test compute_indicators with insufficient data."""
        filter_instance = self.create_filter()
        
        # Use first few rows only
        short_df = filter_test_df.iloc[:2]
        
        indicators = {}
        ind_np = {}
        
        # Should not crash
        filter_instance.compute_indicators(short_df, indicators, ind_np)
        
        # Should still produce some output (zeros or NaNs)
        assert len(indicators) > 0
    
    # ========================================================================
    # Real Data Tests
    # ========================================================================
    
    def test_with_real_data(self, real_data_bundle):
        """Test filter on real market data."""
        filter_instance = self.create_filter()
        
        # Compute indicators on real data
        indicators = {}
        ind_np = {}
        filter_instance.compute_indicators(real_data_bundle.strategy, indicators, ind_np)
        
        # Create a signal frame with some signals (at regular intervals)
        signals = pd.Series(0, index=real_data_bundle.strategy.index, dtype=np.int8)
        stride = max(1, len(signals) // 20)  # ~20 signals
        for i in range(0, len(signals), stride):
            signals.iloc[i] = 1 if i % (stride * 2) == 0 else 2
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )
        
        # Apply filter
        result = filter_instance.apply_filter(
            signal_frame=signal_frame,
            df=real_data_bundle.strategy,
            indicators=indicators,
            ind_np=ind_np,
            mode="analytics"
        )
        
        # Basic validation
        assert result.metadata.signals_in >= result.metadata.signals_out
        assert result.metadata.signals_rejected >= 0
        
        # Log results for inspection
        print(f"\n{filter_instance.name} Real Data Test:")
        print(f"  Signals in: {result.metadata.signals_in}")
        print(f"  Signals out: {result.metadata.signals_out}")
        print(f"  Rejected: {result.metadata.signals_rejected}")
        if result.metadata.signals_in > 0:
            print(f"  Pass rate: {result.metadata.signals_out/result.metadata.signals_in*100:.1f}%")