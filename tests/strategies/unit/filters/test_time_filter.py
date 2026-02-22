"""
Unit Tests for TimeFilter
==========================
Tests session hours filtering.
"""

import pytest
import numpy as np
import pandas as pd

from src.strategies.specific.filters.time_filter import TimeFilter
from src.config.config_schema import TimeFilterConfig
from src.strategies.contracts.filter_contracts import FilterStatus
from src.strategies.contracts.signal_contracts import SignalFrame


class TestTimeFilter:
    """Tests for TimeFilter class."""
    
    def create_filter(self, enabled=True, start_hour=8, start_minute=0, 
                      end_hour=16, end_minute=0):
        """
        Create TimeFilter with given config.
        Note: excluded_days is not in TimeFilterConfig, so it's removed.
        """
        config = TimeFilterConfig(
            enabled=enabled,
            session_start_hour=start_hour,
            session_start_minute=start_minute,
            session_end_hour=end_hour,
            session_end_minute=end_minute
        )
        return TimeFilter(config=config)
    
    def test_initialization(self):
        """Test filter initialization."""
        time_filter = self.create_filter(start_hour=9, start_minute=30, end_hour=17, end_minute=30)
        
        assert time_filter.enabled is True
        assert time_filter.session_start_hour == 9
        assert time_filter.session_start_minute == 30
        assert time_filter.session_end_hour == 17
        assert time_filter.session_end_minute == 30
        
        # Check minute conversions
        assert time_filter.session_start_minutes == 9*60 + 30
        assert time_filter.session_end_minutes == 17*60 + 30
    
    def test_disabled_filter(self, sample_signal_frame_with_mixed_signals, filter_test_df):
        """Test that disabled filter passes all signals."""
        time_filter = self.create_filter(enabled=False)
        
        # Use a subset of the signal frame to match filter_test_df length
        signal_subset = sample_signal_frame_with_mixed_signals.signals.iloc[:len(filter_test_df)]
        signal_frame_subset = SignalFrame(
            signals=signal_subset,
            indicator_data=None,
            signal_metadata=sample_signal_frame_with_mixed_signals.signal_metadata
        )
        
        result = time_filter.apply_filter(
            signal_frame=signal_frame_subset,
            df=filter_test_df,
            indicators={},
            ind_np={},
            mode="core"
        )
        
        assert result.passed is True
        assert result.metadata.status == FilterStatus.SKIPPED
        assert result.metadata.signals_in == result.metadata.signals_out
        assert result.metadata.signals_rejected == 0
    
    def test_no_signals(self, sample_signal_frame_no_signals, filter_test_df):
        """Test with no signals."""
        time_filter = self.create_filter()
        
        # Use a subset of the signal frame to match filter_test_df length
        signal_subset = sample_signal_frame_no_signals.signals.iloc[:len(filter_test_df)]
        signal_frame_subset = SignalFrame(
            signals=signal_subset,
            indicator_data=None,
            signal_metadata=sample_signal_frame_no_signals.signal_metadata
        )
        
        result = time_filter.apply_filter(
            signal_frame=signal_frame_subset,
            df=filter_test_df,
            indicators={},
            ind_np={},
            mode="core"
        )
        
        assert result.metadata.signals_in == 0
        assert result.metadata.signals_out == 0
        assert result.metadata.status == FilterStatus.SKIPPED
    
    def test_filter_by_session_hours(self):
        """Test filtering based on session hours."""
        # Create timestamps at various hours
        dates = pd.date_range(start="2025-01-01 08:30:00", periods=24, freq="1h")
        
        signals = pd.Series(0, index=dates, dtype=np.int8)
        for i in range(len(dates)):
            signals.iloc[i] = 1  # All BUY signals
        
        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )
        
        # Filter: 09:00 to 17:00
        time_filter = self.create_filter(start_hour=9, start_minute=0, end_hour=17, end_minute=0)
        
        df_dummy = pd.DataFrame(index=dates)
        result = time_filter.apply_filter(
            signal_frame=signal_frame,
            df=df_dummy,
            indicators={},
            ind_np={},
            mode="analytics"
        )
        
        # Should keep signals from 09:00 to 16:00 (since end is exclusive)
        expected_kept = sum(1 for ts in dates if 9 <= ts.hour < 17)
        
        assert result.metadata.signals_in == len(dates)
        assert result.metadata.signals_out == expected_kept
        assert result.metadata.signals_rejected == len(dates) - expected_kept
        
        # Check that filtered signals are correct
        filtered = result.signal_frame.signals
        for ts in dates:
            if 9 <= ts.hour < 17:
                assert filtered[ts] == 1
            else:
                assert filtered[ts] == 0
    
    def test_excluded_days(self):
        """
        Test exclusion of specific weekdays.
        Note: This test is skipped because TimeFilter doesn't support excluded_days.
        The TimeFilter only filters by time of day, not by day of week.
        """
        pytest.skip("TimeFilter does not support day-of-week exclusion")
    
    def test_boundary_conditions(self):
        """Test exact boundary conditions."""
        # Create timestamps at exactly start and end times
        dates = pd.to_datetime([
            "2025-01-01 09:00:00",  # Start inclusive
            "2025-01-01 17:00:00",  # End exclusive
        ])
        
        signals = pd.Series([1, 1], index=dates, dtype=np.int8)
        signal_frame = SignalFrame(signals=signals, indicator_data=None, signal_metadata={})
        
        # Filter: 09:00 to 17:00
        time_filter = self.create_filter(start_hour=9, start_minute=0, end_hour=17, end_minute=0)
        
        df_dummy = pd.DataFrame(index=dates)
        result = time_filter.apply_filter(
            signal_frame=signal_frame,
            df=df_dummy,
            indicators={},
            ind_np={},
            mode="analytics"
        )
        
        # 09:00 should be kept (inclusive), 17:00 should be rejected (exclusive)
        assert result.metadata.signals_out == 1
        assert result.metadata.signals_rejected == 1
    
    def test_24h_session(self):
        """
        Test 24-hour session (all hours allowed).
        
        Note: Since session_end_hour must be 0-23, we use start=0, end=23
        with end_minute=59 to cover all hours.
        """
        # Create timestamps at every hour
        dates = pd.date_range(start="2025-01-01 00:00:00", periods=24, freq="1h")
        
        signals = pd.Series(1, index=dates, dtype=np.int8)
        signal_frame = SignalFrame(signals=signals, indicator_data=None, signal_metadata={})
        
        # Filter: 00:00 to 23:59 (effectively all day)
        # This covers all hours from 00:00 to 23:00
        time_filter = self.create_filter(start_hour=0, start_minute=0, end_hour=23, end_minute=59)
        
        df_dummy = pd.DataFrame(index=dates)
        result = time_filter.apply_filter(
            signal_frame=signal_frame,
            df=df_dummy,
            indicators={},
            ind_np={},
            mode="analytics"
        )
        
        # All hours from 00:00 to 23:00 should be included
        # 23:00 is included because end_hour=23 is inclusive when checking < end_hour
        # 23:59 is the end minute, but we're checking by hour only
        assert result.metadata.signals_out == 24
        assert result.metadata.signals_rejected == 0
    
    def test_compute_indicators_noop(self, filter_test_df):
        """Test that compute_indicators does nothing."""
        time_filter = self.create_filter()
        
        indicators = {}
        ind_np = {}
        
        time_filter.compute_indicators(filter_test_df, indicators, ind_np)
        
        # Should not add any indicators
        assert len(indicators) == 0
        assert len(ind_np) == 0
    
    def test_timing_collected(self, sample_signal_frame_with_mixed_signals, filter_test_df):
        """Test that execution time is always collected."""
        # Use a subset of the signal frame to match filter_test_df length
        signal_subset = sample_signal_frame_with_mixed_signals.signals.iloc[:len(filter_test_df)]
        signal_frame_subset = SignalFrame(
            signals=signal_subset,
            indicator_data=None,
            signal_metadata=sample_signal_frame_with_mixed_signals.signal_metadata
        )
        
        time_filter = self.create_filter()
        
        result = time_filter.apply_filter(
            signal_frame=signal_frame_subset,
            df=filter_test_df,
            indicators={},
            ind_np={},
            mode="core"
        )
        
        assert result.metadata.execution_time_ms is not None
        assert result.metadata.execution_time_ms > 0
    
    def test_analytics_mode_logging(self, sample_signal_frame_with_mixed_signals, filter_test_df, caplog):
        """Test that analytics mode logs removal rate."""
        # Use a subset of the signal frame to match filter_test_df length
        signal_subset = sample_signal_frame_with_mixed_signals.signals.iloc[:len(filter_test_df)]
        signal_frame_subset = SignalFrame(
            signals=signal_subset,
            indicator_data=None,
            signal_metadata=sample_signal_frame_with_mixed_signals.signal_metadata
        )
        
        time_filter = self.create_filter(start_hour=9, start_minute=0, end_hour=17, end_minute=0)
        
        with caplog.at_level("INFO"):
            result = time_filter.apply_filter(
                signal_frame=signal_frame_subset,
                df=filter_test_df,
                indicators={},
                ind_np={},
                mode="analytics"
            )
        
        if result.metadata.signals_rejected > 0:
            assert "time_filter" in caplog.text
            assert "removed" in caplog.text
    
    # ========================================================================
    # Real Data Test
    # ========================================================================
    
    def test_with_real_data(self, real_data_bundle):
        """Test time filter on real data with actual timestamps."""
        # Create signals on every bar
        signals = pd.Series(1, index=real_data_bundle.strategy.index, dtype=np.int8)
        signal_frame = SignalFrame(signals=signals, indicator_data=None, signal_metadata={})
        
        # Filter for London session (08:00-16:00 UTC)
        time_filter = self.create_filter(
            start_hour=8, start_minute=0,
            end_hour=16, end_minute=0
        )
        
        result = time_filter.apply_filter(
            signal_frame=signal_frame,
            df=real_data_bundle.strategy,
            indicators={},
            ind_np={},
            mode="analytics"
        )
        
        print(f"\nTime Filter Real Data Test:")
        print(f"  Total bars: {len(real_data_bundle.strategy)}")
        print(f"  Signals in: {result.metadata.signals_in}")
        print(f"  Signals out: {result.metadata.signals_out}")
        print(f"  Rejected: {result.metadata.signals_rejected}")
        if result.metadata.signals_in > 0:
            print(f"  Pass rate: {result.metadata.signals_out/result.metadata.signals_in*100:.1f}%")
        
        # Verify that timestamps are within session
        filtered_signals = result.signal_frame.signals
        passed_times = filtered_signals[filtered_signals != 0].index
        
        for ts in passed_times:
            assert 8 <= ts.hour < 16