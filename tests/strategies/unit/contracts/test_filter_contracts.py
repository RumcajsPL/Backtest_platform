"""
Unit Tests for Filter Contracts
=================================
Tests FilterStatus enum, FilterMetadata, FilterResult, FilterPipelineResult,
and FilterProtocol.
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock

from src.strategies.contracts.filter_contracts import (
    FilterStatus,
    FilterMetadata,
    FilterResult,
    FilterPipelineResult,
    FilterProtocol
)
from src.strategies.contracts.signal_contracts import SignalFrame


class TestFilterStatus:
    """Tests for FilterStatus enum."""

    def test_enum_values(self):
        """Test enum values exist."""
        assert FilterStatus.PASSED is not None
        assert FilterStatus.REJECTED is not None
        assert FilterStatus.SKIPPED is not None
        assert FilterStatus.ERROR is not None

    def test_str_representation(self):
        """Test string representation."""
        assert str(FilterStatus.PASSED) == "PASSED"
        assert str(FilterStatus.REJECTED) == "REJECTED"
        assert str(FilterStatus.SKIPPED) == "SKIPPED"
        assert str(FilterStatus.ERROR) == "ERROR"


class TestFilterMetadata:
    """Tests for FilterMetadata contract."""

    def test_valid_metadata(self):
        """Test creating valid filter metadata."""
        metadata = FilterMetadata(
            filter_name="rsi_filter",
            status=FilterStatus.PASSED,
            signals_in=100,
            signals_out=80,
            signals_rejected=20,
            reason="RSI filter passed",
            indicator_values={"rsi_mean": 55.5},
            execution_time_ms=1.5
        )
        
        assert metadata.filter_name == "rsi_filter"
        assert metadata.status == FilterStatus.PASSED
        assert metadata.signals_in == 100
        assert metadata.signals_out == 80
        assert metadata.signals_rejected == 20
        assert metadata.reason == "RSI filter passed"
        assert metadata.indicator_values == {"rsi_mean": 55.5}
        assert metadata.execution_time_ms == 1.5

    def test_signals_rejected_auto_calculation(self):
        """Test that signals_rejected is auto-calculated if not provided."""
        metadata = FilterMetadata(
            filter_name="test",
            status=FilterStatus.PASSED,
            signals_in=100,
            signals_out=75,
            signals_rejected=0  # Should be overridden
        )
        
        assert metadata.signals_rejected == 25  # 100 - 75

    def test_rejection_rate(self):
        """Test rejection rate calculation."""
        metadata = FilterMetadata(
            filter_name="test",
            status=FilterStatus.PASSED,
            signals_in=100,
            signals_out=60
        )
        
        assert metadata.rejection_rate == 40.0  # 40% rejected
        
        # Zero signals in
        metadata_zero = FilterMetadata(
            filter_name="test",
            status=FilterStatus.SKIPPED,
            signals_in=0,
            signals_out=0
        )
        assert metadata_zero.rejection_rate == 0.0

    def test_to_dict(self):
        """Test serialization to dict."""
        metadata = FilterMetadata(
            filter_name="rsi_filter",
            status=FilterStatus.PASSED,
            signals_in=100,
            signals_out=80,
            reason="Test reason",
            indicator_values={"rsi_mean": 55.5},
            execution_time_ms=1.5
        )
        
        d = metadata.to_dict()
        
        assert d["filter_name"] == "rsi_filter"
        assert d["status"] == "PASSED"
        assert d["signals_in"] == 100
        assert d["signals_out"] == 80
        assert d["signals_rejected"] == 20
        # The rejection rate is calculated as (signals_rejected/signals_in)*100
        # 20/100*100 = 20%, not 40%
        assert "20.0%" in d["rejection_rate"]
        assert d["reason"] == "Test reason"
        assert d["indicator_values"] == {"rsi_mean": 55.5}
        assert d["execution_time_ms"] == 1.5

    def test_str_representation_passed(self):
        """Test string representation for passed filter."""
        metadata = FilterMetadata(
            filter_name="rsi_filter",
            status=FilterStatus.PASSED,
            signals_in=100,
            signals_out=80
        )
        
        s = str(metadata)
        assert "✅" in s
        assert "rsi_filter" in s
        assert "100 → 80" in s
        assert "-20" in s
        assert "20.0%" in s

    def test_str_representation_rejected(self):
        """Test string representation for rejected filter."""
        metadata = FilterMetadata(
            filter_name="rsi_filter",
            status=FilterStatus.REJECTED,
            signals_in=100,
            signals_out=0,
            reason="All rejected"
        )
        
        s = str(metadata)
        assert "❌" in s
        assert "All rejected" in s

    def test_str_representation_skipped(self):
        """Test string representation for skipped filter."""
        metadata = FilterMetadata(
            filter_name="rsi_filter",
            status=FilterStatus.SKIPPED,
            signals_in=100,
            signals_out=100
        )
        
        s = str(metadata)
        assert "⏭️" in s

    def test_str_representation_error(self):
        """Test string representation for error filter."""
        metadata = FilterMetadata(
            filter_name="rsi_filter",
            status=FilterStatus.ERROR,
            signals_in=100,
            signals_out=0
        )
        
        s = str(metadata)
        assert "⚠️" in s


class TestFilterResult:
    """Tests for FilterResult contract."""

    @pytest.fixture
    def sample_signal_frame(self):
        """Create sample signal frame."""
        dates = pd.date_range(start="2025-01-01", periods=10, freq="1min")
        signals = pd.Series([1, 0, 2, 0, 1, 0, 2, 0, 1, 0], 
                           index=dates, dtype=np.int8)
        
        return SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )

    def test_valid_result(self, sample_signal_frame):
        """Test creating valid filter result."""
        metadata = FilterMetadata(
            filter_name="test",
            status=FilterStatus.PASSED,
            signals_in=10,
            signals_out=5
        )
        
        result = FilterResult(
            passed=True,
            signal_frame=sample_signal_frame,
            metadata=metadata
        )
        
        assert result.passed is True
        assert result.signal_frame == sample_signal_frame
        assert result.metadata == metadata

    def test_signals_count_property(self, sample_signal_frame):
        """Test signals_count property."""
        metadata = FilterMetadata(
            filter_name="test",
            status=FilterStatus.PASSED,
            signals_in=10,
            signals_out=5
        )
        
        result = FilterResult(
            passed=True,
            signal_frame=sample_signal_frame,
            metadata=metadata
        )
        
        # Should count non-zero signals
        assert result.signals_count == 5  # 5 signals in sample

    def test_is_empty_property(self, sample_signal_frame):
        """Test is_empty property."""
        metadata = FilterMetadata(
            filter_name="test",
            status=FilterStatus.PASSED,
            signals_in=10,
            signals_out=5
        )
        
        # Non-empty
        result = FilterResult(
            passed=True,
            signal_frame=sample_signal_frame,
            metadata=metadata
        )
        assert result.is_empty is False
        
        # Empty (no signals passed)
        empty_frame = SignalFrame(
            signals=pd.Series([0]*10, index=sample_signal_frame.signals.index, dtype=np.int8),
            indicator_data=None,
            signal_metadata={}
        )
        
        result_empty = FilterResult(
            passed=False,
            signal_frame=empty_frame,
            metadata=metadata
        )
        assert result_empty.is_empty is True

    def test_str_representation(self, sample_signal_frame):
        """Test string representation."""
        metadata = FilterMetadata(
            filter_name="test",
            status=FilterStatus.PASSED,
            signals_in=10,
            signals_out=5
        )
        
        result = FilterResult(
            passed=True,
            signal_frame=sample_signal_frame,
            metadata=metadata
        )
        
        s = str(result)
        assert "FilterResult" in s
        assert str(metadata) in s


class TestFilterPipelineResult:
    """Tests for FilterPipelineResult contract."""

    @pytest.fixture
    def sample_filter_results(self):
        """Create sample filter results."""
        return [
            FilterMetadata(
                filter_name="time_filter",
                status=FilterStatus.PASSED,
                signals_in=100,
                signals_out=80
            ),
            FilterMetadata(
                filter_name="rsi_filter",
                status=FilterStatus.PASSED,
                signals_in=80,
                signals_out=60
            ),
            FilterMetadata(
                filter_name="adx_filter",
                status=FilterStatus.PASSED,
                signals_in=60,
                signals_out=50
            )
        ]

    @pytest.fixture
    def sample_rejection_reasons(self):
        """Sample rejection reasons."""
        return {
            "time_filter": 20,
            "rsi_filter": 20,
            "adx_filter": 10
        }

    @pytest.fixture
    def final_signals(self):
        """Create final signals with DatetimeIndex."""
        dates = pd.date_range(start="2025-01-01", periods=50, freq="1min")
        signals = pd.Series([1]*50, index=dates, dtype=np.int8)
        return SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )

    def test_valid_result(self, sample_filter_results, sample_rejection_reasons, final_signals):
        """Test creating valid pipeline result."""
        result = FilterPipelineResult(
            final_signals=final_signals,
            raw_count=100,
            time_filtered_count=80,
            technical_filtered_count=50,
            final_count=50,
            filter_results=sample_filter_results,
            rejection_reasons=sample_rejection_reasons,
            execution_time_ms=15.5
        )
        
        assert result.raw_count == 100
        assert result.time_filtered_count == 80
        assert result.technical_filtered_count == 50
        assert result.final_count == 50
        assert len(result.filter_results) == 3
        assert result.rejection_reasons == sample_rejection_reasons
        assert result.execution_time_ms == 15.5

    def test_rejection_counts(self, sample_filter_results, final_signals):
        """Test rejection count properties."""
        result = FilterPipelineResult(
            final_signals=final_signals,
            raw_count=100,
            time_filtered_count=80,
            technical_filtered_count=50,
            final_count=50,
            filter_results=sample_filter_results
        )
        
        assert result.time_rejection_count == 20  # 100 - 80
        assert result.technical_rejection_count == 30  # 80 - 50
        assert result.total_rejection_count == 50  # 100 - 50

    def test_pass_rate(self, sample_filter_results, final_signals):
        """Test pass rate calculation."""
        # Normal case
        result = FilterPipelineResult(
            final_signals=final_signals,
            raw_count=100,
            time_filtered_count=80,
            technical_filtered_count=50,
            final_count=50,
            filter_results=sample_filter_results
        )
        assert result.pass_rate == 50.0
        
        # Zero raw count
        result_zero = FilterPipelineResult(
            final_signals=final_signals,
            raw_count=0,
            time_filtered_count=0,
            technical_filtered_count=0,
            final_count=0,
            filter_results=[]
        )
        assert result_zero.pass_rate == 0.0

    def test_to_dict(self, sample_filter_results, sample_rejection_reasons, final_signals):
        """Test serialization to dict."""
        result = FilterPipelineResult(
            final_signals=final_signals,
            raw_count=100,
            time_filtered_count=80,
            technical_filtered_count=50,
            final_count=50,
            filter_results=sample_filter_results,
            rejection_reasons=sample_rejection_reasons,
            execution_time_ms=15.5
        )
        
        d = result.to_dict()
        
        assert d["counts"]["raw"] == 100
        assert d["counts"]["time_filtered"] == 80
        assert d["counts"]["technical_filtered"] == 50
        assert d["counts"]["final"] == 50
        assert d["rejections"]["time_filter"] == 20
        assert d["rejections"]["technical_filters"] == 30
        assert d["rejections"]["total"] == 50
        assert "50.0%" in d["pass_rate"]
        assert len(d["filters"]) == 3
        assert d["rejection_reasons"] == sample_rejection_reasons
        assert d["execution_time_ms"] == 15.5

    def test_get_stats_summary(self, sample_filter_results, final_signals):
        """Test stats summary generation."""
        result = FilterPipelineResult(
            final_signals=final_signals,
            raw_count=100,
            time_filtered_count=80,
            technical_filtered_count=50,
            final_count=50,
            filter_results=sample_filter_results,
            execution_time_ms=15.5
        )
        
        summary = result.get_stats_summary()
        
        assert "Raw signals: 100" in summary
        assert "Time filtered: 80" in summary
        assert "Technical filtered: 50" in summary
        assert "Final signals: 50" in summary
        assert "Pass rate: 50.0%" in summary
        assert "Execution time: 15.5ms" in summary

    def test_str_representation(self, sample_filter_results, final_signals):
        """Test string representation."""
        result = FilterPipelineResult(
            final_signals=final_signals,
            raw_count=100,
            time_filtered_count=80,
            technical_filtered_count=50,
            final_count=50,
            filter_results=sample_filter_results
        )
        
        s = str(result)
        assert "FilterPipelineResult" in s
        assert "50/100 signals" in s
        assert "50.0%" in s

class TestFilterProtocol:
    """Tests for FilterProtocol interface."""

    def test_protocol_methods_exist(self):
        """Test that protocol defines required methods."""
        # The FilterProtocol is a Protocol class that defines an interface
        # It defines what methods a class must implement to be considered a protocol implementer
        
        # Check that the protocol defines the expected methods
        assert hasattr(FilterProtocol, "compute_indicators")
        assert hasattr(FilterProtocol, "apply_filter")
        
        # Note: 'name' and 'enabled' are expected to be instance attributes,
        # not class attributes, so they won't appear in FilterProtocol.__dict__
        # They are part of the protocol specification but are implemented as instance variables

    def test_can_implement_protocol(self):
        """Test that a class can implement the protocol."""
        # Create a mock that implements the protocol
        mock_filter = Mock(spec=FilterProtocol)
        
        # Set the required attributes
        mock_filter.name = "test_filter"
        mock_filter.enabled = True
        
        # These should exist
        assert mock_filter.name == "test_filter"
        assert mock_filter.enabled is True
        assert hasattr(mock_filter, "compute_indicators")
        assert hasattr(mock_filter, "apply_filter")