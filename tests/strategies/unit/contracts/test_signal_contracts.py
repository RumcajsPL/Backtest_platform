"""
Unit Tests for Signal Contracts
=================================
Tests SignalType enum, Signal, SignalFrame, and SignalStats contracts.
"""

import pytest
import pandas as pd
import numpy as np

from src.strategies.contracts.signal_contracts import (
    SignalType,
    Signal,
    SignalFrame,
    SignalStats
)


class TestSignalType:
    """Tests for SignalType enum."""

    def test_enum_values(self):
        """Test enum values exist."""
        assert SignalType.BUY is not None
        assert SignalType.SELL is not None

    def test_str_representation(self):
        """Test string representation."""
        assert str(SignalType.BUY) == "BUY"
        assert str(SignalType.SELL) == "SELL"

    def test_from_string_valid(self):
        """Test converting valid strings to SignalType."""
        assert SignalType.from_string("BUY") == SignalType.BUY
        assert SignalType.from_string("buy") == SignalType.BUY
        assert SignalType.from_string("SELL") == SignalType.SELL
        assert SignalType.from_string("sell") == SignalType.SELL
        assert SignalType.from_string("LONG") == SignalType.BUY
        assert SignalType.from_string("SHORT") == SignalType.SELL

    def test_from_string_invalid(self):
        """Test converting invalid string returns None."""
        assert SignalType.from_string("INVALID") is None
        assert SignalType.from_string("") is None
        assert SignalType.from_string(None) is None

    def test_from_code_valid(self):
        """Test converting valid codes to SignalType."""
        assert SignalType.from_code(1) == SignalType.BUY
        assert SignalType.from_code(2) == SignalType.SELL

    def test_from_code_invalid(self):
        """Test converting invalid code returns None."""
        assert SignalType.from_code(0) is None
        assert SignalType.from_code(3) is None
        assert SignalType.from_code(-1) is None

    def test_is_long_short_properties(self):
        """Test is_long and is_short properties."""
        assert SignalType.BUY.is_long is True
        assert SignalType.BUY.is_short is False
        assert SignalType.SELL.is_long is False
        assert SignalType.SELL.is_short is True


class TestSignal:
    """Tests for Signal contract."""

    @pytest.fixture
    def sample_timestamp(self):
        """Sample timestamp for testing."""
        return pd.Timestamp("2025-01-01 10:30:00")

    def test_valid_signal(self, sample_timestamp):
        """Test creating valid signal."""
        signal = Signal(
            timestamp=sample_timestamp,
            signal_type=SignalType.BUY,
            mid_price=100.0,
            metadata={"confidence": 0.85}
        )

        assert signal.timestamp == sample_timestamp
        assert signal.signal_type == SignalType.BUY
        assert signal.mid_price == 100.0
        assert signal.metadata == {"confidence": 0.85}

    def test_metadata_default(self, sample_timestamp):
        """Test metadata default to empty dict."""
        signal = Signal(
            timestamp=sample_timestamp,
            signal_type=SignalType.SELL,
            mid_price=100.0
        )

        assert signal.metadata == {}

    def test_validation_positive_price(self, sample_timestamp):
        """Test that mid_price must be positive."""
        with pytest.raises(ValueError, match="mid_price must be positive"):
            Signal(
                timestamp=sample_timestamp,
                signal_type=SignalType.BUY,
                mid_price=0.0
            )

        with pytest.raises(ValueError, match="mid_price must be positive"):
            Signal(
                timestamp=sample_timestamp,
                signal_type=SignalType.BUY,
                mid_price=-10.0
            )

    def test_is_long_short_properties(self, sample_timestamp):
        """Test is_long and is_short properties."""
        buy_signal = Signal(
            timestamp=sample_timestamp,
            signal_type=SignalType.BUY,
            mid_price=100.0
        )
        assert buy_signal.is_long is True
        assert buy_signal.is_short is False

        sell_signal = Signal(
            timestamp=sample_timestamp,
            signal_type=SignalType.SELL,
            mid_price=100.0
        )
        assert sell_signal.is_long is False
        assert sell_signal.is_short is True

    def test_str_representation(self, sample_timestamp):
        """Test string representation."""
        signal = Signal(
            timestamp=sample_timestamp,
            signal_type=SignalType.BUY,
            mid_price=100.0
        )

        s = str(signal)
        assert "BUY" in s
        assert "2025-01-01 10:30:00" in s
        assert "100.00" in s


class TestSignalFrame:
    """Tests for SignalFrame contract."""

    @pytest.fixture
    def sample_dates(self):
        """Sample dates for testing."""
        return pd.date_range(start="2025-01-01", periods=10, freq="1min")

    @pytest.fixture
    def sample_signals(self, sample_dates):
        """Create sample signals series."""
        return pd.Series(
            [1, 0, 2, 0, 1, 0, 2, 0, 1, 0],
            index=sample_dates,
            dtype=np.int8
        )

    @pytest.fixture
    def sample_indicator_data(self, sample_dates):
        """Create sample indicator data."""
        return pd.DataFrame({
            "close": [100.0 + i for i in range(10)],
            "rsi": [50 + i for i in range(10)],
            "we_buy": [True, False, False, False, True, False, False, False, True, False],
            "we_sell": [False, False, True, False, False, False, True, False, False, False]
        }, index=sample_dates)

    def test_valid_signal_frame(self, sample_signals):
        """Test creating valid signal frame."""
        frame = SignalFrame(
            signals=sample_signals,
            indicator_data=None,
            signal_metadata={"source": "test"}
        )

        assert frame.signals.equals(sample_signals)
        assert frame.indicator_data is None
        assert frame.signal_metadata == {"source": "test"}

    def test_validation_datetimeindex(self, sample_signals):
        """Test that signals must have DatetimeIndex."""
        # Remove index
        bad_signals = sample_signals.reset_index(drop=True)

        with pytest.raises(ValueError, match="must have a DatetimeIndex"):
            SignalFrame(signals=bad_signals)

    def test_from_wbws_trigger_core_mode(self, sample_indicator_data, sample_dates):
        """Test creating SignalFrame in core mode (no metadata)."""
        frame = SignalFrame.from_wbws_trigger(
            signals_df=sample_indicator_data,
            strategy_df=sample_indicator_data,
            include_metadata=False
        )

        assert frame.indicator_data is None
        assert frame.signal_metadata["mode"] == "core"
        assert frame.signal_metadata["source"] == "wbws_trigger"

        # Check signal conversion
        assert frame.signals.dtype == np.int8
        counts = frame.count_by_type()
        # Based on we_buy/we_sell columns
        assert counts["buy"] == 3  # indices 0,4,8
        assert counts["sell"] == 2  # indices 2,6

    def test_from_wbws_trigger_analytics_mode(self, sample_indicator_data, sample_dates):
        """Test creating SignalFrame in analytics mode (with metadata)."""
        frame = SignalFrame.from_wbws_trigger(
            signals_df=sample_indicator_data,
            strategy_df=sample_indicator_data,
            include_metadata=True
        )

        assert frame.indicator_data is not None
        assert frame.signal_metadata["mode"] == "analytics"
        assert "close" in frame.indicator_data.columns
        assert "rsi" in frame.indicator_data.columns

    def test_iter_analytics_mode(self, sample_signals, sample_indicator_data):
        """Test iteration in analytics mode."""
        frame = SignalFrame(
            signals=sample_signals,
            indicator_data=sample_indicator_data,
            signal_metadata={"mode": "analytics"}
        )

        signals = list(iter(frame))
        assert len(signals) == 5  # 5 non-zero signals
        for sig in signals:
            assert isinstance(sig, Signal)
            assert sig.mid_price > 0
            assert "rsi" in sig.metadata

    def test_iter_core_mode_raises(self, sample_signals):
        """Test that iteration in core mode raises error (DEC-024)."""
        frame = SignalFrame(
            signals=sample_signals,
            indicator_data=None,
            signal_metadata={"mode": "core"}
        )

        with pytest.raises(RuntimeError, match="requires indicator_data"):
            list(iter(frame))

    def test_iter_raw(self, sample_signals):
        """Test fast raw iteration."""
        frame = SignalFrame(
            signals=sample_signals,
            indicator_data=None,
            signal_metadata={}
        )

        pairs = list(frame.iter_raw())
        assert len(pairs) == 5  # 5 non-zero signals

        timestamps, codes = zip(*pairs)
        assert all(code in (1, 2) for code in codes)

    def test_get_signal_at(self, sample_signals, sample_indicator_data):
        """Test getting signal at specific timestamp."""
        frame = SignalFrame(
            signals=sample_signals,
            indicator_data=sample_indicator_data
        )

        # Existing signal
        signal = frame.get_signal_at(sample_signals.index[0])
        assert signal is not None
        assert signal.signal_type == SignalType.BUY
        assert signal.mid_price == sample_indicator_data.iloc[0]["close"]

        # No signal
        signal = frame.get_signal_at(sample_signals.index[1])
        assert signal is None

        # Non-existent timestamp
        signal = frame.get_signal_at(pd.Timestamp("2026-01-01"))
        assert signal is None

    def test_buy_signals_property(self, sample_signals):
        """Test buy_signals property."""
        frame = SignalFrame(signals=sample_signals)
        buy_series = frame.buy_signals

        assert len(buy_series) == 3  # 3 BUY signals
        assert (buy_series == 1).all()

    def test_sell_signals_property(self, sample_signals):
        """Test sell_signals property."""
        frame = SignalFrame(signals=sample_signals)
        sell_series = frame.sell_signals

        assert len(sell_series) == 2  # 2 SELL signals
        assert (sell_series == 2).all()

    def test_count_by_type(self, sample_signals):
        """Test vectorised counting."""
        frame = SignalFrame(signals=sample_signals)
        counts = frame.count_by_type()

        assert counts["buy"] == 3
        assert counts["sell"] == 2
        assert counts["total"] == 5

    def test_len(self, sample_signals):
        """Test __len__ method."""
        frame = SignalFrame(signals=sample_signals)
        assert len(frame) == 10

    def test_str_representation(self, sample_signals):
        """Test string representation."""
        frame = SignalFrame(
            signals=sample_signals,
            signal_metadata={"mode": "analytics"}
        )
        s = str(frame)

        assert "SignalFrame" in s
        assert "5 signals" in s
        assert "3 BUY" in s
        assert "2 SELL" in s
        assert "mode=analytics" in s


class TestSignalStats:
    """Tests for SignalStats contract."""

    def test_default_stats(self):
        """Test default signal stats."""
        stats = SignalStats()

        assert stats.buy_count == 0
        assert stats.sell_count == 0
        assert stats.total_count == 0
        assert stats.buy_percentage == 0.0
        assert stats.sell_percentage == 0.0
        assert stats.metadata == {}

    def test_custom_stats(self):
        """Test creating custom stats."""
        stats = SignalStats(
            buy_count=10,
            sell_count=5,
            total_count=15,
            buy_percentage=66.67,
            sell_percentage=33.33,
            metadata={"source": "test"}
        )

        assert stats.buy_count == 10
        assert stats.sell_count == 5
        assert stats.total_count == 15
        assert stats.buy_percentage == 66.67
        assert stats.sell_percentage == 33.33
        assert stats.metadata == {"source": "test"}

    @pytest.fixture
    def sample_signal_frame(self):
        """Create sample signal frame for stats testing."""
        dates = pd.date_range(start="2025-01-01", periods=100, freq="1min")
        signals = pd.Series(
            np.random.choice([0, 1, 2], 100, p=[0.7, 0.15, 0.15]),
            index=dates,
            dtype=np.int8
        )
        return SignalFrame(
            signals=signals,
            signal_metadata={"strategy": "test"}
        )

    def test_from_signal_frame_verbose(self, sample_signal_frame):
        """Test creating stats from signal frame with metadata."""
        stats = SignalStats.from_signal_frame(
            signal_frame=sample_signal_frame,
            verbose=True
        )

        counts = sample_signal_frame.count_by_type()
        assert stats.buy_count == counts["buy"]
        assert stats.sell_count == counts["sell"]
        assert stats.total_count == counts["total"]
        assert stats.metadata == {"strategy": "test"}

    def test_from_signal_frame_non_verbose(self, sample_signal_frame):
        """Test creating stats from signal frame without metadata."""
        stats = SignalStats.from_signal_frame(
            signal_frame=sample_signal_frame,
            verbose=False
        )

        counts = sample_signal_frame.count_by_type()
        assert stats.buy_count == counts["buy"]
        assert stats.sell_count == counts["sell"]
        assert stats.total_count == counts["total"]
        assert stats.metadata == {}

    def test_to_dict(self):
        """Test serialization to dict."""
        stats = SignalStats(
            buy_count=10,
            sell_count=5,
            total_count=15,
            buy_percentage=66.666666,
            sell_percentage=33.333333,
            metadata={"source": "test"}
        )

        d = stats.to_dict()

        assert d["buy"] == 10
        assert d["sell"] == 5
        assert d["total"] == 15
        assert d["buy_percentage"] == 66.67  # Rounded
        assert d["sell_percentage"] == 33.33  # Rounded
        assert d["source"] == "test"

    def test_str_representation(self):
        """Test string representation."""
        stats = SignalStats(
            buy_count=10,
            sell_count=5,
            total_count=15,
            buy_percentage=66.67,
            sell_percentage=33.33
        )

        s = str(stats)
        assert "BUY: 10 (66.7%)" in s
        assert "SELL: 5 (33.3%)" in s
        assert "Total: 15" in s