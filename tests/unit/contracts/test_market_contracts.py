"""
Unit Tests for Market Contracts
=================================
Tests MarketFrame contract for price bar representation.
"""

import pytest
import pandas as pd
import numpy as np

from src.strategies.contracts.market_contracts import MarketFrame


class TestMarketFrame:
    """Tests for MarketFrame contract."""

    @pytest.fixture
    def valid_timestamp(self):
        """Valid timestamp for testing."""
        return pd.Timestamp("2025-01-01 10:30:00")

    def test_valid_market_frame(self, valid_timestamp):
        """Test creating valid market frame."""
        frame = MarketFrame(
            timestamp=valid_timestamp,
            open=100.0,
            high=105.0,
            low=99.0,
            close=102.5,
            volume=1000
        )
        
        assert frame.timestamp == valid_timestamp
        assert frame.open == 100.0
        assert frame.high == 105.0
        assert frame.low == 99.0
        assert frame.close == 102.5
        assert frame.volume == 1000
        assert frame.htf is None
        assert frame.ltf is None
        assert frame.indicators == {}
        assert frame.state == {}

    def test_invalid_ohlc_open_low(self, valid_timestamp):
        """Test validation when open < low."""
        with pytest.raises(ValueError, match="open.*not in range"):
            MarketFrame(
                timestamp=valid_timestamp,
                open=98.0,  # Below low
                high=105.0,
                low=99.0,
                close=102.5,
                volume=1000
            )

    def test_invalid_ohlc_open_high(self, valid_timestamp):
        """Test validation when open > high."""
        with pytest.raises(ValueError, match="open.*not in range"):
            MarketFrame(
                timestamp=valid_timestamp,
                open=106.0,  # Above high
                high=105.0,
                low=99.0,
                close=102.5,
                volume=1000
            )

    def test_invalid_ohlc_close_low(self, valid_timestamp):
        """Test validation when close < low."""
        with pytest.raises(ValueError, match="close.*not in range"):
            MarketFrame(
                timestamp=valid_timestamp,
                open=100.0,
                high=105.0,
                low=99.0,
                close=98.0,  # Below low
                volume=1000
            )

    def test_invalid_ohlc_close_high(self, valid_timestamp):
        """Test validation when close > high."""
        with pytest.raises(ValueError, match="close.*not in range"):
            MarketFrame(
                timestamp=valid_timestamp,
                open=100.0,
                high=105.0,
                low=99.0,
                close=106.0,  # Above high
                volume=1000
            )

    def test_negative_volume(self, valid_timestamp):
        """Test that negative volume raises error."""
        with pytest.raises(ValueError, match="Volume cannot be negative"):
            MarketFrame(
                timestamp=valid_timestamp,
                open=100.0,
                high=105.0,
                low=99.0,
                close=102.5,
                volume=-100
            )

    def test_price_range_property(self, valid_timestamp):
        """Test price_range property."""
        frame = MarketFrame(
            timestamp=valid_timestamp,
            open=100.0,
            high=105.0,
            low=99.0,
            close=102.5,
            volume=1000
        )
        
        assert frame.price_range == 6.0  # 105 - 99

    def test_body_size_property(self, valid_timestamp):
        """Test body_size property."""
        # Bullish bar
        frame_bull = MarketFrame(
            timestamp=valid_timestamp,
            open=100.0,
            high=105.0,
            low=99.0,
            close=102.5,
            volume=1000
        )
        assert frame_bull.body_size == 2.5  # 102.5 - 100
        
        # Bearish bar
        frame_bear = MarketFrame(
            timestamp=valid_timestamp,
            open=102.5,
            high=105.0,
            low=99.0,
            close=100.0,
            volume=1000
        )
        assert frame_bear.body_size == 2.5  # 102.5 - 100

    def test_direction_properties(self, valid_timestamp):
        """Test bullish/bearish/doji properties."""
        # Bullish
        bull = MarketFrame(
            timestamp=valid_timestamp,
            open=100.0,
            high=105.0,
            low=99.0,
            close=102.5,
            volume=1000
        )
        assert bull.is_bullish is True
        assert bull.is_bearish is False
        assert bull.is_doji is False
        
        # Bearish
        bear = MarketFrame(
            timestamp=valid_timestamp,
            open=102.5,
            high=105.0,
            low=99.0,
            close=100.0,
            volume=1000
        )
        assert bear.is_bullish is False
        assert bear.is_bearish is True
        assert bear.is_doji is False
        
        # Doji
        doji = MarketFrame(
            timestamp=valid_timestamp,
            open=100.0,
            high=105.0,
            low=99.0,
            close=100.0,
            volume=1000
        )
        assert doji.is_bullish is False
        assert doji.is_bearish is False
        assert doji.is_doji is True

    def test_wick_properties(self, valid_timestamp):
        """Test upper and lower wick properties."""
        # Bar with upper and lower wicks
        frame = MarketFrame(
            timestamp=valid_timestamp,
            open=102.0,
            high=105.0,
            low=99.0,
            close=101.0,
            volume=1000
        )
        
        # Upper wick: high - max(open, close) = 105 - 102 = 3
        assert frame.upper_wick == 3.0
        
        # Lower wick: min(open, close) - low = 101 - 99 = 2
        assert frame.lower_wick == 2.0

    def test_with_optional_data(self, valid_timestamp):
        """Test frame with HTF and LTF data."""
        htf_series = pd.Series({"open": 100, "high": 110, "low": 95, "close": 105})
        ltf_df = pd.DataFrame({
            "open": [100, 101],
            "high": [101, 102],
            "low": [99, 100],
            "close": [100.5, 101.5]
        })
        indicators = {"rsi": 65.5}
        state = {"trend": "bullish"}
        
        frame = MarketFrame(
            timestamp=valid_timestamp,
            open=100.0,
            high=105.0,
            low=99.0,
            close=102.5,
            volume=1000,
            htf=htf_series,
            ltf=ltf_df,
            indicators=indicators,
            state=state
        )
        
        assert frame.has_htf is True
        assert frame.has_ltf is True
        assert frame.indicators == indicators
        assert frame.state == state

    def test_has_htf_property(self, valid_timestamp):
        """Test has_htf property."""
        # With HTF
        frame_with = MarketFrame(
            timestamp=valid_timestamp,
            open=100.0,
            high=105.0,
            low=99.0,
            close=102.5,
            volume=1000,
            htf=pd.Series({"close": 105})
        )
        assert frame_with.has_htf is True
        
        # Without HTF
        frame_without = MarketFrame(
            timestamp=valid_timestamp,
            open=100.0,
            high=105.0,
            low=99.0,
            close=102.5,
            volume=1000
        )
        assert frame_without.has_htf is False

    def test_has_ltf_property(self, valid_timestamp):
        """Test has_ltf property."""
        # With LTF
        frame_with = MarketFrame(
            timestamp=valid_timestamp,
            open=100.0,
            high=105.0,
            low=99.0,
            close=102.5,
            volume=1000,
            ltf=pd.DataFrame({"close": [101, 102]})
        )
        assert frame_with.has_ltf is True
        
        # Without LTF
        frame_without = MarketFrame(
            timestamp=valid_timestamp,
            open=100.0,
            high=105.0,
            low=99.0,
            close=102.5,
            volume=1000
        )
        assert frame_without.has_ltf is False

    def test_from_series(self, valid_timestamp):
        """Test creating frame from pandas Series."""
        series = pd.Series({
            "open": 100.0,
            "high": 105.0,
            "low": 99.0,
            "close": 102.5,
            "volume": 1000
        }, name=valid_timestamp)
        
        frame = MarketFrame.from_series(series)
        
        assert frame.timestamp == valid_timestamp
        assert frame.open == 100.0
        assert frame.high == 105.0
        assert frame.low == 99.0
        assert frame.close == 102.5
        assert frame.volume == 1000

    def test_from_series_with_optional(self, valid_timestamp):
        """Test creating frame from series with optional data."""
        series = pd.Series({
            "open": 100.0,
            "high": 105.0,
            "low": 99.0,
            "close": 102.5,
            "volume": 1000
        }, name=valid_timestamp)
        
        htf = pd.Series({"close": 105})
        ltf = pd.DataFrame({"close": [101, 102]})
        indicators = {"rsi": 65}
        state = {"trend": "up"}
        
        frame = MarketFrame.from_series(
            series,
            htf=htf,
            ltf=ltf,
            indicators=indicators,
            state=state
        )
        
        assert frame.htf.equals(htf)
        assert frame.ltf.equals(ltf)
        assert frame.indicators == indicators
        assert frame.state == state

    def test_from_series_invalid_timestamp(self):
        """Test from_series with invalid timestamp."""
        series = pd.Series({
            "open": 100.0,
            "high": 105.0,
            "low": 99.0,
            "close": 102.5,
            "volume": 1000
        }, name="not_a_timestamp")
        
        with pytest.raises(ValueError, match="must have a Timestamp"):
            MarketFrame.from_series(series)

    def test_from_series_missing_columns(self, valid_timestamp):
        """Test from_series with missing columns."""
        series = pd.Series({
            "open": 100.0,
            "close": 102.5
        }, name=valid_timestamp)
        
        with pytest.raises(ValueError, match="missing required columns"):
            MarketFrame.from_series(series)

    def test_from_dataframe_row(self, valid_timestamp):
        """Test creating frame from DataFrame row."""
        df = pd.DataFrame({
            "open": [100.0, 101.0],
            "high": [105.0, 106.0],
            "low": [99.0, 100.0],
            "close": [102.5, 103.5],
            "volume": [1000, 1100]
        }, index=[valid_timestamp, pd.Timestamp("2025-01-01 10:31:00")])
        
        frame = MarketFrame.from_dataframe_row(df, valid_timestamp)
        
        assert frame.timestamp == valid_timestamp
        assert frame.open == 100.0
        assert frame.close == 102.5

    def test_from_dataframe_row_missing_timestamp(self, valid_timestamp):
        """Test from_dataframe_row with missing timestamp."""
        df = pd.DataFrame({
            "open": [100.0],
            "high": [105.0],
            "low": [99.0],
            "close": [102.5],
            "volume": [1000]
        }, index=[pd.Timestamp("2025-01-02")])
        
        with pytest.raises(ValueError, match="not found in DataFrame"):
            MarketFrame.from_dataframe_row(df, valid_timestamp)

    def test_to_dict(self, valid_timestamp):
        """Test serialization to dict."""
        frame = MarketFrame(
            timestamp=valid_timestamp,
            open=100.0,
            high=105.0,
            low=99.0,
            close=102.5,
            volume=1000
        )
        
        d = frame.to_dict()
        
        assert d["timestamp"] == valid_timestamp
        assert d["open"] == 100.0
        assert d["high"] == 105.0
        assert d["low"] == 99.0
        assert d["close"] == 102.5
        assert d["volume"] == 1000
        assert d["price_range"] == 6.0
        assert d["body_size"] == 2.5
        assert d["is_bullish"] is True
        assert d["is_bearish"] is False
        assert d["upper_wick"] == 3.0  # 105 - 102 = 3
        assert d["lower_wick"] == 1.0  # 100 - 99 = 1
        assert d["has_htf"] is False
        assert d["has_ltf"] is False

    def test_str_representation_bullish(self, valid_timestamp):
        """Test string representation for bullish bar."""
        frame = MarketFrame(
            timestamp=valid_timestamp,
            open=100.0,
            high=105.0,
            low=99.0,
            close=102.5,
            volume=1000
        )
        
        s = str(frame)
        assert "MarketFrame" in s
        assert "2025-01-01 10:30:00" in s
        assert "↑" in s  # Bullish arrow
        assert "O:100.00 H:105.00 L:99.00 C:102.50" in s

    def test_str_representation_bearish(self, valid_timestamp):
        """Test string representation for bearish bar."""
        frame = MarketFrame(
            timestamp=valid_timestamp,
            open=102.5,
            high=105.0,
            low=99.0,
            close=100.0,
            volume=1000
        )
        
        s = str(frame)
        assert "↓" in s  # Bearish arrow

    def test_str_representation_doji(self, valid_timestamp):
        """Test string representation for doji."""
        frame = MarketFrame(
            timestamp=valid_timestamp,
            open=100.0,
            high=105.0,
            low=99.0,
            close=100.0,
            volume=1000
        )
        
        s = str(frame)
        assert "→" in s  # Doji arrow