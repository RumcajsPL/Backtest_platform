"""
Unit Tests for Position Contracts
===================================
Tests Position contract for open position tracking.
"""

import pytest
import pandas as pd

from src.strategies.contracts.position_contracts import Position
from src.strategies.contracts.trade_contracts import TradeDirection


class TestPosition:
    """Tests for Position contract."""

    @pytest.fixture
    def sample_timestamp(self):
        """Sample timestamp for testing."""
        return pd.Timestamp("2025-01-01 10:30:00")

    def test_valid_long_position(self, sample_timestamp):
        """Test creating valid LONG position."""
        position = Position(
            position_id=1,
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=99.0,
            take_profit=105.0,
            size=1.0,
            open_time=sample_timestamp,
            meta={"signal_id": 123}
        )

        assert position.position_id == 1
        assert position.direction == TradeDirection.LONG
        assert position.entry_price == 100.0
        assert position.stop_loss == 99.0
        assert position.take_profit == 105.0
        assert position.size == 1.0
        assert position.open_time == sample_timestamp
        assert position.meta == {"signal_id": 123}

    def test_valid_short_position(self, sample_timestamp):
        """Test creating valid SHORT position."""
        position = Position(
            position_id=2,
            direction=TradeDirection.SHORT,
            entry_price=100.0,
            stop_loss=101.0,
            take_profit=95.0,
            size=1.0,
            open_time=sample_timestamp
        )

        assert position.position_id == 2
        assert position.direction == TradeDirection.SHORT
        assert position.entry_price == 100.0
        assert position.stop_loss == 101.0
        assert position.take_profit == 95.0

    def test_validation_entry_price_positive(self, sample_timestamp):
        """Test that entry price must be positive."""
        with pytest.raises(ValueError, match="Entry price must be positive"):
            Position(
                position_id=1,
                direction=TradeDirection.LONG,
                entry_price=0.0,
                stop_loss=99.0,
                take_profit=105.0,
                size=1.0,
                open_time=sample_timestamp
            )

    def test_validation_position_size_positive(self, sample_timestamp):
        """Test that position size must be positive."""
        with pytest.raises(ValueError, match="Position size must be positive"):
            Position(
                position_id=1,
                direction=TradeDirection.LONG,
                entry_price=100.0,
                stop_loss=99.0,
                take_profit=105.0,
                size=0.0,
                open_time=sample_timestamp
            )

    def test_validation_long_sl_tp_ordering(self, sample_timestamp):
        """Test LONG position SL/TP ordering validation."""
        # SL >= entry
        with pytest.raises(ValueError, match="Invalid LONG position"):
            Position(
                position_id=1,
                direction=TradeDirection.LONG,
                entry_price=100.0,
                stop_loss=101.0,  # SL above entry
                take_profit=105.0,
                size=1.0,
                open_time=sample_timestamp
            )

        # TP <= entry
        with pytest.raises(ValueError, match="Invalid LONG position"):
            Position(
                position_id=1,
                direction=TradeDirection.LONG,
                entry_price=100.0,
                stop_loss=99.0,
                take_profit=95.0,  # TP below entry
                size=1.0,
                open_time=sample_timestamp
            )

    def test_validation_short_sl_tp_ordering(self, sample_timestamp):
        """Test SHORT position SL/TP ordering validation."""
        # SL <= entry
        with pytest.raises(ValueError, match="Invalid SHORT position"):
            Position(
                position_id=1,
                direction=TradeDirection.SHORT,
                entry_price=100.0,
                stop_loss=99.0,  # SL below entry
                take_profit=95.0,
                size=1.0,
                open_time=sample_timestamp
            )

        # TP >= entry
        with pytest.raises(ValueError, match="Invalid SHORT position"):
            Position(
                position_id=1,
                direction=TradeDirection.SHORT,
                entry_price=100.0,
                stop_loss=101.0,
                take_profit=105.0,  # TP above entry
                size=1.0,
                open_time=sample_timestamp
            )

    def test_is_long_property(self, sample_timestamp):
        """Test is_long property."""
        long_pos = Position(
            position_id=1,
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=99.0,
            take_profit=105.0,
            size=1.0,
            open_time=sample_timestamp
        )
        assert long_pos.is_long is True
        assert long_pos.is_short is False

        short_pos = Position(
            position_id=2,
            direction=TradeDirection.SHORT,
            entry_price=100.0,
            stop_loss=101.0,
            take_profit=95.0,
            size=1.0,
            open_time=sample_timestamp
        )
        assert short_pos.is_long is False
        assert short_pos.is_short is True

    def test_sl_distance_property(self, sample_timestamp):
        """Test stop loss distance property."""
        long_pos = Position(
            position_id=1,
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=98.0,
            take_profit=105.0,
            size=1.0,
            open_time=sample_timestamp
        )
        assert long_pos.sl_distance == 2.0

        short_pos = Position(
            position_id=2,
            direction=TradeDirection.SHORT,
            entry_price=100.0,
            stop_loss=102.0,
            take_profit=95.0,
            size=1.0,
            open_time=sample_timestamp
        )
        assert short_pos.sl_distance == 2.0

    def test_tp_distance_property(self, sample_timestamp):
        """Test take profit distance property."""
        long_pos = Position(
            position_id=1,
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=98.0,
            take_profit=107.0,
            size=1.0,
            open_time=sample_timestamp
        )
        assert long_pos.tp_distance == 7.0

        short_pos = Position(
            position_id=2,
            direction=TradeDirection.SHORT,
            entry_price=100.0,
            stop_loss=102.0,
            take_profit=93.0,
            size=1.0,
            open_time=sample_timestamp
        )
        assert short_pos.tp_distance == 7.0

    def test_risk_reward_ratio_property(self, sample_timestamp):
        """Test risk:reward ratio calculation."""
        pos = Position(
            position_id=1,
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=99.0,  # Risk: 1.0
            take_profit=105.0,  # Reward: 5.0
            size=1.0,
            open_time=sample_timestamp
        )
        assert pos.risk_reward_ratio == 5.0

        # Zero risk case
        pos_zero = Position(
            position_id=2,
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=100.0,  # No risk
            take_profit=105.0,
            size=1.0,
            open_time=sample_timestamp
        )
        assert pos_zero.risk_reward_ratio == 0.0

    def test_get_unrealized_pnl_long(self, sample_timestamp):
        """Test unrealized P&L for LONG position."""
        pos = Position(
            position_id=1,
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=99.0,
            take_profit=105.0,
            size=1.0,
            open_time=sample_timestamp
        )

        # Profit
        assert pos.get_unrealized_pnl(105.0) == 5.0

        # Loss
        assert pos.get_unrealized_pnl(95.0) == -5.0

        # Break even
        assert pos.get_unrealized_pnl(100.0) == 0.0

    def test_get_unrealized_pnl_short(self, sample_timestamp):
        """Test unrealized P&L for SHORT position."""
        pos = Position(
            position_id=1,
            direction=TradeDirection.SHORT,
            entry_price=100.0,
            stop_loss=101.0,
            take_profit=95.0,
            size=1.0,
            open_time=sample_timestamp
        )

        # Profit (price went down)
        assert pos.get_unrealized_pnl(95.0) == 5.0

        # Loss (price went up)
        assert pos.get_unrealized_pnl(105.0) == -5.0

        # Break even
        assert pos.get_unrealized_pnl(100.0) == 0.0

    def test_get_unrealized_pnl_percent(self, sample_timestamp):
        """Test unrealized P&L percentage calculation."""
        pos = Position(
            position_id=1,
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=99.0,
            take_profit=105.0,
            size=1.0,
            open_time=sample_timestamp
        )

        # 5% profit
        assert pos.get_unrealized_pnl_percent(105.0) == 5.0

        # 5% loss
        assert pos.get_unrealized_pnl_percent(95.0) == -5.0

    def test_is_sl_hit_long(self, sample_timestamp):
        """Test SL hit detection for LONG position."""
        pos = Position(
            position_id=1,
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=98.0,
            take_profit=105.0,
            size=1.0,
            open_time=sample_timestamp
        )

        # Price above SL
        assert pos.is_sl_hit(99.0) is False

        # Price at SL
        assert pos.is_sl_hit(98.0) is True

        # Price below SL
        assert pos.is_sl_hit(97.0) is True

        # With tolerance
        assert pos.is_sl_hit(98.1, tolerance=0.1) is True
        assert pos.is_sl_hit(98.2, tolerance=0.1) is False

    def test_is_sl_hit_short(self, sample_timestamp):
        """Test SL hit detection for SHORT position."""
        pos = Position(
            position_id=1,
            direction=TradeDirection.SHORT,
            entry_price=100.0,
            stop_loss=102.0,
            take_profit=95.0,
            size=1.0,
            open_time=sample_timestamp
        )

        # Price below SL
        assert pos.is_sl_hit(101.0) is False

        # Price at SL
        assert pos.is_sl_hit(102.0) is True

        # Price above SL
        assert pos.is_sl_hit(103.0) is True

        # With tolerance
        assert pos.is_sl_hit(101.9, tolerance=0.1) is True
        assert pos.is_sl_hit(101.8, tolerance=0.1) is False

    def test_is_tp_hit_long(self, sample_timestamp):
        """Test TP hit detection for LONG position."""
        pos = Position(
            position_id=1,
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=98.0,
            take_profit=105.0,
            size=1.0,
            open_time=sample_timestamp
        )

        # Price below TP
        assert pos.is_tp_hit(104.0) is False

        # Price at TP
        assert pos.is_tp_hit(105.0) is True

        # Price above TP
        assert pos.is_tp_hit(106.0) is True

        # With tolerance
        assert pos.is_tp_hit(104.9, tolerance=0.1) is True
        assert pos.is_tp_hit(104.8, tolerance=0.1) is False

    def test_is_tp_hit_short(self, sample_timestamp):
        """Test TP hit detection for SHORT position."""
        pos = Position(
            position_id=1,
            direction=TradeDirection.SHORT,
            entry_price=100.0,
            stop_loss=102.0,
            take_profit=95.0,
            size=1.0,
            open_time=sample_timestamp
        )

        # Price above TP
        assert pos.is_tp_hit(96.0) is False

        # Price at TP
        assert pos.is_tp_hit(95.0) is True

        # Price below TP
        assert pos.is_tp_hit(94.0) is True

        # With tolerance
        assert pos.is_tp_hit(95.1, tolerance=0.1) is True
        assert pos.is_tp_hit(95.2, tolerance=0.1) is False

    def test_to_dict(self, sample_timestamp):
        """Test serialization to dict."""
        pos = Position(
            position_id=42,
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=98.0,
            take_profit=105.0,
            size=1.5,
            open_time=sample_timestamp,
            meta={"strategy": "test"}
        )

        d = pos.to_dict()

        assert d['position_id'] == 42
        assert d['direction'] == "BUY"
        assert d['entry_price'] == 100.0
        assert d['stop_loss'] == 98.0
        assert d['take_profit'] == 105.0
        assert d['size'] == 1.5
        assert d['open_time'] == sample_timestamp
        assert d['sl_distance'] == 2.0
        assert d['tp_distance'] == 5.0
        assert d['risk_reward_ratio'] == 2.5

    def test_str_representation(self, sample_timestamp):
        """Test string representation."""
        pos = Position(
            position_id=42,
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=98.0,
            take_profit=105.0,
            size=1.5,
            open_time=sample_timestamp
        )

        s = str(pos)
        assert "Position" in s
        assert "42" in s
        assert "BUY" in s
        assert "100.00" in s
        assert "98.00" in s
        assert "105.00" in s