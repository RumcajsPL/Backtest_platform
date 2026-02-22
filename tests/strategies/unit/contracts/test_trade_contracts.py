"""
Unit Tests for Trade Contracts
================================
Tests TradeDirection, ExitReason, DecisionType enums, and all trade-related dataclasses.
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta

from src.strategies.contracts.trade_contracts import (
    TradeDirection,
    ExitReason,
    DecisionType,
    TradeParameters,
    TradeEntry,
    TradeExit,
    Trade,
    RejectedSignal,
    TradeResult,
    TradeDecision
)


class TestTradeDirection:
    """Tests for TradeDirection enum."""

    def test_enum_values(self):
        """Test enum values."""
        assert TradeDirection.LONG.value == 1
        assert TradeDirection.SHORT.value == -1

    def test_from_string_valid(self):
        """Test converting valid strings."""
        assert TradeDirection.from_string("BUY") == TradeDirection.LONG
        assert TradeDirection.from_string("LONG") == TradeDirection.LONG
        assert TradeDirection.from_string("SELL") == TradeDirection.SHORT
        assert TradeDirection.from_string("SHORT") == TradeDirection.SHORT
        assert TradeDirection.from_string("buy") == TradeDirection.LONG

    def test_from_string_invalid(self):
        """Test converting invalid string raises error."""
        with pytest.raises(ValueError, match="Invalid direction"):
            TradeDirection.from_string("INVALID")

    def test_to_string(self):
        """Test conversion to string."""
        assert TradeDirection.LONG.to_string() == "BUY"
        assert TradeDirection.SHORT.to_string() == "SELL"

    def test_is_long_short_properties(self):
        """Test is_long and is_short properties."""
        assert TradeDirection.LONG.is_long is True
        assert TradeDirection.LONG.is_short is False
        assert TradeDirection.SHORT.is_long is False
        assert TradeDirection.SHORT.is_short is True


class TestExitReason:
    """Tests for ExitReason enum."""

    def test_enum_values(self):
        """Test enum values exist."""
        assert ExitReason.STOP_LOSS is not None
        assert ExitReason.TAKE_PROFIT is not None
        assert ExitReason.OPPOSITE_SIGNAL is not None
        assert ExitReason.END_OF_DATA is not None
        assert ExitReason.MANUAL is not None
        assert ExitReason.TIME_EXIT is not None

    def test_from_string_valid(self):
        """Test converting valid strings."""
        assert ExitReason.from_string("STOP_LOSS") == ExitReason.STOP_LOSS
        assert ExitReason.from_string("TAKE_PROFIT") == ExitReason.TAKE_PROFIT
        assert ExitReason.from_string("OPPOSITE_SIGNAL") == ExitReason.OPPOSITE_SIGNAL
        assert ExitReason.from_string("END_OF_DATA") == ExitReason.END_OF_DATA

    def test_from_string_invalid(self):
        """Test converting invalid string raises error."""
        with pytest.raises(ValueError, match="Invalid exit reason"):
            ExitReason.from_string("INVALID")

    def test_to_string(self):
        """Test conversion to string."""
        assert ExitReason.STOP_LOSS.to_string() == "STOP_LOSS"


class TestDecisionType:
    """Tests for DecisionType enum."""

    def test_enum_values(self):
        """Test enum values exist."""
        assert DecisionType.NONE is not None
        assert DecisionType.OPEN is not None
        assert DecisionType.CLOSE is not None
        assert DecisionType.REVERSE is not None
        assert DecisionType.MODIFY is not None
        assert DecisionType.REJECT is not None
        assert DecisionType.CLOSE_AND_REVERSE is not None

    def test_from_string_valid(self):
        """Test converting valid strings."""
        assert DecisionType.from_string("OPEN") == DecisionType.OPEN
        assert DecisionType.from_string("CLOSE") == DecisionType.CLOSE
        assert DecisionType.from_string("REJECT") == DecisionType.REJECT

    def test_from_string_invalid(self):
        """Test converting invalid string raises error."""
        with pytest.raises(ValueError, match="Invalid decision type"):
            DecisionType.from_string("INVALID")


class TestTradeParameters:
    """Tests for TradeParameters contract."""

    def test_valid_parameters(self):
        """Test creating valid trade parameters."""
        params = TradeParameters(
            entry_price_mid=100.0,
            entry_price_executed=100.5,
            stop_loss_raw=99.0,
            stop_loss_trigger=98.5,
            take_profit=105.0,
            position_size=1.0,
            atr_value=2.5,
            sl_distance=2.0,
            tp_distance=5.0,
            risk_reward_ratio=2.5,
            spread_enabled=True,
            spread_points=0.5,
            comment="Test trade"
        )

        assert params.entry_price_mid == 100.0
        assert params.entry_price_executed == 100.5
        assert params.stop_loss_raw == 99.0
        assert params.stop_loss_trigger == 98.5
        assert params.take_profit == 105.0
        assert params.position_size == 1.0
        assert params.atr_value == 2.5
        assert params.sl_distance == 2.0
        assert params.tp_distance == 5.0
        assert params.risk_reward_ratio == 2.5
        assert params.spread_enabled is True
        assert params.spread_points == 0.5
        assert params.comment == "Test trade"

    def test_validation_entry_price_positive(self):
        """Test that entry price must be positive."""
        with pytest.raises(ValueError, match="Entry price must be positive"):
            TradeParameters(
                entry_price_mid=100.0,
                entry_price_executed=0.0,  # Invalid
                stop_loss_raw=99.0,
                stop_loss_trigger=98.5,
                take_profit=105.0
            )

    def test_validation_position_size_positive(self):
        """Test that position size must be positive."""
        with pytest.raises(ValueError, match="Position size must be positive"):
            TradeParameters(
                entry_price_mid=100.0,
                entry_price_executed=100.0,
                stop_loss_raw=99.0,
                stop_loss_trigger=98.5,
                take_profit=105.0,
                position_size=0.0
            )

    def test_to_dict(self):
        """Test serialization to dict."""
        params = TradeParameters(
            entry_price_mid=100.0,
            entry_price_executed=100.5,
            stop_loss_raw=99.0,
            stop_loss_trigger=98.5,
            take_profit=105.0,
            position_size=1.5,
            sl_distance=2.0,
            tp_distance=5.0,
            risk_reward_ratio=2.5,
            spread_enabled=True,
            spread_points=0.5,
            comment="Test"
        )

        d = params.to_dict()

        assert d['entry_price_mid'] == 100.0
        assert d['executed_entry'] == 100.5
        assert d['raw_sl'] == 99.0
        assert d['trigger_sl'] == 98.5
        assert d['tp'] == 105.0
        assert d['position_size'] == 1.5
        assert d['sl_distance'] == 2.0
        assert d['tp_distance'] == 5.0
        assert d['risk_reward_ratio'] == 2.5
        assert d['spread_enabled'] is True
        assert d['spread_points'] == 0.5
        assert d['comment'] == "Test"


class TestTradeEntry:
    """Tests for TradeEntry contract."""

    @pytest.fixture
    def sample_timestamp(self):
        """Sample timestamp for testing."""
        return pd.Timestamp("2025-01-01 10:30:00")

    @pytest.fixture
    def sample_params(self):
        """Sample trade parameters."""
        return TradeParameters(
            entry_price_mid=100.0,
            entry_price_executed=100.5,
            stop_loss_raw=99.0,
            stop_loss_trigger=98.5,
            take_profit=105.0,
            sl_distance=1.5,
            tp_distance=4.5,
            risk_reward_ratio=3.0,
            spread_enabled=True,
            spread_points=0.5,
            comment="Test"
        )

    def test_valid_entry(self, sample_timestamp):
        """Test creating valid trade entry."""
        entry = TradeEntry(
            entry_id="E123",
            trade_manager_id=42,
            position_id=42,
            signal_id=123,
            entry_time=sample_timestamp,
            direction=TradeDirection.LONG,
            entry_price=100.5,
            stop_loss=98.5,
            take_profit=105.0,
            position_size=1.5,
            sl_distance=2.0,
            tp_distance=4.5,
            risk_reward_ratio=2.25,
            spread_enabled=True,
            spread_points=0.5,
            comment="Test entry"
        )

        assert entry.entry_id == "E123"
        assert entry.trade_manager_id == 42
        assert entry.position_id == 42
        assert entry.signal_id == 123
        assert entry.entry_time == sample_timestamp
        assert entry.direction == TradeDirection.LONG
        assert entry.entry_price == 100.5
        assert entry.stop_loss == 98.5
        assert entry.take_profit == 105.0
        assert entry.position_size == 1.5
        assert entry.sl_distance == 2.0
        assert entry.tp_distance == 4.5
        assert entry.risk_reward_ratio == 2.25
        assert entry.spread_enabled is True
        assert entry.spread_points == 0.5
        assert entry.comment == "Test entry"

    def test_validation_entry_price_positive(self, sample_timestamp):
        """Test that entry price must be positive."""
        with pytest.raises(ValueError, match="Entry price must be positive"):
            TradeEntry(
                entry_id="E1",
                entry_time=sample_timestamp,
                direction=TradeDirection.LONG,
                entry_price=0.0,
                stop_loss=98.0,
                take_profit=105.0,
                position_size=1.0
            )

    def test_validation_position_size_positive(self, sample_timestamp):
        """Test that position size must be positive."""
        with pytest.raises(ValueError, match="Position size must be positive"):
            TradeEntry(
                entry_id="E1",
                entry_time=sample_timestamp,
                direction=TradeDirection.LONG,
                entry_price=100.0,
                stop_loss=98.0,
                take_profit=105.0,
                position_size=0.0
            )

    def test_is_long_short_properties(self, sample_timestamp):
        """Test is_long and is_short properties."""
        long_entry = TradeEntry(
            entry_id="E1",
            entry_time=sample_timestamp,
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=98.0,
            take_profit=105.0,
            position_size=1.0
        )
        assert long_entry.is_long is True
        assert long_entry.is_short is False

        short_entry = TradeEntry(
            entry_id="E2",
            entry_time=sample_timestamp,
            direction=TradeDirection.SHORT,
            entry_price=100.0,
            stop_loss=102.0,
            take_profit=95.0,
            position_size=1.0
        )
        assert short_entry.is_long is False
        assert short_entry.is_short is True

    def test_from_trade_parameters(self, sample_timestamp, sample_params):
        """Test creating entry from trade parameters."""
        entry = TradeEntry.from_trade_parameters(
            entry_id="E123",
            timestamp=sample_timestamp,
            direction=TradeDirection.LONG,
            params=sample_params,
            trade_manager_id=42,
            signal_id=123
        )

        assert entry.entry_id == "E123"
        assert entry.trade_manager_id == 42
        assert entry.signal_id == 123
        assert entry.entry_time == sample_timestamp
        assert entry.direction == TradeDirection.LONG
        assert entry.entry_price == sample_params.entry_price_executed
        assert entry.stop_loss == sample_params.stop_loss_trigger
        assert entry.take_profit == sample_params.take_profit
        assert entry.sl_distance == sample_params.sl_distance
        assert entry.tp_distance == sample_params.tp_distance
        assert entry.risk_reward_ratio == sample_params.risk_reward_ratio
        assert entry.spread_enabled == sample_params.spread_enabled
        assert entry.spread_points == sample_params.spread_points
        assert entry.comment == sample_params.comment

    def test_to_dict(self, sample_timestamp):
        """Test serialization to dict."""
        entry = TradeEntry(
            entry_id="E123",
            trade_manager_id=42,
            position_id=42,
            signal_id=123,
            entry_time=sample_timestamp,
            direction=TradeDirection.LONG,
            entry_price=100.5,
            stop_loss=98.5,
            take_profit=105.0,
            position_size=1.5,
            sl_distance=2.0,
            tp_distance=4.5,
            risk_reward_ratio=2.25,
            comment="Test"
        )

        d = entry.to_dict()

        assert d['trade_id'] == "E123"
        assert d['trade_manager_trade_id'] == 42
        assert d['position_id'] == 42
        assert d['signal_id'] == 123
        assert d['entry_time'] == sample_timestamp
        assert d['direction'] == "BUY"
        assert d['entry_price'] == 100.5
        assert d['sl_price'] == 98.5
        assert d['tp_price'] == 105.0
        assert d['sl_distance'] == 2.0
        assert d['tp_distance'] == 4.5
        assert d['risk_reward_ratio'] == 2.25
        assert d['comment'] == "Test"
        assert d['status'] == "OPEN"


class TestTradeExit:
    """Tests for TradeExit contract."""

    @pytest.fixture
    def sample_entry(self):
        """Sample trade entry for testing."""
        return TradeEntry(
            entry_id="E123",
            entry_time=pd.Timestamp("2025-01-01 10:30:00"),
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=98.0,
            take_profit=105.0,
            position_size=1.0
        )

    def test_create_exit_long(self, sample_entry):
        """Test creating exit for LONG position."""
        exit_time = pd.Timestamp("2025-01-01 11:30:00")
        
        exit_trade = TradeExit.create(
            entry=sample_entry,
            exit_time=exit_time,
            exit_price=105.0,
            exit_reason=ExitReason.TAKE_PROFIT,
            ltf_execution=True,
            ltf_execution_mode="LTF_OHLC_V5"
        )

        assert exit_trade.exit_id == "E123_EXIT"
        assert exit_trade.entry_id == "E123"
        assert exit_trade.exit_time == exit_time
        assert exit_trade.exit_price == 105.0
        assert exit_trade.exit_reason == ExitReason.TAKE_PROFIT
        assert exit_trade.pnl_points == 5.0
        assert exit_trade.pnl_percent == 5.0
        assert exit_trade.is_win is True
        assert exit_trade.is_loss is False
        assert exit_trade.duration_minutes == 60.0
        assert exit_trade.ltf_execution is True
        assert exit_trade.ltf_execution_mode == "LTF_OHLC_V5"

    def test_create_exit_short(self, sample_entry):
        """Test creating exit for SHORT position."""
        short_entry = TradeEntry(
            entry_id="E124",
            entry_time=pd.Timestamp("2025-01-01 10:30:00"),
            direction=TradeDirection.SHORT,
            entry_price=100.0,
            stop_loss=102.0,
            take_profit=95.0,
            position_size=1.0
        )
        
        exit_time = pd.Timestamp("2025-01-01 11:30:00")
        
        exit_trade = TradeExit.create(
            entry=short_entry,
            exit_time=exit_time,
            exit_price=95.0,
            exit_reason=ExitReason.TAKE_PROFIT
        )

        assert exit_trade.pnl_points == 5.0  # 100 - 95
        assert exit_trade.is_win is True

    def test_create_exit_loss(self, sample_entry):
        """Test creating losing exit."""
        exit_time = pd.Timestamp("2025-01-01 10:45:00")
        
        exit_trade = TradeExit.create(
            entry=sample_entry,
            exit_time=exit_time,
            exit_price=98.0,
            exit_reason=ExitReason.STOP_LOSS
        )

        assert exit_trade.pnl_points == -2.0
        assert exit_trade.is_win is False
        assert exit_trade.is_loss is True
        assert exit_trade.duration_minutes == 15.0

    def test_to_dict(self, sample_entry):
        """Test serialization to dict."""
        exit_time = pd.Timestamp("2025-01-01 11:30:00")
        
        exit_trade = TradeExit.create(
            entry=sample_entry,
            exit_time=exit_time,
            exit_price=105.0,
            exit_reason=ExitReason.TAKE_PROFIT,
            exit_bar_high=105.5,
            exit_bar_low=104.5
        )

        d = exit_trade.to_dict()

        assert d['exit_time'] == exit_time
        assert d['exit_price'] == 105.0
        assert d['exit_reason'] == "TAKE_PROFIT"
        assert d['pnl_points'] == 5.0
        assert d['pnl_percent'] == 5.0
        assert d['duration_minutes'] == 60.0
        assert d['is_win'] is True
        assert d['is_loss'] is False
        assert d['exit_bar_high'] == 105.5
        assert d['exit_bar_low'] == 104.5


class TestTrade:
    """Tests for Trade contract."""

    @pytest.fixture
    def sample_entry(self):
        """Sample trade entry."""
        return TradeEntry(
            entry_id="E123",
            entry_time=pd.Timestamp("2025-01-01 10:30:00"),
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=98.0,
            take_profit=105.0,
            position_size=1.0
        )

    @pytest.fixture
    def sample_exit(self, sample_entry):
        """Sample trade exit."""
        return TradeExit.create(
            entry=sample_entry,
            exit_time=pd.Timestamp("2025-01-01 11:30:00"),
            exit_price=105.0,
            exit_reason=ExitReason.TAKE_PROFIT
        )

    def test_open_trade(self, sample_entry):
        """Test open trade (no exit)."""
        trade = Trade(entry=sample_entry, exit=None)

        assert trade.is_open is True
        assert trade.is_closed is False
        assert trade.trade_id == "E123"
        assert trade.status == "OPEN"
        assert trade.direction == TradeDirection.LONG
        assert trade.entry_time == sample_entry.entry_time
        assert trade.exit_time is None
        assert trade.pnl_points is None
        assert trade.is_win is False
        assert trade.is_loss is False

    def test_closed_trade(self, sample_entry, sample_exit):
        """Test closed trade."""
        trade = Trade(entry=sample_entry, exit=sample_exit)

        assert trade.is_open is False
        assert trade.is_closed is True
        assert trade.status == "CLOSED"
        assert trade.exit_time == sample_exit.exit_time
        assert trade.duration_minutes == 60.0
        assert trade.pnl_points == 5.0
        assert trade.is_win is True
        assert trade.is_loss is False
        assert trade.exit_reason == ExitReason.TAKE_PROFIT

    def test_to_dict_open(self, sample_entry):
        """Test to_dict for open trade."""
        trade = Trade(entry=sample_entry, exit=None)
        d = trade.to_dict()

        assert d['trade_id'] == "E123"
        assert d['status'] == "OPEN"
        assert d['entry_time'] == sample_entry.entry_time
        assert d['exit_time'] is None
        assert d['pnl_points'] == 0

    def test_to_dict_closed(self, sample_entry, sample_exit):
        """Test to_dict for closed trade."""
        trade = Trade(entry=sample_entry, exit=sample_exit)
        d = trade.to_dict()

        assert d['status'] == "CLOSED"
        assert d['exit_time'] == sample_exit.exit_time
        assert d['exit_reason'] == "TAKE_PROFIT"
        assert d['pnl_points'] == 5.0
        assert d['is_win'] is True

    def test_str_representation_open(self, sample_entry):
        """Test string representation for open trade."""
        trade = Trade(entry=sample_entry)
        s = str(trade)

        assert "Trade" in s
        assert "E123" in s
        assert "BUY" in s
        assert "OPEN" in s
        assert "P&L: N/A" in s

    def test_str_representation_closed(self, sample_entry, sample_exit):
        """Test string representation for closed trade."""
        trade = Trade(entry=sample_entry, exit=sample_exit)
        s = str(trade)

        assert "CLOSED" in s
        assert "TAKE_PROFIT" in s
        assert "+5.00 pts" in s


class TestRejectedSignal:
    """Tests for RejectedSignal contract."""

    @pytest.fixture
    def sample_timestamp(self):
        """Sample timestamp for testing."""
        return pd.Timestamp("2025-01-01 10:30:00")

    def test_valid_rejected_signal(self, sample_timestamp):
        """Test creating valid rejected signal."""
        rejected = RejectedSignal(
            rejection_id="R123",
            signal_id=456,
            rejection_time=sample_timestamp,
            direction="BUY",
            rejection_stage="RISK",
            rejection_reason="Risk percentile exceeded",
            current_price=100.5,
            meta={"atr_value": 2.5}
        )

        assert rejected.rejection_id == "R123"
        assert rejected.signal_id == 456
        assert rejected.rejection_time == sample_timestamp
        assert rejected.direction == "BUY"
        assert rejected.rejection_stage == "RISK"
        assert rejected.rejection_reason == "Risk percentile exceeded"
        assert rejected.current_price == 100.5
        assert rejected.meta == {"atr_value": 2.5}

    def test_minimal_rejected_signal(self):
        """Test rejected signal with minimal fields."""
        rejected = RejectedSignal(
            rejection_id="R123",
            rejection_stage="POSITION",
            rejection_reason="Max positions reached"
        )

        assert rejected.rejection_id == "R123"
        assert rejected.signal_id is None
        assert rejected.direction == "BUY"
        assert rejected.rejection_stage == "POSITION"
        assert rejected.rejection_reason == "Max positions reached"
        assert rejected.current_price is None

    def test_to_dict(self, sample_timestamp):
        """Test serialization to dict."""
        rejected = RejectedSignal(
            rejection_id="R123",
            signal_id=456,
            rejection_time=sample_timestamp,
            direction="SELL",
            rejection_stage="FILTER",
            rejection_reason="ADX too low",
            current_price=100.5
        )

        d = rejected.to_dict()

        assert d["rejection_id"] == "R123"
        assert d["signal_id"] == 456
        assert d["rejection_time"] == sample_timestamp
        assert d["direction"] == "SELL"
        assert d["rejection_stage"] == "FILTER"
        assert d["rejection_reason"] == "ADX too low"
        assert d["current_price"] == 100.5
        assert d["status"] == "REJECTED"

    def test_str_representation(self):
        """Test string representation."""
        rejected = RejectedSignal(
            rejection_id="R123",
            direction="BUY",
            rejection_stage="RISK",
            rejection_reason="Risk too high"
        )

        s = str(rejected)
        assert "RejectedSignal" in s
        assert "R123" in s
        assert "BUY" in s
        assert "Risk too high" in s


class TestTradeResult:
    """Tests for TradeResult contract."""

    @pytest.fixture
    def sample_trades(self):
        """Sample trades for testing."""
        entry1 = TradeEntry(
            entry_id="E1",
            entry_time=pd.Timestamp("2025-01-01 10:30:00"),
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=98.0,
            take_profit=105.0,
            position_size=1.0
        )
        exit1 = TradeExit.create(
            entry=entry1,
            exit_time=pd.Timestamp("2025-01-01 11:30:00"),
            exit_price=105.0,
            exit_reason=ExitReason.TAKE_PROFIT
        )
        trade1 = Trade(entry=entry1, exit=exit1)

        entry2 = TradeEntry(
            entry_id="E2",
            entry_time=pd.Timestamp("2025-01-01 12:00:00"),
            direction=TradeDirection.SHORT,
            entry_price=105.0,
            stop_loss=107.0,
            take_profit=100.0,
            position_size=1.0
        )
        exit2 = TradeExit.create(
            entry=entry2,
            exit_time=pd.Timestamp("2025-01-01 13:00:00"),
            exit_price=107.0,
            exit_reason=ExitReason.STOP_LOSS
        )
        trade2 = Trade(entry=entry2, exit=exit2)

        entry3 = TradeEntry(
            entry_id="E3",
            entry_time=pd.Timestamp("2025-01-01 14:00:00"),
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=98.0,
            take_profit=105.0,
            position_size=1.0
        )
        trade3 = Trade(entry=entry3, exit=None)  # Open trade

        return [trade1, trade2, trade3]

    @pytest.fixture
    def sample_rejected(self):
        """Sample rejected signals."""
        return [
            RejectedSignal(
                rejection_id="R1",
                signal_id=101,
                direction="BUY",
                rejection_stage="RISK",
                rejection_reason="Risk too high"
            ),
            RejectedSignal(
                rejection_id="R2",
                signal_id=102,
                direction="SELL",
                rejection_stage="FILTER",
                rejection_reason="ADX too low"
            )
        ]

    def test_from_trades(self, sample_trades, sample_rejected):
        """Test creating trade result from components."""
        exit_stats = {
            "TAKE_PROFIT": 1,
            "STOP_LOSS": 1
        }
        risk_stats = {
            "total_approved": 5,
            "total_rejected": 2,
            "total_adjusted": 1
        }
        position_rejected = {"buy": 1, "sell": 1}
        trade_manager_metrics = {"signals_accepted": 3}
        
        result = TradeResult.from_trades(
            trades=sample_trades,
            rejected_signals=sample_rejected,
            exit_stats=exit_stats,
            risk_stats=risk_stats,
            position_rejected=position_rejected,
            trade_manager_metrics=trade_manager_metrics,
            execution_mode="LTF_OHLC_V5",
            execution_time_ms=125.5
        )
        
        assert len(result.trades) == 3
        assert len(result.rejected_signals) == 2
        assert result.total_entries == 5  # 3 trades + 2 rejected
        assert result.total_opened == 3
        assert result.total_closed == 2
        assert result.total_rejected == 2
        assert result.currently_open == 1
        assert result.exits_by_reason == exit_stats
        assert result.risk_approved == 5
        assert result.risk_rejected == 2
        assert result.risk_adjusted == 1
        assert result.position_rejected == position_rejected
        assert result.win_count == 1
        assert result.loss_count == 1
        assert result.win_rate == 50.0
        # Trade1: +5.0, Trade2: -2.0, Trade3: open (no P&L) = total +3.0
        assert result.total_pnl_points == 3.0
        assert result.average_pnl_points == 1.5  # 3.0 / 2 closed trades

    def test_open_trades_property(self, sample_trades, sample_rejected):
        """Test open_trades property."""
        result = TradeResult.from_trades(
            trades=sample_trades,
            rejected_signals=sample_rejected,
            exit_stats={},
            risk_stats={},
            position_rejected={},
            trade_manager_metrics={},
            execution_mode="test"
        )

        open_trades = result.open_trades
        assert len(open_trades) == 1
        assert open_trades[0].entry.entry_id == "E3"

    def test_closed_trades_property(self, sample_trades, sample_rejected):
        """Test closed_trades property."""
        result = TradeResult.from_trades(
            trades=sample_trades,
            rejected_signals=sample_rejected,
            exit_stats={},
            risk_stats={},
            position_rejected={},
            trade_manager_metrics={},
            execution_mode="test"
        )

        closed_trades = result.closed_trades
        assert len(closed_trades) == 2
        assert {t.entry.entry_id for t in closed_trades} == {"E1", "E2"}

    def test_to_dataframe(self, sample_trades, sample_rejected):
        """Test conversion to DataFrame."""
        result = TradeResult.from_trades(
            trades=sample_trades,
            rejected_signals=sample_rejected,
            exit_stats={},
            risk_stats={},
            position_rejected={},
            trade_manager_metrics={},
            execution_mode="test"
        )

        df = result.to_dataframe()
        assert len(df) == 3
        assert "trade_id" in df.columns
        assert "pnl_points" in df.columns

        # Empty trades case
        empty_result = TradeResult.from_trades(
            trades=[],
            rejected_signals=[],
            exit_stats={},
            risk_stats={},
            position_rejected={},
            trade_manager_metrics={},
            execution_mode="test"
        )
        assert empty_result.to_dataframe().empty

    def test_to_dict(self, sample_trades, sample_rejected):
        """Test serialization to dict."""
        result = TradeResult.from_trades(
            trades=sample_trades,
            rejected_signals=sample_rejected,
            exit_stats={"TAKE_PROFIT": 1},
            risk_stats={"total_approved": 5},
            position_rejected={"buy": 1},
            trade_manager_metrics={"accepted": 3},
            execution_mode="test"
        )

        d = result.to_dict()

        assert len(d['all_trades']) == 3
        assert len(d['closed_trades']) == 2
        assert len(d['open_trades']) == 1
        assert len(d['rejected_signals']) == 2
        assert d['exit_stats'] == {"TAKE_PROFIT": 1}
        assert d['risk_stats']['total_approved'] == 5
        assert d['position_rejected_count'] == {"buy": 1}
        assert d['execution_mode'] == "test"

    def test_to_json(self, sample_trades, sample_rejected):
        """Test JSON serialization."""
        result = TradeResult.from_trades(
            trades=sample_trades,
            rejected_signals=sample_rejected,
            exit_stats={},
            risk_stats={},
            position_rejected={},
            trade_manager_metrics={},
            execution_mode="test"
        )

        json_str = result.to_json()
        assert isinstance(json_str, str)
        assert '"all_trades"' in json_str

        json_str_pretty = result.to_json(indent=2)
        assert "\n" in json_str_pretty

    def test_get_summary(self, sample_trades, sample_rejected):
        """Test summary string."""
        result = TradeResult.from_trades(
            trades=sample_trades,
            rejected_signals=sample_rejected,
            exit_stats={"TAKE_PROFIT": 1, "STOP_LOSS": 1},
            risk_stats={"total_approved": 5, "total_rejected": 2},
            position_rejected={"buy": 1, "sell": 1},
            trade_manager_metrics={},
            execution_mode="LTF_OHLC_V5",
            execution_time_ms=125.5
        )
        
        summary = result.get_summary()
        
        assert "TradeResult Summary" in summary
        assert "Total Entries: 5" in summary
        assert "Opened: 3" in summary
        assert "Closed: 2" in summary
        assert "Rejected: 2" in summary
        assert "Win Rate: 50.0%" in summary
        assert "Total P&L: +3.00 points" in summary
        assert "Avg P&L: +1.50 points/trade" in summary

    def test_str_representation(self, sample_trades, sample_rejected):
        """Test string representation."""
        result = TradeResult.from_trades(
            trades=sample_trades,
            rejected_signals=sample_rejected,
            exit_stats={},
            risk_stats={},
            position_rejected={},
            trade_manager_metrics={},
            execution_mode="test"
        )

        s = str(result)
        assert "TradeResult Summary" in s


class TestTradeDecision:
    """Tests for TradeDecision contract."""

    def test_open_decision(self):
        """Test OPEN decision."""
        decision = TradeDecision(
            decision_type=DecisionType.OPEN,
            reason="Opening LONG position",
            new_trade_id=1
        )

        assert decision.decision_type == DecisionType.OPEN
        assert decision.reason == "Opening LONG position"
        assert decision.close_trade_ids is None
        assert decision.new_trade_id == 1
        assert decision.is_open is True
        assert decision.is_close is False
        assert decision.is_reject is False

    def test_close_decision(self):
        """Test CLOSE decision."""
        decision = TradeDecision(
            decision_type=DecisionType.CLOSE,
            reason="Closing positions",
            close_trade_ids=[1, 2, 3]
        )

        assert decision.decision_type == DecisionType.CLOSE
        assert decision.close_trade_ids == [1, 2, 3]
        assert decision.new_trade_id is None
        assert decision.is_open is False
        assert decision.is_close is True
        assert decision.is_reject is False

    def test_reject_decision(self):
        """Test REJECT decision."""
        decision = TradeDecision(
            decision_type=DecisionType.REJECT,
            reason="Max positions reached"
        )

        assert decision.decision_type == DecisionType.REJECT
        assert decision.reason == "Max positions reached"
        assert decision.is_open is False
        assert decision.is_close is False
        assert decision.is_reject is True

    def test_close_and_reverse_decision(self):
        """Test CLOSE_AND_REVERSE decision."""
        decision = TradeDecision(
            decision_type=DecisionType.CLOSE_AND_REVERSE,
            reason="Reversing position",
            close_trade_ids=[1],
            new_trade_id=2
        )

        assert decision.decision_type == DecisionType.CLOSE_AND_REVERSE
        assert decision.close_trade_ids == [1]
        assert decision.new_trade_id == 2
        assert decision.is_open is True
        assert decision.is_close is True
        assert decision.is_reject is False

    def test_to_dict(self):
        """Test serialization to dict."""
        decision = TradeDecision(
            decision_type=DecisionType.OPEN,
            reason="Test reason",
            new_trade_id=42
        )

        d = decision.to_dict()

        assert d['action'] == "OPEN"
        assert d['reason'] == "Test reason"
        assert d['close_trade_ids'] is None
        assert d['new_trade_id'] == 42