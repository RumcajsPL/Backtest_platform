"""
Unit Tests for TradeManager
============================
Tests position management, pyramiding, and reversal logic.
"""

import pytest
import pandas as pd

from src.strategies.specific.modules.trade_manager import TradeManager
from src.strategies.contracts.trade_contracts import DecisionType, TradeDirection
from src.strategies.contracts.position_contracts import Position


class TestTradeManager:
    """Tests for TradeManager class."""

    @pytest.fixture
    def sample_timestamp(self):
        """Sample timestamp for tests."""
        return pd.Timestamp("2025-01-01 10:00:00")

    def test_initialization_with_config(self, test_config):
        """Test initializing TradeManager with StrategyConfig."""
        manager = TradeManager(config=test_config)

        assert manager.close_on_opposite == test_config.trade_management.position_control.close_on_opposite
        assert manager.pyramiding_enabled == test_config.trade_management.position_control.pyramiding_enabled
        assert manager.max_positions == test_config.trade_management.position_control.max_positions
        assert len(manager.current_positions) == 0
        assert manager.trade_counter == 0

    def test_initialization_with_custom_config(self, base_config_dict):
        """Test initialization with custom position settings."""
        from src.config.config_schema import StrategyConfig
        
        # Customize position control
        base_config_dict["trade_management"]["position_control"] = {
            "pyramiding_enabled": True,
            "close_on_opposite": False,
            "max_positions": 3
        }
        
        config = StrategyConfig.from_dict(base_config_dict)
        manager = TradeManager(config=config)

        assert manager.pyramiding_enabled is True
        assert manager.close_on_opposite is False
        assert manager.max_positions == 3

    def test_current_direction_none(self, test_config):
        """Test current_direction when no positions."""
        manager = TradeManager(config=test_config)
        assert manager.current_direction is None

    def test_handle_signal_no_positions(self, test_config, sample_timestamp):
        """Test handling signal when no positions exist."""
        manager = TradeManager(config=test_config)

        decision = manager.handle_signal(
            timestamp=sample_timestamp,
            signal_type="BUY",
            entry_price=100.0,
            stop_loss=99.0,
            take_profit=105.0
        )

        assert decision.decision_type == DecisionType.OPEN
        assert "Opening" in decision.reason
        assert decision.close_trade_ids is None
        assert decision.new_trade_id == 1

        # Metrics should be updated
        metrics = manager.get_metrics()
        assert metrics["total_signals_received"] == 1
        assert metrics["signals_accepted"] == 1

    def test_open_position(self, test_config, sample_timestamp):
        """Test opening a position."""
        manager = TradeManager(config=test_config)

        manager.open_position(
            trade_id=1,
            timestamp=sample_timestamp,
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=99.0,
            take_profit=105.0,
            position_size=1.0,
            meta={"signal_id": 123}
        )

        assert len(manager.current_positions) == 1
        assert manager.trade_counter == 1
        assert manager.current_direction == TradeDirection.LONG

        position = manager.current_positions[0]
        assert position.position_id == 1
        assert position.direction == TradeDirection.LONG
        assert position.entry_price == 100.0
        assert position.stop_loss == 99.0
        assert position.take_profit == 105.0
        assert position.size == 1.0
        assert position.open_time == sample_timestamp
        assert position.meta == {"signal_id": 123}

    def test_same_direction_with_pyramiding(self, test_config, sample_timestamp):
        """Test same-direction signal with pyramiding enabled."""
        # Enable pyramiding
        test_config.trade_management.position_control.pyramiding_enabled = True
        test_config.trade_management.position_control.max_positions = 3
        
        manager = TradeManager(config=test_config)

        # Open first position
        manager.open_position(
            trade_id=1,
            timestamp=sample_timestamp,
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=99.0,
            take_profit=105.0
        )

        # Same direction signal
        decision = manager.handle_signal(
            timestamp=sample_timestamp + pd.Timedelta(minutes=5),
            signal_type="BUY",
            entry_price=101.0,
            stop_loss=100.0,
            take_profit=106.0
        )

        assert decision.decision_type == DecisionType.OPEN
        assert decision.new_trade_id == 2
        assert len(manager.current_positions) == 1  # Not opened yet
        assert manager.current_direction == TradeDirection.LONG

    def test_same_direction_without_pyramiding(self, test_config, sample_timestamp):
        """Test same-direction signal with pyramiding disabled."""
        # Disable pyramiding
        test_config.trade_management.position_control.pyramiding_enabled = False
        
        manager = TradeManager(config=test_config)

        # Open first position
        manager.open_position(
            trade_id=1,
            timestamp=sample_timestamp,
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=99.0,
            take_profit=105.0
        )

        # Same direction signal - should reject
        decision = manager.handle_signal(
            timestamp=sample_timestamp + pd.Timedelta(minutes=5),
            signal_type="BUY",
            entry_price=101.0,
            stop_loss=100.0,
            take_profit=106.0
        )

        assert decision.decision_type == DecisionType.REJECT
        assert "Pyramiding disabled" in decision.reason
        assert decision.new_trade_id is None

        # Check metrics
        metrics = manager.get_metrics()
        assert metrics["rejected_reasons"]["pyramiding_disabled"] == 1
        assert metrics["signals_rejected"] == 1

    def test_max_positions_reached(self, test_config, sample_timestamp):
        """Test rejection when max positions reached."""
        # Enable pyramiding with max=2
        test_config.trade_management.position_control.pyramiding_enabled = True
        test_config.trade_management.position_control.max_positions = 2
        
        manager = TradeManager(config=test_config)

        # Open two positions
        manager.open_position(trade_id=1, timestamp=sample_timestamp, direction=TradeDirection.LONG,
                              entry_price=100.0, stop_loss=99.0, take_profit=105.0)
        manager.open_position(trade_id=2, timestamp=sample_timestamp + pd.Timedelta(minutes=1),
                              direction=TradeDirection.LONG, entry_price=101.0,
                              stop_loss=100.0, take_profit=106.0)

        # Third signal - should reject
        decision = manager.handle_signal(
            timestamp=sample_timestamp + pd.Timedelta(minutes=5),
            signal_type="BUY",
            entry_price=102.0,
            stop_loss=101.0,
            take_profit=107.0
        )

        assert decision.decision_type == DecisionType.REJECT
        assert "Max positions" in decision.reason
        assert "2" in decision.reason

        # Check metrics
        metrics = manager.get_metrics()
        assert metrics["rejected_reasons"]["max_positions_reached"] == 1

    def test_opposite_signal_with_close_on_opposite(self, test_config, sample_timestamp):
        """Test opposite signal with close_on_opposite enabled."""
        # Enable close on opposite
        test_config.trade_management.position_control.close_on_opposite = True
        
        manager = TradeManager(config=test_config)

        # Open LONG position
        manager.open_position(
            trade_id=1,
            timestamp=sample_timestamp,
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=99.0,
            take_profit=105.0
        )

        # Opposite SELL signal
        decision = manager.handle_signal(
            timestamp=sample_timestamp + pd.Timedelta(minutes=5),
            signal_type="SELL",
            entry_price=102.0,
            stop_loss=103.0,
            take_profit=98.0
        )

        assert decision.decision_type == DecisionType.CLOSE_AND_REVERSE
        assert "Closing 1 LONG positions and reversing to SELL" in decision.reason
        assert decision.close_trade_ids == [1]
        assert decision.new_trade_id == 2

        # Check metrics
        metrics = manager.get_metrics()
        assert metrics["positions_closed_by_opposite"] == 1
        assert metrics["positions_reversed"] == 1

    def test_opposite_signal_without_close_on_opposite(self, test_config, sample_timestamp):
        """Test opposite signal with close_on_opposite disabled."""
        # Disable close on opposite
        test_config.trade_management.position_control.close_on_opposite = False
        
        manager = TradeManager(config=test_config)

        # Open LONG position
        manager.open_position(
            trade_id=1,
            timestamp=sample_timestamp,
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=99.0,
            take_profit=105.0
        )

        # Opposite SELL signal - should reject
        decision = manager.handle_signal(
            timestamp=sample_timestamp + pd.Timedelta(minutes=5),
            signal_type="SELL",
            entry_price=102.0,
            stop_loss=103.0,
            take_profit=98.0
        )

        assert decision.decision_type == DecisionType.REJECT
        assert "Opposite signal ignored" in decision.reason

        # Check metrics
        metrics = manager.get_metrics()
        assert metrics["rejected_reasons"]["opposite_ignored"] == 1

    def test_close_positions(self, test_config, sample_timestamp):
        """Test closing positions."""
        manager = TradeManager(config=test_config)

        # Open two positions
        manager.open_position(trade_id=1, timestamp=sample_timestamp, direction=TradeDirection.LONG,
                              entry_price=100.0, stop_loss=99.0, take_profit=105.0)
        manager.open_position(trade_id=2, timestamp=sample_timestamp + pd.Timedelta(minutes=1),
                              direction=TradeDirection.LONG, entry_price=101.0,
                              stop_loss=100.0, take_profit=106.0)

        assert len(manager.current_positions) == 2

        # Close first position
        manager.close_positions([1])
        
        assert len(manager.current_positions) == 1
        assert manager.current_positions[0].position_id == 2

        # Close remaining
        manager.close_positions([2])
        assert len(manager.current_positions) == 0
        assert manager.current_direction is None

    def test_has_open_position(self, test_config, sample_timestamp):
        """Test has_open_position method."""
        manager = TradeManager(config=test_config)

        assert manager.has_open_position() is False

        manager.open_position(trade_id=1, timestamp=sample_timestamp, direction=TradeDirection.LONG,
                              entry_price=100.0, stop_loss=99.0, take_profit=105.0)

        assert manager.has_open_position() is True

        manager.close_positions([1])
        assert manager.has_open_position() is False

    def test_get_current_positions(self, test_config, sample_timestamp):
        """Test getting copy of current positions."""
        manager = TradeManager(config=test_config)

        manager.open_position(trade_id=1, timestamp=sample_timestamp, direction=TradeDirection.LONG,
                              entry_price=100.0, stop_loss=99.0, take_profit=105.0)

        positions = manager.get_current_positions()
        assert len(positions) == 1
        assert positions[0].position_id == 1

        # Modifying returned list shouldn't affect internal state
        positions.clear()
        assert len(manager.current_positions) == 1

    def test_get_metrics_copy(self, test_config):
        """Test that get_metrics returns a copy."""
        manager = TradeManager(config=test_config)

        metrics1 = manager.get_metrics()
        metrics1["total_signals_received"] = 999

        metrics2 = manager.get_metrics()
        assert metrics2["total_signals_received"] == 0

    def test_reset(self, test_config, sample_timestamp):
        """Test resetting manager state."""
        manager = TradeManager(config=test_config)

        # Add some state
        manager.open_position(trade_id=1, timestamp=sample_timestamp, direction=TradeDirection.LONG,
                              entry_price=100.0, stop_loss=99.0, take_profit=105.0)
        manager.handle_signal(timestamp=sample_timestamp, signal_type="BUY",
                              entry_price=100.0, stop_loss=99.0, take_profit=105.0)

        assert len(manager.current_positions) == 1
        assert manager.trade_counter == 1
        assert manager.get_metrics()["total_signals_received"] == 1

        # Reset
        manager.reset()

        assert len(manager.current_positions) == 0
        assert manager.trade_counter == 0
        assert manager.get_metrics()["total_signals_received"] == 0

    def test_multiple_positions_different_directions_not_allowed(self, test_config, sample_timestamp):
        """Test that opposite directions aren't allowed simultaneously."""
        manager = TradeManager(config=test_config)

        # Open LONG
        manager.open_position(trade_id=1, timestamp=sample_timestamp, direction=TradeDirection.LONG,
                              entry_price=100.0, stop_loss=99.0, take_profit=105.0)

        # Try to open SHORT - should be rejected or reversed based on config
        # With default config (close_on_opposite=False), should reject
        decision = manager.handle_signal(
            timestamp=sample_timestamp + pd.Timedelta(minutes=5),
            signal_type="SELL",
            entry_price=102.0,
            stop_loss=103.0,
            take_profit=98.0
        )

        assert decision.decision_type == DecisionType.REJECT

        # LONG position still open
        assert len(manager.current_positions) == 1
        assert manager.current_direction == TradeDirection.LONG

    def test_pyramiding_with_max_limit(self, test_config, sample_timestamp):
        """Test pyramiding up to max positions."""
        test_config.trade_management.position_control.pyramiding_enabled = True
        test_config.trade_management.position_control.max_positions = 3
        
        manager = TradeManager(config=test_config)

        # Open positions up to limit
        for i in range(3):
            manager.open_position(
                trade_id=i+1,
                timestamp=sample_timestamp + pd.Timedelta(minutes=i),
                direction=TradeDirection.LONG,
                entry_price=100.0 + i,
                stop_loss=99.0 + i,
                take_profit=105.0 + i
            )

        assert len(manager.current_positions) == 3

        # Next signal should reject
        decision = manager.handle_signal(
            timestamp=sample_timestamp + pd.Timedelta(minutes=5),
            signal_type="BUY",
            entry_price=103.0,
            stop_loss=102.0,
            take_profit=108.0
        )

        assert decision.decision_type == DecisionType.REJECT
        assert "Max positions" in decision.reason

    def test_signal_with_metadata(self, test_config, sample_timestamp):
        """Test handling signal with metadata."""
        manager = TradeManager(config=test_config)

        decision = manager.handle_signal(
            timestamp=sample_timestamp,
            signal_type="BUY",
            entry_price=100.0,
            stop_loss=99.0,
            take_profit=105.0,
            meta={"custom": "value", "signal_id": 456}
        )

        assert decision.decision_type == DecisionType.OPEN

        # Open the position with metadata
        manager.open_position(
            trade_id=decision.new_trade_id,
            timestamp=sample_timestamp,
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=99.0,
            take_profit=105.0,
            meta={"custom": "value", "signal_id": 456}
        )

        position = manager.current_positions[0]
        assert position.meta["custom"] == "value"
        assert position.meta["signal_id"] == 456