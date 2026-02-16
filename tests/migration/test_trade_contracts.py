"""
Unit tests for Trade Contracts Validation
Session 11 - Quick Win Implementation

Tests validation logic in trade_contracts.py dataclasses.
Ensures contracts catch invalid inputs at creation time.
"""

# Add project root to path for proper module resolution
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parents[2]  # Go up from tests/migration/ to project root
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import pytest
import pandas as pd
from src.strategies.contracts.trade_contracts import (
    TradeDirection,
    ExitReason,
    TradeParameters,
    TradeEntry,
    TradeExit,
    Trade,
    RejectedSignal,
    TradeResult,
)


class TestTradeParametersValidation:
    """Test TradeParameters validation"""
    
    def test_valid_parameters(self):
        """Valid parameters should create successfully"""
        params = TradeParameters(
            entry_price_mid=100.0,
            entry_price_executed=100.05,
            stop_loss_raw=98.0,
            stop_loss_trigger=98.0,
            take_profit=102.0,
            position_size=1.0,
        )
        assert params.entry_price_executed == 100.05
    
    def test_negative_entry_price_raises(self):
        """Negative entry price should raise ValueError"""
        with pytest.raises(ValueError, match="Entry price must be positive"):
            TradeParameters(
                entry_price_mid=100.0,
                entry_price_executed=-100.0,  # Invalid
                stop_loss_raw=98.0,
                stop_loss_trigger=98.0,
                take_profit=102.0,
            )
    
    def test_zero_entry_price_raises(self):
        """Zero entry price should raise ValueError"""
        with pytest.raises(ValueError, match="Entry price must be positive"):
            TradeParameters(
                entry_price_mid=100.0,
                entry_price_executed=0.0,  # Invalid
                stop_loss_raw=98.0,
                stop_loss_trigger=98.0,
                take_profit=102.0,
            )
    
    def test_negative_position_size_raises(self):
        """Negative position size should raise ValueError"""
        with pytest.raises(ValueError, match="Position size must be positive"):
            TradeParameters(
                entry_price_mid=100.0,
                entry_price_executed=100.0,
                stop_loss_raw=98.0,
                stop_loss_trigger=98.0,
                take_profit=102.0,
                position_size=-1.0,  # Invalid
            )


class TestTradeEntryValidation:
    """Test TradeEntry validation"""
    
    def test_valid_entry(self):
        """Valid entry should create successfully"""
        entry = TradeEntry(
            entry_id="E1",
            entry_time=pd.Timestamp("2025-01-01"),
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=98.0,
            take_profit=102.0,
        )
        assert entry.entry_id == "E1"
        assert entry.is_long
    
    def test_negative_entry_price_raises(self):
        """Negative entry price should raise ValueError"""
        with pytest.raises(ValueError, match="Entry price must be positive"):
            TradeEntry(
                entry_id="E1",
                entry_time=pd.Timestamp("2025-01-01"),
                direction=TradeDirection.LONG,
                entry_price=-100.0,  # Invalid
                stop_loss=98.0,
                take_profit=102.0,
            )
    
    def test_zero_entry_price_raises(self):
        """Zero entry price should raise ValueError"""
        with pytest.raises(ValueError, match="Entry price must be positive"):
            TradeEntry(
                entry_id="E1",
                entry_time=pd.Timestamp("2025-01-01"),
                direction=TradeDirection.LONG,
                entry_price=0.0,  # Invalid
                stop_loss=98.0,
                take_profit=102.0,
            )
    
    def test_negative_position_size_raises(self):
        """Negative position size should raise ValueError"""
        with pytest.raises(ValueError, match="Position size must be positive"):
            TradeEntry(
                entry_id="E1",
                entry_time=pd.Timestamp("2025-01-01"),
                direction=TradeDirection.LONG,
                entry_price=100.0,
                stop_loss=98.0,
                take_profit=102.0,
                position_size=-1.0,  # Invalid
            )


class TestTradeDirectionEnum:
    """Test TradeDirection enum conversions"""
    
    def test_from_string_buy(self):
        """'BUY' should convert to LONG"""
        assert TradeDirection.from_string("BUY") == TradeDirection.LONG
        assert TradeDirection.from_string("buy") == TradeDirection.LONG
        assert TradeDirection.from_string("LONG") == TradeDirection.LONG
    
    def test_from_string_sell(self):
        """'SELL' should convert to SHORT"""
        assert TradeDirection.from_string("SELL") == TradeDirection.SHORT
        assert TradeDirection.from_string("sell") == TradeDirection.SHORT
        assert TradeDirection.from_string("SHORT") == TradeDirection.SHORT
    
    def test_from_string_invalid_raises(self):
        """Invalid direction should raise ValueError"""
        with pytest.raises(ValueError, match="Invalid direction"):
            TradeDirection.from_string("INVALID")
    
    def test_to_string(self):
        """Enum should convert back to string"""
        assert TradeDirection.LONG.to_string() == "BUY"
        assert TradeDirection.SHORT.to_string() == "SELL"
    
    def test_is_long_property(self):
        """is_long property should work correctly"""
        assert TradeDirection.LONG.is_long
        assert not TradeDirection.SHORT.is_long
    
    def test_is_short_property(self):
        """is_short property should work correctly"""
        assert TradeDirection.SHORT.is_short
        assert not TradeDirection.LONG.is_short


class TestExitReasonEnum:
    """Test ExitReason enum conversions"""
    
    def test_from_string_valid(self):
        """Valid exit reasons should convert"""
        assert ExitReason.from_string("STOP_LOSS") == ExitReason.STOP_LOSS
        assert ExitReason.from_string("TAKE_PROFIT") == ExitReason.TAKE_PROFIT
        assert ExitReason.from_string("END_OF_DATA") == ExitReason.END_OF_DATA
    
    def test_from_string_invalid_raises(self):
        """Invalid exit reason should raise ValueError"""
        with pytest.raises(ValueError, match="Invalid exit reason"):
            ExitReason.from_string("INVALID_REASON")
    
    def test_to_string(self):
        """Enum should convert back to string"""
        assert ExitReason.STOP_LOSS.to_string() == "STOP_LOSS"
        assert ExitReason.TAKE_PROFIT.to_string() == "TAKE_PROFIT"


class TestTradeExitCreation:
    """Test TradeExit.create() factory method"""
    
    def test_long_win_calculation(self):
        """Long winning trade should calculate P&L correctly"""
        entry = TradeEntry(
            entry_id="E1",
            entry_time=pd.Timestamp("2025-01-01 10:00"),
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=98.0,
            take_profit=102.0,
        )
        
        exit = TradeExit.create(
            entry=entry,
            exit_time=pd.Timestamp("2025-01-01 10:30"),
            exit_price=102.0,
            exit_reason=ExitReason.TAKE_PROFIT,
        )
        
        assert exit.pnl_points == 2.0
        assert exit.pnl_percent == 2.0
        assert exit.is_win
        assert not exit.is_loss
        assert exit.duration_minutes == 30.0
    
    def test_long_loss_calculation(self):
        """Long losing trade should calculate P&L correctly"""
        entry = TradeEntry(
            entry_id="E1",
            entry_time=pd.Timestamp("2025-01-01 10:00"),
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=98.0,
            take_profit=102.0,
        )
        
        exit = TradeExit.create(
            entry=entry,
            exit_time=pd.Timestamp("2025-01-01 10:15"),
            exit_price=98.0,
            exit_reason=ExitReason.STOP_LOSS,
        )
        
        assert exit.pnl_points == -2.0
        assert exit.pnl_percent == -2.0
        assert not exit.is_win
        assert exit.is_loss
        assert exit.duration_minutes == 15.0
    
    def test_short_win_calculation(self):
        """Short winning trade should calculate P&L correctly"""
        entry = TradeEntry(
            entry_id="E1",
            entry_time=pd.Timestamp("2025-01-01 10:00"),
            direction=TradeDirection.SHORT,
            entry_price=100.0,
            stop_loss=102.0,
            take_profit=98.0,
        )
        
        exit = TradeExit.create(
            entry=entry,
            exit_time=pd.Timestamp("2025-01-01 10:45"),
            exit_price=98.0,
            exit_reason=ExitReason.TAKE_PROFIT,
        )
        
        assert exit.pnl_points == 2.0
        assert exit.pnl_percent == 2.0
        assert exit.is_win
        assert not exit.is_loss
        assert exit.duration_minutes == 45.0


class TestTradeProperties:
    """Test Trade composite properties"""
    
    def test_open_trade_properties(self):
        """Open trade should have correct property values"""
        entry = TradeEntry(
            entry_id="E1",
            entry_time=pd.Timestamp("2025-01-01"),
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=98.0,
            take_profit=102.0,
        )
        
        trade = Trade(entry=entry, exit=None)
        
        assert trade.is_open
        assert not trade.is_closed
        assert trade.status == "OPEN"
        assert trade.pnl_points is None
        assert trade.pnl_percent is None
        assert trade.exit_reason is None
        assert not trade.is_win
        assert not trade.is_loss
    
    def test_closed_trade_properties(self):
        """Closed trade should have correct property values"""
        entry = TradeEntry(
            entry_id="E1",
            entry_time=pd.Timestamp("2025-01-01 10:00"),
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=98.0,
            take_profit=102.0,
        )
        
        exit = TradeExit.create(
            entry=entry,
            exit_time=pd.Timestamp("2025-01-01 10:30"),
            exit_price=102.0,
            exit_reason=ExitReason.TAKE_PROFIT,
        )
        
        trade = Trade(entry=entry, exit=exit)
        
        assert not trade.is_open
        assert trade.is_closed
        assert trade.status == "CLOSED"
        assert trade.pnl_points == 2.0
        assert trade.pnl_percent == 2.0
        assert trade.exit_reason == ExitReason.TAKE_PROFIT
        assert trade.is_win
        assert not trade.is_loss


class TestRejectedSignal:
    """Test RejectedSignal contract"""
    
    def test_rejected_signal_creation(self):
        """RejectedSignal should create with required fields"""
        rejected = RejectedSignal(
            rejection_id="R1",
            signal_id=123,
            rejection_time=pd.Timestamp("2025-01-01"),
            direction="BUY",
            rejection_stage="RISK",
            rejection_reason="Risk limit exceeded",
        )
        
        assert rejected.rejection_id == "R1"
        assert rejected.direction == "BUY"
        assert rejected.rejection_stage == "RISK"
    
    def test_rejected_signal_to_dict(self):
        """RejectedSignal.to_dict() should return clean format"""
        rejected = RejectedSignal(
            rejection_id="R1",
            rejection_time=pd.Timestamp("2025-01-01"),
            direction="SELL",
            rejection_reason="Position limit",
        )
        
        result = rejected.to_dict()
        
        assert result["rejection_id"] == "R1"
        assert result["direction"] == "SELL"
        assert result["status"] == "REJECTED"


class TestTradeResultSerialization:
    """Test TradeResult JSON serialization (Quick Win #1)"""
    
    def test_to_json_and_back(self):
        """TradeResult should serialize to JSON and deserialize correctly"""
        # Create simple trade
        entry = TradeEntry(
            entry_id="E1",
            entry_time=pd.Timestamp("2025-01-01 10:00"),
            direction=TradeDirection.LONG,
            entry_price=100.0,
            stop_loss=98.0,
            take_profit=102.0,
        )
        
        exit = TradeExit.create(
            entry=entry,
            exit_time=pd.Timestamp("2025-01-01 10:30"),
            exit_price=102.0,
            exit_reason=ExitReason.TAKE_PROFIT,
        )
        
        trade = Trade(entry=entry, exit=exit)
        
        # Create TradeResult
        result = TradeResult.from_trades(
            trades=[trade],
            rejected_signals=[],
            exit_stats={"TAKE_PROFIT": 1},
            risk_stats={"total_approved": 1, "total_rejected": 0, "total_adjusted": 0},
            position_rejected={},
            trade_manager_metrics={},
            execution_mode="TEST",
        )
        
        # Serialize to JSON
        json_str = result.to_json(indent=2)
        assert isinstance(json_str, str)
        assert "TAKE_PROFIT" in json_str
        
        # Deserialize back
        result_restored = TradeResult.from_json(json_str)
        
        # Verify
        assert len(result_restored.trades) == 1
        assert result_restored.win_count == 1
        assert result_restored.execution_mode == "TEST"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])