"""Unit tests for TradeManager Position contract integration

Tests that TradeManager correctly creates and manages Position contracts
with proper validation, immutability, and helper methods.

Session 8 - Test Suite 2
"""
import pytest
import pandas as pd
from datetime import datetime

# Add project root to path for imports
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.strategies.specific.modules.trade_manager import TradeManager
from src.strategies.contracts.position_contracts import Position
from src.strategies.contracts.trade_contracts import TradeDirection


class TestPositionContractCreation:
    """Test Position contract creation and storage"""
    
    def test_position_contract_created(self):
        """Test Position contract is created correctly"""
        config = {'trade_management': {'position_control': {}}}
        tm = TradeManager(config)
        
        tm.open_position(
            trade_id=1,
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            direction=TradeDirection.LONG,
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0,
            position_size=1.5
        )
        
        positions = tm.get_current_positions()
        assert len(positions) == 1
        
        pos = positions[0]
        assert isinstance(pos, Position)
        assert pos.position_id == 1
        assert pos.direction == TradeDirection.LONG
        assert pos.entry_price == 19875.0
        assert pos.stop_loss == 19850.0
        assert pos.take_profit == 19950.0
        assert pos.size == 1.5
        assert pos.open_time == pd.Timestamp('2025-02-13 10:00:00')
    
    def test_position_with_metadata(self):
        """Test Position contract stores metadata"""
        config = {'trade_management': {'position_control': {}}}
        tm = TradeManager(config)
        
        meta = {
            'signal_id': 123,
            'strategy': 'RSI_CCI',
            'custom_field': 'test_value'
        }
        
        tm.open_position(
            trade_id=1,
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            direction=TradeDirection.LONG,
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0,
            meta=meta
        )
        
        pos = tm.get_current_positions()[0]
        assert pos.meta == meta
        assert pos.meta['signal_id'] == 123
        assert pos.meta['strategy'] == 'RSI_CCI'
    
    def test_position_default_metadata(self):
        """Test Position contract creates empty meta dict by default"""
        config = {'trade_management': {'position_control': {}}}
        tm = TradeManager(config)
        
        tm.open_position(
            trade_id=1,
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            direction=TradeDirection.LONG,
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        
        pos = tm.get_current_positions()[0]
        assert pos.meta == {}


class TestPositionImmutability:
    """Test Position contract immutability (frozen dataclass)"""
    
    def test_position_is_frozen(self):
        """Test Position contract is immutable"""
        config = {'trade_management': {'position_control': {}}}
        tm = TradeManager(config)
        
        tm.open_position(
            trade_id=1,
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            direction=TradeDirection.LONG,
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        
        pos = tm.get_current_positions()[0]
        
        # Should raise FrozenInstanceError when trying to modify
        with pytest.raises(Exception):
            pos.entry_price = 20000.0
        
        with pytest.raises(Exception):
            pos.stop_loss = 19800.0
        
        with pytest.raises(Exception):
            pos.direction = TradeDirection.SHORT


class TestPositionDirections:
    """Test Position contract with different trade directions"""
    
    def test_long_position(self):
        """Test LONG position creation"""
        config = {'trade_management': {'position_control': {}}}
        tm = TradeManager(config)
        
        tm.open_position(
            trade_id=1,
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            direction=TradeDirection.LONG,
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        
        pos = tm.get_current_positions()[0]
        assert pos.direction == TradeDirection.LONG
        assert pos.is_long is True
        assert pos.is_short is False
    
    def test_short_position(self):
        """Test SHORT position creation"""
        config = {'trade_management': {'position_control': {}}}
        tm = TradeManager(config)
        
        tm.open_position(
            trade_id=1,
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            direction=TradeDirection.SHORT,
            entry_price=19880.0,
            stop_loss=19905.0,
            take_profit=19805.0
        )
        
        pos = tm.get_current_positions()[0]
        assert pos.direction == TradeDirection.SHORT
        assert pos.is_long is False
        assert pos.is_short is True


class TestPositionCalculations:
    """Test Position contract calculation methods"""
    
    def test_sl_distance_long(self):
        """Test SL distance calculation for LONG position"""
        config = {'trade_management': {'position_control': {}}}
        tm = TradeManager(config)
        
        tm.open_position(
            trade_id=1,
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            direction=TradeDirection.LONG,
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        
        pos = tm.get_current_positions()[0]
        assert pos.sl_distance == 25.0  # 19875 - 19850
    
    def test_sl_distance_short(self):
        """Test SL distance calculation for SHORT position"""
        config = {'trade_management': {'position_control': {}}}
        tm = TradeManager(config)
        
        tm.open_position(
            trade_id=1,
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            direction=TradeDirection.SHORT,
            entry_price=19880.0,
            stop_loss=19905.0,
            take_profit=19805.0
        )
        
        pos = tm.get_current_positions()[0]
        assert pos.sl_distance == 25.0  # 19905 - 19880
    
    def test_tp_distance_long(self):
        """Test TP distance calculation for LONG position"""
        config = {'trade_management': {'position_control': {}}}
        tm = TradeManager(config)
        
        tm.open_position(
            trade_id=1,
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            direction=TradeDirection.LONG,
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        
        pos = tm.get_current_positions()[0]
        assert pos.tp_distance == 75.0  # 19950 - 19875
    
    def test_tp_distance_short(self):
        """Test TP distance calculation for SHORT position"""
        config = {'trade_management': {'position_control': {}}}
        tm = TradeManager(config)
        
        tm.open_position(
            trade_id=1,
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            direction=TradeDirection.SHORT,
            entry_price=19880.0,
            stop_loss=19905.0,
            take_profit=19805.0
        )
        
        pos = tm.get_current_positions()[0]
        assert pos.tp_distance == 75.0  # 19880 - 19805
    
    def test_risk_reward_ratio(self):
        """Test risk-reward ratio calculation"""
        config = {'trade_management': {'position_control': {}}}
        tm = TradeManager(config)
        
        # LONG: SL=25 points, TP=75 points → R:R = 3.0
        tm.open_position(
            trade_id=1,
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            direction=TradeDirection.LONG,
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        
        pos_long = tm.get_current_positions()[0]
        assert abs(pos_long.risk_reward_ratio - 3.0) < 0.01
        
        # SHORT: SL=25 points, TP=75 points → R:R = 3.0
        tm.open_position(
            trade_id=2,
            timestamp=pd.Timestamp('2025-02-13 11:00:00'),
            direction=TradeDirection.SHORT,
            entry_price=19880.0,
            stop_loss=19905.0,
            take_profit=19805.0
        )
        
        pos_short = tm.get_current_positions()[1]
        assert abs(pos_short.risk_reward_ratio - 3.0) < 0.01


class TestPositionStateManagement:
    """Test Position state management in TradeManager"""
    
    def test_multiple_positions(self):
        """Test managing multiple positions"""
        config = {
            'trade_management': {
                'position_control': {
                    'pyramiding_enabled': True
                }
            }
        }
        tm = TradeManager(config)
        
        # Open 3 positions
        for i in range(3):
            tm.open_position(
                trade_id=i+1,
                timestamp=pd.Timestamp(f'2025-02-13 {10+i}:00:00'),
                direction=TradeDirection.LONG,
                entry_price=19875.0 + i*25,
                stop_loss=19850.0 + i*25,
                take_profit=19950.0 + i*25
            )
        
        positions = tm.get_current_positions()
        assert len(positions) == 3
        
        # Verify all positions
        for i, pos in enumerate(positions):
            assert pos.position_id == i + 1
            assert pos.entry_price == 19875.0 + i*25
    
    def test_close_single_position(self):
        """Test closing single position from multiple"""
        config = {
            'trade_management': {
                'position_control': {
                    'pyramiding_enabled': True
                }
            }
        }
        tm = TradeManager(config)
        
        # Open 3 positions
        for i in range(3):
            tm.open_position(
                trade_id=i+1,
                timestamp=pd.Timestamp(f'2025-02-13 {10+i}:00:00'),
                direction=TradeDirection.LONG,
                entry_price=19875.0 + i*25,
                stop_loss=19850.0 + i*25,
                take_profit=19950.0 + i*25
            )
        
        # Close position 2
        tm.close_positions([2])
        
        positions = tm.get_current_positions()
        assert len(positions) == 2
        assert positions[0].position_id == 1
        assert positions[1].position_id == 3
    
    def test_close_all_positions(self):
        """Test closing all positions"""
        config = {
            'trade_management': {
                'position_control': {
                    'pyramiding_enabled': True
                }
            }
        }
        tm = TradeManager(config)
        
        # Open 3 positions
        for i in range(3):
            tm.open_position(
                trade_id=i+1,
                timestamp=pd.Timestamp(f'2025-02-13 {10+i}:00:00'),
                direction=TradeDirection.LONG,
                entry_price=19875.0 + i*25,
                stop_loss=19850.0 + i*25,
                take_profit=19950.0 + i*25
            )
        
        # Close all
        tm.close_positions([1, 2, 3])
        
        assert len(tm.get_current_positions()) == 0
        assert tm.has_open_position() is False
    
    def test_current_direction_property(self):
        """Test current_direction property with Position contracts"""
        config = {'trade_management': {'position_control': {}}}
        tm = TradeManager(config)
        
        # No positions
        assert tm.current_direction is None
        
        # LONG position
        tm.open_position(
            trade_id=1,
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            direction=TradeDirection.LONG,
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        
        assert tm.current_direction == TradeDirection.LONG
        
        # Close and open SHORT
        tm.close_positions([1])
        tm.open_position(
            trade_id=2,
            timestamp=pd.Timestamp('2025-02-13 11:00:00'),
            direction=TradeDirection.SHORT,
            entry_price=19880.0,
            stop_loss=19905.0,
            take_profit=19805.0
        )
        
        assert tm.current_direction == TradeDirection.SHORT


class TestPositionToDict:
    """Test Position contract to_dict() conversion"""
    
    def test_position_to_dict(self):
        """Test Position.to_dict() method"""
        config = {'trade_management': {'position_control': {}}}
        tm = TradeManager(config)
        
        tm.open_position(
            trade_id=1,
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            direction=TradeDirection.LONG,
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0,
            position_size=1.5
        )
        
        pos = tm.get_current_positions()[0]
        pos_dict = pos.to_dict()
        
        # Verify dict structure
        assert isinstance(pos_dict, dict)
        assert pos_dict['position_id'] == 1
        assert pos_dict['entry_price'] == 19875.0
        assert pos_dict['stop_loss'] == 19850.0
        assert pos_dict['take_profit'] == 19950.0
        assert pos_dict['size'] == 1.5


class TestPositionReset:
    """Test position state reset"""
    
    def test_reset_clears_positions(self):
        """Test reset() clears all positions"""
        config = {
            'trade_management': {
                'position_control': {
                    'pyramiding_enabled': True
                }
            }
        }
        tm = TradeManager(config)
        
        # Open positions
        for i in range(3):
            tm.open_position(
                trade_id=i+1,
                timestamp=pd.Timestamp(f'2025-02-13 {10+i}:00:00'),
                direction=TradeDirection.LONG,
                entry_price=19875.0 + i*25,
                stop_loss=19850.0 + i*25,
                take_profit=19950.0 + i*25
            )
        
        assert len(tm.get_current_positions()) == 3
        
        # Reset
        tm.reset()
        
        assert len(tm.get_current_positions()) == 0
        assert tm.trade_counter == 0
        assert tm.has_open_position() is False


if __name__ == '__main__':
    pytest.main([__file__, '-v'])