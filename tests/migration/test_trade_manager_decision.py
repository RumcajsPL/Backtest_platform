"""Unit tests for TradeManager TradeDecision contract integration

Tests that TradeManager correctly returns TradeDecision contracts
with proper decision types, reasons, and metadata.

Session 8 - Test Suite 1
"""
import pytest
import pandas as pd
from datetime import datetime

# Add project root to path for imports
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.strategies.specific.modules.trade_manager import TradeManager
from src.strategies.contracts.trade_contracts import TradeDecision, DecisionType, TradeDirection


class TestTradeDecisionBasic:
    """Basic TradeDecision contract functionality"""
    
    def test_handle_signal_returns_trade_decision(self):
        """Test that handle_signal returns TradeDecision contract"""
        config = {
            'trade_management': {
                'position_control': {
                    'close_on_opposite': False,
                    'pyramiding_enabled': False,
                }
            }
        }
        
        tm = TradeManager(config)
        
        decision = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            signal_type='BUY',
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0,
            position_size=1.0
        )
        
        # Verify contract type
        assert isinstance(decision, TradeDecision)
        assert hasattr(decision, 'decision_type')
        assert hasattr(decision, 'reason')
        assert hasattr(decision, 'close_trade_ids')
        assert hasattr(decision, 'new_trade_id')
    
    def test_decision_is_frozen(self):
        """Test that TradeDecision is immutable"""
        config = {'trade_management': {'position_control': {}}}
        tm = TradeManager(config)
        
        decision = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            signal_type='BUY',
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        
        # Should raise error when trying to modify
        with pytest.raises(Exception):  # FrozenInstanceError
            decision.decision_type = DecisionType.REJECT


class TestOpenDecisions:
    """Tests for OPEN decision type"""
    
    def test_open_new_position_no_existing(self):
        """Test opening position when no positions exist"""
        config = {
            'trade_management': {
                'position_control': {
                    'close_on_opposite': False,
                    'pyramiding_enabled': False,
                }
            }
        }
        
        tm = TradeManager(config)
        
        decision = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            signal_type='BUY',
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0,
            position_size=1.0
        )
        
        assert decision.decision_type == DecisionType.OPEN
        assert decision.new_trade_id == 1
        assert decision.close_trade_ids is None
        assert 'Opening' in decision.reason
        assert 'BUY' in decision.reason
    
    def test_open_pyramiding_enabled(self):
        """Test opening second position with pyramiding enabled"""
        config = {
            'trade_management': {
                'position_control': {
                    'close_on_opposite': False,
                    'pyramiding_enabled': True,
                }
            }
        }
        
        tm = TradeManager(config)
        
        # Open first position
        decision1 = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            signal_type='BUY',
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        
        tm.open_position(
            trade_id=decision1.new_trade_id,
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            direction=TradeDirection.LONG,
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        
        # Try second BUY signal (should open due to pyramiding)
        decision2 = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 11:00:00'),
            signal_type='BUY',
            entry_price=19900.0,
            stop_loss=19875.0,
            take_profit=19975.0
        )
        
        assert decision2.decision_type == DecisionType.OPEN
        assert decision2.new_trade_id == 2
        assert decision2.close_trade_ids is None
    
    def test_open_with_sell_signal(self):
        """Test opening SELL position"""
        config = {'trade_management': {'position_control': {}}}
        tm = TradeManager(config)
        
        decision = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            signal_type='SELL',
            entry_price=19880.0,
            stop_loss=19905.0,
            take_profit=19805.0
        )
        
        assert decision.decision_type == DecisionType.OPEN
        assert 'SELL' in decision.reason


class TestRejectDecisions:
    """Tests for REJECT decision type"""
    
    def test_reject_pyramiding_disabled(self):
        """Test rejection when pyramiding disabled"""
        config = {
            'trade_management': {
                'position_control': {
                    'close_on_opposite': False,
                    'pyramiding_enabled': False,
                }
            }
        }
        
        tm = TradeManager(config)
        
        # Open first position
        decision1 = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            signal_type='BUY',
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        
        tm.open_position(
            trade_id=decision1.new_trade_id,
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            direction=TradeDirection.LONG,
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        
        # Try second BUY signal (should reject)
        decision2 = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 11:00:00'),
            signal_type='BUY',
            entry_price=19900.0,
            stop_loss=19875.0,
            take_profit=19975.0
        )
        
        assert decision2.decision_type == DecisionType.REJECT
        assert 'Pyramiding disabled' in decision2.reason
        assert decision2.new_trade_id is None
        assert decision2.close_trade_ids is None
    
    def test_reject_opposite_signal_ignored(self):
        """Test rejection of opposite signal when close_on_opposite=False"""
        config = {
            'trade_management': {
                'position_control': {
                    'close_on_opposite': False,
                    'pyramiding_enabled': False,
                }
            }
        }
        
        tm = TradeManager(config)
        
        # Open BUY position
        decision1 = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            signal_type='BUY',
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        
        tm.open_position(
            trade_id=decision1.new_trade_id,
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            direction=TradeDirection.LONG,
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        
        # SELL signal (opposite) - should reject
        decision2 = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 11:00:00'),
            signal_type='SELL',
            entry_price=19880.0,
            stop_loss=19905.0,
            take_profit=19805.0
        )
        
        assert decision2.decision_type == DecisionType.REJECT
        assert 'Opposite signal ignored' in decision2.reason
        assert decision2.new_trade_id is None
        assert decision2.close_trade_ids is None


class TestCloseAndReverseDecisions:
    """Tests for CLOSE_AND_REVERSE decision type"""
    
    def test_close_and_reverse_single_position(self):
        """Test close and reverse with single open position"""
        config = {
            'trade_management': {
                'position_control': {
                    'close_on_opposite': True,
                    'pyramiding_enabled': False,
                }
            }
        }
        
        tm = TradeManager(config)
        
        # Open BUY position
        decision1 = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            signal_type='BUY',
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        
        tm.open_position(
            trade_id=decision1.new_trade_id,
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            direction=TradeDirection.LONG,
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        
        # SELL signal (opposite)
        decision2 = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 11:00:00'),
            signal_type='SELL',
            entry_price=19880.0,
            stop_loss=19905.0,
            take_profit=19805.0
        )
        
        assert decision2.decision_type == DecisionType.CLOSE_AND_REVERSE
        assert decision2.close_trade_ids == [1]
        assert decision2.new_trade_id == 2
        assert 'Closing' in decision2.reason
        assert 'reversing' in decision2.reason
    
    def test_close_and_reverse_multiple_positions(self):
        """Test close and reverse with multiple pyramided positions"""
        config = {
            'trade_management': {
                'position_control': {
                    'close_on_opposite': True,
                    'pyramiding_enabled': True,
                }
            }
        }
        
        tm = TradeManager(config)
        
        # Open 3 BUY positions
        for i in range(3):
            decision = tm.handle_signal(
                timestamp=pd.Timestamp(f'2025-02-13 {10+i}:00:00'),
                signal_type='BUY',
                entry_price=19875.0 + i*25,
                stop_loss=19850.0 + i*25,
                take_profit=19950.0 + i*25
            )
            
            tm.open_position(
                trade_id=decision.new_trade_id,
                timestamp=pd.Timestamp(f'2025-02-13 {10+i}:00:00'),
                direction=TradeDirection.LONG,
                entry_price=19875.0 + i*25,
                stop_loss=19850.0 + i*25,
                take_profit=19950.0 + i*25
            )
        
        # SELL signal (opposite) - should close all 3
        decision_reverse = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 13:00:00'),
            signal_type='SELL',
            entry_price=19880.0,
            stop_loss=19905.0,
            take_profit=19805.0
        )
        
        assert decision_reverse.decision_type == DecisionType.CLOSE_AND_REVERSE
        assert decision_reverse.close_trade_ids == [1, 2, 3]
        assert decision_reverse.new_trade_id == 4
        assert '3' in decision_reverse.reason


class TestDecisionProperties:
    """Test TradeDecision helper properties"""
    
    def test_is_open_property(self):
        """Test is_open property"""
        config = {'trade_management': {'position_control': {}}}
        tm = TradeManager(config)
        
        decision = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            signal_type='BUY',
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        
        assert decision.is_open is True
    
    def test_is_reject_property(self):
        """Test is_reject property"""
        config = {
            'trade_management': {
                'position_control': {
                    'close_on_opposite': False,
                    'pyramiding_enabled': False,
                }
            }
        }
        
        tm = TradeManager(config)
        
        # Open position first
        decision1 = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            signal_type='BUY',
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        tm.open_position(
            trade_id=decision1.new_trade_id,
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            direction=TradeDirection.LONG,
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        
        # Second signal (should reject)
        decision2 = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 11:00:00'),
            signal_type='BUY',
            entry_price=19900.0,
            stop_loss=19875.0,
            take_profit=19975.0
        )
        
        assert decision2.is_reject is True
    
    def test_to_dict_method(self):
        """Test to_dict() conversion"""
        config = {'trade_management': {'position_control': {}}}
        tm = TradeManager(config)
        
        decision = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            signal_type='BUY',
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        
        decision_dict = decision.to_dict()
        
        # Verify dict structure
        assert isinstance(decision_dict, dict)
        assert 'action' in decision_dict
        assert 'reason' in decision_dict
        assert 'close_trade_ids' in decision_dict
        assert 'new_trade_id' in decision_dict
        
        # Verify values
        assert decision_dict['action'] == 'OPEN'
        assert decision_dict['new_trade_id'] == 1


class TestMetricsWithDecisions:
    """Test that metrics tracking works correctly with TradeDecision"""
    
    def test_metrics_track_decisions(self):
        """Test metrics are updated correctly"""
        config = {
            'trade_management': {
                'position_control': {
                    'close_on_opposite': True,
                    'pyramiding_enabled': False,
                }
            }
        }
        
        tm = TradeManager(config)
        
        # Signal 1: OPEN
        decision1 = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            signal_type='BUY',
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0,
            position_size=1.0
        )
        
        # IMPORTANT: Actually open the position
        tm.open_position(
            trade_id=decision1.new_trade_id,
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            direction=TradeDirection.LONG,
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0,
            position_size=1.0
        )
        
        metrics = tm.get_metrics()
        assert metrics['total_signals_received'] == 1
        assert metrics['signals_accepted'] == 1
        assert metrics['signals_rejected'] == 0
        
        # Signal 2: Should REJECT (pyramiding disabled)
        decision2 = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 11:00:00'),
            signal_type='BUY',
            entry_price=19900.0,
            stop_loss=19875.0,
            take_profit=19975.0,
            position_size=1.0
        )
        
        # Verify decision type is REJECT
        assert decision2.decision_type == DecisionType.REJECT
        
        metrics = tm.get_metrics()
        assert metrics['total_signals_received'] == 2
        assert metrics['signals_accepted'] == 1  # Still 1 accepted
        assert metrics['signals_rejected'] == 1  # 1 rejected
        assert metrics['rejected_reasons']['pyramiding_disabled'] == 1


if __name__ == '__main__':
    pytest.main([__file__, '-v'])