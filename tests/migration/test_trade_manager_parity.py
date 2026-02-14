"""Parity tests for TradeManager migration

Verifies that contract-based TradeManager produces identical behavior
to legacy dict-based implementation.

Session 8 - Test Suite 3
"""
import pytest
import pandas as pd
from datetime import datetime

# Add project root to path for imports
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.strategies.specific.modules.trade_manager import TradeManager
from src.strategies.contracts.trade_contracts import DecisionType, TradeDirection


class TestLegacyDictParity:
    """Test that to_dict() matches legacy format exactly"""
    
    def test_open_decision_dict_format(self):
        """Test OPEN decision to_dict() matches legacy"""
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
        
        # Verify exact legacy structure
        assert set(decision_dict.keys()) == {'action', 'reason', 'close_trade_ids', 'new_trade_id'}
        assert decision_dict['action'] == 'OPEN'
        assert isinstance(decision_dict['reason'], str)
        assert decision_dict['close_trade_ids'] is None
        assert decision_dict['new_trade_id'] == 1
    
    def test_reject_decision_dict_format(self):
        """Test REJECT decision to_dict() matches legacy"""
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
        
        # Second signal (reject)
        decision2 = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 11:00:00'),
            signal_type='BUY',
            entry_price=19900.0,
            stop_loss=19875.0,
            take_profit=19975.0
        )
        
        decision_dict = decision2.to_dict()
        
        assert decision_dict['action'] == 'REJECT'
        assert decision_dict['close_trade_ids'] is None
        assert decision_dict['new_trade_id'] is None
    
    def test_close_and_reverse_dict_format(self):
        """Test CLOSE_AND_REVERSE decision to_dict() matches legacy"""
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
        
        # SELL signal (reverse)
        decision2 = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 11:00:00'),
            signal_type='SELL',
            entry_price=19880.0,
            stop_loss=19905.0,
            take_profit=19805.0
        )
        
        decision_dict = decision2.to_dict()
        
        assert decision_dict['action'] == 'CLOSE_AND_REVERSE'
        assert decision_dict['close_trade_ids'] == [1]
        assert decision_dict['new_trade_id'] == 2


class TestMetricsParity:
    """Test that metrics tracking is identical to legacy"""
    
    def test_metrics_structure_unchanged(self):
        """Test metrics dict structure matches legacy"""
        config = {'trade_management': {'position_control': {}}}
        tm = TradeManager(config)
        
        metrics = tm.get_metrics()
        
        # Verify exact legacy structure
        expected_keys = {
            'total_signals_received',
            'signals_accepted',
            'signals_rejected',
            'rejected_reasons',
            'positions_closed_by_opposite',
            'positions_reversed',
        }
        assert set(metrics.keys()) == expected_keys
        
        # Verify rejected_reasons sub-dict
        assert set(metrics['rejected_reasons'].keys()) == {
            'pyramiding_disabled',
            'opposite_ignored',
        }
    
    def test_metrics_values_match_legacy(self):
        """Test metrics values match legacy behavior"""
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
            take_profit=19950.0
        )
        assert decision1.decision_type == DecisionType.OPEN
        
        tm.open_position(
            trade_id=decision1.new_trade_id,
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            direction=TradeDirection.LONG,
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        
        metrics_after_open = tm.get_metrics()
        print(f"\nAfter OPEN: accepted={metrics_after_open['signals_accepted']}")
        
        # Signal 2: REJECT (pyramiding)
        decision2 = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 11:00:00'),
            signal_type='BUY',
            entry_price=19900.0,
            stop_loss=19875.0,
            take_profit=19975.0
        )
        assert decision2.decision_type == DecisionType.REJECT
        
        metrics_after_reject = tm.get_metrics()
        print(f"After REJECT: accepted={metrics_after_reject['signals_accepted']}")
        
        # Signal 3: CLOSE_AND_REVERSE
        decision3 = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 12:00:00'),
            signal_type='SELL',
            entry_price=19880.0,
            stop_loss=19905.0,
            take_profit=19805.0
        )
        assert decision3.decision_type == DecisionType.CLOSE_AND_REVERSE, \
            f"Expected CLOSE_AND_REVERSE, got {decision3.decision_type}"
        
        metrics = tm.get_metrics()
        print(f"After REVERSE: accepted={metrics['signals_accepted']}")
        
        # Verify exact counts
        assert metrics['total_signals_received'] == 3
        assert metrics['signals_accepted'] == 2  # OPEN + CLOSE_AND_REVERSE
        assert metrics['signals_rejected'] == 1  # Pyramiding
        assert metrics['rejected_reasons']['pyramiding_disabled'] == 1
        assert metrics['rejected_reasons']['opposite_ignored'] == 0
        assert metrics['positions_closed_by_opposite'] == 1
        assert metrics['positions_reversed'] == 1


class TestBehaviorParity:
    """Test that decision logic behavior matches legacy exactly"""
    
    def test_pyramiding_disabled_behavior(self):
        """Test pyramiding disabled behavior matches legacy"""
        config = {
            'trade_management': {
                'position_control': {
                    'pyramiding_enabled': False
                }
            }
        }
        tm = TradeManager(config)
        
        # First signal: should open
        decision1 = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            signal_type='BUY',
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        assert decision1.decision_type == DecisionType.OPEN
        
        tm.open_position(
            trade_id=decision1.new_trade_id,
            timestamp=pd.Timestamp('2025-02-13 10:00:00'),
            direction=TradeDirection.LONG,
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        
        # Second same-direction signal: should reject
        decision2 = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 11:00:00'),
            signal_type='BUY',
            entry_price=19900.0,
            stop_loss=19875.0,
            take_profit=19975.0
        )
        assert decision2.decision_type == DecisionType.REJECT
        
        # Position count should be 1 (unchanged)
        assert len(tm.get_current_positions()) == 1
    
    def test_pyramiding_enabled_behavior(self):
        """Test pyramiding enabled behavior matches legacy"""
        config = {
            'trade_management': {
                'position_control': {
                    'pyramiding_enabled': True
                }
            }
        }
        tm = TradeManager(config)
        
        # Open multiple same-direction positions
        for i in range(3):
            decision = tm.handle_signal(
                timestamp=pd.Timestamp(f'2025-02-13 {10+i}:00:00'),
                signal_type='BUY',
                entry_price=19875.0 + i*25,
                stop_loss=19850.0 + i*25,
                take_profit=19950.0 + i*25
            )
            assert decision.decision_type == DecisionType.OPEN
            
            tm.open_position(
                trade_id=decision.new_trade_id,
                timestamp=pd.Timestamp(f'2025-02-13 {10+i}:00:00'),
                direction=TradeDirection.LONG,
                entry_price=19875.0 + i*25,
                stop_loss=19850.0 + i*25,
                take_profit=19950.0 + i*25
            )
        
        # Should have 3 positions
        assert len(tm.get_current_positions()) == 3
    
    def test_close_on_opposite_true_behavior(self):
        """Test close_on_opposite=True behavior matches legacy"""
        config = {
            'trade_management': {
                'position_control': {
                    'close_on_opposite': True,
                    'pyramiding_enabled': True
                }
            }
        }
        tm = TradeManager(config)
        
        # Open 2 BUY positions
        for i in range(2):
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
        
        # Opposite signal (SELL)
        decision_reverse = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 12:00:00'),
            signal_type='SELL',
            entry_price=19880.0,
            stop_loss=19905.0,
            take_profit=19805.0
        )
        
        # Should close both and prepare to reverse
        assert decision_reverse.decision_type == DecisionType.CLOSE_AND_REVERSE
        assert set(decision_reverse.close_trade_ids) == {1, 2}
        assert decision_reverse.new_trade_id == 3
    
    def test_close_on_opposite_false_behavior(self):
        """Test close_on_opposite=False behavior matches legacy"""
        config = {
            'trade_management': {
                'position_control': {
                    'close_on_opposite': False,
                    'pyramiding_enabled': False
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
        
        # Opposite signal (SELL) - should reject
        decision2 = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 11:00:00'),
            signal_type='SELL',
            entry_price=19880.0,
            stop_loss=19905.0,
            take_profit=19805.0
        )
        
        assert decision2.decision_type == DecisionType.REJECT
        
        # Position count unchanged
        assert len(tm.get_current_positions()) == 1


class TestTradeIDParity:
    """Test that trade ID sequencing matches legacy"""
    
    def test_trade_id_sequence(self):
        """Test trade IDs increment correctly"""
        config = {
            'trade_management': {
                'position_control': {
                    'pyramiding_enabled': True
                }
            }
        }
        tm = TradeManager(config)
        
        # Open 5 positions
        for i in range(5):
            decision = tm.handle_signal(
                timestamp=pd.Timestamp(f'2025-02-13 {10+i}:00:00'),
                signal_type='BUY',
                entry_price=19875.0 + i*25,
                stop_loss=19850.0 + i*25,
                take_profit=19950.0 + i*25
            )
            
            # IDs should be 1, 2, 3, 4, 5
            assert decision.new_trade_id == i + 1
            
            tm.open_position(
                trade_id=decision.new_trade_id,
                timestamp=pd.Timestamp(f'2025-02-13 {10+i}:00:00'),
                direction=TradeDirection.LONG,
                entry_price=19875.0 + i*25,
                stop_loss=19850.0 + i*25,
                take_profit=19950.0 + i*25
            )
        
        # Trade counter should be 5
        assert tm.trade_counter == 5
    
    def test_trade_id_after_close(self):
        """Test trade IDs continue after closing positions"""
        config = {
            'trade_management': {
                'position_control': {
                    'pyramiding_enabled': True
                }
            }
        }
        tm = TradeManager(config)
        
        # Open 2 positions
        for i in range(2):
            decision = tm.handle_signal(
                timestamp=pd.Timestamp(f'2025-02-13 {10+i}:00:00'),
                signal_type='BUY',
                entry_price=19875.0,
                stop_loss=19850.0,
                take_profit=19950.0
            )
            tm.open_position(
                trade_id=decision.new_trade_id,
                timestamp=pd.Timestamp(f'2025-02-13 {10+i}:00:00'),
                direction=TradeDirection.LONG,
                entry_price=19875.0,
                stop_loss=19850.0,
                take_profit=19950.0
            )
        
        # Close position 1
        tm.close_positions([1])
        
        # Next position should be ID 3 (not restart from 1)
        decision3 = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 12:00:00'),
            signal_type='BUY',
            entry_price=19900.0,
            stop_loss=19875.0,
            take_profit=19975.0
        )
        assert decision3.new_trade_id == 3


class TestResetParity:
    """Test that reset() behavior matches legacy"""
    
    def test_reset_clears_everything(self):
        """Test reset() returns to initial state"""
        config = {
            'trade_management': {
                'position_control': {
                    'close_on_opposite': True,
                    'pyramiding_enabled': True
                }
            }
        }
        tm = TradeManager(config)
        
        # Perform various operations
        for i in range(3):
            decision = tm.handle_signal(
                timestamp=pd.Timestamp(f'2025-02-13 {10+i}:00:00'),
                signal_type='BUY',
                entry_price=19875.0,
                stop_loss=19850.0,
                take_profit=19950.0
            )
            tm.open_position(
                trade_id=decision.new_trade_id,
                timestamp=pd.Timestamp(f'2025-02-13 {10+i}:00:00'),
                direction=TradeDirection.LONG,
                entry_price=19875.0,
                stop_loss=19850.0,
                take_profit=19950.0
            )
        
        # Reset
        tm.reset()
        
        # Verify everything cleared
        assert len(tm.get_current_positions()) == 0
        assert tm.trade_counter == 0
        assert tm.has_open_position() is False
        
        metrics = tm.get_metrics()
        assert metrics['total_signals_received'] == 0
        assert metrics['signals_accepted'] == 0
        assert metrics['signals_rejected'] == 0
        
        # Next trade ID should be 1 again
        decision_after_reset = tm.handle_signal(
            timestamp=pd.Timestamp('2025-02-13 14:00:00'),
            signal_type='BUY',
            entry_price=19875.0,
            stop_loss=19850.0,
            take_profit=19950.0
        )
        assert decision_after_reset.new_trade_id == 1


if __name__ == '__main__':
    pytest.main([__file__, '-v'])