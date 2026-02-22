"""
Unit Tests for MetricsCalculator
=================================
Tests calculation of all 17 performance metrics with edge cases.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from src.strategies.specific.modules.metrics_calculator import (
    MetricsCalculator,
    calculate_metrics,
    calculate_metrics_with_timing
)
from src.strategies.contracts.trade_contracts import (
    Trade, TradeEntry, TradeExit, ExitReason, TradeDirection
)
from src.strategies.contracts.metrics_contracts import MetricsReport


class TestMetricsCalculator:
    """Tests for MetricsCalculator class."""

    @pytest.fixture
    def sample_trades(self):
        """Generate sample trades for testing using proper TradeExit.create() method."""
        trades = []
        base_time = pd.Timestamp("2025-01-01 10:00:00")
        
        # Winning trades
        for i in range(10):
            entry = TradeEntry(
                entry_id=f"E{i}",
                trade_manager_id=i,
                entry_time=base_time + timedelta(minutes=i*30),
                direction=TradeDirection.LONG,
                entry_price=100.0,
                stop_loss=99.0,
                take_profit=105.0,
                position_size=1.0
            )
            exit = TradeExit.create(
                entry=entry,
                exit_time=entry.entry_time + timedelta(minutes=15),
                exit_price=105.0,
                exit_reason=ExitReason.TAKE_PROFIT
            )
            trades.append(Trade(entry=entry, exit=exit))
        
        # Losing trades
        for i in range(5):
            entry = TradeEntry(
                entry_id=f"E{i+10}",
                trade_manager_id=i+10,
                entry_time=base_time + timedelta(minutes=i*30 + 15),
                direction=TradeDirection.LONG,
                entry_price=100.0,
                stop_loss=99.0,
                take_profit=105.0,
                position_size=1.0
            )
            exit = TradeExit.create(
                entry=entry,
                exit_time=entry.entry_time + timedelta(minutes=5),
                exit_price=99.0,
                exit_reason=ExitReason.STOP_LOSS
            )
            trades.append(Trade(entry=entry, exit=exit))
        
        return trades

    @pytest.fixture
    def sample_trade_result(self, sample_trades):
        """Create a mock TradeResult with sample trades."""
        from src.strategies.contracts.trade_contracts import TradeResult
        
        # Create a mock TradeResult
        class MockTradeResult:
            def __init__(self, trades):
                self.trades = trades
                self.closed_trades = [t for t in trades if t.exit is not None]
        
        return MockTradeResult(sample_trades)

    def test_calculate_with_valid_trades(self, sample_trade_result):
        """Test metrics calculation with valid trades."""
        metrics = MetricsCalculator.calculate(sample_trade_result)
        
        assert isinstance(metrics, MetricsReport)
        assert metrics.total_trades == 15
        assert metrics.winning_trades == 10
        assert metrics.losing_trades == 5
        assert metrics.win_rate == round(10/15 * 100, 2)
        
        # P&L calculations
        assert metrics.total_pnl_points == 10*5.0 + 5*(-1.0)  # 50 - 5 = 45
        assert metrics.expectancy_points == 45.0 / 15  # 3.0
        
        # Profit factor: gross profit / gross loss
        # Gross profit: 10 * 5.0 = 50
        # Gross loss: 5 * 1.0 = 5
        # Profit factor = 50 / 5 = 10.0
        assert metrics.profit_factor == 10.0

    def test_calculate_with_no_trades(self):
        """Test metrics calculation with no trades."""
        class MockEmptyTradeResult:
            def __init__(self):
                self.trades = []
                self.closed_trades = []
        
        metrics = MetricsCalculator.calculate(MockEmptyTradeResult())
        
        assert isinstance(metrics, MetricsReport)
        assert metrics.total_trades == 0
        assert metrics.winning_trades == 0
        assert metrics.losing_trades == 0
        assert metrics.win_rate == 0.0
        assert metrics.total_pnl_points == 0.0
        assert metrics.profit_factor == 0.0
        assert metrics.max_drawdown == 0.0

    def test_calculate_with_timing(self, sample_trade_result):
        """Test metrics calculation with timing."""
        import time
        start = time.perf_counter()
        
        metrics = MetricsCalculator.calculate(sample_trade_result, start_time=start)
        
        assert metrics.execution_duration_ms > 0
        assert isinstance(metrics.execution_duration_ms, float)

    def test_calculate_win_loss_counts(self, sample_trades):
        """Test win/loss count calculation."""
        calculator = MetricsCalculator()
        
        winning, losing = calculator._calculate_win_loss_counts(sample_trades)
        
        assert winning == 10
        assert losing == 5

    def test_calculate_win_rate(self):
        """Test win rate calculation."""
        calculator = MetricsCalculator()
        
        # Normal case
        win_rate = calculator._calculate_win_rate(10, 20)
        assert win_rate == 50.0
        
        # Edge cases
        assert calculator._calculate_win_rate(0, 10) == 0.0
        assert calculator._calculate_win_rate(10, 0) == 0.0  # Should handle division by zero
        assert calculator._calculate_win_rate(0, 0) == 0.0

    def test_calculate_pnl_metrics(self, sample_trades):
        """Test P&L metrics calculation."""
        calculator = MetricsCalculator()
        
        total_pnl, expectancy, avg_pnl = calculator._calculate_pnl_metrics(sample_trades, 15)
        
        assert total_pnl == 45.0
        assert expectancy == 3.0
        assert avg_pnl == 3.0

    def test_calculate_profit_factor(self, sample_trades):
        """Test profit factor calculation."""
        calculator = MetricsCalculator()
        
        profit_factor = calculator._calculate_profit_factor(sample_trades)
        
        # 10 wins of 5.0 = 50 gross profit
        # 5 losses of 1.0 = 5 gross loss
        assert profit_factor == 10.0

    def test_profit_factor_edge_cases(self):
        """Test profit factor edge cases."""
        calculator = MetricsCalculator()

        # No trades
        assert calculator._calculate_profit_factor([]) == 0.0

        # Only winning trades
        class MockWinTrade:
            def __init__(self):
                self.exit = type('obj', (), {
                    'is_win': True,
                    'is_loss': False,
                    'pnl_points': 10.0
                })

        win_trades = [MockWinTrade() for _ in range(5)]
        assert calculator._calculate_profit_factor(win_trades) == float('inf')

        # Only losing trades
        class MockLossTrade:
            def __init__(self):
                self.exit = type('obj', (), {
                    'is_win': False,
                    'is_loss': True,
                    'pnl_points': -5.0
                })

        loss_trades = [MockLossTrade() for _ in range(3)]
        assert calculator._calculate_profit_factor(loss_trades) == 0.0

    def test_calculate_extremes(self, sample_trades):
        """Test largest win/loss calculation."""
        calculator = MetricsCalculator()
        
        largest_win, largest_loss = calculator._calculate_extremes(
            sample_trades, 10, 5
        )
        
        assert largest_win == 5.0
        assert largest_loss == -1.0

    def test_calculate_max_drawdown(self):
        """Test max drawdown calculation."""
        calculator = MetricsCalculator()
        
        # Create trades with known P&L sequence using proper TradeExit.create()
        trades = []
        base_time = pd.Timestamp("2025-01-01")
        
        # Sequence: +10, +5, -8, +3, -5, +2
        # Let's calculate drawdown correctly:
        # Trade 1: +10 → cumulative = 10, peak = 10, drawdown = 0
        # Trade 2: +5 → cumulative = 15, peak = 15, drawdown = 0
        # Trade 3: -8 → cumulative = 7, peak = 15, drawdown = -8
        # Trade 4: +3 → cumulative = 10, peak = 15, drawdown = -5
        # Trade 5: -5 → cumulative = 5, peak = 15, drawdown = -10 (max)
        # Trade 6: +2 → cumulative = 7, peak = 15, drawdown = -8
        # Maximum drawdown is -10.0
        
        pnl_sequence = [10.0, 5.0, -8.0, 3.0, -5.0, 2.0]
        
        for i, pnl in enumerate(pnl_sequence):
            entry = TradeEntry(
                entry_id=f"E{i}",
                entry_time=base_time + timedelta(days=i),
                direction=TradeDirection.LONG,
                entry_price=100.0,
                stop_loss=99.0,
                take_profit=105.0
            )
            exit = TradeExit.create(
                entry=entry,
                exit_time=entry.entry_time + timedelta(hours=1),
                exit_price=100.0 + pnl,
                exit_reason=ExitReason.TAKE_PROFIT if pnl > 0 else ExitReason.STOP_LOSS
            )
            trades.append(Trade(entry=entry, exit=exit))
        
        max_dd = calculator._calculate_max_drawdown(trades)
        
        # Expected max drawdown: after cumulative 15, then -10 = -10.0
        assert max_dd == -10.0

    def test_calculate_max_drawdown_empty(self):
        """Test max drawdown with empty trades list."""
        calculator = MetricsCalculator()
        assert calculator._calculate_max_drawdown([]) == 0.0

    def test_calculate_streaks(self):
        """Test winning/losing streak calculation using proper TradeExit.create()."""
        calculator = MetricsCalculator()
        
        # Create trades with pattern: W, W, L, W, L, L, L, W
        trades = []
        base_time = pd.Timestamp("2025-01-01")
        win_loss_pattern = [True, True, False, True, False, False, False, True]
        
        for i, is_win in enumerate(win_loss_pattern):
            entry = TradeEntry(
                entry_id=f"E{i}",
                entry_time=base_time + timedelta(days=i),
                direction=TradeDirection.LONG,
                entry_price=100.0,
                stop_loss=99.0,
                take_profit=105.0
            )
            exit = TradeExit.create(
                entry=entry,
                exit_time=entry.entry_time + timedelta(hours=1),
                exit_price=105.0 if is_win else 99.0,
                exit_reason=ExitReason.TAKE_PROFIT if is_win else ExitReason.STOP_LOSS
            )
            trades.append(Trade(entry=entry, exit=exit))
        
        win_streak, loss_streak = calculator._calculate_streaks(trades)
        
        assert win_streak == 2  # First two wins
        assert loss_streak == 3  # Three losses in a row

    def test_calculate_streaks_empty(self):
        """Test streaks with empty trades."""
        calculator = MetricsCalculator()
        win_streak, loss_streak = calculator._calculate_streaks([])
        assert win_streak == 0
        assert loss_streak == 0

    def test_calculate_frequency(self):
        """Test trade frequency calculation using proper TradeExit.create()."""
        calculator = MetricsCalculator()
        
        # Create trades spanning 10 days
        trades = []
        base_time = pd.Timestamp("2025-01-01")
        
        for i in range(20):
            entry = TradeEntry(
                entry_id=f"E{i}",
                entry_time=base_time + timedelta(days=i//2),  # 2 trades per day
                direction=TradeDirection.LONG,
                entry_price=100.0,
                stop_loss=99.0,
                take_profit=105.0
            )
            exit = TradeExit.create(
                entry=entry,
                exit_time=entry.entry_time + timedelta(hours=1),
                exit_price=105.0,
                exit_reason=ExitReason.TAKE_PROFIT
            )
            trades.append(Trade(entry=entry, exit=exit))
        
        # First entry: day 0, last exit: day 9.5 -> 10 calendar days
        trades_per_day, trades_per_week = calculator._calculate_frequency(trades, 20)
        
        assert trades_per_day == 20 / 10  # 2.0
        assert trades_per_week == 2.0 * 7  # 14.0

    def test_calculate_frequency_single_day(self):
        """Test frequency with all trades on same day using proper TradeExit.create()."""
        calculator = MetricsCalculator()
        
        trades = []
        base_time = pd.Timestamp("2025-01-01 10:00:00")
        
        for i in range(10):
            entry = TradeEntry(
                entry_id=f"E{i}",
                entry_time=base_time + timedelta(minutes=i*10),
                direction=TradeDirection.LONG,
                entry_price=100.0,
                stop_loss=99.0,
                take_profit=105.0
            )
            exit = TradeExit.create(
                entry=entry,
                exit_time=entry.entry_time + timedelta(minutes=5),
                exit_price=105.0,
                exit_reason=ExitReason.TAKE_PROFIT
            )
            trades.append(Trade(entry=entry, exit=exit))
        
        trades_per_day, trades_per_week = calculator._calculate_frequency(trades, 10)
        
        assert trades_per_day == 10.0  # 10 trades / 1 day
        assert trades_per_week == 70.0

    def test_calculate_frequency_no_trades(self):
        """Test frequency with no trades."""
        calculator = MetricsCalculator()
        
        trades_per_day, trades_per_week = calculator._calculate_frequency([], 0)
        
        assert trades_per_day == 0.0
        assert trades_per_week == 0.0

    def test_calculate_with_mixed_trade_properties(self):
        """Test metrics with trades that have different property access patterns."""
        # Create trades that use different patterns (is_win property vs exit.is_win)
        trades = []
        base_time = pd.Timestamp("2025-01-01")

        # Trade with is_win property - needs entry attribute for frequency calculation
        class MockTradeWithIsWin:
            def __init__(self, is_win, pnl):
                self.is_win = is_win
                self.is_loss = not is_win
                self.entry = type('obj', (), {
                    'entry_time': base_time
                })
                self.exit = type('obj', (), {
                    'pnl_points': pnl,
                    'exit_time': base_time,
                    'is_win': is_win,
                    'is_loss': not is_win
                })

        # Trade with exit.is_win pattern - needs entry attribute for frequency calculation
        class MockTradeWithExitIsWin:
            def __init__(self, is_win, pnl):
                self.entry = type('obj', (), {
                    'entry_time': base_time
                })
                self.exit = type('obj', (), {
                    'is_win': is_win,
                    'is_loss': not is_win,
                    'pnl_points': pnl,
                    'exit_time': base_time
                })

        for i in range(5):
            trades.append(MockTradeWithIsWin(True, 5.0))
        for i in range(3):
            trades.append(MockTradeWithExitIsWin(False, -2.0))

        class MockTradeResult:
            def __init__(self, trades):
                self.trades = trades
                self.closed_trades = trades

        metrics = MetricsCalculator.calculate(MockTradeResult(trades))

        assert metrics.winning_trades == 5
        assert metrics.losing_trades == 3
        assert metrics.total_pnl_points == 5*5.0 + 3*(-2.0)  # 25 - 6 = 19

    def test_calculate_metrics_function(self, sample_trade_result):
        """Test the convenience calculate_metrics function."""
        metrics = calculate_metrics(sample_trade_result)
        
        assert isinstance(metrics, MetricsReport)
        assert metrics.total_trades == 15

    def test_calculate_metrics_with_timing_function(self, sample_trade_result):
        """Test the convenience calculate_metrics_with_timing function."""
        import time
        start = time.perf_counter()
        
        metrics = calculate_metrics_with_timing(sample_trade_result, start)
        
        assert metrics.execution_duration_ms > 0