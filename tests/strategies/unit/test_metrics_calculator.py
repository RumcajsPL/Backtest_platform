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
        assert metrics.total_pnl_points == 45.0  # 10*5.0 + 5*(-1.0) = 45
        assert metrics.expectancy_points == 3.0  # 45.0 / 15 = 3.0
        
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
        winning, losing = MetricsCalculator._calculate_win_loss_counts(sample_trades)
        
        assert winning == 10
        assert losing == 5

    def test_calculate_win_rate(self):
        """Test win rate calculation."""
        # Normal case
        win_rate = MetricsCalculator._calculate_win_rate(10, 20)
        assert win_rate == 50.0
        
        # Edge cases
        assert MetricsCalculator._calculate_win_rate(0, 10) == 0.0
        assert MetricsCalculator._calculate_win_rate(10, 0) == 0.0
        assert MetricsCalculator._calculate_win_rate(0, 0) == 0.0

    def test_calculate_pnl_metrics(self, sample_trades):
        """Test P&L metrics calculation."""
        total_pnl, expectancy, avg_pnl = MetricsCalculator._calculate_pnl_metrics(
            sample_trades, 15
        )
        
        assert total_pnl == 45.0
        assert expectancy == 3.0
        assert avg_pnl == 3.0

    def test_calculate_profit_factor(self, sample_trades):
        """Test profit factor calculation."""
        profit_factor = MetricsCalculator._calculate_profit_factor(sample_trades)
        
        # 10 wins of 5.0 = 50 gross profit
        # 5 losses of 1.0 = 5 gross loss
        assert profit_factor == 10.0

    def test_profit_factor_edge_cases(self):
        """Test profit factor edge cases."""
        # No trades
        assert MetricsCalculator._calculate_profit_factor([]) == 0.0

        # Only winning trades - need to create proper Trade objects with exit
        trades = []
        base_time = pd.Timestamp("2025-01-01")
        
        for i in range(5):
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
                exit_price=110.0,  # Win of 10.0
                exit_reason=ExitReason.TAKE_PROFIT
            )
            trades.append(Trade(entry=entry, exit=exit))
        
        # All winning trades should return infinity
        assert MetricsCalculator._calculate_profit_factor(trades) == float('inf')

        # Only losing trades
        trades = []
        for i in range(3):
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
                exit_price=95.0,  # Loss of -5.0
                exit_reason=ExitReason.STOP_LOSS
            )
            trades.append(Trade(entry=entry, exit=exit))
        
        assert MetricsCalculator._calculate_profit_factor(trades) == 0.0

    def test_calculate_extremes(self, sample_trades):
        """Test largest win/loss calculation."""
        largest_win, largest_loss = MetricsCalculator._calculate_extremes(sample_trades)
        
        assert largest_win == 5.0
        assert largest_loss == -1.0

    def test_calculate_max_drawdown(self):
        """Test max drawdown calculation."""
        # Create trades with known P&L sequence using proper TradeExit.create()
        trades = []
        base_time = pd.Timestamp("2025-01-01")
        
        # Sequence: +10, +5, -8, +3, -5, +2
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
        
        max_dd = MetricsCalculator._calculate_max_drawdown(trades)
        
        # Expected max drawdown: after cumulative 15, then -10 = -10.0
        assert max_dd == -10.0

    def test_calculate_max_drawdown_empty(self):
        """Test max drawdown with empty trades list."""
        assert MetricsCalculator._calculate_max_drawdown([]) == 0.0

    def test_calculate_streaks(self):
        """Test winning/losing streak calculation using proper TradeExit.create()."""
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
            exit_price = 105.0 if is_win else 99.0
            exit_reason = ExitReason.TAKE_PROFIT if is_win else ExitReason.STOP_LOSS
            exit = TradeExit.create(
                entry=entry,
                exit_time=entry.entry_time + timedelta(hours=1),
                exit_price=exit_price,
                exit_reason=exit_reason
            )
            trades.append(Trade(entry=entry, exit=exit))
        
        win_streak, loss_streak = MetricsCalculator._calculate_streaks(trades)
        
        assert win_streak == 2  # First two wins
        assert loss_streak == 3  # Three losses in a row

    def test_calculate_streaks_empty(self):
        """Test streaks with empty trades."""
        win_streak, loss_streak = MetricsCalculator._calculate_streaks([])
        assert win_streak == 0
        assert loss_streak == 0

    def test_calculate_frequency(self):
        """Test trade frequency calculation using proper TradeExit.create()."""
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
        
        trades_per_day, trades_per_week = MetricsCalculator._calculate_frequency(trades, 20)
        
        assert trades_per_day == 2.0  # 20 trades / 10 days
        assert trades_per_week == 14.0  # 2.0 * 7

    def test_calculate_frequency_single_day(self):
        """Test frequency with all trades on same day using proper TradeExit.create()."""
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
        
        trades_per_day, trades_per_week = MetricsCalculator._calculate_frequency(trades, 10)
        
        assert trades_per_day == 10.0  # 10 trades / 1 day
        assert trades_per_week == 70.0

    def test_calculate_frequency_no_trades(self):
        """Test frequency with no trades."""
        trades_per_day, trades_per_week = MetricsCalculator._calculate_frequency([], 0)
        
        assert trades_per_day == 0.0
        assert trades_per_week == 0.0

    def test_calculate_with_mixed_trade_properties(self):
        """Test metrics with real Trade objects (no mocks needed - implementation uses contract properties)."""
        # Create real Trade objects, not mocks
        trades = []
        base_time = pd.Timestamp("2025-01-01")

        # Create winning trades
        for i in range(5):
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
                exit_price=105.0,  # Win of 5.0
                exit_reason=ExitReason.TAKE_PROFIT
            )
            trades.append(Trade(entry=entry, exit=exit))

        # Create losing trades
        for i in range(3):
            entry = TradeEntry(
                entry_id=f"E{i+5}",
                entry_time=base_time + timedelta(days=i+5),
                direction=TradeDirection.LONG,
                entry_price=100.0,
                stop_loss=99.0,
                take_profit=105.0
            )
            exit = TradeExit.create(
                entry=entry,
                exit_time=entry.entry_time + timedelta(hours=1),
                exit_price=98.0,  # Loss of -2.0
                exit_reason=ExitReason.STOP_LOSS
            )
            trades.append(Trade(entry=entry, exit=exit))

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