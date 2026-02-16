"""
Metrics Calculator - High-Performance Metrics from TradeResult

Session 13 - MetricsCalculator
Version: 1.1.0 (Production)

Calculates performance metrics from TradeResult contract.
Optimized for speed (memory-only, vectorized operations).

Design Principles:
- Single Responsibility: Only calculates metrics
- Performance-Driven: Vectorized (numpy), <10ms target
- Explicit Contracts: TradeResult → MetricsReport
- Type Safety: All methods typed
- Production-Ready: Memory-only, no file I/O

Trade Frequency Notes:
- Uses calendar days between first entry and last exit of closed trades
- Adds 1 day to include the full range (e.g., trades on Day1 and Day3 span 3 days)
- Single trade = 1.0 trades/day (intuitive for human reading)
- All float metrics rounded to 2 decimal places for consistency
- Edge cases (zero trades) handled gracefully
"""
import time
from typing import List, Tuple
import numpy as np

from src.strategies.contracts.trade_contracts import TradeResult, Trade
from src.strategies.contracts.metrics_contracts import (
    MetricsReport,
    create_empty_metrics_report
)


class MetricsCalculator:
    """
    High-performance metrics calculator.
    
    Converts TradeResult to MetricsReport with 14 essential metrics:
    - Performance metrics (11): counts, P&L, profit factor, extremes, drawdown, streaks
    - Trade summary (2): frequency (per day/week)
    - Metadata (1): execution duration
    
    Design:
    - Static methods (no state)
    - Vectorized operations (numpy)
    - Single pass where possible
    - Memory-only (no I/O)
    
    Performance Target: <10ms for 1000 trades
    
    Usage:
        result: TradeResult = simulator.simulate_trades(...)
        metrics: MetricsReport = MetricsCalculator.calculate(result)
        
        # Pass to backtester (memory-only)
        backtester.consume(metrics)
    """
    
    @staticmethod
    def calculate(
        trade_result: TradeResult,
        start_time: float | None = None
    ) -> MetricsReport:
        """
        Calculate all metrics from TradeResult.
        
        Args:
            trade_result: Simulation results (in memory)
            start_time: Execution start time (from time.perf_counter())
        
        Returns:
            MetricsReport with all metrics
        
        Performance: O(n) single pass + O(n log n) sort for streaks
        
        Example:
            start = time.perf_counter()
            result = simulator.simulate_trades(...)
            metrics = MetricsCalculator.calculate(result, start_time=start)
            print(f"Win Rate: {metrics.win_rate:.1f}%")
        """
        # Calculate execution duration
        execution_duration_ms = 0.0
        if start_time is not None:
            execution_duration_ms = (time.perf_counter() - start_time) * 1000
        
        # Handle empty trades
        closed_trades = trade_result.closed_trades
        if len(closed_trades) == 0:
            return create_empty_metrics_report(execution_duration_ms)
        
        # Calculate all metrics
        total_trades = len(closed_trades)
        winning_trades, losing_trades = MetricsCalculator._calculate_win_loss_counts(closed_trades)
        win_rate = MetricsCalculator._calculate_win_rate(winning_trades, total_trades)
        
        total_pnl, expectancy, avg_pnl = MetricsCalculator._calculate_pnl_metrics(closed_trades, total_trades)
        profit_factor = MetricsCalculator._calculate_profit_factor(closed_trades)
        largest_win, largest_loss = MetricsCalculator._calculate_extremes(closed_trades, winning_trades, losing_trades)
        max_drawdown = MetricsCalculator._calculate_max_drawdown(closed_trades)
        winning_streak, losing_streak = MetricsCalculator._calculate_streaks(closed_trades)
        trades_per_day, trades_per_week = MetricsCalculator._calculate_frequency(closed_trades, total_trades)
        
        return MetricsReport(
            total_trades=total_trades,
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            win_rate=win_rate,
            total_pnl_points=total_pnl,
            expectancy_points=expectancy,
            profit_factor=profit_factor,
            avg_pnl_points=avg_pnl,
            largest_win=largest_win,
            largest_loss=largest_loss,
            max_drawdown=max_drawdown,
            losing_streak=losing_streak,
            winning_streak=winning_streak,
            trades_per_week=trades_per_week,
            trades_per_day=trades_per_day,
            execution_duration_ms=execution_duration_ms,
        )
    
    # ========================================================================
    # CALCULATION METHODS (Private)
    # ========================================================================
    
    @staticmethod
    def _calculate_win_loss_counts(trades: List[Trade]) -> Tuple[int, int]:
        """
        Calculate winning and losing trade counts.
        
        Args:
            trades: List of closed trades
        
        Returns:
            (winning_trades, losing_trades)
        """
        winning_trades = sum(1 for t in trades if t.is_win)
        losing_trades = len(trades) - winning_trades
        return winning_trades, losing_trades
    
    @staticmethod
    def _calculate_win_rate(winning_trades: int, total_trades: int) -> float:
        """
        Calculate win rate as percentage.
        
        Args:
            winning_trades: Number of wins
            total_trades: Total trades
        
        Returns:
            Win rate (0-100) rounded to 2 decimals
        """
        if total_trades == 0:
            return 0.0
        return round((winning_trades / total_trades * 100), 2)
    
    @staticmethod
    def _calculate_pnl_metrics(trades: List[Trade], total_trades: int) -> Tuple[float, float, float]:
        """
        Calculate P&L metrics.
        
        Args:
            trades: List of closed trades
            total_trades: Total trades
        
        Returns:
            (total_pnl, expectancy, avg_pnl) all rounded to 2 decimals
        """
        total_pnl = sum(t.pnl_points for t in trades)
        expectancy = total_pnl / total_trades if total_trades > 0 else 0.0
        avg_pnl = expectancy  # Same as expectancy
        
        return round(total_pnl, 2), round(expectancy, 2), round(avg_pnl, 2)
    
    @staticmethod
    def _calculate_profit_factor(trades: List[Trade]) -> float:
        """
        Calculate profit factor (gross_profit / gross_loss).
        
        Args:
            trades: List of closed trades
        
        Returns:
            Profit factor rounded to 2 decimals
            - 0.0 = all losses
            - inf = all wins
            - >1 = profitable strategy
        """
        gross_profit = sum(t.pnl_points for t in trades if t.is_win)
        gross_loss = abs(sum(t.pnl_points for t in trades if t.is_loss))
        
        if gross_loss == 0:
            # All winning trades
            return float('inf') if gross_profit > 0 else 0.0
        
        return round(gross_profit / gross_loss, 2)
    
    @staticmethod
    def _calculate_extremes(
        trades: List[Trade],
        winning_trades: int,
        losing_trades: int
    ) -> Tuple[float, float]:
        """
        Calculate largest win and loss.
        
        Args:
            trades: List of closed trades
            winning_trades: Number of wins
            losing_trades: Number of losses
        
        Returns:
            (largest_win, largest_loss) both rounded to 2 decimals
        """
        largest_win = max((t.pnl_points for t in trades if t.is_win), default=0.0)
        largest_loss = min((t.pnl_points for t in trades if t.is_loss), default=0.0)
        
        return round(largest_win, 2), round(largest_loss, 2)
    
    @staticmethod
    def _calculate_max_drawdown(trades: List[Trade]) -> float:
        """
        Calculate maximum drawdown using iterative algorithm.
        
        Algorithm:
        1. Sort trades by exit time
        2. Track cumulative P&L starting from 0
        3. Track peak equity (highest cumulative value)
        4. Calculate drawdown = current equity - peak
        5. Track worst (most negative) drawdown
        
        Args:
            trades: List of closed trades
        
        Returns:
            Max drawdown rounded to 2 decimals (negative value)
        
        Performance: O(n log n) for sort + O(n) for iteration
        """
        if len(trades) == 0:
            return 0.0
        
        # Sort by exit time
        sorted_trades = sorted(trades, key=lambda t: t.exit.exit_time)
        
        cumulative = 0.0
        peak = 0.0  # Start at 0 (initial equity)
        max_drawdown = 0.0
        
        for trade in sorted_trades:
            cumulative += trade.pnl_points
            if cumulative > peak:
                peak = cumulative
            drawdown = cumulative - peak  # Will be negative or zero
            if drawdown < max_drawdown:
                max_drawdown = drawdown
        
        return round(max_drawdown, 2)
    
    @staticmethod
    def _calculate_streaks(trades: List[Trade]) -> Tuple[int, int]:
        """
        Calculate longest winning and losing streaks.
        
        Args:
            trades: List of closed trades
        
        Returns:
            (winning_streak, losing_streak)
        
        Performance: O(n log n) for sort + O(n) for iteration
        """
        if len(trades) == 0:
            return 0, 0
        
        # Sort by exit time
        sorted_trades = sorted(trades, key=lambda t: t.exit.exit_time)
        
        max_win_streak = 0
        max_loss_streak = 0
        current_win_streak = 0
        current_loss_streak = 0
        
        for trade in sorted_trades:
            if trade.is_win:
                current_win_streak += 1
                current_loss_streak = 0
                max_win_streak = max(max_win_streak, current_win_streak)
            else:
                current_loss_streak += 1
                current_win_streak = 0
                max_loss_streak = max(max_loss_streak, current_loss_streak)
        
        return max_win_streak, max_loss_streak
    
    @staticmethod
    def _calculate_frequency(trades: List[Trade], total_trades: int) -> Tuple[float, float]:
        """
        Calculate trade frequency (per day and per week).
        
        Uses calendar days between first entry and last exit of closed trades,
        counting full days (adds 1 to include both start and end days).
        
        Args:
            trades: List of closed trades
            total_trades: Total trades
        
        Returns:
            (trades_per_day, trades_per_week) rounded to 2 decimals
        """
        if total_trades == 0:
            return 0.0, 0.0
        
        # Get time range from first entry to last exit
        first_entry = min(t.entry.entry_time for t in trades)
        last_exit = max(t.exit.exit_time for t in trades)
        
        # Calculate calendar days (count full days between dates + 1)
        delta = last_exit.date() - first_entry.date()
        calendar_days = delta.days + 1  # +1 to include both start and end days
        
        # For same-day trading, calendar_days will be 1
        calendar_days = max(1.0, float(calendar_days))
        
        # Calculate frequencies
        trades_per_day = total_trades / calendar_days
        trades_per_week = trades_per_day * 7
        
        # Round to 2 decimals
        return round(trades_per_day, 2), round(trades_per_week, 2)


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def calculate_metrics(trade_result: TradeResult) -> MetricsReport:
    """
    Convenience function for metrics calculation.
    
    Args:
        trade_result: Simulation results
    
    Returns:
        MetricsReport with all metrics
    
    Example:
        result = simulator.simulate_trades(...)
        metrics = calculate_metrics(result)
    """
    return MetricsCalculator.calculate(trade_result)


def calculate_metrics_with_timing(trade_result: TradeResult, start_time: float) -> MetricsReport:
    """
    Calculate metrics with execution timing.
    
    Args:
        trade_result: Simulation results
        start_time: Execution start (from time.perf_counter())
    
    Returns:
        MetricsReport with execution_duration_ms
    
    Example:
        start = time.perf_counter()
        result = simulator.simulate_trades(...)
        metrics = calculate_metrics_with_timing(result, start)
        print(f"Duration: {metrics.execution_duration_ms:.2f}ms")
    """
    return MetricsCalculator.calculate(trade_result, start_time=start_time)