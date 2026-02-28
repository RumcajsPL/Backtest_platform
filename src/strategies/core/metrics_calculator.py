"""Metrics Calculator — computes MetricsReport from a completed TradeResult.
Version: 2.0.0
"""
import time
from typing import List

from src.strategies.contracts.metrics_contracts import (
    MetricsReport,
    create_empty_metrics_report,
)

class MetricsCalculator:
    @staticmethod
    def calculate(trade_result, start_time=None) -> MetricsReport:
        execution_duration_ms = 0.0
        if start_time is not None:
            execution_duration_ms = (time.perf_counter() - start_time) * 1000
        closed_trades = trade_result.closed_trades

        if len(closed_trades) == 0:
            return create_empty_metrics_report(execution_duration_ms)

        total_trades = len(closed_trades)
        winning_trades, losing_trades = MetricsCalculator._calculate_win_loss_counts(closed_trades)
        win_rate = MetricsCalculator._calculate_win_rate(winning_trades, total_trades)
        total_pnl, expectancy, avg_pnl = MetricsCalculator._calculate_pnl_metrics(closed_trades, total_trades)
        profit_factor = MetricsCalculator._calculate_profit_factor(closed_trades)
        largest_win, largest_loss = MetricsCalculator._calculate_extremes(closed_trades)
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

    @staticmethod
    def _calculate_win_loss_counts(trades):        
        winning = sum(1 for t in trades if t.is_win)
        return winning, len(trades) - winning

    @staticmethod
    def _calculate_win_rate(winning_trades, total_trades):
        if total_trades == 0:
            return 0.0
        return round(winning_trades / total_trades * 100, 2)

    @staticmethod
    def _calculate_pnl_metrics(trades, total_trades):
        pnl_values = [t.pnl_points for t in trades]
        total_pnl  = sum(pnl_values)
        expectancy = total_pnl / total_trades if total_trades else 0.0
        return round(total_pnl, 2), round(expectancy, 2), round(expectancy, 2)

    @staticmethod
    def _calculate_profit_factor(trades):
        gross_profit = sum(t.pnl_points for t in trades if t.is_win)
        gross_loss   = abs(sum(t.pnl_points for t in trades if t.is_loss))
        if gross_loss == 0:
            return float('inf') if gross_profit > 0 else 0.0
        return round(gross_profit / gross_loss, 2)

    @staticmethod
    def _calculate_extremes(trades):
        largest_win  = max((t.pnl_points for t in trades if t.is_win),  default=0.0)
        largest_loss = min((t.pnl_points for t in trades if t.is_loss), default=0.0)
        return round(largest_win, 2), round(largest_loss, 2)

    @staticmethod
    def _calculate_max_drawdown(trades):
        if not trades:
            return 0.0
        sorted_trades = sorted(trades, key=lambda t: t.exit.exit_time)
        cumulative = peak = max_dd = 0.0
        for t in sorted_trades:
            cumulative += t.pnl_points
            if cumulative > peak:
                peak = cumulative
            dd = cumulative - peak
            if dd < max_dd:
                max_dd = dd
        return round(max_dd, 2)

    @staticmethod
    def _calculate_streaks(trades):
        if not trades:
            return 0, 0
        sorted_trades = sorted(trades, key=lambda t: t.exit.exit_time)
        max_win = max_loss = cur_win = cur_loss = 0
        for t in sorted_trades:
            if t.is_win:
                cur_win += 1; cur_loss = 0
                max_win = max(max_win, cur_win)
            else:
                cur_loss += 1; cur_win = 0
                max_loss = max(max_loss, cur_loss)
        return max_win, max_loss

    @staticmethod
    def _calculate_frequency(trades, total_trades):
        if total_trades == 0:
            return 0.0, 0.0
        first_entry = min(t.entry.entry_time for t in trades)
        last_exit   = max(t.exit.exit_time   for t in trades)
        delta = last_exit.date() - first_entry.date()
        calendar_days = max(1.0, float(delta.days + 1))
        trades_per_day  = total_trades / calendar_days
        trades_per_week = trades_per_day * 7
        return round(trades_per_day, 2), round(trades_per_week, 2)

def calculate_metrics(trade_result) -> MetricsReport:
    return MetricsCalculator.calculate(trade_result)

def calculate_metrics_with_timing(trade_result, start_time: float) -> MetricsReport:
    return MetricsCalculator.calculate(trade_result, start_time=start_time)