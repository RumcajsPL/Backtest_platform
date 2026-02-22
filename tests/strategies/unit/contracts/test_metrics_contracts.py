"""
Unit Tests for Metrics Contracts
==================================
Tests MetricsReport contract, validation, serialization, and helper functions.
"""

import pytest
import json
from datetime import datetime

from src.strategies.contracts.metrics_contracts import (
    MetricsReport,
    create_empty_metrics_report
)


class TestMetricsReport:
    """Tests for MetricsReport contract."""

    def test_valid_metrics_report(self):
        """Test creating a valid metrics report."""
        metrics = MetricsReport(
            total_trades=1151,
            winning_trades=194,
            losing_trades=957,
            win_rate=16.85,
            total_pnl_points=-2998.05,
            expectancy_points=-2.6,
            profit_factor=0.81,
            avg_pnl_points=-2.6,
            largest_win=159.08,
            largest_loss=-62.06,
            max_drawdown=-3383.85,
            losing_streak=41,
            winning_streak=5,
            trades_per_week=56.11,
            trades_per_day=12.24,
            execution_duration_ms=2765.23
        )

        assert metrics.total_trades == 1151
        assert metrics.winning_trades == 194
        assert metrics.losing_trades == 957
        assert metrics.win_rate == 16.85
        assert metrics.total_pnl_points == -2998.05
        assert metrics.expectancy_points == -2.6
        assert metrics.profit_factor == 0.81
        assert metrics.avg_pnl_points == -2.6
        assert metrics.largest_win == 159.08
        assert metrics.largest_loss == -62.06
        assert metrics.max_drawdown == -3383.85
        assert metrics.losing_streak == 41
        assert metrics.winning_streak == 5
        assert metrics.trades_per_week == 56.11
        assert metrics.trades_per_day == 12.24
        assert metrics.execution_duration_ms == 2765.23
        assert isinstance(metrics.execution_date, str)

    def test_validation_negative_total_trades(self):
        """Test that negative total_trades raises error."""
        with pytest.raises(ValueError, match="total_trades must be >= 0"):
            MetricsReport(
                total_trades=-1,
                winning_trades=0,
                losing_trades=0,
                win_rate=0.0,
                total_pnl_points=0.0,
                expectancy_points=0.0,
                profit_factor=0.0,
                avg_pnl_points=0.0,
                largest_win=0.0,
                largest_loss=0.0,
                max_drawdown=0.0,
                losing_streak=0,
                winning_streak=0,
                trades_per_week=0.0,
                trades_per_day=0.0,
                execution_duration_ms=0.0
            )

    def test_validation_negative_winning_trades(self):
        """Test that negative winning_trades raises error."""
        with pytest.raises(ValueError, match="winning_trades must be >= 0"):
            MetricsReport(
                total_trades=10,
                winning_trades=-1,
                losing_trades=0,
                win_rate=0.0,
                total_pnl_points=0.0,
                expectancy_points=0.0,
                profit_factor=0.0,
                avg_pnl_points=0.0,
                largest_win=0.0,
                largest_loss=0.0,
                max_drawdown=0.0,
                losing_streak=0,
                winning_streak=0,
                trades_per_week=0.0,
                trades_per_day=0.0,
                execution_duration_ms=0.0
            )

    def test_validation_negative_losing_trades(self):
        """Test that negative losing_trades raises error."""
        with pytest.raises(ValueError, match="losing_trades must be >= 0"):
            MetricsReport(
                total_trades=10,
                winning_trades=0,
                losing_trades=-1,
                win_rate=0.0,
                total_pnl_points=0.0,
                expectancy_points=0.0,
                profit_factor=0.0,
                avg_pnl_points=0.0,
                largest_win=0.0,
                largest_loss=0.0,
                max_drawdown=0.0,
                losing_streak=0,
                winning_streak=0,
                trades_per_week=0.0,
                trades_per_day=0.0,
                execution_duration_ms=0.0
            )

    @pytest.mark.parametrize("win_rate", [-10, 110])
    def test_validation_win_rate_range(self, win_rate):
        """Test that win_rate outside 0-100 raises error."""
        with pytest.raises(ValueError, match="win_rate must be between 0 and 100"):
            MetricsReport(
                total_trades=10,
                winning_trades=5,
                losing_trades=5,
                win_rate=win_rate,
                total_pnl_points=0.0,
                expectancy_points=0.0,
                profit_factor=0.0,
                avg_pnl_points=0.0,
                largest_win=0.0,
                largest_loss=0.0,
                max_drawdown=0.0,
                losing_streak=0,
                winning_streak=0,
                trades_per_week=0.0,
                trades_per_day=0.0,
                execution_duration_ms=0.0
            )

    def test_validation_negative_profit_factor(self):
        """Test that negative profit_factor raises error."""
        with pytest.raises(ValueError, match="profit_factor must be >= 0"):
            MetricsReport(
                total_trades=10,
                winning_trades=5,
                losing_trades=5,
                win_rate=50.0,
                total_pnl_points=0.0,
                expectancy_points=0.0,
                profit_factor=-1.0,
                avg_pnl_points=0.0,
                largest_win=0.0,
                largest_loss=0.0,
                max_drawdown=0.0,
                losing_streak=0,
                winning_streak=0,
                trades_per_week=0.0,
                trades_per_day=0.0,
                execution_duration_ms=0.0
            )

    def test_validation_negative_streaks(self):
        """Test that negative streaks raise error."""
        with pytest.raises(ValueError, match="losing_streak must be >= 0"):
            MetricsReport(
                total_trades=10,
                winning_trades=5,
                losing_trades=5,
                win_rate=50.0,
                total_pnl_points=0.0,
                expectancy_points=0.0,
                profit_factor=1.0,
                avg_pnl_points=0.0,
                largest_win=0.0,
                largest_loss=0.0,
                max_drawdown=0.0,
                losing_streak=-1,
                winning_streak=0,
                trades_per_week=0.0,
                trades_per_day=0.0,
                execution_duration_ms=0.0
            )

        with pytest.raises(ValueError, match="winning_streak must be >= 0"):
            MetricsReport(
                total_trades=10,
                winning_trades=5,
                losing_trades=5,
                win_rate=50.0,
                total_pnl_points=0.0,
                expectancy_points=0.0,
                profit_factor=1.0,
                avg_pnl_points=0.0,
                largest_win=0.0,
                largest_loss=0.0,
                max_drawdown=0.0,
                losing_streak=0,
                winning_streak=-1,
                trades_per_week=0.0,
                trades_per_day=0.0,
                execution_duration_ms=0.0
            )

    def test_validation_negative_trade_frequency(self):
        """Test that negative trade frequency raises error."""
        with pytest.raises(ValueError, match="trades_per_week must be >= 0"):
            MetricsReport(
                total_trades=10,
                winning_trades=5,
                losing_trades=5,
                win_rate=50.0,
                total_pnl_points=0.0,
                expectancy_points=0.0,
                profit_factor=1.0,
                avg_pnl_points=0.0,
                largest_win=0.0,
                largest_loss=0.0,
                max_drawdown=0.0,
                losing_streak=0,
                winning_streak=0,
                trades_per_week=-1.0,
                trades_per_day=0.0,
                execution_duration_ms=0.0
            )

    def test_validation_negative_execution_duration(self):
        """Test that negative execution duration raises error."""
        with pytest.raises(ValueError, match="execution_duration_ms must be >= 0"):
            MetricsReport(
                total_trades=10,
                winning_trades=5,
                losing_trades=5,
                win_rate=50.0,
                total_pnl_points=0.0,
                expectancy_points=0.0,
                profit_factor=1.0,
                avg_pnl_points=0.0,
                largest_win=0.0,
                largest_loss=0.0,
                max_drawdown=0.0,
                losing_streak=0,
                winning_streak=0,
                trades_per_week=0.0,
                trades_per_day=0.0,
                execution_duration_ms=-1.0
            )

    def test_gross_profit_property(self):
        """Test gross profit calculation."""
        # Profitable system
        profitable = MetricsReport(
            total_trades=100,
            winning_trades=60,
            losing_trades=40,
            win_rate=60.0,
            total_pnl_points=500.0,
            expectancy_points=5.0,
            profit_factor=2.0,
            avg_pnl_points=5.0,
            largest_win=50.0,
            largest_loss=-10.0,
            max_drawdown=-100.0,
            losing_streak=3,
            winning_streak=5,
            trades_per_week=10.0,
            trades_per_day=2.0,
            execution_duration_ms=100.0
        )
        assert profitable.gross_profit > 0

        # Losing system
        losing = MetricsReport(
            total_trades=100,
            winning_trades=40,
            losing_trades=60,
            win_rate=40.0,
            total_pnl_points=-500.0,
            expectancy_points=-5.0,
            profit_factor=0.5,
            avg_pnl_points=-5.0,
            largest_win=50.0,
            largest_loss=-10.0,
            max_drawdown=-100.0,
            losing_streak=3,
            winning_streak=5,
            trades_per_week=10.0,
            trades_per_day=2.0,
            execution_duration_ms=100.0
        )
        # Should not raise

    def test_gross_loss_property(self):
        """Test gross loss calculation."""
        metrics = MetricsReport(
            total_trades=100,
            winning_trades=60,
            losing_trades=40,
            win_rate=60.0,
            total_pnl_points=500.0,
            expectancy_points=5.0,
            profit_factor=2.0,
            avg_pnl_points=5.0,
            largest_win=50.0,
            largest_loss=-10.0,
            max_drawdown=-100.0,
            losing_streak=3,
            winning_streak=5,
            trades_per_week=10.0,
            trades_per_day=2.0,
            execution_duration_ms=100.0
        )
        # Should not raise
        assert metrics.gross_loss >= 0

    def test_is_profitable_property(self):
        """Test is_profitable property."""
        # Profitable
        profitable = MetricsReport(
            total_trades=100,
            winning_trades=60,
            losing_trades=40,
            win_rate=60.0,
            total_pnl_points=500.0,
            expectancy_points=5.0,
            profit_factor=2.0,
            avg_pnl_points=5.0,
            largest_win=50.0,
            largest_loss=-10.0,
            max_drawdown=-100.0,
            losing_streak=3,
            winning_streak=5,
            trades_per_week=10.0,
            trades_per_day=2.0,
            execution_duration_ms=100.0
        )
        assert profitable.is_profitable is True

        # Unprofitable
        unprofitable = MetricsReport(
            total_trades=100,
            winning_trades=60,
            losing_trades=40,
            win_rate=60.0,
            total_pnl_points=-500.0,
            expectancy_points=-5.0,
            profit_factor=2.0,
            avg_pnl_points=-5.0,
            largest_win=50.0,
            largest_loss=-10.0,
            max_drawdown=-100.0,
            losing_streak=3,
            winning_streak=5,
            trades_per_week=10.0,
            trades_per_day=2.0,
            execution_duration_ms=100.0
        )
        assert unprofitable.is_profitable is False

        # Zero P&L
        zero = MetricsReport(
            total_trades=100,
            winning_trades=60,
            losing_trades=40,
            win_rate=60.0,
            total_pnl_points=0.0,
            expectancy_points=0.0,
            profit_factor=1.0,
            avg_pnl_points=0.0,
            largest_win=50.0,
            largest_loss=-10.0,
            max_drawdown=-100.0,
            losing_streak=3,
            winning_streak=5,
            trades_per_week=10.0,
            trades_per_day=2.0,
            execution_duration_ms=100.0
        )
        assert zero.is_profitable is False

    def test_to_dict_format(self):
        """Test to_dict method matches backtester format."""
        metrics = MetricsReport(
            total_trades=1151,
            winning_trades=194,
            losing_trades=957,
            win_rate=16.85,
            total_pnl_points=-2998.05,
            expectancy_points=-2.6,
            profit_factor=0.81,
            avg_pnl_points=-2.6,
            largest_win=159.08,
            largest_loss=-62.06,
            max_drawdown=-3383.85,
            losing_streak=41,
            winning_streak=5,
            trades_per_week=56.11,
            trades_per_day=12.24,
            execution_duration_ms=2765.23
        )

        d = metrics.to_dict()

        # Check structure
        assert "simulation_results" in d
        assert "performance_metrics" in d["simulation_results"]
        assert "trade_summary" in d["simulation_results"]
        assert "execution_date" in d
        assert "execution_duration" in d

        # Check values
        perf = d["simulation_results"]["performance_metrics"]
        assert perf["total_trades"] == 1151
        assert perf["winning_trades"] == 194
        assert perf["losing_trades"] == 957
        assert perf["win_rate"] == 16.85
        assert perf["total_pnl_points"] == -2998.05
        assert perf["profit_factor"] == 0.81

        summary = d["simulation_results"]["trade_summary"]
        assert summary["trades_per_week"] == 56.11
        assert summary["trades_per_day"] == 12.24

        assert d["execution_duration"] == "2765.23ms"

    def test_to_json_serialization(self):
        """Test JSON serialization."""
        metrics = MetricsReport(
            total_trades=100,
            winning_trades=60,
            losing_trades=40,
            win_rate=60.0,
            total_pnl_points=500.0,
            expectancy_points=5.0,
            profit_factor=2.0,
            avg_pnl_points=5.0,
            largest_win=50.0,
            largest_loss=-10.0,
            max_drawdown=-100.0,
            losing_streak=3,
            winning_streak=5,
            trades_per_week=10.0,
            trades_per_day=2.0,
            execution_duration_ms=100.0
        )

        # Compact JSON
        json_str = metrics.to_json()
        assert isinstance(json_str, str)
        data = json.loads(json_str)
        assert data["simulation_results"]["performance_metrics"]["total_trades"] == 100

        # Pretty JSON
        json_pretty = metrics.to_json(indent=2)
        assert "\n" in json_pretty
        assert "  " in json_pretty

    def test_to_flat_dict(self):
        """Test flat dictionary format."""
        metrics = MetricsReport(
            total_trades=100,
            winning_trades=60,
            losing_trades=40,
            win_rate=60.0,
            total_pnl_points=500.0,
            expectancy_points=5.0,
            profit_factor=2.0,
            avg_pnl_points=5.0,
            largest_win=50.0,
            largest_loss=-10.0,
            max_drawdown=-100.0,
            losing_streak=3,
            winning_streak=5,
            trades_per_week=10.0,
            trades_per_day=2.0,
            execution_duration_ms=100.0
        )

        flat = metrics.to_flat_dict()

        assert flat["total_trades"] == 100
        assert flat["winning_trades"] == 60
        assert flat["losing_trades"] == 40
        assert flat["win_rate"] == 60.0
        assert flat["total_pnl_points"] == 500.0
        assert flat["expectancy_points"] == 5.0
        assert flat["profit_factor"] == 2.0
        assert flat["avg_pnl_points"] == 5.0
        assert flat["largest_win"] == 50.0
        assert flat["largest_loss"] == -10.0
        assert flat["max_drawdown"] == -100.0
        assert flat["losing_streak"] == 3
        assert flat["winning_streak"] == 5
        assert flat["trades_per_week"] == 10.0
        assert flat["trades_per_day"] == 2.0
        assert flat["execution_duration_ms"] == 100.0
        assert "execution_date" in flat

    def test_str_representation(self):
        """Test string representation."""
        metrics = MetricsReport(
            total_trades=100,
            winning_trades=60,
            losing_trades=40,
            win_rate=60.0,
            total_pnl_points=500.0,
            expectancy_points=5.0,
            profit_factor=2.0,
            avg_pnl_points=5.0,
            largest_win=50.0,
            largest_loss=-10.0,
            max_drawdown=-100.0,
            losing_streak=3,
            winning_streak=5,
            trades_per_week=10.0,
            trades_per_day=2.0,
            execution_duration_ms=100.0
        )

        s = str(metrics)
        assert "MetricsReport" in s
        assert "Trades: 100 (60W / 40L)" in s
        assert "Win Rate: 60.0%" in s
        assert "Total P&L: +500.00 points" in s
        assert "Profit Factor: 2.00" in s
        assert "Duration: 100.00ms" in s


class TestCreateEmptyMetricsReport:
    """Tests for create_empty_metrics_report helper."""

    def test_create_empty(self):
        """Test creating empty metrics report."""
        metrics = create_empty_metrics_report(execution_duration_ms=5.2)

        assert metrics.total_trades == 0
        assert metrics.winning_trades == 0
        assert metrics.losing_trades == 0
        assert metrics.win_rate == 0.0
        assert metrics.total_pnl_points == 0.0
        assert metrics.expectancy_points == 0.0
        assert metrics.profit_factor == 0.0
        assert metrics.avg_pnl_points == 0.0
        assert metrics.largest_win == 0.0
        assert metrics.largest_loss == 0.0
        assert metrics.max_drawdown == 0.0
        assert metrics.losing_streak == 0
        assert metrics.winning_streak == 0
        assert metrics.trades_per_week == 0.0
        assert metrics.trades_per_day == 0.0
        assert metrics.execution_duration_ms == 5.2

    def test_create_empty_default_duration(self):
        """Test creating empty report with default duration."""
        metrics = create_empty_metrics_report()

        assert metrics.total_trades == 0
        assert metrics.execution_duration_ms == 0.0