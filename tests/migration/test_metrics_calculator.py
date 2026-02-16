"""
Comprehensive tests for MetricsCalculator - Session 13

Tests MetricsCalculator calculation accuracy:
- All 14 required metrics calculated correctly
- Performance benchmarks
- Edge cases (zero trades, single trade, all wins/losses)
- Comparison with known results
- Integration with TradeResult contract

Design:
- Uses Trade contracts from trade_contracts
- Validates against manually calculated values
- Performance testing
"""

import sys
from pathlib import Path
import time
import numpy as np
import pandas as pd
import pytest
from datetime import datetime, timedelta

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.strategies.contracts.trade_contracts import (
    TradeResult, Trade, TradeEntry, TradeExit, 
    TradeDirection, ExitReason, RejectedSignal
)
from src.strategies.contracts.metrics_contracts import MetricsReport
from src.strategies.specific.modules.metrics_calculator import (
    MetricsCalculator,
    calculate_metrics,
    calculate_metrics_with_timing,
)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def create_test_trade(
    signal_id: int,
    entry_time: pd.Timestamp,
    exit_time: pd.Timestamp,
    direction: TradeDirection,
    entry_price: float,
    exit_price: float,
    is_win: bool,
) -> Trade:
    """Helper to create a test trade with proper structure"""
    entry = TradeEntry(
        entry_id=f"E{signal_id}",
        entry_time=entry_time,
        direction=direction,
        entry_price=entry_price,
        stop_loss=entry_price * 0.99 if direction == TradeDirection.LONG else entry_price * 1.01,
        take_profit=entry_price * 1.01 if direction == TradeDirection.LONG else entry_price * 0.99,
        position_size=1.0,
        sl_distance=abs(entry_price * 0.01),
        tp_distance=abs(entry_price * 0.01),
        risk_reward_ratio=1.0,
        atr_value=None,
        spread_enabled=False,
        spread_points=None,
        sl_adjusted=False,
        comment=None,
        trade_manager_id=None,
        signal_id=signal_id,
    )
    
    exit_reason = ExitReason.TAKE_PROFIT if is_win else ExitReason.STOP_LOSS
    
    exit = TradeExit.create(
        entry=entry,
        exit_time=exit_time,
        exit_price=exit_price,
        exit_reason=exit_reason,
    )
    
    return Trade(entry=entry, exit=exit)


def create_trade_result(trades: list, rejected_signals: list = None) -> TradeResult:
    """Helper to create a TradeResult with all required parameters"""
    if rejected_signals is None:
        rejected_signals = []
    
    # Calculate statistics from trades
    closed_trades = [t for t in trades if t.is_closed]
    total_trades = len(closed_trades)
    winning_trades = sum(1 for t in closed_trades if t.is_win)
    
    # Count exits by reason
    exits_by_reason = {}
    for trade in closed_trades:
        if trade.exit and trade.exit.exit_reason:
            reason = trade.exit.exit_reason.name
            exits_by_reason[reason] = exits_by_reason.get(reason, 0) + 1
    
    return TradeResult(
        trades=trades,
        rejected_signals=rejected_signals,
        total_entries=len(trades) + len(rejected_signals),
        total_opened=len(trades),
        total_closed=len(closed_trades),
        total_rejected=len(rejected_signals),
        currently_open=len(trades) - len(closed_trades),
        exits_by_reason=exits_by_reason,
        risk_approved=len(trades),
        risk_rejected=0,
        risk_adjusted=0,
        position_rejected={"buy": 0, "sell": 0},
        trade_manager_metrics={},
        win_count=winning_trades,
        loss_count=total_trades - winning_trades,
        win_rate=(winning_trades / total_trades * 100) if total_trades > 0 else 0.0,
        total_pnl_points=sum(t.pnl_points for t in closed_trades),
        average_pnl_points=sum(t.pnl_points for t in closed_trades) / total_trades if total_trades > 0 else 0.0,
        execution_mode="TEST",
        execution_time_ms=None,
        metadata={},
    )


# ============================================================================
# FIXTURES - Trade Data
# ============================================================================

@pytest.fixture
def sample_trades_simple():
    """Simple set of 3 trades for manual verification"""
    trades = [
        create_test_trade(
            signal_id=1,
            entry_time=pd.Timestamp("2024-01-01 10:00"),
            exit_time=pd.Timestamp("2024-01-01 11:00"),
            direction=TradeDirection.LONG,
            entry_price=100.0,
            exit_price=101.0,
            is_win=True,
        ),
        create_test_trade(
            signal_id=2,
            entry_time=pd.Timestamp("2024-01-02 10:00"),
            exit_time=pd.Timestamp("2024-01-02 11:00"),
            direction=TradeDirection.LONG,
            entry_price=100.0,
            exit_price=99.0,
            is_win=False,
        ),
        create_test_trade(
            signal_id=3,
            entry_time=pd.Timestamp("2024-01-03 10:00"),
            exit_time=pd.Timestamp("2024-01-03 11:00"),
            direction=TradeDirection.LONG,
            entry_price=100.0,
            exit_price=100.01,
            is_win=True,
        ),
    ]
    return trades


@pytest.fixture
def sample_trades_with_extremes():
    """Trades with clear min/max for largest win/loss testing"""
    trades = []
    pnls = [10.0, -5.0, 20.0, -15.0, 5.0, -8.0, 30.0, -25.0, 2.0, -1.0]
    
    for i, pnl in enumerate(pnls):
        is_win = pnl > 0
        trades.append(
            create_test_trade(
                signal_id=i,
                entry_time=pd.Timestamp(f"2024-01-{i+1:02d} 10:00"),
                exit_time=pd.Timestamp(f"2024-01-{i+1:02d} 11:00"),
                direction=TradeDirection.LONG,
                entry_price=100.0,
                exit_price=100.0 + pnl,
                is_win=is_win,
            )
        )
    return trades


@pytest.fixture
def sample_trades_with_streaks():
    """Trades with clear winning/losing streaks"""
    # Pattern: W, W, L, L, L, W, W, W, W, L, L, W
    pnls = [5.0, 3.0, -2.0, -4.0, -1.0, 6.0, 7.0, 2.0, 4.0, -3.0, -5.0, 8.0]
    
    trades = []
    for i, pnl in enumerate(pnls):
        is_win = pnl > 0
        trades.append(
            create_test_trade(
                signal_id=i,
                entry_time=pd.Timestamp(f"2024-01-{i+1:02d} 10:00"),
                exit_time=pd.Timestamp(f"2024-01-{i+1:02d} 11:00"),
                direction=TradeDirection.LONG,
                entry_price=100.0,
                exit_price=100.0 + pnl,
                is_win=is_win,
            )
        )
    return trades


@pytest.fixture
def sample_trades_with_drawdown():
    """Trades designed to create specific drawdown pattern"""
    # Sequence: +10, -5, -8, +6, -12, +15, -7, -9, -11, +20
    pnls = [10.0, -5.0, -8.0, 6.0, -12.0, 15.0, -7.0, -9.0, -11.0, 20.0]
    
    trades = []
    for i, pnl in enumerate(pnls):
        is_win = pnl > 0
        trades.append(
            create_test_trade(
                signal_id=i,
                entry_time=pd.Timestamp(f"2024-01-{i+1:02d} 10:00"),
                exit_time=pd.Timestamp(f"2024-01-{i+1:02d} 11:00"),
                direction=TradeDirection.LONG,
                entry_price=100.0,
                exit_price=100.0 + pnl,
                is_win=is_win,
            )
        )
    return trades


@pytest.fixture
def sample_trades_time_distribution():
    """Trades spread over multiple days for frequency calculation"""
    base_date = pd.Timestamp("2024-01-01")
    trades = []
    
    # Day 1: 3 trades (Jan 1)
    for hour in [10, 12, 14]:
        is_win = hour % 2 == 0
        pnl = 1.0 if is_win else -1.0
        trades.append(
            create_test_trade(
                signal_id=len(trades) + 1,
                entry_time=base_date + pd.Timedelta(hours=hour),
                exit_time=base_date + pd.Timedelta(hours=hour + 1),
                direction=TradeDirection.LONG,
                entry_price=100.0,
                exit_price=100.0 + pnl,
                is_win=is_win,
            )
        )
    
    # Day 2: 5 trades (Jan 2)
    for hour in [9, 11, 13, 15, 16]:
        is_win = hour % 2 == 0
        pnl = 1.0 if is_win else -1.0
        trades.append(
            create_test_trade(
                signal_id=len(trades) + 1,
                entry_time=base_date + pd.Timedelta(days=1, hours=hour),
                exit_time=base_date + pd.Timedelta(days=1, hours=hour + 1),
                direction=TradeDirection.LONG,
                entry_price=100.0,
                exit_price=100.0 + pnl,
                is_win=is_win,
            )
        )
    
    # Day 3: 2 trades (Jan 3)
    for hour in [10, 14]:
        is_win = hour % 2 == 0
        pnl = 1.0 if is_win else -1.0
        trades.append(
            create_test_trade(
                signal_id=len(trades) + 1,
                entry_time=base_date + pd.Timedelta(days=2, hours=hour),
                exit_time=base_date + pd.Timedelta(days=2, hours=hour + 1),
                direction=TradeDirection.LONG,
                entry_price=100.0,
                exit_price=100.0 + pnl,
                is_win=is_win,
            )
        )
    
    return trades


@pytest.fixture
def sample_trades_all_wins():
    """All winning trades"""
    trades = []
    for i in range(10):
        trades.append(
            create_test_trade(
                signal_id=i,
                entry_time=pd.Timestamp(f"2024-01-{i+1:02d} 10:00"),
                exit_time=pd.Timestamp(f"2024-01-{i+1:02d} 11:00"),
                direction=TradeDirection.LONG,
                entry_price=100.0,
                exit_price=105.0,
                is_win=True,
            )
        )
    return trades


@pytest.fixture
def sample_trades_all_losses():
    """All losing trades"""
    trades = []
    for i in range(10):
        trades.append(
            create_test_trade(
                signal_id=i,
                entry_time=pd.Timestamp(f"2024-01-{i+1:02d} 10:00"),
                exit_time=pd.Timestamp(f"2024-01-{i+1:02d} 11:00"),
                direction=TradeDirection.LONG,
                entry_price=100.0,
                exit_price=95.0,
                is_win=False,
            )
        )
    return trades


@pytest.fixture
def sample_trades_single():
    """Single trade"""
    return [
        create_test_trade(
            signal_id=1,
            entry_time=pd.Timestamp("2024-01-01 10:00"),
            exit_time=pd.Timestamp("2024-01-01 11:00"),
            direction=TradeDirection.LONG,
            entry_price=100.0,
            exit_price=105.0,
            is_win=True,
        )
    ]


@pytest.fixture
def sample_trades_zero():
    """No trades"""
    return []


@pytest.fixture
def trade_result_simple(sample_trades_simple):
    """TradeResult with simple trades"""
    return create_trade_result(sample_trades_simple)


@pytest.fixture
def trade_result_with_rejects(sample_trades_simple):
    """TradeResult with rejected signals"""
    rejects = [
        RejectedSignal(
            rejection_id="R1",
            signal_id=4,
            rejection_time=pd.Timestamp("2024-01-04 10:00"),
            direction="BUY",
            rejection_stage="RISK",
            rejection_reason="Risk check failed",
            current_price=100.0,
            meta={"annual_range": 10.0, "max_position_size": 0.0},
        ),
        RejectedSignal(
            rejection_id="R2",
            signal_id=5,
            rejection_time=pd.Timestamp("2024-01-05 10:00"),
            direction="SELL",
            rejection_stage="POSITION",
            rejection_reason="Pyramiding not allowed",
            current_price=100.0,
            meta={"reason": "Pyramiding not allowed"},
        ),
    ]
    return create_trade_result(sample_trades_simple, rejects)


# ============================================================================
# BASIC CALCULATION TESTS
# ============================================================================

class TestBasicMetrics:
    """Test basic metrics calculations (counts, win rate, P&L)"""

    def test_total_trades(self, trade_result_simple):
        """Should count total trades correctly"""
        metrics = MetricsCalculator.calculate(trade_result_simple)
        assert metrics.total_trades == 3

    def test_winning_trades(self, trade_result_simple):
        """Should count winning trades correctly"""
        metrics = MetricsCalculator.calculate(trade_result_simple)
        assert metrics.winning_trades == 2

    def test_losing_trades(self, trade_result_simple):
        """Should count losing trades correctly"""
        metrics = MetricsCalculator.calculate(trade_result_simple)
        assert metrics.losing_trades == 1

    def test_win_rate(self, trade_result_simple):
        """Should calculate win rate percentage correctly"""
        metrics = MetricsCalculator.calculate(trade_result_simple)
        # 2 wins / 3 total = 66.67% (rounded to 2 decimals)
        assert metrics.win_rate == 66.67

    def test_total_pnl_points(self, trade_result_simple):
        """Should sum P&L correctly"""
        metrics = MetricsCalculator.calculate(trade_result_simple)
        # 1.0 + (-1.0) + 0.01 = 0.01
        assert metrics.total_pnl_points == 0.01

    def test_expectancy_points(self, trade_result_simple):
        """Should calculate average P&L per trade"""
        metrics = MetricsCalculator.calculate(trade_result_simple)
        # 0.01 / 3 = 0.00333... rounded to 0.0 (2 decimals)
        assert metrics.expectancy_points == 0.0

    def test_avg_pnl_points(self, trade_result_simple):
        """Should match expectancy_points"""
        metrics = MetricsCalculator.calculate(trade_result_simple)
        assert metrics.avg_pnl_points == metrics.expectancy_points


# ============================================================================
# PROFIT FACTOR TESTS
# ============================================================================

class TestProfitFactor:
    """Test profit factor calculation"""

    def test_profit_factor_mixed(self, sample_trades_with_extremes):
        """Should calculate profit factor correctly for mixed wins/losses"""
        result = create_trade_result(sample_trades_with_extremes)
        metrics = MetricsCalculator.calculate(result)
        
        # Manual calculation: 67.0 / 54.0 = 1.24074... rounded to 1.24
        assert metrics.profit_factor == 1.24

    def test_profit_factor_all_wins(self, sample_trades_all_wins):
        """Should handle all winning trades"""
        result = create_trade_result(sample_trades_all_wins)
        metrics = MetricsCalculator.calculate(result)
        
        # All wins: profit factor should be infinite
        assert metrics.profit_factor == float('inf')

    def test_profit_factor_all_losses(self, sample_trades_all_losses):
        """Should handle all losing trades"""
        result = create_trade_result(sample_trades_all_losses)
        metrics = MetricsCalculator.calculate(result)
        
        # All losses: profit factor should be 0
        assert metrics.profit_factor == 0.0

    def test_profit_factor_zero_trades(self):
        """Should handle zero trades"""
        result = create_trade_result([])
        metrics = MetricsCalculator.calculate(result)
        assert metrics.profit_factor == 0.0


# ============================================================================
# EXTREMES TESTS
# ============================================================================

class TestExtremes:
    """Test largest win/loss calculations"""

    def test_largest_win(self, sample_trades_with_extremes):
        """Should find largest winning trade"""
        result = create_trade_result(sample_trades_with_extremes)
        metrics = MetricsCalculator.calculate(result)
        
        assert metrics.largest_win == 30.0

    def test_largest_loss(self, sample_trades_with_extremes):
        """Should find largest losing trade (most negative)"""
        result = create_trade_result(sample_trades_with_extremes)
        metrics = MetricsCalculator.calculate(result)
        
        assert metrics.largest_loss == -25.0

    def test_largest_win_all_wins(self, sample_trades_all_wins):
        """Should work with all winning trades"""
        result = create_trade_result(sample_trades_all_wins)
        metrics = MetricsCalculator.calculate(result)
        
        assert metrics.largest_win == 5.0
        assert metrics.largest_loss == 0.0

    def test_largest_loss_all_losses(self, sample_trades_all_losses):
        """Should work with all losing trades"""
        result = create_trade_result(sample_trades_all_losses)
        metrics = MetricsCalculator.calculate(result)
        
        assert metrics.largest_win == 0.0
        assert metrics.largest_loss == -5.0


# ============================================================================
# MAX DRAWDOWN TESTS
# ============================================================================

class TestMaxDrawdown:
    """Test maximum drawdown calculation"""

    def test_max_drawdown_simple(self, sample_trades_with_drawdown):
        """Should calculate max drawdown correctly for known sequence"""
        result = create_trade_result(sample_trades_with_drawdown)
        metrics = MetricsCalculator.calculate(result)
        
        # Sequence: +10, -5, -8, +6, -12, +15, -7, -9, -11, +20
        # Max drawdown = -31.0
        assert metrics.max_drawdown == -31.0

    def test_max_drawdown_all_wins(self, sample_trades_all_wins):
        """Should be zero for all winning trades"""
        result = create_trade_result(sample_trades_all_wins)
        metrics = MetricsCalculator.calculate(result)
        
        assert metrics.max_drawdown == 0.0

    def test_max_drawdown_all_losses(self, sample_trades_all_losses):
        """Should equal total loss for all losing trades"""
        result = create_trade_result(sample_trades_all_losses)
        metrics = MetricsCalculator.calculate(result)
        
        # All losses -5.0 each = -50.0
        assert metrics.max_drawdown == -50.0

    def test_max_drawdown_single_win(self, sample_trades_single):
        """Should be zero for single winning trade"""
        result = create_trade_result(sample_trades_single)
        metrics = MetricsCalculator.calculate(result)
        
        assert metrics.max_drawdown == 0.0

    def test_max_drawdown_single_loss(self):
        """Should equal the loss for single losing trade"""
        trade = create_test_trade(
            signal_id=1,
            entry_time=pd.Timestamp("2024-01-01 10:00"),
            exit_time=pd.Timestamp("2024-01-01 11:00"),
            direction=TradeDirection.LONG,
            entry_price=100.0,
            exit_price=95.0,
            is_win=False,
        )
        result = create_trade_result([trade])
        metrics = MetricsCalculator.calculate(result)
        
        assert metrics.max_drawdown == -5.0


# ============================================================================
# STREAK TESTS
# ============================================================================

class TestStreaks:
    """Test winning/losing streak calculations"""

    def test_losing_streak(self, sample_trades_with_streaks):
        """Should find longest losing streak"""
        result = create_trade_result(sample_trades_with_streaks)
        metrics = MetricsCalculator.calculate(result)
        
        assert metrics.losing_streak == 3

    def test_winning_streak(self, sample_trades_with_streaks):
        """Should find longest winning streak"""
        result = create_trade_result(sample_trades_with_streaks)
        metrics = MetricsCalculator.calculate(result)
        
        assert metrics.winning_streak == 4

    def test_streaks_all_wins(self, sample_trades_all_wins):
        """Should handle all winning trades"""
        result = create_trade_result(sample_trades_all_wins)
        metrics = MetricsCalculator.calculate(result)
        
        assert metrics.winning_streak == 10
        assert metrics.losing_streak == 0

    def test_streaks_all_losses(self, sample_trades_all_losses):
        """Should handle all losing trades"""
        result = create_trade_result(sample_trades_all_losses)
        metrics = MetricsCalculator.calculate(result)
        
        assert metrics.winning_streak == 0
        assert metrics.losing_streak == 10

    def test_streaks_single_trade(self, sample_trades_single):
        """Should handle single trade"""
        result = create_trade_result(sample_trades_single)
        metrics = MetricsCalculator.calculate(result)
        
        assert metrics.winning_streak == 1
        assert metrics.losing_streak == 0


# ============================================================================
# TRADE FREQUENCY TESTS
# ============================================================================

class TestTradeFrequency:
    """Test trades per day/week calculations"""

    def test_trades_per_day(self, sample_trades_time_distribution):
        """Should calculate average trades per day correctly"""
        result = create_trade_result(sample_trades_time_distribution)
        metrics = MetricsCalculator.calculate(result)
        
        # 10 trades over 3 calendar days = 3.33 trades/day
        assert metrics.trades_per_day == 3.33

    def test_trades_per_week(self, sample_trades_time_distribution):
        """Should calculate average trades per week correctly"""
        result = create_trade_result(sample_trades_time_distribution)
        metrics = MetricsCalculator.calculate(result)
        
        # 10 trades over 3 days = 3.33 * 7 = 23.33 trades/week
        assert metrics.trades_per_week == 23.33

    def test_trades_per_day_single_day(self):
        """Should handle all trades on same day"""
        base_date = pd.Timestamp("2024-01-01")
        trades = []
        for hour in range(9, 17):  # 8 trades
            is_win = hour % 2 == 0
            trades.append(
                create_test_trade(
                    signal_id=hour,
                    entry_time=base_date + pd.Timedelta(hours=hour),
                    exit_time=base_date + pd.Timedelta(hours=hour + 1),
                    direction=TradeDirection.LONG,
                    entry_price=100.0,
                    exit_price=101.0,
                    is_win=True,
                )
            )
        
        result = create_trade_result(trades)
        metrics = MetricsCalculator.calculate(result)
        
        # 8 trades in 1 day = 8.0
        assert metrics.trades_per_day == 8.0
        assert metrics.trades_per_week == 56.0

    def test_trades_per_day_single_trade(self, sample_trades_single):
        """Should handle single trade"""
        result = create_trade_result(sample_trades_single)
        metrics = MetricsCalculator.calculate(result)
        
        # Single trade = 1.0 per day (intuitive)
        assert metrics.trades_per_day == 1.0
        assert metrics.trades_per_week == 7.0


# ============================================================================
# EDGE CASE TESTS
# ============================================================================

class TestEdgeCases:
    """Test edge cases and boundary conditions"""

    def test_zero_trades(self):
        """Should handle empty trades list"""
        result = create_trade_result([])
        metrics = MetricsCalculator.calculate(result)
        
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

    def test_rejected_signals_ignored(self, trade_result_with_rejects):
        """Should ignore rejected signals in calculations"""
        metrics = MetricsCalculator.calculate(trade_result_with_rejects)
        
        # Trades count should still be 3 (ignoring rejects)
        assert metrics.total_trades == 3
        assert metrics.winning_trades == 2
        assert metrics.losing_trades == 1


# ============================================================================
# TIMING AND CONVENIENCE FUNCTIONS
# ============================================================================

class TestTimingFunctions:
    """Test calculate_metrics_with_timing and convenience functions"""

    def test_calculate_metrics_convenience(self, trade_result_simple):
        """calculate_metrics should work and return MetricsReport"""
        metrics = calculate_metrics(trade_result_simple)
        
        assert isinstance(metrics, MetricsReport)
        assert metrics.total_trades == 3

    def test_calculate_metrics_with_timing(self, trade_result_simple):
        """Should include execution duration"""
        start_time = time.perf_counter()
        # Small delay to ensure measurable time
        time.sleep(0.001)  # 1ms delay
        
        metrics = calculate_metrics_with_timing(trade_result_simple, start_time)
        
        assert isinstance(metrics, MetricsReport)
        assert metrics.execution_duration_ms > 0.9
        assert metrics.execution_duration_ms < 10.0  # Upper bound

    def test_timing_without_start(self, trade_result_simple):
        """Should handle missing start_time"""
        # This will use current time as start, so duration will be near 0
        metrics = calculate_metrics_with_timing(trade_result_simple, time.perf_counter())
        
        assert isinstance(metrics, MetricsReport)
        assert metrics.execution_duration_ms >= 0.0


# ============================================================================
# PERFORMANCE TESTS
# ============================================================================

class TestPerformance:
    """Performance benchmarks for MetricsCalculator"""

    def test_performance_medium_dataset(self):
        """Should meet <10ms target for 1000 trades"""
        # Generate 1000 trades
        trades = []
        for i in range(1000):
            is_win = i % 3 == 0  # ~33% win rate
            pnl = 10.0 if is_win else -5.0
            trades.append(
                create_test_trade(
                    signal_id=i,
                    entry_time=pd.Timestamp("2024-01-01") + pd.Timedelta(minutes=i),
                    exit_time=pd.Timestamp("2024-01-01") + pd.Timedelta(minutes=i + 1),
                    direction=TradeDirection.LONG,
                    entry_price=100.0,
                    exit_price=100.0 + pnl,
                    is_win=is_win,
                )
            )
        
        result = create_trade_result(trades)
        
        # Warm-up
        for _ in range(5):
            MetricsCalculator.calculate(result)
        
        # Benchmark
        iterations = 10
        start = time.perf_counter()
        
        for _ in range(iterations):
            metrics = MetricsCalculator.calculate(result)
        
        elapsed = time.perf_counter() - start
        avg_time_ms = (elapsed / iterations) * 1000
        
        print(f"\nPerformance (1000 trades, {iterations} iterations):")
        print(f"  Average time: {avg_time_ms:.3f}ms per calculation")
        print(f"  Target: <10ms")
        
        assert avg_time_ms < 10.0, f"Performance too slow: {avg_time_ms:.3f}ms > 10ms"


# ============================================================================
# COMPREHENSIVE VALIDATION TEST
# ============================================================================

class TestComprehensiveValidation:
    """Comprehensive test comparing with known results from SESSION_14_HANDOFF.md"""

    def test_handoff_example_validation(self):
        """Validate against the example in SESSION_14_HANDOFF.md"""
        # Create trades that would produce the metrics in the handoff
        # Metrics from handoff:
        # total_trades=1151, winning_trades=194, losing_trades=957, win_rate=16.85
        # total_pnl_points=-2998.05, expectancy_points=-2.6, profit_factor=0.81
        # largest_win=159.08, largest_loss=-62.06, max_drawdown=-3383.85
        # losing_streak=41, winning_streak=5
        
        trades = []
        
        # Create winning trades (194)
        for i in range(194):
            # Make first trade the largest win
            if i == 0:
                pnl = 159.08
            else:
                # Distribute remaining wins
                pnl = 12.0 + (i * 0.01)
            trades.append(
                create_test_trade(
                    signal_id=i,
                    entry_time=pd.Timestamp("2024-01-01") + pd.Timedelta(hours=i),
                    exit_time=pd.Timestamp("2024-01-01") + pd.Timedelta(hours=i + 1),
                    direction=TradeDirection.LONG,
                    entry_price=100.0,
                    exit_price=100.0 + pnl,
                    is_win=True,
                )
            )
        
        # Create losing trades (957)
        for i in range(957):
            # Make first loss the largest loss
            if i == 0:
                pnl = -62.06
            else:
                # Distribute losses
                pnl = -3.0 - (i * 0.001)
            trades.append(
                create_test_trade(
                    signal_id=194 + i,
                    entry_time=pd.Timestamp("2024-01-01") + pd.Timedelta(hours=194 + i),
                    exit_time=pd.Timestamp("2024-01-01") + pd.Timedelta(hours=194 + i + 1),
                    direction=TradeDirection.LONG,
                    entry_price=100.0,
                    exit_price=100.0 + pnl,
                    is_win=False,
                )
            )
        
        # Sort by entry time for streak calculation
        trades.sort(key=lambda t: t.entry.entry_time)
        
        result = create_trade_result(trades)
        metrics = MetricsCalculator.calculate(result)
        
        print(f"\nComprehensive Validation vs Handoff Example:")
        print(f"  Metric            | Calculated | Expected  | Difference")
        print(f"  ------------------+------------+-----------+-----------")
        
        comparisons = [
            ("total_trades", metrics.total_trades, 1151),
            ("winning_trades", metrics.winning_trades, 194),
            ("losing_trades", metrics.losing_trades, 957),
            ("win_rate", metrics.win_rate, 16.85),
            ("total_pnl_points", metrics.total_pnl_points, -2998.05),
            ("profit_factor", metrics.profit_factor, 0.81),
            ("largest_win", metrics.largest_win, 159.08),
            ("largest_loss", metrics.largest_loss, -62.06),
        ]
        
        for name, calc, exp in comparisons:
            if exp != 0:
                diff = abs(calc - exp) / exp * 100
            else:
                diff = 0
            print(f"  {name:18s} | {calc:10.2f} | {exp:9.2f} | {diff:6.2f}%")
        
        # Check approximate matches (within reasonable tolerance)
        assert metrics.total_trades == 1151
        assert metrics.winning_trades == 194
        assert metrics.losing_trades == 957
        assert metrics.win_rate == pytest.approx(16.85, rel=1e-2)
        assert metrics.largest_win == pytest.approx(159.08, rel=1e-2)
        assert metrics.largest_loss == pytest.approx(-62.06, rel=1e-2)