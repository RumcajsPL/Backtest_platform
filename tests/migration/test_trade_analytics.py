"""
test_trade_analytics_session15.py
==================================
Session 15 tests for TradeAnalytics — Time Performance + Trade Quality.

Run with:
    pytest tests/migration/test_trade_analytics_session15.py -v

All tests use lightweight fakes — no real simulator needed.
"""

import pytest
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from unittest.mock import MagicMock
import pandas as pd
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# ============================================================
# FAKES — lightweight stand-ins for contracts
# (mirrors the real dataclasses without importing the full project)
# ============================================================

@dataclass
class FakeMetricsReport:
    total_trades:    int   = 100
    winning_trades:  int   = 20
    losing_trades:   int   = 80
    win_rate:        float = 20.0
    total_pnl_points: float = 100.0
    expectancy_points: float = 1.0
    profit_factor:   float = 1.5
    avg_pnl_points:  float = 1.0
    largest_win:     float = 15.0
    largest_loss:    float = -5.0
    max_drawdown:    float = -30.0
    losing_streak:   int   = 5
    winning_streak:  int   = 3
    trades_per_week: float = 20.0
    trades_per_day:  float = 4.0
    execution_duration_ms: float = 1.5
    execution_date:  str   = "2026-02-17"

    def to_dict(self):
        return self.__dict__


def _ts(date_str: str) -> pd.Timestamp:
    """Parse a datetime string into a pd.Timestamp."""
    return pd.Timestamp(date_str)


def _make_trade(
    entry_time: str,
    pnl: float,
    duration_bars: int = 5,
    is_win: Optional[bool] = None,
) -> MagicMock:
    """
    Build a minimal Trade mock with entry + exit set.

    Args:
        entry_time:    ISO datetime string (UTC assumed).
        pnl:           P&L in points — positive = win, negative = loss.
        duration_bars: How many bars the trade lasted.
        is_win:        Override; defaults to pnl > 0.
    """
    if is_win is None:
        is_win = pnl > 0
    is_loss = not is_win

    trade_exit = MagicMock()
    trade_exit.pnl_points     = pnl
    trade_exit.is_win         = is_win
    trade_exit.is_loss        = is_loss
    trade_exit.duration_bars  = duration_bars

    trade_entry = MagicMock()
    trade_entry.entry_time    = _ts(entry_time)

    trade = MagicMock()
    trade.entry = trade_entry
    trade.exit  = trade_exit

    return trade


def _make_trade_result(trades: List) -> MagicMock:
    """Wrap a list of trade mocks into a fake TradeResult."""
    tr = MagicMock()
    tr.trades = trades
    return tr


# ============================================================
# IMPORT MODULE UNDER TEST
# (adjust sys.path if running outside project root)
# ============================================================

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

# Import only the pieces we need so the test works even with
# Session-16 stubs still in place.
from src.strategies.specific.modules.trade_analytics import TradeAnalytics
from src.strategies.contracts.analytics_contracts import TradingSessionConfig


# ============================================================
# FIXTURES
# ============================================================

@pytest.fixture
def default_config() -> TradingSessionConfig:
    return TradingSessionConfig()   # Asia(0-8) / London(8-16) / NY(16-24)


@pytest.fixture
def metrics() -> FakeMetricsReport:
    return FakeMetricsReport()


# ── Trade builders ────────────────────────────────────────────────────────────

def _asia_win(pnl=5.0, bars=4):
    """A winning trade during Asia session (02:00 UTC, Wednesday)."""
    return _make_trade("2024-10-09 02:00:00", pnl, duration_bars=bars)

def _london_win(pnl=8.0, bars=6):
    """A winning trade during London session (09:00 UTC, Tuesday)."""
    return _make_trade("2024-10-08 09:00:00", pnl, duration_bars=bars)

def _ny_loss(pnl=-3.0, bars=5):
    """A losing trade during NY session (17:00 UTC, Monday)."""
    return _make_trade("2024-10-07 17:00:00", pnl, duration_bars=bars)

def _monday_loss(pnl=-4.0, bars=3):
    """A losing trade on Monday (09:00 UTC)."""
    return _make_trade("2024-10-07 09:00:00", pnl, duration_bars=bars)


# ============================================================
# TEST GROUP 1: _get_session_for_hour
# ============================================================

class TestGetSessionForHour:
    def test_asia_start(self, default_config):
        assert TradeAnalytics._get_session_for_hour(0, default_config) == "Asia"

    def test_asia_end_boundary(self, default_config):
        # Hour 7 is still Asia (< 8)
        assert TradeAnalytics._get_session_for_hour(7, default_config) == "Asia"

    def test_london_start(self, default_config):
        assert TradeAnalytics._get_session_for_hour(8, default_config) == "London"

    def test_london_mid(self, default_config):
        assert TradeAnalytics._get_session_for_hour(12, default_config) == "London"

    def test_ny_start(self, default_config):
        assert TradeAnalytics._get_session_for_hour(16, default_config) == "NY"

    def test_ny_end(self, default_config):
        assert TradeAnalytics._get_session_for_hour(23, default_config) == "NY"

    def test_custom_sessions(self):
        config = TradingSessionConfig(sessions={"Morning": (6, 12), "Afternoon": (12, 18)})
        assert TradeAnalytics._get_session_for_hour(6,  config) == "Morning"
        assert TradeAnalytics._get_session_for_hour(11, config) == "Morning"
        assert TradeAnalytics._get_session_for_hour(12, config) == "Afternoon"
        assert TradeAnalytics._get_session_for_hour(4,  config) == "Other"


# ============================================================
# TEST GROUP 2: _calculate_session_metrics
# ============================================================

class TestCalculateSessionMetrics:
    def test_empty_trades(self):
        sm = TradeAnalytics._calculate_session_metrics([], "Asia")
        assert sm.session_name   == "Asia"
        assert sm.trades         == 0
        assert sm.winning_trades == 0
        assert sm.win_rate       == 0.0
        assert sm.total_pnl      == 0.0

    def test_all_wins(self):
        trades = [_asia_win(5.0), _asia_win(3.0), _asia_win(8.0)]
        sm = TradeAnalytics._calculate_session_metrics(trades, "Asia")
        assert sm.trades         == 3
        assert sm.winning_trades == 3
        assert sm.win_rate       == pytest.approx(100.0)
        assert sm.total_pnl      == pytest.approx(16.0)
        assert sm.largest_win    == pytest.approx(8.0)
        assert sm.largest_loss   == pytest.approx(3.0)   # min of pnl values

    def test_all_losses(self):
        trades = [_ny_loss(-3.0), _ny_loss(-5.0)]
        sm = TradeAnalytics._calculate_session_metrics(trades, "NY")
        assert sm.trades         == 2
        assert sm.winning_trades == 0
        assert sm.win_rate       == pytest.approx(0.0)
        assert sm.total_pnl      == pytest.approx(-8.0)
        assert sm.largest_loss   == pytest.approx(-5.0)

    def test_mixed(self):
        trades = [
            _make_trade("2024-10-07 09:00:00", 10.0, duration_bars=5),   # win
            _make_trade("2024-10-07 09:30:00", -4.0, duration_bars=3),   # loss
            _make_trade("2024-10-07 10:00:00",  6.0, duration_bars=4),   # win
        ]
        sm = TradeAnalytics._calculate_session_metrics(trades, "London")
        assert sm.trades         == 3
        assert sm.winning_trades == 2
        assert sm.win_rate       == pytest.approx(200 / 3)
        assert sm.total_pnl      == pytest.approx(12.0)
        assert sm.avg_pnl        == pytest.approx(4.0)


# ============================================================
# TEST GROUP 3: _analyze_time_performance — grouping
# ============================================================

class TestAnalyzeTimePerformanceGrouping:
    def test_no_closed_trades(self, metrics, default_config):
        """Open-only trades → empty breakdown."""
        open_trade = MagicMock()
        open_trade.exit = None
        tr = _make_trade_result([open_trade])

        result = TradeAnalytics._analyze_time_performance(tr, metrics, default_config)
        assert result.by_session == {}
        assert result.by_hour    == {}
        assert result.by_day     == {}
        assert result.best_session  == "N/A"
        assert result.worst_session == "N/A"

    def test_correct_session_grouping(self, metrics, default_config):
        trades = [
            _make_trade("2024-10-07 02:00:00",  5.0, duration_bars=5),  # Asia   (Mon)
            _make_trade("2024-10-07 09:00:00", -3.0, duration_bars=5),  # London (Mon)
            _make_trade("2024-10-07 17:00:00",  2.0, duration_bars=5),  # NY     (Mon)
        ]
        tr = _make_trade_result(trades)
        result = TradeAnalytics._analyze_time_performance(tr, metrics, default_config)

        assert "Asia"   in result.by_session
        assert "London" in result.by_session
        assert "NY"     in result.by_session
        assert result.by_session["Asia"].trades   == 1
        assert result.by_session["London"].trades == 1
        assert result.by_session["NY"].trades     == 1

    def test_best_and_worst_session(self, metrics, default_config):
        # Asia: +100 London: +20 NY: -40
        asia_trades   = [_make_trade(f"2024-10-07 0{i}:00:00", 10.0, duration_bars=5) for i in range(3)]
        asia_trades  += [_make_trade("2024-10-07 03:00:00", 70.0, duration_bars=5)]
        london_trades = [_make_trade("2024-10-07 09:00:00", 20.0, duration_bars=5)]
        ny_trades     = [_make_trade("2024-10-07 17:00:00", -40.0, duration_bars=5)]
        tr = _make_trade_result(asia_trades + london_trades + ny_trades)

        result = TradeAnalytics._analyze_time_performance(tr, metrics, default_config)
        assert result.best_session  == "Asia"
        assert result.worst_session == "NY"

    def test_hourly_grouping(self, metrics, default_config):
        trades = [
            _make_trade("2024-10-07 09:00:00",  5.0, duration_bars=5),
            _make_trade("2024-10-07 09:30:00",  3.0, duration_bars=5),
            _make_trade("2024-10-07 10:00:00", -2.0, duration_bars=5),
        ]
        tr = _make_trade_result(trades)
        result = TradeAnalytics._analyze_time_performance(tr, metrics, default_config)
        # Hour 9 should have 2 trades, hour 10 should have 1
        assert result.by_hour[9].trades  == 2
        assert result.by_hour[10].trades == 1

    def test_day_grouping(self, metrics, default_config):
        trades = [
            _make_trade("2024-10-07 09:00:00",  5.0, duration_bars=5),  # Monday
            _make_trade("2024-10-08 09:00:00", -3.0, duration_bars=5),  # Tuesday
            _make_trade("2024-10-09 09:00:00",  7.0, duration_bars=5),  # Wednesday
        ]
        tr = _make_trade_result(trades)
        result = TradeAnalytics._analyze_time_performance(tr, metrics, default_config)
        assert "Monday"    in result.by_day
        assert "Tuesday"   in result.by_day
        assert "Wednesday" in result.by_day


# ============================================================
# TEST GROUP 4: _generate_time_insights
# ============================================================

class TestGenerateTimeInsights:
    """Verify each insight rule fires (and doesn't fire) correctly."""

    def _sm(self, name, trades=100, wins=20, pnl=0.0, win_rate=20.0):
        """Quick SessionMetrics builder with consistent data."""
        from src.strategies.contracts.analytics_contracts import SessionMetrics
        # Ensure winning_trades doesn't exceed total trades
        wins = min(wins, trades)
        return SessionMetrics(
            session_name=name,
            trades=trades,
            winning_trades=wins,
            win_rate=win_rate,
            total_pnl=pnl,
            avg_pnl=pnl / trades if trades else 0.0,
            largest_win=max(pnl, 0) if pnl > 0 else 0.0,
            largest_loss=min(pnl, 0) if pnl < 0 else 0.0,
        )

    # ── Rule 1: session losing significantly ────────────────────────────────

    def test_critical_session_loss_fires(self, metrics):
        by_session = {
            "Asia": self._sm("Asia", trades=100, wins=10, pnl=-45.0, win_rate=10.0),
            "London": self._sm("London", trades=200, wins=44, pnl=180.0, win_rate=22.0),
        }
        insights = TradeAnalytics._generate_time_insights(by_session, {}, {}, metrics)
        critical = [i for i in insights if i.severity == "critical"]
        assert len(critical) >= 1
        assert any("Asia" in i.message for i in critical)

    def test_critical_session_loss_not_fired_if_few_trades(self, metrics):
        by_session = {
            "Asia": self._sm("Asia", trades=10, wins=1, pnl=-45.0, win_rate=10.0),
        }
        insights = TradeAnalytics._generate_time_insights(by_session, {}, {}, metrics)
        criticals = [i for i in insights if i.severity == "critical" and "Asia" in i.message]
        assert len(criticals) == 0   # Should be warning, not critical

    # ── Rule 2: session win rate below average ───────────────────────────────

    def test_low_win_rate_session_warning(self, metrics):
        metrics.win_rate = 20.0
        by_session = {
            "Asia": self._sm("Asia", trades=80, wins=6, pnl=-5.0, win_rate=7.5),   # < 70% of 20
        }
        insights = TradeAnalytics._generate_time_insights(by_session, {}, {}, metrics)
        warnings = [i for i in insights if i.severity == "warning" and "win rate" in i.message]
        assert len(warnings) >= 1

    def test_win_rate_ok_does_not_fire(self, metrics):
        metrics.win_rate = 20.0
        by_session = {
            "London": self._sm("London", trades=100, wins=18, pnl=80.0, win_rate=18.0),  # 90% of 20 → OK
        }
        insights = TradeAnalytics._generate_time_insights(by_session, {}, {}, metrics)
        win_rate_warnings = [i for i in insights if "win rate" in i.message and "London" in i.message]
        assert len(win_rate_warnings) == 0

    # ── Rule 3: day underperforming ──────────────────────────────────────────

    def test_negative_day_fires_warning(self, metrics):
        from src.strategies.contracts.analytics_contracts import SessionMetrics
        by_day = {
            "Wednesday": SessionMetrics(
                session_name="Wednesday", trades=50, winning_trades=8,
                win_rate=16.0, total_pnl=-25.0, avg_pnl=-0.5,
                largest_win=3.0, largest_loss=-8.0,
            )
        }
        insights = TradeAnalytics._generate_time_insights({}, {}, by_day, metrics)
        day_warnings = [i for i in insights if "Wednesday" in i.message]
        assert len(day_warnings) >= 1

    # ── Rule 4: session drives most profit ──────────────────────────────────

    def test_primary_session_success(self, metrics):
        metrics.total_pnl_points = 200.0
        by_session = {
            "London": self._sm("London", trades=200, wins=44, pnl=160.0, win_rate=22.0),  # 80% of profit
            "NY":     self._sm("NY",     trades=100, wins=18, pnl=40.0,  win_rate=18.0),
        }
        insights = TradeAnalytics._generate_time_insights(by_session, {}, {}, metrics)
        successes = [i for i in insights if i.severity == "success" and "London" in i.message]
        assert len(successes) >= 1

    # ── Rule 5: session loss contribution ───────────────────────────────────

    def test_dominant_loss_session_fires(self, metrics):
        by_session = {
            "Asia":   self._sm("Asia",   trades=100, wins=8, pnl=-80.0, win_rate=8.0),
            "London": self._sm("London", trades=200, wins=36, pnl=-20.0, win_rate=18.0),
        }
        insights = TradeAnalytics._generate_time_insights(by_session, {}, {}, metrics)
        # Asia accounts for -80 / -100 = 80% of losses → should fire
        loss_warnings = [
            i for i in insights
            if "Asia" in i.message and "losses" in i.message
        ]
        assert len(loss_warnings) >= 1

    # ── Rule 6: best hour cluster ────────────────────────────────────────────

    def test_best_hour_info_fires(self, metrics):
        by_hour = {
            9:  self._sm("9",  trades=50, wins=13, pnl=50.0,  win_rate=26.0),
            10: self._sm("10", trades=40, wins=9, pnl=30.0,  win_rate=22.5),
            14: self._sm("14", trades=20, wins=4, pnl=15.0,  win_rate=20.0),
        }
        insights = TradeAnalytics._generate_time_insights({}, by_hour, {}, metrics)
        info = [i for i in insights if i.severity == "info"]
        assert len(info) >= 1


# ============================================================
# TEST GROUP 5: _calculate_trade_distribution
# ============================================================

class TestCalculateTradeDistribution:
    def test_empty_trades(self):
        dist = TradeAnalytics._calculate_trade_distribution([], is_wins=True)
        assert dist.small_count == 0
        assert dist.small_pct   == 0.0

    def test_all_small(self):
        trades = [_make_trade("2024-10-07 09:00:00", 1.5, duration_bars=5) for _ in range(10)]
        dist = TradeAnalytics._calculate_trade_distribution(trades, is_wins=True)
        assert dist.small_count  == 10
        assert dist.medium_count == 0
        assert dist.large_count  == 0
        assert dist.small_pct    == pytest.approx(100.0)

    def test_all_medium(self):
        trades = [_make_trade("2024-10-07 09:00:00", 5.0, duration_bars=5) for _ in range(6)]
        dist = TradeAnalytics._calculate_trade_distribution(trades, is_wins=True)
        assert dist.medium_count == 6
        assert dist.medium_pct   == pytest.approx(100.0)

    def test_all_large(self):
        trades = [_make_trade("2024-10-07 09:00:00", 10.0, duration_bars=5) for _ in range(4)]
        dist = TradeAnalytics._calculate_trade_distribution(trades, is_wins=True)
        assert dist.large_count == 4
        assert dist.large_pct   == pytest.approx(100.0)

    def test_mixed_distribution(self):
        trades = [
            _make_trade("2024-10-07 09:00:00", 1.0, duration_bars=5),   # small
            _make_trade("2024-10-07 09:00:00", 2.9, duration_bars=5),   # small
            _make_trade("2024-10-07 09:00:00", 3.0, duration_bars=5),   # medium (boundary)
            _make_trade("2024-10-07 09:00:00", 7.0, duration_bars=5),   # medium (boundary)
            _make_trade("2024-10-07 09:00:00", 7.1, duration_bars=5),   # large
        ]
        dist = TradeAnalytics._calculate_trade_distribution(trades, is_wins=True)
        assert dist.small_count  == 2
        assert dist.medium_count == 2
        assert dist.large_count  == 1
        assert dist.small_pct  == pytest.approx(40.0)
        assert dist.medium_pct == pytest.approx(40.0)
        assert dist.large_pct  == pytest.approx(20.0)

    def test_loss_uses_absolute_value(self):
        """Losses are negative — distribution should use |pnl|."""
        trades = [_make_trade("2024-10-07 09:00:00", -8.0, duration_bars=5)]   # large loss
        dist = TradeAnalytics._calculate_trade_distribution(trades, is_wins=False)
        assert dist.large_count == 1

    def test_percentages_sum_to_100(self):
        trades = [
            _make_trade("2024-10-07 09:00:00", 1.0, duration_bars=5),
            _make_trade("2024-10-07 09:00:00", 4.0, duration_bars=5),
            _make_trade("2024-10-07 09:00:00", 9.0, duration_bars=5),
        ]
        dist = TradeAnalytics._calculate_trade_distribution(trades, is_wins=True)
        total = dist.small_pct + dist.medium_pct + dist.large_pct
        assert total == pytest.approx(100.0, abs=0.1)


# ============================================================
# TEST GROUP 6: _analyze_duration_patterns
# ============================================================

class TestAnalyzeDurationPatterns:
    def test_empty_trades(self):
        dur = TradeAnalytics._analyze_duration_patterns([])
        assert dur.avg_bars       == 0.0
        assert dur.median_bars    == 0
        assert dur.fast_exits_pct == 0.0

    def test_all_fast_exits(self):
        trades = [_make_trade("2024-10-07 09:00:00", 2.0, duration_bars=1) for _ in range(5)]
        dur = TradeAnalytics._analyze_duration_patterns(trades)
        assert dur.fast_exits_count    == 5
        assert dur.normal_exits_count  == 0
        assert dur.fast_exits_pct      == pytest.approx(100.0)

    def test_all_normal(self):
        trades = [_make_trade("2024-10-07 09:00:00", 2.0, duration_bars=5) for _ in range(4)]
        dur = TradeAnalytics._analyze_duration_patterns(trades)
        assert dur.normal_exits_count  == 4
        assert dur.fast_exits_count    == 0
        assert dur.prolonged_exits_count == 0

    def test_all_prolonged(self):
        trades = [_make_trade("2024-10-07 09:00:00", 2.0, duration_bars=15) for _ in range(3)]
        dur = TradeAnalytics._analyze_duration_patterns(trades)
        assert dur.prolonged_exits_count == 3

    def test_avg_and_median(self):
        trades = [
            _make_trade("2024-10-07 09:00:00", 2.0, duration_bars=2),
            _make_trade("2024-10-07 09:00:00", 2.0, duration_bars=4),
            _make_trade("2024-10-07 09:00:00", 2.0, duration_bars=6),
        ]
        dur = TradeAnalytics._analyze_duration_patterns(trades)
        assert dur.avg_bars    == pytest.approx(4.0)
        assert dur.median_bars == 4

    def test_text_insight_on_high_fast_exits(self):
        trades = [_make_trade("2024-10-07 09:00:00", 2.0, duration_bars=1) for _ in range(10)]
        dur = TradeAnalytics._analyze_duration_patterns(trades)
        assert len(dur.insights) > 0


# ============================================================
# TEST GROUP 7: _generate_quality_insights
# ============================================================

class TestGenerateQualityInsights:
    def _dist(self, small=2, medium=2, large=1):
        from src.strategies.contracts.analytics_contracts import TradeDistribution
        total = small + medium + large
        return TradeDistribution(
            small_count=small, medium_count=medium, large_count=large,
            small_pct=round(small/total*100, 2) if total else 0.0,
            medium_pct=round(medium/total*100, 2) if total else 0.0,
            large_pct=round(large/total*100, 2) if total else 0.0,
        )

    def _dur(self, fast_pct=30.0):
        from src.strategies.contracts.analytics_contracts import DurationAnalysis
        total = 100
        fast = int(total * fast_pct / 100)
        return DurationAnalysis(
            avg_bars=5.0, median_bars=5,
            fast_exits_count=fast,
            normal_exits_count=total - fast,
            prolonged_exits_count=0,
            fast_exits_pct=fast_pct,
            insights=[],
        )

    def test_critical_fast_exit(self, metrics):
        dur = self._dur(fast_pct=85.0)
        insights = TradeAnalytics._generate_quality_insights(
            self._dist(), self._dist(), dur, metrics, 2.0, 5.0
        )
        criticals = [i for i in insights if i.severity == "critical"]
        assert len(criticals) >= 1

    def test_warning_fast_exit(self, metrics):
        dur = self._dur(fast_pct=65.0)
        insights = TradeAnalytics._generate_quality_insights(
            self._dist(), self._dist(), dur, metrics, 3.0, 5.0
        )
        warnings = [i for i in insights if i.severity == "warning" and "early-exit" in i.message]
        assert len(warnings) >= 1

    def test_no_fast_exit_insight_when_low(self, metrics):
        dur = self._dur(fast_pct=20.0)
        insights = TradeAnalytics._generate_quality_insights(
            self._dist(), self._dist(), dur, metrics, 3.0, 5.0
        )
        exit_insights = [i for i in insights if "exit" in i.message.lower() and i.severity in ("critical", "warning")]
        # Should not flag fast exits when rate is low
        fast_exit_flags = [i for i in exit_insights if "early-exit" in i.message]
        assert len(fast_exit_flags) == 0

    def test_winner_faster_than_loser_success(self, metrics):
        dur = self._dur(fast_pct=20.0)
        insights = TradeAnalytics._generate_quality_insights(
            self._dist(), self._dist(), dur, metrics,
            avg_bars_to_profit=2.0,   # winners exit fast
            avg_bars_to_loss=4.0,     # losers take longer
        )
        successes = [i for i in insights if i.severity == "success"]
        assert len(successes) >= 1

    def test_loser_faster_than_winner_warning(self, metrics):
        dur = self._dur(fast_pct=20.0)
        insights = TradeAnalytics._generate_quality_insights(
            self._dist(), self._dist(), dur, metrics,
            avg_bars_to_profit=6.0,   # winners take long
            avg_bars_to_loss=2.0,     # losers exit fast
        )
        warnings = [i for i in insights if "cut winners early" in i.message or "let losses run" in i.message]
        assert len(warnings) >= 1

    def test_large_loss_distribution_fires(self, metrics):
        loss_dist = self._dist(small=1, medium=1, large=8)  # 80% large losses
        dur = self._dur(fast_pct=20.0)
        insights = TradeAnalytics._generate_quality_insights(
            self._dist(), loss_dist, dur, metrics, 3.0, 4.0
        )
        large_loss_warnings = [i for i in insights if "loss" in i.message and "large" in i.message]
        assert len(large_loss_warnings) >= 1


# ============================================================
# TEST GROUP 8: _analyze_trade_quality (integration)
# ============================================================

class TestAnalyzeTradeQualityIntegration:
    def test_empty_result(self, metrics):
        tr = _make_trade_result([])
        result = TradeAnalytics._analyze_trade_quality(tr, metrics)
        assert result.avg_bars_to_profit is None
        assert result.avg_bars_to_loss   is None
        assert result.premature_exit_estimate == "No data"

    def test_wins_and_losses_separated(self, metrics):
        trades = [
            _make_trade("2024-10-07 09:00:00",  8.0, duration_bars=6),   # win
            _make_trade("2024-10-07 10:00:00",  3.0, duration_bars=4),   # win
            _make_trade("2024-10-07 11:00:00", -4.0, duration_bars=2),   # loss
        ]
        tr = _make_trade_result(trades)
        result = TradeAnalytics._analyze_trade_quality(tr, metrics)

        assert result.win_distribution.small_count  + \
               result.win_distribution.medium_count + \
               result.win_distribution.large_count  == 2
        assert result.loss_distribution.small_count + \
               result.loss_distribution.medium_count + \
               result.loss_distribution.large_count == 1

    def test_avg_bars_calculated(self, metrics):
        trades = [
            _make_trade("2024-10-07 09:00:00",  5.0, duration_bars=4),   # win
            _make_trade("2024-10-07 10:00:00",  2.0, duration_bars=6),   # win
            _make_trade("2024-10-07 11:00:00", -3.0, duration_bars=3),   # loss
        ]
        tr = _make_trade_result(trades)
        result = TradeAnalytics._analyze_trade_quality(tr, metrics)
        assert result.avg_bars_to_profit == pytest.approx(5.0)   # (4+6)/2
        assert result.avg_bars_to_loss   == pytest.approx(3.0)

    def test_premature_exit_narrative_populated(self, metrics):
        trades = [_make_trade("2024-10-07 09:00:00", 2.0, duration_bars=1) for _ in range(20)]
        tr = _make_trade_result(trades)
        result = TradeAnalytics._analyze_trade_quality(tr, metrics)
        assert len(result.premature_exit_estimate) > 10   # Non-trivial string


# ============================================================
# TEST GROUP 9: _analyze_time_performance — insight integration
# ============================================================

class TestTimePerformanceInsightIntegration:
    def test_losing_asia_session_generates_insight(self, default_config):
        """60 trades all during Asia, all losing → should fire critical insight."""
        metrics = FakeMetricsReport(
            win_rate=20.0, total_pnl_points=-60.0
        )
        trades = [
            _make_trade(f"2024-10-{7 + i // 10:02d} 03:00:00", -1.0, duration_bars=3)
            for i in range(60)
        ]
        tr = _make_trade_result(trades)
        result = TradeAnalytics._analyze_time_performance(tr, metrics, default_config)

        critical = [i for i in result.insights if i.severity == "critical"]
        assert len(critical) >= 1
        assert any("Asia" in i.message for i in critical)

    def test_dominant_london_generates_success(self, default_config):
        """London drives > 60% of profit → success insight."""
        metrics = FakeMetricsReport(total_pnl_points=200.0, win_rate=20.0)
        # 130 pts London, 70 pts NY
        london = [_make_trade("2024-10-07 09:00:00", 1.3, duration_bars=4) for _ in range(100)]
        ny     = [_make_trade("2024-10-07 17:00:00", 0.7, duration_bars=4) for _ in range(100)]
        tr = _make_trade_result(london + ny)
        result = TradeAnalytics._analyze_time_performance(tr, metrics, default_config)

        successes = [i for i in result.insights if i.severity == "success"]
        assert len(successes) >= 1

    def test_insights_are_valid_insight_objects(self, metrics, default_config):
        """All generated insights must pass Insight contract validation."""
        trades = [
            _make_trade("2024-10-07 02:00:00", -1.0, duration_bars=5) for _ in range(60)
        ] + [
            _make_trade("2024-10-07 09:00:00", 3.0, duration_bars=5) for _ in range(40)
        ]
        tr = _make_trade_result(trades)
        result = TradeAnalytics._analyze_time_performance(tr, metrics, default_config)

        from src.strategies.contracts.analytics_contracts import Insight
        for insight in result.insights:
            assert isinstance(insight, Insight)
            assert insight.confidence in {"High", "Medium", "Low"}
            assert insight.severity   in {"critical", "warning", "info", "success"}
            assert insight.category   in {"time", "quality", "risk", "general"}


# ============================================================
# TEST GROUP 10: Edge cases
# ============================================================

class TestEdgeCases:
    def test_all_trades_same_session(self, metrics, default_config):
        trades = [_make_trade("2024-10-07 09:00:00", 2.0, duration_bars=5) for _ in range(10)]
        tr = _make_trade_result(trades)
        result = TradeAnalytics._analyze_time_performance(tr, metrics, default_config)
        assert len(result.by_session) == 1
        assert result.best_session == result.worst_session

    def test_single_trade(self, metrics, default_config):
        tr = _make_trade_result([_make_trade("2024-10-07 09:00:00", 5.0, duration_bars=3)])
        time_result = TradeAnalytics._analyze_time_performance(tr, metrics, default_config)
        assert time_result.by_session["London"].trades == 1

        quality_result = TradeAnalytics._analyze_trade_quality(tr, metrics)
        assert quality_result.win_distribution.small_count  + \
               quality_result.win_distribution.medium_count + \
               quality_result.win_distribution.large_count  == 1

    def test_all_zero_pnl(self, metrics, default_config):
        trades = [_make_trade("2024-10-07 09:00:00", 0.0, duration_bars=2) for _ in range(5)]
        tr = _make_trade_result(trades)
        # Should not crash
        result = TradeAnalytics._analyze_time_performance(tr, metrics, default_config)
        assert result.by_session["London"].total_pnl == pytest.approx(0.0)

    def test_analyse_does_not_crash_with_all_wins(self, metrics, default_config):
        trades = [_make_trade("2024-10-07 09:00:00", 5.0, duration_bars=5) for _ in range(20)]
        tr = _make_trade_result(trades)
        result = TradeAnalytics._analyze_trade_quality(tr, metrics)
        assert result.avg_bars_to_loss is None   # No losses → None

    def test_analyse_does_not_crash_with_all_losses(self, metrics, default_config):
        trades = [_make_trade("2024-10-07 09:00:00", -3.0, duration_bars=5) for _ in range(20)]
        tr = _make_trade_result(trades)
        result = TradeAnalytics._analyze_trade_quality(tr, metrics)
        assert result.avg_bars_to_profit is None   # No wins → None

    def test_custom_session_config(self, metrics):
        config = TradingSessionConfig(sessions={"AM": (6, 12), "PM": (12, 18)})
        trades = [
            _make_trade("2024-10-07 07:00:00", 3.0, duration_bars=5),  # AM
            _make_trade("2024-10-07 13:00:00", 5.0, duration_bars=5),  # PM
        ]
        tr = _make_trade_result(trades)
        result = TradeAnalytics._analyze_time_performance(tr, metrics, config)
        assert "AM" in result.by_session
        assert "PM" in result.by_session