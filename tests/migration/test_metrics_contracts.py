"""
Unit tests for MetricsReport contract - Session 13

Tests MetricsReport dataclass functionality:
- Creation and validation
- Serialization methods (to_dict, to_json, to_flat_dict)
- Derived properties (gross_profit, gross_loss, is_profitable)
- Edge cases and error conditions
- Empty metrics report helper

Design:
- Isolated tests (no dependencies on calculator)
- Comprehensive validation coverage
- Type safety verification
"""

import sys
from pathlib import Path
import json
import pytest
from datetime import datetime

# Add project root to path using path utility
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Use path utility for imports
from src.utils.paths import contract_path
from src.strategies.contracts.metrics_contracts import MetricsReport, create_empty_metrics_report


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def sample_metrics_data():
    """Sample metrics data for a losing strategy"""
    return {
        "total_trades": 1151,
        "winning_trades": 194,
        "losing_trades": 957,
        "win_rate": 16.85,
        "total_pnl_points": -2998.05,
        "expectancy_points": -2.6,
        "profit_factor": 0.81,
        "avg_pnl_points": -2.6,
        "largest_win": 159.08,
        "largest_loss": -62.06,
        "max_drawdown": -3383.85,
        "losing_streak": 41,
        "winning_streak": 5,
        "trades_per_week": 56.11,
        "trades_per_day": 12.24,
        "execution_duration_ms": 2765.23,
    }


@pytest.fixture
def profitable_metrics_data():
    """Sample metrics data for a profitable strategy"""
    return {
        "total_trades": 500,
        "winning_trades": 300,
        "losing_trades": 200,
        "win_rate": 60.0,
        "total_pnl_points": 5000.0,
        "expectancy_points": 10.0,
        "profit_factor": 2.5,
        "avg_pnl_points": 10.0,
        "largest_win": 200.0,
        "largest_loss": -50.0,
        "max_drawdown": -1000.0,
        "losing_streak": 5,
        "winning_streak": 15,
        "trades_per_week": 25.0,
        "trades_per_day": 5.0,
        "execution_duration_ms": 1000.0,
    }


@pytest.fixture
def zero_trades_data():
    """Metrics data for zero trades scenario"""
    return {
        "total_trades": 0,
        "winning_trades": 0,
        "losing_trades": 0,
        "win_rate": 0.0,
        "total_pnl_points": 0.0,
        "expectancy_points": 0.0,
        "profit_factor": 0.0,
        "avg_pnl_points": 0.0,
        "largest_win": 0.0,
        "largest_loss": 0.0,
        "max_drawdown": 0.0,
        "losing_streak": 0,
        "winning_streak": 0,
        "trades_per_week": 0.0,
        "trades_per_day": 0.0,
        "execution_duration_ms": 150.0,
    }


# ============================================================================
# CREATION & VALIDATION TESTS
# ============================================================================

class TestMetricsReportCreation:
    """Test MetricsReport creation and validation"""

    def test_create_valid_metrics(self, sample_metrics_data):
        """Should create MetricsReport with valid data"""
        metrics = MetricsReport(**sample_metrics_data)
        
        assert metrics.total_trades == 1151
        assert metrics.winning_trades == 194
        assert metrics.losing_trades == 957
        assert metrics.win_rate == 16.85
        assert metrics.total_pnl_points == -2998.05
        assert metrics.expectancy_points == -2.6
        assert metrics.profit_factor == 0.81
        assert metrics.losing_streak == 41
        assert metrics.winning_streak == 5
        assert metrics.execution_duration_ms == 2765.23
        
        # Verify execution_date is set (not empty)
        assert metrics.execution_date
        assert isinstance(metrics.execution_date, str)

    def test_negative_total_trades_raises_error(self, sample_metrics_data):
        """Should raise ValueError for negative total_trades"""
        sample_metrics_data["total_trades"] = -1
        with pytest.raises(ValueError, match="total_trades must be >= 0"):
            MetricsReport(**sample_metrics_data)

    def test_negative_winning_trades_raises_error(self, sample_metrics_data):
        """Should raise ValueError for negative winning_trades"""
        sample_metrics_data["winning_trades"] = -5
        with pytest.raises(ValueError, match="winning_trades must be >= 0"):
            MetricsReport(**sample_metrics_data)

    def test_negative_losing_trades_raises_error(self, sample_metrics_data):
        """Should raise ValueError for negative losing_trades"""
        sample_metrics_data["losing_trades"] = -10
        with pytest.raises(ValueError, match="losing_trades must be >= 0"):
            MetricsReport(**sample_metrics_data)

    def test_win_rate_below_zero_raises_error(self, sample_metrics_data):
        """Should raise ValueError for win_rate < 0"""
        sample_metrics_data["win_rate"] = -5.0
        with pytest.raises(ValueError, match="win_rate must be between 0 and 100"):
            MetricsReport(**sample_metrics_data)

    def test_win_rate_above_100_raises_error(self, sample_metrics_data):
        """Should raise ValueError for win_rate > 100"""
        sample_metrics_data["win_rate"] = 105.0
        with pytest.raises(ValueError, match="win_rate must be between 0 and 100"):
            MetricsReport(**sample_metrics_data)

    def test_negative_profit_factor_raises_error(self, sample_metrics_data):
        """Should raise ValueError for negative profit_factor"""
        sample_metrics_data["profit_factor"] = -0.5
        with pytest.raises(ValueError, match="profit_factor must be >= 0"):
            MetricsReport(**sample_metrics_data)

    def test_negative_streaks_raise_error(self, sample_metrics_data):
        """Should raise ValueError for negative streaks"""
        sample_metrics_data["losing_streak"] = -3
        with pytest.raises(ValueError, match="losing_streak must be >= 0"):
            MetricsReport(**sample_metrics_data)
        
        sample_metrics_data["losing_streak"] = 41  # Reset
        sample_metrics_data["winning_streak"] = -1
        with pytest.raises(ValueError, match="winning_streak must be >= 0"):
            MetricsReport(**sample_metrics_data)

    def test_negative_trade_frequency_raises_error(self, sample_metrics_data):
        """Should raise ValueError for negative trade frequency"""
        sample_metrics_data["trades_per_week"] = -5.0
        with pytest.raises(ValueError, match="trades_per_week must be >= 0"):
            MetricsReport(**sample_metrics_data)
        
        sample_metrics_data["trades_per_week"] = 56.11  # Reset
        sample_metrics_data["trades_per_day"] = -1.0
        with pytest.raises(ValueError, match="trades_per_day must be >= 0"):
            MetricsReport(**sample_metrics_data)

    def test_negative_execution_duration_raises_error(self, sample_metrics_data):
        """Should raise ValueError for negative execution duration"""
        sample_metrics_data["execution_duration_ms"] = -100.0
        with pytest.raises(ValueError, match="execution_duration_ms must be >= 0"):
            MetricsReport(**sample_metrics_data)

    def test_immutability(self, sample_metrics_data):
        """Should be immutable (frozen dataclass)"""
        metrics = MetricsReport(**sample_metrics_data)
        
        with pytest.raises(Exception):  # dataclass.FrozenInstanceError or AttributeError
            metrics.total_trades = 1000


# ============================================================================
# DERIVED PROPERTIES TESTS
# ============================================================================

class TestDerivedProperties:
    """Test derived properties (gross_profit, gross_loss, is_profitable)"""

    def test_gross_profit_profitable_strategy(self, profitable_metrics_data):
        """Should calculate gross profit correctly for profitable strategy"""
        metrics = MetricsReport(**profitable_metrics_data)
        
        # For profitable strategy: gross_profit = total_pnl_points + gross_loss
        # gross_loss = abs(largest_loss) for simplicity in calculation
        expected_gross_profit = 5000.0 + 50.0  # total_pnl + abs(largest_loss)
        
        assert metrics.gross_profit == pytest.approx(expected_gross_profit, rel=1e-2)
        assert metrics.gross_profit > 0

    def test_gross_loss_profitable_strategy(self, profitable_metrics_data):
        """Should calculate gross loss correctly for profitable strategy"""
        metrics = MetricsReport(**profitable_metrics_data)
        
        # gross_loss = gross_profit / profit_factor
        expected_gross_loss = (5000.0 + 50.0) / 2.5
        
        assert metrics.gross_loss == pytest.approx(expected_gross_loss, rel=1e-2)
        assert metrics.gross_loss > 0

    def test_gross_profit_losing_strategy(self, sample_metrics_data):
        """Should calculate gross profit correctly for losing strategy"""
        metrics = MetricsReport(**sample_metrics_data)
        
        # For losing strategy: gross_profit = abs(total_pnl_points) * profit_factor
        expected_gross_profit = abs(-2998.05) * 0.81
        
        assert metrics.gross_profit == pytest.approx(expected_gross_profit, rel=1e-2)

    def test_gross_loss_losing_strategy(self, sample_metrics_data):
        """Should calculate gross loss correctly for losing strategy"""
        metrics = MetricsReport(**sample_metrics_data)
        
        # gross_loss should equal abs(total_pnl_points)
        assert metrics.gross_loss == pytest.approx(abs(-2998.05), rel=1e-2)

    def test_zero_profit_factor_handling(self, zero_trades_data):
        """Should handle profit_factor = 0 gracefully"""
        metrics = MetricsReport(**zero_trades_data)
        
        assert metrics.gross_profit == 0.0
        assert metrics.gross_loss == 0.0

    def test_is_profitable_profitable_strategy(self, profitable_metrics_data):
        """Should return True for profitable strategy"""
        metrics = MetricsReport(**profitable_metrics_data)
        assert metrics.is_profitable is True

    def test_is_profitable_losing_strategy(self, sample_metrics_data):
        """Should return False for losing strategy"""
        metrics = MetricsReport(**sample_metrics_data)
        assert metrics.is_profitable is False

    def test_is_profitable_breakeven_strategy(self):
        """Should return False for breakeven strategy"""
        data = {
            "total_trades": 100,
            "winning_trades": 50,
            "losing_trades": 50,
            "win_rate": 50.0,
            "total_pnl_points": 0.0,
            "expectancy_points": 0.0,
            "profit_factor": 1.0,
            "avg_pnl_points": 0.0,
            "largest_win": 100.0,
            "largest_loss": -100.0,
            "max_drawdown": -500.0,
            "losing_streak": 5,
            "winning_streak": 5,
            "trades_per_week": 10.0,
            "trades_per_day": 2.0,
            "execution_duration_ms": 500.0,
        }
        metrics = MetricsReport(**data)
        assert metrics.is_profitable is False  # Not > 0


# ============================================================================
# SERIALIZATION TESTS
# ============================================================================

class TestSerialization:
    """Test serialization methods (to_dict, to_json, to_flat_dict)"""

    def test_to_dict_format(self, sample_metrics_data):
        """Should produce dict matching backtester format"""
        metrics = MetricsReport(**sample_metrics_data)
        result = metrics.to_dict()
        
        # Check structure
        assert "simulation_results" in result
        assert "performance_metrics" in result["simulation_results"]
        assert "trade_summary" in result["simulation_results"]
        assert "execution_date" in result
        assert "execution_duration" in result
        
        # Check performance metrics (values should be rounded)
        perf = result["simulation_results"]["performance_metrics"]
        assert perf["total_trades"] == 1151
        assert perf["win_rate"] == 16.85  # Already 2 decimals
        assert isinstance(perf["win_rate"], float)
        
        # Check trade summary
        summary = result["simulation_results"]["trade_summary"]
        assert summary["trades_per_week"] == 56.11
        assert summary["trades_per_day"] == 12.24
        
        # Check execution duration format
        assert "ms" in result["execution_duration"]
        assert "2765.23ms" in result["execution_duration"]

    def test_to_dict_rounding(self):
        """Should round floating point values to 2 decimals"""
        data = {
            "total_trades": 100,
            "winning_trades": 60,
            "losing_trades": 40,
            "win_rate": 60.12345,
            "total_pnl_points": 1234.56789,
            "expectancy_points": 12.34567,
            "profit_factor": 2.34567,
            "avg_pnl_points": 12.34567,
            "largest_win": 234.56789,
            "largest_loss": -123.45678,
            "max_drawdown": -567.89012,
            "losing_streak": 5,
            "winning_streak": 10,
            "trades_per_week": 25.6789,
            "trades_per_day": 5.12345,
            "execution_duration_ms": 1234.56789,
        }
        metrics = MetricsReport(**data)
        result = metrics.to_dict()
        
        perf = result["simulation_results"]["performance_metrics"]
        assert perf["win_rate"] == 60.12
        assert perf["total_pnl_points"] == 1234.57
        assert perf["expectancy_points"] == 12.35
        assert perf["profit_factor"] == 2.35
        assert perf["largest_win"] == 234.57
        assert perf["largest_loss"] == -123.46
        assert perf["max_drawdown"] == -567.89
        
        summary = result["simulation_results"]["trade_summary"]
        assert summary["trades_per_week"] == 25.68
        assert summary["trades_per_day"] == 5.12

    def test_to_json(self, sample_metrics_data):
        """Should produce valid JSON string"""
        metrics = MetricsReport(**sample_metrics_data)
        json_str = metrics.to_json()
        
        # Should be valid JSON
        parsed = json.loads(json_str)
        assert "simulation_results" in parsed
        
        # Test with indent
        json_pretty = metrics.to_json(indent=2)
        assert "\n" in json_pretty
        assert "  " in json_pretty

    def test_to_flat_dict(self, sample_metrics_data):
        """Should produce flat dict with all fields"""
        metrics = MetricsReport(**sample_metrics_data)
        flat = metrics.to_flat_dict()
        
        # Should have all fields at top level
        expected_fields = [
            "total_trades", "winning_trades", "losing_trades", "win_rate",
            "total_pnl_points", "expectancy_points", "profit_factor",
            "avg_pnl_points", "largest_win", "largest_loss", "max_drawdown",
            "losing_streak", "winning_streak", "trades_per_week", "trades_per_day",
            "execution_date", "execution_duration_ms"
        ]
        
        for field in expected_fields:
            assert field in flat
        
        # Values should match original (not rounded)
        assert flat["win_rate"] == 16.85
        assert flat["trades_per_week"] == 56.11

# ============================================================================
# STRING REPRESENTATION TESTS
# ============================================================================

class TestStringRepresentation:
    """Test __str__ method"""

    def test_str_contains_key_metrics(self, sample_metrics_data):
        """String representation should contain key metrics"""
        metrics = MetricsReport(**sample_metrics_data)
        str_repr = str(metrics)
        
        # Should contain main metrics
        assert "Trades: 1151" in str_repr
        assert "194W / 957L" in str_repr
        # Win rate is rounded to 1 decimal (16.85% → 16.9%)
        assert "Win Rate: 16.9%" in str_repr
        assert "Total P&L: -2998.05 points" in str_repr
        assert "Profit Factor: 0.81" in str_repr
        assert "Max Drawdown: -3383.85 points" in str_repr
        assert "Largest Win: +159.08" in str_repr
        assert "Loss: -62.06" in str_repr
        assert "Streaks: 5W / 41L" in str_repr
        assert "Frequency: 12.2/day, 56.1/week" in str_repr
        assert "Duration: 2765.23ms" in str_repr

    def test_str_zero_trades(self, zero_trades_data):
        """String representation for zero trades should work"""
        metrics = MetricsReport(**zero_trades_data)
        str_repr = str(metrics)
        
        assert "Trades: 0" in str_repr
        assert "Win Rate: 0.0%" in str_repr
        
# ============================================================================
# EMPTY METRICS REPORT TESTS
# ============================================================================

class TestEmptyMetricsReport:
    """Test create_empty_metrics_report helper"""

    def test_create_empty_with_duration(self):
        """Should create empty report with specified duration"""
        metrics = create_empty_metrics_report(execution_duration_ms=42.5)
        
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
        assert metrics.execution_duration_ms == 42.5

    def test_create_empty_default_duration(self):
        """Should create empty report with default duration 0"""
        metrics = create_empty_metrics_report()
        assert metrics.execution_duration_ms == 0.0

    def test_empty_report_validates(self):
        """Empty report should pass validation"""
        metrics = create_empty_metrics_report()
        
        # Should not raise any validation errors
        assert metrics.total_trades >= 0
        assert 0 <= metrics.win_rate <= 100
        assert metrics.profit_factor >= 0


# ============================================================================
# EDGE CASES AND BOUNDARY TESTS
# ============================================================================

class TestEdgeCases:
    """Test edge cases and boundary conditions"""

    def test_win_rate_boundaries(self):
        """Should accept win_rate = 0 and win_rate = 100"""
        data = {
            "total_trades": 100,
            "winning_trades": 0,
            "losing_trades": 100,
            "win_rate": 0.0,
            "total_pnl_points": -5000.0,
            "expectancy_points": -50.0,
            "profit_factor": 0.0,
            "avg_pnl_points": -50.0,
            "largest_win": 0.0,
            "largest_loss": -200.0,
            "max_drawdown": -5000.0,
            "losing_streak": 100,
            "winning_streak": 0,
            "trades_per_week": 10.0,
            "trades_per_day": 2.0,
            "execution_duration_ms": 100.0,
        }
        metrics = MetricsReport(**data)
        assert metrics.win_rate == 0.0
        
        data["winning_trades"] = 100
        data["losing_trades"] = 0
        data["win_rate"] = 100.0
        data["total_pnl_points"] = 5000.0
        data["largest_loss"] = 0.0
        data["losing_streak"] = 0
        data["winning_streak"] = 100
        
        metrics2 = MetricsReport(**data)
        assert metrics2.win_rate == 100.0

    def test_profit_factor_zero(self):
        """Should handle profit_factor = 0 (all losses)"""
        data = {
            "total_trades": 100,
            "winning_trades": 0,
            "losing_trades": 100,
            "win_rate": 0.0,
            "total_pnl_points": -5000.0,
            "expectancy_points": -50.0,
            "profit_factor": 0.0,
            "avg_pnl_points": -50.0,
            "largest_win": 0.0,
            "largest_loss": -200.0,
            "max_drawdown": -5000.0,
            "losing_streak": 100,
            "winning_streak": 0,
            "trades_per_week": 10.0,
            "trades_per_day": 2.0,
            "execution_duration_ms": 100.0,
        }
        metrics = MetricsReport(**data)
        assert metrics.profit_factor == 0.0
        assert metrics.gross_profit == 0.0
        assert metrics.gross_loss == 5000.0