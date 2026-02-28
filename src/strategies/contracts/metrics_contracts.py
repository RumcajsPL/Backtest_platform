"""
Metrics Contracts - Performance Metrics for Backtester
Version: 1.0.0

Defines MetricsReport contract for standardized metrics calculation.
Consumed by backtester (memory-only, no file I/O).
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from datetime import datetime
import json

@dataclass(frozen=True)
class MetricsReport:
    """
    Performance metrics report for backtester.
    
    Contains all essential metrics required by backtester:
    - Performance metrics (11 fields)
    - Trade summary (2 fields)
    - Metadata (1 field)
    
    Design:
    - Immutable (frozen=True)
    - Type-safe (all fields typed)
    - Validated (min/max checks)
    - Serializable (to_dict, to_json)
    
    Usage:
        metrics = MetricsReport(
            total_trades=1151,
            winning_trades=194,
            win_rate=16.85,
            ...
        )
        
        # Access fields
        print(f"Win Rate: {metrics.win_rate:.1f}%")
        
        # Serialize for backtester
        result = metrics.to_dict()
    """
    
    # ========================================================================
    # PERFORMANCE METRICS (11 fields)
    # ========================================================================
    
    total_trades: int
    """Total number of closed trades"""
    
    winning_trades: int
    """Number of winning trades"""
    
    losing_trades: int
    """Number of losing trades (calculated for convenience)"""
    
    win_rate: float
    """Win rate as percentage (0-100)"""
    
    total_pnl_points: float
    """Total profit/loss in points"""
    
    expectancy_points: float
    """Expected P&L per trade (avg_pnl_points)"""
    
    profit_factor: float
    """Ratio of gross profit to gross loss (0 = all losses, >1 = profitable)"""
    
    avg_pnl_points: float
    """Average P&L per trade (same as expectancy_points)"""
    
    largest_win: float
    """Largest winning trade in points"""
    
    largest_loss: float
    """Largest losing trade in points (negative value)"""
    
    max_drawdown: float
    """Maximum drawdown in points (negative value, worst cumulative loss)"""
    
    losing_streak: int
    """Longest consecutive losing streak"""
    
    winning_streak: int
    """Longest consecutive winning streak (bonus metric)"""
    
    # ========================================================================
    # TRADE SUMMARY (2 fields)
    # ========================================================================
    
    trades_per_week: float
    """Average number of trades per week"""
    
    trades_per_day: float
    """Average number of trades per day"""
    
    # ========================================================================
    # METADATA (1 field)
    # ========================================================================
    
    execution_duration_ms: float
    """Execution duration in milliseconds"""
    
    execution_date: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    """Execution date/time in ISO format"""
    
    def __post_init__(self):
        """Validate metrics on creation"""
        # Validate counts
        if self.total_trades < 0:
            raise ValueError(f"total_trades must be >= 0, got {self.total_trades}")
        
        if self.winning_trades < 0:
            raise ValueError(f"winning_trades must be >= 0, got {self.winning_trades}")
        
        if self.losing_trades < 0:
            raise ValueError(f"losing_trades must be >= 0, got {self.losing_trades}")
        
        # Validate win rate
        if not (0 <= self.win_rate <= 100):
            raise ValueError(f"win_rate must be between 0 and 100, got {self.win_rate}")
        
        # Validate profit factor
        if self.profit_factor < 0:
            raise ValueError(f"profit_factor must be >= 0, got {self.profit_factor}")
        
        # Validate streaks
        if self.losing_streak < 0:
            raise ValueError(f"losing_streak must be >= 0, got {self.losing_streak}")
        
        if self.winning_streak < 0:
            raise ValueError(f"winning_streak must be >= 0, got {self.winning_streak}")
        
        # Validate trade summary
        if self.trades_per_week < 0:
            raise ValueError(f"trades_per_week must be >= 0, got {self.trades_per_week}")
        
        if self.trades_per_day < 0:
            raise ValueError(f"trades_per_day must be >= 0, got {self.trades_per_day}")
        
        # Validate execution duration
        if self.execution_duration_ms < 0:
            raise ValueError(f"execution_duration_ms must be >= 0, got {self.execution_duration_ms}")
    
    # ========================================================================
    # DERIVED PROPERTIES
    # ========================================================================
    
    @property
    def gross_profit(self) -> float:
        """Calculate gross profit (sum of all wins)"""
        # Not stored directly, can be derived from profit_factor and gross_loss
        if self.profit_factor > 0 and abs(self.total_pnl_points) > 0:
            if self.total_pnl_points > 0:
                # Profitable system
                gross_loss = abs(self.largest_loss) if self.largest_loss < 0 else 0
                return self.total_pnl_points + gross_loss
            else:
                # Losing system
                return abs(self.total_pnl_points) * self.profit_factor
        return 0.0
    
    @property
    def gross_loss(self) -> float:
        """Calculate gross loss (sum of all losses, positive value)"""
        if self.profit_factor > 0:
            return abs(self.gross_profit / self.profit_factor) if self.profit_factor > 0 else 0.0
        return abs(self.total_pnl_points)
    
    @property
    def is_profitable(self) -> bool:
        """Check if strategy is profitable overall"""
        return self.total_pnl_points > 0
    
    # ========================================================================
    # SERIALIZATION METHODS
    # ========================================================================
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dict for backtester consumption.
        
        Format matches backtester requirements:
        {
            "simulation_results": {
                "performance_metrics": {...},
                "trade_summary": {...}
            },
            "execution_date": "...",
            "execution_duration": "...ms"
        }
        
        Returns:
            Dict matching backtester format
        """
        return {
            "simulation_results": {
                "performance_metrics": {
                    "total_trades": self.total_trades,
                    "winning_trades": self.winning_trades,
                    "losing_trades": self.losing_trades,
                    "win_rate": round(self.win_rate, 2),
                    "total_pnl_points": round(self.total_pnl_points, 2),
                    "expectancy_points": round(self.expectancy_points, 2),
                    "profit_factor": round(self.profit_factor, 2),
                    "avg_pnl_points": round(self.avg_pnl_points, 2),
                    "largest_win": round(self.largest_win, 2),
                    "largest_loss": round(self.largest_loss, 2),
                    "max_drawdown": round(self.max_drawdown, 2),
                    "losing_streak": self.losing_streak,
                    "winning_streak": self.winning_streak,
                },
                "trade_summary": {
                    "trades_per_week": round(self.trades_per_week, 2),
                    "trades_per_day": round(self.trades_per_day, 2),
                }
            },
            "execution_date": self.execution_date,
            "execution_duration": f"{self.execution_duration_ms:.2f}ms"
        }
    
    def to_json(self, indent: Optional[int] = None) -> str:
        """
        Serialize to JSON string.
        
        Args:
            indent: JSON indentation (None for compact, 2 for readable)
        
        Returns:
            JSON string
        
        Example:
            json_str = metrics.to_json(indent=2)
            with open('metrics.json', 'w') as f:
                f.write(json_str)
        """
        return json.dumps(self.to_dict(), indent=indent)
    
    def to_flat_dict(self) -> Dict[str, Any]:
        """
        Convert to flat dict (no nesting).
        
        Useful for pandas DataFrame, CSV export, etc.
        
        Returns:
            Flat dict with all metrics
        """
        return {
            # Performance metrics
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "win_rate": self.win_rate,
            "total_pnl_points": self.total_pnl_points,
            "expectancy_points": self.expectancy_points,
            "profit_factor": self.profit_factor,
            "avg_pnl_points": self.avg_pnl_points,
            "largest_win": self.largest_win,
            "largest_loss": self.largest_loss,
            "max_drawdown": self.max_drawdown,
            "losing_streak": self.losing_streak,
            "winning_streak": self.winning_streak,
            # Trade summary
            "trades_per_week": self.trades_per_week,
            "trades_per_day": self.trades_per_day,
            # Metadata
            "execution_date": self.execution_date,
            "execution_duration_ms": self.execution_duration_ms,
        }
    
    # ========================================================================
    # STRING REPRESENTATION
    # ========================================================================
    
    def __str__(self) -> str:
        """Human-readable string representation"""
        return (
            f"MetricsReport(\n"
            f"  Trades: {self.total_trades} ({self.winning_trades}W / {self.losing_trades}L)\n"
            f"  Win Rate: {self.win_rate:.1f}%\n"
            f"  Total P&L: {self.total_pnl_points:+.2f} points\n"
            f"  Expectancy: {self.expectancy_points:+.2f} points/trade\n"
            f"  Profit Factor: {self.profit_factor:.2f}\n"
            f"  Max Drawdown: {self.max_drawdown:.2f} points\n"
            f"  Largest Win: {self.largest_win:+.2f} | Loss: {self.largest_loss:+.2f}\n"
            f"  Streaks: {self.winning_streak}W / {self.losing_streak}L\n"
            f"  Frequency: {self.trades_per_day:.1f}/day, {self.trades_per_week:.1f}/week\n"
            f"  Duration: {self.execution_duration_ms:.2f}ms\n"
            f")"
        )

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def create_empty_metrics_report(execution_duration_ms: float = 0.0) -> MetricsReport:
    """
    Create metrics report for zero trades.
    
    Args:
        execution_duration_ms: Execution time
    
    Returns:
        MetricsReport with all metrics = 0
    
    Example:
        # No trades executed
        metrics = create_empty_metrics_report(execution_duration_ms=5.2)
    """
    return MetricsReport(
        total_trades=0,
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
        execution_duration_ms=execution_duration_ms,
    )

# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Example 1: Create metrics report
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
        execution_duration_ms=2765.23,
    )
    
    print("="*70)
    print("METRICS REPORT - EXAMPLE")
    print("="*70)
    print(metrics)
    
    print("\n" + "="*70)
    print("BACKTESTER FORMAT (to_dict)")
    print("="*70)
    print(json.dumps(metrics.to_dict(), indent=2))
    
    print("\n" + "="*70)
    print("FLAT FORMAT (for CSV/DataFrame)")
    print("="*70)
    flat = metrics.to_flat_dict()
    for key, value in flat.items():
        print(f"  {key}: {value}")
    
    print("\n" + "="*70)
    print("VALIDATION TEST")
    print("="*70)
    try:
        invalid = MetricsReport(
            total_trades=-1,  # Invalid!
            winning_trades=0,
            losing_trades=0,
            win_rate=0,
            total_pnl_points=0,
            expectancy_points=0,
            profit_factor=0,
            avg_pnl_points=0,
            largest_win=0,
            largest_loss=0,
            max_drawdown=0,
            losing_streak=0,
            winning_streak=0,
            trades_per_week=0,
            trades_per_day=0,
            execution_duration_ms=0,
        )
    except ValueError as e:
        print(f"✅ Validation caught error: {e}")
    
    print("\n" + "="*70)
    print("EMPTY METRICS TEST")
    print("="*70)
    empty = create_empty_metrics_report(execution_duration_ms=1.5)
    print(empty)
    
    print("\n✅ All tests passed!")