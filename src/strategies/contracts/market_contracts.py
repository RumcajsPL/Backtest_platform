"""
Market Contracts - Price Data Frames
Version: 1.0.0
This contract represents market price data for a single bar, including OHLCV
and optional higher/lower timeframe data.
"""
from dataclasses import dataclass, field
from typing import Dict, Any, Optional
import pandas as pd

__all__ = ['MarketFrame']

@dataclass(frozen=True)
class MarketFrame:
    """
    Market price data for a single bar.
    
    Represents OHLCV data at a specific timestamp, with optional
    higher and lower timeframe context.
    
    This replaces the old SignalFrame from trade_management to avoid
    naming conflict with Phase 2 SignalFrame (signal generation).
    """
    # Core OHLCV data
    timestamp: pd.Timestamp
    open: float
    high: float
    low: float
    close: float
    volume: float
    
    # Multi-timeframe data (optional)
    htf: Optional[pd.Series] = None             # Higher timeframe data (e.g., 1H bar)
    ltf: Optional[pd.DataFrame] = None          # Lower timeframe data (e.g., 1s bars)
    
    # Computed indicators (optional)
    indicators: Dict[str, Any] = field(default_factory=dict)
    
    # State/metadata (optional)
    state: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate OHLC relationships"""
        if not (self.low <= self.open <= self.high):
            raise ValueError(f"Invalid OHLC: open={self.open} not in range [{self.low}, {self.high}]")
        if not (self.low <= self.close <= self.high):
            raise ValueError(f"Invalid OHLC: close={self.close} not in range [{self.low}, {self.high}]")
        if self.volume < 0:
            raise ValueError(f"Volume cannot be negative: {self.volume}")
    
    @property
    def price_range(self) -> float:
        """Get bar's price range (high - low)"""
        return self.high - self.low
    
    @property
    def body_size(self) -> float:
        """Get candle body size (abs(close - open))"""
        return abs(self.close - self.open)
    
    @property
    def is_bullish(self) -> bool:
        """Is this a bullish bar? (close > open)"""
        return self.close > self.open
    
    @property
    def is_bearish(self) -> bool:
        """Is this a bearish bar? (close < open)"""
        return self.close < self.open
    
    @property
    def is_doji(self) -> bool:
        """Is this a doji bar? (close == open)"""
        return self.close == self.open
    
    @property
    def upper_wick(self) -> float:
        """Get upper wick size"""
        return self.high - max(self.open, self.close)
    
    @property
    def lower_wick(self) -> float:
        """Get lower wick size"""
        return min(self.open, self.close) - self.low
    
    @property
    def has_htf(self) -> bool:
        """Does this frame have higher timeframe data?"""
        return self.htf is not None and not self.htf.empty
    
    @property
    def has_ltf(self) -> bool:
        """Does this frame have lower timeframe data?"""
        return self.ltf is not None and not self.ltf.empty
    
    @classmethod
    def from_series(
        cls,
        series: pd.Series,
        htf: Optional[pd.Series] = None,
        ltf: Optional[pd.DataFrame] = None,
        indicators: Optional[Dict[str, Any]] = None,
        state: Optional[Dict[str, Any]] = None,
    ) -> 'MarketFrame':
        """
        Create MarketFrame from pandas Series.
        
        Args:
            series: Series with OHLCV data (must have 'open', 'high', 'low', 'close', 'volume')
            htf: Optional higher timeframe series
            ltf: Optional lower timeframe DataFrame
            indicators: Optional indicators dict
            state: Optional state dict
        """
        if not isinstance(series.name, pd.Timestamp):
            raise ValueError("Series must have a Timestamp as name/index")
        
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        missing = [col for col in required_cols if col not in series.index]
        if missing:
            raise ValueError(f"Series missing required columns: {missing}")
        
        return cls(
            timestamp=series.name,
            open=float(series['open']),
            high=float(series['high']),
            low=float(series['low']),
            close=float(series['close']),
            volume=float(series['volume']),
            htf=htf,
            ltf=ltf,
            indicators=indicators or {},
            state=state or {},
        )
    
    @classmethod
    def from_dataframe_row(
        cls,
        df: pd.DataFrame,
        timestamp: pd.Timestamp,
        htf: Optional[pd.Series] = None,
        ltf: Optional[pd.DataFrame] = None,
        indicators: Optional[Dict[str, Any]] = None,
        state: Optional[Dict[str, Any]] = None,
    ) -> 'MarketFrame':
        """
        Create MarketFrame from DataFrame row.
        
        Args:
            df: DataFrame with OHLCV data
            timestamp: Timestamp of the row to extract
            htf: Optional higher timeframe series
            ltf: Optional lower timeframe DataFrame
            indicators: Optional indicators dict
            state: Optional state dict
        """
        if timestamp not in df.index:
            raise ValueError(f"Timestamp {timestamp} not found in DataFrame")
        
        row = df.loc[timestamp]
        return cls.from_series(row, htf, ltf, indicators, state)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict"""
        return {
            'timestamp': self.timestamp,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'price_range': self.price_range,
            'body_size': self.body_size,
            'is_bullish': self.is_bullish,
            'is_bearish': self.is_bearish,
            'upper_wick': self.upper_wick,
            'lower_wick': self.lower_wick,
            'has_htf': self.has_htf,
            'has_ltf': self.has_ltf,
        }
    
    def __str__(self) -> str:
        """String representation"""
        direction = "↑" if self.is_bullish else "↓" if self.is_bearish else "→"
        return (
            f"MarketFrame({self.timestamp}, {direction} "
            f"O:{self.open:.2f} H:{self.high:.2f} L:{self.low:.2f} C:{self.close:.2f})"
        )