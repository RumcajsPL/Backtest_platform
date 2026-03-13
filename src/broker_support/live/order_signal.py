"""
OrderSignal — typed contract output of SignalBridge.

Represents a fully validated, execution-ready trade signal with
all prices computed from the live strategy pipeline.

This is the bridge between the strategy world (SignalFrame, TradeParameters)
and the execution world (OrderRouter.open_position).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import pandas as pd


@dataclass(frozen=True)
class OrderSignal:
    """
    Execution-ready signal produced by SignalBridge.

    All price fields are absolute levels suitable for direct use
    in OrderRouter.open_position(stop_loss_rate, take_profit_rate).

    Fields
    ------
    timestamp        : Bar timestamp that triggered the signal (UTC).
    symbol           : Instrument key (e.g. 'DAX').
    direction        : 'BUY' (long) or 'SELL' (short).
    entry_price_mid  : Mid-price at signal bar (bid price from strategy).
    stop_loss_rate   : Absolute SL price level (trigger price, spread-adjusted).
    take_profit_rate : Absolute TP price level (trigger price, spread-adjusted).
    atr_value        : ATR at signal bar (for position sizing reference).
    sl_distance      : SL distance in points.
    tp_distance      : TP distance in points.
    risk_reward_ratio: R:R ratio computed by RiskManager.
    candidate_id     : Strategy candidate_id from YAML metadata.
    wbws_window_valid: True if signal passed the WBWS+ hour filter.
    meta             : Additional context (spread info, ATR multiplier, etc.).
    """
    timestamp:         pd.Timestamp
    symbol:            str
    direction:         str           # 'BUY' | 'SELL'
    entry_price_mid:   float
    stop_loss_rate:    float
    take_profit_rate:  float
    atr_value:         float
    sl_distance:       float
    tp_distance:       float
    risk_reward_ratio: float
    candidate_id:      str
    wbws_window_valid: bool
    max_positions:     int              # from strategy YAML position_control.max_positions
    meta:              Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.direction not in ("BUY", "SELL"):
            raise ValueError(
                f"OrderSignal.direction must be 'BUY' or 'SELL', got {self.direction!r}"
            )
        if self.entry_price_mid <= 0:
            raise ValueError(
                f"OrderSignal.entry_price_mid must be positive, got {self.entry_price_mid}"
            )
        if self.stop_loss_rate <= 0:
            raise ValueError(
                f"OrderSignal.stop_loss_rate must be positive, got {self.stop_loss_rate}"
            )
        if self.take_profit_rate <= 0:
            raise ValueError(
                f"OrderSignal.take_profit_rate must be positive, got {self.take_profit_rate}"
            )

    @property
    def is_long(self) -> bool:
        return self.direction == "BUY"

    @property
    def is_short(self) -> bool:
        return self.direction == "SELL"

    def summary(self) -> str:
        """One-line summary for logging."""
        window_tag = "✅ WBWS+" if self.wbws_window_valid else "⚠️ outside WBWS+"
        return (
            f"OrderSignal | {self.direction} {self.symbol} | "
            f"ts={self.timestamp} | "
            f"entry={self.entry_price_mid:.2f} | "
            f"sl={self.stop_loss_rate:.2f} (dist={self.sl_distance:.2f}pts) | "
            f"tp={self.take_profit_rate:.2f} (dist={self.tp_distance:.2f}pts) | "
            f"rr={self.risk_reward_ratio:.1f}x | "
            f"atr={self.atr_value:.2f} | "
            f"{window_tag}"
        )
