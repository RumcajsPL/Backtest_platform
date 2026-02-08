from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from enum import Enum, auto

import pandas as pd

# === CORE ENUMS & CONTRACTS ======================================

class TradeDirection(Enum):
    LONG = 1
    SHORT = -1

@dataclass(frozen=True)
class TradeParameters:
    direction: TradeDirection
    entry: float
    stop_loss: float
    take_profit: float
    size: float

    risk_pct: Optional[float] = None
    tag: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    def validate(self):
        if self.entry <= 0:
            raise ValueError("Entry price must be positive")
        if self.size <= 0:
            raise ValueError("Trade size must be positive")

        if self.direction == TradeDirection.LONG:
            if not (self.stop_loss < self.entry < self.take_profit):
                raise ValueError("Invalid SL/TP for LONG trade")
        elif self.direction == TradeDirection.SHORT:
            if not (self.take_profit < self.entry < self.stop_loss):
                raise ValueError("Invalid SL/TP for SHORT trade")

class DecisionType(Enum):
    NONE = auto()
    OPEN = auto()
    CLOSE = auto()
    REVERSE = auto()
    MODIFY = auto()  # reserved for future use

@dataclass(frozen=True)
class TradeDecision:
    decision_type: DecisionType
    trade_params: Optional[TradeParameters] = None

    confidence: Optional[float] = None
    reason: Optional[str] = None
    tags: Dict[str, Any] = field(default_factory=dict)

    def validate(self):
        if self.decision_type in (DecisionType.OPEN, DecisionType.REVERSE):
            if self.trade_params is None:
                raise ValueError(f"{self.decision_type.name} requires trade_params")
            self.trade_params.validate()

@dataclass(frozen=True)
class Position:
    direction: TradeDirection
    entry: float
    stop_loss: float
    take_profit: float
    size: float
    open_time: pd.Timestamp
    meta: Dict[str, Any] = field(default_factory=dict)

    def is_long(self) -> bool:
        return self.direction == TradeDirection.LONG

    def is_short(self) -> bool:
        return self.direction == TradeDirection.SHORT

@dataclass(frozen=True)
class TradeRecord:
    direction: TradeDirection
    entry: float
    exit: float
    size: float

    open_time: pd.Timestamp
    close_time: pd.Timestamp

    pnl: float
    pnl_pct: float

    reason_open: str
    reason_close: str

    meta: Dict[str, Any] = field(default_factory=dict)

@dataclass(frozen=True)
class SignalFrame:
    timestamp: pd.Timestamp
    open: float
    high: float
    low: float
    close: float
    volume: float

    htf: Optional[pd.Series] = None
    ltf: Optional[pd.DataFrame] = None

    indicators: Dict[str, Any] = field(default_factory=dict)
    state: Dict[str, Any] = field(default_factory=dict)

    def get_indicator(self, name: str, default=None):
        return self.indicators.get(name, default)

    def get_state(self, key: str, default=None):
        return self.state.get(key, default)

# === SIMULATOR CORE ==============================================

class Simulator:
    """
    Simulator Core v1.0

    - Consumes SignalFrame + TradeDecision
    - Manages a single Position
    - Produces TradeRecord list
    """

    def __init__(self, initial_equity: float = 100_000.0):
        self.initial_equity = float(initial_equity)
        self.equity = float(initial_equity)

        self.position: Optional[Position] = None
        self.trade_log: List[TradeRecord] = []
        self.bar_index: int = 0

    # --- PUBLIC API ------------------------------------------------

    def process_bar(self, sf: SignalFrame, decision: TradeDecision):
        """
        Main per-bar entry point.

        Caller is responsible for:
        - building SignalFrame
        - calling strategy to get TradeDecision
        """
        self.bar_index += 1
        decision.validate()

        # 1) Check SL/TP first (position may close regardless of decision)
        if self.position is not None:
            closed = self._check_exit_conditions(sf)
            if closed:
                # position is now closed; decision may still request OPEN/REVERSE
                pass

        # 2) Apply decision
        if decision.decision_type == DecisionType.NONE:
            return

        if decision.decision_type == DecisionType.OPEN:
            self._handle_open(sf, decision)

        elif decision.decision_type == DecisionType.CLOSE:
            self._handle_close(sf, decision)

        elif decision.decision_type == DecisionType.REVERSE:
            self._handle_reverse(sf, decision)

        # MODIFY reserved for future use

    def finalize(self):
        """
        Called at the end of the backtest.
        If a position is still open, you may choose to close it at last price
        or leave it open. Here we leave it as-is for clarity.
        """
        return {
            "initial_equity": self.initial_equity,
            "final_equity": self.equity,
            "trades": self.trade_log,
        }

    # --- INTERNAL HELPERS ------------------------------------------

    def _handle_open(self, sf: SignalFrame, decision: TradeDecision):
        if self.position is not None:
            # Strategy tried to open while already in position.
            # For now we ignore; could also log a warning.
            return

        tp = decision.trade_params
        assert tp is not None

        pos = Position(
            direction=tp.direction,
            entry=tp.entry,
            stop_loss=tp.stop_loss,
            take_profit=tp.take_profit,
            size=tp.size,
            open_time=sf.timestamp,
            meta=tp.meta.copy(),
        )
        self.position = pos

    def _handle_close(self, sf: SignalFrame, decision: TradeDecision):
        if self.position is None:
            return

        # Close at current close price
        self._close_position(
            sf=sf,
            exit_price=sf.close,
            reason_close=decision.reason or "manual_close",
        )

    def _handle_reverse(self, sf: SignalFrame, decision: TradeDecision):
        tp = decision.trade_params
        assert tp is not None

        # If in position, close first
        if self.position is not None:
            self._close_position(
                sf=sf,
                exit_price=sf.close,
                reason_close=decision.reason or "reverse_close",
            )

        # Then open new position
        self._handle_open(sf, decision)

    def _check_exit_conditions(self, sf: SignalFrame) -> bool:
        """
        Check SL/TP against current bar.
        Returns True if position was closed.
        """
        if self.position is None:
            return False

        pos = self.position
        high = sf.high
        low = sf.low

        sl_hit = False
        tp_hit = False
        sl_price = pos.stop_loss
        tp_price = pos.take_profit

        if pos.is_long():
            if low <= sl_price:
                sl_hit = True
            if high >= tp_price:
                tp_hit = True
        else:  # short
            if high >= sl_price:
                sl_hit = True
            if low <= tp_price:
                tp_hit = True

        if not sl_hit and not tp_hit:
            return False

        # If both hit in same bar, we need a rule.
        # For now: assume SL has priority (more conservative).
        if sl_hit:
            exit_price = sl_price
            reason = "stop_loss"
        else:
            exit_price = tp_price
            reason = "take_profit"

        self._close_position(sf=sf, exit_price=exit_price, reason_close=reason)
        return True

    def _close_position(self, sf: SignalFrame, exit_price: float, reason_close: str):
        assert self.position is not None
        pos = self.position

        direction_factor = 1 if pos.is_long() else -1
        pnl = (exit_price - pos.entry) * pos.size * direction_factor
        pnl_pct = (exit_price / pos.entry - 1.0) * direction_factor

        record = TradeRecord(
            direction=pos.direction,
            entry=pos.entry,
            exit=exit_price,
            size=pos.size,
            open_time=pos.open_time,
            close_time=sf.timestamp,
            pnl=pnl,
            pnl_pct=pnl_pct,
            reason_open=pos.meta.get("reason_open", "strategy"),
            reason_close=reason_close,
            meta=pos.meta.copy(),
        )

        self.trade_log.append(record)
        self.equity += pnl
        self.position = None