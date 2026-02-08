"""
WBWS Strategy — Phase 1 Refactor
This class is the template for all future strategies.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional

from strategies.trade_management import (
    SignalFrame,
    TradeDecision,
    TradeParameters,
    TradeDirection,
    DecisionType,
)

@dataclass
class WBWSStrategy:
    """
    WBWS Strategy using the new architecture.
    This class is the canonical template for all future strategies.

    Responsibilities:
    - Maintain internal state
    - Read indicators from SignalFrame
    - Produce TradeDecision objects
    """

    # Persistent strategy state (rolling buffers, last signals, etc.)
    state: Dict[str, Any] = field(default_factory=dict)

    # Strategy parameters (example — adjust to your real parameters)
    risk_pct: float = 0.01
    tag: str = "wbws"

    # ------------------------------------------------------------------
    # Main strategy entry point
    # ------------------------------------------------------------------
    def on_bar(self, sf: SignalFrame) -> TradeDecision:
        """
        Called once per bar by the runner.
        Must return a TradeDecision object.
        """

        # 1. Read indicators
        # Example — replace with your real indicator names
        ema_fast = sf.get_indicator("ema_fast")
        ema_slow = sf.get_indicator("ema_slow")
        rsi = sf.get_indicator("rsi")

        # 2. Update internal state if needed
        # Example: store last close
        self.state["last_close"] = sf.close

        # 3. Generate signals (placeholder logic)
        # Replace with your real WBWS logic
        if ema_fast is None or ema_slow is None:
            return TradeDecision(decision_type=DecisionType.NONE)

        # Example: simple crossover logic (placeholder)
        if ema_fast > ema_slow:
            return self._open_long(sf)

        if ema_fast < ema_slow:
            return self._open_short(sf)

        # Default: do nothing
        return TradeDecision(decision_type=DecisionType.NONE)

    # ------------------------------------------------------------------
    # Trade construction helpers
    # ------------------------------------------------------------------
    def _open_long(self, sf: SignalFrame) -> TradeDecision:
        """
        Example long entry — replace with your real WBWS logic.
        """

        entry = sf.close
        stop_loss = entry * 0.99
        take_profit = entry * 1.02
        size = self._compute_size(entry, stop_loss)

        params = TradeParameters(
            direction=TradeDirection.LONG,
            entry=entry,
            stop_loss=stop_loss,
            take_profit=take_profit,
            size=size,
            risk_pct=self.risk_pct,
            tag=self.tag,
            meta={"reason_open": "long_signal"},
        )

        return TradeDecision(
            decision_type=DecisionType.OPEN,
            trade_params=params,
            reason="long_signal",
        )

    def _open_short(self, sf: SignalFrame) -> TradeDecision:
        """
        Example short entry — replace with your real WBWS logic.
        """

        entry = sf.close
        stop_loss = entry * 1.01
        take_profit = entry * 0.98
        size = self._compute_size(entry, stop_loss)

        params = TradeParameters(
            direction=TradeDirection.SHORT,
            entry=entry,
            stop_loss=stop_loss,
            take_profit=take_profit,
            size=size,
            risk_pct=self.risk_pct,
            tag=self.tag,
            meta={"reason_open": "short_signal"},
        )

        return TradeDecision(
            decision_type=DecisionType.OPEN,
            trade_params=params,
            reason="short_signal",
        )

    # ------------------------------------------------------------------
    # Risk / position sizing
    # ------------------------------------------------------------------
    def _compute_size(self, entry: float, stop_loss: float) -> float:
        """
        Example position sizing — replace with your real risk model.
        """
        risk_per_unit = abs(entry - stop_loss)
        if risk_per_unit == 0:
            return 0.0

        # Example: fixed risk percentage of equity (placeholder)
        equity = self.state.get("equity", 100_000.0)
        risk_amount = equity * self.risk_pct
        size = risk_amount / risk_per_unit

        return size