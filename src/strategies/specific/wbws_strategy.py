"""
WBWS Strategy — v1.4
Step 4: TradeManager integration
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional

import pandas as pd

from src.strategies.trade_management.trade_manager import TradeManager
from src.strategies.trade_management.trade_decision import TradeDecision
from src.strategies.trade_management.trade_parameters import TradeParameters
from src.strategies.trade_management.trade_direction import TradeDirection
from src.strategies.trade_management.decision_type import DecisionType
from src.strategies.trade_management.signal_frame import SignalFrame
from src.strategies.trade_management.risk_manager import RiskManager


@dataclass
class WBWSStrategy:
    risk_manager: RiskManager
    trade_manager: TradeManager
    state: Dict[str, Any] = field(default_factory=dict)
    risk_pct: float = 0.01
    tag: str = "wbws"

    def on_bar(self, sf: SignalFrame) -> TradeDecision:
        self.state["last_close"] = sf.close

        raw_signal = sf.get_state("raw_signal")
        filtered_signal = sf.get_state("filtered_signal")

        if filtered_signal is None or filtered_signal is pd.NA:
            return TradeDecision(decision_type=DecisionType.NONE, reason="no_filtered_signal")

        # Convert filtered signal → intent
        if filtered_signal == "BUY":
            intent = self._intent_open_long(sf, raw_signal, filtered_signal)
        elif filtered_signal == "SELL":
            intent = self._intent_open_short(sf, raw_signal, filtered_signal)
        else:
            intent = TradeDecision(decision_type=DecisionType.NONE, reason="filtered_none")

        # Pass intent to TradeManager
        final_decision = self.trade_manager.process(intent, sf)
        return final_decision

    # ------------------------------------------------------------------ #
    # Intent builders (Strategy-level)
    # ------------------------------------------------------------------ #
    def _intent_open_long(self, sf, raw_signal, filtered_signal):
        params_dict = self.risk_manager.compute_trade_parameters(
            timestamp=sf.timestamp,
            bid_price=sf.close,
            is_long=True,
        )

        if params_dict is None:
            return TradeDecision(decision_type=DecisionType.NONE, reason="risk_rejected_long")

        trade_params = self._build_trade_parameters(
            sf=sf,
            is_long=True,
            params_dict=params_dict,
            raw_signal=raw_signal,
            filtered_signal=filtered_signal,
        )

        return TradeDecision(
            decision_type=DecisionType.OPEN,
            trade_params=trade_params,
            reason=params_dict.get("comment", "long_signal"),
        )

    def _intent_open_short(self, sf, raw_signal, filtered_signal):
        params_dict = self.risk_manager.compute_trade_parameters(
            timestamp=sf.timestamp,
            bid_price=sf.close,
            is_long=False,
        )

        if params_dict is None:
            return TradeDecision(decision_type=DecisionType.NONE, reason="risk_rejected_short")

        trade_params = self._build_trade_parameters(
            sf=sf,
            is_long=False,
            params_dict=params_dict,
            raw_signal=raw_signal,
            filtered_signal=filtered_signal,
        )

        return TradeDecision(
            decision_type=DecisionType.OPEN,
            trade_params=trade_params,
            reason=params_dict.get("comment", "short_signal"),
        )

    # ------------------------------------------------------------------ #
    # Adapter: RiskManager dict → TradeParameters
    # ------------------------------------------------------------------ #
    def _build_trade_parameters(self, sf, is_long, params_dict, raw_signal, filtered_signal):
        direction = TradeDirection.LONG if is_long else TradeDirection.SHORT

        entry = params_dict["executed_entry"]
        stop_loss = params_dict["trigger_sl"]
        take_profit = params_dict["tp"]

        risk_per_unit = abs(entry - stop_loss)
        if risk_per_unit == 0:
            size = 0.0
        else:
            equity = self.state.get("equity", 100_000.0)
            risk_amount = equity * self.risk_pct
            size = risk_amount / risk_per_unit

        meta = {
            "raw_signal": raw_signal,
            "filtered_signal": filtered_signal,
            "risk_comment": params_dict.get("comment"),
            "sl_adjusted": params_dict.get("sl_adjusted", False),
            "spread_applied": params_dict.get("spread_applied", False),
            "spread_value": params_dict.get("spread_value", 0.0),
        }

        return TradeParameters(
            direction=direction,
            entry=entry,
            stop_loss=stop_loss,
            take_profit=take_profit,
            size=size,
            risk_pct=self.risk_pct,
            tag=self.tag,
            meta=meta,
        )