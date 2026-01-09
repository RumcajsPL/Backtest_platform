import numpy as np
from itertools import product

class ParameterSpace:
    def __init__(self, zone_config: dict):
        self.zone_config = zone_config["optimization"]

    def expand_range(self, low, high, step=1):
        return list(np.arange(low, high + step, step))

    def build(self):
        space = {}

        # RSI
        rsi = self.zone_config.get("rsi", {})
        if rsi:
            space["rsi_overbought"] = self.expand_range(
                rsi["overbought"][0], rsi["overbought"][1], rsi.get("step", 1)
            )
            space["rsi_oversold"] = self.expand_range(
                rsi["oversold"][0], rsi["oversold"][1], rsi.get("step", 1)
            )

        # HTF
        htf = self.zone_config.get("htf", {})
        if htf:
            space["htf_timeframe"] = htf["timeframe"]

        # ATR
        atr = self.zone_config.get("atr", {})
        if atr:
            space["atr_length"] = self.expand_range(atr["length"][0], atr["length"][1], 1)
            space["atr_multiplier"] = self.expand_range(atr["multiplier"][0], atr["multiplier"][1], 0.1)

        # Risk
        risk = self.zone_config.get("risk", {})
        if risk:
            space["max_risk_percentile"] = self.expand_range(
                risk["max_risk_percentile"][0],
                risk["max_risk_percentile"][1],
                0.01
            )

        # RR
        if "rr_target" in self.zone_config:
            space["rr_target"] = self.expand_range(
                self.zone_config["rr_target"][0],
                self.zone_config["rr_target"][1],
                0.25
            )

        # Session windows
        session = self.zone_config.get("session", {})
        if session:
            space["session_window"] = session["windows"]

        return space