import logging
import importlib
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import numpy as np
import pandas_ta_classic as pta

logger = logging.getLogger(__name__)

# Acronym-aware mapping: rsi_filter -> RSIFilter, macd_filter -> MACDFilter, etc.
ACRONYMS = {"rsi", "macd", "cci", "adx", "dpo", "ma"}


def key_to_classname(key: str) -> str:
    parts = key.split("_")
    class_name = ""
    for part in parts:
        if part in ACRONYMS:
            class_name += part.upper()
        else:
            class_name += part.capitalize()
    return class_name


class FilterPipeline:
    """Orchestrates filter application with pre-computation and configurable order."""

    def __init__(self, config: Dict):
        self.config = config
        self.filters_cfg: Dict = self.config.get("filters", {})
        self.filter_sequence: List[str] = config.get(
            "filter_sequence",
            [
                "rsi_filter",
                "choppiness_filter",
                "bollinger_filter",
                "adx_filter",
                "supertrend_filter",
                "ma_filter",
                "pivot_filter",
                "cci_filter",
                "macd_filter",
                "dpo_filter",
            ],
        )

        self.filters: Dict = {}
        self.indicators: Dict = {}
        self.progressive_tracker = None

        self._load_time_filter()
        self._load_technical_filters()
        self.filters["risk"] = None  # RiskManager initialized later

    # ------------------------------------------------------------------ #
    # Time filter (always first level)
    # ------------------------------------------------------------------ #
    def _load_time_filter(self):
        from src.strategies.filters.time_filter import TimeManager

        time_cfg = self.config.get("trade_management", {})
        self.filters["time"] = TimeManager(time_cfg)

    # ------------------------------------------------------------------ #
    # Dynamic technical filter loading
    # ------------------------------------------------------------------ #
    def _load_technical_filters(self):
        """
        Dynamically load all technical filter classes based on YAML keys.
        Example: rsi_filter -> src.strategies.filters.rsi_filter.RSIFilter
        """
        for key, cfg in self.filters_cfg.items():
            if not cfg.get("enabled", False):
                continue

            module_name = f"src.strategies.filters.{key}"
            class_name = key_to_classname(key)

            try:
                module = importlib.import_module(module_name)
                cls = getattr(module, class_name)
            except Exception as e:
                logger.error(f"Failed to load filter {key}: {e}")
                continue

            try:
                self.filters[key] = cls(**cfg)
            except Exception as e:
                logger.error(f"Failed to instantiate {class_name}: {e}")

    # ------------------------------------------------------------------ #
    # Risk manager
    # ------------------------------------------------------------------ #
    def initialize_risk_manager(self, df_full: pd.DataFrame):
        from src.strategies.trade_management.risk_manager import RiskManager

        self.filters["risk"] = RiskManager(self.config, df_full)

    # ------------------------------------------------------------------ #
    # Progressive tracker
    # ------------------------------------------------------------------ #
    def set_progressive_tracker(self, tracker):
        self.progressive_tracker = tracker

    # ------------------------------------------------------------------ #
    # Indicator precomputation (speed critical)
    # ------------------------------------------------------------------ #
    def compute_indicators(self, df: pd.DataFrame):
        """Pre-compute all indicators once based on enabled filters."""
        if df.empty or "close" not in df.columns:
            raise ValueError("Cannot compute indicators: empty DF or missing 'close'")

        required_cols = ["open", "high", "low", "close", "volume"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Missing columns for indicators: {missing}")

        df = df[required_cols].copy().astype("float32")
        self.indicators = {}

        for f_name in self.filter_sequence:
            cfg = self.filters_cfg.get(f_name, {})
            if not cfg.get("enabled", False):
                continue

            try:
                if f_name == "rsi_filter":
                    length = cfg.get("length", 14)
                    self.indicators["rsi"] = (
                        pta.rsi(df["close"], length=length)
                        .astype("float32")
                        .fillna(50.0)
                    )

                elif f_name == "choppiness_filter":
                    length = cfg.get("length", 14)
                    self.indicators["choppiness"] = (
                        pta.chop(df["high"], df["low"], df["close"], length=length)
                        .astype("float32")
                        .fillna(50.0)
                    )

                elif f_name == "bollinger_filter":
                    length = cfg.get("length", 14)
                    std = cfg.get("std_dev", 2.0)
                    bb = pta.bbands(df["close"], length=length, std=std)
                    if not bb.empty:
                        basis_col = f"BBM_{length}_{std}"
                        upper_col = f"BBU_{length}_{std}"
                        lower_col = f"BBL_{length}_{std}"
                        if (
                            basis_col in bb.columns
                            and upper_col in bb.columns
                            and lower_col in bb.columns
                        ):
                            bw = ((bb[upper_col] - bb[lower_col]) / bb[basis_col]) * 100
                            self.indicators["bbw"] = bw.astype("float32")
                            ma_len = cfg.get("width_ma_length", 30)
                            if ma_len > 0:
                                self.indicators["bbw_ma"] = (
                                    bw.rolling(ma_len, min_periods=ma_len)
                                    .mean()
                                    .astype("float32")
                                )

                elif f_name == "adx_filter":
                    length = cfg.get("adx_length", 14)
                    adx_df = pta.adx(df["high"], df["low"], df["close"], length=length)
                    if not adx_df.empty:
                        self.indicators["adx"] = (
                            adx_df[f"ADX_{length}"].astype("float32").fillna(0)
                        )

                elif f_name == "supertrend_filter":
                    length = cfg.get("atr_length", 10)
                    mult = cfg.get("factor", 3.0)
                    st = pta.supertrend(
                        df["high"], df["low"], df["close"], length=length, multiplier=mult
                    )
                    if not st.empty:
                        st_col = f"SUPERT_{length}_{mult}"
                        dir_col = f"SUPERTd_{length}_{mult}"
                        if st_col in st.columns and dir_col in st.columns:
                            self.indicators["supertrend"] = st[
                                [st_col, dir_col]
                            ].astype("float32")

                elif f_name == "ma_filter":
                    ma_filter = self.filters.get(f_name)
                    if ma_filter is not None:
                        ma = ma_filter._calculate_ma(df["close"])
                        self.indicators["ma"] = ma

                elif f_name == "cci_filter":
                    length = cfg.get("length", 20)
                    self.indicators["cci"] = (
                        pta.cci(df["high"], df["low"], df["close"], length=length)
                        .astype("float32")
                        .fillna(0)
                    )

                elif f_name == "macd_filter":
                    fast = cfg.get("fast_length", 12)
                    slow = cfg.get("slow_length", 26)
                    sig = cfg.get("signal_length", 9)
                    macd_df = pta.macd(df["close"], fast=fast, slow=slow, signal=sig)
                    if not macd_df.empty:
                        hist_col = f"MACDh_{fast}_{slow}_{sig}"
                        if hist_col in macd_df.columns:
                            self.indicators["macd_hist"] = (
                                macd_df[hist_col].astype("float32").fillna(0)
                            )

                elif f_name == "dpo_filter":
                    length = cfg.get("length", 20)
                    dpo_raw = pta.dpo(df["close"], length=length)
                    self.indicators["dpo"] = (
                        (dpo_raw / df["close"] * 100).astype("float32").fillna(0)
                    )

                elif f_name == "pivot_filter":
                    pivot = self.filters.get(f_name)
                    if pivot is not None:
                        self.indicators["pivot_bias"] = pivot._calculate_pivot_structure(
                            df
                        ).astype("int8")

            except Exception as e:
                logger.warning(f"Failed to compute {f_name}: {e}")

        logger.info(f"Pre-computed {len(self.indicators)} indicators")

    # ------------------------------------------------------------------ #
    # Time filter
    # ------------------------------------------------------------------ #
    def apply_time_filter(
        self, raw_signals: pd.Series, signal_id_map: Dict = None
    ) -> pd.Series:
        """Apply time filter to signals (first level)."""
        time_manager = self.filters.get("time")
        if time_manager is None or not getattr(time_manager, "enabled", True):
            return raw_signals.copy()

        raw_signals_df = pd.DataFrame(
            {"timestamp": raw_signals.index, "signal": raw_signals.values}
        ).dropna(subset=["signal"])

        time_filtered_df = time_manager.filter_signals_by_time(
            raw_signals_df, timestamp_col="timestamp"
        )

        time_filtered_signals = pd.Series(index=raw_signals.index, dtype=object)
        if not time_filtered_df.empty:
            time_filtered_signals.loc[
                time_filtered_df["timestamp"].values
            ] = time_filtered_df["signal"].values

        return time_filtered_signals

    # ------------------------------------------------------------------ #
    # Full filter chain: time + technical
    # ------------------------------------------------------------------ #
    def apply_filters(
        self, df: pd.DataFrame, raw_signals: pd.Series, signal_id_map: Dict = None
    ) -> Tuple[pd.Series, Dict]:

        stats = {
            "raw": {
                "buy": int((raw_signals == "BUY").sum()),
                "sell": int((raw_signals == "SELL").sum()),
                "total": int(raw_signals.notna().sum()),
            },
            "time_filtered": {"buy": 0, "sell": 0, "total": 0, "rejected": 0},
            "technical": {"buy": 0, "sell": 0, "total": 0, "rejected": 0},
            "final": {"buy": 0, "sell": 0, "total": 0},
        }

        if raw_signals.dropna().empty:
            return raw_signals, stats

        # ---------------------------------------------------------
        # 1) TIME FILTER
        # ---------------------------------------------------------
        time_filtered = self.apply_time_filter(raw_signals, signal_id_map)
        current = (time_filtered.notna()).astype("int8")

        stats["time_filtered"]["buy"] = int((time_filtered == "BUY").sum())
        stats["time_filtered"]["sell"] = int((time_filtered == "SELL").sum())
        stats["time_filtered"]["total"] = int(time_filtered.notna().sum())
        stats["time_filtered"]["rejected"] = (
            stats["raw"]["total"] - stats["time_filtered"]["total"]
        )

        if current.sum() == 0:
            stats["final"] = stats["time_filtered"].copy()
            return pd.Series(pd.NA, index=raw_signals.index), stats

        is_long = (raw_signals == "BUY")
        is_short = (raw_signals == "SELL")

        # ---------------------------------------------------------
        # 2) TECHNICAL FILTERS (signal-based)
        # ---------------------------------------------------------
        for f_name in self.filter_sequence:
            cfg = self.filters_cfg.get(f_name, {})
            if not cfg.get("enabled", False):
                continue

            mask = pd.Series(1, index=df.index, dtype="int8")

            try:
                # -------------------------------------------------
                # RSI FILTER
                # -------------------------------------------------
                if f_name == "rsi_filter":
                    rsi = self.indicators.get("rsi")
                    if rsi is not None:
                        mask.loc[is_long] = (rsi[is_long] < cfg["overbought"]).astype("int8")
                        mask.loc[is_short] = (rsi[is_short] > cfg["oversold"]).astype("int8")

                # -------------------------------------------------
                # CHOPPINESS FILTER
                # -------------------------------------------------
                elif f_name == "choppiness_filter":
                    ci = self.indicators.get("choppiness")
                    if ci is not None:
                        mask.loc[is_long] = (ci[is_long] < cfg["threshold"]).astype("int8")
                        mask.loc[is_short] = (ci[is_short] < cfg["threshold"]).astype("int8")

                # -------------------------------------------------
                # BOLLINGER FILTER
                # -------------------------------------------------
                elif f_name == "bollinger_filter":
                    bw = self.indicators.get("bbw")
                    bw_ma = self.indicators.get("bbw_ma")
                    if bw is not None and bw_ma is not None:
                        cond = bw > (bw_ma * cfg["filter_multiplier"])
                        mask.loc[is_long] = cond[is_long].astype("int8")
                        mask.loc[is_short] = cond[is_short].astype("int8")

                # -------------------------------------------------
                # ADX FILTER
                # -------------------------------------------------
                elif f_name == "adx_filter":
                    adx = self.indicators.get("adx")
                    if adx is not None:
                        cond = adx > cfg["threshold"]
                        mask.loc[is_long] = cond[is_long].astype("int8")
                        mask.loc[is_short] = cond[is_short].astype("int8")

                # -------------------------------------------------
                # SUPERTREND FILTER
                # -------------------------------------------------
                elif f_name == "supertrend_filter":
                    st = self.indicators.get("supertrend")
                    if st is not None:
                        length = cfg["atr_length"]
                        mult = cfg["factor"]
                        st_col = f"SUPERT_{length}_{mult}"
                        dir_col = f"SUPERTd_{length}_{mult}"

                        long_cond = (st[dir_col] == 1) & (df["close"] > st[st_col])
                        short_cond = (st[dir_col] == -1) & (df["close"] < st[st_col])

                        mask.loc[is_long] = long_cond[is_long].astype("int8")
                        mask.loc[is_short] = short_cond[is_short].astype("int8")

                # -------------------------------------------------
                # MA FILTER
                # -------------------------------------------------
                elif f_name == "ma_filter":
                    ma = self.indicators.get("ma")
                    if ma is not None:
                        ma_ago = ma.shift(cfg["slope_length"])
                        long_cond = ma > ma_ago
                        short_cond = ma < ma_ago

                        mask.loc[is_long] = long_cond[is_long].astype("int8")
                        mask.loc[is_short] = short_cond[is_short].astype("int8")

                # -------------------------------------------------
                # PIVOT FILTER
                # -------------------------------------------------
                elif f_name == "pivot_filter":
                    bias = self.indicators.get("pivot_bias")
                    if bias is not None:
                        mask.loc[is_long] = (bias[is_long] == 1).astype("int8")
                        mask.loc[is_short] = (bias[is_short] == -1).astype("int8")

                # -------------------------------------------------
                # CCI FILTER
                # -------------------------------------------------
                elif f_name == "cci_filter":
                    cci = self.indicators.get("cci")
                    if cci is not None:
                        mask.loc[is_long] = (cci[is_long] < cfg["overbought"]).astype("int8")
                        mask.loc[is_short] = (cci[is_short] > cfg["oversold"]).astype("int8")

                # -------------------------------------------------
                # MACD FILTER
                # -------------------------------------------------
                elif f_name == "macd_filter":
                    hist = self.indicators.get("macd_hist")
                    if hist is not None:
                        mask.loc[is_long] = (hist[is_long] > 0).astype("int8")
                        mask.loc[is_short] = (hist[is_short] < 0).astype("int8")

                # -------------------------------------------------
                # DPO FILTER
                # -------------------------------------------------
                elif f_name == "dpo_filter":
                    dpo = self.indicators.get("dpo")
                    if dpo is not None:
                        t = cfg["threshold"]
                        mask.loc[is_long] = ((dpo[is_long] < 0) & (dpo[is_long] > -t)).astype("int8")
                        mask.loc[is_short] = ((dpo[is_short] > 0) & (dpo[is_short] < t)).astype("int8")

            except Exception as e:
                logger.warning(f"Filter {f_name} failed: {e}")
                continue

            # Combine masks (fast int8 bitwise AND)
            current &= mask

            if current.sum() == 0:
                break

        # ---------------------------------------------------------
        # Final signals
        # ---------------------------------------------------------
        final_signals = raw_signals.where(current.astype(bool), pd.NA)

        stats["technical"]["buy"] = int((final_signals == "BUY").sum())
        stats["technical"]["sell"] = int((final_signals == "SELL").sum())
        stats["technical"]["total"] = int(final_signals.notna().sum())
        stats["technical"]["rejected"] = (
            stats["time_filtered"]["total"] - stats["technical"]["total"]
        )

        stats["final"] = stats["technical"].copy()

        return final_signals, stats