import logging
import importlib
from typing import Dict, List, Tuple

import pandas as pd
import numpy as np
import pandas_ta_classic as pta

from src.backtesting.tools.filter_pipeline_cache import FilterPipelineCache

logger = logging.getLogger(__name__)

ACRONYMS = {"rsi", "macd", "cci", "adx", "dpo", "ma"}


def key_to_classname(key: str) -> str:
    parts = key.split("_")
    class_name = ""
    for part in parts:
        class_name += part.upper() if part in ACRONYMS else part.capitalize()
    return class_name


class FilterPipeline:
    """
    FilterPipeline v3:
    - Pure technical filtering
    - Numpy-optimized core
    - Indicator caching
    - Progressive tracker hook kept as no-op for compatibility
    """

    def __init__(self, config: Dict, cache: FilterPipelineCache = None):
        self.config = config
        self.filters_cfg: Dict = config.get("filters", {})
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
        self.ind_np: Dict = {}

        self.cache = cache or FilterPipelineCache()
        self.progressive_tracker = None  # kept for API compatibility

        self._load_time_filter()
        self._load_technical_filters()

    # ------------------------------------------------------------------ #
    # Progressive tracker (no-op hook)
    # ------------------------------------------------------------------ #
    def set_progressive_tracker(self, tracker):
        """
        Kept for backward compatibility.
        FilterPipeline no longer uses progressive tracking internally.
        """
        self.progressive_tracker = tracker

    # ------------------------------------------------------------------ #
    # Load filters
    # ------------------------------------------------------------------ #
    def _load_time_filter(self):
        from src.strategies.filters.time_filter import TimeManager
        self.filters["time"] = TimeManager(self.config.get("trade_management", {}))

    def _load_technical_filters(self):
        for key, cfg in self.filters_cfg.items():
            if not cfg.get("enabled", False):
                continue

            module_name = f"src.strategies.filters.{key}"
            class_name = key_to_classname(key)

            try:
                module = importlib.import_module(module_name)
                cls = getattr(module, class_name)
                self.filters[key] = cls(**cfg)
            except Exception as e:
                logger.error(f"Failed to load filter {key}: {e}")

    # ------------------------------------------------------------------ #
    # Indicator computation with caching
    # ------------------------------------------------------------------ #
    def compute_indicators(self, df: pd.DataFrame):
        """
        Compute indicators OR load them from cache.
        """
        cache_id = self.cache.compute_cache_id(df)

        if self.cache.has(cache_id):
            cached = self.cache.get(cache_id)
            self.indicators = cached["indicators"]
            self.ind_np = cached["indicators_np"]
            logger.info("Loaded indicators from cache")
            return

        self.indicators = {}
        self.ind_np = {}

        df = df.astype("float32")

        for f_name in self.filter_sequence:
            cfg = self.filters_cfg.get(f_name, {})
            if not cfg.get("enabled", False):
                continue

            try:
                if f_name == "rsi_filter":
                    length = cfg.get("length", 14)
                    rsi = pta.rsi(df["close"], length=length).astype("float32").fillna(50)
                    self.indicators["rsi"] = rsi
                    self.ind_np["rsi"] = rsi.to_numpy()

                elif f_name == "choppiness_filter":
                    length = cfg.get("length", 14)
                    ci = pta.chop(df["high"], df["low"], df["close"], length=length)
                    ci = ci.astype("float32").fillna(50)
                    self.indicators["choppiness"] = ci
                    self.ind_np["choppiness"] = ci.to_numpy()

                elif f_name == "bollinger_filter":
                    length = cfg.get("length", 14)
                    std = cfg.get("std_dev", 2.0)
                    bb = pta.bbands(df["close"], length=length, std=std)
                    if not bb.empty:
                        basis = bb[f"BBM_{length}_{std}"]
                        upper = bb[f"BBU_{length}_{std}"]
                        lower = bb[f"BBL_{length}_{std}"]
                        bw = ((upper - lower) / basis * 100).astype("float32")
                        self.indicators["bbw"] = bw
                        self.ind_np["bbw"] = bw.to_numpy()

                        ma_len = cfg.get("width_ma_length", 30)
                        bbw_ma = bw.rolling(ma_len).mean().astype("float32")
                        self.indicators["bbw_ma"] = bbw_ma
                        self.ind_np["bbw_ma"] = bbw_ma.to_numpy()

                elif f_name == "adx_filter":
                    length = cfg.get("adx_length", 14)
                    adx_df = pta.adx(df["high"], df["low"], df["close"], length=length)
                    adx = adx_df[f"ADX_{length}"].astype("float32").fillna(0)
                    self.indicators["adx"] = adx
                    self.ind_np["adx"] = adx.to_numpy()

                elif f_name == "supertrend_filter":
                    length = cfg.get("atr_length", 10)
                    mult = cfg.get("factor", 3.0)
                    st = pta.supertrend(df["high"], df["low"], df["close"], length, mult)
                    st_price = st[f"SUPERT_{length}_{mult}"].astype("float32")
                    st_dir = st[f"SUPERTd_{length}_{mult}"].astype("float32")
                    self.indicators["supertrend"] = st
                    self.ind_np["supertrend_price"] = st_price.to_numpy()
                    self.ind_np["supertrend_dir"] = st_dir.to_numpy()

                elif f_name == "ma_filter":
                    ma_filter = self.filters.get(f_name)
                    ma = ma_filter._calculate_ma(df["close"]).astype("float32")
                    self.indicators["ma"] = ma
                    self.ind_np["ma"] = ma.to_numpy()

                elif f_name == "cci_filter":
                    length = cfg.get("length", 20)
                    cci = pta.cci(df["high"], df["low"], df["close"], length).astype("float32")
                    self.indicators["cci"] = cci
                    self.ind_np["cci"] = cci.to_numpy()

                elif f_name == "macd_filter":
                    fast = cfg.get("fast_length", 12)
                    slow = cfg.get("slow_length", 26)
                    sig = cfg.get("signal_length", 9)
                    macd_df = pta.macd(df["close"], fast, slow, sig)
                    hist = macd_df[f"MACDh_{fast}_{slow}_{sig}"].astype("float32")
                    self.indicators["macd_hist"] = hist
                    self.ind_np["macd_hist"] = hist.to_numpy()

                elif f_name == "dpo_filter":
                    length = cfg.get("length", 20)
                    dpo = pta.dpo(df["close"], length)
                    dpo = (dpo / df["close"] * 100).astype("float32")
                    self.indicators["dpo"] = dpo
                    self.ind_np["dpo"] = dpo.to_numpy()

                elif f_name == "pivot_filter":
                    pivot = self.filters.get(f_name)
                    bias = pivot._calculate_pivot_structure(df).astype("int8")
                    self.indicators["pivot_bias"] = bias
                    self.ind_np["pivot_bias"] = bias.to_numpy()

            except Exception as e:
                logger.warning(f"Failed to compute {f_name}: {e}")

        self.cache.store(cache_id, self.indicators, self.ind_np)
        logger.info("Indicators computed and cached")

    # ------------------------------------------------------------------ #
    # Time filter
    # ------------------------------------------------------------------ #
    def apply_time_filter(self, raw_signals: pd.Series) -> pd.Series:
        time_manager = self.filters.get("time")
        if time_manager is None or not getattr(time_manager, "enabled", True):
            return raw_signals.copy()

        df = pd.DataFrame({"timestamp": raw_signals.index, "signal": raw_signals.values})
        df = df.dropna(subset=["signal"])

        filtered = time_manager.filter_signals_by_time(df, "timestamp")

        out = pd.Series(index=raw_signals.index, dtype=object)
        if not filtered.empty:
            out.loc[filtered["timestamp"].values] = filtered["signal"].values
        return out

    # ------------------------------------------------------------------ #
    # Full filter chain (numpy optimized)
    # ------------------------------------------------------------------ #
    def apply_filters(self, df: pd.DataFrame, raw_signals: pd.Series) -> Tuple[pd.Series, Dict]:
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

        # Time filter
        time_filtered = self.apply_time_filter(raw_signals)
        current = time_filtered.notna().to_numpy(np.int8)

        stats["time_filtered"]["buy"] = int((time_filtered == "BUY").sum())
        stats["time_filtered"]["sell"] = int((time_filtered == "SELL").sum())
        stats["time_filtered"]["total"] = int(time_filtered.notna().sum())
        stats["time_filtered"]["rejected"] = (
            stats["raw"]["total"] - stats["time_filtered"]["total"]
        )

        if current.sum() == 0:
            return pd.Series(pd.NA, index=df.index), stats

        n = len(df)
        ind = self.ind_np
        is_long = (raw_signals == "BUY").to_numpy(bool)
        is_short = (raw_signals == "SELL").to_numpy(bool)
        long_idx = np.where(is_long)[0]
        short_idx = np.where(is_short)[0]

        for f_name in self.filter_sequence:
            cfg = self.filters_cfg.get(f_name, {})
            if not cfg.get("enabled", False):
                continue

            mask = np.ones(n, np.int8)

            try:
                if f_name == "rsi_filter":
                    rsi = ind.get("rsi")
                    if rsi is not None:
                        ob = np.float32(cfg["overbought"])
                        os = np.float32(cfg["oversold"])
                        if long_idx.size:
                            mask[long_idx] = (rsi[long_idx] < ob).astype(np.int8)
                        if short_idx.size:
                            mask[short_idx] = (rsi[short_idx] > os).astype(np.int8)

                elif f_name == "choppiness_filter":
                    ci = ind.get("choppiness")
                    if ci is not None:
                        thr = np.float32(cfg["threshold"])
                        cond = ci < thr
                        if long_idx.size:
                            mask[long_idx] = cond[long_idx].astype(np.int8)
                        if short_idx.size:
                            mask[short_idx] = cond[short_idx].astype(np.int8)

                elif f_name == "bollinger_filter":
                    bw = ind.get("bbw")
                    bw_ma = ind.get("bbw_ma")
                    if bw is not None and bw_ma is not None:
                        mult = np.float32(cfg["filter_multiplier"])
                        cond = bw > (bw_ma * mult)
                        if long_idx.size:
                            mask[long_idx] = cond[long_idx].astype(np.int8)
                        if short_idx.size:
                            mask[short_idx] = cond[short_idx].astype(np.int8)

                elif f_name == "adx_filter":
                    adx = ind.get("adx")
                    if adx is not None:
                        thr = np.float32(cfg["threshold"])
                        cond = adx > thr
                        if long_idx.size:
                            mask[long_idx] = cond[long_idx].astype(np.int8)
                        if short_idx.size:
                            mask[short_idx] = cond[short_idx].astype(np.int8)

                elif f_name == "supertrend_filter":
                    st_price = ind.get("supertrend_price")
                    st_dir = ind.get("supertrend_dir")
                    if st_price is not None:
                        close_np = df["close"].to_numpy(np.float32)
                        if long_idx.size:
                            cond = (st_dir[long_idx] == 1) & (close_np[long_idx] > st_price[long_idx])
                            mask[long_idx] = cond.astype(np.int8)
                        if short_idx.size:
                            cond = (st_dir[short_idx] == -1) & (close_np[short_idx] < st_price[short_idx])
                            mask[short_idx] = cond.astype(np.int8)

                elif f_name == "ma_filter":
                    ma = ind.get("ma")
                    if ma is not None:
                        sl = int(cfg.get("slope_length", 10))
                        ma_ago = np.empty_like(ma)
                        ma_ago[:] = np.nan
                        if sl < len(ma):
                            ma_ago[sl:] = ma[:-sl]
                        if long_idx.size:
                            mask[long_idx] = (ma[long_idx] > ma_ago[long_idx]).astype(np.int8)
                        if short_idx.size:
                            mask[short_idx] = (ma[short_idx] < ma_ago[short_idx]).astype(np.int8)

                elif f_name == "pivot_filter":
                    bias = ind.get("pivot_bias")
                    if bias is not None:
                        if long_idx.size:
                            mask[long_idx] = (bias[long_idx] == 1).astype(np.int8)
                        if short_idx.size:
                            mask[short_idx] = (bias[short_idx] == -1).astype(np.int8)

                elif f_name == "cci_filter":
                    cci = ind.get("cci")
                    if cci is not None:
                        ob = np.float32(cfg["overbought"])
                        os = np.float32(cfg["oversold"])
                        if long_idx.size:
                            mask[long_idx] = (cci[long_idx] < ob).astype(np.int8)
                        if short_idx.size:
                            mask[short_idx] = (cci[short_idx] > os).astype(np.int8)

                elif f_name == "macd_filter":
                    hist = ind.get("macd_hist")
                    if hist is not None:
                        if long_idx.size:
                            mask[long_idx] = (hist[long_idx] > 0).astype(np.int8)
                        if short_idx.size:
                            mask[short_idx] = (hist[short_idx] < 0).astype(np.int8)

                elif f_name == "dpo_filter":
                    dpo = ind.get("dpo")
                    if dpo is not None:
                        t = np.float32(cfg["threshold"])
                        if long_idx.size:
                            cond = (dpo[long_idx] < 0) & (dpo[long_idx] > -t)
                            mask[long_idx] = cond.astype(np.int8)
                        if short_idx.size:
                            cond = (dpo[short_idx] > 0) & (dpo[short_idx] < t)
                            mask[short_idx] = cond.astype(np.int8)

            except Exception as e:
                logger.warning(f"Filter {f_name} failed: {e}")
                continue

            np.bitwise_and(current, mask, out=current)

            if current.sum() == 0:
                break

        final = raw_signals.where(pd.Series(current.astype(bool), index=df.index), pd.NA)

        stats["technical"]["buy"] = int((final == "BUY").sum())
        stats["technical"]["sell"] = int((final == "SELL").sum())
        stats["technical"]["total"] = int(final.notna().sum())
        stats["technical"]["rejected"] = (
            stats["time_filtered"]["total"] - stats["technical"]["total"]
        )
        stats["final"] = stats["technical"].copy()

        return final, stats