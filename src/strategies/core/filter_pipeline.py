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


# ======================================================================
# Base filter interface
# ======================================================================
class BaseFilter:
    """
    Unified filter interface:
    - compute_indicators(df, indicators, ind_np)
    - apply_filter(df, raw_signals, indicators, ind_np, current_mask, long_idx, short_idx)
    """

    def __init__(self, name: str, cfg: Dict):
        self.name = name
        self.cfg = cfg

    def compute_indicators(
        self,
        df: pd.DataFrame,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
    ) -> None:
        """Optional: compute and store indicators into indicators / ind_np."""
        return

    def apply_filter(
        self,
        df: pd.DataFrame,
        raw_signals: pd.Series,
        indicators: Dict[str, pd.Series],
        ind_np: Dict[str, np.ndarray],
        current_mask: np.ndarray,
        long_idx: np.ndarray,
        short_idx: np.ndarray,
    ) -> np.ndarray:
        """Must return updated mask (np.int8 array)."""
        return current_mask


# ======================================================================
# Concrete filters (using existing numpy logic)
# ======================================================================
class RsiFilter(BaseFilter):
    def compute_indicators(self, df, indicators, ind_np):
        length = self.cfg.get("length", 14)
        rsi = pta.rsi(df["close"], length=length).astype("float32").fillna(50)
        indicators["rsi"] = rsi
        ind_np["rsi"] = rsi.to_numpy()

    def apply_filter(self, df, raw_signals, indicators, ind_np, current_mask, long_idx, short_idx):
        rsi = ind_np.get("rsi")
        if rsi is None:
            return current_mask

        ob = np.float32(self.cfg["overbought"])
        os = np.float32(self.cfg["oversold"])
        mask = np.ones_like(current_mask, dtype=np.int8)

        if long_idx.size:
            mask[long_idx] = (rsi[long_idx] < ob).astype(np.int8)
        if short_idx.size:
            mask[short_idx] = (rsi[short_idx] > os).astype(np.int8)

        return mask


class ChoppinessFilter(BaseFilter):
    def compute_indicators(self, df, indicators, ind_np):
        length = self.cfg.get("length", 14)
        ci = pta.chop(df["high"], df["low"], df["close"], length=length)
        ci = ci.astype("float32").fillna(50)
        indicators["choppiness"] = ci
        ind_np["choppiness"] = ci.to_numpy()

    def apply_filter(self, df, raw_signals, indicators, ind_np, current_mask, long_idx, short_idx):
        ci = ind_np.get("choppiness")
        if ci is None:
            return current_mask

        thr = np.float32(self.cfg["threshold"])
        cond = ci < thr
        mask = np.ones_like(current_mask, dtype=np.int8)

        if long_idx.size:
            mask[long_idx] = cond[long_idx].astype(np.int8)
        if short_idx.size:
            mask[short_idx] = cond[short_idx].astype(np.int8)

        return mask


class BollingerFilter(BaseFilter):
    def compute_indicators(self, df, indicators, ind_np):
        length = self.cfg.get("length", 14)
        std = self.cfg.get("std_dev", 2.0)
        bb = pta.bbands(df["close"], length=length, std=std)
        if bb.empty:
            return

        basis = bb[f"BBM_{length}_{std}"]
        upper = bb[f"BBU_{length}_{std}"]
        lower = bb[f"BBL_{length}_{std}"]
        bw = ((upper - lower) / basis * 100).astype("float32")
        indicators["bbw"] = bw
        ind_np["bbw"] = bw.to_numpy()

        ma_len = self.cfg.get("width_ma_length", 30)
        bbw_ma = bw.rolling(ma_len).mean().astype("float32")
        indicators["bbw_ma"] = bbw_ma
        ind_np["bbw_ma"] = bbw_ma.to_numpy()

    def apply_filter(self, df, raw_signals, indicators, ind_np, current_mask, long_idx, short_idx):
        bw = ind_np.get("bbw")
        bbw_ma = ind_np.get("bbw_ma")
        if bw is None or bbw_ma is None:
            return current_mask

        mult = np.float32(self.cfg["filter_multiplier"])
        cond = bw > (bbw_ma * mult)
        mask = np.ones_like(current_mask, dtype=np.int8)

        if long_idx.size:
            mask[long_idx] = cond[long_idx].astype(np.int8)
        if short_idx.size:
            mask[short_idx] = cond[short_idx].astype(np.int8)

        return mask


class AdxFilter(BaseFilter):
    def compute_indicators(self, df, indicators, ind_np):
        length = self.cfg.get("adx_length", 14)
        adx_df = pta.adx(df["high"], df["low"], df["close"], length=length)
        adx = adx_df[f"ADX_{length}"].astype("float32").fillna(0)
        indicators["adx"] = adx
        ind_np["adx"] = adx.to_numpy()

    def apply_filter(self, df, raw_signals, indicators, ind_np, current_mask, long_idx, short_idx):
        adx = ind_np.get("adx")
        if adx is None:
            return current_mask

        thr = np.float32(self.cfg["threshold"])
        cond = adx > thr
        mask = np.ones_like(current_mask, dtype=np.int8)

        if long_idx.size:
            mask[long_idx] = cond[long_idx].astype(np.int8)
        if short_idx.size:
            mask[short_idx] = cond[short_idx].astype(np.int8)

        return mask


class SupertrendFilter(BaseFilter):
    def compute_indicators(self, df, indicators, ind_np):
        length = self.cfg.get("atr_length", 10)
        mult = self.cfg.get("factor", 3.0)
        st = pta.supertrend(df["high"], df["low"], df["close"], length, mult)
        st_price = st[f"SUPERT_{length}_{mult}"].astype("float32")
        st_dir = st[f"SUPERTd_{length}_{mult}"].astype("float32")
        indicators["supertrend"] = st
        ind_np["supertrend_price"] = st_price.to_numpy()
        ind_np["supertrend_dir"] = st_dir.to_numpy()

    def apply_filter(self, df, raw_signals, indicators, ind_np, current_mask, long_idx, short_idx):
        st_price = ind_np.get("supertrend_price")
        st_dir = ind_np.get("supertrend_dir")
        if st_price is None or st_dir is None:
            return current_mask

        close_np = df["close"].to_numpy(np.float32)
        mask = np.ones_like(current_mask, dtype=np.int8)

        if long_idx.size:
            cond = (st_dir[long_idx] == 1) & (close_np[long_idx] > st_price[long_idx])
            mask[long_idx] = cond.astype(np.int8)
        if short_idx.size:
            cond = (st_dir[short_idx] == -1) & (close_np[short_idx] < st_price[short_idx])
            mask[short_idx] = cond.astype(np.int8)

        return mask


class MaFilter(BaseFilter):
    def __init__(self, name: str, cfg: Dict, external_filter=None):
        super().__init__(name, cfg)
        self.external_filter = external_filter  # existing MA filter class if available

    def compute_indicators(self, df, indicators, ind_np):
        if self.external_filter is not None:
            ma = self.external_filter._calculate_ma(df["close"]).astype("float32")
        else:
            length = self.cfg.get("length", 50)
            ma = df["close"].rolling(length).mean().astype("float32")
        indicators["ma"] = ma
        ind_np["ma"] = ma.to_numpy()

    def apply_filter(self, df, raw_signals, indicators, ind_np, current_mask, long_idx, short_idx):
        ma = ind_np.get("ma")
        if ma is None:
            return current_mask

        sl = int(self.cfg.get("slope_length", 10))
        ma_ago = np.empty_like(ma)
        ma_ago[:] = np.nan
        if sl < len(ma):
            ma_ago[sl:] = ma[:-sl]

        mask = np.ones_like(current_mask, dtype=np.int8)
        if long_idx.size:
            mask[long_idx] = (ma[long_idx] > ma_ago[long_idx]).astype(np.int8)
        if short_idx.size:
            mask[short_idx] = (ma[short_idx] < ma_ago[short_idx]).astype(np.int8)

        return mask


class CciFilter(BaseFilter):
    def compute_indicators(self, df, indicators, ind_np):
        length = self.cfg.get("length", 20)
        cci = pta.cci(df["high"], df["low"], df["close"], length).astype("float32")
        indicators["cci"] = cci
        ind_np["cci"] = cci.to_numpy()

    def apply_filter(self, df, raw_signals, indicators, ind_np, current_mask, long_idx, short_idx):
        cci = ind_np.get("cci")
        if cci is None:
            return current_mask

        ob = np.float32(self.cfg["overbought"])
        os = np.float32(self.cfg["oversold"])
        mask = np.ones_like(current_mask, dtype=np.int8)

        if long_idx.size:
            mask[long_idx] = (cci[long_idx] < ob).astype(np.int8)
        if short_idx.size:
            mask[short_idx] = (cci[short_idx] > os).astype(np.int8)

        return mask


class MacdFilter(BaseFilter):
    def compute_indicators(self, df, indicators, ind_np):
        fast = self.cfg.get("fast_length", 12)
        slow = self.cfg.get("slow_length", 26)
        sig = self.cfg.get("signal_length", 9)
        macd_df = pta.macd(df["close"], fast, slow, sig)
        hist = macd_df[f"MACDh_{fast}_{slow}_{sig}"].astype("float32")
        indicators["macd_hist"] = hist
        ind_np["macd_hist"] = hist.to_numpy()

    def apply_filter(self, df, raw_signals, indicators, ind_np, current_mask, long_idx, short_idx):
        hist = ind_np.get("macd_hist")
        if hist is None:
            return current_mask

        mask = np.ones_like(current_mask, dtype=np.int8)
        if long_idx.size:
            mask[long_idx] = (hist[long_idx] > 0).astype(np.int8)
        if short_idx.size:
            mask[short_idx] = (hist[short_idx] < 0).astype(np.int8)
        return mask


class DpoFilter(BaseFilter):
    def compute_indicators(self, df, indicators, ind_np):
        length = self.cfg.get("length", 20)
        dpo = pta.dpo(df["close"], length)
        dpo = (dpo / df["close"] * 100).astype("float32")
        indicators["dpo"] = dpo
        ind_np["dpo"] = dpo.to_numpy()

    def apply_filter(self, df, raw_signals, indicators, ind_np, current_mask, long_idx, short_idx):
        dpo = ind_np.get("dpo")
        if dpo is None:
            return current_mask

        t = np.float32(self.cfg["threshold"])
        mask = np.ones_like(current_mask, dtype=np.int8)

        if long_idx.size:
            cond = (dpo[long_idx] < 0) & (dpo[long_idx] > -t)
            mask[long_idx] = cond.astype(np.int8)
        if short_idx.size:
            cond = (dpo[short_idx] > 0) & (dpo[short_idx] < t)
            mask[short_idx] = cond.astype(np.int8)

        return mask


class PivotFilter(BaseFilter):
    def __init__(self, name: str, cfg: Dict, external_filter=None):
        super().__init__(name, cfg)
        self.external_filter = external_filter

    def compute_indicators(self, df, indicators, ind_np):
        if self.external_filter is None:
            return
        bias = self.external_filter._calculate_pivot_structure(df).astype("int8")
        indicators["pivot_bias"] = bias
        ind_np["pivot_bias"] = bias.to_numpy()

    def apply_filter(self, df, raw_signals, indicators, ind_np, current_mask, long_idx, short_idx):
        bias = ind_np.get("pivot_bias")
        if bias is None:
            return current_mask

        mask = np.ones_like(current_mask, dtype=np.int8)
        if long_idx.size:
            mask[long_idx] = (bias[long_idx] == 1).astype(np.int8)
        if short_idx.size:
            mask[short_idx] = (bias[short_idx] == -1).astype(np.int8)
        return mask


# ======================================================================
# FilterPipeline v4
# ======================================================================
class FilterPipeline:
    """
    FilterPipeline v4:
    - Unified filter architecture (BaseFilter subclasses)
    - Indicator caching
    - Numpy-optimized core
    - Time filter + technical filters
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

        self.filters: Dict[str, BaseFilter] = {}
        self.indicators: Dict[str, pd.Series] = {}
        self.ind_np: Dict[str, np.ndarray] = {}

        self.cache = cache or FilterPipelineCache()

        self._load_time_filter()
        self._load_technical_filters()

    # ------------------------------------------------------------------ #
    # Load filters
    # ------------------------------------------------------------------ #
    def _load_time_filter(self):
        from src.strategies.filters.time_filter import TimeManager
        self.time_manager = TimeManager(self.config.get("trade_management", {}))

    def _load_technical_filters(self):
        """
        Build unified filter objects.
        If external filter classes exist (ma_filter, pivot_filter), we pass them in.
        """
        for key, cfg in self.filters_cfg.items():
            if not cfg.get("enabled", False):
                continue

            if key == "rsi_filter":
                self.filters[key] = RsiFilter(key, cfg)
            elif key == "choppiness_filter":
                self.filters[key] = ChoppinessFilter(key, cfg)
            elif key == "bollinger_filter":
                self.filters[key] = BollingerFilter(key, cfg)
            elif key == "adx_filter":
                self.filters[key] = AdxFilter(key, cfg)
            elif key == "supertrend_filter":
                self.filters[key] = SupertrendFilter(key, cfg)
            elif key == "ma_filter":
                external = None
                try:
                    module = importlib.import_module("src.strategies.filters.ma_filter")
                    cls = getattr(module, key_to_classname("ma_filter"))
                    external = cls(**cfg)
                except Exception:
                    external = None
                self.filters[key] = MaFilter(key, cfg, external_filter=external)
            elif key == "pivot_filter":
                external = None
                try:
                    module = importlib.import_module("src.strategies.filters.pivot_filter")
                    cls = getattr(module, key_to_classname("pivot_filter"))
                    external = cls(**cfg)
                except Exception:
                    external = None
                self.filters[key] = PivotFilter(key, cfg, external_filter=external)
            elif key == "cci_filter":
                self.filters[key] = CciFilter(key, cfg)
            elif key == "macd_filter":
                self.filters[key] = MacdFilter(key, cfg)
            elif key == "dpo_filter":
                self.filters[key] = DpoFilter(key, cfg)
            else:
                logger.warning(f"Unknown filter key in config: {key}")

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
            filt = self.filters.get(f_name)
            if filt is None:
                continue
            try:
                filt.compute_indicators(df, self.indicators, self.ind_np)
            except Exception as e:
                logger.warning(f"Failed to compute indicators for {f_name}: {e}")

        self.cache.store(cache_id, self.indicators, self.ind_np)
        logger.info("Indicators computed and cached")

    # ------------------------------------------------------------------ #
    # Time filter
    # ------------------------------------------------------------------ #
    def apply_time_filter(self, raw_signals: pd.Series) -> pd.Series:
        tm = self.time_manager
        if tm is None or not getattr(tm, "enabled", True):
            return raw_signals.copy()

        df = pd.DataFrame({"timestamp": raw_signals.index, "signal": raw_signals.values})
        df = df.dropna(subset=["signal"])

        filtered = tm.filter_signals_by_time(df, "timestamp")

        out = pd.Series(index=raw_signals.index, dtype=object)
        if not filtered.empty:
            out.loc[filtered["timestamp"].values] = filtered["signal"].values
        return out

    # ------------------------------------------------------------------ #
    # Full filter chain
    # ------------------------------------------------------------------ #
    def apply_filters(self, df: pd.DataFrame, raw_signals: pd.Series) -> Tuple[pd.Series, Dict]:
        """
        Apply time filter + technical filters.
        Returns:
            final_signals (pd.Series)
            stats (Dict)
        """
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

        # Ensure indicators are ready
        if not self.indicators and not self.ind_np:
            self.compute_indicators(df)

        for f_name in self.filter_sequence:
            filt = self.filters.get(f_name)
            if filt is None:
                continue

            try:
                mask = filt.apply_filter(
                    df=df,
                    raw_signals=raw_signals,
                    indicators=self.indicators,
                    ind_np=self.ind_np,
                    current_mask=current,
                    long_idx=long_idx,
                    short_idx=short_idx,
                )
            except Exception as e:
                logger.warning(f"Filter {f_name} failed: {e}")
                continue

            if mask is None:
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