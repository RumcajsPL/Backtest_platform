import hashlib
import pickle
from typing import Dict, Any


class FilterPipelineCache:
    """
    Lightweight cache for indicator sets.
    Stores precomputed indicators keyed by a cache_id.
    The cache_id should uniquely represent the OHLCV dataset.
    """

    def __init__(self):
        self._cache: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def compute_cache_id(df) -> str:
        """
        Compute a stable hash for the OHLCV dataset.
        Uses first/last timestamps + row count + checksum of close prices.
        Fast and reliable.
        """
        if df is None or df.empty:
            return "empty"

        h = hashlib.sha1()
        h.update(str(df.index[0]).encode())
        h.update(str(df.index[-1]).encode())
        h.update(str(len(df)).encode())
        h.update(pickle.dumps(df["close"].head(50).to_numpy()))
        h.update(pickle.dumps(df["close"].tail(50).to_numpy()))
        return h.hexdigest()

    def has(self, cache_id: str) -> bool:
        return cache_id in self._cache

    def get(self, cache_id: str) -> Dict[str, Any]:
        return self._cache.get(cache_id, {})

    def store(self, cache_id: str, indicators: Dict[str, Any], indicators_np: Dict[str, Any]):
        self._cache[cache_id] = {
            "indicators": indicators,
            "indicators_np": indicators_np,
        }