"""
LiveDataFetcher — fetches recent OHLCV candles from eToro API.

Builds DataFrames with the exact schema expected by the strategy pipeline:
  - DatetimeIndex (UTC, tz-naive, floored to second)
  - Columns: open, high, low, close, volume (float64)
  - Sorted ascending (oldest first)

Candles endpoint:
  GET /market-data/instruments/{id}/history/candles/{direction}/{interval}/{count}
  direction: 'desc' (newest first) → reversed to asc before returning
  interval:  'OneMinute' | 'OneHour' | ... (from LiveDataConfig)
  count:     max 1000

Casing quirk (from API_REFERENCE.md):
  Outer wrapper uses 'instrumentId' (lowercase).
  Inner candle objects use 'instrumentID' (capital).
  'fromDate' = candle open time. 'volume' is always 0 for DAX — kept for schema compat.
"""
from __future__ import annotations

from typing import Tuple

import pandas as pd
from loguru import logger

from src.broker_support.client.client import EToroClient
from src.broker_support.config.broker_support_config import LiveDataConfig
from src.broker_support.enrichment.instrument_resolver import InstrumentResolver


class LiveDataFetchError(Exception):
    """Raised when candle fetch fails or returns unusable data."""


class LiveDataFetcher:
    """
    Fetches live OHLCV candles for strategy and HTF timeframes.

    Delegates all HTTP to EToroClient._make_request() — never implements
    its own HTTP logic.
    """

    _BASE_PATH = "api/v1"

    def __init__(
        self,
        client: EToroClient,
        resolver: InstrumentResolver,
        config: LiveDataConfig,
    ) -> None:
        self._client = client
        self._resolver = resolver
        self._config = config

    def fetch(self, symbol: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Fetch strategy-TF and HTF candles for symbol.

        Args:
            symbol: Instrument key from instrument_map.yaml, e.g. 'DAX'.

        Returns:
            (df_strategy, df_htf) — both sorted ascending with DatetimeIndex.

        Raises:
            LiveDataFetchError: if instrument cannot be resolved, fetch fails,
                                or response is empty/malformed.
        """
        instrument_id = self._resolver.instrument_id(symbol)
        if instrument_id is None:
            raise LiveDataFetchError(
                f"Cannot resolve symbol '{symbol}' to an instrumentId. "
                f"Add it to instrument_map.yaml or verify spelling."
            )

        logger.info(
            f"LiveDataFetcher.fetch: symbol={symbol}, instrumentId={instrument_id}, "
            f"strategy={self._config.strategy_bars_to_fetch}x{self._config.strategy_interval}, "
            f"htf={self._config.htf_bars_to_fetch}x{self._config.htf_interval}"
        )

        df_strategy = self._fetch_candles(
            instrument_id=instrument_id,
            interval=self._config.strategy_interval,
            count=self._config.strategy_bars_to_fetch,
            label="strategy",
        )

        df_htf = self._fetch_candles(
            instrument_id=instrument_id,
            interval=self._config.htf_interval,
            count=self._config.htf_bars_to_fetch,
            label="htf",
        )

        logger.info(
            f"LiveDataFetcher.fetch complete: "
            f"strategy={len(df_strategy)} bars "
            f"[{df_strategy.index[0]} → {df_strategy.index[-1]}], "
            f"htf={len(df_htf)} bars "
            f"[{df_htf.index[0]} → {df_htf.index[-1]}]"
        )

        return df_strategy, df_htf

    def _fetch_candles(
        self,
        instrument_id: int,
        interval: str,
        count: int,
        label: str,
    ) -> pd.DataFrame:
        """
        Fetch candles and return a clean DataFrame sorted ascending.

        Response structure (API_REFERENCE.md):
          {
            "interval": "OneMinute",
            "candles": [
              {
                "instrumentId": 32,          ← lowercase (outer)
                "candles": [
                  {
                    "instrumentID": 32,       ← capital (inner)
                    "fromDate": "2025-03-05T10:34:00Z",
                    "open": 23556.77,
                    "high": 23560.00,
                    "low": 23550.00,
                    "close": 23558.50,
                    "volume": 0
                  }, ...
                ]
              }
            ]
          }

        We always request 'desc' (newest first) and reverse to asc.
        This ensures we always get the most recent N bars regardless of
        any time parameter.
        """
        endpoint = (
            f"{self._BASE_PATH}/market-data/instruments"
            f"/{instrument_id}/history/candles"
            f"/{self._config.candle_direction}/{interval}/{count}"
        )

        logger.debug(f"Fetching {label} candles: {endpoint}")

        try:
            response = self._client._make_request("GET", endpoint)
        except Exception as exc:
            raise LiveDataFetchError(
                f"Candle fetch failed for {label} ({interval}, {count} bars): {exc}"
            ) from exc

        df = self._parse_candles_response(response, label)

        if df.empty:
            raise LiveDataFetchError(
                f"{label} candle response parsed to empty DataFrame. "
                f"Instrument {instrument_id}, interval={interval}. "
                f"Check that the instrument is currently tradable."
            )

        return df

    def _parse_candles_response(
        self,
        response: dict,
        label: str,
    ) -> pd.DataFrame:
        """
        Parse the nested candles response into a clean DataFrame.

        Handles the casing quirk: outer 'instrumentId' (lowercase),
        inner candle objects 'instrumentID' (capital).

        Returns DataFrame sorted ascending with tz-naive UTC DatetimeIndex.
        """
        # Navigate: response → candles[0] → candles[]
        outer_candles = response.get("candles", [])
        if not outer_candles:
            logger.warning(f"{label}: response has empty 'candles' list. Response keys: {list(response.keys())}")
            return pd.DataFrame()

        # There should be exactly one instrument entry
        instrument_entry = outer_candles[0]
        inner_candles = instrument_entry.get("candles", [])

        if not inner_candles:
            logger.warning(f"{label}: inner 'candles' list is empty for entry: {list(instrument_entry.keys())}")
            return pd.DataFrame()

        records = []
        for bar in inner_candles:
            from_date = bar.get("fromDate")
            if from_date is None:
                logger.warning(f"{label}: candle missing 'fromDate', skipping: {bar}")
                continue
            records.append({
                "timestamp": from_date,
                "open":   float(bar.get("open")   or 0.0),
                "high":   float(bar.get("high")   or 0.0),
                "low":    float(bar.get("low")    or 0.0),
                "close":  float(bar.get("close")  or 0.0),
                "volume": float(bar.get("volume") or 0.0),
            })

        if not records:
            logger.warning(f"{label}: no valid candle records extracted from response")
            return pd.DataFrame()

        df = pd.DataFrame(records)
        # fromDate is ISO 8601 with Z suffix (e.g. "2026-03-26T17:34:55.042Z").
        # Explicit format="ISO8601" avoids pandas UserWarning on mixed-precision
        # fractional seconds and ensures consistent parsing across all candle TFs.
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True)
        df["timestamp"] = df["timestamp"].dt.tz_localize(None)  # tz-naive UTC
        df["timestamp"] = df["timestamp"].dt.floor("s")
        df = df.set_index("timestamp")
        df = df.sort_index()  # asc — oldest first (reverse of 'desc' fetch)

        # Drop duplicate timestamps (shouldn't happen, defensive)
        if not df.index.is_unique:
            dup_count = df.index.duplicated().sum()
            logger.warning(f"{label}: {dup_count} duplicate timestamps, keeping last")
            df = df[~df.index.duplicated(keep="last")]

        logger.debug(
            f"{label} candles parsed: {len(df)} bars, "
            f"range [{df.index[0]} → {df.index[-1]}]"
        )
        return df