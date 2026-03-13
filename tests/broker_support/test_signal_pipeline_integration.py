"""
Integration tests for the run_signal.py pipeline.

Tests the wired pipeline end-to-end:
  BrokerSupportConfig → LiveConfigPatcher → LiveDataFetcher
  → build_live_data_bundle → SignalBridge.get_signal() → OrderSignal

All external I/O is mocked:
  - EToroClient._make_request  → candle API responses
  - StrategyConfig.from_dict   → avoids parquet/YAML file I/O after patching
  - RiskManager                → controls whether signal is approved or rejected
  - is_valid_trading_window    → controls WBWS+ flag
  - BrokerSupportConfig        → built programmatically (no real YAML/parquet files)
  - _check_pyramiding          → isolated in Stage 2 tests

No real files, no real API calls, no real parquet reads.

Run:
    pytest tests/broker_support/test_signal_pipeline_integration.py -v

Test classes:
  TestLiveDataFetcherParsing     — _parse_candles_response correctness
  TestLiveDataBundleConstruction — build_live_data_bundle contract
  TestLiveConfigPatcherPatch     — patch() dict mutations, immutability
  TestSignalBridgeGetSignal      — full pipeline: signal found / not found / rejected
  TestSignalBridgeWBWS           — WBWS+ gate: flag set correctly on OrderSignal
  TestCheckPyramiding            — _check_pyramiding() guard logic
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import datetime, timezone

import pandas as pd
import numpy as np
import pytest

# ---------------------------------------------------------------------------
# Path bootstrap — mirror run_signal.py approach
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.broker_support.live.live_data_fetcher import LiveDataFetcher, LiveDataFetchError
from src.broker_support.live.live_data_bundle import build_live_data_bundle, LiveDataBundleError
from src.broker_support.live.live_config_patcher import LiveConfigPatcher
from src.broker_support.live.order_signal import OrderSignal
from src.broker_support.live.signal_bridge import SignalBridge


# ---------------------------------------------------------------------------
# Shared test data builders
# ---------------------------------------------------------------------------

def _make_ohlcv_df(n: int = 100, start: str = "2026-03-13 09:00:00") -> pd.DataFrame:
    """
    Build a minimal valid OHLCV DataFrame with DatetimeIndex (UTC, tz-naive).
    Prices are realistic DAX levels (~20000).
    """
    idx = pd.date_range(start=start, periods=n, freq="1min")
    rng = np.random.default_rng(42)
    base = 20_000.0
    closes = base + rng.normal(0, 10, n).cumsum()
    df = pd.DataFrame(
        {
            "open":   closes - rng.uniform(0, 5, n),
            "high":   closes + rng.uniform(0, 8, n),
            "low":    closes - rng.uniform(0, 8, n),
            "close":  closes,
            "volume": np.zeros(n),
        },
        index=idx,
    )
    # Ensure high >= low (defensive after random offsets)
    df["high"] = df[["open", "high", "close"]].max(axis=1)
    df["low"]  = df[["open", "low",  "close"]].min(axis=1)
    return df


def _make_candle_api_response(
    instrument_id: int = 32,
    n: int = 10,
    interval: str = "OneMinute",
) -> dict:
    """Build a minimal valid eToro candles API response dict."""
    base_ts = pd.Timestamp("2026-03-13 09:00:00", tz="UTC")
    candles = [
        {
            "instrumentID": instrument_id,
            "fromDate": (base_ts + pd.Timedelta(minutes=i)).isoformat().replace("+00:00", "Z"),
            "open":   20_000.0 + i,
            "high":   20_010.0 + i,
            "low":    19_990.0 + i,
            "close":  20_005.0 + i,
            "volume": 0,
        }
        for i in range(n)
    ]
    return {
        "interval": interval,
        "candles": [
            {
                "instrumentId": instrument_id,   # lowercase — outer wrapper casing quirk
                "candles": candles,
            }
        ],
    }


def _make_bs_config(
    tmp_path: Path,
    *,
    allowed_hours_utc: list | None = None,
    skip_hours_utc: list | None = None,
    wbws_enabled: bool = True,
) -> MagicMock:
    """
    Return a MagicMock that mimics BrokerSupportConfig.
    All sub-configs are also MagicMocks with correct attribute names.
    Uses tmp_path so no real files need to exist.
    """
    # Create placeholder files that config schema might stat
    artf = tmp_path / "artf.parquet"
    artf.touch()
    strategy_yaml = tmp_path / "strategy.yaml"
    strategy_yaml.touch()
    instrument_map = tmp_path / "instrument_map.yaml"
    instrument_map.touch()

    cfg = MagicMock()
    cfg.execution.symbol = "DAX"
    cfg.execution.amount_usd = 60.0
    cfg.execution.leverage = 20
    cfg.execution.instrument_map_path = instrument_map

    cfg.live_data.artf_ohlcv_path = artf
    cfg.live_data.strategy_bars_to_fetch = 500
    cfg.live_data.htf_bars_to_fetch = 120
    cfg.live_data.strategy_interval = "OneMinute"
    cfg.live_data.htf_interval = "OneHour"
    cfg.live_data.candle_direction = "desc"

    cfg.trading_window.enabled = wbws_enabled
    cfg.trading_window.allowed_hours_utc = allowed_hours_utc or [9, 10, 11, 12, 13, 14, 15, 16]
    cfg.trading_window.skip_hours_utc = skip_hours_utc or [17, 18]

    cfg.strategy.yaml_path = strategy_yaml

    return cfg


# ---------------------------------------------------------------------------
# Paths to patch
# ---------------------------------------------------------------------------
_WBWS_PATH      = "src.broker_support.live.signal_bridge.is_valid_trading_window"
_RISK_MGR_PATH  = "src.broker_support.live.signal_bridge.RiskManager"
_SIG_GEN_PATH   = "src.broker_support.live.signal_bridge.SignalGenerator"
_FILTER_PATH    = "src.broker_support.live.signal_bridge.FilterPipeline"
_BUNDLE_PATH    = "src.broker_support.live.signal_bridge.build_live_data_bundle"
_PATCHER_PATH   = "src.broker_support.live.signal_bridge.LiveConfigPatcher"
_STRATEGY_CFG   = "src.broker_support.live.signal_bridge.StrategyConfig"


# ===========================================================================
# TestLiveDataFetcherParsing
# ===========================================================================

class TestLiveDataFetcherParsing:
    """
    Tests LiveDataFetcher._parse_candles_response() and _fetch_candles() in
    isolation — no strategy pipeline involved.
    """

    def _make_fetcher(self) -> LiveDataFetcher:
        client = MagicMock()
        resolver = MagicMock()
        resolver.instrument_id.return_value = 32
        live_data_cfg = MagicMock()
        live_data_cfg.strategy_bars_to_fetch = 500
        live_data_cfg.htf_bars_to_fetch = 120
        live_data_cfg.strategy_interval = "OneMinute"
        live_data_cfg.htf_interval = "OneHour"
        live_data_cfg.candle_direction = "desc"
        return LiveDataFetcher(client=client, resolver=resolver, config=live_data_cfg)

    def test_parse_returns_dataframe_with_datetime_index(self):
        fetcher = self._make_fetcher()
        response = _make_candle_api_response(n=20)
        df = fetcher._parse_candles_response(response, "strategy")
        assert isinstance(df.index, pd.DatetimeIndex)

    def test_parse_returns_expected_columns(self):
        fetcher = self._make_fetcher()
        response = _make_candle_api_response(n=20)
        df = fetcher._parse_candles_response(response, "strategy")
        assert {"open", "high", "low", "close", "volume"}.issubset(df.columns)

    def test_parse_index_is_tz_naive(self):
        """Pipeline expects tz-naive UTC index."""
        fetcher = self._make_fetcher()
        response = _make_candle_api_response(n=10)
        df = fetcher._parse_candles_response(response, "strategy")
        assert df.index.tz is None

    def test_parse_index_is_sorted_ascending(self):
        """Candles fetched desc must be reversed to asc."""
        fetcher = self._make_fetcher()
        response = _make_candle_api_response(n=10)
        df = fetcher._parse_candles_response(response, "strategy")
        assert df.index.is_monotonic_increasing

    def test_parse_correct_bar_count(self):
        fetcher = self._make_fetcher()
        response = _make_candle_api_response(n=15)
        df = fetcher._parse_candles_response(response, "strategy")
        assert len(df) == 15

    def test_parse_empty_outer_candles_returns_empty_df(self):
        fetcher = self._make_fetcher()
        df = fetcher._parse_candles_response({"candles": []}, "strategy")
        assert df.empty

    def test_parse_empty_inner_candles_returns_empty_df(self):
        fetcher = self._make_fetcher()
        response = {"candles": [{"instrumentId": 32, "candles": []}]}
        df = fetcher._parse_candles_response(response, "strategy")
        assert df.empty

    def test_fetch_raises_on_unknown_symbol(self):
        """Unresolvable symbol → LiveDataFetchError before any HTTP call."""
        fetcher = self._make_fetcher()
        fetcher._resolver.instrument_id.return_value = None
        with pytest.raises(LiveDataFetchError, match="Cannot resolve symbol"):
            fetcher.fetch("UNKNOWN")

    def test_fetch_raises_on_empty_response(self):
        """Empty candle response → LiveDataFetchError."""
        fetcher = self._make_fetcher()
        fetcher._client._make_request.return_value = {"candles": []}
        with pytest.raises(LiveDataFetchError):
            fetcher._fetch_candles(instrument_id=32, interval="OneMinute", count=10, label="test")

    def test_fetch_raises_on_api_exception(self):
        """HTTP layer exception propagates as LiveDataFetchError."""
        fetcher = self._make_fetcher()
        fetcher._client._make_request.side_effect = RuntimeError("network error")
        with pytest.raises(LiveDataFetchError, match="Candle fetch failed"):
            fetcher._fetch_candles(instrument_id=32, interval="OneMinute", count=10, label="test")

    def test_fetch_full_calls_make_request_twice(self):
        """fetch() should call _make_request once for strategy, once for HTF."""
        fetcher = self._make_fetcher()
        fetcher._client._make_request.return_value = _make_candle_api_response(n=10)
        fetcher.fetch("DAX")
        assert fetcher._client._make_request.call_count == 2

    def test_price_values_are_float(self):
        fetcher = self._make_fetcher()
        response = _make_candle_api_response(n=5)
        df = fetcher._parse_candles_response(response, "strategy")
        for col in ["open", "high", "low", "close"]:
            assert df[col].dtype == np.float64, f"{col} not float64"


# ===========================================================================
# TestLiveDataBundleConstruction
# ===========================================================================

class TestLiveDataBundleConstruction:
    """
    Tests build_live_data_bundle() contract — artf loaded from parquet
    is mocked so no real file I/O is needed.
    """

    _LOAD_ARTF = "src.broker_support.live.live_data_bundle._load_artf"

    def _build(self, df_strategy=None, df_htf=None, artf=None, tmp_path=None):
        df_s = df_strategy if df_strategy is not None else _make_ohlcv_df(100)
        df_h = df_htf      if df_htf      is not None else _make_ohlcv_df(50, start="2026-03-13 08:00:00")
        df_a = artf        if artf        is not None else _make_ohlcv_df(24, start="2026-01-01 00:00:00")
        path = (tmp_path or Path("/tmp")) / "artf.parquet"
        with patch(self._LOAD_ARTF, return_value=df_a):
            return build_live_data_bundle(df_s, df_h, path)

    def test_returns_data_bundle(self):
        from src.strategies.contracts.data_contracts import DataBundle
        bundle = self._build()
        assert isinstance(bundle, DataBundle)

    def test_strategy_and_full_are_same_object(self):
        """In live context full == strategy (no WFO slicing)."""
        bundle = self._build()
        assert bundle.full is bundle.strategy

    def test_htf_populated(self):
        df_h = _make_ohlcv_df(50)
        bundle = self._build(df_htf=df_h)
        assert bundle.htf is df_h

    def test_ltf_is_none(self):
        """LTF is never fetched live."""
        bundle = self._build()
        assert bundle.ltf is None

    def test_config_is_none(self):
        """DataConfig not needed in live context — must be None."""
        bundle = self._build()
        assert bundle.config is None

    def test_artf_populated_from_parquet(self):
        df_artf = _make_ohlcv_df(24)
        bundle = self._build(artf=df_artf)
        assert bundle.artf is df_artf

    def test_info_ltf_timeframe_is_string(self):
        """DataInfo.ltf_timeframe is str not Optional — must be '1s' in live context."""
        bundle = self._build()
        assert bundle.info.ltf_timeframe == "1s"
        assert isinstance(bundle.info.ltf_timeframe, str)

    def test_info_bar_counts_match(self):
        df_s = _make_ohlcv_df(100)
        df_h = _make_ohlcv_df(50)
        bundle = self._build(df_strategy=df_s, df_htf=df_h)
        assert bundle.info.strategy_bars == 100
        assert bundle.info.htf_bars == 50
        assert bundle.info.ltf_bars == 0

    def test_validation_is_valid_for_clean_data(self):
        bundle = self._build()
        assert bundle.validation.is_valid is True

    def test_raises_on_empty_strategy_df(self):
        with patch(self._LOAD_ARTF, return_value=_make_ohlcv_df(24)):
            with pytest.raises(LiveDataBundleError, match="strategy"):
                build_live_data_bundle(pd.DataFrame(), _make_ohlcv_df(50), Path("/tmp/x.parquet"))

    def test_raises_on_missing_ohlc_column(self):
        bad_df = _make_ohlcv_df(50).drop(columns=["close"])
        with patch(self._LOAD_ARTF, return_value=_make_ohlcv_df(24)):
            with pytest.raises(LiveDataBundleError, match="missing required columns"):
                build_live_data_bundle(bad_df, _make_ohlcv_df(50), Path("/tmp/x.parquet"))

    def test_raises_on_non_datetime_index(self):
        bad_df = _make_ohlcv_df(50).reset_index(drop=True)
        with patch(self._LOAD_ARTF, return_value=_make_ohlcv_df(24)):
            with pytest.raises(LiveDataBundleError, match="DatetimeIndex"):
                build_live_data_bundle(bad_df, _make_ohlcv_df(50), Path("/tmp/x.parquet"))

    def test_raises_when_artf_file_missing(self):
        """_load_artf raises LiveDataBundleError if path does not exist."""
        with pytest.raises(LiveDataBundleError, match="ARTF parquet not found"):
            build_live_data_bundle(
                _make_ohlcv_df(100),
                _make_ohlcv_df(50),
                Path("/nonexistent/artf.parquet"),
            )


# ===========================================================================
# TestLiveConfigPatcherPatch
# ===========================================================================

class TestLiveConfigPatcherPatch:
    """
    Tests LiveConfigPatcher.patch() — verifies correct dict mutations
    and that the original is never mutated.
    """

    def _raw_dict(self) -> dict:
        """Minimal strategy YAML dict with fields patcher touches."""
        return {
            "data": {
                "paths": {
                    "strategy_ohlcv": "old/strategy.parquet",
                    "htf_ohlcv":      "old/htf.parquet",
                    "artf_ohlcv":     "old/artf.parquet",
                    "ltf_ohlcv":      "old/ltf.parquet",
                },
                "date_range": ["2024-01-01", "2025-01-01"],
            },
            "execution": {"mode": "full"},
            "output":    {"reports": {"enabled": True}},
            "filters": {
                "time_filter": {"start": "08:30", "end": "20:30"},
            },
            "trade_management": {
                "position_control": {
                    "max_positions": 1,
                    "pyramiding_enabled": False,
                }
            },
        }

    def _make_patcher(self, artf_path: str = "data/artf.parquet") -> LiveConfigPatcher:
        bs = MagicMock()
        bs.live_data.artf_ohlcv_path = Path(artf_path)
        return LiveConfigPatcher(bs)

    def test_artf_path_patched_correctly(self):
        raw = self._raw_dict()
        patched = self._make_patcher("data/artf.parquet").patch(raw)
        assert Path(patched["data"]["paths"]["artf_ohlcv"]) == Path("data/artf.parquet")

    def test_strategy_ohlcv_gets_sentinel(self):
        """strategy_ohlcv replaced with artf path as sentinel."""
        raw = self._raw_dict()
        patched = self._make_patcher("data/artf.parquet").patch(raw)
        assert Path(patched["data"]["paths"]["strategy_ohlcv"]) == Path("data/artf.parquet")

    def test_htf_ohlcv_gets_sentinel(self):
        raw = self._raw_dict()
        patched = self._make_patcher("data/artf.parquet").patch(raw)
        assert Path(patched["data"]["paths"]["htf_ohlcv"]) == Path("data/artf.parquet")

    def test_ltf_ohlcv_set_to_none(self):
        """LTF not fetched live — must be None."""
        raw = self._raw_dict()
        patched = self._make_patcher().patch(raw)
        assert patched["data"]["paths"]["ltf_ohlcv"] is None

    def test_date_range_set_to_none(self):
        """No date filtering on live data."""
        raw = self._raw_dict()
        patched = self._make_patcher().patch(raw)
        assert patched["data"]["date_range"] is None

    def test_execution_mode_set_to_core(self):
        raw = self._raw_dict()
        patched = self._make_patcher().patch(raw)
        assert patched["execution"]["mode"] == "core"

    def test_reports_disabled(self):
        raw = self._raw_dict()
        patched = self._make_patcher().patch(raw)
        assert patched["output"]["reports"]["enabled"] is False

    def test_original_dict_not_mutated(self):
        """patch() must deep-copy — original must be unchanged."""
        raw = self._raw_dict()
        original_mode = raw["execution"]["mode"]
        self._make_patcher().patch(raw)
        assert raw["execution"]["mode"] == original_mode
        assert raw["data"]["date_range"] == ["2024-01-01", "2025-01-01"]

    def test_filter_params_not_touched(self):
        """Backtested filter params must never be patched."""
        raw = self._raw_dict()
        patched = self._make_patcher().patch(raw)
        assert patched["filters"]["time_filter"]["start"] == "08:30"
        assert patched["filters"]["time_filter"]["end"]   == "20:30"

    def test_position_control_not_touched(self):
        """position_control must not be patched — backtested constraint."""
        raw = self._raw_dict()
        patched = self._make_patcher().patch(raw)
        pc = patched["trade_management"]["position_control"]
        assert pc["max_positions"] == 1
        assert pc["pyramiding_enabled"] is False


# ===========================================================================
# Shared SignalBridge fixture helpers
# ===========================================================================

def _make_trade_params(
    direction: str = "BUY",
    entry: float = 20_000.0,
) -> MagicMock:
    """Return a MagicMock mimicking TradeParameters for the given direction."""
    is_long = direction == "BUY"
    tp = MagicMock()
    tp.entry_price_executed = entry
    tp.entry_price_mid      = entry
    tp.stop_loss_trigger    = entry - 50.0 if is_long else entry + 50.0
    tp.take_profit_trigger  = entry + 100.0 if is_long else entry - 100.0
    tp.sl_distance          = 50.0
    tp.tp_distance          = 100.0
    tp.risk_reward_ratio    = 2.0
    tp.atr_value            = 25.0
    tp.spread_applied       = True
    tp.spread_points        = 1.5
    tp.atr_multiplier       = 1.5
    tp.tp_mode              = "atr"
    tp.comment              = "ok"
    return tp


def _make_signal_frame(
    last_ts: pd.Timestamp,
    signal_code: int,          # 0=none, 1=buy, 2=sell (or -1=sell depending on project)
    n: int = 100,
) -> MagicMock:
    """Return a MagicMock mimicking SignalFrame with a signal at last_ts."""
    idx = pd.date_range(end=last_ts, periods=n, freq="1min")
    codes = pd.Series(0, index=idx)
    codes.iloc[-1] = signal_code

    sf = MagicMock()
    sf.signals = codes
    counts = {"buy": 1 if signal_code == 1 else 0, "sell": 1 if signal_code == 2 else 0, "total": 1 if signal_code != 0 else 0}
    sf.count_by_type.return_value = counts
    return sf


def _wire_signal_bridge(
    tmp_path: Path,
    *,
    signal_code: int = 1,           # 1=BUY on last bar, 2=SELL, 0=no signal
    risk_params: MagicMock | None = None,
    wbws_result: bool = True,
    wbws_enabled: bool = True,
    allowed_hours_utc: list | None = None,
    strategy_yaml_content: dict | None = None,
) -> tuple[SignalBridge, MagicMock]:
    """
    Build a fully mocked SignalBridge ready for get_signal().

    Mocks:
      - LiveConfigPatcher.load_and_patch  → returns minimal dict
      - StrategyConfig.from_dict          → MagicMock
      - _load_raw_yaml                    → minimal position_control dict
      - LiveDataFetcher.fetch             → two DataFrames
      - build_live_data_bundle            → MagicMock DataBundle
      - SignalGenerator                   → produces signal at last bar
      - FilterPipeline                    → passes through signal
      - RiskManager                       → approves or rejects
      - is_valid_trading_window           → wbws_result

    Returns (bridge, mock_fetcher).
    """
    bs_config = _make_bs_config(
        tmp_path,
        allowed_hours_utc=allowed_hours_utc,
        wbws_enabled=wbws_enabled,
    )

    df_strategy = _make_ohlcv_df(100)
    df_htf      = _make_ohlcv_df(50, start="2026-03-13 08:00:00")
    last_ts     = df_strategy.index[-1]

    mock_fetcher = MagicMock()
    mock_fetcher.fetch.return_value = (df_strategy, df_htf)

    # Minimal dict that patcher would return — StrategyConfig.from_dict is mocked anyway
    patched_dict = {"execution": {"mode": "core"}}

    # position_control from raw YAML
    raw_yaml_data = strategy_yaml_content or {
        "backtester_metadata": {"candidate_id": "c424a0e04327"},
        "trade_management": {
            "position_control": {"max_positions": 1, "pyramiding_enabled": False}
        },
    }

    # SignalFrame with signal at last bar
    signal_frame = _make_signal_frame(last_ts, signal_code)

    # FilterPipelineResult mock
    from src.strategies.contracts.signal_contracts import SignalType
    filter_result = MagicMock()
    filter_result.raw_count   = 5
    filter_result.final_count = 1 if signal_code != 0 else 0
    filter_result.pass_rate   = 20.0 if signal_code != 0 else 0.0
    filter_result.final_signals = signal_frame

    # RiskManager mock
    risk_mgr_instance = MagicMock()
    if risk_params is None and signal_code != 0:
        direction = "BUY" if signal_code == 1 else "SELL"
        risk_params = _make_trade_params(direction)
    risk_mgr_instance.compute_trade_parameters.return_value = risk_params
    risk_mgr_instance.get_risk_summary.return_value = {}

    with (
        patch(_PATCHER_PATH + ".load_and_patch", return_value=patched_dict),
        patch(_STRATEGY_CFG + ".from_dict",      return_value=MagicMock()),
        patch("src.broker_support.live.signal_bridge._load_raw_yaml", return_value=raw_yaml_data),
    ):
        bridge = SignalBridge(bs_config=bs_config, fetcher=mock_fetcher)

    # Attach mocks that activate during get_signal()
    bridge._mock_signal_frame  = signal_frame
    bridge._mock_filter_result = filter_result
    bridge._mock_risk_instance = risk_mgr_instance
    bridge._mock_last_ts       = last_ts
    bridge._mock_df_strategy   = df_strategy
    bridge._mock_df_htf        = df_htf
    bridge._mock_wbws_result   = wbws_result

    return bridge, mock_fetcher


def _run_get_signal(bridge: SignalBridge, wbws_result: bool = True) -> OrderSignal | None:
    """
    Run bridge.get_signal() with all pipeline internals mocked.
    Wraps the four heavy dependencies consistently across all tests.
    """
    from src.strategies.contracts.signal_contracts import SignalType

    mock_bundle = MagicMock()
    mock_bundle.strategy   = bridge._mock_df_strategy
    mock_bundle.full       = bridge._mock_df_strategy
    mock_bundle.htf        = bridge._mock_df_htf
    mock_bundle.artf       = _make_ohlcv_df(24)

    mock_sg_instance = MagicMock()
    mock_sg_instance.generate_signals.return_value = bridge._mock_signal_frame

    mock_fp_instance = MagicMock()
    mock_fp_instance.apply_filters.return_value = bridge._mock_filter_result

    with (
        patch(_BUNDLE_PATH, return_value=mock_bundle),
        patch(_SIG_GEN_PATH, return_value=mock_sg_instance),
        patch(_FILTER_PATH,  return_value=mock_fp_instance),
        patch(_RISK_MGR_PATH, return_value=bridge._mock_risk_instance),
        patch(_WBWS_PATH,    return_value=wbws_result),
    ):
        return bridge.get_signal()


# ===========================================================================
# TestSignalBridgeGetSignal
# ===========================================================================

class TestSignalBridgeGetSignal:
    """
    Full SignalBridge.get_signal() pipeline — signal found, not found, rejected.
    """

    def test_returns_order_signal_when_buy_on_last_bar(self, tmp_path):
        bridge, _ = _wire_signal_bridge(tmp_path, signal_code=1)
        result = _run_get_signal(bridge, wbws_result=True)
        assert isinstance(result, OrderSignal)
        assert result.direction == "BUY"

    def test_returns_order_signal_when_sell_on_last_bar(self, tmp_path):
        bridge, _ = _wire_signal_bridge(tmp_path, signal_code=2)
        result = _run_get_signal(bridge, wbws_result=True)
        assert isinstance(result, OrderSignal)
        assert result.direction == "SELL"

    def test_returns_none_when_no_signal_on_last_bar(self, tmp_path):
        bridge, _ = _wire_signal_bridge(tmp_path, signal_code=0)
        result = _run_get_signal(bridge, wbws_result=True)
        assert result is None

    def test_returns_none_when_risk_manager_rejects(self, tmp_path):
        """RiskManager returns None → pipeline returns None."""
        bridge, _ = _wire_signal_bridge(tmp_path, signal_code=1, risk_params=None)
        # Override: force risk_mgr to return None explicitly
        bridge._mock_risk_instance.compute_trade_parameters.return_value = None
        result = _run_get_signal(bridge, wbws_result=True)
        assert result is None

    def test_fetcher_called_with_correct_symbol(self, tmp_path):
        bridge, mock_fetcher = _wire_signal_bridge(tmp_path, signal_code=1)
        _run_get_signal(bridge, wbws_result=True)
        mock_fetcher.fetch.assert_called_once_with("DAX")

    def test_order_signal_symbol_matches_config(self, tmp_path):
        bridge, _ = _wire_signal_bridge(tmp_path, signal_code=1)
        result = _run_get_signal(bridge, wbws_result=True)
        assert result.symbol == "DAX"

    def test_order_signal_candidate_id_extracted(self, tmp_path):
        bridge, _ = _wire_signal_bridge(tmp_path, signal_code=1)
        result = _run_get_signal(bridge, wbws_result=True)
        assert result.candidate_id == "c424a0e04327"

    def test_order_signal_max_positions_from_yaml(self, tmp_path):
        """max_positions=1 comes from strategy YAML position_control."""
        bridge, _ = _wire_signal_bridge(tmp_path, signal_code=1)
        result = _run_get_signal(bridge, wbws_result=True)
        assert result.max_positions == 1

    def test_order_signal_sl_tp_from_risk_manager(self, tmp_path):
        tp = _make_trade_params("BUY", entry=20_100.0)
        bridge, _ = _wire_signal_bridge(tmp_path, signal_code=1, risk_params=tp)
        result = _run_get_signal(bridge, wbws_result=True)
        assert result.stop_loss_rate  == tp.stop_loss_trigger
        assert result.take_profit_rate == tp.take_profit_trigger

    def test_order_signal_entry_price_positive(self, tmp_path):
        bridge, _ = _wire_signal_bridge(tmp_path, signal_code=1)
        result = _run_get_signal(bridge, wbws_result=True)
        assert result.entry_price_mid > 0

    def test_order_signal_rr_from_risk_manager(self, tmp_path):
        tp = _make_trade_params("BUY")
        tp.risk_reward_ratio = 3.5
        bridge, _ = _wire_signal_bridge(tmp_path, signal_code=1, risk_params=tp)
        result = _run_get_signal(bridge, wbws_result=True)
        assert result.risk_reward_ratio == pytest.approx(3.5)

    def test_order_signal_is_frozen_dataclass(self, tmp_path):
        """OrderSignal is frozen — mutations must raise."""
        bridge, _ = _wire_signal_bridge(tmp_path, signal_code=1)
        result = _run_get_signal(bridge, wbws_result=True)
        with pytest.raises((AttributeError, TypeError)):
            result.direction = "HOLD"  # type: ignore[misc]

    def test_risk_manager_called_with_last_bar_timestamp(self, tmp_path):
        """RiskManager must receive the exact last-bar timestamp."""
        bridge, _ = _wire_signal_bridge(tmp_path, signal_code=1)
        _run_get_signal(bridge, wbws_result=True)
        call_kwargs = bridge._mock_risk_instance.compute_trade_parameters.call_args[1]
        assert call_kwargs["timestamp"] == bridge._mock_last_ts

    def test_meta_contains_spread_and_atr_info(self, tmp_path):
        bridge, _ = _wire_signal_bridge(tmp_path, signal_code=1)
        result = _run_get_signal(bridge, wbws_result=True)
        assert "spread_applied"  in result.meta
        assert "atr_multiplier"  in result.meta


# ===========================================================================
# TestSignalBridgeWBWS
# ===========================================================================

class TestSignalBridgeWBWS:
    """
    WBWS+ gate: verifies wbws_window_valid is set correctly on OrderSignal.
    Gate is non-blocking — signal is returned regardless; flag reflects window state.
    """

    def test_wbws_valid_true_when_in_window(self, tmp_path):
        bridge, _ = _wire_signal_bridge(tmp_path, signal_code=1, wbws_result=True)
        result = _run_get_signal(bridge, wbws_result=True)
        assert result.wbws_window_valid is True

    def test_wbws_valid_false_when_outside_window(self, tmp_path):
        bridge, _ = _wire_signal_bridge(tmp_path, signal_code=1, wbws_result=False)
        result = _run_get_signal(bridge, wbws_result=False)
        assert result.wbws_window_valid is False

    def test_signal_returned_even_when_wbws_closed(self, tmp_path):
        """Gate is non-blocking — OrderSignal is returned even outside window."""
        bridge, _ = _wire_signal_bridge(tmp_path, signal_code=1, wbws_result=False)
        result = _run_get_signal(bridge, wbws_result=False)
        assert result is not None

    def test_wbws_always_false_when_window_disabled(self, tmp_path):
        """trading_window.enabled=False → wbws_window_valid always False."""
        bridge, _ = _wire_signal_bridge(
            tmp_path, signal_code=1, wbws_result=True, wbws_enabled=False
        )
        result = _run_get_signal(bridge, wbws_result=True)
        # When tw.enabled is False the expression is `False and is_valid_trading_window(...)`
        # which short-circuits to False without calling is_valid_trading_window
        assert result.wbws_window_valid is False

    def test_none_signal_when_no_bar_signal_regardless_of_wbws(self, tmp_path):
        """No signal on last bar → None even if WBWS+ is open."""
        bridge, _ = _wire_signal_bridge(tmp_path, signal_code=0, wbws_result=True)
        result = _run_get_signal(bridge, wbws_result=True)
        assert result is None


# ===========================================================================
# TestCheckPyramiding
# ===========================================================================

class TestCheckPyramiding:
    """
    Tests for _check_pyramiding() from run_signal.py.

    Imports and tests the function directly — no full pipeline needed.
    The function calls client._make_request("GET", "api/v1/trading/info/demo/portfolio").
    """

    # Import at class level to keep tests clean
    @pytest.fixture(autouse=True)
    def _import(self):
        from scripts.broker_support.run_signal import _check_pyramiding
        self._check_pyramiding = _check_pyramiding

    def _make_portfolio_response(self, positions: list) -> dict:
        return {"clientPortfolio": {"positions": positions}}

    def _make_position(self, instrument_id: int) -> dict:
        return {"instrumentID": instrument_id, "positionID": 12345, "isOpen": True}

    def _make_client_and_resolver(self, instrument_id: int, portfolio_positions: list):
        client = MagicMock()
        client._make_request.return_value = self._make_portfolio_response(portfolio_positions)
        resolver = MagicMock()
        resolver.instrument_id.return_value = instrument_id
        return client, resolver

    def test_no_existing_positions_does_not_exit(self):
        """0 open positions for DAX (instrumentID=32) → no sys.exit."""
        client, resolver = self._make_client_and_resolver(32, [])
        # Should return normally (no SystemExit)
        self._check_pyramiding(client=client, resolver=resolver, symbol="DAX", max_positions=1)

    def test_exits_zero_when_max_positions_reached(self):
        """1 open position and max_positions=1 → sys.exit(0)."""
        positions = [self._make_position(32)]
        client, resolver = self._make_client_and_resolver(32, positions)
        with pytest.raises(SystemExit) as exc_info:
            self._check_pyramiding(client=client, resolver=resolver, symbol="DAX", max_positions=1)
        assert exc_info.value.code == 0

    def test_exits_zero_when_exceeds_max_positions(self):
        """2 open positions and max_positions=1 → sys.exit(0)."""
        positions = [self._make_position(32), self._make_position(32)]
        client, resolver = self._make_client_and_resolver(32, positions)
        with pytest.raises(SystemExit) as exc_info:
            self._check_pyramiding(client=client, resolver=resolver, symbol="DAX", max_positions=1)
        assert exc_info.value.code == 0

    def test_filters_by_instrument_id(self):
        """Positions for a different instrumentID must not count."""
        positions = [self._make_position(999)]  # different instrument
        client, resolver = self._make_client_and_resolver(32, positions)
        # Should not exit — position is for instrument 999, not 32
        self._check_pyramiding(client=client, resolver=resolver, symbol="DAX", max_positions=1)

    def test_mixed_instruments_counts_only_target(self):
        """1 DAX position + 1 other → counts as 1 for DAX."""
        positions = [self._make_position(32), self._make_position(999)]
        client, resolver = self._make_client_and_resolver(32, positions)
        with pytest.raises(SystemExit) as exc_info:
            self._check_pyramiding(client=client, resolver=resolver, symbol="DAX", max_positions=1)
        assert exc_info.value.code == 0

    def test_exits_one_on_portfolio_fetch_failure(self):
        """Portfolio API failure → sys.exit(1) (hard error, not a skip)."""
        client = MagicMock()
        client._make_request.side_effect = RuntimeError("API down")
        resolver = MagicMock()
        resolver.instrument_id.return_value = 32
        with pytest.raises(SystemExit) as exc_info:
            self._check_pyramiding(client=client, resolver=resolver, symbol="DAX", max_positions=1)
        assert exc_info.value.code == 1

    def test_exits_one_on_unresolvable_symbol(self):
        """Unresolvable symbol → sys.exit(1)."""
        client = MagicMock()
        resolver = MagicMock()
        resolver.instrument_id.return_value = None
        with pytest.raises(SystemExit) as exc_info:
            self._check_pyramiding(client=client, resolver=resolver, symbol="UNKNOWN", max_positions=1)
        assert exc_info.value.code == 1

    def test_portfolio_fetch_uses_demo_endpoint(self):
        """Must call /demo/portfolio not /portfolio."""
        client, resolver = self._make_client_and_resolver(32, [])
        self._check_pyramiding(client=client, resolver=resolver, symbol="DAX", max_positions=1)
        call_args = client._make_request.call_args
        endpoint = call_args[0][1] if len(call_args[0]) > 1 else call_args[1].get("path", "")
        assert "demo" in endpoint

    def test_max_positions_two_allows_one_open(self):
        """max_positions=2 with 1 open → should NOT exit."""
        positions = [self._make_position(32)]
        client, resolver = self._make_client_and_resolver(32, positions)
        # Should return normally
        self._check_pyramiding(client=client, resolver=resolver, symbol="DAX", max_positions=2)