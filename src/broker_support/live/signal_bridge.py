"""
SignalBridge — runs the strategy pipeline on live data and returns an OrderSignal.

Pipeline stages executed (in order):
  1. LiveDataFetcher.fetch()         → df_strategy, df_htf
  2. LiveDataBundle.build()          → DataBundle
  3. SignalGenerator.generate()      → SignalFrame
  4. FilterPipeline.apply_filters()  → FilterPipelineResult
  5. Last-bar signal check           → (timestamp, direction) or None
  6. RiskManager.compute_trade_parameters() → TradeParameters (SL/TP/ATR)
  7. is_valid_trading_window()       → WBWS+ gate (non-blocking — sets flag)

Stages NOT run:
  - TradeSimulator  — simulates historical exits, irrelevant for live open
  - DataLoader      — replaced by LiveDataFetcher + LiveDataBundle
  - ReportGenerator — no HTML reports in live context

Trade constraints (from strategy YAML position_control):
  - max_risk_percentile : enforced by RiskManager (uses ARTF for price normalisation)
  - max_positions       : carried in OrderSignal.max_positions — enforced by run_signal.py
                          _check_pyramiding() before OrderRouter.open_position()
  - pyramiding_enabled  : same enforcement path as max_positions
  - close_on_opposite   : falls out correctly — pyramiding guard blocks all new opens
                          when max_positions already reached, regardless of direction

Returns:
  OrderSignal  — if the last bar has a signal AND RiskManager approves it.
  None         — if no signal on the last bar, or RiskManager rejects it.

WBWS+ gate is non-blocking: if the signal exists but the hour is outside
the allowed window, OrderSignal is returned with wbws_window_valid=False.
The caller (run_signal.py) decides whether to place the order.
This allows Stage 1 dry-run to always show what would be signalled,
even outside the execution window.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
from loguru import logger

from src.broker_support.config.broker_support_config import BrokerSupportConfig
from src.broker_support.live.live_config_patcher import LiveConfigPatcher
from src.broker_support.live.live_data_bundle import build_live_data_bundle
from src.broker_support.live.live_data_fetcher import LiveDataFetcher
from src.broker_support.live.order_signal import OrderSignal
from src.broker_support.utils.time_utils import is_valid_trading_window
from src.strategies.config.config_schema import StrategyConfig
from src.strategies.contracts.signal_contracts import SignalType
from src.strategies.core.filter_pipeline import FilterPipeline
from src.strategies.core.signal_generator import SignalGenerator
from src.strategies.market.risk_manager import RiskManager


class SignalBridgeError(Exception):
    """Raised on unrecoverable signal bridge failure."""


class SignalBridge:
    """
    Orchestrates the strategy pipeline for live signal generation.

    Instantiated once per run_signal.py invocation.
    Not designed for multi-threaded use.
    """

    def __init__(
        self,
        bs_config: BrokerSupportConfig,
        fetcher: LiveDataFetcher,
    ) -> None:
        self._bs = bs_config
        self._fetcher = fetcher

        # Load and patch strategy config once at construction
        patched_dict = LiveConfigPatcher.load_and_patch(bs_config)
        self._strategy_config: StrategyConfig = StrategyConfig.from_dict(patched_dict)

        # Extract candidate_id + position_control from YAML (for OrderSignal tagging)
        raw_yaml = _load_raw_yaml(bs_config.strategy.yaml_path)
        meta = raw_yaml.get("backtester_metadata", {})
        self._candidate_id: str = str(meta.get("candidate_id", "unknown"))[:12]
        pos_ctrl = raw_yaml.get("trade_management", {}).get("position_control", {})
        self._max_positions: int = int(pos_ctrl.get("max_positions", 1))
        self._pyramiding_enabled: bool = bool(pos_ctrl.get("pyramiding_enabled", False))

        logger.info(
            f"SignalBridge initialised: "
            f"symbol={bs_config.execution.symbol}, "
            f"candidate_id={self._candidate_id}, "
            f"strategy_bars={bs_config.live_data.strategy_bars_to_fetch}, "
            f"htf_bars={bs_config.live_data.htf_bars_to_fetch}"
        )

    def get_signal(self) -> Optional[OrderSignal]:
        """
        Run the full pipeline and return an OrderSignal for the latest bar.

        Returns:
            OrderSignal if the last bar has a valid signal that passes
            RiskManager. None otherwise.

        Logs clearly at every decision point so Stage 1 dry-run output
        is informative even when no signal is found.

        Return-None paths are distinguished by a terminal log line:
            result=NO_SIGNAL        — no signal on the last bar
            result=RISK_REJECTED    — signal found but RiskManager rejected it
        The caller uses these to log an accurate summary message.
        """
        symbol = self._bs.execution.symbol
        cfg = self._strategy_config

        # ── Stage 1: Fetch live data ──────────────────────────────────────
        logger.info("SignalBridge: fetching live candles …")
        df_strategy, df_htf = self._fetcher.fetch(symbol)

        # ── Stage 2: Build DataBundle ─────────────────────────────────────
        bundle = build_live_data_bundle(
            df_strategy=df_strategy,
            df_htf=df_htf,
            artf_ohlcv_path=self._bs.live_data.artf_ohlcv_path,
        )

        # ── Stage 3: Signal generation ────────────────────────────────────
        logger.info("SignalBridge: generating signals …")
        generator = SignalGenerator(config=cfg, mode="core")
        signal_frame = generator.generate_signals(bundle)
        counts = signal_frame.count_by_type()
        logger.info(
            f"Raw signals: buy={counts['buy']}, sell={counts['sell']}, "
            f"total={counts['total']}"
        )

        # ── Stage 4: Filter pipeline ──────────────────────────────────────
        logger.info("SignalBridge: running filter pipeline …")
        pipeline = FilterPipeline(config=cfg, mode="core")
        filter_result = pipeline.apply_filters(
            signal_frame=signal_frame,
            df=bundle.strategy,
            mode="core",
        )
        logger.info(
            f"After filters: {filter_result.raw_count} → {filter_result.final_count} signals "
            f"(pass_rate={filter_result.pass_rate:.1f}%)"
        )

        # ── Stage 5: Last-bar signal check ────────────────────────────────
        last_ts = bundle.strategy.index[-1]
        last_signal = _get_last_bar_signal(filter_result.final_signals.signals, last_ts)

        if last_signal is None:
            logger.info(
                f"SignalBridge: no signal on last bar ({last_ts}). "
                f"Latest signals: {_describe_recent_signals(filter_result.final_signals.signals)}"
            )
            logger.info("SignalBridge: result=NO_SIGNAL")
            return None

        direction = "BUY" if last_signal == SignalType.BUY else "SELL"
        is_long = last_signal == SignalType.BUY
        bid_price = float(bundle.strategy.at[last_ts, "close"])

        logger.info(
            f"SignalBridge: signal found at last bar — "
            f"{direction} @ {last_ts}, bid={bid_price:.2f}"
        )

        # ── Stage 6: RiskManager — compute SL/TP ─────────────────────────
        logger.info("SignalBridge: computing trade parameters …")
        risk_manager = RiskManager(
            config=cfg,
            ohlcv_data=bundle.full,
            ohlcv_artf=bundle.artf,
            mode="core",
            cache_manager=None,
        )
        trade_params = risk_manager.compute_trade_parameters(
            timestamp=last_ts,
            bid_price=bid_price,
            is_long=is_long,
        )

        if trade_params is None:
            logger.warning(
                f"SignalBridge: RiskManager rejected trade at {last_ts}. "
                f"Risk summary: {risk_manager.get_risk_summary()}"
            )
            logger.info("SignalBridge: result=RISK_REJECTED")
            return None

        logger.info(
            f"SignalBridge: trade params — "
            f"entry={trade_params.entry_price_executed:.2f}, "
            f"sl={trade_params.stop_loss_trigger:.2f} "
            f"(dist={trade_params.sl_distance:.2f}pts), "
            f"tp={trade_params.take_profit_trigger:.2f} "
            f"(dist={trade_params.tp_distance:.2f}pts), "
            f"rr={trade_params.risk_reward_ratio:.1f}x, "
            f"atr={trade_params.atr_value:.2f}"
        )

        # ── Stage 7: WBWS+ window check (non-blocking) ───────────────────
        tw = self._bs.trading_window
        wbws_valid = tw.enabled and is_valid_trading_window(
            dt=last_ts.to_pydatetime() if hasattr(last_ts, "to_pydatetime") else last_ts,
            allowed_hours_utc=tw.allowed_hours_utc,
            skip_hours_utc=tw.skip_hours_utc,
        )

        if not wbws_valid and tw.enabled:
            logger.warning(
                f"SignalBridge: signal found but WBWS+ window is closed "
                f"(UTC hour={pd.Timestamp(last_ts).hour}, "
                f"allowed={tw.allowed_hours_utc}). "
                f"OrderSignal returned with wbws_window_valid=False — "
                f"caller decides whether to place."
            )

        # ── Build OrderSignal ─────────────────────────────────────────────
        signal = OrderSignal(
            timestamp=last_ts,
            symbol=symbol,
            direction=direction,
            entry_price_mid=trade_params.entry_price_mid,
            stop_loss_rate=trade_params.stop_loss_trigger,
            take_profit_rate=trade_params.take_profit_trigger,
            atr_value=trade_params.atr_value,
            sl_distance=trade_params.sl_distance,
            tp_distance=trade_params.tp_distance,
            risk_reward_ratio=trade_params.risk_reward_ratio,
            candidate_id=self._candidate_id,
            wbws_window_valid=wbws_valid,
            max_positions=self._max_positions,
            meta={
                "spread_applied": trade_params.spread_applied,
                "spread_points":  trade_params.spread_points,
                "atr_multiplier": trade_params.atr_multiplier,
                "tp_mode":        trade_params.tp_mode,
                "risk_comment":   trade_params.comment,
            },
        )

        logger.info(f"SignalBridge: {signal.summary()}")
        return signal


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_last_bar_signal(
    signals: pd.Series,
    last_ts: pd.Timestamp,
) -> Optional[SignalType]:
    """
    Return SignalType at last_ts, or None if no signal.

    Checks the exact last timestamp only. We do not look back further
    — a signal on an older bar is stale and should not trigger an order.
    """
    if last_ts not in signals.index:
        return None
    code = int(signals.loc[last_ts])
    return SignalType.from_code(code)


def _describe_recent_signals(signals: pd.Series, n: int = 5) -> str:
    """Return a brief description of the last N non-zero signals for logging."""
    active = signals[signals != 0].tail(n)
    if active.empty:
        return "no recent signals"
    parts = [
        f"{ts} → {'BUY' if code == 1 else 'SELL'}"
        for ts, code in active.items()
    ]
    return ", ".join(parts)


def _load_raw_yaml(path: Path) -> dict:
    import yaml
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}