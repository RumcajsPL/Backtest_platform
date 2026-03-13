#!/usr/bin/env python
"""
run_signal_loop.py — polls for a signal every 60 seconds, places ONE order, then stops.

Behaviour:
  - Runs the full strategy pipeline once per minute.
  - If no signal on the last bar: logs and waits 60 seconds.
  - If a signal is found but WBWS+ window is closed: logs and waits (no order).
  - If a signal is found and WBWS+ window is open: places the order and exits 0.
  - _check_pyramiding() still runs before OrderRouter — safe to leave running.
  - Ctrl+C exits cleanly at any time.

Usage:
    python scripts/broker_support/run_signal_loop.py
    python scripts/broker_support/run_signal_loop.py --verbose
    python scripts/broker_support/run_signal_loop.py --config configs/broker_support/broker_support_config.yaml
"""
import argparse
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

from loguru import logger

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.broker_support.client.client import EToroClient
from src.broker_support.config.broker_support_config import BrokerSupportConfig
from src.broker_support.enrichment.instrument_resolver import InstrumentResolver
from src.broker_support.execution.order_router import OrderRouter, OutsideTradingHoursError
from src.broker_support.live.live_data_fetcher import LiveDataFetcher
from src.broker_support.live.signal_bridge import SignalBridge

DEFAULT_CONFIG  = "configs/broker_support/broker_support_config.yaml"
DEFAULT_LOG_DIR = "outputs/broker_support/logs"
POLL_INTERVAL   = 60  # seconds


def _configure_logging(log_dir: str, verbose: bool) -> None:
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    level = "DEBUG" if verbose else "INFO"
    logger.remove()
    logger.add(
        sys.stderr,
        level=level,
        colorize=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
               "<level>{level: <8}</level> | {message}",
    )
    logger.add(
        str(log_path / "run_signal_loop_{time:YYYY-MM-DD}.log"),
        level="DEBUG",
        rotation="00:00",
        retention="30 days",
        encoding="utf-8",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{line} | {message}",
    )


def _check_pyramiding(client, resolver, symbol: str, max_positions: int) -> bool:
    """
    Returns True if safe to place (open positions < max_positions).
    Returns False if already at limit — caller skips the order.
    Exits 1 on API failure.
    """
    instrument_id = resolver.instrument_id(symbol)
    if instrument_id is None:
        logger.error(f"Pyramiding check: cannot resolve '{symbol}'. Verify instrument_map.yaml.")
        sys.exit(1)
    try:
        portfolio = client._make_request("GET", "api/v1/trading/info/demo/portfolio")
    except Exception as exc:
        logger.error(f"Pyramiding check: portfolio fetch failed: {exc}")
        sys.exit(1)

    positions = portfolio.get("clientPortfolio", {}).get("positions", [])
    open_count = sum(1 for p in positions if p.get("instrumentID") == instrument_id)
    logger.info(
        f"Pyramiding check: {open_count} open position(s) for {symbol} "
        f"(max_positions={max_positions})"
    )
    if open_count >= max_positions:
        logger.info(
            f"ORDER SKIPPED: {open_count}/{max_positions} position(s) already open. "
            f"pyramiding_enabled=False."
        )
        return False
    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Signal loop — polls every 60s, places one order, then stops."
    )
    parser.add_argument("--config",   default=DEFAULT_CONFIG)
    parser.add_argument("--log-dir",  default=DEFAULT_LOG_DIR)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    _configure_logging(args.log_dir, args.verbose)

    logger.info("=" * 60)
    logger.info("run_signal_loop.py — ONE-SHOT loop (stops after first order)")
    logger.info(f"  config        : {args.config}")
    logger.info(f"  poll interval : {POLL_INTERVAL}s")
    logger.info("  Ctrl+C to stop at any time")
    logger.info("=" * 60)

    # ── Load config and build infrastructure once ─────────────────────────
    config_path = Path(args.config)
    try:
        bs_config = BrokerSupportConfig.from_yaml(config_path)
    except Exception as exc:
        logger.error(f"Failed to load config: {exc}")
        sys.exit(1)

    try:
        client   = EToroClient()
        resolver = InstrumentResolver(map_path=bs_config.execution.instrument_map_path)
        fetcher  = LiveDataFetcher(client=client, resolver=resolver, config=bs_config.live_data)
        bridge   = SignalBridge(bs_config=bs_config, fetcher=fetcher)
        router   = OrderRouter(client=client, resolver=resolver)
    except Exception as exc:
        logger.error(f"Failed to initialise infrastructure: {exc}")
        sys.exit(1)

    iteration = 0

    while True:
        iteration += 1
        now_utc = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        logger.info(f"── Poll #{iteration} at {now_utc} ──")

        # ── Run pipeline ──────────────────────────────────────────────────
        try:
            signal = bridge.get_signal()
        except Exception as exc:
            logger.error(f"Pipeline error: {exc}. Retrying in {POLL_INTERVAL}s …")
            time.sleep(POLL_INTERVAL)
            continue

        # ── No signal ─────────────────────────────────────────────────────
        if signal is None:
            logger.info(f"No signal on last bar. Next poll in {POLL_INTERVAL}s …")
            time.sleep(POLL_INTERVAL)
            continue

        # ── Signal found — log it always ──────────────────────────────────
        logger.info("SIGNAL FOUND:")
        logger.info(f"  {signal.summary()}")
        logger.info(f"  direction    : {signal.direction}")
        logger.info(f"  entry (mid)  : {signal.entry_price_mid:.2f}")
        logger.info(f"  stop_loss    : {signal.stop_loss_rate:.2f} ({signal.sl_distance:.2f} pts)")
        logger.info(f"  take_profit  : {signal.take_profit_rate:.2f} ({signal.tp_distance:.2f} pts)")
        logger.info(f"  R:R          : {signal.risk_reward_ratio:.1f}x")
        logger.info(f"  WBWS+ window : {'✅ OPEN' if signal.wbws_window_valid else '⚠️ CLOSED'}")

        # ── WBWS+ gate ────────────────────────────────────────────────────
        if not signal.wbws_window_valid:
            logger.warning(
                f"Signal outside WBWS+ window — not placing order. "
                f"Next poll in {POLL_INTERVAL}s …"
            )
            time.sleep(POLL_INTERVAL)
            continue

        # ── Pyramiding guard ──────────────────────────────────────────────
        safe = _check_pyramiding(
            client=client,
            resolver=resolver,
            symbol=bs_config.execution.symbol,
            max_positions=signal.max_positions,
        )
        if not safe:
            logger.info(f"Max positions reached — skipping. Next poll in {POLL_INTERVAL}s …")
            time.sleep(POLL_INTERVAL)
            continue

        # ── Place order ───────────────────────────────────────────────────
        logger.info("Placing order …")
        try:
            position_id = router.open_position(
                symbol=bs_config.execution.symbol,
                direction=signal.direction,
                amount=bs_config.execution.amount_usd,
                leverage=bs_config.execution.leverage,
                stop_loss_rate=signal.stop_loss_rate,
                take_profit_rate=signal.take_profit_rate,
            )
        except OutsideTradingHoursError as exc:
            logger.error(f"ORDER BLOCKED by trading hours guard: {exc}")
            sys.exit(1)
        except Exception as exc:
            logger.error(f"ORDER FAILED: {exc}")
            sys.exit(1)

        # ── Success — log and stop ────────────────────────────────────────
        logger.info("=" * 60)
        logger.info("ORDER PLACED — loop stopping.")
        logger.info(f"  positionID   : {position_id}")
        logger.info(f"  symbol       : {signal.symbol}")
        logger.info(f"  direction    : {signal.direction}")
        logger.info(f"  amount       : {bs_config.execution.amount_usd} USD")
        logger.info(f"  leverage     : {bs_config.execution.leverage}x")
        logger.info(f"  stop_loss    : {signal.stop_loss_rate:.2f}")
        logger.info(f"  take_profit  : {signal.take_profit_rate:.2f}")
        logger.info("=" * 60)
        logger.info(
            "Next: run inspect_portfolio.py to confirm, "
            "then run_tracker_loop.py to track close."
        )
        sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Interrupted by user — loop stopped cleanly.")
        sys.exit(0)