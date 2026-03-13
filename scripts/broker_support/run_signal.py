#!/usr/bin/env python
"""
run_signal.py — Phase 2 signal runner for paper trading.

Stage 1 (default — no flags):
  Runs the full strategy pipeline on live data.
  Prints signal details and simulated order params.
  Does NOT place any orders.
  Use this to validate the pipeline end-to-end before committing real orders.

Stage 2 (--place-order):
  Runs Stage 1 first, then — if a signal is found — places ONE demo order
  via OrderRouter. Prints the positionID on success.
  Supervised: manually review output before running again.
  WBWS+ gate applies: if signal is outside allowed hours, aborts (use --force-window to override).

Usage:
    # Stage 1 — dry-run signal validation
    python scripts/broker_support/run_signal.py

    # Stage 2 — place one order (supervised)
    python scripts/broker_support/run_signal.py --place-order

    # Stage 2 — place order even outside WBWS+ window (testing only)
    python scripts/broker_support/run_signal.py --place-order --force-window

    # Use a non-default config
    python scripts/broker_support/run_signal.py --config configs/broker_support/broker_support_config.yaml

    # Verbose debug output
    python scripts/broker_support/run_signal.py --verbose
"""
import argparse
import sys
from pathlib import Path

from loguru import logger

# ---------------------------------------------------------------------------
# Path bootstrap — ensure project root is on sys.path when run as a script
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.broker_support.client.client import EToroClient
from src.broker_support.config.broker_support_config import BrokerSupportConfig
from src.broker_support.enrichment.instrument_resolver import InstrumentResolver
from src.broker_support.execution.order_router import (
    OrderRouter,
    OutsideTradingHoursError,
)
from src.broker_support.live.live_data_fetcher import LiveDataFetcher
from src.broker_support.live.signal_bridge import SignalBridge

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_CONFIG = "configs/broker_support/broker_support_config.yaml"
DEFAULT_LOG_DIR = "outputs/broker_support/logs"


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

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
        str(log_path / "run_signal_{time:YYYY-MM-DD}.log"),
        level="DEBUG",
        rotation="00:00",
        retention="30 days",
        encoding="utf-8",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{line} | {message}",
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 2 signal runner — Stage 1 (dry-run) or Stage 2 (--place-order)."
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG,
        help=f"Path to broker_support_config.yaml (default: {DEFAULT_CONFIG})",
    )
    parser.add_argument(
        "--place-order",
        action="store_true",
        help="Stage 2: place a real demo order if signal found.",
    )
    parser.add_argument(
        "--force-window",
        action="store_true",
        help="Place order even if outside WBWS+ trading window. Use for testing only.",
    )
    parser.add_argument(
        "--log-dir",
        default=DEFAULT_LOG_DIR,
        help=f"Log directory (default: {DEFAULT_LOG_DIR})",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable DEBUG-level console output.",
    )
    args = parser.parse_args()

    _configure_logging(args.log_dir, args.verbose)

    stage = "Stage 2 (place-order)" if args.place_order else "Stage 1 (dry-run)"
    logger.info("=" * 60)
    logger.info(f"run_signal.py — {stage}")
    logger.info(f"  config       : {args.config}")
    logger.info(f"  force-window : {args.force_window}")
    logger.info("=" * 60)

    # ── Load broker_support config ────────────────────────────────────────
    config_path = Path(args.config)
    try:
        bs_config = BrokerSupportConfig.from_yaml(config_path)
    except Exception as exc:
        logger.error(f"Failed to load broker_support config: {exc}")
        sys.exit(1)

    logger.info(
        f"Config loaded: symbol={bs_config.execution.symbol}, "
        f"amount={bs_config.execution.amount_usd} USD, "
        f"leverage={bs_config.execution.leverage}x, "
        f"wbws_enabled={bs_config.trading_window.enabled}"
    )

    # ── Instantiate infrastructure ────────────────────────────────────────
    try:
        client = EToroClient()
        resolver = InstrumentResolver(
            map_path=bs_config.execution.instrument_map_path
        )
        fetcher = LiveDataFetcher(
            client=client,
            resolver=resolver,
            config=bs_config.live_data,
        )
        bridge = SignalBridge(
            bs_config=bs_config,
            fetcher=fetcher,
        )
    except Exception as exc:
        logger.error(f"Failed to initialise infrastructure: {exc}")
        sys.exit(1)

    # ── Run pipeline — get signal ─────────────────────────────────────────
    logger.info("Running pipeline …")
    try:
        signal = bridge.get_signal()
    except Exception as exc:
        logger.error(f"Pipeline failed: {exc}")
        sys.exit(1)

    # ── Stage 1 output ────────────────────────────────────────────────────
    if signal is None:
        logger.info("─" * 60)
        logger.info("RESULT: No signal on latest bar. No action.")
        logger.info("─" * 60)
        sys.exit(0)

    # Signal found — print full summary regardless of stage
    logger.info("─" * 60)
    logger.info("SIGNAL FOUND:")
    logger.info(f"  {signal.summary()}")
    logger.info(f"  candidate_id : {signal.candidate_id}")
    logger.info(f"  direction    : {signal.direction}")
    logger.info(f"  entry (mid)  : {signal.entry_price_mid:.2f}")
    logger.info(f"  stop_loss    : {signal.stop_loss_rate:.2f} ({signal.sl_distance:.2f} pts)")
    logger.info(f"  take_profit  : {signal.take_profit_rate:.2f} ({signal.tp_distance:.2f} pts)")
    logger.info(f"  R:R ratio    : {signal.risk_reward_ratio:.1f}x")
    logger.info(f"  ATR          : {signal.atr_value:.2f}")
    logger.info(f"  WBWS+ window : {'✅ OPEN' if signal.wbws_window_valid else '⚠️ CLOSED'}")
    logger.info(f"  max_positions: {signal.max_positions} (pyramiding=False)")
    if signal.meta:
        logger.info(f"  meta         : {signal.meta}")
    logger.info("─" * 60)

    if not args.place_order:
        logger.info("Stage 1 complete — dry-run only. Use --place-order to execute.")
        sys.exit(0)

    # ── Stage 2 — place order ─────────────────────────────────────────────
    logger.info("Stage 2: attempting to place order …")

    # WBWS+ gate — abort unless --force-window
    if not signal.wbws_window_valid and not args.force_window:
        logger.warning(
            "ORDER BLOCKED: signal is outside WBWS+ trading window. "
            "Use --force-window to override (testing only). "
            "Exiting without placing order."
        )
        sys.exit(0)

    if args.force_window and not signal.wbws_window_valid:
        logger.warning("--force-window: bypassing WBWS+ gate. Testing use only.")

    # ── Pyramiding / max_positions guard ─────────────────────────────────
    # strategy YAML: position_control.max_positions=1, pyramiding_enabled=false.
    # TradeSimulator enforces this in backtesting. In live context it is not
    # run, so we enforce it here explicitly before touching OrderRouter.
    # max_positions comes from the strategy YAML via signal.max_positions —
    # the backtested constraint, stricter than broker_support_config safety section.
    _check_pyramiding(
        client=client,
        resolver=resolver,
        symbol=bs_config.execution.symbol,
        max_positions=signal.max_positions,
    )

    # Instantiate OrderRouter
    router = OrderRouter(client=client, resolver=resolver)

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

    logger.info("=" * 60)
    logger.info("ORDER PLACED SUCCESSFULLY")
    logger.info(f"  positionID   : {position_id}")
    logger.info(f"  symbol       : {signal.symbol}")
    logger.info(f"  direction    : {signal.direction}")
    logger.info(f"  amount       : {bs_config.execution.amount_usd} USD")
    logger.info(f"  leverage     : {bs_config.execution.leverage}x")
    logger.info(f"  stop_loss    : {signal.stop_loss_rate:.2f}")
    logger.info(f"  take_profit  : {signal.take_profit_rate:.2f}")
    logger.info("=" * 60)
    logger.info(
        "Next steps: run inspect_portfolio.py to confirm position, "
        "then let tracker loop detect close."
    )



# ---------------------------------------------------------------------------
# Pre-trade guards
# ---------------------------------------------------------------------------

def _check_pyramiding(
    client,
    resolver,
    symbol: str,
    max_positions: int,
) -> None:
    """
    Enforce strategy YAML position_control.max_positions and pyramiding_enabled=False.

    Fetches current open demo positions, counts those matching the instrument.
    If count >= max_positions, logs clearly and exits 0 (not an error — just no trade).

    This is the live equivalent of TradeSimulator's pyramiding guard.
    Called only in Stage 2 (--place-order), never in Stage 1 dry-run.

    Args:
        client:        EToroClient instance.
        resolver:      InstrumentResolver instance.
        symbol:        Instrument key (e.g. 'DAX').
        max_positions: From strategy YAML position_control.max_positions.

    Raises:
        SystemExit(0): if max_positions already reached — not an error, just skip.
        SystemExit(1): if portfolio fetch fails.
    """
    instrument_id = resolver.instrument_id(symbol)
    if instrument_id is None:
        logger.error(
            f"Pyramiding check: cannot resolve instrumentId for '{symbol}'. "
            f"Verify instrument_map.yaml."
        )
        sys.exit(1)

    try:
        portfolio = client._make_request(
            "GET", "api/v1/trading/info/demo/portfolio"
        )
    except Exception as exc:
        logger.error(f"Pyramiding check: portfolio fetch failed: {exc}")
        sys.exit(1)

    positions = portfolio.get("clientPortfolio", {}).get("positions", [])
    open_for_instrument = [
        p for p in positions if p.get("instrumentID") == instrument_id
    ]
    count = len(open_for_instrument)

    logger.info(
        f"Pyramiding check: {count} open position(s) found for {symbol} "
        f"(instrumentID={instrument_id}), max_positions={max_positions}"
    )

    if count >= max_positions:
        logger.info(
            f"ORDER SKIPPED: {count}/{max_positions} position(s) already open for {symbol}. "
            f"pyramiding_enabled=False — no new position opened. "
            f"Wait for existing position to close before next signal."
        )
        sys.exit(0)


if __name__ == "__main__":
    main()
