#!/usr/bin/env python
"""
run_signal_loop.py — persistent paper trading loop with full circuit breakers.

Runs continuously across the trading week. Places orders whenever a valid signal
is found, then keeps running (does NOT stop after one order). Stops only when a
circuit breaker fires, the kill switch is set, or Ctrl+C is pressed.

Safety circuit breakers (all configurable in broker_support_config.yaml):
  - Kill switch file (default: STOP in project root) — checked every poll
  - Max consecutive losses — hard_stop or pause_until_next_day
  - Daily drawdown % of session-open credit — hard stop
  - Minimum available cash — hard stop
  - Consecutive pipeline error streak — hard stop

Guards also enforced (unchanged from Phase 2):
  - WBWS+ trading window gate
  - Pyramiding / max_positions guard (from strategy YAML)
  - is_trading_hours() gate inside OrderRouter

Loop behaviour:
  - Polls every 60 seconds
  - While a position is open: _check_pyramiding returns False → loop idles
  - Daily state (loss streak, drawdown baseline) resets at UTC date rollover
  - All circuit breaker actions are logged with full context before exit/pause

Usage:
    # Normal operation — console output + log file
    python scripts/broker_support/run_signal_loop.py

    # Quiet mode — log file only, no console output (run in background terminal)
    python scripts/broker_support/run_signal_loop.py --quiet

    # Custom config
    python scripts/broker_support/run_signal_loop.py \\
        --config configs/broker_support/broker_support_config.yaml

    # Debug console output
    python scripts/broker_support/run_signal_loop.py --verbose

Kill switch:
    Create a file named STOP (or the configured kill_switch_file value) in the
    project root. The loop halts at the next poll cycle. Delete the file before
    restarting.
"""
import argparse
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from loguru import logger

# ---------------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.broker_support.client.client import EToroClient
from src.broker_support.config.broker_support_config import BrokerSupportConfig
from src.broker_support.enrichment.instrument_resolver import InstrumentResolver
from src.broker_support.execution.order_router import OrderRouter, OutsideTradingHoursError
from src.broker_support.live.live_data_fetcher import LiveDataFetcher
from src.broker_support.live.signal_bridge import SignalBridge
from src.broker_support.safeguards.paper_trading_guard import (
    HaltLoopError,
    PaperTradingGuard,
    PauseUntilTomorrowError,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_CONFIG  = "configs/broker_support/broker_support_config.yaml"
DEFAULT_LOG_DIR = "outputs/broker_support/logs"
DEFAULT_JOURNAL = "outputs/broker_support/journal/trades.csv"
POLL_INTERVAL   = 60   # seconds


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _configure_logging(log_dir: str, verbose: bool, quiet: bool) -> None:
    """
    Configure loguru sinks.

    quiet=True  → file sink only, no console output.
    verbose=True → console at DEBUG level (ignored when quiet=True).
    """
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    logger.remove()

    # File sink — always active, always DEBUG
    logger.add(
        str(log_path / "run_signal_loop_{time:YYYY-MM-DD}.log"),
        level="DEBUG",
        rotation="00:00",
        retention="30 days",
        encoding="utf-8",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{line} | {message}",
    )

    # Console sink — suppressed in quiet mode
    if not quiet:
        level = "DEBUG" if verbose else "INFO"
        logger.add(
            sys.stderr,
            level=level,
            colorize=True,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                   "<level>{level: <8}</level> | {message}",
        )


# ---------------------------------------------------------------------------
# Pyramiding guard (inline — no sys.exit, returns bool)
# ---------------------------------------------------------------------------

def _check_pyramiding(
    client: EToroClient,
    resolver: InstrumentResolver,
    symbol: str,
    max_positions: int,
) -> bool:
    """
    Return True if safe to place a new order (open positions < max_positions).
    Return False if already at limit.
    Raises RuntimeError on API failure (caller handles it as a pipeline error).
    """
    instrument_id = resolver.instrument_id(symbol)
    if instrument_id is None:
        raise RuntimeError(
            f"Pyramiding check: cannot resolve '{symbol}'. Verify instrument_map.yaml."
        )
    try:
        portfolio = client._make_request("GET", "api/v1/trading/info/demo/portfolio")
    except Exception as exc:
        raise RuntimeError(f"Pyramiding check: portfolio fetch failed: {exc}") from exc

    positions = portfolio.get("clientPortfolio", {}).get("positions", [])
    open_count = sum(1 for p in positions if p.get("instrumentID") == instrument_id)
    credit = portfolio.get("clientPortfolio", {}).get("credit", 0.0)

    logger.info(
        f"Pyramiding check: {open_count} open position(s) for {symbol} "
        f"(max_positions={max_positions}) | credit={credit:.2f}"
    )
    if open_count >= max_positions:
        logger.info(
            f"Max positions reached ({open_count}/{max_positions}) — "
            f"no new order. Continuing to poll."
        )
        return False, credit
    return True, credit


# ---------------------------------------------------------------------------
# Today's P&L from journal (for guard reconstruction on restart)
# ---------------------------------------------------------------------------

def _load_todays_pnl(journal_path: Path) -> list:
    """
    Load today's closed-trade P&L values from the journal CSV in chronological order.
    Returns empty list if journal absent or has no today entries.
    """
    if not journal_path.exists() or journal_path.stat().st_size == 0:
        return []
    try:
        import pandas as pd
        df = pd.read_csv(journal_path)
        today_str = datetime.now(timezone.utc).date().isoformat()
        if "close_time" not in df.columns or "profit_loss" not in df.columns:
            return []
        mask = df["close_time"].astype(str).str.startswith(today_str)
        return df.loc[mask, "profit_loss"].tolist()
    except Exception as exc:
        logger.warning(f"Could not load today's P&L from journal: {exc}")
        return []


# ---------------------------------------------------------------------------
# Off-hours sleep helper
# ---------------------------------------------------------------------------

def _seconds_until_next_allowed_hour(allowed_hours_utc: list) -> float:
    """
    Return seconds until the next hour that appears in allowed_hours_utc.

    Scans forward minute-by-minute from now (UTC) until a matching hour is
    found, up to 24 h ahead.  Always returns a positive value >= 0.

    Args:
        allowed_hours_utc: Sorted or unsorted list of UTC hours (0-23).

    Returns:
        Seconds until the start of the next allowed hour.
    """
    if not allowed_hours_utc:
        return 0.0
    allowed = set(allowed_hours_utc)
    now = datetime.now(timezone.utc)
    # Walk forward hour by hour (max 25 to guarantee termination)
    for hours_ahead in range(1, 26):
        candidate = (now + timedelta(hours=hours_ahead)).replace(
            minute=0, second=0, microsecond=0
        )
        if candidate.hour in allowed:
            return max(0.0, (candidate - now).total_seconds())
    # Fallback — should never reach here with a non-empty list
    return 3600.0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Persistent paper trading loop with full circuit breakers."
    )
    parser.add_argument("--config",  default=DEFAULT_CONFIG)
    parser.add_argument("--log-dir", default=DEFAULT_LOG_DIR)
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress all console output — log file only. "
             "Use when running in a background terminal.",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable DEBUG-level console output (ignored when --quiet is set).",
    )
    args = parser.parse_args()

    _configure_logging(args.log_dir, args.verbose, args.quiet)

    logger.info("=" * 60)
    logger.info("run_signal_loop.py — PERSISTENT loop (runs until circuit breaker or Ctrl+C)")
    logger.info(f"  config       : {args.config}")
    logger.info(f"  poll interval: {POLL_INTERVAL}s")
    logger.info(f"  quiet mode   : {args.quiet}")
    logger.info("  Ctrl+C to stop at any time | create STOP file to halt at next poll")
    logger.info("=" * 60)

    # ── Load config ───────────────────────────────────────────────────────
    config_path = Path(args.config)
    try:
        bs_config = BrokerSupportConfig.from_yaml(config_path)
    except Exception as exc:
        logger.error(f"Failed to load config: {exc}")
        sys.exit(1)

    logger.info(
        f"Config: symbol={bs_config.execution.symbol}, "
        f"amount={bs_config.execution.amount_usd} USD, "
        f"leverage={bs_config.execution.leverage}x | "
        f"Safety: max_losses={bs_config.safety.max_consecutive_losses} "
        f"({bs_config.safety.consecutive_loss_action}), "
        f"max_drawdown={bs_config.safety.max_daily_drawdown_pct:.1f}%, "
        f"min_cash={bs_config.safety.min_available_cash_usd:.2f}, "
        f"kill_switch='{bs_config.safety.kill_switch_file}'"
    )

    # ── Build infrastructure ──────────────────────────────────────────────
    try:
        client   = EToroClient()
        resolver = InstrumentResolver(map_path=bs_config.execution.instrument_map_path)
        fetcher  = LiveDataFetcher(client=client, resolver=resolver, config=bs_config.live_data)
        bridge   = SignalBridge(bs_config=bs_config, fetcher=fetcher)
        router   = OrderRouter(client=client, resolver=resolver)
    except Exception as exc:
        logger.error(f"Failed to initialise infrastructure: {exc}")
        sys.exit(1)

    # ── Fetch initial portfolio credit for guard baseline ─────────────────
    try:
        init_portfolio = client._make_request("GET", "api/v1/trading/info/demo/portfolio")
        session_open_credit = float(
            init_portfolio.get("clientPortfolio", {}).get("credit", 0.0)
        )
        logger.info(f"Session open credit: {session_open_credit:.2f} USD")
    except Exception as exc:
        logger.error(f"Failed to fetch initial portfolio: {exc}")
        sys.exit(1)

    # ── Initialise guard ─────────────────────────────────────────────────
    journal_path = Path(DEFAULT_JOURNAL)
    todays_pnl = _load_todays_pnl(journal_path)
    guard = PaperTradingGuard(
        config=bs_config,
        session_open_credit=session_open_credit,
        journal_trades_today=todays_pnl,
    )

    # ── Main loop ─────────────────────────────────────────────────────────
    iteration = 0
    orders_placed = 0

    while True:
        iteration += 1
        now_utc = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        logger.info(f"── Poll #{iteration} at {now_utc} ──")

        # ── Per-poll guard checks ─────────────────────────────────────────
        try:
            guard.check_kill_switch()
        except HaltLoopError as exc:
            logger.warning(f"HALT: {exc}")
            sys.exit(0)

        # ── Off-hours gate ────────────────────────────────────────────────
        # If the current UTC hour is not in allowed_hours_utc (or is in
        # skip_hours_utc), sleep until the next allowed hour opens rather
        # than burning API calls on polls that can never result in an order.
        current_hour = datetime.now(timezone.utc).hour
        allowed = bs_config.trading_window.allowed_hours_utc
        skipped = bs_config.trading_window.skip_hours_utc
        if current_hour not in allowed or current_hour in skipped:
            wait_s = _seconds_until_next_allowed_hour(
                [h for h in allowed if h not in skipped]
            )
            next_open = datetime.now(timezone.utc)
            next_open = (next_open + timedelta(seconds=wait_s)).replace(
                minute=0, second=0, microsecond=0
            )
            logger.info(
                f"Outside trading hours (UTC hour={current_hour}). "
                f"Sleeping until next session open: "
                f"{next_open.strftime('%Y-%m-%d %H:%M UTC')} "
                f"({wait_s / 3600:.1f}h) …"
            )
            _sleep_interruptible(wait_s, chunk=300, guard=guard)
            # ── Session open banner ───────────────────────────────────────
            logger.info("=" * 60)
            logger.info(
                f"Session open — resuming at UTC hour "
                f"{datetime.now(timezone.utc).hour:02d}:00."
            )
            logger.info("=" * 60)
            continue
        # Reuse the pyramiding portfolio fetch result to avoid a second API call.
        # _check_pyramiding returns (safe: bool, credit: float).
        try:
            safe_to_trade, current_credit = _check_pyramiding(
                client=client,
                resolver=resolver,
                symbol=bs_config.execution.symbol,
                max_positions=1,  # always 1 — strategy YAML max_positions
            )
        except RuntimeError as exc:
            logger.error(f"Portfolio fetch error: {exc}")
            try:
                guard.record_pipeline_error()
            except HaltLoopError as halt:
                logger.error(f"HALT: {halt}")
                sys.exit(0)
            time.sleep(POLL_INTERVAL)
            continue

        # ── Date rollover check ───────────────────────────────────────────
        if guard.check_date_rollover(current_credit):
            logger.info(
                f"New trading day. Session open credit reset to {current_credit:.2f}. "
                f"All daily counters cleared."
            )

        # ── Drawdown + cash guards ────────────────────────────────────────
        try:
            guard.check_daily_drawdown(current_credit)
            guard.check_min_cash(current_credit)
        except HaltLoopError as exc:
            logger.error(f"HALT: {exc}")
            sys.exit(0)

        # ── Position already open — idle ──────────────────────────────────
        if not safe_to_trade:
            logger.info(f"Position open — idling. Next poll in {POLL_INTERVAL}s …")
            time.sleep(POLL_INTERVAL)
            continue

        # ── Run signal pipeline ───────────────────────────────────────────
        logger.info("SignalBridge: fetching live candles …")
        try:
            signal = bridge.get_signal()
            guard.reset_pipeline_error_streak()
        except Exception as exc:
            logger.error(f"Pipeline error: {exc}. Retrying in {POLL_INTERVAL}s …")
            try:
                guard.record_pipeline_error()
            except HaltLoopError as halt:
                logger.error(f"HALT: {halt}")
                sys.exit(0)
            time.sleep(POLL_INTERVAL)
            continue

        # ── No signal ─────────────────────────────────────────────────────
        if signal is None:
            logger.info(f"No signal on last bar. Next poll in {POLL_INTERVAL}s …")
            time.sleep(POLL_INTERVAL)
            continue

        # ── Signal found — always log it ──────────────────────────────────
        logger.info("SIGNAL FOUND:")
        logger.info(f"  {signal.summary()}")
        logger.info(f"  direction    : {signal.direction}")
        logger.info(f"  entry (mid)  : {signal.entry_price_mid:.2f}")
        logger.info(f"  stop_loss    : {signal.stop_loss_rate:.2f} ({signal.sl_distance:.2f} pts)")
        logger.info(f"  take_profit  : {signal.take_profit_rate:.2f} ({signal.tp_distance:.2f} pts)")
        logger.info(f"  R:R          : {signal.risk_reward_ratio:.1f}x")
        logger.info(f"  WBWS+ window : {'✅ OPEN' if signal.wbws_window_valid else '⚠️  CLOSED'}")
        logger.info(f"  {guard.status_summary()}")

        # ── WBWS+ gate ────────────────────────────────────────────────────
        if not signal.wbws_window_valid:
            logger.warning(
                f"Signal outside WBWS+ window — not placing order. "
                f"Next poll in {POLL_INTERVAL}s …"
            )
            time.sleep(POLL_INTERVAL)
            continue

        # ── Consecutive loss circuit breaker ──────────────────────────────
        try:
            guard.check_consecutive_losses()
        except HaltLoopError as exc:
            logger.error(f"HALT: {exc}")
            sys.exit(0)
        except PauseUntilTomorrowError as exc:
            resume_at = exc.resume_at
            now = datetime.now(timezone.utc)
            wait_seconds = max(0.0, (resume_at - now).total_seconds())
            logger.warning(
                f"PAUSE: {exc} "
                f"Sleeping {wait_seconds / 3600:.1f}h until "
                f"{resume_at.strftime('%Y-%m-%d %H:%M UTC')} …"
            )
            # Sleep in chunks so kill switch is still checked periodically
            _sleep_interruptible(wait_seconds, chunk=300, guard=guard)
            # After pause: reset daily state with fresh credit
            try:
                wake_portfolio = client._make_request(
                    "GET", "api/v1/trading/info/demo/portfolio"
                )
                wake_credit = float(
                    wake_portfolio.get("clientPortfolio", {}).get("credit", 0.0)
                )
            except Exception:
                wake_credit = current_credit  # best effort
            guard.reset_daily_state(wake_credit)
            logger.info(
                f"Resuming after pause. New session_open_credit={wake_credit:.2f}"
            )
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
            time.sleep(POLL_INTERVAL)
            continue
        except Exception as exc:
            logger.error(f"ORDER FAILED: {exc}")
            time.sleep(POLL_INTERVAL)
            continue

        orders_placed += 1
        logger.info("=" * 60)
        logger.info(f"ORDER PLACED #{orders_placed}")
        logger.info(f"  positionID   : {position_id}")
        logger.info(f"  symbol       : {signal.symbol}")
        logger.info(f"  direction    : {signal.direction}")
        logger.info(f"  amount       : {bs_config.execution.amount_usd} USD")
        logger.info(f"  leverage     : {bs_config.execution.leverage}x")
        logger.info(f"  stop_loss    : {signal.stop_loss_rate:.2f}")
        logger.info(f"  take_profit  : {signal.take_profit_rate:.2f}")
        logger.info(f"  session total: {orders_placed} order(s) placed")
        logger.info("=" * 60)

        # Loop continues — tracker loop (run separately) handles close detection
        # and calls guard.record_trade_result() is NOT called here because the
        # trade is still open. The tracker loop writes to the journal; the loop
        # reconstructs consecutive_losses from the journal on the next day reset.
        # NOTE: see tracker integration note in docs/ctp/BROKER_INTEGRATION.md.

        time.sleep(POLL_INTERVAL)


# ---------------------------------------------------------------------------
# Interruptible sleep — checks kill switch every `chunk` seconds
# ---------------------------------------------------------------------------

def _sleep_interruptible(
    total_seconds: float,
    chunk: int,
    guard: PaperTradingGuard,
) -> None:
    """
    Sleep for total_seconds, waking every `chunk` seconds to check kill switch.
    Exits loop cleanly if kill switch is detected during a pause.
    """
    remaining = total_seconds
    while remaining > 0:
        sleep_for = min(chunk, remaining)
        time.sleep(sleep_for)
        remaining -= sleep_for
        try:
            guard.check_kill_switch()
        except HaltLoopError as exc:
            logger.warning(f"HALT during pause: {exc}")
            sys.exit(0)
        if remaining > 0:
            logger.info(
                f"Still paused — {remaining / 3600:.1f}h remaining until session open."
            )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Interrupted by user (Ctrl+C) — loop stopped cleanly.")
        sys.exit(0)