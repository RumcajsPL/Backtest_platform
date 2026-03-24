#!/usr/bin/env python
"""
run_signal_loop.py — persistent paper trading loop with full circuit breakers.

Runs continuously across the trading week. Places orders whenever a valid signal
is found, then keeps running (does NOT stop after one order). Stops only when a
circuit breaker fires, the kill switch is set, or Ctrl+C is pressed.

Safety circuit breakers (all configurable in broker_support_config.yaml):
  - Kill switch file — checked every poll (per-instance AND master STOP)
  - Max consecutive losses — hard_stop or pause_until_next_day
  - Daily CTP drawdown % of session-open credit — hard stop (journal-scoped)
  - Minimum available cash — hard stop
  - Consecutive pipeline error streak — hard stop

Instance isolation (parallel loop support):
  Each loop instance is identified by --instance <id> (e.g. c424, 240166).
  All file paths are derived from the instance ID:

    Journal dir   : outputs/broker_support/journal/<id>/
    trades.csv    : outputs/broker_support/journal/<id>/trades.csv
    open_positions: outputs/broker_support/journal/<id>/open_positions.json
    Log file      : outputs/broker_support/logs/run_signal_loop_<id>_YYYY-MM-DD.log
    Kill switch   : STOP_<id>  (per-instance) + STOP (master — halts all loops)

  When --instance is omitted, paths fall back to the legacy single-loop layout
  (outputs/broker_support/journal/trades.csv etc.) for backward compatibility
  with the original c424 loop if desired — but using --instance c424 is
  strongly recommended for consistency.

Isolation from external account activity:
  - Pyramiding check filters the live portfolio to CTP-placed positionIDs only.
    Manual trades or other strategies on the same demo account are invisible.
  - Daily drawdown uses CTP's own realised P&L from trades.csv only.
    External losses on the account do NOT trigger CTP's circuit breakers.
  - CTP-placed positionIDs are tracked in open_positions.json (instance dir).
    On restart the loop seeds from this file so pyramiding survives restarts.

Guards also enforced (unchanged from Phase 2):
  - WBWS+ trading window gate
  - Pyramiding / max_positions guard (from strategy YAML)
  - is_trading_hours() gate inside OrderRouter

Loop behaviour:
  - Polls every 60 seconds
  - While a CTP position is open: _check_pyramiding returns False → loop idles
  - Daily state (loss streak, drawdown baseline) resets at UTC date rollover
  - All circuit breaker actions are logged with full context before exit/pause

Usage:
    # Named instance (recommended — full isolation)
    python scripts/broker_support/run_signal_loop.py --instance c424
    python scripts/broker_support/run_signal_loop.py --instance 240166
    python scripts/broker_support/run_signal_loop.py --instance 7ffbc5
    python scripts/broker_support/run_signal_loop.py --instance 61875

    # Custom config (instance ID still required for path isolation)
    python scripts/broker_support/run_signal_loop.py \\
        --instance c424 \\
        --config configs/broker_support/broker_support_config.yaml

    # Quiet mode — log file only, no console output (background terminal)
    python scripts/broker_support/run_signal_loop.py --instance 240166 --quiet

    # Debug console output
    python scripts/broker_support/run_signal_loop.py --instance 240166 --verbose

Kill switch:
    Per-instance: create STOP_<id> in project root (e.g. STOP_240166).
                  Halts only that instance at the next poll cycle.
    Master:       create STOP in project root. Halts ALL running instances.
    Delete the file(s) before restarting.
"""
import argparse
import json
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
DEFAULT_CONFIG   = "configs/broker_support/broker_support_config.yaml"
DEFAULT_LOG_DIR  = "outputs/broker_support/logs"
JOURNAL_BASE_DIR = "outputs/broker_support/journal"
POLL_INTERVAL    = 60   # seconds
MASTER_KILL_FILE = "STOP"


# ---------------------------------------------------------------------------
# Instance path resolution
# ---------------------------------------------------------------------------

def _resolve_paths(instance_id: str | None) -> tuple[Path, Path]:
    """
    Resolve journal and open_positions paths from instance ID.

    With instance_id:
        journal   → outputs/broker_support/journal/<id>/trades.csv
        positions → outputs/broker_support/journal/<id>/open_positions.json

    Without instance_id (legacy fallback):
        journal   → outputs/broker_support/journal/trades.csv
        positions → outputs/broker_support/journal/open_positions.json

    Returns:
        (journal_path, open_positions_path)
    """
    base = _PROJECT_ROOT / JOURNAL_BASE_DIR
    if instance_id:
        instance_dir = base / instance_id
        return (
            instance_dir / "trades.csv",
            instance_dir / "open_positions.json",
        )
    return (
        base / "trades.csv",
        base / "open_positions.json",
    )


def _default_config(instance_id: str | None) -> str:
    """
    Return default config path for a given instance ID.

    Known mappings:
        c424    → broker_support_config.yaml          (existing primary)
        240166  → broker_support_config_240166.yaml
        7ffbc5  → broker_support_config_7ffbc5.yaml
        61875   → broker_support_config_61875.yaml

    Falls back to DEFAULT_CONFIG for unknown or missing instance IDs so that
    the legacy single-loop invocation without --instance still works.
    """
    _CONFIG_MAP = {
        "c424":   "configs/broker_support/broker_support_config.yaml",
        "240166": "configs/broker_support/broker_support_config_240166.yaml",
        "7ffbc5": "configs/broker_support/broker_support_config_7ffbc5.yaml",
        "61875":  "configs/broker_support/broker_support_config_61875.yaml",
    }
    if instance_id and instance_id in _CONFIG_MAP:
        return _CONFIG_MAP[instance_id]
    return DEFAULT_CONFIG


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _configure_logging(
    log_dir: str,
    instance_id: str | None,
    verbose: bool,
    quiet: bool,
) -> None:
    """
    Configure loguru sinks.

    Log filename includes instance ID when provided:
        run_signal_loop_<id>_{time:YYYY-MM-DD}.log
        run_signal_loop_{time:YYYY-MM-DD}.log   (no instance)

    quiet=True  → file sink only, no console output.
    verbose=True → console at DEBUG level (ignored when quiet=True).
    """
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    logger.remove()

    suffix = f"_{instance_id}" if instance_id else ""
    log_filename = f"run_signal_loop{suffix}_{{time:YYYY-MM-DD}}.log"

    logger.add(
        str(log_path / log_filename),
        level="DEBUG",
        rotation="00:00",
        retention="30 days",
        encoding="utf-8",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{line} | {message}",
    )

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
# CTP open position tracking (isolation from external account activity)
# ---------------------------------------------------------------------------

def _load_ctp_open_position_ids(open_positions_path: Path) -> set[int]:
    """
    Load CTP-placed positionIDs from open_positions.json.

    Returns empty set if file absent or unreadable (safe — pyramiding check
    will then allow a trade if no CTP position is in the live portfolio under
    the CTP IDs; worst case: one extra poll before tracker loop reconciles).
    """
    if not open_positions_path.exists():
        return set()
    try:
        data = json.loads(open_positions_path.read_text(encoding="utf-8"))
        ids = {int(pid) for pid in data.get("position_ids", [])}
        if ids:
            logger.info(
                f"Seeded {len(ids)} CTP open positionID(s) from "
                f"{open_positions_path}: {ids}"
            )
        return ids
    except Exception as exc:
        logger.warning(
            f"Could not read {open_positions_path}: {exc}. "
            f"Starting with empty CTP position set."
        )
        return set()


def _persist_ctp_open_position_ids(
    open_positions_path: Path,
    position_ids: set[int],
) -> None:
    """
    Persist the current set of CTP open positionIDs to disk.

    Called after every add or remove so the set survives a loop restart.
    Writes atomically (build string then write_text).
    """
    try:
        open_positions_path.parent.mkdir(parents=True, exist_ok=True)
        data = {"position_ids": sorted(position_ids)}
        open_positions_path.write_text(
            json.dumps(data, indent=2), encoding="utf-8"
        )
    except Exception as exc:
        logger.warning(
            f"Could not persist CTP open positions to {open_positions_path}: {exc}. "
            f"In-memory set is still correct for this session."
        )


# ---------------------------------------------------------------------------
# Pyramiding guard — CTP-scoped
# ---------------------------------------------------------------------------

def _check_pyramiding(
    client: EToroClient,
    resolver: InstrumentResolver,
    symbol: str,
    max_positions: int,
    ctp_open_position_ids: set[int],
) -> tuple[bool, float]:
    """
    Return (safe_to_trade, current_credit).

    safe_to_trade is True only if the number of LIVE portfolio positions whose
    positionID is in ctp_open_position_ids is strictly less than max_positions.

    External positions on the same account (manual trades, other strategies)
    are invisible — only CTP-placed IDs are counted.

    Args:
        client:               EToroClient instance.
        resolver:             InstrumentResolver for symbol → instrument_id.
        symbol:               Trading symbol (e.g. 'GER40').
        max_positions:        Maximum allowed concurrent CTP positions.
        ctp_open_position_ids: Set of positionIDs placed by this CTP loop.

    Returns:
        (safe_to_trade: bool, current_credit: float)

    Raises:
        RuntimeError: on instrument resolution failure or API error.
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
    credit = float(portfolio.get("clientPortfolio", {}).get("credit", 0.0))

    ctp_open_count = sum(
        1 for p in positions
        if int(p.get("positionID", -1)) in ctp_open_position_ids
    )

    total_instrument_count = sum(
        1 for p in positions if p.get("instrumentID") == instrument_id
    )
    external_count = total_instrument_count - ctp_open_count

    logger.info(
        f"Pyramiding check: {ctp_open_count} CTP position(s) for {symbol} "
        f"(max_positions={max_positions}) | "
        f"external positions on same instrument: {external_count} (ignored) | "
        f"credit={credit:.2f}"
    )

    if ctp_open_count >= max_positions:
        logger.info(
            f"CTP max positions reached ({ctp_open_count}/{max_positions}) — "
            f"no new order. Continuing to poll."
        )
        return False, credit
    return True, credit


# ---------------------------------------------------------------------------
# Journal helpers
# ---------------------------------------------------------------------------

def _load_todays_ctp_pnl(journal_path: Path) -> tuple[list[float], float]:
    """
    Load today's closed CTP trade data from trades.csv.

    Returns:
        (pnl_list, pnl_sum) where:
          pnl_list: P&L values in chronological order (for consecutive loss
                    reconstruction via _count_tail_losses).
          pnl_sum:  Sum of all today's P&L values (for drawdown check).

    Returns ([], 0.0) if journal absent, empty, or unreadable.
    """
    if not journal_path.exists() or journal_path.stat().st_size == 0:
        return [], 0.0
    try:
        import pandas as pd
        df = pd.read_csv(journal_path)
        today_str = datetime.now(timezone.utc).date().isoformat()
        if "close_time" not in df.columns or "profit_loss" not in df.columns:
            return [], 0.0
        mask = df["close_time"].astype(str).str.startswith(today_str)
        pnl_list = df.loc[mask, "profit_loss"].tolist()
        pnl_sum = float(sum(pnl_list))
        return pnl_list, pnl_sum
    except Exception as exc:
        logger.warning(f"Could not load today's CTP P&L from journal: {exc}")
        return [], 0.0


# ---------------------------------------------------------------------------
# Off-hours sleep helper
# ---------------------------------------------------------------------------

def _seconds_until_next_allowed_hour(allowed_hours_utc: list) -> float:
    """
    Return seconds until the next hour that appears in allowed_hours_utc.

    Scans forward hour-by-hour from now (UTC) until a matching hour is found,
    up to 25 h ahead. Always returns a positive value >= 0.
    """
    if not allowed_hours_utc:
        return 0.0
    allowed = set(allowed_hours_utc)
    now = datetime.now(timezone.utc)
    for hours_ahead in range(1, 26):
        candidate = (now + timedelta(hours=hours_ahead)).replace(
            minute=0, second=0, microsecond=0
        )
        if candidate.hour in allowed:
            return max(0.0, (candidate - now).total_seconds())
    return 3600.0


# ---------------------------------------------------------------------------
# Kill switch — checks both master and per-instance files
# ---------------------------------------------------------------------------

def _check_kill_switches(instance_id: str | None, guard: PaperTradingGuard) -> None:
    """
    Check both master STOP file and per-instance kill switch.

    Master STOP halts all loops regardless of instance.
    Per-instance kill switch (from guard config) halts only this instance.

    Raises:
        HaltLoopError: if either kill switch file is detected.
    """
    master = _PROJECT_ROOT / MASTER_KILL_FILE
    if master.exists():
        raise HaltLoopError(
            f"Master kill switch '{MASTER_KILL_FILE}' detected. "
            f"Loop [{instance_id or 'default'}] halted. "
            f"Remove the file before restarting any loop."
        )
    # Per-instance check delegated to guard (reads kill_switch_file from config)
    guard.check_kill_switch()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Persistent paper trading loop with full circuit breakers."
    )
    parser.add_argument(
        "--instance", "-i",
        default=None,
        help=(
            "Instance ID for parallel loop isolation (e.g. c424, 240166, 7ffbc5, 61875). "
            "Determines journal dir, log filename, and default config path. "
            "Omit only for legacy single-loop operation."
        ),
    )
    parser.add_argument(
        "--config",
        default=None,
        help=(
            "Path to broker_support_config YAML. "
            "Defaults to the standard config for the given --instance, "
            "or configs/broker_support/broker_support_config.yaml if --instance is omitted."
        ),
    )
    parser.add_argument("--log-dir", default=DEFAULT_LOG_DIR)
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress all console output — log file only.",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable DEBUG-level console output (ignored when --quiet is set).",
    )
    args = parser.parse_args()

    instance_id: str | None = args.instance
    config_path_str: str = args.config or _default_config(instance_id)

    _configure_logging(args.log_dir, instance_id, args.verbose, args.quiet)

    instance_label = instance_id or "default"
    logger.info("=" * 60)
    logger.info(f"run_signal_loop.py — instance [{instance_label}]")
    logger.info("PERSISTENT loop (runs until circuit breaker or Ctrl+C)")
    logger.info(f"  config       : {config_path_str}")
    logger.info(f"  poll interval: {POLL_INTERVAL}s")
    logger.info(f"  quiet mode   : {args.quiet}")
    logger.info(
        f"  kill switches: '{MASTER_KILL_FILE}' (master — all loops) | "
        f"per-instance configured in YAML"
    )
    logger.info("  Ctrl+C to stop at any time")
    logger.info("=" * 60)

    # ── Load config ───────────────────────────────────────────────────────
    config_path = Path(config_path_str)
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
        f"max_drawdown={bs_config.safety.max_daily_drawdown_pct:.1f}% (CTP journal-scoped), "
        f"min_cash={bs_config.safety.min_available_cash_usd:.2f}, "
        f"kill_switch='{bs_config.safety.kill_switch_file}'"
    )

    # ── Resolve instance-scoped paths ─────────────────────────────────────
    journal_path, open_positions_path = _resolve_paths(instance_id)
    logger.info(
        f"Instance paths: "
        f"journal={journal_path}, "
        f"open_positions={open_positions_path}"
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

    # ── Seed CTP open position tracking ──────────────────────────────────
    ctp_open_position_ids: set[int] = _load_ctp_open_position_ids(open_positions_path)

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

    # ── Initialise guard ──────────────────────────────────────────────────
    todays_pnl_list, _ = _load_todays_ctp_pnl(journal_path)
    guard = PaperTradingGuard(
        config=bs_config,
        session_open_credit=session_open_credit,
        journal_trades_today=todays_pnl_list,
    )

    # ── Main loop ─────────────────────────────────────────────────────────
    iteration = 0
    orders_placed = 0

    while True:
        iteration += 1
        now_utc = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        logger.info(f"── [{instance_label}] Poll #{iteration} at {now_utc} ──")

        # ── Per-poll kill switch checks (master + per-instance) ───────────
        try:
            _check_kill_switches(instance_id, guard)
        except HaltLoopError as exc:
            logger.warning(f"HALT [{instance_label}]: {exc}")
            sys.exit(0)

        # ── Off-hours gate ────────────────────────────────────────────────
        current_hour = datetime.now(timezone.utc).hour
        allowed = bs_config.trading_window.allowed_hours_utc
        skipped = bs_config.trading_window.skip_hours_utc
        if current_hour not in allowed or current_hour in skipped:
            wait_s = _seconds_until_next_allowed_hour(
                [h for h in allowed if h not in skipped]
            )
            next_open = (
                datetime.now(timezone.utc) + timedelta(seconds=wait_s)
            ).replace(minute=0, second=0, microsecond=0)
            logger.info(
                f"[{instance_label}] Outside trading hours (UTC hour={current_hour}). "
                f"Sleeping until next session open: "
                f"{next_open.strftime('%Y-%m-%d %H:%M UTC')} "
                f"({wait_s / 3600:.1f}h) …"
            )
            _sleep_interruptible(wait_s, chunk=300, guard=guard, instance_id=instance_id)
            logger.info("=" * 60)
            logger.info(
                f"[{instance_label}] Session open — resuming at UTC hour "
                f"{datetime.now(timezone.utc).hour:02d}:00."
            )
            logger.info("=" * 60)
            continue

        # ── Portfolio fetch (pyramiding — CTP-scoped) ─────────────────────
        try:
            safe_to_trade, current_credit = _check_pyramiding(
                client=client,
                resolver=resolver,
                symbol=bs_config.execution.symbol,
                max_positions=1,
                ctp_open_position_ids=ctp_open_position_ids,
            )
        except RuntimeError as exc:
            logger.error(f"[{instance_label}] Portfolio fetch error: {exc}")
            try:
                guard.record_pipeline_error()
            except HaltLoopError as halt:
                logger.error(f"HALT [{instance_label}]: {halt}")
                sys.exit(0)
            time.sleep(POLL_INTERVAL)
            continue

        # ── Date rollover check ───────────────────────────────────────────
        if guard.check_date_rollover(current_credit):
            logger.info(
                f"[{instance_label}] New trading day. "
                f"Session open credit reset to {current_credit:.2f}. "
                f"All daily counters cleared."
            )

        # ── CTP drawdown + cash guards ────────────────────────────────────
        _, ctp_pnl_today = _load_todays_ctp_pnl(journal_path)
        try:
            guard.check_daily_drawdown(ctp_pnl_today)
            guard.check_min_cash(current_credit)
        except HaltLoopError as exc:
            logger.error(f"HALT [{instance_label}]: {exc}")
            sys.exit(0)

        # ── Position already open — idle ──────────────────────────────────
        if not safe_to_trade:
            logger.info(
                f"[{instance_label}] CTP position open — idling. "
                f"Next poll in {POLL_INTERVAL}s …"
            )
            time.sleep(POLL_INTERVAL)
            continue

        # ── Run signal pipeline ───────────────────────────────────────────
        logger.info(f"[{instance_label}] SignalBridge: fetching live candles …")
        try:
            signal = bridge.get_signal()
            guard.reset_pipeline_error_streak()
        except Exception as exc:
            logger.error(
                f"[{instance_label}] Pipeline error: {exc}. "
                f"Retrying in {POLL_INTERVAL}s …"
            )
            try:
                guard.record_pipeline_error()
            except HaltLoopError as halt:
                logger.error(f"HALT [{instance_label}]: {halt}")
                sys.exit(0)
            time.sleep(POLL_INTERVAL)
            continue

        # ── No signal ─────────────────────────────────────────────────────
        if signal is None:
            logger.info(
                f"[{instance_label}] No actionable signal this poll. "
                f"Next poll in {POLL_INTERVAL}s …"
            )
            time.sleep(POLL_INTERVAL)
            continue

        # ── Signal found ──────────────────────────────────────────────────
        logger.info(f"[{instance_label}] SIGNAL FOUND:")
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
                f"[{instance_label}] Signal outside WBWS+ window — not placing order. "
                f"Next poll in {POLL_INTERVAL}s …"
            )
            time.sleep(POLL_INTERVAL)
            continue

        # ── Consecutive loss circuit breaker ──────────────────────────────
        try:
            guard.check_consecutive_losses()
        except HaltLoopError as exc:
            logger.error(f"HALT [{instance_label}]: {exc}")
            sys.exit(0)
        except PauseUntilTomorrowError as exc:
            resume_at = exc.resume_at
            now = datetime.now(timezone.utc)
            wait_seconds = max(0.0, (resume_at - now).total_seconds())
            logger.warning(
                f"[{instance_label}] PAUSE: {exc} "
                f"Sleeping {wait_seconds / 3600:.1f}h until "
                f"{resume_at.strftime('%Y-%m-%d %H:%M UTC')} …"
            )
            _sleep_interruptible(wait_seconds, chunk=300, guard=guard, instance_id=instance_id)
            try:
                wake_portfolio = client._make_request(
                    "GET", "api/v1/trading/info/demo/portfolio"
                )
                wake_credit = float(
                    wake_portfolio.get("clientPortfolio", {}).get("credit", 0.0)
                )
            except Exception:
                wake_credit = current_credit
            guard.reset_daily_state(wake_credit)
            logger.info(
                f"[{instance_label}] Resuming after pause. "
                f"New session_open_credit={wake_credit:.2f}"
            )
            continue

        # ── Place order ───────────────────────────────────────────────────
        logger.info(f"[{instance_label}] Placing order …")
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
            logger.error(
                f"[{instance_label}] ORDER BLOCKED by trading hours guard: {exc}"
            )
            time.sleep(POLL_INTERVAL)
            continue
        except Exception as exc:
            logger.error(f"[{instance_label}] ORDER FAILED: {exc}")
            time.sleep(POLL_INTERVAL)
            continue

        # ── Track placed positionID for CTP isolation ─────────────────────
        ctp_open_position_ids.add(int(position_id))
        _persist_ctp_open_position_ids(open_positions_path, ctp_open_position_ids)
        logger.info(
            f"[{instance_label}] CTP open positions updated: {ctp_open_position_ids} "
            f"(persisted to {open_positions_path})"
        )

        orders_placed += 1
        logger.info("=" * 60)
        logger.info(f"[{instance_label}] ORDER PLACED #{orders_placed}")
        logger.info(f"  positionID   : {position_id}")
        logger.info(f"  symbol       : {signal.symbol}")
        logger.info(f"  direction    : {signal.direction}")
        logger.info(f"  amount       : {bs_config.execution.amount_usd} USD")
        logger.info(f"  leverage     : {bs_config.execution.leverage}x")
        logger.info(f"  stop_loss    : {signal.stop_loss_rate:.2f}")
        logger.info(f"  take_profit  : {signal.take_profit_rate:.2f}")
        logger.info(f"  session total: {orders_placed} order(s) placed")
        logger.info("=" * 60)

        time.sleep(POLL_INTERVAL)


# ---------------------------------------------------------------------------
# Interruptible sleep — checks kill switches every `chunk` seconds
# ---------------------------------------------------------------------------

def _sleep_interruptible(
    total_seconds: float,
    chunk: int,
    guard: PaperTradingGuard,
    instance_id: str | None = None,
) -> None:
    """
    Sleep for total_seconds, waking every `chunk` seconds to check kill switches.
    Exits cleanly if either master or per-instance kill switch is detected.
    """
    instance_label = instance_id or "default"
    remaining = total_seconds
    while remaining > 0:
        sleep_for = min(chunk, remaining)
        time.sleep(sleep_for)
        remaining -= sleep_for
        try:
            _check_kill_switches(instance_id, guard)
        except HaltLoopError as exc:
            logger.warning(f"HALT during pause [{instance_label}]: {exc}")
            sys.exit(0)
        if remaining > 0:
            logger.info(
                f"[{instance_label}] Still paused — "
                f"{remaining / 3600:.1f}h remaining until session open."
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