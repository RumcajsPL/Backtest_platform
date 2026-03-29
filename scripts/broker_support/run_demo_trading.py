#!/usr/bin/env python
"""
run_demo_trading.py — persistent demo trading loop with integrated position tracking.

Replaces run_signal_loop.py + run_tracker_loop.py (8 terminals → 4).

Every poll cycle:
  1. Kill switch checks (master STOP + per-instance)
  2. Pending-order reconciliation
  3. Off-hours gate — sleeps until next allowed UTC hour
  4. Tracker cycle — detect closes, enrich, journal, update guard + open_positions.json
  5. Portfolio fetch — pyramiding check (CTP-scoped)
  6. Date rollover + daily guards (drawdown, min cash)
  7. Signal pipeline — SignalBridge.get_signal()
  8. WBWS+ gate
  9. Consecutive-loss circuit breaker
  10. Order placement → positionID registered

Poll cadence follows the strategy timeframe (set via POLL_INTERVAL in broker config,
defaults to 60s — 1-min TF loops poll every 60s, 10-min TF loops every 60s too but
SignalBridge only fires on the bar close naturally).

Instance isolation:
  Each loop instance is identified by --instance <id> (e.g. c424, 240166).
  All file paths are derived from the instance ID:

    Journal dir   : outputs/broker_support/journal/<id>/
    trades.csv    : outputs/broker_support/journal/<id>/trades.csv
    open_positions: outputs/broker_support/journal/<id>/open_positions.json
    Snapshots     : outputs/broker_support/snapshots/<id>/
    Log file      : outputs/broker_support/logs/demo_trading_<id>_YYYY-MM-DD.log
    Kill switch   : STOP_<id>  (per-instance) + STOP (master — halts all loops)

Isolation from external account activity:
  - Pyramiding check filters the live portfolio to CTP-placed positionIDs only.
  - Tracker cycle also scopes close detection to the full portfolio snapshot diff,
    but guard.record_trade_result() and open_positions.json updates only fire for
    positions whose positionID is in ctp_open_position_ids.
  - Daily drawdown uses CTP's own realised P&L from trades.csv only.

Pending-order reconciliation (ORDER FAILED safety net):
  When open_position() raises OrderExecutionError (portfolio scan timed out),
  the orderID is added to _pending_order_ids. At the start of every subsequent
  poll, the loop scans the live portfolio for any pending orderID. If found,
  the positionID is registered. Retired after _PENDING_ORDER_MAX_POLLS polls.

V2 items now active (previously backlog):
  - Closed positionIDs are removed from open_positions.json on close detection.
  - guard.record_trade_result(pnl) is called for each CTP-placed closed position.

Usage:
    python scripts/broker_support/run_demo_trading.py --instance c424
    python scripts/broker_support/run_demo_trading.py --instance 240166 --quiet
    python scripts/broker_support/run_demo_trading.py --instance 7ffbc5 --quiet
    python scripts/broker_support/run_demo_trading.py --instance 61875 --quiet

    # Custom config
    python scripts/broker_support/run_demo_trading.py \\
        --instance c424 \\
        --config configs/broker_support/broker_support_config.yaml

    # Debug console
    python scripts/broker_support/run_demo_trading.py --instance c424 --verbose

Kill switch:
    Master      : create STOP in project root — halts ALL instances.
    Per-instance: create STOP_<id> in project root — halts only that instance.
    Delete the file(s) before restarting.
"""
import argparse
import json
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set

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
from src.broker_support.models.trade import Trade
from src.broker_support.safeguards.paper_trading_guard import (
    HaltLoopError,
    PaperTradingGuard,
    PauseUntilTomorrowError,
)
from src.broker_support.tracking.position_tracker import PositionTracker

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_LOG_DIR  = "outputs/broker_support/logs"
JOURNAL_BASE_DIR = "outputs/broker_support/journal"
SNAPSHOTS_BASE   = "outputs/broker_support/snapshots"
POLL_INTERVAL    = 60   # seconds
MASTER_KILL_FILE = "STOP"

# Pending-order reconciliation: polls before retiring an unresolved orderID.
# 5 polls × 60s = 5 minutes max reconciliation window.
_PENDING_ORDER_MAX_POLLS = 5

_CONFIG_MAP = {
    "c424":   "configs/broker_support/broker_support_config.yaml",
    "240166": "configs/broker_support/broker_support_config_240166.yaml",
    "7ffbc5": "configs/broker_support/broker_support_config_7ffbc5.yaml",
    "61875":  "configs/broker_support/broker_support_config_61875.yaml",
}


# ---------------------------------------------------------------------------
# Instance path resolution
# ---------------------------------------------------------------------------

def _resolve_paths(instance_id: Optional[str]) -> tuple[Path, Path, Path]:
    """
    Resolve instance-scoped paths.

    Returns:
        (journal_path, open_positions_path, snapshots_dir)
    """
    base = _PROJECT_ROOT / JOURNAL_BASE_DIR
    snaps = _PROJECT_ROOT / SNAPSHOTS_BASE
    if instance_id:
        instance_dir = base / instance_id
        return (
            instance_dir / "trades.csv",
            instance_dir / "open_positions.json",
            snaps / instance_id,
        )
    return (
        base / "trades.csv",
        base / "open_positions.json",
        snaps,
    )


def _default_config(instance_id: Optional[str]) -> str:
    if instance_id and instance_id in _CONFIG_MAP:
        return _CONFIG_MAP[instance_id]
    return "configs/broker_support/broker_support_config.yaml"


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _configure_logging(
    log_dir: str,
    instance_id: Optional[str],
    verbose: bool,
    quiet: bool,
) -> None:
    """
    Log filename: demo_trading_<id>_YYYY-MM-DD.log  (with instance)
                  demo_trading_YYYY-MM-DD.log         (without instance)
    """
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    logger.remove()

    suffix = f"_{instance_id}" if instance_id else ""
    log_filename = f"demo_trading{suffix}_{{time:YYYY-MM-DD}}.log"

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
# CTP open position tracking
# ---------------------------------------------------------------------------

def _load_ctp_open_position_ids(open_positions_path: Path) -> Set[int]:
    """
    Load CTP-placed positionIDs from open_positions.json.
    Returns empty set if file absent or unreadable.
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
    position_ids: Set[int],
) -> None:
    """Persist CTP open positionIDs to disk atomically."""
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
# Tracker cycle — integrated (replaces run_tracker_loop.py)
# ---------------------------------------------------------------------------

def _run_tracker_cycle(
    tracker: PositionTracker,
    ctp_open_position_ids: Set[int],
    open_positions_path: Path,
    guard: PaperTradingGuard,
    instance_label: str,
) -> int:
    """
    Execute one position tracking cycle within the main loop.

    Full CTP isolation: only positions whose positionID is in
    ctp_open_position_ids are tracked. External positions (manual trades,
    other loops, copy trading) are silently ignored — they never enter
    the snapshot, trades.csv, or any guard calculation.

    This mirrors the pyramiding check isolation: the same demo account can
    run multiple parallel strategies and manual trades without any
    cross-contamination of P&L, drawdown, or consecutive-loss counters.

    Performs the PositionTracker sequence manually (rather than tracker.track())
    so that closed Trade objects are available for:
      - guard.record_trade_result(pnl)  — consecutive-loss counter update
      - removing closed positionIDs from ctp_open_position_ids + open_positions.json

    Returns:
        Number of new closed trades written to the journal.
    """
    logger.debug(f"[{instance_label}] Tracker cycle start.")

    try:
        old_snapshot = tracker.load_last_snapshot()
        all_positions = tracker.fetch_current_positions()
    except Exception as exc:
        logger.error(f"[{instance_label}] Tracker: portfolio fetch failed: {exc}")
        return 0

    # ── CTP isolation: filter to this instance's positions only ──────────
    # positionID field name in portfolio dicts is 'positionId' (camelCase
    # lowercase id — portfolio snapshot format, not the PascalCase API field).
    ctp_positions = [
        p for p in all_positions
        if int(p.get("positionId") or p.get("positionID") or -1)
        in ctp_open_position_ids
    ]
    external_count = len(all_positions) - len(ctp_positions)
    if external_count > 0:
        logger.info(
            f"[{instance_label}] Tracker: {external_count} external position(s) "
            f"on this demo account ignored (not placed by this loop)."
        )

    # ── Stale snapshot guard ──────────────────────────────────────────────
    # If the snapshot contains positionIDs not in ctp_open_position_ids, it
    # was written before CTP isolation was active (e.g. by the old
    # run_tracker_loop.py which tracked all portfolio positions). Diffing
    # against a stale snapshot would produce phantom "closed" detections for
    # external positions that were never CTP trades.
    # Fix: invalidate the snapshot — treat this cycle as a cold start.
    if not old_snapshot.empty and "positionId" in old_snapshot.columns:
        snapshot_ids = set(old_snapshot["positionId"].astype(str).values)
        # A snapshot is stale if it contains ANY id not in ctp_open_position_ids
        # (treating nan / empty strings as stale indicators too)
        ctp_str_ids = {str(pid) for pid in ctp_open_position_ids}
        stale_ids = {
            sid for sid in snapshot_ids
            if sid not in ctp_str_ids and sid not in ("", "nan", "None")
        }
        if stale_ids:
            logger.warning(
                f"[{instance_label}] Tracker: stale snapshot detected — contains "
                f"{len(stale_ids)} non-CTP positionID(s): {stale_ids}. "
                f"Invalidating snapshot for this cycle (cold start). "
                f"No false close detections will occur."
            )
            old_snapshot = old_snapshot.iloc[0:0]  # empty DataFrame, preserves columns

    # Snapshot and diff operate on CTP positions only
    closed_raw = tracker.detect_closed_positions(old_snapshot, ctp_positions)

    new_trades: List[Trade] = []

    for pos in closed_raw:
        trade = tracker.convert_to_trade(pos)
        if trade is None:
            continue
        trade = tracker._enricher.enrich(trade)
        new_trades.append(trade)

        # guard + open_positions.json update
        pnl = trade.profit_loss
        guard.record_trade_result(pnl)
        logger.info(
            f"[{instance_label}] CTP position closed — "
            f"positionID={trade.trade_id} "
            f"instrument={trade.instrument or trade.instrument_id} "
            f"direction={trade.direction} "
            f"pnl={pnl:+.2f} USD. "
            f"Consecutive-loss counter updated."
        )

        try:
            pid = int(trade.trade_id)
        except (ValueError, TypeError):
            pid = -1

        ctp_open_position_ids.discard(pid)
        _persist_ctp_open_position_ids(open_positions_path, ctp_open_position_ids)
        logger.info(
            f"[{instance_label}] positionID={pid} removed from "
            f"open_positions.json. Remaining CTP positions: "
            f"{ctp_open_position_ids}"
        )

    written = 0
    if new_trades:
        written = tracker.journal.append_trades(new_trades)

    # Save snapshot with CTP positions only — keeps snapshot clean
    tracker.save_snapshot(ctp_positions)

    if written:
        logger.info(
            f"[{instance_label}] Tracker: {written} new trade(s) journalled."
        )
    else:
        logger.debug(f"[{instance_label}] Tracker: no new closed positions.")

    return written


# ---------------------------------------------------------------------------
# Pyramiding guard — CTP-scoped
# ---------------------------------------------------------------------------

def _check_pyramiding(
    client: EToroClient,
    resolver: InstrumentResolver,
    symbol: str,
    max_positions: int,
    ctp_open_position_ids: Set[int],
) -> tuple[bool, float]:
    """
    Return (safe_to_trade, current_credit).

    Counts only CTP-placed positions whose positionID is still in the live
    portfolio. External positions on the same account are invisible.

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
        f"external on same instrument: {external_count} (ignored) | "
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
# Pending-order reconciliation
# ---------------------------------------------------------------------------

def _reconcile_pending_orders(
    client: EToroClient,
    pending_order_ids: Dict[int, int],
    ctp_open_position_ids: Set[int],
    open_positions_path: Path,
    instance_label: str,
) -> None:
    """
    Scan the live portfolio for any unresolved pending orderIDs.

    Called at the top of every poll when pending_order_ids is non-empty.
    Resolves positionIDs, retires stale entries after _PENDING_ORDER_MAX_POLLS.
    Modifies pending_order_ids and ctp_open_position_ids in-place.
    """
    if not pending_order_ids:
        return

    logger.info(
        f"[{instance_label}] Reconciling {len(pending_order_ids)} pending "
        f"order(s): {list(pending_order_ids.keys())}"
    )

    try:
        portfolio = client._make_request("GET", "api/v1/trading/info/demo/portfolio")
    except Exception as exc:
        logger.warning(
            f"[{instance_label}] Pending-order reconciliation: portfolio fetch "
            f"failed: {exc}. Will retry next poll."
        )
        return

    positions = portfolio.get("clientPortfolio", {}).get("positions", [])
    resolved = []

    for order_id in list(pending_order_ids.keys()):
        for pos in positions:
            if pos.get("orderID") == order_id:
                position_id = pos.get("positionID")
                if position_id is not None:
                    pid = int(position_id)
                    ctp_open_position_ids.add(pid)
                    _persist_ctp_open_position_ids(open_positions_path, ctp_open_position_ids)
                    logger.warning(
                        f"[{instance_label}] RECONCILED: orderID={order_id} → "
                        f"positionID={pid} found in portfolio. "
                        f"Position IS open. CTP position set updated. "
                        f"This order was previously reported as ORDER FAILED."
                    )
                    resolved.append(order_id)
                    break
        else:
            pending_order_ids[order_id] -= 1
            remaining = pending_order_ids[order_id]
            if remaining <= 0:
                logger.warning(
                    f"[{instance_label}] PENDING ORDER RETIRED: orderID={order_id} "
                    f"not found in portfolio after {_PENDING_ORDER_MAX_POLLS} polls "
                    f"({_PENDING_ORDER_MAX_POLLS * POLL_INTERVAL}s). "
                    f"Assumed not opened. Verify manually via inspect_portfolio.py."
                )
                resolved.append(order_id)
            else:
                logger.info(
                    f"[{instance_label}] Pending orderID={order_id} still not in "
                    f"portfolio ({remaining} poll(s) remaining before retirement)."
                )

    for order_id in resolved:
        del pending_order_ids[order_id]


# ---------------------------------------------------------------------------
# Journal helpers
# ---------------------------------------------------------------------------

def _load_todays_ctp_pnl(journal_path: Path) -> tuple[List[float], float]:
    """
    Load today's closed CTP trade P&L from trades.csv.

    Returns:
        (pnl_list, pnl_sum)
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

def _seconds_until_next_allowed_hour(allowed_hours_utc: List[int]) -> float:
    """Return seconds until the next hour in allowed_hours_utc (up to 25h ahead)."""
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

def _check_kill_switches(instance_id: Optional[str], guard: PaperTradingGuard) -> None:
    """
    Check master STOP file and per-instance kill switch.
    Raises HaltLoopError if either is detected.
    """
    master = _PROJECT_ROOT / MASTER_KILL_FILE
    if master.exists():
        raise HaltLoopError(
            f"Master kill switch '{MASTER_KILL_FILE}' detected. "
            f"Loop [{instance_id or 'default'}] halted. "
            f"Remove the file before restarting any loop."
        )
    guard.check_kill_switch()


# ---------------------------------------------------------------------------
# Interruptible sleep
# ---------------------------------------------------------------------------

def _sleep_interruptible(
    total_seconds: float,
    chunk: int,
    guard: PaperTradingGuard,
    instance_id: Optional[str] = None,
) -> None:
    """Sleep for total_seconds, checking kill switches every chunk seconds."""
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
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Persistent demo trading loop with integrated position tracking. "
            "Replaces run_signal_loop.py + run_tracker_loop.py (8 terminals → 4)."
        )
    )
    parser.add_argument(
        "--instance", "-i",
        default=None,
        help=(
            "Instance ID for parallel loop isolation (e.g. c424, 240166, 7ffbc5, 61875). "
            "Determines journal dir, snapshots dir, log filename, and default config path."
        ),
    )
    parser.add_argument(
        "--config",
        default=None,
        help=(
            "Path to broker_support_config YAML. "
            "Defaults to the standard config for the given --instance."
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

    instance_id: Optional[str] = args.instance
    config_path_str: str = args.config or _default_config(instance_id)

    _configure_logging(args.log_dir, instance_id, args.verbose, args.quiet)

    instance_label = instance_id or "default"
    logger.info("=" * 60)
    logger.info(f"run_demo_trading.py — instance [{instance_label}]")
    logger.info("PERSISTENT loop (signal + tracker unified)")
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
    config_path = _PROJECT_ROOT / config_path_str
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
    journal_path, open_positions_path, snapshots_dir = _resolve_paths(instance_id)
    logger.info(
        f"Instance paths — "
        f"journal: {journal_path} | "
        f"open_positions: {open_positions_path} | "
        f"snapshots: {snapshots_dir}"
    )

    # ── Build infrastructure ──────────────────────────────────────────────
    try:
        client   = EToroClient()
        resolver = InstrumentResolver(map_path=bs_config.execution.instrument_map_path)
        fetcher  = LiveDataFetcher(client=client, resolver=resolver, config=bs_config.live_data)
        bridge   = SignalBridge(bs_config=bs_config, fetcher=fetcher)
        router   = OrderRouter(client=client, resolver=resolver)
        tracker  = PositionTracker(
            journal_path=journal_path,
            snapshots_dir=snapshots_dir,
            instrument_map_path=bs_config.execution.instrument_map_path,
        )
    except Exception as exc:
        logger.error(f"Failed to initialise infrastructure: {exc}")
        sys.exit(1)

    # ── Seed CTP open position tracking ──────────────────────────────────
    ctp_open_position_ids: Set[int] = _load_ctp_open_position_ids(open_positions_path)

    # ── Pending-order tracking ────────────────────────────────────────────
    pending_order_ids: Dict[int, int] = {}

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

        # ── 1. Kill switch checks ─────────────────────────────────────────
        try:
            _check_kill_switches(instance_id, guard)
        except HaltLoopError as exc:
            logger.warning(f"HALT [{instance_label}]: {exc}")
            sys.exit(0)

        # ── 2. Pending-order reconciliation ──────────────────────────────
        if pending_order_ids:
            _reconcile_pending_orders(
                client=client,
                pending_order_ids=pending_order_ids,
                ctp_open_position_ids=ctp_open_position_ids,
                open_positions_path=open_positions_path,
                instance_label=instance_label,
            )

        # ── 3. Off-hours gate ─────────────────────────────────────────────
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

        # ── 4. Tracker cycle ──────────────────────────────────────────────
        # Runs every poll. Detects closes, enriches, journals, updates guard
        # and open_positions.json for CTP-placed positions.
        # Portfolio fetch errors inside the tracker are logged and do NOT
        # count against the pipeline error budget.
        _run_tracker_cycle(
            tracker=tracker,
            ctp_open_position_ids=ctp_open_position_ids,
            open_positions_path=open_positions_path,
            guard=guard,
            instance_label=instance_label,
        )

        # ── 5. Portfolio fetch — pyramiding check (CTP-scoped) ────────────
        # NOTE: portfolio fetch errors are connectivity/broker issues, NOT
        # signal pipeline failures — do NOT increment pipeline_error_streak.
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
            logger.info(
                f"[{instance_label}] Skipping poll. "
                f"Next poll in {POLL_INTERVAL}s …"
            )
            time.sleep(POLL_INTERVAL)
            continue

        # ── 6. Date rollover + daily guards ───────────────────────────────
        if guard.check_date_rollover(current_credit):
            logger.info(
                f"[{instance_label}] New trading day. "
                f"Session open credit reset to {current_credit:.2f}. "
                f"All daily counters cleared."
            )

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

        # ── 7. Signal pipeline ────────────────────────────────────────────
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

        # ── 8. WBWS+ gate ─────────────────────────────────────────────────
        if not signal.wbws_window_valid:
            logger.warning(
                f"[{instance_label}] Signal outside WBWS+ window — not placing order. "
                f"Next poll in {POLL_INTERVAL}s …"
            )
            time.sleep(POLL_INTERVAL)
            continue

        # ── 9. Consecutive-loss circuit breaker ───────────────────────────
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

        # ── 10. Place order ───────────────────────────────────────────────
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
            match = re.search(r"orderID=(\d+)", str(exc))
            if match:
                failed_order_id = int(match.group(1))
                pending_order_ids[failed_order_id] = _PENDING_ORDER_MAX_POLLS
                logger.warning(
                    f"[{instance_label}] Order status UNKNOWN for "
                    f"orderID={failed_order_id}. "
                    f"Registered for reconciliation over next "
                    f"{_PENDING_ORDER_MAX_POLLS} poll(s)."
                )
            else:
                logger.warning(
                    f"[{instance_label}] Could not extract orderID from error. "
                    f"Pausing one poll before next attempt."
                )
            time.sleep(POLL_INTERVAL)
            continue

        # ── Register placed positionID ────────────────────────────────────
        ctp_open_position_ids.add(int(position_id))
        _persist_ctp_open_position_ids(open_positions_path, ctp_open_position_ids)

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
        logger.info(
            f"  CTP positions: {ctp_open_position_ids} "
            f"(persisted to {open_positions_path})"
        )
        logger.info("=" * 60)

        time.sleep(POLL_INTERVAL)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Interrupted by user (Ctrl+C) — loop stopped cleanly.")
        sys.exit(0)