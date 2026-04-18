#!/usr/bin/env python
"""
week_one_health_check.py — CTP multi-instance health check.

Performs a single-pass analysis of all instance log files and produces
a structured report covering:

  1. Signal pipeline summary per instance
       - Total polls, signals found, RISK_REJECTED, NO_SIGNAL, WBWS+ blocked
       - Pass-through rate at each pipeline stage
       - Risk rejection reasons (from RiskManager log lines)

  2. Order outcome summary per instance
       - Orders placed (confirmed positionID)
       - ORDER FAILED events (portfolio scan timeout)
       - Unconfirmed orderIDs requiring trade history reconciliation

  3. Guard / circuit breaker audit
       - Every HALT, PAUSE, and circuit breaker event with context
       - Pipeline error counter events — distinguishes portfolio fetch errors
         (should NOT count) vs signal pipeline errors (should count)

  4. Constraint adherence check
       - Pyramiding blocks logged correctly
       - WBWS+ blocks logged correctly
       - Consecutive loss counter behaviour

  5. Infrastructure error summary
       - 429 / 503 / timeout events with timestamps
       - Distinguishes broker-side outages from genuine pipeline failures

  6. Journal state
       - trades.csv status per instance
       - open_positions.json state
       - Actions required before next restart

  7. P&L summary (from trades.csv)
       - Total trades, win rate, total PnL
       - Avg win / avg loss / largest win / largest loss
       - Per-instrument breakdown

Usage:
    python scripts/diagnostics/week_one_health_check.py
    python scripts/diagnostics/week_one_health_check.py --instance 240166
    python scripts/diagnostics/week_one_health_check.py --dates 2026-03-30 2026-04-17
    python scripts/diagnostics/week_one_health_check.py --verbose

Output:
    Console report (always)
    outputs/broker_support/diagnostics/health_check_YYYY-MM-DD.txt (always)
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ALL_INSTANCES = ["c424", "240166", "7ffbc5", "61875"]

LOGS_DIR    = _PROJECT_ROOT / "outputs" / "broker_support" / "logs"
JOURNAL_DIR = _PROJECT_ROOT / "outputs" / "broker_support" / "journal"
DIAG_DIR    = _PROJECT_ROOT / "outputs" / "broker_support" / "diagnostics"

# Log filename prefix — matches run_demo_trading.py log naming
_LOG_PREFIX = "demo_trading"

# Default date range: last 7 calendar days (covers any rolling week)
def _default_dates() -> List[str]:
    today = datetime.now(timezone.utc).date()
    return [(today - timedelta(days=i)).isoformat() for i in range(6, -1, -1)]


# ---------------------------------------------------------------------------
# Log line patterns
# ---------------------------------------------------------------------------

_RE_TIMESTAMP    = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
_RE_POLL         = re.compile(r"Poll #(\d+) at (\d{2}:\d{2}:\d{2} UTC)")
_RE_NO_SIGNAL    = re.compile(r"result=NO_SIGNAL")
_RE_RISK_REJ     = re.compile(r"result=RISK_REJECTED")
_RE_RISK_SUM     = re.compile(r"Risk summary: (.+)")
_RE_RAW_SIGS     = re.compile(r"Raw signals: buy=(\d+), sell=(\d+), total=(\d+)")
_RE_AFTER_FILT   = re.compile(r"After filters: (\d+) → (\d+) signals \(pass_rate=([\d.]+)%\)")
_RE_SIGNAL_FOUND = re.compile(r"SIGNAL FOUND:")
_RE_WBWS_BLOCK   = re.compile(r"Signal outside WBWS\+ window")
_RE_ORDER_PLACED = re.compile(r"ORDER PLACED #(\d+)")
_RE_POSITION_ID  = re.compile(r"positionID\s*:\s*(\d+)")
_RE_ORDER_FAILED = re.compile(r"ORDER FAILED: Portfolio scan: positionID for orderID=(\d+)")
_RE_PYRAMID_BLOCK= re.compile(r"CTP max positions reached \((\d+)/(\d+)\)")
_RE_HALT         = re.compile(r"HALT \[[\w]+\]: (.+)")
_RE_PAUSE        = re.compile(r"PAUSE: (.+)")
_RE_PIPELINE_ERR = re.compile(r"Pipeline error streak: (\d+)/(\d+)")
_RE_PORT_ERR     = re.compile(r"Portfolio fetch error: (.+)")
_RE_429          = re.compile(r"API error 429")
_RE_503          = re.compile(r"API error 503")
_RE_502          = re.compile(r"API error 502")
_RE_409          = re.compile(r"API error 409")
_RE_TIMEOUT      = re.compile(r"Read timed out|Connection aborted|ConnectionResetError")
_RE_GUARD_STATUS = re.compile(
    r"Guard: consecutive_losses=(\d+)/(\d+) \| pipeline_errors=(\d+)/(\d+)"
)
# Tracker cycle close detection (new in run_demo_trading.py)
_RE_CTP_CLOSED   = re.compile(
    r"CTP position closed — positionID=(\S+) .*pnl=([+-]?[\d.]+)"
)
_RE_TRACKER_NEW  = re.compile(r"Tracker: (\d+) new trade\(s\) journalled")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class OrderFailedEvent:
    timestamp: str
    order_id: int


@dataclass
class HaltEvent:
    timestamp: str
    reason: str


@dataclass
class InfraErrorEvent:
    timestamp: str
    error_type: str
    context: str


@dataclass
class RejectionEvent:
    timestamp: str
    risk_summary: str


@dataclass
class ClosedPositionEvent:
    timestamp: str
    position_id: str
    pnl: float


@dataclass
class InstanceStats:
    instance_id: str
    dates_found: List[str] = field(default_factory=list)

    # Poll counts
    total_polls: int = 0

    # Pipeline
    no_signal_polls: int = 0
    risk_rejected_count: int = 0
    risk_rejection_events: List[RejectionEvent] = field(default_factory=list)
    signals_found: int = 0
    wbws_blocked: int = 0

    # Orders
    orders_placed: int = 0
    orders_failed: List[OrderFailedEvent] = field(default_factory=list)
    position_ids_confirmed: List[int] = field(default_factory=list)

    # Tracker (integrated)
    tracker_cycles_with_closes: int = 0
    ctp_closed_events: List[ClosedPositionEvent] = field(default_factory=list)

    # Guards
    halt_events: List[HaltEvent] = field(default_factory=list)
    pause_events: List[HaltEvent] = field(default_factory=list)
    pyramid_blocks: int = 0
    pipeline_error_streaks: List[Tuple[int, int]] = field(default_factory=list)
    portfolio_fetch_errors: int = 0
    max_consecutive_losses_seen: int = 0

    # Infrastructure
    infra_errors: List[InfraErrorEvent] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Log parser
# ---------------------------------------------------------------------------

def _parse_log_file(path: Path, stats: InstanceStats, verbose: bool) -> None:
    """Single-pass parse of one log file into InstanceStats."""
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()

    # Extract date from filename: demo_trading_<id>_YYYY-MM-DD.log
    stem_parts = path.stem.split("_")
    log_date = stem_parts[-1] if stem_parts else path.stem
    if log_date not in stats.dates_found:
        stats.dates_found.append(log_date)

    pending_risk_rejection: Optional[RejectionEvent] = None

    for i, line in enumerate(lines):
        ts_m = _RE_TIMESTAMP.match(line)
        ts = ts_m.group(1) if ts_m else "unknown"

        # Poll counter
        if _RE_POLL.search(line):
            stats.total_polls += 1

        # NO_SIGNAL
        if _RE_NO_SIGNAL.search(line):
            stats.no_signal_polls += 1

        # RISK_REJECTED
        if _RE_RISK_REJ.search(line):
            stats.risk_rejected_count += 1
            pending_risk_rejection = RejectionEvent(timestamp=ts, risk_summary="")

        # Risk summary (follows RISK_REJECTED)
        m = _RE_RISK_SUM.search(line)
        if m and pending_risk_rejection:
            pending_risk_rejection.risk_summary = m.group(1).strip()
            stats.risk_rejection_events.append(pending_risk_rejection)
            pending_risk_rejection = None

        # SIGNAL FOUND
        if _RE_SIGNAL_FOUND.search(line):
            stats.signals_found += 1

        # WBWS+ blocked
        if _RE_WBWS_BLOCK.search(line):
            stats.wbws_blocked += 1

        # ORDER PLACED
        if _RE_ORDER_PLACED.search(line):
            stats.orders_placed += 1

        # positionID confirmed (within ORDER PLACED block)
        m = _RE_POSITION_ID.search(line)
        if m:
            context = "".join(lines[max(0, i - 5): i + 1])
            if "ORDER PLACED" in context:
                pid = int(m.group(1))
                if pid not in stats.position_ids_confirmed:
                    stats.position_ids_confirmed.append(pid)

        # ORDER FAILED
        m = _RE_ORDER_FAILED.search(line)
        if m:
            stats.orders_failed.append(
                OrderFailedEvent(timestamp=ts, order_id=int(m.group(1)))
            )

        # Tracker close detection (integrated loop — run_demo_trading.py)
        m = _RE_CTP_CLOSED.search(line)
        if m:
            stats.ctp_closed_events.append(
                ClosedPositionEvent(
                    timestamp=ts,
                    position_id=m.group(1),
                    pnl=float(m.group(2)),
                )
            )

        m = _RE_TRACKER_NEW.search(line)
        if m and int(m.group(1)) > 0:
            stats.tracker_cycles_with_closes += 1

        # Pyramiding block
        if _RE_PYRAMID_BLOCK.search(line):
            stats.pyramid_blocks += 1

        # Pipeline error streak
        m = _RE_PIPELINE_ERR.search(line)
        if m:
            stats.pipeline_error_streaks.append((int(m.group(1)), int(m.group(2))))

        # Portfolio fetch errors
        if _RE_PORT_ERR.search(line):
            stats.portfolio_fetch_errors += 1

        # HALT / PAUSE
        m = _RE_HALT.search(line)
        if m:
            stats.halt_events.append(HaltEvent(timestamp=ts, reason=m.group(1).strip()))

        m = _RE_PAUSE.search(line)
        if m and "WBWS" not in line:
            stats.pause_events.append(HaltEvent(timestamp=ts, reason=m.group(1).strip()))

        # Infrastructure errors
        if _RE_429.search(line):
            stats.infra_errors.append(InfraErrorEvent(ts, "429_TOO_MANY_REQUESTS", line.strip()[-80:]))
        elif _RE_503.search(line):
            stats.infra_errors.append(InfraErrorEvent(ts, "503_SERVICE_UNAVAILABLE", line.strip()[-80:]))
        elif _RE_502.search(line):
            stats.infra_errors.append(InfraErrorEvent(ts, "502_BAD_GATEWAY", line.strip()[-80:]))
        elif _RE_409.search(line):
            stats.infra_errors.append(InfraErrorEvent(ts, "409_CONFLICT", line.strip()[-80:]))
        elif _RE_TIMEOUT.search(line):
            stats.infra_errors.append(InfraErrorEvent(ts, "TIMEOUT_OR_CONN_RESET", line.strip()[-80:]))

        # Guard status
        m = _RE_GUARD_STATUS.search(line)
        if m:
            cl = int(m.group(1))
            if cl > stats.max_consecutive_losses_seen:
                stats.max_consecutive_losses_seen = cl


# ---------------------------------------------------------------------------
# Journal helpers
# ---------------------------------------------------------------------------

def _check_journals(instances: List[str]) -> Dict[str, dict]:
    """Check journal state for each instance."""
    results = {}
    for inst in instances:
        inst_dir    = JOURNAL_DIR / inst
        trades_csv  = inst_dir / "trades.csv"
        open_pos    = inst_dir / "open_positions.json"

        open_ids: List[int] = []
        if open_pos.exists():
            try:
                data = json.loads(open_pos.read_text(encoding="utf-8"))
                open_ids = data.get("position_ids", [])
            except Exception:
                pass

        trade_count = 0
        if trades_csv.exists() and trades_csv.stat().st_size > 0:
            try:
                import pandas as pd
                df = pd.read_csv(trades_csv)
                trade_count = len(df)
            except Exception:
                trade_count = -1

        results[inst] = {
            "trades_csv_exists": trades_csv.exists(),
            "trade_count": trade_count,
            "open_position_ids": open_ids,
            "trades_csv_path": trades_csv,
        }
    return results


def _load_pnl_dataframe(trades_csv: Path):
    """Load trades.csv as DataFrame. Returns None if unavailable."""
    if not trades_csv.exists() or trades_csv.stat().st_size == 0:
        return None
    try:
        import pandas as pd
        df = pd.read_csv(trades_csv)
        required = {"profit_loss", "direction"}
        if not required.issubset(df.columns):
            return None
        df["profit_loss"] = pd.to_numeric(df["profit_loss"], errors="coerce")
        return df.dropna(subset=["profit_loss"])
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Report helpers
# ---------------------------------------------------------------------------

SEP  = "=" * 80
SEP2 = "-" * 60


def _fmt_section(title: str) -> str:
    return f"\n{SEP}\n  {title}\n{SEP}"


def _fmt_sub(title: str) -> str:
    return f"\n{SEP2}\n  {title}\n{SEP2}"


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------

def _build_report(
    all_stats: Dict[str, InstanceStats],
    journal_info: Dict[str, dict],
    dates: List[str],
    verbose: bool,
) -> str:
    lines: List[str] = []
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines.append(f"\nCTP HEALTH CHECK  —  generated {now}")
    lines.append(f"Instances : {', '.join(all_stats.keys())}")
    lines.append(f"Dates     : {dates[0]} → {dates[-1]}\n")

    # ── Section 1: Signal pipeline summary ───────────────────────────────
    lines.append(_fmt_section("1. SIGNAL PIPELINE SUMMARY"))

    for inst, s in all_stats.items():
        if s.total_polls == 0:
            lines.append(f"\n[{inst}]  No log files found — instance may not have run.")
            continue

        pipeline_runs = s.no_signal_polls + s.risk_rejected_count + s.signals_found
        risk_rej_pct = (
            s.risk_rejected_count / pipeline_runs * 100 if pipeline_runs > 0 else 0.0
        )
        lines.append(f"\n[{inst}]")
        lines.append(f"  Log dates found    : {', '.join(sorted(s.dates_found))}")
        lines.append(f"  Total polls        : {s.total_polls}")
        lines.append(f"  Pipeline runs      : {pipeline_runs}")
        lines.append(f"  NO_SIGNAL          : {s.no_signal_polls}")
        lines.append(
            f"  RISK_REJECTED      : {s.risk_rejected_count}  "
            f"({risk_rej_pct:.1f}% of pipeline runs)"
        )
        lines.append(f"  SIGNAL FOUND       : {s.signals_found}  (passed RiskManager)")
        lines.append(
            f"  WBWS+ blocked      : {s.wbws_blocked}  "
            f"(signal valid but hour outside window)"
        )
        lines.append(f"  Orders placed      : {s.orders_placed}")
        lines.append(
            f"  ORDER FAILED       : {len(s.orders_failed)}  "
            f"(portfolio scan timeout — position status UNKNOWN)"
        )
        lines.append(
            f"  Pyramid blocks     : {s.pyramid_blocks}  "
            f"(correctly idled while CTP position open)"
        )
        lines.append(
            f"  Tracker closes     : {len(s.ctp_closed_events)}  "
            f"(CTP positions detected closed by integrated tracker)"
        )
        lines.append(f"  Max consec. losses : {s.max_consecutive_losses_seen}")

        if s.risk_rejection_events and verbose:
            lines.append(f"\n  Risk rejection details:")
            seen: Dict[str, int] = defaultdict(int)
            for ev in s.risk_rejection_events:
                seen[ev.risk_summary] += 1
            for summary, count in sorted(seen.items(), key=lambda x: -x[1]):
                lines.append(f"    × {count:3d}  {summary or '(no summary logged)'}")

    # ── Section 2: Order outcome summary ─────────────────────────────────
    lines.append(_fmt_section("2. ORDER OUTCOME SUMMARY"))

    for inst, s in all_stats.items():
        if s.total_polls == 0:
            continue
        lines.append(f"\n[{inst}]")
        lines.append(f"  Confirmed positionIDs : {s.position_ids_confirmed or 'none'}")

        if s.orders_failed:
            lines.append(f"  Unconfirmed orders (portfolio scan timed out):")
            for ev in s.orders_failed:
                lines.append(
                    f"    {ev.timestamp}  orderID={ev.order_id}"
                    f"  ← verify via inspect_portfolio.py or eToro UI"
                )
        else:
            lines.append(f"  Unconfirmed orders    : none")

        if s.ctp_closed_events:
            lines.append(f"  Closes detected by tracker:")
            for ev in s.ctp_closed_events:
                pnl_str = f"{ev.pnl:+.2f}"
                lines.append(
                    f"    {ev.timestamp}  positionID={ev.position_id}  pnl={pnl_str} USD"
                )

    # ── Section 3: Guard / circuit breaker audit ─────────────────────────
    lines.append(_fmt_section("3. GUARD / CIRCUIT BREAKER AUDIT"))

    for inst, s in all_stats.items():
        if s.total_polls == 0:
            continue
        lines.append(f"\n[{inst}]")

        if s.halt_events:
            lines.append(f"  HALT events ({len(s.halt_events)}):")
            for ev in s.halt_events:
                lines.append(f"    {ev.timestamp}  {ev.reason[:120]}")
        else:
            lines.append(f"  HALT events           : none")

        if s.pause_events:
            lines.append(f"  PAUSE events ({len(s.pause_events)}):")
            for ev in s.pause_events:
                lines.append(f"    {ev.timestamp}  {ev.reason[:120]}")
        else:
            lines.append(f"  PAUSE events          : none")

        lines.append(
            f"  Portfolio fetch errors : {s.portfolio_fetch_errors}  "
            f"(connectivity — excluded from pipeline error budget)"
        )

        if s.pipeline_error_streaks:
            max_streak = max(v for v, _ in s.pipeline_error_streaks)
            lines.append(
                f"  Pipeline error streaks: max={max_streak}  "
                f"(signal pipeline only — threshold=5)"
            )
            if max_streak >= 5:
                lines.append(
                    f"    ⚠️  Max streak reached threshold — loop halted. "
                    f"See HALT events above."
                )
        else:
            lines.append(f"  Pipeline error streaks: none")

    # ── Section 4: Infrastructure error summary ───────────────────────────
    lines.append(_fmt_section("4. INFRASTRUCTURE ERROR SUMMARY"))

    for inst, s in all_stats.items():
        if s.total_polls == 0:
            continue
        if not s.infra_errors:
            lines.append(f"\n[{inst}]  No infrastructure errors.")
            continue

        by_type: Dict[str, List[str]] = defaultdict(list)
        for ev in s.infra_errors:
            by_type[ev.error_type].append(ev.timestamp)

        lines.append(f"\n[{inst}]  {len(s.infra_errors)} infrastructure error(s):")
        for err_type, timestamps in sorted(by_type.items()):
            lines.append(f"  {err_type:<30} × {len(timestamps)}")
            if verbose:
                for ts in timestamps:
                    lines.append(f"    {ts}")

    # ── Section 5: Journal state ──────────────────────────────────────────
    lines.append(_fmt_section("5. JOURNAL STATE"))

    for inst, info in journal_info.items():
        lines.append(f"\n[{inst}]")
        if info["trades_csv_exists"]:
            lines.append(
                f"  trades.csv        : EXISTS  ({info['trade_count']} trades)"
            )
        else:
            lines.append(f"  trades.csv        : MISSING")

        if info["open_position_ids"]:
            lines.append(f"  open_positions    : {info['open_position_ids']}")
            lines.append(
                f"  ⚠️  Position(s) still marked open in open_positions.json."
            )
            lines.append(
                f"     Verify: python scripts/broker_support/inspect_portfolio.py "
                f"--instance {inst} --all-positions"
            )
            lines.append(
                f"     If already closed on eToro: remove positionID from file "
                f"before restarting loop."
            )
        else:
            lines.append(f"  open_positions    : empty")

    # ── Section 6: Actions required ───────────────────────────────────────
    lines.append(_fmt_section("6. ACTIONS REQUIRED BEFORE RESTARTING"))
    lines.append("")

    all_unconfirmed = [
        (inst, ev.order_id, ev.timestamp)
        for inst, s in all_stats.items()
        for ev in s.orders_failed
    ]

    if all_unconfirmed:
        lines.append("  A. Verify unconfirmed orders in eToro trade history:")
        lines.append(
            "     Orders had statusID=1 (broker accepted) but positionID "
            "resolution timed out."
        )
        for inst, oid, ts in all_unconfirmed:
            lines.append(f"       [{inst}]  orderID={oid}  at {ts}")
        lines.append(
            "     Command: python scripts/broker_support/inspect_portfolio.py "
            "--instance <id> --all-positions"
        )
        lines.append("")

    needs_open_pos_check = [
        inst for inst, info in journal_info.items() if info["open_position_ids"]
    ]
    if needs_open_pos_check:
        lines.append("  B. Verify open_positions.json before restarting:")
        for inst in needs_open_pos_check:
            ids = journal_info[inst]["open_position_ids"]
            lines.append(f"     [{inst}]  position_ids={ids}")
            lines.append(
                f"       python scripts/broker_support/inspect_portfolio.py "
                f"--instance {inst} --all-positions"
            )
        lines.append("")

    lines.append("  C. Deploy run_demo_trading.py (replaces run_signal_loop + run_tracker):")
    lines.append("       python scripts/broker_support/run_demo_trading.py --instance c424 --quiet")
    lines.append("       python scripts/broker_support/run_demo_trading.py --instance 240166 --quiet")
    lines.append("       python scripts/broker_support/run_demo_trading.py --instance 7ffbc5 --quiet")
    lines.append("       python scripts/broker_support/run_demo_trading.py --instance 61875 --quiet")
    lines.append("")

    lines.append("  D. RiskManager calibration:")
    lines.append(
        "     Insufficient data for 0.45% threshold review. "
        "Continue another full week before adjusting."
    )

    # ── Section 7: P&L summary ────────────────────────────────────────────
    lines.append(_fmt_section("7. P&L SUMMARY (from trades.csv)"))

    any_pnl_data = False

    for inst, info in journal_info.items():
        df = _load_pnl_dataframe(info["trades_csv_path"])
        if df is None or df.empty:
            lines.append(f"\n[{inst}]  No trade data available.")
            continue

        any_pnl_data = True
        pnl = df["profit_loss"]
        wins  = pnl[pnl > 0]
        losses = pnl[pnl <= 0]
        total = len(pnl)
        win_rate = len(wins) / total * 100 if total > 0 else 0.0
        total_pnl = pnl.sum()

        lines.append(f"\n[{inst}]")
        lines.append(f"  Total trades       : {total}")
        lines.append(f"  Win rate           : {win_rate:.1f}%  ({len(wins)}W / {len(losses)}L)")
        lines.append(f"  Total P&L          : {total_pnl:+.2f} USD")

        if len(wins) > 0:
            lines.append(f"  Avg win            : {wins.mean():+.2f} USD")
            lines.append(f"  Largest win        : {wins.max():+.2f} USD")
        else:
            lines.append(f"  Avg win            : n/a")
            lines.append(f"  Largest win        : n/a")

        if len(losses) > 0:
            lines.append(f"  Avg loss           : {losses.mean():+.2f} USD")
            lines.append(f"  Largest loss       : {losses.min():+.2f} USD")
        else:
            lines.append(f"  Avg loss           : n/a")
            lines.append(f"  Largest loss       : n/a")

        # Profit factor
        gross_profit = wins.sum() if len(wins) > 0 else 0.0
        gross_loss   = abs(losses.sum()) if len(losses) > 0 else 0.0
        if gross_loss > 0:
            lines.append(f"  Profit factor      : {gross_profit / gross_loss:.2f}")
        else:
            lines.append(f"  Profit factor      : n/a (no losses)")

        # Per-instrument breakdown
        if "instrument" in df.columns:
            lines.append(f"  Per instrument:")
            for instr, group in df.groupby("instrument"):
                g_pnl  = group["profit_loss"]
                g_wins = g_pnl[g_pnl > 0]
                g_wr   = len(g_wins) / len(g_pnl) * 100 if len(g_pnl) > 0 else 0.0
                lines.append(
                    f"    {str(instr):<12}  {len(g_pnl):3d} trades  "
                    f"WR={g_wr:.0f}%  PnL={g_pnl.sum():+.2f} USD"
                )

        # Daily P&L breakdown
        if "close_time" in df.columns:
            try:
                import pandas as pd
                df2 = df.copy()
                df2["close_date"] = pd.to_datetime(
                    df2["close_time"], errors="coerce"
                ).dt.date
                daily = df2.groupby("close_date")["profit_loss"].sum()
                if not daily.empty:
                    lines.append(f"  Daily P&L:")
                    for day, day_pnl in daily.items():
                        marker = "✅" if day_pnl > 0 else "❌"
                        lines.append(f"    {day}  {day_pnl:+.2f} USD  {marker}")
            except Exception:
                pass

    if not any_pnl_data:
        lines.append(
            "\n  No trades.csv data found for any instance. "
            "Trades will appear here once the integrated tracker journals closes."
        )

    lines.append(f"\n{SEP}")
    lines.append("  END OF REPORT")
    lines.append(SEP)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="CTP health check — multi-instance signal and guard audit."
    )
    parser.add_argument(
        "--instance", "-i",
        nargs="+",
        default=None,
        help="Instance ID(s) to analyse (default: all 4).",
    )
    parser.add_argument(
        "--dates",
        nargs="+",
        default=None,
        help=(
            "Date(s) YYYY-MM-DD to analyse "
            "(default: last 7 calendar days)."
        ),
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Include per-event detail in risk rejections and infra errors.",
    )
    args = parser.parse_args()

    instances = args.instance or ALL_INSTANCES
    dates     = args.dates     or _default_dates()

    print(f"CTP Health Check")
    print(f"Instances : {instances}")
    print(f"Dates     : {dates[0]} → {dates[-1]}")
    print(f"Verbose   : {args.verbose}")
    print()

    all_stats: Dict[str, InstanceStats] = {}

    for inst in instances:
        stats = InstanceStats(instance_id=inst)
        files_found = 0

        for d in dates:
            log_path = LOGS_DIR / f"{_LOG_PREFIX}_{inst}_{d}.log"
            if not log_path.exists():
                if args.verbose:
                    print(f"  [{inst}] {d}: log not found — skipped")
                continue
            files_found += 1
            print(f"  [{inst}] {d}: parsing {log_path.name} …")
            _parse_log_file(log_path, stats, args.verbose)

        if files_found == 0:
            print(f"  [{inst}] No log files found for date range.")

        all_stats[inst] = stats

    print()
    journal_info = _check_journals(instances)
    report = _build_report(all_stats, journal_info, dates, args.verbose)

    print(report)

    DIAG_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_path = DIAG_DIR / f"health_check_{today}.txt"
    out_path.write_text(report, encoding="utf-8")
    print(f"\nReport saved: {out_path}")


if __name__ == "__main__":
    main()