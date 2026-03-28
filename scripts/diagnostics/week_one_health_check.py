#!/usr/bin/env python
"""
week_one_health_check.py — CTP week-one full health check.

Performs a single-pass analysis of all instance log files and produces
a structured report covering:

  1. Signal pipeline summary per instance
       - Total polls, signals found, RISK_REJECTED, NO_SIGNAL, WBWS+ blocked
       - Pass-through rate at each pipeline stage
       - Risk rejection reasons (from RiskManager.get_risk_summary() log lines)

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

  6. Journal gap diagnosis
       - Reports missing trades.csv per instance
       - Explains why (tracker loop not instance-aware)
       - Lists unconfirmed orderIDs that require manual trade history lookup

Usage:
    python scripts/diagnostics/week_one_health_check.py
    python scripts/diagnostics/week_one_health_check.py --instance 240166
    python scripts/diagnostics/week_one_health_check.py --dates 2026-03-24 2026-03-27
    python scripts/diagnostics/week_one_health_check.py --verbose

Output:
    Console report (always)
    outputs/broker_support/diagnostics/week_one_health_check_YYYY-MM-DD.txt (always)
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

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
WEEK_DATES = ["2026-03-24", "2026-03-25", "2026-03-26", "2026-03-27"]

LOGS_DIR      = _PROJECT_ROOT / "outputs" / "broker_support" / "logs"
JOURNAL_DIR   = _PROJECT_ROOT / "outputs" / "broker_support" / "journal"
DIAG_DIR      = _PROJECT_ROOT / "outputs" / "broker_support" / "diagnostics"

# Log line patterns
_RE_TIMESTAMP   = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
_RE_LEVEL       = re.compile(r"\|\s*(DEBUG|INFO|WARNING|ERROR)\s*\|")
_RE_POLL        = re.compile(r"Poll #(\d+) at (\d{2}:\d{2}:\d{2} UTC)")
_RE_NO_SIGNAL   = re.compile(r"result=NO_SIGNAL")
_RE_RISK_REJ    = re.compile(r"result=RISK_REJECTED")
_RE_RISK_SUM    = re.compile(r"Risk summary: (.+)")
_RE_RAW_SIGS    = re.compile(r"Raw signals: buy=(\d+), sell=(\d+), total=(\d+)")
_RE_AFTER_FILT  = re.compile(r"After filters: (\d+) → (\d+) signals \(pass_rate=([\d.]+)%\)")
_RE_SIGNAL_FOUND= re.compile(r"SIGNAL FOUND:")
_RE_SIGNAL_LINE = re.compile(r"OrderSignal \| (BUY|SELL) (\w+) \| ts=([^\|]+) \|")
_RE_WBWS_BLOCK  = re.compile(r"Signal outside WBWS\+ window")
_RE_WBWS_OPEN   = re.compile(r"WBWS\+ window.*✅ OPEN")
_RE_ORDER_PLACED= re.compile(r"ORDER PLACED #(\d+)")
_RE_POSITION_ID = re.compile(r"positionID\s*:\s*(\d+)")
_RE_ORDER_FAILED= re.compile(r"ORDER FAILED: Portfolio scan: positionID for orderID=(\d+)")
_RE_ORDER_BLOCKED=re.compile(r"ORDER BLOCKED")
_RE_PYRAMID_BLOCK=re.compile(r"CTP max positions reached \((\d+)/(\d+)\)")
_RE_PYRAMID_CHECK=re.compile(r"Pyramiding check: (\d+) CTP position\(s\)")
_RE_HALT        = re.compile(r"HALT \[[\w]+\]: (.+)")
_RE_PAUSE       = re.compile(r"PAUSE: (.+)")
_RE_PIPELINE_ERR= re.compile(r"Pipeline error streak: (\d+)/(\d+)")
_RE_PORT_FETCH_ERR=re.compile(r"Portfolio fetch error: (.+)")
_RE_429         = re.compile(r"API error 429")
_RE_503         = re.compile(r"API error 503")
_RE_502         = re.compile(r"API error 502")
_RE_409         = re.compile(r"API error 409")
_RE_TIMEOUT     = re.compile(r"Read timed out|Connection aborted|ConnectionResetError")
_RE_CONSEC_LOSS = re.compile(r"consecutive_losses=(\d+)/(\d+)")
_RE_GUARD_STATUS= re.compile(r"Guard: consecutive_losses=(\d+)/(\d+) \| pipeline_errors=(\d+)/(\d+)")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class SignalEvent:
    timestamp: str
    direction: str
    symbol: str
    outcome: str  # PLACED | WBWS_BLOCKED | ORDER_FAILED | (should not happen)


@dataclass
class RejectionEvent:
    timestamp: str
    risk_summary: str  # populated if next log line has Risk summary


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
    error_type: str  # 429 | 503 | 502 | 409 | TIMEOUT | CONN_RESET
    context: str


@dataclass
class InstanceStats:
    instance_id: str
    dates_found: list[str] = field(default_factory=list)

    # Poll counts
    total_polls: int = 0

    # Pipeline stage counts
    total_raw_signals: int = 0
    total_after_filter: int = 0
    no_signal_polls: int = 0
    risk_rejected_count: int = 0
    risk_rejection_events: list[RejectionEvent] = field(default_factory=list)

    # Signal → order stage
    signals_found: int = 0
    wbws_blocked: int = 0
    orders_placed: int = 0
    orders_failed: list[OrderFailedEvent] = field(default_factory=list)
    position_ids_confirmed: list[int] = field(default_factory=list)

    # Guard events
    halt_events: list[HaltEvent] = field(default_factory=list)
    pause_events: list[HaltEvent] = field(default_factory=list)
    pyramid_blocks: int = 0
    pipeline_error_streaks: list[tuple[int,int]] = field(default_factory=list)
    portfolio_fetch_errors: int = 0

    # Infrastructure errors
    infra_errors: list[InfraErrorEvent] = field(default_factory=list)

    # Max consecutive losses seen
    max_consecutive_losses_seen: int = 0


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

def _parse_log_file(path: Path, stats: InstanceStats, verbose: bool) -> None:
    """Single-pass parse of one log file into InstanceStats."""
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    stats.dates_found.append(path.stem.split("_")[-1])  # YYYY-MM-DD

    pending_risk_rejection: Optional[RejectionEvent] = None
    pending_signal_found: bool = False

    for i, line in enumerate(lines):
        ts_match = _RE_TIMESTAMP.match(line)
        ts = ts_match.group(1) if ts_match else "unknown"

        # ── Poll counter ─────────────────────────────────────────────────
        if _RE_POLL.search(line):
            stats.total_polls += 1

        # ── Raw signal counts (per pipeline run) ──────────────────────────
        m = _RE_RAW_SIGS.search(line)
        if m:
            stats.total_raw_signals += int(m.group(3))

        # ── After filter counts ───────────────────────────────────────────
        m = _RE_AFTER_FILT.search(line)
        if m:
            stats.total_after_filter += int(m.group(2))

        # ── No signal ─────────────────────────────────────────────────────
        if _RE_NO_SIGNAL.search(line):
            stats.no_signal_polls += 1
            pending_signal_found = False

        # ── Risk rejected ─────────────────────────────────────────────────
        if _RE_RISK_REJ.search(line):
            stats.risk_rejected_count += 1
            pending_risk_rejection = RejectionEvent(timestamp=ts, risk_summary="")
            pending_signal_found = False

        # ── Risk summary (follows RISK_REJECTED on next line) ─────────────
        m = _RE_RISK_SUM.search(line)
        if m and pending_risk_rejection:
            pending_risk_rejection.risk_summary = m.group(1).strip()
            stats.risk_rejection_events.append(pending_risk_rejection)
            pending_risk_rejection = None

        # ── Signal found ──────────────────────────────────────────────────
        if _RE_SIGNAL_FOUND.search(line):
            stats.signals_found += 1
            pending_signal_found = True

        # ── WBWS+ blocked ─────────────────────────────────────────────────
        if _RE_WBWS_BLOCK.search(line):
            stats.wbws_blocked += 1
            pending_signal_found = False

        # ── Order placed ──────────────────────────────────────────────────
        if _RE_ORDER_PLACED.search(line):
            stats.orders_placed += 1
            pending_signal_found = False

        # ── positionID confirmed ──────────────────────────────────────────
        m = _RE_POSITION_ID.search(line)
        if m and "ORDER PLACED" in "".join(lines[max(0,i-5):i+1]):
            pid = int(m.group(1))
            if pid not in stats.position_ids_confirmed:
                stats.position_ids_confirmed.append(pid)

        # ── ORDER FAILED ──────────────────────────────────────────────────
        m = _RE_ORDER_FAILED.search(line)
        if m:
            stats.orders_failed.append(
                OrderFailedEvent(timestamp=ts, order_id=int(m.group(1)))
            )
            pending_signal_found = False

        # ── Pyramiding block ──────────────────────────────────────────────
        if _RE_PYRAMID_BLOCK.search(line):
            stats.pyramid_blocks += 1

        # ── Pipeline error streak ─────────────────────────────────────────
        m = _RE_PIPELINE_ERR.search(line)
        if m:
            stats.pipeline_error_streaks.append((int(m.group(1)), int(m.group(2))))

        # ── Portfolio fetch errors ────────────────────────────────────────
        if _RE_PORT_FETCH_ERR.search(line):
            stats.portfolio_fetch_errors += 1

        # ── HALT / PAUSE events ───────────────────────────────────────────
        m = _RE_HALT.search(line)
        if m:
            stats.halt_events.append(HaltEvent(timestamp=ts, reason=m.group(1).strip()))

        m = _RE_PAUSE.search(line)
        if m and "WBWS" not in line:  # exclude WBWS+ window log lines
            stats.pause_events.append(HaltEvent(timestamp=ts, reason=m.group(1).strip()))

        # ── Infrastructure errors ─────────────────────────────────────────
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

        # ── Consecutive losses tracker ────────────────────────────────────
        m = _RE_GUARD_STATUS.search(line)
        if m:
            cl = int(m.group(1))
            if cl > stats.max_consecutive_losses_seen:
                stats.max_consecutive_losses_seen = cl


# ---------------------------------------------------------------------------
# Journal check
# ---------------------------------------------------------------------------

def _check_journals(instances: list[str]) -> dict[str, dict]:
    """Check journal state for each instance."""
    results = {}
    for inst in instances:
        inst_dir = JOURNAL_DIR / inst
        trades_csv = inst_dir / "trades.csv"
        open_pos   = inst_dir / "open_positions.json"

        import json
        open_ids = []
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
                trade_count = -1  # unreadable

        results[inst] = {
            "trades_csv_exists": trades_csv.exists(),
            "trade_count": trade_count,
            "open_position_ids": open_ids,
        }
    return results


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------

SEP  = "=" * 80
SEP2 = "-" * 60

def _fmt_section(title: str) -> str:
    return f"\n{SEP}\n  {title}\n{SEP}"

def _fmt_sub(title: str) -> str:
    return f"\n{SEP2}\n  {title}\n{SEP2}"


def _build_report(
    all_stats: dict[str, InstanceStats],
    journal_info: dict[str, dict],
    verbose: bool,
) -> str:
    lines = []
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines.append(f"\nCTP WEEK-ONE HEALTH CHECK  —  generated {now}")
    lines.append(f"Instances: {', '.join(all_stats.keys())}")
    lines.append(f"Dates analysed: {WEEK_DATES[0]} → {WEEK_DATES[-1]}\n")

    # ── Section 1: Signal pipeline summary ───────────────────────────────
    lines.append(_fmt_section("1. SIGNAL PIPELINE SUMMARY"))

    for inst, s in all_stats.items():
        if s.total_polls == 0:
            lines.append(f"\n[{inst}]  No log files found — instance may not have run.")
            continue

        signal_pipeline_runs = s.no_signal_polls + s.risk_rejected_count + s.signals_found
        risk_rej_pct = (
            s.risk_rejected_count / signal_pipeline_runs * 100
            if signal_pipeline_runs > 0 else 0.0
        )
        lines.append(f"\n[{inst}]")
        lines.append(f"  Log dates found    : {', '.join(s.dates_found)}")
        lines.append(f"  Total polls        : {s.total_polls}")
        lines.append(f"  Pipeline runs      : {signal_pipeline_runs}  "
                     f"(polls where signal pipeline executed)")
        lines.append(f"  NO_SIGNAL          : {s.no_signal_polls}  "
                     f"(last bar had no filtered signal)")
        lines.append(f"  RISK_REJECTED      : {s.risk_rejected_count}  "
                     f"({risk_rej_pct:.1f}% of pipeline runs)")
        lines.append(f"  SIGNAL FOUND       : {s.signals_found}  "
                     f"(passed RiskManager)")
        lines.append(f"  WBWS+ blocked      : {s.wbws_blocked}  "
                     f"(signal valid but hour outside window)")
        lines.append(f"  Orders placed      : {s.orders_placed}")
        lines.append(f"  ORDER FAILED       : {len(s.orders_failed)}  "
                     f"(portfolio scan timeout — position status UNKNOWN)")
        lines.append(f"  Pyramid blocks     : {s.pyramid_blocks}  "
                     f"(correctly idled while CTP position open)")
        lines.append(f"  Max consec. losses : {s.max_consecutive_losses_seen}")

        if s.risk_rejection_events and verbose:
            lines.append(f"\n  Risk rejection details:")
            seen_summaries: dict[str, int] = defaultdict(int)
            for ev in s.risk_rejection_events:
                seen_summaries[ev.risk_summary] += 1
            for summary, count in sorted(seen_summaries.items(),
                                         key=lambda x: -x[1]):
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
                lines.append(f"    {ev.timestamp}  orderID={ev.order_id}  "
                             f"← verify via trade history or inspect_portfolio.py")
        else:
            lines.append(f"  Unconfirmed orders    : none")

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

        lines.append(f"  Portfolio fetch errors : {s.portfolio_fetch_errors}  "
                     f"(connectivity — do NOT count vs pipeline error budget)")

        if s.pipeline_error_streaks:
            max_streak = max(v for v, _ in s.pipeline_error_streaks)
            lines.append(f"  Pipeline error streaks: max={max_streak}  "
                         f"(signal pipeline only — threshold=5)")
            if max_streak >= 5:
                lines.append(f"    ⚠️  Max streak reached threshold — loop halted. "
                             f"See HALT events above.")
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

        by_type: dict[str, list] = defaultdict(list)
        for ev in s.infra_errors:
            by_type[ev.error_type].append(ev.timestamp)

        lines.append(f"\n[{inst}]  {len(s.infra_errors)} infrastructure error(s):")
        for err_type, timestamps in sorted(by_type.items()):
            lines.append(f"  {err_type:<30} × {len(timestamps)}")
            if verbose:
                for ts in timestamps:
                    lines.append(f"    {ts}")

    # ── Section 5: Journal gap diagnosis ──────────────────────────────────
    lines.append(_fmt_section("5. JOURNAL GAP DIAGNOSIS"))
    lines.append("")
    lines.append("  Root cause: run_tracker_loop.py has no --instance flag.")
    lines.append("  It writes to a hardcoded legacy path:")
    lines.append("    outputs/broker_support/journal/trades.csv  (root, no instance subdir)")
    lines.append("  CTP signal loops write to instance-scoped paths:")
    lines.append("    outputs/broker_support/journal/<instance>/trades.csv")
    lines.append("  These paths never matched → no closed trades were journalled")
    lines.append("  for any instance this week, regardless of whether trades closed.")
    lines.append("")
    lines.append("  Consequence for safeguards:")
    lines.append("    _load_todays_ctp_pnl() returns ([], 0.0) at every restart.")
    lines.append("    consecutive_losses always reconstructs as 0.")
    lines.append("    check_daily_drawdown() always sees 0 loss — guard is blind.")
    lines.append("  This is a V2 backlog item — run_tracker_loop.py needs --instance.")
    lines.append("")

    for inst, info in journal_info.items():
        lines.append(f"  [{inst}]")
        if info["trades_csv_exists"]:
            lines.append(f"    trades.csv        : EXISTS  ({info['trade_count']} trades)")
        else:
            lines.append(f"    trades.csv        : MISSING  ← no closed trades recorded")
        if info["open_position_ids"]:
            lines.append(f"    open_positions    : {info['open_position_ids']}")
            lines.append(f"    ⚠️  Position(s) still marked open in open_positions.json.")
            lines.append(f"       Verify via inspect_portfolio.py --instance {inst}")
            lines.append(f"       If already closed on eToro: remove from file manually")
            lines.append(f"       before restarting loop, or reconciliation will idle.")
        else:
            lines.append(f"    open_positions    : empty")

    # ── Section 6: Actions required before next week ──────────────────────
    lines.append(_fmt_section("6. ACTIONS REQUIRED BEFORE RESTARTING"))
    lines.append("")

    # Collect all unconfirmed order IDs across instances
    all_unconfirmed = []
    for inst, s in all_stats.items():
        for ev in s.orders_failed:
            all_unconfirmed.append((inst, ev.order_id, ev.timestamp))

    if all_unconfirmed:
        lines.append("  A. Verify unconfirmed orders in eToro trade history:")
        lines.append("     These orders had statusID=1 (accepted by broker) but")
        lines.append("     positionID resolution timed out. May or may not have opened.")
        for inst, oid, ts in all_unconfirmed:
            lines.append(f"       [{inst}]  orderID={oid}  at {ts}")
        lines.append("     Command: python scripts/broker_support/inspect_portfolio.py")
        lines.append("     Also check eToro demo account trade history directly.")
        lines.append("")

    lines.append("  B. Deploy fixed files from 2026-03-28 session:")
    lines.append("     scripts/broker_support/run_signal_loop.py")
    lines.append("       → pending-order reconciliation (prevents constraint breach)")
    lines.append("       → portfolio fetch errors no longer count vs pipeline budget")
    lines.append("     src/broker_support/live/live_data_fetcher.py")
    lines.append("       → pandas ISO8601 format fix (eliminates UserWarning)")
    lines.append("     scripts/broker_support/inspect_portfolio.py")
    lines.append("       → --instance and --all-positions flags added")
    lines.append("")

    lines.append("  C. Verify open_positions.json state before restarting each loop:")
    for inst, info in journal_info.items():
        if info["open_position_ids"]:
            lines.append(f"     [{inst}]  position_ids={info['open_position_ids']}")
            lines.append(f"       Check if still open: python scripts/broker_support/"
                        f"inspect_portfolio.py --instance {inst}")
            lines.append(f"       If closed: clear open_positions.json manually")
    lines.append("")

    lines.append("  D. run_tracker_loop.py needs --instance flag (V2 backlog):")
    lines.append("     Until fixed, drawdown guard and consecutive loss reconstruction")
    lines.append("     from journal are non-functional for all instances.")
    lines.append("     Workaround for now: guards still work in-memory within a session;")
    lines.append("     only cross-restart reconstruction is broken.")
    lines.append("")

    lines.append("  E. RiskManager calibration:")
    lines.append("     Only 1 week of live data — insufficient for 0.45% threshold review.")
    lines.append("     Continue for another full week before adjusting.")

    lines.append(f"\n{SEP}")
    lines.append("  END OF REPORT")
    lines.append(SEP)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="CTP week-one health check — full signal and guard audit."
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
        help="Date(s) YYYY-MM-DD to analyse (default: 2026-03-24 to 2026-03-27).",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Include per-event detail in risk rejections and infra errors.",
    )
    args = parser.parse_args()

    instances = args.instance or ALL_INSTANCES
    dates     = args.dates     or WEEK_DATES

    print(f"CTP Week-One Health Check")
    print(f"Instances : {instances}")
    print(f"Dates     : {dates}")
    print(f"Verbose   : {args.verbose}")
    print()

    all_stats: dict[str, InstanceStats] = {}

    for inst in instances:
        stats = InstanceStats(instance_id=inst)
        files_found = 0

        for d in dates:
            log_path = LOGS_DIR / f"run_signal_loop_{inst}_{d}.log"
            if not log_path.exists():
                print(f"  [{inst}] {d}: log not found — skipped")
                continue
            files_found += 1
            print(f"  [{inst}] {d}: parsing {log_path.name} …")
            _parse_log_file(log_path, stats, args.verbose)

        if files_found == 0:
            print(f"  [{inst}] No log files found.")

        all_stats[inst] = stats

    print()
    journal_info = _check_journals(instances)

    report = _build_report(all_stats, journal_info, args.verbose)

    # Print to console
    print(report)

    # Write to file
    DIAG_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_path = DIAG_DIR / f"week_one_health_check_{today}.txt"
    out_path.write_text(report, encoding="utf-8")
    print(f"\nReport saved: {out_path}")


if __name__ == "__main__":
    main()