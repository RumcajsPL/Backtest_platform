"""
PaperTradingGuard — runtime safeguards for the live paper trading loop.

Owns all session-level state and enforces every configurable circuit breaker.
Called by run_signal_loop.py before each order attempt and after each trade result.

Safeguards enforced:
  - Kill switch file (STOP in project root) — checked every poll
  - Max consecutive losses — hard stop or pause until next session open
  - Daily drawdown % of session-open credit — CTP journal-scoped only
  - Minimum available cash — hard stop
  - Consecutive pipeline error streak — hard stop

Daily drawdown is scoped exclusively to CTP-placed trades recorded in the
journal (trades.csv).  External positions on the same demo account (manual
trades, other strategies) are invisible to this guard by design.  The caller
passes the sum of today's closed CTP P&L; this guard never reads the raw
account credit for the drawdown calculation.

Daily state (consecutive_losses, drawdown baseline) resets automatically
when the UTC date advances to a new trading day.

Session resume after pause: first hour in trading_window.allowed_hours_utc
on the next calendar day (UTC).  This reuses the existing YAML config and
requires no new config fields.

Design rules:
  - Never calls sys.exit() — raises HaltLoopError or PauseUntilTomorrowError.
    The loop decides how to exit.
  - Never implements HTTP — receives values from caller who already fetched
    the portfolio (no duplicate API calls).
  - All state is in-memory; no persistence across process restarts.
    On restart, daily counters reconstruct from CSVJournal (consecutive losses
    and today's CTP P&L sum) and a fresh portfolio fetch (session-open credit).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

from loguru import logger

from src.broker_support.config.broker_support_config import BrokerSupportConfig


# ---------------------------------------------------------------------------
# Sentinel exceptions — never inherit from SystemExit
# ---------------------------------------------------------------------------

class HaltLoopError(Exception):
    """
    Raised when a safeguard triggers a permanent stop of the trading loop.
    The loop logs the reason and exits cleanly (sys.exit(0)).
    """


class PauseUntilTomorrowError(Exception):
    """
    Raised when a safeguard triggers a pause until the next session open.
    Carries the UTC datetime at which the loop should resume.

    Attributes:
        resume_at: UTC datetime of next allowed session open.
    """
    def __init__(self, reason: str, resume_at: datetime) -> None:
        super().__init__(reason)
        self.resume_at = resume_at


# ---------------------------------------------------------------------------
# Guard
# ---------------------------------------------------------------------------

@dataclass
class PaperTradingGuard:
    """
    Runtime safeguard state for the paper trading loop.

    Instantiate once at loop startup. Call reset_daily_state() at startup
    and whenever the UTC date advances past session_date.

    Args:
        config:              Full BrokerSupportConfig (reads safety + trading_window).
        session_open_credit: Portfolio credit at the start of this session (USD).
                             Kept for status logging and min_cash checks only —
                             NOT used as drawdown baseline (drawdown is CTP-scoped).
        journal_trades_today: Closed CTP trade P&L values already journalled today,
                              in chronological order.  Used to reconstruct
                              consecutive_losses on restart.
    """
    config: BrokerSupportConfig
    session_open_credit: float
    journal_trades_today: List[float] = field(default_factory=list)

    # mutable session state
    consecutive_losses: int = field(default=0, init=False)
    pipeline_error_streak: int = field(default=0, init=False)
    session_date: date = field(
        default_factory=lambda: datetime.now(timezone.utc).date(), init=False
    )
    _paused_until: Optional[datetime] = field(default=None, init=False)

    def __post_init__(self) -> None:
        self.consecutive_losses = self._count_tail_losses(self.journal_trades_today)
        logger.info(
            f"PaperTradingGuard initialised: "
            f"session_open_credit={self.session_open_credit:.2f}, "
            f"consecutive_losses={self.consecutive_losses} (from journal), "
            f"max_consecutive_losses={self.config.safety.max_consecutive_losses}, "
            f"consecutive_loss_action={self.config.safety.consecutive_loss_action}, "
            f"max_daily_drawdown_pct={self.config.safety.max_daily_drawdown_pct:.1f}% "
            f"(CTP journal-scoped — external account activity ignored), "
            f"min_cash={self.config.safety.min_available_cash_usd:.2f}, "
            f"max_pipeline_errors={self.config.safety.max_pipeline_errors}, "
            f"kill_switch_file='{self.config.safety.kill_switch_file}'"
        )

    # ------------------------------------------------------------------
    # Daily state management
    # ------------------------------------------------------------------

    def reset_daily_state(self, new_session_open_credit: float) -> None:
        """
        Reset all daily counters for a new trading session.

        Called:
          - At loop startup (after initial portfolio fetch).
          - When the loop detects UTC date has advanced past session_date.

        Args:
            new_session_open_credit: Current portfolio credit (kept for
                                     status logging and min_cash checks).
        """
        prev_date = self.session_date
        self.session_date = datetime.now(timezone.utc).date()
        self.consecutive_losses = 0
        self.pipeline_error_streak = 0
        self.session_open_credit = new_session_open_credit
        self._paused_until = None
        logger.info(
            f"PaperTradingGuard: daily state reset "
            f"(prev_date={prev_date}, new_date={self.session_date}, "
            f"session_open_credit={new_session_open_credit:.2f})"
        )

    def check_date_rollover(self, current_credit: float) -> bool:
        """
        Check whether the UTC date has advanced past session_date.
        If so, reset daily state and return True.

        Args:
            current_credit: Current portfolio credit for the new baseline.

        Returns:
            True if a date rollover occurred and state was reset.
        """
        today = datetime.now(timezone.utc).date()
        if today > self.session_date:
            logger.info(
                f"PaperTradingGuard: date rollover detected "
                f"({self.session_date} → {today}). Resetting daily state."
            )
            self.reset_daily_state(new_session_open_credit=current_credit)
            return True
        return False

    # ------------------------------------------------------------------
    # Per-poll checks — call in this order every poll iteration
    # ------------------------------------------------------------------

    def check_kill_switch(self) -> None:
        """
        Check for STOP file in project root.

        Raises:
            HaltLoopError: if the kill switch file exists.
        """
        kill_path = Path(self.config.safety.kill_switch_file)
        if kill_path.exists():
            raise HaltLoopError(
                f"Kill switch file '{kill_path}' detected. "
                f"Loop halted. Remove the file before restarting."
            )

    def check_daily_drawdown(self, ctp_realised_pnl_today: float) -> None:
        """
        Halt if CTP's own realised P&L today breaches max_daily_drawdown_pct.

        Drawdown is scoped exclusively to trades placed by this CTP loop and
        recorded in the journal (trades.csv).  External positions on the same
        demo account — manual trades, other strategies — are invisible to this
        check by design.

        Drawdown formula:
            drawdown_pct = -ctp_realised_pnl_today / session_open_credit * 100

        A negative ctp_realised_pnl_today (net loss) produces a positive
        drawdown_pct.  A positive value (net profit) produces 0% drawdown
        (clamped — profits don't offset the limit).

        Only triggers if session_open_credit > 0.

        Args:
            ctp_realised_pnl_today: Sum of profit_loss for all CTP trades
                                    closed today (from journal). Negative = loss.

        Raises:
            HaltLoopError: if CTP drawdown threshold is breached.
        """
        if self.session_open_credit <= 0:
            return

        # Only losses count toward drawdown — clamp at 0
        drawdown_pct = max(
            0.0,
            -ctp_realised_pnl_today / self.session_open_credit * 100.0,
        )
        limit = self.config.safety.max_daily_drawdown_pct

        if drawdown_pct >= limit:
            raise HaltLoopError(
                f"CTP daily drawdown limit reached: {drawdown_pct:.2f}% >= {limit:.1f}% "
                f"(session_open_credit={self.session_open_credit:.2f}, "
                f"ctp_realised_pnl_today={ctp_realised_pnl_today:+.2f}). "
                f"Loop halted for today. External account activity excluded."
            )
        logger.debug(
            f"CTP drawdown check: {drawdown_pct:.2f}% / {limit:.1f}% limit OK "
            f"(ctp_realised_pnl_today={ctp_realised_pnl_today:+.2f}, "
            f"session_open_credit={self.session_open_credit:.2f})"
        )

    def check_min_cash(self, current_credit: float) -> None:
        """
        Halt if available cash falls below the configured minimum.

        Uses raw account credit — this is a capital preservation floor,
        not a strategy-scoped metric.

        Args:
            current_credit: Current portfolio credit.

        Raises:
            HaltLoopError: if credit < min_available_cash_usd.
        """
        minimum = self.config.safety.min_available_cash_usd
        if current_credit < minimum:
            raise HaltLoopError(
                f"Available cash too low: {current_credit:.2f} < {minimum:.2f} USD minimum. "
                f"Loop halted to protect capital."
            )

    def check_consecutive_losses(self) -> None:
        """
        Enforce max_consecutive_losses circuit breaker.

        Action depends on consecutive_loss_action:
          'hard_stop'            → HaltLoopError (loop exits permanently)
          'pause_until_next_day' → PauseUntilTomorrowError (loop sleeps until
                                   first allowed_hours_utc hour next calendar day)

        Raises:
            HaltLoopError:           if action=hard_stop and limit reached.
            PauseUntilTomorrowError: if action=pause_until_next_day and limit reached.
        """
        limit = self.config.safety.max_consecutive_losses
        if self.consecutive_losses < limit:
            return

        reason = (
            f"Consecutive loss limit reached: {self.consecutive_losses}/{limit}."
        )
        action = self.config.safety.consecutive_loss_action

        if action == "hard_stop":
            raise HaltLoopError(f"{reason} Action=hard_stop. Loop halted.")

        resume_at = self._next_session_open()
        raise PauseUntilTomorrowError(
            f"{reason} Action=pause_until_next_day. "
            f"Resuming at {resume_at.strftime('%Y-%m-%d %H:%M UTC')}.",
            resume_at=resume_at,
        )

    # ------------------------------------------------------------------
    # Pipeline error tracking
    # ------------------------------------------------------------------

    def record_pipeline_error(self) -> None:
        """
        Record one consecutive pipeline failure.

        Raises:
            HaltLoopError: if streak reaches max_pipeline_errors.
        """
        self.pipeline_error_streak += 1
        limit = self.config.safety.max_pipeline_errors
        logger.warning(
            f"Pipeline error streak: {self.pipeline_error_streak}/{limit}"
        )
        if self.pipeline_error_streak >= limit:
            raise HaltLoopError(
                f"Pipeline error limit reached: {self.pipeline_error_streak} consecutive "
                f"failures. Loop halted — investigate infrastructure before restarting."
            )

    def reset_pipeline_error_streak(self) -> None:
        """Reset pipeline error streak on a successful run."""
        if self.pipeline_error_streak > 0:
            logger.debug(
                f"Pipeline error streak reset (was {self.pipeline_error_streak})."
            )
        self.pipeline_error_streak = 0

    # ------------------------------------------------------------------
    # Trade result recording
    # ------------------------------------------------------------------

    def record_trade_result(self, pnl_usd: float) -> None:
        """
        Update consecutive loss counter after a trade closes.

        A win (pnl_usd > 0) resets the counter.
        A loss (pnl_usd <= 0) increments it.

        Args:
            pnl_usd: Net profit/loss in USD for the closed trade.
        """
        if pnl_usd > 0:
            if self.consecutive_losses > 0:
                logger.info(
                    f"Trade result: WIN ({pnl_usd:+.2f} USD). "
                    f"Consecutive loss streak reset (was {self.consecutive_losses})."
                )
            else:
                logger.info(f"Trade result: WIN ({pnl_usd:+.2f} USD).")
            self.consecutive_losses = 0
        else:
            self.consecutive_losses += 1
            logger.info(
                f"Trade result: LOSS ({pnl_usd:+.2f} USD). "
                f"Consecutive losses: {self.consecutive_losses}/"
                f"{self.config.safety.max_consecutive_losses}."
            )

    # ------------------------------------------------------------------
    # Status summary (for logging)
    # ------------------------------------------------------------------

    def status_summary(self) -> str:
        """One-line status string for periodic loop logging."""
        return (
            f"Guard: consecutive_losses={self.consecutive_losses}/"
            f"{self.config.safety.max_consecutive_losses} | "
            f"pipeline_errors={self.pipeline_error_streak}/"
            f"{self.config.safety.max_pipeline_errors} | "
            f"session_open_credit={self.session_open_credit:.2f}"
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _next_session_open(self) -> datetime:
        """
        Compute the UTC datetime of the next session open.

        Next session open = first hour in trading_window.allowed_hours_utc
        on the next calendar day (UTC).
        """
        allowed = sorted(self.config.trading_window.allowed_hours_utc)
        first_hour = allowed[0] if allowed else 9

        now_utc = datetime.now(timezone.utc)
        next_day = (now_utc + timedelta(days=1)).date()
        return datetime(
            next_day.year, next_day.month, next_day.day,
            first_hour, 0, 0,
            tzinfo=timezone.utc,
        )

    @staticmethod
    def _count_tail_losses(pnl_list: List[float]) -> int:
        """
        Count consecutive losses at the tail of a P&L list.

        Used to reconstruct the in-memory streak from today's journal on restart.

        Args:
            pnl_list: List of P&L values in chronological order.

        Returns:
            Number of consecutive losses at the end of the list.
        """
        count = 0
        for pnl in reversed(pnl_list):
            if pnl <= 0:
                count += 1
            else:
                break
        return count