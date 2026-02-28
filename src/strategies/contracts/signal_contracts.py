"""Signal Layer Contracts for WBWSStrategy Migration.
Version: 2.3.0
Defines the data structures used for representing trading signals and their associated metadata in both core and analytics modes.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, Iterator, Optional, Tuple

import numpy as np
import pandas as pd

# =============================================================================
# SIGNAL TYPE ENUM
# =============================================================================

class SignalType(Enum):
    """Typed replacement for string-based "BUY"/"SELL" communication."""

    BUY = auto()
    SELL = auto()

    def __str__(self) -> str:  # noqa: D105
        return self.name

    @classmethod
    def from_string(cls, s: str) -> Optional["SignalType"]:
        """Convert string to SignalType (case-insensitive)."""
        s_upper = (s or "").upper()
        if s_upper == "BUY":
            return cls.BUY
        if s_upper == "SELL":
            return cls.SELL
        return None

    @classmethod
    def from_code(cls, code: int) -> Optional["SignalType"]:
        """Convert int8 code to SignalType (1=BUY, 2=SELL)."""
        if code == 1:
            return cls.BUY
        if code == 2:
            return cls.SELL
        return None

    @property
    def is_long(self) -> bool:
        """True for BUY signals."""
        return self == SignalType.BUY

    @property
    def is_short(self) -> bool:
        """True for SELL signals."""
        return self == SignalType.SELL

# =============================================================================
# SIGNAL (single point-in-time)
# =============================================================================

@dataclass(frozen=True)
class Signal:
    """Single trading signal at a specific timestamp.

    ``mid_price`` must be positive; use ``Signal.mid_price > 0`` as a guard
    before arithmetic.  ``metadata`` defaults to an empty dict.
    """

    timestamp: pd.Timestamp
    signal_type: SignalType
    mid_price: float
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.mid_price <= 0:
            raise ValueError(f"mid_price must be positive, got {self.mid_price}")

    def __str__(self) -> str:  # noqa: D105
        return f"{self.signal_type} @ {self.timestamp} (price: {self.mid_price:.2f})"

    @property
    def is_long(self) -> bool:
        """True for BUY / LONG signals."""
        return self.signal_type.is_long

    @property
    def is_short(self) -> bool:
        """True for SELL / SHORT signals."""
        return self.signal_type.is_short

# =============================================================================
# SIGNAL FRAME
# =============================================================================

@dataclass(frozen=True)
class SignalFrame:
    """Collection of signals with associated indicator data.

    Storage
    -------
    ``signals`` is a ``pd.Series`` with ``dtype=int8``:
    * 0 = no signal
    * 1 = BUY
    * 2 = SELL

    ``indicator_data`` is present only in **analytics** mode; it is ``None``
    in **core** mode.  This is the authoritative flag for which mode produced
    the frame.
    """

    signals: pd.Series          # int8: 1=BUY, 2=SELL, 0=no signal; DatetimeIndex
    indicator_data: Optional[pd.DataFrame] = None
    signal_metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.signals.index, pd.DatetimeIndex):
            raise ValueError("SignalFrame.signals must have a DatetimeIndex.")

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    @classmethod
    def from_wbws_trigger(
        cls,
        signals_df: pd.DataFrame,
        strategy_df: pd.DataFrame,
        include_metadata: bool = True,
    ) -> "SignalFrame":
        """Create a SignalFrame from WBWSTrigger output.

        Parameters
        ----------
        signals_df:
            DataFrame with boolean columns ``we_buy`` and ``we_sell``.
        strategy_df:
            OHLCV DataFrame used to attach price data in analytics mode.
        include_metadata:
            ``True``  → analytics mode (full ``indicator_data`` attached).
            ``False`` → core mode (``indicator_data=None``, fastest path).
        """
        buy_mask = signals_df["we_buy"].values
        sell_mask = signals_df["we_sell"].values

        n = len(signals_df)
        signal_values = np.zeros(n, dtype=np.int8)
        signal_values[buy_mask] = 1   # BUY
        signal_values[sell_mask] = 2  # SELL

        signals = pd.Series(
            signal_values,
            index=signals_df.index,
            name="signal_type",
            dtype="int8",
        )

        indicator_data = None
        if include_metadata:
            indicator_data = strategy_df.assign(
                we_buy=buy_mask,
                we_sell=sell_mask,
            )

        # DEC-022: mode tag is "analytics" (never "debug")
        mode_tag = "analytics" if include_metadata else "core"

        return cls(
            signals=signals,
            indicator_data=indicator_data,
            signal_metadata={
                "source": "wbws_trigger",
                "mode": mode_tag,
            },
        )

    # ------------------------------------------------------------------
    # Iteration
    # ------------------------------------------------------------------

    def __iter__(self) -> Iterator[Signal]:
        """Iterate over ``Signal`` objects.

        .. warning::
            **Requires analytics mode** (``indicator_data`` must not be
            ``None``).  In core mode, ``indicator_data`` is ``None`` and
            constructing ``Signal`` objects would silently produce
            ``mid_price=0.0`` — an invalid state that causes wrong P&L.

            Use ``iter_raw()`` instead when ``indicator_data`` is absent.

        Raises
        ------
        RuntimeError
            If ``indicator_data is None`` (i.e. core mode).
        """
        if self.indicator_data is None:
            raise RuntimeError(
                "SignalFrame.__iter__ requires indicator_data (analytics mode only). "
                "In core mode, indicator_data is None — use iter_raw() which returns "
                "(timestamp, signal_code) tuples without needing price data. "                
            )

        active = self.signals[self.signals != 0]
        for ts, code in active.items():
            sig_type = SignalType.from_code(int(code))
            if sig_type is None:
                continue

            mid_price = 0.0
            metadata: Dict[str, Any] = {}
            if ts in self.indicator_data.index:
                row = self.indicator_data.loc[ts]
                if "close" in self.indicator_data.columns:
                    mid_price = float(row["close"])
                metadata = row.to_dict()

            yield Signal(
                timestamp=ts,
                signal_type=sig_type,
                mid_price=mid_price,
                metadata=metadata,
            )

    def iter_raw(self) -> Iterator[Tuple[pd.Timestamp, int]]:
        """Fast iterator — no ``Signal`` object construction.

        Returns ``(timestamp, code)`` pairs where ``code`` is ``1`` (BUY)
        or ``2`` (SELL).  Works in both core and analytics mode.
        """
        for ts, code in self.signals[self.signals != 0].items():
            yield ts, int(code)

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    def get_signal_at(self, timestamp: pd.Timestamp) -> Optional[Signal]:
        """Return the ``Signal`` at ``timestamp``, or ``None``."""
        if timestamp not in self.signals.index:
            return None
        code = int(self.signals.loc[timestamp])
        if code == 0:
            return None
        sig_type = SignalType.from_code(code)
        if sig_type is None:
            return None

        mid_price = 0.0
        metadata: Dict[str, Any] = {}
        if self.indicator_data is not None and timestamp in self.indicator_data.index:
            row = self.indicator_data.loc[timestamp]
            if "close" in self.indicator_data.columns:
                mid_price = float(row["close"])
            metadata = row.to_dict()

        return Signal(
            timestamp=timestamp,
            signal_type=sig_type,
            mid_price=mid_price,
            metadata=metadata,
        )

    @property
    def buy_signals(self) -> pd.Series:
        """Series of BUY signal timestamps (code == 1)."""
        return self.signals[self.signals == 1]

    @property
    def sell_signals(self) -> pd.Series:
        """Series of SELL signal timestamps (code == 2)."""
        return self.signals[self.signals == 2]

    def count_by_type(self) -> Dict[str, int]:
        """Vectorised count — no Python loop.

        Returns ``{"buy": N, "sell": N, "total": N}``.
        """
        values = self.signals.values  # numpy int8 array
        buy_count = int(np.sum(values == 1))
        sell_count = int(np.sum(values == 2))
        return {"buy": buy_count, "sell": sell_count, "total": buy_count + sell_count}

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __len__(self) -> int:  # noqa: D105
        return len(self.signals)

    def __str__(self) -> str:  # noqa: D105
        c = self.count_by_type()
        mode = self.signal_metadata.get("mode", "unknown")
        return (
            f"SignalFrame({c['total']} signals: {c['buy']} BUY, {c['sell']} SELL"
            f", mode={mode})"
        )

# =============================================================================
# SIGNAL STATISTICS
# =============================================================================

@dataclass(frozen=True)
class SignalStats:
    """Aggregated statistics about a set of signals.
    """

    buy_count: int = 0
    sell_count: int = 0
    total_count: int = 0
    buy_percentage: float = 0.0
    sell_percentage: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_signal_frame(
        cls,
        signal_frame: SignalFrame,
        verbose: bool = True,
    ) -> "SignalStats":
        """Build ``SignalStats`` from a ``SignalFrame``.

        Parameters
        ----------
        signal_frame:
            Source frame.
        verbose:
            ``True`` → include ``signal_metadata`` in stats (analytics mode).
            ``False`` → empty metadata dict (core mode).
        """
        counts = signal_frame.count_by_type()
        total = counts["total"]
        buy_pct = (counts["buy"] / total * 100) if total > 0 else 0.0
        sell_pct = (counts["sell"] / total * 100) if total > 0 else 0.0

        metadata = signal_frame.signal_metadata.copy() if verbose else {}

        return cls(
            buy_count=counts["buy"],
            sell_count=counts["sell"],
            total_count=total,
            buy_percentage=buy_pct,
            sell_percentage=sell_pct,
            metadata=metadata,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialise to dict (JSON-safe)."""
        return {
            "buy": self.buy_count,
            "sell": self.sell_count,
            "total": self.total_count,
            "buy_percentage": round(self.buy_percentage, 2),
            "sell_percentage": round(self.sell_percentage, 2),
            **self.metadata,
        }

    def __str__(self) -> str:  # noqa: D105
        return (
            f"BUY: {self.buy_count} ({self.buy_percentage:.1f}%), "
            f"SELL: {self.sell_count} ({self.sell_percentage:.1f}%), "
            f"Total: {self.total_count}"
        )