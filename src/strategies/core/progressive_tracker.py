"""
ProgressiveTracker v2
Debug-only, row-level introspection of signal → filter → risk → trade lifecycle.
Optimized for clarity, minimal schema, and performance.
"""

import pandas as pd
import logging
from typing import Dict, Optional
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class ProgressiveTracker:
    """
    ProgressiveTracker v2:
    - Debug-only component
    - Tracks each signal through all decision stages
    - Produces a clean, minimal, stable CSV for human inspection
    - Not used in core/backtester mode
    """

    # --- Column groups (lean, stable, meaningful) ---
    BASIC_COLS = [
        "signal_id", "timestamp", "signal", "mid_price",
        "htf_bull", "htf_bear", "candle_type",
        "we_buy_indicator", "we_sell_indicator"
    ]

    STAGE_COLS = [
        "stage_raw", "stage_time", "stage_technical",
        "stage_position", "stage_risk", "stage_trade"
    ]

    STAGE_REASON_COLS = [
        "stage_time_reason", "stage_technical_reason",
        "stage_position_reason", "stage_risk_reason",
        "stage_trade_reason"
    ]

    TIME_FILTER_COLS = [
        "session_start", "session_end", "hour", "minute", "is_in_session"
    ]

    TECHNICAL_COLS = [
        "technical_details"   # flexible string for any number of filters
    ]

    POSITION_COLS = [
        "current_direction", "open_positions_count",
        "pyramiding_enabled", "close_on_opposite",
        "can_open_new_position"
    ]

    RISK_COLS = [
        "entry_price", "sl_price", "tp_price",
        "atr_value", "atr_length", "atr_multiplier",
        "sl_distance_raw", "sl_price_raw",
        "annual_range_value", "risk_percentile_calculated",
        "max_risk_percentile", "risk_percentile_passed",
        "sl_price_final", "tp_price_final", "rr_ratio",
        "spread_enabled", "spread_type", "spread_value",
        "spread_points", "entry_price_mid",
        "entry_price_adjusted", "spread_efficiency_percent"
    ]

    TRADE_EXEC_COLS = [
        "trade_id", "position_id",
        "entry_time", "entry_price_executed",
        "sl_price_executed", "tp_price_executed"
    ]

    TRADE_EXIT_COLS = [
        "exit_time", "exit_price", "exit_reason",
        "pnl_points", "pnl_percent",
        "duration_bars", "duration_minutes",
        "is_win", "is_loss",
        "exit_check_high", "exit_check_low"
    ]

    META_COLS = [
        "final_status", "processing_notes", "execution_timestamp"
    ]

    def __init__(self, config: Dict):
        self.config = config
        self.signals = []
        self.signal_dict = {}
        self.signal_counter = 0

        # Final column order
        self.columns = (
            self.BASIC_COLS +
            self.STAGE_COLS + self.STAGE_REASON_COLS +
            self.TIME_FILTER_COLS + self.TECHNICAL_COLS +
            self.POSITION_COLS + self.RISK_COLS +
            self.TRADE_EXEC_COLS + self.TRADE_EXIT_COLS +
            self.META_COLS
        )

    # ----------------------------------------------------------------------
    # RAW SIGNAL
    # ----------------------------------------------------------------------
    def record_raw_signal(
        self, timestamp: pd.Timestamp, signal: str, mid_price: float,
        indicator_row: Optional[pd.Series] = None
    ) -> int:

        self.signal_counter += 1
        sig_id = self.signal_counter

        record = {col: None for col in self.columns}
        record.update({
            "signal_id": sig_id,
            "timestamp": timestamp,
            "signal": signal,
            "mid_price": mid_price,
            "stage_raw": "PASS",
            "final_status": "RAW",
            "execution_timestamp": datetime.now().isoformat()
        })

        if indicator_row is not None:
            record["htf_bull"] = indicator_row.get("htf_bull")
            record["htf_bear"] = indicator_row.get("htf_bear")
            record["candle_type"] = indicator_row.get("candle_type")
            record["we_buy_indicator"] = indicator_row.get("we_buy")
            record["we_sell_indicator"] = indicator_row.get("we_sell")

        self.signals.append(record)
        self.signal_dict[sig_id] = record
        return sig_id

    # ----------------------------------------------------------------------
    # TIME FILTER
    # ----------------------------------------------------------------------
    def update_time_filter_details(
        self, signal_id: int, passed: bool, reason: str = None,
        session_start: str = None, session_end: str = None,
        hour: int = None, minute: int = None, is_in_session: bool = None
    ):
        sig = self.signal_dict.get(signal_id)
        if not sig:
            return False

        sig["stage_time"] = "PASS" if passed else "REJECT"
        sig["stage_time_reason"] = reason
        sig["session_start"] = session_start
        sig["session_end"] = session_end
        sig["hour"] = hour
        sig["minute"] = minute
        sig["is_in_session"] = is_in_session

        if not passed:
            sig["final_status"] = "REJECTED_TIME"
        return True

    # ----------------------------------------------------------------------
    # TECHNICAL FILTER (RSI + future filters)
    # ----------------------------------------------------------------------
    def update_technical_details(
        self, signal_id: int, passed: bool, reason: str = None,
        technical_details: Optional[str] = None
    ):
        sig = self.signal_dict.get(signal_id)
        if not sig:
            return False

        sig["stage_technical"] = "PASS" if passed else "REJECT"
        sig["stage_technical_reason"] = reason
        sig["technical_details"] = technical_details

        if not passed and sig["final_status"] == "RAW":
            sig["final_status"] = "REJECTED_TECHNICAL"
        return True

    # ----------------------------------------------------------------------
    # POSITION MANAGEMENT
    # ----------------------------------------------------------------------
    def update_position_management_details(
        self, signal_id: int, action: str, reason: str = None,
        current_direction: str = None, open_positions_count: int = 0,
        pyramiding_enabled: bool = False, close_on_opposite: bool = False,
        can_open_new_position: bool = False
    ):
        sig = self.signal_dict.get(signal_id)
        if not sig:
            return False

        sig["stage_position"] = action
        sig["stage_position_reason"] = reason
        sig["current_direction"] = current_direction
        sig["open_positions_count"] = open_positions_count
        sig["pyramiding_enabled"] = pyramiding_enabled
        sig["close_on_opposite"] = close_on_opposite
        sig["can_open_new_position"] = can_open_new_position

        if action == "REJECT":
            sig["final_status"] = "REJECTED_POSITION"
        return True

    # ----------------------------------------------------------------------
    # RISK MANAGEMENT
    # ----------------------------------------------------------------------
    def update_risk_management_details(self, signal_id: int, approved: bool, **kwargs):
        sig = self.signal_dict.get(signal_id)
        if not sig:
            return False

        sig["stage_risk"] = "APPROVED" if approved else "REJECTED"
        sig["stage_risk_reason"] = kwargs.get("reason")

        for key in self.RISK_COLS:
            if key in kwargs:
                sig[key] = kwargs[key]

        if not approved:
            sig["final_status"] = "REJECTED_RISK"
        return True

    # ----------------------------------------------------------------------
    # TRADE EXECUTION
    # ----------------------------------------------------------------------
    def update_trade_execution_details(self, signal_id: int, **kwargs):
        sig = self.signal_dict.get(signal_id)
        if not sig:
            return False

        trade_id = kwargs.get("trade_id")
        sig["stage_trade"] = "EXECUTED" if trade_id else "NOT_EXECUTED"
        sig["stage_trade_reason"] = kwargs.get("reason")

        for key in self.TRADE_EXEC_COLS + self.TRADE_EXIT_COLS:
            if key in kwargs:
                sig[key] = kwargs[key]

        if trade_id:
            sig["final_status"] = "TRADE_CLOSED" if kwargs.get("exit_price") is not None else "TRADE_OPEN"
        else:
            sig["final_status"] = "NO_TRADE"
        return True

    # ----------------------------------------------------------------------
    # NOTES
    # ----------------------------------------------------------------------
    def add_processing_note(self, signal_id: int, note: str):
        sig = self.signal_dict.get(signal_id)
        if not sig:
            return False

        existing = sig.get("processing_notes")
        sig["processing_notes"] = f"{existing}; {note}" if existing else note
        return True

    # ----------------------------------------------------------------------
    # EXPORT
    # ----------------------------------------------------------------------
    def get_dataframe(self) -> pd.DataFrame:
        if not self.signals:
            return pd.DataFrame(columns=self.columns)

        df = pd.DataFrame(self.signals)

        # Ensure all columns exist
        for col in self.columns:
            if col not in df.columns:
                df[col] = None

        # Minimal dtype optimization
        bool_cols = ["is_in_session", "pyramiding_enabled", "close_on_opposite",
                     "can_open_new_position", "risk_percentile_passed",
                     "spread_enabled", "is_win", "is_loss"]

        for col in bool_cols:
            if col in df.columns:
                df[col] = df[col].astype("bool")

        return df[self.columns]
    
    def get_statistics(self) -> Dict:
        """Get signal progression statistics"""
        df = self.get_dataframe()
        
        if df.empty:
            return { 
                "total_signals": 0, 
                "by_stage": {}, 
                "by_final_status": {}, 
                "rejection_breakdown": {} 
            }
        
        stats = {
            'total_signals': len(df),
            'by_stage': {},
            'by_final_status': df['final_status'].value_counts().to_dict(),
            'rejection_breakdown': {}
        }
        
        # Count by stage
        for stage in ['time', 'technical', 'position', 'risk', 'trade']:
            col = f"stage_{stage}"
            if col in df.columns:
                stats["by_stage"][stage] = df[col].value_counts().to_dict()
        
        # Rejection reasons
        for stage in ['time', 'technical', 'position', 'risk']:
            final_status = f"REJECTED_{stage.upper()}"
            rejected = df[df["final_status"] == final_status]
            if not rejected.empty:
                reason_col = f"stage_{stage}_reason"
                if reason_col in rejected.columns:
                    stats["rejection_breakdown"][stage.upper()] = ( 
                        rejected[reason_col].value_counts().to_dict()
                    )
        return stats

    def save_to_csv(self, project_root: Path, timestamp_str: str) -> Path:
        out_dir = project_root / "outputs" / "signals" / "progressive"
        out_dir.mkdir(parents=True, exist_ok=True)

        path = out_dir / f"signals_progressive_{timestamp_str}.csv"
        self.get_dataframe().to_csv(path, index=False)

        logger.info(f"Progressive tracker: {len(self.signals)} signals saved to CSV")
        return path