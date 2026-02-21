"""
Signal Generator v3 - Final Hardened Implementation

Version: 3.0.0 (Hardening II Final)
Session: 21 - Final Hardening

Changes from v2.2.0:
- Block A: Accepts StrategyConfig instead of htf_period string (DEC-034)
- Block B: Validates htf_period against known pandas offset aliases (SG-1)
- Block B: Adds empty DataFrame guard with clear error message (SG-2)
- Block B: Removes all "debug" mode references - strict mode validation
- Returns SignalFrame with typed signals (int8 codes)
"""

import logging
from typing import Optional

import numpy as np
import pandas as pd

from src.indicators.wbws_trigger import WBWSTrigger
from src.strategies.contracts.data_contracts import DataBundle
from src.strategies.contracts.signal_contracts import SignalFrame, SignalStats
from src.config.config_schema import StrategyConfig

logger = logging.getLogger(__name__)

# Valid pandas offset aliases for HTF periods
_VALID_HTF_PERIODS = frozenset({
    "1min", "5min", "15min", "30min", "1H", "2H", "4H", "1D", "1W"
})


class SignalGenerator:
    """
    Signal Generator v3 - Typed Contracts & Dual-Mode

    Generates trading signals from DataBundle using WBWSTrigger,
    returning typed SignalFrame instead of string-based signals.

    Attributes:
        config: StrategyConfig instance
        mode: Execution mode ("core" or "analytics")
        trigger: WBWSTrigger instance (reused for performance)

    Performance:
        - Core mode: ~22-25ms (with int8 optimization)
        - Analytics mode: ~28-30ms (with lazy metadata)
    """

    def __init__(self, config: StrategyConfig, mode: str = "core"):
        """
        Initialize SignalGenerator with typed config.

        Args:
            config: StrategyConfig instance (DEC-034)
            mode: Execution mode — "core" (fast, minimal output) or
                  "analytics" (full metadata for reporting)

        Raises:
            ValueError: If htf_period is missing/invalid or mode is invalid
        """
        # Mode validation - strict, no "debug" allowed
        valid_modes = {"core", "analytics"}
        if mode not in valid_modes:
            raise ValueError(
                f"mode must be one of {valid_modes}, got '{mode}'. "
                f"'debug' is not a valid mode and has been removed."
            )
        self.mode = mode

        # Extract and validate htf_period from config (DEC-034)
        htf_period = config.data.htf_period
        if not htf_period or not htf_period.strip():
            raise ValueError(
                "data.htf_period is required in strategy config. "
                "Add htf_period to the data: section of your YAML."
            )

        # SG-1: Validate htf_period format
        htf_period = htf_period.strip()
        if htf_period not in _VALID_HTF_PERIODS:
            raise ValueError(
                f"data.htf_period='{htf_period}' is not a recognised period. "
                f"Valid values: {sorted(_VALID_HTF_PERIODS)}"
            )

        self.htf_period = htf_period
        self.config = config

        # Reuse WBWSTrigger instance for performance
        self.trigger = WBWSTrigger(htf_period=self.htf_period)

        if self.mode == "analytics":
            logger.info(
                f"[SignalGenerator] initialized (mode={mode}, htf_period={htf_period})"
            )

    def generate_signals(self, data_bundle: DataBundle) -> SignalFrame:
        """
        Generate signals from DataBundle.

        Args:
            data_bundle: Loaded data from DataLoader

        Returns:
            SignalFrame with typed signals (int8 codes)

        Raises:
            ValueError: If data_bundle is invalid or dataframes are empty
        """
        if data_bundle is None:
            raise ValueError("data_bundle cannot be None")

        # SG-2: Check for None AND empty DataFrames
        if data_bundle.strategy is None or data_bundle.strategy.empty:
            raise ValueError(
                "data_bundle.strategy is missing or empty. "
                "Verify data.paths.strategy_ohlcv points to a valid file with data "
                "in the configured date_range."
            )
        if data_bundle.htf is None or data_bundle.htf.empty:
            raise ValueError(
                "data_bundle.htf is missing or empty. "
                "Verify data.paths.htf_ohlcv exists and covers the configured date_range. "
                "htf data is required by SignalGenerator — it cannot be omitted."
            )

        if self.mode == "analytics":
            logger.info(
                f"[SignalGenerator] Generating signals from "
                f"{len(data_bundle.strategy)} strategy bars..."
            )

        # WBWSTrigger is stateless — returns result directly
        signals_df = self.trigger.calculate_signals(
            data_bundle.strategy,
            df_htf=data_bundle.htf
        )

        # Core mode: skip metadata for speed
        # Analytics mode: include full metadata for reporting
        include_metadata = (self.mode == "analytics")

        signal_frame = SignalFrame.from_wbws_trigger(
            signals_df=signals_df,
            strategy_df=data_bundle.strategy,
            include_metadata=include_metadata
        )

        if self.mode == "analytics":
            logger.info(f"[SignalGenerator] Generated signals: {signal_frame.count_by_type()}")

        return signal_frame

    def get_signal_stats(
        self,
        signal_frame: SignalFrame,
        verbose: Optional[bool] = None
    ) -> SignalStats:
        """
        Get signal statistics.

        Args:
            signal_frame: SignalFrame to analyze
            verbose: If True, include detailed metadata.
                     If None, auto-detect from mode (analytics=verbose, core=minimal)

        Returns:
            SignalStats instance
        """
        if signal_frame is None:
            return SignalStats()

        if verbose is None:
            verbose = (self.mode == "analytics")

        stats = SignalStats.from_signal_frame(signal_frame, verbose=verbose)

        if self.mode == "analytics":
            logger.info(f"[SignalGenerator] Signal stats: {stats}")

        return stats