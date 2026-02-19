"""
Signal Generator v2 - Typed Contracts & Dual-Mode

This module replaces the string-based SignalGenerator with a typed contract
implementation that integrates with DataBundle and returns SignalFrame.

Key Features:
- Accepts DataBundle (not raw DataFrames)
- Returns SignalFrame (not tuple)
- Dual-mode support (core/analytics)
- Preserves WBWSTrigger performance
- SignalType enum (not strings)

Author: Migration Project
Version: 2.2.0
Date: 2026-02-19
Session: 20 — Block B (removed SignalGeneratorAdapter legacy class)
"""

import logging
from typing import Optional

from src.indicators.wbws_trigger import WBWSTrigger
from src.strategies.contracts.data_contracts import DataBundle
from src.strategies.contracts.signal_contracts import SignalFrame, SignalStats

logger = logging.getLogger(__name__)


class SignalGenerator:
    """
    Signal Generator v2 - Typed Contracts & Dual-Mode
    
    Generates trading signals from DataBundle using WBWSTrigger,
    returning typed SignalFrame instead of string-based signals.
    
    Attributes:
        htf_period: Higher timeframe period (e.g., "1H")
        mode: Execution mode ("core" or "analytics")
        trigger: WBWSTrigger instance (reused for performance)
    
    Performance:
        - Core mode: ~22-25ms (with int8 optimization)
        - Analytics mode: ~28-30ms (with lazy metadata)
    """

    def __init__(self, htf_period: str, mode: str = "core"):
        """
        Initialize SignalGenerator.
        
        Args:
            htf_period: Higher timeframe period (e.g., "1H")
            mode: Execution mode — "core" (fast, minimal output) or
                  "analytics" (full metadata for reporting)
        
        Raises:
            ValueError: If htf_period is missing or mode is invalid
        """
        if not htf_period:
            raise ValueError("htf_period configuration is missing.")
        
        # Block A note: "debug" guard will be added in Block A rename pass
        valid_modes = {"core", "analytics"}
        if mode not in valid_modes:
            raise ValueError(
                f"mode must be one of {valid_modes}, got '{mode}'. "
                f"Note: 'debug' has been renamed to 'analytics'."
            )
        
        self.htf_period = htf_period
        self.mode = mode
        
        # Reuse WBWSTrigger instance for performance (DEC-025: stateless between calls)
        self.trigger = WBWSTrigger(htf_period=self.htf_period)
        
        if self.mode == "analytics":
            logger.info(f"[SignalGenerator] initialized (mode={mode}, htf_period={htf_period})")
    
    def generate_signals(self, data_bundle: DataBundle) -> SignalFrame:
        """
        Generate signals from DataBundle.
        
        Args:
            data_bundle: Loaded data from DataLoader
            
        Returns:
            SignalFrame with typed signals (int8 codes)
        
        Raises:
            ValueError: If data_bundle is invalid
        """
        if data_bundle is None:
            raise ValueError("data_bundle cannot be None")
        
        if data_bundle.strategy is None or data_bundle.htf is None:
            raise ValueError("data_bundle.strategy and data_bundle.htf are required")
        
        if self.mode == "analytics":
            logger.info(
                f"[SignalGenerator] Generating signals from "
                f"{len(data_bundle.strategy)} strategy bars..."
            )
        
        # WBWSTrigger is stateless (DEC-025) — returns result directly
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