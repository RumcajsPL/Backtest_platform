"""
Structured Logger - Production-Grade JSON Logging

Session 12 - Task 2
Version: 1.0.1

Provides structured JSON logging for audit trails and analysis.
Replaces scattered print/logger statements with consistent, parseable logs.

Design Principles:
- Single Responsibility: Only logging, no business logic
- Performance-Driven: Minimal overhead, optional analytics detail
- Explicit Contracts: All fields typed, no hidden state
- Type Safety: Enum-based log levels
- Production-Ready: JSON format for log aggregation tools
"""
import json
import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional
from pathlib import Path

import pandas as pd


class LogLevel(Enum):
    """Log level enumeration"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogStage(Enum):
    """Pipeline stage enumeration"""
    DATA_LOAD = "data_load"
    SIGNAL_GENERATION = "signal_generation"
    FILTER_TIME = "filter_time"
    FILTER_TECHNICAL = "filter_technical"
    RISK_MANAGEMENT = "risk_management"
    POSITION_MANAGEMENT = "position_management"
    TRADE_EXECUTION = "trade_execution"


class StructuredLogger:
    """
    Structured JSON logger for production environments.
    
    Features:
    - JSON output for log aggregation (ELK, Splunk, CloudWatch)
    - Consistent schema across all modules
    - Performance-aware (minimal overhead in production)
    - Type-safe logging with enums
    - Automatic timestamp and context tracking
    
    Example:
        logger = StructuredLogger("SignalGenerator")
        logger.log_event(
            stage=LogStage.SIGNAL_GENERATION,
            event="signal_generated",
            signal_type="BUY",
            timestamp=pd.Timestamp("2025-01-15 10:30"),
            confidence=0.85
        )
    """
    
    def __init__(
        self,
        module_name: str,
        log_dir: Optional[Path] = None,
        enable_console: bool = True,
        enable_file: bool = True,
        min_level: LogLevel = LogLevel.INFO,
    ):
        """
        Initialize structured logger.
        
        Args:
            module_name: Name of the module using this logger
            log_dir: Directory for log files (default: outputs/logs)
            enable_console: Log to console
            enable_file: Log to file
            min_level: Minimum log level to capture
        """
        self.module_name = module_name
        self.min_level = min_level
        
        # Setup Python logger
        self.logger = logging.getLogger(f"structured.{module_name}")
        self.logger.setLevel(logging.DEBUG)
        self.logger.handlers.clear()  # Remove existing handlers
        
        # Console handler (human-readable in dev, JSON in prod)
        if enable_console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(self._get_logging_level(min_level))
            console_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_formatter)
            self.logger.addHandler(console_handler)
        
        # File handler (always JSON)
        if enable_file:
            if log_dir is None:
                log_dir = Path("outputs/logs")
            log_dir.mkdir(parents=True, exist_ok=True)
            
            log_file = log_dir / f"{module_name.lower()}.log"
            file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
            file_handler.setLevel(self._get_logging_level(min_level))
            file_handler.setFormatter(logging.Formatter('%(message)s'))  # JSON only
            self.logger.addHandler(file_handler)
    
    def _get_logging_level(self, level: LogLevel) -> int:
        """Convert LogLevel enum to logging module level"""
        mapping = {
            LogLevel.DEBUG: logging.DEBUG,
            LogLevel.INFO: logging.INFO,
            LogLevel.WARNING: logging.WARNING,
            LogLevel.ERROR: logging.ERROR,
            LogLevel.CRITICAL: logging.CRITICAL,
        }
        return mapping[level]
    
    def log_event(
        self,
        stage: LogStage,
        event: str,
        level: LogLevel = LogLevel.INFO,
        **metadata: Any
    ) -> None:
        """
        Log a structured event.
        
        Args:
            stage: Pipeline stage (enum)
            event: Event name (e.g., "signal_generated", "filter_passed")
            level: Log level
            **metadata: Additional context (will be JSON serialized)
        
        Example:
            logger.log_event(
                stage=LogStage.SIGNAL_GENERATION,
                event="signal_generated",
                signal_type="BUY",
                timestamp=pd.Timestamp.now(),
                price=1.2345
            )
        """
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),  # Fixed deprecation
            "module": self.module_name,
            "stage": stage.value,
            "event": event,
            "level": level.value,
        }
        
        # Add metadata, converting non-JSON types
        for key, value in metadata.items():
            log_entry[key] = self._serialize_value(value)
        
        # Log as JSON
        json_str = json.dumps(log_entry)
        self.logger.log(self._get_logging_level(level), json_str)
    
    def log_decision(
        self,
        stage: LogStage,
        decision: str,
        reason: str,
        **context: Any
    ) -> None:
        """
        Log a decision point (filter, risk, position).
        
        Args:
            stage: Pipeline stage
            decision: Decision made ("pass", "reject", "approve", "open")
            reason: Reason for decision
            **context: Additional context
        
        Example:
            logger.log_decision(
                stage=LogStage.RISK_MANAGEMENT,
                decision="reject",
                reason="Risk exceeds 3% of annual range",
                signal_id=123,
                risk_percentile=3.2
            )
        """
        self.log_event(
            stage=stage,
            event="decision",
            decision=decision,
            reason=reason,
            **context
        )
    
    def log_error(
        self,
        stage: LogStage,
        error: Exception,
        **context: Any
    ) -> None:
        """
        Log an error with full context.
        
        Args:
            stage: Pipeline stage where error occurred
            error: Exception object
            **context: Additional context
        
        Example:
            try:
                result = compute_indicators(df)
            except Exception as e:
                logger.log_error(
                    stage=LogStage.FILTER_TECHNICAL,
                    error=e,
                    filter_name="TrendFilter",
                    timestamp=pd.Timestamp.now()
                )
        """
        self.log_event(
            stage=stage,
            event="error",
            level=LogLevel.ERROR,
            error_type=type(error).__name__,
            error_message=str(error),
            **context
        )
    
    def log_performance(
        self,
        stage: LogStage,
        operation: str,
        duration_ms: float,
        **metrics: Any
    ) -> None:
        """
        Log performance metrics.
        
        Args:
            stage: Pipeline stage
            operation: Operation name
            duration_ms: Duration in milliseconds
            **metrics: Additional performance metrics
        
        Example:
            start = time.perf_counter()
            result = expensive_operation()
            duration = (time.perf_counter() - start) * 1000
            
            logger.log_performance(
                stage=LogStage.FILTER_TECHNICAL,
                operation="compute_indicators",
                duration_ms=duration,
                indicator_count=10,
                bar_count=88194
            )
        """
        self.log_event(
            stage=stage,
            event="performance",
            level=LogLevel.DEBUG,
            operation=operation,
            duration_ms=round(duration_ms, 2),
            **metrics
        )
    
    def _serialize_value(self, value: Any) -> Any:
        """
        Serialize value for JSON output.
        
        Handles pandas Timestamps, Enums, numpy types, and other non-JSON types.
        """
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        elif isinstance(value, (pd.Series, pd.DataFrame)):
            return f"<{type(value).__name__} shape={getattr(value, 'shape', None)}>"
        elif isinstance(value, Enum):  # Moved this check BEFORE the dataclass check
            return value.value
        elif hasattr(value, '__dict__') and hasattr(value, '__class__'):
            # Dataclass or custom object
            return f"<{value.__class__.__name__}>"
        else:
            # Fallback: convert to string if not JSON serializable
            try:
                json.dumps(value)
                return value
            except (TypeError, ValueError):
                return str(value)


# Convenience functions for common logging patterns

def log_signal_generated(
    logger: StructuredLogger,
    timestamp: pd.Timestamp,
    signal_type: str,
    **context: Any
) -> None:
    """Log signal generation event"""
    logger.log_event(
        stage=LogStage.SIGNAL_GENERATION,
        event="signal_generated",
        timestamp=timestamp,
        signal_type=signal_type,
        **context
    )


def log_filter_decision(
    logger: StructuredLogger,
    filter_name: str,
    passed: bool,
    reason: Optional[str] = None,
    **context: Any
) -> None:
    """Log filter decision"""
    stage = LogStage.FILTER_TIME if "time" in filter_name.lower() else LogStage.FILTER_TECHNICAL
    logger.log_decision(
        stage=stage,
        decision="pass" if passed else "reject",
        reason=reason or ("Filter passed" if passed else "Filter rejected"),
        filter_name=filter_name,
        **context
    )


def log_risk_decision(
    logger: StructuredLogger,
    approved: bool,
    reason: str,
    **context: Any
) -> None:
    """Log risk management decision"""
    logger.log_decision(
        stage=LogStage.RISK_MANAGEMENT,
        decision="approve" if approved else "reject",
        reason=reason,
        **context
    )


def log_trade_opened(
    logger: StructuredLogger,
    trade_id: str,
    direction: str,
    entry_price: float,
    **context: Any
) -> None:
    """Log trade opened event"""
    logger.log_event(
        stage=LogStage.TRADE_EXECUTION,
        event="trade_opened",
        trade_id=trade_id,
        direction=direction,
        entry_price=entry_price,
        **context
    )


def log_trade_closed(
    logger: StructuredLogger,
    trade_id: str,
    exit_reason: str,
    pnl_points: float,
    **context: Any
) -> None:
    """Log trade closed event"""
    logger.log_event(
        stage=LogStage.TRADE_EXECUTION,
        event="trade_closed",
        trade_id=trade_id,
        exit_reason=exit_reason,
        pnl_points=pnl_points,
        **context
    )


# Example usage
if __name__ == "__main__":
    # Demo logging
    logger = StructuredLogger("DemoModule")
    
    # Signal generation
    log_signal_generated(
        logger,
        timestamp=pd.Timestamp("2025-01-15 10:30"),
        signal_type="BUY",
        confidence=0.85,
        price=1.2345
    )
    
    # Filter decision
    log_filter_decision(
        logger,
        filter_name="TrendFilter",
        passed=False,
        reason="ADX below threshold",
        adx_value=18.5,
        threshold=25.0
    )
    
    # Risk decision
    log_risk_decision(
        logger,
        approved=True,
        reason="Risk within limits",
        risk_percentile=1.8,
        max_risk=3.0
    )
    
    # Trade execution
    log_trade_opened(
        logger,
        trade_id="E123",
        direction="LONG",
        entry_price=1.2345,
        stop_loss=1.2300,
        take_profit=1.2400
    )
    
    print("\n✅ Demo logs written to outputs/logs/demomodule.log")
    print("✅ Check console output above for human-readable format")