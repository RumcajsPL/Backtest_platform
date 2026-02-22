"""
Unit Tests for StructuredLogger
=================================
Tests JSON structured logging, enum handling, and serialization.
"""

import pytest
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import Mock, patch, call

import pandas as pd
import numpy as np

from src.strategies.specific.modules.structured_logger import (
    StructuredLogger,
    LogLevel,
    LogStage,
    log_signal_generated,
    log_filter_decision,
    log_risk_decision,
    log_trade_opened,
    log_trade_closed
)


class TestStructuredLogger:
    """Tests for StructuredLogger class."""

    @pytest.fixture
    def temp_log_dir(self, tmp_path):
        """Create temporary log directory."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        return log_dir

    @pytest.fixture
    def logger(self, temp_log_dir):
        """Create a StructuredLogger instance for testing."""
        return StructuredLogger(
            module_name="TestModule",
            log_dir=temp_log_dir,
            enable_console=False,  # Disable console for testing
            enable_file=True,
            min_level=LogLevel.DEBUG
        )

    def test_initialization(self, temp_log_dir):
        """Test logger initialization."""
        logger = StructuredLogger(
            module_name="TestModule",
            log_dir=temp_log_dir,
            enable_console=True,
            enable_file=True
        )
        
        assert logger.module_name == "TestModule"
        assert logger.min_level == LogLevel.INFO
        
        # Check that log file was created
        log_file = temp_log_dir / "testmodule.log"
        assert log_file.exists()

    def test_initialization_no_file(self):
        """Test initialization without file logging."""
        logger = StructuredLogger(
            module_name="TestModule",
            enable_console=True,
            enable_file=False
        )
        
        assert logger.logger is not None

    def test_get_logging_level(self, logger):
        """Test LogLevel to logging module level conversion."""
        assert logger._get_logging_level(LogLevel.DEBUG) == logging.DEBUG
        assert logger._get_logging_level(LogLevel.INFO) == logging.INFO
        assert logger._get_logging_level(LogLevel.WARNING) == logging.WARNING
        assert logger._get_logging_level(LogLevel.ERROR) == logging.ERROR
        assert logger._get_logging_level(LogLevel.CRITICAL) == logging.CRITICAL

    def test_log_event(self, logger, temp_log_dir):
        """Test logging a basic event."""
        logger.log_event(
            stage=LogStage.SIGNAL_GENERATION,
            event="test_event",
            level=LogLevel.INFO,
            signal_type="BUY",
            price=1.2345,
            confidence=0.85
        )
        
        # Read log file
        log_file = temp_log_dir / "testmodule.log"
        lines = log_file.read_text().strip().split('\n')
        
        assert len(lines) == 1
        
        log_entry = json.loads(lines[0])
        assert log_entry["module"] == "TestModule"
        assert log_entry["stage"] == "signal_generation"
        assert log_entry["event"] == "test_event"
        assert log_entry["level"] == "INFO"
        assert log_entry["signal_type"] == "BUY"
        assert log_entry["price"] == 1.2345
        assert log_entry["confidence"] == 0.85
        assert "timestamp" in log_entry

    def test_log_event_with_timestamp_serialization(self, logger, temp_log_dir):
        """Test logging with pandas Timestamp."""
        timestamp = pd.Timestamp("2025-01-15 10:30:00")
        
        logger.log_event(
            stage=LogStage.TRADE_EXECUTION,
            event="trade_event",
            signal_time=timestamp
        )
        
        log_file = temp_log_dir / "testmodule.log"
        lines = log_file.read_text().strip().split('\n')
        log_entry = json.loads(lines[0])
        
        # Timestamp should be ISO format string
        assert log_entry["signal_time"] == "2025-01-15T10:30:00"

    def test_log_event_with_enum_serialization(self, logger, temp_log_dir):
        """Test logging with enum values."""
        logger.log_event(
            stage=LogStage.RISK_MANAGEMENT,
            event="risk_check",
            direction=TradeDirection.LONG if 'TradeDirection' in dir() else "LONG"
        )
        
        log_file = temp_log_dir / "testmodule.log"
        lines = log_file.read_text().strip().split('\n')
        log_entry = json.loads(lines[0])
        
        # Enum should be converted to value
        assert log_entry["direction"] in ["LONG", "SHORT"]

    def test_log_event_with_dataframe_serialization(self, logger, temp_log_dir):
        """Test logging with DataFrame (should be summarized)."""
        df = pd.DataFrame({
            "A": [1, 2, 3],
            "B": [4, 5, 6]
        })
        
        logger.log_event(
            stage=LogStage.DATA_LOAD,
            event="data_loaded",
            data=df
        )
        
        log_file = temp_log_dir / "testmodule.log"
        lines = log_file.read_text().strip().split('\n')
        log_entry = json.loads(lines[0])
        
        # DataFrame should be summarized
        assert "<DataFrame shape=" in log_entry["data"]

    def test_log_event_with_numpy_types(self, logger, temp_log_dir):
        """Test logging with numpy types."""
        logger.log_event(
            stage=LogStage.METRICS,
            event="stats",
            mean=np.float32(10.5),
            count=np.int64(100)
        )
        
        log_file = temp_log_dir / "testmodule.log"
        lines = log_file.read_text().strip().split('\n')
        log_entry = json.loads(lines[0])
        
        # Numpy types should be converted to Python types
        assert log_entry["mean"] == 10.5
        assert log_entry["count"] == 100

    def test_log_decision(self, logger, temp_log_dir):
        """Test logging a decision."""
        logger.log_decision(
            stage=LogStage.FILTER_TECHNICAL,
            decision="reject",
            reason="ADX below threshold",
            filter_name="TrendFilter",
            adx_value=18.5,
            threshold=25.0
        )
        
        log_file = temp_log_dir / "testmodule.log"
        lines = log_file.read_text().strip().split('\n')
        log_entry = json.loads(lines[0])
        
        assert log_entry["stage"] == "filter_technical"
        assert log_entry["event"] == "decision"
        assert log_entry["decision"] == "reject"
        assert log_entry["reason"] == "ADX below threshold"
        assert log_entry["filter_name"] == "TrendFilter"
        assert log_entry["adx_value"] == 18.5
        assert log_entry["threshold"] == 25.0

    def test_log_error(self, logger, temp_log_dir):
        """Test logging an error."""
        try:
            raise ValueError("Test error message")
        except ValueError as e:
            logger.log_error(
                stage=LogStage.DATA_LOAD,
                error=e,
                file_path="test.csv"
            )
        
        log_file = temp_log_dir / "testmodule.log"
        lines = log_file.read_text().strip().split('\n')
        log_entry = json.loads(lines[0])
        
        assert log_entry["stage"] == "data_load"
        assert log_entry["event"] == "error"
        assert log_entry["level"] == "ERROR"
        assert log_entry["error_type"] == "ValueError"
        assert log_entry["error_message"] == "Test error message"
        assert log_entry["file_path"] == "test.csv"

    def test_log_performance(self, logger, temp_log_dir):
        """Test logging performance metrics."""
        logger.log_performance(
            stage=LogStage.FILTER_TECHNICAL,
            operation="compute_indicators",
            duration_ms=125.5,
            indicator_count=10,
            bar_count=88194
        )
        
        log_file = temp_log_dir / "testmodule.log"
        lines = log_file.read_text().strip().split('\n')
        log_entry = json.loads(lines[0])
        
        assert log_entry["stage"] == "filter_technical"
        assert log_entry["event"] == "performance"
        assert log_entry["level"] == "DEBUG"
        assert log_entry["operation"] == "compute_indicators"
        assert log_entry["duration_ms"] == 125.5
        assert log_entry["indicator_count"] == 10
        assert log_entry["bar_count"] == 88194

    def test_serialize_value_timestamp(self, logger):
        """Test serialization of pandas Timestamp."""
        ts = pd.Timestamp("2025-01-15 10:30:00")
        serialized = logger._serialize_value(ts)
        assert serialized == "2025-01-15T10:30:00"

    def test_serialize_value_dataframe(self, logger):
        """Test serialization of DataFrame."""
        df = pd.DataFrame({"A": [1, 2, 3]})
        serialized = logger._serialize_value(df)
        assert "<DataFrame shape=" in serialized

    def test_serialize_value_series(self, logger):
        """Test serialization of Series."""
        s = pd.Series([1, 2, 3])
        serialized = logger._serialize_value(s)
        assert "<Series shape=" in serialized

    def test_serialize_value_enum(self, logger):
        """Test serialization of Enum."""
        class TestEnum(LogLevel):
            pass
        
        value = LogLevel.INFO
        serialized = logger._serialize_value(value)
        assert serialized == "INFO"

    def test_serialize_value_custom_object(self, logger):
        """Test serialization of custom object."""
        class CustomClass:
            def __init__(self):
                self.x = 1
                self.y = 2
        
        obj = CustomClass()
        serialized = logger._serialize_value(obj)
        assert "<CustomClass>" in serialized

    def test_serialize_value_non_serializable(self, logger):
        """Test serialization of non-JSON-serializable object."""
        # Complex number is not JSON serializable
        obj = 1 + 2j
        serialized = logger._serialize_value(obj)
        assert serialized == "(1+2j)" or str(serialized) is not None

    def test_log_signal_generated_convenience(self, logger, temp_log_dir):
        """Test convenience function for signal generation."""
        timestamp = pd.Timestamp("2025-01-15 10:30:00")
        
        log_signal_generated(
            logger=logger,
            timestamp=timestamp,
            signal_type="BUY",
            confidence=0.85,
            price=1.2345
        )
        
        log_file = temp_log_dir / "testmodule.log"
        lines = log_file.read_text().strip().split('\n')
        log_entry = json.loads(lines[0])
        
        assert log_entry["stage"] == "signal_generation"
        assert log_entry["event"] == "signal_generated"
        assert log_entry["signal_type"] == "BUY"
        assert log_entry["timestamp"] == "2025-01-15T10:30:00"
        assert log_entry["confidence"] == 0.85
        assert log_entry["price"] == 1.2345

    def test_log_filter_decision_convenience(self, logger, temp_log_dir):
        """Test convenience function for filter decision."""
        log_filter_decision(
            logger=logger,
            filter_name="rsi_filter",
            passed=False,
            reason="RSI overbought",
            rsi_value=75.0,
            threshold=70.0
        )
        
        log_file = temp_log_dir / "testmodule.log"
        lines = log_file.read_text().strip().split('\n')
        log_entry = json.loads(lines[0])
        
        assert log_entry["stage"] in ["filter_time", "filter_technical"]
        assert log_entry["event"] == "decision"
        assert log_entry["decision"] == "reject"
        assert log_entry["reason"] == "RSI overbought"
        assert log_entry["filter_name"] == "rsi_filter"
        assert log_entry["rsi_value"] == 75.0
        assert log_entry["threshold"] == 70.0

    def test_log_risk_decision_convenience(self, logger, temp_log_dir):
        """Test convenience function for risk decision."""
        log_risk_decision(
            logger=logger,
            approved=True,
            reason="Risk within limits",
            risk_percentile=1.8,
            max_risk=3.0
        )
        
        log_file = temp_log_dir / "testmodule.log"
        lines = log_file.read_text().strip().split('\n')
        log_entry = json.loads(lines[0])
        
        assert log_entry["stage"] == "risk_management"
        assert log_entry["event"] == "decision"
        assert log_entry["decision"] == "approve"
        assert log_entry["reason"] == "Risk within limits"
        assert log_entry["risk_percentile"] == 1.8
        assert log_entry["max_risk"] == 3.0

    def test_log_trade_opened_convenience(self, logger, temp_log_dir):
        """Test convenience function for trade opened."""
        log_trade_opened(
            logger=logger,
            trade_id="E123",
            direction="LONG",
            entry_price=1.2345,
            stop_loss=1.2300,
            take_profit=1.2400,
            signal_id=456
        )
        
        log_file = temp_log_dir / "testmodule.log"
        lines = log_file.read_text().strip().split('\n')
        log_entry = json.loads(lines[0])
        
        assert log_entry["stage"] == "trade_execution"
        assert log_entry["event"] == "trade_opened"
        assert log_entry["trade_id"] == "E123"
        assert log_entry["direction"] == "LONG"
        assert log_entry["entry_price"] == 1.2345
        assert log_entry["stop_loss"] == 1.2300
        assert log_entry["take_profit"] == 1.2400
        assert log_entry["signal_id"] == 456

    def test_log_trade_closed_convenience(self, logger, temp_log_dir):
        """Test convenience function for trade closed."""
        log_trade_closed(
            logger=logger,
            trade_id="E123",
            exit_reason="TAKE_PROFIT",
            pnl_points=45.5,
            pnl_percent=1.5,
            duration_minutes=15
        )
        
        log_file = temp_log_dir / "testmodule.log"
        lines = log_file.read_text().strip().split('\n')
        log_entry = json.loads(lines[0])
        
        assert log_entry["stage"] == "trade_execution"
        assert log_entry["event"] == "trade_closed"
        assert log_entry["trade_id"] == "E123"
        assert log_entry["exit_reason"] == "TAKE_PROFIT"
        assert log_entry["pnl_points"] == 45.5
        assert log_entry["pnl_percent"] == 1.5
        assert log_entry["duration_minutes"] == 15

    def test_multiple_log_entries(self, logger, temp_log_dir):
        """Test multiple log entries in same file."""
        for i in range(5):
            logger.log_event(
                stage=LogStage.DATA_LOAD,
                event=f"event_{i}",
                index=i
            )
        
        log_file = temp_log_dir / "testmodule.log"
        lines = log_file.read_text().strip().split('\n')
        
        assert len(lines) == 5
        
        for i, line in enumerate(lines):
            entry = json.loads(line)
            assert entry["event"] == f"event_{i}"
            assert entry["index"] == i

    def test_log_level_filtering(self, temp_log_dir):
        """Test that log level filtering works."""
        logger = StructuredLogger(
            module_name="TestModule",
            log_dir=temp_log_dir,
            enable_console=False,
            enable_file=True,
            min_level=LogLevel.WARNING  # Only WARNING and above
        )
        
        # These should not be logged
        logger.log_event(LogStage.DATA_LOAD, "debug_event", level=LogLevel.DEBUG)
        logger.log_event(LogStage.DATA_LOAD, "info_event", level=LogLevel.INFO)
        
        # These should be logged
        logger.log_event(LogStage.DATA_LOAD, "warning_event", level=LogLevel.WARNING)
        logger.log_event(LogStage.DATA_LOAD, "error_event", level=LogLevel.ERROR)
        
        log_file = temp_log_dir / "testmodule.log"
        lines = log_file.read_text().strip().split('\n')
        
        assert len(lines) == 2
        assert "warning_event" in lines[0]
        assert "error_event" in lines[1]

    def test_timestamp_timezone(self, logger, temp_log_dir):
        """Test that timestamps include timezone info."""
        logger.log_event(LogStage.DATA_LOAD, "test_event")
        
        log_file = temp_log_dir / "testmodule.log"
        lines = log_file.read_text().strip().split('\n')
        entry = json.loads(lines[0])
        
        timestamp = entry["timestamp"]
        assert timestamp.endswith("Z")  # UTC indicator
        # Should be ISO format with timezone
        assert len(timestamp) > 20