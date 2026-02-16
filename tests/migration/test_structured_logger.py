"""
Unit Tests for Structured Logger

Session 12 - Infrastructure Testing
Version: 1.0.0

Tests the structured logging utility for correctness and reliability.
"""

# Add project root to path for proper module resolution
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import pytest
import json
from datetime import datetime

from src.utils.structured_logger import (
    StructuredLogger,
    LogLevel,
    LogStage,
    log_signal_generated,
    log_filter_decision,
    log_risk_decision,
    log_trade_opened,
    log_trade_closed,
)

import pandas as pd


class TestStructuredLogger:
    """Test StructuredLogger core functionality"""
    
    def test_logger_initialization(self):
        """Logger should initialize without errors"""
        logger = StructuredLogger(
            "TestModule",
            enable_console=False,
            enable_file=False
        )
        assert logger.module_name == "TestModule"
        assert logger.min_level == LogLevel.INFO
    
    def test_log_event_basic(self, tmp_path):
        """log_event should create valid JSON log entry"""
        # The logger creates files with lowercase module name
        log_file = tmp_path / "testmodule.log"  # Changed from test.log
        logger = StructuredLogger(
            "TestModule",
            log_dir=tmp_path,
            enable_console=False,
            enable_file=True
        )
        
        logger.log_event(
            stage=LogStage.SIGNAL_GENERATION,
            event="test_event",
            test_field="test_value"
        )
        
        # Read log file
        assert log_file.exists()
        with open(log_file, 'r') as f:
            log_line = f.readline()
        
        # Parse JSON
        log_data = json.loads(log_line)
        
        # Verify fields
        assert log_data["module"] == "TestModule"
        assert log_data["stage"] == "signal_generation"
        assert log_data["event"] == "test_event"
        assert log_data["level"] == "INFO"
        assert log_data["test_field"] == "test_value"
        assert "timestamp" in log_data
    
    def test_log_decision(self, tmp_path):
        """log_decision should capture decision context"""
        # The logger creates files with lowercase module name
        log_file = tmp_path / "testmodule.log"  # Changed from test.log
        logger = StructuredLogger(
            "TestModule",
            log_dir=tmp_path,
            enable_console=False
        )
        
        logger.log_decision(
            stage=LogStage.RISK_MANAGEMENT,
            decision="approve",
            reason="Risk within limits",
            risk_percentile=1.8,
            max_risk=3.0
        )
        
        assert log_file.exists()  # Add assertion before opening
        with open(log_file, 'r') as f:
            log_data = json.loads(f.readline())
        
        assert log_data["event"] == "decision"
        assert log_data["decision"] == "approve"
        assert log_data["reason"] == "Risk within limits"
        assert log_data["risk_percentile"] == 1.8
        assert log_data["max_risk"] == 3.0
    
    def test_log_error(self, tmp_path):
        """log_error should capture exception details"""
        logger = StructuredLogger(
            "TestModule",
            log_dir=tmp_path,
            enable_console=False
        )
        
        try:
            raise ValueError("Test error message")
        except Exception as e:
            logger.log_error(
                stage=LogStage.FILTER_TECHNICAL,
                error=e,
                filter_name="TestFilter"
            )
        
        log_file = tmp_path / "testmodule.log"
        with open(log_file, 'r') as f:
            log_data = json.loads(f.readline())
        
        assert log_data["event"] == "error"
        assert log_data["level"] == "ERROR"
        assert log_data["error_type"] == "ValueError"
        assert log_data["error_message"] == "Test error message"
        assert log_data["filter_name"] == "TestFilter"
    
    def test_log_performance(self, tmp_path):
        """log_performance should track timing metrics"""
        logger = StructuredLogger(
            "TestModule",
            log_dir=tmp_path,
            enable_console=False,
            min_level=LogLevel.DEBUG
        )
        
        logger.log_performance(
            stage=LogStage.FILTER_TECHNICAL,
            operation="compute_indicators",
            duration_ms=123.45,
            indicator_count=10
        )
        
        log_file = tmp_path / "testmodule.log"
        with open(log_file, 'r') as f:
            log_data = json.loads(f.readline())
        
        assert log_data["event"] == "performance"
        assert log_data["operation"] == "compute_indicators"
        assert log_data["duration_ms"] == 123.45
        assert log_data["indicator_count"] == 10
    
    def test_timestamp_serialization(self, tmp_path):
        """Pandas Timestamp should serialize to ISO format"""
        logger = StructuredLogger(
            "TestModule",
            log_dir=tmp_path,
            enable_console=False
        )
        
        ts = pd.Timestamp("2025-01-15 10:30:00")
        logger.log_event(
            stage=LogStage.SIGNAL_GENERATION,
            event="test",
            signal_time=ts
        )
        
        log_file = tmp_path / "testmodule.log"
        with open(log_file, 'r') as f:
            log_data = json.loads(f.readline())
        
        assert log_data["signal_time"] == "2025-01-15T10:30:00"
    
    def test_enum_serialization(self, tmp_path):
        """Enum values should serialize correctly"""
        logger = StructuredLogger(
            "TestModule",
            log_dir=tmp_path,
            enable_console=False
        )
        
        logger.log_event(
            stage=LogStage.SIGNAL_GENERATION,
            event="test",
            stage_enum=LogStage.RISK_MANAGEMENT
        )
        
        log_file = tmp_path / "testmodule.log"
        with open(log_file, 'r') as f:
            log_data = json.loads(f.readline())
        
        # This will still fail until we fix structured_logger.py
        assert log_data["stage_enum"] == "risk_management"


class TestConvenienceFunctions:
    """Test convenience logging functions"""
    
    def test_log_signal_generated(self, tmp_path):
        """log_signal_generated should work correctly"""
        logger = StructuredLogger(
            "TestModule",
            log_dir=tmp_path,
            enable_console=False
        )
        
        log_signal_generated(
            logger,
            timestamp=pd.Timestamp("2025-01-15 10:30"),
            signal_type="BUY",
            confidence=0.85
        )
        
        log_file = tmp_path / "testmodule.log"
        with open(log_file, 'r') as f:
            log_data = json.loads(f.readline())
        
        assert log_data["stage"] == "signal_generation"
        assert log_data["event"] == "signal_generated"
        assert log_data["signal_type"] == "BUY"
        assert log_data["confidence"] == 0.85
    
    def test_log_filter_decision(self, tmp_path):
        """log_filter_decision should work correctly"""
        logger = StructuredLogger(
            "TestModule",
            log_dir=tmp_path,
            enable_console=False
        )
        
        log_filter_decision(
            logger,
            filter_name="TrendFilter",
            passed=False,
            reason="ADX below threshold",
            adx_value=18.5
        )
        
        log_file = tmp_path / "testmodule.log"
        with open(log_file, 'r') as f:
            log_data = json.loads(f.readline())
        
        assert log_data["event"] == "decision"
        assert log_data["decision"] == "reject"
        assert log_data["filter_name"] == "TrendFilter"
    
    def test_log_trade_opened(self, tmp_path):
        """log_trade_opened should work correctly"""
        logger = StructuredLogger(
            "TestModule",
            log_dir=tmp_path,
            enable_console=False
        )
        
        log_trade_opened(
            logger,
            trade_id="E123",
            direction="LONG",
            entry_price=1.2345,
            stop_loss=1.2300,
            take_profit=1.2400
        )
        
        log_file = tmp_path / "testmodule.log"
        with open(log_file, 'r') as f:
            log_data = json.loads(f.readline())
        
        assert log_data["stage"] == "trade_execution"
        assert log_data["event"] == "trade_opened"
        assert log_data["trade_id"] == "E123"
        assert log_data["direction"] == "LONG"
    
    def test_log_trade_closed(self, tmp_path):
        """log_trade_closed should work correctly"""
        logger = StructuredLogger(
            "TestModule",
            log_dir=tmp_path,
            enable_console=False
        )
        
        log_trade_closed(
            logger,
            trade_id="E123",
            exit_reason="STOP_LOSS",
            pnl_points=-2.1
        )
        
        log_file = tmp_path / "testmodule.log"
        with open(log_file, 'r') as f:
            log_data = json.loads(f.readline())
        
        assert log_data["event"] == "trade_closed"
        assert log_data["trade_id"] == "E123"
        assert log_data["exit_reason"] == "STOP_LOSS"
        assert log_data["pnl_points"] == -2.1


class TestLogLevels:
    """Test different log levels"""
    
    def test_min_level_filtering(self, tmp_path):
        """Logger should respect minimum log level"""
        logger = StructuredLogger(
            "TestModule",
            log_dir=tmp_path,
            enable_console=False,
            min_level=LogLevel.WARNING
        )
        
        # Should not log (INFO < WARNING)
        logger.log_event(
            stage=LogStage.SIGNAL_GENERATION,
            event="info_event",
            level=LogLevel.INFO
        )
        
        # Should log (ERROR > WARNING)
        logger.log_event(
            stage=LogStage.SIGNAL_GENERATION,
            event="error_event",
            level=LogLevel.ERROR
        )
        
        log_file = tmp_path / "testmodule.log"
        with open(log_file, 'r') as f:
            lines = f.readlines()
        
        # Only ERROR should be logged
        assert len(lines) == 1
        log_data = json.loads(lines[0])
        assert log_data["event"] == "error_event"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])