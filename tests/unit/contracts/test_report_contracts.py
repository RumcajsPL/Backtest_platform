"""
Unit Tests for Report Contracts
=================================
Tests ReportConfig and GeneratedReport contracts.
"""

import pytest
import json
from pathlib import Path
from unittest.mock import Mock

from src.strategies.contracts.report_contracts import (
    ReportConfig,
    GeneratedReport
)


class TestReportConfig:
    """Tests for ReportConfig contract."""

    def test_default_config(self):
        """Test default report configuration."""
        config = ReportConfig()

        assert config.title == "Strategy Performance Report"
        assert config.brand_name == "WBWSStrategy"
        assert config.output_dir == Path("outputs/reports")
        assert config.include_raw_data is True
        assert config.theme == "dark"
        assert config.chart_height_px == 300
        assert config.subtitle is None

    def test_custom_config(self):
        """Test custom report configuration."""
        config = ReportConfig(
            title="Custom Report",
            brand_name="MyStrategy",
            output_dir=Path("/tmp/reports"),
            include_raw_data=False,
            theme="light",
            chart_height_px=400,
            subtitle="Test Subtitle"
        )

        assert config.title == "Custom Report"
        assert config.brand_name == "MyStrategy"
        assert config.output_dir == Path("/tmp/reports")
        assert config.include_raw_data is False
        assert config.theme == "light"
        assert config.chart_height_px == 400
        assert config.subtitle == "Test Subtitle"

    @pytest.mark.parametrize("invalid_theme", ["dark_mode", "light_mode", "blue", ""])
    def test_invalid_theme(self, invalid_theme):
        """Test that invalid theme raises error."""
        with pytest.raises(ValueError, match="theme must be one of"):
            ReportConfig(theme=invalid_theme)

    @pytest.mark.parametrize("invalid_height", [50, 900, 0, -100])
    def test_invalid_chart_height(self, invalid_height):
        """Test that chart height outside 100-800 raises error."""
        with pytest.raises(ValueError, match="chart_height_px must be 100–800"):
            ReportConfig(chart_height_px=invalid_height)

    def test_blank_brand_name(self):
        """Test that blank brand name raises error."""
        with pytest.raises(ValueError, match="brand_name must not be blank"):
            ReportConfig(brand_name="")

        with pytest.raises(ValueError, match="brand_name must not be blank"):
            ReportConfig(brand_name="   ")


class TestGeneratedReport:
    """Tests for GeneratedReport contract."""

    @pytest.fixture
    def mock_analytics_report(self):
        """Mock AnalyticsReport for testing."""
        mock = Mock()
        mock.analysis_timestamp = "2025-01-01T12:00:00"
        return mock

    def test_valid_generated_report(self, mock_analytics_report):
        """Test creating valid generated report."""
        report = GeneratedReport(
            html_path=Path("/tmp/report.html"),
            html_content="<html><body>Test</body></html>",
            generation_duration_ms=125.5,
            analytics_report=mock_analytics_report,
            layers_included=["executive", "analytical", "raw"]
        )

        assert report.html_path == Path("/tmp/report.html")
        assert report.html_content == "<html><body>Test</body></html>"
        assert report.generation_duration_ms == 125.5
        assert report.analytics_report == mock_analytics_report
        assert report.layers_included == ["executive", "analytical", "raw"]

    def test_minimal_layers(self, mock_analytics_report):
        """Test report with minimal layers."""
        report = GeneratedReport(
            html_path=Path("/tmp/report.html"),
            html_content="<html></html>",
            generation_duration_ms=50.0,
            analytics_report=mock_analytics_report,
            layers_included=["executive"]
        )

        assert report.layers_included == ["executive"]

    def test_to_dict(self, mock_analytics_report):
        """Test serialization to dict."""
        report = GeneratedReport(
            html_path=Path("/tmp/report.html"),
            html_content="<html></html>",
            generation_duration_ms=125.5,
            analytics_report=mock_analytics_report,
            layers_included=["executive", "analytical"]
        )

        d = report.to_dict()

        assert d["html_path"] == "/tmp/report.html"
        assert d["generation_duration_ms"] == 125.5
        assert d["layers_included"] == ["executive", "analytical"]
        assert d["analytics_timestamp"] == "2025-01-01T12:00:00"

    def test_to_json(self, mock_analytics_report):
        """Test JSON serialization."""
        report = GeneratedReport(
            html_path=Path("/tmp/report.html"),
            html_content="<html></html>",
            generation_duration_ms=125.5,
            analytics_report=mock_analytics_report,
            layers_included=["executive"]
        )

        json_str = report.to_json()
        data = json.loads(json_str)

        assert data["html_path"] == "/tmp/report.html"
        assert data["generation_duration_ms"] == 125.5
        assert data["layers_included"] == ["executive"]
        assert data["analytics_timestamp"] == "2025-01-01T12:00:00"