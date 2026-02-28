"""Report Contracts — ReportGenerator input/output types.
Version: 1.0.0
Defines the data structures used for configuring report generation and representing the generated report output.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    from src.strategies.contracts.analytics_contracts import AnalyticsReport

# ============================================================
# REPORT CONFIGURATION
# ============================================================

@dataclass(frozen=True)
class ReportConfig:
    """Configuration for HTML report generation.

    Controls visual styling, output location, brand identity, and which
    layers of the report to include.
    """

    title: str = "Strategy Performance Report"
    brand_name: str = "Strategy Builder"        # Appears in header + footer
    output_dir: Path = Path("outputs/reports")
    include_raw_data: bool = True           # Layer 3 toggle
    theme: str = "dark"                     # "dark" | "light"
    chart_height_px: int = 300
    subtitle: Optional[str] = None          # Optional subtitle under main title

    def __post_init__(self) -> None:
        valid_themes = {"dark", "light"}
        if self.theme not in valid_themes:
            raise ValueError(f"theme must be one of {valid_themes}, got '{self.theme}'")
        if self.chart_height_px < 100 or self.chart_height_px > 800:
            raise ValueError(
                f"chart_height_px must be 100–800, got {self.chart_height_px}"
            )
        if not self.brand_name.strip():
            raise ValueError("brand_name must not be blank")

# ============================================================
# GENERATED REPORT OUTPUT
# ============================================================
@dataclass(frozen=True)
class GeneratedReport:
    """Output of ``ReportGenerator.generate()``.

    Contains both the saved file path and the full HTML content so callers
    can inspect or test the output without reading from disk.
    """

    html_path: Path                        # Where file was saved
    html_content: str                      # Full HTML string (for tests)
    generation_duration_ms: float
    analytics_report: "AnalyticsReport"   # Source data reference
    layers_included: List[str]            # ["executive", "analytical", "raw"]

    def to_dict(self) -> Dict:
        return {
            "html_path":               str(self.html_path),
            "generation_duration_ms":  round(self.generation_duration_ms, 2),
            "layers_included":         self.layers_included,
            "analytics_timestamp":     self.analytics_report.analysis_timestamp,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

# ============================================================
# MODULE METADATA
# ============================================================

__all__ = [
    "ReportConfig",
    "GeneratedReport",
]