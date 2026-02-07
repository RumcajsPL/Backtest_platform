"""
Report Generator v2
Aligned with ProgressiveTracker v2 and MetricsCalculator v2.
- Core mode: minimal JSON (pipeline/backtester)
- Debug mode: full JSON + progressive CSV
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List


class ReportGenerator:
    """
    ReportGenerator v2:
    - Core mode: minimal JSON for pipeline/backtester
    - Debug mode: full JSON + progressive CSV path
    """

    def __init__(self, config: Dict, project_root: Path):
        self.config = config
        self.project_root = project_root

    # ------------------------------------------------------------------
    # CSV GENERATION (debug only)
    # ------------------------------------------------------------------
    def generate_csv(self, trades: List[Dict], timestamp_str: str) -> Optional[Path]:
        """
        CSV generation intentionally disabled.
        ProgressiveTracker CSV replaces trade_details CSV.
        """
        return None

    # ------------------------------------------------------------------
    # JSON GENERATION
    # ------------------------------------------------------------------
    def generate_json(self, report_data: Dict, timestamp_str: str) -> Path:
        """Generate JSON report (debug mode only)."""

        out_cfg = self.config.get("output", {})
        report_dir = (
            self.project_root
            / out_cfg.get("outputs_dir", "outputs")
            / out_cfg.get("reports_dir", "reports/WBWS")
        )
        report_dir.mkdir(parents=True, exist_ok=True)

        report_path = report_dir / f"strategy_report_{timestamp_str}.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=4)

        return report_path

    # ------------------------------------------------------------------
    # BUILD REPORT DATA
    # ------------------------------------------------------------------
    def build_report_data(
        self,
        config: Dict,
        data_info: Dict,
        filter_stats: Dict,
        simulation_results: Dict,
        performance_metrics: Dict,
        csv_path: Optional[Path],
        mode: str = "debug",
    ) -> Dict:
        """
        Build report structure.
        mode:
            - "core": minimal output (pipeline/backtester)
            - "debug": full output (manual runs)
        """

        # Convert CSV path to relative (debug only)
        csv_relative = None
        if csv_path and csv_path.exists():
            try:
                csv_relative = str(csv_path.relative_to(self.project_root))
            except ValueError:
                csv_relative = str(csv_path)

        # Extract key metrics (core + debug)
        max_dd = performance_metrics.get(
            "max_drawdown_points",
            performance_metrics.get("drawdown_analysis", {}).get(
                "max_drawdown_points", 0
            ),
        )
        losing_streak = performance_metrics.get(
            "max_losing_streak",
            performance_metrics.get("losing_streak_analysis", {}).get(
                "max_losing_streak", 0
            ),
        )

        # ------------------------------------------------------------------
        # CORE MODE (minimal)
        # ------------------------------------------------------------------
        if mode == "core":
            return {
                "//_COMMENT": "=== CORE MODE: PIPELINE ESSENTIALS ONLY ===",
                "simulation_results": {
                    "performance_metrics": {
                        "total_trades": performance_metrics.get("total_trades", 0),
                        "winning_trades": performance_metrics.get(
                            "winning_trades", 0
                        ),
                        "win_rate": round(
                            performance_metrics.get("win_rate", 0), 2
                        ),
                        "total_pnl_points": round(
                            performance_metrics.get("total_pnl_points", 0), 2
                        ),
                        "expectancy_points": round(
                            performance_metrics.get("expectancy_points", 0), 2
                        ),
                        "profit_factor": round(
                            performance_metrics.get("profit_factor", 0), 2
                        ),
                        "avg_pnl_points": round(
                            performance_metrics.get("avg_pnl_points", 0), 2
                        ),
                        "largest_win": round(
                            performance_metrics.get("largest_win", 0), 2
                        ),
                        "largest_loss": round(
                            performance_metrics.get("largest_loss", 0), 2
                        ),
                        "max_drawdown": round(max_dd, 2),
                        "losing_streak": losing_streak,
                    },
                    "trade_summary": {
                        "trades_per_day": round(
                            len(simulation_results["closed_trades"])
                            / max(
                                (
                                    pd.to_datetime(data_info["date_range"][1])
                                    - pd.to_datetime(data_info["date_range"][0])
                                ).days
                                + 1,
                                1,
                            ),
                            2,
                        )
                        if data_info["date_range"]
                        else 0
                    },
                },
                "execution_time": datetime.now().isoformat(),
                "mode": "core",
            }

        # ------------------------------------------------------------------
        # DEBUG MODE (full report)
        # ------------------------------------------------------------------

        # Config section
        config_section = {
            "data_period": {
                "start": data_info.get("date_range", [None, None])[0],
                "end": data_info.get("date_range", [None, None])[1],
            },
            "indicator": config["indicator"]["name"],
            "htf_period": config["indicator"]["htf_period"],
            "time_filter": (
                {
                    "enabled": True,
                    "session": f"{config['trade_management']['time_filter']['session_start']['hour']:02d}:"
                    f"{config['trade_management']['time_filter']['session_start']['minute']:02d}-"
                    f"{config['trade_management']['time_filter']['session_end']['hour']:02d}:"
                    f"{config['trade_management']['time_filter']['session_end']['minute']:02d}",
                }
                if config["trade_management"]["time_filter"]["enabled"]
                else {"enabled": False}
            ),
            "technical_filters": config["filters"],
            "position_control": config["trade_management"].get(
                "position_control", {}
            ),
        }

        # Signal flow
        signal_flow_section = { 
            "step1_raw_signals": filter_stats["raw"], 
            "step2_time_filtered": filter_stats["time_filtered"], 
            "step3_technical_filtered": { 
                "buy": filter_stats["technical"]["buy"], 
                "sell": filter_stats["technical"]["sell"], 
                "total": filter_stats["technical"]["total"], 
                "rejected": filter_stats["technical"]["rejected"], 
            }, 
            "step4_risk_managed": simulation_results.get("risk_stats", {}), 
            "step5_position_managed": { 
                "closed_trades": len(simulation_results["closed_trades"]), 
                "rejected_buy": simulation_results["position_rejected_count"]["buy"], 
                "rejected_sell": simulation_results["position_rejected_count"]["sell"], 
                "exit_statistics": simulation_results["exit_stats"], 
                "trade_manager_metrics": simulation_results["trade_manager_metrics"], 
            }, 
        }            

        # Unused simulation details (debug only)
        simulation_results_unused = {
            "open_trades": len(simulation_results["open_trades"]),
            "rejected_signals": len(simulation_results["rejected_trades"]),
            "performance_metrics_details": {
                "exit_reasons": performance_metrics.get("exit_reasons", {}),
                "long_short_breakdown": performance_metrics.get(
                    "long_short_breakdown", {}
                ),
                "monthly_performance": performance_metrics.get(
                    "monthly_performance", {}
                ),
                "spread_analysis": performance_metrics.get(
                    "spread_analysis", {}
                ),
                "drawdown_analysis": performance_metrics.get(
                    "drawdown_analysis", {}
                ),
            },
        }

        # Outputs
        outputs_section = {
            "progressive_csv_file": csv_relative,
            "trades_csv_file": csv_relative,  # kept for compatibility
        }

        # Progressive tracking summary
        progressive_section = {
            "signal_progression_summary": filter_stats.get("progressive", {}),
            "total_signals_tracked": filter_stats.get("progressive", {}).get(
                "total_signals", 0
            ),
        }

        # Final report structure
        return {
            "//_COMMENT_PART1": "=== PART 1: REQUIRED FOR PIPELINE ===",
            "simulation_results": {
                "performance_metrics": performance_metrics,
                "trade_summary": {
                    "trades_per_day": round(
                        len(simulation_results["closed_trades"])
                        / max(
                            (
                                pd.to_datetime(data_info["date_range"][1])
                                - pd.to_datetime(data_info["date_range"][0])
                            ).days
                            + 1,
                            1,
                        ),
                        2,
                    )
                    if data_info["date_range"]
                    else 0
                },
            },
            "//_COMMENT_PART2": "=== PART 2: DEBUG MODE DETAILS ===",
            "execution_time": datetime.now().isoformat(),
            "config": config_section,
            "signal_flow": signal_flow_section,
            "simulation_results_unused": simulation_results_unused,
            "outputs": outputs_section,
            "progressive_tracking": progressive_section,
        }