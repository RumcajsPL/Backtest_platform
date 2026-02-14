import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler

# ----------------------------------------------------------------------
# UTF‑8 console fix for Windows
# ----------------------------------------------------------------------
if sys.platform.startswith("win"):
    import os
    os.system("chcp 65001 > nul")
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

# ----------------------------------------------------------------------
# Ensure project root is on sys.path
# ----------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.paths import PROJECT_ROOT, LOGS_DIR

# ----------------------------------------------------------------------
# ROOT LOGGER CONFIGURATION
# ----------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

file_handler = RotatingFileHandler(
    LOGS_DIR / "wbws_strategy.log",
    maxBytes=10 * 1024 * 1024,
    backupCount=5,
    encoding="utf-8"
)
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.getLogger().addHandler(file_handler)

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Imports AFTER logging is configured
# ----------------------------------------------------------------------
from src.strategies.core.data_loader import DataLoader
from src.strategies.core.signal_generator import SignalGenerator
from src.strategies.core.filter_pipeline import FilterPipeline
from src.strategies.core.trade_simulator import TradeSimulator
from src.strategies.core.report_generator import ReportGenerator
from src.strategies.core.metrics_calculator import calculate_performance_metrics
from src.strategies.core.progressive_tracker import ProgressiveTracker
from src.strategies.core.null_progressive_tracker import NullProgressiveTracker


# ======================================================================
# MAIN STRATEGY RUNNER
# ======================================================================
def run_wbws_strategy(config_path: str, verbose: bool = False):
    logger.info("=" * 70)
    logger.info("WBWS STRATEGY WORKFLOW v2.3")
    logger.info("=" * 70)

    try:
        # ------------------------------------------------------------------
        # STEP 1 — LOAD CONFIG + DATA
        # ------------------------------------------------------------------
        logger.info("STEP 1: LOADING DATA")

        data_loader = DataLoader(config_path)
        config = data_loader.load_config()

        execution_mode = config.get("execution", {}).get("mode", "debug")
        is_core_mode = execution_mode == "core"

        # Configure output behavior
        if is_core_mode:
            logger.info("  CORE MODE: Optimized for pipeline performance")
            config.setdefault("output", {})
            config["output"]["enable_progressive_tracking"] = False
            config["output"]["enable_detailed_metrics"] = False
            config["output"]["save_signals_csv"] = False
            config["output"]["enable_cache_stats"] = False
            verbose = False
        else:
            logger.info("  DEBUG MODE: Full detailed tracking and outputs")
            config.setdefault("output", {})
            config["output"].setdefault("enable_progressive_tracking", True)
            config["output"].setdefault("enable_detailed_metrics", True)
            config["output"].setdefault("save_signals_csv", True)
            config["output"].setdefault("enable_cache_stats", True)

        df_full, df_strategy, df_htf, df_ltf, df_artf = data_loader.load_data()
        config["data"]["df_artf"] = df_artf  # Pass ARTF to config for RiskManager

        if df_ltf is None or df_ltf.empty:
            raise ValueError("LTF data missing or empty. Check config paths.")

        # Progressive tracker
        enable_tracking = config["output"].get("enable_progressive_tracking", not is_core_mode)
        if enable_tracking:
            progressive_tracker = ProgressiveTracker(config)
            logger.info("  Progressive tracking: ENABLED")
        else:
            progressive_tracker = NullProgressiveTracker(config)
            logger.info("  Progressive tracking: DISABLED")

        # Data info
        data_info = data_loader.get_data_info()
        logger.info(f"  Full dataset: {data_info['full_bars']:,} bars")
        logger.info(f"  Strategy period: {data_info['strategy_bars']:,} bars")
        logger.info(f"  HTF dataset: {data_info['htf_bars']:,} bars")
        logger.info(f"  LTF dataset: {data_info['ltf_bars']:,} bars")
        logger.info(f"  Date range: {data_info['date_range']}")

        validation = data_loader.validate_data()
        if not validation["is_valid"]:
            raise ValueError(f"Data validation failed: {validation}")

        # ------------------------------------------------------------------
        # STEP 2 — SIGNAL GENERATION
        # ------------------------------------------------------------------
        logger.info("STEP 2: GENERATING SIGNALS")

        sg = SignalGenerator(config["indicator"]["htf_period"])
        raw_signals, indicator_df = sg.generate_signals(df_strategy, df_htf)

        signal_stats = sg.get_signal_stats(raw_signals)
        logger.info(
            f"  Raw BUY: {signal_stats['buy']:,}, "
            f"SELL: {signal_stats['sell']:,}, "
            f"Total: {signal_stats['total']:,}"
        )

        # Build signal_id_map (debug only)
        signal_id_map = {}
        if enable_tracking:
            for ts, sig in raw_signals.dropna().items():
                mid_price = df_strategy.loc[ts, "close"]
                indicator_row = indicator_df.loc[ts] if ts in indicator_df.index else None
                signal_id_map[ts] = progressive_tracker.record_raw_signal(
                    timestamp=ts,
                    signal=sig,
                    mid_price=mid_price,
                    indicator_row=indicator_row
                )
        else:
            signal_id_map = {ts: 0 for ts in raw_signals.dropna().index}

        # ------------------------------------------------------------------
        # STEP 3 — FILTER PIPELINE (v4 unified architecture)
        # ------------------------------------------------------------------
        logger.info("STEP 3: APPLYING FILTERS")

        pipeline = FilterPipeline(config)
        filtered_signals, filter_stats = pipeline.apply_filters(df_strategy, raw_signals)

        logger.info(f"  Raw signals:        {filter_stats['raw']['total']:,}")
        logger.info(
            f"  Time filtered:      {filter_stats['time_filtered']['total']:,} "
            f"({filter_stats['time_filtered']['buy']:,} BUY, "
            f"{filter_stats['time_filtered']['sell']:,} SELL)"
        )
        logger.info(
            f"  Technical filtered: {filter_stats['technical']['total']:,} "
            f"({filter_stats['technical']['buy']:,} BUY, "
            f"{filter_stats['technical']['sell']:,} SELL)"
        )

        # ------------------------------------------------------------------
        # STEP 4 — TRADE SIMULATION
        # ------------------------------------------------------------------
        logger.info("STEP 4: SIMULATING TRADES")

        simulator = TradeSimulator(config, df_full)
        simulation_results = simulator.simulate_trades(
            df_strategy=df_strategy,
            filtered_signals=filtered_signals,
            verbose=verbose and not is_core_mode,
            progressive_tracker=progressive_tracker,
            signal_id_map=signal_id_map,
            df_ltf=df_ltf,
        )

        logger.info(
            f"  Closed trades: {len(simulation_results['closed_trades']):,}, "
            f"Open: {len(simulation_results['open_trades']):,}, "
            f"Rejected: {len(simulation_results['rejected_trades']):,}"
        )

        # ------------------------------------------------------------------
        # STEP 5 — PERFORMANCE METRICS
        # ------------------------------------------------------------------
        logger.info("STEP 5: CALCULATING METRICS")

        if simulation_results["all_trades"]:
            trades_df = pd.DataFrame(simulation_results["all_trades"])
            detailed = config["output"].get("enable_detailed_metrics", not is_core_mode)
            performance_metrics = calculate_performance_metrics(
                trades_df, df_strategy, detailed=detailed
            )
        else:
            performance_metrics = {"total_trades": 0, "message": "No trades executed"}

        logger.info(
            f"  Total P&L: {performance_metrics.get('total_pnl_points', 0):+.2f} pts | "
            f"Win rate: {performance_metrics.get('win_rate', 0):.2f}% | "
            f"Max DD: {performance_metrics.get('max_drawdown_points', 0):.2f} pts"
        )

        # ------------------------------------------------------------------
        # STEP 6 — OUTPUTS (debug only)
        # ------------------------------------------------------------------
        logger.info("STEP 6: GENERATING REPORTS")

        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_gen = ReportGenerator(config, PROJECT_ROOT)

        progressive_csv_path = None
        if enable_tracking and not is_core_mode:
            progressive_csv_path = progressive_tracker.save_to_csv(PROJECT_ROOT, timestamp_str)

        csv_path = None  # trade CSV disabled by design

        # Add progressive stats
        filter_stats["progressive"] = progressive_tracker.get_statistics()

        report_data = report_gen.build_report_data(
            config=config,
            data_info=data_info,
            filter_stats=filter_stats,
            simulation_results=simulation_results,
            performance_metrics=performance_metrics,
            csv_path=csv_path,
            mode=execution_mode,
        )

        if config["output"].get("enable_cache_stats", not is_core_mode):
            report_data.setdefault("validation", {})["data_loader_cache_stats"] = data_loader.get_cache_stats()

        json_path = report_gen.generate_json(report_data, timestamp_str)
        logger.info(f"  JSON saved: {json_path.relative_to(PROJECT_ROOT)}")

        logger.info("=" * 70)
        logger.info(f"EXECUTION COMPLETED ({execution_mode.upper()} MODE)")
        logger.info("=" * 70)

        return df_strategy, simulation_results["all_trades"], report_data

    except Exception as e:
        logger.error(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


# ----------------------------------------------------------------------
# CLI ENTRY POINT
# ----------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) > 1:
        verbose_flag = "--verbose" in sys.argv
        config_arg = sys.argv[1] if sys.argv[1] != "--verbose" else sys.argv[2]
        run_wbws_strategy(config_arg, verbose=verbose_flag)
    else:
        print("Usage: python scripts/runners/run_wbws_strategy.py <config_path> [--verbose]")
        sys.exit(1)