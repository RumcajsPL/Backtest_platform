"""
reproduce_crash.py
Reproduces the crash for exploration zone candidate #102 (index 1, seed=42).

Phase 1: Tests each indicator in isolation on full dataset — safe, no pipeline.
Phase 2: Runs full pipeline via StrategyConfig.from_yaml() + StrategyOrchestrator,
         exactly replicating what strategy_runner.py does in the backtester.

Usage:
    python scripts/diagnostics/reproduce_crash.py

Output: stdout + crash_reproduce.log in project root.
"""
import logging
import sys
import tempfile
from pathlib import Path

import pandas as pd
import yaml

# ── project root on path ──────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(ROOT / "crash_reproduce.log", mode="w"),
    ],
)
logger = logging.getLogger("reproduce_crash")

# ── exact parameters from candidate #102 (exploration zone, seed=42, N=100) ──
CANDIDATE = {
    "atr_length":      11,
    "atr_multiplier":  1.9,
    "rr_target":       4.6,
    "risk_percentile": 1.16,
    "dpo_length":      11,
    "dpo_smooth":      7,
    "dpo_threshold":   0.2,
    "macd_fast":       8,
    "macd_slow":       23,
    "macd_signal":     14,
    "cci_length":      20,
    "cci_overbought":  64,
    "cci_oversold":    -95,
}

# ── exact mapping from strategy_runner.py _PARAM_KEY_MAP ─────────────────────
_PARAM_KEY_MAP = {
    "dpo_length":       "filters.technical_filters.dpo_filter.length",
    "dpo_smooth":       "filters.technical_filters.dpo_filter.smooth",
    "dpo_threshold":    "filters.technical_filters.dpo_filter.threshold",
    "macd_fast":        "filters.technical_filters.macd_filter.fast_length",
    "macd_slow":        "filters.technical_filters.macd_filter.slow_length",
    "macd_signal":      "filters.technical_filters.macd_filter.signal_length",
    "cci_length":       "filters.technical_filters.cci_filter.length",
    "cci_overbought":   "filters.technical_filters.cci_filter.overbought",
    "cci_oversold":     "filters.technical_filters.cci_filter.oversold",
    "atr_length":       "trade_management.risk.atr_length",
    "atr_multiplier":   "trade_management.risk.atr_multiplier_sl",
    "rr_target":        "trade_management.risk.risk_to_reward_ratio",
    "risk_percentile":  "trade_management.risk.max_risk_percentile",
}

STRATEGY_YAML = ROOT / "configs/strategies/strategy_template.yaml"
DATA_15MIN    = ROOT / "data/processed/ohlcv/DEUIDXEUR_15min_20221201_20260301.parquet"


def _set_nested(d: dict, dotted_key: str, value) -> None:
    """Set a value in a nested dict using a dotted key path."""
    keys = dotted_key.split(".")
    for k in keys[:-1]:
        d = d[k]
    d[keys[-1]] = value


def build_patched_yaml(params: dict) -> dict:
    """
    Build a patched strategy config dict using the exact same logic as
    strategy_runner.py — apply _PARAM_KEY_MAP entries, then force-enable
    the filters used by this candidate.
    """
    cfg = yaml.safe_load(STRATEGY_YAML.read_text())

    # Apply parameter values via key map
    for param_name, yaml_key in _PARAM_KEY_MAP.items():
        if param_name in params:
            _set_nested(cfg, yaml_key, params[param_name])
            logger.debug("  SET %s = %s", yaml_key, params[param_name])

    # Force-enable filters used by exploration zone
    # (template has macd_filter.enabled=true and cci_filter.enabled=true already,
    #  but set explicitly to match backtester behaviour)
    cfg["filters"]["technical_filters"]["dpo_filter"]["enabled"]  = True
    cfg["filters"]["technical_filters"]["macd_filter"]["enabled"] = True
    cfg["filters"]["technical_filters"]["cci_filter"]["enabled"]  = True

    return cfg


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 1 — isolated indicator tests (no pipeline, no temp YAML)
# ─────────────────────────────────────────────────────────────────────────────

def phase1_isolated_tests(params: dict) -> None:
    logger.info("=" * 60)
    logger.info("PHASE 1 — Isolated indicator tests (no pipeline)")
    logger.info("=" * 60)

    logger.info("Loading 15min data...")
    df = pd.read_parquet(DATA_15MIN)
    logger.info("Data loaded: %d rows", len(df))

    import pandas_ta_classic as pta

    # ── DPO (no TA-Lib path) ──────────────────────────────────────────────
    logger.info("--- TEST 1: DPO (no TA-Lib) ---")
    logger.info("  dpo_length=%d  smooth=%d  threshold=%s",
                params["dpo_length"], params["dpo_smooth"], params["dpo_threshold"])
    try:
        dpo = pta.dpo(df["close"], length=params["dpo_length"], centered=False)
        logger.info("  OK — NaN=%d", dpo.isna().sum())
    except Exception as e:
        logger.error("  FAILED: %s", e, exc_info=True)

    # ── MACD via TA-Lib ───────────────────────────────────────────────────
    logger.info("--- TEST 2: MACD via TA-Lib (default) ---")
    logger.info("  fast=%d  slow=%d  signal=%d",
                params["macd_fast"], params["macd_slow"], params["macd_signal"])
    try:
        result = pta.macd(df["close"], fast=params["macd_fast"],
                          slow=params["macd_slow"], signal=params["macd_signal"])
        logger.info("  OK — shape=%s", result.shape if result is not None else None)
    except Exception as e:
        logger.error("  FAILED: %s", e, exc_info=True)

    # ── MACD talib=False ──────────────────────────────────────────────────
    logger.info("--- TEST 3: MACD talib=False ---")
    try:
        result = pta.macd(df["close"], fast=params["macd_fast"],
                          slow=params["macd_slow"], signal=params["macd_signal"],
                          talib=False)
        logger.info("  OK — shape=%s", result.shape if result is not None else None)
    except Exception as e:
        logger.error("  FAILED: %s", e, exc_info=True)

    # ── CCI via TA-Lib ────────────────────────────────────────────────────
    logger.info("--- TEST 4: CCI via TA-Lib (default) ---")
    logger.info("  length=%d  overbought=%d  oversold=%d",
                params["cci_length"], params["cci_overbought"], params["cci_oversold"])
    try:
        result = pta.cci(high=df["high"], low=df["low"], close=df["close"],
                         length=params["cci_length"])
        logger.info("  OK — NaN=%d", result.isna().sum())
    except Exception as e:
        logger.error("  FAILED: %s", e, exc_info=True)

    # ── CCI talib=False ───────────────────────────────────────────────────
    logger.info("--- TEST 5: CCI talib=False ---")
    try:
        result = pta.cci(high=df["high"], low=df["low"], close=df["close"],
                         length=params["cci_length"], talib=False)
        logger.info("  OK — NaN=%d", result.isna().sum())
    except Exception as e:
        logger.error("  FAILED: %s", e, exc_info=True)

    # ── MACD on short slices (WFO window simulation) ──────────────────────
    logger.info("--- TEST 6: MACD on short slices (WFO window simulation) ---")
    for n in [20, 30, 50, 100, 200, 500, 1000]:
        sl = df.tail(n)
        logger.info("  slice n=%d:", n)
        try:
            r = pta.macd(sl["close"], fast=params["macd_fast"],
                         slow=params["macd_slow"], signal=params["macd_signal"])
            logger.info("    TA-Lib OK — shape=%s", r.shape if r is not None else None)
        except Exception as e:
            logger.error("    TA-Lib FAILED at n=%d: %s", n, e, exc_info=True)
        try:
            r = pta.macd(sl["close"], fast=params["macd_fast"],
                         slow=params["macd_slow"], signal=params["macd_signal"],
                         talib=False)
            logger.info("    talib=False OK")
        except Exception as e:
            logger.error("    talib=False FAILED at n=%d: %s", n, e, exc_info=True)

    # ── CCI on short slices ───────────────────────────────────────────────
    logger.info("--- TEST 7: CCI on short slices ---")
    for n in [20, 30, 50, 100, 200]:
        sl = df.tail(n)
        logger.info("  slice n=%d:", n)
        try:
            r = pta.cci(high=sl["high"], low=sl["low"], close=sl["close"],
                        length=params["cci_length"])
            logger.info("    TA-Lib OK — NaN=%d", r.isna().sum())
        except Exception as e:
            logger.error("    TA-Lib FAILED at n=%d: %s", n, e, exc_info=True)

    logger.info("=" * 60)
    logger.info("PHASE 1 COMPLETE")
    logger.info("If you see this: raw indicator calls are safe on this machine.")
    logger.info("Crash must be inside the pipeline — proceed to Phase 2.")
    logger.info("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 2 — full pipeline via StrategyConfig + StrategyOrchestrator
# Exactly replicates strategy_runner.py behaviour
# ─────────────────────────────────────────────────────────────────────────────

def phase2_full_pipeline(params: dict) -> None:
    logger.info("=" * 60)
    logger.info("PHASE 2 — Full pipeline (StrategyConfig + StrategyOrchestrator)")
    logger.info("=" * 60)

    from src.strategies.config.config_schema import StrategyConfig
    from src.strategies.orchestrator import StrategyOrchestrator

    cfg = build_patched_yaml(params)

    # Write to temp YAML — same pattern as strategy_runner.py
    tmp_dir = ROOT / "temp" / "diagnostics"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = tmp_dir / "candidate_102_reproduce.yaml"
    tmp_path.write_text(yaml.dump(cfg, default_flow_style=False))
    logger.info("Temp YAML written: %s", tmp_path)

    try:
        logger.info("Loading StrategyConfig...")
        config = StrategyConfig.from_yaml(tmp_path)
        logger.info("StrategyConfig loaded OK")

        logger.info("Instantiating StrategyOrchestrator...")
        orch = StrategyOrchestrator(config)
        logger.info("StrategyOrchestrator instantiated OK")

        logger.info("Running pipeline (mode=core)...")
        result = orch.run()
        logger.info("Pipeline COMPLETED")
        logger.info("  total_trades = %s", getattr(result, "total_trades", "?"))
        logger.info("  win_rate     = %s", getattr(result, "win_rate", "?"))

    except Exception as e:
        logger.error("Pipeline FAILED: %s", e, exc_info=True)
    finally:
        # Keep temp YAML for inspection — do not delete
        logger.info("Temp YAML preserved at: %s", tmp_path)


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("reproduce_crash.py — candidate #102")
    logger.info("Parameters: %s", CANDIDATE)
    logger.info("")

    phase1_isolated_tests(CANDIDATE)

    logger.info("")
    logger.info("Proceeding to Phase 2...")
    logger.info("")

    phase2_full_pipeline(CANDIDATE)