"""
=============================================================================
ACT 0 — Layer 4 Diagnostic: Trade Simulation
=============================================================================
Purpose : Collect ground-truth facts on trade simulation for both Legacy
          and New pipelines. Covers: signals entering simulation, spread
          config as applied, risk filter (ATR, RAR, max_risk_percentile as
          used), position control defaults, trade-by-trade comparison,
          LTF coverage, and final trade counts.
Output  : outputs/diagnostics/layer4_trades.log
Run     : python tests/strategies/diagnostics/diag_layer4_trades.py
=============================================================================
"""

import logging
import sys
import importlib
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# Bootstrap — depth-independent project root resolution
# ---------------------------------------------------------------------------
_here = Path(__file__).resolve()
_candidate = _here.parent
for _ in range(10):
    if (_candidate / "src" / "utils" / "paths.py").exists():
        break
    _candidate = _candidate.parent
else:
    raise RuntimeError(f"Cannot locate project root from {_here}")

PROJECT_ROOT = _candidate
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.paths import CONFIGS_DIR, OUTPUTS_DIR

# ---------------------------------------------------------------------------
# Log setup
# ---------------------------------------------------------------------------
LOG_DIR = OUTPUTS_DIR / "diagnostics"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "layer4_trades.log"

file_handler = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s"))

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s"))

log = logging.getLogger("diag_layer4")
log.setLevel(logging.DEBUG)
log.addHandler(file_handler)
log.addHandler(console_handler)

# ---------------------------------------------------------------------------
# Config paths
# ---------------------------------------------------------------------------
LEGACY_CONFIG_PATH = CONFIGS_DIR / "strategies" / "wbws" / "wbws_strategy.yaml"
NEW_CONFIG_PATH    = CONFIGS_DIR / "strategies" / "strategy_template.yaml"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def section(title):
    log.info("=" * 70)
    log.info(f"  {title}")
    log.info("=" * 70)

def subsection(title):
    log.info("-" * 60)
    log.info(f"  {title}")
    log.info("-" * 60)

def fact(label, legacy_val, new_val, note=""):
    match = "MATCH  ✅" if str(legacy_val) == str(new_val) else "DIFFER ❌"
    log.info(f"  [{match}] {label}")
    log.debug(f"           Legacy : {legacy_val}")
    log.debug(f"           New    : {new_val}")
    if note:
        log.debug(f"           Note   : {note}")

# ===========================================================================
# SETUP — Run both pipelines through to filter output
# ===========================================================================
section("SETUP — Running both pipelines to filter output stage")

import pandas as pd
import yaml

# ── NEW pipeline ─────────────────────────────────────────────────────────────
log.info("  [NEW] Loading config → data → signals → filters...")
try:
    from src.config.config_schema import StrategyConfig
    from src.strategies.specific.modules.data_loader import DataLoader
    from src.strategies.specific.modules.signal_generator import SignalGenerator
    from src.strategies.specific.modules.filter_pipeline import FilterPipeline
    from src.strategies.specific.modules.trade_simulator import TradeSimulator

    new_config       = StrategyConfig.from_yaml(NEW_CONFIG_PATH)
    new_loader       = DataLoader(new_config)
    new_bundle       = new_loader.load_data()
    new_gen          = SignalGenerator(new_config)
    new_sf           = new_gen.generate_signals(new_bundle)
    new_fp           = FilterPipeline(new_config)
    new_fr           = new_fp.apply_filters(new_sf, new_bundle.strategy)
    new_final_signals = new_fr.final_signals.signals

    log.info(f"  [NEW] Setup OK | final signals: "
             f"BUY={int((new_final_signals==1).sum())} "
             f"SELL={int((new_final_signals==2).sum())} "
             f"TOTAL={int((new_final_signals!=0).sum())}")
except Exception as e:
    log.error(f"  [NEW] Setup FAILED: {e}", exc_info=True)
    sys.exit(1)

# ── LEGACY pipeline ──────────────────────────────────────────────────────────
log.info("  [LEGACY] Loading config → data → signals → filters...")
try:
    with open(LEGACY_CONFIG_PATH) as f:
        raw_legacy = yaml.safe_load(f)

    def _load_file(rel_path, date_start=None, date_end=None):
        p = PROJECT_ROOT / rel_path
        df = pd.read_parquet(p) if str(p).endswith(".parquet") else \
             pd.read_csv(p, parse_dates=["timestamp"], index_col="timestamp")
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        if date_start and date_end:
            df = df.loc[date_start:date_end]
        return df

    data_cfg     = raw_legacy["data"]
    l_start      = str(data_cfg["date_range"]["start"])
    l_end        = str(data_cfg["date_range"]["end"])
    leg_strategy = _load_file(data_cfg["file"],     l_start, l_end)
    leg_full     = _load_file(data_cfg["file"])
    leg_htf      = _load_file(data_cfg["file_htf"])
    leg_ltf      = _load_file(data_cfg["file_ltf"])
    leg_artf     = _load_file(data_cfg["file_artf"])

    leg_sg_mod   = importlib.import_module("src.strategies.core.signal_generator")
    leg_sg       = leg_sg_mod.SignalGenerator(raw_legacy)
    leg_sf       = leg_sg.generate_signals(leg_strategy, leg_htf)

    leg_fp_mod   = importlib.import_module("src.strategies.core.filter_pipeline")
    leg_fp       = leg_fp_mod.FilterPipeline(raw_legacy)

    # Run filter pipeline — probe both arg orders
    leg_fr = None
    for args in [(leg_sf, leg_strategy), (leg_strategy, leg_sf)]:
        try:
            leg_fr = leg_fp.apply_filters(*args)
            if leg_fr is not None:
                log.info(f"  [LEGACY] FilterPipeline succeeded with arg order: {[type(a).__name__ for a in args]}")
                break
        except Exception:
            continue

    # Extract legacy final signals
    leg_final_signals = None
    if leg_fr is not None:
        for attr in ["final_signals", "signals", "output_signals"]:
            fs = getattr(leg_fr, attr, None)
            if fs is not None:
                leg_final_signals = fs.signals if hasattr(fs, "signals") else fs
                if isinstance(leg_final_signals, pd.Series):
                    break

    # Fallback: reconstruct from confirmed pipeline run log counts
    # Legacy run log confirmed: time filtered=13, final=13 (from original log)
    # BUT Layer 3 showed legacy actually passes 14 — use that
    if leg_final_signals is None:
        log.warning("  [LEGACY] Could not extract final signal series from FilterPipeline")
        log.warning("  [LEGACY] Proceeding — legacy trade simulator will be called directly below")

    if leg_final_signals is not None:
        log.info(f"  [LEGACY] Setup OK | final signals: "
                 f"BUY={int((leg_final_signals==1).sum())} "
                 f"SELL={int((leg_final_signals==2).sum())} "
                 f"TOTAL={int((leg_final_signals!=0).sum())}")
    else:
        log.info("  [LEGACY] Signal series not captured — will run full legacy pipeline directly")

except Exception as e:
    log.error(f"  [LEGACY] Setup FAILED: {e}", exc_info=True)
    sys.exit(1)

# ===========================================================================
# BLOCK 1 — RISK CONFIG: How max_risk_percentile is interpreted in each pipeline
# ===========================================================================
section("BLOCK 1 — Risk Config: max_risk_percentile Interpretation")

subsection("NEW pipeline: RiskManager config as instantiated")
try:
    from src.strategies.specific.modules.risk_manager import RiskManager
    from src.strategies.core.cache_manager import CacheManager

    new_cm = CacheManager()
    new_rm = RiskManager(
        config=new_config,
        df_full=new_bundle.full,
        df_artf=new_bundle.artf,
        cache_manager=new_cm,
    )
    log.info(f"  [NEW] RiskManager instantiated OK")

    # Log every accessible config field on the risk config
    risk_cfg = new_config.trade_management.risk
    log.info(f"  [NEW] risk config full repr          : {risk_cfg}")
    log.info(f"  [NEW] risk.atr_length                : {risk_cfg.atr_length}")
    log.info(f"  [NEW] risk.atr_multiplier_sl         : {risk_cfg.atr_multiplier_sl}")
    log.info(f"  [NEW] risk.max_risk_percentile        : {risk_cfg.max_risk_percentile}")
    log.info(f"  [NEW] risk.max_risk_percentile type   : {type(risk_cfg.max_risk_percentile).__name__}")
    log.info(f"  [NEW] risk.tp_mode                   : {risk_cfg.tp_mode}")
    log.info(f"  [NEW] risk.risk_to_reward_ratio       : {risk_cfg.risk_to_reward_ratio}")

    # Access internal threshold as used — check if stored as fraction or percentage
    # The ARCHITECTURE doc says: "0.15 stands for 0.15%" so value IS the percentage
    # Compute what 0.1% of annual range means in points for context
    log.info(f"  [NEW] Interpretation: max_risk_percentile={risk_cfg.max_risk_percentile} "
             f"means SL cannot exceed {risk_cfg.max_risk_percentile}% of 12-month rolling annual range")

    # Try to access internal threshold representation if exposed
    for attr in ["_max_risk_pct", "_threshold", "_max_risk_percentile",
                 "max_risk_pct", "threshold"]:
        v = getattr(new_rm, attr, None)
        if v is not None:
            log.info(f"  [NEW] RiskManager internal attr .{attr} = {v}  type={type(v).__name__}")

    # Get risk summary if available (analytics mode populates it; core may not)
    try:
        summary = new_rm.get_risk_summary()
        log.info(f"  [NEW] RiskManager.get_risk_summary() = {summary}")
    except Exception as e2:
        log.info(f"  [NEW] get_risk_summary() not available at this stage: {e2}")

except Exception as e:
    log.error(f"  [NEW] RiskManager block FAILED: {e}", exc_info=True)

subsection("LEGACY pipeline: Risk config raw values")
try:
    leg_risk_raw = raw_legacy.get("trade_management", {}).get("risk_management", {})
    leg_sl_tp    = raw_legacy.get("trade_management", {}).get("sl_tp", {})
    log.info(f"  [LEGACY] risk_management block (raw)   : {leg_risk_raw}")
    log.info(f"  [LEGACY] sl_tp block (raw)             : {leg_sl_tp}")
    log.info(f"  [LEGACY] max_risk_percentile (raw)     : {leg_risk_raw.get('max_risk_percentile')}")
    log.info(f"  [LEGACY] max_risk_percentile type      : {type(leg_risk_raw.get('max_risk_percentile')).__name__}")
    log.info(f"  [LEGACY] allow_exceed_limit            : {leg_risk_raw.get('allow_exceed_limit')}")
    log.info(f"  [LEGACY] atr_length                    : {leg_sl_tp.get('atr_length')}")
    log.info(f"  [LEGACY] sl_multiplier                 : {leg_sl_tp.get('sl_multiplier')}")
    log.info(f"  [LEGACY] risk_to_reward_ratio          : {leg_sl_tp.get('risk_to_reward_ratio')}")

    # Attempt to instantiate legacy RiskManager to see internal value
    leg_rm = None
    for mod_path in ["src.strategies.core.risk_manager",
                     "src.strategies.specific.modules.risk_manager"]:
        try:
            mod = importlib.import_module(mod_path)
            if hasattr(mod, "RiskManager"):
                leg_rm = mod.RiskManager(raw_legacy, leg_full, leg_artf)
                log.info(f"  [LEGACY] RiskManager found at: {mod_path}")
                break
        except Exception as em:
            log.debug(f"  [LEGACY] {mod_path}: {em}")
            continue

    if leg_rm is not None:
        log.info(f"  [LEGACY] RiskManager instantiated OK")
        for attr in ["_max_risk_pct", "_threshold", "_max_risk_percentile",
                     "max_risk_pct", "threshold", "max_risk_percentile"]:
            v = getattr(leg_rm, attr, None)
            if v is not None:
                log.info(f"  [LEGACY] RiskManager internal attr .{attr} = {v}  type={type(v).__name__}")
        try:
            summary = leg_rm.get_risk_summary()
            log.info(f"  [LEGACY] get_risk_summary() = {summary}")
        except Exception as e2:
            log.debug(f"  [LEGACY] get_risk_summary(): {e2}")
    else:
        log.warning("  [LEGACY] RiskManager could not be instantiated standalone")

except Exception as e:
    log.error(f"  [LEGACY] Risk config block FAILED: {e}", exc_info=True)

# ===========================================================================
# BLOCK 2 — SPREAD CONFIG: apply_to_long/short defaults
# ===========================================================================
section("BLOCK 2 — Spread Config: apply_to_long / apply_to_short Defaults")

subsection("NEW pipeline: SpreadManager as instantiated")
try:
    from src.strategies.specific.modules.spread_manager import SpreadManager

    new_spread_cfg = new_config.trade_management.spread
    log.info(f"  [NEW] spread config full repr          : {new_spread_cfg}")
    log.info(f"  [NEW] spread.enabled                   : {new_spread_cfg.enabled}")
    log.info(f"  [NEW] spread.config_path               : {new_spread_cfg.config_path}")

    new_sm = SpreadManager(
        symbol=new_config.asset.symbol,
        config_path=str(PROJECT_ROOT / new_spread_cfg.config_path),
        cache_manager=new_cm,
    )
    log.info(f"  [NEW] SpreadManager instantiated OK")

    # Log global broker settings
    for attr in ["apply_to_long", "apply_to_short", "_apply_to_long", "_apply_to_short",
                 "global_settings", "_global_settings"]:
        v = getattr(new_sm, attr, None)
        if v is not None:
            log.info(f"  [NEW] SpreadManager.{attr} = {v}")

    # Log spread value for DEUIDXEUR
    try:
        spread_pts = new_sm.get_spread_points(price=24000.0)
        log.info(f"  [NEW] spread_points at price=24000 : {spread_pts}")
    except Exception as e2:
        log.debug(f"  [NEW] get_spread_points: {e2}")
    try:
        spread_val = new_sm.spread_value
        log.info(f"  [NEW] spread_value                 : {spread_val}")
    except Exception:
        pass
    try:
        spread_type = new_sm.spread_type
        log.info(f"  [NEW] spread_type                  : {spread_type}")
    except Exception:
        pass

    # Log full spread config loaded from broker file
    for attr in ["_config", "_spread_config", "config", "spread_config"]:
        v = getattr(new_sm, attr, None)
        if v is not None:
            log.info(f"  [NEW] SpreadManager.{attr} = {v}")
            break

except Exception as e:
    log.error(f"  [NEW] SpreadManager block FAILED: {e}", exc_info=True)

subsection("LEGACY pipeline: spread config raw + SpreadManager")
try:
    leg_spread_raw = raw_legacy.get("trade_management", {}).get("spread", {})
    log.info(f"  [LEGACY] spread block (raw)            : {leg_spread_raw}")
    log.info(f"  [LEGACY] spread.apply_to_long          : {leg_spread_raw.get('apply_to_long')}")
    log.info(f"  [LEGACY] spread.apply_to_short         : {leg_spread_raw.get('apply_to_short')}")
    log.info(f"  [LEGACY] spread.log_spread_impact      : {leg_spread_raw.get('log_spread_impact')}")

    # Attempt legacy SpreadManager
    for mod_path in ["src.strategies.core.spread_manager",
                     "src.strategies.specific.modules.spread_manager"]:
        try:
            mod = importlib.import_module(mod_path)
            if hasattr(mod, "SpreadManager"):
                leg_sm = mod.SpreadManager(raw_legacy)
                log.info(f"  [LEGACY] SpreadManager found at: {mod_path}")
                for attr in ["apply_to_long", "apply_to_short", "_apply_to_long",
                             "_apply_to_short", "spread_value", "spread_type"]:
                    v = getattr(leg_sm, attr, None)
                    if v is not None:
                        log.info(f"  [LEGACY] SpreadManager.{attr} = {v}")
                break
        except Exception as em:
            log.debug(f"  [LEGACY] {mod_path}: {em}")
            continue

except Exception as e:
    log.error(f"  [LEGACY] Spread block FAILED: {e}", exc_info=True)

# ===========================================================================
# BLOCK 3 — POSITION CONTROL: max_positions default in legacy
# ===========================================================================
section("BLOCK 3 — Position Control: max_positions Default")

subsection("NEW pipeline: TradeManager config")
try:
    from src.strategies.specific.modules.trade_manager import TradeManager

    new_tm = TradeManager(new_config)
    pos_cfg = new_config.trade_management.position_control
    log.info(f"  [NEW] position_control full repr       : {pos_cfg}")
    log.info(f"  [NEW] max_positions                    : {pos_cfg.max_positions}")
    log.info(f"  [NEW] pyramiding_enabled               : {pos_cfg.pyramiding_enabled}")
    log.info(f"  [NEW] close_on_opposite                : {pos_cfg.close_on_opposite}")

    for attr in ["_max_positions", "max_positions", "_max_concurrent"]:
        v = getattr(new_tm, attr, None)
        if v is not None:
            log.info(f"  [NEW] TradeManager.{attr} = {v}")

except Exception as e:
    log.error(f"  [NEW] TradeManager block FAILED: {e}", exc_info=True)

subsection("LEGACY pipeline: TradeManager config")
try:
    leg_pos_raw = raw_legacy.get("trade_management", {}).get("position_control", {})
    log.info(f"  [LEGACY] position_control block (raw)  : {leg_pos_raw}")
    log.info(f"  [LEGACY] max_positions (raw)           : {leg_pos_raw.get('max_positions', 'NOT SET')}")
    log.info(f"  [LEGACY] pyramiding_enabled            : {leg_pos_raw.get('pyramiding_enabled')}")
    log.info(f"  [LEGACY] close_on_opposite             : {leg_pos_raw.get('close_on_opposite')}")

    for mod_path in ["src.strategies.core.trade_manager",
                     "src.strategies.specific.modules.trade_manager"]:
        try:
            mod = importlib.import_module(mod_path)
            if hasattr(mod, "TradeManager"):
                leg_tm = mod.TradeManager(raw_legacy)
                log.info(f"  [LEGACY] TradeManager found at: {mod_path}")
                for attr in ["_max_positions", "max_positions", "_max_concurrent",
                             "max_concurrent_trades"]:
                    v = getattr(leg_tm, attr, None)
                    if v is not None:
                        log.info(f"  [LEGACY] TradeManager.{attr} = {v}")
                log.debug(f"  [LEGACY] TradeManager full repr: {vars(leg_tm) if hasattr(leg_tm,'__dict__') else leg_tm}")
                break
        except Exception as em:
            log.debug(f"  [LEGACY] {mod_path}: {em}")
            continue

except Exception as e:
    log.error(f"  [LEGACY] Position control block FAILED: {e}", exc_info=True)

# ===========================================================================
# BLOCK 4 — ATR and RAR: values at signal bars
# ===========================================================================
section("BLOCK 4 — ATR and Annual Range (RAR) at Signal Bars")

subsection("NEW pipeline: ATR and RAR at each filtered signal bar")
try:
    import numpy as np

    # Get signal timestamps entering simulation
    new_sim_ts = new_fr.final_signals.signals
    new_signal_ts = new_sim_ts.index[new_sim_ts != 0]

    # Compute ATR on full dataset (as RiskManager does)
    try:
        import pandas_ta_classic as ta
        atr_length = new_config.trade_management.risk.atr_length
        new_atr = ta.atr(
            new_bundle.full["high"],
            new_bundle.full["low"],
            new_bundle.full["close"],
            length=atr_length,
        )
        log.info(f"  [NEW] ATR computed (pandas_ta, length={atr_length})")
    except ImportError:
        try:
            import talib
            atr_length = new_config.trade_management.risk.atr_length
            new_atr = pd.Series(
                talib.ATR(
                    new_bundle.full["high"].values,
                    new_bundle.full["low"].values,
                    new_bundle.full["close"].values,
                    timeperiod=atr_length,
                ),
                index=new_bundle.full.index,
            )
            log.info(f"  [NEW] ATR computed (talib, length={atr_length})")
        except ImportError:
            new_atr = None
            log.warning("  [NEW] Neither pandas_ta nor talib — ATR cannot be computed in diagnostic")

    # Compute RAR from ARTF (rolling 12-month annual range)
    # RAR = max(high) - min(low) over rolling 12 ARTF bars
    artf = new_bundle.artf
    artf_rolling_high = artf["high"].rolling(12).max()
    artf_rolling_low  = artf["low"].rolling(12).min()
    rar_series = artf_rolling_high - artf_rolling_low
    log.info(f"  [NEW] RAR series (12-month rolling): non-null={rar_series.notna().sum()} of {len(rar_series)}")
    log.debug(f"  [NEW] RAR series full:\n{rar_series.to_string()}")

    # At each signal bar, find the applicable ARTF month and RAR value
    log.info(f"  [NEW] ATR + RAR at each signal bar entering simulation ({len(new_signal_ts)}):")
    atr_multiplier = new_config.trade_management.risk.atr_multiplier_sl
    max_risk_pct   = new_config.trade_management.risk.max_risk_percentile

    for ts in new_signal_ts:
        val = int(new_sim_ts.loc[ts])
        direction = "BUY " if val == 1 else "SELL"

        # ATR at this bar
        atr_val = None
        if new_atr is not None and ts in new_atr.index and pd.notna(new_atr.loc[ts]):
            atr_val = round(float(new_atr.loc[ts]), 4)
        atr_sl = round(atr_val * atr_multiplier, 4) if atr_val else None

        # RAR: find the ARTF bar for the month of this signal
        ts_month_end = artf.index[artf.index <= ts]
        rar_val = None
        rar_ts  = None
        if len(ts_month_end) > 0:
            rar_ts  = ts_month_end[-1]
            if rar_ts in rar_series.index and pd.notna(rar_series.loc[rar_ts]):
                rar_val = round(float(rar_series.loc[rar_ts]), 4)

        # Threshold in points
        threshold_pts = round(rar_val * max_risk_pct / 100, 4) if rar_val else None

        # Risk decision
        if atr_sl is not None and threshold_pts is not None:
            passes = atr_sl <= threshold_pts
            decision = "PASS" if passes else "REJECT"
        else:
            decision = "UNKNOWN"

        log.info(
            f"  [NEW] {ts} {direction} | "
            f"ATR={atr_val} | ATR×{atr_multiplier}={atr_sl} | "
            f"RAR_ts={rar_ts} RAR={rar_val} | "
            f"threshold={max_risk_pct}%×RAR={threshold_pts} | "
            f"risk_decision={decision}"
        )

except Exception as e:
    log.error(f"  [NEW] ATR/RAR block FAILED: {e}", exc_info=True)

subsection("LEGACY pipeline: ATR and RAR at each signal bar")
try:
    leg_atr_length = raw_legacy.get("trade_management", {}).get("sl_tp", {}).get("atr_length", 14)
    leg_atr_mult   = raw_legacy.get("trade_management", {}).get("sl_tp", {}).get("sl_multiplier", 1.4)
    leg_max_risk   = raw_legacy.get("trade_management", {}).get("risk_management", {}).get("max_risk_percentile", 0.001)

    log.info(f"  [LEGACY] atr_length={leg_atr_length}  sl_multiplier={leg_atr_mult}  max_risk_percentile={leg_max_risk}")

    # Compute ATR on legacy full dataset
    try:
        import pandas_ta_classic as ta
        leg_atr = ta.atr(leg_full["high"], leg_full["low"], leg_full["close"], length=leg_atr_length)
        log.info(f"  [LEGACY] ATR computed (pandas_ta, length={leg_atr_length})")
    except ImportError:
        try:
            import talib
            leg_atr = pd.Series(
                talib.ATR(leg_full["high"].values, leg_full["low"].values,
                          leg_full["close"].values, timeperiod=leg_atr_length),
                index=leg_full.index,
            )
            log.info(f"  [LEGACY] ATR computed (talib, length={leg_atr_length})")
        except ImportError:
            leg_atr = None
            log.warning("  [LEGACY] ATR cannot be computed")

    # RAR from ARTF
    leg_rar_high = leg_artf["high"].rolling(12).max()
    leg_rar_low  = leg_artf["low"].rolling(12).min()
    leg_rar      = leg_rar_high - leg_rar_low
    log.debug(f"  [LEGACY] RAR series full:\n{leg_rar.to_string()}")

    # Legacy signal timestamps — use confirmed list (14 signals from Layer 3)
    # Reconstruct from manual time filter audit: all 19 raw minus 5 removed
    removed_by_legacy = [
        pd.Timestamp("2025-12-12 20:36:00"),
        pd.Timestamp("2025-12-12 20:38:00"),
        pd.Timestamp("2025-12-12 20:48:00"),
        pd.Timestamp("2025-12-12 20:53:00"),
        pd.Timestamp("2025-12-12 20:57:00"),
    ]
    all_raw = new_sf.signals.index[new_sf.signals != 0]  # same raw signals confirmed
    leg_sim_ts = [ts for ts in all_raw if ts not in removed_by_legacy]
    log.info(f"  [LEGACY] Signal timestamps entering simulation ({len(leg_sim_ts)}):")
    for ts in leg_sim_ts:
        log.info(f"           {ts}")

    log.info(f"  [LEGACY] ATR + RAR at each signal bar ({len(leg_sim_ts)}):")
    for ts in leg_sim_ts:
        val = int(new_sf.signals.loc[ts])  # same signal directions confirmed
        direction = "BUY " if val == 1 else "SELL"

        atr_val = None
        if leg_atr is not None and ts in leg_atr.index and pd.notna(leg_atr.loc[ts]):
            atr_val = round(float(leg_atr.loc[ts]), 4)
        atr_sl = round(atr_val * leg_atr_mult, 4) if atr_val else None

        rar_ts_before = leg_artf.index[leg_artf.index <= ts]
        rar_val = None
        rar_ts  = None
        if len(rar_ts_before) > 0:
            rar_ts = rar_ts_before[-1]
            if rar_ts in leg_rar.index and pd.notna(leg_rar.loc[rar_ts]):
                rar_val = round(float(leg_rar.loc[rar_ts]), 4)

        threshold_pts = round(rar_val * leg_max_risk / 100, 4) if rar_val else None

        if atr_sl is not None and threshold_pts is not None:
            passes = atr_sl <= threshold_pts
            decision = "PASS" if passes else "REJECT"
        else:
            decision = "UNKNOWN"

        log.info(
            f"  [LEGACY] {ts} {direction} | "
            f"ATR={atr_val} | ATR×{leg_atr_mult}={atr_sl} | "
            f"RAR_ts={rar_ts} RAR={rar_val} | "
            f"threshold={leg_max_risk}%×RAR={threshold_pts} | "
            f"risk_decision={decision}"
        )

except Exception as e:
    log.error(f"  [LEGACY] ATR/RAR block FAILED: {e}", exc_info=True)

# ===========================================================================
# BLOCK 5 — RUN NEW TradeSimulator and capture full TradeResult
# ===========================================================================
section("BLOCK 5 — NEW PIPELINE: TradeSimulator Full Run")

new_trade_result = None
try:
    new_sim = TradeSimulator(new_config, new_bundle.full, cache_manager=new_cm)
    new_trade_result = new_sim.simulate_trades(
        df_strategy=new_bundle.strategy,
        signal_frame=new_fr.final_signals,
        df_ltf=new_bundle.ltf,
    )

    log.info(f"  [NEW] TradeResult:")
    log.info(f"  [NEW]   total_entries     : {new_trade_result.total_entries}")
    log.info(f"  [NEW]   total_opened      : {new_trade_result.total_opened}")
    log.info(f"  [NEW]   total_closed      : {new_trade_result.total_closed}")
    log.info(f"  [NEW]   total_rejected    : {new_trade_result.total_rejected}")
    log.info(f"  [NEW]   currently_open    : {new_trade_result.currently_open}")
    log.info(f"  [NEW]   win_count         : {new_trade_result.win_count}")
    log.info(f"  [NEW]   loss_count        : {new_trade_result.loss_count}")
    log.info(f"  [NEW]   win_rate          : {new_trade_result.win_rate}")
    log.info(f"  [NEW]   total_pnl_points  : {new_trade_result.total_pnl_points}")
    log.info(f"  [NEW]   risk_approved     : {new_trade_result.risk_approved}")
    log.info(f"  [NEW]   risk_rejected     : {new_trade_result.risk_rejected}")
    log.info(f"  [NEW]   exits_by_reason   : {new_trade_result.exits_by_reason}")
    log.info(f"  [NEW]   position_rejected : {new_trade_result.position_rejected}")
    log.info(f"  [NEW]   execution_mode    : {new_trade_result.execution_mode}")

    subsection("NEW: Trade-by-trade detail")
    for i, trade in enumerate(new_trade_result.trades):
        log.info(f"  [NEW] Trade {i+1}:")
        for attr in ["entry_time", "exit_time", "direction", "entry_price",
                     "exit_price", "stop_loss", "take_profit", "pnl_points",
                     "exit_reason", "entry_price_executed", "stop_loss_trigger",
                     "take_profit_trigger"]:
            v = getattr(trade, attr, None)
            if v is not None:
                log.info(f"           {attr:<28}: {v}")

    subsection("NEW: Rejected signal detail")
    for i, rej in enumerate(new_trade_result.rejected_signals):
        log.info(f"  [NEW] Rejected signal {i+1}:")
        for attr in ["timestamp", "direction", "reason", "signal_value"]:
            v = getattr(rej, attr, None)
            if v is not None:
                log.info(f"           {attr:<28}: {v}")

except Exception as e:
    log.error(f"  [NEW] TradeSimulator FAILED: {e}", exc_info=True)

# ===========================================================================
# BLOCK 6 — RUN LEGACY TradeSimulator and capture full output
# ===========================================================================
section("BLOCK 6 — LEGACY PIPELINE: TradeSimulator Full Run")

leg_trade_result = None
try:
    # Locate legacy trade simulator
    leg_sim_mod = None
    for mod_path in ["src.strategies.core.trade_simulator",
                     "src.strategies.specific.modules.trade_simulator"]:
        try:
            mod = importlib.import_module(mod_path)
            if hasattr(mod, "TradeSimulator"):
                leg_sim_mod = mod
                log.info(f"  [LEGACY] TradeSimulator found at: {mod_path}")
                break
        except ImportError:
            continue

    if leg_sim_mod is None:
        log.warning("  [LEGACY] TradeSimulator not found via import")
    else:
        LegacyTS = leg_sim_mod.TradeSimulator
        # Try common instantiation signatures
        leg_sim = None
        for init_args in [
            (raw_legacy, leg_full),
            (raw_legacy,),
            (raw_legacy, leg_full, leg_artf),
        ]:
            try:
                leg_sim = LegacyTS(*init_args)
                log.info(f"  [LEGACY] TradeSimulator instantiated with {len(init_args)} args")
                break
            except Exception as em:
                log.debug(f"  [LEGACY] Init with {len(init_args)} args failed: {em}")
                continue

        if leg_sim is not None:
            # Try simulate_trades with common signatures
            for sim_args in [
                {"df_strategy": leg_strategy, "signal_frame": leg_sf,
                 "df_ltf": leg_ltf, "df_full": leg_full},
                {"df_strategy": leg_strategy, "signal_frame": leg_sf, "df_ltf": leg_ltf},
                {"df_strategy": leg_strategy, "signal_frame": leg_sf},
                (leg_strategy, leg_sf, leg_ltf),
                (leg_strategy, leg_sf),
            ]:
                try:
                    if isinstance(sim_args, dict):
                        leg_trade_result = leg_sim.simulate_trades(**sim_args)
                    else:
                        leg_trade_result = leg_sim.simulate_trades(*sim_args)
                    log.info(f"  [LEGACY] simulate_trades succeeded")
                    break
                except Exception as em:
                    log.debug(f"  [LEGACY] simulate_trades attempt failed: {em}")
                    continue

    if leg_trade_result is not None:
        log.info(f"  [LEGACY] TradeResult type: {type(leg_trade_result).__name__}")

        # Log counts — probe both attribute names (legacy may differ)
        for attr in ["total_entries", "input_count", "entries"]:
            v = getattr(leg_trade_result, attr, None)
            if v is not None:
                log.info(f"  [LEGACY]   {attr:<24}: {v}")

        for attr in ["total_opened", "opened_count", "trades_opened"]:
            v = getattr(leg_trade_result, attr, None)
            if v is not None:
                log.info(f"  [LEGACY]   {attr:<24}: {v}")

        for attr in ["total_closed", "closed_count", "trades_closed"]:
            v = getattr(leg_trade_result, attr, None)
            if v is not None:
                log.info(f"  [LEGACY]   {attr:<24}: {v}")

        for attr in ["total_rejected", "rejected_count", "signals_rejected"]:
            v = getattr(leg_trade_result, attr, None)
            if v is not None:
                log.info(f"  [LEGACY]   {attr:<24}: {v}")

        for attr in ["win_count", "wins"]:
            v = getattr(leg_trade_result, attr, None)
            if v is not None:
                log.info(f"  [LEGACY]   {attr:<24}: {v}")

        for attr in ["loss_count", "losses"]:
            v = getattr(leg_trade_result, attr, None)
            if v is not None:
                log.info(f"  [LEGACY]   {attr:<24}: {v}")

        for attr in ["total_pnl_points", "total_pnl", "pnl_points", "pnl"]:
            v = getattr(leg_trade_result, attr, None)
            if v is not None:
                log.info(f"  [LEGACY]   {attr:<24}: {v}")

        for attr in ["risk_approved", "risk_rejected", "exits_by_reason",
                     "position_rejected", "rejection_reasons"]:
            v = getattr(leg_trade_result, attr, None)
            if v is not None:
                log.info(f"  [LEGACY]   {attr:<24}: {v}")

        # Full repr for any missed fields
        log.debug(f"  [LEGACY] TradeResult full repr: {leg_trade_result}")

        # Trade-by-trade detail
        subsection("LEGACY: Trade-by-trade detail")
        trades_attr = None
        for attr in ["trades", "trade_list", "completed_trades"]:
            trades_attr = getattr(leg_trade_result, attr, None)
            if trades_attr:
                break

        if trades_attr:
            for i, trade in enumerate(trades_attr):
                log.info(f"  [LEGACY] Trade {i+1}:")
                for attr in ["entry_time", "exit_time", "direction", "entry_price",
                             "exit_price", "stop_loss", "take_profit", "pnl_points",
                             "pnl", "exit_reason"]:
                    v = getattr(trade, attr, None)
                    if v is None and isinstance(trade, dict):
                        v = trade.get(attr)
                    if v is not None:
                        log.info(f"           {attr:<28}: {v}")
                log.debug(f"           full repr: {trade}")

        # Rejected signals detail
        subsection("LEGACY: Rejected signal detail")
        rej_attr = None
        for attr in ["rejected_signals", "rejections", "rejected"]:
            rej_attr = getattr(leg_trade_result, attr, None)
            if rej_attr:
                break

        if rej_attr:
            for i, rej in enumerate(rej_attr):
                log.info(f"  [LEGACY] Rejected {i+1}: {rej}")

    else:
        log.warning("  [LEGACY] TradeSimulator could not be run — using pipeline run log as reference")
        log.info("  [LEGACY] From pipeline run log (confirmed):")
        log.info("  [LEGACY]   Closed trades : 2")
        log.info("  [LEGACY]   Open trades   : 0")
        log.info("  [LEGACY]   Rejected      : 11")
        log.info("  [LEGACY]   Total P&L     : +20.91 pts")
        log.info("  [LEGACY]   Win rate      : 50.00%")

except Exception as e:
    log.error(f"  [LEGACY] TradeSimulator block FAILED: {e}", exc_info=True)

# ===========================================================================
# BLOCK 7 — LTF COVERAGE: does full vs sliced LTF affect exit precision?
# ===========================================================================
section("BLOCK 7 — LTF Coverage: Full File vs Window Slice")

try:
    log.info(f"  [NEW]    ltf shape: {new_bundle.ltf.shape}  "
             f"first={new_bundle.ltf.index[0]}  last={new_bundle.ltf.index[-1]}")
    log.info(f"  [LEGACY] ltf shape: {leg_ltf.shape}  "
             f"first={leg_ltf.index[0]}  last={leg_ltf.index[-1]}")

    # Slice legacy LTF to same window as New
    leg_ltf_window = leg_ltf.loc[
        new_bundle.ltf.index[0]:new_bundle.ltf.index[-1]
    ]
    log.info(f"  [LEGACY] ltf sliced to window: {leg_ltf_window.shape}")

    # Compare row counts and first/last ticks in the window
    fact("ltf window row count",    len(leg_ltf_window),  len(new_bundle.ltf))
    fact("ltf window first ts",     str(leg_ltf_window.index[0]),  str(new_bundle.ltf.index[0]))
    fact("ltf window last ts",      str(leg_ltf_window.index[-1]), str(new_bundle.ltf.index[-1]))
    fact("ltf window open[0]",
         round(float(leg_ltf_window["open"].iloc[0]),  6),
         round(float(new_bundle.ltf["open"].iloc[0]),  6))
    fact("ltf window close[-1]",
         round(float(leg_ltf_window["close"].iloc[-1]), 6),
         round(float(new_bundle.ltf["close"].iloc[-1]), 6))

    # Check if any trade exit timestamps fall outside new LTF window
    if new_trade_result is not None:
        subsection("LTF coverage check: exit timestamps vs LTF range")
        ltf_start = new_bundle.ltf.index[0]
        ltf_end   = new_bundle.ltf.index[-1]
        for i, trade in enumerate(new_trade_result.trades):
            exit_ts = getattr(trade, "exit_time", None)
            if exit_ts is not None:
                in_ltf = ltf_start <= pd.Timestamp(exit_ts) <= ltf_end
                log.info(f"  [NEW] Trade {i+1} exit={exit_ts}  in_ltf_window={in_ltf}")

except Exception as e:
    log.error(f"  LTF coverage block FAILED: {e}", exc_info=True)

# ===========================================================================
# BLOCK 8 — DIRECT COMPARISON SUMMARY
# ===========================================================================
section("BLOCK 8 — Direct Comparison: Trade Simulation Results")

try:
    subsection("Trade counts")

    # New actuals
    n_opened   = new_trade_result.total_opened   if new_trade_result else "N/A"
    n_closed   = new_trade_result.total_closed   if new_trade_result else "N/A"
    n_rejected = new_trade_result.total_rejected if new_trade_result else "N/A"
    n_risk_rej = new_trade_result.risk_rejected  if new_trade_result else "N/A"
    n_pnl      = new_trade_result.total_pnl_points if new_trade_result else "N/A"
    n_wins     = new_trade_result.win_count      if new_trade_result else "N/A"

    # Legacy actuals (from result or from run log fallback)
    if leg_trade_result is not None:
        l_opened   = getattr(leg_trade_result, "total_opened",   getattr(leg_trade_result, "opened_count",   "N/A"))
        l_closed   = getattr(leg_trade_result, "total_closed",   getattr(leg_trade_result, "closed_count",   "N/A"))
        l_rejected = getattr(leg_trade_result, "total_rejected", getattr(leg_trade_result, "rejected_count", "N/A"))
        l_risk_rej = getattr(leg_trade_result, "risk_rejected",  "N/A")
        l_pnl      = getattr(leg_trade_result, "total_pnl_points", getattr(leg_trade_result, "total_pnl", "N/A"))
        l_wins     = getattr(leg_trade_result, "win_count",      getattr(leg_trade_result, "wins", "N/A"))
    else:
        # Fallback from pipeline run log
        l_opened, l_closed, l_rejected, l_risk_rej, l_pnl, l_wins = 2, 2, 11, "N/A", 20.91, 1

    fact("signals entering simulation", 14, 13,
         note="Legacy=14 (20:30 passes), New=13 (20:30 rejected) — confirmed Layer 3")
    fact("trades opened",    l_opened,   n_opened)
    fact("trades closed",    l_closed,   n_closed)
    fact("total rejected",   l_rejected, n_rejected)
    fact("risk rejected",    l_risk_rej, n_risk_rej)
    fact("win count",        l_wins,     n_wins)
    fact("total pnl points", l_pnl,      n_pnl)

    subsection("Risk filter")
    fact("max_risk_percentile (raw YAML)",
         raw_legacy["trade_management"]["risk_management"]["max_risk_percentile"],
         new_config.trade_management.risk.max_risk_percentile)

    subsection("Position control")
    leg_max_pos = raw_legacy.get("trade_management", {}).get("position_control", {}).get("max_positions", "NOT SET")
    new_max_pos = new_config.trade_management.position_control.max_positions
    fact("max_positions", leg_max_pos, new_max_pos)

except Exception as e:
    log.error(f"  Comparison block FAILED: {e}", exc_info=True)

# ===========================================================================
# BLOCK 9 — SUMMARY TABLE
# ===========================================================================
section("BLOCK 9 — Layer 4 Summary Table")

try:
    log.info("  ┌─────────────────────────────────────────────┬──────────┬──────────┬──────────┐")
    log.info("  │ Fact                                        │  Legacy  │   New    │  Status  │")
    log.info("  ├─────────────────────────────────────────────┼──────────┼──────────┼──────────┤")

    def row(label, lv, nv):
        status = "✅ MATCH " if str(lv) == str(nv) else "❌ DIFFER"
        log.info(f"  │ {label:<43} │ {str(lv):<8} │ {str(nv):<8} │ {status} │")

    row("Signals entering simulation",    14,        13)
    row("Trades opened",                  l_opened,  n_opened)
    row("Trades closed",                  l_closed,  n_closed)
    row("Total rejected",                 l_rejected,n_rejected)
    row("Risk rejected",                  l_risk_rej,n_risk_rej)
    row("Win count",                      l_wins,    n_wins)
    row("Total PnL (pts)",                l_pnl,     n_pnl)
    row("max_risk_percentile (raw)",
        raw_legacy["trade_management"]["risk_management"]["max_risk_percentile"],
        new_config.trade_management.risk.max_risk_percentile)
    row("max_positions",                  leg_max_pos, new_max_pos)

    log.info("  └─────────────────────────────────────────────┴──────────┴──────────┴──────────┘")

except Exception as e:
    log.warning(f"  Summary table incomplete: {e}")

# ===========================================================================
# DONE
# ===========================================================================
section("DIAGNOSTIC COMPLETE")
log.info(f"  Log file : {LOG_FILE}")
log.info(f"  Timestamp: {datetime.now().isoformat()}")
log.info("  Next step: share outputs/diagnostics/layer4_trades.log for analysis")
log.info("=" * 70)