"""
=============================================================================
ACT 0 — Layer 3 Diagnostic: Filter Pipeline
=============================================================================
Purpose : Collect ground-truth facts on filter pipeline execution for both
          Legacy and New pipelines on the same date window.
          Covers: time filter removals (with timestamps), RSI filter applied
          values, per-filter rejection counts, final signal counts, and
          filter config as actually applied (not just YAML).
Output  : outputs/diagnostics/layer3_filters.log
Run     : python tests/strategies/diagnostics/diag_layer3_filters.py
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
LOG_FILE = LOG_DIR / "layer3_filters.log"

file_handler = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s"))

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s"))

log = logging.getLogger("diag_layer3")
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
# SETUP — Load both pipelines up to signals
# ===========================================================================
section("SETUP — Loading both pipelines to signal stage")

import pandas as pd
import yaml

# ── NEW pipeline ─────────────────────────────────────────────────────────────
log.info("  [NEW] Loading config, data, signals...")
try:
    from src.config.config_schema import StrategyConfig
    from src.strategies.specific.modules.data_loader import DataLoader
    from src.strategies.specific.modules.signal_generator import SignalGenerator
    from src.strategies.specific.modules.filter_pipeline import FilterPipeline

    new_config      = StrategyConfig.from_yaml(NEW_CONFIG_PATH)
    new_loader      = DataLoader(new_config)
    new_bundle      = new_loader.load_data()
    new_gen         = SignalGenerator(new_config)
    new_signal_frame = new_gen.generate_signals(new_bundle)
    new_signals     = new_signal_frame.signals

    log.info(f"  [NEW] Setup OK | strategy_bars={len(new_bundle.strategy)} "
             f"| raw signals BUY={int((new_signals==1).sum())} "
             f"SELL={int((new_signals==2).sum())} "
             f"TOTAL={int((new_signals!=0).sum())}")
except Exception as e:
    log.error(f"  [NEW] Setup FAILED: {e}", exc_info=True)
    sys.exit(1)

# ── LEGACY pipeline ──────────────────────────────────────────────────────────
log.info("  [LEGACY] Loading config, data, signals...")
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

    data_cfg  = raw_legacy["data"]
    l_start   = str(data_cfg["date_range"]["start"])
    l_end     = str(data_cfg["date_range"]["end"])
    leg_strategy = _load_file(data_cfg["file"],      l_start, l_end)
    leg_htf      = _load_file(data_cfg["file_htf"])

    # Legacy SignalGenerator
    leg_sg_mod   = importlib.import_module("src.strategies.core.signal_generator")
    LegacySG     = leg_sg_mod.SignalGenerator
    leg_sg       = LegacySG(raw_legacy)
    leg_sf       = leg_sg.generate_signals(leg_strategy, leg_htf)

    # Capture legacy signals — probe interface
    leg_signals_raw = None
    if hasattr(leg_sf, "signals"):
        leg_signals_raw = leg_sf.signals
        log.info("  [LEGACY] Signal source: .signals attribute")
    elif isinstance(leg_sf, pd.Series):
        leg_signals_raw = leg_sf
        log.info("  [LEGACY] Signal source: Series returned directly")
    elif isinstance(leg_sf, pd.DataFrame):
        log.info(f"  [LEGACY] SignalGenerator returned DataFrame with cols: {list(leg_sf.columns)}")
        # Store full df for filter pipeline — legacy FilterPipeline may need it
        leg_signal_df = leg_sf
        for col in ["signal", "signals", "buy_signal", "sell_signal"]:
            if col in leg_sf.columns:
                leg_signals_raw = leg_sf[col]
                log.info(f"  [LEGACY] Signal column: '{col}'")
                break

    if leg_signals_raw is not None:
        log.info(f"  [LEGACY] Setup OK | strategy_bars={len(leg_strategy)} "
                 f"| signal dtype={leg_signals_raw.dtype} "
                 f"| unique_values={sorted(leg_signals_raw.unique().tolist())}")
    else:
        log.warning("  [LEGACY] Signal series not captured — filter pipeline will run with object only")

    log.info("  [LEGACY] Data + signals loaded OK")

except Exception as e:
    log.error(f"  [LEGACY] Setup FAILED: {e}", exc_info=True)
    sys.exit(1)

# ===========================================================================
# BLOCK 1 — NEW PIPELINE: FilterPipeline execution with full introspection
# ===========================================================================
section("BLOCK 1 — NEW PIPELINE: FilterPipeline Full Execution")

new_filter_result = None
try:
    new_fp     = FilterPipeline(new_config)
    new_filter_result = new_fp.apply_filters(new_signal_frame, new_bundle.strategy)

    log.info(f"  [NEW] raw_count              : {new_filter_result.raw_count}")
    log.info(f"  [NEW] time_filtered_count    : {new_filter_result.time_filtered_count}")
    log.info(f"  [NEW] technical_filtered_count: {new_filter_result.technical_filtered_count}")
    log.info(f"  [NEW] final_count            : {new_filter_result.final_count}")

    # Final signals breakdown
    final_sig = new_filter_result.final_signals.signals
    log.info(f"  [NEW] final BUY count        : {int((final_sig==1).sum())}")
    log.info(f"  [NEW] final SELL count       : {int((final_sig==2).sum())}")
    log.info(f"  [NEW] final TOTAL count      : {int((final_sig!=0).sum())}")

    # Rejection reasons
    log.info(f"  [NEW] rejection_reasons      : {new_filter_result.rejection_reasons}")

    # Per-filter metadata
    subsection("NEW: Per-filter metadata")
    if new_filter_result.filter_results:
        for fm in new_filter_result.filter_results:
            log.info(f"  [NEW] filter={getattr(fm,'name','?'):<25} "
                     f"removed={getattr(fm,'removed_count', getattr(fm,'signals_removed','?'))} "
                     f"enabled={getattr(fm,'enabled','?')} "
                     f"config={getattr(fm,'config',{})}")
            log.debug(f"       full metadata: {fm}")
    else:
        log.info("  [NEW] filter_results list is empty or None")

    # Execution time
    log.info(f"  [NEW] execution_time_ms      : {new_filter_result.execution_time_ms}")

    # Signals surviving time filter (before technical filters)
    subsection("NEW: Signals after TIME filter (before technical filters)")
    # Infer from counts: raw=19, after_time = raw - (raw - time_filtered_count)
    # time_filtered_count is signals that PASSED time filter
    raw_ts    = new_signals.index[new_signals != 0]
    final_ts  = final_sig.index[final_sig != 0]
    log.info(f"  [NEW] Raw signal timestamps ({len(raw_ts)}):")
    for ts in raw_ts:
        log.info(f"         {ts}  val={new_signals.loc[ts]}")

    log.info(f"  [NEW] Final signal timestamps ({len(final_ts)}):")
    for ts in final_ts:
        log.info(f"         {ts}  val={final_sig.loc[ts]}")

    # Time-filtered out (in raw but not in final)
    # Note: technical filters may also remove — capture both stages separately
    removed_by_any = set(str(t) for t in raw_ts) - set(str(t) for t in final_ts)
    log.info(f"  [NEW] Timestamps removed by any filter ({len(removed_by_any)}):")
    for ts_str in sorted(removed_by_any):
        log.info(f"         {ts_str}")

except Exception as e:
    log.error(f"  [NEW] FilterPipeline FAILED: {e}", exc_info=True)

# ===========================================================================
# BLOCK 2 — NEW PIPELINE: Time filter deep-dive
# ===========================================================================
section("BLOCK 2 — NEW PIPELINE: Time Filter Deep-Dive")

try:
    # Re-run just the time filter in isolation by checking each raw signal timestamp
    # against the session config — to produce an exact per-timestamp audit
    tf_cfg = new_config.filters.time_filters.time_filter
    log.info(f"  [NEW] time_filter.enabled      : {tf_cfg.enabled}")
    log.info(f"  [NEW] session_start            : {tf_cfg.session_start}")
    log.info(f"  [NEW] session_end              : {tf_cfg.session_end}")

    from datetime import time as dtime

    # Parse session boundaries
    try:
        # session_start/end may be dicts or objects
        ss = tf_cfg.session_start
        se = tf_cfg.session_end
        if hasattr(ss, 'hour'):
            start_h, start_m = ss.hour, ss.minute
            end_h,   end_m   = se.hour, se.minute
        else:
            start_h, start_m = ss['hour'], ss['minute']
            end_h,   end_m   = se['hour'], se['minute']
        session_start = dtime(start_h, start_m)
        session_end   = dtime(end_h,   end_m)
        log.info(f"  [NEW] Session: {session_start} → {session_end}")
    except Exception as ep:
        log.warning(f"  [NEW] Could not parse session times: {ep}")
        session_start, session_end = None, None

    if session_start and session_end:
        subsection("NEW: Per-signal time filter audit")
        for ts in raw_ts:
            bar_time = ts.time()
            in_session = session_start <= bar_time <= session_end
            val = int(new_signals.loc[ts])
            direction = "BUY " if val == 1 else "SELL"
            status = "PASS" if in_session else "FAIL"
            log.info(f"  [NEW] {ts}  {direction}  bar_time={bar_time}  "
                     f"in_session={in_session}  time_filter={status}")

except Exception as e:
    log.error(f"  [NEW] Time filter deep-dive FAILED: {e}", exc_info=True)

# ===========================================================================
# BLOCK 3 — NEW PIPELINE: RSI filter deep-dive
# ===========================================================================
section("BLOCK 3 — NEW PIPELINE: RSI Filter Deep-Dive")

try:
    # Get RSI filter config as actually applied
    rsi_cfg = new_config.filters.technical_filters.rsi_filter \
              if hasattr(new_config.filters.technical_filters, 'rsi_filter') \
              else None
    if rsi_cfg is None:
        # try dict-style access
        tech = new_config.filters.technical_filters
        rsi_cfg = getattr(tech, 'rsi_filter', None) or \
                  (tech.get('rsi_filter') if isinstance(tech, dict) else None)

    log.info(f"  [NEW] RSI filter config (parsed object): {rsi_cfg}")
    if rsi_cfg:
        log.info(f"  [NEW] rsi_filter.enabled    : {getattr(rsi_cfg, 'enabled', 'N/A')}")
        log.info(f"  [NEW] rsi_filter.length     : {getattr(rsi_cfg, 'length', 'N/A')}")
        log.info(f"  [NEW] rsi_filter.overbought : {getattr(rsi_cfg, 'overbought', 'N/A')}")
        log.info(f"  [NEW] rsi_filter.oversold   : {getattr(rsi_cfg, 'oversold', 'N/A')}")

    # Compute RSI on new strategy data directly to see values at signal bars
    subsection("NEW: RSI values at each signal bar (time-passed signals only)")
    try:
        import pandas_ta_classic as ta
        rsi_length = getattr(rsi_cfg, 'length', 14) if rsi_cfg else 14
        overbought = getattr(rsi_cfg, 'overbought', 70) if rsi_cfg else 70
        oversold   = getattr(rsi_cfg, 'oversold',   30) if rsi_cfg else 30

        rsi_series = ta.rsi(new_bundle.strategy["close"], length=rsi_length)
        log.info(f"  [NEW] RSI computed (length={rsi_length}) | "
                 f"non-null={rsi_series.notna().sum()} | "
                 f"first_valid={rsi_series.first_valid_index()}")

        # At each signal bar, log RSI value and whether it would pass filter
        # RSI filter logic: BUY passes if RSI < overbought, SELL passes if RSI > oversold
        for ts in final_ts:  # final_ts = survived time filter
            if ts in rsi_series.index and pd.notna(rsi_series.loc[ts]):
                val     = int(final_sig.loc[ts])
                rsi_val = round(float(rsi_series.loc[ts]), 4)
                if val == 1:   # BUY
                    passes = rsi_val < overbought
                    rule   = f"RSI({rsi_val}) < overbought({overbought})"
                else:          # SELL
                    passes = rsi_val > oversold
                    rule   = f"RSI({rsi_val}) > oversold({oversold})"
                status = "PASS" if passes else "FAIL"
                direction = "BUY " if val == 1 else "SELL"
                log.info(f"  [NEW] {ts}  {direction}  RSI={rsi_val}  rule={rule}  result={status}")
            else:
                log.info(f"  [NEW] {ts}  RSI=NaN (warmup period)")

    except ImportError:
        log.warning("  [NEW] pandas_ta not available — trying ta-lib fallback")
        try:
            import talib
            rsi_length = getattr(rsi_cfg, 'length', 14) if rsi_cfg else 14
            import numpy as np
            rsi_arr    = talib.RSI(new_bundle.strategy["close"].values, timeperiod=rsi_length)
            rsi_series = pd.Series(rsi_arr, index=new_bundle.strategy.index)
            log.info(f"  [NEW] RSI computed via talib (length={rsi_length})")
            for ts in final_ts:
                if ts in rsi_series.index and pd.notna(rsi_series.loc[ts]):
                    log.info(f"  [NEW] {ts}  RSI={round(float(rsi_series.loc[ts]),4)}")
        except ImportError:
            log.warning("  [NEW] Neither pandas_ta nor talib available — skipping RSI values")

except Exception as e:
    log.error(f"  [NEW] RSI deep-dive FAILED: {e}", exc_info=True)

# ===========================================================================
# BLOCK 4 — LEGACY PIPELINE: Filter execution
# ===========================================================================
section("BLOCK 4 — LEGACY PIPELINE: Filter Execution")

leg_filter_result = None
leg_final_signals = None

try:
    # Locate legacy filter pipeline
    leg_fp_mod = None
    for mod_path in [
        "src.strategies.core.filter_pipeline",
        "src.strategies.specific.modules.filter_pipeline",
    ]:
        try:
            mod = importlib.import_module(mod_path)
            if hasattr(mod, "FilterPipeline"):
                leg_fp_mod = mod
                log.info(f"  [LEGACY] FilterPipeline found at: {mod_path}")
                break
        except ImportError:
            continue

    if leg_fp_mod is None:
        log.warning("  [LEGACY] FilterPipeline not found via import — attempting manual filter audit")
    else:
        # Try to run legacy filter pipeline
        LegacyFP = leg_fp_mod.FilterPipeline
        try:
            # Legacy may accept (config, signal_frame, df) or (config, df)
            leg_fp = LegacyFP(raw_legacy)
            # Try with signal frame object
            leg_filter_result = leg_fp.apply_filters(leg_sf, leg_strategy)
            log.info("  [LEGACY] FilterPipeline ran with (signal_frame, strategy_df)")
        except TypeError:
            try:
                leg_filter_result = leg_fp.apply_filters(leg_strategy, leg_sf)
                log.info("  [LEGACY] FilterPipeline ran with (strategy_df, signal_frame)")
            except Exception as e2:
                log.warning(f"  [LEGACY] FilterPipeline both arg orders failed: {e2}")

        if leg_filter_result is not None:
            # Extract counts — may differ by attribute name
            for attr in ["raw_count", "total_signals", "input_count"]:
                v = getattr(leg_filter_result, attr, None)
                if v is not None:
                    log.info(f"  [LEGACY] {attr}: {v}")

            for attr in ["time_filtered_count", "after_time_filter", "time_passed"]:
                v = getattr(leg_filter_result, attr, None)
                if v is not None:
                    log.info(f"  [LEGACY] {attr}: {v}")

            for attr in ["technical_filtered_count", "after_technical_filter", "tech_passed"]:
                v = getattr(leg_filter_result, attr, None)
                if v is not None:
                    log.info(f"  [LEGACY] {attr}: {v}")

            for attr in ["final_count", "output_count", "passed_count"]:
                v = getattr(leg_filter_result, attr, None)
                if v is not None:
                    log.info(f"  [LEGACY] {attr}: {v}")

            for attr in ["rejection_reasons", "filter_stats", "removed_by"]:
                v = getattr(leg_filter_result, attr, None)
                if v is not None:
                    log.info(f"  [LEGACY] {attr}: {v}")

            # Try to get final signals
            for attr in ["final_signals", "signals", "output_signals"]:
                fs = getattr(leg_filter_result, attr, None)
                if fs is not None:
                    if hasattr(fs, "signals"):
                        leg_final_signals = fs.signals
                    elif isinstance(fs, pd.Series):
                        leg_final_signals = fs
                    if leg_final_signals is not None:
                        log.info(f"  [LEGACY] Final signals extracted via .{attr}")
                        break

            log.debug(f"  [LEGACY] Full filter result repr: {leg_filter_result}")

        else:
            log.warning("  [LEGACY] FilterPipeline returned None")

except Exception as e:
    log.error(f"  [LEGACY] Filter block FAILED: {e}", exc_info=True)

# Manual fallback: apply time filter and RSI manually using legacy config
# so we can at least confirm per-timestamp what legacy would pass/reject
subsection("LEGACY: Manual filter audit (time + RSI) using legacy config values")
try:
    from datetime import time as dtime

    # Time filter
    tf_raw = raw_legacy.get("trade_management", {}).get("time_filter", {})
    ss_raw = tf_raw.get("session_start", {})
    se_raw = tf_raw.get("session_end",   {})
    leg_session_start = dtime(ss_raw.get("hour", 8),  ss_raw.get("minute", 30))
    leg_session_end   = dtime(se_raw.get("hour", 20), se_raw.get("minute", 30))
    log.info(f"  [LEGACY] Session: {leg_session_start} → {leg_session_end}")

    # RSI config
    rsi_raw = raw_legacy.get("filters", {}).get("rsi_filter", {})
    leg_rsi_enabled    = rsi_raw.get("enabled", True)
    leg_rsi_length     = rsi_raw.get("length", 14)
    leg_rsi_overbought = rsi_raw.get("overbought", 70)
    leg_rsi_oversold   = rsi_raw.get("oversold",   30)
    log.info(f"  [LEGACY] RSI filter: enabled={leg_rsi_enabled} "
             f"length={leg_rsi_length} ob={leg_rsi_overbought} os={leg_rsi_oversold}")

    # Compute RSI on legacy strategy data
    try:
        import pandas_ta_classic as ta
        leg_rsi = ta.rsi(leg_strategy["close"], length=leg_rsi_length)
    except ImportError:
        try:
            import talib, numpy as np
            arr     = talib.RSI(leg_strategy["close"].values, timeperiod=leg_rsi_length)
            leg_rsi = pd.Series(arr, index=leg_strategy.index)
        except ImportError:
            leg_rsi = None
            log.warning("  [LEGACY] Cannot compute RSI — neither pandas_ta nor talib available")

    # Apply filters manually to raw signal timestamps
    if leg_signals_raw is not None:
        raw_leg_ts = leg_signals_raw.index[leg_signals_raw != 0] \
                     if hasattr(leg_signals_raw, 'index') else []
    else:
        # Fall back to new pipeline timestamps (confirmed identical counts)
        raw_leg_ts = new_signals.index[new_signals != 0]
        log.warning("  [LEGACY] Using New pipeline timestamps as proxy (counts confirmed equal)")

    log.info(f"  [LEGACY] Auditing {len(raw_leg_ts)} raw signal bars:")
    leg_passed_time = []
    leg_passed_rsi  = []
    leg_removed_time = []
    leg_removed_rsi  = []

    for ts in raw_leg_ts:
        bar_time   = ts.time()
        in_session = leg_session_start <= bar_time <= leg_session_end

        # Get signal direction — from raw legacy or fall back to new
        if leg_signals_raw is not None and ts in leg_signals_raw.index:
            raw_val = leg_signals_raw.loc[ts]
        else:
            raw_val = new_signals.loc[ts] if ts in new_signals.index else 0

        # Normalise direction
        if str(raw_val).upper() in ("TRUE", "1", "BUY"):
            direction, norm_val = "BUY ", 1
        elif str(raw_val).upper() in ("SELL", "2"):
            direction, norm_val = "SELL", 2
        else:
            direction, norm_val = "BUY " if raw_val == 1 else "SELL", int(raw_val) if raw_val in (1,2) else 0

        time_status = "PASS" if in_session else "FAIL"
        if not in_session:
            leg_removed_time.append(ts)
            log.info(f"  [LEGACY] {ts}  {direction}  time={bar_time}  TIME={time_status}")
            continue

        leg_passed_time.append(ts)

        # RSI check
        if leg_rsi is not None and ts in leg_rsi.index and pd.notna(leg_rsi.loc[ts]):
            rsi_val = round(float(leg_rsi.loc[ts]), 4)
            if norm_val == 1:   # BUY
                rsi_passes = rsi_val < leg_rsi_overbought
                rule = f"RSI({rsi_val}) < ob({leg_rsi_overbought})"
            else:               # SELL
                rsi_passes = rsi_val > leg_rsi_oversold
                rule = f"RSI({rsi_val}) > os({leg_rsi_oversold})"
            rsi_status = "PASS" if rsi_passes else "FAIL"
            if rsi_passes:
                leg_passed_rsi.append(ts)
            else:
                leg_removed_rsi.append(ts)
            log.info(f"  [LEGACY] {ts}  {direction}  time=PASS  RSI={rule}  RSI={rsi_status}")
        else:
            rsi_val = None
            leg_passed_rsi.append(ts)
            log.info(f"  [LEGACY] {ts}  {direction}  time=PASS  RSI=NaN(warmup)  RSI=PASS(default)")

    log.info(f"  [LEGACY] Time filter: {len(raw_leg_ts)} in → {len(leg_passed_time)} passed "
             f"({len(leg_removed_time)} removed)")
    log.info(f"  [LEGACY] RSI filter:  {len(leg_passed_time)} in → {len(leg_passed_rsi)} passed "
             f"({len(leg_removed_rsi)} removed)")
    log.info(f"  [LEGACY] Final count (manual): {len(leg_passed_rsi)}")
    log.info(f"  [LEGACY] Time-removed timestamps: {[str(t) for t in leg_removed_time]}")
    log.info(f"  [LEGACY] RSI-removed timestamps : {[str(t) for t in leg_removed_rsi]}")
    log.info(f"  [LEGACY] Passed timestamps      : {[str(t) for t in leg_passed_rsi]}")

except Exception as e:
    log.error(f"  [LEGACY] Manual filter audit FAILED: {e}", exc_info=True)

# ===========================================================================
# BLOCK 5 — DIRECT COMPARISON
# ===========================================================================
section("BLOCK 5 — Direct Comparison: New vs Legacy Filter Results")

try:
    subsection("Signal counts through filter stages")

    # New pipeline actuals from FilterPipelineResult
    new_raw        = new_filter_result.raw_count           if new_filter_result else "N/A"
    new_after_time = new_filter_result.time_filtered_count if new_filter_result else "N/A"
    new_after_tech = new_filter_result.technical_filtered_count if new_filter_result else "N/A"
    new_final      = new_filter_result.final_count         if new_filter_result else "N/A"

    # Legacy actuals: from result object if available, else from manual audit
    if leg_filter_result is not None:
        leg_raw        = getattr(leg_filter_result, "raw_count",
                         getattr(leg_filter_result, "total_signals", "N/A"))
        leg_after_time = getattr(leg_filter_result, "time_filtered_count",
                         getattr(leg_filter_result, "after_time_filter", "N/A"))
        leg_after_tech = getattr(leg_filter_result, "technical_filtered_count",
                         getattr(leg_filter_result, "after_technical_filter", "N/A"))
        leg_final      = getattr(leg_filter_result, "final_count",
                         getattr(leg_filter_result, "output_count", "N/A"))
    else:
        leg_raw        = len(raw_leg_ts)
        leg_after_time = len(leg_passed_time)
        leg_after_tech = len(leg_passed_rsi)
        leg_final      = len(leg_passed_rsi)

    fact("raw signal count",              leg_raw,        new_raw)
    fact("signals after time filter",     leg_after_time, new_after_time)
    fact("signals after technical filter",leg_after_tech, new_after_tech)
    fact("final signal count",            leg_final,      new_final)

    subsection("RSI filter config as applied")
    fact("rsi_filter.length",     leg_rsi_length,     getattr(rsi_cfg, 'length',     'N/A') if rsi_cfg else 'N/A')
    fact("rsi_filter.overbought", leg_rsi_overbought, getattr(rsi_cfg, 'overbought', 'N/A') if rsi_cfg else 'N/A')
    fact("rsi_filter.oversold",   leg_rsi_oversold,   getattr(rsi_cfg, 'oversold',   'N/A') if rsi_cfg else 'N/A')

    subsection("Time filter config as applied")
    fact("session_start", str(leg_session_start), str(session_start) if session_start else 'N/A')
    fact("session_end",   str(leg_session_end),   str(session_end)   if session_end   else 'N/A')

    subsection("Removed timestamp comparison")
    new_removed = sorted(removed_by_any) if 'removed_by_any' in dir() else []
    leg_removed = sorted([str(t) for t in leg_removed_time + leg_removed_rsi])
    fact("total removed count",   len(leg_removed), len(new_removed))
    log.info(f"  Legacy removed : {leg_removed}")
    log.info(f"  New removed    : {new_removed}")

    # Final signal timestamps
    new_final_ts_list = sorted([str(t) for t in final_ts])
    leg_final_ts_list = sorted([str(t) for t in leg_passed_rsi])
    fact("final signal timestamps match", leg_final_ts_list, new_final_ts_list)
    log.info(f"  Legacy final ts: {leg_final_ts_list}")
    log.info(f"  New    final ts: {new_final_ts_list}")

except Exception as e:
    log.error(f"  Comparison FAILED: {e}", exc_info=True)

# ===========================================================================
# BLOCK 6 — SUMMARY TABLE
# ===========================================================================
section("BLOCK 6 — Layer 3 Summary")

try:
    log.info("  ┌─────────────────────────────────────────────┬──────────┬──────────┬──────────┐")
    log.info("  │ Fact                                        │  Legacy  │   New    │  Status  │")
    log.info("  ├─────────────────────────────────────────────┼──────────┼──────────┼──────────┤")

    def row(label, lv, nv):
        status = "✅ MATCH " if str(lv) == str(nv) else "❌ DIFFER"
        log.info(f"  │ {label:<43} │ {str(lv):<8} │ {str(nv):<8} │ {status} │")

    row("Raw signal count",               leg_raw,        new_raw)
    row("After time filter",              leg_after_time, new_after_time)
    row("After RSI filter",               leg_after_tech if leg_filter_result else len(leg_passed_rsi), new_after_tech)
    row("Final count",                    leg_final,      new_final)
    row("Time-removed count",             len(leg_removed_time), new_raw - new_after_time if isinstance(new_raw,int) and isinstance(new_after_time,int) else "N/A")
    row("RSI-removed count",              len(leg_removed_rsi),  new_after_time - new_after_tech if isinstance(new_after_time,int) and isinstance(new_after_tech,int) else "N/A")
    row("RSI length",                     leg_rsi_length, getattr(rsi_cfg,'length','N/A') if rsi_cfg else 'N/A')
    row("RSI overbought",                 leg_rsi_overbought, getattr(rsi_cfg,'overbought','N/A') if rsi_cfg else 'N/A')
    row("RSI oversold",                   leg_rsi_oversold,   getattr(rsi_cfg,'oversold','N/A') if rsi_cfg else 'N/A')

    log.info("  └─────────────────────────────────────────────┴──────────┴──────────┴──────────┘")

except Exception as e:
    log.warning(f"  Summary table incomplete: {e}")

# ===========================================================================
# DONE
# ===========================================================================
section("DIAGNOSTIC COMPLETE")
log.info(f"  Log file : {LOG_FILE}")
log.info(f"  Timestamp: {datetime.now().isoformat()}")
log.info("  Next step: share outputs/diagnostics/layer3_filters.log for analysis")
log.info("=" * 70)