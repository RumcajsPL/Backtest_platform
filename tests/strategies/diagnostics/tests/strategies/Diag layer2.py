"""
=============================================================================
ACT 0 — Layer 2 Diagnostic: Raw Signal Generation
=============================================================================
Purpose : Collect ground-truth facts on raw signal generation for both
          Legacy and New pipelines on the same date window.
          Covers: signal counts, timestamps, values, HTF alignment,
          indicator data availability, and per-bar signal comparison.
Output  : outputs/diagnostics/layer2_signals.log
Run     : python tests/strategies/diagnostics/diag_layer2_signals.py
=============================================================================
"""

import logging
import sys
import json
import hashlib
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
LOG_FILE = LOG_DIR / "layer2_signals.log"

file_handler = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s"))

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s"))

log = logging.getLogger("diag_layer2")
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

def series_fingerprint(s):
    """Produce a short hash of a Series values for quick equality check."""
    import pandas as pd
    if s is None or len(s) == 0:
        return "EMPTY"
    vals = s.astype(str).values.tobytes()
    return hashlib.md5(vals).hexdigest()[:12]

def index_fingerprint(idx):
    """Hash of index timestamps."""
    vals = idx.astype(str).values.tobytes()
    return hashlib.md5(vals).hexdigest()[:12]

# ===========================================================================
# LOAD BOTH PIPELINES
# ===========================================================================
section("SETUP — Loading both pipelines")

import pandas as pd
import yaml

# ── NEW pipeline ────────────────────────────────────────────────────────────
log.info("  Loading NEW pipeline config and data...")
try:
    from src.config.config_schema import StrategyConfig
    from src.strategies.specific.modules.data_loader import DataLoader
    from src.strategies.specific.modules.signal_generator import SignalGenerator

    new_config = StrategyConfig.from_yaml(NEW_CONFIG_PATH)
    new_loader  = DataLoader(new_config)
    new_bundle  = new_loader.load_data()
    log.info("  [NEW] Config + DataBundle loaded OK")
    log.info(f"  [NEW] strategy bars : {len(new_bundle.strategy)}")
    log.info(f"  [NEW] htf bars      : {len(new_bundle.htf) if new_bundle.htf is not None else 'None'}")
except Exception as e:
    log.error(f"  [NEW] Setup FAILED: {e}", exc_info=True)
    sys.exit(1)

# ── LEGACY pipeline ─────────────────────────────────────────────────────────
log.info("  Loading LEGACY pipeline config and data...")
try:
    import importlib
    legacy_loader_mod = importlib.import_module("src.strategies.core.data_loader")
    LegacyDataLoader  = legacy_loader_mod.DataLoader

    with open(LEGACY_CONFIG_PATH) as f:
        raw_legacy = yaml.safe_load(f)

    # Locate legacy signal generator / trigger
    # Try known locations
    LegacySignalGen = None
    legacy_trigger_mod = None
    for mod_path in [
        "src.strategies.core.signal_generator",
        "src.indicators.wbws_trigger",
        "src.strategies.specific.modules.signal_generator",  # may share
    ]:
        try:
            mod = importlib.import_module(mod_path)
            if hasattr(mod, "SignalGenerator"):
                LegacySignalGen = mod.SignalGenerator
                log.info(f"  Legacy SignalGenerator found at: {mod_path}")
                break
            if hasattr(mod, "WBWSTrigger"):
                legacy_trigger_mod = mod
                log.info(f"  Legacy WBWSTrigger found at: {mod_path}")
                break
        except ImportError:
            continue

    # Load legacy data directly via pandas (same as Layer 1 Block 4)
    def _load_file(rel_path, label, date_start=None, date_end=None):
        p = PROJECT_ROOT / rel_path
        log.info(f"  [LEGACY] Loading {label}: exists={p.exists()}  path={p}")
        if not p.exists():
            log.error(f"  [LEGACY] File not found: {p}")
            return None
        df = pd.read_parquet(p) if str(p).endswith(".parquet") else pd.read_csv(
            p, parse_dates=["timestamp"], index_col="timestamp")
        if date_start and date_end:
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)
            df = df.loc[date_start:date_end]
        log.info(f"  [LEGACY] {label}: shape={df.shape}  first={df.index[0]}  last={df.index[-1]}")
        return df

    data_cfg = raw_legacy["data"]
    l_start  = str(data_cfg["date_range"]["start"])
    l_end    = str(data_cfg["date_range"]["end"])

    leg_strategy = _load_file(data_cfg["file"],      "strategy", l_start, l_end)
    leg_full     = _load_file(data_cfg["file"],      "full")
    leg_htf      = _load_file(data_cfg["file_htf"],  "htf")        # full — as legacy does
    leg_ltf      = _load_file(data_cfg["file_ltf"],  "ltf")        # full — as legacy does
    leg_artf     = _load_file(data_cfg["file_artf"], "artf")

    log.info("  [LEGACY] Data loaded OK")

except Exception as e:
    log.error(f"  [LEGACY] Setup FAILED: {e}", exc_info=True)
    sys.exit(1)

# ===========================================================================
# BLOCK 1 — NEW PIPELINE: SignalGenerator output
# ===========================================================================
section("BLOCK 1 — NEW PIPELINE: SignalGenerator Output")

new_signal_frame = None
try:
    new_gen          = SignalGenerator(new_config)
    new_signal_frame = new_gen.generate_signals(new_bundle)

    new_signals = new_signal_frame.signals  # pd.Series int8: 1=BUY 2=SELL 0=none
    new_buy     = (new_signals == 1).sum()
    new_sell    = (new_signals == 2).sum()
    new_total   = (new_signals != 0).sum()

    log.info(f"  [NEW] Signal counts: BUY={new_buy}  SELL={new_sell}  TOTAL={new_total}")
    log.info(f"  [NEW] Signal series length (all bars): {len(new_signals)}")
    log.info(f"  [NEW] Signal dtype: {new_signals.dtype}")
    log.info(f"  [NEW] Signal index first: {new_signals.index[0]}")
    log.info(f"  [NEW] Signal index last:  {new_signals.index[-1]}")
    log.info(f"  [NEW] Signal series fingerprint: {series_fingerprint(new_signals)}")
    log.info(f"  [NEW] Signal index fingerprint:  {index_fingerprint(new_signals.index)}")

    # All non-zero signal timestamps and values
    new_signal_bars = new_signals[new_signals != 0]
    log.info(f"  [NEW] Non-zero signal bars ({len(new_signal_bars)}):")
    for ts, val in new_signal_bars.items():
        direction = "BUY " if val == 1 else "SELL"
        log.info(f"         {ts}  {direction}  raw_value={val}")

    # Indicator data availability
    if new_signal_frame.indicator_data is not None:
        ind = new_signal_frame.indicator_data
        log.info(f"  [NEW] indicator_data shape: {ind.shape}")
        log.info(f"  [NEW] indicator_data columns: {list(ind.columns)}")
        log.debug(f"  [NEW] indicator_data first 3 rows:\n{ind.head(3).to_string()}")
        log.debug(f"  [NEW] indicator_data last 3 rows:\n{ind.tail(3).to_string()}")
        # Log indicator values at each signal bar
        subsection("NEW: Indicator values at signal bars")
        for ts, val in new_signal_bars.items():
            if ts in ind.index:
                row = ind.loc[ts]
                log.info(f"  [NEW] {ts} | signal={'BUY' if val==1 else 'SELL'} | {row.to_dict()}")
    else:
        log.info("  [NEW] indicator_data: None (core mode — expected)")

    # signal_metadata
    log.info(f"  [NEW] signal_metadata: {new_signal_frame.signal_metadata}")

except Exception as e:
    log.error(f"  [NEW] SignalGenerator FAILED: {e}", exc_info=True)

# ===========================================================================
# BLOCK 2 — LEGACY PIPELINE: Signal generation
# ===========================================================================
section("BLOCK 2 — LEGACY PIPELINE: Signal Generation")

leg_signals_series = None
leg_indicator_data = None

try:
    # Attempt to run legacy SignalGenerator if found
    if LegacySignalGen is not None:
        log.info("  [LEGACY] Running legacy SignalGenerator...")
        try:
            leg_gen    = LegacySignalGen(raw_legacy)
            leg_sf     = leg_gen.generate_signals(leg_strategy, leg_htf)
            if hasattr(leg_sf, "signals"):
                leg_signals_series = leg_sf.signals
            elif isinstance(leg_sf, pd.Series):
                leg_signals_series = leg_sf
            log.info(f"  [LEGACY] SignalGenerator ran OK via class")
        except Exception as e2:
            log.warning(f"  [LEGACY] SignalGenerator class failed ({e2}), falling back to direct trigger")

    # Direct WBWSTrigger path (most likely legacy path)
    if leg_signals_series is None:
        log.info("  [LEGACY] Attempting direct WBWSTrigger instantiation...")
        for mod_path in [
            "src.indicators.wbws_trigger",
            "src.strategies.specific.modules.signal_generator",
        ]:
            try:
                mod = importlib.import_module(mod_path)
                if hasattr(mod, "WBWSTrigger"):
                    htf_period = raw_legacy.get("indicator", {}).get("htf_period", "1H")
                    trigger = mod.WBWSTrigger(htf_period=htf_period)
                    result  = trigger.calculate(leg_strategy, leg_htf)
                    log.info(f"  [LEGACY] WBWSTrigger.calculate() returned type: {type(result)}")

                    # Result may be DataFrame with signal columns or a Series
                    if isinstance(result, pd.DataFrame):
                        log.info(f"  [LEGACY] Trigger result columns: {list(result.columns)}")
                        log.debug(f"  [LEGACY] Trigger result first 3 rows:\n{result.head(3).to_string()}")
                        leg_indicator_data = result
                        # Find signal column — common names
                        for col in ["signal", "signals", "buy_sell", "direction", "entry"]:
                            if col in result.columns:
                                leg_signals_series = result[col]
                                log.info(f"  [LEGACY] Signal column identified: '{col}'")
                                break
                        if leg_signals_series is None:
                            log.warning(f"  [LEGACY] No known signal column found in trigger output — logging all columns")
                            for col in result.columns:
                                non_zero = result[col][result[col] != 0]
                                if len(non_zero) > 0 and len(non_zero) < 200:
                                    log.info(f"  [LEGACY] Candidate signal col '{col}': {len(non_zero)} non-zero values")
                    elif isinstance(result, pd.Series):
                        leg_signals_series = result
                        log.info(f"  [LEGACY] WBWSTrigger returned Series directly")
                    log.info(f"  [LEGACY] WBWSTrigger found at: {mod_path}")
                    break
            except ImportError:
                continue
            except Exception as e3:
                log.warning(f"  [LEGACY] {mod_path} trigger failed: {e3}")
                continue

    # If we have a signal series, analyse it
    if leg_signals_series is not None:
        # Slice to strategy window
        s = leg_strategy.index[0]
        e = leg_strategy.index[-1]
        if leg_signals_series.index[0] < s or leg_signals_series.index[-1] > e:
            leg_signals_sliced = leg_signals_series.loc[s:e]
            log.info(f"  [LEGACY] Signal series sliced to strategy window: {len(leg_signals_series)} → {len(leg_signals_sliced)}")
            leg_signals_series = leg_signals_sliced

        log.info(f"  [LEGACY] Signal series dtype: {leg_signals_series.dtype}")
        log.info(f"  [LEGACY] Signal unique values: {sorted(leg_signals_series.unique().tolist())}")

        # Determine encoding: could be bool, int, string, 1/2, True/False, "BUY"/"SELL"
        unique_vals = set(leg_signals_series.unique().tolist())
        log.info(f"  [LEGACY] Unique values (raw): {unique_vals}")

        # Try to normalise to 1=BUY, 2=SELL, 0=none
        def normalise_signal(series):
            u = set(series.dropna().unique().tolist())
            # int8 style: 1/2/0
            if u <= {0, 1, 2}:
                return series.fillna(0).astype(int), "int_1_2"
            # bool buy/sell style
            if u <= {True, False, 0, 1}:
                return series.fillna(0).astype(int), "bool_01"
            # string style
            str_u = {str(v).upper().strip() for v in u}
            if str_u <= {"BUY", "SELL", "NONE", "0", "NAN", "", "FALSE", "TRUE"}:
                mapping = {}
                for v in u:
                    sv = str(v).upper().strip()
                    if sv in ("BUY", "1", "TRUE"):
                        mapping[v] = 1
                    elif sv in ("SELL", "2"):
                        mapping[v] = 2
                    else:
                        mapping[v] = 0
                return series.map(mapping).fillna(0).astype(int), "string_mapped"
            return series.fillna(0), "unknown"

        leg_norm, leg_encoding = normalise_signal(leg_signals_series)
        log.info(f"  [LEGACY] Signal encoding detected: {leg_encoding}")

        leg_buy   = (leg_norm == 1).sum()
        leg_sell  = (leg_norm == 2).sum()
        leg_total = (leg_norm != 0).sum()
        log.info(f"  [LEGACY] Signal counts (normalised): BUY={leg_buy}  SELL={leg_sell}  TOTAL={leg_total}")
        log.info(f"  [LEGACY] Signal series length: {len(leg_norm)}")
        log.info(f"  [LEGACY] Signal series fingerprint: {series_fingerprint(leg_norm)}")
        log.info(f"  [LEGACY] Signal index fingerprint:  {index_fingerprint(leg_norm.index)}")

        leg_signal_bars = leg_norm[leg_norm != 0]
        log.info(f"  [LEGACY] Non-zero signal bars ({len(leg_signal_bars)}):")
        for ts, val in leg_signal_bars.items():
            direction = "BUY " if val == 1 else "SELL"
            log.info(f"         {ts}  {direction}  raw_value={leg_signals_series.loc[ts] if ts in leg_signals_series.index else 'N/A'}")

        # Indicator data at signal bars if available
        if leg_indicator_data is not None:
            leg_ind_window = leg_indicator_data.loc[s:e] if leg_indicator_data.index[0] < s else leg_indicator_data
            subsection("LEGACY: Indicator values at signal bars")
            for ts, val in leg_signal_bars.items():
                if ts in leg_ind_window.index:
                    row = leg_ind_window.loc[ts]
                    log.info(f"  [LEGACY] {ts} | signal={'BUY' if val==1 else 'SELL'} | {row.to_dict()}")
    else:
        log.warning("  [LEGACY] Could not obtain signal series — logging full trigger output structure only")
        log.warning("  [LEGACY] Manual inspection of legacy signal_generator.py required")

except Exception as e:
    log.error(f"  [LEGACY] Signal block FAILED: {e}", exc_info=True)

# ===========================================================================
# BLOCK 3 — HTF DATA PASSED TO TRIGGER: New vs Legacy
# ===========================================================================
section("BLOCK 3 — HTF Data Passed to Signal Trigger")

subsection("NEW pipeline HTF (sliced to window)")
if new_bundle.htf is not None:
    htf_n = new_bundle.htf
    log.info(f"  [NEW] htf shape       : {htf_n.shape}")
    log.info(f"  [NEW] htf first ts    : {htf_n.index[0]}")
    log.info(f"  [NEW] htf last ts     : {htf_n.index[-1]}")
    log.debug(f"  [NEW] htf full content:\n{htf_n.to_string()}")

subsection("LEGACY pipeline HTF (full file, unsliced)")
if leg_htf is not None:
    # Focus on the window-relevant bars
    htf_window = leg_htf.loc[leg_strategy.index[0]:leg_strategy.index[-1]]
    log.info(f"  [LEGACY] htf full shape              : {leg_htf.shape}")
    log.info(f"  [LEGACY] htf bars within window      : {htf_window.shape}")
    log.info(f"  [LEGACY] htf first ts (full)         : {leg_htf.index[0]}")
    log.info(f"  [LEGACY] htf last ts (full)          : {leg_htf.index[-1]}")
    log.info(f"  [LEGACY] htf first ts (window)       : {htf_window.index[0] if len(htf_window) > 0 else 'EMPTY'}")
    log.info(f"  [LEGACY] htf last ts (window)        : {htf_window.index[-1] if len(htf_window) > 0 else 'EMPTY'}")
    log.debug(f"  [LEGACY] htf window content:\n{htf_window.to_string()}")

subsection("HTF window content comparison")
if new_bundle.htf is not None and leg_htf is not None and len(htf_window) > 0:
    htf_n_sorted = htf_n.sort_index()
    htf_l_sorted = htf_window.sort_index()
    fact("htf window row count",  len(htf_l_sorted),             len(htf_n_sorted))
    fact("htf window first ts",   str(htf_l_sorted.index[0]),    str(htf_n_sorted.index[0]))
    fact("htf window last ts",    str(htf_l_sorted.index[-1]),   str(htf_n_sorted.index[-1]))
    fact("htf window open[0]",    round(float(htf_l_sorted["open"].iloc[0]),  6),
                                  round(float(htf_n_sorted["open"].iloc[0]),  6))
    fact("htf window close[-1]",  round(float(htf_l_sorted["close"].iloc[-1]), 6),
                                  round(float(htf_n_sorted["close"].iloc[-1]), 6))

# ===========================================================================
# BLOCK 4 — SIGNAL ALIGNMENT: Bar-by-bar comparison
# ===========================================================================
section("BLOCK 4 — Signal Alignment: Bar-by-Bar Comparison")

if new_signals is not None and leg_signals_series is not None and leg_norm is not None:
    subsection("Index alignment check")
    fact("strategy index length",  len(leg_strategy), len(new_bundle.strategy))
    fact("signal series length",   len(leg_norm),     len(new_signals))

    # Align on common index
    common_idx = new_signals.index.intersection(leg_norm.index)
    log.info(f"  Common index length : {len(common_idx)}")
    log.info(f"  New-only timestamps : {len(new_signals.index.difference(leg_norm.index))}")
    log.info(f"  Legacy-only timestamps: {len(leg_norm.index.difference(new_signals.index))}")

    new_aligned = new_signals.reindex(common_idx).fillna(0).astype(int)
    leg_aligned = leg_norm.reindex(common_idx).fillna(0).astype(int)

    subsection("Signal value comparison on common index")
    fact("aligned BUY count",   int((leg_aligned == 1).sum()), int((new_aligned == 1).sum()))
    fact("aligned SELL count",  int((leg_aligned == 2).sum()), int((new_aligned == 2).sum()))
    fact("aligned TOTAL count", int((leg_aligned != 0).sum()), int((new_aligned != 0).sum()))
    fact("signal fingerprint",  series_fingerprint(leg_aligned), series_fingerprint(new_aligned))

    # Find divergent bars
    divergent = common_idx[new_aligned.values != leg_aligned.values]
    log.info(f"  Divergent bars (signal differs): {len(divergent)}")
    if len(divergent) > 0:
        log.info("  Divergent bar details:")
        for ts in divergent:
            lv = leg_aligned.loc[ts]
            nv = new_aligned.loc[ts]
            l_dir = {0: "NONE", 1: "BUY", 2: "SELL"}.get(int(lv), str(lv))
            n_dir = {0: "NONE", 1: "BUY", 2: "SELL"}.get(int(nv), str(nv))
            log.info(f"    {ts}  Legacy={l_dir}  New={n_dir}")

    # Timestamps of signals in each pipeline
    subsection("Signal timestamp lists")
    leg_ts_buy  = leg_aligned.index[leg_aligned == 1].tolist()
    leg_ts_sell = leg_aligned.index[leg_aligned == 2].tolist()
    new_ts_buy  = new_aligned.index[new_aligned == 1].tolist()
    new_ts_sell = new_aligned.index[new_aligned == 2].tolist()

    log.info(f"  Legacy BUY  timestamps : {[str(t) for t in leg_ts_buy]}")
    log.info(f"  New    BUY  timestamps : {[str(t) for t in new_ts_buy]}")
    log.info(f"  Legacy SELL timestamps : {[str(t) for t in leg_ts_sell]}")
    log.info(f"  New    SELL timestamps : {[str(t) for t in new_ts_sell]}")

    # Symmetric difference — signals in one but not the other
    buy_only_legacy  = set(str(t) for t in leg_ts_buy)  - set(str(t) for t in new_ts_buy)
    buy_only_new     = set(str(t) for t in new_ts_buy)  - set(str(t) for t in leg_ts_buy)
    sell_only_legacy = set(str(t) for t in leg_ts_sell) - set(str(t) for t in new_ts_sell)
    sell_only_new    = set(str(t) for t in new_ts_sell) - set(str(t) for t in leg_ts_sell)

    log.info(f"  BUY  in Legacy only : {sorted(buy_only_legacy)}")
    log.info(f"  BUY  in New only    : {sorted(buy_only_new)}")
    log.info(f"  SELL in Legacy only : {sorted(sell_only_legacy)}")
    log.info(f"  SELL in New only    : {sorted(sell_only_new)}")

else:
    log.warning("  Skipping bar-by-bar comparison — one or both signal series unavailable")

# ===========================================================================
# BLOCK 5 — HTF SHIFT(1) LOOKAHEAD PROTECTION
# ===========================================================================
section("BLOCK 5 — HTF shift(1) Lookahead Protection Verification")

log.info("  Checking whether HTF signal values are forward-shifted in each pipeline.")
log.info("  Method: compare HTF close value at signal bar vs current and previous HTF bar.")

try:
    if new_bundle.htf is not None and new_signal_frame is not None:
        htf_n = new_bundle.htf
        subsection("NEW pipeline: HTF alignment at signal bars")
        for ts, val in new_signal_bars.items():
            # Find which HTF bar this 1-min bar belongs to
            htf_before = htf_n.index[htf_n.index <= ts]
            if len(htf_before) == 0:
                log.info(f"  [NEW] {ts}: no HTF bar at or before this timestamp")
                continue
            htf_ts_current  = htf_before[-1]
            htf_ts_prev     = htf_before[-2] if len(htf_before) >= 2 else None
            htf_close_curr  = htf_n.loc[htf_ts_current, "close"]
            htf_close_prev  = htf_n.loc[htf_ts_prev, "close"] if htf_ts_prev is not None else "N/A"
            log.info(f"  [NEW] signal={ts} val={val} | "
                     f"htf_current_bar={htf_ts_current} close={htf_close_curr} | "
                     f"htf_prev_bar={htf_ts_prev} close={htf_close_prev}")

    if leg_htf is not None and leg_signals_series is not None:
        htf_window_full = leg_htf  # legacy uses full HTF
        subsection("LEGACY pipeline: HTF alignment at signal bars")
        for ts, val in leg_signal_bars.items():
            htf_before = htf_window_full.index[htf_window_full.index <= ts]
            if len(htf_before) == 0:
                log.info(f"  [LEGACY] {ts}: no HTF bar at or before this timestamp")
                continue
            htf_ts_current = htf_before[-1]
            htf_ts_prev    = htf_before[-2] if len(htf_before) >= 2 else None
            htf_close_curr = htf_window_full.loc[htf_ts_current, "close"]
            htf_close_prev = htf_window_full.loc[htf_ts_prev, "close"] if htf_ts_prev is not None else "N/A"
            log.info(f"  [LEGACY] signal={ts} val={val} | "
                     f"htf_current_bar={htf_ts_current} close={htf_close_curr} | "
                     f"htf_prev_bar={htf_ts_prev} close={htf_close_prev}")

except Exception as e:
    log.error(f"  HTF alignment check FAILED: {e}", exc_info=True)

# ===========================================================================
# BLOCK 6 — SUMMARY TABLE
# ===========================================================================
section("BLOCK 6 — Layer 2 Summary")

try:
    log.info("  ┌─────────────────────────────────────────────┬──────────┬──────────┬──────────┐")
    log.info("  │ Fact                                        │  Legacy  │   New    │  Status  │")
    log.info("  ├─────────────────────────────────────────────┼──────────┼──────────┼──────────┤")

    def row(label, lv, nv):
        status = "✅ MATCH " if str(lv) == str(nv) else "❌ DIFFER"
        log.info(f"  │ {label:<43} │ {str(lv):<8} │ {str(nv):<8} │ {status} │")

    row("Raw BUY count",          leg_buy   if leg_signals_series is not None else "N/A", int(new_buy))
    row("Raw SELL count",         leg_sell  if leg_signals_series is not None else "N/A", int(new_sell))
    row("Raw TOTAL count",        leg_total if leg_signals_series is not None else "N/A", int(new_total))
    row("Signal series length",   len(leg_norm) if leg_norm is not None else "N/A", len(new_signals))
    row("Divergent bars",         len(divergent) if 'divergent' in dir() else "N/A", 0 if 'divergent' in dir() and len(divergent)==0 else "see above")
    row("HTF bars in window",     len(htf_window) if leg_htf is not None else "N/A", len(new_bundle.htf) if new_bundle.htf is not None else "N/A")

    log.info("  └─────────────────────────────────────────────┴──────────┴──────────┴──────────┘")

except Exception as e:
    log.warning(f"  Summary table incomplete: {e}")

# ===========================================================================
# DONE
# ===========================================================================
section("DIAGNOSTIC COMPLETE")
log.info(f"  Log file : {LOG_FILE}")
log.info(f"  Timestamp: {datetime.now().isoformat()}")
log.info("  Next step: share outputs/diagnostics/layer2_signals.log for analysis")
log.info("=" * 70)