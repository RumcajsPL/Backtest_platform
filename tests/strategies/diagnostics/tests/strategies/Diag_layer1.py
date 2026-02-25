"""
=============================================================================
ACT 0 — Layer 1 Diagnostic: Config & Data Load
=============================================================================
Purpose : Collect ground-truth facts on config parsing and data loading
          for both Legacy and New pipelines on the same date window.
Output  : Structured log file at outputs/diagnostics/layer1_config_data.log
Run     : python tests/strategies/diagnostics/diag_layer1_config_data.py
=============================================================================
"""

import logging
import sys
import json
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# Bootstrap: walk up from this file until src/utils/paths.py is found.
# This is depth-independent — works regardless of where the script sits.
# ---------------------------------------------------------------------------
_here = Path(__file__).resolve()
_candidate = _here.parent
for _ in range(10):
    if (_candidate / "src" / "utils" / "paths.py").exists():
        break
    _candidate = _candidate.parent
else:
    raise RuntimeError(
        f"Cannot locate project root (src/utils/paths.py not found) "
        f"starting from {_here}"
    )

PROJECT_ROOT = _candidate
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.paths import CONFIGS_DIR, OUTPUTS_DIR

# ---------------------------------------------------------------------------
# Log setup — file + console (console shows progress only, file has everything)
# ---------------------------------------------------------------------------
LOG_DIR = OUTPUTS_DIR / "diagnostics"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "layer1_config_data.log"

from src.utils.paths import DATA_DIR

file_handler = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s"))

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s"))

log = logging.getLogger("diag_layer1")
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
def section(title: str) -> None:
    log.info("=" * 70)
    log.info(f"  {title}")
    log.info("=" * 70)

def subsection(title: str) -> None:
    log.info("-" * 60)
    log.info(f"  {title}")
    log.info("-" * 60)

def fact(label: str, legacy_val, new_val, note: str = "") -> None:
    match = "MATCH  ✅" if str(legacy_val) == str(new_val) else "DIFFER ❌"
    log.info(f"  [{match}] {label}")
    log.debug(f"           Legacy : {legacy_val}")
    log.debug(f"           New    : {new_val}")
    if note:
        log.debug(f"           Note   : {note}")

def dump_df_facts(label: str, df, pipeline: str) -> None:
    """Log shape, dtypes, index type, first/last 3 rows for a DataFrame."""
    if df is None:
        log.info(f"  [{pipeline}] {label}: None")
        return
    log.info(f"  [{pipeline}] {label}: shape={df.shape}  index_type={type(df.index).__name__}  index_dtype={df.index.dtype}")
    log.debug(f"  [{pipeline}] {label} dtypes:\n{df.dtypes.to_string()}")
    log.debug(f"  [{pipeline}] {label} first 3 rows:\n{df.head(3).to_string()}")
    log.debug(f"  [{pipeline}] {label} last 3 rows:\n{df.tail(3).to_string()}")
    # Index details
    log.info(f"  [{pipeline}] {label}: first_ts={df.index[0]}  last_ts={df.index[-1]}")
    if hasattr(df.index, 'tz'):
        log.info(f"  [{pipeline}] {label}: timezone={df.index.tz}")
    # Check for duplicates
    dup_count = df.index.duplicated().sum()
    log.info(f"  [{pipeline}] {label}: duplicate_index_count={dup_count}")
    # Check for NaNs in OHLCV columns
    ohlcv = [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
    if ohlcv:
        nan_counts = df[ohlcv].isna().sum().to_dict()
        log.info(f"  [{pipeline}] {label}: nan_counts={nan_counts}")

# ===========================================================================
# BLOCK 1 — RAW YAML PARSING
# ===========================================================================
section("BLOCK 1 — RAW YAML CONTENT (key fields)")

import yaml

def load_raw_yaml(path: Path) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

log.info(f"  Legacy config path : {LEGACY_CONFIG_PATH}")
log.info(f"  New config path    : {NEW_CONFIG_PATH}")
log.info(f"  Legacy exists      : {LEGACY_CONFIG_PATH.exists()}")
log.info(f"  New exists         : {NEW_CONFIG_PATH.exists()}")

raw_legacy = load_raw_yaml(LEGACY_CONFIG_PATH)
raw_new    = load_raw_yaml(NEW_CONFIG_PATH)

subsection("Date Range")
legacy_start = raw_legacy.get("data", {}).get("date_range", {}).get("start")
legacy_end   = raw_legacy.get("data", {}).get("date_range", {}).get("end")
new_start    = raw_new.get("data", {}).get("date_range", {}).get("start")
new_end      = raw_new.get("data", {}).get("date_range", {}).get("end")
fact("date_range.start", legacy_start, new_start)
fact("date_range.end",   legacy_end,   new_end)

subsection("Data File Paths")
legacy_strat_file = raw_legacy.get("data", {}).get("file")
new_strat_file    = raw_new.get("data", {}).get("paths", {}).get("strategy_ohlcv")
fact("strategy_ohlcv file", legacy_strat_file, new_strat_file)

legacy_htf_file = raw_legacy.get("data", {}).get("file_htf")
new_htf_file    = raw_new.get("data", {}).get("paths", {}).get("htf_ohlcv")
fact("htf_ohlcv file", legacy_htf_file, new_htf_file)

legacy_ltf_file = raw_legacy.get("data", {}).get("file_ltf")
new_ltf_file    = raw_new.get("data", {}).get("paths", {}).get("ltf_ohlcv")
fact("ltf_ohlcv file", legacy_ltf_file, new_ltf_file)

legacy_artf_file = raw_legacy.get("data", {}).get("file_artf")
new_artf_file    = raw_new.get("data", {}).get("paths", {}).get("artf_ohlcv")
fact("artf_ohlcv file", legacy_artf_file, new_artf_file)

subsection("HTF Period")
legacy_htf_period = raw_legacy.get("indicator", {}).get("htf_period")
new_htf_period    = raw_new.get("data", {}).get("htf_period")
fact("htf_period", legacy_htf_period, new_htf_period)

subsection("Risk Config (raw YAML values)")
legacy_risk_raw = raw_legacy.get("trade_management", {}).get("risk_management", {})
new_risk_raw    = raw_new.get("trade_management", {}).get("risk", {})
log.info(f"  Legacy risk_management block (raw): {json.dumps(legacy_risk_raw, default=str)}")
log.info(f"  New risk block (raw)              : {json.dumps(new_risk_raw, default=str)}")

legacy_mrp = legacy_risk_raw.get("max_risk_percentile")
new_mrp    = new_risk_raw.get("max_risk_percentile")
fact("max_risk_percentile (raw YAML)", legacy_mrp, new_mrp,
     note="Values may look different but represent same threshold — confirm in Layer 4")

legacy_atr_sl = raw_legacy.get("trade_management", {}).get("sl_tp", {}).get("sl_multiplier")
new_atr_sl    = new_risk_raw.get("atr_multiplier_sl")
fact("atr_multiplier_sl", legacy_atr_sl, new_atr_sl)

legacy_rrr = raw_legacy.get("trade_management", {}).get("sl_tp", {}).get("risk_to_reward_ratio")
new_rrr    = new_risk_raw.get("risk_to_reward_ratio")
fact("risk_to_reward_ratio", legacy_rrr, new_rrr)

legacy_atr_len = raw_legacy.get("trade_management", {}).get("sl_tp", {}).get("atr_length")
new_atr_len    = new_risk_raw.get("atr_length")
fact("atr_length", legacy_atr_len, new_atr_len)

subsection("Position Control")
legacy_pos = raw_legacy.get("trade_management", {}).get("position_control", {})
new_pos    = raw_new.get("trade_management", {}).get("position_control", {})
fact("pyramiding_enabled", legacy_pos.get("pyramiding_enabled"), new_pos.get("pyramiding_enabled"))
fact("close_on_opposite",  legacy_pos.get("close_on_opposite"),  new_pos.get("close_on_opposite"))
legacy_max_pos = raw_legacy.get("trade_management", {}).get("position_control", {}).get("max_positions", "NOT SET")
new_max_pos    = new_pos.get("max_positions", "NOT SET")
fact("max_positions", legacy_max_pos, new_max_pos)

subsection("Spread Config (raw)")
legacy_spread = raw_legacy.get("trade_management", {}).get("spread", {})
new_spread    = raw_new.get("trade_management", {}).get("spread", {})
fact("spread.enabled",       legacy_spread.get("enabled"),  new_spread.get("enabled"))
fact("spread.apply_to_long", legacy_spread.get("apply_to_long", "NOT SET"), new_spread.get("apply_to_long", "NOT SET"))
fact("spread.apply_to_short",legacy_spread.get("apply_to_short", "NOT SET"), new_spread.get("apply_to_short", "NOT SET"))

subsection("Time Filter")
legacy_tf = raw_legacy.get("trade_management", {}).get("time_filter", {})
new_tf    = raw_new.get("filters", {}).get("time_filters", {}).get("time_filter", {})
fact("time_filter.enabled",        legacy_tf.get("enabled"),      new_tf.get("enabled"))
fact("session_start hour",         legacy_tf.get("session_start", {}).get("hour"), new_tf.get("session_start", {}).get("hour"))
fact("session_start minute",       legacy_tf.get("session_start", {}).get("minute"), new_tf.get("session_start", {}).get("minute"))
fact("session_end hour",           legacy_tf.get("session_end", {}).get("hour"), new_tf.get("session_end", {}).get("hour"))
fact("session_end minute",         legacy_tf.get("session_end", {}).get("minute"), new_tf.get("session_end", {}).get("minute"))

subsection("Active Technical Filters")
legacy_filters = raw_legacy.get("filters", {})
new_filters    = raw_new.get("filters", {}).get("technical_filters", {})
all_filter_names = [
    "rsi_filter", "dpo_filter", "bollinger_filter", "choppiness_filter",
    "supertrend_filter", "cci_filter", "adx_filter", "macd_filter",
    "ma_filter", "pivot_filter"
]
for fname in all_filter_names:
    l_enabled = legacy_filters.get(fname, {}).get("enabled", "NOT SET")
    n_enabled = new_filters.get(fname, {}).get("enabled", "NOT SET")
    fact(f"{fname}.enabled", l_enabled, n_enabled)

# Pivot filter reversal_percent
l_pivot_rp = legacy_filters.get("pivot_filter", {}).get("reversal_percent", "NOT SET")
n_pivot_rp = new_filters.get("pivot_filter", {}).get("reversal_percent", "NOT SET")
fact("pivot_filter.reversal_percent", l_pivot_rp, n_pivot_rp,
     note="Both disabled — low impact now but record for completeness")

subsection("Filter Sequence")
legacy_seq = raw_legacy.get("filter_sequence", [])
new_seq    = raw_new.get("filters", {}).get("filter_sequence", [])
log.info(f"  Legacy sequence : {legacy_seq}")
log.info(f"  New sequence    : {new_seq}")
fact("filter_sequence", legacy_seq, new_seq)

subsection("Execution Mode")
legacy_mode = raw_legacy.get("execution", {}).get("mode", "NOT SET")
new_mode    = raw_new.get("execution", {}).get("mode", "NOT SET")
fact("execution.mode", legacy_mode, new_mode)

# ===========================================================================
# BLOCK 2 — NEW PIPELINE: StrategyConfig parsed object
# ===========================================================================
section("BLOCK 2 — NEW PIPELINE: Parsed StrategyConfig Object")

try:
    from src.config.config_schema import StrategyConfig
    new_config = StrategyConfig.from_yaml(NEW_CONFIG_PATH)
    log.info("  StrategyConfig loaded successfully")

    # Data
    log.info(f"  [NEW] date_range.start     : {new_config.data.date_range.start if new_config.data.date_range else 'None'}")
    log.info(f"  [NEW] date_range.end       : {new_config.data.date_range.end if new_config.data.date_range else 'None'}")
    log.info(f"  [NEW] strategy_ohlcv path  : {new_config.data.strategy_data.file_path}")
    log.info(f"  [NEW] htf path             : {new_config.data.htf_data.file_path if new_config.data.htf_data else 'None'}")
    log.info(f"  [NEW] ltf path             : {new_config.data.ltf_data.file_path if new_config.data.ltf_data else 'None'}")
    log.info(f"  [NEW] artf path            : {new_config.data.artf_data.file_path if new_config.data.artf_data else 'None'}")
    log.info(f"  [NEW] htf_period           : {new_config.data.htf_period}")

    # Risk — log every field on the risk config object
    risk = new_config.trade_management.risk
    log.info(f"  [NEW] risk.atr_length           : {risk.atr_length}")
    log.info(f"  [NEW] risk.atr_multiplier_sl    : {risk.atr_multiplier_sl}")
    log.info(f"  [NEW] risk.tp_mode              : {risk.tp_mode}")
    log.info(f"  [NEW] risk.risk_to_reward_ratio : {risk.risk_to_reward_ratio}")
    log.info(f"  [NEW] risk.max_risk_percentile  : {risk.max_risk_percentile}")
    log.info(f"  [NEW] risk (full repr)          : {risk}")

    # Position control
    pos = new_config.trade_management.position_control
    log.info(f"  [NEW] position_control          : {pos}")

    # Spread
    spread = new_config.trade_management.spread
    log.info(f"  [NEW] spread.enabled            : {spread.enabled}")
    log.info(f"  [NEW] spread.config_path        : {spread.config_path}")

    # Time filter
    tf = new_config.filters.time_filters.time_filter
    log.info(f"  [NEW] time_filter               : {tf}")

    # Active technical filters
    tech = new_config.filters.technical_filters
    log.info(f"  [NEW] technical_filters (full repr): {tech}")

    # Execution
    log.info(f"  [NEW] execution.mode            : {new_config.execution.mode}")

except Exception as e:
    log.error(f"  StrategyConfig load FAILED: {e}", exc_info=True)

# ===========================================================================
# BLOCK 3 — NEW PIPELINE: DataLoader output (DataBundle)
# ===========================================================================
section("BLOCK 3 — NEW PIPELINE: DataLoader Output (DataBundle)")

try:
    from src.strategies.specific.modules.data_loader import DataLoader
    loader = DataLoader(new_config)
    bundle = loader.load_data()
    log.info("  DataLoader completed successfully")

    log.info(f"  [NEW] bundle.strategy shape    : {bundle.strategy.shape}")
    log.info(f"  [NEW] bundle.full shape        : {bundle.full.shape}")
    log.info(f"  [NEW] bundle.htf shape         : {bundle.htf.shape if bundle.htf is not None else 'None'}")
    log.info(f"  [NEW] bundle.ltf shape         : {bundle.ltf.shape if bundle.ltf is not None else 'None'}")
    log.info(f"  [NEW] bundle.artf shape        : {bundle.artf.shape if bundle.artf is not None else 'None'}")

    dump_df_facts("strategy", bundle.strategy, "NEW")
    dump_df_facts("full",     bundle.full,     "NEW")
    dump_df_facts("htf",      bundle.htf,      "NEW")
    dump_df_facts("ltf",      bundle.ltf,      "NEW")
    dump_df_facts("artf",     bundle.artf,     "NEW")

    log.info(f"  [NEW] DataInfo: {bundle.info}")

except Exception as e:
    log.error(f"  DataLoader FAILED: {e}", exc_info=True)

# ===========================================================================
# BLOCK 4 — LEGACY PIPELINE: Data loading (direct YAML + pandas read)
# ===========================================================================
section("BLOCK 4 — LEGACY PIPELINE: Data Loading")

try:
    import pandas as pd

    # The legacy pipeline loads data inside run_wbws_strategy.py via DataLoader
    # We reproduce the same loading logic by importing the legacy data_loader directly
    # and using the raw yaml config to drive it.

    # Attempt to import legacy DataLoader (it lives in src/strategies/core/data_loader.py
    # or similar — adjust import path if needed based on actual legacy location)
    legacy_loader_imported = False
    legacy_bundle = {}

    # Try to locate legacy data loader
    possible_legacy_paths = [
        "src.strategies.core.data_loader",
        "src.data.data_loader",
        "src.backtesting.data_loader",
    ]

    LegacyDataLoader = None
    for mod_path in possible_legacy_paths:
        try:
            import importlib
            mod = importlib.import_module(mod_path)
            if hasattr(mod, "DataLoader"):
                LegacyDataLoader = mod.DataLoader
                log.info(f"  Legacy DataLoader found at: {mod_path}")
                legacy_loader_imported = True
                break
        except ImportError:
            continue

    if not legacy_loader_imported:
        log.warning("  Legacy DataLoader not found via import — falling back to direct pandas load")

    # Regardless of import success, load data directly using pandas
    # to capture ground truth of what the legacy pipeline would see.

    raw_legacy_reloaded = load_raw_yaml(LEGACY_CONFIG_PATH)
    legacy_data_cfg = raw_legacy_reloaded.get("data", {})

    def resolve_path(rel: str) -> Path:
        """Resolve a relative data path against project root."""
        p = PROJECT_ROOT / rel
        if not p.exists():
            p2 = PROJECT_ROOT / rel.lstrip("/").lstrip("\\")
            return p2 if p2.exists() else p
        return p

    def load_parquet_or_csv(rel_path: str, label: str, date_start=None, date_end=None):
        """Load file, optionally slice to date range."""
        p = resolve_path(rel_path)
        log.info(f"  [LEGACY] Loading {label}: {p}  exists={p.exists()}")
        if not p.exists():
            log.error(f"  [LEGACY] {label} file NOT FOUND: {p}")
            return None
        if str(p).endswith(".parquet"):
            df = pd.read_parquet(p)
        else:
            df = pd.read_csv(p, parse_dates=["timestamp"], index_col="timestamp")
        log.info(f"  [LEGACY] {label} raw shape: {df.shape}")
        if date_start and date_end and not df.empty:
            # Ensure index is datetime
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)
            sliced = df.loc[date_start:date_end]
            log.info(f"  [LEGACY] {label} after slice [{date_start} → {date_end}]: {sliced.shape}")
            return sliced
        return df

    l_start = str(legacy_data_cfg.get("date_range", {}).get("start", ""))
    l_end   = str(legacy_data_cfg.get("date_range", {}).get("end", ""))

    # Strategy (sliced)
    l_strat = load_parquet_or_csv(legacy_data_cfg["file"], "strategy", l_start, l_end)
    # Full (not sliced)
    l_full  = load_parquet_or_csv(legacy_data_cfg["file"], "full_unsliced")
    # HTF (not sliced — legacy loads full then uses it)
    l_htf   = load_parquet_or_csv(legacy_data_cfg["file_htf"], "htf")
    # LTF
    l_ltf   = load_parquet_or_csv(legacy_data_cfg["file_ltf"], "ltf")
    # ARTF (full, no slice)
    l_artf  = load_parquet_or_csv(legacy_data_cfg["file_artf"], "artf")

    dump_df_facts("strategy (sliced)", l_strat, "LEGACY")
    dump_df_facts("full",              l_full,  "LEGACY")
    dump_df_facts("htf",               l_htf,   "LEGACY")
    dump_df_facts("ltf",               l_ltf,   "LEGACY")
    dump_df_facts("artf",              l_artf,  "LEGACY")

except Exception as e:
    log.error(f"  Legacy data load FAILED: {e}", exc_info=True)

# ===========================================================================
# BLOCK 5 — DIRECT COMPARISON: New bundle vs Legacy direct load
# ===========================================================================
section("BLOCK 5 — DIRECT COMPARISON: New DataBundle vs Legacy Direct Load")

try:
    import pandas as pd

    def compare_frames(label: str, df_legacy, df_new):
        """Compare two DataFrames on key structural facts."""
        subsection(f"Comparing: {label}")
        if df_legacy is None and df_new is None:
            log.info(f"  Both None — skipping")
            return
        if df_legacy is None:
            log.warning(f"  Legacy is None, New has shape {df_new.shape}")
            return
        if df_new is None:
            log.warning(f"  New is None, Legacy has shape {df_legacy.shape}")
            return

        fact(f"{label} row count",   len(df_legacy),        len(df_new))
        fact(f"{label} col count",   len(df_legacy.columns), len(df_new.columns))
        fact(f"{label} columns",     sorted(df_legacy.columns.tolist()), sorted(df_new.columns.tolist()))
        fact(f"{label} index type",  type(df_legacy.index).__name__,     type(df_new.index).__name__)
        fact(f"{label} index dtype", str(df_legacy.index.dtype),         str(df_new.index.dtype))

        if not df_legacy.empty and not df_new.empty:
            fact(f"{label} first timestamp", str(df_legacy.index[0]),  str(df_new.index[0]))
            fact(f"{label} last timestamp",  str(df_legacy.index[-1]), str(df_new.index[-1]))
            fact(f"{label} index tz",        str(getattr(df_legacy.index, 'tz', None)),
                                             str(getattr(df_new.index,    'tz', None)))

        # Value spot-check: open/close of first bar
        for col in ["open", "close"]:
            if col in df_legacy.columns and col in df_new.columns and not df_legacy.empty and not df_new.empty:
                fact(f"{label} first bar [{col}]",
                     round(float(df_legacy[col].iloc[0]), 6),
                     round(float(df_new[col].iloc[0]),    6))
                fact(f"{label} last bar [{col}]",
                     round(float(df_legacy[col].iloc[-1]), 6),
                     round(float(df_new[col].iloc[-1]),    6))

    compare_frames("strategy", l_strat,  bundle.strategy)
    compare_frames("full",     l_full,   bundle.full)
    compare_frames("htf",      l_htf,    bundle.htf)
    compare_frames("ltf",      l_ltf,    bundle.ltf)
    compare_frames("artf",     l_artf,   bundle.artf)

except Exception as e:
    log.error(f"  Comparison block FAILED: {e}", exc_info=True)

# ===========================================================================
# DONE
# ===========================================================================
section("DIAGNOSTIC COMPLETE")
log.info(f"  Log file written to: {LOG_FILE}")
log.info(f"  Timestamp: {datetime.now().isoformat()}")
log.info("  Next: copy full log content and share for analysis")
log.info("=" * 70)