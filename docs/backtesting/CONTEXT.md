# Session Handoff — Block 9O (End of Session)
**Date:** 2026-03-10
**Status:** BLOCKED — system crash. Next session must resolve OOM before any run.
---
## ⚠️ CRITICAL UNSOLVED ISSUE — READ FIRST
### System crash caused by OOM in Stage 4 WFO workers
The last calibration run crashed the host system (VSCode terminated, system unstable).
The root cause is **not fully fixed**. Two separate OOM sources exist. Both must be
patched before attempting another run, even with `max_workers: 2`.
---
## OOM Root Cause Analysis — Two Sources
### Source 1 — df_full (strategy file): FIXED ✅
**Patch:** B9O-007 in `data_loader.py` v3.4.0
**What it does:** When `date_range` is set (WFO window evaluation), slices `df_full` to
`[window_start − 200 bars : window_end]` immediately after loading and deletes the full
850MB file. RiskManager ATR/RAR computation is correct with 200-bar warmup prefix.
**Status:** Confirmed present in the version uploaded at end of session. Working.
---
### Source 2 — LTF file sort_index() peak during cache miss: NOT FIXED ❌
**This is the crash that killed the system.**
**File:** `src/strategies/core/data_loader.py` — `_load_file_with_cache()`
**Chain of events:**
1. `run_cleaner.py` clears all `.pkl` cache files before every run ✅ (correct)
2. Stage 4 WFO starts — workers dispatched for all windows simultaneously
3. Each worker calls `_load_file_with_cache()` for the LTF file — **all workers hit a cache miss** (cache was just cleared)
4. Each worker executes: `pd.read_parquet()` → 22.4M rows → `df.index.floor("s")` → `df.sort_index()`
5. `sort_index()` internally calls `.copy()` on the **full 22.4M-row DataFrame** = **856MB peak per worker**
6. With `max_workers=2`: 2 × 856MB = 1.7GB peak just for LTF sort — system OOM
**Why B9O-006 did NOT fix this:**
B9O-006 correctly fixed what gets **stored in cache** (the slice, not the full file).
It did not fix the **loading peak** — `sort_index()` runs on the full file before the
B9O-006 slice. The slice runs too late; the damage is already done.
**Why `max_workers=2` is not enough:**
Even 2 workers × 856MB = 1.7GB simultaneously, on top of all other pipeline memory.
Combined with B9O-007 memory for df_full, OS overhead, and Python interpreter, total
RSS easily exceeds 8–12GB on a cold cache miss scenario.
---
## Required Fix Before Next Run — Patch B9O-008
**File:** `src/strategies/core/data_loader.py`
**Version:** 3.4.0 → 3.5.0
**Strategy:** For sorted Parquet files (true for all well-formed market data), slice to
the target date range **before** calling `sort_index()`. The sort then operates on the
small window slice (~20MB) instead of the full file (856MB).
### Step 1 — Update version docstring
```
Version: 3.4.0  →  Version: 3.5.0
```
### Step 2 — Replace Parquet timestamp-index branch in `_load_file_with_cache()`
**Find** (inside `elif suffix == ".parquet":`, the `if df.index.name == "timestamp":` block):
```python
            if df.index.name == "timestamp":
                if hasattr(df.index, "tz") and df.index.tz is not None:
                    df.index = df.index.tz_localize(None)
                df.index = df.index.floor("s")
                df = df.sort_index()
                if not df.index.is_unique:
                    dup_count = df.index.duplicated().sum()
                    self._log("warning", f"  ⚠️ Found {dup_count} duplicate timestamps in {file_path.name}, keeping last")
                    df = df[~df.index.duplicated(keep="last")]
```
**Replace with:**
```python
            if df.index.name == "timestamp":
                if hasattr(df.index, "tz") and df.index.tz is not None:
                    df.index = df.index.tz_localize(None)
                df.index = df.index.floor("s")

                # ── B9O-008: Slice BEFORE sort to avoid 856MB sort_index peak ──
                # sort_index() calls .copy() on the full DataFrame internally.
                # For sorted files (all well-formed market data Parquet), slice
                # to the target date_range FIRST so sort operates on ~20MB not 856MB.
                # Unsorted files fall back to sort-then-slice with a warning.
                if (apply_date_range
                        and date_range
                        and data_type != "artf"
                        and date_range.start
                        and date_range.end
                        and df.index.is_monotonic_increasing):
                    # Fast path: already sorted — slice first, sort slice only
                    df = df.loc[date_range.start : date_range.end]
                elif not df.index.is_monotonic_increasing:
                    # Fallback: unsorted index — must sort full file first (856MB peak)
                    self._log("warning",
                        f"  ⚠️ {file_path.name} index is unsorted — "
                        f"sorting full file before slice (856MB peak unavoidable). "
                        f"Consider re-exporting this Parquet sorted by timestamp."
                    )
                    df = df.sort_index()

                df = df.sort_index()
                if not df.index.is_unique:
                    dup_count = df.index.duplicated().sum()
                    self._log("warning", f"  ⚠️ Found {dup_count} duplicate timestamps in {file_path.name}, keeping last")
                    df = df[~df.index.duplicated(keep="last")]
```
### Step 3 — Update B9O-006 block comment (no logic change needed)
The B9O-006 `df.loc[start:end].copy()` at the bottom of `_load_file_with_cache` still
runs correctly after this change. For sorted Parquet files `df` is now already a
window-sized view, so `loc[start:end].copy()` is cheap (~20MB). For CSV and unsorted
Parquet, `df` is still the full file and the slice runs as before.
Only update the comment header from:
```python
        # ── B9O-006: Slice BEFORE caching ─────────────────────────────────────
```
To:
```python
        # ── B9O-006 + B9O-008: Slice BEFORE caching ───────────────────────────
        # B9O-008: for sorted Parquet, df is already sliced above (window-sized view).
        # B9O-006: cache stores the slice. del df releases the view (cheap — no copy held).
```
### Memory impact after B9O-008
| Path | Before B9O-008 | After B9O-008 |
|---|---|---|
| LTF cache miss, sorted Parquet | 856MB peak (sort_index copy) | ~20MB peak (sort slice) |
| LTF cache miss, unsorted Parquet | 856MB peak | 856MB peak (fallback) |
| LTF cache hit | ~20MB | ~20MB (unchanged) |
| 2 workers, cold cache | ~1.7GB LTF peak | ~40MB LTF peak |
---
## Execution Order for Next Session
```
STEP 1  Apply B9O-008 to data_loader.py (see exact change above)
        Bump version string to 3.5.0
STEP 2  Verify B9O-007 is still present:
        Confirm _WFO_WARMUP_BARS = 200 exists at module level
        Confirm _is_wfo_window gate exists in load_data()
        Confirm del df_full_raw exists in the WFO branch
STEP 3  Clear cache manually (run_cleaner runs automatically but verify):
        python scripts/runners/run_backtester.py calls clean_environment()
STEP 4  Run calibration with max_workers: 2 (do NOT raise to 6 yet)
        YAML: configs/backtesting/backtest_calibration_fullhistory_v3.yaml (v4.0.0)
        Watch first 60 seconds — if any worker OOMs, stop immediately
STEP 5  After confirming first run survives Stage 4 on cache miss:
        Test max_workers: 4 on a short run
        Only raise to max_workers: 6 after confirming stable
STEP 6  After successful calibration run:
        Extract net_pnl from wfo_window_results (Stage 4)
        _SIGMOID_SCALE = stdev(net_pnl) × 0.5
        Update src/backtesting/wfo/consistency_scorer.py: _SIGMOID_SCALE = <value>
        (Preliminary value from run 221ad474: 230.4 — 34 samples, use if run fails again)
STEP 7  Apply RC-1 fix to calibration YAML (if Stage 1 pass rate still low):
        min_profit_factor: 0.90 → 0.75
        Expected: 25–40 Stage 1 passers (was 9/60 with 0.90)
STEP 8  After calibration complete: run production YAML overnight
        Update backtest_V1_01.yaml:
          max_workers: 2
          min_win_rate: 0.11
          min_expectancy: -2.0
          (max_workers: 6 only after B9O-008 confirmed stable)
```
---
## Patches Applied This Session (Block 9O — cumulative)
| Patch | File | Status | Description |
|---|---|---|---|
| B9O-001 | data_loader.py | ✅ Applied | Sliced strategy cache (apply_date_range=False path) |
| B9O-002 | run_cleaner.py | ✅ Applied | Pre-run cache + temp YAML cleaner |
| B9O-003 | mc_engine.py | ✅ Applied | config.get() for mc_prefilter block |
| B9O-004 | ga_engine.py | ✅ Applied | config.get() + _GA_DEFAULTS dict |
| B9O-005 | orchestrator.py | ✅ Applied | stages: toggle enforcement |
| B9O-006 | data_loader.py | ✅ Applied | Slice-before-cache for LTF/HTF |
| B9O-007 | data_loader.py | ✅ Applied | Warmup-buffered df_full for WFO windows |
| **B9O-008** | **data_loader.py** | **❌ NOT YET APPLIED** | **Slice-before-sort for LTF loading peak** |
---
## Three Root Causes of Poor Calibration Results (RC-1, RC-2, RC-3)
### RC-1 — Stage 1 pass rate 9/60 (15%): min_profit_factor too tight
- 38-month avg PF = 0.830; threshold 0.90 eliminates ~65% of candidates
- Fix: `min_profit_factor: 0.75` in calibration YAML
- Expected: 25–40 Stage 1 passers
- **Status: YAML change ready, not yet applied**
### RC-2 — WFO_INSUFFICIENT_WINDOWS: 8/9 candidates fail
- W02, W05, W06 fail across ALL candidates — structural regime problem
- 50% validity threshold (≥4/7 windows) unachievable when 3 windows are structurally bad
- Fix options: lower threshold to 0.40, or reduce to 5 windows (drop W05/W06)
- Diagnostic script needed first to confirm per-window pass rates
- **Status: PENDING — diagnostic not written**
## RC-3 — OOM crash in Stage 4 WFO workers
- **Status: PARTIALLY FIXED (B9O-007 ✅) — B9O-008 STILL REQUIRED ❌**
---
## Calibration Constants State
### Preliminary _SIGMOID_SCALE (from run 221ad474, 34 samples)
```
N=34, Mean=-153.19, Stdev=460.71
_SIGMOID_SCALE = 460.71 × 0.5 = 230.4   (was 131.0 from 3-month track)
```
Use 230.4 as fallback if next run also crashes before Stage 4 completes.
Will refine after a full successful calibration run with more Stage 1 passers.
### DO NOT mix tracks
```
3-month production:   _SIGMOID_SCALE = 131.0,  _MAX_EXPECTED_DRAWDOWN = 1_000.0  (FROZEN)
Full-history calib:   _SIGMOID_SCALE = 230.4*, _MAX_EXPECTED_DRAWDOWN = 2_500.0  (*preliminary)
```
---
## Current File Versions
```
data_loader.py              v3.5.0  (B9O-007 applied, B9O-008 MISSING — apply before run)
consistency_scorer.py       _SIGMOID_SCALE = 131.0 (3-month value — update after calibration)
                            _MAX_EXPECTED_DRAWDOWN = 2_500.0 (full-history — correct)
calibration YAML            v4.0.0 — min_profit_factor: 0.90 (consider 0.75 per RC-1)
backtest_V1_01.yaml         NOT ready — constraints and max_workers not updated yet
```
---
## Key Files
```
src/strategies/core/data_loader.py                         — apply B9O-008 here
src/backtesting/wfo/consistency_scorer.py                  — update _SIGMOID_SCALE after run
configs/backtesting/backtest_calibration_fullhistory_v3.yaml — active calibration YAML
outputs/backtesting/backtester.db                          — query wfo_window_results for net_pnl
outputs/PATCH_B9O_008_slice_before_sort.md                 — full patch spec with exact find/replace
```
---
## Lessons Learned This Session (L-49 through L-52)
```
L-49: df_full in DataBundle is consumed by TradeSimulator → RiskManager ONLY.
      WFO window evaluations need only a warmup-buffered slice (window_start − 200 bars).
      DataLoader is the correct fix location — not frozen TradeSimulator/RiskManager.
L-50: Diagnostic test false positive: assertion accidentally matched _WFO_WARMUP_BARS
      via a different code path. Always verify the constant exists in the target file
      before trusting a test that checks for it.
L-51: run_cleaner.py clears cache before every run. OOM on cache miss (first worker
      per window) is the actual failure mode — not stale cache.
L-52: B9O-006 fixed what gets cached (the slice) but not the sort_index() peak
      DURING loading. sort_index() calls .copy() on the full DataFrame internally —
      856MB for the 22.4M-row LTF file — before the B9O-006 slice runs.
      Fix: check is_monotonic_increasing and slice before sort for sorted Parquet files.
      This is B9O-008. It must be applied before any further calibration run.
```
---
## What NOT to Do (additions this session)
- Do not run any full-history WFO run before applying B9O-008 — system will crash again
- Do not raise max_workers above 2 until B9O-008 is confirmed stable on a first run
- Do not assume B9O-006 fixed the loading peak — it fixed only the cache value
- Do not interpret a diagnostic "pass" as proof a patch is applied — verify the constant exists in the file