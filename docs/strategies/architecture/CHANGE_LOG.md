# WBWSStrategy Change Log

---

## v3.4.0 — DataLoader LTF Coverage Warning (2026-02-25)

**Scope**: `data_loader.py`  
**Architecture compliance**: Single Responsibility preserved — DataLoader
warns, does not abort. The abort/accept decision stays with the operator.
No contract modified, no module boundary crossed.

---

### data_loader.py → 3.2.0

#### [GUARD-2] `load_data` — LTF date-range coverage check (~0 ms overhead)

**Root cause — late visibility of a configuration problem**

When the LTF Parquet file on disk does not fully cover the configured
`date_range`, `_load_file_with_cache` slices silently to whatever rows
are available. The existing [M2] guard only catches a fully empty result
(zero overlap). Partial overlap — which is the real backtester risk — passes
[M2] silently and produces no diagnostic output until `TradeSimulator
[GUARD-1]` fires inside `_precompute_ltf_windows`, several seconds into the
pipeline after signal generation and filtering have already run.

**Fix — two timestamp comparisons after `df_ltf` is loaded**

Placed immediately after `df_ltf` is loaded and sanitized, before ARTF,
before `_validate_dataframe`, before `DataBundle` construction — the
earliest point at which both `df_ltf` and `date_range` are available.

Detects two independent gap types:

*Head gap* — `df_ltf.index[0] > date_range.start`  
The LTF file starts after the strategy window opens. Trades at the
beginning of the window lack LTF execution data.

*Tail gap* — `df_ltf.index[-1] < date_range.end`  
The LTF file ends before the strategy window closes. Trades at the end of
the window lack LTF execution data. This is the primary backtester risk
when a 3-month LTF file is used with a 6-month strategy window.

Both gaps may be present simultaneously (file is a subset of the window).

Warning message includes gap type, uncovered day count, both date
boundaries, and a pointer to TradeSimulator [GUARD-1] for the bar-level
count:

```
LTF file does not fully cover the strategy date_range —
tail gap [2024-03-31 → 2024-06-28] (89d uncovered at end).
LTF file: [2024-01-02 → 2024-03-31]. Strategy window: [2024-01-02 → 2024-06-28].
Trades in uncovered bars will close at end-of-data price.
Extend the LTF file to eliminate this gap.
TradeSimulator [GUARD-1] will report the exact bar count.
```

**Why warning and not abort**

DataLoader's responsibility is loading and slicing. Whether 89 uncovered
days is acceptable (e.g. a rolling backtester window deliberately extending
beyond the current LTF file) or unacceptable (misconfiguration) is an
operator decision. Aborting here would break legitimate use cases.
TradeSimulator [GUARD-1] aborts only on zero coverage — the case where
there is no valid execution data at all.

**Warning is unconditional on mode** (both `core` and `analytics`).
LTF coverage gaps are always operationally significant — suppressing them
in core mode would defeat the purpose of the guard.

**Runtime cost**: two `pd.Timestamp` comparisons and conditional string
formatting. Unmeasurable relative to file I/O.

---

## v3.3.0 — LTF Coverage Guard (2026-02-25)

**Scope**: `trade_simulator.py`  
**Architecture compliance**: Enforces the fail-fast principle (Architecture
Principle #6) for a previously silent failure mode. No contract modified,
no module boundary crossed.

---

### trade_simulator.py → 5.4.0

#### [GUARD-1] `_precompute_ltf_windows` — LTF coverage validation (~0 ms overhead)

**Root cause — silent data gap**

DataLoader slices LTF to `[date_range.start, date_range.end]` before passing
it to `simulate_trades()` via `bundle.ltf`. If the LTF file on disk does not
cover the full strategy window (e.g. a 3-month file used for a 6-month
backtester run), `df.loc[start:end]` silently returns only the available
rows. `_precompute_ltf_windows` builds no windows for strategy bars with no
matching LTF ticks. `_check_exits_with_ltf_ohlc` returns early for those
bars. Positions that should have been stopped out remain open and close at
end-of-data price via `_close_remaining_positions`. No exception, no warning
— incorrect results with no diagnostic signal.

This violates Architecture Principle #6: *"Missing data at runtime rejects
the trade — it never silently approves it."* The equivalent principle for
execution data is: missing LTF coverage must never silently degrade results.

**Fix — two-tier guard after window build**

The guard is placed after both the uniform fast path and the non-uniform
fallback complete, because only then is the actual window count known.

*Zero windows → `ValueError` (hard abort)*

The LTF file and the strategy date range do not overlap at all. Every exit
check would silently skip; every trade would close at end-of-data price.
Error message includes both date boundaries so the operator can immediately
identify whether the wrong file is configured or the date range is wrong.

```
LTF coverage error: zero windows built for strategy period
[2024-01-02 09:01 → 2024-06-28 17:29].
LTF file covers [2023-10-01 00:00 → 2023-12-31 23:59].
The LTF file does not overlap the strategy date range.
Extend the LTF file or correct the date_range configuration.
```

*Partial coverage → `logger.warning` (not abort)*

Accepted risk for backtester runs where the strategy window extends slightly
beyond the LTF file boundary (e.g. last few bars of a rolling window). The
warning includes coverage percentage, missing bar count, and both date
boundaries so the operator can assess severity.

```
LTF partial coverage: 67,800 of 68,400 strategy bars have LTF windows
(99.1% coverage, 600 bars missing).
Strategy period: [2024-01-02 09:01 → 2024-03-29 17:29].
LTF file covers: [2024-01-02 09:00 → 2024-03-28 17:30].
Trades in uncovered bars will close at end-of-data price.
```

**Runtime cost**: negligible — two integer comparisons and string formatting
after the window build loop. No impact on hot-path performance.

---

*Version 3.3.0 | 2026-02-25*

---

## v3.2.0 — Performance Hardening (2026-02-25)

**Scope**: `trade_simulator.py`, `risk_manager.py`  
**Architecture compliance**: All four changes are pure hot-path optimisations.
No contract field added or removed, no module boundary crossed, no frozen
dataclass mutated, no new silent fallbacks introduced. Fail-fast and
Single-Source-of-Truth principles are unchanged.

---

### trade_simulator.py → 5.3.0

#### [PERF-1] `_precompute_ltf_windows` — vectorised index conversion and searchsorted (~517 ms saved)

**Root cause**: Two operations inside the 68,400-iteration loop dominated
precomputation time:
- `np.datetime64(strategy_time)` was called once per bar — 68,400 dtype
  conversions accumulating to ~136 ms.
- `ltf_index_np.searchsorted(...)` was called twice per bar — 136,800 calls
  accumulating to ~180 ms.

**Fix**:
1. `df_strategy.index.to_numpy()` is now called once before the loop,
   producing `strat_np` and `end_np` arrays. (68,400 → 1 conversion.)
2. Both `searchsorted` calls are now vectorised over the full strategy index
   arrays. (136,800 → 2 calls.)
3. A **uniform fast path** detects when every strategy bar maps to the same
   number of LTF ticks (normal for clean second-level data). When detected,
   numpy `reshape` + `min/max(axis=1)` computes all `min_low` / `max_high`
   values in two array operations instead of one per-bar Python call.
   A size-mismatch guard falls through to the per-bar loop for non-uniform
   data — no regression risk on irregular inputs.
4. A module-level constant `_ARRAY_THRESHOLD: int = 4` introduced for PERF-2
   (see below).

#### [PERF-2] `_check_exits_with_ltf_ohlc` — direct attribute access for small trade counts (~190 ms saved)

**Root cause**: The method rebuilt Python lists and called `np.array()` on
every strategy bar regardless of the number of open trades.  For the typical
case of 0–3 simultaneous open trades the allocation is pure overhead.

**Fix**: For `n_open_trades < _ARRAY_THRESHOLD` (= 4), each trade's SL/TP
is evaluated directly against the pre-computed window `min_low` / `max_high`
scalars via attribute access. The full LTF scan (`_find_exact_exit_bar_numba`)
runs only when the cheap scalar comparison confirms a hit — same logic,
fewer allocations.

The original `np.array()` vectorised path is fully preserved for
`n_open_trades >= _ARRAY_THRESHOLD` where array construction pays off.

The separate `open_list` list comprehension was merged into a single loop
that populates `long_trades` and `short_trades` simultaneously, eliminating
one extra pass over `_open_trades`.

---

### risk_manager.py → 2.5.0

#### [PERF-3] `compute_trade_parameters` — spread info removed from hot path (~5 ms saved)

**Root cause**: `SpreadManager.get_spread_info()` constructs a new dict on
every call. `compute_trade_parameters` is called once per approved signal;
only two fields (`spread_type`, `spread_value`) are consumed from that dict.
Both values are stable for the lifetime of the `RiskManager` instance.

**Fix**: In `__init__`, after `SpreadManager` construction, the two fields
are cached as `self._spread_type` and `self._spread_value`. The
`get_spread_info()` call in `compute_trade_parameters` is replaced with
direct attribute reads.

#### [PERF-4] `.loc[]` → `.at[]` for scalar Series access (~10–15 ms saved)

**Root cause**: `Series.loc[]` with a single scalar key performs label
broadcasting and alignment checks designed for multi-label slicing. For a
single timestamp on a Series with a unique `DatetimeIndex`, this overhead
is unnecessary. `.at[]` is the pandas-recommended API for single-label
scalar access and bypasses the broadcasting machinery.

**Fix**: All four scalar ATR / RAR reads replaced:
- `compute_trade_parameters`: `atr_series.loc[timestamp]`
  → `atr_series.at[timestamp]`
- `compute_trade_parameters`: `annual_range_series.loc[timestamp]`
  → `annual_range_series.at[timestamp]`
- `validate_risk_percentile`: `annual_range_series.loc[timestamp]`
  → `annual_range_series.at[timestamp]`

Semantics are identical for a single key on a Series with a unique index.
The `timestamp not in index` membership guard in `validate_risk_percentile`
is unchanged — it continues to enforce the fail-safe rejection introduced
in v2.4.0 [FIX-1].

---

### Performance summary

| Change   | File               | Method                        | Saving  |
|----------|--------------------|-------------------------------|---------|
| PERF-1   | trade_simulator.py | `_precompute_ltf_windows`     | ~517 ms |
| PERF-2   | trade_simulator.py | `_check_exits_with_ltf_ohlc`  | ~190 ms |
| PERF-3   | risk_manager.py    | `compute_trade_parameters`    | ~5 ms   |
| PERF-4   | risk_manager.py    | `compute_trade_parameters` /  | ~10 ms  |
|          |                    | `validate_risk_percentile`    |         |
| **Total**|                    |                               | **~722 ms** |

Pre-optimisation baseline: ~4,855 ms (68,400-bar strategy, 4M-row LTF, DAX 1-min).  
Post-optimisation target: ~4,133 ms.  
Remaining gap vs signals/filters (~50 ms): structural — stateful sequential
simulation × LTF second-level scanning cannot be vectorised.

---

*Version 3.2.0 | 2026-02-25*