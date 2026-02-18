# ROADMAP Chapter E — End-to-End Test Findings
## Real-data parity run: observations and improvements for New architecture only

**Parent**: `POST_MIGRATION_ROADMAP.md`  
**Source**: `LEGACY_VS_NEW_TEST_REPORT.md` — 2026-02-17 real-data run  
**Priority**: 🔴 High (E1, E2) / 🟡 Medium (E3, E4)  
**Load when**: Working on performance tuning or production hardening

> **Scope**: Legacy is abandoned once migration completes. This chapter contains
> only observations and actions that concern New architecture code. No Legacy
> fixes, no Legacy comparisons beyond what establishes a baseline.

---

## Confirmed: What Is Already Good

Before the action items, record what the test validated so it is not re-investigated:

| Metric | Value | Confidence |
|--------|-------|------------|
| Business logic parity (trades, P&L, win rate) | ✅ 100% match | Locked |
| Overall speedup vs Legacy (hot run) | 55–57% | Stable baseline |
| Trade simulation speedup | 55–57% | Biggest architectural win |
| Signal generation speedup | 35–95% | Large gain in debug mode |
| Filter application speedup | 11–20% | Consistent |
| ARTF data loads correctly | 62 bars confirmed | ✅ |
| All 8 test combinations complete | ✅ 100% | Framework robust |

---

## E1. Core Mode Trade Simulation Slower Than Debug Mode 🔴

**Observed**: Hot-run trade simulation is **11% slower in Core mode than Debug mode**.

```
Trade Simulation — hot run:
  Core:  14.776s
  Debug: 13.294s   ← Debug is faster — counterintuitive
```

**What this means for our code**: Core mode should have *less* overhead (no metadata
collection, no execution timing). The inversion means Core mode has unintended overhead
that Debug mode does not — or Debug mode has an optimisation path that Core bypasses.

**Investigation steps** (Session 19 scan target — Chapter 5):
- [ ] Inspect `TradeSimulator` — is there a conditional code path that Core takes and Debug skips?
- [ ] Check if `debug_mode=False` inadvertently disables a Numba JIT warm path
- [ ] Profile at function level: `cProfile` or `line_profiler` on the hot loop
- [ ] Check `FilterPipelineCache` — Core and Debug may be hitting different cache states

**Expected fix**: Remove the hidden overhead from Core mode. Target: Core ≤ Debug at all times.

**Estimated effort**: 1–2 hours (investigate + fix + benchmark confirmation)

---

## E2. Cache Hit Rate 50% in New vs 100% in Legacy 🔴

**Observed**: New architecture achieves only 50% cache hit rate on hot runs.
Legacy achieves 100%.

```
Cache hit rate — hot run:
  Legacy: 100%
  New:    50%    ← Full cache never reached
```

**What this means**: Some pipeline stage is re-computing on every run rather than
reading from cache. The `FilterPipelineCache` (SHA1-based) is likely not being
triggered correctly for one of the two cached stages.

**Investigation steps** (Session 19 scan target — Chapter 4):
- [ ] Identify which stage misses: Data loading? Signal generation? Filter application?
- [ ] Inspect `FilterPipelineCache` invalidation logic — is the cache key stable between runs?
- [ ] Check if `DataBundle` produces a consistent hash across identical loads
  (timezone-aware index equality, dtype consistency)
- [ ] Verify cache writes are happening after the first (cold) run

**Expected fix**: Consistent SHA1 key generation. Target: 100% cache hit rate on hot runs.

**Estimated effort**: 1–3 hours depending on root cause

---

## E3. ARTF Warning Despite Successful Load 🟡

**Observed**: New architecture loads ARTF data (62 bars confirmed) but emits
a "Monthly ARTF data missing" warning.

**What this matters**: A false warning in production creates noise, reduces trust
in the log, and may mask a future real warning. Worse: if risk management silently
falls back to non-ARTF logic despite data being present, the backtest is wrong.

**Investigation steps** (Session 19 scan target — Chapter 5):
- [ ] Trace the `artf_loaded` flag through `RiskManager` — is it set after the load?
- [ ] Find the warning trigger: is it checking the flag before it is set?
- [ ] Confirm ARTF data is *actually used* in risk calculations (not loaded then ignored)
- [ ] Fix the flag assignment order or the warning condition

**Expected fix**: Warning only emitted when ARTF genuinely absent. No warning when
62 bars are loaded and used.

**Estimated effort**: ~1 hour

---

## E4. Debug Mode Data Loading Anomaly 🟡

**Observed**: In Debug mode, data loading is only **2.7% faster** than Legacy.
In Core mode the same stage is **49% faster**. The gap is unexplained.

```
Data loading speedup:
  Core mode:  49.4% faster than Legacy   ← expected
  Debug mode:  2.7% faster than Legacy   ← unexplained
```

**What this means**: Debug mode adds instrumentation overhead specifically to
the data loading stage, erasing most of the architectural gain.

**Investigation steps** (Session 19 scan target — Chapter 2):
- [ ] Inspect `DataLoader` — what extra work is done when `mode="debug"`?
- [ ] Is there a full DataFrame copy happening in Debug that Core skips?
- [ ] Is metadata collection for `DataBundle` disproportionately expensive?

**Expected fix**: Debug instrumentation should add ≤5% overhead to data loading,
not 47%. Target: Debug data loading ≥ 40% faster than Legacy.

**Estimated effort**: ~1 hour

---

## E5. Data Structure Field Naming (New Architecture Only) 🟢 Low Priority

**Context**: The test report flagged `full` vs `full_bars`, `strategy` vs `strategy_bars`
etc. as parity mismatches. These are not bugs — the New architecture deliberately
uses descriptive suffixed names (`_bars`).

**Action**: No fix needed for New architecture code. The `_bars` naming is correct
and intentional per our contracts. When Legacy is abandoned, the "mismatch" disappears.

**However**: Confirm in Session 19 scan (Chapter 2) that:
- [ ] `DataBundle` field names are documented in `CONTRACTS_REFERENCE.md` ✅ (already there)
- [ ] Any test harness or reporting tool that reads `DataBundle` fields uses `full_bars`
  not `full` (no silent KeyError waiting in the wings)

**Estimated effort**: 15 min — doc check only, no code change needed.

---

## E6. Filter Statistics Now Richer Than Legacy 🟢 Feature, Not a Bug

**Context**: New architecture exposes `time_filtered`, `technical_filtered`,
`final_buy`, `final_sell`, `final_signals` counts that Legacy never captured.
The test reported these as "parity mismatches" — they are not.

**Action**: These fields are **exactly the kind of data** a multi-run automated
backtester needs. Record them correctly in `FilterPipelineResult` and ensure
`TradeAnalytics` can consume them to generate filter-related insights.

**Session 19 scan target** (Chapter 4):
- [ ] Confirm all 5 fields are present in `FilterPipelineResult` contract
- [ ] Confirm `TradeAnalytics` uses them (or note that it doesn't yet — roadmap item)

**Potential enhancement**: `TradeAnalytics` insight: *"Time filter removes X% of signals —
review session boundaries if this seems high."* Low effort, high value for backtester users.

---

## Priority Summary for Scan & Fix Sessions

| # | Item | Sessions | Effort | Priority |
|---|------|----------|--------|----------|
| E1 | Core mode trade sim overhead | 19 (scan) + 20 (fix) | 1-2h | 🔴 |
| E2 | Cache hit rate 50% → 100% | 19 (scan) + 20 (fix) | 1-3h | 🔴 |
| E3 | ARTF false warning | 19 (scan) + 20 (fix) | 1h | 🟡 |
| E4 | Debug data loading slow | 19 (scan) + 20 (fix) | 1h | 🟡 |
| E5 | Field naming doc check | 19 (scan) | 15min | 🟢 |
| E6 | Filter stats in analytics | 20 | 1h | 🟢 |

**Add E1 and E2 as P0 items in `PHASE8_SCAN_REPORT.md`** when populated in Session 19.