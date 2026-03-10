# Session Handoff — Block 9P (Start of Session)
**Date:** 2026-03-10 → 2026-03-11
**Status:** Overnight production run IN PROGRESS — `backtest_V1_01.yaml` v3.0.0
**Next session objective:** Analyse overnight run. If auto_go candidates exist → V1 final documentation and project closure.
---
## Overnight Run — What to Check First
```
Run config:  configs/backtesting/backtest_V1_01.yaml  (v3.0.0)
Expected runtime: 14-20 hours at max_workers=2
Query:       python scripts/diagnostics/query_run.py --run-id <latest> --section all
```
**What a successful run looks like:**
- Stage 1: 80-120 passers from 400 candidates (~30-35% with calibrated constraints)
- Stage 4: At least 5 candidates with WFO score > 0.40 and windows_evaluated >= 4/13
- Stage 5: At least 1 candidate with ruin_prob < 0.05
- Stage 7: At least 1 `go` or `borderline` verdict
**If Stage 5 still shows all ruin > 0.40:** The MC Deep input is the 38-month continuous evaluation dataset. The MC perturbation compounds over 38 months producing false ruin. This is a known architectural issue (see Open Issues below). If this occurs, the V1 closure gate (Phase 1 in CTP_ROADMAP.md) may need to be redefined — or the MC ruin_threshold raised for full-history runs only.
**Watch for `f86f7e6c491a`-type candidates:** In calibration run 9f73d667, this candidate scored WFO=0.4048, ruin=0.041, collapsed=0. It received `no_go` despite passing both MC and WFO gates. Investigate whether the verdict logic has an off-by-one on `go_wfo_floor=0.40` vs score=0.4048. This may be a bug.
---
## Calibration State — CONFIRMED FINAL
```python
# consistency_scorer.py — DO NOT CHANGE before overnight run completes
_SIGMOID_SCALE: float = 310.0           # confirmed N=231, stdev=620.09 (runs 2912e028, 519f84e2)
_MAX_EXPECTED_DRAWDOWN: float = 2_500.0 # full-history track — correct
# Note: run 9f73d667 produced _SIGMOID_SCALE=221.1 (N=1312 including GA 2-window samples)
# This is NOT the correct calibration value — GA partial-window net_pnl distribution
# differs from full 7-window WFO. Use 310.0 only.
```
---
## Patches Applied This Session (Block 9O — complete)
| Patch | File | Status | Description |
|-------|------|--------|-------------|
| B9O-001 | data_loader.py | ✅ | Sliced strategy cache (apply_date_range=False path) |
| B9O-002 | run_cleaner.py | ✅ | Pre-run cache + temp YAML cleaner |
| B9O-003 | mc_engine.py | ✅ | config.get() for mc_prefilter block |
| B9O-004 | ga_engine.py | ✅ | config.get() + _GA_DEFAULTS dict |
| B9O-005 | orchestrator.py | ✅ | stages: toggle enforcement |
| B9O-006 | data_loader.py | ✅ | Slice-before-cache for LTF/HTF |
| B9O-007 | data_loader.py | ✅ | Warmup-buffered df_full for WFO windows |
| B9O-008 | data_loader.py | ✅ | Slice-before-sort for LTF loading peak |
**data_loader.py current version: 3.5.0** (all B9O patches applied)
---
## Current File Versions
```
data_loader.py              v3.5.0  — all B9O patches applied
consistency_scorer.py       _SIGMOID_SCALE = 310.0 (confirmed)
                            _MAX_EXPECTED_DRAWDOWN = 2_500.0
backtest_V1_01.yaml         v3.0.0  — overnight production run (updated this session)
CTP_ROADMAP.md              v1.2    — V2 architecture blueprint + V3 vision added
```
---
## Open Issues for Next Session
### OOM Architecture (max_workers constraint)
- **Status:** Workaround applied (max_workers=2). Root cause: 6 workers × 897MB cold-cache `read_parquet()` = 5.38GB peak.
- **Fix:** B9O-009 = V2 architectural redesign (RawDataStore + WindowSlicer + SharedMemory). See CTP_ROADMAP.md Phase 3.
- **Impact:** Runtime 14-20h instead of ~5h. Acceptable for overnight. Not acceptable for production iteration speed.
### MC Deep False Ruin on 38-Month Dataset
- **Status:** Observed in run 9f73d667 — all top WFO candidates ruin > 0.80. One exception: `f86f7e6c491a` (ruin=0.041).
- **Hypothesis:** MC perturbation (slippage, spread noise, shuffle) compounds over 38-month continuous equity curve producing false ruin signals. 3-month MC was calibrated at ruin_threshold=0.20 for short equity curves.
- **V2 fix:** MC Deep should evaluate on per-window equity curves (3-month slices), not the full 38-month dataset. This is consistent with the WFO per-window evaluation philosophy.
- **Immediate check:** If overnight run also shows all ruin > 0.40, confirm this hypothesis by checking whether Stage 5 input candidates are evaluated on the full 38-month range or per-window ranges.
### RSI-SENS-2 — Confirmed Closed
- **Status:** Definitive. Zero delta on all RSI parameters (rsi_period, rsi_overbought, rsi_oversold) across all 5 sensitivity candidates in run 9f73d667.
- **Action (V2):** Remove RSI from search space. Update zones in both safe and exploration.
### WinError 32 Temp YAML File Lock
- **Status:** Cosmetic. Non-blocking. Occurs during GA stage worker teardown on Windows.
- **V2 fix:** Per-worker temp directories, clean at worker exit not in parent finally.
---
## Lessons Learned This Session (L-49 through L-54)
```
L-49: df_full in DataBundle is consumed by TradeSimulator → RiskManager ONLY.
      WFO window evaluations need only a warmup-buffered slice (window_start − 200 bars).
      DataLoader is the correct fix location — not frozen TradeSimulator/RiskManager.
L-50: Diagnostic test false positive: assertion accidentally matched _WFO_WARMUP_BARS
      via a different code path. Always verify the constant exists in target file
      before trusting a test that checks for it.
L-51: run_cleaner.py clears cache before every run. OOM on cache miss (first worker
      per window) is the actual failure mode — not stale cache.
L-52: B9O-006 fixed what gets cached (the slice) but not the sort_index() peak
      DURING loading. sort_index() calls .copy() on the full DataFrame internally.
      Fix: check is_monotonic_increasing and slice before sort for sorted Parquet.
L-53: max_workers=6 OOM root cause: pd.read_parquet() on 897MB LTF file × 6 workers
      simultaneously on cold cache = 5.38GB peak → PyArrow C++ allocator failure.
      B9O-008 cannot help — slicing happens AFTER read_parquet() returns.
      Fix: max_workers=2 (1.79GB peak). Permanent fix: V2 shared memory architecture.
L-54: MC pre-filter is not safe for 38-month continuous evaluation. Perturbation
      compounds over 38 months producing false ruin on viable candidates.
      Stage 2 passed 1/18 candidates in run 9f73d667, eliminating WFO-0.92 and WFO-0.80
      candidates. Disable mc_prefilter for all full-history runs. Stage 4 WFO is the gate.
```
---
## What NOT to Do (additions this session)
- Do not use mc_prefilter for full-history (38-month) runs — compounds perturbation into false ruin
- Do not use max_workers > 2 until B9O-009 (shared memory) is implemented
- Do not use _SIGMOID_SCALE=221.1 — this was computed from GA 2-window samples, not full WFO
- Do not interpret GA-run _SIGMOID_SCALE as calibration — only Stage 1+4 runs produce correct distribution
- Do not raise _SIGMOID_SCALE from 310.0 without a new Stage-1+4-only calibration run
---
## Next Session Execution Order
```
STEP 1  Query overnight run:
        python scripts/diagnostics/query_run.py --run-id <latest> --section all
STEP 2  Check Stage 7 verdict count:
        If no_go only → investigate MC ruin (see Open Issues above)
        If go/borderline exists → proceed to V1 closure
STEP 3  If verdicts exist → V1 Final Documentation:
        - Update SKILL.md with final calibration constants and lessons L-49..L-54
        - Write OPERATOR_RUNBOOK_9P_DELTA.md (overnight run results + paper trade candidates)
        - Write ARCHITECTURE_9P_DELTA.md (B9O patches, V2 architectural decisions)
        - Declare Phase 1 gate status (CTP_ROADMAP.md)
STEP 5  If no verdicts → scope MC Deep architectural fix:
        Determine if Stage 5 evaluates on full 38-month range or per-window ranges
        Implement per-window MC evaluation if needed
        Re-run with fix applied
STEP 6  Begin Phase 0 (broker_support fixes) — parallel track, no blockers
```
---
## Key Files
```
src/strategies/core/data_loader.py              — v3.5.0 (all B9O patches)
src/backtesting/wfo/consistency_scorer.py       — _SIGMOID_SCALE=310.0
configs/backtesting/backtest_V1_01.yaml         — v3.0.0 (overnight run)
outputs/backtesting/backtester.db               — query for overnight run_id
docs/ctp/CTP_ROADMAP.md                         — v1.2 (V2+V3 architecture added)
outputs/RUN_ANALYSIS_9f73d667.md                — full pipeline calibration analysis
```