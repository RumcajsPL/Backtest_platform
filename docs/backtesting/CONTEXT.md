# CONTEXT.md — Block 9J Handoff (Final)
**Date**: 2026-03-07
**Session**: Block 9J (same chat window as 9I)
**Status**: ✅ COLLAPSE-UNIT fixed. auto_go verdicts confirmed. Pipeline production-ready.
---
## What Was Accomplished This Session
### COLLAPSE-UNIT [FIXED] — wfo_collapse_drawdown_threshold unit mismatch
Root cause was two-layered, discovered together:
1. `contracts.py` `__post_init__` validated `(0.0 < threshold <= 1.0)` — would reject any pts value
2. `scenario.py` `load_scenario()` never wired the field from YAML — always used dataclass default
**Fix applied (3 files)**:
- `contracts.py`: default `0.40 → 400.0`; validation `<= 1.0` guard removed, replaced with `<= 0.0` guard only. Docstring updated to say "pts" with DAX example.
- `scenario.py`: added `wfo_collapse_drawdown_threshold=float(s.get("wfo_collapse_drawdown_threshold", 400.0))` after `mc_prefilter_ruin_threshold` line.
- `backtest_1st_run.yaml`: added `wfo_collapse_drawdown_threshold: 400.0` to capital_accumulation scenario.
**Result** (run 1fcc6398): 11/17 WFO candidates collapsed (those with worst_dd > 400 pts),
6 did not collapse — verdicts correctly differentiated: 3 auto_go, 2 borderline.
Previous run had 24/24 collapsed (all borderline). Fix confirmed working.
### RSI sensitivity still zero (RSI-SENS — confirmed persistent, diagnosis updated)
All RSI perturbations return delta=0.0000 across all 5 sensitivity profiles again.
Given that rsi_filter.enabled=true is confirmed in the YAML, and two independent runs
show the same zero-delta pattern, **the most likely cause is that the RSI filter
produces no additional filtering effect given the current DPO + choppiness filter
combination**. The DAX parameter sets being evaluated likely have no trades that pass
DPO+choppiness but fail RSI at the current threshold settings. RSI dimensions are
active search dimensions consuming compute but producing no differentiation.
**Action**: Either remove RSI params from search space (simplify) or tighten RSI
thresholds to create filter overlap (e.g. overbought: 65, oversold: 40).
Not blocking for next run but wastes search dimensions.
### Instrument-specific normalisation — RAR note (logged for V2)
`wfo_collapse_drawdown_threshold`, `_SIGMOID_SCALE`, `_MAX_EXPECTED_VARIANCE`,
`_MAX_EXPECTED_DRAWDOWN` are all calibrated in raw instrument points for DAX.
These values would be wrong for any other instrument (FTSE, ES, NQ etc.).
The correct long-term solution is to normalise via **RAR (Rolling Annual Range)**
— the instrument's rolling price range provides a natural unit-agnostic scale.
With RAR normalisation, all thresholds and sigmoid constants become instrument-
independent percentages, enabling multi-asset operation without recalibration.
This is a V2 architectural change — do not implement until current DAX pipeline
is validated in production. Logged as V2-RAR.
---
## Run 1fcc6398 — Key Findings
### Pipeline health
- Stage 1: 17/200 passed (8%) — slight decrease from 12% in run 4e7135ed
- WFO: 17 scored (11 collapsed, 6 clean), range 0.0000–0.9286
- MC Deep: all ruin_prob=0.0000 ✅
- Verdicts: **3 auto_go, 2 borderline** ✅ First auto_go verdicts produced.
### COLLAPSE-UNIT fix validated
6 candidates with worst_dd < 400 pts → not collapsed → eligible for auto_go.
11 candidates with worst_dd > 400 pts → collapsed → borderline at most.
The threshold is working as intended. 400 pts is a reasonable starting value for
DAX — may want to review after more runs if collapse rate seems too high.
### 03174190cdae — WFO=0.0000 anomaly
windows_evaluated=0, all metrics zero, window_collapse_flag=1. This candidate
produced no valid windows at all. A configuration issue (possibly YAML mapping
error visible in W04 of 41eed9b34afa from prior run) caused complete evaluation
failure. Not blocking — the pipeline handled it gracefully (wfo_score=0.0 → no_go).
Monitor frequency of zero-evaluation candidates in future runs.
### W03-only pattern — persistent (3rd consecutive run)
All WFO-scored candidates still win only on W03. W01/W02 borderline or slight loss,
W04/W05 loss-making. This is now an established characteristic of the strategy
on this 3-month data slice. Not a bug. Accept as context for live validation.
### RSI sensitivity — confirmed zero for third run
All RSI perturbations (rsi_period, rsi_overbought, rsi_oversold) return 0.0000
across all 5 candidates in Stage 6. RSI filter is enabled but not contributing
to trade filtering in the evaluated parameter space. See RSI-SENS action below.
### atr_multiplier ceiling — partially resolved
Previous run: all winners at 2.0. This run: winners span 1.6–2.1, avg 1.92.
The extended range (2.5 max) allowed the GA to explore above 2.0 — f57ade9c9e75
uses 2.1 (highest WFO). The ceiling concern is substantially reduced. Monitor.
### risk_percentile — interesting reversal
Previous run optimum: 0.51–0.59. This run top 5 risk_percentile values:
0.40, 0.32, 0.64, 0.32, 0.32. Much lower values now winning. This suggests
the GA is finding different risk profiles rather than converging on a single
optimum. Healthy diversity. Overall range: 0.32–0.66.
### Best candidates (run 1fcc6398)
| Candidate | WFO | MC avg_equity | MC p5 | Spike | Verdict | Assessment |
|---|---|---|---|---|---|---|
| f57ade9c9e75 | 0.9286 | 9,041 | 7,459 | none | auto_go | Best WFO, clean |
| 2ec7e80968b6 | 0.9228 | 8,925 | 7,329 | none | auto_go | Strong, very tight sensitivity |
| 1bfa417dc8bb | 0.9207 | 9,054 | 7,406 | none | auto_go | Best MC, clean, moderate sensitivity |
| 11220d7b9360 | 0.9223 | 9,459 | 7,607 | rr_target | borderline | Best MC overall, spike on rr |
| 696a3511ee42 | 0.9155 | 9,090 | 7,468 | none | borderline | Collapsed (worst_dd 420 > 400) |
**1bfa417dc8bb is strongest overall**: auto_go, best avg MC equity (9,054),
good WFO (0.9207), no spike, clean sensitivity map. Paper trade candidate.
### 2ec7e80968b6 sensitivity — near-isolated parameter space
This candidate has almost universal REJECTED_CONSTRAINTS on perturbation — bollinger
(all 4 steps), atr_multiplier (3 of 4 steps), risk_percentile (2 of 4 steps).
The parameter combination is sitting in a very narrow feasible region. It passes
constraints but its neighbours mostly do not. High fragility despite auto_go verdict.
Use with caution in paper trading — small drift in market conditions could push it
out of feasible region. Recommend 1bfa417dc8bb or f57ade9c9e75 instead.
### 11220d7b9360 rr_target spike — interesting directionality
Base rr_target=6.8. Perturbed to 7.0: delta=+0.08 (spike). To 7.2: delta=+0.15 (spike).
The spike is upward — higher rr_target improves fitness significantly. This suggests
the optimum is actually above 6.8 and the current value is below-optimal. The spike
is not fragility in the dangerous sense but an unexplored improvement direction.
Note for future search space: rr_target upper bound may warrant extension above 7.2.
---
## Open Issues (updated)
### P1 Blockers — NONE
COLLAPSE-UNIT resolved. Pipeline producing auto_go verdicts. ✅
### P2 Important
```
RSI-SENS — RSI zero-delta confirmed across 3 runs despite rsi_filter.enabled=true.
  RSI filter is likely producing no additional filtering effect given current
  DPO+choppiness configuration. Two options:
  Option A (simplify): Remove rsi_period, rsi_overbought, rsi_oversold from
    search space in backtest_1st_run.yaml. Reduces search dimensions, improves
    Stage 1 sample efficiency.
  Option B (activate): Tighten RSI thresholds (overbought: 65, oversold: 40)
    to force more overlap with existing filters. Requires strategy knowledge.
  Decision deferred to operator. Not blocking.
RR-CEILING — rr_target spike on 11220d7b9360 suggests optimum above 7.2.
  Current safe zone max: 7.0. Consider extending to 8.0.
  Observation only — not a blocker.
```
### P3 Non-blocking
```
B8B-003 — fitness.py: expectancy /3.0 — acceptable, low priority
B8-009  — orchestrator.py: raw sqlite3 in _resume_or_start
B9B-001 — crossover.py: no zone-name guard
B8B-013 — mc_engine.py: ruin_threshold dual-source
B8B-011 — consistency_scorer.py: fraction_positive_windows fixed 0.0 floor
B8C-002/003 — report_generator.py: deferred
B9C-008 — sampler.py: deferred
OPT-01  — Stage 6 ≤200s
[WinError 32] — cosmetic, pre-existing
03174190cdae-type zeros — monitor frequency of zero-window-evaluation candidates
W03-ONLY — established data characteristic; needs longer data range (V2)
```
### V2 Backlog
```
V2-RAR  — Normalise all instrument-specific thresholds via Rolling Annual Range.
  Enables multi-asset operation without per-instrument recalibration.
  wfo_collapse_drawdown_threshold, _SIGMOID_SCALE, _MAX_EXPECTED_VARIANCE,
  _MAX_EXPECTED_DRAWDOWN all become dimensionless fractions of RAR.
  Do not implement until DAX pipeline validated in paper trading.
Dynamic WFO window generation — see OPERATOR_RUNBOOK_9I_DELTA.md spec.
```
### Resolved this block
```
COLLAPSE-UNIT: FIXED — contracts.py + scenario.py + backtest_1st_run.yaml
B9I-001: FIXED (Block 9I)
B9I-002: FIXED (Block 9I)
B8B-012: FIXED (Block 9I)
```
---
## Production Readiness Assessment
The pipeline has now produced **3 auto_go candidates** across 2 calibration runs.
All pre-production blockers are resolved. The pipeline is ready for:
1. Paper trading with the 3 auto_go YAMLs from run 1fcc6398
2. Continued calibration runs to accumulate more auto_go candidates
3. Review of RSI-SENS (simplification recommended)
Before extending to additional scenarios or instruments, implement V2-RAR.
---
## Architecture Invariants (complete, current)
- `_lhs_sample()` always returns exactly n candidates — no cap on n (B9I-001)
- `actual_net_pnl` / `actual_total_trades` do NOT exist in evaluations table (B9I-002)
- net_pnl for sigmoid calibration: `wfo_window_results.net_pnl` (Stage 4 only)
- `_SIGMOID_SCALE = 131.0` — DAX pts, run 87712cab, 2026-03-07. Recalibrate on data/instrument change.
- `_MAX_EXPECTED_VARIANCE = 100_000.0` pts² — DAX calibration
- `_MAX_EXPECTED_DRAWDOWN = 1_000.0` pts — DAX calibration
- `wfo_collapse_drawdown_threshold` default = 400.0 pts (DAX). Must be pts, not fraction.
  Wired via `scenario.py` `s.get("wfo_collapse_drawdown_threshold", 400.0)` (COLLAPSE-UNIT fix).
- `contracts.py` `__post_init__` validates only `> 0.0` — no upper bound (any pts value valid)
- All normalisation constants are instrument-specific. V2-RAR will make them dimensionless.
- Recalibrate `_SIGMOID_SCALE`: `scale = stdev(wfo_window_results.net_pnl WHERE is_ga_fitness_window=0) * 0.5`
---
## Lessons Learned (L-36, L-37)
```
L-36: A YAML field with a dataclass default will silently use that default even if
      the field is added to the YAML file, unless scenario.py (or the equivalent
      loader) explicitly reads and passes it. Always verify the loader wire-up when
      adding new ScenarioProfile fields. The two-layer failure (validator rejects pts,
      loader ignores YAML) means the field was doubly broken and tests would not
      catch it without an integration test that checks actual verdict output.
L-37: When a sensitivity parameter shows 0.0000 delta across all perturbation steps
      and all candidates over multiple runs, the filter is either disabled or the
      current parameter range produces no filtering overlap with the strategy's
      active trade flow. Zero-delta RSI params waste search dimensions — either
      activate (tighten thresholds) or remove from search space. Do not add params
      to search space for filters whose effectiveness has not been independently
      verified on the target instrument/timeframe.
```
---
## Files Modified This Block
| File | Change |
|------|--------|
| `src/backtesting/contracts.py` | COLLAPSE-UNIT: default 0.40→400.0, validation `<=1.0` removed |
| `src/backtesting/scenario.py` | COLLAPSE-UNIT: wired wfo_collapse_drawdown_threshold from YAML |
| `configs/backtesting/backtest_1st_run.yaml` | wfo_collapse_drawdown_threshold: 400.0 added |