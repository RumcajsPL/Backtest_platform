# CONTEXT_9O.md — Block 9O: Full-History Calibration
**Date:** 2026-03-09  
**Track:** Full-history calibration (38-month, 2023-01-02 → 2026-02-28)  
**Goal:** Get net_pnl distribution from Stage 4 WFO → calculate `_SIGMOID_SCALE` for full-history track  
**Status at session end:** Calibration restarted with all B9O patches applied. Run in progress.
---
## Session Objective
Run the calibration YAML (`backtest_calibration_fullhistory_v3.yaml`, internally v4.0.0) to produce Stage 4 WFO `net_pnl` values. These are needed to compute:
```
_SIGMOID_SCALE = stdev(net_pnl across all WFO windows × all Stage 4 passers) × 0.5
```
This constant must be updated in `consistency_scorer.py` before the overnight production run on `backtest_V1_01.yaml`.
---
## Calibration Runs This Session
| Run ID | Config | Candidates | S1 Pass | Outcome | Root Cause |
|--------|--------|------------|---------|---------|-----------|
| `756a7829` | v3.0.0 | 60 | 1/60 | OOM in WFO; `fitness_weights` sum=1.50 bug | Constraints too tight; full-dataset cache; bad fitness weights |
| `9d4669a7` | v4.0.0 | 60 | 9/60 | OOM partially fixed; `mc_engine` KeyError | `data_loader.py` v3.3 not enough — `TradeSimulator` still holds `df_full` |
| `d9a81454` | v4.0.0 | 60 | 9/60 | Pipeline stopped at t=3310s with KeyError `'genetic'` | Stage 3 GA called with no `genetic:` block in YAML |
| **Current** | v4.0.0 + B9O-003/004/005 | 60 | ~20–40 expected | **In progress** | All known bugs patched |
**Key insight from run `d9a81454`:** The pipeline ran ~55 minutes before crashing at Stage 3. This means Stage 1 and Stage 4 WFO likely completed. The `net_pnl` values for `_SIGMOID_SCALE` calculation may already be in the database.
**Action required:** When the current run finishes (or even now), run `query_run.py` against run `d9a81454` to check if `wfo_window_results` are populated.
---
## Stage 1 Distributions (stable — confirmed across multiple runs)
```
Metric            Min       Avg       Max       Source runs
win_rate        0.0945    0.1486    0.2265    756a7829, 9d4669a7, d9a81454
expectancy      -2.56     -1.69     +0.35     same
profit_factor    0.67      0.830     1.02      same
trades/week      4.11     35.38     87.35     same
losing_streak     25       46.8       82      same
max_drawdown    0.0858    0.8203    1.0000    same (38-month accumulation — do not constrain)
```
---
## Calibration YAML — Current State (v4.0.0)
File: `configs/backtesting/backtest_calibration_fullhistory_v3.yaml`
Key parameters:
```yaml
run:
  max_workers: 2           # Reduced from 6 — TradeSimulator OOM fix (see B9O-OOM below)
random_search:
  samples_per_zone: 30    # 30 × 2 zones = 60 total candidates
  seed: 42
stages:
  mc_prefilter:       false
  genetic_algorithm:  false   # GA disabled — no genetic: block in YAML
  walk_forward:       true
  monte_carlo_deep:   false
  sensitivity:        false
  report:             true
constraints:
  min_win_rate:      0.11   # Loosened from 0.12 (v3) — removes bottom ~5% only
  min_expectancy:    -2.0   # Loosened from -1.0 (v3) — targets top ~60-65%
  max_losing_streak: 200    # Correct — 38-month max observed = 82
  min_profit_factor: 0.90   # Unchanged
  min_trades_per_week: 3.0  # Unchanged
  # max_drawdown: REMOVED — accumulates over 38 months
walk_forward:
  windows: W01–W07 only     # 7 windows (2023-01-02 → 2024-09-30)
  input_count: 60           # Accept all Stage 1 passers
```
---
## What Changes After This Run Completes
1. Extract `net_pnl` from `wfo_window_results` table (Stage 4)
2. Calculate: `_SIGMOID_SCALE = stdev(all net_pnl values) × 0.5`
3. Update `src/backtesting/wfo/consistency_scorer.py`: `_SIGMOID_SCALE = <new value>`
4. Update `backtest_V1_01.yaml`:
   - `max_workers: 2` (mandatory — same OOM risk)
   - `min_win_rate: 0.11`
   - `min_expectancy: -2.0`
5. Run `backtest_V1_01.yaml` overnight (full 13-window production run)
---
## Patches Applied This Session
See `ARCHITECTURE_9O_DELTA.md` for the complete, audited patch record with rollback guidance.
---
## Run History (updated)
```
87712cab  9I   calibration (3M)     _SIGMOID_SCALE=131.0
4e7135ed  9J   production           3 auto_go — COLLAPSE-UNIT validated
1fcc6398  9J   production           3+2 borderline — best: 1bfa417dc8bb
2ab4fd0e  9K   production           3+2 borderline — RSI active Stage 1; W03-only
b3237ec9  9L   production           3+2 borderline — patch validation; no regression
f545f0f2  9M   production           5 auto_go — V1 DECLARED
d1ce2b1d  9M   FH-calib-v1          0/200 — Stage 0 KeyError: scenario.py ct["max_drawdown"]
46d6edc7  9N   FH-calib-v2          0/200 — Stage 1: constraints too tight for 38-month eval
756a7829  9O   FH-calib-v3 (v4.0.0) 1/60 — fitness_weights bug; OOM; constraints loosened
9d4669a7  9O   FH-calib-v4 (v4.0.0) 9/60 — OOM not fully resolved; mc_engine KeyError
d9a81454  9O   FH-calib-v5 (v4.0.0) 9/60 — stopped t=3310s; KeyError 'genetic'; WFO may be complete
CURRENT   9O   FH-calib-v6 (v4.0.0) IN PROGRESS — all B9O patches applied
```
---
## Open Issues (carried forward)
```
SIGMOID-SCALE [P1 — now]   Extract net_pnl from d9a81454 or current run. Calculate _SIGMOID_SCALE.
PROD-RUN      [P1 — next]  Update backtest_V1_01.yaml (max_workers=2, constraints). Run overnight.
BROKER-TEST   [P1 — now]   Empirical test: does /trade/history return demo trades?
BROKER-BUGS   [P1 — now]   Four broker_support bugs (see SKILL.md Track B)
ORCH-AUDIT    [P2]         Verify no other config["key"] hard lookups remain in orchestrator.py
                           Stages to confirm: walk_forward, sensitivity, report blocks
RSI-SENS-2    [P2]         RSI zero delta × 6 runs. Remove from V2 search space.
B9N-001       [P3]         scenario.py systematic ct.get() fix — all constraint fields → V2
CAL-01        [P3]         normalisation_freq_ref_trades_per_week 20.0 → 50.0 → V2
V2-RAR        [P1/V2]      Dimensionless normalisation via Rolling Annual Range
```