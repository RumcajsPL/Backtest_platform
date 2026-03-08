# CONTEXT.md — Block 9N Handoff
**Date**: 2026-03-08
**Session**: Block 9N
**Status**: ✅ V1 PRODUCTION DECLARED. Full-history calibration in progress (v3 needed). Broker integration scoped. CTP roadmap finalised.
---
## Dual-Track Status
### Track A — Backtesting Full-History Calibration
| Step | Status | Run ID |
|------|--------|--------|
| V1 production declared | ✅ Complete | f545f0f2 |
| Code change: _MAX_EXPECTED_DRAWDOWN = 2_500.0 | ✅ Applied | — |
| Code change: scenario.py ct.get() fix | ✅ Applied | — |
| Calibration v1 (d1ce2b1d) | ❌ Failed — 0/200 (max_drawdown gate, 38-month issue) | d1ce2b1d |
| Calibration v2 (46d6edc7) | ❌ Failed — 0/200 (expectancy + win_rate constraints too tight) | 46d6edc7 |
| Calibration v3 | ⏳ Pending — loosened constraints defined this session | — |
| _SIGMOID_SCALE recalculation | 🔒 Blocked on v3 passing candidates | — |
| Full-history production run | 🔒 Blocked on calibration completing | — |
### Track B — Broker Integration (broker_support)
| Step | Status |
|------|--------|
| Working API connection confirmed | ✅ |
| Four bugs identified and scoped | ✅ |
| Empirical demo history test | ⏳ Pending — highest priority next action |
| Bug fixes applied | ⏳ Pending |
| InstrumentID map for DAX | ⏳ Pending |
| Clean manual tracker cycle | ⏳ Pending |
---
## What Was Accomplished This Session (9N)
### Full-history calibration failure diagnosis — two runs
**Run d1ce2b1d** (v1): Stage 0 failure. Root cause: `scenario.py` used `ct["max_drawdown"]` hard lookup — KeyError when field removed from YAML. Fixed.
**Run 46d6edc7** (v2): Stage 1 0/200. Root cause: expectancy and win_rate constraints calibrated for 3-month windows. Over 38 continuous months, avg expectancy = -1.83 pts, avg win_rate = 0.155. The constraints were too tight for this evaluation mode.
### Calibration v3 — constraints defined
New Stage 1 constraint targets for full-history YAML:
```yaml
expectancy:        >= -1.0    (was >= 0.0 approximately; top ~15-20% of observed -3.40→+0.41)
win_rate:          >= 0.12    (was >= 0.15 approximately; top third of observed 0.082→0.238)
trades_per_week:   keep current (only 1 failure at current threshold)
max_drawdown:      REMOVED (confirmed correct from v1 diagnosis)
max_losing_streak: 200        (confirmed correct from v1 diagnosis)
```
Target: 20–60 Stage 1 passers. Stage 4 WFO does the real filtering.
### Strategic roadmap finalised
Full 5-phase CTP roadmap agreed with revised phase order:
- Phase 0 + Phase 1 run in parallel (NOW)
- Phase 2: automated paper trading (requires both Phase 0 and Phase 1 gates)
- Phase 3: Backtesting V2 (informed by live paper trading data)
- Phase 4: Multi-strategy / multi-asset
- Phase 5: Live trading deployment
Decision: freeze V2 scope until paper trading data is available. V2 priorities determined by live evidence, not speculation.
### Broker integration fully scoped
- `broker_support` package reviewed: correct architecture, four specific bugs identified
- eToro official OpenAPI spec fetched and analysed
- 5-step development plan defined (Fix → InstrumentID → Close price → Tracker loop → Signal bridge)
- Key open question: does `/api/v1/trading/info/trade/history` return demo trades? (empirical test required)
---
## Calibration v3 — Exact YAML Changes Required
In `backtest_calibration_fullhistory_v2.yaml`, update the Stage 1 constraints block:
```yaml
constraints:
  # max_drawdown: REMOVED — correct, keep removed
  # max_losing_streak stays at 200
  expectancy: -1.0          # loosened from previous value (was ~0.0)
  win_rate: 0.12            # loosened from previous value (was ~0.15)
  # trades_per_week: keep current value
```
Save as `backtest_calibration_fullhistory_v3.yaml`. Run it. Share query_run.py output.
**Expected result**: 20–60 Stage 1 passers. If still 0, share output and we diagnose further.
**After v3 passes**: assistant calculates `_SIGMOID_SCALE = stdev(net_pnl_of_passers) × 0.5`. Then update `consistency_scorer.py` and run `backtest_production_fullhistory_v2.yaml` (with the same loosened constraints) overnight.
---
## Calibration Run Data — 46d6edc7 (reference for v3 design)
```
Stage 1 metric distributions (200 candidates, 2023-01-02 → 2026-02-28):
metric                 min        avg        max
win_rate             0.0816     0.1553     0.2378
max_drawdown         0.0501     0.7906     1.0000   ← correctly unconstrained
expectancy          -3.4000    -1.8348     0.4100
profit_factor        0.6700     0.8161     1.0300
trades/week          1.3100    33.5200    96.5000
losing_streak       20.0000    44.3000    78.0000   ← correctly unconstrained
Rejection breakdown:
  expectancy       102 / 200 failed
  win_rate          97 / 200 failed
  trades_per_week    1 / 200 failed
```
This is the calibration dataset for all full-history constraint design. Keep this data.
---
## Architecture Invariants (current — dual track)
### 3-Month Production Track (V1 — FROZEN)
```
_SIGMOID_SCALE                       = 131.0     ← DO NOT CHANGE for 3-month runs
_MAX_EXPECTED_DRAWDOWN               = 1_000.0   ← must be restored after full-history work
_MAX_EXPECTED_VARIANCE               = 100_000.0
wfo_collapse_drawdown_threshold      = 400.0 pts
normalisation_expectancy_ref_pts     = 3.0 pts
normalisation_freq_ref_trades_per_week = 20.0
```
### Full-History Track (38-month — IN PROGRESS)
```
_MAX_EXPECTED_DRAWDOWN               = 2_500.0   ← currently applied in code
_SIGMOID_SCALE                       = TBD        ← requires v3 calibration run
Stage 1 expectancy constraint        = -1.0
Stage 1 win_rate constraint          = 0.12
max_drawdown, max_losing_streak      = removed/200
```
### CRITICAL RESTORE REMINDER
After completing the full-history run sequence, restore in `consistency_scorer.py`:
```python
_MAX_EXPECTED_DRAWDOWN: float = 1_000.0
_SIGMOID_SCALE: float = 131.0
```
before running any new 3-month production runs. Or track via YAML field (V2-RAR backlog item).
---
## Open Issues
| ID | Priority | Description | Target |
|----|----------|-------------|--------|
| RSI-SENS-2 | P2 | RSI zero delta × 6 runs. Remove from V2 search space. | V2 |
| CAL-01 | P3 | normalisation_freq_ref_trades_per_week=20.0 → 50.0 | V2 |
| RR-CEILING-2 | P3 | Revert safe zone rr_target.max 8.5 → 7.0 in next YAML | Next YAML |
| B9N-001 | P3 | scenario.py systematic ct.get() fix for all constraint fields | V2 |
| V2-RAR | P1 (V2) | Dimensionless normalisation via Rolling Annual Range | V2 |
| DYN-WFO | P2 (V2) | Dynamic window generation from data_range + window_size | V2 |
| BROKER-TEST | P1 (now) | Empirical test: does /trade/history return demo trades? | Immediately |
| BROKER-BUGS | P1 (now) | Four broker_support bugs to fix before new development | Step 1 |
---
## Run History (complete)
| Run | Block | Type | auto_go | Notes |
|-----|-------|------|---------|-------|
| 87712cab | 9I | calibration | — | _SIGMOID_SCALE=131.0 |
| 4e7135ed | 9J | production | 3 | COLLAPSE-UNIT fix validated |
| 1fcc6398 | 9J | production | 3+2 borderline | Best: 1bfa417dc8bb |
| 2ab4fd0e | 9K | production | 3+2 borderline | RSI active Stage 1; W03-only |
| b3237ec9 | 9L | production | 3+2 borderline | Patch validation; no regression |
| f545f0f2 | 9M | production | **5** | **V1 declared. First exploration auto_go. W03 broken.** |
| d1ce2b1d | 9M | FH calib v1 | 0 | Stage 0 fail — scenario.py KeyError. Diagnosed. |
| 46d6edc7 | 9N | FH calib v2 | 0 | Stage 1 0/200 — constraints too tight for 38-month eval. Diagnosed. |
---
## Paper Trade Candidates (unchanged from 9M)
| Candidate | Zone | Run | WFO | Ruin | p5_equity | worst_dd | Use |
|-----------|------|-----|-----|------|-----------|----------|-----|
| c4f0aea11a3e | exploration | f545f0f2 | 0.9166 | 0.000 | 9,230 | 0.131 | ✅ Primary |
| da38ecc0ddc6 | safe | f545f0f2 | 0.9257 | 0.000 | 7,547 | 0.471 | ✅ Secondary |
| 3a149e208a62 | safe | f545f0f2 | 0.9408 | 0.000 | 7,236 | 0.425 | ❌ Fragile |
Paper trade YAMLs already generated in `outputs\backtesting\trading_yamls\`.
Paper trading can start independently of the full-history run sequence.
---
## Next Session — 9O (or 10A if major milestone reached)
**Primary**: Share calibration v3 query_run.py output → receive new _SIGMOID_SCALE → run full-history production overnight.
**Secondary** (if calibration is running / waiting): Begin broker_support bug fixes. Run empirical demo history test. Share result.
**If full-history production completes**: analyse results — focus on W05–W08 (2024 stress windows). Gate: ≥3 auto_go candidates surviving stress windows.