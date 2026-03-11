# CTP Project — Handoff Context Block 9P
*Generated: 2026-03-11 | Status: Phase 1 closing*
---
## Where We Are
Phase 1 (full-history backtesting) is functionally complete. Run 63b85270 produced viable
paper trade candidates and confirmed the pipeline is production-stable. One confirmation run
remains before the Phase 1 gate is formally closed, then the project pivots to Phase 0
(broker_support fixes) as the active development track.
---
## Immediate Next Action
**Run the confirmation run** — single YAML change only:
```yaml
# configs/backtesting/backtest_V1_01.yaml
sensitivity:
  input_count: 10    # was 5
```
This aligns the verdict set with the MC Deep set (both now top 10 by WFO score), giving
verdicts to all 10 MC-evaluated candidates including c209820886c8 (strongest MC profile,
ruin=0.000, avg_equity=9370, currently unverdiected because it ranked 10th by WFO).
After the confirmation run: analyse verdicts, update paper trade candidate list if
c209820886c8 or others upgrade, then declare Phase 1 closed.
**No other config changes.** All other settings confirmed correct for full-history runs.
---
## Phase 1 Gate — Status

| Criterion | Status |
|-----------|--------|
| Pipeline production-stable (no crashes, clean completion) | ✅ PASS |
| Full-history calibration constants confirmed | ✅ PASS (310.0, N=231) |
| At least one GO or BORDERLINE verdict | ✅ PASS (4 borderline) |
| Runtime acceptable for overnight cadence | ✅ PASS (~7 hours confirmed) |
| MC-DEEP-FULLHIST severity resolved | ✅ DOWNGRADED (top candidates viable) |
| All MC-evaluated candidates receive verdicts | ⏳ PENDING (confirmation run) |
| Phase 1 gate formally closed | ⏳ PENDING |
---
## Run 63b85270 — Summary
```
Config:   backtest_V1_01.yaml v3.0.0 | scenario: capital_accumulation
Runtime:  25742.5s (~7 hours) ← confirmed acceptable
Stage 1:  234/400 passed (58.5%) — distributions stable vs prior baseline
Stage 4:  30 WFO candidates (all collapsed — windows_evaluated < 13 due to structural suppression)
Stage 5:  10 candidates evaluated; 4 with ruin=0.000; 1 no_go (ruin=0.593)
Stage 7:  4 BORDERLINE, 1 NO_GO
Null windows: 7 (4 WINZIP-32 cosmetic, 3 REJECTED_INSUFFICIENT_TRADES — non-blocking)
```
---
## Paper Trade Candidates — Run 63b85270
### PRIMARY — c424a0e04327
```
WFO score:      0.8108  (rank 1)
Windows:        9/13 evaluated, 9/9 profitable (frac_pos=1.000)
Median return:  +313 pts/window
MC ruin:        0.000
MC avg equity:  8,778
MC p5 equity:   5,383
Sensitivity:    1 spike — atr_multiplier (delta=+0.154 at step -1, asymmetric upward)
                Spike is upward: smaller atr_multiplier performs better.
                Implication: value is improvable, not fragile on the downside.
Verdict:        BORDERLINE (spike only)
YAML:           outputs/backtesting/trading_yamls/63b85270_c424a0e04327_strategy.yaml
Recommended:    PROMOTE to paper trading. Strongest overall profile.
```
### SECONDARY — 20745ca991be
```
WFO score:      0.7201  (rank 2)
Windows:        7/13 evaluated, 7/7 profitable (frac_pos=1.000)
Median return:  +926 pts/window  ← high
MC ruin:        0.054            ← marginal
MC avg equity:  8,099
Sensitivity:    clean (no spikes)
Verdict:        BORDERLINE (ruin 0.054 > 0.05 threshold)
YAML:           outputs/backtesting/trading_yamls/63b85270_20745ca991be_strategy.yaml
Recommended:    MONITOR in paper trading. High upside but regime-sensitive — observe
                live performance before full allocation.
```
### MONITOR — c42f8b009283
```
WFO score:      0.6473  (rank 4)
Windows:        evaluated, viable
MC ruin:        0.000
MC avg equity:  5,147
Sensitivity:    parameter fragility observed (many REJECTED_CONSTRAINTS on perturbation)
Verdict:        BORDERLINE
YAML:           outputs/backtesting/trading_yamls/63b85270_c42f8b009283_strategy.yaml
Recommended:    LOW PRIORITY paper trade. Fragility concern — watch closely.
```
### LOW PRIORITY — c4f0aea11a3e
```
WFO score:      0.6233  (rank 5)
Windows:        12/13 evaluated, 2/12 profitable (frac_pos=0.167)  ← structural concern
MC ruin:        0.000
MC avg equity:  5,756
Verdict:        BORDERLINE
YAML:           outputs/backtesting/trading_yamls/63b85270_c4f0aea11a3e_strategy.yaml
Recommended:    DO NOT promote yet. frac_pos=0.167 means only 2 windows are profitable —
                equity curve is driven by 2 exceptional windows masking 10 losing ones.
                Await confirmation run; if no improvement, discard.
```
### UNVERDIECTED — c209820886c8 (confirmation run target)
```
WFO score:      0.5699  (rank ~10 — outside top-5 in run 63b85270)
MC ruin:        0.000
MC avg equity:  9,370  ← STRONGEST MC PROFILE of all 10 evaluated candidates
Verdict:        NONE in run 63b85270 (ranked outside sensitivity.input_count=5)
Expected:       Will receive verdict in confirmation run (sensitivity.input_count=10)
Recommended:    HIGH WATCH. If verdict is GO or BORDERLINE (clean), consider promoting
                above c42f8b009283 and c4f0aea11a3e based on MC profile alone.
```
### NO_GO — 5d89157ad626
```
WFO score:      0.7094  (rank 3)
MC ruin:        0.5927  ← disqualifying
Verdict:        NO_GO
Action:         Discard. Do not use.
```
---
## Confirmation Run — What to Watch For
1. **c209820886c8 verdict**: GO or clean BORDERLINE → promote above c42f8b009283.
   If NO_GO → strong MC profile was not enough; discard.
2. **Ranks 6–9 (currently unverdiected)**: Any surprise GO candidates? Unlikely given
   WFO scores below 0.57, but note if any appear.
3. **c4f0aea11a3e frac_pos**: sensitivity profile unchanged → discard signal.
4. **Overall verdict count**: If confirmation run still yields 0 GO verdicts, that is
   acceptable — BORDERLINE candidates are promotable to paper trading. Phase 1 closes
   regardless of GO/BORDERLINE split.
5. **Runtime**: Should remain ~7 hours (sensitivity.input_count change adds negligible compute).
---
## Phase 0 — Broker Support (next active track)
Begins after confirmation run analysis. Four bugs to fix in order:
### Bug 1 — Wrong portfolio endpoint
```python
# client.py — change:
'/demo/portfolio'  →  '/demo/pnl'
```
### Bug 2 — Orphaned fetch_closed_trades function
```python
# client.py — second fetch_closed_trades is a free function at module level
# Fix: indent as class method, delete stub
```
### Bug 3 — Wrong date parameter name
```python
# client.py — change:
params={'from': date_str}        # or 'fromDate'
params={'minDate': date_str}     # confirmed official eToro param
```
### Bug 4 — Wrong trade field alias + missing fields
```python
# models.py — change:
Field(alias='id')  →  Field(alias='positionId')
# Add fields: fees, leverage, sl_rate, tp_rate
```
### After bug fixes — Empirical demo history test (P1, mandatory before architecture)
```python
# Run this before designing any new broker features:
client._make_request(
    'GET',
    'api/v1/trading/info/trade/history',
    params={'minDate': '2026-01-01'}
)
# Question: do demo account trades appear in real-account history endpoint?
# This determines whether snapshot-comparison is permanent or replaceable.
# DO NOT design close-price enrichment or position reconciliation until this is answered.
```
---
## CTP Roadmap — Phase Sequence
```
Phase 0  Broker fixes (4 bugs) + empirical demo history test     ← NEXT
Phase 1  Full-history backtesting                                 ← CLOSING (confirmation run)
Phase 2  Automated paper trading (signal → eToro order)          ← blocked on Phase 0
Phase 3  V2 backtesting engine (RawDataStore + SharedMemory)     ← deferred
Phase 4  Live trading with risk controls                          ← far future
Phase 5  Strategy Setup Builder (V3 meta-optimiser)              ← far future
```
---
## Key Numbers to Remember
| Constant | Value | Source |
|----------|-------|--------|
| _SIGMOID_SCALE (full-history) | 310.0 | N=231, runs 2912e028 + 519f84e2 |
| _SIGMOID_SCALE (3-month) | 131.0 | run 87712cab — restore for 3-month runs |
| _MAX_EXPECTED_DRAWDOWN (full-hist) | 2,500.0 | confirmed |
| _MAX_EXPECTED_DRAWDOWN (3-month) | 1,000.0 | restore for 3-month runs |
| max_workers | 2 | HARD LIMIT — OOM at 6 confirmed |
| Full-history runtime | ~7 hours | confirmed run 63b85270 |
| Stage 1 pass rate | ~58% | stable across 5 full-history runs |
| sensitivity.input_count | 5 → **10** | change for confirmation run |
| monte_carlo.deep.input_count | 10 | unchanged |
---
## Open Issues (carry forward)
| ID | Severity | Description |
|----|----------|-------------|
| B9O-009 | LOW | V2 shared memory — OOM safety, not runtime urgency |
| WINZIP-32 | INFO | Cosmetic temp file lock on Windows, ~4 null windows/run |
| RSI-SENS-2 | CLOSED | RSI dead — remove in V2 implementation |
| MC-DEEP-FULLHIST | DOWNGRADED | Top candidates viable; V2 per-window fix still recommended |
---
## Documents Produced This Block
| File | Description |
|------|-------------|
| `outputs/RUN_ANALYSIS_63b85270.md` | Full overnight run analysis |
| `outputs/ARCHITECTURE.md` | Production-ready v5.0.0 (V2/V3 developer reference) |
| `outputs/OPERATOR_RUNBOOK.md` | Production-ready v4.0.0 (operator reference) |
| `outputs/SKILL.md` | Updated skill (replace user skill file) |
| `outputs/CONTEXT_9P.md` | This file |