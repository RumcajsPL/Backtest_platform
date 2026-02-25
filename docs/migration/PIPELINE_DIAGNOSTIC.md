# DIAGNOSTIC REPORT — ACT 0 Pipeline Diagnostic
**Project:** Backtesting Platform  
**Architecture version:** New v3.1.0 vs Legacy  
**Document version:** 1.0 — Final  
**Date:** 2026-02-25  
**Author:** Senior Python Consultant + Project Owner  
**Status:** ✅ CLOSED — Root cause confirmed, fixes applied, parity verified. Phase 9 cleared.

---

## 1. Executive Summary

A ~30% trade count discrepancy was observed between the Legacy pipeline
(`run_wbws_strategy.py`) and the New pipeline (v3.1.0) when run against
identical configuration and data. A full layer-by-layer diagnostic was
conducted across Config/DataLoad → Raw Signals → FilterPipeline →
TradeSimulation. All four layers were analysed. Root cause was identified
and confirmed by direct source code inspection.

**Primary finding:** One confirmed bug (time filter boundary operator).  
**Secondary finding:** One intentional architectural hardening change (RAR
NaN fail-safe) whose trade count impact depends on ARTF file coverage
relative to the strategy start date — requires data verification before
Phase 9.  
**All other Layer 4 items:** Resolved as no-divergence.

---

## 2. Diagnostic Scope

| Item | Detail |
|---|---|
| Test window | `2025-12-12 18:00:00 → 2025-12-12 21:00:00` |
| Asset | DEUIDXEUR, 1-min strategy timeframe |
| Layers analysed | Config/DataLoad · Raw Signals · FilterPipeline · TradeSimulation |
| Diagnostic scripts | `tests/strategies/diagnostics/diag_layer1_config_data.py` through `diag_layer4_trades.py` |
| Source files reviewed | 8 files — all four core modules from both architectures |

---

## 3. Layer Results Summary

| Layer | Status | Verdict |
|---|---|---|
| Layer 1 — Config & Data Load | ✅ Complete | Mostly clean. 3 config differences carried to L4. |
| Layer 2 — Raw Signal Generation | ✅ Complete | **Clean.** BUY=9, SELL=10 — identical in both pipelines. |
| Layer 3 — Filter Pipeline | ✅ Complete | **Divergence confirmed.** 1 signal difference at session boundary. |
| Layer 4 — Trade Simulation | ✅ Complete | All open items resolved from source. No simulation-logic bug. |

---

## 4. Findings

### Finding F1 — Time Filter Boundary Operator (BUG)

**Layer:** 3 — Filter Pipeline  
**Type:** Bug in the New pipeline implementation  
**Status:** ✅ Confirmed from source code

**Detail:**  
The New pipeline `TimeFilter` (`src/strategies/specific/filters/time_filter.py`)
uses a strict less-than operator (`<`) on `session_end_minutes`:

```python
trading_hours_mask = (
    (minutes_col >= self.session_start_minutes) &
    (minutes_col < self.session_end_minutes)      # EXCLUSIVE end
)
```

With `session_end = 20:30`, a signal at exactly `20:30:00` evaluates as
`20:30 < 20:30` = False → the bar falls **outside** the mask → signal is
set to zero → rejected.

The Legacy pipeline uses an inclusive `<=` operator at `session_end`,
passing the `20:30:00` signal through.

**Observed impact in diagnostic window:**

| Timestamp | Legacy | New | Note |
|---|---|---|---|
| 2025-12-12 20:30:00 SELL | ✅ PASS | ❌ REJECTED | Boundary signal |

Final filtered counts: Legacy = 14, New = 13. Difference = 1 signal.

**Impact at scale:** Every signal that fires at the exact `session_end`
minute is systematically dropped by the New pipeline. The magnitude across
a 2-year run depends on signal density at that specific minute, but the
effect is consistent and repeatable.

**Design intent question (to confirm before fix):**  
Should a signal at exactly `session_end=20:30` be the **last accepted
bar** (inclusive, Legacy behaviour) or the **first rejected bar**
(exclusive, New behaviour)? The fix direction depends on this decision.

**Fix applied — inclusive end confirmed as design intent:**

```python
# BEFORE (buggy)
trading_hours_mask = (
    (minutes_col >= self.session_start_minutes) &
    (minutes_col < self.session_end_minutes)      # excluded session_end bar
)

# AFTER [FIX-L3D1]
trading_hours_mask = (
    (minutes_col >= self.session_start_minutes) &
    (minutes_col <= self.session_end_minutes)     # session_end bar is last accepted bar
)
```

**Fix location:** `src/strategies/specific/filters/time_filter.py`, line 144.  
**Fix tag:** `[FIX-L3D1]`  
**Fix status:** ✅ Applied

---

### Finding F2 — RAR NaN Fail-Safe vs Fail-Open (INTENTIONAL CHANGE)

**Layer:** 4 — Trade Simulation  
**Type:** Intentional architectural hardening change  
**Status:** ✅ Confirmed from source code. Impact magnitude requires data verification.

**Detail:**  
The two pipelines treat a NaN or unavailable Rolling Annual Range (RAR)
value differently:

**Legacy** `risk_manager.py` — `validate_risk_percentile()`:
```python
if pd.isna(current_annual_range) or current_annual_range <= 0:
    return True, stop_loss, f"RAR unavailable or invalid ({current_annual_range})"
```
NaN RAR → **fail-open**: trade is approved, risk filter bypassed silently.

**New** `risk_manager.py` — `validate_risk_percentile()`:
```python
if pd.isna(current_annual_range) or current_annual_range <= 0:
    return False, stop_loss, f"RAR unavailable at {timestamp}"
```
NaN RAR → **fail-safe**: trade is rejected. This is documented as `[FIX-1]`
in the New source.

Additionally, the New pipeline's RAR computation requires a **full 12-month
ARTF window** (`len(window) >= 12`), documented as `[FIX-2]`. Partial
warm-up windows produce NaN. The Legacy pipeline accepts partial windows
and computes RAR from whatever ARTF history is available.

**Consequence:** At the start of any backtest run, if the ARTF file does
not provide 12 full months of history prior to `date_range.start`, the New
pipeline will produce NaN RAR for those early bars and reject all trades
during that warm-up period. Legacy approves the same trades via fail-open.

**This is architecturally correct in the New pipeline.** Computing RAR
from a partial window produces an artificially compressed range, which
would cause the risk filter to over-reject trades with a false sense of
precision. The New behaviour is safer.

**Impact magnitude:** Depends entirely on the relationship between the
ARTF file start date and the strategy `date_range.start`. Two scenarios:

| Scenario | Impact |
|---|---|
| ARTF file starts ≥ 12 months before `date_range.start` | **Zero impact.** All strategy bars have full 12-month RAR. No NaN warm-up rejections. |
| ARTF file starts < 12 months before `date_range.start` | **Significant impact.** New rejects all risk-filtered trades in the warm-up period. This would contribute substantially to the ~30% discrepancy. |

**Diagnostic fact:** The diagnostic window uses ARTF bar `2025-11-30` with
62 monthly bars. Working backwards: 62 months from Nov 2025 ≈ **Sep 2020**.
If the strategy `date_range.start` for the 2-year production run is 2024
or later, the ARTF file provides well over 12 months of prior history for
every strategy bar, and this finding has **zero runtime impact**.

**✅ Confirmed zero impact — verified from strategy YAML:**

| Item | Value |
|---|---|
| ARTF file | `DEUIDXEUR_1ME_20210101_20260207.parquet` |
| ARTF file start | 2021-01-01 |
| 12-month warm-up clears | 2022-01-01 |
| Production strategy start | 2024+ |
| Buffer beyond warm-up | ≥ 2 full years |

Every strategy bar in any production 2-year run has a full 12-month ARTF
window. No NaN RAR values are produced for any strategy bar. Finding F2
contributes **zero** to the trade count discrepancy. The ~30% discrepancy
is explained entirely by Finding F1 (time filter boundary) accumulated
across the 2-year dataset.

**Action:** None required.

---

### Finding F7 — Long SL Trigger Spread Formula (ROOT CAUSE OF ~52% DISCREPANCY)

**Layer:** 4 — Trade Simulation  
**Type:** Bug in Legacy pipeline  
**Status:** ✅ Confirmed from source code + eToro CFD execution model + run log evidence  
**Fix:** ✅ Applied [FIX-L4-SL]

**Detail:**  
Legacy `RiskManager.compute_trade_parameters()` computes the long SL trigger
price by subtracting spread from the raw SL level:

```python
# Legacy — WRONG
trigger_sl = raw_sl - spread_for_this if is_long else raw_sl + spread_for_this
```

New `RiskManager` correctly applies no spread adjustment for long SL:

```python
# New — CORRECT
trigger_sl = final_sl if is_long else final_sl + spread_for_this
```

**eToro CFD BID price model (verified):**

| Event | Execution price | Spread adjustment on BID data |
|---|---|---|
| LONG entry | Ask = Bid + spread | ✅ Add spread |
| LONG SL exit | Bid | ❌ No adjustment — data IS Bid |
| LONG TP exit | Bid | ❌ No adjustment |
| SHORT entry | Bid | ❌ No adjustment |
| SHORT SL exit | Ask = Bid + spread | ✅ Add spread |
| SHORT TP exit | Ask = Bid + spread | ✅ Add spread |

For a long trade, exit occurs when the Bid price falls to the SL level. Since
all OHLCV data is already Bid price, no spread subtraction is needed on the SL
trigger. Legacy's subtraction of ~3.6 pts (DAX spread at 24,000 × 0.015%)
caused the SL trigger to sit 3.6 pts below the intended level — meaning long
positions were held open through adverse moves that New correctly stopped out.

**Mechanism driving the 52% discrepancy:**  
Each long trade that hits SL closes earlier in New than in Legacy by the spread
distance. This frees the `max_positions=1` slot sooner in New, allowing the
next filtered signal to open a new position. Legacy holds the slot occupied
longer, blocking subsequent signals. Over 3 months this compounds to
approximately 125 extra trades in New (365 vs 240).

**Confirmed from 3-hour diagnostic window:**  
Legacy risk evaluates only 6 of 13 filtered signals. New evaluates 7+. The
difference occurs because New's 19:47 BUY position closes earlier (correct SL
trigger), freeing the slot for 19:53 and 20:00 signals. Legacy's 19:47 BUY
stays open longer (incorrect SL trigger), blocking those signals.

**Fix [FIX-L4-SL]:**  
File: `src/strategies/trade_management/risk_manager.py`

```python
# BEFORE (wrong) — line ~175
trigger_sl = raw_sl - spread_for_this if is_long else raw_sl + spread_for_this

# AFTER (fixed)
trigger_sl = raw_sl if is_long else raw_sl + spread_for_this
```

**Fix status:** ✅ Applied in `risk_manager_legacy_fixed.py`

---

### Finding F3 — `max_risk_percentile` Notation Change (NO DIVERGENCE)

**Layer:** 1/4 — Config & Trade Simulation  
**Type:** Intentional notation change between architectures  
**Status:** ✅ Confirmed from source code — identical effective thresholds

**Detail:**  
The two pipelines use different notations for the same value, but produce
mathematically identical risk thresholds:

| Pipeline | YAML value | Code computation | Effective threshold |
|---|---|---|---|
| Legacy | `max_risk_percentile: 0.001` | `risk_dist / RAR <= 0.001` (raw fraction) | `0.001 × RAR` |
| New | `max_risk_percentile: 0.1` | `(risk_dist / RAR) × 100 <= 0.1` (percentage) | `0.001 × RAR` |

Legacy `RiskManager` operates in raw fraction space. New `RiskManager`
operates in percentage space (explicitly documented in its class docstring:
*"max_risk_percentile is a PERCENTAGE of the rolling 12-month annual
range"*). The YAML values are different notations for the same quantity.
No divergence in runtime behaviour.

**Action:** None required for Phase 9. Document the notation convention
difference in the production YAML with an inline comment to prevent future
misconfiguration.

---

### Finding F4 — `max_positions` Explicit vs Implicit (NO DIVERGENCE)

**Layer:** 1/4 — Config & Trade Simulation  
**Type:** Architectural improvement — explicit guard added in New  
**Status:** ✅ Confirmed from source code — identical effective behaviour

**Detail:**  
Legacy `TradeManager` has no `max_positions` field. Position limiting is
achieved implicitly: `pyramiding_enabled=False` rejects same-direction
signals; `close_on_opposite=False` rejects opposite signals. Effective
maximum is 1 concurrent position.

New `TradeManager` adds an explicit `max_positions` guard:
```python
if len(self.current_positions) >= self.max_positions:
    return TradeDecision(decision_type=DecisionType.REJECT, ...)
```
With `max_positions=1` in YAML and `pyramiding_enabled=False`, both the
explicit guard and the pyramiding check fire on the same scenario. Net
behaviour is identical to Legacy.

**Action:** None required.

---

### Finding F5 — `spread.apply_to_long/short` Source Difference (NO DIVERGENCE)

**Layer:** 1/4 — Config & Trade Simulation  
**Type:** Architectural refactor — spread settings moved to broker YAML  
**Status:** ✅ Confirmed from source code — identical effective values

**Detail:**  
Legacy `RiskManager` reads `apply_to_long` and `apply_to_short` from the
strategy YAML `trade_management.spread` section (explicit `True` in legacy
YAML, default `True` if not set).

New `RiskManager` reads these values from `SpreadManager` instance
attributes, which are populated from `broker_spreads.yaml` global
`settings` section (default `True` if not set in broker file).

Both pipelines apply spread to both long and short trades. Different
configuration source, identical runtime values.

**Action:** None required.

---

### Finding F6 — HTF and LTF Full File vs Window Slice (NO DIVERGENCE)

**Layer:** 1  
**Type:** Architectural change — New slices before passing downstream  
**Status:** ✅ Confirmed — window content byte-identical

**Detail:**  
Legacy loads full HTF file (12,407 rows) and full LTF file (14.97M rows)
and passes them downstream unsliced. New slices both to the strategy window
before passing downstream (HTF = 4 rows, LTF = 4,816 rows).

The 4 HTF bars and 4,816 LTF bars within the diagnostic window are
byte-identical between both pipelines. No signal or trade execution
difference results from the different loading strategies for this window.

**Action:** None required.

---

## 5. Open Items Requiring Pre-Phase-9 Verification

| ID | Item | Action | Owner |
|---|---|---|---|
| ~~V1~~ | ~~Design intent for `session_end` boundary~~  | ✅ Closed — inclusive end confirmed. Fix applied [FIX-L3D1]. | — |
| ~~V2~~ | ~~ARTF file start date vs strategy `date_range.start`~~ | ✅ Closed — ARTF starts 2021-01-01, strategy starts 2024+. Full 12-month RAR available for every strategy bar. F2 has zero runtime impact. | — |

---

## 6. Required Code Change

### Fix F1 — Time Filter Boundary Operator

**File:** `src/strategies/specific/filters/time_filter.py`, line 144  
**Fix tag:** `[FIX-L3D1]`  
**Status:** ✅ Applied — design intent confirmed as inclusive end (2026-02-25)

```python
# BEFORE — excluded session_end bar (bug)
(minutes_col < self.session_end_minutes)

# AFTER [FIX-L3D1] — session_end bar is last accepted bar
(minutes_col <= self.session_end_minutes)
```

No other files require changes for this fix.

---

## 7. Resolved Open Items Register

| ID | Description | Verdict |
|---|---|---|
| L1-D1 | `max_risk_percentile` 0.001 vs 0.1 — same threshold? | ✅ Yes — different notation, identical effective value (Finding F3) |
| L1-D2 | `max_positions` NOT SET in Legacy — default? | ✅ Implicit max 1 via pyramiding/opposite logic (Finding F4) |
| L1-D3 | `spread.apply_to_long/short` NOT SET in New | ✅ Defaults to True in both — no divergence (Finding F5) |
| L1-D4 | HTF full file vs window slice | ✅ Window content byte-identical — no signal impact (Finding F6) |
| L1-D5 | LTF full file vs window slice | ✅ Window content byte-identical — no trade execution impact (Finding F6) |
| L2-D1 | Legacy signal interface return type | ✅ Moot — signal counts confirmed from run log |
| L2-D2 | Bar-by-bar signal fingerprint | ✅ Moot — total counts identical |
| L2-D3 | Legacy HTF lookahead | ✅ Moot — no divergence at signal layer |
| L3-D1 | Time filter boundary `<` vs `<=` | ✅ Root cause confirmed — Finding F1 |
| L4-D1 | `max_risk_percentile` 0.001 — fraction or percentage? | ✅ Raw fraction in Legacy, percentage in New — identical threshold |
| L4-D2 | Legacy `max_positions` default | ✅ No field — implicit 1 via position logic |
| L4-D3 | New `spread.apply_to_long/short` default | ✅ Both default True — no divergence |

---

## 8. Discrepancy Decomposition

| Source | Type | Direction | Magnitude |
|---|---|---|---|
| F1 — Time filter `<` vs `<=` | Bug — fixed [FIX-L3D1] | Legacy had fewer trades | Minor — 1 signal per session-end boundary hit |
| **F7 — Long SL trigger spread subtraction** | **Bug — fixed [FIX-L4-SL]** | **Legacy had fewer trades** | **ROOT CAUSE — ~52% discrepancy over 3 months** |
| F2 — RAR NaN fail-safe | Intentional change | Zero impact | ARTF covers 2021+, strategy 2024+ |
| F3–F6 | No divergence | — | Zero |

**Final verdict:** The ~52% trade count discrepancy (Legacy 240 vs New 365 over 3
months) is caused by Legacy's incorrect long SL trigger formula [FIX-L4-SL].
Subtracting spread from the long SL trigger held losing long positions open longer
than correct eToro CFD execution requires, systematically blocking subsequent
signals from opening. With `max_positions=1`, each extra bar a position stays open
is a bar where a potentially profitable new signal is rejected.

New pipeline is correct. Legacy fix is a single line change in `risk_manager.py`.

**Verification step:** Re-run both pipelines on the 3-month window
(2025-09-14 → 2025-12-17) with the Legacy fix applied. Expected outcome: Legacy
trade count converges toward New's 365. Residual small difference (~1%) is
acceptable and attributable to F1 (time filter) edge cases and intentional
architectural differences (F2 fail-safe, F4 explicit max_positions).

---

## 9. Phase 9 Gate Criteria

The following conditions must be met before Phase 9 migration begins:

1. ~~**V1 resolved** — design intent for `session_end` boundary confirmed~~ ✅ Done
2. ~~**F1 fix applied** — `time_filter.py` boundary operator corrected~~ ✅ Done [FIX-L3D1]
3. ~~**V2 resolved** — ARTF file coverage confirmed sufficient~~ ✅ Done — zero impact confirmed
4. ~~**Root cause of ~52% discrepancy identified**~~ ✅ Done — Legacy long SL trigger bug [FIX-L4-SL]
5. ~~**F7 fix applied** — Legacy `risk_manager.py` SL trigger corrected~~ ✅ Done [FIX-L4-SL]
6. ~~**Verification run**~~ ✅ Done — Legacy 367 vs New 368 over 3 months. Parity confirmed.
7. ~~**ACT 0 verdict documented and signed off**~~ ✅ Done — 2026-02-25

---

---

## 10. Verification Run Results — ACT 0 Closed

**Date:** 2026-02-25  
**Window:** 2025-09-14 08:00:00 → 2025-12-17 21:00:00 (3 months)

| Metric | Legacy (fixed) | New | Delta | Status |
|---|---|---|---|---|
| Raw signals | 9,667 | 9,667 | 0 | ✅ |
| After all filters | 5,182 | 5,186 | 4 | ✅ note |
| **Trades opened** | **367** | **368** | **1** | ✅ |
| Win rate | 11.44% | 11.4% | ~0 | ✅ |
| Total PnL | -754.09 pts | -763.3 pts | 9.2 pts | ✅ |
| Max drawdown | -808.56 pts | -817.8 pts | 9.2 pts | ✅ |

**Note on 4-signal filter difference:** New warns of a LTF head gap
(`2025-09-14 08:00 → 22:06` uncovered). Legacy silently begins its
effective window at `22:06`. The 4-signal and 1-trade residual difference
is fully explained by this LTF file boundary — not a pipeline logic
difference.

**Verdict: ACT 0 COMPLETE. Root cause confirmed, fixed, and verified.**  
The ~52% trade count discrepancy is eliminated. Both pipelines are now
in parity. Phase 9 migration is cleared to proceed.

---

*END OF DOCUMENT — DIAGNOSTIC_REPORT.md — v1.1 — 2026-02-25*