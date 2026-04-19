# LIVE_VALIDATION.md — Live vs Backtest Correspondence Validation
# CTP Broker Integration — Analytical Project
# Purpose: Validate that live pipeline inputs and signals correspond closely
#          to what backtesting estimated. Time-sensitive — informs paper trading
#          continuation decisions.
# Owner: Claude.ai | Created: 2026-04-19
---

## 1. OBJECTIVE

Determine whether the live paper trading pipeline is operating on data and
producing signals that correspond closely to backtest expectations.
Two independent validation tracks — both executable quickly using existing
infrastructure and data.

If either track reveals material divergence, it informs an immediate decision:
adjust, fix, or halt paper trading of the affected candidate.

---

## 2. TRACK A — Price Series Alignment (Broker vs Historical OHLCV)

### Question
Do broker candle closes match the historical OHLCV data used in backtesting,
or is there a systematic offset indicating a bid/mid/ask mismatch?

### Why it matters
Backtesting used bid-only OHLCV. The broker candle endpoint may return mid or
ask prices. If so, every signal bar close used live is offset by ~0.5–1× spread
from what the strategy was trained on. This would create systematic entry
distortion invisible in backtest — potentially invalidating SL/TP distance
calculations based on those closes.

### Approach
Fetch broker candles for GER40 (instrument_id=32) for the overlap window
(2026-03-17 to 2026-04-01 — within both live run and historical data coverage).
Load matching bars from the historical 10-min OHLCV parquet.
Align on timestamp and compute per-bar deviations on close (primary), and
on high/low (to distinguish bid vs mid/ask).

### What a systematic bid/mid/ask offset looks like
- Bid data: broker close ≈ historical close (near zero mean deviation)
- Mid data: broker close ≈ historical close + 0.5 × spread (positive mean deviation)
- Ask data: broker close ≈ historical close + spread (positive mean deviation)
GER40 typical spread: 1–3 points. Mean deviation > 0.5 pts consistently = flag.

### Deliverable
Script: `scripts/diagnostics/price_alignment_check.py`
Output: `outputs/broker_support/diagnostics/price_alignment_YYYY-MM-DD.txt`

Report sections:
1. Bar count matched / unmatched
2. Close deviation: mean, std, max, P95 (broker minus historical)
3. High deviation and Low deviation (same stats)
4. Bias direction: NEUTRAL / POSITIVE_BIAS / NEGATIVE_BIAS with magnitude
5. Spread context: deviation as fraction of typical spread
6. Conclusion: ALIGNED / INVESTIGATE / MISALIGNED with one-line rationale

### Input files needed
- Historical 10-min OHLCV parquet path (Agent reads from config or hardcode GER40)
- Broker API access (uses existing EToroClient via settings)
- Overlap window: 2026-03-17 to 2026-04-01

**Note on 1-hour data lag:** Local historical parquet and broker candle data both
have ~1 hour availability lag. The comparison window ends 2026-04-01 — well within
both datasets' confirmed coverage. No boundary issue expected for this specific run.
For future Track A re-runs on recent data: end the comparison window at least
1 hour before the data update time to avoid spurious boundary mismatches.

**Note on candle pagination:** 1000 bars per API request maximum. The overlap
window (2026-03-17 to 2026-04-01 = ~15 trading days × ~84 10-min bars/day ≈ 1260 bars)
exceeds a single request. The diagnostic script must issue two requests or fetch
with direction=desc count=1000 and clip to the window — 1000 most recent bars from
2026-04-01 desc covers back to approximately 2026-03-26 (500 bars = ~6 trading days).
To cover the full window from 2026-03-17, a second request anchored earlier is needed.
Agent B must handle this in the script.

### Assigned to
Agent B (Codex) — new script, self-contained, no src/ changes.
Agent C (Qwen) — verifies output and runs it.

---

## 3. TRACK B — Signal Correspondence (Live Pipeline vs Backtest Expectation)

### Question
When the live pipeline evaluates a bar, do the signal decisions (fire / no-fire)
and trade parameters (SL distance, ATR, R:R) correspond to what the backtest
predicted for those same market conditions?

### Why it matters
If the live pipeline fires signals at different rates, or with materially different
SL/TP distances than backtest, the paper trading results are testing a different
strategy than the one validated. Root causes could include: data misalignment
(Track A), filter behaviour on live data edge cases, or RiskManager ATR
computation differences on a rolling live window vs full historical series.

### Key insight from log analysis (2026-04-19)
The existing logs already contain all necessary data:
- Per-poll: last bar timestamp, result (NO_SIGNAL / RISK_REJECTED / OrderSignal)
- On signal: bid_price, entry_price_mid, SL distance, TP distance, ATR, R:R
- On rejection: risk_manager.get_risk_summary() output
No new logging code is required for this track.

### What to extract from logs
Per poll where a signal was generated (result=OrderSignal):
  timestamp, direction, bid_price, entry_price_mid, sl_distance, tp_distance,
  atr_value, risk_reward_ratio, spread_points, candidate_id

Per poll where RiskManager rejected (result=RISK_REJECTED):
  timestamp, risk_summary content

### Backtest comparison baseline
Run the backtesting pipeline on the same candidate (c424a0e04327 / 822f1889)
over the overlap period (2026-03-17 to 2026-04-01) in offline mode.
Extract: signal timestamps, SL distances, ATR values, R:R ratios.
Compare distributions — not bar-by-bar identity (live data arrives incrementally,
backtest sees the full series) but statistical correspondence:
  - Signal frequency: live signals/day vs backtest signals/day over same period
  - ATR distribution: mean, std of ATR at signal bars — should be close
  - SL distance distribution: same
  - R:R distribution: same

### Deliverable
Step 1: Script `scripts/diagnostics/signal_log_extractor.py`
  — parses existing demo_trading logs, outputs structured CSV:
    `outputs/broker_support/diagnostics/live_signals_extracted.csv`
  Fields: date, instance, poll_timestamp, bar_timestamp, result,
          direction, bid_price, sl_distance, tp_distance, atr_value,
          risk_reward_ratio, rejection_reason

Step 2: Claude.ai analysis
  — compare extracted CSV against backtest expectations
  — produce correspondence report

### Assigned to
Step 1: Agent C (Qwen) — log parsing, read-only, no src/ changes.
Step 2: Claude.ai — analysis after CSV is relayed.

---

## 4. EXECUTION ORDER

```
Week of 2026-04-19:
  [DONE] Author instruction for Agent C → signal_log_extractor.py (log parse)
  [DONE] Run Track B extractor → live_signals_extracted.csv (12,900 rows)
  [DONE] Claude.ai Track B analysis → docs/broker/TRACK_B_ANALYSIS.md
  [DONE] Fix extractor bugs (alphanumeric instance IDs + UTF-8 encoding)
  [TODO] Re-run extractor to capture c424 and 7ffbc5 data
  [TODO] Author instruction for Agent B → price_alignment_check.py
  [TODO] Run Track A script, relay output to Claude.ai
  [TODO] Claude.ai Track A findings
  [TODO] Re-run Track B extractor after 2+ more weeks for larger signal sample
```

---

## 5. DECISION FRAMEWORK

### Track A outcomes
| Result | Action |
|--------|--------|
| ALIGNED (mean deviation < 0.5 pts) | No action — bid data confirmed |
| INVESTIGATE (0.5–2.0 pts) | Characterise offset — check spread implementation in SignalBridge |
| MISALIGNED (> 2.0 pts or negative bias) | Immediate review — potential systematic entry error |

### Track B outcomes
| Result | Action |
|--------|--------|
| Signal freq within 20% of backtest, ATR/SL within 15% | Correspondence confirmed |
| Signal freq diverges >20% | Investigate filter pipeline behaviour on live data edge |
| ATR/SL diverges >15% | Investigate RiskManager ATR window effect on rolling live data |
| Zero signals over >5 trading days | Immediate flag — strategy may not fire on live data |

---

## 6. OPEN QUESTIONS (to resolve during analysis)

1. What is the exact field name for close price in the historical 10-min parquet?
   (likely `close` — Agent C to confirm before Track A script is written)
2. What timeframe parquet exists for 10-min GER40 historical data?
   (Track A needs the matching TF — confirm path before authoring instruction)
3. How many actual signal events (result=OrderSignal) exist in the logs to date?
   (Track B extractor will answer this — if very few, comparison is limited)
