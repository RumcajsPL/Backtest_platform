# CONTEXT.md — CTP Session State
# Updated: 2026-03-20 (logging clarity fix — result=NO_SIGNAL / result=RISK_REJECTED)
---
## Where We Are
```
BROKER INTEGRATION:    Phase 2 COMPLETE as of 2026-03-18.
CTP ISOLATION FIX:     2026-03-19 — loop halted due to external account activity
                       interfering with drawdown guard. Fix implemented and delivered.
LOGGING FIX:           2026-03-20 — misleading "No signal on last bar" message
                       replaced. See detail below.
First live trade:      2026-03-17 13:06 UTC — BUY GER40 @ 23705.89
                       positionID=3466009287, orderID=336588020
                       SL=23676.47, TP=23891.07, R:R=8.8x
                       Status: successful (very profitable per operator note)
Loop halt (incident):  2026-03-19 10:02 UTC — Poll #4
                       session_open=4031.20, current=3056.43, drawdown=24.18%
                       Root cause: manual trades on same demo account depleted
                       credit before CTP placed any orders. Not a CTP bug.
                       Fix: drawdown now CTP journal-scoped only.
Next action:           Loop running. Next week: review first full week of data.
                       Command: python scripts/broker_support/run_signal_loop.py
```
---
## 2026-03-20 Session — First Full Day Analysis
```
Loop ran cleanly all day. CTP isolation confirmed working — no external
interference observed (contrast: 2026-03-19 incident).
Signals detected:
  Poll #133  11:12 UTC — SELL @ 22861.62 — RiskManager REJECTED
  Poll #253  13:12 UTC — BUY  @ 22827.07 — RiskManager REJECTED
  Poll #417  16:14 UTC — SELL            — RiskManager REJECTED
  (3 signals total, all rejected — consistent with 0.45% threshold calibration)
Apparent "silent fail" at Poll #417 investigated and ruled out:
  - 16:14 SELL appeared in Poll #418 "latest signals" list but not in Poll #417
    rejection log. Initially flagged as possible silent swallow.
  - Root cause: both "no signal on last bar" (Stage 5) and "RiskManager
    rejected" (Stage 6) returned None and hit the same log line in the loop —
    "No signal on last bar" — making a rejection look identical to a flat bar.
  - Poll #417 DID process the signal correctly. No signals were lost.
  - The "latest signals" appearance at Poll #418 is expected behaviour:
    _describe_recent_signals() scans the full 500-bar filter result, so
    a bar from the prior poll remains in the window.
Logging fix delivered (2 files, 3 lines):
  src/broker_support/live/signal_bridge.py
    - result=NO_SIGNAL logged before Stage 5 return None
    - result=RISK_REJECTED logged before Stage 6 return None
  scripts/broker_support/run_signal_loop.py
    - "No signal on last bar" → "No actionable signal this poll"
      (accurate for both Stage 5 and Stage 6 outcomes)
Post-fix log sequence for a rejection will read:
  SignalBridge: signal found at last bar — SELL @ …
  SignalBridge: RiskManager rejected trade at … Risk summary: …
  SignalBridge: result=RISK_REJECTED
  No actionable signal this poll. Next poll in 60s …
```
---
## CTP Isolation Design (implemented 2026-03-19)
```
Problem:  Demo account is shared between CTP and manual/other strategies.
          Account-wide credit movements triggered CTP's circuit breakers
          even when CTP had placed no trades.
Solution: Both safeguards now scoped exclusively to CTP-placed activity.
1. Drawdown guard — journal-scoped:
   - check_daily_drawdown(ctp_realised_pnl_today: float)
   - ctp_realised_pnl_today = sum(profit_loss) from trades.csv for today (UTC)
   - drawdown_pct = max(0, -ctp_realised_pnl_today / session_open_credit * 100)
   - Account-wide credit comparison REMOVED entirely
   - External losses on same account are invisible to this guard by design
2. Pyramiding guard — CTP position set:
   - _check_pyramiding() now accepts ctp_open_position_ids: set[int]
   - Only positions whose positionID is in that set are counted
   - External positions on same instrument are logged but ignored
   - ctp_open_position_ids maintained in-memory, persisted to
     outputs/broker_support/journal/open_positions.json
   - Written on every order placement, seeded from file on restart
3. open_positions.json schema:
   {"position_ids": [3466009287, ...]}
   - Created/updated by run_signal_loop.py on order placement
   - Stale entries (closed externally) are harmless — they won't appear
     in live portfolio so ctp_open_count will be 0 regardless
   - Tracker loop integration (removing entries on close) remains V2 backlog
```
---
## Phase 2 Deliverables (complete 2026-03-18)
```
configs/broker_support/broker_support_config.yaml
src/broker_support/config/broker_support_config.py
src/broker_support/utils/time_utils.py
src/broker_support/live/__init__.py
src/broker_support/live/live_data_fetcher.py
src/broker_support/live/live_config_patcher.py
src/broker_support/live/live_data_bundle.py
src/broker_support/live/order_signal.py
src/broker_support/live/signal_bridge.py
src/broker_support/execution/order_router.py   ← fast-fill fix
src/broker_support/safeguards/__init__.py
src/broker_support/safeguards/paper_trading_guard.py
scripts/broker_support/run_signal.py
scripts/broker_support/run_signal_loop.py
tests/broker_support/test_time_utils.py
tests/broker_support/test_signal_pipeline_integration.py
```
## 2026-03-19 Deliverables
```
src/broker_support/safeguards/paper_trading_guard.py  ← check_daily_drawdown redesigned
scripts/broker_support/run_signal_loop.py             ← CTP isolation, open_positions.json
```
## 2026-03-20 Deliverables
```
src/broker_support/live/signal_bridge.py    ← result=NO_SIGNAL / result=RISK_REJECTED
scripts/broker_support/run_signal_loop.py   ← "No actionable signal this poll"
```
---
## Test Status
```
90/90  unit tests passing
63/63  integration tests passing
Note:  PaperTradingGuard, order_router fast-fill path, CTP isolation not yet
       covered by tests — all in test backlog for V2.
       Logging fix (result= sentinel lines) not tested — trivial log-only change.
```
---
## Trade Constraint Status
| Constraint | Value | Enforced where |
|---|---|---|
| max_risk_percentile | 0.45 | RiskManager — uses full ARTF parquet |
| pyramiding_enabled | false | _check_pyramiding() — CTP positionIDs only |
| max_positions | 1 | Same guard — source: strategy YAML |
| close_on_opposite | false | Emergent from pyramiding guard |
| max_consecutive_losses | 3 | PaperTradingGuard — hard_stop (first week) |
| max_daily_drawdown_pct | 5.0% | PaperTradingGuard — CTP journal-scoped |
| min_available_cash_usd | 200.0 | PaperTradingGuard — account-wide (capital floor) |
| max_pipeline_errors | 5 | PaperTradingGuard — hard_stop |
| kill_switch_file | STOP | PaperTradingGuard — checked every poll |
| off-hours gate | allowed_hours_utc | Loop — sleeps until next session open |
---
## Paper Trade Candidates (run b651ec5c — production reference)
| Priority | Candidate | WFO | Ruin | Status |
|----------|-----------|-----|------|--------|
| 1st | c424a0e04327 | 0.8108 | 0.000 | PRIMARY — active |
| 2nd | 20745ca991be | 0.7201 | 0.054 | SECONDARY — after PRIMARY stable |
| Watch | c42f8b009283 | 0.6473 | 0.000 | MONITOR |
| Watch | c209820886c8 | 0.5699 | 0.000 | SECONDARY MONITOR — do NOT promote |
---
## Live Signal Loop — Status
```
2026-03-16: Poll #324, 14:35 UTC — BUY @ 23605.05
            REJECTED by RiskManager (threshold_pct=0.45). Expected.
2026-03-17: Poll #235, 13:06 UTC — BUY @ 23694.81
            PLACED. orderID=336588020 → positionID=3466009287
            openRate=23705.89, SL=23676.47, TP=23891.07, R:R=8.8x
            Trade was profitable (per operator).
2026-03-19: Loop halted Poll #4 — external account drawdown 24.18%.
            Fixed. Restarted.
2026-03-20: 3 signals detected (11:12 SELL, 13:12 BUY, 16:14 SELL).
            All rejected by RiskManager. Loop healthy.
RiskManager calibration — pending:
  Only ~5 data points across 3 days. Run full week before reviewing 0.45%
  threshold. All rejections consistent with elevated DAX volatility / ATR.
```
---
## Useful Commands
```powershell
# Live trading — persistent loop (recommended)
python scripts/broker_support/run_signal_loop.py --config configs/broker_support/broker_support_config.yaml
python scripts/broker_support/run_signal_loop.py --quiet
# Kill switch — halt loop at next poll
echo "" > STOP          # create file
del STOP                # resume (restart loop manually)
# Supervised single trade
python scripts/broker_support/run_signal.py --verbose
python scripts/broker_support/run_signal.py --place-order --verbose
# Diagnostics
python scripts/broker_support/inspect_portfolio.py
python scripts/broker_support/run_tracker_loop.py --once --no-hours-guard
# Tests
pytest tests/broker_support/ -v
```
---
## Key Paths
```
Strategy YAML:       outputs/backtesting/trading_yamls/b651ec5c_c424a0e04327_strategy.yaml
ARTF parquet:        data/processed/ohlcv/DEUIDXEUR_1ME_20210101_20260301.parquet
BS config:           configs/broker_support/broker_support_config.yaml
Instrument map:      configs/broker_support/instrument_map.yaml  (symbol key: GER40)
Credentials:         configs/broker_support/broker_settings.env
Journal (closed):    outputs/broker_support/journal/trades.csv
Open positions:      outputs/broker_support/journal/open_positions.json
Logs:                outputs/broker_support/logs/run_signal_loop_YYYY-MM-DD.log
```
---
## Next Session Start
1. python scripts/broker_support/inspect_portfolio.py — check open positions
2. Review logs: outputs/broker_support/logs/run_signal_loop_YYYY-MM-DD.log
3. Count: signals found, RiskManager rejections vs approvals, orders placed
4. After 1 full week (target: end of w/c 2026-03-23):
   - Review RiskManager 0.45% threshold vs DAX volatility profile
   - Assess whether rejection rate is consistent with backtest (~39% approval)
   - If consistent: threshold is correctly calibrated, no change
   - If approval rate significantly lower: consider threshold review
5. If circuit breaker fired: diagnose before restarting loop
6. Secondary candidate 20745ca991be: do NOT promote until PRIMARY stable 1 week
---
## V2 Backlog (do not act until Phase 2 week complete)
```
- Tests for PaperTradingGuard (new drawdown logic + CTP isolation)
- Tests for order_router fast-fill path
- run_tracker_loop.py → remove closed positionID from open_positions.json
  on close detection (currently stale entries are harmless but cleanup is clean)
- run_tracker_loop.py → call guard.record_trade_result() on close detection
  (requires shared guard state or inter-process journal polling)
- daily_order_cap safeguard (deferred)
- SafetyConfig evolution — V2 discussion
- Promote secondary candidate 20745ca991be after PRIMARY stable 1 week
```