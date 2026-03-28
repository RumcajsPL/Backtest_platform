# CONTEXT.md — CTP Session State
# Updated: 2026-03-28 (pending-order reconciliation + pipeline error scope fix)
---
## Where We Are
```
BROKER INTEGRATION:    Phase 2 COMPLETE as of 2026-03-18.
CTP ISOLATION FIX:     2026-03-19 — loop halted due to external account activity.
                       Fix implemented and delivered.
MULTI-INSTANCE WEEK:   2026-03-24 to 2026-03-28 — 4 candidates running in parallel.
                       Good data collected. Two bugs identified and fixed this session.
First live trade:      2026-03-17 13:06 UTC — BUY GER40 @ 23705.89
                       positionID=3466009287, orderID=336588020
                       SL=23676.47, TP=23891.07, R:R=8.8x
                       Status: successful (very profitable per operator note)
Loop halt (incident):  2026-03-19 10:02 UTC — Poll #4
                       Root cause: manual trades depleted demo credit before CTP.
                       Fix: drawdown now CTP journal-scoped only.
Bug-fix session:       2026-03-28 — two bugs fixed in run_signal_loop.py (see below).
Next action:           Deploy fixed files. Restart all 4 loops next trading week.
```
---
## 2026-03-28 Bug Fixes
### Bug 1 — Multiple orders placed from one signal (240166 instance, 2026-03-26)
```
Root cause (two compounding problems):
  1a. Portfolio scan in _find_position_in_portfolio timed out 3 consecutive times
      (20s / 10 attempts each). Orders were accepted by broker (statusID=1 in POST)
      but positionIDs never appeared in the portfolio within the timeout window.
      This appears to be a new eToro behaviour: with several external positions
      already open on the same account, the new CTP positions' orderID field
      was not visible in the portfolio within 20s.
  1b. open_position() raised OrderExecutionError (ORDER FAILED log). The loop
      caught this as a generic Exception, slept 60s, then continued to the next
      poll. ctp_open_position_ids was never updated, so _check_pyramiding found
      0 CTP positions and safe_to_trade=True. The loop re-signalled and placed
      another order. This repeated 3 times before the 4th attempt succeeded.
      The 4th positionID (3475134299) was eventually registered and pyramiding
      correctly blocked all subsequent polls.
Fix (run_signal_loop.py):
  Pending-order reconciliation. When open_position() raises, the orderID is
  extracted from the exception message (regex on "orderID=<N>") and added to
  pending_order_ids dict with a countdown of _PENDING_ORDER_MAX_POLLS=5.
  At the start of every subsequent poll (before pyramiding check), the loop
  calls _reconcile_pending_orders() which scans the live portfolio for those
  orderIDs. If found, the positionID is registered in ctp_open_position_ids
  and persisted. This blocks new orders correctly even if positionID resolution
  originally timed out.
  If still not found after 5 polls (5 minutes), the entry is retired with a
  warning instructing the operator to verify manually.
New empirical API fact logged:
  With 3+ external positions open on the same demo account, newly placed
  positions' orderID field may not appear in GET /demo/portfolio for >20s
  after a confirmed fast-fill (statusID=1 in POST). This is beyond the current
  portfolio scan timeout and may require operator awareness.
```
### Bug 2 — c424 loop halted by broker API outage (2026-03-27 15:01–15:06 UTC)
```
Root cause:
  eToro API experienced a ~5 minute outage (503, 409, 502, ConnectionReset,
  timeout in rapid succession). The portfolio fetch errors in _check_pyramiding
  were each calling guard.record_pipeline_error(), which counts against the
  max_pipeline_errors=5 budget. After 5 consecutive errors (all portfolio fetch
  failures, zero signal pipeline failures), the loop halted permanently.
  The pipeline error counter was designed for SignalBridge failures (broken
  infrastructure), not transient broker connectivity issues.
Fix (run_signal_loop.py):
  Portfolio fetch errors in _check_pyramiding now skip the poll cleanly
  (log + sleep + continue) without calling guard.record_pipeline_error().
  guard.record_pipeline_error() is called ONLY from the SignalBridge except
  block — its original design intent. Portfolio outages are logged at ERROR
  level but do not count against the pipeline error budget.
```
### Fix 3 — pandas UserWarning on timestamp parsing (live_data_fetcher.py)
```
Root cause:
  pd.to_datetime(df["timestamp"], utc=True) emits UserWarning when pandas
  cannot infer the format from the first element due to mixed-precision
  fractional seconds in ISO 8601 strings (e.g. "2026-03-26T17:34:55.042Z").
Fix:
  pd.to_datetime(df["timestamp"], format="ISO8601", utc=True)
  Explicit format eliminates the warning and guarantees consistent parsing.
```
### Fix 4 — inspect_portfolio.py updated
```
Added:
  - --instance flag: loads matching open_positions.json, annotates CTP vs external
  - --all-positions flag: prints all positions (default: first only)
  - Prominent orderID display on every position (critical for pending-order diagnosis)
  - Key fields shown first before full raw dump
```
---
## 2026-03-28 Deliverables
```
scripts/broker_support/run_signal_loop.py     ← Bug 1 + Bug 2 fixes
src/broker_support/live/live_data_fetcher.py  ← pandas ISO8601 fix
scripts/broker_support/inspect_portfolio.py   ← updated diagnostic
docs/ctp/CONTEXT.md                           ← this file
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
     outputs/broker_support/journal/<instance>/open_positions.json
   - Written on every order placement, seeded from file on restart
3. open_positions.json schema:
   {"position_ids": [3466009287, ...]}
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
---
## Test Status
```
90/90  unit tests passing
63/63  integration tests passing
Note:  PaperTradingGuard, order_router fast-fill path, CTP isolation not yet
       covered by tests — all in test backlog for V2.
       pending_order reconciliation also in test backlog.
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
| max_pipeline_errors | 5 | PaperTradingGuard — SignalBridge failures ONLY |
| kill_switch_file | STOP | PaperTradingGuard — checked every poll |
| off-hours gate | allowed_hours_utc | Loop — sleeps until next session open |
---
## Multi-Instance Week Observations (2026-03-24 to 2026-03-28)
```
Active candidates:
  240166  (822f1889_240166da287e, 10-min)  — most signals, all confirmed trades
  c424    (b651ec5c_c424a0e04327, 1-min)   — loop halted 2026-03-27 (broker outage)
  7ffbc5  (7ffbc5, 1-min)                  — several candle timeout errors, loop stable
  61875   — pandas UserWarning (fixed), loop stable
Key observations:
  - 240166 generated majority of signals. Confirmed trades (exact count TBD from logs).
  - 4 consecutive order attempts on 240166 (2026-03-26 18:34–18:38 UTC) due to
    portfolio scan timeout on first 3 orders. Only 4th confirmed (positionID=3475134299).
    Whether orders 1–3 actually opened requires manual check of trade history.
  - c424 halted permanently due to broker API outage hitting pipeline error budget.
    Fixed: portfolio fetch errors no longer count against pipeline errors.
  - All candidates generated signals during the week — good validation.
  - RiskManager calibration: insufficient data for 0.45% threshold review yet.
    Run another full week before reviewing.
  - WBWS+ window: 240166 config includes hours 17, 19, 20 UTC — confirmed active.
```
---
## Paper Trade Candidates (run b651ec5c — production reference)
| Priority | Candidate | WFO | Ruin | Status |
|----------|-----------|-----|------|--------|
| 1st | c424a0e04327 | 0.8108 | 0.000 | PRIMARY — active |
| 2nd | 20745ca991be | 0.7201 | 0.054 | SECONDARY — after PRIMARY stable |
| Watch | c42f8b009283 | 0.6473 | 0.000 | MONITOR |
| Watch | c209820886c8 | 0.5699 | 0.000 | SECONDARY MONITOR — do NOT promote |

240166 candidate (run 822f1889):
  WFO=0.8886 | Windows=4 | Est filtered P&L=2708 pts | Ruin=0.000
  Active as parallel instance. Promote decision deferred.
---
## Useful Commands
```powershell
# Live trading — persistent loop (recommended)
python scripts/broker_support/run_signal_loop.py --instance c424
python scripts/broker_support/run_signal_loop.py --instance 240166 --quiet
python scripts/broker_support/run_signal_loop.py --instance 7ffbc5 --quiet
python scripts/broker_support/run_signal_loop.py --instance 61875 --quiet
# Kill switch — halt loop at next poll
echo "" > STOP_240166   # halt 240166 only
echo "" > STOP          # halt ALL loops
del STOP_240166         # resume (restart loop manually)
# Diagnostics
python scripts/broker_support/inspect_portfolio.py --instance 240166 --all-positions
python scripts/broker_support/inspect_portfolio.py --instance c424
python scripts/broker_support/run_tracker_loop.py --once --no-hours-guard
# Tests
pytest tests/broker_support/ -v
```
---
## Key Paths
```
Strategy YAML:       outputs/backtesting/trading_yamls/b651ec5c_c424a0e04327_strategy.yaml
ARTF parquet:        data/processed/ohlcv/DEUIDXEUR_1ME_20210101_20260301.parquet
BS config (c424):    configs/broker_support/broker_support_config.yaml
BS config (240166):  configs/broker_support/broker_support_config_240166.yaml
Instrument map:      configs/broker_support/instrument_map.yaml  (symbol key: GER40)
Credentials:         configs/broker_support/broker_settings.env
Journal (closed):    outputs/broker_support/journal/<instance>/trades.csv
Open positions:      outputs/broker_support/journal/<instance>/open_positions.json
Logs:                outputs/broker_support/logs/run_signal_loop_<instance>_YYYY-MM-DD.log
```
---
## V2 Backlog (do not act until next phase review)
```
- Tests for PaperTradingGuard (drawdown logic + CTP isolation)
- Tests for order_router fast-fill path
- Tests for pending_order reconciliation (_reconcile_pending_orders)
- run_tracker_loop.py → remove closed positionID from open_positions.json
  on close detection (stale entries are harmless but cleanup is clean)
- run_tracker_loop.py → call guard.record_trade_result() on close detection
- daily_order_cap safeguard (deferred)
- SafetyConfig evolution — V2 discussion
- Promote secondary candidate 20745ca991be after PRIMARY stable 1 week
- Investigate eToro portfolio scan timeout with multiple external positions:
  consider increasing _PORTFOLIO_POLL_MAX_ATTEMPTS from 10 to 20 (40s window)
  if the issue recurs next week.
```
---
## Next Session Start
1. python scripts/broker_support/inspect_portfolio.py --instance 240166 --all-positions
   — check whether the 3 "failed" orders from 2026-03-26 18:34–18:37 actually opened
   — look for positionIDs corresponding to orderIDs 338749124, 338770199, 338747252
2. Review all 4 instance logs for the week — count signals, trades, circuit breakers
3. Deploy fixed files from this session before restarting any loop
4. After 2 full weeks: review RiskManager 0.45% threshold vs DAX volatility
5. If pending-order reconciliation fires next week: investigate eToro portfolio
   visibility lag with multiple concurrent accounts sharing the demo API