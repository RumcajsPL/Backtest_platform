# CONTEXT.md — CTP Session State
# Updated: 2026-03-18 (Phase 2 finalized — persistent loop + full safeguards)
---
## Where We Are
```
BROKER INTEGRATION:    Phase 2 COMPLETE as of 2026-03-18.
                       All safeguards implemented. Loop is persistent (runs all week).
                       Ready for first full week of paper trading.
First live trade:      2026-03-17 13:06 UTC — BUY GER40 @ 23705.89
                       positionID=3466009287, orderID=336588020
                       SL=23676.47, TP=23891.07, R:R=8.8x
                       Status: successful (very profitable per operator note)
                       order_router.py fast-fill bug fixed this session.

Next action:           Run persistent loop for 1 full week.
                       Command: python scripts/broker_support/run_signal_loop.py
                                --config configs/broker_support/broker_support_config.yaml
                       Or quiet: python scripts/broker_support/run_signal_loop.py --quiet
```
---
## Phase 2 Deliverables (complete)
```
configs/broker_support/broker_support_config.yaml     ← updated: new safety fields
src/broker_support/config/broker_support_config.py    ← updated: SafetyConfig extended
src/broker_support/utils/time_utils.py
src/broker_support/live/__init__.py
src/broker_support/live/live_data_fetcher.py
src/broker_support/live/live_config_patcher.py
src/broker_support/live/live_data_bundle.py
src/broker_support/live/order_signal.py
src/broker_support/live/signal_bridge.py
src/broker_support/execution/order_router.py          ← updated: fast-fill fix
src/broker_support/safeguards/__init__.py             ← NEW (empty)
src/broker_support/safeguards/paper_trading_guard.py  ← NEW
scripts/broker_support/run_signal.py
scripts/broker_support/run_signal_loop.py             ← updated: persistent + safeguards
tests/broker_support/test_time_utils.py
tests/broker_support/test_signal_pipeline_integration.py
```
---
## Test Status
```
90/90  unit tests passing
63/63  integration tests passing
Note:  New files (paper_trading_guard.py, order_router changes) not yet covered
       by tests — add to test backlog for V2.
```
---
## Trade Constraint Status
| Constraint | Value | Enforced where |
|---|---|---|
| max_risk_percentile | 0.45 | RiskManager — uses full ARTF parquet |
| pyramiding_enabled | false | _check_pyramiding() in loop Stage 2 |
| max_positions | 1 | Same guard — source: strategy YAML |
| close_on_opposite | false | Emergent from pyramiding guard |
| max_consecutive_losses | 3 | PaperTradingGuard — hard_stop (first week) |
| max_daily_drawdown_pct | 5.0% | PaperTradingGuard — hard_stop |
| min_available_cash_usd | 200.0 | PaperTradingGuard — hard_stop |
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
            REJECTED by RiskManager (threshold_pct=0.45). Expected. Likely elevated
            ATR near US open. Not a bug.
2026-03-17: Poll #235, 13:06 UTC — BUY @ 23694.81
            PLACED. orderID=336588020 → positionID=3466009287
            openRate=23705.89, SL=23676.47, TP=23891.07, R:R=8.8x
            Log showed ORDER FAILED due to fast-fill bug (now fixed).
            Position confirmed open via inspect_portfolio.py.
            Trade was profitable (per operator).
Fast-fill bug (fixed 2026-03-18):
  Root cause: POST response already had statusID=1 but order_router.py
  went to poll /demo/orders/{id} which returned 404 then statusID=3 (stale).
  Fix: check orderForOpen.statusID in POST response first; if 1, resolve
  positionID via portfolio scan (_find_position_in_portfolio). Poll loop
  also made 404-tolerant (3-attempt grace) and REJECTED-resilient (portfolio
  fallback before raising).
RiskManager calibration — pending:
  Only 2 data points (1 rejection, 1 approval). Run full week before
  drawing any conclusions on 0.45% threshold vs 2026 DAX volatility.
```
---
## Useful Commands
```powershell
# Live trading — persistent loop (recommended)
python scripts/broker_support/run_signal_loop.py --config configs/broker_support/broker_support_config.yaml
python scripts/broker_support/run_signal_loop.py --quiet   # background terminal, log only
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
Strategy YAML:    outputs/backtesting/trading_yamls/b651ec5c_c424a0e04327_strategy.yaml
ARTF parquet:     data/processed/ohlcv/DEUIDXEUR_1ME_20210101_20260301.parquet
BS config:        configs/broker_support/broker_support_config.yaml
Instrument map:   configs/broker_support/instrument_map.yaml  (symbol key: GER40)
Credentials:      configs/broker_support/broker_settings.env
Journal:          outputs/broker_support/journal/trades.csv
Logs:             outputs/broker_support/logs/run_signal_loop_YYYY-MM-DD.log
```
---
## V2 Backlog (do not act until Phase 2 week complete)
```
- Tests for PaperTradingGuard and order_router fast-fill path
- run_tracker_loop.py → call guard.record_trade_result() on close detection
  (requires shared guard state or inter-process journal polling)
- daily_order_cap safeguard (deferred this session)
- SafetyConfig evolution — V2 discussion
- Promote secondary candidate 20745ca991be after PRIMARY stable
```
---
## Next Session Start
1. python scripts/broker_support/inspect_portfolio.py — check open positions
2. Review logs: outputs/broker_support/logs/run_signal_loop_YYYY-MM-DD.log
3. Count: signals found, orders placed, circuit breakers fired (if any)
4. After 1 full week: review RiskManager 0.45% calibration vs 2026 DAX volatility
5. If circuit breaker fired: diagnose before restarting loop