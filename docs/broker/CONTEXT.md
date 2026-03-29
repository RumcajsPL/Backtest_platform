# CONTEXT.md — CTP Session State
# Claude session-to-session continuity. Facts live in ARCHITECTURE.md.
# Completed changes goes to SESSION_LOG appendix
# Updated: 2026-03-29
---
## Current State
```
Phase 2 (live pipeline):    COMPLETE 2026-03-18
Multi-instance week 1:      COMPLETE 2026-03-24 to 2026-03-28
Loop consolidation:         COMPLETE 2026-03-29 (8 terminals → 4)
  run_demo_trading.py:      Signal + tracker unified — DEPLOYED, RUNNING
  TradeEnricher fix:        Applied — 29-day lookback (30-day boundary is exclusive)
  Tracker isolation:        Full CTP scope — external positions never enter trades.csv
  Stale snapshot guard:     Active — auto-invalidates pre-isolation snapshots on first run
  week_one_health_check:    Updated — new log filename + trades.csv P&L section 7
Week 2 loops:               RUNNING from 2026-03-29 (~21:14 UTC)
First live trade: 2026-03-17 13:06 UTC
  positionID=3466009287, orderID=336588020
  BUY GER40 @ 23705.89, SL=23676.47, TP=23891.07, R:R=8.8x — profitable
```
---
## Open Issues
None. All issues resolved.
---
## Watch Items — Week 2
```
1. open_positions.json: deleted manually for 240166 (position 3475134299 was closed).
   File will be auto-created on first new position placement by run_demo_trading.py.
   Confirm this happens correctly on first signal.
2. trades.csv: not yet created for any instance (correct — no closed CTP trades yet
   under new loop). Will be created by tracker cycle on first detected close.
   Confirm correct exit_price and profit_loss (TradeEnricher fix now active).
3. Stale snapshot guard: fired once on 240166 restart (expected — old snapshot from
   run_tracker_loop.py contained external positions). Will not fire again once
   last_positions.csv is rebuilt under CTP isolation. Monitor other 3 instances
   for same one-time warning on first start.
4. 240166 unconfirmed orders from week 1 (all pre-dates new loop):
   orderID=338749124 / 338770199 / 338747252 / 339031085
   positionID=3475134299 confirmed for 4th attempt — all now closed.
   No action required. Logged for reference only.
```
---
## Active Instances
```
c424    → broker_support_config.yaml           (1-min)
240166  → broker_support_config_240166.yaml    (10-min, most signals)
7ffbc5  → broker_support_config_7ffbc5.yaml    (1-min)
61875   → broker_support_config_61875.yaml     (1-min)
```
RiskManager calibration: 1 week of data — insufficient. Review 0.45% threshold
after week 2 completes.
---
## Paper Trade Candidates
| Priority | Candidate | WFO | Ruin | Notes |
|----------|-----------|-----|------|-------|
| 1st | c424a0e04327 | 0.8108 | 0.000 | PRIMARY |
| 2nd | 20745ca991be | 0.7201 | 0.054 | SECONDARY — promote after PRIMARY stable 1 week |
| Watch | c42f8b009283 | 0.6473 | 0.000 | MONITOR |
| Watch | c209820886c8 | 0.5699 | 0.000 | Do NOT promote |
240166 candidate (run 822f1889): WFO=0.8886, Ruin=0.000. Promote decision deferred.
---
## Next Session Actions
```
1. Review week 2 results — run health_check after week ends
2. Review RiskManager 0.45% threshold with 2 weeks of live data
3. Promote 20745ca991be after PRIMARY (c424) stable 1 full week
4. V2 backlog items (see below)
```
---
## Test Status
```
90/90  unit tests passing
63/63  integration tests passing
Not yet covered (V2 backlog):
  PaperTradingGuard drawdown + CTP isolation
  order_router fast-fill path
  pending_order reconciliation
  _run_tracker_cycle (new — integrated tracker)
```
---
## V2 Backlog
```
- Tests: PaperTradingGuard, order_router fast-fill, pending_order reconciliation,
         _run_tracker_cycle integration
- Increase _PORTFOLIO_POLL_MAX_ATTEMPTS from 10 to 20 if scan timeout recurs
- daily_order_cap safeguard
- Scripts and tests documentation session (separate session)
```
---
## Useful Commands
```powershell
# Loops (unified — 4 terminals)
python scripts/broker_support/run_demo_trading.py --instance c424
python scripts/broker_support/run_demo_trading.py --instance 240166 --quiet
python scripts/broker_support/run_demo_trading.py --instance 7ffbc5 --quiet
python scripts/broker_support/run_demo_trading.py --instance 61875 --quiet
# Kill switches
echo "" > STOP             # halt all
echo "" > STOP_240166      # halt one
del STOP_240166
# Diagnostics
python scripts/broker_support/inspect_portfolio.py --instance 240166 --all-positions
python scripts/diagnostics/week_one_health_check.py
# Tests
pytest tests/broker_support/ -v
```
---
## Key Paths
```
Strategy YAML:    outputs/backtesting/trading_yamls/b651ec5c_c424a0e04327_strategy.yaml
ARTF parquet:     data/processed/ohlcv/DEUIDXEUR_1ME_20210101_20260301.parquet
Instrument map:   configs/broker_support/instrument_map.yaml
Credentials:      configs/broker_support/broker_settings.env
Journal:          outputs/broker_support/journal/<instance>/trades.csv
Open positions:   outputs/broker_support/journal/<instance>/open_positions.json
Snapshots:        outputs/broker_support/snapshots/<instance>/last_positions.csv
Signal logs:      outputs/broker_support/logs/demo_trading_<instance>_YYYY-MM-DD.log
Architecture:     docs/broker/ARCHITECTURE.md
```
---
## SESSION_LOG appendix
See docs/broker/SESSION_LOG.md for full history of completed changes.