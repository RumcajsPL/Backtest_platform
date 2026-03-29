# CONTEXT.md — CTP Session State
# Claude session-to-session continuity. Facts live in ARCHITECTURE.md.
# IMPORTANT: from next session CONTEXT move completed changes to SESSION_LOG appendix and replace this line by "Completed changes goes to SESSION_LOG appendix"
# Updated: 2026-03-28
---
## Current State
```
Phase 2 (live pipeline):    COMPLETE 2026-03-18
Multi-instance week 1:      COMPLETE 2026-03-24 to 2026-03-28
Documentation overhaul:     IN PROGRESS 2026-03-28
  ARCHITECTURE.md:          Complete — all sources read and documented
  BROKER_INTEGRATION.md:    Updated 2026-03-28
  SKILL.md:                 Updated 2026-03-28 (description trimmed to 1024 chars)
  CONTEXT.md:               This file
First live trade: 2026-03-17 13:06 UTC
  positionID=3466009287, orderID=336588020
  BUY GER40 @ 23705.89, SL=23676.47, TP=23891.07, R:R=8.8x — profitable
```
---
## Deliverables This Session (2026-03-28)
```
scripts/broker_support/run_signal_loop.py
  - Pending-order reconciliation (Bug 1 fix)
  - Portfolio fetch errors no longer count against pipeline error budget (Bug 2 fix)
src/broker_support/live/live_data_fetcher.py
  - pd.to_datetime(..., format="ISO8601", utc=True) — pandas UserWarning fix
scripts/broker_support/run_tracker_loop.py
  - --instance flag: instance-scoped journal path and log filename
scripts/broker_support/inspect_portfolio.py
  - --instance flag, --all-positions flag, orderID display, CTP annotation
scripts/diagnostics/week_one_health_check.py
  - New: 6-section log analyser, writes txt report
docs/broker/ARCHITECTURE.md   — complete rewrite with Mermaid diagrams
docs/broker/BROKER_INTEGRATION.md — restructured, no redundancy
docs/broker/SKILL.md           — restructured, description ≤ 1024 chars
docs/broker/SESSION_LOG.md     — session appended log of changes -> uncharging other session documents from keeping history of important changes. Avaiable on request if deeper analysis required. Once change logged it is remove from respective session document to avoid redundancy.
docs/broker/CONTEXT.md         — this file
```
---
## Open Issues
### TradeEnricher 403 — fix ready, not yet applied
```
File:    src/broker_support/enrichment/trade_enricher.py
Problem: _HISTORY_LOOKBACK_DAYS = 90 hardcoded. Working window is 30 days
         (DEFAULT_DAYS_BACK=30 in broker_settings.env).
         90-day request → 403. Confirmed root cause 2026-03-28.
Fix:
  Remove: _HISTORY_LOOKBACK_DAYS = 90
  Change: from_date = datetime.now() - timedelta(days=_HISTORY_LOOKBACK_DAYS)
  To:     from_date = datetime.now() - timedelta(days=settings.default_days_back)
  Add import: from src.broker_support.config.settings import settings
Effect when fixed: trades.csv will have correct exit_price and profit_loss.
  Drawdown guard and consecutive loss reconstruction will work across restarts.
```
### 240166 unconfirmed orders — verify before restart
```
4 orders placed 2026-03-26 to 2026-03-27 where portfolio scan timed out.
Verify each via: inspect_portfolio.py --instance 240166 --all-positions
or eToro trade history UI.
orderID=338749124  2026-03-26 18:35:15
orderID=338770199  2026-03-26 18:36:35
orderID=338747252  2026-03-26 18:37:56
orderID=339031085  2026-03-27 17:29:59
Confirmed positionID: 3475134299 (4th attempt only)
Check open_positions.json for instance 240166 before restart.
If 3475134299 is closed: remove from file before restarting loop.
```
---
## Active Instances
```
c424    → broker_support_config.yaml           (1-min, halted 2026-03-27 — bug fixed)
240166  → broker_support_config_240166.yaml    (10-min, most signals)
7ffbc5  → broker_support_config_7ffbc5.yaml    (1-min, stable)
61875   → broker_support_config_61875.yaml     (1-min, stable)
```
RiskManager calibration: insufficient data after 1 week.
Continue another full week before reviewing 0.45% threshold.
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
1. Fix TradeEnricher (see Open Issues above — one-line change)
2. Deploy all 2026-03-28 deliverables before restarting loops
3. Verify 240166 unconfirmed orders and open_positions.json state
4. Restart all 4 loops for week 2
5. After week 2: review RiskManager 0.45% threshold
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
```
---
## V2 Backlog
```
- Tests: PaperTradingGuard, order_router fast-fill, pending_order reconciliation
- run_tracker_loop.py: remove closed positionID from open_positions.json on close
- run_tracker_loop.py: call guard.record_trade_result() on close detection
- Merge run_tracker_loop.py into run_signal_loop.py (reduce 8 terminals to 4)
- Increase _PORTFOLIO_POLL_MAX_ATTEMPTS from 10 to 20 if scan timeout recurs
- daily_order_cap safeguard
- Promote 20745ca991be after PRIMARY stable 1 week
- Scripts and tests documentation session (separate session)
```
---
## Useful Commands
```powershell
# Loops
python scripts/broker_support/run_signal_loop.py --instance c424
python scripts/broker_support/run_signal_loop.py --instance 240166 --quiet
python scripts/broker_support/run_signal_loop.py --instance 7ffbc5 --quiet
python scripts/broker_support/run_signal_loop.py --instance 61875 --quiet
# Kill switches
echo "" > STOP           # halt all
echo "" > STOP_240166    # halt one
del STOP_240166
# Diagnostics
python scripts/broker_support/inspect_portfolio.py --instance 240166 --all-positions
python scripts/broker_support/run_tracker_loop.py --instance 240166 --once --no-hours-guard
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
Signal logs:      outputs/broker_support/logs/run_signal_loop_<instance>_YYYY-MM-DD.log
Tracker logs:     outputs/broker_support/logs/tracker_<instance>_YYYY-MM-DD.log
Architecture:     docs/ctp/ARCHITECTURE.md
```