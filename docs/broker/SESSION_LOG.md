# SESSION_LOG.md — CTP Broker Integration Change History
# Append-only. One entry per session. Most-recent first.
# Details live here so CONTEXT.md and ARCHITECTURE.md stay lean.
---
## 2026-03-29 — Loop consolidation + TradeEnricher fix

### Objective
Merge run_tracker_loop.py into run_signal_loop.py → run_demo_trading.py.
Reduce parallel terminals from 8 to 4. Apply TradeEnricher 403 fix.
Update week_one_health_check.py for new log filename and trades.csv P&L stats.

### Files changed

**src/broker_support/enrichment/trade_enricher.py**
- Removed: `_HISTORY_LOOKBACK_DAYS = 90` constant
- Removed: `from typing import Optional` (unused after refactor)
- Added: `from src.broker_support.config.settings import settings`
- Added: `timezone` to datetime imports
- Changed: `from_date = datetime.now() - timedelta(days=_HISTORY_LOOKBACK_DAYS)`
  → `from_date = datetime.now(timezone.utc) - timedelta(days=settings.default_days_back)`
- Changed: warning log message references `settings.default_days_back` (30) not 90
- Effect: trades.csv now receives correct exit_price and profit_loss.
  Drawdown guard and consecutive-loss reconstruction work across restarts.

**scripts/broker_support/run_demo_trading.py** (new — replaces run_signal_loop.py)
- Merged full run_tracker_loop.py functionality via `_run_tracker_cycle()`
- `_run_tracker_cycle()` unrolls tracker.track() to obtain Trade objects, enabling:
  - V2 item: `guard.record_trade_result(pnl)` called per CTP-placed closed position
  - V2 item: closed positionIDs removed from ctp_open_position_ids + open_positions.json
- Tracker cycle runs at step 4 of every poll (before pyramiding check), same cadence
  as signal loop (1min or 10min per TF)
- Snapshots now instance-scoped: outputs/broker_support/snapshots/<id>/
- Log filename changed: `run_signal_loop_<id>_YYYY-MM-DD.log`
  → `demo_trading_<id>_YYYY-MM-DD.log`
- All logic from run_signal_loop.py preserved without modification:
  pending-order reconciliation, pyramiding check, guard sequence, WBWS+ gate,
  consecutive-loss handler, order placement, kill switch checks
- Tracker portfolio fetch errors are logged, never counted against pipeline budget

**scripts/broker_support/run_tracker_loop.py**
- Superseded by run_demo_trading.py. Can be archived.
- No code changes made — kept intact as reference until loops confirmed stable.

**scripts/diagnostics/week_one_health_check.py**
- Updated log filename pattern: `run_signal_loop_` → `demo_trading_`
- Added Section 7: trades.csv P&L summary (total trades, win rate, total PnL,
  avg win, avg loss, largest win, largest loss, per-instrument breakdown)

### Architecture rules confirmed unchanged
- _make_request() not touched
- LiveConfigPatcher strategy fields not touched
- PaperTradingGuard does not call sys.exit()
- pipeline_error_streak only from SignalBridge block
- open_positions.json written only by run_demo_trading.py (not tracker)

### V2 items promoted to active
- remove closed positionID from open_positions.json on close detection ✅
- call guard.record_trade_result() on close detection ✅

### Deployment sequence
1. Deploy trade_enricher.py
2. Stop existing run_signal_loop.py + run_tracker_loop.py processes
3. Deploy run_demo_trading.py
4. Verify 240166 open_positions.json before restart (see CONTEXT open issues)
5. Start 4 × run_demo_trading.py --instance <id>
6. Monitor demo_trading_<id>_YYYY-MM-DD.log for first tracker cycle output

---
## 2026-03-28 — Documentation overhaul + Bug fixes

### Files changed
- scripts/broker_support/run_signal_loop.py: pending-order reconciliation (Bug 1),
  portfolio fetch errors excluded from pipeline error budget (Bug 2)
- src/broker_support/live/live_data_fetcher.py: pandas UserWarning fix
  (pd.to_datetime format="ISO8601")
- scripts/broker_support/run_tracker_loop.py: --instance flag added
- scripts/broker_support/inspect_portfolio.py: --instance, --all-positions,
  orderID display, CTP annotation
- scripts/diagnostics/week_one_health_check.py: new 6-section log analyser
- docs/broker/ARCHITECTURE.md: complete rewrite with Mermaid diagrams
- docs/broker/BROKER_INTEGRATION.md: restructured, redundancy removed
- docs/broker/SKILL.md: restructured, description ≤ 1024 chars

### Known issue carried forward
- TradeEnricher _HISTORY_LOOKBACK_DAYS=90 → fix confirmed, applied 2026-03-29
---
## 2026-03-29 — Loop consolidation + TradeEnricher fix

### Objective
Merge run_tracker_loop.py into run_signal_loop.py → run_demo_trading.py.
Reduce parallel terminals from 8 to 4. Apply TradeEnricher 403 fix.
Update week_one_health_check.py for new log filename and trades.csv P&L stats.

### Files changed

**src/broker_support/enrichment/trade_enricher.py**
- Removed: `_HISTORY_LOOKBACK_DAYS = 90` constant
- Removed: `from typing import Optional` (unused after refactor)
- Added: `from src.broker_support.config.settings import settings`
- Added: `timezone` to datetime imports
- Changed: `from_date = datetime.now() - timedelta(days=_HISTORY_LOOKBACK_DAYS)`
  → `from_date = datetime.now(timezone.utc) - timedelta(days=settings.default_days_back)`
- Changed: warning log message references `settings.default_days_back` (30) not 90
- Effect: trades.csv now receives correct exit_price and profit_loss.
  Drawdown guard and consecutive-loss reconstruction work across restarts.

**scripts/broker_support/run_demo_trading.py** (new — replaces run_signal_loop.py)
- Merged full run_tracker_loop.py functionality via `_run_tracker_cycle()`
- `_run_tracker_cycle()` unrolls tracker.track() to obtain Trade objects, enabling:
  - V2 item: `guard.record_trade_result(pnl)` called per CTP-placed closed position
  - V2 item: closed positionIDs removed from ctp_open_position_ids + open_positions.json
- Tracker cycle runs at step 4 of every poll (before pyramiding check), same cadence
  as signal loop (1min or 10min per TF)
- Snapshots now instance-scoped: outputs/broker_support/snapshots/<id>/
- Log filename changed: `run_signal_loop_<id>_YYYY-MM-DD.log`
  → `demo_trading_<id>_YYYY-MM-DD.log`
- All logic from run_signal_loop.py preserved without modification:
  pending-order reconciliation, pyramiding check, guard sequence, WBWS+ gate,
  consecutive-loss handler, order placement, kill switch checks
- Tracker portfolio fetch errors are logged, never counted against pipeline budget

**scripts/broker_support/run_tracker_loop.py**
- Superseded by run_demo_trading.py. Can be archived.
- No code changes made — kept intact as reference until loops confirmed stable.

**scripts/diagnostics/week_one_health_check.py**
- Updated log filename pattern: `run_signal_loop_` → `demo_trading_`
- Added Section 7: trades.csv P&L summary (total trades, win rate, total PnL,
  avg win, avg loss, largest win, largest loss, per-instrument breakdown)

### Architecture rules confirmed unchanged
- _make_request() not touched
- LiveConfigPatcher strategy fields not touched
- PaperTradingGuard does not call sys.exit()
- pipeline_error_streak only from SignalBridge block
- open_positions.json written only by run_demo_trading.py (not tracker)

### V2 items promoted to active
- remove closed positionID from open_positions.json on close detection ✅
- call guard.record_trade_result() on close detection ✅

### Deployment sequence
1. Deploy trade_enricher.py
2. Stop existing run_signal_loop.py + run_tracker_loop.py processes
3. Deploy run_demo_trading.py
4. Verify 240166 open_positions.json before restart (see CONTEXT open issues)
5. Start 4 × run_demo_trading.py --instance <id>
6. Monitor demo_trading_<id>_YYYY-MM-DD.log for first tracker cycle output

---
## 2026-03-28 — Documentation overhaul + Bug fixes

### Files changed
- scripts/broker_support/run_signal_loop.py: pending-order reconciliation (Bug 1),
  portfolio fetch errors excluded from pipeline error budget (Bug 2)
- src/broker_support/live/live_data_fetcher.py: pandas UserWarning fix
  (pd.to_datetime format="ISO8601")
- scripts/broker_support/run_tracker_loop.py: --instance flag added
- scripts/broker_support/inspect_portfolio.py: --instance, --all-positions,
  orderID display, CTP annotation
- scripts/diagnostics/week_one_health_check.py: new 6-section log analyser
- docs/broker/ARCHITECTURE.md: complete rewrite with Mermaid diagrams
- docs/broker/BROKER_INTEGRATION.md: restructured, redundancy removed
- docs/broker/SKILL.md: restructured, description ≤ 1024 chars

### Known issue carried forward
- TradeEnricher _HISTORY_LOOKBACK_DAYS=90 → fix confirmed, applied 2026-03-29

### Runtime fixes discovered during deployment testing (2026-03-29 evening)

**trade_enricher.py — 403 boundary fix**
- `timedelta(days=30)` lands exactly on the exclusive API boundary → 403
- Changed to `days=settings.default_days_back - 1` (29 days)
- New empirical API fact: trade/history 30-day window boundary is exclusive.
  Exactly 30 days back returns 403. Use 29 days in practice.
- ARCHITECTURE §13 should be updated: "Working window: 30 days (exclusive boundary —
  use 29 days max to avoid 403)"

**run_demo_trading.py — stale snapshot guard**
- Old run_tracker_loop.py wrote last_positions.csv containing all portfolio positions
  (including external/manual trades). On first run of isolated tracker cycle, the
  diff between stale snapshot (external IDs) and new CTP-only list produced phantom
  "closed" detections with positionId=nan, instrumentId=0.
- Added stale snapshot guard: checks if snapshot contains any positionID not in
  ctp_open_position_ids. If yes — logs WARNING, invalidates snapshot (cold start),
  no diff runs that cycle. Snapshot is rebuilt clean on same poll.
- One-time guard — fires only on first start after deployment. Self-clears.

**run_demo_trading.py — tracker CTP isolation confirmed working**
- "2 external position(s) on this demo account ignored" logged correctly
- External manual trades never entered snapshot, trades.csv, or guard calculations
- trades.csv not created (correct — no closed CTP trades yet)
- last_positions.csv created empty (correct — no CTP positions open)
- open_positions.json deleted manually (3475134299 was closed pre-new-loop);
  will be auto-created on first new position placement

**Deployment state at session end**
- 240166 instance confirmed running cleanly
- c424 / 7ffbc5 / 61875 to be started (will hit stale snapshot guard once, self-correct)
- All 4 instances use run_demo_trading.py — run_tracker_loop.py fully superseded