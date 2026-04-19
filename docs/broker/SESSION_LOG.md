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

---
## 2026-04-19 — Governance framework + agent monitoring protocol

### Objective
Create GOV.md for broker integration (adapted from V2 Backtester GOV.md).
Update SKILL.md and CONTEXT.md to reflect new governance, agent roster, and
Claude.ai expanded authorities. No source code changed this session.

### Documents changed

**docs/broker/GOV.md** (new file)
- Created broker-integration-specific governance document
- 4-agent roster: A=Claude Code, B=Codex, C=Qwen Code 3.6 (local), D=OpenCode/Gemma4
- Claude.ai role expanded: direct file read (confirmed), write authority on .md/text files
  (no approval required), trading advisor role formalised
- Monitoring protocol defined (Section 6):
  - Agent C: scheduled health checks every 24h, report-only, no autonomous action
  - Agent D: loop liveness checks every 30 min, bounded autonomous restart authority
  - Restart strictly forbidden after HaltLoopError / PauseUntilTomorrowError / kill switch
  - Auto-restart limit: 2 per instance per 24h
- Agent instruction template defined (Section 9) — standard format for all agent tasks
- Environment structure carried from V2 GOV with broker-specific production rules
- Sprint/session checklist formalised (Section 10)

**docs/broker/SKILL.md** (updated)
- Added governance summary block at top (agent roster, Claude.ai authorities)
- Added GOV.md to session start protocol (step 3)
- Added monitoring "What NOT to Do" rules (no restart after HaltLoopError etc.)
- Updated open_positions.json rule: references run_demo_trading.py (not run_signal_loop.py)
- Updated session deliverables: added GOV.md to list

**docs/broker/CONTEXT.md** (updated)
- Added GOV.md governance status section
- Restructured Next Session Actions: priority 1=trading advisory, 2=monitoring
  implementation, 3=V2 backlog dev
- Clarified health check 2026-04-18 as open issue requiring analysis
- RiskManager threshold review elevated (3 weeks data now available)

### Not done this session
- Health check 2026-04-18 analysis (deferred — priority 1 next session)
- Monitoring agent implementation scripts (GOV.md defines protocol; implementation pending)
- No source code changes

---
## 2026-04-19 (continued) — Live validation project + agent instructions

### Objective
Design and formalise the live vs backtest correspondence validation project.
Author ready-to-execute agent instructions for both tracks.

### Analysis done
- Reviewed SignalBridge source to confirm what is already logged per poll
- Reviewed existing logs (demo_trading_240166_2026-04-01.log) to confirm log
  structure, message patterns, and data availability
- Confirmed 10-min parquet coverage to 2026-04-01 (matches live trading start)
- Key finding: Track B requires NO new logging code — all needed data already
  exists in demo_trading logs (bar_timestamp, bid_price, ATR, SL/TP distances,
  result, rejection reason)

### Documents created/updated

**docs/broker/LIVE_VALIDATION.md** (new file)
- Full project definition: Track A (price alignment) + Track B (signal correspondence)
- Approach, deliverables, input files, decision framework per track
- Assigned agents: A1 → Agent B, B1 → Agent C
- Open questions identified before script authoring

**docs/broker/AGENT_INSTRUCTIONS_VALIDATION.md** (new file)
- INSTRUCTION A1: Agent B → price_alignment_check.py
  Fetches broker TenMin candles for overlap window, compares to historical parquet,
  produces alignment report with bias direction and ALIGNED/INVESTIGATE/MISALIGNED verdict
- INSTRUCTION B1: Agent C → signal_log_extractor.py
  Parses all demo_trading_*.log files, extracts per-poll pipeline outcomes to CSV
  (bar_timestamp, result, direction, ATR, SL/TP distances, rejection reason)

**docs/broker/CONTEXT.md** (updated)
- Elevated live validation to Priority 1 next session
- Added Key Paths entry for validation outputs

### Not done
- Scripts not yet written (agent instructions ready for relay)
- Track A/B analysis not yet done (scripts must run first)

---
## 2026-04-19 (continued) — Track B analysis + extractor fixes

### Objective
Run signal_log_extractor.py, analyse results, produce Track B findings.

### Results
- 13,901 total polls, 12,900 stage-5, date range 2026-03-29 to 2026-04-17
- 3 signals, 24 RISK_REJECTED, 12,873 NO_SIGNAL
- Only 240166 and 61875 captured (c424/7ffbc5 missing — extractor bug)

### Analysis findings (full detail in docs/broker/TRACK_B_ANALYSIS.md)
- Signal frequency: CONSISTENT with backtest expectations for 240166
- Signal direction: all SELL — correct for market conditions (April sell-off)
- Signal quality 240166: R:R 7.0x, SL ~1.1×ATR — healthy
- Signal quality 61875: one signal R:R 2.6x — watch for 1-min calibration issue
- RISK_REJECTED: all 24 from 61875, threshold_pct=0.28 — suspected ATR scale
  mismatch between 1-min strategy ATR and monthly ARTF percentile distribution
  This is now Priority 2 investigation item

### Files changed
**docs/broker/TRACK_B_ANALYSIS.md** (new)
  Full signal correspondence analysis with per-signal breakdown, rejection
  analysis, verdict table, and action items

**scripts/diagnostics/signal_log_extractor.py** (two bug fixes applied directly)
  Bug 1: POLL_HEADER_PATTERN \d+ → [\w]+ to capture alphanumeric instance IDs
         (c424, 7ffbc5 were silently excluded)
  Bug 2: open() encoding='utf-8' → errors='replace' to handle Windows-1252
         characters (0xa6 byte in Apr 8-9 logs caused mid-file parse abort)

**docs/broker/LIVE_VALIDATION.md** (execution order updated to reflect done/todo)
**docs/broker/CONTEXT.md** (priorities updated: Track B done, Track A next,
  61875 RISK_REJECTED investigation added as Priority 2)

---
## 2026-04-19 (continued) — API facts: candle pagination + OHLCV lag

### Objective
Document two precision points raised by Owner: 1000-bar per request limit
with pagination requirement, and 1-hour lag on historical OHLCV availability.

### Documents updated

**docs/broker/API_REFERENCE.md**
- Candle endpoint section expanded:
  - Hard maximum 1000 bars per single request, applies to all timeframes equally
  - Pagination note: windows > 1000 bars require multiple requests with date offsets
  - CTP current config (500 + 120) confirmed well within limit
  - `OneWeek` confirmed as highest available broker TF; `OneMonth` does NOT exist
  - 1-hour lag documented with example and validation impact note

**docs/broker/ARCHITECTURE.md** (Section 13 Empirical API Facts)
- Added: Candles hard limit precision (pagination note added)
- Added: Broker timeframe ceiling (`OneWeek` max, `OneMonth` absent)
- Added: Historical OHLCV lag (~1h, boundary comparison guidance)

**docs/broker/LIVE_VALIDATION.md** (Track A section)
- Added pagination note: overlap window ~1260 bars > 1000-bar limit
  Agent B must issue two requests and merge/deduplicate
- Added 1-hour lag note with guidance for future re-runs

**docs/broker/AGENT_INSTRUCTIONS_VALIDATION.md** (INSTRUCTION A1)
- Step 2 updated: two-request fetch strategy specified explicitly
  Agent B instructed to merge, deduplicate, sort before comparison