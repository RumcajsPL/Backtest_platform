# CONTEXT.md — CTP Session State
# Claude session-to-session continuity. Facts live in ARCHITECTURE.md.
# Governance, roles, monitoring: see GOV.md.
# Completed changes go to SESSION_LOG.md appendix.
# Updated: 2026-05-06
---

## Current State
```
Phase 2 (live pipeline):    COMPLETE 2026-03-18
Multi-instance week 1:      COMPLETE 2026-03-24 to 2026-03-28
Loop consolidation:         COMPLETE 2026-03-29 (8 terminals → 4)
  run_demo_trading.py:      Signal + tracker unified — DEPLOYED, RUNNING
  Tracker isolation:        Full CTP scope — external positions never enter trades.csv
  Stale snapshot guard:     Active — auto-invalidates pre-isolation snapshots on first run
  week_one_health_check:    Updated — new log filename + trades.csv P&L section 7
Week 2-4 loops:             RUNNING from 2026-03-29
  3 weeks of paper trading completed and analysed 2026-04-18
  health_check: outputs\broker_support\diagnostics\health_check_2026-04-18.txt
Broker integration bug fixes: COMPLETE 2026-05-06 (see SESSION_LOG)
  PascalCase field mismatch:  FIXED — trades now written with correct trade_id/instrument
  30-day boundary:            FIXED — DEFAULT_DAYS_BACK=29 across all components
  Enricher lookback:          FIXED — uses full 29 days (was erroneously 28)
First live trade: 2026-03-17 13:06 UTC
  positionID=3466009287, orderID=336588020
  BUY GER40 @ 23705.89, SL=23676.47, TP=23891.07, R:R=8.8x — profitable
```

---

## Active Instances
```
c424    → broker_support_config.yaml           (1-min)
240166  → broker_support_config_240166.yaml    (10-min, most signals)
7ffbc5  → broker_support_config_7ffbc5.yaml    (1-min)
61875   → broker_support_config_61875.yaml     (1-min)
```
RiskManager calibration: 3 weeks of data now available. Review 0.45% threshold.

---

## Open Issues
```
1. trade/history endpoint returning 403 (InsufficientPermissions) — UNDER INVESTIGATION
   Observed: backfill_trades_csv.py dry-run, 2026-05-06
   minDate=2026-04-06 (29 days back) — date boundary is NOT the cause
   Key is Demo Write — confirmed placing trades successfully with same key
   eToro returns identical 403 errorCode for both permission and boundary violations
   — cannot distinguish from error message alone
   Action next session: test with minDate=2026-05-05 (1 day back) to isolate
   whether boundary or genuine permission scope issue. Read eToro portal key details.
   Impact: enricher silently returns exit_price=0.0 / profit_loss=0.0 on every close
   until resolved. trade_id and instrument ARE now written correctly (PascalCase fix).

2. trades.csv corrupted records — DATA ACCEPTED AS LOST
   All four instances have 1 corrupted row (trade_id=nan, UNKNOWN_0, exit_price=0.0)
   These pre-date the PascalCase fix and are outside reliable recovery scope.
   Backfill attempted but blocked by issue 1 above.
   Action: once issue 1 resolved, re-run backfill dry-run to recover if within window.
   If still blocked: delete corrupted rows manually (they carry no valid data).

3. Health check 2026-04-18 — issues to analyse (not yet done):
   File: outputs\broker_support\diagnostics\health_check_2026-04-18.txt
   Action: Claude.ai to review at next session and produce advisory + action list.

4. RiskManager 0.45% threshold — review deferred from week 2.
   Now 3 weeks of data available. Claude.ai to assess whether recalibration needed.
```

---

## Watch Items
```
1. trades.csv going forward: PascalCase fix deployed. Confirm on first new close:
   - trade_id is a valid integer string (not 'nan')
   - instrument is 'GER40' (not 'UNKNOWN_0')
   - exit_price and profit_loss non-zero IF history endpoint resolves (issue 1 above)
   - open_positions.json correctly shrinks by 1 on each close detection
2. open_positions.json all four instances: currently contain stale positionIDs
   (positions that closed before PascalCase fix — cleanup failed silently).
   These will NOT cause phantom closes — stale snapshot guard handles this.
   They will be cleaned up naturally as new positions are placed and closed.
   Do not manually edit unless confirmed all stale IDs are >29 days old.
```

---

## Paper Trade Candidates
| Priority | Candidate | WFO | Ruin | Notes |
|----------|-----------|-----|------|-------|
| 1st | c424a0e04327 | 0.8108 | 0.000 | PRIMARY — running 3+ weeks |
| 2nd | 20745ca991be | 0.7201 | 0.054 | SECONDARY — promote after PRIMARY stable 1 week |
| Watch | c42f8b009283 | 0.6473 | 0.000 | MONITOR |
| Watch | c209820886c8 | 0.5699 | 0.000 | Do NOT promote |
240166 candidate (run 822f1889): WFO=0.8886, Ruin=0.000. Promote decision deferred.

---

## Next Session Actions
```
Priority 1 — Resolve trade/history 403 (Open Issue 1):
  Test: python -c "from src.broker_support.client.client import EToroClient; 
        from datetime import datetime, timedelta, timezone;
        c=EToroClient(); print(c.fetch_closed_trades(
        from_date=datetime.now(timezone.utc)-timedelta(days=1)))"
  If 403 with 1 day back → genuine permission scope issue → regenerate Demo Write key
  If 200 with 1 day back → boundary still wrong somewhere → read fetch_closed_trades()
  Once resolved: re-run backfill dry-run to recover corrupted trades.csv records

Priority 2 — Confirm first new close journalled correctly (Watch Item 1):
  After loop runs for a few days, check trades.csv for:
  trade_id not nan, instrument=GER40, exit_price>0, profit_loss non-zero
  If exit_price=0.0 still: history endpoint still 403 — escalate Priority 1

Priority 3 — Live validation (IN PROGRESS):
  Track B: COMPLETE. Analysis in docs/broker/TRACK_B_ANALYSIS.md
  Track A: NOT YET STARTED
    Instructions in docs/broker/AGENT_INSTRUCTIONS_VALIDATION.md (INSTRUCTION A1)
    Relay to Agent B next session
  61875 RISK_REJECTED investigation: still pending

Priority 4 — Trading advisory (Claude.ai):
  a. Review health_check_2026-04-18.txt → produce findings + action list
  b. Review RiskManager 0.45% threshold with 3 weeks of live data
  c. Assess candidate promotion: 20745ca991be readiness vs PRIMARY stability

Priority 5 — Monitoring setup (GOV.md defined — needs implementation):
  a. Author Agent C health check schedule instruction
  b. Author Agent D liveness check script instruction

Priority 6 — V2 backlog (dev work):
  a. Tests: PaperTradingGuard, order_router fast-fill, pending_order reconciliation,
            _run_tracker_cycle integration
  b. Scripts and tests documentation
  c. Increase _PORTFOLIO_POLL_MAX_ATTEMPTS from 10 to 20 if scan timeout recurs
  d. daily_order_cap safeguard
```

---

## Test Status
```
90/90  unit tests passing
63/63  integration tests passing
Not yet covered (backlog):
  PaperTradingGuard drawdown + CTP isolation
  order_router fast-fill path
  pending_order reconciliation
  _run_tracker_cycle (new — integrated tracker)
```

---

## Governance Status
```
GOV.md:   CREATED 2026-04-19 (new — broker integration specific)
  Agent A: Claude Code — Dev Lead
  Agent B: Codex — Rapid Dev
  Agent C: Qwen Code 3.6 (local) — QA / Search / Health Monitor
  Agent D: OpenCode / Gemma4 — Dev (B overflow) / Loop Liveness Monitor
  Monitoring protocol: defined in GOV.md Section 6
  Agent instruction template: defined in GOV.md Section 9
  Implementation of monitoring agents: PENDING (next session priority)
```

---

## Useful Commands
```powershell
# Loops (unified — 4 terminals)
python scripts/broker_support/run_demo_trading.py --instance c424 --quiet
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
Health check:     outputs/broker_support/diagnostics/
Architecture:     docs/broker/ARCHITECTURE.md
Governance:       docs/broker/GOV.md
```

---

## SESSION_LOG appendix
See docs/broker/SESSION_LOG.md for full history of completed changes.
