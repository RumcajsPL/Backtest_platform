# CONTEXT.md — CTP Session State
# Updated: 2026-03-13 (Block 9P+4 end)
---
## Where We Are
```
BACKTESTING ENGINE:    V1 PRODUCTION — frozen.
BROKER INTEGRATION:    Phase 2 pipeline confirmed live 2026-03-13.
                       Stage 1 dry-run ✅. Stage 2 place-order path ✅.
                       Awaiting first live signal to place demo order.
```
---
## Phase 2 Deliverables (all in place — use phase2_deliverables_v3.zip)
```
configs/broker_support/broker_support_config.yaml
src/broker_support/config/broker_support_config.py
src/broker_support/utils/time_utils.py           (+ is_valid_trading_window())
src/broker_support/live/__init__.py
src/broker_support/live/live_data_fetcher.py
src/broker_support/live/live_config_patcher.py
src/broker_support/live/live_data_bundle.py      (FIXED: ltf_timeframe="1s", config=None)
src/broker_support/live/order_signal.py          (+ max_positions field)
src/broker_support/live/signal_bridge.py         (reads max_positions from YAML)
scripts/broker_support/run_signal.py             (+ _check_pyramiding() guard)
scripts/broker_support/run_signal_loop.py        (NEW: polls 60s, places 1 order, stops)
tests/broker_support/test_time_utils.py          (+ 19 WBWS+ tests, 30 total)
tests/broker_support/test_signal_pipeline_integration.py  (NEW: 63 integration tests)
```
---
## Test Status
```
90/90  unit tests passing       (pytest tests/broker_support/ -v)
63/63  integration tests passing (test_signal_pipeline_integration.py)
```
---
## Trade Constraint Status (all four closed)
| Constraint | Value | Enforced where |
|---|---|---|
| max_risk_percentile | 0.45 | RiskManager — uses full ARTF parquet |
| pyramiding_enabled | false | _check_pyramiding() in run_signal.py Stage 2 |
| max_positions | 1 | Same guard — source: strategy YAML |
| close_on_opposite | false | Emergent from pyramiding guard |
---
## Paper Trade Candidates
| Priority | Candidate | Status |
|----------|-----------|--------|
| 1st | c424a0e04327 | PRIMARY — active in run_signal.py |
| 2nd | 20745ca991be | SECONDARY — after PRIMARY stable |
| Watch | c42f8b009283 | MONITOR |
| Watch | c209820886c8 | SECONDARY MONITOR — do NOT promote (hard atr_multiplier cliff) |
---
## Open Issues
| ID | Description | Priority |
|----|-------------|----------|
| PHASE-2-STAGE2 | First live demo order: open → track → close | P0 — next signal |
| RESOLVER-FIELDS | InstrumentResolver missing 'fields' param + exact-match | P1 |
| WINZIP-32 | WinError 32 on GA temp YAMLs | Cosmetic |
---
## Useful Commands
```powershell
# Tests
pytest tests/broker_support/ -v

# Signal loop (polls 60s, places 1 order, stops)
python scripts/broker_support/run_signal_loop.py --verbose

# Single run (dry-run)
python scripts/broker_support/run_signal.py --verbose

# Single run (place order)
python scripts/broker_support/run_signal.py --place-order --verbose

# Diagnostics
python scripts/broker_support/inspect_portfolio.py
python scripts/broker_support/run_tracker_loop.py --once --no-hours-guard
python scripts/broker_support/run_tracker_loop.py
```
---
## Key Paths
```
Strategy YAML:    outputs/backtesting/trading_yamls/b651ec5c_c424a0e04327_strategy.yaml
ARTF parquet:     data/processed/ohlcv/DEUIDXEUR_1ME_20210101_20260301.parquet
BS config:        configs/broker_support/broker_support_config.yaml
Instrument map:   configs/broker_support/instrument_map.yaml  (symbol key: GER40)
Credentials:      configs/broker_support/broker_settings.env
```
---
## Next Session Start
1. Check if first demo order was placed (inspect_portfolio.py)
2. If yes: confirm journal entry, review tracker loop output
3. If no: run_signal_loop.py during DAX hours (09:00–16:00 UTC, skip 17–18)
4. After first order confirmed: plan Stage 3 automation loop