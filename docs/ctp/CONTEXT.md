# CONTEXT -- Block 9P+4 End (2026-03-13)
## Session summary (2026-03-13)
- Phase 2 architecture designed and all deliverables built
- Full pipeline design discussion: DataLoader bypass strategy, WBWS+ layering rationale
- All Phase 2 files produced
- data_contracts.py verified -- fixed 3 bugs in live_data_bundle.py (v2 zip)
- Trade constraints analysis completed:
    max_risk_percentile: confirmed OK (RiskManager + full ARTF parquet)
    pyramiding_enabled / max_positions / close_on_opposite: gap identified and closed
- _check_pyramiding() guard implemented in run_signal.py
- max_positions added to OrderSignal; signal_bridge.py reads it from raw YAML
- SKILL.md updated to reflect all Phase 2 changes
- Final delivery: phase2_deliverables_v3.zip
---
## Where we are
### Documentation: COMPLETE
- docs/ctp/BROKER_INTEGRATION.md -- CTP paper trading reference
- docs/ctp/API_REFERENCE.md -- full eToro API reference
### Phase 0 + Steps 1-5: COMPLETE
All broker_support infrastructure built, tested (71/71), confirmed on live API.
### Phase 2 -- Stage 1 + infrastructure: COMPLETE (this session)
All new files delivered and verified. Not yet tested on live API.
---
## Phase 2 Deliverables (use phase2_deliverables_v3.zip)
```
configs/broker_support/broker_support_config.yaml   <- live execution config
src/broker_support/config/broker_support_config.py  <- typed config schema
src/broker_support/utils/time_utils.py              <- UPDATED: + is_valid_trading_window()
src/broker_support/live/__init__.py                 <- (create empty)
src/broker_support/live/live_data_fetcher.py        <- fetches candles from eToro API
src/broker_support/live/live_config_patcher.py      <- patches strategy YAML for live
src/broker_support/live/live_data_bundle.py         <- FIXED: ltf_timeframe="1s", config=None
src/broker_support/live/order_signal.py             <- UPDATED: + max_positions field
src/broker_support/live/signal_bridge.py            <- UPDATED: reads max_positions from YAML
scripts/broker_support/run_signal.py                <- UPDATED: + _check_pyramiding() guard
tests/broker_support/test_time_utils.py             <- UPDATED: + 19 WBWS+ tests (30 total)
```
### Change log (v1 -> v2 -> v3)
v2 (data_contracts.py verification):
  - live_data_bundle.py: ltf_timeframe None->"1s"
  - live_data_bundle.py: dropped DataConfig sentinel, config=None
  - live_data_bundle.py: removed unused DataConfig/DataFileConfig/DateRange imports
v3 (trade constraint enforcement):
  - order_signal.py: added max_positions: int field
  - signal_bridge.py: reads pos_ctrl from raw YAML, sets _max_positions + _pyramiding_enabled
  - signal_bridge.py: passes max_positions to OrderSignal constructor
  - signal_bridge.py: updated docstring with constraint responsibility table
  - run_signal.py: added _check_pyramiding() helper function
  - run_signal.py: added _check_pyramiding() call in Stage 2 between WBWS+ gate and OrderRouter
  - run_signal.py: Stage 1 output now shows max_positions
---
## Trade Constraint Status (all four closed)
| Constraint | Value | Status | Notes |
|---|---|---|---|
| max_risk_percentile | 0.45 | CLOSED | RiskManager uses full ARTF parquet -- correct |
| pyramiding_enabled | false | CLOSED | _check_pyramiding() in run_signal.py Stage 2 |
| max_positions | 1 | CLOSED | Same guard, source = strategy YAML (not safety section) |
| close_on_opposite | false | CLOSED | Emergent: pyramiding guard fires first |
---
## Phase 2 Architecture
### Live pipeline (Stage 2 with all guards)
```
BrokerSupportConfig.from_yaml()
    |
LiveConfigPatcher.load_and_patch() -> patched StrategyConfig
    |
LiveDataFetcher.fetch(symbol) -> (df_strategy, df_htf)
    |
build_live_data_bundle(...) -> DataBundle [artf=full historical parquet]
    |
SignalGenerator -> FilterPipeline [strategy time_filter 08:30-20:30 CET]
    |
Last-bar signal check
    |
RiskManager [max_risk_percentile enforced using ARTF]
    |
WBWS+ gate [non-blocking, wbws_window_valid flag]
    |
OrderSignal(direction, sl, tp, max_positions=1, ...)
    |
[Stage 2 only]:
  _check_pyramiding() -> GET /demo/portfolio -> count open DAX -> abort if >=1
    |
OrderRouter.open_position() -> positionID
```
### Key design decisions (locked)
1. DataLoader bypassed -- no parquet reads in live context except artf
2. TradeSimulator NOT called -- only last-bar signal + RiskManager needed
3. Strategy time_filter kept unchanged in patched config
4. WBWS+ is a separate, non-blocking gate
5. artf path explicit in broker_support_config.yaml
6. Sentinel pattern for strategy/htf paths
7. max_positions from strategy YAML (backtested), not safety section
---
## Phase 2 Plan -- Next Steps
### Immediate (next session)
1. Copy v3 zip files into project
2. Create src/broker_support/live/__init__.py (empty)
3. Run existing 71 tests -- must still pass (changes are additive)
4. Run new test_time_utils.py -- expect 90/90
5. Run Stage 1 dry-run: python scripts/broker_support/run_signal.py --verbose
6. Review output: confirm signal or confirm "no signal" with recent signal history
7. Fix any issues from live run
### Stage 2 (after Stage 1 confirmed)
1. During DAX hours, WBWS+ window open:
   python scripts/broker_support/run_signal.py --place-order --verbose
2. Confirm positionID in journal / inspect_portfolio.py
3. Let tracker loop detect close
4. Review full journal entry
---
## Open Issues
| ID | Description | Priority |
|----|-------------|----------|
| PHASE-2-STAGE1 | Copy files, run tests, run Stage 1 dry-run | P0 -- next session |
| PHASE-2-STAGE2 | First live demo trade: open->track->close | P0 -- after Stage 1 |
| DATA-CONTRACTS | RESOLVED -- verified and fixed in v2/v3 | CLOSED |
| RESOLVER-FIELDS | InstrumentResolver missing 'fields' param + exact-match | P1 |
| B9O-009 | V2 shared memory for backtester | Deferred Phase 3 |
| WINZIP-32 | WinError 32 on GA temp YAMLs | Cosmetic |
---
## Paper trade candidates (Phase 2 order)
| Priority | Candidate | Status | Notes |
|----------|-----------|--------|-------|
| 1st | c424a0e04327 | PRIMARY -- active in run_signal.py | Start here |
| 2nd | 20745ca991be | SECONDARY | After PRIMARY stable |
| Watch | c42f8b009283 | MONITOR | |
| Watch | c209820886c8 | SECONDARY MONITOR | Hard atr_multiplier cliff -- do not promote |
---
## Useful commands
```powershell
# Tests
pytest tests/broker_support/ -v
# Existing
python scripts/broker_support/inspect_portfolio.py
python scripts/broker_support/run_tracker_loop.py --once --no-hours-guard
python scripts/broker_support/run_tracker_loop.py
# Phase 2
python scripts/broker_support/run_signal.py --verbose                  # Stage 1 dry-run
python scripts/broker_support/run_signal.py --place-order --verbose    # Stage 2 supervised
python scripts/broker_support/run_signal.py --place-order --force-window --verbose  # testing only
```