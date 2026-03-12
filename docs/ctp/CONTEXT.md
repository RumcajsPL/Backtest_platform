# CONTEXT — Block 9P+2 End (2026-03-12)

## Session summary
- API key type root cause found and fixed: Demo Write key required for all /demo/ endpoints
- Official API docs (OpenAPI v1.138.0) fully ingested — Groups 1, 3, 4 complete
- `portfolio.py` fixed: `positionID`/`instrumentID` aliases corrected (PascalCase confirmed live)
- `client.py` fixed: `/demo/portfolio` endpoint, `InstrumentID` close body, `get_order_info()` added
- `order_router.py` rewritten: two-step open flow (place → poll → positionID)
- `test_order_router.py` rewritten: 34/34 passing against new two-step flow
- Full suite: **71/71 tests passing**
- `docs/ctp/BROKER_INTEGRATION.md` complete (Groups 2 deferred — not needed for paper trading)
---
## Where we are
### Phase 0 COMPLETE — Steps 1–5 all done
All broker_support infrastructure is built, tested, and confirmed on the live API.
The tracker loop is running. OrderRouter can open and close demo positions.
### Next session: Phase 2 — first live demo trade via OrderRouter
**Goal**: manually execute one full open→track→close cycle end-to-end.
**Step-by-step**:
1. Write a minimal `scripts/broker_support/run_signal.py`:
   - Instantiate `EToroClient`, `InstrumentResolver`, `OrderRouter`
   - Call `router.open_position('DAX', 'SELL', amount=60.0, leverage=20, ...)`
   - Print returned `positionID`
   - Confirm it appears in `inspect_portfolio.py` output
2. Let `run_tracker_loop.py` run — confirm it detects the position
3. Manually close via `router.close_position(positionID, instrument_id=32)`
4. Confirm journal entry written to `outputs/broker_support/journal/trades.csv`
5. If all good → promote to Phase 2 automation: read signals from strategy YAML
**Primary candidate for first paper trade**:
`outputs/backtesting/trading_yamls/b651ec5c_c424a0e04327_strategy.yaml`
---
## Key empirical findings (locked — do not re-derive)
| Endpoint | Status | Notes |
|----------|--------|-------|
| GET /trading/info/demo/portfolio | ✅ 200 | Demo Write key required |
| GET /trading/info/demo/orders/{id} | ✅ docs | positionID resolution |
| GET /trading/info/real/pnl | ✅ 200 | Real Write key |
| GET /trading/info/trade/history | ✅ 200 | Write key; minDate=YYYY-MM-DD |
| GET /market-data/search?searchText=...&fields=... | ✅ 200 | fields param REQUIRED |
| GET /market-data/instruments?instrumentIds=32 | ✅ 200 | DAX confirmed |
| GET /trading/info/portfolio (no /demo/) | ❌ 403 | Wrong endpoint for Demo key |
| GET /trading/info/demo/pnl | ❌ 403 | Wrong key type in prior tests |
**Live position confirmed (2026-03-12 20:51)**:
- positionID=3464232739, instrumentID=32, isBuy=false, openRate=23556.77
- All 34 portfolio field names confirmed matching OpenPosition model
**Two-step open flow** (do not shortcut):
```
POST market-open-orders/by-amount → orderForOpen.orderID
GET demo/orders/{orderID} poll until statusID==1 → positions[0].positionID
```
---
## Test suite state
```
71/71 passing (2026-03-12, Python 3.13.12, Windows 10)
test_models.py             8 tests
test_csv_journal.py        7 tests
test_position_tracker.py  11 tests
test_time_utils.py        11 tests
test_order_router.py      34 tests
```
---
## Open issues
| ID | Description | Priority |
|----|-------------|----------|
| PHASE-2 | Wire strategy YAML → OrderRouter → live trades | P0 — next session |
| RESOLVER-FIELDS | InstrumentResolver missing 'fields' param + exact-match | P1 |
| B9O-009 | V2 shared memory for backtester | Deferred Phase 3 |
| WINZIP-32 | WinError 32 on GA temp YAMLs | Cosmetic |
---
## Useful commands
```powershell
# Full test suite
pytest tests/broker_support/ -v
# Inspect live portfolio
python scripts/broker_support/inspect_portfolio.py
# Tracker single cycle
python scripts/broker_support/run_tracker_loop.py --once --no-hours-guard
# Tracker continuous
python scripts/broker_support/run_tracker_loop.py
```
---
## Paper trade candidates (Phase 2 order)
| Priority | Candidate | Status | Notes |
|----------|-----------|--------|-------|
| 1st | c424a0e04327 | PRIMARY | Start here |
| 2nd | 20745ca991be | SECONDARY | After PRIMARY stable |
| Watch | c42f8b009283 | MONITOR | |
| Watch | c209820886c8 | SECONDARY MONITOR | Hard atr_multiplier cliff — do not promote above c42f |