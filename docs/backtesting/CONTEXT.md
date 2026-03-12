# CONTEXT — Block 9P+1 End (2026-03-12)
## Session summary
- Confirmation run b651ec5c analysed — Phase 1 gate FULLY CLOSED
- broker_support Phase 0 Steps 1–4 built and confirmed on live API
- Step 4 (tracker loop) delivered, 37/37 tests passing
- Tracker confirmed end-to-end: portfolio fetch working, 0 positions (no open trade yet)
---
## Where we are
### Active work: Step 5 — Signal Bridge
Next session starts here. Everything below Step 5 is done.
**Step 5 goal**: given a signal from a strategy YAML, open/close a demo trade on eToro.
**Files to create**:
- `src/broker_support/execution/order_router.py` — `OrderRouter` class
  - `open_position(signal) -> str` (positionId)
  - `close_position(position_id, instrument_id) -> bool`
- `tests/broker_support/test_order_router.py`
**Client stubs to implement** (currently `raise NotImplementedError`):
```python
# src/broker_support/client/client.py
def place_market_order(self, instrument_id, is_buy, amount, leverage=1,
                       stop_loss_rate=None, take_profit_rate=None) -> dict:
    # POST /api/v1/trading/execution/demo/market-open-orders/by-amount
    # Body PascalCase: { InstrumentID, IsBuy, Leverage, Amount }
    # StopLossRate / TakeProfitRate = absolute price levels (not distances)
def close_position(self, position_id, instrument_id, units_to_deduct=None) -> dict:
    # POST /api/v1/trading/execution/demo/market-close-orders/positions/{positionId}
    # Body: { "InstrumentId": ..., "UnitsToDeduct": null }  ← null = full close
```
**Prerequisite before implementing**: open a demo trade on eToro, then run:
```
python scripts/broker_support/inspect_portfolio.py
```
This confirms OpenPosition field names match the Pydantic model. `positions=0`
currently so field names are not yet empirically verified.
**Signal contract (TBD — agree at session start)**:
- Could be parsed from a strategy YAML `direction:` field
- Could be a simple dict `{instrument_id, direction, amount, leverage}`
- Keep it minimal for Step 5 — no complex queue needed yet
### Backtesting status (CLOSED — reference only)
- Phase 1 fully closed. Engine frozen. No further runs planned.
- Paper trade candidates finalised — see SKILL.md for table.
- Trading YAMLs in `outputs/backtesting/trading_yamls/b651ec5c_*.yaml`
---
## Key empirical findings (DO NOT re-derive)
### API endpoints (confirmed live)
| Endpoint | Status | Notes |
|----------|--------|-------|
| GET /trading/info/portfolio | ✅ 200 | Use this for portfolio. No /demo/ prefix. |
| GET /trading/info/real/pnl | ✅ 200 | Also works, same response |
| GET /trading/info/demo/pnl | ❌ 403 | Does not exist for any key type |
| GET /trading/info/demo/portfolio | ❌ 403 | Does not exist for any key type |
| GET /trading/info/trade/history | ✅ 200 | Requires Write key. minDate=YYYY-MM-DD |
| GET /market-data/search?searchText=... | ✅ 200 | Use searchText (not internalSymbolFull) |
| GET /market-data/instruments?instrumentIds=32 | ✅ 200 | DAX confirmed |
| GET /watchlists | ✅ 200 | Connection test |
### Key architecture decisions (already made, do not revisit)
- RESULT A confirmed: demo trades appear in `/trading/info/trade/history` (Write key required)
- PositionTracker uses snapshot diff (not direct query — was considered, RESULT A made it optional but snapshot diff retained as primary)
- TradeEnricher: searches history by positionId, up to 10 pages (1000 trades), 90-day lookback
- Trading hours guard: 08:00–22:00 Europe/Berlin (CET/CEST auto-handled via zoneinfo)
- Poll interval: 5 minutes (configurable via --interval flag)
- Log rotation: daily, 30-day retention, at `outputs/broker_support/logs/`
### Instrument map
```yaml
# configs/broker_support/instrument_map.yaml
instruments:
  DAX:
    instrument_id: 32
    symbol_full: GER40
```
---
## Test suite state
```
37/37 passing (2026-03-12, Python 3.13.12, Windows 10)

tests/broker_support/test_models.py          — 8 tests
tests/broker_support/test_csv_journal.py     — 7 tests
tests/broker_support/test_position_tracker.py — 11 tests
tests/broker_support/test_time_utils.py      — 11 tests
```
Run: `pytest tests/broker_support/ -v`
---
## Useful diagnostic commands
```powershell
# Single tracker cycle (force run outside hours)
python scripts/broker_support/run_tracker_loop.py --once --no-hours-guard
# Continuous loop
python scripts/broker_support/run_tracker_loop.py
# Inspect portfolio (useful once you have an open demo trade)
python scripts/broker_support/inspect_portfolio.py
# Inspect instrument
python scripts/broker_support/inspect_instruments.py
```
---
## Open issues
| ID | Description | Priority |
|----|-------------|----------|
| STEP-5 | Signal bridge / order router | P0 — next session |
| PORT-FIELDS | OpenPosition field names not empirically verified (positions=0) | P0 — need open trade |
| B9O-009 | V2 shared memory for backtester | Deferred to Phase 3 |
| WINZIP-32 | WinError 32 on GA temp YAMLs | Cosmetic, deferred to V2 |
---
## Candidate shortlist for paper trading (Phase 2)
When Step 5 is complete and manual demo testing passes, start paper trading with:
1. **c424a0e04327** (PRIMARY) — `b651ec5c_c424a0e04327_strategy.yaml`
2. **20745ca991be** (SECONDARY) — `b651ec5c_20745ca991be_strategy.yaml`
3. Monitor **c42f8b009283** and **c209820886c8** before promoting
Do NOT promote c209820886c8 above c42f8b009283 despite better MC profile —
it has a hard cliff on atr_multiplier (delta=-0.1621 at +1 step).