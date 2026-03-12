# CONTEXT — Block 9P+3 Start (2026-03-12)
## Session summary
- API documentation complete: `API_REFERENCE.md` (full reference) + slimmed `BROKER_INTEGRATION.md` created
- Both HTML performance reports analyzed: full history (1,498 trades) + last 3 months (90 trades)
- Time/day filter opportunity quantified — significant improvement possible
- Phase 2 plan refined: graduated automation loop with manual confirmation stages
- Strategy enhancement ("WBWS+") approach defined: hour/day filter layer
---
## Where we are
### Documentation: COMPLETE
- `docs/ctp/BROKER_INTEGRATION.md` — CTP paper trading reference (slimmed, project-focused)
- `docs/ctp/API_REFERENCE.md` — full eToro API reference (all endpoints, schemas, WebSocket)
### Phase 0 + Steps 1–5: COMPLETE
All broker_support infrastructure built, tested (71/71), confirmed on live API.
### Next session: Phase 2 — graduated automation + strategy filter layer
---
## Phase 2 Plan (refined 2026-03-12)
### Graduated automation stages
**Stage 1 — Signal validation (manual)**
- Run signal generator, print output, manually review
- No trades placed
**Stage 2 — Single trade cycle (1 order, supervised)**
- `run_signal.py`: open 1 position manually via OrderRouter
- Confirm positionID in portfolio
- Let tracker detect it
- Close manually, confirm journal entry
- Full review before proceeding
**Stage 3 — 3-trade batch (supervised)**
- Let automation place 3 orders
- Same analysis cycle
- Confirm journal, P&L, enrichment all correct
**Stage 4 — Loop with abort mechanism**
- Full automation loop
- Embedded abort conditions (see below)
- Monitor live; can always reset demo account if needed
### Abort/safety conditions for automation loop
- Max open positions exceeded (e.g. > 3 simultaneous)
- Available cash below threshold (e.g. < $200)
- Consecutive loss streak (e.g. 5 in a row)
- Hours guard: 08:00–22:00 CET only (already in time_utils)
- Time/day filter: WBWS+ filter (see below)
- Manual kill switch: env flag or file-based stop signal
---
## Strategy Enhancement — WBWS+ Filter Layer
### Analysis summary (from 2026-03-12 reports)
**Full history (1,498 trades, Jan 2023 – Mar 2026):**
- Total P&L: +1,776.9 pts | Win rate: 11.88% | Profit factor: 1.08 | Grade: F/15
- London session: +1,320.8 pts (74% of total)
- Risk/reward: 7.99x average win/loss — excellent
- Critical weakness: 98% of losses are large (>7 pts), heavy-tailed distribution
- Max drawdown: -1,745.9 pts — nearly equal to total profit
**Last 3 months (90 trades, Dec 2025 – Feb 2026):**
- Total P&L: +654.1 pts | Win rate: 14.4% | Profit factor: 1.41 | Grade: D-/20
- London session: +521.3 pts (80% of total)
- Risk/reward: 8.34x — slightly improved
- Expectancy per trade: +7.27 pts vs +1.19 pts full history → recent environment better
- Max drawdown: -860.2 pts (still large relative to gains)
**Key divergence between periods — regime-dependent behaviour:**
| Factor | Full history | Last 3 months | Implication |
|--------|-------------|---------------|-------------|
| Monday | -492.7 pts | +287.4 pts | Unstable — do not filter hard |
| Hour 10 UTC | -107.1 pts | +221.7 pts | Unstable — monitor |
| Hour 13 UTC | +79.0 pts | -282.0 pts | Unstable — recent turned bad |
| Hour 11 UTC | +745.6 pts | +202.0 pts | Consistent positive both periods |
| Hour 14 UTC | +269.5 pts | +278.7 pts | Strong and consistent |
| Hour 16 UTC | +840.9 pts | +116.0 pts | Consistent positive |
| Wednesday | +179.2 pts | -212.6 pts | Unstable |
| London session | 74% of profit | 80% of profit | Very consistent |
### Filter recommendation: Conservative WBWS+ filter
**Only apply filters that are consistent across BOTH periods:**
| Filter | Rationale |
|--------|-----------|
| ✅ Skip hour 17–18 UTC | Negative in both: -184.5 and -547.8 full history; -183.9 and +96.2 recent (17 consistent bad) |
| ✅ Trade London session preference (08–16 UTC) | 74–80% of profit in both periods |
| ⚠️ Skip Monday | Only reliable in full history; recent inverted — use reduced size, not full skip |
| ❌ Skip hour 10, 13 | Regime-dependent — contradicts across periods |
**Proposed WBWS+ filter (Phase 2 implementation):**
```
Active trading hours: 09:00–16:00 UTC (London core + early NY overlap)
Skip hour 17:00 UTC (consistently negative both periods)
Day weighting: no hard day filter (Monday divergence too strong)
Optional: reduce position size on Wednesday (recent -212 pts)
```
**Expected improvement (conservative, based on full history):**
- Skip hours 17–18 (622 trades removed): saves ~+730 pts of losses
- Concentrate 09–16 UTC: 73% of trades, estimated 85%+ of profit
- Net effect: higher profit factor, lower drawdown, more trades skipped on bad signal hours
### Implementation approach
- Add `trading_hours_filter` to strategy YAML or `broker_settings.env`
- `OrderRouter.open_position()` checks `is_valid_trading_window(signal_time)` before placing
- Filter logic in `utils/time_utils.py` (already has `is_trading_hours()`)
- Parameterise: `allowed_hours: [9,10,11,12,13,14,15,16]` in strategy config
---
## Key empirical findings (locked)
| Endpoint | Status | Notes |
|----------|--------|-------|
| GET /trading/info/demo/portfolio | ✅ 200 | Demo Write key required |
| GET /trading/info/demo/orders/{id} | ✅ docs | positionID resolution |
| GET /trading/info/real/pnl | ✅ 200 | Real Write key |
| GET /trading/info/trade/history | ✅ 200 | Write key; minDate=YYYY-MM-DD |
| GET /market-data/search?fields=... | ✅ 200 | fields param REQUIRED |
| GET /market-data/instruments?instrumentIds=32 | ✅ 200 | DAX confirmed |
**Live position confirmed (2026-03-12):**
- positionID=3464232739, instrumentID=32, isBuy=false, openRate=23556.77
- All 34 portfolio field names confirmed matching OpenPosition model
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
| PHASE-2-STAGE1 | Write run_signal.py, validate signal output | P0 — next session |
| PHASE-2-STAGE2 | First live demo trade: open→track→close cycle | P0 |
| WBWS-FILTER | Implement hour filter in time_utils + OrderRouter | P1 — after Stage 2 confirmed |
| RESOLVER-FIELDS | InstrumentResolver missing 'fields' param + exact-match | P1 |
| B9O-009 | V2 shared memory for backtester | Deferred Phase 3 |
| WINZIP-32 | WinError 32 on GA temp YAMLs | Cosmetic |
---
## Useful commands
```powershell
pytest tests/broker_support/ -v
python scripts/broker_support/inspect_portfolio.py
python scripts/broker_support/run_tracker_loop.py --once --no-hours-guard
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