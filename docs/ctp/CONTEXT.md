# CONTEXT.md — CTP Session State
# Updated: 2026-03-24 (parallel loop support — 4 instances)
---
## Where We Are
```
BROKER INTEGRATION:    Phase 2 COMPLETE as of 2026-03-18.
CTP ISOLATION FIX:     2026-03-19 — loop halted due to external account activity
                       interfering with drawdown guard. Fix implemented and delivered.
PARALLEL LOOPS:        2026-03-24 — 4-instance parallel paper trading implemented.
                       run_signal_loop.py extended with --instance flag.
                       Three new config files created for new candidates.
First live trade:      2026-03-17 13:06 UTC — BUY GER40 @ 23705.89
                       positionID=3466009287, orderID=336588020
                       SL=23676.47, TP=23891.07, R:R=8.8x
                       Status: successful (very profitable per operator note)
Loop halt (incident):  2026-03-19 10:02 UTC — Poll #4
                       session_open=4031.20, current=3056.43, drawdown=24.18%
                       Root cause: manual trades on same demo account depleted
                       credit before CTP placed any orders. Not a CTP bug.
                       Fix: drawdown now CTP journal-scoped only.
Next action:           Deploy all files below and start 4 loops in separate terminals.
```
---
## 4 Active Instances
| Instance | TF | Candidate | Strategy YAML | Config |
|---|---|---|---|---|
| `c424` | 1-min | c424a0e04327 | outputs/backtesting/trading_yamls/b651ec5c_c424a0e04327_strategy.yaml | broker_support_config.yaml |
| `240166` | 10-min | 240166da287e | configs/backtesting/candidates/822f1889_240166da287e_strategy.yaml | broker_support_config_240166.yaml |
| `7ffbc5` | 5-min | 7ffbc5e3522c | configs/backtesting/candidates/b8b6f21a_7ffbc5e3522c_strategy.yaml | broker_support_config_7ffbc5.yaml |
| `61875` | 1-min | 61875464b3aa | configs/backtesting/candidates/cd67ceb0_61875464b3aa_strategy.yaml | broker_support_config_61875.yaml |
Note on `61875`: htf_period: 1min — HTF equals strategy TF. Fetcher makes two
identical OneMinute calls. Harmless — DataBundle handles both correctly.
---
## Parallel Loop Isolation Design (implemented 2026-03-24)
```
Each instance is identified by --instance <id>.
All file paths are derived from the instance ID:
  Journal dir   : outputs/broker_support/journal/<id>/
  trades.csv    : outputs/broker_support/journal/<id>/trades.csv
  open_positions: outputs/broker_support/journal/<id>/open_positions.json
  Log file      : outputs/broker_support/logs/run_signal_loop_<id>_YYYY-MM-DD.log
  Kill switch   : STOP_<id> (per-instance) + STOP (master — halts ALL loops)
Config auto-resolution from instance ID (no --config flag needed):
  c424    → configs/broker_support/broker_support_config.yaml
  240166  → configs/broker_support/broker_support_config_240166.yaml
  7ffbc5  → configs/broker_support/broker_support_config_7ffbc5.yaml
  61875   → configs/broker_support/broker_support_config_61875.yaml
Each instance has fully independent:
  - Circuit breakers (drawdown, consecutive losses, pipeline errors)
  - CTP position tracking (open_positions.json)
  - Journal (trades.csv)
  - WBWS+ trading window (per-candidate backtest analysis)
  - Kill switch
Shared (read-only — no conflict):
  - EToroClient (stateless HTTP)
  - instrument_map.yaml
  - broker_settings.env
  - artf parquet (DEUIDXEUR_1ME_20210101_20260301.parquet)
  - broker_spreads.yaml
```
---
## Files Deployed 2026-03-24
```
scripts/broker_support/run_signal_loop.py             ← --instance flag, master kill switch
configs/broker_support/broker_support_config_240166.yaml   ← NEW
configs/broker_support/broker_support_config_7ffbc5.yaml   ← NEW
configs/broker_support/broker_support_config_61875.yaml    ← NEW
```
---
## CTP Isolation Design (implemented 2026-03-19, unchanged)
```
1. Drawdown guard — journal-scoped:
   - check_daily_drawdown(ctp_realised_pnl_today: float)
   - ctp_realised_pnl_today = sum(profit_loss) from instance trades.csv for today (UTC)
   - drawdown_pct = max(0, -ctp_realised_pnl_today / session_open_credit * 100)
   - Account-wide credit comparison REMOVED entirely
2. Pyramiding guard — CTP position set:
   - _check_pyramiding() counts only positionIDs in ctp_open_position_ids
   - External positions on same instrument logged but ignored
   - ctp_open_position_ids per-instance, persisted to instance open_positions.json
3. open_positions.json schema (per-instance):
   {"position_ids": [3466009287, ...]}
```
---
## Phase 2 Deliverables (complete 2026-03-18)
```
configs/broker_support/broker_support_config.yaml
src/broker_support/config/broker_support_config.py
src/broker_support/utils/time_utils.py
src/broker_support/live/__init__.py
src/broker_support/live/live_data_fetcher.py
src/broker_support/live/live_config_patcher.py
src/broker_support/live/live_data_bundle.py
src/broker_support/live/order_signal.py
src/broker_support/live/signal_bridge.py
src/broker_support/execution/order_router.py   ← fast-fill fix
src/broker_support/safeguards/__init__.py
src/broker_support/safeguards/paper_trading_guard.py
scripts/broker_support/run_signal.py
scripts/broker_support/run_signal_loop.py
tests/broker_support/test_time_utils.py
tests/broker_support/test_signal_pipeline_integration.py
```
---
## Test Status
```
90/90  unit tests passing
63/63  integration tests passing
Note:  PaperTradingGuard, order_router fast-fill path, CTP isolation,
       parallel loop paths not yet covered — all in test backlog for V2.
```
---
## Trade Constraint Status (all instances)
| Constraint | Value | Enforced where |
|---|---|---|
| max_risk_percentile | per YAML | RiskManager — uses full ARTF parquet |
| pyramiding_enabled | false | _check_pyramiding() — CTP positionIDs only |
| max_positions | 1 | Same guard — source: strategy YAML |
| close_on_opposite | false | Emergent from pyramiding guard |
| max_consecutive_losses | 3 | PaperTradingGuard — hard_stop |
| max_daily_drawdown_pct | 5.0% | PaperTradingGuard — CTP journal-scoped |
| min_available_cash_usd | 200.0 | PaperTradingGuard — account-wide (capital floor) |
| max_pipeline_errors | 5 | PaperTradingGuard — hard_stop |
| kill_switch_file | STOP_<id> | PaperTradingGuard — per-instance |
| master kill switch | STOP | run_signal_loop.py — halts all instances |
| off-hours gate | allowed_hours_utc | Loop — per-instance config |
---
## Paper Trade Candidates
| Priority | Instance | Candidate | WFO | Windows | Ruin | Status |
|---|---|---|---|---|---|---|
| Active | c424 | c424a0e04327 | 0.8108 | — | 0.000 | PRIMARY — running |
| New | 240166 | 240166da287e | 0.8886 | 4 | 0.000 | NEW — deploy |
| New | 7ffbc5 | 7ffbc5e3522c | 0.6869 | 9 | 0.000 | NEW — deploy |
| New | 61875 | 61875464b3aa | 0.7731 | 8 | 0.000 | NEW — deploy |
| Watch | — | 65df7121489f | 0.7941 | 8 | 0.000 | HOLD — promote if slot opens |
| Watch | — | 2a891e2cce6c | 0.8058 | 9 | 0.000 | HOLD — promote if slot opens |
---
## Useful Commands
```powershell
# Start all 4 instances (one terminal each)
python scripts/broker_support/run_signal_loop.py --instance c424
python scripts/broker_support/run_signal_loop.py --instance 240166
python scripts/broker_support/run_signal_loop.py --instance 7ffbc5
python scripts/broker_support/run_signal_loop.py --instance 61875
# Quiet mode (background terminals)
python scripts/broker_support/run_signal_loop.py --instance c424 --quiet
python scripts/broker_support/run_signal_loop.py --instance 240166 --quiet
python scripts/broker_support/run_signal_loop.py --instance 7ffbc5 --quiet
python scripts/broker_support/run_signal_loop.py --instance 61875 --quiet
# Kill switch — halt one instance
echo "" > STOP_240166     # halt 240166 only
del STOP_240166           # resume (restart manually)
# Kill switch — halt ALL instances
echo "" > STOP            # master kill
del STOP                  # resume all (restart each manually)
# Diagnostics
python scripts/broker_support/inspect_portfolio.py
python scripts/broker_support/run_tracker_loop.py --once --no-hours-guard
# Tests
pytest tests/broker_support/ -v
```
---
## Key Paths
```
Configs:
  configs/broker_support/broker_support_config.yaml          ← c424
  configs/broker_support/broker_support_config_240166.yaml   ← 240166
  configs/broker_support/broker_support_config_7ffbc5.yaml   ← 7ffbc5
  configs/broker_support/broker_support_config_61875.yaml    ← 61875
Strategy YAMLs:
  outputs/backtesting/trading_yamls/b651ec5c_c424a0e04327_strategy.yaml
  configs/backtesting/candidates/822f1889_240166da287e_strategy.yaml
  configs/backtesting/candidates/b8b6f21a_7ffbc5e3522c_strategy.yaml
  configs/backtesting/candidates/cd67ceb0_61875464b3aa_strategy.yaml
ARTF parquet:        data/processed/ohlcv/DEUIDXEUR_1ME_20210101_20260301.parquet
Instrument map:      configs/broker_support/instrument_map.yaml
Credentials:         configs/broker_support/broker_settings.env
Journals (closed):
  outputs/broker_support/journal/c424/trades.csv
  outputs/broker_support/journal/240166/trades.csv
  outputs/broker_support/journal/7ffbc5/trades.csv
  outputs/broker_support/journal/61875/trades.csv
Open positions:
  outputs/broker_support/journal/c424/open_positions.json
  outputs/broker_support/journal/240166/open_positions.json
  outputs/broker_support/journal/7ffbc5/open_positions.json
  outputs/broker_support/journal/61875/open_positions.json
Logs:
  outputs/broker_support/logs/run_signal_loop_c424_YYYY-MM-DD.log
  outputs/broker_support/logs/run_signal_loop_240166_YYYY-MM-DD.log
  outputs/broker_support/logs/run_signal_loop_7ffbc5_YYYY-MM-DD.log
  outputs/broker_support/logs/run_signal_loop_61875_YYYY-MM-DD.log
```
---
## V2 Backlog (do not act until paper trading week(s) complete)
```
- Tests: PaperTradingGuard new drawdown logic + CTP isolation
- Tests: order_router fast-fill path
- Tests: parallel loop path isolation
- run_tracker_loop.py → remove closed positionID from instance open_positions.json
  on close detection (currently stale entries are harmless)
- run_tracker_loop.py → call guard.record_trade_result() on close detection
- daily_order_cap safeguard
- SafetyConfig evolution — V2 discussion
- Promote 65df7121489f or 2a891e2cce6c after current 4 instances evaluated
- Review RiskManager 0.45% calibration after 1 full week of c424 data
```
---
## Next Session Start
1. python scripts/broker_support/inspect_portfolio.py — check open positions
2. Review logs for each instance:
   outputs/broker_support/logs/run_signal_loop_<id>_YYYY-MM-DD.log
3. Count per instance: signals found, orders placed, circuit breakers fired
4. After 1 full week: compare live signal frequency vs backtest baseline per candidate
5. After 2 full weeks: review RiskManager thresholds per candidate
6. If any circuit breaker fired: diagnose before restarting that instance