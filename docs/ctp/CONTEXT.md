# CONTEXT.md — CTP Session State
# Updated: 2026-03-17 (live signal followed by successful trade openning)
---
## Where We Are```
BROKER INTEGRATION:    Phase 2 pipeline LIVE as of 2026-03-13.
                       Signal loop running. First signals observed 2026-03-16 14:35 UTC.
                       Rejected by RiskManager — expected behaviour, not a bug.
                       Plan: run full week, review RiskManager calibration if pattern repeats.
                       Command: python scripts/broker_support/run_signal_loop.py
                                --config configs/broker_support/broker_support_config.yaml
                        2026-03-17 signal and trade to analyze and add
                        Finish development (all safe guared implemented) run for 1 week
```
---
## Phase 2 Deliverables
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
## Paper Trade Candidates (run b651ec5c — production reference)
| Priority | Candidate | WFO | Ruin | Status |
|----------|-----------|-----|------|--------|
| 1st | c424a0e04327 | 0.8108 | 0.000 | PRIMARY — active in run_signal.py |
| 2nd | 20745ca991be | 0.7201 | 0.054 | SECONDARY — after PRIMARY stable |
| Watch | c42f8b009283 | 0.6473 | 0.000 | MONITOR |
| Watch | c209820886c8 | 0.5699 | 0.000 | SECONDARY MONITOR — do NOT promote |
---
## Live Signal Loop — Status
```
First day:  2026-03-16, 09:00–16:00 UTC
Signal:     Poll #324, 14:35 UTC — BUY @ 23605.05 (bid)
            49 raw → 1 filter survivor (2% pass rate — consistent with backtest)
            REJECTED by RiskManager (threshold_pct=0.45)
            Likely cause: elevated ATR near US open 14:30 UTC
Backtest baseline (c424a0e04327, 38 months):
  ~4.6 filter signals/day, ~1.8 trades/day approved (~39% RiskManager pass rate)
Next steps:
  - Run full week before any conclusions on RiskManager calibration
  - If rejections cluster near 14:30 UTC → check whether 0.45% threshold needs
    recalibration for 2026 DAX volatility vs 2023–2024 backtest period
  - If random rejections across session → investigate ATR distribution
  - No code changes until pattern confirmed across 3–5 trading days
```
---
## Useful Commands
```powershell
# Live trading
python scripts/broker_support/run_signal_loop.py --config configs/broker_support/broker_support_config.yaml
python scripts/broker_support/run_signal.py --verbose
python scripts/broker_support/run_signal.py --place-order --verbose
python scripts/broker_support/inspect_portfolio.py
python scripts/broker_support/run_tracker_loop.py --once --no-hours-guard
# Diagnostics
python scripts/diagnostics/query_run.py
python scripts/diagnostics/diagnose_crash_candidate.py
python scripts/diagnostics/reproduce_crash.py
# Tests
pytest tests/broker_support/ -v
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
1. Check live signal loop: python scripts/broker_support/inspect_portfolio.py
2. If trade placed → confirm journal entry, review tracker loop output
3. If no trade → note time-of-day pattern on any RiskManager rejections
4. After 1 week of live signals → review RiskManager 0.45% calibration