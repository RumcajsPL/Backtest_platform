# PROJECT_SKILL.md — CTP Technical Reference
# How to work on this project. Architecture rules, frozen constants, patterns.
# Updated: 2026-03-13
---
## Architecture Rules (non-negotiable)
```python
# Contracts:   Pydantic models / frozen dataclasses — never raw dicts across boundaries
# Fail fast:   invalid config raises at construction, no silent fallbacks
# Datetime:    datetime.now(timezone.utc) — NEVER datetime.utcnow()
# Paths:       pathlib.Path — never hardcoded separators
# Logging:     logger.info/debug only — never print()
# HTTP:        _make_request() is the engine — never implement HTTP in public methods
# Live data:   DataLoader bypassed — use LiveDataFetcher + build_live_data_bundle
# Pyramiding:  _check_pyramiding() in run_signal.py — portfolio fetch before OrderRouter
# Constraints: strategy YAML position_control values are authoritative, not safety section
# WBWS+ gate:  is_valid_trading_window() — non-blocking, sets flag only
# Time filter: strategy time_filter params never patched — backtested values must not change
```
---
## Frozen Constants (do not change)
```python
_SIGMOID_SCALE         = 310.0        # NOT 359.4, NOT 221.1
_MAX_EXPECTED_DRAWDOWN = 2_500.0
max_workers            = 2            # OOM at 6 — mandatory
```
---
## data_contracts.py — Verified Facts
```python
DataInfo.ltf_timeframe: str = "1s"        # NOT Optional — use "1s" when LTF not loaded
DataBundle.config: Optional[DataConfig] = None  # pass None in live context
DataBundle.ltf:   Optional[pd.DataFrame] = None  # OK to pass None
DataBundle.artf:  Optional[pd.DataFrame] = None  # OK to pass None
# DataFileConfig.__post_init__ validates file extension — do NOT use sentinel DataFileConfig
# DataValidationResult: is_valid, checks, errors, warnings — all present
```
---
## LiveConfigPatcher — What Is Patched vs Kept
```
PATCHED (live context overrides):
  data.paths.strategy_ohlcv → artf path (sentinel — passes validation, never read)
  data.paths.htf_ohlcv      → artf path (sentinel)
  data.paths.artf_ohlcv     → real path from bs_config.live_data.artf_ohlcv_path
  data.paths.ltf_ohlcv      → None
  data.date_range           → None
  execution.mode            → 'core'
  output.reports.enabled    → False
NOT PATCHED (backtested signal logic — must stay as-is):
  filters.*                 → all filter params + sequence (incl. time_filter)
  trade_management.*        → SL/TP/ATR/spread/risk params (incl. max_risk_percentile)
  asset.*                   → symbol, pip_size, point_size
  position_control.*        → max_positions, pyramiding_enabled, close_on_opposite
```
---
## _check_pyramiding() Logic
```python
# run_signal.py Stage 2 only — never in Stage 1 dry-run
instrument_id = resolver.instrument_id(symbol)
portfolio = client._make_request("GET", "api/v1/trading/info/demo/portfolio")
positions = portfolio["clientPortfolio"]["positions"]
open_for_instrument = [p for p in positions if p["instrumentID"] == instrument_id]
if len(open_for_instrument) >= signal.max_positions:
    sys.exit(0)  # clean exit — not an error
```
`signal.max_positions` = strategy YAML `position_control.max_positions` = 1 (backtested).
---
## Live Pipeline Flow
```
BrokerSupportConfig.from_yaml()
    → LiveConfigPatcher.load_and_patch() → patched StrategyConfig
    → LiveDataFetcher.fetch(symbol) → (df_strategy, df_htf)
    → build_live_data_bundle(...) → DataBundle [artf = full historical parquet]
    → SignalGenerator → FilterPipeline [strategy time_filter 08:30–20:30 CET]
    → Last-bar signal check
    → RiskManager [max_risk_percentile enforced using ARTF]
    → WBWS+ gate [non-blocking — sets wbws_window_valid flag]
    → OrderSignal(direction, sl, tp, max_positions=1, ...)
    [Stage 2 only]:
    → _check_pyramiding() → GET /demo/portfolio → abort if >= max_positions
    → OrderRouter.open_position() → positionID
```
---
## broker_support_config.yaml Structure
```yaml
strategy:
  yaml_path: outputs/backtesting/trading_yamls/b651ec5c_c424a0e04327_strategy.yaml
live_data:
  artf_ohlcv_path: data/processed/ohlcv/DEUIDXEUR_1ME_20210101_20260301.parquet
  strategy_bars_to_fetch: 500    # 1-min bars; API max 1000
  htf_bars_to_fetch: 120         # 1H bars
  strategy_interval: OneMinute
  htf_interval: OneHour
  candle_direction: desc         # newest first, reversed before pipeline
trading_window:
  enabled: true
  allowed_hours_utc: [9, 10, 11, 12, 13, 14, 15, 16]
  skip_hours_utc: [17, 18]
  monday_size_factor: 1.0
execution:
  instrument_map_path: configs/broker_support/instrument_map.yaml
  symbol: GER40                  # must match instrument_map.yaml key exactly
  amount_usd: 60.0
  leverage: 20
safety:                          # Stage 4 only — Stage 2 ignores
  max_open_positions: 3          # NOT the pyramiding limit (strategy YAML = 1)
  min_available_cash_usd: 200.0
  max_consecutive_losses: 5
  kill_switch_file: STOP
```
---
## What NOT To Do
```
# API
- Do NOT call /demo/ endpoints with Real key → 403
- Do NOT omit 'fields' param on market-data/search → empty results
- Do NOT use 'from'/'fromDate' for trade history → use minDate=YYYY-MM-DD
- Do NOT use Read-only key for trade/history → 403
- Do NOT assume positionID is in open-order response → poll order info
- Do NOT send 'InstrumentId' (lowercase d) in close body → must be 'InstrumentID'
- Do NOT confuse 'credit' (/portfolio) with 'credits' (/pnl)
- Do NOT use bar.get("field", 0.0) for OHLC parsing → value can be None even when key exists
# Architecture
- Do NOT refactor _make_request() — solid, do not touch
- Do NOT set LIVE_APPROVED in code — operator-only
- Do NOT call DataLoader in live context
- Do NOT call TradeSimulator in live context
- Do NOT modify strategy time_filter in LiveConfigPatcher
- Do NOT modify position_control in LiveConfigPatcher
- Do NOT use broker_support_config.yaml safety.max_open_positions as pyramiding limit
- Do NOT hardcode allowed_hours_utc — always read from BrokerSupportConfig
- Do NOT use ltf_timeframe=None in DataInfo — use "1s"
- Do NOT build a sentinel DataFileConfig — pass DataBundle(config=None)
- Do NOT use datetime.utcnow() — use datetime.now(timezone.utc)
```
---
## Platform
```
OS:          Windows 10, Python 3.13.12
Timezone:    OHLCV/signals CET/CEST; pipeline timestamps UTC
Project:     E:\Trading\Backtest_platform
API base:    https://public-api.etoro.com/api/v1
Credentials: configs/broker_support/broker_settings.env (Demo Write key)
```
---
## Stage Sequence
```
Stage 1: run_signal.py (no flags)           — dry-run, no orders      ✅ CONFIRMED
Stage 2: run_signal_loop.py                 — polls 60s, 1 order, stops  🔄 ACTIVE
Stage 3: 3-trade automation batch           — after Stage 2 confirmed
Stage 4: full loop with abort conditions    — after Stage 3 confirmed
```