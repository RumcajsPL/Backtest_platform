---
name: backtester-project
description: >
  Use this skill whenever working on the Backtesting & Optimization Framework project
  OR the broker_support / eToro API integration project. Triggers: any mention of
  backtester, backtest pipeline, CandidateStore, GA engine, WFO evaluator, Monte Carlo
  engine, fitness evaluator, scenario profile, backtest_template.yaml, sensitivity
  evaluator, verdict engine, report generator, any module from src/backtesting/,
  broker_support, EToroClient, PositionTracker, CSVJournal, paper trading automation,
  eToro API, signal bridge, CTP roadmap, WBWS+, time filter, hour filter,
  LiveDataFetcher, SignalBridge, OrderSignal, LiveConfigPatcher, BrokerSupportConfig,
  pyramiding, max_positions, _check_pyramiding.
  Read this SKILL.md before writing any code, creating any file, or making any design
  decision for this project.
---
# CTP Project Skill — Backtesting + Broker Integration

## Project Status (2026-03-13, Block 9P+4 end-of-session)
```
BACKTESTING ENGINE:    V1 PRODUCTION — PHASE 1 GATE FULLY CLOSED. Engine frozen.

BROKER INTEGRATION:    Steps 1-5 COMPLETE. 71/71 tests passing.
                       Documentation complete (API_REFERENCE.md + BROKER_INTEGRATION.md).

PHASE 2:               All files delivered and verified (v3 zip).
                       data_contracts.py verified — 3 bugs fixed in live_data_bundle.py.
                       All 4 trade constraints enforced — no open gaps.
                       Not yet tested on live API.
                       Next: copy files, run tests, run Stage 1 dry-run.
```

---
## ACTIVE TRACK — broker_support / Phase 2

### Project path
E:\Trading\Backtest_platform — package src/broker_support/ (editable install)

### Test suite
```
71/71 passing (pre-session baseline 2026-03-12)
+ 19 new tests in test_time_utils.py for is_valid_trading_window()
Expected after copy: 90/90
```
Run: pytest tests/broker_support/ -v

---
## Phase 2 Architecture (2026-03-13)

### Live pipeline flow
```
broker_support_config.yaml -> BrokerSupportConfig
    |
LiveConfigPatcher.load_and_patch() -> patched StrategyConfig
    |
LiveDataFetcher.fetch(symbol) -> (df_strategy, df_htf)    [eToro candles API]
    |
build_live_data_bundle(df_strategy, df_htf, artf_path) -> DataBundle
    |
SignalGenerator(patched_config).generate_signals(bundle) -> SignalFrame
    |
FilterPipeline(patched_config).apply_filters(...) -> FilterPipelineResult
    [strategy time_filter 08:30-20:30 CET runs here -- backtested, not WBWS+]
    |
Last-bar signal check -> SignalType or None
    |
RiskManager(patched_config, ohlcv_data=df_strategy, ohlcv_artf=bundle.artf)
    .compute_trade_parameters(last_ts, bid_price, is_long) -> TradeParameters
    [max_risk_percentile enforced here -- uses full ARTF parquet, not 500-bar window]
    |
is_valid_trading_window(last_ts, ...) -> WBWS+ gate [non-blocking, sets flag]
    |
OrderSignal(direction, sl, tp, max_positions, wbws_window_valid, ...)
    |
run_signal.py --place-order:
    _check_pyramiding() -> portfolio fetch -> count open DAX positions
    [enforces max_positions=1, pyramiding_enabled=False, close_on_opposite=False]
    |
OrderRouter.open_position()
```

### Key design decisions (locked -- do not revisit)
1. DataLoader bypassed entirely -- no parquet reads in live context except artf
2. TradeSimulator NOT called -- only last-bar signal + RiskManager needed
3. Strategy time_filter kept unchanged in patched config -- backtested params must not be altered
4. WBWS+ is a separate, non-blocking gate -- signals shown even outside window
5. artf path explicit in broker_support_config.yaml -- Principle 4 (explicit > implicit)
6. Sentinel pattern for strategy/htf paths -- artf path used as sentinel to pass StrategyConfig validation
7. max_positions comes from strategy YAML (backtested), not broker_support_config.yaml safety section

### New package structure (Phase 2)
```
src/broker_support/
  live/
    __init__.py                  <- (empty)
    live_data_fetcher.py         <- fetches candles from eToro API -> DataFrame
    live_config_patcher.py       <- patches strategy YAML dict for live context
    live_data_bundle.py          <- builds DataBundle from live DataFrames + artf
    order_signal.py              <- OrderSignal typed contract (incl. max_positions)
    signal_bridge.py             <- full pipeline -> OrderSignal
  config/
    broker_support_config.py     <- BrokerSupportConfig schema (frozen dataclasses)

configs/broker_support/
  broker_support_config.yaml     <- live execution config (single source of truth)
  instrument_map.yaml            <- DAX: {instrument_id: 32, symbol_full: GER40}
  broker_settings.env            <- ETORO_API_KEY, ETORO_USER_KEY

scripts/broker_support/
  run_signal.py                  <- Stage 1: dry-run | Stage 2: --place-order
  run_tracker_loop.py            <- existing polling loop
  inspect_portfolio.py           <- existing diagnostic
```

---
## Trade Constraint Enforcement (all four constraints closed -- no gaps)

| Constraint | Value | Enforced where | How |
|---|---|---|---|
| max_risk_percentile | 0.45 | RiskManager.compute_trade_parameters() | Uses full ARTF parquet for price normalisation -- same context as backtesting |
| pyramiding_enabled | false | _check_pyramiding() in run_signal.py | Portfolio fetch before OrderRouter -- aborts if open positions >= max_positions |
| max_positions | 1 | Same _check_pyramiding() | Source: strategy YAML (backtested constraint), NOT broker_support_config safety section |
| close_on_opposite | false | Emergent from pyramiding guard | Opposite signal while position open -> guard fires -> no new position opened |

_check_pyramiding() logic (run_signal.py, Stage 2 only):
```python
instrument_id = resolver.instrument_id(symbol)
portfolio = client._make_request("GET", "api/v1/trading/info/demo/portfolio")
positions = portfolio["clientPortfolio"]["positions"]
open_for_instrument = [p for p in positions if p["instrumentID"] == instrument_id]
if len(open_for_instrument) >= signal.max_positions:
    logger.info("ORDER SKIPPED: max_positions reached")
    sys.exit(0)  # clean exit -- not an error
```

signal.max_positions is read from strategy YAML position_control.max_positions via
signal_bridge.py -> OrderSignal.max_positions. Backtested value = 1.

---
## Empirically confirmed API facts (frozen -- overrule any docs that contradict)
```
KEY TYPE:               ETORO_USER_KEY = Demo Write key.
                        Real key -> 403 on ALL /demo/ endpoints.

Portfolio:              GET /api/v1/trading/info/demo/portfolio
                        Field: 'credit' (/portfolio) vs 'credits' (/pnl) -- do NOT mix.
                        Positions: PascalCase + capital ID. instrumentID (capital).

Candles endpoint:       GET /api/v1/market-data/instruments/{id}/history/candles/{dir}/{interval}/{count}
                        dir: 'asc' | 'desc'
                        interval: 'OneMinute' | 'FiveMinutes' | 'TenMinutes' | 'FifteenMinutes' |
                                  'ThirtyMinutes' | 'OneHour' | 'FourHours' | 'OneDay' | 'OneWeek'
                        count: max 1000
                        Response: { interval, candles: [{ instrumentId, candles: [{ instrumentID,
                                   fromDate, open, high, low, close, volume }] }] }
                        Casing quirk: outer instrumentId (lowercase), inner instrumentID (capital)
                        volume: always 0 for DAX -- keep for schema compat

Two-step open flow:     POST market-open-orders/by-amount -> orderForOpen.orderID
                        GET demo/orders/{orderID} poll until statusID==1 -> positions[0].positionID
                        positionID is NOT in the open-order response

Execution body:         PascalCase + capital ID: InstrumentID, IsBuy, Amount, Leverage
                        SL/TP: StopLossRate, TakeProfitRate (absolute price levels)
                        Close body: InstrumentID (capital) -- NOT InstrumentId (lowercase d)

Trade history:          GET /api/v1/trading/info/trade/history?minDate=YYYY-MM-DD
                        Requires Demo Write key. Demo trades appear here.

DAX:                    instrumentId=32, symbolFull="GER40", settlementTypeID=0 (CFD)
```

---
## WBWS+ Filter (implemented -- do not re-design)
```python
# Consistent across BOTH full history AND last 3 months:
allowed_hours_utc: [9, 10, 11, 12, 13, 14, 15, 16]   # London core + early NY
skip_hours_utc:    [17, 18]                            # Consistently negative

# Regime-dependent -- NOT filtered:
# Monday: unstable (monday_size_factor: 1.0 -- no reduction applied)
# Hour 10, 13: opposite sign across periods -- included, not skipped

# Implementation: src/broker_support/utils/time_utils.is_valid_trading_window()
# Config: configs/broker_support/broker_support_config.yaml -> trading_window section
# Gate: non-blocking in SignalBridge -- sets wbws_window_valid flag on OrderSignal
# Enforcement: run_signal.py blocks --place-order if wbws_window_valid=False
#              (unless --force-window passed for testing)
```

---
## broker_support_config.yaml structure
```yaml
strategy:
  yaml_path: outputs/backtesting/trading_yamls/b651ec5c_c424a0e04327_strategy.yaml

live_data:
  artf_ohlcv_path: data/processed/ohlcv/DEUIDXEUR_1ME_20210101_20260301.parquet
  strategy_bars_to_fetch: 500    # 1-min bars; max 1000
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
  symbol: DAX
  amount_usd: 60.0
  leverage: 20

safety:                  # Stage 4 (automation loop) only -- Stage 2 ignores these
  max_open_positions: 3  # NOT the pyramiding limit (that is strategy YAML max_positions=1)
  min_available_cash_usd: 200.0
  max_consecutive_losses: 5
  kill_switch_file: STOP
```

---
## LiveConfigPatcher -- what is patched vs kept
```
PATCHED (live context overrides):
  data.paths.strategy_ohlcv -> artf path (sentinel -- passes validation, never read)
  data.paths.htf_ohlcv      -> artf path (sentinel -- passes validation, never read)
  data.paths.artf_ohlcv     -> real path from bs_config.live_data.artf_ohlcv_path
  data.paths.ltf_ohlcv      -> None (LTF not fetched live)
  data.date_range           -> None (live frames used as-is, no date filtering)
  execution.mode            -> 'core' (no analytics overhead)
  output.reports.enabled    -> False

NOT PATCHED (backtested signal logic -- must stay as-is):
  filters.*                 -> all filter params + sequence (incl. time_filter)
  trade_management.*        -> SL/TP/ATR/spread/risk params (incl. max_risk_percentile)
  asset.*                   -> symbol, pip_size, point_size
  position_control.*        -> max_positions, pyramiding_enabled, close_on_opposite
```

---
## data_contracts.py -- verified facts (2026-03-13)
```python
# DataInfo.ltf_timeframe: str = "1s"   <- NOT Optional -- use "1s" when LTF not loaded
# DataBundle.config: Optional[DataConfig] = None  <- pass None in live context
# DataBundle.ltf: Optional[pd.DataFrame] = None   <- OK to pass None
# DataBundle.artf: Optional[pd.DataFrame] = None  <- OK to pass None
# DataFileConfig.__post_init__ validates file extension matches format string
#   -> do NOT use sentinel DataFileConfig -- pass config=None instead
# DataValidationResult: is_valid, checks, errors, warnings -- all present
# DataInfo.date_range: Optional[Tuple[datetime, datetime]]
```

---
## Stage sequence (Phase 2)
```
Stage 1: run_signal.py (no flags)          -- dry-run, print signal, no orders
Stage 2: run_signal.py --place-order       -- 1 trade, supervised, all guards active
Stage 3: 3-trade automation batch          -- after Stage 2 confirmed
Stage 4: full loop with abort conditions   -- after Stage 3 confirmed
```

### Stage 2 guard order (run_signal.py --place-order)
```
1. Config loaded + validated (BrokerSupportConfig.from_yaml)
2. Pipeline runs -> OrderSignal or None
3. WBWS+ gate   -> abort if wbws_window_valid=False (unless --force-window)
4. _check_pyramiding() -> portfolio fetch -> abort if open positions >= max_positions
5. OrderRouter.open_position() -> positionID
```

### Stage 4 abort conditions (broker_support_config.yaml safety section)
```python
max_open_positions    = 3       # halt loop if exceeded (portfolio-level, not per-instrument)
min_available_cash    = 200.0   # USD -- halt if below
max_consecutive_losses = 5      # halt streak
hours_guard           = True    # 08:00-22:00 CET (is_trading_hours())
trading_window_filter = True    # WBWS+ hour filter
kill_switch_file      = 'STOP'  # create this file in project root to halt loop
```

---
## What NOT to do
- Do NOT use /trading/info/portfolio (no /demo/) -- wrong for Demo key -> 403
- Do NOT use 'InstrumentId' (lowercase d) in close body -- must be 'InstrumentID'
- Do NOT call search without 'fields' param -- returns empty
- Do NOT assume positionID is in the open-order response -- poll order info
- Do NOT use 'from'/'fromDate' for trade history -- correct param is 'minDate'
- Do NOT refactor _make_request() -- solid, do not touch
- Do NOT confuse 'credit' (/demo/portfolio) with 'credits' (/demo/pnl)
- Do NOT set LIVE_APPROVED in code -- operator-only
- Do NOT call DataLoader in live context -- use LiveDataFetcher + build_live_data_bundle
- Do NOT call TradeSimulator in live context -- use last-bar signal + RiskManager only
- Do NOT modify strategy time_filter params in LiveConfigPatcher -- backtested values
- Do NOT modify position_control params in LiveConfigPatcher -- backtested values
- Do NOT hardcode allowed_hours_utc -- always read from BrokerSupportConfig.trading_window
- Do NOT use broker_support_config.yaml safety.max_open_positions as pyramiding limit
  -- use strategy YAML position_control.max_positions (carried in OrderSignal.max_positions)
- Do NOT use ltf_timeframe=None in DataInfo -- use "1s" (field is str, not Optional)
- Do NOT build a sentinel DataFileConfig -- pass DataBundle(config=None) in live context
- Do NOT use print() -- use logger.info/debug

---
## CLOSED TRACK -- Backtesting (reference only)

### Paper trade candidates (run b651ec5c)
| Rank | Candidate | Status | WFO | Ruin |
|------|-----------|--------|-----|------|
| 1 | c424a0e04327 | PRIMARY | 0.8108 | 0.000 |
| 2 | 20745ca991be | SECONDARY | 0.7201 | 0.054 |
| 3 | c42f8b009283 | MONITOR | 0.6473 | 0.000 |
| 4 | c209820886c8 | SECONDARY MONITOR | 0.5699 | 0.000 |

Do NOT promote c209820886c8 above c42f8b009283 -- hard cliff on atr_multiplier (+1 step).
Trading YAMLs: outputs/backtesting/trading_yamls/b651ec5c_<id>_strategy.yaml

Frozen constants:
```python
_SIGMOID_SCALE = 310.0        # NOT 359.4, NOT 221.1
_MAX_EXPECTED_DRAWDOWN = 2_500.0
max_workers = 2               # OOM at 6 -- mandatory
```

---
## Architecture Rules (non-negotiable)
```python
# Contracts: Pydantic models / frozen dataclasses -- never raw dicts across boundaries
# Fail fast: invalid config raises at construction, no silent fallbacks
# Datetime: datetime.now(timezone.utc) -- NEVER datetime.utcnow()
# Paths: pathlib.Path -- never hardcoded separators
# Logging: logger.info/debug only -- never print()
# Broker: _make_request() is the HTTP engine -- never implement HTTP in public methods
# Enrichment: TradeEnricher searches up to 10 pages (1000 trades, 90-day window)
# Trading hours: gate execution via is_trading_hours() -- 08:00-22:00 CET/CEST
# WBWS+ gate: is_valid_trading_window() -- separate from is_trading_hours()
# Live pipeline: DataLoader bypassed -- use LiveDataFetcher + build_live_data_bundle
# Pyramiding: _check_pyramiding() in run_signal.py -- portfolio fetch before OrderRouter
# Constraints: strategy YAML position_control values are authoritative, not safety section
```

## Platform
- OS: Windows 10, Python 3.13.12
- Timezone: OHLCV/signals CET/CEST; pipeline timestamps UTC
- Project: E:\Trading\Backtest_platform
- API base: https://public-api.etoro.com/api/v1
- Credentials: configs/broker_support/broker_settings.env
- CTP API reference: docs/ctp/API_REFERENCE.md
- Broker integration reference: docs/ctp/BROKER_INTEGRATION.md

## Session Deliverables (end of every session)
- Updated docs/ctp/CONTEXT.md
- Updated SKILL.md in outputs/ (replace user skill)
- docs/ctp/BROKER_INTEGRATION.md if API findings changed
