# BACKTESTER_USER_GUIDE.md
**Version**: 1.0 — Block 1 scope (parameter configuration)
**Audience**: Single operator (you)
---
## What the Backtester Controls vs. What You Control Manually
There are two config files. They serve completely different purposes and
you should never confuse them.
---
### backtest_template.yaml — What the Backtester Optimises
This file is the backtester's source of truth. Everything in it is either
directly tunable by the optimizer or controls how the optimizer behaves.
**The optimizer searches over these (zones → parameters):**
| Parameter group | Examples | Zone |
|---|---|---|
| RSI filter | rsi_period, rsi_overbought, rsi_oversold | safe, exploration |
| Bollinger filter | bollinger_length, bollinger_multiplier, bollinger_width_ma | safe, exploration |
| ATR / risk | atr_length, atr_multiplier, rr_target, risk_percentile | safe, exploration |
| ADX filter | adx_enabled, adx_length, adx_threshold | exploration only |
| Choppiness filter | choppiness_enabled, choppiness_length, choppiness_threshold | exploration only |
| Supertrend filter | supertrend_enabled, supertrend_atr_length, supertrend_factor | exploration only |
| CCI, MACD, MA, Pivot, DPO | all params + enabled flags | discovery (disabled) |
The `safe` zone uses fixed-on RSI + Bollinger with narrow ranges — the optimizer
only tunes numeric params. The `exploration` zone additionally lets the optimizer
toggle ADX, Choppiness, and Supertrend on/off as discrete parameters.
**You configure (not the optimizer):**
- `scenario` — which fitness objective applies (capital_accumulation, swing_trading, conservative)
- `random_search.samples_per_zone` — how wide the initial search is
- `walk_forward.windows` — the WFO date windows (calibrate for your data slice length)
- `monte_carlo.deep.iterations` — simulation depth
- `verdict_thresholds` inside the scenario — recalibrate after Block 5
- Which zones are `enabled: true/false`
---
### strategy_template.yaml — What You Control Manually
This file defines the strategy's base configuration. The backtester reads it
as a starting point and overlays candidate parameters on top during each evaluation.
You never edit this file for backtesting purposes — it is frozen as the baseline.
**What stays permanently under manual control (never optimised, v1):**
| Setting | Reason not optimised |
|---|---|
| `data.paths.*` | File paths, not numeric params. TF changes require a different file. |
| `data.htf_period` | Coupled to htf_ohlcv path — can't change one without the other. |
| `data.date_range` | Training window — set once, changed only when extending data. |
| `filters.time_filters.time_filter` | Session start/end are nested {hour, minute} dicts, not scalars. |
| `filters.filter_sequence` | 10! orderings, no gradient. v2+ feature. |
| `trade_management.spread` | Broker config — fixed for a given broker. |
| `trade_management.position_control` | Max positions, pyramiding — strategic decisions, not optimisable. |
| `trade_management.risk.tp_mode` | Discrete mode switch — interaction too high for v1. |
| `output.*` | Reporting settings — no effect on strategy performance. |
| `asset.*`, `execution.mode` | Fixed for this instrument and run mode. |
**What the backtester overlays (from _PARAM_KEY_MAP in strategy_runner.py):**
Every parameter in the zones maps to a dotted path in this file. When the
backtester evaluates a candidate, it copies strategy_template.yaml, applies
the candidate's parameter overrides at the mapped paths, and runs the strategy
on the result. After evaluation the temp file is deleted.
The mapping lives exclusively in `src/backtesting/strategy_runner.py` —
`_PARAM_KEY_MAP`. If you add a new strategy parameter that should be optimised,
it must be added to that dict AND to the appropriate zone in backtest_template.yaml.
---
## Setting Up a Backtest Run
1. **Set the scenario** — top of backtest_template.yaml: `scenario: capital_accumulation`
2. **Check zones** — confirm `safe: enabled: true`, `exploration: enabled: true`,
   `discovery: enabled: false`
3. **Verify WFO windows** match your data slice dates
4. **Run**: `python -m src.backtesting.orchestrator --config configs/backtesting/backtest_template.yaml`
5. **Outputs** in `outputs/backtesting/` — HTML report, JSON, Parquet, SQLite DB
---
## What Gets Decided Automatically
The backtester decides automatically:
- Which candidate parameter sets to explore (LHS sampling within zone ranges)
- Which candidates survive to GA, WFO, MC Deep, and Sensitivity stages
- The final verdict for each surviving candidate (auto_go / borderline / no_go)
The backtester never decides:
- Whether a candidate is approved for live trading (`LIVE_APPROVED` — operator only)
- Which scenario to use
- What data slice to run on