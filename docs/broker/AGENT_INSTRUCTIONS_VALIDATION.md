# AGENT INSTRUCTIONS — Live Validation Track A + Track B
# Authored by: Claude.ai | Date: 2026-04-19
# Relay verbatim to agents. Do not paraphrase.
# Reference: docs/broker/LIVE_VALIDATION.md
---

## INSTRUCTION A1
## Agent B (Codex) — Price Alignment Diagnostic Script

```
== INSTRUCTION ==
Agent:        B (Codex)
Environment:  Sandbox (write), Production (read — data files only)
Task:         Write scripts/diagnostics/price_alignment_check.py — a standalone
              diagnostic that fetches broker candles for GER40 and compares them
              to matching bars in the historical 10-min parquet, producing a
              structured alignment report.

Context:
  We need to know whether the broker candle endpoint returns bid prices
  (consistent with our historical OHLCV) or mid/ask prices (which would
  introduce a systematic offset). The overlap window where we have both
  live trading logs and historical data is 2026-03-17 to 2026-04-01.

  Historical parquet:
    data/processed/ohlcv/DEUIDXEUR_10min_20221201_20260401.parquet
  Broker instrument: GER40, instrument_id=32
  Broker interval string: "TenMinutes"

Files to read before starting:
  1. src/broker_support/client/client.py
     — to understand how EToroClient is instantiated and how
       fetch candle endpoint is called (do NOT reimplement HTTP)
  2. src/broker_support/live/live_data_fetcher.py
     — to see how broker candle response is parsed into a DataFrame
       and how OHLCV values are extracted (bar.get("f") or 0.0 pattern)
  3. src/broker_support/config/settings.py
     — to understand how settings (API keys) are loaded

Scope:
  Write ONE new file: scripts/diagnostics/price_alignment_check.py
  Do NOT modify any existing file.
  Do NOT implement HTTP directly — use EToroClient._make_request() via
  the existing public interface only.
  Do NOT use LiveDataFetcher — call EToroClient candle endpoint directly
  so we control the exact date range fetched.

Architecture constraints:
  - Use logger.info/debug — never print()
  - Use pathlib.Path — never hardcoded separators
  - Use datetime.now(timezone.utc) — never datetime.utcnow()
  - Fail fast if parquet file not found or API call fails — no silent fallbacks
  - settings instance imported from src.broker_support.config.settings

Script behaviour:
  1. Load historical 10-min parquet into DataFrame.
     Lowercase column names. Parse index as DatetimeIndex UTC tz-naive.
     Filter to rows where index >= 2026-03-17 00:00 and <= 2026-04-01 23:59.

  2. Fetch broker candles for GER40 TenMinutes covering 2026-03-17 to 2026-04-01
     using EToroClient. The overlap window is ~1260 bars — exceeds the 1000-bar
     single-request limit. Issue TWO requests:
       Request 1: direction=desc, count=1000 → covers most recent ~1000 bars
       Request 2: direction=desc, count=1000 with an earlier anchor if the API
                  supports a date offset param, OR fetch direction=asc from
                  2026-03-17 count=1000 to cover the earlier portion.
     Merge both result sets, deduplicate on timestamp, sort ascending.
     Parse each response using same logic as live_data_fetcher.py
     (inner candles array, bar.get("o") or 0.0 etc., reverse desc to ascending).
     Filter merged result to date range 2026-03-17 00:00 to 2026-04-01 23:59.

  3. Align DataFrames on timestamp index. Inner join — only matched timestamps.
     Log count of matched vs unmatched bars.

  4. Compute per-bar deviations (broker minus historical) for close, high, low.

  5. Produce statistics for each field (close, high, low):
       mean_deviation, std_deviation, max_abs_deviation, p95_abs_deviation
     Compute bias_direction:
       if abs(mean_deviation_close) < 0.3:  NEUTRAL
       elif mean_deviation_close > 0:        POSITIVE_BIAS
       else:                                 NEGATIVE_BIAS
     Compute spread_fraction = mean_deviation_close / 1.5
     (1.5 pts is a reasonable GER40 spread estimate for context)

  6. Write report to:
       outputs/broker_support/diagnostics/price_alignment_<YYYY-MM-DD>.txt
     Report format (plain text, one section per line group):

     === CTP Price Alignment Check ===
     Run date:       <datetime UTC>
     Overlap window: 2026-03-17 to 2026-04-01
     Instrument:     GER40 (id=32), TenMinutes

     --- Bar Coverage ---
     Historical bars in window:  <N>
     Broker bars in window:      <N>
     Matched bars (inner join):  <N>
     Unmatched historical:       <N>
     Unmatched broker:           <N>

     --- Close Deviation (broker minus historical) ---
     Mean:    <+/-X.XX> pts
     Std:     <X.XX> pts
     Max abs: <X.XX> pts
     P95 abs: <X.XX> pts

     --- High Deviation ---
     (same format)

     --- Low Deviation ---
     (same format)

     --- Summary ---
     Bias direction: <NEUTRAL / POSITIVE_BIAS / NEGATIVE_BIAS>
     Mean close deviation as fraction of est. spread (1.5pts): <X.XX>
     Conclusion: <ALIGNED / INVESTIGATE / MISALIGNED>
       ALIGNED     if abs(mean_close_dev) < 0.5
       INVESTIGATE if 0.5 <= abs(mean_close_dev) < 2.0
       MISALIGNED  if abs(mean_close_dev) >= 2.0 or mean_close_dev < 0
     One-line rationale: <text>

  7. Also log the full report content to logger.info so it appears in console.

Acceptance criteria:
  - Script runs without error from project root:
    python scripts/diagnostics/price_alignment_check.py
  - Output file created in outputs/broker_support/diagnostics/
  - Report contains all sections listed above
  - No HTTP implemented outside EToroClient
  - No print() statements
  - No hardcoded path separators

Output format: Complete file. No diff — new file only.
== END INSTRUCTION ==
```

---

## INSTRUCTION B1
## Agent C (Qwen Code 3.6) — Signal Log Extractor

```
== INSTRUCTION ==
Agent:        C (Qwen Code 3.6)
Environment:  Production (read-only)
Task:         Write scripts/diagnostics/signal_log_extractor.py — a standalone
              script that parses all existing demo_trading_* log files and
              extracts a structured CSV of every pipeline result (signal generated,
              risk rejected, no signal), with key parameters.

Context:
  The existing demo_trading logs contain per-poll pipeline outcomes logged by
  SignalBridge. We need to extract these into a structured dataset to compare
  live signal behaviour against backtest expectations.

  Log files location: outputs/broker_support/logs/demo_trading_*.log
  Log format: loguru — each line is:
    YYYY-MM-DD HH:MM:SS | LEVEL | module:line | message

  Key log lines to extract (from signal_bridge.py):

  a) Last bar timestamp (logged on every poll that reaches stage 5):
     "SignalBridge: no signal on last bar (TIMESTAMP)"
     "SignalBridge: signal found at last bar — DIRECTION @ TIMESTAMP, bid=PRICE"

  b) Signal result:
     "SignalBridge: result=NO_SIGNAL"
     "SignalBridge: result=RISK_REJECTED"
     OrderSignal summary line (logged by signal.summary() at end):
     "SignalBridge: BUY GER40 @ ..." or "SignalBridge: SELL GER40 @ ..."
     — this line contains entry_price, sl, tp, R:R, ATR details

  c) Trade parameters (logged before result on signal):
     "SignalBridge: trade params — entry=X, sl=X (dist=X pts), tp=X (dist=X pts),
      rr=Xx, atr=X"

  d) Risk rejection detail (logged on RISK_REJECTED):
     "SignalBridge: RiskManager rejected trade at TIMESTAMP. Risk summary: ..."

  e) Poll identifier per instance (logged at poll start in run_demo_trading.py):
     "[INSTANCE] Poll #N at HH:MM:SS UTC"

Files to read before starting:
  1. src/broker_support/live/signal_bridge.py
     — read the actual log messages in get_signal() to ensure regex patterns
       match exactly what is logged. Do NOT assume — read the source.
  2. One sample log file: outputs/broker_support/logs/demo_trading_240166_2026-04-01.log
     — verify log format and message patterns before writing any regex.

Scope:
  Write ONE new file: scripts/diagnostics/signal_log_extractor.py
  Do NOT modify any existing file.
  Do NOT make any API calls — log files only, fully offline.
  Process ALL demo_trading_*.log files in outputs/broker_support/logs/

Architecture constraints:
  - Use logger.info/debug — never print()
  - Use pathlib.Path — never hardcoded separators
  - Use datetime.now(timezone.utc) — never datetime.utcnow()

Script behaviour:
  1. Find all files matching outputs/broker_support/logs/demo_trading_*.log
     Extract instance_id from filename: demo_trading_<INSTANCE>_<DATE>.log

  2. Parse each file line by line. Group log lines into poll blocks using the
     Poll #N header line. For each poll block, extract:
       - poll_timestamp: from Poll header (HH:MM:SS → combine with log line date)
       - instance: from filename
       - log_date: from filename
       - bar_timestamp: from "no signal on last bar (TIMESTAMP)" or
                        "signal found at last bar — ... @ TIMESTAMP"
       - result: NO_SIGNAL / RISK_REJECTED / SIGNAL
       - direction: BUY / SELL / None
       - bid_price: float or None
       - entry_price_mid: float or None (from trade params line)
       - sl_distance: float or None (from "dist=X pts" in sl section)
       - tp_distance: float or None (from "dist=X pts" in tp section)
       - atr_value: float or None
       - risk_reward_ratio: float or None
       - rejection_reason: str or None (from risk summary line)

  3. Write all extracted rows to:
       outputs/broker_support/diagnostics/live_signals_extracted.csv
     One row per poll that reached stage 5 (has a bar_timestamp).
     Skip polls that did not reach stage 5 (off-hours, portfolio fetch errors, etc.)

  4. After writing CSV, log a summary:
       Total polls parsed: N
       Polls reaching stage 5: N
       Result breakdown: NO_SIGNAL=N, RISK_REJECTED=N, SIGNAL=N
       Date range: FIRST_DATE to LAST_DATE
       Instances: list

  5. If a log line cannot be parsed as expected, log a WARNING with the
     filename, line number, and content — do not crash.

Acceptance criteria:
  - Script runs without error from project root:
    python scripts/diagnostics/signal_log_extractor.py
  - CSV created at outputs/broker_support/diagnostics/live_signals_extracted.csv
  - CSV has correct headers and at least one data row
  - Summary statistics logged to console
  - Script handles missing or malformed log lines gracefully (warn, not crash)
  - No print() statements
  - No API calls

Output format: Complete file. No diff — new file only.
== END INSTRUCTION ==
```
