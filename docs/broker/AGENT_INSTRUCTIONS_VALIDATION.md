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

== INSTRUCTION ==
Agent:        B
Environment:  Staging
Task:         Write a one-time backfill script that queries eToro closed trade
              history and reconstructs trades.csv for all four CTP instances
              from authoritative API data, replacing corrupted records.

Context:
  The PositionTracker had a PascalCase field mismatch (now fixed) that caused
  all recorded trades to have trade_id=nan, instrument=UNKNOWN_0, exit_price=0.0,
  profit_loss=0.0. Additionally CSVJournal deduplication collapsed all records
  to a single corrupted row per instance. The real closed trades exist in the
  eToro trade history API but were never correctly journalled.

  The demo account is a shared paper trading account. It holds trades from
  multiple sources (manual trades, other strategies). The backfill must only
  write trades whose positionID appears in the instance's open_positions.json —
  those are the CTP-placed positions for that instance. No other trades must
  enter trades.csv.

  open_positions.json currently contains the full set of CTP-placed positionIDs
  for each instance, including both still-open and recently-closed ones, because
  the tracker failed to remove closed IDs (due to the same field bug). This makes
  it the correct source of truth for "which positionIDs belong to this instance".

  The API 30-day window means trades closed before 2026-04-05 may not be
  recoverable via API. The script must log a warning for any positionID in
  open_positions.json that is not found in the API response.

Files to read before starting:
  src/broker_support/client/client.py
      → fetch_closed_trades() signature, params, return format
      → field names in history response: camelCase+lowercase id
        (positionId, instrumentId, closeRate, netProfit, fees, leverage,
         stopLossRate, takeProfitRate, isBuy, openDate, closeDate)
  src/broker_support/models/trade.py
      → Trade model field aliases — use these for construction
  src/broker_support/tracking/csv_journal.py
      → CSVJournal.append_trades() — use this to write, not pd.to_csv directly
  src/broker_support/enrichment/instrument_resolver.py
      → InstrumentResolver.symbol() — use this to resolve instrument name
  configs/broker_support/instrument_map.yaml
      → instrument map format
  outputs/broker_support/journal/c424/open_positions.json
  outputs/broker_support/journal/240166/open_positions.json
  outputs/broker_support/journal/7ffbc5/open_positions.json
  outputs/broker_support/journal/61875/open_positions.json
      → CTP positionIDs to filter against

Scope:
  PRODUCE — scripts/broker_support/backfill_trades_csv.py  (new file)

  The script must:
  1. Accept --instance (one or more, default all four) and --dry-run flags
  2. For each instance:
     a. Load CTP positionIDs from open_positions.json
        — if file absent or empty: log warning and skip instance
     b. Call fetch_closed_trades() with minDate 30 days back, paginating
        until raw_trades is empty (max 10 pages as in TradeEnricher)
     c. Filter API results to records whose positionId is in the CTP set
     d. For each matching record, construct a Trade via Trade.model_validate()
        using the history field aliases (positionId, instrumentId, closeRate,
        netProfit, openDate mapped to openTimestamp, closeDate mapped to
        closeTimestamp, isBuy, fees, leverage, stopLossRate, takeProfitRate)
     e. Resolve instrument symbol via InstrumentResolver.symbol(instrument_id)
        and set trade.instrument — use model_copy(update={'instrument': symbol})
     f. Read the current trades.csv to identify the corrupted row
        (trade_id == 'nan' or str(trade_id) == 'nan')
     g. In --dry-run mode: print what would be written, write nothing
     h. In live mode:
        — Delete trades.csv for this instance (removes the corrupted row)
        — Write all recovered trades via CSVJournal.append_trades()
        — Log count of trades written and any positionIDs not found in API
  3. Log a warning for every CTP positionID not found in the API response,
     with the message: "positionID=<id> not found in API — may be outside
     30-day window or still open"
  4. Never write a trade with trade_id='nan' or instrument='UNKNOWN_0'
     — assert these are absent before writing; raise ValueError if found

  DO NOT CHANGE any existing source file
  DO NOT modify trades.csv directly with pd.to_csv — use CSVJournal only
  DO NOT call DataLoader or TradeSimulator
  DO NOT call inspect_portfolio.py or any live portfolio endpoint
    — fetch_closed_trades() only; no portfolio fetch

Architecture constraints (from SKILL.md):
  - datetime.now(timezone.utc) — never datetime.utcnow()
  - pathlib.Path — never hardcoded separators
  - logger.info/debug — never print() (except --dry-run summary to stdout is acceptable)
  - fetch_closed_trades() uses minDate=YYYY-MM-DD (not 'from' or 'fromDate')
  - History returns camelCase+lowercase id (positionId, instrumentId)
  - Do NOT use bar.get("field", 0.0) pattern for fields that can be None —
    use explicit None checks
  - Do NOT call _make_request() directly — use client methods only

Acceptance criteria:
  1. --dry-run runs without error and prints a clear per-instance summary
     showing which positionIDs were found, which were not, and what
     would be written to trades.csv
  2. After live run on staging:
     — trades.csv for each instance contains only rows with real trade_id
       (no 'nan', no 'UNKNOWN_0' instrument)
     — trade count per instance matches the number of CTP positionIDs
       found in the API response
     — Any positionID not found is clearly logged as a warning
  3. Script is idempotent: running it twice produces the same trades.csv
     (CSVJournal deduplication handles this)
  4. pytest tests/broker_support/ -v passes with zero regressions
     (no new tests required for this script — it is a one-time utility)

Output format:
  Complete file: scripts/broker_support/backfill_trades_csv.py
  Dry-run output from staging: python scripts/broker_support/backfill_trades_csv.py --dry-run
  (relay dry-run output back to Claude.ai before running live)
== END INSTRUCTION ==