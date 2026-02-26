# E2E Pipeline — `run_strategy.py`
## Prerequisites
```bash
# From project root, with venv active
cd E:\Trading\Backtest_platform
.venv\Scripts\activate
```
---
## Basic Usage
```bash
# 1. Default run (uses strategy_template.yaml, mode from YAML)
python scripts/runners/run_strategy.py
# 2. Run your WBWS config
python scripts/runners/run_strategy.py --config configs/strategies/wbws/wbws_strategy_v2.yaml
# 3. Force analytics mode (overrides YAML without modifying it)
python scripts/runners/run_strategy.py --config configs/strategies/wbws/wbws_strategy_v2.yaml --mode analytics
# 4. Force core mode (fastest — no analytics or report)
python scripts/runners/run_strategy.py --config configs/strategies/wbws/wbws_strategy_v2.yaml --mode core
# 5. Debug — full log output
python scripts/runners/run_strategy.py --config configs/strategies/wbws/wbws_strategy_v2.yaml --mode analytics --log-level DEBUG
```
---
## Expected Output — core mode
```
============================================================
RESULT SUMMARY
============================================================
  Mode          : core
  Total trades  : 142
  Win rate      : 61.3%
  Total PnL     : +1842.5 pts
  Expectancy    : +12.97 pts/trade
  Profit factor : 2.14
  Max drawdown  : -312.0 pts
  Stage timing:
    data          :    234.1 ms
    signals       :     87.3 ms
    filters       :    412.6 ms
    trades        :     98.4 ms
    metrics       :     12.1 ms
    TOTAL         :    844.5 ms
============================================================
```
---
## Expected Output — analytics mode
```
============================================================
RESULT SUMMARY
============================================================
  Mode          : analytics
  Total trades  : 142
  Win rate      : 61.3%
  Total PnL     : +1842.5 pts
  Expectancy    : +12.97 pts/trade
  Profit factor : 2.14
  Max drawdown  : -312.0 pts
  Grade         : B+
  Assessment    : Strategy shows good performance (grade B+) across 142 trades…
  Insights      : 14 generated
  Analytics ms  : 23.4
  Report        : outputs/strategies/reports/wbws/report_20260226_143022.html
  Report ms     : 187.6
  Stage timing:
    data          :    234.1 ms
    signals       :     87.3 ms
    filters       :    412.6 ms
    trades        :     98.4 ms
    metrics       :     12.1 ms
    analytics     :     23.4 ms
    report        :    187.6 ms
    TOTAL         :   1055.5 ms
============================================================
```
---
## What to Validate E2E
Work through these in order — each confirms a deeper layer is wired correctly.
| Step | Command                                    | Confirms                                                  |
|------|--------------------------------------------|-----------------------------------------------------------|
| 1    | `--log-level DEBUG` (no `--mode`)          | Config loads, YAML parses, DataLoader finds files         |
| 2    | `--mode core`                              | Full pipeline runs, trades generated, metrics calculated  |
| 3    | `--mode analytics`                         | Analytics + report generated, HTML file exists on disk    |
| 4    | Compare core vs analytics PnL              | Must be identical — mode only adds analytics metadata     |
| 5    | Open HTML report in browser                | All 3 tabs render, charts display, grade badge visible    |
| 6    | Set `output.reports.enabled: false`, rerun | Analytics block printed, report path absent from output   |
| 7    | Change one YAML param, rerun               | Result changes as expected — config is being read         |
---
## What the Analytics Block Tells You
The `Grade` line is computed from 4 components (25 pts each, total 100):

| Component        | Scoring                                          |
|------------------|--------------------------------------------------|
| Win rate         | ≥20% = 25, ≥15% = 20, ≥10% = 10                 |
| Profit factor    | ≥2.0 = 25, ≥1.5 = 20, ≥1.2 = 10                 |
| Drawdown control | DD < 20% of profit = 25, < 50% = 15, < 100% = 5 |
| Consistency      | Score ≥70 = 25, ≥50 = 15, ≥30 = 5               |
Score → Grade: 90+ A+ | 85 A | 80 A− | 75 B+ | 70 B | 65 B− | 60 C+ | 55 C | 50 C− | 40 D+ | 30 D | <30 F
The `Insights` count is the total across all categories (time/quality/risk/general).
Open the HTML report's **Executive** tab to read each insight with its recommendation.
---
## Controlling Report Generation
```yaml
# In strategy YAML
output:
  reports:
    enabled: true              # false → analytics runs but no HTML written
    output_dir: "outputs/strategies/reports/wbws"
    theme: "dark"              # "dark" | "light"
    chart_height_px: 300
    brand_name: "WBWSStrategy"
    include_raw_data: true     # false → Raw Data tab hidden
```
Setting `enabled: false` is useful during development or parameter sweeps where you want
analytics metrics (`result.analytics`) but do not need the disk I/O of HTML generation.
---
## Common Failures and What They Mean
| Error                             | Cause                                        | Fix                                                                     |
|-----------------------------------|----------------------------------------------|-------------------------------------------------------------------------|
| `Config file not found`           | Wrong path or cwd                            | Check `--config` path is relative to project root                      |
| `Configuration error: ...`        | YAML schema violation                        | Check YAML against `StrategyConfig` fields                             |
| `File not found` inside pipeline  | DataLoader can't find OHLCV files            | Check `data.paths.strategy_ohlcv` in YAML                             |
| `Invalid mode 'debug'`            | Old YAML has `mode: debug`                   | Change to `core` or `analytics` in YAML                               |
| `asset.symbol is blank`           | Spread enabled but no symbol                 | Add `asset.symbol` to YAML or disable spread                          |
| Pipeline runs but 0 trades        | Filters too aggressive or date range narrow  | Run `--mode analytics --log-level DEBUG` to see filter rejection counts |
| Report not generated              | `output.reports.enabled: false`              | Set to `true` in YAML                                                  |
| HTML report exists but charts blank | No internet / CDN blocked                  | Chart.js loads from CDN; check network; Raw Data tab still works       |
| Grade is F with positive PnL      | Consistency score very low                   | High P&L variance — check for outlier trades dominating the curve      |
---
## Reading the Debug Log
Key lines to look for with `--log-level DEBUG`:
```
# Config loaded cleanly
INFO | StrategyOrchestrator | Config loaded: wbws_strategy_v2.yaml
# Data found and sliced
INFO | DataLoader | Loaded 48320 bars (2020-01-02 → 2023-12-29)
# Filters wired correctly
INFO | FilterPipeline | time_filter=enabled, technical_filters=3, cfg_hash=a3f8c2...
INFO | FilterPipeline | Time filter: 08:30 – 20:30
# ATR cache behaviour (first run = miss, subsequent = hit)
INFO | RiskManager | ATR computed and cached (Wilder RMA, length=14, key=4a2b1c...)
# Filter pass rates
INFO | FilterPipeline | rsi_filter: 340 → 187 (153 rejected)
INFO | FilterPipeline | FilterPipeline complete: 340 → 112 signals (32.9% pass rate)
# Risk filter summary (analytics mode — always shown at INFO)
INFO | TradeSimulator | Risk filter summary | filter=ACTIVE | threshold=0.2300% |
      checked=112 | approved=89 | rejected=23 | rejection_rate=20.5%
# Analytics and report stages (analytics mode only)
INFO | StrategyOrchestrator |   [analytics]      23.4 ms
INFO | StrategyOrchestrator |   [report]        187.6 ms
INFO | StrategyOrchestrator | Analytics complete | grade=B+ | insights=14 | duration=23.4ms
INFO | StrategyOrchestrator | Report generated | path=outputs/.../report_20260226_143022.html
INFO | run_strategy | HTML report saved → outputs/strategies/reports/wbws/report_20260226_143022.html
```
If you see a stage missing from the log entirely, that module is not being reached — the error will be in the stage before it. If `[analytics]` appears but `[report]` does not, check `output.reports.enabled` in your YAML.