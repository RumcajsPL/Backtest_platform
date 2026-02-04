(venv) PS E:\Trading\Backtest_platform> python scripts\runners\run_wbws_strategy.py configs\strategies\wbws\wbws_strategy.yaml
2026-02-04 20:52:29,104 [INFO] ======================================================================
2026-02-04 20:52:29,105 [INFO] WBWS STRATEGY WORKFLOW
2026-02-04 20:52:29,106 [INFO] ======================================================================
2026-02-04 20:52:29,277 [INFO]   Strategy period: 2,786 bars
2026-02-04 20:52:29,278 [INFO]   HTF dataset: 49 bars
2026-02-04 20:52:29,278 [INFO]   LTF dataset: 73,163 bars (TF: 1s)
2026-02-04 20:52:29,279 [INFO]   Date range: 2025-12-15 08:00:00 to 2025-12-17 21:00:00
2026-02-04 20:52:29,288 [INFO] STEP 2: GENERATING SIGNALS
2026-02-04 20:52:29,309 [INFO]   Raw BUY: 146, SELL: 178, Total: 324
2026-02-04 20:52:29,309 [INFO] STEP 3: APPLYING FILTERS
2026-02-04 20:52:30,058 [INFO]   Applying time filter...
2026-02-04 20:52:30,071 [INFO]     Time filtered: 219 (97 BUY, 122 SELL)
2026-02-04 20:52:30,072 [INFO]   Applying RSI filter...
2026-02-04 20:52:30,088 [INFO]     RSI filtered: 210 (94 BUY, 116 SELL)
2026-02-04 20:52:30,097 [INFO] STEP 4: SIMULATING TRADES
2026-02-04 20:52:33,346 [INFO]   Simulated: 20 closed, 0 open, 190 rejected
2026-02-04 20:52:33,346 [INFO]   Execution: LTF (1s) for SL/TP
2026-02-04 20:52:33,347 [INFO] STEP 5: CALCULATING METRICS
2026-02-04 20:52:33,365 [INFO]   Total P&L: +289.09 pts
2026-02-04 20:52:33,366 [INFO]   Win Rate: 45.0%
2026-02-04 20:52:33,366 [INFO]   Profit Factor: 3.19
2026-02-04 20:52:33,367 [INFO] STEP 6: GENERATING REPORTS
2026-02-04 20:52:33,367 [INFO]   Generating JSON report...
2026-02-04 20:52:33,371 [INFO]     JSON saved: outputs\reports\WBWS\strategy_report_20260204_205233.json
2026-02-04 20:52:33,371 [INFO] ======================================================================
2026-02-04 20:52:33,372 [INFO] EXECUTION COMPLETED (CORE MODE)
2026-02-04 20:52:33,372 [INFO] ======================================================================
2026-02-04 20:52:33,373 [INFO] PERFORMANCE SUMMARY:
2026-02-04 20:52:33,374 [INFO]   Mode:              CORE
2026-02-04 20:52:33,375 [INFO]   Raw Signals:       324
2026-02-04 20:52:33,375 [INFO]   Executed Trades:   20
2026-02-04 20:52:33,375 [INFO]   Rejection Rate:    93.8%
2026-02-04 20:52:33,376 [INFO]   Total P&L:         +289.09 pts
2026-02-04 20:52:33,377 [INFO]   Win Rate:          45.0%
2026-02-04 20:52:33,377 [INFO]   Profit Factor:     3.19
2026-02-04 20:52:33,379 [INFO] OUTPUT FILES:
2026-02-04 20:52:33,379 [INFO]   Config:            wbws_strategy.yaml
2026-02-04 20:52:33,380 [INFO]   JSON Report:       outputs\reports\WBWS\strategy_report_20260204_205233.json
2026-02-04 20:52:33,381 [INFO] Completed:         2026-02-04 20:52:33
2026-02-04 20:52:33,381 [INFO] ======================================================================