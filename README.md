📘 Backtesting Platform (BT)
📌 Overview
This project provides a high‑precision, data‑driven backtesting environment designed to complement TradingView rather than replace it.

TradingView is excellent for:
      - Live trading
      - Charting
      - Alerts
      - Visual strategy development
…but its Strategy Tester has limitations:
      - Limited historical depth (5k–40k bars)
      - Bar‑based execution instead of tick‑accurate simulation
      - No realistic spread/slippage modeling
      - No multi‑timeframe execution with true LTF fills

Conclusion: TradingView is ideal for high‑level validation, but insufficient for designing a professional trading system intended for live deployment.

This BT fills that gap by providing:
      - Real tick data ingestion
      - High‑quality OHLCV generation
      - Modular strategy translation from Pine Script to Python
      - Automated backtesting, optimization, and evaluation

BT is not intended for publication or commercial use. 
It exists to support and improve a personal trading workflow.

🧱 Design Principles
Single Responsibility — each module does one thing well
Separation of Concerns — data, logic, and presentation are isolated
Reusability — modules can be reused across strategies
Testability — each component is independently testable
Maintainability — clear folder structure, consistent imports
Automation Ready — scripts compatible with batch execution
Performance First — caching, vectorization, and optimized data loading

🎯 Development Principles
Generate high‑quality OHLCV from real tick data (≥ 2 years)
Provide a modular, maintainable Python framework for TradingView strategy translation
Enable automated backtesting with GA, WFO, Monte Carlo, and parameter sweeps
Support accurate execution modeling using LTF data for SL/TP fills
Maintain a clean, testable, automation‑ready codebase
Use GitHub as a structured workspace for version control and AI‑assisted development

🧱 Design Principles
Single Responsibility — each module does one thing well
Separation of Concerns — data, logic, and presentation are isolated
Reusability — modules can be reused across strategies
Testability — each component is independently testable
Maintainability — clear folder structure, consistent imports
Automation Ready — scripts compatible with batch execution
Performance First — caching, vectorization, and optimized data loading

📊 Supported Assets (Dukascopy Naming Convention & eToro CFD Spreads)
| Asset            | Dukascopy Datafeed Name | Spread / Fee (eToro CFD) | Unit   |
| ---------------- | ----------------------- | ------------------------ | ------ |
| GOLD (XAUUSD)    | xauusd                  | 0.025                    | %      |
| DAX40 (GER40)    | deuidxeur               | 0.015                    | %      |
| SPX500           | usa500idxusd            | 0.015                    | %      |
| DOW (DJ30)       | usa30idxusd             | 6                        | points |
| NASDAQ (NS100)   | usatechidxusd           | 0.015                    | %      |
| CAC40            | fraidxeur               | 1                        | point  |
| UK100 (FTSE 100) | gbridxgbp               | 1.5                      | points |
| AUDUSD           | audusd                  | 1                        | pip    |
| EURJPY           | eurjpy                  | 2                        | pips   |
| EURUSD           | eurusd                  | 1                        | pip    |
| GBPUSD           | gbpusd                  | 2                        | pips   |
| USDCAD           | usdcad                  | 1.5                      | pips   |
| USDCHF           | usdchf                  | 1.5                      | pips   |
| USDJPY           | usdjpy                  | 1                        | pip    |

📂 Repository Structure
Code
project_root/
│
├── configs/                        # All YAML configuration files
│   ├── spreads/
│   ├── data/
│   ├── backtesting/
│       └── configs\backtesting\wbws_backtest.yaml # Configuration files for run_wbws_strategy.py
│   └── strategies/
│       └── wbws/
│           └── wbws_rsi_strategy.yaml # Model configuration files for run_wbws_strategy.py
│
├── data/                           # All input datasets
│   ├── raw/                        # Tick data (.bi5)
│   ├── processed/                  # OHLCV datasets
│   ├── features/
│   └── exports/
│
├── outputs/                        # All generated outputs
│   ├── backtests/
│   ├── logs/
│   ├── reports/
│   └── signals/
│
├── scripts/                        # Entry-point scripts (CLI only)
│   ├── data/
│   │   ├── generate_ohlcv.py
│   │   ├── download_raw_ticks.py
│   │   └── update_raw_ticks.py
│   ├── validation/
│   │   └── validate_strategy_data.py
│   ├── runners/
│   │   ├── run_wbws_strategy.py
│   │   └── dashboard_standalone.py
│   └── setup/
│
├── src/                            # Core library (importable, reusable modules)
│   ├── backtesting/
│   |   └── orchestrator_fixed.py # Backtester script
│   ├── indicators/
│   ├── strategies/ 
│   │   ├── core/
│   │   ├── filters/
│   │   ├── trade_management/
│   │   └── wbws/
│   ├── validation/
│   ├── dashboard/
│   ├── utils/
│   │   └── paths.py                # Centralized path resolver
│   ├── visualization/
│   ├── generate_ohlcv.py
│   ├── download_raw_ticks.py
│   └── update_raw_ticks.py
│
├── pine_scripts/
├── docs/
├── notebooks/
├── tests/
└── venv/

🔗 Dependency Diagram
                          +----------------------+
                          |      scripts/        |
                          |  CLI entrypoints     |
                          |  - runners           |
                          |  - data scripts      |
                          |  - validation CLI    |
                          +----------+-----------+
                                     |
                                     v
+-----------------------------------------------------------------------+
|                               src/                                   |
|                                                                       |
|  +------------------+      +------------------+      +--------------+ |
|  |  strategies/     | ---> | backtesting/     | ---> | outputs/     | |
|  |  Signal logic    |      | Orchestrator, GA |      | (results)    | |
|  |  Filters, TM     |      | WFO, MC, Eval    |      +--------------+ |
|  +------------------+      +------------------+                       |
|           ^                        ^                                  |
|           |                        |                                  |
|  +------------------+      +------------------+                       |
|  | indicators/      |      | validation/      |                       |
|  | WBWS trigger     |      | Data/Strategy QC |                       |
|  +------------------+      +------------------+                       |
|                                                                       |
|  +------------------+      +------------------+                       |
|  | dashboard/       | ---> | visualization/   |                       |
|  | Metrics, charts  |      | Plot utilities   |                       |
|  +------------------+      +------------------+                       |
|                                                                       |
|  +------------------+                                                |
|  | utils/           |                                                |
|  | Shared helpers   |                                                |
|  | paths.py         |                                                |
|  +------------------+                                                |
+-----------------------------------------------------------------------+
                                     ^
                                     |
                          +----------------------+
                          |      configs/        |
                          | YAML configs         |
                          +----------------------+

                          +----------------------+
                          |       data/          |
                          | raw/ processed/ etc. |
                          +----------------------+
🧭 BT Path Resolution Model 
All scripts and modules use a centralized path resolver: src/utils/paths.py
This module defines:
      - PROJECT_ROOT
      - DATA_DIR
      - OUTPUTS_DIR
      - CONFIGS_DIR
      - LOGS_DIR
      - helper functions like data_path(), output_path(), config_path()
Example: 
python from src.utils.paths import PROJECT_ROOT, LOGS_DIR, data_path
file = data_path("processed", "ohlcv", "xauusd_m1.csv")
log_file = LOGS_DIR / "wbws_strategy.log"

▶️ Usage of BT tools
1. Activate virtual environment: .\venv\Scripts\Activate.ps1
2. Download raw tick data (.bi5, hourly, UTC): python scripts/data/download_raw_ticks.py (donwload config inside script)
3. Update raw tick data (delta): python scripts/data/update_raw_ticks.py (update config inside script)
4. Generate OHLCV from tick data: python scripts/data/generate_ohlcv.py configs/data/data_aggregator.yaml
      Example of ohlcv data file structure (Important all prices are BID prices and timestamp in CET/CEST):
      timestamp,open,high,low,close,volume
      2025-12-22 14:49:00,24252.788000,24254.777000,24249.777000,24251.799000,80305408680.000000
      2025-12-22 14:50:00,24250.755000,24252.299000,24249.255000,24249.755000,43976771420.000000
5. Run WBWS strategy (in stand‑alone): python scripts/runners/run_wbws_strategy.py configs/strategies/wbws/wbws_rsi_strategy.yaml
6. Run the Backtesting Orchestrator: src\backtesting\orchestrator_fixed.py configs/backtesting/wbws_backtest.yaml