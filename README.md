# 📘 Backtesting Platform
## 📌 Overview
This project aims to build a **high‑precision trading backtesting environment** that complements TradingView rather than replacing it.
The idea originates from practical limitations observed in the TradingView Strategy Tester (TV/ST):
* Limited historical depth (typically ~5–40k bars)
* Bar‑based simulation instead of true tick‑level execution
**Conclusion:** TradingView backtests are useful for *high‑level validation* but are insufficient for designing or validating a professional trading system intended for live deployment.
This project therefore focuses on **accurate, data‑driven backtesting using real tick data**, while keeping TradingView as the primary platform for:
* Live trading
* Charting
* Alerts and execution
This repository is **not intended for commercialization or publication**. It exists solely to support and improve my personal trading workflow.
---
## 🎯 Project Objectives
At a detailed level, the platform aims to:
* Provide access to high‑quality historical OHLCV data generated from **real tick data**, with at least **2 years of history**.
* Offer a **modular and maintainable framework** for translating TradingView Pine Script strategies into Python.
* Enable **automated backtesting** to identify optimal parameter configurations for strategies executed live on TradingView.
* Use GitHub as a structured workspace for version control and AI‑assisted development.
* Support backtesting across multiple:
  * Asset classes (Forex, indices, gold, etc.)
  * Timeframes
---
## 🧱 Code Structure & Design Principles
* Scripts should remain **small, reusable, and well‑encapsulated**, each addressing a single responsibility.
* Avoid monolithic scripts combining data loading, computation, visualization, and reporting.
* Build strategies from **composable building blocks** (indicators, filters, trade management modules).
* Keep the repository **clean, adaptive, and logically structured**.
* When designing new strategies:
  * Reuse existing components whenever possible.
  * Write new components with **future reuse** in mind.
* Maintain clear and professional documentation to support long‑term scalability.
* Ensure all scripts and configurations are compatible with automated runners and batch backtesting workflows.
---
## 🤝 Collaboration & Development Workflow
GitHub is used for version control and AI‑assisted development. All significant refactoring and feature additions are tracked through commit history.
The long‑term goal is to build an **automated, highly iterative backtesting pipeline**, where AI assistance helps:
* Explore parameter spaces
* Identify optimal configurations
* Detect structural weaknesses in strategies
---
## 📊 Supported Assets (Dukascopy Naming Convention)

| Asset            | Dukascopy Datafeed Name |
| ---------------- | ----------------------- |
| GOLD (XAUUSD)    | xauusd                  |
| DAX40 (GER40)    | deuidxeur               |
| SPX500           | usa500idxusd            |
| DOW (DJ30)       | usa30idxusd             |
| NASDAQ (NS100)   | usatechidxusd           |
| CAC40            | fraidxeur               |
| UK100 (FTSE 100) | gbridxgbp               |
| AUDUSD           | audusd                  |
| EURJPY           | eurjpy                  |
| EURUSD           | eurusd                  |
| GBPUSD           | gbpusd                  |
| USDCAD           | usdcad                  |
| USDCHF           | usdchf                  |
| USDJPY           | usdjpy                  |
---
## 📅 Project Status (as of 29/12/2025)
### General
* **Project start date:** 05/12/2025
* **First selected strategy:** *We Buy / We Sell Trigger* (Pine Script v6)
### Data Pipeline
* Raw tick data (.bi5) download from Dukascopy implemented and tested
* Incremental tick updates supported
* Tick‑to‑OHLCV transformation validated
* At least **2 years of real tick data** available (from 01 Dec 2023)
* 1‑minute DAX40 OHLCV dataset available as a development baseline
### Strategy Translation
* WBWS Trigger indicator fully translated to Python
* RSI filter translated and validated
* Time‑based trade filtering implemented
* ATR‑based risk management (SL/TP + RR) implemented
* High similarity with TradingView results confirmed across components
* Trade/position management integrated (managing pyramiding and oposit signal detection)
### Configuration
* YAML‑based, asset‑agnostic configuration system implemented
* Strategy, filter, and data aggregation parameters fully externalized
---
## 📂 Repository Structure (Current)
project_root/
│
├── .gitignore
├── README.md                                    # This document
├── requirements.txt                             # Required packages with versions
│
├── configs/                                     # YAML configuration files
│   └── wbws_dax40_60min.yaml                   # Default WBWS config for DAX40
│   └── data_aggregator.yaml                   # Settings file for generate_ohlcv.py to create csv data files
│
├── data/
│   ├── exports/
│   ├── features/
│   ├── processed/                               # Processed data ready for backtesting
|   |   ├── ohlcv/ csv files => different instruments, time frames, full date ranges (~2 years)
|   |       ├── ... (csv files)
│   │   ├── DAX40_FULL.parquet
│   │   ├── DAX40_LAST_10000.parquet
│   │   ├── DAX40_LAST_30_DAYS.parquet
│   │   ├── DAX40_REAL_PROCESSED.parquet
│   │   └── DAX40_TV_RANGE.parquet              # Currently used validation dataset
│   ├── raw/                                    # Raw data files
|   |   ├── dukascopy_bi5/             # Datafeed from Dukascopy
|   |   |   └── ... subfolders with real tick data for at least 2 years (organized in hourly .bi5)
│   │   
│   └── results/
│
├── docs/  # folder for documentation
│
├── notebooks/ # folder for notebooks in ipynb format
│   └── example_usage.ipynb
│
├── outputs/
│   ├── backtests/                               # backtest results/outputs
│   ├── logs/                                    # logs for strategies and platform functionning
│   ├── reports/
│   │   ├── Data_quality/                        # Data quality check reports
│   │   └── WBWS/                                # WBWS execution and validation reports
│   │       ├── execution_YYYYMMDD_HHMMSS.json  # Execution reports from indicator
│   │       └── validation_YYYYMMDD_HHMMSS.json # Validation reports
│   └── signals/                                # Signal/trade exports (CSV)
│       └── trade_details_YYYYMMDD_HHMMSS.csv   # Filtered and time/risk validated signals fromrunner
│
├── pine_scripts/                               # Original TradingView Pine v6 scripts
│   ├── StrategyBuilderLab.pine                 # Strategy components (filters, trade mgmt)
│   └── WBWS_Trigger.pine                       # WeBuy WeSell TradingView indicator
│
├── scripts/
│   ├── data_preprocessing/                      # Data preprocessing utilities
│   │   ├── __init__.py
|   |   └── generate_ohlcv.py           # main script generating desired ohlcv csv files from real tick data
│   ├── data_scripts/                           # Data helper scripts
│   │   ├── download_raw_ticks.py               # Dukascopy datafeed real tick .bi5 file downloader
|   |   └── update_raw_ticks                    # Dukascopy datafeed delta real tick .bi5 file downloader
│   ├── setup_scripts/                          # Backtesting setup scripts
│   └── validation_scripts/                     # Utilities to validate strategies and indicators
│       ├── Filters/                            # Filters specific validations
│       |   └── test_rsi_filter.py              # Simple test for RSI filer using standard settings
│       ├── Strategy/                           # Strategies specific validations
|       └── WBWS/                               # WBWS-specific validations
│           └── validate_strategy_data.py       # Script validating availability, structure and quality of historical ohlc data (.csv) for strategy runner
│   ├── dashboard_standalone.py                 # Main enhanced strategy dashboard
│   ├── backtest_simulator.py                   # (obsolete)/placeholder
│   ├── run_wbws_strategy.py                    # Runner script assembling WeBuy WeSell trigger with filters
│   └── run_wbws_trigger.py                     # Runner script WeBuy WeSell trigger only
│
├── src/                                # Basctesting platform sources, utilities
│   ├── __init__.py
│   ├── main.py
│   ├── backtesting/                    # Backtesting automatation
│   ├── config/                         # Configuration management
│   │   ├── __init__.py
│   │   └── WBWS/                       # specifig WBWS 
│   |       ├── filter_configs.yaml     # Filter configuration settings
|   |       └── wbws_rsi_strategy.yaml  # WBWS Strategy with filter configurations settings
│   ├── indicators/                     # indicator scripts for backtesting
│   │   ├── __init__.py
│   │   └── wbws_trigger.py             # WBWS calculation engine and signal trigger
│   ├── strategies/                     # strategy scripts for backtesting 
│   │   ├── filters/                    # strategy scripts for signal filtering
|   |   |   └── rsi_filter.py           # WBWS Strategy with filter configurations settings
│   │   ├── trade_management/           # strategy scripts for trage and risk management
|   |   |   ├── __init__.py
|   |   |   ├── time_manager.py         # Filtering signal for specific session time
|   |   |   ├── trade_manager.py        # Managing pyramiding possibility and opposite signal detection
|   |   |   └── risk_manager.py         # Applying risk mgt StopLoss ATR based and RR TakeProfit
│   │   └── WBWS                        # strategy scripts specific for WBWS strategy 
│   ├── utils/                          # Utility modules
│   │   ├── __init__.py
|   |   ├── json_to_md converter (for reports)
│   │   └── report_generator.py         # Report generation utilities
│   └── visualization/                  # Vizualization utilities
│
├── tests                               # folder for testing
|   ├── test_time_manager.py            # basic tests script for time/session managment
|   ├── test_time_manager.py            # basic tests script for risk managment 
|   └── test_trade_manager.py           # basic tests script for trade managment
└── venv/                               # Venv specific folders and files
---
## Key Development platform Components
**`requirements.txt`**
# Environement OS = Microsoft Windows [version 10.0.22621.4317]
# Core tools/packages for data analysis and backtesting
- python==3.13.9
- pandas==2.3.3
- numpy==2.3.5
- matplotlib==3.10.7
- seaborn==0.12.2
- ta-lib==0.4.24
# Backtesting and visualization libraries
- vectorbt==0.28.1
# Data handling and finance-specific packages
- pyarrow==22.0.0
-  yfinance==0.2.66
# Development and documentation tools
- jupyterlab_widgets==3.0.16
- pyyaml==6.0.3
# Additional packages for trade management modules
- pytz==2025.2

---

🔄 Development Roadmap (In Progress – target 31/12/2025)
### Continue translation of TradingView filters into Python:
* DPO
* Bollinger Bands
* Choppiness Index
### Finalize full WBWS strategy execution, validation and dashboard
### Prepare automated, parameter‑driven backtesting pipelines
---
## 📖 Key Backtest testing & execution components
---
## 🚀 Quick Start ### Prerequisites / Guidances
---
### Raw and preprocessed data management
---
# Run ducascopy dowloader tick to get raw real tick data (.bi5 hourly files) for an instrument
`python scripts/data_scripts/download_raw_ticks.py`
# Run ducascopy dowloader tick to get delta of raw real tick data (.bi5 hourly files) for an instrument => checs the last available .bi5 file and gets the most recent .bi5 files
`python scripts/data_scripts/update_raw_ticks`
# Run transformating tool to generate time framed ohlcv csv file from .bi5 hourly files => uses yaml configuration file with settings like: instrument, desired TimeFrame, data range...
# Remark: .bi5 file are in UTC timezone whilst all csv are converted to desired timeframe for exemple: CET/CEST
`python scripts/data_preprocessing/generate_ohlcv.py configs/data_aggregator.yaml`
# Example of ohlcv data file structure:
timestamp,open,high,low,close,volume
2025-12-22 14:49:00,24252.788000,24254.777000,24249.777000,24251.799000,80305408680.000000
2025-12-22 14:50:00,24250.755000,24252.299000,24249.255000,24249.755000,43976771420.000000
---
### Activate virtual environment (venv).
`.\venv\Scripts\Activate.ps1`
---
### Main Orchestrators/Runners
**`scripts/run_wbws_strategy.py` => for WBWS strategy**
- **Purpose:** End-to-end workflow orchestrator assembling signal triggering indicator, filters, time manager, risk manager & initial metrics
- **Workflow:**
  1. Load YAML configuration
  2. Run WBWS Trigger indicator
  3. Run Filetrs (currently only RSI) agains triggered signals
  3. Generate reports and outputs
- **Usage:** `python scripts/run_wbws_strategy.py src\config\WBWS\wbws_rsi_strategy.yaml`
---
## 📊 Current Performance metrics for DAX40 sample data (to be used as reference in intermediary testing)
- **Usage:** `python scripts/dashboard_standalone.py outputs/reports/WBWS/strategy_report_20251227_224945.json --visualize` (--visualize optional)
---
### Auxiliary tools
---
## WBWS trigger runner only for testing **
- **Purpose:** Workflow orchestrator for WBWS trigger
- **Workflow:**
  1. Load YAML configuration
  2. Run WBWS Trigger indicator
  3. Generate reports and outputs
- **Usage:** `python scripts/run_wbws_trigger.py wbws_rsi_strategy.yaml`
---
### Report Generation for testing**
- **Purpose:** Handle all output operations (reports, exports, console printing)
- **Features:**
  - Save JSON execution reports
  - Export signals to CSV (optional)
  - Print formatted summaries
  - Display sample signals
**Usage:** `python src/utils/report_generator.py`
---
### Validation Scripts
- **Purpose:** Test on simple config data of Rsi filter
- **Features:** - Prints simple signal reports
**Usage:** `python scripts\validation_scripts\Filters\test_rsi_filter.py`
- **Purpose:** Validates readines of the indicated OHLCV data
- **Features:**
  - Uses same .yaml config file as strategy orchestrators for input
  - Prints reports on data availability, structure, quality and confirms readiness 
  - Can be used preliminary to launch strategy runner to validate data quality
  **Usage:** `python scripts\validation_scripts\validate_strategy_data.py.py src\config\WBWS\wbws_rsi_strategy.yaml`
- **Purpose:** Validates time management script against serveral test scenarios
- **Features:** - Tests time manager(trading session) script
  - Uses same .yaml config file as strategy orchestrators for input
  **Usage:** `tests\test_time_manager.py src\config\WBWS\wbws_rsi_strategy.yaml`
  - **Purpose:** Validates risk management script against serveral test scenarios
- **Features:** - Tests SL, RR TP and risk percentile
  - Uses same .yaml config file as strategy orchestrators for input
  **Usage:** `tests\test_risk_manager.py src\config\WBWS\wbws_rsi_strategy.yaml`
  **Features:** - Tests pyramiding and opposit signal management
  - Uses same .yaml config file as strategy orchestrators for input
  **Usage:** `tests\test_trade_manager.py src\config\WBWS\wbws_rsi_strategy.yaml`
---
*End of README*