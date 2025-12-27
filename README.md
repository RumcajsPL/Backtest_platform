# 📘 Backtesting Platform

## 📌 Overview
The Backtest Platform project aims to create an alternative trading backtesting environment that complements the TradingView platform.
The idea comes from the observed limitations of the TradingView Strategy Tester (TV/ST):
- Limited historical data (typically 5–40k historical bars)
- Historical bars are simulated, not based on actual tick data
**Conclusion:** Backtesting results from TV/ST can provide only a high-level idea of strategy potential. They cannot be reliably used to design or validate a professional trading system for live trading.
This project is therefore a custom, high-precision backtesting environment designed to overcome these limitations.
It is not intended to become a live trading platform with charts, alerts, and order execution. TradingView will remain the main platform for live trading.
This project will never be published or commercialized — it is solely for supporting and improving my personal trading workflow.
---
## 🎯 Project Objectives
At the detailed level, this project aims to:
- Provide access to high-quality historical OHLCV data based on real ticks, with a minimum of 2 years of history.
- Offer a simple and modular structure for translating TradingView Pine Scripts to Python.
- Automate backtesting to quickly derive optimal parameter settings for strategies executed live on TradingView.
- Use GitHub to simplify collaboration with AI assistance for development.
- Support backtesting of various assets:
  - Forex, indices, gold, and others.
- Support multiple trading timeframes.
---
## 🧱 Rules for Code Structure & Content Management
- Scripts should remain reasonably sized, reusable, and encapsulated, each focusing on a specific task.
- Avoid monolithic scripts combining computation, visualization, and data handling.
- Build the strategy from small, composable code bricks.
- Maintain a clean, adaptive, and well-organized repository.
- When designing new strategies:
  - Reuse existing code bricks whenever possible.
  - Ensure new components are written to be reusable in future strategies as well.
- Maintain professional documentation to support long-term project scalability.
---
## 🤝 Collaboration
This project uses GitHub for version control and AI-assisted development. All major refactoring and feature additions are documented in commit history.
Backtesting in its target deployment should be automated, highly iterative, manage by AI assitance as far as possible to create an autooptimizing algorithms helping in identifying the optimal settings for a strategy.
Inspiration can be Strategy Tester mechanism from MetaTrader 5 (like for exemple a genetic optimization alghorithm) 
## List of operated assets (with Ducascopy naming convention)
    Asset	Dukascopy Datafeed Name
    GOLD (XAUUSD)	xauusd
    DAX40 (GER40)	deuidxeur 
    SPTRD (SPX500)	usa500idxusd
    DOW (DJ30)	usa30idxusd
    NASDAQ (NS100)	usatechidxusd (US Tech / Nasdaq 100)
    CAC (FR40)	fraidxeur
    UK100 (FTSE 100)	gbridxgbp
    AUDUSD	audusd
    EURJPY	eurjpy
    EURUSD	eurusd
    GBPUSD	gbpusd
    USDCAD	usdcad
    USDCHF	usdchf
    USDJPY	usdjpy

---
## 📅 Project Status (as of 21/12/2025)
- **Project initiation date:** 05/12/2025
- **First strategy selected:** We Buy / We Sell Trigger (Pine v6)
  - Trigger indicator located at: `pine_scripts/WBWS_Trigger.pine`
- **Historical data for Backtest platform**
  - scripts to download .bi5 data, then delta of .bi5 available and secessfully tested
  - script to transforma .bi5 files into required date range and TF .csv file available and
    scucessfully tested
  - .bi5 real tick hourly data available for all assets and for 2 years period (from 01 DEC 2023)
  - 1 min DAX40 ohlc .csv available for all historical data
✅ **Refactored to modular architecture (12/12/2025)**
- **Configuration system implemented:**
  - ✅ YAML-based configuration for asset-agnostic operation
  - ✅ Modular preprocessing, calculation, and reporting
- **requirements.txt prepared** with all installed packages dependencies (including PyYAML)
- **Folder structure refactored** for better modularity (see below)
- **WBWS Trigger successfully translated to Python:**
  - WBWS indicator tested on settings : 1 minute for main timeframe and 60 minutes for 
    Higher Time Frame
  - Quality testing completed
  - High similarity with TradingView results
- **RSI filter translated to Python:**
  - RSI filter tested on settings : length 14, overbought: 70, oversold: 30
  - Quality testing completed
  - High similarity with TradingView results
- **RSI filter translated to Python:**
  - RSI filter tested on settings : length 14, overbought: 70, oversold: 30
  - Quality testing completed
  - High similarity with TradingView results
---
## 📂 Repository Structure (21/12/2025)
```
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
│   └── signals/                                 # Optional signal exports (CSV)
│
├── pine_scripts/                               # Original TradingView Pine v6 scripts
│   ├── StrategyBuilderLab.pine                 # Strategy components (filters, trade mgmt)
│   └── WBWS_Trigger.pine                       # WeBuy WeSell TradingView indicator
│
├── scripts/
│   ├── data_preprocessing/                      # Data preprocessing utilities
│   │   ├── __init__.py
|   |   └── generate_ohlcv.py          # main script generating desired ohlcv csv files from real tick data
│   ├── data_scripts/                           # Data helper scripts
│   │   ├── download_raw_ticks.py               # Dukascopy datafeed real tick .bi5 file downloader
|   |   └── update_raw_ticks                    # Dukascopy datafeed delta real tick .bi5 file downloader
│   ├── run_wbws_trigger.py                     # Main orchestrator script
│   ├── setup_scripts/                          # Backtesting setup scripts
│   └── validation_scripts/                     # Utilities to validate strategies and indicators
│   |   ├── Filters/                            # Filters specific validations
│   |   |   └── test_rsi_filter.py              # Simple test for RSI filer using standard settings
│   |   ├── Strategy/                           # Strategies specific validations
│   |   └── WBWS/                               # WBWS-specific validations
│   |        └── validate_strategy_data.py      # Script validating availability, structure and quality of historical ohlc data (.csv) for strategy runner
│   ├── run_wbws_strategy.py                    # Runner script assembling WeBuy WeSell trigger with filters
│   └── run_wbws_trigger.py.pine                # Runner script WeBuy WeSell trigger only
│
├── src/                                # Basctesting platform sources, utilities
│   ├── __init__.py
│   ├── main.py
│   ├── backtesting/                    # Backtesting automatation
│   ├── config/                         # Configuration management
│   │   ├── __init__.py
│   │   ├── config_loader.py            # YAML config loader
│   │   ├── paths.py                    # script setting up scripts (obsolete)
│   │   └── WBWS/                       # specifig WBWS 
│   |   │   ├── filter_configs.yaml     # Filter configuration settings
|   |   |   └── wbws_rsi_strategy.yaml  # WBWS Strategy with filter configurations settings
│   ├── indicators/                     # indicator scripts for backtesting
│   │   ├── __init__.py
│   │   └── wbws_trigger.py             # WBWS calculation engine and signal trigger
│   ├── strategies/                     # strategy scripts for backtesting 
│   │   ├── filters/                    # strategy scripts for signal filtering
|   |   |   └── rsi_filter.py           # WBWS Strategy with filter configurations settings
│   │   ├── trade_management/           # strategy scripts for trage and risk management
|   |   |   ├── __init__.py
|   |   |   ├── time_manager.py         # Filtering signal for specific session time
|   |   |   └── risk_manager.py         # Applying risk mgt StopLoss ATR based and RR TakeProfit
│   │   └── WBWS                        # strategy scripts specific for WBWS strategy 
│   ├── utils/                          # Utility modules
│   │   ├── __init__.py
|   |   ├── json_to_md converter (for reports)
│   │   └── report_generator.py         # Report generation utilities
│   └── visualization/                  # Vizualization utilities
│
├── tests                               # folder for testing
|   ├── test_time_manager.py            # basic tests script for time_manager 
|   └── test_risk_manager.py            # basic tests script for risk_manager
└── venv/                               # Venv specific folders and files
---
## Key Development platform Components
**`requirements.txt`**
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

## 📖 Key Components Documentation
### Strategy trigger indicators
**`src/indicators/wbws_trigger.py`**
- **Purpose:** Signal calculation engine for WBWS Trigger indicator
- **Scope:** Core logic - candle classification, reversal detection, HTF alignment, signal generation
- **Input:** Preprocessed OHLCV DataFrame (DatetimeIndex, standardized columns)
- **Output:** DataFrame with buy/sell signals + execution statistics
- **Key Feature:** Asset-agnostic, configuration-driven

### Strategy filters
**`src\strategies\filters\rsi_filter.py`**
  - RSI Filter - filters signals for overbought/oversold bias
  - Inputs: length, overbought, oversold
  - Print formatted summaries/status/error messages

### Strategy trade management
**`src\strategies\trade_management\time_manager.py`**
  - Filtering signals to specifically defined session start and end hours (withing ohlcv file timestap)
  - Inputs: start hour/minutes; end hour/minutes (based on .yaml config)
**`src\strategies\trade_management\risk_manager.py`**
  - Applying risk management with ATR based StopLoss and Rist to Reward TakeProfit
  - Inputs SL: ATR length, multiplier (default 14, 1.4); Inputs TP: RR (default 2)
  - Input Risk Percentile: special function modifying SL if exceeding some define price percentile change

### Main Orchestrators/Runners
**`scripts/run_wbws_trigger.py` => for WBWS trigger only**
- **Purpose:** Workflow orchestrator for WBWS trigger
- **Workflow:**
  1. Load YAML configuration
  2. Run WBWS Trigger indicator
  3. Generate reports and outputs
- **Usage:** `python scripts/run_wbws_trigger.py configs/<name>.yaml`
**`scripts/run_wbws_strategy.py` => for WBWS strategy**
- **Purpose:** End-to-end workflow orchestrator assembling signal triggering indicator and filters
- **Workflow:**
  1. Load YAML configuration
  2. Run WBWS Trigger indicator
  3. Run Filetrs (currently only RSI) agains triggered signals
  3. Generate reports and outputs
- **Usage:** `python scripts/run_wbws_strategy.py src\config\WBWS\wbws_rsi_strategy.yaml`

### Report Generation
**`src/utils/report_generator.py`**
- **Purpose:** Handle all output operations (reports, exports, console printing)
- **Features:**
  - Save JSON execution reports
  - Export signals to CSV (optional)
  - Print formatted summaries
  - Display sample signals

### Validation Scripts
**`scripts\validation_scripts\Filters\test_rsi_filter.py`**
- **Purpose:** Test on simple config data of rsi filter
- **Features:**
  - Prints simple signal reports
**`scripts\validation_scripts\validate_strategy_data.py.py`**
- **Purpose:** Validates readines the indicated OHLCV data
- **Features:**
  - Uses same .yaml config file as strategy orchestrators for input
  - Prints reports on data availability, structure, quality and confirms readiness 
  - To be used preliminary to launch strategy runner 
---
## 🗂 Development Plan
### 🔄 In Progress (to do by 28/12/2025)
- Continue translation of TradingView filters into Python (one filter per script):
  - DPO
  - Bollinger Bands
  - Choppiness Index
- Finalize translation of Pine strategy trade management:
  - Additional validation vs TradingView
  - Dashboard
- Finalize strategy execution for DAX40 historical data
- Start preparing automated backtesting pipeline
---
## 🚀 Quick Start
### Prerequisites / Guidances
```
### Raw and preprocessed data management
# Run ducascopy dowloader tick to get raw real tick data (.bi5 hourly files) for an instrument
python scripts/data_scripts/download_raw_ticks.py
# Run ducascopy dowloader tick to get delta of raw real tick data (.bi5 hourly files) for an instrument => checs the last available .bi5 file and gets the most recent .bi5 files
python scripts/data_scripts/update_raw_ticks
# Run transformating tool to generate time framed ohlcv csv file from .bi5 hourly files => uses yaml configuration file with settings like: instrument, desired TimeFrame, data range...
# Remark: .bi5 file are in UTC timezone whilst all csv are converted to desired timeframe for exemple: CET/CEST
python scripts/data_preprocessing/generate_ohlcv.py configs/data_aggregator.yaml
Example of ohlcv data file structure:
timestamp,open,high,low,close,volume
2025-12-22 14:49:00,24252.788000,24254.777000,24249.777000,24251.799000,80305408680.000000
2025-12-22 14:50:00,24250.755000,24252.299000,24249.255000,24249.755000,43976771420.000000

### Initiating virtual env.
```bash
# Activate virtual environment (venv)
.\venv\Scripts\Activate.ps1
---
## 📊 Current Performance metrics for DAX40 sample data (to be used as reference in intermediary testing)
--- End of file ---