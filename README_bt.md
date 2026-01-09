# 📊 WBWS Backtesting & Optimization Framework

This document describes the backtesting, optimization, and robustness testing architecture used for the WBWS RSI Strategy.

The goal of this framework is not just to test a strategy, but to systematically evaluate, optimize, and validate it under realistic market conditions while controlling risk, overfitting, and execution bias.

🎯 Philosophy

This framework is designed around a few core principles:

Automation first – minimal manual tuning

Risk awareness – drawdown, streaks, ruin probability matter

Robustness over raw profit – stability beats lucky results

No curve-fitting – validation across time and randomness

Modular architecture – easy to extend with new filters

The objective is to find parameter regions that survive:

Different market periods

Different trade sequences

Execution noise (spread, slippage)

Statistical stress tests

🧱 System Architecture

The backtesting pipeline follows this structure:

Parameter Sampling
        ↓
Strategy Execution
        ↓
Metrics Extraction
        ↓
Fitness Evaluation
        ↓
Candidate Ranking
        ↓
(Next: WFO / GA / Monte-Carlo)


Each component is independent and reusable.

⚙️ Key Components
# 1. Strategy Runner

Runs the WBWS RSI strategy using a YAML configuration.

File: scripts/run_wbws_strategy.py

Output: strategy_report_*.json

2. Parameter Sampler

Generates parameter sets from predefined zones:

Safe – conservative, industry-standard ranges

Exploration – wider but realistic ranges

Discovery – unconventional / experimental ranges

Defined in:

src/config/WBWS/wbws_backtest.yaml

3. Orchestrator

Controls the full backtesting process:

Generates parameter samples

Runs the strategy

Collects reports

Extracts metrics

Applies fitness rules

Ranks candidates

Main file:

src/backtesting/orchestrator.py


Run with:

python src/backtesting/orchestrator.py src/config/WBWS/wbws_backtest.yaml

4. Metrics & Fitness Evaluation

Only essential metrics are used for optimization:

Win rate

Expectancy

Drawdown

Trade count

Risk–Reward

Losing streak

Fitness scoring balances:

Profitability

Risk

Stability

5. Candidate Ranking

All valid candidates are stored and ranked:

outputs/backtests/<zone>/<timestamp>/
  ├── candidates.json
  └── top_candidates.json


This allows:

Manual inspection

Further optimization

Future GA / WFO processing

🧪 Validation Philosophy

A strategy is considered acceptable only if it:

Maintains ≥55% win rate

Keeps drawdown under control

Avoids extreme losing streaks

Has a positive expectancy

Survives multiple market conditions

Future validation layers:

Walk-Forward Optimization (WFO)

Genetic Algorithms (GA)

Monte-Carlo robustness testing

📁 Folder Structure (Backtesting)
src/
└── backtesting/
    ├── __init__.py

    ├── orchestrator.py          # Option A: Main controller (pipeline)

    ├── config/
    │   ├── __init__.py
    │   ├── loader.py            # Loads + validates optimization YAML
    │   └── schema.py            # Optional: YAML schema / rules

    ├── optimization/
    │   ├── __init__.py
    │   ├── parameter_space.py  # Expands YAML ranges into parameters
    │   ├── sampler.py          # Random / Latin / hybrid sampling
    │   └── genetic.py          # GA logic

    ├── validation/
    │   ├── __init__.py
    │   ├── walk_forward.py     # WFO engine
    │   └── monte_carlo.py      # Monte Carlo simulation

    ├── execution/
    │   ├── __init__.py
    │   └── strategy_runner.py  # Calls your existing run_wbws_strategy.py

    ├── evaluation/
    │   ├── __init__.py
    │   ├── metrics.py          # Lean optimization metrics
    │   └── fitness.py          # Fitness score calculation

    ├── reporting/
    │   ├── __init__.py
    │   ├── comparator.py       # Safe vs Exploration vs Discovery
    │   └── final_report.py     # Summary generator

outputs/
└── backtests/
    ├── safe/
    │   ├── random_search/
    │   ├── genetic/
    │   ├── walk_forward/
    │   └── monte_carlo/
    ├── exploration/
    ├── discovery/
    └── comparison_report.json

outputs/backtests/

🧠 Why This Approach?

Retail backtesting often fails because:

Results depend on one lucky period

Overfitting hides risk

Spread and execution are ignored

Drawdowns are underestimated

This framework is built to avoid those traps.

It treats trading as a research problem, not a guessing game.

🚀 Next Steps

Planned extensions:

Walk-Forward Optimization

Genetic Parameter Search

Monte-Carlo Stress Testing

Regime-based filters

Position sizing optimization

📌 Final Note

This backtesting system is not about finding a “perfect strategy”.

It is about finding survivable, realistic, and statistically robust trading behavior.

That is what protects capital.

If you want, next we can:

Re-integrate WFO cleanly

Or refine the fitness function

Or improve execution realism (spread/slippage models)