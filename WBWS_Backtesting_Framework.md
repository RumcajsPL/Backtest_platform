# 📊 Backtesting & Optimization Framework Project 
**Systematic Strategy Evaluation, Optimization & Robustness Automatic Testing**
# Environement OS = Microsoft Windows [version 10.0.22621.4317]
---
## 1. General Description
The **Backtesting & Optimization Framework** is designed to go beyond simple strategy testing.  
Its goal is to **systematically evaluate, optimize, and validate trading strategies** under realistic market conditions while controlling:
- Risk exposure  
- Overfitting  
- Execution bias  
- Random performance effects  
The framework focuses on **automatation, robustness and long-term stability**
---
## 2. Core Philosophy and Architecture Principles
The framework is built around the following principles:
- **Automation first** – all backest pipline to run automatically with auto-optimization.   
- **Risk awareness** – Drawdown, streaks, and ruin probability matter  
- **Robustness over raw profit** – Stability beats lucky results  
- **No curve-fitting** – Validation across time and randomness  
- **Modular architecture** – See architecture principles for more details  
- **Inteligent analytics** – End of backtesting pipline is 1 step. Rich analytics and statistics from backtesting help for "what if", optimization structured and adhoc data analysis. Having all   
---
## Architecture Principles to respect in each develoment, modifications, updates etc.
### 1. Single Responsibility
One module, one concern. No module reaches into another module's domain. Each module trusts its inputs implicitly — validation happens at configuration boundaries.
All config is created and data are validated on the 
### 2. Contracts Are the Interface
Every module accepts and returns typed, frozen dataclasses. There are no raw dicts, no shared state, no global variables passed between modules. If you need to add information that crosses a module boundary, add a field to the relevant contract — do not bypass the contract.
### 3. Immutability
All contracts use frozen=True. Any module that needs to derive a field at construction time uses object.__setattr__ in __post_init__ — that is the only acceptable use. After construction, contracts are read-only.
### 4. Explicit Over Implicit
No hidden defaults buried in logic. Mode-gated behaviour (core vs analytics) is explicit at every call site. Expensive operations run when the mode requires them.
### 5. Vectorisation First
Hot paths use can numpy/pandas vectorised operations. Python loops appear only where the logic cannot be vectorised. Computations and config loading are cached via the central CacheManager.
### 6. Fail Fast
Invalid configuration raises immediately at construction via __post_init__ validation. There are no silent fallbacks, no auto-corrections of bad input. If a value is wrong, the system tells you before any computation begins. Missing data at runtime the backesting — it never silently approves it.
### 7. Single Source of Truth
Configuration flows from config file to all modules. No module loads its own config. Config file controls backtester work with defined setting and boanderies.
### 8. Cache Lifecycle Management
All module-level caches and are managed by a central CacheManager. Call clear_all_caches() between backtester runs to ensure clean state.
### 9. Code hygiene -> Test management integration
Code delivered has no MagicMocks, no debug flags, no print statements,
no test artifacts, no dummies, no commented-out blocks. Type hints are present and
minimal — they document intent, not implementation. Comments explain *why*, never *what*.
Every file is the right size: not so small it hides structure, not so large it hides complexity.
Mockups, dummies, debug, assumptions are domain of unit test developed together with principal code.
Ttesting on real data with real conditions are integrated from early stages.
Fail-fast principle (in Architecture Code): no assumptions, no checking different folders, no trying, no guessing.  
If something is not there: not matching, not answering, no data — backtester aborts
with a clear error message. Testing can retake for detailed debgging and diagnosis.
## 3. High-Level Backtest Flow (example only not design decision)
```
[Random Search]
      ↓
[Genetic Optimization]
      ↓
[Walk-Forward]
      ↓
[Monte Carlo]
      ↓
[Final Report and Analytics]
```
### Detailed Pipeline Flow (example only not design decision)
```
Sampler
  ↓
Strategy Runner
  ↓
Metrics
  ↓
Fitness
  ↓
Candidate Store
  ↓
Walk-Forward Optimization
  ↓
Genetic Algorithm
  ↓
Monte-Carlo
  ↓
Robust Ranking
```
## 4. Project Folder Structure (example only not design decision)
From `<project root>`:
---
script/
└── runners/
    └──backtest_runner.py (strategy runner script uses .yaml as its config)
src/
├── backtesting/
    ├── __init__.py
    ├── orchestrator.py
    ├── optimization/
    │   ├── parameter_space.py
    │   └── sampler.py
    ├── ga/
    │   ├── crossover.py
    |   ├── ga_engine.py
    |   ├── mutation.py
    |   ├── population.py
    │   └── selection.py
    ├── monte_carlo/
    │   ├── equity_simulator.py
    |   ├── mc_engine.py
    |   ├── mc_metrics.py
    │   └── perturbation.py
    ├── evaluation/
    |   ├── candidate_store.py
    |   ├── ranker.py
    │   ├── metrics.py
    │   └── fitness.py
    ├── wfo/
    │   ├── wfo_engine.py
    │   ├── wfo_evaluator.py
    │   └── window_generator.py
└── config/
    └── backtesting/
        └── backtest_template.yaml (backtesting orchestrator.py yaml config)
---
### Outputs (example only not design decision)
outputs/
└── backtests/
    ├── reports/
    └── logs/ 
---
# IMPORTANT => All below represent exemples only, the real structure and architecture will be defined during design phase

## 5. The Orchestrator - `orchestrator.py`
The orchestrator is the **central control unit** of the system.
- Loads `backtest_template.yaml`
- Generates parameter sets
- Creates temporary strategy YAMLs
- Calls `run_strategy.py`
- Catches strategy outputs
- Computes fitness
- Selects best configs
- (Runs Walk-Forward Optimization)
- (Runs Monte Carlo simulations)
- Saves and compares results
---
## 6. Configuration File – `backtest_template.yaml`
### Optimization Zones
| Zone | Purpose |
|------|--------|
| Safe | Can my strategy work with standard settings? |
| Exploration | Where is the performance plateau? |
| Discovery | Is there hidden edge outside norms? |
### Optimizable Parameters
- RSI  
- HTF timeframe  
- ATR length  
- ATR multiplier  
- Risk percentile  
- RR target  
- Session windows  
### Example Constraints
- min_winrate: 0.55
- max_drawdown: 0.25  
- max_losing_streak: 12 
- min_trades_per_day: 4
- min_expectancy: 0.2
---
## 7. Main Modules
### 7.1 Parameter Sampler
`parameter_space.py` (Expands YAML ranges into parameter grids)
`sampler.py` (Selects smart subsets (Random / LHS))
- Reads wbws_backtest.yaml
- Understands zones (safe / exploration / discovery)
- Expands ranges (RSI, ATR, HTF, risk, etc.)
- Samples parameter sets (Random or Latin Hypercube)
- Outputs ready-to-run strategy YAML configs
- Keeps everything compatible with existing runner
### 7.2 Fitness Evaluator
`metrics.py` (Extracts required metrics from JSON)
- Total trades
- Wins (TP)
- Win rate
- Net P&L
- Max drawdown
- Losing streak
- Expectancy
- Profit factor
- Trades per day
`fitness.py` (Applies constraints + computes fitness)                      
### 7.3 Candidate Storage & Ranking
`candidate_store.py`
- Appends candidates to a JSON file
- Keeps everything structured
- Allows later analysis / GA / WFO
`ranker.py`
- Sort by fitness
- Extract Top-N
- Optionally filter by robustness metrics
### 7.4 Genetic Algorithm (GA)
- Uses fitness (not in-sample luck)
- Evolves parameter sets
- Respects your parameter zones
- Avoids brute-force explosion
- Produces robust candidates
- Flow: Initial Population→Fitness(WFO)→Selection→Crossover→Mutation→New Generation→Repeat→Best Candidates
`population.py`
`selection.py`
`crossover.py`
`mutation.py`
`ga_engine.py`
### 7.5 Walk-Forward Optimization (WFO)
- Train on in-sample window
- Test on out-of-sample window
- Roll forward
- Aggregate performance
- Rank by stability + performance
`window_generator.py` (Defines rolling train/test splits)
`wfo_evaluator.py` (Runs strategy per window and scores robustness)
`wfo_engine.py` (Orchestrates rolling evaluation)
### 7.6 Monte Carlo Testing
- Trade shuffling - Order dependency
- Return resampling - Distribution robustness
- Spread noise - Execution realism
- Risk perturbation - Parameter fragility
- Equity path simulation - Drawdown risk
`equity_simulator.py`
`perturbation.py`
`mc_metrics.py`
`mc_engine.py`
## 8. Metrics Summary
### Fitness Metrics
- Trades  
- Win rate  
- Net P&L  
- Drawdown  
- Expectancy  
- Profit factor  
### WFO Metrics
- avg_fitness  
- fitness_std  
- stability  
### Monte Carlo Metrics
- mc_avg_final  
- mc_worst_dd  
- mc_ruin_prob  
-----------------------------