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
## 2. Core Philosophy
The framework is built around the following principles:
- **Automation first** – Minimal manual tuning  
- **Risk awareness** – Drawdown, streaks, and ruin probability matter  
- **Robustness over raw profit** – Stability beats lucky results  
- **No curve-fitting** – Validation across time and randomness  
- **Modular architecture** – Easy to extend with new filters and logic  
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
No hidden defaults buried in logic. Mode-gated behaviour (core vs analytics) is explicit at every call site. Expensive operations (LTF precomputation, progressive tracking, signal ID lookups) run only when the mode requires them.
### 5. Vectorisation First
Hot paths use numpy/pandas vectorised operations. Python loops appear only where the logic cannot be vectorised (e.g. stateful trade management). ATR computation and spread config loading are cached via the central CacheManager.
### 6. Fail Fast
Invalid configuration raises immediately at construction via __post_init__ validation. There are no silent fallbacks, no auto-corrections of bad input. If a value is wrong, the system tells you before any computation begins. Missing data at runtime (e.g. RAR unavailable for a timestamp) rejects the trade — it never silently approves it.
### 7. Single Source of Truth
Configuration flows from strategy_template.yaml → StrategyConfig → all modules. No module loads its own config. Spread values are read exclusively from broker_spreads.yaml — the strategy template contains only the path to this file.
### 8. Cache Lifecycle Management
All module-level caches (ATR, annual range, spread configs) are managed by a central CacheManager. Call clear_all_caches() between backtester runs to ensure clean state.
### 9. Code hygiene -> Test management integration
Architecture Code delivered has no MagicMocks, no debug flags, no print statements,
no test artifacts, no dummies, no commented-out blocks. Type hints are present and
minimal — they document intent, not implementation. Comments explain *why*, never *what*.
Every file is the right size: not so small it hides structure, not so large it hides complexity.
Mockups, dummies, debug, assumptions are domain of unit test developed together with principal code.
Ttested on real data with real conditions are integrated from early stages.
Fail-fast principle (in Architecture Code): no assumptions, no checking different folders, no trying, no guessing.  
If something is not there: not matching, not answering, no data — the strategy aborts
with a clear error message. Testing can retake for detailed debgging and diagnosis
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
### Detailed Pipeline Flow
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
## 4. Project Folder Structure
From `<project root>`:
```
script/
└── run_wbws_strategy.py (strategy runner script uses .yaml as its config)
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
    └── WBWS/
        ├── wbws_backtest.yaml (orchestrator.py yaml config)
        └── wbws_rsi_strategy.yaml (strategy yaml config)
```
### Outputs
```
outputs/
└── backtests/
    ├── safe/yyyymmdd_hhss/
    |   ├── candidates.json
    |   ├── top_candidates.json
    |   └── strategy_report_001.json
    ├── exploration/
    ├── discovery/
    └── comparison_report.json
```
## 5. Usage
`python src/backtesting/orchestrator.py configs/wbws_backtest.yaml`

## 6. The Orchestrator - `orchestrator.py`
The orchestrator is the **central control unit** of the system.
- Loads `wbws_backtest.yaml`
- Generates parameter sets
- Creates temporary strategy YAMLs
- Calls `run_wbws_strategy.py`
- Reads JSON / CSV outputs
- Computes fitness
- Selects best configs
- (Runs Walk-Forward Optimization)
- (Runs Monte Carlo simulations)
- Saves and compares results
---
## 7. Configuration File – `wbws_backtest.yaml`
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
## 8. Main Modules
### 8.1 Parameter Sampler
`parameter_space.py` (Expands YAML ranges into parameter grids)
`sampler.py` (Selects smart subsets (Random / LHS))
- Reads wbws_backtest.yaml
- Understands zones (safe / exploration / discovery)
- Expands ranges (RSI, ATR, HTF, risk, etc.)
- Samples parameter sets (Random or Latin Hypercube)
- Outputs ready-to-run strategy YAML configs
- Keeps everything compatible with existing runner
### 8.2 Fitness Evaluator
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
### 8.3 Candidate Storage & Ranking
`candidate_store.py`
- Appends candidates to a JSON file
- Keeps everything structured
- Allows later analysis / GA / WFO
`ranker.py`
- Sort by fitness
- Extract Top-N
- Optionally filter by robustness metrics
### 8.4 Genetic Algorithm (GA)
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
### 8.5 Walk-Forward Optimization (WFO)
- Train on in-sample window
- Test on out-of-sample window
- Roll forward
- Aggregate performance
- Rank by stability + performance
`window_generator.py` (Defines rolling train/test splits)
`wfo_evaluator.py` (Runs strategy per window and scores robustness)
`wfo_engine.py` (Orchestrates rolling evaluation)
### 8.6 Monte Carlo Testing
- Trade shuffling - Order dependency
- Return resampling - Distribution robustness
- Spread noise - Execution realism
- Risk perturbation - Parameter fragility
- Equity path simulation - Drawdown risk
`equity_simulator.py`
`perturbation.py`
`mc_metrics.py`
`mc_engine.py`
## 9. Metrics Summary
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
------------------------------------------
# 10. Concerns & Configuration Issues Log  
## 10.1 Orchestrator Troubleshooting
### Fixed Issues / Concern points
- Yaml configuration file cleaned and unified
- orchestrator_fixed.py created as troubleshooting copy of orchestrator.py
- orchestrator_fixed.py troubleshooted succesfully optimization, evaluation, strategy integration
- integration of GA in orchestrator_fixed.py
- GA population.py reintegrated
- Perf improvment 
    - cache integration
    - parallel execution
- Better Random Search + GA Integration
  - GA starts with ALL best random candidates
  - No dilution with random individuals
  - Focused search around promising regions
- Strategy => Core vs Debug mode implemented (Core limited computation and outputs to required by orchestrator)
Strategy perf
      - Cache indicator calculations (to check if worth of doing)  
      - Vectorize signal generation (to check if worth of doing - if using loops)
      - Cache trade simulation results (to check if worth of doing - if deterministic)    
### Remaining Issues / Concern points
### 10.1.1 Non critical issues / concern points
- Rest of perf optimizations:
  - Strategy
      - Parallelize multi-timeframe processing (maybe: to check if worth of doing)
      - Batch indicator calculations (maybe: to check if worth of doing)
  - Reduced I/O (batch file operations)
- Analyze if update required for Strategy core mode in orchestrator
- Implement Core vs Debug mode to orchestrator
- Recovery mechanism (continue from last good state)
- Inconsistencies in Data Handling and Dates
- Floating-point precision and type handling could introduce subtle inconsistencie in some scripts
- Subprocess and file handling lacks full error resilience in some scripts GA
- Error handling for rest pending edge cases
- Metrics Extraction Assumptions
- Add Deduplication (analyze 1st as might be no issue with bigger populations)
- latin_hypercube in config unused—sampler is random
- Final updates of orchestrator_fixed.py
### 10.1.2 Future evolutions
- Re-integration of WFO in orchestrator_fixed.py 
- Re-integration of Monte Carlo in orchestrator_fixed.py
---
## 10.2 Dependency Requirements
| Module | Required Methods |
|--------|------------------|
| ParameterSpace | build() |
| ParameterSampler | random_sample() |
| OptimizationMetrics | get() |
| FitnessEvaluator | score() |
| CandidateStore | add(), save() |
| CandidateRanker | top_n() |
| WalkForwardEngine | run() |
| GeneticOptimizer | run() |
| MonteCarloEngine | run() |
---