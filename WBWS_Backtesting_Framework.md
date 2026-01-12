# 📊 WBWS Backtesting & Optimization Framework  
**Systematic Strategy Evaluation, Optimization & Robustness Testing**
---
## 1. General Description
The **WBWS Backtesting & Optimization Framework** is designed to go beyond simple strategy testing.  
Its goal is to **systematically evaluate, optimize, and validate trading strategies** under realistic market conditions while controlling:
- Risk exposure  
- Overfitting  
- Execution bias  
- Random performance effects  
The framework focuses on **robustness and long-term stability**, not just short-term profitability.
---
## 2. Core Philosophy
The framework is built around the following principles:
- **Automation first** – Minimal manual tuning  
- **Risk awareness** – Drawdown, streaks, and ruin probability matter  
- **Robustness over raw profit** – Stability beats lucky results  
- **No curve-fitting** – Validation across time and randomness  
- **Modular architecture** – Easy to extend with new filters and logic  
---
## 3. High-Level Backtest Flow
```
[Random Search]
      ↓
[Genetic Optimization]
      ↓
[Walk-Forward]
      ↓
[Monte Carlo]
      ↓
[Final Report]
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
└── run_wbws_strategy.py (strategy runner script uses wbws_rsi_strategy.yaml as its config)
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
- Runs Walk-Forward Optimization
- Runs Monte Carlo simulations
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
### 8.4 Walk-Forward Optimization (WFO)
- Train on in-sample window
- Test on out-of-sample window
- Roll forward
- Aggregate performance
- Rank by stability + performance
`window_generator.py` (Defines rolling train/test splits)
`wfo_evaluator.py` (Runs strategy per window and scores robustness)
`wfo_engine.py` (Orchestrates rolling evaluation)
### 8.5 Genetic Algorithm (GA)
- Uses WFO fitness (not in-sample luck)
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
## 10.1 YAML Configuration Discrepancies
### 10.1.1 Duplicate GA Configuration
Two GA configuration sections exist. Only `ga` is used.  
The `genetic` section is ignored, causing important parameters to be unused.
### 10.1.2 Duplicate WFO Configuration
Both `wfo` and `walk_forward` exist.  
This creates ambiguity about which controls the optimization behavior.
### 10.1.3 Duplicate Monte Carlo Configuration
`monte_carlo_old` is ignored in favor of `monte_carlo`.
### 10.1.4 Missing Parameter Structure Contract
`config['optimization'] = params` assumes a specific structure that is not formally defined.
---
## 10.2 Orchestrator Discrepancies
### Fixed Issues
| Issue | Fix |
|------|-----|
| run_and_evaluate() returned bool | Now returns metrics |
| GA lacked Monte Carlo | MC added |
| Store API inconsistent | Standardized |
| No error handling | Try-catch added |
### Remaining Issues
- Parameter structure mismatch  
- Mixed WFO configuration  
- Assumed metric fields  
---
## 10.3 Dependency Requirements
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
## 10.4 Resolution Priority
### High Priority
1. Consolidate YAML configs  
2. Define parameter structure  
3. Validate metrics  
### Medium Priority
1. Standardize config access  
2. Add YAML schema  
3. Improve errors  
### Low Priority
1. Remove unused configs  
2. Add versioning  
3. Enhance logs  
---