# Backtesting Platform Architecture Audit & Code Quality Guidelines / Recommendations
**Current Scope:** WBWS Strategy update and modification before backtesting development start
*Applies to all modules under `src/` and `scripts/`*  
**Version:** 1.0  
**Owner:** Krzysztof  
---
## 📌 Objectives
### **Primary Goals**
- Audit existing code of wbs_run_strategy and underpinning modules
- Optimize current code of run_wns_strategy and underpining modules.
- Establish a clean, modular, testable, scalable system.
- Replace dict‑based communication with typed domain contracts. (to discuss if better)
### **Continues Goals**
- Improve code hygiene and consistency.
- Reduce technical debt in runners and simulators.
- Prepare the platform for multi‑strategy support and orchestrator integration.
- Apply 500 lines code warning rule => once code reaches 500 lines analysis needs to be performed if sustainable
- Apply careful step by step modifications without destroying its current architecture by cascades of dependecies and impact, slower and progressive would be better that a "bigbang"
---
### **Completed**
- Strategy and all modules are integrated, tested, speed optimized and working well together, producing comparable results  
---
**Code Analysis Deficiencies spotted with best practices, Lessons Learned & Development Rules**
---
## 1. Overview
This document synthesizes the architectural insights, recurring issues, and best practices identified during the WBWS strategy testing and the evolution of the Backtesting Platform (BT).
It covers:
*   Deficiencies across all modules (strategy, filters, simulator, risk, data loader, reporting, metrics, progressive tracking)
*   Best practices and lessons learned
*   Performance optimization principles
*   Structural and hygiene rules for developers
*   Cultural guidelines for long-term maintainability
The goal is to ensure BT evolves into a clean, modular, high-performance, professional-grade research engine.
---
## 2. Recurring Deficiencies (What We Must Avoid)
This section captures patterns observed across the entire codebase.
### 2.1 God-Script Anti-Pattern (Runner Overload)
`run_wbws_strategy.py` currently orchestrates:
*   Data loading
*   Signal generation
*   Filtering
*   Trade simulation
*   Metrics
*   Reporting
*   Logging
*   Progressive tracking
**Problem:** Too many responsibilities → hard to test, reuse, or evolve.
### 2.2 Mixed Responsibilities Inside Modules
**Examples:**
*   `TradeSimulator` handles risk, position logic, LTF execution, logging, profiling, tracking
*   `FilterPipeline` handles indicator computation, caching, filtering, time filtering
*   `DataLoader` handles config parsing, validation, caching, and data loading
*   `ReportGenerator` mixes formatting, file writing, and data aggregation
**Problem:** Hard to isolate bugs, optimize, or reuse.
### 2.3 Implicit Data Dependencies
Modules assume:
*   DatetimeIndex
*   OHLCV columns
*   LTF = 1-second
*   Indicators exist
*   Config keys exist
*   Spread config exists
**Problem:** Silent failures, brittle integrations, hidden assumptions. 
### 2.4 Inconsistent Naming Conventions
**Examples:**
*   `"BUY"` vs `"LONG"`
*   `"SELL"` vs `"SHORT"`
*   `"sl_price"` vs `"raw_sl"` vs `"trigger_sl"`
*   `"entry_price"` vs `"executed_entry"`
*   `"signal"` vs `"direction"`
**Problem:** Mental overhead, refactor risk, unclear semantics.
### 2.5 Excessive Use of Dictionaries
Seen in:
*   trades
*   risk parameters
*   simulator state
*   progressive tracking
*   filter outputs
**Problem:** No type safety, no IDE support, no refactor safety.
### 2.6 Circular Dependency Risks
**Examples:**
*   Strategy ↔ FilterPipeline
*   Simulator ↔ RiskManager ↔ SpreadManager
*   DataLoader ↔ Config structure
**Problem:** Hard to modularize, test, or reuse.
### 2.7 Mixed Vectorized & Per-Bar Logic
*   Filters vectorized
*   Strategy per-bar
*   Simulator per-bar
*   RiskManager per-bar
*   Indicators vectorized
**Problem:** Hard to unify architecture, inconsistent performance patterns.
### 2.8 Lack of Domain Contracts
Before refactor, no:
*   specifically (but not limited to) in TradeSimulator and FilterPipeline
**Problem:** Modules communicated via ad-hoc dicts and DataFrames.
### 2.9 Magic Numbers & Implicit Config Usage
**Examples:**
*   ATR multipliers
*   R:R ratios
*   Filter thresholds
*   Spread rules
*   Time windows
**Problem:** Hard to tune, test, or document.
### 2.10 Inconsistent Error Handling
Some modules:
*   raise
*   log
*   skip silently
*   return None
**Problem:** Unpredictable behavior, debugging difficulty.
### 2.11 Overly Complex DataLoader
`DataLoader` currently:
*   loads config
*   validates config
*   loads data
*   caches data
*   logs info
*   returns multiple DataFrames
**Problem:** Too many responsibilities → should be split into:
*   `ConfigLoader`
*   `DataLoader`
*   `DataValidator`
### 2.12 Metrics & Reporting Coupled to Simulator Output
`metrics_calculator` and `report_generator` assume:
*   dict-based trade logs
*   specific field names
*   specific simulator structure
**Problem:** Hard to evolve simulator or strategy architecture.
### 2.13 Progressive Tracker Too Deeply Integrated
Progressive tracking is:
*   deeply embedded in simulator
*   tightly coupled to trade dict structure
*   not optional in some paths
**Problem:** Instrumentation should be optional and decoupled.
---
## 3. Best Practices & Lessons Learned
These emerged naturally during the architecture redesign.
### 3.1 Define Explicit Domain Contracts (for mudules concerned)
**Benefit:** Clear, typed communication between modules.
### 3.2 Strict Separation of Concerns
*   Strategy → decides what
*   Simulator → executes how
*   RiskManager → validates whether
*   FilterPipeline → validates when
*   Runner → orchestrates
*   DataLoader → loads data
*   ReportGenerator → formats output
**Benefit:** Predictable, testable architecture.
### 3.3 Prefer Dataclasses Over Dicts
**Benefit:** Type safety, IDE support, refactor safety.
### 3.4 Vectorize Heavy Computation
*   Filters
*   Indicator computation
*   LTF exit detection (Numba)
**Benefit:** Massive speedups.
### 3.5 Avoid Circular Imports by Design
Split enums, keep domain models independent.
### 3.6 Centralized Path Resolution
`paths.py` is a strong architectural asset.
### 3.7 Externalize All Configuration (already done but can be used more if needed)
YAML configs for:
*   strategy
*   backtesting
*   spreads
*   data
### 3.8 Optional Instrumentation
Progressive tracking only in debug mode.
### 3.9 Use float32 for OHLCV (already done, also .parquet file format deployed under .yaml control)
**Benefit:** 2× memory savings, faster vectorized ops.
### 3.10 Keep Execution Engine Independent
LTF execution should not depend on strategy logic.
---
## 4. Module-Specific Recommendations
This section expands the earlier analysis to cover all modules.
### 4.1 DataLoader
**Issues**
*   Too many responsibilities
*   Implicit assumptions about data structure
*   Mixed validation, loading, and caching
**Recommendations**
*   Split into:
    *   `ConfigLoader`
    *   `DataLoader`
    *   `DataValidator`
*   Add explicit type hints
*   Validate required columns
*   Avoid returning 4+ DataFrames; use a dataclass bundle
*   Add caching at the file level, not inside the loader
### 4.2 FilterPipeline
**Issues**
*   Mixed indicator computation + filtering
*   Complex caching logic
*   Hard to test individual filters
**Recommendations**
*   Move indicator computation to a dedicated module
*   Keep filters pure (input → mask)
*   Use dataclasses for filter config
*   Add unit tests for each filter
### 4.3 RiskManager
**Issues**
*   Very large class
*   Mixed ATR, spread, annual range, validation
*   Returns dicts
**Recommendations**
*   Split into:
    *   `ATRCalculator`
    *   `SpreadAdjuster`
    *   `RiskValidator`
*   idea: return `TradeParameters` directly
*   Add caching for ATR and annual range / ATR computation as filter based on dedicated libraries 
### 4.4 TradeManager
**Issues**
*   Uses strings `"BUY"` / `"SELL"`
*   Returns dicts
*   Hardcoded logic
**Recommendations**
*   Idea use `TradeDirection` enum
*   Return `DecisionType`
*   Move logic into a pure state machine
### 4.5 TradeSimulator
**Issues**
*   Very large
*   Too many responsibilities
*   Dict-based trades
*   Deep coupling to RiskManager and TradeManager
**Recommendations**
*   Split into:
    *   `ExecutionEngine`
    *   `ExitEngine`
    *   `TradeLogger`
*   Use `TradeRecord` dataclass
*   Keep LTF logic isolated
### 4.6 MetricsCalculator
**Issues**
*   Assumes dict-based trades
*   Hardcoded field names
**Recommendations**
*   Accept `List[TradeRecord]`
*   Use typed metrics objects
*   Add unit tests
### 4.7 ReportGenerator
**Issues**
*   Mixed formatting + file writing
*   Hardcoded JSON structure
**Recommendations**
*   Split into:
    *   `ReportBuilder`
    *   `ReportWriter`
*   Use dataclasses for report sections
### 4.8 ProgressiveTracker
**Issues**
*   Deeply integrated into simulator
*   Hard to disable cleanly
**Recommendations**
*   Make it a plugin
*   Use an event-based interface
*   Keep it optional
---
## 5. Code Structure & Hygiene Rules
**Rule 1 — One Responsibility Per Module**  
If a file does more than one thing → split it.
**Rule 2 — All Domain Objects Must Be Dataclasses**  
No dicts for trades, signals, or parameters.
**Rule 3 — No Deep Imports**  
*Allowed*:
*   strategy → trade_management
*   runner → strategy
*   simulator → trade_management  
*Not allowed*:
*   trade_management → strategy
*   filters → simulator
**Rule 4 — No Magic Numbers**  
Everything must come from config.
**Rule 5 — Validate Inputs Aggressively**  
Every function must validate:
*   index type
*   required columns
*   required indicators
*   required config keys
**Rule 6 — No Silent Failures**  
Raise or log clearly.
**Rule 7 — Use Type Hints Everywhere**  
Mandatory for maintainability.
**Rule 8 — Separate Vectorized & Per-Bar Logic**  
*   Filters = vectorized
*   Strategy = per-bar
*   Simulator = per-bar
**Rule 9 — Keep Runner Thin**  
Runner should only:
1.  load config
2.  load data
3.  instantiate strategy
4.  instantiate simulator
5.  loop
6.  collect results
**Rule 10 — Document Every Module**  
Top-level docstring must explain:
*   purpose
*   dependencies
*   inputs/outputs
---
## 6. Performance Optimization Principles
### 6.1 Use Numpy Arrays for Hot Loops
Especially LTF exit detection.
### 6.2 Use float32 for OHLCV
Better memory + speed.
### 6.3 Cache Indicators
`FilterPipelineCache` is essential.
### 6.4 Avoid DataFrame Ops Inside Loops
Convert to numpy first.
### 6.5 Precompute LTF Windows
Huge speedup.
### 6.6 Avoid Repeated `.loc` in Loops
Use numpy indexing.
### 6.7 Use Numba for Hot Paths
Exit detection is a perfect use case.
### 6.8 Avoid Unnecessary Object Creation
Reuse arrays and buffers.
### 6.9 Keep Strategy Logic Lightweight
Strategy should not compute indicators.
### 6.10 Lazy Evaluation
Compute only what is needed.
---
## 7. Cultural Rules for Developers
*   **Be explicit** - No hidden assumptions.
*   **Be modular** - Small files, small classes, small functions.
*   **Be predictable** - Follow naming conventions and contracts.
*   **Be defensive** - Validate inputs aggressively.
*   **Be consistent** - Same patterns everywhere.
*   **Be performance-aware** - Think vectorization and memory.
*   **Be incremental** - Small, safe changes.
*   **Be documented** - Explain why, not just what.
*   **Be testable** - Every module testable in isolation.
*   **Be refactor-friendly** - Avoid tight coupling.
---
## 8. Final Notes
This document is the foundation for a clean, scalable, high-performance backtesting engine. It should be updated as the architecture evolves, especially during:

*   WBWSStrategy optimization
*   Backtester orchestrator development
*   Multi-strategy support
*   Optimization pipelines (GA, WFO, MC)