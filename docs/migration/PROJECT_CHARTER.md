# WBWSStrategy Migration Project Charter
## Project Overview
**Objective**: Migrate existing WBWSStrategy from dict-based architecture to typed domain contracts while maintaining 100% functional parity and performance. Parity is same important as Perf. If no parity and no technical bug then analysis and decision to be made if accepted (exemple bug in legacy discovered) but globaly we expext parity matches.   
**Strategic Goal**: Create reusable platform foundation for future strategies and automated backtesting orchestrator.
**Timeline**: Multi-session project (estimated 18-28 sessions)
**Success Criteria**:
- ✅ Where relevant modules use typed contracts (no dict-based trade/signal communication)
- ✅ Performance maintained or improved (≤110% of baseline)
- ✅ Old system continues working in parallel
- ✅ Stage-by-stage validation against old outputs
- ✅ Ready for orchestrator integration
---
## Constraints & Principles
### Non-Negotiable
1. **No performance regression** - New ≤ 110% of old execution time
2. **Backward compatibility** - Old system untouched and functional, migration happens paralelly to existing
3. **Incremental validation** - Test after each module migration
4. **Session continuity** - Handoff protocol for chat window limits
### Design Principles
- Single Responsibility (one module = one concern)
- Performance-driven (vectorization, caching and more advanced)
- Explicit contracts (no hidden assumptions)
- Type safety (dataclasses over dicts)
- Performance-aware (vectorization, caching, etc.)
- Test-driven (validate each step)
---
## Current System Baseline
**Performance** (3-month sample):
- Original: <2 minutes end-to-end>
- **Target**: No regression/improvement
---
**Test Dataset**: 3 months of data
- Sufficient for metrics validation
- Fast iteration cycles
----
**Output Artifacts**:
- Raw signals (pre-filter)
- Time-filtered signals
- Technical-filtered signals
- Trade entries/exits
- Performance metrics
- Progressive tracking (debug mode)
---
## Migration Strategy
**Approach**: Hybrid Big Bang + Thin Slice
- **Big Bang**: Simple modules (DataLoader, SignalGenerator)
- **Thin Slice**: Complex modules (FilterPipeline, TradeSimulator)
**Parallel Execution**:
```
src/strategies/
├── core/              # OLD (frozen, working)
├── specific/          # NEW (migration target)
└── contracts/         # SHARED (domain types)
```
**Validation Framework**:
- Module-by-module output comparison
- Performance benchmarking
- Metrics parity testing
---
## Risk Management
| Risk | Mitigation |
|------|------------|
| Chat window limit | Progressive checkpoints every 3-5 exchanges |
| Performance regression | Benchmark at each step |
| Breaking old system | Parallel architecture, no modifications to `core/` |
| Incomplete migration | Phase-based approach with rollback points |
---
## Current Status = > sessions handoffs and migration plan