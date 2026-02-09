# WBWSStrategy Migration Project Charter

## Project Overview
**Objective**: Migrate existing WBWSStrategy from dict-based architecture to typed domain contracts while maintaining 100% functional parity and performance.

**Strategic Goal**: Create reusable platform foundation for future strategies and automated backtesting orchestrator.

**Timeline**: Multi-session project (estimated 18-28 sessions)

**Success Criteria**:
- ✅ All modules use typed contracts (no dict-based trade/signal communication)
- ✅ Performance maintained or improved (≤110% of baseline)
- ✅ Old system continues working in parallel
- ✅ Stage-by-stage validation against old outputs
- ✅ Ready for orchestrator integration

---

## Constraints & Principles

### Non-Negotiable
1. **No performance regression** - New ≤ 110% of old execution time
2. **Backward compatibility** - Old system untouched and functional
3. **Incremental validation** - Test after each module migration
4. **Session continuity** - Handoff protocol for chat window limits

### Design Principles
- Single Responsibility (one module = one concern)
- Explicit contracts (no hidden assumptions)
- Type safety (dataclasses over dicts)
- Performance-aware (vectorization, caching)
- Test-driven (validate each step)

---

## Current System Baseline

**Performance** (3-month sample):
- Original: ~30 minutes end-to-end
- Optimized: <2 minutes end-to-end (15x improvement)
- **Target**: Maintain <2 min performance

**Test Dataset**: 3 days of data (~2 second execution)
- Sufficient for metrics validation
- Fast iteration cycles

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
- Stage-by-stage output comparison
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

## Current Status
- **Phase**: 0 - Foundation
- **Last Updated**: [DATE]
- **Next Milestone**: DataLoader migration