# Architecture Decision Log

## Format
Each decision follows this structure:
- **Decision**: What was decided
- **Context**: Why it mattered
- **Options Considered**: Alternatives evaluated
- **Rationale**: Why this option was chosen
- **Implications**: What this means for the project
- **Date**: When decided

---

## Decision 001: Parallel Architecture (Old vs New)
**Date**: [SESSION 1]

**Decision**: Keep old system in `src/strategies/core/`, build new in `src/strategies/specific/`

**Context**: Need to maintain working system while migrating

**Options Considered**:
1. In-place migration (modify existing files)
2. Parallel architecture (separate folders)
3. Branch-based isolation (Git branches only)

**Rationale**:
- Option 2 provides maximum safety
- Old system remains executable for validation
- No risk of breaking production
- Easy rollback if needed

**Implications**:
- Duplicate code during migration
- Need adapter layer if modules interact
- Final cleanup phase to remove old code

---

## Decision 002: Hybrid Migration Strategy
**Date**: [SESSION 1]

**Decision**: Use Big Bang for simple modules (DataLoader, SignalGenerator), Thin Slice for complex modules (FilterPipeline, TradeSimulator)

**Context**: Different modules have different complexity levels

**Options Considered**:
1. Pure Big Bang (migrate entire module at once)
2. Pure Thin Slice (incremental across all modules)
3. Hybrid approach

**Rationale**:
- DataLoader is self-contained → Big Bang is safe
- TradeSimulator has many dependencies → Thin Slice reduces risk
- Hybrid optimizes for speed + safety

**Implications**:
- Need to identify which modules qualify for Big Bang
- Different validation strategies per approach

---

## Decision 003: Checkpoint Protocol
**Date**: [SESSION 1]

**Decision**: Provide checkpoint every 3-5 substantial exchanges with resume command

**Context**: Chat window limits risk losing progress

**Options Considered**:
1. Checkpoint at end of session only
2. Checkpoint every exchange
3. Checkpoint every 3-5 exchanges

**Rationale**:
- Option 3 balances continuity with verbosity
- Provides multiple recovery points
- Doesn't overwhelm with checkpoints

**Implications**:
- Need to maintain checkpoint discipline
- User must save checkpoints locally

---

## Decision 004: [Next Decision]
[To be filled as we progress]

## Decision 004: DataBundle Validation Strategy
**Date**: Session 1 (2025-02-09)

**Decision**: Validate DataFrame structure in `DataBundle.__post_init__()`

**Context**: Need to ensure DataFrames have correct structure before use

**Options Considered**:
1. Validate in __post_init__() (eager validation)
2. Validate on first access (lazy validation)
3. Make validation optional (controlled by config)
4. Validate only strategy DataFrame, skip optional ones

**Rationale**:
- Chose Option 1 for fail-fast behavior
- Better to catch errors early than during execution

**Implications**:
- ✅ Catches structural issues immediately
- ✅ Clear error messages at load time
- ❌ **Performance regression: +31%**

**Revision Needed**: Yes - Session 2 will optimize this

**Proposed Fix**:
- Use Option 3: Make validation optional (default=False)
- Only validate when in "debug" mode
- Or use Option 4: Only validate strategy DataFrame

---
```

---

## 📋 HANDOFF INSTRUCTIONS FOR TOMORROW

### **Step 1: Start New Chat with This Context**

**Paste this at the start of your next session:**
```
I'm continuing the WBWSStrategy migration project from Session 1.

CONTEXT:
- Project: Migrating WBWSStrategy from dict-based to typed contracts
- Phase: Phase 1 - Data Layer Migration
- Status: DataLoader_v2 implemented but has performance regression

COMPLETED IN SESSION 1:
- ✅ All contracts defined (DataConfig, DataBundle, SignalFrame, etc.)
- ✅ DataLoader_v2 implemented and tested
- ✅ DataFrame parity: 100% match with old DataLoader
- ✅ Metadata parity: 100% match with old DataLoader

CURRENT ISSUE:
Performance regression in DataLoader_v2:
- Old DataLoader: 746.8 ms
- New DataLoader: 977.9 ms  
- Regression: +30.9% (threshold is ≤110%, we're at 131%)
- Failing test: tests/migration/test_dataloader_parity.py

HYPOTHESIS:
The performance issue is caused by DataFrame validation in DataBundle.__post_init__().
The validation checks 4 DataFrames (full, strategy, htf, ltf) on every load.

TASK FOR THIS SESSION:
1. Profile DataLoader_v2 to confirm bottleneck
2. Optimize validation (make it optional or lazy)
3. Re-run test and achieve ≤821ms (110% of 747ms baseline)
4. Once performance test passes, proceed to Phase 2 (SignalGenerator)

FILES INVOLVED:
- src/strategies/specific/modules/data_loader.py (to optimize)
- src/strategies/contracts/data_contracts.py (DataBundle class)
- tests/migration/test_dataloader_parity.py (validation test)

Please help me profile and fix the performance regression.