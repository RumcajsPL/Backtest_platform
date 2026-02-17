# SESSION 15 STARTUP CHECKLIST
**For New Chat Window**

## 📋 ESSENTIAL FILES TO PROVIDE

### 1. **SESSION_14_HANDOFF.md** (CRITICAL)
- Complete session summary
- All deliverables overview
- Architecture decisions
- Next steps clear

### 2. **analytics_contracts.py** (850+ lines)
- All 13 contract definitions
- Fully validated (34 tests passing)
- Ready for use

### 3. **trade_analytics.py** (700+ lines)
- Module skeleton with method stubs
- Clear implementation markers
- Type hints complete

### 4. **SESSION_15_16_PLAN.md** (CRITICAL)
- Step-by-step implementation guide
- Time estimates for each task
- Success criteria

### 5. **DECISION_LOG.md** (Optional but helpful)
- All 22 architectural decisions
- Rationale for design choices

### 6. **ARCHITECTURAL_DECISION.md** (Optional but helpful)
- Optional metrics parameter decision
- Usage patterns explained

---

## 📝 CONTEXT TO PROVIDE

### Opening Message Template

```markdown
Hi, we're continuing Session 15 of the WBWSStrategy migration project.

**Current Status**: Session 14 (Design) Complete ✅
**Next Phase**: Session 15 - TradeAnalytics Implementation (Core Analytics)

**What We Completed (Session 14)**:
- ✅ Complete analytics contracts (13 dataclasses, 850+ lines)
- ✅ Module skeleton (trade_analytics.py, 700+ lines)
- ✅ All tests passing (34 tests, 0.49s)
- ✅ Architectural decisions finalized (22 total)
- ✅ Implementation plan detailed

**What We're Building (Session 15)**:
- Time-based performance analysis (sessions/hours/days)
- Trade quality analysis (distributions/durations)
- Insight generation (5-10 actionable insights)
- Auto-calculation of metrics (optional parameter)

**Session 15 Goals** (~4-5 hours):
1. Implement time performance analysis
2. Implement trade quality analysis
3. Test with real data
4. Generate insights

**Files Attached**:
1. SESSION_14_HANDOFF.md - Complete handoff
2. analytics_contracts.py - Contract definitions
3. trade_analytics.py - Module skeleton
4. SESSION_15_16_PLAN.md - Implementation guide

Please review SESSION_14_HANDOFF.md and SESSION_15_16_PLAN.md, then we can start implementation.
```

---

## 🎯 KEY INFORMATION TO MENTION

### Architecture Context
- **Option A Confirmed**: TradeAnalytics aggregates metrics + adds insights
- **Metrics Optional**: Auto-calculates if not provided
- **No Performance Constraints**: Accuracy over speed

### Implementation Focus (Session 15)
1. **Time Performance Analysis** (~2h)
   - Parse timestamps from trades
   - Group by session/hour/day
   - Calculate SessionMetrics for each
   - Generate 3-5 time insights

2. **Trade Quality Analysis** (~2h)
   - Separate wins/losses
   - Calculate distributions (small/medium/large)
   - Analyze durations (fast/normal/prolonged)
   - Generate 2-4 quality insights

3. **Auto-Calculation Feature** (~15min)
   - Implement metrics auto-calculation
   - Support both usage patterns

### Success Criteria
- Time & quality analytics working
- 5-10 insights generated
- Test with real TradeResult
- No crashes on edge cases

---

## 📂 PROJECT STRUCTURE (For Reference)

```
src/strategies/
├── contracts/
│   ├── metrics_contracts.py (Session 13 ✅)
│   └── analytics_contracts.py (Session 14 ✅ - to move)
└── specific/modules/
    ├── metrics_calculator.py (Session 13 ✅)
    └── trade_analytics.py (Session 14 skeleton → Session 15 implement)

tests/migration/
└── test_analytics_contracts.py (34 tests passing ✅)

docs/migration/
├── SESSION_14_HANDOFF.md
├── SESSION_15_16_PLAN.md
├── DECISION_LOG.md
└── CONTRACTS_REFERENCE.md (updated Session 14 ✅)
```

---

## 🚀 FIRST STEPS (Session 15)

1. **Review Handoff** (~5min)
   - Read SESSION_14_HANDOFF.md
   - Understand what was designed

2. **Review Implementation Plan** (~10min)
   - Read SESSION_15_16_PLAN.md
   - Understand step-by-step approach

3. **Move Files to Project** (~5min)
   - Move analytics_contracts.py to src/strategies/contracts/
   - Move trade_analytics.py to src/strategies/specific/modules/
   - Update imports

4. **Implement Auto-Calculation** (~15min)
   - Add MetricsCalculator import
   - Implement metrics=None handling

5. **Implement Time Analysis** (~2h)
   - Follow SESSION_15_16_PLAN.md steps
   - Test incrementally

6. **Implement Quality Analysis** (~2h)
   - Follow SESSION_15_16_PLAN.md steps
   - Test incrementally

7. **Integration Test** (~30min)
   - Test with real data
   - Validate insights

---

## 💡 HELPFUL REMINDERS

### Design Principles (Session 14)
- **AI-like insights**: Recommendations with confidence levels
- **Accuracy over speed**: No performance constraints
- **Markdown primary**: Human-readable executive summary
- **Structured secondary**: JSON for ReportGenerator

### Insight Generation Rules
```python
# CRITICAL (severity="critical", confidence="High")
if session_pnl < -30 and session_trades > 50:
    → "Exclude session X to gain +Xpts"

# WARNING (severity="warning", confidence="Medium")
if fast_exits_pct > 70:
    → "Consider wider stops (73% premature exits)"

# SUCCESS (severity="success", confidence="High")
if session_pnl > total_pnl * 0.6:
    → "Session X is primary driver - maintain focus"
```

### Performance Grading
- Win rate: 0-25 points
- Profit factor: 0-25 points
- Drawdown: 0-25 points
- Consistency: 0-25 points
- Total → Grade (A+ to F)

---

## 📊 WHAT'S ALREADY DONE

✅ Complete contract definitions (all 13 contracts)  
✅ Module skeleton (all methods stubbed)  
✅ All tests passing (34/34)  
✅ Architecture decisions finalized  
✅ Implementation plan detailed  
✅ Type hints complete  
✅ Validation logic implemented  

---

## 🎯 WHAT NEEDS TO BE DONE (Session 15)

⏳ Implement time performance analysis  
⏳ Implement trade quality analysis  
⏳ Implement insight generation  
⏳ Test with real data  
⏳ Validate edge cases  

---

## 📞 EXPECTED QUESTIONS FROM ASSISTANT

The assistant will likely ask:

1. ✅ **"Should I review the handoff?"** → Yes, read SESSION_14_HANDOFF.md
2. ✅ **"Where should files go?"** → Move to src/strategies/contracts/ and modules/
3. ✅ **"What's the implementation order?"** → Follow SESSION_15_16_PLAN.md
4. ✅ **"How to generate insights?"** → See insight rules in plan
5. ✅ **"Need real data for testing?"** → Provide TradeResult when asked

---

## ⚠️ COMMON PITFALLS TO AVOID

1. **Don't create new contracts** - All 13 already defined and tested
2. **Don't skip insight generation** - Core value-add of analytics
3. **Don't optimize for speed** - Accuracy is prioritized
4. **Don't forget validation** - Edge cases (zero trades, all wins, etc.)
5. **Don't duplicate metrics** - Reuse MetricsCalculator

---

## ✅ SESSION 15 SUCCESS = WHEN

- [ ] Time performance analysis working
- [ ] Trade quality analysis working
- [ ] 5-10 insights generated automatically
- [ ] Test with real data passes
- [ ] Edge cases handled gracefully
- [ ] Code follows Session 12+ principles

---

**Prepared By**: Project Manager (Session 14)  
**Date**: 2026-02-16  
**For**: Session 15 Implementation  
**Estimated Duration**: 4-5 hours