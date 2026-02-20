# SESSION 20 — IMPLEMENTATION PLAN
**Date**: 2026-02-19 | **Phase**: 8 — Hardening & Polish  
**Status**: ✅ CLOSED — All blocks complete as of 2026-02-20  
**Successor**: See `SESSION_21_HANDOFF.md`

---

## Completion Summary

| Block | Focus | Status | Notes |
|-------|-------|--------|-------|
| A | Global rename `"debug"` → `"analytics"` | ✅ | Applied across all new-arch files; migration guard in DataLoader and SignalGenerator |
| B | Delete all legacy adapters | ✅ | 5 methods/classes removed across 5 files |
| C | Create `strategy_template.yaml` | ✅ | `configs/strategies/strategy_template.yaml` created; `max_risk_percentile` validation fixed (0–5.0) |
| D | Fix filter pipeline: logging + cache key | ✅ | Logging gated on analytics mode; filter config fingerprint added to cache key |
| E | Fix core mode performance (TradeSimulator) | ✅ | LTF, progressive tracking, signal ID lookups gated on analytics mode |
| F | Add caching (RiskManager + SpreadManager) | ✅ | Class-level ATR cache and spread config cache added |
| G | Fix config validation + freeze all contracts | ✅ | All new-arch config dataclasses frozen; `SignalFrame.__iter__` guard added |
| H | Performance optimisations (filters) | ✅ | `count_by_type()` replaced in hot paths; unused Bollinger arrays removed |
| I | ReportGenerator polish | ✅ | `brand_name`, None guard, stale `__main__` block removed |
| J | Write ~30 new tests | ✅ | 19 new tests added to `test_config_schema.py` targeting new-arch modules |
| K | Update architecture docs | ✅ | `ARCHITECTURE.md` rewritten as production reference (v2.2.0) |

**Files delivered (Session 20)**: 28 source/config files + `ARCHITECTURE.md` + `test_config_schema.py`  
**Test count at close**: ~302  
**Performance at close**: Core mode inversion resolved; core mode materially faster than analytics mode.

---

## Carry-Forward to Session 21

The following items were deferred out of scope for Session 20 and are the opening agenda for Session 21:

1. **Performance regression baseline**: lock post-Session-20 numbers as the new non-regression floor.
2. **P2 observability**: `AnalyticsConfig` contract, per-stage timing in `AnalyticsReport`, cache statistics in `MetricsReport`.
3. **`wbws_strategy_v2.yaml`**: WBWS-specific config derived from `strategy_template.yaml`.
4. **P1-CH3-8** (`TimeFilter` typed parameters): coordinated change with `filter_pipeline.py`, deferred from Block H.

---

*This document is closed. Do not add to it. Append new decisions to `DECISION_LOG.md`.*