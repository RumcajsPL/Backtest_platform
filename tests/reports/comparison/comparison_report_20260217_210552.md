# LEGACY VS NEW ARCHITECTURE TEST REPORT
**Date:** 2026-02-17 21:05:52
**Tolerance:** +/-0.1

## OVERALL STATUS
**Status:** ❌ FAILED

## 📊 ARTF DATA STATUS
- **New CORE**: ✅ Loaded
- **New DEBUG**: ✅ Loaded
- **Legacy CORE**: ❌ Missing
- **Legacy DEBUG**: ❌ Missing

## 🔄 PARITY VALIDATION
### CORE Mode - ❌
- **13 mismatches detected**
  - data: 8 mismatches
  - filters: 5 mismatches
### Detailed Mismatches:
- data.full_bars: Missing in Legacy (New=702488)
- data.htf: Missing in New (Legacy=1548)
- data.htf_bars: Missing in Legacy (New=1548)
- data.ltf: Missing in New (Legacy=2057478)
- data.strategy: Missing in New (Legacy=88194)
- data.ltf_bars: Missing in Legacy (New=2057478)
- data.strategy_bars: Missing in Legacy (New=88194)
- data.full: Missing in New (Legacy=702488)
- filters.final_buy: Missing in Legacy (New=2737)
- filters.time_filtered: Missing in Legacy (New=5437)
  - ... and 3 more mismatches

### DEBUG Mode - ❌
- **13 mismatches detected**
  - data: 8 mismatches
  - filters: 5 mismatches
### Detailed Mismatches:
- data.full_bars: Missing in Legacy (New=702488)
- data.htf: Missing in New (Legacy=1548)
- data.htf_bars: Missing in Legacy (New=1548)
- data.ltf: Missing in New (Legacy=2057478)
- data.strategy: Missing in New (Legacy=88194)
- data.ltf_bars: Missing in Legacy (New=2057478)
- data.strategy_bars: Missing in Legacy (New=88194)
- data.full: Missing in New (Legacy=702488)
- filters.final_buy: Missing in Legacy (New=2737)
- filters.time_filtered: Missing in Legacy (New=5437)
  - ... and 3 more mismatches

## ⚡ PERFORMANCE VALIDATION
*(New should be faster than Legacy)*

### CORE Mode - ✅
  - 🚀 data_loading: New 0.914s vs Legacy 1.805s (49.4% faster)
  - 🚀 signal_generation: New 0.031s vs Legacy 0.048s (35.2% faster)
  - 🚀 filter_application: New 0.057s vs Legacy 0.072s (20.5% faster)
  - 🚀 trade_simulation: New 14.776s vs Legacy 33.149s (55.4% faster)
  - 🚀 end_to_end: New 15.778s vs Legacy 35.131s (55.1% faster)

### DEBUG Mode - ❌
(2.7% slower)
  - 🚀 signal_generation: New 0.072s vs Legacy 1.509s (95.3% faster)
  - 🚀 filter_application: New 0.058s vs Legacy 0.065s (11.3% faster)
  - 🚀 trade_simulation: New 13.294s vs Legacy 31.281s (57.5% faster)
  - 🚀 end_to_end: New 14.901s vs Legacy 35.101s (57.5% faster)

## 🎯 CORE VS DEBUG VALIDATION
*(Core should be faster than Debug)*

### Result - ❌
  - 🚀 data_loading: Core 0.914s vs Debug 1.477s (38.1% slower in debug)
  - 🚀 signal_generation: Core 0.031s vs Debug 0.072s (56.6% slower in debug)
  - 🚀 filter_application: Core 0.057s vs Debug 0.058s (0.7% slower in debug)
  - 🐢 trade_simulation: Core 14.776s vs Debug 13.294s (-11.1% slower in debug)
  - 🚀 end_to_end: Core 15.778s vs Debug 14.901s (-5.9% slower in debug)

## 📈 PERFORMANCE DETAILS

```
Architecture  Mode  Run  Data (s)  Signals (s)  Filters (s)  Trades (s)  Metrics (s)  Total (s) Cache % ARTF
      LEGACY  CORE COLD    17.308        0.061        0.141      32.244        0.053     49.818     N/A    ✅
         NEW  CORE COLD     7.324        0.035        0.050      16.328        0.000     23.736    50.0    ✅
      LEGACY  CORE  HOT     1.805        0.048        0.072      33.149        0.050     35.131   100.0    ❌
         NEW  CORE  HOT     0.914        0.031        0.057      14.776        0.000     15.778    50.0    ✅
      LEGACY DEBUG COLD    12.645        1.514        0.069      29.162        0.102     44.182     N/A    ✅
         NEW DEBUG COLD     6.715        0.048        0.058      13.258        0.000     20.079    50.0    ✅
      LEGACY DEBUG  HOT     1.518        1.509        0.065      31.281        0.063     35.101   100.0    ❌
         NEW DEBUG  HOT     1.477        0.072        0.058      13.294        0.000     14.901    50.0    ✅
```