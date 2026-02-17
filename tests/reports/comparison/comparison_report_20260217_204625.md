# LEGACY VS NEW ARCHITECTURE TEST REPORT
**Date:** 2026-02-17 20:46:25
**Tolerance:** +/-0.1

## OVERALL STATUS
**Status:** FAILED

## PARITY VALIDATION
*(Statistics should match between Legacy and New)*

### CORE Mode - FAIL
- **13 mismatches detected:**
  - `parity.data.htf: missing in New`
  - `parity.data.strategy_bars: missing in Legacy`
  - `parity.data.strategy: missing in New`
  - `parity.data.full: missing in New`
  - `parity.data.ltf: missing in New`
  - ... and 8 more
  - Data: FAIL
  - Signals: PASS
  - Filters: FAIL
  - Trades: PASS

### DEBUG Mode - FAIL
- **13 mismatches detected:**
  - `parity.data.htf: missing in New`
  - `parity.data.strategy_bars: missing in Legacy`
  - `parity.data.strategy: missing in New`
  - `parity.data.full: missing in New`
  - `parity.data.ltf: missing in New`
  - ... and 8 more
  - Data: FAIL
  - Signals: PASS
  - Filters: FAIL
  - Trades: PASS

## PERFORMANCE VALIDATION
*(Legacy should be slower than New)*

### CORE Mode - PASS
  - FASTER: data_loading - Legacy 1.567s vs New 0.859s (1.82x)
  - FASTER: signal_generation - Legacy 0.065s vs New 0.031s (2.08x)
  - FASTER: filter_application - Legacy 0.092s vs New 0.052s (1.77x)
  - FASTER: trade_simulation - Legacy 26.208s vs New 12.666s (2.07x)
  - FASTER: end_to_end - Legacy 27.993s vs New 13.608s (2.06x)

### DEBUG Mode - FAIL
  - SLOWER: data_loading - Legacy 1.463s vs New 1.681s (0.87x)
  - FASTER: signal_generation - Legacy 1.410s vs New 0.039s (35.71x)
  - FASTER: filter_application - Legacy 0.073s vs New 0.043s (1.70x)
  - FASTER: trade_simulation - Legacy 27.632s vs New 12.286s (2.25x)
  - FASTER: end_to_end - Legacy 31.294s vs New 14.049s (2.23x)

## CORE VS DEBUG VALIDATION
*(Core should be faster than Debug)*

### Result - FAIL
  - FASTER: data_loading - Core 0.859s vs Debug 1.681s (1.96x)
  - FASTER: signal_generation - Core 0.031s vs Debug 0.039s (1.27x)
  - SLOWER: filter_application - Core 0.052s vs Debug 0.043s (0.82x)
  - FASTER: trade_simulation - Core 12.666s vs Debug 12.286s (0.97x)
  - FASTER: end_to_end - Core 13.608s vs Debug 14.049s (1.03x)

## PERFORMANCE DETAILS

```
Architecture  Mode  Run  Data (s)  Signals (s)  Filters (s)  Trades (s)  Metrics (s)  Total (s) Cache %
      LEGACY  CORE COLD    13.221        0.077        0.127      36.335        0.049     49.815     N/A
         NEW  CORE COLD     6.432        0.029        0.060      13.690        0.000     20.212    50.0
      LEGACY  CORE  HOT     1.567        0.065        0.092      26.208        0.054     27.993   100.0
         NEW  CORE  HOT     0.859        0.031        0.052      12.666        0.000     13.608    50.0
      LEGACY DEBUG COLD    12.025        1.480        0.067      26.932        0.116     41.269     N/A
         NEW DEBUG COLD     6.466        0.046        0.062      12.388        0.000     18.962    50.0
      LEGACY DEBUG  HOT     1.463        1.410        0.073      27.632        0.063     31.294   100.0
         NEW DEBUG  HOT     1.681        0.039        0.043      12.286        0.000     14.049    50.0
```