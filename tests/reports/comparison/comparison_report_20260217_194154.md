# LEGACY VS NEW ARCHITECTURE TEST REPORT
**Date:** 2026-02-17 19:41:54
**Tolerance:** +/-0.1

## OVERALL STATUS
**Status:** FAILED

## PARITY VALIDATION
*(Statistics should match between Legacy and New)*

## PERFORMANCE VALIDATION
*(Legacy should be slower than New)*

## CORE VS DEBUG VALIDATION
*(Core should be faster than Debug)*


## PERFORMANCE DETAILS

```
Architecture  Mode  Run  Data (s)  Signals (s)  Filters (s)  Trades (s)  Metrics (s)  Total (s) Cache %
      LEGACY  CORE COLD    12.803        0.069        0.114      31.050        0.063     44.120     N/A
      LEGACY  CORE  HOT     1.540        0.047        0.071      27.922        0.056     29.642   100.0
      LEGACY DEBUG COLD    13.666        1.714        0.089      27.133        0.142     43.397     N/A
      LEGACY DEBUG  HOT     1.536        1.430        0.068      29.460        0.061     33.187   100.0
```