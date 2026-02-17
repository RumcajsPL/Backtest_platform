# LEGACY VS NEW ARCHITECTURE TEST REPORT
**Date:** 2026-02-17 20:23:21
**Tolerance:** +/-0.1

## OVERALL STATUS
**Status:** FAILED

## NEW ARCHITECTURE ERRORS
### CORE COLD
**Error:** New architecture execution failed: 'numpy.int8' object has no attribute 'upper'
```
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 474, in execute
    trade_result = trade_sim.simulate_trades(
        filtered_signals=final_signals_series,  # Now passing Series, not SignalFrame
        df_strategy=data_bundle.strategy,
        df_ltf=data_bundle.ltf
    )
  File "E:\Trading\Backtest_platform\src\strategies\specific\modules\trade_simulator.py", line 602, in simulate_trades
    result = tm.handle_signal(
        timestamp=timestamp,
...
```
### CORE HOT
**Error:** New architecture execution failed: 'numpy.int8' object has no attribute 'upper'
```
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 474, in execute
    trade_result = trade_sim.simulate_trades(
        filtered_signals=final_signals_series,  # Now passing Series, not SignalFrame
        df_strategy=data_bundle.strategy,
        df_ltf=data_bundle.ltf
    )
  File "E:\Trading\Backtest_platform\src\strategies\specific\modules\trade_simulator.py", line 602, in simulate_trades
    result = tm.handle_signal(
        timestamp=timestamp,
...
```
### DEBUG COLD
**Error:** New architecture execution failed: 'numpy.int8' object has no attribute 'upper'
```
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 474, in execute
    trade_result = trade_sim.simulate_trades(
        filtered_signals=final_signals_series,  # Now passing Series, not SignalFrame
        df_strategy=data_bundle.strategy,
        df_ltf=data_bundle.ltf
    )
  File "E:\Trading\Backtest_platform\src\strategies\specific\modules\trade_simulator.py", line 602, in simulate_trades
    result = tm.handle_signal(
        timestamp=timestamp,
...
```
### DEBUG HOT
**Error:** New architecture execution failed: 'numpy.int8' object has no attribute 'upper'
```
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 474, in execute
    trade_result = trade_sim.simulate_trades(
        filtered_signals=final_signals_series,  # Now passing Series, not SignalFrame
        df_strategy=data_bundle.strategy,
        df_ltf=data_bundle.ltf
    )
  File "E:\Trading\Backtest_platform\src\strategies\specific\modules\trade_simulator.py", line 602, in simulate_trades
    result = tm.handle_signal(
        timestamp=timestamp,
...
```

## PARITY VALIDATION
*(Statistics should match between Legacy and New)*

## PERFORMANCE VALIDATION
*(Legacy should be slower than New)*

## CORE VS DEBUG VALIDATION
*(Core should be faster than Debug)*


## PERFORMANCE DETAILS

```
Architecture  Mode  Run  Data (s)  Signals (s)  Filters (s)  Trades (s)  Metrics (s)  Total (s) Cache %
      LEGACY  CORE COLD    12.561        0.057        0.076      33.016        0.073     45.800     N/A
      LEGACY  CORE  HOT     1.527        0.054        0.076      28.852        0.051     30.566   100.0
      LEGACY DEBUG COLD    14.155        1.435        0.091      27.961        0.130     44.421     N/A
      LEGACY DEBUG  HOT     1.680        1.426        0.065      26.923        0.065     30.799   100.0
```