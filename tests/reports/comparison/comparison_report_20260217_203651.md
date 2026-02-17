# LEGACY VS NEW ARCHITECTURE TEST REPORT
**Date:** 2026-02-17 20:36:51
**Tolerance:** +/-0.1

## OVERALL STATUS
**Status:** FAILED

## NEW ARCHITECTURE ERRORS
### CORE COLD
**Error:** New architecture execution failed: 'int' object has no attribute 'upper'
```
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 504, in execute
    trade_result = trade_sim.simulate_trades(
        filtered_signals=string_signals,  # Now passing strings, not int8
        df_strategy=data_bundle.strategy,
        df_ltf=data_bundle.ltf
    )
  File "E:\Trading\Backtest_platform\src\strategies\specific\modules\trade_simulator.py", line 602, in simulate_trades
    result = tm.handle_signal(
        timestamp=timestamp,
...
```
### CORE HOT
**Error:** New architecture execution failed: 'int' object has no attribute 'upper'
```
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 504, in execute
    trade_result = trade_sim.simulate_trades(
        filtered_signals=string_signals,  # Now passing strings, not int8
        df_strategy=data_bundle.strategy,
        df_ltf=data_bundle.ltf
    )
  File "E:\Trading\Backtest_platform\src\strategies\specific\modules\trade_simulator.py", line 602, in simulate_trades
    result = tm.handle_signal(
        timestamp=timestamp,
...
```
### DEBUG COLD
**Error:** New architecture execution failed: 'int' object has no attribute 'upper'
```
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 504, in execute
    trade_result = trade_sim.simulate_trades(
        filtered_signals=string_signals,  # Now passing strings, not int8
        df_strategy=data_bundle.strategy,
        df_ltf=data_bundle.ltf
    )
  File "E:\Trading\Backtest_platform\src\strategies\specific\modules\trade_simulator.py", line 602, in simulate_trades
    result = tm.handle_signal(
        timestamp=timestamp,
...
```
### DEBUG HOT
**Error:** New architecture execution failed: 'int' object has no attribute 'upper'
```
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 504, in execute
    trade_result = trade_sim.simulate_trades(
        filtered_signals=string_signals,  # Now passing strings, not int8
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
      LEGACY  CORE COLD    13.297        0.066        0.094      31.431        0.058     44.962     N/A
      LEGACY  CORE  HOT     1.599        0.045        0.069      33.044        0.052     34.816   100.0
      LEGACY DEBUG COLD    18.011        1.652        0.106      33.934        0.122     54.501     N/A
      LEGACY DEBUG  HOT     1.583        1.450        0.069      29.199        0.064     33.001   100.0
```