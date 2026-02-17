# LEGACY VS NEW ARCHITECTURE TEST REPORT
**Date:** 2026-02-17 19:57:09
**Tolerance:** +/-0.1

## OVERALL STATUS
**Status:** FAILED

## NEW ARCHITECTURE ERRORS
### CORE COLD
**Error:** New architecture execution failed: 'SignalFrame' object has no attribute 'index'
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 406, in execute
    trade_result = trade_sim.simulate_trades(
        filtered_signals=final_signals,
        df_strategy=data_bundle.strategy,
        df_ltf=data_bundle.ltf
    )
  File "E:\Trading\Backtest_platform\src\strategies\specific\modules\trade_simulator.py", line 534, in simulate_trades
    if timestamp not in filtered_signals.index or pd.isna(filtered_signals[timestamp]):
                        ^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'SignalFrame' object has no attribute 'index'

```
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 406, in execute
    trade_result = trade_sim.simulate_trades(
        filtered_signals=final_signals,
        df_strategy=data_bundle.strategy,
        df_ltf=data_bundle.ltf
    )
  File "E:\Trading\Backtest_platform\src\strategies\specific\modules\trade_simulator.py", line 534, in simulate_trades
    if timestamp not in filtered_signals.index or pd.isna(filtered_signals[timestamp]):
                        ^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'SignalFrame' object has no attribute 'index'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 681, in run_new
    timings, stats, cache_hit_rate, warnings = executor.execute()
                                               ~~~~~~~~~~~~~~~~^^
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 450, in execute
    raise RuntimeError(f"New architecture execution failed: {e}\n{tb}") from e
RuntimeError: New architecture execution failed: 'SignalFrame' object has no attribute 'index'
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 406, in execute
    trade_result = trade_sim.simulate_trades(
        filtered_signals=final_signals,
        df_strategy=data_bundle.strategy,
        df_ltf=data_bundle.ltf
    )
  File "E:\Trading\Backtest_platform\src\strategies\specific\modules\trade_simulator.py", line 534, in simulate_trades
    if timestamp not in filtered_signals.index or pd.isna(filtered_signals[timestamp]):
                        ^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'SignalFrame' object has no attribute 'index'


```
### CORE HOT
**Error:** New architecture execution failed: 'SignalFrame' object has no attribute 'index'
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 406, in execute
    trade_result = trade_sim.simulate_trades(
        filtered_signals=final_signals,
        df_strategy=data_bundle.strategy,
        df_ltf=data_bundle.ltf
    )
  File "E:\Trading\Backtest_platform\src\strategies\specific\modules\trade_simulator.py", line 534, in simulate_trades
    if timestamp not in filtered_signals.index or pd.isna(filtered_signals[timestamp]):
                        ^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'SignalFrame' object has no attribute 'index'

```
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 406, in execute
    trade_result = trade_sim.simulate_trades(
        filtered_signals=final_signals,
        df_strategy=data_bundle.strategy,
        df_ltf=data_bundle.ltf
    )
  File "E:\Trading\Backtest_platform\src\strategies\specific\modules\trade_simulator.py", line 534, in simulate_trades
    if timestamp not in filtered_signals.index or pd.isna(filtered_signals[timestamp]):
                        ^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'SignalFrame' object has no attribute 'index'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 681, in run_new
    timings, stats, cache_hit_rate, warnings = executor.execute()
                                               ~~~~~~~~~~~~~~~~^^
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 450, in execute
    raise RuntimeError(f"New architecture execution failed: {e}\n{tb}") from e
RuntimeError: New architecture execution failed: 'SignalFrame' object has no attribute 'index'
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 406, in execute
    trade_result = trade_sim.simulate_trades(
        filtered_signals=final_signals,
        df_strategy=data_bundle.strategy,
        df_ltf=data_bundle.ltf
    )
  File "E:\Trading\Backtest_platform\src\strategies\specific\modules\trade_simulator.py", line 534, in simulate_trades
    if timestamp not in filtered_signals.index or pd.isna(filtered_signals[timestamp]):
                        ^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'SignalFrame' object has no attribute 'index'


```
### DEBUG COLD
**Error:** New architecture execution failed: 'SignalFrame' object has no attribute 'index'
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 406, in execute
    trade_result = trade_sim.simulate_trades(
        filtered_signals=final_signals,
        df_strategy=data_bundle.strategy,
        df_ltf=data_bundle.ltf
    )
  File "E:\Trading\Backtest_platform\src\strategies\specific\modules\trade_simulator.py", line 534, in simulate_trades
    if timestamp not in filtered_signals.index or pd.isna(filtered_signals[timestamp]):
                        ^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'SignalFrame' object has no attribute 'index'

```
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 406, in execute
    trade_result = trade_sim.simulate_trades(
        filtered_signals=final_signals,
        df_strategy=data_bundle.strategy,
        df_ltf=data_bundle.ltf
    )
  File "E:\Trading\Backtest_platform\src\strategies\specific\modules\trade_simulator.py", line 534, in simulate_trades
    if timestamp not in filtered_signals.index or pd.isna(filtered_signals[timestamp]):
                        ^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'SignalFrame' object has no attribute 'index'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 681, in run_new
    timings, stats, cache_hit_rate, warnings = executor.execute()
                                               ~~~~~~~~~~~~~~~~^^
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 450, in execute
    raise RuntimeError(f"New architecture execution failed: {e}\n{tb}") from e
RuntimeError: New architecture execution failed: 'SignalFrame' object has no attribute 'index'
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 406, in execute
    trade_result = trade_sim.simulate_trades(
        filtered_signals=final_signals,
        df_strategy=data_bundle.strategy,
        df_ltf=data_bundle.ltf
    )
  File "E:\Trading\Backtest_platform\src\strategies\specific\modules\trade_simulator.py", line 534, in simulate_trades
    if timestamp not in filtered_signals.index or pd.isna(filtered_signals[timestamp]):
                        ^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'SignalFrame' object has no attribute 'index'


```
### DEBUG HOT
**Error:** New architecture execution failed: 'SignalFrame' object has no attribute 'index'
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 406, in execute
    trade_result = trade_sim.simulate_trades(
        filtered_signals=final_signals,
        df_strategy=data_bundle.strategy,
        df_ltf=data_bundle.ltf
    )
  File "E:\Trading\Backtest_platform\src\strategies\specific\modules\trade_simulator.py", line 534, in simulate_trades
    if timestamp not in filtered_signals.index or pd.isna(filtered_signals[timestamp]):
                        ^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'SignalFrame' object has no attribute 'index'

```
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 406, in execute
    trade_result = trade_sim.simulate_trades(
        filtered_signals=final_signals,
        df_strategy=data_bundle.strategy,
        df_ltf=data_bundle.ltf
    )
  File "E:\Trading\Backtest_platform\src\strategies\specific\modules\trade_simulator.py", line 534, in simulate_trades
    if timestamp not in filtered_signals.index or pd.isna(filtered_signals[timestamp]):
                        ^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'SignalFrame' object has no attribute 'index'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 681, in run_new
    timings, stats, cache_hit_rate, warnings = executor.execute()
                                               ~~~~~~~~~~~~~~~~^^
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 450, in execute
    raise RuntimeError(f"New architecture execution failed: {e}\n{tb}") from e
RuntimeError: New architecture execution failed: 'SignalFrame' object has no attribute 'index'
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 406, in execute
    trade_result = trade_sim.simulate_trades(
        filtered_signals=final_signals,
        df_strategy=data_bundle.strategy,
        df_ltf=data_bundle.ltf
    )
  File "E:\Trading\Backtest_platform\src\strategies\specific\modules\trade_simulator.py", line 534, in simulate_trades
    if timestamp not in filtered_signals.index or pd.isna(filtered_signals[timestamp]):
                        ^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'SignalFrame' object has no attribute 'index'


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
      LEGACY  CORE COLD    12.681        0.051        0.092      31.069        0.061     43.969     N/A
      LEGACY  CORE  HOT     1.493        0.045        0.074      31.976        0.049     33.644   100.0
      LEGACY DEBUG COLD    12.690        1.501        0.090      29.148        0.121     44.194     N/A
      LEGACY DEBUG  HOT     1.502        1.429        0.073      27.805        0.063     31.509   100.0
```