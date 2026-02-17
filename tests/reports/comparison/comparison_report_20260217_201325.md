# LEGACY VS NEW ARCHITECTURE TEST REPORT
**Date:** 2026-02-17 20:13:25
**Tolerance:** +/-0.1

## OVERALL STATUS
**Status:** FAILED

## NEW ARCHITECTURE ERRORS
### CORE COLD
**Error:** New architecture execution failed: Can only use the '.list' accessor with 'list[pyarrow]' dtype, not int8.
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 418, in execute
    self._debug_print("Extracted signals Series", final_signals_series)
    ~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 328, in _debug_print
    public_attrs = [a for a in dir(obj) if not a.startswith('_') and not callable(getattr(obj, a))]
                                                                                  ~~~~~~~^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\generic.py", line 6321, in __getattr__
    return object.__getattribute__(self, name)
           ~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\accessor.py", line 224, in __get__
    accessor_obj = self._accessor(obj)
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 73, in __init__
    super().__init__(
    ~~~~~~~~~~~~~~~~^
        data,
        ^^^^^
        validation_msg="Can only use the '.list' accessor with "
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        "'list[pyarrow]' dtype, not {dtype}.",
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 41, in __init__
    self._validate(data)
    ~~~~~~~~~~~~~~^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 51, in _validate
    raise AttributeError(self._validation_msg.format(dtype=dtype))
AttributeError: Can only use the '.list' accessor with 'list[pyarrow]' dtype, not int8.. Did you mean: 'hist'?

```
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 418, in execute
    self._debug_print("Extracted signals Series", final_signals_series)
    ~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 328, in _debug_print
    public_attrs = [a for a in dir(obj) if not a.startswith('_') and not callable(getattr(obj, a))]
                                                                                  ~~~~~~~^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\generic.py", line 6321, in __getattr__
    return object.__getattribute__(self, name)
           ~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\accessor.py", line 224, in __get__
    accessor_obj = self._accessor(obj)
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 73, in __init__
    super().__init__(
    ~~~~~~~~~~~~~~~~^
        data,
        ^^^^^
        validation_msg="Can only use the '.list' accessor with "
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        "'list[pyarrow]' dtype, not {dtype}.",
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 41, in __init__
    self._validate(data)
    ~~~~~~~~~~~~~~^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 51, in _validate
    raise AttributeError(self._validation_msg.format(dtype=dtype))
AttributeError: Can only use the '.list' accessor with 'list[pyarrow]' dtype, not int8.. Did you mean: 'hist'?

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 682, in run_new
    timings, stats, cache_hit_rate, warnings = executor.execute()
                                               ~~~~~~~~~~~~~~~~^^
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 469, in execute
    raise RuntimeError(f"New architecture execution failed: {e}\n{tb}") from e
RuntimeError: New architecture execution failed: Can only use the '.list' accessor with 'list[pyarrow]' dtype, not int8.
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 418, in execute
    self._debug_print("Extracted signals Series", final_signals_series)
    ~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 328, in _debug_print
    public_attrs = [a for a in dir(obj) if not a.startswith('_') and not callable(getattr(obj, a))]
                                                                                  ~~~~~~~^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\generic.py", line 6321, in __getattr__
    return object.__getattribute__(self, name)
           ~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\accessor.py", line 224, in __get__
    accessor_obj = self._accessor(obj)
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 73, in __init__
    super().__init__(
    ~~~~~~~~~~~~~~~~^
        data,
        ^^^^^
        validation_msg="Can only use the '.list' accessor with "
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        "'list[pyarrow]' dtype, not {dtype}.",
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 41, in __init__
    self._validate(data)
    ~~~~~~~~~~~~~~^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 51, in _validate
    raise AttributeError(self._validation_msg.format(dtype=dtype))
AttributeError: Can only use the '.list' accessor with 'list[pyarrow]' dtype, not int8.. Did you mean: 'hist'?


```
### CORE HOT
**Error:** New architecture execution failed: Can only use the '.list' accessor with 'list[pyarrow]' dtype, not int8.
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 418, in execute
    self._debug_print("Extracted signals Series", final_signals_series)
    ~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 328, in _debug_print
    public_attrs = [a for a in dir(obj) if not a.startswith('_') and not callable(getattr(obj, a))]
                                                                                  ~~~~~~~^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\generic.py", line 6321, in __getattr__
    return object.__getattribute__(self, name)
           ~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\accessor.py", line 224, in __get__
    accessor_obj = self._accessor(obj)
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 73, in __init__
    super().__init__(
    ~~~~~~~~~~~~~~~~^
        data,
        ^^^^^
        validation_msg="Can only use the '.list' accessor with "
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        "'list[pyarrow]' dtype, not {dtype}.",
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 41, in __init__
    self._validate(data)
    ~~~~~~~~~~~~~~^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 51, in _validate
    raise AttributeError(self._validation_msg.format(dtype=dtype))
AttributeError: Can only use the '.list' accessor with 'list[pyarrow]' dtype, not int8.. Did you mean: 'hist'?

```
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 418, in execute
    self._debug_print("Extracted signals Series", final_signals_series)
    ~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 328, in _debug_print
    public_attrs = [a for a in dir(obj) if not a.startswith('_') and not callable(getattr(obj, a))]
                                                                                  ~~~~~~~^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\generic.py", line 6321, in __getattr__
    return object.__getattribute__(self, name)
           ~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\accessor.py", line 224, in __get__
    accessor_obj = self._accessor(obj)
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 73, in __init__
    super().__init__(
    ~~~~~~~~~~~~~~~~^
        data,
        ^^^^^
        validation_msg="Can only use the '.list' accessor with "
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        "'list[pyarrow]' dtype, not {dtype}.",
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 41, in __init__
    self._validate(data)
    ~~~~~~~~~~~~~~^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 51, in _validate
    raise AttributeError(self._validation_msg.format(dtype=dtype))
AttributeError: Can only use the '.list' accessor with 'list[pyarrow]' dtype, not int8.. Did you mean: 'hist'?

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 682, in run_new
    timings, stats, cache_hit_rate, warnings = executor.execute()
                                               ~~~~~~~~~~~~~~~~^^
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 469, in execute
    raise RuntimeError(f"New architecture execution failed: {e}\n{tb}") from e
RuntimeError: New architecture execution failed: Can only use the '.list' accessor with 'list[pyarrow]' dtype, not int8.
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 418, in execute
    self._debug_print("Extracted signals Series", final_signals_series)
    ~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 328, in _debug_print
    public_attrs = [a for a in dir(obj) if not a.startswith('_') and not callable(getattr(obj, a))]
                                                                                  ~~~~~~~^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\generic.py", line 6321, in __getattr__
    return object.__getattribute__(self, name)
           ~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\accessor.py", line 224, in __get__
    accessor_obj = self._accessor(obj)
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 73, in __init__
    super().__init__(
    ~~~~~~~~~~~~~~~~^
        data,
        ^^^^^
        validation_msg="Can only use the '.list' accessor with "
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        "'list[pyarrow]' dtype, not {dtype}.",
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 41, in __init__
    self._validate(data)
    ~~~~~~~~~~~~~~^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 51, in _validate
    raise AttributeError(self._validation_msg.format(dtype=dtype))
AttributeError: Can only use the '.list' accessor with 'list[pyarrow]' dtype, not int8.. Did you mean: 'hist'?


```
### DEBUG COLD
**Error:** New architecture execution failed: Can only use the '.list' accessor with 'list[pyarrow]' dtype, not int8.
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 418, in execute
    self._debug_print("Extracted signals Series", final_signals_series)
    ~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 328, in _debug_print
    public_attrs = [a for a in dir(obj) if not a.startswith('_') and not callable(getattr(obj, a))]
                                                                                  ~~~~~~~^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\generic.py", line 6321, in __getattr__
    return object.__getattribute__(self, name)
           ~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\accessor.py", line 224, in __get__
    accessor_obj = self._accessor(obj)
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 73, in __init__
    super().__init__(
    ~~~~~~~~~~~~~~~~^
        data,
        ^^^^^
        validation_msg="Can only use the '.list' accessor with "
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        "'list[pyarrow]' dtype, not {dtype}.",
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 41, in __init__
    self._validate(data)
    ~~~~~~~~~~~~~~^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 51, in _validate
    raise AttributeError(self._validation_msg.format(dtype=dtype))
AttributeError: Can only use the '.list' accessor with 'list[pyarrow]' dtype, not int8.. Did you mean: 'hist'?

```
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 418, in execute
    self._debug_print("Extracted signals Series", final_signals_series)
    ~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 328, in _debug_print
    public_attrs = [a for a in dir(obj) if not a.startswith('_') and not callable(getattr(obj, a))]
                                                                                  ~~~~~~~^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\generic.py", line 6321, in __getattr__
    return object.__getattribute__(self, name)
           ~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\accessor.py", line 224, in __get__
    accessor_obj = self._accessor(obj)
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 73, in __init__
    super().__init__(
    ~~~~~~~~~~~~~~~~^
        data,
        ^^^^^
        validation_msg="Can only use the '.list' accessor with "
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        "'list[pyarrow]' dtype, not {dtype}.",
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 41, in __init__
    self._validate(data)
    ~~~~~~~~~~~~~~^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 51, in _validate
    raise AttributeError(self._validation_msg.format(dtype=dtype))
AttributeError: Can only use the '.list' accessor with 'list[pyarrow]' dtype, not int8.. Did you mean: 'hist'?

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 682, in run_new
    timings, stats, cache_hit_rate, warnings = executor.execute()
                                               ~~~~~~~~~~~~~~~~^^
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 469, in execute
    raise RuntimeError(f"New architecture execution failed: {e}\n{tb}") from e
RuntimeError: New architecture execution failed: Can only use the '.list' accessor with 'list[pyarrow]' dtype, not int8.
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 418, in execute
    self._debug_print("Extracted signals Series", final_signals_series)
    ~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 328, in _debug_print
    public_attrs = [a for a in dir(obj) if not a.startswith('_') and not callable(getattr(obj, a))]
                                                                                  ~~~~~~~^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\generic.py", line 6321, in __getattr__
    return object.__getattribute__(self, name)
           ~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\accessor.py", line 224, in __get__
    accessor_obj = self._accessor(obj)
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 73, in __init__
    super().__init__(
    ~~~~~~~~~~~~~~~~^
        data,
        ^^^^^
        validation_msg="Can only use the '.list' accessor with "
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        "'list[pyarrow]' dtype, not {dtype}.",
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 41, in __init__
    self._validate(data)
    ~~~~~~~~~~~~~~^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 51, in _validate
    raise AttributeError(self._validation_msg.format(dtype=dtype))
AttributeError: Can only use the '.list' accessor with 'list[pyarrow]' dtype, not int8.. Did you mean: 'hist'?


```
### DEBUG HOT
**Error:** New architecture execution failed: Can only use the '.list' accessor with 'list[pyarrow]' dtype, not int8.
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 418, in execute
    self._debug_print("Extracted signals Series", final_signals_series)
    ~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 328, in _debug_print
    public_attrs = [a for a in dir(obj) if not a.startswith('_') and not callable(getattr(obj, a))]
                                                                                  ~~~~~~~^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\generic.py", line 6321, in __getattr__
    return object.__getattribute__(self, name)
           ~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\accessor.py", line 224, in __get__
    accessor_obj = self._accessor(obj)
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 73, in __init__
    super().__init__(
    ~~~~~~~~~~~~~~~~^
        data,
        ^^^^^
        validation_msg="Can only use the '.list' accessor with "
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        "'list[pyarrow]' dtype, not {dtype}.",
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 41, in __init__
    self._validate(data)
    ~~~~~~~~~~~~~~^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 51, in _validate
    raise AttributeError(self._validation_msg.format(dtype=dtype))
AttributeError: Can only use the '.list' accessor with 'list[pyarrow]' dtype, not int8.. Did you mean: 'hist'?

```
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 418, in execute
    self._debug_print("Extracted signals Series", final_signals_series)
    ~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 328, in _debug_print
    public_attrs = [a for a in dir(obj) if not a.startswith('_') and not callable(getattr(obj, a))]
                                                                                  ~~~~~~~^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\generic.py", line 6321, in __getattr__
    return object.__getattribute__(self, name)
           ~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\accessor.py", line 224, in __get__
    accessor_obj = self._accessor(obj)
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 73, in __init__
    super().__init__(
    ~~~~~~~~~~~~~~~~^
        data,
        ^^^^^
        validation_msg="Can only use the '.list' accessor with "
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        "'list[pyarrow]' dtype, not {dtype}.",
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 41, in __init__
    self._validate(data)
    ~~~~~~~~~~~~~~^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 51, in _validate
    raise AttributeError(self._validation_msg.format(dtype=dtype))
AttributeError: Can only use the '.list' accessor with 'list[pyarrow]' dtype, not int8.. Did you mean: 'hist'?

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 682, in run_new
    timings, stats, cache_hit_rate, warnings = executor.execute()
                                               ~~~~~~~~~~~~~~~~^^
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 469, in execute
    raise RuntimeError(f"New architecture execution failed: {e}\n{tb}") from e
RuntimeError: New architecture execution failed: Can only use the '.list' accessor with 'list[pyarrow]' dtype, not int8.
Traceback (most recent call last):
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 418, in execute
    self._debug_print("Extracted signals Series", final_signals_series)
    ~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "e:\Trading\Backtest_platform\tests\test_legacy_vs_new.py", line 328, in _debug_print
    public_attrs = [a for a in dir(obj) if not a.startswith('_') and not callable(getattr(obj, a))]
                                                                                  ~~~~~~~^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\generic.py", line 6321, in __getattr__
    return object.__getattribute__(self, name)
           ~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\accessor.py", line 224, in __get__
    accessor_obj = self._accessor(obj)
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 73, in __init__
    super().__init__(
    ~~~~~~~~~~~~~~~~^
        data,
        ^^^^^
        validation_msg="Can only use the '.list' accessor with "
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        "'list[pyarrow]' dtype, not {dtype}.",
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 41, in __init__
    self._validate(data)
    ~~~~~~~~~~~~~~^^^^^^
  File "E:\Trading\Backtest_platform\venv\Lib\site-packages\pandas\core\arrays\arrow\accessors.py", line 51, in _validate
    raise AttributeError(self._validation_msg.format(dtype=dtype))
AttributeError: Can only use the '.list' accessor with 'list[pyarrow]' dtype, not int8.. Did you mean: 'hist'?


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
      LEGACY  CORE COLD    12.995        0.069        0.111      28.920        0.054     42.158     N/A
      LEGACY  CORE  HOT     1.532        0.046        0.071      29.338        0.054     31.048   100.0
      LEGACY DEBUG COLD    13.507        1.956        0.091      32.132        0.129     48.516     N/A
      LEGACY DEBUG  HOT     1.569        1.430        0.074      30.479        0.062     34.272   100.0
```