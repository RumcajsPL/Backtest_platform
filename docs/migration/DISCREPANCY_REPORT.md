DISCREPANCY REPORT: FilterPipeline Implementation Issues
Report Date: 2026-02-23
Test Suite: test_filter_pipeline.py
Status: 22 passed, 2 failed
Test Runner: strategy_unit_test.py

Issue 1: Time Filter Configuration Not Properly Overridden
Test Case
test_apply_filters_full_pipeline - Line 435

Expected Behavior
When configuring the time filter with:

python
"time_filter": {
    "enabled": True,
    "config": {
        "session_start": {"hour": 0, "minute": 0},
        "session_end": {"hour": 23, "minute": 59},
        "excluded_days": []
    }
}
The filter should accept signals at all hours (00:00-23:59).

Actual Behavior
The time filter is still using the default session hours:

text
session_hours: '08:30–20:30'  # From base_config_dict default
Error Evidence
python
FilterMetadata(
    filter_name='time_filter',
    status=<FilterStatus.REJECTED: 2>,
    signals_in=40,
    signals_out=0,
    signals_rejected=40,
    reason='All signals outside trading hours',
    indicator_values=None,
    execution_time_ms=0.42119997669942677
)
Root Cause Analysis
The test completely replaces the time_filters dictionary, but the actual filter is still using the default hours from base_config_dict. This suggests:

Option A: The TimeFilterConfig is not being properly constructed from the dictionary

Option B: There's a default value somewhere in the filter chain that's overriding the config

Option C: The base_config_dict fixture has a hardcoded default that's being merged instead of replaced

Code Path to Investigate
python
# In filter_pipeline.py - _load_time_filter()
time_filter_cfg = self.config.filters.time_filters.get("time_filter")
if time_filter_cfg is None or not time_filter_cfg.enabled:
    return

# The issue may be in how time_filter_cfg.config is being processed
typed_config = TimeFilterConfig.from_dict({
    "enabled": time_filter_cfg.enabled,
    **time_filter_cfg.config  # <-- Are the session hours being extracted correctly?
})
Severity
HIGH - Affects ability to properly configure time filters in tests and potentially in production.

Issue 2: Filter Configuration Hash Not Parameter-Sensitive
Test Case
test_compute_filter_cfg_hash_changes_with_params - Line 623

Expected Behavior
Two configurations with the same filter name but different parameters should produce different hashes:

python
# Config 1: length=14
"rsi_filter": {
    "enabled": True,
    "length": 14,
    "overbought": 70,
    "oversold": 30
}

# Config 2: length=21
"rsi_filter": {
    "enabled": True,
    "length": 21,  # Different!
    "overbought": 70,
    "oversold": 30
}
Expected: hash1 != hash2

Actual Behavior
Both configurations produce the exact same hash:

text
hash1: '78eaea486202'
hash2: '78eaea486202'
Error Evidence
python
assert pipeline1._filter_cfg_hash != pipeline2._filter_cfg_hash
AssertionError: assert '78eaea486202' != '78eaea486202'
Root Cause Analysis
Looking at the implementation in filter_pipeline.py:

python
@staticmethod
def _compute_filter_cfg_hash(config: StrategyConfig) -> str:
    active = {
        name: fcfg.config  # <-- This should contain the parameters
        for name, fcfg in config.filters.technical_filters.items()
        if fcfg.enabled
    }
    serialized = json.dumps(active, sort_keys=True, default=str)
    return hashlib.md5(serialized.encode()).hexdigest()[:12]
The fact that the hash is identical suggests that fcfg.config is empty or contains the same values despite different parameters. This indicates:

Option A: The FilterConfig objects are not being populated with the parameters correctly

Option B: The parameters are being stored elsewhere (not in .config)

Option C: There's a bug in StrategyConfig.from_dict() that's not passing through the filter parameters

Code Path to Investigate
python
# In config_schema.py - FilterConfig.from_dict()
@classmethod
def from_dict(cls, d: Dict[str, Any]) -> 'FilterConfig':
    known_keys = {'enabled', 'error_strategy'}
    config_params = {k: v for k, v in d.items() if k not in known_keys}
    return cls(
        enabled=bool(d.get('enabled', True)),
        error_strategy=str(d.get('error_strategy', 'pass_through')),
        config=config_params  # <-- Are parameters making it here?
    )
Severity
CRITICAL - This affects:

Cache invalidation: Indicator cache won't refresh when filter parameters change

Test reliability: Tests cannot verify parameter changes

Production correctness: Strategy behavior may not match configuration

Additional Observations
What's Working Correctly
✅ Time filter disabled behavior (returns None)
✅ Technical filter loading
✅ Filter skipping for disabled/unknown filters
✅ Cache hit/miss logic
✅ Filter error handling
✅ Metadata tracking
✅ Mode propagation to filters
✅ Basic hash stability (same config = same hash)

Impact Analysis
Area	Impact	Priority
Time Filter Configuration	Cannot override default hours	High
Cache System	Wrong cache hits with changed params	Critical
Test Coverage	Two tests blocked	Medium
Production Reliability	Potential misconfiguration	High
Recommended Actions
Immediate (Hotfix)
Issue 1: Investigate why time filter config is being merged with defaults instead of replaced

Issue 2: Add debug logging in _compute_filter_cfg_hash to see what's actually in fcfg.config

Short-term Fix
python
# Proposed debug patch for filter_pipeline.py
@staticmethod
def _compute_filter_cfg_hash(config: StrategyConfig) -> str:
    active = {}
    for name, fcfg in config.filters.technical_filters.items():
        if fcfg.enabled:
            active[name] = fcfg.config
            # Add debug logging
            logger.debug(f"Filter {name} config: {fcfg.config}")
    
    serialized = json.dumps(active, sort_keys=True, default=str)
    logger.debug(f"Serialized config: {serialized}")
    return hashlib.md5(serialized.encode()).hexdigest()[:12]
Long-term Fix
Unit test the FilterConfig construction in config_schema.py

Add validation that fcfg.config contains expected parameters

Consider including filter sequence in hash (though not the current issue)

Test File Status
The test file test_filter_pipeline.py is correctly implemented and should pass once these implementation issues are fixed. No further changes are needed to the test script.

Total Tests: 24
Passing: 22 ✅
Failing (Implementation Issues): 2 ❌

The test suite has done its job: it has identified two genuine bugs in the implementation.

This fingerprint only considers:

Length of DataFrame

First index value

Last index value

Extra tag

## Risk Manager
def _dataframe_fingerprint
does not consider the actual OHLC values! So even if we dramatically change the price data, as long as the index boundaries and length remain the same, the fingerprint will be identical, and the cache will return the same ATR series.
But this test actually reveals a design issue in the caching strategy. The fingerprint should include a hash of the data content, not just the index boundaries. A better fingerprint would be:

python
def _dataframe_fingerprint(df: pd.DataFrame, extra: str = "") -> str:
    """Stable fingerprint including data content."""
    if df is None or df.empty:
        return f"empty_{extra}"
    
    # Include a sample of the data in the fingerprint
    # For large DataFrames, we can sample to keep it efficient
    sample_size = min(100, len(df))
    sample_indices = np.linspace(0, len(df)-1, sample_size, dtype=int)
    
    # Create a hash of the sampled OHLC values
    sample_data = df.iloc[sample_indices][['open', 'high', 'low', 'close']].values.tobytes()
    data_hash = hashlib.md5(sample_data).hexdigest()[:8]
    
    return f"{len(df)}_{data_hash}_{extra}"

## test_filter_pipeline.py
(venv) PS E:\Trading\Backtest_platform> python tests/strategies/runners/strategy_unit_test.py --test test_filter_pipeline.py   
============================================================== FAILURES ============================================================== 
________________________________________ TestFilterPipeline.test_apply_filters_full_pipeline _________________________________________ 

self = <unit.test_filter_pipeline.TestFilterPipeline object at 0x000001C116B4B110>
base_config_dict = {'asset': {'pip_size': 0.0001, 'point_size': 1e-05, 'symbol': 'TEST'}, 'data': {'artf_timeframe': '1ME', 'date_range':...s': [], 'session_end': {'hour': 23, 'minute': 59}, 'session_start': {'hour': 0, 'minute': 0}}, 'enabled': True}}}, ...}
sample_df =                            open        high        low       close  volume
2025-01-01 00:00:00  100.248357  100.292315...   99.942730     168
2025-01-01 01:39:00   99.882706  100.618908  99.314814  100.618908     645

[100 rows x 5 columns]

    def test_apply_filters_full_pipeline(self, base_config_dict, sample_df):
        """Test full pipeline with time filter and technical filters."""
        from src.config.config_schema import StrategyConfig

        # Configure full pipeline - completely replace all filter configs
        config_dict = base_config_dict.copy()

        # Completely replace time filters (don't modify, replace)
        config_dict["filters"]["time_filters"] = {
            "time_filter": {
                "enabled": True,
                "config": {
                    "session_start": {"hour": 0, "minute": 0},
                    "session_end": {"hour": 23, "minute": 59},
                    "excluded_days": []
                }
            }
        }

        # Replace filter sequence and technical filters
        config_dict["filters"]["filter_sequence"] = ["rsi_filter", "adx_filter"]
        config_dict["filters"]["technical_filters"] = {
            "rsi_filter": {
                "enabled": True,
                "length": 14,
                "overbought": 70,
                "oversold": 30
            },
            "adx_filter": {
                "enabled": True,
                "adx_length": 14,
                "threshold": 25
            }
        }

        config = StrategyConfig.from_dict(config_dict)
        pipeline = FilterPipeline(config=config, mode="analytics")

        # Create signal frame
        signals = pd.Series(0, index=sample_df.index, dtype=np.int8)
        signals.iloc[10:20] = 1
        signals.iloc[30:40] = 2
        signals.iloc[50:60] = 1
        signals.iloc[70:80] = 2

        signal_frame = SignalFrame(
            signals=signals,
            indicator_data=None,
            signal_metadata={}
        )

        result = pipeline.apply_filters(
            signal_frame=signal_frame,
            df=sample_df
        )

        assert result.raw_count > 0
        assert result.final_count <= result.raw_count
        # Should have time_filter + 2 technical filters = 3 results
>       assert len(result.filter_results) == 3, f"Expected 3 filter results, got {len(result.filter_results)}"
E       AssertionError: Expected 3 filter results, got 1
E       assert 1 == 3
E        +  where 1 = len([FilterMetadata(filter_name='time_filter', status=<FilterStatus.REJECTED: 2>, signals_in=40, signals_out=0, signals_rejected=40, reason='All signals outside trading hours', indicator_values=None, execution_time_ms=0.5694999999832362)])
E        +    where [FilterMetadata(filter_name='time_filter', status=<FilterStatus.REJECTED: 2>, signals_in=40, signals_out=0, signals_rejected=40, reason='All signals outside trading hours', indicator_values=None, execution_time_ms=0.5694999999832362)] = FilterPipelineResult(final_signals=SignalFrame(signals=2025-01-01 00:00:00    0\n2025-01-01 00:01:00    0\n2025-01-01 00:02:00    0\n2025-01-01 00:03:00    0\n2025-01-01 00:04:00    0\n                      ..\n2025-01-01 01:35:00    0\n2025-01-01 01:36:00    0\n2025-01-01 01:37:00    0\n2025-01-01 01:38:00    0\n2025-01-01 01:39:00    0\nFreq: min, Length: 100, dtype: int8, indicator_data=None, signal_metadata={'source': 'time_filter', 'mode': 'analytics', 'session_hours': '08:30–20:30'}), raw_count=40, time_filtered_count=0, technical_filtered_count=0, final_count=0, filter_results=[FilterMetadata(filter_name='time_filter', status=<FilterStatus.REJECTED: 2>, signals_in=40, signals_out=0, signals_rejected=40, reason='All signals outside trading hours', indicator_values=None, execution_time_ms=0.5694999999832362)], rejection_reasons={'time_filter': 40}, execution_time_ms=0.6840999994892627).filter_results

tests\strategies\unit\test_filter_pipeline.py:435: AssertionError
________________________________ TestFilterPipeline.test_compute_filter_cfg_hash_changes_with_params _________________________________ 

self = <unit.test_filter_pipeline.TestFilterPipeline object at 0x000001C116E30650>
base_config_without_time_filter = {'asset': {'pip_size': 0.0001, 'point_size': 1e-05, 'symbol': 'TEST'}, 'data': {'artf_timeframe': '1ME', 'date_range':...: [], 'session_end': {'hour': 20, 'minute': 30}, 'session_start': {'hour': 8, 'minute': 30}}, 'enabled': False}}}, ...}

    def test_compute_filter_cfg_hash_changes_with_params(self, base_config_without_time_filter):
        """Test that hash changes when filter parameters change."""
        from src.config.config_schema import StrategyConfig

        # Create two configs with same filter but different parameters
        config1_dict = base_config_without_time_filter.copy()
        config1_dict["filters"]["filter_sequence"] = ["rsi_filter"]
        config1_dict["filters"]["technical_filters"] = {
            "rsi_filter": {
                "enabled": True,
                "length": 14,
                "overbought": 70,
                "oversold": 30
            }
        }

        config2_dict = base_config_without_time_filter.copy()
        config2_dict["filters"]["filter_sequence"] = ["rsi_filter"]
        config2_dict["filters"]["technical_filters"] = {
            "rsi_filter": {
                "enabled": True,
                "length": 21,
                "overbought": 70,
                "oversold": 30
            }
        }

        config1 = StrategyConfig.from_dict(config1_dict)
        config2 = StrategyConfig.from_dict(config2_dict)

        pipeline1 = FilterPipeline(config=config1, mode="core")
        pipeline2 = FilterPipeline(config=config2, mode="core")

        # The hash should be different for different parameters
        # Note: This is still failing, indicating a deeper issue in _compute_filter_cfg_hash
        # The hash is the same despite different parameters
>       assert pipeline1._filter_cfg_hash != pipeline2._filter_cfg_hash
E       AssertionError: assert '78eaea486202' != '78eaea486202'
E        +  where '78eaea486202' = <src.strategies.specific.modules.filter_pipeline.FilterPipeline object at 0x000001C116ECB6B0>._filter_cfg_hash
E        +  and   '78eaea486202' = <src.strategies.specific.modules.filter_pipeline.FilterPipeline object at 0x000001C116ECAC10>._filter_cfg_hash

tests\strategies\unit\test_filter_pipeline.py:623: AssertionError
====================================================== short test summary info ======================================================= 
FAILED tests/strategies/unit/test_filter_pipeline.py::TestFilterPipeline::test_apply_filters_full_pipeline - AssertionError: Expected 3 filter results, got 1
FAILED tests/strategies/unit/test_filter_pipeline.py::TestFilterPipeline::test_compute_filter_cfg_hash_changes_with_params - AssertionError: assert '78eaea486202' != '78eaea486202'
==================================================== 2 failed, 22 passed in 1.80s ====================================================