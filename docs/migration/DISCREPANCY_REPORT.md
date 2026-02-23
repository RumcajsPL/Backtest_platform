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