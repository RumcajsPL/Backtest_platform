# WBWSStrategy — Production Hardening Audit Report

This report identifies specific files and code locations that require changes to align with the `HARDENING_GUIDE.md` (Version 2.1.0). The audit is based on a review of the provided codebase.

## 1. Module Docstring

**Findings:** The docstrings in many core files contain session, task, or phase references, which violate section 1 of the guide.

| File | Location | Issue | Recommended Action |
| :--- | :--- | :--- | :--- |
| `src/strategies/core/signal_generator.py` | Line 4 | `Session: 21 - Final Hardening` | Remove the session reference. Keep version and change log. |
| `src/strategies/market/spread_manager.py` | Line 4 | `Session: 21 - Final Hardening` | Remove the session reference. Keep version and change log. |
| `src/strategies/core/trade_simulator.py` | Line 3 | `Session: duration_bars fix` | Replace with a dated change log entry, e.g., `- [2026-02-27] Added duration_bars calculation for LTF exits.` |
| `src/strategies/filters/*.py` | Multiple files | Many filter files (e.g., `dpo_filter.py`, `ma_filter.py`, `macd_filter.py`) have `Version: 3.0.1` but no change log. | Add a concise change log entry in the docstring, even if it just says `- Maintenance release.` |
| `src/strategies/orchestrator.py` | Line 5 | `Version: 2.2.0` - No change log. | Add a change log for versions 2.1.0 → 2.2.0. |
| `src/strategies/contracts/analytics_contracts.py` | Line 8 | Contains a note `Session 20 change: ...` | Replace with a standard change log entry, e.g., `- [DEC-004] Added frozen=True to TradingSessionConfig.` |

## 2. Imports

### 2a. Dead imports
**Findings:** Several files import modules that are not used.

| File | Location | Issue | Recommended Action |
| :--- | :--- | :--- | :--- |
| `src/strategies/contracts/analytics_contracts.py` | Line 2 | `import json` is not used. | Remove the import. |
| `src/strategies/contracts/trade_contracts.py` | Line 8 | `import json` is after other imports. Standard library imports should come first. | Move `import json` to line 1. |

### 2b. Unparameterized generics
**Findings:** The codebase is generally excellent in this regard, with most generics properly parameterized. However, a few instances were found.

| File | Location | Issue | Recommended Action |
| :--- | :--- | :--- | :--- |
| `src/strategies/core/report_generator.py` | Line 289 | `List` in function signature is not parameterized. | Change `List` to `List[str]`. |
| `src/strategies/core/trade_simulator.py` | Line 307 | `Dict` used in type hint is not parameterized. | Change `Dict` to `Dict[str, int]`. |
| `src/strategies/core/trade_simulator.py` | Line 492 | `Dict` used in type hint is not parameterized. | Change `Dict` to `Dict[str, Any]`. |

### 2c. Missing `Optional` on nullable defaults
**Findings:** All nullable defaults were correctly wrapped in `Optional` throughout the codebase. No issues found.

### 2d. Untyped parameters on `__init__`
**Findings:** All `__init__` methods in core modules are fully typed. The use of `TYPE_CHECKING` in `analytics_contracts.py` is correct and follows best practices.

### 2e. Import ordering (PEP 8)
**Findings:** The majority of files follow PEP 8. A few minor inconsistencies were found.

| File | Location | Issue | Recommended Action |
| :--- | :--- | :--- | :--- |
| `src/strategies/core/trade_analytics.py` | Line 18 | `from typing import TYPE_CHECKING` is after other imports. It should be at the top of the file with other typing imports. | Move `from typing import TYPE_CHECKING` to the top. |
| `src/strategies/contracts/trade_contracts.py` | Line 8 | As noted above, `import json` is misplaced. | Move `import json` to line 1. |
| `src/strategies/core/trade_simulator.py` | Lines 19-22 | Imports of `RiskManager`, `SpreadManager`, etc., are local modules but are mixed with third-party imports (numpy, pandas). | Create a blank line between the third-party group and the local group. |

## 3. Contract Access

**Findings on `getattr` and `isinstance` guards:**
The codebase has successfully eliminated almost all instances of `getattr` used as a probe and `isinstance` guards for MagicMock. This principle is well-adhered to. However, a specific pattern of `getattr` on typed config objects was found in `config_schema.py` that requires attention.

| File | Location | Issue | Recommended Action |
| :--- | :--- | :--- | :--- |
| `src/strategies/config/config_schema.py` | Lines 415-418 | `_analytics = getattr(config, 'analytics', None)`. This `getattr` is used to probe for an optional `analytics` attribute on `StrategyConfig`. This is a valid use case because the `analytics` attribute may not exist. However, the guide's preferred pattern is to use `hasattr` for existence checks. | Replace with `if hasattr(config, 'analytics'): _analytics = config.analytics else: _analytics = None`. |

## 4. Fail-Fast Principle (Architecture Principle 6)

**Findings:** The codebase strictly follows the fail-fast principle. No instances of swallowing exceptions during construction were found. The `try/except` blocks in `filter_pipeline.py` are correctly placed around third-party filter initialization and runtime application, not around critical construction logic.

## 5. Null/None Handling

**Findings:** The codebase handles `None` from YAML correctly. The pattern of checking `if config_item is None` is used consistently, and disabled config items correctly result in `None` attributes.

| File | Location | Issue | Recommended Action |
| :--- | :--- | :--- | :--- |
| `src/strategies/config/config_schema.py` | Line 501 | `if data.paths.artf_ohlcv is None:`. This correctly checks for `None` after it has already been set from YAML. This is good. | No action needed. |
| `src/strategies/core/filter_pipeline.py` | Line 145 | `if time_filter_cfg is None or not time_filter_cfg.enabled:`. This correctly treats a missing or disabled config as the same state. This is the recommended pattern from the guide. | No action needed. |

## 6. Frozen Dataclass Field Ordering

**Findings:** All dataclasses are `frozen=True` and correctly ordered with fields without defaults preceding those with defaults. The `Insight` class in `analytics_contracts.py` is a perfect example of this.

## 7. `__main__` and Demo Blocks

**Findings:** No production modules contain `if __name__ == "__main__":` demo blocks. The only file with this block is `metrics_contracts.py`, which is a test/example file in its `__main__` section. This is acceptable as per the guide's exception for docstring examples and test files.

## 8. Explicit Over Implicit (Architecture Principle 4)

**Findings:** The system is excellent in this regard. Modes are passed explicitly, and optional config sections are accessed safely.

| File | Location | Issue | Recommended Action |
| :--- | :--- | :--- | :--- |
| `src/strategies/config/config_schema.py` | Lines 415-418 | As noted in section 3, `getattr(config, 'analytics', None)` is used. While it's a valid probe, the guide's recommended pattern for typed objects is a two-step check: `_analytics = getattr(config, 'analytics', None)` followed by `enabled = bool(getattr(_analytics, 'profile_simulator', False))` is exactly what's done here. This is correct. | No action needed. |
| `src/strategies/core/data_loader.py` | Line 170 | `self._build_data_config()` correctly builds a `DataConfig` from the `StrategyConfig` without any implicit inference. This is good. | No action needed. |

## 9. Version Bump Convention

**Findings:** The current versions of the files are consistent with the Phase 5 hardening. However, the changes recommended in this audit should trigger a patch or minor version bump according to the convention.

- **Patch bump (X.Y.Z → X.Y.Z+1)**: For changes like cleaning docstrings, removing dead imports, fixing import ordering, and correcting `getattr` patterns.
- **Minor bump (X.Y.Z → X.Y+1.0)**: If any behavioural fix is implemented (e.g., correcting the `getattr` in `config_schema.py` could be considered a behaviour fix if it prevents a potential error, though currently it's safe). For this audit, most changes are cosmetic, so a patch bump is sufficient.

**Recommendation:** Update the `Version` in the docstring of any file you modify, following the patch bump convention (e.g., `Version: 2.2.0` → `Version: 2.2.1`). Add a change log entry describing the change (e.g., `- [HARDENING] Removed session reference from docstring.`).

## Summary of High-Priority Actions

1.  **Docstrings:** Clean all session/task/phase references from docstrings in `signal_generator.py`, `spread_manager.py`, `trade_simulator.py`, and all filter files.
2.  **Imports:**
    - Remove unused `import json` from `analytics_contracts.py`.
    - Move `import json` to line 1 in `trade_contracts.py`.
    - Parameterize the `List` in `report_generator.py` (Line 289) and the `Dict` in `trade_simulator.py` (Lines 307, 492).
    - Correct import ordering in `trade_analytics.py` and `trade_simulator.py`.
3.  **Contract Access:** Replace the `getattr` probe in `config_schema.py` (Line 415) with the recommended two-step pattern for clarity.
4.  **Version Bump:** Update the version numbers and add change logs for all modified files.