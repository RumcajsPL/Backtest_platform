# HARDENING II — Final Polish for Production Readiness
**Date:** 2026-02-21
**Lead Consultant:** Senior Python Consultant
**Audit Principles:** Single Responsibility, Performance (Multi-Run First), Explicit Contracts, Type Safety, Production Readiness (Fail-Fast, No Debug Artifacts).

## I. EXECUTIVE SUMMARY

The system has successfully completed its first hardening phase, with the major architectural migrations and performance overhauls (Sessions 20-21) complete. The foundation is solid.

This final hardening phase targets the remaining "lint" that prevents the code from being truly production-grade. The findings are categorized into three logical blocks:

1.  **Block A: Architectural Purity & Config Centralization:** Eliminates the last few violations of the Single Responsibility Principle, most notably the ad-hoc YAML re-parsing in the orchestrator and the direct use of raw dicts in filters.
2.  **Block B: Production Readiness & Fail-Fast:** Removes all development-era artifacts ("debug" mode, commented code, `if __name__ == "__main__":` blocks) and enforces strict fail-fast validation (e.g., `htf_period` format, signal ID lookups).
3.  **Block C: Multi-Run Hardening & Performance:** Optimizes for unattended parameter sweeps by adding proper cache lifecycle management and removing a final source of unnecessary object overhead.

Implementing these blocks will result in a clean, predictable, and highly performant system ready for production deployment.

---

## II. DETAILED FINDINGS & MIGRATION PLAN

### Block A — Architectural Purity & Config Centralization

**Goal:** Ensure `StrategyConfig` is the single source of truth for all modules and that all module boundaries are enforced by typed contracts. No module should load or validate its own configuration.

---

#### A-1: Purge Ad-Hoc YAML Parsing in Orchestrator

*   **File:** `src/strategies/orchestrator.py`
*   **Severity:** High
*   **Principle:** Single Responsibility, Production Readiness (Fail-Fast)
*   **Description:**
    The `StrategyOrchestrator` currently contains `_read_htf_period()`, a static method that re-parses the YAML file to read `data.htf_period`. This is a clear violation of the Single Responsibility Principle. The `orchestrator` should orchestrate, not parse configs. It should trust the `StrategyConfig` object it was given. This workaround exists because `SignalGenerator` (pre-migration) required `htf_period` as a string, and `DataConfig` didn't yet have this field.
*   **Resolution:**
    *   **Action 1 (Pre-requisite):** Ensure `DataConfig` in `config_schema.py` includes the `htf_period` field (as defined in DEC-033 of `SESSION_21_HANDOFF.md`). This field already exists in the provided `strategy_template.yaml`.
    *   **Action 2:** Modify `SignalGenerator.__init__` to accept the full `StrategyConfig` object. It can then read `config.data.htf_period` directly. This change is part of DEC-034.
    *   **Action 3:** Remove `_read_htf_period()` and its call from `StrategyOrchestrator.__init__`.
    *   **Action 4:** Update `StrategyOrchestrator._generate_signals()` to instantiate `SignalGenerator` with `self._config` instead of `self._htf_period`.

---

#### A-2: Finalize Filter Parameter Typing (P1-CH3-8 Completion)

*   **File:** `src/strategies/specific/filters/time_filter.py` (and all other filters)
*   **Severity:** Medium
*   **Principle:** Type Safety, Explicit Contracts
*   **Description:**
    The `SESSION_21_HANDOFF.md` flagged `TimeFilter` as still accepting a raw `config: Dict` in its `__init__`. While `time_filter.py` correctly unpacks the dict immediately, the public interface is still a dict, which is against the typed-contract architecture. This makes it harder to understand dependencies and is inconsistent with the rest of the hardened system.
*   **Resolution:**
    *   **Action 1:** Define a typed configuration dataclass for the time filter, e.g., `TimeFilterConfig`, with fields `enabled`, `session_start_hour`, `session_start_minute`, `session_end_hour`, `session_end_minute`. This could live in `config_schema.py` or a new `filter_configs.py` module.
    *   **Action 2:** Update `TimeFilter.__init__` to accept `config: TimeFilterConfig`.
    *   **Action 3:** Update `FilterPipeline._load_time_filter()` to extract the necessary dict from the main `StrategyConfig` and instantiate `TimeFilterConfig` before passing it to the `TimeFilter`.
    *   **Action 4 (Future-proofing):** While other technical filters currently use typed `__init__` arguments (e.g., `adx_length`, `threshold`), consider a similar refactor for them into dedicated config objects for ultimate consistency. However, this is lower priority than `TimeFilter`.

---

### Block B — Production Readiness & Fail-Fast

**Goal:** The codebase must contain no artifacts from the development process. All assumptions about data and configuration must be validated at the earliest possible point with clear, actionable error messages.

---

#### B-1: Eradicate "debug" Mode Everywhere

*   **Files:** All Python files in `src/strategies/`
*   **Severity:** High
*   **Principle:** Production Readiness
*   **Description:**
    `SESSION_21_HANDOFF.md` correctly notes that the `"debug"` mode is deprecated and replaced by `"analytics"`. While many files have guards raising a `ValueError` (e.g., `risk_manager.py`), other files might still contain logic that checks for `mode == "debug"`. This creates a risk of silent misbehavior if the mode string `"debug"` ever appears in a config file. A complete sweep is required to ensure `"debug"` is treated as invalid input universally.
*   **Resolution:**
    *   **Action 1:** Audit all modules that accept a `mode` parameter (`DataLoader`, `SignalGenerator`, `FilterPipeline`, `RiskManager`, `SpreadManager`, `TradeSimulator`, and all filters). Ensure that `__init__` or the primary execution method validates the mode against `{"core", "analytics"}`.
    *   **Action 2:** Where `"debug"` is used as a key for config lookups (e.g., `config.get("debug", {})` in `TradeSimulator.__init__`), replace it with `"analytics"` to correctly source the new config section. The legacy key should be ignored.

---

#### B-2: Remove All `if __name__ == "__main__":` Demo Blocks

*   **Files:** All Python files, especially utility and contract files (e.g., `structured_logger.py`, `metrics_contracts.py`, `trade_analytics.py`).
*   **Severity:** Low
*   **Principle:** Production Readiness
*   **Description:**
    Several files contain example usage or demo blocks under `if __name__ == "__main__":`. While useful during development, these serve no purpose in a production library and increase the file size. They can also be misleading or become outdated.
*   **Resolution:**
    *   **Action 1:** Remove all `if __name__ == "__main__":` blocks and their contents from the codebase. Any example code should be moved to dedicated documentation or example scripts in the `scripts/` directory.

---

#### B-3: Validate `htf_period` Against Known Offsets

*   **File:** `src/strategies/specific/modules/signal_generator.py`
*   **Severity:** Medium
*   **Principle:** Production Readiness (Fail-Fast)
*   **Description:**
    The `SESSION_21_HANDOFF.md` Fail-Fast Audit (SG-1) correctly identifies that `SignalGenerator` validates `htf_period` for emptiness but not for content. An invalid string like `"INVALID"` will only fail deep inside a pandas `resample()` call with a cryptic error.
*   **Resolution:**
    *   **Action 1:** Define a set of valid pandas offset aliases, e.g., `_VALID_HTF_PERIODS = {"1min", "5min", "15min", "30min", "1H", "2H", "4H", "1D", "1W"}` in `signal_generator.py`.
    *   **Action 2:** In `SignalGenerator.__init__`, after ensuring the value is not empty, validate it against this set. Raise a clear `ValueError` listing the valid options if it doesn't match. This aligns with the "fail-fast" principle by catching the error at the config boundary.

---

#### B-4: Harden TradeSimulator Exit Reason Lookup

*   **File:** `src/strategies/specific/modules/trade_simulator.py`
*   **Severity:** High
*   **Principle:** Production Readiness (Fail-Fast)
*   **Description:**
    The Fail-Fast Audit (TS-2) flagged a critical issue: an unknown `exit_reason` string is caught by a `KeyError` and defaults to `END_OF_DATA` with a warning. This corrupts exit statistics and masks a programming error. The correct behavior is to fail immediately, as an unknown exit reason is a code defect.
*   **Resolution:**
    *   **Action 1:** In `_execute_trade_exit`, replace the `try/except` block with a direct dictionary-style lookup on the `ExitReason` enum. Catch the `KeyError` and re-raise it as a `ValueError` with a clear message about the invalid `exit_reason` and the valid enum names. This is a 5-minute fix with significant impact on data integrity.

---

#### B-5: Remove `from_yaml_config` Adapter from DataConfig

*   **File:** `src/strategies/contracts/data_contracts.py`
*   **Severity:** Medium
*   **Principle:** Explicit Contracts, Production Readiness
*   **Description:**
    `DataConfig` currently has a `from_yaml_config` class method. This is a legacy adapter pattern that should have been removed in the final migration (as noted in `SESSION_21_HANDOFF.md`). The canonical way to build a `DataConfig` is now via `StrategyConfig`. Keeping this adapter adds an unnecessary, and potentially confusing, way to construct this contract.
*   **Resolution:**
    *   **Action 1:** Delete the `from_yaml_config` method from the `DataConfig` dataclass in `data_contracts.py`.
    *   **Action 2:** Verify that the only caller of this method, `DataLoader.load_config()`, has been refactored to build `DataConfig` directly from the `StrategyConfig` object, as described in DEC-033 of the handoff document. If `DataLoader` is migrated, this method is safe to remove.

---

### Block C — Multi-Run Hardening & Performance

**Goal:** Optimize the system for its primary purpose: unattended, multi-run backtesting. This involves ensuring that state is correctly managed between runs and eliminating final performance bottlenecks.

---

#### C-1: Centralize and Formalize Cache Lifecycle Management

*   **Files:** `src/strategies/specific/modules/risk_manager.py`, `src/strategies/specific/modules/filter_pipeline.py`, `src/strategies/specific/modules/spread_manager.py`
*   **Severity:** High
*   **Principle:** Performance — Multi-Run Backtester First
*   **Description:**
    Multiple modules (`RiskManager`, `FilterPipeline`, `SpreadManager`) maintain their own internal caches. Currently, clearing these caches is a manual process. The orchestrator handles `RiskManager.clear_cache()` by convention, but the `FilterPipeline` cache is tied to the pipeline instance's lifecycle, and `SpreadManager` caches globally. For a reliable multi-run backtester, there must be a single, authoritative way to reset *all* cached state between runs.
*   **Resolution:**
    *   **Action 1:** Create a new module, e.g., `src/strategies/core/cache_manager.py`, with a class `CacheManager`.
    *   **Action 2:** The `CacheManager` will hold references to the cache dicts of `RiskManager`, `SpreadManager`, and the `FilterPipelineCache`. It will provide a public method `clear_all_caches()`.
    *   **Action 3:** Refactor `RiskManager`, `SpreadManager`, and `FilterPipelineCache` to have a `register_with_cache_manager` method or to be instantiated with a reference to a shared `CacheManager`. The global `_ATR_CACHE`, `_CONFIG_CACHE`, etc., should become class attributes that are accessed via the manager.
    *   **Action 4:** Update `StrategyOrchestrator.run()` to accept an optional `CacheManager` instance. The caller (e.g., `run_strategy.py` or a future backtester loop) will be responsible for creating a single `CacheManager` and calling `clear_all_caches()` between runs. The orchestrator's `clear_cache` flag should be removed in favor of this explicit management.

---

#### C-2: Remove Orchestrator's Signal Translation Layer (CF-6)

*   **File:** `src/strategies/orchestrator.py`
*   **Severity:** Medium
*   **Principle:** Single Responsibility
*   **Description:**
    The orchestrator contains a dictionary `_SIGNAL_CODE_TO_STR` and a `.map().dropna()` call to convert an `int8`-based `SignalFrame` into a `pd.Series` of strings for the `TradeSimulator`. This is a data transformation that belongs either in the `TradeSimulator` or in a dedicated adapter. The orchestrator's job is to call modules, not to reformat data between them. This was correctly identified as CF-6 in the handoff.
*   **Resolution:**
    *   **Action 1:** In `TradeSimulator.simulate_trades()`, add logic at the beginning of the method to accept either a `pd.Series` of strings (the current format) OR a `SignalFrame`. If a `SignalFrame` is received, perform the translation internally using a private helper method.
    *   **Action 2:** Update `StrategyOrchestrator._simulate_trades()` to pass `filter_result.final_signals` (a `SignalFrame`) directly to `TradeSimulator.simulate_trades()`, removing the translation step.
    *   **Action 3:** Remove `_SIGNAL_CODE_TO_STR` from the orchestrator.

---

## III. COMPREHENSIVE IMPLEMENTATION PLAN

The following plan sequences the work to minimize risk and maintain a working system after each step. Each logical block can be developed in parallel by different team members, as they are largely independent.

### Phase 1: Configuration & Type Safety (Block A)
- **Goal:** Solidify the config as the single source of truth.
1.  **DEC-033 Completion (A-1, Action 1):** Ensure `DataConfig` in `config_schema.py` includes `htf_period`, `ltf_timeframe`, and `artf_timeframe`. This is a schema-only change and is safe.
2.  **DEC-034 & A-1 Completion:** Migrate `SignalGenerator` to accept `StrategyConfig`.
    *   Modify `signal_generator.py` `__init__`.
    *   Update `orchestrator.py` to pass `self._config` and remove `_read_htf_period()`.
    *   Run E2E test to ensure output matches baseline.
3.  **P1-CH3-8 Completion (A-2):** Refactor `TimeFilter` to use a typed config object.
    *   Create `TimeFilterConfig` in `config_schema.py`.
    *   Update `time_filter.py` `__init__`.
    *   Update `filter_pipeline.py` `_load_time_filter()` to instantiate the config object.
    *   Run filter-specific tests to ensure logic unchanged.

### Phase 2: Production Readiness Sweep (Block B)
- **Goal:** Remove all development artifacts and enforce fail-fast validation.
1.  **B-1 (Debug Mode Eradication):** Perform a global search for `"debug"` in the `src/` directory. Audit and fix all instances, ensuring they either raise a `ValueError` or are replaced with `"analytics"`.
2.  **B-2 (Demo Blocks Removal):** Perform a global search for `if __name__ == "__main__":` and remove the blocks and their contents.
3.  **B-3 (htf_period Validation):** Implement the validation against a set of known offsets in `signal_generator.py`.
4.  **B-4 (Exit Reason Fail-Fast):** Implement the strict `ExitReason` lookup in `trade_simulator.py`.
5.  **B-5 (Legacy Adapter Removal):** Remove `from_yaml_config` from `data_contracts.py`. Verify the system still runs (it should, as it's not used in the new architecture).

### Phase 3: Multi-Run Optimization (Block C)
- **Goal:** Optimize for performance and state management in a backtester loop.
1.  **C-1 (Cache Manager):** This is a larger structural change.
    *   Create the `cache_manager.py` module.
    *   Refactor `RiskManager`, `SpreadManager`, and `FilterPipelineCache` to integrate with it.
    *   Update `orchestrator.py` to optionally accept a `CacheManager` and remove its internal `clear_cache` logic.
    *   Update `run_strategy.py` to be cache-aware (though a simple single-run script doesn't need to clear caches).
2.  **C-2 (Signal Translation):**
    *   Modify `TradeSimulator.simulate_trades()` to handle a `SignalFrame` input.
    *   Update `orchestrator.py` to pass the `SignalFrame` directly.
    *   Remove the translation dictionary and logic from the orchestrator.

### Phase 4: Documentation & Final Verification
- **Goal:** Update architecture docs and establish new performance baseline.
1.  **Update `ARCHITECTURE.md`:** Document the new `CacheManager` and any changes to module interfaces (e.g., `SignalGenerator` now takes `StrategyConfig`, `TradeSimulator` now accepts `SignalFrame`).
2.  **Establish `PERFORMANCE_BASELINE_FINAL.md`:** Run the full E2E test in both `core` and `analytics` modes to establish a new baseline after all hardening changes. The numbers should be equal to or better than the `PERFORMANCE_BASELINE_S21`.
3.  **Final Code Review:** A final pass to ensure all principles are met and the code is clean and ready for production.