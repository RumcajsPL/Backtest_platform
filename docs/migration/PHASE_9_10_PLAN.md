**BACKTESTING PLATFORM**
**Phase 9 & 10 --- Comprehensive Plan**
Move to Production · Pipeline Diagnostics · Documentation Update
  ---------------------------- ------------------------------------------
  **Version**                  1.0 --- Draft for Review
  **Date**                     2026-02-25
  **Status**                   Planning
  **Author**                   Senior Python Consultant + Project Owner
  **Architecture**             v3.1.0 (Production Ready)
  ---------------------------- ------------------------------------------
**Executive Summary**
This document covers the final two phases of the Backtesting Platform
project. It is structured in three Acts that must be executed in
sequence:
  --------- ------------------ -------------------------------------------
  **Act**   **Scope**          **Objective**
  **ACT 0** **E2E Pipeline     Establish ground-truth facts on the \~30%
            Diagnostics**      trade-count discrepancy between Legacy and
                               New pipelines. Full Layer-by-Layer analysis
                               from Config/DataLoad through
                               TradeSimulation. No hypothesis launched
                               until all facts are collected.
  **PHASE   **Move to          Migrate new architecture modules to
  9**       Production**       production destinations, validate path
                               resolution and imports, archive and remove
                               legacy code. Short and mechanical ---
                               intelligence is in the architecture.
  **PHASE   **Documentation    Update all documentation to reflect the
  10**      Update**           production architecture, remove legacy
                               references, and produce the final project
                               handover package.
  --------- ------------------ -------------------------------------------
  ------------- -------------------------------------------------------------
  **⚠ KEY       ACT 0 is a prerequisite to PHASE 9. We do not move to
  PRINCIPLE**   production until the discrepancy root cause is understood and
                either confirmed as benign or fixed in the new pipeline.
  ------------- -------------------------------------------------------------
**ACT 0 --- E2E Pipeline Diagnostic**
**0.1 Context & Objectives**
A \~30% discrepancy in trade count has been observed between the Legacy
pipeline (run_wbws_strategy.py) and the New pipeline (new architecture
v3.1.0) when executed against identical configuration and data. Focused
testing on RiskManager did not identify a logic or computation issue at
that level. The discrepancy is therefore suspected to originate at an
earlier pipeline stage.
Objectives of ACT 0:
-   Establish verifiable, documented facts at each pipeline layer --- no
    assumptions, no guesses
-   Identify precisely where the two pipelines first diverge
-   Collect sufficient evidence to formulate targeted, testable
    hypotheses
-   Decide: is the discrepancy a bug in the new pipeline, a known
    behavioural change, or a legacy artefact?
-   If a bug is found in the new pipeline: define the fix. If the new
    pipeline is correct: document the intentional behavioural
    difference.
**0.2 Diagnostic Scope & Constraints**
  ---------------------- ------------------------------------------------
  **Constraint**         **Detail**
  **Test data window**   Identified 3-hour data range that is
                         representative and exposes enough signals to
                         make the discrepancy visible. Use this range for
                         all diagnostic runs to minimise noise.
  **Scope boundary**     Diagnostics cover: Config & DataLoad → Raw
                         Signals → FilterPipeline → TradeSimulation
                         (spread, risk, trade management). Analytics,
                         HTML reporting, and progressive tracker are OUT
                         of scope.
  **Investment rule**    We are NOT investing in Legacy code. We observe
                         and document Legacy behaviour as the reference
                         baseline only.
  **Hypothesis control** No hypothesis is launched until all four layers
                         have been analysed and all facts are documented.
                         Facts first, interpretation second.
  **Chat window          Work is split into four analytical blocks
  management**           aligned to the four pipeline layers. Each block
                         is self-contained and can be continued in a new
                         chat window with minimal context re-load.
  ---------------------- ------------------------------------------------
**0.3 Diagnostic Approach --- Instrumented Comparison**
We will use dedicated ad-hoc diagnostic scripts (not modified production
code) that run both pipelines side-by-side and emit structured
comparison logs. This preserves production code integrity while enabling
deep inspection.
**Diagnostic Tooling Strategy**
-   Ad-hoc diagnostic Python scripts per layer --- standalone, not
    imported by production modules
-   Structured log output (JSON-lines or CSV) for each layer: counts,
    hashes, key field samples
-   Side-by-side comparison: Legacy output vs New output for the same
    input
-   Diff summary table produced at end of each layer --- facts, not
    interpretation
-   No alterations to production modules. Diagnostic scripts import them
    read-only.
**0.4 Layer-by-Layer Diagnostic Plan**
**Layer 1 --- Config & Data Load**
  ---------- -------------------------------------------------------------
  **GOAL**   Confirm both pipelines receive identical data and that config
             interpretation is equivalent for all parameters that affect
             downstream computation.
  ---------- -------------------------------------------------------------
Diagnostic checkpoints:
-   Config parameters side-by-side: all trade_management, risk, spread,
    filter settings
-   Date range applied: first/last timestamp of df_strategy in both
    pipelines
-   Row count of loaded DataFrames: strategy, htf, ltf, artf
-   OHLCV column presence and dtype confirmation
-   Timestamp index type, timezone, and frequency
-   First 5 and last 5 rows of strategy and HTF DataFrames (printed as
    structured log)
-   ARTF row count and date range (full file, pre-slice)
Expected output artefact: Layer1_Config_Data_Facts.json --- structured
comparison of all above checkpoints with MATCH / MISMATCH flag per item.
**Layer 2 --- Raw Signal Generation**
  ---------- -------------------------------------------------------------
  **GOAL**   Confirm that the WBWS trigger produces the same BUY/SELL
             signal counts, positions, and values in both pipelines before
             any filtering is applied.
  ---------- -------------------------------------------------------------
Diagnostic checkpoints:
-   Total raw signal count: BUY count, SELL count, TOTAL
-   Signal index alignment: are signals on identical timestamps?
-   Signal value encoding: Legacy uses different encoding? (e.g.
    True/False vs 1/2 vs string)
-   HTF alignment: does shift(1) lookahead protection apply in both?
    Verify with sample rows.
-   First signal timestamp and last signal timestamp
-   Distribution of signals across the 3-hour window (e.g. per 30-min
    bucket)
-   Sample of 10 signal rows from each pipeline --- timestamp, signal
    value, key indicator fields
Expected output artefact: Layer2_Signal_Facts.json --- counts, timestamp
alignment diff, sample rows.
**Layer 3 --- Filter Pipeline**
  ---------- -------------------------------------------------------------
  **GOAL**   Confirm which filters are active, what each filter removes,
             and whether the final filtered signal count matches between
             pipelines.
  ---------- -------------------------------------------------------------
Diagnostic checkpoints:
-   Active filters list in each pipeline (by name and enabled/disabled
    state)
-   Signal count after time filters (session, day-of-week) in each
    pipeline
-   Signal count after each individual technical filter
-   Final filtered signal count: BUY / SELL / TOTAL
-   Rejection reason breakdown: how many signals removed by each filter
-   Filter configuration comparison: threshold values, parameters
    side-by-side
-   Sample of 5 rejected signals per pipeline --- timestamp and
    rejection reason
Expected output artefact: Layer3_Filter_Facts.json --- per-filter
counts, rejection breakdown, config diff.
**Layer 4 --- Trade Simulation (Spread, Risk, Trade Management)**
  ---------- -------------------------------------------------------------
  **GOAL**   Determine whether trade-count divergence occurs during
             simulation. Isolate whether it is Spread, Risk, or Trade
             Management that is responsible.
  ---------- -------------------------------------------------------------
Diagnostic checkpoints:
-   Signals entering simulation: count in each pipeline
-   Spread application: spread value applied per trade, any rejection at
    spread stage?
-   Risk filter: checked / approved / rejected counts from
    RiskManager.get_risk_summary()
-   Risk filter config: atr_multiplier_sl, max_risk_percentile,
    atr_period --- side-by-side
-   ARTF data used for annual range: sample of 12-month rolling window
    values at key timestamps
-   ATR sample at 10 evenly-spaced timestamps --- value comparison
    Legacy vs New
-   Trade manager: max_concurrent_trades, pyramiding settings ---
    side-by-side
-   Position-rejected count and reasons
-   Total trades opened / closed / rejected --- final tally
-   PnL per trade (first 10 trades) --- entry price, SL, TP, exit
    reason, PnL points
Expected output artefact: Layer4_Trade_Facts.json --- all above with
MATCH / MISMATCH flags.
**0.5 Root Cause Analysis (After All Layers Complete)**
  ------------- -------------------------------------------------------------
  **PROCESS**   Gather all four Layer\_\*\_Facts.json files. Identify the
                first layer showing a MISMATCH. That is the candidate root
                cause layer. Launch hypotheses only from that point.
  ------------- -------------------------------------------------------------
Root cause analysis process:
1.  Review Layer1 facts --- if MISMATCH: data or config is the root
    cause. Stop here.
2.  Review Layer2 facts --- if MISMATCH on signal counts: trigger or HTF
    alignment is root cause.
3.  Review Layer3 facts --- if MISMATCH on filter output: filter config
    or logic difference.
4.  Review Layer4 facts --- if MISMATCH on trade counts: simulation
    logic is root cause.
5.  Formulate 1-3 specific, testable hypotheses based on the first
    mismatch layer.
6.  Design confirmation test (minimal data, controlled input) for each
    hypothesis.
7.  Run confirmation tests → establish verdict: BUG IN NEW / INTENTIONAL
    CHANGE / LEGACY BUG.
8.  If BUG IN NEW: define fix, implement, re-run diagnostic, confirm
    resolution.
9.  Document verdict and resolution in DIAGNOSTIC_REPORT.md.
**Phase 9 --- Move to Production**
**9.1 Overview**
Phase 9 is architecturally straightforward because the new architecture
was designed from the start with the production destination in mind. The
primary work is file migration and validation of path resolution and
imports. No logic changes are expected.
  ------------------ -------------------------------------------------------------
  **PREREQUISITE**   ACT 0 must be completed and the discrepancy verdict
                     documented before Phase 9 begins. If a bug fix is required,
                     it is implemented and verified before migration.
  ------------------ -------------------------------------------------------------
**9.2 Migration Steps**
**Step 9.1 --- Inventory & Migration Map**
Produce a migration map: current location → production destination for
every file in the new architecture. Reference the full file list in
ARCHITECTURE.md Section \'Full list of files\'.
  ------------------------------------------- -------------------------------------------
  **Current Location (New Architecture)**     **Production Destination**
  src/config/config_schema.py                 src/config/config_schema.py
  src/core/cache_manager.py                   src/strategies/core/cache_manager.py
  src/strategies/contracts/\*.py              src/strategies/contracts/\*.py
  src/strategies/specific/modules/\*.py       src/strategies/specific/modules/\*.py
  src/strategies/specific/filters/\*.py       src/strategies/specific/filters/\*.py
  scripts/runners/run_strategy.py             scripts/runners/run_strategy.py
  configs/strategies/strategy_template.yaml   configs/strategies/strategy_template.yaml
  configs/spreads/broker_spreads.yaml         configs/spreads/broker_spreads.yaml
  ------------------------------------------- -------------------------------------------
**Step 9.2 --- Path Resolution Validation**
After migration, validate that src/utils/paths.py resolves all paths
correctly from the new file locations. Run a dry-run import check on all
modules.
-   python -c \"from src.config.config_schema import StrategyConfig\"
    --- must import cleanly
-   python -c \"from src.strategies.core.cache_manager import
    CacheManager\" --- must import cleanly
-   python -c \"from src.strategies.specific.modules.data_loader import
    DataLoader\" --- must import cleanly
-   Run all filter imports identically
-   Run scripts/runners/run_strategy.py \--dry-run (or equivalent config
    validation path)
**Step 9.3 --- End-to-End Smoke Test**
Run the new architecture E2E on the 3-hour diagnostic window in
analytics mode. Confirm:
-   No import errors, no path errors
-   HTML report generated successfully
-   Trade count matches the verified count from ACT 0 diagnostic
-   Metrics are within expected range for the test window
**Step 9.4 --- Legacy Archive & Removal**
Once smoke test passes, archive legacy code:
-   Create archive/legacy_YYYYMMDD/ directory
-   Move legacy modules: run_wbws_strategy.py, wbws_trigger.py (old),
    all old strategy modules
-   Move legacy configs: wbws_strategy.yaml (old), wbws_backtest.yaml
    (old)
-   Update .gitignore to exclude archive/ from active tracking if
    desired
-   Remove legacy imports from any shared utilities
-   Confirm project runs cleanly with zero references to legacy modules
**Step 9.5 --- Final Production Validation**
Run the full production pipeline on the standard 2-year dataset in core
mode and analytics mode. Confirm performance is equivalent to or better
than legacy baseline.
**9.3 Phase 9 Checklist**
  --------- ------------------------------------------------------ ------------
  **\#**    **Task**                                               **Status**
  **9.1**   ACT 0 complete, verdict documented                     ⬜ TODO
  **9.2**   Bug fix applied (if required) and verified             ⬜ TODO
  **9.3**   Migration map produced and reviewed                    ⬜ TODO
  **9.4**   All files migrated to production destinations          ⬜ TODO
  **9.5**   Path resolution validated (all imports clean)          ⬜ TODO
  **9.6**   E2E smoke test passed on 3-hour window                 ⬜ TODO
  **9.7**   Legacy code archived                                   ⬜ TODO
  **9.8**   Legacy imports removed from shared modules             ⬜ TODO
  **9.9**   Production full-dataset run completed successfully     ⬜ TODO
  --------- ------------------------------------------------------ ------------
**Phase 10 --- Documentation Update**
**10.1 Overview**
Phase 10 updates all documentation to reflect the production
architecture, removes or archives legacy references, and produces the
final project handover package. Documentation is the last deliverable
--- it is only written once the production system is stable.
**10.2 Documentation Inventory**
  ----------------------------------------------- ------------------------- ----------- --------------
  **Document**                                    **Action**                **Owner**   **Priority**
  **docs/architecture/ARCHITECTURE.md**           Update version, file      Dev         **P1**
                                                  paths, module list to                 
                                                  reflect production                    
                                                  layout. Remove any                    
                                                  placeholders.                         
  **README.md**                                   Full rewrite. Replace     Dev         **P1**
                                                  legacy sections with new              
                                                  architecture. Update                  
                                                  repository structure,                 
                                                  tool versions, usage                  
                                                  commands.                             
  **configs/strategies/strategy_template.yaml**   Ensure all keys are       Dev         **P1**
                                                  documented with inline                
                                                  comments. Remove any                  
                                                  legacy keys.                          
  **DIAGNOSTIC_REPORT.md**                        NEW --- Document ACT 0    Dev         **P1**
                                                  findings, root cause                  
                                                  verdict, and resolution.              
                                                  Permanent project record.             
  **docs/operations/RUNBOOK.md**                  NEW --- Step-by-step      Dev         **P2**
                                                  operational guide for                 
                                                  running the production                
                                                  pipeline, managing data               
                                                  updates, and interpreting             
  **tests/ unit test docs**                       Review and update test    Dev         **P3**
                                                  documentation to match                
                                                  new module locations and              
                                                  contracts.                            
  **CHANGELOG.md**                                NEW --- Record all        Dev         **P3**
                                                  significant changes from              
                                                  project start to v1.0                 
                                                  production release.                   
  ----------------------------------------------- ------------------------- ----------- --------------
**10.3 Phase 10 Checklist**
  ---------- ------------------------------------------------------ ------------
  **\#**     **Task**                                               **Status**
  **10.1**   ARCHITECTURE.md updated to v3.1.0 production           ⬜ TODO
  **10.2**   README.md fully rewritten for new architecture         ⬜ TODO
  **10.3**   strategy_template.yaml inline documentation complete   ⬜ TODO
  **10.4**   DIAGNOSTIC_REPORT.md written and reviewed              ⬜ TODO
  **10.5**   RUNBOOK.md written (operational guide)                 ⬜ TODO
  **10.6**   RISK_CALIBRATION.md written                            ⬜ TODO
  **10.7**   CHANGELOG.md written                                   ⬜ TODO
  **10.8**   All legacy references removed from documentation       ⬜ TODO
  **10.9**   Final documentation review --- all links valid, no     ⬜ TODO
             TODOs                                                  
  ---------- ------------------------------------------------------ ------------
**Overall Execution Sequence**
  ---------- ------------------ -------------------------------------- ----------
  **Step**   **Phase**          **Key Deliverable**                    **Gate**
  **1**      ACT 0 --- Layer 1  Layer1_Config_Data_Facts.json ---      Facts only
                                MATCH/MISMATCH on data load            
  **2**      ACT 0 --- Layer 2  Layer2_Signal_Facts.json --- raw       Facts only
                                signal count comparison                
  **3**      ACT 0 --- Layer 3  Layer3_Filter_Facts.json ---           Facts only
                                per-filter rejection breakdown         
  **4**      ACT 0 --- Layer 4  Layer4_Trade_Facts.json --- simulation Facts only
                                trade count comparison                 
  **5**      ACT 0 --- RCA      DIAGNOSTIC_REPORT.md --- root cause    Decision
                                verdict + fix plan                     
  **6**      ACT 0 --- Fix      Bug fix implemented and verified (if   Fix
                                required)                              verified
  **7**      Phase 9.1-9.3      Migration map + all files migrated +   Import
                                imports validated                      clean
  **8**      Phase 9.4-9.5      Smoke test passed + legacy archived    Smoke pass
  **9**      Phase 9 Final      Full 2-year production run completed   Run clean
  **10**     Phase 10 P1        ARCHITECTURE.md + README.md +          Review
                                DIAGNOSTIC_REPORT.md                   
  **11**     Phase 10 P2-P3     RUNBOOK + RISK_CALIBRATION + CHANGELOG Review
  **12**     Project Close      Final review, all checklists ✅,       DONE ✅
                                project signed off                     
  ---------- ------------------ -------------------------------------- ----------
**Appendix --- Architecture Reference Summary**
**New Architecture Pipeline (v3.1.0)**

Config → DataLoader → SignalGenerator → FilterPipeline → TradeSimulator
→ MetricsCalculator → (analytics mode only) TradeAnalytics →
ReportGenerator
**Key Architecture Principles (Diagnostic Relevance)**
-   Fail-fast: any missing data aborts with clear error --- no silent
    defaults. Relevant for RAR/ATR gaps.
-   Single Source of Truth: spread values from broker_spreads.yaml only.
    Config template holds path, not values.
-   Immutable Contracts: frozen dataclasses --- SignalFrame,
    FilterPipelineResult, TradeResult, etc.
-   HTF alignment: shift(1) applied in SignalGenerator --- no lookahead.
    Must be verified in Layer 2 diagnostic.
-   RAR (12-month rolling annual range): requires 12 months of ARTF
    history. Partial windows produce NaN → trade rejected. Relevant to
    Layer 4.
-   max_risk_percentile: percentage of annual range --- highly
    timeframe-sensitive. Miscalibration symptoms documented in
    ARCHITECTURE.md.
**Legacy Pipeline Reference**
-   Entry point: scripts/runners/run_wbws_strategy.py
-   Indicator: src/indicators/wbws_trigger.py
-   Strategy config: configs/backtesting/wbws_backtest.yaml
-   Legacy is observed for comparison only. No investment in legacy
    code.
*END OF DOCUMENT --- Phase 9 & 10 Comprehensive Plan --- v1.0 ---
2026-02-25*