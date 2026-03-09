---
name: backtester-project
description: >
  Use this skill whenever working on the Backtesting & Optimization Framework project
  OR the broker_support / eToro API integration project. Triggers: any mention of
  backtester, backtest pipeline, CandidateStore, GA engine, WFO evaluator, Monte Carlo
  engine, fitness evaluator, scenario profile, backtest_template.yaml, sensitivity
  evaluator, verdict engine, report generator, any module from src/backtesting/,
  broker_support, EToroClient, PositionTracker, CSVJournal, paper trading automation,
  eToro API, signal bridge, or CTP roadmap.
  Read this SKILL.md before writing any code, creating any file, or making any design
  decision for this project.
---
# CTP Project Skill — Backtesting + Broker Integration
## Project Status (2026-03-09, Block 9O)
```
BACKTESTING ENGINE:    V1 PRODUCTION DECLARED (f545f0f2, 2026-03-08)
                       Full-history calibration IN PROGRESS — Block 9O run in progress
                       6 calibration runs attempted; all B9O patches applied; run live now
                       Next: extract _SIGMOID_SCALE from current run → update consistency_scorer.py
                       Paper trade candidates: c4f0aea11a3e (primary), da38ecc0ddc6 (secondary)

BROKER INTEGRATION:    broker_support package — connection confirmed, 4 bugs to fix
                       5-step plan scoped. Empirical demo history test PENDING (P1).

CTP ROADMAP:           5-phase plan agreed. Phase 0 (broker fixes) + Phase 1 (full-history)
                       running in parallel NOW. Phase 2 = automated paper trading.
```
---
## DUAL-TRACK CONTEXT

### Track A — Backtesting Full-History (in progress)
Six calibration runs attempted (Block 9O). Multiple bugs discovered and fixed — all triggered
by the 38-month full-history evaluation regime, never seen in 3-month production runs.

**Current calibration YAML: `configs/backtesting/backtest_calibration_fullhistory_v3.yaml` (v4.0.0)**

Calibration run in progress. When complete:
1. Extract `net_pnl` from `wfo_window_results` (Stage 4) via `query_run.py`
2. `_SIGMOID_SCALE = stdev(net_pnl across all windows × passers) × 0.5`
3. Update `consistency_scorer.py`: `_SIGMOID_SCALE = <new value>`
4. Update `backtest_V1_01.yaml`: `max_workers: 2`, `min_win_rate: 0.11`, `min_expectancy: -2.0`
5. Run `backtest_V1_01.yaml` overnight

**Pre-calibration also check:** `d9a81454` — pipeline ran 55 min before B9O-004 crash. Stage 4 WFO may already be complete. Query this run_id before waiting for the current run.

**Stage 1 distributions (38-month — stable across 3+ runs):**
```
win_rate:      min=0.0945  avg=0.1486  max=0.2265
expectancy:    min=-2.56   avg=-1.69   max=+0.35
profit_factor: min=0.67    avg=0.8295  max=1.02
trades/week:   min=4.11    avg=35.38   max=87.35
losing_streak: min=25      avg=46.8    max=82
max_drawdown:  min=0.0858  avg=0.8203  max=1.0000  (do not constrain — 38-month accumulation)
```

**Constraint rationale (v4.0.0):**
```yaml
min_win_rate: 0.11        # removes bottom ~5% only — avg=0.1486
min_expectancy: -2.0      # targets top ~60-65% — avg=-1.69
max_losing_streak: 200    # correct — 38-month max observed=82
min_profit_factor: 0.90   # unchanged
min_trades_per_week: 3.0  # unchanged
# max_drawdown: REMOVED   — accumulates over 38 months
```

### Track B — Broker Integration (pending fixes)
**Project path**: `E:\Trading\Broker_support`
**Confirmed working**: `_make_request()`, `test_connection()`, portfolio fetch, `CSVJournal`, `PositionTracker` snapshot logic.

**Four bugs to fix before any new development**:
1. `get_portfolio()` endpoint: `/demo/portfolio` → `/demo/pnl` (official spec)
2. Orphaned function: second `fetch_closed_trades` in client.py is a free function — indent as class method, delete stub
3. Date param: `from`/`fromDate` → `minDate` (confirmed official param name)
4. Trade alias: `Field(alias='id')` → `Field(alias='positionId')`; add `fees`, `leverage`, `sl_rate`, `tp_rate`

**Empirical test required FIRST** (before any architecture decisions on close-price enrichment):
```python
result = client._make_request('GET', 'api/v1/trading/info/trade/history', params={'minDate': '2026-01-01'})
```
Determines whether demo trades appear in real-account history endpoint, or if snapshot approach is permanent.

---
## Backtesting Pipeline (in order — do not reorder)
```
Stage 0: Validation & Init     (min 3 WFO windows; param name validation vs _PARAM_KEY_MAP) ✅
Stage 1: Random Search         (LHS/random, significance guard, constraint filter) ✅
Stage 2: MC Pre-Filter         (re-evaluates candidates; cheap ruin screen) ✅
Stage 3: GA                    (WFO-aware: random 2 windows/generation + diversity penalty) ✅
Stage 4: Full WFO              (all windows, 4-metric composite consistency score) ✅
Stage 5: MC Deep               (full iterations, all perturbation types, WFO survivors only) ✅
Stage 6: Parameter Sensitivity (±1 step only [OPT-01], fitness delta map, spike = borderline) ✅
Stage 7: Report & Output       (HTML + checklist + JSON/Parquet + SQLite + YAML) ✅
```
All stages fully implemented. OOS gate: implemented but off by default (enforce_oos_gate: false).

---
## Architecture Rules (non-negotiable)
```python
# Contracts: always frozen dataclasses, never raw dicts crossing module boundaries
# Fail fast: invalid config raises at construction, no silent fallbacks
# Datetime: datetime.now(timezone.utc) — NEVER datetime.utcnow() (deprecated Python 3.12+)
# Paths: pathlib.Path + src/utils/paths.py — never hardcoded separators
# Concurrency: ProcessPoolExecutor spawn mode — never multiprocessing fork
# Candidate ID: always CandidateParameterSet.create() — deterministic SHA-256 of params dict
# "Candidate" is NOT a contract type — use CandidateParameterSet
# LIVE_APPROVED: never set in code — operator-only manual action
# strategy_runner.run(): mode_override="core" — NOT mode="core"
# Timing: logger.info only — never print(), never debug flags
# store.close(): always in finally block
# Mutation: snap-then-clamp order — never clamp-then-snap
# Temp YAML filenames: full candidate_id (64 chars) — NEVER truncate (B9H-003)
# expand_zones() returns Dict[str, Dict[str, List]] — per-param lists, NOT Cartesian product (B9F-001)
# _lhs_sample() always returns exactly n candidates — no cap on n (B9I-001)
# actual_net_pnl / actual_total_trades do NOT exist in evaluations table — never query (B9I-002)
# net_pnl for calibration: wfo_window_results.net_pnl only (Stage 4)
# wfo_collapse_drawdown_threshold: default 400.0 pts (DAX). Must be pts, not fraction (COLLAPSE-UNIT)
# scenario.py wires via s.get("wfo_collapse_drawdown_threshold", 400.0) — YAML field optional
# contracts.py validates threshold > 0.0 only — no upper bound (any pts value valid)
# All normalisation constants are DAX-specific. V2-RAR will make them dimensionless.
# run_mc() Stage 2 MUST pass ruin_threshold=scenario.mc_prefilter_ruin_threshold (B8B-013)
# crossover(): zone guard first — if parent zones differ, return parent_a unchanged (B9B-001)
# bollinger_width_ma: step must be 1 (int type — float step causes silent errors) (L-41)
# Do NOT open raw sqlite3.connect() to backtester DB — always use CandidateStore API (L-40, B8-009)
# exploration zone: all filters enabled=true — no toggle params in zone definition
# Stage 0 validates all zone param names against _PARAM_KEY_MAP before any evaluation
# scenario.py constraint loader: use ct.get(key, default) NOT ct[key] — hard lookup = Stage 0 KeyError (B9N-001)
# max_drawdown constraint: DO NOT use for Stage 1 date ranges > 3 months (accumulates across full range)
# max_losing_streak: DO NOT set ≤50 for Stage 1 date ranges > 3 months (observed max 82 over 38 months)
# _MAX_EXPECTED_DRAWDOWN is dataset-range-specific — 3-month: 1_000.0, 38-month: 2_500.0 — do NOT mix tracks
# config["key"] hard lookup fails for any optional stage config block when that stage is
#   disabled and the YAML omits the block. Pattern: config.get("key", {}) + defaults dict.
#   Confirmed affected: mc_prefilter (B9O-003), genetic (B9O-004). Others TBD — audit before new stages.
# TradeSimulator holds df_full (~850MB for 38-month dataset) per worker for LTF tick resolution.
#   max_workers must be ≤ floor(available_RAM_GB / 0.85) for full-history runs. 3-month unaffected.
#   This is architectural — not fixable in DataLoader. max_workers: 2 for full-history.
# stages: toggle block in YAML is now enforced by orchestrator (B9O-005). Default all True.
#   When mc_prefilter disabled: _promote_random_to_mc_pass() must run to seed Stage 3 GA.
# Pre-run cache clear mandatory after data_loader.py upgrades. run_cleaner.py handles automatically.
```
---
## Calibration Constants — TWO TRACKS (NEVER MIX)

### 3-Month Production Track (V1 — FROZEN)
```python
_SIGMOID_SCALE: float = 131.0           # stdev(net_pnl)=261.98 × 0.5 (run 87712cab)
_MAX_EXPECTED_VARIANCE: float = 100_000.0
_MAX_EXPECTED_DRAWDOWN: float = 1_000.0
# Restore these after full-history work. V2-RAR will eliminate this two-track complexity.
```
### Full-History Track (38-month — in progress)
```python
_MAX_EXPECTED_DRAWDOWN: float = 2_500.0  # currently applied in code
_SIGMOID_SCALE: float = TBD              # calculated after calibration v3 run
```
### Shared (both tracks)
```
wfo_collapse_drawdown_threshold      = 400.0 pts  (per-window — never changes)
normalisation_expectancy_ref_pts     = 3.0 pts
normalisation_freq_ref_trades_per_week = 20.0 (CAL-01: raise to 50.0 before V2 only)
mc_prefilter_ruin_threshold          = 0.25 (capital_accumulation)
```
### Recalibration triggers
```
_SIGMOID_SCALE: recalibrate if stdev(net_pnl) shifts >30% from baseline
                3-month baseline: 261.98 pts
                Full-history baseline: TBD (from v3 run)
```
---
## CandidateStore Write API (verified)
```python
store.write_candidate(record: CandidateRecord)
store.write_candidate_stub(candidate: CandidateParameterSet)  # INSERT OR IGNORE — safe always
store.write_wfo_window_result(result: WFOWindowResult, run_id: str)
store.flag_candidate_wfo_insufficient(candidate_id: str, run_id: str)
store.write_wfo_consistency_score(score: WFOConsistencyScore, run_id: str)
store.write_mc_result(result: MCResult, run_id: str)
store.write_sensitivity_profile(profile: SensitivityProfile, run_id: str)
store.write_verdict(verdict: VerdictResult, run_id: str)
store.initialise_run(run_metadata: RunMetadata)
store.set_checkpoint(run_id: str, checkpoint: Checkpoint)  # orchestrator ONLY
store.flush()
store.close()
# There is NO write_fitness_result() method
# query_mc_results: mode is plain string "deep"/"pre_filter" — NOT MCMode enum
# get_candidate_result() returns trades=None / metrics=None ALWAYS — do NOT use for MC input
# write_candidate_stub() MUST be called before any FK-referencing write (B9G-001)
# _wfo_result_id() is deterministic SHA-256[:32] of run_id+candidate_id+window_id (B9H-002)
# INSERT OR REPLACE on wfo_window_results deduplicates correctly (B9H-002)
```
## CandidateStore Read API
```python
store.get_incomplete_run(config_hash: str) -> Optional[str]
store.get_any_incomplete_run() -> Optional[tuple[str, str]]
# Both added Block 9K (B8-009) — replace raw sqlite3 in _resume_or_start()
```
---
## FitnessResult — stored actuals (evaluations table)
```python
actual_win_rate         # 0-1 fraction
actual_max_drawdown     # 0-1 fraction (abs(pts) / ref_pts)
actual_losing_streak    # raw int
actual_trades_per_week  # raw float
actual_expectancy       # raw pts
actual_profit_factor    # raw float
# NOT in evaluations: actual_net_pnl, actual_total_trades
# net_pnl: wfo_window_results.net_pnl (Stage 4 only)
```
---
## Open Issues (prioritised)
```
SIGMOID-SCALE [P1 — now]   Extract net_pnl from d9a81454 or current run. Calculate _SIGMOID_SCALE.
                            Query: SELECT net_pnl FROM wfo_window_results WHERE run_id='d9a81454-...'
PROD-RUN      [P1 — next]  Update backtest_V1_01.yaml (max_workers=2, constraints). Run overnight.
BROKER-TEST   [P1 — now]   Empirical test: does /trade/history return demo trades? Run first.
BROKER-BUGS   [P1 — now]   Four broker_support bugs (see Track B section above)
ORCH-AUDIT    [P2]         Verify no other config["key"] hard lookups remain. Check: walk_forward,
                            sensitivity, report blocks in orchestrator.py and any sub-modules.
RSI-SENS-2    [P2]         RSI zero delta × 6 runs. Remove from V2 search space.
B9N-001       [P3]         scenario.py systematic ct.get() fix for all constraint fields → V2
CAL-01        [P3]         normalisation_freq_ref_trades_per_week 20.0 → 50.0 → V2
RR-CEILING-2  [P3]         Revert safe zone rr_target.max 8.5 → 7.0 in next YAML
V2-RAR        [P1/V2]      Dimensionless normalisation via Rolling Annual Range
DYN-WFO       [P2/V2]      Dynamic window generation from data_range + window_size
B8C-002/003   [P3]         report_generator.py cosmetic HTML — deferred
```
---
## Production YAML State
```
Active (3-month):  configs/backtesting/backtest_production_v1.yaml  (V1.0.0) — FROZEN
Planned:           configs/backtesting/backtest_production_v1.1.yaml
  Changes:         rr_target safe zone max: 8.5 → 7.0  (RR-CEILING-2)

Active (full-hist calib): configs/backtesting/backtest_calibration_fullhistory_v3.yaml (v4.0.0)
  Status: IN PROGRESS

Pending (full-hist prod): configs/backtesting/backtest_V1_01.yaml
  Required before running:
    max_workers: 6 → 2         (MANDATORY — OOM risk identical to calibration)
    min_win_rate: 0.15 → 0.11  (align with full-history distribution)
    min_expectancy: 0.0 → -2.0 (align with full-history distribution)
    _SIGMOID_SCALE: TBD        (update consistency_scorer.py first)
```
---
## V2 Backlog
```
V2-RAR: Normalise all instrument-specific constants via Rolling Annual Range.
  Eliminates two-track calibration complexity. Enables multi-asset without recalibration.
  Do not implement until paper trading results reviewed (2-week mark).

Dynamic WFO windows: data_range as single param, windows auto-derived.

V2 parameter space:
  - Remove rsi_period, rsi_overbought, rsi_oversold (RSI-SENS-2)
  - CAL-01: raise normalisation_freq_ref_trades_per_week to 50.0
  - Time window analysis: intra-day best/worst performance periods
  - Filter discovery mode: which filter combinations survive across auto_go candidates

B9N-001: Systematic scenario.py ct.get() fix for all constraint fields.

Promote _MAX_EXPECTED_DRAWDOWN + _MAX_EXPECTED_VARIANCE to scenario YAML fields
  (same pattern as normalisation_expectancy_ref_pts) — eliminates code changes between run types.
```
---
## Paper Trade Candidates (V1)
```
Primary:   c4f0aea11a3e (run f545f0f2, exploration zone)
           WFO=0.9166, ruin=0.000, worst_dd=0.131, p5=9230
           YAML: outputs\backtesting\trading_yamls\f545f0f2_c4f0aea11a3e_strategy.yaml

Secondary: da38ecc0ddc6 (run f545f0f2, safe zone)
           WFO=0.9257, ruin=0.000, worst_dd=0.471, p5=7547
           YAML: outputs\backtesting\trading_yamls\f545f0f2_da38ecc0ddc6_strategy.yaml

Monitor:   c7ac46b51748, 93586055bd1b (4-window candidates, run f545f0f2)
Do not use: 3a149e208a62 — fragile (7/9 sensitivity params at constraint boundary)
```
---
## Run History
```
87712cab  9I   calibration (3M)     _SIGMOID_SCALE=131.0
4e7135ed  9J   production           3 auto_go — COLLAPSE-UNIT validated
1fcc6398  9J   production           3+2 borderline — best: 1bfa417dc8bb
2ab4fd0e  9K   production           3+2 borderline — RSI active Stage 1; W03-only
b3237ec9  9L   production           3+2 borderline — patch validation; no regression
f545f0f2  9M   production           5 auto_go — V1 DECLARED. First exploration auto_go. W03 broken.
d1ce2b1d  9M   FH-calib-v1          0/200 — Stage 0 KeyError: scenario.py ct["max_drawdown"]. Fixed.
46d6edc7  9N   FH-calib-v2          0/200 — Stage 1: constraints too tight for 38-month eval. Fixed.
756a7829  9O   FH-calib-v3 (v4.0.0) 1/60 — fitness_weights sum=1.50 bug; OOM max_workers=6
9d4669a7  9O   FH-calib-v4 (v4.0.0) 9/60 — OOM persists (TradeSimulator arch); mc_engine KeyError
d9a81454  9O   FH-calib-v5 (v4.0.0) 9/60 — stopped t=3310s; KeyError 'genetic'; WFO may be complete
CURRENT   9O   FH-calib-v6 (v4.0.0) IN PROGRESS — all B9O patches applied
```
---
## eToro API — Confirmed Reference
```
Demo portfolio + open positions + PnL:
  GET /api/v1/trading/info/demo/pnl             (NOT /demo/portfolio — that's wrong)

Real account trade history:
  GET /api/v1/trading/info/trade/history?minDate=YYYY-MM-DD
  Returns: positionId, openRate, closeRate, netProfit, openTimestamp, closeTimestamp,
           instrumentId, isBuy, leverage, fees, stopLossRate, takeProfitRate

Demo trade history: NOT documented — empirical test required

Open demo order by amount:
  POST /api/v1/trading/execution/demo/market-orders-by-amount
  Body: InstrumentID, IsBuy, Leverage, Amount, StopLossRate, TakeProfitRate

Close demo position:
  POST /api/v1/trading/execution/demo/close-position

Market rates (current price):
  GET /api/v1/market/rates?instrumentIds={id}

Auth headers (all requests): x-api-key, x-user-key, x-request-id (UUID)
```
---
## Broker Integration — Architecture Principles
```
demo/real symmetry: endpoints identical except path prefix (/demo/ vs /)
                    paper trading code = production code. Config flag only at go-live.
snapshot approach:  PositionTracker compares portfolio snapshots to detect closures
                    may be the only method available for demo — confirm empirically
instrument IDs:     all eToro endpoints use integer InstrumentID, not ticker symbols
                    DAX ID must be confirmed via /api/v1/market/instruments search
```
---
## Module Map (current state — all from 9K patches + 9O patches)
```
orchestrator.py          — All stages. B8-009 store API. B8B-013 ruin_threshold.
                           B9O-005: stages: toggle guards in _execute_pipeline().
                           _promote_random_to_mc_pass() helper added.
fitness.py               — B8B-003: normalisation_expectancy_ref_pts.
contracts.py             — B8B-003: new field. All invariants current.
candidate_store.py       — B8-009: get_incomplete_run/get_any_incomplete_run.
strategy_runner.py       — B9F-005/B9H-003.
parameter_space.py       — B9F-001.
sampler.py               — B9I-001.
scenario.py              — COLLAPSE-UNIT + B8B-003 wired + B9N-001 ct.get() fix (partial).
ga/crossover.py          — B9B-001: zone guard.
ga/ga_engine.py          — B9O-004: config.get("genetic", {}) + _GA_DEFAULTS dict.
monte_carlo/mc_engine.py — B8B-013: ruin_threshold param. B9O-003: config.get() fixes.
wfo/consistency_scorer.py — B8B-012: scale=131.0. _MAX_EXPECTED_DRAWDOWN=2_500.0 (full-hist track).
strategies/core/data_loader.py — B9O-001 v3.3: sliced strategy cache keyed by date range.
report_generator.py      — B8C-002/003 open (deferred).
src/utils/run_cleaner.py — B9O-002: new utility. Pre-run cache + temp cleaner.
scripts/runners/run_backtester.py — B9O-002: calls clean_environment() before every run.
```
---
## Evaluate / Strategy Signatures
```python
def evaluate_window(
    candidate: CandidateParameterSet,
    window: WFOWindow,
    base_yaml_path: Path,
    temp_dir: Path,
    scenario: ScenarioProfile,
    min_significant_trades: int = 30,
    oos_gate_enabled: bool = False,
) -> WFOWindowResult: ...

def evaluate(
    candidate: CandidateParameterSet,
    base_yaml_path: Path,
    temp_dir: Path,
    min_significant_trades: int = 30,
    retain_temp_yamls: bool = False,
    date_start: Optional[Union[date, datetime]] = None,
    date_end: Optional[Union[date, datetime]] = None,
) -> CandidateResult: ...
```
---
## Lessons Learned (L-01 through L-44)
```
L-01 through L-37: see Block 9I/9J CONTEXT.md
L-38: Threshold read into local variable ≠ threshold passed downstream. Verify full chain.
L-39: Zero delta → non-zero after range tightening = coverage issue, not code.
L-40: Raw sqlite3.connect() to WAL DB is architecturally wrong. Use store API.
L-41: Integer-type parameters must have integer steps. Float step on int type = silent errors.
L-42: A filter producing zero sensitivity delta × 6 runs is structurally inactive for the
      current instrument/timeframe. Remove from search space in next major version.
L-43: Constraints calibrated for short windows (3 months) are invalid for long continuous
      evaluations (38 months). max_drawdown and max_losing_streak accumulate. Correct
      granularity for these constraints is the WFO window (Stage 4), not the full dataset.
L-44: Before removing a YAML field, verify the loader uses dict.get() not dict[].
      Hard key access causes Stage 0 failure — pipeline does not run at all.
L-45: config["key"] hard lookup fails for any optional stage config block when that stage is
      disabled and the YAML omits the block. Pattern: config.get("key", {}) + defaults dict.
      Affects: mc_prefilter, genetic, walk_forward, sensitivity, monte_carlo.deep — audit all.
L-46: TradeSimulator holds df_full (~850MB for 38-month dataset) per worker for LTF tick
      resolution. max_workers must be ≤ floor(available_RAM_GB / 0.85) for full-history runs.
      3-month runs unaffected (~20MB per worker). This is architectural — not fixable in DataLoader.
L-47: Pre-run cache clear is mandatory after data_loader.py upgrades. run_cleaner.py automates this.
L-48: The stages: toggle block in YAML was parsed but never enforced until B9O-005. Any code
      that relies on stages being conditionally disabled must verify the orchestrator guard exists.
```
---
## What NOT To Do
- Do not guess or reconstruct code not available — ask for the file
- Do not modify `src/strategies/` — strategy architecture is frozen
- Do not use `analytics` mode — `core` mode only (`mode_override="core"`)
- Do not add `print()` — use `logger.info`
- Do not set `deployment_status = LIVE_APPROVED` in code
- Do not use `datetime.utcnow()`
- Do not use `Candidate` type — use `CandidateParameterSet`
- Do not patch inside ProcessPoolExecutor workers (spawn boundary)
- Do not use `e2e_test` scenario for production runs
- Do not clamp-before-snap in mutation
- Do not use trade.pnl — use trade.pnl_points (B9F-004)
- Do not call store.get_candidate_result() for MC input (L-15)
- Do not truncate candidate_id in temp YAML filenames (B9H-003)
- Do not use uuid4() as result_id in wfo_window_results (B9H-002)
- Do not call expand_zones() expecting List[Dict] (B9F-001)
- Do not cap _lhs_sample() at min_universe_size (B9I-001)
- Do not query actual_net_pnl or actual_total_trades from evaluations (B9I-002)
- Do not set wfo_collapse_drawdown_threshold as fraction for DAX — must be pts (COLLAPSE-UNIT)
- Do not enable exploration zone before B9B-001 patch (crossover zone guard) is applied
- Do not open raw sqlite3.connect() to backtester DB (L-40)
- Do not call run_mc() in Stage 2 without passing ruin_threshold (L-38, B8B-013)
- Do not use float step on int-type zone parameters (L-41)
- Do not add filter toggle params to exploration zone — fix filters enabled=true
- Do not close RSI-SENS based on Stage 1 raw metric variance alone — Stage 6 is definitive
- Do not raise normalisation_expectancy_ref_pts before observing >20% passers exceeding value
- Do not paper trade candidates with >50% sensitivity parameters at REJECTED_CONSTRAINTS boundary
- Do not use max_drawdown as Stage 1 constraint when Stage 1 date range > 3 months (L-43)
- Do not use max_losing_streak ≤ 50 when Stage 1 date range > 3 months (L-43)
- Do not use ct["key"] for optional constraint fields in scenario.py — use ct.get() (L-44, B9N-001)
- Do not mix 3-month and full-history calibration constants — they are separate tracks
- Do not change _SIGMOID_SCALE = 131.0 without restoring it for 3-month production runs
- Do not call eToro GET /demo/portfolio — correct endpoint is /demo/pnl
- Do not use 'from'/'fromDate' for eToro trade history — correct param is 'minDate'
- Do not architecture broker close-price enrichment before running empirical demo history test
- Do not use config["key"] hard access for any optional stage config block — use config.get("key", {}) (L-45, B9O-003/004)
- Do not set max_workers > 2 for full-history WFO runs — TradeSimulator holds df_full per worker (L-46)
- Do not run full-history YAML without pre-clearing cache — use run_cleaner.py (L-47)
- Do not assume stages: YAML toggles are enforced without checking orchestrator guard (L-48, B9O-005)
---
## Platform
- **OS**: Windows 10, Python 3.13.12
- **Timezone**: OHLCV/signals CET/CEST; pipeline timestamps UTC
- **Paths**: always `src/utils/paths.py`
- **DB**: `outputs/backtesting/backtester.db`
- **Production YAML**: `configs/backtesting/backtest_production_v1.yaml` (V1.0.0)
- **Broker project**: `E:\Trading\Broker_support` (broker-support v0.1.0)
---
## Session Deliverables (end of every session)
- Updated `outputs/CONTEXT_<block>.md`
- `outputs/RUN_ANALYSIS_<run_id>.md` (if production run analysed)
- `outputs/ARCHITECTURE_<block>_DELTA.md` (if structural changes)
- `outputs/OPERATOR_RUNBOOK_<block>_DELTA.md` (if operator-visible changes)
- Updated `SKILL.md` in outputs/ (replace user skill)