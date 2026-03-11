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

## Project Status (2026-03-11, Block 9P end)
```
BACKTESTING ENGINE:    V1 PRODUCTION — PHASE 1 GATE CLOSED (2026-03-11)
                       Run 63b85270: 4 borderline verdicts, 1 no_go. Runtime 7h (confirmed).
                       Paper trade candidates identified (see below).
                       Confirmation run PENDING: sensitivity.input_count raised to 10
                       to expose 5 additional MC-evaluated candidates for verdict.
                       After confirmation run → Phase 1 fully closed.

BROKER INTEGRATION:    broker_support package — connection confirmed, 4 bugs to fix.
                       Phase 0 begins after Phase 1 confirmation run completes.
                       Empirical demo history test required before any new architecture.

CTP ROADMAP:           Phase 1 (full-history backtesting) CLOSING.
                       Phase 0 (broker fixes) = next active track.
                       Phase 2 = automated paper trading (blocked on Phase 0).
                       V2 architecture blueprint complete (RawDataStore+WindowSlicer+SignalCache).
                       V3 = Strategy Setup Builder (meta-optimiser over configuration space).
```

---
## DUAL-TRACK CONTEXT

### Track A — Backtesting Full-History (Phase 1 closing)

**Run 63b85270 — ANALYSED (2026-03-11):**
```
Config:   backtest_V1_01.yaml v3.0.0
Runtime:  25742.5s (~7 hours) — confirmed acceptable for overnight cadence
Stage 1:  234/400 passed (58.5%)
Stage 4:  30 candidates, all collapsed (windows_evaluated < 13)
Stage 5:  4 candidates ruin=0.000 — MC-DEEP-FULLHIST severity DOWNGRADED
Stage 7:  4 borderline, 1 no_go
```

**Paper trade candidates from run 63b85270:**
```
PRIMARY:   c424a0e04327 — WFO=0.8108, ruin=0.000, frac_pos=1.000
           1 spike on atr_multiplier (upward asymmetric — value improvable)
           YAML: outputs/backtesting/trading_yamls/63b85270_c424a0e04327_strategy.yaml

SECONDARY: 20745ca991be — WFO=0.7201, ruin=0.054, frac_pos=1.000
           High avg_equity=8099, high variance, regime-dependent
           YAML: outputs/backtesting/trading_yamls/63b85270_20745ca991be_strategy.yaml

MONITOR:   c42f8b009283 — WFO=0.6473, ruin=0.000
           Parameter fragility (many REJECTED_CONSTRAINTS on perturbation)
           YAML: outputs/backtesting/trading_yamls/63b85270_c42f8b009283_strategy.yaml

LOW PRI:   c4f0aea11a3e — WFO=0.6233, ruin=0.000, frac_pos=0.167
           12/13 windows evaluated but only 1 profitable — structural concern
           YAML: outputs/backtesting/trading_yamls/63b85270_c4f0aea11a3e_strategy.yaml
```

**Missed candidate — no verdict in run 63b85270:**
```
c209820886c8 — WFO=0.5699, ruin=0.000, avg_equity=9370 (BEST MC profile of all 10)
Ranked 10th by WFO → outside sensitivity.input_count=5 → no verdict assigned.
Will receive verdict in confirmation run (sensitivity.input_count raised to 10).
```

**Confirmation run config change (one line only):**
```yaml
sensitivity:
  input_count: 10    # was 5 — gives verdicts to all MC-evaluated candidates
```

**Calibration constants — CONFIRMED FINAL (full-history track):**
```python
_SIGMOID_SCALE: float = 310.0           # N=231, stdev=620.09 (runs 2912e028, 519f84e2) CONFIRMED
_MAX_EXPECTED_DRAWDOWN: float = 2_500.0 # full-history track — correct
# Note: run 63b85270 reported suggested scale=359.4 (N=3389 including GA samples) — DO NOT USE
# Correct calibration requires Stage 1+4 only runs. 310.0 remains the confirmed value.
```

**Stage 1 distributions (38-month — stable across 5 runs including 63b85270):**
```
win_rate:      min=0.081  avg=0.151  max=0.251  (confirmed consistent with prior baseline)
expectancy:    min=-4.79  avg=-1.82  max=1.58
profit_factor: min=0.43   avg=0.821  max=1.11
trades/week:   min=1.28   avg=32.85  max=91.09
losing_streak: min=18     avg=46.7   max=91
```

**Confirmed constraint values for full-history runs:**
```yaml
min_win_rate: 0.11         # removes bottom ~5% only
min_expectancy: -2.0       # targets top ~60-65%
min_profit_factor: 0.75    # no failures observed
max_losing_streak: 200     # 38-month max observed=91
min_trades_per_week: 3.0   # unchanged
# max_drawdown: REMOVED    — accumulates over 38 months
mc_prefilter: false        # compounds 38-month perturbation into false ruin — DISABLE
go_wfo_floor: 0.40         # structural W02/W04/W05 suppress scores
borderline_wfo_floor: 0.25
max_workers: 2             # MANDATORY — OOM confirmed at 6 on cold cache (8GB RAM)
```

**Full-history run performance (confirmed):**
```
Runtime: ~7 hours (25742.5s confirmed in run 63b85270)
This is acceptable for overnight cadence. B9O-009 (V2 shared memory) is not
urgently needed for runtime — it remains open for OOM safety margin only.
```

**RSI confirmed dead — RSI-SENS-2 CLOSED:**
Zero delta on rsi_period, rsi_overbought, rsi_oversold across ALL candidates in
run 63b85270. Confirmed across 7+ runs. Remove from V2 search space.

### Track B — Broker Integration (Phase 0 — next active)
**Project path**: `E:\Trading\Broker_support`
**Confirmed working**: `_make_request()`, `test_connection()`, portfolio fetch, `CSVJournal`,
`PositionTracker` snapshot logic.

**Four bugs to fix before any new development:**
1. `get_portfolio()` endpoint: `/demo/portfolio` → `/demo/pnl` (official spec)
2. Orphaned function: second `fetch_closed_trades` in client.py is a free function — indent
   as class method, delete stub
3. Date param: `from`/`fromDate` → `minDate` (confirmed official param name)
4. Trade alias: `Field(alias='id')` → `Field(alias='positionId')`; add `fees`, `leverage`,
   `sl_rate`, `tp_rate`

**Empirical demo history test (P1 — before any architecture decisions):**
```python
# Run before any new broker code
client._make_request('GET', 'api/v1/trading/info/trade/history', params={'minDate': '2026-01-01'})
# Determines if demo trades appear in real-account history endpoint
# This governs whether snapshot-comparison is permanent or can be replaced by direct query
```

---
## Backtesting Pipeline (in order — do not reorder)
```
Stage 0: Validation & Init     (min 3 WFO windows; param name validation vs _PARAM_KEY_MAP) ✅
Stage 1: Random Search         (LHS/random, significance guard, constraint filter) ✅
Stage 2: MC Pre-Filter         (re-evaluates candidates; cheap ruin screen) ✅ — DISABLED full-history
Stage 3: GA                    (WFO-aware: random 2 windows/generation + diversity penalty) ✅
Stage 4: Full WFO              (all windows, 4-metric composite consistency score) ✅
Stage 5: MC Deep               (full iterations, all perturbation types, WFO survivors only) ✅
Stage 6: Parameter Sensitivity (±1 step only [OPT-01], fitness delta map, spike = borderline) ✅
Stage 7: Report & Output       (HTML + checklist + JSON/Parquet + SQLite + YAML) ✅
```
All stages fully implemented. OOS gate: implemented but off by default (enforce_oos_gate: false).
MC Pre-Filter: disable for full-history (38-month) runs — see L-54.

**Stage input count relationships (CRITICAL — misreading causes false bug reports):**
```
Stage 5 MC Deep input:     monte_carlo.deep.input_count  (default 10) — top N by WFO score
Stage 6 Sensitivity input: sensitivity.input_count       (default 5)  — top N by WFO score
Stage 7 Verdict input:     sensitivity.input_count       (same set as Stage 6 — ALWAYS PAIRED)

To give verdicts to ALL MC-evaluated candidates: set sensitivity.input_count = monte_carlo.deep.input_count
Do NOT raise monte_carlo.deep.input_count to get more verdicts — raise sensitivity.input_count.
```

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
# Temp YAML filenames: full candidate_id (64 chars) — NEVER truncate
# expand_zones() returns Dict[str, Dict[str, List]] — per-param lists, NOT Cartesian product
# _lhs_sample() always returns exactly n candidates — no cap on n
# actual_net_pnl / actual_total_trades do NOT exist in evaluations table — never query
# net_pnl for calibration: wfo_window_results.net_pnl only (Stage 4)
# wfo_collapse_drawdown_threshold: default 400.0 pts (DAX). Must be pts, not fraction
# scenario.py wires via s.get("wfo_collapse_drawdown_threshold", 400.0) — YAML field optional
# contracts.py validates threshold > 0.0 only — no upper bound (any pts value valid)
# All normalisation constants are DAX-specific. V2-RAR will make them dimensionless.
# run_mc() Stage 2 MUST pass ruin_threshold=scenario.mc_prefilter_ruin_threshold
# crossover(): zone guard first — if parent zones differ, return parent_a unchanged
# bollinger_width_ma: step must be 1 (int type — float step causes silent errors)
# Do NOT open raw sqlite3.connect() to backtester DB — always use CandidateStore API
# exploration zone: all filters enabled=true — no toggle params in zone definition
# Stage 0 validates all zone param names against _PARAM_KEY_MAP before any evaluation
# scenario.py constraint loader: use ct.get(key, default) NOT ct[key] — hard lookup = Stage 0 KeyError
# max_drawdown constraint: DO NOT use for Stage 1 date ranges > 3 months (accumulates)
# max_losing_streak: DO NOT set ≤50 for Stage 1 date ranges > 3 months (observed max 91)
# _MAX_EXPECTED_DRAWDOWN is dataset-range-specific — 3-month: 1_000.0, 38-month: 2_500.0 — do NOT mix tracks
# config["key"] hard lookup fails for any optional stage config block when that stage is
#   disabled and the YAML omits the block. Pattern: config.get("key", {}) + defaults dict.
# mc_prefilter: DISABLE for full-history runs — 38-month MC perturbation compounds into false ruin
# max_workers: HARD LIMIT 2 for full-history WFO — cold-cache read_parquet 897MB × workers = OOM
# _SIGMOID_SCALE calibration: use Stage 1+4 only runs (pure full-window WFO net_pnl distribution).
#   GA partial-window (2-window) net_pnl produces different stdev — do NOT use for calibration.
#   run_63b85270 suggested 359.4 (N=3389 including GA samples) — DO NOT USE. 310.0 is confirmed.
# RSI parameters: zero sensitivity delta confirmed across 7+ full-history runs. Remove from V2.
#
# verdict.py — CONFIRMED CORRECT (reviewed 2026-03-10):
#   - Uses >= on go_wfo_floor. Float comparison correct.
#   - NO_GO fires only if wfo_pillar_no_go OR mc_pillar_no_go.
#   - window_collapse_flag=True → BORDERLINE (modifier only), never NO_GO.
#   - get_mc_result(candidate_id, MCMode.DEEP): uses mode.value="deep" in SQL — correct.
#   - Stage 7 uses sens_config["input_count"] (default 5) for verdict candidates.
#   - Stage 5 uses mc_config["input_count"] (default 10) — may evaluate MORE than get verdicts.
#   - Candidate in Stage 5 MC but not Stage 7 verdicts = expected — outside top-N WFO.
#   - VERDICT-BUG is CLOSED — was a misread of query output. No bug exists.
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

### Full-History Track (38-month — CONFIRMED FINAL)
```python
_SIGMOID_SCALE: float = 310.0           # N=231, stdev=620.09 (runs 2912e028, 519f84e2) CONFIRMED
_MAX_EXPECTED_DRAWDOWN: float = 2_500.0 # currently applied in code
```

### Shared (both tracks)
```
wfo_collapse_drawdown_threshold        = 400.0 pts  (per-window — never changes)
normalisation_expectancy_ref_pts       = 3.0 pts
normalisation_freq_ref_trades_per_week = 20.0 (CAL-01: raise to 50.0 before V2 only)
mc_prefilter_ruin_threshold            = 0.25 (capital_accumulation — Stage 2 disabled full-history)
```

---
## OOM Architecture — max_workers Constraint
```
Root cause (confirmed): pd.read_parquet() on 897MB LTF Parquet × N workers simultaneously
on cold cache (run_cleaner wipes cache before every run) = N × 897MB peak → PyArrow OOM.

Cold cache peak:
  6 workers: 6 × 897MB = 5.38GB → system crash (confirmed)
  2 workers: 2 × 897MB = 1.79GB → within 8GB RAM budget (confirmed stable)

Actual runtime at max_workers=2 over 38 months: ~7 hours (confirmed run 63b85270).
This is acceptable for overnight runs. B9O-009 priority is OOM safety, not runtime.

V2 fix: RawDataStore loads files once in parent process → WindowSlicer places slices in
named SharedMemory blocks → workers map shm (zero-copy, ~20MB per worker).
With shared memory: 6 workers × 20MB = 120MB total. max_workers constraint removed.
```

---
## Patches Applied (all blocks — complete)
| Patch | File | Description |
|-------|------|-------------|
| B9O-001 | data_loader.py | Sliced strategy cache (apply_date_range=False path) |
| B9O-002 | run_cleaner.py | Pre-run cache + temp YAML cleaner |
| B9O-003 | mc_engine.py | config.get() for mc_prefilter block |
| B9O-004 | ga_engine.py | config.get() + _GA_DEFAULTS dict |
| B9O-005 | orchestrator.py | stages: toggle enforcement |
| B9O-006 | data_loader.py | Slice-before-cache for LTF/HTF |
| B9O-007 | data_loader.py | Warmup-buffered df_full for WFO windows |
| B9O-008 | data_loader.py | Slice-before-sort for LTF loading peak |

**data_loader.py current version: 3.5.0**

---
## Module Map (current state — all patches applied)
```
orchestrator.py          — All stages. stages: toggle guards. _promote_random_to_mc_pass() helper.
                           Stage 7 uses sens_config["input_count"] for verdict set — CORRECT BY DESIGN.
fitness.py               — normalisation_expectancy_ref_pts wired.
contracts.py             — All invariants current.
candidate_store.py       — get_mc_result(candidate_id, MCMode.DEEP) uses mode.value="deep" — correct.
                           query_mc_results(run_id, mode: str) — diagnostic only, not verdict path.
strategy_runner.py       — date_start/date_end injection. Full 64-char temp YAML filenames.
parameter_space.py       — expand_zones() returns Dict[str, Dict[str, List]].
sampler.py               — _lhs_sample() returns exactly n candidates (cycling strata).
scenario.py              — COLLAPSE-UNIT fix. ct.get() for all constraint fields.
ga/crossover.py          — Zone guard: cross-zone parents return parent_a unchanged.
ga/ga_engine.py          — config.get("genetic", {}) + _GA_DEFAULTS dict.
monte_carlo/mc_engine.py — ruin_threshold param. config.get() for all blocks.
wfo/consistency_scorer.py — _SIGMOID_SCALE=310.0. _MAX_EXPECTED_DRAWDOWN=2_500.0.
evaluation/verdict.py    — CONFIRMED CORRECT 2026-03-10. Two-pillar logic verified.
strategies/core/data_loader.py — v3.5.0: all B9O patches applied.
src/utils/run_cleaner.py — Pre-run cache + temp cleaner. Auto-called by runner.
scripts/runners/run_backtester.py — clean_environment() before every run.
```

---
## CandidateStore — Two MC Query Methods (IMPORTANT DISTINCTION)
```python
# For pipeline verdict computation (Stage 7):
store.get_mc_result(candidate_id: str, mode: MCMode) -> Optional[MCResult]
  # Uses MCMode enum: MCMode.DEEP, MCMode.PRE_FILTER
  # mode.value used in SQL WHERE clause: "deep" or "pre_filter"

# For diagnostic/reporting queries (query_run.py):
store.query_mc_results(run_id: str, mode: str) -> List[Dict]
  # Uses plain string: "deep" or "pre_filter"
  # NOT used in verdict computation path
```

---
## CandidateStore Write API (verified)
```python
store.write_candidate(record: CandidateRecord)
store.write_candidate_stub(candidate: CandidateParameterSet, run_id, stage, generation)
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
# get_candidate_result() returns trades=None / metrics=None ALWAYS — do NOT use for MC input
# write_candidate_stub() MUST be called before any FK-referencing write
# _wfo_result_id() is deterministic SHA-256[:32] of run_id+candidate_id+window_id
# INSERT OR REPLACE on wfo_window_results deduplicates correctly
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
    oos_gate_enabled: bool = False,   # 7th positional arg — pass positionally in pool.submit()
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
## Lessons Learned (L-01 through L-58)
```
L-01 through L-48: see prior SKILL.md versions and CHANGE_LOG.md

L-49: df_full in DataBundle is consumed by TradeSimulator → RiskManager ONLY.
      WFO window evaluations need only a warmup-buffered slice (window_start − 200 bars).
      DataLoader is the correct fix location — not frozen TradeSimulator/RiskManager.

L-50: Diagnostic test false positive: assertion accidentally matched _WFO_WARMUP_BARS
      via a different code path. Always verify the constant exists in target file
      before trusting a test that checks for it.

L-51: run_cleaner.py clears cache before every run. OOM on cache miss (first worker
      per window) is the actual failure mode — not stale cache.

L-52: B9O-006 fixed what gets cached (the slice) but not the sort_index() peak
      DURING loading. sort_index() calls .copy() on the full DataFrame internally.
      Fix: check is_monotonic_increasing and slice before sort for sorted Parquet files.

L-53: max_workers=6 OOM root cause: pd.read_parquet() on 897MB LTF file × 6 workers
      simultaneously on cold cache. Fix: max_workers=2 (1.79GB peak).
      Permanent fix: V2 shared memory architecture.
      Actual runtime at max_workers=2: ~7 hours — acceptable for overnight cadence.

L-54: MC pre-filter is NOT safe for 38-month continuous evaluation. MC perturbation
      compounds over 38-month equity curve producing false ruin signals.
      Fix: disable mc_prefilter for all full-history runs. Stage 4 WFO is the primary gate.

L-55: Stage 7 verdict input_count comes from sensitivity.input_count, not
      monte_carlo.deep.input_count. A candidate in Stage 5 MC but absent from Stage 7
      verdicts is expected behaviour — outside top-N for sensitivity/verdict.
      VERDICT-BUG is CLOSED. verdict.py, orchestrator.py, candidate_store.py confirmed correct.

L-56: Before declaring a verdict logic bug, cross-check Stage 5 and Stage 7 input counts
      in the YAML. They use DIFFERENT config keys.

L-57: MC-DEEP-FULLHIST severity was overstated based on run 9f73d667. Run 63b85270
      produced 4 candidates with ruin=0.000 on the 38-month dataset. False ruin
      only affects lower WFO candidates (5d89157ad626, 2cd6f1886371). Top WFO
      candidates (score > 0.70) are robust to full-history MC evaluation.
      The V2 per-window MC fix remains desirable but is not urgently blocking.

L-58: Null window results from temp YAML file-lock (WinError 32 / WINZIP-32) affect
      only the GA stage on Windows. They produce None fitness for affected windows but
      do not corrupt the run — WFO scoring handles missing windows via windows_evaluated.
      These errors are cosmetic and non-blocking. Do not investigate unless rate exceeds
      ~5 null windows per run or temp/ accumulates unreleased files.
```

---
## Open Issues
```
B9O-009  V2 shared memory architecture (RawDataStore + WindowSlicer + SignalCache).
         Eliminates max_workers OOM risk. Runtime is acceptable at 7h without this.
         Priority: OOM safety margin, not runtime. Deferred to Phase 3 (V2).

WINZIP-32  WinError 32 temp YAML file lock during GA stage worker teardown on Windows.
           Cosmetic — pipeline completes. Observed rate: ~4 null windows per run.
           V2 fix: per-worker temp dirs, clean at worker exit. Deferred to V2.

RSI-SENS-2  RSI confirmed dead across 7+ runs. Remove from V2 search space.
            STATUS: CONFIRMED CLOSED — action deferred to V2 implementation.

MC-DEEP-FULLHIST  SEVERITY DOWNGRADED. Top WFO candidates (>0.70) robust on 38-month MC.
                  False ruin affects lower-ranked candidates only. V2 per-window MC
                  evaluation still recommended for correctness but not blocking.
```

---
## What NOT to Do
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
- Do not use trade.pnl — use trade.pnl_points
- Do not call store.get_candidate_result() for MC input
- Do not truncate candidate_id in temp YAML filenames (must be 64 chars)
- Do not call expand_zones() expecting List[Dict]
- Do not cap _lhs_sample() at min_universe_size
- Do not query actual_net_pnl or actual_total_trades from evaluations
- Do not set wfo_collapse_drawdown_threshold as fraction for DAX — must be pts
- Do not open raw sqlite3.connect() to backtester DB
- Do not call run_mc() in Stage 2 without passing ruin_threshold
- Do not use float step on int-type zone parameters
- Do not use max_drawdown as Stage 1 constraint when Stage 1 date range > 3 months
- Do not use max_losing_streak ≤ 50 when Stage 1 date range > 3 months
- Do not use ct["key"] for optional constraint fields in scenario.py — use ct.get()
- Do not mix 3-month and full-history calibration constants — they are separate tracks
- Do not change _SIGMOID_SCALE = 131.0 without restoring it for 3-month production runs
- Do not use _SIGMOID_SCALE=359.4 from run 63b85270 — computed from GA+full samples, wrong
- Do not use _SIGMOID_SCALE=221.1 from run 9f73d667 — computed from GA 2-window samples, wrong
- Do not calibrate _SIGMOID_SCALE from runs with GA enabled — Stage 1+4 only runs only
- Do not call eToro GET /demo/portfolio — correct endpoint is /demo/pnl
- Do not use 'from'/'fromDate' for eToro trade history — correct param is 'minDate'
- Do not architecture broker close-price enrichment before running empirical demo history test
- Do not use config["key"] hard access for any optional stage config block
- Do not set max_workers > 2 for full-history WFO runs — OOM confirmed at 6
- Do not run full-history YAML without pre-clearing cache — use run_cleaner.py
- Do not enable mc_prefilter for full-history (38-month) runs — false ruin confirmed
- Do not assume a candidate in Stage 5 MC but absent from Stage 7 verdicts is a bug
- Do not raise monte_carlo.deep.input_count expecting more verdicts — raise sensitivity.input_count
- Do not re-examine verdict.py, orchestrator.py, or candidate_store.py for VERDICT-BUG — CLOSED

---
## Platform
- **OS**: Windows 10, Python 3.13.12
- **Timezone**: OHLCV/signals CET/CEST; pipeline timestamps UTC
- **Paths**: always `src/utils/paths.py`
- **DB**: `outputs/backtesting/backtester.db`
- **Production YAML**: `configs/backtesting/backtest_V1_01.yaml` (v3.0.0)
- **Confirmation run YAML**: same YAML with `sensitivity.input_count: 10`
- **Broker project**: `E:\Trading\Broker_support` (broker-support v0.1.0)

---
## Session Deliverables (end of every session)
- Updated `outputs/CONTEXT_<block>.md`
- `outputs/RUN_ANALYSIS_<run_id>.md` (if production run analysed)
- `outputs/ARCHITECTURE_<block>_DELTA.md` (if structural changes)
- `outputs/OPERATOR_RUNBOOK_<block>_DELTA.md` (if operator-visible changes)
- Updated `SKILL.md` in outputs/ (replace user skill)