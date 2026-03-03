# TECHNICAL_SPEC.md
## Backtesting & Optimization Framework — Technical Specification
**Version**: 1.1.0
**Date**: 2026-03-03
**Phase**: Phase 1 — Design → Phase 6 — Hardening & Delivery
**Status**: Complete

---

## Table of Contents
1. [Resolved Open Decisions](#1-resolved-open-decisions)
2. [Inter-Module Contracts](#2-inter-module-contracts)
3. [Enumerations](#3-enumerations)
4. [Module Interface Signatures](#4-module-interface-signatures)
5. [Configuration Schema Reference](#5-configuration-schema-reference)

---

## 1. Resolved Open Decisions

### D-01 — Strategy Integration Mode
**Resolution**: Direct Python call — import and invoke the strategy orchestrator within each worker process.

**Rationale**: The strategy architecture already exposes a clean `run(config: StrategyConfig) -> RunResult` entry point. Calling it directly from a `ProcessPoolExecutor` worker avoids subprocess spawn overhead (which is substantial on Windows for hundreds of candidates) while maintaining full process isolation. Each worker is a separate spawned process — a strategy crash cannot corrupt the parent orchestrator or sibling workers. `CacheManager.clear_all_caches()` is called in the `finally` block of every worker, ensuring no cross-candidate cache contamination.

**Implications**: The strategy package must be importable in worker processes. The worker receives a `CandidateParameterSet`, builds a temp YAML, calls `StrategyConfig.from_yaml(path)`, runs the strategy, and returns the result contract. No subprocess, no IPC beyond the `ProcessPoolExecutor` serialisation boundary.

**Benchmark requirement**: Phase 2 must time a batch of 50 candidate evaluations in direct-call mode and confirm it fits within the Stage 1 time budget. If it does not, revert to subprocess mode (next best option).

---

### D-02 — SQLite Write Concurrency
**Resolution**: SQLite WAL (Write-Ahead Logging) mode with a serialised write queue.

**Rationale**: WAL mode allows multiple readers and one writer concurrently, which is exactly the access pattern during parallel candidate evaluation (6 readers returning results + 1 writer draining results). However, rather than having each worker attempt a direct SQLite write (which creates contention even in WAL mode under 6 concurrent writers), all write operations are submitted to a single-writer queue managed by the orchestrator process. Workers submit their `CandidateRecord` to a `multiprocessing.Queue`; a dedicated writer thread drains this queue and performs all SQLite writes sequentially. This eliminates write contention entirely while keeping WAL mode for read performance.

**Fallback**: If the queue-based approach introduces unacceptable latency, fall back to per-candidate JSON file writes during parallel stages, merged into SQLite at stage completion. This is simpler but loses the real-time queryability during a run.

**Benchmark requirement**: Phase 2 must prototype the queue-based WAL writer under simulated 6-worker load and confirm no data loss or corruption over 500 concurrent write submissions.

---

### D-03 — Temporary YAML Lifecycle
**Resolution**: One temporary YAML per candidate, named by the candidate's parameter hash (e.g. `temp_candidate_{hash[:12]}.yaml`), written to a configurable `temp_dir` at the start of evaluation and deleted in the `finally` block of the strategy runner after the result is captured.

**Rationale**: Named-by-hash files allow correlation between a temp YAML and its candidate in logs, which is invaluable for debugging. Deletion in `finally` ensures cleanup even on worker crash. If the operator wants to inspect temp YAMLs for debugging (e.g. to diagnose a systematic strategy failure), they can set `output.retain_temp_yamls: true` in the YAML, which suppresses the deletion.

---

### D-04 — GA Population Seeding
**Resolution**: Top-N by fitness score from `MC_PREFILTER_PASS`, where N equals the configured GA population size. No diversity-based seeding in v1.

**Rationale**: The GA's diversity penalty (GA-07) handles population diversity during evolution. Seeding with diversity is redundant if the diversity penalty is correctly calibrated. Top-N seeding ensures the GA starts from the best-known configurations, not random ones. If the diversity penalty proves insufficient (detectable via CandidateStore monitoring — parameter spread collapses across generations), this decision can be revisited without pipeline restructuring.

---

### ~~D-05~~ — GA Lightweight WFO Window Selection
**Resolution** (carried from BACKTESTER_PLAN.md v1.2): Randomly sample 2 windows from the full WFO window list at the start of each generation. Sampling is done without replacement per generation. Requires minimum 3 configured WFO windows (validated in Stage 0).

---

### D-06 — Stage Transition Candidate Counts (Default Values)
**Resolution**: The following default counts are the starting point. All are configurable in YAML. They must be profiled in Phase 3 and Phase 6 and adjusted if the 4-hour target is at risk.

| Transition | Default Count | Configurable Key |
|---|---|---|
| Random Search pool size | 200 per zone (up to 3 zones = 600 total) | `random_search.samples_per_zone` |
| MC Pre-Filter input (from Random) | Top 120 | `mc_prefilter.input_count` |
| GA initial population (from MC Pre-Filter) | Top 60 | `genetic.population_size` |
| GA generations | 30 | `genetic.generations` |
| Full WFO input (from GA + Random combined) | Top 30 | `walk_forward.input_count` |
| MC Deep input (from WFO) | Top 10 | `monte_carlo.deep.input_count` |
| Sensitivity input (from MC Deep) | Top 5 | `sensitivity.input_count` |
| Final shortlist (in report) | Top 3–5 | `output.shortlist_count` |

**Time budget reasoning**: At ~4 seconds per candidate evaluation in core mode, 600 Random + 120 MC Pre-filter + (60 × 30 GA candidates) + 30 WFO × N windows + 10 × MC Deep + 5 × Sensitivity perturbations fits comfortably within 4 hours at 6 workers. This must be verified empirically in Phase 3.

---

### D-07 — Composite Verdict Thresholds (Starting Values)
**Resolution**: The following are starting values for Phase 6 calibration. They are YAML-configurable per scenario, never hardcoded. After the first real pipeline run on WBWSStrategy data, these will be recalibrated.

| Threshold | go | borderline zone | no_go |
|---|---|---|---|
| WFO consistency score | ≥ 0.65 | 0.40 – 0.65 | < 0.40 |
| MC deep ruin probability | ≤ 0.05 (5%) | 0.05 – 0.15 | > 0.15 (15%) |
| Sensitivity spike delta | < 0.15 fitness drop | — | N/A (spike = borderline always) |

**Confirmed boundary operators** (from `verdict.py` source, verified Block 5, 2026-03-03):

```python
# WFO pillar
wfo_pillar_go    = wfo_composite >= go_wfo_floor        # >= INCLUSIVE at go threshold
wfo_pillar_no_go = wfo_composite < borderline_wfo_floor  # < strictly less than

# MC pillar
mc_pillar_go    = ruin_prob <= go_mc_ruin_ceiling        # <= INCLUSIVE at go threshold
mc_pillar_no_go = ruin_prob > borderline_mc_ruin_ceiling  # > strictly greater than

# ruin_prob is None → mc_pillar_no_go = True → NO_GO
# (verdict.py logs WARNING — expected, not a bug)

# oos_gate_triggered = oos_gate_enabled AND wfo_score.oos_gate_triggered
# Either condition alone does NOT trigger the flag.

# Final verdict
if wfo_pillar_no_go OR mc_pillar_no_go:          → NO_GO
elif wfo_pillar_go AND mc_pillar_go AND no flags: → AUTO_GO
else:                                             → BORDERLINE
```

**Why inclusive operators at go thresholds matter**: Using `>` instead of `>=` at the go floor would incorrectly classify a candidate scoring exactly at the go threshold as BORDERLINE rather than AUTO_GO. The go thresholds are intended as inclusive gates — a candidate that meets the standard exactly passes. Confirmed and locked; do not change to strict inequality.

**Note**: The `capital_accumulation` production scenario thresholds are `go_wfo_floor=0.65`, `borderline_wfo_floor=0.40`, `go_mc_ruin_ceiling=0.05`, `borderline_mc_ruin_ceiling=0.15`. These are D-07 starting values — recalibrate after the first real run. All thresholds are scenario-specific and live in `backtest_template.yaml`.

---

### D-08 — Sensitivity Map Scope
**Resolution**: All optimizable parameters. The cost of Stage 6 is: `N_candidates × N_parameters × 4 perturbations × evaluation_time`. With 5 candidates, ~15 parameters, and 4 perturbations each, that is 300 evaluations. At 4 seconds each with 6 workers, this is approximately 200 seconds — well within budget.

---

### D-09 — Parquet vs JSON per Candidate
**Resolution**: Both, configurable in YAML. Both formats are enabled by default. JSON for universal compatibility; Parquet for efficient ML ingestion. Both contain identical data. Operators with storage constraints can disable either via `output.formats.json: false` or `output.formats.parquet: false`.

---

### D-10 — HTML Report Generator
**Resolution**: Build new (`report_generator.py` in the backtester package). The existing strategy `ReportGenerator` produces a single-run, single-candidate analytics report. The backtester report is multi-candidate, multi-stage, and scenario-framed — structurally different enough that extending the existing one would produce a coupled mess. The new generator reads entirely from the `CandidateStore` and produces a self-contained HTML file using Jinja2 templates.

---

### D-11 — GA Diversity Penalty Distance Metric
**Resolution**: Hybrid metric. For continuous parameters (e.g. ATR multiplier, RSI thresholds), use normalised Euclidean distance in the parameter space (each parameter normalised to [0, 1] by its zone range). For discrete parameters (e.g. session filter, timeframe), use Hamming distance (0 = same value, 1 = different value). The overall distance is the weighted average of the two sub-distances, with weights proportional to the fraction of parameters that are continuous vs. discrete.

**Rationale**: A pure Euclidean metric applied to discrete parameters is meaningless (the distance between `H1` and `H4` timeframe is not numerically meaningful). A pure Hamming metric on continuous parameters loses information about how far apart two values are. The hybrid approach handles both correctly.

---

### D-12 — IS/OOS Gate Default Configuration
**Resolution**: `enforce_oos_gate: false` by default. Threshold when enabled: > 50% degradation triggers borderline flag. The default is off because IS/OOS delta is sensitive to window placement artefacts that can produce spurious borderline flags for genuinely robust strategies. Operators who want this gate can enable it in the YAML.

---

## 1a. Windows Spawn Mode — Test Patch Constraint

> **Cross-reference**: ARCHITECTURE.md §9 for full patch rules table and confirmed error message.

On Windows, `ProcessPoolExecutor` uses the `spawn` start method. Child worker processes are fresh Python interpreters — they import modules from scratch and do **not** inherit any `unittest.mock.patch` decorators applied in the parent process.

**Rule**: `unittest.mock patches DO NOT cross the ProcessPoolExecutor spawn boundary on Windows.`

This has two concrete consequences for test design:

**Stage 6 (Sensitivity)** — tests that exercise the orchestrator's Stage 6 loop behaviour (continue on failure, write profile, advance checkpoint) must patch `src.backtesting.orchestrator.evaluate_sensitivity`, not the worker function `src.backtesting.evaluation.sensitivity._evaluate_perturbation`. Patching the worker function is silently ignored — the original runs in the child process.

**Stage 5 (MC Deep)** — `run_mc` is a local import inside `_run_stage_5_mc_deep`. Patch it at `src.backtesting.monte_carlo.mc_engine.run_mc` (module level), not `src.backtesting.orchestrator.run_mc` (AttributeError — not on orchestrator's namespace).

**Confirmed failure mode (Block 4, ROB-09)**:
```
ERROR: Can't pickle <class 'unittest.mock.MagicMock'>:
       it's not the same object as unittest.mock.MagicMock
```
Root cause: the mock object failed pickling when `ProcessPoolExecutor` attempted to transmit it to a worker process. The fix is to patch above the worker boundary, not inside it.

---

## 2. Inter-Module Contracts

All contracts are frozen dataclasses. No mutable fields. Validation in `__post_init__`. No raw dicts cross module boundaries.

```python
# ============================================================
# contracts.py  (src/backtesting/contracts.py)
# All inter-module contracts for the backtesting framework.
# ============================================================

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Dict, FrozenSet, List, Optional, Tuple
import uuid


# ────────────────────────────────────────────────────────────
# Enumerations
# ────────────────────────────────────────────────────────────

class Checkpoint(Enum):
    """Pipeline stage checkpoint states, in execution order."""
    NOT_STARTED           = 0
    RUN_INITIALISED       = 1
    RANDOM_SEARCH_COMPLETE = 2
    MC_PREFILTER_COMPLETE  = 3
    GA_COMPLETE            = 4
    WFO_COMPLETE           = 5
    MONTE_CARLO_COMPLETE   = 6
    SENSITIVITY_COMPLETE   = 7
    COMPLETE               = 8


class CandidateStage(Enum):
    """The pipeline stage that produced or last evaluated a candidate."""
    RANDOM              = "RANDOM"
    MC_PREFILTER_PASS   = "MC_PREFILTER_PASS"
    MC_PREFILTER_FAIL   = "MC_PREFILTER_FAIL"
    GA                  = "GA"
    WFO                 = "WFO"
    MC_DEEP             = "MC_DEEP"
    SENSITIVITY         = "SENSITIVITY"


class RejectionReason(Enum):
    REJECTED_INSUFFICIENT_TRADES = "REJECTED_INSUFFICIENT_TRADES"
    REJECTED_CONSTRAINTS         = "REJECTED_CONSTRAINTS"
    EVALUATION_ERROR             = "EVALUATION_ERROR"
    MC_PREFILTER_FAIL            = "MC_PREFILTER_FAIL"
    MC_DEEP_FAIL                 = "MC_DEEP_FAIL"
    WFO_INSUFFICIENT_WINDOWS     = "WFO_INSUFFICIENT_WINDOWS"


class Verdict(Enum):
    AUTO_GO    = "auto_go"
    BORDERLINE = "borderline"
    NO_GO      = "no_go"


class DeploymentStatus(Enum):
    PAPER_TRADE_REQUIRED = "PAPER_TRADE_REQUIRED"
    LIVE_APPROVED        = "LIVE_APPROVED"


class MCMode(Enum):
    PRE_FILTER = "pre_filter"
    DEEP       = "deep"


# ────────────────────────────────────────────────────────────
# RunMetadata
# ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class RunMetadata:
    """
    Written once at run initialisation. Immutable for the lifetime of the run.
    Any field change requires a new run (new run_id, new record).
    """
    run_id: str                        # UUID4 string
    config_hash: str                   # SHA-256 hex digest of backtest_template.yaml content
    scenario_name: str                 # Active scenario name (e.g. "capital_accumulation")
    started_at: datetime               # UTC timestamp of run initialisation
    perturbation_profile_name: str     # MC perturbation profile name from YAML
    random_search_seed: int            # RNG seed for Stage 1 sampling
    ga_seed: int                       # RNG seed for GA operations
    mc_prefilter_seed: int             # RNG seed for Stage 2 MC
    mc_deep_seed: int                  # RNG seed for Stage 5 MC
    sensitivity_seed: int              # RNG seed for Stage 6 (if any randomness used)
    wfo_window_ids: Tuple[str, ...]    # Ordered tuple of all configured WFO window IDs
    checkpoint: Checkpoint             # Current pipeline checkpoint (mutable via store, not here)
    backtester_version: str            # Semantic version string of the backtester package

    def __post_init__(self):
        if not self.run_id:
            raise ValueError("run_id must not be empty")
        if len(self.config_hash) != 64:
            raise ValueError(f"config_hash must be a 64-character SHA-256 hex digest, got {len(self.config_hash)}")
        if not self.scenario_name:
            raise ValueError("scenario_name must not be empty")
        if len(self.wfo_window_ids) < 3:
            raise ValueError(f"Minimum 3 WFO windows required for GA random sampling; got {len(self.wfo_window_ids)}")


# ────────────────────────────────────────────────────────────
# ScenarioProfile
# ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ScenarioProfile:
    """
    The active scenario's evaluation lens. Built once at run start.
    Passed to FitnessEvaluator, Ranker, VerdictEngine, and ReportGenerator.
    """
    name: str
    description: str                          # Plain-language objective statement

    # Fitness weights — must sum to 1.0 (validated in __post_init__)
    weight_net_pnl: float
    weight_expectancy: float
    weight_max_drawdown: float                # Penalising weight — higher drawdown = lower fitness
    weight_win_rate: float
    weight_trade_frequency: float
    weight_profit_factor: float

    # Hard constraint thresholds (scenario-specific)
    min_win_rate: float                       # e.g. 0.45
    max_drawdown: float                       # e.g. 0.20 (20% max drawdown)
    max_losing_streak: int                    # e.g. 8
    min_trades_per_week: float                # e.g. 2.0
    min_expectancy: float                     # e.g. 0.5 (R-multiple)
    min_profit_factor: float                  # e.g. 1.3

    # MC pre-filter threshold
    mc_prefilter_ruin_threshold: float        # e.g. 0.30 — candidates above this are MC_PREFILTER_FAIL

    # WFO temporal consistency weights (must sum to 1.0)
    wfo_weight_median_return: float
    wfo_weight_variance: float                # Inverted — higher variance = lower consistency
    wfo_weight_worst_drawdown: float          # Inverted
    wfo_weight_fraction_positive: float

    # Verdict thresholds
    verdict_go_wfo_floor: float               # e.g. 0.65 — consistency score >= this → WFO pillar passes (INCLUSIVE)
    verdict_borderline_wfo_floor: float       # e.g. 0.40 — below this (strictly <) → no_go on WFO pillar
    verdict_go_mc_ruin_ceiling: float         # e.g. 0.05 — ruin prob <= this → MC pillar passes (INCLUSIVE)
    verdict_borderline_mc_ruin_ceiling: float # e.g. 0.15 — above this (strictly >) → no_go on MC pillar
    verdict_sensitivity_spike_threshold: float # e.g. 0.15 — fitness delta above this = borderline flag

    # Report emphasis — ordered list of metric names to lead the HTML report with
    report_emphasis: Tuple[str, ...]

    def __post_init__(self):
        fitness_weights = (
            self.weight_net_pnl + self.weight_expectancy + self.weight_max_drawdown
            + self.weight_win_rate + self.weight_trade_frequency + self.weight_profit_factor
        )
        if not (0.9999 <= fitness_weights <= 1.0001):
            raise ValueError(f"Fitness weights must sum to 1.0; got {fitness_weights:.6f}")

        wfo_weights = (
            self.wfo_weight_median_return + self.wfo_weight_variance
            + self.wfo_weight_worst_drawdown + self.wfo_weight_fraction_positive
        )
        if not (0.9999 <= wfo_weights <= 1.0001):
            raise ValueError(f"WFO temporal weights must sum to 1.0; got {wfo_weights:.6f}")

        if not (0.0 <= self.mc_prefilter_ruin_threshold <= 1.0):
            raise ValueError(f"mc_prefilter_ruin_threshold must be in [0, 1]")
        if not (0.0 <= self.min_win_rate <= 1.0):
            raise ValueError(f"min_win_rate must be in [0, 1]")
        if not (0.0 <= self.max_drawdown <= 1.0):
            raise ValueError(f"max_drawdown must be in [0, 1]")
        if self.verdict_borderline_wfo_floor >= self.verdict_go_wfo_floor:
            raise ValueError("verdict_borderline_wfo_floor must be strictly less than verdict_go_wfo_floor")
        if self.verdict_go_mc_ruin_ceiling >= self.verdict_borderline_mc_ruin_ceiling:
            raise ValueError("verdict_go_mc_ruin_ceiling must be strictly less than verdict_borderline_mc_ruin_ceiling")


# ────────────────────────────────────────────────────────────
# CandidateParameterSet
# ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class CandidateParameterSet:
    """
    The parameter configuration for a single candidate evaluation.
    candidate_id is derived at construction — it is the SHA-256 hash of the
    canonical string representation of parameters. Identical parameter sets
    always produce the same candidate_id.
    """
    zone_name: str                            # e.g. "safe", "exploration", "discovery"
    parameters: Dict[str, object]             # Param name → value. Frozen via __post_init__.
    candidate_id: str                         # Computed from parameters in __post_init__
    generation: Optional[int] = None          # GA generation number, None for Random Search

    def __post_init__(self):
        if not self.zone_name:
            raise ValueError("zone_name must not be empty")
        if not self.parameters:
            raise ValueError("parameters must not be empty")
        # Validate candidate_id is consistent with parameters
        import hashlib, json
        expected_id = hashlib.sha256(
            json.dumps(self.parameters, sort_keys=True, default=str).encode()
        ).hexdigest()
        if self.candidate_id != expected_id:
            raise ValueError(
                f"candidate_id '{self.candidate_id}' does not match computed hash '{expected_id}'. "
                "Use CandidateParameterSet.create() factory method to construct instances."
            )

    @staticmethod
    def create(zone_name: str, parameters: Dict[str, object], generation: Optional[int] = None) -> "CandidateParameterSet":
        """Factory method — always use this to construct instances."""
        import hashlib, json
        candidate_id = hashlib.sha256(
            json.dumps(parameters, sort_keys=True, default=str).encode()
        ).hexdigest()
        return CandidateParameterSet(
            zone_name=zone_name,
            parameters=parameters,
            candidate_id=candidate_id,
            generation=generation,
        )


# ────────────────────────────────────────────────────────────
# CandidateResult
# ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class CandidateResult:
    """
    The output of a single strategy evaluation. Returned by strategy_runner.py.
    If evaluation failed for any reason, metrics and trades are None and
    error contains the failure description. The runner never raises — all
    failures surface as CandidateResult with error set.
    """
    candidate_id: str
    evaluated_at: datetime
    metrics: Optional[object]           # MetricsReport from strategy architecture, or None
    trades: Optional[object]            # TradeResult from strategy architecture, or None
    total_trades: Optional[int]         # Extracted for quick access; None if evaluation failed
    error: Optional[str] = None         # RejectionReason.value or exception message

    @property
    def is_valid(self) -> bool:
        return self.metrics is not None and self.trades is not None and self.error is None

    def __post_init__(self):
        if not self.candidate_id:
            raise ValueError("candidate_id must not be empty")
        if self.is_valid and self.total_trades is None:
            raise ValueError("total_trades must be set when metrics and trades are present")


# ────────────────────────────────────────────────────────────
# FitnessResult
# ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class FitnessResult:
    """
    Output of fitness.py. Contains the composite fitness score and the
    pass/fail result of every constraint, with actual metric values.
    """
    candidate_id: str
    scenario_name: str
    fitness_score: Optional[float]           # None if any constraint failed
    passed_constraints: bool
    rejection_reason: Optional[str]          # Set if passed_constraints is False
    failing_constraint: Optional[str]        # The specific constraint that failed first
    failing_value: Optional[float]           # The actual metric value that failed

    # Constraint actuals (always populated, even for rejected candidates)
    actual_win_rate: Optional[float]
    actual_max_drawdown: Optional[float]
    actual_losing_streak: Optional[int]
    actual_trades_per_week: Optional[float]
    actual_expectancy: Optional[float]
    actual_profit_factor: Optional[float]

    def __post_init__(self):
        if not self.candidate_id:
            raise ValueError("candidate_id must not be empty")
        if self.passed_constraints and self.fitness_score is None:
            raise ValueError("fitness_score must be set when constraints are passed")
        if not self.passed_constraints and self.rejection_reason is None:
            raise ValueError("rejection_reason must be set when constraints failed")
        if self.fitness_score is not None and not (0.0 <= self.fitness_score <= 1.0):
            raise ValueError(f"fitness_score must be in [0, 1]; got {self.fitness_score}")


# ────────────────────────────────────────────────────────────
# WFOWindow
# ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class WFOWindow:
    """
    A single evaluation window for WFO. No train/test split — just a date range
    representing one temporal period for consistency measurement.
    """
    window_id: str                    # Unique identifier, e.g. "W01", "W02"
    start_date: date
    end_date: date

    def __post_init__(self):
        if not self.window_id:
            raise ValueError("window_id must not be empty")
        if self.start_date >= self.end_date:
            raise ValueError(f"start_date ({self.start_date}) must be before end_date ({self.end_date})")


# ────────────────────────────────────────────────────────────
# WFOWindowResult
# ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class WFOWindowResult:
    """
    Result of evaluating one candidate on one WFO window.
    """
    candidate_id: str
    window_id: str
    evaluated_at: datetime
    fitness_score: Optional[float]     # None if evaluation failed
    total_trades: Optional[int]
    net_pnl: Optional[float]
    max_drawdown: Optional[float]
    win_rate: Optional[float]
    expectancy: Optional[float]
    profit_factor: Optional[float]
    oos_delta: Optional[float]         # IS/OOS performance delta (informational)
    error: Optional[str] = None        # Set if evaluation failed

    @property
    def is_valid(self) -> bool:
        return self.fitness_score is not None and self.error is None

    def __post_init__(self):
        if not self.candidate_id:
            raise ValueError("candidate_id must not be empty")
        if not self.window_id:
            raise ValueError("window_id must not be empty")


# ────────────────────────────────────────────────────────────
# WFOConsistencyScore
# ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class WFOConsistencyScore:
    """
    The composite WFO consistency score for one candidate across all windows.
    Produced by consistency_scorer.py. All four sub-metrics are preserved
    individually for report transparency and SQL queryability.
    """
    candidate_id: str
    windows_evaluated: int             # How many windows contributed (failed windows excluded)
    windows_total: int                 # Total configured windows
    median_window_return: float        # Median per-window net P&L or fitness across windows
    window_return_variance: float      # Variance of per-window returns (lower = more consistent)
    worst_window_drawdown: float       # Maximum drawdown seen in the worst-performing window
    fraction_positive_windows: float   # Fraction of windows with positive return [0, 1]
    composite_score: float             # Scenario-weighted combination of the four metrics [0, 1]
    oos_gate_triggered: bool           # True if enforce_oos_gate and IS/OOS degradation > 50%
    window_collapse_flag: bool         # True if any window performance collapsed severely

    def __post_init__(self):
        if not self.candidate_id:
            raise ValueError("candidate_id must not be empty")
        if self.windows_evaluated < 0 or self.windows_evaluated > self.windows_total:
            raise ValueError(f"windows_evaluated ({self.windows_evaluated}) must be in [0, windows_total ({self.windows_total})]")
        if not (0.0 <= self.fraction_positive_windows <= 1.0):
            raise ValueError(f"fraction_positive_windows must be in [0, 1]; got {self.fraction_positive_windows}")
        if not (0.0 <= self.composite_score <= 1.0):
            raise ValueError(f"composite_score must be in [0, 1]; got {self.composite_score}")


# ────────────────────────────────────────────────────────────
# MCResult
# ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class MCResult:
    """
    Monte Carlo simulation summary for one candidate in one mode.

    run_mc() never raises. Failures are returned as MCResult(error="...",
    ruin_probability=None). verdict.py maps ruin_probability=None →
    mc_pillar_no_go=True → NO_GO. The orchestrator logs a WARNING and
    continues — the result is still written to the store.
    """
    candidate_id: str
    mode: MCMode
    perturbation_profile_name: str
    iterations: int
    evaluated_at: datetime
    avg_final_equity: Optional[float]         # Mean equity at end of all paths
    worst_drawdown_across_paths: Optional[float]  # Max drawdown seen across the full ensemble
    ruin_probability: Optional[float]         # Fraction of paths hitting the ruin threshold [0, 1]
    p5_final_equity: Optional[float]          # 5th percentile final equity (downside tail)
    error: Optional[str] = None

    @property
    def is_valid(self) -> bool:
        return self.ruin_probability is not None and self.error is None

    def __post_init__(self):
        if not self.candidate_id:
            raise ValueError("candidate_id must not be empty")
        if self.iterations <= 0:
            raise ValueError(f"iterations must be positive; got {self.iterations}")
        if self.ruin_probability is not None and not (0.0 <= self.ruin_probability <= 1.0):
            raise ValueError(f"ruin_probability must be in [0, 1]; got {self.ruin_probability}")


# ────────────────────────────────────────────────────────────
# SensitivityProfile
# ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ParameterSensitivity:
    """Sensitivity data for a single parameter at a single step."""
    parameter_name: str
    step: int                        # e.g. -2, -1, +1, +2
    perturbed_value: object          # The actual parameter value used
    fitness_delta: Optional[float]   # perturbed_fitness - baseline_fitness; None if eval failed
    evaluation_error: Optional[str] = None


@dataclass(frozen=True)
class SensitivityProfile:
    """
    Full sensitivity map for one candidate across all parameters and steps.

    profile_complete=False when >50% of perturbation evaluations failed for
    this candidate. This sets the sensitivity_profile_incomplete modifier flag
    in verdict.py → demotes AUTO_GO to BORDERLINE. The pipeline never aborts —
    the profile is always written to the store even when incomplete.
    """
    candidate_id: str
    baseline_fitness: float
    parameter_sensitivities: Tuple[ParameterSensitivity, ...]
    spike_detected: bool             # True if any |fitness_delta| > configured spike_threshold
    spike_parameters: Tuple[str, ...] # Names of parameters that triggered the spike
    profile_complete: bool           # False if >50% of perturbations failed to evaluate

    def __post_init__(self):
        if not self.candidate_id:
            raise ValueError("candidate_id must not be empty")
        if not (0.0 <= self.baseline_fitness <= 1.0):
            raise ValueError(f"baseline_fitness must be in [0, 1]; got {self.baseline_fitness}")
        if self.spike_detected and not self.spike_parameters:
            raise ValueError("spike_parameters must be non-empty when spike_detected is True")


# ────────────────────────────────────────────────────────────
# VerdictResult
# ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class VerdictResult:
    """
    Final pipeline verdict for one candidate. Contains both the verdict
    and the complete evidence that supports it.
    """
    candidate_id: str
    scenario_name: str
    verdict: Verdict
    deployment_status: DeploymentStatus     # Always PAPER_TRADE_REQUIRED for go/borderline

    # Pillar scores (None if the stage was not reached)
    wfo_consistency_score: Optional[float]
    mc_deep_ruin_probability: Optional[float]

    # Modifier flags
    sensitivity_spike: bool
    oos_gate_triggered: bool                # True only if enforce_oos_gate is on AND degraded
    window_collapse_flag: bool
    sensitivity_profile_incomplete: bool

    # Informational evidence (stored but not verdict gates)
    median_oos_delta: Optional[float]       # Median IS/OOS delta across all WFO windows
    parameter_region_width: Optional[float] # Width of robust parameter region (informational)
    yaml_output_path: Optional[str]         # Path to trading-ready YAML, if generated

    # Human-readable evidence summary (plain language, included in report)
    evidence_summary: str

    def __post_init__(self):
        if not self.candidate_id:
            raise ValueError("candidate_id must not be empty")
        if self.verdict in (Verdict.AUTO_GO, Verdict.BORDERLINE):
            if self.deployment_status != DeploymentStatus.PAPER_TRADE_REQUIRED:
                raise ValueError(
                    f"deployment_status must be PAPER_TRADE_REQUIRED for {self.verdict.value} verdicts; "
                    f"got {self.deployment_status.value}. Operator must manually promote to LIVE_APPROVED."
                )
        if not self.evidence_summary:
            raise ValueError("evidence_summary must not be empty")


# ────────────────────────────────────────────────────────────
# CandidateRecord  (SQLite row representation)
# ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class CandidateRecord:
    """
    The flattened SQLite row for a candidate at a specific pipeline stage.
    One record per candidate per stage. All fields are primitive types
    suitable for direct SQLite column storage (no nested objects).
    """
    run_id: str
    candidate_id: str
    zone_name: str
    stage: str                        # CandidateStage.value
    generation: Optional[int]
    recorded_at: datetime

    # Parameters — all primitive (str, int, float, bool)
    # Stored as individual columns in SQLite (one per parameter).
    # The CandidateStore expands the parameters dict into individual columns dynamically
    # based on the parameter space definition. Not represented as fixed fields here
    # because parameter names are not hardcoded outside strategy_runner.py.
    parameters_json: str              # Full JSON backup for audit; individual columns are primary

    # Fitness
    fitness_score: Optional[float]
    passed_constraints: Optional[bool]
    rejection_reason: Optional[str]
    failing_constraint: Optional[str]
    failing_value: Optional[float]

    # Constraint actuals
    actual_win_rate: Optional[float]
    actual_max_drawdown: Optional[float]
    actual_losing_streak: Optional[int]
    actual_trades_per_week: Optional[float]
    actual_expectancy: Optional[float]
    actual_profit_factor: Optional[float]

    # WFO consistency (populated at WFO stage)
    wfo_median_window_return: Optional[float]
    wfo_window_return_variance: Optional[float]
    wfo_worst_window_drawdown: Optional[float]
    wfo_fraction_positive_windows: Optional[float]
    wfo_consistency_score: Optional[float]
    wfo_windows_evaluated: Optional[int]
    wfo_oos_gate_triggered: Optional[bool]
    wfo_window_collapse_flag: Optional[bool]

    # MC pre-filter (populated at MC_PREFILTER stage)
    mc_prefilter_ruin_probability: Optional[float]
    mc_prefilter_avg_final_equity: Optional[float]
    mc_prefilter_iterations: Optional[int]

    # MC deep (populated at MC_DEEP stage)
    mc_deep_ruin_probability: Optional[float]
    mc_deep_avg_final_equity: Optional[float]
    mc_deep_worst_drawdown: Optional[float]
    mc_deep_p5_final_equity: Optional[float]
    mc_deep_iterations: Optional[int]

    # Sensitivity (populated at SENSITIVITY stage)
    sensitivity_spike_detected: Optional[bool]
    sensitivity_spike_parameters: Optional[str]   # Comma-separated parameter names
    sensitivity_profile_complete: Optional[bool]

    # Verdict (populated at COMPLETE stage)
    verdict: Optional[str]
    deployment_status: Optional[str]
    evidence_summary: Optional[str]

    def __post_init__(self):
        if not self.run_id:
            raise ValueError("run_id must not be empty")
        if not self.candidate_id:
            raise ValueError("candidate_id must not be empty")
```

---

## 3. Enumerations

All enumerations are defined in `contracts.py` and reproduced above. Key rules:
- Use `.value` (string) when writing to SQLite columns — never store enum objects as blobs
- Use the enum class when passing values between Python modules — never raw strings
- Add new enum values only when a new pipeline stage or status is formally added

---

## 4. Module Interface Signatures

The following function signatures define the public interface of each module. Internal helpers are not specified here — they are implementation details.

```python
# ── orchestrator.py ──────────────────────────────────────────
def run(config_path: Path) -> None:
    """Entry point. Loads config, resumes or starts fresh, runs all stages."""

def _resume_or_start(store: CandidateStore, config_path: Path) -> RunMetadata:
    """Checks for existing run, validates config hash, returns RunMetadata."""

# ── parameter_space.py ───────────────────────────────────────
def expand_zones(config: dict) -> Dict[str, List[Dict[str, object]]]:
    """Reads zone definitions from config, returns expanded parameter sets per zone."""

def validate_combination(params: Dict[str, object], zone_def: dict) -> bool:
    """Returns True if the parameter combination is within zone boundaries."""

# ── sampler.py ───────────────────────────────────────────────
def sample_lhs(expanded_space: Dict[str, List], n_per_zone: int, seed: int) -> List[CandidateParameterSet]:
    """Latin Hypercube Sampling across all zones. Returns CandidateParameterSets."""

def sample_random(expanded_space: Dict[str, List], n_per_zone: int, seed: int) -> List[CandidateParameterSet]:
    """Uniform random sampling. Used when method: random in YAML."""

# ── scenario.py ──────────────────────────────────────────────
def load_scenario(config: dict) -> ScenarioProfile:
    """Reads active scenario name from config, builds and validates ScenarioProfile."""

# ── strategy_runner.py ───────────────────────────────────────
def evaluate(candidate: CandidateParameterSet, base_yaml_path: Path, temp_dir: Path) -> CandidateResult:
    """
    Builds temp YAML, runs strategy in core mode, applies significance guard.
    NEVER raises — all failures returned as CandidateResult with error set.
    Calls CacheManager.clear_all_caches() in finally block.
    """

# ── fitness.py ───────────────────────────────────────────────
def evaluate_fitness(result: CandidateResult, scenario: ScenarioProfile) -> FitnessResult:
    """Stateless. Applies constraints then computes weighted score."""

# ── candidate_store.py ───────────────────────────────────────
def initialise(db_path: Path, run_metadata: RunMetadata) -> CandidateStore:
    """Creates SQLite DB, writes run metadata row, enables WAL mode."""

def write_candidate(record: CandidateRecord) -> None:
    """Thread-safe write via internal queue. Non-blocking for callers."""

def get_checkpoint(run_id: str) -> Checkpoint:
def set_checkpoint(run_id: str, checkpoint: Checkpoint) -> None:
    """
    set_checkpoint() is called ONLY by orchestrator.py.
    All other modules read checkpoints — none write them.
    """

def query_candidates(
    run_id: str,
    stage: Optional[CandidateStage] = None,
    min_fitness: Optional[float] = None,
    verdict: Optional[Verdict] = None,
    limit: Optional[int] = None,
    order_by: str = "fitness_score DESC"
) -> List[CandidateRecord]:

# ── ranker.py ────────────────────────────────────────────────
def rank(store: CandidateStore, run_id: str, stage: CandidateStage, top_n: int) -> List[CandidateRecord]:
    """Returns top N records from the given stage, ranked by fitness_score DESC."""

def rank_by_wfo(store: CandidateStore, run_id: str, top_n: int) -> List[CandidateRecord]:
    """Ranks by wfo_consistency_score DESC. Used for MC Deep and Sensitivity input."""

# ── ga/ga_engine.py ──────────────────────────────────────────
def run_ga(
    store: CandidateStore,
    run_id: str,
    scenario: ScenarioProfile,
    wfo_windows: List[WFOWindow],
    config: dict,
    seed: int,
) -> None:
    """Runs full GA evolution loop. Writes all candidates to store. Returns nothing."""

# ── ga/diversity.py ──────────────────────────────────────────
def compute_penalty(
    candidate: CandidateParameterSet,
    elites: List[CandidateParameterSet],
    parameter_space_def: dict,
    distance_threshold: float,
    penalty_weight: float,
) -> float:
    """Returns a penalty scalar in [0, penalty_weight]. 0 if candidate is diverse enough."""

# ── wfo/wfo_engine.py ────────────────────────────────────────
def run_wfo(
    candidates: List[CandidateParameterSet],
    windows: List[WFOWindow],
    store: CandidateStore,
    run_id: str,
    scenario: ScenarioProfile,
    base_yaml_path: Path,
    temp_dir: Path,
    mode: str,  # "full" | "lightweight"
) -> Dict[str, WFOConsistencyScore]:
    """Returns map of candidate_id → WFOConsistencyScore."""

# ── wfo/wfo_evaluator.py ─────────────────────────────────────
def evaluate_window(
    candidate: CandidateParameterSet,
    window: WFOWindow,
    base_yaml_path: Path,
    temp_dir: Path,
    scenario: ScenarioProfile,
) -> WFOWindowResult:
    """Evaluates one candidate on one window. Never raises."""

# ── wfo/consistency_scorer.py ────────────────────────────────
def compute_consistency(
    window_results: List[WFOWindowResult],
    windows_total: int,
    scenario: ScenarioProfile,
    oos_gate_enabled: bool,
    oos_degradation_threshold: float,
) -> WFOConsistencyScore:
    """Aggregates window results into four metrics and composite score."""

# ── monte_carlo/mc_engine.py ─────────────────────────────────
def run_mc(
    candidate: CandidateParameterSet,
    candidate_result: CandidateResult,
    mode: MCMode,
    config: dict,
    seed: int,
) -> MCResult:
    """
    Runs MC simulation in the specified mode. Never raises — errors returned
    in MCResult(error="...", ruin_probability=None).
    verdict.py maps ruin_probability=None → mc_pillar_no_go=True → NO_GO.
    """

# ── evaluation/sensitivity.py ────────────────────────────────
def evaluate_sensitivity(
    candidate: CandidateParameterSet,
    baseline_fitness: float,
    parameter_space_def: dict,
    base_yaml_path: Path,
    temp_dir: Path,
    scenario: ScenarioProfile,
    spike_threshold: float,
    max_steps: int = 2,
) -> SensitivityProfile:
    """
    Perturbs each parameter ±1..max_steps steps, computes fitness deltas.
    Never raises — failures surface as SensitivityProfile with
    profile_complete=False when >50% of perturbations fail.
    Uses ProcessPoolExecutor (Windows spawn mode).
    PATCH NOTE: patch at orchestrator level for integration tests —
    do not patch _evaluate_perturbation (worker function, spawn boundary).
    See ARCHITECTURE.md §9 and D-07 Windows spawn note above.
    """

# ── evaluation/verdict.py ────────────────────────────────────
def compute_verdict(
    candidate_id: str,
    wfo_score: WFOConsistencyScore,
    mc_result: MCResult,
    sensitivity: SensitivityProfile,
    scenario: ScenarioProfile,
    oos_gate_enabled: bool,
) -> VerdictResult:
    """
    Applies two-pillar logic + modifiers. Returns VerdictResult. Never raises.
    Boundary operators (confirmed from source, Block 5):
      wfo_pillar_go    = composite >= go_wfo_floor        (>= INCLUSIVE)
      wfo_pillar_no_go = composite < borderline_wfo_floor  (< strictly less than)
      mc_pillar_go     = ruin_prob <= go_mc_ruin_ceiling   (<= INCLUSIVE)
      mc_pillar_no_go  = ruin_prob > borderline_mc_ruin_ceiling (> strictly greater)
      ruin_prob=None   → mc_pillar_no_go=True → NO_GO
    """

# ── report_generator.py ──────────────────────────────────────
def generate_report(
    store: CandidateStore,
    run_id: str,
    scenario: ScenarioProfile,
    output_dir: Path,
    formats: dict,  # {"html": True, "json": True, "parquet": True}
) -> None:
    """Reads all stage results from store, produces all configured output formats."""

# ── yaml_generator.py ────────────────────────────────────────
def generate_trading_yaml(
    candidate: CandidateParameterSet,
    verdict: VerdictResult,
    run_metadata: RunMetadata,
    base_strategy_yaml_path: Path,
    output_path: Path,
) -> Path:
    """
    Merges candidate parameters into base strategy YAML.
    Embeds metadata: scenario, run_id, config_hash, deployment_status.
    Validates output against StrategyConfig schema before writing.
    Returns the path to the written file.
    """
```

---

## 5. Configuration Schema Reference

Full schema of `backtest_template.yaml`. All keys listed. Types and defaults specified. Required keys have no default. For the current production values of all scenario thresholds and zone definitions, see `configs/backtesting/backtest_template.yaml` — it is the single source of truth.

```yaml
# backtest_template.yaml — full schema

# ── Top-level ─────────────────────────────────────────────────
backtester_version: "1.0.0"         # string, required — validated against package version
scenario: "capital_accumulation"    # string, required — must match a defined scenario name

# ── Run settings ──────────────────────────────────────────────
run:
  mode: "full_pipeline"             # "full_pipeline" | "random_only" | "ga_only" (for dev)
  output_dir: "outputs/backtesting" # string, required
  temp_dir: "temp/backtesting"      # string, required
  retain_temp_yamls: false          # bool, default false — set true for debugging
  max_workers: 6                    # int 1–16, default 6
  log_level: "INFO"                 # "DEBUG" | "INFO" | "WARNING"

# ── Stage enables (all true in full_pipeline mode) ────────────
stages:
  random_search: true
  mc_prefilter: true
  genetic_algorithm: true
  walk_forward: true
  monte_carlo_deep: true
  sensitivity: true
  report: true

# ── Random Search ─────────────────────────────────────────────
random_search:
  method: "lhs"                     # "lhs" | "random"
  samples_per_zone: 200             # int, required
  min_significant_trades: 30        # int — significance guard threshold
  seed: 42                          # int, required

# ── MC Pre-Filter ─────────────────────────────────────────────
mc_prefilter:
  input_count: 120                  # int — top N from Random Search
  iterations: 300                   # int — low; this is the cheap screen
  perturbation_profile: "default"   # string — must match a profile in perturbation_profiles
  seed: 43                          # int, required

# ── Genetic Algorithm ─────────────────────────────────────────
genetic:
  population_size: 60               # int
  generations: 30                   # int
  elite_fraction: 0.10              # float [0, 1]
  mutation_rate: 0.15               # float [0, 1]
  crossover_rate: 0.70              # float [0, 1]
  tournament_size: 5                # int
  stagnation_generations: 10        # int — early stop if no improvement for N generations
  diversity_penalty_weight: 0.10    # float [0, 1] — weight of diversity term in GA fitness
  diversity_distance_threshold: 0.15 # float — normalised distance below which penalty applies
  seed: 44                          # int, required

# ── Walk-Forward Optimisation ─────────────────────────────────
walk_forward:
  input_count: 30                   # int — top N from combined Random + GA pool
  enforce_oos_gate: false           # bool, default false (D-12)
  oos_degradation_threshold: 0.50   # float [0, 1] — only used if enforce_oos_gate: true

  windows:                          # list, minimum 3 entries required (Stage 0 enforced)
    - id: "W01"
      start: "YYYY-MM-DD"           # date string
      end: "YYYY-MM-DD"
    # ... minimum 3 windows

# ── Monte Carlo Deep ──────────────────────────────────────────
monte_carlo:
  deep:
    input_count: 10                 # int — top N from WFO by consistency score
    iterations: 3000                # int
    perturbation_profile: "default" # string
    ruin_threshold: 0.20            # float — equity fraction at which ruin is declared
    seed: 45                        # int, required

# ── Parameter Sensitivity ─────────────────────────────────────
sensitivity:
  input_count: 5                    # int — top N from MC Deep
  max_steps: 2                      # int — perturb ±1 and ±2 steps
  spike_threshold: 0.15             # float — |fitness_delta| above this = spike flag

# ── Output ────────────────────────────────────────────────────
output:
  shortlist_count: 5                # int — candidates in final ranked shortlist
  formats:
    html: true
    json: true
    parquet: true
  save_intermediates:
    equity_curves: false
    trade_logs: false
    candidate_yamls: false          # the generated temp YAMLs, not the trading YAML

# ── Perturbation Profiles ─────────────────────────────────────
perturbation_profiles:
  default:
    version: "1.0"
    spread_noise_bps_range: [0, 3]  # list of 2 floats — spread noise in basis points
    slippage_pips_range: [0, 1]     # list of 2 floats
    risk_noise_fraction: 0.05       # float — ±5% position size noise
    shuffle_trades: true
    resample_returns: true
    execution_delay_bars: 0         # int — 0 = no delay simulation

# ── Scenarios ─────────────────────────────────────────────────
# Built-in scenarios: capital_accumulation, swing_trading, conservative, e2e_test
# For current production values see backtest_template.yaml (single source of truth)
#
# Verdict threshold operators (confirmed from verdict.py source, Block 5):
#   wfo_pillar_go    = composite >= go_wfo_floor        (>= INCLUSIVE)
#   wfo_pillar_no_go = composite < borderline_wfo_floor  (< strictly less than)
#   mc_pillar_go     = ruin_prob <= go_mc_ruin_ceiling   (<= INCLUSIVE)
#   mc_pillar_no_go  = ruin_prob > borderline_mc_ruin_ceiling (> strictly greater than)
#   ruin_prob=None   → NO_GO
#
# capital_accumulation defaults (D-07 starting values — recalibrate after first run):
#   go_wfo_floor: 0.65
#   borderline_wfo_floor: 0.40
#   go_mc_ruin_ceiling: 0.05
#   borderline_mc_ruin_ceiling: 0.15
#   sensitivity_spike_threshold: 0.15

# ── Parameter Zones ───────────────────────────────────────────
# For current zone definitions see backtest_template.yaml.
# Structure for each zone:
# - name: zone identifier ("safe" | "exploration" | "discovery")
# - enabled: bool
# - parameters: dict of parameter_name → definition
#   - type: "int" | "float" | "choice"
#   - min, max, step (for int/float — both bounds inclusive, step-aligned grid)
#   - choices: list (for choice)
#
# Parameter NAMES must exactly match _PARAM_KEY_MAP in strategy_runner.py.
# Adding a new optimizable parameter requires: (1) add to zone in YAML,
# (2) add mapping in _PARAM_KEY_MAP. No other code changes required.
```

---

## Changelog

| Version | Date | Change |
|---|---|---|
| 1.0.0 | 2026-02-27 | Initial — Phase 1 design. All 12 decisions resolved. Full contracts, signatures, schema. |
| 1.1.0 | 2026-03-03 | Block 6: Added D-07 confirmed boundary operators (`>=`/`<=` inclusive at go thresholds) with rationale. Added Windows spawn mode test patch constraint (Section 1a). Added `run_mc` never-raises contract note to MCResult docstring. Added `SensitivityProfile.profile_complete=False` behaviour to SensitivityProfile docstring. Added `set_checkpoint` orchestrator-only note. Added boundary operator summary comments to `compute_verdict` and `evaluate_sensitivity` signatures. Updated Section 5 schema to cross-reference backtest_template.yaml for current values. |