"""
contracts.py — All inter-module contracts for the backtesting framework.

All contracts are frozen dataclasses. No mutable fields. Validation in __post_init__.
No raw dicts cross module boundaries. Use .value when writing enums to SQLite.

Source of truth: docs/backtesting/TECHNICAL_SPEC.md Section 2.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Dict, List, Optional, Tuple


# ────────────────────────────────────────────────────────────
# Enumerations
# ────────────────────────────────────────────────────────────

class Checkpoint(Enum):
    """Pipeline stage checkpoint states, in execution order."""
    NOT_STARTED            = 0
    RUN_INITIALISED        = 1
    RANDOM_SEARCH_COMPLETE = 2
    MC_PREFILTER_COMPLETE  = 3
    GA_COMPLETE            = 4
    WFO_COMPLETE           = 5
    MONTE_CARLO_COMPLETE   = 6
    SENSITIVITY_COMPLETE   = 7
    COMPLETE               = 8


class CandidateStage(Enum):
    """The pipeline stage that produced or last evaluated a candidate."""
    RANDOM            = "RANDOM"
    MC_PREFILTER_PASS = "MC_PREFILTER_PASS"
    MC_PREFILTER_FAIL = "MC_PREFILTER_FAIL"
    GA                = "GA"
    WFO               = "WFO"
    MC_DEEP           = "MC_DEEP"
    SENSITIVITY       = "SENSITIVITY"


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
    run_id: str
    config_hash: str                    # SHA-256 hex digest of backtest_template.yaml content
    scenario_name: str
    started_at: datetime                # UTC
    perturbation_profile_name: str
    random_search_seed: int
    ga_seed: int
    mc_prefilter_seed: int
    mc_deep_seed: int
    sensitivity_seed: int
    wfo_window_ids: Tuple[str, ...]     # Ordered tuple; minimum 3
    checkpoint: Checkpoint
    backtester_version: str

    def __post_init__(self):
        if not self.run_id:
            raise ValueError("run_id must not be empty")
        if len(self.config_hash) != 64:
            raise ValueError(
                f"config_hash must be a 64-character SHA-256 hex digest; got {len(self.config_hash)}"
            )
        if not self.scenario_name:
            raise ValueError("scenario_name must not be empty")
        if len(self.wfo_window_ids) < 3:
            raise ValueError(
                f"Minimum 3 WFO windows required for GA random sampling; got {len(self.wfo_window_ids)}"
            )


# ────────────────────────────────────────────────────────────
# ScenarioProfile
# ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ScenarioProfile:
    """
    The active scenario's evaluation lens. Built once at run start.
    Passed to FitnessEvaluator, Ranker, VerdictEngine, and ReportGenerator.

    Field additions (Block 7B audit remediation):
      M-02: normalisation_drawdown_ref_points, normalisation_pnl_ref_points,
            normalisation_freq_ref_trades_per_week — fitness normalisation constants
            previously hardcoded in fitness.py. Defaults reproduce prior behaviour.
      M-03: wfo_collapse_drawdown_threshold — WFO window collapse flag threshold
            previously hardcoded as 0.40 in consistency_scorer.py.
            Default 0.40 reproduces prior behaviour.
    All new fields have defaults and are appended at the end so existing
    YAML loaders and test fixtures require no changes.

    Block 8C change (B8C-001): report_emphasis validated as non-empty sequence
    in __post_init__. A scalar string (e.g. "balanced") would be accepted by the
    type hint but silently cause _render_scenario_metrics to iterate over individual
    characters. The validation produces a clear error message pointing to the
    correct YAML format.
    """
    name: str
    description: str

    # Fitness weights — must sum to 1.0
    weight_net_pnl: float
    weight_expectancy: float
    weight_max_drawdown: float          # Penalising weight
    weight_win_rate: float
    weight_trade_frequency: float
    weight_profit_factor: float

    # Hard constraint thresholds
    min_win_rate: float
    max_drawdown: float
    max_losing_streak: int
    min_trades_per_week: float
    min_expectancy: float
    min_profit_factor: float

    mc_prefilter_ruin_threshold: float

    # WFO temporal consistency weights — must sum to 1.0
    wfo_weight_median_return: float
    wfo_weight_variance: float          # Inverted
    wfo_weight_worst_drawdown: float    # Inverted
    wfo_weight_fraction_positive: float

    # Verdict thresholds
    verdict_go_wfo_floor: float
    verdict_borderline_wfo_floor: float
    verdict_go_mc_ruin_ceiling: float
    verdict_borderline_mc_ruin_ceiling: float
    verdict_sensitivity_spike_threshold: float

    # Report metric emphasis — must be a non-empty tuple of metric name strings.
    # Controls the order and selection of per-candidate metrics in the HTML report.
    # Example YAML: report_emphasis: [wfo_consistency_score, mc_deep_ruin_probability]
    report_emphasis: Tuple[str, ...]

    # ── M-03: WFO collapse threshold (was hardcoded 0.40 in consistency_scorer.py) ──
    # Any valid window with max_drawdown >= this value triggers window_collapse_flag.
    # conservative scenario should use a lower value (e.g. 0.20) to flag earlier.
    wfo_collapse_drawdown_threshold: float = 0.40

    # ── M-02: Fitness normalisation reference constants (were hardcoded in fitness.py) ──
    # normalisation_drawdown_ref_points: "100% drawdown" reference equity in points.
    #   MetricsReport.max_drawdown (negative points) / this value → fraction in [0,1].
    #   Recalibrate after first real run. Default 10_000 is conservatively large.
    normalisation_drawdown_ref_points: float = 10_000.0
    # normalisation_pnl_ref_points: net P&L value considered "excellent" for fitness scoring.
    #   net_pnl / this value → normalised contribution. Default 5_000 pts.
    normalisation_pnl_ref_points: float = 5_000.0
    # normalisation_freq_ref_trades_per_week: trade frequency ceiling for fitness scoring.
    #   trades_per_week / this value → normalised contribution. Default 20 trades/week.
    normalisation_freq_ref_trades_per_week: float = 20.0

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
            raise ValueError("mc_prefilter_ruin_threshold must be in [0, 1]")
        if not (0.0 <= self.min_win_rate <= 1.0):
            raise ValueError("min_win_rate must be in [0, 1]")
        if not (0.0 <= self.max_drawdown <= 1.0):
            raise ValueError("max_drawdown must be in [0, 1]")
        if self.verdict_borderline_wfo_floor >= self.verdict_go_wfo_floor:
            raise ValueError(
                "verdict_borderline_wfo_floor must be strictly less than verdict_go_wfo_floor"
            )
        if self.verdict_go_mc_ruin_ceiling >= self.verdict_borderline_mc_ruin_ceiling:
            raise ValueError(
                "verdict_go_mc_ruin_ceiling must be strictly less than verdict_borderline_mc_ruin_ceiling"
            )
        if not (0.0 < self.wfo_collapse_drawdown_threshold <= 1.0):
            raise ValueError(
                f"wfo_collapse_drawdown_threshold must be in (0, 1]; "
                f"got {self.wfo_collapse_drawdown_threshold}"
            )
        if self.normalisation_drawdown_ref_points <= 0.0:
            raise ValueError(
                f"normalisation_drawdown_ref_points must be positive; "
                f"got {self.normalisation_drawdown_ref_points}"
            )
        if self.normalisation_pnl_ref_points <= 0.0:
            raise ValueError(
                f"normalisation_pnl_ref_points must be positive; "
                f"got {self.normalisation_pnl_ref_points}"
            )
        if self.normalisation_freq_ref_trades_per_week <= 0.0:
            raise ValueError(
                f"normalisation_freq_ref_trades_per_week must be positive; "
                f"got {self.normalisation_freq_ref_trades_per_week}"
            )
        # B8C-001: report_emphasis must be a non-empty sequence of metric name strings.
        # A scalar string (e.g. "balanced") is accepted by the type hint but causes
        # _render_scenario_metrics in report_generator.py to iterate over individual
        # characters instead of metric names, producing garbage HTML cells silently.
        # Validation here produces a clear error at construction time (fail fast — P6).
        if not isinstance(self.report_emphasis, (list, tuple)) or len(self.report_emphasis) == 0:
            raise ValueError(
                "report_emphasis must be a non-empty list or tuple of metric name strings; "
                f"got {type(self.report_emphasis).__name__}: {self.report_emphasis!r}. "
                "Example YAML: report_emphasis: [wfo_consistency_score, mc_deep_ruin_probability]"
            )


# ────────────────────────────────────────────────────────────
# CandidateParameterSet
# ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class CandidateParameterSet:
    """
    Parameter configuration for a single candidate. candidate_id is the SHA-256
    hash of the canonical JSON representation of parameters. Always use .create().
    """
    zone_name: str
    parameters: Dict[str, object]
    candidate_id: str
    generation: Optional[int] = None    # None for Random Search; int for GA

    def __post_init__(self):
        if not self.zone_name:
            raise ValueError("zone_name must not be empty")
        if not self.parameters:
            raise ValueError("parameters must not be empty")
        expected_id = hashlib.sha256(
            json.dumps(self.parameters, sort_keys=True, default=str).encode()
        ).hexdigest()
        if self.candidate_id != expected_id:
            raise ValueError(
                f"candidate_id '{self.candidate_id}' does not match computed hash '{expected_id}'. "
                "Use CandidateParameterSet.create() factory method."
            )

    @staticmethod
    def create(
        zone_name: str,
        parameters: Dict[str, object],
        generation: Optional[int] = None,
    ) -> "CandidateParameterSet":
        """Factory — always use this to construct instances."""
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
    Output of strategy_runner.py. Never raises — all failures surface here.
    metrics and trades are None if evaluation failed.
    """
    candidate_id: str
    evaluated_at: datetime
    metrics: Optional[object]           # MetricsReport from strategy architecture
    trades: Optional[object]            # TradeResult from strategy architecture
    total_trades: Optional[int]
    error: Optional[str] = None

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
    """Output of fitness.py. Composite score + constraint pass/fail + actuals."""
    candidate_id: str
    scenario_name: str
    fitness_score: Optional[float]      # None if any constraint failed
    passed_constraints: bool
    rejection_reason: Optional[str]
    failing_constraint: Optional[str]
    failing_value: Optional[float]

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
    """A single temporal evaluation window for WFO."""
    window_id: str
    start_date: date
    end_date: date

    def __post_init__(self):
        if not self.window_id:
            raise ValueError("window_id must not be empty")
        if self.start_date >= self.end_date:
            raise ValueError(
                f"start_date ({self.start_date}) must be before end_date ({self.end_date})"
            )


# ────────────────────────────────────────────────────────────
# WFOWindowResult
# ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class WFOWindowResult:
    """Result of evaluating one candidate on one WFO window."""
    candidate_id: str
    window_id: str
    evaluated_at: datetime
    fitness_score: Optional[float]
    total_trades: Optional[int]
    net_pnl: Optional[float]
    max_drawdown: Optional[float]
    win_rate: Optional[float]
    expectancy: Optional[float]
    profit_factor: Optional[float]
    oos_delta: Optional[float]
    error: Optional[str] = None

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
    """Composite WFO consistency score across all windows for one candidate.

    Block 7D change (M-01): median_oos_delta field added.
    Computed in consistency_scorer.py from per-window oos_delta values.
    None when oos_gate is disabled or no windows have oos_delta populated.
    Default None so all existing constructors remain valid.
    """
    candidate_id: str
    windows_evaluated: int
    windows_total: int
    median_window_return: float
    window_return_variance: float
    worst_window_drawdown: float
    fraction_positive_windows: float
    composite_score: float
    oos_gate_triggered: bool
    window_collapse_flag: bool

    # M-01: Median IS/OOS delta across all valid windows.
    # Negative value = OOS underperforms IS. Populated by consistency_scorer.
    # None when no windows carry oos_delta (gate disabled or pre-OOS runs).
    median_oos_delta: Optional[float] = None

    def __post_init__(self):
        if not self.candidate_id:
            raise ValueError("candidate_id must not be empty")
        if self.windows_evaluated < 0 or self.windows_evaluated > self.windows_total:
            raise ValueError(
                f"windows_evaluated ({self.windows_evaluated}) must be in "
                f"[0, windows_total ({self.windows_total})]"
            )
        if not (0.0 <= self.fraction_positive_windows <= 1.0):
            raise ValueError(
                f"fraction_positive_windows must be in [0, 1]; got {self.fraction_positive_windows}"
            )
        if not (0.0 <= self.composite_score <= 1.0):
            raise ValueError(f"composite_score must be in [0, 1]; got {self.composite_score}")


# ────────────────────────────────────────────────────────────
# MCResult
# ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class MCResult:
    """Monte Carlo simulation summary for one candidate in one mode."""
    candidate_id: str
    mode: MCMode
    perturbation_profile_name: str
    iterations: int
    evaluated_at: datetime
    avg_final_equity: Optional[float]
    worst_drawdown_across_paths: Optional[float]
    ruin_probability: Optional[float]
    p5_final_equity: Optional[float]
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
# SensitivityProfile + sub-contract
# ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ParameterSensitivity:
    """Sensitivity data for a single parameter at a single step."""
    parameter_name: str
    step: int
    perturbed_value: object
    fitness_delta: Optional[float]
    evaluation_error: Optional[str] = None


@dataclass(frozen=True)
class SensitivityProfile:
    """Full sensitivity map for one candidate across all parameters and steps."""
    candidate_id: str
    baseline_fitness: float
    parameter_sensitivities: Tuple[ParameterSensitivity, ...]
    spike_detected: bool
    spike_parameters: Tuple[str, ...]
    profile_complete: bool

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
    """Final pipeline verdict for one candidate with full supporting evidence."""
    candidate_id: str
    scenario_name: str
    verdict: Verdict
    deployment_status: DeploymentStatus   # Always PAPER_TRADE_REQUIRED for go/borderline

    wfo_consistency_score: Optional[float]
    mc_deep_ruin_probability: Optional[float]

    sensitivity_spike: bool
    oos_gate_triggered: bool
    window_collapse_flag: bool
    sensitivity_profile_incomplete: bool

    median_oos_delta: Optional[float]
    parameter_region_width: Optional[float]
    yaml_output_path: Optional[str]

    evidence_summary: str

    def __post_init__(self):
        if not self.candidate_id:
            raise ValueError("candidate_id must not be empty")
        if self.verdict in (Verdict.AUTO_GO, Verdict.BORDERLINE):
            if self.deployment_status != DeploymentStatus.PAPER_TRADE_REQUIRED:
                raise ValueError(
                    f"deployment_status must be PAPER_TRADE_REQUIRED for {self.verdict.value} "
                    f"verdicts; got {self.deployment_status.value}. "
                    "Operator must manually promote to LIVE_APPROVED."
                )
        if not self.evidence_summary:
            raise ValueError("evidence_summary must not be empty")


# ────────────────────────────────────────────────────────────
# CandidateRecord  (SQLite row representation)
# ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class CandidateRecord:
    """
    Flattened SQLite row for a candidate at a specific pipeline stage.
    One record per candidate per stage. All fields are primitive types.
    Individual parameter columns are stored separately in candidate_parameters;
    this record carries the parameters_json audit backup.
    """
    run_id: str
    candidate_id: str
    zone_name: str
    stage: str                              # CandidateStage.value
    generation: Optional[int]
    recorded_at: datetime

    parameters_json: str                    # Full JSON backup

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
    wfo_median_oos_delta: Optional[float]       # M-01: from WFOConsistencyScore.median_oos_delta

    # MC pre-filter
    mc_prefilter_ruin_probability: Optional[float]
    mc_prefilter_avg_final_equity: Optional[float]
    mc_prefilter_iterations: Optional[int]

    # MC deep
    mc_deep_ruin_probability: Optional[float]
    mc_deep_avg_final_equity: Optional[float]
    mc_deep_worst_drawdown: Optional[float]
    mc_deep_p5_final_equity: Optional[float]
    mc_deep_iterations: Optional[int]

    # Sensitivity
    sensitivity_spike_detected: Optional[bool]
    sensitivity_spike_parameters: Optional[str]   # Comma-separated
    sensitivity_profile_complete: Optional[bool]

    # Verdict
    verdict: Optional[str]
    deployment_status: Optional[str]
    evidence_summary: Optional[str]

    def __post_init__(self):
        if not self.run_id:
            raise ValueError("run_id must not be empty")
        if not self.candidate_id:
            raise ValueError("candidate_id must not be empty")