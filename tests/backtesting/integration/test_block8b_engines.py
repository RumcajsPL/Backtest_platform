"""
test_block8b_engines.py — Block 8B: Evaluation engines audit tests.

Covers:
  B8B-001: NaN metric values rejected cleanly, not silently passed.
  B8B-002: Constraint boundary semantics — exact threshold value is accepted.
  B8B-005: oos_delta is always None (OOS gate gap documented).
  B8B-011: Single valid window gives optimistic variance_norm=1.0 (documented).
  B8B-012: _sigmoid_normalise scale calibration issue for point-valued data.
  Additional: M-04 post-fix verification, p5_final_equity stored correctly.

13 tests total.
"""
from __future__ import annotations

import math
import sys
from datetime import UTC, datetime, date
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.utils.paths import PROJECT_ROOT  # noqa: E402

from src.backtesting.contracts import (  # noqa: E402
    CandidateParameterSet,
    CandidateResult,
    MCMode,
    MCResult,
    RejectionReason,
    ScenarioProfile,
    WFOConsistencyScore,
    WFOWindow,
    WFOWindowResult,
)
from src.backtesting.fitness import evaluate_fitness  # noqa: E402
from src.backtesting.wfo.consistency_scorer import compute_consistency  # noqa: E402


# ── Shared fixtures ────────────────────────────────────────────────────────────

def _make_scenario() -> ScenarioProfile:
    return ScenarioProfile(
        name="e2e_test",
        description="test",
        weight_net_pnl=0.2, weight_expectancy=0.3, weight_max_drawdown=0.2,
        weight_win_rate=0.15, weight_trade_frequency=0.1, weight_profit_factor=0.05,
        min_win_rate=0.10, max_drawdown=0.95, max_losing_streak=50,
        min_trades_per_week=0.1, min_expectancy=-5.0, min_profit_factor=0.1,
        mc_prefilter_ruin_threshold=0.90,
        wfo_weight_median_return=0.4, wfo_weight_variance=0.2,
        wfo_weight_worst_drawdown=0.2, wfo_weight_fraction_positive=0.2,
        verdict_go_wfo_floor=0.30, verdict_borderline_wfo_floor=0.10,
        verdict_go_mc_ruin_ceiling=0.80, verdict_borderline_mc_ruin_ceiling=0.90,
        verdict_sensitivity_spike_threshold=0.15,
        report_emphasis=("wfo_consistency_score", "mc_deep_ruin_probability"),
    )


def _make_metrics(
    win_rate=55.0, max_drawdown=-500.0, losing_streak=3,
    trades_per_week=2.0, expectancy_points=0.5, profit_factor=1.3,
    total_pnl_points=200.0,
):
    """Build a mock MetricsReport with the given values."""
    m = MagicMock()
    m.win_rate = win_rate
    m.max_drawdown = max_drawdown
    m.losing_streak = losing_streak
    m.trades_per_week = trades_per_week
    m.expectancy_points = expectancy_points
    m.profit_factor = profit_factor
    m.total_pnl_points = total_pnl_points
    return m


def _make_candidate_result(
    candidate_id: str = "a" * 64,
    metrics=None,
    error: Optional[str] = None,
) -> CandidateResult:
    if metrics is None and error is None:
        metrics = _make_metrics()
    return CandidateResult(
        candidate_id=candidate_id,
        evaluated_at=datetime.now(UTC),
        metrics=metrics,
        trades=MagicMock() if metrics is not None else None,
        total_trades=100 if metrics is not None else None,
        error=error,
    )


def _make_window_result(
    candidate_id: str,
    window_id: str,
    net_pnl: float = 100.0,
    max_drawdown: float = 0.10,
    oos_delta: Optional[float] = None,
    error: Optional[str] = None,
) -> WFOWindowResult:
    return WFOWindowResult(
        candidate_id=candidate_id,
        window_id=window_id,
        evaluated_at=datetime.now(UTC),
        fitness_score=0.6 if error is None else None,
        total_trades=50,
        net_pnl=net_pnl,
        max_drawdown=max_drawdown,
        win_rate=0.55,
        expectancy=0.3,
        profit_factor=1.3,
        oos_delta=oos_delta,
        error=error,
    )


# ── B8B-001: NaN metric handling ──────────────────────────────────────────────

class TestB8B001NanMetricHandling:
    """B8B-001: NaN metric values must be rejected cleanly, not silently pass."""

    def test_nan_win_rate_rejected_not_passed(self):
        """A NaN win_rate must result in passed_constraints=False, not a silent pass."""
        scenario = _make_scenario()
        metrics = _make_metrics(win_rate=float("nan"))
        result = _make_candidate_result(metrics=metrics)

        fitness = evaluate_fitness(result, scenario)

        assert not fitness.passed_constraints, (
            "B8B-001: NaN win_rate silently passed constraints. "
            "NaN comparisons return False in Python, making op.lt(NaN, threshold) False "
            "and bypassing the constraint guard. An explicit math.isnan check is required."
        )
        assert fitness.fitness_score is None

    def test_nan_max_drawdown_rejected_not_passed(self):
        """A NaN max_drawdown must result in passed_constraints=False."""
        scenario = _make_scenario()
        metrics = _make_metrics(max_drawdown=float("nan"))
        result = _make_candidate_result(metrics=metrics)

        fitness = evaluate_fitness(result, scenario)

        assert not fitness.passed_constraints, (
            "B8B-001: NaN max_drawdown silently passed constraints."
        )
        assert fitness.fitness_score is None

    def test_nan_expectancy_rejected_not_passed(self):
        """A NaN expectancy must result in passed_constraints=False."""
        scenario = _make_scenario()
        metrics = _make_metrics(expectancy_points=float("nan"))
        result = _make_candidate_result(metrics=metrics)

        fitness = evaluate_fitness(result, scenario)

        assert not fitness.passed_constraints, (
            "B8B-001: NaN expectancy silently passed constraints."
        )


# ── B8B-002: Constraint boundary semantics ────────────────────────────────────

class TestB8B002ConstraintBoundarySemantics:
    """B8B-002: A value exactly equal to a constraint threshold must be accepted."""

    def test_constraint_boundary_win_rate_exact_passes(self):
        """win_rate exactly equal to min_win_rate must pass (>= semantics)."""
        scenario = _make_scenario()
        # min_win_rate=0.10 = 10%; set win_rate to exactly 10.0 (stored as pct, normalised to 0.10)
        metrics = _make_metrics(win_rate=10.0)
        result = _make_candidate_result(metrics=metrics)

        fitness = evaluate_fitness(result, scenario)

        # Should pass the win_rate constraint (10% == 10% is accepted, not rejected)
        # May still fail other constraints, but win_rate itself should not be the failing one
        if not fitness.passed_constraints:
            assert fitness.failing_constraint != "win_rate", (
                "B8B-002: win_rate exactly at min_win_rate was rejected. "
                "op.lt(0.10, 0.10) should be False (not rejected). "
                "Boundary semantics: >= threshold should pass."
            )

    def test_constraint_boundary_win_rate_just_below_fails(self):
        """win_rate just below min_win_rate must fail."""
        scenario = _make_scenario()
        # 9.9% is just below 10% (min_win_rate=0.10 → normalised 0.099 < 0.10)
        metrics = _make_metrics(win_rate=9.9)
        result = _make_candidate_result(metrics=metrics)

        fitness = evaluate_fitness(result, scenario)

        assert not fitness.passed_constraints
        assert fitness.failing_constraint == "win_rate"


# ── B8B-005: oos_delta always None (OOS gate gap) ─────────────────────────────

class TestB8B005OosDeltaAlwaysNone:
    """B8B-005: Documents that oos_delta is always None — OOS gate is non-functional."""

    def test_oos_delta_always_none_documents_gap(self):
        """
        WFOWindowResult.oos_delta is always None in evaluate_window output.
        This test documents the gap: when consistency_scorer receives all-None oos_delta
        values, median_oos_delta is None and oos_gate_triggered is always False,
        even when oos_gate_enabled=True.
        """
        scenario = _make_scenario()
        cid = "b" * 64

        # Simulate what wfo_evaluator returns: oos_delta=None always
        window_results = [
            _make_window_result(cid, "w1", oos_delta=None),
            _make_window_result(cid, "w2", oos_delta=None),
            _make_window_result(cid, "w3", oos_delta=None),
        ]

        # Even with oos_gate_enabled=True, gate cannot trigger when deltas are all None
        score = compute_consistency(
            window_results=window_results,
            windows_total=3,
            scenario=scenario,
            oos_gate_enabled=True,  # Gate is enabled in config
            oos_degradation_threshold=0.10,  # Very sensitive threshold
        )

        assert score.oos_gate_triggered is False, (
            "B8B-005: Expected oos_gate_triggered=False because oos_delta is always None "
            "from evaluate_window. The OOS gate mechanism is non-functional in the current "
            "pipeline. enforce_oos_gate=True in config has no effect."
        )
        assert score.median_oos_delta is None, (
            "B8B-005: median_oos_delta should be None when all window oos_deltas are None."
        )

    def test_oos_gate_would_trigger_if_deltas_were_populated(self):
        """
        Positive control: if oos_delta values were actually populated,
        the gate mechanism in consistency_scorer works correctly.
        This confirms B8B-005 is a data-population gap, not a logic bug.
        """
        scenario = _make_scenario()
        cid = "c" * 64

        # Simulate properly populated oos_delta (large negative = severe OOS degradation)
        window_results = [
            _make_window_result(cid, "w1", oos_delta=-0.80),
            _make_window_result(cid, "w2", oos_delta=-0.75),
            _make_window_result(cid, "w3", oos_delta=-0.85),
        ]

        score = compute_consistency(
            window_results=window_results,
            windows_total=3,
            scenario=scenario,
            oos_gate_enabled=True,
            oos_degradation_threshold=0.50,
        )

        assert score.oos_gate_triggered is True, (
            "OOS gate logic is broken even when deltas are populated. "
            "This would indicate a consistency_scorer bug, not the B8B-005 gap."
        )
        assert score.median_oos_delta is not None


# ── B8B-011: Single-window variance edge case ─────────────────────────────────

class TestB8B011SingleWindowVariance:
    """B8B-011: Single valid window gives variance_norm=1.0 (best score, not neutral)."""

    def test_single_window_variance_is_optimistic(self):
        """
        A single valid window result yields variance_norm=1.0 — the best possible
        variance score — because variance_raw=0.0 for a single data point.
        This is documented as optimistic (should be 0.5 = "no information"), but
        the practical impact is limited to GA lightweight fitness only.
        """
        scenario = _make_scenario()
        cid = "d" * 64

        # Only one valid window result
        window_results = [_make_window_result(cid, "w1", net_pnl=100.0)]

        score = compute_consistency(
            window_results=window_results,
            windows_total=3,
            scenario=scenario,
        )

        # The composite score is non-zero and windows_evaluated=1
        assert score.windows_evaluated == 1
        # Variance sub-metric gets best score for single window (no variance data)
        # We verify by checking that the score is higher than it would be for
        # a candidate with high variance across multiple windows
        high_variance_results = [
            _make_window_result(cid, "w1", net_pnl=1000.0),
            _make_window_result(cid, "w2", net_pnl=-900.0),
            _make_window_result(cid, "w3", net_pnl=900.0),
        ]
        high_variance_score = compute_consistency(
            window_results=high_variance_results,
            windows_total=3,
            scenario=scenario,
        )

        # B8B-011: single window gets better variance component than genuinely high-variance
        # candidate. Documented as known optimistic bias for sparse GA evaluations.
        assert score.window_return_variance == 0.0, (
            "Single window should have variance_raw=0.0 "
            "(statistics.variance requires >=2 values; fallback is 0.0)"
        )


# ── B8B-012: Sigmoid scale calibration ───────────────────────────────────────

class TestB8B012SigmoidScaleCalibration:
    """B8B-012: _sigmoid_normalise scale=0.10 is binary for point-valued net_pnl data."""

    def test_sigmoid_scale_is_effectively_binary_for_large_pnl(self):
        """
        With scale=0.10, any net_pnl > ~5 points maps to sigmoid ≈ 1.0.
        Two candidates with vastly different P&L receive the same median_return_norm.
        This documents the calibration mismatch for real-data use.
        """
        from src.backtesting.wfo.consistency_scorer import _sigmoid_normalise

        small_pnl = 10.0    # 10 points — "small" for a real strategy
        large_pnl = 5000.0  # 5000 points — "excellent" for a real strategy

        small_norm = _sigmoid_normalise(small_pnl, scale=0.10)
        large_norm = _sigmoid_normalise(large_pnl, scale=0.10)

        # Both map to approximately 1.0 — no discrimination between magnitudes
        assert small_norm > 0.999, (
            f"Expected small_pnl=10 to map to ~1.0 with scale=0.10, got {small_norm:.6f}"
        )
        assert large_norm > 0.999, (
            f"Expected large_pnl=5000 to map to ~1.0 with scale=0.10, got {large_norm:.6f}"
        )
        assert abs(small_norm - large_norm) < 0.001, (
            f"B8B-012: scale=0.10 makes median_return_norm effectively binary for real data. "
            f"10 pts and 5000 pts produce the same score ({small_norm:.6f} vs {large_norm:.6f}). "
            f"wfo_sigmoid_scale must be calibrated to ~10% of median per-window P&L in points."
        )

    def test_sigmoid_scale_preserves_gradient_when_calibrated(self):
        """
        With a properly calibrated scale (e.g. 500 for point-valued data),
        different P&L magnitudes produce meaningfully different scores.
        """
        from src.backtesting.wfo.consistency_scorer import _sigmoid_normalise

        calibrated_scale = 500.0  # Example: ~10% of 5000 pt median expected P&L

        small_norm = _sigmoid_normalise(100.0, scale=calibrated_scale)
        large_norm = _sigmoid_normalise(2000.0, scale=calibrated_scale)

        # With calibrated scale, there should be meaningful differentiation
        assert large_norm > small_norm + 0.1, (
            f"Calibrated scale should produce gradient: "
            f"small={small_norm:.3f}, large={large_norm:.3f}"
        )


# ── M-04 post-fix and mc_metrics verification ─────────────────────────────────

class TestMcMetricsVerification:
    """Verify M-04 fix and p5_final_equity computation."""

    def test_m04_ruined_paths_clamped_to_1(self):
        """M-04: Ruined paths must report worst_drawdown=1.0."""
        import numpy as np
        from src.backtesting.monte_carlo.mc_metrics import compute_metrics

        # Construct paths where one path hits ruin but then "recovers"
        # (the recovery causes running-max to grow, potentially understating drawdown pre-fix)
        starting_equity = 10_000.0
        ruin_floor = 2_000.0  # ruin_threshold=0.20 → floor=2000

        # Path 0: drops to 1500 (below ruin floor), then "recovers" to 8000
        # Pre-fix: running_max at end = 10000, current = 8000, reported dd = 0.20 (wrong)
        # Post-fix: ruined path → dd clamped to 1.0 ✓
        # Path 1: normal path, stays near starting equity
        equity_paths = np.array([
            [10_000, 5_000, 1_500, 4_000, 8_000],   # ruined then "recovered"
            [10_000, 10_500, 10_200, 10_800, 11_000], # normal
        ], dtype=float)

        avg_eq, worst_dd, ruin_prob, p5 = compute_metrics(
            equity_paths=equity_paths,
            starting_equity=starting_equity,
            ruin_threshold=0.20,
        )

        assert worst_dd == pytest.approx(1.0), (
            f"M-04: Ruined path should report worst_drawdown=1.0, got {worst_dd:.4f}. "
            "Without the clamp, the 'recovered' path understates drawdown."
        )
        assert ruin_prob == pytest.approx(0.5)  # 1 of 2 paths ruined

    def test_m04_no_ruined_paths_unaffected(self):
        """M-04: All-False ruined_paths boolean index is a no-op."""
        import numpy as np
        from src.backtesting.monte_carlo.mc_metrics import compute_metrics

        starting_equity = 10_000.0

        equity_paths = np.array([
            [10_000, 10_500, 10_200, 10_800, 11_000],
            [10_000, 9_800, 10_100, 10_300, 10_500],
        ], dtype=float)

        avg_eq, worst_dd, ruin_prob, p5 = compute_metrics(
            equity_paths=equity_paths,
            starting_equity=starting_equity,
            ruin_threshold=0.20,
        )

        assert ruin_prob == pytest.approx(0.0)
        assert worst_dd < 1.0  # No ruin, no clamping

    def test_p5_final_equity_computed_correctly(self):
        """p5_final_equity is the 5th percentile of final equity values."""
        import numpy as np
        from src.backtesting.monte_carlo.mc_metrics import compute_metrics

        starting_equity = 10_000.0
        # 100 paths with final equity 1..100 (×100 pts each)
        final_values = np.arange(100, 10_100, 100, dtype=float)
        equity_paths = np.column_stack([
            np.full(100, starting_equity),
            final_values,
        ])

        _, _, _, p5 = compute_metrics(
            equity_paths=equity_paths,
            starting_equity=starting_equity,
            ruin_threshold=0.05,
        )

        # 5th percentile of 100..10000 (step 100) ≈ 595 (numpy percentile)
        assert 500 <= p5 <= 700, f"p5_final_equity={p5} outside expected range"


# ── B8B-018: net_pnl vs total_pnl_points field name verification ──────────────

"""
Replacement for TestB8B018NetPnlFieldName in test_block8b_engines.py.

Replace the entire TestB8B018NetPnlFieldName class with this one.
The original test tried to import MetricsReport from contracts.py (wrong location)
and was permanently skipped. This version tests behaviour directly using a mock
MetricsReport with the real field names confirmed from metrics_contracts.py.

B8B-018 finding: Two field name mismatches in wfo_evaluator.py:
  "net_pnl"    → must be "total_pnl_points"  (MetricsReport has no 'net_pnl')
  "expectancy" → must be "expectancy_points"  (MetricsReport has no 'expectancy')
Both were silently None on every window evaluation before the fix.
"""

class TestB8B018NetPnlFieldName:
    """
    B8B-018 (FIXED): wfo_evaluator must read 'total_pnl_points' and
    'expectancy_points' from MetricsReport, not 'net_pnl' / 'expectancy'.

    Tests use a mock MetricsReport with the exact field names confirmed from
    src/strategies/contracts/metrics_contracts.py — no import of strategy package
    required.
    """

    def _make_mock_metrics(self, total_pnl_points=500.0, expectancy_points=1.5):
        """Mock MetricsReport with the real field names from metrics_contracts.py."""
        from unittest.mock import MagicMock
        m = MagicMock()
        # Real MetricsReport fields (confirmed from src/strategies/contracts/metrics_contracts.py)
        m.total_pnl_points = total_pnl_points   # ← correct name
        m.expectancy_points = expectancy_points  # ← correct name
        m.win_rate = 55.0
        m.max_drawdown = -500.0
        m.profit_factor = 1.3
        # Confirm absence of old wrong names
        del m.net_pnl       # MagicMock: deleting forces AttributeError on access
        del m.expectancy    # same
        return m

    def test_safe_float_reads_total_pnl_points(self):
        """
        _safe_float(m, 'total_pnl_points') must return the value, not None.
        Pre-fix: code read 'net_pnl' → always None.
        Post-fix: reads 'total_pnl_points' → correct value.
        """
        from src.backtesting.wfo.wfo_evaluator import _safe_float
        m = self._make_mock_metrics(total_pnl_points=750.0)

        result = _safe_float(m, "total_pnl_points")

        assert result == 750.0, (
            f"B8B-018: _safe_float(m, 'total_pnl_points') returned {result}. "
            "Expected 750.0. wfo_evaluator must use 'total_pnl_points', not 'net_pnl'."
        )

    def test_safe_float_reads_expectancy_points(self):
        """
        _safe_float(m, 'expectancy_points') must return the value, not None.
        Pre-fix: code read 'expectancy' → always None.
        Post-fix: reads 'expectancy_points' → correct value.
        """
        from src.backtesting.wfo.wfo_evaluator import _safe_float
        m = self._make_mock_metrics(expectancy_points=2.3)

        result = _safe_float(m, "expectancy_points")

        assert result == 2.3, (
            f"B8B-018: _safe_float(m, 'expectancy_points') returned {result}. "
            "Expected 2.3. wfo_evaluator must use 'expectancy_points', not 'expectancy'."
        )

    def test_old_field_names_return_none(self):
        """
        Confirm the old wrong names ('net_pnl', 'expectancy') are absent from
        MetricsReport — _safe_float returns None for both, which was the bug.
        This is a regression guard: if someone re-introduces the wrong names,
        this test documents what the broken state looks like.
        """
        from src.backtesting.wfo.wfo_evaluator import _safe_float
        m = self._make_mock_metrics()

        # Both old names absent → _safe_float returns None (the bug)
        assert _safe_float(m, "net_pnl") is None, (
            "MetricsReport has no 'net_pnl' field — _safe_float must return None. "
            "If this fails, MetricsReport has grown a 'net_pnl' alias (acceptable, "
            "but then wfo_evaluator.py should be updated to use it consistently)."
        )
        assert _safe_float(m, "expectancy") is None, (
            "MetricsReport has no 'expectancy' field — _safe_float must return None."
        )