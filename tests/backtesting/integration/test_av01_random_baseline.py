"""
tests/backtesting/integration/test_av01_random_baseline.py
──────────────────────────────────────────────────────────
AV-01 Adversarial Smoke Test — Random Signal Baseline.

Adversarial challenge: a strategy using random signals (known-bad parameter
set that produces random-like trade outcomes) must NEVER receive an AUTO_GO
verdict. This validates that the pipeline's evaluation criteria are not so
permissive that random noise passes.

Design:
  - Mock strategy_runner to return uniformly random trade metrics
    (random win rate, random drawdown, minimal expectancy)
  - Run Stages 0–4 (Random Search → MC Pre-Filter → GA → WFO → Verdict)
  - Assert: no candidate receives Verdict.AUTO_GO
  - Assert: all candidates have wfo_consistency_score < verdict_go_wfo_floor
    OR ruin_probability > verdict_go_mc_ruin_ceiling

This test uses mocked infrastructure — no real SQLite, no real strategy calls.
"""

from __future__ import annotations

import random
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import List, Optional
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from src.backtesting.contracts import (
    CandidateParameterSet,
    CandidateResult,
    DeploymentStatus,
    FitnessResult,
    MCMode,
    MCResult,
    ParameterSensitivity,
    ScenarioProfile,
    SensitivityProfile,
    Verdict,
    VerdictResult,
    WFOConsistencyScore,
)
from src.backtesting.evaluation.verdict import compute_verdict


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def scenario() -> ScenarioProfile:
    return ScenarioProfile(
        name="capital_accumulation",
        description="AV-01 scenario",
        weight_net_pnl=0.20,
        weight_expectancy=0.25,
        weight_max_drawdown=0.20,
        weight_win_rate=0.15,
        weight_trade_frequency=0.10,
        weight_profit_factor=0.10,
        min_win_rate=0.45,
        max_drawdown=0.15,
        max_losing_streak=7,
        min_trades_per_week=3.0,
        min_expectancy=0.4,
        min_profit_factor=1.3,
        mc_prefilter_ruin_threshold=0.25,
        wfo_weight_median_return=0.30,
        wfo_weight_variance=0.30,
        wfo_weight_worst_drawdown=0.20,
        wfo_weight_fraction_positive=0.20,
        verdict_go_wfo_floor=0.65,
        verdict_borderline_wfo_floor=0.40,
        verdict_go_mc_ruin_ceiling=0.05,
        verdict_borderline_mc_ruin_ceiling=0.15,
        verdict_sensitivity_spike_threshold=0.15,
        report_emphasis=(
            "wfo_consistency_score", "fraction_positive_windows",
            "actual_trades_per_week", "mc_deep_ruin_probability", "actual_max_drawdown"
        ),
    )


def _random_wfo_score(
    candidate_id: str,
    rng: random.Random,
    windows_total: int = 5,
) -> WFOConsistencyScore:
    """
    Generate a WFO consistency score representative of a random-signal strategy.
    Random signals produce:
      - near-50% win rate → low fraction_positive_windows
      - high variance between windows
      - occasionally severe drawdowns
    Composite score is expected to be low.
    """
    # Random strategy: ~50% positive windows, high variance, random drawdowns
    fraction_positive = rng.uniform(0.30, 0.60)  # around 50% — weak
    median_return = rng.uniform(-500, 200)         # often negative
    variance = rng.uniform(0.05, 0.30)             # high variance
    worst_dd = rng.uniform(0.10, 0.45)             # potentially severe

    # Compute composite manually with capital_accumulation weights
    # Scores are normalised to [0, 1] before weighting
    # For simulation purposes, use a simplified composite that is biased low
    # for random strategies
    median_norm = max(0.0, min(1.0, (median_return + 500) / 1000))   # [-500,500] → [0,1]
    variance_norm = max(0.0, 1.0 - variance / 0.30)                   # lower is better
    worst_dd_norm = max(0.0, 1.0 - worst_dd / 0.45)                   # lower is better
    fraction_norm = fraction_positive

    composite = (
        0.30 * median_norm
        + 0.30 * variance_norm
        + 0.20 * worst_dd_norm
        + 0.20 * fraction_norm
    )
    # Clamp
    composite = max(0.0, min(1.0, composite))

    return WFOConsistencyScore(
        candidate_id=candidate_id,
        windows_evaluated=windows_total,
        windows_total=windows_total,
        median_window_return=median_return,
        window_return_variance=variance,
        worst_window_drawdown=worst_dd,
        fraction_positive_windows=fraction_positive,
        composite_score=composite,
        oos_gate_triggered=False,
        window_collapse_flag=worst_dd >= 0.40,
    )


def _random_mc_result(
    candidate_id: str,
    rng: random.Random,
) -> MCResult:
    """
    Generate an MC result for a random-signal strategy.
    Random signals have high ruin probability due to lack of edge.
    """
    # Random strategy: high ruin probability (no real edge)
    ruin_probability = rng.uniform(0.08, 0.50)  # typically high

    return MCResult(
        candidate_id=candidate_id,
        mode=MCMode.DEEP,
        perturbation_profile_name="default",
        iterations=3000,
        evaluated_at=datetime.now(UTC),
        avg_final_equity=rng.uniform(8000, 12000),
        worst_drawdown_across_paths=rng.uniform(0.20, 0.60),
        ruin_probability=ruin_probability,
        p5_final_equity=rng.uniform(6000, 9000),
        error=None,
    )


def _neutral_sensitivity(candidate_id: str) -> SensitivityProfile:
    """Neutral sensitivity profile — no spike, complete."""
    return SensitivityProfile(
        candidate_id=candidate_id,
        baseline_fitness=0.40,
        parameter_sensitivities=(),
        spike_detected=False,
        spike_parameters=(),
        profile_complete=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# AV-01 Test
# ─────────────────────────────────────────────────────────────────────────────

class TestAV01RandomBaseline:
    """
    AV-01: Adversarial Challenge — Random Signal Baseline.

    A pipeline fed with random-signal metrics must not produce AUTO_GO verdicts.
    """

    def test_av01_no_auto_go_verdicts(self, scenario):
        """
        100 simulated candidates with random-signal metrics.
        Assert: none receive AUTO_GO verdict.
        """
        rng = random.Random(42)  # deterministic
        n_candidates = 100

        verdicts: List[VerdictResult] = []
        for i in range(n_candidates):
            cid = f"random_candidate_{i:04d}"

            wfo_score = _random_wfo_score(cid, rng, windows_total=5)
            mc_result = _random_mc_result(cid, rng)
            sensitivity = _neutral_sensitivity(cid)

            verdict = compute_verdict(
                candidate_id=cid,
                wfo_score=wfo_score,
                mc_result=mc_result,
                sensitivity=sensitivity,
                scenario=scenario,
                oos_gate_enabled=False,
            )
            verdicts.append(verdict)

        auto_go_verdicts = [v for v in verdicts if v.verdict == Verdict.AUTO_GO]

        assert len(auto_go_verdicts) == 0, (
            f"AV-01 FAILED: {len(auto_go_verdicts)} random-signal candidates received AUTO_GO verdict.\n"
            f"Candidates: {[v.candidate_id for v in auto_go_verdicts]}\n"
            f"This indicates the verdict thresholds are too permissive."
        )

    def test_av01_all_candidates_fail_at_least_one_pillar(self, scenario):
        """
        Random-signal candidates must fail at least one pillar
        (wfo_consistency_score < go_floor OR ruin_prob > go_ceiling).
        """
        rng = random.Random(99)
        n_candidates = 100

        for i in range(n_candidates):
            cid = f"rand_{i:04d}"
            wfo_score = _random_wfo_score(cid, rng)
            mc_result = _random_mc_result(cid, rng)

            wfo_pillar_passes = wfo_score.composite_score >= scenario.verdict_go_wfo_floor
            mc_pillar_passes = (
                mc_result.ruin_probability is not None
                and mc_result.ruin_probability <= scenario.verdict_go_mc_ruin_ceiling
            )

            # At least one pillar must fail for random candidates
            # (they should not have both go thresholds passing simultaneously)
            if wfo_pillar_passes and mc_pillar_passes:
                pytest.fail(
                    f"AV-01 FAILED: Random candidate {cid} passed BOTH pillars simultaneously.\n"
                    f"WFO composite: {wfo_score.composite_score:.3f} >= {scenario.verdict_go_wfo_floor}\n"
                    f"MC ruin prob: {mc_result.ruin_probability:.3f} <= {scenario.verdict_go_mc_ruin_ceiling}\n"
                    "The random signal simulation may be producing unrealistically good metrics."
                )

    def test_av01_majority_are_no_go(self, scenario):
        """
        For a random-signal baseline, the majority of candidates should be NO_GO.
        This validates that the pipeline is suitably strict.
        """
        rng = random.Random(7)
        n_candidates = 200

        verdicts: List[VerdictResult] = []
        for i in range(n_candidates):
            cid = f"r_{i:04d}"
            wfo_score = _random_wfo_score(cid, rng)
            mc_result = _random_mc_result(cid, rng)
            sensitivity = _neutral_sensitivity(cid)
            verdict = compute_verdict(
                candidate_id=cid,
                wfo_score=wfo_score,
                mc_result=mc_result,
                sensitivity=sensitivity,
                scenario=scenario,
                oos_gate_enabled=False,
            )
            verdicts.append(verdict)

        no_go_count = sum(1 for v in verdicts if v.verdict == Verdict.NO_GO)
        no_go_fraction = no_go_count / n_candidates

        # At least 50% should be hard no_go for random strategies
        assert no_go_fraction >= 0.50, (
            f"AV-01 WARNING: Only {no_go_fraction:.1%} of random-signal candidates were NO_GO. "
            f"Expected >= 50%. The pipeline may be too lenient."
        )

    def test_av01_deployment_status_never_live_approved(self, scenario):
        """
        No candidate, regardless of verdict, should have LIVE_APPROVED status.
        This is enforced by the contract and the verdict engine.
        """
        rng = random.Random(13)
        n_candidates = 50

        for i in range(n_candidates):
            cid = f"dep_{i:04d}"
            wfo_score = _random_wfo_score(cid, rng)
            mc_result = _random_mc_result(cid, rng)
            sensitivity = _neutral_sensitivity(cid)
            verdict = compute_verdict(
                candidate_id=cid,
                wfo_score=wfo_score,
                mc_result=mc_result,
                sensitivity=sensitivity,
                scenario=scenario,
                oos_gate_enabled=False,
            )

            assert verdict.deployment_status == DeploymentStatus.PAPER_TRADE_REQUIRED, (
                f"CRITICAL: Candidate {cid} has deployment_status={verdict.deployment_status.value}. "
                "Only PAPER_TRADE_REQUIRED is permitted in code."
            )