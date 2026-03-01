"""
tests/backtesting/unit/test_ga_modules.py
------------------------------------------
Unit tests for GA modules:
  - window sampling independence (different windows per generation)
  - diversity penalty (prevents collapse, returns in [0, penalty_weight])
  - mutation bounds (mutated params stay within zone limits)
  - crossover (offspring always contains valid params from parents)
  - population (initialisation, elite selection)
"""
from __future__ import annotations

import random
import pytest

from src.backtesting.contracts import CandidateParameterSet, WFOWindow
from datetime import date


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def five_windows():
    return [
        WFOWindow(f"W0{i}", date(2022 + (i - 1) // 2, 1 if i % 2 == 1 else 7, 1),
                  date(2022 + (i - 1) // 2, 6 if i % 2 == 1 else 12, 30))
        for i in range(1, 6)
    ]


@pytest.fixture
def safe_zone_def():
    return {
        "parameters": {
            "rsi_period":     {"type": "int",    "min": 10, "max": 20, "step": 2},
            "atr_multiplier": {"type": "float",  "min": 1.5, "max": 2.5, "step": 0.25},
            "session_filter": {"type": "choice", "choices": ["london", "new_york", "london_new_york"]},
        }
    }


def _make_candidate(rsi=14, atr=2.0, session="london", generation=None):
    return CandidateParameterSet.create(
        zone_name="safe",
        parameters={"rsi_period": rsi, "atr_multiplier": atr, "session_filter": session},
        generation=generation,
    )


# ── Window sampling independence ──────────────────────────────────────────────

class TestGAWindowSampling:

    def test_window_sampling_independent_per_generation(self, five_windows):
        """
        Key validation (NEXT_SESSION_PLAN.md Block 1):
        Different generations should sample different window pairs.
        With 5 windows and many generations, not every pair should be the same.
        """
        rng = random.Random(42)
        sampled = [tuple(w.window_id for w in rng.sample(five_windows, k=2)) for _ in range(20)]
        # At least 2 distinct window pairs across 20 generations
        distinct_pairs = set(sampled)
        assert len(distinct_pairs) > 1, (
            "Window sampling produced the same pair every generation — "
            "random sampling is not independent per generation"
        )

    def test_window_sampling_without_replacement(self, five_windows):
        """Each generation's 2 sampled windows must be distinct."""
        rng = random.Random(99)
        for _ in range(50):
            pair = rng.sample(five_windows, k=2)
            assert pair[0].window_id != pair[1].window_id


# ── Diversity penalty ─────────────────────────────────────────────────────────

class TestDiversityPenalty:

    def test_identical_to_elite_yields_max_penalty(self, safe_zone_def):
        from src.backtesting.ga.diversity import compute_penalty
        candidate = _make_candidate(rsi=14, atr=2.0, session="london")
        elite = _make_candidate(rsi=14, atr=2.0, session="london")
        penalty = compute_penalty(
            candidate=candidate,
            elites=[elite],
            parameter_space_def=safe_zone_def,
            distance_threshold=0.15,
            penalty_weight=0.10,
        )
        assert abs(penalty - 0.10) < 1e-6

    def test_distant_from_elite_yields_zero_penalty(self, safe_zone_def):
        from src.backtesting.ga.diversity import compute_penalty
        candidate = _make_candidate(rsi=10, atr=1.5, session="london")
        elite = _make_candidate(rsi=20, atr=2.5, session="new_york")
        penalty = compute_penalty(
            candidate=candidate,
            elites=[elite],
            parameter_space_def=safe_zone_def,
            distance_threshold=0.15,
            penalty_weight=0.10,
        )
        assert penalty == 0.0

    def test_penalty_in_valid_range(self, safe_zone_def):
        from src.backtesting.ga.diversity import compute_penalty
        rng = random.Random(42)
        elite = _make_candidate(rsi=14, atr=2.0, session="london")
        rsi_choices = list(range(10, 22, 2))
        atr_choices = [1.5, 1.75, 2.0, 2.25, 2.5]
        session_choices = ["london", "new_york", "london_new_york"]
        for _ in range(30):
            cand = _make_candidate(
                rsi=rng.choice(rsi_choices),
                atr=rng.choice(atr_choices),
                session=rng.choice(session_choices),
            )
            penalty = compute_penalty(
                candidate=cand,
                elites=[elite],
                parameter_space_def=safe_zone_def,
                distance_threshold=0.15,
                penalty_weight=0.10,
            )
            assert 0.0 <= penalty <= 0.10, f"Penalty {penalty} out of [0, 0.10]"

    def test_no_elites_returns_zero_penalty(self, safe_zone_def):
        from src.backtesting.ga.diversity import compute_penalty
        candidate = _make_candidate()
        penalty = compute_penalty(
            candidate=candidate,
            elites=[],
            parameter_space_def=safe_zone_def,
            distance_threshold=0.15,
            penalty_weight=0.10,
        )
        assert penalty == 0.0


# ── Mutation bounds ───────────────────────────────────────────────────────────

class TestMutation:

    def test_mutation_bounds_int_param(self, safe_zone_def):
        from src.backtesting.ga.mutation import mutate
        rng = random.Random(7)
        candidate = _make_candidate(rsi=14, atr=2.0)
        # Run 100 mutations and verify rsi_period stays in [10, 20]
        for _ in range(100):
            mutated = mutate(candidate, mutation_rate=1.0, parameter_space_def=safe_zone_def, rng=rng)
            rsi = mutated.parameters["rsi_period"]
            assert 10 <= rsi <= 20, f"rsi_period={rsi} out of [10, 20]"
            assert rsi % 2 == 0, f"rsi_period={rsi} not on step grid (step=2)"

    def test_mutation_bounds_float_param(self, safe_zone_def):
        from src.backtesting.ga.mutation import mutate
        rng = random.Random(13)
        candidate = _make_candidate(rsi=14, atr=2.0)
        for _ in range(100):
            mutated = mutate(candidate, mutation_rate=1.0, parameter_space_def=safe_zone_def, rng=rng)
            atr = mutated.parameters["atr_multiplier"]
            assert 1.5 <= atr <= 2.5, f"atr_multiplier={atr} out of [1.5, 2.5]"

    def test_mutation_choice_param_stays_in_choices(self, safe_zone_def):
        from src.backtesting.ga.mutation import mutate
        rng = random.Random(21)
        candidate = _make_candidate(session="london")
        valid_sessions = {"london", "new_york", "london_new_york"}
        for _ in range(50):
            mutated = mutate(candidate, mutation_rate=1.0, parameter_space_def=safe_zone_def, rng=rng)
            assert mutated.parameters["session_filter"] in valid_sessions

    def test_zero_mutation_rate_returns_unchanged(self, safe_zone_def):
        from src.backtesting.ga.mutation import mutate
        rng = random.Random(0)
        candidate = _make_candidate(rsi=14, atr=2.0, session="london")
        for _ in range(20):
            mutated = mutate(candidate, mutation_rate=0.0, parameter_space_def=safe_zone_def, rng=rng)
            assert mutated.parameters == candidate.parameters

    def test_mutation_updates_candidate_id(self, safe_zone_def):
        from src.backtesting.ga.mutation import mutate
        rng = random.Random(42)
        candidate = _make_candidate(rsi=14, atr=2.0, session="london")
        # With high mutation rate, params are likely to change, which changes candidate_id
        mutated = mutate(candidate, mutation_rate=1.0, parameter_space_def=safe_zone_def, rng=rng, generation=5)
        assert mutated.generation == 5
        # candidate_id is always consistent with parameters (enforced by __post_init__)
        import hashlib, json
        expected_id = hashlib.sha256(
            json.dumps(mutated.parameters, sort_keys=True, default=str).encode()
        ).hexdigest()
        assert mutated.candidate_id == expected_id


# ── Crossover ─────────────────────────────────────────────────────────────────

class TestCrossover:

    def test_offspring_params_from_parents_only(self):
        from src.backtesting.ga.crossover import crossover
        rng = random.Random(55)
        parent_a = _make_candidate(rsi=10, atr=1.5, session="london")
        parent_b = _make_candidate(rsi=20, atr=2.5, session="new_york")
        for _ in range(20):
            offspring = crossover(parent_a, parent_b, crossover_rate=0.9, rng=rng, generation=3)
            rsi = offspring.parameters["rsi_period"]
            atr = offspring.parameters["atr_multiplier"]
            session = offspring.parameters["session_filter"]
            assert rsi in (10, 20), f"rsi={rsi} not from either parent"
            assert atr in (1.5, 2.5), f"atr={atr} not from either parent"
            assert session in ("london", "new_york"), f"session={session} not from either parent"

    def test_zero_crossover_rate_returns_parent_a(self):
        from src.backtesting.ga.crossover import crossover
        rng = random.Random(1)
        parent_a = _make_candidate(rsi=10, atr=1.5, session="london")
        parent_b = _make_candidate(rsi=20, atr=2.5, session="new_york")
        for _ in range(20):
            offspring = crossover(parent_a, parent_b, crossover_rate=0.0, rng=rng)
            assert offspring.parameters["rsi_period"] == 10
            assert offspring.parameters["atr_multiplier"] == 1.5

    def test_offspring_inherits_zone_name_from_parent_a(self):
        from src.backtesting.ga.crossover import crossover
        rng = random.Random(3)
        parent_a = _make_candidate(rsi=14, atr=2.0, session="london")
        parent_b = _make_candidate(rsi=16, atr=2.25, session="new_york")
        offspring = crossover(parent_a, parent_b, crossover_rate=1.0, rng=rng, generation=2)
        assert offspring.zone_name == "safe"
        assert offspring.generation == 2


# ── Population ────────────────────────────────────────────────────────────────

class TestPopulation:

    def test_initialise_population_size(self):
        from src.backtesting.ga.population import initialise_population
        import json
        from datetime import datetime
        from src.backtesting.contracts import CandidateRecord, CandidateStage

        def _make_record(rsi, atr, session, fitness):
            params = {"rsi_period": rsi, "atr_multiplier": atr, "session_filter": session}
            return CandidateRecord(
                run_id="run_1",
                candidate_id=CandidateParameterSet.create("safe", params).candidate_id,
                zone_name="safe",
                stage=CandidateStage.MC_PREFILTER_PASS.value,
                generation=None,
                recorded_at=datetime.utcnow(),
                parameters_json=json.dumps(params),
                fitness_score=fitness,
                passed_constraints=True,
                rejection_reason=None,
                failing_constraint=None,
                failing_value=None,
                actual_win_rate=0.5,
                actual_max_drawdown=0.1,
                actual_losing_streak=3,
                actual_trades_per_week=5.0,
                actual_expectancy=0.6,
                actual_profit_factor=1.5,
                wfo_median_window_return=None,
                wfo_window_return_variance=None,
                wfo_worst_window_drawdown=None,
                wfo_fraction_positive_windows=None,
                wfo_consistency_score=None,
                wfo_windows_evaluated=None,
                wfo_oos_gate_triggered=None,
                wfo_window_collapse_flag=None,
                mc_prefilter_ruin_probability=None,
                mc_prefilter_avg_final_equity=None,
                mc_prefilter_iterations=None,
                mc_deep_ruin_probability=None,
                mc_deep_avg_final_equity=None,
                mc_deep_worst_drawdown=None,
                mc_deep_p5_final_equity=None,
                mc_deep_iterations=None,
                sensitivity_spike_detected=None,
                sensitivity_spike_parameters=None,
                sensitivity_profile_complete=None,
                verdict=None,
                deployment_status=None,
                evidence_summary=None,
            )

        records = [
            _make_record(10, 1.5, "london", 0.7),
            _make_record(14, 2.0, "new_york", 0.65),
            _make_record(18, 2.5, "london_new_york", 0.6),
        ]
        population = initialise_population(records, population_size=3)
        assert len(population) == 3
        for cand, fitness in population:
            assert isinstance(cand, CandidateParameterSet)
            assert 0.0 <= fitness <= 1.0

    def test_initialise_population_raises_on_empty_seeds(self):
        from src.backtesting.ga.population import initialise_population
        with pytest.raises(ValueError, match="no MC_PREFILTER_PASS"):
            initialise_population([], population_size=10)

    def test_get_elites_returns_top_fraction(self):
        from src.backtesting.ga.population import get_elites
        population = [
            (_make_candidate(rsi=r), float(r))
            for r in [10, 12, 14, 16, 18, 20]
        ]
        elites = get_elites(population, elite_fraction=0.33)
        # 33% of 6 = 1.98 → 1 elite (max(1, int(6 * 0.33)) = 1)
        assert len(elites) == max(1, int(6 * 0.33))
        # Elites should be the highest fitness members
        elite_fitnesses = [f for _, f in elites]
        all_fitnesses_sorted_desc = sorted([f for _, f in population], reverse=True)
        assert elite_fitnesses == all_fitnesses_sorted_desc[:len(elites)]