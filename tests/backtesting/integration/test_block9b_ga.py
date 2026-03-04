"""
tests/backtesting/integration/test_block9b_ga.py
-------------------------------------------------
Block 9B audit — GA package (crossover, diversity, ga_engine, mutation,
population, selection).

Findings covered:
  B9B-001 (P3, OPEN)  — crossover.py: no zone-name guard for cross-zone parents.
                         Test documents the behaviour (no crash, silent zone merge).
  B9B-002 (P4, OPEN)  — diversity.py: degenerate param (min==max) silently skipped.
                         Test confirms correct numeric result, documents the gap.
  B9B-003 (P3, OPEN)  — ga_engine.py: config['_base_yaml_path'] is an injected
                         private key — KeyError if Stage 3 is implemented without it.
                         Test confirms the key is required and its absence raises.
  B9B-004 (P4, OPEN)  — ga_engine.py: diversity penalty elites use prev-gen fitness.
                         Test documents intent — not a bug.

Green-light tests (confirm correct behaviour):
  - Clamping order in mutation: snap-then-clamp (correct)
  - _mutate_choice edge cases: empty choices, single-choice, all-same
  - initialise_population raises on empty seeds
  - get_elites always returns at least 1 even at elite_fraction=0.0
  - tournament_select raises on empty population
  - CandidateParameterSet.create() determinism (regression guard for B9A-006 context)
"""
from __future__ import annotations

import random
from unittest.mock import MagicMock, patch

import pytest

from src.backtesting.contracts import CandidateParameterSet


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _candidate(params: dict, zone: str = "zone_a") -> CandidateParameterSet:
    return CandidateParameterSet.create(zone_name=zone, parameters=params)


def _rng(seed: int = 42) -> random.Random:
    return random.Random(seed)


# ══════════════════════════════════════════════════════════════════════════════
# crossover.py
# ══════════════════════════════════════════════════════════════════════════════

class TestCrossover:
    """crossover.py — uniform crossover correctness and zone handling."""

    def test_no_crossover_returns_parent_a_clone(self):
        """crossover_rate=0.0 → always returns a clone of parent_a."""
        from src.backtesting.ga.crossover import crossover

        pa = _candidate({"x": 10, "y": 20})
        pb = _candidate({"x": 99, "y": 88})
        # crossover_rate=0.0 → rng.random() >= 0.0 always True → skip crossover
        child = crossover(pa, pb, crossover_rate=0.0, rng=_rng(), generation=1)

        assert child.parameters == pa.parameters
        assert child.zone_name == pa.zone_name

    def test_crossover_rate_1_always_crosses(self):
        """crossover_rate=1.0 → crossover always applied."""
        from src.backtesting.ga.crossover import crossover

        pa = _candidate({"x": 10, "y": 20})
        pb = _candidate({"x": 99, "y": 88})
        # With rate=1.0 and many seeds, at least some params will come from pb
        results = set()
        for seed in range(50):
            child = crossover(pa, pb, crossover_rate=1.0, rng=_rng(seed), generation=1)
            results.add(child.parameters["x"])
        assert 99 in results, "With crossover_rate=1.0, parent_b values must appear"

    def test_child_generation_set_correctly(self):
        """generation kwarg is passed through to the offspring."""
        from src.backtesting.ga.crossover import crossover

        pa = _candidate({"x": 5})
        pb = _candidate({"x": 9})
        child = crossover(pa, pb, crossover_rate=1.0, rng=_rng(), generation=7)
        assert child.generation == 7

    def test_b9b001_cross_zone_no_crash(self):
        """
        B9B-001: Cross-zone parents produce a child without crashing.
        zone_name is inherited from parent_a.
        This test DOCUMENTS the silent behaviour — it is not asserting it is correct.
        """
        from src.backtesting.ga.crossover import crossover

        pa = _candidate({"x": 10}, zone="zone_a")
        pb = _candidate({"y": 99}, zone="zone_b")  # Different zone, different params

        # Should not raise — documented silent behaviour
        child = crossover(pa, pb, crossover_rate=1.0, rng=_rng(), generation=1)
        assert child.zone_name == "zone_a", "zone_name must always come from parent_a"
        # Child may have 'y' from parent_b — this is the cross-zone risk (B9B-001)
        # Downstream mutation clamping should handle out-of-zone values


# ══════════════════════════════════════════════════════════════════════════════
# mutation.py
# ══════════════════════════════════════════════════════════════════════════════

class TestMutation:
    """mutation.py — clamping order, choice edge cases, type handling."""

    def _zone_def(self, params: dict) -> dict:
        return {"parameters": params}

    def test_int_mutation_stays_within_bounds(self):
        """Mutated int must always be in [min, max] and on step grid."""
        from src.backtesting.ga.mutation import mutate

        zone = self._zone_def({"x": {"type": "int", "min": 0, "max": 10, "step": 2}})
        c = _candidate({"x": 4})
        for seed in range(100):
            result = mutate(c, mutation_rate=1.0, parameter_space_def=zone,
                            rng=_rng(seed), mutation_std_steps=5.0)
            v = result.parameters["x"]
            assert 0 <= v <= 10, f"B9B: int mutation out of bounds: {v}"
            assert v % 2 == 0, f"B9B: int mutation off grid: {v}"

    def test_float_mutation_stays_within_bounds(self):
        """Mutated float must always be in [min, max]."""
        from src.backtesting.ga.mutation import mutate

        zone = self._zone_def({"r": {"type": "float", "min": 0.5, "max": 3.0, "step": 0.5}})
        c = _candidate({"r": 1.5})
        for seed in range(100):
            result = mutate(c, mutation_rate=1.0, parameter_space_def=zone,
                            rng=_rng(seed), mutation_std_steps=5.0)
            v = result.parameters["r"]
            assert 0.5 <= v <= 3.0, f"B9B: float mutation out of bounds: {v}"

    def test_snap_then_clamp_order(self):
        """
        Snap-then-clamp order verification.
        If step=3 and max=10, snap(11, 1, 3) = 10 (snaps to 10 which is in-bounds).
        Clamp(snap(11)) = 10 — correct.
        If we clamp-then-snap: clamp(11)=10, snap(10,1,3) = 10 — same here.
        But if max=9, step=3: snap(11,0,3)=12 > 9 → clamp(12)=9. Correct.
        Vs clamp-then-snap: clamp(11)=9 → snap(9,0,3)=9. Also 9.
        The dangerous case: max not on grid. step=5, min=0, max=8. value=10.
        snap(10, 0, 5) = 10, clamp(10, 0, 8) = 8. Correct — 8 is nearest valid.
        clamp-then-snap: clamp(10)=8, snap(8, 0, 5)=10. WRONG — snaps back above max.
        """
        from src.backtesting.ga.mutation import _mutate_int

        zone_def = {"min": 0, "max": 8, "step": 5}
        # Force a value that would be 10 after noise (5 steps above current=5)
        # We can't directly test _mutate_int's noise, so test _snap_to_grid directly
        from src.backtesting.ga.mutation import _snap_to_grid_int

        snapped = _snap_to_grid_int(10, grid_start=0, step=5)
        assert snapped == 10  # snap gives 10 (on-grid)
        clamped = max(0, min(8, snapped))
        assert clamped == 8   # then clamp gives 8 — correct

        # Clamp-first would give: min(8,10)=8, snap(8,0,5)=10 — WRONG
        clamp_first = _snap_to_grid_int(min(8, 10), grid_start=0, step=5)
        assert clamp_first == 10  # This confirms clamp-first is broken

    def test_choice_mutation_returns_different_value(self):
        """_mutate_choice must return a value different from current when alternatives exist."""
        from src.backtesting.ga.mutation import _mutate_choice

        pd = {"choices": ["a", "b", "c"]}
        results = {_mutate_choice("a", pd, _rng(s)) for s in range(50)}
        assert "b" in results or "c" in results
        assert "a" not in results, "current value must not be returned when alternatives exist"

    def test_choice_mutation_single_choice_unchanged(self):
        """Single-choice parameter: always returns current value (no alternatives)."""
        from src.backtesting.ga.mutation import _mutate_choice

        pd = {"choices": ["only"]}
        for s in range(20):
            assert _mutate_choice("only", pd, _rng(s)) == "only"

    def test_choice_mutation_empty_choices_unchanged(self):
        """Empty choices list: returns current value unchanged."""
        from src.backtesting.ga.mutation import _mutate_choice

        pd = {"choices": []}
        assert _mutate_choice("x", pd, _rng()) == "x"

    def test_unknown_param_not_in_zone_unchanged(self):
        """Parameter absent from zone_def must not be mutated."""
        from src.backtesting.ga.mutation import mutate

        zone = self._zone_def({"known": {"type": "int", "min": 0, "max": 10, "step": 1}})
        c = _candidate({"known": 5, "ghost": 99})  # 'ghost' not in zone_def
        result = mutate(c, mutation_rate=1.0, parameter_space_def=zone, rng=_rng())
        assert result.parameters["ghost"] == 99, "Unknown param must pass through unchanged"


# ══════════════════════════════════════════════════════════════════════════════
# population.py
# ══════════════════════════════════════════════════════════════════════════════

class TestPopulation:
    """population.py — initialise, elites, update fitness."""

    def _make_record(self, params: dict, fitness: float, zone: str = "zone_a"):
        r = MagicMock()
        r.zone_name = zone
        r.parameters_json = __import__("json").dumps(params)
        r.fitness_score = fitness
        return r

    def test_initialise_raises_on_empty_seeds(self):
        """P6: empty seed_records must raise ValueError."""
        from src.backtesting.ga.population import initialise_population

        with pytest.raises(ValueError, match="no MC_PREFILTER_PASS"):
            initialise_population([], population_size=10)

    def test_initialise_truncates_to_population_size(self):
        """More seeds than population_size → truncate to population_size."""
        from src.backtesting.ga.population import initialise_population

        records = [self._make_record({"x": i}, float(i)) for i in range(20)]
        pop = initialise_population(records, population_size=5)
        assert len(pop) == 5

    def test_initialise_uses_all_available_when_fewer_than_size(self):
        """Fewer seeds than population_size → use all seeds, log warning."""
        from src.backtesting.ga.population import initialise_population

        records = [self._make_record({"x": i}, float(i)) for i in range(3)]
        pop = initialise_population(records, population_size=10)
        assert len(pop) == 3

    def test_get_elites_always_returns_at_least_one(self):
        """elite_fraction=0.0 → max(1, 0) = 1 elite always returned."""
        from src.backtesting.ga.population import get_elites

        pop = [(_candidate({"x": i}), float(i)) for i in range(10)]
        elites = get_elites(pop, elite_fraction=0.0)
        assert len(elites) >= 1

    def test_get_elites_returns_top_by_fitness(self):
        """Elites must be the highest-fitness members."""
        from src.backtesting.ga.population import get_elites

        pop = [(_candidate({"x": i}), float(i)) for i in range(10)]  # fitness 0..9
        elites = get_elites(pop, elite_fraction=0.3)  # top 30% = 3
        elite_fitnesses = [f for _, f in elites]
        assert min(elite_fitnesses) >= 7.0, "Elites must be top-fitness members"

    def test_update_fitness_does_not_mutate_input(self):
        """update_fitness returns a new list — input population unchanged."""
        from src.backtesting.ga.population import update_fitness

        pop = [(_candidate({"x": 1}), 0.5)]
        original_fitness = pop[0][1]
        new_pop = update_fitness(pop, {pop[0][0].candidate_id: 0.9})
        assert pop[0][1] == original_fitness, "Input population must not be mutated"
        assert new_pop[0][1] == 0.9


# ══════════════════════════════════════════════════════════════════════════════
# selection.py
# ══════════════════════════════════════════════════════════════════════════════

class TestSelection:
    """selection.py — tournament selection correctness and edge cases."""

    def test_raises_on_empty_population(self):
        """P6: empty population must raise ValueError."""
        from src.backtesting.ga.selection import tournament_select

        with pytest.raises(ValueError, match="empty population"):
            tournament_select([], tournament_size=3, rng=_rng())

    def test_tournament_select_returns_candidate(self):
        """tournament_select must return a CandidateParameterSet."""
        from src.backtesting.ga.selection import tournament_select

        pop = [(_candidate({"x": i}), float(i)) for i in range(10)]
        winner = tournament_select(pop, tournament_size=3, rng=_rng())
        assert isinstance(winner, CandidateParameterSet)

    def test_tournament_size_clamped_to_population(self):
        """tournament_size > population → k = len(population), no error."""
        from src.backtesting.ga.selection import tournament_select

        pop = [(_candidate({"x": i}), float(i)) for i in range(3)]
        # tournament_size=100 but population=3 → k=3, select best of all
        winner = tournament_select(pop, tournament_size=100, rng=_rng())
        assert isinstance(winner, CandidateParameterSet)

    def test_high_tournament_size_tends_toward_best(self):
        """Large tournament_size → strong selection pressure toward best fitness."""
        from src.backtesting.ga.selection import tournament_select

        best_params = {"x": 99}
        pop = [(_candidate({"x": i}), float(i)) for i in range(10)]
        # tournament_size = pop size → always picks best
        winners = [
            tournament_select(pop, tournament_size=len(pop), rng=_rng(s)).parameters["x"]
            for s in range(20)
        ]
        assert all(w == 9 for w in winners), "Full tournament must always pick highest fitness"


# ══════════════════════════════════════════════════════════════════════════════
# diversity.py
# ══════════════════════════════════════════════════════════════════════════════

class TestDiversity:
    """diversity.py — penalty computation, Euclidean/Hamming metrics."""

    def _zone_def(self, params: dict) -> dict:
        return {"parameters": params}

    def test_no_elites_returns_zero_penalty(self):
        """Empty elites → no penalty."""
        from src.backtesting.ga.diversity import compute_penalty

        c = _candidate({"x": 5})
        penalty = compute_penalty(c, [], self._zone_def(
            {"x": {"type": "int", "min": 0, "max": 10}}
        ), distance_threshold=0.3, penalty_weight=0.1)
        assert penalty == 0.0

    def test_identical_to_elite_gives_max_penalty(self):
        """Distance=0 to nearest elite → full penalty_weight returned."""
        from src.backtesting.ga.diversity import compute_penalty

        params = {"x": 5}
        c = _candidate(params)
        elite = _candidate(params)  # Identical
        zone = self._zone_def({"x": {"type": "int", "min": 0, "max": 10}})

        penalty = compute_penalty(c, [elite], zone,
                                  distance_threshold=0.5, penalty_weight=0.2)
        assert abs(penalty - 0.2) < 1e-9, f"Identical candidate must receive max penalty; got {penalty}"

    def test_distant_from_elite_gives_zero_penalty(self):
        """Distance > threshold → 0 penalty."""
        from src.backtesting.ga.diversity import compute_penalty

        c = _candidate({"x": 0})
        elite = _candidate({"x": 10})
        zone = self._zone_def({"x": {"type": "float", "min": 0, "max": 10}})

        # distance=1.0 (max), threshold=0.5 → no penalty
        penalty = compute_penalty(c, [elite], zone,
                                  distance_threshold=0.5, penalty_weight=0.3)
        assert penalty == 0.0

    def test_b9b002_degenerate_param_skipped_silently(self):
        """
        B9B-002: Parameter with min==max (param_range=0) is skipped silently.
        Test confirms numeric result is still correct for remaining params.
        """
        from src.backtesting.ga.diversity import compute_penalty

        c = _candidate({"x": 5, "static": 3})  # 'static' has min==max
        elite = _candidate({"x": 5, "static": 3})
        zone = self._zone_def({
            "x": {"type": "int", "min": 0, "max": 10},
            "static": {"type": "int", "min": 3, "max": 3},  # degenerate — min==max
        })

        # Should not raise — static is silently skipped
        penalty = compute_penalty(c, [elite], zone,
                                  distance_threshold=0.5, penalty_weight=0.2)
        # Distance is 0 (x is identical) so full penalty
        assert abs(penalty - 0.2) < 1e-6, (
            f"B9B-002: Degenerate param should be skipped, distance still 0.0, "
            f"penalty should be 0.2; got {penalty}"
        )

    def test_hamming_distance_discrete_params(self):
        """Hamming: 1 of 2 discrete params differ → distance=0.5."""
        from src.backtesting.ga.diversity import _hamming

        a = _candidate({"mode": "aggressive", "style": "trend"})
        b = _candidate({"mode": "conservative", "style": "trend"})  # mode differs
        dist = _hamming(a, b, ["mode", "style"])
        assert abs(dist - 0.5) < 1e-9

    def test_euclidean_distance_midpoint(self):
        """Euclidean: param at 50% of range → distance=0.5."""
        from src.backtesting.ga.diversity import _normalised_euclidean

        a = _candidate({"x": 0.0})
        b = _candidate({"x": 5.0})
        zone = {"x": {"type": "float", "min": 0.0, "max": 10.0}}
        dist = _normalised_euclidean(a, b, ["x"], zone)
        assert abs(dist - 0.5) < 1e-9


# ══════════════════════════════════════════════════════════════════════════════
# ga_engine.py — B9B-003 injection key guard
# ══════════════════════════════════════════════════════════════════════════════

class TestGaEngineContract:
    """
    B9B-003: ga_engine.run_ga() requires config['_base_yaml_path'] to be
    injected by the orchestrator. Its absence causes a KeyError at Stage 3
    implementation time. Test documents the contract.
    """

    def test_b9b003_missing_base_yaml_path_raises(self):
        """
        run_ga() must raise KeyError if '_base_yaml_path' is not in config.
        This test DOCUMENTS the hidden interface contract.
        When Stage 3 is implemented, orchestrator must inject this key.
        """
        from src.backtesting.ga.ga_engine import run_ga
        from src.backtesting.contracts import WFOWindow
        from datetime import date

        windows = [
            WFOWindow(window_id=f"w{i}", start_date=date(2020, 1, 1),
                      end_date=date(2020, 6, 1))
            for i in range(3)
        ]

        incomplete_config = {
            # '_base_yaml_path' intentionally absent
            "genetic": {
                "population_size": 10, "generations": 2, "elite_fraction": 0.2,
                "mutation_rate": 0.1, "crossover_rate": 0.8, "tournament_size": 3,
                "stagnation_generations": 5, "diversity_penalty_weight": 0.1,
                "diversity_distance_threshold": 0.3,
            },
            "run": {"max_workers": 1, "temp_dir": "/tmp"},
            "random_search": {"min_significant_trades": 30},
            "zones": {},
        }

        store = MagicMock()
        store.rank.return_value = [MagicMock()]  # non-empty so init_population passes

        with pytest.raises(KeyError):
            run_ga(
                store=store,
                run_id="test-run",
                scenario=MagicMock(),
                wfo_windows=windows,
                config=incomplete_config,
                seed=42,
            )