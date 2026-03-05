"""
test_block9c_supporting.py
──────────────────────────
Block 9C audit tests for supporting modules:
  - ranker.py
  - scenario.py
  - wfo_engine.py
  - parameter_space.py
  - sampler.py
  - yaml_generator.py

Run:
    pytest tests/backtesting/integration/test_block9c_supporting.py -v

Import convention: sys.path FIRST, then project imports.
"""
from __future__ import annotations

import sys
from datetime import UTC, datetime, date
from pathlib import Path
from unittest.mock import MagicMock, patch, call
import pytest

# ── 1. sys.path anchor (MUST be first) ────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# ── 2. Contracts first ────────────────────────────────────────────────────────
from src.backtesting.contracts import (
    CandidateParameterSet,
    CandidateRecord,
    CandidateStage,
    ScenarioProfile,
    WFOConsistencyScore,
    WFOWindow,
    WFOWindowResult,
    RunMetadata,
    VerdictResult,
    Verdict,
    DeploymentStatus,
)

# ── 3. Modules under test ─────────────────────────────────────────────────────
from src.backtesting.ranker import rank, rank_by_wfo, rank_combined
from src.backtesting.scenario import load_scenario
from src.backtesting.parameter_space import expand_zones, validate_combination, _range_values
from src.backtesting.sampler import sample_lhs, sample_random, _lhs_sample
from src.backtesting.yaml_generator import generate_trading_yaml, build_output_path
from src.backtesting.wfo.wfo_engine import run_wfo


# ══════════════════════════════════════════════════════════════════════════════
# Shared fixtures
# ══════════════════════════════════════════════════════════════════════════════

def _make_scenario_config(name: str = "balanced") -> dict:
    """Minimal valid config structure for load_scenario()."""
    return {
        "scenario": name,
        "scenarios": {
            name: {
                "description": "Test scenario",
                "fitness_weights": {
                    "net_pnl": 0.30,
                    "expectancy": 0.25,
                    "max_drawdown": 0.20,
                    "win_rate": 0.10,
                    "trade_frequency": 0.05,
                    "profit_factor": 0.10,
                },
                "constraints": {
                    "min_win_rate": 0.45,
                    "max_drawdown": 0.20,
                    "max_losing_streak": 8,
                    "min_trades_per_week": 1.0,
                    "min_expectancy": 0.0,
                    "min_profit_factor": 1.0,
                },
                "mc_prefilter_ruin_threshold": 0.05,
                "wfo_temporal_weights": {
                    "median_return": 0.40,
                    "variance": 0.30,
                    "worst_drawdown": 0.20,
                    "fraction_positive": 0.10,
                },
                "verdict_thresholds": {
                    "go_wfo_floor": 0.65,
                    "borderline_wfo_floor": 0.50,
                    "go_mc_ruin_ceiling": 0.05,
                    "borderline_mc_ruin_ceiling": 0.10,
                    "sensitivity_spike_threshold": 0.30,
                },
                "report_emphasis": ["wfo", "mc"],
            }
        },
    }


def _make_candidate(zone: str = "zone_a", params: dict | None = None) -> CandidateParameterSet:
    if params is None:
        params = {"rsi_period": 14, "adx_threshold": 25}
    return CandidateParameterSet.create(zone_name=zone, parameters=params, generation=None)


def _make_candidate_record(
    candidate: CandidateParameterSet,
    fitness: float = 0.75,
    passed: bool = True,
    wfo_score: float | None = None,
) -> CandidateRecord:
    import json
    from datetime import timezone
    return CandidateRecord(
        run_id="run_test",
        candidate_id=candidate.candidate_id,
        zone_name=candidate.zone_name,
        stage=CandidateStage.RANDOM.value,          # stage is str (.value), not enum
        generation=candidate.generation,
        recorded_at=datetime.now(timezone.utc),
        parameters_json=json.dumps(candidate.parameters, sort_keys=True, default=str),
        fitness_score=fitness,
        passed_constraints=passed,
        rejection_reason=None,
        failing_constraint=None,
        failing_value=None,
        actual_win_rate=None,
        actual_max_drawdown=None,
        actual_losing_streak=None,
        actual_trades_per_week=None,
        actual_expectancy=None,
        actual_profit_factor=None,
        wfo_median_window_return=None,
        wfo_window_return_variance=None,
        wfo_worst_window_drawdown=None,
        wfo_fraction_positive_windows=None,
        wfo_consistency_score=wfo_score,
        wfo_windows_evaluated=None,
        wfo_oos_gate_triggered=None,
        wfo_window_collapse_flag=None,
        wfo_median_oos_delta=None,
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


def _make_zone_config() -> dict:
    return {
        "zones": {
            "zone_a": {
                "enabled": True,
                "parameters": {
                    "rsi_period": {"type": "int", "min": 10, "max": 14, "step": 2},
                    "adx_threshold": {"type": "float", "min": 20.0, "max": 25.0, "step": 2.5},
                    "session": {"type": "choice", "choices": ["london", "ny"]},
                },
            },
        }
    }


# ══════════════════════════════════════════════════════════════════════════════
# ranker.py — 7 tests
# ══════════════════════════════════════════════════════════════════════════════

class TestRanker:

    def _make_store(self, records: list) -> MagicMock:
        store = MagicMock()
        store.query_candidates.return_value = records
        return store

    def test_rank_returns_typed_candidate_records(self):
        """rank() must return List[CandidateRecord], not raw dicts."""
        c = _make_candidate()
        rec = _make_candidate_record(c, fitness=0.80, passed=True)
        store = self._make_store([rec])
        result = rank(store, run_id="run1", stage=CandidateStage.RANDOM, top_n=5)
        assert isinstance(result, list)
        assert all(isinstance(r, CandidateRecord) for r in result)

    def test_rank_filters_failed_constraints(self):
        """rank() must exclude records where passed_constraints is not True."""
        c1 = _make_candidate(params={"rsi_period": 14, "adx_threshold": 25})
        c2 = _make_candidate(params={"rsi_period": 10, "adx_threshold": 20})
        r1 = _make_candidate_record(c1, fitness=0.90, passed=True)
        r2 = _make_candidate_record(c2, fitness=0.95, passed=False)
        store = self._make_store([r1, r2])
        result = rank(store, run_id="run1", stage=CandidateStage.RANDOM, top_n=5)
        assert len(result) == 1
        assert result[0].candidate_id == r1.candidate_id

    def test_rank_respects_top_n(self):
        """rank() must truncate to top_n."""
        candidates = [_make_candidate(params={"rsi_period": i, "adx_threshold": 25}) for i in range(10, 20)]
        records = [_make_candidate_record(c, fitness=float(i), passed=True) for i, c in enumerate(candidates)]
        store = self._make_store(records)
        result = rank(store, run_id="run1", stage=CandidateStage.RANDOM, top_n=3)
        assert len(result) == 3

    def test_rank_by_wfo_returns_typed_records(self):
        """rank_by_wfo() must return List[CandidateRecord] — resolves B9A-001 at ranker level."""
        c = _make_candidate()
        rec = _make_candidate_record(c, wfo_score=0.72)
        store = self._make_store([rec])
        result = rank_by_wfo(store, run_id="run1", top_n=5)
        assert isinstance(result, list)
        assert all(isinstance(r, CandidateRecord) for r in result)

    def test_rank_by_wfo_excludes_records_without_wfo_score(self):
        """rank_by_wfo() must exclude records where wfo_consistency_score is None."""
        c1 = _make_candidate(params={"rsi_period": 14, "adx_threshold": 25})
        c2 = _make_candidate(params={"rsi_period": 10, "adx_threshold": 20})
        r1 = _make_candidate_record(c1, wfo_score=0.70)
        r2 = _make_candidate_record(c2, wfo_score=None)
        store = self._make_store([r1, r2])
        result = rank_by_wfo(store, run_id="run1", top_n=5)
        assert len(result) == 1
        assert result[0].wfo_consistency_score == 0.70

    def test_rank_combined_deduplicates_by_candidate_id(self):
        """rank_combined() must not return the same candidate_id twice."""
        c = _make_candidate()
        rec = _make_candidate_record(c, fitness=0.80, passed=True)
        store = self._make_store([rec, rec])  # same record returned from both stages
        result = rank_combined(
            store,
            run_id="run1",
            stages=[CandidateStage.RANDOM, CandidateStage.GA],
            top_n=10,
        )
        ids = [r.candidate_id for r in result]
        assert len(ids) == len(set(ids)), "Duplicate candidate_ids in rank_combined result"

    def test_rank_combined_sorts_by_fitness_descending(self):
        """rank_combined() result must be ordered by fitness_score descending."""
        c1 = _make_candidate(params={"rsi_period": 14, "adx_threshold": 25})
        c2 = _make_candidate(params={"rsi_period": 10, "adx_threshold": 20})
        r1 = _make_candidate_record(c1, fitness=0.60, passed=True)
        r2 = _make_candidate_record(c2, fitness=0.90, passed=True)
        store = self._make_store([r1, r2])
        result = rank_combined(
            store, run_id="run1",
            stages=[CandidateStage.RANDOM],
            top_n=10,
        )
        scores = [r.fitness_score for r in result]
        assert scores == sorted(scores, reverse=True)


# ══════════════════════════════════════════════════════════════════════════════
# scenario.py — 6 tests
# ══════════════════════════════════════════════════════════════════════════════

class TestScenario:

    def test_load_scenario_returns_scenario_profile(self):
        config = _make_scenario_config()
        profile = load_scenario(config)
        assert isinstance(profile, ScenarioProfile)

    def test_load_scenario_correct_name(self):
        config = _make_scenario_config("balanced")
        profile = load_scenario(config)
        assert profile.name == "balanced"

    def test_load_scenario_missing_scenario_key_raises(self):
        config = _make_scenario_config()
        del config["scenario"]
        with pytest.raises(ValueError, match="config\\['scenario'\\]"):
            load_scenario(config)

    def test_load_scenario_unknown_scenario_name_raises(self):
        config = _make_scenario_config()
        config["scenario"] = "nonexistent_scenario"
        with pytest.raises(ValueError, match="Unknown scenario"):
            load_scenario(config)

    def test_load_scenario_spike_threshold_loaded(self):
        """verdict_sensitivity_spike_threshold must be loaded from verdict_thresholds."""
        config = _make_scenario_config()
        profile = load_scenario(config)
        assert profile.verdict_sensitivity_spike_threshold == pytest.approx(0.30)

    def test_load_scenario_fitness_weights_sum_to_one(self):
        """ScenarioProfile validates weight sum — must not raise for valid config."""
        config = _make_scenario_config()
        # Should not raise
        profile = load_scenario(config)
        total = (
            profile.weight_net_pnl + profile.weight_expectancy +
            profile.weight_max_drawdown + profile.weight_win_rate +
            profile.weight_trade_frequency + profile.weight_profit_factor
        )
        assert total == pytest.approx(1.0, abs=1e-6)


# ══════════════════════════════════════════════════════════════════════════════
# parameter_space.py — 8 tests
# ══════════════════════════════════════════════════════════════════════════════

class TestParameterSpace:

    def test_expand_zones_int_range(self):
        """Int params: values generated from min to max inclusive at step."""
        config = {
            "zones": {
                "z": {
                    "enabled": True,
                    "parameters": {
                        "rsi": {"type": "int", "min": 10, "max": 14, "step": 2}
                    },
                }
            }
        }
        result = expand_zones(config)
        assert "z" in result
        rsi_values = [combo["rsi"] for combo in result["z"]]
        assert sorted(rsi_values) == [10, 12, 14]

    def test_expand_zones_float_range(self):
        """Float params: values generated with fp-safe arithmetic."""
        config = {
            "zones": {
                "z": {
                    "enabled": True,
                    "parameters": {
                        "atr": {"type": "float", "min": 1.0, "max": 1.2, "step": 0.1}
                    },
                }
            }
        }
        result = expand_zones(config)
        values = sorted([combo["atr"] for combo in result["z"]])
        assert len(values) == 3
        assert values[0] == pytest.approx(1.0)
        assert values[1] == pytest.approx(1.1)
        assert values[2] == pytest.approx(1.2)

    def test_expand_zones_choice(self):
        """Choice params: all choices appear in output."""
        config = {
            "zones": {
                "z": {
                    "enabled": True,
                    "parameters": {
                        "session": {"type": "choice", "choices": ["london", "ny", "asia"]}
                    },
                }
            }
        }
        result = expand_zones(config)
        sessions = {combo["session"] for combo in result["z"]}
        assert sessions == {"london", "ny", "asia"}

    def test_expand_zones_cartesian_product(self):
        """Cartesian product: combination count = product of individual value counts."""
        config = {
            "zones": {
                "z": {
                    "enabled": True,
                    "parameters": {
                        "rsi": {"type": "int", "min": 10, "max": 14, "step": 2},  # 3 values
                        "session": {"type": "choice", "choices": ["london", "ny"]},  # 2 values
                    },
                }
            }
        }
        result = expand_zones(config)
        assert len(result["z"]) == 6  # 3 × 2

    def test_expand_zones_disabled_zone_excluded(self):
        """Disabled zones must not appear in output."""
        config = {
            "zones": {
                "active": {
                    "enabled": True,
                    "parameters": {"rsi": {"type": "int", "min": 10, "max": 12, "step": 2}},
                },
                "inactive": {
                    "enabled": False,
                    "parameters": {"rsi": {"type": "int", "min": 20, "max": 22, "step": 2}},
                },
            }
        }
        result = expand_zones(config)
        assert "active" in result
        assert "inactive" not in result

    def test_expand_zones_missing_zones_key_raises(self):
        with pytest.raises(ValueError, match="config\\['zones'\\]"):
            expand_zones({})

    def test_expand_zones_empty_range_raises(self):
        """min > max produces empty range — must raise ValueError."""
        config = {
            "zones": {
                "z": {
                    "enabled": True,
                    "parameters": {
                        "rsi": {"type": "int", "min": 20, "max": 10, "step": 2}
                    },
                }
            }
        }
        with pytest.raises(ValueError, match="empty range"):
            expand_zones(config)

    def test_validate_combination_in_range(self):
        """validate_combination returns True for in-range int/float values."""
        zone_def = {
            "parameters": {
                "rsi": {"type": "int", "min": 10, "max": 20, "step": 2},
                "session": {"type": "choice", "choices": ["london", "ny"]},
            }
        }
        assert validate_combination({"rsi": 14, "session": "london"}, zone_def) is True
        assert validate_combination({"rsi": 25, "session": "london"}, zone_def) is False
        assert validate_combination({"rsi": 14, "session": "tokyo"}, zone_def) is False


# ══════════════════════════════════════════════════════════════════════════════
# sampler.py — 7 tests
# ══════════════════════════════════════════════════════════════════════════════

class TestSampler:

    def _make_expanded_space(self) -> dict:
        config = _make_zone_config()
        return expand_zones(config)

    def test_sample_lhs_returns_candidate_parameter_sets(self):
        space = self._make_expanded_space()
        results = sample_lhs(space, n_per_zone=4, seed=42)
        assert all(isinstance(r, CandidateParameterSet) for r in results)

    def test_sample_lhs_seed_deterministic(self):
        """Same seed must produce identical results."""
        space = self._make_expanded_space()
        r1 = sample_lhs(space, n_per_zone=4, seed=42)
        r2 = sample_lhs(space, n_per_zone=4, seed=42)
        assert [c.parameters for c in r1] == [c.parameters for c in r2]

    def test_sample_lhs_different_seeds_differ(self):
        """Different seeds must (almost certainly) produce different results."""
        space = self._make_expanded_space()
        r1 = sample_lhs(space, n_per_zone=4, seed=1)
        r2 = sample_lhs(space, n_per_zone=4, seed=999)
        params1 = [c.parameters for c in r1]
        params2 = [c.parameters for c in r2]
        # With a real parameter space this will differ; protect against trivially equal spaces
        if len(space[list(space.keys())[0]]) > 4:
            assert params1 != params2

    def test_sample_lhs_respects_n_per_zone_cap(self):
        """n_per_zone must be capped at zone's actual combination count."""
        space = self._make_expanded_space()
        total_combos = sum(len(v) for v in space.values())
        results = sample_lhs(space, n_per_zone=total_combos + 100, seed=42)
        assert len(results) <= total_combos

    def test_sample_random_returns_candidate_parameter_sets(self):
        space = self._make_expanded_space()
        results = sample_random(space, n_per_zone=4, seed=42)
        assert all(isinstance(r, CandidateParameterSet) for r in results)

    def test_sample_random_no_duplicates_within_zone(self):
        """sample_random uses rng.sample (without replacement) — no duplicate combos per zone."""
        space = self._make_expanded_space()
        results = sample_random(space, n_per_zone=6, seed=42)
        # Group by zone and check uniqueness within each zone
        from collections import defaultdict
        by_zone: dict = defaultdict(list)
        for r in results:
            by_zone[r.zone_name].append(r.parameters)
        for zone, params_list in by_zone.items():
            tuples = [tuple(sorted(p.items())) for p in params_list]
            assert len(tuples) == len(set(tuples)), f"Duplicate params in zone {zone}"

    def test_lhs_sample_internal_numeric_sort_preserves_space_filling(self):
        """
        B9C-012: _lhs_sample sorts values by (str(type), str(val)) which is
        lexicographic for numerics. Verify the actual sampled values span
        the parameter range (not clumped at one end due to sort order).
        """
        import random
        rng = random.Random(42)
        combos = [{"rsi": i} for i in range(10, 25, 2)]  # [10, 12, 14, 16, 18, 20, 22, 24]
        sampled = _lhs_sample(combos, n=4, rng=rng)
        values = sorted([c["rsi"] for c in sampled])
        # With correct numeric sort, strata cover [10-12], [14-16], [18-20], [22-24]
        # Minimum expected spread: first value ≤ 16, last value ≥ 18
        assert values[0] <= 16, f"LHS undersampling low range: {values}"
        assert values[-1] >= 18, f"LHS undersampling high range: {values}"


# ══════════════════════════════════════════════════════════════════════════════
# yaml_generator.py — 7 tests
# ══════════════════════════════════════════════════════════════════════════════

class TestYamlGenerator:

    def _make_base_yaml(self, tmp_path: Path) -> Path:
        import yaml
        base = {
            "strategy": {"timeframe": "15m", "htf_timeframe": "1h"},
            "parameters": {"rsi_period": 14, "adx_threshold": 20},
            "filters": {},
        }
        p = tmp_path / "base_strategy.yaml"
        with p.open("w") as f:
            yaml.dump(base, f)
        return p

    def _make_run_metadata(self) -> RunMetadata:
        from datetime import timezone
        from src.backtesting.contracts import Checkpoint
        return RunMetadata(
            run_id="run_abc12345",
            config_hash="a" * 64,                      # 64-char SHA-256 hex digest
            scenario_name="balanced",
            started_at=datetime.now(timezone.utc),
            perturbation_profile_name="default",
            random_search_seed=1,
            ga_seed=2,
            mc_prefilter_seed=3,
            mc_deep_seed=4,
            sensitivity_seed=5,
            wfo_window_ids=("w0", "w1", "w2"),          # minimum 3 required
            checkpoint=Checkpoint.NOT_STARTED,
            backtester_version="0.9.0",
        )

    def _make_verdict(self, candidate: CandidateParameterSet) -> VerdictResult:
        return VerdictResult(
            candidate_id=candidate.candidate_id,
            scenario_name="balanced",
            verdict=Verdict.AUTO_GO,
            deployment_status=DeploymentStatus.PAPER_TRADE_REQUIRED,
            wfo_consistency_score=0.72,
            mc_deep_ruin_probability=0.03,
            sensitivity_spike=False,
            oos_gate_triggered=False,
            window_collapse_flag=False,
            sensitivity_profile_incomplete=False,
            median_oos_delta=None,
            parameter_region_width=None,
            yaml_output_path=None,
            evidence_summary="Test verdict",
        )

    def test_generate_trading_yaml_writes_file(self, tmp_path):
        base_yaml = self._make_base_yaml(tmp_path)
        c = _make_candidate()
        output_path = build_output_path(tmp_path, "run_abc12345", c.candidate_id)
        generate_trading_yaml(c, self._make_verdict(c), self._make_run_metadata(), base_yaml, output_path)
        assert output_path.exists()

    def test_generate_trading_yaml_embeds_metadata(self, tmp_path):
        import yaml
        base_yaml = self._make_base_yaml(tmp_path)
        c = _make_candidate()
        output_path = build_output_path(tmp_path, "run_abc12345", c.candidate_id)
        generate_trading_yaml(c, self._make_verdict(c), self._make_run_metadata(), base_yaml, output_path)
        with output_path.open() as f:
            out = yaml.safe_load(f)
        assert "backtester_metadata" in out
        meta = out["backtester_metadata"]
        assert meta["run_id"] == "run_abc12345"
        assert meta["candidate_id"] == c.candidate_id
        assert meta["deployment_status"] == DeploymentStatus.PAPER_TRADE_REQUIRED.value

    def test_generate_trading_yaml_always_paper_trade_status(self, tmp_path):
        """deployment_status must always be PAPER_TRADE_REQUIRED — never LIVE_APPROVED."""
        import yaml
        base_yaml = self._make_base_yaml(tmp_path)
        c = _make_candidate()
        output_path = build_output_path(tmp_path, "run_abc12345", c.candidate_id)
        generate_trading_yaml(c, self._make_verdict(c), self._make_run_metadata(), base_yaml, output_path)
        with output_path.open() as f:
            out = yaml.safe_load(f)
        assert out["backtester_metadata"]["deployment_status"] == DeploymentStatus.PAPER_TRADE_REQUIRED.value
        assert out["backtester_metadata"]["deployment_status"] != DeploymentStatus.LIVE_APPROVED.value

    def test_generate_trading_yaml_merges_candidate_params(self, tmp_path):
        """Known mapped params must be written to correct YAML sections."""
        import yaml
        base_yaml = self._make_base_yaml(tmp_path)
        c = _make_candidate(params={"rsi_period": 20, "adx_threshold": 30})
        output_path = build_output_path(tmp_path, "run_abc12345", c.candidate_id)
        generate_trading_yaml(c, self._make_verdict(c), self._make_run_metadata(), base_yaml, output_path)
        with output_path.open() as f:
            out = yaml.safe_load(f)
        assert out["parameters"]["rsi_period"] == 20
        assert out["parameters"]["adx_threshold"] == 30

    def test_generate_trading_yaml_missing_base_raises(self, tmp_path):
        c = _make_candidate()
        output_path = build_output_path(tmp_path, "run_abc12345", c.candidate_id)
        with pytest.raises(FileNotFoundError):
            generate_trading_yaml(
                c, self._make_verdict(c), self._make_run_metadata(),
                tmp_path / "nonexistent.yaml",
                output_path,
            )

    def test_generate_trading_yaml_embeds_seeds(self, tmp_path):
        """All run seeds must be present in backtester_metadata for audit trail."""
        import yaml
        base_yaml = self._make_base_yaml(tmp_path)
        c = _make_candidate()
        output_path = build_output_path(tmp_path, "run_abc12345", c.candidate_id)
        generate_trading_yaml(c, self._make_verdict(c), self._make_run_metadata(), base_yaml, output_path)
        with output_path.open() as f:
            out = yaml.safe_load(f)
        meta = out["backtester_metadata"]
        for seed_key in ("random_search_seed", "ga_seed", "mc_prefilter_seed", "mc_deep_seed", "sensitivity_seed"):
            assert seed_key in meta, f"Missing seed key: {seed_key}"

    def test_build_output_path_canonical_format(self, tmp_path):
        """Output path must follow spec: trading_yamls/{run_id[:8]}_{cid[:12]}_strategy.yaml"""
        run_id = "run_abc12345xyz"
        cid = "cand_abcdefghijklmno"
        path = build_output_path(tmp_path, run_id, cid)
        assert path.parent.name == "trading_yamls"
        assert path.name == f"{run_id[:8]}_{cid[:12]}_strategy.yaml"


# ══════════════════════════════════════════════════════════════════════════════
# wfo_engine.py — 7 tests
# ══════════════════════════════════════════════════════════════════════════════

class TestWfoEngine:

    def _make_wfo_window(self, idx: int = 0) -> WFOWindow:
        from datetime import date
        return WFOWindow(
            window_id=f"w{idx}",
            start_date=date(2023, 1, 1),
            end_date=date(2023, 12, 31),
        )

    def _make_window_result(self, candidate_id: str, window_id: str) -> WFOWindowResult:
        from datetime import timezone
        return WFOWindowResult(
            candidate_id=candidate_id,
            window_id=window_id,
            evaluated_at=datetime.now(timezone.utc),
            fitness_score=0.72,
            total_trades=50,
            net_pnl=80.0,
            max_drawdown=0.10,
            win_rate=0.55,
            expectancy=0.5,
            profit_factor=1.3,
            oos_delta=None,
        )

    def _make_scenario(self) -> ScenarioProfile:
        return load_scenario(_make_scenario_config())

    def _make_store(self) -> MagicMock:
        store = MagicMock()
        return store

    def test_run_wfo_invalid_mode_raises(self):
        store = self._make_store()
        c = _make_candidate()
        scenario = self._make_scenario()
        with pytest.raises(ValueError, match="WFO mode must be"):
            run_wfo(
                candidates=[c],
                windows=[self._make_wfo_window()],
                store=store,
                run_id="run1",
                scenario=scenario,
                base_yaml_path=Path("base.yaml"),
                temp_dir=Path("/tmp"),
                mode="invalid_mode",
            )

    def test_run_wfo_returns_dict_keyed_by_candidate_id(self):
        store = self._make_store()
        c = _make_candidate()
        scenario = self._make_scenario()
        window = self._make_wfo_window()
        window_result = self._make_window_result(c.candidate_id, window.window_id)

        with patch("src.backtesting.wfo.wfo_engine.evaluate_window", return_value=window_result), \
             patch("src.backtesting.wfo.wfo_engine.compute_consistency") as mock_cc:
            mock_cc.return_value = WFOConsistencyScore(
                candidate_id=c.candidate_id,
                windows_evaluated=1,
                windows_total=1,
                median_window_return=50.0,
                window_return_variance=10.0,
                worst_window_drawdown=0.05,
                fraction_positive_windows=1.0,
                composite_score=0.70,
                oos_gate_triggered=False,
                window_collapse_flag=False,
            )
            result = run_wfo(
                candidates=[c],
                windows=[window],
                store=store,
                run_id="run1",
                scenario=scenario,
                base_yaml_path=Path("base.yaml"),
                temp_dir=Path("/tmp"),
                mode="lightweight",
            )

        assert c.candidate_id in result
        assert isinstance(result[c.candidate_id], WFOConsistencyScore)

    def test_run_wfo_full_mode_writes_consistency_score(self):
        """Full mode must write WFOConsistencyScore to store."""
        store = self._make_store()
        c = _make_candidate()
        scenario = self._make_scenario()
        window = self._make_wfo_window()
        window_result = self._make_window_result(c.candidate_id, window.window_id)
        consistency = WFOConsistencyScore(
                candidate_id=c.candidate_id,
                windows_evaluated=1,
                windows_total=1,
                median_window_return=50.0,
                window_return_variance=10.0,
                worst_window_drawdown=0.05,
                fraction_positive_windows=1.0,
                composite_score=0.70,
                oos_gate_triggered=False,
                window_collapse_flag=False,
            )

        with patch("src.backtesting.wfo.wfo_engine.evaluate_window", return_value=window_result), \
             patch("src.backtesting.wfo.wfo_engine.compute_consistency", return_value=consistency):
            run_wfo(
                candidates=[c],
                windows=[window],
                store=store,
                run_id="run1",
                scenario=scenario,
                base_yaml_path=Path("base.yaml"),
                temp_dir=Path("/tmp"),
                mode="full",
            )

        store.write_wfo_consistency_score.assert_called_once_with(consistency, "run1")

    def test_run_wfo_lightweight_does_not_write_consistency_score(self):
        """Lightweight mode must NOT write consistency score to store."""
        store = self._make_store()
        c = _make_candidate()
        scenario = self._make_scenario()
        window = self._make_wfo_window()
        window_result = self._make_window_result(c.candidate_id, window.window_id)
        consistency = WFOConsistencyScore(
                candidate_id=c.candidate_id,
                windows_evaluated=1,
                windows_total=1,
                median_window_return=40.0,
                window_return_variance=5.0,
                worst_window_drawdown=0.08,
                fraction_positive_windows=1.0,
                composite_score=0.65,
                oos_gate_triggered=False,
                window_collapse_flag=False,
            )

        with patch("src.backtesting.wfo.wfo_engine.evaluate_window", return_value=window_result), \
             patch("src.backtesting.wfo.wfo_engine.compute_consistency", return_value=consistency):
            run_wfo(
                candidates=[c],
                windows=[window],
                store=store,
                run_id="run1",
                scenario=scenario,
                base_yaml_path=Path("base.yaml"),
                temp_dir=Path("/tmp"),
                mode="lightweight",
            )

        store.write_wfo_consistency_score.assert_not_called()

    def test_run_wfo_writes_window_result_to_store(self):
        """Every window result must be written to store for audit queryability."""
        from concurrent.futures import Future
        store = self._make_store()
        c = _make_candidate()
        scenario = self._make_scenario()
        window = self._make_wfo_window()
        window_result = self._make_window_result(c.candidate_id, window.window_id)
        consistency = WFOConsistencyScore(
                candidate_id=c.candidate_id,
                windows_evaluated=1,
                windows_total=1,
                median_window_return=45.0,
                window_return_variance=8.0,
                worst_window_drawdown=0.06,
                fraction_positive_windows=1.0,
                composite_score=0.68,
                oos_gate_triggered=False,
                window_collapse_flag=False,
            )

        # ProcessPoolExecutor crosses spawn boundary — mock the executor so the
        # future resolves synchronously and write_wfo_window_result is called.
        future = Future()
        future.set_result(window_result)

        mock_pool = MagicMock()
        mock_pool.__enter__ = MagicMock(return_value=mock_pool)
        mock_pool.__exit__ = MagicMock(return_value=False)
        mock_pool.submit.return_value = future

        with patch("src.backtesting.wfo.wfo_engine.ProcessPoolExecutor", return_value=mock_pool), \
             patch("src.backtesting.wfo.wfo_engine.as_completed", return_value=[future]), \
             patch("src.backtesting.wfo.wfo_engine.compute_consistency", return_value=consistency):
            run_wfo(
                candidates=[c],
                windows=[window],
                store=store,
                run_id="run1",
                scenario=scenario,
                base_yaml_path=Path("base.yaml"),
                temp_dir=Path("/tmp"),
                mode="lightweight",
            )

        store.write_wfo_window_result.assert_called_once_with(window_result, "run1")

    def test_run_wfo_worker_exception_is_handled_gracefully(self):
        """If a worker raises, the candidate still gets scored (with empty window results)."""
        store = self._make_store()
        c = _make_candidate()
        scenario = self._make_scenario()
        window = self._make_wfo_window()
        consistency = WFOConsistencyScore(
                candidate_id=c.candidate_id,
                windows_evaluated=0,
                windows_total=1,
                median_window_return=0.0,
                window_return_variance=0.0,
                worst_window_drawdown=0.0,
                fraction_positive_windows=0.0,
                composite_score=0.0,
                oos_gate_triggered=False,
                window_collapse_flag=False,
            )

        with patch("src.backtesting.wfo.wfo_engine.evaluate_window", side_effect=RuntimeError("worker crash")), \
             patch("src.backtesting.wfo.wfo_engine.compute_consistency", return_value=consistency):
            result = run_wfo(
                candidates=[c],
                windows=[window],
                store=store,
                run_id="run1",
                scenario=scenario,
                base_yaml_path=Path("base.yaml"),
                temp_dir=Path("/tmp"),
                mode="lightweight",
            )

        # Should not raise; candidate still scored
        assert c.candidate_id in result

    def test_run_wfo_accepted_modes(self):
        """Both 'full' and 'lightweight' are valid modes — neither raises ValueError."""
        store = self._make_store()
        c = _make_candidate()
        scenario = self._make_scenario()
        window = self._make_wfo_window()
        window_result = self._make_window_result(c.candidate_id, window.window_id)
        consistency = WFOConsistencyScore(
                candidate_id=c.candidate_id,
                windows_evaluated=1,
                windows_total=1,
                median_window_return=30.0,
                window_return_variance=3.0,
                worst_window_drawdown=0.04,
                fraction_positive_windows=1.0,
                composite_score=0.60,
                oos_gate_triggered=False,
                window_collapse_flag=False,
            )

        for mode in ("full", "lightweight"):
            with patch("src.backtesting.wfo.wfo_engine.evaluate_window", return_value=window_result), \
                 patch("src.backtesting.wfo.wfo_engine.compute_consistency", return_value=consistency):
                # Should not raise
                run_wfo(
                    candidates=[c],
                    windows=[window],
                    store=store,
                    run_id="run1",
                    scenario=scenario,
                    base_yaml_path=Path("base.yaml"),
                    temp_dir=Path("/tmp"),
                    mode=mode,
                )