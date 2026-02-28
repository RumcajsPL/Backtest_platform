"""
Unit tests for parameter_space.py and sampler.py.
"""
from __future__ import annotations

import unittest

from src.backtesting.parameter_space import expand_zones, validate_combination
from src.backtesting.sampler import sample_lhs, sample_random


# ── Shared fixture ────────────────────────────────────────────────────────────

_SAFE_ZONE_CONFIG = {
    "zones": {
        "safe": {
            "enabled": True,
            "parameters": {
                "rsi_period":     {"type": "int",    "min": 10, "max": 20, "step": 2},
                "atr_multiplier": {"type": "float",  "min": 1.5, "max": 2.5, "step": 0.5},
                "session_filter": {"type": "choice", "choices": ["london", "new_york"]},
            },
        },
        "disabled_zone": {
            "enabled": False,
            "parameters": {
                "rsi_period": {"type": "int", "min": 5, "max": 30, "step": 1},
            },
        },
    }
}

# rsi_period: 10,12,14,16,18,20 (6 values)
# atr_multiplier: 1.5,2.0,2.5 (3 values)
# session_filter: london, new_york (2 values)
# Total safe zone: 6 × 3 × 2 = 36 combinations
_EXPECTED_SAFE_COMBOS = 36


class TestExpandZones(unittest.TestCase):
    def test_expand_zones_safe(self):
        """Expands safe zone; all values within defined bounds."""
        result = expand_zones(_SAFE_ZONE_CONFIG)
        self.assertIn("safe", result)
        self.assertEqual(len(result["safe"]), _EXPECTED_SAFE_COMBOS)

        for combo in result["safe"]:
            self.assertIn(combo["rsi_period"], [10, 12, 14, 16, 18, 20])
            self.assertIn(combo["atr_multiplier"], [1.5, 2.0, 2.5])
            self.assertIn(combo["session_filter"], ["london", "new_york"])

    def test_expand_zones_disabled(self):
        """Disabled zones produce no output."""
        result = expand_zones(_SAFE_ZONE_CONFIG)
        self.assertNotIn("disabled_zone", result)

    def test_expand_zones_int_values(self):
        """Int parameter values are Python ints."""
        result = expand_zones(_SAFE_ZONE_CONFIG)
        for combo in result["safe"]:
            self.assertIsInstance(combo["rsi_period"], int)

    def test_expand_zones_float_values(self):
        """Float parameter values are Python floats."""
        result = expand_zones(_SAFE_ZONE_CONFIG)
        for combo in result["safe"]:
            self.assertIsInstance(combo["atr_multiplier"], float)

    def test_expand_zones_raises_on_unknown_type(self):
        cfg = {"zones": {"z": {"enabled": True, "parameters": {"p": {"type": "unknown"}}}}}
        with self.assertRaises(ValueError):
            expand_zones(cfg)

    def test_expand_zones_raises_on_missing_zones(self):
        with self.assertRaises(ValueError):
            expand_zones({})

    def test_float_step_boundary(self):
        """Float step 0.25 should not miss the upper boundary due to fp drift."""
        cfg = {"zones": {"z": {"enabled": True, "parameters": {
            "atr_multiplier": {"type": "float", "min": 1.5, "max": 2.5, "step": 0.25},
        }}}}
        result = expand_zones(cfg)
        values = [c["atr_multiplier"] for c in result["z"]]
        self.assertIn(2.5, values, f"Upper boundary 2.5 missing; got {values}")
        self.assertIn(1.5, values)


class TestValidateCombination(unittest.TestCase):
    def _zone_def(self):
        return _SAFE_ZONE_CONFIG["zones"]["safe"]

    def test_valid_combination(self):
        params = {"rsi_period": 14, "atr_multiplier": 2.0, "session_filter": "london"}
        self.assertTrue(validate_combination(params, self._zone_def()))

    def test_out_of_range_int(self):
        params = {"rsi_period": 99, "atr_multiplier": 2.0, "session_filter": "london"}
        self.assertFalse(validate_combination(params, self._zone_def()))

    def test_invalid_choice(self):
        params = {"rsi_period": 14, "atr_multiplier": 2.0, "session_filter": "tokyo"}
        self.assertFalse(validate_combination(params, self._zone_def()))

    def test_unknown_parameter(self):
        params = {"unknown_param": 42}
        self.assertFalse(validate_combination(params, self._zone_def()))


class TestSampler(unittest.TestCase):
    def setUp(self):
        self.expanded = expand_zones(_SAFE_ZONE_CONFIG)

    def test_lhs_correct_count(self):
        samples = sample_lhs(self.expanded, n_per_zone=20, seed=42)
        self.assertEqual(len(samples), 20)

    def test_lhs_no_duplicates(self):
        """200 LHS samples from safe zone — no duplicate candidate_ids."""
        samples = sample_lhs(self.expanded, n_per_zone=36, seed=42)
        ids = [s.candidate_id for s in samples]
        self.assertEqual(len(ids), len(set(ids)), "Duplicate candidate_ids found")

    def test_lhs_covers_range(self):
        """Sampled values fall within zone bounds."""
        samples = sample_lhs(self.expanded, n_per_zone=36, seed=42)
        for s in samples:
            p = s.parameters
            self.assertGreaterEqual(p["rsi_period"], 10)
            self.assertLessEqual(p["rsi_period"], 20)
            self.assertGreaterEqual(p["atr_multiplier"], 1.5)
            self.assertLessEqual(p["atr_multiplier"], 2.5)
            self.assertIn(p["session_filter"], ["london", "new_york"])

    def test_lhs_respects_zone_name(self):
        samples = sample_lhs(self.expanded, n_per_zone=10, seed=42)
        for s in samples:
            self.assertEqual(s.zone_name, "safe")

    def test_lhs_generation_is_none(self):
        """LHS samples have generation=None (Random Search)."""
        samples = sample_lhs(self.expanded, n_per_zone=5, seed=42)
        for s in samples:
            self.assertIsNone(s.generation)

    def test_random_sample_count(self):
        samples = sample_random(self.expanded, n_per_zone=15, seed=99)
        self.assertEqual(len(samples), 15)

    def test_random_no_duplicates(self):
        samples = sample_random(self.expanded, n_per_zone=36, seed=7)
        ids = [s.candidate_id for s in samples]
        self.assertEqual(len(ids), len(set(ids)))

    def test_different_seeds_produce_different_samples(self):
        s1 = sample_lhs(self.expanded, n_per_zone=20, seed=1)
        s2 = sample_lhs(self.expanded, n_per_zone=20, seed=2)
        ids1 = {s.candidate_id for s in s1}
        ids2 = {s.candidate_id for s in s2}
        # Not identical (with overwhelming probability for this space)
        self.assertNotEqual(ids1, ids2)

    def test_same_seed_reproducible(self):
        s1 = sample_lhs(self.expanded, n_per_zone=10, seed=42)
        s2 = sample_lhs(self.expanded, n_per_zone=10, seed=42)
        ids1 = [s.candidate_id for s in s1]
        ids2 = [s.candidate_id for s in s2]
        self.assertEqual(ids1, ids2)


if __name__ == "__main__":
    unittest.main(verbosity=2)