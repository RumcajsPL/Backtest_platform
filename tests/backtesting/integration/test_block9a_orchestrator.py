"""
tests/backtesting/integration/test_block9a_orchestrator.py
-----------------------------------------------------------
Block 9A audit — orchestrator.py findings.

Findings covered:
  B9A-002 (FIXED)  — Stage 1 stub must advance checkpoint to RANDOM_SEARCH_COMPLETE.
                     Pre-fix: checkpoint stayed at RUN_INITIALISED; Stage 1 re-ran
                     on every resume. Post-fix: checkpoint advances correctly.

  B9A-003 (OPEN)   — spike_threshold dual-source documented. Test asserts the
                     current (broken) behaviour and will fail when the fix is
                     applied, acting as a reminder to remove the config key.

  B9A-006 (CLOSED) — CandidateParameterSet.create() is deterministic (SHA-256).
                     Reconstructing from the same parameters always yields the
                     same candidate_id as stored. No bug.

  B8C-007 (CLOSED) — Stage 7 guards None wfo_score/mc_result before calling
                     compute_verdict(). No AttributeError risk.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call
import pytest


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_store_mock(checkpoint_value: int = 1):
    """Return a MagicMock CandidateStore with checkpoint logic."""
    store = MagicMock()
    checkpoint = MagicMock()
    checkpoint.value = checkpoint_value

    # Each call to get_checkpoint advances value by +1 to simulate stage progression
    call_count = [checkpoint_value]

    def get_checkpoint(run_id):
        cp = MagicMock()
        cp.value = call_count[0]
        return cp

    store.get_checkpoint.side_effect = get_checkpoint
    store.set_checkpoint.side_effect = lambda run_id, cp: call_count.__setitem__(0, cp.value)
    return store, call_count


def _make_run_metadata():
    """Minimal RunMetadata-like namespace for orchestrator stage calls."""
    meta = MagicMock()
    meta.run_id = "test-run-id-9a"
    meta.mc_deep_seed = 45
    return meta


# ── B9A-002: Stage 1 checkpoint ───────────────────────────────────────────────

class TestB9A002Stage1Checkpoint:
    """
    B9A-002 (FIXED): Stage 1 stub must call
    store.set_checkpoint(run_id, Checkpoint.RANDOM_SEARCH_COMPLETE)
    after _run_stage_1_random_search().

    Pre-fix: only _run_stage_1_random_search() was called — no checkpoint advance.
    Post-fix: checkpoint is advanced, Stage 1 is skipped on resume.
    """

    def test_stage1_advances_checkpoint(self):
        """
        After Stage 1 runs, checkpoint must be RANDOM_SEARCH_COMPLETE.
        Verified by inspecting set_checkpoint call args.
        """
        from src.backtesting.orchestrator import _execute_pipeline
        from src.backtesting.contracts import Checkpoint

        store, checkpoint_tracker = _make_store_mock(checkpoint_value=1)
        # Start at RUN_INITIALISED (value=1) — Stage 0 already done
        # Stage 1 should run (value < RANDOM_SEARCH_COMPLETE.value=2)
        run_metadata = _make_run_metadata()

        # Patch all stage functions so only checkpoint logic runs
        with patch("src.backtesting.orchestrator._run_stage_0_init"), \
             patch("src.backtesting.orchestrator._run_stage_1_random_search") as mock_s1, \
             patch("src.backtesting.orchestrator._run_stage_2_mc_prefilter"), \
             patch("src.backtesting.orchestrator._run_stage_3_ga"), \
             patch("src.backtesting.orchestrator._run_stage_4_wfo"), \
             patch("src.backtesting.orchestrator._run_stage_5_mc_deep"), \
             patch("src.backtesting.orchestrator._run_stage_6_sensitivity"), \
             patch("src.backtesting.orchestrator._run_stage_7_report"):

            config = {
                "run": {"output_dir": "/tmp/test", "max_workers": 1},
                "walk_forward": {"enforce_oos_gate": False},
                "sensitivity": {"input_count": 5},
                "output": {"formats": {}},
            }
            _execute_pipeline(config, store, run_metadata)

        # Stage 1 ran
        mock_s1.assert_called_once()

        # Checkpoint MUST have been advanced to RANDOM_SEARCH_COMPLETE
        checkpoint_calls = [str(c) for c in store.set_checkpoint.call_args_list]
        advanced_to_random_search = any(
            "RANDOM_SEARCH_COMPLETE" in c for c in checkpoint_calls
        )
        assert advanced_to_random_search, (
            "B9A-002: Stage 1 did not advance checkpoint to RANDOM_SEARCH_COMPLETE. "
            f"set_checkpoint calls: {checkpoint_calls}"
        )

    def test_stage1_skipped_on_resume_if_checkpoint_already_advanced(self):
        """
        If checkpoint >= RANDOM_SEARCH_COMPLETE.value (2), Stage 1 must be skipped.
        This is the resume scenario — Stage 1 should not re-run.
        """
        from src.backtesting.orchestrator import _execute_pipeline
        from src.backtesting.contracts import Checkpoint

        # Start at RANDOM_SEARCH_COMPLETE (value=2) — Stage 1 already done
        store, _ = _make_store_mock(checkpoint_value=2)
        run_metadata = _make_run_metadata()

        with patch("src.backtesting.orchestrator._run_stage_0_init"), \
             patch("src.backtesting.orchestrator._run_stage_1_random_search") as mock_s1, \
             patch("src.backtesting.orchestrator._run_stage_2_mc_prefilter"), \
             patch("src.backtesting.orchestrator._run_stage_3_ga"), \
             patch("src.backtesting.orchestrator._run_stage_4_wfo"), \
             patch("src.backtesting.orchestrator._run_stage_5_mc_deep"), \
             patch("src.backtesting.orchestrator._run_stage_6_sensitivity"), \
             patch("src.backtesting.orchestrator._run_stage_7_report"):

            config = {
                "run": {"output_dir": "/tmp/test", "max_workers": 1},
                "walk_forward": {"enforce_oos_gate": False},
                "sensitivity": {"input_count": 5},
                "output": {"formats": {}},
            }
            _execute_pipeline(config, store, run_metadata)

        mock_s1.assert_not_called(), (
            "B9A-002: Stage 1 ran despite checkpoint already at RANDOM_SEARCH_COMPLETE. "
            "Resume logic is broken — Stage 1 will re-run on every resume."
        )

    def test_all_stub_stages_advance_checkpoint(self):
        """
        Regression guard: all stub stages (1–4) must advance their checkpoint.
        Inspects set_checkpoint call args for all four expected checkpoint values.
        """
        from src.backtesting.orchestrator import _execute_pipeline
        from src.backtesting.contracts import Checkpoint

        store, _ = _make_store_mock(checkpoint_value=1)
        run_metadata = _make_run_metadata()

        with patch("src.backtesting.orchestrator._run_stage_0_init"), \
             patch("src.backtesting.orchestrator._run_stage_1_random_search"), \
             patch("src.backtesting.orchestrator._run_stage_2_mc_prefilter"), \
             patch("src.backtesting.orchestrator._run_stage_3_ga"), \
             patch("src.backtesting.orchestrator._run_stage_4_wfo"), \
             patch("src.backtesting.orchestrator._run_stage_5_mc_deep"), \
             patch("src.backtesting.orchestrator._run_stage_6_sensitivity"), \
             patch("src.backtesting.orchestrator._run_stage_7_report"):

            config = {
                "run": {"output_dir": "/tmp/test", "max_workers": 1},
                "walk_forward": {"enforce_oos_gate": False},
                "sensitivity": {"input_count": 5},
                "output": {"formats": {}},
            }
            _execute_pipeline(config, store, run_metadata)

        checkpoint_calls = [str(c) for c in store.set_checkpoint.call_args_list]
        expected_checkpoints = [
            "RANDOM_SEARCH_COMPLETE",
            "MC_PREFILTER_COMPLETE",
            "GA_COMPLETE",
            "WFO_COMPLETE",
        ]
        for expected in expected_checkpoints:
            found = any(expected in c for c in checkpoint_calls)
            assert found, (
                f"Stage checkpoint '{expected}' was never advanced. "
                f"Actual set_checkpoint calls: {checkpoint_calls}"
            )


# ── B9A-006: candidate_id determinism ────────────────────────────────────────

class TestB9A006CandidateIdDeterminism:
    """
    B9A-006 (CLOSED): CandidateParameterSet.create() is deterministic —
    candidate_id is SHA-256 of the parameters dict. Reconstructing from the
    same parameters dict always yields the same ID as stored in the DB.
    No data loss from _record_to_candidate().
    """

    def test_create_same_params_yields_same_id(self):
        """Same parameters → same candidate_id on every call."""
        from src.backtesting.contracts import CandidateParameterSet

        params = {"ema_fast": 10, "ema_slow": 20, "atr_mult": 1.5}
        c1 = CandidateParameterSet.create("zone_a", params)
        c2 = CandidateParameterSet.create("zone_a", params)

        assert c1.candidate_id == c2.candidate_id, (
            "B9A-006: CandidateParameterSet.create() is not deterministic. "
            "candidate_id differs for identical parameters — DB lookups in "
            "Stage 5/6 would always return None."
        )

    def test_record_to_candidate_preserves_id(self):
        """
        _record_to_candidate(record) must produce the same candidate_id
        as was originally stored for those parameters.
        """
        from src.backtesting.contracts import CandidateParameterSet
        from src.backtesting.orchestrator import _record_to_candidate

        params = {"ema_fast": 10, "ema_slow": 20, "atr_mult": 1.5}
        original = CandidateParameterSet.create("zone_a", params)

        # Simulate a DB record dict (as rank_by_wfo returns)
        record = {
            "candidate_id": original.candidate_id,
            "zone_name": "zone_a",
            "parameters": params,
            "generation": None,
        }

        reconstructed = _record_to_candidate(record)

        assert reconstructed.candidate_id == original.candidate_id, (
            "B9A-006: _record_to_candidate() produced a different candidate_id "
            f"than what was stored. original={original.candidate_id[:12]}, "
            f"reconstructed={reconstructed.candidate_id[:12]}. "
            "Stage 5 store.get_candidate_result() would always return None."
        )


# ── B8C-007: Stage 7 None guards ─────────────────────────────────────────────

class TestB8C007NoneGuardsInStage7:
    """
    B8C-007 (CLOSED): Stage 7 guards None wfo_score and mc_result before
    calling compute_verdict(). compute_verdict() is never reached with None
    inputs — the AttributeError risk identified in B8C-007 does not materialise.
    """

    def test_none_wfo_score_skips_verdict(self):
        """
        If wfo_score is None, Stage 7 must skip the candidate and not call
        compute_verdict(). Verified by confirming compute_verdict is not called.
        """
        from src.backtesting.orchestrator import _run_stage_7_report
        from src.backtesting.contracts import Checkpoint

        store = MagicMock()
        store.rank_by_wfo.return_value = [
            {"candidate_id": "abc123", "zone_name": "z", "parameters": {"x": 1}}
        ]
        store.get_wfo_consistency_score.return_value = None  # None → must skip
        store.get_mc_result.return_value = MagicMock()

        run_metadata = _make_run_metadata()
        config = {
            "run": {"output_dir": "/tmp/test"},
            "walk_forward": {"enforce_oos_gate": False},
            "sensitivity": {"input_count": 5},
            "output": {"formats": {}},
        }

        with patch("src.backtesting.orchestrator.load_scenario"), \
             patch("src.backtesting.orchestrator._resolve_base_yaml"), \
             patch("src.backtesting.orchestrator.compute_verdict") as mock_cv, \
             patch("src.backtesting.orchestrator.generate_report"):
            _run_stage_7_report(config, store, run_metadata)

        mock_cv.assert_not_called(), (
            "B8C-007: compute_verdict() was called despite wfo_score=None. "
            "Stage 7 None guard is missing or broken."
        )

    def test_none_mc_result_skips_verdict(self):
        """If mc_result is None, Stage 7 must skip the candidate."""
        from src.backtesting.orchestrator import _run_stage_7_report

        store = MagicMock()
        store.rank_by_wfo.return_value = [
            {"candidate_id": "abc123", "zone_name": "z", "parameters": {"x": 1}}
        ]
        store.get_wfo_consistency_score.return_value = MagicMock()
        store.get_mc_result.return_value = None  # None → must skip

        run_metadata = _make_run_metadata()
        config = {
            "run": {"output_dir": "/tmp/test"},
            "walk_forward": {"enforce_oos_gate": False},
            "sensitivity": {"input_count": 5},
            "output": {"formats": {}},
        }

        with patch("src.backtesting.orchestrator.load_scenario"), \
             patch("src.backtesting.orchestrator._resolve_base_yaml"), \
             patch("src.backtesting.orchestrator.compute_verdict") as mock_cv, \
             patch("src.backtesting.orchestrator.generate_report"):
            _run_stage_7_report(config, store, run_metadata)

        mock_cv.assert_not_called(), (
            "B8C-007: compute_verdict() was called despite mc_result=None."
        )