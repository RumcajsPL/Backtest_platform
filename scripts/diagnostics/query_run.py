"""
query_run.py — Pipeline Result Analysis
========================================
Auto-discovers the latest run_id from the DB.
Pass --run-id <id> to analyse a specific run.

Usage:
    python query_run.py
    python query_run.py --run-id b0faec30-5860-4e1d-a796-7353ad1aaf7c
    python query_run.py --section wfo
    python query_run.py --section stage1
    python query_run.py --section health
    python query_run.py --section schema
    python query_run.py --section sigmoid
    python query_run.py --section zone
    python query_run.py --section jsonparams

Sections:
    all | health | summary | stages | stage1 | ga | wfo | mc | sensitivity |
    verdicts | params | jsonparams | schema | sigmoid | zone | wincoverage
"""
import argparse
import json
import sqlite3
import sys
import statistics
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DB_PATH = PROJECT_ROOT / "outputs/backtesting/backtester.db"

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def connect(db_path: str) -> sqlite3.Connection:
    if not Path(db_path).exists():
        print(f"ERROR: DB not found at {db_path}")
        print(f"Current working directory: {Path.cwd()}")
        print(f"Script location: {SCRIPT_DIR}")
        print(f"Project root: {PROJECT_ROOT}")
        sys.exit(1)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def resolve_run_id(conn: sqlite3.Connection, requested: str | None) -> str:
    if requested:
        row = conn.execute(
            "SELECT run_id FROM runs WHERE run_id = ?", (requested,)
        ).fetchone()
        if not row:
            print(f"ERROR: run_id '{requested}' not found in DB.")
            available = conn.execute(
                "SELECT run_id, scenario_name, started_at FROM runs ORDER BY started_at DESC LIMIT 5"
            ).fetchall()
            print("Available runs:")
            for r in available:
                print(f"  {r['run_id']}  scenario={r['scenario_name']}  started={r['started_at']}")
            sys.exit(1)
        return requested
    else:
        row = conn.execute(
            "SELECT run_id FROM runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        if not row:
            print("ERROR: No runs found in DB.")
            sys.exit(1)
        return row["run_id"]


def section(title: str) -> None:
    print()
    print("=" * 72)
    print(f"  {title}")
    print("=" * 72)


def subsection(title: str) -> None:
    print(f"\n  [{title}]")


def cell(v) -> str:
    if v is None:            return "None"
    if isinstance(v, float): return f"{v:.4f}"
    return str(v)


def fmt_table(rows: list, cols: list[str] | None = None) -> None:
    if not rows:
        print("  (no rows)")
        return
    if cols is None:
        cols = list(rows[0].keys())
    col_widths = {c: max(len(c), max(len(cell(r[c])) for r in rows)) for c in cols}
    header = "  " + "  ".join(c.ljust(col_widths[c]) for c in cols)
    print(header)
    print("  " + "  ".join("-" * col_widths[c] for c in cols))
    for row in rows:
        print("  " + "  ".join(cell(row[c]).ljust(col_widths[c]) for c in cols))


def _get_direct_param_cols(conn: sqlite3.Connection, run_id: str) -> list[str]:
    """
    Dynamically discover which candidate_parameters columns are worth showing:
    - Exclude bookkeeping cols (candidate_id, parameters_json, created_at, run_id etc.)
    - Exclude columns that are NULL for every WFO candidate in this run
      (e.g. rsi_period, rsi_overbought, bollinger_length — early-dev relics that
      don't apply to the current strategy family).
    Returns a list of column names in their natural schema order.
    """
    # All columns in the table
    all_cols = [
        row[1] for row in
        conn.execute("PRAGMA table_info(candidate_parameters)").fetchall()
    ]
    # Columns that are purely bookkeeping / not parameters
    skip = {"candidate_id", "run_id", "parameters_json", "created_at", "updated_at"}
    candidate_cols = [c for c in all_cols if c not in skip]

    if not candidate_cols:
        return []

    # For this run, find which columns have at least one non-NULL value among
    # WFO-scored candidates (broader than top-5 so we don't miss sparse params)
    wfo_ids = [
        row[0] for row in
        conn.execute(
            "SELECT candidate_id FROM wfo_consistency_scores WHERE run_id = ?",
            (run_id,)
        ).fetchall()
    ]
    if not wfo_ids:
        return candidate_cols  # can't filter — return all

    placeholders = ",".join("?" * len(wfo_ids))
    active_cols = []
    for col in candidate_cols:
        row = conn.execute(
            f"SELECT COUNT(*) FROM candidate_parameters "
            f"WHERE candidate_id IN ({placeholders}) AND {col} IS NOT NULL",
            wfo_ids
        ).fetchone()
        if row and row[0] > 0:
            active_cols.append(col)

    return active_cols


def _decode_json_params(parameters_json: str | None) -> dict:
    """Safely decode parameters_json; return {} on any failure."""
    try:
        return json.loads(parameters_json or "{}")
    except Exception:
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# Schema Inspection
# ─────────────────────────────────────────────────────────────────────────────

def q_schema(conn, run_id=None):
    section("DATABASE SCHEMA INSPECTION")
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    for t in tables:
        name = t[0]
        print(f"\n=== {name} ===")
        cols = conn.execute(f"PRAGMA table_info({name})").fetchall()
        for c in cols:
            print(f"  {c[1]:35s} {c[2]}")


# ─────────────────────────────────────────────────────────────────────────────
# Sigmoid Scale Calibration
# ─────────────────────────────────────────────────────────────────────────────

def q_sigmoid(conn, run_id):
    section("SIGMOID SCALE CALIBRATION")
    rows = conn.execute("""
        SELECT window_id, net_pnl
        FROM wfo_window_results
        WHERE run_id = ? AND net_pnl IS NOT NULL
        ORDER BY window_id
    """, (run_id,)).fetchall()

    null_rows = conn.execute("""
        SELECT window_id, evaluation_error, COUNT(*) as cnt
        FROM wfo_window_results
        WHERE run_id = ? AND net_pnl IS NULL
        GROUP BY window_id, evaluation_error
        ORDER BY window_id
    """, (run_id,)).fetchall()

    if not rows:
        print("ERROR: No net_pnl rows found for this run_id.")
        return

    vals = [r[1] for r in rows]
    n = len(vals)
    mean = statistics.mean(vals)
    stdev = statistics.stdev(vals)
    sigmoid_scale = stdev * 0.5

    print("=" * 60)
    print("  _SIGMOID_SCALE CALIBRATION")
    print("=" * 60)
    print(f"  Run ID : {run_id}")
    print(f"  N      : {n} net_pnl values")
    print(f"  Min    : {min(vals):.2f}")
    print(f"  Mean   : {mean:.2f}")
    print(f"  Max    : {max(vals):.2f}")
    print(f"  Stdev  : {stdev:.2f}")
    print()
    print(f"  _SIGMOID_SCALE = stdev × 0.5 = {sigmoid_scale:.1f}")
    print("=" * 60)

    print()
    print("=" * 60)
    print("  NULL WINDOW RESULTS (fitness=None) BY WINDOW + ERROR")
    print("=" * 60)
    if null_rows:
        print(f"  {'window_id':<10} {'error':<45} {'count'}")
        print(f"  {'-'*10} {'-'*45} {'-'*5}")
        for wid, err, cnt in null_rows:
            err_str = str(err)[:45] if err else "None"
            print(f"  {wid:<10} {err_str:<45} {cnt}")
    else:
        print("  (none — all windows have net_pnl)")
    print("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline Health
# ─────────────────────────────────────────────────────────────────────────────

def q_pipeline_health(conn, run_id):
    section("PIPELINE HEALTH CHECK")
    checks = []

    r = conn.execute("""
        SELECT COUNT(*) as total, SUM(passed_constraints) as passed
        FROM evaluations WHERE run_id = ? AND stage = 'RANDOM'
    """, (run_id,)).fetchone()
    total, passed = r["total"] or 0, r["passed"] or 0
    rate = passed / total if total > 0 else 0
    checks.append(("Stage 1 pass rate",
                   f"{passed}/{total} ({rate:.0%})",
                   "OK" if passed > 0 else "FAIL: 0 candidates passed - check constraints"))

    r = conn.execute("""
        SELECT COUNT(*) as n FROM candidates
        WHERE run_id = ? AND origin_stage = 'GA'
    """, (run_id,)).fetchone()
    ga_n = r["n"] or 0
    checks.append(("Stage 3 GA candidates", str(ga_n),
                   "OK" if ga_n > 0 else "WARN: No GA candidates"))

    r = conn.execute("""
        SELECT COUNT(*) as scored,
               SUM(CASE WHEN window_collapse_flag = 1 THEN 1 ELSE 0 END) as collapsed
        FROM wfo_consistency_scores WHERE run_id = ?
    """, (run_id,)).fetchone()
    scored, collapsed = r["scored"] or 0, r["collapsed"] or 0
    checks.append(("Stage 4 WFO scored",
                   f"{scored} candidates ({collapsed} collapsed)",
                   "OK" if scored > 0 else "FAIL: No WFO scores"))

    r = conn.execute("""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN evaluation_error IS NOT NULL THEN 1 ELSE 0 END) as errors,
               SUM(CASE WHEN evaluation_error IS NULL THEN 1 ELSE 0 END) as success
        FROM mc_results WHERE run_id = ? AND mode = 'deep'
    """, (run_id,)).fetchone()
    mc_total, mc_err, mc_ok = r["total"] or 0, r["errors"] or 0, r["success"] or 0
    checks.append(("Stage 5 MC Deep",
                   f"{mc_ok} success / {mc_err} errors / {mc_total} total",
                   "OK" if mc_ok > 0 else "WARN: All MC failed"))

    r = conn.execute("""
        SELECT COUNT(*) as n, SUM(spike_detected) as spikes
        FROM sensitivity_profiles WHERE run_id = ?
    """, (run_id,)).fetchone()
    sens_n, sens_sp = r["n"] or 0, r["spikes"] or 0
    checks.append(("Stage 6 Sensitivity",
                   f"{sens_n} profiles, {sens_sp} spike(s)",
                   "OK" if sens_n > 0 else "WARN: No sensitivity profiles"))

    rows = conn.execute("""
        SELECT verdict, COUNT(*) as count FROM verdicts
        WHERE run_id = ? GROUP BY verdict ORDER BY count DESC
    """, (run_id,)).fetchall()
    summary = ", ".join(f"{r['verdict']}={r['count']}" for r in rows) or "(none)"
    has_go = any(r["verdict"] in ("auto_go", "borderline") for r in rows)
    checks.append(("Stage 7 verdicts", summary,
                   "OK" if has_go else "WARN: No auto_go/borderline verdicts"))

    r = conn.execute("""
        SELECT COUNT(*) as c FROM verdicts
        WHERE run_id = ? AND verdict IN ('auto_go', 'borderline')
          AND yaml_output_path IS NOT NULL
    """, (run_id,)).fetchone()
    yaml_n = r["c"] or 0
    checks.append(("Trading YAMLs written", str(yaml_n),
                   "OK" if yaml_n > 0 else "WARN: No YAML paths recorded"))

    label_w = max(len(c[0]) for c in checks)
    for label, value, status in checks:
        tag = "[OK  ]" if status.startswith("OK") else ("[FAIL]" if "FAIL" in status else "[WARN]")
        print(f"  {tag}  {label.ljust(label_w)}  {value}")
        if not status.startswith("OK"):
            print(f"         {'':>{label_w}}  ^ {status}")


# ─────────────────────────────────────────────────────────────────────────────
# Run Summary
# ─────────────────────────────────────────────────────────────────────────────

def q_run_summary(conn, run_id):
    section("RUN SUMMARY")
    row = conn.execute("""
        SELECT run_id, scenario_name, backtester_version,
               started_at, completed_at, checkpoint,
               total_candidates_evaluated,
               ROUND(total_runtime_seconds / 60.0, 1) as runtime_min
        FROM runs WHERE run_id = ?
    """, (run_id,)).fetchone()
    if row:
        for k in row.keys():
            print(f"  {k:35s} {row[k]}")


# ─────────────────────────────────────────────────────────────────────────────
# Stage Counts
# ─────────────────────────────────────────────────────────────────────────────

def q_stage_counts(conn, run_id):
    section("CANDIDATE COUNTS BY STAGE")
    rows = conn.execute("""
        SELECT stage,
               COUNT(*) as total,
               SUM(passed_constraints) as passed,
               COUNT(*) - SUM(passed_constraints) as failed,
               ROUND(AVG(fitness_score), 4) as avg_fitness,
               ROUND(MAX(fitness_score), 4) as best_fitness
        FROM evaluations WHERE run_id = ?
        GROUP BY stage
        ORDER BY CASE stage
            WHEN 'RANDOM' THEN 1 WHEN 'MC_PREFILTER_PASS' THEN 2
            WHEN 'MC_PREFILTER_FAIL' THEN 3 WHEN 'GA' THEN 4 ELSE 5 END
    """, (run_id,)).fetchall()
    fmt_table(rows, ["stage", "total", "passed", "failed", "avg_fitness", "best_fitness"])


# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 Analysis
# ─────────────────────────────────────────────────────────────────────────────

def q_rejection_breakdown(conn, run_id):
    section("STAGE 1 - REJECTION BREAKDOWN")
    rows = conn.execute("""
        SELECT
            CASE passed_constraints WHEN 1 THEN 'PASS' ELSE 'FAIL' END as result,
            COALESCE(rejection_reason, 'n/a') as reason,
            COALESCE(failing_constraint, 'n/a') as "constraint",
            COUNT(*) as count
        FROM evaluations
        WHERE run_id = ? AND stage = 'RANDOM'
        GROUP BY passed_constraints, rejection_reason, failing_constraint
        ORDER BY count DESC
    """, (run_id,)).fetchall()
    fmt_table(rows, ["result", "reason", "constraint", "count"])


def q_constraint_margins(conn, run_id):
    section("STAGE 1 - METRIC DISTRIBUTIONS (all candidates)")
    subsection("Use this to calibrate constraint thresholds for production runs")
    r = conn.execute("""
        SELECT
            ROUND(MIN(actual_win_rate), 4)          as wr_min,
            ROUND(AVG(actual_win_rate), 4)          as wr_avg,
            ROUND(MAX(actual_win_rate), 4)          as wr_max,
            ROUND(MIN(actual_max_drawdown), 4)      as dd_min,
            ROUND(AVG(actual_max_drawdown), 4)      as dd_avg,
            ROUND(MAX(actual_max_drawdown), 4)      as dd_max,
            ROUND(MIN(actual_expectancy), 4)        as exp_min,
            ROUND(AVG(actual_expectancy), 4)        as exp_avg,
            ROUND(MAX(actual_expectancy), 4)        as exp_max,
            ROUND(MIN(actual_profit_factor), 4)     as pf_min,
            ROUND(AVG(actual_profit_factor), 4)     as pf_avg,
            ROUND(MAX(actual_profit_factor), 4)     as pf_max,
            ROUND(MIN(actual_trades_per_week), 2)   as tpw_min,
            ROUND(AVG(actual_trades_per_week), 2)   as tpw_avg,
            ROUND(MAX(actual_trades_per_week), 2)   as tpw_max,
            ROUND(MIN(actual_losing_streak), 0)     as streak_min,
            ROUND(AVG(actual_losing_streak), 1)     as streak_avg,
            ROUND(MAX(actual_losing_streak), 0)     as streak_max
        FROM evaluations WHERE run_id = ? AND stage = 'RANDOM'
    """, (run_id,)).fetchone()

    metrics = [
        ("win_rate",       r["wr_min"],     r["wr_avg"],     r["wr_max"]),
        ("max_drawdown",   r["dd_min"],     r["dd_avg"],     r["dd_max"]),
        ("expectancy",     r["exp_min"],    r["exp_avg"],    r["exp_max"]),
        ("profit_factor",  r["pf_min"],     r["pf_avg"],     r["pf_max"]),
        ("trades/week",    r["tpw_min"],    r["tpw_avg"],    r["tpw_max"]),
        ("losing_streak",  r["streak_min"], r["streak_avg"], r["streak_max"]),
    ]
    print(f"\n  {'metric':20s}  {'min':>10s}  {'avg':>10s}  {'max':>10s}")
    print(f"  {'-'*20}  {'-'*10}  {'-'*10}  {'-'*10}")
    for name, mn, avg, mx in metrics:
        def f(v): return f"{v:.4f}" if isinstance(v, float) else str(v) if v is not None else "None"
        print(f"  {name:20s}  {f(mn):>10s}  {f(avg):>10s}  {f(mx):>10s}")


def q_closest_to_passing(conn, run_id):
    section("STAGE 1 - CLOSEST-TO-PASSING FAILURES (top 10 by fitness)")
    subsection("Candidates that failed one constraint — key for threshold tuning")
    rows = conn.execute("""
        SELECT
            SUBSTR(c.candidate_id, 1, 12)       as candidate,
            e.failing_constraint,
            ROUND(e.failing_value, 4)           as failing_val,
            ROUND(e.actual_win_rate, 4)         as win_rate,
            ROUND(e.actual_max_drawdown, 4)     as drawdown,
            ROUND(e.actual_expectancy, 4)       as expectancy,
            ROUND(e.actual_profit_factor, 4)    as pf,
            ROUND(e.actual_trades_per_week, 2)  as tpw,
            ROUND(e.fitness_score, 4)           as fitness
        FROM evaluations e
        JOIN candidates c ON c.candidate_id = e.candidate_id
        WHERE e.run_id = ? AND e.stage = 'RANDOM'
          AND e.passed_constraints = 0
          AND e.error_message IS NULL
        ORDER BY e.fitness_score DESC
        LIMIT 10
    """, (run_id,)).fetchall()
    fmt_table(rows, ["candidate", "failing_constraint", "failing_val",
                     "win_rate", "drawdown", "expectancy", "pf", "tpw", "fitness"])


def q_top_stage1(conn, run_id):
    section("STAGE 1 - TOP 10 PASSED CANDIDATES")
    rows = conn.execute("""
        SELECT
            SUBSTR(c.candidate_id, 1, 12)       as candidate,
            c.zone_name                         as zone,
            ROUND(e.actual_win_rate, 4)         as win_rate,
            ROUND(e.actual_max_drawdown, 4)     as drawdown,
            ROUND(e.actual_expectancy, 4)       as expectancy,
            ROUND(e.actual_profit_factor, 4)    as pf,
            ROUND(e.actual_trades_per_week, 2)  as tpw,
            ROUND(e.fitness_score, 4)           as fitness
        FROM evaluations e
        JOIN candidates c ON c.candidate_id = e.candidate_id
        WHERE e.run_id = ? AND e.stage = 'RANDOM' AND e.passed_constraints = 1
        ORDER BY e.fitness_score DESC
        LIMIT 10
    """, (run_id,)).fetchall()
    fmt_table(rows, ["candidate", "zone", "win_rate", "drawdown", "expectancy",
                     "pf", "tpw", "fitness"])


# ─────────────────────────────────────────────────────────────────────────────
# GA Generations
# ─────────────────────────────────────────────────────────────────────────────

def q_ga_generations(conn, run_id):
    section("STAGE 3 - GA GENERATION PROGRESSION")
    rows = conn.execute("""
        SELECT generation,
               COUNT(*) as candidates,
               ROUND(MAX(fitness_score), 4) as best,
               ROUND(AVG(fitness_score), 4) as avg,
               ROUND(MIN(fitness_score), 4) as worst
        FROM evaluations
        WHERE run_id = ? AND stage = 'GA' AND generation IS NOT NULL
        GROUP BY generation ORDER BY generation
    """, (run_id,)).fetchall()
    if not rows:
        subsection("generation column not populated for GA stage - showing totals")
        r = conn.execute("""
            SELECT COUNT(*) as n FROM candidates
            WHERE run_id = ? AND origin_stage = 'GA'
        """, (run_id,)).fetchone()
        print(f"  Total GA candidates: {r['n']}  (fitness scores not stored for GA stubs)")
    else:
        fmt_table(rows, ["generation", "candidates", "best", "avg", "worst"])


# ─────────────────────────────────────────────────────────────────────────────
# WFO Scores + Window Detail
# ─────────────────────────────────────────────────────────────────────────────

def q_wfo_scores(conn, run_id):
    section("STAGE 4 - WFO CONSISTENCY SCORES")
    rows = conn.execute("""
        SELECT
            SUBSTR(w.candidate_id, 1, 12)          as candidate,
            ROUND(w.wfo_consistency_score, 4)      as wfo_score,
            w.windows_evaluated,
            w.windows_total,
            ROUND(w.median_window_return, 4)       as median_ret,
            ROUND(w.window_return_variance, 4)     as variance,
            ROUND(w.worst_window_drawdown, 4)      as worst_dd,
            ROUND(w.fraction_positive_windows, 4)  as frac_pos,
            w.window_collapse_flag                 as collapsed,
            w.oos_gate_triggered                   as oos_fail
        FROM wfo_consistency_scores w
        WHERE w.run_id = ?
        ORDER BY w.wfo_consistency_score DESC
    """, (run_id,)).fetchall()
    fmt_table(rows, ["candidate", "wfo_score", "windows_evaluated", "windows_total",
                     "median_ret", "variance", "worst_dd", "frac_pos", "collapsed", "oos_fail"])


def q_wfo_window_detail(conn, run_id):
    section("STAGE 4 - PER-WINDOW DETAIL (top 5 WFO candidates)")
    top5 = conn.execute("""
        SELECT candidate_id, ROUND(wfo_consistency_score, 4) as score
        FROM wfo_consistency_scores WHERE run_id = ?
        ORDER BY wfo_consistency_score DESC LIMIT 5
    """, (run_id,)).fetchall()

    if not top5:
        print("  (no WFO results)")
        return

    for row in top5:
        cid = row["candidate_id"]
        print(f"\n  Candidate {cid[:12]}  WFO={row['score']}")
        windows = conn.execute("""
            SELECT window_id,
                   0                                        as ga_win,
                   ROUND(MAX(fitness_score), 4)             as fitness,
                   MAX(total_trades)                        as total_trades,
                   ROUND(AVG(win_rate), 4)                  as win_rate,
                   ROUND(MAX(net_pnl), 2)                   as net_pnl,
                   ROUND(MIN(max_drawdown), 4)              as drawdown,
                   ROUND(AVG(expectancy), 4)                as expectancy,
                   ROUND(AVG(oos_delta), 4)                 as oos_delta,
                   COALESCE(MAX(evaluation_error), '')      as error
            FROM wfo_window_results
            WHERE run_id = ? AND candidate_id = ?
              AND is_ga_fitness_window = 0
            GROUP BY window_id
            ORDER BY window_id
        """, (run_id, cid)).fetchall()
        fmt_table(windows, ["window_id", "ga_win", "fitness", "total_trades",
                             "win_rate", "net_pnl", "drawdown", "expectancy",
                             "oos_delta", "error"])


# ─────────────────────────────────────────────────────────────────────────────
# MC Results
# ─────────────────────────────────────────────────────────────────────────────

def q_mc_results(conn, run_id):
    section("STAGE 5 - MC DEEP RESULTS")
    rows = conn.execute("""
        SELECT
            SUBSTR(m.candidate_id, 1, 12)          as candidate,
            ROUND(m.ruin_probability, 4)            as ruin_prob,
            ROUND(m.p5_final_equity, 4)             as p5_equity,
            ROUND(m.avg_final_equity, 4)            as avg_equity,
            ROUND(m.worst_drawdown_across_paths, 4) as worst_dd,
            m.iterations                            as iters,
            COALESCE(m.evaluation_error, '')        as error
        FROM mc_results m
        WHERE m.run_id = ? AND m.mode = 'deep'
        ORDER BY m.ruin_probability ASC
    """, (run_id,)).fetchall()
    fmt_table(rows, ["candidate", "ruin_prob", "p5_equity", "avg_equity",
                     "worst_dd", "iters", "error"])


# ─────────────────────────────────────────────────────────────────────────────
# Sensitivity
# ─────────────────────────────────────────────────────────────────────────────

def q_sensitivity(conn, run_id):
    section("STAGE 6 - SENSITIVITY PROFILES")
    subsection("Summary per candidate")
    profiles = conn.execute("""
        SELECT
            SUBSTR(candidate_id, 1, 12)    as candidate,
            ROUND(baseline_fitness, 4)     as base_fitness,
            spike_detected,
            profile_complete,
            COALESCE(spike_parameters, '') as spike_params
        FROM sensitivity_profiles WHERE run_id = ?
        ORDER BY spike_detected DESC, baseline_fitness DESC
    """, (run_id,)).fetchall()
    fmt_table(profiles, ["candidate", "base_fitness", "spike_detected",
                         "profile_complete", "spike_params"])

    subsection("Per-parameter deltas")
    results = conn.execute("""
        SELECT
            SUBSTR(r.candidate_id, 1, 12)  as candidate,
            r.parameter_name,
            r.step,
            r.perturbed_value,
            ROUND(r.baseline_fitness, 4)   as base,
            ROUND(r.perturbed_fitness, 4)  as perturbed,
            ROUND(r.fitness_delta, 4)      as delta,
            r.is_spike,
            COALESCE(r.evaluation_error,'') as error
        FROM sensitivity_results r
        WHERE r.run_id = ?
        ORDER BY r.candidate_id, r.parameter_name, r.step
    """, (run_id,)).fetchall()
    fmt_table(results, ["candidate", "parameter_name", "step", "perturbed_value",
                        "base", "perturbed", "delta", "is_spike", "error"])


# ─────────────────────────────────────────────────────────────────────────────
# Verdicts
# ─────────────────────────────────────────────────────────────────────────────

def q_verdicts(conn, run_id):
    section("STAGE 7 - FINAL VERDICTS")
    rows = conn.execute("""
        SELECT
            SUBSTR(v.candidate_id, 1, 12)        as candidate,
            v.verdict,
            ROUND(v.wfo_consistency_score, 4)    as wfo_score,
            ROUND(v.mc_deep_ruin_probability, 4) as ruin_prob,
            v.sensitivity_spike                  as spike,
            v.oos_gate_triggered                 as oos_fail,
            v.window_collapse_flag               as collapsed,
            COALESCE(v.yaml_output_path, '')     as yaml
        FROM verdicts v
        WHERE v.run_id = ?
        ORDER BY v.wfo_consistency_score DESC
    """, (run_id,)).fetchall()
    fmt_table(rows, ["candidate", "verdict", "wfo_score", "ruin_prob",
                     "spike", "oos_fail", "collapsed", "yaml"])


# ─────────────────────────────────────────────────────────────────────────────
# Parameter Spread — dynamic, no hardcoded strategy columns
# ─────────────────────────────────────────────────────────────────────────────

def q_param_spread(conn, run_id):
    """
    Parameter spread across WFO winners vs losers.
    Columns are discovered dynamically: only direct candidate_parameters columns
    that are non-NULL for at least one WFO candidate in this run are shown.
    Strategy-specific params stored in parameters_json are shown separately
    via q_json_params. This means no hardcoded RSI/Bollinger/etc. columns.
    """
    section("PARAMETER SPREAD - Winners vs Losers")
    subsection(
        "Direct columns only — strategy-specific params: use --section jsonparams"
    )

    cols = _get_direct_param_cols(conn, run_id)
    if not cols:
        print("  (no active direct parameter columns found for this run)")
        return

    def print_group(group: list, label: str) -> None:
        print(f"\n  {label}")
        hdr = f"  {'candidate':14s}  {'wfo':7s}  {'zone':12s}  " + \
              "  ".join(f"{c:14s}" for c in cols)
        print(hdr)
        print("  " + "-" * (len(hdr) - 2))
        for row in group:
            cid  = row["candidate_id"]
            wfo  = row["wfo_score"]
            zone = row["zone_name"] or "unknown"
            p = conn.execute(
                f"SELECT {', '.join(cols)} FROM candidate_parameters "
                f"WHERE candidate_id = ?", (cid,)
            ).fetchone()
            if p:
                vals = "  ".join(
                    str(p[c] if p[c] is not None else "n/a").ljust(14)
                    for c in cols
                )
                print(f"  {cid[:12]:14s}  {wfo:7.4f}  {zone:12s}  {vals}")
            else:
                print(f"  {cid[:12]:14s}  {wfo:7.4f}  {zone:12s}  (no params)")

    top5 = conn.execute("""
        SELECT w.candidate_id,
               ROUND(w.wfo_consistency_score, 4) as wfo_score,
               COALESCE(c.zone_name, 'unknown')  as zone_name
        FROM wfo_consistency_scores w
        LEFT JOIN candidates c ON c.candidate_id = w.candidate_id
        WHERE w.run_id = ?
        ORDER BY w.wfo_consistency_score DESC LIMIT 5
    """, (run_id,)).fetchall()

    bottom5 = conn.execute("""
        SELECT w.candidate_id,
               ROUND(w.wfo_consistency_score, 4) as wfo_score,
               COALESCE(c.zone_name, 'unknown')  as zone_name
        FROM wfo_consistency_scores w
        LEFT JOIN candidates c ON c.candidate_id = w.candidate_id
        WHERE w.run_id = ?
        ORDER BY w.wfo_consistency_score ASC LIMIT 5
    """, (run_id,)).fetchall()

    print_group(top5,    "TOP 5 (highest WFO score)")
    print_group(bottom5, "BOTTOM 5 (lowest WFO score)")

    subsection("Range summary across all WFO-scored candidates (by zone)")
    zones = conn.execute("""
        SELECT DISTINCT COALESCE(c.zone_name, 'unknown') as zone_name
        FROM wfo_consistency_scores w
        LEFT JOIN candidates c ON c.candidate_id = w.candidate_id
        WHERE w.run_id = ?
        ORDER BY zone_name
    """, (run_id,)).fetchall()
    zone_names = [z["zone_name"] for z in zones]

    print(f"\n  {'param':22s}  {'zone':12s}  {'min':>8s}  {'avg':>8s}  {'max':>8s}")
    print(f"  {'-'*22}  {'-'*12}  {'-'*8}  {'-'*8}  {'-'*8}")

    def _f(v):
        if v is None: return "n/a"
        return f"{v:.4f}" if isinstance(v, float) else str(v)

    for col in cols:
        first = True
        for zone in zone_names:
            r = conn.execute(f"""
                SELECT MIN(cp.{col}), AVG(cp.{col}), MAX(cp.{col})
                FROM candidate_parameters cp
                JOIN wfo_consistency_scores w ON w.candidate_id = cp.candidate_id
                LEFT JOIN candidates c ON c.candidate_id = cp.candidate_id
                WHERE w.run_id = ?
                  AND COALESCE(c.zone_name, 'unknown') = ?
                  AND cp.{col} IS NOT NULL
            """, (run_id, zone)).fetchone()
            mn, avg, mx = r[0], r[1], r[2]
            col_label = col if first else ""
            print(f"  {col_label:22s}  {zone:12s}  {_f(mn):>8s}  {_f(avg):>8s}  {_f(mx):>8s}")
            first = False
        if len(zone_names) > 1:
            # All-zones combined row
            r = conn.execute(f"""
                SELECT MIN(cp.{col}), AVG(cp.{col}), MAX(cp.{col})
                FROM candidate_parameters cp
                JOIN wfo_consistency_scores w ON w.candidate_id = cp.candidate_id
                WHERE w.run_id = ? AND cp.{col} IS NOT NULL
            """, (run_id,)).fetchone()
            mn, avg, mx = r[0], r[1], r[2]
            print(f"  {'':22s}  {'(all zones)':12s}  {_f(mn):>8s}  {_f(avg):>8s}  {_f(mx):>8s}")


# ─────────────────────────────────────────────────────────────────────────────
# JSON Params — replaces q_bollinger_params with a generic decoder
# ─────────────────────────────────────────────────────────────────────────────

def q_json_params(conn, run_id):
    """
    Decode parameters_json for all WFO-scored candidates and display the
    strategy-specific parameters not captured as direct columns.
    Columns shown are whatever keys actually appear in the JSON for this run
    (DPO, MACD, CCI, ADX, etc.) — fully dynamic, no hardcoded filter names.
    """
    section("STRATEGY PARAMETERS (decoded from parameters_json)")
    subsection(
        "All non-direct params for WFO-scored candidates — sorted by WFO score"
    )

    rows = conn.execute("""
        SELECT cp.candidate_id, cp.parameters_json,
               ROUND(w.wfo_consistency_score, 4) as wfo_score,
               COALESCE(c.zone_name, 'unknown')  as zone_name
        FROM candidate_parameters cp
        JOIN wfo_consistency_scores w ON w.candidate_id = cp.candidate_id
        LEFT JOIN candidates c ON c.candidate_id = cp.candidate_id
        WHERE w.run_id = ?
        ORDER BY w.wfo_consistency_score DESC
    """, (run_id,)).fetchall()

    if not rows:
        print("  (no rows)")
        return

    # Also get the direct columns so we can exclude them from the JSON display
    direct_cols = set(_get_direct_param_cols(conn, run_id))

    # Collect all JSON keys that appear across all candidates
    all_keys: list[str] = []
    decoded: list[dict] = []
    for row in rows:
        p = _decode_json_params(row["parameters_json"])
        # Exclude keys already shown as direct columns
        p_filtered = {k: v for k, v in p.items() if k not in direct_cols}
        decoded.append(p_filtered)
        for k in p_filtered:
            if k not in all_keys:
                all_keys.append(k)

    if not all_keys:
        print("  (no strategy-specific JSON params found — all params are direct columns)")
        return

    # Header
    col_w = 10
    cand_w = 12
    zone_w = 12
    wfo_w  = 7
    hdr = (
        f"  {'candidate':{cand_w}s}  {'wfo':>{wfo_w}s}  {'zone':{zone_w}s}  " +
        "  ".join(f"{k[:col_w]:{col_w}s}" for k in all_keys)
    )
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))

    for i, row in enumerate(rows):
        cid  = row["candidate_id"][:cand_w]
        wfo  = row["wfo_score"]
        zone = row["zone_name"][:zone_w]
        p    = decoded[i]
        vals = "  ".join(
            str(p.get(k, "n/a"))[:col_w].ljust(col_w)
            for k in all_keys
        )
        print(f"  {cid:{cand_w}s}  {wfo:>{wfo_w}.4f}  {zone:{zone_w}s}  {vals}")

    # Summary: range per JSON param, split by zone
    subsection("JSON param ranges across WFO candidates (by zone)")
    zones = list({row["zone_name"] for row in rows})
    zones.sort()

    def _f(v):
        if v is None: return "n/a"
        try:
            return f"{float(v):.4f}"
        except (TypeError, ValueError):
            return str(v)

    print(f"\n  {'param':22s}  {'zone':12s}  {'min':>10s}  {'avg':>10s}  {'max':>10s}")
    print(f"  {'-'*22}  {'-'*12}  {'-'*10}  {'-'*10}  {'-'*10}")

    for key in all_keys:
        first = True
        for zone in (zones if len(zones) > 1 else zones):
            zone_vals = [
                float(decoded[i].get(key))
                for i, row in enumerate(rows)
                if row["zone_name"] == zone
                and decoded[i].get(key) is not None
                and _is_numeric(decoded[i].get(key))
            ]
            if not zone_vals:
                continue
            mn  = min(zone_vals)
            avg = statistics.mean(zone_vals)
            mx  = max(zone_vals)
            key_label = key if first else ""
            print(f"  {key_label:22s}  {zone:12s}  {_f(mn):>10s}  {_f(avg):>10s}  {_f(mx):>10s}")
            first = False

        if len(zones) > 1:
            all_vals = [
                float(decoded[i].get(key))
                for i in range(len(rows))
                if decoded[i].get(key) is not None and _is_numeric(decoded[i].get(key))
            ]
            if all_vals:
                print(f"  {'':22s}  {'(all zones)':12s}  "
                      f"{_f(min(all_vals)):>10s}  "
                      f"{_f(statistics.mean(all_vals)):>10s}  "
                      f"{_f(max(all_vals)):>10s}")


def _is_numeric(v) -> bool:
    try:
        float(v)
        return True
    except (TypeError, ValueError):
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Zone Funnel — NEW: CCI exploration diagnostic
# ─────────────────────────────────────────────────────────────────────────────

def q_zone_funnel(conn, run_id):
    """
    Zone-level survival rate at each pipeline stage.
    Shows how many candidates from each zone (safe, exploration, discovery…)
    made it through Stage 1 → GA → WFO → final verdict.
    Primary use: diagnose whether CCI exploration candidates are dying at
    Stage 1 (constraint mismatch), WFO (trade starvation / poor consistency),
    or simply scoring lower than safe-zone candidates at the final verdict stage.
    """
    section("ZONE FUNNEL — SURVIVAL BY ZONE")
    subsection(
        "Use this to diagnose why exploration-zone candidates disappear from top-10"
    )

    # Stage 1: all random search candidates, pass/fail by zone
    s1 = conn.execute("""
        SELECT
            COALESCE(c.zone_name, 'unknown')     as zone,
            COUNT(*)                              as s1_total,
            SUM(e.passed_constraints)             as s1_passed,
            COUNT(*) - SUM(e.passed_constraints)  as s1_failed,
            ROUND(
                100.0 * SUM(e.passed_constraints) / COUNT(*), 1
            )                                     as s1_pass_pct
        FROM evaluations e
        JOIN candidates c ON c.candidate_id = e.candidate_id
        WHERE e.run_id = ? AND e.stage = 'RANDOM'
        GROUP BY c.zone_name
        ORDER BY c.zone_name
    """, (run_id,)).fetchall()

    print(f"\n  Stage 1 — Random search (pass rate by zone)")
    fmt_table(s1, ["zone", "s1_total", "s1_passed", "s1_failed", "s1_pass_pct"])

    # Stage 1: rejection reasons by zone (only FAIL rows)
    s1_rej = conn.execute("""
        SELECT
            COALESCE(c.zone_name, 'unknown')       as zone,
            COALESCE(e.failing_constraint, 'n/a')  as failing_constraint,
            COUNT(*)                                as count
        FROM evaluations e
        JOIN candidates c ON c.candidate_id = e.candidate_id
        WHERE e.run_id = ? AND e.stage = 'RANDOM'
          AND e.passed_constraints = 0
        GROUP BY c.zone_name, e.failing_constraint
        ORDER BY c.zone_name, count DESC
    """, (run_id,)).fetchall()

    print(f"\n  Stage 1 — Rejection reasons by zone")
    fmt_table(s1_rej, ["zone", "failing_constraint", "count"])

    # GA: candidates produced per zone
    ga = conn.execute("""
        SELECT
            COALESCE(c.zone_name, 'unknown') as zone,
            COUNT(*)                          as ga_candidates
        FROM candidates c
        WHERE c.run_id = ? AND c.origin_stage = 'GA'
        GROUP BY c.zone_name
        ORDER BY c.zone_name
    """, (run_id,)).fetchall()

    print(f"\n  Stage 3 — GA candidates by zone")
    fmt_table(ga, ["zone", "ga_candidates"])

    # WFO: how many WFO-scored candidates per zone, avg windows evaluated
    wfo = conn.execute("""
        SELECT
            COALESCE(c.zone_name, 'unknown')          as zone,
            COUNT(*)                                   as wfo_candidates,
            ROUND(AVG(w.windows_evaluated), 2)         as avg_windows,
            ROUND(AVG(w.wfo_consistency_score), 4)     as avg_wfo_score,
            ROUND(MAX(w.wfo_consistency_score), 4)     as best_wfo_score,
            SUM(w.window_collapse_flag)                as collapsed_count
        FROM wfo_consistency_scores w
        LEFT JOIN candidates c ON c.candidate_id = w.candidate_id
        WHERE w.run_id = ?
        GROUP BY c.zone_name
        ORDER BY c.zone_name
    """, (run_id,)).fetchall()

    print(f"\n  Stage 4 — WFO results by zone")
    fmt_table(wfo, ["zone", "wfo_candidates", "avg_windows",
                    "avg_wfo_score", "best_wfo_score", "collapsed_count"])

    # WFO top-5 per zone
    print(f"\n  Stage 4 — Top 5 WFO candidates per zone")
    zones = conn.execute("""
        SELECT DISTINCT COALESCE(c.zone_name, 'unknown') as zone_name
        FROM wfo_consistency_scores w
        LEFT JOIN candidates c ON c.candidate_id = w.candidate_id
        WHERE w.run_id = ?
        ORDER BY zone_name
    """, (run_id,)).fetchall()

    for z in zones:
        zone = z["zone_name"]
        top = conn.execute("""
            SELECT
                SUBSTR(w.candidate_id, 1, 12)          as candidate,
                ROUND(w.wfo_consistency_score, 4)      as wfo_score,
                w.windows_evaluated,
                ROUND(w.median_window_return, 2)       as median_ret,
                w.window_collapse_flag                 as collapsed
            FROM wfo_consistency_scores w
            LEFT JOIN candidates c ON c.candidate_id = w.candidate_id
            WHERE w.run_id = ?
              AND COALESCE(c.zone_name, 'unknown') = ?
            ORDER BY w.wfo_consistency_score DESC
            LIMIT 5
        """, (run_id, zone)).fetchall()
        print(f"\n    Zone: {zone}")
        fmt_table(top, ["candidate", "wfo_score", "windows_evaluated",
                        "median_ret", "collapsed"])

    # Final verdicts by zone
    verd = conn.execute("""
        SELECT
            COALESCE(c.zone_name, 'unknown') as zone,
            v.verdict,
            COUNT(*)                          as count
        FROM verdicts v
        LEFT JOIN candidates c ON c.candidate_id = v.candidate_id
        WHERE v.run_id = ?
        GROUP BY c.zone_name, v.verdict
        ORDER BY c.zone_name, count DESC
    """, (run_id,)).fetchall()

    print(f"\n  Stage 7 — Final verdicts by zone")
    fmt_table(verd, ["zone", "verdict", "count"])


# ─────────────────────────────────────────────────────────────────────────────
# Window Coverage by Zone — NEW: per-window fitness=None breakdown
# ─────────────────────────────────────────────────────────────────────────────

def q_wfo_window_coverage(conn, run_id):
    """
    Per-window fitness=None (REJECTED_INSUFFICIENT_TRADES) breakdown by zone.
    Shows which windows are killing which zones. Key for deciding whether the
    CCI exploration zone is dying because of a specific window or universally.
    Also shows avg trades per window per zone so you can spot starvation.
    """
    section("WFO WINDOW COVERAGE BY ZONE")
    subsection(
        "fitness=None counts and avg trades per window — key for trade starvation diagnosis"
    )

    # Null (rejected) counts per window per zone
    null_by_window = conn.execute("""
        SELECT
            wr.window_id,
            COALESCE(c.zone_name, 'unknown')                    as zone,
            COUNT(*)                                             as total_windows,
            SUM(CASE WHEN wr.fitness_score IS NULL THEN 1 ELSE 0 END) as null_count,
            SUM(CASE WHEN wr.fitness_score IS NOT NULL THEN 1 ELSE 0 END) as scored_count,
            ROUND(
                100.0 * SUM(CASE WHEN wr.fitness_score IS NULL THEN 1 ELSE 0 END)
                / COUNT(*), 1
            )                                                    as null_pct,
            ROUND(AVG(CASE WHEN wr.total_trades IS NOT NULL
                      THEN wr.total_trades ELSE NULL END), 1)   as avg_trades,
            ROUND(MIN(CASE WHEN wr.total_trades IS NOT NULL
                      THEN wr.total_trades ELSE NULL END), 0)   as min_trades
        FROM wfo_window_results wr
        LEFT JOIN candidates c ON c.candidate_id = wr.candidate_id
        WHERE wr.run_id = ? AND wr.is_ga_fitness_window = 0
        GROUP BY wr.window_id, c.zone_name
        ORDER BY wr.window_id, c.zone_name
    """, (run_id,)).fetchall()

    fmt_table(null_by_window, ["window_id", "zone", "total_windows",
                                "null_count", "scored_count", "null_pct",
                                "avg_trades", "min_trades"])

    # Rejection error breakdown per window per zone
    subsection("Rejection error reasons per window per zone (fitness=None only)")
    errors = conn.execute("""
        SELECT
            wr.window_id,
            COALESCE(c.zone_name, 'unknown')                         as zone,
            COALESCE(wr.evaluation_error, 'no_error_recorded')       as error,
            COUNT(*)                                                  as count
        FROM wfo_window_results wr
        LEFT JOIN candidates c ON c.candidate_id = wr.candidate_id
        WHERE wr.run_id = ?
          AND wr.is_ga_fitness_window = 0
          AND wr.fitness_score IS NULL
          AND wr.evaluation_error IS NOT NULL
        GROUP BY wr.window_id, c.zone_name, wr.evaluation_error
        ORDER BY wr.window_id, c.zone_name, count DESC
    """, (run_id,)).fetchall()

    # Separately count silent failures (fitness=None, error=None)
    silent = conn.execute("""
        SELECT
            wr.window_id,
            COALESCE(c.zone_name, 'unknown') as zone,
            COUNT(*)                          as count
        FROM wfo_window_results wr
        LEFT JOIN candidates c ON c.candidate_id = wr.candidate_id
        WHERE wr.run_id = ?
          AND wr.is_ga_fitness_window = 0
          AND wr.fitness_score IS NULL
          AND wr.evaluation_error IS NULL
        GROUP BY wr.window_id, c.zone_name
        ORDER BY wr.window_id, c.zone_name
    """, (run_id,)).fetchall()

    if errors:
        fmt_table(errors, ["window_id", "zone", "error", "count"])
    else:
        print("  (no windows with recorded rejection errors)")

    if silent:
        subsection("Silent failures (fitness=None, no error recorded)")
        fmt_table(silent, ["window_id", "zone", "count"])

    # Summary: avg windows_evaluated per zone (honest coverage bar)
    subsection("Avg windows evaluated per zone (honest coverage — higher is better)")
    coverage = conn.execute("""
        SELECT
            COALESCE(c.zone_name, 'unknown')             as zone,
            COUNT(*)                                      as wfo_candidates,
            ROUND(AVG(w.windows_evaluated), 2)            as avg_windows_evaluated,
            ROUND(AVG(w.windows_total), 0)                as windows_total,
            SUM(CASE WHEN w.windows_evaluated >= 3
                     THEN 1 ELSE 0 END)                   as honest_count,
            SUM(CASE WHEN w.windows_evaluated < 3
                     THEN 1 ELSE 0 END)                   as phantom_risk_count
        FROM wfo_consistency_scores w
        LEFT JOIN candidates c ON c.candidate_id = w.candidate_id
        WHERE w.run_id = ?
        GROUP BY c.zone_name
        ORDER BY c.zone_name
    """, (run_id,)).fetchall()

    fmt_table(coverage, ["zone", "wfo_candidates", "avg_windows_evaluated",
                          "windows_total", "honest_count", "phantom_risk_count"])


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Backtesting pipeline result analyser")
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--section", default="all",
        help=(
            "all | health | summary | stages | stage1 | ga | wfo | mc | "
            "sensitivity | verdicts | params | jsonparams | schema | sigmoid | "
            "zone | wincoverage"
        )
    )
    args = parser.parse_args()

    conn = connect(str(DB_PATH))

    if args.section == "schema":
        q_schema(conn, None)
        conn.close()
        return

    run_id = resolve_run_id(conn, args.run_id)
    print(f"\nAnalysing run: {run_id}")

    s = args.section
    run_all = s == "all"

    if run_all or s == "health":       q_pipeline_health(conn, run_id)
    if run_all or s == "summary":      q_run_summary(conn, run_id)
    if run_all or s == "stages":       q_stage_counts(conn, run_id)
    if run_all or s == "stage1":
        q_rejection_breakdown(conn, run_id)
        q_constraint_margins(conn, run_id)
        q_closest_to_passing(conn, run_id)
        q_top_stage1(conn, run_id)
    if run_all or s == "ga":           q_ga_generations(conn, run_id)
    if run_all or s == "wfo":
        q_wfo_scores(conn, run_id)
        q_wfo_window_detail(conn, run_id)
    if run_all or s == "mc":           q_mc_results(conn, run_id)
    if run_all or s == "sensitivity":  q_sensitivity(conn, run_id)
    if run_all or s == "verdicts":     q_verdicts(conn, run_id)
    if run_all or s == "params":       q_param_spread(conn, run_id)
    if run_all or s == "jsonparams":   q_json_params(conn, run_id)
    if run_all or s == "sigmoid":      q_sigmoid(conn, run_id)
    if run_all or s == "zone":         q_zone_funnel(conn, run_id)
    if run_all or s == "wincoverage":  q_wfo_window_coverage(conn, run_id)

    print()
    conn.close()


if __name__ == "__main__":
    main()