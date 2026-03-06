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

Sections: all | health | summary | stages | stage1 | ga | wfo | mc | sensitivity | verdicts | params | bollinger
"""

import argparse
import json
import sqlite3
import sys
from pathlib import Path

DB_PATH = "outputs/backtesting/backtester.db"

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def connect(db_path: str) -> sqlite3.Connection:
    if not Path(db_path).exists():
        print(f"ERROR: DB not found at {db_path}")
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


# ─────────────────────────────────────────────────────────────────────────────
# Sections
# ─────────────────────────────────────────────────────────────────────────────

def q_pipeline_health(conn, run_id):
    section("PIPELINE HEALTH CHECK")
    checks = []

    # Stage 1
    r = conn.execute("""
        SELECT COUNT(*) as total, SUM(passed_constraints) as passed
        FROM evaluations WHERE run_id = ? AND stage = 'RANDOM'
    """, (run_id,)).fetchone()
    total, passed = r["total"] or 0, r["passed"] or 0
    rate = passed / total if total > 0 else 0
    checks.append(("Stage 1 pass rate",
                   f"{passed}/{total} ({rate:.0%})",
                   "OK" if passed > 0 else "FAIL: 0 candidates passed - check constraints"))

    # Stage 3 GA — query candidates table (stubs write no evaluations row)
    # B9H-001: was querying evaluations WHERE stage='GA' which always returns 0
    # because write_candidate_stub() does not write an evaluations row.
    r = conn.execute("""
        SELECT COUNT(*) as n
        FROM candidates WHERE run_id = ? AND origin_stage = 'GA'
    """, (run_id,)).fetchone()
    ga_n = r["n"] or 0
    checks.append(("Stage 3 GA candidates", str(ga_n),
                   "OK" if ga_n > 0 else "WARN: No GA candidates"))

    # Stage 4 WFO
    r = conn.execute("""
        SELECT COUNT(*) as scored,
               SUM(CASE WHEN window_collapse_flag = 1 THEN 1 ELSE 0 END) as collapsed
        FROM wfo_consistency_scores WHERE run_id = ?
    """, (run_id,)).fetchone()
    scored, collapsed = r["scored"] or 0, r["collapsed"] or 0
    checks.append(("Stage 4 WFO scored",
                   f"{scored} candidates ({collapsed} collapsed)",
                   "OK" if scored > 0 else "FAIL: No WFO scores"))

    # Stage 5 MC Deep
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

    # Stage 6 Sensitivity
    r = conn.execute("""
        SELECT COUNT(*) as n, SUM(spike_detected) as spikes
        FROM sensitivity_profiles WHERE run_id = ?
    """, (run_id,)).fetchone()
    sens_n, sens_sp = r["n"] or 0, r["spikes"] or 0
    checks.append(("Stage 6 Sensitivity",
                   f"{sens_n} profiles, {sens_sp} spike(s)",
                   "OK" if sens_n > 0 else "WARN: No sensitivity profiles"))

    # Stage 7 Verdicts
    rows = conn.execute("""
        SELECT verdict, COUNT(*) as count FROM verdicts
        WHERE run_id = ? GROUP BY verdict ORDER BY count DESC
    """, (run_id,)).fetchall()
    summary = ", ".join(f"{r['verdict']}={r['count']}" for r in rows) or "(none)"
    has_go = any(r["verdict"] in ("auto_go", "borderline") for r in rows)
    checks.append(("Stage 7 verdicts", summary,
                   "OK" if has_go else "WARN: No auto_go/borderline verdicts"))

    # Trading YAMLs
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
            ROUND(MIN(actual_net_pnl), 2)           as pnl_min,
            ROUND(AVG(actual_net_pnl), 2)           as pnl_avg,
            ROUND(MAX(actual_net_pnl), 2)           as pnl_max,
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
        ("net_pnl_pts",    r["pnl_min"],    r["pnl_avg"],    r["pnl_max"]),
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
            e.actual_total_trades               as trades,
            ROUND(e.actual_net_pnl, 2)          as net_pnl,
            ROUND(e.fitness_score, 4)           as fitness
        FROM evaluations e
        JOIN candidates c ON c.candidate_id = e.candidate_id
        WHERE e.run_id = ? AND e.stage = 'RANDOM' AND e.passed_constraints = 1
        ORDER BY e.fitness_score DESC
        LIMIT 10
    """, (run_id,)).fetchall()
    fmt_table(rows, ["candidate", "zone", "win_rate", "drawdown", "expectancy",
                     "pf", "tpw", "trades", "net_pnl", "fitness"])


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
        # B9H-001: count GA candidates from candidates table (stubs write no evaluations row)
        r = conn.execute("""
            SELECT COUNT(*) as n
            FROM candidates WHERE run_id = ? AND origin_stage = 'GA'
        """, (run_id,)).fetchone()
        print(f"  Total GA candidates: {r['n']}  (fitness scores not stored for GA stubs)")
    else:
        fmt_table(rows, ["generation", "candidates", "best", "avg", "worst"])


def q_wfo_scores(conn, run_id):
    section("STAGE 4 - WFO CONSISTENCY SCORES")
    rows = conn.execute("""
        SELECT
            SUBSTR(w.candidate_id, 1, 12)         as candidate,
            ROUND(w.wfo_consistency_score, 4)     as wfo_score,
            w.windows_evaluated,
            w.windows_total,
            ROUND(w.median_window_return, 4)      as median_ret,
            ROUND(w.window_return_variance, 4)    as variance,
            ROUND(w.worst_window_drawdown, 4)     as worst_dd,
            ROUND(w.fraction_positive_windows, 4) as frac_pos,
            w.window_collapse_flag                as collapsed,
            w.oos_gate_triggered                  as oos_fail
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
        # B9H-002: Deduplicate rows per window_id. _write_wfo_window_result uses
        # INSERT OR REPLACE with a fresh uuid4() PK, so duplicate rows accumulate
        # when the same candidate is evaluated multiple times (GA + Stage 4).
        # GROUP BY window_id with MAX(fitness_score) keeps the best result per window,
        # consistent with how consistency_scorer.py treats the window results.
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


def q_param_spread(conn, run_id):
    section("PARAMETER SPREAD - Winners vs Losers")

    cols = ["rsi_period", "rsi_overbought", "rsi_oversold",
            "atr_length", "atr_multiplier", "rr_target", "risk_percentile"]

    def print_group(group, label):
        print(f"\n  {label}")
        hdr = f"  {'candidate':14s}  {'wfo':7s}  " + "  ".join(f"{c:16s}" for c in cols)
        print(hdr)
        print("  " + "-" * (len(hdr) - 2))
        for row in group:
            cid = row["candidate_id"]
            wfo = row["wfo_score"]
            p = conn.execute(
                f"SELECT {', '.join(cols)} FROM candidate_parameters WHERE candidate_id = ?",
                (cid,)
            ).fetchone()
            if p:
                vals = "  ".join(str(p[c] if p[c] is not None else "n/a").ljust(16) for c in cols)
                print(f"  {cid[:12]:14s}  {wfo:7.4f}  {vals}")
            else:
                print(f"  {cid[:12]:14s}  {wfo:7.4f}  (no params)")

    top5 = conn.execute("""
        SELECT candidate_id, ROUND(wfo_consistency_score,4) as wfo_score
        FROM wfo_consistency_scores WHERE run_id = ?
        ORDER BY wfo_consistency_score DESC LIMIT 5
    """, (run_id,)).fetchall()

    bottom5 = conn.execute("""
        SELECT candidate_id, ROUND(wfo_consistency_score,4) as wfo_score
        FROM wfo_consistency_scores WHERE run_id = ?
        ORDER BY wfo_consistency_score ASC LIMIT 5
    """, (run_id,)).fetchall()

    print_group(top5, "TOP 5 (highest WFO score)")
    print_group(bottom5, "BOTTOM 5 (lowest WFO score)")

    subsection("Range summary across all WFO-scored candidates")
    print(f"\n  {'param':20s}  {'min':>8s}  {'avg':>8s}  {'max':>8s}")
    print(f"  {'-'*20}  {'-'*8}  {'-'*8}  {'-'*8}")
    for col in cols:
        r = conn.execute(f"""
            SELECT MIN(cp.{col}), AVG(cp.{col}), MAX(cp.{col})
            FROM candidate_parameters cp
            JOIN wfo_consistency_scores w ON w.candidate_id = cp.candidate_id
            WHERE w.run_id = ?
        """, (run_id,)).fetchone()
        mn, avg, mx = r[0], r[1], r[2]
        def f(v): return f"{v:.4f}" if isinstance(v, float) else str(v) if v is not None else "n/a"
        print(f"  {col:20s}  {f(mn):>8s}  {f(avg):>8s}  {f(mx):>8s}")


def q_bollinger_params(conn, run_id):
    section("BOLLINGER PARAMETERS (decoded from parameters_json)")
    subsection("bollinger_length and bollinger_multiplier are not individual columns")
    rows = conn.execute("""
        SELECT cp.candidate_id, cp.parameters_json,
               ROUND(w.wfo_consistency_score, 4) as wfo_score
        FROM candidate_parameters cp
        JOIN wfo_consistency_scores w ON w.candidate_id = cp.candidate_id
        WHERE w.run_id = ?
        ORDER BY w.wfo_consistency_score DESC LIMIT 10
    """, (run_id,)).fetchall()

    if not rows:
        print("  (no rows)")
        return

    print(f"\n  {'candidate':14s}  {'wfo':7s}  {'boll_len':>8s}  {'boll_mult':>9s}")
    print(f"  {'-'*14}  {'-'*7}  {'-'*8}  {'-'*9}")
    for row in rows:
        cid  = row["candidate_id"][:12]
        wfo  = row["wfo_score"] or 0.0
        try:
            p  = json.loads(row["parameters_json"] or "{}")
            bl = p.get("bollinger_length", "n/a")
            bm = p.get("bollinger_multiplier", "n/a")
        except Exception:
            bl, bm = "err", "err"
        print(f"  {cid:14s}  {wfo:7.4f}  {str(bl):>8s}  {str(bm):>9s}")


# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Backtesting pipeline result analyser")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--section", default="all",
                        help="all | health | summary | stages | stage1 | ga | "
                             "wfo | mc | sensitivity | verdicts | params | bollinger")
    args = parser.parse_args()

    conn = connect(DB_PATH)
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
    if s == "bollinger":               q_bollinger_params(conn, run_id)

    print()
    conn.close()


if __name__ == "__main__":
    main()