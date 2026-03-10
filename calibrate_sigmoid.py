"""
compute_sigmoid_scale.py
Computes _SIGMOID_SCALE from WFO window net_pnl distribution.
Run from project root: python compute_sigmoid_scale.py
"""
import sqlite3
import statistics
from pathlib import Path

DB_PATH = Path("outputs/backtesting/backtester.db")
RUN_ID = "519f84e2-959b-4bcf-b33c-7d647236415d"

if not DB_PATH.exists():
    print(f"ERROR: DB not found at {DB_PATH}")
    raise SystemExit(1)

conn = sqlite3.connect(DB_PATH)

# All net_pnl values from Stage 4 WFO window results for this run
rows = conn.execute("""
    SELECT window_id, net_pnl
    FROM wfo_window_results
    WHERE run_id = ?
    AND net_pnl IS NOT NULL
    ORDER BY window_id
""", (RUN_ID,)).fetchall()

# Also get null count to understand coverage
null_rows = conn.execute("""
    SELECT window_id, evaluation_error, COUNT(*) as cnt
    FROM wfo_window_results
    WHERE run_id = ?
    AND net_pnl IS NULL
    GROUP BY window_id, evaluation_error
    ORDER BY window_id
""", (RUN_ID,)).fetchall()

conn.close()

if not rows:
    print("ERROR: No net_pnl rows found for this run_id.")
    raise SystemExit(1)

vals = [r[1] for r in rows]
n = len(vals)
mean = statistics.mean(vals)
stdev = statistics.stdev(vals)
sigmoid_scale = stdev * 0.5

print("=" * 60)
print("  _SIGMOID_SCALE CALIBRATION")
print("=" * 60)
print(f"  Run ID : {RUN_ID}")
print(f"  N      : {n} net_pnl values")
print(f"  Min    : {min(vals):.2f}")
print(f"  Mean   : {mean:.2f}")
print(f"  Max    : {max(vals):.2f}")
print(f"  Stdev  : {stdev:.2f}")
print(f"")
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