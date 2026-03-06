import sqlite3

conn = sqlite3.connect('outputs/backtesting/backtester.db')
conn.row_factory = sqlite3.Row

print('=== QUERY 1 — Rejection breakdown ===')
rows = conn.execute("""
    SELECT passed_constraints, rejection_reason, failing_constraint, COUNT(*) as count
    FROM evaluations
    WHERE run_id = 'e5675094-0cbe-4a6e-bc41-1bdab461521a'
      AND stage = 'RANDOM'
    GROUP BY passed_constraints, rejection_reason, failing_constraint
    ORDER BY count DESC
""").fetchall()
for r in rows:
    print(dict(r))

print()
print('=== QUERY 2 — Top 15 closest to passing ===')
rows = conn.execute("""
    SELECT actual_win_rate, actual_max_drawdown, actual_expectancy,
           actual_profit_factor, actual_trades_per_week, actual_losing_streak,
           failing_constraint, failing_value, fitness_score
    FROM evaluations
    WHERE run_id = 'e5675094-0cbe-4a6e-bc41-1bdab461521a'
      AND stage = 'RANDOM'
      AND passed_constraints = 0
    ORDER BY fitness_score DESC
    LIMIT 15
""").fetchall()
for r in rows:
    print(dict(r))

conn.close()