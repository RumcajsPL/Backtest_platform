import sqlite3

conn = sqlite3.connect('outputs/backtesting/backtester.db')
r = conn.execute("""
    SELECT
        COUNT(*) as n,
        ROUND(AVG(net_pnl), 2) as mean,
        ROUND(SQRT(AVG(net_pnl*net_pnl) - AVG(net_pnl)*AVG(net_pnl)), 2) as stdev
    FROM wfo_window_results
    WHERE run_id = '87712cab-1e85-4bd4-a096-25c167cab6cb'
      AND is_ga_fitness_window = 0
      AND net_pnl IS NOT NULL
""").fetchone()
print(f"n={r[0]}  mean={r[1]}  stdev={r[2]}")
conn.close()