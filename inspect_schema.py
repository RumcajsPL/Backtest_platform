import sqlite3
conn = sqlite3.connect('outputs/backtesting/backtester.db')
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
for t in tables:
    name = t[0]
    print(f"\n=== {name} ===")
    cols = conn.execute(f"PRAGMA table_info({name})").fetchall()
    for c in cols:
        print(f"  {c[1]:35s} {c[2]}")
conn.close()