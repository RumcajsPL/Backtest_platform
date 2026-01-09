import subprocess

def test_orchestrator_matches_runner():
    print("Running baseline strategy...")
    subprocess.run([
        "python",
        "scripts/run_wbws_strategy.py",
        "src/config/WBWS/wbws_rsi_strategy.yaml"
    ], check=True)

    print("Running orchestrator...")
    subprocess.run([
        "python",
        "src/backtesting/orchestrator.py",
        "src/config/WBWS/wbws_rsi_strategy.yaml"
    ], check=True)

    print("✔ Orchestrator test executed successfully.")