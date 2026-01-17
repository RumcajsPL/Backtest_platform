"""
Diagnostic tests for the WBWS Backtesting Pipeline
"""
import sys
import os
from pathlib import Path

# Add project root to path - FIXED
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))  # Add src directory

# Now import
import json
import yaml
import subprocess
import tempfile
from datetime import datetime
import shutil

def setup_test_environment():
    """Setup clean test environment"""
    test_dir = project_root / "tests" / "diagnostic_output"
    test_dir.mkdir(parents=True, exist_ok=True)
    
    # Clean previous test outputs
    for item in test_dir.glob("*"):
        if item.is_file():
            item.unlink()
        else:
            shutil.rmtree(item)
    
    return test_dir

def test_failed_ga_parameters_simple():
    """Simple test without orchestrator dependency"""
    print("=" * 70)
    print("🔍 TEST 1: Failed GA Parameters (Isolated - SIMPLE)")
    print("=" * 70)
    
    test_dir = setup_test_environment()
    
    # Exact failed parameters from your GA run
    failed_params = {
        "rsi_overbought": 64,
        "rsi_oversold": 36,
        "htf_timeframe": "1H",
        "atr_length": 11,
        "atr_multiplier": 2.0,  # Simplified
        "max_risk_percentile": 0.09,
        "rr_target": 3.5,
        "session_window": ["07:00", "11:30"]
    }
    
    print(f"\n📊 Testing parameters:")
    for key, value in failed_params.items():
        print(f"  {key}: {value}")
    
    # Create a simple YAML config directly
    config = {
        "strategy": {
            "name": "WBWS Diagnostic Test",
            "version": "1.0.0"
        },
        "asset": {
            "symbol": "DEUIDXEUR",
            "currency": "EUR"
        },
        "data": {
            "format": "csv",
            "file": "Backtest_platform/data/processed/ohlcv/DEUIDXEUR_1min_20240101_20260104.csv",
            "file_htf": "Backtest_platform/data/processed/ohlcv/DEUIDXEUR_1H_20230101_20260104.csv",
            "file_ltf": "Backtest_platform/data/processed/ohlcv/DEUIDXEUR_1s_20240101_20260104.csv",
            "date_range": {
                "start": "2024-01-01",
                "end": "2024-01-07"
            }
        },
        "indicator": {
            "name": "WBWS_Trigger",
            "htf_period": failed_params["htf_timeframe"]
        },
        "filters": {
            "rsi_filter": {
                "enabled": True,
                "length": 14,
                "overbought": failed_params["rsi_overbought"],
                "oversold": failed_params["rsi_oversold"]
            }
        },
        "trade_management": {
            "time_filter": {
                "enabled": True,
                "session_start": {
                    "hour": int(failed_params["session_window"][0].split(":")[0]),
                    "minute": int(failed_params["session_window"][0].split(":")[1])
                },
                "session_end": {
                    "hour": int(failed_params["session_window"][1].split(":")[0]),
                    "minute": int(failed_params["session_window"][1].split(":")[1])
                }
            },
            "sl_tp": {
                "enabled": True,
                "atr_length": failed_params["atr_length"],
                "sl_multiplier": failed_params["atr_multiplier"],
                "risk_to_reward_ratio": failed_params["rr_target"]
            },
            "risk_management": {
                "enabled": True,
                "max_risk_percentile": failed_params["max_risk_percentile"],
                "allow_exceed_limit": False
            }
        },
        "output": {
            "outputs_dir": "outputs",
            "reports_dir": "reports/WBWS",
            "save_signals_csv": True
        }
    }
    
    # Save config
    config_file = test_dir / "test_config.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    
    print(f"\n📄 Created config file: {config_file}")
    
    # Run strategy
    print(f"\n▶ Running strategy...")
    
    cmd = [
        sys.executable,
        "-X", "utf8",
        "scripts/run_wbws_strategy.py",
        str(config_file)
    ]
    
    env = os.environ.copy()
    env['PYTHONIOENCODING'] = 'utf-8'
    
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            cwd=str(project_root),
            env=env,
            encoding='utf-8',
            errors='replace',
            timeout=120  # 2 minute timeout
        )
        
        print(f"\n✅ Strategy execution SUCCESSFUL")
        print(f"  Exit code: {result.returncode}")
        
        # Look for success message in output
        if "ENHANCED STRATEGY EXECUTION COMPLETED" in result.stdout:
            print("  Strategy completed message found")
        
        # Look for report file
        reports_dir = project_root / "outputs" / "reports" / "WBWS"
        if reports_dir.exists():
            json_files = list(reports_dir.glob("strategy_report_*.json"))
            if json_files:
                latest = max(json_files, key=lambda f: f.stat().st_mtime)
                print(f"\n📊 Report generated: {latest}")
                
                with open(latest, 'r') as f:
                    report = json.load(f)
                
                print(f"\n📈 Metrics extracted:")
                print(f"  Total trades: {report.get('total_trades', 'N/A')}")
                print(f"  Net P&L: {report.get('net_pnl', 'N/A'):.2f}")
                print(f"  Win rate: {report.get('winrate', 'N/A'):.2%}")
                
                # Save the successful config
                success_dir = test_dir / "success"
                success_dir.mkdir(exist_ok=True)
                shutil.copy(config_file, success_dir / "working_config.yaml")
                shutil.copy(latest, success_dir / "report.json")
                
                return True
            else:
                print(f"\n⚠️  No report files found in {reports_dir}")
                return False
        else:
            print(f"\n⚠️  Reports directory not found: {reports_dir}")
            return False
            
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Strategy execution FAILED")
        print(f"  Exit code: {e.returncode}")
        
        # Save error details
        error_dir = test_dir / "errors"
        error_dir.mkdir(exist_ok=True)
        
        error_file = error_dir / "strategy_error.txt"
        with open(error_file, 'w', encoding='utf-8') as f:  # Just add encoding='utf-8'
            f.write("=" * 50 + "\n")
            f.write("COMMAND:\n")
            f.write("=" * 50 + "\n")
            f.write(" ".join(cmd))
            f.write("\n\n" + "=" * 50 + "\n")
            f.write("STDOUT:\n")
            f.write("=" * 50 + "\n")
            f.write(e.stdout if e.stdout else "(empty)")  # Line 206 - will now work
            f.write("\n\n" + "=" * 50 + "\n")
            f.write("STDERR:\n")
            f.write("=" * 50 + "\n")
            f.write(e.stderr if e.stderr else "(empty)")
                
        print(f"\n📝 Error details saved to: {error_file}")
        
        if e.stdout:
            print(f"\n📋 Stdout (last 500 chars):")
            print(e.stdout[-500:] if len(e.stdout) > 500 else e.stdout)
        
        if e.stderr:
            print(f"\n🔴 Stderr (first 500 chars):")
            stderr_preview = e.stderr[:500] if len(e.stderr) > 500 else e.stderr
            print(stderr_preview)
            
            # Check for common errors
            if "ModuleNotFoundError" in stderr_preview:
                print("\n💡 Module import error detected!")
            if "FileNotFoundError" in stderr_preview:
                print("\n💡 File not found error!")
            if "KeyError" in stderr_preview:
                print("\n💡 Missing key in configuration!")
        
        # Save the failing config
        fail_dir = test_dir / "failed_configs"
        fail_dir.mkdir(exist_ok=True)
        shutil.copy(config_file, fail_dir / "failed_config.yaml")
        
        return False
    
    except subprocess.TimeoutExpired:
        print(f"\n⏱️  Strategy execution TIMED OUT after 2 minutes")
        return False

def analyze_existing_data():
    """Analyze existing data without running new strategies"""
    print("\n" + "=" * 70)
    print("🔍 TEST 2: Analysis of Existing Data")
    print("=" * 70)
    
    # Look for candidates.json in latest run
    backtests_dir = project_root / "outputs" / "backtests"
    
    if not backtests_dir.exists():
        print("❌ No backtest outputs found")
        return False
    
    # Find all runs
    all_runs = []
    for zone in backtests_dir.iterdir():
        if zone.is_dir():
            for run in zone.iterdir():
                if run.is_dir():
                    all_runs.append(run)
    
    if not all_runs:
        print("❌ No backtest runs found")
        return False
    
    print(f"\n📊 Found {len(all_runs)} backtest runs")
    
    # Analyze the most recent run
    latest_run = max(all_runs, key=lambda p: p.stat().st_mtime)
    print(f"\n📅 Analyzing latest run: {latest_run.relative_to(backtests_dir)}")
    
    # Check for candidates.json
    candidates_file = latest_run / "candidates.json"
    
    if not candidates_file.exists():
        print(f"❌ candidates.json not found in {latest_run}")
        return False
    
    with open(candidates_file, 'r') as f:
        candidates = json.load(f)
    
    successful = [c for c in candidates if c.get('fitness', -1000) > -1000]
    failed = [c for c in candidates if c.get('fitness', -1000) == -1000]
    
    print(f"\n📈 Run Statistics:")
    print(f"  Total candidates: {len(candidates)}")
    print(f"  Successful: {len(successful)} ({len(successful)/len(candidates)*100:.1f}%)")
    print(f"  Failed: {len(failed)} ({len(failed)/len(candidates)*100:.1f}%)")
    
    if failed:
        print(f"\n❌ FAILED CANDIDATES ANALYSIS:")
        
        # Show first few failed candidates
        for i, candidate in enumerate(failed[:3]):
            print(f"\n  Failed candidate #{i+1}:")
            print(f"    Source: {candidate.get('source', 'unknown')}")
            print(f"    Sample index: {candidate.get('sample_index', 'unknown')}")
            
            params = candidate.get('params', {})
            if params:
                print(f"    Parameters:")
                for key, value in params.items():
                    print(f"      {key}: {value}")
            
            metrics = candidate.get('metrics', {})
            print(f"    Metrics: {'Empty' if not metrics else f'{len(metrics)} metrics'}")
    
    if successful:
        print(f"\n✅ SUCCESSFUL CANDIDATES ANALYSIS:")
        
        # Show top 3 successful candidates
        top_successful = sorted(successful, key=lambda x: x.get('fitness', -1000), reverse=True)[:3]
        
        for i, candidate in enumerate(top_successful):
            print(f"\n  Top candidate #{i+1}:")
            print(f"    Fitness: {candidate.get('fitness', 0):.2f}")
            print(f"    Source: {candidate.get('source', 'unknown')}")
            
            metrics = candidate.get('metrics', {})
            if metrics:
                print(f"    Trades: {metrics.get('total_trades', 'N/A')}")
                print(f"    Net P&L: {metrics.get('net_pnl', 'N/A'):.2f}")
                print(f"    Win rate: {metrics.get('winrate', 'N/A'):.2%}")
    
    return True

def check_system_environment():
    """Check Python environment and dependencies"""
    print("\n" + "=" * 70)
    print("🔍 TEST 3: System Environment Check")
    print("=" * 70)
    
    checks = []
    
    # Check Python version
    python_version = sys.version_info
    checks.append(f"Python version: {python_version.major}.{python_version.minor}.{python_version.micro}")
    
    # Check working directory
    cwd = Path.cwd()
    checks.append(f"Working directory: {cwd}")
    
    # Check project structure
    required_dirs = ["src", "scripts", "outputs", "data"]
    for dir_name in required_dirs:
        dir_path = project_root / dir_name
        checks.append(f"{dir_name}/: {'✅ Exists' if dir_path.exists() else '❌ Missing'}")
    
    # Check for common import issues
    try:
        import pandas
        checks.append(f"pandas: ✅ {pandas.__version__}")
    except ImportError:
        checks.append("pandas: ❌ Not installed")
    
    try:
        import numpy
        checks.append(f"numpy: ✅ {numpy.__version__}")
    except ImportError:
        checks.append("numpy: ❌ Not installed")
    
    try:
        import yaml
        checks.append("PyYAML: ✅ Installed")
    except ImportError:
        checks.append("PyYAML: ❌ Not installed")
    
    # Print all checks
    for check in checks:
        print(f"  {check}")
    
    return True

def main():
    """Run all diagnostic tests"""
    print("🚀 WBWS Pipeline Diagnostic Tests")
    print("=" * 70)
    
    # Create test output directory
    test_output_dir = project_root / "tests" / "diagnostic_output"
    test_output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Output directory: {test_output_dir}")
    
    # Run tests
    results = {}
    
    # Test 1: Failed parameters (simple version)
    print("\n" + "=" * 70)
    try:
        results['test1'] = test_failed_ga_parameters_simple()
    except Exception as e:
        print(f"❌ Test 1 failed with exception: {e}")
        import traceback
        traceback.print_exc()
        results['test1'] = False
    
    # Test 2: Existing data analysis
    print("\n" + "=" * 70)
    try:
        results['test2'] = analyze_existing_data()
    except Exception as e:
        print(f"❌ Test 2 failed with exception: {e}")
        results['test2'] = False
    
    # Test 3: System environment
    print("\n" + "=" * 70)
    try:
        results['test3'] = check_system_environment()
    except Exception as e:
        print(f"❌ Test 3 failed with exception: {e}")
        results['test3'] = False
    
    # Summary
    print("\n" + "=" * 70)
    print("📋 DIAGNOSTIC SUMMARY")
    print("=" * 70)
    
    for test_name, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{test_name}: {status}")
    
    # Save summary
    summary_file = test_output_dir / "diagnostic_summary.json"
    with open(summary_file, 'w') as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "results": results,
            "test_output_dir": str(test_output_dir)
        }, f, indent=2)
    
    print(f"\n📄 Summary saved to: {summary_file}")
    
    if all(results.values()):
        print("\n🎉 All diagnostic tests passed!")
    else:
        print("\n⚠️  Some tests failed. Check the output above for details.")
        
    # Recommendations based on results
    print("\n" + "=" * 70)
    print("💡 RECOMMENDATIONS")
    print("=" * 70)
    
    if not results.get('test1', False):
        print("1. The failed GA parameters work in isolation - issue is in GA context")
        print("   → Check for race conditions or resource exhaustion in GA")
    
    if results.get('test2', False):
        print("2. Analyze failure patterns in existing data")
        print("   → Look for common parameter combinations in failed candidates")
    
    print("3. Consider adding retry logic for failed strategy executions")
    print("4. Add better error logging to capture why strategies fail")

if __name__ == "__main__":
    main()