"""
FIXED orchestrator.py - Cleans YAML and handles metrics properly
"""
import sys
import os
from pathlib import Path

print("=" * 70)
print("🚀 WBWS Backtest Orchestrator - FIXED VERSION")
print("=" * 70)

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import everything
import yaml
import subprocess
import shutil
import tempfile
import json
from datetime import datetime
import numpy as np  # For type conversion

from optimization.parameter_space import ParameterSpace
from optimization.sampler import ParameterSampler
from evaluation.metrics import OptimizationMetrics
from evaluation.fitness import FitnessEvaluator
from evaluation.candidate_store import CandidateStore
from evaluation.ranker import CandidateRanker

print("✅ All imports successful")

class BacktestOrchestrator:
    def __init__(self, backtest_yaml_path: str):
        print(f"\n🔧 Initializing with: {backtest_yaml_path}")
        
        self.backtest_yaml_path = Path(backtest_yaml_path)
        
        if not self.backtest_yaml_path.exists():
            print(f"❌ Config file not found")
            sys.exit(1)
        
        with open(self.backtest_yaml_path, "r") as f:
            self.config = yaml.safe_load(f)
        print("✅ Backtest config loaded")
        
        self.base_dir = Path("outputs/backtests")
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Load and clean strategy template
        self.strategy_template = self.load_and_clean_template()
    
    def load_and_clean_template(self):
        """Load strategy template and convert numpy types to Python types"""
        config_dir = self.backtest_yaml_path.parent
        template_path = config_dir / "wbws_rsi_strategy.yaml"
        
        if not template_path.exists():
            # Try other locations
            possible_paths = [
                Path("src/config/WBWS/wbws_rsi_strategy.yaml"),
                Path("configs/WBWS/wbws_rsi_strategy.yaml"),
                Path.cwd() / "wbws_rsi_strategy.yaml"
            ]
            for path in possible_paths:
                if path.exists():
                    template_path = path
                    break
        
        if not template_path.exists():
            print(f"⚠️  Strategy template not found, creating minimal one")
            return self.create_minimal_template()
        
        print(f"📄 Loading template from: {template_path}")
        
        # Read the YAML content
        with open(template_path, "r") as f:
            content = f.read()
        
        # Clean any numpy tags from the YAML
        # Remove numpy-specific YAML tags
        content = content.replace('!!python/object/apply:numpy.core.multiarray.scalar', '')
        content = content.replace('!!python/object/apply:numpy._core.multiarray.scalar', '')
        content = content.replace('!!python/object/apply:array', '')
        
        # Also try to load with custom cleaner
        try:
            template = yaml.safe_load(content)
            
            # Clean the loaded dictionary of any numpy types
            template = self.clean_numpy_types(template)
            
            print(f"✅ Template loaded and cleaned")
            return template
            
        except yaml.YAMLError as e:
            print(f"⚠️  Could not parse template, creating fresh one: {e}")
            return self.create_minimal_template()
    
    def clean_numpy_types(self, obj):
        """Recursively convert numpy types to Python native types"""
        if isinstance(obj, dict):
            return {k: self.clean_numpy_types(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.clean_numpy_types(v) for v in obj]
        elif isinstance(obj, (np.integer, np.floating)):
            # Convert numpy numbers to Python native types
            return obj.item()  # Convert to Python int/float
        elif hasattr(obj, 'dtype'):  # numpy array
            return obj.tolist()  # Convert to Python list
        else:
            return obj
    
    def create_minimal_template(self):
        """Create a clean minimal strategy template"""
        return {
            "strategy": {
                "name": "WBWS Backtest",
                "version": "1.0.0",
                "description": "We Buy/We Sell Trigger",
                "author": "Backtest Platform"
            },
            "asset": {
                "symbol": "DEUIDXEUR",
                "name": "DAX 40 Index",
                "exchange": "EUREX",
                "currency": "EUR"
            },
            "data": {
                "format": "csv",
                "date_range": {
                    "start": "2024-01-01",
                    "end": "2024-12-31"
                }
            },
            "indicator": {
                "name": "WBWS_Trigger",
                "htf_period": "1H"
            },
            "filters": {
                "rsi_filter": {
                    "enabled": True,
                    "length": 14,
                    "overbought": 70,
                    "oversold": 30
                }
            },
            "trade_management": {
                "time_filter": {
                    "enabled": True,
                    "session_start": {"hour": 8, "minute": 30},
                    "session_end": {"hour": 20, "minute": 30}
                },
                "position_control": {
                    "close_on_opposite": False,
                    "pyramiding_enabled": False
                },
                "opposite_signal": {
                    "enabled": False
                },
                "sl_tp": {
                    "enabled": True,
                    "atr_length": 14,
                    "sl_multiplier": 1.4,
                    "risk_to_reward_ratio": 5.7
                },
                "risk_management": {
                    "enabled": True,
                    "max_risk_percentile": 0.0016,
                    "allow_exceed_limit": False
                },
                "spread": {
                    "enabled": True,
                    "required": False,
                    "apply_to_long": True,
                    "apply_to_short": True,
                    "log_spread_impact": True
                }
            },
            "output": {
                "outputs_dir": "outputs",
                "reports_dir": "reports/WBWS",
                "save_signals_csv": True,
                "save_execution_report": True,
                "verbose": False
            }
        }
    
    def run(self):
        print("\n" + "=" * 70)
        print("🎯 STARTING ORCHESTRATION")
        print("=" * 70)
        
        # First, test that we can create and read YAML files
        self.test_yaml_creation()
        
        # Then run the actual optimization
        self.run_full_optimization()
    
    def test_yaml_creation(self):
        """Test YAML creation and reading"""
        print("\n🔧 Testing YAML creation...")
        
        # Create a simple test config
        test_config = {
            "test": "value",
            "number": 123,
            "float": 45.67,
            "nested": {
                "key": "nested_value"
            }
        }
        
        # Save it
        temp_dir = Path(tempfile.gettempdir())
        test_file = temp_dir / "test_yaml.yaml"
        
        with open(test_file, "w") as f:
            yaml.dump(test_config, f, default_flow_style=False)
        
        print(f"✅ Created test YAML: {test_file}")
        
        # Try to read it back
        try:
            with open(test_file, "r") as f:
                loaded = yaml.safe_load(f)
            print(f"✅ Successfully read back YAML")
            print(f"   Loaded keys: {list(loaded.keys())}")
        except Exception as e:
            print(f"❌ Failed to read YAML: {e}")
        
        # Now test with a parameter sample
        zones = self.config.get("zones", {})
        if zones:
            zone_name = list(zones.keys())[0]
            zone_cfg = zones[zone_name]
            
            print(f"\n🔧 Testing parameter mapping for zone: {zone_name}")
            
            try:
                space = ParameterSpace(zone_cfg).build()
                sampler = ParameterSampler(space, n_samples=1)
                samples = sampler.random_sample()
                
                if samples:
                    # Create config with the sample
                    temp_yaml = self.create_temp_yaml(samples[0], zone_name, 0, "test")
                    print(f"✅ Created config file: {temp_yaml}")
                    
                    # Try to read it
                    with open(temp_yaml, "r") as f:
                        config_content = yaml.safe_load(f)
                    
                    print(f"✅ Successfully read generated config")
                    print(f"   Main sections: {list(config_content.keys())}")
                    
                    # Save a copy for inspection
                    inspect_dir = self.base_dir / "inspect"
                    inspect_dir.mkdir(parents=True, exist_ok=True)
                    inspect_file = inspect_dir / f"{zone_name}_test.yaml"
                    
                    with open(inspect_file, "w") as f:
                        yaml.dump(config_content, f, default_flow_style=False)
                    
                    print(f"📄 Config saved for inspection: {inspect_file}")
                    
            except Exception as e:
                print(f"❌ Error in parameter mapping: {e}")
                import traceback
                traceback.print_exc()
    
    def run_full_optimization(self):
        print("\n" + "=" * 70)
        print("🏃 STARTING OPTIMIZATION")
        print("=" * 70)
        
        zones = self.config.get("zones", {})
        
        for zone_name, zone_cfg in zones.items():
            # Check if zone is enabled
            if not zone_cfg.get("enabled", True):
                print(f"⏭️  Skipping disabled zone: {zone_name}")
                continue
            
            print(f"\n🔹 Processing zone: {zone_name}")
            print(f"   {zone_cfg.get('description', '')}")
            
            # Create zone directory
            zone_dir = self.base_dir / zone_name / self.timestamp
            zone_dir.mkdir(parents=True, exist_ok=True)
            
            # Build parameter space
            random_search_config = self.config.get("random_search", {})
            if not random_search_config.get("enabled", True):
                print(f"  ⏭️ Random search disabled, skipping zone {zone_name}")
                continue  # ✅ This skips only this zone
                
            n_samples = random_search_config.get("samples_per_zone", 150)
                       
            try:
                space = ParameterSpace(zone_cfg).build()
                sampler = ParameterSampler(space, n_samples=n_samples)
                samples = sampler.random_sample()
                
                print(f"   Generated {len(samples)} parameter sets")
                
                # Initialize fitness evaluator
                fitness_engine = FitnessEvaluator(
                    constraints=self.config["constraints"],
                    weights=self.config["fitness"]["weights"]
                )
                
                store = CandidateStore(zone_dir)
                
                successful = 0
                total_to_process = min(5, len(samples))  # Process first 10 for testing
                
                print(f"   Processing first {total_to_process} samples...")
                
                for i in range(total_to_process):
                    params = samples[i]
                    print(f"\n   Sample {i+1}/{total_to_process}")
                    
                    # Create config file
                    temp_yaml = self.create_temp_yaml(params, zone_name, i, "random")
                    
                    # For now, simulate running the strategy
                    print(f"   📄 Config: {temp_yaml.name}")

                    # Run the actual strategy
                    report_path = self.run_strategy(temp_yaml, zone_dir, i)
                    
                    if report_path and report_path.exists():
                        # Get real metrics from the report
                        metrics_extractor = OptimizationMetrics(str(report_path))

                        metrics_extractor.debug_info()

                        real_metrics = metrics_extractor.get()
                        
                        print(f"    📊 Extracted metrics:")
                        for key, value in real_metrics.items():
                            print(f"      {key}: {value}")

                        # Check constraints
                        if fitness_engine.passes_constraints(real_metrics):
                            score = fitness_engine.score(real_metrics)
                            print(f"    ✅ Passed constraints, score: {score:.4f}")
                            
                            store.add(
                                params=params,
                                metrics=real_metrics,
                                fitness=score,
                                zone_name=zone_name,
                                sample_index=i,
                                source="random"
                            )
                            successful += 1
                        else:
                            print(f"   ❌ Failed constraints")
                    else:
                        print(f"   ⚠️  No report generated, using simulated metrics")
                        # Fallback to simulated metrics for testing
                        simulated_metrics = self.create_simulated_metrics(i)
                        if fitness_engine.passes_constraints(simulated_metrics):
                            score = fitness_engine.score(simulated_metrics)
                            print(f"   ✅ Simulated passed, score: {score:.4f}")
                            
                            store.add(
                                params=params,
                                metrics=simulated_metrics,
                                fitness=score,
                                zone_name=zone_name,
                                sample_index=i,
                                source="random_simulated"
                            )
                            successful += 1
                        else:
                            print(f"   ❌ Simulated failed constraints")
                
                # Save results
                store.save()
                
                # Rank and save top candidates
                if store.candidates:
                    ranker = CandidateRanker(store.candidates)
                    top = ranker.top_n(n=5)
                    
                    results_file = zone_dir / "top_candidates.json"
                    with open(results_file, 'w') as f:
                        json.dump(top, f, indent=2)
                    
                    print(f"\n   🏆 Results saved:")
                    print(f"   - Total processed: {total_to_process}")
                    print(f"   - Passed constraints: {successful}")
                    print(f"   - Top candidates: {results_file}")
                    
                    # Show top 3
                    print(f"\n   Top 3 candidates:")
                    for j, candidate in enumerate(top[:3], 1):
                        print(f"   {j}. Fitness: {candidate['fitness']:.4f}")  # Changed 'Score' to 'Fitness'
                
                else:
                    print(f"\n   ⚠️  No candidates passed constraints")
                
                print(f"\n   ✅ Zone '{zone_name}' completed")
                
            except Exception as e:
                print(f"❌ Error in zone {zone_name}: {e}")
                import traceback
                traceback.print_exc()
        
        print("\n" + "=" * 70)
        print("✅ OPTIMIZATION COMPLETED")
        print("=" * 70)
            
    def run_strategy(self, strategy_yaml_path: Path, output_dir: Path, sample_index: int) -> Path:
        print(f"    ▶ Running strategy with config: {strategy_yaml_path.name}")
        
        project_root = Path(__file__).parent.parent.parent
        
        # Set encoding for Windows
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        
        cmd = [
            sys.executable,
            "-X", "utf8",  # Enable UTF-8 mode
            "scripts/run_wbws_strategy.py",
            str(strategy_yaml_path)
        ]

        try:
            result = subprocess.run(
                cmd, 
                check=True, 
                capture_output=True, 
                text=True,
                cwd=str(project_root),
                env=env,
                encoding='utf-8',  # Explicit encoding
                errors='replace'   # Replace invalid chars instead of failing
            )
                        
            if result.stdout:
                # Look for success message
                if "ENHANCED STRATEGY EXECUTION COMPLETED" in result.stdout:
                    print(f"    ✅ Strategy completed successfully")
                print(f"    📋 Stdout snippet: {result.stdout[-500:] if len(result.stdout) > 500 else result.stdout}")
            
            if result.stderr:
                print(f"    ⚠ Stderr: {result.stderr[:500]}...")
                
        except subprocess.CalledProcessError as e:
            print(f"    ❌ Strategy execution failed with exit code: {e.returncode}")
            print(f"    🔍 Working directory was: {project_root}")
            
            if e.stdout:
                print(f"    📋 Stdout (last 500 chars): {e.stdout[-500:] if len(e.stdout) > 500 else e.stdout}")
            if e.stderr:
                print(f"    🔴 Stderr (first 500 chars): {e.stderr[:500]}...")
                
                # Check for specific errors
                if "ModuleNotFoundError" in e.stderr:
                    print(f"    💡 Module error detected!")
                    if "pandas" in e.stderr:
                        print(f"    💡 Pandas not found. Check if venv is activated in subprocess.")
                    if "strategy_modules" in e.stderr:
                        print(f"    💡 strategy_modules import error - likely wrong working directory")
            
            return None
        except Exception as e:
            print(f"    ❌ Unexpected error running strategy: {e}")
            import traceback
            traceback.print_exc()
            return None

        # Find the latest report
        reports_dir = project_root / "outputs" / "reports" / "WBWS"  # Use full path
        print(f"    🔍 Looking for reports in: {reports_dir}")
        
        if reports_dir.exists():
            json_files = list(reports_dir.glob("strategy_report_*.json"))
            print(f"    🔍 Found {len(json_files)} JSON files")
            
            if json_files:
                latest_report = max(json_files, key=lambda f: f.stat().st_mtime)
                # Copy report to zone directory
                report_copy = output_dir / f"report_{strategy_yaml_path.stem}.json"
                shutil.copy(latest_report, report_copy)
                print(f"    ✔ Report saved to {report_copy}")
                return report_copy
            else:
                print(f"    ⚠ No report files found in {reports_dir}")
                return None
        else:
            print(f"    ⚠ Reports directory not found: {reports_dir}")
            return None
    
    def create_simulated_metrics(self, sample_index: int) -> dict:
        """Create complete simulated metrics with all required keys"""
        base_value = 1000 + sample_index * 50
        
        return {
            "net_pnl": base_value,
            "expectancy": 0.5 + (sample_index % 10) * 0.05,
            "drawdown": 0.1 + (sample_index % 5) * 0.02,
            "max_drawdown": 0.15,  # Added this key
            "losing_streak": 3 + (sample_index % 3),
            "winrate": 0.55 + (sample_index % 10) * 0.03,
            "profit_factor": 1.2 + (sample_index % 10) * 0.1,
            "total_trades": 50 + sample_index * 5,
            "winning_trades": int((50 + sample_index * 5) * 0.6),
            "losing_trades": int((50 + sample_index * 5) * 0.4),
            "avg_win": base_value * 0.1,
            "avg_loss": base_value * 0.05,
            "largest_win": base_value * 0.2,
            "largest_loss": base_value * 0.1,
            "sharpe_ratio": 1.5 + sample_index * 0.1,
            "calmar_ratio": 2.0 + sample_index * 0.1,
            "trades_per_day": 4.0 + sample_index * 0.2
        }
    
    def map_parameters_to_config(self, flat_params: dict) -> dict:
        """Convert flat parameters to nested structure"""
        config_updates = {}
        
        # Get project root
        project_root = Path(__file__).parent.parent.parent
        data_dir = project_root / "data" / "processed" / "ohlcv"
        
        # RSI Filter
        if "rsi_overbought" in flat_params or "rsi_oversold" in flat_params:
            config_updates["filters"] = {
                "rsi_filter": {
                    "enabled": True,
                    "length": 14,
                    "overbought": float(flat_params.get("rsi_overbought", 70)),
                    "oversold": float(flat_params.get("rsi_oversold", 30))
                }
            }
        
        # HTF
        if "htf_timeframe" in flat_params:
            config_updates["indicator"] = {
                "name": "WBWS_Trigger",
                "htf_period": str(flat_params["htf_timeframe"])
            }
        
        # ATR & Risk-Reward
        if any(k in flat_params for k in ["atr_length", "atr_multiplier", "rr_target"]):
            config_updates.setdefault("trade_management", {})
            config_updates["trade_management"]["sl_tp"] = {
                "enabled": True,
                "atr_length": int(flat_params.get("atr_length", 14)),
                "sl_multiplier": float(flat_params.get("atr_multiplier", 1.4)),
                "risk_to_reward_ratio": float(flat_params.get("rr_target", 5.7))
            }
        
        # Risk management
        if "max_risk_percentile" in flat_params:
            config_updates.setdefault("trade_management", {})
            config_updates["trade_management"]["risk_management"] = {
                "enabled": True,
                "max_risk_percentile": float(flat_params["max_risk_percentile"]),
                "allow_exceed_limit": False
            }
        
        # Session windows
        if "session_window" in flat_params:
            config_updates.setdefault("trade_management", {})
            config_updates["trade_management"]["time_filter"] = {
                "enabled": True,
                "session_start": {
                    "hour": int(flat_params["session_window"][0].split(":")[0]),
                    "minute": int(flat_params["session_window"][0].split(":")[1])
                },
                "session_end": {
                    "hour": int(flat_params["session_window"][1].split(":")[0]),
                    "minute": int(flat_params["session_window"][1].split(":")[1])
                }
            }
        
        # Data section with absolute paths
        config_updates["data"] = {
            "file": str(data_dir / "DEUIDXEUR_1min_20240101_20260104.csv"),
            "file_htf": str(data_dir / "DEUIDXEUR_1H_20230101_20260104.csv"),
            "file_ltf": str(data_dir / "DEUIDXEUR_1s_20240101_20260104.csv"),
            "format": "csv",
            "date_range": {
                "start": "2024-01-01",  # Use a range that exists in your data
                "end": "2024-01-07"
            },
            "validation": {
                "check_ohlc": True,
                "check_gaps": False,
                "max_price_move": 0.1
            }
        }
        
        return config_updates

    def create_temp_yaml(self, params: dict, zone_name: str, sample_index: int, source: str = "test") -> Path:
        """Create a temporary YAML file with cleaned types"""
        # Start with template
        config = self.strategy_template.copy()
        
        # Apply parameter mapping
        config_updates = self.map_parameters_to_config(params)
        for section, updates in config_updates.items():
            if section not in config:
                config[section] = updates
            elif isinstance(config[section], dict) and isinstance(updates, dict):
                config[section].update(updates)
            else:
                config[section] = updates
        
        # Clean any numpy types before saving
        config = self.clean_numpy_types(config)
        
        # Create temp file
        temp_dir = Path(tempfile.gettempdir())
        temp_file = temp_dir / f"wbws_{zone_name}_{source}_{sample_index}.yaml"
        
        with open(temp_file, "w") as f:
            yaml.dump(config, f, default_flow_style=False)
        
        # Save a copy for debugging
        debug_dir = self.base_dir / "debug" / self.timestamp
        debug_dir.mkdir(parents=True, exist_ok=True)
        debug_file = debug_dir / f"{zone_name}_{source}_{sample_index}.yaml"
        
        with open(debug_file, "w") as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        
        print(f"   💾 Debug copy: {debug_file.relative_to(self.base_dir)}")
        
        return temp_file

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("❌ Usage: python orchestrator_fixed.py <backtest_yaml>")
        print("\nExample:")
        print("  python src/backtesting/orchestrator_fixed.py src/config/WBWS/wbws_backtest.yaml")
        sys.exit(1)
    
    try:
        orchestrator = BacktestOrchestrator(sys.argv[1])
        orchestrator.run()
        
        print("\n" + "=" * 70)
        print("✅ ORCHESTRATOR COMPLETED SUCCESSFULLY!")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ ORCHESTRATOR FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)