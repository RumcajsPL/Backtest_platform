import yaml
import subprocess
import shutil
import tempfile
import json
from datetime import date, datetime
import numpy as np  # For type conversion
import hashlib
from functools import lru_cache
import sys
import os
from pathlib import Path
import pickle

print("=" * 70)
print("🚀 WBWS Backtest Orchestrator - FIXED VERSION")
print("=" * 70)

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import everything
from optimization.parameter_space import ParameterSpace
from optimization.sampler import ParameterSampler
from evaluation.metrics import OptimizationMetrics
from evaluation.fitness import FitnessEvaluator
from evaluation.candidate_store import CandidateStore
from evaluation.ranker import CandidateRanker
from ga.ga_engine import GeneticOptimizer

print("✅ All imports successful")

class HybridCacheManager:
    """Hybrid caching system - simple but effective"""
    
    def __init__(self, cache_dir=".orchestrator_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        
        # Simple memory caches (dictionaries)
        self.data_cache = {}          # For loaded data
        self.yaml_config_cache = {}   # For generated YAML configs (NEW)
        self.yaml_file_cache = {}     # For YAML file paths (NEW)
        self.result_cache = {}        # For strategy results
        self.metric_cache = {}        # For extracted metrics
        
        # Persistent disk cache
        self.disk_cache_file = self.cache_dir / "disk_cache.pkl"
        self.disk_cache = self._load_disk_cache()
        
        # Statistics
        self.hits = 0
        self.misses = 0
    
    def _load_disk_cache(self):
        """Load disk cache if exists"""
        try:
            if self.disk_cache_file.exists():
                with open(self.disk_cache_file, 'rb') as f:
                    return pickle.load(f)
        except Exception as e:
            print(f"⚠️  Could not load disk cache: {e}")
        return {}
    
    def save_disk_cache(self):
        """Save disk cache"""
        try:
            with open(self.disk_cache_file, 'wb') as f:
                pickle.dump(self.disk_cache, f)
        except Exception as e:
            print(f"⚠️  Could not save disk cache: {e}")
    
    def generate_key(self, *args, **kwargs):
        """Generate cache key from arguments"""
        # Convert to JSON string for consistent hashing
        key_data = {
            'args': args,
            'kwargs': kwargs
        }
        key_str = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    @staticmethod
    def disk_cache():
        """Decorator factory for disk caching"""
        def decorator(func):
            def wrapper(self_instance, *args, **kwargs):
                # Note: self_instance is the BacktestOrchestrator instance
                key = self_instance.cache.generate_key(func.__name__, *args, **kwargs)
                
                if key in self_instance.cache.disk_cache:
                    self_instance.cache.hits += 1
                    return self_instance.cache.disk_cache[key]
                
                self_instance.cache.misses += 1
                result = func(self_instance, *args, **kwargs)
                self_instance.cache.disk_cache[key] = result
                return result
            
            return wrapper
        return decorator
    
    @classmethod
    def memory_cache(cls, cache_name="default"):
        """Decorator factory for memory caching with named cache"""
        def decorator(func):
            def wrapper(self_instance, *args, **kwargs):
                # Generate key
                key = self_instance.cache.generate_key(func.__name__, *args, **kwargs)
                
                # Select cache dictionary
                cache_dict = cls._get_cache_dict(self_instance, cache_name)
                
                if key in cache_dict:
                    self_instance.cache.hits += 1
                    return cache_dict[key]
                
                self_instance.cache.misses += 1
                result = func(self_instance, *args, **kwargs)
                cache_dict[key] = result
                return result
            
            return wrapper
        return decorator
    
    @staticmethod
    def _get_cache_dict(instance, cache_name):
        """Helper to get the right cache dictionary"""
        if cache_name == "data":
            return instance.cache.data_cache
        elif cache_name == "yaml_config":
            return instance.cache.yaml_config_cache
        elif cache_name == "yaml_file":
            return instance.cache.yaml_file_cache
        elif cache_name == "result":
            return instance.cache.result_cache
        elif cache_name == "metric":
            return instance.cache.metric_cache
        else:
            return instance.cache.disk_cache  # fallback
    
    def get_stats(self):
        """Get cache statistics"""
        total = self.hits + self.misses
        hit_rate = (self.hits / total * 100) if total > 0 else 0
        
        return {
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": f"{hit_rate:.1f}%",
            "memory_caches": {
                "data": len(self.data_cache),
                "yaml_config": len(self.yaml_config_cache),
                "yaml_file": len(self.yaml_file_cache),
                "result": len(self.result_cache),
                "metric": len(self.metric_cache)
            },
            "disk_cache": len(self.disk_cache)
        }
    
    def print_stats(self):
        """Print cache statistics"""
        stats = self.get_stats()
        print("\n" + "=" * 60)
        print("📊 CACHE STATISTICS")
        print("=" * 60)
        print(f"Hits: {stats['hits']} | Misses: {stats['misses']}")
        print(f"Hit Rate: {stats['hit_rate']}")
        print(f"\nMemory Cache Sizes:")
        print(f"  Data: {stats['memory_caches']['data']}")
        print(f"  YAML Config: {stats['memory_caches']['yaml_config']}")
        print(f"  YAML Files: {stats['memory_caches']['yaml_file']}")
        print(f"  Results: {stats['memory_caches']['result']}")
        print(f"  Metrics: {stats['memory_caches']['metric']}")
        print(f"\nDisk Cache: {stats['disk_cache']} entries")
        print("=" * 60)
    
    def clear_memory(self):
        """Clear all memory caches"""
        self.data_cache.clear()
        self.yaml_config_cache.clear()
        self.yaml_file_cache.clear()
        self.result_cache.clear()
        self.metric_cache.clear()
        print("🧹 Cleared all memory caches")
class BacktestOrchestrator:
    def __init__(self, backtest_yaml_path: str):
        
        print(f"\n🔧 Initializing cache manager")
        self.cache = HybridCacheManager()
        
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
        
        # Load and cache template
        self.strategy_template = self.load_and_clean_template()

    @lru_cache(maxsize=1)
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
        print("🎯 STARTING ORCHESTRATION (WITH HYBRID CACHING)")
        print("=" * 70)
        
        # Clear previous memory cache if needed (optional)
        # self.cache.clear_memory()
        
        # Run optimization
        self.run_full_optimization()
        
        # Print cache statistics
        self.cache.print_stats()
        
        # Save disk cache for next run
        self.cache.save_disk_cache()
        
        print(f"\n💾 Disk cache saved for next run")
        print("=" * 70)
    
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
            
            # Check if random search is enabled
            random_search_config = self.config.get("random_search", {})
            if not random_search_config.get("enabled", True):
                print(f"  ⏭️ Random search disabled, skipping zone {zone_name}")
                continue
            
            n_samples = random_search_config.get("samples_per_zone", 150)
            
            try:
                # Build parameter space and sampler
                space = ParameterSpace(zone_cfg).build()
                sampler = ParameterSampler(space, n_samples=n_samples)
                
                # Get samples for random search
                samples = sampler.random_sample()
                print(f"   Generated {len(samples)} parameter sets for random search")
                
                # Initialize fitness evaluator
                fitness_engine = FitnessEvaluator(
                    constraints=self.config["constraints"],
                    weights=self.config["fitness"]["weights"]
                )
                
                # Initialize candidate store
                store = CandidateStore(zone_dir)
                
                # ========== RANDOM SEARCH PHASE ==========
                print(f"\n   🎯 Starting Random Search Phase")
                random_candidates = []
                successful_random = 0
                
                # Process random samples (limited for testing)
                total_to_process = min(len(samples), 5)  # Process first 5 for testing
                
                for i in range(total_to_process):
                    params = samples[i]
                    print(f"\n   Random Sample {i+1}/{total_to_process}")
                    
                    # Create config file
                    temp_yaml = self.create_temp_yaml(params, zone_name, i, "random")
                    
                    # Run strategy
                    report_path = self.run_strategy(temp_yaml, zone_dir, i)
                    
                    if report_path and report_path.exists():
                        # Extract metrics
                        metrics_extractor = OptimizationMetrics(str(report_path))
                        real_metrics = metrics_extractor.get()
                        
                        # Check constraints
                        if fitness_engine.passes_constraints(real_metrics):
                            score = fitness_engine.score(real_metrics)
                            print(f"    ✅ Passed constraints, score: {score:.4f}")
                            
                            # Store candidate
                            store.add(
                                params=params,
                                metrics=real_metrics,
                                fitness=score,
                                zone_name=zone_name,
                                sample_index=i,
                                source="random"
                            )
                            
                            # Add to list for GA initialization
                            random_candidates.append({
                                'parameters': params,
                                'metrics': real_metrics,
                                'fitness': score
                            })
                            
                            successful_random += 1
                        else:
                            print(f"    ❌ Failed constraints")
                    else:
                        print(f"    ⚠️  Strategy execution failed")
                
                print(f"\n   ✅ Random Search completed")
                print(f"   - Processed: {total_to_process}")
                print(f"   - Successful: {successful_random}")
                
                # ========== GENETIC ALGORITHM PHASE ==========
                if successful_random > 0:
                    ga_results = self.integrate_genetic_algorithm(
                        zone_name=zone_name,
                        zone_cfg=zone_cfg,
                        zone_dir=zone_dir,
                        fitness_engine=fitness_engine,
                        initial_candidates=random_candidates,
                        sampler=sampler
                    )
                    
                    # Store GA candidates
                    if ga_results:
                        for i, (params, score, metrics) in enumerate(ga_results):
                            store.add(
                                params=params,
                                metrics=metrics,
                                fitness=score,
                                zone_name=zone_name,
                                sample_index=i + len(samples),  # Continue numbering
                                source="ga"
                            )
                
                # Save all candidates
                store.save()
                
                # Rank and save top candidates
                if store.candidates:
                    ranker = CandidateRanker(store.candidates)
                    top = ranker.top_n(n=5)
                    
                    top_clean = self.clean_for_json(top)

                    results_file = zone_dir / "top_candidates.json"
                    with open(results_file, 'w') as f:
                        json.dump(top_clean, f, indent=2)
                    
                    print(f"\n   🏆 Final Results:")
                    print(f"   - Total candidates: {len(store.candidates)}")
                    print(f"   - Results saved: {results_file}")
                    
                    # Show top candidates
                    print(f"\n   Top 3 candidates:")
                    for j, candidate in enumerate(top[:3], 1):
                        print(f"   {j}. Source: {candidate.get('source', 'unknown')}")
                        print(f"      Fitness: {candidate.get('fitness', 0):.4f}")
                        print(f"      Net P&L: {candidate.get('metrics', {}).get('net_pnl', 0):.2f}")
                        print(f"      Win Rate: {candidate.get('metrics', {}).get('winrate', 0):.2%}")
                
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
        """Run strategy with result caching"""
        print(f"    ▶ Running strategy with config: {strategy_yaml_path.name}")
        
        # Create cache key from file content
        with open(strategy_yaml_path, 'r', encoding='utf-8') as f:
            content = f.read()
        content_hash = hashlib.md5(content.encode()).hexdigest()
        cache_key = f"strategy_result_{content_hash}"
        
        # Check cache
        if cache_key in self.cache.result_cache:
            cached_report = self.cache.result_cache[cache_key]
            if cached_report.exists():
                print(f"    🔄 Using cached strategy results")
                # Copy to output directory
                report_copy = output_dir / f"report_{strategy_yaml_path.stem}.json"
                if not report_copy.exists():
                    shutil.copy(cached_report, report_copy)
                return report_copy
        
        # Run strategy (your existing code)
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
                encoding='utf-8',
                errors='replace'
            )
                        
            if result.stdout:
                if "ENHANCED STRATEGY EXECUTION COMPLETED" in result.stdout:
                    print(f"    ✅ Strategy completed successfully")
            
            if result.stderr:
                print(f"    ⚠ Stderr: {result.stderr[:500]}...")
                
        except subprocess.CalledProcessError as e:
            print(f"    ❌ Strategy execution failed with exit code: {e.returncode}")
            if e.stdout:
                print(f"    📋 Stdout (last 500 chars): {e.stdout[-500:] if len(e.stdout) > 500 else e.stdout}")
            if e.stderr:
                print(f"    🔴 Stderr (first 500 chars): {e.stderr[:500]}...")
            return None
        except Exception as e:
            print(f"    ❌ Unexpected error running strategy: {e}")
            import traceback
            traceback.print_exc()
            return None

        # Find the latest report
        reports_dir = project_root / "outputs" / "reports" / "WBWS"
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
                
                # Cache the result
                self.cache.result_cache[cache_key] = report_copy
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
    
    def clean_for_json(self, obj):
        """Recursively clean numpy types for JSON serialization"""
        if isinstance(obj, dict):
            return {k: self.clean_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.clean_for_json(v) for v in obj]
        elif isinstance(obj, (np.integer, np.floating)):
            return obj.item()  # Convert to Python int/float
        elif isinstance(obj, np.ndarray):
            return obj.tolist()  # Convert numpy arrays to lists
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat()  # Convert dates to strings
        else:
            return obj
        
    def integrate_genetic_algorithm(self, zone_name, zone_cfg, zone_dir, fitness_engine, 
                                   initial_candidates, sampler):
        """Integrate GA optimization into the pipeline"""
        
        # Check if GA is enabled in config
        ga_config = self.config.get("genetic", {})
        if not ga_config.get("enabled", False):
            print(f"   ⏭️ GA optimization disabled, skipping")
            return []
        
        print(f"\n   🧬 Starting Genetic Algorithm optimization for zone: {zone_name}")
        
        try:
                      
            # Create GA optimizer
            ga_optimizer = GeneticOptimizer(
                sampler=sampler,
                orchestrator=self,
                fitness_engine=fitness_engine,
                config=ga_config,
                zone_name=zone_name,
                zone_dir=zone_dir
            )
            
            # Prepare initial population from best random candidates
            if initial_candidates:
                # Sort by fitness first
                sorted_candidates = sorted(
                    initial_candidates,
                    key=lambda x: x.get('fitness', -1000),
                    reverse=True
                )
                
                # Convert initial candidates to parameter sets
                initial_population = []
                for candidate in sorted_candidates[:10]:  # Take top 10
                    if isinstance(candidate, dict) and 'parameters' in candidate:
                        initial_population.append(candidate['parameters'])
                    elif isinstance(candidate, tuple) and len(candidate) > 0:
                        initial_population.append(candidate[0])  # Assuming (params, score, metrics)
                
                print(f"   Using {len(initial_population)} best candidates as initial population")
            else:
                initial_population = None
                print(f"   No initial candidates, starting with random population")
            
            # Run GA
            ga_results = ga_optimizer.run(initial_population=initial_population)
            
            if ga_results:
                print(f"   ✅ GA completed with {len(ga_results)} candidates")
                
                # Save GA results
                ga_results_file = zone_dir / "ga_results.json"
                ga_summary = []
                
                for i, (params, score, metrics) in enumerate(ga_results):
                    ga_summary.append({
                        "rank": i + 1,
                        "fitness": score,
                        "parameters": params,
                        "metrics_summary": {
                            "net_pnl": metrics.get("net_pnl", 0),
                            "winrate": metrics.get("winrate", 0),
                            "drawdown": metrics.get("drawdown", 0),
                            "expectancy": metrics.get("expectancy", 0)
                        }
                    })
                
                # Clean numpy types before saving
                ga_summary_clean = self.clean_for_json(ga_summary)
                
                with open(ga_results_file, "w") as f:
                    json.dump(ga_summary_clean, f, indent=2)
                
                print(f"   📊 GA results saved to: {ga_results_file}")
                
                # Return GA results
                return ga_results
            
            else:
                print(f"   ⚠️ GA optimization produced no results")
                return []
                
        except Exception as e:
            print(f"   ❌ GA optimization failed: {e}")
            import traceback
            traceback.print_exc()
            return []

    @HybridCacheManager.disk_cache()
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
    
    @HybridCacheManager.memory_cache(cache_name="yaml_config")
    def _generate_yaml_config(self, params: dict, zone_name: str) -> dict:
        """Cached: Pure function that generates YAML config dict"""
        # Start with template
        config = self.strategy_template.copy()
        
        # Apply parameter mapping (already cached via @disk_cache)
        config_updates = self.map_parameters_to_config(params)
        
        # Apply updates to config
        for section, updates in config_updates.items():
            if section not in config:
                config[section] = updates
            elif isinstance(config[section], dict) and isinstance(updates, dict):
                config[section].update(updates)
            else:
                config[section] = updates
        
        # Clean any numpy types
        config = self.clean_numpy_types(config)
        
        return config

    def create_temp_yaml(self, params: dict, zone_name: str, sample_index: int, source: str = "test") -> Path:
        """Create temp YAML file with intelligent caching"""
        print(f"   💾 Creating YAML for {zone_name}_{source}_{sample_index}")
        
        # 1. Generate/retrieve config (cached via decorator)
        config = self._generate_yaml_config(params, zone_name)
        
        # 2. Check for existing file (manual file cache)
        file_key = self.cache.generate_key("yaml_file", zone_name, params, source)
        if file_key in self.cache.yaml_file_cache:
            cached_file = self.cache.yaml_file_cache[file_key]
            if cached_file.exists():
                print(f"   🔄 Using cached YAML file")
                return cached_file
        
        # 3. Convert config to YAML string
        yaml_content = yaml.dump(config, default_flow_style=False, sort_keys=False)
        
        # 4. Create temp file with unique name
        temp_dir = Path(tempfile.gettempdir())
        temp_file = temp_dir / f"wbws_{zone_name}_{source}_{sample_index}.yaml"
        
        with open(temp_file, "w") as f:
            f.write(yaml_content)
        
        # 5. Save a debug copy
        debug_dir = self.base_dir / "debug" / self.timestamp
        debug_dir.mkdir(parents=True, exist_ok=True)
        debug_file = debug_dir / f"{zone_name}_{source}_{sample_index}.yaml"
        
        with open(debug_file, "w") as f:
            f.write(yaml_content)
        
        print(f"   💾 Debug copy: {debug_file.relative_to(self.base_dir)}")
        
        # 6. Cache file path
        self.cache.yaml_file_cache[file_key] = temp_file
        
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