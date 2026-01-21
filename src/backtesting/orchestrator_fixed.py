import yaml
import subprocess
import shutil
import tempfile
import json
from datetime import date, datetime
import numpy as np
import hashlib
from functools import lru_cache
import sys
import os
from pathlib import Path
import pickle
from typing import List, Dict, Tuple, Optional
import concurrent.futures
from threading import Lock, RLock
import psutil  # For resource monitoring
import time

print("=" * 70)
print("🚀 WBWS Backtest Orchestrator - PRODUCTION VERSION")
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

class ThreadSafeCache:
    """Thread-safe cache wrapper for parallel execution"""
    
    def __init__(self):
        self.lock = RLock()  # Reentrant lock for nested access
        self._cache = {}
    
    def get(self, key, default=None):
        with self.lock:
            return self._cache.get(key, default)
    
    def set(self, key, value):
        with self.lock:
            self._cache[key] = value
    
    def __contains__(self, key):
        with self.lock:
            return key in self._cache
    
    def __len__(self):
        with self.lock:
            return len(self._cache)
    
    def clear(self):
        with self.lock:
            self._cache.clear()

class HybridCacheManager:
    """Enhanced hybrid caching system with thread safety"""
    
    def __init__(self, cache_dir=".orchestrator_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        
        # Thread-safe memory caches
        self.data_cache = ThreadSafeCache()
        self.yaml_config_cache = ThreadSafeCache()
        self.yaml_file_cache = ThreadSafeCache()
        self.result_cache = ThreadSafeCache()
        self.metric_cache = ThreadSafeCache()
        self.fitness_cache = ThreadSafeCache()
        
        # Persistent disk cache with lock
        self.disk_cache_lock = Lock()
        self.disk_cache_file = self.cache_dir / "disk_cache.pkl"
        self.disk_cache = self._load_disk_cache()
        
        # Statistics with lock
        self.stats_lock = Lock()
        self.hits = 0
        self.misses = 0
        self.data_loader_stats_collector = None
    
    def _load_disk_cache(self):
        try:
            if self.disk_cache_file.exists():
                with open(self.disk_cache_file, 'rb') as f:
                    return pickle.load(f)
        except Exception as e:
            print(f"⚠️  Could not load disk cache: {e}")
        return {}
    
    def save_disk_cache(self):
        with self.disk_cache_lock:
            try:
                with open(self.disk_cache_file, 'wb') as f:
                    pickle.dump(self.disk_cache, f)
            except Exception as e:
                print(f"⚠️  Could not save disk cache: {e}")
    
    def generate_key(self, *args, **kwargs):
        key_data = {'args': args, 'kwargs': kwargs}
        key_str = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def increment_hit(self):
        with self.stats_lock:
            self.hits += 1
    
    def increment_miss(self):
        with self.stats_lock:
            self.misses += 1
    
    @staticmethod
    def disk_cache():
        def decorator(func):
            def wrapper(self_instance, *args, **kwargs):
                key = self_instance.cache.generate_key(func.__name__, *args, **kwargs)
                
                with self_instance.cache.disk_cache_lock:
                    if key in self_instance.cache.disk_cache:
                        self_instance.cache.increment_hit()
                        return self_instance.cache.disk_cache[key]
                
                self_instance.cache.increment_miss()
                result = func(self_instance, *args, **kwargs)
                
                with self_instance.cache.disk_cache_lock:
                    self_instance.cache.disk_cache[key] = result
                
                return result
            return wrapper
        return decorator
    
    @classmethod
    def memory_cache(cls, cache_name="default"):
        def decorator(func):
            def wrapper(self_instance, *args, **kwargs):
                key = self_instance.cache.generate_key(func.__name__, *args, **kwargs)
                cache_dict = cls._get_cache_dict(self_instance, cache_name)
                
                if key in cache_dict:
                    self_instance.cache.increment_hit()
                    return cache_dict.get(key)
                
                self_instance.cache.increment_miss()
                result = func(self_instance, *args, **kwargs)
                cache_dict.set(key, result)
                return result
            return wrapper
        return decorator
    
    @staticmethod
    def _get_cache_dict(instance, cache_name):
        if cache_name == "data": return instance.cache.data_cache
        elif cache_name == "yaml_config": return instance.cache.yaml_config_cache
        elif cache_name == "yaml_file": return instance.cache.yaml_file_cache
        elif cache_name == "result": return instance.cache.result_cache
        elif cache_name == "metric": return instance.cache.metric_cache
        elif cache_name == "fitness": return instance.cache.fitness_cache
        else: return instance.cache.disk_cache

    def set_data_loader_stats_collector(self, stats_collector):
        self.data_loader_stats_collector = stats_collector

    def get_stats(self):
        with self.stats_lock:
            total = self.hits + self.misses
            hit_rate = (self.hits / total * 100) if total > 0 else 0
            
            stats = {
                "orchestrator": {
                    "hits": self.hits,
                    "misses": self.misses,
                    "hit_rate": f"{hit_rate:.1f}%",
                    "memory_caches": {
                        "data": len(self.data_cache),
                        "yaml_config": len(self.yaml_config_cache),
                        "yaml_file": len(self.yaml_file_cache),
                        "result": len(self.result_cache),
                        "metric": len(self.metric_cache),
                        "fitness": len(self.fitness_cache)
                    },
                    "disk_cache": len(self.disk_cache)
                }
            }
            if self.data_loader_stats_collector:
                stats["data_loader"] = self.data_loader_stats_collector.get_summary()
            return stats

    def print_stats(self):
        stats = self.get_stats()
        print("\n" + "="*60)
        print("📊 UNIFIED CACHE STATISTICS")
        print("="*60)
        
        o_stats = stats["orchestrator"]
        print(f"\n🧠 ORCHESTRATOR CACHES:")
        print(f"  Hits: {o_stats['hits']} | Misses: {o_stats['misses']}")
        print(f"  Hit Rate: {o_stats['hit_rate']}")
        print(f"\n  Memory Cache Breakdown:")
        print(f"    1. Data: {o_stats['memory_caches']['data']}")
        print(f"    2. YAML Config: {o_stats['memory_caches']['yaml_config']}")
        print(f"    3. YAML Files: {o_stats['memory_caches']['yaml_file']}")
        print(f"    4. Results (Run): {o_stats['memory_caches']['result']}")
        print(f"    5. Metrics (IO): {o_stats['memory_caches']['metric']}")
        print(f"    6. Fitness (CPU): {o_stats['memory_caches']['fitness']}")
        print(f"  Disk Cache: {o_stats['disk_cache']} entries")
        
        if "data_loader" in stats:
            dl_stats = stats["data_loader"]
            print(f"\n💽 DATALOADER CACHE (OHLCV Data):")
            print(f"  Hits: {dl_stats.get('hits', 0)} | Misses: {dl_stats.get('misses', 0)}")
            print(f"  Hit Rate: {dl_stats.get('hit_rate', '0%')}")
        
        print("="*60)

    def clear_memory(self):
        self.data_cache.clear()
        self.yaml_config_cache.clear()
        self.yaml_file_cache.clear()
        self.result_cache.clear()
        self.metric_cache.clear()
        self.fitness_cache.clear()
        print("🧹 Cleared all memory caches")

class DataLoaderStatsCollector:
    """Collects and aggregates DataLoader cache statistics across strategy runs"""
    
    def __init__(self):
        self.lock = Lock()
        self.all_stats = []
        self.aggregated = self._create_empty_stats()
    
    def _create_empty_stats(self):
        return {
            'hits': 0,
            'misses': 0,
            'total_requests': 0,
            'hit_rate': "0%",
            'cache_files': 0,
            'cache_size_mb': 0.0,
            'cache_dir': '~/.wbws_data_cache/'
        }
    
    def add_stats(self, stats: Dict):
        if not isinstance(stats, dict):
            return
        
        with self.lock:
            self.all_stats.append(stats.copy())
            self.aggregated['hits'] += stats.get('hits', 0)
            self.aggregated['misses'] += stats.get('misses', 0)
            
            if 'cache_files' in stats:
                self.aggregated['cache_files'] = stats['cache_files']
            if 'cache_size_mb' in stats:
                self.aggregated['cache_size_mb'] = stats['cache_size_mb']
            if 'cache_dir' in stats:
                self.aggregated['cache_dir'] = stats['cache_dir']
            
            self.aggregated['total_requests'] = self.aggregated['hits'] + self.aggregated['misses']
            
            if self.aggregated['total_requests'] > 0:
                hit_rate = (self.aggregated['hits'] / self.aggregated['total_requests']) * 100
                self.aggregated['hit_rate'] = f"{hit_rate:.1f}%"
            else:
                self.aggregated['hit_rate'] = "0%"
    
    def get_summary(self) -> Dict:
        with self.lock:
            return self.aggregated.copy()
    
    def get_detailed(self) -> List[Dict]:
        with self.lock:
            return self.all_stats.copy()
    
    def clear(self):
        with self.lock:
            self.all_stats = []
            self.aggregated = self._create_empty_stats()

class ResourceMonitor:
    """Monitor system resources during parallel execution"""
    
    def __init__(self, warning_threshold_percent=80):
        self.warning_threshold = warning_threshold_percent
        self.initial_memory = None
        self.peak_memory = 0
    
    def start_monitoring(self):
        """Record initial memory state"""
        self.initial_memory = psutil.virtual_memory().percent
        self.peak_memory = self.initial_memory
        print(f"📊 Initial memory usage: {self.initial_memory:.1f}%")
    
    def check_resources(self) -> Tuple[bool, str]:
        """
        Check if system resources are healthy
        
        Returns:
            Tuple of (is_healthy, message)
        """
        mem = psutil.virtual_memory()
        current_mem_percent = mem.percent
        
        # Update peak
        if current_mem_percent > self.peak_memory:
            self.peak_memory = current_mem_percent
        
        # Check if we're approaching limits
        if current_mem_percent > self.warning_threshold:
            return False, f"⚠️  High memory usage: {current_mem_percent:.1f}%"
        
        return True, f"✅ Memory: {current_mem_percent:.1f}%"
    
    def print_summary(self):
        """Print resource usage summary"""
        mem = psutil.virtual_memory()
        print(f"\n📊 Resource Usage Summary:")
        print(f"   Initial: {self.initial_memory:.1f}%")
        print(f"   Peak: {self.peak_memory:.1f}%")
        print(f"   Final: {mem.percent:.1f}%")
        print(f"   Available: {mem.available / (1024**3):.1f} GB")

class BacktestOrchestrator:
    def __init__(self, backtest_yaml_path: str):
        
        print(f"\n🔧 Initializing cache manager")
        self.cache = HybridCacheManager()
        
        # Initialize DataLoader stats collector
        self.data_loader_stats = DataLoaderStatsCollector()
        self.cache.set_data_loader_stats_collector(self.data_loader_stats)
        
        # Initialize resource monitor
        self.resource_monitor = ResourceMonitor(warning_threshold_percent=80)
        
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

        # Initialize Fitness Engine globally
        self.fitness_engine = FitnessEvaluator(
            constraints=self.config.get("constraints", {}),
            weights=self.config.get("fitness", {}).get("weights", {})
        )
        
        # Configure parallel execution
        parallel_config = self.config.get("parallel_execution", {})
        self.parallel_enabled = parallel_config.get("enabled", True)
        
        # Auto-detect optimal worker count if not specified
        max_workers_config = parallel_config.get("max_workers", None)
        if max_workers_config is None:
            # Use 75% of CPU cores, minimum 2, maximum 8
            cpu_count = os.cpu_count() or 4
            self.max_workers = max(2, min(8, int(cpu_count * 0.75)))
        else:
            self.max_workers = max_workers_config
        
        # Safety limits
        self.max_workers = min(self.max_workers, 12)  # Hard cap
        
        # Retry configuration
        self.max_retries = parallel_config.get("max_retries", 2)
        self.retry_delay = parallel_config.get("retry_delay_seconds", 5)
        
        print(f"⚡ Parallel execution: {'Enabled' if self.parallel_enabled else 'Disabled'}")
        if self.parallel_enabled:
            print(f"   Max workers: {self.max_workers}")
            print(f"   Max retries: {self.max_retries}")
            print(f"   Retry delay: {self.retry_delay}s")

    @HybridCacheManager.memory_cache(cache_name="metric")
    def get_cached_metrics(self, report_path: Path) -> dict:
        """Thread-safe cached metrics extraction"""
        if not Path(report_path).exists():
            return {}
        return OptimizationMetrics(str(report_path)).get()

    @HybridCacheManager.memory_cache(cache_name="fitness")
    def get_cached_fitness(self, metrics: dict) -> float:
        """Thread-safe cached fitness calculation"""
        return self.fitness_engine.score(metrics)

    @HybridCacheManager.memory_cache(cache_name="data")
    def get_cached_parameter_space(self, zone_name: str, zone_cfg: dict) -> dict:
        """Thread-safe cached parameter space building"""
        print(f"   🔧 Building parameter space for zone: {zone_name}")
        space = ParameterSpace(zone_cfg).build()
        print(f"   ✅ Parameter space built: {len(space)} parameters")
        return space

    @lru_cache(maxsize=1)
    def load_and_clean_template(self):
        """Load strategy template and convert numpy types to Python types"""
        config_dir = self.backtest_yaml_path.parent
        template_path = config_dir / "wbws_rsi_strategy.yaml"
        
        if not template_path.exists():
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
        
        with open(template_path, "r") as f:
            content = f.read()
        
        content = content.replace('!!python/object/apply:numpy.core.multiarray.scalar', '')
        content = content.replace('!!python/object/apply:numpy._core.multiarray.scalar', '')
        content = content.replace('!!python/object/apply:array', '')
        
        try:
            template = yaml.safe_load(content)
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
            return obj.item()
        elif hasattr(obj, 'dtype'):
            return obj.tolist()
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
    
    def run_strategy_wrapper(self, args: Tuple) -> Tuple[int, Optional[Path], Optional[str]]:
        """
        Thread-safe wrapper for run_strategy with retry logic
        
        Args:
            args: Tuple of (temp_yaml, output_dir, sample_index)
            
        Returns:
            Tuple of (sample_index, report_path, error_message)
        """
        temp_yaml, output_dir, sample_index = args
        
        for attempt in range(self.max_retries + 1):
            try:
                report_path = self.run_strategy(temp_yaml, output_dir, sample_index)
                
                if report_path and report_path.exists():
                    return (sample_index, report_path, None)
                else:
                    error_msg = "Strategy completed but no report generated"
                    if attempt < self.max_retries:
                        print(f"    ⚠️  Attempt {attempt + 1} failed: {error_msg}, retrying...")
                        time.sleep(self.retry_delay)
                    else:
                        return (sample_index, None, error_msg)
                        
            except Exception as e:
                error_msg = str(e)
                if attempt < self.max_retries:
                    print(f"    ⚠️  Attempt {attempt + 1} failed: {error_msg}, retrying...")
                    time.sleep(self.retry_delay)
                else:
                    print(f"    ❌ All {self.max_retries + 1} attempts failed for strategy {sample_index}")
                    return (sample_index, None, error_msg)
        
        return (sample_index, None, "Max retries exceeded")
    
    def run_strategies_parallel(
        self, 
        strategy_configs: List[Tuple[Path, Path, int]],
        phase_name: str = "Strategy Execution"
    ) -> List[Tuple[int, Optional[Path], Optional[str]]]:
        """
        Run multiple strategies in parallel with safety checks
        
        Args:
            strategy_configs: List of (temp_yaml, output_dir, sample_index) tuples
            phase_name: Name of the execution phase (for logging)
            
        Returns:
            List of (sample_index, report_path, error_message) tuples
        """
        if not self.parallel_enabled or len(strategy_configs) == 1:
            print(f"   ⚙️  Running {len(strategy_configs)} strategies sequentially")
            return [self.run_strategy_wrapper(config) for config in strategy_configs]
        
        print(f"   ⚡ {phase_name}: Running {len(strategy_configs)} strategies in parallel")
        print(f"      Workers: {min(self.max_workers, len(strategy_configs))}")
        
        # Check initial resources
        self.resource_monitor.start_monitoring()
        
        results = {}
        errors = {}
        completed = 0
        start_time = time.time()
        
        actual_workers = min(self.max_workers, len(strategy_configs))
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=actual_workers) as executor:
            # Submit all jobs
            future_to_index = {
                executor.submit(self.run_strategy_wrapper, config): config[2]
                for config in strategy_configs
            }
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_index):
                sample_index = future_to_index[future]
                
                try:
                    idx, report_path, error_msg = future.result(timeout=300)  # 5 min timeout
                    results[idx] = report_path
                    errors[idx] = error_msg
                    completed += 1
                    
                    # Progress indicator
                    progress = (completed / len(strategy_configs)) * 100
                    status = "✅" if report_path else "❌"
                    
                    print(f"   {status} Strategy {idx} completed [{progress:.0f}% - {completed}/{len(strategy_configs)}]")
                    
                    if error_msg:
                        print(f"      Error: {error_msg}")
                    
                    # Check resources every 5 completions
                    if completed % 5 == 0:
                        is_healthy, msg = self.resource_monitor.check_resources()
                        if not is_healthy:
                            print(f"      {msg}")
                    
                except concurrent.futures.TimeoutError:
                    print(f"   ⏱️  Strategy {sample_index} timed out after 300s")
                    results[sample_index] = None
                    errors[sample_index] = "Timeout after 300 seconds"
                    completed += 1
                    
                except Exception as e:
                    print(f"   ❌ Strategy {sample_index} raised exception: {e}")
                    results[sample_index] = None
                    errors[sample_index] = str(e)
                    completed += 1
        
        # Summary
        elapsed = time.time() - start_time
        successful = sum(1 for r in results.values() if r is not None)
        failed = len(results) - successful
        
        print(f"\n   📊 {phase_name} Summary:")
        print(f"      Total: {len(strategy_configs)}")
        print(f"      Successful: {successful}")
        print(f"      Failed: {failed}")
        print(f"      Time: {elapsed:.1f}s")
        print(f"      Avg: {elapsed/len(strategy_configs):.1f}s per strategy")
        
        if failed > 0:
            print(f"\n   ⚠️  Failed strategies:")
            for idx, error in errors.items():
                if error:
                    print(f"      - Strategy {idx}: {error}")
        
        # Resource summary
        self.resource_monitor.print_summary()
        
        # Return results in original order
        return [(idx, results.get(idx), errors.get(idx)) for idx in range(len(strategy_configs))]
    
    def run(self):
        print("\n" + "=" * 70)
        print("🎯 STARTING ORCHESTRATION (PRODUCTION MODE)")
        print("=" * 70)
        
        try:
            # Run optimization
            self.run_full_optimization()
            
            # Print cache statistics
            self.cache.print_stats()
            
            # Save disk cache for next run
            self.cache.save_disk_cache()
            
            print(f"\n💾 Disk cache saved for next run")
            print("=" * 70)
            
        except KeyboardInterrupt:
            print("\n\n⚠️  Orchestration interrupted by user")
            print("   Saving cache state...")
            self.cache.save_disk_cache()
            print("   ✅ Cache saved")
            sys.exit(0)
            
        except Exception as e:
            print(f"\n\n❌ Orchestration failed with error: {e}")
            import traceback
            traceback.print_exc()
            print("\n   Attempting to save cache state...")
            try:
                self.cache.save_disk_cache()
                print("   ✅ Cache saved")
            except:
                print("   ❌ Could not save cache")
            sys.exit(1)
    
    def run_full_optimization(self):
        """Main optimization with parallel execution and safety checks"""
        print("\n" + "=" * 70)
        print("🏃 STARTING OPTIMIZATION")
        print("=" * 70)
        
        zones = self.config.get("zones", {})
        
        for zone_name, zone_cfg in zones.items():
            if not zone_cfg.get("enabled", True):
                print(f"⏭️  Skipping disabled zone: {zone_name}")
                continue
            
            print(f"\n🔹 Processing zone: {zone_name}")
            print(f"   {zone_cfg.get('description', '')}")
            
            zone_dir = self.base_dir / zone_name / self.timestamp
            zone_dir.mkdir(parents=True, exist_ok=True)
            
            random_search_config = self.config.get("random_search", {})
            if not random_search_config.get("enabled", True):
                print(f"  ⏭️ Random search disabled, skipping zone {zone_name}")
                continue
            
            n_samples = random_search_config.get("samples_per_zone", 150)
            
            try:
                # Build parameter space (cached)
                space = self.get_cached_parameter_space(zone_name, zone_cfg)
                sampler = ParameterSampler(space, n_samples=n_samples)
                samples = sampler.random_sample()
                print(f"   Generated {len(samples)} parameter sets for random search")
                
                fitness_engine = FitnessEvaluator(
                    constraints=self.config["constraints"],
                    weights=self.config["fitness"]["weights"]
                )
                
                store = CandidateStore(zone_dir)
                
                # ========== PARALLEL RANDOM SEARCH PHASE ==========
                print(f"\n   🎯 Starting Random Search Phase")
                random_candidates = []
                
                total_to_process = min(len(samples), 5)  # Testing with 5
                
                # Prepare batch of configs
                strategy_configs = []
                for i in range(total_to_process):
                    params = samples[i]
                    temp_yaml = self.create_temp_yaml(params, zone_name, i, "random")
                    strategy_configs.append((temp_yaml, zone_dir, i))
                
                # Run in parallel with safety checks
                results = self.run_strategies_parallel(strategy_configs, "Random Search")
                
                # Process results
                successful_random = 0
                for i, report_path, error in results:
                    params = samples[i]
                    print(f"\n   Processing Random Sample {i+1}/{total_to_process}")
                    
                    if report_path and report_path.exists():
                        real_metrics = self.get_cached_metrics(report_path)
                        
                        if self.fitness_engine.passes_constraints(real_metrics):
                            score = self.get_cached_fitness(real_metrics)
                            print(f"    ✅ Passed constraints, score: {score:.4f}")
                            
                            store.add(
                                params=params,
                                metrics=real_metrics,
                                fitness=score,
                                zone_name=zone_name,
                                sample_index=i,
                                source="random"
                            )
                            
                            random_candidates.append({
                                'parameters': params,
                                'metrics': real_metrics,
                                'fitness': score
                            })
                            
                            successful_random += 1
                        else:
                            print(f"    ❌ Failed constraints")
                    else:
                        error_msg = error or "Unknown error"
                        print(f"    ⚠️  Strategy execution failed: {error_msg}")
                
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
                    
                    if ga_results:
                        for i, (params, score, metrics) in enumerate(ga_results):
                            store.add(
                                params=params,
                                metrics=metrics,
                                fitness=score,
                                zone_name=zone_name,
                                sample_index=i + len(samples),
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
        """Run strategy with result caching (existing implementation)"""
        print(f"    ▶ Running strategy with config: {strategy_yaml_path.name}")
        
        with open(strategy_yaml_path, 'r', encoding='utf-8') as f:
            content = f.read()
        content_hash = hashlib.md5(content.encode()).hexdigest()
        cache_key = f"strategy_result_{content_hash}"
        
        # Thread-safe cache check
        cached_report = self.cache.result_cache.get(cache_key)
        if cached_report and cached_report.exists():
            print(f"    🔄 Using cached strategy results")
            self._extract_dataloader_stats_from_report(cached_report)
            report_copy = output_dir / f"report_{strategy_yaml_path.stem}.json"
            if not report_copy.exists():
                shutil.copy(cached_report, report_copy)
            return report_copy
        
        project_root = Path(__file__).parent.parent.parent
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        
        cmd = [
            sys.executable,
            "-X", "utf8",
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
                errors='replace',
                timeout=300  # 5 minute timeout
            )
                        
            if result.stdout and "ENHANCED STRATEGY EXECUTION COMPLETED" in result.stdout:
                print(f"    ✅ Strategy completed successfully")
            
            if result.stderr:
                print(f"    ⚠ Stderr: {result.stderr[:500]}...")
                
        except subprocess.TimeoutExpired:
            print(f"    ⏱️  Strategy execution timed out after 300s")
            return None
            
        except subprocess.CalledProcessError as e:
            print(f"    ❌ Strategy execution failed with exit code: {e.returncode}")
            if e.stdout:
                print(f"    📋 Stdout (last 500 chars): {e.stdout[-500:] if len(e.stdout) > 500 else e.stdout}")
            if e.stderr:
                print(f"    🔴 Stderr (first 500 chars): {e.stderr[:500]}...")
            return None
            
        except Exception as e:
            print(f"    ❌ Unexpected error running strategy: {e}")
            return None

        reports_dir = project_root / "outputs" / "reports" / "WBWS"
        
        if reports_dir.exists():
            json_files = list(reports_dir.glob("strategy_report_*.json"))
            
            if json_files:
                latest_report = max(json_files, key=lambda f: f.stat().st_mtime)
                self._extract_dataloader_stats_from_report(latest_report)
                
                report_copy = output_dir / f"report_{strategy_yaml_path.stem}.json"
                shutil.copy(latest_report, report_copy)
                print(f"    ✔ Report saved to {report_copy}")
                
                # Thread-safe cache update
                self.cache.result_cache.set(cache_key, report_copy)
                return report_copy
        
        return None
    
    def _extract_dataloader_stats_from_report(self, report_path: Path):
        """Extract DataLoader cache statistics from strategy report (existing implementation)"""
        try:
            with open(report_path, 'r', encoding='utf-8') as f:
                report_data = json.load(f)
            
            stats = None
            if 'data_loader_cache_stats' in report_data:
                stats = report_data['data_loader_cache_stats']
            elif 'validation' in report_data and 'data_loader_cache' in report_data['validation']:
                stats = report_data['validation']['data_loader_cache']
            elif 'validation' in report_data and 'dataloader_cache_stats' in report_data['validation']:
                stats = report_data['validation']['dataloader_cache_stats']
            
            if stats and isinstance(stats, dict):
                if 'total_requests' not in stats:
                    hits = stats.get('hits', 0)
                    misses = stats.get('misses', 0)
                    stats['total_requests'] = hits + misses
                
                self.data_loader_stats.add_stats(stats)
                return True
                    
        except Exception as e:
            pass
        
        return False
    
    def clean_for_json(self, obj):
        """Recursively clean numpy types for JSON serialization"""
        if isinstance(obj, dict):
            return {k: self.clean_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.clean_for_json(v) for v in obj]
        elif isinstance(obj, (np.integer, np.floating)):
            return obj.item()
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat()
        else:
            return obj
    
    def integrate_genetic_algorithm(self, zone_name, zone_cfg, zone_dir, fitness_engine, 
                                   initial_candidates, sampler):
        """Integrate GA optimization (existing implementation)"""
        ga_config = self.config.get("genetic", {})
        if not ga_config.get("enabled", False):
            print(f"   ⏭️ GA optimization disabled, skipping")
            return []
        
        print(f"\n   🧬 Starting Genetic Algorithm optimization for zone: {zone_name}")
        
        try:
            ga_optimizer = GeneticOptimizer(
                sampler=sampler,
                orchestrator=self,
                fitness_engine=self.fitness_engine,
                config=ga_config,
                zone_name=zone_name,
                zone_dir=zone_dir
            )
            
            initial_population = None
            if initial_candidates:
                sorted_candidates = sorted(
                    initial_candidates,
                    key=lambda x: x.get('fitness', -1000),
                    reverse=True
                )
                
                initial_population = []
                for candidate in sorted_candidates[:10]:
                    if isinstance(candidate, dict) and 'parameters' in candidate:
                        initial_population.append(candidate['parameters'])
                    elif isinstance(candidate, tuple) and len(candidate) > 0:
                        initial_population.append(candidate[0])
                
                print(f"   Using {len(initial_population)} best candidates as initial population")
            else:
                print(f"   No initial candidates, starting with random population")
            
            ga_results = ga_optimizer.run(initial_population=initial_population)
            
            if ga_results:
                print(f"   ✅ GA completed with {len(ga_results)} candidates")
                
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
                
                ga_summary_clean = self.clean_for_json(ga_summary)
                
                with open(ga_results_file, "w") as f:
                    json.dump(ga_summary_clean, f, indent=2)
                
                print(f"   📊 GA results saved to: {ga_results_file}")
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
        """Convert flat parameters to nested structure (existing implementation)"""
        config_updates = {}
        project_root = Path(__file__).parent.parent.parent
        data_dir = project_root / "data" / "processed" / "ohlcv"
        
        if "rsi_overbought" in flat_params or "rsi_oversold" in flat_params:
            config_updates["filters"] = {
                "rsi_filter": {
                    "enabled": True,
                    "length": 14,
                    "overbought": float(flat_params.get("rsi_overbought", 70)),
                    "oversold": float(flat_params.get("rsi_oversold", 30))
                }
            }
        
        if "htf_timeframe" in flat_params:
            config_updates["indicator"] = {
                "name": "WBWS_Trigger",
                "htf_period": str(flat_params["htf_timeframe"])
            }
        
        if any(k in flat_params for k in ["atr_length", "atr_multiplier", "rr_target"]):
            config_updates.setdefault("trade_management", {})
            config_updates["trade_management"]["sl_tp"] = {
                "enabled": True,
                "atr_length": int(flat_params.get("atr_length", 14)),
                "sl_multiplier": float(flat_params.get("atr_multiplier", 1.4)),
                "risk_to_reward_ratio": float(flat_params.get("rr_target", 5.7))
            }
        
        if "max_risk_percentile" in flat_params:
            config_updates.setdefault("trade_management", {})
            config_updates["trade_management"]["risk_management"] = {
                "enabled": True,
                "max_risk_percentile": float(flat_params["max_risk_percentile"]),
                "allow_exceed_limit": False
            }
        
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
        
        config_updates["data"] = {
            "file": str(data_dir / "DEUIDXEUR_1min_20240101_20260104.csv"),
            "file_htf": str(data_dir / "DEUIDXEUR_1H_20230101_20260104.csv"),
            "file_ltf": str(data_dir / "DEUIDXEUR_1s_20240101_20260104.csv"),
            "format": "csv",
            "date_range": {
                "start": "2024-01-01",
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
        config = self.strategy_template.copy()
        config_updates = self.map_parameters_to_config(params)
        
        for section, updates in config_updates.items():
            if section not in config:
                config[section] = updates
            elif isinstance(config[section], dict) and isinstance(updates, dict):
                config[section].update(updates)
            else:
                config[section] = updates
        
        config = self.clean_numpy_types(config)
        return config

    def create_temp_yaml(self, params: dict, zone_name: str, sample_index: int, source: str = "test") -> Path:
        """Create temp YAML file with intelligent caching"""
        config = self._generate_yaml_config(params, zone_name)
        
        file_key = self.cache.generate_key("yaml_file", zone_name, params, source)
        cached_file = self.cache.yaml_file_cache.get(file_key)
        
        if cached_file and cached_file.exists():
            return cached_file
        
        yaml_content = yaml.dump(config, default_flow_style=False, sort_keys=False)
        
        temp_dir = Path(tempfile.gettempdir())
        temp_file = temp_dir / f"wbws_{zone_name}_{source}_{sample_index}.yaml"
        
        with open(temp_file, "w") as f:
            f.write(yaml_content)
        
        debug_dir = self.base_dir / "debug" / self.timestamp
        debug_dir.mkdir(parents=True, exist_ok=True)
        debug_file = debug_dir / f"{zone_name}_{source}_{sample_index}.yaml"
        
        with open(debug_file, "w") as f:
            f.write(yaml_content)
        
        self.cache.yaml_file_cache.set(file_key, temp_file)
        return temp_file

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("❌ Usage: python orchestrator_production.py <backtest_yaml>")
        print("\nExample:")
        print("  python src/backtesting/orchestrator_production.py src/config/WBWS/wbws_backtest.yaml")
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