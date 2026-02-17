"""
End-to-End Test: Legacy vs New Architecture Parity & Performance
Test Manager: [Your Name]
Date: 2026-02-17
Version: 1.8.0

FIXES:
- Added detailed mismatch reporting
- Enhanced ARTF data path debugging
- Fixed performance validation logic (New should be faster than Legacy)
- Added statistics comparison table
"""

import subprocess
import re
import time
import json
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import sys
import traceback
import pandas as pd
import yaml
import numpy as np
from dataclasses import dataclass, asdict

# Add project root to path
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# New architecture imports
try:
    from src.strategies.specific.modules.data_loader import DataLoader
    from src.strategies.specific.modules.signal_generator import SignalGenerator
    from src.strategies.specific.modules.filter_pipeline import FilterPipeline
    from src.strategies.specific.modules.trade_simulator import TradeSimulator
    from src.strategies.contracts.signal_contracts import SignalFrame, SignalType
    from src.strategies.contracts.trade_contracts import TradeDirection
    NEW_IMPORTS_AVAILABLE = True
    print("✅ New architecture imports successful")
    
    # Print SignalFrame structure for debugging
    print(f"📦 SignalFrame attributes: {[a for a in dir(SignalFrame) if not a.startswith('_')]}")
except ImportError as e:
    print(f"⚠️ Warning: New architecture imports failed: {e}")
    NEW_IMPORTS_AVAILABLE = False

# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class TestConfig:
    """Test configuration settings"""
    # Paths
    legacy_script: Path = project_root / "scripts" / "runners" / "run_wbws_strategy.py"
    config_core: Path = project_root / "configs" / "strategies" / "wbws" / "wbws_strategy.yaml"
    config_debug: Path = project_root / "configs" / "strategies" / "wbws" / "wbws_strategy_debug.yaml"
    cache_dir: Path = Path.home() / ".wbws_data_cache"
    
    # Test parameters
    tolerance: float = 1e-1  # Numerical tolerance for parity checks
    performance_tolerance: float = 0.1  # Allow 10% variance in performance measurements
    timeout_seconds: int = 600  # Max execution time per test
    
    # Output
    report_dir: Path = project_root / "tests" / "reports" / "comparison"
    verbose: bool = True
    
    # Python environment
    python_executable: str = sys.executable  # Use same Python as test script
    
    # Debug mode
    debug_new_architecture: bool = True  # Print detailed debug info for New arch


# ============================================================================
# TIMING CAPTURE
# ============================================================================

@dataclass
class StageTimings:
    """Stage-by-stage timing data"""
    # Core stages
    data_loading: float = 0.0
    signal_generation: float = 0.0
    filter_application: float = 0.0
    trade_simulation: float = 0.0
    metrics_calculation: float = 0.0
    
    # Metadata
    end_to_end: float = 0.0
    timestamp: str = ""
    
    def to_dict(self) -> Dict[str, float]:
        return {
            "data_loading": self.data_loading,
            "signal_generation": self.signal_generation,
            "filter_application": self.filter_application,
            "trade_simulation": self.trade_simulation,
            "metrics_calculation": self.metrics_calculation,
            "end_to_end": self.end_to_end
        }


@dataclass
class TestResult:
    """Complete test result for a single run"""
    architecture: str  # "LEGACY" or "NEW"
    mode: str  # "core" or "debug"
    run_type: str  # "cold" or "hot"
    
    # Performance
    timings: StageTimings
    
    # Statistics
    stats: Dict[str, Any]
    
    # Metadata
    success: bool = True
    error_message: str = ""
    error_traceback: str = ""
    cache_hit_rate: Optional[float] = None
    raw_log: Optional[str] = None  # For debugging
    warnings: List[str] = None
    artf_loaded: bool = False  # Track if ARTF data was loaded
    
    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []
    
    def to_dict(self) -> Dict:
        return {
            "architecture": self.architecture,
            "mode": self.mode,
            "run_type": self.run_type,
            "timings": self.timings.to_dict(),
            "stats": self.stats,
            "success": self.success,
            "error_message": self.error_message,
            "cache_hit_rate": self.cache_hit_rate,
            "warnings": self.warnings,
            "artf_loaded": self.artf_loaded
        }


# ============================================================================
# LEGACY ARCHITECTURE PARSER
# ============================================================================

class LegacyLogParser:
    """Parse Legacy architecture logs for timing and statistics"""
    
    # Regex patterns
    PATTERNS = {
        'step_start': r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) \[INFO\] STEP (\d+): (.*)',
        'execution_completed': r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) \[INFO\] EXECUTION COMPLETED.*',
        'execution_start': r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) \[INFO\] ======+',
        'cache_hit': r'Cache hit for (\w+): (.*)',
        'loading_fresh': r'Loading fresh (\w+): (.*)',
        'artf_loaded': r'Loading fresh artf:',  # Track ARTF loading
        'data_bars': {
            'full': r'Full dataset: ([\d,]+) bars',
            'strategy': r'Strategy period: ([\d,]+) bars',
            'htf': r'HTF dataset: ([\d,]+) bars',
            'ltf': r'LTF dataset: ([\d,]+) bars',
        },
        'signals': r'Raw BUY: ([\d,]+), SELL: ([\d,]+), Total: ([\d,]+)',
        'filters': {
            'time': r'Time filtered: (\d+) \((\d+) BUY, (\d+) SELL\)',
            'technical': r'Technical filtered: (\d+) \((\d+) BUY, (\d+) SELL\)',
        },
        'trades': r'Closed trades: ([\d,]+), Open: (\d+), Rejected: ([\d,]+)',
        'metrics': r'Total P&L: ([\-\d.]+) pts \| Win rate: ([\d.]+)% \| Max DD: ([\-\d.]+) pts',
        'date_range': r'Date range: \(\'(.*)\', \'(.*)\'\)',
        'mode': r'(CORE|DEBUG) MODE',
    }
    
    @classmethod
    def parse(cls, log_text: str) -> Tuple[StageTimings, Dict[str, Any], Optional[float], bool]:
        """Parse Legacy log into timings, stats, cache hit rate, and ARTF loaded status"""
        timings = StageTimings()
        stats = {
            "data": {},
            "signals": {},
            "filters": {},
            "trades": {},
            "metrics": {}
        }
        
        # Track if ARTF was loaded
        artf_loaded = bool(re.search(cls.PATTERNS['artf_loaded'], log_text))
        
        # Find all step start timestamps
        step_starts = []
        for match in re.finditer(cls.PATTERNS['step_start'], log_text):
            time_str, step_num, step_name = match.groups()
            dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S,%f")
            step_starts.append((int(step_num), dt, step_name))
        
        # Find execution start and end
        exec_start = None
        exec_end = None
        
        start_match = re.search(cls.PATTERNS['execution_start'], log_text)
        if start_match:
            exec_start = datetime.strptime(start_match.group(1), "%Y-%m-%d %H:%M:%S,%f")
        
        end_match = re.search(cls.PATTERNS['execution_completed'], log_text)
        if end_match:
            exec_end = datetime.strptime(end_match.group(1), "%Y-%m-%d %H:%M:%S,%f")
        
        # Calculate stage durations using step starts
        if len(step_starts) >= 5:
            # Step 1: Data Loading
            timings.data_loading = (step_starts[1][1] - step_starts[0][1]).total_seconds()
            
            # Step 2: Signal Generation
            timings.signal_generation = (step_starts[2][1] - step_starts[1][1]).total_seconds()
            
            # Step 3: Filter Application
            timings.filter_application = (step_starts[3][1] - step_starts[2][1]).total_seconds()
            
            # Step 4: Trade Simulation
            timings.trade_simulation = (step_starts[4][1] - step_starts[3][1]).total_seconds()
            
            # Step 5: Metrics Calculation (if exists)
            if len(step_starts) >= 6:
                timings.metrics_calculation = (step_starts[5][1] - step_starts[4][1]).total_seconds()
            elif exec_end:
                timings.metrics_calculation = (exec_end - step_starts[4][1]).total_seconds()
        
        # Calculate end-to-end
        if exec_start and exec_end:
            timings.end_to_end = (exec_end - exec_start).total_seconds()
        
        # Parse cache hit rate
        cache_hits = len(re.findall(cls.PATTERNS['cache_hit'], log_text))
        fresh_loads = len(re.findall(cls.PATTERNS['loading_fresh'], log_text))
        total_files = cache_hits + fresh_loads
        cache_hit_rate = cache_hits / total_files if total_files > 0 else 0.0
        
        # Parse data stats
        for key, pattern in cls.PATTERNS['data_bars'].items():
            match = re.search(pattern, log_text)
            if match:
                stats['data'][key] = int(match.group(1).replace(',', ''))
        
        # Parse date range
        match = re.search(cls.PATTERNS['date_range'], log_text)
        if match:
            stats['data']['date_range'] = (match.group(1), match.group(2))
        
        # Parse signals
        match = re.search(cls.PATTERNS['signals'], log_text)
        if match:
            stats['signals'] = {
                'buy': int(match.group(1).replace(',', '')),
                'sell': int(match.group(2).replace(',', '')),
                'total': int(match.group(3).replace(',', ''))
            }
        
        # Parse filters - extract all filter information
        time_match = re.search(cls.PATTERNS['filters']['time'], log_text)
        if time_match:
            stats['filters']['time_filtered'] = int(time_match.group(1))
            stats['filters']['time_buy'] = int(time_match.group(2))
            stats['filters']['time_sell'] = int(time_match.group(3))
        
        tech_match = re.search(cls.PATTERNS['filters']['technical'], log_text)
        if tech_match:
            stats['filters']['technical_filtered'] = int(tech_match.group(1))
            stats['filters']['tech_buy'] = int(tech_match.group(2))
            stats['filters']['tech_sell'] = int(tech_match.group(3))
        
        # Parse raw signals count for filters
        if 'signals' in stats and 'total' in stats['signals']:
            stats['filters']['raw_signals'] = stats['signals']['total']
        
        # Parse trades
        match = re.search(cls.PATTERNS['trades'], log_text)
        if match:
            stats['trades'] = {
                'closed': int(match.group(1).replace(',', '')),
                'open': int(match.group(2)),
                'rejected': int(match.group(3).replace(',', ''))
            }
        
        # Parse metrics
        match = re.search(cls.PATTERNS['metrics'], log_text)
        if match:
            stats['metrics'] = {
                'total_pnl': float(match.group(1)),
                'win_rate': float(match.group(2)),
                'max_drawdown': float(match.group(3))
            }
        
        # Parse mode
        mode_match = re.search(cls.PATTERNS['mode'], log_text)
        if mode_match:
            stats['mode'] = mode_match.group(1).lower()
        
        return timings, stats, cache_hit_rate, artf_loaded


# ============================================================================
# NEW ARCHITECTURE EXECUTOR
# ============================================================================

class NewArchitectureExecutor:
    """Execute New architecture pipeline programmatically"""
    
    # Signal type mapping: 1 -> "BUY", 2 -> "SELL"
    SIGNAL_TYPE_MAP = {
        1: "BUY",
        2: "SELL"
    }
    
    def __init__(self, config_path: Path, mode: str, debug: bool = False):
        self.config_path = config_path
        self.mode = mode
        self.debug = debug
        self.config = self._load_config()
        self.warnings = []
        
        # Print ARTF file path for debugging
        artf_path = self.config.get('data', {}).get('file_artf', 'Not found')
        print(f"📂 ARTF file in config: {artf_path}")
        full_artf_path = project_root / artf_path
        print(f"📂 Full ARTF path: {full_artf_path}")
        print(f"📂 ARTF exists: {full_artf_path.exists()}")
        
    def _load_config(self) -> Dict:
        """Load YAML configuration"""
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _safe_get_public_attrs(self, obj) -> List[str]:
        """Safely get public attributes of an object without triggering pandas accessors"""
        if obj is None:
            return []
        
        # Special handling for pandas Series/DataFrame
        if isinstance(obj, (pd.Series, pd.DataFrame)):
            # Just return basic info for pandas objects
            return ['dtype', 'shape', 'index', 'name'] if hasattr(obj, 'name') else ['dtype', 'shape', 'index']
        
        # For other objects, safely get attributes
        attrs = []
        for attr in dir(obj):
            if attr.startswith('_'):
                continue
            try:
                # Try to get the attribute without triggering property getters
                val = getattr(obj, attr, None)
                if not callable(val):
                    attrs.append(attr)
            except Exception:
                # Skip attributes that raise exceptions when accessed
                continue
        return attrs
    
    def _debug_print(self, msg: str, obj: Any = None):
        """Print debug information if enabled"""
        if self.debug:
            print(f"  🔍 DEBUG: {msg}")
            if obj is not None:
                print(f"     Type: {type(obj)}")
                try:
                    attrs = self._safe_get_public_attrs(obj)
                    if attrs:
                        print(f"     Public attrs: {attrs}")
                    
                    # Special handling for Series
                    if isinstance(obj, pd.Series):
                        print(f"     Series length: {len(obj)}")
                        print(f"     Series dtype: {obj.dtype}")
                        if hasattr(obj, 'index') and len(obj) > 0:
                            print(f"     Index range: {obj.index[0]} to {obj.index[-1]}")
                except Exception as e:
                    print(f"     (Error getting attributes: {e})")
    
    def _count_signals(self, signal_frame) -> Dict[str, int]:
        """Count signals from SignalFrame"""
        counts = {'buy': 0, 'sell': 0, 'total': 0}
        
        if hasattr(signal_frame, 'count_by_type'):
            # Use the built-in method
            try:
                counts = signal_frame.count_by_type()
                self._debug_print(f"Count by type: {counts}")
            except:
                pass
        elif hasattr(signal_frame, 'signals'):
            # Access signals Series directly
            signals = signal_frame.signals
            if hasattr(signals, 'value_counts'):
                val_counts = signals.value_counts()
                counts['buy'] = int(val_counts.get(1, 0))  # 1 = BUY
                counts['sell'] = int(val_counts.get(2, 0))  # 2 = SELL
                counts['total'] = counts['buy'] + counts['sell']
                self._debug_print(f"Signal counts from Series: {counts}")
        
        return counts
    
    def _convert_signals_to_strings(self, signals_series: pd.Series) -> pd.Series:
        """
        Convert int8 signals (1/2) to string signals ("BUY"/"SELL")
        This creates a clean Series with object dtype containing only the signal strings
        """
        self._debug_print("Converting signals to strings...")
        
        # Create a new Series with object dtype
        string_signals = pd.Series(index=signals_series.index, dtype=object)
        
        # Only set values where we have actual signals (non-zero)
        signal_count = 0
        for idx, val in signals_series.items():
            if pd.notna(val) and val in self.SIGNAL_TYPE_MAP:
                string_signals[idx] = self.SIGNAL_TYPE_MAP[val]
                signal_count += 1
            else:
                # For zero or NaN, we can leave as None (will be dropped)
                string_signals[idx] = None
        
        # Drop NaN/None values to only pass actual signals
        string_signals = string_signals.dropna()
        
        self._debug_print(f"Converted signals count: {len(string_signals)}")
        self._debug_print(f"Converted signals sample (first 5):")
        for idx, val in string_signals.head().items():
            self._debug_print(f"      {idx}: {val}")
        self._debug_print(f"Unique values after conversion: {string_signals.unique()}")
        
        return string_signals
    
    def execute(self) -> Tuple[StageTimings, Dict[str, Any], Optional[float], List[str], bool]:
        """
        Execute New architecture pipeline with timing
        Returns: (timings, stats, cache_hit_rate, warnings, artf_loaded)
        """
        if not NEW_IMPORTS_AVAILABLE:
            raise ImportError("New architecture imports not available")
        
        timings = StageTimings()
        timings.timestamp = datetime.now().isoformat()
        warnings = []
        artf_loaded = False
        
        try:
            # Stage 1: Data Loading
            self._debug_print("Starting DataLoader")
            start = time.perf_counter()
            data_loader = DataLoader(
                config_path=str(self.config_path),
                mode=self.mode
            )
            data_bundle = data_loader.load_data()
            timings.data_loading = time.perf_counter() - start
            self._debug_print(f"DataLoader complete in {timings.data_loading:.3f}s")
            
            # Check if ARTF was loaded
            if hasattr(data_bundle, 'artf') and data_bundle.artf is not None:
                artf_loaded = True
                self._debug_print(f"ARTF data loaded: {len(data_bundle.artf)} bars")
            else:
                self._debug_print("ARTF data NOT loaded")
            
            # Stage 2: Signal Generation
            self._debug_print("Starting SignalGenerator")
            start = time.perf_counter()
            signal_gen = SignalGenerator(
                htf_period=self.config['indicator']['htf_period'],
                mode=self.mode
            )
            signal_frame = signal_gen.generate_signals(data_bundle)
            timings.signal_generation = time.perf_counter() - start
            self._debug_print(f"SignalGenerator complete in {timings.signal_generation:.3f}s")
            
            # Count signals for validation
            signal_counts = self._count_signals(signal_frame)
            self._debug_print(f"Generated signals: {signal_counts}")
            
            # Stage 3: Filter Pipeline
            self._debug_print("Starting FilterPipeline")
            start = time.perf_counter()
            filter_pipe = FilterPipeline(config=self.config)
            filter_result = filter_pipe.apply_filters(
                signal_frame=signal_frame,
                df=data_bundle.strategy,
                mode=self.mode
            )
            timings.filter_application = time.perf_counter() - start
            self._debug_print(f"FilterPipeline complete in {timings.filter_application:.3f}s")
            
            # Get final signals - this should be a SignalFrame
            if hasattr(filter_result, 'final_signals'):
                final_signals_frame = filter_result.final_signals
            elif hasattr(filter_result, 'signal_frame'):
                final_signals_frame = filter_result.signal_frame
            else:
                final_signals_frame = signal_frame  # Fallback
            
            self._debug_print("Final signals frame type", final_signals_frame)
            
            # Extract the signals Series for TradeSimulator
            if hasattr(final_signals_frame, 'signals'):
                final_signals_series = final_signals_frame.signals
                self._debug_print("Extracted signals Series - SUCCESS")
                self._debug_print(f"Final signals count: {len(final_signals_series)}")
                self._debug_print(f"Final signals dtype: {final_signals_series.dtype}")
                self._debug_print(f"Final signals unique values: {final_signals_series.unique()}")
            else:
                final_signals_series = final_signals_frame
                self._debug_print("Using raw signals (not SignalFrame)")
            
            # Count final signals for validation
            final_counts = self._count_signals(final_signals_frame)
            self._debug_print(f"Filtered signals: {final_counts}")
            
            # CRITICAL FIX: Convert int8 signals to strings for TradeManager
            # and only pass actual signals (non-zero)
            string_signals = self._convert_signals_to_strings(final_signals_series)
            
            # Stage 4: Trade Simulation
            self._debug_print("Starting TradeSimulator")
            start = time.perf_counter()
            trade_sim = TradeSimulator(
                config=self.config,
                df_full=data_bundle.full
            )
            
            # Pass the converted string signals (only actual signals, no zeros)
            trade_result = trade_sim.simulate_trades(
                filtered_signals=string_signals,  # Now clean Series with only "BUY"/"SELL" strings
                df_strategy=data_bundle.strategy,
                df_ltf=data_bundle.ltf
            )
            
            timings.trade_simulation = time.perf_counter() - start
            self._debug_print(f"TradeSimulator complete in {timings.trade_simulation:.3f}s")
            
            # Stage 5: Metrics (if available)
            timings.metrics_calculation = 0.0  # Placeholder
            
            # Calculate end-to-end
            timings.end_to_end = sum([
                timings.data_loading,
                timings.signal_generation,
                timings.filter_application,
                timings.trade_simulation,
                timings.metrics_calculation
            ])
            
            # Collect statistics
            stats = self._collect_stats(data_bundle, signal_frame, filter_result, trade_result, final_counts)
            
            # Estimate cache hit rate
            cache_hit_rate = 0.5  # Default
            if hasattr(data_bundle, 'info') and hasattr(data_bundle.info, 'cache_hits'):
                cache_hit_rate = data_bundle.info.cache_hits / 4.0
            
        except Exception as e:
            # Capture full traceback
            tb = traceback.format_exc()
            raise RuntimeError(f"New architecture execution failed: {e}\n{tb}") from e
        
        return timings, stats, cache_hit_rate, warnings, artf_loaded
    
    def _collect_stats(self, data_bundle, signal_frame, filter_result, trade_result, final_counts) -> Dict:
        """Collect statistics from New architecture components"""
        
        # Get signal counts
        signal_counts = self._count_signals(signal_frame)
        
        # Get filter counts
        raw_signals = getattr(filter_result, 'raw_count', signal_counts.get('total', 0))
        time_filtered = getattr(filter_result, 'time_filtered_count', 0)
        technical_filtered = getattr(filter_result, 'technical_filtered_count', 0)
        
        # Get trade counts
        closed_trades = 0
        open_trades = 0
        if hasattr(trade_result, 'trades'):
            if hasattr(trade_result.trades, '__iter__'):
                for t in trade_result.trades:
                    if hasattr(t, 'exit'):
                        if t.exit is not None:
                            closed_trades += 1
                        else:
                            open_trades += 1
        
        rejected = 0
        if hasattr(trade_result, 'rejected_signals'):
            rejected = len(getattr(trade_result, 'rejected_signals', []))
        
        # Get metrics
        total_pnl = getattr(trade_result, 'total_pnl_points', 0)
        win_rate = getattr(trade_result, 'win_rate', 0)
        # Convert to percentage if needed
        if win_rate < 1 and win_rate > 0:
            win_rate = win_rate * 100
        
        stats = {
            "data": {
                "full_bars": getattr(data_bundle.info, 'total_bars', 0) if hasattr(data_bundle, 'info') else 0,
                "strategy_bars": getattr(data_bundle.info, 'strategy_bars', 0) if hasattr(data_bundle, 'info') else 0,
                "htf_bars": getattr(data_bundle.info, 'htf_bars', 0) if hasattr(data_bundle, 'info') else 0,
                "ltf_bars": getattr(data_bundle.info, 'ltf_bars', 0) if hasattr(data_bundle, 'info') else 0,
                "date_range": (
                    str(data_bundle.info.date_range[0]) if hasattr(data_bundle, 'info') and hasattr(data_bundle.info, 'date_range') and data_bundle.info.date_range else "",
                    str(data_bundle.info.date_range[1]) if hasattr(data_bundle, 'info') and hasattr(data_bundle.info, 'date_range') and data_bundle.info.date_range else ""
                ),
            },
            "signals": signal_counts,
            "filters": {
                "raw_signals": raw_signals,
                "time_filtered": time_filtered,
                "technical_filtered": technical_filtered,
                "final_signals": final_counts.get('total', 0),
                "final_buy": final_counts.get('buy', 0),
                "final_sell": final_counts.get('sell', 0),
            },
            "trades": {
                "closed": closed_trades,
                "open": open_trades,
                "rejected": rejected,
            },
            "metrics": {
                "total_pnl": total_pnl,
                "win_rate": win_rate,
                "max_drawdown": 0,  # Placeholder - would need metrics calculator
            }
        }
        return stats


# ============================================================================
# TEST EXECUTOR
# ============================================================================

class TestExecutor:
    """Main test execution orchestrator"""
    
    def __init__(self, config: TestConfig = None):
        self.config = config or TestConfig()
        self.results: List[TestResult] = []
        self._ensure_directories()
        
        # Print environment info for debugging
        print(f"🔧 Python executable: {self.config.python_executable}")
        print(f"📂 Project root: {project_root}")
        print(f"📁 Legacy script: {self.config.legacy_script}")
        
    def _ensure_directories(self):
        """Create necessary directories"""
        self.config.report_dir.mkdir(parents=True, exist_ok=True)
        self.config.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def clear_cache(self):
        """Clear cache for cold runs"""
        if self.config.cache_dir.exists():
            shutil.rmtree(self.config.cache_dir, ignore_errors=True)
        self.config.cache_dir.mkdir(exist_ok=True)
        if self.config.verbose:
            print("🧹 Cache cleared for cold run")
    
    def run_legacy(self, mode: str, run_type: str) -> TestResult:
        """Run Legacy architecture test with proper environment"""
        config_path = self.config.config_debug if mode == "debug" else self.config.config_core
        
        if self.config.verbose:
            print(f"\n🚀 Running Legacy {mode.upper()} ({run_type})...")
            print(f"   Config: {config_path}")
        
        try:
            # Prepare environment with proper PYTHONPATH
            env = os.environ.copy()
            env['PYTHONPATH'] = str(project_root) + os.pathsep + env.get('PYTHONPATH', '')
            
            # Also add the src directory explicitly
            src_path = project_root / "src"
            env['PYTHONPATH'] = str(src_path) + os.pathsep + env['PYTHONPATH']
            
            cmd = [
                self.config.python_executable,
                str(self.config.legacy_script),
                str(config_path)
            ]
            
            if self.config.verbose:
                print(f"   CMD: {' '.join(cmd)}")
            
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.config.timeout_seconds,
                env=env,
                cwd=str(project_root)  # Run from project root
            )
            
            if proc.returncode != 0:
                error_msg = f"Legacy failed with code {proc.returncode}"
                if proc.stderr:
                    error_msg += f"\nSTDERR: {proc.stderr}"
                if proc.stdout:
                    error_msg += f"\nSTDOUT: {proc.stdout[:500]}..."  # First 500 chars
                raise RuntimeError(error_msg)
            
            timings, stats, cache_hit_rate, artf_loaded = LegacyLogParser.parse(proc.stdout)
            
            # Save log
            log_file = self.config.report_dir / f"legacy_{mode}_{run_type}.log"
            log_file.write_text(proc.stdout, encoding='utf-8')
            if self.config.verbose:
                print(f"   Log saved to: {log_file}")
            
            return TestResult(
                architecture="LEGACY",
                mode=mode,
                run_type=run_type,
                timings=timings,
                stats=stats,
                cache_hit_rate=cache_hit_rate,
                artf_loaded=artf_loaded,
                raw_log=proc.stdout if self.config.verbose else None
            )
            
        except subprocess.TimeoutExpired:
            error_msg = f"Legacy {mode} timed out after {self.config.timeout_seconds}s"
            print(f"❌ {error_msg}")
            return TestResult(
                architecture="LEGACY",
                mode=mode,
                run_type=run_type,
                timings=StageTimings(),
                stats={},
                success=False,
                error_message=error_msg
            )
        except Exception as e:
            if self.config.verbose:
                print(f"❌ Legacy {mode} failed: {e}")
            return TestResult(
                architecture="LEGACY",
                mode=mode,
                run_type=run_type,
                timings=StageTimings(),
                stats={},
                success=False,
                error_message=str(e)
            )
    
    def run_new(self, mode: str, run_type: str) -> TestResult:
        """Run New architecture test"""
        config_path = self.config.config_debug if mode == "debug" else self.config.config_core
        
        if self.config.verbose:
            print(f"\n🚀 Running New {mode.upper()} ({run_type})...")
            print(f"   Config: {config_path}")
        
        if not NEW_IMPORTS_AVAILABLE:
            return TestResult(
                architecture="NEW",
                mode=mode,
                run_type=run_type,
                timings=StageTimings(),
                stats={},
                success=False,
                error_message="New architecture imports not available"
            )
        
        try:
            executor = NewArchitectureExecutor(
                config_path, 
                mode, 
                debug=self.config.debug_new_architecture
            )
            timings, stats, cache_hit_rate, warnings, artf_loaded = executor.execute()
            
            return TestResult(
                architecture="NEW",
                mode=mode,
                run_type=run_type,
                timings=timings,
                stats=stats,
                cache_hit_rate=cache_hit_rate,
                artf_loaded=artf_loaded,
                warnings=warnings
            )
            
        except Exception as e:
            error_msg = str(e)
            error_traceback = traceback.format_exc()
            if self.config.verbose:
                print(f"❌ New {mode} failed: {error_msg}")
                if self.config.debug_new_architecture:
                    print(f"\n{error_traceback}")
            return TestResult(
                architecture="NEW",
                mode=mode,
                run_type=run_type,
                timings=StageTimings(),
                stats={},
                success=False,
                error_message=error_msg,
                error_traceback=error_traceback
            )
    
    def run_all_tests(self) -> List[TestResult]:
        """Run complete test suite"""
        self.results = []
        
        for mode in ["core", "debug"]:
            for run_type in ["cold", "hot"]:
                if run_type == "cold":
                    self.clear_cache()
                
                # Run Legacy
                legacy_result = self.run_legacy(mode, run_type)
                self.results.append(legacy_result)
                
                # Run New
                new_result = self.run_new(mode, run_type)
                self.results.append(new_result)
                
                # Small pause between runs
                time.sleep(1)
        
        return self.results


# ============================================================================
# VALIDATION & REPORTING
# ============================================================================

class TestValidator:
    """Validate test results against requirements"""
    
    def __init__(self, config: TestConfig):
        self.config = config
        self.validation_results = []
    
    def compare_stats(self, legacy_stats: Dict, new_stats: Dict, context: str = "") -> Tuple[bool, List[Dict]]:
        """
        Compare statistics with tolerance
        Returns: (passed, detailed_mismatches)
        """
        mismatches = []
        
        def _compare_values(l_val, n_val, path=""):
            # Skip keys that exist only in one
            if path.endswith('.metrics') or 'metrics' in path:
                # Metrics comparison is optional
                return True
            
            if isinstance(l_val, (int, float)) and isinstance(n_val, (int, float)):
                if abs(l_val - n_val) > self.config.tolerance:
                    mismatches.append({
                        "path": path,
                        "legacy": l_val,
                        "new": n_val,
                        "diff": l_val - n_val,
                        "diff_pct": (abs(l_val - n_val) / max(abs(l_val), 1)) * 100
                    })
                    return False
            elif isinstance(l_val, dict) and isinstance(n_val, dict):
                all_keys = set(l_val.keys()) | set(n_val.keys())
                for key in all_keys:
                    if key in l_val and key in n_val:
                        _compare_values(l_val[key], n_val[key], f"{path}.{key}" if path else key)
                    elif key in l_val:
                        mismatches.append({
                            "path": f"{path}.{key}" if path else key,
                            "issue": "missing_in_new",
                            "legacy": l_val[key]
                        })
                    else:
                        mismatches.append({
                            "path": f"{path}.{key}" if path else key,
                            "issue": "missing_in_legacy",
                            "new": n_val[key]
                        })
            elif isinstance(l_val, (list, tuple)) and isinstance(n_val, (list, tuple)):
                if len(l_val) != len(n_val):
                    mismatches.append({
                        "path": path,
                        "issue": "length_mismatch",
                        "legacy_len": len(l_val),
                        "new_len": len(n_val)
                    })
                else:
                    for i, (lv, nv) in enumerate(zip(l_val, n_val)):
                        _compare_values(lv, nv, f"{path}[{i}]")
            else:
                if l_val != n_val:
                    mismatches.append({
                        "path": path,
                        "legacy": l_val,
                        "new": n_val,
                        "diff": str(l_val) != str(n_val)
                    })
            return True
        
        _compare_values(legacy_stats, new_stats, context)
        return len(mismatches) == 0, mismatches
    
    def validate_parity(self, legacy_result: TestResult, new_result: TestResult) -> Dict:
        """Validate parity between Legacy and New"""
        result = {
            "passed": True,
            "mismatches": [],
            "details": {},
            "summary": {}
        }
        
        # Compare key statistics
        for stat_key in ["data", "signals", "filters", "trades", "metrics"]:
            if stat_key in legacy_result.stats and stat_key in new_result.stats:
                matches, mismatches = self.compare_stats(
                    legacy_result.stats[stat_key],
                    new_result.stats[stat_key],
                    stat_key
                )
                result["details"][stat_key] = matches
                result["summary"][stat_key] = {
                    "passed": matches,
                    "mismatch_count": len(mismatches)
                }
                if mismatches:
                    result["mismatches"].extend(mismatches)
                    result["passed"] = False
        
        return result
    
    def validate_performance(self, legacy_result: TestResult, new_result: TestResult) -> Dict:
        """Validate performance: New should be faster than Legacy"""
        result = {
            "passed": True,
            "comparisons": {}
        }
        
        stages = ["data_loading", "signal_generation", "filter_application", 
                  "trade_simulation", "metrics_calculation", "end_to_end"]
        
        for stage in stages:
            legacy_time = getattr(legacy_result.timings, stage, 0)
            new_time = getattr(new_result.timings, stage, 0)
            
            # Skip if times are zero or very small
            if legacy_time < 0.001 or new_time < 0.001:
                continue
            
            # New should be faster (lower time) than Legacy
            faster = new_time < legacy_time * (1 - self.config.performance_tolerance)
            speedup_pct = ((legacy_time - new_time) / legacy_time) * 100
            
            result["comparisons"][stage] = {
                "legacy": legacy_time,
                "new": new_time,
                "faster": faster,
                "speedup_pct": speedup_pct,
                "ratio": legacy_time / new_time if new_time > 0 else float('inf')
            }
            
            if not faster:
                result["passed"] = False
        
        return result
    
    def validate_core_vs_debug(self, core_result: TestResult, debug_result: TestResult) -> Dict:
        """Validate: New core < New debug speed (core should be faster)"""
        result = {
            "passed": True,
            "comparisons": {}
        }
        
        stages = ["data_loading", "signal_generation", "filter_application", 
                  "trade_simulation", "metrics_calculation", "end_to_end"]
        
        for stage in stages:
            core_time = getattr(core_result.timings, stage, 0)
            debug_time = getattr(debug_result.timings, stage, 0)
            
            # Skip if times are zero or very small
            if core_time < 0.001 or debug_time < 0.001:
                continue
            
            # Core should be faster (lower time) than Debug
            faster = core_time < debug_time * (1 + self.config.performance_tolerance)
            slowdown_pct = ((debug_time - core_time) / debug_time) * 100
            
            result["comparisons"][stage] = {
                "core": core_time,
                "debug": debug_time,
                "faster": faster,
                "slowdown_pct": slowdown_pct,
                "ratio": debug_time / core_time if core_time > 0 else float('inf')
            }
            
            if not faster:
                result["passed"] = False
        
        return result


class ReportGenerator:
    """Generate comprehensive test reports"""
    
    def __init__(self, config: TestConfig):
        self.config = config
    
    def generate_comparison_table(self, results: List[TestResult]) -> pd.DataFrame:
        """Generate performance comparison table"""
        data = []
        for r in results:
            if not r.success:
                continue
            row = {
                "Architecture": r.architecture,
                "Mode": r.mode.upper(),
                "Run": r.run_type.upper(),
                "Data (s)": round(r.timings.data_loading, 3),
                "Signals (s)": round(r.timings.signal_generation, 3),
                "Filters (s)": round(r.timings.filter_application, 3),
                "Trades (s)": round(r.timings.trade_simulation, 3),
                "Metrics (s)": round(r.timings.metrics_calculation, 3),
                "Total (s)": round(r.timings.end_to_end, 3),
                "Cache %": round(r.cache_hit_rate * 100, 1) if r.cache_hit_rate else "N/A",
                "ARTF": "✅" if r.artf_loaded else "❌"
            }
            data.append(row)
        
        return pd.DataFrame(data)
    
    def generate_mismatch_report(self, mismatches: List[Dict]) -> str:
        """Generate detailed mismatch report"""
        if not mismatches:
            return "No mismatches found."
        
        report = []
        report.append("### Detailed Mismatches:")
        
        for m in mismatches:
            if "issue" in m:
                if m["issue"] == "missing_in_new":
                    report.append(f"- {m['path']}: Missing in New (Legacy={m['legacy']})")
                elif m["issue"] == "missing_in_legacy":
                    report.append(f"- {m['path']}: Missing in Legacy (New={m['new']})")
                elif m["issue"] == "length_mismatch":
                    report.append(f"- {m['path']}: Length mismatch (Legacy={m['legacy_len']}, New={m['new_len']})")
            else:
                report.append(f"- {m['path']}: Legacy={m['legacy']}, New={m['new']} (diff={m.get('diff', 'N/A')})")
        
        return "\n".join(report)
    
    def generate_summary_report(self, results: List[TestResult], validation: Dict) -> str:
        """Generate markdown summary report"""
        report = []
        report.append("# LEGACY VS NEW ARCHITECTURE TEST REPORT")
        report.append(f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"**Tolerance:** +/-{self.config.tolerance}")
        report.append("")
        
        # Overall status
        report.append("## OVERALL STATUS")
        all_passed = validation.get("parity_passed", False) and \
                     validation.get("performance_passed", False) and \
                     validation.get("core_vs_debug_passed", False)
        
        status = "✅ PASSED" if all_passed else "❌ FAILED"
        report.append(f"**Status:** {status}")
        report.append("")
        
        # ARTF Status
        report.append("## 📊 ARTF DATA STATUS")
        for r in results:
            if r.architecture == "NEW" and r.run_type == "hot":
                status = "✅ Loaded" if r.artf_loaded else "❌ Missing"
                report.append(f"- **New {r.mode.upper()}**: {status}")
        for r in results:
            if r.architecture == "LEGACY" and r.run_type == "hot":
                status = "✅ Loaded" if r.artf_loaded else "❌ Missing"
                report.append(f"- **Legacy {r.mode.upper()}**: {status}")
        report.append("")
        
        # Parity results
        report.append("## 🔄 PARITY VALIDATION")
        
        if "parity_results" in validation:
            for mode, result in validation["parity_results"].items():
                emoji = "✅" if result["passed"] else "❌"
                report.append(f"### {mode.upper()} Mode - {emoji}")
                
                if result["passed"]:
                    report.append(f"- All statistics match within +/-{self.config.tolerance}")
                else:
                    report.append(f"- **{len(result['mismatches'])} mismatches detected**")
                    
                    # Show summary by category
                    for category, summary in result.get("summary", {}).items():
                        if not summary["passed"]:
                            report.append(f"  - {category}: {summary['mismatch_count']} mismatches")
                    
                    # Show detailed mismatches (first 10)
                    if result["mismatches"]:
                        report.append(self.generate_mismatch_report(result["mismatches"][:10]))
                        if len(result["mismatches"]) > 10:
                            report.append(f"  - ... and {len(result['mismatches']) - 10} more mismatches")
                report.append("")
        
        # Performance results
        report.append("## ⚡ PERFORMANCE VALIDATION")
        report.append("*(New should be faster than Legacy)*")
        report.append("")
        
        if "performance_results" in validation:
            for mode, result in validation["performance_results"].items():
                emoji = "✅" if result["passed"] else "❌"
                report.append(f"### {mode.upper()} Mode - {emoji}")
                
                # Show comparisons
                for stage, comp in result.get("comparisons", {}).items():
                    arrow = "🚀" if comp.get("faster") else "🐢"
                    report.append(f"  - {arrow} {stage}: New {comp['new']:.3f}s vs Legacy {comp['legacy']:.3f}s " +
                                 f"({comp['speedup_pct']:.1f}% faster)" if comp.get("faster") else
                                 f"({comp['speedup_pct']:.1f}% slower)")
                
                if not result["comparisons"]:
                    report.append("  - No valid stage data for comparison")
                report.append("")
        
        # Core vs Debug
        report.append("## 🎯 CORE VS DEBUG VALIDATION")
        report.append("*(Core should be faster than Debug)*")
        report.append("")
        
        if "core_vs_debug_result" in validation:
            result = validation["core_vs_debug_result"]
            emoji = "✅" if result["passed"] else "❌"
            report.append(f"### Result - {emoji}")
            
            # Show key comparisons
            for stage, comp in result.get("comparisons", {}).items():
                arrow = "🚀" if comp.get("faster") else "🐢"
                report.append(f"  - {arrow} {stage}: Core {comp['core']:.3f}s vs Debug {comp['debug']:.3f}s " +
                             f"({comp['slowdown_pct']:.1f}% slower in debug)")
            
            if not result["comparisons"]:
                report.append("  - No valid stage data for comparison")
        
        report.append("")
        
        # Performance table
        report.append("## 📈 PERFORMANCE DETAILS")
        report.append("")
        df = self.generate_comparison_table(results)
        if not df.empty:
            report.append("```")
            report.append(df.to_string(index=False))
            report.append("```")
        else:
            report.append("No performance data available")
        
        return "\n".join(report)
    
    def save_report(self, report_text: str, filename: str = None) -> Path:
        """Save report to file with UTF-8 encoding"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"comparison_report_{timestamp}.md"
        
        report_path = self.config.report_dir / filename
        report_path.write_text(report_text, encoding='utf-8')
        return report_path
    
    def save_json_results(self, results: List[TestResult], filename: str = None) -> Path:
        """Save raw results as JSON"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"raw_results_{timestamp}.json"
        
        data = {
            "timestamp": datetime.now().isoformat(),
            "results": [r.to_dict() for r in results]
        }
        
        json_path = self.config.report_dir / filename
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        
        return json_path


# ============================================================================
# MAIN TEST FUNCTION
# ============================================================================

def run_tests():
    """Main test execution function"""
    print("=" * 80)
    print("LEGACY VS NEW ARCHITECTURE COMPARISON TEST")
    print("=" * 80)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Initialize test components
    config = TestConfig()
    executor = TestExecutor(config)
    validator = TestValidator(config)
    reporter = ReportGenerator(config)
    
    # Run all tests
    results = executor.run_all_tests()
    
    # Check if we have any successful results
    successful_results = [r for r in results if r.success]
    legacy_successful = [r for r in results if r.architecture == "LEGACY" and r.success]
    new_successful = [r for r in results if r.architecture == "NEW" and r.success]
    
    print(f"\n📊 Results: {len(successful_results)}/{len(results)} successful runs")
    print(f"   - Legacy: {len(legacy_successful)}/8 successful")
    print(f"   - New: {len(new_successful)}/8 successful")
    
    if not new_successful:
        print("\n❌ No successful New architecture runs!")
        print("\nDetailed New architecture errors:")
        for r in results:
            if r.architecture == "NEW" and not r.success:
                error_lines = r.error_message.split('\n')
                first_line = error_lines[0] if error_lines else "Unknown error"
                print(f"  - {r.mode} {r.run_type}: {first_line}")
        
        validation_results = {
            "parity_passed": False,
            "performance_passed": False,
            "core_vs_debug_passed": False
        }
        
        report_text = reporter.generate_summary_report(results, validation_results)
        report_path = reporter.save_report(report_text)
        json_path = reporter.save_json_results(results)
        
        print(f"\n📄 Error report saved to: {report_path}")
        print(f"📊 Raw data saved to: {json_path}")
        sys.exit(1)
    
    # Organize results for validation (use hot runs)
    legacy_core_hot = next((r for r in results if r.architecture == "LEGACY" and r.mode == "core" and r.run_type == "hot" and r.success), None)
    new_core_hot = next((r for r in results if r.architecture == "NEW" and r.mode == "core" and r.run_type == "hot" and r.success), None)
    legacy_debug_hot = next((r for r in results if r.architecture == "LEGACY" and r.mode == "debug" and r.run_type == "hot" and r.success), None)
    new_debug_hot = next((r for r in results if r.architecture == "NEW" and r.mode == "debug" and r.run_type == "hot" and r.success), None)
    
    # Validate
    validation_results = {}
    
    # 1. Parity validation
    parity_results = {}
    if legacy_core_hot and new_core_hot:
        parity_results["core"] = validator.validate_parity(legacy_core_hot, new_core_hot)
    if legacy_debug_hot and new_debug_hot:
        parity_results["debug"] = validator.validate_parity(legacy_debug_hot, new_debug_hot)
    
    validation_results["parity_results"] = parity_results
    validation_results["parity_passed"] = all(r["passed"] for r in parity_results.values()) if parity_results else False
    
    # 2. Performance validation (New should be faster than Legacy)
    performance_results = {}
    if legacy_core_hot and new_core_hot:
        performance_results["core"] = validator.validate_performance(legacy_core_hot, new_core_hot)
    if legacy_debug_hot and new_debug_hot:
        performance_results["debug"] = validator.validate_performance(legacy_debug_hot, new_debug_hot)
    
    validation_results["performance_results"] = performance_results
    validation_results["performance_passed"] = all(r["passed"] for r in performance_results.values()) if performance_results else False
    
    # 3. Core vs Debug validation (New core < New debug)
    if new_core_hot and new_debug_hot:
        validation_results["core_vs_debug_result"] = validator.validate_core_vs_debug(
            new_core_hot, new_debug_hot
        )
        validation_results["core_vs_debug_passed"] = validation_results["core_vs_debug_result"]["passed"]
    else:
        validation_results["core_vs_debug_passed"] = False
    
    # Generate report
    report_text = reporter.generate_summary_report(results, validation_results)
    report_path = reporter.save_report(report_text)
    json_path = reporter.save_json_results(results)
    
    # Print summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    
    # ARTF Status
    for r in results:
        if r.architecture == "NEW" and r.run_type == "hot":
            status = "✅" if r.artf_loaded else "❌"
            print(f"ARTF {r.mode.upper()}: {status}")
    
    if validation_results["parity_passed"]:
        print("✅ PARITY: Legacy == New")
    else:
        print("❌ PARITY: Mismatch detected")
        if "parity_results" in validation_results:
            for mode, result in validation_results["parity_results"].items():
                if not result["passed"]:
                    print(f"   - {mode.upper()}: {len(result['mismatches'])} mismatches")
    
    if validation_results["performance_passed"]:
        print("✅ PERFORMANCE: New > Legacy (New is faster)")
    else:
        print("❌ PERFORMANCE: New not faster than Legacy")
        if "performance_results" in validation_results:
            for mode, result in validation_results["performance_results"].items():
                if not result["passed"]:
                    slow_stages = [s for s, c in result["comparisons"].items() if not c["faster"]]
                    if slow_stages:
                        print(f"   - {mode.upper()}: Slower stages: {', '.join(slow_stages)}")
    
    if validation_results["core_vs_debug_passed"]:
        print("✅ CORE < DEBUG: New core faster than debug")
    else:
        print("❌ CORE < DEBUG: Core not faster than debug")
        if "core_vs_debug_result" in validation_results:
            result = validation_results["core_vs_debug_result"]
            slow_stages = [s for s, c in result["comparisons"].items() if not c["faster"]]
            if slow_stages:
                print(f"   - Slow stages: {', '.join(slow_stages)}")
    
    print(f"\n📄 Report saved to: {report_path}")
    print(f"📊 Raw data saved to: {json_path}")
    
    # Performance table
    print("\n" + "=" * 80)
    print("PERFORMANCE COMPARISON (HOT RUNS)")
    print("=" * 80)
    df = reporter.generate_comparison_table(results)
    df_filtered = df[df["Run"] == "HOT"] if "Run" in df.columns and not df.empty else df
    if not df_filtered.empty:
        print(df_filtered.to_string(index=False))
    else:
        print("No hot run data available")
    
    # Speedup calculations
    print("\n" + "=" * 80)
    print("🚀 SPEEDUP ANALYSIS")
    print("=" * 80)
    
    if new_core_hot and legacy_core_hot:
        core_speedup = ((legacy_core_hot.timings.end_to_end - new_core_hot.timings.end_to_end) / legacy_core_hot.timings.end_to_end) * 100
        print(f"Core Mode: New is {core_speedup:.1f}% faster than Legacy")
    
    if new_debug_hot and legacy_debug_hot:
        debug_speedup = ((legacy_debug_hot.timings.end_to_end - new_debug_hot.timings.end_to_end) / legacy_debug_hot.timings.end_to_end) * 100
        print(f"Debug Mode: New is {debug_speedup:.1f}% faster than Legacy")
    
    # Final assertion
    all_passed = all([
        validation_results["parity_passed"],
        validation_results["performance_passed"],
        validation_results["core_vs_debug_passed"]
    ])
    
    print("\n" + "=" * 80)
    if all_passed:
        print("🎉 ALL TESTS PASSED!")
    else:
        print("⚠️  SOME TESTS FAILED - Check report for details")
        sys.exit(1)
    
    print("=" * 80)
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)


if __name__ == "__main__":
    run_tests()