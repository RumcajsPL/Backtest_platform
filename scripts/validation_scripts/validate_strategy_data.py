"""
Strategy Data Validator
Validates data quality and compatibility for strategy execution.
Can be run standalone or called from strategy runner.
"""
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import warnings

# Get project root
project_root = Path(__file__).resolve().parent.parent

# Add to Python path
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'src'))

warnings.filterwarnings('ignore')

class StrategyDataValidator:
    """
    Validates data for strategy execution.
    Focuses only on data quality and compatibility checks.
    """
    
    def __init__(self, config_path: str, verbose: bool = False):
        """
        Initialize validator with configuration.
        
        Args:
            config_path: Path to strategy configuration YAML
            verbose: Detailed output mode
        """
        self.config_path = Path(config_path)
        self.config = None
        self.data = None
        self.verbose = verbose
        self.validation_results = {}
        
        # Get project root (Backtest_platform folder)
        # Validator is at scripts/validation_scripts/validate_strategy_data.py
        # Need to go up 3 levels: validation_scripts → scripts → project_root
        script_dir = Path(__file__).resolve().parent  # scripts/validation_scripts
        self.project_root = script_dir.parent.parent  # scripts → project_root
        print(f"📁 Project root: {self.project_root}")
        
        # Add to Python path if needed
        if str(self.project_root) not in sys.path:
            sys.path.insert(0, str(self.project_root))
        if str(self.project_root / 'src') not in sys.path:
            sys.path.insert(0, str(self.project_root / 'src'))
    
    def load_configuration(self):
        """Load strategy configuration from YAML."""
        print("📋 Loading strategy configuration...")
        
        config_file = self.resolve_config_path(self.config_path)
        
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_file}")
        
        try:
            import yaml
            with open(config_file, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
            
            strategy_name = self.config.get('strategy', {}).get('name', 'Unnamed Strategy')
            print(f"   ✅ Configuration loaded: {strategy_name}")
            
        except Exception as e:
            raise Exception(f"Configuration loading failed: {e}")
    
    def resolve_config_path(self, config_arg):
        """Resolve configuration file path."""
        config_path = Path(config_arg)
        
        if config_path.is_absolute():
            return config_path
        
        # Check relative to current working directory
        cwd_path = Path.cwd() / config_path
        if cwd_path.exists():
            return cwd_path
        
        # Check relative to project root
        project_path = self.project_root / config_path
        if project_path.exists():
            return project_path
        
        # Check if it's just a filename in WBWS config directory
        wbws_config_path = self.project_root / 'src' / 'config' / 'WBWS' / config_path.name
        if wbws_config_path.exists():
            return wbws_config_path
        
        # Return what we tried for error reporting
        return project_path
    
    def load_data(self):
        """Load data for validation."""
        print("📊 Loading data for validation...")
        
        if not self.config:
            raise ValueError("Configuration not loaded")
        
        data_config = self.config.get('data', {})
        data_file = data_config.get('file')
        
        if not data_file:
            raise ValueError("No data file specified in configuration")
        
        # Resolve data file path
        data_path = Path(data_file)
        
        # If not absolute, try multiple locations
        if not data_path.is_absolute():
            # Try relative to project root first
            project_path = self.project_root / data_file
            if project_path.exists():
                data_path = project_path
            else:
                # Try as is (might be relative to current directory)
                cwd_path = Path.cwd() / data_file
                if cwd_path.exists():
                    data_path = cwd_path
                else:
                    raise FileNotFoundError(
                        f"Data file not found: {data_file}\n"
                        f"Tried:\n"
                        f"  1. {data_file} (as given)\n"
                        f"  2. {project_path}\n"
                        f"  3. {cwd_path}"
                    )
        
        print(f"   Source: {data_path}")
        
        # Verify file exists
        if not data_path.exists():
            raise FileNotFoundError(f"Data file not found: {data_path}")
        
        # Load data
        if data_path.suffix.lower() == '.csv':
            self.data = pd.read_csv(data_path, parse_dates=['timestamp'])
        elif data_path.suffix.lower() == '.parquet':
            self.data = pd.read_parquet(data_path)
        else:
            raise ValueError(f"Unsupported format: {data_path.suffix}")
        
        print(f"   Loaded {len(self.data):,} rows")
        
        # Set timestamp as index
        if 'timestamp' in self.data.columns:
            self.data = self.data.set_index('timestamp')
        
        # Sort by timestamp
        self.data = self.data.sort_index()
        
        # Standardize column names
        self.data.columns = self.data.columns.str.lower()
        
        return self.data
    
    def validate_schema(self):
        """Validate data schema and required columns."""
        print("\n🔍 Validating data schema...")
        
        results = {}
        
        # Check required columns
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        missing_cols = [col for col in required_cols if col not in self.data.columns]
        
        if missing_cols:
            results['schema'] = {
                'status': 'FAIL',
                'message': f"Missing required columns: {missing_cols}",
                'available_columns': list(self.data.columns)
            }
        else:
            results['schema'] = {
                'status': 'PASS',
                'message': f"All required columns present: {required_cols}"
            }
        
        # Check index type
        if not isinstance(self.data.index, pd.DatetimeIndex):
            results['index'] = {
                'status': 'FAIL',
                'message': "Index is not DatetimeIndex"
            }
        else:
            results['index'] = {
                'status': 'PASS',
                'message': "Index is DatetimeIndex"
            }
        
        self.validation_results['schema'] = results
        return results
    
    def validate_data_quality(self):
        """Validate data quality metrics."""
        print("🔍 Validating data quality...")
        
        results = {}
        
        # Check for missing values
        missing = self.data[['open', 'high', 'low', 'close', 'volume']].isnull().sum()
        total_missing = missing.sum()
        
        results['missing_values'] = {
            'status': 'PASS' if total_missing == 0 else 'WARNING',
            'message': f"Missing values found: {total_missing}" if total_missing > 0 else "No missing values",
            'details': missing.to_dict()
        }
        
        # Check for duplicate timestamps
        duplicate_count = self.data.index.duplicated().sum()
        results['duplicates'] = {
            'status': 'PASS' if duplicate_count == 0 else 'WARNING',
            'message': f"Found {duplicate_count} duplicate timestamps" if duplicate_count > 0 else "No duplicate timestamps",
            'count': int(duplicate_count)
        }
        
        # Check OHLC consistency
        invalid_ohlc = (
            (self.data['high'] < self.data['low']) |
            (self.data['open'] > self.data['high']) |
            (self.data['open'] < self.data['low']) |
            (self.data['close'] > self.data['high']) |
            (self.data['close'] < self.data['low'])
        )
        invalid_count = invalid_ohlc.sum()
        
        results['ohlc_consistency'] = {
            'status': 'PASS' if invalid_count == 0 else 'FAIL',
            'message': f"Found {invalid_count} bars with invalid OHLC relationships" if invalid_count > 0 else "OHLC consistency OK",
            'count': int(invalid_count)
        }
        
        # Check for non-positive prices
        price_cols = ['open', 'high', 'low', 'close']
        negative_prices = {}
        for col in price_cols:
            neg_count = (self.data[col] <= 0).sum()
            if neg_count > 0:
                negative_prices[col] = int(neg_count)
        
        results['price_validation'] = {
            'status': 'PASS' if not negative_prices else 'FAIL',
            'message': f"Found negative prices: {negative_prices}" if negative_prices else "All prices positive",
            'details': negative_prices
        }
        
        # Check timestamp monotonicity
        is_monotonic = self.data.index.is_monotonic_increasing
        results['timestamp_order'] = {
            'status': 'PASS' if is_monotonic else 'FAIL',
            'message': "Timestamps are monotonic increasing" if is_monotonic else "Timestamps are not monotonic increasing"
        }
        
        self.validation_results['quality'] = results
        return results
    
    def validate_time_continuity(self):
        """Validate time continuity and gaps."""
        print("🔍 Validating time continuity...")
        
        results = {}
        
        if not isinstance(self.data.index, pd.DatetimeIndex):
            results['continuity'] = {
                'status': 'SKIP',
                'message': "Cannot check continuity without DatetimeIndex"
            }
            return results
        
        # Calculate time differences
        time_diffs = self.data.index.to_series().diff()
        
        # Remove first NaN
        time_diffs = time_diffs.dropna()
        
        if len(time_diffs) == 0:
            results['continuity'] = {
                'status': 'SKIP',
                'message': "Not enough data points for continuity check"
            }
            return results
        
        # Get expected timeframe from config
        timeframe = self.config.get('indicator', {}).get('base_timeframe', '1min')
        
        # Convert timeframe to timedelta
        if timeframe == '1min':
            expected_diff = pd.Timedelta(minutes=1)
        elif 'min' in timeframe:
            minutes = int(timeframe.replace('min', ''))
            expected_diff = pd.Timedelta(minutes=minutes)
        elif timeframe == '1H':
            expected_diff = pd.Timedelta(hours=1)
        else:
            expected_diff = None
        
        if expected_diff:
            # For market data, allow gaps up to 24 hours (weekends/holidays)
            # Only flag gaps longer than 24 hours as warnings
            max_allowed_gap = pd.Timedelta(hours=24)
            
            # Check for large gaps (> 24 hours)
            large_gaps = time_diffs[time_diffs > max_allowed_gap]
            large_gap_count = len(large_gaps)
            
            # Check for regular gaps (> expected timeframe but < 24 hours)
            regular_gaps = time_diffs[(time_diffs > expected_diff) & (time_diffs <= max_allowed_gap)]
            regular_gap_count = len(regular_gaps)
            
            total_gaps = large_gap_count + regular_gap_count
            
            if total_gaps > 0:
                if large_gap_count > 0:
                    max_gap = large_gaps.max() if not large_gaps.empty else regular_gaps.max()
                    avg_gap = large_gaps.mean() if not large_gaps.empty else regular_gaps.mean()
                    
                    results['gaps'] = {
                        'status': 'WARNING' if large_gap_count > 10 else 'INFO',
                        'message': f"Found {total_gaps} time gaps ({large_gap_count} > 24h, {regular_gap_count} regular)",
                        'large_gaps_count': int(large_gap_count),
                        'regular_gaps_count': int(regular_gap_count),
                        'max_gap': str(max_gap),
                        'avg_gap': str(avg_gap)
                    }
                else:
                    # Only regular gaps (expected for market data)
                    results['gaps'] = {
                        'status': 'INFO',
                        'message': f"Found {regular_gap_count} regular time gaps (market closures, weekends)",
                        'regular_gaps_count': int(regular_gap_count)
                    }
            else:
                results['gaps'] = {
                    'status': 'PASS',
                    'message': "No significant time gaps found"
                }
        
        # Check data frequency distribution
        freq_counts = time_diffs.value_counts().head(10)  # Top 10 most common intervals
        
        # Convert Timedelta keys to strings for JSON serialization
        freq_counts_dict = {}
        for td, count in freq_counts.items():
            freq_counts_dict[str(td)] = int(count)
        
        results['frequency_distribution'] = {
            'status': 'INFO',
            'message': f"Most common time intervals: {freq_counts_dict}",
            'distribution': freq_counts_dict
        }
        
        self.validation_results['continuity'] = results
        return results
    
    def validate_statistics(self):
        """Calculate and validate basic statistics."""
        print("🔍 Calculating data statistics...")
        
        results = {}
        
        # Basic statistics
        stats = self.data[['open', 'high', 'low', 'close']].describe()
        
        results['basic_stats'] = {
            'status': 'INFO',
            'message': "Basic statistics calculated",
            'statistics': stats.to_dict()
        }
        
        # Volume statistics
        if 'volume' in self.data.columns:
            volume_stats = self.data['volume'].describe()
            results['volume_stats'] = {
                'status': 'INFO',
                'message': "Volume statistics calculated",
                'statistics': volume_stats.to_dict()
            }
        
        # Price changes
        price_changes = self.data['close'].pct_change().dropna()
        large_moves = price_changes.abs() > 0.05  # 5% moves
        
        if large_moves.any():
            large_move_count = large_moves.sum()
            max_move = price_changes.abs().max() * 100
            
            results['price_changes'] = {
                'status': 'WARNING' if large_move_count > 10 else 'INFO',
                'message': f"Found {large_move_count} price moves > 5% (max: {max_move:.2f}%)",
                'large_move_count': int(large_move_count),
                'max_move_percent': float(max_move)
            }
        
        self.validation_results['statistics'] = results
        return results
    
    def validate_compatibility(self):
        """Validate compatibility with strategy requirements."""
        print("🔍 Validating strategy compatibility...")
        
        results = {}
        
        # Check minimum data length for indicators
        rsi_config = self.config.get('filters', {}).get('rsi_filter', {})
        if rsi_config.get('enabled', False):
            rsi_length = rsi_config.get('length', 14)
            
            if len(self.data) < rsi_length:
                results['indicator_compatibility'] = {
                    'status': 'FAIL',
                    'message': f"Not enough data for RSI (length={rsi_length}). Need at least {rsi_length} bars, have {len(self.data)}"
                }
            else:
                results['indicator_compatibility'] = {
                    'status': 'PASS',
                    'message': f"Sufficient data for RSI (length={rsi_length})"
                }
        
        # Check date range if specified in config
        data_config = self.config.get('data', {})
        date_range = data_config.get('date_range')
        
        if date_range:
            start_date = pd.to_datetime(date_range.get('start'))
            end_date = pd.to_datetime(date_range.get('end'))
            
            if start_date and end_date:
                data_start = self.data.index.min()
                data_end = self.data.index.max()
                
                if data_start > start_date or data_end < end_date:
                    results['date_range'] = {
                        'status': 'WARNING',
                        'message': f"Data range ({data_start} to {data_end}) doesn't fully cover requested range ({start_date} to {end_date})"
                    }
                else:
                    results['date_range'] = {
                        'status': 'PASS',
                        'message': f"Data fully covers requested date range"
                    }
        
        self.validation_results['compatibility'] = results
        return results
    
    def clean_for_serialization(self, data):
        """Recursively clean data for JSON serialization."""
        if isinstance(data, dict):
            return {str(k) if not isinstance(k, (str, int, float, bool)) and k is not None else k: 
                    self.clean_for_serialization(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self.clean_for_serialization(item) for item in data]
        elif isinstance(data, (pd.Timestamp, datetime)):
            return data.isoformat()
        elif isinstance(data, pd.Timedelta):
            return str(data)
        elif isinstance(data, pd.DataFrame):
            return data.to_dict()
        elif isinstance(data, pd.Series):
            return data.to_dict()
        elif isinstance(data, np.integer):
            return int(data)
        elif isinstance(data, np.floating):
            return float(data)
        elif isinstance(data, np.ndarray):
            return data.tolist()
        elif isinstance(data, (str, int, float, bool)) or data is None:
            return data
        elif hasattr(data, 'to_dict'):
            return data.to_dict()
        else:
            return str(data)  # Last resort: convert to string
    
    def run_all_validations(self):
        """Run all validation checks."""
        print("\n" + "="*70)
        print("🔬 STRATEGY DATA VALIDATION")
        print("="*70)
        
        try:
            # Load configuration and data
            self.load_configuration()
            self.load_data()
            
            # Run validations
            schema_results = self.validate_schema()
            quality_results = self.validate_data_quality()
            continuity_results = self.validate_time_continuity()
            stats_results = self.validate_statistics()
            compat_results = self.validate_compatibility()
            
            # Compile overall status
            self.compile_overall_status()
            
            # Print summary
            self.print_summary()
            
            # Save report
            report_file = self.save_report()
            
            # Show relative path for cleaner output
            try:
                relative_path = report_file.relative_to(self.project_root)
                print(f"\n✅ Validation completed. Report saved to: {relative_path}")
            except ValueError:
                # If path is not relative to project root, show absolute path
                print(f"\n✅ Validation completed. Report saved to: {report_file}")
            
            return self.validation_results
            
        except Exception as e:
            print(f"\n❌ Validation failed: {e}")
            if self.verbose:
                import traceback
                traceback.print_exc()
            raise
    
    def compile_overall_status(self):
        """Compile overall validation status."""
        all_results = []
        
        for category, results in self.validation_results.items():
            for check_name, check_result in results.items():
                if 'status' in check_result:
                    all_results.append(check_result['status'])
        
        # Determine overall status
        if 'FAIL' in all_results:
            overall_status = 'FAIL'
        elif 'WARNING' in all_results:
            overall_status = 'WARNING'
        elif all(all(r == 'PASS' or r == 'INFO' for r in all_results)):
            overall_status = 'PASS'
        else:
            overall_status = 'UNKNOWN'
        
        self.validation_results['overall'] = {
            'status': overall_status,
            'timestamp': datetime.now().isoformat(),
            'data_summary': {
                'rows': len(self.data),
                'columns': list(self.data.columns),
                'date_range': {
                    'start': self.data.index.min().isoformat(),
                    'end': self.data.index.max().isoformat()
                }
            }
        }
    
    def print_summary(self):
        """Print validation summary."""
        print("\n" + "="*70)
        print("📊 VALIDATION SUMMARY")
        print("="*70)
        
        overall = self.validation_results.get('overall', {})
        print(f"Overall Status: {overall.get('status', 'UNKNOWN')}")
        
        if 'data_summary' in overall:
            data = overall['data_summary']
            print(f"Data: {data['rows']:,} rows, {len(data['columns'])} columns")
            print(f"Date Range: {data['date_range']['start']} to {data['date_range']['end']}")
        
        print("\nDetailed Results:")
        for category, results in self.validation_results.items():
            if category == 'overall':
                continue
            
            print(f"\n{category.upper()}:")
            for check_name, check_result in results.items():
                status = check_result.get('status', 'UNKNOWN')
                message = check_result.get('message', '')
                print(f"  {check_name}: {status} - {message}")
                
                if self.verbose and 'details' in check_result:
                    for key, value in check_result['details'].items():
                        print(f"    {key}: {value}")
        
        print("="*70)
    
    def save_report(self):
        """Save validation report to file."""
        output_config = self.config.get('output', {})
        
        # Use the exact same structure as your strategy runner
        outputs_dir = self.project_root / output_config.get('outputs_dir', 'outputs')
        reports_dir = outputs_dir / output_config.get('reports_dir', 'reports/WBWS')
        
        # Create directory if it doesn't exist
        reports_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        asset_symbol = self.config.get('asset', {}).get('symbol', 'UNKNOWN')
        report_file = reports_dir / f"data_validation_{asset_symbol}_{timestamp}.json"
        
        # Clean data for serialization
        cleaned_results = self.clean_for_serialization(self.validation_results)
        
        # Save as JSON
        import json
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(cleaned_results, f, indent=2)
        
        return report_file
    
    def is_valid_for_strategy(self):
        """Check if data is valid for strategy execution."""
        overall = self.validation_results.get('overall', {})
        return overall.get('status') in ['PASS', 'WARNING']


def main():
    """Main entry point for standalone validation."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Validate strategy data quality and compatibility')
    parser.add_argument('config', help='Path to strategy configuration YAML file')
    parser.add_argument('--verbose', '-v', action='store_true', help='Detailed output')
    parser.add_argument('--check-only', action='store_true', help='Only check validity, minimal output')
    
    args = parser.parse_args()
    
    try:
        validator = StrategyDataValidator(args.config, verbose=args.verbose)
        results = validator.run_all_validations()
        
        if args.check_only:
            # Minimal output for automation
            overall = results.get('overall', {})
            if validator.is_valid_for_strategy():
                print("VALID")
                sys.exit(0)
            else:
                print("INVALID")
                sys.exit(1)
        else:
            # Return exit code based on validity
            if validator.is_valid_for_strategy():
                sys.exit(0)
            else:
                sys.exit(1)
                
    except Exception as e:
        print(f"❌ Validation error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()