# src/utils/report_generator.py
"""
Enhanced Report Generation Utility

Handles comprehensive report generation with minimal terminal output.
All detailed results go to JSON reports, terminal shows only status.
"""
import json
import os
import pandas as pd
from datetime import datetime
from typing import Dict, Optional, List

class ReportGenerator:
    """
    Handles all report generation and file output operations.
    Follows the principle: Terminal = Status, Reports = Details
    """
    
    def __init__(self, output_config: dict, project_root: Optional[str] = None):
        """
        Initialize report generator.
        
        Args:
            output_config: Output configuration from YAML
            project_root: Project root directory (auto-detected if None)
        """
        self.config = output_config
        
        if project_root is None:
            self.project_root = os.path.abspath(
                os.path.join(os.path.dirname(__file__), '..', '..')
            )
        else:
            self.project_root = project_root
        
        self.verbose = output_config.get('verbose', False)
    
    def save_comprehensive_report(
        self,
        config: dict,
        preprocessing_info: dict,
        execution_stats: dict,
        signals_df: pd.DataFrame,
        report_type: str = 'WBWS'
    ) -> str:
        """
        Save comprehensive execution report with all details.
        
        Args:
            config: Full configuration dictionary
            preprocessing_info: Data preprocessing information
            execution_stats: Indicator execution statistics
            signals_df: DataFrame with signals
            report_type: Report category
            
        Returns:
            Path to saved report file
        """
        if not self.config.get('save_execution_report', True):
            return None
        
        # Get reports directory
        reports_dir = self.config.get('reports_dir', f'outputs/reports/{report_type}')
        reports_path = os.path.join(self.project_root, reports_dir)
        
        # Create directory
        try:
            os.makedirs(reports_path, exist_ok=True)
        except Exception as e:
            print(f"   ⚠️  Warning: Could not create reports directory: {e}")
            return None
        
        # Build comprehensive report
        report = {
            'report_metadata': {
                'generated_at': datetime.now().isoformat(),
                'report_type': report_type,
                'config_name': config.get('name', 'Unnamed')
            },
            'configuration': {
                'name': config.get('name', 'Unnamed'),
                'description': config.get('description', ''),
                'version': config.get('version', '1.0'),
                'asset': config.get('asset', {}),
                'data': {
                    'file': os.path.basename(config['data']['file']),
                    'timeframe': config['data'].get('timeframe', 'N/A'),
                    'format': config['data'].get('format', 'N/A')
                },
                'indicator': config.get('indicator', {})
            },
            'data_preprocessing': preprocessing_info,
            'execution': execution_stats,
            'signal_analysis': self._analyze_signals(signals_df),
            'sample_signals': self._extract_sample_signals(signals_df),
            'candle_distribution': self._analyze_candle_types(signals_df),
            'htf_analysis': self._analyze_htf_conditions(signals_df),
            'reversal_patterns': self._analyze_reversals(signals_df)
        }
        
        # Generate filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = os.path.join(reports_path, f'execution_{timestamp}.json')
        
        # Save report
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            # Terminal: only show file path
            print(f"   📊 Report saved: {os.path.basename(report_file)}")
            
            return report_file
            
        except Exception as e:
            print(f"   ⚠️  Warning: Could not save report: {e}")
            return None
    
    def _analyze_signals(self, signals_df: pd.DataFrame) -> dict:
        """Analyze signal statistics."""
        buy_signals = signals_df[signals_df['we_buy']]
        sell_signals = signals_df[signals_df['we_sell']]
        
        return {
            'total_bars': len(signals_df),
            'buy_signals': {
                'count': int(buy_signals['we_buy'].sum()),
                'percentage': float(buy_signals['we_buy'].sum() / len(signals_df) * 100)
            },
            'sell_signals': {
                'count': int(sell_signals['we_sell'].sum()),
                'percentage': float(sell_signals['we_sell'].sum() / len(signals_df) * 100)
            },
            'total_signals': {
                'count': int(buy_signals['we_buy'].sum() + sell_signals['we_sell'].sum()),
                'percentage': float((buy_signals['we_buy'].sum() + sell_signals['we_sell'].sum()) / len(signals_df) * 100)
            }
        }
    
    def _extract_sample_signals(self, signals_df: pd.DataFrame, n: int = 5) -> dict:
        """Extract sample signals for the report."""
        # Ensure timestamp column
        if 'timestamp' not in signals_df.columns:
            df = signals_df.reset_index()
            df.rename(columns={'index': 'timestamp'}, inplace=True)
        else:
            df = signals_df
        
        buy_signals = df[df['we_buy']].head(n)
        sell_signals = df[df['we_sell']].head(n)
        
        return {
            'buy_samples': [
                {
                    'timestamp': row['timestamp'].isoformat(),
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close']),
                    'volume': float(row['volume']),
                    'candle_type': int(row['candle_type']) if pd.notna(row['candle_type']) else None
                }
                for _, row in buy_signals.iterrows()
            ],
            'sell_samples': [
                {
                    'timestamp': row['timestamp'].isoformat(),
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close']),
                    'volume': float(row['volume']),
                    'candle_type': int(row['candle_type']) if pd.notna(row['candle_type']) else None
                }
                for _, row in sell_signals.iterrows()
            ]
        }
    
    def _analyze_candle_types(self, signals_df: pd.DataFrame) -> dict:
        """Analyze candle type distribution."""
        candle_counts = signals_df['candle_type'].value_counts().sort_index()
        classified_count = signals_df['candle_type'].notna().sum()
        total_bars = len(signals_df)
        
        distribution = {}
        if 1 in candle_counts.index:
            distribution['inside_bars'] = {
                'count': int(candle_counts[1]),
                'percentage': float(candle_counts[1] / classified_count * 100)
            }
        if 2 in candle_counts.index:
            distribution['directional_up'] = {
                'count': int(candle_counts[2]),
                'percentage': float(candle_counts[2] / classified_count * 100)
            }
        if -2 in candle_counts.index:
            distribution['directional_down'] = {
                'count': int(candle_counts[-2]),
                'percentage': float(candle_counts[-2] / classified_count * 100)
            }
        if 3 in candle_counts.index:
            distribution['outside_bars'] = {
                'count': int(candle_counts[3]),
                'percentage': float(candle_counts[3] / classified_count * 100)
            }
        
        return {
            'total_bars': total_bars,
            'classified_bars': int(classified_count),
            'unclassified_bars': int(total_bars - classified_count),
            'classification_rate': float(classified_count / total_bars * 100),
            'distribution': distribution
        }
    
    def _analyze_htf_conditions(self, signals_df: pd.DataFrame) -> dict:
        """Analyze HTF condition distribution."""
        htf_bull_count = int(signals_df['htf_bull'].sum())
        htf_bear_count = int(signals_df['htf_bear'].sum())
        total_bars = len(signals_df)
        htf_neutral = total_bars - htf_bull_count - htf_bear_count
        
        return {
            'htf_bull_bars': {
                'count': htf_bull_count,
                'percentage': float(htf_bull_count / total_bars * 100)
            },
            'htf_bear_bars': {
                'count': htf_bear_count,
                'percentage': float(htf_bear_count / total_bars * 100)
            },
            'htf_neutral_bars': {
                'count': htf_neutral,
                'percentage': float(htf_neutral / total_bars * 100)
            }
        }
    
    def _analyze_reversals(self, signals_df: pd.DataFrame) -> dict:
        """Analyze reversal pattern statistics."""
        rev_2d_2u = int(signals_df['rev_2d_2u'].sum())
        rev_2u_2d = int(signals_df['rev_2u_2d'].sum())
        
        buy_signals = int(signals_df['we_buy'].sum())
        sell_signals = int(signals_df['we_sell'].sum())
        
        return {
            'reversals_2d_to_2u': {
                'count': rev_2d_2u,
                'converted_to_buy': buy_signals,
                'conversion_rate': float(buy_signals / rev_2d_2u * 100) if rev_2d_2u > 0 else 0
            },
            'reversals_2u_to_2d': {
                'count': rev_2u_2d,
                'converted_to_sell': sell_signals,
                'conversion_rate': float(sell_signals / rev_2u_2d * 100) if rev_2u_2d > 0 else 0
            }
        }
    
    def print_minimal_summary(self, execution_stats: dict):
        """Print minimal execution summary to terminal (status only)."""
        if self.verbose:
            # Verbose mode - show more details
            print(f"\n{'='*70}")
            print(f"📊 EXECUTION SUMMARY")
            print(f"{'='*70}")
            print(f"HTF Period:     {execution_stats.get('htf_period', 'N/A')}")
            print(f"Bars Processed: {execution_stats.get('total_bars', 0):,}")
            
            if 'signals' in execution_stats:
                sig = execution_stats['signals']
                print(f"Buy Signals:    {sig.get('buy', 0):,}")
                print(f"Sell Signals:   {sig.get('sell', 0):,}")
                print(f"Total Signals:  {sig.get('total', 0):,}")
            
            print(f"{'='*70}\n")
        else:
            # Minimal mode - just counts
            if 'signals' in execution_stats:
                sig = execution_stats['signals']
                total = sig.get('total', 0)
                buy = sig.get('buy', 0)
                sell = sig.get('sell', 0)
                print(f"   ✅ Signals calculated: {total:,} total ({buy:,} buy, {sell:,} sell)")
    
    def save_signals_csv(self, signals_df: pd.DataFrame, asset_symbol: str = '') -> Optional[str]:
        """Export signals to CSV file (optional)."""
        if not self.config.get('save_signals_csv', False):
            return None
        
        signals_file = self.config.get('signals_file', 'outputs/signals/signals_{timestamp}.csv')
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        signals_file = signals_file.replace('{timestamp}', timestamp)
        if asset_symbol:
            signals_file = signals_file.replace('{symbol}', asset_symbol)
        
        signals_path = os.path.join(self.project_root, signals_file)
        os.makedirs(os.path.dirname(signals_path), exist_ok=True)
        
        export_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume',
                       'candle_type', 'we_buy', 'we_sell']
        
        if 'timestamp' not in signals_df.columns:
            signals_df_export = signals_df.reset_index()
            signals_df_export.rename(columns={'index': 'timestamp'}, inplace=True)
        else:
            signals_df_export = signals_df.copy()
        
        available_cols = [col for col in export_cols if col in signals_df_export.columns]
        
        try:
            signals_df_export[available_cols].to_csv(signals_path, index=False)
            print(f"   💾 Signals CSV: {os.path.basename(signals_path)}")
            return signals_path
        except Exception as e:
            print(f"   ⚠️  Warning: Could not export signals CSV: {e}")
            return None


def create_report_generator(config: dict) -> ReportGenerator:
    """
    Factory function to create ReportGenerator from config.
    
    Args:
        config: Full configuration dictionary with 'output' section
        
    Returns:
        ReportGenerator instance
    """
    output_config = config.get('output', {})
    return ReportGenerator(output_config)