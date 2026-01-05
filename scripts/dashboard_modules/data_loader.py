# UPDATE TO: scripts/dashboard_modules/data_loader.py

"""
Dashboard Data Loader Module
Enhanced to prioritize progressive CSV as primary data source
"""
import json
import pandas as pd
from pathlib import Path
from typing import Dict, Optional, Tuple
import warnings

class DashboardDataLoader:
    def __init__(self, report_path: str):
        self.report_path = Path(report_path).resolve()
        self.project_root = self.report_path.parent.parent.parent.parent
        
        self.report_data = None
        self.progressive_df = None  # Primary data source
        self.trades_df = None       # Derived from progressive data (for compatibility)
        self.config = None
        self.data_loaded = False
        
    def load_all_data(self) -> Tuple[Dict, Optional[pd.DataFrame], Optional[Dict]]:
        """
        Load all data needed for dashboard with progressive CSV as primary source
        
        Returns:
            Tuple of (report_data, trades_df, config)
            trades_df is now derived from progressive data when available
        """
        print("📊 Loading dashboard data...")
        
        # 1. Load JSON report
        self.report_data = self._load_json_report()
        
        # 2. Load progressive CSV as primary data source
        self.progressive_df = self._load_progressive_csv()
        
        # 3. Extract trades from progressive data (for backward compatibility)
        self.trades_df = self._extract_trades_from_progressive()
        
        # 4. If no progressive data, fall back to traditional CSV
        if self.trades_df is None or self.trades_df.empty:
            print("⚠️  No progressive data available, falling back to trade details CSV")
            self.trades_df = self._load_legacy_trades_csv()
        
        # 5. Extract config from report
        self.config = self.report_data.get('config', {})
        
        self.data_loaded = True
        return self.report_data, self.trades_df, self.config
    
    def _load_json_report(self) -> Dict:
        """Load and validate JSON report"""
        if not self.report_path.exists():
            raise FileNotFoundError(f"Report file not found: {self.report_path}")
        
        with open(self.report_path, 'r', encoding='utf-8') as f:
            report_data = json.load(f)
        
        # Basic validation
        required_keys = ['execution_time', 'signal_flow', 'simulation_results']
        for key in required_keys:
            if key not in report_data:
                warnings.warn(f"Report missing '{key}', some features may be limited")
        
        print(f"✅ JSON report loaded: {self.report_path.name}")
        return report_data
    
    def _load_progressive_csv(self) -> Optional[pd.DataFrame]:
        """
        Load progressive signals CSV as primary data source
        
        Returns:
            DataFrame with all progressive signals, or None if not available
        """
        progressive_path = self._find_progressive_csv()
        
        if not progressive_path:
            print("📊 Progressive CSV not found")
            return None
        
        try:
            print(f"📊 Loading progressive signals from: {progressive_path.name}")
            progressive_df = pd.read_csv(progressive_path, low_memory=False)
            
            if progressive_df.empty:
                print("⚠️  Progressive CSV file is empty")
                return None
            
            # Convert datetime columns
            datetime_columns = ['timestamp', 'entry_time', 'exit_time']
            for col in datetime_columns:
                if col in progressive_df.columns:
                    try:
                        progressive_df[col] = pd.to_datetime(progressive_df[col])
                    except Exception as e:
                        print(f"⚠️  Could not parse {col}: {e}")
            
            # Add derived columns for analysis
            progressive_df = self._add_derived_columns(progressive_df)
            
            print(f"   Loaded {len(progressive_df):,} progressive signals")
            print(f"   Signal stages: {self._get_stage_summary(progressive_df)}")
            
            return progressive_df
            
        except Exception as e:
            print(f"❌ Error loading progressive CSV: {e}")
            return None
    
    def _find_progressive_csv(self) -> Optional[Path]:
        """Find progressive CSV file"""
        # Method 1: Check report for progressive reference
        if self.report_data and 'progressive_tracking' in self.report_data:
            progressive_ref = self.report_data['progressive_tracking'].get('progressive_csv_file')
            if progressive_ref:
                progressive_path = self.project_root / progressive_ref
                if progressive_path.exists():
                    return progressive_path
        
        # Method 2: Infer from report filename
        report_name = self.report_path.stem
        if 'strategy_report_' in report_name:
            timestamp_part = report_name.replace('strategy_report_', '')
            
            # Try different naming patterns
            possible_names = [
                f"signals_progressive_{timestamp_part}.csv",
                f"signals_progressive_{timestamp_part[:8]}_{timestamp_part[9:]}.csv",
            ]
            
            progressive_dir = self.project_root / "outputs" / "signals" / "progressive"
            if progressive_dir.exists():
                for name in possible_names:
                    progressive_path = progressive_dir / name
                    if progressive_path.exists():
                        return progressive_path
        
        # Method 3: Look for most recent progressive CSV
        progressive_dir = self.project_root / "outputs" / "signals" / "progressive"
        if progressive_dir.exists():
            progressive_files = list(progressive_dir.glob("signals_progressive_*.csv"))
            if progressive_files:
                # Get most recent file
                progressive_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                return progressive_files[0]
        
        return None
    
    def _add_derived_columns(self, progressive_df: pd.DataFrame) -> pd.DataFrame:
        """Add useful derived columns for analysis"""
        df = progressive_df.copy()
        
        # Add is_executed flag
        if 'stage_trade' in df.columns:
            df['is_executed'] = df['stage_trade'] == 'EXECUTED'
        elif 'trade_id' in df.columns:
            df['is_executed'] = df['trade_id'].notna()
        
        # Add is_closed flag
        if 'exit_reason' in df.columns:
            df['is_closed'] = df['exit_reason'].notna()
        
        # Add win/loss flags if not present
        if 'pnl_points' in df.columns and 'is_win' not in df.columns:
            df['is_win'] = df['pnl_points'] > 0
            df['is_loss'] = df['pnl_points'] < 0
        
        # Add duration in hours if minutes available
        if 'duration_minutes' in df.columns:
            df['duration_hours'] = df['duration_minutes'] / 60
        
        return df
    
    def _extract_trades_from_progressive(self) -> Optional[pd.DataFrame]:
        """
        Extract trade data from progressive DataFrame for backward compatibility
        
        Returns:
            DataFrame with executed trades only
        """
        if self.progressive_df is None or self.progressive_df.empty:
            return None
        
        # Identify executed trades
        executed_mask = (
            (self.progressive_df['is_executed'] == True) 
            if 'is_executed' in self.progressive_df.columns
            else self.progressive_df['trade_id'].notna()
        )
        
        executed_trades = self.progressive_df[executed_mask].copy()
        
        if executed_trades.empty:
            print("⚠️  No executed trades found in progressive data")
            return None
        
        print(f"📈 Extracted {len(executed_trades):,} executed trades from progressive data")
        
        # Ensure required columns exist for compatibility
        if 'status' not in executed_trades.columns:
            executed_trades['status'] = 'CLOSED'
            if 'exit_reason' not in executed_trades.columns:
                executed_trades['status'] = 'OPEN'
        
        return executed_trades
    
    def _load_legacy_trades_csv(self) -> Optional[pd.DataFrame]:
        """Fallback: Load legacy trade details CSV"""
        csv_path = self._find_legacy_csv()
        
        if not csv_path:
            print("❌ No trade data available")
            return None
        
        try:
            print(f"📁 Loading legacy trades from: {csv_path.name}")
            trades_df = pd.read_csv(csv_path)
            
            if trades_df.empty:
                print("⚠️  Legacy CSV file is empty")
                return None
            
            closed_trades = len(trades_df[trades_df['status'] == 'CLOSED']) if 'status' in trades_df.columns else 0
            print(f"   Loaded {len(trades_df):,} records, {closed_trades:,} closed trades")
            
            # Convert datetime columns
            datetime_columns = ['entry_time', 'exit_time', 'timestamp']
            for col in datetime_columns:
                if col in trades_df.columns:
                    trades_df[col] = pd.to_datetime(trades_df[col])
            
            return trades_df
            
        except Exception as e:
            print(f"❌ Error loading legacy CSV: {e}")
            return None
    
    def _find_legacy_csv(self) -> Optional[Path]:
        """Find legacy trade details CSV"""
        if self.report_data is None:
            return None
        
        outputs = self.report_data.get('outputs', {})
        
        # Get CSV path from report
        csv_relative_path = None
        for key in ['trades_csv_file', 'signals_csv_file']:
            if key in outputs:
                csv_relative_path = outputs[key]
                break
        
        if not csv_relative_path:
            return None
        
        csv_filename = Path(csv_relative_path).name
        csv_path = self.project_root / "outputs" / "signals" / "strategy" / csv_filename
        
        return csv_path if csv_path.exists() else None
    
    def _get_stage_summary(self, progressive_df: pd.DataFrame) -> str:
        """Get summary of signal progression stages"""
        if progressive_df.empty:
            return "No signals"
        
        summaries = []
        
        # Count by final status
        if 'final_status' in progressive_df.columns:
            status_counts = progressive_df['final_status'].value_counts()
            key_statuses = ['TRADE_CLOSED', 'TRADE_OPEN', 'REJECTED_']
            for status, count in status_counts.items():
                if any(key in str(status) for key in key_statuses) and count > 0:
                    summaries.append(f"{status}: {count}")
        
        return "; ".join(summaries[:3]) + ("..." if len(summaries) > 3 else "")
    
    def get_data_summary(self) -> Dict:
        """Get comprehensive summary of loaded data"""
        summary = {
            'report_file': str(self.report_path.name),
            'report_loaded': self.report_data is not None,
            'progressive_data_loaded': self.progressive_df is not None and not self.progressive_df.empty,
            'trades_data_loaded': self.trades_df is not None and not self.trades_df.empty,
            'data_source': 'progressive' if self.progressive_df is not None else 'legacy',
        }
        
        if self.progressive_df is not None:
            summary.update({
                'total_signals': len(self.progressive_df),
                'executed_trades': len(self.progressive_df[self.progressive_df['is_executed'] == True]) 
                                 if 'is_executed' in self.progressive_df.columns else 0,
                'closed_trades': len(self.progressive_df[self.progressive_df['is_closed'] == True]) 
                                if 'is_closed' in self.progressive_df.columns else 0,
            })
        
        if self.trades_df is not None:
            summary.update({
                'total_trade_records': len(self.trades_df),
                'closed_trades_legacy': len(self.trades_df[self.trades_df['status'] == 'CLOSED']) 
                                      if 'status' in self.trades_df.columns else 0,
                'open_trades_legacy': len(self.trades_df[self.trades_df['status'] == 'OPEN']) 
                                     if 'status' in self.trades_df.columns else 0,
            })
        
        return summary
    
    def get_progressive_data(self) -> Optional[pd.DataFrame]:
        """Get the progressive DataFrame (primary data source)"""
        return self.progressive_df