"""
Dashboard Data Loader Module
Loads reports, trades, and configuration for the dashboard
"""
import json
import pandas as pd
from pathlib import Path
from typing import Dict, Optional, Tuple

class DashboardDataLoader:
    def __init__(self, report_path: str):
        self.report_path = Path(report_path).resolve()
        
        # Calculate project root correctly - based on your finding
        # report_path: project_root/outputs/reports/WBWS/file.json
        # So: parent.parent.parent.parent gives project_root
        self.project_root = self.report_path.parent.parent.parent.parent
        
        self.report_data = None
        self.trades_df = None
        self.config = None
        self._csv_path_cache = None  # Cache for CSV path
        self._csv_file_found = False
        
    def load_all_data(self) -> Tuple[Dict, Optional[pd.DataFrame], Optional[Dict]]:
        """
        Load all data needed for dashboard
        
        Returns:
            Tuple of (report_data, trades_df, config)
        """
        # 1. Load JSON report
        self.report_data = self._load_json_report()
        
        # 2. Load CSV trades
        self.trades_df = self._load_trades_csv()
        
        # 3. Extract config from report
        self.config = self.report_data.get('config', {})
        
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
                raise ValueError(f"Invalid report: missing '{key}'")
        
        return report_data
    
    def _load_trades_csv(self) -> Optional[pd.DataFrame]:
        """Load trades CSV if available"""
        csv_path = self._find_csv_file()
        
        if not csv_path:
            return None
        
        try:
            print(f"📁 Loading trades from: {csv_path.name}")
            trades_df = pd.read_csv(csv_path)
            
            # Basic validation
            if trades_df.empty:
                print("⚠️  CSV file is empty")
                return None
            
            closed_trades = len(trades_df[trades_df['status'] == 'CLOSED']) if 'status' in trades_df.columns else 0
            print(f"   Loaded {len(trades_df)} records, {closed_trades} closed trades")
            
            # Convert datetime columns
            datetime_columns = ['entry_time', 'exit_time', 'timestamp']
            for col in datetime_columns:
                if col in trades_df.columns:
                    trades_df[col] = pd.to_datetime(trades_df[col])
            
            # Add is_win column if not present
            if 'pnl_points' in trades_df.columns and 'is_win' not in trades_df.columns:
                trades_df['is_win'] = trades_df['pnl_points'] > 0
            
            return trades_df
            
        except Exception as e:
            print(f"❌ Error loading CSV file: {e}")
            return None
    
    def _find_csv_file(self) -> Optional[Path]:
        """Find CSV file from report data - cached version"""
        # Return cached result if available
        if self._csv_path_cache is not None:
            return self._csv_path_cache
        
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
            self._csv_path_cache = None
            self._csv_file_found = False
            return None
        
        # Extract just the filename
        csv_filename = Path(csv_relative_path).name
        
        # Construct the path
        # CSV should be at: project_root/outputs/signals/strategy/filename.csv
        csv_path = self.project_root / "outputs" / "signals" / "strategy" / csv_filename
        
        if csv_path.exists():
            print(f"✅ Found exact CSV file: {csv_filename}")
            self._csv_path_cache = csv_path
            self._csv_file_found = True
            return csv_path
        
        print(f"❌ CSV file not found: {csv_filename}")
        print(f"   Expected location: {csv_path}")
        self._csv_path_cache = None
        self._csv_file_found = False
        return None
    
    def get_data_summary(self) -> Dict:
        """Get summary of loaded data"""
        summary = {
            'report_file': str(self.report_path.name),
            'report_loaded': self.report_data is not None,
            'trades_loaded': self.trades_df is not None and not self.trades_df.empty,
            'config_loaded': self.config is not None,
            'csv_file_found': self._csv_file_found,
        }
        
        if self.trades_df is not None:
            summary.update({
                'total_records': len(self.trades_df),
                'closed_trades': len(self.trades_df[self.trades_df['status'] == 'CLOSED']) if 'status' in self.trades_df.columns else 0,
                'open_trades': len(self.trades_df[self.trades_df['status'] == 'OPEN']) if 'status' in self.trades_df.columns else 0,
                'rejected_trades': len(self.trades_df[self.trades_df['status'] == 'REJECTED']) if 'status' in self.trades_df.columns else 0,
            })
        else:
            summary.update({
                'total_records': 0,
                'closed_trades': 0,
                'open_trades': 0,
                'rejected_trades': 0,
            })
        
        return summary