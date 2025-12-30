"""
Report Generation Module
Generates JSON and CSV reports
"""
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

class ReportGenerator:
    def __init__(self, config: Dict, project_root: Path):
        self.config = config
        self.project_root = project_root
        
    def generate_csv(self, trades: List[Dict], timestamp_str: str) -> Optional[Path]:
        """Generate CSV report with all trades"""
        save_csv = self.config.get('output', {}).get('save_signals_csv', True)
        if not save_csv or not trades:
            return None
        
        out_cfg = self.config.get('output', {})
        signals_dir = self.project_root / out_cfg.get('outputs_dir', 'outputs') / out_cfg.get('signals_dir', 'signals/strategy')
        signals_dir.mkdir(parents=True, exist_ok=True)
        
        csv_filename = f"trade_details_{timestamp_str}.csv"
        csv_path = signals_dir / csv_filename
        
        trades_df = pd.DataFrame(trades)
        
        # Convert datetime objects to strings for CSV
        for col in ['entry_time', 'exit_time']:
            if col in trades_df.columns:
                trades_df[col] = trades_df[col].apply(
                    lambda x: x.isoformat() if pd.notnull(x) and not isinstance(x, str) else x
                )
        
        trades_df.to_csv(csv_path, index=False)
        return csv_path
    
    def generate_json(self, report_data: Dict, timestamp_str: str) -> Path:
        """Generate JSON report"""
        out_cfg = self.config.get('output', {})
        report_dir = self.project_root / out_cfg.get('outputs_dir', 'outputs') / out_cfg.get('reports_dir', 'reports/WBWS')
        report_dir.mkdir(parents=True, exist_ok=True)
        
        report_path = report_dir / f"strategy_report_{timestamp_str}.json"
        with open(report_path, 'w') as f:
            json.dump(report_data, f, indent=4)
            
        return report_path
    
    def build_report_data(self, config: Dict, data_info: Dict, 
                         filter_stats: Dict, simulation_results: Dict,
                         performance_metrics: Dict, csv_path: Optional[Path]) -> Dict:
        """Build comprehensive report data structure"""
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Prepare CSV path for JSON
        csv_relative = None
        if csv_path and csv_path.exists():
            try:
                csv_relative = str(csv_path.relative_to(self.project_root))
            except ValueError:
                csv_relative = str(csv_path)
        
        report_data = {
            "execution_time": datetime.now().isoformat(),
            "config": {
                "data_period": {
                    "start": data_info.get('date_range', [None, None])[0],
                    "end": data_info.get('date_range', [None, None])[1]
                },
                "indicator": config['indicator']['name'],
                "htf_period": config['indicator']['htf_period'],
                "time_filter": {
                    "enabled": config['trade_management']['time_filter']['enabled'],
                    "session": f"{config['trade_management']['time_filter']['session_start']['hour']:02d}:"
                              f"{config['trade_management']['time_filter']['session_start']['minute']:02d}-"
                              f"{config['trade_management']['time_filter']['session_end']['hour']:02d}:"
                              f"{config['trade_management']['time_filter']['session_end']['minute']:02d}"
                } if config['trade_management']['time_filter']['enabled'] else {"enabled": False},
                "rsi_filter": config['filters']['rsi_filter'],
                "position_control": config['trade_management'].get('position_control', {})
            },
            "signal_flow": {
                "step1_raw_signals": filter_stats['raw'],
                "step2_time_filtered": filter_stats['time_filtered'],
                "step3_rsi_filtered": filter_stats['rsi_filtered'],
                "step4_risk_managed": {
                    "buy": filter_stats['risk_filtered']['approved']['buy'],
                    "sell": filter_stats['risk_filtered']['approved']['sell'],
                    "total": filter_stats['risk_filtered']['total_approved'],
                    "rejected_buy": filter_stats['risk_filtered']['rejected']['buy'],
                    "rejected_sell": filter_stats['risk_filtered']['rejected']['sell'],
                    "rejected_total": filter_stats['risk_filtered']['total_rejected'],
                    "adjusted_buy": filter_stats['risk_filtered']['adjusted']['buy'],
                    "adjusted_sell": filter_stats['risk_filtered']['adjusted']['sell'],
                    "adjusted_total": filter_stats['risk_filtered']['total_adjusted']
                },
                "step5_position_managed": {
                    "buy_opens": len([t for t in simulation_results['closed_trades'] if t['direction'] == 'BUY']),
                    "sell_opens": len([t for t in simulation_results['closed_trades'] if t['direction'] == 'SELL']),
                    "total_opens": len(simulation_results['closed_trades']),
                    "rejected_buy": simulation_results['position_rejected_count']['buy'],
                    "rejected_sell": simulation_results['position_rejected_count']['sell'],
                    "rejected_total": simulation_results['position_rejected_count']['buy'] + simulation_results['position_rejected_count']['sell'],
                    "exit_statistics": simulation_results['exit_stats'],
                    "trade_manager_metrics": simulation_results['trade_manager_metrics']
                }
            },
            "simulation_results": {
                "total_trades_simulated": len(simulation_results['all_trades']),
                "closed_trades": len(simulation_results['closed_trades']),
                "open_trades": len(simulation_results['open_trades']),
                "rejected_signals": len(simulation_results['rejected_trades']),
                "performance_metrics": performance_metrics,
                "trade_summary": {
                    "first_trade": simulation_results['all_trades'][0]['entry_time'].isoformat() if simulation_results['all_trades'] else None,
                    "last_trade": simulation_results['all_trades'][-1]['entry_time'].isoformat() if simulation_results['all_trades'] else None,
                    "total_duration_days": (pd.to_datetime(data_info['date_range'][1]) - pd.to_datetime(data_info['date_range'][0])).days if data_info['date_range'] else 0,
                    "trades_per_day": len(simulation_results['closed_trades']) / max((pd.to_datetime(data_info['date_range'][1]) - pd.to_datetime(data_info['date_range'][0])).days + 1, 1) if data_info['date_range'] else 0
                },
                "position_management": {
                    "max_concurrent_positions": len(self._get_current_positions(simulation_results['all_trades'])),
                    "pyramiding_used": any(len([t for t in simulation_results['all_trades'] if t['direction'] == d and t['status'] == 'OPEN']) > 1 
                                          for d in ['BUY', 'SELL']),
                    "close_and_reverse_count": len([t for t in simulation_results['all_trades'] if 'Reversal' in str(t.get('comment', ''))])
                }
            },
            "overall_rejection": {
                "total_rejected": filter_stats['raw']['total'] - len(simulation_results['closed_trades']),
                "total_rejection_rate_pct": ((filter_stats['raw']['total'] - len(simulation_results['closed_trades'])) / filter_stats['raw']['total'] * 100) if filter_stats['raw']['total'] > 0 else 0
            },
            "risk_details": {
                "atr_length": config['trade_management']['sl_tp'].get('atr_length', 14),
                "sl_multiplier": config['trade_management']['sl_tp'].get('sl_multiplier', 1.4),
                "risk_to_reward": config['trade_management']['sl_tp'].get('risk_to_reward_ratio', 2.0),
                "max_risk_percentile": config['trade_management']['risk_management'].get('max_risk_percentile', 1.0),
                "allow_exceed_limit": config['trade_management']['risk_management'].get('allow_exceed_limit', False)
            },
            "outputs": {
                "signals_csv_file": csv_relative,
                "trades_csv_file": csv_relative,
            }
        }
        
        return report_data
    
    def _get_current_positions(self, trades: List[Dict]) -> List[Dict]:
        """Get current positions from trades"""
        positions = {}
        for trade in trades:
            if trade['status'] == 'OPEN':
                pos_id = trade['position_id']
                if pos_id not in positions:
                    positions[pos_id] = {
                        'position_id': pos_id,
                        'direction': trade['direction'],
                        'trades': []
                    }
                positions[pos_id]['trades'].append(trade)
        return list(positions.values())