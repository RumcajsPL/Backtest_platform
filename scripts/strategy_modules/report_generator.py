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
                         performance_metrics: Dict, csv_path: Optional[Path],
                         mode: str = 'debug') -> Dict:
        """
        Build report data structure
        
        Args:
            mode: 'core' for minimal output (Part 1 only), 'debug' for full output (default)
        """
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Prepare CSV path for JSON
        csv_relative = None
        if csv_path and csv_path.exists():
            try:
                csv_relative = str(csv_path.relative_to(self.project_root))
            except ValueError:
                csv_relative = str(csv_path)
        
        # Extract the new metrics for easy access
        # In core mode, these are directly in performance_metrics
        # In debug mode, they're also in the detailed analysis sections
        max_drawdown = performance_metrics.get('max_drawdown_points', 
                        performance_metrics.get('drawdown_analysis', {}).get('max_drawdown_points', 0))
        losing_streak = performance_metrics.get('max_losing_streak',
                        performance_metrics.get('losing_streak_analysis', {}).get('max_losing_streak', 0))
        
        # PART 1: REQUIRED FOR PIPELINE
        simulation_results_section = {
            "performance_metrics": {
                "total_trades": performance_metrics.get('total_trades', 0),
                "winning_trades": performance_metrics.get('winning_trades', 0),
                "win_rate": round(performance_metrics.get('win_rate', 0), 2),
                "total_pnl_points": round(performance_metrics.get('total_pnl_points', 0), 2),
                "expectancy_points": round(performance_metrics.get('expectancy_points', 0), 2),
                "profit_factor": round(performance_metrics.get('profit_factor', 0), 2),
                "avg_pnl_points": round(performance_metrics.get('avg_pnl_points', 0), 2),
                "largest_win": round(performance_metrics.get('largest_win', 0), 2),
                "largest_loss": round(performance_metrics.get('largest_loss', 0), 2),
                "total_pnl_percent": round(performance_metrics.get('total_pnl_percent', 0), 2),
                "avg_win_points": round(performance_metrics.get('avg_win_points', 0), 2),
                "avg_loss_points": round(performance_metrics.get('avg_loss_points', 0), 2),
                "max_drawdown": round(max_drawdown, 2),
                "losing_streak": losing_streak
            },
            "trade_summary": {
                "trades_per_day": round(len(simulation_results['closed_trades']) / max((pd.to_datetime(data_info['date_range'][1]) - pd.to_datetime(data_info['date_range'][0])).days + 1, 1), 2) if data_info['date_range'] else 0
            }
        }
        
        validation_section = {
            "data_loader_cache_stats": {
                # This will be populated by the main script
                # Placeholder structure
                "hits": 0,
                "misses": 0,
                "hit_rate": "0.0%",
                "cache_files": 0,
                "cache_size_mb": 0.0,
                "cache_dir": ""
            }
        }
        
        # CORE MODE: Return minimal report with only Part 1 (required for pipeline)
        if mode == 'core':
            return {
                "//_COMMENT": "=== CORE MODE: PIPELINE ESSENTIALS ONLY ===",
                
                "simulation_results": simulation_results_section,
                
                "validation": validation_section,
                
                "execution_time": datetime.now().isoformat(),
                
                "mode": "core"
            }
        
        # === DEBUG MODE: Build full report structure ===
        
        # PART 2: OTHER DATA
        config_section = {
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
        }
        
        signal_flow_section = {
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
        }
        
        simulation_results_unused_section = {
            "open_trades": len(simulation_results['open_trades']),
            "rejected_signals": len(simulation_results['rejected_trades']),
            "performance_metrics_details": {
                "exit_reasons": performance_metrics.get('exit_reasons', {}),
                "long_short_breakdown": performance_metrics.get('long_short_breakdown', {}),
                "monthly_performance": performance_metrics.get('monthly_performance', {}),
                "spread_analysis": performance_metrics.get('spread_analysis', {}),
                "drawdown_analysis": performance_metrics.get('drawdown_analysis', {})
            },
            "position_management": {
                "max_concurrent_positions": len(self._get_current_positions(simulation_results['all_trades'])),
                "pyramiding_used": any(len([t for t in simulation_results['all_trades'] if t['direction'] == d and t['status'] == 'OPEN']) > 1 
                                      for d in ['BUY', 'SELL']),
                "close_and_reverse_count": len([t for t in simulation_results['all_trades'] if 'Reversal' in str(t.get('comment', ''))])
            }
        }
        
        overall_rejection_section = {
            "total_rejected": filter_stats['raw']['total'] - len(simulation_results['closed_trades']),
            "total_rejection_rate_pct": ((filter_stats['raw']['total'] - len(simulation_results['closed_trades'])) / filter_stats['raw']['total'] * 100) if filter_stats['raw']['total'] > 0 else 0
        }
        
        risk_details_section = {
            "atr_length": config['trade_management']['sl_tp'].get('atr_length', 14),
            "sl_multiplier": config['trade_management']['sl_tp'].get('sl_multiplier', 1.4),
            "risk_to_reward": config['trade_management']['sl_tp'].get('risk_to_reward_ratio', 2.0),
            "max_risk_percentile": config['trade_management']['risk_management'].get('max_risk_percentile', 1.0),
            "allow_exceed_limit": config['trade_management']['risk_management'].get('allow_exceed_limit', False)
        }
        
        outputs_section = {
            "signals_csv_file": csv_relative,
            "trades_csv_file": csv_relative,
        }
        
        progressive_tracking_section = {
            "signal_progression_summary": filter_stats.get('progressive', {}),
            "total_signals_tracked": filter_stats.get('progressive', {}).get('total_signals', 0)
        }
        
        # BUILD THE REORGANIZED REPORT
        report_data = {
            "//_COMMENT_PART1": "=== PART 1: REQUIRED FOR PIPELINE ===",
            
            "simulation_results": simulation_results_section,
            
            "validation": validation_section,
            
            "//_COMMENT_PART2": "=== PART 2: OTHER DATA ===",
            
            "execution_time": datetime.now().isoformat(),
            
            "config": config_section,
            
            "signal_flow": signal_flow_section,
            
            "simulation_results_unused_subsections": simulation_results_unused_section,
            
            "overall_rejection": overall_rejection_section,
            
            "risk_details": risk_details_section,
            
            "outputs": outputs_section,
            
            "progressive_tracking": progressive_tracking_section
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