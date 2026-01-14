import json

class OptimizationMetrics:
    def __init__(self, report_path: str):
        with open(report_path, "r") as f:
            self.data = json.load(f)

    def get(self):
        """Extract metrics from the actual report structure"""
        perf = self.data.get("simulation_results", {}).get("performance_metrics", {})
        summary = self.data.get("simulation_results", {}).get("trade_summary", {})
        
        # Calculate metrics based on your report structure
        total_trades = perf.get("total_trades", 0)
        winning_trades = perf.get("winning_trades", 0)
        
        return {
            "total_trades": total_trades,
            "wins": winning_trades,
            "winrate": perf.get("win_rate", 0) / 100 if perf.get("win_rate") else 0,  # Convert % to decimal
            "net_pnl": perf.get("total_pnl_points", 0),
            "expectancy": perf.get("expectancy_points", 0),
            "max_drawdown": 0,  # Your report doesn't have drawdown in this example
            "losing_streak": 0,  # Not in report - we'll need to calculate or add
            "profit_factor": perf.get("profit_factor", 0),
            "trades_per_day": summary.get("trades_per_day", 0),
            
            # Additional metrics that might be useful
            "avg_pnl": perf.get("avg_pnl_points", 0),
            "largest_win": perf.get("largest_win", 0),
            "largest_loss": perf.get("largest_loss", 0),
            "total_pnl_percent": perf.get("total_pnl_percent", 0),
            "avg_win_points": perf.get("avg_win_points", 0),
            "avg_loss_points": perf.get("avg_loss_points", 0)
        }
    
    def debug_info(self):
        """Print debug info about available metrics"""
        print("\n📊 METRICS DEBUG INFO:")
        print(f"  Total trades: {self.data.get('simulation_results', {}).get('performance_metrics', {}).get('total_trades', 'NOT FOUND')}")
        print(f"  Winning trades: {self.data.get('simulation_results', {}).get('performance_metrics', {}).get('winning_trades', 'NOT FOUND')}")
        print(f"  Win rate: {self.data.get('simulation_results', {}).get('performance_metrics', {}).get('win_rate', 'NOT FOUND')}")
        print(f"  Total P&L: {self.data.get('simulation_results', {}).get('performance_metrics', {}).get('total_pnl_points', 'NOT FOUND')}")
        print(f"  Profit factor: {self.data.get('simulation_results', {}).get('performance_metrics', {}).get('profit_factor', 'NOT FOUND')}")
        print(f"  Trades per day: {self.data.get('simulation_results', {}).get('trade_summary', {}).get('trades_per_day', 'NOT FOUND')}")