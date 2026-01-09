import json

class OptimizationMetrics:
    def __init__(self, report_path: str):
        with open(report_path, "r") as f:
            self.data = json.load(f)

    def get(self):
        return {
            "total_trades": self.data["summary"]["total_trades"],
            "wins": self.data["summary"]["winning_trades"],
            "winrate": self.data["summary"]["winning_trades"] / max(1, self.data["summary"]["total_trades"]),
            "net_pnl": self.data["performance"]["net_pnl"],
            "expectancy": self.data["performance"]["expectancy_r"],
            "max_drawdown": abs(self.data["drawdown"]["max_drawdown_pct"]),
            "losing_streak": self.data["risk"]["max_consecutive_losses"],
            "profit_factor": self.data["performance"]["profit_factor"],
            "trades_per_day": self.data["summary"]["trades_per_day"],
        }