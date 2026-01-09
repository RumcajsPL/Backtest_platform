import numpy as np

class WFOEvaluator:
    def __init__(self, fitness_engine):
        self.fitness = fitness_engine

    def evaluate(self, window_results):
        scores = []
        winrates = []
        drawdowns = []

        for r in window_results:
            metrics = r["metrics"]
            scores.append(self.fitness.score(metrics))
            winrates.append(metrics["winrate"])
            drawdowns.append(metrics["max_drawdown"])

        return {
            "avg_fitness": float(np.mean(scores)),
            "fitness_std": float(np.std(scores)),
            "avg_winrate": float(np.mean(winrates)),
            "avg_drawdown": float(np.mean(drawdowns)),
            "stability": float(1 / (1 + np.std(scores)))
        }