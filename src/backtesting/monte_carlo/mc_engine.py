import random
from monte_carlo.equity_simulator import EquitySimulator
from monte_carlo.perturbation import PerturbationEngine
from monte_carlo.mc_metrics import mc_summary

class MonteCarloEngine:
    def __init__(self, config):
        self.simulator = EquitySimulator(config["initial_balance"])
        self.perturb = PerturbationEngine(
            spread_noise=config["spread_noise"],
            risk_noise=config["risk_noise"]
        )
        self.iterations = config["iterations"]

    def run(self, trade_returns):
        curves = []

        for _ in range(self.iterations):
            shuffled = trade_returns.copy()
            random.shuffle(shuffled)

            perturbed = self.perturb.apply(shuffled)
            curve = self.simulator.simulate(perturbed)

            curves.append(curve)

        return mc_summary(curves)