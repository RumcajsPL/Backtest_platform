import random

class PerturbationEngine:
    def __init__(self, spread_noise=0.2, risk_noise=0.1):
        self.spread_noise = spread_noise
        self.risk_noise = risk_noise

    def apply(self, returns):
        perturbed = []

        for r in returns:
            noise = random.uniform(-self.spread_noise, self.spread_noise)
            perturbed.append(r * (1 + noise))

        return perturbed