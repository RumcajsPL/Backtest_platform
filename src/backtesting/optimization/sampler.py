import random
from itertools import product

class ParameterSampler:
    def __init__(self, param_space: dict, n_samples: int = 100):
        self.param_space = param_space
        self.n_samples = n_samples

    def random_sample(self):
        keys = list(self.param_space.keys())
        samples = []

        for _ in range(self.n_samples):
            sample = {
                k: random.choice(self.param_space[k])
                for k in keys
            }
            samples.append(sample)

        return samples