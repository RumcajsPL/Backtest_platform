import random

class ParameterSampler:
    def __init__(self, param_space: dict, n_samples: int = 100):
        self.param_space = param_space
        self.n_samples = n_samples

    def _generate_single_sample(self):
        """Generate a single random parameter set (private helper)"""
        keys = list(self.param_space.keys())
        return {
            k: random.choice(self.param_space[k])
            for k in keys
        }

    def random_sample(self, n_samples=None):
        """Return a list of parameter sets"""
        if n_samples is None:
            n_samples = self.n_samples
        
        samples = []
        for _ in range(n_samples):
            samples.append(self._generate_single_sample())
        return samples

    def sample(self):
        """Return a single random parameter set"""
        return self._generate_single_sample()