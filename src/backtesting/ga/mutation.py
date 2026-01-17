import random

class ParameterMutation:
    def __init__(self, sampler, rate=0.1):
        self.sampler = sampler
        self.rate = rate
        self.param_space = sampler.param_space  # Store parameter space

    def mutate(self, params):
        new_params = params.copy()

        for key in params:
            if random.random() < self.rate:
                # Only mutate this specific key
                if key in self.param_space:
                    new_params[key] = random.choice(self.param_space[key])

        return new_params