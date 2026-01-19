import random

class Population:
    def __init__(self, sampler, size):
        self.sampler = sampler
        self.size = size

    def generate(self):
        """Generate a population of parameter sets"""
        population = []
        
        # Try different ways to generate samples
        if hasattr(self.sampler, 'generate_population'):
            return self.sampler.generate_population(self.size)
        elif hasattr(self.sampler, 'random_sample'):
            # Use random_sample with n_samples
            samples = self.sampler.random_sample(n_samples=self.size)
            return samples[:self.size] if samples else []
        else:
            # Fallback: generate individuals one by one
            for _ in range(self.size):
                individual = self.get_random_parameters()
                if individual:  # Only add if we got valid parameters
                    population.append(individual)
            
            return population
    
    def get_random_parameters(self):
        """Helper method to get a single random parameter set"""
        if hasattr(self.sampler, 'sample'):
            return self.sampler.sample()
        elif hasattr(self.sampler, 'random_sample'):
            samples = self.sampler.random_sample(n_samples=1)
            return samples[0] if samples else {}
        else:
            # Ultimate fallback - create empty dict
            print("⚠️  Warning: Could not generate random parameters from sampler")
            return {}