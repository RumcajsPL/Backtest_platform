import random
from ga.selection import TournamentSelection
from ga.crossover import UniformCrossover
from ga.mutation import ParameterMutation
from ga.population import Population  

class GeneticOptimizer:
    def __init__(self, sampler, orchestrator, fitness_engine, config, zone_name, zone_dir):
        self.sampler = sampler
        self.orchestrator = orchestrator
        self.fitness = fitness_engine
        self.config = config
        self.zone_name = zone_name  # Store zone name
        self.zone_dir = zone_dir    # Store zone directory
        
        # GA configuration
        self.population_size = config.get("population_size", 4)
        self.generations = config.get("generations", 2)
        self.mutation_rate = config.get("mutation_rate", 0.15)
        self.crossover_rate = config.get("crossover_rate", 0.6)
        self.elite_fraction = config.get("elite_fraction", 0.2)
        self.tournament_k = config.get("tournament_k", 3)
        
        # Initialize components
        self.selector = TournamentSelection(self.tournament_k)
        self.crossover_op = UniformCrossover()
        self.mutation_op = ParameterMutation(sampler, self.mutation_rate)
        self.population_generator = Population(sampler, self.population_size)
    
    def get_random_parameters(self):
        """Get random parameters using the Population generator"""
        return self.population_generator.get_random_parameters()
    
    def evaluate_individual(self, params, individual_index):
        """Evaluate a single parameter set"""
        # Use the provided zone directory
        temp_yaml = self.orchestrator.create_temp_yaml(
            params, 
            self.zone_name, 
            individual_index, 
            "ga"
        )
        
        report_path = self.orchestrator.run_strategy(temp_yaml, self.zone_dir, individual_index)
        
        if report_path:
            from evaluation.metrics import OptimizationMetrics
            metrics_extractor = OptimizationMetrics(str(report_path))
            real_metrics = metrics_extractor.get()
            
            if self.fitness.passes_constraints(real_metrics):
                score = self.fitness.score(real_metrics)
                return (params, score, real_metrics)
            else:
                # Return metrics anyway, but with penalty score
                return (params, -1000, real_metrics)
        
        if not self.fitness.passes_constraints(real_metrics):
            print(f"⚠️ Candidate {individual_index} failed constraints: {real_metrics}")
        
        return (params, -1000, {})  # Fallback for no-report cases
    
    def run(self, initial_population=None):
        """Run GA optimization"""
        print(f"🧬 Starting Genetic Algorithm optimization")
        print(f"   Population: {self.population_size}")
        print(f"   Generations: {self.generations}")
        print(f"   Mutation rate: {self.mutation_rate}")
        
        # Initialize population using Population class
        if initial_population:
            # Use provided initial population
            population = initial_population[:self.population_size]
            
            # If initial population is smaller than required, generate the rest
            if len(population) < self.population_size:
                remaining_count = self.population_size - len(population)
                additional = self.population_generator.generate()[:remaining_count]
                population.extend(additional)
        else:
            # Generate full random population
            population = self.population_generator.generate()
        
        # Validate population size
        if len(population) != self.population_size:
            print(f"⚠️  Warning: Population size mismatch. Expected {self.population_size}, got {len(population)}")
            # Adjust if needed
            if len(population) > self.population_size:
                population = population[:self.population_size]
            else:
                # Fallback: generate missing individuals
                while len(population) < self.population_size:
                    population.append(self.get_random_parameters())
        
        # Evaluate initial population
        evaluated = []
        for i, params in enumerate(population):
            print(f"   Evaluating individual {i+1}/{len(population)}")
            result = self.evaluate_individual(params, i)
            evaluated.append({
                "params": params,
                "fitness": result[1],
                "metrics": result[2]
            })
        
        # Evolution loop
        for gen in range(self.generations):
            print(f"\n   Generation {gen+1}/{self.generations}")
            
            # Sort by fitness
            evaluated.sort(key=lambda x: x["fitness"], reverse=True)
            
            # Apply elitism
            elite_count = max(1, int(self.population_size * self.elite_fraction))
            new_population = [evaluated[i]["params"] for i in range(elite_count)]
            
            # Create new population
            while len(new_population) < self.population_size:
                # Selection
                parent1 = self.selector.select(evaluated)
                parent2 = self.selector.select(evaluated)
                
                # Crossover
                if random.random() < self.crossover_rate:
                    child = self.crossover_op.crossover(parent1["params"], parent2["params"])
                else:
                    child = parent1["params"].copy()
                
                # Mutation
                child = self.mutation_op.mutate(child)
                new_population.append(child)
            
            # Evaluate new population (skip elites - they keep their evaluation)
            new_evaluated = [evaluated[i] for i in range(elite_count)]  # Keep elites
            
            for i in range(elite_count, len(new_population)):
                params = new_population[i]
                result = self.evaluate_individual(params, i + gen * self.population_size)
                new_evaluated.append({
                    "params": params,
                    "fitness": result[1],
                    "metrics": result[2]
                })
            
            evaluated = new_evaluated
            
            # Show progress
            best_fitness = evaluated[0]["fitness"]
            avg_fitness = sum(x["fitness"] for x in evaluated) / len(evaluated)
            print(f"   Best: {best_fitness:.4f}, Avg: {avg_fitness:.4f}")
        
        # Return results in format expected by orchestrator
        evaluated.sort(key=lambda x: x["fitness"], reverse=True)
        results = []
        for eval_item in evaluated[:10]:  # Top 10
            results.append((
                eval_item["params"],
                eval_item["fitness"],
                eval_item["metrics"]
            ))
        
        print(f"\n✅ GA optimization completed with {len(results)} candidates")
        return results