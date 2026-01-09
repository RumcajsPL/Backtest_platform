import yaml
import subprocess
import shutil
import tempfile
import json
from pathlib import Path
from datetime import datetime
from optimization.parameter_space import ParameterSpace
from optimization.sampler import ParameterSampler
from evaluation.metrics import OptimizationMetrics
from evaluation.fitness import FitnessEvaluator
from evaluation.candidate_store import CandidateStore
from evaluation.ranker import CandidateRanker
from wfo.wfo_engine import WalkForwardEngine
from ga.ga_engine import GeneticOptimizer
from monte_carlo.mc_engine import MonteCarloEngine

class BacktestOrchestrator:
    def __init__(self, backtest_yaml_path: str):
        self.backtest_yaml_path = Path(backtest_yaml_path)
        self.base_dir = Path("outputs/backtests")
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        with open(self.backtest_yaml_path, "r") as f:
            self.config = yaml.safe_load(f)

    def run(self):
        print("🚀 Starting Backtest Orchestrator")
        print(f"📊 Mode: {self.config['backtest']['mode']}")

        zones = self.config.get("zones", {})
        for zone_name, zone_cfg in zones.items():
            print(f"\n🔹 Running zone: {zone_name.upper()}")
            self.run_zone(zone_name, zone_cfg)

        print("\n✅ Orchestration completed.")

    def run_zone(self, zone_name: str, zone_cfg: dict):
        zone_dir = self.base_dir / zone_name / self.timestamp
        zone_dir.mkdir(parents=True, exist_ok=True)

        # Build parameter space and sample parameters for initial exploration
        space = ParameterSpace(zone_cfg).build()
        sampler = ParameterSampler(space, n_samples=150)
        
        # Initialize evaluation components for this zone
        fitness_engine = FitnessEvaluator(
            constraints=self.config["constraints"],
            weights=self.config["fitness"]["weights"]
        )
        store = CandidateStore(zone_dir)

        # Get optimization configurations
        ga_enabled = self.config.get("ga", {}).get("enabled", False)
        ga_config = self.config.get("ga", {})
        mc_enabled = self.config.get("monte_carlo", {}).get("enabled", False)
        mc_config = self.config.get("monte_carlo", {})
        
        # Run initial random search
        print(f"  🎯 Running initial random search...")
        samples = sampler.random_sample()
        initial_candidates = []
        
        for i, params in enumerate(samples):
            print(f"\n    Sample {i+1}/{len(samples)} for zone '{zone_name}'")
            temp_yaml = self.create_temp_yaml(params, zone_name, i, "random")
            
            # Run strategy and get metrics
            metrics = self.run_strategy_with_metrics(params, temp_yaml, zone_dir, 
                                                    fitness_engine, mc_enabled, mc_config,
                                                    zone_name, i, "random")
            
            if metrics and "fitness_score" in metrics:
                # Add candidate to store
                store.add(
                    params=params,
                    metrics=metrics,
                    score=metrics["fitness_score"],
                    zone_name=zone_name,
                    sample_index=i,
                    source="random"
                )
                initial_candidates.append((params, metrics["fitness_score"]))
                
                # Also save individual candidate file
                self.save_candidate(params, metrics, metrics["fitness_score"], 
                                  zone_dir, i, "random")
        
        # Run Genetic Algorithm if enabled
        if ga_enabled and initial_candidates:
            print(f"\n  🧬 Starting Genetic Algorithm optimization...")
            
            # Create GA optimizer
            ga = GeneticOptimizer(
                sampler=sampler,
                orchestrator=self,
                fitness_engine=fitness_engine,
                config=ga_config
            )
            
            # Run GA optimization starting with best initial candidates
            print(f"    Population: {ga_config.get('population_size', 20)}")
            print(f"    Generations: {ga_config.get('generations', 10)}")
            
            # Sort initial candidates by score and take top for initial population
            sorted_candidates = sorted(initial_candidates, key=lambda x: x[1], reverse=True)
            initial_population = [candidate[0] for candidate in 
                                 sorted_candidates[:ga_config.get('population_size', 20)]]
            
            ga_results = ga.run(initial_population=initial_population)
            
            # Save GA results
            if ga_results:
                with open(zone_dir / "ga_results.json", "w") as f:
                    # Save only parameters and scores for brevity
                    ga_summary = []
                    for i, (params, score, metrics) in enumerate(ga_results):
                        ga_summary.append({
                            "rank": i + 1,
                            "score": score,
                            "parameters": params
                        })
                    json.dump(ga_summary[:10], f, indent=2)
                
                print(f"\n  🏆 GA optimization completed with {len(ga_results)} candidates")
                
                # Process and add GA candidates to store
                for i, (params, score, full_metrics) in enumerate(ga_results):
                    # Run Monte Carlo if enabled
                    final_metrics = full_metrics.copy() if full_metrics else {}
                    
                    if mc_enabled and "trade_returns" in final_metrics:
                        mc_metrics = self.run_monte_carlo_analysis(
                            final_metrics["trade_returns"], mc_config
                        )
                        final_metrics.update(mc_metrics)
                    
                    # Add candidate to store
                    store.add(
                        params=params,
                        metrics=final_metrics,
                        score=score,
                        zone_name=zone_name,
                        sample_index=i + len(samples),  # Continue numbering
                        source="ga"
                    )
        
        # Save all candidates to store
        store.save()
        
        # Rank and save top candidates
        if store.candidates:
            ranker = CandidateRanker(store.candidates)
            top = ranker.top_n(n=10)
            
            with open(zone_dir / "top_candidates.json", "w") as f:
                json.dump(top, f, indent=2)
            
            print(f"\n  🏆 Top 10 candidates saved for zone {zone_name}")
            
            # Print summary
            print(f"  📊 Zone '{zone_name}' completed with {len(store.candidates)} total candidates")
            
            # Separate stats by source
            sources = {}
            for candidate in store.candidates:
                source = candidate.get('source', 'random')
                if source not in sources:
                    sources[source] = []
                sources[source].append(candidate['score'])
            
            for source, scores in sources.items():
                print(f"    {source.upper()}: {len(scores)} candidates, "
                      f"Best: {max(scores):.4f}, Avg: {sum(scores)/len(scores):.4f}")
        else:
            print(f"\n  ⚠ Zone '{zone_name}' completed with no candidates")

    def run_strategy_with_metrics(self, params: dict, strategy_yaml_path: Path, output_dir: Path,
                                 fitness_engine: FitnessEvaluator, mc_enabled: bool, mc_config: dict,
                                 zone_name: str, sample_index: int, source: str = "random") -> dict:
        """Run strategy and return metrics dictionary"""
        
        # Check if WFO is enabled
        wfo_enabled = self.config.get("wfo", {}).get("enabled", False)
        
        if wfo_enabled:
            # Run Walk-Forward Optimization
            print(f"    🔄 Running WFO analysis...")
            wfo_engine = WalkForwardEngine(self, fitness_engine, self.config["wfo"])
            
            # Get date range from configuration or use defaults
            date_range = self.get_date_range()
            wfo_metrics, window_results = wfo_engine.run(params, date_range)
            
            if wfo_metrics and "avg_fitness" in wfo_metrics:
                base_metrics = wfo_metrics.copy()
                score = wfo_metrics["avg_fitness"]
                print(f"    ✅ WFO completed with average fitness: {score:.4f}")
                
                # Run Monte Carlo analysis if enabled
                if mc_enabled and "trade_returns" in base_metrics:
                    mc_metrics = self.run_monte_carlo_analysis(
                        base_metrics["trade_returns"], mc_config
                    )
                    base_metrics.update(mc_metrics)
                
                # Add metadata
                base_metrics.update({
                    "fitness_score": score,
                    "source": source,
                    "zone": zone_name,
                    "sample_index": sample_index,
                    "has_wfo": True,
                    "window_results": window_results
                })
                
                return base_metrics
            else:
                print(f"    ❌ WFO analysis failed or produced no valid metrics")
                return None
        else:
            # Run single backtest
            latest_report = self.run_strategy(strategy_yaml_path, output_dir, sample_index)
            
            if latest_report:
                # Get base metrics from report
                base_metrics = OptimizationMetrics(latest_report).get()
                
                if fitness_engine.passes_constraints(base_metrics):
                    score = fitness_engine.score(base_metrics)
                    print(f"    ✅ Candidate passed constraints with score: {score:.4f}")
                    
                    # Run Monte Carlo analysis if enabled
                    if mc_enabled and "trade_returns" in base_metrics:
                        mc_metrics = self.run_monte_carlo_analysis(
                            base_metrics["trade_returns"], mc_config
                        )
                        base_metrics.update(mc_metrics)
                    
                    # Add metadata
                    base_metrics.update({
                        "fitness_score": score,
                        "source": source,
                        "zone": zone_name,
                        "sample_index": sample_index,
                        "has_wfo": False
                    })
                    
                    return base_metrics
                else:
                    print(f"    ❌ Candidate failed constraints")
                    return None
            else:
                print(f"    ⚠ No report generated for sample {sample_index}")
                return None

    def run_monte_carlo_analysis(self, trade_returns: list, mc_config: dict) -> dict:
        """Run Monte Carlo analysis on trade returns"""
        print(f"    🎲 Running Monte Carlo analysis...")
        
        try:
            mc_engine = MonteCarloEngine(mc_config)
            mc_metrics = mc_engine.run(trade_returns)
            
            # Format the metrics for storage
            formatted_metrics = {
                "mc_avg_final": mc_metrics.get("avg_final_balance", 0),
                "mc_worst_dd": mc_metrics.get("worst_drawdown", 0),
                "mc_ruin_prob": mc_metrics.get("ruin_probability", 0),
                "mc_iterations": mc_config.get("iterations", 1000),
                "mc_initial_balance": mc_config.get("initial_balance", 10000)
            }
            
            print(f"    ✅ Monte Carlo: Avg Final=${formatted_metrics['mc_avg_final']:.2f}, "
                  f"Worst DD={formatted_metrics['mc_worst_dd']:.2%}, "
                  f"Ruin Prob={formatted_metrics['mc_ruin_prob']:.2%}")
            
            return formatted_metrics
        except Exception as e:
            print(f"    ⚠ Monte Carlo analysis failed: {e}")
            return {}

    def create_temp_yaml(self, params: dict, zone_name: str, sample_index: int, source: str = "random") -> Path:
        """Create a temporary YAML file with specific parameters"""
        # Load the base configuration
        with open(self.backtest_yaml_path, "r") as f:
            config = yaml.safe_load(f)
        
        # Update the configuration with sampled parameters
        config['optimization'] = params
        
        # Create a temporary file
        temp_dir = Path(tempfile.gettempdir())
        temp_file = temp_dir / f"wbws_{zone_name}_{source}_{sample_index}.yaml"
        
        with open(temp_file, "w") as f:
            yaml.dump(config, f)
        
        return temp_file

    def run_strategy(self, strategy_yaml_path: Path, output_dir: Path, sample_index: int) -> Path:
        """Run strategy and return path to the generated report"""
        print(f"    ▶ Running strategy with config: {strategy_yaml_path.name}")

        cmd = [
            "python",
            "scripts/run_wbws_strategy.py",
            str(strategy_yaml_path)
        ]

        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            if result.stderr:
                print(f"    ⚠ Strategy stderr: {result.stderr[:200]}...")
        except subprocess.CalledProcessError as e:
            print(f"    ❌ Strategy execution failed: {e}")
            if e.stdout:
                print(f"    Stdout: {e.stdout[:200]}...")
            if e.stderr:
                print(f"    Stderr: {e.stderr[:200]}...")
            return None

        # Find the latest report
        reports_dir = Path("outputs/reports/WBWS")
        if reports_dir.exists():
            json_files = list(reports_dir.glob("strategy_report_*.json"))
            if json_files:
                latest_report = max(json_files, key=lambda f: f.stat().st_mtime)
                # Copy report to zone directory
                report_copy = output_dir / f"report_{strategy_yaml_path.stem}.json"
                shutil.copy(latest_report, report_copy)
                print(f"    ✔ Report saved to {report_copy}")
                return report_copy
            else:
                print(f"    ⚠ No report files found in {reports_dir}")
                return None
        else:
            print(f"    ⚠ Reports directory not found: {reports_dir}")
            return None

    def save_candidate(self, params: dict, metrics: dict, score: float, output_dir: Path, 
                      sample_index: int, source: str = "random"):
        """Save a candidate that passed constraints"""
        candidate_data = {
            'parameters': params,
            'metrics': metrics,
            'fitness_score': score,
            'source': source,
            'timestamp': datetime.now().isoformat(),
            'sample_index': sample_index,
            'has_mc': 'mc_avg_final' in metrics,
            'has_wfo': metrics.get('has_wfo', False)
        }
        
        # Generate a unique filename
        candidate_id = f"{source}_candidate_{sample_index:04d}_score_{score:.4f}"
        candidate_file = output_dir / f"{candidate_id}.json"
        
        with open(candidate_file, 'w') as f:
            json.dump(candidate_data, f, indent=2)
        
        print(f"    💾 {source.upper()} candidate saved: {candidate_file}")

    def get_date_range(self):
        """Get date range for WFO analysis"""
        # Try to get date range from walk_forward windows if available
        if "walk_forward" in self.config and "windows" in self.config["walk_forward"]:
            windows = self.config["walk_forward"]["windows"]
            if windows:
                # Get the earliest train start and latest test end
                all_dates = []
                for window in windows:
                    all_dates.extend(window["train"])
                    all_dates.extend(window["test"])
                return {
                    "start": min(all_dates),
                    "end": max(all_dates)
                }
        
        # Default date range if not specified
        return {
            "start": "2024-01-01",
            "end": "2024-12-31"
        }

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python orchestrator.py <backtest_yaml>")
        sys.exit(1)

    orchestrator = BacktestOrchestrator(sys.argv[1])
    orchestrator.run()