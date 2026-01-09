import yaml
import json
import subprocess
import shutil
from pathlib import Path
from datetime import datetime

from optimization.parameter_space import ParameterSpace
from optimization.sampler import ParameterSampler
from evaluation.metrics import OptimizationMetrics
from evaluation.fitness import FitnessEvaluator
from evaluation.candidate_store import CandidateStore
from evaluation.ranker import CandidateRanker


class BacktestOrchestrator:
    def __init__(self, backtest_yaml_path: str):
        self.backtest_yaml_path = Path(backtest_yaml_path)
        self.base_dir = Path("outputs/backtests")
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        with open(self.backtest_yaml_path, "r") as f:
            self.config = yaml.safe_load(f)

    # -------------------------------------------------
    # Main entry
    # -------------------------------------------------
    def run(self):
        print("🚀 Starting Backtest Orchestrator")

        zones = self.config.get("zones", {})
        for zone_name, zone_cfg in zones.items():
            print(f"\n🔹 Running zone: {zone_name.upper()}")
            self.run_zone(zone_name, zone_cfg)

        print("\n✅ Orchestration completed.")

    # -------------------------------------------------
    # Zone execution
    # -------------------------------------------------
    def run_zone(self, zone_name: str, zone_cfg: dict):
        zone_dir = self.base_dir / zone_name / self.timestamp
        zone_dir.mkdir(parents=True, exist_ok=True)

        space = ParameterSpace(zone_cfg).build()
        sampler = ParameterSampler(space, n_samples=zone_cfg["samples"])
        samples = sampler.random_sample()

        store = CandidateStore(zone_dir)

        for i, params in enumerate(samples):
            print(f"▶ Candidate {i+1}/{len(samples)}")

            temp_yaml = self.create_temp_yaml(params, zone_name, i)
            report_path = self.run_strategy(temp_yaml, zone_dir)

            metrics = OptimizationMetrics(report_path).get()

            fitness_engine = FitnessEvaluator(
                constraints=self.config["constraints"],
                weights=self.config["fitness"]["weights"]
            )

            if not fitness_engine.passes_constraints(metrics):
                continue

            score = fitness_engine.score(metrics)
            store.add(params, metrics, score)

        store.save()

        ranker = CandidateRanker(store.candidates)
        top = ranker.top_n(n=10)

        with open(zone_dir / "top_candidates.json", "w") as f:
            json.dump(top, f, indent=2)

        print(f"🏆 Top 10 candidates saved for zone {zone_name}")

    # -------------------------------------------------
    # Run strategy
    # -------------------------------------------------
    def run_strategy(self, strategy_yaml_path: Path, output_dir: Path):
        print(f"▶ Running strategy with config: {strategy_yaml_path.name}")

        cmd = [
            "python",
            "scripts/run_wbws_strategy.py",
            str(strategy_yaml_path)
        ]

        subprocess.run(cmd, check=True)

        reports_dir = Path("outputs/reports/WBWS")
        latest_report = max(
            reports_dir.glob("strategy_report_*.json"),
            key=lambda f: f.stat().st_mtime
        )

        dest = output_dir / latest_report.name
        shutil.copy(latest_report, dest)

        print(f"✔ Report saved to {dest}")
        return dest

    # -------------------------------------------------
    # Create temp YAML
    # -------------------------------------------------
    def create_temp_yaml(self, params: dict, zone_name: str, idx: int):
        temp_dir = Path("temp_configs") / zone_name
        temp_dir.mkdir(parents=True, exist_ok=True)

        with open(self.backtest_yaml_path, "r") as f:
            base_yaml = yaml.safe_load(f)

        base_yaml["strategy"]["parameters"].update(params)

        temp_path = temp_dir / f"candidate_{idx}.yaml"
        with open(temp_path, "w") as f:
            yaml.dump(base_yaml, f)

        return temp_path


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python orchestrator.py <backtest_yaml>")
        sys.exit(1)

    orchestrator = BacktestOrchestrator(sys.argv[1])
    orchestrator.run()