#!/usr/bin/env python
"""
Strategy Unit Test Runner
=========================
Runs unit tests for all strategy modules with comprehensive reporting.

Usage:
    python tests/runners/strategy_unit_test.py
    python tests/runners/strategy_unit_test.py --config custom_config.yaml
    python tests/runners/strategy_unit_test.py --mode all
    python tests/runners/strategy_unit_test.py --test test_signal_generator
"""

import argparse
import sys
import time
import yaml
import pytest
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import logging

# Add project root to path
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.utils.paths import CONFIGS_DIR
from src.config.config_schema import StrategyConfig


class TestRunner:
    """Orchestrates unit test execution with comprehensive reporting."""

    def __init__(self, config_path: Path):
        """Initialize test runner with configuration."""
        self.config_path = config_path
        self.config = self._load_config()
        self.start_time = None
        self.results = {
            "passed": [],
            "failed": [],
            "skipped": [],
            "errors": [],
            "timings": {},
        }

    def _load_config(self) -> Dict:
        """Load test runner configuration."""
        if not self.config_path.exists():
            print(f"⚠️  Config not found: {self.config_path}, using defaults")
            return {
                "run_mode": "all",
                "enabled_tests": {},
                "execution": {"verbose": True},
                "report": {"output_dir": "tests/reports"},
            }

        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)

    def _get_test_modules(self) -> List[str]:
        """Get list of test modules to run based on configuration."""
        if self.config.get("run_mode") == "all":
            # Discover all test_*.py files in tests/unit
            unit_dir = _PROJECT_ROOT / "tests" / "unit"
            return [
                f.stem for f in unit_dir.glob("test_*.py")
                if f.stem != "__init__"
            ]

        # Return enabled tests from config
        enabled = self.config.get("enabled_tests", {})
        return [
            test_name for test_name, enabled_flag in enabled.items()
            if enabled_flag
        ]

    def _build_pytest_args(self, test_modules: List[str]) -> List[str]:
        """Build pytest arguments."""
        args = []

        # Add verbosity
        if self.config.get("execution", {}).get("verbose", True):
            args.append("-v")

        # Add parallel execution
        if self.config.get("execution", {}).get("parallel", False):
            workers = self.config.get("execution", {}).get("workers", 4)
            args.extend(["-n", str(workers)])

        # Add fail-fast
        if self.config.get("execution", {}).get("fail_fast", False):
            args.append("-x")

        # Add test modules
        for module in test_modules:
            args.append(f"tests/unit/{module}.py")

        return args

    def _generate_report(self, exit_code: int, duration: float) -> Path:
        """Generate comprehensive markdown test report."""
        report_dir = Path(self.config.get("report", {}).get("output_dir", "tests/reports"))
        report_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = report_dir / f"test_report_{timestamp}.md"

        with open(report_path, 'w') as f:
            f.write(f"# Strategy Unit Test Report\n\n")
            f.write(f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Duration:** {duration:.2f}s\n")
            f.write(f"**Exit Code:** {exit_code} ({'✅ PASSED' if exit_code == 0 else '❌ FAILED'})\n\n")

            # Configuration summary
            f.write(f"## Test Configuration\n\n")
            f.write(f"- **Run Mode:** {self.config.get('run_mode', 'unknown')}\n")
            f.write(f"- **Parallel:** {self.config.get('execution', {}).get('parallel', False)}\n")
            f.write(f"- **Fail Fast:** {self.config.get('execution', {}).get('fail_fast', False)}\n\n")

            # Results summary
            f.write(f"## Results Summary\n\n")
            f.write(f"| Result | Count |\n")
            f.write(f"|--------|-------|\n")
            f.write(f"| ✅ Passed | {len(self.results['passed'])} |\n")
            f.write(f"| ❌ Failed | {len(self.results['failed'])} |\n")
            f.write(f"| ⏭️ Skipped | {len(self.results['skipped'])} |\n")
            f.write(f"| ⚠️ Errors | {len(self.results['errors'])} |\n\n")

            # Detailed results
            if self.results['passed']:
                f.write(f"## ✅ Passed Tests\n\n")
                for test in sorted(self.results['passed']):
                    timing = self.results['timings'].get(test, 0)
                    f.write(f"- `{test}` ({timing:.3f}s)\n")
                f.write(f"\n")

            if self.results['failed']:
                f.write(f"## ❌ Failed Tests\n\n")
                for test in sorted(self.results['failed']):
                    f.write(f"- `{test}`\n")
                f.write(f"\n")

            if self.results['skipped']:
                f.write(f"## ⏭️ Skipped Tests\n\n")
                for test in sorted(self.results['skipped']):
                    f.write(f"- `{test}`\n")
                f.write(f"\n")

            if self.results['errors']:
                f.write(f"## ⚠️ Errors\n\n")
                for error in self.results['errors']:
                    f.write(f"- `{error}`\n")
                f.write(f"\n")

            f.write(f"---\n")
            f.write(f"*Report generated by Strategy Unit Test Runner*\n")

        return report_path
    
    def report_real_data_coverage(self, exit_code: int, duration: float) -> Path:
        """Enhanced report including real data test status."""
        report_path = super()._generate_report(exit_code, duration)
        
        # Add real data section
        with open(report_path, 'a') as f:
            f.write(f"\n## Real Data Test Coverage\n\n")
            
            # Count tests that used real data vs mocks
            real_data_tests = [t for t in self.results['passed'] 
                            if 'real_data' in t.lower()]
            
            f.write(f"- **Tests using real data:** {len(real_data_tests)}/{len(self.results['passed'])}\n")
            
            if real_data_tests:
                f.write("\n**Real data tests executed:**\n")
                for test in sorted(real_data_tests):
                    f.write(f"  - `{test}`\n")
        
        return report_path

    def run(self) -> int:
        """Execute tests and generate report."""
        self.start_time = time.time()

        print("=" * 70)
        print("STRATEGY UNIT TEST RUNNER")
        print("=" * 70)
        print(f"Config: {self.config_path}")

        # Get test modules
        test_modules = self._get_test_modules()
        print(f"Tests to run: {len(test_modules)}")

        if not test_modules:
            print("⚠️  No tests selected to run")
            return 0

        # Build pytest arguments
        pytest_args = self._build_pytest_args(test_modules)

        # Add custom reporting plugin
        pytest_args.extend([
            "--tb=short",
            "--strict-markers",
        ])

        # Run pytest
        print(f"\n🚀 Running pytest...\n")
        exit_code = pytest.main(pytest_args, plugins=[self])

        duration = time.time() - self.start_time

        # Generate report
        report_path = self._generate_report(exit_code, duration)
        print(f"\n📊 Test report: {report_path}")

        print(f"\n{'=' * 70}")
        print(f"Test run complete: {'✅ PASSED' if exit_code == 0 else '❌ FAILED'}")
        print(f"Duration: {duration:.2f}s")
        print(f"{'=' * 70}")

        return exit_code

    # pytest plugin hooks
    def pytest_runtest_logreport(self, report):
        """Collect test results."""
        if report.when != "call":
            return

        test_name = report.nodeid.split("::")[-1]

        if report.passed:
            self.results["passed"].append(test_name)
            self.results["timings"][test_name] = report.duration
        elif report.failed:
            self.results["failed"].append(test_name)
        elif report.skipped:
            self.results["skipped"].append(test_name)


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Strategy Unit Test Runner")
    parser.add_argument(
        "--config",
        type=Path,
        default=CONFIGS_DIR / "tests" / "test_config.yaml",
        help="Test runner config file"
    )
    parser.add_argument(
        "--mode",
        choices=["all", "selected"],
        help="Override run mode"
    )
    parser.add_argument(
        "--test",
        help="Run single test module (e.g., test_signal_generator)"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available test modules"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output"
    )

    args = parser.parse_args()

    # Handle list mode
    if args.list:
        unit_dir = _PROJECT_ROOT / "tests" / "unit"
        tests = sorted([f.stem for f in unit_dir.glob("test_*.py") if f.stem != "__init__"])
        print("\nAvailable test modules:")
        for test in tests:
            print(f"  - {test}")
        return 0

    # Override config with command line arguments
    if args.mode:
        # This would require modifying the loaded config
        pass

    if args.test:
        # Run single test
        pytest_args = ["-v", f"tests/unit/{args.test}.py"]
        return pytest.main(pytest_args)

    # Run full test suite
    runner = TestRunner(args.config)
    return runner.run()


if __name__ == "__main__":
    sys.exit(main())