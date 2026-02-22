#!/usr/bin/env python
"""
Strategy Unit Test Runner
=========================
Runs unit tests for all strategy modules with comprehensive reporting.
Includes real data test coverage tracking.

Usage:
    python tests/strategies/runners/strategy_unit_test.py
    python tests/strategies/runners/strategy_unit_test.py --config custom_config.yaml
    python tests/strategies/runners/strategy_unit_test.py --mode all
    python tests/strategies/runners/strategy_unit_test.py --test test_signal_generator
    python tests/strategies/runners/strategy_unit_test.py --report-coverage
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

# Set console encoding for Windows
if sys.platform == 'win32':
    import codecs
    # Only set if we're not in a pipe/redirection scenario
    if sys.stdout.isatty():
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'ignore')
    if sys.stderr.isatty():
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'ignore')

# Add project root to path
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.utils.paths import CONFIGS_DIR, test_path, ensure_dir


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
            "real_data_tests": [],  # Track tests that use real data
        }
        
        # Base directories for tests
        self.tests_base = _PROJECT_ROOT / "tests" / "strategies"
        self.unit_dir = self.tests_base / "unit"
        self.contracts_dir = self.unit_dir / "contracts"
        self.filters_dir = self.unit_dir / "filters"

    def _load_config(self) -> Dict:
        """Load test runner configuration."""
        if not self.config_path.exists():
            print(f"⚠️  Config not found: {self.config_path}, using defaults")
            return {
                "run_mode": "all",
                "enabled_tests": {},
                "execution": {"verbose": True},
                "report": {"output_dir": "tests/strategies/reports"},
                "test_data": {"use_real_data": True},
            }

        with open(self.config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def _get_test_modules(self) -> List[str]:
        """
        Get list of test modules to run based on configuration.
        Returns list of file paths relative to tests/strategies/
        """
        if self.config.get("run_mode") == "all":
            test_files = []
            
            # Core unit tests (directly in unit/)
            for f in self.unit_dir.glob("test_*.py"):
                if f.stem != "__init__":
                    # Return path relative to tests/strategies/
                    test_files.append(f"unit/{f.stem}")
            
            # Contract tests
            if self.contracts_dir.exists():
                for f in self.contracts_dir.glob("test_*.py"):
                    test_files.append(f"unit/contracts/{f.stem}")
            
            # Filter tests
            if self.filters_dir.exists():
                for f in self.filters_dir.glob("test_*.py"):
                    test_files.append(f"unit/filters/{f.stem}")
            
            return sorted(test_files)

        # Return enabled tests from config
        enabled = self.config.get("enabled_tests", {})
        return [
            test_name for test_name, enabled_flag in enabled.items()
            if enabled_flag
        ]

    def _build_pytest_args(self, test_modules: List[str]) -> List[str]:
        """
        Build pytest arguments.
        test_modules are paths relative to tests/strategies/
        """
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

        # Add test modules with full paths
        for module in test_modules:
            # Construct full path
            test_path = self.tests_base / f"{module}.py"
            args.append(str(test_path))

        return args

    def _is_real_data_test(self, test_name: str) -> bool:
        """Heuristic to identify real data tests by name."""
        real_data_indicators = [
            "real_data", "real_", "with_real", "actual_",
            "broker_config", "market_data", "real_trades"
        ]
        test_lower = test_name.lower()
        return any(indicator in test_lower for indicator in real_data_indicators)

    def _generate_report(self, exit_code: int, duration: float) -> Path:
        """Generate comprehensive markdown test report."""
        report_dir = Path(self.config.get("report", {}).get("output_dir", "tests/strategies/reports"))
        ensure_dir(report_dir)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = report_dir / f"test_report_{timestamp}.md"

        use_real_data = self.config.get("test_data", {}).get("use_real_data", True)

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(f"# Strategy Unit Test Report\n\n")
            f.write(f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Duration:** {duration:.2f}s\n")
            f.write(f"**Exit Code:** {exit_code} ({'PASSED' if exit_code == 0 else 'FAILED'})\n\n")

            # Configuration summary
            f.write(f"## Test Configuration\n\n")
            f.write(f"- **Run Mode:** {self.config.get('run_mode', 'unknown')}\n")
            f.write(f"- **Parallel:** {self.config.get('execution', {}).get('parallel', False)}\n")
            f.write(f"- **Fail Fast:** {self.config.get('execution', {}).get('fail_fast', False)}\n")
            f.write(f"- **Use Real Data:** {use_real_data}\n\n")

            # Results summary
            f.write(f"## Results Summary\n\n")
            f.write(f"| Result | Count |\n")
            f.write(f"|--------|-------|\n")
            f.write(f"| Passed | {len(self.results['passed'])} |\n")
            f.write(f"| Failed | {len(self.results['failed'])} |\n")
            f.write(f"| Skipped | {len(self.results['skipped'])} |\n")
            f.write(f"| Errors | {len(self.results['errors'])} |\n\n")

            # Real Data Test Coverage
            f.write(f"## Real Data Test Coverage\n\n")
            total_tests = len(self.results['passed']) + len(self.results['failed'])
            real_data_count = len(self.results['real_data_tests'])
            
            if total_tests > 0:
                coverage_pct = (real_data_count / total_tests) * 100
                f.write(f"- **Tests using real data:** {real_data_count}/{total_tests} ({coverage_pct:.1f}%)\n\n")
                
                if self.results['real_data_tests']:
                    f.write("**Real data tests executed:**\n\n")
                    for test in sorted(self.results['real_data_tests']):
                        timing = self.results['timings'].get(test, 0)
                        status = "PASSED" if test in self.results['passed'] else "FAILED"
                        f.write(f"- {status}: `{test}` ({timing:.3f}s)\n")
            else:
                f.write("- No tests executed\n\n")

            # Detailed results
            if self.results['passed']:
                f.write(f"\n## Passed Tests\n\n")
                for test in sorted(self.results['passed']):
                    timing = self.results['timings'].get(test, 0)
                    real_data_marker = " (real data)" if self._is_real_data_test(test) else ""
                    f.write(f"- `{test}`{real_data_marker} ({timing:.3f}s)\n")
                f.write(f"\n")

            if self.results['failed']:
                f.write(f"\n## Failed Tests\n\n")
                for test in sorted(self.results['failed']):
                    real_data_marker = " (real data)" if self._is_real_data_test(test) else ""
                    f.write(f"- `{test}`{real_data_marker}\n")
                f.write(f"\n")

            if self.results['skipped']:
                f.write(f"\n## Skipped Tests\n\n")
                for test in sorted(self.results['skipped']):
                    f.write(f"- `{test}`\n")
                f.write(f"\n")

            if self.results['errors']:
                f.write(f"\n## Errors\n\n")
                for error in self.results['errors']:
                    f.write(f"- `{error}`\n")
                f.write(f"\n")

            f.write(f"---\n")
            f.write(f"*Report generated by Strategy Unit Test Runner*\n")

        return report_path

    def run(self) -> int:
        """Execute tests and generate report."""
        self.start_time = time.time()

        print("=" * 70)
        print("STRATEGY UNIT TEST RUNNER")
        print("=" * 70)
        print(f"Config: {self.config_path}")
        print(f"Real data: {'ENABLED' if self.config.get('test_data', {}).get('use_real_data', True) else 'DISABLED'}")
        print(f"Test base: {self.tests_base}")

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
        print(f"\n{'='*70}")
        print("Running pytest...")
        print(f"{'='*70}\n")
        exit_code = pytest.main(pytest_args, plugins=[self])

        duration = time.time() - self.start_time

        # Generate report
        report_path = self._generate_report(exit_code, duration)
        print(f"\nTest report: {report_path}")

        # Print real data coverage summary
        total_tests = len(self.results['passed']) + len(self.results['failed'])
        real_data_count = len(self.results['real_data_tests'])
        if total_tests > 0:
            coverage_pct = (real_data_count / total_tests) * 100
            print(f"\nReal Data Test Coverage: {real_data_count}/{total_tests} ({coverage_pct:.1f}%)")

        print(f"\n{'=' * 70}")
        print(f"Test run complete: {'PASSED' if exit_code == 0 else 'FAILED'}")
        print(f"Duration: {duration:.2f}s")
        print(f"{'=' * 70}")

        return exit_code

    # pytest plugin hooks
    def pytest_runtest_logreport(self, report):
        """Collect test results."""
        if report.when != "call":
            return

        # Extract just the test function name, not the full path
        test_name = report.nodeid.split("::")[-1]

        if report.passed:
            self.results["passed"].append(test_name)
            self.results["timings"][test_name] = report.duration
            if self._is_real_data_test(test_name):
                self.results["real_data_tests"].append(test_name)
        elif report.failed:
            self.results["failed"].append(test_name)
            if self._is_real_data_test(test_name):
                self.results["real_data_tests"].append(test_name)
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
    parser.add_argument(
        "--report-coverage",
        action="store_true",
        help="Generate coverage report only (no tests)"
    )
    parser.add_argument(
        "--no-real-data",
        action="store_true",
        help="Disable real data tests (use mocks only)"
    )

    args = parser.parse_args()

    # Handle list mode
    if args.list:
        tests_base = _PROJECT_ROOT / "tests" / "strategies"
        unit_dir = tests_base / "unit"
        contracts_dir = unit_dir / "contracts"
        filters_dir = unit_dir / "filters"
        
        print("\nAvailable test modules:")
        print("=" * 60)
        
        # Core unit tests
        print("\nCore Modules (tests/strategies/unit/):")
        for f in sorted(unit_dir.glob("test_*.py")):
            if f.stem != "__init__":
                print(f"  - {f.stem}")
        
        # Contract tests
        if contracts_dir.exists():
            print("\nContracts (tests/strategies/unit/contracts/):")
            for f in sorted(contracts_dir.glob("test_*.py")):
                print(f"  - contracts/{f.stem}")
        
        # Filter tests
        if filters_dir.exists():
            print("\nFilters (tests/strategies/unit/filters/):")
            for f in sorted(filters_dir.glob("test_*.py")):
                print(f"  - filters/{f.stem}")
        
        print("\n" + "=" * 60)
        print("\nUsage examples:")
        print("  python tests/strategies/runners/strategy_unit_test.py --test test_signal_generator")
        print("  python tests/strategies/runners/strategy_unit_test.py --test contracts/test_analytics_contracts")
        print("  python tests/strategies/runners/strategy_unit_test.py --test filters/test_rsi_filter")
        return 0

    # Handle coverage report only
    if args.report_coverage:
        print("\nReal Data Test Coverage Report")
        print("=" * 40)
        print("To generate coverage report, run tests first.")
        print("Example: python tests/strategies/runners/strategy_unit_test.py")
        return 0

    # Handle single test run
    if args.test:
        tests_base = _PROJECT_ROOT / "tests" / "strategies"
        
        # Remove .py if provided (handle both "test_file" and "test_file.py")
        test_name = args.test
        if test_name.endswith('.py'):
            test_name = test_name[:-3]
        
        # Construct the full test path
        if test_name.startswith("contracts/"):
            # Contract test
            base_name = test_name.split('/')[-1]
            test_file = tests_base / "unit" / "contracts" / f"{base_name}.py"
        elif test_name.startswith("filters/"):
            # Filter test
            base_name = test_name.split('/')[-1]
            test_file = tests_base / "unit" / "filters" / f"{base_name}.py"
        else:
            # Core unit test
            test_file = tests_base / "unit" / f"{test_name}.py"
        
        if not test_file.exists():
            print(f"Error: Test file not found: {test_file}")
            print("\nAvailable tests:")
            print("  - Core: test_signal_generator, test_risk_manager, test_config_schema, etc.")
            print("  - Contracts: contracts/test_analytics_contracts, contracts/test_data_contracts, etc.")
            print("  - Filters: filters/test_rsi_filter, filters/test_adx_filter, etc.")
            return 1
        
        print(f"\n{'='*70}")
        print(f"Running single test: {test_file}")
        print(f"{'='*70}\n")
        
        pytest_args = ["-v", str(test_file)]
        if args.verbose:
            pytest_args.append("-v")
        return pytest.main(pytest_args)

    # Run full test suite
    runner = TestRunner(args.config)
    return runner.run()


if __name__ == "__main__":
    sys.exit(main())