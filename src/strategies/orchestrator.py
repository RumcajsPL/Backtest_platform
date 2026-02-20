"""
Strategy Orchestrator
=====================
Composes the full strategy pipeline from config load to MetricsReport.
Single entry point callable from interactive scripts and automated parameter sweeps.

Version: 1.0.0 (Phase 9, Session 21)
Scope: Core pipeline only — DataLoader → SignalGenerator → FilterPipeline →
       TradeSimulator → MetricsCalculator.
       TradeAnalytics and ReportGenerator are deferred to Phase 9.2.

Usage (single run):
    orchestrator = StrategyOrchestrator.from_yaml(Path("configs/strategies/wbws/wbws_strategy_v2.yaml"))
    result = orchestrator.run()

Usage (parameter sweep):
    for params in grid:
        config = build_config(params)
        orchestrator = StrategyOrchestrator(config)
        result = orchestrator.run()
        # RiskManager.clear_cache() is called automatically between runs
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from time import perf_counter
from typing import Optional

# --- Pipeline modules ---
from src.strategies.specific.modules.data_loader import DataLoader
from src.strategies.specific.modules.signal_generator import SignalGenerator
from src.strategies.specific.modules.filter_pipeline import FilterPipeline
from src.strategies.specific.modules.trade_simulator import TradeSimulator
from src.strategies.specific.modules.risk_manager import RiskManager
from src.strategies.specific.modules.metrics_calculator import calculate_metrics

# --- Contracts ---
from src.strategies.contracts.data_contracts import DataBundle
from src.strategies.contracts.signal_contracts import SignalFrame
from src.strategies.contracts.filter_contracts import FilterPipelineResult
from src.strategies.contracts.trade_contracts import TradeResult
from src.strategies.contracts.metrics_contracts import MetricsReport

# --- Config ---
# ASSUMPTION: StrategyConfig lives at src/config/config_schema.py (new arch path)
from src.config.config_schema import StrategyConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result contract
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class OrchestratorResult:
    """
    Output of a single orchestrator run.

    All intermediate contracts are preserved so callers can inspect any stage
    without re-running the pipeline. In a tight backtester loop, callers that
    only need MetricsReport can ignore the rest.
    """
    config: StrategyConfig
    mode: str                           # "core" | "analytics"

    # Stage outputs — all populated on success
    data_bundle: DataBundle
    signal_frame: SignalFrame
    filter_result: FilterPipelineResult
    trade_result: TradeResult
    metrics: MetricsReport

    # Timing
    stage_durations_ms: dict            # {"data": X, "signals": X, "filters": X, "trades": X, "metrics": X}
    total_duration_ms: float

    @property
    def total_trades(self) -> int:
        return self.metrics.total_trades

    @property
    def win_rate(self) -> float:
        return self.metrics.win_rate

    @property
    def total_pnl_points(self) -> float:
        return self.metrics.total_pnl_points

    def summary(self) -> str:
        """One-line result summary for logging and console output."""
        return (
            f"[{self.mode.upper()}] "
            f"trades={self.total_trades} | "
            f"win_rate={self.win_rate:.1f}% | "
            f"pnl={self.total_pnl_points:+.1f}pts | "
            f"total={self.total_duration_ms:.0f}ms"
        )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

class StrategyOrchestrator:
    """
    Composes and executes the strategy pipeline.

    Responsibilities:
    - Accept a StrategyConfig and run all pipeline stages in order
    - Gate expensive operations on execution mode
    - Enforce RiskManager.clear_cache() between runs when reused
    - Log per-stage timing at INFO level
    - Raise immediately on any stage failure (fail-fast, DEC leitmotif)

    Not responsible for:
    - Config file discovery or path resolution (caller's job)
    - TradeAnalytics or ReportGenerator (Phase 9.2)
    - Parameter grid management (caller's job)
    """

    def __init__(self, config: StrategyConfig) -> None:
        self._config = config

        # ASSUMPTION: execution mode is at config.execution.mode
        # If StrategyConfig exposes mode differently (e.g. config.mode), update here.
        self._mode: str = config.execution.mode

        if self._mode not in {"core", "analytics"}:
            raise ValueError(
                f"Invalid execution mode '{self._mode}'. Must be 'core' or 'analytics'. "
                f"Check execution.mode in your strategy YAML."
            )

        logger.info(
            "StrategyOrchestrator initialised | mode=%s | config=%s",
            self._mode,
            # ASSUMPTION: StrategyConfig has no __str__ that leaks sensitive paths,
            # so we just log the data path as a run identifier.
            # If DataPathsConfig exposes strategy_ohlcv differently, update this.
            config.data.paths.strategy_ohlcv,
        )

    @classmethod
    def from_yaml(cls, path: Path) -> "StrategyOrchestrator":
        """
        Convenience constructor — load config from YAML and return orchestrator.

        Raises:
            FileNotFoundError: if path does not exist (fail-fast)
            ValueError: if config is invalid
        """
        if not path.exists():
            raise FileNotFoundError(
                f"Strategy config not found: {path}. "
                f"Use configs/strategies/strategy_template.yaml as a starting point."
            )
        config = StrategyConfig.from_yaml(path)
        return cls(config)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, clear_cache: bool = True) -> OrchestratorResult:
        """
        Execute the full pipeline and return an OrchestratorResult.

        Args:
            clear_cache: Call RiskManager.clear_cache() before running.
                         Default True — safe for single runs and sweep loops.
                         Set False only if you are managing cache lifecycle
                         externally (advanced multi-run patterns).

        Returns:
            OrchestratorResult with all stage outputs and timing.

        Raises:
            Any exception from a pipeline stage propagates immediately.
            The orchestrator does not swallow errors (fail-fast principle).
        """
        run_start = perf_counter()
        durations: dict = {}

        if clear_cache:
            RiskManager.clear_cache()
            logger.debug("RiskManager cache cleared before run.")

        logger.info("─" * 60)
        logger.info("Pipeline run starting | mode=%s", self._mode)

        # Stage 1: Data
        data_bundle = self._run_stage(
            name="data",
            durations=durations,
            fn=self._load_data,
        )

        # Stage 2: Signals
        signal_frame = self._run_stage(
            name="signals",
            durations=durations,
            fn=lambda: self._generate_signals(data_bundle),
        )

        # Stage 3: Filters
        filter_result = self._run_stage(
            name="filters",
            durations=durations,
            fn=lambda: self._run_filters(signal_frame),
        )

        # Stage 4: Trade simulation
        trade_result = self._run_stage(
            name="trades",
            durations=durations,
            fn=lambda: self._simulate_trades(filter_result, data_bundle),
        )

        # Stage 5: Metrics
        metrics = self._run_stage(
            name="metrics",
            durations=durations,
            fn=lambda: calculate_metrics(trade_result),
        )

        total_ms = (perf_counter() - run_start) * 1000

        result = OrchestratorResult(
            config=self._config,
            mode=self._mode,
            data_bundle=data_bundle,
            signal_frame=signal_frame,
            filter_result=filter_result,
            trade_result=trade_result,
            metrics=metrics,
            stage_durations_ms=durations,
            total_duration_ms=total_ms,
        )

        logger.info("Pipeline run complete | %s", result.summary())
        logger.info("─" * 60)

        # Phase 9.2 extension point:
        # if self._mode == "analytics":
        #     analytics = TradeAnalytics.analyze(trade_result, self._config, metrics=metrics)
        #     report = ReportGenerator.generate(analytics, trade_result=trade_result, config=...)
        #     return OrchestratorResult(..., analytics=analytics, report=report)

        return result

    # ------------------------------------------------------------------
    # Private stage methods
    # ------------------------------------------------------------------

    def _load_data(self) -> DataBundle:
        """
        Stage 1: Load OHLCV data via DataLoader.

        ASSUMPTION: DataLoader accepts (config) at __init__ with no mode parameter.
        If DataLoader also requires mode, change to DataLoader(self._config, mode=self._mode).
        """
        loader = DataLoader(self._config)
        # ASSUMPTION: the load method is .load() with no arguments.
        # If the method is .load_data() or .run(), update here.
        bundle = loader.load()
        logger.info(
            "Data loaded | bars=%s | htf=%s | ltf=%s",
            bundle.info.strategy_bar_count,   # ASSUMPTION: DataInfo has strategy_bar_count
            bundle.has_htf(),
            bundle.has_ltf(),
        )
        return bundle

    def _generate_signals(self, data_bundle: DataBundle) -> SignalFrame:
        """
        Stage 2: Generate trading signals.

        ASSUMPTION: SignalGenerator accepts (config) at __init__.
        ASSUMPTION: The generation method is .generate(data_bundle).
        """
        generator = SignalGenerator(self._config)
        frame = generator.generate(data_bundle)
        counts = frame.count_by_type()
        logger.info(
            "Signals generated | buy=%d | sell=%d | total=%d",
            counts["buy"],
            counts["sell"],
            counts["total"],
        )
        return frame

    def _run_filters(self, signal_frame: SignalFrame) -> FilterPipelineResult:
        """
        Stage 3: Apply time and technical filters.

        ASSUMPTION: FilterPipeline accepts (config) at __init__ — mode is read
        from config.execution.mode internally, consistent with other modules.
        ASSUMPTION: The run method is .run(signal_frame).
        If the method is .apply() or .execute(), update here.
        """
        pipeline = FilterPipeline(self._config)
        result = pipeline.run(signal_frame)
        logger.info(
            "Filters applied | in=%d | out=%d | pass_rate=%.1f%%",
            result.raw_count,
            result.final_count,
            result.pass_rate * 100,
        )
        return result

    def _simulate_trades(
        self,
        filter_result: FilterPipelineResult,
        data_bundle: DataBundle,
    ) -> TradeResult:
        """
        Stage 4: Simulate trade execution.

        ASSUMPTION: TradeSimulator accepts (config) at __init__.
        ASSUMPTION: simulate_trades() signature is:
            simulate_trades(signal_frame, data_bundle, mode) -> TradeResult
        where signal_frame is the filtered signals from FilterPipelineResult.
        """
        simulator = TradeSimulator(self._config)
        result = simulator.simulate_trades(
            signal_frame=filter_result.final_signals,
            data_bundle=data_bundle,
            mode=self._mode,
        )
        logger.info(
            "Trades simulated | trades=%d | wins=%d | losses=%d | pnl=%+.1f pts | mode=%s",
            result.total_trades if hasattr(result, "total_trades") else len(result.trades),
            result.win_count,
            result.loss_count,
            result.total_pnl_points,
            result.execution_mode,
        )
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _run_stage(name: str, durations: dict, fn) -> object:
        """
        Execute a pipeline stage, record its duration, and log timing.
        Any exception propagates immediately — no swallowing.
        """
        t0 = perf_counter()
        result = fn()
        elapsed_ms = (perf_counter() - t0) * 1000
        durations[name] = round(elapsed_ms, 2)
        logger.info("  %-10s %8.1f ms", f"[{name}]", elapsed_ms)
        return result