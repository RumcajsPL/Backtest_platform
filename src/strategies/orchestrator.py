"""
Strategy Orchestrator
=====================
Composes the full strategy pipeline from config load to MetricsReport.
Single entry point callable from interactive scripts and automated parameter sweeps.

Version: 2.1.0
Session: Block 1 — Production Hardening

Changes from v2.0.0:
- [C4] _load_data: passes StrategyConfig and mode directly to DataLoader —
       eliminates the config_path reconstruction hack and the TODO comment.
       DataLoader now receives the same effective_mode as all other stages,
       consistent with Principle 4 (Explicit Over Implicit).
- Block A: Removed _read_htf_period() - now reads from StrategyConfig directly
- Block A: SignalGenerator now accepts StrategyConfig
- Block C: Signal translation moved to TradeSimulator (CF-6)
- Block C: CacheManager integration for multi-run state management
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Optional

from src.strategies.core.cache_manager import CacheManager
from src.strategies.specific.modules.data_loader import DataLoader
from src.strategies.specific.modules.signal_generator import SignalGenerator
from src.strategies.specific.modules.filter_pipeline import FilterPipeline
from src.strategies.specific.modules.trade_simulator import TradeSimulator
from src.strategies.specific.modules.metrics_calculator import calculate_metrics

from src.strategies.contracts.data_contracts import DataBundle
from src.strategies.contracts.signal_contracts import SignalFrame
from src.strategies.contracts.filter_contracts import FilterPipelineResult
from src.strategies.contracts.trade_contracts import TradeResult
from src.strategies.contracts.metrics_contracts import MetricsReport

from src.config.config_schema import StrategyConfig

logger = logging.getLogger(__name__)

_VALID_MODES = frozenset({"core", "analytics"})


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
    mode: str                       # "core" | "analytics" — the mode actually used

    # Stage outputs
    data_bundle: DataBundle
    signal_frame: SignalFrame
    filter_result: FilterPipelineResult
    trade_result: TradeResult
    metrics: MetricsReport

    # Timing
    stage_durations_ms: dict        # {"data": X, "signals": X, "filters": X, "trades": X, "metrics": X}
    total_duration_ms: float

    # Phase 9.2 extension stubs — None in Phase 9.1
    # analytics: Optional[AnalyticsReport] = None
    # report: Optional[GeneratedReport] = None

    @property
    def total_trades(self) -> int:
        """Total closed trades — authoritative count from MetricsReport."""
        return self.metrics.total_trades

    @property
    def win_rate(self) -> float:
        """Win rate as percentage (0–100)."""
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
    - Enforce cache management between runs via CacheManager
    - Log per-stage timing at INFO level
    - Raise immediately on any stage failure (fail-fast)

    Not responsible for:
    - Config file discovery (caller's job)
    - TradeAnalytics or ReportGenerator (Phase 9.2)
    - Parameter grid management (caller's job)
    """

    def __init__(
        self,
        config: StrategyConfig,
        cache_manager: Optional[CacheManager] = None,
    ) -> None:
        """
        Args:
            config: Validated StrategyConfig (from StrategyConfig.from_yaml).
            cache_manager: Optional cache manager for multi-run backtesting.
                           If not provided, a new one is created.
        """
        self._config = config
        self._cache_manager = cache_manager or CacheManager()

        self._mode: str = config.execution.mode

        if self._mode not in _VALID_MODES:
            raise ValueError(
                f"Invalid execution mode '{self._mode}'. "
                f"Must be one of {sorted(_VALID_MODES)}."
            )

        logger.info(
            "StrategyOrchestrator initialised | mode=%s | data=%s",
            self._mode,
            config.data.paths.strategy_ohlcv,
        )

    # ------------------------------------------------------------------
    # Class-level constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_yaml(cls, path: Path) -> "StrategyOrchestrator":
        """
        Convenience constructor — load config from YAML and return orchestrator.

        Raises:
            FileNotFoundError: if path does not exist
            ValueError: if config is invalid
        """
        if not path.exists():
            raise FileNotFoundError(
                f"Strategy config not found: {path}. "
                f"Copy configs/strategies/strategy_template.yaml as a starting point."
            )
        config = StrategyConfig.from_yaml(path)
        return cls(config)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        mode_override: Optional[str] = None,
    ) -> OrchestratorResult:
        """
        Execute the full pipeline and return an OrchestratorResult.

        Args:
            mode_override:  If provided, use this mode instead of config.execution.mode.
                            Useful for CLI --mode flag without editing YAML.
                            Must be 'core' or 'analytics'.

        Returns:
            OrchestratorResult with all stage outputs and timing.
            OrchestratorResult.mode reflects the mode actually used.

        Raises:
            ValueError: if mode_override is invalid.
            Any pipeline stage exception propagates immediately (fail-fast).
        """
        if mode_override is not None:
            if mode_override not in _VALID_MODES:
                raise ValueError(
                    f"Invalid mode_override '{mode_override}'. "
                    f"Must be one of {sorted(_VALID_MODES)}."
                )
            effective_mode = mode_override
            logger.info("Mode overridden by caller: %s → %s", self._mode, effective_mode)
        else:
            effective_mode = self._mode

        run_start = perf_counter()
        durations: dict = {}

        logger.info("─" * 60)
        logger.info("Pipeline run starting | mode=%s", effective_mode)

        # Stage 1: Data
        data_bundle = self._run_stage(
            name="data",
            durations=durations,
            fn=lambda: self._load_data(effective_mode),
        )

        # Stage 2: Signals
        signal_frame = self._run_stage(
            name="signals",
            durations=durations,
            fn=lambda: self._generate_signals(data_bundle, effective_mode),
        )

        # Stage 3: Filters
        filter_result = self._run_stage(
            name="filters",
            durations=durations,
            fn=lambda: self._run_filters(signal_frame, data_bundle, effective_mode),
        )

        # Stage 4: Trade simulation
        trade_result = self._run_stage(
            name="trades",
            durations=durations,
            fn=lambda: self._simulate_trades(filter_result, data_bundle, effective_mode),
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
            mode=effective_mode,
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

        return result

    # ------------------------------------------------------------------
    # Private stage methods
    # ------------------------------------------------------------------

    def _load_data(self, mode: str) -> DataBundle:
        """
        Stage 1: Load OHLCV data via DataLoader.

        [C4] DataLoader now receives StrategyConfig directly and the effective
        execution mode — consistent with every other pipeline stage and with
        Principle 4 (Explicit Over Implicit). The mode governs DataLoader's
        verbosity, LTF precomputation gating, and cache stats collection.
        """
        loader = DataLoader(
            config=self._config,
            mode=mode,
        )
        bundle = loader.load_data()

        logger.info(
            "Data loaded | strategy_bars=%d | total_bars=%d | htf=%s | ltf=%s | cache=%s",
            bundle.info.strategy_bars,
            bundle.info.total_bars,
            bundle.has_htf,
            bundle.has_ltf,
            bundle.info.cache_hit,
        )
        return bundle

    def _generate_signals(self, data_bundle: DataBundle, mode: str) -> SignalFrame:
        """
        Stage 2: Generate trading signals.

        SignalGenerator accepts StrategyConfig directly (DEC-034).
        """
        generator = SignalGenerator(
            config=self._config,
            mode=mode,
        )
        frame = generator.generate_signals(data_bundle)
        counts = frame.count_by_type()
        logger.info(
            "Signals generated | buy=%d | sell=%d | total=%d",
            counts["buy"],
            counts["sell"],
            counts["total"],
        )
        return frame

    def _run_filters(
        self,
        signal_frame: SignalFrame,
        data_bundle: DataBundle,
        mode: str,
    ) -> FilterPipelineResult:
        """
        Stage 3: Apply time and technical filters.
        """
        pipeline = FilterPipeline(config=self._config, mode=mode)

        result = pipeline.apply_filters(
            signal_frame=signal_frame,
            df=data_bundle.strategy,
            mode=mode,
        )

        logger.info(
            "Filters applied | in=%d | out=%d | pass_rate=%.1f%%",
            result.raw_count,
            result.final_count,
            result.pass_rate,
        )
        return result

    def _simulate_trades(
        self,
        filter_result: FilterPipelineResult,
        data_bundle: DataBundle,
        mode: str,
    ) -> TradeResult:
        """
        Stage 4: Simulate trade execution.

        TradeSimulator accepts SignalFrame directly (CF-6).
        """
        simulator = TradeSimulator(
            config=self._config,
            df_full=data_bundle.full,
            df_artf=data_bundle.artf,  # ← ADD THIS LINE
            cache_manager=self._cache_manager,
        )

        verbose = (mode == "analytics")

        result = simulator.simulate_trades(
            df_strategy=data_bundle.strategy,
            signal_frame=filter_result.final_signals,
            verbose=verbose,
            progressive_tracker=None,
            signal_id_map=None,
            df_ltf=data_bundle.ltf,
        )

        logger.info(
            "Trades simulated | opened=%d | closed=%d | wins=%d | losses=%d | "
            "pnl=%+.1f pts | mode=%s",
            result.total_opened,
            result.total_closed,
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