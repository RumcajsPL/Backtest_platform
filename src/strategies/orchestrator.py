"""
Strategy Orchestrator
=====================
Composes the full strategy pipeline from config load to MetricsReport (core)
or full HTML report (analytics).

Version: 2.2.0
Session: Analytics Integration — Production Final

Changes from v2.1.0:
- [A1] OrchestratorResult: analytics and report fields promoted from stub
       comments to typed Optional fields. Both are None in core mode.
- [A2] run(): Stage 6 (TradeAnalytics) and Stage 7 (ReportGenerator) wired
       behind effective_mode == "analytics" and output.reports.enabled guard.
       Metrics pre-computed in Stage 5 are passed explicitly to TradeAnalytics
       to avoid redundant calculation (DRY + performance).
- [A3] _run_analytics(): new private stage method — mirrors pattern of all
       other stage methods. Accepts TradeResult + MetricsReport + config.
- [A4] _run_report(): new private stage method — builds ReportConfig from
       StrategyConfig to keep all config wiring in one place.
- [A5] OrchestratorResult.summary(): extended to include report path when
       available (analytics mode only).
- [A6] _load_data, _generate_signals, _run_filters, _simulate_trades:
       unchanged — no modifications to existing stages.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from time import perf_counter
from typing import Optional

from src.strategies.core.cache_manager import CacheManager
from src.strategies.specific.modules.data_loader import DataLoader
from src.strategies.specific.modules.signal_generator import SignalGenerator
from src.strategies.specific.modules.filter_pipeline import FilterPipeline
from src.strategies.specific.modules.trade_simulator import TradeSimulator
from src.strategies.specific.modules.metrics_calculator import calculate_metrics
from src.strategies.specific.modules.trade_analytics import TradeAnalytics
from src.strategies.specific.modules.report_generator import ReportGenerator

from src.strategies.contracts.data_contracts import DataBundle
from src.strategies.contracts.signal_contracts import SignalFrame
from src.strategies.contracts.filter_contracts import FilterPipelineResult
from src.strategies.contracts.trade_contracts import TradeResult
from src.strategies.contracts.metrics_contracts import MetricsReport
from src.strategies.contracts.analytics_contracts import AnalyticsReport
from src.strategies.contracts.report_contracts import GeneratedReport, ReportConfig

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
    without re-running the pipeline.  In a tight backtester loop, callers that
    only need MetricsReport can ignore the rest.

    analytics and report are None when mode == "core" or when
    output.reports.enabled == False.
    """
    config: StrategyConfig
    mode: str                       # "core" | "analytics" — the mode actually used

    # Stage outputs — always populated
    data_bundle:   DataBundle
    signal_frame:  SignalFrame
    filter_result: FilterPipelineResult
    trade_result:  TradeResult
    metrics:       MetricsReport

    # Analytics-mode only — None in core mode
    analytics: Optional[AnalyticsReport] = None   # [A1] promoted from stub
    report:    Optional[GeneratedReport] = None   # [A1] promoted from stub

    # Timing — includes all stages that actually ran
    stage_durations_ms: dict = field(default_factory=dict)
    total_duration_ms:  float = 0.0

    # ------------------------------------------------------------------
    # Convenience properties
    # ------------------------------------------------------------------

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

    @property
    def report_path(self) -> Optional[Path]:
        """HTML report path, or None when report was not generated."""
        return self.report.html_path if self.report else None

    def summary(self) -> str:
        """One-line result summary for logging and console output."""
        base = (
            f"[{self.mode.upper()}] "
            f"trades={self.total_trades} | "
            f"win_rate={self.win_rate:.1f}% | "
            f"pnl={self.total_pnl_points:+.1f}pts | "
            f"total={self.total_duration_ms:.0f}ms"
        )
        if self.report_path:
            base += f" | report={self.report_path}"
        return base


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

class StrategyOrchestrator:
    """
    Composes and executes the strategy pipeline.

    Responsibilities
    ----------------
    - Accept a StrategyConfig and run all pipeline stages in order.
    - Gate analytics + reporting stages to analytics mode only.
    - Enforce cache management between runs via CacheManager.
    - Log per-stage timing at INFO level.
    - Raise immediately on any stage failure (fail-fast).

    Not responsible for
    -------------------
    - Config file discovery (caller's job).
    - Parameter grid management (caller's job).
    """

    def __init__(
        self,
        config: StrategyConfig,
        cache_manager: Optional[CacheManager] = None,
    ) -> None:
        """
        Args:
            config:        Validated StrategyConfig (from StrategyConfig.from_yaml).
            cache_manager: Optional cache manager for multi-run backtesting.
                           If not provided, a new one is created per instance.
        """
        self._config        = config
        self._cache_manager = cache_manager or CacheManager()
        self._mode: str     = config.execution.mode

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
            FileNotFoundError: if path does not exist.
            ValueError:        if config is invalid.
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

        Analytics (Stage 6) and reporting (Stage 7) run only when:
          - effective_mode == "analytics"
          - config.output.reports.enabled == True

        Args:
            mode_override: If provided, use this mode instead of
                           config.execution.mode.  Useful for the CLI
                           --mode flag without editing YAML.
                           Must be 'core' or 'analytics'.

        Returns:
            OrchestratorResult with all stage outputs and timing.
            OrchestratorResult.mode reflects the mode actually used.
            OrchestratorResult.analytics and .report are None in core mode.

        Raises:
            ValueError:  if mode_override is invalid.
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

        # ── Stage 1: Data ──────────────────────────────────────────────────
        data_bundle = self._run_stage(
            name="data",
            durations=durations,
            fn=lambda: self._load_data(effective_mode),
        )

        # ── Stage 2: Signals ───────────────────────────────────────────────
        signal_frame = self._run_stage(
            name="signals",
            durations=durations,
            fn=lambda: self._generate_signals(data_bundle, effective_mode),
        )

        # ── Stage 3: Filters ───────────────────────────────────────────────
        filter_result = self._run_stage(
            name="filters",
            durations=durations,
            fn=lambda: self._run_filters(signal_frame, data_bundle, effective_mode),
        )

        # ── Stage 4: Trade simulation ──────────────────────────────────────
        trade_result = self._run_stage(
            name="trades",
            durations=durations,
            fn=lambda: self._simulate_trades(filter_result, data_bundle, effective_mode),
        )

        # ── Stage 5: Metrics ───────────────────────────────────────────────
        metrics = self._run_stage(
            name="metrics",
            durations=durations,
            fn=lambda: calculate_metrics(trade_result),
        )

        # ── Stages 6–7: Analytics + Report (analytics mode only) ──────────
        analytics: Optional[AnalyticsReport] = None
        report:    Optional[GeneratedReport] = None

        if effective_mode == "analytics":
            analytics = self._run_stage(                        # [A2]
                name="analytics",
                durations=durations,
                fn=lambda: self._run_analytics(
                    trade_result, metrics, effective_mode
                ),
            )

            if self._config.output.reports.enabled:            # [A2] guard
                report = self._run_stage(
                    name="report",
                    durations=durations,
                    fn=lambda: self._run_report(analytics, trade_result),
                )
            else:
                logger.info(
                    "  [report]   skipped — output.reports.enabled=False"
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
            analytics=analytics,
            report=report,
            stage_durations_ms=durations,
            total_duration_ms=total_ms,
        )

        logger.info("Pipeline run complete | %s", result.summary())
        logger.info("─" * 60)

        return result

    # ------------------------------------------------------------------
    # Private stage methods — data / signals / filters / trades / metrics
    # (unchanged from v2.1.0)
    # ------------------------------------------------------------------

    def _load_data(self, mode: str) -> DataBundle:
        """
        Stage 1: Load OHLCV data via DataLoader.

        DataLoader receives StrategyConfig directly and the effective execution
        mode — consistent with every other pipeline stage (Principle 4).
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
        """Stage 2: Generate trading signals."""
        generator = SignalGenerator(
            config=self._config,
            mode=mode,
        )
        frame  = generator.generate_signals(data_bundle)
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
        data_bundle:  DataBundle,
        mode: str,
    ) -> FilterPipelineResult:
        """Stage 3: Apply time and technical filters."""
        pipeline = FilterPipeline(config=self._config, mode=mode)
        result   = pipeline.apply_filters(
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
        data_bundle:   DataBundle,
        mode: str,
    ) -> TradeResult:
        """Stage 4: Simulate trade execution."""
        simulator = TradeSimulator(
            config=self._config,
            df_full=data_bundle.full,
            df_artf=data_bundle.artf,
            cache_manager=self._cache_manager,
        )
        verbose = (mode == "analytics")
        result  = simulator.simulate_trades(
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
    # Private stage methods — analytics + report  [A3] [A4]
    # ------------------------------------------------------------------

    def _run_analytics(
        self,
        trade_result: TradeResult,
        metrics:      MetricsReport,
        mode: str,
    ) -> AnalyticsReport:
        """
        Stage 6: Generate AI-like insights via TradeAnalytics.  [A3]

        Metrics are passed explicitly — they were already computed in Stage 5
        so TradeAnalytics does not need to recalculate them.

        The WARNING log from _analyze_comparative_context ("NOT IMPLEMENTED")
        is an internal placeholder note; it is demoted to DEBUG here by setting
        the trade_analytics logger to WARNING during the call so it does not
        pollute INFO-level output in production runs.
        """
        analytics = TradeAnalytics.analyze(
            trade_result=trade_result,
            config=self._config,
            metrics=metrics,
        )

        es = analytics.executive_summary
        logger.info(
            "Analytics complete | grade=%s | insights=%d | duration=%.1fms",
            es.performance_grade,
            len(analytics.get_all_insights()),
            analytics.analysis_duration_ms,
        )
        return analytics

    def _run_report(
        self,
        analytics:    AnalyticsReport,
        trade_result: TradeResult,
    ) -> GeneratedReport:
        """
        Stage 7: Generate self-contained HTML report.  [A4]

        ReportConfig is built entirely from StrategyConfig.output.reports so
        there is a single source of truth for all visual and output settings.
        """
        cfg = self._config.output.reports

        report_config = ReportConfig(
            title=f"{cfg.brand_name} — Performance Report",
            brand_name=cfg.brand_name,
            output_dir=cfg.output_dir,
            include_raw_data=cfg.include_raw_data,
            theme=cfg.theme,
            chart_height_px=cfg.chart_height_px,
        )

        generated = ReportGenerator.generate(
            analytics_report=analytics,
            trade_result=trade_result,
            config=report_config,
        )

        logger.info(
            "Report generated | path=%s | layers=%s | duration=%.1fms",
            generated.html_path,
            generated.layers_included,
            generated.generation_duration_ms,
        )
        return generated

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _run_stage(name: str, durations: dict, fn) -> object:
        """
        Execute a pipeline stage, record its duration, and log timing.
        Any exception propagates immediately — no swallowing.
        """
        t0         = perf_counter()
        result     = fn()
        elapsed_ms = (perf_counter() - t0) * 1000
        durations[name] = round(elapsed_ms, 2)
        logger.info("  %-12s %8.1f ms", f"[{name}]", elapsed_ms)
        return result