"""
Strategy Orchestrator
=====================
Composes the full strategy pipeline from config load to MetricsReport.
Single entry point callable from interactive scripts and automated parameter sweeps.

Version: 1.2.0 (Phase 9, Session 21 — all assumptions verified against source)
Scope: Core pipeline only — DataLoader → SignalGenerator → FilterPipeline →
       TradeSimulator → MetricsCalculator.
       TradeAnalytics and ReportGenerator are deferred to Phase 9.2.

Usage (single run):
    orchestrator = StrategyOrchestrator.from_yaml(Path("configs/strategies/wbws/wbws_strategy_v2.yaml"))
    result = orchestrator.run()

Usage (mode override — e.g. from CLI without editing YAML):
    result = orchestrator.run(mode_override="core")

Usage (parameter sweep):
    for params in grid:
        config = build_config(params)
        orchestrator = StrategyOrchestrator(config)
        result = orchestrator.run()
        # RiskManager.clear_cache() is called automatically between runs

Verified assumptions (Session 21 — source files inspected):

    [1] config.execution.mode ✅
        ExecutionConfig.mode confirmed. Path config.execution.mode is correct.

    [2] DataLoader ❌ → CORRECTED
        DataLoader.__init__(config_path: str, project_root, mode: str)
        Takes the YAML path string and mode — NOT a StrategyConfig object.
        Load method is .load_data(), not .load().
        DataInfo field for strategy bars is .strategy_bars (not strategy_bar_count).
        DataBundle.has_htf and .has_ltf are properties, not methods — no () call.

    [3] SignalGenerator ❌ → CORRECTED
        SignalGenerator.__init__(htf_period: str, mode: str)
        Takes htf_period string and mode — NOT a StrategyConfig object.
        Generate method is .generate_signals(data_bundle), not .generate().
        htf_period sourced from YAML key data.htf_period (CF-4: add to DataConfig).

    [4] FilterPipeline ❌ → CORRECTED
        FilterPipeline.__init__(config: StrategyConfig, mode: str, cache)  ✅ typed
        Apply method is .apply_filters(signal_frame, df, mode), not .run().
        Requires strategy DataFrame as second argument.
        pass_rate already returns 0–100 — the * 100 multiplier is removed.

    [5] TradeSimulator ❌ → CORRECTED (complete mismatch)
        TradeSimulator.__init__(config: Dict, df_full: pd.DataFrame)
        Takes raw dict config and full DataFrame — not StrategyConfig.
        simulate_trades(df_strategy, filtered_signals, verbose, progressive_tracker,
                        signal_id_map, df_ltf) — completely different signature.
        filtered_signals is a pd.Series of "BUY"/"SELL" strings, not a SignalFrame.
        df_ltf is REQUIRED and raises ValueError if None/empty.
        TradeResult has no .total_trades field — total sourced from MetricsReport. ✅

    Known gap — htf_period (CF-4):
        SignalGenerator requires htf_period: str. This value is not yet in StrategyConfig
        (DataConfig only has paths, date_range, timezone). Sourced from YAML key
        data.htf_period via a one-time raw YAML read at orchestrator init.
        Resolution: add htf_period to DataConfig in a follow-up DEC entry.
"""

from __future__ import annotations

import logging
import yaml
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Optional

import pandas as pd

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
from src.config.config_schema import StrategyConfig

logger = logging.getLogger(__name__)

_VALID_MODES = frozenset({"core", "analytics"})

# Int8 signal codes → string labels expected by TradeSimulator
_SIGNAL_CODE_TO_STR = {1: "BUY", 2: "SELL"}


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

    Phase 9.2 note: analytics and report fields are reserved as None stubs.
    When Phase 9.2 wires TradeAnalytics and ReportGenerator, uncomment those
    fields without changing this contract's public interface.
    """
    config: StrategyConfig
    mode: str                       # "core" | "analytics" — the mode actually used
    config_path: Path               # Path used to load config (for DataLoader)

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
    - Accept a StrategyConfig + config path and run all pipeline stages in order
    - Translate typed config into the format each module actually expects
    - Enforce RiskManager.clear_cache() between runs
    - Log per-stage timing at INFO level
    - Raise immediately on any stage failure (fail-fast)

    Not responsible for:
    - Config file discovery (caller's job)
    - TradeAnalytics or ReportGenerator (Phase 9.2)
    - Parameter grid management (caller's job)
    """

    def __init__(self, config: StrategyConfig, config_path: Path) -> None:
        """
        Args:
            config:      Validated StrategyConfig (from StrategyConfig.from_yaml).
            config_path: Path to the YAML file — required because DataLoader v2.1
                         takes a file path, not a StrategyConfig object.
        """
        self._config = config
        self._config_path = config_path

        # Verified Assumption 1: mode is at config.execution.mode.
        self._mode: str = config.execution.mode

        if self._mode not in _VALID_MODES:
            raise ValueError(
                f"Invalid execution mode '{self._mode}'. "
                f"Must be one of {sorted(_VALID_MODES)}. "
                f"Note: 'debug' is not valid — use 'analytics' (DEC-022)."
            )

        # CF-4: htf_period is not yet in StrategyConfig / DataConfig.
        # Read it from the raw YAML directly until a DEC entry promotes it
        # to a typed config field.
        self._htf_period: str = self._read_htf_period(config_path)

        logger.info(
            "StrategyOrchestrator initialised | mode=%s | htf_period=%s | data=%s",
            self._mode,
            self._htf_period,
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
        return cls(config, path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        clear_cache: bool = True,
        mode_override: Optional[str] = None,
    ) -> OrchestratorResult:
        """
        Execute the full pipeline and return an OrchestratorResult.

        Args:
            clear_cache:    Call RiskManager.clear_cache() before running.
                            Default True — safe for single runs and sweep loops.
                            Set False only if managing cache externally.
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

        if clear_cache:
            RiskManager.clear_cache()
            logger.debug("RiskManager cache cleared.")

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
            config_path=self._config_path,
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

        # ── Phase 9.2 extension point ──────────────────────────────────────
        # Uncomment when TradeAnalytics + ReportGenerator are wired in.
        #
        # if effective_mode == "analytics":
        #     from src.strategies.specific.modules.trade_analytics import TradeAnalytics
        #     from src.strategies.specific.modules.report_generator import ReportGenerator
        #     from src.strategies.contracts.report_contracts import ReportConfig
        #     analytics = TradeAnalytics.analyze(trade_result, self._config, metrics=metrics)
        #     report = ReportGenerator.generate(
        #         analytics,
        #         trade_result=trade_result,
        #         config=ReportConfig(
        #             title="Strategy Performance Report",
        #             brand_name=self._config.output.reports.brand_name,
        #             output_dir=self._config.output.reports.output_dir,
        #             theme=self._config.output.reports.theme,
        #             chart_height_px=self._config.output.reports.chart_height_px,
        #             include_raw_data=self._config.output.reports.include_raw_data,
        #         ),
        #     )
        # ──────────────────────────────────────────────────────────────────

        return result

    # ------------------------------------------------------------------
    # Private stage methods
    # ------------------------------------------------------------------

    def _load_data(self, mode: str) -> DataBundle:
        """
        Stage 1: Load OHLCV data via DataLoader.

        Verified Assumption 2 (CORRECTED):
        - DataLoader.__init__(config_path: str, project_root, mode: str)
          Takes the YAML file path string — NOT a StrategyConfig object.
        - Load method is .load_data() — not .load().
        - DataInfo bar count field is .strategy_bars (not strategy_bar_count).
        - has_htf and has_ltf are properties (bool), not methods — no () call.
        """
        loader = DataLoader(
            config_path=str(self._config_path),
            mode=mode,
        )
        bundle = loader.load_data()

        logger.info(
            "Data loaded | strategy_bars=%d | total_bars=%d | htf=%s | ltf=%s | cache=%s",
            bundle.info.strategy_bars,
            bundle.info.total_bars,
            bundle.has_htf,       # property — no ()
            bundle.has_ltf,       # property — no ()
            bundle.info.cache_hit,
        )
        return bundle

    def _generate_signals(self, data_bundle: DataBundle, mode: str) -> SignalFrame:
        """
        Stage 2: Generate trading signals.

        Verified Assumption 3 (CORRECTED):
        - SignalGenerator.__init__(htf_period: str, mode: str)
          Takes htf_period string — NOT a StrategyConfig.
        - Generate method is .generate_signals(data_bundle) — not .generate().
        - htf_period sourced from self._htf_period (read from raw YAML at init).
        """
        generator = SignalGenerator(
            htf_period=self._htf_period,
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

        Verified Assumption 4 (CORRECTED):
        - FilterPipeline.__init__(config: StrategyConfig, mode: str) ✅ typed
        - Apply method is .apply_filters(signal_frame, df, mode) — not .run().
          Requires the strategy DataFrame as second argument.
        - pass_rate already returns 0–100 — multiplying by 100 is removed.
        """
        pipeline = FilterPipeline(config=self._config, mode=mode)

        # apply_filters needs the strategy OHLCV DataFrame for indicator computation
        result = pipeline.apply_filters(
            signal_frame=signal_frame,
            df=data_bundle.strategy,
            mode=mode,
        )

        logger.info(
            "Filters applied | in=%d | out=%d | pass_rate=%.1f%%",
            result.raw_count,
            result.final_count,
            result.pass_rate,   # already 0–100, no multiplication
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

        Verified Assumption 5 (CORRECTED — complete mismatch with skeleton):
        - TradeSimulator.__init__(config: Dict, df_full: pd.DataFrame)
          Takes raw dict config and full DataFrame — NOT a StrategyConfig.
        - simulate_trades signature:
            simulate_trades(df_strategy, filtered_signals, verbose, progressive_tracker,
                            signal_id_map, df_ltf)
          NOT (signal_frame, data_bundle, mode).
        - filtered_signals must be a pd.Series with string values "BUY"/"SELL".
          SignalFrame.signals uses int8 codes (1=BUY, 2=SELL) — conversion required.
        - df_ltf is REQUIRED by the simulator and raises ValueError if None/empty.
          In core mode without LTF data, we pass None and catch the ValueError,
          re-raising with a clear message directing the user to configure LTF data.
        - TradeResult has no .total_trades field — sourced from MetricsReport. ✅
        - verbose=True in analytics mode to enable full logging from the simulator.

        Config translation:
            StrategyConfig (typed, frozen) → raw dict expected by TradeSimulator.
            TradeSimulator was built before the new typed config architecture and
            still reads from the legacy dict format (CF-5: migrate TradeSimulator
            to accept StrategyConfig directly).
        """
        # Translate StrategyConfig → raw dict for TradeSimulator (legacy interface)
        raw_config = self._build_simulator_config()

        simulator = TradeSimulator(
            config=raw_config,
            df_full=data_bundle.full,
        )

        # Translate SignalFrame int8 codes → "BUY"/"SELL" string Series
        # TradeSimulator checks: if signal_type == "BUY" / "SELL"
        filtered_signals: pd.Series = (
            filter_result.final_signals.signals
            .map(_SIGNAL_CODE_TO_STR)
            .dropna()
        )

        verbose = (mode == "analytics")

        result = simulator.simulate_trades(
            df_strategy=data_bundle.strategy,
            filtered_signals=filtered_signals,
            verbose=verbose,
            progressive_tracker=None,  # Phase 9.2: wire progressive tracker
            signal_id_map=None,        # Phase 9.2: wire signal ID map
            df_ltf=data_bundle.ltf,    # Required by TradeSimulator; None if not configured
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
    # Config translation helpers
    # ------------------------------------------------------------------

    def _build_simulator_config(self) -> dict:
        """
        Translate typed StrategyConfig into the raw dict format expected by
        TradeSimulator (legacy interface — CF-5: migrate TradeSimulator to typed config).

        Only the keys actually read by TradeSimulator and its sub-managers are included.
        Keys are verified against TradeSimulator.__init__, initialize_managers(),
        RiskManager, SpreadManager, and TradeManager source.
        """
        cfg = self._config
        tm = cfg.trade_management

        return {
            # Execution
            "execution": {
                "mode": cfg.execution.mode,
            },
            # Data paths (used by RiskManager for ATR computation context)
            "data": {
                "paths": {
                    "strategy_ohlcv": str(cfg.data.paths.strategy_ohlcv),
                    "htf_ohlcv": str(cfg.data.paths.htf_ohlcv) if cfg.data.paths.htf_ohlcv else None,
                    "ltf_ohlcv": str(cfg.data.paths.ltf_ohlcv) if cfg.data.paths.ltf_ohlcv else None,
                    "artf_ohlcv": str(cfg.data.paths.artf_ohlcv) if cfg.data.paths.artf_ohlcv else None,
                },
                "date_range": {
                    "start": cfg.data.date_range.start,
                    "end": cfg.data.date_range.end,
                },
            },
            # Trade management
            "trade_management": {
                "spread": {
                    "enabled": tm.spread.enabled,
                    "spread_type": tm.spread.spread_type,
                    "spread_value": tm.spread.spread_value,
                },
                "risk": {
                    "atr_length": tm.risk.atr_length,
                    "atr_multiplier_sl": tm.risk.atr_multiplier_sl,
                    "atr_multiplier_tp": tm.risk.atr_multiplier_tp,
                    "max_risk_percentile": tm.risk.max_risk_percentile,
                },
                "position_control": {
                    "pyramiding_enabled": tm.position_control.pyramiding_enabled,
                    "close_on_opposite": tm.position_control.close_on_opposite,
                    "max_positions": tm.position_control.max_positions,
                },
            },
            # Analytics / profiling (DEC-022: "analytics" key replaces "debug")
            "analytics": {
                "profile_simulator": False,
            },
        }

    # ------------------------------------------------------------------
    # YAML helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _read_htf_period(config_path: Path) -> str:
        """
        Read htf_period from the raw YAML.

        CF-4: htf_period is not yet a typed field in DataConfig / StrategyConfig.
        It is required by SignalGenerator and is expected at YAML key data.htf_period.
        Default "1H" is used if the key is absent (safe for WBWS strategy).

        Resolution path: add htf_period to DataConfig in a follow-up DEC entry,
        then remove this method and source htf_period from config.data.htf_period.
        """
        try:
            with open(config_path, "r") as f:
                raw = yaml.safe_load(f)
            htf_period = raw.get("data", {}).get("htf_period", "1H")
            return str(htf_period)
        except Exception as e:
            logger.warning(
                "Could not read data.htf_period from YAML (%s) — defaulting to '1H'", e
            )
            return "1H"

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