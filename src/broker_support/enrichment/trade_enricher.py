"""
TradeEnricher — fills incomplete Trade fields after snapshot closure detection.

Architecture confirmed by empirical test (2026-03-12):
  RESULT A — demo trades appear in GET /trading/info/trade/history.
  Enrichment path: fetch trade by positionId from history, fill exit_price
  and profit_loss directly from the authoritative API response.

Two-stage enrichment:
  1. enrich_from_history(trade) — fills exit_price, profit_loss, fees, leverage,
     sl_rate, tp_rate from the trade history endpoint.
  2. resolve_instrument(trade)  — fills trade.instrument symbol via InstrumentResolver.

PositionTracker calls enrich() which runs both stages in order.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

from loguru import logger

from src.broker_support.client.client import EToroClient
from src.broker_support.config.settings import settings
from src.broker_support.enrichment.instrument_resolver import InstrumentResolver
from src.broker_support.models.trade import Trade


class TradeEnricher:
    """
    Enriches Trade objects produced by PositionTracker with data from the
    trade history endpoint and the instrument map.
    """

    def __init__(self, instrument_map_path: Path) -> None:
        self._client = EToroClient()
        self._resolver = InstrumentResolver(instrument_map_path)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def enrich(self, trade: Trade) -> Trade:
        """
        Run full enrichment on a Trade:
          1. Fill exit_price, profit_loss and financial fields from history.
          2. Fill instrument symbol from InstrumentResolver.

        Returns the enriched Trade (same object, mutated via model_copy).
        If history lookup fails, the trade is returned with placeholder values
        and a warning is logged — never raises.
        """
        trade = self._enrich_from_history(trade)
        trade = self._resolve_instrument(trade)
        return trade

    # ------------------------------------------------------------------
    # Stage 1 — history enrichment
    # ------------------------------------------------------------------

    def _enrich_from_history(self, trade: Trade) -> Trade:
        """
        Fetch the authoritative closed-trade record from history and fill
        exit_price, profit_loss, fees, leverage, sl_rate, tp_rate.

        Uses settings.default_days_back as the lookback window (currently 29 days).
        The eToro API hard limit is 30 days but the boundary is exclusive —
        exactly 30 days back returns 403. default_days_back=29 stays safely inside.
        """
        from_date = datetime.now(timezone.utc) - timedelta(days=settings.default_days_back)

        try:
            # Fetch history page by page until we find the positionId
            # (API returns up to 100 per page; most recent trades come first)
            for page in range(1, 11):  # max 10 pages = 1000 trades
                raw_trades = self._client.fetch_closed_trades(
                    from_date=from_date, page=page, page_size=100
                )
                if not raw_trades:
                    break

                for raw in raw_trades:
                    if str(raw.get('positionId', '')) == str(trade.trade_id):
                        return self._apply_history_fields(trade, raw)

            logger.warning(
                f"TradeEnricher: positionId={trade.trade_id} not found in "
                f"last {settings.default_days_back} days of history. "
                f"exit_price and profit_loss remain 0.0."
            )

        except Exception as exc:
            logger.error(
                f"TradeEnricher: history lookup failed for "
                f"positionId={trade.trade_id}: {exc}"
            )

        return trade

    def _apply_history_fields(self, trade: Trade, raw: dict) -> Trade:
        """
        Return a new Trade with financial fields populated from a raw history record.
        Uses model_copy(update=...) to produce an updated immutable-style copy.
        """
        updates = {
            'exit_price':  float(raw.get('closeRate', trade.exit_price)),
            'profit_loss': float(raw.get('netProfit', trade.profit_loss)),
            'fees':        float(raw.get('fees', trade.fees)),
            'leverage':    int(raw.get('leverage', trade.leverage)),
            'sl_rate':     raw.get('stopLossRate', trade.sl_rate),
            'tp_rate':     raw.get('takeProfitRate', trade.tp_rate),
        }
        enriched = trade.model_copy(update=updates)
        logger.info(
            f"TradeEnricher: enriched positionId={trade.trade_id} "
            f"exit={updates['exit_price']} pnl={updates['profit_loss']}"
        )
        return enriched

    # ------------------------------------------------------------------
    # Stage 2 — instrument resolution
    # ------------------------------------------------------------------

    def _resolve_instrument(self, trade: Trade) -> Trade:
        """Fill trade.instrument with the symbol string for trade.instrument_id."""
        if trade.instrument is not None:
            return trade  # already resolved

        symbol = self._resolver.symbol(trade.instrument_id)
        return trade.model_copy(update={'instrument': symbol})