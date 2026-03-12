"""
InstrumentResolver — maps instrumentId ↔ symbol/display name.

Primary source: configs/broker_support/instrument_map.yaml (static, hand-maintained).
Fallback: GET /api/v1/market-data/instruments?instrumentIds=<id> (live API lookup).

Confirmed IDs (2026-03-12):
  32 → GER40  (DAX — "GER40 Index (Non Expiry)")

Usage:
    resolver = InstrumentResolver(Path('configs/broker_support/instrument_map.yaml'))
    resolver.symbol(32)      # → 'GER40'
    resolver.display(32)     # → 'DAX (GER40 Index)'
    resolver.instrument_id('GER40')  # → 32
"""
from pathlib import Path
from typing import Dict, Optional

import yaml
from loguru import logger

from src.broker_support.client.client import EToroClient


class InstrumentResolver:
    """Resolves instrumentId ↔ symbol using a static YAML map with API fallback."""

    def __init__(self, map_path: Path) -> None:
        self._map_path = Path(map_path)
        self._by_id: Dict[int, Dict] = {}      # id → {symbol, display}
        self._by_symbol: Dict[str, int] = {}   # symbol → id
        self._client: Optional[EToroClient] = None
        self._load()

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    def _load(self) -> None:
        """Load instrument_map.yaml into lookup dicts."""
        if not self._map_path.exists():
            logger.warning(f"Instrument map not found at {self._map_path}. "
                           f"Only API fallback will be available.")
            return

        with open(self._map_path, encoding='utf-8') as f:
            data = yaml.safe_load(f)

        instruments = data.get('instruments', {})
        for iid, info in instruments.items():
            iid = int(iid)
            symbol = info.get('symbol', '')
            display = info.get('display', symbol)
            self._by_id[iid] = {'symbol': symbol, 'display': display}
            if symbol:
                self._by_symbol[symbol.upper()] = iid

        logger.info(f"InstrumentResolver loaded {len(self._by_id)} instrument(s) "
                    f"from {self._map_path}.")

    # ------------------------------------------------------------------
    # Lookups
    # ------------------------------------------------------------------

    def symbol(self, instrument_id: int) -> str:
        """
        Return the symbol string for an instrumentId.
        Falls back to API lookup and caches the result.
        Returns 'UNKNOWN_<id>' if not resolvable.
        """
        if instrument_id in self._by_id:
            return self._by_id[instrument_id]['symbol']

        # API fallback
        info = self._fetch_from_api(instrument_id)
        if info:
            return info['symbol']

        logger.warning(f"Cannot resolve instrumentId={instrument_id} — returning placeholder.")
        return f"UNKNOWN_{instrument_id}"

    def display(self, instrument_id: int) -> str:
        """Return the human-readable display name for an instrumentId."""
        if instrument_id in self._by_id:
            return self._by_id[instrument_id]['display']
        info = self._fetch_from_api(instrument_id)
        return info['display'] if info else f"Unknown ({instrument_id})"

    def instrument_id(self, symbol: str) -> Optional[int]:
        """Return the instrumentId for a symbol string, or None if not found."""
        return self._by_symbol.get(symbol.upper())

    # ------------------------------------------------------------------
    # API fallback
    # ------------------------------------------------------------------

    def _fetch_from_api(self, instrument_id: int) -> Optional[Dict]:
        """
        Live lookup via GET /market-data/instruments?instrumentIds=<id>.
        Caches result into the in-memory maps for this session.
        Does NOT persist to instrument_map.yaml — update that file manually.
        """
        if self._client is None:
            self._client = EToroClient()

        try:
            result = self._client._make_request(
                'GET',
                'api/v1/market-data/instruments',
                params={'instrumentIds': str(instrument_id)},
            )
            items = result.get('instrumentDisplayDatas', [])
            if not items:
                return None

            d = items[0]
            symbol = d.get('symbolFull', '')
            display = d.get('instrumentDisplayName', symbol)
            info = {'symbol': symbol, 'display': display}

            # Cache for remainder of session
            self._by_id[instrument_id] = info
            if symbol:
                self._by_symbol[symbol.upper()] = instrument_id
            logger.info(f"API fallback resolved id={instrument_id} → {symbol!r}. "
                        f"Consider adding to instrument_map.yaml.")
            return info

        except Exception as exc:
            logger.error(f"API fallback failed for instrumentId={instrument_id}: {exc}")
            return None