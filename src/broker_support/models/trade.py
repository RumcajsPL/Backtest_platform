"""
Pydantic model for a closed trade as returned by:
  GET /api/v1/trading/info/trade/history

Field aliases match the eToro API response exactly (camelCase).
populate_by_name=True allows construction by Python field name where needed.
"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator, model_validator


class Trade(BaseModel):
    """Represents a single closed trade from eToro trade history."""

    # Core identifiers — Bug 4 fix: alias was 'id', must be 'positionId'
    trade_id: str = Field(..., alias='positionId')

    @field_validator('trade_id', mode='before')
    @classmethod
    def coerce_to_str(cls, v: object) -> str:
        return str(v)
    instrument_id: int = Field(..., alias='instrumentId')

    # instrument symbol — populated post-lookup via InstrumentResolver (Step 2)
    # Not in API response; set to None at construction, filled later.
    instrument: Optional[str] = Field(default=None)

    # Direction — derived from isBuy in model_validator below
    direction: str = Field(default='')

    # Timestamps
    open_time: datetime = Field(..., alias='openTimestamp')
    close_time: datetime = Field(..., alias='closeTimestamp')

    # Prices
    entry_price: float = Field(..., alias='openRate')
    exit_price: float = Field(..., alias='closeRate')

    # Position sizing
    volume: float = Field(..., alias='investment')
    units: float = Field(default=0.0, alias='units')

    # P&L
    profit_loss: float = Field(..., alias='netProfit')

    # Additional fields confirmed in official API schema
    fees: float = Field(default=0.0, alias='fees')
    leverage: int = Field(default=1, alias='leverage')
    sl_rate: Optional[float] = Field(default=None, alias='stopLossRate')
    tp_rate: Optional[float] = Field(default=None, alias='takeProfitRate')
    trailing_stop_loss: bool = Field(default=False, alias='trailingStopLoss')

    # isBuy is consumed by the validator and not stored directly
    is_buy: Optional[bool] = Field(default=None, alias='isBuy')

    @model_validator(mode='after')
    def derive_direction(self) -> 'Trade':
        """Derive direction string from isBuy flag."""
        if self.is_buy is not None:
            object.__setattr__(self, 'direction', 'BUY' if self.is_buy else 'SELL')
        return self

    model_config = {
        "populate_by_name": True,
    }