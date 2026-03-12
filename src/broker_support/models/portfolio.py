"""
Pydantic models for the demo portfolio response.
  GET /api/v1/trading/info/demo/pnl  →  { clientPortfolio: { credit, positions, ... } }

These models are read-only (used for snapshot comparison in PositionTracker).
"""
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field


class OpenPosition(BaseModel):
    """A single open position as returned in clientPortfolio.positions."""

    position_id: int = Field(..., alias='positionId')
    instrument_id: int = Field(..., alias='instrumentId')
    is_buy: bool = Field(..., alias='isBuy')
    open_rate: float = Field(..., alias='openRate')
    open_date_time: datetime = Field(..., alias='openDateTime')
    amount: float = Field(..., alias='amount')
    units: float = Field(default=0.0, alias='units')
    stop_loss_rate: float = Field(default=0.0, alias='stopLossRate')
    take_profit_rate: float = Field(default=0.0, alias='takeProfitRate')

    model_config = {"populate_by_name": True}


class ClientPortfolio(BaseModel):
    """Top-level portfolio container from /trading/info/demo/pnl."""

    credit: float = Field(default=0.0, alias='credit')
    positions: List[OpenPosition] = Field(default_factory=list, alias='positions')
    orders: list = Field(default_factory=list, alias='orders')
    mirrors: list = Field(default_factory=list, alias='mirrors')

    model_config = {"populate_by_name": True}


class PortfolioResponse(BaseModel):
    """Full API response wrapper."""

    client_portfolio: ClientPortfolio = Field(..., alias='clientPortfolio')

    model_config = {"populate_by_name": True}