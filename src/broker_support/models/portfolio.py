"""
Pydantic models for the demo portfolio response.
  GET /api/v1/trading/info/demo/portfolio  →  { clientPortfolio: { credit, positions, ... } }
  GET /api/v1/trading/info/demo/pnl        →  { clientPortfolio: { credits, positions, ... } }

Field casing: portfolio positions use PascalCase + capital-ID suffix.
This is DIFFERENT from trade history (camelCase + lowercase id). Do not mix.

These models are read-only (used for snapshot comparison in PositionTracker).
"""
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field


class OpenPosition(BaseModel):
    """
    A single open position as returned in clientPortfolio.positions.

    API source: GET /api/v1/trading/info/demo/portfolio
    Schema confirmed: OpenAPI v1.138.0 (2026-03-12)

    CRITICAL: aliases use PascalCase with capital ID suffix (positionID not positionId).
    The API returns positionID, instrumentID — NOT positionId, instrumentId.
    """

    position_id: int = Field(..., alias='positionID')       # ← capital ID
    instrument_id: int = Field(..., alias='instrumentID')   # ← capital ID
    is_buy: bool = Field(..., alias='isBuy')
    open_rate: float = Field(..., alias='openRate')
    open_date_time: datetime = Field(..., alias='openDateTime')
    amount: float = Field(..., alias='amount')
    units: float = Field(default=0.0, alias='units')
    stop_loss_rate: float = Field(default=0.0, alias='stopLossRate')
    take_profit_rate: float = Field(default=0.0, alias='takeProfitRate')
    leverage: int = Field(default=1, alias='leverage')
    order_id: int = Field(default=0, alias='orderID')       # ← capital ID
    mirror_id: int = Field(default=0, alias='mirrorID')     # ← capital ID; 0 = manual trade
    is_no_stop_loss: bool = Field(default=True, alias='isNoStopLoss')       # true = SL DISABLED
    is_no_take_profit: bool = Field(default=True, alias='isNoTakeProfit')   # true = TP DISABLED
    initial_amount_in_dollars: float = Field(default=0.0, alias='initialAmountInDollars')
    is_partially_altered: bool = Field(default=False, alias='isPartiallyAltered')
    settlement_type_id: int = Field(default=0, alias='settlementTypeID')    # 0=CFD, 1=Real, 4=Future

    model_config = {"populate_by_name": True}


class OrderForOpen(BaseModel):
    """Pending market order to open a position (from clientPortfolio.ordersForOpen)."""

    order_id: int = Field(..., alias='orderID')             # ← capital ID
    instrument_id: int = Field(..., alias='instrumentID')   # ← capital ID
    amount: float = Field(..., alias='amount')
    mirror_id: int = Field(default=0, alias='mirrorID')     # 0 = manual; !=0 = copy trading

    model_config = {"populate_by_name": True}


class PendingOrder(BaseModel):
    """Pending MIT/limit order (from clientPortfolio.orders)."""

    order_id: int = Field(..., alias='orderID')             # ← capital ID
    instrument_id: int = Field(..., alias='instrumentID')   # ← capital ID
    amount: float = Field(..., alias='amount')

    model_config = {"populate_by_name": True}


class ClientPortfolio(BaseModel):
    """
    Top-level portfolio container.

    Field name differs by endpoint:
      /demo/portfolio → credit
      /demo/pnl       → credits
    We use /demo/portfolio for tracking, so the correct field is 'credit'.
    """

    credit: float = Field(default=0.0, alias='credit')
    positions: List[OpenPosition] = Field(default_factory=list, alias='positions')
    orders: List[PendingOrder] = Field(default_factory=list, alias='orders')
    orders_for_open: List[OrderForOpen] = Field(default_factory=list, alias='ordersForOpen')
    mirrors: list = Field(default_factory=list, alias='mirrors')

    model_config = {"populate_by_name": True, "extra": "ignore"}

    def available_cash(self) -> float:
        """
        Calculate available cash per official formula:
          available = credit
                      - sum(ordersForOpen[i].amount where mirrorID == 0)
                      - sum(orders[i].amount for all)

        Only manual ordersForOpen (mirrorID == 0) are deducted.
        All pending orders (MIT/limit) are deducted regardless of mirrorID.
        Source: https://api-portal.etoro.com/guides/calculate-available-cash.md
        """
        manual_reserved = sum(
            o.amount for o in self.orders_for_open if o.mirror_id == 0
        )
        orders_reserved = sum(o.amount for o in self.orders)
        return self.credit - manual_reserved - orders_reserved

class PortfolioResponse(BaseModel):
    """Full API response wrapper for /demo/portfolio and /demo/pnl."""

    client_portfolio: ClientPortfolio = Field(..., alias='clientPortfolio')

    model_config = {"populate_by_name": True, "extra": "ignore"}