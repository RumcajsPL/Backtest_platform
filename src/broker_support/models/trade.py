"""
Pydantic models for trade data validation.
"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator
import json


class Trade(BaseModel):
    """Represents a single trade in the journal."""
    
    trade_id: str = Field(..., alias='id')
    instrument: str
    direction: str  # 'BUY' or 'SELL'
    open_time: datetime
    close_time: datetime
    entry_price: float
    exit_price: float
    volume: float
    profit_loss: float
    profit_loss_currency: str = 'USD'
    
    @field_validator('direction')
    @classmethod
    def validate_direction(cls, v: str) -> str:
        v = v.upper()
        if v not in ('BUY', 'SELL'):
            raise ValueError(f'Invalid direction: {v}')
        return v
    
    @property
    def profit_pips(self) -> float:
        """Calculate profit/loss in pips."""
        price_diff = self.exit_price - self.entry_price
        if self.direction == 'SELL':
            price_diff = -price_diff
        
        # Determine pip size based on instrument
        if 'JPY' in self.instrument:
            return price_diff * 100
        else:
            return price_diff * 10000
    
    def model_dump(self, **kwargs):
        """Custom dump to handle datetime serialization."""
        data = super().model_dump(**kwargs)
        # Convert datetime objects to ISO format strings
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat()
        return data
    
    model_config = {
        "populate_by_name": True,
    }