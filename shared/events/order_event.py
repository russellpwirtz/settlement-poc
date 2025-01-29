from pydantic import BaseModel
from datetime import datetime
from typing import Literal, Optional
from uuid import uuid4
from decimal import Decimal

OrderEventType = Literal[
    "order_placed",
    "order_updated",
    "order_canceled",
    "order_partially_filled",
    "order_fully_filled",
    "order_expired"
]

OrderSide = Literal["buy", "sell"]
OrderStatus = Literal["open", "partial", "filled", "canceled", "expired"]

class OrderEvent(BaseModel):
    event_id: str = str(uuid4())
    event_type: OrderEventType
    order_id: str
    user_id: str
    symbol: str
    side: OrderSide
    quantity: Decimal
    filled_quantity: Decimal = Decimal('0')
    remaining_quantity: Decimal
    price: Optional[Decimal]  # None for market orders
    status: OrderStatus
    timestamp: datetime = datetime.utcnow()
    
    # Execution details
    execution_price: Optional[Decimal] = None
    fee_currency: Optional[str] = None
    fee_amount: Optional[Decimal] = None
    matching_order_id: Optional[str] = None
    exchange_order_id: Optional[str] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            Decimal: lambda v: str(v)
        }

    def to_trade_dict(self) -> dict:
        return {
            "order_id": self.order_id,
            "executed_at": self.timestamp,
            "quantity": self.filled_quantity,
            "price": self.execution_price,
            "side": self.side
        }

    @classmethod
    def create_from_command(cls, command: 'PlaceOrderCommand'):
        return cls(
            event_type="order_placed",
            order_id=command.order_id,
            user_id=command.user_id,
            symbol=command.symbol,
            side=command.side,
            quantity=Decimal(str(command.quantity)),
            remaining_quantity=Decimal(str(command.quantity)),
            price=Decimal(str(command.price)) if command.price else None,
            status="open"
        )