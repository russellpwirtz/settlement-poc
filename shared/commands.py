from pydantic import BaseModel
from datetime import datetime
from typing import Literal, Optional
import uuid

OrderType = Literal["limit", "market"]
OrderSide = Literal["buy", "sell"]

class BaseCommand(BaseModel):
    command_id: str = str(uuid.uuid4())
    created_at: datetime = datetime.utcnow()
    user_id: str

class PlaceOrderCommand(BaseCommand):
    command_type: Literal["place_order"] = "place_order"
    symbol: str  # e.g., "BTC/USD"
    side: OrderSide
    price: Optional[float]  # Required for limit orders
    quantity: float
    order_type: OrderType
    order_id: str = str(uuid.uuid4())

class OrderConfirmationCommand(BaseCommand):
    command_type: Literal["order_confirmation"] = "order_confirmation"
    order_id: str
    confirmation_id: str = str(uuid.uuid4())

class CancelOrderCommand(BaseCommand):
    command_type: Literal["cancel_order"] = "cancel_order"
    order_id: str
    symbol: str

class DepositFundsCommand(BaseCommand):
    command_type: Literal["deposit_funds"] = "deposit_funds"
    currency: str
    amount: float
    transaction_id: str = str(uuid.uuid4())

class DepositConfirmationCommand(BaseCommand):
    command_type: Literal["deposit_confirmation"] = "deposit_confirmation"
    transaction_id: str

class WithdrawFundsCommand(BaseCommand):
    command_type: Literal["withdraw_funds"] = "withdraw_funds"
    currency: str
    amount: float
    destination_address: str
    transaction_id: str = str(uuid.uuid4())