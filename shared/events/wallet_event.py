from pydantic import BaseModel
from datetime import datetime
from typing import Literal, Optional
from uuid import uuid4

WalletEventType = Literal[
    "deposit_initiated",
    "deposit_confirmed",
    "withdrawal_requested",
    "withdrawal_completed",
    "balance_hold",
    "balance_released",
    "order_funds_reserved",
    "order_funds_released",
    "order_funds_transferred",
    "funds_reservation_confirmed",
    "funds_reservation_failed",
    "funds_released",
    "funds_transfer_completed",
    "funds_release_failed"
]

class WalletEvent(BaseModel):
    event_id: str = str(uuid4())
    event_type: WalletEventType
    user_id: str
    currency: str
    amount: float
    timestamp: datetime = datetime.utcnow()
    
    # Context-specific fields
    transaction_id: Optional[str] = None
    reference_order_id: Optional[str] = None
    balance_before: Optional[float] = None
    balance_after: Optional[float] = None
    fee: Optional[float] = None
    network_confirmations: Optional[int] = None
    destination_address: Optional[str] = None
    error_message: Optional[str] = None

    def __init__(self, **data):
        super().__init__(**data)
        self.event_id = str(uuid4())

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

    def to_ledger_entry(self) -> dict:
        return {
            "event_id": self.event_id,
            "user_id": self.user_id,
            "currency": self.currency,
            "amount": self.amount,
            "timestamp": self.timestamp,
            "type": self.event_type
        }