from typing import Dict, Optional
import asyncio
import json
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

class BalanceSnapshot:
    def __init__(self):
        self._confirmed_balances: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self._pending_balances: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self._lock = asyncio.Lock()
        
    async def get_balance(self, user_id: str, currency: str) -> float:
        """Get the combined balance (confirmed + pending)"""
        return self._confirmed_balances[user_id][currency] + self._pending_balances[user_id][currency]
            
    async def get_confirmed_balance(self, user_id: str, currency: str) -> float:
        """Get only the confirmed balance"""
        return self._confirmed_balances[user_id][currency]
            
    async def get_pending_balance(self, user_id: str, currency: str) -> float:
        """Get only the pending balance"""
        return self._pending_balances[user_id][currency]

    async def update_balance(self, user_id: str, currency: str, amount: float) -> float:
        async with self._lock:
            self._confirmed_balances[user_id][currency] += amount
            print(f"Updating confirmed balance for user {user_id} and currency {currency} with amount {amount}. Balance: {self._confirmed_balances[user_id][currency]}")
            return self._confirmed_balances[user_id][currency]
            
    async def update_pending_deposit(self, user_id: str, currency: str, amount: float) -> None:
        """Update the pending deposit for a user"""
        async with self._lock:
            print(f"Updating pending deposit for user {user_id} and currency {currency} with amount {amount}")
            self._pending_balances[user_id][currency] += amount
            print(f"Pending deposits: {dict(self._pending_balances)}")

    async def clear_pending_deposit(self, user_id: str, currency: str, amount: float) -> None:
        """Clear the pending deposit for a user"""
        async with self._lock:
            if self._pending_balances[user_id][currency] < amount:
                raise ValueError(f"Pending deposit underflow: Tried to clear {amount} but only {self._pending_balances[user_id][currency]} available")
            self._pending_balances[user_id][currency] -= amount
            if self._pending_balances[user_id][currency] == 0:
                del self._pending_balances[user_id][currency]
            print(f"Pending deposits after clearing: {dict(self._pending_balances)}")

    async def get_pending_deposit(self, user_id: str, currency: str) -> float:
        """Get the pending deposit for a user"""
        print(f"Getting pending deposit for user {user_id} and currency {currency}")
        return self._pending_balances[user_id][currency]

    async def rebuild_from_events(self, events: list):
        """Rebuild balances from a list of wallet events"""
        async with self._lock:
            logger.info(f"Rebuilding balances from events")
            self._confirmed_balances.clear()
            self._pending_balances.clear()
            for event in [this_event['data'] for this_event in events]:
                if event == {}:
                    logger.info(f"Skipping empty event")
                    continue
                logger.debug(f"Processing event of type {event['event_type']}")
                if event['event_type'] in ('deposit_confirmed', 'withdrawal_completed'):
                    user_id = event['user_id']
                    currency = event['currency']
                    amount = event['amount']
                    if event['event_type'] == 'withdrawal_completed':
                        amount = -amount
                    # Add to confirmed balance
                    self._confirmed_balances[user_id][currency] += amount
                    logger.debug(f"Updated confirmed balance for {user_id}: {amount} {currency}")
                    
                    # Remove from pending if this is a confirmation
                    if event['event_type'] == 'deposit_confirmed':
                        if self._pending_balances[user_id][currency] >= amount:
                            self._pending_balances[user_id][currency] -= amount
                            if self._pending_balances[user_id][currency] <= 0:
                                del self._pending_balances[user_id][currency]
                            logger.debug(f"Cleared pending deposit for {user_id} after confirmation")
                        else:
                            logger.warning(f"Pending deposit underflow for {user_id}: {currency}")
                elif event['event_type'] == 'deposit_initiated':
                    user_id = event['user_id']
                    currency = event['currency']
                    amount = event['amount']
                    self._pending_balances[user_id][currency] += amount
                    logger.debug(f"Added pending deposit for {user_id}: {amount} {currency}")
            
            # Log final balances with both confirmed and pending amounts
            all_users = set(self._confirmed_balances.keys()).union(self._pending_balances.keys())
            for user_id in all_users:
                confirmed = self._confirmed_balances.get(user_id, {})
                pending = self._pending_balances.get(user_id, {})
                combined = {}
                for currency in set(confirmed.keys()).union(pending.keys()):
                    confirmed_balance = confirmed.get(currency, 0.0)
                    pending_balance = pending.get(currency, 0.0)
                    
                    if confirmed_balance != 0 or pending_balance != 0:
                        combined[currency] = {
                            "confirmed": float(confirmed_balance),
                            "pending": float(pending_balance)
                        }
                if combined:
                    logger.info(f"Final balance for user {user_id}: {json.dumps(combined)}")
                logger.info("Completed rebuilding balances from events")

    async def get_all_balances(self, user_id: str) -> Dict[str, float]:
        """Return both confirmed and pending balances"""
        return {
            'confirmed': dict(self._confirmed_balances[user_id]),
            'pending': dict(self._pending_balances[user_id])
        } 
