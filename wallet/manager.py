from shared.events.wallet_event import WalletEvent
from shared.event_store import PostgresEventStore
from shared.commands import DepositFundsCommand, WithdrawFundsCommand, DepositConfirmationCommand, ReserveFundsCommand, ReleaseFundsCommand
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from shared.streaming import consume_messages, get_consumer
import asyncio
import asyncpg
import logging
import json
import logging
from shared.snapshots import BalanceSnapshot
from datetime import datetime
import uuid
import os
from shared import load_env

load_env()

logger = logging.getLogger(__name__)

class WalletManager:
    def __init__(self):
        logger.warning("WalletManager initializing...")
        self.consumer = AIOKafkaConsumer(
            "wallet_commands",
            bootstrap_servers='localhost:9092',
            group_id="wallet-manager-group"
        )
        self.producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
        self.event_store = None
        self.balance_snapshot = BalanceSnapshot()
        self.wallet_command_consumer = AIOKafkaConsumer(
            "wallet_commands",
            bootstrap_servers='localhost:9092',
            group_id="wallet-manager-group"
        )
        logger.warning("WalletManager initialized")

    async def start(self):
        try:
            await self.consumer.start()
            await self.producer.start()
            self.event_store = PostgresEventStore(await asyncpg.connect(os.getenv("DB_CONNECT_STRING")))
            logger.warning("WalletManager started")
            async for msg in consume_messages(self.consumer):
                try:
                    msg_dict = json.loads(msg)
                    await self.process_message(msg_dict)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        except Exception as e:
            logger.error(f"Error starting WalletManager: {e}")
        finally:
            await self.producer.close()
            await self.consumer.close()

    async def process_message(self, msg_dict: dict):
        if msg_dict["command_type"] == "deposit_funds":
            command = DepositFundsCommand.parse_obj(msg_dict)
            logger.debug("Received deposit funds command")
            await self.handle_deposit(command)
            logger.info("Deposit funds command processed successfully")
        elif msg_dict["command_type"] == "withdraw_funds":
            command = WithdrawFundsCommand.parse_obj(msg_dict)
            logger.debug("Received withdraw funds command")
            await self.handle_withdrawal(command)
            logger.info("Withdrawal funds command processed successfully")
        elif msg_dict["command_type"] == "deposit_confirmation":
            command = DepositConfirmationCommand.parse_obj(msg_dict)
            logger.debug("Received deposit confirmation command")
            await self.handle_deposit_confirmation(command)
            logger.info("Deposit confirmation command processed successfully")
        elif msg_dict["command_type"] == "reserve_funds":
            command = ReserveFundsCommand.parse_obj(msg_dict)
            logger.debug("Received reserve funds command")
            await self.handle_reserve_funds(command)
            logger.info("Reserve funds command processed successfully")
        elif msg_dict["command_type"] == "release_funds":
            command = ReleaseFundsCommand.parse_obj(msg_dict)
            logger.debug("Received release funds command")
            await self.handle_release_funds(command)
            logger.info("Release funds command processed successfully")
        else:
            logger.warning(f"Unknown command type received: {msg_dict['command_type']}")

    async def handle_deposit(self, command: DepositFundsCommand) -> None:
        """Process a deposit command and create corresponding events"""
        # Save initial deposit event
        event = WalletEvent(
            event_type="deposit_initiated",
            user_id=command.user_id,
            currency=command.currency,
            amount=command.amount,
            transaction_id=command.transaction_id
        )
        logger.warning("Saving deposit initiated event")
        await self.event_store.save_event(command.user_id, event.dict())
        
        # Update pending deposit in snapshot
        logger.warning(f"Updating pending deposit for user {command.user_id}")
        await self.balance_snapshot.update_pending_deposit(command.user_id, command.currency, command.amount)

    async def handle_withdrawal(self, command: WithdrawFundsCommand):
        # Check available balance before processing withdrawal
        current_balance = await self._get_user_balance(command.user_id, command.currency)
        if current_balance < command.amount:
            raise ValueError("Insufficient funds for withdrawal")
            
        event = WalletEvent(
            event_type="withdrawal_requested",
            user_id=command.user_id,
            currency=command.currency,
            amount=command.amount,
            destination_address=command.destination_address,
            transaction_id=command.transaction_id
        )
        logger.warning(f"Saving withdrawal requested event for user {command.user_id} and currency {command.currency}, event_id: {event.event_id}")
        await self.event_store.save_event(command.user_id, event.dict())
        await self.balance_snapshot.update_balance(command.user_id, command.currency, -command.amount)

    async def handle_deposit_confirmation(self, command: DepositConfirmationCommand) -> None:
        """Process a deposit confirmation command"""
        try:
            # Check if transaction already processed for this event type
            existing_event = await self.event_store.get_event_by_transaction_id_and_type(
                command.transaction_id,
                "deposit_confirmed"
            )
            if existing_event:
                logger.warning(f"Transaction {command.transaction_id} already processed as deposit_confirmed")
                return

            # Get the transaction ID from the command
            transaction_id = command.transaction_id
            
            # Find the matching deposit event
            events = await self.event_store.list_events_by_transaction_id(transaction_id)
            if not events:
                logger.error(f"No events found for transaction {transaction_id}")
                return
            
            deposit_event = next((e for e in events if e['event_type'] == 'deposit_initiated'), None)
            if not deposit_event:
                logger.error(f"No deposit event found for transaction {transaction_id}")
                return

            # Extract relevant data from the deposit event
            user_id = deposit_event['data']['user_id']
            currency = deposit_event['data']['currency']
            amount = deposit_event['data']['amount']

            # Confirm the deposit by creating a confirmation event
            confirmation_event = WalletEvent(
                event_type="deposit_confirmed",
                user_id=user_id,
                currency=currency,
                amount=amount,
                transaction_id=transaction_id,
                balance_before=await self._get_user_balance(user_id, currency),
                balance_after=await self._get_user_balance(user_id, currency) + amount
            )
            
            logger.info(f"Confirming deposit of {amount} {currency} for user {user_id}")
            await self.event_store.save_event(user_id, confirmation_event.dict())
            
            # Now update the balance and clear any pending deposit
            await self._update_user_balance(user_id, currency, amount)
            await self.balance_snapshot.clear_pending_deposit(user_id, currency, amount)
            
            logger.info(f"Successfully confirmed deposit for transaction {transaction_id}")
        except Exception as e:
            logger.error(f"Error confirming deposit for transaction {transaction_id}: {str(e)}")
            raise

    async def _get_user_balance(self, user_id: str, currency: str) -> float:
        return await self.balance_snapshot.get_balance(user_id, currency)

    async def _update_user_balance(self, user_id: str, currency: str, amount: float):
        return await self.balance_snapshot.update_balance(user_id, currency, amount)

    async def rebuild_state(self):
        """Rebuild state from all events in the event store"""
        # events = await self.event_store.get_all_events()
        events = await self.event_store.get_events("user123") # TODO: load all events for all users
        await self.balance_snapshot.rebuild_from_events(events)

    async def _init_event_store(self):
        self.event_store = PostgresEventStore(await asyncpg.connect(os.getenv("DB_CONNECT_STRING")))

    async def reserve_order_funds(self, user_id: str, base_currency: str, quote_currency: str, base_amount: float, quote_amount: float) -> None:
        """Reserve funds for an order"""
        # Check available balance
        current_balance = await self._get_user_balance(user_id, quote_currency)
        if current_balance <= 0 or current_balance < quote_amount:
            logger.warning(f"Insufficient funds for order {user_id} {quote_currency} Amount: {quote_amount} Current Balance: {current_balance}")
            raise ValueError("Insufficient funds for order")
        else:
            logger.warning(f"Sufficient funds for order {user_id} {quote_currency} Amount: {quote_amount} Current Balance: {current_balance}")

        logger.warning(f"Reserving {quote_amount} {quote_currency} for user {user_id}")
        # Create reservation events
        reserve_event = WalletEvent(
            event_type="order_funds_reserved",
            user_id=user_id,
            currency=quote_currency,
            amount=-quote_amount,  # Deduct from available balance
            transaction_id=str(uuid.uuid4())
        )
        
        logger.warning(f"Reserving {base_amount} {base_currency} for user {user_id}")
        pending_event = WalletEvent(
            event_type="order_funds_reserved",
            user_id=user_id,
            currency=base_currency,
            amount=base_amount,  # Add to pending balance
            transaction_id=reserve_event.transaction_id
        )
        
        # Save events
        logger.warning(f"Saving reserve event for user {user_id}")
        await self.event_store.save_event(user_id, reserve_event.dict())
        logger.warning(f"Saving pending event for user {user_id}")
        await self.event_store.save_event(user_id, pending_event.dict())
        
        # Update snapshots
        logger.warning(f"Updating balance for user {user_id}")
        await self.balance_snapshot.update_balance(user_id, quote_currency, -quote_amount)
        logger.warning(f"Updating pending deposit for user {user_id}")
        await self.balance_snapshot.update_pending_deposit(user_id, base_currency, base_amount)

    async def release_order_funds(self, user_id: str, base_currency: str, quote_currency: str, base_amount: float, quote_amount: float) -> None:
        """Release reserved funds if order fails"""
        logger.warning(f"Releasing {quote_amount} {quote_currency} for user {user_id}")
        release_event = WalletEvent(
            event_type="order_funds_released",
            user_id=user_id,
            currency=quote_currency,
            amount=quote_amount,  # Return to available balance
            transaction_id=str(uuid.uuid4())
        )
        
        logger.warning(f"Saving release event for user {user_id}")
        pending_event = WalletEvent(
            event_type="order_funds_released",
            user_id=user_id,
            currency=base_currency,
            amount=-base_amount,  # Remove from pending balance
            transaction_id=release_event.transaction_id
        )
        
        logger.warning(f"Saving pending event for user {user_id}")
        await self.event_store.save_event(user_id, release_event.dict())
        await self.event_store.save_event(user_id, pending_event.dict())
        
        # Update snapshots
        logger.warning(f"Updating balance for user {user_id}")
        await self.balance_snapshot.update_balance(user_id, quote_currency, quote_amount)
        logger.warning(f"Clearing pending deposit for user {user_id}")
        await self.balance_snapshot.clear_pending_deposit(user_id, base_currency, base_amount)

    async def complete_order_transfer(self, user_id: str, base_currency: str, quote_currency: str, base_amount: float, quote_amount: float) -> None:
        """Complete the order transfer after confirmation"""
        logger.warning(f"Completing order transfer for user {user_id}")
        transfer_event = WalletEvent(
            event_type="order_funds_transferred",
            user_id=user_id,
            currency=base_currency,
            amount=base_amount,  # Add to confirmed balance
            transaction_id=str(uuid.uuid4())
        )
        
        logger.warning(f"Saving transfer event for user {user_id}")
        await self.event_store.save_event(user_id, transfer_event.dict())
        
        # Update snapshots
        logger.warning(f"Clearing pending deposit for user {user_id}")
        await self.balance_snapshot.clear_pending_deposit(user_id, base_currency, base_amount)
        logger.warning(f"Updating balance for user {user_id}")
        await self.balance_snapshot.update_balance(user_id, base_currency, base_amount)

    async def handle_reserve_funds(self, command: ReserveFundsCommand):
        """Handle reserve funds command"""
        try:
            logger.warning(f"Reserving funds for user {command.user_id} for {command.order_side} order")
            
            if command.order_side == "buy":
                # For buy orders, we need to reserve quote currency
                await self.reserve_order_funds(
                    user_id=command.user_id,
                    base_currency=command.base_currency,
                    quote_currency=command.quote_currency,
                    base_amount=0,  # Not needed for buy orders
                    quote_amount=command.quote_amount
                )
            elif command.order_side == "sell":
                # For sell orders, we need to reserve base currency
                await self.reserve_order_funds(
                    user_id=command.user_id,
                    base_currency=command.base_currency,
                    quote_currency=command.quote_currency,
                    base_amount=command.base_amount,
                    quote_amount=0  # Not needed for sell orders
                )
            
            # Send confirmation event
            logger.warning(f"Sending confirmation event for user {command.user_id}")
            confirmation_event = WalletEvent(
                event_type="funds_reservation_confirmed",
                user_id=command.user_id,
                currency=command.quote_currency,
                amount=command.quote_amount,
                transaction_id=command.order_transaction_id
            )
            logger.warning(f"Saving confirmation event for user {command.user_id}")
            await self.event_store.save_event(command.user_id, confirmation_event.dict())
            logger.warning(f"Sending confirmation event for user {command.user_id}")
            await self.producer.send("wallet_events", confirmation_event.json().encode())
            
        except Exception as e:
            logger.warning(f"Error reserving funds for user {command.user_id}")
            error_event = WalletEvent(
                event_type="funds_reservation_failed",
                user_id=command.user_id,
                currency=command.quote_currency,
                amount=command.quote_amount,
                transaction_id=command.order_transaction_id,
                error_message=str(e)
            )
            logger.warning(f"Saving error event for user {command.user_id}")
            await self.event_store.save_event(command.user_id, error_event.dict())
            logger.warning(f"Sending error event for user {command.user_id}")
            await self.producer.send("wallet_events", error_event.json().encode())
            raise

    async def handle_release_funds(self, command: ReleaseFundsCommand):
        """Handle release funds command"""
        try:
            logger.warning(f"Releasing funds for user {command.user_id}")
            await self.release_order_funds(
                user_id=command.user_id,
                base_currency=command.base_currency,
                quote_currency=command.quote_currency,
                base_amount=command.base_amount,
                quote_amount=command.quote_amount
            )
            
            # Send confirmation event
            logger.warning(f"Sending confirmation event for user {command.user_id}")
            confirmation_event = WalletEvent(
                event_type="funds_release_confirmed",
                user_id=command.user_id,
                currency=command.quote_currency,
                amount=command.quote_amount,
                transaction_id=command.order_transaction_id
            )
            logger.warning(f"Saving confirmation event for user {command.user_id}")
            await self.event_store.save_event(command.user_id, confirmation_event.dict())
            logger.warning(f"Sending confirmation event for user {command.user_id}")
            await self.producer.send("wallet_events", confirmation_event.json().encode())
            
        except Exception as e:
            logger.warning(f"Error releasing funds for user {command.user_id}")
            error_event = WalletEvent(
                event_type="funds_release_failed",
                user_id=command.user_id,
                currency=command.quote_currency,
                amount=command.quote_amount,
                transaction_id=command.order_transaction_id,
                error_message=str(e)
            )
            logger.warning(f"Saving error event for user {command.user_id}")
            await self.event_store.save_event(command.user_id, error_event.dict())
            logger.warning(f"Sending error event for user {command.user_id}")
            await self.producer.send("wallet_events", error_event.json().encode())
            raise

async def main():
    logger.warning("WalletManager starting...")
    wallet_manager = WalletManager()
    await wallet_manager._init_event_store()
    await wallet_manager.rebuild_state()
    await wallet_manager.start()

asyncio.run(main())