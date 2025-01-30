from aiokafka import AIOKafkaConsumer
from shared.streaming import consume_messages
from shared.commands import PlaceOrderCommand, OrderConfirmationCommand
from shared.event_store import PostgresEventStore
from shared.events.order_event import OrderEvent
from shared.events.wallet_event import WalletEvent
import asyncpg
import logging
import os
from shared import load_env
import asyncio
import json
from datetime import datetime
import uuid
from fastapi.encoders import jsonable_encoder

logger = logging.getLogger(__name__)

load_env()

class LiquidityEngine:
    def __init__(self):
        self.consumer = AIOKafkaConsumer(
            "commands",
            bootstrap_servers='localhost:9092',
            group_id="liquidity-engine-group"
        )
        self.event_store = None
        logger.info("LiquidityEngine initialized")

    async def start(self):
        try:
            await self.consumer.start()
            logger.info("LiquidityEngine started")
            async for msg in consume_messages(self.consumer):
                try:
                    # Parse the message from JSON
                    msg_dict = json.loads(msg)
                    
                    if msg_dict["command_type"] == "place_order":
                        command = PlaceOrderCommand.parse_obj(msg_dict)
                        logger.info(f"Received {command.command_type} command for {command.symbol}")
                        await self.process_command(command)
                    elif msg_dict["command_type"] == "order_confirmation":
                        command = OrderConfirmationCommand.parse_obj(msg_dict)
                        logger.info(f"Received {command.command_type} command for {command.transaction_id}")
                        await self.handle_order_confirmation(command)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        except Exception as e:
            logger.error(f"Error starting LiquidityEngine: {e}")
        finally:
            await self.consumer.stop()

    async def process_command(self, command: PlaceOrderCommand):
        # Handle order placement by initiating liquidity request
        await self.request_liquidity(command)

    async def request_liquidity(self, command: PlaceOrderCommand):
        """Request liquidity from merchant"""
        try:
            # Create and save liquidity requested event using OrderEvent
            liquidity_event = OrderEvent(
                event_type="order_placed",
                transaction_id=command.transaction_id,
                user_id=command.user_id,
                symbol=command.symbol,
                quantity=command.quantity,
                price=command.price,
                side=command.side,
                remaining_quantity=command.quantity,
                status="open"
            )
            await self.event_store.save_event(command.transaction_id, jsonable_encoder(liquidity_event))
            
            # Send tokens to exchange (blockchain operation)
            await self.send_tokens_to_exchange(command)
            
            # Initiate API call to merchant
            await self.initiate_merchant_swap(command)
            
            logger.info(f"Liquidity requested for transaction {command.transaction_id}. Waiting for confirmation.")
            
        except Exception as e:
            logger.error(f"Error requesting liquidity: {e}")
            # Save failed event
            await self.event_store.save_event(command.transaction_id, {
                "event_id": str(uuid.uuid4()),
                "event_type": "liquidity_failed",
                "transaction_id": command.transaction_id,
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            })

    async def handle_order_confirmation(self, command: OrderConfirmationCommand):
        try:
            # Get the pending request from event store
            events = await self.event_store.list_events_by_transaction_id(command.transaction_id)
            if not events:
                logger.error(f"No events found for transaction {command.transaction_id}")
                return
            
            pending_request = next((e for e in events if e['event_type'] == 'order_placed'), None)
            if not pending_request:
                logger.error(f"No pending order found for transaction {command.transaction_id}")
                return
            
            logger.info(f"Pending request: {pending_request}")
            
            # Create confirmation event with proper event_id
            confirmation_event = {
                "event_id": str(uuid.uuid4()),  # Generate new event_id
                "event_type": "liquidity_confirmed",
                "transaction_id": command.transaction_id,
                "confirmation_id": command.confirmation_id,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            await self.event_store.save_event(command.transaction_id, confirmation_event)
            
            logger.info(f"Transaction {command.transaction_id} successfully confirmed with confirmation ID {command.confirmation_id}")
            
        except Exception as e:
            logger.error(f"Error handling order confirmation: {e}")
            # Save failed event with proper event_id
            failed_event = {
                "event_id": str(uuid.uuid4()),  # Generate new event_id
                "event_type": "liquidity_confirmation_failed",
                "transaction_id": command.transaction_id,
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
            await self.event_store.save_event(command.transaction_id, failed_event)
            raise

    async def send_order_confirmation(self, command: PlaceOrderCommand):
        """No longer needed as we handle this in handle_order_confirmation"""
        pass

    async def send_tokens_to_exchange(self, command: PlaceOrderCommand):
        """Send tokens to exchange (blockchain operation)"""
        # Calculate the total amount based on quantity and price
        total_amount = float(command.quantity) * float(command.price)
        logger.info(f"Sending {total_amount} {command.symbol.split('/')[1]} tokens to exchange")
        
    async def initiate_merchant_swap(self, command: PlaceOrderCommand):
        """Initiate API call to merchant"""
        total_amount = float(command.quantity) * float(command.price)
        logger.info(f"Initiating merchant swap for transaction {command.transaction_id} with {command.quantity} {command.symbol.split('/')[0]} at {command.price} {command.symbol.split('/')[1]} each (total: {total_amount})")
        
    async def receive_tokens_from_merchant(self, command: PlaceOrderCommand):
        """Receive tokens from merchant (blockchain operation)"""
        # TODO: Implement blockchain token receipt
        # This would involve monitoring the blockchain for deposits
        # For now, just log the transaction
        logger.info(f"Receiving tokens from merchant...")
        
    async def confirm_liquidity(self, command: PlaceOrderCommand):
        """Confirm liquidity provision"""
        total_amount = float(command.quantity) * float(command.price)
        confirmation_event = WalletEvent(
            event_type="liquidity_confirmed",
            user_id=command.user_id,
            currency=command.symbol.split('/')[1],  # Use the quote currency
            amount=total_amount,
            transaction_id=command.transaction_id
        )
        
        await self.event_store.save_event(command.user_id, confirmation_event.dict())
        logger.info(f"Liquidity confirmed for transaction {command.transaction_id}")

    async def _init_event_store(self):
        self.event_store = PostgresEventStore(await asyncpg.connect(os.getenv("DB_CONNECT_STRING")))

    async def stop(self):
        await self.consumer.stop() 

async def main():
    engine = LiquidityEngine()
    try:
        await engine._init_event_store()
        await engine.start()
    except Exception as e:
        logger.error(f"Engine failed: {e}")
    finally:
        await engine.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down liquidity engine...") 