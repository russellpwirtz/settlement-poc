from aiokafka import AIOKafkaConsumer
from shared.streaming import consume_messages
from shared.commands import PlaceOrderCommand, OrderConfirmationCommand
from shared.event_store import PostgresEventStore
from shared.events.order_event import OrderEvent
import asyncio
import asyncpg
import logging
import os
from shared import load_env

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
        self.pending_swaps = {} 
        logger.info("LiquidityEngine initialized")

    async def start(self):
        try:
            await self.consumer.start()
            logger.info("LiquidityEngine started")
            async for msg in consume_messages(self.consumer):
                try:
                    if msg["command_type"] == "place_order":
                        command = PlaceOrderCommand.parse_obj(msg)
                        logger.info(f"Received {command.command_type} command for {command.symbol}")
                        await self.process_command(command)
                    elif msg["command_type"] == "order_confirmation":
                        command = OrderConfirmationCommand.parse_obj(msg)
                        logger.info(f"Received {command.command_type} command for {command.order_id}")
                        await self.handle_order_confirmation(command)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        except Exception as e:
            logger.error(f"Error starting LiquidityEngine: {e}")

    async def process_command(self, command: PlaceOrderCommand):
        # Handle order placement by initiating liquidity request
        await self.request_liquidity(command)

    async def request_liquidity(self, command: PlaceOrderCommand):
        """Request liquidity from merchant"""
        try:
            # Send tokens to exchange (blockchain operation)
            await self.send_tokens_to_exchange(command)
            # Initiate API call to merchant
            await self.initiate_merchant_swap(command)
            # Add command to pending swaps
            logger.info(f"Liquidity requested for order {command.order_id}. Waiting for confirmation.")
            self.pending_swaps[command.order_id] = {
                'command': command,
                'status': 'pending'
            }
        
        except Exception as e:
            logger.error(f"Error requesting liquidity: {e}")

    async def handle_order_confirmation(self, command: OrderConfirmationCommand):
        """Handle order confirmation from API"""
        try:
            if command.order_id not in self.pending_swaps:
                raise ValueError(f"No pending swap found for order {command.order_id}")
                
            swap = self.pending_swaps[command.order_id]
            original_command = swap['command']
            
            # Step 3: Merchant sends tokens back to exchange
            await self.receive_tokens_from_merchant(original_command)
            
            # Step 4: Confirm the liquidity provision
            await self.confirm_liquidity(original_command)
            
            # Step 5: Send order confirmation
            await self.send_order_confirmation(original_command)
            
            # Clean up pending swap
            del self.pending_swaps[command.order_id]
            
        except Exception as e:
            logger.error(f"Error handling order confirmation: {e}")
            raise

    async def send_order_confirmation(self, command: PlaceOrderCommand):
        """Send order confirmation after successful liquidity swap"""
        confirmation_event = OrderEvent(
            event_type="order_confirmed",
            order_id=command.order_id,
            symbol=command.symbol,
            side=command.side,
            quantity=command.quantity,
            price=command.price
        )
        
        await self.event_store.save_event(command.user_id, confirmation_event.dict())
        logger.info(f"Order {command.order_id} confirmed")

    async def send_tokens_to_exchange(self, command: PlaceOrderCommand):
        """Send tokens to exchange (blockchain operation)"""
        # TODO: Implement blockchain token transfer
        # This would involve interacting with the blockchain
        # For now, just log the transaction
        logger.info(f"Sending {command.amount} {command.currency} tokens to exchange")
        
    async def initiate_merchant_swap(self, command: PlaceOrderCommand):
        """Initiate API call to merchant"""
        # TODO: Implement API call to merchant
        # This would involve sending the command to the merchant API
        # For now, just log the transaction
        logger.info(f"Initiating merchant swap for order {command.order_id}")
        
    async def receive_tokens_from_merchant(self, command: PlaceOrderCommand):
        """Receive tokens from merchant (blockchain operation)"""
        # TODO: Implement blockchain token receipt
        # This would involve monitoring the blockchain for deposits
        # For now, just log the transaction
        logger.info(f"Receiving tokens from merchant...")
        
    async def confirm_liquidity(self, command: PlaceOrderCommand):
        """Confirm liquidity provision"""
        confirmation_event = WalletEvent(
            event_type="liquidity_confirmed",
            user_id=command.user_id,
            currency=command.currency,
            amount=command.amount,
            transaction_id=command.transaction_id
        )
        
        await self.event_store.save_event(command.user_id, confirmation_event.dict())
        logger.info(f"Liquidity confirmed for transaction {command.transaction_id}")

    async def _init_event_store(self):
        self.event_store = PostgresEventStore(await asyncpg.connect(os.getenv("DB_CONNECT_STRING")))

    async def stop(self):
        await self.consumer.stop() 