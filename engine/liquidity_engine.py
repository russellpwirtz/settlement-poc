from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from shared.streaming import consume_messages
from shared.commands import PlaceOrderCommand, OrderConfirmationCommand, ReserveFundsCommand, ReleaseFundsCommand
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
        self.producer = AIOKafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.event_store = None
        logger.info("LiquidityEngine initialized")

    async def start(self):
        try:
            logger.info("Starting Kafka consumer and producer...")
            await self.consumer.start()
            await self.producer.start()
            
            logger.info("Connecting to the database...")
            self.event_store = PostgresEventStore(await asyncpg.connect(os.getenv("DB_CONNECT_STRING")))
            
            logger.info("Starting wallet events consumer...")
            self.wallet_consumer = AIOKafkaConsumer(
                "wallet_events",
                bootstrap_servers='localhost:9092',
                group_id="liquidity-engine-wallet-group"
            )
            await self.wallet_consumer.start()
            
            logger.info("LiquidityEngine started")
            
            # Start processing commands and wallet events
            await asyncio.gather(
                self.process_commands(),
                self.process_wallet_events()
            )
            
        except Exception as e:
            logger.error(f"Error starting LiquidityEngine: {e}")
        finally:
            logger.info("Stopping Kafka consumer and producer...")
            await self.producer.stop()
            await self.consumer.stop()
            await self.wallet_consumer.stop()

    async def process_commands(self):
        async for msg in consume_messages(self.consumer):
            try:
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

    async def process_wallet_events(self):
        async for msg in consume_messages(self.wallet_consumer):
            try:
                # The message is already parsed as a dictionary, so we can use it directly
                event_type = msg.get("event_type")
                
                if event_type == "funds_reservation_confirmed":
                    logger.warning(f"Received funds reservation confirmed event for transaction {msg.get('transaction_id')}")
                    transaction_id = msg.get("transaction_id")
                    await self._handle_reservation_confirmation(transaction_id)
                elif event_type == "funds_reservation_failed":
                    logger.warning(f"Received funds reservation failed event for transaction {msg.get('transaction_id')}")
                    transaction_id = msg.get("transaction_id")
                    await self._handle_reservation_failure(transaction_id)
                elif event_type == "funds_release_confirmed":
                    logger.warning(f"Received funds release confirmed event for transaction {msg.get('transaction_id')}")
                    transaction_id = msg.get("transaction_id")
                    await self._handle_release_confirmation(transaction_id)
                
            except Exception as e:
                logger.error(f"Error processing wallet event: {e}")

    async def process_command(self, command: PlaceOrderCommand):
        try:
            logger.info(f"Processing command: {command}")
            base_currency, quote_currency = command.symbol.split('/')
            total_quote_amount = command.quantity * command.price
            
            # Log the calculated amounts
            logger.info(f"Base currency: {base_currency}, Quote currency: {quote_currency}, Total quote amount: {total_quote_amount}")
            
            # Create and save order placed event with order_type
            order_event = OrderEvent(
                event_type="order_placed",
                transaction_id=command.transaction_id,
                user_id=command.user_id,
                symbol=command.symbol,
                side=command.side,
                quantity=command.quantity,
                remaining_quantity=command.quantity,
                price=command.price,
                status="open",
                order_type=command.order_type  # Add order_type to event
            )
            await self.event_store.save_event(command.transaction_id, jsonable_encoder(order_event))
            
            # Send reserve funds command with order side
            reserve_cmd = ReserveFundsCommand(
                user_id=command.user_id,
                base_currency=base_currency,
                quote_currency=quote_currency,
                base_amount=command.quantity if command.side == "sell" else 0,
                quote_amount=total_quote_amount if command.side == "buy" else 0,
                order_transaction_id=command.transaction_id,
                order_side=command.side
            )
            
            logger.info(f"Sending reserve funds command: {reserve_cmd}")
            await self.producer.send("wallet_commands", reserve_cmd.json())
            logger.info("Reserve funds command sent successfully")
            
        except Exception as e:
            logger.error(f"Order processing failed: {e}")
            # Send release funds command
            release_cmd = ReleaseFundsCommand(
                user_id=command.user_id,
                base_currency=base_currency,
                quote_currency=quote_currency,
                base_amount=command.quantity,
                quote_amount=total_quote_amount,
                order_transaction_id=command.transaction_id
            )
            logger.info(f"Sending release funds command: {release_cmd}")
            await self.producer.send("wallet_commands", release_cmd.json())
            logger.info("Release funds command sent successfully")
            raise

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

    async def _handle_reservation_confirmation(self, transaction_id: str):
        """Handle confirmed fund reservations"""
        try:
            # Get the order event from the transaction ID
            events = await self.event_store.list_events_by_transaction_id(transaction_id)
            if not events:
                logger.error(f"No events found for transaction {transaction_id}")
                return
            
            order_event = next((e for e in events if e['event_type'] == 'order_placed'), None)
            if not order_event:
                logger.error(f"No order event found for transaction {transaction_id}")
                return

            # Fix: Use the correct field name and add order_type
            command = PlaceOrderCommand(
                transaction_id=order_event['data']['transaction_id'],
                user_id=order_event['data']['user_id'],
                symbol=order_event['data']['symbol'],
                side=order_event['data']['side'],
                quantity=order_event['data']['quantity'],
                price=order_event['data']['price'],
                order_type=order_event['data'].get('order_type', 'market')  # Use 'market' as default
            )
            
            # Request liquidity from merchant
            await self.request_liquidity(command)

            # Update order status to reflect that liquidity was successfully reserved
            await self.event_store.save_event(
                transaction_id,
                {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "liquidity_reserved",
                    "transaction_id": transaction_id,
                    "status": "reserved",
                    "timestamp": datetime.utcnow().isoformat()
                }
            )

        except Exception as e:
            logger.error(f"Error handling reservation confirmation: {e}")
            raise

    async def _handle_reservation_failure(self, transaction_id: str):
        """Handle failed fund reservations"""
        try:
            # Get the order event from the transaction ID
            events = await self.event_store.list_events_by_transaction_id(transaction_id)
            if not events:
                logger.error(f"No events found for transaction {transaction_id}")
                return
            
            order_event = next((e for e in events if e['event_type'] == 'order_placed'), None)
            if not order_event:
                logger.error(f"No order event found for transaction {transaction_id}")
                return

            # Cancel the order since reservation failed
            await self.event_store.save_event(
                transaction_id,
                {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "order_cancelled",
                    "transaction_id": transaction_id,
                    "status": "cancelled",
                    "error": "Failed to reserve funds",
                    "timestamp": datetime.utcnow().isoformat()
                }
            )

            # Release any reserved funds (if applicable)
            release_cmd = ReleaseFundsCommand(
                user_id=order_event['data']['user_id'],
                base_currency=order_event['data']['symbol'].split('/')[0],
                quote_currency=order_event['data']['symbol'].split('/')[1],
                base_amount=order_event['data']['quantity'],
                quote_amount=float(order_event['data']['quantity']) * float(order_event['data']['price']),
                order_transaction_id=transaction_id
            )
            await self.producer.send("wallet_commands", release_cmd.json())

        except Exception as e:
            logger.error(f"Error handling reservation failure: {e}")
            raise

    async def _handle_release_confirmation(self, transaction_id: str):
        """Handle confirmed fund releases"""
        try:
            # Get the order event from the transaction ID
            events = await self.event_store.list_events_by_transaction_id(transaction_id)
            if not events:
                logger.error(f"No events found for transaction {transaction_id}")
                return
            
            order_event = next((e for e in events if e['event_type'] == 'order_placed'), None)
            if not order_event:
                logger.error(f"No order event found for transaction {transaction_id}")
                return

            # Update order status to reflect that funds were released
            await self.event_store.save_event(
                transaction_id,
                {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "funds_released",
                    "transaction_id": transaction_id,
                    "status": "released",
                    "timestamp": datetime.utcnow().isoformat()
                }
            )

            # Confirm the liquidity provision
            await self.confirm_liquidity(PlaceOrderCommand.parse_obj(order_event['data']))

        except Exception as e:
            logger.error(f"Error handling release confirmation: {e}")
            raise

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