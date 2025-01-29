from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from contextlib import asynccontextmanager
import asyncio
import json
import os

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

async def get_producer():
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        compression_type="gzip"
    )
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()

@asynccontextmanager
async def get_consumer(topic: str, group_id: str):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset='earliest',
        enable_auto_commit=False
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.stop()

async def consume_messages(consumer: AIOKafkaConsumer):
    async for msg in consumer:
        try:
            message = msg.value.decode('utf-8')
            try:
                message = json.loads(message)
                yield message
            except json.JSONDecodeError as e:
                print(f"Error decoding json message: {e}. Message: {message}")
        except Exception as e:
            print(f"Error processing message: {e}")