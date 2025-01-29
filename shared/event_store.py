from abc import ABC, abstractmethod
from typing import List, Dict, Any
import uuid
import logging
import json
import asyncpg
import asyncio
from datetime import datetime
import os

logger = logging.getLogger(__name__)

class EventStore(ABC):
    @abstractmethod
    async def save_event(self, aggregate_id: str, event: Dict[str, Any]):
        pass
    
    @abstractmethod
    async def get_events(self, aggregate_id: str) -> List[Dict[str, Any]]:
        pass

class PostgresEventStore(EventStore):
    
    def __init__(self, connection):
        self.conn = connection
        if self.conn is None or self.conn.is_closed():
            logger.error("Failed to connect to database")
            raise Exception("Failed to connect to database")
    
    async def _create_tables(self):
        try:
            async with self.conn.transaction():
                sql_query = """
                    CREATE TABLE IF NOT EXISTS events (
                        event_id UUID PRIMARY KEY,
                        aggregate_id VARCHAR(255) NOT NULL,
                        event_type VARCHAR(255) NOT NULL,
                        data JSONB NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    CREATE INDEX IF NOT EXISTS idx_aggregate_id ON events (aggregate_id);
                    CREATE INDEX IF NOT EXISTS idx_event_type ON events (event_type);
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_transaction_id_type 
                    ON events ((data ->> 'transaction_id'), event_type);
                """
                logger.debug(f"Executing SQL query: {sql_query}")
                await self.conn.execute(sql_query)
        except Exception as e:
            logger.error(f"Error creating tables: {e}")

    @classmethod
    async def create(cls, connection):
        instance = cls(connection)
        await instance._create_tables()
        return instance

    async def _ensure_connection(self):
        if not self.conn or self.conn.is_closed():
            logger.info("Attempting to reconnect to database...")
            for attempt in range(3):  # Try 3 times with 1 second delay
                try:
                    self.conn = await asyncpg.connect(os.getenv("DB_CONNECT_STRING"))
                    if self.conn.is_closed():
                        raise Exception("Connection failed after reconnect")
                    logger.info("Database reconnection successful")
                    return
                except Exception as e:
                    logger.error(f"Reconnection attempt {attempt + 1} failed: {e}")
                    await asyncio.sleep(1)
            logger.error("Exhausted all reconnection attempts")
            raise

    async def save_event(self, aggregate_id: str, event: Dict[str, Any]):
        try:
            required_fields = {'event_id', 'event_type'}
            if not required_fields.issubset(event.keys()):
                missing = required_fields - event.keys()
                raise ValueError(f"Event missing required fields: {missing}")
            
            # If no event_id is provided, generate a new one
            if 'event_id' not in event:
                event['event_id'] = str(uuid.uuid4())
            else:
                try:
                    uuid.UUID(event['event_id'])
                except ValueError:
                    raise ValueError("Invalid UUID format for event_id")
            
            await self._ensure_connection()
            
            try:
                def _serialize_datetime(o):
                    if isinstance(o, datetime):
                        return o.isoformat()
                    return o
                
                event_data = json.dumps(event, default=_serialize_datetime)
                
                sql_query = """
                    INSERT INTO events (event_id, aggregate_id, event_type, data)
                    VALUES ($1, $2, $3, $4::jsonb)
                """
                logger.debug(f"Executing SQL query: {sql_query} with params: {event['event_id'], aggregate_id, event['event_type'], event_data}")
                await self.conn.execute(sql_query,
                    event['event_id'],
                    aggregate_id,
                    event['event_type'],
                    event_data
                )
                logger.info(f"Event saved successfully: {event['event_id']}, {aggregate_id}, {event['event_type']}")
            except asyncpg.PostgresError as e:
                if "duplicate key value violates unique constraint" in str(e):
                    logger.warning(f"Existing event found for aggregate_id: {aggregate_id} eventType: {event['event_type']}, skipping save")
                    return
                else:
                    logger.error(f"Database operation failed: {e}")
                    raise
        except Exception as e:
            logger.error(f"Error saving event: {e}")
            raise

    async def get_events(self, aggregate_id: str) -> List[Dict[str, Any]]:
        try:
            sql_query = """
                SELECT event_id, event_type, data, created_at
                FROM events
                WHERE aggregate_id = $1
                ORDER BY created_at ASC
            """
            logger.debug(f"Executing SQL query: {sql_query} with params: {aggregate_id}")
            result = await self.conn.fetch(sql_query, aggregate_id)
            
            events = []
            for row in result:
                event = {
                    "event_id": row[0],
                    "event_type": row[1],
                    "data": json.loads(row[2]), 
                    "created_at": row[3]
                }
                events.append(event)
            return events
        except Exception as e:
            logger.error(f"Failed to get events: {e}")
            raise

    async def get_event_stream(self, aggregate_id: str):
        """Generator for streaming events as they arrive"""
        async with self.conn.transaction() as tx:
            async with tx.cursor() as cursor:
                cursor.itersize = 100  # Batch size for server-side cursor
                sql_query = """
                    SELECT event_id, event_type, data, created_at
                    FROM events
                    WHERE aggregate_id = %s
                    ORDER BY created_at ASC
                """
                logger.debug(f"Executing SQL query: {sql_query} with params: {aggregate_id}")
                await cursor.execute(sql_query, (aggregate_id,))
                
                while True:
                    rows = await cursor.fetchmany(100)
                    if not rows:
                        break
                    for row in rows:
                        yield {
                            "event_id": row[0],
                            "event_type": row[1],
                            "data": row[2],
                            "created_at": row[3]
                        }

    async def list_events_by_transaction_id(self, transaction_id: str) -> List[Dict[str, Any]]:
        query = """
            SELECT event_id, aggregate_id, event_type, data, created_at 
            FROM events 
            WHERE data @> json_build_object('transaction_id', $1::text)::jsonb
        """
        result = await self.conn.fetch(query, transaction_id)
        if result:
            return [
                {
                    "event_id": row[0],
                    "aggregate_id": row[1],
                    "event_type": row[2],
                    "data": json.loads(row[3]),
                    "created_at": row[4]
                }
                for row in result
            ]
        return []

    async def get_event_by_transaction_id(self, transaction_id: str) -> Dict[str, Any]:
        """Get a single event by transaction_id"""
        query = """
            SELECT event_id, aggregate_id, event_type, data, created_at 
            FROM events 
            WHERE data @> json_build_object('transaction_id', $1::text)::jsonb
            LIMIT 1
        """
        result = await self.conn.fetch(query, transaction_id)
        if result:
            row = result[0]
            return {
                "event_id": row[0],
                "aggregate_id": row[1],
                "event_type": row[2],
                "data": json.loads(row[3]),
                "created_at": row[4]
            }
        return None

    async def get_event_by_transaction_id_and_type(self, transaction_id: str, event_type: str) -> Dict[str, Any]:
        """Get a single event by transaction_id and event_type"""
        query = """
            SELECT event_id, aggregate_id, event_type, data, created_at 
            FROM events 
            WHERE data @> json_build_object('transaction_id', $1::text)::jsonb
            AND event_type = $2
            LIMIT 1
        """
        result = await self.conn.fetch(query, transaction_id, event_type)
        if result:
            row = result[0]
            return {
                "event_id": row[0],
                "aggregate_id": row[1],
                "event_type": row[2],
                "data": json.loads(row[3]),
                "created_at": row[4]
            }
        return None