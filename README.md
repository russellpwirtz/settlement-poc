# Settlement PoC

A proof of concept implementation for a trading exchange backend settlement system.

## Stack

| Component          | Technology                              |
|--------------------|-----------------------------------------|
| Language           | Python 3.11+ (FastAPI, aiokafka, psycopg2) |
| Event Streaming     | Apache Kafka (with Confluent Kafka for Python) |
| Liquidity Engine         | Blockchain / API                    |
| Databases          | PostgreSQL (event store, user balances) |
| Auth               | AWS Cognito (OAuth 2.0/JWT)            |
| Infrastructure    | Docker (local PoC), AWS ECS/EKS (production) |
| Monitoring         | Prometheus + Grafana (metrics), OpenTelemetry (tracing) |

## Prerequisites

- Docker installed on your system
- docker-compose installed
- uvicorn installed
- Python 3.11 or higher (tested version)

## Setup

First, start the supporting services:

```bash
docker-compose up -d
```

This starts the required services:
- Kafka (Zookeeper and Broker)
- PostgreSQL database

## Services

1. API Server:
```bash
uvicorn api.main:app --reload --port 8000
```

2. Wallet Manager:
```bash
python -m wallet.manager
```

## Example Usage

### Deposit Funds
```bash
curl -X POST "http://localhost:8000/wallet/deposit" \
     -H "Content-Type: application/json" \
     -d '{
           "user_id": "user123",
           "currency": "BTC",
           "amount": 1.5,
           "transaction_id": "769f1e96-da1b-4baa-94df-8495b613e772"
         }'
```

### Confirm Deposit
```bash
curl -X POST "http://localhost:8000/wallet/deposit/confirm" \
     -H "Content-Type: application/json" \
     -d '{
           "user_id": "user123",
           "transaction_id": "769f1e96-da1b-4baa-94df-8495b613e772"
         }'
```

## Data Flow Example: Placing an Order

### User Authentication
- Client sends `POST /order` with JWT token
- API Gateway validates token via Cognito

### Order Placement
```bash
POST /order HTTP/1.1
Content-Type: application/json
Authorization: Bearer <JWT_TOKEN>

{
    "user_id": "user123",
    "symbol": "BTC/USD",
    "side": "BUY",
    "type": "LIMIT",
    "price": 45000,
    "quantity": 0.5
}
```

### Liquidity Matching 
- FastAPI publishes `order_placed` event to Kafka
- Liquidity Engine reads `order_placed` from Kafka
- Executes liquidity swap with merchant
- When completed, publishes `trade_executed` event to Kafka

### Liquidity Engine
- FastAPI publishes `order_placed` event to Kafka
- Liquidity Engine reads `order_placed` from Kafka
- Executes liquidity swap with merchant
- When completed, publishes `trade_executed` event to Kafka

### Ledger Updates
- Event store ingests `order_placed` and `trade_executed` events
- Update snapshots of user balances (currently in-memory snapshots, could be postgres in production)

### User Notification
- Real-time updates via WebSocket (not implemented in this PoC)

## Architecture

The system uses:
- Event sourcing for transaction processing
- Kafka for command processing
- PostgreSQL for event store and user balances

The wallet manager maintains user balances and processes transactions asynchronously.