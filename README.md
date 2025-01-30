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

3. Liquidity Engine:
```bash
python -m engine.liquidity_engine
```

## Example Usage

### Deposit Funds
```bash
curl -X POST "http://localhost:8000/wallet/deposit" \
     -H "Content-Type: application/json" \
     -d '{
           "user_id": "user123",
           "currency": "ETH",
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

### Place Order
Place a BUY order for 0.03 BTC at a price of 33.33 ETH per BTC:
```bash
curl -X POST "http://localhost:8000/orders" \
     -H "Content-Type: application/json" \
     -d '{
           "user_id": "user123",
           "symbol": "BTC/ETH",
           "side": "buy",
           "order_type": "limit",
           "price": 33.33,
           "quantity": 0.03
         }'
```


### Confirm Order
Confirm a BUY order for 0.03 BTC at a price of 33.33 ETH per BTC:
```bash
curl -X POST "http://localhost:8000/orders/confirm" \
     -H "Content-Type: application/json" \
     -d '{
           "user_id": "user123",
           "transaction_id": "order123",
           "confirmation_id": "confirmation123"
         }'
```

### Wallet Manager
- **FastAPI Endpoints**:
  - `POST /wallet/deposit`: Accepts `DepositFundsCommand` to initiate deposits
  - `POST /wallet/deposit/confirm`: Accepts `DepositConfirmationCommand` to confirm deposits
  - `POST /wallet/withdraw`: Accepts `WithdrawFundsCommand` to initiate withdrawals
  - `GET /wallet/{user_id}`: Retrieves current balance (currently returns placeholder data)
- **Kafka Integration**:
  - Consumes `deposit_funds`, `deposit_confirmation`, and `withdraw_funds` commands
  - Produces wallet events to event store
- **Event Sourcing**:
  - Maintains wallet state through `deposit_initiated`, `deposit_confirmed`, `withdrawal_requested` events
  - Stores events in PostgreSQL with transaction history
- **Core Operations**:
  - Deposit initiation and confirmation
  - Withdrawal processing
  - Balance snapshot maintenance
  - Transaction verification
- **Error Handling**:
  - Validates sufficient funds for withdrawals
  - Prevents duplicate transaction confirmations
  - Maintains transaction consistency through event sourcing

### Liquidity Engine
- **FastAPI Endpoints**:
  - `POST /orders`: Accepts `PlaceOrderCommand` to initiate new orders
  - `POST /orders/confirm`: Accepts `OrderConfirmationCommand` to confirm order execution
- **Kafka Integration**:
  - Consumes `place_order` and `order_confirmation` commands
  - Produces order events to event store
- **Event Sourcing**:
  - Maintains order state through `order_placed`, `liquidity_confirmed`, and `liquidity_failed` events
  - Stores events in PostgreSQL with transaction history
- **Core Operations**:
  - Token transfers to exchange (blockchain integration)
  - Merchant swap initiation
  - Order confirmation handling
  - Error recovery through event replay
- **Error Handling**:
  - Creates `liquidity_failed` and `liquidity_confirmation_failed` events for error cases
  - Maintains transaction consistency through event sourcing

### Ledger Updates
- Event store ingests events such as: `order_placed`, `liquidity_failed`, `liquidity_confirmed`, `liquidity_confirmation_failed`, `order_confirmed`
- Update snapshots of user balances (currently in-memory snapshots, could be postgres in production)

### User Notification
- Real-time updates via WebSocket (not implemented in this PoC)

## Architecture

The system uses:
- Event sourcing for transaction processing
- Kafka for command processing
- PostgreSQL for event store and user balances

The wallet manager maintains user balances and processes transactions asynchronously.