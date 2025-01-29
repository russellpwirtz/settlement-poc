from fastapi import FastAPI, Depends, HTTPException
from shared.streaming import get_producer
from shared.commands import PlaceOrderCommand, DepositFundsCommand, WithdrawFundsCommand, DepositConfirmationCommand, OrderConfirmationCommand
from aiokafka import AIOKafkaProducer
from shared import load_env

load_env()

app = FastAPI()

@app.post("/orders")
async def place_order(command: PlaceOrderCommand, producer: AIOKafkaProducer = Depends(get_producer)):
    await producer.send("commands", command.json())
    return {"status": "Command accepted"}

@app.post("/orders/confirm")
async def place_order(command: OrderConfirmationCommand, producer: AIOKafkaProducer = Depends(get_producer)):
    await producer.send("commands", command.json())
    return {"status": "Command accepted"}

@app.get("/wallet/{user_id}")
async def get_balance(user_id: str):
    # Will implement with Redis caching
    return {"user_id": user_id, "balance": 0}

@app.post("/wallet/deposit")
async def deposit_funds(command: DepositFundsCommand, producer: AIOKafkaProducer = Depends(get_producer)):
    try:
        await producer.send("commands", command.json())
        return {"status": "Deposit command accepted"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/wallet/withdraw")
async def withdraw_funds(command: WithdrawFundsCommand, producer: AIOKafkaProducer = Depends(get_producer)):
    try:
        await producer.send("commands", command.json())
        return {"status": "Withdrawal command accepted"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/wallet/deposit/confirm")
async def confirm_deposit(command: DepositConfirmationCommand, producer: AIOKafkaProducer = Depends(get_producer)):
    try:
        await producer.send("commands", command.json())
        return {"status": "Deposit confirmation accepted"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))