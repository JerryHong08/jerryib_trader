import asyncio
import json

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from .polygon_manager import PolygonWebSocketManager

app = FastAPI()
manager = PolygonWebSocketManager(api_key="YOUR_POLYGON_API_KEY")


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(manager.stream_forever())


@app.websocket("/ws/quotes")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    try:
        data = await websocket.receive_json()
        print(f"debug:{data}")
        symbols = data.get("symbols", [])
        if isinstance(symbols, str):
            symbols = [s.strip().upper() for s in symbols.split(",")]

        await manager.subscribe(websocket, symbols)

        # 每个 symbol 一个消费协程
        consumer_tasks = [
            asyncio.create_task(consume_symbol(websocket, sym)) for sym in symbols
        ]

        while True:
            msg = await websocket.receive_text()
            if msg.startswith("unsubscribe:"):
                symbol = msg.replace("unsubscribe:", "").strip().upper()
                await manager.unsubscribe(websocket, symbol)
            else:
                pass

    except WebSocketDisconnect:
        await manager.disconnect(websocket)
        for task in consumer_tasks:
            task.cancel()


async def consume_symbol(websocket: WebSocket, symbol: str):
    """持续消费单个 symbol 的队列并推送给前端"""
    q = manager.queues.get(symbol)
    while True:
        data = await q.get()
        await websocket.send_json(data)
