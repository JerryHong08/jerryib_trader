import asyncio
import json
import os

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

from .polygon_manager import PolygonWebSocketManager

load_dotenv()
app = FastAPI()
manager = PolygonWebSocketManager(api_key=os.getenv("POLYGON_API_KEY"))


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(manager.stream_forever())


# debug
@app.get("/debug/status")
async def debug_status():
    """查看连接状态"""
    # 计算所有客户端订阅的 symbols
    all_client_symbols = {}
    for client, symbols in manager.connections.items():
        client_id = (
            f"client_{id(client)}" if hasattr(client, "__hash__") else str(client)
        )
        all_client_symbols[client_id] = symbols

    return {
        "connected": manager.connected,
        "subscribed_symbols": list(manager.subscribed_symbols),
        "active_connections": len(manager.connections),
        "client_subscriptions": all_client_symbols,
        "queue_lengths": {
            symbol: queue.qsize() for symbol, queue in manager.queues.items()
        },
    }


@app.get("/debug/latest/{symbol}")
async def get_latest_quote(symbol: str):
    """获取某个股票的最新报价"""
    symbol = symbol.upper()
    queue = manager.queues.get(symbol)

    if not queue:
        return {"error": f"Symbol {symbol} not subscribed"}

    if queue.empty():
        return {"message": f"No data available for {symbol}"}

    # 获取队列中最新的数据（非阻塞）
    try:
        latest_data = None
        while not queue.empty():
            latest_data = await asyncio.wait_for(queue.get(), timeout=0.1)

        return latest_data or {"message": "No data"}
    except asyncio.TimeoutError:
        return {"message": "No recent data"}


@app.get("/debug/subscribe/{symbol}")
@app.post("/debug/subscribe/{symbol}")
async def debug_subscribe(symbol: str):
    """手动订阅某个股票"""
    symbol = symbol.upper()
    await manager.subscribe("debug_client", [symbol])
    return {"message": f"Subscribed to {symbol}"}


@app.get("/debug/unsubscribe/{symbol}")
@app.post("/debug/unsubscribe/{symbol}")
async def debug_unsubscribe(symbol: str):
    """手动取消订阅某个股票"""
    symbol = symbol.upper()
    await manager.unsubscribe("debug_client", symbol)
    return {"message": f"Unsubscribed to {symbol}"}


@app.get("/debug", response_class=HTMLResponse)
async def debug_page():
    """调试页面"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Polygon WebSocket Debug</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .container { max-width: 800px; }
            .section { margin: 20px 0; padding: 15px; border: 1px solid #ddd; }
            button { margin: 5px; padding: 8px 16px; cursor: pointer; }
            #output { background: #f5f5f5; padding: 10px; height: 200px; overflow-y: auto; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🔍 Polygon WebSocket Debug</h1>

            <div class="section">
                <h3>📊 Status</h3>
                <button onclick="checkStatus()">Check Status</button>
                <div id="status"></div>
            </div>

            <div class="section">
                <h3>📡 Subscribe</h3>
                <input type="text" id="symbolInput" placeholder="Enter symbol (e.g., AAPL)" />
                <button onclick="subscribe()">Subscribe</button>
            </div>

            <div class="section">
                <h3>📡 Unsubscribe</h3>
                <input type="text" id="unsymbolInput" placeholder="Enter symbol (e.g., AAPL)" />
                <button onclick="unsubscribe()">Unsubscribe</button>
            </div>

            <div class="section">
                <h3>📈 Latest Quote</h3>
                <input type="text" id="quoteSymbol" placeholder="Symbol" />
                <button onclick="getLatestQuote()">Get Quote</button>
            </div>

            <div class="section">
                <h3>📋 Output</h3>
                <div id="output"></div>
                <button onclick="clearOutput()">Clear</button>
            </div>
        </div>

        <script>
            function log(message) {
                const output = document.getElementById('output');
                output.innerHTML += '<div>' + new Date().toLocaleTimeString() + ': ' + message + '</div>';
                output.scrollTop = output.scrollHeight;
            }

            async function checkStatus() {
                try {
                    const response = await fetch('/debug/status');
                    const data = await response.json();
                    document.getElementById('status').innerHTML = '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
                    log('✅ Status checked');
                } catch (error) {
                    log('❌ Error checking status: ' + error);
                }
            }

            async function subscribe() {
                const symbol = document.getElementById('symbolInput').value.toUpperCase();
                if (!symbol) {
                    log('❌ Please enter a symbol');
                    return;
                }

                try {
                    const response = await fetch(`/debug/subscribe/${symbol}`, { method: 'POST' });
                    const data = await response.json();
                    log(`📡 ${data.message}`);
                    document.getElementById('symbolInput').value = '';
                } catch (error) {
                    log('❌ Error subscribing: ' + error);
                }
            }

            async function unsubscribe() {
                const symbol = document.getElementById('unsymbolInput').value.toUpperCase();
                if (!symbol) {
                    log('❌ Please enter a symbol');
                    return;
                }

                try {
                    const response = await fetch(`/debug/unsubscribe/${symbol}`, { method: 'POST' });
                    const data = await response.json();
                    log(`📡 ${data.message}`);
                    document.getElementById('unsymbolInput').value = '';
                } catch (error) {
                    log('❌ Error unsubscribing: ' + error);
                }
            }

            async function getLatestQuote() {
                const symbol = document.getElementById('quoteSymbol').value.toUpperCase();
                if (!symbol) {
                    log('❌ Please enter a symbol');
                    return;
                }

                try {
                    const response = await fetch(`/debug/latest/${symbol}`);
                    const data = await response.json();
                    if (data.error || data.message) {
                        log(`ℹ️ ${data.error || data.message}`);
                    } else {
                        log(`📈 ${symbol}: Bid $${data.bid} x ${data.bid_size} | Ask $${data.ask} x ${data.ask_size}`);
                    }
                } catch (error) {
                    log('❌ Error getting quote: ' + error);
                }
            }

            function clearOutput() {
                document.getElementById('output').innerHTML = '';
            }

            // 自动检查状态
            // checkStatus();
            // setInterval(checkStatus, 5000);
        </script>
    </body>
    </html>
    """


# websocket
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
        # print(data)
        await websocket.send_json(data)
