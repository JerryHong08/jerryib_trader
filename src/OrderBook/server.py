import asyncio
import json
import os

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

from ..DataSupply.polygon_manager import PolygonWebSocketManager
from ..DataSupply.simulator_manager import LocalReplayWebSocketManager
from ..utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True)

load_dotenv()
app = FastAPI()

manager = PolygonWebSocketManager(api_key=os.getenv("POLYGON_API_KEY"))
# manager = LocalReplayWebSocketManager("20251112")


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(manager.stream_forever())


# debug
@app.get("/debug/status")
async def debug_status():
    """check for all conneccted clients status"""
    all_client_symbols = {}
    for client, stream_keys in manager.connections.items():
        client_id = (
            f"client_{id(client)}" if hasattr(client, "__hash__") else str(client)
        )
        all_client_symbols[client_id] = list(stream_keys)

    return {
        "connected": manager.connected,
        "subscribed_streams": list(manager.subscribed_streams),
        "active_connections": len(manager.connections),
        "client_subscriptions": all_client_symbols,
        "queue_lengths": {
            stream: queue.qsize() for stream, queue in manager.queues.items()
        },
    }


@app.get("/debug/latest/{symbol}")
async def get_latest_quote(symbol: str, event: str = "Q"):
    """get symbol latest event data (default Q=quote). Use ?event=T for trades."""
    symbol = symbol.upper()
    stream_key = f"{event}.{symbol}"
    queue = manager.queues.get(stream_key)

    if not queue:
        return {"error": f"Stream {stream_key} not subscribed"}

    if queue.empty():
        return {"message": f"No data available for {stream_key}"}

    # get the latest event data
    try:
        latest_data = None
        while not queue.empty():
            latest_data = await asyncio.wait_for(queue.get(), timeout=0.1)

        return latest_data or {"message": "No data"}
    except asyncio.TimeoutError:
        return {"message": "No recent data"}


@app.get("/debug/subscribe/{symbol}")
@app.post("/debug/subscribe/{symbol}")
async def debug_subscribe(symbol: str, events: str = "Q"):
    """subscribe a symbol. Pass events comma-separated via ?events=Q,T"""
    symbol = symbol.upper()
    evs = [e.strip().upper() for e in events.split(",") if e.strip()]
    await manager.subscribe("debug_client", [symbol], events=evs)
    return {"message": f"Subscribed to {symbol} events={evs}"}


@app.get("/debug/unsubscribe/{symbol}")
@app.post("/debug/unsubscribe/{symbol}")
async def debug_unsubscribe(symbol: str, events: str = "Q"):
    """unsubscribe a symbol. Pass events comma-separated via ?events=Q,T"""
    symbol = symbol.upper()
    evs = [e.strip().upper() for e in events.split(",") if e.strip()]
    await manager.unsubscribe("debug_client", symbol, events=evs)
    return {"message": f"Unsubscribed {symbol} events={evs}"}


@app.get("/debug", response_class=HTMLResponse)
async def debug_page():
    """Debug Page"""
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

            // auto check status
            // checkStatus();
            // setInterval(checkStatus, 5000);
        </script>
    </body>
    </html>
    """


# websocket
@app.websocket("/ws/tickdata")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    consumer_tasks = {}  # store each symbol consume task {symbol: task}
    # now track stream_keys (e.g. Q.AAPL, T.AAPL)
    consumer_tasks = {}
    current_streams = set()

    try:
        while True:
            msg = await websocket.receive_text()

            # support legacy string unsubscribe:unsubscribe:SYMBOL or unsubscribe:Q.SYMBOL
            if isinstance(msg, str) and msg.startswith("unsubscribe:"):
                val = msg.replace("unsubscribe:", "").strip().upper()
                # if stream_key format provided (ev.SYMBOL)
                if "." in val:
                    stream_key = val
                    logger.debug(f"unsubscribe: {stream_key}")
                    ev, sym = stream_key.split(".", 1)
                    await manager.unsubscribe(websocket, sym, events=[ev])
                    if stream_key in current_streams:
                        consumer_tasks[stream_key].cancel()
                        del consumer_tasks[stream_key]
                    current_streams.discard(stream_key)
                else:
                    # unsubscribe all events for this symbol
                    # best-effort: try Q and T
                    await manager.unsubscribe(websocket, val, events=["Q", "T"])
                    # cancel any tasks matching that symbol
                    for sk in list(current_streams):
                        if sk.endswith(f".{val}"):
                            consumer_tasks[sk].cancel()
                            del consumer_tasks[sk]
                            current_streams.discard(sk)

            else:
                try:
                    data = json.loads(msg)

                    # handle unsubscribe action from JSON message
                    if data.get("action") == "unsubscribe":
                        sym = data.get("symbol", "").strip().upper()
                        evs = data.get("events", ["Q"])
                        evs = [e.strip().upper() for e in evs]

                        logger.info(f"📤 Unsubscribe request: {sym} events={evs}")
                        await manager.unsubscribe(websocket, sym, events=evs)

                        # cancel consumer tasks for these stream_keys
                        for ev in evs:
                            stream_key = f"{ev}.{sym}"
                            if stream_key in current_streams:
                                consumer_tasks[stream_key].cancel()
                                del consumer_tasks[stream_key]
                                current_streams.discard(stream_key)
                        continue

                    # new message shape: either
                    # { "subscriptions": [{"symbol":"AAPL","events":["Q","T"]}, ...] }
                    # or { "symbols": ["AAPL","MSFT"], "events": ["Q","T"] }

                    new_streams = set()

                    if "subscriptions" in data:
                        for entry in data["subscriptions"]:
                            sym = entry.get("symbol", "").strip().upper()
                            evs = entry.get("events", ["Q"])
                            evs = [e.strip().upper() for e in evs]
                            # subscribe per symbol+events
                            await manager.subscribe(websocket, [sym], events=evs)
                            for ev in evs:
                                new_streams.add(f"{ev}.{sym}")

                    else:
                        symbols = data.get("symbols", [])
                        events = data.get("events", ["Q"])
                        if isinstance(symbols, str):
                            symbols = [s.strip().upper() for s in symbols.split(",")]
                        symbols = [s.strip().upper() for s in symbols]
                        events = [e.strip().upper() for e in events]

                        if symbols:
                            await manager.subscribe(websocket, symbols, events=events)
                            for sym in symbols:
                                for ev in events:
                                    new_streams.add(f"{ev}.{sym}")

                    # create consumer tasks for new_streams not already present
                    to_add = new_streams - current_streams
                    for sk in to_add:
                        task = asyncio.create_task(consume_stream(websocket, sk))
                        consumer_tasks[sk] = task
                    current_streams.update(to_add)

                except json.JSONDecodeError as e:
                    print(f"⚠️ JSON decode error: {e}")

    except WebSocketDisconnect:
        await manager.disconnect(websocket)
        for task in consumer_tasks.values():
            task.cancel()


async def consume_stream(websocket: WebSocket, stream_key: str):
    """consume a specific stream_key queue and push to frontend"""
    q = manager.queues.get(stream_key)
    if q is None:
        # nothing to consume
        return
    while True:
        data = await q.get()
        await websocket.send_json(data)
