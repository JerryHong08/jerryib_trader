import asyncio
import json
import os

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

from ..DataSupply.polygon_manager import PolygonWebSocketManager
from ..DataSupply.simulator_manager import LocalReplayWebSocketManager
from ..DataSupply.thetadata_manager import ThetaDataManager
from ..utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True)

load_dotenv()
app = FastAPI()


# ============================================================================
# Unified Subscription Adapter
# ============================================================================
class SubscriptionAdapter:
    """
    Adapter to translate unified subscription format to manager-specific format.

    Unified Format (from frontend):
    {
        "provider": "polygon" | "theta" | "simulator" (optional, uses MANAGER_TYPE if not provided),
        "subscriptions": [
            {
                "symbol": "AAPL",
                "events": ["QUOTE", "TRADE"],
                "sec_type": "STOCK" (optional, for ThetaData),
                "contract": {} (optional, for options)
            }
        ]
    }
    """

    @staticmethod
    def to_polygon(subscriptions: list) -> tuple:
        """
        Convert to Polygon format.
        Returns: (symbols, events)
        """
        symbols = []
        events = set()

        for sub in subscriptions:
            symbol = sub.get("symbol", "").upper()
            sub_events = sub.get("events", ["QUOTE"])

            if symbol:
                symbols.append(symbol)

            # Map unified event names to Polygon codes
            for ev in sub_events:
                ev = ev.upper()
                if ev in ("QUOTE", "Q"):
                    events.add("Q")
                elif ev in ("TRADE", "T"):
                    events.add("T")

        return symbols, list(events)

    @staticmethod
    def to_theta(subscriptions: list) -> list:
        """
        Convert to ThetaData format.
        Returns: List of subscription dicts
        """
        theta_subs = []

        for sub in subscriptions:
            symbol = sub.get("symbol", "").upper()
            events = sub.get("events", ["QUOTE"])
            sec_type = sub.get("sec_type", "STOCK").upper()
            contract = sub.get("contract", {})

            # Build contract dict
            if not contract:
                contract = {"root": symbol}
            elif "root" not in contract:
                contract["root"] = symbol

            # Map unified event names to ThetaData codes
            req_types = []
            for ev in events:
                ev = ev.upper()
                if ev in ("QUOTE", "Q"):
                    req_types.append("QUOTE")
                elif ev in ("TRADE", "T"):
                    req_types.append("TRADE")

            theta_subs.append(
                {"sec_type": sec_type, "req_types": req_types, "contract": contract}
            )

        return theta_subs

    @staticmethod
    def to_simulator(subscriptions: list) -> list:
        """
        Convert to Simulator format.
        Returns: List of symbols
        """
        symbols = []
        for sub in subscriptions:
            symbol = sub.get("symbol", "").upper()
            if symbol:
                symbols.append(symbol)
        return symbols

    @staticmethod
    def generate_stream_keys(subscriptions: list, manager_type: str) -> set:
        """
        Generate stream keys for tracking based on manager type.
        """
        stream_keys = set()

        for sub in subscriptions:
            symbol = sub.get("symbol", "").upper()
            events = sub.get("events", ["QUOTE"])
            sec_type = sub.get("sec_type", "STOCK").upper()
            contract = sub.get("contract", {})

            for ev in events:
                ev = ev.upper()
                # Normalize event name
                if ev == "QUOTE":
                    ev = "Q"
                elif ev == "TRADE":
                    ev = "T"

                if manager_type == "polygon":
                    stream_keys.add(f"{ev}.{symbol}")

                elif manager_type == "theta":
                    # Generate ThetaData stream key
                    if sec_type in ("STOCK", "INDEX"):
                        identifier = symbol
                    elif sec_type == "OPTION":
                        root = contract.get("root", symbol)
                        exp = contract.get("expiration", "")
                        strike = contract.get("strike", "")
                        right = contract.get("right", "")
                        identifier = f"{root}_{exp}_{strike}_{right}"
                    else:
                        identifier = symbol

                    # Map to ThetaData event names
                    theta_ev = "QUOTE" if ev == "Q" else "TRADE"
                    stream_keys.add(f"{sec_type}.{theta_ev}.{identifier}")

                elif manager_type == "simulator":
                    # Simulator uses simple symbol keys
                    stream_keys.add(f"{ev}.{symbol}")

        return stream_keys


adapter = SubscriptionAdapter()


# ============================================================================
# Manager Configuration - Choose ONE
# ============================================================================
MANAGER_TYPE = os.getenv("DATA_MANAGER", "polygon")  # polygon, theta, simulator

if MANAGER_TYPE == "polygon":
    manager = PolygonWebSocketManager(api_key=os.getenv("POLYGON_API_KEY"))
elif MANAGER_TYPE == "theta":
    manager = ThetaDataManager()
elif MANAGER_TYPE == "simulator":
    replay_date = os.getenv("REPLAY_DATE", "20251112")
    start_time = os.getenv("REPLAY_START_TIME", None)  # Format: "09:30" or "09:30:00"
    manager = LocalReplayWebSocketManager(replay_date, start_time=start_time)
else:
    raise ValueError(f"Unknown MANAGER_TYPE: {MANAGER_TYPE}")

logger.info(f"🚀 Using {MANAGER_TYPE.upper()} data manager")


@app.on_event("startup")
async def startup_event():
    if MANAGER_TYPE != "simulator":
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
    consumer_tasks = {}
    current_streams = set()

    try:
        while True:
            msg = await websocket.receive_text()
            try:
                data = json.loads(msg)

                # ============================================================================
                # UNSUBSCRIBE HANDLING (Unified)
                # ============================================================================
                if data.get("action") == "unsubscribe":
                    subscriptions = data.get("subscriptions", [])

                    # Backward compatibility: single symbol unsubscribe
                    if not subscriptions and "symbol" in data:
                        subscriptions = [
                            {
                                "symbol": data.get("symbol"),
                                "events": data.get("events", ["Q"]),
                                "sec_type": data.get("sec_type", "STOCK"),
                                "contract": data.get("contract", {}),
                            }
                        ]

                    logger.info(f"📤 Unsubscribe request: {subscriptions}")

                    # Unsubscribe from manager
                    if MANAGER_TYPE == "polygon":
                        for sub in subscriptions:
                            sym = sub.get("symbol", "").upper()
                            evs = sub.get("events", ["Q"])
                            evs = [
                                (
                                    e.upper()
                                    if e.upper() in ("Q", "T")
                                    else ("Q" if e.upper() == "QUOTE" else "T")
                                )
                                for e in evs
                            ]
                            await manager.unsubscribe(websocket, sym, events=evs)

                    elif MANAGER_TYPE == "theta":
                        for sub in subscriptions:
                            sec_type = sub.get("sec_type", "STOCK").upper()
                            evs = sub.get("events", ["QUOTE"])
                            req_types = [
                                "QUOTE" if e.upper() in ("Q", "QUOTE") else "TRADE"
                                for e in evs
                            ]
                            contract = sub.get(
                                "contract", {"root": sub.get("symbol", "").upper()}
                            )
                            if "root" not in contract:
                                contract["root"] = sub.get("symbol", "").upper()
                            await manager.unsubscribe(
                                websocket, sec_type, req_types, contract
                            )

                    elif MANAGER_TYPE == "simulator":
                        # Simulator unsubscribe
                        for sub in subscriptions:
                            sym = sub.get("symbol", "").upper()
                            evs = sub.get("events", ["Q"])
                            evs = [
                                (
                                    e.upper()
                                    if e.upper() in ("Q", "T")
                                    else ("Q" if e.upper() == "QUOTE" else "T")
                                )
                                for e in evs
                            ]
                            await manager.unsubscribe(websocket, sym, events=evs)

                    # Cancel consumer tasks for unsubscribed streams
                    unsubscribed_keys = adapter.generate_stream_keys(
                        subscriptions, MANAGER_TYPE
                    )
                    for stream_key in unsubscribed_keys:
                        if stream_key in current_streams:
                            consumer_tasks[stream_key].cancel()
                            del consumer_tasks[stream_key]
                            current_streams.discard(stream_key)

                    continue

                # ============================================================================
                # SUBSCRIBE HANDLING (Unified)
                # ============================================================================
                subscriptions = data.get("subscriptions", [])

                # Backward compatibility: legacy polygon format
                if not subscriptions:
                    symbols = data.get("symbols", [])
                    events = data.get("events", ["Q"])
                    if isinstance(symbols, str):
                        symbols = [s.strip() for s in symbols.split(",")]

                    subscriptions = [
                        {"symbol": sym, "events": events, "sec_type": "STOCK"}
                        for sym in symbols
                        if sym
                    ]

                if not subscriptions:
                    logger.warning("⚠️ No subscriptions found in message")
                    continue

                logger.info(f"📥 Subscribe request: {subscriptions}")

                # Subscribe to manager
                if MANAGER_TYPE == "polygon":
                    symbols, events = adapter.to_polygon(subscriptions)
                    if symbols:
                        await manager.subscribe(websocket, symbols, events=events)

                elif MANAGER_TYPE == "theta":
                    theta_subs = adapter.to_theta(subscriptions)
                    await manager.subscribe(websocket, theta_subs)

                elif MANAGER_TYPE == "simulator":
                    symbols = adapter.to_simulator(subscriptions)
                    if symbols:
                        # Simulator subscribe needs a callback
                        async def callback(symbol: str, payload: dict):
                            q = manager.queues.get(symbol)
                            if q:
                                await q.put(payload)

                        await manager.subscribe(websocket, symbols, callback)

                # Generate stream keys and create consumer tasks
                new_streams = adapter.generate_stream_keys(subscriptions, MANAGER_TYPE)
                to_add = new_streams - current_streams

                for sk in to_add:
                    task = asyncio.create_task(consume_stream(websocket, sk))
                    consumer_tasks[sk] = task
                    logger.debug(f"📡 Created consumer task for {sk}")

                current_streams.update(to_add)

            except json.JSONDecodeError as e:
                logger.error(f"⚠️ JSON decode error: {e}")

    except WebSocketDisconnect:
        logger.info("🔌 WebSocket disconnected")
        await manager.disconnect(websocket)
        for task in consumer_tasks.values():
            task.cancel()


async def consume_stream(websocket: WebSocket, stream_key: str):
    """Consume a specific stream_key queue and push to frontend"""
    q = manager.queues.get(stream_key)
    if q is None:
        logger.warning(f"⚠️ No queue found for stream_key: {stream_key}")
        return

    logger.debug(f"🎧 Consumer started for {stream_key}")

    try:
        while True:
            data = await q.get()

            # Normalize data format for frontend (convert ThetaData/Simulator to Polygon format)
            normalized_data = normalize_data_for_frontend(data, MANAGER_TYPE)

            await websocket.send_json(normalized_data)
    except asyncio.CancelledError:
        logger.debug(f"🛑 Consumer cancelled for {stream_key}")
    except Exception as e:
        logger.error(f"❌ Error in consumer for {stream_key}: {e}")


def normalize_data_for_frontend(data: dict, manager_type: str) -> dict:
    """
    Normalize data from different managers to a unified frontend format (Polygon-like).

    Frontend expects:
    - Quote: { event_type: "Q", symbol, bid, ask, bid_size, ask_size, timestamp }
    - Trade: { event_type: "T", symbol, price, size, timestamp }
    """
    event_type = data.get("event_type", "").upper()

    # Polygon format is already correct
    if manager_type == "polygon":
        return data

    # Convert ThetaData format
    if manager_type == "theta":
        symbol = data.get("symbol", "")
        timestamp = data.get("timestamp")

        if event_type == "QUOTE":
            return {
                "event_type": "Q",
                "symbol": symbol,
                "bid": data.get("bid"),
                "ask": data.get("ask"),
                "bid_size": data.get("bid_size"),
                "ask_size": data.get("ask_size"),
                "timestamp": timestamp,
            }
        elif event_type == "TRADE":
            return {
                "event_type": "T",
                "symbol": symbol,
                "price": data.get("price"),
                "size": data.get("size"),
                "timestamp": timestamp,
            }

    # Convert Simulator format (should already be similar to Polygon)
    if manager_type == "simulator":
        # Simulator uses "Q" and "T" already, but double-check
        if event_type in ("QUOTE", "Q"):
            data["event_type"] = "Q"
        elif event_type in ("TRADE", "T"):
            data["event_type"] = "T"
        return data

    # Fallback: return as-is
    return data
