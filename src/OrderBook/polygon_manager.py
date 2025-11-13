import asyncio
import json
import logging

import websockets

logger = logging.getLogger(__name__)


class PolygonWebSocketManager:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.ws = None
        self.connected = False
        self.queues = {}  # dict for storing each symbol quuote data
        self.connections = {}  # { websocket_client: [symbols] }
        self.subscribed_symbols = set()

    async def connect(self):
        try:
            url = "wss://socket.polygon.io/stocks"
            self.ws = await websockets.connect(url)

            # send authentication request
            await self.ws.send(json.dumps({"action": "auth", "params": self.api_key}))

            print("✅ Authenticated to Polygon")
            self.connected = True
            logger.info("🔐 Polygon WebSocket connected & authenticated")

        except Exception as e:
            logger.error(f"❌ Failed to connect: {e}")
            self.connected = False
            self.ws = None
            raise

    async def subscribe(self, websocket_client, symbols):
        """subscribe new symbols"""
        if not self.connected:
            await self.connect()

        print(f"Debug: Subscribing to symbols: {symbols}")

        # update client symbol list
        if websocket_client not in self.connections:
            self.connections[websocket_client] = []

        # merge new symbols to client list
        existing_symbols = set(self.connections[websocket_client])
        new_symbols = set(symbols)
        self.connections[websocket_client] = list(existing_symbols | new_symbols)

        for sym in symbols:
            if sym not in self.queues:
                self.queues[sym] = asyncio.Queue()

            # incrementally subscribe
            if sym not in self.subscribed_symbols:
                try:
                    await self.ws.send(
                        json.dumps({"action": "subscribe", "params": f"Q.{sym}"})
                    )
                    self.subscribed_symbols.add(sym)
                    logger.info(f"📡 Subscribed to Polygon: {sym}")
                    print(f"📡 Successfully subscribed to {sym}")
                except Exception as e:
                    logger.error(f"❌ Failed to subscribe to {sym}: {e}")
                    self.connected = False
                    self.ws = None
            else:
                print(f"ℹ️ {sym} already subscribed to Polygon")

    async def unsubscribe(self, websocket_client, symbol):
        """unsubscribe symbol"""
        if websocket_client in self.connections:
            if symbol in self.connections[websocket_client]:
                self.connections[websocket_client].remove(symbol)

        # check if there is other client subscribing this symbol
        still_needed = any(symbol in syms for syms in self.connections.values())

        if not still_needed and self.connected and symbol in self.subscribed_symbols:
            try:
                await self.ws.send(
                    json.dumps({"action": "unsubscribe", "params": f"Q.{symbol}"})
                )
                self.subscribed_symbols.discard(symbol)
                self.queues.pop(symbol, None)
                logger.info(f"❌ Unsubscribed from Polygon: {symbol}")
                print(f"❌ Unsubscribed from {symbol}")
            except Exception as e:
                logger.error(f"❌ Failed to unsubscribe from {symbol}: {e}")
                self.connected = False
                self.ws = None
        else:
            print(f"ℹ️ {symbol} still needed by other clients")

    async def disconnect(self, websocket_client):
        """client disconnect"""
        client_symbols = self.connections.pop(websocket_client, [])

        for symbol in client_symbols:
            still_needed = any(symbol in syms for syms in self.connections.values())

            if (
                not still_needed
                and self.connected
                and symbol in self.subscribed_symbols
            ):
                try:
                    await self.ws.send(
                        json.dumps({"action": "unsubscribe", "params": f"Q.{symbol}"})
                    )
                    self.subscribed_symbols.discard(symbol)
                    self.queues.pop(symbol, None)
                    logger.info(f"❌ Auto-unsubscribed from {symbol} (no more clients)")
                except Exception as e:
                    logger.error(f"❌ Failed to auto-unsubscribe from {symbol}: {e}")

        logger.info("🔌 Client disconnected")

    async def stream_forever(self):
        """Polygon WebSocket Forever"""
        while True:
            try:
                await self.connect()

                async for msg in self.ws:
                    try:
                        data = json.loads(msg)
                    except json.JSONDecodeError:
                        continue

                    if not isinstance(data, list):
                        continue

                    for item in data:
                        if item.get("ev") == "Q":
                            symbol = item["sym"]
                            payload = {
                                "symbol": symbol,
                                "bid": item["bp"],
                                "ask": item["ap"],
                                "bid_size": item["bs"],
                                "ask_size": item["as"],
                                "timestamp": item["t"],
                            }
                            # print(f"Quote: {payload}")
                            q = self.queues.get(symbol)
                            if q:
                                await q.put(payload)

            except websockets.exceptions.ConnectionClosed:
                logger.warning(
                    "🔌 Polygon WebSocket connection closed, reconnecting..."
                )
                self.connected = False
                self.ws = None
                self.subscribed_symbols.clear()
                await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"❌ Error in stream_forever: {e}")
                self.connected = False
                self.ws = None
                self.subscribed_symbols.clear()
                await asyncio.sleep(5)
