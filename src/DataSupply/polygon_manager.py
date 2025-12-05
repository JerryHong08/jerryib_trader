# src/DataSupply/polygon_manager.py
import asyncio
import json
import logging

import websockets

from ..utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True)


class PolygonWebSocketManager:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.ws = None
        self.connected = False
        self.queues = {}  # dict for storing each symbol quotes/trades... data
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

    async def subscribe(self, websocket_client, symbols, events=["Q"]):
        """
        subscribe new symbols, supporting multiple event types.
        (e.g., T for Trades, Q for Quotes)
        """
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

            for ev in events:
                stream_key = f"{ev}.{sym}"

                # incrementally subscribe
                if stream_key not in self.subscribed_symbols:
                    try:
                        await self.ws.send(
                            json.dumps({"action": "subscribe", "params": stream_key})
                        )
                        self.subscribed_symbols.add(stream_key)
                        logger.info(f"📡 Subscribed to Polygon: {stream_key}")
                        print(f"📡 Successfully subscribed to {stream_key}")
                    except Exception as e:
                        logger.error(f"❌ Failed to subscribe to {stream_key}: {e}")
                        self.connected = False
                        self.ws = None
                else:
                    print(f"ℹ️ {sym} already subscribed to Polygon")

    async def unsubscribe(self, websocket_client, symbol, events=["Q"]):
        """
        unsubscribe symbol
        """

        if websocket_client in self.connections:
            if symbol in self.connections[websocket_client]:
                self.connections[websocket_client].remove(symbol)

        # check if there is other client subscribing this symbol
        still_needed = any(symbol in syms for syms in self.connections.values())

        if not still_needed and self.connected:
            for ev in events:
                stream_key = f"{ev}.{symbol}"
                if stream_key in self.subscribed_symbols:
                    try:
                        await self.ws.send(
                            json.dumps(
                                {"action": "unsubscribe", "params": f"Q.{symbol}"}
                            )
                        )
                        self.subscribed_symbols.discard(stream_key)
                        logger.info(f"❌ Unsubscribed from Polygon: {stream_key}")
                        print(f"❌ Unsubscribed for {stream_key}")
                    except Exception as e:
                        logger.error(f"❌ Failed to unsubscribe from {symbol}: {e}")
                        self.connected = False
                        self.ws = None
            self.queues.pop(symbol, None)
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
                # and stream_key in self.subscribed_symbols
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
                                "event_type": "Q",
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

                        # elif item.get("ev") == "T":  # Trade
                        #     symbol = item["sym"]
                        #     payload = {
                        #         "event_type": "T",
                        #         "symbol": symbol,
                        #         "price": item["p"],
                        #         "size": item["s"],
                        #         "timestamp": item["t"],
                        #     }

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
