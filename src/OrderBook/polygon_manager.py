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
        self.queues = {}  # 每个 symbol 一个队列
        self.connections = {}  # { websocket_client: [symbols] }
        self.subscribed_symbols = set()  # 已订阅的 symbols

    async def connect(self):
        try:
            url = "wss://socket.polygon.io/stocks"
            self.ws = await websockets.connect(url)

            # 发送认证
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
        """用户前端订阅行情"""
        if not self.connected:
            await self.connect()

        print(f"Debug: Subscribing to symbols: {symbols}")

        # 更新客户端的订阅列表
        if websocket_client not in self.connections:
            self.connections[websocket_client] = []

        # 合并新的 symbols 到客户端的订阅列表
        existing_symbols = set(self.connections[websocket_client])
        new_symbols = set(symbols)
        self.connections[websocket_client] = list(existing_symbols | new_symbols)

        for sym in symbols:
            if sym not in self.queues:
                self.queues[sym] = asyncio.Queue()

            # 只订阅还未在 Polygon 订阅的 symbols
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
        """用户前端取消某个 symbol"""
        if websocket_client in self.connections:
            if symbol in self.connections[websocket_client]:
                self.connections[websocket_client].remove(symbol)

        # 检查是否还有其他客户端订阅这个 symbol
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
        """用户前端断开连接"""
        # 获取该客户端订阅的所有 symbols
        client_symbols = self.connections.pop(websocket_client, [])

        # 检查每个 symbol 是否还被其他客户端需要
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
        """持续监听 Polygon WebSocket 数据"""
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
                        if item.get("ev") == "Q":  # Quote事件
                            symbol = item["sym"]
                            payload = {
                                "symbol": symbol,
                                "bid": item["bp"],
                                "ask": item["ap"],
                                "bid_size": item["bs"],
                                "ask_size": item["as"],
                                "timestamp": item["t"],
                            }
                            # print(f"📊 Quote: {payload}")
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
                await asyncio.sleep(5)  # 等待 5 秒后重连

            except Exception as e:
                logger.error(f"❌ Error in stream_forever: {e}")
                self.connected = False
                self.ws = None
                self.subscribed_symbols.clear()
                await asyncio.sleep(5)
