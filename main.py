import os
from typing import List

import dotenv
import polars as pl
from polygon import RESTClient, WebSocketClient
from polygon.rest.models import TickerSnapshot
from polygon.websocket.models import Feed, Market, WebSocketMessage

dotenv.load_dotenv()

polygon_api_key = os.getenv("POLYGON_API_KEY")

client = WebSocketClient(
    api_key=polygon_api_key, feed=Feed.RealTime, market=Market.Stocks
)

# quotes
client.subscribe("Q.AAPL")


def handle_msg(msgs: List[WebSocketMessage]):
    for m in msgs:
        print(m)


# print messages
client.run(handle_msg)
