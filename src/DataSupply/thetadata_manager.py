# src/DataSupply/thetadata_manager.py
import asyncio
import datetime as dt
import json
import os
import traceback

# import arcticdb as adb
import pandas as pd
import pytz
import websockets

arctic = adb.Arctic("lmdb://arcticdb_options")
lib = arctic.get_library("trades", create_if_missing=True)


def get_trade_datetime(today, ms_of_day):
    return today + dt.timedelta(milliseconds=ms_of_day)


def get_days_to_expiration(today, expiration):
    return (expiration - today).days


# target
TARGET_CONTRACTS = [
    {"root": "QQQ", "expiration": "20250428", "strike": "462000", "right": "P"}
]


# helper method
def match_contract(msg_contract, targets):
    if not msg_contract:
        return False

    if isinstance(targets, list):
        for target in targets:
            if match_single_contract(msg_contract, target):
                return True
        return False


def match_single_contract(msg_contract, target):
    msg_root = msg_contract.get("root")
    msg_expiration = msg_contract.get("expiration")
    msg_strike = msg_contract.get("strike")
    msg_right = msg_contract.get("right")

    print(f"Debug msg value: \n{msg_root}\n{msg_expiration}\n{msg_strike}\n{msg_right}")

    return (
        msg_root == target["root"]
        and str(msg_expiration) == target["expiration"]
        and str(msg_strike) == target["strike"]
        and msg_right == target["right"]
    )


# Handler
# can be seen as callback
# [QQQ 4620P] {'header': {'type': 'QUOTE', 'status': 'CONNECTED'},
# 'contract': {'security_type': 'OPTION', 'root': 'QQQ', 'expiration': 20250428, 'strike': 462000, 'right': 'P'},
# 'quote': {'ms_of_day': 52123621, 'bid_size': 992, 'bid_exchange': 6, 'bid': 0.02, 'bid_condition': 50, 'ask_size': 2546, 'ask_exchange': 4, 'ask': 0.03, 'ask_condition': 50, 'date': 20250428}}
# [QQQ 4620P] {'header': {'type': 'OHLC', 'status': 'CONNECTED'},
# 'contract': {'security_type': 'OPTION', 'root': 'QQQ', 'expiration': 20250428, 'strike': 462000, 'right': 'P'},
# 'ohlc': {'ms_of_day': 52124417, 'open': 0.23, 'high': 0.46, 'low': 0.01, 'close': 0.02, 'volume': 369267, 'count': 63553, 'date': 20250428}}
# [QQQ 4620P] {'header': {'type': 'TRADE', 'status': 'CONNECTED'},
# 'contract': {'security_type': 'OPTION', 'root': 'QQQ', 'expiration': 20250428, 'strike': 462000, 'right': 'P'},
# 'trade': {'ms_of_day': 52124417, 'sequence': -256390465, 'size': 1, 'condition': 18, 'price': 0.02, 'exchange': 5, 'date': 20250428}}
async def process_qqq_4620p(msg):
    print("[QQQ 4620P]", msg)
    # today = dt.datetime.now(
    #     pytz.timezone("US/Eastern")
    # ).replace(
    #     hour=0,
    #     minute=0,
    #     second=0,
    #     microsecond=0
    # )

    # I use this as today because my thetadata is on dev mode, which for when the market is not open.
    # In thetadata dev mode, the dev FPSS server replays data from a random trading day in the past.
    # see more in 'https://http-docs.thetadata.us/Streaming/Getting-Started.html#dev-fpss-for-development'
    # So the date in msg will be 'today'
    trade_data = msg.get("trade", {})
    if not trade_data:
        return
    date_str = str(trade_data.get("date", ""))
    if not date_str:
        return
    today = pytz.timezone("US/Eastern").localize(
        dt.datetime.strptime(date_str, "%Y%m%d")
    )

    contract_data = msg.get("contract", {})
    if match_contract(contract_data, TARGET_CONTRACTS):
        print("Debug: macthed.")
        trade_datetime = get_trade_datetime(today, msg.get("trade").get("ms_of_day"))
        expiration = pd.to_datetime(msg.get("contract").get("expiration")).tz_localize(
            "US/Eastern"
        )
        days_to_expiration = get_days_to_expiration(today, expiration)
        symbol = msg.get("contract").get("root")
        trade = {
            "root": symbol,
            "expiration": expiration,
            "days_to_expiration": days_to_expiration,
            "is_call": msg.get("contract").get("isCall"),
            "strike": msg.get("contract").get("stike"),
            "size": msg.get("trade").get("size"),
            "trade_price": msg.get("trade").get("prize"),
            "exchange": str(msg.get("trade").get("exchange")),
        }
        print(f"Debug trade: {trade}")
        trade_df = pd.DataFrame(trade, index=[trade_datetime])
        if symbol in lib.list_symbols():
            lib.update(symbol, trade_df, upsert=True)
        else:
            lib.write(symbol, trade_df)


CONTRACT_HANDLERS = {("QQQ", 20250428, 462000, "P"): process_qqq_4620p}


# Websocket reader
async def websocket_reader(uri, outgoing_queue):
    old_http_proxy = os.environ.get("http_proxy")
    old_https_proxy = os.environ.get("https_proxy")

    try:
        if "http_proxy" in os.environ:
            del os.environ["http_proxy"]
        if "https_proxy" in os.environ:
            del os.environ["https_proxy"]

        async with websockets.connect(uri) as ws:
            print("[Connected to ThetaData]")
            req = {}
            req["msg_type"] = "STREAM"
            # req['msg_type'] = 'STREAM_BULK' # Whole market data
            req["sec_type"] = "OPTION"
            req["req_type"] = "TRADE"
            # req['add'] = False
            req["add"] = True
            req["id"] = 0
            req["contract"] = {}
            req["contract"]["root"] = "QQQ"
            req["contract"]["expiration"] = "20250428"
            req["contract"]["strike"] = "462000"
            req["contract"]["right"] = "P"
            print(f"Debug req:{req}")

            # await ws.send(json.dumps(req))
            await ws.send(req.__str__())

            while True:
                async for message in ws:
                    await outgoing_queue.put(message)

    finally:
        if old_http_proxy:
            os.environ["http_proxy"] = old_http_proxy
        if old_https_proxy:
            os.environ["https_proxy"] = old_https_proxy


# Dispatcher
async def dispatcher(incoming_queue):
    while True:
        raw = await incoming_queue.get()
        try:
            msg = json.loads(raw)
            print(f"Debug msg: \n{msg}")
            c = msg.get("contract", None)
            if not c:
                continue

            # match_contract(c, TARGET_CONTRACTS)

            key = (c.get("root"), c.get("expiration"), c.get("strike"), c.get("right"))
            # print(f'Debug key: {key}')
            # Debug key: ('TSLA', 20250620, 210000, 'P')
            # Debug key: ('TSLA', 20250516, 360000, 'C')
            handler = CONTRACT_HANDLERS.get(key)

            if handler:
                asyncio.create_task(handler(msg))

        except Exception as e:
            print("Dispatcher error:", e)
            traceback.print_exc()


async def main():
    queue = asyncio.Queue()

    uri = "ws://127.0.0.1:25520/v1/events"

    await asyncio.gather(websocket_reader(uri, queue), dispatcher(queue))


if __name__ == "__main__":
    # Recieve and print
    asyncio.run(main())
