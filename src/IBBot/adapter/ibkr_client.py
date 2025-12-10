import time

import pandas as pd
from ibapi.client import EClient

from utils import DEFAULT_MARKET_DATA_ID, TRADE_BAR_PROPERTIES, Tick


# Imports a base class from the IB API and impletments a custom class we'll use to build our traing app:
class IBClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)
        self.market_data = {}

    def cancel_all_orders(self):
        self.reqGlobalCancel()

    def cancel_order_by_id(self, order_id):
        self.cancelOrder(orderId=order_id, manualCancelOrderTime="")

    def update_order(self, contract, order, order_id):
        self.cancel_order_by_id(order_id)
        return self.send_order(contract, order)

    def send_order(self, contract, order):
        order_id = self.wrapper.nextValidOrderId
        self.placeOrder(orderId=order_id, contract=contract, order=order)
        self.reqIds(-1)
        return order_id

    # --------- Chapter12 Sending orders based on portfolio targets ---------

    # Add the method that computes the quantity and places an order for a fixed amount of money:
    def order_value(self, contract, order_type, value, **kwargs):
        quantity = self._calculate_order_value_quantity(contract, value)
        order = order_type(quantity=quantity, **kwargs)
        return self.send_order(contract, order)

    # Add the method to order the specified asset according to the percent of the current portfolio value:
    def order_percent(self, contract, order_type, percent, **kwargs):
        quantity = self._calculate_order_percent_quantity(contract, percent)
        order = order_type(quantity=quantity, **kwargs)
        return self.send_order(contract, order)

    def _calculate_order_percent_quantity(self, contract, percent):
        net_liquidation_value = self.get_account_values(key="NetLiquidation")[0]
        value = net_liquidation_value * percent
        return self._calculate_order_value_quantity(contract, value)

    # update orders. If the positions doesn't exist, the code sends a new order. If the position
    # exists, the code sends an order for the difference between the target value and the current position value.
    def order_target_value(self, contract, order_type, target, **kwargs):
        target_quantity = self._calculate_order_value_quantity(contract, target)
        quantity = self._calculate_order_target_quantity(contract, target_quantity)
        order = order_type(
            action="SELL" if quantity < 0 else "BUY", quantity=abs(quantity), **kwargs
        )
        return self.send_order(contract, order)

    # Add the method to place an order to adjust a position to the target number of contracts:
    def order_target_quantity(self, contract, order_type, target, **kwargs):
        quantity = self._calculate_order_target_quantity(contract, target)
        order = order_type(
            action="SELL" if quantity < 0 else "BUY", quantity=abs(quantity), **kwargs
        )
        return self.send_order(contract, order)

    # Add the helper method that loops through the positions and computes the target number of contracts to order:
    def _calculate_order_target_quantity(self, contract, target):
        positions = self.get_positions()
        if contract.symbol in positions.keys():
            current_position = positions[contract.symbol]["position"]
            target -= current_position
        return int(target)

    # the helper method
    def _calculate_order_value_quantity(self, contract, value):
        last_price = self.get_market_data(
            request_id=DEFAULT_MARKET_DATA_ID, contract=contract, tick_type=4
        )
        multiplier = contract.multiplier if contract.multiplier != "" else 1
        return int(value / (last_price * multiplier))

    # same as the order_target_quantity but based on percent
    def order_target_percent(self, contract, order_type, target, **kwargs):
        quantity = self._calculate_order_target_percent_quantity(contract, target)
        order = order_type(
            action="SELL" if quantity < 0 else "BUY", quantity=abs(quantity), **kwargs
        )
        return self.send_order(contract, order)

    def _calculate_order_target_percent_quantity(self, contract, target):
        target_quantity = self._calculate_order_percent_quantity(contract, target)
        return self._calculate_order_target_quantity(contract, target_quantity)

    # --------------------------------------------------------------

    # historical data
    def get_historical_data(self, request_id, contract, duration, bar_size):
        self.reqHistoricalData(
            reqId=request_id,
            contract=contract,
            endDateTime="",
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow="MIDPOINT",
            useRTH=1,
            formatDate=1,
            keepUpToDate=False,
            chartOptions=[],
        )
        time.sleep(5)
        bar_sizes = ["day", "D", "week", "W", "month"]
        if any(x in bar_size for x in bar_sizes):
            fmt = "%Y%m%d"
        else:
            fmt = "%Y%m%d %H:%M:%S %Z"

        data = self.historical_data[request_id]
        df = pd.DataFrame(data, columns=TRADE_BAR_PROPERTIES)
        df.set_index(pd.to_datetime(df.time, format=fmt), inplace=True)
        df.drop("time", axis=1, inplace=True)
        df["symbol"] = contract.symbol
        df.request_id = request_id
        return df

    def get_historical_data_for_many(
        self, request_id, contracts, duration, bar_size, col_to_use="close"
    ):
        dfs = []
        for contract in contracts:
            data = self.get_historical_data(request_id, contract, duration, bar_size)
            dfs.append(data)
            request_id += 1
        return (
            pd.concat(dfs)
            .reset_index()
            .pivot(index="time", columns="symbol", values=col_to_use)
        )

    def get_market_data(self, request_id, contract, tick_type=4):
        self.reqMktData(
            reqId=request_id,
            contract=contract,
            genericTickList="",
            snapshot=True,
            regulatorySnapshot=False,
            mktDataOptions=[],
        )
        time.sleep(5)
        self.cancelMktData(reqId=request_id)
        return self.market_data[request_id].get(tick_type)

    def tickPrice(self, request_id, tick_type, price, attrib):
        if request_id not in self.market_data.keys():
            self.market_data[request_id] = {}
            self.market_data[request_id][tick_type] = float(price)

    def get_streaming_data(self, request_id, contract):
        self.reqTickByTickData(
            reqId=request_id,
            contract=contract,
            tickType="BidAsk",
            numberOfTicks=0,
            ignoreSize=True,
        )
        time.sleep(10)
        while True:
            if self.stream_event.is_set():
                yield Tick(*self.streaming_data[request_id])
                self.stream_event.clear()

    def stop_streaming_data(self, request_id):
        self.cancelTickByTickData(reqId=request_id)

    def get_account_values(self, key=None):
        self.reqAccountUpdates(True, self.account)
        time.sleep(2)
        if key:
            return self.account_values[key]
        return self.account_values

    def get_positions(self):
        self.reqAccountUpdates(True, self.account)
        time.sleep(2)
        return self.positions

    def get_pnl(self, request_id):
        self.reqPnL(request_id, self.account, "")
        time.sleep(2)
        return self.account_pnl

    def get_streaming_pnl(self, request_id, interval=60, pnl_type="unrealized_pnl"):
        interval = max(interval, 5) - 2
        while True:
            pnl = self.get_pnl(request_id=request_id)
            yield {"date": pd.Timestamp.now(), "pnl": pnl[request_id].get(pnl_type)}
            time.sleep(interval)

    def get_streaming_returns(self, request_id, interval, pnl_type):
        returns = pd.Series
        for snapshot in self.get_streaming_pnl(
            request_id=request_id, interval=interval, pnl_type=pnl_type
        ):
            returns.loc[snapshot["date"]] = snapshot["pnl"]
            if len(returns) > 1:
                self.portfolio_returns = returns.pct_change().dropna()

    def resolve_contract(self, contract, request_id=DEFAULT_MARKET_DATA_ID):
        self.reqContractDetails(reqId=request_id, contract=contract)
        time.sleep(2)
        self.contractDetailsEnd(reqId=request_id)
        return self.resolved_contract
