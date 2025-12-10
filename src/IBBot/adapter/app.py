import os
import sqlite3
import threading
import time

import empyrical as ep

from IBBot.adapter.ibkr_client import IBClient
from IBBot.adapter.ibkr_wrapper import IBWrapper
from IBBot.models.contract import stock
from IBBot.models.order import BUY, SELL, limit, market
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True)

windows_host = os.getenv("WINDOWS_HOST")


class IBApp(IBWrapper, IBClient):
    def __init__(self, ip, port, client_id, account, interval=5):
        IBWrapper.__init__(self)
        IBClient.__init__(self, wrapper=self)
        self.account = account
        # self.create_table()

        self.connect(ip, port, client_id)

        threading.Thread(target=self.run, daemon=True).start()
        time.sleep(2)
        threading.Thread(
            target=self.get_streaming_returns,
            agrs=(99, interval, "unrealized_pnl"),
            daemon=True,
        ).start()

    @property
    def connection(self):
        return sqlite3.connect("tick_data.sqlite", isolation_level=None)

    def create_table(self):
        cursor = self.connection.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS bid_ask_data ("
            "timestamp datetime, symbol string, "
            "bid_price real, ask_price real, "
            "bid_size integer, ask_size integer)"
        )

    def stream_to_sqlite(self, request_id, contract, run_for_in_seconds=23400):
        cursor = self.connection.cursor()
        end_time = time.time() + run_for_in_seconds + 10
        for tick in self.get_streaming_data(request_id, contract):
            query = (
                "INSERT INTO bid_ask_data ("
                "timestamp, symbol, bid_price, "
                "ask_price, bid_size, ask_size) "
                "VALUES (?, ?, ?, ?, ?, ?)"
            )
            values = (
                tick.timestamp_.strftime("%Y-%m-%d %H:%M:%S"),
                contract.symbol,
                tick.bid_price,
                tick.ask_price,
                tick.bid_size,
                tick.ask_size,
            )
            cursor.execute(query, values)
            if time.time() >= end_time:
                break
        self.stop_streaming_data(request_id)

    @property
    def cumulative_returns(self):
        return ep.cum_returns(self.portfolio_returns, 1)

    @property
    def max_drawdown(self):
        return ep.max_drawdown(self.portfolio_returns)

    @property
    def volatility(self):
        return self.portfolio_returns.std(ddof=1)

    @property
    def omega_ratio(self):
        return ep.omega_ratio(self.portfolio_returns, annualization=1)

    @property
    def sharpe_ratio(self):
        return self.portfolio_returns.mean() / self.portfolio_returns.std(ddof=1)

    @property
    def cvar(self):
        net_liquidation = self.get_account_values("Netliquidation")[0]
        cvar_ = ep.conditional_value_at_risk(self.portfolio_returns)
        return (cvar_, cvar_ * net_liquidation)


if __name__ == "__main__":

    app = IBApp(windows_host, 7497, client_id=10)
    security = stock("AAPL", "SMART", "USD")

    window = 60
    thresh = 2
    order = market(BUY, 10)
    app.send_order(security, order)

    app.disconnect()
