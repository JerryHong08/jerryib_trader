import os
import sqlite3
import threading
import time

import empyrical as ep
from client import IBClient
from contract import combo_leg, future, option, spread, stock
from order import BUY, SELL, limit, market
from wrapper import IBWrapper

windows_host = os.getenv("WINDOWS_HOST")


class IBApp(IBWrapper, IBClient):
    def __init__(self, ip, port, client_id, account, interval=5):
        IBWrapper.__init__(self)
        IBClient.__init__(self, wrapper=self)
        self.account = account
        self.create_table()
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
    # app = IBApp("127.0.0.1", 7497, client_id=10)
    aapl = stock("AAPL", "SMART", "USD")
    gbl = future("GBL", "EUREX", "202512")
    pltr = option("PLTR", "BOX", "20251114", 20, "C")
    limit_order = limit(BUY, 100, 190.00)

    # ------- Chapter12 Deploying an options combo strategy --------

    # long_call_contract = option("TSLA", "SMART", "202603", 500, "CALL")
    # long_call = app.resolve_contract(long_call_contract)

    # long_put_contract = option("TSLA", "SMART", "202603", 260, "PUT")
    # long_put = app.resolve_contract(long_put_contract)

    # leg_1 = combo_leg(long_call, 1, BUY)
    # leg_2 = combo_leg(long_put, 1, BUY)
    # long_strangle = spread([leg_1, leg_2])
    # order = market(BUY, 1)
    # app.send_order(long_strangle, order)

    # ------ Chapter12 Deploying an intradat multi-asset mean reversion strategy --------
    psx = stock("PSX", "SMART", "USD")
    ho = future("HO", "NYMEX", "202403")
    rb = future("RB", "NYMEX", "202403")
    cl = future("CL", "NYMEX", "202403")

    window = 60
    thresh = 2

    while True:
        data = app.get_historical_data_for_many(
            request_id=99,
            contracts=[psx, ho, rb, cl],
            duration="1 W",
            bar_size="1 min",
        ).dropna()
        # print(f'Debug data: {data}')

        data["crack_spread"] = data.HO + 2 * data.RB - 3 * data.CL
        data["crack_spread_rank"] = data.crack_spread.rolling(window).rank(pct=True)
        data["refiner_rank"] = data.PSX.rolling(window).rank(pct=True)
        data["rank_spread"] = data.refiner_rank - data.crack_spread_rank

        roll = data.rank_spread.rolling(window)
        zscore = (data.rank_spread - roll.mean()) / roll.std()
        signal = zscore[-1]
        holding = psx.symbol in app.positions.keys()

        if signal <= -thresh and not holding:
            order = market(BUY, 10)
            app.send_order(psx, order)
        elif signal >= 0 and holding:
            app.order_target_percent(psx, market, 0)

        if signal >= thresh and not holding:
            order = market(SELL, 10)
            app.send_order(psx, order)
        elif signal <= 0 and holding:
            app.order_target_percent(psx.market, 0)

        time.sleep(30)
    # app.disconnect()

    # Deploying a monthly factor portfolio strategy is skipped for
    # it's basically a Zipline backtest we have learned in Chapter 6&7
