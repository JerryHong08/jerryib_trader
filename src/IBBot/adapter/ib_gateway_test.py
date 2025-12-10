# src/ibkr_bot/ib_gateway.py
import logging
import os

from ib_insync import IB

logger = logging.getLogger(__name__)

windows_host = os.getenv("WINDOWS_HOST")

print(f"debug: {windows_host}")

ib_config = {
    "Windows_TWS"[
        "host":windows_host,
        "port":7497,  # 7497 for paper, 7496 for live
        "client_id":"jerry_trade",
        "timeout":20,
    ],
    "Ubuntu_IBGateway"[
        "host":"localhost",
        "port":4002,  # 4001 for paper, 4002 for live
        "client_id":"jerry_trade",
        "timeout":20,
    ],
}


class IBGateway:
    # for windows TWS
    # def __init__(self, host=windows_host, port=7497, client_id=1, timeout=20):
    # for Ubuntu IB Gateway
    def __init__(self, host="127.0.0.1", port=4002, client_id=1, timeout=20):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.timeout = timeout
        self.ib = IB()

    def connect(self):
        try:
            self.ib.connect(
                self.host, self.port, clientId=self.client_id, timeout=self.timeout
            )
            logger.info("✅ Connected to IB Gateway (socket established)")
            logger.info(
                "IB serverVersion=%s", getattr(self.ib, "serverVersion", "unknown")
            )
            logger.info("IB clientId=%s", self.client_id)
        except Exception as e:
            logger.exception("❌ Failed to connect (handshake): %s", e)
            raise

    def disconnect(self):
        self.ib.disconnect()
        logger.info("🔌 Disconnected from IB Gateway")

    def get_current_time(self):
        return self.ib.reqCurrentTime()
