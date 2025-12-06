# src/ibkr_bot/main.py

import logging

from src.IBGateway.ib_gateway import IBGateway

logging.basicConfig(level=logging.INFO)


def main():
    gateway = IBGateway()
    gateway.connect()

    current_time = gateway.get_current_time()
    print("🕒 IBKR Current Time:", current_time)

    gateway.disconnect()


if __name__ == "__main__":
    main()
