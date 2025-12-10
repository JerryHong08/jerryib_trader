# src/ibkr_bot/main.py


from ..utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True)


from IBBot.adapter.ib_gateway_test import IBGateway


def main():
    gateway = IBGateway()
    gateway.connect()

    current_time = gateway.get_current_time()
    print("🕒 IBKR Current Time:", current_time)

    gateway.disconnect()


if __name__ == "__main__":
    main()
