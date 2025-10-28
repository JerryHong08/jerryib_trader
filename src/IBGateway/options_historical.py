from ib_insync import IB, Stock

from IBGateway.ib_gateway import IBGateway

ib = IB()
ib.connect(host="127.0.0.1", port=4002, clientId=1)

nvda = Stock("NVDA", "SMART", "USD")
ib.qualifyContracts(nvda)
[ticker] = ib.reqTickers(nvda)
nvdaValue = ticker.marketPrice

print(nvdaValue)

ib.disconnect()
