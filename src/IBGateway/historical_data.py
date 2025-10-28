# util.startLoop()  # uncomment this line when in a notebook
import pydoc

from ib_insync import IB, Stock, util

ib = IB()
ib.connect("127.0.0.1", 4002, clientId=1)

contract = Stock("AAPL", "SMART", "USD")
bars = ib.reqHistoricalData(
    contract,
    endDateTime="",
    durationStr="1 D",
    barSizeSetting="1 min",
    whatToShow="MIDPOINT",
    useRTH=True,
)

# convert to pandas dataframe (pandas needs to be installed):
df = util.df(bars).to_string()
# print(df)
pydoc.pager(df)
