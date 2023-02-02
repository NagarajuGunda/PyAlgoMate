from pyalgotrade import strategy
import pyalgomate.backtesting.CustomCSVFeed as CustomCSVFeed
from pyalgotrade.barfeed import quandlfeed


class MyStrategy(strategy.BacktestingStrategy):
    def __init__(self, feed):
        super(MyStrategy, self).__init__(feed)

    def onBars(self, bars):
        for key, value in bars.items():
            if key == "BANKNIFTY":
                print(f"{value.getDateTime()}")


# Load the bar feed from the CSV file
feed = CustomCSVFeed.CustomCSVFeed()
feed.addBarsFromCSV("nov-2022-monthly.csv", skipMalformedBars=True)

# Evaluate the strategy with the feed's bars.
myStrategy = MyStrategy(feed)
myStrategy.run()
