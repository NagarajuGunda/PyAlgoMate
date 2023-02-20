import logging
import glob
import datetime

from pyalgomate.brokers.finvasia.broker import BacktestingBroker
from pyalgomate.strategies.OptionsStrangleIntraday import OptionsStrangleIntraday
from pyalgomate.strategies.OptionsStraddleIntraday import OptionsStraddleIntraday
from pyalgotrade.stratanalyzer import returns as stratReturns, drawdown, trades
from pyalgomate.backtesting import CustomCSVFeed

logging.basicConfig(level=logging.INFO)


def main(dataFiles):
    underlyingInstrument = 'BANKNIFTY'
    start = datetime.datetime.now()
    feed = CustomCSVFeed.CustomCSVFeed()
    for files in dataFiles:
        for file in glob.glob(files):
            feed.addBarsFromParquet(path=file, ticker=underlyingInstrument)

    print("")
    print(f"Time took in loading data <{datetime.datetime.now()-start}>")
    start = datetime.datetime.now()

    broker = BacktestingBroker(200000, feed)
    strat = OptionsStraddleIntraday(feed, broker, underlyingInstrument)

    returnsAnalyzer = stratReturns.Returns()
    tradesAnalyzer = trades.Trades()
    drawDownAnalyzer = drawdown.DrawDown()

    strat.attachAnalyzer(returnsAnalyzer)
    strat.attachAnalyzer(drawDownAnalyzer)
    strat.attachAnalyzer(tradesAnalyzer)

    strat.run()

    print("")
    print(
        f"Time took in running the strategy <{datetime.datetime.now()-start}>")
    start = datetime.datetime.now()

    print("")
    print("Final portfolio value: ₹ %.2f" % strat.getResult())
    print("Cumulative returns: %.2f %%" %
          (returnsAnalyzer.getCumulativeReturns()[-1] * 100))
    print("Max. drawdown: %.2f %%" % (drawDownAnalyzer.getMaxDrawDown() * 100))
    print("Longest drawdown duration: %s" %
          (drawDownAnalyzer.getLongestDrawDownDuration()))

    print("")
    print("Total trades: %d" % (tradesAnalyzer.getCount()))
    if tradesAnalyzer.getCount() > 0:
        profits = tradesAnalyzer.getAll()
        print("Avg. profit: ₹ %2.f" % (profits.mean()))
        print("Profits std. dev.: ₹ %2.f" % (profits.std()))
        print("Max. profit: ₹ %2.f" % (profits.max()))
        print("Min. profit: ₹ %2.f" % (profits.min()))
        returns = tradesAnalyzer.getAllReturns()
        print("Avg. return: %2.f %%" % (returns.mean() * 100))
        print("Returns std. dev.: %2.f %%" % (returns.std() * 100))
        print("Max. return: %2.f %%" % (returns.max() * 100))
        print("Min. return: %2.f %%" % (returns.min() * 100))
        commissions = tradesAnalyzer.getCommissionsForAllTrades()
        print("Avg. commission: ₹ %2.f" % (commissions.mean()))
        print("Tot. commission: ₹ %2.f" % (commissions.sum()))
        print("Commissions std. dev.: ₹ %2.f" % (commissions.std()))
        print("Max. commission: ₹ %2.f" % (commissions.max()))
        print("Min. commission: ₹ %2.f" % (commissions.min()))

    print("")
    print("Profitable trades: %d" % (tradesAnalyzer.getProfitableCount()))
    if tradesAnalyzer.getProfitableCount() > 0:
        profits = tradesAnalyzer.getProfits()
        print("Avg. profit: ₹ %2.f" % (profits.mean()))
        print("Profits std. dev.: ₹ %2.f" % (profits.std()))
        print("Max. profit: ₹ %2.f" % (profits.max()))
        print("Min. profit: ₹ %2.f" % (profits.min()))
        returns = tradesAnalyzer.getPositiveReturns()
        print("Avg. return: %2.f %%" % (returns.mean() * 100))
        print("Returns std. dev.: %2.f %%" % (returns.std() * 100))
        print("Max. return: %2.f %%" % (returns.max() * 100))
        print("Min. return: %2.f %%" % (returns.min() * 100))
        commissions = tradesAnalyzer.getCommissionsForProfitableTrades()
        print("Avg. commission: ₹ %2.f" % (commissions.mean()))
        print("Tot. commission: ₹ %2.f" % (commissions.sum()))
        print("Commissions std. dev.: ₹ %2.f" % (commissions.std()))
        print("Max. commission: ₹ %2.f" % (commissions.max()))
        print("Min. commission: ₹ %2.f" % (commissions.min()))

    print("")
    print("Unprofitable trades: %d" % (tradesAnalyzer.getUnprofitableCount()))
    if tradesAnalyzer.getUnprofitableCount() > 0:
        losses = tradesAnalyzer.getLosses()
        print("Avg. loss: ₹ %2.f" % (losses.mean()))
        print("Losses std. dev.: ₹ %2.f" % (losses.std()))
        print("Max. loss: ₹ %2.f" % (losses.min()))
        print("Min. loss: ₹ %2.f" % (losses.max()))
        returns = tradesAnalyzer.getNegativeReturns()
        print("Avg. return: %2.f %%" % (returns.mean() * 100))
        print("Returns std. dev.: %2.f %%" % (returns.std() * 100))
        print("Max. return: %2.f %%" % (returns.max() * 100))
        print("Min. return: %2.f %%" % (returns.min() * 100))
        commissions = tradesAnalyzer.getCommissionsForUnprofitableTrades()
        print("Avg. commission: ₹ %2.f" % (commissions.mean()))
        print("Tot. commission: ₹ %2.f" % (commissions.sum()))
        print("Commissions std. dev.: ₹ %2.f" % (commissions.std()))
        print("Max. commission: ₹ %2.f" % (commissions.max()))
        print("Min. commission: ₹ %2.f" % (commissions.min()))

    print("")
    print(f"Time took in analyzing <{datetime.datetime.now()-start}>")


if __name__ == "__main__":
    main(["pyalgomate/backtesting/data/2022-*11.parquet"])
