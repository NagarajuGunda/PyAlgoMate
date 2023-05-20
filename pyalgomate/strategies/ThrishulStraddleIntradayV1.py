import logging
import datetime
import pandas as pd

import pyalgomate.utils as utils
from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
from pyalgomate.strategies.BaseOptionsGreeksStrategy import State, Expiry


class ThrishulStraddleIntradayV1(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker, underlying, callback=None, resampleFrequency=None, lotSize=None, collectData=None):
        super(ThrishulStraddleIntradayV1, self).__init__(feed, broker,
                                                         strategyName=__class__.__name__,
                                                         logger=logging.getLogger(
                                                             __file__),
                                                         callback=callback, resampleFrequency=resampleFrequency, collectData=collectData)

        self.entryTime = datetime.time(hour=9, minute=17)
        self.exitTime = datetime.time(hour=15, minute=15)
        self.expiry = Expiry.WEEKLY
        self.lotSize = lotSize if lotSize is not None else 25
        self.lots = 1
        self.quantity = self.lotSize * self.lots

        self.underlying = underlying
        self.strikeStep = 100

        self.__reset__()

    def __reset__(self):
        super().reset()
        # members that needs to be reset after exit time
        self.atmStrike = None
        self.downStraddle = self.atmStraddle = self.upStraddle = None

    def closeAllPositions(self):
        if self.state == State.EXITED:
            return

        self.state = State.EXITED
        for position in list(self.getActivePositions()):
            if not position.exitActive():
                position.exitMarket()

    def initiateStraddle(self, strike, expiry):
        ceSymbol = self.getOptionSymbol(
            self.underlying, expiry, strike, 'c')
        peSymbol = self.getOptionSymbol(
            self.underlying, expiry, strike, 'p')

        if self.haveLTP(ceSymbol) is None or self.haveLTP(peSymbol) is None:
            return

        self.log(f'Opening short straddle of strike <{strike}>')
        self.state = State.PLACING_ORDERS
        ceShort = self.enterShort(ceSymbol, self.quantity)
        peShort = self.enterShort(peSymbol, self.quantity)

        return (ceShort, peShort)

    def onBars(self, bars):
        self.log(f"Bar date times - {bars.getDateTime()}", logging.DEBUG)

        self.overallPnL = self.getOverallPnL()

        currentExpiry = utils.getNearestWeeklyExpiryDate(bars.getDateTime().date(
        ))

        if bars.getDateTime().time() >= self.marketEndTime:
            if (len(self.openPositions) + len(self.closedPositions)) > 0:
                self.log(
                    f"Overall PnL for {bars.getDateTime().date()} is {self.overallPnL}")
            if self.state != State.LIVE:
                self.__reset__()
        # Exit all positions if exit time is met or portfolio SL is hit
        elif (bars.getDateTime().time() >= self.exitTime):
            if self.state != State.EXITED:
                self.log(
                    f'Current time <{bars.getDateTime().time()}> has crossed exit time <{self.exitTime}. Closing all positions!')
                self.closeAllPositions()
        elif (self.state == State.LIVE) and (self.entryTime <= bars.getDateTime().time() < self.exitTime) and (self.haveLTP(self.underlying)):
            self.atmStrike = self.getATMStrike(
                self.getLTP(self.underlying), self.strikeStep)
            self.log('Initiating straddles')
            self.atmStraddle = self.initiateStraddle(
                self.atmStrike, currentExpiry)
            self.upStraddle = self.initiateStraddle(
                self.atmStrike + self.strikeStep, currentExpiry)
            self.downStraddle = self.initiateStraddle(
                self.atmStrike - self.strikeStep, currentExpiry)
        elif self.state == State.PLACING_ORDERS:
            for position in list(self.getActivePositions()):
                if position.getInstrument() not in self.openPositions:
                    return
            self.state = State.ENTERED
        elif self.state == State.ENTERED:
            atmStrike = self.getATMStrike(
                self.getLTP(self.underlying), self.strikeStep)
            if atmStrike > self.atmStrike:
                self.log(
                    f'Current ATM strike <{atmStrike}> is > than last known ATM strike <{self.atmStrike}>')
                self.log(
                    f'Closing <{self.atmStrike - self.strikeStep}> short straddle')
                self.state = State.PLACING_ORDERS
                self.downStraddle[0].exitMarket()
                self.downStraddle[1].exitMarket()
                self.downStraddle = self.atmStraddle
                self.atmStraddle = self.upStraddle
                self.upStraddle = self.initiateStraddle(
                    atmStrike + self.strikeStep, currentExpiry)
                self.atmStrike = atmStrike
            elif atmStrike < self.atmStrike:
                self.log(
                    f'Current ATM strike <{atmStrike}> is < than last known ATM strike <{self.atmStrike}>')
                self.log(
                    f'Closing <{self.atmStrike + self.strikeStep}> short straddle')
                self.state = State.PLACING_ORDERS
                self.upStraddle[0].exitMarket()
                self.upStraddle[1].exitMarket()
                self.upStraddle = self.atmStraddle
                self.atmStraddle = self.downStraddle
                self.downStraddle = self.initiateStraddle(
                    atmStrike - self.strikeStep, currentExpiry)
                self.atmStrike = atmStrike
        # Check if we are in the EXITED state
        elif self.state == State.EXITED:
            pass


if __name__ == "__main__":
    from pyalgomate.backtesting import CustomCSVFeed
    from pyalgomate.brokers import BacktestingBroker
    from pyalgotrade.stratanalyzer import returns as stratReturns, drawdown, trades
    import logging
    logging.basicConfig(
        filename='ThrishulStraddleIntradayV1.log', level=logging.INFO)

    underlyingInstrument = 'BANKNIFTY'

    start = datetime.datetime.now()
    feed = CustomCSVFeed.CustomCSVFeed()
    feed.addBarsFromParquets(dataFiles=[
                             "pyalgomate/backtesting/data/2023/banknifty/*.parquet"], ticker=underlyingInstrument)

    print("")
    print(f"Time took in loading data <{datetime.datetime.now()-start}>")
    start = datetime.datetime.now()

    broker = BacktestingBroker(200000, feed)
    strat = ThrishulStraddleIntradayV1(
        feed=feed, broker=broker, underlying=underlyingInstrument, lotSize=25)

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

    print("")
    print("Final portfolio value: ₹ %.2f" % strat.getResult())
    print("Cumulative returns: %.2f %%" %
          (returnsAnalyzer.getCumulativeReturns()[-1] * 100))
    print("Max. drawdown: %.2f %%" % (drawDownAnalyzer.getMaxDrawDown() * 100))
    print("Longest drawdown duration: %s" %
          (drawDownAnalyzer.getLongestDrawDownDuration()))

    print("")
    print("Total trades: %d" % (tradesAnalyzer.getCount()))
